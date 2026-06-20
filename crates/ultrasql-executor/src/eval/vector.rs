//! Vector distance/norm builtins and dispatch helpers.
//!
//! Extracted verbatim from the original `eval.rs`; pure code motion.

use super::*;

#[derive(Clone, Copy, Debug)]
pub(crate) enum VectorDistanceOp {
    L2,
    InnerProduct,
    NegativeInnerProduct,
    Cosine,
    L1,
}

pub(crate) fn eval_vector_metric(args: &[Value], op: VectorDistanceOp) -> Result<Value, EvalError> {
    if args.len() != 2 {
        return Err(EvalError::Type(format!(
            "vector metric: expected 2 args, got {}",
            args.len()
        )));
    }
    if matches!(args, [Value::Null, _] | [_, Value::Null]) {
        return Ok(Value::Null);
    }
    vector_distance(&args[0], &args[1], op)
}

pub(crate) fn eval_vector_norm(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "vector_norm: expected 1 arg, got {}",
            args.len()
        )));
    }
    if matches!(args[0], Value::Null) {
        return Ok(Value::Null);
    }
    vector_norm(&args[0]).map(Value::Float64)
}

pub(crate) fn eval_vector_dims(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "vector_dims: expected 1 arg, got {}",
            args.len()
        )));
    }
    if matches!(args[0], Value::Null) {
        return Ok(Value::Null);
    }
    let dims = vector_value_dims(&args[0]).ok_or_else(|| {
        EvalError::Type(format!(
            "vector_dims requires vector-family value, got {:?}",
            args[0].data_type()
        ))
    })?;
    let dims = i32::try_from(dims)
        .map_err(|_| EvalError::Type("vector_dims: dimension count exceeds int32".to_owned()))?;
    Ok(Value::Int32(dims))
}

pub(crate) fn vector_distance(
    left: &Value,
    right: &Value,
    op: VectorDistanceOp,
) -> Result<Value, EvalError> {
    if vector_metric_kind(left) != vector_metric_kind(right) || vector_metric_kind(left).is_none() {
        return Err(EvalError::Type(format!(
            "vector distance requires matching vector, halfvec, or sparsevec operands, got {:?} and {:?}",
            left.data_type(),
            right.data_type()
        )));
    }
    let left_dims = vector_value_dims(left).ok_or_else(|| {
        EvalError::Type(format!(
            "vector distance requires vector-family left operand, got {:?}",
            left.data_type()
        ))
    })?;
    let right_dims = vector_value_dims(right).ok_or_else(|| {
        EvalError::Type(format!(
            "vector distance requires vector-family right operand, got {:?}",
            right.data_type()
        ))
    })?;
    if left_dims != right_dims {
        return Err(EvalError::Type(format!(
            "vector dimension mismatch: {} and {}",
            left_dims, right_dims
        )));
    }
    if left_dims == 0 {
        return Err(EvalError::Type(
            "vector distance requires non-empty vectors".to_owned(),
        ));
    }

    let result = match (left, right) {
        (Value::Vector(left), Value::Vector(right))
        | (Value::HalfVec(left), Value::HalfVec(right)) => dense_vector_distance(left, right, op)?,
        (Value::SparseVec(left), Value::SparseVec(right)) => {
            sparse_vector_distance(left, right, op)?
        }
        _ => {
            return Err(EvalError::Type(format!(
                "vector distance requires matching vector, halfvec, or sparsevec operands, got {:?} and {:?}",
                left.data_type(),
                right.data_type()
            )));
        }
    };
    Ok(Value::Float64(result))
}

pub(crate) fn dense_vector_distance(
    left: &[f32],
    right: &[f32],
    op: VectorDistanceOp,
) -> Result<f64, EvalError> {
    if left
        .iter()
        .chain(right.iter())
        .any(|value| !value.is_finite())
    {
        return Err(EvalError::Type(
            "vector distance requires finite elements".to_owned(),
        ));
    }
    let result = match op {
        VectorDistanceOp::L2 => {
            f64::from(ultrasql_vec::kernels::vector::l2_distance_f32(left, right))
        }
        VectorDistanceOp::InnerProduct => {
            f64::from(ultrasql_vec::kernels::vector::dot_f32(left, right))
        }
        VectorDistanceOp::NegativeInnerProduct => {
            -f64::from(ultrasql_vec::kernels::vector::dot_f32(left, right))
        }
        VectorDistanceOp::Cosine => f64::from(
            ultrasql_vec::kernels::vector::cosine_distance_f32(left, right).ok_or_else(|| {
                EvalError::Type("cosine distance requires non-zero vectors".to_owned())
            })?,
        ),
        VectorDistanceOp::L1 => left
            .iter()
            .zip(right.iter())
            .map(|(l, r)| (f64::from(*l) - f64::from(*r)).abs())
            .sum::<f64>(),
    };
    Ok(result)
}

pub(crate) fn sparse_vector_distance(
    left: &SparseVector,
    right: &SparseVector,
    op: VectorDistanceOp,
) -> Result<f64, EvalError> {
    let result = match op {
        VectorDistanceOp::L2 => sparse_l2_squared(left, right).sqrt(),
        VectorDistanceOp::InnerProduct => sparse_dot(left, right),
        VectorDistanceOp::NegativeInnerProduct => -sparse_dot(left, right),
        VectorDistanceOp::Cosine => {
            let left_norm = sparse_norm(left);
            let right_norm = sparse_norm(right);
            if left_norm == 0.0 || right_norm == 0.0 {
                return Err(EvalError::Type(
                    "cosine distance requires non-zero vectors".to_owned(),
                ));
            }
            1.0 - (sparse_dot(left, right) / (left_norm * right_norm))
        }
        VectorDistanceOp::L1 => sparse_l1(left, right),
    };
    Ok(result)
}

pub(crate) fn vector_norm(value: &Value) -> Result<f64, EvalError> {
    match value {
        Value::Vector(values) | Value::HalfVec(values) => {
            if values.iter().any(|value| !value.is_finite()) {
                return Err(EvalError::Type(
                    "vector_norm requires finite elements".to_owned(),
                ));
            }
            Ok(values
                .iter()
                .map(|value| {
                    let value = f64::from(*value);
                    value * value
                })
                .sum::<f64>()
                .sqrt())
        }
        Value::SparseVec(vector) => Ok(sparse_norm(vector)),
        other => Err(EvalError::Type(format!(
            "vector_norm requires vector, halfvec, or sparsevec, got {:?}",
            other.data_type()
        ))),
    }
}

pub(crate) fn vector_value_dims(value: &Value) -> Option<usize> {
    match value {
        Value::Vector(values) | Value::HalfVec(values) => Some(values.len()),
        Value::SparseVec(vector) => usize::try_from(vector.dims).ok(),
        Value::BitVec { dims, .. } => usize::try_from(*dims).ok(),
        _ => None,
    }
}

pub(crate) fn vector_metric_kind(value: &Value) -> Option<u8> {
    match value {
        Value::Vector(_) => Some(0),
        Value::HalfVec(_) => Some(1),
        Value::SparseVec(_) => Some(2),
        Value::BitVec { .. } => None,
        _ => None,
    }
}

pub(crate) fn sparse_dot(left: &SparseVector, right: &SparseVector) -> f64 {
    let mut left_idx = 0_usize;
    let mut right_idx = 0_usize;
    let mut dot = 0.0_f64;
    while left_idx < left.entries.len() && right_idx < right.entries.len() {
        let (left_pos, left_value) = left.entries[left_idx];
        let (right_pos, right_value) = right.entries[right_idx];
        match left_pos.cmp(&right_pos) {
            std::cmp::Ordering::Equal => {
                dot += f64::from(left_value) * f64::from(right_value);
                left_idx += 1;
                right_idx += 1;
            }
            std::cmp::Ordering::Less => left_idx += 1,
            std::cmp::Ordering::Greater => right_idx += 1,
        }
    }
    dot
}

pub(crate) fn sparse_norm(vector: &SparseVector) -> f64 {
    vector
        .entries
        .iter()
        .map(|(_, value)| {
            let value = f64::from(*value);
            value * value
        })
        .sum::<f64>()
        .sqrt()
}

pub(crate) fn sparse_l2_squared(left: &SparseVector, right: &SparseVector) -> f64 {
    sparse_union_fold(left, right, |left, right| {
        let delta = left - right;
        delta * delta
    })
}

pub(crate) fn sparse_l1(left: &SparseVector, right: &SparseVector) -> f64 {
    sparse_union_fold(left, right, |left, right| (left - right).abs())
}

pub(crate) fn sparse_union_fold(
    left: &SparseVector,
    right: &SparseVector,
    contribution: impl Fn(f64, f64) -> f64,
) -> f64 {
    let mut left_idx = 0_usize;
    let mut right_idx = 0_usize;
    let mut acc = 0.0_f64;
    while left_idx < left.entries.len() || right_idx < right.entries.len() {
        match (left.entries.get(left_idx), right.entries.get(right_idx)) {
            (Some(&(left_pos, left_value)), Some(&(right_pos, right_value))) => {
                match left_pos.cmp(&right_pos) {
                    std::cmp::Ordering::Equal => {
                        acc += contribution(f64::from(left_value), f64::from(right_value));
                        left_idx += 1;
                        right_idx += 1;
                    }
                    std::cmp::Ordering::Less => {
                        acc += contribution(f64::from(left_value), 0.0);
                        left_idx += 1;
                    }
                    std::cmp::Ordering::Greater => {
                        acc += contribution(0.0, f64::from(right_value));
                        right_idx += 1;
                    }
                }
            }
            (Some(&(_, left_value)), None) => {
                acc += contribution(f64::from(left_value), 0.0);
                left_idx += 1;
            }
            (None, Some(&(_, right_value))) => {
                acc += contribution(0.0, f64::from(right_value));
                right_idx += 1;
            }
            (None, None) => break,
        }
    }
    acc
}
