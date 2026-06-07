//! Shared arithmetic helpers for aggregate accumulators.
//!
//! Hash and sort aggregates keep independent state machines, but dense
//! vector arithmetic must stay identical across both paths.

use num_traits::ToPrimitive;
use ultrasql_core::Value;

use crate::ExecError;

pub(crate) fn widen_sum_seed(value: Value) -> Value {
    match value {
        Value::Int16(value) => Value::Int64(i64::from(value)),
        Value::Int32(value) => Value::Int64(i64::from(value)),
        Value::Float32(value) => Value::Float64(f64::from(value)),
        other => other,
    }
}

pub(crate) fn usize_to_f64(value: usize) -> f64 {
    value.to_f64().unwrap_or(f64::MAX)
}

pub(crate) fn percentile_cont_indexes(row_number: f64, len: usize) -> (usize, usize) {
    let max_index = len.saturating_sub(1);
    (
        bounded_nonnegative_index(row_number.floor(), max_index),
        bounded_nonnegative_index(row_number.ceil(), max_index),
    )
}

pub(crate) fn percentile_disc_index(fraction: f64, len: usize) -> usize {
    let max_index = len.saturating_sub(1);
    let rank = (fraction * usize_to_f64(len)).ceil();
    if rank.is_nan() || rank <= 1.0 {
        0
    } else {
        rank.to_usize()
            .unwrap_or(len)
            .saturating_sub(1)
            .min(max_index)
    }
}

fn bounded_nonnegative_index(value: f64, max_index: usize) -> usize {
    if value.is_nan() || value <= 0.0 {
        0
    } else {
        value.to_usize().unwrap_or(max_index).min(max_index)
    }
}

pub(crate) fn add_dense_vector_values(
    left: Vec<f32>,
    right: Vec<f32>,
    type_name: &str,
) -> Result<Vec<f32>, ExecError> {
    if left.len() != right.len() {
        return Err(ExecError::TypeMismatch(format!(
            "{type_name} sum dimension mismatch: left {}, right {}",
            left.len(),
            right.len()
        )));
    }

    let mut out = Vec::with_capacity(left.len());
    for (idx, (left, right)) in left.into_iter().zip(right).enumerate() {
        let sum = left + right;
        if !sum.is_finite() {
            return Err(ExecError::TypeMismatch(format!(
                "{type_name} sum produced non-finite element at index {idx}"
            )));
        }
        out.push(sum);
    }
    Ok(out)
}

pub(crate) fn divide_dense_vector_values(values: Vec<f32>, count: i64) -> Vec<f32> {
    debug_assert!(count > 0, "vector average count must be positive");
    #[allow(
        clippy::cast_precision_loss,
        reason = "aggregate count becomes a dense-vector denominator"
    )]
    let denominator = count as f32;
    values
        .into_iter()
        .map(|value| value / denominator)
        .collect()
}
