//! Shared arithmetic helpers for aggregate accumulators.
//!
//! Hash and sort aggregates keep independent state machines, but dense
//! vector arithmetic must stay identical across both paths.

use crate::ExecError;

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
