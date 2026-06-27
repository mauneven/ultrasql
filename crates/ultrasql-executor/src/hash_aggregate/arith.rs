//! Numeric helpers shared by the aggregate state machine and the
//! vectorised kernels.
//!
//! These cover the widening arithmetic that `SUM`/`AVG` accumulators rely
//! on, the checked integer counters, decimal rescaling, and the
//! `f64`/ordering coercions used when folding numeric values.

use num_traits::ToPrimitive;
use ultrasql_core::Value;

use crate::ExecError;
use crate::aggregate_math::{add_dense_vector_values, divide_dense_vector_values};

// ---------------------------------------------------------------------------
// Checked integer counters
// ---------------------------------------------------------------------------

pub(crate) fn increment_count(acc: &mut i64, delta: i64) -> Result<(), ExecError> {
    *acc = acc.checked_add(delta).ok_or_else(|| {
        ExecError::NumericFieldOverflow("HashAggregate COUNT overflow".to_owned())
    })?;
    Ok(())
}

pub(crate) fn checked_sum_i64(acc: i64, delta: i64, context: &str) -> Result<i64, ExecError> {
    acc.checked_add(delta)
        .ok_or_else(|| ExecError::NumericFieldOverflow(format!("{context} overflow")))
}

// ---------------------------------------------------------------------------
// Arithmetic helpers
// ---------------------------------------------------------------------------

/// Add two numeric values, widening to Int64 or Float64 as appropriate.
///
/// The `Sum` and `Avg` accumulators store the running total as the
/// widened type (Int64 for integers, Float64 for floats) after the
/// first non-null input — but the *new* row arrives unwidened from
/// the child operator, so this helper must accept any mix of
/// narrower-on-the-right and widened-on-the-left integer and float
/// types. The output type is always the widened type to match.
pub(crate) fn add_values(a: Value, b: Value) -> Result<Value, ExecError> {
    match (a, b) {
        (
            Value::Decimal {
                value: x,
                scale: xs,
            },
            Value::Decimal {
                value: y,
                scale: ys,
            },
        ) => add_decimal_values(x, xs, y, ys),
        // Pure narrow-narrow promotions (first-step folding).
        (Value::Int16(x), Value::Int16(y)) => Ok(Value::Int64(i64::from(x) + i64::from(y))),
        (Value::Int32(x), Value::Int32(y)) => Ok(Value::Int64(i64::from(x) + i64::from(y))),
        (Value::Int64(x), Value::Int64(y)) => {
            checked_sum_i64(x, y, "HashAggregate SUM(BIGINT)").map(Value::Int64)
        }
        // Widened accumulator + narrower fresh row (the common case in
        // SUM / AVG once the accumulator has stepped through one input).
        (Value::Int64(x), Value::Int16(y)) | (Value::Int16(y), Value::Int64(x)) => {
            checked_sum_i64(x, i64::from(y), "HashAggregate SUM(INT)").map(Value::Int64)
        }
        (Value::Int64(x), Value::Int32(y)) | (Value::Int32(y), Value::Int64(x)) => {
            checked_sum_i64(x, i64::from(y), "HashAggregate SUM(INT)").map(Value::Int64)
        }
        (Value::Float32(x), Value::Float32(y)) => Ok(Value::Float64(f64::from(x) + f64::from(y))),
        (Value::Float64(x), Value::Float64(y)) => Ok(Value::Float64(x + y)),
        (Value::Float64(x), Value::Float32(y)) | (Value::Float32(y), Value::Float64(x)) => {
            Ok(Value::Float64(x + f64::from(y)))
        }
        (Value::Vector(x), Value::Vector(y)) => {
            add_dense_vector_values(x, y, "vector").map(Value::Vector)
        }
        (Value::HalfVec(x), Value::HalfVec(y)) => {
            add_dense_vector_values(x, y, "halfvec").map(Value::HalfVec)
        }
        (a, b) => Err(ExecError::TypeMismatch(format!(
            "sum type mismatch: {a:?} and {b:?}"
        ))),
    }
}

/// Divide a running sum by the count to produce an average.
pub(crate) fn divide_value(sum: Value, count: i64) -> Value {
    let count_f64 = i64_to_f64_saturating(count);
    match sum {
        Value::Int64(s) => Value::Float64(i64_to_f64_saturating(s) / count_f64),
        Value::Float64(s) => Value::Float64(s / count_f64),
        Value::Decimal { value, scale } => Value::Float64(decimal_to_f64(value, scale) / count_f64),
        Value::Vector(values) => Value::Vector(divide_dense_vector_values(values, count)),
        Value::HalfVec(values) => Value::HalfVec(divide_dense_vector_values(values, count)),
        other => other,
    }
}

fn add_decimal_values(
    left_value: i128,
    left_scale: i32,
    right_value: i128,
    right_scale: i32,
) -> Result<Value, ExecError> {
    let common_scale = left_scale.max(right_scale);
    let left = rescale_decimal_value(left_value, left_scale, common_scale)?;
    let right = rescale_decimal_value(right_value, right_scale, common_scale)?;
    let value = left
        .checked_add(right)
        .ok_or_else(|| ExecError::NumericFieldOverflow("decimal sum overflow".to_owned()))?;
    Ok(Value::Decimal {
        value,
        scale: common_scale,
    })
}

fn rescale_decimal_value(
    value: i128,
    current_scale: i32,
    target_scale: i32,
) -> Result<i128, ExecError> {
    let scale_delta = target_scale - current_scale;
    if scale_delta < 0 {
        return Err(ExecError::TypeMismatch(
            "decimal rescale underflow".to_owned(),
        ));
    }
    let factor = pow10_i128(
        u32::try_from(scale_delta)
            .map_err(|_| ExecError::NumericFieldOverflow("decimal rescale overflow".to_owned()))?,
    )
    .ok_or_else(|| ExecError::NumericFieldOverflow("decimal rescale overflow".to_owned()))?;
    value
        .checked_mul(factor)
        .ok_or_else(|| ExecError::NumericFieldOverflow("decimal rescale overflow".to_owned()))
}

fn pow10_i128(exp: u32) -> Option<i128> {
    (0..exp).try_fold(1_i128, |acc, _| acc.checked_mul(10))
}

pub(crate) fn decimal_to_f64(value: i128, scale: i32) -> f64 {
    let raw = i128_to_f64_saturating(value);
    raw / 10_f64.powi(scale)
}

fn i128_to_f64_saturating(value: i128) -> f64 {
    value.to_f64().unwrap_or_else(|| {
        if value.is_negative() {
            f64::MIN
        } else {
            f64::MAX
        }
    })
}

pub(crate) fn i64_to_f64_saturating(value: i64) -> f64 {
    value.to_f64().unwrap_or_else(|| {
        if value.is_negative() {
            f64::MIN
        } else {
            f64::MAX
        }
    })
}

/// Returns `true` if `a < b` under the natural total order.
pub(crate) fn value_lt(a: &Value, b: &Value) -> bool {
    use crate::sort::compare_values_nullable;
    matches!(
        compare_values_nullable(a, b, false),
        std::cmp::Ordering::Less
    )
}
