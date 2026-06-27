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
///
/// Integer and numeric sums divide **exactly** in `i128` decimal space and
/// yield a [`Value::Decimal`] (PostgreSQL `AVG(int)`/`AVG(numeric)` →
/// `numeric`). Float sums keep `f64` division (`AVG(float)` → `double
/// precision`). `count` is the caller-guaranteed non-zero group size.
pub(crate) fn divide_value(sum: Value, count: i64) -> Value {
    match sum {
        // Integer sum: an integer is a decimal with scale 0.
        Value::Int64(s) => {
            avg_decimal_division(i128::from(s), 0, count).unwrap_or_else(|| {
                // Overflow is unreachable in practice (the scaled numerator
                // fits i128 for any i64 sum at the chosen result scale); fall
                // back to the float path rather than panicking.
                Value::Float64(i64_to_f64_saturating(s) / i64_to_f64_saturating(count))
            })
        }
        Value::Decimal { value, scale } => avg_decimal_division(value, scale, count)
            .unwrap_or_else(|| {
                Value::Float64(decimal_to_f64(value, scale) / i64_to_f64_saturating(count))
            }),
        Value::Float64(s) => Value::Float64(s / i64_to_f64_saturating(count)),
        Value::Vector(values) => Value::Vector(divide_dense_vector_values(values, count)),
        Value::HalfVec(values) => Value::HalfVec(divide_dense_vector_values(values, count)),
        other => other,
    }
}

/// PostgreSQL `numeric` base, `NBASE = 10000`, four decimal digits per group.
const PG_DEC_DIGITS: i32 = 4;
/// PostgreSQL `NUMERIC_MIN_SIG_DIGITS`: at least this many significant
/// decimal digits in a division result.
const PG_NUMERIC_MIN_SIG_DIGITS: i32 = 16;
/// PostgreSQL `NUMERIC_MAX_DISPLAY_SCALE`: division result scale is clamped
/// to this upper bound.
const PG_NUMERIC_MAX_DISPLAY_SCALE: i32 = 1000;

/// PostgreSQL `select_div_scale`: the display scale of `dividend / divisor`,
/// where the dividend is `value(dividend_scale)` and the divisor is the
/// (integer) `count`. Reproduces `numeric.c`'s rule so `AVG` over
/// integer/numeric input renders with the same trailing precision as
/// PostgreSQL (value/weight-dependent: ~16 significant digits, fewer
/// fractional digits as the magnitude grows).
fn avg_div_result_scale(dividend_value: i128, dividend_scale: i32, count: i64) -> i32 {
    let (w1, fd1) = base10000_weight_and_first_digit(dividend_value, dividend_scale);
    // The divisor is the integer `count` (scale 0).
    let (w2, fd2) = base10000_weight_and_first_digit(i128::from(count), 0);
    let mut qweight = w1 - w2;
    if fd1 <= fd2 {
        qweight -= 1;
    }
    let mut rscale = PG_NUMERIC_MIN_SIG_DIGITS - qweight * PG_DEC_DIGITS;
    // `max(.., var1->dscale, var2->dscale, NUMERIC_MIN_DISPLAY_SCALE=0)`.
    rscale = rscale.max(dividend_scale).max(0);
    rscale.min(PG_NUMERIC_MAX_DISPLAY_SCALE)
}

/// Base-10000 weight (group index of the most-significant non-zero group)
/// and the value of that group, for `value / 10^scale`. Mirrors the
/// `weight`/`digits[0]` fields PostgreSQL's `select_div_scale` reads from a
/// `NumericVar`. A zero magnitude yields weight 0, first digit 0.
fn base10000_weight_and_first_digit(value: i128, scale: i32) -> (i32, i128) {
    // Work on the magnitude as i128 (|i128::MIN| fits in i128 only via the
    // unsigned abs, but for AVG sums i128::MIN is unreachable; saturate to be
    // safe).
    let magnitude = value.checked_abs().unwrap_or(i128::MAX);
    if magnitude == 0 {
        return (0, 0);
    }
    // Number of base-10 integer digits of `value / 10^scale` minus one is the
    // base-10 weight of the most-significant digit.
    let digits10 = count_base10_digits(magnitude);
    let weight10 = digits10 - 1 - scale;
    // Base-10000 weight = floor(weight10 / 4) (floor toward negative infinity).
    let weight = weight10.div_euclid(PG_DEC_DIGITS);
    // The most-significant base-10000 group = floor(magnitude / 10^(scale +
    // 4*weight)) mod 10000 (the value's leading four-decimal-digit group).
    let expo = scale + PG_DEC_DIGITS * weight;
    let first_digit = if expo >= 0 {
        pow10_i128(u32::try_from(expo).unwrap_or(u32::MAX)).map_or(0, |p| (magnitude / p) % 10_000)
    } else {
        pow10_i128(u32::try_from(-expo).unwrap_or(u32::MAX))
            .map_or(0, |p| magnitude.saturating_mul(p) % 10_000)
    };
    (weight, first_digit)
}

fn count_base10_digits(mut magnitude: i128) -> i32 {
    let mut digits = 1;
    while magnitude >= 10 {
        magnitude /= 10;
        digits += 1;
    }
    digits
}

/// Exact decimal division `value(scale) / count` at the PostgreSQL-compatible
/// `AVG` result scale, rounded half away from zero. Returns `None` on i128
/// overflow so the caller can fall back to the float path. `count` is
/// non-zero by construction. Shared with the cached-aggregate fast path in
/// [`crate::filter_sum_op`].
pub(crate) fn avg_decimal_division(value: i128, scale: i32, count: i64) -> Option<Value> {
    let result_scale = avg_div_result_scale(value, scale, count);
    // Scale the numerator up so the integer quotient already carries
    // `result_scale` fractional digits: numerator = value * 10^(result_scale
    // - scale), then divide by count.
    let scale_up = u32::try_from(result_scale.checked_sub(scale)?).ok()?;
    let factor = pow10_i128(scale_up)?;
    let numerator = value.checked_mul(factor)?;
    let denominator = i128::from(count);
    let mut quotient = numerator.checked_div(denominator)?;
    let remainder = numerator % denominator;
    if remainder != 0 {
        // Round half away from zero (half-up), matching the scalar decimal
        // division kernel in `eval::arithmetic`.
        let twice_remainder = remainder.checked_abs()?.checked_mul(2)?;
        let divisor = denominator.checked_abs()?;
        if twice_remainder >= divisor {
            let adjustment = if (numerator >= 0) == (denominator >= 0) {
                1
            } else {
                -1
            };
            quotient = quotient.checked_add(adjustment)?;
        }
    }
    Some(Value::Decimal {
        value: quotient,
        scale: result_scale,
    })
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

#[cfg(test)]
mod tests {
    use ultrasql_core::Value;

    use super::divide_value;

    fn dec(sum: Value, count: i64) -> (i128, i32) {
        match divide_value(sum, count) {
            Value::Decimal { value, scale } => (value, scale),
            other => panic!("expected Decimal, got {other:?}"),
        }
    }

    // AVG over integer/numeric input must be exact `numeric`, matching
    // PostgreSQL's value/weight-dependent division scale (`select_div_scale`).
    // Each expectation below was confirmed against PostgreSQL 14 via `psql`
    // (e.g. `SELECT avg(c) FROM (VALUES ...) v(c)`).

    #[test]
    fn avg_int_small_magnitude_is_scale_16() {
        // avg(1,2) = 1.5 -> 1.5000000000000000 (scale 16): value = 1.5 * 1e16.
        assert_eq!(dec(Value::Int64(3), 2), (15 * 10_i128.pow(15), 16));
        // avg(10,20,30) = 20 -> 20.0000000000000000.
        assert_eq!(dec(Value::Int64(60), 3), (20 * 10_i128.pow(16), 16));
    }

    #[test]
    fn avg_quotient_below_one_widens_to_scale_20() {
        // -2048/4096 = -0.5 -> -0.50000000000000000000 (scale 20 in PG).
        assert_eq!(dec(Value::Int64(-2048), 4096), (-5 * 10_i128.pow(19), 20));
    }

    #[test]
    fn avg_large_magnitude_shrinks_scale() {
        // 22500000000000000000 / 3 = 7500000000000000000 (scale 0 in PG).
        let sum = Value::Decimal {
            value: 22_500_000_000_000_000_000,
            scale: 0,
        };
        assert_eq!(dec(sum, 3), (7_500_000_000_000_000_000, 0));
    }

    #[test]
    fn avg_numeric_repeating_rounds_half_up() {
        // avg(1,1,1)/... use 10/3 = 3.3333... rounded at scale 16.
        // 10/3 = 3.3333333333333333 (scale 16, last digit rounded from 3...).
        let (value, scale) = dec(Value::Int64(10), 3);
        assert_eq!(scale, 16);
        // 3.3333333333333333
        assert_eq!(value, 33_333_333_333_333_333);
    }

    #[test]
    fn avg_float_stays_double_precision() {
        // AVG(float) must remain f64 (double precision), not numeric.
        match divide_value(Value::Float64(3.0), 2) {
            Value::Float64(v) => assert!((v - 1.5).abs() < f64::EPSILON),
            other => panic!("expected Float64, got {other:?}"),
        }
    }
}
