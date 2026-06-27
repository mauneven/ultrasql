//! Value comparison and 3VL equality helpers.
//!
//! Extracted verbatim from the original `eval.rs`; pure code motion.

use super::*;

// ---------------------------------------------------------------------------
// Comparison helpers
// ---------------------------------------------------------------------------

/// Compare two like-typed values and apply the `test` function to the
/// resulting `Ordering`.
pub(crate) fn value_compare(
    lv: &Value,
    rv: &Value,
    test: impl Fn(std::cmp::Ordering) -> bool,
) -> Result<Value, EvalError> {
    let ord = compare_values(lv, rv)?;
    Ok(Value::Bool(test(ord)))
}

pub(crate) fn value_eq(lv: &Value, rv: &Value) -> Result<Value, EvalError> {
    sql_eq_3vl(lv, rv).map(bool_or_null)
}

pub(crate) fn value_not_eq(lv: &Value, rv: &Value) -> Result<Value, EvalError> {
    sql_eq_3vl(lv, rv).map(|result| bool_or_null(result.map(|eq| !eq)))
}

pub(crate) fn bool_or_null(value: Option<bool>) -> Value {
    value.map_or(Value::Null, Value::Bool)
}

pub(crate) fn sql_eq_3vl(lv: &Value, rv: &Value) -> Result<Option<bool>, EvalError> {
    if matches!((lv, rv), (Value::Null, _) | (_, Value::Null)) {
        return Ok(None);
    }
    match (lv, rv) {
        (Value::Record(left), Value::Record(right)) => record_eq_3vl(left, right),
        (Value::Record(_), _) | (_, Value::Record(_)) => Err(EvalError::Type(format!(
            "record comparison type mismatch: {lv:?} and {rv:?}"
        ))),
        _ => compare_values(lv, rv).map(|ordering| Some(ordering == std::cmp::Ordering::Equal)),
    }
}

pub(crate) fn sql_is_distinct_from(lv: &Value, rv: &Value) -> Result<bool, EvalError> {
    match (lv, rv) {
        (Value::Null, Value::Null) => Ok(false),
        (Value::Null, _) | (_, Value::Null) => Ok(true),
        (Value::Record(left), Value::Record(right)) => record_is_distinct_from(left, right),
        (Value::Record(_), _) | (_, Value::Record(_)) => Err(EvalError::Type(format!(
            "record comparison type mismatch: {lv:?} and {rv:?}"
        ))),
        _ => compare_values(lv, rv).map(|ordering| ordering != std::cmp::Ordering::Equal),
    }
}

pub(crate) fn record_is_distinct_from(
    left: &[(String, Value)],
    right: &[(String, Value)],
) -> Result<bool, EvalError> {
    if left.len() != right.len() {
        return Err(EvalError::Type(format!(
            "record arity mismatch: {} and {}",
            left.len(),
            right.len()
        )));
    }
    for ((_, left_value), (_, right_value)) in left.iter().zip(right.iter()) {
        if sql_is_distinct_from(left_value, right_value)? {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(crate) fn record_eq_3vl(
    left: &[(String, Value)],
    right: &[(String, Value)],
) -> Result<Option<bool>, EvalError> {
    if left.len() != right.len() {
        return Err(EvalError::Type(format!(
            "record arity mismatch: {} and {}",
            left.len(),
            right.len()
        )));
    }

    let mut saw_unknown = false;
    for ((_, left_value), (_, right_value)) in left.iter().zip(right.iter()) {
        match sql_eq_3vl(left_value, right_value)? {
            Some(true) => {}
            Some(false) => return Ok(Some(false)),
            None => saw_unknown = true,
        }
    }

    Ok((!saw_unknown).then_some(true))
}

/// Total ordering for Value pairs of the same type.
///
/// Only types that have a natural total order are supported. Mismatched
/// types return [`EvalError::Type`].
pub(crate) fn compare_values(lv: &Value, rv: &Value) -> Result<std::cmp::Ordering, EvalError> {
    if let (Some(left), Some(right)) = (oid_alias_value(lv), oid_alias_value(rv)) {
        return Ok(left.cmp(&right));
    }
    if let Some(ordering) = compare_oid_alias_with_integer(lv, rv) {
        return Ok(ordering);
    }

    if matches!(
        (lv, rv),
        (Value::Decimal { .. }, _) | (_, Value::Decimal { .. })
    ) {
        let Some((left_value, left_scale)) = numeric_to_decimal(lv)? else {
            return Err(EvalError::Type(format!(
                "comparison type mismatch: {lv:?} and {rv:?}"
            )));
        };
        let Some((right_value, right_scale)) = numeric_to_decimal(rv)? else {
            return Err(EvalError::Type(format!(
                "comparison type mismatch: {lv:?} and {rv:?}"
            )));
        };
        return Ok(compare_decimal_values(
            left_value,
            left_scale,
            right_value,
            right_scale,
        ));
    }

    match (lv, rv) {
        (Value::Int16(l), Value::Int16(r)) => Ok(l.cmp(r)),
        (Value::Int32(l), Value::Int32(r)) => Ok(l.cmp(r)),
        (Value::Int64(l), Value::Int64(r)) => Ok(l.cmp(r)),
        (Value::Oid(l), Value::Oid(r))
        | (Value::RegClass(l), Value::RegClass(r))
        | (Value::RegType(l), Value::RegType(r)) => Ok(l.cmp(r)),
        (Value::PgLsn(l), Value::PgLsn(r)) => Ok(l.cmp(r)),
        (Value::Float32(l), Value::Float32(r)) => l
            .partial_cmp(r)
            .ok_or_else(|| EvalError::Type("comparison of NaN is undefined".to_owned())),
        (Value::Float64(l), Value::Float64(r)) => l
            .partial_cmp(r)
            .ok_or_else(|| EvalError::Type("comparison of NaN is undefined".to_owned())),
        (Value::Text(l), Value::Text(r)) => Ok(l.cmp(r)),
        (Value::Char(l), Value::Char(r)) => {
            Ok(bpchar_semantic_text(l).cmp(bpchar_semantic_text(r)))
        }
        (Value::Char(l), Value::Text(r)) => Ok(bpchar_semantic_text(l).cmp(r)),
        (Value::Text(l), Value::Char(r)) => Ok(l.as_str().cmp(bpchar_semantic_text(r))),
        (Value::BitString(l), Value::BitString(r)) => Ok(l.to_bit_text().cmp(&r.to_bit_text())),
        (Value::Network(l), Value::Network(r)) => (*l)
            .cmp_network(*r)
            .ok_or_else(|| EvalError::Type("network comparison type mismatch".to_owned())),
        (Value::Bool(l), Value::Bool(r)) => Ok(l.cmp(r)),
        (Value::Range(l), Value::Range(r)) if l.range_type == r.range_type => {
            if l == r {
                Ok(std::cmp::Ordering::Equal)
            } else {
                Ok(l.to_string().cmp(&r.to_string()))
            }
        }
        (Value::Geometry(l), Value::Geometry(r)) if l.geometry_type == r.geometry_type => {
            if l == r {
                Ok(std::cmp::Ordering::Equal)
            } else {
                Ok(l.to_string().cmp(&r.to_string()))
            }
        }
        (
            Value::Decimal {
                value: lv,
                scale: ls,
            },
            Value::Decimal {
                value: rv,
                scale: rs,
            },
        ) => Ok(compare_decimal_values(*lv, *ls, *rv, *rs)),
        (Value::Date(l), Value::Date(r)) => Ok(l.cmp(r)),
        (Value::Time(l), Value::Time(r)) => Ok(l.cmp(r)),
        (
            Value::TimeTz {
                micros: lm,
                offset_seconds: lo,
            },
            Value::TimeTz {
                micros: rm,
                offset_seconds: ro,
            },
        ) => Ok(timetz_utc_micros(*lm, *lo).cmp(&timetz_utc_micros(*rm, *ro))),
        (Value::Timestamp(l), Value::Timestamp(r))
        | (Value::TimestampTz(l), Value::TimestampTz(r))
        | (Value::Timestamp(l), Value::TimestampTz(r))
        | (Value::TimestampTz(l), Value::Timestamp(r)) => Ok(l.cmp(r)),
        (Value::Date(l), Value::Timestamp(r)) | (Value::Date(l), Value::TimestampTz(r)) => {
            Ok(date_as_timestamp(*l)?.cmp(r))
        }
        (Value::Timestamp(l), Value::Date(r)) | (Value::TimestampTz(l), Value::Date(r)) => {
            Ok(l.cmp(&date_as_timestamp(*r)?))
        }
        (
            Value::Interval {
                months: lm,
                days: ld,
                microseconds: lus,
            },
            Value::Interval {
                months: rm,
                days: rd,
                microseconds: rus,
            },
        ) => Ok((lm, ld, lus).cmp(&(rm, rd, rus))),
        // Mixed integer/float comparison: PostgreSQL compares numerically by
        // promoting the integer side to f64 (int -> float8 is the preferred
        // implicit cast). Mirrors the float NaN handling above.
        (
            Value::Int16(_) | Value::Int32(_) | Value::Int64(_),
            Value::Float32(_) | Value::Float64(_),
        )
        | (
            Value::Float32(_) | Value::Float64(_),
            Value::Int16(_) | Value::Int32(_) | Value::Int64(_),
        ) => {
            // SAFETY: both arms are guaranteed convertible by the match guard.
            let (Some(left), Some(right)) = (as_f64_for_cmp(lv), as_f64_for_cmp(rv)) else {
                return Err(EvalError::Type(format!(
                    "comparison type mismatch: {lv:?} and {rv:?}"
                )));
            };
            left.partial_cmp(&right)
                .ok_or_else(|| EvalError::Type("comparison of NaN is undefined".to_owned()))
        }
        (l, r) => Err(EvalError::Type(format!(
            "comparison type mismatch: {l:?} and {r:?}"
        ))),
    }
}

/// Promote an integer or float [`Value`] to `f64` for cross-type numeric
/// comparison. Returns `None` for non-int/float values.
pub(crate) fn as_f64_for_cmp(value: &Value) -> Option<f64> {
    match value {
        Value::Int16(v) => Some(f64::from(*v)),
        Value::Int32(v) => Some(f64::from(*v)),
        #[allow(clippy::cast_precision_loss)]
        Value::Int64(v) => Some(*v as f64),
        Value::Float32(v) => Some(f64::from(*v)),
        Value::Float64(v) => Some(*v),
        _ => None,
    }
}

pub(crate) fn oid_alias_value(value: &Value) -> Option<Oid> {
    match value {
        Value::Oid(oid) | Value::RegClass(oid) | Value::RegType(oid) => Some(*oid),
        _ => None,
    }
}

pub(crate) fn compare_oid_alias_with_integer(lv: &Value, rv: &Value) -> Option<std::cmp::Ordering> {
    if let (Some(left), Some(right)) = (oid_alias_value(lv), integer_value_i128(rv)) {
        return Some(i128::from(left.raw()).cmp(&right));
    }
    if let (Some(left), Some(right)) = (integer_value_i128(lv), oid_alias_value(rv)) {
        return Some(left.cmp(&i128::from(right.raw())));
    }
    None
}

pub(crate) fn integer_value_i128(value: &Value) -> Option<i128> {
    match value {
        Value::Int16(v) => Some(i128::from(*v)),
        Value::Int32(v) => Some(i128::from(*v)),
        Value::Int64(v) => Some(i128::from(*v)),
        _ => None,
    }
}

pub(crate) fn oid_or_integer_arg(value: &Value) -> Option<u32> {
    if let Some(oid) = oid_alias_value(value) {
        return Some(oid.raw());
    }
    match value {
        Value::Int16(v) => u32::try_from(i64::from(*v)).ok(),
        Value::Int32(v) => u32::try_from(i64::from(*v)).ok(),
        Value::Int64(v) => u32::try_from(*v).ok(),
        _ => None,
    }
}

pub(crate) fn date_as_timestamp(days_since_2000_01_01: i32) -> Result<i64, EvalError> {
    i64::from(days_since_2000_01_01)
        .checked_mul(MICROS_PER_DAY)
        .ok_or_else(|| EvalError::Type("date timestamp overflow".to_owned()))
}

pub(crate) fn rescale_decimal_value(
    value: i128,
    current_scale: i32,
    target_scale: i32,
) -> Result<i128, EvalError> {
    let scale_delta = target_scale - current_scale;
    if scale_delta < 0 {
        return Err(EvalError::Type("decimal rescale underflow".to_owned()));
    }
    let factor = pow10_i128(u32::try_from(scale_delta).map_err(|_| EvalError::Overflow)?)
        .ok_or(EvalError::Overflow)?;
    value.checked_mul(factor).ok_or(EvalError::Overflow)
}

pub(crate) fn compare_decimal_values(
    left_value: i128,
    left_scale: i32,
    right_value: i128,
    right_scale: i32,
) -> std::cmp::Ordering {
    match (left_value.cmp(&0), right_value.cmp(&0)) {
        (std::cmp::Ordering::Equal, std::cmp::Ordering::Equal) => {
            return std::cmp::Ordering::Equal;
        }
        (std::cmp::Ordering::Equal, std::cmp::Ordering::Less)
        | (std::cmp::Ordering::Greater, std::cmp::Ordering::Less) => {
            return std::cmp::Ordering::Greater;
        }
        (std::cmp::Ordering::Less, std::cmp::Ordering::Equal)
        | (std::cmp::Ordering::Less, std::cmp::Ordering::Greater) => {
            return std::cmp::Ordering::Less;
        }
        _ => {}
    }

    let left = DecimalMagnitude::new(left_value, left_scale);
    let right = DecimalMagnitude::new(right_value, right_scale);
    let magnitude_order = left.cmp_abs(&right);
    if left.negative {
        magnitude_order.reverse()
    } else {
        magnitude_order
    }
}

#[derive(Debug)]
pub(crate) struct DecimalMagnitude {
    negative: bool,
    digits: String,
    integer_digits: i64,
}

impl DecimalMagnitude {
    fn new(value: i128, scale: i32) -> Self {
        let negative = value < 0;
        let mut magnitude = value.unsigned_abs();
        let mut scale = i64::from(scale);
        while magnitude != 0 && magnitude % 10 == 0 {
            magnitude /= 10;
            scale = scale.saturating_sub(1);
        }
        let digits = magnitude.to_string();
        let digit_count = i64::try_from(digits.len()).unwrap_or(i64::MAX);
        Self {
            negative,
            digits,
            integer_digits: digit_count.saturating_sub(scale),
        }
    }

    fn cmp_abs(&self, other: &Self) -> std::cmp::Ordering {
        match self.integer_digits.cmp(&other.integer_digits) {
            std::cmp::Ordering::Equal => {}
            non_equal => return non_equal,
        }

        let max_len = self.digits.len().max(other.digits.len());
        let left = self.digits.as_bytes();
        let right = other.digits.as_bytes();
        for idx in 0..max_len {
            let l = left.get(idx).copied().unwrap_or(b'0');
            let r = right.get(idx).copied().unwrap_or(b'0');
            match l.cmp(&r) {
                std::cmp::Ordering::Equal => {}
                non_equal => return non_equal,
            }
        }
        std::cmp::Ordering::Equal
    }
}

pub(crate) fn pow10_i128(exp: u32) -> Option<i128> {
    (0..exp).try_fold(1_i128, |acc, _| acc.checked_mul(10))
}
