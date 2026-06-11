//! Canonical total ordering and hashing utilities for [`Value`].
//!
//! [`Value`] deliberately does not implement [`std::cmp::Ord`] or
//! [`std::hash::Hash`] because floating-point NaN semantics and the
//! `NULL` ordering contract are not universally agreed upon across
//! subsystems. This module provides the specific choices required by
//! the statistics layer:
//!
//! - **NULL** sorts *last* (consistent with PostgreSQL `NULLS LAST`).
//! - **Numeric cross-type comparisons** compare integers exactly and
//!   compare integer/float pairs without losing integer precision.
//!   Decimal values compare exactly against other decimals.
//! - **NaN** sorts last among floats.
//! - **Mixed-type comparisons** (e.g., Int32 vs Text) fall back to a
//!   stable discriminant ordering so the result is always a total
//!   order, even if not meaningful across types.
//!
//! These choices are *internal* to the statistics subsystem and must
//! not be exposed as a general `Ord` impl for `Value`.

use std::cmp::Ordering;

use num_traits::ToPrimitive;
use ultrasql_core::{Value, bpchar_semantic_text, timetz_utc_micros};

/// Compare two [`Value`]s under the statistics layer's total ordering.
///
/// - `NULL` is greater than every non-NULL value (sorts last).
/// - Numeric values compare with exact integer ordering and precision-aware
///   integer/float ordering; decimal pairs compare with exact scale-aware
///   ordering.
/// - `Text` values are compared lexicographically.
/// - `Bytea` values are compared lexicographically.
/// - Mixed types fall back to discriminant ordering.
pub(super) fn compare_values(a: &Value, b: &Value) -> Ordering {
    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Greater,
        (_, Value::Null) => Ordering::Less,

        // Numeric: widen to f64 for comparison.
        (
            Value::Decimal {
                value: left_value,
                scale: left_scale,
            },
            Value::Decimal {
                value: right_value,
                scale: right_scale,
            },
        ) => compare_decimals(*left_value, *left_scale, *right_value, *right_scale),
        (lhs, rhs) if both_numeric(lhs, rhs) => compare_numeric_values(lhs, rhs),

        (Value::Text(la), Value::Text(lb)) => la.cmp(lb),
        (Value::Char(la), Value::Char(lb)) => {
            bpchar_semantic_text(la).cmp(bpchar_semantic_text(lb))
        }
        (Value::Char(la), Value::Text(lb)) => bpchar_semantic_text(la).cmp(lb),
        (Value::Text(la), Value::Char(lb)) => la.as_str().cmp(bpchar_semantic_text(lb)),
        (Value::Json(la), Value::Json(lb))
        | (Value::Jsonb(la), Value::Jsonb(lb))
        | (Value::Xml(la), Value::Xml(lb)) => la.cmp(lb),
        (Value::BitString(la), Value::BitString(lb)) => la.to_bit_text().cmp(&lb.to_bit_text()),
        (Value::Bytea(la), Value::Bytea(lb)) => la.cmp(lb),
        (Value::Bool(la), Value::Bool(lb)) => la.cmp(lb),
        (Value::Money(la), Value::Money(lb)) => la.cmp(lb),
        (Value::Oid(la), Value::Oid(lb))
        | (Value::RegClass(la), Value::RegClass(lb))
        | (Value::RegType(la), Value::RegType(lb)) => la.cmp(lb),
        (Value::PgLsn(la), Value::PgLsn(lb)) => la.cmp(lb),
        (Value::Date(la), Value::Date(lb)) => la.cmp(lb),
        (Value::Time(la), Value::Time(lb))
        | (Value::Timestamp(la), Value::Timestamp(lb))
        | (Value::TimestampTz(la), Value::TimestampTz(lb)) => la.cmp(lb),
        (
            Value::TimeTz {
                micros: la,
                offset_seconds: lo,
            },
            Value::TimeTz {
                micros: lb,
                offset_seconds: ro,
            },
        ) => timetz_utc_micros(*la, *lo).cmp(&timetz_utc_micros(*lb, *ro)),
        (Value::Uuid(la), Value::Uuid(lb)) => la.cmp(lb),
        (Value::Network(la), Value::Network(lb)) => (*la)
            .cmp_network(*lb)
            .unwrap_or_else(|| la.to_string().cmp(&lb.to_string())),
        (Value::Range(la), Value::Range(lb)) if la.range_type == lb.range_type => {
            la.to_string().cmp(&lb.to_string())
        }
        (Value::Geometry(la), Value::Geometry(lb)) if la.geometry_type == lb.geometry_type => {
            la.to_string().cmp(&lb.to_string())
        }
        (Value::Vector(la), Value::Vector(lb)) | (Value::HalfVec(la), Value::HalfVec(lb)) => {
            compare_f32_slices(la, lb)
        }
        (Value::SparseVec(la), Value::SparseVec(lb)) => la.to_string().cmp(&lb.to_string()),
        (
            Value::BitVec {
                dims: l_dims,
                bytes: l_bytes,
            },
            Value::BitVec {
                dims: r_dims,
                bytes: r_bytes,
            },
        ) => l_dims.cmp(r_dims).then_with(|| l_bytes.cmp(r_bytes)),
        (
            Value::Array {
                element_type: l_ty,
                elements: l_vals,
            },
            Value::Array {
                element_type: r_ty,
                elements: r_vals,
            },
        ) if l_ty == r_ty => compare_value_slices(l_vals, r_vals),

        // Cross-type fallback: order by discriminant.
        _ => discriminant(a).cmp(&discriminant(b)),
    }
}

/// Produce a canonical byte key for a [`Value`] suitable for use as a
/// `HashMap` / `HashSet` key within the statistics module.
///
/// Two values that are considered equal under [`compare_values`] produce
/// the same key. The encoding is not stable across engine versions and
/// must not be persisted.
pub(super) fn value_key(v: &Value) -> Vec<u8> {
    match v {
        Value::Null => vec![0],
        Value::Bool(b) => vec![1, u8::from(*b)],
        Value::Int16(_)
        | Value::Int32(_)
        | Value::Int64(_)
        | Value::Float32(_)
        | Value::Float64(_) => numeric_key(v),
        Value::Oid(oid) => {
            let mut out = vec![30];
            out.extend_from_slice(&oid.raw().to_be_bytes());
            out
        }
        Value::RegClass(oid) => {
            let mut out = vec![31];
            out.extend_from_slice(&oid.raw().to_be_bytes());
            out
        }
        Value::RegType(oid) => {
            let mut out = vec![32];
            out.extend_from_slice(&oid.raw().to_be_bytes());
            out
        }
        Value::PgLsn(lsn) => {
            let mut out = vec![33];
            out.extend_from_slice(&lsn.raw().to_be_bytes());
            out
        }
        Value::Text(s) => {
            let mut out = vec![7];
            out.extend_from_slice(s.as_bytes());
            out
        }
        Value::Char(s) => {
            let mut out = vec![26];
            out.extend_from_slice(bpchar_semantic_text(s).as_bytes());
            out
        }
        Value::Json(s) => {
            let mut out = vec![17];
            out.extend_from_slice(s.as_bytes());
            out
        }
        Value::Jsonb(s) => {
            let mut out = vec![18];
            out.extend_from_slice(s.as_bytes());
            out
        }
        Value::Xml(s) => {
            let mut out = vec![34];
            out.extend_from_slice(s.as_bytes());
            out
        }
        Value::Bytea(b) => {
            let mut out = vec![8];
            out.extend_from_slice(b);
            out
        }
        Value::Timestamp(t) => {
            let mut out = vec![9];
            out.extend_from_slice(&t.to_be_bytes());
            out
        }
        Value::TimestampTz(t) => {
            let mut out = vec![10];
            out.extend_from_slice(&t.to_be_bytes());
            out
        }
        Value::Date(d) => {
            let mut out = vec![11];
            out.extend_from_slice(&d.to_be_bytes());
            out
        }
        Value::Time(t) => {
            let mut out = vec![12];
            out.extend_from_slice(&t.to_be_bytes());
            out
        }
        Value::TimeTz {
            micros,
            offset_seconds,
        } => {
            let mut out = vec![27];
            out.extend_from_slice(&timetz_utc_micros(*micros, *offset_seconds).to_be_bytes());
            out
        }
        Value::Uuid(u) => {
            let mut out = vec![13];
            out.extend_from_slice(u);
            out
        }
        Value::Decimal { value, scale } => {
            let normalized = DecimalMagnitude::new(*value, *scale);
            let mut out = vec![14];
            out.push(u8::from(normalized.negative));
            out.extend_from_slice(&normalized.scale.to_be_bytes());
            out.extend_from_slice(normalized.digits.as_bytes());
            out
        }
        Value::Money(cents) => {
            let mut out = vec![25];
            out.extend_from_slice(&cents.to_be_bytes());
            out
        }
        Value::Interval {
            months,
            days,
            microseconds,
        } => {
            let mut out = vec![15];
            out.extend_from_slice(&months.to_be_bytes());
            out.extend_from_slice(&days.to_be_bytes());
            out.extend_from_slice(&microseconds.to_be_bytes());
            out
        }
        Value::Range(v) => {
            let mut out = vec![16];
            out.extend_from_slice(v.to_string().as_bytes());
            out
        }
        Value::Geometry(v) => {
            let mut out = vec![17];
            out.extend_from_slice(v.to_string().as_bytes());
            out
        }
        Value::Array {
            element_type,
            elements,
        } => {
            let mut out = vec![19];
            out.extend_from_slice(element_type.to_string().as_bytes());
            out.push(0);
            for element in elements {
                out.extend_from_slice(&value_key(element));
                out.push(0);
            }
            out
        }
        Value::Vector(values) | Value::HalfVec(values) => {
            let mut out = vec![discriminant(v)];
            for value in values {
                let bits = if value.is_nan() {
                    f32::NAN.to_bits()
                } else {
                    value.to_bits()
                };
                out.extend_from_slice(&bits.to_be_bytes());
            }
            out
        }
        Value::SparseVec(value) => {
            let mut out = vec![22];
            out.extend_from_slice(value.to_string().as_bytes());
            out
        }
        Value::BitVec { dims, bytes } => {
            let mut out = vec![23];
            out.extend_from_slice(&dims.to_be_bytes());
            out.extend_from_slice(bytes);
            out
        }
        Value::BitString(bits) => {
            let mut out = vec![28];
            out.extend_from_slice(&bits.len().to_be_bytes());
            out.extend_from_slice(bits.bytes());
            out
        }
        Value::Network(network) => {
            let mut out = vec![29];
            out.extend_from_slice(network.to_string().as_bytes());
            out
        }
        Value::Record(fields) => {
            let mut out = vec![24];
            for (name, value) in fields {
                out.extend_from_slice(name.as_bytes());
                out.push(0);
                out.extend_from_slice(&value_key(value));
                out.push(0);
            }
            out
        }
    }
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

const fn both_numeric(a: &Value, b: &Value) -> bool {
    is_numeric(a) && is_numeric(b)
}

const fn is_numeric(v: &Value) -> bool {
    matches!(
        v,
        Value::Int16(_) | Value::Int32(_) | Value::Int64(_) | Value::Float32(_) | Value::Float64(_)
    )
}

fn compare_numeric_values(lhs: &Value, rhs: &Value) -> Ordering {
    if let (Some(left), Some(right)) = (as_i64(lhs), as_i64(rhs)) {
        return left.cmp(&right);
    }

    if let (Some(left), Some(right)) = (as_i64(lhs), as_f64(rhs)) {
        return compare_i64_to_f64(left, right);
    }

    if let (Some(left), Some(right)) = (as_f64(lhs), as_i64(rhs)) {
        return compare_i64_to_f64(right, left).reverse();
    }

    compare_f64_values(to_f64(lhs), to_f64(rhs))
}

fn as_i64(v: &Value) -> Option<i64> {
    match v {
        Value::Int16(i) => Some(i64::from(*i)),
        Value::Int32(i) => Some(i64::from(*i)),
        Value::Int64(i) => Some(*i),
        _ => None,
    }
}

fn as_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Float32(f) => Some(f64::from(*f)),
        Value::Float64(f) => Some(*f),
        _ => None,
    }
}

fn to_f64(v: &Value) -> f64 {
    match v {
        Value::Int16(i) => f64::from(*i),
        Value::Int32(i) => f64::from(*i),
        Value::Int64(i) => i64_to_f64_saturating(*i),
        Value::Float32(f) => f64::from(*f),
        Value::Float64(f) => *f,
        _ => f64::NAN,
    }
}

fn i64_to_f64_saturating(value: i64) -> f64 {
    value.to_f64().unwrap_or_else(|| {
        if value.is_negative() {
            f64::MIN
        } else {
            f64::MAX
        }
    })
}

fn compare_i64_to_f64(integer: i64, float: f64) -> Ordering {
    if float.is_nan() {
        return Ordering::Less;
    }

    let Some(truncated) = float.to_i64() else {
        return if float.is_sign_negative() {
            Ordering::Greater
        } else {
            Ordering::Less
        };
    };

    match integer.cmp(&truncated) {
        Ordering::Equal => match float.fract().partial_cmp(&0.0).unwrap_or(Ordering::Equal) {
            Ordering::Greater => Ordering::Less,
            Ordering::Less => Ordering::Greater,
            Ordering::Equal => Ordering::Equal,
        },
        non_equal => non_equal,
    }
}

fn compare_f64_values(left: f64, right: f64) -> Ordering {
    match (left.is_nan(), right.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        (false, false) => left.partial_cmp(&right).unwrap_or(Ordering::Equal),
    }
}

fn numeric_key(v: &Value) -> Vec<u8> {
    let mut out = vec![2];
    if let Some(integer) = numeric_integral_i64(v) {
        out.push(0);
        out.extend_from_slice(&integer.to_be_bytes());
        return out;
    }

    let float = to_f64(v);
    if float.is_nan() {
        out.push(2);
        return out;
    }

    out.push(1);
    let bits = if float == 0.0 {
        0.0f64.to_bits()
    } else {
        float.to_bits()
    };
    out.extend_from_slice(&bits.to_be_bytes());
    out
}

fn numeric_integral_i64(v: &Value) -> Option<i64> {
    if let Some(integer) = as_i64(v) {
        return Some(integer);
    }

    let float = as_f64(v)?;
    if !float.is_finite() || float.fract() != 0.0 {
        return None;
    }
    float.to_i64()
}

fn compare_decimals(l: i64, l_scale: i32, r: i64, r_scale: i32) -> Ordering {
    match (l.cmp(&0), r.cmp(&0)) {
        (Ordering::Equal, Ordering::Equal) => return Ordering::Equal,
        (Ordering::Equal, Ordering::Less) | (Ordering::Greater, Ordering::Less) => {
            return Ordering::Greater;
        }
        (Ordering::Less, Ordering::Equal) | (Ordering::Less, Ordering::Greater) => {
            return Ordering::Less;
        }
        _ => {}
    }

    let left = DecimalMagnitude::new(l, l_scale);
    let right = DecimalMagnitude::new(r, r_scale);
    let magnitude_order = left.cmp_abs(&right);
    if left.negative {
        magnitude_order.reverse()
    } else {
        magnitude_order
    }
}

#[derive(Debug)]
struct DecimalMagnitude {
    negative: bool,
    digits: String,
    scale: i64,
    integer_digits: i64,
}

impl DecimalMagnitude {
    fn new(value: i64, scale: i32) -> Self {
        if value == 0 {
            return Self {
                negative: false,
                digits: "0".to_owned(),
                scale: 0,
                integer_digits: 1,
            };
        }

        let mut magnitude = i128::from(value);
        let negative = magnitude < 0;
        if negative {
            magnitude = -magnitude;
        }

        let mut scale = i64::from(scale);
        while magnitude % 10 == 0 {
            magnitude /= 10;
            scale = scale.saturating_sub(1);
        }

        let digits = magnitude.to_string();
        let digit_count = i64::try_from(digits.len()).unwrap_or(i64::MAX);
        Self {
            negative,
            digits,
            scale,
            integer_digits: digit_count.saturating_sub(scale),
        }
    }

    fn cmp_abs(&self, other: &Self) -> Ordering {
        match self.integer_digits.cmp(&other.integer_digits) {
            Ordering::Equal => {}
            non_equal => return non_equal,
        }

        let max_len = self.digits.len().max(other.digits.len());
        let left = self.digits.as_bytes();
        let right = other.digits.as_bytes();
        for idx in 0..max_len {
            let l = left.get(idx).copied().unwrap_or(b'0');
            let r = right.get(idx).copied().unwrap_or(b'0');
            match l.cmp(&r) {
                Ordering::Equal => {}
                non_equal => return non_equal,
            }
        }
        Ordering::Equal
    }
}

fn compare_value_slices(a: &[Value], b: &[Value]) -> Ordering {
    for (left, right) in a.iter().zip(b) {
        let ordering = compare_values(left, right);
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    a.len().cmp(&b.len())
}

fn compare_f32_slices(a: &[f32], b: &[f32]) -> Ordering {
    for (left, right) in a.iter().zip(b) {
        let ordering = match (left.is_nan(), right.is_nan()) {
            (true, true) => Ordering::Equal,
            (true, false) => Ordering::Greater,
            (false, true) => Ordering::Less,
            (false, false) => left.partial_cmp(right).unwrap_or(Ordering::Equal),
        };
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    a.len().cmp(&b.len())
}

const fn discriminant(v: &Value) -> u8 {
    match v {
        Value::Null => 0,
        Value::Bool(_) => 1,
        Value::Int16(_) => 2,
        Value::Int32(_) => 3,
        Value::Int64(_) => 4,
        Value::Float32(_) => 5,
        Value::Float64(_) => 6,
        Value::Text(_) => 7,
        Value::Char(_) => 26,
        Value::Json(_) => 17,
        Value::Jsonb(_) => 18,
        Value::Xml(_) => 34,
        Value::Bytea(_) => 8,
        Value::Timestamp(_) => 9,
        Value::TimestampTz(_) => 10,
        Value::Date(_) => 11,
        Value::Time(_) => 12,
        Value::TimeTz { .. } => 27,
        Value::Uuid(_) => 13,
        Value::Decimal { .. } => 14,
        Value::Interval { .. } => 15,
        Value::Range(_) => 16,
        Value::Geometry(_) => 17,
        Value::Array { .. } => 19,
        Value::Vector(_) => 20,
        Value::HalfVec(_) => 21,
        Value::SparseVec(_) => 22,
        Value::BitVec { .. } => 23,
        Value::Record(_) => 24,
        Value::Money(_) => 25,
        Value::BitString(_) => 28,
        Value::Network(_) => 29,
        Value::Oid(_) => 30,
        Value::RegClass(_) => 31,
        Value::RegType(_) => 32,
        Value::PgLsn(_) => 33,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn null_sorts_last() {
        assert_eq!(
            compare_values(&Value::Null, &Value::Int32(0)),
            Ordering::Greater
        );
        assert_eq!(
            compare_values(&Value::Int32(0), &Value::Null),
            Ordering::Less
        );
        assert_eq!(compare_values(&Value::Null, &Value::Null), Ordering::Equal);
    }

    #[test]
    fn numeric_cross_type_comparison() {
        assert_eq!(
            compare_values(&Value::Int32(5), &Value::Float64(5.0)),
            Ordering::Equal
        );
        assert_eq!(
            compare_values(&Value::Int32(4), &Value::Float64(5.0)),
            Ordering::Less
        );
    }

    #[test]
    fn int64_values_compare_exactly_beyond_f64_precision() {
        assert_eq!(
            compare_values(&Value::Int64(i64::MAX - 1), &Value::Int64(i64::MAX)),
            Ordering::Less
        );
        assert_eq!(
            compare_values(&Value::Int64(i64::MAX), &Value::Int64(i64::MAX - 1)),
            Ordering::Greater
        );
    }

    #[test]
    fn integer_float_comparison_preserves_i64_precision() {
        const ABOVE_F64_EXACT_RANGE: i64 = 9_007_199_254_740_993;
        let rounded_float = Value::Float64(9_007_199_254_740_992.0);
        let exact_int = Value::Int64(ABOVE_F64_EXACT_RANGE);

        assert_eq!(
            compare_values(&exact_int, &rounded_float),
            Ordering::Greater
        );
        assert_eq!(compare_values(&rounded_float, &exact_int), Ordering::Less);
        assert_ne!(value_key(&exact_int), value_key(&rounded_float));
    }

    #[test]
    fn value_key_matches_numeric_cross_type_equality() {
        assert_eq!(
            value_key(&Value::Int16(0)),
            value_key(&Value::Float64(-0.0))
        );
        assert_eq!(value_key(&Value::Int32(5)), value_key(&Value::Float32(5.0)));
        assert_eq!(value_key(&Value::Int64(5)), value_key(&Value::Float64(5.0)));
        assert_eq!(
            value_key(&Value::Float32(f32::NAN)),
            value_key(&Value::Float64(f64::NAN))
        );
    }

    #[test]
    fn decimal_values_compare_by_numeric_magnitude() {
        assert_eq!(
            compare_values(
                &Value::Decimal { value: 1, scale: 0 },
                &Value::Decimal { value: 2, scale: 0 },
            ),
            Ordering::Less
        );
        assert_eq!(
            compare_values(
                &Value::Decimal {
                    value: 10,
                    scale: 1,
                },
                &Value::Decimal { value: 1, scale: 0 },
            ),
            Ordering::Equal
        );
    }

    #[test]
    fn value_key_same_for_equal_values() {
        assert_eq!(value_key(&Value::Int32(42)), value_key(&Value::Int32(42)));
        assert_ne!(value_key(&Value::Int32(42)), value_key(&Value::Int32(43)));
        assert_eq!(
            value_key(&Value::Decimal {
                value: 10,
                scale: 1,
            }),
            value_key(&Value::Decimal { value: 1, scale: 0 })
        );
    }
}
