//! Canonical total ordering and hashing utilities for [`Value`].
//!
//! [`Value`] deliberately does not implement [`std::cmp::Ord`] or
//! [`std::hash::Hash`] because floating-point NaN semantics and the
//! `NULL` ordering contract are not universally agreed upon across
//! subsystems. This module provides the specific choices required by
//! the statistics layer:
//!
//! - **NULL** sorts *last* (consistent with PostgreSQL `NULLS LAST`).
//! - **Numeric cross-type comparisons** widen to `f64`; integers widen
//!   losslessly via [`i64::from`] before conversion to `f64`.
//! - **NaN** sorts last among floats.
//! - **Mixed-type comparisons** (e.g., Int32 vs Text) fall back to a
//!   stable discriminant ordering so the result is always a total
//!   order, even if not meaningful across types.
//!
//! These choices are *internal* to the statistics subsystem and must
//! not be exposed as a general `Ord` impl for `Value`.

use std::cmp::Ordering;

use ultrasql_core::Value;

/// Compare two [`Value`]s under the statistics layer's total ordering.
///
/// - `NULL` is greater than every non-NULL value (sorts last).
/// - Numeric values are compared by widening to `f64`.
/// - `Text` values are compared lexicographically.
/// - `Bytea` values are compared lexicographically.
/// - Mixed types fall back to discriminant ordering.
pub(super) fn compare_values(a: &Value, b: &Value) -> Ordering {
    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Greater,
        (_, Value::Null) => Ordering::Less,

        // Numeric: widen to f64 for comparison.
        (lhs, rhs) if both_numeric(lhs, rhs) => {
            let fa = to_f64(lhs);
            let fb = to_f64(rhs);
            // NaN sorts last.
            match (fa.is_nan(), fb.is_nan()) {
                (true, true) => Ordering::Equal,
                (true, false) => Ordering::Greater,
                (false, true) => Ordering::Less,
                (false, false) => fa.partial_cmp(&fb).unwrap_or(Ordering::Equal),
            }
        }

        (Value::Text(la), Value::Text(lb)) => la.cmp(lb),
        (Value::Jsonb(la), Value::Jsonb(lb)) => la.cmp(lb),
        (Value::Bytea(la), Value::Bytea(lb)) => la.cmp(lb),
        (Value::Bool(la), Value::Bool(lb)) => la.cmp(lb),
        (Value::Date(la), Value::Date(lb)) => la.cmp(lb),
        (Value::Time(la), Value::Time(lb))
        | (Value::Timestamp(la), Value::Timestamp(lb))
        | (Value::TimestampTz(la), Value::TimestampTz(lb)) => la.cmp(lb),
        (Value::Uuid(la), Value::Uuid(lb)) => la.cmp(lb),
        (Value::Range(la), Value::Range(lb)) if la.range_type == lb.range_type => {
            la.to_string().cmp(&lb.to_string())
        }
        (Value::Geometry(la), Value::Geometry(lb)) if la.geometry_type == lb.geometry_type => {
            la.to_string().cmp(&lb.to_string())
        }
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
        Value::Int16(i) => {
            let mut out = vec![2];
            out.extend_from_slice(&i.to_be_bytes());
            out
        }
        Value::Int32(i) => {
            let mut out = vec![3];
            out.extend_from_slice(&i.to_be_bytes());
            out
        }
        Value::Int64(i) => {
            let mut out = vec![4];
            out.extend_from_slice(&i.to_be_bytes());
            out
        }
        Value::Float32(f) => {
            // Normalize: canonicalize NaN to a single bit pattern;
            // use the bits directly for hashing.
            let bits = if f.is_nan() {
                f32::NAN.to_bits()
            } else {
                f.to_bits()
            };
            let mut out = vec![5];
            out.extend_from_slice(&bits.to_be_bytes());
            out
        }
        Value::Float64(f) => {
            let bits = if f.is_nan() {
                f64::NAN.to_bits()
            } else {
                f.to_bits()
            };
            let mut out = vec![6];
            out.extend_from_slice(&bits.to_be_bytes());
            out
        }
        Value::Text(s) => {
            let mut out = vec![7];
            out.extend_from_slice(s.as_bytes());
            out
        }
        Value::Jsonb(s) => {
            let mut out = vec![18];
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
        Value::Uuid(u) => {
            let mut out = vec![13];
            out.extend_from_slice(u);
            out
        }
        Value::Decimal { value, scale } => {
            let mut out = vec![14];
            out.extend_from_slice(&value.to_be_bytes());
            out.extend_from_slice(&scale.to_be_bytes());
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

fn to_f64(v: &Value) -> f64 {
    match v {
        Value::Int16(i) => f64::from(*i),
        Value::Int32(i) => f64::from(*i),
        Value::Int64(i) => *i as f64,
        Value::Float32(f) => f64::from(*f),
        Value::Float64(f) => *f,
        _ => f64::NAN,
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
        Value::Jsonb(_) => 18,
        Value::Bytea(_) => 8,
        Value::Timestamp(_) => 9,
        Value::TimestampTz(_) => 10,
        Value::Date(_) => 11,
        Value::Time(_) => 12,
        Value::Uuid(_) => 13,
        Value::Decimal { .. } => 14,
        Value::Interval { .. } => 15,
        Value::Range(_) => 16,
        Value::Geometry(_) => 17,
        Value::Array { .. } => 19,
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
    fn value_key_same_for_equal_values() {
        assert_eq!(value_key(&Value::Int32(42)), value_key(&Value::Int32(42)));
        assert_ne!(value_key(&Value::Int32(42)), value_key(&Value::Int32(43)));
    }
}
