//! Hash-map key wrappers for GROUP BY.
//!
//! [`KeyValue`] and [`GroupKey`] provide the `Hash + Eq` implementations
//! that let a sequence of [`Value`]s serve as a hash-map key with SQL
//! group-key semantics (bitwise float equality, scale-normalised decimal
//! equality, `bpchar` blank-padding, timezone-normalised `timetz`).

use std::hash::{Hash, Hasher};

use ultrasql_core::{Value, bpchar_semantic_text, timetz_utc_micros};

use crate::value_key::{decimal_values_equal, hash_decimal_key};

// ---------------------------------------------------------------------------
// Hash-map key wrapper
// ---------------------------------------------------------------------------

/// A wrapper around [`Value`] that implements `Hash + Eq` so it can serve
/// as a component of a hash-map key.
///
/// [`Value`] derives only `PartialEq` (not `Eq`) because `f32`/`f64` are not
/// `Eq`. We implement `Eq` manually: for floating-point values we use
/// bitwise equality (NaN == NaN in this context, consistent with join
/// semantics for floating-point GROUP BY keys).
#[derive(Debug)]
pub(crate) struct KeyValue(pub(crate) Value);

impl PartialEq for KeyValue {
    fn eq(&self, other: &Self) -> bool {
        match (&self.0, &other.0) {
            (Value::Float32(a), Value::Float32(b)) => a.to_bits() == b.to_bits(),
            (Value::Float64(a), Value::Float64(b)) => a.to_bits() == b.to_bits(),
            (Value::Vector(a), Value::Vector(b)) | (Value::HalfVec(a), Value::HalfVec(b)) => {
                a.len() == b.len() && a.iter().zip(b).all(|(l, r)| l.to_bits() == r.to_bits())
            }
            (
                Value::Decimal {
                    value: left_value,
                    scale: left_scale,
                },
                Value::Decimal {
                    value: right_value,
                    scale: right_scale,
                },
            ) => decimal_values_equal(*left_value, *left_scale, *right_value, *right_scale),
            _ => self.0 == other.0,
        }
    }
}

impl Eq for KeyValue {}

impl Hash for KeyValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        hash_value(&self.0, state);
    }
}

/// Hash a [`Value`] by discriminant + bit-pattern for floating-point types.
pub(crate) fn hash_value<H: Hasher>(v: &Value, state: &mut H) {
    match v {
        Value::Null => state.write_u8(0),
        Value::Bool(b) => {
            state.write_u8(1);
            b.hash(state);
        }
        Value::Int16(x) => {
            state.write_u8(2);
            x.hash(state);
        }
        Value::Int32(x) => {
            state.write_u8(3);
            x.hash(state);
        }
        Value::Int64(x) => {
            state.write_u8(4);
            x.hash(state);
        }
        Value::Money(x) => {
            state.write_u8(23);
            x.hash(state);
        }
        Value::Oid(x) => {
            state.write_u8(27);
            x.hash(state);
        }
        Value::RegClass(x) => {
            state.write_u8(28);
            x.hash(state);
        }
        Value::RegType(x) => {
            state.write_u8(29);
            x.hash(state);
        }
        Value::PgLsn(x) => {
            state.write_u8(30);
            x.hash(state);
        }
        Value::Float32(x) => {
            state.write_u8(5);
            x.to_bits().hash(state);
        }
        Value::Float64(x) => {
            state.write_u8(6);
            x.to_bits().hash(state);
        }
        Value::Text(s) => {
            state.write_u8(7);
            s.hash(state);
        }
        Value::Char(s) => {
            state.write_u8(24);
            bpchar_semantic_text(s).hash(state);
        }
        Value::Json(s) => {
            state.write_u8(16);
            s.hash(state);
        }
        Value::Jsonb(s) => {
            state.write_u8(17);
            s.hash(state);
        }
        Value::Xml(s) => {
            state.write_u8(31);
            s.hash(state);
        }
        Value::Bytea(b) => {
            state.write_u8(8);
            b.hash(state);
        }
        Value::Timestamp(x) | Value::TimestampTz(x) | Value::Time(x) => {
            state.write_u8(9);
            x.hash(state);
        }
        Value::TimeTz {
            micros,
            offset_seconds,
        } => {
            state.write_u8(9);
            timetz_utc_micros(*micros, *offset_seconds).hash(state);
        }
        Value::Date(x) => {
            state.write_u8(10);
            x.hash(state);
        }
        Value::Uuid(u) => {
            state.write_u8(11);
            u.hash(state);
        }
        Value::Decimal { value, scale } => {
            state.write_u8(12);
            hash_decimal_key(state, *value, *scale);
        }
        Value::Interval {
            months,
            days,
            microseconds,
        } => {
            state.write_u8(13);
            months.hash(state);
            days.hash(state);
            microseconds.hash(state);
        }
        Value::Range(v) => {
            state.write_u8(14);
            v.hash(state);
        }
        Value::Geometry(v) => {
            state.write_u8(15);
            v.hash(state);
        }
        Value::Array {
            element_type,
            elements,
        } => {
            state.write_u8(18);
            element_type.hash(state);
            elements.hash(state);
        }
        Value::Vector(values) | Value::HalfVec(values) => {
            state.write_u8(19);
            for value in values {
                value.to_bits().hash(state);
            }
        }
        Value::SparseVec(value) => {
            state.write_u8(20);
            value.hash(state);
        }
        Value::BitVec { dims, bytes } => {
            state.write_u8(21);
            dims.hash(state);
            bytes.hash(state);
        }
        Value::BitString(bits) => {
            state.write_u8(25);
            bits.hash(state);
        }
        Value::Network(network) => {
            state.write_u8(26);
            network.hash(state);
        }
        Value::Record(fields) => {
            state.write_u8(22);
            fields.hash(state);
        }
    }
}

/// A group key — a sequence of zero or more [`KeyValue`]s.
///
/// Uses a newtype wrapper so we can implement `Hash + Eq` for a
/// `Vec<Value>` without a coherence violation.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct GroupKey(Vec<KeyValue>);

impl Hash for GroupKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for kv in &self.0 {
            kv.hash(state);
        }
    }
}

impl GroupKey {
    pub(crate) fn from_values(values: Vec<Value>) -> Self {
        Self(values.into_iter().map(KeyValue).collect())
    }

    pub(crate) fn into_values(self) -> Vec<Value> {
        self.0.into_iter().map(|kv| kv.0).collect()
    }
}
