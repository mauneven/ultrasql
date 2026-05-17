//! Runtime scalar value representation.
//!
//! `Datum` is the tagged in-memory representation of a single scalar. It
//! is used everywhere a value crosses an executor boundary at row level;
//! column-oriented batch storage uses the dedicated layouts in
//! `ultrasql-vec`.
//!
//! The variants are deliberately *not* zero-cost — each access pays for
//! a discriminant check. That is the right tradeoff for OLTP paths
//! (tuple-at-a-time, type known per row, branch predictor is happy)
//! while OLAP paths bypass this representation entirely.

use std::fmt;
use std::hash::{Hash, Hasher};

use crate::types::DataType;

/// Scalar value used at the row-at-a-time executor boundary.
///
/// `Value` is the runtime, type-erased representation. `Datum` is an
/// alias retained for naming consistency with the literature.
///
/// ## `Hash` / `Eq` semantics for floating-point variants
///
/// SQL NULL has no equality and no well-defined hash in the SQL
/// standard, but Rust's `Hash + Eq` are required for use in
/// `HashMap` / `HashSet` (e.g. the unique-constraint checker). The
/// implementation uses the raw bit pattern of `f32`/`f64` so that
/// NaN-valued keys are treated as equal to themselves — an artificial
/// but safe property for constraint enforcement.
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    /// SQL NULL.
    Null,
    /// `BOOLEAN`.
    Bool(bool),
    /// `SMALLINT`.
    Int16(i16),
    /// `INTEGER`.
    Int32(i32),
    /// `BIGINT`.
    Int64(i64),
    /// `REAL`.
    Float32(f32),
    /// `DOUBLE PRECISION`.
    Float64(f64),
    /// UTF-8 text.
    Text(String),
    /// Binary.
    Bytea(Vec<u8>),
    /// Microsecond-precision timestamp (no zone). Microseconds since
    /// 2000-01-01 00:00:00.
    Timestamp(i64),
    /// Microsecond-precision timestamp (UTC). Microseconds since
    /// 2000-01-01 00:00:00 UTC.
    TimestampTz(i64),
    /// Date — days since 2000-01-01.
    Date(i32),
    /// Time — microseconds since midnight.
    Time(i64),
    /// UUID — raw 16 bytes.
    Uuid([u8; 16]),
    /// Decimal/Numeric — scaled integer representation. The runtime
    /// value is `value * 10^-scale`. Storage shape is `i64` to keep
    /// the eval path numeric-fast; `DECIMAL(p, s)` columns whose
    /// product or sum overflows i64 must be widened by the planner
    /// before the operation lands here.
    Decimal {
        /// Scaled integer payload.
        value: i64,
        /// Number of digits after the decimal point.
        scale: i32,
    },
    /// Interval — separate month / day / microsecond components, matching
    /// the PostgreSQL `INTERVAL` value shape so that `DATE + INTERVAL`
    /// month-aware arithmetic gives the same result.
    Interval {
        /// Whole months.
        months: i32,
        /// Whole days.
        days: i32,
        /// Sub-day microseconds.
        microseconds: i64,
    },
}

/// `Eq` is satisfied because `PartialEq` is reflexive on the bit-pattern
/// definition used by `Hash` below.
impl Eq for Value {}

#[allow(clippy::match_same_arms)] // Arms are spelled out per-variant for
// explicitness; bodies look identical
// because only two need special handling
// (Float32/Float64 use to_bits()), and
// merging the rest into an or-pattern is
// impossible when inner types differ.
impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Hash the discriminant first so variants with the same payload
        // produce different hashes.
        core::mem::discriminant(self).hash(state);
        match self {
            Self::Null => {}
            Self::Bool(v) => v.hash(state),
            Self::Int16(v) => v.hash(state),
            Self::Int32(v) => v.hash(state),
            Self::Int64(v) => v.hash(state),
            // Use the raw IEEE-754 bit pattern so the impl is consistent
            // with the `PartialEq` derive (which compares bits via f32 ==
            // for non-NaN, and treats NaN != NaN). For constraint checking
            // purposes we hash NaN by its bit pattern; two NaN values with
            // the same bit pattern hash equal and compare equal under this
            // impl, which is fine for `HashSet` keying.
            Self::Float32(v) => v.to_bits().hash(state),
            Self::Float64(v) => v.to_bits().hash(state),
            Self::Text(v) => v.hash(state),
            Self::Bytea(v) => v.hash(state),
            Self::Timestamp(v) | Self::TimestampTz(v) | Self::Time(v) => v.hash(state),
            Self::Date(v) => v.hash(state),
            Self::Uuid(v) => v.hash(state),
            Self::Decimal { value, scale } => {
                value.hash(state);
                scale.hash(state);
            }
            Self::Interval {
                months,
                days,
                microseconds,
            } => {
                months.hash(state);
                days.hash(state);
                microseconds.hash(state);
            }
        }
    }
}

/// Conventional alias used in PostgreSQL literature.
pub type Datum = Value;

impl Value {
    /// The dynamic [`DataType`] of this value.
    #[must_use]
    pub const fn data_type(&self) -> DataType {
        match self {
            Self::Null => DataType::Null,
            Self::Bool(_) => DataType::Bool,
            Self::Int16(_) => DataType::Int16,
            Self::Int32(_) => DataType::Int32,
            Self::Int64(_) => DataType::Int64,
            Self::Float32(_) => DataType::Float32,
            Self::Float64(_) => DataType::Float64,
            Self::Text(_) => DataType::Text { max_len: None },
            Self::Bytea(_) => DataType::Bytea,
            Self::Timestamp(_) => DataType::Timestamp,
            Self::TimestampTz(_) => DataType::TimestampTz,
            Self::Date(_) => DataType::Date,
            Self::Time(_) => DataType::Time,
            Self::Uuid(_) => DataType::Uuid,
            Self::Decimal { scale, .. } => DataType::Decimal {
                precision: None,
                scale: Some(*scale),
            },
            Self::Interval { .. } => DataType::Interval,
        }
    }

    /// Width category used during planning: `None` for varlena values.
    #[must_use]
    pub const fn fixed_size(&self) -> Option<usize> {
        match self {
            Self::Bool(_) => Some(1),
            Self::Int16(_) => Some(2),
            Self::Int32(_) | Self::Float32(_) | Self::Date(_) => Some(4),
            Self::Int64(_)
            | Self::Float64(_)
            | Self::Time(_)
            | Self::Timestamp(_)
            | Self::TimestampTz(_) => Some(8),
            Self::Uuid(_) => Some(16),
            _ => None,
        }
    }

    /// `true` iff this value is SQL NULL.
    #[must_use]
    pub const fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    /// Borrowed `i64` view if this is an integer type, widening from
    /// narrower integers losslessly. `None` for non-integers.
    #[must_use]
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Int16(v) => Some(i64::from(*v)),
            Self::Int32(v) => Some(i64::from(*v)),
            Self::Int64(v) => Some(*v),
            _ => None,
        }
    }

    /// Borrowed `f64` view if this is a floating-point type, widening
    /// `f32` to `f64`. `None` for non-floats.
    #[must_use]
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Float32(v) => Some(f64::from(*v)),
            Self::Float64(v) => Some(*v),
            _ => None,
        }
    }

    /// Borrowed string view if this is text. `None` otherwise.
    #[must_use]
    pub fn as_text(&self) -> Option<&str> {
        match self {
            Self::Text(s) => Some(s.as_str()),
            _ => None,
        }
    }

    /// Borrowed byte-slice view if this is bytea. `None` otherwise.
    #[must_use]
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Bytea(b) => Some(b.as_slice()),
            _ => None,
        }
    }

    /// Borrowed bool view. `None` for non-boolean.
    #[must_use]
    pub const fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Bool(b) => Some(*b),
            _ => None,
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Null => f.write_str("NULL"),
            Self::Bool(b) => f.write_str(if *b { "true" } else { "false" }),
            Self::Int16(v) => write!(f, "{v}"),
            Self::Int32(v) => write!(f, "{v}"),
            Self::Int64(v) => write!(f, "{v}"),
            Self::Float32(v) => write!(f, "{v}"),
            Self::Float64(v) => write!(f, "{v}"),
            Self::Text(s) => write!(f, "{s}"),
            Self::Bytea(b) => {
                f.write_str("\\x")?;
                for byte in b {
                    write!(f, "{byte:02x}")?;
                }
                Ok(())
            }
            Self::Timestamp(us) | Self::TimestampTz(us) => write!(f, "{us}us"),
            Self::Date(d) => write!(f, "{d}d"),
            Self::Time(t) => write!(f, "{t}us"),
            Self::Decimal { value, scale } => {
                // PostgreSQL-style fixed-point text. `value` is the
                // scaled integer; insert the decimal point `scale`
                // digits from the right. Negative scale (allowed by
                // the type) appends trailing zeros instead.
                let sign = if *value < 0 { "-" } else { "" };
                let mag = value.unsigned_abs();
                if *scale <= 0 {
                    let pow = u64::checked_pow(10, scale.unsigned_abs()).unwrap_or(1);
                    write!(f, "{sign}{}", mag.saturating_mul(pow))
                } else {
                    let scale_u = u32::try_from(*scale).unwrap_or(0);
                    let divisor = u64::checked_pow(10, scale_u).unwrap_or(1);
                    let whole = mag / divisor;
                    let frac = mag % divisor;
                    write!(f, "{sign}{whole}.{frac:0width$}", width = scale_u as usize)
                }
            }
            Self::Interval {
                months,
                days,
                microseconds,
            } => write!(f, "{months}mon {days}d {microseconds}us"),
            Self::Uuid(u) => {
                write!(
                    f,
                    "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
                    u[0],
                    u[1],
                    u[2],
                    u[3],
                    u[4],
                    u[5],
                    u[6],
                    u[7],
                    u[8],
                    u[9],
                    u[10],
                    u[11],
                    u[12],
                    u[13],
                    u[14],
                    u[15]
                )
            }
        }
    }
}

impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Self::Bool(v)
    }
}
impl From<i16> for Value {
    fn from(v: i16) -> Self {
        Self::Int16(v)
    }
}
impl From<i32> for Value {
    fn from(v: i32) -> Self {
        Self::Int32(v)
    }
}
impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Self::Int64(v)
    }
}
impl From<f32> for Value {
    fn from(v: f32) -> Self {
        Self::Float32(v)
    }
}
impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Self::Float64(v)
    }
}
impl From<String> for Value {
    fn from(v: String) -> Self {
        Self::Text(v)
    }
}
impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Self::Text(v.to_owned())
    }
}
impl From<Vec<u8>> for Value {
    fn from(v: Vec<u8>) -> Self {
        Self::Bytea(v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn null_is_null() {
        assert!(Value::Null.is_null());
        assert!(!Value::Int32(0).is_null());
    }

    #[test]
    fn data_type_matches_variant() {
        assert_eq!(Value::Int32(1).data_type(), DataType::Int32);
        assert_eq!(Value::Int64(1).data_type(), DataType::Int64);
        assert_eq!(Value::Bool(true).data_type(), DataType::Bool);
        assert_eq!(
            Value::Text("hi".into()).data_type(),
            DataType::Text { max_len: None }
        );
        assert_eq!(Value::Null.data_type(), DataType::Null);
    }

    #[test]
    fn integer_widening_accessors() {
        assert_eq!(Value::Int16(7).as_i64(), Some(7));
        assert_eq!(Value::Int32(7).as_i64(), Some(7));
        assert_eq!(Value::Int64(7).as_i64(), Some(7));
        assert_eq!(Value::Float32(7.0).as_i64(), None);
        assert_eq!(Value::Null.as_i64(), None);
    }

    #[test]
    fn float_widening_accessors() {
        assert_eq!(Value::Float32(1.5).as_f64(), Some(1.5));
        assert_eq!(Value::Float64(2.5).as_f64(), Some(2.5));
        assert_eq!(Value::Int32(1).as_f64(), None);
    }

    #[test]
    fn text_and_bytes_accessors() {
        let t = Value::Text("hello".into());
        assert_eq!(t.as_text(), Some("hello"));
        assert_eq!(t.as_bytes(), None);
        let b = Value::Bytea(vec![0xde, 0xad]);
        assert_eq!(b.as_bytes(), Some(&[0xde, 0xad][..]));
        assert_eq!(b.as_text(), None);
    }

    #[test]
    fn display_round_trip_for_simple_values() {
        assert_eq!(Value::Null.to_string(), "NULL");
        assert_eq!(Value::Bool(true).to_string(), "true");
        assert_eq!(Value::Int64(-7).to_string(), "-7");
        assert_eq!(Value::Text("hi".into()).to_string(), "hi");
        assert_eq!(Value::Bytea(vec![0xde, 0xad]).to_string(), "\\xdead");
    }

    #[test]
    fn uuid_display_is_canonical() {
        let bytes = [
            0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc,
            0xde, 0xf0,
        ];
        assert_eq!(
            Value::Uuid(bytes).to_string(),
            "12345678-9abc-def0-1234-56789abcdef0"
        );
    }

    #[test]
    fn from_impls() {
        let v: Value = 7_i32.into();
        assert_eq!(v, Value::Int32(7));
        let v: Value = "abc".into();
        assert_eq!(v, Value::Text("abc".into()));
    }
}
