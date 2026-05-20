//! SQL data type system.
//!
//! `DataType` describes the *logical* type of a column or expression. The
//! physical representation (Datum) is in [`crate::value`]. The two are
//! kept separate so an executor that already knows the type from the
//! plan can skip the discriminant on the value.
//!
//! The catalog stores types by `Oid` (matching PostgreSQL's `pg_type`
//! conventions); the in-memory `DataType` is a richer enum used during
//! planning, type-checking, and execution.

use std::fmt;

use crate::error::{Error, Result};

/// Maximum number of `f32` elements in a pgvector-compatible `vector`
/// value.
pub const MAX_VECTOR_DIMS: u32 = 16_000;

/// PostgreSQL range type families supported by the v0.8 GiST operator
/// surface.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RangeType {
    /// `int4range`.
    Int4,
    /// `int8range`.
    Int8,
    /// `numrange`.
    Num,
    /// `daterange`.
    Date,
    /// `tsrange`.
    Timestamp,
    /// `tstzrange`.
    TimestampTz,
}

impl fmt::Display for RangeType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Int4 => "int4range",
            Self::Int8 => "int8range",
            Self::Num => "numrange",
            Self::Date => "daterange",
            Self::Timestamp => "tsrange",
            Self::TimestampTz => "tstzrange",
        })
    }
}

/// PostgreSQL geometric type families supported by the v0.8 GiST
/// operator surface.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum GeometryType {
    /// `point`.
    Point,
    /// `box`.
    Box,
    /// `circle`.
    Circle,
    /// `line`.
    Line,
    /// `lseg`.
    Lseg,
    /// `path`.
    Path,
    /// `polygon`.
    Polygon,
}

impl fmt::Display for GeometryType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Point => "point",
            Self::Box => "box",
            Self::Circle => "circle",
            Self::Line => "line",
            Self::Lseg => "lseg",
            Self::Path => "path",
            Self::Polygon => "polygon",
        })
    }
}

/// Logical SQL type.
///
/// Mirrors the PostgreSQL type family hierarchy in shape, but is
/// represented as a single enum because the engine only needs to
/// distinguish a manageable handful of physical layouts.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum DataType {
    /// `BOOLEAN`. Three-valued: TRUE, FALSE, NULL.
    Bool,

    /// `SMALLINT` / `INT2`.
    Int16,

    /// `INTEGER` / `INT4`.
    Int32,

    /// `BIGINT` / `INT8`.
    Int64,

    /// `REAL` / `FLOAT4`.
    Float32,

    /// `DOUBLE PRECISION` / `FLOAT8`.
    Float64,

    /// Arbitrary-precision decimal with optional precision/scale.
    Decimal {
        /// Total number of significant digits, `1..=131_072` (per
        /// PostgreSQL spec). `None` means "no precision specified."
        precision: Option<u32>,
        /// Number of digits after the decimal point.
        scale: Option<i32>,
    },

    /// Variable-length UTF-8 text. `max_len` is the declared cap if any.
    Text {
        /// Optional declared maximum length, in *characters*.
        max_len: Option<u32>,
    },

    /// Variable-length binary string.
    Bytea,

    /// Microsecond-precision timestamp without time zone.
    Timestamp,

    /// Microsecond-precision timestamp with time zone (UTC stored).
    TimestampTz,

    /// `DATE` (days since 2000-01-01).
    Date,

    /// `TIME` without time zone (microseconds in the day).
    Time,

    /// `INTERVAL` (months, days, microseconds).
    Interval,

    /// UUID.
    Uuid,

    /// JSON-binary (JSONB-compatible).
    Jsonb,

    /// pgvector-compatible single-precision embedding vector.
    ///
    /// `None` represents the unconstrained `vector` type. `Some(n)`
    /// represents `vector(n)` and requires values to have exactly `n`
    /// finite `f32` elements when value storage lands.
    Vector {
        /// Fixed dimension for `vector(n)`, or `None` for unconstrained
        /// `vector`.
        dims: Option<u32>,
    },

    /// PostgreSQL range value family.
    Range(RangeType),

    /// PostgreSQL geometric value family.
    Geometry(GeometryType),

    /// A fixed-length array of any [`DataType`].
    Array(Box<Self>),

    /// Record / row type — used in row constructors and composite
    /// columns.
    Record(Vec<(String, Self)>),

    /// The `NULL` type — the type of an untyped NULL literal before
    /// coercion.
    Null,
}

impl DataType {
    /// Size in bytes if the type is fixed-width; `None` for varlena
    /// types.
    #[must_use]
    pub const fn fixed_size(&self) -> Option<usize> {
        match self {
            Self::Bool => Some(1),
            Self::Int16 => Some(2),
            Self::Int32 | Self::Float32 | Self::Date => Some(4),
            Self::Int64 | Self::Float64 | Self::Time | Self::Timestamp | Self::TimestampTz => {
                Some(8)
            }
            Self::Interval | Self::Uuid => Some(16),
            _ => None,
        }
    }

    /// Natural alignment in bytes. All non-fixed types fall back to the
    /// pointer-width alignment used by varlena indirection slots.
    #[must_use]
    pub const fn alignment(&self) -> usize {
        match self {
            Self::Bool => 1,
            Self::Int16 => 2,
            Self::Int32 | Self::Float32 | Self::Date => 4,
            _ => 8,
        }
    }

    /// Numeric category for implicit conversion rules.
    #[must_use]
    pub const fn is_numeric(&self) -> bool {
        matches!(
            self,
            Self::Int16
                | Self::Int32
                | Self::Int64
                | Self::Float32
                | Self::Float64
                | Self::Decimal { .. }
        )
    }

    /// Integer category.
    #[must_use]
    pub const fn is_integer(&self) -> bool {
        matches!(self, Self::Int16 | Self::Int32 | Self::Int64)
    }

    /// Floating-point category.
    #[must_use]
    pub const fn is_float(&self) -> bool {
        matches!(self, Self::Float32 | Self::Float64)
    }

    /// String-y category (subject to text functions).
    #[must_use]
    pub const fn is_textlike(&self) -> bool {
        matches!(self, Self::Text { .. })
    }

    /// Date/time category.
    #[must_use]
    pub const fn is_temporal(&self) -> bool {
        matches!(
            self,
            Self::Date | Self::Time | Self::Timestamp | Self::TimestampTz | Self::Interval
        )
    }

    /// Whether values of this type are stored out-of-line (varlena).
    #[must_use]
    pub const fn is_varlena(&self) -> bool {
        self.fixed_size().is_none() && !matches!(self, Self::Null)
    }

    /// Compute the implicit numeric coercion type for a binary
    /// arithmetic operation between two numeric types. Returns an
    /// error if the inputs are not jointly numeric.
    ///
    /// The rule follows PostgreSQL: prefer the most general numeric
    /// category (decimal > float > integer), and within a category
    /// prefer the wider variant.
    pub fn numeric_join(&self, other: &Self) -> Result<Self> {
        if !self.is_numeric() || !other.is_numeric() {
            return Err(Error::Type(format!(
                "non-numeric types {self} and {other} in arithmetic"
            )));
        }

        // Decimal absorbs everything.
        if matches!(self, Self::Decimal { .. }) || matches!(other, Self::Decimal { .. }) {
            return Ok(Self::Decimal {
                precision: None,
                scale: None,
            });
        }

        // Float absorbs integer.
        if self.is_float() || other.is_float() {
            return Ok(if self == &Self::Float64 || other == &Self::Float64 {
                Self::Float64
            } else {
                Self::Float32
            });
        }

        // Both integer: widen.
        Ok(match (self, other) {
            (Self::Int64, _) | (_, Self::Int64) => Self::Int64,
            (Self::Int32, _) | (_, Self::Int32) => Self::Int32,
            _ => Self::Int16,
        })
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bool => f.write_str("boolean"),
            Self::Int16 => f.write_str("smallint"),
            Self::Int32 => f.write_str("integer"),
            Self::Int64 => f.write_str("bigint"),
            Self::Float32 => f.write_str("real"),
            Self::Float64 => f.write_str("double precision"),
            Self::Decimal { precision, scale } => match (precision, scale) {
                (Some(p), Some(s)) => write!(f, "numeric({p},{s})"),
                (Some(p), None) => write!(f, "numeric({p})"),
                _ => f.write_str("numeric"),
            },
            Self::Text { max_len: Some(n) } => write!(f, "varchar({n})"),
            Self::Text { max_len: None } => f.write_str("text"),
            Self::Bytea => f.write_str("bytea"),
            Self::Timestamp => f.write_str("timestamp"),
            Self::TimestampTz => f.write_str("timestamptz"),
            Self::Date => f.write_str("date"),
            Self::Time => f.write_str("time"),
            Self::Interval => f.write_str("interval"),
            Self::Uuid => f.write_str("uuid"),
            Self::Jsonb => f.write_str("jsonb"),
            Self::Vector { dims: Some(dims) } => write!(f, "vector({dims})"),
            Self::Vector { dims: None } => f.write_str("vector"),
            Self::Range(range_type) => write!(f, "{range_type}"),
            Self::Geometry(geometry_type) => write!(f, "{geometry_type}"),
            Self::Array(inner) => write!(f, "{inner}[]"),
            Self::Record(fields) => {
                f.write_str("record(")?;
                for (i, (name, ty)) in fields.iter().enumerate() {
                    if i > 0 {
                        f.write_str(", ")?;
                    }
                    write!(f, "{name} {ty}")?;
                }
                f.write_str(")")
            }
            Self::Null => f.write_str("null"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_sizes_match_disk_layout() {
        assert_eq!(DataType::Bool.fixed_size(), Some(1));
        assert_eq!(DataType::Int32.fixed_size(), Some(4));
        assert_eq!(DataType::Int64.fixed_size(), Some(8));
        assert_eq!(DataType::Timestamp.fixed_size(), Some(8));
        assert_eq!(DataType::Uuid.fixed_size(), Some(16));
        assert_eq!(DataType::Vector { dims: Some(3) }.fixed_size(), None);
        assert_eq!(DataType::Text { max_len: None }.fixed_size(), None);
        assert_eq!(DataType::Bytea.fixed_size(), None);
    }

    #[test]
    fn alignment_at_least_byte() {
        for ty in [
            DataType::Bool,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
            DataType::Timestamp,
            DataType::Uuid,
        ] {
            assert!(ty.alignment() >= 1);
            assert!(ty.alignment().is_power_of_two());
        }
    }

    #[test]
    fn numeric_join_widens_integers() {
        let i16 = DataType::Int16;
        let i32 = DataType::Int32;
        let i64 = DataType::Int64;
        assert_eq!(i16.numeric_join(&i32).unwrap(), i32);
        assert_eq!(i32.numeric_join(&i64).unwrap(), i64);
        assert_eq!(i16.numeric_join(&i16).unwrap(), i16);
    }

    #[test]
    fn numeric_join_promotes_to_float() {
        assert_eq!(
            DataType::Int32.numeric_join(&DataType::Float32).unwrap(),
            DataType::Float32
        );
        assert_eq!(
            DataType::Int32.numeric_join(&DataType::Float64).unwrap(),
            DataType::Float64
        );
        assert_eq!(
            DataType::Float32.numeric_join(&DataType::Float64).unwrap(),
            DataType::Float64
        );
    }

    #[test]
    fn vector_display_renders_pgvector_style_type_name() {
        assert_eq!(DataType::Vector { dims: Some(3) }.to_string(), "vector(3)");
        assert_eq!(DataType::Vector { dims: None }.to_string(), "vector");
    }

    #[test]
    fn numeric_join_promotes_to_decimal() {
        let dec = DataType::Decimal {
            precision: Some(20),
            scale: Some(4),
        };
        assert!(matches!(
            DataType::Int32.numeric_join(&dec).unwrap(),
            DataType::Decimal { .. }
        ));
    }

    #[test]
    fn numeric_join_rejects_text() {
        let text = DataType::Text { max_len: None };
        assert!(DataType::Int32.numeric_join(&text).is_err());
    }

    #[test]
    fn display_matches_sql_names() {
        assert_eq!(DataType::Int32.to_string(), "integer");
        assert_eq!(DataType::Int64.to_string(), "bigint");
        assert_eq!(
            DataType::Text { max_len: Some(50) }.to_string(),
            "varchar(50)"
        );
        assert_eq!(DataType::Text { max_len: None }.to_string(), "text");
        assert_eq!(
            DataType::Array(Box::new(DataType::Int32)).to_string(),
            "integer[]"
        );
    }

    #[test]
    fn categorization() {
        assert!(DataType::Int32.is_numeric());
        assert!(DataType::Int32.is_integer());
        assert!(!DataType::Int32.is_float());
        assert!(DataType::Float64.is_float());
        assert!(DataType::Float64.is_numeric());
        assert!(!DataType::Float64.is_integer());
        assert!(DataType::Text { max_len: None }.is_textlike());
        assert!(DataType::Timestamp.is_temporal());
        assert!(!DataType::Int32.is_temporal());
    }

    #[test]
    fn varlena_classification() {
        assert!(!DataType::Int64.is_varlena());
        assert!(DataType::Text { max_len: None }.is_varlena());
        assert!(DataType::Bytea.is_varlena());
        assert!(!DataType::Null.is_varlena());
    }
}
