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
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::id::Oid;

/// Maximum number of `f32` elements in a SQL vector `vector`
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

    /// `MONEY` / `cash`, stored as signed 64-bit fractional currency
    /// units.
    Money,

    /// PostgreSQL `oid`: unsigned 32-bit catalog object identifier.
    Oid,

    /// PostgreSQL `regclass`: relation OID alias.
    RegClass,

    /// PostgreSQL `regtype`: type OID alias.
    RegType,

    /// Variable-length UTF-8 text. `max_len` is the declared cap if any.
    Text {
        /// Optional declared maximum length, in *characters*.
        max_len: Option<u32>,
    },

    /// Blank-padded fixed-length character string (`CHAR(n)` / `bpchar`).
    ///
    /// Values store their padded bytes. Equality and ordering compare the
    /// value with trailing ASCII pad spaces ignored.
    Char {
        /// Optional declared length, in *characters*. `None` represents
        /// PostgreSQL's internal unconstrained `bpchar` family.
        len: Option<u32>,
    },

    /// User-defined enum type created by `CREATE TYPE ... AS ENUM`.
    ///
    /// Values use the text storage path, while the type carries its catalog
    /// OID so RowDescription can advertise the user type instead of `text`.
    Enum {
        /// `pg_type.oid` for the enum type.
        oid: Oid,
        /// Case-folded type name visible in SQL.
        name: Arc<str>,
        /// Allowed labels in declaration order.
        labels: Arc<[String]>,
    },

    /// User-defined composite type created by `CREATE TYPE ... AS (...)`.
    ///
    /// Values currently use PostgreSQL-style composite text storage, while
    /// the type carries its catalog OID so RowDescription advertises the
    /// named type instead of anonymous `record`.
    Composite {
        /// `pg_type.oid` for the composite type.
        oid: Oid,
        /// Case-folded type name visible in SQL.
        name: Arc<str>,
        /// Attribute names and types in declaration order.
        fields: Arc<[(String, Self)]>,
    },

    /// User-defined domain type created by `CREATE DOMAIN`.
    ///
    /// Values use the base type's physical storage path, while the type
    /// carries its catalog OID so RowDescription can advertise the domain
    /// instead of the base type. Runtime CHECK predicates live above core.
    Domain {
        /// `pg_type.oid` for the domain type.
        oid: Oid,
        /// Case-folded type name visible in SQL.
        name: Arc<str>,
        /// Underlying base type used for storage and expression coercion.
        base_type: Box<Self>,
        /// Domain-level NOT NULL constraint.
        not_null: bool,
    },

    /// Variable-length binary string.
    Bytea,

    /// Fixed-length SQL bit string (`BIT(n)`).
    Bit {
        /// Exact bit length. `None` is reserved for internal
        /// unconstrained-family metadata.
        len: Option<u32>,
    },

    /// Variable-length SQL bit string (`VARBIT(n)` / `BIT VARYING(n)`).
    VarBit {
        /// Optional maximum bit length. `None` means unbounded
        /// `BIT VARYING`.
        max_len: Option<u32>,
    },

    /// IPv4/IPv6 host or network address with optional prefix length.
    Inet,

    /// IPv4/IPv6 network address. Host bits outside the prefix are zero.
    Cidr,

    /// Six-byte media access control address.
    MacAddr,

    /// Eight-byte media access control address.
    MacAddr8,

    /// Microsecond-precision timestamp without time zone.
    Timestamp,

    /// Microsecond-precision timestamp with time zone (UTC stored).
    TimestampTz,

    /// `DATE` (days since 2000-01-01).
    Date,

    /// `TIME` without time zone (microseconds in the day).
    Time,

    /// `TIME WITH TIME ZONE` (`timetz`), stored as packed time-of-day
    /// microseconds plus fixed UTC offset seconds.
    TimeTz,

    /// PostgreSQL `pg_lsn`: 64-bit write-ahead-log byte position.
    PgLsn,

    /// `INTERVAL` (months, days, microseconds).
    Interval,

    /// UUID.
    Uuid,

    /// Textual JSON. Values preserve the accepted input spelling.
    Json,

    /// JSON-binary (JSONB-compatible). Values use canonical text storage.
    Jsonb,

    /// Textual XML. Values preserve the accepted input spelling.
    Xml,

    /// SQL vector single-precision embedding vector.
    ///
    /// `None` represents the unconstrained `vector` type. `Some(n)`
    /// represents `vector(n)` and requires values to have exactly `n`
    /// finite `f32` elements when value storage lands.
    Vector {
        /// Fixed dimension for `vector(n)`, or `None` for unconstrained
        /// `vector`.
        dims: Option<u32>,
    },

    /// SQL vector half-precision embedding vector.
    ///
    /// Runtime values are kept as finite `f32` values at expression
    /// boundaries; storage can choose a narrower binary layout while the
    /// type system carries the declared dimension.
    HalfVec {
        /// Fixed dimension for `halfvec(n)`, or `None` for unconstrained
        /// `halfvec`.
        dims: Option<u32>,
    },

    /// SQL vector sparse embedding vector.
    SparseVec {
        /// Fixed dimension for `sparsevec(n)`, or `None` for unconstrained
        /// `sparsevec`.
        dims: Option<u32>,
    },

    /// Dense bit-vector used by pgvector-style binary embeddings.
    BitVec {
        /// Fixed dimension for `bitvec(n)`, or `None` for unconstrained
        /// `bitvec`.
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
            Self::Int32
            | Self::Float32
            | Self::Date
            | Self::Oid
            | Self::RegClass
            | Self::RegType => Some(4),
            Self::Int64
            | Self::Money
            | Self::Float64
            | Self::Time
            | Self::TimeTz
            | Self::Timestamp
            | Self::TimestampTz
            | Self::PgLsn => Some(8),
            Self::Interval | Self::Uuid => Some(16),
            Self::Domain { base_type, .. } => base_type.fixed_size(),
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
            Self::Int32
            | Self::Float32
            | Self::Date
            | Self::Oid
            | Self::RegClass
            | Self::RegType => 4,
            Self::Domain { base_type, .. } => base_type.alignment(),
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

    /// PostgreSQL OID alias category (`oid`, `regclass`, `regtype`).
    #[must_use]
    pub const fn is_oid_alias(&self) -> bool {
        matches!(self, Self::Oid | Self::RegClass | Self::RegType)
    }

    /// Floating-point category.
    #[must_use]
    pub const fn is_float(&self) -> bool {
        matches!(self, Self::Float32 | Self::Float64)
    }

    /// String-y category (subject to text functions).
    #[must_use]
    pub const fn is_textlike(&self) -> bool {
        matches!(self, Self::Text { .. } | Self::Char { .. })
    }

    /// User-defined enum category.
    #[must_use]
    pub const fn is_enum(&self) -> bool {
        matches!(self, Self::Enum { .. })
    }

    /// User-defined composite category.
    #[must_use]
    pub const fn is_composite(&self) -> bool {
        matches!(self, Self::Composite { .. })
    }

    /// User-defined domain category.
    #[must_use]
    pub const fn is_domain(&self) -> bool {
        matches!(self, Self::Domain { .. })
    }

    /// Physical storage type for values of this logical type.
    ///
    /// Domains use their base type's storage. All other types store as
    /// themselves.
    #[must_use]
    pub fn storage_type(&self) -> &Self {
        match self {
            Self::Domain { base_type, .. } => base_type.storage_type(),
            other => other,
        }
    }

    /// SQL bit-string category.
    #[must_use]
    pub const fn is_bit_string(&self) -> bool {
        matches!(self, Self::Bit { .. } | Self::VarBit { .. })
    }

    /// SQL network-address category (`inet`, `cidr`, `macaddr`, `macaddr8`).
    #[must_use]
    pub const fn is_network_address(&self) -> bool {
        matches!(
            self,
            Self::Inet | Self::Cidr | Self::MacAddr | Self::MacAddr8
        )
    }

    /// SQL IP network category (`inet`, `cidr`).
    #[must_use]
    pub const fn is_ip_network(&self) -> bool {
        matches!(self, Self::Inet | Self::Cidr)
    }

    /// SQL MAC address category (`macaddr`, `macaddr8`).
    #[must_use]
    pub const fn is_mac_address(&self) -> bool {
        matches!(self, Self::MacAddr | Self::MacAddr8)
    }

    /// Date/time category.
    #[must_use]
    pub const fn is_temporal(&self) -> bool {
        matches!(
            self,
            Self::Date
                | Self::Time
                | Self::TimeTz
                | Self::Timestamp
                | Self::TimestampTz
                | Self::Interval
        )
    }

    /// Whether this type is one of the vector-family embedding types.
    #[must_use]
    pub const fn is_vector_family(&self) -> bool {
        matches!(
            self,
            Self::Vector { .. }
                | Self::HalfVec { .. }
                | Self::SparseVec { .. }
                | Self::BitVec { .. }
        )
    }

    /// Declared vector-family dimension metadata, if this is a vector type.
    #[must_use]
    pub const fn vector_dims(&self) -> Option<Option<u32>> {
        match self {
            Self::Vector { dims }
            | Self::HalfVec { dims }
            | Self::SparseVec { dims }
            | Self::BitVec { dims } => Some(*dims),
            _ => None,
        }
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
            Self::Money => f.write_str("money"),
            Self::Oid => f.write_str("oid"),
            Self::RegClass => f.write_str("regclass"),
            Self::RegType => f.write_str("regtype"),
            Self::Text { max_len: Some(n) } => write!(f, "varchar({n})"),
            Self::Text { max_len: None } => f.write_str("text"),
            Self::Char { len: Some(n) } => write!(f, "character({n})"),
            Self::Char { len: None } => f.write_str("bpchar"),
            Self::Enum { name, .. } => f.write_str(name),
            Self::Composite { name, .. } => f.write_str(name),
            Self::Domain { name, .. } => f.write_str(name),
            Self::Bytea => f.write_str("bytea"),
            Self::Bit { len: Some(n) } => write!(f, "bit({n})"),
            Self::Bit { len: None } => f.write_str("bit"),
            Self::VarBit { max_len: Some(n) } => write!(f, "varbit({n})"),
            Self::VarBit { max_len: None } => f.write_str("varbit"),
            Self::Inet => f.write_str("inet"),
            Self::Cidr => f.write_str("cidr"),
            Self::MacAddr => f.write_str("macaddr"),
            Self::MacAddr8 => f.write_str("macaddr8"),
            Self::Timestamp => f.write_str("timestamp"),
            Self::TimestampTz => f.write_str("timestamptz"),
            Self::Date => f.write_str("date"),
            Self::Time => f.write_str("time"),
            Self::TimeTz => f.write_str("timetz"),
            Self::PgLsn => f.write_str("pg_lsn"),
            Self::Interval => f.write_str("interval"),
            Self::Uuid => f.write_str("uuid"),
            Self::Json => f.write_str("json"),
            Self::Jsonb => f.write_str("jsonb"),
            Self::Xml => f.write_str("xml"),
            Self::Vector { dims: Some(dims) } => write!(f, "vector({dims})"),
            Self::Vector { dims: None } => f.write_str("vector"),
            Self::HalfVec { dims: Some(dims) } => write!(f, "halfvec({dims})"),
            Self::HalfVec { dims: None } => f.write_str("halfvec"),
            Self::SparseVec { dims: Some(dims) } => write!(f, "sparsevec({dims})"),
            Self::SparseVec { dims: None } => f.write_str("sparsevec"),
            Self::BitVec { dims: Some(dims) } => write!(f, "bitvec({dims})"),
            Self::BitVec { dims: None } => f.write_str("bitvec"),
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

/// Count attributes in PostgreSQL-style composite text, e.g. `(a,b)`.
///
/// This parser is intentionally structural: it validates balanced outer
/// parentheses, top-level commas, double-quoted fields, backslash escapes
/// inside quotes, and nested parenthesised text. It does not coerce field
/// values; callers use it to reject values whose arity cannot match a named
/// composite type.
#[must_use]
pub fn composite_text_arity(value: &str) -> Option<usize> {
    let trimmed = value.trim();
    if !trimmed.starts_with('(') || !trimmed.ends_with(')') {
        return None;
    }
    let inner = &trimmed[1..trimmed.len().checked_sub(1)?];
    if inner.is_empty() {
        return Some(0);
    }
    let mut fields = 1_usize;
    let mut depth = 0_usize;
    let mut in_quote = false;
    let mut escaped = false;
    for ch in inner.chars() {
        if in_quote {
            if escaped {
                escaped = false;
                continue;
            }
            match ch {
                '\\' => escaped = true,
                '"' => in_quote = false,
                _ => {}
            }
            continue;
        }
        match ch {
            '"' => in_quote = true,
            '(' => depth = depth.checked_add(1)?,
            ')' => depth = depth.checked_sub(1)?,
            ',' if depth == 0 => fields = fields.checked_add(1)?,
            _ => {}
        }
    }
    if in_quote || escaped || depth != 0 {
        return None;
    }
    Some(fields)
}

/// Return `true` if composite text has exactly `expected` attributes.
#[must_use]
pub fn composite_text_matches_arity(value: &str, expected: usize) -> bool {
    composite_text_arity(value) == Some(expected)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_sizes_match_disk_layout() {
        assert_eq!(DataType::Bool.fixed_size(), Some(1));
        assert_eq!(DataType::Int32.fixed_size(), Some(4));
        assert_eq!(DataType::Int64.fixed_size(), Some(8));
        assert_eq!(DataType::Money.fixed_size(), Some(8));
        assert_eq!(DataType::Oid.fixed_size(), Some(4));
        assert_eq!(DataType::RegClass.fixed_size(), Some(4));
        assert_eq!(DataType::RegType.fixed_size(), Some(4));
        assert_eq!(DataType::PgLsn.fixed_size(), Some(8));
        assert_eq!(DataType::Timestamp.fixed_size(), Some(8));
        assert_eq!(DataType::Uuid.fixed_size(), Some(16));
        assert_eq!(DataType::Vector { dims: Some(3) }.fixed_size(), None);
        assert_eq!(DataType::Text { max_len: None }.fixed_size(), None);
        assert_eq!(DataType::Bytea.fixed_size(), None);
        assert_eq!(DataType::Json.fixed_size(), None);
        assert_eq!(DataType::Jsonb.fixed_size(), None);
        assert_eq!(DataType::Xml.fixed_size(), None);
    }

    #[test]
    fn alignment_at_least_byte() {
        for ty in [
            DataType::Bool,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::Money,
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
    fn money_display_matches_postgres_type_name() {
        assert_eq!(DataType::Money.to_string(), "money");
    }

    #[test]
    fn json_display_distinguishes_text_and_binary_json() {
        assert_eq!(DataType::Json.to_string(), "json");
        assert_eq!(DataType::Jsonb.to_string(), "jsonb");
        assert!(DataType::Json.is_varlena());
        assert!(DataType::Jsonb.is_varlena());
    }

    #[test]
    fn xml_display_matches_postgres_type_name() {
        assert_eq!(DataType::Xml.to_string(), "xml");
        assert!(DataType::Xml.is_varlena());
    }

    #[test]
    fn char_display_matches_postgres_bpchar_names() {
        assert_eq!(DataType::Char { len: Some(4) }.to_string(), "character(4)");
        assert_eq!(DataType::Char { len: None }.to_string(), "bpchar");
        assert!(DataType::Char { len: Some(4) }.is_textlike());
    }

    #[test]
    fn bit_display_matches_postgres_names() {
        assert_eq!(DataType::Bit { len: Some(4) }.to_string(), "bit(4)");
        assert_eq!(
            DataType::VarBit { max_len: Some(6) }.to_string(),
            "varbit(6)"
        );
        assert!(DataType::Bit { len: Some(4) }.is_bit_string());
    }

    #[test]
    fn composite_text_arity_counts_top_level_fields() {
        assert_eq!(composite_text_arity("(Main,90210)"), Some(2));
        assert_eq!(composite_text_arity("(\"Main, East\",90210)"), Some(2));
        assert_eq!(composite_text_arity("((nested,field),90210)"), Some(2));
        assert_eq!(composite_text_arity("(OnlyStreet)"), Some(1));
        assert_eq!(composite_text_arity("OnlyStreet"), None);
        assert_eq!(composite_text_arity("(\"unterminated,90210)"), None);
    }

    #[test]
    fn vector_family_display_keeps_dimension_metadata() {
        assert_eq!(
            DataType::HalfVec { dims: Some(3) }.to_string(),
            "halfvec(3)"
        );
        assert_eq!(DataType::HalfVec { dims: None }.to_string(), "halfvec");
        assert_eq!(
            DataType::SparseVec { dims: Some(5) }.to_string(),
            "sparsevec(5)"
        );
        assert_eq!(DataType::SparseVec { dims: None }.to_string(), "sparsevec");
        assert_eq!(DataType::BitVec { dims: Some(8) }.to_string(), "bitvec(8)");
        assert_eq!(DataType::BitVec { dims: None }.to_string(), "bitvec");
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
