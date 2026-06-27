use super::*;

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
#[derive(Clone, Debug)]
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
    /// PostgreSQL `oid`.
    Oid(Oid),
    /// PostgreSQL `regclass` relation OID alias.
    RegClass(Oid),
    /// PostgreSQL `regtype` type OID alias.
    RegType(Oid),
    /// PostgreSQL `pg_lsn`.
    PgLsn(Lsn),
    /// `REAL`.
    Float32(f32),
    /// `DOUBLE PRECISION`.
    Float64(f64),
    /// UTF-8 text.
    Text(String),
    /// Blank-padded `CHAR(n)` / `bpchar` text storage.
    Char(String),
    /// Textual JSON payload that preserves the accepted input spelling.
    Json(String),
    /// JSONB-compatible canonical textual payload.
    Jsonb(String),
    /// Well-formed textual XML document payload.
    Xml(String),
    /// SQL vector finite single-precision vector.
    Vector(Vec<f32>),
    /// SQL vector finite half-precision vector.
    HalfVec(Vec<f32>),
    /// SQL vector sparse vector.
    SparseVec(SparseVector),
    /// Dense bit vector. Bits are packed most-significant-bit first
    /// inside each byte; `dims` names the logical bit count.
    BitVec {
        /// Logical bit count.
        dims: u32,
        /// Packed bit payload.
        bytes: Vec<u8>,
    },
    /// SQL `BIT` / `VARBIT` value.
    BitString(BitString),
    /// SQL `INET` / `CIDR` / `MACADDR` / `MACADDR8` value.
    Network(NetworkValue),
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
    /// Time with time zone — microseconds since midnight plus fixed UTC
    /// offset seconds, matching PostgreSQL `timetz` output semantics.
    TimeTz {
        /// Time of day in microseconds.
        micros: i64,
        /// UTC offset in seconds; east of UTC is positive.
        offset_seconds: i32,
    },
    /// UUID — raw 16 bytes.
    Uuid([u8; 16]),
    /// Decimal/Numeric — scaled integer runtime representation. The value
    /// is `value * 10^-scale`. Heap storage uses the row codec's
    /// PostgreSQL-style base-10000 numeric payload; the executor keeps
    /// this shape to keep current eval paths numeric-fast.
    Decimal {
        /// Scaled integer payload. Backed by `i128` (~38 significant
        /// digits) so that NUMERIC literals and computed values that
        /// exceed `i64` are represented exactly rather than silently
        /// truncated. Values beyond `i128` raise SQLSTATE `22003`.
        value: i128,
        /// Number of digits after the decimal point.
        scale: i32,
    },
    /// Money — signed 64-bit cents, matching PostgreSQL's `Cash`
    /// binary shape at the protocol boundary.
    Money(i64),
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
    /// PostgreSQL range value.
    Range(RangeValue),
    /// PostgreSQL geometric value.
    Geometry(GeometryValue),
    /// PostgreSQL array value with a homogeneous element type.
    Array {
        /// Element type shared by every non-NULL array element.
        element_type: DataType,
        /// Array elements in logical order.
        elements: Vec<Value>,
    },
    /// PostgreSQL record / row value.
    Record(Vec<(String, Value)>),
}

/// `Eq` is satisfied because `PartialEq` is reflexive on the bit-pattern
/// definition used by `Hash` below.
impl Eq for Value {}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Null, Self::Null) => true,
            (Self::Bool(left), Self::Bool(right)) => left == right,
            (Self::Int16(left), Self::Int16(right)) => left == right,
            (Self::Int32(left), Self::Int32(right)) => left == right,
            (Self::Int64(left), Self::Int64(right)) => left == right,
            (Self::Oid(left), Self::Oid(right))
            | (Self::RegClass(left), Self::RegClass(right))
            | (Self::RegType(left), Self::RegType(right)) => left == right,
            (Self::PgLsn(left), Self::PgLsn(right)) => left == right,
            (Self::Float32(left), Self::Float32(right)) => left == right,
            (Self::Float64(left), Self::Float64(right)) => left == right,
            (Self::Text(left), Self::Text(right)) => left == right,
            (Self::Char(left), Self::Char(right)) => {
                bpchar_semantic_text(left) == bpchar_semantic_text(right)
            }
            (Self::Json(left), Self::Json(right))
            | (Self::Jsonb(left), Self::Jsonb(right))
            | (Self::Xml(left), Self::Xml(right)) => left == right,
            (Self::Vector(left), Self::Vector(right))
            | (Self::HalfVec(left), Self::HalfVec(right)) => left == right,
            (Self::SparseVec(left), Self::SparseVec(right)) => left == right,
            (
                Self::BitVec {
                    dims: left_dims,
                    bytes: left_bytes,
                },
                Self::BitVec {
                    dims: right_dims,
                    bytes: right_bytes,
                },
            ) => left_dims == right_dims && left_bytes == right_bytes,
            (Self::BitString(left), Self::BitString(right)) => left == right,
            (Self::Network(left), Self::Network(right)) => left == right,
            (Self::Bytea(left), Self::Bytea(right)) => left == right,
            (Self::Timestamp(left), Self::Timestamp(right))
            | (Self::TimestampTz(left), Self::TimestampTz(right))
            | (Self::Time(left), Self::Time(right)) => left == right,
            (
                Self::TimeTz {
                    micros: left_micros,
                    offset_seconds: left_offset,
                },
                Self::TimeTz {
                    micros: right_micros,
                    offset_seconds: right_offset,
                },
            ) => {
                timetz_utc_micros(*left_micros, *left_offset)
                    == timetz_utc_micros(*right_micros, *right_offset)
            }
            (Self::Date(left), Self::Date(right)) => left == right,
            (Self::Uuid(left), Self::Uuid(right)) => left == right,
            (
                Self::Decimal {
                    value: left_value,
                    scale: left_scale,
                },
                Self::Decimal {
                    value: right_value,
                    scale: right_scale,
                },
            ) => left_value == right_value && left_scale == right_scale,
            (Self::Money(left), Self::Money(right)) => left == right,
            (
                Self::Interval {
                    months: left_months,
                    days: left_days,
                    microseconds: left_microseconds,
                },
                Self::Interval {
                    months: right_months,
                    days: right_days,
                    microseconds: right_microseconds,
                },
            ) => {
                left_months == right_months
                    && left_days == right_days
                    && left_microseconds == right_microseconds
            }
            (Self::Range(left), Self::Range(right)) => left == right,
            (Self::Geometry(left), Self::Geometry(right)) => left == right,
            (
                Self::Array {
                    element_type: left_type,
                    elements: left_elements,
                },
                Self::Array {
                    element_type: right_type,
                    elements: right_elements,
                },
            ) => left_type == right_type && left_elements == right_elements,
            (Self::Record(left), Self::Record(right)) => left == right,
            _ => false,
        }
    }
}

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
            Self::Oid(v) | Self::RegClass(v) | Self::RegType(v) => v.hash(state),
            Self::PgLsn(v) => v.hash(state),
            // Use the raw IEEE-754 bit pattern so the impl is consistent
            // with the `PartialEq` derive (which compares bits via f32 ==
            // for non-NaN, and treats NaN != NaN). For constraint checking
            // purposes we hash NaN by its bit pattern; two NaN values with
            // the same bit pattern hash equal and compare equal under this
            // impl, which is fine for `HashSet` keying.
            Self::Float32(v) => v.to_bits().hash(state),
            Self::Float64(v) => v.to_bits().hash(state),
            Self::Text(v) => v.hash(state),
            Self::Char(v) => bpchar_semantic_text(v).hash(state),
            Self::Json(v) | Self::Jsonb(v) | Self::Xml(v) => v.hash(state),
            Self::Vector(v) | Self::HalfVec(v) => {
                for element in v {
                    element.to_bits().hash(state);
                }
            }
            Self::SparseVec(v) => v.hash(state),
            Self::BitVec { dims, bytes } => {
                dims.hash(state);
                bytes.hash(state);
            }
            Self::BitString(v) => v.hash(state),
            Self::Network(v) => v.hash(state),
            Self::Bytea(v) => v.hash(state),
            Self::Timestamp(v) | Self::TimestampTz(v) | Self::Time(v) => v.hash(state),
            Self::TimeTz {
                micros,
                offset_seconds,
            } => timetz_utc_micros(*micros, *offset_seconds).hash(state),
            Self::Date(v) => v.hash(state),
            Self::Uuid(v) => v.hash(state),
            Self::Decimal { value, scale } => {
                value.hash(state);
                scale.hash(state);
            }
            Self::Money(v) => v.hash(state),
            Self::Interval {
                months,
                days,
                microseconds,
            } => {
                months.hash(state);
                days.hash(state);
                microseconds.hash(state);
            }
            Self::Range(v) => v.hash(state),
            Self::Geometry(v) => v.hash(state),
            Self::Array {
                element_type,
                elements,
            } => {
                element_type.hash(state);
                elements.hash(state);
            }
            Self::Record(fields) => fields.hash(state),
        }
    }
}
