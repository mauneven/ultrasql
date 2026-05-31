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

use chrono::{Days, LocalResult, NaiveDate, NaiveTime, Offset, TimeZone};
use chrono_tz::OffsetName;

use crate::bit_string::BitString;
use crate::bpchar::{bpchar_semantic_text, coerce_bpchar_text};
use crate::id::{Lsn, Oid};
use crate::money::{format_money_text, parse_money_text};
use crate::network::NetworkValue;
use crate::types::{DataType, GeometryType, MAX_VECTOR_DIMS, RangeType};

/// Microseconds in one civil day.
pub const MICROS_PER_DAY: i64 = 86_400_000_000;

const MICROS_PER_HOUR: i64 = 3_600_000_000;
const MICROS_PER_MINUTE: i64 = 60_000_000;
const MICROS_PER_SECOND: i64 = 1_000_000;
const TIMETZ_OFFSET_BITS: u32 = 18;
const TIMETZ_OFFSET_BIAS_SECONDS: i32 = 86_400;
const TIMETZ_OFFSET_MASK: i64 = (1_i64 << TIMETZ_OFFSET_BITS) - 1;

/// Resolved display metadata for a `TIMESTAMP WITH TIME ZONE` instant.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TimestampTzDisplay {
    /// Local timestamp micros after applying the display timezone offset.
    pub local_micros: i64,
    /// UTC offset in seconds at the displayed instant.
    pub offset_seconds: i32,
    /// Human timezone label for non-ISO styles when one is known.
    pub zone_name: Option<String>,
}

/// Runtime representation for a PostgreSQL range value.
///
/// Bounds are normalised into an ordered scalar space. Integer ranges
/// use their integer value, `daterange` uses days since 2000-01-01, and
/// timestamp ranges currently accept numeric microseconds. This is enough
/// for GiST-style `&&`, `@>`, and `<@` semantics over stored SQL values.
#[derive(Clone, Debug, PartialEq)]
pub struct RangeValue {
    /// Range family.
    pub range_type: RangeType,
    /// Inclusive lower-bound flag. Ignored when `lower` is unbounded.
    pub lower_inc: bool,
    /// Inclusive upper-bound flag. Ignored when `upper` is unbounded.
    pub upper_inc: bool,
    lower: Option<f64>,
    upper: Option<f64>,
    empty: bool,
}

impl RangeValue {
    /// Parse PostgreSQL's common textual range form, e.g. `[1,10)`.
    #[must_use]
    pub fn parse(range_type: RangeType, text: &str) -> Option<Self> {
        let trimmed = text.trim();
        if trimmed.eq_ignore_ascii_case("empty") {
            return Some(Self {
                range_type,
                lower_inc: false,
                upper_inc: false,
                lower: None,
                upper: None,
                empty: true,
            });
        }
        let mut chars = trimmed.chars();
        let first = chars.next()?;
        let last = trimmed.chars().last()?;
        let lower_inc = match first {
            '[' => true,
            '(' => false,
            _ => return None,
        };
        let upper_inc = match last {
            ']' => true,
            ')' => false,
            _ => return None,
        };
        let inner = &trimmed[first.len_utf8()..trimmed.len().checked_sub(last.len_utf8())?];
        let (lower_s, upper_s) = split_once_unquoted_comma(inner)?;
        let lower = parse_range_bound(range_type, lower_s.trim())?;
        let upper = parse_range_bound(range_type, upper_s.trim())?;
        let empty = range_is_empty(lower, upper, lower_inc, upper_inc);
        Some(Self {
            range_type,
            lower_inc,
            upper_inc,
            lower,
            upper,
            empty,
        })
    }

    /// `true` when two ranges share any point.
    #[must_use]
    pub fn overlaps(&self, other: &Self) -> bool {
        if self.range_type != other.range_type || self.empty || other.empty {
            return false;
        }
        !upper_before_lower(self.upper, self.upper_inc, other.lower, other.lower_inc)
            && !upper_before_lower(other.upper, other.upper_inc, self.lower, self.lower_inc)
    }

    /// `true` when this range contains `other`.
    #[must_use]
    pub fn contains_range(&self, other: &Self) -> bool {
        if self.range_type != other.range_type || self.empty {
            return false;
        }
        if other.empty {
            return true;
        }
        lower_covers_lower(self.lower, self.lower_inc, other.lower, other.lower_inc)
            && upper_covers_upper(self.upper, self.upper_inc, other.upper, other.upper_inc)
    }
}

impl Hash for RangeValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.range_type.hash(state);
        self.lower_inc.hash(state);
        self.upper_inc.hash(state);
        self.lower.map(f64::to_bits).hash(state);
        self.upper.map(f64::to_bits).hash(state);
        self.empty.hash(state);
    }
}

impl fmt::Display for RangeValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.empty {
            return f.write_str("empty");
        }
        f.write_str(if self.lower_inc { "[" } else { "(" })?;
        if let Some(lower) = self.lower {
            write_range_number(f, lower)?;
        }
        f.write_str(",")?;
        if let Some(upper) = self.upper {
            write_range_number(f, upper)?;
        }
        f.write_str(if self.upper_inc { "]" } else { ")" })
    }
}

/// Axis-aligned bounding box used for GiST geometric consistency checks.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct BoundingBox {
    /// Minimum x coordinate.
    pub min_x: f64,
    /// Minimum y coordinate.
    pub min_y: f64,
    /// Maximum x coordinate.
    pub max_x: f64,
    /// Maximum y coordinate.
    pub max_y: f64,
}

impl BoundingBox {
    fn from_points(points: &[(f64, f64)]) -> Option<Self> {
        let &(first_x, first_y) = points.first()?;
        let mut bbox = Self {
            min_x: first_x,
            min_y: first_y,
            max_x: first_x,
            max_y: first_y,
        };
        for &(x, y) in &points[1..] {
            bbox.min_x = bbox.min_x.min(x);
            bbox.min_y = bbox.min_y.min(y);
            bbox.max_x = bbox.max_x.max(x);
            bbox.max_y = bbox.max_y.max(y);
        }
        Some(bbox)
    }

    fn overlaps(self, other: Self) -> bool {
        self.min_x <= other.max_x
            && self.max_x >= other.min_x
            && self.min_y <= other.max_y
            && self.max_y >= other.min_y
    }

    fn contains(self, other: Self) -> bool {
        self.min_x <= other.min_x
            && self.min_y <= other.min_y
            && self.max_x >= other.max_x
            && self.max_y >= other.max_y
    }
}

impl Hash for BoundingBox {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.min_x.to_bits().hash(state);
        self.min_y.to_bits().hash(state);
        self.max_x.to_bits().hash(state);
        self.max_y.to_bits().hash(state);
    }
}

/// Runtime representation for PostgreSQL geometric values.
#[derive(Clone, Debug, PartialEq)]
pub struct GeometryValue {
    /// Geometry family.
    pub geometry_type: GeometryType,
    /// Bounding box used by v0.8 GiST operators.
    pub bbox: BoundingBox,
}

impl GeometryValue {
    /// Parse common PostgreSQL geometric literal text into a bounding box.
    #[must_use]
    pub fn parse(geometry_type: GeometryType, text: &str) -> Option<Self> {
        let nums = extract_numbers(text)?;
        let bbox = match geometry_type {
            GeometryType::Point => {
                if nums.len() < 2 {
                    return None;
                }
                BoundingBox::from_points(&[(nums[0], nums[1])])?
            }
            GeometryType::Circle => {
                if nums.len() < 3 {
                    return None;
                }
                let radius = nums[2].abs();
                BoundingBox {
                    min_x: nums[0] - radius,
                    min_y: nums[1] - radius,
                    max_x: nums[0] + radius,
                    max_y: nums[1] + radius,
                }
            }
            GeometryType::Box
            | GeometryType::Line
            | GeometryType::Lseg
            | GeometryType::Path
            | GeometryType::Polygon => {
                if nums.len() < 4 {
                    return None;
                }
                let mut points = Vec::with_capacity(nums.len() / 2);
                for pair in nums.chunks_exact(2) {
                    points.push((pair[0], pair[1]));
                }
                BoundingBox::from_points(&points)?
            }
        };
        Some(Self {
            geometry_type,
            bbox,
        })
    }

    /// `true` when bounding boxes overlap.
    #[must_use]
    pub fn overlaps(&self, other: &Self) -> bool {
        self.bbox.overlaps(other.bbox)
    }

    /// `true` when this value's bounding box contains `other`.
    #[must_use]
    pub fn contains_geometry(&self, other: &Self) -> bool {
        self.bbox.contains(other.bbox)
    }
}

impl Hash for GeometryValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.geometry_type.hash(state);
        self.bbox.hash(state);
    }
}

impl fmt::Display for GeometryValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.geometry_type {
            GeometryType::Point => write!(f, "({},{})", self.bbox.min_x, self.bbox.min_y),
            _ => write!(
                f,
                "{}(({},{}) , ({},{}))",
                self.geometry_type,
                self.bbox.min_x,
                self.bbox.min_y,
                self.bbox.max_x,
                self.bbox.max_y
            ),
        }
    }
}

/// Runtime sparse vector with one-based element indexes.
#[derive(Clone, Debug, PartialEq)]
pub struct SparseVector {
    /// Declared dense dimension count.
    pub dims: u32,
    /// Sorted, unique one-based non-zero entries.
    pub entries: Vec<(u32, f32)>,
}

impl SparseVector {
    /// Construct a sparse vector, validating dimension and entries.
    #[must_use]
    pub fn new(dims: u32, mut entries: Vec<(u32, f32)>) -> Option<Self> {
        if dims == 0 || dims > MAX_VECTOR_DIMS {
            return None;
        }
        entries.sort_unstable_by_key(|(idx, _)| *idx);
        let mut previous = None;
        for (idx, value) in &entries {
            if *idx == 0 || *idx > dims || !value.is_finite() || previous == Some(*idx) {
                return None;
            }
            previous = Some(*idx);
        }
        Some(Self { dims, entries })
    }
}

impl Eq for SparseVector {}

impl Hash for SparseVector {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.dims.hash(state);
        for (idx, value) in &self.entries {
            idx.hash(state);
            value.to_bits().hash(state);
        }
    }
}

impl fmt::Display for SparseVector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("{")?;
        for (entry_idx, (idx, value)) in self.entries.iter().enumerate() {
            if entry_idx > 0 {
                f.write_str(",")?;
            }
            write!(f, "{idx}:{value}")?;
        }
        write!(f, "}}/{}", self.dims)
    }
}

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
        /// Scaled integer payload.
        value: i64,
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

/// Conventional alias used in PostgreSQL literature.
pub type Datum = Value;

impl Value {
    /// The dynamic [`DataType`] of this value.
    #[must_use]
    pub fn data_type(&self) -> DataType {
        match self {
            Self::Null => DataType::Null,
            Self::Bool(_) => DataType::Bool,
            Self::Int16(_) => DataType::Int16,
            Self::Int32(_) => DataType::Int32,
            Self::Int64(_) => DataType::Int64,
            Self::Oid(_) => DataType::Oid,
            Self::RegClass(_) => DataType::RegClass,
            Self::RegType(_) => DataType::RegType,
            Self::PgLsn(_) => DataType::PgLsn,
            Self::Float32(_) => DataType::Float32,
            Self::Float64(_) => DataType::Float64,
            Self::Text(_) => DataType::Text { max_len: None },
            Self::Char(s) => DataType::Char {
                len: u32::try_from(s.chars().count()).ok(),
            },
            Self::Json(_) => DataType::Json,
            Self::Jsonb(_) => DataType::Jsonb,
            Self::Xml(_) => DataType::Xml,
            Self::Vector(v) => DataType::Vector {
                dims: u32::try_from(v.len()).ok(),
            },
            Self::HalfVec(v) => DataType::HalfVec {
                dims: u32::try_from(v.len()).ok(),
            },
            Self::SparseVec(v) => DataType::SparseVec { dims: Some(v.dims) },
            Self::BitVec { dims, .. } => DataType::BitVec { dims: Some(*dims) },
            Self::BitString(v) => DataType::VarBit {
                max_len: Some(v.len()),
            },
            Self::Network(v) => v.data_type(),
            Self::Bytea(_) => DataType::Bytea,
            Self::Timestamp(_) => DataType::Timestamp,
            Self::TimestampTz(_) => DataType::TimestampTz,
            Self::Date(_) => DataType::Date,
            Self::Time(_) => DataType::Time,
            Self::TimeTz { .. } => DataType::TimeTz,
            Self::Uuid(_) => DataType::Uuid,
            Self::Decimal { scale, .. } => DataType::Decimal {
                precision: None,
                scale: Some(*scale),
            },
            Self::Money(_) => DataType::Money,
            Self::Interval { .. } => DataType::Interval,
            Self::Range(v) => DataType::Range(v.range_type),
            Self::Geometry(v) => DataType::Geometry(v.geometry_type),
            Self::Array { element_type, .. } => DataType::Array(Box::new(element_type.clone())),
            Self::Record(fields) => DataType::Record(
                fields
                    .iter()
                    .map(|(name, value)| (name.clone(), value.data_type()))
                    .collect(),
            ),
        }
    }

    /// Width category used during planning: `None` for varlena values.
    #[must_use]
    pub const fn fixed_size(&self) -> Option<usize> {
        match self {
            Self::Bool(_) => Some(1),
            Self::Int16(_) => Some(2),
            Self::Int32(_)
            | Self::Float32(_)
            | Self::Date(_)
            | Self::Oid(_)
            | Self::RegClass(_)
            | Self::RegType(_) => Some(4),
            Self::Int64(_)
            | Self::Money(_)
            | Self::Float64(_)
            | Self::Time(_)
            | Self::TimeTz { .. }
            | Self::Timestamp(_)
            | Self::TimestampTz(_)
            | Self::PgLsn(_) => Some(8),
            Self::Uuid(_) => Some(16),
            _ => None,
        }
    }

    /// `true` iff this value is SQL NULL.
    #[must_use]
    pub const fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    /// Parse a PostgreSQL UUID literal into raw 16-byte storage.
    ///
    /// Accepts canonical hyphenated text and compact 32-hex-digit text.
    #[must_use]
    pub fn parse_uuid(text: &str) -> Option<[u8; 16]> {
        let mut nibbles = [0_u8; 32];
        let mut len = 0_usize;
        for byte in text.bytes() {
            if byte == b'-' {
                continue;
            }
            if len >= nibbles.len() {
                return None;
            }
            nibbles[len] = match byte {
                b'0'..=b'9' => byte - b'0',
                b'a'..=b'f' => byte - b'a' + 10,
                b'A'..=b'F' => byte - b'A' + 10,
                _ => return None,
            };
            len += 1;
        }
        if len != nibbles.len() {
            return None;
        }
        let mut out = [0_u8; 16];
        for idx in 0..out.len() {
            out[idx] = (nibbles[idx * 2] << 4) | nibbles[idx * 2 + 1];
        }
        Some(out)
    }

    /// Parse PostgreSQL hex-style `bytea` text (`\xdeadbeef`).
    #[must_use]
    pub fn parse_bytea(text: &str) -> Option<Vec<u8>> {
        let hex = text
            .strip_prefix("\\x")
            .or_else(|| text.strip_prefix("\\X"))?;
        if hex.len() % 2 != 0 {
            return None;
        }
        let mut out = Vec::with_capacity(hex.len() / 2);
        let bytes = hex.as_bytes();
        for idx in (0..bytes.len()).step_by(2) {
            let hi = hex_nibble(bytes[idx])?;
            let lo = hex_nibble(bytes[idx + 1])?;
            out.push((hi << 4) | lo);
        }
        Some(out)
    }

    /// Validate a basic PostgreSQL `xml` literal and return stored text.
    ///
    /// The initial XML surface accepts one well-formed document with
    /// balanced element tags, quoted attributes, comments, CDATA, and
    /// processing instructions. It intentionally does not implement DTD
    /// validation, namespaces, or XPath semantics.
    #[must_use]
    pub fn validate_xml_text(text: &str) -> Option<String> {
        let trimmed = text.trim();
        if trimmed.is_empty() || !xml_document_is_well_formed(trimmed) {
            return None;
        }
        Some(trimmed.to_owned())
    }

    /// Return `true` when text is one well-formed XML document.
    ///
    /// Validation is local only: DTD declarations and external entity
    /// expansion are rejected rather than resolved.
    #[must_use]
    pub fn xml_document_is_well_formed(text: &str) -> bool {
        xml_document_is_well_formed(text)
    }

    /// Return `true` when text is well-formed XML content.
    ///
    /// Content may contain more than one top-level element. The same local-only
    /// security policy as [`Self::xml_document_is_well_formed`] applies.
    #[must_use]
    pub fn xml_content_is_well_formed(text: &str) -> bool {
        xml_content_is_well_formed(text)
    }

    /// Parse a pgvector-style vector literal, such as `[1,2.5,-3]`.
    ///
    /// Elements are `f32` and must be finite. Empty vectors and values
    /// above [`MAX_VECTOR_DIMS`] are rejected.
    #[must_use]
    pub fn parse_vector(text: &str) -> Option<Self> {
        let trimmed = text.trim();
        let inner = trimmed.strip_prefix('[')?.strip_suffix(']')?.trim();
        if inner.is_empty() {
            return None;
        }
        let mut values = Vec::new();
        for raw in inner.split(',') {
            let element = raw.trim();
            if element.is_empty() {
                return None;
            }
            let value = element.parse::<f32>().ok()?;
            if !value.is_finite() {
                return None;
            }
            values.push(value);
        }
        let dims = u32::try_from(values.len()).ok()?;
        if dims == 0 || dims > MAX_VECTOR_DIMS {
            return None;
        }
        Some(Self::Vector(values))
    }

    /// Parse a pgvector-style `halfvec` literal. Runtime values remain
    /// finite `f32` values; the SQL type controls storage/precision policy.
    #[must_use]
    pub fn parse_halfvec(text: &str) -> Option<Self> {
        let Self::Vector(values) = Self::parse_vector(text)? else {
            return None;
        };
        Some(Self::HalfVec(values))
    }

    /// Parse a pgvector-style sparse vector literal, e.g. `{1:1,3:2}/5`.
    #[must_use]
    pub fn parse_sparsevec(text: &str) -> Option<Self> {
        let trimmed = text.trim();
        let (entries_text, dims_text) = split_once_unquoted_slash(trimmed)?;
        let dims = dims_text.trim().parse::<u32>().ok()?;
        let inner = entries_text
            .trim()
            .strip_prefix('{')?
            .strip_suffix('}')?
            .trim();
        let mut entries = Vec::new();
        if !inner.is_empty() {
            for raw in inner.split(',') {
                let (idx, value) = split_once_unquoted_colon(raw)?;
                let idx = idx.trim().parse::<u32>().ok()?;
                let value = value.trim().parse::<f32>().ok()?;
                entries.push((idx, value));
            }
        }
        SparseVector::new(dims, entries).map(Self::SparseVec)
    }

    /// Parse a dense bit-vector literal containing only `0` and `1`.
    #[must_use]
    pub fn parse_bitvec(text: &str) -> Option<Self> {
        let trimmed = text.trim();
        if trimmed.is_empty() {
            return None;
        }
        let dims = u32::try_from(trimmed.len()).ok()?;
        if dims == 0 || dims > MAX_VECTOR_DIMS {
            return None;
        }
        let mut bytes = vec![0_u8; trimmed.len().div_ceil(8)];
        for (idx, byte) in trimmed.bytes().enumerate() {
            match byte {
                b'0' => {}
                b'1' => {
                    let byte_idx = idx / 8;
                    let bit_idx = idx % 8;
                    bytes[byte_idx] |= 1_u8 << (7 - bit_idx);
                }
                _ => return None,
            }
        }
        Some(Self::BitVec { dims, bytes })
    }

    /// Parse a SQL bit-string literal containing only `0` and `1`.
    #[must_use]
    pub fn parse_bit_string(text: &str) -> Option<Self> {
        BitString::parse(text).map(Self::BitString)
    }

    /// Parse a PostgreSQL network-address literal for a target type.
    #[must_use]
    pub fn parse_network(target: &DataType, text: &str) -> Option<Self> {
        NetworkValue::parse_for_type(target, text).map(Self::Network)
    }

    /// Parse a PostgreSQL `oid` text literal.
    #[must_use]
    pub fn parse_oid_text(text: &str) -> Option<Oid> {
        let trimmed = text.trim();
        if trimmed.is_empty() || trimmed.starts_with('-') {
            return None;
        }
        let raw = trimmed.parse::<u64>().ok()?;
        u32::try_from(raw).ok().map(Oid::new)
    }

    /// Parse a PostgreSQL `pg_lsn` text literal (`HEX/HEX`).
    #[must_use]
    pub fn parse_pg_lsn_text(text: &str) -> Option<Lsn> {
        let (high, low) = text.trim().split_once('/')?;
        if high.is_empty() || low.is_empty() {
            return None;
        }
        let high = u32::try_from(u64::from_str_radix(high, 16).ok()?).ok()?;
        let low = u32::try_from(u64::from_str_radix(low, 16).ok()?).ok()?;
        Some(Lsn::new((u64::from(high) << 32) | u64::from(low)))
    }

    /// Parse PostgreSQL's common text-array form, e.g. `{1,2,NULL}`.
    ///
    /// The parser is intentionally conservative: it supports the
    /// scalar element families UltraSQL can already store in rows and
    /// rejects malformed input instead of guessing.
    #[must_use]
    pub fn parse_array(element_type: DataType, text: &str) -> Option<Self> {
        let trimmed = text.trim();
        if !(trimmed.starts_with('{') && trimmed.ends_with('}')) {
            return None;
        }
        let inner = &trimmed[1..trimmed.len().checked_sub(1)?];
        let elements = if inner.is_empty() {
            Vec::new()
        } else {
            split_array_elements(inner)?
                .into_iter()
                .map(|part| parse_array_element(&element_type, part))
                .collect::<Option<Vec<_>>>()?
        };
        let value = Self::Array {
            element_type,
            elements,
        };
        value.array_dimensions()?;
        Some(value)
    }

    /// Borrowed `i64` view if this is an integer type, widening from
    /// narrower integers losslessly. `None` for non-integers.
    #[must_use]
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Int16(v) => Some(i64::from(*v)),
            Self::Int32(v) => Some(i64::from(*v)),
            Self::Int64(v) => Some(*v),
            Self::Oid(v) | Self::RegClass(v) | Self::RegType(v) => Some(i64::from(v.raw())),
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
            Self::Text(s) | Self::Char(s) => Some(s.as_str()),
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

    /// Borrowed array view. `None` for non-array values.
    #[must_use]
    pub fn as_array(&self) -> Option<(&DataType, &[Value])> {
        match self {
            Self::Array {
                element_type,
                elements,
            } => Some((element_type, elements.as_slice())),
            _ => None,
        }
    }

    /// Dimensions of a rectangular PostgreSQL array value.
    ///
    /// Returns `None` for non-array values and for ragged nested arrays.
    /// Empty nested arrays report the dimensions that can be proven from
    /// stored values, matching the runtime representation's lack of
    /// explicit dimension headers.
    #[must_use]
    pub fn array_dimensions(&self) -> Option<Vec<usize>> {
        match self {
            Self::Array {
                element_type,
                elements,
            } => array_dimensions(element_type, elements),
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

/// Return `true` when `text` is one locally parsed XML document.
///
/// The parser rejects DTD declarations and unknown entity references. It never
/// resolves external entities, so validation cannot read local files or touch
/// the network.
#[must_use]
pub fn xml_document_is_well_formed(text: &str) -> bool {
    let text = text.trim();
    if text.is_empty() {
        return false;
    }
    let mut stack: Vec<String> = Vec::new();
    let mut cursor = 0_usize;
    let mut saw_root = false;
    let mut root_closed = false;

    while let Some(relative) = text[cursor..].find('<') {
        let open = cursor + relative;
        let text_segment = &text[cursor..open];
        if !xml_text_segment_is_well_formed(text_segment) {
            return false;
        }
        if stack.is_empty() && !saw_root && !text_segment.trim().is_empty() {
            return false;
        }
        if stack.is_empty() && root_closed && !text_segment.trim().is_empty() {
            return false;
        }
        let Some(next) = text.as_bytes().get(open + 1).copied() else {
            return false;
        };
        match next {
            b'?' => {
                let Some(end) = text[open + 2..].find("?>") else {
                    return false;
                };
                cursor = open + 2 + end + 2;
            }
            b'!' if text[open..].starts_with("<!--") => {
                let Some(end) = text[open + 4..].find("-->") else {
                    return false;
                };
                cursor = open + 4 + end + 3;
            }
            b'!' if text[open..].starts_with("<![CDATA[") => {
                if stack.is_empty() {
                    return false;
                }
                let Some(end) = text[open + 9..].find("]]>") else {
                    return false;
                };
                cursor = open + 9 + end + 3;
            }
            b'!' => return false,
            b'/' => {
                let Some(close) = xml_tag_end(text, open + 2) else {
                    return false;
                };
                let name = text[open + 2..close].trim();
                if name.is_empty()
                    || name.bytes().any(|byte| byte.is_ascii_whitespace())
                    || xml_name_len(name.as_bytes()) != name.len()
                    || stack.pop().as_deref() != Some(name)
                {
                    return false;
                }
                if stack.is_empty() {
                    root_closed = true;
                }
                cursor = close + 1;
            }
            _ => {
                if root_closed {
                    return false;
                }
                let Some(close) = xml_tag_end(text, open + 1) else {
                    return false;
                };
                let mut content = text[open + 1..close].trim();
                let self_closing = content.ends_with('/');
                if self_closing {
                    content = content[..content.len() - 1].trim_end();
                }
                let name_len = xml_name_len(content.as_bytes());
                if name_len == 0 {
                    return false;
                }
                let name = &content[..name_len];
                let rest = &content[name_len..];
                if !xml_attributes_are_well_formed(rest) {
                    return false;
                }
                saw_root = true;
                if self_closing {
                    if stack.is_empty() {
                        root_closed = true;
                    }
                } else {
                    stack.push(name.to_owned());
                }
                cursor = close + 1;
            }
        }
    }

    let trailing = &text[cursor..];
    saw_root
        && stack.is_empty()
        && xml_text_segment_is_well_formed(trailing)
        && trailing.trim().is_empty()
}

/// Return `true` when `text` is locally parsed XML content.
///
/// Content accepts more than one top-level element by validating it inside a
/// synthetic wrapper. DTD declarations and unknown entity references remain
/// rejected.
#[must_use]
pub fn xml_content_is_well_formed(text: &str) -> bool {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return false;
    }
    let wrapped = format!("<__ultrasql_xml_content>{trimmed}</__ultrasql_xml_content>");
    xml_document_is_well_formed(&wrapped)
}

/// Return fragments selected by a small, deterministic XPath subset.
///
/// Supported paths are absolute child paths such as `/root/item/name` with
/// optional equality filters on element attributes:
/// `/root/item[@id="42"]`. Element wildcards, terminal `@attr`/`@*`,
/// `text()` selections, basic explicit axes, and bounded scalar functions
/// are also supported. Unsupported path syntax returns `None`. Missing matches
/// return `Some(Vec::new())`.
#[must_use]
pub fn xml_xpath_element_fragments(path: &str, document: &str) -> Option<Vec<String>> {
    xml_xpath_element_fragments_with_namespaces(path, document, &[])
}

/// Return fragments selected by the supported XPath subset using explicit
/// namespace alias-to-URI bindings.
///
/// Bindings use `(alias, uri)` pairs matching PostgreSQL's `xpath(...,
/// nsarray)` contract. Empty bindings preserve the legacy raw-name matching
/// behavior for unqualified paths.
#[must_use]
pub fn xml_xpath_element_fragments_with_namespaces(
    path: &str,
    document: &str,
    namespace_bindings: &[(String, String)],
) -> Option<Vec<String>> {
    let document = document.trim();
    if !xml_document_is_well_formed(document) {
        return None;
    }
    match path.trim() {
        "true()" => return Some(vec!["true".to_owned()]),
        "false()" => return Some(vec!["false".to_owned()]),
        _ => {}
    }
    if let Some(inner_path) = xml_xpath_function_argument(path, "string") {
        let matches =
            xml_xpath_element_fragments_with_namespaces(inner_path, document, namespace_bindings)?;
        return Some(vec![matches.first().map_or_else(String::new, |fragment| {
            xml_xpath_string_value(fragment)
        })]);
    }
    if let Some(inner_path) = xml_xpath_function_argument(path, "boolean") {
        let matches =
            xml_xpath_element_fragments_with_namespaces(inner_path, document, namespace_bindings)?;
        return Some(vec![(!matches.is_empty()).to_string()]);
    }
    if let Some(inner_path) = xml_xpath_function_argument(path, "not") {
        let matches =
            xml_xpath_element_fragments_with_namespaces(inner_path, document, namespace_bindings)?;
        return Some(vec![matches.is_empty().to_string()]);
    }
    if let Some(inner_path) = xml_xpath_function_argument(path, "name") {
        let matches =
            xml_xpath_element_fragments_with_namespaces(inner_path, document, namespace_bindings)?;
        return Some(vec![matches.first().map_or_else(String::new, |fragment| {
            xml_xpath_name_value(fragment)
        })]);
    }
    if let Some(inner_path) = xml_xpath_function_argument(path, "local-name") {
        let matches =
            xml_xpath_element_fragments_with_namespaces(inner_path, document, namespace_bindings)?;
        return Some(vec![matches.first().map_or_else(String::new, |fragment| {
            xml_xpath_local_name_value(fragment)
        })]);
    }
    if let Some(inner_path) = xml_xpath_function_argument(path, "normalize-space") {
        let matches =
            xml_xpath_element_fragments_with_namespaces(inner_path, document, namespace_bindings)?;
        return Some(vec![matches.first().map_or_else(String::new, |fragment| {
            xml_xpath_normalize_space_value(fragment)
        })]);
    }
    if let Some(inner_path) = xml_xpath_function_argument(path, "string-length") {
        let matches =
            xml_xpath_element_fragments_with_namespaces(inner_path, document, namespace_bindings)?;
        return Some(vec![
            matches
                .first()
                .map_or_else(String::new, |fragment| xml_xpath_string_value(fragment))
                .chars()
                .count()
                .to_string(),
        ]);
    }
    if let Some((inner_path, needle)) =
        xml_xpath_string_literal_function_arguments(path, "contains")
    {
        let matches =
            xml_xpath_element_fragments_with_namespaces(inner_path, document, namespace_bindings)?;
        let value = xml_xpath_first_string_value(&matches);
        return Some(vec![value.contains(&needle).to_string()]);
    }
    if let Some((inner_path, prefix)) =
        xml_xpath_string_literal_function_arguments(path, "starts-with")
    {
        let matches =
            xml_xpath_element_fragments_with_namespaces(inner_path, document, namespace_bindings)?;
        let value = xml_xpath_first_string_value(&matches);
        return Some(vec![value.starts_with(&prefix).to_string()]);
    }
    if let Some((inner_path, delimiter)) =
        xml_xpath_string_literal_function_arguments(path, "substring-before")
    {
        let matches =
            xml_xpath_element_fragments_with_namespaces(inner_path, document, namespace_bindings)?;
        let value = xml_xpath_first_string_value(&matches);
        let before = if delimiter.is_empty() {
            String::new()
        } else {
            value
                .find(&delimiter)
                .map_or_else(String::new, |idx| value[..idx].to_owned())
        };
        return Some(vec![before]);
    }
    if let Some((inner_path, delimiter)) =
        xml_xpath_string_literal_function_arguments(path, "substring-after")
    {
        let matches =
            xml_xpath_element_fragments_with_namespaces(inner_path, document, namespace_bindings)?;
        let value = xml_xpath_first_string_value(&matches);
        let after = if delimiter.is_empty() {
            value
        } else {
            value
                .find(&delimiter)
                .map_or_else(String::new, |idx| value[idx + delimiter.len()..].to_owned())
        };
        return Some(vec![after]);
    }
    if let Some(arguments) = xml_xpath_concat_arguments(path) {
        let mut out = String::new();
        for argument in arguments {
            match argument {
                XmlXPathValueArgument::Path(inner_path) => {
                    let matches = xml_xpath_element_fragments_with_namespaces(
                        inner_path,
                        document,
                        namespace_bindings,
                    )?;
                    out.push_str(&xml_xpath_first_string_value(&matches));
                }
                XmlXPathValueArgument::Literal(value) => out.push_str(&value),
            }
        }
        return Some(vec![out]);
    }
    if let Some(inner_path) = xml_xpath_function_argument(path, "number") {
        let number = xml_xpath_number_function_value(inner_path, document, namespace_bindings)?;
        return Some(vec![xml_xpath_format_number(number)]);
    }
    if let Some(inner_path) = xml_xpath_function_argument(path, "floor") {
        let number = xml_xpath_number_function_value(inner_path, document, namespace_bindings)?;
        return Some(vec![xml_xpath_format_number(number.floor())]);
    }
    if let Some(inner_path) = xml_xpath_function_argument(path, "ceiling") {
        let number = xml_xpath_number_function_value(inner_path, document, namespace_bindings)?;
        return Some(vec![xml_xpath_format_number(number.ceil())]);
    }
    if let Some(inner_path) = xml_xpath_function_argument(path, "round") {
        let number = xml_xpath_number_function_value(inner_path, document, namespace_bindings)?;
        return Some(vec![xml_xpath_format_number(xml_xpath_round_number(
            number,
        ))]);
    }
    if let Some(inner_path) = xml_xpath_function_argument(path, "sum") {
        let matches =
            xml_xpath_element_fragments_with_namespaces(inner_path, document, namespace_bindings)?;
        return Some(vec![xml_xpath_format_number(xml_xpath_sum_value(&matches))]);
    }
    if let Some(inner_path) = xml_xpath_count_argument(path) {
        let matches =
            xml_xpath_element_fragments_with_namespaces(inner_path, document, namespace_bindings)?;
        return Some(vec![matches.len().to_string()]);
    }
    let steps = parse_xml_path(path)?;
    let root = xml_root_element(document)?;
    let mut current = match &steps[0] {
        XmlPathStep::Element {
            descendant,
            position_filter,
            ..
        } => {
            if *descendant {
                let mut matches = Vec::new();
                if xml_step_matches(&root, &steps[0], namespace_bindings) {
                    matches.push(root.clone());
                }
                let mut step_matches =
                    |element: &XmlElement| xml_step_matches(element, &steps[0], namespace_bindings);
                collect_xml_descendant_elements(document, &root, &mut matches, &mut step_matches);
                xml_apply_position_filter(matches, position_filter.as_ref())
            } else if xml_step_matches(&root, &steps[0], namespace_bindings) {
                xml_apply_position_filter(vec![root], position_filter.as_ref())
            } else {
                Vec::new()
            }
        }
        XmlPathStep::SelfNode => vec![root],
        XmlPathStep::Attribute(_) | XmlPathStep::Text => return None,
    };
    for (idx, step) in steps[1..].iter().enumerate() {
        let terminal = idx + 2 == steps.len();
        match step {
            XmlPathStep::Element {
                descendant,
                position_filter,
                ..
            } => {
                let mut next = Vec::new();
                for element in &current {
                    if *descendant {
                        let mut matches = Vec::new();
                        let mut step_matches =
                            |child: &XmlElement| xml_step_matches(child, step, namespace_bindings);
                        collect_xml_descendant_elements(
                            document,
                            element,
                            &mut matches,
                            &mut step_matches,
                        );
                        next.extend(xml_apply_position_filter(matches, position_filter.as_ref()));
                    } else {
                        let matches = xml_direct_child_elements(document, element)
                            .into_iter()
                            .filter(|child| xml_step_matches(child, step, namespace_bindings))
                            .collect();
                        next.extend(xml_apply_position_filter(matches, position_filter.as_ref()));
                    }
                }
                current = next;
                if current.is_empty() {
                    break;
                }
            }
            XmlPathStep::Attribute(name) if terminal => {
                return Some(
                    current
                        .iter()
                        .flat_map(|element| {
                            element
                                .attrs
                                .iter()
                                .filter(|(attr_name, _)| {
                                    xml_attribute_matches(
                                        attr_name,
                                        &element.namespaces,
                                        name,
                                        namespace_bindings,
                                    )
                                })
                                .map(|(_, value)| value.clone())
                        })
                        .collect(),
                );
            }
            XmlPathStep::Text if terminal => {
                return Some(
                    current
                        .iter()
                        .filter_map(|element| xml_direct_text(document, element))
                        .collect(),
                );
            }
            XmlPathStep::SelfNode => {}
            XmlPathStep::Attribute(_) | XmlPathStep::Text => return None,
        }
    }
    Some(
        current
            .into_iter()
            .map(|element| document[element.open_start..element.close_end].to_owned())
            .collect(),
    )
}

fn xml_xpath_count_argument(path: &str) -> Option<&str> {
    xml_xpath_function_argument(path, "count")
}

fn xml_xpath_function_argument<'a>(path: &'a str, function: &str) -> Option<&'a str> {
    let trimmed = path.trim();
    let inner = trimmed
        .strip_prefix(function)?
        .trim_start()
        .strip_prefix('(')?
        .strip_suffix(')')?
        .trim();
    inner.starts_with('/').then_some(inner)
}

fn xml_xpath_string_literal_function_arguments<'a>(
    path: &'a str,
    function: &str,
) -> Option<(&'a str, String)> {
    let trimmed = path.trim();
    let inner = trimmed
        .strip_prefix(function)?
        .trim_start()
        .strip_prefix('(')?
        .strip_suffix(')')?
        .trim();
    let comma = xml_xpath_top_level_comma(inner)?;
    let left = inner[..comma].trim();
    let right = inner[comma + 1..].trim();
    let literal = unquote_xml_path_literal(right)?;
    left.starts_with('/').then_some((left, literal))
}

#[derive(Debug)]
enum XmlXPathValueArgument<'a> {
    Path(&'a str),
    Literal(String),
}

fn xml_xpath_concat_arguments(path: &str) -> Option<Vec<XmlXPathValueArgument<'_>>> {
    let trimmed = path.trim();
    let inner = trimmed
        .strip_prefix("concat")?
        .trim_start()
        .strip_prefix('(')?
        .strip_suffix(')')?
        .trim();
    let parts = xml_xpath_top_level_comma_split(inner)?;
    (parts.len() >= 2)
        .then(|| {
            parts
                .into_iter()
                .map(xml_xpath_value_argument)
                .collect::<Option<Vec<_>>>()
        })
        .flatten()
}

fn xml_xpath_value_argument(argument: &str) -> Option<XmlXPathValueArgument<'_>> {
    let argument = argument.trim();
    if argument.starts_with('/') {
        Some(XmlXPathValueArgument::Path(argument))
    } else {
        unquote_xml_path_literal(argument).map(XmlXPathValueArgument::Literal)
    }
}

fn xml_xpath_top_level_comma_split(text: &str) -> Option<Vec<&str>> {
    let mut parts = Vec::new();
    let mut start = 0;
    let mut quote = None;
    for (idx, ch) in text.char_indices() {
        match quote {
            Some(active) if ch == active => quote = None,
            Some(_) => {}
            None if matches!(ch, '\'' | '"') => quote = Some(ch),
            None if ch == ',' => {
                let part = text[start..idx].trim();
                if part.is_empty() {
                    return None;
                }
                parts.push(part);
                start = idx + ch.len_utf8();
            }
            None => {}
        }
    }
    if quote.is_some() {
        return None;
    }
    let part = text[start..].trim();
    if part.is_empty() {
        return None;
    }
    parts.push(part);
    Some(parts)
}

fn xml_xpath_top_level_comma(text: &str) -> Option<usize> {
    let mut quote = None;
    for (idx, ch) in text.char_indices() {
        match quote {
            Some(active) if ch == active => quote = None,
            Some(_) => {}
            None if matches!(ch, '\'' | '"') => quote = Some(ch),
            None if ch == ',' => return Some(idx),
            None => {}
        }
    }
    None
}

#[derive(Clone, Debug)]
enum XmlPathStep {
    Element {
        name: String,
        attr_filter: Option<(String, String)>,
        position_filter: Option<XmlPositionPredicate>,
        descendant: bool,
    },
    Attribute(String),
    Text,
    SelfNode,
}

#[derive(Clone, Debug)]
enum XmlPositionPredicate {
    Index(usize),
    Last,
}

#[derive(Clone, Debug)]
struct XmlElement {
    name: String,
    attrs: Vec<(String, String)>,
    namespaces: Vec<(String, String)>,
    open_start: usize,
    content_start: usize,
    close_start: usize,
    close_end: usize,
}

fn parse_xml_path(path: &str) -> Option<Vec<XmlPathStep>> {
    let path = path.trim();
    if !path.starts_with('/') {
        return None;
    }
    let mut steps = Vec::new();
    let mut cursor = 0_usize;
    while cursor < path.len() {
        let descendant = if path[cursor..].starts_with("//") {
            cursor += 2;
            true
        } else if path[cursor..].starts_with('/') {
            cursor += 1;
            false
        } else {
            return None;
        };
        if cursor >= path.len() {
            return None;
        }
        let relative_end = path[cursor..].find('/');
        let segment_end = relative_end.map_or(path.len(), |offset| cursor + offset);
        let terminal = segment_end == path.len();
        let segment = path[cursor..segment_end].trim();
        if segment.is_empty() || segment == ".." {
            return None;
        }
        if segment == "." || segment == "self::node()" {
            if descendant {
                return None;
            }
            steps.push(XmlPathStep::SelfNode);
            cursor = segment_end;
            continue;
        }
        if segment == "text()" {
            if descendant || !terminal {
                return None;
            }
            steps.push(XmlPathStep::Text);
            cursor = segment_end;
            continue;
        }
        if let Some(attr_name) = segment.strip_prefix("attribute::") {
            if descendant || !terminal || attr_name.is_empty() || !xml_path_name_is_valid(attr_name)
            {
                return None;
            }
            steps.push(XmlPathStep::Attribute(attr_name.to_owned()));
            cursor = segment_end;
            continue;
        }
        if let Some(attr_name) = segment.strip_prefix('@') {
            if descendant || !terminal || attr_name.is_empty() || !xml_path_name_is_valid(attr_name)
            {
                return None;
            }
            steps.push(XmlPathStep::Attribute(attr_name.to_owned()));
            cursor = segment_end;
            continue;
        }
        let (segment, descendant) = if let Some(name) = segment.strip_prefix("child::") {
            (name, descendant)
        } else if let Some(name) = segment.strip_prefix("descendant::") {
            if descendant {
                return None;
            }
            (name, true)
        } else if segment.contains("::") {
            return None;
        } else {
            (segment, descendant)
        };
        let (name, attr_filter, position_filter) = if let Some(open) = segment.find('[') {
            let predicate = segment.get(open + 1..segment.len().checked_sub(1)?)?.trim();
            if !segment.ends_with(']') {
                return None;
            }
            if let Some(attr_predicate) = predicate.strip_prefix('@') {
                let (attr_name, attr_value) = attr_predicate.split_once('=')?;
                let attr_name = attr_name.trim();
                let attr_value = unquote_xml_path_literal(attr_value.trim())?;
                (
                    &segment[..open],
                    Some((attr_name.to_owned(), attr_value)),
                    None,
                )
            } else {
                (
                    &segment[..open],
                    None,
                    Some(parse_xml_position_predicate(predicate)?),
                )
            }
        } else {
            (segment, None, None)
        };
        if !xml_path_name_is_valid(name) {
            return None;
        }
        if let Some((attr_name, _)) = &attr_filter
            && !xml_path_name_is_valid(attr_name)
        {
            return None;
        }
        steps.push(XmlPathStep::Element {
            name: name.to_owned(),
            attr_filter,
            position_filter,
            descendant,
        });
        cursor = segment_end;
    }
    if steps.is_empty() { None } else { Some(steps) }
}

fn parse_xml_position_predicate(predicate: &str) -> Option<XmlPositionPredicate> {
    let predicate = predicate.trim();
    if predicate == "last()" {
        return Some(XmlPositionPredicate::Last);
    }
    if let Ok(index) = predicate.parse::<usize>() {
        return (index > 0).then_some(XmlPositionPredicate::Index(index));
    }
    let (left, right) = predicate.split_once('=')?;
    if left.trim() != "position()" {
        return None;
    }
    let right = right.trim();
    if right == "last()" {
        Some(XmlPositionPredicate::Last)
    } else {
        let index = right.parse::<usize>().ok()?;
        (index > 0).then_some(XmlPositionPredicate::Index(index))
    }
}

fn unquote_xml_path_literal(text: &str) -> Option<String> {
    let quote = text.as_bytes().first().copied()?;
    if !matches!(quote, b'\'' | b'"') || text.as_bytes().last().copied() != Some(quote) {
        return None;
    }
    Some(text[1..text.len().checked_sub(1)?].to_owned())
}

fn xml_root_element(text: &str) -> Option<XmlElement> {
    let mut cursor = 0_usize;
    while let Some(relative) = text[cursor..].find('<') {
        let open = cursor + relative;
        let next = text.as_bytes().get(open + 1).copied()?;
        match next {
            b'?' => {
                let end = text[open + 2..].find("?>")?;
                cursor = open + 2 + end + 2;
            }
            b'!' if text[open..].starts_with("<!--") => {
                let end = text[open + 4..].find("-->")?;
                cursor = open + 4 + end + 3;
            }
            b'!' => return None,
            b'/' => return None,
            _ => return read_xml_element_at(text, open, &[]),
        }
    }
    None
}

fn read_xml_element_at(
    text: &str,
    open: usize,
    inherited_namespaces: &[(String, String)],
) -> Option<XmlElement> {
    if text.as_bytes().get(open) != Some(&b'<') {
        return None;
    }
    let next = text.as_bytes().get(open + 1).copied()?;
    if matches!(next, b'/' | b'!' | b'?') {
        return None;
    }
    let tag_close = xml_tag_end(text, open + 1)?;
    let mut content = text[open + 1..tag_close].trim();
    let self_closing = content.ends_with('/');
    if self_closing {
        content = content[..content.len().checked_sub(1)?].trim_end();
    }
    let name_len = xml_name_len(content.as_bytes());
    if name_len == 0 {
        return None;
    }
    let name = content[..name_len].to_owned();
    let attrs = xml_parse_attributes(&content[name_len..])?;
    let namespaces = xml_namespace_context(inherited_namespaces, &attrs);
    let content_start = tag_close + 1;
    if self_closing {
        return Some(XmlElement {
            name,
            attrs,
            namespaces,
            open_start: open,
            content_start,
            close_start: content_start,
            close_end: content_start,
        });
    }

    let mut cursor = content_start;
    let mut same_name_depth = 1_usize;
    while let Some(relative) = text[cursor..].find('<') {
        let tag_open = cursor + relative;
        let next = text.as_bytes().get(tag_open + 1).copied()?;
        match next {
            b'?' => {
                let end = text[tag_open + 2..].find("?>")?;
                cursor = tag_open + 2 + end + 2;
            }
            b'!' if text[tag_open..].starts_with("<!--") => {
                let end = text[tag_open + 4..].find("-->")?;
                cursor = tag_open + 4 + end + 3;
            }
            b'!' if text[tag_open..].starts_with("<![CDATA[") => {
                let end = text[tag_open + 9..].find("]]>")?;
                cursor = tag_open + 9 + end + 3;
            }
            b'/' => {
                let close = xml_tag_end(text, tag_open + 2)?;
                let closing_name = text[tag_open + 2..close].trim();
                if closing_name == name {
                    same_name_depth = same_name_depth.checked_sub(1)?;
                    if same_name_depth == 0 {
                        return Some(XmlElement {
                            name,
                            attrs,
                            namespaces,
                            open_start: open,
                            content_start,
                            close_start: tag_open,
                            close_end: close + 1,
                        });
                    }
                }
                cursor = close + 1;
            }
            _ => {
                let child_close = xml_tag_end(text, tag_open + 1)?;
                let mut child_content = text[tag_open + 1..child_close].trim();
                let child_self_closing = child_content.ends_with('/');
                if child_self_closing {
                    child_content = child_content[..child_content.len().checked_sub(1)?].trim_end();
                }
                let child_name_len = xml_name_len(child_content.as_bytes());
                if child_name_len == 0 {
                    return None;
                }
                if child_content[..child_name_len] == name && !child_self_closing {
                    same_name_depth = same_name_depth.checked_add(1)?;
                }
                cursor = child_close + 1;
            }
        }
    }
    None
}

fn xml_direct_child_elements(text: &str, parent: &XmlElement) -> Vec<XmlElement> {
    let mut out = Vec::new();
    let mut cursor = parent.content_start;
    while cursor < parent.close_start {
        let Some(relative) = text[cursor..parent.close_start].find('<') else {
            break;
        };
        let open = cursor + relative;
        let Some(next) = text.as_bytes().get(open + 1).copied() else {
            break;
        };
        match next {
            b'?' => {
                let Some(end) = text[open + 2..parent.close_start].find("?>") else {
                    break;
                };
                cursor = open + 2 + end + 2;
            }
            b'!' if text[open..].starts_with("<!--") => {
                let Some(end) = text[open + 4..parent.close_start].find("-->") else {
                    break;
                };
                cursor = open + 4 + end + 3;
            }
            b'!' if text[open..].starts_with("<![CDATA[") => {
                let Some(end) = text[open + 9..parent.close_start].find("]]>") else {
                    break;
                };
                cursor = open + 9 + end + 3;
            }
            b'/' => break,
            _ => {
                let Some(element) = read_xml_element_at(text, open, &parent.namespaces) else {
                    break;
                };
                cursor = element.close_end;
                out.push(element);
            }
        }
    }
    out
}

fn collect_xml_descendant_elements<F>(
    text: &str,
    parent: &XmlElement,
    out: &mut Vec<XmlElement>,
    matches: &mut F,
) where
    F: FnMut(&XmlElement) -> bool,
{
    for child in xml_direct_child_elements(text, parent) {
        if matches(&child) {
            out.push(child.clone());
        }
        collect_xml_descendant_elements(text, &child, out, matches);
    }
}

fn xml_apply_position_filter(
    elements: Vec<XmlElement>,
    filter: Option<&XmlPositionPredicate>,
) -> Vec<XmlElement> {
    match filter {
        None => elements,
        Some(XmlPositionPredicate::Index(index)) => elements
            .into_iter()
            .nth(index.saturating_sub(1))
            .into_iter()
            .collect(),
        Some(XmlPositionPredicate::Last) => elements.into_iter().last().into_iter().collect(),
    }
}

fn xml_direct_text(text: &str, element: &XmlElement) -> Option<String> {
    let mut out = String::new();
    let mut cursor = element.content_start;
    while cursor < element.close_start {
        let Some(relative) = text[cursor..element.close_start].find('<') else {
            out.push_str(&text[cursor..element.close_start]);
            break;
        };
        let open = cursor + relative;
        out.push_str(&text[cursor..open]);
        let next = text.as_bytes().get(open + 1).copied()?;
        match next {
            b'?' => {
                let end = text[open + 2..element.close_start].find("?>")?;
                cursor = open + 2 + end + 2;
            }
            b'!' if text[open..].starts_with("<!--") => {
                let end = text[open + 4..element.close_start].find("-->")?;
                cursor = open + 4 + end + 3;
            }
            b'!' if text[open..].starts_with("<![CDATA[") => {
                let end = text[open + 9..element.close_start].find("]]>")?;
                out.push_str(&text[open + 9..open + 9 + end]);
                cursor = open + 9 + end + 3;
            }
            b'/' => break,
            _ => {
                let child = read_xml_element_at(text, open, &element.namespaces)?;
                cursor = child.close_end;
            }
        }
    }
    (!out.is_empty()).then_some(out)
}

fn xml_xpath_string_value(fragment: &str) -> String {
    let trimmed = fragment.trim();
    let Some(root) = xml_root_element(trimmed) else {
        return fragment.to_owned();
    };
    let mut out = String::new();
    xml_collect_string_value(trimmed, &root, &mut out);
    out
}

fn xml_xpath_first_string_value(matches: &[String]) -> String {
    matches
        .first()
        .map_or_else(String::new, |fragment| xml_xpath_string_value(fragment))
}

fn xml_xpath_number_function_value(
    inner_path: &str,
    document: &str,
    namespace_bindings: &[(String, String)],
) -> Option<f64> {
    let matches =
        xml_xpath_element_fragments_with_namespaces(inner_path, document, namespace_bindings)?;
    let value = xml_xpath_first_string_value(&matches);
    Some(value.trim().parse::<f64>().unwrap_or(f64::NAN))
}

fn xml_xpath_format_number(value: f64) -> String {
    if value.is_nan() {
        "NaN".to_owned()
    } else if value.is_infinite() && value.is_sign_positive() {
        "Infinity".to_owned()
    } else if value.is_infinite() {
        "-Infinity".to_owned()
    } else {
        value.to_string()
    }
}

fn xml_xpath_round_number(value: f64) -> f64 {
    if value.is_finite() {
        (value + 0.5).floor()
    } else {
        value
    }
}

fn xml_xpath_sum_value(matches: &[String]) -> f64 {
    let mut sum = 0.0_f64;
    for fragment in matches {
        let value = xml_xpath_string_value(fragment);
        let Ok(number) = value.trim().parse::<f64>() else {
            return f64::NAN;
        };
        sum += number;
    }
    sum
}

fn xml_collect_string_value(text: &str, element: &XmlElement, out: &mut String) {
    let mut cursor = element.content_start;
    while cursor < element.close_start {
        let Some(relative) = text[cursor..element.close_start].find('<') else {
            out.push_str(&text[cursor..element.close_start]);
            break;
        };
        let open = cursor + relative;
        out.push_str(&text[cursor..open]);
        let Some(next) = text.as_bytes().get(open + 1).copied() else {
            break;
        };
        match next {
            b'?' => {
                let Some(end) = text[open + 2..element.close_start].find("?>") else {
                    break;
                };
                cursor = open + 2 + end + 2;
            }
            b'!' if text[open..].starts_with("<!--") => {
                let Some(end) = text[open + 4..element.close_start].find("-->") else {
                    break;
                };
                cursor = open + 4 + end + 3;
            }
            b'!' if text[open..].starts_with("<![CDATA[") => {
                let Some(end) = text[open + 9..element.close_start].find("]]>") else {
                    break;
                };
                out.push_str(&text[open + 9..open + 9 + end]);
                cursor = open + 9 + end + 3;
            }
            b'/' => break,
            _ => {
                let Some(child) = read_xml_element_at(text, open, &element.namespaces) else {
                    break;
                };
                xml_collect_string_value(text, &child, out);
                cursor = child.close_end;
            }
        }
    }
}

fn xml_xpath_name_value(fragment: &str) -> String {
    xml_root_element(fragment.trim()).map_or_else(String::new, |root| root.name)
}

fn xml_xpath_local_name_value(fragment: &str) -> String {
    let name = xml_xpath_name_value(fragment);
    if let Some((_, local)) = name.rsplit_once(':') {
        local.to_owned()
    } else {
        name
    }
}

fn xml_xpath_normalize_space_value(fragment: &str) -> String {
    let value = xml_xpath_string_value(fragment);
    let mut out = String::new();
    let mut saw_space = false;
    for ch in value.chars() {
        if ch.is_whitespace() {
            saw_space = true;
        } else {
            if saw_space && !out.is_empty() {
                out.push(' ');
            }
            out.push(ch);
            saw_space = false;
        }
    }
    out
}

fn xml_namespace_context(
    inherited: &[(String, String)],
    attrs: &[(String, String)],
) -> Vec<(String, String)> {
    let mut namespaces = inherited.to_vec();
    for (name, value) in attrs {
        if name == "xmlns" {
            xml_upsert_namespace(&mut namespaces, "", value);
        } else if let Some(prefix) = name.strip_prefix("xmlns:")
            && !prefix.is_empty()
        {
            xml_upsert_namespace(&mut namespaces, prefix, value);
        }
    }
    namespaces
}

fn xml_upsert_namespace(namespaces: &mut Vec<(String, String)>, prefix: &str, uri: &str) {
    if let Some((_, existing_uri)) = namespaces
        .iter_mut()
        .find(|(existing_prefix, _)| existing_prefix == prefix)
    {
        *existing_uri = uri.to_owned();
    } else {
        namespaces.push((prefix.to_owned(), uri.to_owned()));
    }
}

fn xml_name_matches(
    actual: &str,
    actual_namespaces: &[(String, String)],
    expected: &str,
    namespace_bindings: &[(String, String)],
    default_namespace_applies: bool,
) -> bool {
    let (expected_prefix, expected_local) = xml_split_qname(expected);
    if namespace_bindings.is_empty() || expected_prefix.is_empty() {
        return actual == expected;
    }
    let Some(expected_uri) = xml_namespace_uri(namespace_bindings, expected_prefix) else {
        return false;
    };
    let (actual_prefix, actual_local) = xml_split_qname(actual);
    if actual_local != expected_local {
        return false;
    }
    xml_namespace_uri_for_name(actual_namespaces, actual_prefix, default_namespace_applies)
        .is_some_and(|actual_uri| actual_uri == expected_uri)
}

fn xml_path_name_is_valid(name: &str) -> bool {
    !name.is_empty() && (name == "*" || xml_name_len(name.as_bytes()) == name.len())
}

fn xml_element_name_matches(
    actual: &str,
    actual_namespaces: &[(String, String)],
    expected: &str,
    namespace_bindings: &[(String, String)],
) -> bool {
    expected == "*"
        || xml_name_matches(
            actual,
            actual_namespaces,
            expected,
            namespace_bindings,
            true,
        )
}

fn xml_attribute_matches(
    actual: &str,
    actual_namespaces: &[(String, String)],
    expected: &str,
    namespace_bindings: &[(String, String)],
) -> bool {
    if expected == "*" {
        return !xml_is_namespace_attribute(actual);
    }
    xml_name_matches(
        actual,
        actual_namespaces,
        expected,
        namespace_bindings,
        false,
    )
}

fn xml_is_namespace_attribute(name: &str) -> bool {
    name == "xmlns"
        || name
            .strip_prefix("xmlns:")
            .is_some_and(|prefix| !prefix.is_empty())
}

fn xml_split_qname(name: &str) -> (&str, &str) {
    name.split_once(':')
        .map_or(("", name), |(prefix, local)| (prefix, local))
}

fn xml_namespace_uri<'a>(namespaces: &'a [(String, String)], prefix: &str) -> Option<&'a str> {
    namespaces
        .iter()
        .rev()
        .find(|(candidate, _)| candidate == prefix)
        .map(|(_, uri)| uri.as_str())
}

fn xml_namespace_uri_for_name<'a>(
    namespaces: &'a [(String, String)],
    prefix: &str,
    default_namespace_applies: bool,
) -> Option<&'a str> {
    if prefix.is_empty() && !default_namespace_applies {
        return None;
    }
    xml_namespace_uri(namespaces, prefix)
}

fn xml_step_matches(
    element: &XmlElement,
    step: &XmlPathStep,
    namespace_bindings: &[(String, String)],
) -> bool {
    let XmlPathStep::Element {
        name, attr_filter, ..
    } = step
    else {
        return false;
    };
    xml_element_name_matches(&element.name, &element.namespaces, name, namespace_bindings)
        && attr_filter
            .as_ref()
            .is_none_or(|(expected_name, expected_value)| {
                element.attrs.iter().any(|(name, value)| {
                    value == expected_value
                        && xml_attribute_matches(
                            name,
                            &element.namespaces,
                            expected_name,
                            namespace_bindings,
                        )
                })
            })
}

fn xml_tag_end(text: &str, start: usize) -> Option<usize> {
    let mut quote = None;
    for (offset, byte) in text.as_bytes().get(start..)?.iter().copied().enumerate() {
        match (quote, byte) {
            (Some(q), b) if b == q => quote = None,
            (None, b'\'' | b'"') => quote = Some(byte),
            (None, b'>') => return Some(start + offset),
            _ => {}
        }
    }
    None
}

fn xml_name_len(bytes: &[u8]) -> usize {
    let Some((&first, rest)) = bytes.split_first() else {
        return 0;
    };
    if !xml_name_start_byte(first) {
        return 0;
    }
    let mut len = 1_usize;
    for byte in rest {
        if !xml_name_byte(*byte) {
            break;
        }
        len += 1;
    }
    len
}

fn xml_name_start_byte(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || matches!(byte, b'_' | b':')
}

fn xml_name_byte(byte: u8) -> bool {
    xml_name_start_byte(byte) || byte.is_ascii_digit() || matches!(byte, b'-' | b'.')
}

fn xml_attributes_are_well_formed(rest: &str) -> bool {
    xml_parse_attributes(rest).is_some()
}

fn xml_parse_attributes(rest: &str) -> Option<Vec<(String, String)>> {
    let bytes = rest.as_bytes();
    let mut cursor = 0_usize;
    let mut attrs = Vec::new();
    while cursor < bytes.len() {
        while bytes
            .get(cursor)
            .is_some_and(|byte| byte.is_ascii_whitespace())
        {
            cursor += 1;
        }
        if cursor == bytes.len() {
            return Some(attrs);
        }
        let name_len = xml_name_len(&bytes[cursor..]);
        if name_len == 0 {
            return None;
        }
        let name = rest[cursor..cursor + name_len].to_owned();
        cursor += name_len;
        while bytes
            .get(cursor)
            .is_some_and(|byte| byte.is_ascii_whitespace())
        {
            cursor += 1;
        }
        if bytes.get(cursor) != Some(&b'=') {
            return None;
        }
        cursor += 1;
        while bytes
            .get(cursor)
            .is_some_and(|byte| byte.is_ascii_whitespace())
        {
            cursor += 1;
        }
        let Some(quote @ (b'\'' | b'"')) = bytes.get(cursor).copied() else {
            return None;
        };
        cursor += 1;
        let value_start = cursor;
        while bytes.get(cursor).is_some_and(|byte| *byte != quote) {
            if bytes[cursor] == b'<' {
                return None;
            }
            cursor += 1;
        }
        if !xml_text_segment_is_well_formed(&rest[value_start..cursor]) {
            return None;
        }
        if bytes.get(cursor) != Some(&quote) {
            return None;
        }
        attrs.push((name, rest[value_start..cursor].to_owned()));
        cursor += 1;
    }
    Some(attrs)
}

fn xml_text_segment_is_well_formed(text: &str) -> bool {
    let bytes = text.as_bytes();
    let mut cursor = 0_usize;
    while let Some(relative) = bytes[cursor..].iter().position(|byte| *byte == b'&') {
        let amp = cursor + relative;
        let Some(entity_len) = xml_entity_ref_len(&bytes[amp..]) else {
            return false;
        };
        cursor = amp + entity_len;
    }
    true
}

fn xml_entity_ref_len(bytes: &[u8]) -> Option<usize> {
    if bytes.first() != Some(&b'&') {
        return None;
    }
    let semi = bytes.iter().take(64).position(|byte| *byte == b';')?;
    if semi <= 1 {
        return None;
    }
    let body = std::str::from_utf8(&bytes[1..semi]).ok()?;
    if matches!(body, "amp" | "lt" | "gt" | "apos" | "quot") {
        return Some(semi + 1);
    }
    if let Some(hex) = body.strip_prefix("#x").or_else(|| body.strip_prefix("#X")) {
        if !hex.is_empty() && hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Some(semi + 1);
        }
    } else if let Some(dec) = body.strip_prefix('#')
        && !dec.is_empty()
        && dec.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Some(semi + 1);
    }
    None
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
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
            Self::Oid(v) | Self::RegClass(v) | Self::RegType(v) => write!(f, "{}", v.raw()),
            Self::PgLsn(v) => write!(f, "{v}"),
            Self::Float32(v) => write!(f, "{v}"),
            Self::Float64(v) => write!(f, "{v}"),
            Self::Text(s) | Self::Char(s) | Self::Json(s) | Self::Jsonb(s) | Self::Xml(s) => {
                write!(f, "{s}")
            }
            Self::Bytea(b) => {
                f.write_str("\\x")?;
                for byte in b {
                    write!(f, "{byte:02x}")?;
                }
                Ok(())
            }
            Self::Timestamp(us) => f.write_str(&format_timestamp_micros(*us)),
            Self::TimestampTz(us) => f.write_str(&format_timestamptz_micros_utc(*us)),
            Self::Date(d) => write!(f, "{}", format_date(*d)),
            Self::Time(t) => f.write_str(&format_time_micros(*t)),
            Self::TimeTz {
                micros,
                offset_seconds,
            } => f.write_str(&format_timetz(*micros, *offset_seconds)),
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
            Self::Money(v) => f.write_str(&format_money_text(*v)),
            Self::Interval {
                months,
                days,
                microseconds,
            } => write!(f, "{months}mon {days}d {microseconds}us"),
            Self::Range(v) => write!(f, "{v}"),
            Self::Geometry(v) => write!(f, "{v}"),
            Self::Vector(values) | Self::HalfVec(values) => {
                f.write_str("[")?;
                for (idx, value) in values.iter().enumerate() {
                    if idx > 0 {
                        f.write_str(",")?;
                    }
                    write!(f, "{value}")?;
                }
                f.write_str("]")
            }
            Self::SparseVec(v) => write!(f, "{v}"),
            Self::BitString(v) => write!(f, "{v}"),
            Self::Network(v) => write!(f, "{v}"),
            Self::BitVec { dims, bytes } => {
                let dims_usize = usize::try_from(*dims).map_err(|_| fmt::Error)?;
                let required_bytes = dims_usize.div_ceil(8);
                if bytes.len() < required_bytes {
                    return Err(fmt::Error);
                }
                for idx in 0..dims_usize {
                    let byte_idx = idx / 8;
                    let bit_idx = idx % 8;
                    let bit = (bytes[byte_idx] >> (7 - bit_idx)) & 1;
                    f.write_str(if bit == 1 { "1" } else { "0" })?;
                }
                Ok(())
            }
            Self::Array { elements, .. } => {
                f.write_str("{")?;
                for (idx, element) in elements.iter().enumerate() {
                    if idx > 0 {
                        f.write_str(",")?;
                    }
                    write_array_element(f, element)?;
                }
                f.write_str("}")
            }
            Self::Record(fields) => {
                f.write_str("(")?;
                for (idx, (_, value)) in fields.iter().enumerate() {
                    if idx > 0 {
                        f.write_str(",")?;
                    }
                    write!(f, "{value}")?;
                }
                f.write_str(")")
            }
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

fn parse_array_element(element_type: &DataType, raw: &str) -> Option<Value> {
    let trimmed = raw.trim();
    if trimmed.eq_ignore_ascii_case("NULL") {
        return Some(Value::Null);
    }
    if let DataType::Array(inner) = element_type {
        return Value::parse_array((**inner).clone(), trimmed);
    }
    let text = unescape_array_text(trimmed)?;
    match element_type {
        DataType::Bool => match text.to_ascii_lowercase().as_str() {
            "t" | "true" => Some(Value::Bool(true)),
            "f" | "false" => Some(Value::Bool(false)),
            _ => None,
        },
        DataType::Int16 => text.parse::<i16>().ok().map(Value::Int16),
        DataType::Int32 => text.parse::<i32>().ok().map(Value::Int32),
        DataType::Int64 => text.parse::<i64>().ok().map(Value::Int64),
        DataType::Oid => Value::parse_oid_text(&text).map(Value::Oid),
        DataType::RegClass => Value::parse_oid_text(&text).map(Value::RegClass),
        DataType::RegType => Value::parse_oid_text(&text).map(Value::RegType),
        DataType::PgLsn => Value::parse_pg_lsn_text(&text).map(Value::PgLsn),
        DataType::Float32 => text.parse::<f32>().ok().map(Value::Float32),
        DataType::Float64 => text.parse::<f64>().ok().map(Value::Float64),
        DataType::Text { .. } | DataType::TsVector | DataType::TsQuery => Some(Value::Text(text)),
        DataType::Char { len } => coerce_bpchar_text(&text, *len, false).ok().map(Value::Char),
        DataType::Json => Some(Value::Json(text)),
        DataType::Jsonb => Some(Value::Jsonb(text)),
        DataType::Xml => Value::validate_xml_text(&text).map(Value::Xml),
        DataType::Bytea => Value::parse_bytea(&text).map(Value::Bytea),
        DataType::Uuid => Value::parse_uuid(&text).map(Value::Uuid),
        DataType::Money => parse_money_text(&text).ok(),
        _ => None,
    }
}

fn split_array_elements(text: &str) -> Option<Vec<&str>> {
    let mut out = Vec::new();
    let mut start = 0;
    let mut in_string = false;
    let mut escape = false;
    let mut depth = 0_usize;
    for (idx, ch) in text.char_indices() {
        if escape {
            escape = false;
            continue;
        }
        match ch {
            '\\' if in_string => escape = true,
            '"' => in_string = !in_string,
            '{' if !in_string => {
                depth = depth.checked_add(1)?;
            }
            '}' if !in_string => {
                depth = depth.checked_sub(1)?;
            }
            ',' if !in_string && depth == 0 => {
                out.push(&text[start..idx]);
                start = idx + ch.len_utf8();
            }
            _ => {}
        }
    }
    if in_string || escape || depth != 0 {
        return None;
    }
    out.push(&text[start..]);
    Some(out)
}

fn array_dimensions(element_type: &DataType, elements: &[Value]) -> Option<Vec<usize>> {
    let mut dims = vec![elements.len()];
    if matches!(element_type, DataType::Array(_)) {
        let mut nested_dims: Option<Vec<usize>> = None;
        for element in elements {
            if element.is_null() {
                continue;
            }
            if !matches!(element, Value::Array { .. }) {
                return None;
            }
            let dims = element.array_dimensions()?;
            if let Some(expected) = &nested_dims {
                if expected != &dims {
                    return None;
                }
            } else {
                nested_dims = Some(dims);
            }
        }
        if let Some(mut nested) = nested_dims {
            dims.append(&mut nested);
        }
    }
    Some(dims)
}

fn unescape_array_text(text: &str) -> Option<String> {
    let trimmed = text.trim();
    if !(trimmed.starts_with('"') || trimmed.ends_with('"')) {
        return Some(trimmed.to_owned());
    }
    if !(trimmed.starts_with('"') && trimmed.ends_with('"')) || trimmed.len() < 2 {
        return None;
    }
    let inner = &trimmed[1..trimmed.len().checked_sub(1)?];
    let mut out = String::with_capacity(inner.len());
    let mut escape = false;
    for ch in inner.chars() {
        if escape {
            out.push(ch);
            escape = false;
        } else if ch == '\\' {
            escape = true;
        } else {
            out.push(ch);
        }
    }
    if escape {
        return None;
    }
    Some(out)
}

fn write_array_element(f: &mut fmt::Formatter<'_>, value: &Value) -> fmt::Result {
    match value {
        Value::Null => f.write_str("NULL"),
        Value::Array { .. } => write!(f, "{value}"),
        Value::Text(s) | Value::Char(s) => write_array_text(f, s),
        other => write_array_text(f, &other.to_string()),
    }
}

fn write_array_text(f: &mut fmt::Formatter<'_>, text: &str) -> fmt::Result {
    let needs_quotes = text.is_empty()
        || text.eq_ignore_ascii_case("NULL")
        || text
            .chars()
            .any(|ch| matches!(ch, ',' | '{' | '}' | '"' | '\\') || ch.is_whitespace());
    if !needs_quotes {
        return f.write_str(text);
    }
    f.write_str("\"")?;
    for ch in text.chars() {
        if matches!(ch, '"' | '\\') {
            f.write_str("\\")?;
        }
        write!(f, "{ch}")?;
    }
    f.write_str("\"")
}

fn split_once_unquoted_comma(s: &str) -> Option<(&str, &str)> {
    let idx = s.find(',')?;
    Some((&s[..idx], &s[idx + 1..]))
}

fn split_once_unquoted_slash(s: &str) -> Option<(&str, &str)> {
    let idx = s.find('/')?;
    Some((&s[..idx], &s[idx + 1..]))
}

fn split_once_unquoted_colon(s: &str) -> Option<(&str, &str)> {
    let idx = s.find(':')?;
    Some((&s[..idx], &s[idx + 1..]))
}

fn parse_range_bound(range_type: RangeType, text: &str) -> Option<Option<f64>> {
    if text.is_empty() {
        return Some(None);
    }
    let text = text.trim_matches('"').trim_matches('\'');
    match range_type {
        RangeType::Int4 | RangeType::Int8 => text.parse::<i64>().ok().map(|v| Some(v as f64)),
        RangeType::Num | RangeType::Timestamp | RangeType::TimestampTz => {
            text.parse::<f64>().ok().map(Some)
        }
        RangeType::Date => parse_date_days(text).map(|v| Some(f64::from(v))),
    }
}

fn range_is_empty(
    lower: Option<f64>,
    upper: Option<f64>,
    lower_inc: bool,
    upper_inc: bool,
) -> bool {
    match (lower, upper) {
        (Some(l), Some(u)) if l > u => true,
        (Some(l), Some(u)) if l == u => !(lower_inc && upper_inc),
        _ => false,
    }
}

fn upper_before_lower(
    upper: Option<f64>,
    upper_inc: bool,
    lower: Option<f64>,
    lower_inc: bool,
) -> bool {
    match (upper, lower) {
        (Some(u), Some(l)) if u < l => true,
        (Some(u), Some(l)) if u > l => false,
        (Some(_), Some(_)) => !(upper_inc && lower_inc),
        (None, _) | (_, None) => false,
    }
}

fn lower_covers_lower(
    container: Option<f64>,
    container_inc: bool,
    inner: Option<f64>,
    inner_inc: bool,
) -> bool {
    match (container, inner) {
        (None, _) => true,
        (Some(_), None) => false,
        (Some(c), Some(i)) if c < i => true,
        (Some(c), Some(i)) if c > i => false,
        (Some(_), Some(_)) => container_inc || !inner_inc,
    }
}

fn upper_covers_upper(
    container: Option<f64>,
    container_inc: bool,
    inner: Option<f64>,
    inner_inc: bool,
) -> bool {
    match (container, inner) {
        (None, _) => true,
        (Some(_), None) => false,
        (Some(c), Some(i)) if c > i => true,
        (Some(c), Some(i)) if c < i => false,
        (Some(_), Some(_)) => container_inc || !inner_inc,
    }
}

fn write_range_number(f: &mut fmt::Formatter<'_>, v: f64) -> fmt::Result {
    if v.fract() == 0.0 {
        write!(f, "{v:.0}")
    } else {
        write!(f, "{v}")
    }
}

fn extract_numbers(text: &str) -> Option<Vec<f64>> {
    let mut out = Vec::new();
    let mut buf = String::new();
    for ch in text.chars() {
        if ch.is_ascii_digit() || matches!(ch, '-' | '+' | '.' | 'e' | 'E') {
            buf.push(ch);
        } else if !buf.is_empty() {
            out.push(buf.parse::<f64>().ok()?);
            buf.clear();
        }
    }
    if !buf.is_empty() {
        out.push(buf.parse::<f64>().ok()?);
    }
    Some(out)
}

fn parse_date_days(text: &str) -> Option<i32> {
    let (year, month, day) = parse_date_parts(text)?;
    days_from_civil(year, month, day)
}

fn parse_date_parts(text: &str) -> Option<(i32, u32, u32)> {
    let mut parts = text.split('-');
    let year = parts.next()?.parse::<i32>().ok()?;
    let month = parts.next()?.parse::<u32>().ok()?;
    let day = parts.next()?.parse::<u32>().ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some((year, month, day))
}

/// Parse PostgreSQL ISO `DATE` text into days since UltraSQL's date epoch.
#[must_use]
pub fn parse_date_text(text: &str) -> Option<i32> {
    parse_date_days(text.trim())
}

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    reason = "civil date arithmetic; intermediate ranges are bounded by calendar algorithm"
)]
fn days_from_civil(year: i32, month: u32, day: u32) -> Option<i32> {
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    let y = year - i32::from(month <= 2);
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u32;
    let mp = if month > 2 { month - 3 } else { month + 9 };
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days_since_0000 = era * 146_097 + doe as i32 - 719_468;
    Some(days_since_0000 - 10_957)
}

fn format_date(days_since_2000_01_01: i32) -> String {
    let (year, month, day) = civil_from_days(days_since_2000_01_01);
    format!("{year:04}-{month:02}-{day:02}")
}

/// Format days since UltraSQL's date epoch as PostgreSQL ISO `DATE` text.
#[must_use]
pub fn format_date_days(days_since_2000_01_01: i32) -> String {
    format_date(days_since_2000_01_01)
}

/// Return `(year, month, day)` for days since UltraSQL's date epoch.
#[must_use]
pub fn date_parts_from_days(days_since_2000_01_01: i32) -> Option<(i32, u32, u32)> {
    let (year, month, day) = civil_from_days(days_since_2000_01_01);
    Some((year, u32::try_from(month).ok()?, u32::try_from(day).ok()?))
}

/// Format `TIME` in PostgreSQL's default ISO style.
#[must_use]
pub fn format_time_micros(micros: i64) -> String {
    if !(0..=MICROS_PER_DAY).contains(&micros) {
        return format!("{micros}us");
    }
    let hour = micros / MICROS_PER_HOUR;
    let rem = micros % MICROS_PER_HOUR;
    let minute = rem / MICROS_PER_MINUTE;
    let rem = rem % MICROS_PER_MINUTE;
    let second = rem / MICROS_PER_SECOND;
    let frac = rem % MICROS_PER_SECOND;
    format_time_parts(hour, minute, second, frac)
}

/// Format `TIMESTAMP WITHOUT TIME ZONE` in PostgreSQL ISO style.
#[must_use]
pub fn format_timestamp_micros(micros: i64) -> String {
    let days = micros.div_euclid(MICROS_PER_DAY);
    let time = micros.rem_euclid(MICROS_PER_DAY);
    let Ok(days) = i32::try_from(days) else {
        return format!("{micros}us");
    };
    format!("{} {}", format_date(days), format_time_micros(time))
}

/// Return `(year, month, day, time_micros)` for timestamp micros.
#[must_use]
pub fn timestamp_parts_from_micros(micros: i64) -> Option<(i32, u32, u32, i64)> {
    let days = i32::try_from(micros.div_euclid(MICROS_PER_DAY)).ok()?;
    let time = micros.rem_euclid(MICROS_PER_DAY);
    let (year, month, day) = date_parts_from_days(days)?;
    Some((year, month, day, time))
}

/// Format `TIMESTAMP WITH TIME ZONE` using UTC display.
#[must_use]
pub fn format_timestamptz_micros_utc(micros: i64) -> String {
    format!("{}+00", format_timestamp_micros(micros))
}

/// Format `TIMESTAMP WITH TIME ZONE` using an explicit display offset.
#[must_use]
pub fn format_timestamptz_micros_with_offset(micros: i64, offset_seconds: i32) -> Option<String> {
    let local_micros =
        micros.checked_add(i64::from(offset_seconds).checked_mul(MICROS_PER_SECOND)?)?;
    Some(format!(
        "{}{}",
        format_timestamp_micros(local_micros),
        format_timezone_offset(offset_seconds)
    ))
}

/// Format `TIMESTAMP WITH TIME ZONE` using a fixed-offset or IANA timezone.
#[must_use]
pub fn format_timestamptz_micros_in_timezone(micros: i64, timezone: &str) -> Option<String> {
    let display = timestamptz_display_in_timezone(micros, timezone)?;
    format_timestamptz_micros_with_offset(micros, display.offset_seconds)
}

/// Resolve timezone display metadata for a `TIMESTAMPTZ` instant.
#[must_use]
pub fn timestamptz_display_in_timezone(micros: i64, timezone: &str) -> Option<TimestampTzDisplay> {
    let trimmed = timezone.trim();
    if let Some(offset_seconds) = parse_timezone_offset(trimmed) {
        return Some(TimestampTzDisplay {
            local_micros: apply_timezone_offset(micros, offset_seconds)?,
            offset_seconds,
            zone_name: fixed_timezone_display_name(trimmed),
        });
    }
    let timezone = trimmed.parse::<chrono_tz::Tz>().ok()?;
    let utc = naive_datetime_from_timestamp_micros(micros)?;
    let offset = timezone.offset_from_utc_datetime(&utc);
    let offset_seconds = offset.fix().local_minus_utc();
    Some(TimestampTzDisplay {
        local_micros: apply_timezone_offset(micros, offset_seconds)?,
        offset_seconds,
        zone_name: offset.abbreviation().map(ToOwned::to_owned),
    })
}

/// Format a UTC offset in PostgreSQL text form.
#[must_use]
pub fn format_timezone_offset_seconds(offset_seconds: i32) -> String {
    format_timezone_offset(offset_seconds)
}

/// Format `TIME WITH TIME ZONE` in PostgreSQL ISO style.
#[must_use]
pub fn format_timetz(micros: i64, offset_seconds: i32) -> String {
    format!(
        "{}{}",
        format_time_micros(micros),
        format_timezone_offset(offset_seconds)
    )
}

fn format_time_parts(hour: i64, minute: i64, second: i64, frac: i64) -> String {
    if frac == 0 {
        return format!("{hour:02}:{minute:02}:{second:02}");
    }
    let mut frac_text = format!("{frac:06}");
    while frac_text.ends_with('0') {
        frac_text.pop();
    }
    format!("{hour:02}:{minute:02}:{second:02}.{frac_text}")
}

fn format_timezone_offset(offset_seconds: i32) -> String {
    let sign = if offset_seconds < 0 { '-' } else { '+' };
    let abs = offset_seconds.unsigned_abs();
    let hours = abs / 3_600;
    let minutes = (abs % 3_600) / 60;
    let seconds = abs % 60;
    if seconds != 0 {
        format!("{sign}{hours:02}:{minutes:02}:{seconds:02}")
    } else if minutes != 0 {
        format!("{sign}{hours:02}:{minutes:02}")
    } else {
        format!("{sign}{hours:02}")
    }
}

/// Parse PostgreSQL-style `TIME` text. Any numeric timezone suffix is
/// silently ignored, matching `time without time zone` coercion.
#[must_use]
pub fn parse_time_text(text: &str) -> Option<i64> {
    parse_time_and_optional_offset(text).map(|(micros, _)| micros)
}

/// Parse PostgreSQL ISO `TIMESTAMP WITHOUT TIME ZONE` text into
/// microseconds since UltraSQL's timestamp epoch.
#[must_use]
pub fn parse_timestamp_text(text: &str) -> Option<i64> {
    let (date, time) = split_timestamp_text(text)?;
    let days = i64::from(parse_date_text(date)?);
    let micros = parse_time_text(time)?;
    days.checked_mul(MICROS_PER_DAY)?.checked_add(micros)
}

/// Parse PostgreSQL-style `TIMESTAMPTZ` text into UTC microseconds since
/// UltraSQL's timestamp epoch.
#[must_use]
pub fn parse_timestamptz_text(text: &str) -> Option<i64> {
    let (date, time) = split_timestamp_text(text)?;
    let days = i64::from(parse_date_text(date)?);
    let (_, time_token, zone_token) = split_time_and_optional_zone(time)?;
    let micros = parse_time_token(time_token)?;
    let offset_seconds = match zone_token {
        Some(zone) => parse_timezone_offset(zone)
            .or_else(|| parse_named_timezone_offset(date, micros, zone))?,
        None => 0,
    };
    days.checked_mul(MICROS_PER_DAY)?
        .checked_add(micros)?
        .checked_sub(i64::from(offset_seconds).checked_mul(MICROS_PER_SECOND)?)
}

/// Parse PostgreSQL-style `TIMETZ` text into time-of-day and UTC offset.
#[must_use]
pub fn parse_timetz_text(text: &str) -> Option<(i64, i32)> {
    parse_time_and_optional_offset(text).map(|(micros, offset)| (micros, offset.unwrap_or(0)))
}

fn parse_time_and_optional_offset(text: &str) -> Option<(i64, Option<i32>)> {
    let (date_token, time_token, zone_token) = split_time_and_optional_zone(text)?;
    let micros = parse_time_token(time_token)?;
    let offset = match zone_token {
        Some(zone) => Some(parse_timezone_offset(zone).or_else(|| {
            date_token.and_then(|date| parse_named_timezone_offset(date, micros, zone))
        })?),
        None => None,
    };
    Some((micros, offset))
}

fn split_time_and_optional_zone(text: &str) -> Option<(Option<&str>, &str, Option<&str>)> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return None;
    }
    let tokens: Vec<&str> = trimmed.split_whitespace().collect();
    let (date_token, time_token, zone_token) = match tokens.as_slice() {
        [single] => {
            let (time, zone) = split_inline_timezone(single);
            (None, time, zone)
        }
        [first, second] if looks_like_iso_date(first) => (Some(*first), *second, None),
        [first, second] => (None, *first, Some(*second)),
        [first, second, third, ..] if looks_like_iso_date(first) => {
            (Some(*first), *second, Some(*third))
        }
        _ => return None,
    };
    Some((date_token, time_token, zone_token))
}

fn looks_like_iso_date(text: &str) -> bool {
    let bytes = text.as_bytes();
    bytes.len() >= 10 && bytes.get(4) == Some(&b'-') && bytes.get(7) == Some(&b'-')
}

fn split_timestamp_text(text: &str) -> Option<(&str, &str)> {
    let trimmed = text.trim();
    let split_at = trimmed
        .char_indices()
        .find_map(|(idx, ch)| (ch == 'T' || ch.is_ascii_whitespace()).then_some(idx))?;
    let date = trimmed[..split_at].trim();
    let time =
        trimmed[split_at..].trim_start_matches(|ch: char| ch == 'T' || ch.is_ascii_whitespace());
    (!date.is_empty() && !time.is_empty()).then_some((date, time))
}

fn split_inline_timezone(token: &str) -> (&str, Option<&str>) {
    let mut split_at = None;
    for (idx, ch) in token.char_indices().skip(1) {
        if ch == '+' || ch == '-' {
            split_at = Some(idx);
        }
    }
    split_at.map_or((token, None), |idx| (&token[..idx], Some(&token[idx..])))
}

fn parse_time_token(token: &str) -> Option<i64> {
    let mut parts = token.splitn(3, ':');
    let hour_text = parts.next()?;
    let minute_text = parts.next()?;
    let second_text = parts.next().unwrap_or("0");
    let hour: i64 = hour_text.parse().ok()?;
    let minute: i64 = minute_text.parse().ok()?;
    let (second_part, frac_part) = second_text
        .split_once('.')
        .map_or((second_text, ""), |(sec, frac)| (sec, frac));
    let second: i64 = second_part.parse().ok()?;
    if !(0..=24).contains(&hour) || !(0..=59).contains(&minute) || !(0..=59).contains(&second) {
        return None;
    }
    let mut frac_micros = 0_i64;
    let mut scale = 100_000_i64;
    for ch in frac_part.chars().take(6) {
        let digit = i64::from(ch.to_digit(10)?);
        frac_micros = frac_micros.checked_add(digit.checked_mul(scale)?)?;
        scale /= 10;
    }
    if hour == 24 && (minute != 0 || second != 0 || frac_micros != 0) {
        return None;
    }
    hour.checked_mul(MICROS_PER_HOUR)?
        .checked_add(minute.checked_mul(MICROS_PER_MINUTE)?)?
        .checked_add(second.checked_mul(MICROS_PER_SECOND)?)?
        .checked_add(frac_micros)
}

fn parse_timezone_offset(token: &str) -> Option<i32> {
    let lower = token.to_ascii_lowercase();
    if matches!(lower.as_str(), "z" | "zulu" | "utc") {
        return Some(0);
    }
    if let Some(offset) = parse_timezone_abbreviation(&lower) {
        return Some(offset);
    }
    let sign = match token.as_bytes().first()? {
        b'+' => 1_i32,
        b'-' => -1_i32,
        _ => return None,
    };
    let body = &token[1..];
    let (hours, minutes, seconds) = if body.contains(':') {
        let mut parts = body.split(':');
        let hours = parts.next()?.parse::<i32>().ok()?;
        let minutes = parts.next().unwrap_or("0").parse::<i32>().ok()?;
        let seconds = parts.next().unwrap_or("0").parse::<i32>().ok()?;
        if parts.next().is_some() {
            return None;
        }
        (hours, minutes, seconds)
    } else if body.len() > 2 {
        let hours = body[..body.len() - 2].parse::<i32>().ok()?;
        let minutes = body[body.len() - 2..].parse::<i32>().ok()?;
        (hours, minutes, 0)
    } else {
        (body.parse::<i32>().ok()?, 0, 0)
    };
    if !(0..=15).contains(&hours) || !(0..=59).contains(&minutes) || !(0..=59).contains(&seconds) {
        return None;
    }
    let total = hours
        .checked_mul(3_600)?
        .checked_add(minutes.checked_mul(60)?)?
        .checked_add(seconds)?;
    sign.checked_mul(total)
}

fn parse_timezone_abbreviation(lower: &str) -> Option<i32> {
    let hours = match lower {
        "gmt" | "ut" | "wet" => 0,
        "west" | "cet" => 1,
        "cest" | "eet" => 2,
        "eest" => 3,
        "edt" => -4,
        "est" | "cdt" => -5,
        "cst" | "mdt" => -6,
        "mst" | "pdt" => -7,
        "pst" => -8,
        _ => return None,
    };
    Some(hours * 3_600)
}

fn apply_timezone_offset(micros: i64, offset_seconds: i32) -> Option<i64> {
    micros.checked_add(i64::from(offset_seconds).checked_mul(MICROS_PER_SECOND)?)
}

fn fixed_timezone_display_name(token: &str) -> Option<String> {
    let lower = token.to_ascii_lowercase();
    if matches!(lower.as_str(), "z" | "zulu" | "utc") {
        return Some("UTC".to_owned());
    }
    if parse_timezone_abbreviation(&lower).is_some()
        && !matches!(token.as_bytes().first(), Some(b'+' | b'-'))
    {
        return Some(token.to_ascii_uppercase());
    }
    None
}

fn naive_datetime_from_timestamp_micros(micros: i64) -> Option<chrono::NaiveDateTime> {
    let days = micros.div_euclid(MICROS_PER_DAY);
    let time_micros = micros.rem_euclid(MICROS_PER_DAY);
    let (year, month, day) = civil_from_days(i32::try_from(days).ok()?);
    let date = NaiveDate::from_ymd_opt(year, u32::try_from(month).ok()?, u32::try_from(day).ok()?)?;
    let hour = u32::try_from(time_micros / MICROS_PER_HOUR).ok()?;
    let rem = time_micros % MICROS_PER_HOUR;
    let minute = u32::try_from(rem / MICROS_PER_MINUTE).ok()?;
    let rem = rem % MICROS_PER_MINUTE;
    let second = u32::try_from(rem / MICROS_PER_SECOND).ok()?;
    let micros = u32::try_from(rem % MICROS_PER_SECOND).ok()?;
    let time = NaiveTime::from_hms_micro_opt(hour, minute, second, micros)?;
    Some(date.and_time(time))
}

fn parse_named_timezone_offset(date_text: &str, micros: i64, zone: &str) -> Option<i32> {
    let timezone = zone.parse::<chrono_tz::Tz>().ok()?;
    let (year, month, day) = parse_date_parts(date_text)?;
    let mut date = NaiveDate::from_ymd_opt(year, month, day)?;
    let mut local_micros = micros;
    if local_micros == MICROS_PER_DAY {
        date = date.checked_add_days(Days::new(1))?;
        local_micros = 0;
    }
    let hour = u32::try_from(local_micros / MICROS_PER_HOUR).ok()?;
    let rem = local_micros % MICROS_PER_HOUR;
    let minute = u32::try_from(rem / MICROS_PER_MINUTE).ok()?;
    let rem = rem % MICROS_PER_MINUTE;
    let second = u32::try_from(rem / MICROS_PER_SECOND).ok()?;
    let micros = u32::try_from(rem % MICROS_PER_SECOND).ok()?;
    let time = NaiveTime::from_hms_micro_opt(hour, minute, second, micros)?;
    let local = date.and_time(time);
    let resolved = match timezone.from_local_datetime(&local) {
        LocalResult::Single(value) => value,
        LocalResult::Ambiguous(earliest, _) => earliest,
        LocalResult::None => return None,
    };
    Some(resolved.offset().fix().local_minus_utc())
}

/// Pack `TIMETZ` into an `i64` batch payload.
#[must_use]
pub fn pack_timetz(micros: i64, offset_seconds: i32) -> Option<i64> {
    if !(0..=MICROS_PER_DAY).contains(&micros)
        || !(-TIMETZ_OFFSET_BIAS_SECONDS..=TIMETZ_OFFSET_BIAS_SECONDS).contains(&offset_seconds)
    {
        return None;
    }
    let biased = i64::from(offset_seconds.checked_add(TIMETZ_OFFSET_BIAS_SECONDS)?);
    Some((micros << TIMETZ_OFFSET_BITS) | biased)
}

/// Unpack an `i64` batch payload into `TIMETZ` components.
#[must_use]
pub fn unpack_timetz(packed: i64) -> Option<(i64, i32)> {
    if packed < 0 {
        return None;
    }
    let micros = packed >> TIMETZ_OFFSET_BITS;
    let biased = i32::try_from(packed & TIMETZ_OFFSET_MASK).ok()?;
    let offset_seconds = biased.checked_sub(TIMETZ_OFFSET_BIAS_SECONDS)?;
    if !(0..=MICROS_PER_DAY).contains(&micros) {
        return None;
    }
    Some((micros, offset_seconds))
}

/// Normalize `TIMETZ` to UTC time-of-day micros for equality, hashing,
/// ordering, and hash joins.
#[must_use]
pub fn timetz_utc_micros(micros: i64, offset_seconds: i32) -> i64 {
    micros
        .saturating_sub(i64::from(offset_seconds).saturating_mul(MICROS_PER_SECOND))
        .rem_euclid(MICROS_PER_DAY)
}

/// Inverse of Howard Hinnant's `days_from_civil`, rebased on UltraSQL's
/// 2000-01-01 date epoch.
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    reason = "civil-from-days arithmetic; doe / yoe fit in i32 by construction"
)]
fn civil_from_days(days_since_2000_01_01: i32) -> (i32, i32, i32) {
    let z = days_since_2000_01_01 + 10_957;
    let z = z + 719_468;
    let era = if z >= 0 {
        z / 146_097
    } else {
        (z - 146_096) / 146_097
    };
    let doe = (z - era * 146_097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = (yoe as i32) + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as i32;
    let month = if mp < 10 {
        mp as i32 + 3
    } else {
        mp as i32 - 9
    };
    let year = if month <= 2 { y + 1 } else { y };
    (year, month, day)
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
    use crate::parse_money_text;

    #[test]
    fn date_display_uses_iso_calendar_text() {
        assert_eq!(Value::Date(0).to_string(), "2000-01-01");
        assert_eq!(Value::Date(-1).to_string(), "1999-12-31");
        assert_eq!(Value::Date(8_766).to_string(), "2024-01-01");
    }

    #[test]
    fn temporal_display_uses_postgres_iso_text() {
        assert_eq!(Value::Time(3_723_456_789).to_string(), "01:02:03.456789");
        assert_eq!(
            Value::Timestamp(90_245_006_789).to_string(),
            "2000-01-02 01:04:05.006789"
        );
        assert_eq!(
            Value::TimestampTz(90_245_000_000).to_string(),
            "2000-01-02 01:04:05+00"
        );
        assert_eq!(
            Value::TimeTz {
                micros: 14_706_789_000,
                offset_seconds: -28_800,
            }
            .to_string(),
            "04:05:06.789-08"
        );
    }

    #[test]
    fn iso_date_and_timestamp_text_helpers_round_trip() {
        assert_eq!(parse_date_text(" 2000-01-02 "), Some(1));
        assert_eq!(format_date_days(1), "2000-01-02");
        assert_eq!(
            parse_timestamp_text("2000-01-01T01:02:03.456789"),
            Some(3_723_456_789)
        );
        assert_eq!(
            parse_timestamp_text("2000-01-01 01:02:03.456789-08"),
            Some(3_723_456_789)
        );
        assert_eq!(parse_timestamp_text("2000-01-01"), None);
        assert_eq!(parse_timestamp_text("2000-01-01 bad"), None);
    }

    #[test]
    fn timetz_equality_uses_utc_time_of_day() {
        assert_eq!(
            Value::TimeTz {
                micros: 64_800_000_000,
                offset_seconds: -25_200,
            },
            Value::TimeTz {
                micros: 61_200_000_000,
                offset_seconds: -28_800,
            }
        );
    }

    #[test]
    fn null_is_null() {
        assert!(Value::Null.is_null());
        assert!(!Value::Int32(0).is_null());
    }

    #[test]
    fn data_type_matches_variant() {
        assert_eq!(Value::Int32(1).data_type(), DataType::Int32);
        assert_eq!(Value::Int64(1).data_type(), DataType::Int64);
        assert_eq!(Value::Money(123).data_type(), DataType::Money);
        assert_eq!(Value::Bool(true).data_type(), DataType::Bool);
        assert_eq!(
            Value::Text("hi".into()).data_type(),
            DataType::Text { max_len: None }
        );
        assert_eq!(
            Value::Array {
                element_type: DataType::Int32,
                elements: vec![Value::Int32(1), Value::Int32(2)]
            }
            .data_type(),
            DataType::Array(Box::new(DataType::Int32))
        );
        assert_eq!(Value::Json(r#"{"a":1}"#.into()).data_type(), DataType::Json);
        assert_eq!(
            Value::Jsonb(r#"{"a":1}"#.into()).data_type(),
            DataType::Jsonb
        );
        assert_eq!(Value::Xml("<root/>".into()).data_type(), DataType::Xml);
        assert_eq!(Value::Null.data_type(), DataType::Null);
    }

    #[test]
    fn xml_validator_accepts_balanced_document_and_rejects_open_tag() {
        assert_eq!(
            Value::validate_xml_text(r#"<root attr="v"><child>text</child></root>"#),
            Some(r#"<root attr="v"><child>text</child></root>"#.to_owned())
        );
        assert_eq!(
            Value::validate_xml_text(r#"<?xml version="1.0"?><root><copy/></root>"#),
            Some(r#"<?xml version="1.0"?><root><copy/></root>"#.to_owned())
        );
        assert_eq!(Value::validate_xml_text("<root>"), None);
        assert_eq!(Value::validate_xml_text("<root attr=v/>"), None);
        assert_eq!(Value::validate_xml_text("<a/><b/>"), None);
    }

    #[test]
    fn xml_xpath_subset_filters_children_without_entity_resolution() {
        let doc = r#"<root><item id="1"><name>A</name></item><item id="2"><name>B</name></item><empty/></root>"#;
        assert_eq!(
            xml_xpath_element_fragments(r#"/root/item[@id="2"]/name"#, doc),
            Some(vec!["<name>B</name>".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments("/root/item/@id", doc),
            Some(vec!["1".to_owned(), "2".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments("/root/item/name/text()", doc),
            Some(vec!["A".to_owned(), "B".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments("/root/*", doc),
            Some(vec![
                r#"<item id="1"><name>A</name></item>"#.to_owned(),
                r#"<item id="2"><name>B</name></item>"#.to_owned(),
                "<empty/>".to_owned(),
            ])
        );
        assert_eq!(
            xml_xpath_element_fragments("/root/*/@*", doc),
            Some(vec!["1".to_owned(), "2".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments("count(/root/item)", doc),
            Some(vec!["2".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments("string(/root/item/name)", doc),
            Some(vec!["A".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments("boolean(/root/item)", doc),
            Some(vec!["true".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments("true()", doc),
            Some(vec!["true".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments("false()", doc),
            Some(vec!["false".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments("not(/root/missing)", doc),
            Some(vec!["true".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments("not(/root/item)", doc),
            Some(vec!["false".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments("name(/root/item)", doc),
            Some(vec!["item".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments("local-name(/root/item)", doc),
            Some(vec!["item".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments(
                "normalize-space(/root/item)",
                r#"<root><item>  Ada   Lovelace </item></root>"#
            ),
            Some(vec!["Ada Lovelace".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments(
                "string-length(/root/item)",
                r#"<root><item>  Ada   Lovelace </item></root>"#
            ),
            Some(vec!["17".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments(
                r#"contains(/root/item, "Ada")"#,
                r#"<root><item>Ada Lovelace</item></root>"#
            ),
            Some(vec!["true".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments(
                r#"contains(/root/item, "Turing")"#,
                r#"<root><item>Ada Lovelace</item></root>"#
            ),
            Some(vec!["false".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments(
                r#"starts-with(/root/item, "Ada")"#,
                r#"<root><item>Ada Lovelace</item></root>"#
            ),
            Some(vec!["true".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments(
                r#"starts-with(/root/missing, "Ada")"#,
                r#"<root><item>Ada Lovelace</item></root>"#
            ),
            Some(vec!["false".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments(
                r#"substring-before(/root/item, " ")"#,
                r#"<root><item>Ada Lovelace</item></root>"#
            ),
            Some(vec!["Ada".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments(
                r#"substring-after(/root/item, " ")"#,
                r#"<root><item>Ada Lovelace</item></root>"#
            ),
            Some(vec!["Lovelace".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments(
                r#"substring-before(/root/item, "x")"#,
                r#"<root><item>Ada Lovelace</item></root>"#
            ),
            Some(vec![String::new()])
        );
        assert_eq!(
            xml_xpath_element_fragments(
                r#"concat(/root/first, " ", /root/last)"#,
                r#"<root><first>Ada</first><last>Lovelace</last></root>"#
            ),
            Some(vec!["Ada Lovelace".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments(
                r#"concat("prefix-", /root/missing)"#,
                r#"<root><first>Ada</first></root>"#
            ),
            Some(vec!["prefix-".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments(
                "number(/root/value)",
                r#"<root><value> 42.5 </value></root>"#
            ),
            Some(vec!["42.5".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments(
                "floor(/root/value)",
                r#"<root><value>42.5</value></root>"#
            ),
            Some(vec!["42".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments(
                "ceiling(/root/value)",
                r#"<root><value>42.5</value></root>"#
            ),
            Some(vec!["43".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments(
                "number(/root/missing)",
                r#"<root><value>42.5</value></root>"#
            ),
            Some(vec!["NaN".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments(
                "round(/root/value)",
                r#"<root><value>42.5</value></root>"#
            ),
            Some(vec!["43".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments(
                "round(/root/value)",
                r#"<root><value>-42.5</value></root>"#
            ),
            Some(vec!["-42".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments(
                "sum(/root/value)",
                r#"<root><value>1.5</value><value>2.25</value></root>"#
            ),
            Some(vec!["3.75".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments("sum(/root/missing)", r#"<root><value>1.5</value></root>"#),
            Some(vec!["0".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments(
                "sum(/root/value)",
                r#"<root><value>1</value><value>bad</value></root>"#
            ),
            Some(vec!["NaN".to_owned()])
        );
        let positioned = r#"<root><item>a</item><item>b</item><item>c</item></root>"#;
        assert_eq!(
            xml_xpath_element_fragments("/root/item[position()=1]", positioned),
            Some(vec!["<item>a</item>".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments("/root/item[2]", positioned),
            Some(vec!["<item>b</item>".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments("/root/item[last()]", positioned),
            Some(vec!["<item>c</item>".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments("/root/item[position()=last()]", positioned),
            Some(vec!["<item>c</item>".to_owned()])
        );
        let nested = r#"<root><group><item id="1"><name>A</name></item><item id="2"><name>B</name></item></group><name>C</name></root>"#;
        assert_eq!(
            xml_xpath_element_fragments(r#"//item[@id="2"]/name"#, nested),
            Some(vec!["<name>B</name>".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments("/root//name", nested),
            Some(vec![
                "<name>A</name>".to_owned(),
                "<name>B</name>".to_owned(),
                "<name>C</name>".to_owned()
            ])
        );
        let namespaced =
            r#"<r:root xmlns:r="urn:r" xmlns:x="urn:x"><r:item x:id="7">Z</r:item></r:root>"#;
        assert_eq!(
            xml_xpath_element_fragments("/r:root/r:item/@x:id", namespaced),
            Some(vec!["7".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments("/r:root/r:item/text()", namespaced),
            Some(vec!["Z".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments("local-name(/r:root/r:item)", namespaced),
            Some(vec!["item".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments("/r:root/@*", namespaced),
            Some(Vec::new())
        );
        assert_eq!(
            xml_xpath_element_fragments("/root/missing", doc),
            Some(Vec::new())
        );
        assert_eq!(xml_xpath_element_fragments("/root/..", doc), None);
        assert!(Value::xml_content_is_well_formed("<a/><b/>"));
        assert!(!Value::xml_content_is_well_formed("&unknown;"));
        assert!(!Value::xml_document_is_well_formed(
            r#"<!DOCTYPE root [<!ENTITY xxe SYSTEM "file:///etc/passwd">]><root/>"#
        ));
    }

    #[test]
    fn xml_xpath_subset_resolves_namespace_uri_aliases() {
        let doc =
            r#"<root xmlns="urn:root" xmlns:i="urn:item"><i:child i:id="7">z</i:child></root>"#;
        let namespaces = vec![
            ("r".to_owned(), "urn:root".to_owned()),
            ("item".to_owned(), "urn:item".to_owned()),
        ];

        assert_eq!(
            xml_xpath_element_fragments_with_namespaces(
                "/r:root/item:child/@item:id",
                doc,
                &namespaces
            ),
            Some(vec!["7".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments_with_namespaces(
                "/r:root/item:child/text()",
                doc,
                &namespaces
            ),
            Some(vec!["z".to_owned()])
        );
    }

    #[test]
    fn range_values_cover_overlap_containment_and_empty_edges() {
        let left = RangeValue::parse(RangeType::Int4, "[1,10)").unwrap();
        let overlapping = RangeValue::parse(RangeType::Int4, "[9,12]").unwrap();
        let inside = RangeValue::parse(RangeType::Int4, "[2,3]").unwrap();
        let outside = RangeValue::parse(RangeType::Int4, "[10,12]").unwrap();
        let empty = RangeValue::parse(RangeType::Int4, "[5,5)").unwrap();

        assert!(left.overlaps(&overlapping));
        assert!(!left.overlaps(&outside));
        assert!(left.contains_range(&inside));
        assert!(left.contains_range(&empty));
        assert_eq!(empty.to_string(), "empty");
        assert_eq!(
            RangeValue::parse(RangeType::Num, "(1.5,2.25]")
                .unwrap()
                .to_string(),
            "(1.5,2.25]"
        );
        assert_eq!(
            RangeValue::parse(RangeType::Date, "[2000-01-01,2000-01-03)")
                .unwrap()
                .to_string(),
            "[0,2)"
        );
        assert!(RangeValue::parse(RangeType::Int4, "bad").is_none());
        assert!(!left.overlaps(&RangeValue::parse(RangeType::Int8, "[1,10)").unwrap()));
    }

    #[test]
    fn geometry_values_use_bounding_boxes_for_gist_predicates() {
        let point = GeometryValue::parse(GeometryType::Point, "(1,2)").unwrap();
        let circle = GeometryValue::parse(GeometryType::Circle, "<(5,5),2>").unwrap();
        let container = GeometryValue::parse(GeometryType::Box, "((0,0),(10,10))").unwrap();
        let far = GeometryValue::parse(GeometryType::Polygon, "((20,20),(21,21),(22,20))").unwrap();

        assert_eq!(point.to_string(), "(1,2)");
        assert!(container.contains_geometry(&circle));
        assert!(container.overlaps(&circle));
        assert!(!container.overlaps(&far));
        assert!(GeometryValue::parse(GeometryType::Point, "(1)").is_none());
        assert!(GeometryValue::parse(GeometryType::Circle, "(1,2)").is_none());
    }

    #[test]
    fn array_display_and_parse_round_trip() {
        let value = Value::Array {
            element_type: DataType::Text { max_len: None },
            elements: vec![
                Value::Text("red".into()),
                Value::Text("green,blue".into()),
                Value::Null,
            ],
        };
        assert_eq!(value.to_string(), r#"{red,"green,blue",NULL}"#);
        assert_eq!(
            Value::parse_array(DataType::Text { max_len: None }, &value.to_string()),
            Some(value)
        );

        let xml = Value::Array {
            element_type: DataType::Xml,
            elements: vec![
                Value::Xml(r#"<item id="1">a</item>"#.into()),
                Value::Xml("Ada Lovelace".into()),
            ],
        };
        assert_eq!(
            xml.to_string(),
            r#"{"<item id=\"1\">a</item>","Ada Lovelace"}"#
        );
    }

    #[test]
    fn array_display_and_parse_multi_dimensional_round_trip() {
        let matrix_type = DataType::Array(Box::new(DataType::Int32));
        let value = Value::Array {
            element_type: matrix_type.clone(),
            elements: vec![
                Value::Array {
                    element_type: DataType::Int32,
                    elements: vec![Value::Int32(1), Value::Int32(2)],
                },
                Value::Array {
                    element_type: DataType::Int32,
                    elements: vec![Value::Int32(3), Value::Int32(4)],
                },
            ],
        };
        assert_eq!(value.to_string(), "{{1,2},{3,4}}");
        assert_eq!(value.array_dimensions(), Some(vec![2, 2]));
        assert_eq!(
            Value::parse_array(matrix_type.clone(), &value.to_string()),
            Some(value)
        );
        assert_eq!(Value::parse_array(matrix_type, "{{1,2},{3}}"), None);
    }

    #[test]
    fn array_parser_covers_scalar_element_families_and_escaping() {
        assert_eq!(
            Value::parse_array(DataType::Bool, "{t,false,NULL}")
                .unwrap()
                .to_string(),
            "{true,false,NULL}"
        );
        assert_eq!(
            Value::parse_array(DataType::Int16, "{-1,2}")
                .unwrap()
                .to_string(),
            "{-1,2}"
        );
        assert_eq!(
            Value::parse_array(DataType::Float64, "{1.5,2.25}")
                .unwrap()
                .to_string(),
            "{1.5,2.25}"
        );
        assert_eq!(
            Value::parse_array(DataType::Oid, "{42}"),
            Some(Value::Array {
                element_type: DataType::Oid,
                elements: vec![Value::Oid(crate::Oid::new(42))]
            })
        );
        assert_eq!(
            Value::parse_array(DataType::RegClass, "{43}"),
            Some(Value::Array {
                element_type: DataType::RegClass,
                elements: vec![Value::RegClass(crate::Oid::new(43))]
            })
        );
        assert_eq!(
            Value::parse_array(DataType::RegType, "{44}"),
            Some(Value::Array {
                element_type: DataType::RegType,
                elements: vec![Value::RegType(crate::Oid::new(44))]
            })
        );
        assert_eq!(
            Value::parse_array(DataType::PgLsn, "{0/2A}"),
            Some(Value::Array {
                element_type: DataType::PgLsn,
                elements: vec![Value::PgLsn(crate::Lsn::new(42))]
            })
        );
        assert_eq!(
            Value::parse_array(DataType::Char { len: Some(3) }, r#"{"a"}"#)
                .unwrap()
                .to_string(),
            r#"{"a  "}"#
        );
        assert_eq!(
            Value::parse_array(DataType::Bytea, r#"{"\\xdead"}"#)
                .unwrap()
                .to_string(),
            r#"{\xdead}"#
        );
        assert_eq!(
            Value::parse_array(DataType::Money, "{$1.25}")
                .unwrap()
                .to_string(),
            "{$1.25}"
        );
        assert!(Value::parse_array(DataType::Uuid, "{not-a-uuid}").is_none());
        assert!(
            Value::parse_array(DataType::Text { max_len: None }, r#"{"unterminated}"#).is_none()
        );
        assert!(Value::parse_array(DataType::Vector { dims: None }, "{[1,2]}").is_none());
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
    fn money_display_and_parse_use_pg_cash_cents() {
        assert_eq!(Value::Money(123_456).to_string(), "$1,234.56");
        assert_eq!(Value::Money(-123).to_string(), "-$1.23");
        assert_eq!(
            parse_money_text("$1,234.565").expect("money parses"),
            Value::Money(123_457)
        );
        assert_eq!(
            parse_money_text("($1.23)").expect("parenthesized negative parses"),
            Value::Money(-123)
        );
    }

    #[test]
    fn char_values_preserve_padding_but_compare_trimmed() {
        assert_eq!(Value::Char("ok  ".to_owned()).to_string(), "ok  ");
        assert_eq!(
            Value::Char("ok  ".to_owned()).data_type(),
            DataType::Char { len: Some(4) }
        );
        assert_eq!(Value::Char("ok  ".to_owned()), Value::Char("ok".to_owned()));
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
    fn bytea_parse_accepts_hex_text() {
        assert_eq!(
            Value::parse_bytea("\\xdeadBEEF"),
            Some(vec![0xde, 0xad, 0xbe, 0xef])
        );
        assert_eq!(Value::parse_bytea("\\xabc"), None);
        assert_eq!(Value::parse_bytea("deadbeef"), None);
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
    fn vector_parse_rejects_non_finite_elements() {
        assert_eq!(
            Value::parse_vector("[1, 2.5, -3]").unwrap(),
            Value::Vector(vec![1.0, 2.5, -3.0])
        );
        assert_eq!(
            Value::Vector(vec![1.0, 2.5, -3.0]).to_string(),
            "[1,2.5,-3]"
        );
        assert!(Value::parse_vector("[]").is_none());
        assert!(Value::parse_vector("[NaN]").is_none());
        assert!(Value::parse_vector("[Infinity]").is_none());
    }

    #[test]
    fn vector_family_literals_parse_and_render() {
        assert_eq!(
            Value::parse_halfvec("[1, 2.5, -3]").unwrap(),
            Value::HalfVec(vec![1.0, 2.5, -3.0])
        );
        assert_eq!(
            Value::HalfVec(vec![1.0, 2.5, -3.0]).to_string(),
            "[1,2.5,-3]"
        );

        assert_eq!(
            Value::parse_sparsevec("{1:1,3:2.5}/5").unwrap(),
            Value::SparseVec(SparseVector::new(5, vec![(1, 1.0), (3, 2.5)]).unwrap())
        );
        assert_eq!(
            Value::SparseVec(SparseVector::new(5, vec![(1, 1.0), (3, 2.5)]).unwrap()).to_string(),
            "{1:1,3:2.5}/5"
        );

        assert_eq!(
            Value::parse_bitvec("101001").unwrap(),
            Value::BitVec {
                dims: 6,
                bytes: vec![0b1010_0100]
            }
        );
        assert_eq!(
            (Value::BitVec {
                dims: 6,
                bytes: vec![0b1010_0100],
            })
            .to_string(),
            "101001"
        );

        assert!(Value::parse_halfvec("[NaN]").is_none());
        assert!(Value::parse_sparsevec("{0:1}/5").is_none());
        assert!(Value::parse_sparsevec("{1:1}/0").is_none());
        assert!(Value::parse_bitvec("102").is_none());
        assert!(Value::parse_bitvec("").is_none());
    }

    #[test]
    fn uuid_parse_accepts_canonical_and_compact() {
        let expected = [
            0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc,
            0xde, 0xf0,
        ];
        assert_eq!(
            Value::parse_uuid("12345678-9abc-def0-1234-56789abcdef0"),
            Some(expected)
        );
        assert_eq!(
            Value::parse_uuid("123456789ABCDEF0123456789ABCDEF0"),
            Some(expected)
        );
        assert_eq!(Value::parse_uuid("not-a-uuid"), None);
    }

    #[test]
    fn time_text_parser_and_timetz_pack_reject_bad_edges() {
        assert_eq!(
            parse_time_text("2000-01-01 04:05:06.789 -08"),
            Some(14_706_789_000)
        );
        assert_eq!(
            parse_timestamptz_text("2000-01-01 00:00:00 America/New_York"),
            Some(18_000_000_000)
        );
        assert_eq!(
            parse_timestamptz_text("2000-07-01 00:00:00 America/New_York"),
            parse_timestamp_text("2000-07-01 04:00:00")
        );
        assert_eq!(
            parse_timetz_text("2000-01-01 04:05:06 America/New_York"),
            Some((14_706_000_000, -18_000))
        );
        assert_eq!(
            parse_timetz_text("2000-07-01 04:05:06 America/New_York"),
            Some((14_706_000_000, -14_400))
        );
        assert_eq!(parse_timetz_text("04:05 zulu"), Some((14_700_000_000, 0)));
        assert_eq!(
            parse_timetz_text("04:05:06+0530"),
            Some((14_706_000_000, 19_800))
        );
        assert_eq!(
            format_timetz(14_706_789_000, 19_830),
            "04:05:06.789+05:30:30"
        );
        assert_eq!(parse_time_text("24:00"), Some(MICROS_PER_DAY));
        assert_eq!(parse_time_text("24:00:00.000001"), None);
        assert_eq!(parse_timetz_text("04:05 +16"), None);

        let packed = pack_timetz(MICROS_PER_DAY, 86_400).unwrap();
        assert_eq!(unpack_timetz(packed), Some((MICROS_PER_DAY, 86_400)));
        assert_eq!(pack_timetz(-1, 0), None);
        assert_eq!(pack_timetz(0, 86_401), None);
        assert_eq!(unpack_timetz(-1), None);
        assert_eq!(
            unpack_timetz((MICROS_PER_DAY + 1) << TIMETZ_OFFSET_BITS),
            None
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
