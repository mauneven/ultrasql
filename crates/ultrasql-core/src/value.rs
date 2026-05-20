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

use crate::types::{DataType, GeometryType, RangeType};

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
    /// JSONB-compatible textual payload.
    Jsonb(String),
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
            Self::Jsonb(v) => v.hash(state),
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
            Self::Range(v) => v.hash(state),
            Self::Geometry(v) => v.hash(state),
            Self::Array {
                element_type,
                elements,
            } => {
                element_type.hash(state);
                elements.hash(state);
            }
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
            Self::Float32(_) => DataType::Float32,
            Self::Float64(_) => DataType::Float64,
            Self::Text(_) => DataType::Text { max_len: None },
            Self::Jsonb(_) => DataType::Jsonb,
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
            Self::Range(v) => DataType::Range(v.range_type),
            Self::Geometry(v) => DataType::Geometry(v.geometry_type),
            Self::Array { element_type, .. } => DataType::Array(Box::new(element_type.clone())),
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
            split_array_elements(inner)
                .into_iter()
                .map(|part| parse_array_element(&element_type, part))
                .collect::<Option<Vec<_>>>()?
        };
        Some(Self::Array {
            element_type,
            elements,
        })
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

    /// Borrowed bool view. `None` for non-boolean.
    #[must_use]
    pub const fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Bool(b) => Some(*b),
            _ => None,
        }
    }
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
            Self::Float32(v) => write!(f, "{v}"),
            Self::Float64(v) => write!(f, "{v}"),
            Self::Text(s) => write!(f, "{s}"),
            Self::Jsonb(s) => write!(f, "{s}"),
            Self::Bytea(b) => {
                f.write_str("\\x")?;
                for byte in b {
                    write!(f, "{byte:02x}")?;
                }
                Ok(())
            }
            Self::Timestamp(us) | Self::TimestampTz(us) => write!(f, "{us}us"),
            Self::Date(d) => write!(f, "{}", format_date(*d)),
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
            Self::Range(v) => write!(f, "{v}"),
            Self::Geometry(v) => write!(f, "{v}"),
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
        DataType::Float32 => text.parse::<f32>().ok().map(Value::Float32),
        DataType::Float64 => text.parse::<f64>().ok().map(Value::Float64),
        DataType::Text { .. } => Some(Value::Text(text)),
        DataType::Jsonb => Some(Value::Jsonb(text)),
        DataType::Bytea => Value::parse_bytea(&text).map(Value::Bytea),
        DataType::Uuid => Value::parse_uuid(&text).map(Value::Uuid),
        _ => None,
    }
}

fn split_array_elements(text: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut start = 0;
    let mut in_string = false;
    let mut escape = false;
    for (idx, ch) in text.char_indices() {
        if escape {
            escape = false;
            continue;
        }
        match ch {
            '\\' if in_string => escape = true,
            '"' => in_string = !in_string,
            ',' if !in_string => {
                out.push(&text[start..idx]);
                start = idx + ch.len_utf8();
            }
            _ => {}
        }
    }
    out.push(&text[start..]);
    out
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
        Value::Text(s) => write_array_text(f, s),
        other => write!(f, "{other}"),
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
    let mut parts = text.split('-');
    let year = parts.next()?.parse::<i32>().ok()?;
    let month = parts.next()?.parse::<u32>().ok()?;
    let day = parts.next()?.parse::<u32>().ok()?;
    if parts.next().is_some() {
        return None;
    }
    days_from_civil(year, month, day)
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

    #[test]
    fn date_display_uses_iso_calendar_text() {
        assert_eq!(Value::Date(0).to_string(), "2000-01-01");
        assert_eq!(Value::Date(-1).to_string(), "1999-12-31");
        assert_eq!(Value::Date(8_766).to_string(), "2024-01-01");
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
        assert_eq!(
            Value::Jsonb(r#"{"a":1}"#.into()).data_type(),
            DataType::Jsonb
        );
        assert_eq!(Value::Null.data_type(), DataType::Null);
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
    fn from_impls() {
        let v: Value = 7_i32.into();
        assert_eq!(v, Value::Int32(7));
        let v: Value = "abc".into();
        assert_eq!(v, Value::Text("abc".into()));
    }
}
