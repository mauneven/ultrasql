use super::*;

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
                // Coordinates arrive as (x, y) pairs; an odd count is a trailing
                // coordinate with no mate. Reject it rather than letting the
                // `chunks_exact(2)` below silently drop the dangling value.
                if nums.len() % COORDINATES_PER_POINT != 0 {
                    return None;
                }
                let mut points =
                    Vec::with_capacity(nums.len().checked_div(COORDINATES_PER_POINT).unwrap_or(0));
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
