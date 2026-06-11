//! Dictionary encoding for low-cardinality string columns.
//!
//! A [`DictionaryColumn`] stores:
//! - `dict`: unique string values in insertion order.
//! - `codes`: per-row `u32` code — each is an index into `dict`.
//!
//! This representation allows equality filters and GROUP BY operations to
//! compare small integer codes rather than variable-length strings.  For a
//! column with `D` distinct values and `N` rows, the dict uses `O(D)` memory
//! and the codes buffer uses `O(N * 4)` bytes, whereas a raw string column
//! uses `O(N * avg_len)` bytes.
//!
//! ## Operations
//!
//! - [`DictionaryColumn::from_strings`] — build from an iterator of `&str`.
//! - [`DictionaryColumn::decode_at`] — decode a single code back to `&str`.
//! - [`filter_eq_dict_code`] — filter to rows whose code matches a target.
//! - [`group_by_dict`] — compute per-code row groups (for GROUP BY).

use std::collections::HashMap;

use crate::bitmap::Bitmap;
use crate::column::{ColumnError, NumericColumn, StringColumn};
use crate::int_cast::u32_to_usize;

/// Errors raised while building dictionary-encoded string columns.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum DictionaryError {
    /// The number of distinct values does not fit the on-disk/in-memory code
    /// width.
    #[error("dictionary has too many distinct values: {distinct} exceeds u32::MAX")]
    TooManyDistinctValues {
        /// Distinct values seen before assigning the next code.
        distinct: usize,
    },

    /// The generated validity bitmap does not match the code buffer length.
    #[error("dictionary code bitmap: {0}")]
    Column(#[from] ColumnError),
}

// ============================================================================
// DictionaryColumn
// ============================================================================

/// A dictionary-encoded string column.
///
/// Every row stores a `u32` code that is an index into the `dict` array of
/// unique strings. Null rows are represented by `u32::MAX` in `codes` and a
/// cleared bit in the optional validity bitmap.
///
/// ## Invariants
///
/// - `codes.len()` rows, each in `0..dict.len()` for non-null rows.
/// - For null rows the code is `u32::MAX` and the validity bitmap bit is 0.
/// - `dict` contains no duplicate strings.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DictionaryColumn {
    /// Unique values in insertion order. Index `i` corresponds to code `i`.
    pub dict: Vec<String>,
    /// Per-row code: index into `dict`, or `u32::MAX` for null rows.
    pub codes: NumericColumn<u32>,
}

impl DictionaryColumn {
    /// Build a `DictionaryColumn` from an iterator of string slices.
    ///
    /// `None` entries are treated as SQL NULL. All unique non-null values are
    /// added to the dictionary in first-seen order.
    ///
    /// # Performance
    ///
    /// Uses a `HashMap` for deduplication; one pass over the input.
    pub fn from_strings<'a, I>(iter: I) -> Result<Self, DictionaryError>
    where
        I: IntoIterator<Item = Option<&'a str>>,
    {
        let mut dict: Vec<String> = Vec::new();
        let mut map: HashMap<String, u32> = HashMap::new();
        let mut code_data: Vec<u32> = Vec::new();
        let mut null_positions: Vec<usize> = Vec::new();

        for (i, item) in iter.into_iter().enumerate() {
            match item {
                None => {
                    code_data.push(u32::MAX);
                    null_positions.push(i);
                }
                Some(s) => {
                    let code = if let Some(&c) = map.get(s) {
                        c
                    } else {
                        let c = u32::try_from(dict.len()).map_err(|_| {
                            DictionaryError::TooManyDistinctValues {
                                distinct: dict.len(),
                            }
                        })?;
                        dict.push(s.to_owned());
                        map.insert(s.to_owned(), c);
                        c
                    };
                    code_data.push(code);
                }
            }
        }

        let n = code_data.len();
        let codes = if null_positions.is_empty() {
            NumericColumn::from_data(code_data)
        } else {
            let mut bm = Bitmap::new(n, true);
            for pos in &null_positions {
                bm.set(*pos, false);
            }
            NumericColumn::with_nulls(code_data, bm)?
        };

        Ok(Self { dict, codes })
    }

    /// Number of rows.
    #[must_use]
    pub fn len(&self) -> usize {
        self.codes.len()
    }

    /// Whether the column has zero rows.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.codes.is_empty()
    }

    /// Look up the string value for row `i`.
    ///
    /// Returns `None` when `i` is out of bounds, the row is SQL NULL, or
    /// the stored code does not point at a dictionary entry.
    #[must_use]
    pub fn try_decode_at(&self, i: usize) -> Option<&str> {
        if self.codes.nulls().is_some_and(|nulls| !nulls.get(i)) {
            return None;
        }
        let code = *self.codes.data().get(i)?;
        if code == u32::MAX {
            return None;
        }
        let idx = u32_to_usize(code);
        self.dict.get(idx).map(String::as_str)
    }

    /// Look up the string value for row `i`.
    ///
    /// Prefer [`Self::try_decode_at`] for externally supplied row indexes.
    /// This convenience accessor fails closed to the empty string for
    /// out-of-bounds, NULL, or malformed dictionary codes.
    #[must_use]
    pub fn decode_at(&self, i: usize) -> &str {
        self.try_decode_at(i).unwrap_or("")
    }

    /// Look up the dict code for `value`, or `None` if not present.
    #[must_use]
    pub fn code_for(&self, value: &str) -> Option<u32> {
        self.dict
            .iter()
            .position(|s| s == value)
            .and_then(|i| u32::try_from(i).ok())
    }
}

// ============================================================================
// Automatic dictionary selection
// ============================================================================

/// Cardinality policy for automatic dictionary encoding.
///
/// The policy intentionally uses integer thresholds so the choice is stable
/// across platforms and does not depend on floating-point rounding. A column
/// is dictionary-encoded when it has enough rows, its non-null distinct count
/// fits under `max_distinct_values`, and its distinct/non-null ratio is at or
/// below `max_cardinality_percent`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DictionaryEncodingPolicy {
    /// Minimum row count before dictionary encoding is considered.
    pub min_rows: usize,
    /// Maximum non-null distinct values allowed for dictionary encoding.
    pub max_distinct_values: usize,
    /// Maximum `distinct * 100 / non_null` percentage.
    pub max_cardinality_percent: u8,
}

impl Default for DictionaryEncodingPolicy {
    fn default() -> Self {
        Self {
            min_rows: 1024,
            max_distinct_values: 4096,
            max_cardinality_percent: 20,
        }
    }
}

impl DictionaryEncodingPolicy {
    /// Decide whether a string column should use dictionary encoding.
    #[must_use]
    pub fn should_dictionary_encode(
        self,
        rows: usize,
        non_null_rows: usize,
        distinct_values: usize,
    ) -> bool {
        if rows < self.min_rows || non_null_rows == 0 || distinct_values == 0 {
            return false;
        }
        if distinct_values > self.max_distinct_values {
            return false;
        }
        distinct_values.saturating_mul(100)
            <= non_null_rows.saturating_mul(usize::from(self.max_cardinality_percent))
    }
}

/// Physical string encoding selected by [`encode_strings_auto`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StringEncoding {
    /// Arrow-style UTF-8 offsets + value bytes.
    Raw(StringColumn),
    /// Dictionary-encoded UTF-8 values.
    Dictionary(DictionaryColumn),
}

impl StringEncoding {
    /// Number of rows.
    #[must_use]
    pub fn len(&self) -> usize {
        match self {
            Self::Raw(c) => c.len(),
            Self::Dictionary(c) => c.len(),
        }
    }

    /// Whether the encoded column has zero rows.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// `true` iff this encoding stores dictionary codes.
    #[must_use]
    pub const fn is_dictionary(&self) -> bool {
        matches!(self, Self::Dictionary(_))
    }
}

/// Automatically choose raw UTF-8 or dictionary encoding for a string column.
///
/// This helper is the policy-level entry point used by vectorized operators
/// that can exploit code comparisons. It preserves SQL NULLs in both returned
/// encodings and keeps first-seen dictionary order when dictionary encoding is
/// selected.
#[must_use]
pub fn encode_strings_auto<'a, I>(iter: I, policy: DictionaryEncodingPolicy) -> StringEncoding
where
    I: IntoIterator<Item = Option<&'a str>>,
{
    let rows: Vec<Option<String>> = iter.into_iter().map(|v| v.map(str::to_owned)).collect();
    let dict = match DictionaryColumn::from_strings(rows.iter().map(|v| v.as_deref())) {
        Ok(dict) => dict,
        Err(_) => return StringEncoding::Raw(raw_string_column_from_rows(&rows)),
    };
    let non_null_rows = rows.iter().filter(|v| v.is_some()).count();

    if policy.should_dictionary_encode(rows.len(), non_null_rows, dict.dict.len()) {
        StringEncoding::Dictionary(dict)
    } else {
        StringEncoding::Raw(raw_string_column_from_rows(&rows))
    }
}

fn raw_string_column_from_rows(rows: &[Option<String>]) -> StringColumn {
    if rows.iter().all(Option::is_some) {
        return StringColumn::from_data(rows.iter().filter_map(Clone::clone));
    }

    let mut nulls = Bitmap::new(rows.len(), true);
    let mut values = Vec::with_capacity(rows.len());
    for (i, v) in rows.iter().enumerate() {
        match v {
            Some(s) => values.push(s.clone()),
            None => {
                nulls.set(i, false);
                values.push(String::new());
            }
        }
    }
    match StringColumn::with_nulls(values, nulls) {
        Ok(column) => column,
        Err(err) => {
            debug_assert!(
                false,
                "raw dictionary fallback built mismatched string nulls: {err}"
            );
            StringColumn::from_data(std::iter::empty::<String>())
        }
    }
}

// ============================================================================
// filter_eq_dict_code
// ============================================================================

/// Filter rows where the dict code equals `target_code`.
///
/// Returns a `Bitmap` of length `column.len()` where bit `i` is set iff
/// `column.codes[i] == target_code` AND the row is non-null.
///
/// This operation never touches the string dictionary — it compares `u32`
/// codes only, which is significantly faster than comparing variable-length
/// strings.
#[must_use]
pub fn filter_eq_dict_code(column: &DictionaryColumn, target_code: u32) -> Bitmap {
    let n = column.len();
    let data = column.codes.data();
    let mut out = Bitmap::new(n, false);
    let validity = column.codes.nulls();

    for (i, &code) in data.iter().enumerate() {
        let valid = validity.is_none_or(|bm| bm.get(i));
        if valid && code == target_code {
            out.set(i, true);
        }
    }
    out
}

// ============================================================================
// group_by_dict
// ============================================================================

/// Compute GROUP BY on a dictionary-encoded column.
///
/// Returns a `Vec` of `(code, row_indices)` pairs — one entry per distinct
/// non-null code that appears in the column. The `row_indices` slice contains
/// the 0-based row positions (ascending) for that group.
///
/// NULL rows (validity = 0) are excluded from all groups.
///
/// The output order follows the code values: codes are processed in the range
/// `0..dict.len()`, so the output is in dict-insertion order.
#[must_use]
pub fn group_by_dict(column: &DictionaryColumn) -> Vec<(u32, Vec<usize>)> {
    let data = column.codes.data();
    let dict_size = column.dict.len();
    let validity = column.codes.nulls();

    let mut groups: Vec<Vec<usize>> = vec![Vec::new(); dict_size];

    for (i, &code) in data.iter().enumerate() {
        let valid = validity.is_none_or(|bm| bm.get(i));
        if valid
            && code != u32::MAX
            && let Ok(idx) = usize::try_from(code)
            && idx < dict_size
        {
            groups[idx].push(i);
        }
    }

    groups
        .into_iter()
        .enumerate()
        .filter(|(_, rows)| !rows.is_empty())
        .filter_map(|(code, rows)| u32::try_from(code).ok().map(|code| (code, rows)))
        .collect()
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn dict<'a, I>(iter: I) -> DictionaryColumn
    where
        I: IntoIterator<Item = Option<&'a str>>,
    {
        DictionaryColumn::from_strings(iter).expect("test dictionary should fit u32 codes")
    }

    // ---- DictionaryColumn::from_strings ----

    #[test]
    fn from_strings_deduplicates_values() {
        let col = dict(
            ["alpha", "beta", "alpha", "gamma", "beta"]
                .iter()
                .map(|s| Some(*s)),
        );
        assert_eq!(col.dict.len(), 3);
        assert_eq!(col.len(), 5);
        assert!(col.codes.nulls().is_none());
    }

    #[test]
    fn from_strings_codes_point_to_correct_dict_entries() {
        let col = dict(["x", "y", "x"].iter().map(|s| Some(*s)));
        assert_eq!(col.decode_at(0), "x");
        assert_eq!(col.decode_at(1), "y");
        assert_eq!(col.decode_at(2), "x");
    }

    #[test]
    fn from_strings_handles_nulls() {
        let col = dict([Some("a"), None, Some("b"), None, Some("a")]);
        assert_eq!(col.len(), 5);
        let nulls = col.codes.nulls().expect("nullable column");
        assert!(nulls.get(0));
        assert!(!nulls.get(1));
        assert!(nulls.get(2));
        assert!(!nulls.get(3));
        assert!(nulls.get(4));
    }

    #[test]
    fn decode_at_returns_correct_string() {
        let col = dict(["cat", "dog", "cat"].iter().map(|s| Some(*s)));
        assert_eq!(col.decode_at(0), "cat");
        assert_eq!(col.decode_at(1), "dog");
        assert_eq!(col.decode_at(2), "cat");
    }

    #[test]
    fn code_for_returns_correct_index() {
        let col = dict(["a", "b", "c"].iter().map(|s| Some(*s)));
        assert_eq!(col.code_for("a"), Some(0));
        assert_eq!(col.code_for("b"), Some(1));
        assert_eq!(col.code_for("c"), Some(2));
        assert_eq!(col.code_for("z"), None);
    }

    // ---- filter_eq_dict_code ----

    #[test]
    fn filter_eq_dict_code_basic() {
        let col = dict(["a", "b", "a", "c", "b"].iter().map(|s| Some(*s)));
        let code_a = col.code_for("a").unwrap();
        let mask = filter_eq_dict_code(&col, code_a);
        assert!(mask.get(0));
        assert!(!mask.get(1));
        assert!(mask.get(2));
        assert!(!mask.get(3));
        assert!(!mask.get(4));
    }

    #[test]
    fn filter_eq_dict_code_excludes_nulls() {
        let col = dict([Some("a"), None, Some("a")]);
        let code_a = col.code_for("a").unwrap();
        let mask = filter_eq_dict_code(&col, code_a);
        assert!(mask.get(0));
        assert!(!mask.get(1)); // null
        assert!(mask.get(2));
    }

    #[test]
    fn filter_eq_dict_code_no_match_returns_all_zero() {
        let col = dict(["x", "y"].iter().map(|s| Some(*s)));
        let mask = filter_eq_dict_code(&col, 99); // code 99 does not exist
        assert_eq!(mask.count_ones(), 0);
    }

    // ---- group_by_dict ----

    #[test]
    fn group_by_dict_produces_correct_groups() {
        let col = dict(["a", "b", "a", "c", "b", "a"].iter().map(|s| Some(*s)));
        let groups = group_by_dict(&col);
        // 3 groups: a, b, c
        assert_eq!(groups.len(), 3);
        // Find group for "a" (code 0)
        let code_a = col.code_for("a").unwrap();
        let group_a = groups.iter().find(|(c, _)| *c == code_a).unwrap();
        assert_eq!(group_a.1, vec![0, 2, 5]);
    }

    #[test]
    fn group_by_dict_excludes_null_rows() {
        let col = dict([Some("x"), None, Some("x")]);
        let groups = group_by_dict(&col);
        assert_eq!(groups.len(), 1);
        let (_, rows) = &groups[0];
        assert_eq!(rows, &[0, 2]); // row 1 (null) excluded
    }

    #[test]
    fn group_by_dict_skips_out_of_range_codes() {
        let col = DictionaryColumn {
            dict: vec!["a".to_owned(), "b".to_owned()],
            codes: NumericColumn::from_data(vec![0, 2, 1]),
        };

        let groups = group_by_dict(&col);

        assert_eq!(groups, vec![(0, vec![0]), (1, vec![2])]);
    }

    #[test]
    fn decode_at_fails_closed_for_out_of_range_code() {
        let col = DictionaryColumn {
            dict: vec!["a".to_owned(), "b".to_owned()],
            codes: NumericColumn::from_data(vec![2]),
        };

        assert_eq!(col.decode_at(0), "");
    }

    #[test]
    fn try_decode_at_rejects_invalid_code_without_panic() {
        let col = DictionaryColumn {
            dict: vec!["a".to_owned()],
            codes: NumericColumn::from_data(vec![5]),
        };

        assert_eq!(col.try_decode_at(0), None);
    }

    #[test]
    fn dict_round_trip_empty() {
        let col = dict(std::iter::empty::<Option<&str>>());
        assert!(col.is_empty());
        assert!(col.dict.is_empty());
        assert_eq!(group_by_dict(&col).len(), 0);
    }

    #[test]
    fn auto_encoding_chooses_dictionary_for_low_cardinality() {
        let values: Vec<String> = (0..2048).map(|i| format!("code{}", i % 8)).collect();
        let encoded = encode_strings_auto(
            values.iter().map(|s| Some(s.as_str())),
            DictionaryEncodingPolicy::default(),
        );
        let StringEncoding::Dictionary(col) = encoded else {
            panic!("low-cardinality column should use dictionary encoding");
        };
        assert_eq!(col.len(), 2048);
        assert_eq!(col.dict.len(), 8);
        assert_eq!(col.decode_at(0), "code0");
        assert_eq!(col.decode_at(9), "code1");
    }

    #[test]
    fn auto_encoding_keeps_raw_for_high_cardinality() {
        let values: Vec<String> = (0..2048).map(|i| format!("v{i}")).collect();
        let encoded = encode_strings_auto(
            values.iter().map(|s| Some(s.as_str())),
            DictionaryEncodingPolicy::default(),
        );
        let StringEncoding::Raw(col) = encoded else {
            panic!("high-cardinality column should stay raw");
        };
        assert_eq!(col.len(), 2048);
        assert_eq!(col.value(17), "v17");
        assert!(col.nulls().is_none());
    }

    #[test]
    fn auto_encoding_keeps_raw_below_min_rows() {
        let values = ["x", "y", "x", "z"];
        let encoded = encode_strings_auto(
            values.iter().map(|s| Some(*s)),
            DictionaryEncodingPolicy::default(),
        );
        assert!(!encoded.is_dictionary());
    }

    #[test]
    fn auto_encoding_preserves_nulls_in_raw_and_dictionary() {
        let small_policy = DictionaryEncodingPolicy {
            min_rows: 1,
            max_distinct_values: 8,
            max_cardinality_percent: 67,
        };
        let dict = encode_strings_auto([Some("a"), None, Some("a"), Some("b")], small_policy);
        let StringEncoding::Dictionary(dict) = dict else {
            panic!("2 distinct / 3 non-null should dictionary-encode under test policy");
        };
        assert!(!dict.codes.nulls().expect("nullable dict").get(1));

        let raw = encode_strings_auto([Some("a"), None, Some("b"), Some("c")], small_policy);
        let StringEncoding::Raw(raw) = raw else {
            panic!("3 distinct / 3 non-null should stay raw under test policy");
        };
        assert!(!raw.nulls().expect("nullable raw").get(1));
    }
}
