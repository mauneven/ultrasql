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
use crate::column::NumericColumn;

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
#[derive(Clone, Debug)]
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
    pub fn from_strings<'a, I>(iter: I) -> Self
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
                        let c = dict.len().try_into().expect("dict size fits u32");
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
            NumericColumn::with_nulls(code_data, bm).expect("validity length matches data length")
        };

        Self { dict, codes }
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
    /// # Panics
    ///
    /// Panics if `i >= self.len()` or if row `i` is null (code == `u32::MAX`).
    #[must_use]
    pub fn decode_at(&self, i: usize) -> &str {
        let code = self.codes.data()[i];
        assert_ne!(code, u32::MAX, "decode_at called on null row {i}");
        &self.dict[code as usize]
    }

    /// Look up the dict code for `value`, or `None` if not present.
    #[must_use]
    pub fn code_for(&self, value: &str) -> Option<u32> {
        self.dict
            .iter()
            .position(|s| s == value)
            .map(|i| i.try_into().expect("dict position fits u32"))
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
        if valid && code != u32::MAX {
            let idx = code as usize;
            if idx < dict_size {
                groups[idx].push(i);
            }
        }
    }

    groups
        .into_iter()
        .enumerate()
        .filter(|(_, rows)| !rows.is_empty())
        .map(|(code, rows)| (code.try_into().expect("code fits u32"), rows))
        .collect()
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ---- DictionaryColumn::from_strings ----

    #[test]
    fn from_strings_deduplicates_values() {
        let col = DictionaryColumn::from_strings(
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
        let col = DictionaryColumn::from_strings(["x", "y", "x"].iter().map(|s| Some(*s)));
        assert_eq!(col.decode_at(0), "x");
        assert_eq!(col.decode_at(1), "y");
        assert_eq!(col.decode_at(2), "x");
    }

    #[test]
    fn from_strings_handles_nulls() {
        let col = DictionaryColumn::from_strings([Some("a"), None, Some("b"), None, Some("a")]);
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
        let col = DictionaryColumn::from_strings(["cat", "dog", "cat"].iter().map(|s| Some(*s)));
        assert_eq!(col.decode_at(0), "cat");
        assert_eq!(col.decode_at(1), "dog");
        assert_eq!(col.decode_at(2), "cat");
    }

    #[test]
    fn code_for_returns_correct_index() {
        let col = DictionaryColumn::from_strings(["a", "b", "c"].iter().map(|s| Some(*s)));
        assert_eq!(col.code_for("a"), Some(0));
        assert_eq!(col.code_for("b"), Some(1));
        assert_eq!(col.code_for("c"), Some(2));
        assert_eq!(col.code_for("z"), None);
    }

    // ---- filter_eq_dict_code ----

    #[test]
    fn filter_eq_dict_code_basic() {
        let col =
            DictionaryColumn::from_strings(["a", "b", "a", "c", "b"].iter().map(|s| Some(*s)));
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
        let col = DictionaryColumn::from_strings([Some("a"), None, Some("a")]);
        let code_a = col.code_for("a").unwrap();
        let mask = filter_eq_dict_code(&col, code_a);
        assert!(mask.get(0));
        assert!(!mask.get(1)); // null
        assert!(mask.get(2));
    }

    #[test]
    fn filter_eq_dict_code_no_match_returns_all_zero() {
        let col = DictionaryColumn::from_strings(["x", "y"].iter().map(|s| Some(*s)));
        let mask = filter_eq_dict_code(&col, 99); // code 99 does not exist
        assert_eq!(mask.count_ones(), 0);
    }

    // ---- group_by_dict ----

    #[test]
    fn group_by_dict_produces_correct_groups() {
        let col =
            DictionaryColumn::from_strings(["a", "b", "a", "c", "b", "a"].iter().map(|s| Some(*s)));
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
        let col = DictionaryColumn::from_strings([Some("x"), None, Some("x")]);
        let groups = group_by_dict(&col);
        assert_eq!(groups.len(), 1);
        let (_, rows) = &groups[0];
        assert_eq!(rows, &[0, 2]); // row 1 (null) excluded
    }

    #[test]
    fn dict_round_trip_empty() {
        let col = DictionaryColumn::from_strings(std::iter::empty::<Option<&str>>());
        assert!(col.is_empty());
        assert!(col.dict.is_empty());
        assert_eq!(group_by_dict(&col).len(), 0);
    }
}
