//! Text kernels over [`StringColumn`].
//!
//! Each kernel has a scalar reference implementation (`_scalar` suffix). The
//! production kernels are the source of truth for the executor; the `_scalar`
//! versions are kept as algorithmic oracles for property tests.
//!
//! # Coverage
//!
//! - [`len_text`] — byte length per row, materialised as an `i64` column.
//! - [`lower_text`] — ASCII lowercase per row (UTF-8 bytes ≥ 128 are passed
//!   through unchanged).
//! - [`upper_text`] — ASCII uppercase per row.
//!
//! # NULL handling
//!
//! Every kernel accepts an optional `&Bitmap` validity mask. Rows whose
//! validity bit is 0:
//!   - For [`len_text`] produce `0` in the output and the result validity
//!     bitmap marks those positions NULL.
//!   - For [`lower_text`] / [`upper_text`] produce an empty string slot and
//!     the result validity bitmap marks those positions NULL.
//!
//! # Encoding policy
//!
//! `lower_text` and `upper_text` perform **ASCII-only** case folding, matching
//! PostgreSQL's behaviour for the default `C` collation. Non-ASCII bytes are
//! copied through unchanged. A full Unicode case-folding kernel will land in
//! a follow-up alongside the ICU integration.

use crate::bitmap::Bitmap;
use crate::column::{ColumnError, NumericColumn, StringColumn};

// ============================================================================
// len_text
// ============================================================================

/// Per-row byte length of a [`StringColumn`], materialised as an `i64`
/// column. The output value at row `i` is `(offsets[i+1] - offsets[i]) as i64`
/// for non-null rows; null rows produce `0`.
///
/// The output column carries the merged validity bitmap of `column` and the
/// caller-supplied `validity` mask (if any). When both are present they are
/// AND-folded; when neither is present the output is non-nullable.
///
/// # Panics
///
/// Panics if `validity` is present and its length disagrees with the
/// column length.
#[must_use]
pub fn len_text(column: &StringColumn, validity: Option<&Bitmap>) -> NumericColumn<i64> {
    let n = column.len();
    let offsets = column.offsets();
    let mut data: Vec<i64> = Vec::with_capacity(n);
    for i in 0..n {
        data.push(i64::from(offsets[i + 1] - offsets[i]));
    }
    finalize_numeric_i64(data, column.nulls(), validity, n)
}

/// Scalar reference implementation for [`len_text`].
#[must_use]
pub fn len_text_scalar(column: &StringColumn, validity: Option<&Bitmap>) -> NumericColumn<i64> {
    let n = column.len();
    let mut data: Vec<i64> = Vec::with_capacity(n);
    for i in 0..n {
        let bytes = column.value(i).as_bytes();
        data.push(i64::try_from(bytes.len()).expect("row byte length fits in i64"));
    }
    finalize_numeric_i64(data, column.nulls(), validity, n)
}

// ============================================================================
// lower_text
// ============================================================================

/// Per-row ASCII lowercase of a [`StringColumn`]. Bytes in `A..=Z` are mapped
/// to `a..=z`; all other bytes pass through unchanged. The output column has
/// exactly the same row count and per-row byte length as the input — ASCII
/// case folding is byte-stable.
///
/// NULL rows produce empty strings; the merged validity bitmap (column nulls
/// AND-folded with the supplied `validity`) tags those positions as NULL.
///
/// # Panics
///
/// Panics if `validity` is present and its length disagrees with the
/// column length.
#[must_use]
pub fn lower_text(column: &StringColumn, validity: Option<&Bitmap>) -> StringColumn {
    let bytes = column.values();
    let mut out_values: Vec<u8> = Vec::with_capacity(bytes.len());
    for &b in bytes {
        out_values.push(if b.is_ascii_uppercase() { b + 32 } else { b });
    }
    finalize_string_with(column, out_values, validity)
}

/// Scalar reference implementation for [`lower_text`].
#[must_use]
pub fn lower_text_scalar(column: &StringColumn, validity: Option<&Bitmap>) -> StringColumn {
    let n = column.len();
    let mut rows: Vec<String> = Vec::with_capacity(n);
    for i in 0..n {
        let s = column.value(i);
        let mut buf = String::with_capacity(s.len());
        for ch in s.chars() {
            if ch.is_ascii_uppercase() {
                buf.push(ch.to_ascii_lowercase());
            } else {
                buf.push(ch);
            }
        }
        rows.push(buf);
    }
    finalize_string_from_rows(rows, column.nulls(), validity, n)
}

// ============================================================================
// upper_text
// ============================================================================

/// Per-row ASCII uppercase of a [`StringColumn`]. Bytes in `a..=z` are mapped
/// to `A..=Z`; all other bytes pass through unchanged.
///
/// NULL rows produce empty strings; the merged validity bitmap tags those
/// positions as NULL.
///
/// # Panics
///
/// Panics if `validity` is present and its length disagrees with the
/// column length.
#[must_use]
pub fn upper_text(column: &StringColumn, validity: Option<&Bitmap>) -> StringColumn {
    let bytes = column.values();
    let mut out_values: Vec<u8> = Vec::with_capacity(bytes.len());
    for &b in bytes {
        out_values.push(if b.is_ascii_lowercase() { b - 32 } else { b });
    }
    finalize_string_with(column, out_values, validity)
}

/// Scalar reference implementation for [`upper_text`].
#[must_use]
pub fn upper_text_scalar(column: &StringColumn, validity: Option<&Bitmap>) -> StringColumn {
    let n = column.len();
    let mut rows: Vec<String> = Vec::with_capacity(n);
    for i in 0..n {
        let s = column.value(i);
        let mut buf = String::with_capacity(s.len());
        for ch in s.chars() {
            if ch.is_ascii_lowercase() {
                buf.push(ch.to_ascii_uppercase());
            } else {
                buf.push(ch);
            }
        }
        rows.push(buf);
    }
    finalize_string_from_rows(rows, column.nulls(), validity, n)
}

// ============================================================================
// Helpers
// ============================================================================

/// Combine the column's own nulls bitmap with the caller-supplied `validity`
/// mask into a single bitmap. `None` if neither is present.
fn combined_validity(column_nulls: Option<&Bitmap>, validity: Option<&Bitmap>) -> Option<Bitmap> {
    match (column_nulls, validity) {
        (None, None) => None,
        (Some(a), None) | (None, Some(a)) => Some(a.clone()),
        (Some(a), Some(b)) => {
            assert_eq!(a.len(), b.len(), "validity length mismatch in text kernel");
            let mut merged = a.clone();
            for i in 0..a.len() {
                merged.set(i, a.get(i) && b.get(i));
            }
            Some(merged)
        }
    }
}

/// Wrap `data` in a [`NumericColumn<i64>`] honouring the combined validity.
fn finalize_numeric_i64(
    mut data: Vec<i64>,
    column_nulls: Option<&Bitmap>,
    validity: Option<&Bitmap>,
    n: usize,
) -> NumericColumn<i64> {
    let merged = combined_validity(column_nulls, validity);
    if let Some(bm) = merged {
        for (i, slot) in data.iter_mut().enumerate().take(n) {
            if !bm.get(i) {
                *slot = 0;
            }
        }
        match NumericColumn::with_nulls(data, bm) {
            Ok(c) => c,
            Err(ColumnError::LengthMismatch { bitmap, column }) => {
                panic!("finalize_numeric_i64: validity length {bitmap} != column length {column}")
            }
        }
    } else {
        NumericColumn::from_data(data)
    }
}

/// Build a [`StringColumn`] from the input column's offsets and the
/// (already case-folded) `values` buffer, applying the combined validity.
///
/// Null rows are masked by zeroing their byte range — done by rewriting the
/// offsets so the row's slice is empty — and the merged validity bitmap is
/// attached.
fn finalize_string_with(
    column: &StringColumn,
    values: Vec<u8>,
    validity: Option<&Bitmap>,
) -> StringColumn {
    let n = column.len();
    let merged = combined_validity(column.nulls(), validity);
    if let Some(ref bm) = merged {
        // Rebuild offsets so null rows have a zero-length slice. This is
        // the cleanest way to ensure no garbage bytes ever leak through the
        // string accessor.
        let mut new_offsets: Vec<u32> = Vec::with_capacity(n + 1);
        let mut new_values: Vec<u8> = Vec::with_capacity(values.len());
        new_offsets.push(0);
        let src_offsets = column.offsets();
        for i in 0..n {
            if bm.get(i) {
                let start = usize::try_from(src_offsets[i]).expect("offset fits in usize");
                let end = usize::try_from(src_offsets[i + 1]).expect("offset fits in usize");
                new_values.extend_from_slice(&values[start..end]);
            }
            let len_u32 =
                u32::try_from(new_values.len()).expect("text kernel total bytes fit in u32 offset");
            new_offsets.push(len_u32);
        }
        let rows: Vec<String> = (0..n)
            .map(|i| {
                let start = usize::try_from(new_offsets[i]).expect("offset fits in usize");
                let end = usize::try_from(new_offsets[i + 1]).expect("offset fits in usize");
                // `new_values` here is exclusively ASCII-stable transforms of
                // the source UTF-8 bytes (or zero-length null slots), so the
                // bytes are valid UTF-8.
                std::str::from_utf8(&new_values[start..end])
                    .expect("ASCII case fold preserves UTF-8")
                    .to_string()
            })
            .collect();
        match StringColumn::with_nulls(rows, bm.clone()) {
            Ok(c) => c,
            Err(ColumnError::LengthMismatch { bitmap, column }) => {
                panic!("finalize_string_with: validity length {bitmap} != column length {column}")
            }
        }
    } else {
        // No nulls — reuse the source column's offsets verbatim.
        let src_offsets = column.offsets();
        let rows: Vec<String> = (0..n)
            .map(|i| {
                let start = usize::try_from(src_offsets[i]).expect("offset fits in usize");
                let end = usize::try_from(src_offsets[i + 1]).expect("offset fits in usize");
                std::str::from_utf8(&values[start..end])
                    .expect("ASCII case fold preserves UTF-8")
                    .to_string()
            })
            .collect();
        StringColumn::from_data(rows)
    }
}

/// Wrap an explicit `Vec<String>` in a [`StringColumn`] honouring the
/// combined validity. Used by the scalar reference path which materialises
/// strings row-by-row.
fn finalize_string_from_rows(
    rows: Vec<String>,
    column_nulls: Option<&Bitmap>,
    validity: Option<&Bitmap>,
    n: usize,
) -> StringColumn {
    let merged = combined_validity(column_nulls, validity);
    if let Some(bm) = merged {
        let masked: Vec<String> = rows
            .into_iter()
            .enumerate()
            .map(|(i, s)| if bm.get(i) { s } else { String::new() })
            .collect();
        assert_eq!(masked.len(), n, "row count mismatch in text scalar kernel");
        match StringColumn::with_nulls(masked, bm) {
            Ok(c) => c,
            Err(ColumnError::LengthMismatch { bitmap, column }) => {
                panic!(
                    "finalize_string_from_rows: validity length {bitmap} != column length {column}"
                )
            }
        }
    } else {
        StringColumn::from_data(rows)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bitmap::Bitmap;

    fn col(rows: &[&str]) -> StringColumn {
        StringColumn::from_data(rows.iter().map(|s| (*s).to_string()))
    }

    // ---- len_text ----

    #[test]
    fn len_text_basic() {
        let c = col(&["alpha", "", "beta"]);
        let out = len_text(&c, None);
        assert_eq!(out.data(), &[5_i64, 0, 4]);
        assert!(out.nulls().is_none());
    }

    #[test]
    fn len_text_counts_bytes_not_chars() {
        // "é" is 2 UTF-8 bytes (0xC3 0xA9).
        let c = col(&["é", "ab"]);
        let out = len_text(&c, None);
        assert_eq!(out.data(), &[2_i64, 2]);
    }

    #[test]
    fn len_text_propagates_validity() {
        let c = col(&["alpha", "beta", "gamma"]);
        let mut bm = Bitmap::new(3, true);
        bm.set(1, false);
        let out = len_text(&c, Some(&bm));
        assert_eq!(out.data()[0], 5);
        assert_eq!(out.data()[1], 0); // null
        assert_eq!(out.data()[2], 5);
        let nulls = out.nulls().expect("nullable output");
        assert!(nulls.get(0));
        assert!(!nulls.get(1));
        assert!(nulls.get(2));
    }

    #[test]
    fn len_text_matches_scalar() {
        let c = col(&["", "x", "abc", "longerstring", "12345"]);
        let got = len_text(&c, None);
        let want = len_text_scalar(&c, None);
        assert_eq!(got.data(), want.data());
    }

    // ---- lower_text ----

    #[test]
    fn lower_text_basic() {
        let c = col(&["HELLO", "World!", "ALREADY-low"]);
        let out = lower_text(&c, None);
        assert_eq!(out.value(0), "hello");
        assert_eq!(out.value(1), "world!");
        assert_eq!(out.value(2), "already-low");
    }

    #[test]
    fn lower_text_preserves_non_ascii() {
        // "É" is 0xC3 0x89 in UTF-8 — must pass through unchanged.
        let c = col(&["É", "abcÉ"]);
        let out = lower_text(&c, None);
        assert_eq!(out.value(0), "É");
        assert_eq!(out.value(1), "abcÉ");
    }

    #[test]
    fn lower_text_matches_scalar() {
        let c = col(&["HELLO", "Mixed Case 123", "ALL UPPER", ""]);
        let got = lower_text(&c, None);
        let want = lower_text_scalar(&c, None);
        for i in 0..got.len() {
            assert_eq!(got.value(i), want.value(i), "row {i}");
        }
    }

    // ---- upper_text ----

    #[test]
    fn upper_text_basic() {
        let c = col(&["hello", "World!", "ALREADY-UP"]);
        let out = upper_text(&c, None);
        assert_eq!(out.value(0), "HELLO");
        assert_eq!(out.value(1), "WORLD!");
        assert_eq!(out.value(2), "ALREADY-UP");
    }

    #[test]
    fn upper_text_preserves_non_ascii() {
        let c = col(&["é", "abcé"]);
        let out = upper_text(&c, None);
        assert_eq!(out.value(0), "é");
        assert_eq!(out.value(1), "ABCé");
    }

    #[test]
    fn upper_text_matches_scalar() {
        let c = col(&["hello", "Mixed Case 123", "all lower", ""]);
        let got = upper_text(&c, None);
        let want = upper_text_scalar(&c, None);
        for i in 0..got.len() {
            assert_eq!(got.value(i), want.value(i), "row {i}");
        }
    }

    #[test]
    fn upper_text_propagates_validity() {
        let c = col(&["hello", "world", "rust"]);
        let mut bm = Bitmap::new(3, true);
        bm.set(0, false);
        let out = upper_text(&c, Some(&bm));
        assert_eq!(out.value(0), "");
        assert_eq!(out.value(1), "WORLD");
        assert_eq!(out.value(2), "RUST");
        let nulls = out.nulls().expect("nullable output");
        assert!(!nulls.get(0));
        assert!(nulls.get(1));
        assert!(nulls.get(2));
    }

    // ========================================================================
    // Property tests — each kernel cross-checked against `_scalar` reference
    // over at least 1024 random inputs.
    // ========================================================================

    fn arb_string_column() -> impl proptest::strategy::Strategy<Value = StringColumn> {
        use proptest::collection::vec;
        use proptest::prelude::any;
        use proptest::strategy::Strategy;
        vec(any::<String>(), 0_usize..=100).prop_map(StringColumn::from_data)
    }

    proptest::proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig {
            cases: 1024, .. proptest::prelude::ProptestConfig::default()
        })]

        #[test]
        fn prop_len_text_matches_scalar(c in arb_string_column()) {
            let got = len_text(&c, None);
            let want = len_text_scalar(&c, None);
            proptest::prop_assert_eq!(got.data(), want.data());
        }

        #[test]
        fn prop_lower_text_matches_scalar(c in arb_string_column()) {
            let got = lower_text(&c, None);
            let want = lower_text_scalar(&c, None);
            proptest::prop_assert_eq!(got.len(), want.len());
            for i in 0..got.len() {
                proptest::prop_assert_eq!(got.value(i), want.value(i));
            }
        }

        #[test]
        fn prop_upper_text_matches_scalar(c in arb_string_column()) {
            let got = upper_text(&c, None);
            let want = upper_text_scalar(&c, None);
            proptest::prop_assert_eq!(got.len(), want.len());
            for i in 0..got.len() {
                proptest::prop_assert_eq!(got.value(i), want.value(i));
            }
        }
    }
}
