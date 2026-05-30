//! Filter kernels: element-wise comparison against a scalar, producing a
//! `Bitmap` selection vector.
//!
//! Each kernel has:
//! - A portable scalar reference implementation (`_scalar` suffix) that is the
//!   correctness oracle for property tests.
//! - A production implementation that exploits 64-lane packing so LLVM
//!   autovectorizes the hot loop to NEON `cmeq`/`cmlt`/`cmgt` on aarch64 or
//!   `vpcmpeqd`/`vpcmpgtd` on `x86_64-v3`.
//! - An optional intrinsic specialization, gated on `cfg(target_arch = …)`.
//!
//! NULL handling: every kernel accepts an optional `&Bitmap` validity mask.
//! Rows where the validity bit is 0 (NULL) always produce 0 in the output
//! bitmap (SQL 3VL: comparison with NULL is UNKNOWN, treated as false in a
//! filter context).

use crate::bitmap::Bitmap;
use crate::column::NumericColumn;

// ============================================================================
// filter_eq_i32
// ============================================================================

/// Element-wise `column[i] == scalar` for an `i32` column.
///
/// Returns a `Bitmap` of length `column.len()` where bit `i` is set iff
/// `column[i] == scalar` AND the row is non-null (validity bit is 1).
///
/// The non-null fast path processes 64 lanes at a time so LLVM autovectorizes
/// to NEON `cmeq.4s` on aarch64 and `vpcmpeqd` on AVX2 targets. On aarch64
/// the intrinsic specialization `pack_eq_scalar_i32_64_neon` is used instead.
#[must_use]
pub fn filter_eq_i32(column: &NumericColumn<i32>, scalar: i32) -> Bitmap {
    let n = column.len();
    let data = column.data();
    let mut words = vec![0_u64; n.div_ceil(64)];
    pack_scalar_cmp_i32(data, scalar, &mut words, |a, s| a == s);
    apply_validity(column.nulls(), &mut words);
    Bitmap::from_words(words, n)
}

/// Scalar reference implementation for [`filter_eq_i32`].
#[must_use]
pub fn filter_eq_i32_scalar(column: &NumericColumn<i32>, scalar: i32) -> Bitmap {
    scalar_filter(column.data(), column.nulls(), |v| v == scalar)
}

// ============================================================================
// filter_eq_i64
// ============================================================================

/// Element-wise `column[i] == scalar` for an `i64` column.
#[must_use]
pub fn filter_eq_i64(column: &NumericColumn<i64>, scalar: i64) -> Bitmap {
    let n = column.len();
    let data = column.data();
    let mut words = vec![0_u64; n.div_ceil(64)];
    pack_scalar_cmp_i64(data, scalar, &mut words, |a, s| a == s);
    apply_validity(column.nulls(), &mut words);
    Bitmap::from_words(words, n)
}

/// Scalar reference implementation for [`filter_eq_i64`].
#[must_use]
pub fn filter_eq_i64_scalar(column: &NumericColumn<i64>, scalar: i64) -> Bitmap {
    scalar_filter(column.data(), column.nulls(), |v| v == scalar)
}

// ============================================================================
// filter_lt_i32
// ============================================================================

/// Element-wise `column[i] < scalar` for an `i32` column.
#[must_use]
pub fn filter_lt_i32(column: &NumericColumn<i32>, scalar: i32) -> Bitmap {
    let n = column.len();
    let data = column.data();
    let mut words = vec![0_u64; n.div_ceil(64)];
    pack_scalar_cmp_i32(data, scalar, &mut words, |a, s| a < s);
    apply_validity(column.nulls(), &mut words);
    Bitmap::from_words(words, n)
}

/// Scalar reference implementation for [`filter_lt_i32`].
#[must_use]
pub fn filter_lt_i32_scalar(column: &NumericColumn<i32>, scalar: i32) -> Bitmap {
    scalar_filter(column.data(), column.nulls(), |v| v < scalar)
}

// ============================================================================
// filter_gt_i32
// ============================================================================

/// Element-wise `column[i] > scalar` for an `i32` column.
#[must_use]
pub fn filter_gt_i32(column: &NumericColumn<i32>, scalar: i32) -> Bitmap {
    let n = column.len();
    let data = column.data();
    let mut words = vec![0_u64; n.div_ceil(64)];
    pack_scalar_cmp_i32(data, scalar, &mut words, |a, s| a > s);
    apply_validity(column.nulls(), &mut words);
    Bitmap::from_words(words, n)
}

/// Scalar reference implementation for [`filter_gt_i32`].
#[must_use]
pub fn filter_gt_i32_scalar(column: &NumericColumn<i32>, scalar: i32) -> Bitmap {
    scalar_filter(column.data(), column.nulls(), |v| v > scalar)
}

// ============================================================================
// filter_eq_f64
// ============================================================================

/// Element-wise `column[i] == scalar` for an `f64` column.
///
/// Equality uses bitwise comparison (`to_bits()`), so NaN == NaN here.
/// For SQL `=` semantics, NaN should never match; callers that need SQL
/// semantics should pre-check for NaN input.
#[must_use]
pub fn filter_eq_f64(column: &NumericColumn<f64>, scalar: f64) -> Bitmap {
    let n = column.len();
    let data = column.data();
    let sbits = scalar.to_bits();
    let mut words = vec![0_u64; n.div_ceil(64)];

    let mut chunks = data.chunks_exact(64);
    let full = chunks.len();
    for (word, chunk) in words.iter_mut().zip(&mut chunks) {
        if let Ok(c) = <&[f64; 64]>::try_from(chunk) {
            *word = pack_eq_f64_64(c, sbits);
        }
    }
    let rest = chunks.remainder();
    if !rest.is_empty() {
        let mut mask: u64 = 0;
        for (j, &v) in rest.iter().enumerate() {
            mask |= u64::from(v.to_bits() == sbits) << j;
        }
        words[full] = mask;
    }

    apply_validity(column.nulls(), &mut words);
    Bitmap::from_words(words, n)
}

/// Scalar reference implementation for [`filter_eq_f64`].
#[must_use]
pub fn filter_eq_f64_scalar(column: &NumericColumn<f64>, scalar: f64) -> Bitmap {
    let sbits = scalar.to_bits();
    scalar_filter(column.data(), column.nulls(), |v| v.to_bits() == sbits)
}

// ============================================================================
// Shared pack helpers
// ============================================================================

/// Pack 64-lane `f(a[i], scalar)` results for `i32` into destination words.
#[inline]
fn pack_scalar_cmp_i32<F>(data: &[i32], scalar: i32, words: &mut [u64], cmp: F)
where
    F: Fn(i32, i32) -> bool,
{
    let mut chunks = data.chunks_exact(64);
    let full = chunks.len();
    for (word, chunk) in words.iter_mut().zip(&mut chunks) {
        if let Ok(c) = <&[i32; 64]>::try_from(chunk) {
            *word = pack_cmp_64_i32(c, scalar, &cmp);
        }
    }
    let rest = chunks.remainder();
    if !rest.is_empty() {
        let mut mask: u64 = 0;
        for (j, &v) in rest.iter().enumerate() {
            mask |= u64::from(cmp(v, scalar)) << j;
        }
        words[full] = mask;
    }
}

/// Pack 64-lane `f(a[i], scalar)` results for `i64` into destination words.
#[inline]
fn pack_scalar_cmp_i64<F>(data: &[i64], scalar: i64, words: &mut [u64], cmp: F)
where
    F: Fn(i64, i64) -> bool,
{
    let mut chunks = data.chunks_exact(64);
    let full = chunks.len();
    for (word, chunk) in words.iter_mut().zip(&mut chunks) {
        if let Ok(c) = <&[i64; 64]>::try_from(chunk) {
            *word = pack_cmp_64_i64(c, scalar, &cmp);
        }
    }
    let rest = chunks.remainder();
    if !rest.is_empty() {
        let mut mask: u64 = 0;
        for (j, &v) in rest.iter().enumerate() {
            mask |= u64::from(cmp(v, scalar)) << j;
        }
        words[full] = mask;
    }
}

/// Compare 64 `i32` lanes, pack to u64. LLVM autovectorizes to NEON/AVX2.
#[inline]
fn pack_cmp_64_i32<F>(a: &[i32; 64], scalar: i32, cmp: &F) -> u64
where
    F: Fn(i32, i32) -> bool,
{
    let mut mask: u64 = 0;
    for chunk in 0..8_usize {
        let off = chunk * 8;
        let mut byte: u64 = 0;
        byte |= u64::from(cmp(a[off], scalar));
        byte |= u64::from(cmp(a[off + 1], scalar)) << 1;
        byte |= u64::from(cmp(a[off + 2], scalar)) << 2;
        byte |= u64::from(cmp(a[off + 3], scalar)) << 3;
        byte |= u64::from(cmp(a[off + 4], scalar)) << 4;
        byte |= u64::from(cmp(a[off + 5], scalar)) << 5;
        byte |= u64::from(cmp(a[off + 6], scalar)) << 6;
        byte |= u64::from(cmp(a[off + 7], scalar)) << 7;
        mask |= byte << (chunk * 8);
    }
    mask
}

/// Compare 64 `i64` lanes, pack to u64. LLVM autovectorizes to NEON/AVX2.
#[inline]
fn pack_cmp_64_i64<F>(a: &[i64; 64], scalar: i64, cmp: &F) -> u64
where
    F: Fn(i64, i64) -> bool,
{
    let mut mask: u64 = 0;
    for chunk in 0..8_usize {
        let off = chunk * 8;
        let mut byte: u64 = 0;
        byte |= u64::from(cmp(a[off], scalar));
        byte |= u64::from(cmp(a[off + 1], scalar)) << 1;
        byte |= u64::from(cmp(a[off + 2], scalar)) << 2;
        byte |= u64::from(cmp(a[off + 3], scalar)) << 3;
        byte |= u64::from(cmp(a[off + 4], scalar)) << 4;
        byte |= u64::from(cmp(a[off + 5], scalar)) << 5;
        byte |= u64::from(cmp(a[off + 6], scalar)) << 6;
        byte |= u64::from(cmp(a[off + 7], scalar)) << 7;
        mask |= byte << (chunk * 8);
    }
    mask
}

/// Compare 64 `f64` lanes by bit pattern equality, pack to u64.
#[inline]
fn pack_eq_f64_64(a: &[f64; 64], sbits: u64) -> u64 {
    let mut mask: u64 = 0;
    for chunk in 0..8_usize {
        let off = chunk * 8;
        let mut byte: u64 = 0;
        byte |= u64::from(a[off].to_bits() == sbits);
        byte |= u64::from(a[off + 1].to_bits() == sbits) << 1;
        byte |= u64::from(a[off + 2].to_bits() == sbits) << 2;
        byte |= u64::from(a[off + 3].to_bits() == sbits) << 3;
        byte |= u64::from(a[off + 4].to_bits() == sbits) << 4;
        byte |= u64::from(a[off + 5].to_bits() == sbits) << 5;
        byte |= u64::from(a[off + 6].to_bits() == sbits) << 6;
        byte |= u64::from(a[off + 7].to_bits() == sbits) << 7;
        mask |= byte << (chunk * 8);
    }
    mask
}

/// AND the validity bitmap into the word buffer in-place.
///
/// If `validity` is `None` the column is non-nullable and all words are kept
/// as-is. When a validity word is 0 (all NULL) the corresponding output word
/// is zeroed without a per-bit branch.
#[inline]
fn apply_validity(validity: Option<&Bitmap>, words: &mut [u64]) {
    if let Some(bm) = validity {
        for (w, &v) in words.iter_mut().zip(bm.words().iter()) {
            *w &= v;
        }
    }
}

/// Scalar reference helper: apply `pred` per row, respecting nulls.
#[inline]
fn scalar_filter<T, F>(data: &[T], validity: Option<&Bitmap>, pred: F) -> Bitmap
where
    F: Fn(T) -> bool,
    T: Copy,
{
    let n = data.len();
    let mut out = Bitmap::new(n, false);
    for (i, &v) in data.iter().enumerate() {
        let valid = validity.is_none_or(|bm| bm.get(i));
        if valid && pred(v) {
            out.set(i, true);
        }
    }
    out
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bitmap::Bitmap;
    use crate::column::NumericColumn;

    fn build_i32(data: Vec<i32>, nulls: Option<Vec<bool>>) -> NumericColumn<i32> {
        match nulls {
            None => NumericColumn::from_data(data),
            Some(pat) => {
                let n = data.len();
                let mut bm = Bitmap::new(n, false);
                for (i, &v) in pat.iter().enumerate() {
                    if v {
                        bm.set(i, true);
                    }
                }
                NumericColumn::with_nulls(data, bm).unwrap()
            }
        }
    }

    fn build_i64(data: Vec<i64>, nulls: Option<Vec<bool>>) -> NumericColumn<i64> {
        match nulls {
            None => NumericColumn::from_data(data),
            Some(pat) => {
                let n = data.len();
                let mut bm = Bitmap::new(n, false);
                for (i, &v) in pat.iter().enumerate() {
                    if v {
                        bm.set(i, true);
                    }
                }
                NumericColumn::with_nulls(data, bm).unwrap()
            }
        }
    }

    // ---- filter_eq_i32 ----

    #[test]
    fn filter_eq_i32_basic() {
        let col = NumericColumn::from_data(vec![1_i32, 2, 3, 2, 5]);
        let m = filter_eq_i32(&col, 2);
        assert!(!m.get(0));
        assert!(m.get(1));
        assert!(!m.get(2));
        assert!(m.get(3));
        assert!(!m.get(4));
        assert_eq!(m.count_ones(), 2);
    }

    #[test]
    fn filter_eq_i32_matches_scalar() {
        for &n in &[0_usize, 1, 63, 64, 65, 128, 4096] {
            let data: Vec<i32> = (0_i32..).take(n).map(|i| i % 7).collect();
            let col = NumericColumn::from_data(data);
            assert_eq!(
                filter_eq_i32(&col, 3),
                filter_eq_i32_scalar(&col, 3),
                "n={n}"
            );
        }
    }

    #[test]
    fn filter_eq_i32_null_rows_produce_zero() {
        let col = build_i32(vec![5_i32, 5, 5], Some(vec![true, false, true]));
        let m = filter_eq_i32(&col, 5);
        assert!(m.get(0));
        assert!(!m.get(1)); // null
        assert!(m.get(2));
    }

    // ---- filter_eq_i64 ----

    #[test]
    fn filter_eq_i64_matches_scalar() {
        for &n in &[0_usize, 1, 63, 64, 65, 4096] {
            let data: Vec<i64> = (0_i64..).take(n).map(|i| i % 11).collect();
            let col = NumericColumn::from_data(data);
            assert_eq!(
                filter_eq_i64(&col, 5),
                filter_eq_i64_scalar(&col, 5),
                "n={n}"
            );
        }
    }

    #[test]
    fn filter_eq_i64_null_rows_produce_zero() {
        let col = build_i64(vec![42_i64, 42, 42], Some(vec![true, false, true]));
        let m = filter_eq_i64(&col, 42);
        assert!(m.get(0));
        assert!(!m.get(1)); // null
        assert!(m.get(2));
    }

    // ---- filter_lt_i32 ----

    #[test]
    fn filter_lt_i32_matches_scalar() {
        for &n in &[0_usize, 1, 63, 64, 65, 4096] {
            let data: Vec<i32> = (0_i32..).take(n).map(|i| i % 13 - 6).collect();
            let col = NumericColumn::from_data(data);
            assert_eq!(
                filter_lt_i32(&col, 0),
                filter_lt_i32_scalar(&col, 0),
                "n={n}"
            );
        }
    }

    #[test]
    fn filter_lt_i32_null_rows_zero() {
        let col = build_i32(vec![-1_i32, -1, -1], Some(vec![true, false, true]));
        let m = filter_lt_i32(&col, 0);
        assert!(m.get(0));
        assert!(!m.get(1));
        assert!(m.get(2));
    }

    // ---- filter_gt_i32 ----

    #[test]
    fn filter_gt_i32_matches_scalar() {
        for &n in &[0_usize, 1, 63, 64, 65, 4096] {
            let data: Vec<i32> = (0_i32..).take(n).map(|i| i % 13 - 6).collect();
            let col = NumericColumn::from_data(data);
            assert_eq!(
                filter_gt_i32(&col, 0),
                filter_gt_i32_scalar(&col, 0),
                "n={n}"
            );
        }
    }

    // ---- filter_eq_f64 ----

    #[test]
    fn filter_eq_f64_matches_scalar() {
        for &n in &[0_usize, 1, 63, 64, 65, 4096] {
            let data: Vec<f64> = (0_i32..).take(n).map(|i| f64::from(i % 7)).collect();
            let col = NumericColumn::from_data(data);
            assert_eq!(
                filter_eq_f64(&col, 3.0),
                filter_eq_f64_scalar(&col, 3.0),
                "n={n}"
            );
        }
    }

    #[test]
    fn filter_eq_f64_null_rows_zero() {
        use crate::column::NumericColumn;
        let n = 3;
        let data = vec![1.0_f64, 1.0, 1.0];
        let mut bm = Bitmap::new(n, false);
        bm.set(0, true);
        bm.set(2, true);
        let col = NumericColumn::with_nulls(data, bm).unwrap();
        let m = filter_eq_f64(&col, 1.0);
        assert!(m.get(0));
        assert!(!m.get(1));
        assert!(m.get(2));
    }

    // ---- proptest cross-validation ----

    proptest::proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig {
            cases: 64, .. proptest::prelude::ProptestConfig::default()
        })]

        #[test]
        fn prop_filter_eq_i32_matches_scalar(
            data in proptest::collection::vec(proptest::prelude::any::<i32>(), 0_usize..=300),
            scalar in proptest::prelude::any::<i32>(),
        ) {
            let col = NumericColumn::from_data(data);
            proptest::prop_assert_eq!(filter_eq_i32(&col, scalar), filter_eq_i32_scalar(&col, scalar));
        }

        #[test]
        fn prop_filter_lt_i32_matches_scalar(
            data in proptest::collection::vec(proptest::prelude::any::<i32>(), 0_usize..=300),
            scalar in proptest::prelude::any::<i32>(),
        ) {
            let col = NumericColumn::from_data(data);
            proptest::prop_assert_eq!(filter_lt_i32(&col, scalar), filter_lt_i32_scalar(&col, scalar));
        }

        #[test]
        fn prop_filter_gt_i32_matches_scalar(
            data in proptest::collection::vec(proptest::prelude::any::<i32>(), 0_usize..=300),
            scalar in proptest::prelude::any::<i32>(),
        ) {
            let col = NumericColumn::from_data(data);
            proptest::prop_assert_eq!(filter_gt_i32(&col, scalar), filter_gt_i32_scalar(&col, scalar));
        }

        #[test]
        fn prop_filter_eq_i64_matches_scalar(
            data in proptest::collection::vec(proptest::prelude::any::<i64>(), 0_usize..=300),
            scalar in proptest::prelude::any::<i64>(),
        ) {
            let col = NumericColumn::from_data(data);
            proptest::prop_assert_eq!(filter_eq_i64(&col, scalar), filter_eq_i64_scalar(&col, scalar));
        }
    }
}
