//! Vectorized kernels.
//!
//! Each kernel here has a scalar (auto-vectorizable) implementation
//! that is the source of truth. SIMD specializations land alongside
//! the scalar versions and are validated bit-for-bit against scalar
//! in property tests.

use crate::bitmap::Bitmap;
use crate::column::NumericColumn;

/// Element-wise `a == b` over two `i32` columns of equal length.
///
/// The output is a `Bitmap` of length `n` where bit `i` is set iff
/// `a[i] == b[i]`. NULLs (if any) produce a 0 bit. SQL NULL semantics
/// say a comparison with NULL is UNKNOWN, treated as false here for
/// the filter context — a separate three-valued logic kernel will
/// arrive for general WHERE evaluation.
///
/// # Panics
///
/// Panics if the two columns disagree on length. The caller is
/// responsible for upstream length validation.
#[must_use]
pub fn eq_i32(a: &NumericColumn<i32>, b: &NumericColumn<i32>) -> Bitmap {
    assert_eq!(a.len(), b.len(), "eq_i32: column length mismatch");
    let n = a.len();
    let mut out = Bitmap::new(n, false);
    let (xa, xb) = (a.data(), b.data());

    // Auto-vectorizable loop. LLVM produces NEON code on aarch64-apple-m1
    // and AVX2 on x86-64-v3 without intrinsics for this shape.
    for i in 0..n {
        let matched = xa[i] == xb[i];
        let nulls_ok = a.nulls().is_none_or(|m| m.get(i)) && b.nulls().is_none_or(|m| m.get(i));
        if matched && nulls_ok {
            out.set(i, true);
        }
    }
    out
}

/// Sum of a non-null `i64` column. NULL entries are skipped.
#[must_use]
pub fn sum_i64(column: &NumericColumn<i64>) -> i64 {
    column.nulls().map_or_else(
        || column.data().iter().fold(0_i64, |a, b| a.wrapping_add(*b)),
        |nulls| {
            let mut s: i64 = 0;
            for (i, v) in column.data().iter().enumerate() {
                if nulls.get(i) {
                    s = s.wrapping_add(*v);
                }
            }
            s
        },
    )
}

/// Min of a non-null `f64` column. Returns `None` on empty / all-null
/// input. Honors IEEE-754 semantics for NaN: NaN values are skipped.
#[must_use]
pub fn min_f64(column: &NumericColumn<f64>) -> Option<f64> {
    let mut best: Option<f64> = None;
    if let Some(nulls) = column.nulls() {
        for (i, &v) in column.data().iter().enumerate() {
            if !nulls.get(i) || v.is_nan() {
                continue;
            }
            best = Some(best.map_or(v, |b| if v < b { v } else { b }));
        }
    } else {
        for &v in column.data() {
            if v.is_nan() {
                continue;
            }
            best = Some(best.map_or(v, |b| if v < b { v } else { b }));
        }
    }
    best
}

/// Materialize the rows of `column` selected by `selection`. The
/// length of the output equals `selection.count_ones()`.
#[must_use]
pub fn select_i32(column: &NumericColumn<i32>, selection: &Bitmap) -> NumericColumn<i32> {
    assert_eq!(
        column.len(),
        selection.len(),
        "select_i32: selection length mismatch"
    );
    let take = selection.count_ones();
    let mut out = Vec::with_capacity(take);
    for i in selection.iter_ones() {
        out.push(column.data()[i]);
    }
    NumericColumn::from_data(out)
}

/// Count of non-null entries in an `i64` column.
///
/// For a non-nullable column this is exactly `column.len()`. With a
/// validity bitmap, it is the popcount of the bitmap. Both branches
/// are O(n) but the bitmap branch runs over 64x fewer words.
#[must_use]
pub fn count_i64(column: &NumericColumn<i64>) -> usize {
    column
        .nulls()
        .map_or_else(|| column.len(), Bitmap::count_ones)
}

/// Min of a non-null `i64` column. Returns `None` on empty / all-null
/// input.
///
/// The non-nullable fast path is a single auto-vectorizable fold;
/// LLVM emits NEON `smin` on aarch64 and AVX2 `pminsq` on x86-64-v3.
#[must_use]
pub fn min_i64(column: &NumericColumn<i64>) -> Option<i64> {
    column.nulls().map_or_else(
        || {
            // Seed with the first element and fold; this shape vectorizes.
            let data = column.data();
            let (first, rest) = data.split_first()?;
            let mut best = *first;
            for &v in rest {
                if v < best {
                    best = v;
                }
            }
            Some(best)
        },
        |nulls| {
            let mut best: Option<i64> = None;
            for (i, &v) in column.data().iter().enumerate() {
                if !nulls.get(i) {
                    continue;
                }
                best = Some(best.map_or(v, |b| if v < b { v } else { b }));
            }
            best
        },
    )
}

/// Max of a non-null `i64` column. Returns `None` on empty / all-null
/// input. Same shape as [`min_i64`]; LLVM uses the lane-wise `smax`
/// reduction.
#[must_use]
pub fn max_i64(column: &NumericColumn<i64>) -> Option<i64> {
    column.nulls().map_or_else(
        || {
            let data = column.data();
            let (first, rest) = data.split_first()?;
            let mut best = *first;
            for &v in rest {
                if v > best {
                    best = v;
                }
            }
            Some(best)
        },
        |nulls| {
            let mut best: Option<i64> = None;
            for (i, &v) in column.data().iter().enumerate() {
                if !nulls.get(i) {
                    continue;
                }
                best = Some(best.map_or(v, |b| if v > b { v } else { b }));
            }
            best
        },
    )
}

/// Element-wise `a > scalar` over an `i64` column. The output is a
/// `Bitmap` of length `column.len()` where bit `i` is set iff
/// `a[i] > scalar` AND the row is non-null.
///
/// # Panics
///
/// Cannot panic: validity is read through the column's bitmap; the
/// output bitmap is created with the right length.
#[must_use]
pub fn cmp_gt_i64(column: &NumericColumn<i64>, scalar: i64) -> Bitmap {
    let n = column.len();
    let mut out = Bitmap::new(n, false);
    if let Some(nulls) = column.nulls() {
        for (i, &v) in column.data().iter().enumerate() {
            if nulls.get(i) && v > scalar {
                out.set(i, true);
            }
        }
    } else {
        // Auto-vectorizable branchless loop. LLVM emits NEON `cmgt`
        // on aarch64 and AVX2 `vpcmpgtq` on x86-64-v3.
        for (i, &v) in column.data().iter().enumerate() {
            if v > scalar {
                out.set(i, true);
            }
        }
    }
    out
}

/// Sum of an `i64` column with an external mask. Only rows whose
/// `mask` bit is set contribute. Independent of the column's own
/// validity bitmap — the caller is responsible for combining masks.
///
/// # Panics
///
/// Panics if `column.len() != mask.len()`.
#[must_use]
pub fn sum_i64_with_mask(column: &NumericColumn<i64>, mask: &Bitmap) -> i64 {
    assert_eq!(
        column.len(),
        mask.len(),
        "sum_i64_with_mask: length mismatch",
    );
    let data = column.data();
    let mut s: i64 = 0;
    for i in mask.iter_ones() {
        s = s.wrapping_add(data[i]);
    }
    s
}

/// Range mask: bit `i` is set iff `lo <= column[i] <= hi` AND row is
/// non-null. Inclusive on both ends, matching SQL `BETWEEN`.
///
/// # Panics
///
/// Cannot panic for valid inputs.
#[must_use]
pub fn range_mask_i64(column: &NumericColumn<i64>, lo: i64, hi: i64) -> Bitmap {
    let n = column.len();
    let mut out = Bitmap::new(n, false);
    if let Some(nulls) = column.nulls() {
        for (i, &v) in column.data().iter().enumerate() {
            if nulls.get(i) && v >= lo && v <= hi {
                out.set(i, true);
            }
        }
    } else {
        for (i, &v) in column.data().iter().enumerate() {
            if v >= lo && v <= hi {
                out.set(i, true);
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eq_matches_scalar() {
        let a = NumericColumn::from_data(vec![1_i32, 2, 3, 4, 2]);
        let b = NumericColumn::from_data(vec![1_i32, 2, 3, 5, 9]);
        let mask = eq_i32(&a, &b);
        assert!(mask.get(0));
        assert!(mask.get(1));
        assert!(mask.get(2));
        assert!(!mask.get(3));
        assert!(!mask.get(4));
        assert_eq!(mask.count_ones(), 3);
    }

    #[test]
    fn eq_with_nulls_produces_zero_at_null() {
        let a_data = vec![1_i32, 2, 3, 4];
        let mut a_nulls = Bitmap::new(4, true);
        a_nulls.set(1, false);
        let a = NumericColumn::with_nulls(a_data, a_nulls).unwrap();
        let b = NumericColumn::from_data(vec![1_i32, 2, 3, 4]);
        let mask = eq_i32(&a, &b);
        assert!(mask.get(0));
        assert!(!mask.get(1)); // null in a
        assert!(mask.get(2));
        assert!(mask.get(3));
    }

    #[test]
    fn sum_i64_basic() {
        let c = NumericColumn::from_data(vec![1_i64, 2, 3, 4]);
        assert_eq!(sum_i64(&c), 10);
    }

    #[test]
    fn sum_i64_with_nulls_skips_them() {
        let mut nulls = Bitmap::new(4, true);
        nulls.set(2, false);
        let c = NumericColumn::with_nulls(vec![10_i64, 20, 99, 40], nulls).unwrap();
        assert_eq!(sum_i64(&c), 70);
    }

    #[test]
    fn min_f64_skips_nan_and_nulls() {
        let mut nulls = Bitmap::new(5, true);
        nulls.set(0, false);
        let c = NumericColumn::with_nulls(vec![f64::NAN, 1.0, 0.5, f64::NAN, 2.0], nulls).unwrap();
        // Row 0 null, rows 1/4 are 1.0/2.0, row 2 = 0.5, row 3 NaN.
        assert_eq!(min_f64(&c), Some(0.5));
    }

    #[test]
    fn min_f64_all_null_returns_none() {
        let nulls = Bitmap::new(3, false);
        let c = NumericColumn::with_nulls(vec![1.0_f64, 2.0, 3.0], nulls).unwrap();
        assert_eq!(min_f64(&c), None);
    }

    #[test]
    fn select_i32_materializes_subset() {
        let c = NumericColumn::from_data(vec![10_i32, 20, 30, 40, 50]);
        let mut sel = Bitmap::new(5, false);
        sel.set(0, true);
        sel.set(2, true);
        sel.set(4, true);
        let out = select_i32(&c, &sel);
        assert_eq!(out.data(), &[10, 30, 50]);
    }

    #[test]
    fn count_i64_no_nulls_is_length() {
        let c = NumericColumn::from_data(vec![1_i64, 2, 3, 4, 5]);
        assert_eq!(count_i64(&c), 5);
    }

    #[test]
    fn count_i64_skips_nulls() {
        let mut nulls = Bitmap::new(4, true);
        nulls.set(1, false);
        nulls.set(2, false);
        let c = NumericColumn::with_nulls(vec![10_i64, 20, 30, 40], nulls).unwrap();
        assert_eq!(count_i64(&c), 2);
    }

    #[test]
    fn min_i64_basic_and_negative() {
        let c = NumericColumn::from_data(vec![5_i64, -3, 7, 0, -100, 42]);
        assert_eq!(min_i64(&c), Some(-100));
    }

    #[test]
    fn min_i64_empty_returns_none() {
        let c = NumericColumn::<i64>::from_data(vec![]);
        assert_eq!(min_i64(&c), None);
    }

    #[test]
    fn min_i64_with_nulls_skips_them() {
        let mut nulls = Bitmap::new(4, true);
        nulls.set(2, false); // would-be minimum is masked out
        let c = NumericColumn::with_nulls(vec![10_i64, 20, -999, 30], nulls).unwrap();
        assert_eq!(min_i64(&c), Some(10));
    }

    #[test]
    fn min_i64_all_null_returns_none() {
        let nulls = Bitmap::new(3, false);
        let c = NumericColumn::with_nulls(vec![1_i64, 2, 3], nulls).unwrap();
        assert_eq!(min_i64(&c), None);
    }

    #[test]
    fn max_i64_basic_and_negative() {
        let c = NumericColumn::from_data(vec![5_i64, -3, 7, 0, -100, 42]);
        assert_eq!(max_i64(&c), Some(42));
    }

    #[test]
    fn max_i64_empty_returns_none() {
        let c = NumericColumn::<i64>::from_data(vec![]);
        assert_eq!(max_i64(&c), None);
    }

    #[test]
    fn max_i64_with_nulls_skips_them() {
        let mut nulls = Bitmap::new(4, true);
        nulls.set(2, false); // would-be maximum is masked out
        let c = NumericColumn::with_nulls(vec![10_i64, 20, 99_999, 30], nulls).unwrap();
        assert_eq!(max_i64(&c), Some(30));
    }

    #[test]
    fn min_max_match_naive_scalar_reference() {
        // Property-style spot check against a scalar reference.
        let data: Vec<i64> = (0_i64..1024)
            .map(|i| i.wrapping_mul(2_862_933_555_777_941_757) ^ 0x1234_5678)
            .collect();
        let c = NumericColumn::from_data(data.clone());
        let want_min = *data.iter().min().unwrap();
        let want_max = *data.iter().max().unwrap();
        assert_eq!(min_i64(&c), Some(want_min));
        assert_eq!(max_i64(&c), Some(want_max));
    }

    #[test]
    fn cmp_gt_i64_basic() {
        let c = NumericColumn::from_data(vec![1_i64, -5, 10, 0, 100]);
        let m = cmp_gt_i64(&c, 0);
        assert!(m.get(0));
        assert!(!m.get(1));
        assert!(m.get(2));
        assert!(!m.get(3));
        assert!(m.get(4));
        assert_eq!(m.count_ones(), 3);
    }

    #[test]
    fn cmp_gt_i64_with_nulls_zeros_them() {
        let mut nulls = Bitmap::new(4, true);
        nulls.set(0, false); // mark row 0 NULL
        let c = NumericColumn::with_nulls(vec![999_i64, 5, 10, 20], nulls).unwrap();
        let m = cmp_gt_i64(&c, 0);
        assert!(!m.get(0), "null row must be 0 in mask");
        assert!(m.get(1));
        assert!(m.get(2));
        assert!(m.get(3));
    }

    #[test]
    fn sum_i64_with_mask_basic() {
        let c = NumericColumn::from_data(vec![10_i64, 20, 30, 40, 50]);
        let mut mask = Bitmap::new(5, false);
        mask.set(1, true);
        mask.set(3, true);
        assert_eq!(sum_i64_with_mask(&c, &mask), 60);
    }

    #[test]
    fn sum_i64_with_mask_all_set_matches_sum() {
        let data = (0..1000_i64).collect::<Vec<_>>();
        let c = NumericColumn::from_data(data.clone());
        let mask = Bitmap::new(1000, true);
        let want: i64 = data.iter().sum();
        assert_eq!(sum_i64_with_mask(&c, &mask), want);
    }

    #[test]
    fn sum_i64_with_mask_all_clear_is_zero() {
        let c = NumericColumn::from_data(vec![1_i64, 2, 3]);
        let mask = Bitmap::new(3, false);
        assert_eq!(sum_i64_with_mask(&c, &mask), 0);
    }

    #[test]
    #[should_panic(expected = "sum_i64_with_mask: length mismatch")]
    fn sum_i64_with_mask_length_mismatch_panics() {
        let c = NumericColumn::from_data(vec![1_i64, 2, 3]);
        let mask = Bitmap::new(4, false);
        let _ = sum_i64_with_mask(&c, &mask);
    }

    #[test]
    fn filter_sum_via_cmp_and_mask_matches_naive() {
        // Compose cmp_gt_i64 + sum_i64_with_mask and check it matches
        // the naive SQL-style filter+sum reference.
        let data: Vec<i64> = (0_i64..2048).map(|i| (i % 197).wrapping_sub(50)).collect();
        let c = NumericColumn::from_data(data.clone());
        let mask = cmp_gt_i64(&c, 0);
        let got = sum_i64_with_mask(&c, &mask);
        let want: i64 = data.iter().filter(|&&v| v > 0).copied().sum();
        assert_eq!(got, want);
    }

    #[test]
    fn range_mask_i64_inclusive_bounds() {
        let c = NumericColumn::from_data(vec![1_i64, 5, 10, 15, 20]);
        let m = range_mask_i64(&c, 5, 15);
        assert!(!m.get(0));
        assert!(m.get(1));
        assert!(m.get(2));
        assert!(m.get(3));
        assert!(!m.get(4));
        assert_eq!(m.count_ones(), 3);
    }

    #[test]
    fn range_mask_i64_with_nulls_zero_them() {
        let mut nulls = Bitmap::new(5, true);
        nulls.set(2, false); // mid-range row is null
        let c = NumericColumn::with_nulls(vec![1_i64, 5, 10, 15, 20], nulls).unwrap();
        let m = range_mask_i64(&c, 5, 15);
        assert!(m.get(1));
        assert!(!m.get(2), "null row must be 0 even if value would qualify");
        assert!(m.get(3));
    }

    #[test]
    fn range_mask_count_matches_naive() {
        // Check against a naive reference.
        let data: Vec<i64> = (0..4096).map(|i| (i * 17) % 100).collect();
        let c = NumericColumn::from_data(data.clone());
        let m = range_mask_i64(&c, 30, 70);
        let got = m.count_ones();
        let want = data.iter().filter(|&&v| (30..=70).contains(&v)).count();
        assert_eq!(got, want);
    }
}
