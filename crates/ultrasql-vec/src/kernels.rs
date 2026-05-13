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
}
