//! Unary numeric negation kernels and boolean NOT.
//!
//! Integer kernels use `wrapping_neg` so the negation of `iN::MIN` returns
//! `iN::MIN` (the standard two's-complement behaviour). Float kernels flip
//! the sign bit (including for `±0.0` and `±NaN`) per IEEE-754. The boolean
//! NOT preserves SQL three-valued logic through the validity bitmap —
//! `NOT NULL` stays `NULL`.

use super::apply_null_numeric;
use crate::bitmap::Bitmap;
use crate::column::{BoolColumn, NumericColumn};

/// Element-wise unary negation `-x` for an `i32` column. Wrapping: the
/// negation of `i32::MIN` returns `i32::MIN` (`wrapping_neg`) — the same
/// result Rust's `i32::wrapping_neg` documents for two's complement.
///
/// NULL rows produce `0` and are flagged in the output validity bitmap.
#[must_use]
pub fn neg_i32(column: &NumericColumn<i32>, validity: Option<&Bitmap>) -> NumericColumn<i32> {
    let out: Vec<i32> = column.data().iter().map(|&x| x.wrapping_neg()).collect();
    apply_null_numeric(out, validity, column.len(), 0_i32)
}

/// Scalar reference implementation for [`neg_i32`].
#[must_use]
pub fn neg_i32_scalar(
    column: &NumericColumn<i32>,
    validity: Option<&Bitmap>,
) -> NumericColumn<i32> {
    let out: Vec<i32> = column.data().iter().map(|&x| x.wrapping_neg()).collect();
    apply_null_numeric(out, validity, column.len(), 0_i32)
}

/// Element-wise unary negation `-x` for an `i64` column. Wrapping.
#[must_use]
pub fn neg_i64(column: &NumericColumn<i64>, validity: Option<&Bitmap>) -> NumericColumn<i64> {
    let out: Vec<i64> = column.data().iter().map(|&x| x.wrapping_neg()).collect();
    apply_null_numeric(out, validity, column.len(), 0_i64)
}

/// Scalar reference implementation for [`neg_i64`].
#[must_use]
pub fn neg_i64_scalar(
    column: &NumericColumn<i64>,
    validity: Option<&Bitmap>,
) -> NumericColumn<i64> {
    let out: Vec<i64> = column.data().iter().map(|&x| x.wrapping_neg()).collect();
    apply_null_numeric(out, validity, column.len(), 0_i64)
}

/// Element-wise unary negation `-x` for an `f32` column. IEEE-754: flips
/// the sign bit including across `±0.0` and `±NaN`.
#[must_use]
pub fn neg_f32(column: &NumericColumn<f32>, validity: Option<&Bitmap>) -> NumericColumn<f32> {
    let out: Vec<f32> = column.data().iter().map(|&x| -x).collect();
    apply_null_numeric(out, validity, column.len(), 0.0_f32)
}

/// Scalar reference implementation for [`neg_f32`].
#[must_use]
pub fn neg_f32_scalar(
    column: &NumericColumn<f32>,
    validity: Option<&Bitmap>,
) -> NumericColumn<f32> {
    let out: Vec<f32> = column.data().iter().map(|&x| -x).collect();
    apply_null_numeric(out, validity, column.len(), 0.0_f32)
}

/// Element-wise unary negation `-x` for an `f64` column. IEEE-754.
#[must_use]
pub fn neg_f64(column: &NumericColumn<f64>, validity: Option<&Bitmap>) -> NumericColumn<f64> {
    let out: Vec<f64> = column.data().iter().map(|&x| -x).collect();
    apply_null_numeric(out, validity, column.len(), 0.0_f64)
}

/// Scalar reference implementation for [`neg_f64`].
#[must_use]
pub fn neg_f64_scalar(
    column: &NumericColumn<f64>,
    validity: Option<&Bitmap>,
) -> NumericColumn<f64> {
    let out: Vec<f64> = column.data().iter().map(|&x| -x).collect();
    apply_null_numeric(out, validity, column.len(), 0.0_f64)
}

/// Element-wise boolean NOT for a `BoolColumn`. `BoolColumn` is stored as
/// `u8` (1 = true, 0 = false); the kernel maps each byte through `^ 1`.
///
/// NULL rows produce `false` and are flagged in the output validity bitmap.
/// SQL three-valued logic (`NOT NULL` → `NULL`) is preserved through the
/// validity bitmap: the data bit at a null row is forced to `0` but the
/// validity bit stays `0`, so callers reading the column see `NULL`.
///
/// # Panics
///
/// Panics if `validity.is_some()` and the validity length disagrees with
/// `column.len()` (via the `NumericColumn::with_nulls` invariant inside
/// the helper).
#[must_use]
pub fn not_bool(column: &BoolColumn, validity: Option<&Bitmap>) -> BoolColumn {
    let n = column.len();
    let mut out: Vec<bool> = column.data().iter().map(|&b| b == 0).collect();
    if let Some(bm) = validity {
        for (i, slot) in out.iter_mut().enumerate().take(n) {
            if !bm.get(i) {
                *slot = false;
            }
        }
        BoolColumn::with_nulls(out, bm.clone())
            .expect("validity length matches column length by invariant")
    } else {
        BoolColumn::from_data(out)
    }
}

/// Scalar reference implementation for [`not_bool`].
#[must_use]
pub fn not_bool_scalar(column: &BoolColumn, validity: Option<&Bitmap>) -> BoolColumn {
    let n = column.len();
    let mut out: Vec<bool> = Vec::with_capacity(n);
    for &b in column.data() {
        out.push(b == 0);
    }
    if let Some(bm) = validity {
        for (i, slot) in out.iter_mut().enumerate().take(n) {
            if !bm.get(i) {
                *slot = false;
            }
        }
        BoolColumn::with_nulls(out, bm.clone())
            .expect("validity length matches column length by invariant")
    } else {
        BoolColumn::from_data(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bitmap::Bitmap;
    use crate::column::{BoolColumn, NumericColumn};

    #[test]
    fn neg_i32_wraps_at_min() {
        let c = NumericColumn::from_data(vec![1_i32, -2, i32::MIN]);
        let out = neg_i32(&c, None);
        assert_eq!(out.data(), &[-1, 2, i32::MIN]);
    }

    #[test]
    fn neg_i64_wraps_at_min() {
        let c = NumericColumn::from_data(vec![1_i64, -2, i64::MIN]);
        let out = neg_i64(&c, None);
        assert_eq!(out.data(), &[-1, 2, i64::MIN]);
    }

    #[test]
    fn neg_f32_flips_sign() {
        let c = NumericColumn::from_data(vec![1.0_f32, -2.5, 0.0]);
        let out = neg_f32(&c, None);
        assert_eq!(out.data(), &[-1.0_f32, 2.5, -0.0]);
    }

    #[test]
    fn neg_f64_flips_sign() {
        let c = NumericColumn::from_data(vec![1.0_f64, -2.5, 0.0]);
        let out = neg_f64(&c, None);
        assert_eq!(out.data(), &[-1.0_f64, 2.5, -0.0]);
    }

    #[test]
    fn neg_propagates_null() {
        let c = NumericColumn::from_data(vec![1_i32, 2, 3]);
        let mut bm = Bitmap::new(3, true);
        bm.set(1, false);
        let out = neg_i32(&c, Some(&bm));
        assert_eq!(out.data()[0], -1);
        assert_eq!(out.data()[1], 0);
        assert_eq!(out.data()[2], -3);
    }

    #[test]
    fn not_bool_basic() {
        let c = BoolColumn::from_data(vec![true, false, true]);
        let out = not_bool(&c, None);
        assert_eq!(out.data(), &[0_u8, 1, 0]);
    }

    #[test]
    fn not_bool_with_null() {
        let c = BoolColumn::from_data(vec![true, false, true]);
        let mut bm = Bitmap::new(3, true);
        bm.set(0, false);
        let out = not_bool(&c, Some(&bm));
        assert_eq!(out.data()[0], 0);
        assert_eq!(out.data()[1], 1);
        assert_eq!(out.data()[2], 0);
        let nulls = out.nulls().expect("nullable output");
        assert!(!nulls.get(0));
        assert!(nulls.get(1));
        assert!(nulls.get(2));
    }

    #[test]
    fn not_bool_matches_scalar() {
        let c = BoolColumn::from_data(vec![true, false, true, false, true, true, false]);
        let got = not_bool(&c, None);
        let want = not_bool_scalar(&c, None);
        assert_eq!(got.data(), want.data());
    }
}
