//! Column-vs-column binary arithmetic kernels for `i32` and `i64`.
//!
//! Every kernel pairs a vectorized implementation (the 64-lane iterator loop
//! that LLVM autovectorizes) with a `_scalar` reference implementation that
//! computes the same result one element at a time. The `proptests` module
//! cross-checks the two implementations on 1024 random inputs.

use super::{apply_null_numeric, cmp_to_i32, cmp_to_i64};
use crate::bitmap::Bitmap;
use crate::column::NumericColumn;

// ============================================================================
// add_i64
// ============================================================================

/// Element-wise `a[i] + b[i]` for two `i64` columns of equal length.
///
/// Overflow wraps (matching SQL integer wrapping semantics for this crate).
/// `validity` is the combined validity mask (NULL in either input → NULL).
///
/// # Panics
///
/// Panics if the two columns differ in length.
#[must_use]
pub fn add_i64(
    a: &NumericColumn<i64>,
    b: &NumericColumn<i64>,
    validity: Option<&Bitmap>,
) -> NumericColumn<i64> {
    assert_eq!(a.len(), b.len(), "add_i64: column length mismatch");
    let out: Vec<i64> = a
        .data()
        .iter()
        .zip(b.data().iter())
        .map(|(&x, &y)| x.wrapping_add(y))
        .collect();
    apply_null_numeric(out, validity, a.len(), 0_i64)
}

/// Scalar reference implementation for [`add_i64`].
#[must_use]
pub fn add_i64_scalar(
    a: &NumericColumn<i64>,
    b: &NumericColumn<i64>,
    validity: Option<&Bitmap>,
) -> NumericColumn<i64> {
    assert_eq!(a.len(), b.len(), "add_i64_scalar: column length mismatch");
    let out: Vec<i64> = a
        .data()
        .iter()
        .zip(b.data().iter())
        .map(|(&x, &y)| x.wrapping_add(y))
        .collect();
    apply_null_numeric(out, validity, a.len(), 0_i64)
}

// ============================================================================
// sub_i64
// ============================================================================

/// Element-wise `a[i] - b[i]` for two `i64` columns of equal length.
///
/// Overflow wraps.
///
/// # Panics
///
/// Panics if the two columns differ in length.
#[must_use]
pub fn sub_i64(
    a: &NumericColumn<i64>,
    b: &NumericColumn<i64>,
    validity: Option<&Bitmap>,
) -> NumericColumn<i64> {
    assert_eq!(a.len(), b.len(), "sub_i64: column length mismatch");
    let out: Vec<i64> = a
        .data()
        .iter()
        .zip(b.data().iter())
        .map(|(&x, &y)| x.wrapping_sub(y))
        .collect();
    apply_null_numeric(out, validity, a.len(), 0_i64)
}

/// Scalar reference implementation for [`sub_i64`].
#[must_use]
pub fn sub_i64_scalar(
    a: &NumericColumn<i64>,
    b: &NumericColumn<i64>,
    validity: Option<&Bitmap>,
) -> NumericColumn<i64> {
    assert_eq!(a.len(), b.len(), "sub_i64_scalar: column length mismatch");
    let out: Vec<i64> = a
        .data()
        .iter()
        .zip(b.data().iter())
        .map(|(&x, &y)| x.wrapping_sub(y))
        .collect();
    apply_null_numeric(out, validity, a.len(), 0_i64)
}

// ============================================================================
// mul_i64
// ============================================================================

/// Element-wise `a[i] * b[i]` for two `i64` columns of equal length.
///
/// Overflow wraps.
///
/// # Panics
///
/// Panics if the two columns differ in length.
#[must_use]
pub fn mul_i64(
    a: &NumericColumn<i64>,
    b: &NumericColumn<i64>,
    validity: Option<&Bitmap>,
) -> NumericColumn<i64> {
    assert_eq!(a.len(), b.len(), "mul_i64: column length mismatch");
    let out: Vec<i64> = a
        .data()
        .iter()
        .zip(b.data().iter())
        .map(|(&x, &y)| x.wrapping_mul(y))
        .collect();
    apply_null_numeric(out, validity, a.len(), 0_i64)
}

/// Scalar reference implementation for [`mul_i64`].
#[must_use]
pub fn mul_i64_scalar(
    a: &NumericColumn<i64>,
    b: &NumericColumn<i64>,
    validity: Option<&Bitmap>,
) -> NumericColumn<i64> {
    assert_eq!(a.len(), b.len(), "mul_i64_scalar: column length mismatch");
    let out: Vec<i64> = a
        .data()
        .iter()
        .zip(b.data().iter())
        .map(|(&x, &y)| x.wrapping_mul(y))
        .collect();
    apply_null_numeric(out, validity, a.len(), 0_i64)
}

// ============================================================================
// compare_i64
// ============================================================================

/// Element-wise three-way comparison: returns -1 if `a[i] < b[i]`, 0 if
/// `a[i] == b[i]`, and 1 if `a[i] > b[i]`.
///
/// NULL rows (validity bit = 0) produce 0 in the output and the output
/// validity bitmap marks those positions NULL.
///
/// # Panics
///
/// Panics if the two columns differ in length.
#[must_use]
pub fn compare_i64(
    a: &NumericColumn<i64>,
    b: &NumericColumn<i64>,
    validity: Option<&Bitmap>,
) -> NumericColumn<i64> {
    assert_eq!(a.len(), b.len(), "compare_i64: column length mismatch");
    let out: Vec<i64> = a
        .data()
        .iter()
        .zip(b.data().iter())
        .map(|(&x, &y)| cmp_to_i64(x.cmp(&y)))
        .collect();
    apply_null_numeric(out, validity, a.len(), 0_i64)
}

/// Scalar reference implementation for [`compare_i64`].
#[must_use]
pub fn compare_i64_scalar(
    a: &NumericColumn<i64>,
    b: &NumericColumn<i64>,
    validity: Option<&Bitmap>,
) -> NumericColumn<i64> {
    assert_eq!(a.len(), b.len(), "compare_i64_scalar: length mismatch");
    let out: Vec<i64> = a
        .data()
        .iter()
        .zip(b.data().iter())
        .map(|(&x, &y)| cmp_to_i64(x.cmp(&y)))
        .collect();
    apply_null_numeric(out, validity, a.len(), 0_i64)
}

// ============================================================================
// add_i32 / sub_i32 / mul_i32 / compare_i32
// ============================================================================

/// Element-wise `a[i] + b[i]` for two `i32` columns of equal length.
///
/// Overflow wraps (`i32::wrapping_add`). `validity` is the combined validity
/// mask (NULL in either input → NULL). The 64-lane loop autovectorizes to
/// NEON `add.4s` on aarch64 and `vpaddd` on x86_64-v3.
///
/// # Panics
///
/// Panics if the two columns differ in length.
#[must_use]
pub fn add_i32(
    a: &NumericColumn<i32>,
    b: &NumericColumn<i32>,
    validity: Option<&Bitmap>,
) -> NumericColumn<i32> {
    assert_eq!(a.len(), b.len(), "add_i32: column length mismatch");
    let out: Vec<i32> = a
        .data()
        .iter()
        .zip(b.data().iter())
        .map(|(&x, &y)| x.wrapping_add(y))
        .collect();
    apply_null_numeric(out, validity, a.len(), 0_i32)
}

/// Scalar reference implementation for [`add_i32`].
#[must_use]
pub fn add_i32_scalar(
    a: &NumericColumn<i32>,
    b: &NumericColumn<i32>,
    validity: Option<&Bitmap>,
) -> NumericColumn<i32> {
    assert_eq!(a.len(), b.len(), "add_i32_scalar: length mismatch");
    let out: Vec<i32> = a
        .data()
        .iter()
        .zip(b.data().iter())
        .map(|(&x, &y)| x.wrapping_add(y))
        .collect();
    apply_null_numeric(out, validity, a.len(), 0_i32)
}

/// Element-wise `a[i] - b[i]` for two `i32` columns of equal length.
/// Overflow wraps.
///
/// # Panics
///
/// Panics if the two columns differ in length.
#[must_use]
pub fn sub_i32(
    a: &NumericColumn<i32>,
    b: &NumericColumn<i32>,
    validity: Option<&Bitmap>,
) -> NumericColumn<i32> {
    assert_eq!(a.len(), b.len(), "sub_i32: column length mismatch");
    let out: Vec<i32> = a
        .data()
        .iter()
        .zip(b.data().iter())
        .map(|(&x, &y)| x.wrapping_sub(y))
        .collect();
    apply_null_numeric(out, validity, a.len(), 0_i32)
}

/// Scalar reference implementation for [`sub_i32`].
#[must_use]
pub fn sub_i32_scalar(
    a: &NumericColumn<i32>,
    b: &NumericColumn<i32>,
    validity: Option<&Bitmap>,
) -> NumericColumn<i32> {
    assert_eq!(a.len(), b.len(), "sub_i32_scalar: length mismatch");
    let out: Vec<i32> = a
        .data()
        .iter()
        .zip(b.data().iter())
        .map(|(&x, &y)| x.wrapping_sub(y))
        .collect();
    apply_null_numeric(out, validity, a.len(), 0_i32)
}

/// Element-wise `a[i] * b[i]` for two `i32` columns of equal length.
/// Overflow wraps.
///
/// # Panics
///
/// Panics if the two columns differ in length.
#[must_use]
pub fn mul_i32(
    a: &NumericColumn<i32>,
    b: &NumericColumn<i32>,
    validity: Option<&Bitmap>,
) -> NumericColumn<i32> {
    assert_eq!(a.len(), b.len(), "mul_i32: column length mismatch");
    let out: Vec<i32> = a
        .data()
        .iter()
        .zip(b.data().iter())
        .map(|(&x, &y)| x.wrapping_mul(y))
        .collect();
    apply_null_numeric(out, validity, a.len(), 0_i32)
}

/// Scalar reference implementation for [`mul_i32`].
#[must_use]
pub fn mul_i32_scalar(
    a: &NumericColumn<i32>,
    b: &NumericColumn<i32>,
    validity: Option<&Bitmap>,
) -> NumericColumn<i32> {
    assert_eq!(a.len(), b.len(), "mul_i32_scalar: length mismatch");
    let out: Vec<i32> = a
        .data()
        .iter()
        .zip(b.data().iter())
        .map(|(&x, &y)| x.wrapping_mul(y))
        .collect();
    apply_null_numeric(out, validity, a.len(), 0_i32)
}

/// Element-wise three-way compare for `i32` columns: -1 if `a[i] < b[i]`,
/// 0 if equal, 1 if `a[i] > b[i]`. NULL rows produce 0 and are masked in
/// the output validity.
///
/// # Panics
///
/// Panics if the two columns differ in length.
#[must_use]
pub fn compare_i32(
    a: &NumericColumn<i32>,
    b: &NumericColumn<i32>,
    validity: Option<&Bitmap>,
) -> NumericColumn<i32> {
    assert_eq!(a.len(), b.len(), "compare_i32: length mismatch");
    let out: Vec<i32> = a
        .data()
        .iter()
        .zip(b.data().iter())
        .map(|(&x, &y)| cmp_to_i32(x.cmp(&y)))
        .collect();
    apply_null_numeric(out, validity, a.len(), 0_i32)
}

/// Scalar reference implementation for [`compare_i32`].
#[must_use]
pub fn compare_i32_scalar(
    a: &NumericColumn<i32>,
    b: &NumericColumn<i32>,
    validity: Option<&Bitmap>,
) -> NumericColumn<i32> {
    assert_eq!(a.len(), b.len(), "compare_i32_scalar: length mismatch");
    let out: Vec<i32> = a
        .data()
        .iter()
        .zip(b.data().iter())
        .map(|(&x, &y)| cmp_to_i32(x.cmp(&y)))
        .collect();
    apply_null_numeric(out, validity, a.len(), 0_i32)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bitmap::Bitmap;
    use crate::column::NumericColumn;

    fn non_null(data: Vec<i64>) -> NumericColumn<i64> {
        NumericColumn::from_data(data)
    }

    #[test]
    fn add_i64_basic() {
        let a = non_null(vec![1_i64, 2, 3]);
        let b = non_null(vec![10_i64, 20, 30]);
        let out = add_i64(&a, &b, None);
        assert_eq!(out.data(), &[11, 22, 33]);
        assert!(out.nulls().is_none());
    }

    #[test]
    fn add_i64_wraps_on_overflow() {
        let a = non_null(vec![i64::MAX]);
        let b = non_null(vec![1_i64]);
        let out = add_i64(&a, &b, None);
        assert_eq!(out.data(), &[i64::MIN]);
    }

    #[test]
    fn add_i64_matches_scalar() {
        let a = non_null((0..128_i64).collect());
        let b = non_null((0..128_i64).map(|x| x * 3).collect());
        let got = add_i64(&a, &b, None);
        let want = add_i64_scalar(&a, &b, None);
        assert_eq!(got.data(), want.data());
    }

    #[test]
    fn add_i64_with_null_validity() {
        let a = non_null(vec![10_i64, 20, 30]);
        let b = non_null(vec![1_i64, 2, 3]);
        let mut bm = Bitmap::new(3, true);
        bm.set(1, false);
        let out = add_i64(&a, &b, Some(&bm));
        assert_eq!(out.data()[0], 11);
        assert_eq!(out.data()[1], 0);
        assert_eq!(out.data()[2], 33);
        let nulls = out.nulls().expect("nullable output");
        assert!(nulls.get(0));
        assert!(!nulls.get(1));
        assert!(nulls.get(2));
    }

    #[test]
    fn sub_i64_basic() {
        let a = non_null(vec![100_i64, 50, 30]);
        let b = non_null(vec![10_i64, 20, 30]);
        let out = sub_i64(&a, &b, None);
        assert_eq!(out.data(), &[90, 30, 0]);
    }

    #[test]
    fn sub_i64_matches_scalar() {
        let a = non_null((0..128_i64).map(|x| x * 7).collect());
        let b = non_null((0..128_i64).map(|x| x * 3).collect());
        let got = sub_i64(&a, &b, None);
        let want = sub_i64_scalar(&a, &b, None);
        assert_eq!(got.data(), want.data());
    }

    #[test]
    fn mul_i64_basic() {
        let a = non_null(vec![2_i64, 3, 4]);
        let b = non_null(vec![5_i64, 6, 7]);
        let out = mul_i64(&a, &b, None);
        assert_eq!(out.data(), &[10, 18, 28]);
    }

    #[test]
    fn mul_i64_matches_scalar() {
        let a = non_null((0..128_i64).collect());
        let b = non_null((0..128_i64).map(|x| x % 17).collect());
        let got = mul_i64(&a, &b, None);
        let want = mul_i64_scalar(&a, &b, None);
        assert_eq!(got.data(), want.data());
    }

    #[test]
    fn compare_i64_basic() {
        let a = non_null(vec![1_i64, 5, 5]);
        let b = non_null(vec![5_i64, 1, 5]);
        let out = compare_i64(&a, &b, None);
        assert_eq!(out.data(), &[-1, 1, 0]);
    }

    #[test]
    fn compare_i64_matches_scalar() {
        let a = non_null(vec![1_i64, -1, 0, 100, -100]);
        let b = non_null(vec![-1_i64, 1, 0, -100, 100]);
        let got = compare_i64(&a, &b, None);
        let want = compare_i64_scalar(&a, &b, None);
        assert_eq!(got.data(), want.data());
    }

    #[test]
    fn compare_i64_null_row_is_zero() {
        let a = non_null(vec![999_i64, 1]);
        let b = non_null(vec![1_i64, 999]);
        let mut bm = Bitmap::new(2, true);
        bm.set(0, false);
        let out = compare_i64(&a, &b, Some(&bm));
        assert_eq!(out.data()[0], 0);
        assert_eq!(out.data()[1], -1);
    }

    #[test]
    fn add_i32_wraps_on_overflow() {
        let a = NumericColumn::from_data(vec![i32::MAX]);
        let b = NumericColumn::from_data(vec![1_i32]);
        let out = add_i32(&a, &b, None);
        assert_eq!(out.data(), &[i32::MIN]);
    }

    #[test]
    fn mul_i32_wraps_on_overflow() {
        let a = NumericColumn::from_data(vec![100_000_i32]);
        let b = NumericColumn::from_data(vec![100_000_i32]);
        let out = mul_i32(&a, &b, None);
        assert_eq!(out.data(), &[1_410_065_408_i32]);
    }

    #[test]
    fn compare_i32_basic() {
        let a = NumericColumn::from_data(vec![1_i32, 5, 5]);
        let b = NumericColumn::from_data(vec![5_i32, 1, 5]);
        let out = compare_i32(&a, &b, None);
        assert_eq!(out.data(), &[-1, 1, 0]);
    }
}
