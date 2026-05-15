//! Column-vs-column binary arithmetic kernels for `f32` and `f64`.
//!
//! Arithmetic follows IEEE-754; the three-way compare uses
//! `f{32,64}::total_cmp` for a deterministic total order across `NaN`
//! payloads and signed zeros. See [`super`] for the broader policy.

use super::{apply_null_numeric, cmp_to_i32};
use crate::bitmap::Bitmap;
use crate::column::NumericColumn;

// ============================================================================
// add_f32 / sub_f32 / mul_f32 / compare_f32
// ============================================================================

/// Element-wise `a[i] + b[i]` for two `f32` columns of equal length.
///
/// Pure IEEE-754 addition — `NaN` propagates, `+0.0 + -0.0 = +0.0`, infinities
/// behave per IEEE. `validity` is the combined NULL mask.
///
/// # Panics
///
/// Panics if the two columns differ in length.
#[must_use]
pub fn add_f32(
    a: &NumericColumn<f32>,
    b: &NumericColumn<f32>,
    validity: Option<&Bitmap>,
) -> NumericColumn<f32> {
    assert_eq!(a.len(), b.len(), "add_f32: column length mismatch");
    let out: Vec<f32> = a
        .data()
        .iter()
        .zip(b.data().iter())
        .map(|(&x, &y)| x + y)
        .collect();
    apply_null_numeric(out, validity, a.len(), 0.0_f32)
}

/// Scalar reference implementation for [`add_f32`].
#[must_use]
pub fn add_f32_scalar(
    a: &NumericColumn<f32>,
    b: &NumericColumn<f32>,
    validity: Option<&Bitmap>,
) -> NumericColumn<f32> {
    assert_eq!(a.len(), b.len(), "add_f32_scalar: length mismatch");
    let out: Vec<f32> = a
        .data()
        .iter()
        .zip(b.data().iter())
        .map(|(&x, &y)| x + y)
        .collect();
    apply_null_numeric(out, validity, a.len(), 0.0_f32)
}

/// Element-wise `a[i] - b[i]` for two `f32` columns of equal length.
/// IEEE-754 subtraction.
///
/// # Panics
///
/// Panics if the two columns differ in length.
#[must_use]
pub fn sub_f32(
    a: &NumericColumn<f32>,
    b: &NumericColumn<f32>,
    validity: Option<&Bitmap>,
) -> NumericColumn<f32> {
    assert_eq!(a.len(), b.len(), "sub_f32: column length mismatch");
    let out: Vec<f32> = a
        .data()
        .iter()
        .zip(b.data().iter())
        .map(|(&x, &y)| x - y)
        .collect();
    apply_null_numeric(out, validity, a.len(), 0.0_f32)
}

/// Scalar reference implementation for [`sub_f32`].
#[must_use]
pub fn sub_f32_scalar(
    a: &NumericColumn<f32>,
    b: &NumericColumn<f32>,
    validity: Option<&Bitmap>,
) -> NumericColumn<f32> {
    assert_eq!(a.len(), b.len(), "sub_f32_scalar: length mismatch");
    let out: Vec<f32> = a
        .data()
        .iter()
        .zip(b.data().iter())
        .map(|(&x, &y)| x - y)
        .collect();
    apply_null_numeric(out, validity, a.len(), 0.0_f32)
}

/// Element-wise `a[i] * b[i]` for two `f32` columns of equal length.
/// IEEE-754 multiplication.
///
/// # Panics
///
/// Panics if the two columns differ in length.
#[must_use]
pub fn mul_f32(
    a: &NumericColumn<f32>,
    b: &NumericColumn<f32>,
    validity: Option<&Bitmap>,
) -> NumericColumn<f32> {
    assert_eq!(a.len(), b.len(), "mul_f32: column length mismatch");
    let out: Vec<f32> = a
        .data()
        .iter()
        .zip(b.data().iter())
        .map(|(&x, &y)| x * y)
        .collect();
    apply_null_numeric(out, validity, a.len(), 0.0_f32)
}

/// Scalar reference implementation for [`mul_f32`].
#[must_use]
pub fn mul_f32_scalar(
    a: &NumericColumn<f32>,
    b: &NumericColumn<f32>,
    validity: Option<&Bitmap>,
) -> NumericColumn<f32> {
    assert_eq!(a.len(), b.len(), "mul_f32_scalar: length mismatch");
    let out: Vec<f32> = a
        .data()
        .iter()
        .zip(b.data().iter())
        .map(|(&x, &y)| x * y)
        .collect();
    apply_null_numeric(out, validity, a.len(), 0.0_f32)
}

/// Element-wise three-way compare for `f32` columns using
/// [`f32::total_cmp`]. The total order is deterministic in the presence of
/// `NaN`s and `±0.0` and matches `slice::sort_by` on floats.
///
/// Result is stored as `i32`: -1 if `a[i] < b[i]`, 0 if equal, 1 if greater.
///
/// # Panics
///
/// Panics if the two columns differ in length.
#[must_use]
pub fn compare_f32(
    a: &NumericColumn<f32>,
    b: &NumericColumn<f32>,
    validity: Option<&Bitmap>,
) -> NumericColumn<i32> {
    assert_eq!(a.len(), b.len(), "compare_f32: length mismatch");
    let out: Vec<i32> = a
        .data()
        .iter()
        .zip(b.data().iter())
        .map(|(&x, &y)| cmp_to_i32(x.total_cmp(&y)))
        .collect();
    apply_null_numeric(out, validity, a.len(), 0_i32)
}

/// Scalar reference implementation for [`compare_f32`].
#[must_use]
pub fn compare_f32_scalar(
    a: &NumericColumn<f32>,
    b: &NumericColumn<f32>,
    validity: Option<&Bitmap>,
) -> NumericColumn<i32> {
    assert_eq!(a.len(), b.len(), "compare_f32_scalar: length mismatch");
    let out: Vec<i32> = a
        .data()
        .iter()
        .zip(b.data().iter())
        .map(|(&x, &y)| cmp_to_i32(x.total_cmp(&y)))
        .collect();
    apply_null_numeric(out, validity, a.len(), 0_i32)
}

// ============================================================================
// add_f64 / sub_f64 / mul_f64 / compare_f64
// ============================================================================

/// Element-wise `a[i] + b[i]` for two `f64` columns of equal length.
/// IEEE-754 addition.
///
/// # Panics
///
/// Panics if the two columns differ in length.
#[must_use]
pub fn add_f64(
    a: &NumericColumn<f64>,
    b: &NumericColumn<f64>,
    validity: Option<&Bitmap>,
) -> NumericColumn<f64> {
    assert_eq!(a.len(), b.len(), "add_f64: column length mismatch");
    let out: Vec<f64> = a
        .data()
        .iter()
        .zip(b.data().iter())
        .map(|(&x, &y)| x + y)
        .collect();
    apply_null_numeric(out, validity, a.len(), 0.0_f64)
}

/// Scalar reference implementation for [`add_f64`].
#[must_use]
pub fn add_f64_scalar(
    a: &NumericColumn<f64>,
    b: &NumericColumn<f64>,
    validity: Option<&Bitmap>,
) -> NumericColumn<f64> {
    assert_eq!(a.len(), b.len(), "add_f64_scalar: length mismatch");
    let out: Vec<f64> = a
        .data()
        .iter()
        .zip(b.data().iter())
        .map(|(&x, &y)| x + y)
        .collect();
    apply_null_numeric(out, validity, a.len(), 0.0_f64)
}

/// Element-wise `a[i] - b[i]` for two `f64` columns of equal length.
/// IEEE-754 subtraction.
///
/// # Panics
///
/// Panics if the two columns differ in length.
#[must_use]
pub fn sub_f64(
    a: &NumericColumn<f64>,
    b: &NumericColumn<f64>,
    validity: Option<&Bitmap>,
) -> NumericColumn<f64> {
    assert_eq!(a.len(), b.len(), "sub_f64: column length mismatch");
    let out: Vec<f64> = a
        .data()
        .iter()
        .zip(b.data().iter())
        .map(|(&x, &y)| x - y)
        .collect();
    apply_null_numeric(out, validity, a.len(), 0.0_f64)
}

/// Scalar reference implementation for [`sub_f64`].
#[must_use]
pub fn sub_f64_scalar(
    a: &NumericColumn<f64>,
    b: &NumericColumn<f64>,
    validity: Option<&Bitmap>,
) -> NumericColumn<f64> {
    assert_eq!(a.len(), b.len(), "sub_f64_scalar: length mismatch");
    let out: Vec<f64> = a
        .data()
        .iter()
        .zip(b.data().iter())
        .map(|(&x, &y)| x - y)
        .collect();
    apply_null_numeric(out, validity, a.len(), 0.0_f64)
}

/// Element-wise `a[i] * b[i]` for two `f64` columns of equal length.
/// IEEE-754 multiplication.
///
/// # Panics
///
/// Panics if the two columns differ in length.
#[must_use]
pub fn mul_f64(
    a: &NumericColumn<f64>,
    b: &NumericColumn<f64>,
    validity: Option<&Bitmap>,
) -> NumericColumn<f64> {
    assert_eq!(a.len(), b.len(), "mul_f64: column length mismatch");
    let out: Vec<f64> = a
        .data()
        .iter()
        .zip(b.data().iter())
        .map(|(&x, &y)| x * y)
        .collect();
    apply_null_numeric(out, validity, a.len(), 0.0_f64)
}

/// Scalar reference implementation for [`mul_f64`].
#[must_use]
pub fn mul_f64_scalar(
    a: &NumericColumn<f64>,
    b: &NumericColumn<f64>,
    validity: Option<&Bitmap>,
) -> NumericColumn<f64> {
    assert_eq!(a.len(), b.len(), "mul_f64_scalar: length mismatch");
    let out: Vec<f64> = a
        .data()
        .iter()
        .zip(b.data().iter())
        .map(|(&x, &y)| x * y)
        .collect();
    apply_null_numeric(out, validity, a.len(), 0.0_f64)
}

/// Element-wise three-way compare for `f64` columns using
/// [`f64::total_cmp`]. The total order is deterministic across `NaN`
/// payloads and across `+0.0` / `-0.0`.
///
/// Result is stored as `i32`: -1 / 0 / 1.
///
/// # Panics
///
/// Panics if the two columns differ in length.
#[must_use]
pub fn compare_f64(
    a: &NumericColumn<f64>,
    b: &NumericColumn<f64>,
    validity: Option<&Bitmap>,
) -> NumericColumn<i32> {
    assert_eq!(a.len(), b.len(), "compare_f64: length mismatch");
    let out: Vec<i32> = a
        .data()
        .iter()
        .zip(b.data().iter())
        .map(|(&x, &y)| cmp_to_i32(x.total_cmp(&y)))
        .collect();
    apply_null_numeric(out, validity, a.len(), 0_i32)
}

/// Scalar reference implementation for [`compare_f64`].
#[must_use]
pub fn compare_f64_scalar(
    a: &NumericColumn<f64>,
    b: &NumericColumn<f64>,
    validity: Option<&Bitmap>,
) -> NumericColumn<i32> {
    assert_eq!(a.len(), b.len(), "compare_f64_scalar: length mismatch");
    let out: Vec<i32> = a
        .data()
        .iter()
        .zip(b.data().iter())
        .map(|(&x, &y)| cmp_to_i32(x.total_cmp(&y)))
        .collect();
    apply_null_numeric(out, validity, a.len(), 0_i32)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column::NumericColumn;

    #[test]
    fn add_f32_basic() {
        let a = NumericColumn::from_data(vec![1.0_f32, 2.0, 3.0]);
        let b = NumericColumn::from_data(vec![0.5_f32, 0.25, -1.0]);
        let out = add_f32(&a, &b, None);
        assert!((out.data()[0] - 1.5).abs() < 1e-6);
        assert!((out.data()[1] - 2.25).abs() < 1e-6);
        assert!((out.data()[2] - 2.0).abs() < 1e-6);
    }

    #[test]
    fn compare_f32_handles_nan_with_total_cmp() {
        let a = NumericColumn::from_data(vec![f32::NAN, 1.0_f32]);
        let b = NumericColumn::from_data(vec![1.0_f32, f32::NAN]);
        let out = compare_f32(&a, &b, None);
        assert_eq!(out.data(), &[1, -1]);
    }

    #[test]
    fn compare_f32_total_cmp_signed_zero() {
        let a = NumericColumn::from_data(vec![-0.0_f32, 0.0]);
        let b = NumericColumn::from_data(vec![0.0_f32, -0.0]);
        let out = compare_f32(&a, &b, None);
        assert_eq!(out.data(), &[-1, 1]);
    }

    #[test]
    fn mul_f64_basic() {
        let a = NumericColumn::from_data(vec![2.0_f64, 3.0]);
        let b = NumericColumn::from_data(vec![5.0_f64, 7.5]);
        let out = mul_f64(&a, &b, None);
        assert!((out.data()[0] - 10.0).abs() < 1e-12);
        assert!((out.data()[1] - 22.5).abs() < 1e-12);
    }

    #[test]
    fn compare_f64_handles_nan_deterministically() {
        let a = NumericColumn::from_data(vec![f64::NAN, f64::NAN, 1.0_f64]);
        let b = NumericColumn::from_data(vec![1.0_f64, f64::NAN, f64::NAN]);
        let out = compare_f64(&a, &b, None);
        assert_eq!(out.data(), &[1, 0, -1]);
    }
}
