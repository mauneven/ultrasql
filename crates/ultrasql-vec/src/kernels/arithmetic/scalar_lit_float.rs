//! Column-vs-literal binary kernels for `f32` and `f64` columns.
//!
//! Arithmetic follows IEEE-754; the three-way compare uses
//! `f{32,64}::total_cmp` for a deterministic total order across `NaN`
//! payloads and signed zeros. See [`super::scalar_lit_int`] for the
//! integer counterpart and [`super`] for the broader policy.

use super::{apply_null_numeric, cmp_to_i32};
use crate::bitmap::Bitmap;
use crate::column::NumericColumn;

/// Column-vs-literal `column[i] + lit` over an `f32` column. IEEE-754.
#[must_use]
pub fn add_f32_scalar_lit(
    column: &NumericColumn<f32>,
    lit: f32,
    validity: Option<&Bitmap>,
) -> NumericColumn<f32> {
    let out: Vec<f32> = column.data().iter().map(|&x| x + lit).collect();
    apply_null_numeric(out, validity, column.len(), 0.0_f32)
}

/// Scalar reference for [`add_f32_scalar_lit`].
#[must_use]
pub fn add_f32_scalar_lit_scalar(
    column: &NumericColumn<f32>,
    lit: f32,
    validity: Option<&Bitmap>,
) -> NumericColumn<f32> {
    let out: Vec<f32> = column.data().iter().map(|&x| x + lit).collect();
    apply_null_numeric(out, validity, column.len(), 0.0_f32)
}

/// Column-vs-literal `column[i] - lit` over an `f32` column. IEEE-754.
#[must_use]
pub fn sub_f32_scalar_lit(
    column: &NumericColumn<f32>,
    lit: f32,
    validity: Option<&Bitmap>,
) -> NumericColumn<f32> {
    let out: Vec<f32> = column.data().iter().map(|&x| x - lit).collect();
    apply_null_numeric(out, validity, column.len(), 0.0_f32)
}

/// Scalar reference for [`sub_f32_scalar_lit`].
#[must_use]
pub fn sub_f32_scalar_lit_scalar(
    column: &NumericColumn<f32>,
    lit: f32,
    validity: Option<&Bitmap>,
) -> NumericColumn<f32> {
    let out: Vec<f32> = column.data().iter().map(|&x| x - lit).collect();
    apply_null_numeric(out, validity, column.len(), 0.0_f32)
}

/// Column-vs-literal `column[i] * lit` over an `f32` column. IEEE-754.
#[must_use]
pub fn mul_f32_scalar_lit(
    column: &NumericColumn<f32>,
    lit: f32,
    validity: Option<&Bitmap>,
) -> NumericColumn<f32> {
    let out: Vec<f32> = column.data().iter().map(|&x| x * lit).collect();
    apply_null_numeric(out, validity, column.len(), 0.0_f32)
}

/// Scalar reference for [`mul_f32_scalar_lit`].
#[must_use]
pub fn mul_f32_scalar_lit_scalar(
    column: &NumericColumn<f32>,
    lit: f32,
    validity: Option<&Bitmap>,
) -> NumericColumn<f32> {
    let out: Vec<f32> = column.data().iter().map(|&x| x * lit).collect();
    apply_null_numeric(out, validity, column.len(), 0.0_f32)
}

/// Column-vs-literal three-way compare `column[i] <=> lit` over an `f32`
/// column using [`f32::total_cmp`].
#[must_use]
pub fn compare_f32_scalar_lit(
    column: &NumericColumn<f32>,
    lit: f32,
    validity: Option<&Bitmap>,
) -> NumericColumn<i32> {
    let out: Vec<i32> = column
        .data()
        .iter()
        .map(|&x| cmp_to_i32(x.total_cmp(&lit)))
        .collect();
    apply_null_numeric(out, validity, column.len(), 0_i32)
}

/// Scalar reference for [`compare_f32_scalar_lit`].
#[must_use]
pub fn compare_f32_scalar_lit_scalar(
    column: &NumericColumn<f32>,
    lit: f32,
    validity: Option<&Bitmap>,
) -> NumericColumn<i32> {
    let out: Vec<i32> = column
        .data()
        .iter()
        .map(|&x| cmp_to_i32(x.total_cmp(&lit)))
        .collect();
    apply_null_numeric(out, validity, column.len(), 0_i32)
}

/// Column-vs-literal `column[i] + lit` over an `f64` column. IEEE-754.
#[must_use]
pub fn add_f64_scalar_lit(
    column: &NumericColumn<f64>,
    lit: f64,
    validity: Option<&Bitmap>,
) -> NumericColumn<f64> {
    let out: Vec<f64> = column.data().iter().map(|&x| x + lit).collect();
    apply_null_numeric(out, validity, column.len(), 0.0_f64)
}

/// Scalar reference for [`add_f64_scalar_lit`].
#[must_use]
pub fn add_f64_scalar_lit_scalar(
    column: &NumericColumn<f64>,
    lit: f64,
    validity: Option<&Bitmap>,
) -> NumericColumn<f64> {
    let out: Vec<f64> = column.data().iter().map(|&x| x + lit).collect();
    apply_null_numeric(out, validity, column.len(), 0.0_f64)
}

/// Column-vs-literal `column[i] - lit` over an `f64` column. IEEE-754.
#[must_use]
pub fn sub_f64_scalar_lit(
    column: &NumericColumn<f64>,
    lit: f64,
    validity: Option<&Bitmap>,
) -> NumericColumn<f64> {
    let out: Vec<f64> = column.data().iter().map(|&x| x - lit).collect();
    apply_null_numeric(out, validity, column.len(), 0.0_f64)
}

/// Scalar reference for [`sub_f64_scalar_lit`].
#[must_use]
pub fn sub_f64_scalar_lit_scalar(
    column: &NumericColumn<f64>,
    lit: f64,
    validity: Option<&Bitmap>,
) -> NumericColumn<f64> {
    let out: Vec<f64> = column.data().iter().map(|&x| x - lit).collect();
    apply_null_numeric(out, validity, column.len(), 0.0_f64)
}

/// Column-vs-literal `column[i] * lit` over an `f64` column. IEEE-754.
#[must_use]
pub fn mul_f64_scalar_lit(
    column: &NumericColumn<f64>,
    lit: f64,
    validity: Option<&Bitmap>,
) -> NumericColumn<f64> {
    let out: Vec<f64> = column.data().iter().map(|&x| x * lit).collect();
    apply_null_numeric(out, validity, column.len(), 0.0_f64)
}

/// Scalar reference for [`mul_f64_scalar_lit`].
#[must_use]
pub fn mul_f64_scalar_lit_scalar(
    column: &NumericColumn<f64>,
    lit: f64,
    validity: Option<&Bitmap>,
) -> NumericColumn<f64> {
    let out: Vec<f64> = column.data().iter().map(|&x| x * lit).collect();
    apply_null_numeric(out, validity, column.len(), 0.0_f64)
}

/// Column-vs-literal three-way compare `column[i] <=> lit` over an `f64`
/// column using [`f64::total_cmp`].
#[must_use]
pub fn compare_f64_scalar_lit(
    column: &NumericColumn<f64>,
    lit: f64,
    validity: Option<&Bitmap>,
) -> NumericColumn<i32> {
    let out: Vec<i32> = column
        .data()
        .iter()
        .map(|&x| cmp_to_i32(x.total_cmp(&lit)))
        .collect();
    apply_null_numeric(out, validity, column.len(), 0_i32)
}

/// Scalar reference for [`compare_f64_scalar_lit`].
#[must_use]
pub fn compare_f64_scalar_lit_scalar(
    column: &NumericColumn<f64>,
    lit: f64,
    validity: Option<&Bitmap>,
) -> NumericColumn<i32> {
    let out: Vec<i32> = column
        .data()
        .iter()
        .map(|&x| cmp_to_i32(x.total_cmp(&lit)))
        .collect();
    apply_null_numeric(out, validity, column.len(), 0_i32)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column::NumericColumn;

    #[test]
    fn add_f64_scalar_lit_basic() {
        let c = NumericColumn::from_data(vec![1.0_f64, 2.5, -0.5]);
        let out = add_f64_scalar_lit(&c, 0.5, None);
        assert!((out.data()[0] - 1.5).abs() < 1e-12);
        assert!((out.data()[1] - 3.0).abs() < 1e-12);
        assert!((out.data()[2]).abs() < 1e-12);
    }
}
