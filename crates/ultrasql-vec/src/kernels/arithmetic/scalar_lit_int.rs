//! Column-vs-literal binary kernels for `i32` and `i64` columns.
//!
//! Each `*_scalar_lit` kernel computes `column[i] <op> lit` over the whole
//! column and returns a new column of the same shape. NULL handling is the
//! same as the column/column form: rows whose validity bit is 0 produce the
//! type's default and the output validity bitmap marks those positions NULL.
//!
//! These are the fast path for the executor when one operand of a binary
//! expression is a constant literal — no second column needs to be
//! materialised, the literal is broadcast and the inner loop is a pure
//! auto-vectorisable map over the column.

use super::{apply_null_numeric, cmp_to_i32, cmp_to_i64};
use crate::bitmap::Bitmap;
use crate::column::NumericColumn;

/// Column-vs-literal `column[i] + lit` over an `i64` column. Wrapping.
#[must_use]
pub fn add_i64_scalar_lit(
    column: &NumericColumn<i64>,
    lit: i64,
    validity: Option<&Bitmap>,
) -> NumericColumn<i64> {
    let out: Vec<i64> = column.data().iter().map(|&x| x.wrapping_add(lit)).collect();
    apply_null_numeric(out, validity, column.len(), 0_i64)
}

/// Scalar reference for [`add_i64_scalar_lit`].
#[must_use]
pub fn add_i64_scalar_lit_scalar(
    column: &NumericColumn<i64>,
    lit: i64,
    validity: Option<&Bitmap>,
) -> NumericColumn<i64> {
    let out: Vec<i64> = column.data().iter().map(|&x| x.wrapping_add(lit)).collect();
    apply_null_numeric(out, validity, column.len(), 0_i64)
}

/// Column-vs-literal `column[i] - lit` over an `i64` column. Wrapping.
#[must_use]
pub fn sub_i64_scalar_lit(
    column: &NumericColumn<i64>,
    lit: i64,
    validity: Option<&Bitmap>,
) -> NumericColumn<i64> {
    let out: Vec<i64> = column.data().iter().map(|&x| x.wrapping_sub(lit)).collect();
    apply_null_numeric(out, validity, column.len(), 0_i64)
}

/// Scalar reference for [`sub_i64_scalar_lit`].
#[must_use]
pub fn sub_i64_scalar_lit_scalar(
    column: &NumericColumn<i64>,
    lit: i64,
    validity: Option<&Bitmap>,
) -> NumericColumn<i64> {
    let out: Vec<i64> = column.data().iter().map(|&x| x.wrapping_sub(lit)).collect();
    apply_null_numeric(out, validity, column.len(), 0_i64)
}

/// Column-vs-literal `column[i] * lit` over an `i64` column. Wrapping.
#[must_use]
pub fn mul_i64_scalar_lit(
    column: &NumericColumn<i64>,
    lit: i64,
    validity: Option<&Bitmap>,
) -> NumericColumn<i64> {
    let out: Vec<i64> = column.data().iter().map(|&x| x.wrapping_mul(lit)).collect();
    apply_null_numeric(out, validity, column.len(), 0_i64)
}

/// Scalar reference for [`mul_i64_scalar_lit`].
#[must_use]
pub fn mul_i64_scalar_lit_scalar(
    column: &NumericColumn<i64>,
    lit: i64,
    validity: Option<&Bitmap>,
) -> NumericColumn<i64> {
    let out: Vec<i64> = column.data().iter().map(|&x| x.wrapping_mul(lit)).collect();
    apply_null_numeric(out, validity, column.len(), 0_i64)
}

/// Column-vs-literal three-way compare `column[i] <=> lit` over an `i64`
/// column. Returns `i64` with values -1, 0, 1.
#[must_use]
pub fn compare_i64_scalar_lit(
    column: &NumericColumn<i64>,
    lit: i64,
    validity: Option<&Bitmap>,
) -> NumericColumn<i64> {
    let out: Vec<i64> = column
        .data()
        .iter()
        .map(|&x| cmp_to_i64(x.cmp(&lit)))
        .collect();
    apply_null_numeric(out, validity, column.len(), 0_i64)
}

/// Scalar reference for [`compare_i64_scalar_lit`].
#[must_use]
pub fn compare_i64_scalar_lit_scalar(
    column: &NumericColumn<i64>,
    lit: i64,
    validity: Option<&Bitmap>,
) -> NumericColumn<i64> {
    let out: Vec<i64> = column
        .data()
        .iter()
        .map(|&x| cmp_to_i64(x.cmp(&lit)))
        .collect();
    apply_null_numeric(out, validity, column.len(), 0_i64)
}

/// Column-vs-literal `column[i] + lit` over an `i32` column. Wrapping.
#[must_use]
pub fn add_i32_scalar_lit(
    column: &NumericColumn<i32>,
    lit: i32,
    validity: Option<&Bitmap>,
) -> NumericColumn<i32> {
    let out: Vec<i32> = column.data().iter().map(|&x| x.wrapping_add(lit)).collect();
    apply_null_numeric(out, validity, column.len(), 0_i32)
}

/// Scalar reference for [`add_i32_scalar_lit`].
#[must_use]
pub fn add_i32_scalar_lit_scalar(
    column: &NumericColumn<i32>,
    lit: i32,
    validity: Option<&Bitmap>,
) -> NumericColumn<i32> {
    let out: Vec<i32> = column.data().iter().map(|&x| x.wrapping_add(lit)).collect();
    apply_null_numeric(out, validity, column.len(), 0_i32)
}

/// Column-vs-literal `column[i] - lit` over an `i32` column. Wrapping.
#[must_use]
pub fn sub_i32_scalar_lit(
    column: &NumericColumn<i32>,
    lit: i32,
    validity: Option<&Bitmap>,
) -> NumericColumn<i32> {
    let out: Vec<i32> = column.data().iter().map(|&x| x.wrapping_sub(lit)).collect();
    apply_null_numeric(out, validity, column.len(), 0_i32)
}

/// Scalar reference for [`sub_i32_scalar_lit`].
#[must_use]
pub fn sub_i32_scalar_lit_scalar(
    column: &NumericColumn<i32>,
    lit: i32,
    validity: Option<&Bitmap>,
) -> NumericColumn<i32> {
    let out: Vec<i32> = column.data().iter().map(|&x| x.wrapping_sub(lit)).collect();
    apply_null_numeric(out, validity, column.len(), 0_i32)
}

/// Column-vs-literal `column[i] * lit` over an `i32` column. Wrapping.
#[must_use]
pub fn mul_i32_scalar_lit(
    column: &NumericColumn<i32>,
    lit: i32,
    validity: Option<&Bitmap>,
) -> NumericColumn<i32> {
    let out: Vec<i32> = column.data().iter().map(|&x| x.wrapping_mul(lit)).collect();
    apply_null_numeric(out, validity, column.len(), 0_i32)
}

/// Scalar reference for [`mul_i32_scalar_lit`].
#[must_use]
pub fn mul_i32_scalar_lit_scalar(
    column: &NumericColumn<i32>,
    lit: i32,
    validity: Option<&Bitmap>,
) -> NumericColumn<i32> {
    let out: Vec<i32> = column.data().iter().map(|&x| x.wrapping_mul(lit)).collect();
    apply_null_numeric(out, validity, column.len(), 0_i32)
}

/// Column-vs-literal three-way compare `column[i] <=> lit` over an `i32`
/// column. Returns `i32` with values -1, 0, 1.
#[must_use]
pub fn compare_i32_scalar_lit(
    column: &NumericColumn<i32>,
    lit: i32,
    validity: Option<&Bitmap>,
) -> NumericColumn<i32> {
    let out: Vec<i32> = column
        .data()
        .iter()
        .map(|&x| cmp_to_i32(x.cmp(&lit)))
        .collect();
    apply_null_numeric(out, validity, column.len(), 0_i32)
}

/// Scalar reference for [`compare_i32_scalar_lit`].
#[must_use]
pub fn compare_i32_scalar_lit_scalar(
    column: &NumericColumn<i32>,
    lit: i32,
    validity: Option<&Bitmap>,
) -> NumericColumn<i32> {
    let out: Vec<i32> = column
        .data()
        .iter()
        .map(|&x| cmp_to_i32(x.cmp(&lit)))
        .collect();
    apply_null_numeric(out, validity, column.len(), 0_i32)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column::NumericColumn;

    #[test]
    fn add_i32_scalar_lit_basic() {
        let a = NumericColumn::from_data(vec![1_i32, 2, 3]);
        let out = add_i32_scalar_lit(&a, 10, None);
        assert_eq!(out.data(), &[11, 12, 13]);
    }
}
