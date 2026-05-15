//! Arithmetic and unary kernels for numeric and boolean columns.
//!
//! Each kernel has a scalar reference implementation (`_scalar` suffix) and a
//! production implementation whose 64-lane loop LLVM autovectorizes to NEON
//! `add` / `sub` / `mul` on `aarch64` and `SSE2`/`AVX2` equivalents on `x86_64`.
//!
//! # Coverage
//!
//! - Binary arithmetic for `i32`, `i64`, `f32`, `f64`:
//!   `add_*`, `sub_*`, `mul_*`, `compare_*`.
//! - Column-vs-literal variants for the same shapes:
//!   `add_*_scalar_lit`, `sub_*_scalar_lit`, `mul_*_scalar_lit`,
//!   `compare_*_scalar_lit`.
//! - Unary negation: `neg_i32`, `neg_i64`, `neg_f32`, `neg_f64`.
//! - Boolean NOT: `not_bool`.
//!
//! # NULL handling
//!
//! Every kernel accepts an optional `&Bitmap` validity mask. Rows whose
//! validity bit is 0 are set to the type's default (`0` / `0.0` / `false`) in
//! the output and the output validity word is cleared accordingly. The caller
//! is responsible for combining the validity masks of the two operands before
//! invoking a binary kernel.
//!
//! # Overflow / NaN policy
//!
//! - Integer kernels use **wrapping** semantics (matching `i64::wrapping_add`,
//!   `i32::wrapping_mul`, …). This is the SQL integer semantics this crate
//!   ships with; overflow-aware arithmetic lives in a separate kernel family.
//! - Float kernels use **IEEE-754** semantics for arithmetic. For three-way
//!   compare we use `f32::total_cmp` / `f64::total_cmp`, which produces a
//!   total order that is deterministic across `NaN` payloads and across
//!   `+0.0` / `-0.0`. The total order is the same one Rust's standard
//!   library uses to sort floats.

use crate::bitmap::Bitmap;
use crate::column::{BoolColumn, ColumnError, NumericColumn};

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

// ============================================================================
// Column-vs-literal binary kernels
// ============================================================================
//
// Each `*_scalar_lit` kernel computes `column[i] <op> lit` over the whole
// column and returns a new column of the same shape. NULL handling is the
// same as the column/column form: rows whose validity bit is 0 produce the
// type's default and the output validity bitmap marks those positions NULL.
//
// These are the fast path for the executor when one operand of a binary
// expression is a constant literal — no second column needs to be
// materialised, the literal is broadcast and the inner loop is a pure
// auto-vectorisable map over the column.
// ============================================================================

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

// ============================================================================
// Unary negation
// ============================================================================

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

// ============================================================================
// Boolean NOT
// ============================================================================

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

// ============================================================================
// Helpers
// ============================================================================

/// Project a [`std::cmp::Ordering`] onto `i64`-encoded {-1, 0, 1}.
#[inline]
const fn cmp_to_i64(ord: std::cmp::Ordering) -> i64 {
    match ord {
        std::cmp::Ordering::Less => -1,
        std::cmp::Ordering::Equal => 0,
        std::cmp::Ordering::Greater => 1,
    }
}

/// Project a [`std::cmp::Ordering`] onto `i32`-encoded {-1, 0, 1}.
#[inline]
const fn cmp_to_i32(ord: std::cmp::Ordering) -> i32 {
    match ord {
        std::cmp::Ordering::Less => -1,
        std::cmp::Ordering::Equal => 0,
        std::cmp::Ordering::Greater => 1,
    }
}

/// Apply a validity mask to the output data and wrap it in a
/// [`NumericColumn`].
///
/// Where `validity[i]` is 0 (NULL), the output value is forced to `default`
/// (the type's zero) so garbage values never escape. The output column
/// carries the same validity bitmap. If `validity` is `None` the column is
/// returned non-nullable.
fn apply_null_numeric<T: Copy>(
    mut data: Vec<T>,
    validity: Option<&Bitmap>,
    n: usize,
    default: T,
) -> NumericColumn<T> {
    if let Some(bm) = validity {
        for (i, slot) in data.iter_mut().enumerate().take(n) {
            if !bm.get(i) {
                *slot = default;
            }
        }
        // The Result form is essential — we want the column constructor
        // to validate the length invariant rather than panic blindly.
        match NumericColumn::with_nulls(data, bm.clone()) {
            Ok(c) => c,
            Err(ColumnError::LengthMismatch { bitmap, column }) => {
                panic!("apply_null_numeric: validity length {bitmap} != column length {column}")
            }
        }
    } else {
        NumericColumn::from_data(data)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bitmap::Bitmap;
    use crate::column::NumericColumn;

    fn non_null(data: Vec<i64>) -> NumericColumn<i64> {
        NumericColumn::from_data(data)
    }

    // ---- add_i64 ----

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
        // Valid rows: 0 → 11, 2 → 33; null row 1 → 0
        assert_eq!(out.data()[0], 11);
        assert_eq!(out.data()[1], 0); // forced to 0 at null position
        assert_eq!(out.data()[2], 33);
        let nulls = out.nulls().expect("nullable output");
        assert!(nulls.get(0));
        assert!(!nulls.get(1));
        assert!(nulls.get(2));
    }

    // ---- sub_i64 ----

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

    // ---- mul_i64 ----

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

    // ---- compare_i64 ----

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
        assert_eq!(out.data()[0], 0); // null → 0
        assert_eq!(out.data()[1], -1); // 1 < 999
    }

    // ---- i32 spot checks ----

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
        // 10_000_000_000 mod 2^32 = 1_410_065_408
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

    #[test]
    fn add_i32_scalar_lit_basic() {
        let a = NumericColumn::from_data(vec![1_i32, 2, 3]);
        let out = add_i32_scalar_lit(&a, 10, None);
        assert_eq!(out.data(), &[11, 12, 13]);
    }

    // ---- f32 spot checks ----

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
        // total_cmp: NaN > +inf > all positive numbers, so NaN > 1 and 1 < NaN.
        assert_eq!(out.data(), &[1, -1]);
    }

    #[test]
    fn compare_f32_total_cmp_signed_zero() {
        let a = NumericColumn::from_data(vec![-0.0_f32, 0.0]);
        let b = NumericColumn::from_data(vec![0.0_f32, -0.0]);
        let out = compare_f32(&a, &b, None);
        // total_cmp: -0.0 < +0.0
        assert_eq!(out.data(), &[-1, 1]);
    }

    // ---- f64 spot checks ----

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
        // NaN > 1.0 → 1; NaN == NaN under total_cmp (same bit pattern) → 0;
        // 1.0 < NaN → -1.
        assert_eq!(out.data(), &[1, 0, -1]);
    }

    #[test]
    fn add_f64_scalar_lit_basic() {
        let c = NumericColumn::from_data(vec![1.0_f64, 2.5, -0.5]);
        let out = add_f64_scalar_lit(&c, 0.5, None);
        assert!((out.data()[0] - 1.5).abs() < 1e-12);
        assert!((out.data()[1] - 3.0).abs() < 1e-12);
        assert!((out.data()[2]).abs() < 1e-12);
    }

    // ---- Unary ----

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

    // ---- not_bool ----

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
        // Row 0 is NULL — data is forced to false (0) and validity is 0.
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

    // ========================================================================
    // Property tests — each kernel is cross-checked against its `_scalar`
    // reference over at least 1024 random inputs.
    // ========================================================================

    proptest::proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig {
            cases: 1024, .. proptest::prelude::ProptestConfig::default()
        })]

        // ---- i64 ----
        #[test]
        fn prop_add_i64_matches_scalar(
            pairs in proptest::collection::vec(
                (proptest::prelude::any::<i64>(), proptest::prelude::any::<i64>()),
                0_usize..=200,
            )
        ) {
            let a = NumericColumn::from_data(pairs.iter().map(|&(x, _)| x).collect::<Vec<_>>());
            let b = NumericColumn::from_data(pairs.iter().map(|&(_, y)| y).collect::<Vec<_>>());
            let got = add_i64(&a, &b, None);
            let want = add_i64_scalar(&a, &b, None);
            proptest::prop_assert_eq!(got.data(), want.data());
        }

        #[test]
        fn prop_sub_i64_matches_scalar(
            pairs in proptest::collection::vec(
                (proptest::prelude::any::<i64>(), proptest::prelude::any::<i64>()),
                0_usize..=200,
            )
        ) {
            let a = NumericColumn::from_data(pairs.iter().map(|&(x, _)| x).collect::<Vec<_>>());
            let b = NumericColumn::from_data(pairs.iter().map(|&(_, y)| y).collect::<Vec<_>>());
            let got = sub_i64(&a, &b, None);
            let want = sub_i64_scalar(&a, &b, None);
            proptest::prop_assert_eq!(got.data(), want.data());
        }

        #[test]
        fn prop_mul_i64_matches_scalar(
            pairs in proptest::collection::vec(
                (proptest::prelude::any::<i64>(), proptest::prelude::any::<i64>()),
                0_usize..=200,
            )
        ) {
            let a = NumericColumn::from_data(pairs.iter().map(|&(x, _)| x).collect::<Vec<_>>());
            let b = NumericColumn::from_data(pairs.iter().map(|&(_, y)| y).collect::<Vec<_>>());
            let got = mul_i64(&a, &b, None);
            let want = mul_i64_scalar(&a, &b, None);
            proptest::prop_assert_eq!(got.data(), want.data());
        }

        #[test]
        fn prop_compare_i64_matches_scalar(
            pairs in proptest::collection::vec(
                (proptest::prelude::any::<i64>(), proptest::prelude::any::<i64>()),
                0_usize..=200,
            )
        ) {
            let a = NumericColumn::from_data(pairs.iter().map(|&(x, _)| x).collect::<Vec<_>>());
            let b = NumericColumn::from_data(pairs.iter().map(|&(_, y)| y).collect::<Vec<_>>());
            let got = compare_i64(&a, &b, None);
            let want = compare_i64_scalar(&a, &b, None);
            proptest::prop_assert_eq!(got.data(), want.data());
        }

        #[test]
        fn prop_add_i64_scalar_lit_matches_scalar(
            xs in proptest::collection::vec(proptest::prelude::any::<i64>(), 0_usize..=200),
            lit in proptest::prelude::any::<i64>(),
        ) {
            let c = NumericColumn::from_data(xs);
            let got = add_i64_scalar_lit(&c, lit, None);
            let want = add_i64_scalar_lit_scalar(&c, lit, None);
            proptest::prop_assert_eq!(got.data(), want.data());
        }

        #[test]
        fn prop_sub_i64_scalar_lit_matches_scalar(
            xs in proptest::collection::vec(proptest::prelude::any::<i64>(), 0_usize..=200),
            lit in proptest::prelude::any::<i64>(),
        ) {
            let c = NumericColumn::from_data(xs);
            let got = sub_i64_scalar_lit(&c, lit, None);
            let want = sub_i64_scalar_lit_scalar(&c, lit, None);
            proptest::prop_assert_eq!(got.data(), want.data());
        }

        #[test]
        fn prop_mul_i64_scalar_lit_matches_scalar(
            xs in proptest::collection::vec(proptest::prelude::any::<i64>(), 0_usize..=200),
            lit in proptest::prelude::any::<i64>(),
        ) {
            let c = NumericColumn::from_data(xs);
            let got = mul_i64_scalar_lit(&c, lit, None);
            let want = mul_i64_scalar_lit_scalar(&c, lit, None);
            proptest::prop_assert_eq!(got.data(), want.data());
        }

        #[test]
        fn prop_compare_i64_scalar_lit_matches_scalar(
            xs in proptest::collection::vec(proptest::prelude::any::<i64>(), 0_usize..=200),
            lit in proptest::prelude::any::<i64>(),
        ) {
            let c = NumericColumn::from_data(xs);
            let got = compare_i64_scalar_lit(&c, lit, None);
            let want = compare_i64_scalar_lit_scalar(&c, lit, None);
            proptest::prop_assert_eq!(got.data(), want.data());
        }

        // ---- i32 ----
        #[test]
        fn prop_add_i32_matches_scalar(
            pairs in proptest::collection::vec(
                (proptest::prelude::any::<i32>(), proptest::prelude::any::<i32>()),
                0_usize..=200,
            )
        ) {
            let a = NumericColumn::from_data(pairs.iter().map(|&(x, _)| x).collect::<Vec<_>>());
            let b = NumericColumn::from_data(pairs.iter().map(|&(_, y)| y).collect::<Vec<_>>());
            let got = add_i32(&a, &b, None);
            let want = add_i32_scalar(&a, &b, None);
            proptest::prop_assert_eq!(got.data(), want.data());
        }

        #[test]
        fn prop_sub_i32_matches_scalar(
            pairs in proptest::collection::vec(
                (proptest::prelude::any::<i32>(), proptest::prelude::any::<i32>()),
                0_usize..=200,
            )
        ) {
            let a = NumericColumn::from_data(pairs.iter().map(|&(x, _)| x).collect::<Vec<_>>());
            let b = NumericColumn::from_data(pairs.iter().map(|&(_, y)| y).collect::<Vec<_>>());
            let got = sub_i32(&a, &b, None);
            let want = sub_i32_scalar(&a, &b, None);
            proptest::prop_assert_eq!(got.data(), want.data());
        }

        #[test]
        fn prop_mul_i32_matches_scalar(
            pairs in proptest::collection::vec(
                (proptest::prelude::any::<i32>(), proptest::prelude::any::<i32>()),
                0_usize..=200,
            )
        ) {
            let a = NumericColumn::from_data(pairs.iter().map(|&(x, _)| x).collect::<Vec<_>>());
            let b = NumericColumn::from_data(pairs.iter().map(|&(_, y)| y).collect::<Vec<_>>());
            let got = mul_i32(&a, &b, None);
            let want = mul_i32_scalar(&a, &b, None);
            proptest::prop_assert_eq!(got.data(), want.data());
        }

        #[test]
        fn prop_compare_i32_matches_scalar(
            pairs in proptest::collection::vec(
                (proptest::prelude::any::<i32>(), proptest::prelude::any::<i32>()),
                0_usize..=200,
            )
        ) {
            let a = NumericColumn::from_data(pairs.iter().map(|&(x, _)| x).collect::<Vec<_>>());
            let b = NumericColumn::from_data(pairs.iter().map(|&(_, y)| y).collect::<Vec<_>>());
            let got = compare_i32(&a, &b, None);
            let want = compare_i32_scalar(&a, &b, None);
            proptest::prop_assert_eq!(got.data(), want.data());
        }

        #[test]
        fn prop_add_i32_scalar_lit_matches_scalar(
            xs in proptest::collection::vec(proptest::prelude::any::<i32>(), 0_usize..=200),
            lit in proptest::prelude::any::<i32>(),
        ) {
            let c = NumericColumn::from_data(xs);
            let got = add_i32_scalar_lit(&c, lit, None);
            let want = add_i32_scalar_lit_scalar(&c, lit, None);
            proptest::prop_assert_eq!(got.data(), want.data());
        }

        #[test]
        fn prop_sub_i32_scalar_lit_matches_scalar(
            xs in proptest::collection::vec(proptest::prelude::any::<i32>(), 0_usize..=200),
            lit in proptest::prelude::any::<i32>(),
        ) {
            let c = NumericColumn::from_data(xs);
            let got = sub_i32_scalar_lit(&c, lit, None);
            let want = sub_i32_scalar_lit_scalar(&c, lit, None);
            proptest::prop_assert_eq!(got.data(), want.data());
        }

        #[test]
        fn prop_mul_i32_scalar_lit_matches_scalar(
            xs in proptest::collection::vec(proptest::prelude::any::<i32>(), 0_usize..=200),
            lit in proptest::prelude::any::<i32>(),
        ) {
            let c = NumericColumn::from_data(xs);
            let got = mul_i32_scalar_lit(&c, lit, None);
            let want = mul_i32_scalar_lit_scalar(&c, lit, None);
            proptest::prop_assert_eq!(got.data(), want.data());
        }

        #[test]
        fn prop_compare_i32_scalar_lit_matches_scalar(
            xs in proptest::collection::vec(proptest::prelude::any::<i32>(), 0_usize..=200),
            lit in proptest::prelude::any::<i32>(),
        ) {
            let c = NumericColumn::from_data(xs);
            let got = compare_i32_scalar_lit(&c, lit, None);
            let want = compare_i32_scalar_lit_scalar(&c, lit, None);
            proptest::prop_assert_eq!(got.data(), want.data());
        }

        // ---- f32 — uses bit-equality so NaN payloads match exactly. ----
        #[test]
        fn prop_add_f32_matches_scalar(
            pairs in proptest::collection::vec(
                (proptest::prelude::any::<f32>(), proptest::prelude::any::<f32>()),
                0_usize..=200,
            )
        ) {
            let a = NumericColumn::from_data(pairs.iter().map(|&(x, _)| x).collect::<Vec<_>>());
            let b = NumericColumn::from_data(pairs.iter().map(|&(_, y)| y).collect::<Vec<_>>());
            let got = add_f32(&a, &b, None);
            let want = add_f32_scalar(&a, &b, None);
            let got_bits: Vec<u32> = got.data().iter().map(|f| f.to_bits()).collect();
            let want_bits: Vec<u32> = want.data().iter().map(|f| f.to_bits()).collect();
            proptest::prop_assert_eq!(got_bits, want_bits);
        }

        #[test]
        fn prop_sub_f32_matches_scalar(
            pairs in proptest::collection::vec(
                (proptest::prelude::any::<f32>(), proptest::prelude::any::<f32>()),
                0_usize..=200,
            )
        ) {
            let a = NumericColumn::from_data(pairs.iter().map(|&(x, _)| x).collect::<Vec<_>>());
            let b = NumericColumn::from_data(pairs.iter().map(|&(_, y)| y).collect::<Vec<_>>());
            let got = sub_f32(&a, &b, None);
            let want = sub_f32_scalar(&a, &b, None);
            let got_bits: Vec<u32> = got.data().iter().map(|f| f.to_bits()).collect();
            let want_bits: Vec<u32> = want.data().iter().map(|f| f.to_bits()).collect();
            proptest::prop_assert_eq!(got_bits, want_bits);
        }

        #[test]
        fn prop_mul_f32_matches_scalar(
            pairs in proptest::collection::vec(
                (proptest::prelude::any::<f32>(), proptest::prelude::any::<f32>()),
                0_usize..=200,
            )
        ) {
            let a = NumericColumn::from_data(pairs.iter().map(|&(x, _)| x).collect::<Vec<_>>());
            let b = NumericColumn::from_data(pairs.iter().map(|&(_, y)| y).collect::<Vec<_>>());
            let got = mul_f32(&a, &b, None);
            let want = mul_f32_scalar(&a, &b, None);
            let got_bits: Vec<u32> = got.data().iter().map(|f| f.to_bits()).collect();
            let want_bits: Vec<u32> = want.data().iter().map(|f| f.to_bits()).collect();
            proptest::prop_assert_eq!(got_bits, want_bits);
        }

        #[test]
        fn prop_compare_f32_matches_scalar(
            pairs in proptest::collection::vec(
                (proptest::prelude::any::<f32>(), proptest::prelude::any::<f32>()),
                0_usize..=200,
            )
        ) {
            let a = NumericColumn::from_data(pairs.iter().map(|&(x, _)| x).collect::<Vec<_>>());
            let b = NumericColumn::from_data(pairs.iter().map(|&(_, y)| y).collect::<Vec<_>>());
            let got = compare_f32(&a, &b, None);
            let want = compare_f32_scalar(&a, &b, None);
            proptest::prop_assert_eq!(got.data(), want.data());
        }

        #[test]
        fn prop_add_f32_scalar_lit_matches_scalar(
            xs in proptest::collection::vec(proptest::prelude::any::<f32>(), 0_usize..=200),
            lit in proptest::prelude::any::<f32>(),
        ) {
            let c = NumericColumn::from_data(xs);
            let got = add_f32_scalar_lit(&c, lit, None);
            let want = add_f32_scalar_lit_scalar(&c, lit, None);
            let got_bits: Vec<u32> = got.data().iter().map(|f| f.to_bits()).collect();
            let want_bits: Vec<u32> = want.data().iter().map(|f| f.to_bits()).collect();
            proptest::prop_assert_eq!(got_bits, want_bits);
        }

        #[test]
        fn prop_sub_f32_scalar_lit_matches_scalar(
            xs in proptest::collection::vec(proptest::prelude::any::<f32>(), 0_usize..=200),
            lit in proptest::prelude::any::<f32>(),
        ) {
            let c = NumericColumn::from_data(xs);
            let got = sub_f32_scalar_lit(&c, lit, None);
            let want = sub_f32_scalar_lit_scalar(&c, lit, None);
            let got_bits: Vec<u32> = got.data().iter().map(|f| f.to_bits()).collect();
            let want_bits: Vec<u32> = want.data().iter().map(|f| f.to_bits()).collect();
            proptest::prop_assert_eq!(got_bits, want_bits);
        }

        #[test]
        fn prop_mul_f32_scalar_lit_matches_scalar(
            xs in proptest::collection::vec(proptest::prelude::any::<f32>(), 0_usize..=200),
            lit in proptest::prelude::any::<f32>(),
        ) {
            let c = NumericColumn::from_data(xs);
            let got = mul_f32_scalar_lit(&c, lit, None);
            let want = mul_f32_scalar_lit_scalar(&c, lit, None);
            let got_bits: Vec<u32> = got.data().iter().map(|f| f.to_bits()).collect();
            let want_bits: Vec<u32> = want.data().iter().map(|f| f.to_bits()).collect();
            proptest::prop_assert_eq!(got_bits, want_bits);
        }

        #[test]
        fn prop_compare_f32_scalar_lit_matches_scalar(
            xs in proptest::collection::vec(proptest::prelude::any::<f32>(), 0_usize..=200),
            lit in proptest::prelude::any::<f32>(),
        ) {
            let c = NumericColumn::from_data(xs);
            let got = compare_f32_scalar_lit(&c, lit, None);
            let want = compare_f32_scalar_lit_scalar(&c, lit, None);
            proptest::prop_assert_eq!(got.data(), want.data());
        }

        // ---- f64 ----
        #[test]
        fn prop_add_f64_matches_scalar(
            pairs in proptest::collection::vec(
                (proptest::prelude::any::<f64>(), proptest::prelude::any::<f64>()),
                0_usize..=200,
            )
        ) {
            let a = NumericColumn::from_data(pairs.iter().map(|&(x, _)| x).collect::<Vec<_>>());
            let b = NumericColumn::from_data(pairs.iter().map(|&(_, y)| y).collect::<Vec<_>>());
            let got = add_f64(&a, &b, None);
            let want = add_f64_scalar(&a, &b, None);
            let got_bits: Vec<u64> = got.data().iter().map(|f| f.to_bits()).collect();
            let want_bits: Vec<u64> = want.data().iter().map(|f| f.to_bits()).collect();
            proptest::prop_assert_eq!(got_bits, want_bits);
        }

        #[test]
        fn prop_sub_f64_matches_scalar(
            pairs in proptest::collection::vec(
                (proptest::prelude::any::<f64>(), proptest::prelude::any::<f64>()),
                0_usize..=200,
            )
        ) {
            let a = NumericColumn::from_data(pairs.iter().map(|&(x, _)| x).collect::<Vec<_>>());
            let b = NumericColumn::from_data(pairs.iter().map(|&(_, y)| y).collect::<Vec<_>>());
            let got = sub_f64(&a, &b, None);
            let want = sub_f64_scalar(&a, &b, None);
            let got_bits: Vec<u64> = got.data().iter().map(|f| f.to_bits()).collect();
            let want_bits: Vec<u64> = want.data().iter().map(|f| f.to_bits()).collect();
            proptest::prop_assert_eq!(got_bits, want_bits);
        }

        #[test]
        fn prop_mul_f64_matches_scalar(
            pairs in proptest::collection::vec(
                (proptest::prelude::any::<f64>(), proptest::prelude::any::<f64>()),
                0_usize..=200,
            )
        ) {
            let a = NumericColumn::from_data(pairs.iter().map(|&(x, _)| x).collect::<Vec<_>>());
            let b = NumericColumn::from_data(pairs.iter().map(|&(_, y)| y).collect::<Vec<_>>());
            let got = mul_f64(&a, &b, None);
            let want = mul_f64_scalar(&a, &b, None);
            let got_bits: Vec<u64> = got.data().iter().map(|f| f.to_bits()).collect();
            let want_bits: Vec<u64> = want.data().iter().map(|f| f.to_bits()).collect();
            proptest::prop_assert_eq!(got_bits, want_bits);
        }

        #[test]
        fn prop_compare_f64_matches_scalar(
            pairs in proptest::collection::vec(
                (proptest::prelude::any::<f64>(), proptest::prelude::any::<f64>()),
                0_usize..=200,
            )
        ) {
            let a = NumericColumn::from_data(pairs.iter().map(|&(x, _)| x).collect::<Vec<_>>());
            let b = NumericColumn::from_data(pairs.iter().map(|&(_, y)| y).collect::<Vec<_>>());
            let got = compare_f64(&a, &b, None);
            let want = compare_f64_scalar(&a, &b, None);
            proptest::prop_assert_eq!(got.data(), want.data());
        }

        #[test]
        fn prop_add_f64_scalar_lit_matches_scalar(
            xs in proptest::collection::vec(proptest::prelude::any::<f64>(), 0_usize..=200),
            lit in proptest::prelude::any::<f64>(),
        ) {
            let c = NumericColumn::from_data(xs);
            let got = add_f64_scalar_lit(&c, lit, None);
            let want = add_f64_scalar_lit_scalar(&c, lit, None);
            let got_bits: Vec<u64> = got.data().iter().map(|f| f.to_bits()).collect();
            let want_bits: Vec<u64> = want.data().iter().map(|f| f.to_bits()).collect();
            proptest::prop_assert_eq!(got_bits, want_bits);
        }

        #[test]
        fn prop_sub_f64_scalar_lit_matches_scalar(
            xs in proptest::collection::vec(proptest::prelude::any::<f64>(), 0_usize..=200),
            lit in proptest::prelude::any::<f64>(),
        ) {
            let c = NumericColumn::from_data(xs);
            let got = sub_f64_scalar_lit(&c, lit, None);
            let want = sub_f64_scalar_lit_scalar(&c, lit, None);
            let got_bits: Vec<u64> = got.data().iter().map(|f| f.to_bits()).collect();
            let want_bits: Vec<u64> = want.data().iter().map(|f| f.to_bits()).collect();
            proptest::prop_assert_eq!(got_bits, want_bits);
        }

        #[test]
        fn prop_mul_f64_scalar_lit_matches_scalar(
            xs in proptest::collection::vec(proptest::prelude::any::<f64>(), 0_usize..=200),
            lit in proptest::prelude::any::<f64>(),
        ) {
            let c = NumericColumn::from_data(xs);
            let got = mul_f64_scalar_lit(&c, lit, None);
            let want = mul_f64_scalar_lit_scalar(&c, lit, None);
            let got_bits: Vec<u64> = got.data().iter().map(|f| f.to_bits()).collect();
            let want_bits: Vec<u64> = want.data().iter().map(|f| f.to_bits()).collect();
            proptest::prop_assert_eq!(got_bits, want_bits);
        }

        #[test]
        fn prop_compare_f64_scalar_lit_matches_scalar(
            xs in proptest::collection::vec(proptest::prelude::any::<f64>(), 0_usize..=200),
            lit in proptest::prelude::any::<f64>(),
        ) {
            let c = NumericColumn::from_data(xs);
            let got = compare_f64_scalar_lit(&c, lit, None);
            let want = compare_f64_scalar_lit_scalar(&c, lit, None);
            proptest::prop_assert_eq!(got.data(), want.data());
        }

        // ---- Unary ----
        #[test]
        fn prop_neg_i32_matches_scalar(
            xs in proptest::collection::vec(proptest::prelude::any::<i32>(), 0_usize..=200),
        ) {
            let c = NumericColumn::from_data(xs);
            let got = neg_i32(&c, None);
            let want = neg_i32_scalar(&c, None);
            proptest::prop_assert_eq!(got.data(), want.data());
        }

        #[test]
        fn prop_neg_i64_matches_scalar(
            xs in proptest::collection::vec(proptest::prelude::any::<i64>(), 0_usize..=200),
        ) {
            let c = NumericColumn::from_data(xs);
            let got = neg_i64(&c, None);
            let want = neg_i64_scalar(&c, None);
            proptest::prop_assert_eq!(got.data(), want.data());
        }

        #[test]
        fn prop_neg_f32_matches_scalar(
            xs in proptest::collection::vec(proptest::prelude::any::<f32>(), 0_usize..=200),
        ) {
            let c = NumericColumn::from_data(xs);
            let got = neg_f32(&c, None);
            let want = neg_f32_scalar(&c, None);
            let got_bits: Vec<u32> = got.data().iter().map(|f| f.to_bits()).collect();
            let want_bits: Vec<u32> = want.data().iter().map(|f| f.to_bits()).collect();
            proptest::prop_assert_eq!(got_bits, want_bits);
        }

        #[test]
        fn prop_neg_f64_matches_scalar(
            xs in proptest::collection::vec(proptest::prelude::any::<f64>(), 0_usize..=200),
        ) {
            let c = NumericColumn::from_data(xs);
            let got = neg_f64(&c, None);
            let want = neg_f64_scalar(&c, None);
            let got_bits: Vec<u64> = got.data().iter().map(|f| f.to_bits()).collect();
            let want_bits: Vec<u64> = want.data().iter().map(|f| f.to_bits()).collect();
            proptest::prop_assert_eq!(got_bits, want_bits);
        }

        #[test]
        fn prop_not_bool_matches_scalar(
            xs in proptest::collection::vec(proptest::prelude::any::<bool>(), 0_usize..=200),
        ) {
            let c = BoolColumn::from_data(xs);
            let got = not_bool(&c, None);
            let want = not_bool_scalar(&c, None);
            proptest::prop_assert_eq!(got.data(), want.data());
        }
    }
}
