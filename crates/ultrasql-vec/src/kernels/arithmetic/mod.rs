//! Arithmetic and unary kernels for numeric and boolean columns.
//!
//! Each kernel has a scalar reference implementation (`_scalar` suffix) and a
//! production implementation whose 64-lane loop LLVM autovectorizes to NEON
//! `add` / `sub` / `mul` on `aarch64` and `SSE2`/`AVX2` equivalents on `x86_64`.
//!
//! # Coverage
//!
//! - Binary arithmetic for `i32`, `i64`, `f32`, `f64`:
//!   `add_*`, `sub_*`, `mul_*`, `compare_*`. Implementations live in
//!   [`binary_int`] (`i32`/`i64`) and [`binary_float`] (`f32`/`f64`).
//! - Column-vs-literal variants for the same shapes:
//!   `add_*_scalar_lit`, `sub_*_scalar_lit`, `mul_*_scalar_lit`,
//!   `compare_*_scalar_lit`. Implementations live in [`scalar_lit_int`]
//!   and [`scalar_lit_float`].
//! - Unary negation and boolean NOT live in [`unary`].
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

pub mod binary_float;
pub mod binary_int;
pub mod scalar_lit_float;
pub mod scalar_lit_int;
pub mod unary;

#[cfg(test)]
mod proptests;

pub use binary_float::{
    add_f32, add_f32_scalar, add_f64, add_f64_scalar, compare_f32, compare_f32_scalar, compare_f64,
    compare_f64_scalar, mul_f32, mul_f32_scalar, mul_f64, mul_f64_scalar, sub_f32, sub_f32_scalar,
    sub_f64, sub_f64_scalar,
};
pub use binary_int::{
    add_i32, add_i32_scalar, add_i64, add_i64_scalar, compare_i32, compare_i32_scalar, compare_i64,
    compare_i64_scalar, mul_i32, mul_i32_scalar, mul_i64, mul_i64_scalar, sub_i32, sub_i32_scalar,
    sub_i64, sub_i64_scalar,
};
pub use scalar_lit_float::{
    add_f32_scalar_lit, add_f32_scalar_lit_scalar, add_f64_scalar_lit, add_f64_scalar_lit_scalar,
    compare_f32_scalar_lit, compare_f32_scalar_lit_scalar, compare_f64_scalar_lit,
    compare_f64_scalar_lit_scalar, mul_f32_scalar_lit, mul_f32_scalar_lit_scalar,
    mul_f64_scalar_lit, mul_f64_scalar_lit_scalar, sub_f32_scalar_lit, sub_f32_scalar_lit_scalar,
    sub_f64_scalar_lit, sub_f64_scalar_lit_scalar,
};
pub use scalar_lit_int::{
    add_i32_scalar_lit, add_i32_scalar_lit_scalar, add_i64_scalar_lit, add_i64_scalar_lit_scalar,
    compare_i32_scalar_lit, compare_i32_scalar_lit_scalar, compare_i64_scalar_lit,
    compare_i64_scalar_lit_scalar, mul_i32_scalar_lit, mul_i32_scalar_lit_scalar,
    mul_i64_scalar_lit, mul_i64_scalar_lit_scalar, sub_i32_scalar_lit, sub_i32_scalar_lit_scalar,
    sub_i64_scalar_lit, sub_i64_scalar_lit_scalar,
};
pub use unary::{
    neg_f32, neg_f32_scalar, neg_f64, neg_f64_scalar, neg_i32, neg_i32_scalar, neg_i64,
    neg_i64_scalar, not_bool, not_bool_scalar,
};

use crate::bitmap::Bitmap;
use crate::column::{ColumnError, NumericColumn};

/// Project a [`std::cmp::Ordering`] onto `i64`-encoded {-1, 0, 1}.
#[inline]
pub(super) const fn cmp_to_i64(ord: std::cmp::Ordering) -> i64 {
    match ord {
        std::cmp::Ordering::Less => -1,
        std::cmp::Ordering::Equal => 0,
        std::cmp::Ordering::Greater => 1,
    }
}

/// Project a [`std::cmp::Ordering`] onto `i32`-encoded {-1, 0, 1}.
#[inline]
pub(super) const fn cmp_to_i32(ord: std::cmp::Ordering) -> i32 {
    match ord {
        std::cmp::Ordering::Less => -1,
        std::cmp::Ordering::Equal => 0,
        std::cmp::Ordering::Greater => 1,
    }
}

/// Apply a validity mask to the output data and wrap it in a [`NumericColumn`].
///
/// Where `validity[i]` is 0 (NULL), the output value is forced to `default`
/// (the type's zero) so garbage values never escape. The output column
/// carries the same validity bitmap. If `validity` is `None` the column is
/// returned non-nullable. A mismatched validity bitmap fails closed: all rows
/// become NULL and payload slots are set to `default`.
pub(super) fn apply_null_numeric<T: Copy>(
    mut data: Vec<T>,
    validity: Option<&Bitmap>,
    n: usize,
    default: T,
) -> NumericColumn<T> {
    if let Some(bm) = normalize_validity(validity, data.len(), n) {
        for (i, slot) in data.iter_mut().enumerate() {
            if !bm.get(i) {
                *slot = default;
            }
        }
        match NumericColumn::with_nulls(data, bm) {
            Ok(c) => c,
            Err(ColumnError::LengthMismatch { column, .. }) => all_null_numeric(column, default),
            Err(_) => NumericColumn::from_data(Vec::new()),
        }
    } else {
        NumericColumn::from_data(data)
    }
}

fn normalize_validity(
    validity: Option<&Bitmap>,
    column_len: usize,
    expected_len: usize,
) -> Option<Bitmap> {
    validity.map(|bm| {
        if bm.len() == column_len && expected_len == column_len {
            bm.clone()
        } else {
            Bitmap::new(column_len, false)
        }
    })
}

fn all_null_numeric<T: Copy>(len: usize, default: T) -> NumericColumn<T> {
    let data = vec![default; len];
    match NumericColumn::with_nulls(data, Bitmap::new(len, false)) {
        Ok(c) => c,
        Err(_) => NumericColumn::from_data(Vec::new()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_null_numeric_mismatched_validity_fails_closed() {
        let validity = Bitmap::new(1, true);

        let out = apply_null_numeric(vec![10_i64, 20, 30], Some(&validity), 3, 0_i64);

        assert_eq!(out.data(), &[0, 0, 0]);
        let nulls = out
            .nulls()
            .expect("mismatched validity should stay nullable");
        assert_eq!(nulls.len(), 3);
        assert_eq!(nulls.count_ones(), 0);
    }
}
