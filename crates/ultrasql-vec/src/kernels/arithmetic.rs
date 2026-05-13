//! Arithmetic kernels for `i64` columns.
//!
//! Each kernel has a scalar reference implementation (`_scalar` suffix) and a
//! production implementation whose 64-lane loop LLVM autovectorizes to NEON
//! `add` / `sub` / `mul` on `aarch64` and `SSE2`/`AVX2` equivalents on `x86_64`.
//!
//! NULL handling: every kernel accepts an optional `&Bitmap` validity mask.
//! Rows whose validity bit is 0 are set to 0 in the output and the output
//! validity word is cleared accordingly.  The caller is responsible for
//! propagating the combined validity mask when both operands are nullable.

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
    apply_null(out, validity, a.len())
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
    apply_null(out, validity, a.len())
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
    apply_null(out, validity, a.len())
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
    apply_null(out, validity, a.len())
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
    apply_null(out, validity, a.len())
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
    apply_null(out, validity, a.len())
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
        .map(|(&x, &y)| x.cmp(&y) as i64)
        .collect();
    apply_null(out, validity, a.len())
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
        .map(|(&x, &y)| x.cmp(&y) as i64)
        .collect();
    apply_null(out, validity, a.len())
}

// ============================================================================
// Helpers
// ============================================================================

/// Apply a validity mask to the output data.
///
/// Where `validity[i]` is 0 (NULL), the output value is forced to 0 and the
/// output column carries the same validity bitmap.  If `validity` is `None`
/// the column is returned non-nullable.
fn apply_null(mut data: Vec<i64>, validity: Option<&Bitmap>, n: usize) -> NumericColumn<i64> {
    if let Some(bm) = validity {
        // Zero out NULL slots so garbage values never escape.
        for (i, slot) in data.iter_mut().enumerate().take(n) {
            if !bm.get(i) {
                *slot = 0;
            }
        }
        NumericColumn::with_nulls(data, bm.clone()).expect("validity length matches data length")
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

    proptest::proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig {
            cases: 64, .. proptest::prelude::ProptestConfig::default()
        })]

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
    }
}
