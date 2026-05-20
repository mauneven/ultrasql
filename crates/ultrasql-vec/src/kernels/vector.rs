//! Dense `f32` vector similarity kernels.
//!
//! Scalar fallback implementations are the source of truth. Public
//! wrappers share the same contract now and are the insertion points for
//! target-specific SIMD specializations.

/// Dot product over two dense `f32` vectors.
///
/// This wrapper currently delegates to [`dot_f32_scalar`]. Target-specific
/// SIMD kernels will plug in here while keeping the scalar implementation as
/// the correctness oracle.
///
/// # Panics
///
/// Panics if the input slices have different lengths.
#[must_use]
#[inline]
pub fn dot_f32(left: &[f32], right: &[f32]) -> f32 {
    assert_eq!(left.len(), right.len(), "dot_f32: vector length mismatch");
    dot_f32_scalar_same_len(left, right)
}

/// Scalar reference implementation of [`dot_f32`].
///
/// It uses an `f32` accumulator so it matches the storage type and gives SIMD
/// implementations a bit-for-bit target.
///
/// # Panics
///
/// Panics if the input slices have different lengths.
#[must_use]
#[inline]
pub fn dot_f32_scalar(left: &[f32], right: &[f32]) -> f32 {
    assert_eq!(
        left.len(),
        right.len(),
        "dot_f32_scalar: vector length mismatch"
    );
    dot_f32_scalar_same_len(left, right)
}

#[inline]
fn dot_f32_scalar_same_len(left: &[f32], right: &[f32]) -> f32 {
    left.iter()
        .zip(right.iter())
        .fold(0.0_f32, |acc, (&left_value, &right_value)| {
            acc + (left_value * right_value)
        })
}

/// Euclidean distance over two dense `f32` vectors.
///
/// This computes `sqrt(sum((left[i] - right[i])^2))` with an `f32`
/// accumulator. The wrapper currently delegates to [`l2_distance_f32_scalar`].
///
/// # Panics
///
/// Panics if the input slices have different lengths.
#[must_use]
#[inline]
pub fn l2_distance_f32(left: &[f32], right: &[f32]) -> f32 {
    assert_eq!(
        left.len(),
        right.len(),
        "l2_distance_f32: vector length mismatch"
    );
    l2_distance_f32_scalar_same_len(left, right)
}

/// Scalar reference implementation of [`l2_distance_f32`].
///
/// # Panics
///
/// Panics if the input slices have different lengths.
#[must_use]
#[inline]
pub fn l2_distance_f32_scalar(left: &[f32], right: &[f32]) -> f32 {
    assert_eq!(
        left.len(),
        right.len(),
        "l2_distance_f32_scalar: vector length mismatch"
    );
    l2_distance_f32_scalar_same_len(left, right)
}

#[inline]
fn l2_distance_f32_scalar_same_len(left: &[f32], right: &[f32]) -> f32 {
    left.iter()
        .zip(right.iter())
        .fold(0.0_f32, |acc, (&left_value, &right_value)| {
            let delta = left_value - right_value;
            acc + (delta * delta)
        })
        .sqrt()
}

/// Cosine distance over two dense `f32` vectors.
///
/// Returns `Some(1 - cosine_similarity)`. Returns `None` when either vector
/// has zero norm, including the empty-vector case, because cosine distance is
/// undefined there. The wrapper currently delegates to
/// [`cosine_distance_f32_scalar`].
///
/// # Panics
///
/// Panics if the input slices have different lengths.
#[must_use]
#[inline]
pub fn cosine_distance_f32(left: &[f32], right: &[f32]) -> Option<f32> {
    assert_eq!(
        left.len(),
        right.len(),
        "cosine_distance_f32: vector length mismatch"
    );
    cosine_distance_f32_scalar_same_len(left, right)
}

/// Scalar reference implementation of [`cosine_distance_f32`].
///
/// # Panics
///
/// Panics if the input slices have different lengths.
#[must_use]
#[inline]
pub fn cosine_distance_f32_scalar(left: &[f32], right: &[f32]) -> Option<f32> {
    assert_eq!(
        left.len(),
        right.len(),
        "cosine_distance_f32_scalar: vector length mismatch"
    );
    cosine_distance_f32_scalar_same_len(left, right)
}

#[inline]
fn cosine_distance_f32_scalar_same_len(left: &[f32], right: &[f32]) -> Option<f32> {
    let mut dot = 0.0_f32;
    let mut left_norm = 0.0_f32;
    let mut right_norm = 0.0_f32;

    for (&left_value, &right_value) in left.iter().zip(right.iter()) {
        dot += left_value * right_value;
        left_norm += left_value * left_value;
        right_norm += right_value * right_value;
    }

    if left_norm == 0.0 || right_norm == 0.0 {
        return None;
    }

    Some(1.0 - (dot / (left_norm.sqrt() * right_norm.sqrt())))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_close(got: f32, want: f32) {
        assert!((got - want).abs() <= 1.0e-6, "got {got}, want {want}");
    }

    #[test]
    fn dot_f32_computes_inner_product() {
        assert_eq!(dot_f32(&[1.0, 2.0, 3.0], &[4.0, 5.0, 6.0]), 32.0);
    }

    #[test]
    fn l2_distance_f32_computes_euclidean_distance() {
        assert_eq!(l2_distance_f32(&[1.0, 2.0, 3.0], &[1.0, 2.0, 4.0]), 1.0);
        assert_close(
            l2_distance_f32(&[1.0, -2.0, 3.0], &[-1.0, 2.0, 3.0]),
            20.0_f32.sqrt(),
        );
    }

    #[test]
    fn cosine_distance_f32_computes_one_minus_cosine_similarity() {
        assert_eq!(cosine_distance_f32(&[1.0, 0.0], &[0.0, 1.0]), Some(1.0));
        assert_eq!(
            cosine_distance_f32(&[1.0, 2.0, 2.0], &[1.0, 2.0, 2.0]),
            Some(0.0)
        );
    }

    #[test]
    fn cosine_distance_f32_rejects_zero_norm_vectors() {
        assert_eq!(cosine_distance_f32(&[0.0, 0.0], &[1.0, 0.0]), None);
        assert_eq!(cosine_distance_f32(&[], &[]), None);
    }

    #[test]
    #[should_panic(expected = "dot_f32: vector length mismatch")]
    fn dot_f32_panics_on_dimension_mismatch() {
        let _ = dot_f32(&[1.0], &[1.0, 2.0]);
    }

    #[test]
    #[should_panic(expected = "l2_distance_f32: vector length mismatch")]
    fn l2_distance_f32_panics_on_dimension_mismatch() {
        let _ = l2_distance_f32(&[1.0], &[1.0, 2.0]);
    }

    #[test]
    #[should_panic(expected = "cosine_distance_f32: vector length mismatch")]
    fn cosine_distance_f32_panics_on_dimension_mismatch() {
        let _ = cosine_distance_f32(&[1.0], &[1.0, 2.0]);
    }

    proptest::proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig {
            cases: 256, .. proptest::prelude::ProptestConfig::default()
        })]

        #[test]
        fn vector_kernels_match_scalar_fallback(
            pairs in proptest::collection::vec(
                (-1000.0_f32..1000.0_f32, -1000.0_f32..1000.0_f32),
                0_usize..=256,
            )
        ) {
            let left = pairs.iter().map(|&(left, _)| left).collect::<Vec<_>>();
            let right = pairs.iter().map(|&(_, right)| right).collect::<Vec<_>>();

            proptest::prop_assert_eq!(
                dot_f32(&left, &right).to_bits(),
                dot_f32_scalar(&left, &right).to_bits(),
            );
            proptest::prop_assert_eq!(
                l2_distance_f32(&left, &right).to_bits(),
                l2_distance_f32_scalar(&left, &right).to_bits(),
            );

            match (
                cosine_distance_f32(&left, &right),
                cosine_distance_f32_scalar(&left, &right),
            ) {
                (Some(got), Some(want)) => {
                    proptest::prop_assert_eq!(got.to_bits(), want.to_bits());
                }
                (None, None) => {}
                (got, want) => proptest::prop_assert!(false, "got {got:?}, want {want:?}"),
            }
        }
    }
}
