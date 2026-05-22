//! Dense `f32` vector similarity kernels.
//!
//! Scalar fallback implementations are the source of truth. Public wrappers
//! use scalar safe Rust by default. Enabling the `simd-unsafe` feature lets
//! wrappers dispatch to NEON, AVX2, or AVX-512 when available while keeping
//! exact top-k scans on the same metric contract.

use std::cmp::Ordering;

/// Dense vector metric used by exact top-k scans.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VectorMetric {
    /// Euclidean distance, matching pgvector `<->`.
    L2,
    /// Cosine distance, matching pgvector `<=>`.
    Cosine,
    /// Negative inner product, matching pgvector `<#>`.
    NegativeInnerProduct,
    /// Manhattan distance, matching pgvector `<+>`.
    L1,
}

/// Exact top-k vector scan hit.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct VectorTopKHit {
    /// Input row ordinal.
    pub row: usize,
    /// Sort distance for the selected metric.
    pub distance: f32,
}

/// Dot product over two dense `f32` vectors.
///
/// This wrapper dispatches to target-specific SIMD when available, while
/// [`dot_f32_scalar`] remains the correctness oracle.
///
/// # Panics
///
/// Panics if the input slices have different lengths.
#[must_use]
#[inline]
pub fn dot_f32(left: &[f32], right: &[f32]) -> f32 {
    assert_eq!(left.len(), right.len(), "dot_f32: vector length mismatch");
    #[cfg(all(feature = "simd-unsafe", target_arch = "aarch64"))]
    {
        dot_f32_neon_checked(left, right)
    }
    #[cfg(not(all(feature = "simd-unsafe", target_arch = "aarch64")))]
    {
        #[cfg(all(feature = "simd-unsafe", target_arch = "x86_64"))]
        {
            if let Some(result) = dot_f32_avx512_if_available(left, right) {
                return result;
            }
            if let Some(result) = dot_f32_avx2_if_available(left, right) {
                return result;
            }
        }
        dot_f32_scalar_same_len(left, right)
    }
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
/// accumulator. The wrapper dispatches to target-specific SIMD when available,
/// while [`l2_distance_f32_scalar`] remains the correctness oracle.
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
    #[cfg(all(feature = "simd-unsafe", target_arch = "aarch64"))]
    {
        l2_distance_f32_neon_checked(left, right)
    }
    #[cfg(not(all(feature = "simd-unsafe", target_arch = "aarch64")))]
    {
        #[cfg(all(feature = "simd-unsafe", target_arch = "x86_64"))]
        {
            if let Some(result) = l2_distance_f32_avx512_if_available(left, right) {
                return result;
            }
            if let Some(result) = l2_distance_f32_avx2_if_available(left, right) {
                return result;
            }
        }
        l2_distance_f32_scalar_same_len(left, right)
    }
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
/// undefined there. The wrapper dispatches to target-specific SIMD when
/// available, while [`cosine_distance_f32_scalar`] remains the correctness
/// oracle.
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
    #[cfg(all(feature = "simd-unsafe", target_arch = "aarch64"))]
    {
        cosine_distance_f32_neon_checked(left, right)
    }
    #[cfg(not(all(feature = "simd-unsafe", target_arch = "aarch64")))]
    {
        #[cfg(all(feature = "simd-unsafe", target_arch = "x86_64"))]
        {
            if let Some(result) = cosine_distance_f32_avx512_if_available(left, right) {
                return result;
            }
            if let Some(result) = cosine_distance_f32_avx2_if_available(left, right) {
                return result;
            }
        }
        cosine_distance_f32_scalar_same_len(left, right)
    }
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

/// Exact row-major top-k scan over dense `f32` vectors.
///
/// This is the baseline used before approximate indexes: every input row is
/// evaluated with the same exact metric kernels used by scalar SQL operators.
/// Ties are stable by input row ordinal.
///
/// # Panics
///
/// Panics if any vector length differs from `probe.len()`.
#[must_use]
pub fn exact_top_k_f32(
    vectors: &[&[f32]],
    probe: &[f32],
    metric: VectorMetric,
    k: usize,
) -> Vec<VectorTopKHit> {
    if k == 0 {
        return Vec::new();
    }
    let mut kept: Vec<VectorTopKHit> = Vec::with_capacity(k.min(vectors.len()));
    for (row, vector) in vectors.iter().enumerate() {
        assert_eq!(
            vector.len(),
            probe.len(),
            "exact_top_k_f32: vector length mismatch"
        );
        let hit = VectorTopKHit {
            row,
            distance: metric_distance_f32(vector, probe, metric),
        };
        keep_exact_top_k_hit(&mut kept, hit, k);
    }
    kept.sort_by(compare_top_k_hits);
    kept
}

/// Exact top-k scan over row-major dense `f32` vector batches.
///
/// `values` stores rows contiguously, `dims` values per row. This avoids
/// constructing per-row vectors on batch scan paths.
///
/// # Panics
///
/// Panics if `dims == 0`, `probe.len() != dims`, or `values.len()` is not a
/// multiple of `dims`.
#[must_use]
pub fn exact_top_k_f32_flat(
    values: &[f32],
    dims: usize,
    probe: &[f32],
    metric: VectorMetric,
    k: usize,
) -> Vec<VectorTopKHit> {
    assert!(dims > 0, "exact_top_k_f32_flat: dims must be non-zero");
    assert_eq!(
        probe.len(),
        dims,
        "exact_top_k_f32_flat: probe length mismatch"
    );
    assert_eq!(
        values.len() % dims,
        0,
        "exact_top_k_f32_flat: row-major values length mismatch"
    );
    if k == 0 {
        return Vec::new();
    }
    let row_count = values.len() / dims;
    let mut kept: Vec<VectorTopKHit> = Vec::with_capacity(k.min(row_count));
    for (row, vector) in values.chunks_exact(dims).enumerate() {
        let hit = VectorTopKHit {
            row,
            distance: metric_distance_f32(vector, probe, metric),
        };
        keep_exact_top_k_hit(&mut kept, hit, k);
    }
    kept.sort_by(compare_top_k_hits);
    kept
}

#[inline]
fn metric_distance_f32(left: &[f32], right: &[f32], metric: VectorMetric) -> f32 {
    match metric {
        VectorMetric::L2 => l2_distance_f32(left, right),
        VectorMetric::Cosine => cosine_distance_f32(left, right).unwrap_or(f32::INFINITY),
        VectorMetric::NegativeInnerProduct => -dot_f32(left, right),
        VectorMetric::L1 => l1_distance_f32_scalar_same_len(left, right),
    }
}

fn keep_exact_top_k_hit(kept: &mut Vec<VectorTopKHit>, hit: VectorTopKHit, k: usize) {
    if kept.len() < k {
        kept.push(hit);
        return;
    }
    let Some(worst_idx) = worst_top_k_hit_idx(kept) else {
        return;
    };
    if compare_top_k_hits(&hit, &kept[worst_idx]) == Ordering::Less {
        kept[worst_idx] = hit;
    }
}

fn worst_top_k_hit_idx(kept: &[VectorTopKHit]) -> Option<usize> {
    let mut worst = 0_usize;
    for idx in 1..kept.len() {
        if compare_top_k_hits(&kept[idx], &kept[worst]) == Ordering::Greater {
            worst = idx;
        }
    }
    Some(worst)
}

fn compare_top_k_hits(left: &VectorTopKHit, right: &VectorTopKHit) -> Ordering {
    left.distance
        .total_cmp(&right.distance)
        .then_with(|| left.row.cmp(&right.row))
}

#[inline]
fn l1_distance_f32_scalar_same_len(left: &[f32], right: &[f32]) -> f32 {
    debug_assert_eq!(left.len(), right.len());
    left.iter()
        .zip(right.iter())
        .fold(0.0_f32, |acc, (&left_value, &right_value)| {
            acc + (left_value - right_value).abs()
        })
}

#[cfg(all(feature = "simd-unsafe", target_arch = "aarch64"))]
#[inline]
fn dot_f32_neon_checked(left: &[f32], right: &[f32]) -> f32 {
    debug_assert_eq!(left.len(), right.len());
    // SAFETY:
    // - NEON is part of the aarch64 baseline.
    // - Inputs are borrowed slices; the target-feature helper only reads
    //   inside `chunks_exact` bounds and writes to stack lane arrays.
    // - Public wrapper checked equal lengths before dispatch.
    unsafe { dot_f32_neon(left, right) }
}

#[cfg(all(feature = "simd-unsafe", target_arch = "aarch64"))]
#[inline]
fn l2_distance_f32_neon_checked(left: &[f32], right: &[f32]) -> f32 {
    debug_assert_eq!(left.len(), right.len());
    // SAFETY:
    // - NEON is part of the aarch64 baseline.
    // - Inputs are borrowed slices; the target-feature helper only reads
    //   inside `chunks_exact` bounds and writes to stack lane arrays.
    // - Public wrapper checked equal lengths before dispatch.
    unsafe { l2_distance_f32_neon(left, right) }
}

#[cfg(all(feature = "simd-unsafe", target_arch = "aarch64"))]
#[inline]
fn cosine_distance_f32_neon_checked(left: &[f32], right: &[f32]) -> Option<f32> {
    debug_assert_eq!(left.len(), right.len());
    // SAFETY:
    // - NEON is part of the aarch64 baseline.
    // - Inputs are borrowed slices; the target-feature helper only reads
    //   inside `chunks_exact` bounds and writes to stack lane arrays.
    // - Public wrapper checked equal lengths before dispatch.
    unsafe { cosine_distance_f32_neon(left, right) }
}

#[cfg(all(feature = "simd-unsafe", target_arch = "aarch64"))]
#[target_feature(enable = "neon")]
#[inline]
fn load_f32x4(chunk: &[f32]) -> std::arch::aarch64::float32x4_t {
    debug_assert!(chunk.len() >= 4);
    // SAFETY:
    // - Callers pass slices produced by `chunks_exact(4)`, so at least four
    //   initialized contiguous `f32` lanes are available.
    // - `vld1q_f32` permits unaligned loads and does not outlive the slice.
    unsafe { std::arch::aarch64::vld1q_f32(chunk.as_ptr()) }
}

#[cfg(all(feature = "simd-unsafe", target_arch = "aarch64"))]
#[target_feature(enable = "neon")]
#[inline]
fn store_f32x4(vector: std::arch::aarch64::float32x4_t, lanes: &mut [f32; 4]) {
    // SAFETY:
    // - `lanes` points to exactly four initialized `f32` slots on the stack.
    // - `vst1q_f32` permits unaligned stores and writes exactly four lanes.
    unsafe { std::arch::aarch64::vst1q_f32(lanes.as_mut_ptr(), vector) };
}

#[cfg(all(feature = "simd-unsafe", target_arch = "x86_64"))]
#[target_feature(enable = "avx2")]
#[inline]
fn load_m256_f32(chunk: &[f32]) -> std::arch::x86_64::__m256 {
    debug_assert!(chunk.len() >= 8);
    // SAFETY:
    // - Callers pass slices produced by `chunks_exact(8)`, so at least eight
    //   initialized contiguous `f32` lanes are available.
    // - `_mm256_loadu_ps` permits unaligned loads and does not outlive slice.
    unsafe { std::arch::x86_64::_mm256_loadu_ps(chunk.as_ptr()) }
}

#[cfg(all(feature = "simd-unsafe", target_arch = "x86_64"))]
#[target_feature(enable = "avx2")]
#[inline]
fn store_m256_f32(vector: std::arch::x86_64::__m256, lanes: &mut [f32; 8]) {
    // SAFETY:
    // - `lanes` points to exactly eight initialized `f32` slots on stack.
    // - `_mm256_storeu_ps` permits unaligned stores and writes exactly eight
    //   lanes.
    unsafe { std::arch::x86_64::_mm256_storeu_ps(lanes.as_mut_ptr(), vector) };
}

#[cfg(all(feature = "simd-unsafe", target_arch = "x86_64"))]
#[allow(clippy::incompatible_msrv)] // AVX-512 intrinsics are runtime-gated and x86_64-only.
#[target_feature(enable = "avx512f")]
#[inline]
fn load_m512_f32(chunk: &[f32]) -> std::arch::x86_64::__m512 {
    debug_assert!(chunk.len() >= 16);
    // SAFETY:
    // - Callers pass slices produced by `chunks_exact(16)`, so at least sixteen
    //   initialized contiguous `f32` lanes are available.
    // - `_mm512_loadu_ps` permits unaligned loads and does not outlive slice.
    unsafe { std::arch::x86_64::_mm512_loadu_ps(chunk.as_ptr()) }
}

#[cfg(all(feature = "simd-unsafe", target_arch = "x86_64"))]
#[allow(clippy::incompatible_msrv)] // AVX-512 intrinsics are runtime-gated and x86_64-only.
#[target_feature(enable = "avx512f")]
#[inline]
fn store_m512_f32(vector: std::arch::x86_64::__m512, lanes: &mut [f32; 16]) {
    // SAFETY:
    // - `lanes` points to exactly sixteen initialized `f32` slots on stack.
    // - `_mm512_storeu_ps` permits unaligned stores and writes exactly sixteen
    //   lanes.
    unsafe { std::arch::x86_64::_mm512_storeu_ps(lanes.as_mut_ptr(), vector) };
}

#[cfg(all(feature = "simd-unsafe", target_arch = "x86_64"))]
#[inline]
fn dot_f32_avx2_if_available(left: &[f32], right: &[f32]) -> Option<f32> {
    if !std::arch::is_x86_feature_detected!("avx2") {
        return None;
    }
    debug_assert_eq!(left.len(), right.len());
    // SAFETY:
    // - Runtime CPUID confirmed AVX2.
    // - Inputs are borrowed slices; the target-feature helper only reads
    //   inside `chunks_exact` bounds and writes to stack lane arrays.
    // - Public wrapper checked equal lengths before dispatch.
    Some(unsafe { dot_f32_avx2(left, right) })
}

#[cfg(all(feature = "simd-unsafe", target_arch = "x86_64"))]
#[inline]
fn l2_distance_f32_avx2_if_available(left: &[f32], right: &[f32]) -> Option<f32> {
    if !std::arch::is_x86_feature_detected!("avx2") {
        return None;
    }
    debug_assert_eq!(left.len(), right.len());
    // SAFETY:
    // - Runtime CPUID confirmed AVX2.
    // - Inputs are borrowed slices; the target-feature helper only reads
    //   inside `chunks_exact` bounds and writes to stack lane arrays.
    // - Public wrapper checked equal lengths before dispatch.
    Some(unsafe { l2_distance_f32_avx2(left, right) })
}

#[cfg(all(feature = "simd-unsafe", target_arch = "x86_64"))]
#[inline]
fn cosine_distance_f32_avx2_if_available(left: &[f32], right: &[f32]) -> Option<Option<f32>> {
    if !std::arch::is_x86_feature_detected!("avx2") {
        return None;
    }
    debug_assert_eq!(left.len(), right.len());
    // SAFETY:
    // - Runtime CPUID confirmed AVX2.
    // - Inputs are borrowed slices; the target-feature helper only reads
    //   inside `chunks_exact` bounds and writes to stack lane arrays.
    // - Public wrapper checked equal lengths before dispatch.
    Some(unsafe { cosine_distance_f32_avx2(left, right) })
}

#[cfg(all(feature = "simd-unsafe", target_arch = "x86_64"))]
#[inline]
fn dot_f32_avx512_if_available(left: &[f32], right: &[f32]) -> Option<f32> {
    if !std::arch::is_x86_feature_detected!("avx512f") {
        return None;
    }
    debug_assert_eq!(left.len(), right.len());
    // SAFETY:
    // - Runtime CPUID confirmed AVX-512F.
    // - Inputs are borrowed slices; the target-feature helper only reads
    //   inside `chunks_exact` bounds and writes to stack lane arrays.
    // - Public wrapper checked equal lengths before dispatch.
    Some(unsafe { dot_f32_avx512(left, right) })
}

#[cfg(all(feature = "simd-unsafe", target_arch = "x86_64"))]
#[inline]
fn l2_distance_f32_avx512_if_available(left: &[f32], right: &[f32]) -> Option<f32> {
    if !std::arch::is_x86_feature_detected!("avx512f") {
        return None;
    }
    debug_assert_eq!(left.len(), right.len());
    // SAFETY:
    // - Runtime CPUID confirmed AVX-512F.
    // - Inputs are borrowed slices; the target-feature helper only reads
    //   inside `chunks_exact` bounds and writes to stack lane arrays.
    // - Public wrapper checked equal lengths before dispatch.
    Some(unsafe { l2_distance_f32_avx512(left, right) })
}

#[cfg(all(feature = "simd-unsafe", target_arch = "x86_64"))]
#[inline]
fn cosine_distance_f32_avx512_if_available(left: &[f32], right: &[f32]) -> Option<Option<f32>> {
    if !std::arch::is_x86_feature_detected!("avx512f") {
        return None;
    }
    debug_assert_eq!(left.len(), right.len());
    // SAFETY:
    // - Runtime CPUID confirmed AVX-512F.
    // - Inputs are borrowed slices; the target-feature helper only reads
    //   inside `chunks_exact` bounds and writes to stack lane arrays.
    // - Public wrapper checked equal lengths before dispatch.
    Some(unsafe { cosine_distance_f32_avx512(left, right) })
}

#[cfg(all(feature = "simd-unsafe", target_arch = "aarch64"))]
/// NEON target-feature kernel.
///
/// Call through `dot_f32_neon_checked` so CPU-feature policy and equal-length
/// metric semantics stay centralized.
#[target_feature(enable = "neon")]
fn dot_f32_neon(left: &[f32], right: &[f32]) -> f32 {
    use std::arch::aarch64::vmulq_f32;

    debug_assert_eq!(left.len(), right.len());

    let mut sum = 0.0_f32;
    let mut lanes = [0.0_f32; 4];
    let mut left_chunks = left.chunks_exact(4);
    let mut right_chunks = right.chunks_exact(4);

    for (left_chunk, right_chunk) in (&mut left_chunks).zip(&mut right_chunks) {
        let product = vmulq_f32(load_f32x4(left_chunk), load_f32x4(right_chunk));
        store_f32x4(product, &mut lanes);
        for value in lanes {
            sum += value;
        }
    }

    accumulate_dot_tail(left_chunks.remainder(), right_chunks.remainder(), &mut sum);
    sum
}

#[cfg(all(feature = "simd-unsafe", target_arch = "aarch64"))]
/// NEON target-feature kernel.
///
/// Call through `l2_distance_f32_neon_checked` so CPU-feature policy and
/// equal-length metric semantics stay centralized.
#[target_feature(enable = "neon")]
fn l2_distance_f32_neon(left: &[f32], right: &[f32]) -> f32 {
    use std::arch::aarch64::{vmulq_f32, vsubq_f32};

    debug_assert_eq!(left.len(), right.len());

    let mut sum = 0.0_f32;
    let mut lanes = [0.0_f32; 4];
    let mut left_chunks = left.chunks_exact(4);
    let mut right_chunks = right.chunks_exact(4);

    for (left_chunk, right_chunk) in (&mut left_chunks).zip(&mut right_chunks) {
        let delta = vsubq_f32(load_f32x4(left_chunk), load_f32x4(right_chunk));
        store_f32x4(vmulq_f32(delta, delta), &mut lanes);
        for value in lanes {
            sum += value;
        }
    }

    accumulate_l2_squared_tail(left_chunks.remainder(), right_chunks.remainder(), &mut sum);
    sum.sqrt()
}

#[cfg(all(feature = "simd-unsafe", target_arch = "aarch64"))]
/// NEON target-feature kernel.
///
/// Call through `cosine_distance_f32_neon_checked` so CPU-feature policy and
/// equal-length metric semantics stay centralized.
#[target_feature(enable = "neon")]
fn cosine_distance_f32_neon(left: &[f32], right: &[f32]) -> Option<f32> {
    use std::arch::aarch64::vmulq_f32;

    debug_assert_eq!(left.len(), right.len());

    let mut dot = 0.0_f32;
    let mut left_norm = 0.0_f32;
    let mut right_norm = 0.0_f32;
    let mut dot_lanes = [0.0_f32; 4];
    let mut left_norm_lanes = [0.0_f32; 4];
    let mut right_norm_lanes = [0.0_f32; 4];
    let mut left_chunks = left.chunks_exact(4);
    let mut right_chunks = right.chunks_exact(4);

    for (left_chunk, right_chunk) in (&mut left_chunks).zip(&mut right_chunks) {
        let left_vec = load_f32x4(left_chunk);
        let right_vec = load_f32x4(right_chunk);
        store_f32x4(vmulq_f32(left_vec, right_vec), &mut dot_lanes);
        store_f32x4(vmulq_f32(left_vec, left_vec), &mut left_norm_lanes);
        store_f32x4(vmulq_f32(right_vec, right_vec), &mut right_norm_lanes);
        for idx in 0..4 {
            dot += dot_lanes[idx];
            left_norm += left_norm_lanes[idx];
            right_norm += right_norm_lanes[idx];
        }
    }

    accumulate_cosine_tail(
        left_chunks.remainder(),
        right_chunks.remainder(),
        &mut dot,
        &mut left_norm,
        &mut right_norm,
    );

    finish_cosine_distance(dot, left_norm, right_norm)
}

#[cfg(all(feature = "simd-unsafe", target_arch = "x86_64"))]
/// AVX2 target-feature kernel.
///
/// Call through `dot_f32_avx2_if_available` so runtime CPUID policy and
/// equal-length metric semantics stay centralized.
#[target_feature(enable = "avx2")]
fn dot_f32_avx2(left: &[f32], right: &[f32]) -> f32 {
    use std::arch::x86_64::_mm256_mul_ps;

    debug_assert_eq!(left.len(), right.len());

    let mut sum = 0.0_f32;
    let mut lanes = [0.0_f32; 8];
    let mut left_chunks = left.chunks_exact(8);
    let mut right_chunks = right.chunks_exact(8);

    for (left_chunk, right_chunk) in (&mut left_chunks).zip(&mut right_chunks) {
        let product = _mm256_mul_ps(load_m256_f32(left_chunk), load_m256_f32(right_chunk));
        store_m256_f32(product, &mut lanes);
        for value in lanes {
            sum += value;
        }
    }

    accumulate_dot_tail(left_chunks.remainder(), right_chunks.remainder(), &mut sum);
    sum
}

#[cfg(all(feature = "simd-unsafe", target_arch = "x86_64"))]
/// AVX2 target-feature kernel.
///
/// Call through `l2_distance_f32_avx2_if_available` so runtime CPUID policy
/// and equal-length metric semantics stay centralized.
#[target_feature(enable = "avx2")]
fn l2_distance_f32_avx2(left: &[f32], right: &[f32]) -> f32 {
    use std::arch::x86_64::{_mm256_mul_ps, _mm256_sub_ps};

    debug_assert_eq!(left.len(), right.len());

    let mut sum = 0.0_f32;
    let mut lanes = [0.0_f32; 8];
    let mut left_chunks = left.chunks_exact(8);
    let mut right_chunks = right.chunks_exact(8);

    for (left_chunk, right_chunk) in (&mut left_chunks).zip(&mut right_chunks) {
        let delta = _mm256_sub_ps(load_m256_f32(left_chunk), load_m256_f32(right_chunk));
        store_m256_f32(_mm256_mul_ps(delta, delta), &mut lanes);
        for value in lanes {
            sum += value;
        }
    }

    accumulate_l2_squared_tail(left_chunks.remainder(), right_chunks.remainder(), &mut sum);
    sum.sqrt()
}

#[cfg(all(feature = "simd-unsafe", target_arch = "x86_64"))]
/// AVX2 target-feature kernel.
///
/// Call through `cosine_distance_f32_avx2_if_available` so runtime CPUID
/// policy and equal-length metric semantics stay centralized.
#[target_feature(enable = "avx2")]
fn cosine_distance_f32_avx2(left: &[f32], right: &[f32]) -> Option<f32> {
    use std::arch::x86_64::_mm256_mul_ps;

    debug_assert_eq!(left.len(), right.len());

    let mut dot = 0.0_f32;
    let mut left_norm = 0.0_f32;
    let mut right_norm = 0.0_f32;
    let mut dot_lanes = [0.0_f32; 8];
    let mut left_norm_lanes = [0.0_f32; 8];
    let mut right_norm_lanes = [0.0_f32; 8];
    let mut left_chunks = left.chunks_exact(8);
    let mut right_chunks = right.chunks_exact(8);

    for (left_chunk, right_chunk) in (&mut left_chunks).zip(&mut right_chunks) {
        let left_vec = load_m256_f32(left_chunk);
        let right_vec = load_m256_f32(right_chunk);
        store_m256_f32(_mm256_mul_ps(left_vec, right_vec), &mut dot_lanes);
        store_m256_f32(_mm256_mul_ps(left_vec, left_vec), &mut left_norm_lanes);
        store_m256_f32(_mm256_mul_ps(right_vec, right_vec), &mut right_norm_lanes);
        for idx in 0..8 {
            dot += dot_lanes[idx];
            left_norm += left_norm_lanes[idx];
            right_norm += right_norm_lanes[idx];
        }
    }

    accumulate_cosine_tail(
        left_chunks.remainder(),
        right_chunks.remainder(),
        &mut dot,
        &mut left_norm,
        &mut right_norm,
    );

    finish_cosine_distance(dot, left_norm, right_norm)
}

#[cfg(all(feature = "simd-unsafe", target_arch = "x86_64"))]
/// AVX-512F target-feature kernel.
///
/// Call through `dot_f32_avx512_if_available` so runtime CPUID policy and
/// equal-length metric semantics stay centralized.
#[allow(clippy::incompatible_msrv)] // AVX-512 intrinsics are runtime-gated and x86_64-only.
#[target_feature(enable = "avx512f")]
fn dot_f32_avx512(left: &[f32], right: &[f32]) -> f32 {
    use std::arch::x86_64::_mm512_mul_ps;

    debug_assert_eq!(left.len(), right.len());

    let mut sum = 0.0_f32;
    let mut lanes = [0.0_f32; 16];
    let mut left_chunks = left.chunks_exact(16);
    let mut right_chunks = right.chunks_exact(16);

    for (left_chunk, right_chunk) in (&mut left_chunks).zip(&mut right_chunks) {
        let product = _mm512_mul_ps(load_m512_f32(left_chunk), load_m512_f32(right_chunk));
        store_m512_f32(product, &mut lanes);
        for value in lanes {
            sum += value;
        }
    }

    accumulate_dot_tail(left_chunks.remainder(), right_chunks.remainder(), &mut sum);
    sum
}

#[cfg(all(feature = "simd-unsafe", target_arch = "x86_64"))]
/// AVX-512F target-feature kernel.
///
/// Call through `l2_distance_f32_avx512_if_available` so runtime CPUID policy
/// and equal-length metric semantics stay centralized.
#[allow(clippy::incompatible_msrv)] // AVX-512 intrinsics are runtime-gated and x86_64-only.
#[target_feature(enable = "avx512f")]
fn l2_distance_f32_avx512(left: &[f32], right: &[f32]) -> f32 {
    use std::arch::x86_64::{_mm512_mul_ps, _mm512_sub_ps};

    debug_assert_eq!(left.len(), right.len());

    let mut sum = 0.0_f32;
    let mut lanes = [0.0_f32; 16];
    let mut left_chunks = left.chunks_exact(16);
    let mut right_chunks = right.chunks_exact(16);

    for (left_chunk, right_chunk) in (&mut left_chunks).zip(&mut right_chunks) {
        let delta = _mm512_sub_ps(load_m512_f32(left_chunk), load_m512_f32(right_chunk));
        store_m512_f32(_mm512_mul_ps(delta, delta), &mut lanes);
        for value in lanes {
            sum += value;
        }
    }

    accumulate_l2_squared_tail(left_chunks.remainder(), right_chunks.remainder(), &mut sum);
    sum.sqrt()
}

#[cfg(all(feature = "simd-unsafe", target_arch = "x86_64"))]
/// AVX-512F target-feature kernel.
///
/// Call through `cosine_distance_f32_avx512_if_available` so runtime CPUID
/// policy and equal-length metric semantics stay centralized.
#[allow(clippy::incompatible_msrv)] // AVX-512 intrinsics are runtime-gated and x86_64-only.
#[target_feature(enable = "avx512f")]
fn cosine_distance_f32_avx512(left: &[f32], right: &[f32]) -> Option<f32> {
    use std::arch::x86_64::_mm512_mul_ps;

    debug_assert_eq!(left.len(), right.len());

    let mut dot = 0.0_f32;
    let mut left_norm = 0.0_f32;
    let mut right_norm = 0.0_f32;
    let mut dot_lanes = [0.0_f32; 16];
    let mut left_norm_lanes = [0.0_f32; 16];
    let mut right_norm_lanes = [0.0_f32; 16];
    let mut left_chunks = left.chunks_exact(16);
    let mut right_chunks = right.chunks_exact(16);

    for (left_chunk, right_chunk) in (&mut left_chunks).zip(&mut right_chunks) {
        let left_vec = load_m512_f32(left_chunk);
        let right_vec = load_m512_f32(right_chunk);
        store_m512_f32(_mm512_mul_ps(left_vec, right_vec), &mut dot_lanes);
        store_m512_f32(_mm512_mul_ps(left_vec, left_vec), &mut left_norm_lanes);
        store_m512_f32(_mm512_mul_ps(right_vec, right_vec), &mut right_norm_lanes);
        for idx in 0..16 {
            dot += dot_lanes[idx];
            left_norm += left_norm_lanes[idx];
            right_norm += right_norm_lanes[idx];
        }
    }

    accumulate_cosine_tail(
        left_chunks.remainder(),
        right_chunks.remainder(),
        &mut dot,
        &mut left_norm,
        &mut right_norm,
    );

    finish_cosine_distance(dot, left_norm, right_norm)
}

#[cfg(feature = "simd-unsafe")]
#[inline]
fn accumulate_dot_tail(left: &[f32], right: &[f32], sum: &mut f32) {
    for (&left_value, &right_value) in left.iter().zip(right.iter()) {
        *sum += left_value * right_value;
    }
}

#[cfg(feature = "simd-unsafe")]
#[inline]
fn accumulate_l2_squared_tail(left: &[f32], right: &[f32], sum: &mut f32) {
    for (&left_value, &right_value) in left.iter().zip(right.iter()) {
        let delta = left_value - right_value;
        *sum += delta * delta;
    }
}

#[cfg(feature = "simd-unsafe")]
#[inline]
fn accumulate_cosine_tail(
    left: &[f32],
    right: &[f32],
    dot: &mut f32,
    left_norm: &mut f32,
    right_norm: &mut f32,
) {
    for (&left_value, &right_value) in left.iter().zip(right.iter()) {
        *dot += left_value * right_value;
        *left_norm += left_value * left_value;
        *right_norm += right_value * right_value;
    }
}

#[cfg(feature = "simd-unsafe")]
#[inline]
fn finish_cosine_distance(dot: f32, left_norm: f32, right_norm: f32) -> Option<f32> {
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

    #[cfg(feature = "simd-unsafe")]
    fn vectors_for_tail_len(len: usize) -> (Vec<f32>, Vec<f32>) {
        const LEFT_PATTERN: [f32; 17] = [
            -2.0, -1.75, -1.5, -1.25, -1.0, -0.75, -0.5, -0.25, 0.0, 0.25, 0.5, 0.75, 1.0, 1.25,
            1.5, 1.75, 2.0,
        ];
        const RIGHT_PATTERN: [f32; 19] = [
            -1.125, -1.0, -0.875, -0.75, -0.625, -0.5, -0.375, -0.25, -0.125, 0.0, 0.125, 0.25,
            0.375, 0.5, 0.625, 0.75, 0.875, 1.0, 1.125,
        ];
        let left = (0..len)
            .map(|idx| LEFT_PATTERN[idx % LEFT_PATTERN.len()])
            .collect::<Vec<_>>();
        let right = (0..len)
            .map(|idx| RIGHT_PATTERN[(idx * 7) % RIGHT_PATTERN.len()])
            .collect::<Vec<_>>();
        (left, right)
    }

    #[cfg(all(feature = "simd-unsafe", target_arch = "aarch64"))]
    #[test]
    fn neon_kernels_match_scalar_at_tail_boundaries() {
        for len in 0..=35 {
            let (left, right) = vectors_for_tail_len(len);

            assert_eq!(
                dot_f32_neon_checked(&left, &right).to_bits(),
                dot_f32_scalar(&left, &right).to_bits(),
                "dot len={len}"
            );
            assert_eq!(
                l2_distance_f32_neon_checked(&left, &right).to_bits(),
                l2_distance_f32_scalar(&left, &right).to_bits(),
                "l2 len={len}"
            );
            match (
                cosine_distance_f32_neon_checked(&left, &right),
                cosine_distance_f32_scalar(&left, &right),
            ) {
                (Some(got), Some(want)) => {
                    assert_eq!(got.to_bits(), want.to_bits(), "cosine len={len}");
                }
                (None, None) => {}
                (got, want) => panic!("cosine len={len}: got {got:?}, want {want:?}"),
            }
        }
    }

    #[cfg(all(feature = "simd-unsafe", target_arch = "x86_64"))]
    #[test]
    fn avx2_kernels_match_scalar_at_tail_boundaries_when_available() {
        if !std::arch::is_x86_feature_detected!("avx2") {
            return;
        }

        for len in 0..=35 {
            let (left, right) = vectors_for_tail_len(len);

            assert_eq!(
                dot_f32_avx2_if_available(&left, &right)
                    .expect("AVX2 checked above")
                    .to_bits(),
                dot_f32_scalar(&left, &right).to_bits(),
                "dot len={len}"
            );
            assert_eq!(
                l2_distance_f32_avx2_if_available(&left, &right)
                    .expect("AVX2 checked above")
                    .to_bits(),
                l2_distance_f32_scalar(&left, &right).to_bits(),
                "l2 len={len}"
            );
            match (
                cosine_distance_f32_avx2_if_available(&left, &right).expect("AVX2 checked above"),
                cosine_distance_f32_scalar(&left, &right),
            ) {
                (Some(got), Some(want)) => {
                    assert_eq!(got.to_bits(), want.to_bits(), "cosine len={len}");
                }
                (None, None) => {}
                (got, want) => panic!("cosine len={len}: got {got:?}, want {want:?}"),
            }
        }
    }

    #[cfg(all(feature = "simd-unsafe", target_arch = "x86_64"))]
    #[test]
    fn avx512_kernels_match_scalar_at_tail_boundaries_when_available() {
        if !std::arch::is_x86_feature_detected!("avx512f") {
            return;
        }

        for len in 0..=67 {
            let (left, right) = vectors_for_tail_len(len);

            assert_eq!(
                dot_f32_avx512_if_available(&left, &right)
                    .expect("AVX-512F checked above")
                    .to_bits(),
                dot_f32_scalar(&left, &right).to_bits(),
                "dot len={len}"
            );
            assert_eq!(
                l2_distance_f32_avx512_if_available(&left, &right)
                    .expect("AVX-512F checked above")
                    .to_bits(),
                l2_distance_f32_scalar(&left, &right).to_bits(),
                "l2 len={len}"
            );
            match (
                cosine_distance_f32_avx512_if_available(&left, &right)
                    .expect("AVX-512F checked above"),
                cosine_distance_f32_scalar(&left, &right),
            ) {
                (Some(got), Some(want)) => {
                    assert_eq!(got.to_bits(), want.to_bits(), "cosine len={len}");
                }
                (None, None) => {}
                (got, want) => panic!("cosine len={len}: got {got:?}, want {want:?}"),
            }
        }
    }

    #[test]
    fn exact_top_k_f32_l2_orders_rows_and_ties_by_row() {
        let vectors = [
            vec![3.0, 0.0],
            vec![1.0, 0.0],
            vec![0.0, 1.0],
            vec![0.5, 0.0],
        ];
        let rows = vectors.iter().map(Vec::as_slice).collect::<Vec<_>>();

        let hits = exact_top_k_f32(&rows, &[0.0, 0.0], VectorMetric::L2, 3);
        assert_eq!(
            hits,
            vec![
                VectorTopKHit {
                    row: 3,
                    distance: 0.5
                },
                VectorTopKHit {
                    row: 1,
                    distance: 1.0
                },
                VectorTopKHit {
                    row: 2,
                    distance: 1.0
                },
            ]
        );
    }

    #[test]
    fn exact_top_k_f32_flat_scans_row_major_batch() {
        let values = [
            3.0, 0.0, //
            1.0, 0.0, //
            0.0, 1.0, //
            0.5, 0.0,
        ];

        let hits = exact_top_k_f32_flat(&values, 2, &[0.0, 0.0], VectorMetric::L2, 3);
        assert_eq!(
            hits.iter().map(|hit| hit.row).collect::<Vec<_>>(),
            vec![3, 1, 2]
        );
    }

    #[test]
    fn exact_top_k_f32_supports_cosine_inner_product_and_l1() {
        let vectors = [
            vec![1.0, 0.0],
            vec![0.0, 1.0],
            vec![2.0, 0.0],
            vec![-1.0, 0.0],
        ];
        let rows = vectors.iter().map(Vec::as_slice).collect::<Vec<_>>();

        let cosine = exact_top_k_f32(&rows, &[1.0, 0.0], VectorMetric::Cosine, 2);
        assert_eq!(
            cosine.iter().map(|hit| hit.row).collect::<Vec<_>>(),
            vec![0, 2]
        );

        let inner = exact_top_k_f32(&rows, &[1.0, 0.0], VectorMetric::NegativeInnerProduct, 2);
        assert_eq!(
            inner.iter().map(|hit| hit.row).collect::<Vec<_>>(),
            vec![2, 0]
        );

        let l1 = exact_top_k_f32(&rows, &[1.0, 1.0], VectorMetric::L1, 2);
        assert_eq!(l1.iter().map(|hit| hit.row).collect::<Vec<_>>(), vec![0, 1]);
    }

    #[test]
    fn exact_top_k_f32_zero_k_returns_empty() {
        let vectors = [vec![1.0, 0.0]];
        let rows = vectors.iter().map(Vec::as_slice).collect::<Vec<_>>();
        assert!(exact_top_k_f32(&rows, &[0.0, 0.0], VectorMetric::L2, 0).is_empty());
    }

    #[test]
    #[should_panic(expected = "exact_top_k_f32: vector length mismatch")]
    fn exact_top_k_f32_panics_on_dimension_mismatch() {
        let vectors = [vec![1.0]];
        let rows = vectors.iter().map(Vec::as_slice).collect::<Vec<_>>();
        let _ = exact_top_k_f32(&rows, &[0.0, 0.0], VectorMetric::L2, 1);
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
