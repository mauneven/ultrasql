//! Dense `f32` vector similarity kernels.
//!
//! Scalar fallback implementations are the source of truth. Public wrappers
//! dispatch to NEON, AVX2, or AVX-512 when available and keep exact top-k scans
//! on the same metric contract.

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
    #[cfg(target_arch = "aarch64")]
    {
        // SAFETY: NEON is part of the aarch64 baseline; lengths were checked
        // above and the helper reads only inside the borrowed slices.
        unsafe { dot_f32_neon(left, right) }
    }
    #[cfg(not(target_arch = "aarch64"))]
    {
        #[cfg(target_arch = "x86_64")]
        {
            if std::arch::is_x86_feature_detected!("avx512f") {
                // SAFETY: runtime CPUID confirmed AVX-512F; lengths were
                // checked above and the helper reads only inside borrowed slices.
                return unsafe { dot_f32_avx512(left, right) };
            }
            if std::arch::is_x86_feature_detected!("avx2") {
                // SAFETY: runtime CPUID confirmed AVX2; lengths were checked
                // above and the helper reads only inside the borrowed slices.
                return unsafe { dot_f32_avx2(left, right) };
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
    #[cfg(target_arch = "aarch64")]
    {
        // SAFETY: NEON is part of the aarch64 baseline; lengths were checked
        // above and the helper reads only inside the borrowed slices.
        unsafe { l2_distance_f32_neon(left, right) }
    }
    #[cfg(not(target_arch = "aarch64"))]
    {
        #[cfg(target_arch = "x86_64")]
        {
            if std::arch::is_x86_feature_detected!("avx512f") {
                // SAFETY: runtime CPUID confirmed AVX-512F; lengths were
                // checked above and the helper reads only inside borrowed slices.
                return unsafe { l2_distance_f32_avx512(left, right) };
            }
            if std::arch::is_x86_feature_detected!("avx2") {
                // SAFETY: runtime CPUID confirmed AVX2; lengths were checked
                // above and the helper reads only inside the borrowed slices.
                return unsafe { l2_distance_f32_avx2(left, right) };
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
    #[cfg(target_arch = "aarch64")]
    {
        // SAFETY: NEON is part of the aarch64 baseline; lengths were checked
        // above and the helper reads only inside the borrowed slices.
        unsafe { cosine_distance_f32_neon(left, right) }
    }
    #[cfg(not(target_arch = "aarch64"))]
    {
        #[cfg(target_arch = "x86_64")]
        {
            if std::arch::is_x86_feature_detected!("avx512f") {
                // SAFETY: runtime CPUID confirmed AVX-512F; lengths were
                // checked above and the helper reads only inside borrowed slices.
                return unsafe { cosine_distance_f32_avx512(left, right) };
            }
            if std::arch::is_x86_feature_detected!("avx2") {
                // SAFETY: runtime CPUID confirmed AVX2; lengths were checked
                // above and the helper reads only inside the borrowed slices.
                return unsafe { cosine_distance_f32_avx2(left, right) };
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

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn dot_f32_neon(left: &[f32], right: &[f32]) -> f32 {
    use std::arch::aarch64::{vld1q_f32, vmulq_f32, vst1q_f32};

    debug_assert_eq!(left.len(), right.len());

    let mut sum = 0.0_f32;
    let mut lanes = [0.0_f32; 4];
    let mut left_chunks = left.chunks_exact(4);
    let mut right_chunks = right.chunks_exact(4);

    for (left_chunk, right_chunk) in (&mut left_chunks).zip(&mut right_chunks) {
        // SAFETY: chunks_exact(4) guarantees both loads read four contiguous
        // `f32` lanes in-bounds. `lanes` has room for one 128-bit store.
        unsafe {
            let left_vec = vld1q_f32(left_chunk.as_ptr());
            let right_vec = vld1q_f32(right_chunk.as_ptr());
            let product = vmulq_f32(left_vec, right_vec);
            vst1q_f32(lanes.as_mut_ptr(), product);
        }
        for value in lanes {
            sum += value;
        }
    }

    accumulate_dot_tail(left_chunks.remainder(), right_chunks.remainder(), &mut sum);
    sum
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn l2_distance_f32_neon(left: &[f32], right: &[f32]) -> f32 {
    use std::arch::aarch64::{vld1q_f32, vmulq_f32, vst1q_f32, vsubq_f32};

    debug_assert_eq!(left.len(), right.len());

    let mut sum = 0.0_f32;
    let mut lanes = [0.0_f32; 4];
    let mut left_chunks = left.chunks_exact(4);
    let mut right_chunks = right.chunks_exact(4);

    for (left_chunk, right_chunk) in (&mut left_chunks).zip(&mut right_chunks) {
        // SAFETY: chunks_exact(4) guarantees both loads read four contiguous
        // `f32` lanes in-bounds. `lanes` has room for one 128-bit store.
        unsafe {
            let left_vec = vld1q_f32(left_chunk.as_ptr());
            let right_vec = vld1q_f32(right_chunk.as_ptr());
            let delta = vsubq_f32(left_vec, right_vec);
            let squared = vmulq_f32(delta, delta);
            vst1q_f32(lanes.as_mut_ptr(), squared);
        }
        for value in lanes {
            sum += value;
        }
    }

    accumulate_l2_squared_tail(left_chunks.remainder(), right_chunks.remainder(), &mut sum);
    sum.sqrt()
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn cosine_distance_f32_neon(left: &[f32], right: &[f32]) -> Option<f32> {
    use std::arch::aarch64::{vld1q_f32, vmulq_f32, vst1q_f32};

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
        // SAFETY: chunks_exact(4) guarantees both loads read four contiguous
        // `f32` lanes in-bounds. Each lane buffer has room for one 128-bit store.
        unsafe {
            let left_vec = vld1q_f32(left_chunk.as_ptr());
            let right_vec = vld1q_f32(right_chunk.as_ptr());
            vst1q_f32(dot_lanes.as_mut_ptr(), vmulq_f32(left_vec, right_vec));
            vst1q_f32(left_norm_lanes.as_mut_ptr(), vmulq_f32(left_vec, left_vec));
            vst1q_f32(
                right_norm_lanes.as_mut_ptr(),
                vmulq_f32(right_vec, right_vec),
            );
        }
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

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn dot_f32_avx2(left: &[f32], right: &[f32]) -> f32 {
    use std::arch::x86_64::{_mm256_loadu_ps, _mm256_mul_ps, _mm256_storeu_ps};

    debug_assert_eq!(left.len(), right.len());

    let mut sum = 0.0_f32;
    let mut lanes = [0.0_f32; 8];
    let mut left_chunks = left.chunks_exact(8);
    let mut right_chunks = right.chunks_exact(8);

    for (left_chunk, right_chunk) in (&mut left_chunks).zip(&mut right_chunks) {
        // SAFETY: chunks_exact(8) guarantees both loads read eight contiguous
        // `f32` lanes in-bounds. `lanes` has room for one 256-bit store.
        unsafe {
            let left_vec = _mm256_loadu_ps(left_chunk.as_ptr());
            let right_vec = _mm256_loadu_ps(right_chunk.as_ptr());
            let product = _mm256_mul_ps(left_vec, right_vec);
            _mm256_storeu_ps(lanes.as_mut_ptr(), product);
        }
        for value in lanes {
            sum += value;
        }
    }

    accumulate_dot_tail(left_chunks.remainder(), right_chunks.remainder(), &mut sum);
    sum
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn l2_distance_f32_avx2(left: &[f32], right: &[f32]) -> f32 {
    use std::arch::x86_64::{_mm256_loadu_ps, _mm256_mul_ps, _mm256_storeu_ps, _mm256_sub_ps};

    debug_assert_eq!(left.len(), right.len());

    let mut sum = 0.0_f32;
    let mut lanes = [0.0_f32; 8];
    let mut left_chunks = left.chunks_exact(8);
    let mut right_chunks = right.chunks_exact(8);

    for (left_chunk, right_chunk) in (&mut left_chunks).zip(&mut right_chunks) {
        // SAFETY: chunks_exact(8) guarantees both loads read eight contiguous
        // `f32` lanes in-bounds. `lanes` has room for one 256-bit store.
        unsafe {
            let left_vec = _mm256_loadu_ps(left_chunk.as_ptr());
            let right_vec = _mm256_loadu_ps(right_chunk.as_ptr());
            let delta = _mm256_sub_ps(left_vec, right_vec);
            let squared = _mm256_mul_ps(delta, delta);
            _mm256_storeu_ps(lanes.as_mut_ptr(), squared);
        }
        for value in lanes {
            sum += value;
        }
    }

    accumulate_l2_squared_tail(left_chunks.remainder(), right_chunks.remainder(), &mut sum);
    sum.sqrt()
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn cosine_distance_f32_avx2(left: &[f32], right: &[f32]) -> Option<f32> {
    use std::arch::x86_64::{_mm256_loadu_ps, _mm256_mul_ps, _mm256_storeu_ps};

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
        // SAFETY: chunks_exact(8) guarantees both loads read eight contiguous
        // `f32` lanes in-bounds. Each lane buffer has room for one 256-bit store.
        unsafe {
            let left_vec = _mm256_loadu_ps(left_chunk.as_ptr());
            let right_vec = _mm256_loadu_ps(right_chunk.as_ptr());
            _mm256_storeu_ps(dot_lanes.as_mut_ptr(), _mm256_mul_ps(left_vec, right_vec));
            _mm256_storeu_ps(
                left_norm_lanes.as_mut_ptr(),
                _mm256_mul_ps(left_vec, left_vec),
            );
            _mm256_storeu_ps(
                right_norm_lanes.as_mut_ptr(),
                _mm256_mul_ps(right_vec, right_vec),
            );
        }
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

#[cfg(target_arch = "x86_64")]
#[allow(clippy::incompatible_msrv)] // AVX-512 intrinsics are runtime-gated and x86_64-only.
#[target_feature(enable = "avx512f")]
unsafe fn dot_f32_avx512(left: &[f32], right: &[f32]) -> f32 {
    use std::arch::x86_64::{_mm512_loadu_ps, _mm512_mul_ps, _mm512_storeu_ps};

    debug_assert_eq!(left.len(), right.len());

    let mut sum = 0.0_f32;
    let mut lanes = [0.0_f32; 16];
    let mut left_chunks = left.chunks_exact(16);
    let mut right_chunks = right.chunks_exact(16);

    for (left_chunk, right_chunk) in (&mut left_chunks).zip(&mut right_chunks) {
        // SAFETY: chunks_exact(16) guarantees both loads read sixteen
        // contiguous `f32` lanes in-bounds. `lanes` has room for one
        // 512-bit store.
        unsafe {
            let left_vec = _mm512_loadu_ps(left_chunk.as_ptr());
            let right_vec = _mm512_loadu_ps(right_chunk.as_ptr());
            let product = _mm512_mul_ps(left_vec, right_vec);
            _mm512_storeu_ps(lanes.as_mut_ptr(), product);
        }
        for value in lanes {
            sum += value;
        }
    }

    accumulate_dot_tail(left_chunks.remainder(), right_chunks.remainder(), &mut sum);
    sum
}

#[cfg(target_arch = "x86_64")]
#[allow(clippy::incompatible_msrv)] // AVX-512 intrinsics are runtime-gated and x86_64-only.
#[target_feature(enable = "avx512f")]
unsafe fn l2_distance_f32_avx512(left: &[f32], right: &[f32]) -> f32 {
    use std::arch::x86_64::{_mm512_loadu_ps, _mm512_mul_ps, _mm512_storeu_ps, _mm512_sub_ps};

    debug_assert_eq!(left.len(), right.len());

    let mut sum = 0.0_f32;
    let mut lanes = [0.0_f32; 16];
    let mut left_chunks = left.chunks_exact(16);
    let mut right_chunks = right.chunks_exact(16);

    for (left_chunk, right_chunk) in (&mut left_chunks).zip(&mut right_chunks) {
        // SAFETY: chunks_exact(16) guarantees both loads read sixteen
        // contiguous `f32` lanes in-bounds. `lanes` has room for one
        // 512-bit store.
        unsafe {
            let left_vec = _mm512_loadu_ps(left_chunk.as_ptr());
            let right_vec = _mm512_loadu_ps(right_chunk.as_ptr());
            let delta = _mm512_sub_ps(left_vec, right_vec);
            let squared = _mm512_mul_ps(delta, delta);
            _mm512_storeu_ps(lanes.as_mut_ptr(), squared);
        }
        for value in lanes {
            sum += value;
        }
    }

    accumulate_l2_squared_tail(left_chunks.remainder(), right_chunks.remainder(), &mut sum);
    sum.sqrt()
}

#[cfg(target_arch = "x86_64")]
#[allow(clippy::incompatible_msrv)] // AVX-512 intrinsics are runtime-gated and x86_64-only.
#[target_feature(enable = "avx512f")]
unsafe fn cosine_distance_f32_avx512(left: &[f32], right: &[f32]) -> Option<f32> {
    use std::arch::x86_64::{_mm512_loadu_ps, _mm512_mul_ps, _mm512_storeu_ps};

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
        // SAFETY: chunks_exact(16) guarantees both loads read sixteen
        // contiguous `f32` lanes in-bounds. Each lane buffer has room
        // for one 512-bit store.
        unsafe {
            let left_vec = _mm512_loadu_ps(left_chunk.as_ptr());
            let right_vec = _mm512_loadu_ps(right_chunk.as_ptr());
            _mm512_storeu_ps(dot_lanes.as_mut_ptr(), _mm512_mul_ps(left_vec, right_vec));
            _mm512_storeu_ps(
                left_norm_lanes.as_mut_ptr(),
                _mm512_mul_ps(left_vec, left_vec),
            );
            _mm512_storeu_ps(
                right_norm_lanes.as_mut_ptr(),
                _mm512_mul_ps(right_vec, right_vec),
            );
        }
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

#[inline]
fn accumulate_dot_tail(left: &[f32], right: &[f32], sum: &mut f32) {
    for (&left_value, &right_value) in left.iter().zip(right.iter()) {
        *sum += left_value * right_value;
    }
}

#[inline]
fn accumulate_l2_squared_tail(left: &[f32], right: &[f32], sum: &mut f32) {
    for (&left_value, &right_value) in left.iter().zip(right.iter()) {
        let delta = left_value - right_value;
        *sum += delta * delta;
    }
}

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

    #[cfg(target_arch = "aarch64")]
    #[test]
    fn neon_kernels_match_scalar_at_tail_boundaries() {
        for len in 0..=35 {
            let (left, right) = vectors_for_tail_len(len);

            // SAFETY: aarch64 has baseline NEON, and helper preconditions are
            // satisfied by same-length vectors generated above.
            assert_eq!(
                unsafe { dot_f32_neon(&left, &right) }.to_bits(),
                dot_f32_scalar(&left, &right).to_bits(),
                "dot len={len}"
            );
            // SAFETY: aarch64 has baseline NEON, and helper preconditions are
            // satisfied by same-length vectors generated above.
            assert_eq!(
                unsafe { l2_distance_f32_neon(&left, &right) }.to_bits(),
                l2_distance_f32_scalar(&left, &right).to_bits(),
                "l2 len={len}"
            );
            match (
                // SAFETY: aarch64 has baseline NEON, and helper preconditions
                // are satisfied by same-length vectors generated above.
                unsafe { cosine_distance_f32_neon(&left, &right) },
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

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn avx2_kernels_match_scalar_at_tail_boundaries_when_available() {
        if !std::arch::is_x86_feature_detected!("avx2") {
            return;
        }

        for len in 0..=35 {
            let (left, right) = vectors_for_tail_len(len);

            // SAFETY: CPUID confirmed AVX2, and helper preconditions are
            // satisfied by same-length vectors generated above.
            assert_eq!(
                unsafe { dot_f32_avx2(&left, &right) }.to_bits(),
                dot_f32_scalar(&left, &right).to_bits(),
                "dot len={len}"
            );
            // SAFETY: CPUID confirmed AVX2, and helper preconditions are
            // satisfied by same-length vectors generated above.
            assert_eq!(
                unsafe { l2_distance_f32_avx2(&left, &right) }.to_bits(),
                l2_distance_f32_scalar(&left, &right).to_bits(),
                "l2 len={len}"
            );
            match (
                // SAFETY: CPUID confirmed AVX2, and helper preconditions are
                // satisfied by same-length vectors generated above.
                unsafe { cosine_distance_f32_avx2(&left, &right) },
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

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn avx512_kernels_match_scalar_at_tail_boundaries_when_available() {
        if !std::arch::is_x86_feature_detected!("avx512f") {
            return;
        }

        for len in 0..=67 {
            let (left, right) = vectors_for_tail_len(len);

            // SAFETY: CPUID confirmed AVX-512F, and helper preconditions are
            // satisfied by same-length vectors generated above.
            assert_eq!(
                unsafe { dot_f32_avx512(&left, &right) }.to_bits(),
                dot_f32_scalar(&left, &right).to_bits(),
                "dot len={len}"
            );
            // SAFETY: CPUID confirmed AVX-512F, and helper preconditions are
            // satisfied by same-length vectors generated above.
            assert_eq!(
                unsafe { l2_distance_f32_avx512(&left, &right) }.to_bits(),
                l2_distance_f32_scalar(&left, &right).to_bits(),
                "l2 len={len}"
            );
            match (
                // SAFETY: CPUID confirmed AVX-512F, and helper preconditions
                // are satisfied by same-length vectors generated above.
                unsafe { cosine_distance_f32_avx512(&left, &right) },
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
