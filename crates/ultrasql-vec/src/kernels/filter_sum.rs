//! Fused branchless filter+sum kernels.
//!
//! These kernels implement the hot OLAP pattern
//! `SELECT SUM(x) FROM t WHERE y > 0` in a single pass over the inputs
//! with no intermediate `Bitmap` allocation. Each element contributes
//! `x[i] & ((y[i] > 0) ? -1 : 0)` to the running sum, so the predicate
//! is folded into a branchless AND-mask.
//!
//! Three implementations are provided, dispatched at compile time:
//!
//! 1. **`AArch64` NEON** (Apple M-series and ARMv8 Linux) using
//!    128-bit `int64x2_t` lanes, 16 vectors unrolled per iteration
//!    (32 i64 elements per loop trip), and four independent 128-bit
//!    accumulator lanes to break the add-latency dependency chain.
//! 2. **`x86_64` AVX2** using 256-bit `__m256i` lanes, four vectors
//!    unrolled per iteration (16 i64 elements per loop trip), and two
//!    independent accumulators. Gated on `target_feature = "avx2"`.
//! 3. **Portable scalar** with an auto-vectorization-friendly inner loop
//!    that LLVM lowers to a native vectorized form on every supported
//!    target.
//!
//! All implementations agree bit-for-bit (validated by property tests in
//! the unit test module below).
//!
//! NULL handling: the `_with_validity` variant accepts optional
//! validity bitmaps for `x` and `y` and AND-folds them into the per-row
//! mask. Following SQL three-valued logic, a NULL in `y` makes the
//! predicate UNKNOWN (treated as false), and a NULL in `x` contributes
//! nothing to the sum.

use crate::bitmap::Bitmap;
use crate::column::NumericColumn;

// ============================================================================
// Public API
// ============================================================================

/// Fused branchless filter+sum over two `i64` columns.
///
/// Returns `Σ x[i]` for every `i` where `y[i] > 0`. The mask is computed
/// per row as `(y[i] > 0) as i64`, sign-extended to all-ones via
/// `wrapping_neg`, and AND-ed into `x[i]` before accumulation. There is
/// no per-row branch.
///
/// Designed to approach memory-bandwidth-bound throughput on M-series
/// (NEON) and AVX2-class `x86_64` hosts. On a 10 M-row workload
/// (160 MB scanned across the two columns) this kernel targets ≤ 2 ms
/// median on an Apple M4 — the theoretical floor at ~80 GB/s of
/// memory bandwidth.
///
/// Behavior on length mismatch:
/// - In debug builds a `debug_assert` fires.
/// - In release builds the function returns `0` (length is the smaller
///   of the two, but the contract is `n_x == n_y`; callers that care
///   should check up front).
///
/// Wrapping arithmetic: per-row contributions and accumulation use
/// `wrapping_add`, matching the semantics of [`crate::kernels::sum_i64`]
/// and SQL's `SUM` over `BIGINT` (Postgres `bigint` arithmetic wraps in
/// integer arithmetic mode).
#[must_use]
pub fn filter_sum_i64_where_gt_zero(x: &NumericColumn<i64>, y: &NumericColumn<i64>) -> i64 {
    debug_assert_eq!(
        x.len(),
        y.len(),
        "filter_sum_i64_where_gt_zero: length mismatch",
    );
    if x.len() != y.len() {
        return 0;
    }
    let xs = x.data();
    let ys = y.data();
    filter_sum_dispatch(xs, ys)
}

/// Validity-aware variant of [`filter_sum_i64_where_gt_zero`].
///
/// Returns `Σ x[i]` for every row where the following all hold:
/// - `x_validity[i]` is set (or absent),
/// - `y_validity[i]` is set (or absent),
/// - `y[i] > 0`.
///
/// Validity bitmaps follow the Apache Arrow convention (1 = valid /
/// non-null). When both validity arguments are `None` this delegates to
/// the fast path. The slow path iterates the validity bitmaps in 64-bit
/// words and stays branchless within each word.
///
/// # Panics
///
/// Cannot panic for valid inputs. Length-mismatch is debug-asserted and
/// returns 0 in release as in the dense variant.
#[must_use]
pub fn filter_sum_i64_where_gt_zero_with_validity(
    x: &NumericColumn<i64>,
    y: &NumericColumn<i64>,
) -> i64 {
    debug_assert_eq!(
        x.len(),
        y.len(),
        "filter_sum_i64_where_gt_zero_with_validity: length mismatch",
    );
    if x.len() != y.len() {
        return 0;
    }
    let xs = x.data();
    let ys = y.data();

    match (x.nulls(), y.nulls()) {
        (None, None) => filter_sum_dispatch(xs, ys),
        (xb, yb) => filter_sum_with_validity_scalar(xs, ys, xb, yb),
    }
}

/// Portable scalar reference implementation. Source of truth for
/// property tests.
///
/// The inner loop is shaped so LLVM can autovectorize it on every
/// target. We use a branchless mask: `let m = -((y > 0) as i64);` is
/// either `0` or `-1` (`0xFFFF...FFFF`), and `x & m` is either `0` or
/// `x`. Accumulation uses `wrapping_add`.
#[must_use]
#[inline]
pub fn filter_sum_i64_where_gt_zero_scalar(x: &NumericColumn<i64>, y: &NumericColumn<i64>) -> i64 {
    debug_assert_eq!(
        x.len(),
        y.len(),
        "filter_sum_i64_where_gt_zero_scalar: length mismatch",
    );
    if x.len() != y.len() {
        return 0;
    }
    filter_sum_scalar_branchless(x.data(), y.data())
}

// ============================================================================
// Dispatch + scalar fast path
// ============================================================================

#[inline]
fn filter_sum_dispatch(xs: &[i64], ys: &[i64]) -> i64 {
    #[cfg(target_arch = "aarch64")]
    {
        // SAFETY: NEON is part of the AArch64 baseline ABI for every
        // platform we support (`aarch64-apple-darwin` and
        // `aarch64-unknown-linux-gnu` both require ARMv8-A which
        // mandates NEON). The function below only performs unaligned
        // loads (vld1q_s64) bounded by the slice lengths.
        return unsafe { filter_sum_neon(xs, ys) };
    }

    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    {
        // SAFETY: gated on the compile-time `target_feature = "avx2"`,
        // so executing the AVX2 intrinsics is sound on this build.
        return unsafe { filter_sum_avx2(xs, ys) };
    }

    #[allow(unreachable_code)]
    filter_sum_scalar_branchless(xs, ys)
}

/// Branchless scalar implementation. LLVM autovectorizes this to a
/// reasonable SIMD shape on every supported target; the hand-written
/// intrinsic paths above beat it primarily by improving ILP through
/// manual unrolling and dual accumulators.
#[inline]
fn filter_sum_scalar_branchless(xs: &[i64], ys: &[i64]) -> i64 {
    // Defensive: keep the loops in lock-step.
    let n = xs.len().min(ys.len());
    let xs = &xs[..n];
    let ys = &ys[..n];

    // Two independent accumulators for ILP.
    let mut s0: i64 = 0;
    let mut s1: i64 = 0;

    let chunks_x = xs.chunks_exact(8);
    let chunks_y = ys.chunks_exact(8);
    let rem_x = chunks_x.remainder();
    let rem_y = chunks_y.remainder();
    for (cx, cy) in chunks_x.zip(chunks_y) {
        // Process two halves of the 8-lane chunk on independent
        // accumulators. LLVM will widen each half to a SIMD register
        // and hoist the cmp/and/add chain.
        let x_chunk: &[i64; 8] = cx.try_into().expect("chunks_exact(8) yields 8 lanes");
        let y_chunk: &[i64; 8] = cy.try_into().expect("chunks_exact(8) yields 8 lanes");

        for j in 0..4_usize {
            let m = (i64::from(y_chunk[j] > 0)).wrapping_neg();
            s0 = s0.wrapping_add(x_chunk[j] & m);
        }
        for j in 4..8_usize {
            let m = (i64::from(y_chunk[j] > 0)).wrapping_neg();
            s1 = s1.wrapping_add(x_chunk[j] & m);
        }
    }

    // Tail.
    for (xv, yv) in rem_x.iter().zip(rem_y.iter()) {
        let m = (i64::from(*yv > 0)).wrapping_neg();
        s0 = s0.wrapping_add(*xv & m);
    }

    s0.wrapping_add(s1)
}

// ============================================================================
// Validity-aware path
// ============================================================================

/// Scalar validity-aware path.
///
/// We iterate the input in 64-row words. For each word we compute the
/// combined validity word (default to `!0` when a bitmap is absent),
/// then walk the 64 rows applying the branchless mask. A short scan
/// over 64 rows per validity word keeps the inner body small enough
/// for LLVM to keep loop-carried state in registers.
#[inline]
fn filter_sum_with_validity_scalar(
    xs: &[i64],
    ys: &[i64],
    x_valid: Option<&Bitmap>,
    y_valid: Option<&Bitmap>,
) -> i64 {
    let n = xs.len();
    debug_assert_eq!(ys.len(), n);
    if let Some(b) = x_valid {
        debug_assert_eq!(b.len(), n);
    }
    if let Some(b) = y_valid {
        debug_assert_eq!(b.len(), n);
    }

    let mut s0: i64 = 0;
    let mut s1: i64 = 0;

    let nwords = n / 64;
    for w in 0..nwords {
        let base = w * 64;
        let xv = x_valid.map_or(u64::MAX, |b| b.words()[w]);
        let yv = y_valid.map_or(u64::MAX, |b| b.words()[w]);
        let valid_word = xv & yv;

        // Process the 64 rows in halves to feed two accumulators.
        for j in 0..32_usize {
            let i = base + j;
            let valid_bit = ((valid_word >> j) & 1) != 0;
            let valid_mask = i64::from(valid_bit).wrapping_neg();
            let gt_mask = (i64::from(ys[i] > 0)).wrapping_neg();
            let m = valid_mask & gt_mask;
            s0 = s0.wrapping_add(xs[i] & m);
        }
        for j in 32..64_usize {
            let i = base + j;
            let valid_bit = ((valid_word >> j) & 1) != 0;
            let valid_mask = i64::from(valid_bit).wrapping_neg();
            let gt_mask = (i64::from(ys[i] > 0)).wrapping_neg();
            let m = valid_mask & gt_mask;
            s1 = s1.wrapping_add(xs[i] & m);
        }
    }

    // Tail.
    let tail_start = nwords * 64;
    if tail_start < n {
        let last_word = n - tail_start;
        let xv = x_valid.map_or(u64::MAX, |b| b.words()[nwords]);
        let yv = y_valid.map_or(u64::MAX, |b| b.words()[nwords]);
        let valid_word = xv & yv;
        for j in 0..last_word {
            let i = tail_start + j;
            let valid_bit = ((valid_word >> j) & 1) != 0;
            let valid_mask = i64::from(valid_bit).wrapping_neg();
            let gt_mask = (i64::from(ys[i] > 0)).wrapping_neg();
            let m = valid_mask & gt_mask;
            s0 = s0.wrapping_add(xs[i] & m);
        }
    }

    s0.wrapping_add(s1)
}

// ============================================================================
// AArch64 NEON implementation
// ============================================================================

/// NEON dense implementation.
///
/// Layout per loop trip:
/// - Issue four `vld1q_s64_x4` quad loads on each of `x` and `y`,
///   pulling in 16 × 128-bit vectors (32 i64 lanes, 256 B per stream).
///   The `_x4` variant compiles to a single `LD1 {Vd..Vd+3}` 64-byte
///   load on Apple Silicon, the widest grouped load NEON supports.
/// - For each vector, compare `y > vdupq_n_s64(0)` (`vcgtq_s64`) to
///   produce a 0-or-all-ones mask, AND with the corresponding `x`
///   vector, and add into one of *four* independent 128-bit
///   accumulators (`acc0..acc3`). Four accumulators give 4-way ILP so
///   the M-series add unit can issue every cycle through the load
///   latency of the next vector pair.
/// - Tail (< 32 lanes) goes through the branchless scalar path.
///
/// # Safety
///
/// - NEON is part of the `AArch64` baseline ABI on every supported
///   target; `target_feature = "neon"` is implied.
/// - `vld1q_s64_x4` performs four contiguous unaligned 16-byte loads;
///   `i64` slices have at least 8-byte alignment, which is acceptable.
/// - All pointer arithmetic is bounded by the loop count; the inner
///   loop processes only full 32-lane chunks, and the tail is handled
///   separately.
#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn filter_sum_neon(xs: &[i64], ys: &[i64]) -> i64 {
    use core::arch::aarch64::{
        int64x2_t, vaddq_s64, vaddvq_s64, vandq_s64, vcgtq_s64, vdupq_n_s64, vld1q_s64_x4,
        vreinterpretq_s64_u64,
    };

    let n = xs.len().min(ys.len());

    // Four independent accumulator vectors (each holds 2 i64 lanes).
    // The four-way schedule deepens ILP past what the two-vector
    // variant supports: with 4 accumulators the M-series add unit can
    // dual-issue every cycle through the load latency of the next
    // vector pair.
    let mut acc0: int64x2_t = vdupq_n_s64(0);
    let mut acc1: int64x2_t = vdupq_n_s64(0);
    let mut acc2: int64x2_t = vdupq_n_s64(0);
    let mut acc3: int64x2_t = vdupq_n_s64(0);
    let zero: int64x2_t = vdupq_n_s64(0);

    let x_ptr = xs.as_ptr();
    let y_ptr = ys.as_ptr();

    // 32 i64s per iteration → 16 vectors of 2 i64s each
    // (8 × `int64x2x2_t` load pairs). Each iteration touches 256 B of
    // `x` plus 256 B of `y` = 512 B, exactly two cache lines per
    // stream, which lets the M4 hardware prefetcher track both
    // streams cleanly. An explicit `PRFM PLDL1KEEP` lookahead was
    // tested at 4 iterations (1 KiB) ahead and produced no measurable
    // change — the hardware prefetcher already saturates the L1 fill
    // bandwidth on this access pattern. Code left without the hint
    // for clarity.
    //
    // Schedule: each `vld1q_s64_x4` reads four 128-bit vectors = 8
    // i64 lanes (compiles to a single 64-byte `LD1` quad-register
    // instruction on Apple Silicon, the widest grouped load NEON
    // supports). We issue 4 of them per iteration (4 × 8 = 32 lanes),
    // grouped into two sub-blocks. Each sub-block uses two
    // accumulators (acc0/acc1 in block A and acc2/acc3 in block B),
    // preserving 4-way ILP across iterations.
    let chunks = n / 32;
    for k in 0..chunks {
        let off = k * 32;

        // SAFETY: each `_x4` load reads 8 consecutive i64s; we issue
        // four of them at off, off+8, off+16, off+24, so the highest
        // accessed index is `off + 31 < n` because k < chunks = n/32.
        unsafe {
            // Sub-block A: lanes 0..16 (two `_x4` quad-loads).
            let xa = vld1q_s64_x4(x_ptr.add(off));
            let ya = vld1q_s64_x4(y_ptr.add(off));
            let xb = vld1q_s64_x4(x_ptr.add(off + 8));
            let yb = vld1q_s64_x4(y_ptr.add(off + 8));

            let ma0 = vreinterpretq_s64_u64(vcgtq_s64(ya.0, zero));
            let ma1 = vreinterpretq_s64_u64(vcgtq_s64(ya.1, zero));
            let ma2 = vreinterpretq_s64_u64(vcgtq_s64(ya.2, zero));
            let ma3 = vreinterpretq_s64_u64(vcgtq_s64(ya.3, zero));
            let mb0 = vreinterpretq_s64_u64(vcgtq_s64(yb.0, zero));
            let mb1 = vreinterpretq_s64_u64(vcgtq_s64(yb.1, zero));
            let mb2 = vreinterpretq_s64_u64(vcgtq_s64(yb.2, zero));
            let mb3 = vreinterpretq_s64_u64(vcgtq_s64(yb.3, zero));

            acc0 = vaddq_s64(acc0, vandq_s64(xa.0, ma0));
            acc1 = vaddq_s64(acc1, vandq_s64(xa.1, ma1));
            acc2 = vaddq_s64(acc2, vandq_s64(xa.2, ma2));
            acc3 = vaddq_s64(acc3, vandq_s64(xa.3, ma3));
            acc0 = vaddq_s64(acc0, vandq_s64(xb.0, mb0));
            acc1 = vaddq_s64(acc1, vandq_s64(xb.1, mb1));
            acc2 = vaddq_s64(acc2, vandq_s64(xb.2, mb2));
            acc3 = vaddq_s64(acc3, vandq_s64(xb.3, mb3));

            // Sub-block B: lanes 16..32 (two more `_x4` quad-loads).
            let xc = vld1q_s64_x4(x_ptr.add(off + 16));
            let yc = vld1q_s64_x4(y_ptr.add(off + 16));
            let xd = vld1q_s64_x4(x_ptr.add(off + 24));
            let yd = vld1q_s64_x4(y_ptr.add(off + 24));

            let mc0 = vreinterpretq_s64_u64(vcgtq_s64(yc.0, zero));
            let mc1 = vreinterpretq_s64_u64(vcgtq_s64(yc.1, zero));
            let mc2 = vreinterpretq_s64_u64(vcgtq_s64(yc.2, zero));
            let mc3 = vreinterpretq_s64_u64(vcgtq_s64(yc.3, zero));
            let md0 = vreinterpretq_s64_u64(vcgtq_s64(yd.0, zero));
            let md1 = vreinterpretq_s64_u64(vcgtq_s64(yd.1, zero));
            let md2 = vreinterpretq_s64_u64(vcgtq_s64(yd.2, zero));
            let md3 = vreinterpretq_s64_u64(vcgtq_s64(yd.3, zero));

            acc0 = vaddq_s64(acc0, vandq_s64(xc.0, mc0));
            acc1 = vaddq_s64(acc1, vandq_s64(xc.1, mc1));
            acc2 = vaddq_s64(acc2, vandq_s64(xc.2, mc2));
            acc3 = vaddq_s64(acc3, vandq_s64(xc.3, mc3));
            acc0 = vaddq_s64(acc0, vandq_s64(xd.0, md0));
            acc1 = vaddq_s64(acc1, vandq_s64(xd.1, md1));
            acc2 = vaddq_s64(acc2, vandq_s64(xd.2, md2));
            acc3 = vaddq_s64(acc3, vandq_s64(xd.3, md3));
        }
    }

    // Reduce 4 × 2-lane → scalar. `vaddq_s64` and `vaddvq_s64`
    // are register-only and safe under the active `neon` feature.
    let lo = vaddq_s64(acc0, acc1);
    let hi = vaddq_s64(acc2, acc3);
    let acc = vaddq_s64(lo, hi);
    let mut total: i64 = vaddvq_s64(acc);

    // Tail: < 32 lanes via the branchless scalar path.
    let tail_start = chunks * 32;
    if tail_start < n {
        total = total.wrapping_add(filter_sum_scalar_branchless(
            &xs[tail_start..n],
            &ys[tail_start..n],
        ));
    }

    total
}

// ============================================================================
// x86_64 AVX2 implementation
// ============================================================================

/// AVX2 dense implementation.
///
/// Layout per loop trip:
/// - Load four 256-bit vectors of `x` and four of `y`
///   (16 lanes × `i64`, 128 bytes per stream).
/// - `_mm256_cmpgt_epi64(y, _mm256_setzero_si256())` produces a 0-or-all-ones
///   mask per 64-bit lane.
/// - AND with `x` then accumulate into two independent 256-bit
///   accumulators to enable ILP.
/// - Final horizontal reduce extracts the four lanes and sums them.
///
/// # Safety
///
/// Gated on `target_feature = "avx2"`. All loads are unaligned
/// (`_mm256_loadu_si256`). Pointer arithmetic is bounded by the loop
/// count; the tail goes through the scalar path.
#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[target_feature(enable = "avx2")]
unsafe fn filter_sum_avx2(xs: &[i64], ys: &[i64]) -> i64 {
    use core::arch::x86_64::{
        __m256i, _mm_add_epi64, _mm_cvtsi128_si64, _mm_unpackhi_epi64, _mm256_add_epi64,
        _mm256_and_si256, _mm256_cmpgt_epi64, _mm256_extracti128_si256, _mm256_loadu_si256,
        _mm256_setzero_si256,
    };

    let n = xs.len().min(ys.len());

    let mut acc0: __m256i = unsafe { _mm256_setzero_si256() };
    let mut acc1: __m256i = unsafe { _mm256_setzero_si256() };
    let zero: __m256i = unsafe { _mm256_setzero_si256() };

    let x_ptr = xs.as_ptr().cast::<__m256i>();
    let y_ptr = ys.as_ptr().cast::<__m256i>();

    // 16 i64s per iteration → 4 vectors of 4 i64s each.
    let chunks = n / 16;
    for k in 0..chunks {
        let off = (k * 4) as isize;
        // SAFETY: each iteration reads 4 × 4 = 16 lanes; bounded by
        // chunks = n / 16, so we never run past `n`.
        unsafe {
            let xv0 = _mm256_loadu_si256(x_ptr.offset(off));
            let xv1 = _mm256_loadu_si256(x_ptr.offset(off + 1));
            let xv2 = _mm256_loadu_si256(x_ptr.offset(off + 2));
            let xv3 = _mm256_loadu_si256(x_ptr.offset(off + 3));

            let yv0 = _mm256_loadu_si256(y_ptr.offset(off));
            let yv1 = _mm256_loadu_si256(y_ptr.offset(off + 1));
            let yv2 = _mm256_loadu_si256(y_ptr.offset(off + 2));
            let yv3 = _mm256_loadu_si256(y_ptr.offset(off + 3));

            let m0 = _mm256_cmpgt_epi64(yv0, zero);
            let m1 = _mm256_cmpgt_epi64(yv1, zero);
            let m2 = _mm256_cmpgt_epi64(yv2, zero);
            let m3 = _mm256_cmpgt_epi64(yv3, zero);

            acc0 = _mm256_add_epi64(acc0, _mm256_and_si256(xv0, m0));
            acc1 = _mm256_add_epi64(acc1, _mm256_and_si256(xv1, m1));
            acc0 = _mm256_add_epi64(acc0, _mm256_and_si256(xv2, m2));
            acc1 = _mm256_add_epi64(acc1, _mm256_and_si256(xv3, m3));
        }
    }

    // Horizontal reduce: combine the two accumulators, then sum the
    // four 64-bit lanes.
    let acc = unsafe { _mm256_add_epi64(acc0, acc1) };
    let lo = unsafe { _mm256_extracti128_si256(acc, 0) };
    let hi = unsafe { _mm256_extracti128_si256(acc, 1) };
    let half = unsafe { _mm_add_epi64(lo, hi) };
    let half_hi = unsafe { _mm_unpackhi_epi64(half, half) };
    let pair = unsafe { _mm_add_epi64(half, half_hi) };
    let mut total: i64 = unsafe { _mm_cvtsi128_si64(pair) };

    let tail_start = chunks * 16;
    if tail_start < n {
        total = total.wrapping_add(filter_sum_scalar_branchless(
            &xs[tail_start..n],
            &ys[tail_start..n],
        ));
    }

    total
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bitmap::Bitmap;
    use crate::column::NumericColumn;

    fn naive_filter_sum(xs: &[i64], ys: &[i64]) -> i64 {
        let mut s: i64 = 0;
        for (xv, yv) in xs.iter().zip(ys.iter()) {
            if *yv > 0 {
                s = s.wrapping_add(*xv);
            }
        }
        s
    }

    #[test]
    fn basic_small_input() {
        let x = NumericColumn::from_data(vec![1_i64, 2, 3, 4, 5]);
        let y = NumericColumn::from_data(vec![-1_i64, 2, 0, 4, -5]);
        let got = filter_sum_i64_where_gt_zero(&x, &y);
        // y > 0 at indices 1 and 3 → x[1] + x[3] = 2 + 4 = 6.
        assert_eq!(got, 6);
    }

    #[test]
    fn zero_length_returns_zero() {
        let x = NumericColumn::from_data(Vec::<i64>::new());
        let y = NumericColumn::from_data(Vec::<i64>::new());
        assert_eq!(filter_sum_i64_where_gt_zero(&x, &y), 0);
    }

    #[test]
    fn all_y_non_positive_returns_zero() {
        let x = NumericColumn::from_data(vec![10_i64, 20, 30, 40, 50]);
        let y = NumericColumn::from_data(vec![0_i64, -1, -2, -3, i64::MIN]);
        assert_eq!(filter_sum_i64_where_gt_zero(&x, &y), 0);
    }

    #[test]
    fn all_y_positive_returns_full_sum() {
        let xs: Vec<i64> = (1_i64..=100).collect();
        let ys: Vec<i64> = (1_i64..=100).collect();
        let x = NumericColumn::from_data(xs.clone());
        let y = NumericColumn::from_data(ys);
        let want: i64 = xs.iter().sum();
        assert_eq!(filter_sum_i64_where_gt_zero(&x, &y), want);
    }

    #[test]
    fn y_min_treated_as_non_positive() {
        let x = NumericColumn::from_data(vec![100_i64; 4]);
        let y = NumericColumn::from_data(vec![i64::MIN, i64::MIN, i64::MIN, 1]);
        // Only index 3 contributes.
        assert_eq!(filter_sum_i64_where_gt_zero(&x, &y), 100);
    }

    #[test]
    fn x_max_wraps_safely() {
        // Build x with all i64::MAX where y > 0. Sum must wrap, not panic.
        let xs = vec![i64::MAX; 4];
        let ys = vec![1_i64, 1, 1, 1];
        let x = NumericColumn::from_data(xs);
        let y = NumericColumn::from_data(ys);
        let want = i64::MAX
            .wrapping_add(i64::MAX)
            .wrapping_add(i64::MAX)
            .wrapping_add(i64::MAX);
        assert_eq!(filter_sum_i64_where_gt_zero(&x, &y), want);
    }

    #[test]
    fn tail_sizes_exercised() {
        // Lengths around the NEON 32-lane block boundary.
        for n in [
            0_usize, 1, 7, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 129,
        ] {
            let xs: Vec<i64> = (0..n)
                .map(|i| i64::try_from(i).unwrap_or(0) * 3 - 7)
                .collect();
            let ys: Vec<i64> = (0..n)
                .map(|i| i64::try_from(i).unwrap_or(0) % 5 - 2)
                .collect();
            let x = NumericColumn::from_data(xs.clone());
            let y = NumericColumn::from_data(ys.clone());
            let got = filter_sum_i64_where_gt_zero(&x, &y);
            let want = naive_filter_sum(&xs, &ys);
            assert_eq!(got, want, "n = {n}");
        }
    }

    #[test]
    fn matches_scalar_branchless_on_random_input() {
        // Deterministic LCG-style scramble. Cover a length well past the
        // 16-lane unrolled hot region.
        let n: usize = 10_000;
        let mut s: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut xs = Vec::with_capacity(n);
        let mut ys = Vec::with_capacity(n);
        for _ in 0..n {
            s = s
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            xs.push(i64::from_ne_bytes(s.to_ne_bytes()) >> 32);
            s = s
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            ys.push(i64::from_ne_bytes(s.to_ne_bytes()) >> 32);
        }
        let x = NumericColumn::from_data(xs.clone());
        let y = NumericColumn::from_data(ys.clone());
        let got = filter_sum_i64_where_gt_zero(&x, &y);
        let want = naive_filter_sum(&xs, &ys);
        let scalar = filter_sum_i64_where_gt_zero_scalar(&x, &y);
        assert_eq!(got, want, "SIMD result must match naive reference");
        assert_eq!(scalar, want, "scalar result must match naive reference");
    }

    #[test]
    fn validity_path_basic() {
        let xs = vec![10_i64, 20, 30, 40, 50];
        let ys = vec![1_i64, -1, 1, 1, 1];
        let mut x_nulls = Bitmap::new(5, true);
        x_nulls.set(2, false); // null x at row 2 → drop x[2]
        let mut y_nulls = Bitmap::new(5, true);
        y_nulls.set(3, false); // null y at row 3 → predicate UNKNOWN, drop x[3]
        let x = NumericColumn::with_nulls(xs, x_nulls).unwrap();
        let y = NumericColumn::with_nulls(ys, y_nulls).unwrap();
        // Valid rows: 0, 4 (row 1 fails predicate; rows 2/3 nulls).
        // x[0] + x[4] = 10 + 50 = 60.
        assert_eq!(filter_sum_i64_where_gt_zero_with_validity(&x, &y), 60);
    }

    #[test]
    fn validity_no_nulls_matches_dense() {
        let xs: Vec<i64> = (0..100).collect();
        let ys: Vec<i64> = (0..100).map(|i| if i % 3 == 0 { 1 } else { -1 }).collect();
        let x = NumericColumn::from_data(xs);
        let y = NumericColumn::from_data(ys);
        assert_eq!(
            filter_sum_i64_where_gt_zero(&x, &y),
            filter_sum_i64_where_gt_zero_with_validity(&x, &y),
        );
    }

    fn build_with_nulls(data: Vec<i64>, mask: &[bool]) -> NumericColumn<i64> {
        let mut bm = Bitmap::new(data.len(), false);
        for (i, &v) in mask.iter().enumerate() {
            if v {
                bm.set(i, true);
            }
        }
        NumericColumn::with_nulls(data, bm).unwrap()
    }

    #[test]
    fn validity_only_one_side_nullable() {
        let xs = vec![5_i64, 10, 15, 20];
        let ys = vec![1_i64, 1, -1, 1];
        let x = NumericColumn::from_data(xs);
        let y_mask = vec![true, false, true, true];
        let y = build_with_nulls(ys, &y_mask);
        // Row 0: y valid, y>0 ✓ → +5
        // Row 1: y null → skip
        // Row 2: y valid, y<=0 → skip
        // Row 3: y valid, y>0 ✓ → +20
        assert_eq!(filter_sum_i64_where_gt_zero_with_validity(&x, &y), 25);
    }

    proptest::proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig {
            cases: 128,
            .. proptest::prelude::ProptestConfig::default()
        })]

        #[test]
        fn prop_filter_sum_matches_naive(
            rows in proptest::collection::vec(
                (proptest::prelude::any::<i64>(), proptest::prelude::any::<i64>()),
                0_usize..=4096,
            )
        ) {
            let xs: Vec<i64> = rows.iter().map(|(a, _)| *a).collect();
            let ys: Vec<i64> = rows.iter().map(|(_, b)| *b).collect();
            let x = NumericColumn::from_data(xs.clone());
            let y = NumericColumn::from_data(ys.clone());
            let got = filter_sum_i64_where_gt_zero(&x, &y);
            let scalar = filter_sum_i64_where_gt_zero_scalar(&x, &y);
            let naive = naive_filter_sum(&xs, &ys);
            proptest::prop_assert_eq!(got, naive);
            proptest::prop_assert_eq!(scalar, naive);
        }

        #[test]
        fn prop_filter_sum_with_validity_matches_naive(
            rows in proptest::collection::vec(
                (
                    proptest::prelude::any::<i64>(),
                    proptest::prelude::any::<i64>(),
                    proptest::prelude::any::<bool>(),
                    proptest::prelude::any::<bool>(),
                ),
                0_usize..=512,
            )
        ) {
            let n = rows.len();
            let xs: Vec<i64> = rows.iter().map(|t| t.0).collect();
            let ys: Vec<i64> = rows.iter().map(|t| t.1).collect();
            let xn: Vec<bool> = rows.iter().map(|t| t.2).collect();
            let yn: Vec<bool> = rows.iter().map(|t| t.3).collect();
            let x = if n == 0 {
                NumericColumn::from_data(xs.clone())
            } else {
                build_with_nulls(xs.clone(), &xn)
            };
            let y = if n == 0 {
                NumericColumn::from_data(ys.clone())
            } else {
                build_with_nulls(ys.clone(), &yn)
            };
            let got = filter_sum_i64_where_gt_zero_with_validity(&x, &y);
            // Naive validity-aware reference.
            let mut want: i64 = 0;
            for i in 0..n {
                if xn[i] && yn[i] && ys[i] > 0 {
                    want = want.wrapping_add(xs[i]);
                }
            }
            proptest::prop_assert_eq!(got, want);
        }
    }
}
