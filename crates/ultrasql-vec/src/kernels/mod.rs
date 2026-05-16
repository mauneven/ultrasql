//! Vectorized kernels.
//!
//! Each kernel here has a scalar (auto-vectorizable) implementation
//! that is the source of truth. SIMD specializations land alongside
//! the scalar versions and are validated bit-for-bit against scalar
//! in property tests.
//!
//! Sub-modules
//! -----------
//! - [`filter`]     — filter kernels: `filter_eq_i32/i64`, `filter_lt/gt_i32`, `filter_eq_f64`.
//! - [`filter_sum`] — fused branchless filter+sum kernels.
//! - [`arithmetic`] — arithmetic kernels: `add/sub/mul/compare_*` for `i32`,
//!   `i64`, `f32`, `f64`; column-vs-literal `*_scalar_lit` variants for the
//!   same shapes; unary negation `neg_*` and boolean `not_bool`.
//! - [`text`]       — text kernels: `len_text`, `lower_text`, `upper_text`.
//! - [`hash`]       — hash kernels: `hash_i64`, `hash_text_bytes` (FNV-1a).

pub mod arithmetic;
pub mod filter;
pub mod filter_sum;
pub mod hash;
pub mod text;

pub use arithmetic::{
    add_f32, add_f32_scalar, add_f32_scalar_lit, add_f32_scalar_lit_scalar, add_f64,
    add_f64_scalar, add_f64_scalar_lit, add_f64_scalar_lit_scalar, add_i32, add_i32_scalar,
    add_i32_scalar_lit, add_i32_scalar_lit_scalar, add_i64, add_i64_scalar, add_i64_scalar_lit,
    add_i64_scalar_lit_scalar, compare_f32, compare_f32_scalar, compare_f32_scalar_lit,
    compare_f32_scalar_lit_scalar, compare_f64, compare_f64_scalar, compare_f64_scalar_lit,
    compare_f64_scalar_lit_scalar, compare_i32, compare_i32_scalar, compare_i32_scalar_lit,
    compare_i32_scalar_lit_scalar, compare_i64, compare_i64_scalar, compare_i64_scalar_lit,
    compare_i64_scalar_lit_scalar, mul_f32, mul_f32_scalar, mul_f32_scalar_lit,
    mul_f32_scalar_lit_scalar, mul_f64, mul_f64_scalar, mul_f64_scalar_lit,
    mul_f64_scalar_lit_scalar, mul_i32, mul_i32_scalar, mul_i32_scalar_lit,
    mul_i32_scalar_lit_scalar, mul_i64, mul_i64_scalar, mul_i64_scalar_lit,
    mul_i64_scalar_lit_scalar, neg_f32, neg_f32_scalar, neg_f64, neg_f64_scalar, neg_i32,
    neg_i32_scalar, neg_i64, neg_i64_scalar, not_bool, not_bool_scalar, sub_f32, sub_f32_scalar,
    sub_f32_scalar_lit, sub_f32_scalar_lit_scalar, sub_f64, sub_f64_scalar, sub_f64_scalar_lit,
    sub_f64_scalar_lit_scalar, sub_i32, sub_i32_scalar, sub_i32_scalar_lit,
    sub_i32_scalar_lit_scalar, sub_i64, sub_i64_scalar, sub_i64_scalar_lit,
    sub_i64_scalar_lit_scalar,
};
pub use filter::{
    filter_eq_f64, filter_eq_f64_scalar, filter_eq_i32, filter_eq_i32_scalar, filter_eq_i64,
    filter_eq_i64_scalar, filter_gt_i32, filter_gt_i32_scalar, filter_lt_i32, filter_lt_i32_scalar,
};
pub use filter_sum::{
    filter_sum_i64_where_gt_zero, filter_sum_i64_where_gt_zero_scalar,
    filter_sum_i64_where_gt_zero_with_validity, filter_sum_par_auto_i64_where_gt_zero,
    filter_sum_par_i64_where_gt_zero,
};
pub use hash::{hash_i64, hash_i64_scalar, hash_text_bytes, hash_text_bytes_scalar};
pub use text::{
    len_text, len_text_scalar, lower_text, lower_text_scalar, upper_text, upper_text_scalar,
};

use crate::bitmap::Bitmap;
use crate::column::NumericColumn;

/// Element-wise `a == b` over two `i32` columns of equal length.
///
/// The output is a `Bitmap` of length `n` where bit `i` is set iff
/// `a[i] == b[i]`. NULLs (if any) produce a 0 bit. SQL NULL semantics
/// say a comparison with NULL is UNKNOWN, treated as false here for
/// the filter context — a separate three-valued logic kernel will
/// arrive for general WHERE evaluation.
///
/// # Implementation notes
///
/// The non-null fast path processes 64 lanes at a time, collects 64
/// boolean compare results into a packed `u64` mask, and writes the
/// mask word directly into the [`Bitmap`]'s backing buffer. This
/// removes the per-row read-modify-write that the previous
/// [`Bitmap::set`]-driven loop imposed and gives LLVM a shape that
/// autovectorizes to NEON `cmeq` on Apple M-series (and `vpcmpeqd`
/// on `x86_64-v3`). The disassembly was inspected on `apple-m1` and
/// shows a tight NEON compare loop with no scalar fallback in the
/// hot region.
///
/// The null-aware path mirrors the structure: it builds the data
/// compare mask the same way, then AND-folds the two validity words
/// into the result before storing. The slow per-bit `m.get(i)` calls
/// from the previous implementation are gone.
///
/// # Panics
///
/// Panics if the two columns disagree on length. The caller is
/// responsible for upstream length validation.
#[must_use]
pub fn eq_i32(a: &NumericColumn<i32>, b: &NumericColumn<i32>) -> Bitmap {
    assert_eq!(a.len(), b.len(), "eq_i32: column length mismatch");
    let n = a.len();
    let xa = a.data();
    let xb = b.data();

    let mut words = vec![0_u64; n.div_ceil(64)];

    eq_i32_pack_into(xa, xb, &mut words);

    if let Some(na) = a.nulls() {
        for (w, &v) in words.iter_mut().zip(na.words().iter()) {
            *w &= v;
        }
    }
    if let Some(nb) = b.nulls() {
        for (w, &v) in words.iter_mut().zip(nb.words().iter()) {
            *w &= v;
        }
    }

    Bitmap::from_words(words, n)
}

/// Pack 64-lane `a == b` compare results into the destination word
/// buffer. Caller guarantees `words.len() >= a.len().div_ceil(64)`.
///
/// The inner block builds the 64-bit mask via 8 disjoint 8-lane
/// chunks; LLVM autovectorizes the 8-wide compare to NEON `cmeq` on
/// aarch64 (or `vpcmpeqd` on AVX2). The bit-shift accumulation
/// reduces 8 boolean lanes per chunk into a single byte of the
/// output word, matching Arrow / [`Bitmap`] little-endian bit order.
///
/// The body is shaped around fixed-size `[i32; 64]` chunks so LLVM
/// can drop the per-lane bounds checks; the disassembly under
/// `target-cpu=apple-m1` shows the NEON `cmeq.4s` compare lane plus
/// a `tbl`-driven bit-pack with no remaining scalar fallback.
#[inline]
fn eq_i32_pack_into(a: &[i32], b: &[i32], words: &mut [u64]) {
    debug_assert_eq!(a.len(), b.len());
    debug_assert!(words.len() >= a.len().div_ceil(64));

    let mut chunks_a = a.chunks_exact(64);
    let mut chunks_b = b.chunks_exact(64);
    let full_words = chunks_a.len();

    for (out_word, (ca, cb)) in words.iter_mut().zip((&mut chunks_a).zip(&mut chunks_b)) {
        // Fixed-size views give LLVM enough info to drop bounds
        // checks across the eight 8-lane sub-compares below.
        let ca: &[i32; 64] = ca
            .try_into()
            .expect("chunks_exact(64) yields 64-element slices");
        let cb: &[i32; 64] = cb
            .try_into()
            .expect("chunks_exact(64) yields 64-element slices");
        *out_word = pack_eq_64(ca, cb);
    }

    // Trailing partial word, up to 63 lanes.
    let rest_a = chunks_a.remainder();
    let rest_b = chunks_b.remainder();
    if !rest_a.is_empty() {
        let mut mask: u64 = 0;
        for (j, (&av, &bv)) in rest_a.iter().zip(rest_b.iter()).enumerate() {
            mask |= u64::from(av == bv) << j;
        }
        words[full_words] = mask;
    }
}

/// Compare 64 `i32` lanes and produce a packed 64-bit mask.
///
/// On `aarch64` we use NEON intrinsics: eight `vceqq_s32` produce
/// eight 4-lane all-ones-or-zero compare vectors, which we reduce to
/// a single 64-bit mask using `vshrn` / bit-mask-and-add tricks
/// (Wojciech Muła's "movemask emulation"). On every other target we
/// fall back to a scalar loop that LLVM autovectorizes acceptably
/// (still much faster than the prior per-row `set()` shape because
/// the destination is a single word write).
#[cfg(target_arch = "aarch64")]
#[inline]
fn pack_eq_64(a: &[i32; 64], b: &[i32; 64]) -> u64 {
    // SAFETY: aarch64 NEON is unconditionally available on every
    // ARMv8-A CPU, which is the floor of `aarch64-apple-darwin` and
    // `aarch64-unknown-linux-gnu`. The pointer arithmetic stays
    // inside the borrowed `[i32; 64]` arrays.
    unsafe { pack_eq_64_neon(a, b) }
}

#[cfg(not(target_arch = "aarch64"))]
#[inline]
fn pack_eq_64(a: &[i32; 64], b: &[i32; 64]) -> u64 {
    let mut mask: u64 = 0;
    // 8 chunks × 8 lanes per chunk = 64 lanes / word.
    for chunk in 0..8_usize {
        let off = chunk * 8;
        let mut byte: u64 = 0;
        byte |= u64::from(a[off] == b[off]);
        byte |= u64::from(a[off + 1] == b[off + 1]) << 1;
        byte |= u64::from(a[off + 2] == b[off + 2]) << 2;
        byte |= u64::from(a[off + 3] == b[off + 3]) << 3;
        byte |= u64::from(a[off + 4] == b[off + 4]) << 4;
        byte |= u64::from(a[off + 5] == b[off + 5]) << 5;
        byte |= u64::from(a[off + 6] == b[off + 6]) << 6;
        byte |= u64::from(a[off + 7] == b[off + 7]) << 7;
        mask |= byte << (chunk * 8);
    }
    mask
}

/// NEON specialization of [`pack_eq_64`].
///
/// Strategy: load `a` and `b` as 4-lane `int32x4_t` vectors, compare
/// them with `vceqq_s32` to get 4-lane "all-ones per matching lane"
/// results, then collapse each vector to a 4-bit nibble of the
/// destination word.
///
/// We use the "and with a powers-of-two vector, then horizontal
/// add" emulation: each compare lane is 0 or `0xFFFF_FFFF`,
/// AND-ing against `[1, 2, 4, 8]` keeps only the matching weight,
/// and `vaddvq_u32` reduces the 4 lanes to a single value in
/// 0..=15 — exactly the desired 4-bit deposit. Eight such
/// reductions (sixteen NEON loads, eight compares, eight
/// reductions) emit one 64-bit mask word.
///
/// # Safety
///
/// The caller passes `&[i32; 64]` references; we read exactly 64
/// `i32`s starting at each pointer. `vld1q_s32` requires only a
/// 4-byte alignment, which is the minimum for `i32`.
#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn pack_eq_64_neon(a: &[i32; 64], b: &[i32; 64]) -> u64 {
    use core::arch::aarch64::{uint32x4_t, vaddvq_u32, vandq_u32, vceqq_s32, vld1q_s32, vld1q_u32};

    let a_ptr = a.as_ptr();
    let b_ptr = b.as_ptr();

    // Powers-of-two weights for the 4 lanes of each compare vector:
    // bit positions 0..3 within the chunk byte. SAFETY: 16-byte
    // aligned constant stack memory.
    let w_lo: [u32; 4] = [1, 2, 4, 8];

    // SAFETY: pointer is to a 16-byte-aligned constant array.
    let weights: uint32x4_t = unsafe { vld1q_u32(w_lo.as_ptr()) };

    let mut mask: u64 = 0;
    // We process 8 lanes per "pair" iteration → 8 pair iterations
    // would be needed to cover 64 lanes, but the loop deposits 8
    // lanes per byte and steps the byte position by 8 bits per
    // iteration. (Lanes 0..7 → byte 0, lanes 8..15 → byte 1, …)
    for pair in 0..8_usize {
        let off = pair * 8;
        // SAFETY: a_ptr.add(off) is in-bounds because off + 7 <= 63
        // (pair <= 7 → off <= 56, and we read up to off + 7).
        let av_lo = unsafe { vld1q_s32(a_ptr.add(off)) };
        let bv_lo = unsafe { vld1q_s32(b_ptr.add(off)) };
        let av_hi = unsafe { vld1q_s32(a_ptr.add(off + 4)) };
        let bv_hi = unsafe { vld1q_s32(b_ptr.add(off + 4)) };

        // Per-lane compare → 0xFFFFFFFF on match, 0 on miss.
        let eq_lo = vceqq_s32(av_lo, bv_lo);
        let eq_hi = vceqq_s32(av_hi, bv_hi);

        // AND with [1, 2, 4, 8] then horizontal-add → a value in
        // 0..=15 encoding which of the 4 lanes matched.
        let nib_lo = u64::from(vaddvq_u32(vandq_u32(eq_lo, weights)));
        let nib_hi = u64::from(vaddvq_u32(vandq_u32(eq_hi, weights)));
        // Combine into one byte: low nibble = lanes 0..3, high
        // nibble = lanes 4..7 (matching the scalar path bit order).
        let byte = nib_lo | (nib_hi << 4);
        mask |= byte << (pair * 8);
    }
    mask
}

/// Scalar reference implementation of [`eq_i32`].
///
/// Kept as the source-of-truth correctness oracle: property tests
/// compare it against the production kernel over randomly generated
/// inputs. The two must agree on every bit.
///
/// # Panics
///
/// Panics if the two columns disagree on length.
#[must_use]
pub fn eq_i32_scalar(a: &NumericColumn<i32>, b: &NumericColumn<i32>) -> Bitmap {
    assert_eq!(a.len(), b.len(), "eq_i32_scalar: column length mismatch");
    let n = a.len();
    let mut out = Bitmap::new(n, false);
    let (xa, xb) = (a.data(), b.data());

    for i in 0..n {
        let matched = xa[i] == xb[i];
        let nulls_ok = a.nulls().is_none_or(|m| m.get(i)) && b.nulls().is_none_or(|m| m.get(i));
        if matched && nulls_ok {
            out.set(i, true);
        }
    }
    out
}

/// Sum of a non-null `i64` column. NULL entries are skipped.
#[must_use]
pub fn sum_i64(column: &NumericColumn<i64>) -> i64 {
    column.nulls().map_or_else(
        || sum_i64_dense(column.data()),
        |nulls| {
            let mut s: i64 = 0;
            for (i, v) in column.data().iter().enumerate() {
                if nulls.get(i) {
                    s = s.wrapping_add(*v);
                }
            }
            s
        },
    )
}

/// Dense (non-null) sum of an `i64` slice. Hand-vectorised on aarch64;
/// scalar fallback on every other target.
#[inline]
fn sum_i64_dense(data: &[i64]) -> i64 {
    #[cfg(target_arch = "aarch64")]
    {
        sum_i64_dense_neon(data)
    }
    #[cfg(not(target_arch = "aarch64"))]
    {
        data.iter().fold(0_i64, |a, b| a.wrapping_add(*b))
    }
}

/// Hand-rolled aarch64 NEON kernel for `sum(i64 slice)`.
///
/// Processes 16 `i64` lanes per loop iteration through four parallel
/// `int64x2_t` accumulators (8 lanes × 2 i64 / vec) so the CPU can
/// dual-issue the dependent `vaddq_s64` chains. Tail of fewer than 16
/// lanes folds through the standard scalar `wrapping_add` loop.
///
/// Bit-identical to the scalar fold under `i64::wrapping_add`. The
/// `unsafe` block is sound because every `vld1q_s64` reads exactly two
/// `i64` lanes from a slice we have indexed under bounds; no aliasing,
/// no over-read.
#[cfg(target_arch = "aarch64")]
#[inline]
fn sum_i64_dense_neon(data: &[i64]) -> i64 {
    use std::arch::aarch64::{int64x2_t, vaddq_s64, vaddvq_s64, vdupq_n_s64, vld1q_s64};

    let mut a0: int64x2_t = unsafe { vdupq_n_s64(0) };
    let mut a1: int64x2_t = unsafe { vdupq_n_s64(0) };
    let mut a2: int64x2_t = unsafe { vdupq_n_s64(0) };
    let mut a3: int64x2_t = unsafe { vdupq_n_s64(0) };

    let chunks = data.chunks_exact(8);
    let rem = chunks.remainder();
    for c in chunks {
        // SAFETY: `c` is `&[i64; 8]` (chunks_exact); each `vld1q_s64`
        // reads 2 contiguous `i64` lanes within the chunk. No aliasing
        // — `c` is a unique borrow into `data`. The pointer arithmetic
        // stays inside `c.len() == 8`.
        unsafe {
            let v0 = vld1q_s64(c.as_ptr());
            let v1 = vld1q_s64(c.as_ptr().add(2));
            let v2 = vld1q_s64(c.as_ptr().add(4));
            let v3 = vld1q_s64(c.as_ptr().add(6));
            a0 = vaddq_s64(a0, v0);
            a1 = vaddq_s64(a1, v1);
            a2 = vaddq_s64(a2, v2);
            a3 = vaddq_s64(a3, v3);
        }
    }
    // Horizontal reduction.
    let mut sum = unsafe {
        let half0 = vaddq_s64(a0, a1);
        let half1 = vaddq_s64(a2, a3);
        let total = vaddq_s64(half0, half1);
        vaddvq_s64(total)
    };
    for &v in rem {
        sum = sum.wrapping_add(v);
    }
    sum
}

/// Sum of a non-null `i32` column widened to `i64` (the integer-add
/// semantics every SQL `SUM(INT)` uses). NULL entries are skipped.
///
/// Hand-NEON-vectorised on aarch64; scalar fallback elsewhere.
#[must_use]
pub fn sum_i32_widening(column: &NumericColumn<i32>) -> i64 {
    column.nulls().map_or_else(
        || sum_i32_widening_dense(column.data()),
        |nulls| {
            let mut s: i64 = 0;
            for (i, v) in column.data().iter().enumerate() {
                if nulls.get(i) {
                    s = s.wrapping_add(i64::from(*v));
                }
            }
            s
        },
    )
}

#[inline]
fn sum_i32_widening_dense(data: &[i32]) -> i64 {
    #[cfg(target_arch = "aarch64")]
    {
        sum_i32_widening_dense_neon(data)
    }
    #[cfg(not(target_arch = "aarch64"))]
    {
        data.iter()
            .fold(0_i64, |a, b| a.wrapping_add(i64::from(*b)))
    }
}

/// Hand-rolled aarch64 NEON kernel for `sum(i32 slice)` widened to
/// `i64`. Processes 16 `i32` lanes per iteration via `vpaddlq_s32`
/// (pairwise add-and-widen, 4 i32 → 2 i64) into four parallel
/// `int64x2_t` accumulators.
///
/// Equivalent to `data.iter().fold(0, |a, b| a.wrapping_add(i64::from(b)))`.
#[cfg(target_arch = "aarch64")]
#[inline]
fn sum_i32_widening_dense_neon(data: &[i32]) -> i64 {
    use std::arch::aarch64::{
        int64x2_t, vaddq_s64, vaddvq_s64, vdupq_n_s64, vld1q_s32, vpaddlq_s32,
    };

    let mut a0: int64x2_t = unsafe { vdupq_n_s64(0) };
    let mut a1: int64x2_t = unsafe { vdupq_n_s64(0) };
    let mut a2: int64x2_t = unsafe { vdupq_n_s64(0) };
    let mut a3: int64x2_t = unsafe { vdupq_n_s64(0) };

    let chunks = data.chunks_exact(16);
    let rem = chunks.remainder();
    for c in chunks {
        // SAFETY: `c` is `&[i32; 16]`; each `vld1q_s32` reads 4
        // contiguous i32 lanes inside the chunk. The 4 `vpaddlq_s32`
        // calls then pairwise-widen 4 i32 → 2 i64. Final accumulators
        // are `wrapping_add` over the widened i64 stream.
        unsafe {
            let v0 = vld1q_s32(c.as_ptr());
            let v1 = vld1q_s32(c.as_ptr().add(4));
            let v2 = vld1q_s32(c.as_ptr().add(8));
            let v3 = vld1q_s32(c.as_ptr().add(12));
            a0 = vaddq_s64(a0, vpaddlq_s32(v0));
            a1 = vaddq_s64(a1, vpaddlq_s32(v1));
            a2 = vaddq_s64(a2, vpaddlq_s32(v2));
            a3 = vaddq_s64(a3, vpaddlq_s32(v3));
        }
    }
    // Horizontal reduction of the four parallel accumulators.
    let mut sum = unsafe {
        let half0 = vaddq_s64(a0, a1);
        let half1 = vaddq_s64(a2, a3);
        let total = vaddq_s64(half0, half1);
        vaddvq_s64(total)
    };
    for &v in rem {
        sum = sum.wrapping_add(i64::from(v));
    }
    sum
}

/// Min of a non-null `f64` column. Returns `None` on empty / all-null
/// input. Honors IEEE-754 semantics for NaN: NaN values are skipped
/// (Rust's [`f64::min`] returns the non-NaN argument when one is NaN).
///
/// # Implementation notes
///
/// The non-null fast path keeps four parallel accumulators so LLVM
/// can schedule four independent `fmin` chains, which Apple M-series
/// can dual-issue. The accumulators are seeded with
/// [`f64::INFINITY`]; combining with `f64::min` is branch-free and
/// preserves NaN-skip semantics: `min(INF, x) = x` for any non-NaN
/// `x`, and `min(acc, NaN) = acc`. The result is `None` iff the
/// folded `best` is still `INFINITY` AND the input had no `+INF` and
/// no non-NaN value — distinguished by tracking `saw_value`.
///
/// The disassembly (`apple-m1`) shows a tight NEON `fminnm.2d` loop
/// with four-deep accumulator unrolling. Autovectorization is
/// sufficient here, so no hand intrinsics are used.
///
/// The null-aware path is a separate `min_f64_nullable` kernel; the
/// non-null fast path stays branch-free.
#[must_use]
pub fn min_f64(column: &NumericColumn<f64>) -> Option<f64> {
    if let Some(nulls) = column.nulls() {
        return min_f64_nullable(column.data(), nulls);
    }
    min_f64_dense(column.data())
}

/// Dense (non-null) min of an `f64` slice. NaN values are skipped.
/// Returns `None` on empty input.
#[inline]
fn min_f64_dense(data: &[f64]) -> Option<f64> {
    if data.is_empty() {
        return None;
    }

    // Four-wide unrolled fold. Seeding with `INFINITY` is safe for
    // the IEEE NaN-skip semantics that `f64::min` provides: NaN lanes
    // pass through unchanged. We track `saw_value` separately so a
    // column of all NaNs returns `None` instead of `INFINITY`.
    let mut a0 = f64::INFINITY;
    let mut a1 = f64::INFINITY;
    let mut a2 = f64::INFINITY;
    let mut a3 = f64::INFINITY;
    let mut saw_value = false;

    let chunks = data.chunks_exact(4);
    let rem = chunks.remainder();
    for c in chunks {
        let arr: &[f64; 4] = c.try_into().expect("chunks_exact(4) yields 4 elements");
        // `is_nan() ^ true` is `!is_nan()`. We OR all the
        // non-NaN flags into `saw_value` to know if the column has
        // any usable value at the end.
        saw_value |= !arr[0].is_nan() | !arr[1].is_nan() | !arr[2].is_nan() | !arr[3].is_nan();
        a0 = a0.min(arr[0]);
        a1 = a1.min(arr[1]);
        a2 = a2.min(arr[2]);
        a3 = a3.min(arr[3]);
    }
    for &v in rem {
        saw_value |= !v.is_nan();
        a0 = a0.min(v);
    }

    if !saw_value {
        return None;
    }
    Some(a0.min(a1).min(a2.min(a3)))
}

/// Null-aware min of an `f64` slice. NULL entries (validity = 0) and
/// NaN values are both skipped. Returns `None` if no valid non-NaN
/// value remains.
///
/// The null bitmap path is unavoidably branched per row — we cannot
/// pretend a null slot is `INFINITY` because the value at that
/// position is arbitrary garbage. The branch is on the bitmap word,
/// not the row, which keeps it cheap.
#[inline]
fn min_f64_nullable(data: &[f64], nulls: &Bitmap) -> Option<f64> {
    let mut best: Option<f64> = None;
    let words = nulls.words();
    for (word_idx, &word) in words.iter().enumerate() {
        if word == 0 {
            continue;
        }
        let base = word_idx * 64;
        // Iterate over set bits using a `word & word-1` clearing trick.
        let mut w = word;
        while w != 0 {
            let bit = w.trailing_zeros() as usize;
            let i = base + bit;
            if i >= data.len() {
                break;
            }
            let v = data[i];
            if !v.is_nan() {
                best = Some(best.map_or(v, |b| b.min(v)));
            }
            w &= w - 1;
        }
    }
    best
}

/// Scalar reference implementation of [`min_f64`].
///
/// Used by property tests to cross-validate the production kernel.
/// Uses [`f64::min`] so signed-zero ordering matches the fast path
/// (Rust's `f64::min` follows IEEE-754 minNum: `min(-0.0, 0.0) =
/// -0.0`). The previous `<`-based reference treated `-0.0 == 0.0`,
/// which silently differed from the SIMD `fminnm` lowering and made
/// proptest flag a non-bug.
#[must_use]
pub fn min_f64_scalar(column: &NumericColumn<f64>) -> Option<f64> {
    let mut best: Option<f64> = None;
    if let Some(nulls) = column.nulls() {
        for (i, &v) in column.data().iter().enumerate() {
            if !nulls.get(i) || v.is_nan() {
                continue;
            }
            best = Some(best.map_or(v, |b| b.min(v)));
        }
    } else {
        for &v in column.data() {
            if v.is_nan() {
                continue;
            }
            best = Some(best.map_or(v, |b| b.min(v)));
        }
    }
    best
}

/// Materialize the rows of `column` selected by `selection`. The
/// length of the output equals `selection.count_ones()`.
#[must_use]
pub fn select_i32(column: &NumericColumn<i32>, selection: &Bitmap) -> NumericColumn<i32> {
    assert_eq!(
        column.len(),
        selection.len(),
        "select_i32: selection length mismatch"
    );
    let take = selection.count_ones();
    let mut out = Vec::with_capacity(take);
    for i in selection.iter_ones() {
        out.push(column.data()[i]);
    }
    NumericColumn::from_data(out)
}

/// Count of non-null entries in an `i64` column.
///
/// For a non-nullable column this is exactly `column.len()`. With a
/// validity bitmap, it is the popcount of the bitmap. Both branches
/// are O(n) but the bitmap branch runs over 64x fewer words.
#[must_use]
pub fn count_i64(column: &NumericColumn<i64>) -> usize {
    column
        .nulls()
        .map_or_else(|| column.len(), Bitmap::count_ones)
}

/// Min of a non-null `i64` column. Returns `None` on empty / all-null
/// input.
///
/// The non-nullable fast path is a single auto-vectorizable fold;
/// LLVM emits NEON `smin` on aarch64 and AVX2 `pminsq` on x86-64-v3.
#[must_use]
pub fn min_i64(column: &NumericColumn<i64>) -> Option<i64> {
    column.nulls().map_or_else(
        || {
            // Seed with the first element and fold; this shape vectorizes.
            let data = column.data();
            let (first, rest) = data.split_first()?;
            let mut best = *first;
            for &v in rest {
                if v < best {
                    best = v;
                }
            }
            Some(best)
        },
        |nulls| {
            let mut best: Option<i64> = None;
            for (i, &v) in column.data().iter().enumerate() {
                if !nulls.get(i) {
                    continue;
                }
                best = Some(best.map_or(v, |b| if v < b { v } else { b }));
            }
            best
        },
    )
}

/// Max of a non-null `i64` column. Returns `None` on empty / all-null
/// input. Same shape as [`min_i64`]; LLVM uses the lane-wise `smax`
/// reduction.
#[must_use]
pub fn max_i64(column: &NumericColumn<i64>) -> Option<i64> {
    column.nulls().map_or_else(
        || {
            let data = column.data();
            let (first, rest) = data.split_first()?;
            let mut best = *first;
            for &v in rest {
                if v > best {
                    best = v;
                }
            }
            Some(best)
        },
        |nulls| {
            let mut best: Option<i64> = None;
            for (i, &v) in column.data().iter().enumerate() {
                if !nulls.get(i) {
                    continue;
                }
                best = Some(best.map_or(v, |b| if v > b { v } else { b }));
            }
            best
        },
    )
}

/// Element-wise `a > scalar` over an `i64` column. The output is a
/// `Bitmap` of length `column.len()` where bit `i` is set iff
/// `a[i] > scalar` AND the row is non-null.
///
/// # Implementation notes
///
/// The non-null fast path processes 64 lanes at a time, collects 64
/// boolean compare results into a packed `u64` mask, and writes the
/// mask word directly into the output buffer. This removes the
/// per-row read-modify-write that the previous [`Bitmap::set`]-driven
/// loop imposed and gives LLVM a shape that autovectorizes to NEON
/// `cmgt.2d` on Apple M-series (and `vpcmpgtq` on `x86_64-v3`). The
/// trailing partial word (up to 63 lanes) is handled scalar.
///
/// The null-aware path AND-folds the validity bitmap word against the
/// data-compare mask, again without per-row `set()` calls.
///
/// # Panics
///
/// Cannot panic: validity is read through the column's bitmap; the
/// output bitmap is created with the right length.
#[must_use]
pub fn cmp_gt_i64(column: &NumericColumn<i64>, scalar: i64) -> Bitmap {
    let n = column.len();
    let xa = column.data();
    let mut words = vec![0_u64; n.div_ceil(64)];

    cmp_gt_i64_pack_into(xa, scalar, &mut words);

    if let Some(nulls) = column.nulls() {
        for (w, &v) in words.iter_mut().zip(nulls.words().iter()) {
            *w &= v;
        }
    }

    Bitmap::from_words(words, n)
}

/// Pack 64-lane `a > scalar` compare results into `words`. The caller
/// guarantees `words.len() >= a.len().div_ceil(64)`.
///
/// The inner 64-lane block is shaped as eight disjoint 8-lane chunks,
/// each chunk reduced to one byte of the output word via shift-deposit.
/// LLVM autovectorizes the 8-wide compare to NEON `cmgt.2d` on aarch64
/// and the byte-deposit to a `tbl`-style bit-pack.
#[inline]
fn cmp_gt_i64_pack_into(a: &[i64], scalar: i64, words: &mut [u64]) {
    debug_assert!(words.len() >= a.len().div_ceil(64));

    let mut chunks = a.chunks_exact(64);
    let full_words = chunks.len();
    for (out_word, c) in words.iter_mut().zip(&mut chunks) {
        let arr: &[i64; 64] = c
            .try_into()
            .expect("chunks_exact(64) yields 64-element slices");
        *out_word = pack_cmp_gt_64(arr, scalar);
    }

    // Trailing partial word, up to 63 lanes.
    let rest = chunks.remainder();
    if !rest.is_empty() {
        let mut mask: u64 = 0;
        for (j, &v) in rest.iter().enumerate() {
            mask |= u64::from(v > scalar) << j;
        }
        words[full_words] = mask;
    }
}

/// Compare 64 `i64` lanes against a scalar and pack into a 64-bit
/// mask. LLVM lowers each 8-lane block to NEON `cmgt.2d` instructions;
/// the bit deposit is shift-OR. The disassembly at
/// `target-cpu=apple-m1` shows a tight NEON loop with no scalar
/// fallback in the hot region.
#[inline]
fn pack_cmp_gt_64(a: &[i64; 64], scalar: i64) -> u64 {
    let mut mask: u64 = 0;
    // 8 chunks × 8 lanes per chunk = 64 lanes per word.
    for chunk in 0..8_usize {
        let off = chunk * 8;
        let mut byte: u64 = 0;
        byte |= u64::from(a[off] > scalar);
        byte |= u64::from(a[off + 1] > scalar) << 1;
        byte |= u64::from(a[off + 2] > scalar) << 2;
        byte |= u64::from(a[off + 3] > scalar) << 3;
        byte |= u64::from(a[off + 4] > scalar) << 4;
        byte |= u64::from(a[off + 5] > scalar) << 5;
        byte |= u64::from(a[off + 6] > scalar) << 6;
        byte |= u64::from(a[off + 7] > scalar) << 7;
        mask |= byte << (chunk * 8);
    }
    mask
}

/// Scalar reference implementation of [`cmp_gt_i64`]. Used by
/// property tests to cross-validate the production kernel.
#[must_use]
pub fn cmp_gt_i64_scalar(column: &NumericColumn<i64>, scalar: i64) -> Bitmap {
    let n = column.len();
    let mut out = Bitmap::new(n, false);
    if let Some(nulls) = column.nulls() {
        for (i, &v) in column.data().iter().enumerate() {
            if nulls.get(i) && v > scalar {
                out.set(i, true);
            }
        }
    } else {
        for (i, &v) in column.data().iter().enumerate() {
            if v > scalar {
                out.set(i, true);
            }
        }
    }
    out
}

/// Fused predicate-and-sum over an `i32` column.
///
/// Returns `sum(data[i] for i where data[i] > threshold)`, widening
/// to `i64`. Skips both the intermediate `Bitmap` materialisation
/// and the per-bit iteration that `cmp_i32_scalar` +
/// `sum_i32_widening_with_mask` pay separately — the entire
/// `filter_sum` operator collapses to one tight SIMD loop on aarch64.
///
/// Equivalent to:
///
/// ```ignore
/// data.iter().filter(|&&v| v > threshold).fold(0_i64, |a, &v| a.wrapping_add(i64::from(v)))
/// ```
///
/// Negative values are handled correctly: the AND-with-compare-
/// mask trick relies on `vcgtq_s32` producing `0xFFFF_FFFF` for
/// lanes where `v > threshold` and `0` otherwise, then
/// `vandq_s32(v, mask)` preserves two's-complement negatives
/// inside selected lanes (`-5 & -1 == -5`) and zeros out the
/// rest, after which `vpaddlq_s32` widens to `i64` with sign
/// extension.
#[must_use]
pub fn filter_sum_i32_widening_gt(data: &[i32], threshold: i32) -> i64 {
    #[cfg(target_arch = "aarch64")]
    {
        filter_sum_i32_widening_gt_neon(data, threshold)
    }
    #[cfg(not(target_arch = "aarch64"))]
    {
        // Branchless multiply-by-(0 or -1)-as-mask. LLVM
        // autovectorises this to a tight SIMD loop on x86_64-v3
        // (AVX2: `vpcmpgtd` + `vpand` + widening). Result is
        // bit-identical to the aarch64 hand-NEON path.
        let mut s: i64 = 0;
        for &v in data {
            let m = i32::from(v > threshold).wrapping_neg();
            s = s.wrapping_add(i64::from(v & m));
        }
        s
    }
}

/// Hand-NEON `i32 > threshold ⇒ sum` over a contiguous slice.
///
/// Processes 16 `i32` lanes per iteration through 4 parallel
/// `int64x2_t` accumulators. Per 4-lane group:
///
/// 1. `vld1q_s32` loads 4 `i32`s.
/// 2. `vcgtq_s32(v, t)` builds a 0xFFFFFFFF / 0 lane mask.
/// 3. `vandq_s32(v, mask)` keeps the value for set lanes, zeros
///    the rest.
/// 4. `vpaddlq_s32` pairwise-adds-and-widens 4 `i32` → 2 `i64`.
/// 5. `vaddq_s64` accumulates into the running 128-bit sum.
///
/// Tail of fewer than 16 lanes falls back to the scalar branch.
#[cfg(target_arch = "aarch64")]
#[inline]
fn filter_sum_i32_widening_gt_neon(data: &[i32], threshold: i32) -> i64 {
    use std::arch::aarch64::{
        int64x2_t, vaddq_s64, vaddvq_s64, vandq_s32, vcgtq_s32, vdupq_n_s32, vdupq_n_s64,
        vld1q_s32, vpaddlq_s32, vreinterpretq_s32_u32,
    };

    let mut a0: int64x2_t = unsafe { vdupq_n_s64(0) };
    let mut a1: int64x2_t = unsafe { vdupq_n_s64(0) };
    let mut a2: int64x2_t = unsafe { vdupq_n_s64(0) };
    let mut a3: int64x2_t = unsafe { vdupq_n_s64(0) };
    let t = unsafe { vdupq_n_s32(threshold) };

    let chunks = data.chunks_exact(16);
    let rem = chunks.remainder();
    for c in chunks {
        // SAFETY: `c` is `&[i32; 16]` (chunks_exact); every
        // `vld1q_s32` reads 4 contiguous lanes inside the chunk.
        // No aliasing — `c` is a unique borrow into `data`.
        unsafe {
            let v0 = vld1q_s32(c.as_ptr());
            let v1 = vld1q_s32(c.as_ptr().add(4));
            let v2 = vld1q_s32(c.as_ptr().add(8));
            let v3 = vld1q_s32(c.as_ptr().add(12));
            let m0 = vreinterpretq_s32_u32(vcgtq_s32(v0, t));
            let m1 = vreinterpretq_s32_u32(vcgtq_s32(v1, t));
            let m2 = vreinterpretq_s32_u32(vcgtq_s32(v2, t));
            let m3 = vreinterpretq_s32_u32(vcgtq_s32(v3, t));
            a0 = vaddq_s64(a0, vpaddlq_s32(vandq_s32(v0, m0)));
            a1 = vaddq_s64(a1, vpaddlq_s32(vandq_s32(v1, m1)));
            a2 = vaddq_s64(a2, vpaddlq_s32(vandq_s32(v2, m2)));
            a3 = vaddq_s64(a3, vpaddlq_s32(vandq_s32(v3, m3)));
        }
    }
    let mut sum = unsafe {
        let half0 = vaddq_s64(a0, a1);
        let half1 = vaddq_s64(a2, a3);
        let total = vaddq_s64(half0, half1);
        vaddvq_s64(total)
    };
    for &v in rem {
        if v > threshold {
            sum = sum.wrapping_add(i64::from(v));
        }
    }
    sum
}

/// Sum an `i32` column widened to `i64`, masked by an external
/// predicate bitmap.
///
/// Bit `i` set ⇒ lane `i` contributes. Skips the per-lane
/// `Vec<i32>` materialisation a `select_column` + `sum_i32_widening`
/// pair would pay.
///
/// # Panics
///
/// Panics if `column.len() != mask.len()`.
#[must_use]
pub fn sum_i32_widening_with_mask(column: &NumericColumn<i32>, mask: &Bitmap) -> i64 {
    assert_eq!(
        column.len(),
        mask.len(),
        "sum_i32_widening_with_mask: length mismatch",
    );
    let data = column.data();
    let mut s: i64 = 0;
    for i in mask.iter_ones() {
        s = s.wrapping_add(i64::from(data[i]));
    }
    s
}

/// Sum of an `i64` column with an external mask. Only rows whose
/// `mask` bit is set contribute. Independent of the column's own
/// validity bitmap — the caller is responsible for combining masks.
///
/// # Panics
///
/// Panics if `column.len() != mask.len()`.
#[must_use]
pub fn sum_i64_with_mask(column: &NumericColumn<i64>, mask: &Bitmap) -> i64 {
    assert_eq!(
        column.len(),
        mask.len(),
        "sum_i64_with_mask: length mismatch",
    );
    let data = column.data();
    let mut s: i64 = 0;
    for i in mask.iter_ones() {
        s = s.wrapping_add(data[i]);
    }
    s
}

/// Range mask: bit `i` is set iff `lo <= column[i] <= hi` AND row is
/// non-null. Inclusive on both ends, matching SQL `BETWEEN`.
///
/// # Panics
///
/// Cannot panic for valid inputs.
#[must_use]
pub fn range_mask_i64(column: &NumericColumn<i64>, lo: i64, hi: i64) -> Bitmap {
    let n = column.len();
    let mut out = Bitmap::new(n, false);
    if let Some(nulls) = column.nulls() {
        for (i, &v) in column.data().iter().enumerate() {
            if nulls.get(i) && v >= lo && v <= hi {
                out.set(i, true);
            }
        }
    } else {
        for (i, &v) in column.data().iter().enumerate() {
            if v >= lo && v <= hi {
                out.set(i, true);
            }
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Column-vs-scalar comparison kernels for `i32` and `i64`.
//
// Each kernel produces a [`Bitmap`] whose bit `i` is set iff the row
// satisfies the comparison AND is non-null. The kernels follow the same
// 64-lane pack-into-`u64` shape as [`cmp_gt_i64`] so LLVM autovectorizes
// the inner block (NEON `cmgt.4s`/`cmgt.2d` on aarch64, `vpcmpgtd`/`vpcmpgtq`
// on x86-64-v3). The trailing partial word is handled scalar.
//
// All kernels honour SQL NULL semantics in the WHERE-clause sense: a NULL
// row never passes the predicate (3VL `UNKNOWN` is treated as `false`).
// ---------------------------------------------------------------------------

/// Per-row comparison opcode used by the column-vs-scalar kernels below.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CmpOp {
    /// `col == scalar`.
    Eq,
    /// `col != scalar`.
    Ne,
    /// `col <  scalar`.
    Lt,
    /// `col <= scalar`.
    Le,
    /// `col >  scalar`.
    Gt,
    /// `col >= scalar`.
    Ge,
}

#[inline]
const fn cmp_i32_lane(op: CmpOp, a: i32, b: i32) -> bool {
    match op {
        CmpOp::Eq => a == b,
        CmpOp::Ne => a != b,
        CmpOp::Lt => a < b,
        CmpOp::Le => a <= b,
        CmpOp::Gt => a > b,
        CmpOp::Ge => a >= b,
    }
}

#[inline]
const fn cmp_i64_lane(op: CmpOp, a: i64, b: i64) -> bool {
    match op {
        CmpOp::Eq => a == b,
        CmpOp::Ne => a != b,
        CmpOp::Lt => a < b,
        CmpOp::Le => a <= b,
        CmpOp::Gt => a > b,
        CmpOp::Ge => a >= b,
    }
}

#[inline]
fn pack_cmp_i32_64(a: &[i32; 64], scalar: i32, op: CmpOp) -> u64 {
    let mut mask: u64 = 0;
    for chunk in 0..8_usize {
        let off = chunk * 8;
        let mut byte: u64 = 0;
        byte |= u64::from(cmp_i32_lane(op, a[off], scalar));
        byte |= u64::from(cmp_i32_lane(op, a[off + 1], scalar)) << 1;
        byte |= u64::from(cmp_i32_lane(op, a[off + 2], scalar)) << 2;
        byte |= u64::from(cmp_i32_lane(op, a[off + 3], scalar)) << 3;
        byte |= u64::from(cmp_i32_lane(op, a[off + 4], scalar)) << 4;
        byte |= u64::from(cmp_i32_lane(op, a[off + 5], scalar)) << 5;
        byte |= u64::from(cmp_i32_lane(op, a[off + 6], scalar)) << 6;
        byte |= u64::from(cmp_i32_lane(op, a[off + 7], scalar)) << 7;
        mask |= byte << (chunk * 8);
    }
    mask
}

#[inline]
fn cmp_i32_pack_into(a: &[i32], scalar: i32, op: CmpOp, words: &mut [u64]) {
    debug_assert!(words.len() >= a.len().div_ceil(64));
    let mut chunks = a.chunks_exact(64);
    let full_words = chunks.len();
    for (out_word, c) in words.iter_mut().zip(&mut chunks) {
        let arr: &[i32; 64] = c
            .try_into()
            .expect("chunks_exact(64) yields 64-element slices");
        *out_word = pack_cmp_i32_64(arr, scalar, op);
    }
    let rest = chunks.remainder();
    if !rest.is_empty() {
        let mut mask: u64 = 0;
        for (j, &v) in rest.iter().enumerate() {
            mask |= u64::from(cmp_i32_lane(op, v, scalar)) << j;
        }
        words[full_words] = mask;
    }
}

#[inline]
fn pack_cmp_i64_64(a: &[i64; 64], scalar: i64, op: CmpOp) -> u64 {
    let mut mask: u64 = 0;
    for chunk in 0..8_usize {
        let off = chunk * 8;
        let mut byte: u64 = 0;
        byte |= u64::from(cmp_i64_lane(op, a[off], scalar));
        byte |= u64::from(cmp_i64_lane(op, a[off + 1], scalar)) << 1;
        byte |= u64::from(cmp_i64_lane(op, a[off + 2], scalar)) << 2;
        byte |= u64::from(cmp_i64_lane(op, a[off + 3], scalar)) << 3;
        byte |= u64::from(cmp_i64_lane(op, a[off + 4], scalar)) << 4;
        byte |= u64::from(cmp_i64_lane(op, a[off + 5], scalar)) << 5;
        byte |= u64::from(cmp_i64_lane(op, a[off + 6], scalar)) << 6;
        byte |= u64::from(cmp_i64_lane(op, a[off + 7], scalar)) << 7;
        mask |= byte << (chunk * 8);
    }
    mask
}

#[inline]
fn cmp_i64_pack_into(a: &[i64], scalar: i64, op: CmpOp, words: &mut [u64]) {
    debug_assert!(words.len() >= a.len().div_ceil(64));
    let mut chunks = a.chunks_exact(64);
    let full_words = chunks.len();
    for (out_word, c) in words.iter_mut().zip(&mut chunks) {
        let arr: &[i64; 64] = c
            .try_into()
            .expect("chunks_exact(64) yields 64-element slices");
        *out_word = pack_cmp_i64_64(arr, scalar, op);
    }
    let rest = chunks.remainder();
    if !rest.is_empty() {
        let mut mask: u64 = 0;
        for (j, &v) in rest.iter().enumerate() {
            mask |= u64::from(cmp_i64_lane(op, v, scalar)) << j;
        }
        words[full_words] = mask;
    }
}

/// Generic column-vs-scalar comparison for `i32`.
///
/// Bit `i` of the output is set iff the row is non-null AND the
/// comparison `column[i] <op> scalar` is true. NULL rows are masked to
/// 0 — matching SQL three-valued logic for `WHERE` clauses where
/// `UNKNOWN` is treated as `false`.
#[must_use]
pub fn cmp_i32_scalar(column: &NumericColumn<i32>, scalar: i32, op: CmpOp) -> Bitmap {
    let n = column.len();
    let mut words = vec![0_u64; n.div_ceil(64)];
    cmp_i32_pack_into(column.data(), scalar, op, &mut words);
    if let Some(nulls) = column.nulls() {
        for (w, &v) in words.iter_mut().zip(nulls.words().iter()) {
            *w &= v;
        }
    }
    Bitmap::from_words(words, n)
}

/// Generic column-vs-scalar comparison for `i64`.
///
/// See [`cmp_i32_scalar`] for semantics. With `op = Gt` this is the
/// same shape as [`cmp_gt_i64`]; the dispatcher routes the five
/// remaining opcodes through this entry point so the executor's fast
/// path can call a single function regardless of operator.
#[must_use]
pub fn cmp_i64_scalar(column: &NumericColumn<i64>, scalar: i64, op: CmpOp) -> Bitmap {
    let n = column.len();
    let mut words = vec![0_u64; n.div_ceil(64)];
    cmp_i64_pack_into(column.data(), scalar, op, &mut words);
    if let Some(nulls) = column.nulls() {
        for (w, &v) in words.iter_mut().zip(nulls.words().iter()) {
            *w &= v;
        }
    }
    Bitmap::from_words(words, n)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eq_matches_scalar() {
        let a = NumericColumn::from_data(vec![1_i32, 2, 3, 4, 2]);
        let b = NumericColumn::from_data(vec![1_i32, 2, 3, 5, 9]);
        let mask = eq_i32(&a, &b);
        assert!(mask.get(0));
        assert!(mask.get(1));
        assert!(mask.get(2));
        assert!(!mask.get(3));
        assert!(!mask.get(4));
        assert_eq!(mask.count_ones(), 3);
    }

    #[test]
    fn eq_with_nulls_produces_zero_at_null() {
        let a_data = vec![1_i32, 2, 3, 4];
        let mut a_nulls = Bitmap::new(4, true);
        a_nulls.set(1, false);
        let a = NumericColumn::with_nulls(a_data, a_nulls).unwrap();
        let b = NumericColumn::from_data(vec![1_i32, 2, 3, 4]);
        let mask = eq_i32(&a, &b);
        assert!(mask.get(0));
        assert!(!mask.get(1)); // null in a
        assert!(mask.get(2));
        assert!(mask.get(3));
    }

    #[test]
    fn sum_i64_basic() {
        let c = NumericColumn::from_data(vec![1_i64, 2, 3, 4]);
        assert_eq!(sum_i64(&c), 10);
    }

    #[test]
    fn sum_i64_with_nulls_skips_them() {
        let mut nulls = Bitmap::new(4, true);
        nulls.set(2, false);
        let c = NumericColumn::with_nulls(vec![10_i64, 20, 99, 40], nulls).unwrap();
        assert_eq!(sum_i64(&c), 70);
    }

    #[test]
    #[allow(clippy::cast_possible_wrap)]
    fn sum_i64_neon_matches_scalar_at_every_tail_size() {
        // Test sizes that exercise both the 8-wide main loop and
        // every possible tail length (0..=7).
        for n in [0_usize, 1, 7, 8, 15, 16, 23, 64, 100, 1_000, 4_096, 100_000] {
            let data: Vec<i64> = (0..n).map(|i| i as i64 * 3 - 7).collect();
            let scalar: i64 = data.iter().fold(0_i64, |a, b| a.wrapping_add(*b));
            let c = NumericColumn::from_data(data);
            assert_eq!(sum_i64(&c), scalar, "size {n} mismatch");
        }
    }

    #[test]
    fn sum_i32_widening_basic() {
        let c = NumericColumn::from_data(vec![1_i32, 2, 3, 4]);
        assert_eq!(sum_i32_widening(&c), 10);
    }

    #[test]
    fn sum_i32_widening_with_nulls_skips_them() {
        let mut nulls = Bitmap::new(4, true);
        nulls.set(2, false);
        let c = NumericColumn::with_nulls(vec![10_i32, 20, 99, 40], nulls).unwrap();
        assert_eq!(sum_i32_widening(&c), 70);
    }

    #[test]
    #[allow(clippy::cast_possible_wrap)]
    fn sum_i32_widening_neon_matches_scalar_at_every_tail_size() {
        // 16-wide main loop; exercise tails 0..=15.
        for n in [
            0_usize, 1, 7, 8, 15, 16, 17, 23, 31, 32, 64, 100, 1_000, 4_096, 100_000,
        ] {
            let data: Vec<i32> = (0..n)
                .map(|i| {
                    let v = i32::try_from(i).expect("test sizes bounded under i32::MAX");
                    v * 7 - 13
                })
                .collect();
            let scalar: i64 = data
                .iter()
                .fold(0_i64, |a, b| a.wrapping_add(i64::from(*b)));
            let c = NumericColumn::from_data(data);
            assert_eq!(sum_i32_widening(&c), scalar, "size {n} mismatch");
        }
    }

    #[test]
    fn sum_i32_widening_handles_negative_and_overflow_corners() {
        // i32::MIN summed with itself wraps; we want the same wrap
        // semantics the scalar fold provides (`i64::wrapping_add`).
        let data = vec![i32::MIN, i32::MIN, i32::MAX, i32::MAX, 0];
        let c = NumericColumn::from_data(data.clone());
        let scalar: i64 = data
            .iter()
            .fold(0_i64, |a, b| a.wrapping_add(i64::from(*b)));
        assert_eq!(sum_i32_widening(&c), scalar);
    }

    #[test]
    fn min_f64_skips_nan_and_nulls() {
        let mut nulls = Bitmap::new(5, true);
        nulls.set(0, false);
        let c = NumericColumn::with_nulls(vec![f64::NAN, 1.0, 0.5, f64::NAN, 2.0], nulls).unwrap();
        // Row 0 null, rows 1/4 are 1.0/2.0, row 2 = 0.5, row 3 NaN.
        assert_eq!(min_f64(&c), Some(0.5));
    }

    #[test]
    fn min_f64_all_null_returns_none() {
        let nulls = Bitmap::new(3, false);
        let c = NumericColumn::with_nulls(vec![1.0_f64, 2.0, 3.0], nulls).unwrap();
        assert_eq!(min_f64(&c), None);
    }

    #[test]
    fn select_i32_materializes_subset() {
        let c = NumericColumn::from_data(vec![10_i32, 20, 30, 40, 50]);
        let mut sel = Bitmap::new(5, false);
        sel.set(0, true);
        sel.set(2, true);
        sel.set(4, true);
        let out = select_i32(&c, &sel);
        assert_eq!(out.data(), &[10, 30, 50]);
    }

    #[test]
    fn count_i64_no_nulls_is_length() {
        let c = NumericColumn::from_data(vec![1_i64, 2, 3, 4, 5]);
        assert_eq!(count_i64(&c), 5);
    }

    #[test]
    fn count_i64_skips_nulls() {
        let mut nulls = Bitmap::new(4, true);
        nulls.set(1, false);
        nulls.set(2, false);
        let c = NumericColumn::with_nulls(vec![10_i64, 20, 30, 40], nulls).unwrap();
        assert_eq!(count_i64(&c), 2);
    }

    #[test]
    fn min_i64_basic_and_negative() {
        let c = NumericColumn::from_data(vec![5_i64, -3, 7, 0, -100, 42]);
        assert_eq!(min_i64(&c), Some(-100));
    }

    #[test]
    fn min_i64_empty_returns_none() {
        let c = NumericColumn::<i64>::from_data(vec![]);
        assert_eq!(min_i64(&c), None);
    }

    #[test]
    fn min_i64_with_nulls_skips_them() {
        let mut nulls = Bitmap::new(4, true);
        nulls.set(2, false); // would-be minimum is masked out
        let c = NumericColumn::with_nulls(vec![10_i64, 20, -999, 30], nulls).unwrap();
        assert_eq!(min_i64(&c), Some(10));
    }

    #[test]
    fn min_i64_all_null_returns_none() {
        let nulls = Bitmap::new(3, false);
        let c = NumericColumn::with_nulls(vec![1_i64, 2, 3], nulls).unwrap();
        assert_eq!(min_i64(&c), None);
    }

    #[test]
    fn max_i64_basic_and_negative() {
        let c = NumericColumn::from_data(vec![5_i64, -3, 7, 0, -100, 42]);
        assert_eq!(max_i64(&c), Some(42));
    }

    #[test]
    fn max_i64_empty_returns_none() {
        let c = NumericColumn::<i64>::from_data(vec![]);
        assert_eq!(max_i64(&c), None);
    }

    #[test]
    fn max_i64_with_nulls_skips_them() {
        let mut nulls = Bitmap::new(4, true);
        nulls.set(2, false); // would-be maximum is masked out
        let c = NumericColumn::with_nulls(vec![10_i64, 20, 99_999, 30], nulls).unwrap();
        assert_eq!(max_i64(&c), Some(30));
    }

    #[test]
    fn min_max_match_naive_scalar_reference() {
        // Property-style spot check against a scalar reference.
        let data: Vec<i64> = (0_i64..1024)
            .map(|i| i.wrapping_mul(2_862_933_555_777_941_757) ^ 0x1234_5678)
            .collect();
        let c = NumericColumn::from_data(data.clone());
        let want_min = *data.iter().min().unwrap();
        let want_max = *data.iter().max().unwrap();
        assert_eq!(min_i64(&c), Some(want_min));
        assert_eq!(max_i64(&c), Some(want_max));
    }

    #[test]
    fn cmp_gt_i64_basic() {
        let c = NumericColumn::from_data(vec![1_i64, -5, 10, 0, 100]);
        let m = cmp_gt_i64(&c, 0);
        assert!(m.get(0));
        assert!(!m.get(1));
        assert!(m.get(2));
        assert!(!m.get(3));
        assert!(m.get(4));
        assert_eq!(m.count_ones(), 3);
    }

    #[test]
    fn cmp_gt_i64_with_nulls_zeros_them() {
        let mut nulls = Bitmap::new(4, true);
        nulls.set(0, false); // mark row 0 NULL
        let c = NumericColumn::with_nulls(vec![999_i64, 5, 10, 20], nulls).unwrap();
        let m = cmp_gt_i64(&c, 0);
        assert!(!m.get(0), "null row must be 0 in mask");
        assert!(m.get(1));
        assert!(m.get(2));
        assert!(m.get(3));
    }

    #[test]
    fn sum_i64_with_mask_basic() {
        let c = NumericColumn::from_data(vec![10_i64, 20, 30, 40, 50]);
        let mut mask = Bitmap::new(5, false);
        mask.set(1, true);
        mask.set(3, true);
        assert_eq!(sum_i64_with_mask(&c, &mask), 60);
    }

    #[test]
    fn sum_i64_with_mask_all_set_matches_sum() {
        let data = (0..1000_i64).collect::<Vec<_>>();
        let c = NumericColumn::from_data(data.clone());
        let mask = Bitmap::new(1000, true);
        let want: i64 = data.iter().sum();
        assert_eq!(sum_i64_with_mask(&c, &mask), want);
    }

    #[test]
    fn sum_i64_with_mask_all_clear_is_zero() {
        let c = NumericColumn::from_data(vec![1_i64, 2, 3]);
        let mask = Bitmap::new(3, false);
        assert_eq!(sum_i64_with_mask(&c, &mask), 0);
    }

    #[test]
    #[should_panic(expected = "sum_i64_with_mask: length mismatch")]
    fn sum_i64_with_mask_length_mismatch_panics() {
        let c = NumericColumn::from_data(vec![1_i64, 2, 3]);
        let mask = Bitmap::new(4, false);
        let _ = sum_i64_with_mask(&c, &mask);
    }

    #[test]
    fn filter_sum_via_cmp_and_mask_matches_naive() {
        // Compose cmp_gt_i64 + sum_i64_with_mask and check it matches
        // the naive SQL-style filter+sum reference.
        let data: Vec<i64> = (0_i64..2048).map(|i| (i % 197).wrapping_sub(50)).collect();
        let c = NumericColumn::from_data(data.clone());
        let mask = cmp_gt_i64(&c, 0);
        let got = sum_i64_with_mask(&c, &mask);
        let want: i64 = data.iter().filter(|&&v| v > 0).copied().sum();
        assert_eq!(got, want);
    }

    #[test]
    fn cmp_i32_scalar_all_ops_basic() {
        let data = vec![-10_i32, 0, 5, 5, 7, 100];
        let c = NumericColumn::from_data(data);
        let cases = [
            (CmpOp::Eq, 5, vec![false, false, true, true, false, false]),
            (CmpOp::Ne, 5, vec![true, true, false, false, true, true]),
            (CmpOp::Lt, 5, vec![true, true, false, false, false, false]),
            (CmpOp::Le, 5, vec![true, true, true, true, false, false]),
            (CmpOp::Gt, 5, vec![false, false, false, false, true, true]),
            (CmpOp::Ge, 5, vec![false, false, true, true, true, true]),
        ];
        for (op, s, expected) in cases {
            let m = cmp_i32_scalar(&c, s, op);
            for (i, &want) in expected.iter().enumerate() {
                assert_eq!(m.get(i), want, "op={op:?} i={i}");
            }
        }
    }

    #[test]
    fn cmp_i32_scalar_with_nulls_zeros_null_rows() {
        let mut nulls = Bitmap::new(4, true);
        nulls.set(1, false);
        let c = NumericColumn::with_nulls(vec![1_i32, 999, 2, 3], nulls).unwrap();
        let m = cmp_i32_scalar(&c, 0, CmpOp::Gt);
        assert!(m.get(0));
        assert!(!m.get(1), "null row must be 0 in the mask");
        assert!(m.get(2));
        assert!(m.get(3));
    }

    #[test]
    fn cmp_i32_scalar_long_input_matches_naive() {
        // Cover lengths past the 64-lane fast-path boundary.
        for &n in &[0_usize, 1, 63, 64, 65, 128, 1023, 1024, 4096] {
            let data: Vec<i32> = (0..n)
                .map(|i| i32::try_from(i).unwrap_or(0).wrapping_mul(7) - 50)
                .collect();
            let c = NumericColumn::from_data(data.clone());
            let m = cmp_i32_scalar(&c, 100, CmpOp::Gt);
            for (i, &v) in data.iter().enumerate() {
                assert_eq!(m.get(i), v > 100, "n={n} i={i}");
            }
        }
    }

    #[test]
    fn cmp_i64_scalar_matches_cmp_gt_i64_on_gt() {
        let data: Vec<i64> = (0_i64..2048)
            .map(|i| i.wrapping_mul(2_862_933_555_777_941_757) ^ 0x42)
            .collect();
        let c = NumericColumn::from_data(data);
        let want = cmp_gt_i64(&c, 0);
        let got = cmp_i64_scalar(&c, 0, CmpOp::Gt);
        assert_eq!(got, want);
    }

    #[test]
    fn cmp_i64_scalar_all_ops_basic() {
        let c = NumericColumn::from_data(vec![-1_i64, 0, 5, 5, 10]);
        assert_eq!(cmp_i64_scalar(&c, 5, CmpOp::Eq).count_ones(), 2);
        assert_eq!(cmp_i64_scalar(&c, 5, CmpOp::Ne).count_ones(), 3);
        assert_eq!(cmp_i64_scalar(&c, 5, CmpOp::Lt).count_ones(), 2);
        assert_eq!(cmp_i64_scalar(&c, 5, CmpOp::Le).count_ones(), 4);
        assert_eq!(cmp_i64_scalar(&c, 5, CmpOp::Gt).count_ones(), 1);
        assert_eq!(cmp_i64_scalar(&c, 5, CmpOp::Ge).count_ones(), 3);
    }

    #[test]
    fn range_mask_i64_inclusive_bounds() {
        let c = NumericColumn::from_data(vec![1_i64, 5, 10, 15, 20]);
        let m = range_mask_i64(&c, 5, 15);
        assert!(!m.get(0));
        assert!(m.get(1));
        assert!(m.get(2));
        assert!(m.get(3));
        assert!(!m.get(4));
        assert_eq!(m.count_ones(), 3);
    }

    #[test]
    fn range_mask_i64_with_nulls_zero_them() {
        let mut nulls = Bitmap::new(5, true);
        nulls.set(2, false); // mid-range row is null
        let c = NumericColumn::with_nulls(vec![1_i64, 5, 10, 15, 20], nulls).unwrap();
        let m = range_mask_i64(&c, 5, 15);
        assert!(m.get(1));
        assert!(!m.get(2), "null row must be 0 even if value would qualify");
        assert!(m.get(3));
    }

    #[test]
    fn range_mask_count_matches_naive() {
        // Check against a naive reference.
        let data: Vec<i64> = (0..4096).map(|i| (i * 17) % 100).collect();
        let c = NumericColumn::from_data(data.clone());
        let m = range_mask_i64(&c, 30, 70);
        let got = m.count_ones();
        let want = data.iter().filter(|&&v| (30..=70).contains(&v)).count();
        assert_eq!(got, want);
    }

    // ---- eq_i32 cross-validation against the scalar oracle ----

    fn build_column_i32(data: Vec<i32>, null_pattern: Option<&[bool]>) -> NumericColumn<i32> {
        match null_pattern {
            None => NumericColumn::from_data(data),
            Some(pat) => {
                assert_eq!(pat.len(), data.len());
                let mut nulls = Bitmap::new(data.len(), false);
                for (i, &v) in pat.iter().enumerate() {
                    if v {
                        nulls.set(i, true);
                    }
                }
                NumericColumn::with_nulls(data, nulls).unwrap()
            }
        }
    }

    #[test]
    fn eq_i32_matches_scalar_on_assorted_lengths() {
        // Cover lengths that exercise full-word, partial-word, and
        // boundary cases of the 64-lane packed path.
        for &n in &[
            0_usize, 1, 7, 8, 63, 64, 65, 127, 128, 129, 200, 1023, 1024, 1025, 4096,
        ] {
            // Deterministic but interesting input (LCG-style scramble).
            let scramble = |i: usize| -> i32 {
                let k = i32::try_from(i % (i32::MAX as usize)).unwrap_or(0);
                k.wrapping_mul(48271_i32) ^ i32::from_ne_bytes([0x5A, 0x5A, 0x5A, 0x5A])
            };
            let a_data: Vec<i32> = (0..n).map(scramble).collect();
            let b_data: Vec<i32> = (0..n)
                .map(|i| {
                    let mut v = scramble(i);
                    if i.is_multiple_of(3) {
                        v ^= 0x1234; // disturb every third row
                    }
                    v
                })
                .collect();
            let a = NumericColumn::from_data(a_data);
            let b = NumericColumn::from_data(b_data);
            let got = eq_i32(&a, &b);
            let want = eq_i32_scalar(&a, &b);
            assert_eq!(got, want, "mismatch at n = {n}");
        }
    }

    #[test]
    fn eq_i32_matches_scalar_with_nulls() {
        for &n in &[0_usize, 1, 7, 63, 64, 65, 200, 4096] {
            let a_data: Vec<i32> = (0..n)
                .map(|i| i32::try_from(i.rem_euclid(7)).unwrap_or(0))
                .collect();
            let b_data: Vec<i32> = (0..n)
                .map(|i| i32::try_from(i.rem_euclid(7)).unwrap_or(0))
                .collect();
            let a_nulls: Vec<bool> = (0..n).map(|i| !i.is_multiple_of(5)).collect();
            let b_nulls: Vec<bool> = (0..n).map(|i| !i.is_multiple_of(11)).collect();
            let a = build_column_i32(a_data, Some(&a_nulls));
            let b = build_column_i32(b_data, Some(&b_nulls));
            let got = eq_i32(&a, &b);
            let want = eq_i32_scalar(&a, &b);
            assert_eq!(got, want, "mismatch at n = {n}");
        }
    }

    proptest::proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig {
            cases: 64, .. proptest::prelude::ProptestConfig::default()
        })]

        #[test]
        fn eq_i32_proptest_matches_scalar(
            data in proptest::collection::vec(
                (proptest::prelude::any::<i32>(), proptest::prelude::any::<i32>()),
                0_usize..=300,
            )
        ) {
            let a_vec: Vec<i32> = data.iter().map(|(x, _)| *x).collect();
            let b_vec: Vec<i32> = data.iter().map(|(_, y)| *y).collect();
            let a = NumericColumn::from_data(a_vec);
            let b = NumericColumn::from_data(b_vec);
            proptest::prop_assert_eq!(eq_i32(&a, &b), eq_i32_scalar(&a, &b));
        }

        #[test]
        fn eq_i32_proptest_matches_scalar_with_nulls(
            rows in proptest::collection::vec(
                (
                    proptest::prelude::any::<i32>(),
                    proptest::prelude::any::<i32>(),
                    proptest::prelude::any::<bool>(),
                    proptest::prelude::any::<bool>(),
                ),
                0_usize..=300,
            )
        ) {
            let n = rows.len();
            let a_data: Vec<i32> = rows.iter().map(|t| t.0).collect();
            let b_data: Vec<i32> = rows.iter().map(|t| t.1).collect();
            let a_nulls: Vec<bool> = rows.iter().map(|t| t.2).collect();
            let b_nulls: Vec<bool> = rows.iter().map(|t| t.3).collect();
            let a = if n == 0 {
                NumericColumn::from_data(a_data)
            } else {
                build_column_i32(a_data, Some(&a_nulls))
            };
            let b = if n == 0 {
                NumericColumn::from_data(b_data)
            } else {
                build_column_i32(b_data, Some(&b_nulls))
            };
            proptest::prop_assert_eq!(eq_i32(&a, &b), eq_i32_scalar(&a, &b));
        }

        #[test]
        fn min_f64_proptest_matches_scalar(
            // Mix random f64 with sprinkled NaNs by AND-ing two bools.
            rows in proptest::collection::vec(
                (
                    proptest::prelude::any::<f64>(),
                    proptest::prelude::any::<bool>(),
                ),
                0_usize..=300,
            )
        ) {
            // Force every fifth slot to NaN so the NaN-skip path is
            // exercised on most inputs (plain `any::<f64>()` rarely
            // produces NaN, even though the bit pattern allows it).
            let data: Vec<f64> = rows.iter().enumerate()
                .map(|(i, (v, _))| if i % 5 == 0 { f64::NAN } else { *v })
                .collect();
            let c = NumericColumn::from_data(data);
            // Compare bit patterns: f64 doesn't implement Eq, but two
            // identical results (Option<f64>) round-trip through their
            // bit reps perfectly except when both sides are None.
            let got = min_f64(&c);
            let want = min_f64_scalar(&c);
            match (got, want) {
                (None, None) => {}
                (Some(g), Some(w)) => proptest::prop_assert_eq!(g.to_bits(), w.to_bits()),
                _ => proptest::prop_assert!(false, "min_f64 disagrees with scalar"),
            }
        }

        #[test]
        fn min_f64_proptest_matches_scalar_with_nulls(
            rows in proptest::collection::vec(
                (
                    proptest::prelude::any::<f64>(),
                    proptest::prelude::any::<bool>(),
                    proptest::prelude::any::<bool>(),
                ),
                0_usize..=300,
            )
        ) {
            let n = rows.len();
            let data: Vec<f64> = rows.iter().enumerate()
                .map(|(i, (v, is_nan, _))| if i % 7 == 0 || *is_nan { f64::NAN } else { *v })
                .collect();
            let nulls_pat: Vec<bool> = rows.iter().map(|t| t.2).collect();
            let c = if n == 0 {
                NumericColumn::from_data(data)
            } else {
                let mut bm = Bitmap::new(n, false);
                for (i, &v) in nulls_pat.iter().enumerate() {
                    if v {
                        bm.set(i, true);
                    }
                }
                NumericColumn::with_nulls(data, bm).unwrap()
            };
            let got = min_f64(&c);
            let want = min_f64_scalar(&c);
            match (got, want) {
                (None, None) => {}
                (Some(g), Some(w)) => proptest::prop_assert_eq!(g.to_bits(), w.to_bits()),
                _ => proptest::prop_assert!(false, "min_f64 disagrees with scalar"),
            }
        }
    }
}
