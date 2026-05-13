//! Vectorized kernels.
//!
//! Each kernel here has a scalar (auto-vectorizable) implementation
//! that is the source of truth. SIMD specializations land alongside
//! the scalar versions and are validated bit-for-bit against scalar
//! in property tests.

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
        || column.data().iter().fold(0_i64, |a, b| a.wrapping_add(*b)),
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

/// Min of a non-null `f64` column. Returns `None` on empty / all-null
/// input. Honors IEEE-754 semantics for NaN: NaN values are skipped.
#[must_use]
pub fn min_f64(column: &NumericColumn<f64>) -> Option<f64> {
    let mut best: Option<f64> = None;
    if let Some(nulls) = column.nulls() {
        for (i, &v) in column.data().iter().enumerate() {
            if !nulls.get(i) || v.is_nan() {
                continue;
            }
            best = Some(best.map_or(v, |b| if v < b { v } else { b }));
        }
    } else {
        for &v in column.data() {
            if v.is_nan() {
                continue;
            }
            best = Some(best.map_or(v, |b| if v < b { v } else { b }));
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
/// # Panics
///
/// Cannot panic: validity is read through the column's bitmap; the
/// output bitmap is created with the right length.
#[must_use]
pub fn cmp_gt_i64(column: &NumericColumn<i64>, scalar: i64) -> Bitmap {
    let n = column.len();
    let mut out = Bitmap::new(n, false);
    if let Some(nulls) = column.nulls() {
        for (i, &v) in column.data().iter().enumerate() {
            if nulls.get(i) && v > scalar {
                out.set(i, true);
            }
        }
    } else {
        // Auto-vectorizable branchless loop. LLVM emits NEON `cmgt`
        // on aarch64 and AVX2 `vpcmpgtq` on x86-64-v3.
        for (i, &v) in column.data().iter().enumerate() {
            if v > scalar {
                out.set(i, true);
            }
        }
    }
    out
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
    }
}
