//! Fused branchless filter+sum kernels.
//!
//! These kernels implement the hot OLAP pattern
//! `SELECT SUM(x) FROM t WHERE y > 0` in a single pass over the inputs
//! with no intermediate `Bitmap` allocation. Each element contributes
//! `x[i] & ((y[i] > 0) ? -1 : 0)` to the running sum, so the predicate
//! is folded into a branchless AND-mask.
//!
//! The implementation uses portable scalar Rust with an
//! auto-vectorization-friendly inner loop that LLVM can lower to native
//! vector instructions on supported targets. Property tests validate the
//! fused kernel against a straightforward scalar oracle.
//!
//! NULL handling: the `_with_validity` variant accepts optional
//! validity bitmaps for `x` and `y` and AND-folds them into the per-row
//! mask. Following SQL three-valued logic, a NULL in `y` makes the
//! predicate UNKNOWN (treated as false), and a NULL in `x` contributes
//! nothing to the sum.
//!
//! Multi-core fan-out: [`filter_sum_par_i64_where_gt_zero`] partitions
//! input across `n_threads` workers. The convenience entry point
//! [`filter_sum_par_auto_i64_where_gt_zero`] picks a thread count from
//! [`std::thread::available_parallelism`].
//!
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
// Multi-core fan-out
// ============================================================================

/// Below this row count the serial kernel wins because thread-spawn
/// overhead dominates the per-worker slice.
const SERIAL_THRESHOLD: usize = 65_536;

/// Round partition sizes up to this many lanes so worker slices stay
/// cache-line friendly for both `x` and `y` streams.
const PARTITION_ALIGNMENT: usize = 64;

/// Multi-threaded fused branchless filter+sum.
///
/// Same contract as [`filter_sum_i64_where_gt_zero`] but partitions the
/// input across `n_threads` worker threads. Each worker runs the
/// single-threaded scalar kernel on its slice; the harness sums the
/// per-thread partial results in the main thread.
///
/// Threshold: when `x.len() < SERIAL_THRESHOLD` (default 65 536), the
/// kernel falls through to the serial path — thread spawn overhead
/// dominates below that point.
///
/// Partitioning: chunks are rounded up to a multiple of 64 lanes
/// (512 B per stream) so each worker's slice is cache-line friendly.
/// The final partition takes whatever remains. With `n_threads == 1`
/// this degenerates to a direct call into the serial kernel — no thread
/// is spawned.
///
/// Concurrency model: workers run inside a [`std::thread::scope`], so
/// the borrowed `x`/`y` slices outlive every worker without `Arc`. No
/// shared mutable accumulator exists: each worker returns its
/// `i64` partial sum via its closure return value, and the harness
/// folds them after `join`. This means there is zero cross-core
/// cache-line contention on the accumulator until the final
/// `wrapping_add` reduce in the main thread.
///
/// Wrapping arithmetic: identical semantics to the serial kernel —
/// partial sums and the final reduce both use `wrapping_add`.
///
/// # Panics
///
/// Cannot panic for valid inputs. Length-mismatch is debug-asserted
/// and returns 0 in release as in the serial variant.
#[must_use]
pub fn filter_sum_par_i64_where_gt_zero(
    x: &NumericColumn<i64>,
    y: &NumericColumn<i64>,
    n_threads: usize,
) -> i64 {
    debug_assert_eq!(
        x.len(),
        y.len(),
        "filter_sum_par_i64_where_gt_zero: length mismatch",
    );
    if x.len() != y.len() {
        return 0;
    }
    let xs = x.data();
    let ys = y.data();

    // Early-out: tiny inputs go straight to the serial path so we
    // never pay for a `thread::scope` we cannot amortize.
    if n_threads <= 1 || xs.len() < SERIAL_THRESHOLD {
        return filter_sum_dispatch(xs, ys);
    }

    filter_sum_par_slice(xs, ys, n_threads)
}

/// Convenience variant of [`filter_sum_par_i64_where_gt_zero`] that
/// picks `n_threads` from [`std::thread::available_parallelism`].
///
/// Falls back to `1` (i.e. the serial path) if the platform refuses to
/// report a parallelism value.
#[must_use]
pub fn filter_sum_par_auto_i64_where_gt_zero(
    x: &NumericColumn<i64>,
    y: &NumericColumn<i64>,
) -> i64 {
    let n_threads = std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get);
    filter_sum_par_i64_where_gt_zero(x, y, n_threads)
}

/// Slice-level worker dispatch. Splits `xs` / `ys` into
/// `n_threads` partitions (rounded up to the kernel's alignment),
/// runs the single-threaded scalar kernel on each in parallel via
/// `std::thread::scope`, and `wrapping_add`s the partial sums.
///
/// Invariants enforced by the caller:
/// - `xs.len() == ys.len()`
/// - `n_threads >= 2`
/// - `xs.len() >= SERIAL_THRESHOLD`
fn filter_sum_par_slice(xs: &[i64], ys: &[i64], n_threads: usize) -> i64 {
    debug_assert_eq!(xs.len(), ys.len());
    debug_assert!(n_threads >= 2);

    let n = xs.len();
    // Compute a partition size rounded up to `PARTITION_ALIGNMENT`.
    // For inputs that are not an exact multiple of the alignment, the
    // last worker takes the possibly smaller remainder.
    let raw_part = n.div_ceil(n_threads);
    let part = raw_part
        .next_multiple_of(PARTITION_ALIGNMENT)
        .max(PARTITION_ALIGNMENT);

    // If rounding makes the first partition cover the entire input,
    // fall back to the serial path — no point spawning threads we
    // would immediately starve.
    if part >= n {
        return filter_sum_dispatch(xs, ys);
    }

    // Build the per-worker slice pairs.
    //
    // Bounded fan-out: `n_threads` is a usize from the caller; we use
    // it to size a `SmallVec` allocated on the worker harness's stack.
    // No per-iteration heap alloc happens in the worker body — each
    // closure carries only two `&[i64]` slices and returns an `i64`.
    let mut slices: smallvec::SmallVec<[(&[i64], &[i64]); 16]> =
        smallvec::SmallVec::with_capacity(n_threads);
    let mut off = 0_usize;
    while off < n {
        let end = off
            .checked_add(part)
            .map_or(n, |candidate| candidate.min(n));
        slices.push((&xs[off..end], &ys[off..end]));
        off = end;
    }

    // Scoped fan-out. Each worker computes its partial sum and the
    // harness reduces them. `std::thread::scope` guarantees every
    // spawned thread joins before the scope returns, which means the
    // borrowed `&[i64]` slices outlive every worker without an `Arc`
    // or static lifetime.
    //
    // We deliberately do *not* run a partition on the caller's
    // thread. Keeping every partition in a spawned worker gives a
    // consistent reduce path and avoids mixing harness work with the
    // hot scan loop.
    std::thread::scope(|s| {
        // Reserve handles upfront so the spawn loop does no resizing
        // and the `join` order matches the partition order.
        let mut handles: smallvec::SmallVec<[std::thread::ScopedJoinHandle<'_, i64>; 16]> =
            smallvec::SmallVec::with_capacity(slices.len());
        for (x_slice, y_slice) in slices {
            handles.push(s.spawn(move || filter_sum_dispatch(x_slice, y_slice)));
        }
        // Reduce: `wrapping_add` preserves the serial-kernel semantics
        // (partial sums commute under wrapping addition).
        let mut total: i64 = 0;
        let mut worker_panicked = false;
        for h in handles {
            match h.join() {
                Ok(partial) => total = total.wrapping_add(partial),
                Err(_) => worker_panicked = true,
            }
        }
        if worker_panicked {
            return filter_sum_dispatch(xs, ys);
        }
        total
    })
}

// ============================================================================
// Dispatch + scalar fast path
// ============================================================================

#[inline]
fn filter_sum_dispatch(xs: &[i64], ys: &[i64]) -> i64 {
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
        let &[x0, x1, x2, x3, x4, x5, x6, x7] = cx else {
            continue;
        };
        let &[y0, y1, y2, y3, y4, y5, y6, y7] = cy else {
            continue;
        };

        let m0 = (i64::from(y0 > 0)).wrapping_neg();
        let m1 = (i64::from(y1 > 0)).wrapping_neg();
        let m2 = (i64::from(y2 > 0)).wrapping_neg();
        let m3 = (i64::from(y3 > 0)).wrapping_neg();
        s0 = s0.wrapping_add(x0 & m0);
        s0 = s0.wrapping_add(x1 & m1);
        s0 = s0.wrapping_add(x2 & m2);
        s0 = s0.wrapping_add(x3 & m3);

        let m4 = (i64::from(y4 > 0)).wrapping_neg();
        let m5 = (i64::from(y5 > 0)).wrapping_neg();
        let m6 = (i64::from(y6 > 0)).wrapping_neg();
        let m7 = (i64::from(y7 > 0)).wrapping_neg();
        s1 = s1.wrapping_add(x4 & m4);
        s1 = s1.wrapping_add(x5 & m5);
        s1 = s1.wrapping_add(x6 & m6);
        s1 = s1.wrapping_add(x7 & m7);
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

    let nwords = n
        .checked_div(64)
        .expect("division by nonzero bitmap word width must succeed");
    for w in 0..nwords {
        let base = w
            .checked_mul(64)
            .expect("bitmap word base index must fit usize");
        let xv = x_valid.map_or(u64::MAX, |b| b.words()[w]);
        let yv = y_valid.map_or(u64::MAX, |b| b.words()[w]);
        let valid_word = xv & yv;

        // Process the 64 rows in halves to feed two accumulators.
        for j in 0..32_usize {
            let i = base
                .checked_add(j)
                .expect("bitmap half-word row index must fit usize");
            let valid_bit = ((valid_word >> j) & 1) != 0;
            let valid_mask = i64::from(valid_bit).wrapping_neg();
            let gt_mask = (i64::from(ys[i] > 0)).wrapping_neg();
            let m = valid_mask & gt_mask;
            s0 = s0.wrapping_add(xs[i] & m);
        }
        for j in 32..64_usize {
            let i = base
                .checked_add(j)
                .expect("bitmap half-word row index must fit usize");
            let valid_bit = ((valid_word >> j) & 1) != 0;
            let valid_mask = i64::from(valid_bit).wrapping_neg();
            let gt_mask = (i64::from(ys[i] > 0)).wrapping_neg();
            let m = valid_mask & gt_mask;
            s1 = s1.wrapping_add(xs[i] & m);
        }
    }

    // Tail.
    let tail_start = nwords
        .checked_mul(64)
        .expect("bitmap tail start index must fit usize");
    if tail_start < n {
        let last_word = n
            .checked_sub(tail_start)
            .expect("tail start is bounded by row count");
        let xv = x_valid.map_or(u64::MAX, |b| b.words()[nwords]);
        let yv = y_valid.map_or(u64::MAX, |b| b.words()[nwords]);
        let valid_word = xv & yv;
        for j in 0..last_word {
            let i = tail_start
                .checked_add(j)
                .expect("bitmap tail row index must fit usize");
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
        // Lengths around common vectorization and partition boundaries.
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

    // ---- Multi-core fan-out tests ----

    #[test]
    fn par_empty_input_returns_zero() {
        let x = NumericColumn::from_data(Vec::<i64>::new());
        let y = NumericColumn::from_data(Vec::<i64>::new());
        for nt in [1_usize, 2, 4, 8] {
            assert_eq!(filter_sum_par_i64_where_gt_zero(&x, &y, nt), 0);
        }
    }

    #[test]
    fn par_below_threshold_matches_serial() {
        // Just under the SERIAL_THRESHOLD: the par entry point must
        // produce the same bits as the serial kernel even though it
        // falls through without spawning threads.
        let n = 4_096_usize;
        let xs: Vec<i64> = (0_i64..n.try_into().unwrap_or(0)).collect();
        let ys: Vec<i64> = (0_i64..n.try_into().unwrap_or(0))
            .map(|i| if i % 3 == 0 { -i } else { i })
            .collect();
        let x = NumericColumn::from_data(xs);
        let y = NumericColumn::from_data(ys);
        let serial = filter_sum_i64_where_gt_zero(&x, &y);
        for nt in [1_usize, 2, 3, 4, 8, 16] {
            assert_eq!(filter_sum_par_i64_where_gt_zero(&x, &y, nt), serial);
        }
    }

    #[test]
    fn par_above_threshold_matches_serial() {
        // Crossing the threshold spawns workers — exercise multiple
        // partition sizes and confirm every result agrees with the
        // serial kernel bit-for-bit.
        let n = 200_000_usize;
        let mut s: u64 = 0xDEAD_BEEF_C0FF_EE01;
        let mut xs: Vec<i64> = Vec::with_capacity(n);
        let mut ys: Vec<i64> = Vec::with_capacity(n);
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
        let x = NumericColumn::from_data(xs);
        let y = NumericColumn::from_data(ys);
        let serial = filter_sum_i64_where_gt_zero(&x, &y);
        for nt in [2_usize, 3, 4, 5, 7, 8, 11, 16] {
            assert_eq!(
                filter_sum_par_i64_where_gt_zero(&x, &y, nt),
                serial,
                "par with n_threads = {nt} disagrees with serial",
            );
        }
        // The auto entry point picks `available_parallelism()`; it
        // must also agree.
        assert_eq!(
            filter_sum_par_auto_i64_where_gt_zero(&x, &y),
            serial,
            "par_auto disagrees with serial",
        );
    }

    #[test]
    fn par_partition_alignment_corner_cases() {
        // Lengths chosen to land on, just past, and just before the
        // 64-lane partition alignment boundary used by the parallel
        // dispatcher. We force the par path by going above the
        // SERIAL_THRESHOLD via repeated tiling.
        for base in [65_536_usize, 65_537, 65_600, 131_072, 131_135] {
            let xs: Vec<i64> = (0..base)
                .map(|i| i64::try_from(i % 257).unwrap_or(0) - 128)
                .collect();
            let ys: Vec<i64> = (0..base)
                .map(|i| i64::try_from(i % 5).unwrap_or(0) - 2)
                .collect();
            let x = NumericColumn::from_data(xs);
            let y = NumericColumn::from_data(ys);
            let serial = filter_sum_i64_where_gt_zero(&x, &y);
            for nt in [2_usize, 4, 8] {
                assert_eq!(
                    filter_sum_par_i64_where_gt_zero(&x, &y, nt),
                    serial,
                    "n={base}, nt={nt}",
                );
            }
        }
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

    // The par kernel's correctness contract: for any `n_threads`
    // setting in {1, 2, 3, 4, 8, 16}, the parallel kernel must produce
    // the same `i64` as the serial kernel — bit-for-bit. We use a
    // 256-case budget to cover partition-boundary edge cases without
    // blowing up wall-clock for small `n` inputs. Lengths up to
    // 50_000 keep us mostly below `SERIAL_THRESHOLD`; the dedicated
    // `par_above_threshold_matches_serial` unit test covers the
    // worker-spawn path against a 200 K-row dataset.
    proptest::proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig {
            cases: 256,
            .. proptest::prelude::ProptestConfig::default()
        })]

        #[test]
        fn prop_par_matches_serial(
            rows in proptest::collection::vec(
                (proptest::prelude::any::<i64>(), proptest::prelude::any::<i64>()),
                0_usize..=50_000,
            ),
            nt_pick in proptest::prelude::prop::sample::select(
                vec![1_usize, 2, 3, 4, 8, 16],
            ),
        ) {
            let xs: Vec<i64> = rows.iter().map(|(a, _)| *a).collect();
            let ys: Vec<i64> = rows.iter().map(|(_, b)| *b).collect();
            let x = NumericColumn::from_data(xs);
            let y = NumericColumn::from_data(ys);
            let want = filter_sum_i64_where_gt_zero(&x, &y);
            let got = filter_sum_par_i64_where_gt_zero(&x, &y, nt_pick);
            proptest::prop_assert_eq!(got, want);
        }
    }
}
