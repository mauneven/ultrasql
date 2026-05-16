//! Window function operator.
//!
//! [`WindowAgg`] evaluates an `OVER (PARTITION BY … ORDER BY … ROWS/RANGE …)`
//! clause. The operator:
//!
//! 1. Drains the child completely.
//! 2. Partitions rows by the `PARTITION BY` key.
//! 3. Within each partition, sorts by the `ORDER BY` key (if any).
//! 4. Applies the window function to produce one output column per row.
//! 5. Emits the original columns plus the new window column in 4096-row batches.
//!
//! # Supported functions
//!
//! | Function | Status |
//! |----------|--------|
//! | `ROW_NUMBER()` | Supported |
//! | `RANK()` | Supported |
//! | `DENSE_RANK()` | Supported |
//! | `LAG(expr, offset, default)` | Supported |
//! | `LEAD(expr, offset, default)` | Supported |
//! | `FIRST_VALUE(expr)` | Supported |
//! | `LAST_VALUE(expr)` | Supported |
//! | `NTH_VALUE(expr, n)` | Supported |
//! | `NTILE(n)` | Supported |
//!
//! # Frame support
//!
//! For v0.5 the frame is always `ROWS BETWEEN UNBOUNDED PRECEDING AND
//! CURRENT ROW` (the SQL default for functions that use the frame).
//! `RANGE` frames and explicit frame bounds are a v0.6 follow-up.

#![allow(clippy::cast_possible_wrap)]

use std::collections::VecDeque;
use std::thread;

use ultrasql_core::{Schema, Value};
use ultrasql_planner::ScalarExpr;
use ultrasql_vec::Batch;
use ultrasql_vec::column::{Column, NumericColumn};

use crate::eval::Eval;
use crate::filter_op::batch_to_rows;
use crate::seq_scan::build_batch;
use crate::sort::compare_values_nullable;
use crate::{ExecError, Operator};

const BATCH_TARGET_ROWS: usize = 4096;

/// Row-count threshold at which the columnar `row_number` fast path
/// switches from a single-threaded `sort_unstable_by` to a chunked
/// parallel sort + 2-way merge tree. Calibrated on Apple M-class
/// silicon: below 16 384 rows the cost of spawning scoped workers
/// dominates the wall-clock saved by the parallel sort.
const PARALLEL_SORT_THRESHOLD: usize = 16 * 1024;

/// Maximum worker count for the parallel sort. Capped at 8 to match
/// the host topologies our `≥ 2×` performance gate targets (4–8
/// performance cores on Apple M-series, 8–16 cores on x86 server
/// CPUs) and to keep the merge tree shallow (log₂ 8 = 3 passes).
const PARALLEL_SORT_MAX_THREADS: usize = 8;

/// Minimum worker count for the parallel sort. We always want at
/// least two workers when we cross the threshold, otherwise we pay
/// scope overhead with no parallelism in return.
const PARALLEL_SORT_MIN_THREADS: usize = 2;

/// The window function to compute.
#[derive(Debug, Clone)]
pub enum WindowFunc {
    /// `ROW_NUMBER()` — 1-based row number within the partition.
    RowNumber,
    /// `RANK()` — rank with gaps (tied rows share the same rank, next rank skips).
    Rank,
    /// `DENSE_RANK()` — rank without gaps.
    DenseRank,
    /// `LAG(expr, offset, default)` — value from `offset` rows earlier.
    Lag {
        /// The value expression.
        expr: ScalarExpr,
        /// Number of rows back (default 1).
        offset: usize,
        /// Default value when out of partition bounds.
        default: Value,
    },
    /// `LEAD(expr, offset, default)` — value from `offset` rows ahead.
    Lead {
        /// The value expression.
        expr: ScalarExpr,
        /// Number of rows ahead (default 1).
        offset: usize,
        /// Default value when out of partition bounds.
        default: Value,
    },
    /// `FIRST_VALUE(expr)` — first value in the partition.
    FirstValue(ScalarExpr),
    /// `LAST_VALUE(expr)` — last value in the partition.
    LastValue(ScalarExpr),
    /// `NTH_VALUE(expr, n)` — n-th value (1-based) in the partition.
    NthValue {
        /// The value expression.
        expr: ScalarExpr,
        /// 1-based position.
        n: usize,
    },
    /// `NTILE(n)` — divide the partition into `n` buckets.
    Ntile(usize),
}

/// Window function operator.
///
/// Appends one output column (the window function result) to each row.
///
/// # Send
///
/// `Box<dyn Operator>`, `Schema`, `WindowFunc`, and `Vec<Eval>` are all `Send`.
#[derive(Debug)]
pub struct WindowAgg {
    child: Box<dyn Operator>,
    /// Raw partition-by expressions (kept for fast-path shape detection).
    partition_keys: Vec<ScalarExpr>,
    /// Raw order-by expressions (kept for fast-path shape detection).
    order_keys: Vec<ScalarExpr>,
    /// Expressions for the PARTITION BY keys.
    partition_key_evals: Vec<Eval>,
    /// Expressions for the ORDER BY keys.
    order_key_evals: Vec<Eval>,
    /// The window function.
    func: WindowFunc,
    schema: Schema,
    child_schema: Schema,
    pending: VecDeque<Batch>,
    primed: bool,
    eof: bool,
}

impl WindowAgg {
    /// Construct a window aggregate operator.
    ///
    /// - `child` — the input operator.
    /// - `partition_keys` — PARTITION BY expressions.
    /// - `order_keys` — ORDER BY expressions.
    /// - `func` — the window function to compute.
    /// - `schema` — output schema: child columns plus one window output column.
    #[must_use]
    pub fn new(
        child: Box<dyn Operator>,
        partition_keys: Vec<ScalarExpr>,
        order_keys: Vec<ScalarExpr>,
        func: WindowFunc,
        schema: Schema,
    ) -> Self {
        let child_schema = child.schema().clone();
        let partition_key_evals: Vec<Eval> =
            partition_keys.iter().cloned().map(Eval::new).collect();
        let order_key_evals: Vec<Eval> = order_keys.iter().cloned().map(Eval::new).collect();
        Self {
            child,
            partition_keys,
            order_keys,
            partition_key_evals,
            order_key_evals,
            func,
            schema,
            child_schema,
            pending: VecDeque::new(),
            primed: false,
            eof: false,
        }
    }
}

impl Operator for WindowAgg {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.eof {
            return Ok(None);
        }
        if !self.primed {
            let batches = self.execute_into_batches()?;
            self.pending.extend(batches);
            self.primed = true;
        }
        if let Some(batch) = self.pending.pop_front() {
            return Ok(Some(batch));
        }
        self.eof = true;
        Ok(None)
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

impl WindowAgg {
    /// Drive the window aggregate to completion, returning the output
    /// batches ready for streaming through `next_batch`. Dispatches to
    /// the columnar fast path when the query shape qualifies; falls
    /// back to the row-oriented slow path otherwise.
    fn execute_into_batches(&mut self) -> Result<Vec<Batch>, ExecError> {
        if let Some(batches) = self.try_columnar_row_number()? {
            return Ok(batches);
        }
        let rows = self.execute()?;
        if rows.is_empty() {
            return Ok(Vec::new());
        }
        let mut out: Vec<Batch> = Vec::with_capacity(rows.len().div_ceil(BATCH_TARGET_ROWS));
        for chunk in rows.chunks(BATCH_TARGET_ROWS) {
            out.push(build_batch(chunk, &self.schema)?);
        }
        Ok(out)
    }

    /// Columnar fast path for `row_number() OVER (ORDER BY <int_col>)`
    /// with no `PARTITION BY`. Drains the child without converting
    /// batches to rows, sorts a flat `Vec<i64>` of keys, scatters the
    /// rank back into a row-aligned `Vec<i64>`, and emits batches that
    /// reuse the original columns plus an appended Int64 column.
    ///
    /// Returns `None` when the shape does not qualify, in which case
    /// the caller falls back to [`Self::execute`].
    fn try_columnar_row_number(&mut self) -> Result<Option<Vec<Batch>>, ExecError> {
        if !matches!(self.func, WindowFunc::RowNumber) {
            return Ok(None);
        }
        if !self.partition_keys.is_empty() {
            return Ok(None);
        }
        if self.order_keys.len() != 1 {
            return Ok(None);
        }
        let ScalarExpr::Column { index, .. } = &self.order_keys[0] else {
            return Ok(None);
        };
        let order_col_idx = *index;

        // Drain the child as-is; record per-batch row counts so we can
        // slice the window-value column back out without re-walking.
        let mut input_batches: Vec<Batch> = Vec::new();
        let mut row_offsets: Vec<usize> = vec![0];
        let mut total: usize = 0;
        loop {
            let Some(batch) = self.child.next_batch()? else {
                break;
            };
            total += batch.rows();
            row_offsets.push(total);
            input_batches.push(batch);
        }
        if total == 0 {
            return Ok(Some(Vec::new()));
        }

        // Build a flat `Vec<i64>` of keys. NULLs are sorted last via an
        // `i64::MAX` sentinel for the bench shape (the slow path still
        // handles the general null case). For an integer-typed order
        // column the `i64::from(i32)` widening is the only conversion
        // each row pays.
        let mut keys: Vec<i64> = Vec::with_capacity(total);
        for batch in &input_batches {
            let col = batch.columns().get(order_col_idx).ok_or_else(|| {
                ExecError::TypeMismatch(format!(
                    "window: order column index {order_col_idx} out of range"
                ))
            })?;
            match col {
                Column::Int32(c) => {
                    let nulls = c.nulls();
                    for (i, v) in c.data().iter().enumerate() {
                        if nulls.is_some_and(|b| !b.get(i)) {
                            keys.push(i64::MAX);
                        } else {
                            keys.push(i64::from(*v));
                        }
                    }
                }
                Column::Int64(c) => {
                    let nulls = c.nulls();
                    for (i, v) in c.data().iter().enumerate() {
                        if nulls.is_some_and(|b| !b.get(i)) {
                            keys.push(i64::MAX);
                        } else {
                            keys.push(*v);
                        }
                    }
                }
                // Bail out of the fast path for non-integer keys; the
                // slow path handles every supported type.
                _ => return Ok(None),
            }
        }

        // Pre-sorted shortcut. Insertion-order key sequences that are
        // already non-decreasing yield `window_col[i] = i + 1` —
        // identical to what `sort_unstable_by(key, then-by-index)`
        // followed by a scatter would produce. Skipping the sort +
        // scatter pair on this shape collapses the pair-vector build
        // (~50 µs at n = 65 536) and the scatter (~25 µs) into a
        // single O(n) monotonic scan (~30 µs). The bench's
        // `SELECT row_number() OVER (ORDER BY x)` against a table
        // loaded in ascending `x` order takes this path on every hot
        // iteration; the same shape recurs in any
        // `row_number() OVER (ORDER BY pk)` over a heap that has not
        // received updates that moved rows past their original
        // position.
        let window_col: Vec<i64> = if is_non_decreasing(&keys) {
            (1..=i64_from_usize_clamped(total)).collect()
        } else if total >= PARALLEL_SORT_THRESHOLD {
            // Pair-vector parallel sort. `(i64, u32)` lives in 16 bytes
            // so the comparator hits a single L1 line per compare.
            // Above the threshold we fan the sort across scoped
            // workers and merge the sorted runs back into a single
            // output buffer; below the threshold the scope-setup cost
            // would dominate the saved sort work.
            let pairs: Vec<(i64, u32)> = keys
                .iter()
                .enumerate()
                .map(|(i, &k)| (k, u32_from_usize_clamped(i)))
                .collect();
            let sorted = parallel_sort_pairs(pairs);
            scatter_rank_from_pairs(&sorted, total)
        } else {
            let mut pairs: Vec<(i64, u32)> = keys
                .iter()
                .enumerate()
                .map(|(i, &k)| (k, u32_from_usize_clamped(i)))
                .collect();
            pairs.sort_unstable_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
            scatter_rank_from_pairs(&pairs, total)
        };

        // Build output batches by moving the input columns out of
        // each `Batch` (no clone) and pushing the matching window
        // slice. Each input batch carries up to BATCH_TARGET_ROWS so
        // no resplit is needed.
        let mut out: Vec<Batch> = Vec::with_capacity(input_batches.len());
        for (batch, window) in input_batches
            .into_iter()
            .zip(row_offsets.windows(2).map(|w| (w[0], w[1])))
        {
            let (lo, hi) = window;
            let mut columns = batch.into_columns();
            let window_slice: Vec<i64> = window_col[lo..hi].to_vec();
            columns.push(Column::Int64(NumericColumn::from_data(window_slice)));
            out.push(Batch::new(columns).map_err(|e| {
                ExecError::TypeMismatch(format!("window fast path batch build: {e}"))
            })?);
        }
        Ok(Some(out))
    }

    #[allow(clippy::too_many_lines)]
    fn execute(&mut self) -> Result<Vec<Vec<Value>>, ExecError> {
        // Drain child.
        let mut all_rows: Vec<Vec<Value>> = Vec::new();
        loop {
            let Some(batch) = self.child.next_batch()? else {
                break;
            };
            all_rows.extend(batch_to_rows(&batch, &self.child_schema)?);
        }

        let n_total = all_rows.len();
        if n_total == 0 {
            return Ok(Vec::new());
        }

        // Pre-evaluate ORDER BY keys once per row. Previously the sort
        // comparator re-evaluated each expression on every call, which
        // dominated runtime for large partitions.
        let order_key_count = self.order_key_evals.len();
        let order_keys: Vec<Value> = if order_key_count == 0 {
            Vec::new()
        } else {
            let mut buf = Vec::with_capacity(n_total * order_key_count);
            for row in &all_rows {
                for kv in &self.order_key_evals {
                    buf.push(kv.eval(row).unwrap_or(Value::Null));
                }
            }
            buf
        };
        let row_order_key = |idx: usize| -> &[Value] {
            if order_key_count == 0 {
                &[]
            } else {
                let lo = idx * order_key_count;
                &order_keys[lo..lo + order_key_count]
            }
        };

        // Partition the row indices. Fast path: no PARTITION BY hands
        // the entire range to a single partition without building a
        // per-row key vector.
        let partitions: Vec<Vec<usize>> = if self.partition_key_evals.is_empty() {
            vec![(0..n_total).collect()]
        } else {
            let key_count = self.partition_key_evals.len();
            let mut keys: Vec<Value> = Vec::with_capacity(n_total * key_count);
            for row in &all_rows {
                for kv in &self.partition_key_evals {
                    keys.push(kv.eval(row).unwrap_or(Value::Null));
                }
            }
            let key_slice = |i: usize| -> &[Value] {
                let lo = i * key_count;
                &keys[lo..lo + key_count]
            };
            let mut parts: Vec<Vec<usize>> = Vec::new();
            let mut current: Vec<usize> = Vec::new();
            let mut current_key_start: Option<usize> = None;
            for idx in 0..n_total {
                let same = current_key_start
                    .map(|s| keys_equal(&keys[s..s + key_count], key_slice(idx)))
                    .unwrap_or(false);
                if !same {
                    if !current.is_empty() {
                        parts.push(std::mem::take(&mut current));
                    }
                    current_key_start = Some(idx * key_count);
                }
                current.push(idx);
            }
            if !current.is_empty() {
                parts.push(current);
            }
            parts
        };

        // One pre-sized output buffer; we drop the window value into
        // the slot owned by each row's *original* index so the final
        // assembly walks `all_rows` once and consumes it.
        let mut window_values: Vec<Value> = vec![Value::Null; n_total];

        for partition_indices in &partitions {
            // Sort using the cached order-key buffer. Comparator reads
            // a pre-computed slice instead of calling the interpreter.
            let mut sorted_indices = partition_indices.clone();
            if order_key_count != 0 {
                sorted_indices.sort_by(|&a, &b| {
                    let ka = row_order_key(a);
                    let kb = row_order_key(b);
                    for i in 0..order_key_count {
                        let ord = compare_values_nullable(&ka[i], &kb[i], false);
                        if ord != std::cmp::Ordering::Equal {
                            return ord;
                        }
                    }
                    std::cmp::Ordering::Equal
                });
            }

            let n = sorted_indices.len();
            let values: Vec<Value> = match &self.func {
                WindowFunc::RowNumber => (1..=n).map(|i| Value::Int64(i as i64)).collect(),
                WindowFunc::Rank => {
                    let mut out_ranks = vec![1_i64; n];
                    let mut base_rank = 1_usize;
                    let mut prev_pos: Option<usize> = None;
                    for (pos, &idx) in sorted_indices.iter().enumerate() {
                        let same = prev_pos
                            .map(|p| row_order_key(sorted_indices[p]) == row_order_key(idx))
                            .unwrap_or(false);
                        if !same {
                            base_rank = pos + 1;
                            prev_pos = Some(pos);
                        }
                        out_ranks[pos] = base_rank as i64;
                    }
                    out_ranks.into_iter().map(Value::Int64).collect()
                }
                WindowFunc::DenseRank => {
                    let mut out = Vec::with_capacity(n);
                    let mut dense = 1_i64;
                    let mut prev_pos: Option<usize> = None;
                    for (pos, &idx) in sorted_indices.iter().enumerate() {
                        let same = prev_pos
                            .map(|p| row_order_key(sorted_indices[p]) == row_order_key(idx))
                            .unwrap_or(false);
                        if !same {
                            if prev_pos.is_some() {
                                dense += 1;
                            }
                            prev_pos = Some(pos);
                        }
                        out.push(Value::Int64(dense));
                    }
                    out
                }
                WindowFunc::Lag {
                    expr,
                    offset,
                    default,
                } => {
                    let interp = Eval::new(expr.clone());
                    let offset = *offset;
                    let default = default.clone();
                    sorted_indices
                        .iter()
                        .enumerate()
                        .map(|(pos, &_idx)| {
                            if pos < offset {
                                default.clone()
                            } else {
                                let prev_idx = sorted_indices[pos - offset];
                                interp
                                    .eval(&all_rows[prev_idx])
                                    .unwrap_or_else(|_| default.clone())
                            }
                        })
                        .collect()
                }
                WindowFunc::Lead {
                    expr,
                    offset,
                    default,
                } => {
                    let interp = Eval::new(expr.clone());
                    let offset = *offset;
                    let default = default.clone();
                    sorted_indices
                        .iter()
                        .enumerate()
                        .map(|(pos, &_idx)| {
                            if pos + offset >= n {
                                default.clone()
                            } else {
                                let next_idx = sorted_indices[pos + offset];
                                interp
                                    .eval(&all_rows[next_idx])
                                    .unwrap_or_else(|_| default.clone())
                            }
                        })
                        .collect()
                }
                WindowFunc::FirstValue(expr) => {
                    let interp = Eval::new(expr.clone());
                    let first = sorted_indices.first().map_or(Value::Null, |&i| {
                        interp.eval(&all_rows[i]).unwrap_or(Value::Null)
                    });
                    vec![first; n]
                }
                WindowFunc::LastValue(expr) => {
                    let interp = Eval::new(expr.clone());
                    let last = sorted_indices.last().map_or(Value::Null, |&i| {
                        interp.eval(&all_rows[i]).unwrap_or(Value::Null)
                    });
                    vec![last; n]
                }
                WindowFunc::NthValue { expr, n: nth } => {
                    let interp = Eval::new(expr.clone());
                    let nth = *nth;
                    let val = if nth == 0 || nth > n {
                        Value::Null
                    } else {
                        let idx = sorted_indices[nth - 1];
                        interp.eval(&all_rows[idx]).unwrap_or(Value::Null)
                    };
                    vec![val; n]
                }
                WindowFunc::Ntile(bucket_count) => {
                    let bucket_count = *bucket_count;
                    (0..n)
                        .map(|pos| {
                            let bucket = if bucket_count == 0 {
                                1
                            } else {
                                (pos * bucket_count) / n + 1
                            };
                            Value::Int64(bucket as i64)
                        })
                        .collect()
                }
            };

            // Scatter the partition's window values back into the
            // global buffer at each row's original index.
            for (pos, &orig_idx) in sorted_indices.iter().enumerate() {
                window_values[orig_idx] = values[pos].clone();
            }
        }

        // Final assembly: walk `all_rows` once, consume it, and
        // append the corresponding window value. No clone of the
        // input row, no global sort.
        let output: Vec<Vec<Value>> = all_rows
            .into_iter()
            .zip(window_values)
            .map(|(mut row, win_val)| {
                row.push(win_val);
                row
            })
            .collect();

        Ok(output)
    }
}

fn keys_equal(a: &[Value], b: &[Value]) -> bool {
    a.len() == b.len()
        && a.iter().zip(b.iter()).all(|(av, bv)| match (av, bv) {
            (Value::Null, Value::Null) => true,
            _ => av == bv,
        })
}

/// `true` iff `keys` is monotonically non-decreasing.
///
/// `windows(2).all(...)` reads two adjacent elements per step; on a
/// hot 65 536-element `Vec<i64>` the loop hits ~512 KiB sequentially
/// and finishes in ~30 µs on Apple M-class silicon (one branch per
/// pair, fully pipelined). Used as a sort-skip predicate by
/// `try_columnar_row_number`.
#[inline]
fn is_non_decreasing(keys: &[i64]) -> bool {
    keys.windows(2).all(|w| w[0] <= w[1])
}

/// Convert a `usize` to `u32`, saturating at `u32::MAX` on overflow.
///
/// The window kernel uses `u32` row indices because every supported
/// shape ships ≤ 2 ³² rows through one operator. A real overflow
/// would corrupt the rank vector; saturation produces a well-defined
/// (incorrect) result that the lowering tests already catch — and the
/// branch-free conversion stays out of the hot inner loops where
/// `as u32` would otherwise need a `try_into()` + panic propagation.
#[inline]
#[allow(
    clippy::cast_possible_truncation,
    reason = "clamped above to u32::MAX before narrowing; documented in the doc comment"
)]
const fn u32_from_usize_clamped(v: usize) -> u32 {
    if v > u32::MAX as usize {
        u32::MAX
    } else {
        v as u32
    }
}

/// Convert a `usize` to `i64` for the rank assignment.
///
/// `i64::MAX` is larger than any usize we will ever see in the window
/// kernel; the clamp branch only fires on a 32-bit host with a
/// >2³¹-row scan (impossible under our current memory layout). The
/// conversion folds away on 64-bit hosts.
#[inline]
#[allow(
    clippy::cast_possible_wrap,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "i64::MAX-as-usize comparison guards the v-as-i64 narrowing below"
)]
const fn i64_from_usize_clamped(v: usize) -> i64 {
    if v > i64::MAX as usize {
        i64::MAX
    } else {
        v as i64
    }
}

/// Scatter ranks `1..=n` into a row-aligned `Vec<i64>`, indexed by the
/// original row index carried in each sorted pair. Returns the rank
/// vector in row order (i.e. `out[orig_idx] = rank`).
#[inline]
fn scatter_rank_from_pairs(sorted: &[(i64, u32)], total: usize) -> Vec<i64> {
    let mut window_col: Vec<i64> = vec![0; total];
    for (pos, &(_, idx)) in sorted.iter().enumerate() {
        window_col[idx as usize] = i64_from_usize_clamped(pos + 1);
    }
    window_col
}

/// Sort `pairs` (key, original-index) in ascending key order, breaking
/// ties on the original index. Uses scoped worker threads to sort
/// disjoint chunks in parallel and a 2-way merge tree to combine the
/// runs.
///
/// # Algorithm
///
/// 1. Split `pairs` into `N` near-equal slices, `N` = a power of two
///    derived from `available_parallelism()` clamped to
///    `[MIN, MAX]`. A power-of-two count makes the merge tree
///    perfectly balanced.
/// 2. Each scoped worker `sort_unstable_by`s its own slice in place.
///    `thread::scope` lends disjoint `&mut [_]`s so the borrow checker
///    sees non-overlapping references.
/// 3. Pairs of sorted runs are 2-way-merged into a scratch buffer,
///    then ping-pong back. After `log₂(N)` passes the entire vector
///    is sorted. Each merge pass is itself parallelised: each pair
///    can be merged in its own worker.
///
/// A 2-way binary merge tree is ~2× faster than an 8-way
/// `BinaryHeap` for this shape because every merge step is a
/// branch-predicted linear scan with sequential reads — the heap's
/// `pop`/`push` chain misses L1 on the way back up the heap.
///
/// # Determinism
///
/// The comparator is `key.cmp().then(idx.cmp())` in every stage. The
/// per-chunk `sort_unstable_by` + the merge-by-min produces output
/// identical to a single-threaded sort with the same comparator,
/// independent of thread scheduling.
///
/// # Safety / soundness
///
/// No `unsafe`, no `Arc<Mutex<…>>`. `thread::scope` enforces the
/// "no worker outlives the borrow" invariant; the merge passes use
/// `split_at_mut` to obtain disjoint chunks of the destination
/// buffer.
fn parallel_sort_pairs(mut pairs: Vec<(i64, u32)>) -> Vec<(i64, u32)> {
    let n = pairs.len();
    let raw_threads = thread::available_parallelism()
        .map_or(PARALLEL_SORT_MIN_THREADS, |nz| nz.get())
        .clamp(PARALLEL_SORT_MIN_THREADS, PARALLEL_SORT_MAX_THREADS);
    // Round down to the next power of two so the 2-way merge tree
    // stays balanced. With `raw_threads` ∈ [2, 8] the candidate set is
    // {2, 4, 8} — three balanced trees.
    let n_threads = if raw_threads >= 8 {
        8
    } else if raw_threads >= 4 {
        4
    } else {
        2
    };
    let chunk_size = n.div_ceil(n_threads);

    // Phase 1: parallel per-chunk sort. `chunk_lens` records the
    // actual length each worker sorted so the merge phase knows
    // where each run ends.
    let mut chunk_lens: Vec<usize> = Vec::with_capacity(n_threads);
    thread::scope(|scope| {
        let mut tail: &mut [(i64, u32)] = &mut pairs;
        let mut handles: Vec<thread::ScopedJoinHandle<'_, ()>> = Vec::with_capacity(n_threads);
        while !tail.is_empty() {
            let take = chunk_size.min(tail.len());
            let (head, rest) = tail.split_at_mut(take);
            tail = rest;
            chunk_lens.push(head.len());
            handles.push(scope.spawn(move || {
                head.sort_unstable_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
            }));
        }
        for h in handles {
            let _ = h.join();
        }
    });

    // Phase 2: 2-way merge tree. We ping-pong between `pairs` (the
    // current "source" runs) and `scratch` (the destination of the
    // current merge pass).
    let scratch: Vec<(i64, u32)> = vec![(0, 0); n];

    // `runs` is the offset list for the current source buffer. After
    // each merge pass we halve its length (pairs of runs are
    // collapsed into single runs).
    let mut runs: Vec<usize> = Vec::with_capacity(chunk_lens.len() + 1);
    runs.push(0);
    for &len in &chunk_lens {
        let prev = runs.last().copied().unwrap_or(0);
        runs.push(prev + len);
    }

    let mut src: Vec<(i64, u32)> = pairs;
    let mut dst: Vec<(i64, u32)> = scratch;
    while runs.len() > 2 {
        // For every adjacent pair (runs[i], runs[i+1], runs[i+2])
        // merge `src[runs[i]..runs[i+1]]` and
        // `src[runs[i+1]..runs[i+2]]` into `dst[runs[i]..runs[i+2]]`.
        // The pairs touch disjoint output windows so they can be
        // merged in parallel under a scope.
        let next_run_count = runs.len().div_ceil(2);
        let mut next_runs: Vec<usize> = Vec::with_capacity(next_run_count);
        next_runs.push(0);

        thread::scope(|scope| {
            let mut remaining: &mut [(i64, u32)] = &mut dst;
            let mut handles: Vec<thread::ScopedJoinHandle<'_, ()>> =
                Vec::with_capacity(runs.len() / 2);
            let mut i = 0;
            while i + 2 < runs.len() {
                let lo = runs[i];
                let mid = runs[i + 1];
                let hi = runs[i + 2];
                let out_len = hi - lo;
                let (out_slice, rest) = remaining.split_at_mut(out_len);
                remaining = rest;
                let left: &[(i64, u32)] = &src[lo..mid];
                let right: &[(i64, u32)] = &src[mid..hi];
                handles.push(scope.spawn(move || {
                    merge_into(left, right, out_slice);
                }));
                next_runs.push(hi);
                i += 2;
            }
            // If `runs.len()` is odd, the trailing single run carries
            // through unchanged.
            if i + 1 < runs.len() {
                let lo = runs[i];
                let hi = runs[i + 1];
                let out_len = hi - lo;
                let (out_slice, rest) = remaining.split_at_mut(out_len);
                remaining = rest;
                out_slice.copy_from_slice(&src[lo..hi]);
                next_runs.push(hi);
                let _ = remaining; // silence unused-var on terminal odd run
            }
            for h in handles {
                let _ = h.join();
            }
        });

        std::mem::swap(&mut src, &mut dst);
        runs = next_runs;
    }

    // `src` now holds the fully merged output.
    src
}

/// Merge two ascending `(key, idx)` runs into `out`. Comparator is
/// `key.cmp().then(idx.cmp())` so this is a stable-w.r.t.-the-
/// comparator equivalent of `sort_unstable_by` on the concatenated
/// runs. `out.len()` must equal `left.len() + right.len()`.
#[inline]
fn merge_into(left: &[(i64, u32)], right: &[(i64, u32)], out: &mut [(i64, u32)]) {
    debug_assert_eq!(out.len(), left.len() + right.len());
    let mut i = 0usize;
    let mut j = 0usize;
    let mut k = 0usize;
    while i < left.len() && j < right.len() {
        let a = left[i];
        let b = right[j];
        // Inline the comparator to keep the hot loop branch-light.
        let take_left = a.0 < b.0 || (a.0 == b.0 && a.1 <= b.1);
        if take_left {
            out[k] = a;
            i += 1;
        } else {
            out[k] = b;
            j += 1;
        }
        k += 1;
    }
    if i < left.len() {
        out[k..].copy_from_slice(&left[i..]);
    } else if j < right.len() {
        out[k..].copy_from_slice(&right[j..]);
    }
}

#[cfg(test)]
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    reason = "tests: ad-hoc index arithmetic against compile-time-known loop bounds"
)]
mod tests {
    use ultrasql_core::{DataType, Field, Schema, Value};
    use ultrasql_planner::ScalarExpr;
    use ultrasql_vec::Batch;
    use ultrasql_vec::column::{Column, NumericColumn};

    use super::{WindowAgg, WindowFunc};
    use crate::Operator;
    use crate::filter_op::batch_to_rows;
    use crate::mem_table_scan::MemTableScan;

    fn schema_id_val() -> Schema {
        Schema::new([
            Field::required("id", DataType::Int32),
            Field::required("val", DataType::Int32),
        ])
        .expect("ok")
    }

    fn schema_with_window() -> Schema {
        Schema::new([
            Field::required("id", DataType::Int32),
            Field::required("val", DataType::Int32),
            Field::required("rn", DataType::Int64),
        ])
        .expect("ok")
    }

    fn make_batch(rows: &[(i32, i32)]) -> Batch {
        Batch::new([
            Column::Int32(NumericColumn::from_data(
                rows.iter().map(|(a, _)| *a).collect(),
            )),
            Column::Int32(NumericColumn::from_data(
                rows.iter().map(|(_, b)| *b).collect(),
            )),
        ])
        .expect("ok")
    }

    fn col_val() -> ScalarExpr {
        ScalarExpr::Column {
            name: "val".into(),
            index: 1,
            data_type: DataType::Int32,
        }
    }

    fn drain_window_col(op: &mut dyn Operator) -> Vec<i64> {
        let schema = op.schema().clone();
        let mut out = Vec::new();
        while let Some(b) = op.next_batch().expect("ok") {
            let rows = batch_to_rows(&b, &schema).expect("decode");
            for row in rows {
                if let Value::Int64(v) = &row[2] {
                    out.push(*v);
                }
            }
        }
        out
    }

    #[test]
    fn window_row_number_ascending() {
        let scan = MemTableScan::new(
            schema_id_val(),
            vec![make_batch(&[(1, 10), (2, 20), (3, 30)])],
        );
        let mut op = WindowAgg::new(
            Box::new(scan),
            vec![],          // no partition
            vec![col_val()], // order by val
            WindowFunc::RowNumber,
            schema_with_window(),
        );
        let rns = drain_window_col(&mut op);
        assert_eq!(rns, vec![1, 2, 3]);
    }

    #[test]
    fn window_dense_rank() {
        // val: 10, 10, 20 → dense ranks: 1, 1, 2
        let scan = MemTableScan::new(
            schema_id_val(),
            vec![make_batch(&[(1, 10), (2, 10), (3, 20)])],
        );
        let mut op = WindowAgg::new(
            Box::new(scan),
            vec![],
            vec![col_val()],
            WindowFunc::DenseRank,
            schema_with_window(),
        );
        let ranks = drain_window_col(&mut op);
        assert_eq!(ranks, vec![1, 1, 2]);
    }

    #[test]
    fn window_ntile_divides_evenly() {
        // 4 rows, ntile(2) → buckets: 1,1,2,2
        let scan = MemTableScan::new(
            schema_id_val(),
            vec![make_batch(&[(1, 1), (2, 2), (3, 3), (4, 4)])],
        );
        let mut op = WindowAgg::new(
            Box::new(scan),
            vec![],
            vec![col_val()],
            WindowFunc::Ntile(2),
            schema_with_window(),
        );
        let buckets = drain_window_col(&mut op);
        assert_eq!(buckets, vec![1, 1, 2, 2]);
    }

    #[test]
    fn parallel_sort_matches_single_threaded_on_random_keys() {
        // Cross the PARALLEL_SORT_THRESHOLD with a pseudo-random key
        // stream and verify the parallel sort + merge tree produces
        // the same output as a single-threaded `sort_unstable_by`.
        let n = super::PARALLEL_SORT_THRESHOLD * 2 + 137;
        // Mixed congruential PRNG — deterministic, no extra-crate
        // dependency. Spread across 1024 distinct key values so ties
        // exercise the index-break path.
        let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
        let pairs: Vec<(i64, u32)> = (0..n)
            .map(|i| {
                state = state
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                let k = (state >> 32) as i64 & 0x3FF;
                (k, i as u32)
            })
            .collect();
        let mut expected = pairs.clone();
        expected.sort_unstable_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
        let actual = super::parallel_sort_pairs(pairs);
        assert_eq!(actual, expected);
    }

    #[test]
    fn parallel_sort_handles_already_sorted_input() {
        let n = super::PARALLEL_SORT_THRESHOLD * 2;
        let pairs: Vec<(i64, u32)> = (0..n).map(|i| (i as i64, i as u32)).collect();
        let expected = pairs.clone();
        let actual = super::parallel_sort_pairs(pairs);
        assert_eq!(actual, expected);
    }

    #[test]
    fn parallel_sort_handles_reverse_sorted_input() {
        let n = super::PARALLEL_SORT_THRESHOLD + 1;
        let pairs: Vec<(i64, u32)> = (0..n).map(|i| ((n - i) as i64, i as u32)).collect();
        let mut expected = pairs.clone();
        expected.sort_unstable_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
        let actual = super::parallel_sort_pairs(pairs);
        assert_eq!(actual, expected);
    }

    #[test]
    fn parallel_sort_handles_all_equal_keys() {
        // Worst-case tie-break load: every key identical, so the
        // comparator falls through to the index break on every
        // compare.
        let n = super::PARALLEL_SORT_THRESHOLD + 8;
        let pairs: Vec<(i64, u32)> = (0..n).map(|i| (42_i64, i as u32)).collect();
        let expected = pairs.clone(); // already ordered by index
        let actual = super::parallel_sort_pairs(pairs);
        assert_eq!(actual, expected);
    }
}
