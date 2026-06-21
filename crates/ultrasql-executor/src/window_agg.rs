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

use std::cmp::Ordering;
use std::collections::{HashMap, VecDeque};
use std::thread;

use ultrasql_core::{Schema, Value};
use ultrasql_planner::ScalarExpr;
use ultrasql_vec::Batch;
use ultrasql_vec::column::{Column, NumericColumn};

use crate::aggregate_math::widen_sum_seed;
use crate::eval::Eval;
use crate::filter_op::batch_to_rows;
use crate::seq_scan::build_batch;
use crate::sort::{compare_f64_sql, compare_non_null_values, compare_values_nullable};
use crate::{ExecError, Operator, eval_error_to_exec_error};

const BATCH_TARGET_ROWS: usize = 4096;

/// Row-count threshold at which the columnar `row_number` fast path
/// switches from a single-threaded `sort_unstable_by` to a chunked
/// parallel sort + 2-way merge tree. Calibrated on Apple M-class
/// silicon: below 16 384 rows the cost of spawning scoped workers
/// dominates the wall-clock saved by the parallel sort.
const PARALLEL_SORT_THRESHOLD: usize = 16 * 1024;

/// Maximum worker count for the parallel sort. Capped at 8 to match
/// the host topologies our benchmark leadership gate targets (4–8
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
    /// `NTH_VALUE(expr, n)` — n-th value (1-based) in the frame.
    NthValue {
        /// The value expression.
        expr: ScalarExpr,
        /// 1-based position.
        n: usize,
    },
    /// `NTILE(n)` — divide the partition into `n` buckets.
    Ntile(usize),
    /// A frame-aware aggregate: `SUM`/`AVG`/`COUNT`/`MIN`/`MAX(expr)`.
    Aggregate {
        /// Which aggregate to compute over the frame.
        kind: WindowAggKind,
        /// The argument expression evaluated per row.
        expr: ScalarExpr,
    },
    /// `COUNT(*)` — counts all rows in the frame.
    CountStar,
}

/// The aggregate kernels usable as frame-aware window functions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowAggKind {
    /// `SUM(expr)`.
    Sum,
    /// `AVG(expr)`.
    Avg,
    /// `COUNT(expr)` — counts non-NULL values in the frame.
    Count,
    /// `MIN(expr)`.
    Min,
    /// `MAX(expr)`.
    Max,
}

/// Frame mode for a window frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameUnits {
    /// `ROWS` — physical row offsets.
    Rows,
    /// `RANGE` — logical offsets by `ORDER BY` value.
    Range,
    /// `GROUPS` — logical offsets by number of peer groups.
    Groups,
}

/// One endpoint of a window frame.
#[derive(Debug, Clone)]
pub enum FrameBound {
    /// `UNBOUNDED PRECEDING`.
    UnboundedPreceding,
    /// `<offset> PRECEDING`.
    Preceding(ScalarExpr),
    /// `CURRENT ROW`.
    CurrentRow,
    /// `<offset> FOLLOWING`.
    Following(ScalarExpr),
    /// `UNBOUNDED FOLLOWING`.
    UnboundedFollowing,
}

/// `EXCLUDE` option on a window frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameExclusion {
    /// `EXCLUDE NO OTHERS` (default).
    NoOthers,
    /// `EXCLUDE CURRENT ROW`.
    CurrentRow,
    /// `EXCLUDE GROUP`.
    Group,
    /// `EXCLUDE TIES`.
    Ties,
}

/// A window frame computed per row by the [`WindowAgg`] kernel.
#[derive(Debug, Clone)]
pub struct FrameSpec {
    /// Frame mode.
    pub units: FrameUnits,
    /// Frame start bound.
    pub start: FrameBound,
    /// Frame end bound.
    pub end: FrameBound,
    /// `EXCLUDE` option.
    pub exclude: FrameExclusion,
}

impl FrameSpec {
    /// The whole-partition default frame: `RANGE BETWEEN UNBOUNDED
    /// PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE NO OTHERS`. Used when a
    /// caller constructs a [`WindowAgg`] without an explicit frame, which
    /// preserves the historical whole-partition behaviour.
    #[must_use]
    pub fn whole_partition() -> Self {
        Self {
            units: FrameUnits::Range,
            start: FrameBound::UnboundedPreceding,
            end: FrameBound::UnboundedFollowing,
            exclude: FrameExclusion::NoOthers,
        }
    }
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
    /// Per-`ORDER BY`-key direction as `(ascending, nulls_first)`, parallel to
    /// `order_keys`. Defaults to `(true, false)` (ASC, NULLS LAST) for every
    /// key; the lowering layer overrides it via
    /// [`WindowAgg::with_order_directions`] so `ORDER BY x DESC` /
    /// `NULLS FIRST` produce the correct ordering instead of being silently
    /// ignored.
    order_directions: Vec<(bool, bool)>,
    /// Expressions for the PARTITION BY keys.
    partition_key_evals: Vec<Eval>,
    /// Expressions for the ORDER BY keys.
    order_key_evals: Vec<Eval>,
    /// The window function.
    func: WindowFunc,
    /// The window frame. Defaults to the whole-partition frame; callers
    /// override it via [`WindowAgg::with_frame`].
    frame: FrameSpec,
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
        // Default every key to ASC / NULLS LAST; callers override via
        // `with_order_directions`.
        let order_directions = vec![(true, false); order_keys.len()];
        Self {
            child,
            partition_keys,
            order_keys,
            order_directions,
            partition_key_evals,
            order_key_evals,
            func,
            frame: FrameSpec::whole_partition(),
            schema,
            child_schema,
            pending: VecDeque::new(),
            primed: false,
            eof: false,
        }
    }

    /// Override the window frame. Without this call the operator uses the
    /// whole-partition frame (`RANGE UNBOUNDED PRECEDING AND UNBOUNDED
    /// FOLLOWING`).
    #[must_use]
    pub fn with_frame(mut self, frame: FrameSpec) -> Self {
        self.frame = frame;
        self
    }

    /// Override the per-`ORDER BY`-key sort direction.
    ///
    /// `directions` is `(ascending, nulls_first)` per key, parallel to the
    /// `order_keys` passed to [`Self::new`]. Lengths shorter than `order_keys`
    /// leave the remaining keys at the default ASC / NULLS LAST; extra entries
    /// are ignored. Without this call the operator sorts every key ascending
    /// with NULLs last, which is why `ORDER BY x DESC` used to be ignored.
    #[must_use]
    pub fn with_order_directions(mut self, directions: Vec<(bool, bool)>) -> Self {
        for (slot, dir) in self.order_directions.iter_mut().zip(directions) {
            *slot = dir;
        }
        self
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

    fn profile_children(&self) -> Vec<&dyn Operator> {
        vec![self.child.as_ref()]
    }

    fn estimated_row_count(&self) -> Option<usize> {
        self.child.estimated_row_count()
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
        // The fast path bakes in ASC / NULLS LAST (NULLs map to an i64::MAX
        // sentinel and the rank is assigned over an ascending sort). For any
        // other direction, fall back to the slow path, which honours
        // `order_directions`.
        if self.order_directions.first().copied() != Some((true, false)) {
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
                    buf.push(eval_window_expr(kv, row)?);
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
            let mut parts: Vec<Vec<usize>> = Vec::new();
            let mut part_by_key: HashMap<Vec<Value>, usize> = HashMap::new();
            for (idx, row) in all_rows.iter().enumerate() {
                let mut key: Vec<Value> = Vec::with_capacity(key_count);
                for kv in &self.partition_key_evals {
                    key.push(eval_window_expr(kv, row)?);
                }
                let part_idx = match part_by_key.entry(key) {
                    std::collections::hash_map::Entry::Occupied(entry) => *entry.get(),
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        let part_idx = parts.len();
                        entry.insert(part_idx);
                        parts.push(Vec::new());
                        part_idx
                    }
                };
                parts[part_idx].push(idx);
            }
            parts
        };

        // One pre-sized output buffer; we drop the window value into
        // the slot owned by each row's *original* index so the final
        // assembly walks `all_rows` once and consumes it.
        let mut window_values: Vec<Value> = vec![Value::Null; n_total];

        // Per-key (ascending, nulls_first) direction; default ASC/NULLS LAST
        // for any key the caller did not specify.
        let order_directions = self.order_directions.clone();

        for partition_indices in &partitions {
            // Sort using the cached order-key buffer. Comparator reads
            // a pre-computed slice instead of calling the interpreter, and
            // applies each key's direction (ASC/DESC, NULLS FIRST/LAST).
            let mut sorted_indices = partition_indices.clone();
            if order_key_count != 0 {
                sorted_indices.sort_by(|&a, &b| {
                    let ka = row_order_key(a);
                    let kb = row_order_key(b);
                    for i in 0..order_key_count {
                        let (asc, nulls_first) =
                            order_directions.get(i).copied().unwrap_or((true, false));
                        // NULL placement is ABSOLUTE: it follows `nulls_first`
                        // directly and is NOT flipped by DESC. Only the
                        // comparison of two non-NULL values is reversed for
                        // DESC. (Reversing the whole `compare_values_nullable`
                        // result would move NULLs to the wrong end, breaking
                        // explicit NULLS FIRST/LAST under DESC.)
                        let ord = order_key_ordering(&ka[i], &kb[i], asc, nulls_first);
                        if ord != std::cmp::Ordering::Equal {
                            return ord;
                        }
                    }
                    std::cmp::Ordering::Equal
                });
            }

            let n = sorted_indices.len();

            // Build peer-group infrastructure once per partition using the
            // exact order-key equality already proven by Rank/DenseRank.
            // `group_of[pos]` is the 0-based peer-group index of the row at
            // sorted position `pos`; `group_bounds[g] = (first_pos,
            // last_pos_exclusive)`. With no ORDER BY every row is one peer
            // group (all rows are peers). This single structure serves
            // RANGE CURRENT ROW, GROUPS offsets, and EXCLUDE TIES/GROUP.
            let (group_of, group_bounds) =
                build_peer_groups(&sorted_indices, order_key_count, &row_order_key);

            // Whether this function needs the per-row frame at all. Ranking
            // and offset functions are frame-insensitive.
            let needs_frame = matches!(
                &self.func,
                WindowFunc::FirstValue(_)
                    | WindowFunc::LastValue(_)
                    | WindowFunc::NthValue { .. }
                    | WindowFunc::Aggregate { .. }
                    | WindowFunc::CountStar
            );

            // Resolve frame offset values (constant per partition) with
            // execution-time validation, then build a per-position frame
            // resolver. Only done when the function is frame-sensitive.
            let frame_ctx = if needs_frame {
                Some(FrameContext::build(
                    &self.frame,
                    &sorted_indices,
                    &group_of,
                    &group_bounds,
                    order_key_count,
                    &order_directions,
                    &row_order_key,
                    &all_rows,
                )?)
            } else {
                None
            };

            let values: Vec<Value> = match &self.func {
                WindowFunc::RowNumber => Ok((1..=n)
                    .map(|i| Value::Int64(i64_from_usize_clamped(i)))
                    .collect()),
                WindowFunc::Rank => {
                    let mut out_ranks = vec![1_i64; n];
                    let mut base_rank = 1_usize;
                    let mut prev_pos: Option<usize> = None;
                    for (pos, &idx) in sorted_indices.iter().enumerate() {
                        let same = prev_pos
                            .map(|p| {
                                same_order_key(row_order_key(sorted_indices[p]), row_order_key(idx))
                            })
                            .unwrap_or(false);
                        if !same {
                            base_rank = pos + 1;
                            prev_pos = Some(pos);
                        }
                        out_ranks[pos] = i64_from_usize_clamped(base_rank);
                    }
                    Ok(out_ranks.into_iter().map(Value::Int64).collect())
                }
                WindowFunc::DenseRank => {
                    let mut out = Vec::with_capacity(n);
                    let mut dense = 1_i64;
                    let mut prev_pos: Option<usize> = None;
                    for (pos, &idx) in sorted_indices.iter().enumerate() {
                        let same = prev_pos
                            .map(|p| {
                                same_order_key(row_order_key(sorted_indices[p]), row_order_key(idx))
                            })
                            .unwrap_or(false);
                        if !same {
                            if prev_pos.is_some() {
                                dense += 1;
                            }
                            prev_pos = Some(pos);
                        }
                        out.push(Value::Int64(dense));
                    }
                    Ok(out)
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
                                Ok(default.clone())
                            } else {
                                let prev_idx = sorted_indices[pos - offset];
                                eval_window_expr(&interp, &all_rows[prev_idx])
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
                                Ok(default.clone())
                            } else {
                                let next_idx = sorted_indices[pos + offset];
                                eval_window_expr(&interp, &all_rows[next_idx])
                            }
                        })
                        .collect()
                }
                WindowFunc::FirstValue(expr) => {
                    let interp = Eval::new(expr.clone());
                    let ctx = frame_ctx
                        .as_ref()
                        .ok_or(ExecError::Internal("window frame context missing"))?;
                    (0..n)
                        .map(|pos| match ctx.first_included(pos) {
                            Some(idx) => eval_window_expr(&interp, &all_rows[idx]),
                            None => Ok(Value::Null),
                        })
                        .collect()
                }
                WindowFunc::LastValue(expr) => {
                    let interp = Eval::new(expr.clone());
                    let ctx = frame_ctx
                        .as_ref()
                        .ok_or(ExecError::Internal("window frame context missing"))?;
                    (0..n)
                        .map(|pos| match ctx.last_included(pos) {
                            Some(idx) => eval_window_expr(&interp, &all_rows[idx]),
                            None => Ok(Value::Null),
                        })
                        .collect()
                }
                WindowFunc::NthValue { expr, n: nth } => {
                    let interp = Eval::new(expr.clone());
                    let nth = *nth;
                    let ctx = frame_ctx
                        .as_ref()
                        .ok_or(ExecError::Internal("window frame context missing"))?;
                    (0..n)
                        .map(|pos| match ctx.nth_included(pos, nth) {
                            Some(idx) => eval_window_expr(&interp, &all_rows[idx]),
                            None => Ok(Value::Null),
                        })
                        .collect()
                }
                WindowFunc::Aggregate { kind, expr } => {
                    let interp = Eval::new(expr.clone());
                    let ctx = frame_ctx
                        .as_ref()
                        .ok_or(ExecError::Internal("window frame context missing"))?;
                    (0..n)
                        .map(|pos| {
                            frame_aggregate(*kind, &interp, ctx, pos, &sorted_indices, &all_rows)
                        })
                        .collect()
                }
                WindowFunc::CountStar => {
                    let ctx = frame_ctx
                        .as_ref()
                        .ok_or(ExecError::Internal("window frame context missing"))?;
                    Ok((0..n)
                        .map(|pos| Value::Int64(i64_from_usize_clamped(ctx.included_count(pos))))
                        .collect())
                }
                WindowFunc::Ntile(bucket_count) => {
                    let bucket_count = *bucket_count;
                    Ok((0..n)
                        .map(|pos| {
                            let bucket = if bucket_count == 0 {
                                1
                            } else {
                                (pos * bucket_count) / n + 1
                            };
                            Value::Int64(i64_from_usize_clamped(bucket))
                        })
                        .collect())
                }
            }?;

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

fn eval_window_expr(eval: &Eval, row: &[Value]) -> Result<Value, ExecError> {
    eval.eval(row).map_err(eval_error_to_exec_error)
}

/// Build per-partition peer-group structure from the sorted order.
///
/// Returns `(group_of, group_bounds)` where `group_of[pos]` is the
/// 0-based peer-group index of the row at sorted position `pos`, and
/// `group_bounds[g] = (first_pos, last_pos_exclusive)`. Two adjacent
/// positions are in the same group iff their order keys are equal (the
/// same equality Rank/DenseRank already use). With no ORDER BY the whole
/// partition is one peer group.
fn build_peer_groups<'a>(
    sorted_indices: &[usize],
    order_key_count: usize,
    row_order_key: &dyn Fn(usize) -> &'a [Value],
) -> (Vec<usize>, Vec<(usize, usize)>) {
    let n = sorted_indices.len();
    let mut group_of = vec![0_usize; n];
    let mut group_bounds: Vec<(usize, usize)> = Vec::new();
    if n == 0 {
        return (group_of, group_bounds);
    }
    let mut cur_group = 0_usize;
    let mut group_start = 0_usize;
    group_of[0] = 0;
    for pos in 1..n {
        let same = order_key_count != 0
            && same_order_key(
                row_order_key(sorted_indices[pos - 1]),
                row_order_key(sorted_indices[pos]),
            );
        if !same {
            group_bounds.push((group_start, pos));
            cur_group += 1;
            group_start = pos;
        }
        group_of[pos] = cur_group;
    }
    group_bounds.push((group_start, n));
    (group_of, group_bounds)
}

/// Order two window ORDER-BY key values honouring ASC/DESC and absolute
/// NULL placement.
///
/// NULL placement is governed solely by `nulls_first` (NULL-vs-non-NULL
/// and NULL-vs-NULL), independent of `asc`. Only the comparison between two
/// non-NULL values is reversed for DESC. This matches PostgreSQL, where the
/// direction reverses the value order but NULLS FIRST/LAST is an absolute
/// placement of NULLs at one end of the result.
fn order_key_ordering(a: &Value, b: &Value, asc: bool, nulls_first: bool) -> Ordering {
    match (a.is_null(), b.is_null()) {
        (true, true) => Ordering::Equal,
        (true, false) => {
            if nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        (false, true) => {
            if nulls_first {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        (false, false) => {
            let ord = compare_non_null_values(a, b).unwrap_or(Ordering::Equal);
            if asc { ord } else { ord.reverse() }
        }
    }
}

/// Whether two ORDER-BY key slices are peers (equal under the SAME SQL
/// comparator the sort uses).
///
/// Uses `compare_values_nullable` per key rather than structural `Vec<Value>`
/// equality so that the peer relation agrees with the sort: `NaN` compares
/// equal to `NaN` (so adjacent NaN rows form one peer group), and NULL equals
/// NULL. `nulls_first` is irrelevant here because equality is symmetric.
fn same_order_key(a: &[Value], b: &[Value]) -> bool {
    a.len() == b.len()
        && a.iter()
            .zip(b.iter())
            .all(|(x, y)| compare_values_nullable(x, y, false) == std::cmp::Ordering::Equal)
}

/// Per-partition resolver for the window frame. Holds the resolved
/// `[frame_start, frame_end)` position range for each sorted position
/// plus the peer-group structure, and answers membership queries that
/// honour the `EXCLUDE` option.
struct FrameContext<'a> {
    /// `(frame_start, frame_end_exclusive)` position range per sorted pos.
    bounds: Vec<(usize, usize)>,
    /// Peer-group index per sorted position.
    group_of: &'a [usize],
    /// `EXCLUDE` option.
    exclude: FrameExclusion,
    /// Sorted row indices (position -> original row index).
    sorted_indices: &'a [usize],
}

impl<'a> FrameContext<'a> {
    #[allow(clippy::too_many_arguments)]
    fn build<'b>(
        frame: &FrameSpec,
        sorted_indices: &'a [usize],
        group_of: &'a [usize],
        group_bounds: &'a [(usize, usize)],
        order_key_count: usize,
        order_directions: &[(bool, bool)],
        row_order_key: &dyn Fn(usize) -> &'b [Value],
        all_rows: &[Vec<Value>],
    ) -> Result<Self, ExecError> {
        let n = sorted_indices.len();
        let _ = order_key_count;

        // For RANGE value offsets, precompute the single order-key value per
        // sorted position and the ASC flag from the (single) ORDER BY
        // direction. The membership test must use EXACT arithmetic in the
        // ORDER BY column's own type (Int64 via i128, Decimal via scale-
        // aligned i128); computing the bound in f64 would drop exact boundary
        // peers (e.g. NUMERIC 0.4 - 0.1 = 0.30000000000000004). f64 is used
        // only as a fallback for genuine Float32/Float64 ORDER BY columns.
        let range_offset = matches!(frame.units, FrameUnits::Range)
            && (bound_has_offset(&frame.start) || bound_has_offset(&frame.end));
        let asc = order_directions.first().is_none_or(|d| d.0);

        // Resolve the ROWS/GROUPS offset magnitudes once, with execution-time
        // validation (NULL / negative).
        let start_rows_off = match &frame.start {
            FrameBound::Preceding(e) | FrameBound::Following(e)
                if matches!(frame.units, FrameUnits::Rows | FrameUnits::Groups) =>
            {
                Some(eval_rows_offset(e, all_rows, sorted_indices, true)?)
            }
            _ => None,
        };
        let end_rows_off = match &frame.end {
            FrameBound::Preceding(e) | FrameBound::Following(e)
                if matches!(frame.units, FrameUnits::Rows | FrameUnits::Groups) =>
            {
                Some(eval_rows_offset(e, all_rows, sorted_indices, false)?)
            }
            _ => None,
        };

        // For RANGE value offsets, work in EXACT arithmetic in the ORDER BY
        // column's own type (Int64 via i128, Decimal via scale-aligned i128)
        // so the bound `cur ± off` and the membership test keep exact boundary
        // peers (computing the bound in f64 would drop e.g. NUMERIC 0.4 - 0.1).
        // f64 is used only as a fallback for genuine Float32/Float64 columns.
        let mut range_vals: Vec<Option<RangeScalar>> = Vec::new();
        let mut start_range_off: Option<RangeScalar> = None;
        let mut end_range_off: Option<RangeScalar> = None;
        if range_offset {
            // Pre-evaluate the raw offset Values so the axis scale covers both
            // the ordering values AND the offsets (max decimal scale seen).
            let raw_start_off = match &frame.start {
                FrameBound::Preceding(e) | FrameBound::Following(e) => {
                    Some(eval_raw_range_offset(e, all_rows, sorted_indices, true)?)
                }
                _ => None,
            };
            let raw_end_off = match &frame.end {
                FrameBound::Preceding(e) | FrameBound::Following(e) => {
                    Some(eval_raw_range_offset(e, all_rows, sorted_indices, false)?)
                }
                _ => None,
            };
            let order_values: Vec<Option<&Value>> = (0..n)
                .map(|pos| {
                    row_order_key(sorted_indices[pos])
                        .first()
                        .filter(|v| !v.is_null())
                })
                .collect();
            let extra_scales = [raw_start_off.as_ref(), raw_end_off.as_ref()];
            let axis = RangeAxis::infer(&order_values, extra_scales.into_iter().flatten())?;
            range_vals = order_values
                .iter()
                .map(|v| match v {
                    Some(value) => axis.value_to_scalar(value).map(Some),
                    None => Ok(None),
                })
                .collect::<Result<_, _>>()?;
            if let Some(v) = &raw_start_off {
                let s = axis.value_to_scalar(v)?;
                if s.is_negative() {
                    return Err(ExecError::WindowFrameError(
                        "invalid preceding or following size in window function".to_string(),
                    ));
                }
                start_range_off = Some(s);
            }
            if let Some(v) = &raw_end_off {
                let s = axis.value_to_scalar(v)?;
                if s.is_negative() {
                    return Err(ExecError::WindowFrameError(
                        "invalid preceding or following size in window function".to_string(),
                    ));
                }
                end_range_off = Some(s);
            }
        }

        let mut bounds = Vec::with_capacity(n);
        for pos in 0..n {
            let fs = resolve_frame_pos(
                &frame.start,
                frame.units,
                BoundSide::Start,
                pos,
                n,
                group_of,
                group_bounds,
                &range_vals,
                asc,
                start_rows_off,
                start_range_off,
            );
            let fe = resolve_frame_pos(
                &frame.end,
                frame.units,
                BoundSide::End,
                pos,
                n,
                group_of,
                group_bounds,
                &range_vals,
                asc,
                end_rows_off,
                end_range_off,
            );
            // An inverted frame is empty; never let fe < fs.
            bounds.push((fs.min(n), fe.min(n).max(fs.min(n))));
        }

        Ok(Self {
            bounds,
            group_of,
            exclude: frame.exclude,
            sorted_indices,
        })
    }

    /// Whether sorted position `p` is included in the frame of row at
    /// position `pos`, honouring the `EXCLUDE` option.
    fn included(&self, pos: usize, p: usize) -> bool {
        let (fs, fe) = self.bounds[pos];
        if p < fs || p >= fe {
            return false;
        }
        match self.exclude {
            FrameExclusion::NoOthers => true,
            FrameExclusion::CurrentRow => p != pos,
            FrameExclusion::Group => self.group_of[p] != self.group_of[pos],
            FrameExclusion::Ties => p == pos || self.group_of[p] != self.group_of[pos],
        }
    }

    /// Number of rows included in the frame of `pos`.
    fn included_count(&self, pos: usize) -> usize {
        let (fs, fe) = self.bounds[pos];
        (fs..fe).filter(|&p| self.included(pos, p)).count()
    }

    /// Original row index of the first included row in the frame of `pos`.
    fn first_included(&self, pos: usize) -> Option<usize> {
        let (fs, fe) = self.bounds[pos];
        (fs..fe)
            .find(|&p| self.included(pos, p))
            .map(|p| self.sorted_indices[p])
    }

    /// Original row index of the last included row in the frame of `pos`.
    fn last_included(&self, pos: usize) -> Option<usize> {
        let (fs, fe) = self.bounds[pos];
        (fs..fe)
            .rev()
            .find(|&p| self.included(pos, p))
            .map(|p| self.sorted_indices[p])
    }

    /// Original row index of the `nth` (1-based) included row in the frame
    /// of `pos`, or `None` when the frame has fewer than `nth` rows.
    fn nth_included(&self, pos: usize, nth: usize) -> Option<usize> {
        if nth == 0 {
            return None;
        }
        let (fs, fe) = self.bounds[pos];
        (fs..fe)
            .filter(|&p| self.included(pos, p))
            .nth(nth - 1)
            .map(|p| self.sorted_indices[p])
    }
}

/// `true` for offset frame bounds (`<offset> PRECEDING/FOLLOWING`).
fn bound_has_offset(bound: &FrameBound) -> bool {
    matches!(bound, FrameBound::Preceding(_) | FrameBound::Following(_))
}

/// Coerce a numeric `Value` to `f64` for RANGE value-offset arithmetic.
/// Returns `None` for NULL or non-numeric values (which form their own
/// peer set under RANGE offsets).
fn value_to_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Int16(x) => Some(f64::from(*x)),
        Value::Int32(x) => Some(f64::from(*x)),
        Value::Int64(x) => Some(i64_to_f64(*x)),
        Value::Float32(x) => Some(f64::from(*x)),
        Value::Float64(x) => Some(*x),
        Value::Decimal { value, scale } => Some(i64_to_f64(*value) / 10_f64.powi(*scale)),
        _ => None,
    }
}

/// Convert `i64` to `f64` without a lossy-cast lint trip.
fn i64_to_f64(v: i64) -> f64 {
    use num_traits::ToPrimitive;
    v.to_f64()
        .unwrap_or(if v < 0 { f64::MIN } else { f64::MAX })
}

/// A RANGE ordering value or offset in the ORDER BY column's exact-arithmetic
/// domain. `Int` holds a scale-aligned `i128` (used for Int* and Decimal
/// columns, where the bound `cur ± off` must be exact to keep boundary peers);
/// `Float` holds an `f64` (fallback for genuine Float32/Float64 columns).
#[derive(Clone, Copy)]
enum RangeScalar {
    Int(i128),
    Float(f64),
}

impl RangeScalar {
    fn is_negative(self) -> bool {
        match self {
            RangeScalar::Int(v) => v < 0,
            RangeScalar::Float(v) => v < 0.0,
        }
    }

    /// `self - other`, saturating on i128 overflow.
    fn sub(self, other: RangeScalar) -> RangeScalar {
        match (self, other) {
            (RangeScalar::Int(a), RangeScalar::Int(b)) => RangeScalar::Int(a.saturating_sub(b)),
            (RangeScalar::Float(a), RangeScalar::Float(b)) => RangeScalar::Float(a - b),
            // Mixed variants never occur: the axis fixes one domain per
            // partition for both values and offset. Degrade to float.
            (a, b) => RangeScalar::Float(a.as_f64() - b.as_f64()),
        }
    }

    /// `self + other`, saturating on i128 overflow.
    fn add(self, other: RangeScalar) -> RangeScalar {
        match (self, other) {
            (RangeScalar::Int(a), RangeScalar::Int(b)) => RangeScalar::Int(a.saturating_add(b)),
            (RangeScalar::Float(a), RangeScalar::Float(b)) => RangeScalar::Float(a + b),
            (a, b) => RangeScalar::Float(a.as_f64() + b.as_f64()),
        }
    }

    fn as_f64(self) -> f64 {
        match self {
            RangeScalar::Int(v) => v.to_f64_lossy(),
            RangeScalar::Float(v) => v,
        }
    }

    /// SQL-compatible ordering (NaN handled like the sort comparator).
    fn cmp(self, other: RangeScalar) -> Ordering {
        match (self, other) {
            (RangeScalar::Int(a), RangeScalar::Int(b)) => a.cmp(&b),
            _ => compare_f64_sql(self.as_f64(), other.as_f64()),
        }
    }
}

trait I128ToF64 {
    fn to_f64_lossy(self) -> f64;
}

impl I128ToF64 for i128 {
    fn to_f64_lossy(self) -> f64 {
        use num_traits::ToPrimitive;
        self.to_f64()
            .unwrap_or(if self < 0 { f64::MIN } else { f64::MAX })
    }
}

/// The exact-arithmetic domain for a partition's RANGE ORDER BY column.
///
/// `Int { scale }` aligns every Int*/Decimal value and the offset to a common
/// decimal `scale` represented as `i128`, so `cur ± off` and the membership
/// test are exact. `Float` is the fallback for Float32/Float64 columns.
enum RangeAxis {
    Int { scale: u32 },
    Float,
}

impl RangeAxis {
    /// Infer the domain from the partition's non-null ordering values and the
    /// offset value(s). Picks exact i128 for Int*/Decimal columns (common
    /// scale = max decimal scale seen across values AND offsets), and float
    /// for Float32/Float64. Ordering-value NULLs are pre-filtered.
    fn infer<'v>(
        order_values: &[Option<&Value>],
        offsets: impl Iterator<Item = &'v Value>,
    ) -> Result<RangeAxis, ExecError> {
        let mut saw_float = false;
        let mut saw_exact = false;
        let mut max_scale: u32 = 0;
        let mut fold = |v: &Value| -> Result<(), ExecError> {
            match v {
                Value::Int16(_) | Value::Int32(_) | Value::Int64(_) => saw_exact = true,
                Value::Decimal { scale, .. } => {
                    saw_exact = true;
                    if *scale > 0 {
                        max_scale = max_scale.max(u32::try_from(*scale).unwrap_or(0));
                    }
                }
                Value::Float32(_) | Value::Float64(_) => saw_float = true,
                _ => {
                    return Err(ExecError::WindowFrameError(
                        "RANGE offset requires a numeric ORDER BY column".to_string(),
                    ));
                }
            }
            Ok(())
        };
        for v in order_values.iter().flatten() {
            fold(v)?;
        }
        for v in offsets {
            fold(v)?;
        }
        // A column should be homogeneous; if floats are present, use float.
        if saw_float || !saw_exact {
            Ok(RangeAxis::Float)
        } else {
            Ok(RangeAxis::Int { scale: max_scale })
        }
    }

    /// Convert a numeric `Value` (the ordering value or the offset) into a
    /// [`RangeScalar`] in this axis. Errors on non-numeric values.
    fn value_to_scalar(&self, v: &Value) -> Result<RangeScalar, ExecError> {
        match self {
            RangeAxis::Float => value_to_f64(v).map(RangeScalar::Float).ok_or_else(|| {
                ExecError::WindowFrameError(
                    "invalid preceding or following size in window function".to_string(),
                )
            }),
            RangeAxis::Int { scale } => {
                let aligned = match v {
                    Value::Int16(x) => i128::from(*x).checked_mul(pow10_i128(*scale)),
                    Value::Int32(x) => i128::from(*x).checked_mul(pow10_i128(*scale)),
                    Value::Int64(x) => i128::from(*x).checked_mul(pow10_i128(*scale)),
                    Value::Decimal {
                        value,
                        scale: vscale,
                    } => {
                        // The axis scale is the max decimal scale across all
                        // values and offsets, so `vscale <= scale` always
                        // holds and the rescale is an exact upscale.
                        let vscale = u32::try_from((*vscale).max(0)).unwrap_or(0);
                        let up = scale.saturating_sub(vscale);
                        i128::from(*value).checked_mul(pow10_i128(up))
                    }
                    _ => {
                        return Err(ExecError::WindowFrameError(
                            "invalid preceding or following size in window function".to_string(),
                        ));
                    }
                };
                aligned.map(RangeScalar::Int).ok_or_else(|| {
                    ExecError::WindowFrameError("RANGE offset arithmetic overflow".to_string())
                })
            }
        }
    }
}

/// `10^exp` as `i128`, saturating on overflow (exp is a small decimal scale).
fn pow10_i128(exp: u32) -> i128 {
    (0..exp).fold(1_i128, |acc, _| acc.saturating_mul(10))
}

/// Evaluate a constant-per-partition frame offset expression and validate
/// it for ROWS/GROUPS use (round to bigint, reject NULL/negative).
fn eval_rows_offset(
    expr: &ScalarExpr,
    all_rows: &[Vec<Value>],
    sorted_indices: &[usize],
    is_start: bool,
) -> Result<usize, ExecError> {
    let probe_row = sorted_indices
        .first()
        .map(|&i| all_rows[i].as_slice())
        .unwrap_or(&[]);
    let value = Eval::new(expr.clone())
        .eval(probe_row)
        .map_err(eval_error_to_exec_error)?;
    let which = if is_start { "starting" } else { "ending" };
    if value.is_null() {
        return Err(ExecError::WindowFrameError(format!(
            "frame {which} offset must not be null"
        )));
    }
    let rounded = value_to_f64(&value)
        .ok_or_else(|| {
            ExecError::WindowFrameError(format!("frame {which} offset must be a number"))
        })?
        .round();
    if rounded < 0.0 {
        return Err(ExecError::WindowFrameError(format!(
            "frame {which} offset must not be negative"
        )));
    }
    Ok(f64_to_usize(rounded))
}

/// Evaluate a constant-per-partition RANGE value offset, rejecting NULL.
/// Returns the raw `Value` so the caller can fold its decimal scale into the
/// partition's exact-arithmetic axis before converting it to a [`RangeScalar`].
fn eval_raw_range_offset(
    expr: &ScalarExpr,
    all_rows: &[Vec<Value>],
    sorted_indices: &[usize],
    is_start: bool,
) -> Result<Value, ExecError> {
    let probe_row = sorted_indices
        .first()
        .map(|&i| all_rows[i].as_slice())
        .unwrap_or(&[]);
    let value = Eval::new(expr.clone())
        .eval(probe_row)
        .map_err(eval_error_to_exec_error)?;
    if value.is_null() {
        // SQLSTATE 22013. Distinguish starting vs ending offset (BUG 4a).
        let which = if is_start { "starting" } else { "ending" };
        return Err(ExecError::WindowFrameError(format!(
            "frame {which} offset must not be null"
        )));
    }
    Ok(value)
}

/// Convert a non-negative `f64` to `usize`, saturating on overflow.
fn f64_to_usize(v: f64) -> usize {
    use num_traits::ToPrimitive;
    v.to_usize().unwrap_or(usize::MAX)
}

/// Which side of the frame a bound is being resolved for. A START bound
/// returns the inclusive first position; an END bound returns the
/// exclusive one-past-last position.
#[derive(Clone, Copy, PartialEq, Eq)]
enum BoundSide {
    Start,
    End,
}

/// Resolve one frame bound to a position into the sorted partition.
///
/// For `BoundSide::Start` the result is the inclusive first position;
/// for `BoundSide::End` it is the exclusive position one past the last
/// included row. Offset magnitudes (`rows_off`, `range_off`) are the
/// already-validated per-partition constants.
#[allow(clippy::too_many_arguments)]
fn resolve_frame_pos(
    bound: &FrameBound,
    units: FrameUnits,
    side: BoundSide,
    pos: usize,
    n: usize,
    group_of: &[usize],
    group_bounds: &[(usize, usize)],
    range_vals: &[Option<RangeScalar>],
    asc: bool,
    rows_off: Option<usize>,
    range_off: Option<RangeScalar>,
) -> usize {
    match bound {
        FrameBound::UnboundedPreceding => 0,
        FrameBound::UnboundedFollowing => n,
        FrameBound::CurrentRow => match units {
            FrameUnits::Rows => {
                if side == BoundSide::Start {
                    pos
                } else {
                    pos + 1
                }
            }
            // RANGE / GROUPS CURRENT ROW = the current peer group.
            FrameUnits::Range | FrameUnits::Groups => {
                let (gs, ge) = group_bounds[group_of[pos]];
                if side == BoundSide::Start { gs } else { ge }
            }
        },
        FrameBound::Preceding(_) | FrameBound::Following(_) => {
            let following = matches!(bound, FrameBound::Following(_));
            match units {
                FrameUnits::Rows => {
                    let off = rows_off.unwrap_or(0);
                    resolve_rows_offset(pos, n, side, following, off)
                }
                FrameUnits::Groups => {
                    let off = rows_off.unwrap_or(0);
                    resolve_groups_offset(pos, n, side, following, off, group_of, group_bounds)
                }
                FrameUnits::Range => {
                    let off = range_off.unwrap_or(RangeScalar::Int(0));
                    resolve_range_offset(pos, n, side, following, off, range_vals, asc)
                }
            }
        }
    }
}

/// ROWS `<off> PRECEDING/FOLLOWING` position resolution.
fn resolve_rows_offset(
    pos: usize,
    n: usize,
    side: BoundSide,
    following: bool,
    off: usize,
) -> usize {
    let target = if following {
        pos.saturating_add(off)
    } else {
        pos.saturating_sub(off)
    };
    match side {
        BoundSide::Start => target.min(n),
        // End bound is exclusive: one past the target row.
        BoundSide::End => target.saturating_add(1).min(n),
    }
}

/// GROUPS `<off> PRECEDING/FOLLOWING` position resolution. The offset
/// counts whole peer groups; the result is the row-span of the shifted
/// group clamped into the partition.
fn resolve_groups_offset(
    pos: usize,
    n: usize,
    side: BoundSide,
    following: bool,
    off: usize,
    group_of: &[usize],
    group_bounds: &[(usize, usize)],
) -> usize {
    let cur = group_of[pos];
    let group_count = group_bounds.len();
    if following {
        let target = cur.saturating_add(off);
        if target >= group_count {
            // Past the last group.
            return n;
        }
        let (gs, ge) = group_bounds[target];
        if side == BoundSide::Start { gs } else { ge }
    } else {
        // `<off> PRECEDING`: the target group is `cur - off`. When `off`
        // exceeds the current group index the target underflows below group 0.
        let underflow = off > cur;
        if underflow {
            // The target group is before the first group. For the START side
            // the frame still begins at the partition start; for the END side
            // the target group does not exist, so the frame end must collapse.
            // Returning 0 for either side yields an EMPTY frame after the
            // `fe.max(fs)` clamp at the call site (PostgreSQL: an all-PRECEDING
            // frame whose end group is below 0 selects no rows). Previously the
            // END side returned `group_bounds[cur.saturating_sub(off)]`, i.e.
            // group 0, leaking the leading group into the frame.
            return 0;
        }
        let target = cur - off;
        let (gs, ge) = group_bounds[target];
        match side {
            BoundSide::Start => gs,
            BoundSide::End => ge,
        }
    }
}

/// RANGE `<off> PRECEDING/FOLLOWING` (numeric value offset) resolution.
///
/// Includes every row whose ordering value lies within `[v - off, v +
/// off]` of the current row's value (direction adjusted for ASC/DESC).
/// Rows with a NULL ordering value form their own peer set: a NULL
/// current row frames only over the other NULL rows.
#[allow(clippy::too_many_arguments)]
fn resolve_range_offset(
    pos: usize,
    n: usize,
    side: BoundSide,
    following: bool,
    off: RangeScalar,
    range_vals: &[Option<RangeScalar>],
    asc: bool,
) -> usize {
    let Some(cur) = range_vals[pos] else {
        // NULL current row: its frame is exactly the contiguous NULL run.
        let mut lo = pos;
        while lo > 0 && range_vals[lo - 1].is_none() {
            lo -= 1;
        }
        let mut hi = pos + 1;
        while hi < n && range_vals[hi].is_none() {
            hi += 1;
        }
        return if side == BoundSide::Start { lo } else { hi };
    };

    // The frame value-window bound, computed in EXACT arithmetic in the
    // ORDER BY column's own type. With ASC ordering, `<off> PRECEDING` admits
    // values >= v-off and `<off> FOLLOWING` admits values <= v+off. DESC flips
    // the direction of "preceding"/"following" in value space.
    let preceding = !following;
    let bound_value = if asc {
        if preceding {
            cur.sub(off)
        } else {
            cur.add(off)
        }
    } else if preceding {
        cur.add(off)
    } else {
        cur.sub(off)
    };

    // Scan positions that carry a non-null value, respecting the sort
    // direction, to find the contiguous run admitted by the value bound.
    // Because the partition is sorted on the single key, the admitted set
    // is contiguous among the non-null rows.
    match side {
        BoundSide::Start => {
            // First position whose value is within the lower edge of the
            // admitted window.
            let mut p = 0;
            while p < n {
                match range_vals[p] {
                    Some(val) if value_in_start_window(val, bound_value, asc) => break,
                    _ => p += 1,
                }
            }
            p.min(n)
        }
        BoundSide::End => {
            // One past the last position still inside the window.
            let mut p = n;
            while p > 0 {
                match range_vals[p - 1] {
                    Some(val) if value_in_end_window(val, bound_value, asc) => break,
                    _ => p -= 1,
                }
            }
            p
        }
    }
}

/// Whether `val` is at or beyond the START edge of a RANGE value window.
/// ASC admits `val >= bound`; DESC admits `val <= bound`. Inclusive boundary.
fn value_in_start_window(val: RangeScalar, bound_value: RangeScalar, asc: bool) -> bool {
    if asc {
        val.cmp(bound_value) != Ordering::Less
    } else {
        val.cmp(bound_value) != Ordering::Greater
    }
}

/// Whether `val` is at or within the END edge of a RANGE value window.
/// ASC admits `val <= bound`; DESC admits `val >= bound`. Inclusive boundary.
fn value_in_end_window(val: RangeScalar, bound_value: RangeScalar, asc: bool) -> bool {
    if asc {
        val.cmp(bound_value) != Ordering::Greater
    } else {
        val.cmp(bound_value) != Ordering::Less
    }
}

/// Compute one frame-relative aggregate value for `pos` by folding the
/// included rows of the frame through the shared aggregate kernels.
fn frame_aggregate(
    kind: WindowAggKind,
    interp: &Eval,
    ctx: &FrameContext<'_>,
    pos: usize,
    sorted_indices: &[usize],
    all_rows: &[Vec<Value>],
) -> Result<Value, ExecError> {
    use crate::hash_aggregate::arith::{add_values, divide_value, value_lt};
    let (fs, fe) = ctx.bounds[pos];

    let mut sum_acc: Option<Value> = None;
    let mut count: i64 = 0;
    let mut min_acc: Option<Value> = None;
    let mut max_acc: Option<Value> = None;

    for p in fs..fe {
        if !ctx.included(pos, p) {
            continue;
        }
        let v = eval_window_expr(interp, &all_rows[sorted_indices[p]])?;
        if v.is_null() {
            continue;
        }
        match kind {
            WindowAggKind::Count => count += 1,
            WindowAggKind::Sum | WindowAggKind::Avg => {
                count += 1;
                sum_acc = Some(match sum_acc.take() {
                    None => widen_sum_seed(v),
                    Some(e) => add_values(e, v)?,
                });
            }
            WindowAggKind::Min => {
                min_acc = Some(match min_acc.take() {
                    None => v,
                    Some(e) => {
                        if value_lt(&v, &e) {
                            v
                        } else {
                            e
                        }
                    }
                });
            }
            WindowAggKind::Max => {
                max_acc = Some(match max_acc.take() {
                    None => v,
                    Some(e) => {
                        if value_lt(&e, &v) {
                            v
                        } else {
                            e
                        }
                    }
                });
            }
        }
    }

    Ok(match kind {
        WindowAggKind::Count => Value::Int64(count),
        WindowAggKind::Sum => sum_acc.unwrap_or(Value::Null),
        WindowAggKind::Avg => {
            if count == 0 {
                Value::Null
            } else {
                sum_acc.map_or(Value::Null, |s| divide_value(s, count))
            }
        }
        WindowAggKind::Min => min_acc.unwrap_or(Value::Null),
        WindowAggKind::Max => max_acc.unwrap_or(Value::Null),
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
/// (incorrect) result that the lowering tests already catch. This
/// checked conversion stays out of the hot inner loops.
#[inline]
fn u32_from_usize_clamped(v: usize) -> u32 {
    u32::try_from(v).unwrap_or(u32::MAX)
}

/// Convert a `usize` to `i64` for the rank assignment.
///
/// `i64::MAX` is larger than any usize we will ever see in the window
/// kernel; the clamp branch only fires on a 32-bit host with a
/// >2³¹-row scan (impossible under our current memory layout). The
/// conversion folds away on 64-bit hosts.
#[inline]
fn i64_from_usize_clamped(v: usize) -> i64 {
    i64::try_from(v).unwrap_or(i64::MAX)
}

/// Convert a `u32` row index to `usize` for vector addressing.
#[inline]
fn usize_from_u32(v: u32) -> usize {
    usize::try_from(v).unwrap_or(usize::MAX)
}

/// Scatter ranks `1..=n` into a row-aligned `Vec<i64>`, indexed by the
/// original row index carried in each sorted pair. Returns the rank
/// vector in row order (i.e. `out[orig_idx] = rank`).
#[inline]
fn scatter_rank_from_pairs(sorted: &[(i64, u32)], total: usize) -> Vec<i64> {
    let mut window_col: Vec<i64> = vec![0; total];
    for (pos, &(_, idx)) in sorted.iter().enumerate() {
        window_col[usize_from_u32(idx)] = i64_from_usize_clamped(pos + 1);
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
/// A two-way binary merge tree is materially faster than an eight-way
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
/// No raw memory code, no `Arc<Mutex<…>>`. `thread::scope` enforces the
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
mod tests {
    use ultrasql_core::{DataType, Field, Schema, Value};
    use ultrasql_planner::{BinaryOp, ScalarExpr};
    use ultrasql_vec::Batch;
    use ultrasql_vec::bitmap::Bitmap;
    use ultrasql_vec::column::{Column, NumericColumn};

    use super::{
        FrameBound, FrameExclusion, FrameSpec, FrameUnits, WindowAgg, WindowAggKind, WindowFunc,
    };
    use crate::filter_op::batch_to_rows;
    use crate::mem_table_scan::MemTableScan;
    use crate::{ExecError, Operator};

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

    fn schema_id_val_i64() -> Schema {
        Schema::new([
            Field::required("id", DataType::Int32),
            Field::required("val", DataType::Int64),
        ])
        .expect("ok")
    }

    fn schema_with_value_window(data_type: DataType) -> Schema {
        Schema::new([
            Field::required("id", DataType::Int32),
            Field::required("val", DataType::Int32),
            Field::required("win", data_type),
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

    fn make_batch_i64(rows: &[(i32, i64)], nulls: Option<&[bool]>) -> Batch {
        let id = Column::Int32(NumericColumn::from_data(
            rows.iter().map(|(a, _)| *a).collect(),
        ));
        let vals: Vec<i64> = rows.iter().map(|(_, b)| *b).collect();
        let val_col = if let Some(mask) = nulls {
            let mut bitmap = Bitmap::new(mask.len(), false);
            for (idx, valid) in mask.iter().copied().enumerate() {
                if valid {
                    bitmap.set(idx, true);
                }
            }
            NumericColumn::with_nulls(vals, bitmap).expect("null bitmap matches rows")
        } else {
            NumericColumn::from_data(vals)
        };
        Batch::new([id, Column::Int64(val_col)]).expect("ok")
    }

    fn col_val() -> ScalarExpr {
        ScalarExpr::Column {
            name: "val".into(),
            index: 1,
            data_type: DataType::Int32,
        }
    }

    fn col_val_i64() -> ScalarExpr {
        ScalarExpr::Column {
            name: "val".into(),
            index: 1,
            data_type: DataType::Int64,
        }
    }

    fn col_id() -> ScalarExpr {
        ScalarExpr::Column {
            name: "id".into(),
            index: 0,
            data_type: DataType::Int32,
        }
    }

    /// The default running frame: `RANGE BETWEEN UNBOUNDED PRECEDING AND
    /// CURRENT ROW EXCLUDE NO OTHERS`.
    fn default_running_frame() -> FrameSpec {
        FrameSpec {
            units: FrameUnits::Range,
            start: FrameBound::UnboundedPreceding,
            end: FrameBound::CurrentRow,
            exclude: FrameExclusion::NoOthers,
        }
    }

    /// A frame with explicit `units`, `start`, `end`, `exclude`.
    fn frame(
        units: FrameUnits,
        start: FrameBound,
        end: FrameBound,
        exclude: FrameExclusion,
    ) -> FrameSpec {
        FrameSpec {
            units,
            start,
            end,
            exclude,
        }
    }

    /// Drive a `sum(val) OVER (ORDER BY <order_col> <frame>)` over a single
    /// `(id, val)` batch and return the window column in input-row order.
    fn run_sum_over(
        rows: &[(i32, i32)],
        order_key: ScalarExpr,
        order_dir: (bool, bool),
        frame_spec: FrameSpec,
    ) -> Vec<Value> {
        let schema = schema_with_value_window(DataType::Int64);
        let scan = MemTableScan::new(schema_id_val(), vec![make_batch(rows)]);
        let mut op = WindowAgg::new(
            Box::new(scan),
            vec![],
            vec![order_key],
            WindowFunc::Aggregate {
                kind: WindowAggKind::Sum,
                expr: col_val(),
            },
            schema,
        )
        .with_order_directions(vec![order_dir])
        .with_frame(frame_spec);
        drain_window_values(&mut op)
    }

    /// Like [`run_sum_over`] but for `count(*)`.
    fn run_count_star_over(
        rows: &[(i32, i32)],
        order_key: ScalarExpr,
        frame_spec: FrameSpec,
    ) -> Vec<Value> {
        let schema = schema_with_value_window(DataType::Int64);
        let scan = MemTableScan::new(schema_id_val(), vec![make_batch(rows)]);
        let mut op = WindowAgg::new(
            Box::new(scan),
            vec![],
            vec![order_key],
            WindowFunc::CountStar,
            schema,
        )
        .with_frame(frame_spec);
        drain_window_values(&mut op)
    }

    fn i64s(vs: &[i64]) -> Vec<Value> {
        vs.iter().map(|v| Value::Int64(*v)).collect()
    }

    fn lit_i32(v: i32) -> ScalarExpr {
        ScalarExpr::Literal {
            value: Value::Int32(v),
            data_type: DataType::Int32,
        }
    }

    fn divide_i32_by_zero(name: &str, index: usize) -> ScalarExpr {
        ScalarExpr::Binary {
            op: BinaryOp::Div,
            left: Box::new(ScalarExpr::Column {
                name: name.into(),
                index,
                data_type: DataType::Int32,
            }),
            right: Box::new(lit_i32(0)),
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

    fn drain_window_values(op: &mut dyn Operator) -> Vec<Value> {
        let schema = op.schema().clone();
        let mut out = Vec::new();
        while let Some(b) = op.next_batch().expect("ok") {
            let rows = batch_to_rows(&b, &schema).expect("decode");
            out.extend(rows.into_iter().map(|row| row[2].clone()));
        }
        out
    }

    /// Drive the operator expecting an error from the first batch.
    fn drain_window_values_err(op: &mut dyn Operator) -> ExecError {
        op.next_batch().expect_err("expected window frame error")
    }

    #[derive(Debug)]
    struct EstimatedScan {
        schema: Schema,
        batches: std::vec::IntoIter<Batch>,
        rows: Option<usize>,
    }

    impl Operator for EstimatedScan {
        fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
            Ok(self.batches.next())
        }

        fn schema(&self) -> &Schema {
            &self.schema
        }

        fn estimated_row_count(&self) -> Option<usize> {
            self.rows
        }
    }

    #[test]
    fn window_agg_preserves_child_estimated_row_count() {
        let scan = EstimatedScan {
            schema: schema_id_val(),
            batches: vec![make_batch(&[(1, 10), (2, 20)])].into_iter(),
            rows: Some(65_536),
        };
        let op = WindowAgg::new(
            Box::new(scan),
            vec![],
            vec![col_val()],
            WindowFunc::RowNumber,
            schema_with_window(),
        );
        assert_eq!(op.estimated_row_count(), Some(65_536));
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
    fn window_row_number_descending_honors_direction() {
        // Regression: `ORDER BY val DESC` used to be silently ignored, so
        // results came out ascending. Rows (id,val): (1,10),(2,30),(3,20);
        // DESC order is 30,20,10, so the window column in the ORIGINAL row
        // order is row0(10)->3, row1(30)->1, row2(20)->2.
        let scan = MemTableScan::new(
            schema_id_val(),
            vec![make_batch(&[(1, 10), (2, 30), (3, 20)])],
        );
        let mut op = WindowAgg::new(
            Box::new(scan),
            vec![],
            vec![col_val()],
            WindowFunc::RowNumber,
            schema_with_window(),
        )
        .with_order_directions(vec![(false, false)]); // val DESC, NULLS LAST
        let rns = drain_window_col(&mut op);
        assert_eq!(
            rns,
            vec![3, 1, 2],
            "DESC must number the highest value as row 1",
        );
    }

    #[test]
    fn window_rank_descending_honors_direction_with_ties() {
        // Rows (id,val): (1,10),(2,30),(3,20),(4,30). DESC: 30,30,20,10.
        // RANK ties: the two 30s share rank 1, 20->3, 10->4. In original row
        // order: row0(10)->4, row1(30)->1, row2(20)->3, row3(30)->1.
        let scan = MemTableScan::new(
            schema_id_val(),
            vec![make_batch(&[(1, 10), (2, 30), (3, 20), (4, 30)])],
        );
        let mut op = WindowAgg::new(
            Box::new(scan),
            vec![],
            vec![col_val()],
            WindowFunc::Rank,
            schema_with_window(),
        )
        .with_order_directions(vec![(false, false)]);
        let ranks = drain_window_col(&mut op);
        assert_eq!(
            ranks,
            vec![4, 1, 3, 1],
            "DESC RANK must give the highest value rank 1",
        );
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
    fn window_row_number_partitions_non_contiguous_keys_together() {
        let scan = MemTableScan::new(
            schema_id_val(),
            vec![make_batch(&[(1, 20), (2, 10), (1, 10), (2, 20)])],
        );
        let mut op = WindowAgg::new(
            Box::new(scan),
            vec![col_id()],
            vec![col_val()],
            WindowFunc::RowNumber,
            schema_with_window(),
        );

        let rns = drain_window_col(&mut op);

        assert_eq!(rns, vec![2, 1, 1, 2]);
    }

    #[test]
    fn window_order_key_eval_error_propagates() {
        let scan = MemTableScan::new(schema_id_val(), vec![make_batch(&[(1, 10)])]);
        let mut op = WindowAgg::new(
            Box::new(scan),
            vec![],
            vec![divide_i32_by_zero("val", 1)],
            WindowFunc::RowNumber,
            schema_with_window(),
        );

        let err = op.next_batch().expect_err("order key division must error");
        assert!(
            err.to_string().contains("division by zero"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn window_partition_key_eval_error_propagates() {
        let scan = MemTableScan::new(schema_id_val(), vec![make_batch(&[(1, 10)])]);
        let mut op = WindowAgg::new(
            Box::new(scan),
            vec![divide_i32_by_zero("id", 0)],
            vec![],
            WindowFunc::RowNumber,
            schema_with_window(),
        );

        let err = op
            .next_batch()
            .expect_err("partition key division must error");
        assert!(
            err.to_string().contains("division by zero"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn window_first_value_eval_error_propagates() {
        let scan = MemTableScan::new(schema_id_val(), vec![make_batch(&[(1, 10)])]);
        let mut op = WindowAgg::new(
            Box::new(scan),
            vec![],
            vec![],
            WindowFunc::FirstValue(divide_i32_by_zero("id", 0)),
            schema_with_value_window(DataType::Int32),
        );

        let err = op
            .next_batch()
            .expect_err("first_value expression division must error");
        assert!(
            err.to_string().contains("division by zero"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn window_lag_eval_error_propagates() {
        let scan = MemTableScan::new(schema_id_val(), vec![make_batch(&[(1, 10), (2, 20)])]);
        let mut op = WindowAgg::new(
            Box::new(scan),
            vec![],
            vec![col_val()],
            WindowFunc::Lag {
                expr: divide_i32_by_zero("id", 0),
                offset: 1,
                default: Value::Int32(0),
            },
            schema_with_value_window(DataType::Int32),
        );

        let err = op
            .next_batch()
            .expect_err("lag expression division must error");
        assert!(
            err.to_string().contains("division by zero"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn window_profile_empty_and_unordered_row_number_paths() {
        let empty_scan = MemTableScan::new(schema_id_val(), vec![]);
        let mut empty = WindowAgg::new(
            Box::new(empty_scan),
            vec![],
            vec![col_val()],
            WindowFunc::RowNumber,
            schema_with_window(),
        );
        assert_eq!(empty.profile_children().len(), 1);
        assert!(empty.next_batch().expect("empty ok").is_none());
        assert!(empty.next_batch().expect("eof ok").is_none());

        let scan = MemTableScan::new(schema_id_val(), vec![make_batch(&[(9, 30), (8, 10)])]);
        let mut unordered = WindowAgg::new(
            Box::new(scan),
            vec![],
            vec![],
            WindowFunc::RowNumber,
            schema_with_window(),
        );

        assert_eq!(drain_window_col(&mut unordered), vec![1, 2]);
    }

    #[test]
    fn window_rank_with_ties_uses_gap_semantics() {
        let scan = MemTableScan::new(
            schema_id_val(),
            vec![make_batch(&[(1, 10), (2, 10), (3, 20), (4, 30)])],
        );
        let mut op = WindowAgg::new(
            Box::new(scan),
            vec![],
            vec![col_val()],
            WindowFunc::Rank,
            schema_with_window(),
        );

        assert_eq!(drain_window_col(&mut op), vec![1, 1, 3, 4]);
    }

    #[test]
    fn window_lag_and_lead_follow_sorted_order_with_defaults() {
        let schema = schema_with_value_window(DataType::Int32);
        let rows = vec![make_batch(&[(100, 20), (200, 10), (300, 30)])];
        let lag_scan = MemTableScan::new(schema_id_val(), rows.clone());
        let mut lag = WindowAgg::new(
            Box::new(lag_scan),
            vec![],
            vec![col_val()],
            WindowFunc::Lag {
                expr: col_id(),
                offset: 1,
                default: Value::Int32(-1),
            },
            schema.clone(),
        );
        assert_eq!(
            drain_window_values(&mut lag),
            vec![Value::Int32(200), Value::Int32(-1), Value::Int32(100)]
        );

        let lead_scan = MemTableScan::new(schema_id_val(), rows);
        let mut lead = WindowAgg::new(
            Box::new(lead_scan),
            vec![],
            vec![col_val()],
            WindowFunc::Lead {
                expr: col_id(),
                offset: 1,
                default: Value::Int32(-1),
            },
            schema,
        );
        assert_eq!(
            drain_window_values(&mut lead),
            vec![Value::Int32(300), Value::Int32(100), Value::Int32(-1)]
        );
    }

    /// The default running frame (`RANGE UNBOUNDED PRECEDING AND CURRENT
    /// ROW`) makes `last_value`/`nth_value` frame-relative — the bug that
    /// previously broadcast the partition max/nth is fixed. Rows are
    /// `(id,val) = (100,20),(200,10),(300,30)` ordered by val ascending,
    /// so sorted order is id 200(v10), 100(v20), 300(v30). The output is
    /// emitted in input (id) order.
    #[test]
    fn window_value_functions_are_frame_relative_under_default_running_frame() {
        let rows = vec![make_batch(&[(100, 20), (200, 10), (300, 30)])];
        let schema = schema_with_value_window(DataType::Int32);
        let running = default_running_frame();

        // first_value over the running frame is the partition first (id
        // 200) for every row — coincides with the partition min here.
        let mut first = WindowAgg::new(
            Box::new(MemTableScan::new(schema_id_val(), rows.clone())),
            vec![],
            vec![col_val()],
            WindowFunc::FirstValue(col_id()),
            schema.clone(),
        )
        .with_frame(running.clone());
        assert_eq!(
            drain_window_values(&mut first),
            // input order: id 100 (pos1), 200 (pos0), 300 (pos2)
            vec![Value::Int32(200), Value::Int32(200), Value::Int32(200)]
        );

        // last_value under the running frame = the CURRENT row, not the
        // partition max (the fixed bug).
        let mut last = WindowAgg::new(
            Box::new(MemTableScan::new(schema_id_val(), rows.clone())),
            vec![],
            vec![col_val()],
            WindowFunc::LastValue(col_id()),
            schema.clone(),
        )
        .with_frame(running.clone());
        assert_eq!(
            drain_window_values(&mut last),
            // each row's own id: 100, 200, 300
            vec![Value::Int32(100), Value::Int32(200), Value::Int32(300)]
        );

        // nth_value(2) is NULL until the running frame has grown to 2 rows.
        // pos0 (id 200): frame {200} -> NULL; pos1 (id 100): frame
        // {200,100} -> 100; pos2 (id 300): frame {200,100,300} -> 100.
        let mut second = WindowAgg::new(
            Box::new(MemTableScan::new(schema_id_val(), rows)),
            vec![],
            vec![col_val()],
            WindowFunc::NthValue {
                expr: col_id(),
                n: 2,
            },
            schema,
        )
        .with_frame(running);
        assert_eq!(
            drain_window_values(&mut second),
            // input order: id 100 (pos1 -> 100), 200 (pos0 -> NULL), 300 (pos2 -> 100)
            vec![Value::Int32(100), Value::Null, Value::Int32(100)]
        );
    }

    /// Under the whole-partition frame the value functions broadcast the
    /// partition first/last/nth, as before.
    #[test]
    fn window_value_functions_use_whole_partition_frame() {
        let rows = vec![make_batch(&[(100, 20), (200, 10), (300, 30)])];
        let schema = schema_with_value_window(DataType::Int32);

        let mut last = WindowAgg::new(
            Box::new(MemTableScan::new(schema_id_val(), rows.clone())),
            vec![],
            vec![col_val()],
            WindowFunc::LastValue(col_id()),
            schema.clone(),
        ); // default = whole partition
        assert_eq!(
            drain_window_values(&mut last),
            vec![Value::Int32(300), Value::Int32(300), Value::Int32(300)]
        );

        let mut second = WindowAgg::new(
            Box::new(MemTableScan::new(schema_id_val(), rows)),
            vec![],
            vec![col_val()],
            WindowFunc::NthValue {
                expr: col_id(),
                n: 2,
            },
            schema,
        );
        assert_eq!(
            drain_window_values(&mut second),
            vec![Value::Int32(100), Value::Int32(100), Value::Int32(100)]
        );
    }

    #[test]
    fn window_ntile_zero_and_literal_order_fallback_paths() {
        let scan = MemTableScan::new(
            schema_id_val(),
            vec![make_batch(&[(1, 20), (2, 10), (3, 30)])],
        );
        let mut ntile = WindowAgg::new(
            Box::new(scan),
            vec![],
            vec![ScalarExpr::Literal {
                value: Value::Int32(1),
                data_type: DataType::Int32,
            }],
            WindowFunc::Ntile(0),
            schema_with_window(),
        );

        assert_eq!(drain_window_col(&mut ntile), vec![1, 1, 1]);
    }

    #[test]
    fn columnar_row_number_fast_path_handles_i64_nulls_and_unsorted_keys() {
        let scan = MemTableScan::new(
            schema_id_val_i64(),
            vec![make_batch_i64(
                &[(1, 30), (2, 10), (3, 999), (4, 20)],
                Some(&[true, true, false, true]),
            )],
        );
        let mut op = WindowAgg::new(
            Box::new(scan),
            vec![],
            vec![col_val_i64()],
            WindowFunc::RowNumber,
            Schema::new([
                Field::required("id", DataType::Int32),
                Field::required("val", DataType::Int64),
                Field::required("rn", DataType::Int64),
            ])
            .expect("ok"),
        );

        assert_eq!(drain_window_col(&mut op), vec![3, 1, 4, 2]);
    }

    #[test]
    fn columnar_row_number_empty_and_bad_order_column_paths() {
        let empty_scan = MemTableScan::new(schema_id_val_i64(), vec![]);
        let mut empty = WindowAgg::new(
            Box::new(empty_scan),
            vec![],
            vec![col_val_i64()],
            WindowFunc::RowNumber,
            Schema::new([
                Field::required("id", DataType::Int32),
                Field::required("val", DataType::Int64),
                Field::required("rn", DataType::Int64),
            ])
            .expect("ok"),
        );
        assert!(empty.next_batch().expect("empty fast path ok").is_none());

        let scan = MemTableScan::new(schema_id_val_i64(), vec![make_batch_i64(&[(1, 10)], None)]);
        let mut bad = WindowAgg::new(
            Box::new(scan),
            vec![],
            vec![ScalarExpr::Column {
                name: "missing".into(),
                index: 9,
                data_type: DataType::Int64,
            }],
            WindowFunc::RowNumber,
            Schema::new([
                Field::required("id", DataType::Int32),
                Field::required("val", DataType::Int64),
                Field::required("rn", DataType::Int64),
            ])
            .expect("ok"),
        );
        let err = bad
            .next_batch()
            .expect_err("missing order column must error");
        assert!(
            err.to_string()
                .contains("window: order column index 9 out of range")
        );
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
                let k = i64::try_from(state >> 32).expect("upper u32 fits i64") & 0x3FF;
                (k, super::u32_from_usize_clamped(i))
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
        let pairs: Vec<(i64, u32)> = (0..n)
            .map(|i| {
                (
                    super::i64_from_usize_clamped(i),
                    super::u32_from_usize_clamped(i),
                )
            })
            .collect();
        let expected = pairs.clone();
        let actual = super::parallel_sort_pairs(pairs);
        assert_eq!(actual, expected);
    }

    #[test]
    fn parallel_sort_handles_reverse_sorted_input() {
        let n = super::PARALLEL_SORT_THRESHOLD + 1;
        let pairs: Vec<(i64, u32)> = (0..n)
            .map(|i| {
                (
                    super::i64_from_usize_clamped(n - i),
                    super::u32_from_usize_clamped(i),
                )
            })
            .collect();
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
        let pairs: Vec<(i64, u32)> = (0..n)
            .map(|i| (42_i64, super::u32_from_usize_clamped(i)))
            .collect();
        let expected = pairs.clone(); // already ordered by index
        let actual = super::parallel_sort_pairs(pairs);
        assert_eq!(actual, expected);
    }

    // ----- Window-frame conformance battery (executor kernel) -----------
    // Hand-built partitions over `(id, val)` rows. Each test maps to a
    // case in the spec battery; comments name the case number.

    /// Case 2: `ROWS BETWEEN 1 PRECEDING AND CURRENT ROW`.
    #[test]
    fn frame_rows_trailing_sum() {
        let got = run_sum_over(
            &[(1, 10), (2, 20), (3, 30), (4, 40)],
            col_id(),
            (true, false),
            frame(
                FrameUnits::Rows,
                FrameBound::Preceding(lit_i32(1)),
                FrameBound::CurrentRow,
                FrameExclusion::NoOthers,
            ),
        );
        assert_eq!(got, i64s(&[10, 30, 50, 70]));
    }

    /// Case 3: `ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING`.
    #[test]
    fn frame_rows_suffix_sum() {
        let got = run_sum_over(
            &[(1, 10), (2, 20), (3, 30), (4, 40)],
            col_id(),
            (true, false),
            frame(
                FrameUnits::Rows,
                FrameBound::CurrentRow,
                FrameBound::UnboundedFollowing,
                FrameExclusion::NoOthers,
            ),
        );
        assert_eq!(got, i64s(&[100, 90, 70, 40]));
    }

    /// Case 4: `ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING`.
    #[test]
    fn frame_rows_centered_sum() {
        let got = run_sum_over(
            &[(1, 10), (2, 20), (3, 30), (4, 40)],
            col_id(),
            (true, false),
            frame(
                FrameUnits::Rows,
                FrameBound::Preceding(lit_i32(1)),
                FrameBound::Following(lit_i32(1)),
                FrameExclusion::NoOthers,
            ),
        );
        assert_eq!(got, i64s(&[30, 60, 90, 70]));
    }

    /// Case 5 (RANGE peers): `RANGE BETWEEN UNBOUNDED PRECEDING AND
    /// CURRENT ROW` with duplicate ORDER BY values. Ordering by `g`
    /// (the val column here doubles as g): rows g = 1,1,2,2,3.
    /// Peers share the stepped cumulative result 30,30,100,100,150 —
    /// proving the default frame is RANGE, not ROWS.
    #[test]
    fn frame_range_cumulative_peers() {
        // (id, g) pairs; we order by g and sum g's companion value below.
        // Use a dedicated 3-col layout: reuse make_batch with (id=val_id,
        // val=v) and order by a separate key. Simpler: order by `val`
        // where val encodes the peer group, and sum a parallel column.
        // Here we model the spec's (g, v): order by g, sum v.
        let schema = schema_with_value_window(DataType::Int64);
        // Columns: id=g, val=v.
        let scan = MemTableScan::new(
            schema_id_val(),
            vec![make_batch(&[(1, 10), (1, 20), (2, 30), (2, 40), (3, 50)])],
        );
        let mut op = WindowAgg::new(
            Box::new(scan),
            vec![],
            vec![col_id()], // ORDER BY g
            WindowFunc::Aggregate {
                kind: WindowAggKind::Sum,
                expr: col_val(),
            },
            schema,
        )
        .with_order_directions(vec![(true, false)])
        .with_frame(default_running_frame());
        assert_eq!(drain_window_values(&mut op), i64s(&[30, 30, 100, 100, 150]));
    }

    /// Case 5 contrast: the SAME data under `ROWS UNBOUNDED PRECEDING AND
    /// CURRENT ROW` gives the per-row running sum 10,30,60,100,150.
    #[test]
    fn frame_rows_cumulative_no_peer_grouping() {
        let schema = schema_with_value_window(DataType::Int64);
        let scan = MemTableScan::new(
            schema_id_val(),
            vec![make_batch(&[(1, 10), (1, 20), (2, 30), (2, 40), (3, 50)])],
        );
        let mut op = WindowAgg::new(
            Box::new(scan),
            vec![],
            vec![col_id()],
            WindowFunc::Aggregate {
                kind: WindowAggKind::Sum,
                expr: col_val(),
            },
            schema,
        )
        .with_order_directions(vec![(true, false)])
        .with_frame(frame(
            FrameUnits::Rows,
            FrameBound::UnboundedPreceding,
            FrameBound::CurrentRow,
            FrameExclusion::NoOthers,
        ));
        assert_eq!(drain_window_values(&mut op), i64s(&[10, 30, 60, 100, 150]));
    }

    /// Case 6 (RANGE numeric offset): `RANGE BETWEEN 10 PRECEDING AND 10
    /// FOLLOWING` over val = 10,15,20,40,45. Each row's frame is the rows
    /// whose value is within [v-10, v+10]. sums 45,45,45,85,85; counts
    /// 3,3,3,2,2.
    #[test]
    fn frame_range_numeric_offset() {
        let rows = [(1, 10), (2, 15), (3, 20), (4, 40), (5, 45)];
        let sums = run_sum_over(
            &rows,
            col_val(), // ORDER BY v
            (true, false),
            frame(
                FrameUnits::Range,
                FrameBound::Preceding(lit_i32(10)),
                FrameBound::Following(lit_i32(10)),
                FrameExclusion::NoOthers,
            ),
        );
        assert_eq!(sums, i64s(&[45, 45, 45, 85, 85]));
        let counts = run_count_star_over(
            &rows,
            col_val(),
            frame(
                FrameUnits::Range,
                FrameBound::Preceding(lit_i32(10)),
                FrameBound::Following(lit_i32(10)),
                FrameExclusion::NoOthers,
            ),
        );
        assert_eq!(counts, i64s(&[3, 3, 3, 2, 2]));
    }

    /// Case 7 (GROUPS): `GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW`.
    /// id=g groups: 1,1,2,3,3 with v = 10,20,30,40,50.
    /// sums 30,30,60,120,120; counts 2,2,3,3,3.
    #[test]
    fn frame_groups_preceding_to_current() {
        let rows = [(1, 10), (1, 20), (2, 30), (3, 40), (3, 50)];
        let sums = run_sum_over(
            &rows,
            col_id(), // ORDER BY g
            (true, false),
            frame(
                FrameUnits::Groups,
                FrameBound::Preceding(lit_i32(1)),
                FrameBound::CurrentRow,
                FrameExclusion::NoOthers,
            ),
        );
        assert_eq!(sums, i64s(&[30, 30, 60, 120, 120]));
        let counts = run_count_star_over(
            &rows,
            col_id(),
            frame(
                FrameUnits::Groups,
                FrameBound::Preceding(lit_i32(1)),
                FrameBound::CurrentRow,
                FrameExclusion::NoOthers,
            ),
        );
        assert_eq!(counts, i64s(&[2, 2, 3, 3, 3]));
    }

    /// Case 15 (GROUPS): `GROUPS BETWEEN CURRENT ROW AND 1 FOLLOWING`.
    /// id=g groups 1,1,2,3 with v = 10,20,30,40. sums 60,60,70,40.
    #[test]
    fn frame_groups_current_to_following() {
        let rows = [(1, 10), (1, 20), (2, 30), (3, 40)];
        let sums = run_sum_over(
            &rows,
            col_id(),
            (true, false),
            frame(
                FrameUnits::Groups,
                FrameBound::CurrentRow,
                FrameBound::Following(lit_i32(1)),
                FrameExclusion::NoOthers,
            ),
        );
        assert_eq!(sums, i64s(&[60, 60, 70, 40]));
    }

    /// Case 8 (EXCLUDE CURRENT ROW): whole-partition frame minus self.
    /// val = 10,20,30,40 -> 90,80,70,60.
    #[test]
    fn frame_exclude_current_row() {
        let got = run_sum_over(
            &[(1, 10), (2, 20), (3, 30), (4, 40)],
            col_id(),
            (true, false),
            frame(
                FrameUnits::Rows,
                FrameBound::UnboundedPreceding,
                FrameBound::UnboundedFollowing,
                FrameExclusion::CurrentRow,
            ),
        );
        assert_eq!(got, i64s(&[90, 80, 70, 60]));
    }

    /// Case 9 (EXCLUDE TIES / GROUP): three peers at g=1 plus a lone g=2.
    /// id=g groups 1,1,1,2 with v = 10,20,30,40. full=100.
    /// EXCLUDE TIES keeps self drops peers: 50,60,70,100.
    /// EXCLUDE GROUP drops self+peers: 40,40,40,60.
    #[test]
    fn frame_exclude_ties_and_group() {
        let rows = [(1, 10), (1, 20), (1, 30), (2, 40)];
        let full = run_sum_over(
            &rows,
            col_id(),
            (true, false),
            frame(
                FrameUnits::Range,
                FrameBound::UnboundedPreceding,
                FrameBound::UnboundedFollowing,
                FrameExclusion::NoOthers,
            ),
        );
        assert_eq!(full, i64s(&[100, 100, 100, 100]));

        let ties = run_sum_over(
            &rows,
            col_id(),
            (true, false),
            frame(
                FrameUnits::Range,
                FrameBound::UnboundedPreceding,
                FrameBound::UnboundedFollowing,
                FrameExclusion::Ties,
            ),
        );
        // id=1: 100-20-30=50; id=2: 100-10-30=60; id=3: 100-10-20=70;
        // id=4 (lone group): 100 (no peers to drop).
        assert_eq!(ties, i64s(&[50, 60, 70, 100]));

        let group = run_sum_over(
            &rows,
            col_id(),
            (true, false),
            frame(
                FrameUnits::Range,
                FrameBound::UnboundedPreceding,
                FrameBound::UnboundedFollowing,
                FrameExclusion::Group,
            ),
        );
        // g=1 rows drop {10,20,30} -> 40; g=2 row drops {40} -> 60.
        assert_eq!(group, i64s(&[40, 40, 40, 60]));
    }

    /// Case 17 (execution-time validation): negative ROWS offset errors.
    #[test]
    fn frame_negative_rows_offset_errors() {
        let schema = schema_with_value_window(DataType::Int64);
        let scan = MemTableScan::new(schema_id_val(), vec![make_batch(&[(1, 10), (2, 20)])]);
        let mut op = WindowAgg::new(
            Box::new(scan),
            vec![],
            vec![col_id()],
            WindowFunc::Aggregate {
                kind: WindowAggKind::Sum,
                expr: col_val(),
            },
            schema,
        )
        .with_order_directions(vec![(true, false)])
        .with_frame(frame(
            FrameUnits::Rows,
            FrameBound::Preceding(lit_i32(-1)),
            FrameBound::CurrentRow,
            FrameExclusion::NoOthers,
        ));
        let err = drain_window_values_err(&mut op);
        assert!(
            matches!(&err, ExecError::WindowFrameError(m) if m.contains("must not be negative")),
            "{err:?}"
        );
    }

    /// Aggregate kernels: MIN/MAX/AVG over a running frame.
    #[test]
    fn frame_aggregate_min_max_avg() {
        let rows = vec![make_batch(&[(1, 10), (2, 30), (3, 20)])];

        let mut min_op = WindowAgg::new(
            Box::new(MemTableScan::new(schema_id_val(), rows.clone())),
            vec![],
            vec![col_id()],
            WindowFunc::Aggregate {
                kind: WindowAggKind::Min,
                expr: col_val(),
            },
            schema_with_value_window(DataType::Int32),
        )
        .with_order_directions(vec![(true, false)])
        .with_frame(default_running_frame());
        // running min: 10, 10, 10
        assert_eq!(
            drain_window_values(&mut min_op),
            vec![Value::Int32(10), Value::Int32(10), Value::Int32(10)]
        );

        let mut max_op = WindowAgg::new(
            Box::new(MemTableScan::new(schema_id_val(), rows.clone())),
            vec![],
            vec![col_id()],
            WindowFunc::Aggregate {
                kind: WindowAggKind::Max,
                expr: col_val(),
            },
            schema_with_value_window(DataType::Int32),
        )
        .with_order_directions(vec![(true, false)])
        .with_frame(default_running_frame());
        // running max: 10, 30, 30
        assert_eq!(
            drain_window_values(&mut max_op),
            vec![Value::Int32(10), Value::Int32(30), Value::Int32(30)]
        );

        let mut avg_op = WindowAgg::new(
            Box::new(MemTableScan::new(schema_id_val(), rows)),
            vec![],
            vec![col_id()],
            WindowFunc::Aggregate {
                kind: WindowAggKind::Avg,
                expr: col_val(),
            },
            schema_with_value_window(DataType::Float64),
        )
        .with_order_directions(vec![(true, false)])
        .with_frame(default_running_frame());
        // running avg: 10, 20, 20
        assert_eq!(
            drain_window_values(&mut avg_op),
            vec![
                Value::Float64(10.0),
                Value::Float64(20.0),
                Value::Float64(20.0)
            ]
        );
    }

    // ---- Regression helpers for the bug-fix tests below ----

    /// Window output schema for an `(id Int32, val Int64)` input plus a `win`
    /// column of `win_type`.
    fn schema_i64_window(win_type: DataType) -> Schema {
        Schema::new([
            Field::required("id", DataType::Int32),
            Field::required("val", DataType::Int64),
            Field::nullable("win", win_type),
        ])
        .expect("ok")
    }

    /// Drive `func OVER (ORDER BY val <dir> <frame>)` over a single
    /// `(id, val Int64)` batch (with optional NULL mask on `val`) and return
    /// the window column in input-row order.
    fn run_i64_window(
        rows: &[(i32, i64)],
        nulls: Option<&[bool]>,
        order_dir: (bool, bool),
        func: WindowFunc,
        win_type: DataType,
        frame_spec: Option<FrameSpec>,
    ) -> Vec<Value> {
        let scan = MemTableScan::new(schema_id_val_i64(), vec![make_batch_i64(rows, nulls)]);
        let mut op = WindowAgg::new(
            Box::new(scan),
            vec![],
            vec![col_val_i64()],
            func,
            schema_i64_window(win_type),
        )
        .with_order_directions(vec![order_dir]);
        if let Some(f) = frame_spec {
            op = op.with_frame(f);
        }
        drain_window_values(&mut op)
    }

    fn make_batch_f64(rows: &[(i32, f64)]) -> Batch {
        let id = Column::Int32(NumericColumn::from_data(
            rows.iter().map(|(a, _)| *a).collect(),
        ));
        let vals = Column::Float64(NumericColumn::from_data(
            rows.iter().map(|(_, b)| *b).collect(),
        ));
        Batch::new([id, vals]).expect("ok")
    }

    // ===================== BUG 1: NULLS-default ordering for DESC =========
    //
    // PostgreSQL default NULLS placement depends on direction: ASC -> NULLS
    // LAST, DESC -> NULLS FIRST. Explicit NULLS FIRST/LAST must hold under
    // both ASC and DESC. We assert both the row order (via row_number) and
    // the default RANGE running-frame sum + rank() on a column with NULLs.
    //
    // Data: (id, v) = (1,10),(2,NULL),(3,30),(4,20). Non-null order ASC is
    // 10,20,30; DESC is 30,20,10.

    fn nulls_rows() -> [(i32, i64); 4] {
        [(1, 10), (2, 0), (3, 30), (4, 20)]
    }
    fn nulls_mask() -> [bool; 4] {
        [true, false, true, true]
    }

    /// row_number() in the sorted order, read back per original id (id->rn).
    /// Returns `(id, rn)` pairs sorted by id for stable assertions.
    fn rn_by_id(order_dir: (bool, bool)) -> Vec<(i32, i64)> {
        let scan = MemTableScan::new(
            schema_id_val_i64(),
            vec![make_batch_i64(&nulls_rows(), Some(&nulls_mask()))],
        );
        let mut op = WindowAgg::new(
            Box::new(scan),
            vec![],
            vec![col_val_i64()],
            WindowFunc::RowNumber,
            schema_i64_window(DataType::Int64),
        )
        .with_order_directions(vec![order_dir]);
        let schema = op.schema().clone();
        let mut out = Vec::new();
        while let Some(b) = op.next_batch().expect("ok") {
            for row in batch_to_rows(&b, &schema).expect("decode") {
                let id = match &row[0] {
                    Value::Int32(v) => *v,
                    _ => unreachable!(),
                };
                let rn = match &row[2] {
                    Value::Int64(v) => *v,
                    _ => unreachable!(),
                };
                out.push((id, rn));
            }
        }
        out.sort_by_key(|(id, _)| *id);
        out
    }

    #[test]
    fn window_nulls_default_asc_is_nulls_last() {
        // ASC default -> NULLS LAST: 10(id1),20(id4),30(id3),NULL(id2).
        let rn = rn_by_id((true, false));
        assert_eq!(rn, vec![(1, 1), (2, 4), (3, 3), (4, 2)]);
    }

    #[test]
    fn window_nulls_default_desc_is_nulls_first() {
        // DESC default -> NULLS FIRST: NULL(id2),30(id3),20(id4),10(id1).
        // This is the currently-correct behaviour that must NOT regress.
        let rn = rn_by_id((false, true));
        assert_eq!(
            rn,
            vec![(1, 4), (2, 1), (3, 2), (4, 3)],
            "DESC default must place NULLs first",
        );
    }

    #[test]
    fn window_nulls_explicit_first_asc_and_desc() {
        // NULLS FIRST, ASC: NULL(id2),10(id1),20(id4),30(id3).
        let asc = rn_by_id((true, true));
        assert_eq!(asc, vec![(1, 2), (2, 1), (3, 4), (4, 3)]);
        // NULLS FIRST, DESC: NULL(id2),30(id3),20(id4),10(id1).
        let desc = rn_by_id((false, true));
        assert_eq!(desc, vec![(1, 4), (2, 1), (3, 2), (4, 3)]);
    }

    #[test]
    fn window_nulls_explicit_last_asc_and_desc() {
        // NULLS LAST, ASC: 10,20,30,NULL -> id1,id4,id3,id2.
        let asc = rn_by_id((true, false));
        assert_eq!(asc, vec![(1, 1), (2, 4), (3, 3), (4, 2)]);
        // NULLS LAST, DESC: 30,20,10,NULL -> id3,id4,id1,id2.
        let desc = rn_by_id((false, false));
        assert_eq!(
            desc,
            vec![(1, 3), (2, 4), (3, 1), (4, 2)],
            "NULLS LAST under DESC must place NULLs at the end",
        );
    }

    #[test]
    fn window_rank_and_running_frame_under_desc_nulls_first() {
        // DESC default (NULLS FIRST). Sorted: NULL(id2),30(id3),20(id4),10(id1).
        // rank(): NULL->1, 30->2, 20->3, 10->4.
        let ranks = run_i64_window(
            &nulls_rows(),
            Some(&nulls_mask()),
            (false, true),
            WindowFunc::Rank,
            DataType::Int64,
            None,
        );
        // ranks read back in INPUT order id1,id2,id3,id4:
        // id1(10)->4, id2(NULL)->1, id3(30)->2, id4(20)->3.
        assert_eq!(ranks, i64s(&[4, 1, 2, 3]));

        // Default RANGE running frame (UNBOUNDED PRECEDING .. CURRENT ROW),
        // sum(val). In sorted order: NULL row's frame is the NULL peer set
        // (sum over NULL vals -> NULL), then running sums 30, 50, 60.
        // Input order id1,id2,id3,id4: id1(10)->60, id2(NULL)->NULL,
        // id3(30)->30, id4(20)->50.
        let sums = run_i64_window(
            &nulls_rows(),
            Some(&nulls_mask()),
            (false, true),
            WindowFunc::Aggregate {
                kind: WindowAggKind::Sum,
                expr: col_val_i64(),
            },
            DataType::Int64,
            Some(default_running_frame()),
        );
        assert_eq!(
            sums,
            vec![
                Value::Int64(60),
                Value::Null,
                Value::Int64(30),
                Value::Int64(50),
            ],
        );
    }

    // ===================== BUG 2: exact-arithmetic RANGE ==================

    #[test]
    fn frame_range_large_i64_boundary_peer_exact() {
        // Two Int64 values just above 2^53 where f64 arithmetic loses the
        // unit: 9007199254740993 and 9007199254740995. With RANGE BETWEEN 2
        // PRECEDING AND CURRENT ROW the larger row's window [v-2, v] must
        // include the smaller (count 2). Computing the bound in f64 drops it
        // (count 1) because 9007199254740995.0 - 2.0 = 9007199254740994.0 and
        // 9007199254740992.0 < that.
        let rows = [(1, 9_007_199_254_740_993_i64), (2, 9_007_199_254_740_995)];
        let f = frame(
            FrameUnits::Range,
            FrameBound::Preceding(lit_i32(2)),
            FrameBound::CurrentRow,
            FrameExclusion::NoOthers,
        );
        let counts = run_i64_window(
            &rows,
            None,
            (true, false),
            WindowFunc::CountStar,
            DataType::Int64,
            Some(f.clone()),
        );
        // id1 (smaller): only itself -> 1; id2 (larger): both -> 2.
        assert_eq!(counts, i64s(&[1, 2]));

        let sums = run_i64_window(
            &rows,
            None,
            (true, false),
            WindowFunc::Aggregate {
                kind: WindowAggKind::Sum,
                expr: col_val_i64(),
            },
            DataType::Int64,
            Some(f),
        );
        // id2's frame sum = both values = 18014398509481988.
        assert_eq!(
            sums,
            vec![
                Value::Int64(9_007_199_254_740_993),
                Value::Int64(18_014_398_509_481_988),
            ],
        );
    }

    #[test]
    fn frame_range_small_int_offset_still_correct() {
        // Sanity: the exact path reproduces the existing f64-era result for
        // small ints. val = 10,15,20,40,45; RANGE 10 PRECEDING..10 FOLLOWING.
        let rows = [(1, 10_i64), (2, 15), (3, 20), (4, 40), (5, 45)];
        let counts = run_i64_window(
            &rows,
            None,
            (true, false),
            WindowFunc::CountStar,
            DataType::Int64,
            Some(frame(
                FrameUnits::Range,
                FrameBound::Preceding(lit_i32(10)),
                FrameBound::Following(lit_i32(10)),
                FrameExclusion::NoOthers,
            )),
        );
        assert_eq!(counts, i64s(&[3, 3, 3, 2, 2]));
    }

    // ===================== BUG 3: GROUPS PRECEDING end underflow ==========

    #[test]
    fn frame_groups_preceding_end_underflow_is_empty() {
        // (g=1,v=10),(g=2,v=20),(g=3,v=30). ORDER BY g.
        // GROUPS BETWEEN 2 PRECEDING AND 1 PRECEDING. For g=1 the end target
        // group is "1 PRECEDING" = group -1, which does not exist, so the
        // frame is EMPTY and sum -> NULL (previously returned 10).
        let rows = [(1, 10_i64), (2, 20), (3, 30)];
        let sums = run_i64_window(
            &rows,
            None,
            (true, false),
            WindowFunc::Aggregate {
                kind: WindowAggKind::Sum,
                expr: col_val_i64(),
            },
            DataType::Int64,
            Some(frame(
                FrameUnits::Groups,
                FrameBound::Preceding(lit_i32(2)),
                FrameBound::Preceding(lit_i32(1)),
                FrameExclusion::NoOthers,
            )),
        );
        // g=1: empty -> NULL. g=2: groups {1} -> 10. g=3: groups {1,2} -> 30.
        assert_eq!(sums, vec![Value::Null, Value::Int64(10), Value::Int64(30)],);
    }

    #[test]
    fn frame_groups_following_start_overflow_is_empty() {
        // GROUPS BETWEEN 1 FOLLOWING AND 2 FOLLOWING. For the LAST group the
        // start ("1 FOLLOWING") is past the end, so the frame is EMPTY.
        let rows = [(1, 10_i64), (2, 20), (3, 30)];
        let sums = run_i64_window(
            &rows,
            None,
            (true, false),
            WindowFunc::Aggregate {
                kind: WindowAggKind::Sum,
                expr: col_val_i64(),
            },
            DataType::Int64,
            Some(frame(
                FrameUnits::Groups,
                FrameBound::Following(lit_i32(1)),
                FrameBound::Following(lit_i32(2)),
                FrameExclusion::NoOthers,
            )),
        );
        // g=1: groups {2,3} -> 50. g=2: group {3} -> 30. g=3: empty -> NULL.
        assert_eq!(sums, vec![Value::Int64(50), Value::Int64(30), Value::Null],);
    }

    // ===================== BUG 4(a): offset NULL message fidelity =========

    #[test]
    fn frame_range_null_end_offset_reports_ending() {
        // A NULL ending offset must say "ending", not "starting".
        let scan = MemTableScan::new(schema_id_val_i64(), vec![make_batch_i64(&[(1, 10)], None)]);
        let null_i64 = ScalarExpr::Literal {
            value: Value::Null,
            data_type: DataType::Int64,
        };
        let mut op = WindowAgg::new(
            Box::new(scan),
            vec![],
            vec![col_val_i64()],
            WindowFunc::CountStar,
            schema_i64_window(DataType::Int64),
        )
        .with_order_directions(vec![(true, false)])
        .with_frame(frame(
            FrameUnits::Range,
            FrameBound::Preceding(lit_i32(1)),
            FrameBound::Following(null_i64),
            FrameExclusion::NoOthers,
        ));
        let err = drain_window_values_err(&mut op);
        assert!(
            matches!(&err, ExecError::WindowFrameError(m)
                if m.contains("ending") && m.contains("must not be null")),
            "expected ending-offset NULL message, got {err:?}",
        );
    }

    // ===================== BUG 4(b): NaN peer grouping ====================

    fn schema_id_val_f64() -> Schema {
        Schema::new([
            Field::required("id", DataType::Int32),
            Field::required("val", DataType::Float64),
        ])
        .expect("ok")
    }

    fn schema_f64_window(win_type: DataType) -> Schema {
        Schema::new([
            Field::required("id", DataType::Int32),
            Field::required("val", DataType::Float64),
            Field::nullable("win", win_type),
        ])
        .expect("ok")
    }

    #[test]
    fn window_nan_rows_form_one_peer_group() {
        // Two adjacent NaN rows must be PEERS (the sort treats NaN == NaN),
        // so rank() gives them the same rank and the default RANGE running
        // frame yields the same stepped sum. val = 1.0, NaN, NaN.
        let scan = MemTableScan::new(
            schema_id_val_f64(),
            vec![make_batch_f64(&[(1, 1.0), (2, f64::NAN), (3, f64::NAN)])],
        );
        let col_val_f64 = ScalarExpr::Column {
            name: "val".into(),
            index: 1,
            data_type: DataType::Float64,
        };
        let mut rank_op = WindowAgg::new(
            Box::new(scan),
            vec![],
            vec![col_val_f64.clone()],
            WindowFunc::Rank,
            schema_f64_window(DataType::Int64),
        )
        .with_order_directions(vec![(true, false)]);
        // Sorted: 1.0, NaN, NaN. rank(): 1, 2, 2 (the two NaNs are peers).
        assert_eq!(drain_window_values(&mut rank_op), i64s(&[1, 2, 2]));

        // Default RANGE running frame, sum(val): peers share the stepped sum.
        // 1.0 -> 1.0; the two NaN peers both -> 1.0 + NaN + NaN = NaN.
        let scan2 = MemTableScan::new(
            schema_id_val_f64(),
            vec![make_batch_f64(&[(1, 1.0), (2, f64::NAN), (3, f64::NAN)])],
        );
        let mut sum_op = WindowAgg::new(
            Box::new(scan2),
            vec![],
            vec![col_val_f64.clone()],
            WindowFunc::Aggregate {
                kind: WindowAggKind::Sum,
                expr: col_val_f64,
            },
            schema_f64_window(DataType::Float64),
        )
        .with_order_directions(vec![(true, false)])
        .with_frame(default_running_frame());
        let sums = drain_window_values(&mut sum_op);
        // Both NaN rows belong to the same peer group, so both see the full
        // running frame [1.0, NaN, NaN]; the sum is NaN for all three NaN
        // peers' positions and 1.0 for the first row.
        assert_eq!(sums.len(), 3);
        assert_eq!(sums[0], Value::Float64(1.0));
        for s in &sums[1..] {
            match s {
                Value::Float64(x) => assert!(x.is_nan(), "expected NaN, got {x}"),
                other => panic!("expected Float64 NaN, got {other:?}"),
            }
        }
    }
}
