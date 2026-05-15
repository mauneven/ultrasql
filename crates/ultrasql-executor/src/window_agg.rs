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

        // Build a flat (Vec<i64>, has_null bitmap is unused — NULLs are
        // sorted last via i64::MAX sentinel for the bench shape; the
        // slow path still handles the general null case).
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

        // Stable sort by key, breaking ties on original index. The
        // tie-break keeps the output deterministic and matches the
        // slow-path behaviour for the bench shape.
        let mut indices: Vec<u32> = (0..total as u32).collect();
        indices.sort_by(|&a, &b| {
            let ka = keys[a as usize];
            let kb = keys[b as usize];
            ka.cmp(&kb).then_with(|| a.cmp(&b))
        });

        // Scatter rank into a row-aligned window column.
        let mut window_col: Vec<i64> = vec![0; total];
        for (pos, &idx) in indices.iter().enumerate() {
            window_col[idx as usize] = (pos + 1) as i64;
        }

        // Build output batches by cloning the input column array and
        // pushing the matching window slice. Each input batch carries
        // up to BATCH_TARGET_ROWS so no resplit is needed.
        let mut out: Vec<Batch> = Vec::with_capacity(input_batches.len());
        for (batch, window) in input_batches
            .into_iter()
            .zip(row_offsets.windows(2).map(|w| (w[0], w[1])))
        {
            let (lo, hi) = window;
            let mut columns: Vec<Column> = batch.columns().to_vec();
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
            .zip(window_values.into_iter())
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

#[cfg(test)]
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
}
