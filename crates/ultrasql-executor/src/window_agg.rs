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

use ultrasql_core::{Schema, Value};
use ultrasql_planner::ScalarExpr;
use ultrasql_vec::Batch;

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
    /// Expressions for the PARTITION BY keys.
    partition_key_evals: Vec<Eval>,
    /// Expressions for the ORDER BY keys.
    order_key_evals: Vec<Eval>,
    /// The window function.
    func: WindowFunc,
    schema: Schema,
    child_schema: Schema,
    output: Option<std::vec::IntoIter<Vec<Value>>>,
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
        Self {
            child,
            partition_key_evals: partition_keys.into_iter().map(Eval::new).collect(),
            order_key_evals: order_keys.into_iter().map(Eval::new).collect(),
            func,
            schema,
            child_schema,
            output: None,
            eof: false,
        }
    }
}

impl Operator for WindowAgg {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.eof {
            return Ok(None);
        }
        if self.output.is_none() {
            let rows = self.execute()?;
            self.output = Some(rows.into_iter());
        }
        let iter = self.output.as_mut().expect("just-set");
        let chunk: Vec<Vec<Value>> = iter.by_ref().take(BATCH_TARGET_ROWS).collect();
        if chunk.is_empty() {
            self.eof = true;
            return Ok(None);
        }
        build_batch(&chunk, &self.schema).map(Some)
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

impl WindowAgg {
    fn execute(&mut self) -> Result<Vec<Vec<Value>>, ExecError> {
        // Drain child.
        let mut all_rows: Vec<Vec<Value>> = Vec::new();
        loop {
            let Some(batch) = self.child.next_batch()? else {
                break;
            };
            all_rows.extend(batch_to_rows(&batch, &self.child_schema)?);
        }

        if all_rows.is_empty() {
            return Ok(Vec::new());
        }

        // Compute partition key for each row.
        let partition_keys: Vec<Vec<Value>> = all_rows
            .iter()
            .map(|row| {
                self.partition_key_evals
                    .iter()
                    .map(|ev| ev.eval(row).unwrap_or(Value::Null))
                    .collect()
            })
            .collect();

        // Group row indices by partition key.
        let mut partitions: Vec<Vec<usize>> = Vec::new();
        let mut current_partition: Vec<usize> = Vec::new();
        let mut current_key: Option<Vec<Value>> = None;

        for (idx, key) in partition_keys.iter().enumerate() {
            let same = current_key.as_ref().map_or(false, |ck| keys_equal(ck, key));
            if !same {
                if !current_partition.is_empty() {
                    partitions.push(current_partition.clone());
                    current_partition.clear();
                }
                current_key = Some(key.clone());
            }
            current_partition.push(idx);
        }
        if !current_partition.is_empty() {
            partitions.push(current_partition);
        }

        // Process each partition.
        let mut output_values: Vec<(usize, Value)> = Vec::new(); // (original row index, window value)

        for partition_indices in &partitions {
            // Sort within partition by order keys.
            let mut sorted_indices = partition_indices.clone();
            if !self.order_key_evals.is_empty() {
                let order_evals = &self.order_key_evals;
                let rows = &all_rows;
                sorted_indices.sort_by(|&a, &b| {
                    for ev in order_evals {
                        let av = ev.eval(&rows[a]).unwrap_or(Value::Null);
                        let bv = ev.eval(&rows[b]).unwrap_or(Value::Null);
                        let ord = compare_values_nullable(&av, &bv, false);
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
                    let mut ranks = vec![Value::Int64(1); n];
                    let mut rank = 1_usize;
                    let mut prev_order_key: Option<Vec<Value>> = None;
                    let mut count = 0_usize;
                    for (pos, &idx) in sorted_indices.iter().enumerate() {
                        let order_key: Vec<Value> = self
                            .order_key_evals
                            .iter()
                            .map(|ev| ev.eval(&all_rows[idx]).unwrap_or(Value::Null))
                            .collect();
                        let same = prev_order_key
                            .as_ref()
                            .map_or(false, |pk| keys_equal(pk, &order_key));
                        if same {
                            ranks[pos] = Value::Int64(rank as i64);
                        } else {
                            rank += count;
                            count = 1;
                            ranks[pos] = Value::Int64(rank as i64);
                            prev_order_key = Some(order_key);
                        }
                        count = count.max(1);
                        let _ = count; // suppress unused warning
                        // Actually recompute properly:
                        ranks[pos] = Value::Int64((pos + 1) as i64); // placeholder; fix below
                    }
                    // Proper RANK: scan again.
                    let mut out_ranks = vec![1_i64; n];
                    let mut prev_key: Option<Vec<Value>> = None;
                    let mut base_rank = 1_usize;
                    for (pos, &idx) in sorted_indices.iter().enumerate() {
                        let key: Vec<Value> = self
                            .order_key_evals
                            .iter()
                            .map(|ev| ev.eval(&all_rows[idx]).unwrap_or(Value::Null))
                            .collect();
                        let same = prev_key.as_ref().map_or(false, |pk| keys_equal(pk, &key));
                        if !same {
                            base_rank = pos + 1;
                            prev_key = Some(key);
                        }
                        out_ranks[pos] = base_rank as i64;
                    }
                    out_ranks.into_iter().map(Value::Int64).collect()
                }
                WindowFunc::DenseRank => {
                    let mut out = Vec::with_capacity(n);
                    let mut dense = 1_i64;
                    let mut prev_key: Option<Vec<Value>> = None;
                    for &idx in &sorted_indices {
                        let key: Vec<Value> = self
                            .order_key_evals
                            .iter()
                            .map(|ev| ev.eval(&all_rows[idx]).unwrap_or(Value::Null))
                            .collect();
                        let same = prev_key.as_ref().map_or(false, |pk| keys_equal(pk, &key));
                        if !same {
                            if prev_key.is_some() {
                                dense += 1;
                            }
                            prev_key = Some(key);
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
                    let ev = Eval::new(expr.clone());
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
                                ev.eval(&all_rows[prev_idx]).unwrap_or(default.clone())
                            }
                        })
                        .collect()
                }
                WindowFunc::Lead {
                    expr,
                    offset,
                    default,
                } => {
                    let ev = Eval::new(expr.clone());
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
                                ev.eval(&all_rows[next_idx]).unwrap_or(default.clone())
                            }
                        })
                        .collect()
                }
                WindowFunc::FirstValue(expr) => {
                    let ev = Eval::new(expr.clone());
                    let first = sorted_indices
                        .first()
                        .map(|&i| ev.eval(&all_rows[i]).unwrap_or(Value::Null))
                        .unwrap_or(Value::Null);
                    vec![first; n]
                }
                WindowFunc::LastValue(expr) => {
                    let ev = Eval::new(expr.clone());
                    let last = sorted_indices
                        .last()
                        .map(|&i| ev.eval(&all_rows[i]).unwrap_or(Value::Null))
                        .unwrap_or(Value::Null);
                    vec![last; n]
                }
                WindowFunc::NthValue { expr, n: nth } => {
                    let ev = Eval::new(expr.clone());
                    let nth = *nth;
                    let val = if nth == 0 || nth > n {
                        Value::Null
                    } else {
                        let idx = sorted_indices[nth - 1];
                        ev.eval(&all_rows[idx]).unwrap_or(Value::Null)
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

            for (pos, &orig_idx) in sorted_indices.iter().enumerate() {
                output_values.push((orig_idx, values[pos].clone()));
            }
        }

        // Re-sort by original row index and assemble output rows.
        output_values.sort_by_key(|(idx, _)| *idx);

        let output: Vec<Vec<Value>> = output_values
            .into_iter()
            .zip(all_rows.iter())
            .map(|((_, win_val), orig_row)| {
                let mut row = orig_row.clone();
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
