//! `DISTINCT ON (expr, …)` deduplication operator.
//!
//! [`DistinctOn`] implements PostgreSQL `SELECT DISTINCT ON (e1, e2, …)`: it
//! keeps the **first** row of each group of consecutive rows that share the
//! same values for the ON-key expressions `(e1, e2, …)`.
//!
//! The operator assumes its input is already sorted on the ON keys (the
//! binder always inserts a [`crate::Sort`] over the ON keys, optionally
//! followed by the remaining `ORDER BY` keys, beneath this operator). Under
//! that contract "first per group" reduces to a streaming pass: emit a row
//! only when its ON-key tuple differs from the previously emitted row's
//! ON-key tuple. This is O(1) extra state and O(n) time, mirroring the sort
//! mode of [`crate::Unique`].
//!
//! Unlike `Unique`, the dedup key is **not** the whole row: the ON-key
//! expressions are evaluated against each input row (so an ON key need not
//! appear in the projection — PostgreSQL allows `DISTINCT ON (x)` where `x`
//! is not selected). The full input row is forwarded unchanged when emitted;
//! the projection sits *above* this operator.
//!
//! # NULL semantics
//!
//! Two ON-key tuples that are NULL in the same positions group together —
//! `NULL` is treated as equal to `NULL`, matching PostgreSQL's
//! `IS NOT DISTINCT FROM` grouping for `DISTINCT ON`. Reuses the same
//! comparison as `DISTINCT` (`crate::unique::rows_equal_for_distinct`).

use ultrasql_core::{Schema, Value};
use ultrasql_planner::ScalarExpr;
use ultrasql_vec::Batch;

use crate::eval::Eval;
use crate::filter_op::batch_to_rows;
use crate::seq_scan::build_batch;
use crate::unique::rows_equal_for_distinct;
use crate::{ExecError, Operator, eval_error_to_exec_error};

/// Maximum rows per emitted batch, matching the `ARCHITECTURE.md` §9 contract.
const BATCH_TARGET_ROWS: usize = 4096;

/// `DISTINCT ON (expr, …)` deduplication operator.
///
/// See the module documentation for the streaming "first per group"
/// contract and NULL semantics.
///
/// # Send
///
/// All owned fields (`Box<dyn Operator>`, `Schema`, `Vec<Eval>`,
/// `Vec<Value>`) are `Send`, so the operator is `Send`.
#[derive(Debug)]
pub struct DistinctOn {
    child: Box<dyn Operator>,
    schema: Schema,
    /// Compiled ON-key expressions, evaluated against each input row to
    /// form the dedup key.
    keys: Vec<Eval>,
    /// The ON-key tuple of the previously emitted row. `None` before the
    /// first row is emitted.
    last_key: Option<Vec<Value>>,
    /// `true` after the final `Ok(None)` is returned.
    eof: bool,
}

impl DistinctOn {
    /// Construct a `DISTINCT ON` operator.
    ///
    /// - `child` — the input operator, already sorted on the ON keys.
    /// - `on_keys` — the ON-key expressions, resolved against the child's
    ///   (pre-projection) schema.
    #[must_use]
    pub fn new(child: Box<dyn Operator>, on_keys: Vec<ScalarExpr>) -> Self {
        let schema = child.schema().clone();
        let keys = on_keys.into_iter().map(Eval::new).collect();
        Self {
            child,
            schema,
            keys,
            last_key: None,
            eof: false,
        }
    }

    /// Evaluate the ON-key tuple for one input row.
    fn key_for_row(&self, row: &[Value]) -> Result<Vec<Value>, ExecError> {
        self.keys
            .iter()
            .map(|eval| eval.eval(row).map_err(eval_error_to_exec_error))
            .collect()
    }
}

impl Operator for DistinctOn {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.eof {
            return Ok(None);
        }

        let mut survivors: Vec<Vec<Value>> = Vec::new();

        loop {
            let Some(batch) = self.child.next_batch()? else {
                break;
            };
            let rows = batch_to_rows(&batch, &self.schema)?;
            for row in rows {
                let key = self.key_for_row(&row)?;
                let is_dup = self
                    .last_key
                    .as_ref()
                    .is_some_and(|prev| rows_equal_for_distinct(prev, &key));
                if !is_dup {
                    self.last_key = Some(key);
                    survivors.push(row);
                    if survivors.len() >= BATCH_TARGET_ROWS {
                        return build_batch(&survivors, &self.schema).map(Some);
                    }
                }
            }
            if !survivors.is_empty() {
                return build_batch(&survivors, &self.schema).map(Some);
            }
        }

        if survivors.is_empty() {
            self.eof = true;
            return Ok(None);
        }
        build_batch(&survivors, &self.schema).map(Some)
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn estimated_row_count(&self) -> Option<usize> {
        self.child.estimated_row_count()
    }

    fn profile_children(&self) -> Vec<&dyn Operator> {
        vec![self.child.as_ref()]
    }
}

#[cfg(test)]
mod tests {
    use ultrasql_core::{DataType, Field, Schema, Value};
    use ultrasql_planner::ScalarExpr;
    use ultrasql_vec::Batch;
    use ultrasql_vec::column::{Column, NumericColumn};

    use super::DistinctOn;
    use crate::Operator;
    use crate::filter_op::batch_to_rows;
    use crate::mem_table_scan::MemTableScan;

    fn schema_two() -> Schema {
        Schema::new([
            Field::nullable("k", DataType::Int32),
            Field::nullable("v", DataType::Int32),
        ])
        .expect("schema ok")
    }

    fn batch_two(pairs: &[(Option<i32>, i32)]) -> Batch {
        let mut keys = ultrasql_vec::Bitmap::new(pairs.len(), true);
        let mut kdata = Vec::with_capacity(pairs.len());
        let mut vdata = Vec::with_capacity(pairs.len());
        for (i, (k, v)) in pairs.iter().enumerate() {
            match k {
                Some(x) => kdata.push(*x),
                None => {
                    kdata.push(0);
                    keys.set(i, false);
                }
            }
            vdata.push(*v);
        }
        Batch::new([
            Column::Int32(NumericColumn::with_nulls(kdata, keys).expect("lengths match")),
            Column::Int32(NumericColumn::from_data(vdata)),
        ])
        .expect("batch ok")
    }

    fn col(index: usize) -> ScalarExpr {
        ScalarExpr::Column {
            name: "k".to_owned(),
            index,
            data_type: DataType::Int32,
        }
    }

    fn drain(op: &mut dyn Operator) -> Vec<(Option<i32>, i32)> {
        let schema = op.schema().clone();
        let mut out = Vec::new();
        while let Some(b) = op.next_batch().expect("ok") {
            for row in batch_to_rows(&b, &schema).expect("decode") {
                let k = match &row[0] {
                    Value::Int32(x) => Some(*x),
                    Value::Null => None,
                    other => panic!("unexpected key {other:?}"),
                };
                let v = match &row[1] {
                    Value::Int32(x) => *x,
                    other => panic!("unexpected value {other:?}"),
                };
                out.push((k, v));
            }
        }
        out
    }

    #[test]
    fn keeps_first_row_per_group() {
        // Sorted on k; first per k must survive.
        let scan = MemTableScan::new(
            schema_two(),
            vec![batch_two(&[
                (Some(1), 10),
                (Some(1), 11),
                (Some(2), 20),
                (Some(2), 21),
                (Some(3), 30),
            ])],
        );
        let mut op = DistinctOn::new(Box::new(scan), vec![col(0)]);
        assert_eq!(
            drain(&mut op),
            vec![(Some(1), 10), (Some(2), 20), (Some(3), 30)]
        );
    }

    #[test]
    fn nulls_group_together() {
        // Two leading NULL-key rows form one group; first survives.
        let scan = MemTableScan::new(
            schema_two(),
            vec![batch_two(&[(None, 1), (None, 2), (Some(5), 3)])],
        );
        let mut op = DistinctOn::new(Box::new(scan), vec![col(0)]);
        assert_eq!(drain(&mut op), vec![(None, 1), (Some(5), 3)]);
    }

    #[test]
    fn empty_input_returns_none() {
        let scan = MemTableScan::new(schema_two(), vec![]);
        let mut op = DistinctOn::new(Box::new(scan), vec![col(0)]);
        assert!(op.next_batch().expect("ok").is_none());
    }
}
