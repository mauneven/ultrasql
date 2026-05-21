//! Parallel-query collators.
//!
//! `Gather` and `GatherMerge` are the executor-side fan-in primitives
//! used by future parallel scans. They do not decide how work is split;
//! they own the coordinator contract: combine multiple worker streams
//! into one output stream while preserving schema and batch contracts.

use std::cmp::Ordering;
use std::collections::VecDeque;

use ultrasql_core::{Schema, Value};
use ultrasql_planner::SortKey;
use ultrasql_vec::Batch;

use crate::eval::Eval;
use crate::filter_op::batch_to_rows;
use crate::seq_scan::build_batch;
use crate::sort::compare_values_nullable;
use crate::{ExecError, Operator};

const BATCH_TARGET_ROWS: usize = 4096;

/// Round-robin fan-in for unordered parallel worker streams.
///
/// Each child must emit the same schema. `Gather` returns whole batches
/// without copying their columns, rotating across live children so one
/// worker cannot monopolize the coordinator while other workers are ready.
#[derive(Debug)]
pub struct Gather {
    children: Vec<Option<Box<dyn Operator>>>,
    schema: Schema,
    next_child: usize,
    live_children: usize,
    row_hint: Option<usize>,
}

impl Gather {
    /// Build a `Gather` collator from worker streams.
    ///
    /// # Errors
    ///
    /// Returns [`ExecError::TypeMismatch`] when any child reports a
    /// schema different from `schema`.
    pub fn try_new(children: Vec<Box<dyn Operator>>, schema: Schema) -> Result<Self, ExecError> {
        for (idx, child) in children.iter().enumerate() {
            if child.schema() != &schema {
                return Err(ExecError::TypeMismatch(format!(
                    "Gather child {idx} schema does not match output schema"
                )));
            }
        }
        let row_hint = sum_row_hints(&children);
        let live_children = children.len();
        Ok(Self {
            children: children.into_iter().map(Some).collect(),
            schema,
            next_child: 0,
            live_children,
            row_hint,
        })
    }
}

impl Operator for Gather {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.live_children == 0 {
            return Ok(None);
        }

        let child_count = self.children.len();
        for _ in 0..child_count {
            let idx = self.next_child;
            self.next_child = (self.next_child + 1) % child_count;

            let Some(child) = self.children[idx].as_mut() else {
                continue;
            };
            match child.next_batch()? {
                Some(batch) => return Ok(Some(batch)),
                None => {
                    self.children[idx] = None;
                    self.live_children -= 1;
                    if self.live_children == 0 {
                        return Ok(None);
                    }
                }
            }
        }

        Ok(None)
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn estimated_row_count(&self) -> Option<usize> {
        self.row_hint
    }

    fn profile_children(&self) -> Vec<&dyn Operator> {
        self.children
            .iter()
            .filter_map(std::option::Option::as_ref)
            .map(std::convert::AsRef::as_ref)
            .collect()
    }
}

/// K-way ordered fan-in for sorted parallel worker streams.
///
/// `GatherMerge` assumes each child stream is already sorted by `keys`.
/// It keeps only decoded row buffers from current child batches and emits
/// globally sorted 4096-row output chunks.
#[derive(Debug)]
pub struct GatherMerge {
    children: Vec<MergeChild>,
    keys: Vec<CompiledKey>,
    schema: Schema,
    eof: bool,
    row_hint: Option<usize>,
}

#[derive(Debug)]
struct MergeChild {
    input: Box<dyn Operator>,
    rows: VecDeque<Vec<Value>>,
    done: bool,
}

#[derive(Debug)]
struct CompiledKey {
    eval: Eval,
    asc: bool,
    nulls_first: bool,
}

impl GatherMerge {
    /// Build a `GatherMerge` collator from sorted worker streams.
    ///
    /// # Errors
    ///
    /// Returns [`ExecError::TypeMismatch`] when any child reports a
    /// schema different from `schema`.
    pub fn try_new(
        children: Vec<Box<dyn Operator>>,
        keys: Vec<SortKey>,
        schema: Schema,
    ) -> Result<Self, ExecError> {
        for (idx, child) in children.iter().enumerate() {
            if child.schema() != &schema {
                return Err(ExecError::TypeMismatch(format!(
                    "GatherMerge child {idx} schema does not match output schema"
                )));
            }
        }
        let row_hint = sum_row_hints(&children);
        let keys = keys
            .into_iter()
            .map(|key| CompiledKey {
                eval: Eval::new(key.expr),
                asc: key.asc,
                nulls_first: key.nulls_first,
            })
            .collect();
        Ok(Self {
            children: children
                .into_iter()
                .map(|input| MergeChild {
                    input,
                    rows: VecDeque::new(),
                    done: false,
                })
                .collect(),
            keys,
            schema,
            eof: false,
            row_hint,
        })
    }
}

impl Operator for GatherMerge {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.eof {
            return Ok(None);
        }

        let mut out_rows: Vec<Vec<Value>> = Vec::with_capacity(BATCH_TARGET_ROWS);
        while out_rows.len() < BATCH_TARGET_ROWS {
            for child in &mut self.children {
                fill_child_head(child, &self.schema)?;
            }

            let Some(best_idx) = best_child(&self.children, &self.keys) else {
                self.eof = true;
                break;
            };
            let row = self.children[best_idx]
                .rows
                .pop_front()
                .expect("best_child only returns a child with a head row");
            out_rows.push(row);
        }

        if out_rows.is_empty() {
            Ok(None)
        } else {
            build_batch(&out_rows, &self.schema).map(Some)
        }
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn estimated_row_count(&self) -> Option<usize> {
        self.row_hint
    }

    fn profile_children(&self) -> Vec<&dyn Operator> {
        self.children
            .iter()
            .map(|child| child.input.as_ref())
            .collect()
    }
}

fn fill_child_head(child: &mut MergeChild, schema: &Schema) -> Result<(), ExecError> {
    while child.rows.is_empty() && !child.done {
        match child.input.next_batch()? {
            Some(batch) => {
                let decoded = batch_to_rows(&batch, schema)?;
                child.rows.extend(decoded);
            }
            None => child.done = true,
        }
    }
    Ok(())
}

fn best_child(children: &[MergeChild], keys: &[CompiledKey]) -> Option<usize> {
    let mut best: Option<usize> = None;
    for (idx, child) in children.iter().enumerate() {
        let Some(row) = child.rows.front() else {
            continue;
        };
        let Some(best_idx) = best else {
            best = Some(idx);
            continue;
        };
        let best_row = children[best_idx]
            .rows
            .front()
            .expect("best child has a head row");
        if compare_rows(row, best_row, keys) == Ordering::Less {
            best = Some(idx);
        }
    }
    best
}

fn compare_rows(left: &[Value], right: &[Value], keys: &[CompiledKey]) -> Ordering {
    for key in keys {
        let left_value = key.eval.eval(left).unwrap_or(Value::Null);
        let right_value = key.eval.eval(right).unwrap_or(Value::Null);
        let ord = compare_values_nullable(&left_value, &right_value, key.nulls_first);
        let ord = if key.asc { ord } else { ord.reverse() };
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

fn sum_row_hints(children: &[Box<dyn Operator>]) -> Option<usize> {
    let mut total = 0usize;
    for child in children {
        total = total.checked_add(child.estimated_row_count()?)?;
    }
    Some(total)
}

#[cfg(test)]
mod tests {
    use ultrasql_core::{DataType, Field};
    use ultrasql_planner::ScalarExpr;
    use ultrasql_vec::column::{Column, NumericColumn};

    use super::*;
    use crate::MemTableScan;

    fn schema_i32() -> Schema {
        Schema::new([Field::required("id", DataType::Int32)]).expect("schema ok")
    }

    fn schema_i64() -> Schema {
        Schema::new([Field::required("id", DataType::Int64)]).expect("schema ok")
    }

    fn batch_i32(rows: &[i32]) -> Batch {
        Batch::new([Column::Int32(NumericColumn::from_data(rows.to_vec()))]).expect("batch ok")
    }

    fn scan(schema: &Schema, batches: Vec<Batch>) -> Box<dyn Operator> {
        Box::new(MemTableScan::new(schema.clone(), batches))
    }

    fn drain_i32(op: &mut dyn Operator) -> Vec<i32> {
        let mut out = Vec::new();
        while let Some(batch) = op.next_batch().expect("operator ok") {
            let col = match &batch.columns()[0] {
                Column::Int32(c) => c,
                other => panic!("expected Int32, got {other:?}"),
            };
            out.extend_from_slice(col.data());
        }
        out
    }

    fn sort_key_id(asc: bool) -> SortKey {
        SortKey {
            expr: ScalarExpr::Column {
                name: "id".into(),
                index: 0,
                data_type: DataType::Int32,
            },
            asc,
            nulls_first: false,
        }
    }

    #[test]
    fn gather_round_robins_worker_batches() {
        let schema = schema_i32();
        let children = vec![
            scan(&schema, vec![batch_i32(&[1, 3]), batch_i32(&[5])]),
            scan(&schema, vec![batch_i32(&[2, 4])]),
        ];
        let mut gather = Gather::try_new(children, schema).expect("gather ok");
        assert_eq!(drain_i32(&mut gather), vec![1, 3, 2, 4, 5]);
        assert!(gather.next_batch().expect("eof ok").is_none());
    }

    #[test]
    fn gather_rejects_schema_mismatch() {
        let schema = schema_i32();
        let bad = scan(&schema_i64(), vec![]);
        let err = Gather::try_new(vec![bad], schema).expect_err("schema mismatch");
        assert!(matches!(err, ExecError::TypeMismatch(_)));
    }

    #[test]
    fn gather_merge_preserves_global_order() {
        let schema = schema_i32();
        let children = vec![
            scan(&schema, vec![batch_i32(&[1, 4]), batch_i32(&[7])]),
            scan(&schema, vec![batch_i32(&[2, 3])]),
            scan(&schema, vec![batch_i32(&[0, 5])]),
        ];
        let mut gather =
            GatherMerge::try_new(children, vec![sort_key_id(true)], schema).expect("merge ok");
        assert_eq!(drain_i32(&mut gather), vec![0, 1, 2, 3, 4, 5, 7]);
    }

    #[test]
    fn gather_merge_handles_descending_inputs() {
        let schema = schema_i32();
        let children = vec![
            scan(&schema, vec![batch_i32(&[9, 3])]),
            scan(&schema, vec![batch_i32(&[8, 4]), batch_i32(&[1])]),
        ];
        let mut gather =
            GatherMerge::try_new(children, vec![sort_key_id(false)], schema).expect("merge ok");
        assert_eq!(drain_i32(&mut gather), vec![9, 8, 4, 3, 1]);
    }
}
