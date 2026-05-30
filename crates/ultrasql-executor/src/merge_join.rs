//! Merge equi-join operator.
//!
//! [`MergeJoin`] implements a pairwise scan over two inputs that are
//! already sorted on the join key. Both inputs are drained and the sorted
//! merge join algorithm is applied: advance the cursor on whichever input
//! has the smaller key, emit pairs where the keys are equal.
//!
//! # All join types
//!
//! | Join type | Behaviour |
//! |-----------|-----------|
//! | `Inner`   | Only matching pairs. |
//! | `LeftOuter` | All left rows; unmatched left rows emit NULL right columns. |
//! | `RightOuter` | All right rows; unmatched right rows emit NULL left columns. |
//! | `FullOuter` | All rows from both sides; unmatched emit NULLs on the missing side. |
//! | `Cross` | Returns [`ExecError::Unsupported`] — use `NestedLoopJoin`. |
//!
//! # NULL key semantics
//!
//! NULL keys never match (same as `HashJoin`). A NULL key on either side
//! is treated as "no match possible" for that row.
//!
//! # v0.5 implementation note
//!
//! The join drains both inputs into memory and then applies the merge algorithm
//! on the sorted vectors. This is O(n + m) in time and O(n + m) in memory.
//! A streaming variant requiring only O(k) extra memory (where k is the
//! maximum number of rows with the same key) is a future optimisation.

use std::cmp::Ordering;

use ultrasql_core::{Schema, Value};
use ultrasql_planner::{LogicalJoinType, ScalarExpr};
use ultrasql_vec::Batch;

use crate::eval::Eval;
use crate::filter_op::batch_to_rows;
use crate::seq_scan::build_batch;
use crate::sort::compare_values_nullable;
use crate::{ExecError, Operator};

const BATCH_TARGET_ROWS: usize = 4096;

/// Merge equi-join operator.
///
/// Requires both inputs to be sorted ascending on their respective key
/// expressions. The join is performed by a standard two-pointer merge scan.
///
/// # Send
///
/// `Box<dyn Operator>`, `Eval`, `Schema`, and result buffers are all `Send`.
#[derive(Debug)]
pub struct MergeJoin {
    left: Box<dyn Operator>,
    right: Box<dyn Operator>,
    left_key_eval: Eval,
    right_key_eval: Eval,
    join_type: LogicalJoinType,
    schema: Schema,
    left_schema: Schema,
    right_schema: Schema,
    output: Option<std::vec::IntoIter<Vec<Value>>>,
    eof: bool,
}

impl MergeJoin {
    /// Construct a merge join operator.
    ///
    /// - `left`, `right` — pre-sorted inputs.
    /// - `left_key`, `right_key` — sort-key expressions; both inputs must
    ///   be sorted ascending on these expressions.
    /// - `join_type` — join type; `Cross` returns `ExecError::Unsupported`.
    /// - `schema` — output schema (left columns then right columns).
    /// - `left_schema`, `right_schema` — child schemas for row decoding.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        left: Box<dyn Operator>,
        right: Box<dyn Operator>,
        left_key: ScalarExpr,
        right_key: ScalarExpr,
        join_type: LogicalJoinType,
        schema: Schema,
        left_schema: Schema,
        right_schema: Schema,
    ) -> Self {
        Self {
            left,
            right,
            left_key_eval: Eval::new(left_key),
            right_key_eval: Eval::new(right_key),
            join_type,
            schema,
            left_schema,
            right_schema,
            output: None,
            eof: false,
        }
    }
}

impl Operator for MergeJoin {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.eof {
            return Ok(None);
        }
        if self.output.is_none() {
            let rows = self.execute()?;
            self.output = Some(rows.into_iter());
        }
        let iter = self
            .output
            .as_mut()
            .ok_or(ExecError::Internal("merge join output iterator missing"))?;
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

    fn profile_children(&self) -> Vec<&dyn Operator> {
        vec![self.left.as_ref(), self.right.as_ref()]
    }
}

impl MergeJoin {
    #[allow(clippy::too_many_lines)]
    fn execute(&mut self) -> Result<Vec<Vec<Value>>, ExecError> {
        if self.join_type == LogicalJoinType::Cross {
            return Err(ExecError::Unsupported(
                "cross join via MergeJoin is not supported; use NestedLoopJoin",
            ));
        }

        // Drain both inputs.
        let mut left_rows: Vec<Vec<Value>> = Vec::new();
        loop {
            let Some(b) = self.left.next_batch()? else {
                break;
            };
            left_rows.extend(batch_to_rows(&b, &self.left_schema)?);
        }
        let mut right_rows: Vec<Vec<Value>> = Vec::new();
        loop {
            let Some(b) = self.right.next_batch()? else {
                break;
            };
            right_rows.extend(batch_to_rows(&b, &self.right_schema)?);
        }

        let null_right = vec![Value::Null; self.right_schema.len()];
        let null_left = vec![Value::Null; self.left_schema.len()];

        let mut output: Vec<Vec<Value>> = Vec::new();
        let mut li = 0_usize;
        let mut ri = 0_usize;
        let mut left_matched = vec![false; left_rows.len()];
        let mut right_matched = vec![false; right_rows.len()];

        while li < left_rows.len() && ri < right_rows.len() {
            let lk = eval_join_key(&self.left_key_eval, &left_rows[li])?;
            let rk = eval_join_key(&self.right_key_eval, &right_rows[ri])?;

            // NULL keys never match — skip them.
            if lk.is_null() {
                li += 1;
                continue;
            }
            if rk.is_null() {
                ri += 1;
                continue;
            }

            match compare_values_nullable(&lk, &rk, false) {
                Ordering::Less => {
                    li += 1;
                }
                Ordering::Greater => {
                    ri += 1;
                }
                Ordering::Equal => {
                    // Collect the range of right rows with the same key.
                    let ri_start = ri;
                    while ri < right_rows.len() {
                        let rk2 = eval_join_key(&self.right_key_eval, &right_rows[ri])?;
                        if compare_values_nullable(&lk, &rk2, false) != Ordering::Equal {
                            break;
                        }
                        ri += 1;
                    }
                    // Collect the range of left rows with the same key.
                    let li_start = li;
                    while li < left_rows.len() {
                        let lk2 = eval_join_key(&self.left_key_eval, &left_rows[li])?;
                        if compare_values_nullable(&rk, &lk2, false) != Ordering::Equal {
                            break;
                        }
                        li += 1;
                    }
                    // Emit cross product of matching ranges.
                    for lj in li_start..li {
                        for rj in ri_start..ri {
                            left_matched[lj] = true;
                            right_matched[rj] = true;
                            let joined = concat_rows(&left_rows[lj], &right_rows[rj]);
                            output.push(joined);
                        }
                    }
                }
            }
        }

        // Outer join handling: emit unmatched rows with NULL padding.
        if matches!(
            self.join_type,
            LogicalJoinType::LeftOuter | LogicalJoinType::FullOuter
        ) {
            for (lj, matched) in left_matched.iter().enumerate() {
                if !matched {
                    output.push(concat_rows(&left_rows[lj], &null_right));
                }
            }
        }
        if matches!(
            self.join_type,
            LogicalJoinType::RightOuter | LogicalJoinType::FullOuter
        ) {
            for (rj, matched) in right_matched.iter().enumerate() {
                if !matched {
                    output.push(concat_rows(&null_left, &right_rows[rj]));
                }
            }
        }

        Ok(output)
    }
}

fn eval_join_key(eval: &Eval, row: &[Value]) -> Result<Value, ExecError> {
    eval.eval(row)
        .map_err(|err| ExecError::TypeMismatch(err.to_string()))
}

fn concat_rows(left: &[Value], right: &[Value]) -> Vec<Value> {
    let mut row = Vec::with_capacity(left.len() + right.len());
    row.extend_from_slice(left);
    row.extend_from_slice(right);
    row
}

#[cfg(test)]
mod tests {
    use ultrasql_core::{DataType, Field, Schema, Value};
    use ultrasql_planner::{BinaryOp, LogicalJoinType, ScalarExpr};
    use ultrasql_vec::Batch;
    use ultrasql_vec::column::{Column, NumericColumn};

    use super::MergeJoin;
    use crate::filter_op::batch_to_rows;
    use crate::mem_table_scan::MemTableScan;
    use crate::{ExecError, Operator};

    fn schema_id() -> Schema {
        Schema::new([Field::required("id", DataType::Int32)]).expect("ok")
    }

    fn schema_val() -> Schema {
        Schema::new([Field::required("val", DataType::Int32)]).expect("ok")
    }

    fn schema_joined() -> Schema {
        Schema::new([
            Field::required("id", DataType::Int32),
            Field::required("val", DataType::Int32),
        ])
        .expect("ok")
    }

    fn i32_batch(vals: &[i32]) -> Batch {
        Batch::new([Column::Int32(NumericColumn::from_data(vals.to_vec()))]).expect("ok")
    }

    fn col_i32(name: &str) -> ScalarExpr {
        ScalarExpr::Column {
            name: name.into(),
            index: 0,
            data_type: DataType::Int32,
        }
    }

    fn lit_i32(v: i32) -> ScalarExpr {
        ScalarExpr::Literal {
            value: Value::Int32(v),
            data_type: DataType::Int32,
        }
    }

    fn divide_i32_by_zero(name: &str) -> ScalarExpr {
        ScalarExpr::Binary {
            op: BinaryOp::Div,
            left: Box::new(col_i32(name)),
            right: Box::new(lit_i32(0)),
            data_type: DataType::Int32,
        }
    }

    fn drain_pairs(op: &mut dyn Operator) -> Vec<(i32, i32)> {
        let schema = op.schema().clone();
        let mut out = Vec::new();
        while let Some(b) = op.next_batch().expect("ok") {
            let rows = batch_to_rows(&b, &schema).expect("decode");
            for row in rows {
                let l = match &row[0] {
                    Value::Int32(v) => *v,
                    _ => 0,
                };
                let r = match &row[1] {
                    Value::Int32(v) => *v,
                    _ => 0,
                };
                out.push((l, r));
            }
        }
        out
    }

    #[test]
    fn merge_join_inner_happy_path() {
        let left = MemTableScan::new(schema_id(), vec![i32_batch(&[1, 2, 3, 4])]);
        let right = MemTableScan::new(schema_val(), vec![i32_batch(&[2, 3, 5])]);
        let mut op = MergeJoin::new(
            Box::new(left),
            Box::new(right),
            col_i32("id"),
            col_i32("val"),
            LogicalJoinType::Inner,
            schema_joined(),
            schema_id(),
            schema_val(),
        );
        let mut rows = drain_pairs(&mut op);
        rows.sort_unstable();
        assert_eq!(rows, vec![(2, 2), (3, 3)]);
    }

    #[test]
    fn merge_join_key_eval_error_propagates() {
        let left = MemTableScan::new(schema_id(), vec![i32_batch(&[1])]);
        let right = MemTableScan::new(schema_val(), vec![i32_batch(&[1])]);
        let mut op = MergeJoin::new(
            Box::new(left),
            Box::new(right),
            divide_i32_by_zero("id"),
            col_i32("val"),
            LogicalJoinType::Inner,
            schema_joined(),
            schema_id(),
            schema_val(),
        );

        let err = op.next_batch().expect_err("merge key division must error");
        assert!(
            err.to_string().contains("division by zero"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn merge_join_left_outer_unmatched() {
        let left = MemTableScan::new(schema_id(), vec![i32_batch(&[1, 2])]);
        let right = MemTableScan::new(schema_val(), vec![i32_batch(&[2])]);
        let mut op = MergeJoin::new(
            Box::new(left),
            Box::new(right),
            col_i32("id"),
            col_i32("val"),
            LogicalJoinType::LeftOuter,
            schema_joined(),
            schema_id(),
            schema_val(),
        );
        let mut rows = drain_pairs(&mut op);
        rows.sort_unstable();
        // (1, NULL encoded as 0), (2, 2)
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().any(|&(l, _)| l == 2));
        assert!(rows.iter().any(|&(l, _)| l == 1));
    }

    #[test]
    fn merge_join_empty_input_returns_none() {
        let left = MemTableScan::new(schema_id(), vec![]);
        let right = MemTableScan::new(schema_val(), vec![i32_batch(&[1])]);
        let mut op = MergeJoin::new(
            Box::new(left),
            Box::new(right),
            col_i32("id"),
            col_i32("val"),
            LogicalJoinType::Inner,
            schema_joined(),
            schema_id(),
            schema_val(),
        );
        assert!(drain_pairs(&mut op).is_empty());
    }

    #[test]
    fn merge_join_cross_returns_unsupported() {
        let left = MemTableScan::new(schema_id(), vec![]);
        let right = MemTableScan::new(schema_val(), vec![]);
        let mut op = MergeJoin::new(
            Box::new(left),
            Box::new(right),
            col_i32("id"),
            col_i32("val"),
            LogicalJoinType::Cross,
            schema_joined(),
            schema_id(),
            schema_val(),
        );
        let err = op.next_batch().expect_err("cross must fail");
        assert!(matches!(err, ExecError::Unsupported(_)));
    }
}
