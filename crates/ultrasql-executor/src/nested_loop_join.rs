//! Nested-loop join operator.
//!
//! Implements all SQL join types using a left-driven nested loop. For each
//! left row the right child is re-opened via a caller-supplied factory
//! closure, scanned in full, and matched against the join condition.
//!
//! # Join types
//!
//! | Join type | Behaviour |
//! |-----------|-----------|
//! | `Inner`   | Emit matched pairs only. |
//! | `LeftOuter` | Emit all left rows; unmatched left rows pad right with NULLs. |
//! | `RightOuter` | Re-scan right at the end to emit unmatched right rows padded with left NULLs. |
//! | `FullOuter` | Combination of `LeftOuter` and `RightOuter`. |
//! | `Cross`   | Ignore condition; Cartesian product. |
//! | `Semi`    | Emit each left row once when at least one right row matches. |
//! | `Anti`    | Emit each left row once when no right row matches. |
//!
//! # Right rescan
//!
//! The right child is recreated per left row by calling `right_factory`.
//! Callers responsible for cheap re-creation (typically
//! `|| Ok(Box::new(MemTableScan::new(schema, batches.clone())))` — an
//! O(1) clone of an `Arc`-backed batch).
//!
//! # v0.5 limitation
//!
//! Both sides are materialised as rows in memory per left row, which is
//! O(|right|) per left row. A streaming probe path will arrive in a
//! future wave alongside join spilling.

use ultrasql_core::{Schema, Value};
use ultrasql_planner::{LogicalJoinType, ScalarExpr};
use ultrasql_vec::Batch;

use crate::eval::Eval;
use crate::filter_op::batch_to_rows;
use crate::seq_scan::build_batch;
use crate::{ExecError, Operator};

/// Maximum rows per emitted batch, matching the `ARCHITECTURE.md` section 9 contract.
const BATCH_TARGET_ROWS: usize = 4096;

/// Factory type for recreating the right child on each left-row iteration.
///
/// The closure takes no arguments and returns a fresh `Box<dyn Operator>`
/// positioned at the start of the right-side scan. The closure is `Send`
/// so the operator itself satisfies the `Operator: Send` bound.
pub type RightFactory = Box<dyn Fn() -> Result<Box<dyn Operator>, ExecError> + Send>;

/// Nested-loop join operator.
///
/// Drives the join by iterating over left rows. For each left row it opens
/// a fresh right scan via `right_factory`, applies the join condition, and
/// emits output rows. All join semantics (inner, outer, cross) are
/// implemented inside this single operator.
///
/// # Send bound
///
/// All fields are `Send`: `Box<dyn Operator>`, `RightFactory`, `Option<Eval>`,
/// and `Schema`.
pub struct NestedLoopJoin {
    left: Box<dyn Operator>,
    right_factory: RightFactory,
    join_type: LogicalJoinType,
    /// `None` for CROSS JOIN, `Some(Eval)` otherwise.
    condition: Option<Eval>,
    schema: Schema,
    /// Left schema (for padding right-outer unmatched rows).
    left_schema: Schema,
    /// Right schema (for padding left-outer unmatched rows).
    right_schema: Schema,
    /// Output row buffer built during execution.
    output: Option<std::vec::IntoIter<Vec<Value>>>,
    /// Whether the main (left-driven) phase has completed.
    left_phase_done: bool,
    /// `true` after the final `Ok(None)` is returned.
    eof: bool,
}

impl std::fmt::Debug for NestedLoopJoin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NestedLoopJoin")
            .field("join_type", &self.join_type)
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

impl NestedLoopJoin {
    /// Construct a nested-loop join.
    ///
    /// - `left` — the left (outer) child operator.
    /// - `right_factory` — closure that returns a fresh right (inner) scan
    ///   positioned at the first row.
    /// - `join_type` — the SQL join variant.
    /// - `condition` — the join predicate; `None` for CROSS JOIN.
    /// - `schema` — the output schema (left columns followed by right columns).
    /// - `left_schema` — schema of the left child's output; used to build
    ///   NULL padding rows for right-outer unmatched rows.
    /// - `right_schema` — schema of the right child's output; used to build
    ///   NULL padding rows for left-outer unmatched rows.
    #[must_use]
    pub fn new(
        left: Box<dyn Operator>,
        right_factory: RightFactory,
        join_type: LogicalJoinType,
        condition: Option<ScalarExpr>,
        schema: Schema,
        left_schema: Schema,
        right_schema: Schema,
    ) -> Self {
        Self {
            left,
            right_factory,
            join_type,
            condition: condition.map(Eval::new),
            schema,
            left_schema,
            right_schema,
            output: None,
            left_phase_done: false,
            eof: false,
        }
    }
}

impl Operator for NestedLoopJoin {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.eof {
            return Ok(None);
        }

        // Build all output rows on the first call.
        if !self.left_phase_done {
            let rows = self.execute()?;
            self.output = Some(rows.into_iter());
            self.left_phase_done = true;
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

    fn profile_children(&self) -> Vec<&dyn Operator> {
        vec![self.left.as_ref()]
    }
}

impl NestedLoopJoin {
    /// Execute the full nested-loop and return all output rows.
    fn execute(&mut self) -> Result<Vec<Vec<Value>>, ExecError> {
        // Materialise all left rows.
        let left_rows = drain_operator(&mut *self.left, &self.left_schema)?;

        // For Right/Full outer joins, track which right rows were matched.
        // We need to materialise the right side once for this bookkeeping.
        let needs_right_unmatched = matches!(
            self.join_type,
            LogicalJoinType::RightOuter | LogicalJoinType::FullOuter
        );
        let right_rows_for_tracking: Vec<Vec<Value>> = if needs_right_unmatched {
            let mut right_op = (self.right_factory)()?;
            drain_operator(&mut *right_op, &self.right_schema)?
        } else {
            Vec::new()
        };
        let mut right_matched: Vec<bool> = vec![false; right_rows_for_tracking.len()];

        let mut output: Vec<Vec<Value>> = Vec::new();

        let null_left = null_row(&self.left_schema);
        let null_right = null_row(&self.right_schema);

        for left_row in &left_rows {
            let mut left_matched = false;

            let right_rows_iter: Box<dyn Iterator<Item = (usize, &Vec<Value>)>> =
                if needs_right_unmatched {
                    Box::new(right_rows_for_tracking.iter().enumerate())
                } else {
                    // Open a fresh right scan.
                    let mut right_op = (self.right_factory)()?;
                    let fresh_rows = drain_operator(&mut *right_op, &self.right_schema)?;
                    // We need owned right rows here.
                    let right_rows_owned: Vec<Vec<Value>> = fresh_rows;
                    // Box an iterator over the owned vec — but we can't borrow
                    // a temporary here. Instead materialise into a local and
                    // iterate below via a different branch.
                    //
                    // This branch is reached only when needs_right_unmatched
                    // is false so right_rows_for_tracking is empty. We handle
                    // the non-tracking path separately below.
                    drop(right_rows_owned); // will be re-done below
                    Box::new(std::iter::empty())
                };

            if needs_right_unmatched {
                for (ri, right_row) in right_rows_iter {
                    let joined = concat_rows(left_row, right_row);
                    if self.passes_condition(&joined)? {
                        output.push(joined);
                        left_matched = true;
                        right_matched[ri] = true;
                    }
                }
            } else {
                // Fresh right scan per left row.
                let mut right_op = (self.right_factory)()?;
                let right_rows = drain_operator(&mut *right_op, &self.right_schema)?;
                for right_row in &right_rows {
                    let joined = concat_rows(left_row, right_row);
                    if self.passes_condition(&joined)? {
                        left_matched = true;
                        match self.join_type {
                            LogicalJoinType::Semi | LogicalJoinType::Anti => break,
                            _ => output.push(joined),
                        }
                    }
                }
            }

            match self.join_type {
                LogicalJoinType::Semi if left_matched => output.push(left_row.clone()),
                LogicalJoinType::Anti if !left_matched => output.push(left_row.clone()),
                _ => {}
            }

            // Left/Full outer: emit left ++ NULLs when no match.
            if !left_matched
                && matches!(
                    self.join_type,
                    LogicalJoinType::LeftOuter | LogicalJoinType::FullOuter
                )
            {
                output.push(concat_rows(left_row, &null_right));
            }
        }

        // Right/Full outer: emit NULLs ++ unmatched right rows.
        if needs_right_unmatched {
            for (ri, right_row) in right_rows_for_tracking.iter().enumerate() {
                if !right_matched[ri] {
                    output.push(concat_rows(&null_left, right_row));
                }
            }
        }

        Ok(output)
    }

    /// Evaluate the join condition against a joined row.
    ///
    /// Returns `true` if the row passes (or if there is no condition, i.e.
    /// CROSS JOIN). Returns `false` for a `false` or NULL predicate result
    /// (SQL 3VL).
    fn passes_condition(&self, row: &[Value]) -> Result<bool, ExecError> {
        let Some(cond) = &self.condition else {
            // CROSS JOIN — no condition.
            return Ok(true);
        };
        match cond.eval(row) {
            Ok(Value::Bool(true)) => Ok(true),
            Ok(Value::Bool(false) | Value::Null) => Ok(false),
            Ok(other) => Err(ExecError::TypeMismatch(format!(
                "join condition must evaluate to Bool or Null, got {:?}",
                other.data_type()
            ))),
            Err(e) => Err(ExecError::TypeMismatch(e.to_string())),
        }
    }
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

/// Concatenate two rows into one.
fn concat_rows(left: &[Value], right: &[Value]) -> Vec<Value> {
    let mut row = Vec::with_capacity(left.len() + right.len());
    row.extend_from_slice(left);
    row.extend_from_slice(right);
    row
}

/// Build a row of NULLs matching `schema`'s width.
fn null_row(schema: &Schema) -> Vec<Value> {
    vec![Value::Null; schema.len()]
}

/// Drain all batches from `op` and return as a flat vec of rows.
fn drain_operator(op: &mut dyn Operator, schema: &Schema) -> Result<Vec<Vec<Value>>, ExecError> {
    let mut rows = Vec::new();
    loop {
        match op.next_batch()? {
            None => break,
            Some(batch) => {
                let decoded = batch_to_rows(&batch, schema)?;
                rows.extend(decoded);
            }
        }
    }
    Ok(rows)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use ultrasql_core::{DataType, Field, Schema, Value};
    use ultrasql_planner::{BinaryOp, LogicalJoinType, ScalarExpr};
    use ultrasql_vec::Batch;
    use ultrasql_vec::column::{Column, NumericColumn};

    use super::NestedLoopJoin;
    use crate::Operator;
    use crate::mem_table_scan::MemTableScan;

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    fn schema_id() -> Schema {
        Schema::new([Field::required("id", DataType::Int32)]).expect("schema ok")
    }

    fn schema_val() -> Schema {
        Schema::new([Field::required("val", DataType::Int32)]).expect("schema ok")
    }

    fn schema_id_val() -> Schema {
        Schema::new([
            Field::required("id", DataType::Int32),
            Field::required("val", DataType::Int32),
        ])
        .expect("schema ok")
    }

    fn i32_batch(rows: &[i32]) -> Batch {
        Batch::new([Column::Int32(NumericColumn::from_data(rows.to_vec()))]).expect("batch ok")
    }

    fn make_right_factory(schema: Schema, batches: Vec<Batch>) -> super::RightFactory {
        Box::new(move || {
            Ok(Box::new(MemTableScan::new(schema.clone(), batches.clone())) as Box<dyn Operator>)
        })
    }

    fn drain_rows_i32_i32(op: &mut dyn Operator) -> Vec<(i32, i32)> {
        let schema = op.schema().clone();
        let mut out = Vec::new();
        while let Some(b) = op.next_batch().expect("no error") {
            let rows = crate::filter_op::batch_to_rows(&b, &schema).expect("decode ok");
            for row in rows {
                // batch_to_rows now reports `Value::Null` for the
                // padded-null right side of LEFT OUTER unmatched rows
                // (the NumericColumn validity bitmap distinguishes them
                // from real zeros). Map back to 0 here.
                let l = match &row[0] {
                    Value::Int32(v) => *v,
                    Value::Null => 0,
                    _ => panic!("unexpected left value: {:?}", row[0]),
                };
                let r = match &row[1] {
                    Value::Int32(v) => *v,
                    Value::Null => 0,
                    _ => panic!("unexpected right value: {:?}", row[1]),
                };
                out.push((l, r));
            }
        }
        out
    }

    fn drain_rows_i32(op: &mut dyn Operator) -> Vec<i32> {
        let schema = op.schema().clone();
        let mut out = Vec::new();
        while let Some(b) = op.next_batch().expect("no error") {
            let rows = crate::filter_op::batch_to_rows(&b, &schema).expect("decode ok");
            for row in rows {
                match &row[0] {
                    Value::Int32(v) => out.push(*v),
                    other => panic!("unexpected value: {other:?}"),
                }
            }
        }
        out
    }

    /// Predicate: left.id == right.val  (column 0 = column 1 in joined schema)
    fn pred_col0_eq_col1() -> ScalarExpr {
        ScalarExpr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(ScalarExpr::Column {
                name: "id".into(),
                index: 0,
                data_type: DataType::Int32,
            }),
            right: Box::new(ScalarExpr::Column {
                name: "val".into(),
                index: 1,
                data_type: DataType::Int32,
            }),
            data_type: DataType::Bool,
        }
    }

    // -------------------------------------------------------------------------
    // Test 1: INNER join happy path
    // -------------------------------------------------------------------------

    #[test]
    fn nlj_inner_join_happy_path() {
        // left: [1, 2, 3], right: [2, 3, 4]
        // Matches: (2,2), (3,3)
        let left = MemTableScan::new(schema_id(), vec![i32_batch(&[1, 2, 3])]);
        let right_batches = vec![i32_batch(&[2, 3, 4])];
        let factory = make_right_factory(schema_val(), right_batches);

        let joined_schema = schema_id_val();
        let mut op = NestedLoopJoin::new(
            Box::new(left),
            factory,
            LogicalJoinType::Inner,
            Some(pred_col0_eq_col1()),
            joined_schema,
            schema_id(),
            schema_val(),
        );
        let mut rows = drain_rows_i32_i32(&mut op);
        rows.sort_unstable();
        assert_eq!(rows, vec![(2, 2), (3, 3)]);
    }

    // -------------------------------------------------------------------------
    // Test 2: empty left input returns no rows
    // -------------------------------------------------------------------------

    #[test]
    fn nlj_empty_left_returns_no_rows() {
        let left = MemTableScan::new(schema_id(), vec![]);
        let right_batches = vec![i32_batch(&[1, 2, 3])];
        let factory = make_right_factory(schema_val(), right_batches);
        let mut op = NestedLoopJoin::new(
            Box::new(left),
            factory,
            LogicalJoinType::Inner,
            Some(pred_col0_eq_col1()),
            schema_id_val(),
            schema_id(),
            schema_val(),
        );
        let rows = drain_rows_i32_i32(&mut op);
        assert!(rows.is_empty());
    }

    // -------------------------------------------------------------------------
    // Test 3: LEFT OUTER join — unmatched left rows emit NULLs on right
    // -------------------------------------------------------------------------

    #[test]
    fn nlj_left_outer_unmatched_rows_get_nulls() {
        // left: [1, 2], right: [2]
        // Inner match: (2,2). Left outer: also (1, NULL)
        let left = MemTableScan::new(schema_id(), vec![i32_batch(&[1, 2])]);
        let right_batches = vec![i32_batch(&[2])];
        let factory = make_right_factory(schema_val(), right_batches);
        let mut op = NestedLoopJoin::new(
            Box::new(left),
            factory,
            LogicalJoinType::LeftOuter,
            Some(pred_col0_eq_col1()),
            schema_id_val(),
            schema_id(),
            schema_val(),
        );
        let mut rows = drain_rows_i32_i32(&mut op);
        rows.sort_unstable();
        // build_batch encodes NULL as 0 for Int32 columns (v0.5 no-null-bitmap
        // format), so the decoded sentinel is 0, not i32::MIN.
        assert!(rows.contains(&(2, 2)), "matched pair present");
        assert!(
            rows.contains(&(1, 0)),
            "unmatched left row with NULL right (encoded as 0)"
        );
    }

    // -------------------------------------------------------------------------
    // Test 4: CROSS JOIN — Cartesian product
    // -------------------------------------------------------------------------

    #[test]
    fn nlj_cross_join_cartesian_product() {
        let left = MemTableScan::new(schema_id(), vec![i32_batch(&[1, 2])]);
        let right_batches = vec![i32_batch(&[10, 20, 30])];
        let factory = make_right_factory(schema_val(), right_batches);
        let mut op = NestedLoopJoin::new(
            Box::new(left),
            factory,
            LogicalJoinType::Cross,
            None, // no condition for CROSS JOIN
            schema_id_val(),
            schema_id(),
            schema_val(),
        );
        let rows = drain_rows_i32_i32(&mut op);
        // 2 left * 3 right = 6 rows
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn nlj_semi_join_emits_each_matching_left_row_once() {
        let left = MemTableScan::new(schema_id(), vec![i32_batch(&[1, 2, 2, 3])]);
        let factory = make_right_factory(schema_val(), vec![i32_batch(&[2, 2, 4])]);
        let mut op = NestedLoopJoin::new(
            Box::new(left),
            factory,
            LogicalJoinType::Semi,
            Some(pred_col0_eq_col1()),
            schema_id(),
            schema_id(),
            schema_val(),
        );
        let mut rows = drain_rows_i32(&mut op);
        rows.sort_unstable();
        assert_eq!(rows, vec![2, 2]);
    }

    #[test]
    fn nlj_anti_join_emits_unmatched_left_rows() {
        let left = MemTableScan::new(schema_id(), vec![i32_batch(&[1, 2, 3])]);
        let factory = make_right_factory(schema_val(), vec![i32_batch(&[2, 4])]);
        let mut op = NestedLoopJoin::new(
            Box::new(left),
            factory,
            LogicalJoinType::Anti,
            Some(pred_col0_eq_col1()),
            schema_id(),
            schema_id(),
            schema_val(),
        );
        let mut rows = drain_rows_i32(&mut op);
        rows.sort_unstable();
        assert_eq!(rows, vec![1, 3]);
    }
}
