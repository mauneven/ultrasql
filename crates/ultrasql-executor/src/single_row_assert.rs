//! Single-row guard for scalar subqueries used as expressions.
//!
//! SQL requires an uncorrelated scalar subquery (a `SELECT` used in a
//! value position, e.g. `SELECT a, (SELECT id FROM s) FROM t`) to return
//! **exactly one** row. `SingleRowAssert` enforces that contract at
//! runtime so the decorrelation rewrite can lower the subquery to a
//! `CROSS JOIN` against its (now guaranteed single-row) right side:
//!
//! - **child emits 1 row** → that row passes through unchanged;
//! - **child emits 0 rows** → a single all-NULL row is emitted, so a
//!   `CROSS JOIN` keeps every outer row and NULL-pads the scalar
//!   (PostgreSQL semantics: an empty scalar subquery is NULL, it does
//!   **not** drop the outer row);
//! - **child emits >1 rows** → [`ExecError::CardinalityViolation`]
//!   (PostgreSQL SQLSTATE `21000`, "more than one row returned by a
//!   subquery used as an expression").
//!
//! The operator is eager: the first [`Operator::next_batch`] call drains
//! the child until it has seen a second row (at which point it errors
//! *before* emitting anything) or end-of-stream. It therefore never
//! returns a partial result ahead of the cardinality error.

use ultrasql_core::{Schema, Value};
use ultrasql_vec::Batch;

use crate::seq_scan::build_batch;
use crate::{ExecError, Operator, batch_to_rows};

/// Enforce the "exactly one row" contract of a scalar subquery.
///
/// See the module documentation for the 0 / 1 / >1 row semantics. The
/// output schema is the child's schema unchanged; only the row count is
/// constrained (always exactly one output row, NULL-padded on an empty
/// child).
#[derive(Debug)]
pub struct SingleRowAssert {
    child: Box<dyn Operator>,
    schema: Schema,
    /// Set once the single output row has been produced (or the
    /// NULL-padded empty-input row emitted), so subsequent calls return
    /// end-of-stream without re-draining the child.
    done: bool,
}

impl SingleRowAssert {
    /// Wrap `child`, asserting it yields at most one row and substituting
    /// a NULL-padded row when it yields none.
    #[must_use]
    pub fn new(child: Box<dyn Operator>) -> Self {
        let schema = child.schema().clone();
        Self {
            child,
            schema,
            done: false,
        }
    }

    /// Build the single all-NULL output row used when the child is empty.
    fn null_row(&self) -> Result<Batch, ExecError> {
        let row = vec![Value::Null; self.schema.len()];
        build_batch(std::slice::from_ref(&row), &self.schema)
    }
}

impl Operator for SingleRowAssert {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.done {
            return Ok(None);
        }
        self.done = true;

        // Drain the child, retaining the (at most one) row it is allowed
        // to produce. A second row anywhere in the stream is a hard
        // cardinality error, raised before any output is emitted.
        let mut kept: Option<Vec<Value>> = None;
        while let Some(batch) = self.child.next_batch()? {
            let rows = batch_to_rows(&batch, &self.schema)?;
            for row in rows {
                if kept.is_some() {
                    return Err(ExecError::CardinalityViolation);
                }
                kept = Some(row);
            }
        }

        match kept {
            // Exactly one row: emit it verbatim (re-encoded through the
            // declared schema so the output column types/null-bitmaps are
            // canonical and independent of the child's batch layout).
            Some(row) => build_batch(std::slice::from_ref(&row), &self.schema).map(Some),
            // Empty child: emit a single all-NULL row.
            None => self.null_row().map(Some),
        }
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn estimated_row_count(&self) -> Option<usize> {
        // Always exactly one output row.
        Some(1)
    }

    fn profile_children(&self) -> Vec<&dyn Operator> {
        vec![self.child.as_ref()]
    }
}

#[cfg(test)]
mod tests {
    use ultrasql_core::{DataType, Field, Schema};
    use ultrasql_vec::column::{Column, NumericColumn};

    use super::*;
    use crate::MemTableScan;

    fn schema() -> Schema {
        Schema::new([Field::nullable("id", DataType::Int32)]).expect("schema is well-formed")
    }

    fn int_batch(rows: &[i32]) -> Batch {
        Batch::new([Column::Int32(NumericColumn::from_data(rows.to_vec()))])
            .expect("batch is well-formed")
    }

    #[test]
    fn single_row_passes_through() {
        let scan = MemTableScan::new(schema(), vec![int_batch(&[9])]);
        let mut op = SingleRowAssert::new(Box::new(scan));
        let batch = op.next_batch().unwrap().expect("one row");
        assert_eq!(batch.rows(), 1);
        match &batch.columns()[0] {
            Column::Int32(c) => {
                assert_eq!(c.data()[0], 9);
                assert!(
                    c.nulls().is_none_or(|n| n.get(0)),
                    "the single row is non-NULL"
                );
            }
            other => panic!("unexpected column: {other:?}"),
        }
        assert!(op.next_batch().unwrap().is_none(), "stream is exhausted");
    }

    #[test]
    fn empty_child_emits_one_null_row() {
        let scan = MemTableScan::new(schema(), Vec::new());
        let mut op = SingleRowAssert::new(Box::new(scan));
        let batch = op.next_batch().unwrap().expect("one NULL-padded row");
        assert_eq!(batch.rows(), 1, "empty child yields exactly one row");
        match &batch.columns()[0] {
            Column::Int32(c) => {
                let nulls = c
                    .nulls()
                    .expect("empty-input row carries a validity bitmap");
                assert!(!nulls.get(0), "the single row is NULL");
            }
            other => panic!("unexpected column: {other:?}"),
        }
        assert!(op.next_batch().unwrap().is_none());
    }

    #[test]
    fn multi_row_raises_cardinality_violation() {
        let scan = MemTableScan::new(schema(), vec![int_batch(&[1, 2])]);
        let mut op = SingleRowAssert::new(Box::new(scan));
        let err = op.next_batch().expect_err("two rows must error");
        assert!(matches!(err, ExecError::CardinalityViolation));
    }

    #[test]
    fn multi_row_across_batches_raises_cardinality_violation() {
        let scan = MemTableScan::new(schema(), vec![int_batch(&[1]), int_batch(&[2])]);
        let mut op = SingleRowAssert::new(Box::new(scan));
        let err = op
            .next_batch()
            .expect_err("two rows across batches must error");
        assert!(matches!(err, ExecError::CardinalityViolation));
    }
}
