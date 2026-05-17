//! CTE scan operator.
//!
//! [`CteScan`] is a leaf operator that replays a materialised `Vec<Batch>`
//! captured from a CTE's definition plan. The materialised buffer is
//! produced once by running the definition plan to completion and is then
//! shared across all scan sites via `Arc`.
//!
//! # Re-scan support
//!
//! `CteScan` supports repeated `next_batch` calls across multiple scans by
//! resetting the batch index to zero when the caller resets the operator.
//! This enables multiple references to the same CTE inside the outer query.
//!
//! # v0.5 limitation
//!
//! Recursive CTEs (`WITH RECURSIVE`) are not executed recursively; the
//! recursive flag is preserved by the planner but the fixpoint loop is a
//! v0.6 follow-up. A non-recursive CTE binding is used as-is.

use std::sync::Arc;

use ultrasql_core::Schema;
use ultrasql_vec::Batch;

use crate::{ExecError, Operator};

/// CTE materialisation scan.
///
/// Replays a pre-computed sequence of [`Batch`]es one at a time. The
/// batches are shared behind an `Arc` so multiple scan sites in the same
/// query can reference the same buffer without copying.
///
/// # Send
///
/// `Arc<Vec<Batch>>` and `Schema` are both `Send + Sync`.
#[derive(Debug)]
pub struct CteScan {
    batches: Arc<Vec<Batch>>,
    schema: Schema,
    /// Next batch index to return.
    cursor: usize,
    eof: bool,
}

impl CteScan {
    /// Construct a CTE scan over a materialised batch buffer.
    ///
    /// - `batches` — the full materialised output of the CTE definition plan.
    /// - `schema` — the output schema, which must match every batch in `batches`.
    #[must_use]
    pub const fn new(batches: Arc<Vec<Batch>>, schema: Schema) -> Self {
        Self {
            batches,
            schema,
            cursor: 0,
            eof: false,
        }
    }

    /// Reset the scan to replay from the beginning.
    ///
    /// After reset the next [`Operator::next_batch`] call emits the first
    /// batch again. This is used when the same CTE is referenced more than
    /// once in the outer query.
    pub const fn reset(&mut self) {
        self.cursor = 0;
        self.eof = false;
    }
}

impl Operator for CteScan {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.eof {
            return Ok(None);
        }
        if self.cursor >= self.batches.len() {
            self.eof = true;
            return Ok(None);
        }
        let batch = self.batches[self.cursor].clone();
        self.cursor += 1;
        Ok(Some(batch))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn estimated_row_count(&self) -> Option<usize> {
        Some(self.batches.iter().map(Batch::rows).sum())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use ultrasql_core::{DataType, Field, Schema};
    use ultrasql_vec::Batch;
    use ultrasql_vec::column::{Column, NumericColumn};

    use super::CteScan;
    use crate::Operator;

    fn schema_i32() -> Schema {
        Schema::new([Field::required("v", DataType::Int32)]).expect("schema ok")
    }

    fn i32_batch(vals: &[i32]) -> Batch {
        Batch::new([Column::Int32(NumericColumn::from_data(vals.to_vec()))]).expect("batch ok")
    }

    #[test]
    fn cte_scan_emits_all_batches_in_order() {
        let batches = Arc::new(vec![i32_batch(&[1, 2]), i32_batch(&[3, 4])]);
        let mut scan = CteScan::new(batches, schema_i32());
        let b1 = scan.next_batch().expect("no error").expect("first batch");
        assert_eq!(b1.rows(), 2);
        let b2 = scan.next_batch().expect("no error").expect("second batch");
        assert_eq!(b2.rows(), 2);
        assert!(scan.next_batch().expect("no error").is_none());
    }

    #[test]
    fn cte_scan_empty_batches_returns_none_immediately() {
        let mut scan = CteScan::new(Arc::new(vec![]), schema_i32());
        assert!(scan.next_batch().expect("no error").is_none());
    }

    #[test]
    fn cte_scan_reset_replays_from_start() {
        let batches = Arc::new(vec![i32_batch(&[10, 20])]);
        let mut scan = CteScan::new(batches, schema_i32());
        scan.next_batch().expect("no error");
        assert!(scan.next_batch().expect("no error").is_none());
        scan.reset();
        let b = scan
            .next_batch()
            .expect("no error")
            .expect("replayed batch");
        assert_eq!(b.rows(), 2);
    }

    #[test]
    fn cte_scan_reports_materialized_row_count_hint() {
        let batches = Arc::new(vec![i32_batch(&[1, 2]), i32_batch(&[3, 4, 5])]);
        let scan = CteScan::new(batches, schema_i32());
        assert_eq!(scan.estimated_row_count(), Some(5));
    }
}
