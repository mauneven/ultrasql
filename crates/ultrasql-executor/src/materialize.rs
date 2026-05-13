//! Materialize pipeline-breaker operator.
//!
//! [`Materialize`] drains its child operator to completion on the first
//! [`Operator::next_batch`] call, stores every batch, and then replays
//! the buffered batches on subsequent calls. This breaks the pipeline at
//! this point so that child output can be rescanned without re-executing
//! the child plan.
//!
//! # Use cases
//!
//! - Right side of a `NestedLoopJoin` (child is rescanned for each left row).
//! - CTE materialisation (the definition is executed once and replayed).
//! - Subquery decorrelation checkpoints.
//!
//! # v0.5 trade-off
//!
//! The entire child output is held in memory. A future `work_mem`-aware
//! variant will spill excess batches to a temp file; for v0.5 this is
//! acceptable given the size constraints of development workloads.

use ultrasql_core::Schema;
use ultrasql_vec::Batch;

use crate::{ExecError, Operator};

/// Pipeline-breaker operator that materialises its child's full output.
///
/// The child is drained completely on the first `next_batch` call; after
/// that the stored batches are replayed in order. A [`reset`] method is
/// provided for operators that need to re-scan the materialised data.
///
/// # Send
///
/// `Box<dyn Operator>`, `Vec<Batch>`, and `Schema` are all `Send`.
///
/// [`reset`]: Materialize::reset
#[derive(Debug)]
pub struct Materialize {
    child: Box<dyn Operator>,
    schema: Schema,
    /// Materialised batch buffer. `None` until the child has been drained.
    buffer: Option<Vec<Batch>>,
    /// Next batch index to emit during the replay phase.
    cursor: usize,
    eof: bool,
}

impl Materialize {
    /// Construct a materialize operator.
    ///
    /// The child schema is captured at construction time; `schema` must
    /// match the child's schema.
    #[must_use]
    pub fn new(child: Box<dyn Operator>) -> Self {
        let schema = child.schema().clone();
        Self {
            child,
            schema,
            buffer: None,
            cursor: 0,
            eof: false,
        }
    }

    /// Reset the replay cursor so the materialised buffer is re-read from
    /// the beginning on the next `next_batch` call.
    ///
    /// This does **not** re-execute the child; it only rewinds the read
    /// cursor over the already-materialised batches.
    pub const fn reset(&mut self) {
        self.cursor = 0;
        self.eof = false;
    }
}

impl Operator for Materialize {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.eof {
            return Ok(None);
        }

        // Materialise the child on first call.
        if self.buffer.is_none() {
            let mut buf: Vec<Batch> = Vec::new();
            loop {
                match self.child.next_batch()? {
                    None => break,
                    Some(batch) => buf.push(batch),
                }
            }
            self.buffer = Some(buf);
        }

        let buf = self.buffer.as_ref().expect("just-set");
        if self.cursor >= buf.len() {
            self.eof = true;
            return Ok(None);
        }
        let batch = buf[self.cursor].clone();
        self.cursor += 1;
        Ok(Some(batch))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

#[cfg(test)]
mod tests {
    use ultrasql_core::{DataType, Field, Schema};
    use ultrasql_vec::Batch;
    use ultrasql_vec::column::{Column, NumericColumn};

    use super::Materialize;
    use crate::Operator;
    use crate::mem_table_scan::MemTableScan;

    fn schema_i32() -> Schema {
        Schema::new([Field::required("v", DataType::Int32)]).expect("schema ok")
    }

    fn i32_batch(vals: &[i32]) -> Batch {
        Batch::new([Column::Int32(NumericColumn::from_data(vals.to_vec()))]).expect("batch ok")
    }

    #[test]
    fn materialize_drains_and_replays_batches() {
        let scan = MemTableScan::new(schema_i32(), vec![i32_batch(&[1, 2]), i32_batch(&[3])]);
        let mut mat = Materialize::new(Box::new(scan));
        let b1 = mat.next_batch().expect("ok").expect("first");
        assert_eq!(b1.rows(), 2);
        let b2 = mat.next_batch().expect("ok").expect("second");
        assert_eq!(b2.rows(), 1);
        assert!(mat.next_batch().expect("ok").is_none());
    }

    #[test]
    fn materialize_empty_child_returns_none() {
        let scan = MemTableScan::new(schema_i32(), vec![]);
        let mut mat = Materialize::new(Box::new(scan));
        assert!(mat.next_batch().expect("ok").is_none());
    }

    #[test]
    fn materialize_reset_replays_from_start() {
        let scan = MemTableScan::new(schema_i32(), vec![i32_batch(&[10, 20, 30])]);
        let mut mat = Materialize::new(Box::new(scan));
        // Drain fully.
        while mat.next_batch().expect("ok").is_some() {}
        // Reset and re-read.
        mat.reset();
        let b = mat.next_batch().expect("ok").expect("replayed");
        assert_eq!(b.rows(), 3);
    }
}
