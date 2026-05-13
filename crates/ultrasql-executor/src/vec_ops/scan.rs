//! Vectorized sequential scan operator.
//!
//! [`VectorizedSeqScan`] wraps any [`Operator`] that produces batches and
//! re-emits them via the push [`VectorizedSink`] protocol.
//!
//! In production the underlying `Operator` is a [`SeqScan`]; in tests it is
//! a [`MemTableScan`]. The vectorized wrapper adds no semantics — it exists
//! to bridge the pull model into the push pipeline.

use ultrasql_core::Schema;

use crate::push_pipeline::{SinkVerdict, VectorizedOperator, VectorizedSink};
use crate::{ExecError, Operator};

/// Vectorized sequential scan operator.
///
/// Drains the inner pull-based `Operator` and pushes each batch into the
/// sink.  The inner operator is responsible for MVCC visibility and batch
/// sizing (4096 rows per batch per the `ARCHITECTURE.md` contract).
pub struct VectorizedSeqScan {
    inner: Box<dyn Operator>,
}

impl std::fmt::Debug for VectorizedSeqScan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VectorizedSeqScan")
            .field("schema", self.inner.schema())
            .finish_non_exhaustive()
    }
}

impl VectorizedSeqScan {
    /// Wrap a pull-based operator in a push-based scan.
    ///
    /// `inner` is the source operator (typically `SeqScan` or `MemTableScan`
    /// for tests).
    #[must_use]
    pub fn new(inner: Box<dyn Operator>) -> Self {
        Self { inner }
    }
}

impl VectorizedOperator for VectorizedSeqScan {
    fn drive(&mut self, sink: &mut dyn VectorizedSink) -> Result<(), ExecError> {
        loop {
            let Some(batch) = self.inner.next_batch()? else {
                break;
            };
            if batch.is_empty() {
                continue;
            }
            if sink.consume(batch)? == SinkVerdict::Stop {
                return Ok(());
            }
        }
        Ok(())
    }

    fn schema(&self) -> &Schema {
        self.inner.schema()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use ultrasql_core::{DataType, Field, Schema};
    use ultrasql_vec::Batch;
    use ultrasql_vec::column::{Column, NumericColumn};

    use super::*;
    use crate::mem_table_scan::MemTableScan;
    use crate::push_pipeline::CollectSink;

    fn schema() -> Schema {
        Schema::new([Field::required("id", DataType::Int32)]).expect("schema ok")
    }

    fn make_batch(data: &[i32]) -> Batch {
        Batch::new([Column::Int32(NumericColumn::from_data(data.to_vec()))]).unwrap()
    }

    #[test]
    fn scan_emits_all_batches() {
        let scan = MemTableScan::new(schema(), vec![make_batch(&[1, 2, 3]), make_batch(&[4, 5])]);
        let mut op = VectorizedSeqScan::new(Box::new(scan));
        let mut sink = CollectSink::new();
        op.drive(&mut sink).unwrap();
        let batches = sink.finish();
        let total: usize = batches.iter().map(Batch::rows).sum();
        assert_eq!(total, 5);
    }

    #[test]
    fn scan_empty_source_emits_nothing() {
        let scan = MemTableScan::new(schema(), vec![]);
        let mut op = VectorizedSeqScan::new(Box::new(scan));
        let mut sink = CollectSink::new();
        op.drive(&mut sink).unwrap();
        assert!(sink.finish().is_empty());
    }

    #[test]
    fn scan_stops_on_sink_stop() {
        let scan = MemTableScan::new(schema(), vec![make_batch(&[1, 2, 3]), make_batch(&[4, 5])]);
        let mut op = VectorizedSeqScan::new(Box::new(scan));
        let mut sink = CollectSink::with_limit(3);
        op.drive(&mut sink).unwrap();
        // At most 2 batches consumed before limit
        let batches = sink.finish();
        assert!(batches.len() <= 2);
    }
}
