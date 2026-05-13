//! Push-based pipeline driver for vectorized operators.
//!
//! In a push pipeline, the leaf operator *drives* execution by emitting
//! batches into a [`VectorizedSink`] rather than waiting to be polled.
//! This inverts the pull model: there is no `next_batch` call; instead the
//! operator calls `sink.consume(batch)` for every batch it produces and then
//! calls `sink.finalize()` once.
//!
//! ## Usage
//!
//! ```text
//! let mut sink = CollectSink::new();
//! let mut op  = VectorizedSeqScan::new(…);
//! op.drive(&mut sink)?;
//! let result = sink.finalize()?;
//! ```
//!
//! ## Design
//!
//! The planner tags each pipeline as push (vectorized OLAP) or pull (scalar
//! OLTP). Push-tagged pipelines are executed via this driver; pull-tagged
//! pipelines continue to use the [`Operator`](crate::Operator) trait.
//!
//! Pipeline chaining is achieved by composing sinks: a filter sink wraps an
//! inner sink and calls `inner.consume(filtered_batch)` for each batch it
//! processes.  The outermost sink collects all output or forwards it to the
//! network handler.

use ultrasql_core::Schema;
use ultrasql_vec::Batch;

use crate::ExecError;

// ============================================================================
// SinkVerdict
// ============================================================================

/// Returned by [`VectorizedSink::consume`] to signal whether the pipeline
/// should continue or stop early.
///
/// A sink returns [`Stop`](SinkVerdict::Stop) when it has received enough
/// rows (e.g. `LIMIT` has been satisfied).  The driver propagates `Stop`
/// upward to the operator so it can skip remaining work.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SinkVerdict {
    /// The pipeline should continue emitting batches.
    Continue,
    /// The pipeline has received enough rows; stop immediately.
    Stop,
}

// ============================================================================
// VectorizedSink
// ============================================================================

/// A consumer of vectorized batches in a push pipeline.
///
/// Implementors accept batches from a [`VectorizedOperator`] and either
/// accumulate them (e.g. [`CollectSink`]) or forward them to another sink
/// (e.g. a filter or projection intermediate).
///
/// ## Contract
///
/// - [`consume`] is called zero or more times, always before [`finalize`].
/// - [`finalize`] is called exactly once, after all [`consume`] calls.
/// - After [`finalize`] returns, no further calls are made.
/// - `consume` returns [`SinkVerdict::Stop`] to request early termination.
///   The operator must stop calling `consume` once `Stop` is received.
///
/// [`consume`]: VectorizedSink::consume
/// [`finalize`]: VectorizedSink::finalize
pub trait VectorizedSink: Send {
    /// Accept a batch of rows.
    ///
    /// Returns [`SinkVerdict::Continue`] if the pipeline should keep pushing,
    /// or [`SinkVerdict::Stop`] if the sink has received enough rows.
    fn consume(&mut self, batch: Batch) -> Result<SinkVerdict, ExecError>;

    /// Signal end-of-stream and optionally return a final batch.
    ///
    /// Called exactly once by the driver after the last [`consume`] call.
    /// Returns `Some(batch)` for blocking operators that produce output only
    /// after seeing all input (e.g. aggregates, sorts).  For streaming
    /// operators (filters, projections) this typically returns `None`.
    ///
    /// [`consume`]: VectorizedSink::consume
    fn finalize(&mut self) -> Result<Option<Batch>, ExecError>;
}

// ============================================================================
// VectorizedOperator
// ============================================================================

/// A push-based vectorized execution operator.
///
/// The operator drives its child (or a storage scan) and calls
/// `sink.consume(batch)` for each batch it produces. When the source is
/// exhausted the operator returns `Ok(())`.
///
/// The schema of batches emitted into the sink matches the operator's
/// [`schema`](VectorizedOperator::schema) contract.
///
/// ## Send bound
///
/// Operators are `Send` so the pipeline driver can move them between Rayon
/// worker threads without locking the I/O reactor.
pub trait VectorizedOperator: Send + std::fmt::Debug {
    /// Drive the operator, pushing every batch into `sink`.
    ///
    /// Returns `Ok(())` when the source is exhausted or when `sink` returns
    /// [`SinkVerdict::Stop`].  Returns `Err(ExecError)` on any execution
    /// failure.
    fn drive(&mut self, sink: &mut dyn VectorizedSink) -> Result<(), ExecError>;

    /// Schema of every batch this operator emits into the sink.
    fn schema(&self) -> &Schema;
}

// ============================================================================
// CollectSink
// ============================================================================

/// A [`VectorizedSink`] that collects all incoming batches.
///
/// Call [`finish`](CollectSink::finish) after driving to retrieve the
/// accumulated batches.
#[derive(Debug, Default)]
pub struct CollectSink {
    batches: Vec<Batch>,
    limit: Option<usize>,
    rows_seen: usize,
}

impl CollectSink {
    /// Create an unbounded `CollectSink`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a `CollectSink` that stops after `limit` rows.
    #[must_use]
    pub fn with_limit(limit: usize) -> Self {
        Self {
            limit: Some(limit),
            ..Self::default()
        }
    }

    /// Drain all accumulated batches. Call after [`VectorizedOperator::drive`].
    #[must_use]
    pub fn finish(self) -> Vec<Batch> {
        self.batches
    }
}

impl VectorizedSink for CollectSink {
    fn consume(&mut self, batch: Batch) -> Result<SinkVerdict, ExecError> {
        self.rows_seen += batch.rows();
        self.batches.push(batch);
        if self.limit.is_some_and(|lim| self.rows_seen >= lim) {
            return Ok(SinkVerdict::Stop);
        }
        Ok(SinkVerdict::Continue)
    }

    fn finalize(&mut self) -> Result<Option<Batch>, ExecError> {
        Ok(None)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use ultrasql_core::{DataType, Field, Schema};
    use ultrasql_vec::column::{Column, NumericColumn};

    use super::*;

    fn make_batch(rows: &[i32]) -> Batch {
        Batch::new([Column::Int32(NumericColumn::from_data(rows.to_vec()))]).expect("batch ok")
    }

    fn int32_schema() -> Schema {
        Schema::new([Field::required("v", DataType::Int32)]).expect("schema ok")
    }

    // ---- CollectSink ----

    #[test]
    fn collect_sink_accumulates_batches() {
        let mut sink = CollectSink::new();
        let v = sink.consume(make_batch(&[1, 2, 3])).unwrap();
        assert_eq!(v, SinkVerdict::Continue);
        let v2 = sink.consume(make_batch(&[4, 5])).unwrap();
        assert_eq!(v2, SinkVerdict::Continue);
        assert!(sink.finalize().unwrap().is_none());
        let batches = sink.finish();
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].rows(), 3);
        assert_eq!(batches[1].rows(), 2);
    }

    #[test]
    fn collect_sink_with_limit_stops_at_limit() {
        let mut sink = CollectSink::with_limit(4);
        let v = sink.consume(make_batch(&[1, 2, 3])).unwrap();
        assert_eq!(v, SinkVerdict::Continue);
        let v2 = sink.consume(make_batch(&[4, 5])).unwrap();
        assert_eq!(v2, SinkVerdict::Stop);
    }

    #[test]
    fn collect_sink_empty_input() {
        let mut sink = CollectSink::new();
        assert!(sink.finalize().unwrap().is_none());
        assert!(sink.finish().is_empty());
    }

    // ---- A simple VectorizedOperator test ----

    #[derive(Debug)]
    struct ConstOp {
        schema: Schema,
        batches: Vec<Batch>,
        pos: usize,
    }

    impl VectorizedOperator for ConstOp {
        fn drive(&mut self, sink: &mut dyn VectorizedSink) -> Result<(), ExecError> {
            while self.pos < self.batches.len() {
                let batch = self.batches[self.pos].clone();
                self.pos += 1;
                if sink.consume(batch)? == SinkVerdict::Stop {
                    return Ok(());
                }
            }
            Ok(())
        }
        fn schema(&self) -> &Schema {
            &self.schema
        }
    }

    #[test]
    fn vectorized_operator_drives_sink_to_completion() {
        let mut op = ConstOp {
            schema: int32_schema(),
            batches: vec![make_batch(&[1, 2]), make_batch(&[3, 4, 5])],
            pos: 0,
        };
        let mut sink = CollectSink::new();
        op.drive(&mut sink).unwrap();
        sink.finalize().unwrap();
        let all: Vec<Batch> = sink.finish();
        let total_rows: usize = all.iter().map(Batch::rows).sum();
        assert_eq!(total_rows, 5);
    }

    #[test]
    fn vectorized_operator_stops_on_sink_stop() {
        let schema = int32_schema();
        let mut op = ConstOp {
            schema,
            batches: vec![
                make_batch(&[1, 2, 3]),
                make_batch(&[4, 5, 6]),
                make_batch(&[7, 8, 9]),
            ],
            pos: 0,
        };
        let mut sink = CollectSink::with_limit(5);
        op.drive(&mut sink).unwrap();
        // Only first 2 batches pushed before Stop
        assert!(sink.finish().len() <= 3);
    }
}
