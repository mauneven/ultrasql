//! Concrete [`VectorizedSink`] implementations for the push pipeline.
//!
//! This module provides terminal sinks used by tests, benchmarks, and the
//! query executor to collect, aggregate, or count the output of a
//! [`VectorizedPipeline`](crate::push_pipeline::VectorizedPipeline).
//!
//! ## Available sinks
//!
//! | Sink | Purpose |
//! |---|---|
//! | [`SumSink`] | Running `i64` SUM over column 0 |
//! | [`CountSink`] | `COUNT(*)` — total row count |

use ultrasql_vec::Batch;
use ultrasql_vec::column::Column;

use crate::ExecError;
use crate::push_pipeline::{SinkVerdict, VectorizedSink};

// ============================================================================
// SumSink
// ============================================================================

/// A [`VectorizedSink`] that computes `SUM(col[0])` over `Int64` batches.
///
/// Accumulates a running sum of the first column of every incoming batch.
/// The column must be `Int64`; other column types return
/// [`ExecError::TypeMismatch`].
///
/// After driving the pipeline, call [`final_value`](SumSink::final_value) to
/// retrieve the accumulated sum and [`samples`](SumSink::samples) for the
/// total row count seen.
///
/// # When to use
///
/// Use `SumSink` as a terminal sink in analytical benchmarks and tests where
/// the query ends with a global `SUM` aggregate and intermediate
/// materialisation is undesirable.
#[derive(Debug, Default)]
pub struct SumSink {
    /// Running sum of column 0 across all consumed batches.
    sum: i64,
    /// Total number of rows seen.
    samples: u64,
}

impl SumSink {
    /// Create a new `SumSink` with zero initial state.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Return the accumulated sum.
    ///
    /// Valid after the pipeline's [`finalize`](VectorizedSink::finalize) has
    /// been called.
    #[must_use]
    pub const fn final_value(&self) -> i64 {
        self.sum
    }

    /// Return the total number of rows accumulated.
    #[must_use]
    pub const fn samples(&self) -> u64 {
        self.samples
    }
}

impl VectorizedSink for SumSink {
    fn consume(&mut self, batch: Batch) -> Result<SinkVerdict, ExecError> {
        let cols = batch.columns();
        if cols.is_empty() {
            return Err(ExecError::TypeMismatch(
                "SumSink: batch has no columns; expected at least one Int64 column".to_owned(),
            ));
        }
        match &cols[0] {
            Column::Int64(c) => {
                for &v in c.data() {
                    self.sum = self.sum.wrapping_add(v);
                }
                self.samples += u64::try_from(c.data().len()).map_err(|_| {
                    ExecError::TypeMismatch("SumSink: column length overflows u64".to_owned())
                })?;
            }
            other => {
                return Err(ExecError::TypeMismatch(format!(
                    "SumSink: column 0 must be Int64, got {:?}",
                    other.data_type()
                )));
            }
        }
        Ok(SinkVerdict::Continue)
    }

    fn finalize(&mut self) -> Result<Option<Batch>, ExecError> {
        Ok(None)
    }
}

// ============================================================================
// CountSink
// ============================================================================

/// A [`VectorizedSink`] that counts the total number of rows seen.
///
/// Equivalent to `COUNT(*)`. Does not inspect column values; it accumulates
/// only the row count of each batch.
///
/// After driving the pipeline, call [`count`](CountSink::count) to retrieve
/// the total row count.
///
/// # When to use
///
/// Use `CountSink` when only the row count matters (e.g. verifying that a
/// filter passes the correct number of rows without materialising the output).
#[derive(Debug, Default)]
pub struct CountSink {
    /// Running total of rows seen across all consumed batches.
    count: u64,
}

impl CountSink {
    /// Create a new `CountSink` with zero initial count.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Return the total number of rows counted.
    ///
    /// Valid after the pipeline's [`finalize`](VectorizedSink::finalize) has
    /// been called.
    #[must_use]
    pub const fn count(&self) -> u64 {
        self.count
    }
}

impl VectorizedSink for CountSink {
    fn consume(&mut self, batch: Batch) -> Result<SinkVerdict, ExecError> {
        self.count += u64::try_from(batch.rows()).map_err(|_| {
            ExecError::TypeMismatch("CountSink: batch row count overflows u64".to_owned())
        })?;
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
    use ultrasql_vec::Batch;
    use ultrasql_vec::column::{Column, NumericColumn};

    use super::*;

    fn i64_batch(data: &[i64]) -> Batch {
        Batch::new([Column::Int64(NumericColumn::from_data(data.to_vec()))]).expect("batch ok")
    }

    fn i32_batch(data: &[i32]) -> Batch {
        Batch::new([Column::Int32(NumericColumn::from_data(data.to_vec()))]).expect("batch ok")
    }

    // ---- SumSink ----

    #[test]
    fn sum_sink_accumulates_i64_column() {
        let mut sink = SumSink::new();
        sink.consume(i64_batch(&[1, 2, 3])).unwrap();
        sink.consume(i64_batch(&[10, 20])).unwrap();
        sink.finalize().unwrap();
        assert_eq!(sink.final_value(), 36);
        assert_eq!(sink.samples(), 5);
    }

    #[test]
    fn sum_sink_empty_input_is_zero() {
        let mut sink = SumSink::new();
        sink.finalize().unwrap();
        assert_eq!(sink.final_value(), 0);
        assert_eq!(sink.samples(), 0);
    }

    #[test]
    fn sum_sink_rejects_non_i64_column() {
        let mut sink = SumSink::new();
        let err = sink.consume(i32_batch(&[1, 2])).unwrap_err();
        assert!(matches!(err, crate::ExecError::TypeMismatch(_)));
    }

    #[test]
    fn sum_sink_rejects_empty_batch_columns() {
        let mut sink = SumSink::new();
        let empty_batch = Batch::new(Vec::<Column>::new()).expect("empty batch ok");
        let err = sink.consume(empty_batch).unwrap_err();
        assert!(matches!(err, crate::ExecError::TypeMismatch(_)));
    }

    #[test]
    fn sum_sink_always_continues() {
        let mut sink = SumSink::new();
        let verdict = sink.consume(i64_batch(&[1, 2, 3])).unwrap();
        assert_eq!(verdict, SinkVerdict::Continue);
    }

    // ---- CountSink ----

    #[test]
    fn count_sink_counts_all_rows() {
        let mut sink = CountSink::new();
        sink.consume(i64_batch(&[1, 2, 3])).unwrap();
        sink.consume(i32_batch(&[4, 5])).unwrap();
        sink.finalize().unwrap();
        assert_eq!(sink.count(), 5);
    }

    #[test]
    fn count_sink_empty_input_is_zero() {
        let mut sink = CountSink::new();
        sink.finalize().unwrap();
        assert_eq!(sink.count(), 0);
    }

    #[test]
    fn count_sink_always_continues() {
        let mut sink = CountSink::new();
        let verdict = sink.consume(i64_batch(&[1])).unwrap();
        assert_eq!(verdict, SinkVerdict::Continue);
    }

    #[test]
    fn count_sink_accepts_any_column_type() {
        let mut sink = CountSink::new();
        // CountSink does not inspect column types.
        sink.consume(i32_batch(&[1, 2, 3])).unwrap();
        sink.consume(i64_batch(&[4, 5])).unwrap();
        assert_eq!(sink.count(), 5);
    }
}
