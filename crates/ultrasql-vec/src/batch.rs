//! Record batch: an ordered set of columns with a uniform row count.
//!
//! A `Batch` is the input and output unit of every vectorized
//! operator. Operators receive a batch, transform it (filter rows,
//! evaluate expressions, hash-aggregate values, etc.), and emit a
//! new batch with the same or smaller row count.

use smallvec::SmallVec;

use crate::column::Column;

/// Errors specific to batch construction.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum BatchError {
    /// Two columns in the batch report different row counts.
    #[error("batch column lengths disagree: column {index} has {actual} rows, expected {expected}")]
    LengthMismatch {
        /// Index of the disagreeing column.
        index: usize,
        /// Its length.
        actual: usize,
        /// The expected length.
        expected: usize,
    },
    /// Batch is empty but at least one column was supplied. (Empty
    /// batches must have zero columns.)
    #[error("empty batch must have zero columns; got {0}")]
    NonEmptyEmptyBatch(usize),
}

/// A typed columnar batch.
#[derive(Clone, Debug)]
pub struct Batch {
    columns: SmallVec<[Column; 8]>,
    rows: usize,
}

impl Batch {
    /// Construct a batch. Validates that every column reports the
    /// same row count.
    pub fn new<I: IntoIterator<Item = Column>>(columns: I) -> Result<Self, BatchError> {
        let columns: SmallVec<[Column; 8]> = columns.into_iter().collect();
        let rows = columns.first().map_or(0, Column::len);
        for (i, col) in columns.iter().enumerate() {
            if col.len() != rows {
                return Err(BatchError::LengthMismatch {
                    index: i,
                    actual: col.len(),
                    expected: rows,
                });
            }
        }
        Ok(Self { columns, rows })
    }

    /// Borrow the columns slice.
    #[must_use]
    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    /// Row count.
    #[must_use]
    pub const fn rows(&self) -> usize {
        self.rows
    }

    /// Number of columns.
    #[must_use]
    pub fn width(&self) -> usize {
        self.columns.len()
    }

    /// Whether the batch has zero rows.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.rows == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column::NumericColumn;

    #[test]
    fn new_batch_validates_row_counts() {
        let a = Column::Int32(NumericColumn::from_data(vec![1, 2, 3]));
        let b = Column::Int64(NumericColumn::from_data(vec![10_i64, 20, 30]));
        let batch = Batch::new(vec![a, b]).unwrap();
        assert_eq!(batch.rows(), 3);
        assert_eq!(batch.width(), 2);
    }

    #[test]
    fn mismatched_columns_rejected() {
        let a = Column::Int32(NumericColumn::from_data(vec![1, 2, 3]));
        let b = Column::Int64(NumericColumn::from_data(vec![10_i64, 20]));
        let err = Batch::new(vec![a, b]).unwrap_err();
        assert!(matches!(err, BatchError::LengthMismatch { .. }));
    }

    #[test]
    fn empty_batch_allowed() {
        let batch = Batch::new(Vec::<Column>::new()).unwrap();
        assert_eq!(batch.rows(), 0);
        assert_eq!(batch.width(), 0);
        assert!(batch.is_empty());
    }
}
