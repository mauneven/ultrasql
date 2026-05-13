//! In-memory table scan operator.
//!
//! Owns a `Vec<Batch>` constructed up-front by tests, fixtures, and the
//! bring-up CLI. Production scans backed by the storage subsystem will
//! arrive as a separate operator that pulls pages from the buffer pool.

use ultrasql_core::Schema;
use ultrasql_vec::Batch;

use crate::{ExecError, Operator};

/// Leaf operator that emits a pre-built sequence of [`Batch`]es.
///
/// Batches are returned in insertion order. After the last batch the
/// operator returns `Ok(None)` indefinitely, in line with the
/// [`Operator`] contract.
///
/// The scan does **not** validate that every supplied batch matches the
/// declared schema. Callers are expected to hand it well-typed input —
/// the production scan operator built atop the storage layer will own
/// that validation.
#[derive(Debug)]
pub struct MemTableScan {
    schema: Schema,
    batches: std::vec::IntoIter<Batch>,
}

impl MemTableScan {
    /// Construct a memory scan.
    ///
    /// Takes ownership of the batches. The supplied `Schema` is the
    /// schema of every batch and is the schema this operator reports.
    #[must_use]
    pub fn new(schema: Schema, batches: Vec<Batch>) -> Self {
        Self {
            schema,
            batches: batches.into_iter(),
        }
    }
}

impl Operator for MemTableScan {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        Ok(self.batches.next())
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

#[cfg(test)]
mod tests {
    use ultrasql_core::{DataType, Field, Schema};
    use ultrasql_vec::column::{Column, NumericColumn};

    use super::*;

    fn schema() -> Schema {
        Schema::new([Field::required("id", DataType::Int32)]).expect("schema is well-formed")
    }

    fn int_batch(rows: &[i32]) -> Batch {
        Batch::new([Column::Int32(NumericColumn::from_data(rows.to_vec()))])
            .expect("batch is well-formed")
    }

    #[test]
    fn drains_batches_in_order() {
        let mut scan = MemTableScan::new(schema(), vec![int_batch(&[1, 2]), int_batch(&[3, 4, 5])]);
        let first = scan.next_batch().unwrap().unwrap();
        let second = scan.next_batch().unwrap().unwrap();
        assert_eq!(first.rows(), 2);
        assert_eq!(second.rows(), 3);
        assert!(scan.next_batch().unwrap().is_none());
    }

    #[test]
    fn empty_input_is_eof_immediately() {
        let mut scan = MemTableScan::new(schema(), vec![]);
        assert!(scan.next_batch().unwrap().is_none());
    }

    #[test]
    fn reports_declared_schema() {
        let scan = MemTableScan::new(schema(), vec![]);
        assert_eq!(scan.schema().len(), 1);
        assert_eq!(scan.schema().field_at(0).name, "id");
    }
}
