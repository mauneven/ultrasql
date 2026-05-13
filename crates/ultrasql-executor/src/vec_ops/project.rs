//! Vectorized projection operator.
//!
//! [`VectorizedProject`] selects a subset of columns from each incoming
//! batch by index.  It does not evaluate expressions — column renaming
//! and constant columns are handled by the planner before this stage.

use ultrasql_core::Schema;
use ultrasql_vec::Batch;
use ultrasql_vec::column::Column;

use crate::ExecError;
use crate::push_pipeline::{SinkVerdict, VectorizedOperator, VectorizedSink};

/// Vectorized column projection operator.
///
/// Applies a fixed list of column indices to each batch, emitting only the
/// selected columns in the specified order.  The output schema is the
/// projection of the input schema onto the same indices.
///
/// The operator is `Send` because all fields are `Send`.
#[derive(Debug)]
pub struct VectorizedProject {
    child: Box<dyn VectorizedOperator>,
    indices: Vec<usize>,
    schema: Schema,
}

impl VectorizedProject {
    /// Construct a vectorized projection.
    ///
    /// `indices` is the ordered list of column indices to project from the
    /// child's output schema.
    ///
    /// # Errors
    ///
    /// Returns [`ExecError::TypeMismatch`] if any index is out of range.
    pub fn new(child: Box<dyn VectorizedOperator>, indices: Vec<usize>) -> Result<Self, ExecError> {
        let child_schema = child.schema();
        for &idx in &indices {
            if idx >= child_schema.len() {
                return Err(ExecError::TypeMismatch(format!(
                    "VectorizedProject: column index {idx} out of range (schema width {})",
                    child_schema.len()
                )));
            }
        }
        let schema = child_schema.project(&indices).map_err(ExecError::Core)?;
        Ok(Self {
            child,
            indices,
            schema,
        })
    }
}

impl VectorizedOperator for VectorizedProject {
    fn drive(&mut self, sink: &mut dyn VectorizedSink) -> Result<(), ExecError> {
        let indices = self.indices.clone();
        let schema = self.schema.clone();
        let child = &mut self.child;

        let mut project_sink = ProjectSink {
            inner: sink,
            indices,
            schema,
        };
        child.drive(&mut project_sink)
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

// ---- Internal sink ----

struct ProjectSink<'a> {
    inner: &'a mut dyn VectorizedSink,
    indices: Vec<usize>,
    schema: Schema,
}

impl VectorizedSink for ProjectSink<'_> {
    fn consume(&mut self, batch: Batch) -> Result<SinkVerdict, ExecError> {
        let cols = batch.columns();
        let mut out: Vec<Column> = Vec::with_capacity(self.indices.len());
        for &idx in &self.indices {
            out.push(cols[idx].clone());
        }
        let projected = Batch::new(out).map_err(ExecError::from)?;
        let _ = &self.schema;
        self.inner.consume(projected)
    }

    fn finalize(&mut self) -> Result<Option<Batch>, ExecError> {
        self.inner.finalize()
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
    use crate::vec_ops::scan::VectorizedSeqScan;

    fn schema_id_val() -> Schema {
        Schema::new([
            Field::required("id", DataType::Int32),
            Field::required("val", DataType::Int64),
        ])
        .expect("schema ok")
    }

    fn batch_pair(rows: &[(i32, i64)]) -> Batch {
        let ids: Vec<i32> = rows.iter().map(|(a, _)| *a).collect();
        let vals: Vec<i64> = rows.iter().map(|(_, b)| *b).collect();
        Batch::new([
            Column::Int32(NumericColumn::from_data(ids)),
            Column::Int64(NumericColumn::from_data(vals)),
        ])
        .unwrap()
    }

    #[test]
    fn project_selects_single_column() {
        let scan = MemTableScan::new(
            schema_id_val(),
            vec![batch_pair(&[(1, 10), (2, 20), (3, 30)])],
        );
        let child = VectorizedSeqScan::new(Box::new(scan));
        let mut proj = VectorizedProject::new(Box::new(child), vec![1]).unwrap();
        let mut sink = CollectSink::new();
        proj.drive(&mut sink).unwrap();
        let batches = sink.finish();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].width(), 1);
        match &batches[0].columns()[0] {
            Column::Int64(c) => assert_eq!(c.data(), &[10, 20, 30]),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn project_reorders_columns() {
        let scan = MemTableScan::new(schema_id_val(), vec![batch_pair(&[(5, 99)])]);
        let child = VectorizedSeqScan::new(Box::new(scan));
        let mut proj = VectorizedProject::new(Box::new(child), vec![1, 0]).unwrap();
        let mut sink = CollectSink::new();
        proj.drive(&mut sink).unwrap();
        let batches = sink.finish();
        assert_eq!(batches[0].width(), 2);
        // Column 0 should now be Int64, column 1 should be Int32
        assert!(matches!(batches[0].columns()[0], Column::Int64(_)));
        assert!(matches!(batches[0].columns()[1], Column::Int32(_)));
    }

    #[test]
    fn project_out_of_range_index_errors() {
        let scan = MemTableScan::new(schema_id_val(), vec![]);
        let child = VectorizedSeqScan::new(Box::new(scan));
        let result = VectorizedProject::new(Box::new(child), vec![5]);
        assert!(result.is_err());
    }

    #[test]
    fn project_schema_matches_indices() {
        let scan = MemTableScan::new(schema_id_val(), vec![]);
        let child = VectorizedSeqScan::new(Box::new(scan));
        let proj = VectorizedProject::new(Box::new(child), vec![1]).unwrap();
        assert_eq!(proj.schema().len(), 1);
        assert_eq!(proj.schema().field_at(0).name, "val");
    }
}
