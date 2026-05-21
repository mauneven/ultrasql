//! Column projection operator.
//!
//! Picks a subset of the child operator's columns (in caller-specified
//! order) and re-emits each batch with that narrower shape. The child's
//! row count is preserved one-for-one.

use ultrasql_core::Schema;
use ultrasql_vec::Batch;

use crate::{ExecError, Operator};

/// Pull-mode projection over a child [`Operator`].
///
/// The projection is column-index based to match the planner's lowered
/// representation. Names appear only on the resulting [`Schema`]; the
/// runtime evaluation is index-driven and allocates a single fresh
/// `Vec` of column clones per batch. Column buffers are clone-shared
/// when the underlying types are `Arc`-backed; this layer does not
/// memcpy column data.
#[derive(Debug)]
pub struct Project {
    child: Box<dyn Operator>,
    schema: Schema,
    indices: Vec<usize>,
}

impl Project {
    /// Construct a projection.
    ///
    /// `indices` is the ordered list of child column positions to keep.
    /// Returns an error if any index falls outside the child schema —
    /// the error surfaces from [`Schema::project`].
    pub fn new(child: Box<dyn Operator>, indices: Vec<usize>) -> Result<Self, ExecError> {
        let schema = child.schema().project(&indices)?;
        Self::with_schema(child, indices, schema)
    }

    /// Construct a projection with caller-supplied output schema.
    ///
    /// Use this when a lowerer reorders physical columns but must preserve a
    /// planner-owned schema (for example, swapped hash-join lowering).
    pub fn with_schema(
        child: Box<dyn Operator>,
        indices: Vec<usize>,
        schema: Schema,
    ) -> Result<Self, ExecError> {
        let child_width = child.schema().len();
        if schema.len() != indices.len() {
            return Err(ExecError::TypeMismatch(format!(
                "projection schema width {} does not match {} indices",
                schema.len(),
                indices.len()
            )));
        }
        if indices.iter().any(|idx| *idx >= child_width) {
            return Err(ExecError::Internal("projection index out of bounds"));
        }
        Ok(Self {
            child,
            schema,
            indices,
        })
    }
}

impl Operator for Project {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        let Some(input) = self.child.next_batch()? else {
            return Ok(None);
        };
        let cols = input.columns();
        let mut projected = Vec::with_capacity(self.indices.len());
        for &i in &self.indices {
            let col = cols
                .get(i)
                .ok_or(ExecError::Internal("projection index out of bounds"))?;
            projected.push(col.clone());
        }
        Ok(Some(Batch::new(projected)?))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Projection preserves the child's row cardinality one-for-one
    /// (it only narrows the column set), so forward the child's hint
    /// unchanged. Lets `run_select_streamed` size the wire buffer
    /// exactly when the underlying scan replay knows its row count.
    fn estimated_row_count(&self) -> Option<usize> {
        self.child.estimated_row_count()
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
        Schema::new([
            Field::required("a", DataType::Int32),
            Field::required("b", DataType::Int64),
            Field::required("c", DataType::Int32),
        ])
        .expect("schema is well-formed")
    }

    fn batch3() -> Batch {
        Batch::new([
            Column::Int32(NumericColumn::from_data(vec![1_i32, 2, 3])),
            Column::Int64(NumericColumn::from_data(vec![10_i64, 20, 30])),
            Column::Int32(NumericColumn::from_data(vec![100_i32, 200, 300])),
        ])
        .expect("batch is well-formed")
    }

    #[test]
    fn project_emits_subset_of_columns_in_order() {
        let scan = MemTableScan::new(schema(), vec![batch3()]);
        let mut proj = Project::new(Box::new(scan), vec![2, 0]).unwrap();
        let out = proj.next_batch().unwrap().unwrap();
        assert_eq!(out.width(), 2);
        match (&out.columns()[0], &out.columns()[1]) {
            (Column::Int32(left), Column::Int32(right)) => {
                assert_eq!(left.data(), &[100, 200, 300]);
                assert_eq!(right.data(), &[1, 2, 3]);
            }
            other => panic!("unexpected column variants: {other:?}"),
        }
        assert!(proj.next_batch().unwrap().is_none());
    }

    #[test]
    fn project_reports_projected_schema() {
        let scan = MemTableScan::new(schema(), vec![]);
        let proj = Project::new(Box::new(scan), vec![1, 0]).unwrap();
        assert_eq!(proj.schema().len(), 2);
        assert_eq!(proj.schema().field_at(0).name, "b");
        assert_eq!(proj.schema().field_at(1).name, "a");
    }

    #[test]
    fn project_rejects_out_of_bounds_index() {
        let scan = MemTableScan::new(schema(), vec![]);
        let err = Project::new(Box::new(scan), vec![0, 99]).unwrap_err();
        assert!(matches!(err, ExecError::Core(_)));
    }
}
