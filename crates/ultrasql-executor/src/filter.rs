//! Filter operators.
//!
//! [`FilterEqI32`] is a *placeholder* filter: a single i32 column
//! compared to a single i32 constant. It exists so end-to-end pipeline
//! tests have a working predicate before the general expression-eval
//! module lands. Once that module exists, this operator collapses to a
//! special case of `FilterExpr(EqI32(col_ref, const))` and we delete it.

use ultrasql_core::Schema;
use ultrasql_vec::Batch;
use ultrasql_vec::column::{Column, NumericColumn};
use ultrasql_vec::kernels::{eq_i32, select_i32};

use crate::{ExecError, Operator};

/// Filter on `column[col_idx] == const_i32` over an `i32` column.
///
/// This is the simplest possible predicate, exposed only as a stand-in
/// for the eventual expression-tree evaluator. It is not a public
/// long-term API surface; see the module-level documentation.
///
/// Behavior:
///
/// - The output schema is identical to the child's schema (filtering
///   never changes the row shape).
/// - Each input batch produces exactly one output batch (possibly
///   empty); the operator does not coalesce runs of empty batches.
/// - The named column must be of type [`ultrasql_core::DataType::Int32`].
///   A runtime mismatch raises [`ExecError::TypeMismatch`].
#[derive(Debug)]
pub struct FilterEqI32 {
    child: Box<dyn Operator>,
    schema: Schema,
    col_idx: usize,
    constant: i32,
}

impl FilterEqI32 {
    /// Construct the filter.
    ///
    /// Validates at construction time that `col_idx` is in bounds and
    /// that the named column is `Int32`. Production filters resolved
    /// from a logical plan have the same invariant checked during
    /// binding; we duplicate it here so hand-written tests fail fast.
    pub fn new(child: Box<dyn Operator>, col_idx: usize, constant: i32) -> Result<Self, ExecError> {
        let schema = child.schema().clone();
        let field = schema.field(col_idx).ok_or_else(|| {
            ExecError::TypeMismatch(format!(
                "filter column index {col_idx} out of bounds for schema width {}",
                schema.len()
            ))
        })?;
        if field.data_type != ultrasql_core::DataType::Int32 {
            return Err(ExecError::TypeMismatch(format!(
                "FilterEqI32 expects Int32 column at index {col_idx}, found {}",
                field.data_type
            )));
        }
        Ok(Self {
            child,
            schema,
            col_idx,
            constant,
        })
    }

    fn apply(&self, input: &Batch) -> Result<Batch, ExecError> {
        let cols = input.columns();
        let key_col = match cols.get(self.col_idx) {
            Some(Column::Int32(c)) => c,
            Some(other) => {
                return Err(ExecError::TypeMismatch(format!(
                    "FilterEqI32 expected Int32 column at runtime, got {:?}",
                    other.data_type()
                )));
            }
            None => return Err(ExecError::Internal("filter column index out of bounds")),
        };
        // Build a constant column of equal length and reuse the
        // existing kernel. A future specialization will scan against
        // the broadcast constant directly without materializing it.
        let constants = NumericColumn::from_data(vec![self.constant; key_col.len()]);
        let mask = eq_i32(key_col, &constants);

        let mut out_cols = Vec::with_capacity(cols.len());
        for (i, col) in cols.iter().enumerate() {
            match col {
                Column::Int32(c) => out_cols.push(Column::Int32(select_i32(c, &mask))),
                Column::Int64(c) => out_cols.push(Column::Int64(select_i64(c, &mask))),
                _ => {
                    return Err(ExecError::TypeMismatch(format!(
                        "FilterEqI32 does not yet support column type {:?} at index {i}",
                        col.data_type()
                    )));
                }
            }
        }
        Batch::new(out_cols).map_err(Into::into)
    }
}

impl Operator for FilterEqI32 {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        let Some(input) = self.child.next_batch()? else {
            return Ok(None);
        };
        Ok(Some(self.apply(&input)?))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

/// Local `select_i64` analogue. The `ultrasql-vec` crate exports
/// `select_i32`; an `i64` variant is pending. Implemented here in the
/// executor to avoid modifying the kernels crate from this scope; the
/// signature mirrors `select_i32` exactly so the two paths fuse once
/// the kernel arrives.
fn select_i64(column: &NumericColumn<i64>, selection: &ultrasql_vec::Bitmap) -> NumericColumn<i64> {
    let take = selection.count_ones();
    let mut out = Vec::with_capacity(take);
    for i in selection.iter_ones() {
        out.push(column.data()[i]);
    }
    NumericColumn::from_data(out)
}

#[cfg(test)]
mod tests {
    use ultrasql_core::{DataType, Field, Schema};
    use ultrasql_vec::column::{Column, NumericColumn};

    use super::*;
    use crate::MemTableScan;

    fn schema() -> Schema {
        Schema::new([
            Field::required("id", DataType::Int32),
            Field::required("val", DataType::Int64),
        ])
        .expect("schema is well-formed")
    }

    fn pair_batch(rows: &[(i32, i64)]) -> Batch {
        let ids: Vec<i32> = rows.iter().map(|(a, _)| *a).collect();
        let vals: Vec<i64> = rows.iter().map(|(_, b)| *b).collect();
        Batch::new([
            Column::Int32(NumericColumn::from_data(ids)),
            Column::Int64(NumericColumn::from_data(vals)),
        ])
        .expect("batch is well-formed")
    }

    #[test]
    fn filter_keeps_matching_rows() {
        let scan = MemTableScan::new(
            schema(),
            vec![pair_batch(&[(7, 1), (1, 2), (7, 3), (2, 4)])],
        );
        let mut filter = FilterEqI32::new(Box::new(scan), 0, 7).unwrap();
        let out = filter.next_batch().unwrap().unwrap();
        assert_eq!(out.rows(), 2);
        match (&out.columns()[0], &out.columns()[1]) {
            (Column::Int32(ids), Column::Int64(vals)) => {
                assert_eq!(ids.data(), &[7, 7]);
                assert_eq!(vals.data(), &[1, 3]);
            }
            other => panic!("unexpected types: {other:?}"),
        }
        assert!(filter.next_batch().unwrap().is_none());
    }

    #[test]
    fn filter_emits_empty_batch_when_nothing_matches() {
        let scan = MemTableScan::new(schema(), vec![pair_batch(&[(1, 10), (2, 20)])]);
        let mut filter = FilterEqI32::new(Box::new(scan), 0, 7).unwrap();
        let out = filter.next_batch().unwrap().unwrap();
        // The filter does not swallow empty batches; downstream
        // operators must tolerate them.
        assert_eq!(out.rows(), 0);
        assert_eq!(out.width(), 2);
        assert!(filter.next_batch().unwrap().is_none());
    }

    #[test]
    fn filter_rejects_wrong_column_type() {
        let scan = MemTableScan::new(schema(), vec![]);
        let err = FilterEqI32::new(Box::new(scan), 1, 0).unwrap_err();
        assert!(matches!(err, ExecError::TypeMismatch(_)));
    }

    #[test]
    fn filter_rejects_out_of_bounds_index() {
        let scan = MemTableScan::new(schema(), vec![]);
        let err = FilterEqI32::new(Box::new(scan), 99, 0).unwrap_err();
        assert!(matches!(err, ExecError::TypeMismatch(_)));
    }
}
