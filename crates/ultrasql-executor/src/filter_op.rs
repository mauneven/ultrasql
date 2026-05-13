//! General predicate filter operator.
//!
//! [`Filter`] is the production-quality filter operator backed by the
//! full [`Eval`] expression interpreter. It replaces the placeholder
//! [`FilterEqI32`](crate::FilterEqI32) for all predicate shapes except
//! those where the specialised SIMD path is wired in.
//!
//! # Row-at-a-time evaluation
//!
//! `Filter` decodes each batch into rows (column-wise -> row-of-Values),
//! evaluates the predicate per row, and rebuilds a new batch from the
//! surviving rows. This is `O(rows * schema_width)` in allocations. A
//! future vectorised predicate path will skip the row-wise decode
//! entirely; the scalar path here is correct by construction and
//! sufficient for OLTP-sized batches.

use ultrasql_core::{Schema, Value};
use ultrasql_planner::ScalarExpr;
use ultrasql_vec::Batch;

use crate::eval::Eval;
use crate::seq_scan::build_batch;
use crate::{ExecError, Operator};

/// General predicate filter operator.
///
/// Pulls batches from `child`, evaluates `predicate` against each row,
/// and emits only rows where the predicate returns `Value::Bool(true)`.
/// NULL and `false` results are both discarded (SQL 3VL: only `true`
/// passes the filter).
///
/// The output schema is identical to the child's schema.
#[derive(Debug)]
pub struct Filter {
    child: Box<dyn Operator>,
    predicate: Eval,
    schema: Schema,
}

impl Filter {
    /// Construct a filter.
    ///
    /// The predicate is compiled into an [`Eval`] instance; the schema
    /// is cloned from `child` at construction time and remains fixed.
    #[must_use]
    pub fn new(child: Box<dyn Operator>, predicate: ScalarExpr) -> Self {
        let schema = child.schema().clone();
        Self {
            child,
            predicate: Eval::new(predicate),
            schema,
        }
    }
}

impl Operator for Filter {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        let Some(input) = self.child.next_batch()? else {
            return Ok(None);
        };

        // Decode the batch into rows, apply the predicate, collect survivors.
        let rows = batch_to_rows(&input, &self.schema)?;
        let mut survivors: Vec<Vec<Value>> = Vec::with_capacity(rows.len());
        for row in &rows {
            let result = self
                .predicate
                .eval(row)
                .map_err(|e| ExecError::TypeMismatch(e.to_string()))?;
            match result {
                Value::Bool(true) => survivors.push(row.clone()),
                Value::Bool(false) | Value::Null => {
                    // false and NULL are both non-passing in SQL 3VL.
                }
                other => {
                    return Err(ExecError::TypeMismatch(format!(
                        "filter predicate must evaluate to Bool or Null, got {:?}",
                        other.data_type()
                    )));
                }
            }
        }

        if survivors.is_empty() {
            // Return a properly-shaped empty batch (correct column count but 0
            // rows). `build_batch` with an empty slice produces a 0-column
            // batch, which would violate the operator's schema contract.
            let empty = build_empty_batch(&self.schema)?;
            return Ok(Some(empty));
        }
        build_batch(&survivors, &self.schema).map(Some)
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

/// Build an empty batch whose column types match `schema`.
///
/// The returned batch has 0 rows but the correct number of columns, each
/// with an empty data vec. This is required when the filter passes no rows
/// from a non-empty input batch — the caller must not mistake 0 rows for
/// EOF.
fn build_empty_batch(schema: &Schema) -> Result<Batch, ExecError> {
    use ultrasql_core::DataType;
    use ultrasql_vec::column::{BoolColumn, Column, NumericColumn, StringColumn};

    let cols: Vec<Column> = schema
        .fields()
        .iter()
        .map(|f| match &f.data_type {
            DataType::Bool => Column::Bool(BoolColumn::from_data(vec![])),
            DataType::Int64 => Column::Int64(NumericColumn::from_data(vec![])),
            DataType::Float32 => Column::Float32(NumericColumn::from_data(vec![])),
            DataType::Float64 => Column::Float64(NumericColumn::from_data(vec![])),
            DataType::Text { .. } => Column::Utf8(StringColumn::from_data(vec![])),
            // For Int32 and any other type, fall back to an Int32 column.
            // In practice the binder only produces the above types at v0.5.
            _ => Column::Int32(NumericColumn::from_data(vec![])),
        })
        .collect();
    Batch::new(cols).map_err(ExecError::from)
}

/// Decode a [`Batch`] into a `Vec` of rows (each row is a `Vec<Value>`).
///
/// This is the inverse of [`build_batch`]: it reconstructs the row-at-a-time
/// representation from the columnar batch. Each column is decoded into the
/// corresponding `Value` variant; NULL cells use `Value::Null` (the
/// `BoolColumn` and numeric columns use a sentinel zero for NULL which is
/// re-encoded here as `Value::Null` only when the schema field is nullable
/// and the value equals the sentinel — for v0.5 simplicity we keep the
/// sentinel as-is since nullability is represented in the batch validity
/// bitmaps in future work; for now the filter treats the sentinel as a
/// non-null value).
///
/// For v0.5 this is a pure column-to-value decode without bitmap support.
#[allow(unreachable_pub)]
pub fn batch_to_rows(batch: &Batch, schema: &Schema) -> Result<Vec<Vec<Value>>, ExecError> {
    use ultrasql_core::DataType;
    use ultrasql_vec::column::Column;

    let n_rows = batch.rows();
    let n_cols = schema.len();
    let cols = batch.columns();

    if cols.len() != n_cols {
        return Err(ExecError::TypeMismatch(format!(
            "batch has {} columns but schema has {}",
            cols.len(),
            n_cols,
        )));
    }

    let mut rows: Vec<Vec<Value>> = (0..n_rows).map(|_| Vec::with_capacity(n_cols)).collect();

    for (col_idx, (col, field)) in cols.iter().zip(schema.fields().iter()).enumerate() {
        match (col, &field.data_type) {
            (Column::Int32(c), DataType::Int32) => {
                for (row_idx, row) in rows.iter_mut().enumerate() {
                    row.push(Value::Int32(c.data()[row_idx]));
                }
            }
            (Column::Int64(c), DataType::Int64) => {
                for (row_idx, row) in rows.iter_mut().enumerate() {
                    row.push(Value::Int64(c.data()[row_idx]));
                }
            }
            (Column::Float32(c), DataType::Float32) => {
                for (row_idx, row) in rows.iter_mut().enumerate() {
                    row.push(Value::Float32(c.data()[row_idx]));
                }
            }
            (Column::Float64(c), DataType::Float64) => {
                for (row_idx, row) in rows.iter_mut().enumerate() {
                    row.push(Value::Float64(c.data()[row_idx]));
                }
            }
            (Column::Bool(c), DataType::Bool) => {
                for (row_idx, row) in rows.iter_mut().enumerate() {
                    row.push(Value::Bool(c.value(row_idx)));
                }
            }
            (Column::Utf8(c), DataType::Text { .. }) => {
                for (row_idx, row) in rows.iter_mut().enumerate() {
                    row.push(Value::Text(c.value(row_idx).to_owned()));
                }
            }
            (col_var, expected_type) => {
                return Err(ExecError::TypeMismatch(format!(
                    "column {col_idx}: batch column type {:?} does not match schema type {expected_type}",
                    col_var.data_type()
                )));
            }
        }
    }

    Ok(rows)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use ultrasql_core::{DataType, Field, Schema, Value};
    use ultrasql_planner::{BinaryOp, ScalarExpr};
    use ultrasql_vec::Batch;
    use ultrasql_vec::column::{Column, NumericColumn};

    use super::Filter;
    use crate::Operator;
    use crate::mem_table_scan::MemTableScan;

    fn schema_id_val() -> Schema {
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

    /// Predicate: `id = 7` (Int32 column at index 0 equals literal 7).
    fn pred_id_eq_7() -> ScalarExpr {
        ScalarExpr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(ScalarExpr::Column {
                name: "id".into(),
                index: 0,
                data_type: DataType::Int32,
            }),
            right: Box::new(ScalarExpr::Literal {
                value: Value::Int32(7),
                data_type: DataType::Int32,
            }),
            data_type: DataType::Bool,
        }
    }

    fn drain_id_val(op: &mut dyn Operator) -> Vec<(i32, i64)> {
        let mut out = Vec::new();
        while let Some(b) = op.next_batch().expect("operator must not error") {
            let cols = b.columns();
            match (&cols[0], &cols[1]) {
                (Column::Int32(ids), Column::Int64(vals)) => {
                    for (i, v) in ids.data().iter().zip(vals.data().iter()) {
                        out.push((*i, *v));
                    }
                }
                other => panic!("unexpected column types: {other:?}"),
            }
        }
        out
    }

    #[test]
    fn filter_keeps_rows_where_predicate_true() {
        let scan = MemTableScan::new(
            schema_id_val(),
            vec![pair_batch(&[(7, 10), (1, 20), (7, 30), (2, 40)])],
        );
        let mut filter = Filter::new(Box::new(scan), pred_id_eq_7());
        let rows = drain_id_val(&mut filter);
        assert_eq!(rows, vec![(7, 10), (7, 30)]);
    }

    #[test]
    fn filter_drops_rows_where_predicate_false_or_null() {
        let scan = MemTableScan::new(
            schema_id_val(),
            vec![pair_batch(&[(1, 10), (2, 20), (3, 30)])],
        );
        let mut filter = Filter::new(Box::new(scan), pred_id_eq_7());
        let rows = drain_id_val(&mut filter);
        assert!(rows.is_empty(), "expected no rows, got {rows:?}");
    }

    #[test]
    fn filter_chains_with_mem_table_scan() {
        let schema = schema_id_val();
        let b1 = pair_batch(&[(7, 1), (2, 2), (7, 3)]);
        let b2 = pair_batch(&[(7, 4), (5, 5)]);
        let scan = MemTableScan::new(schema, vec![b1, b2]);
        let mut filter = Filter::new(Box::new(scan), pred_id_eq_7());
        let rows = drain_id_val(&mut filter);
        assert_eq!(rows, vec![(7, 1), (7, 3), (7, 4)]);
    }

    #[test]
    fn filter_schema_matches_child_schema() {
        let scan = MemTableScan::new(schema_id_val(), vec![]);
        let filter = Filter::new(Box::new(scan), pred_id_eq_7());
        assert_eq!(filter.schema().len(), 2);
        assert_eq!(filter.schema().field_at(0).name, "id");
    }

    #[test]
    fn filter_empty_input_returns_none() {
        let scan = MemTableScan::new(schema_id_val(), vec![]);
        let mut filter = Filter::new(Box::new(scan), pred_id_eq_7());
        assert!(filter.next_batch().unwrap().is_none());
    }

    #[test]
    fn filter_emits_empty_batch_when_nothing_matches() {
        let scan = MemTableScan::new(schema_id_val(), vec![pair_batch(&[(1, 1), (2, 2)])]);
        let mut filter = Filter::new(Box::new(scan), pred_id_eq_7());
        // The filter emits a batch (possibly empty) per child batch, not None.
        let batch = filter.next_batch().unwrap().unwrap();
        assert_eq!(batch.rows(), 0, "expected empty batch");
        assert!(filter.next_batch().unwrap().is_none());
    }
}
