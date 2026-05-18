//! Values scan operator.
//!
//! [`ValuesScan`] materialises a `VALUES (...)` clause by evaluating
//! each cell through the [`Eval`] interpreter and emitting the results
//! as a single [`Batch`].
//!
//! This is the leaf operator for `LogicalPlan::Values` lowering. It
//! evaluates with an empty row slice and no bound parameters; all cells
//! must be literal-only expressions (column references, sub-queries, or
//! parameters are not expected at this level for v0.5).
//!
//! # Single-batch limitation
//!
//! The current implementation emits all rows in a single batch on the
//! first `next_batch` call, even if the row count exceeds 4096. Splitting
//! into 4096-row batches is a follow-up tracked by TODO(values-batching).

use ultrasql_core::{Schema, Value};
use ultrasql_planner::ScalarExpr;
use ultrasql_vec::Batch;

use crate::eval::Eval;
use crate::seq_scan::build_batch;
use crate::{ExecError, Operator};

/// Leaf operator materialising a list of scalar-expression rows.
///
/// Each inner `Vec<ScalarExpr>` is one row; all inner vecs must have the
/// same length, matching `schema.len()`. The operator emits a single
/// batch on the first `next_batch` call and returns `Ok(None)` on all
/// subsequent calls.
#[derive(Debug)]
pub struct ValuesScan {
    rows: Vec<Vec<ScalarExpr>>,
    schema: Schema,
    /// Set to `true` after the one batch has been emitted.
    emitted: bool,
}

impl ValuesScan {
    /// Construct a values scan.
    ///
    /// `rows` is the list of expression rows and `schema` is the output
    /// schema. The caller (the physical-plan builder) is responsible for
    /// ensuring arity matches; a mismatch will surface as an
    /// [`ExecError::TypeMismatch`] on the first `next_batch` call.
    #[must_use]
    pub const fn new(rows: Vec<Vec<ScalarExpr>>, schema: Schema) -> Self {
        Self {
            rows,
            schema,
            emitted: false,
        }
    }
}

impl Operator for ValuesScan {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;

        if self.rows.is_empty() {
            // Zero rows: emit an empty batch with the declared schema.
            return build_batch(&[], &self.schema).map(Some);
        }

        // Evaluate each cell. We use an empty row slice because VALUES
        // expressions should not contain column references at this level.
        let empty_row: &[Value] = &[];
        let mut decoded: Vec<Vec<Value>> = Vec::with_capacity(self.rows.len());
        for (row_idx, expr_row) in self.rows.iter().enumerate() {
            let mut value_row: Vec<Value> = Vec::with_capacity(expr_row.len());
            for (col_idx, expr) in expr_row.iter().enumerate() {
                let evaluator = Eval::new(expr.clone());
                let val = evaluator.eval(empty_row).map_err(|e| {
                    ExecError::TypeMismatch(format!(
                        "values scan: row {row_idx} col {col_idx}: {e}"
                    ))
                })?;
                value_row.push(val);
            }
            decoded.push(value_row);
        }

        build_batch(&decoded, &self.schema).map(Some)
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use ultrasql_core::{DataType, Field, Schema, Value};
    use ultrasql_planner::ScalarExpr;
    use ultrasql_vec::column::Column;

    use super::ValuesScan;
    use crate::Operator;

    fn lit_i32(v: i32) -> ScalarExpr {
        ScalarExpr::Literal {
            value: Value::Int32(v),
            data_type: DataType::Int32,
        }
    }

    fn lit_text(s: &str) -> ScalarExpr {
        ScalarExpr::Literal {
            value: Value::Text(s.to_owned()),
            data_type: DataType::Text { max_len: None },
        }
    }

    fn schema_id_name() -> Schema {
        Schema::new([
            Field::nullable("id", DataType::Int32),
            Field::nullable("name", DataType::Text { max_len: None }),
        ])
        .expect("schema ok")
    }

    #[test]
    fn values_scan_emits_materialised_rows() {
        let rows = vec![
            vec![lit_i32(1), lit_text("alice")],
            vec![lit_i32(2), lit_text("bob")],
        ];
        let mut scan = ValuesScan::new(rows, schema_id_name());
        let batch = scan.next_batch().unwrap().unwrap();
        assert_eq!(batch.rows(), 2);
        match (&batch.columns()[0], &batch.columns()[1]) {
            (Column::Int32(ids), names @ (Column::Utf8(_) | Column::DictionaryUtf8(_))) => {
                assert_eq!(ids.data(), &[1, 2]);
                assert_eq!(names.text_value(0), Some("alice"));
                assert_eq!(names.text_value(1), Some("bob"));
            }
            other => panic!("unexpected column types: {other:?}"),
        }
        // Second call must return None.
        assert!(scan.next_batch().unwrap().is_none());
    }

    #[test]
    fn values_scan_empty_rows_emits_empty_batch() {
        let schema = Schema::new([Field::nullable("id", DataType::Int32)]).expect("schema ok");
        let mut scan = ValuesScan::new(vec![], schema);
        let batch = scan.next_batch().unwrap().unwrap();
        assert_eq!(batch.rows(), 0);
        assert!(scan.next_batch().unwrap().is_none());
    }

    #[test]
    fn values_scan_schema_matches_declared() {
        let scan = ValuesScan::new(vec![], schema_id_name());
        assert_eq!(scan.schema().len(), 2);
        assert_eq!(scan.schema().field_at(0).name, "id");
    }
}
