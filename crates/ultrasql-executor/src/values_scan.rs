//! Values scan operator.
//!
//! [`ValuesScan`] materialises a `VALUES (...)` clause by evaluating
//! each cell through the [`Eval`] interpreter and emitting the results
//! as executor-sized [`Batch`] chunks.
//!
//! This is the leaf operator for `LogicalPlan::Values` lowering. It
//! evaluates with an empty row slice and no bound parameters; all cells
//! must be literal-only expressions (column references, sub-queries, or
//! parameters are not expected at this level for v0.5).
//!
use ultrasql_core::{Schema, Value, constants::DEFAULT_BATCH_SIZE};
use ultrasql_planner::ScalarExpr;
use ultrasql_vec::Batch;

use crate::eval::Eval;
use crate::seq_scan::build_batch;
use crate::{ExecError, Operator, eval_error_to_exec_error};

/// Leaf operator materialising a list of scalar-expression rows.
///
/// Each inner `Vec<ScalarExpr>` is one row; all inner vecs must have the
/// same length, matching `schema.len()`. The operator emits chunks capped
/// at [`DEFAULT_BATCH_SIZE`] rows and returns `Ok(None)` after all rows
/// have been materialised.
#[derive(Debug)]
pub struct ValuesScan {
    rows: Vec<Vec<ScalarExpr>>,
    schema: Schema,
    /// Start offset for the next non-empty output batch.
    next_row: usize,
    /// Set after the single batch has been emitted for a source with no
    /// output columns — either a zero-row batch (`rows` empty) or the
    /// explicit row markers of `INSERT ... DEFAULT VALUES`.
    emitted_empty: bool,
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
            next_row: 0,
            emitted_empty: false,
        }
    }
}

impl Operator for ValuesScan {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        // Zero-column source (`INSERT ... DEFAULT VALUES` binds to a
        // column-less `VALUES` whose single row carries no cells). The row
        // count can't ride on any column, so emit explicit row markers:
        // `self.rows` holds one empty cell-vector per logical row, and the
        // consuming `ModifyTable` fills every column from its DEFAULT /
        // sequence / identity / generated value (or NULL → NOT NULL).
        if self.schema.is_empty() {
            if self.emitted_empty {
                return Ok(None);
            }
            self.emitted_empty = true;
            return Ok(Some(Batch::row_markers(self.rows.len())));
        }

        if self.rows.is_empty() {
            if self.emitted_empty {
                return Ok(None);
            }
            self.emitted_empty = true;
            // Zero rows: emit an empty batch with the declared schema.
            return build_batch(&[], &self.schema).map(Some);
        }

        if self.next_row >= self.rows.len() {
            return Ok(None);
        }

        let start = self.next_row;
        let end = start
            .saturating_add(DEFAULT_BATCH_SIZE)
            .min(self.rows.len());
        self.next_row = end;

        // Evaluate each cell. We use an empty row slice because VALUES
        // expressions should not contain column references at this level.
        let empty_row: &[Value] = &[];
        let mut decoded: Vec<Vec<Value>> = Vec::with_capacity(end - start);
        for (relative_idx, expr_row) in self.rows[start..end].iter().enumerate() {
            let row_idx = start + relative_idx;
            let mut value_row: Vec<Value> = Vec::with_capacity(expr_row.len());
            for (col_idx, expr) in expr_row.iter().enumerate() {
                let evaluator = Eval::new(expr.clone());
                let val =
                    evaluator.eval(empty_row).map_err(|error| {
                        match eval_error_to_exec_error(error) {
                            ExecError::TypeMismatch(detail) => ExecError::TypeMismatch(format!(
                                "values scan: row {row_idx} col {col_idx}: {detail}"
                            )),
                            typed => typed,
                        }
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
    fn values_scan_chunks_large_inputs_into_executor_batches() {
        let rows: Vec<Vec<ScalarExpr>> = (0_i32..8200).map(|value| vec![lit_i32(value)]).collect();
        let schema = Schema::new([Field::nullable("id", DataType::Int32)]).expect("schema ok");
        let mut scan = ValuesScan::new(rows, schema);

        let mut sizes = Vec::new();
        let mut values = Vec::new();
        while let Some(batch) = scan.next_batch().unwrap() {
            sizes.push(batch.rows());
            match &batch.columns()[0] {
                Column::Int32(ids) => values.extend_from_slice(ids.data()),
                other => panic!("unexpected column type: {other:?}"),
            }
        }

        assert_eq!(sizes, vec![4096, 4096, 8]);
        assert_eq!(values.len(), 8200);
        assert_eq!(values.first(), Some(&0));
        assert_eq!(values.get(4096), Some(&4096));
        assert_eq!(values.last(), Some(&8199));
    }

    #[test]
    fn values_scan_schema_matches_declared() {
        let scan = ValuesScan::new(vec![], schema_id_name());
        assert_eq!(scan.schema().len(), 2);
        assert_eq!(scan.schema().field_at(0).name, "id");
    }

    #[test]
    fn values_scan_zero_column_row_reports_one_row() {
        // `INSERT ... DEFAULT VALUES` binds to a zero-column VALUES whose
        // single row carries no cells. A zero-column batch cannot derive its
        // row count from a column, so the scan must emit an explicit one-row
        // marker batch — otherwise the row vanishes and nothing is inserted.
        let mut scan = ValuesScan::new(vec![vec![]], Schema::empty());
        let batch = scan.next_batch().unwrap().unwrap();
        assert_eq!(batch.rows(), 1, "one zero-column row must survive as 1 row");
        assert_eq!(batch.width(), 0);
        assert!(scan.next_batch().unwrap().is_none());
    }

    #[test]
    fn values_scan_zero_column_zero_rows_reports_no_rows() {
        // A column-less scan with no rows still emits exactly zero rows.
        let mut scan = ValuesScan::new(vec![], Schema::empty());
        let batch = scan.next_batch().unwrap().unwrap();
        assert_eq!(batch.rows(), 0);
        assert_eq!(batch.width(), 0);
        assert!(scan.next_batch().unwrap().is_none());
    }
}
