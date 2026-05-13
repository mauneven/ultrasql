//! Constant-projection result operator.
//!
//! [`ResultOp`] emits exactly one batch containing the single row produced
//! by evaluating a list of constant scalar expressions. This is the physical
//! operator for queries of the form `SELECT 1` or `SELECT pg_version()`
//! where there is no `FROM` clause.
//!
//! The operator emits that one-row batch on the first [`Operator::next_batch`]
//! call and `Ok(None)` on all subsequent calls.

use ultrasql_core::{Schema, Value};
use ultrasql_planner::ScalarExpr;
use ultrasql_vec::Batch;

use crate::eval::Eval;
use crate::seq_scan::build_batch;
use crate::{ExecError, Operator};

/// Single-row constant projection operator.
///
/// Evaluates `exprs` against an empty row (all expressions must be
/// constants or parameter references) and emits the result as a
/// one-row [`Batch`] on the first call to [`Operator::next_batch`].
///
/// Subsequent calls return `Ok(None)`.
///
/// # Send
///
/// `ResultOp` is `Send` because `Vec<Eval>` and `Schema` are both `Send`.
#[derive(Debug)]
pub struct ResultOp {
    exprs: Vec<Eval>,
    schema: Schema,
    emitted: bool,
}

impl ResultOp {
    /// Construct a result operator.
    ///
    /// - `exprs` — constant scalar expressions evaluated against an empty row.
    /// - `schema` — output schema, must have the same width as `exprs`.
    #[must_use]
    pub fn new(exprs: Vec<ScalarExpr>, schema: Schema) -> Self {
        let evals = exprs.into_iter().map(Eval::new).collect();
        Self {
            exprs: evals,
            schema,
            emitted: false,
        }
    }
}

impl Operator for ResultOp {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;

        let row: Vec<Value> = self
            .exprs
            .iter()
            .map(|ev| ev.eval(&[]).unwrap_or(Value::Null))
            .collect();

        let batch = build_batch(&[row], &self.schema)?;
        Ok(Some(batch))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

#[cfg(test)]
mod tests {
    use ultrasql_core::{DataType, Field, Schema, Value};
    use ultrasql_planner::ScalarExpr;

    use super::ResultOp;
    use crate::Operator;

    fn lit_i32(v: i32) -> ScalarExpr {
        ScalarExpr::Literal {
            value: Value::Int32(v),
            data_type: DataType::Int32,
        }
    }

    fn lit_text(s: &str) -> ScalarExpr {
        ScalarExpr::Literal {
            value: Value::Text(s.into()),
            data_type: DataType::Text { max_len: None },
        }
    }

    fn schema_one_i32() -> Schema {
        Schema::new([Field::required("val", DataType::Int32)]).expect("schema ok")
    }

    #[test]
    fn result_emits_single_row() {
        let mut op = ResultOp::new(vec![lit_i32(42)], schema_one_i32());
        let batch = op.next_batch().expect("no error").expect("one batch");
        assert_eq!(batch.rows(), 1);
        assert!(
            op.next_batch().expect("no error").is_none(),
            "EOF after first batch"
        );
    }

    #[test]
    fn result_evaluates_constants() {
        let schema = Schema::new([
            Field::required("n", DataType::Int32),
            Field::required("s", DataType::Text { max_len: None }),
        ])
        .expect("schema ok");
        let mut op = ResultOp::new(vec![lit_i32(7), lit_text("hello")], schema);
        let batch = op.next_batch().expect("no error").expect("batch");
        assert_eq!(batch.rows(), 1);
    }

    #[test]
    fn result_returns_none_after_eof() {
        let mut op = ResultOp::new(vec![lit_i32(1)], schema_one_i32());
        op.next_batch().expect("no error");
        assert!(op.next_batch().expect("no error").is_none());
        assert!(op.next_batch().expect("no error").is_none());
    }
}
