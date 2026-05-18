//! Expression-projection operator.
//!
//! Evaluates a list of scalar expressions per child row and emits a
//! batch whose columns are the per-row evaluated values. Unlike the
//! plain [`crate::Project`] operator (which is index-based column
//! routing), this variant handles arbitrary `ScalarExpr` shapes that
//! the binder lowers from a SELECT list — built-in function calls,
//! CASE / COALESCE, arithmetic, etc.

use ultrasql_core::{DataType, Schema, Value};
use ultrasql_planner::ScalarExpr;
use ultrasql_vec::bitmap::Bitmap;
use ultrasql_vec::column::{BoolColumn, Column, NumericColumn};
use ultrasql_vec::{Batch, DictionaryEncodingPolicy, StringEncoding, encode_strings_auto};

use crate::eval::eval_expr;
use crate::filter_op::batch_to_rows;
use crate::{ExecError, Operator};

/// Pull-mode expression projection.
#[derive(Debug)]
pub struct ProjectExprs {
    child: Box<dyn Operator>,
    schema: Schema,
    exprs: Vec<ScalarExpr>,
    output_types: Vec<DataType>,
    child_schema: Schema,
    done: bool,
}

impl ProjectExprs {
    /// Build a projection operator from the bound `(expr, name)` list
    /// and the resulting output schema (already computed by the
    /// binder).
    pub fn new(
        child: Box<dyn Operator>,
        exprs: &[(ScalarExpr, String)],
        schema: Schema,
    ) -> Result<Self, ExecError> {
        let child_schema = child.schema().clone();
        let scalars: Vec<ScalarExpr> = exprs.iter().map(|(e, _)| e.clone()).collect();
        let output_types: Vec<DataType> = exprs.iter().map(|(e, _)| e.data_type()).collect();
        Ok(Self {
            child,
            schema,
            exprs: scalars,
            output_types,
            child_schema,
            done: false,
        })
    }
}

impl Operator for ProjectExprs {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.done {
            return Ok(None);
        }
        let Some(batch) = self.child.next_batch()? else {
            self.done = true;
            return Ok(None);
        };
        let rows = batch_to_rows(&batch, &self.child_schema)?;
        let n_rows = rows.len();
        let n_cols = self.exprs.len();
        let mut out_values: Vec<Vec<Value>> =
            (0..n_cols).map(|_| Vec::with_capacity(n_rows)).collect();
        let no_params: &[Value] = &[];
        for row in &rows {
            for (col_idx, expr) in self.exprs.iter().enumerate() {
                let val = eval_expr(expr, row, no_params)
                    .map_err(|e| ExecError::TypeMismatch(e.to_string()))?;
                out_values[col_idx].push(val);
            }
        }
        let mut columns: Vec<Column> = Vec::with_capacity(n_cols);
        for (col_idx, vals) in out_values.into_iter().enumerate() {
            let dt = &self.output_types[col_idx];
            columns.push(build_column(dt, vals)?);
        }
        Ok(Some(Batch::new(columns)?))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

/// Assemble a column-oriented batch column from a column-major
/// `Vec<Value>`. The function knows every value type the v0.6 row
/// codec encodes; non-encodable types return a precise error.
fn build_column(dt: &DataType, values: Vec<Value>) -> Result<Column, ExecError> {
    let n = values.len();
    let mut nulls = Bitmap::new(n, true);
    let mut any_null = false;
    macro_rules! numeric_column {
        ($vec_ty:ty, $value_pat:pat => $extract:expr, $default:expr, $column_ctor:ident) => {{
            let mut data: Vec<$vec_ty> = Vec::with_capacity(n);
            for (i, v) in values.iter().enumerate() {
                match v {
                    Value::Null => {
                        nulls.set(i, false);
                        any_null = true;
                        data.push($default);
                    }
                    $value_pat => data.push($extract),
                    other => {
                        return Err(ExecError::TypeMismatch(format!(
                            "projection: expected {dt:?} at row {i}, got {:?}",
                            other.data_type()
                        )));
                    }
                }
            }
            if any_null {
                Ok(Column::$column_ctor(
                    NumericColumn::with_nulls(data, nulls)
                        .map_err(|e| ExecError::TypeMismatch(e.to_string()))?,
                ))
            } else {
                Ok(Column::$column_ctor(NumericColumn::from_data(data)))
            }
        }};
    }
    match dt {
        DataType::Bool => {
            let mut data: Vec<bool> = Vec::with_capacity(n);
            for (i, v) in values.iter().enumerate() {
                match v {
                    Value::Null => {
                        nulls.set(i, false);
                        any_null = true;
                        data.push(false);
                    }
                    Value::Bool(b) => data.push(*b),
                    other => {
                        return Err(ExecError::TypeMismatch(format!(
                            "projection: expected Bool at row {i}, got {:?}",
                            other.data_type()
                        )));
                    }
                }
            }
            if any_null {
                Ok(Column::Bool(
                    BoolColumn::with_nulls(data, nulls)
                        .map_err(|e| ExecError::TypeMismatch(e.to_string()))?,
                ))
            } else {
                Ok(Column::Bool(BoolColumn::from_data(data)))
            }
        }
        DataType::Int16 => {
            // The vec layer has no `Int16` column variant — widen to
            // `Int32` so the downstream wire encoder still works.
            let mut data: Vec<i32> = Vec::with_capacity(n);
            for (i, v) in values.iter().enumerate() {
                match v {
                    Value::Null => {
                        nulls.set(i, false);
                        any_null = true;
                        data.push(0);
                    }
                    Value::Int16(x) => data.push(i32::from(*x)),
                    other => {
                        return Err(ExecError::TypeMismatch(format!(
                            "projection: expected Int16 at row {i}, got {:?}",
                            other.data_type()
                        )));
                    }
                }
            }
            if any_null {
                Ok(Column::Int32(
                    NumericColumn::with_nulls(data, nulls)
                        .map_err(|e| ExecError::TypeMismatch(e.to_string()))?,
                ))
            } else {
                Ok(Column::Int32(NumericColumn::from_data(data)))
            }
        }
        DataType::Int32 => numeric_column!(i32, Value::Int32(v) => *v, 0_i32, Int32),
        DataType::Int64 => numeric_column!(
            i64,
            Value::Int64(v) | Value::Timestamp(v) | Value::TimestampTz(v) | Value::Time(v) => *v,
            0_i64,
            Int64
        ),
        DataType::Float32 => numeric_column!(f32, Value::Float32(v) => *v, 0.0_f32, Float32),
        DataType::Float64 => numeric_column!(f64, Value::Float64(v) => *v, 0.0_f64, Float64),
        DataType::Date => numeric_column!(i32, Value::Date(v) => *v, 0_i32, Int32),
        DataType::Decimal { .. } => numeric_column!(
            i64,
            Value::Decimal { value, .. } => *value,
            0_i64,
            Int64
        ),
        DataType::Timestamp | DataType::TimestampTz | DataType::Time => numeric_column!(
            i64,
            Value::Timestamp(v) | Value::TimestampTz(v) | Value::Time(v) => *v,
            0_i64,
            Int64
        ),
        DataType::Text { .. } => {
            let mut strings: Vec<Option<String>> = Vec::with_capacity(n);
            for (i, v) in values.iter().enumerate() {
                match v {
                    Value::Null => strings.push(None),
                    Value::Text(s) => strings.push(Some(s.clone())),
                    other => {
                        return Err(ExecError::TypeMismatch(format!(
                            "projection: expected Text at row {i}, got {:?}",
                            other.data_type()
                        )));
                    }
                }
            }
            Ok(
                match encode_strings_auto(
                    strings.iter().map(|v| v.as_deref()),
                    DictionaryEncodingPolicy::default(),
                ) {
                    StringEncoding::Raw(c) => Column::Utf8(c),
                    StringEncoding::Dictionary(c) => Column::DictionaryUtf8(c),
                },
            )
        }
        DataType::Null => {
            // All-NULL output column. Carry an Int32 storage; the
            // schema field tag still reports `Null`.
            let data: Vec<i32> = vec![0_i32; n];
            let mut bm = Bitmap::new(n, false);
            for i in 0..n {
                bm.set(i, false);
            }
            Ok(Column::Int32(
                NumericColumn::with_nulls(data, bm)
                    .map_err(|e| ExecError::TypeMismatch(e.to_string()))?,
            ))
        }
        other => Err(ExecError::TypeMismatch(format!(
            "projection: column type {other:?} not yet supported"
        ))),
    }
}
