//! Pushable `column OP literal` predicate for `read_parquet` scans.

use std::cmp::Ordering;

use arrow_array::{
    Array, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, LargeStringArray,
    RecordBatch, StringArray,
};
use arrow_schema::{ArrowError, Schema as ArrowSchema};
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{ArrowPredicateFn, RowFilter};
use ultrasql_core::Value;
use ultrasql_planner::{BinaryOp, ScalarExpr};

use crate::error::ServerError;

/// Predicate shape that can be pushed into a Parquet scan.
#[derive(Clone, Debug)]
pub(in crate::pipeline) struct ParquetPredicate {
    pub(super) column: String,
    pub(super) op: BinaryOp,
    pub(super) literal: ParquetLiteral,
}

#[derive(Clone, Debug)]
pub(super) enum ParquetLiteral {
    Bool(bool),
    Int64(i64),
    Float64(f64),
    Text(String),
}

impl ParquetPredicate {
    /// Extract a simple `column OP literal` predicate.
    pub(in crate::pipeline) fn from_scalar(expr: &ScalarExpr) -> Option<Self> {
        let ScalarExpr::Binary {
            op, left, right, ..
        } = expr
        else {
            return None;
        };
        if !is_supported_cmp(*op) {
            return None;
        }
        if let (Some(column), Some(literal)) = (column_name(left), literal_value(right)) {
            return Some(Self {
                column,
                op: *op,
                literal,
            });
        }
        if let (Some(literal), Some(column)) = (literal_value(left), column_name(right)) {
            return Some(Self {
                column,
                op: reverse_cmp(*op),
                literal,
            });
        }
        None
    }

    pub(super) fn resolved_for_schema(&self, schema: &ArrowSchema) -> Result<Self, ServerError> {
        let field = schema
            .fields()
            .iter()
            .find(|field| field.name().eq_ignore_ascii_case(&self.column))
            .ok_or_else(|| {
                ServerError::CopyFormat(format!(
                    "read_parquet predicate column not found: {}",
                    self.column
                ))
            })?;
        Ok(Self {
            column: field.name().clone(),
            op: self.op,
            literal: self.literal.clone(),
        })
    }

    pub(super) fn row_filter(
        &self,
        parquet_schema: &parquet::schema::types::SchemaDescriptor,
    ) -> RowFilter {
        let column = self.column.clone();
        let op = self.op;
        let literal = self.literal.clone();
        let projection = ProjectionMask::columns(parquet_schema, [column.as_str()]);
        let predicate = ArrowPredicateFn::new(projection, move |batch: RecordBatch| {
            let array = batch.column(0).as_ref();
            evaluate_arrow_predicate(array, op, &literal)
        });
        RowFilter::new(vec![Box::new(predicate)])
    }
}

fn evaluate_arrow_predicate(
    array: &dyn Array,
    op: BinaryOp,
    literal: &ParquetLiteral,
) -> Result<BooleanArray, ArrowError> {
    let values = match literal {
        ParquetLiteral::Bool(value) => {
            let typed = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    ArrowError::ComputeError(
                        "read_parquet Boolean predicate downcast failed".to_owned(),
                    )
                })?;
            (0..typed.len())
                .map(|idx| !typed.is_null(idx) && compare_bool(typed.value(idx), *value, op))
                .collect::<Vec<_>>()
        }
        ParquetLiteral::Int64(value) => {
            if let Some(typed) = array.as_any().downcast_ref::<Int64Array>() {
                (0..typed.len())
                    .map(|idx| !typed.is_null(idx) && compare_i64(typed.value(idx), *value, op))
                    .collect::<Vec<_>>()
            } else if let Some(typed) = array.as_any().downcast_ref::<Int32Array>() {
                (0..typed.len())
                    .map(|idx| {
                        !typed.is_null(idx) && compare_i64(i64::from(typed.value(idx)), *value, op)
                    })
                    .collect::<Vec<_>>()
            } else {
                return Err(ArrowError::ComputeError(
                    "read_parquet integer predicate downcast failed".to_owned(),
                ));
            }
        }
        ParquetLiteral::Float64(value) => {
            if let Some(typed) = array.as_any().downcast_ref::<Float64Array>() {
                (0..typed.len())
                    .map(|idx| !typed.is_null(idx) && compare_f64(typed.value(idx), *value, op))
                    .collect::<Vec<_>>()
            } else if let Some(typed) = array.as_any().downcast_ref::<Float32Array>() {
                (0..typed.len())
                    .map(|idx| {
                        !typed.is_null(idx) && compare_f64(f64::from(typed.value(idx)), *value, op)
                    })
                    .collect::<Vec<_>>()
            } else {
                return Err(ArrowError::ComputeError(
                    "read_parquet float predicate downcast failed".to_owned(),
                ));
            }
        }
        ParquetLiteral::Text(value) => {
            if let Some(typed) = array.as_any().downcast_ref::<StringArray>() {
                (0..typed.len())
                    .map(|idx| {
                        !typed.is_null(idx) && compare_str(typed.value(idx), value.as_str(), op)
                    })
                    .collect::<Vec<_>>()
            } else if let Some(typed) = array.as_any().downcast_ref::<LargeStringArray>() {
                (0..typed.len())
                    .map(|idx| {
                        !typed.is_null(idx) && compare_str(typed.value(idx), value.as_str(), op)
                    })
                    .collect::<Vec<_>>()
            } else {
                return Err(ArrowError::ComputeError(
                    "read_parquet text predicate downcast failed".to_owned(),
                ));
            }
        }
    };
    Ok(BooleanArray::from(values))
}

fn compare_bool(left: bool, right: bool, op: BinaryOp) -> bool {
    compare_ordering(left.cmp(&right), op)
}

fn compare_i64(left: i64, right: i64, op: BinaryOp) -> bool {
    compare_ordering(left.cmp(&right), op)
}

fn compare_f64(left: f64, right: f64, op: BinaryOp) -> bool {
    left.partial_cmp(&right)
        .is_some_and(|ordering| compare_ordering(ordering, op))
}

fn compare_str(left: &str, right: &str, op: BinaryOp) -> bool {
    compare_ordering(left.cmp(right), op)
}

fn compare_ordering(ordering: Ordering, op: BinaryOp) -> bool {
    match op {
        BinaryOp::Eq => ordering == Ordering::Equal,
        BinaryOp::NotEq => ordering != Ordering::Equal,
        BinaryOp::Lt => ordering == Ordering::Less,
        BinaryOp::LtEq => matches!(ordering, Ordering::Less | Ordering::Equal),
        BinaryOp::Gt => ordering == Ordering::Greater,
        BinaryOp::GtEq => matches!(ordering, Ordering::Greater | Ordering::Equal),
        _ => false,
    }
}

fn column_name(expr: &ScalarExpr) -> Option<String> {
    match expr {
        ScalarExpr::Column { name, .. } => Some(name.clone()),
        _ => None,
    }
}

fn literal_value(expr: &ScalarExpr) -> Option<ParquetLiteral> {
    match expr {
        ScalarExpr::Literal {
            value: Value::Bool(value),
            ..
        } => Some(ParquetLiteral::Bool(*value)),
        ScalarExpr::Literal {
            value: Value::Int16(value),
            ..
        } => Some(ParquetLiteral::Int64(i64::from(*value))),
        ScalarExpr::Literal {
            value: Value::Int32(value),
            ..
        } => Some(ParquetLiteral::Int64(i64::from(*value))),
        ScalarExpr::Literal {
            value: Value::Int64(value),
            ..
        } => Some(ParquetLiteral::Int64(*value)),
        ScalarExpr::Literal {
            value: Value::Float32(value),
            ..
        } => Some(ParquetLiteral::Float64(f64::from(*value))),
        ScalarExpr::Literal {
            value: Value::Float64(value),
            ..
        } => Some(ParquetLiteral::Float64(*value)),
        ScalarExpr::Literal {
            value: Value::Text(value),
            ..
        } => Some(ParquetLiteral::Text(value.clone())),
        _ => None,
    }
}

fn is_supported_cmp(op: BinaryOp) -> bool {
    matches!(
        op,
        BinaryOp::Eq
            | BinaryOp::NotEq
            | BinaryOp::Lt
            | BinaryOp::LtEq
            | BinaryOp::Gt
            | BinaryOp::GtEq
    )
}

fn reverse_cmp(op: BinaryOp) -> BinaryOp {
    match op {
        BinaryOp::Lt => BinaryOp::Gt,
        BinaryOp::LtEq => BinaryOp::GtEq,
        BinaryOp::Gt => BinaryOp::Lt,
        BinaryOp::GtEq => BinaryOp::LtEq,
        other => other,
    }
}
