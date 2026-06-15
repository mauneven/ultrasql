//! PIVOT and UNPIVOT physical operators.

use std::collections::HashMap;

use num_traits::ToPrimitive;
use ultrasql_core::{DataType, Schema, Value};
use ultrasql_planner::{
    AggregateFunc, LogicalPivotAggregate, LogicalPivotValue, LogicalUnpivotColumn,
};
use ultrasql_vec::Batch;

use crate::{Eval, ExecError, Operator, RowCodec, batch_to_rows, build_batch};

const BATCH_TARGET_ROWS: usize = 4096;

/// Physical `PIVOT` operator.
#[derive(Debug)]
pub struct Pivot {
    child: Box<dyn Operator>,
    input_schema: Schema,
    group_columns: Vec<usize>,
    pivot_column: usize,
    aggregate: LogicalPivotAggregate,
    pivot_values: Vec<LogicalPivotValue>,
    schema: Schema,
    group_codec: RowCodec,
    output_rows: Option<Vec<Vec<Value>>>,
    offset: usize,
}

impl Pivot {
    /// Build a PIVOT operator.
    ///
    /// # Errors
    ///
    /// Returns an executor error if the group-key schema cannot be built.
    pub fn try_new(
        child: Box<dyn Operator>,
        group_columns: Vec<usize>,
        pivot_column: usize,
        aggregate: LogicalPivotAggregate,
        pivot_values: Vec<LogicalPivotValue>,
        schema: Schema,
    ) -> Result<Self, ExecError> {
        let input_schema = child.schema().clone();
        let group_fields = group_columns
            .iter()
            .map(|idx| input_schema.field_at(*idx).clone())
            .collect::<Vec<_>>();
        let group_schema = Schema::new(group_fields)
            .map_err(|err| ExecError::TypeMismatch(format!("PIVOT group schema: {err}")))?;
        Ok(Self {
            child,
            input_schema,
            group_columns,
            pivot_column,
            aggregate,
            pivot_values,
            schema,
            group_codec: RowCodec::new(group_schema),
            output_rows: None,
            offset: 0,
        })
    }

    fn materialize(&mut self) -> Result<(), ExecError> {
        if self.output_rows.is_some() {
            return Ok(());
        }

        let mut groups: Vec<PivotGroup> = Vec::new();
        let mut group_index: HashMap<Vec<u8>, usize> = HashMap::new();
        let arg_eval = self.aggregate.arg.clone().map(Eval::new);

        while let Some(batch) = self.child.next_batch()? {
            for row in batch_to_rows(&batch, &self.input_schema)? {
                let key_values = self
                    .group_columns
                    .iter()
                    .map(|idx| row[*idx].clone())
                    .collect::<Vec<_>>();
                let encoded_key = self
                    .group_codec
                    .encode(&key_values)
                    .map_err(|err| ExecError::TypeMismatch(format!("PIVOT group key: {err}")))?;
                let idx = if let Some(idx) = group_index.get(&encoded_key) {
                    *idx
                } else {
                    let idx = groups.len();
                    group_index.insert(encoded_key, idx);
                    groups.push(PivotGroup {
                        key_values,
                        states: self
                            .pivot_values
                            .iter()
                            .map(|_| PivotAggState::new(&self.aggregate))
                            .collect(),
                    });
                    idx
                };

                let pivot_value = &row[self.pivot_column];
                if matches!(pivot_value, Value::Null) {
                    continue;
                }
                let arg_value = if let Some(eval) = &arg_eval {
                    Some(eval.eval(&row).map_err(crate::eval_error_to_exec_error)?)
                } else {
                    None
                };
                for (bucket_idx, pivot_spec) in self.pivot_values.iter().enumerate() {
                    if values_equal(pivot_value, &pivot_spec.value) {
                        groups[idx].states[bucket_idx].accumulate(arg_value.as_ref())?;
                    }
                }
            }
        }

        if groups.is_empty() && self.group_columns.is_empty() {
            groups.push(PivotGroup {
                key_values: Vec::new(),
                states: self
                    .pivot_values
                    .iter()
                    .map(|_| PivotAggState::new(&self.aggregate))
                    .collect(),
            });
        }

        let mut rows = Vec::with_capacity(groups.len());
        for group in groups {
            let mut row = group.key_values;
            for state in group.states {
                row.push(state.finish(&self.aggregate)?);
            }
            rows.push(row);
        }
        self.output_rows = Some(rows);
        Ok(())
    }
}

impl Operator for Pivot {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        self.materialize()?;
        let rows = self
            .output_rows
            .as_ref()
            .ok_or(ExecError::Internal("pivot rows initialized"))?;
        if self.offset >= rows.len() {
            return Ok(None);
        }
        let end = self
            .offset
            .saturating_add(BATCH_TARGET_ROWS)
            .min(rows.len());
        let batch = build_batch(&rows[self.offset..end], &self.schema)?;
        self.offset = end;
        Ok(Some(batch))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn estimated_row_count(&self) -> Option<usize> {
        self.output_rows.as_ref().map(Vec::len)
    }

    fn profile_children(&self) -> Vec<&dyn Operator> {
        vec![self.child.as_ref()]
    }
}

#[derive(Debug)]
struct PivotGroup {
    key_values: Vec<Value>,
    states: Vec<PivotAggState>,
}

#[derive(Debug)]
enum PivotAggState {
    Count(i64),
    SumInt { sum: i128, saw: bool },
    SumFloat { sum: f64, saw: bool },
    Avg { sum: f64, count: i64 },
    Min(Option<Value>),
    Max(Option<Value>),
}

impl PivotAggState {
    fn new(aggregate: &LogicalPivotAggregate) -> Self {
        match aggregate.func {
            AggregateFunc::CountStar | AggregateFunc::Count => Self::Count(0),
            AggregateFunc::Sum => {
                if matches!(aggregate.data_type, DataType::Float64 | DataType::Float32) {
                    Self::SumFloat {
                        sum: 0.0,
                        saw: false,
                    }
                } else {
                    Self::SumInt { sum: 0, saw: false }
                }
            }
            AggregateFunc::Avg => Self::Avg { sum: 0.0, count: 0 },
            AggregateFunc::Min => Self::Min(None),
            AggregateFunc::Max => Self::Max(None),
            _ => Self::Count(0),
        }
    }

    fn accumulate(&mut self, value: Option<&Value>) -> Result<(), ExecError> {
        match self {
            Self::Count(count) => {
                if value.is_none() || value.is_some_and(|v| !matches!(v, Value::Null)) {
                    *count = count.checked_add(1).ok_or_else(|| {
                        ExecError::NumericFieldOverflow("PIVOT COUNT overflow".to_owned())
                    })?;
                }
            }
            Self::SumInt { sum, saw } => {
                let Some(value) = value.filter(|v| !matches!(v, Value::Null)) else {
                    return Ok(());
                };
                *sum = sum.checked_add(value_as_i128(value)?).ok_or_else(|| {
                    ExecError::NumericFieldOverflow("PIVOT SUM overflow".to_owned())
                })?;
                *saw = true;
            }
            Self::SumFloat { sum, saw } => {
                let Some(value) = value.filter(|v| !matches!(v, Value::Null)) else {
                    return Ok(());
                };
                *sum += value_as_f64(value)?;
                *saw = true;
            }
            Self::Avg { sum, count } => {
                let Some(value) = value.filter(|v| !matches!(v, Value::Null)) else {
                    return Ok(());
                };
                *sum += value_as_f64(value)?;
                *count = count.checked_add(1).ok_or_else(|| {
                    ExecError::NumericFieldOverflow("PIVOT AVG count overflow".to_owned())
                })?;
            }
            Self::Min(current) => {
                let Some(value) = value.filter(|v| !matches!(v, Value::Null)) else {
                    return Ok(());
                };
                if current
                    .as_ref()
                    .is_none_or(|existing| compare_values(value, existing).is_lt())
                {
                    *current = Some(value.clone());
                }
            }
            Self::Max(current) => {
                let Some(value) = value.filter(|v| !matches!(v, Value::Null)) else {
                    return Ok(());
                };
                if current
                    .as_ref()
                    .is_none_or(|existing| compare_values(value, existing).is_gt())
                {
                    *current = Some(value.clone());
                }
            }
        }
        Ok(())
    }

    fn finish(self, aggregate: &LogicalPivotAggregate) -> Result<Value, ExecError> {
        match self {
            Self::Count(count) => Ok(Value::Int64(count)),
            Self::SumInt { sum, saw } => {
                if saw {
                    let value = i64::try_from(sum).map_err(|_| {
                        ExecError::NumericFieldOverflow("PIVOT SUM overflow".to_owned())
                    })?;
                    Ok(Value::Int64(value))
                } else {
                    Ok(Value::Null)
                }
            }
            Self::SumFloat { sum, saw } => {
                if saw {
                    Ok(Value::Float64(sum))
                } else {
                    Ok(Value::Null)
                }
            }
            Self::Avg { sum, count } => {
                if count == 0 {
                    Ok(Value::Null)
                } else {
                    let denom = count.to_f64().ok_or_else(|| {
                        ExecError::NumericFieldOverflow("PIVOT AVG count overflow".to_owned())
                    })?;
                    Ok(Value::Float64(sum / denom))
                }
            }
            Self::Min(value) | Self::Max(value) => Ok(value.unwrap_or(Value::Null)),
        }
        .map(|value| coerce_output_value(value, &aggregate.data_type))
    }
}

/// Physical `UNPIVOT` operator.
#[derive(Debug)]
pub struct Unpivot {
    child: Box<dyn Operator>,
    input_schema: Schema,
    passthrough_columns: Vec<usize>,
    columns: Vec<LogicalUnpivotColumn>,
    include_nulls: bool,
    schema: Schema,
    value_type: DataType,
    pending_rows: Vec<Vec<Value>>,
}

impl Unpivot {
    /// Build an UNPIVOT operator.
    #[must_use]
    pub fn new(
        child: Box<dyn Operator>,
        passthrough_columns: Vec<usize>,
        columns: Vec<LogicalUnpivotColumn>,
        include_nulls: bool,
        schema: Schema,
    ) -> Self {
        let input_schema = child.schema().clone();
        let value_type = schema
            .fields()
            .last()
            .map_or(DataType::Null, |field| field.data_type.clone());
        Self {
            child,
            input_schema,
            passthrough_columns,
            columns,
            include_nulls,
            schema,
            value_type,
            pending_rows: Vec::new(),
        }
    }

    fn fill_pending(&mut self) -> Result<(), ExecError> {
        while self.pending_rows.is_empty() {
            let Some(batch) = self.child.next_batch()? else {
                return Ok(());
            };
            for row in batch_to_rows(&batch, &self.input_schema)? {
                for column in &self.columns {
                    let value = row[column.source_column].clone();
                    if !self.include_nulls && matches!(value, Value::Null) {
                        continue;
                    }
                    let mut out = self
                        .passthrough_columns
                        .iter()
                        .map(|idx| row[*idx].clone())
                        .collect::<Vec<_>>();
                    out.push(Value::Text(column.label.clone()));
                    out.push(coerce_unpivot_value(value, &self.value_type)?);
                    self.pending_rows.push(out);
                }
            }
        }
        Ok(())
    }
}

impl Operator for Unpivot {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        self.fill_pending()?;
        if self.pending_rows.is_empty() {
            return Ok(None);
        }
        let end = self.pending_rows.len().min(BATCH_TARGET_ROWS);
        let batch = build_batch(&self.pending_rows[..end], &self.schema)?;
        self.pending_rows.drain(..end);
        Ok(Some(batch))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn profile_children(&self) -> Vec<&dyn Operator> {
        vec![self.child.as_ref()]
    }
}

fn coerce_output_value(value: Value, data_type: &DataType) -> Value {
    match (value, data_type) {
        (Value::Int16(v), DataType::Int64) => Value::Int64(i64::from(v)),
        (Value::Int32(v), DataType::Int64) => Value::Int64(i64::from(v)),
        (Value::Float32(v), DataType::Float64) => Value::Float64(f64::from(v)),
        (value, _) => value,
    }
}

fn coerce_unpivot_value(value: Value, data_type: &DataType) -> Result<Value, ExecError> {
    match (value, data_type) {
        (Value::Null, _) => Ok(Value::Null),
        (Value::Int16(v), DataType::Int32) => Ok(Value::Int32(i32::from(v))),
        (Value::Int16(v), DataType::Int64) => Ok(Value::Int64(i64::from(v))),
        (Value::Int32(v), DataType::Int64) => Ok(Value::Int64(i64::from(v))),
        (Value::Int16(v), DataType::Float64) => Ok(Value::Float64(f64::from(v))),
        (Value::Int32(v), DataType::Float64) => Ok(Value::Float64(f64::from(v))),
        (Value::Int64(v), DataType::Float64) => {
            let value = v.to_f64().ok_or_else(|| {
                ExecError::NumericFieldOverflow("UNPIVOT integer to float overflow".to_owned())
            })?;
            Ok(Value::Float64(value))
        }
        (Value::Float32(v), DataType::Float64) => Ok(Value::Float64(f64::from(v))),
        (Value::Char(v), DataType::Text { .. }) => Ok(Value::Text(v)),
        (value, expected) if value.data_type() == *expected => Ok(value),
        (value, expected) => Err(ExecError::TypeMismatch(format!(
            "UNPIVOT cannot coerce {:?} to {expected}",
            value.data_type()
        ))),
    }
}

fn value_as_i128(value: &Value) -> Result<i128, ExecError> {
    match value {
        Value::Int16(v) => Ok(i128::from(*v)),
        Value::Int32(v) => Ok(i128::from(*v)),
        Value::Int64(v) => Ok(i128::from(*v)),
        other => Err(ExecError::TypeMismatch(format!(
            "PIVOT SUM expected integer, got {:?}",
            other.data_type()
        ))),
    }
}

fn value_as_f64(value: &Value) -> Result<f64, ExecError> {
    match value {
        Value::Int16(v) => Ok(f64::from(*v)),
        Value::Int32(v) => Ok(f64::from(*v)),
        Value::Int64(v) => v
            .to_f64()
            .ok_or_else(|| ExecError::NumericFieldOverflow("PIVOT numeric overflow".to_owned())),
        Value::Float32(v) => Ok(f64::from(*v)),
        Value::Float64(v) => Ok(*v),
        other => Err(ExecError::TypeMismatch(format!(
            "PIVOT numeric aggregate expected numeric value, got {:?}",
            other.data_type()
        ))),
    }
}

fn values_equal(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Null, _) | (_, Value::Null) => false,
        (Value::Int16(l), Value::Int16(r)) => l == r,
        (Value::Int16(l), Value::Int32(r)) => i32::from(*l) == *r,
        (Value::Int16(l), Value::Int64(r)) => i64::from(*l) == *r,
        (Value::Int32(l), Value::Int16(r)) => *l == i32::from(*r),
        (Value::Int32(l), Value::Int32(r)) => l == r,
        (Value::Int32(l), Value::Int64(r)) => i64::from(*l) == *r,
        (Value::Int64(l), Value::Int16(r)) => *l == i64::from(*r),
        (Value::Int64(l), Value::Int32(r)) => *l == i64::from(*r),
        (Value::Int64(l), Value::Int64(r)) => l == r,
        (Value::Float32(l), Value::Float32(r)) => l.to_bits() == r.to_bits(),
        (Value::Float32(l), Value::Float64(r)) => f64::from(*l).to_bits() == r.to_bits(),
        (Value::Float64(l), Value::Float32(r)) => l.to_bits() == f64::from(*r).to_bits(),
        (Value::Float64(l), Value::Float64(r)) => l.to_bits() == r.to_bits(),
        (Value::Text(l) | Value::Char(l), Value::Text(r) | Value::Char(r)) => l == r,
        _ => left == right,
    }
}

fn compare_values(left: &Value, right: &Value) -> std::cmp::Ordering {
    match (left, right) {
        (Value::Int16(l), Value::Int16(r)) => l.cmp(r),
        (Value::Int32(l), Value::Int32(r)) => l.cmp(r),
        (Value::Int64(l), Value::Int64(r)) => l.cmp(r),
        (Value::Text(l) | Value::Char(l), Value::Text(r) | Value::Char(r)) => l.cmp(r),
        (Value::Bool(l), Value::Bool(r)) => l.cmp(r),
        (Value::Date(l), Value::Date(r)) => l.cmp(r),
        (Value::Time(l), Value::Time(r)) => l.cmp(r),
        (Value::Timestamp(l), Value::Timestamp(r)) => l.cmp(r),
        (Value::TimestampTz(l), Value::TimestampTz(r)) => l.cmp(r),
        (Value::Float32(l), Value::Float32(r)) => {
            l.partial_cmp(r).unwrap_or(std::cmp::Ordering::Equal)
        }
        (Value::Float64(l), Value::Float64(r)) => {
            l.partial_cmp(r).unwrap_or(std::cmp::Ordering::Equal)
        }
        _ => std::cmp::Ordering::Equal,
    }
}
