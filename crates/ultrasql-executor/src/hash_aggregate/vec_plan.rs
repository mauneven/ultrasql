//! Vectorised scalar- and grouped-aggregate fast-path planning.
//!
//! These types and builders decide whether a `HashAggregate` can run its
//! build phase through the column-oriented kernels (in [`super::vec_step`])
//! instead of the row-at-a-time scalar loop. A plan is produced only when
//! every aggregate falls inside the supported fast set; otherwise the
//! builders return `None` and the operator falls back to the general path.

use std::collections::HashMap;

use ultrasql_core::{DataType, Schema, Value};
use ultrasql_planner::{AggregateFunc, LogicalAggregateExpr, ScalarExpr};
use ultrasql_vec::Batch;
use ultrasql_vec::column::Column;

use crate::ExecError;

// ---------------------------------------------------------------------------
// Vectorised scalar-aggregate fast path
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
pub(crate) enum GroupedKey {
    Int32,
    Int64,
}

#[derive(Clone, Copy, Debug)]
enum GroupedAgg {
    SumColumn {
        index: usize,
    },
    SumMul {
        left_index: usize,
        right_index: usize,
    },
}

#[derive(Clone, Debug)]
pub(crate) struct GroupedVecPlan {
    pub(crate) key: GroupedKey,
    key_index: usize,
    agg: GroupedAgg,
    pub(crate) result_type: DataType,
}

/// One slot of the vectorised plan: which kernel to run and which column it
/// reads. `CountStar` is the only slot without a column reference.
#[derive(Debug, Clone)]
pub(crate) enum VecAggSlot {
    CountStar,
    Count(usize),
    Sum(usize),
    Avg(usize),
    Min(usize),
    Max(usize),
}

/// Build a [`VecAggSlot`] plan when every aggregate is in the supported
/// scalar fast set and references a simple column. Returns `None` if any
/// aggregate falls outside the fast set (e.g. `STRING_AGG`, `BOOL_AND`,
/// `DISTINCT`, or a non-`Column` argument expression).
pub(crate) fn build_vectorized_plan(
    aggregates: &[LogicalAggregateExpr],
) -> Option<Vec<VecAggSlot>> {
    let mut plan = Vec::with_capacity(aggregates.len());
    for agg in aggregates {
        if agg.distinct {
            return None;
        }
        match agg.func {
            AggregateFunc::CountStar => {
                if agg.arg.is_some() {
                    return None;
                }
                plan.push(VecAggSlot::CountStar);
            }
            AggregateFunc::Count
            | AggregateFunc::Sum
            | AggregateFunc::Avg
            | AggregateFunc::Min
            | AggregateFunc::Max => {
                let arg = agg.arg.as_ref()?;
                plan.push(match agg.func {
                    AggregateFunc::Count
                    | AggregateFunc::Avg
                    | AggregateFunc::Min
                    | AggregateFunc::Max => {
                        let (idx, data_type) = column_ref(arg)?;
                        if !matches!(
                            data_type,
                            DataType::Int32
                                | DataType::Int64
                                | DataType::Float32
                                | DataType::Float64
                        ) {
                            return None;
                        }
                        match agg.func {
                            AggregateFunc::Count => VecAggSlot::Count(idx),
                            AggregateFunc::Avg => VecAggSlot::Avg(idx),
                            AggregateFunc::Min => VecAggSlot::Min(idx),
                            AggregateFunc::Max => VecAggSlot::Max(idx),
                            _ => unreachable!(),
                        }
                    }
                    AggregateFunc::Sum => {
                        if let Some((idx, data_type)) = column_ref(arg) {
                            if !matches!(
                                data_type,
                                DataType::Int32
                                    | DataType::Int64
                                    | DataType::Float32
                                    | DataType::Float64
                            ) {
                                return None;
                            }
                            VecAggSlot::Sum(idx)
                        } else {
                            return None;
                        }
                    }
                    _ => unreachable!(),
                });
            }
            _ => return None,
        }
    }
    Some(plan)
}

pub(crate) fn build_grouped_vectorized_plan(
    group_keys: &[ScalarExpr],
    aggregates: &[LogicalAggregateExpr],
    child_schema: &Schema,
) -> Option<GroupedVecPlan> {
    if group_keys.len() != 1 || aggregates.len() != 1 {
        return None;
    }
    let (key_index, key) = match &group_keys[0] {
        ScalarExpr::Column {
            index,
            data_type: DataType::Int32,
            ..
        }
        | ScalarExpr::Column {
            index,
            data_type: DataType::Date,
            ..
        } => (*index, GroupedKey::Int32),
        ScalarExpr::Column {
            index,
            data_type: DataType::Int64,
            ..
        } => (*index, GroupedKey::Int64),
        _ => return None,
    };
    let agg = aggregates.first()?;
    if agg.distinct || agg.func != AggregateFunc::Sum {
        return None;
    }
    let arg = agg.arg.as_ref()?;
    let grouped_agg = match arg {
        ScalarExpr::Column { index, .. }
            if numeric_storage_kind(&child_schema.field_at(*index).data_type) =>
        {
            GroupedAgg::SumColumn { index: *index }
        }
        ScalarExpr::Binary {
            op: ultrasql_planner::BinaryOp::Mul,
            left,
            right,
            ..
        } => {
            let (left_index, right_index) = match (&**left, &**right) {
                (
                    ScalarExpr::Column {
                        index: left_index, ..
                    },
                    ScalarExpr::Column {
                        index: right_index, ..
                    },
                ) => (*left_index, *right_index),
                _ => return None,
            };
            if !numeric_storage_kind(&child_schema.field_at(left_index).data_type)
                || !numeric_storage_kind(&child_schema.field_at(right_index).data_type)
            {
                return None;
            }
            GroupedAgg::SumMul {
                left_index,
                right_index,
            }
        }
        _ => return None,
    };
    Some(GroupedVecPlan {
        key,
        key_index,
        agg: grouped_agg,
        result_type: agg.data_type.clone(),
    })
}

fn numeric_storage_kind(data_type: &DataType) -> bool {
    // NB: `Decimal` is intentionally excluded. Decimal columns now
    // materialise as decimal text (i128-backed, lossless) rather than a
    // raw i64 batch column, so the i64 vectorised SUM path does not apply;
    // decimal SUM/SUM(a*b) falls back to the row-based i128 accumulator,
    // which carries the full mantissa and raises 22003 on i128 overflow.
    matches!(
        data_type,
        DataType::Int32
            | DataType::Int64
            | DataType::Date
            | DataType::Timestamp
            | DataType::TimestampTz
            | DataType::Time
            | DataType::TimeTz
    )
}

pub(crate) fn grouped_vectorized_step(
    plan: &GroupedVecPlan,
    batch: &Batch,
    table: &mut HashMap<Option<i64>, Option<i64>>,
) -> Result<(), ExecError> {
    let cols = batch.columns();
    for row in 0..batch.rows() {
        let key = match plan.key {
            GroupedKey::Int32 => read_i32_key(cols.get(plan.key_index), row)?,
            GroupedKey::Int64 => read_i64_key(cols.get(plan.key_index), row)?,
        };
        let delta = match plan.agg {
            GroupedAgg::SumColumn { index } => read_numeric_value(cols.get(index), row)?,
            GroupedAgg::SumMul {
                left_index,
                right_index,
            } => {
                let left = read_numeric_value(cols.get(left_index), row)?;
                let right = read_numeric_value(cols.get(right_index), row)?;
                match (left, right) {
                    (Some(left), Some(right)) => {
                        Some(left.checked_mul(right).ok_or_else(|| {
                            ExecError::TypeMismatch(
                                "grouped aggregate multiply overflow".to_owned(),
                            )
                        })?)
                    }
                    _ => None,
                }
            }
        };
        let entry = table.entry(key).or_insert(None);
        if let Some(delta) = delta {
            *entry = Some(match *entry {
                Some(current) => current.checked_add(delta).ok_or_else(|| {
                    ExecError::TypeMismatch("grouped aggregate sum overflow".to_owned())
                })?,
                None => delta,
            });
        }
    }
    Ok(())
}

pub(crate) fn read_i32_key(column: Option<&Column>, row: usize) -> Result<Option<i64>, ExecError> {
    match column {
        Some(Column::Int32(c)) => {
            if c.nulls().is_some_and(|nulls| !nulls.get(row)) {
                Ok(None)
            } else {
                Ok(Some(i64::from(c.data()[row])))
            }
        }
        Some(other) => Err(ExecError::TypeMismatch(format!(
            "grouped aggregate Int32 key requires Int32 column, got {:?}",
            other.data_type()
        ))),
        None => Err(ExecError::Internal(
            "grouped aggregate key column out of range",
        )),
    }
}

pub(crate) fn read_i64_key(column: Option<&Column>, row: usize) -> Result<Option<i64>, ExecError> {
    match column {
        Some(Column::Int64(c)) => {
            if c.nulls().is_some_and(|nulls| !nulls.get(row)) {
                Ok(None)
            } else {
                Ok(Some(c.data()[row]))
            }
        }
        Some(other) => Err(ExecError::TypeMismatch(format!(
            "grouped aggregate Int64 key requires Int64 column, got {:?}",
            other.data_type()
        ))),
        None => Err(ExecError::Internal(
            "grouped aggregate key column out of range",
        )),
    }
}

pub(crate) fn read_numeric_value(
    column: Option<&Column>,
    row: usize,
) -> Result<Option<i64>, ExecError> {
    match column {
        Some(Column::Int32(c)) => {
            if c.nulls().is_some_and(|nulls| !nulls.get(row)) {
                Ok(None)
            } else {
                Ok(Some(i64::from(c.data()[row])))
            }
        }
        Some(Column::Int64(c)) => {
            if c.nulls().is_some_and(|nulls| !nulls.get(row)) {
                Ok(None)
            } else {
                Ok(Some(c.data()[row]))
            }
        }
        Some(other) => Err(ExecError::TypeMismatch(format!(
            "grouped aggregate numeric input requires Int32/Int64 column, got {:?}",
            other.data_type()
        ))),
        None => Err(ExecError::Internal(
            "grouped aggregate numeric column out of range",
        )),
    }
}

pub(crate) fn finalize_grouped_sum(sum: i64, data_type: &DataType) -> Result<Value, ExecError> {
    match data_type {
        // Decimal columns no longer use the i64 vectorised SUM path
        // (`numeric_storage_kind` excludes Decimal), so this arm is not
        // reached for decimals in practice; kept for totality with an
        // exact i64->i128 widening (no truncation).
        DataType::Decimal { scale, .. } => Ok(Value::Decimal {
            value: i128::from(sum),
            scale: scale.unwrap_or(0),
        }),
        DataType::Int64 => Ok(Value::Int64(sum)),
        DataType::Int32 => i32::try_from(sum)
            .map(Value::Int32)
            .map_err(|_| ExecError::NumericFieldOverflow("grouped INT sum overflow".to_owned())),
        _ => Ok(Value::Int64(sum)),
    }
}

/// Extract the column index and type from a `ScalarExpr::Column`. Returns
/// `None` for anything else (literals, binary ops, casts, …) — those go
/// through the scalar row loop.
fn column_ref(expr: &ScalarExpr) -> Option<(usize, DataType)> {
    match expr {
        ScalarExpr::Column {
            index, data_type, ..
        } => Some((*index, data_type.clone())),
        _ => None,
    }
}
