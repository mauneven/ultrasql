//! Streaming sort-based aggregate operator.
//!
//! [`SortAggregate`] requires the input to be sorted on the GROUP BY keys.
//! It maintains one running aggregate state per group and emits an output
//! row each time the group key changes. This avoids the O(n) hash table
//! that [`HashAggregate`] builds and delivers O(1) extra memory per group.
//!
//! # Supported aggregate functions
//!
//! Same set as [`HashAggregate`]: COUNT(*), COUNT(expr), SUM, AVG, MIN,
//! MAX, `BOOL_AND`, `BOOL_OR`, `STRING_AGG`, `ARRAY_AGG` plus the statistical
//! extensions added in v0.5 (STDDEV, VARIANCE, CORR, `PERCENTILE_CONT`,
//! `PERCENTILE_DISC`).
//!
//! # NULL semantics
//!
//! Identical to [`HashAggregate`]: NULL inputs are skipped for all
//! aggregates except COUNT(*). Two NULL group-key values are treated as
//! equal (the same group).
//!
//! # Empty-input rule
//!
//! If the input is empty and there are no group keys, a single identity
//! row is emitted (matching the SQL standard and [`HashAggregate`]
//! behaviour).
//!
//! [`HashAggregate`]: crate::HashAggregate

use serde_json::{Number as JsonNumber, Value as JsonValue};
use ultrasql_core::{DataType, Schema, Value};
use ultrasql_planner::{AggregateFunc, LogicalAggregateExpr, ScalarExpr};
use ultrasql_vec::Batch;

use crate::aggregate_math::{add_dense_vector_values, divide_dense_vector_values, widen_sum_seed};
use crate::eval::Eval;
use crate::filter_op::batch_to_rows;
use crate::seq_scan::build_batch;
use crate::value_key::decimal_values_equal;
use crate::{ExecError, Operator};

const BATCH_TARGET_ROWS: usize = 4096;

// ---------------------------------------------------------------------------
// Aggregate state (shared with HashAggregate)
// ---------------------------------------------------------------------------

/// Per-group accumulator for a single aggregate function.
#[allow(dead_code)]
#[derive(Debug)]
pub(crate) enum AggState {
    CountStar(i64),
    Count(i64),
    Sum(Option<Value>),
    Avg(Option<Value>, i64),
    Min(Option<Value>),
    Max(Option<Value>),
    BoolAnd(Option<bool>),
    BoolOr(Option<bool>),
    StringAgg(Vec<String>, String),
    ArrayAgg(Vec<Value>),
    JsonAgg(Vec<Value>),
    /// Running sum and count for STDDEV / VARIANCE.
    /// Fields: (`sum_x`, `sum_x2`, count).
    Variance(f64, f64, i64),
    /// Same accumulator for STDDEV (standard deviation).
    Stddev(f64, f64, i64),
    /// For CORR(y, x): (`sum_x`, `sum_y`, `sum_xy`, `sum_x2`, `sum_y2`, count).
    Corr(f64, f64, f64, f64, f64, i64),
    /// `PERCENTILE_CONT`: accumulates numeric values for interpolation.
    PercentileCont {
        values: Vec<f64>,
        fraction: Option<f64>,
        asc: bool,
    },
    /// `PERCENTILE_DISC`: accumulates values and returns one ordered input.
    PercentileDisc {
        values: Vec<Value>,
        fraction: Option<f64>,
        asc: bool,
        nulls_first: bool,
    },
}

#[allow(clippy::missing_const_for_fn)]
fn init_state(agg: &LogicalAggregateExpr) -> AggState {
    match agg.func {
        AggregateFunc::CountStar => AggState::CountStar(0),
        AggregateFunc::Count => AggState::Count(0),
        AggregateFunc::Sum => AggState::Sum(None),
        AggregateFunc::Avg => AggState::Avg(None, 0),
        AggregateFunc::Min => AggState::Min(None),
        AggregateFunc::Max => AggState::Max(None),
        AggregateFunc::BoolAnd => AggState::BoolAnd(None),
        AggregateFunc::BoolOr => AggState::BoolOr(None),
        AggregateFunc::StringAgg => AggState::StringAgg(Vec::new(), String::new()),
        AggregateFunc::ArrayAgg => AggState::ArrayAgg(Vec::new()),
        AggregateFunc::JsonAgg => AggState::JsonAgg(Vec::new()),
        AggregateFunc::Corr => AggState::Corr(0.0, 0.0, 0.0, 0.0, 0.0, 0),
        AggregateFunc::StddevSamp | AggregateFunc::StddevPop => AggState::Stddev(0.0, 0.0, 0),
        AggregateFunc::VarSamp | AggregateFunc::VarPop => AggState::Variance(0.0, 0.0, 0),
        AggregateFunc::PercentileCont => {
            let asc = agg.order_by.as_ref().is_none_or(|key| key.asc);
            AggState::PercentileCont {
                values: Vec::new(),
                fraction: None,
                asc,
            }
        }
        AggregateFunc::PercentileDisc => {
            let (asc, nulls_first) = agg
                .order_by
                .as_ref()
                .map_or((true, false), |key| (key.asc, key.nulls_first));
            AggState::PercentileDisc {
                values: Vec::new(),
                fraction: None,
                asc,
                nulls_first,
            }
        }
    }
}

fn init_states(aggs: &[LogicalAggregateExpr]) -> Vec<AggState> {
    aggs.iter().map(init_state).collect()
}

#[allow(clippy::too_many_lines)]
fn accumulate(
    state: &mut AggState,
    agg: &LogicalAggregateExpr,
    row: &[Value],
) -> Result<(), ExecError> {
    let arg: Option<Value> = agg
        .arg
        .as_ref()
        .map(|expr| {
            Eval::new(expr.clone())
                .eval(row)
                .map_err(|err| ExecError::TypeMismatch(err.to_string()))
        })
        .transpose()?;

    match state {
        AggState::CountStar(n) => {
            *n = n.saturating_add(1);
        }
        AggState::Count(n) => {
            if !matches!(arg, Some(Value::Null) | None) {
                *n = n.saturating_add(1);
            }
        }
        AggState::Sum(acc) => {
            if let Some(v) = arg {
                if !v.is_null() {
                    *acc = Some(match acc.take() {
                        None => widen_sum_seed(v),
                        Some(e) => add_values(e, v)?,
                    });
                }
            }
        }
        AggState::Avg(sum, cnt) => {
            if let Some(v) = arg {
                if !v.is_null() {
                    *sum = Some(match sum.take() {
                        None => widen_sum_seed(v),
                        Some(e) => add_values(e, v)?,
                    });
                    *cnt = cnt.saturating_add(1);
                }
            }
        }
        AggState::Min(cur) => {
            if let Some(v) = arg {
                if !v.is_null() {
                    *cur = Some(match cur.take() {
                        None => v,
                        Some(e) => {
                            if value_lt(&v, &e) {
                                v
                            } else {
                                e
                            }
                        }
                    });
                }
            }
        }
        AggState::Max(cur) => {
            if let Some(v) = arg {
                if !v.is_null() {
                    *cur = Some(match cur.take() {
                        None => v,
                        Some(e) => {
                            if value_lt(&e, &v) {
                                v
                            } else {
                                e
                            }
                        }
                    });
                }
            }
        }
        AggState::BoolAnd(acc) => {
            if let Some(Value::Bool(b)) = arg {
                *acc = Some(acc.unwrap_or(true) && b);
            }
        }
        AggState::BoolOr(acc) => {
            if let Some(Value::Bool(b)) = arg {
                *acc = Some(acc.unwrap_or(false) || b);
            }
        }
        AggState::StringAgg(parts, _) => {
            if let Some(v) = arg {
                if !v.is_null() {
                    match v {
                        Value::Text(s) | Value::Char(s) => parts.push(s),
                        other => parts.push(other.to_string()),
                    }
                }
            }
        }
        AggState::ArrayAgg(items) => {
            if let Some(v) = arg {
                if !v.is_null() {
                    items.push(v);
                }
            }
        }
        AggState::JsonAgg(items) => {
            if let Some(v) = arg {
                items.push(v);
            }
        }
        AggState::Variance(sum_x, sum_x2, cnt) | AggState::Stddev(sum_x, sum_x2, cnt) => {
            if let Some(v) = arg {
                if let Some(x) = to_f64(&v) {
                    *sum_x += x;
                    *sum_x2 += x * x;
                    *cnt = cnt.saturating_add(1);
                }
            }
        }
        AggState::Corr(sx, sy, sxy, sx2, sy2, cnt) => {
            if let Some(Value::Record(fields)) = arg
                && fields.len() >= 2
                && let (Some(y), Some(x)) = (to_f64(&fields[0].1), to_f64(&fields[1].1))
            {
                *sx += x;
                *sy += y;
                *sxy += x * y;
                *sx2 += x * x;
                *sy2 += y * y;
                *cnt = cnt.saturating_add(1);
            }
        }
        AggState::PercentileCont {
            values, fraction, ..
        } => {
            update_percentile_fraction(fraction, percentile_fraction(agg, row)?)?;
            if let Some(v) = arg {
                if !v.is_null() {
                    let x = to_f64(&v).ok_or_else(|| {
                        ExecError::TypeMismatch(
                            "percentile_cont requires numeric order values".to_owned(),
                        )
                    })?;
                    values.push(x);
                }
            }
        }
        AggState::PercentileDisc {
            values, fraction, ..
        } => {
            update_percentile_fraction(fraction, percentile_fraction(agg, row)?)?;
            if let Some(v) = arg
                && !v.is_null()
            {
                values.push(v);
            }
        }
    }
    Ok(())
}

fn finalise(state: &AggState) -> Value {
    match state {
        AggState::CountStar(n) | AggState::Count(n) => Value::Int64(*n),
        AggState::Sum(acc) | AggState::Min(acc) | AggState::Max(acc) => {
            acc.clone().unwrap_or(Value::Null)
        }
        AggState::Avg(sum, cnt) => {
            if *cnt == 0 {
                return Value::Null;
            }
            sum.as_ref()
                .map_or(Value::Null, |s| divide_value(s.clone(), *cnt))
        }
        AggState::BoolAnd(b) | AggState::BoolOr(b) => b.map_or(Value::Null, Value::Bool),
        AggState::StringAgg(parts, sep) => {
            if parts.is_empty() {
                Value::Null
            } else {
                Value::Text(parts.join(sep))
            }
        }
        AggState::ArrayAgg(items) => {
            if items.is_empty() {
                Value::Null
            } else {
                let element_type = items
                    .iter()
                    .find(|v| !v.is_null())
                    .map(Value::data_type)
                    .unwrap_or(DataType::Null);
                Value::Array {
                    element_type,
                    elements: items.clone(),
                }
            }
        }
        AggState::JsonAgg(items) => {
            if items.is_empty() {
                Value::Null
            } else {
                Value::Jsonb(json_agg_text(items))
            }
        }
        AggState::Variance(sum_x, sum_x2, cnt) => {
            if *cnt < 2 {
                return Value::Null;
            }
            let n = *cnt as f64;
            let variance = (*sum_x2 - (*sum_x * *sum_x) / n) / (n - 1.0);
            Value::Float64(variance)
        }
        AggState::Stddev(sum_x, sum_x2, cnt) => {
            if *cnt < 2 {
                return Value::Null;
            }
            let n = *cnt as f64;
            let variance = (*sum_x2 - (*sum_x * *sum_x) / n) / (n - 1.0);
            Value::Float64(variance.sqrt())
        }
        AggState::Corr(sx, sy, sxy, sx2, sy2, cnt) => {
            if *cnt < 2 {
                return Value::Null;
            }
            let n = *cnt as f64;
            let num = n * sxy - sx * sy;
            // Pearson correlation denominator: sqrt((n*sx2-sx^2)(n*sy2-sy^2))
            #[allow(clippy::suspicious_operation_groupings)]
            let den = ((n * sx2 - sx * sx) * (n * sy2 - sy * sy)).sqrt();
            if den == 0.0 {
                Value::Null
            } else {
                Value::Float64(num / den)
            }
        }
        AggState::PercentileCont {
            values,
            fraction,
            asc,
        } => finalise_percentile_cont(values, *fraction, *asc),
        AggState::PercentileDisc {
            values,
            fraction,
            asc,
            nulls_first,
        } => finalise_percentile_disc(values, *fraction, *asc, *nulls_first),
    }
}

fn percentile_fraction(
    agg: &LogicalAggregateExpr,
    row: &[Value],
) -> Result<Option<f64>, ExecError> {
    let Some(direct_arg) = &agg.direct_arg else {
        return Err(ExecError::TypeMismatch(
            "ordered-set percentile missing fraction".to_owned(),
        ));
    };
    let value = Eval::new(direct_arg.clone())
        .eval(row)
        .map_err(|err| ExecError::TypeMismatch(err.to_string()))?;
    if value.is_null() {
        return Ok(None);
    }
    let fraction = to_f64(&value)
        .ok_or_else(|| ExecError::TypeMismatch("percentile fraction must be numeric".to_owned()))?;
    if !(0.0..=1.0).contains(&fraction) || !fraction.is_finite() {
        return Err(ExecError::TypeMismatch(
            "percentile fraction must be between 0 and 1".to_owned(),
        ));
    }
    Ok(Some(fraction))
}

fn update_percentile_fraction(slot: &mut Option<f64>, next: Option<f64>) -> Result<(), ExecError> {
    let Some(next) = next else {
        return Ok(());
    };
    if let Some(existing) = slot {
        if (*existing - next).abs() > f64::EPSILON {
            return Err(ExecError::TypeMismatch(
                "percentile fraction must be constant per group".to_owned(),
            ));
        }
    } else {
        *slot = Some(next);
    }
    Ok(())
}

fn finalise_percentile_cont(values: &[f64], fraction: Option<f64>, asc: bool) -> Value {
    let Some(fraction) = fraction else {
        return Value::Null;
    };
    if values.is_empty() {
        return Value::Null;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    if !asc {
        sorted.reverse();
    }
    let n = sorted.len();
    #[allow(
        clippy::cast_precision_loss,
        reason = "percentile arithmetic; sample count rarely above 2^53"
    )]
    let n_f64 = n as f64;
    let row_number = fraction * (n_f64 - 1.0);
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "row_number bounded by [0, n - 1]"
    )]
    let lo = row_number.floor() as usize;
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "row_number bounded by [0, n - 1]"
    )]
    let hi = row_number.ceil() as usize;
    if lo == hi {
        return Value::Float64(sorted[lo]);
    }
    #[allow(
        clippy::cast_precision_loss,
        reason = "lo is < 2^53 in practice; percentile interpolation"
    )]
    let frac_part = row_number - lo as f64;
    Value::Float64(sorted[hi].mul_add(frac_part, sorted[lo] * (1.0 - frac_part)))
}

fn finalise_percentile_disc(
    values: &[Value],
    fraction: Option<f64>,
    asc: bool,
    nulls_first: bool,
) -> Value {
    let Some(fraction) = fraction else {
        return Value::Null;
    };
    if values.is_empty() {
        return Value::Null;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| {
        let ord = crate::sort::compare_values_nullable(a, b, nulls_first);
        if asc { ord } else { ord.reverse() }
    });
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "percentile_disc arithmetic; idx bounded by ceil(frac * len) <= len"
    )]
    let idx = (fraction * sorted.len() as f64).ceil() as usize;
    sorted[idx.saturating_sub(1).min(sorted.len() - 1)].clone()
}

fn json_agg_text(items: &[Value]) -> String {
    let values = JsonValue::Array(items.iter().map(sql_value_to_json).collect());
    serde_json::to_string(&values).unwrap_or_else(|_| "[]".to_owned())
}

fn sql_value_to_json(value: &Value) -> JsonValue {
    match value {
        Value::Null => JsonValue::Null,
        Value::Bool(v) => JsonValue::Bool(*v),
        Value::Int16(v) => JsonValue::Number(JsonNumber::from(i64::from(*v))),
        Value::Int32(v) => JsonValue::Number(JsonNumber::from(i64::from(*v))),
        Value::Int64(v) => JsonValue::Number(JsonNumber::from(*v)),
        Value::Float32(v) => {
            JsonNumber::from_f64(f64::from(*v)).map_or(JsonValue::Null, JsonValue::Number)
        }
        Value::Float64(v) => JsonNumber::from_f64(*v).map_or(JsonValue::Null, JsonValue::Number),
        Value::Text(v) | Value::Char(v) => JsonValue::String(v.clone()),
        Value::Json(v) | Value::Jsonb(v) => {
            serde_json::from_str(v).unwrap_or_else(|_| JsonValue::String(v.clone()))
        }
        Value::Vector(values) | Value::HalfVec(values) => JsonValue::Array(
            values
                .iter()
                .map(|v| {
                    JsonNumber::from_f64(f64::from(*v)).map_or(JsonValue::Null, JsonValue::Number)
                })
                .collect(),
        ),
        Value::Array { elements, .. } => {
            JsonValue::Array(elements.iter().map(sql_value_to_json).collect())
        }
        other => JsonValue::String(other.to_string()),
    }
}

// ---------------------------------------------------------------------------
// Helper utilities
// ---------------------------------------------------------------------------

fn to_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Int16(x) => Some(f64::from(*x)),
        Value::Int32(x) => Some(f64::from(*x)),
        Value::Int64(x) => Some(*x as f64),
        Value::Float32(x) => Some(f64::from(*x)),
        Value::Float64(x) => Some(*x),
        Value::Decimal { value, scale } => Some(decimal_to_f64(*value, *scale)),
        _ => None,
    }
}

fn add_values(a: Value, b: Value) -> Result<Value, ExecError> {
    match (a, b) {
        (
            Value::Decimal {
                value: x,
                scale: xs,
            },
            Value::Decimal {
                value: y,
                scale: ys,
            },
        ) => add_decimal_values(x, xs, y, ys),
        (Value::Int16(x), Value::Int16(y)) => Ok(Value::Int64(i64::from(x) + i64::from(y))),
        (Value::Int32(x), Value::Int32(y)) => Ok(Value::Int64(i64::from(x) + i64::from(y))),
        (Value::Int64(x), Value::Int64(y)) => Ok(Value::Int64(x.wrapping_add(y))),
        (Value::Float32(x), Value::Float32(y)) => Ok(Value::Float64(f64::from(x) + f64::from(y))),
        (Value::Float64(x), Value::Float64(y)) => Ok(Value::Float64(x + y)),
        (Value::Vector(x), Value::Vector(y)) => {
            add_dense_vector_values(x, y, "vector").map(Value::Vector)
        }
        (Value::HalfVec(x), Value::HalfVec(y)) => {
            add_dense_vector_values(x, y, "halfvec").map(Value::HalfVec)
        }
        (a, b) => Err(ExecError::TypeMismatch(format!(
            "sum type mismatch: {a:?} and {b:?}"
        ))),
    }
}

fn divide_value(sum: Value, count: i64) -> Value {
    match sum {
        Value::Int64(s) => Value::Float64(s as f64 / count as f64),
        Value::Float64(s) => Value::Float64(s / count as f64),
        Value::Decimal { value, scale } => {
            Value::Float64(decimal_to_f64(value, scale) / count as f64)
        }
        Value::Vector(values) => Value::Vector(divide_dense_vector_values(values, count)),
        Value::HalfVec(values) => Value::HalfVec(divide_dense_vector_values(values, count)),
        other => other,
    }
}

fn add_decimal_values(
    left_value: i64,
    left_scale: i32,
    right_value: i64,
    right_scale: i32,
) -> Result<Value, ExecError> {
    let common_scale = left_scale.max(right_scale);
    let left = rescale_decimal_value(left_value, left_scale, common_scale)?;
    let right = rescale_decimal_value(right_value, right_scale, common_scale)?;
    let sum = left
        .checked_add(right)
        .ok_or_else(|| ExecError::TypeMismatch("decimal sum overflow".to_owned()))?;
    let value = i64::try_from(sum)
        .map_err(|_| ExecError::TypeMismatch("decimal sum overflow".to_owned()))?;
    Ok(Value::Decimal {
        value,
        scale: common_scale,
    })
}

fn rescale_decimal_value(
    value: i64,
    current_scale: i32,
    target_scale: i32,
) -> Result<i128, ExecError> {
    let scale_delta = target_scale - current_scale;
    if scale_delta < 0 {
        return Err(ExecError::TypeMismatch(
            "decimal rescale underflow".to_owned(),
        ));
    }
    let factor = pow10_i128(
        u32::try_from(scale_delta)
            .map_err(|_| ExecError::TypeMismatch("decimal rescale overflow".to_owned()))?,
    )
    .ok_or_else(|| ExecError::TypeMismatch("decimal rescale overflow".to_owned()))?;
    i128::from(value)
        .checked_mul(factor)
        .ok_or_else(|| ExecError::TypeMismatch("decimal rescale overflow".to_owned()))
}

fn pow10_i128(exp: u32) -> Option<i128> {
    (0..exp).try_fold(1_i128, |acc, _| acc.checked_mul(10))
}

fn decimal_to_f64(value: i64, scale: i32) -> f64 {
    #[allow(clippy::cast_precision_loss)]
    let raw = value as f64;
    raw / 10_f64.powi(scale)
}

fn value_lt(a: &Value, b: &Value) -> bool {
    use crate::sort::compare_values_nullable;
    matches!(
        compare_values_nullable(a, b, false),
        std::cmp::Ordering::Less
    )
}

/// Compare two group-key rows for equality under DISTINCT semantics
/// (NULL == NULL).
fn keys_equal(a: &[Value], b: &[Value]) -> bool {
    a.len() == b.len()
        && a.iter().zip(b.iter()).all(|(av, bv)| match (av, bv) {
            (Value::Null, Value::Null) => true,
            (Value::Float32(x), Value::Float32(y)) => x.to_bits() == y.to_bits(),
            (Value::Float64(x), Value::Float64(y)) => x.to_bits() == y.to_bits(),
            (
                Value::Decimal {
                    value: left_value,
                    scale: left_scale,
                },
                Value::Decimal {
                    value: right_value,
                    scale: right_scale,
                },
            ) => decimal_values_equal(*left_value, *left_scale, *right_value, *right_scale),
            _ => av == bv,
        })
}

// ---------------------------------------------------------------------------
// Operator
// ---------------------------------------------------------------------------

/// Streaming sort-based aggregate operator.
///
/// The child must be sorted ascending on `group_keys`. The operator
/// advances one group at a time: it reads rows until the group key
/// changes, finalises the aggregates, and then emits the group row.
///
/// # Send
///
/// `Box<dyn Operator>`, `Schema`, and all state fields are `Send`.
#[derive(Debug)]
pub struct SortAggregate {
    child: Box<dyn Operator>,
    group_key_evals: Vec<Eval>,
    aggregates: Vec<LogicalAggregateExpr>,
    schema: Schema,
    child_schema: Schema,
    output: Option<std::vec::IntoIter<Vec<Value>>>,
    eof: bool,
}

impl SortAggregate {
    /// Construct a sort aggregate operator.
    ///
    /// - `child` — pre-sorted input.
    /// - `group_keys` — GROUP BY expressions; empty means whole-relation aggregate.
    /// - `aggregates` — aggregate function descriptors.
    /// - `schema` — output schema: group-key columns then aggregate columns.
    #[must_use]
    pub fn new(
        child: Box<dyn Operator>,
        group_keys: Vec<ScalarExpr>,
        aggregates: Vec<LogicalAggregateExpr>,
        schema: Schema,
    ) -> Self {
        let child_schema = child.schema().clone();
        let group_key_evals = group_keys.into_iter().map(Eval::new).collect();
        Self {
            child,
            group_key_evals,
            aggregates,
            schema,
            child_schema,
            output: None,
            eof: false,
        }
    }
}

impl Operator for SortAggregate {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.eof {
            return Ok(None);
        }
        if self.output.is_none() {
            let rows = self.build()?;
            self.output = Some(rows.into_iter());
        }
        let iter = self.output.as_mut().ok_or(ExecError::Internal(
            "sort aggregate output iterator missing",
        ))?;
        let chunk: Vec<Vec<Value>> = iter.by_ref().take(BATCH_TARGET_ROWS).collect();
        if chunk.is_empty() {
            self.eof = true;
            return Ok(None);
        }
        build_batch(&chunk, &self.schema).map(Some)
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn profile_children(&self) -> Vec<&dyn Operator> {
        vec![self.child.as_ref()]
    }
}

impl SortAggregate {
    fn build(&mut self) -> Result<Vec<Vec<Value>>, ExecError> {
        let mut output: Vec<Vec<Value>> = Vec::new();
        let has_group_keys = !self.group_key_evals.is_empty();
        let mut saw_any_row = false;

        // Current group state.
        let mut current_key: Option<Vec<Value>> = None;
        let mut current_states: Vec<AggState> = Vec::new();

        loop {
            let Some(batch) = self.child.next_batch()? else {
                break;
            };
            let rows = batch_to_rows(&batch, &self.child_schema).map_err(|error| {
                ExecError::TypeMismatch(format!(
                    "SortAggregate child decode failed (rows={}, width={}): {error}",
                    batch.rows(),
                    batch.width()
                ))
            })?;
            for row in &rows {
                saw_any_row = true;
                let key = eval_group_key_values(&self.group_key_evals, row)?;

                let same_group = current_key.as_ref().is_some_and(|ck| keys_equal(ck, &key));

                if !same_group {
                    // Emit previous group if any.
                    if let Some(prev_key) = current_key.take() {
                        let mut out_row = prev_key;
                        for state in &current_states {
                            out_row.push(finalise(state));
                        }
                        output.push(out_row);
                    }
                    current_key = Some(key);
                    current_states = init_states(&self.aggregates);
                }

                for (state, agg) in current_states.iter_mut().zip(self.aggregates.iter()) {
                    accumulate(state, agg, row)?;
                }
            }
        }

        // Emit the last group.
        if let Some(key) = current_key.take() {
            let mut out_row = key;
            for state in &current_states {
                out_row.push(finalise(state));
            }
            output.push(out_row);
        }

        // Empty-input rule: no group keys → identity row.
        if !saw_any_row && !has_group_keys {
            let identity: Vec<Value> = self
                .aggregates
                .iter()
                .map(|agg| finalise(&init_state(agg)))
                .collect();
            return Ok(vec![identity]);
        }

        Ok(output)
    }
}

fn eval_group_key_values(evals: &[Eval], row: &[Value]) -> Result<Vec<Value>, ExecError> {
    evals
        .iter()
        .map(|eval| {
            eval.eval(row)
                .map_err(|err| ExecError::TypeMismatch(err.to_string()))
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Statistical aggregate extensions
// ---------------------------------------------------------------------------

/// Extended aggregate descriptors for STDDEV, VARIANCE, CORR, and
/// percentile functions.
///
/// These are constructed by callers that want the extended statistical
/// aggregates and are mapped onto the internal [`AggState`] variants.
#[allow(dead_code)]
#[derive(Debug)]
pub(crate) enum StatAggFunc {
    /// Sample standard deviation.
    Stddev,
    /// Sample variance.
    Variance,
    /// Pearson correlation coefficient.
    Corr,
    /// Linear-interpolation percentile (fraction ∈ [0, 1]).
    PercentileCont(f64),
    /// Discrete percentile (fraction ∈ [0, 1]).
    PercentileDisc(f64),
}

/// Build an initial [`AggState`] for a statistical aggregate.
///
/// Callers that want STDDEV etc. call this and manage the state directly.
#[allow(dead_code)]
pub(crate) const fn init_stat_state(func: &StatAggFunc) -> AggState {
    match func {
        StatAggFunc::Stddev => AggState::Stddev(0.0, 0.0, 0),
        StatAggFunc::Variance => AggState::Variance(0.0, 0.0, 0),
        StatAggFunc::Corr => AggState::Corr(0.0, 0.0, 0.0, 0.0, 0.0, 0),
        StatAggFunc::PercentileCont(f) => AggState::PercentileCont {
            values: Vec::new(),
            fraction: Some(*f),
            asc: true,
        },
        StatAggFunc::PercentileDisc(f) => AggState::PercentileDisc {
            values: Vec::new(),
            fraction: Some(*f),
            asc: true,
            nulls_first: false,
        },
    }
}

/// Accumulate a single f64 value into a statistical aggregate state.
#[allow(dead_code)]
pub(crate) fn accumulate_stat(state: &mut AggState, x: f64) {
    match state {
        AggState::Variance(sx, sx2, cnt) | AggState::Stddev(sx, sx2, cnt) => {
            *sx += x;
            *sx2 += x * x;
            *cnt = cnt.saturating_add(1);
        }
        AggState::PercentileCont { values, .. } => {
            values.push(x);
        }
        AggState::PercentileDisc { values, .. } => {
            values.push(Value::Float64(x));
        }
        _ => {}
    }
}

/// Finalise a statistical aggregate state to a [`Value`].
#[allow(dead_code)]
pub(crate) fn finalise_stat(state: &AggState) -> Value {
    finalise(state)
}

#[cfg(test)]
mod tests {
    use ultrasql_core::{DataType, Field, Schema, Value};
    use ultrasql_planner::SortKey;
    use ultrasql_planner::{AggregateFunc, BinaryOp, LogicalAggregateExpr, ScalarExpr};
    use ultrasql_vec::Batch;
    use ultrasql_vec::column::{BoolColumn, Column, NumericColumn, StringColumn};

    use super::{SortAggregate, StatAggFunc, accumulate_stat, finalise_stat, init_stat_state};
    use crate::Operator;
    use crate::filter_op::batch_to_rows;
    use crate::mem_table_scan::MemTableScan;

    fn schema_group_val() -> Schema {
        Schema::new([
            Field::required("group", DataType::Int32),
            Field::required("val", DataType::Int64),
        ])
        .expect("ok")
    }

    fn make_batch(rows: &[(i32, i64)]) -> Batch {
        Batch::new([
            Column::Int32(NumericColumn::from_data(
                rows.iter().map(|(a, _)| *a).collect(),
            )),
            Column::Int64(NumericColumn::from_data(
                rows.iter().map(|(_, b)| *b).collect(),
            )),
        ])
        .expect("ok")
    }

    fn drain_all(op: &mut dyn Operator) -> Vec<Vec<Value>> {
        let schema = op.schema().clone();
        let mut out = Vec::new();
        while let Some(b) = op.next_batch().expect("ok") {
            let rows = batch_to_rows(&b, &schema).expect("decode");
            out.extend(rows);
        }
        out
    }

    fn count_star_agg() -> LogicalAggregateExpr {
        LogicalAggregateExpr {
            func: AggregateFunc::CountStar,
            arg: None,
            direct_arg: None,
            order_by: None,
            distinct: false,
            output_name: "cnt".into(),
            data_type: DataType::Int64,
        }
    }

    fn col_group() -> ScalarExpr {
        ScalarExpr::Column {
            name: "group".into(),
            index: 0,
            data_type: DataType::Int32,
        }
    }

    fn col(name: &str, index: usize, data_type: DataType) -> ScalarExpr {
        ScalarExpr::Column {
            name: name.to_owned(),
            index,
            data_type,
        }
    }

    fn lit_i32(v: i32) -> ScalarExpr {
        ScalarExpr::Literal {
            value: Value::Int32(v),
            data_type: DataType::Int32,
        }
    }

    fn lit(value: Value, data_type: DataType) -> ScalarExpr {
        ScalarExpr::Literal { value, data_type }
    }

    fn divide_i32_by_zero(name: &str, index: usize) -> ScalarExpr {
        ScalarExpr::Binary {
            op: BinaryOp::Div,
            left: Box::new(col(name, index, DataType::Int32)),
            right: Box::new(lit_i32(0)),
            data_type: DataType::Int32,
        }
    }

    fn divide_i64_by_zero(name: &str, index: usize) -> ScalarExpr {
        ScalarExpr::Binary {
            op: BinaryOp::Div,
            left: Box::new(col(name, index, DataType::Int64)),
            right: Box::new(ScalarExpr::Literal {
                value: Value::Int64(0),
                data_type: DataType::Int64,
            }),
            data_type: DataType::Int64,
        }
    }

    fn agg(
        func: AggregateFunc,
        arg: Option<ScalarExpr>,
        output_name: &str,
        data_type: DataType,
    ) -> LogicalAggregateExpr {
        LogicalAggregateExpr {
            func,
            arg,
            direct_arg: None,
            order_by: None,
            distinct: false,
            output_name: output_name.to_owned(),
            data_type,
        }
    }

    fn percentile_agg(
        func: AggregateFunc,
        fraction: Value,
        order_expr: ScalarExpr,
        output_name: &str,
    ) -> LogicalAggregateExpr {
        LogicalAggregateExpr {
            func,
            arg: Some(order_expr.clone()),
            direct_arg: Some(lit(fraction, DataType::Float64)),
            order_by: Some(SortKey {
                expr: order_expr,
                asc: true,
                nulls_first: false,
            }),
            distinct: false,
            output_name: output_name.to_owned(),
            data_type: DataType::Float64,
        }
    }

    #[test]
    fn sort_agg_count_star_with_groups() {
        // Input sorted by group: (1,10), (1,20), (2,30)
        let scan = MemTableScan::new(
            schema_group_val(),
            vec![make_batch(&[(1, 10), (1, 20), (2, 30)])],
        );
        let out_schema = Schema::new([
            Field::required("group", DataType::Int32),
            Field::required("cnt", DataType::Int64),
        ])
        .expect("ok");
        let mut op = SortAggregate::new(
            Box::new(scan),
            vec![col_group()],
            vec![count_star_agg()],
            out_schema,
        );
        let mut rows = drain_all(&mut op);
        rows.sort_by_key(|r| match &r[0] {
            Value::Int32(v) => *v,
            _ => i32::MAX,
        });
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][1], Value::Int64(2)); // group 1 has 2 rows
        assert_eq!(rows[1][1], Value::Int64(1)); // group 2 has 1 row
    }

    #[test]
    fn sort_agg_empty_input_no_group_emits_identity() {
        let scan = MemTableScan::new(schema_group_val(), vec![]);
        let out_schema = Schema::new([Field::required("cnt", DataType::Int64)]).expect("ok");
        let mut op = SortAggregate::new(Box::new(scan), vec![], vec![count_star_agg()], out_schema);
        let rows = drain_all(&mut op);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::Int64(0));
    }

    #[test]
    fn sort_agg_empty_input_with_group_emits_nothing() {
        let scan = MemTableScan::new(schema_group_val(), vec![]);
        let out_schema = Schema::new([
            Field::required("group", DataType::Int32),
            Field::required("cnt", DataType::Int64),
        ])
        .expect("ok");
        let mut op = SortAggregate::new(
            Box::new(scan),
            vec![col_group()],
            vec![count_star_agg()],
            out_schema,
        );
        let rows = drain_all(&mut op);
        assert!(rows.is_empty());
    }

    #[test]
    fn sort_agg_group_key_eval_error_propagates() {
        let scan = MemTableScan::new(schema_group_val(), vec![make_batch(&[(1, 10), (2, 20)])]);
        let out_schema = Schema::new([
            Field::required("group", DataType::Int32),
            Field::required("cnt", DataType::Int64),
        ])
        .expect("schema");
        let mut op = SortAggregate::new(
            Box::new(scan),
            vec![divide_i32_by_zero("group", 0)],
            vec![count_star_agg()],
            out_schema,
        );

        let err = op.next_batch().expect_err("group key division must error");
        assert!(
            err.to_string().contains("division by zero"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn sort_agg_sum_single_int32_row_widens_to_int64() {
        let schema = Schema::new([Field::required("v", DataType::Int32)]).expect("schema ok");
        let scan = MemTableScan::new(
            schema,
            vec![Batch::new([Column::Int32(NumericColumn::from_data(vec![7]))]).expect("batch ok")],
        );
        let out_schema =
            Schema::new([Field::required("total", DataType::Int64)]).expect("schema ok");
        let sum_expr = agg(
            AggregateFunc::Sum,
            Some(col("v", 0, DataType::Int32)),
            "total",
            DataType::Int64,
        );

        let mut op = SortAggregate::new(Box::new(scan), vec![], vec![sum_expr], out_schema);
        let rows = drain_all(&mut op);

        assert_eq!(rows, vec![vec![Value::Int64(7)]]);
    }

    #[test]
    fn sort_agg_group_keys_match_decimal_values_across_scales() {
        assert!(super::keys_equal(
            &[Value::Decimal {
                value: 10,
                scale: 1,
            }],
            &[Value::Decimal { value: 1, scale: 0 }]
        ));
    }

    #[test]
    fn sort_agg_arg_eval_error_propagates() {
        let scan = MemTableScan::new(schema_group_val(), vec![make_batch(&[(1, 10)])]);
        let out_schema = Schema::new([Field::required("total", DataType::Int64)]).expect("schema");
        let mut op = SortAggregate::new(
            Box::new(scan),
            vec![],
            vec![agg(
                AggregateFunc::Sum,
                Some(divide_i64_by_zero("val", 1)),
                "total",
                DataType::Int64,
            )],
            out_schema,
        );

        let err = op
            .next_batch()
            .expect_err("aggregate arg division must error");
        assert!(
            err.to_string().contains("division by zero"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn sort_agg_percentile_fraction_eval_error_propagates() {
        let scan = MemTableScan::new(schema_group_val(), vec![make_batch(&[(1, 10)])]);
        let order_expr = col("val", 1, DataType::Int64);
        let out_schema = Schema::new([
            Field::required("group", DataType::Int32),
            Field::required("p", DataType::Float64),
        ])
        .expect("schema");
        let aggs = vec![LogicalAggregateExpr {
            func: AggregateFunc::PercentileCont,
            arg: Some(order_expr.clone()),
            direct_arg: Some(divide_i32_by_zero("group", 0)),
            order_by: Some(SortKey {
                expr: order_expr,
                asc: true,
                nulls_first: false,
            }),
            distinct: false,
            output_name: "p".to_owned(),
            data_type: DataType::Float64,
        }];
        let mut op = SortAggregate::new(Box::new(scan), vec![col_group()], aggs, out_schema);

        let err = op
            .next_batch()
            .expect_err("percentile fraction division must error");
        assert!(
            err.to_string().contains("division by zero"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn sort_agg_accumulates_scalar_collection_and_json_aggregates() {
        let input_schema = Schema::new([
            Field::required("group", DataType::Int32),
            Field::required("val", DataType::Int64),
            Field::required("flag", DataType::Bool),
            Field::required("label", DataType::Text { max_len: None }),
        ])
        .expect("schema");
        let batch = Batch::new([
            Column::Int32(NumericColumn::from_data(vec![1, 1, 2])),
            Column::Int64(NumericColumn::from_data(vec![10, 20, 5])),
            Column::Bool(BoolColumn::from_data(vec![true, false, true])),
            Column::Utf8(StringColumn::from_data(["a", "b", "c"].map(str::to_owned))),
        ])
        .expect("batch");
        let scan = MemTableScan::new(input_schema, vec![batch]);
        let out_schema = Schema::new([
            Field::required("group", DataType::Int32),
            Field::required("sum", DataType::Int64),
            Field::required("avg", DataType::Float64),
            Field::required("min", DataType::Text { max_len: None }),
            Field::required("max", DataType::Int64),
            Field::required("all", DataType::Bool),
            Field::required("any", DataType::Bool),
            Field::required("str", DataType::Text { max_len: None }),
            Field::required("arr", DataType::Array(Box::new(DataType::Int64))),
            Field::required("json", DataType::Jsonb),
        ])
        .expect("schema");
        let aggs = vec![
            agg(
                AggregateFunc::Sum,
                Some(col("val", 1, DataType::Int64)),
                "sum",
                DataType::Int64,
            ),
            agg(
                AggregateFunc::Avg,
                Some(col("val", 1, DataType::Int64)),
                "avg",
                DataType::Float64,
            ),
            agg(
                AggregateFunc::Min,
                Some(col("label", 3, DataType::Text { max_len: None })),
                "min",
                DataType::Text { max_len: None },
            ),
            agg(
                AggregateFunc::Max,
                Some(col("val", 1, DataType::Int64)),
                "max",
                DataType::Int64,
            ),
            agg(
                AggregateFunc::BoolAnd,
                Some(col("flag", 2, DataType::Bool)),
                "all",
                DataType::Bool,
            ),
            agg(
                AggregateFunc::BoolOr,
                Some(col("flag", 2, DataType::Bool)),
                "any",
                DataType::Bool,
            ),
            agg(
                AggregateFunc::StringAgg,
                Some(col("label", 3, DataType::Text { max_len: None })),
                "str",
                DataType::Text { max_len: None },
            ),
            agg(
                AggregateFunc::ArrayAgg,
                Some(col("val", 1, DataType::Int64)),
                "arr",
                DataType::Array(Box::new(DataType::Int64)),
            ),
            agg(
                AggregateFunc::JsonAgg,
                Some(col("label", 3, DataType::Text { max_len: None })),
                "json",
                DataType::Jsonb,
            ),
        ];
        let mut op = SortAggregate::new(Box::new(scan), vec![col_group()], aggs, out_schema);
        let rows = drain_all(&mut op);

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][1], Value::Int64(30));
        assert_eq!(rows[0][2], Value::Float64(15.0));
        assert_eq!(rows[0][3], Value::Text("a".to_owned()));
        assert_eq!(rows[0][4], Value::Int64(20));
        assert_eq!(rows[0][5], Value::Bool(false));
        assert_eq!(rows[0][6], Value::Bool(true));
        assert_eq!(rows[0][7], Value::Text("ab".to_owned()));
        assert_eq!(
            rows[0][8],
            Value::Array {
                element_type: DataType::Int64,
                elements: vec![Value::Int64(10), Value::Int64(20)],
            }
        );
        assert_eq!(rows[0][9], Value::Jsonb(r#"["a","b"]"#.to_owned()));
    }

    #[test]
    fn sort_agg_avg_vector_skips_nulls_and_returns_dense_vector() {
        let vector_type = DataType::Vector { dims: Some(3) };
        let input_schema = Schema::new([
            Field::required("group", DataType::Int32),
            Field::nullable("embedding", vector_type.clone()),
        ])
        .expect("schema");
        let mut valid = ultrasql_vec::bitmap::Bitmap::new(3, true);
        valid.set(2, false);
        let batch = Batch::new([
            Column::Int32(NumericColumn::from_data(vec![1, 1, 1])),
            Column::Utf8(
                StringColumn::with_nulls(
                    vec![
                        "[1,2,3]".to_owned(),
                        "[3,4,5]".to_owned(),
                        "[99,99,99]".to_owned(),
                    ],
                    valid,
                )
                .expect("string column ok"),
            ),
        ])
        .expect("batch");
        let scan = MemTableScan::new(input_schema, vec![batch]);
        let out_schema = Schema::new([
            Field::required("group", DataType::Int32),
            Field::nullable("avg_embedding", vector_type.clone()),
        ])
        .expect("schema");
        let aggs = vec![agg(
            AggregateFunc::Avg,
            Some(col("embedding", 1, vector_type.clone())),
            "avg_embedding",
            vector_type,
        )];
        let mut op = SortAggregate::new(Box::new(scan), vec![col_group()], aggs, out_schema);
        let rows = drain_all(&mut op);

        assert_eq!(
            rows,
            vec![vec![Value::Int32(1), Value::Vector(vec![2.0, 3.0, 4.0])]]
        );
    }

    #[test]
    fn sort_agg_percentiles_reject_bad_fraction_and_pick_disc_value() {
        let scan = MemTableScan::new(
            schema_group_val(),
            vec![make_batch(&[(1, 10), (1, 20), (1, 30), (1, 40)])],
        );
        let out_schema = Schema::new([
            Field::required("group", DataType::Int32),
            Field::required("pdisc", DataType::Int64),
        ])
        .expect("schema");
        let aggs = vec![percentile_agg(
            AggregateFunc::PercentileDisc,
            Value::Float64(0.5),
            col("val", 1, DataType::Int64),
            "pdisc",
        )];
        let mut op = SortAggregate::new(Box::new(scan), vec![col_group()], aggs, out_schema);
        let rows = drain_all(&mut op);
        assert_eq!(rows[0][1], Value::Int64(20));

        let bad_scan = MemTableScan::new(schema_group_val(), vec![make_batch(&[(1, 10)])]);
        let bad_schema = Schema::new([
            Field::required("group", DataType::Int32),
            Field::required("pcont", DataType::Float64),
        ])
        .expect("schema");
        let bad_aggs = vec![percentile_agg(
            AggregateFunc::PercentileCont,
            Value::Float64(1.5),
            col("val", 1, DataType::Int64),
            "pcont",
        )];
        let mut bad =
            SortAggregate::new(Box::new(bad_scan), vec![col_group()], bad_aggs, bad_schema);
        let err = bad.next_batch().expect_err("bad fraction");
        assert!(err.to_string().contains("between 0 and 1"));
    }

    // Statistical aggregate unit tests.

    #[test]
    fn stat_variance_basic() {
        // variance of [2, 4, 4, 4, 5, 5, 7, 9] = 4.571...
        let mut state = init_stat_state(&StatAggFunc::Variance);
        for &x in &[2.0_f64, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0] {
            accumulate_stat(&mut state, x);
        }
        let result = finalise_stat(&state);
        if let Value::Float64(v) = result {
            assert!((v - 4.571_428_571_428_571).abs() < 1e-9, "variance was {v}");
        } else {
            panic!("expected Float64, got {result:?}");
        }
    }

    #[test]
    fn stat_percentile_cont_median() {
        let mut state = init_stat_state(&StatAggFunc::PercentileCont(0.5));
        for &x in &[1.0_f64, 2.0, 3.0, 4.0, 5.0] {
            accumulate_stat(&mut state, x);
        }
        // median of [1,2,3,4,5] = 3.0
        let result = finalise_stat(&state);
        assert_eq!(result, Value::Float64(3.0));
    }

    #[test]
    fn stat_helpers_cover_stddev_corr_and_percentile_disc() {
        let mut stddev = init_stat_state(&StatAggFunc::Stddev);
        for &x in &[2.0_f64, 4.0, 4.0, 4.0] {
            accumulate_stat(&mut stddev, x);
        }
        let Value::Float64(value) = finalise_stat(&stddev) else {
            panic!("expected stddev");
        };
        assert!((value - 1.0).abs() < 1e-9);

        let corr = init_stat_state(&StatAggFunc::Corr);
        assert_eq!(finalise_stat(&corr), Value::Null);

        let mut disc = init_stat_state(&StatAggFunc::PercentileDisc(0.75));
        for &x in &[1.0_f64, 3.0, 2.0, 4.0] {
            accumulate_stat(&mut disc, x);
        }
        assert_eq!(finalise_stat(&disc), Value::Float64(3.0));
    }
}
