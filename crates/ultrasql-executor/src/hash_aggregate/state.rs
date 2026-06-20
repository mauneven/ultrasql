//! Aggregate state machine.
//!
//! [`AggState`] is the per-group accumulator for a single aggregate
//! function instance. This module owns its construction
//! ([`init_states`]), the per-row fold ([`accumulate`]), and the
//! finalisation into a result [`Value`] ([`finalise`]), including the
//! ordered-set percentile and JSON-aggregate paths.

use std::collections::HashSet;

use serde_json::{Number as JsonNumber, Value as JsonValue};
use ultrasql_core::{DataType, Value};
use ultrasql_planner::{AggregateFunc, LogicalAggregateExpr};

use crate::ExecError;
use crate::aggregate_math::{
    percentile_cont_indexes, percentile_disc_index, usize_to_f64, widen_sum_seed,
};
use crate::eval::Eval;
use crate::eval_error_to_exec_error;
use crate::hash_aggregate::arith::{
    add_values, decimal_to_f64, divide_value, i64_to_f64_saturating, increment_count, value_lt,
};
use crate::hash_aggregate::key::KeyValue;

/// Per-row accumulator for a single aggregate function instance.
#[derive(Debug)]
pub(crate) enum AggState {
    /// DISTINCT wrapper: filters duplicate non-NULL aggregate inputs before
    /// forwarding them into the wrapped aggregate state.
    Distinct {
        inner: Box<AggState>,
        seen: HashSet<KeyValue>,
    },
    /// `COUNT(*)` — counts all rows regardless of NULLs.
    CountStar(i64),
    /// `COUNT(expr)` — counts non-NULL values.
    Count(i64),
    /// `SUM(expr)` — running sum, `None` if all values were NULL.
    Sum(Option<Value>),
    /// `AVG(expr)` — (running sum, count of non-NULLs).
    Avg(Option<Value>, i64),
    /// `MIN(expr)` — current minimum, `None` if no non-NULL seen yet.
    Min(Option<Value>),
    /// `MAX(expr)` — current maximum, `None` if no non-NULL seen yet.
    Max(Option<Value>),
    /// `BOOL_AND(expr)` — `None` until a non-NULL is seen.
    BoolAnd(Option<bool>),
    /// `BOOL_OR(expr)` — `None` until a non-NULL is seen.
    BoolOr(Option<bool>),
    /// `STRING_AGG(expr, sep)` — accumulated (values, separator).
    StringAgg(Vec<String>, String),
    /// `ARRAY_AGG(expr)` — accumulated non-NULL values.
    ArrayAgg(Vec<Value>),
    /// `JSON_AGG(expr)` — accumulated values, preserving SQL NULL.
    JsonAgg(Vec<Value>),
    /// `CORR(y, x)` — sums needed for Pearson correlation.
    Corr {
        count: i64,
        sum_x: f64,
        sum_y: f64,
        sum_xy: f64,
        sum_x2: f64,
        sum_y2: f64,
    },
    /// Welford running aggregate for STDDEV / VARIANCE: `(count,
    /// mean, M2)` where `M2` is the running sum of squared
    /// differences from the mean. Shared between `STDDEV_SAMP`,
    /// `STDDEV_POP`, `VAR_SAMP`, `VAR_POP`; the variant carries
    /// the requested final shape so `finalise` knows whether to
    /// divide by `n` or `n - 1` and whether to take the square
    /// root.
    Welford {
        count: i64,
        mean: f64,
        m2: f64,
        sample: bool,
        sqrt: bool,
    },
    /// `PERCENTILE_CONT`: collect numeric samples for final interpolation.
    PercentileCont {
        values: Vec<f64>,
        fraction: Option<f64>,
        asc: bool,
    },
    /// `PERCENTILE_DISC`: collect ordered-set samples and return one input value.
    PercentileDisc {
        values: Vec<Value>,
        fraction: Option<f64>,
        asc: bool,
        nulls_first: bool,
    },
}

/// Initialise one [`AggState`] for the given aggregate descriptor.
pub(crate) fn init_state_for(agg: &LogicalAggregateExpr) -> AggState {
    let base = init_base_state(agg);
    if agg.distinct {
        AggState::Distinct {
            inner: Box::new(base),
            seen: HashSet::new(),
        }
    } else {
        base
    }
}

/// Build the un-wrapped accumulator state for `agg`, reading any
/// per-aggregate configuration (the `STRING_AGG` delimiter, percentile
/// sort direction) off the descriptor. The `DISTINCT` wrapper, when
/// present, boxes the result of this function.
fn init_base_state(agg: &LogicalAggregateExpr) -> AggState {
    match agg.func {
        AggregateFunc::StringAgg => AggState::StringAgg(Vec::new(), string_agg_separator(agg)),
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
        _ => init_state_for_func(agg.func),
    }
}

/// Resolve the constant delimiter for `STRING_AGG(value, delimiter)`.
///
/// The binder stores the bound delimiter in `direct_arg`; it is a
/// constant text expression, so we evaluate it once against an empty row
/// when initialising the group. A NULL or absent delimiter (and any
/// non-text result) collapses to the empty separator, matching
/// PostgreSQL's treatment of a NULL delimiter as no separator.
fn string_agg_separator(agg: &LogicalAggregateExpr) -> String {
    let Some(expr) = agg.direct_arg.as_ref() else {
        return String::new();
    };
    match Eval::new(expr.clone()).eval(&[]) {
        Ok(Value::Text(s) | Value::Char(s)) => s,
        _ => String::new(),
    }
}

const fn init_state_for_func(func: AggregateFunc) -> AggState {
    match func {
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
        AggregateFunc::Corr => AggState::Corr {
            count: 0,
            sum_x: 0.0,
            sum_y: 0.0,
            sum_xy: 0.0,
            sum_x2: 0.0,
            sum_y2: 0.0,
        },
        AggregateFunc::StddevSamp => AggState::Welford {
            count: 0,
            mean: 0.0,
            m2: 0.0,
            sample: true,
            sqrt: true,
        },
        AggregateFunc::StddevPop => AggState::Welford {
            count: 0,
            mean: 0.0,
            m2: 0.0,
            sample: false,
            sqrt: true,
        },
        AggregateFunc::VarSamp => AggState::Welford {
            count: 0,
            mean: 0.0,
            m2: 0.0,
            sample: true,
            sqrt: false,
        },
        AggregateFunc::VarPop => AggState::Welford {
            count: 0,
            mean: 0.0,
            m2: 0.0,
            sample: false,
            sqrt: false,
        },
        AggregateFunc::PercentileCont => AggState::PercentileCont {
            values: Vec::new(),
            fraction: None,
            asc: true,
        },
        AggregateFunc::PercentileDisc => AggState::PercentileDisc {
            values: Vec::new(),
            fraction: None,
            asc: true,
            nulls_first: false,
        },
    }
}

/// Initialise all aggregate states for a new group.
pub(crate) fn init_states(aggregates: &[LogicalAggregateExpr]) -> Vec<AggState> {
    aggregates.iter().map(init_state_for).collect()
}

/// Feed one input `row` into `state` using the aggregate descriptor `agg`.
pub(crate) fn accumulate(
    state: &mut AggState,
    agg: &LogicalAggregateExpr,
    row: &[Value],
) -> Result<(), ExecError> {
    match state {
        AggState::PercentileCont {
            values, fraction, ..
        } => return accumulate_percentile_cont(values, fraction, agg, row),
        AggState::PercentileDisc {
            values, fraction, ..
        } => return accumulate_percentile_disc(values, fraction, agg, row),
        _ => {}
    }

    // Evaluate the argument expression (if any).
    let arg_val: Option<Value> = agg
        .arg
        .as_ref()
        .map(|expr| {
            Eval::new(expr.clone())
                .eval(row)
                .map_err(eval_error_to_exec_error)
        })
        .transpose()?;

    if let AggState::Distinct { inner, seen } = state {
        let Some(v) = arg_val else {
            return Ok(());
        };
        if v.is_null() || !seen.insert(KeyValue(v.clone())) {
            return Ok(());
        }
        return accumulate_value(inner, Some(v));
    }

    accumulate_value(state, arg_val)
}

pub(crate) fn accumulate_value(state: &mut AggState, arg_val: Option<Value>) -> Result<(), ExecError> {
    match state {
        AggState::Distinct { .. } => unreachable!("distinct wrapper handled before dispatch"),
        AggState::CountStar(n) => {
            increment_count(n, 1)?;
        }
        AggState::Count(n) => {
            if !matches!(arg_val, Some(Value::Null) | None) {
                increment_count(n, 1)?;
            }
        }
        AggState::Sum(acc) => {
            if let Some(v) = arg_val {
                if !v.is_null() {
                    *acc = Some(match acc.take() {
                        None => widen_sum_seed(v),
                        Some(existing) => add_values(existing, v)?,
                    });
                }
            }
        }
        AggState::Avg(sum, cnt) => {
            if let Some(v) = arg_val {
                if !v.is_null() {
                    *sum = Some(match sum.take() {
                        None => widen_sum_seed(v),
                        Some(existing) => add_values(existing, v)?,
                    });
                    increment_count(cnt, 1)?;
                }
            }
        }
        AggState::Min(current) => {
            if let Some(v) = arg_val {
                if !v.is_null() {
                    *current = Some(match current.take() {
                        None => v,
                        Some(existing) => {
                            if value_lt(&v, &existing) {
                                v
                            } else {
                                existing
                            }
                        }
                    });
                }
            }
        }
        AggState::Max(current) => {
            if let Some(v) = arg_val {
                if !v.is_null() {
                    *current = Some(match current.take() {
                        None => v,
                        Some(existing) => {
                            if value_lt(&existing, &v) {
                                v
                            } else {
                                existing
                            }
                        }
                    });
                }
            }
        }
        AggState::BoolAnd(acc) => {
            if let Some(Value::Bool(b)) = arg_val {
                *acc = Some(acc.unwrap_or(true) && b);
            }
        }
        AggState::BoolOr(acc) => {
            if let Some(Value::Bool(b)) = arg_val {
                *acc = Some(acc.unwrap_or(false) || b);
            }
        }
        AggState::StringAgg(parts, _sep) => {
            if let Some(v) = arg_val {
                if !v.is_null() {
                    match v {
                        Value::Text(s) | Value::Char(s) => parts.push(s),
                        other => parts.push(other.to_string()),
                    }
                }
            }
        }
        AggState::ArrayAgg(items) => {
            if let Some(v) = arg_val {
                if !v.is_null() {
                    items.push(v);
                }
            }
        }
        AggState::JsonAgg(items) => {
            if let Some(v) = arg_val {
                items.push(v);
            }
        }
        AggState::Corr {
            count,
            sum_x,
            sum_y,
            sum_xy,
            sum_x2,
            sum_y2,
        } => {
            if let Some(Value::Record(fields)) = arg_val
                && fields.len() >= 2
                && let (Some(y), Some(x)) = (value_as_f64(&fields[0].1), value_as_f64(&fields[1].1))
            {
                increment_count(count, 1)?;
                *sum_x += x;
                *sum_y += y;
                *sum_xy += x * y;
                *sum_x2 += x * x;
                *sum_y2 += y * y;
            }
        }
        AggState::Welford {
            count, mean, m2, ..
        } => {
            if let Some(v) = arg_val {
                if let Some(x) = value_as_f64(&v) {
                    // Welford's online algorithm. Numerically stable
                    // even when `count` is large; avoids the
                    // catastrophic cancellation of the naive
                    // sum-of-squares minus square-of-sum recipe.
                    increment_count(count, 1)?;
                    let delta = x - *mean;
                    *mean += delta / i64_to_f64_saturating(*count);
                    let delta2 = x - *mean;
                    *m2 += delta * delta2;
                }
            }
        }
        AggState::PercentileCont { .. } | AggState::PercentileDisc { .. } => {
            unreachable!("percentile states handled before dispatch");
        }
    }
    Ok(())
}

fn accumulate_percentile_cont(
    values: &mut Vec<f64>,
    fraction: &mut Option<f64>,
    agg: &LogicalAggregateExpr,
    row: &[Value],
) -> Result<(), ExecError> {
    update_percentile_fraction(fraction, percentile_fraction(agg, row)?)?;
    let sample = percentile_sample(agg, row)?;
    if sample.is_null() {
        return Ok(());
    }
    let value = value_as_f64(&sample).ok_or_else(|| {
        ExecError::TypeMismatch("percentile_cont requires numeric order values".to_owned())
    })?;
    values.push(value);
    Ok(())
}

fn accumulate_percentile_disc(
    values: &mut Vec<Value>,
    fraction: &mut Option<f64>,
    agg: &LogicalAggregateExpr,
    row: &[Value],
) -> Result<(), ExecError> {
    update_percentile_fraction(fraction, percentile_fraction(agg, row)?)?;
    let sample = percentile_sample(agg, row)?;
    if !sample.is_null() {
        values.push(sample);
    }
    Ok(())
}

fn percentile_fraction(
    agg: &LogicalAggregateExpr,
    row: &[Value],
) -> Result<Option<f64>, ExecError> {
    let direct_arg = agg.direct_arg.as_ref().ok_or_else(|| {
        ExecError::TypeMismatch("ordered-set percentile missing fraction".to_owned())
    })?;
    let value = Eval::new(direct_arg.clone())
        .eval(row)
        .map_err(eval_error_to_exec_error)?;
    if value.is_null() {
        return Ok(None);
    }
    let fraction = value_as_f64(&value)
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

fn percentile_sample(agg: &LogicalAggregateExpr, row: &[Value]) -> Result<Value, ExecError> {
    let sample_expr = agg
        .order_by
        .as_ref()
        .map(|key| &key.expr)
        .or(agg.arg.as_ref())
        .ok_or_else(|| {
            ExecError::TypeMismatch("ordered-set percentile missing order key".to_owned())
        })?;
    Eval::new(sample_expr.clone())
        .eval(row)
        .map_err(eval_error_to_exec_error)
}

/// Coerce a numeric `Value` to `f64` for floating-point folds.
fn value_as_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Int16(x) => Some(f64::from(*x)),
        Value::Int32(x) => Some(f64::from(*x)),
        Value::Int64(x) => Some(i64_to_f64_saturating(*x)),
        Value::Float32(x) => Some(f64::from(*x)),
        Value::Float64(x) => Some(*x),
        Value::Decimal { value, scale } => Some(decimal_to_f64(*value, *scale)),
        _ => None,
    }
}

/// Finalise an [`AggState`] into its result [`Value`].
pub(crate) fn finalise(state: &AggState) -> Result<Value, ExecError> {
    match state {
        AggState::Distinct { inner, .. } => finalise(inner),
        AggState::CountStar(n) | AggState::Count(n) => Ok(Value::Int64(*n)),
        AggState::Sum(acc) | AggState::Min(acc) | AggState::Max(acc) => {
            Ok(acc.clone().unwrap_or(Value::Null))
        }
        AggState::Avg(sum, cnt) => {
            if *cnt == 0 {
                return Ok(Value::Null);
            }
            Ok(sum
                .as_ref()
                .map_or(Value::Null, |s| divide_value(s.clone(), *cnt)))
        }
        AggState::BoolAnd(b) | AggState::BoolOr(b) => Ok(b.map_or(Value::Null, Value::Bool)),
        AggState::StringAgg(parts, sep) => {
            if parts.is_empty() {
                Ok(Value::Null)
            } else {
                Ok(Value::Text(parts.join(sep)))
            }
        }
        AggState::ArrayAgg(items) => {
            if items.is_empty() {
                Ok(Value::Null)
            } else {
                let element_type = items
                    .iter()
                    .find(|v| !v.is_null())
                    .map(Value::data_type)
                    .unwrap_or(DataType::Null);
                Ok(Value::Array {
                    element_type,
                    elements: items.clone(),
                })
            }
        }
        AggState::JsonAgg(items) => {
            if items.is_empty() {
                Ok(Value::Null)
            } else {
                Ok(Value::Jsonb(json_agg_text(items)))
            }
        }
        AggState::Corr {
            count,
            sum_x,
            sum_y,
            sum_xy,
            sum_x2,
            sum_y2,
        } => {
            if *count < 2 {
                return Ok(Value::Null);
            }
            let n = i64_to_f64_saturating(*count);
            let numerator = n.mul_add(*sum_xy, -(*sum_x * *sum_y));
            let x_term = n.mul_add(*sum_x2, -(*sum_x * *sum_x));
            let y_term = n.mul_add(*sum_y2, -(*sum_y * *sum_y));
            let denominator = (x_term * y_term).sqrt();
            if denominator == 0.0 {
                Ok(Value::Null)
            } else {
                Ok(Value::Float64(numerator / denominator))
            }
        }
        AggState::Welford {
            count,
            m2,
            sample,
            sqrt,
            ..
        } => {
            // Sample variance/stddev needs n - 1 in the denominator
            // and is undefined for fewer than two non-NULL inputs.
            // Population variance/stddev is defined for any non-zero
            // count.
            let n = *count;
            let denom = if *sample { n - 1 } else { n };
            if denom <= 0 {
                return Ok(Value::Null);
            }
            let var = m2 / i64_to_f64_saturating(denom);
            Ok(Value::Float64(if *sqrt { var.sqrt() } else { var }))
        }
        AggState::PercentileCont {
            values,
            fraction,
            asc,
        } => Ok(finalise_percentile_cont(values, *fraction, *asc)),
        AggState::PercentileDisc {
            values,
            fraction,
            asc,
            nulls_first,
        } => finalise_percentile_disc(values, *fraction, *asc, *nulls_first),
    }
}

fn finalise_percentile_cont(values: &[f64], fraction: Option<f64>, asc: bool) -> Value {
    let Some(fraction) = fraction else {
        return Value::Null;
    };
    if values.is_empty() {
        return Value::Null;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| crate::sort::compare_f64_sql(*a, *b));
    if !asc {
        sorted.reverse();
    }
    let n = sorted.len();
    let n_f64 = usize_to_f64(n);
    let row_number = fraction * (n_f64 - 1.0);
    let (lo, hi) = percentile_cont_indexes(row_number, n);
    if lo == hi {
        return Value::Float64(sorted[lo]);
    }
    let frac_part = row_number - usize_to_f64(lo);
    Value::Float64(sorted[hi].mul_add(frac_part, sorted[lo] * (1.0 - frac_part)))
}

fn finalise_percentile_disc(
    values: &[Value],
    fraction: Option<f64>,
    asc: bool,
    nulls_first: bool,
) -> Result<Value, ExecError> {
    let Some(fraction) = fraction else {
        return Ok(Value::Null);
    };
    if values.is_empty() {
        return Ok(Value::Null);
    }
    let mut sorted = values.to_vec();
    validate_percentile_disc_values(&sorted, nulls_first)?;
    sorted.sort_by(|a, b| {
        let ord = crate::sort::try_compare_values_nullable(a, b, nulls_first)
            .unwrap_or(std::cmp::Ordering::Equal);
        if asc { ord } else { ord.reverse() }
    });
    let idx = percentile_disc_index(fraction, sorted.len());
    Ok(sorted[idx].clone())
}

fn validate_percentile_disc_values(values: &[Value], nulls_first: bool) -> Result<(), ExecError> {
    let mut first_non_null: Option<&Value> = None;
    for value in values {
        if value.is_null() {
            continue;
        }
        if let Some(first) = first_non_null {
            crate::sort::try_compare_values_nullable(first, value, nulls_first)?;
        } else {
            crate::sort::try_compare_values_nullable(value, value, nulls_first)?;
            first_non_null = Some(value);
        }
    }
    Ok(())
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
