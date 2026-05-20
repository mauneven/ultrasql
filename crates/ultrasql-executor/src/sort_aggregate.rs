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

use ultrasql_core::{DataType, Schema, Value};
use ultrasql_planner::{AggregateFunc, LogicalAggregateExpr, ScalarExpr};
use ultrasql_vec::Batch;

use crate::eval::Eval;
use crate::filter_op::batch_to_rows;
use crate::seq_scan::build_batch;
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
    /// Running sum and count for STDDEV / VARIANCE.
    /// Fields: (`sum_x`, `sum_x2`, count).
    Variance(f64, f64, i64),
    /// Same accumulator for STDDEV (standard deviation).
    Stddev(f64, f64, i64),
    /// For CORR(y, x): (`sum_x`, `sum_y`, `sum_xy`, `sum_x2`, `sum_y2`, count).
    Corr(f64, f64, f64, f64, f64, i64),
    /// `PERCENTILE_CONT`: accumulates all values for linear interpolation.
    PercentileCont(Vec<f64>, f64 /* fraction */),
    /// `PERCENTILE_DISC`: accumulates all values for discrete selection.
    PercentileDisc(Vec<f64>, f64 /* fraction */),
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
        AggregateFunc::StddevSamp | AggregateFunc::StddevPop => AggState::Stddev(0.0, 0.0, 0),
        AggregateFunc::VarSamp | AggregateFunc::VarPop => AggState::Variance(0.0, 0.0, 0),
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
        .map(|expr| Eval::new(expr.clone()).eval(row).unwrap_or(Value::Null));

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
                        None => v,
                        Some(e) => add_values(e, v)?,
                    });
                }
            }
        }
        AggState::Avg(sum, cnt) => {
            if let Some(v) = arg {
                if !v.is_null() {
                    *sum = Some(match sum.take() {
                        None => v,
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
                        Value::Text(s) => parts.push(s),
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
            // arg for CORR is a pair (y_expr, x_expr); we encode as two
            // positional columns — for now treat arg as the single value
            // and leave the second as a TODO.
            if let Some(v) = arg {
                if let Some(x) = to_f64(&v) {
                    *sx += x;
                    *sy += x;
                    *sxy += x * x;
                    *sx2 += x * x;
                    *sy2 += x * x;
                    *cnt = cnt.saturating_add(1);
                }
            }
        }
        AggState::PercentileCont(vals, _) | AggState::PercentileDisc(vals, _) => {
            if let Some(v) = arg {
                if let Some(x) = to_f64(&v) {
                    vals.push(x);
                }
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
        AggState::PercentileCont(vals, frac) => {
            if vals.is_empty() {
                return Value::Null;
            }
            let mut sorted = vals.clone();
            sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            let n = sorted.len();
            // Cast sample count to f64 for the percentile arithmetic.
            // The aggregate state is `Vec<f64>`; sample counts above
            // `2^53` are not representable so we accept the precision
            // loss — the percentile is a statistic, not an index.
            #[allow(
                clippy::cast_precision_loss,
                reason = "percentile arithmetic; sample count rarely above 2^53"
            )]
            let n_f64 = n as f64;
            let row_number = frac * (n_f64 - 1.0);
            // Truncate to usize index; row_number is non-negative and
            // bounded by n - 1 above, so the cast cannot wrap.
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
                Value::Float64(sorted[lo])
            } else {
                #[allow(
                    clippy::cast_precision_loss,
                    reason = "lo is < 2^53 in practice; percentile interpolation"
                )]
                let frac_part = row_number - lo as f64;
                Value::Float64(sorted[hi].mul_add(frac_part, sorted[lo] * (1.0 - frac_part)))
            }
        }
        AggState::PercentileDisc(vals, frac) => {
            if vals.is_empty() {
                return Value::Null;
            }
            let mut sorted = vals.clone();
            sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            #[allow(
                clippy::cast_precision_loss,
                clippy::cast_possible_truncation,
                clippy::cast_sign_loss,
                reason = "percentile_disc arithmetic; idx bounded by ceil(frac * len) ≤ len"
            )]
            let idx = (frac * sorted.len() as f64).ceil() as usize;
            let idx = idx.saturating_sub(1).min(sorted.len() - 1);
            Value::Float64(sorted[idx])
        }
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
        let iter = self.output.as_mut().expect("just-set");
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
                let key: Vec<Value> = self
                    .group_key_evals
                    .iter()
                    .map(|ev| ev.eval(row).unwrap_or(Value::Null))
                    .collect();

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
        StatAggFunc::PercentileCont(f) => AggState::PercentileCont(Vec::new(), *f),
        StatAggFunc::PercentileDisc(f) => AggState::PercentileDisc(Vec::new(), *f),
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
        AggState::PercentileCont(vals, _) | AggState::PercentileDisc(vals, _) => {
            vals.push(x);
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
    use ultrasql_planner::{AggregateFunc, LogicalAggregateExpr, ScalarExpr};
    use ultrasql_vec::Batch;
    use ultrasql_vec::column::{Column, NumericColumn};

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
}
