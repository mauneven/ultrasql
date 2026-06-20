//! Vectorised aggregate kernels.
//!
//! [`vectorized_step`] applies one batch's worth of column-oriented folds to
//! the single-group `AggState` vector, dispatching each [`VecAggSlot`] to a
//! kernel that matches the column's runtime variant. The remaining functions
//! are the per-aggregate folds (`SUM`, `COUNT`, `MIN`, `MAX`) that those
//! slots invoke, each producing bit-identical results against the scalar
//! row-at-a-time path.

use ultrasql_core::Value;
use ultrasql_vec::column::{Column, NumericColumn};
use ultrasql_vec::{Batch, count_i64, max_i64, min_i64};

use crate::ExecError;
use crate::hash_aggregate::arith::{checked_sum_i64, increment_count, value_lt};
use crate::hash_aggregate::state::AggState;
use crate::hash_aggregate::vec_plan::VecAggSlot;

/// Apply one vectorised batch step to the single-group `states` vector.
///
/// Each slot dispatches to a kernel that matches the column's runtime
/// variant. Bit-identical results against the scalar path are guaranteed for
/// the supported aggregate set:
/// * `Sum`/`Avg` on integer columns keep an `i64` accumulator and return
///   `NumericFieldOverflow` instead of exposing wrapped totals.
/// * `Sum`/`Avg` on float columns keep an `f64` accumulator, matching the
///   widening that `add_values` performs for Float32/Float64.
/// * `Count(expr)` counts non-null entries via the column's optional bitmap.
/// * `CountStar` increments by `batch.rows()`.
/// * `Min`/`Max` defer to [`min_i64`] / [`max_i64`] for `Int64`, and use
///   tight per-type folds for the remaining numeric widths.
pub(crate) fn vectorized_step(
    plan: &[VecAggSlot],
    batch: &Batch,
    states: &mut [AggState],
) -> Result<(), ExecError> {
    let cols = batch.columns();
    let n = batch.rows();
    for (slot, state) in plan.iter().zip(states.iter_mut()) {
        match (slot, state) {
            (VecAggSlot::CountStar, AggState::CountStar(acc)) => {
                increment_count(acc, checked_count_delta(n, "HashAggregate COUNT(*)")?)?;
            }
            (VecAggSlot::Count(ci), AggState::Count(acc)) => {
                increment_count(acc, column_non_null_count(&cols[*ci])?)?;
            }
            (VecAggSlot::Sum(ci), AggState::Sum(acc)) => {
                accumulate_sum(acc, &cols[*ci])?;
            }
            (VecAggSlot::Avg(ci), AggState::Avg(acc, cnt)) => {
                accumulate_sum(acc, &cols[*ci])?;
                increment_count(cnt, column_non_null_count(&cols[*ci])?)?;
            }
            (VecAggSlot::Min(ci), AggState::Min(acc)) => {
                update_extremum(acc, &cols[*ci], /* take_min = */ true)?;
            }
            (VecAggSlot::Max(ci), AggState::Max(acc)) => {
                update_extremum(acc, &cols[*ci], /* take_min = */ false)?;
            }
            // The plan and the states are zipped in the same order and
            // `build_vectorized_plan` only emits slots that correspond to
            // their state variants, so a mismatch here is a logic bug.
            (slot, state) => {
                return Err(ExecError::TypeMismatch(format!(
                    "vectorized aggregate plan/state mismatch: {slot:?} vs {state:?}"
                )));
            }
        }
    }
    Ok(())
}

/// Count of non-null rows in `col`, as `i64`.
pub(crate) fn column_non_null_count(col: &Column) -> Result<i64, ExecError> {
    let total = col.len();
    let valid = match col {
        Column::Int32(c) => c.nulls().map_or(total, ultrasql_vec::Bitmap::count_ones),
        Column::Int64(c) => count_i64(c),
        Column::Float32(c) => c.nulls().map_or(total, ultrasql_vec::Bitmap::count_ones),
        Column::Float64(c) => c.nulls().map_or(total, ultrasql_vec::Bitmap::count_ones),
        Column::Bool(c) => c.nulls().map_or(total, ultrasql_vec::Bitmap::count_ones),
        Column::Utf8(c) => c.nulls().map_or(total, ultrasql_vec::Bitmap::count_ones),
        Column::DictionaryUtf8(c) => c
            .codes
            .nulls()
            .map_or(total, ultrasql_vec::Bitmap::count_ones),
    };
    checked_count_delta(valid, "HashAggregate COUNT")
}

fn checked_count_delta(delta: usize, context: &str) -> Result<i64, ExecError> {
    i64::try_from(delta).map_err(|_| ExecError::NumericFieldOverflow(format!("{context} overflow")))
}

/// Accumulate `SUM(col)` into the running `Value` accumulator. NULL entries
/// are skipped. The accumulator stays `None` until at least one non-null row
/// has been observed, matching the scalar SUM contract.
pub(crate) fn accumulate_sum(acc: &mut Option<Value>, col: &Column) -> Result<(), ExecError> {
    match col {
        Column::Int64(c) => {
            if c.is_empty() {
                return Ok(());
            }
            let (delta, saw) = sum_i64_nullable(c)?;
            if !saw {
                return Ok(());
            }
            *acc = Some(match acc.take() {
                None => Value::Int64(delta),
                Some(Value::Int64(prev)) => {
                    Value::Int64(checked_sum_i64(prev, delta, "HashAggregate SUM(BIGINT)")?)
                }
                Some(other) => {
                    return Err(ExecError::TypeMismatch(format!(
                        "vectorized SUM accumulator/column type mismatch: {other:?} vs Int64"
                    )));
                }
            });
        }
        Column::Int32(c) => {
            if c.is_empty() {
                return Ok(());
            }
            let (delta, saw) = sum_i32_nullable_widened(c)?;
            if !saw {
                return Ok(());
            }
            *acc = Some(match acc.take() {
                None => Value::Int64(delta),
                Some(Value::Int64(prev)) => {
                    Value::Int64(checked_sum_i64(prev, delta, "HashAggregate SUM(INT)")?)
                }
                Some(other) => {
                    return Err(ExecError::TypeMismatch(format!(
                        "vectorized SUM accumulator/column type mismatch: {other:?} vs Int32"
                    )));
                }
            });
        }
        Column::Float64(c) => {
            if c.is_empty() {
                return Ok(());
            }
            let (delta, saw) = sum_f64_nullable(c);
            if !saw {
                return Ok(());
            }
            *acc = Some(match acc.take() {
                None => Value::Float64(delta),
                Some(Value::Float64(prev)) => Value::Float64(prev + delta),
                Some(other) => {
                    return Err(ExecError::TypeMismatch(format!(
                        "vectorized SUM accumulator/column type mismatch: {other:?} vs Float64"
                    )));
                }
            });
        }
        Column::Float32(c) => {
            if c.is_empty() {
                return Ok(());
            }
            let (delta, saw) = sum_f32_nullable_widened(c);
            if !saw {
                return Ok(());
            }
            *acc = Some(match acc.take() {
                None => Value::Float64(delta),
                Some(Value::Float64(prev)) => Value::Float64(prev + delta),
                Some(other) => {
                    return Err(ExecError::TypeMismatch(format!(
                        "vectorized SUM accumulator/column type mismatch: {other:?} vs Float32"
                    )));
                }
            });
        }
        other => {
            return Err(ExecError::TypeMismatch(format!(
                "vectorized SUM not supported for column: {other:?}"
            )));
        }
    }
    Ok(())
}

/// Sum non-null entries of an `i64` column. Returns `(sum, saw_non_null)`.
///
/// The two arms compile to different shapes — the dense path autovectorises
/// to a single NEON / AVX2 fold; the null-aware path keeps a branch per row
/// — so we keep them as a `match`. Clippy's `map_or_else` suggestion would
/// hide that distinction inside a closure body.
#[allow(clippy::option_if_let_else)]
fn sum_i64_nullable(c: &NumericColumn<i64>) -> Result<(i64, bool), ExecError> {
    match c.nulls() {
        None => {
            let mut s = 0_i64;
            for value in c.data().iter().copied() {
                s = checked_sum_i64(s, value, "HashAggregate SUM(BIGINT)")?;
            }
            Ok((s, !c.is_empty()))
        }
        Some(nulls) => {
            let mut s: i64 = 0;
            let mut saw = false;
            for (i, v) in c.data().iter().enumerate() {
                if nulls.get(i) {
                    s = checked_sum_i64(s, *v, "HashAggregate SUM(BIGINT)")?;
                    saw = true;
                }
            }
            Ok((s, saw))
        }
    }
}

/// Sum non-null entries of an `i32` column, widening to `i64`. Dispatches
/// to the hand-NEON [`ultrasql_vec::kernels::sum_i32_widening`] on
/// aarch64 and to the scalar fold on every other target.
#[allow(clippy::option_if_let_else)]
fn sum_i32_nullable_widened(c: &NumericColumn<i32>) -> Result<(i64, bool), ExecError> {
    match c.nulls() {
        None => {
            let s = ultrasql_vec::kernels::sum_i32_widening(c);
            Ok((s, !c.is_empty()))
        }
        Some(nulls) => {
            let mut s: i64 = 0;
            let mut saw = false;
            for (i, &v) in c.data().iter().enumerate() {
                if nulls.get(i) {
                    s = checked_sum_i64(s, i64::from(v), "HashAggregate SUM(INT)")?;
                    saw = true;
                }
            }
            Ok((s, saw))
        }
    }
}

/// Sum non-null entries of an `f64` column. Returns `(sum, saw_non_null)`.
#[allow(clippy::option_if_let_else)]
fn sum_f64_nullable(c: &NumericColumn<f64>) -> (f64, bool) {
    match c.nulls() {
        None => (c.data().iter().sum(), !c.is_empty()),
        Some(nulls) => {
            let mut s = 0.0_f64;
            let mut saw = false;
            for (i, &v) in c.data().iter().enumerate() {
                if nulls.get(i) {
                    s += v;
                    saw = true;
                }
            }
            (s, saw)
        }
    }
}

/// Sum non-null entries of an `f32` column, widening to `f64` (matching
/// the scalar `add_values` widening of Float32 → Float64).
#[allow(clippy::option_if_let_else)]
fn sum_f32_nullable_widened(c: &NumericColumn<f32>) -> (f64, bool) {
    match c.nulls() {
        None => {
            let s = c.data().iter().fold(0.0_f64, |a, &b| a + f64::from(b));
            (s, !c.is_empty())
        }
        Some(nulls) => {
            let mut s = 0.0_f64;
            let mut saw = false;
            for (i, &v) in c.data().iter().enumerate() {
                if nulls.get(i) {
                    s += f64::from(v);
                    saw = true;
                }
            }
            (s, saw)
        }
    }
}

/// Update a running MIN/MAX accumulator from a column. `take_min = true`
/// for MIN; `false` for MAX. NULLs are skipped.
#[allow(clippy::option_if_let_else)]
pub(crate) fn update_extremum(acc: &mut Option<Value>, col: &Column, take_min: bool) -> Result<(), ExecError> {
    let candidate = match col {
        Column::Int64(c) => {
            if take_min {
                min_i64(c).map(Value::Int64)
            } else {
                max_i64(c).map(Value::Int64)
            }
        }
        Column::Int32(c) => extremum_i32(c, take_min).map(Value::Int32),
        Column::Float64(c) => extremum_f64(c, take_min).map(Value::Float64),
        Column::Float32(c) => extremum_f32(c, take_min).map(Value::Float32),
        other => {
            return Err(ExecError::TypeMismatch(format!(
                "vectorized MIN/MAX not supported for column: {other:?}"
            )));
        }
    };
    if let Some(v) = candidate {
        *acc = Some(match acc.take() {
            None => v,
            Some(existing) => {
                let pick_new = if take_min {
                    value_lt(&v, &existing)
                } else {
                    value_lt(&existing, &v)
                };
                if pick_new { v } else { existing }
            }
        });
    }
    Ok(())
}

#[allow(clippy::option_if_let_else)]
fn extremum_i32(c: &NumericColumn<i32>, take_min: bool) -> Option<i32> {
    let mut best: Option<i32> = None;
    if let Some(nulls) = c.nulls() {
        for (i, &v) in c.data().iter().enumerate() {
            if !nulls.get(i) {
                continue;
            }
            best = Some(match best {
                None => v,
                Some(b) => {
                    if take_min {
                        if v < b { v } else { b }
                    } else if v > b {
                        v
                    } else {
                        b
                    }
                }
            });
        }
    } else {
        for &v in c.data() {
            best = Some(match best {
                None => v,
                Some(b) => {
                    if take_min {
                        if v < b { v } else { b }
                    } else if v > b {
                        v
                    } else {
                        b
                    }
                }
            });
        }
    }
    best
}

#[allow(clippy::option_if_let_else)]
fn extremum_f64(c: &NumericColumn<f64>, take_min: bool) -> Option<f64> {
    let mut best: Option<f64> = None;
    let consider = |best: Option<f64>, v: f64| -> Option<f64> {
        if v.is_nan() {
            return best;
        }
        Some(match best {
            None => v,
            Some(b) => {
                if take_min {
                    b.min(v)
                } else {
                    b.max(v)
                }
            }
        })
    };
    if let Some(nulls) = c.nulls() {
        for (i, &v) in c.data().iter().enumerate() {
            if !nulls.get(i) {
                continue;
            }
            best = consider(best, v);
        }
    } else {
        for &v in c.data() {
            best = consider(best, v);
        }
    }
    best
}

#[allow(clippy::option_if_let_else)]
fn extremum_f32(c: &NumericColumn<f32>, take_min: bool) -> Option<f32> {
    let mut best: Option<f32> = None;
    let consider = |best: Option<f32>, v: f32| -> Option<f32> {
        if v.is_nan() {
            return best;
        }
        Some(match best {
            None => v,
            Some(b) => {
                if take_min {
                    b.min(v)
                } else {
                    b.max(v)
                }
            }
        })
    };
    if let Some(nulls) = c.nulls() {
        for (i, &v) in c.data().iter().enumerate() {
            if !nulls.get(i) {
                continue;
            }
            best = consider(best, v);
        }
    } else {
        for &v in c.data() {
            best = consider(best, v);
        }
    }
    best
}
