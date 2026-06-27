//! Numeric/money/decimal arithmetic helpers.
//!
//! Extracted verbatim from the original `eval.rs`; pure code motion.

use super::*;

// ---------------------------------------------------------------------------
// Arithmetic helpers
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
pub(crate) enum ArithOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Pow,
}

pub(crate) fn network_or_numeric_arith(
    lv: Value,
    rv: Value,
    op: ArithOp,
) -> Result<Value, EvalError> {
    match (lv, rv, op) {
        (Value::Network(network), value, ArithOp::Add) => {
            let delta = integer_delta(&value)?;
            let addr = network.inet_addr().ok_or_else(|| {
                EvalError::Type(format!(
                    "network arithmetic requires inet/cidr, got {network:?}"
                ))
            })?;
            addr.checked_add(delta)
                .map(ultrasql_core::NetworkValue::Inet)
                .map(Value::Network)
                .ok_or(EvalError::Overflow)
        }
        (value, Value::Network(network), ArithOp::Add) => {
            let delta = integer_delta(&value)?;
            let addr = network.inet_addr().ok_or_else(|| {
                EvalError::Type(format!(
                    "network arithmetic requires inet/cidr, got {network:?}"
                ))
            })?;
            addr.checked_add(delta)
                .map(ultrasql_core::NetworkValue::Inet)
                .map(Value::Network)
                .ok_or(EvalError::Overflow)
        }
        (Value::Network(left), Value::Network(right), ArithOp::Sub) => {
            let left = left.inet_addr().ok_or_else(|| {
                EvalError::Type(format!(
                    "network subtraction requires inet/cidr, got {left:?}"
                ))
            })?;
            let right = right.inet_addr().ok_or_else(|| {
                EvalError::Type(format!(
                    "network subtraction requires inet/cidr, got {right:?}"
                ))
            })?;
            left.checked_sub_addr(right)
                .map(Value::Int64)
                .ok_or(EvalError::Overflow)
        }
        (Value::Network(network), value, ArithOp::Sub) => {
            let delta = integer_delta(&value)?;
            let delta = delta.checked_neg().ok_or(EvalError::Overflow)?;
            let addr = network.inet_addr().ok_or_else(|| {
                EvalError::Type(format!(
                    "network arithmetic requires inet/cidr, got {network:?}"
                ))
            })?;
            addr.checked_add(delta)
                .map(ultrasql_core::NetworkValue::Inet)
                .map(Value::Network)
                .ok_or(EvalError::Overflow)
        }
        (left, right, op) => numeric_arith(left, right, op),
    }
}

pub(crate) fn integer_delta(value: &Value) -> Result<i64, EvalError> {
    match value {
        Value::Int16(v) => Ok(i64::from(*v)),
        Value::Int32(v) => Ok(i64::from(*v)),
        Value::Int64(v) => Ok(*v),
        other => Err(EvalError::Type(format!(
            "network arithmetic requires integer offset, got {other:?}"
        ))),
    }
}

/// Evaluate an arithmetic binary operation.
///
/// Integer overflow returns [`EvalError::Overflow`]. Division by zero
/// returns [`EvalError::DivByZero`]. Type mismatches return
/// [`EvalError::Type`]. Floating-point overflow produces `f64::INFINITY`
/// (IEEE 754 semantics, consistent with PostgreSQL).
pub(crate) fn numeric_arith(lv: Value, rv: Value, op: ArithOp) -> Result<Value, EvalError> {
    if let Some(value) = money_arith(&lv, &rv, op)? {
        return Ok(value);
    }

    // PostgreSQL's `^` (power) operator is `float8 ^ float8`: it promotes both
    // operands to double precision and returns double precision. We dispatch
    // every numeric/integer/float (and decimal) operand pair through
    // `float64_arith`, which both avoids the integer overflow of `int ^ int`
    // (e.g. `10 ^ 19`) and matches the binder's Float64 result type. (PG
    // returns `numeric` for numeric operands; we return the same value as
    // float8, which is the accepted approximation here.)
    if matches!(op, ArithOp::Pow) {
        if let (Some(left), Some(right)) = (as_f64_for_arith(&lv), as_f64_for_arith(&rv)) {
            return float64_arith(left, right, op);
        }
    }

    if let Some((left, right)) = decimal_float_operands(&lv, &rv) {
        return float64_arith(left, right, op);
    }

    if matches!(
        (&lv, &rv),
        (Value::Decimal { .. }, _) | (_, Value::Decimal { .. })
    ) {
        let Some((left_value, left_scale)) = numeric_to_decimal(&lv)? else {
            return Err(EvalError::Type(format!(
                "arithmetic type mismatch: {lv:?} and {rv:?}"
            )));
        };
        let Some((right_value, right_scale)) = numeric_to_decimal(&rv)? else {
            return Err(EvalError::Type(format!(
                "arithmetic type mismatch: {lv:?} and {rv:?}"
            )));
        };
        return decimal_arith(left_value, left_scale, right_value, right_scale, op);
    }

    match (lv, rv) {
        (Value::Int16(l), Value::Int16(r)) => int16_arith(l, r, op),
        (Value::Int32(l), Value::Int32(r)) => int32_arith(l, r, op),
        (Value::Int64(l), Value::Int64(r)) => int64_arith(l, r, op),
        (Value::Int16(l), Value::Int32(r)) => int32_arith(i32::from(l), r, op),
        (Value::Int32(l), Value::Int16(r)) => int32_arith(l, i32::from(r), op),
        (Value::Int16(l), Value::Int64(r)) => int64_arith(i64::from(l), r, op),
        (Value::Int64(l), Value::Int16(r)) => int64_arith(l, i64::from(r), op),
        (Value::Int32(l), Value::Int64(r)) => int64_arith(i64::from(l), r, op),
        (Value::Int64(l), Value::Int32(r)) => int64_arith(l, i64::from(r), op),
        (Value::Float32(l), Value::Float32(r)) => float32_arith(l, r, op),
        (Value::Float64(l), Value::Float64(r)) => float64_arith(l, r, op),
        (Value::Float64(l), Value::Float32(r)) => float64_arith(l, f64::from(r), op),
        (Value::Float32(l), Value::Float64(r)) => float64_arith(f64::from(l), r, op),
        // Mixed integer/float arithmetic: PostgreSQL promotes both operands to
        // double precision (int -> float8 is the preferred implicit cast) and
        // returns float8, for every (int width x float width) pair in both
        // orders. Float32 (`real`) + integer therefore also yields float8.
        (Value::Float64(l), Value::Int16(r)) => float64_arith(l, f64::from(r), op),
        (Value::Int16(l), Value::Float64(r)) => float64_arith(f64::from(l), r, op),
        (Value::Float64(l), Value::Int32(r)) => float64_arith(l, f64::from(r), op),
        (Value::Int32(l), Value::Float64(r)) => float64_arith(f64::from(l), r, op),
        (Value::Float64(l), Value::Int64(r)) => {
            float64_arith(l, r.to_f64().ok_or(EvalError::Overflow)?, op)
        }
        (Value::Int64(l), Value::Float64(r)) => {
            float64_arith(l.to_f64().ok_or(EvalError::Overflow)?, r, op)
        }
        (Value::Float32(l), Value::Int16(r)) => float64_arith(f64::from(l), f64::from(r), op),
        (Value::Int16(l), Value::Float32(r)) => float64_arith(f64::from(l), f64::from(r), op),
        (Value::Float32(l), Value::Int32(r)) => float64_arith(f64::from(l), f64::from(r), op),
        (Value::Int32(l), Value::Float32(r)) => float64_arith(f64::from(l), f64::from(r), op),
        (Value::Float32(l), Value::Int64(r)) => {
            float64_arith(f64::from(l), r.to_f64().ok_or(EvalError::Overflow)?, op)
        }
        (Value::Int64(l), Value::Float32(r)) => {
            float64_arith(l.to_f64().ok_or(EvalError::Overflow)?, f64::from(r), op)
        }
        (l, r) => Err(EvalError::Type(format!(
            "arithmetic type mismatch: {l:?} and {r:?}"
        ))),
    }
}

/// Promote an integer, float, or decimal [`Value`] to `f64` for the `^`
/// power operator (PostgreSQL's `^` is `float8 ^ float8`). Returns `None`
/// for non-numeric values.
pub(crate) fn as_f64_for_arith(value: &Value) -> Option<f64> {
    match value {
        Value::Int16(v) => Some(f64::from(*v)),
        Value::Int32(v) => Some(f64::from(*v)),
        Value::Int64(v) => v.to_f64(),
        Value::Float32(v) => Some(f64::from(*v)),
        Value::Float64(v) => Some(*v),
        Value::Decimal { value, scale } => decimal_value_to_f64(*value, *scale),
        _ => None,
    }
}

pub(crate) fn money_arith(
    left: &Value,
    right: &Value,
    op: ArithOp,
) -> Result<Option<Value>, EvalError> {
    match (left, right, op) {
        (Value::Money(l), Value::Money(r), ArithOp::Add) => l
            .checked_add(*r)
            .map(Value::Money)
            .map(Some)
            .ok_or(EvalError::Overflow),
        (Value::Money(l), Value::Money(r), ArithOp::Sub) => l
            .checked_sub(*r)
            .map(Value::Money)
            .map(Some)
            .ok_or(EvalError::Overflow),
        (Value::Money(l), Value::Money(r), ArithOp::Div) => money_ratio(*l, *r).map(Some),
        (Value::Money(cents), Value::Int16(divisor), ArithOp::Div) => {
            money_integer_div(*cents, i64::from(*divisor)).map(Some)
        }
        (Value::Money(cents), Value::Int32(divisor), ArithOp::Div) => {
            money_integer_div(*cents, i64::from(*divisor)).map(Some)
        }
        (Value::Money(cents), Value::Int64(divisor), ArithOp::Div) => {
            money_integer_div(*cents, *divisor).map(Some)
        }
        (Value::Money(cents), Value::Float32(divisor), ArithOp::Div) => {
            money_float_div(*cents, f64::from(*divisor)).map(Some)
        }
        (Value::Money(cents), Value::Float64(divisor), ArithOp::Div) => {
            money_float_div(*cents, *divisor).map(Some)
        }
        (Value::Money(cents), Value::Int16(multiplier), ArithOp::Mul) => {
            money_integer_mul(*cents, i64::from(*multiplier)).map(Some)
        }
        (Value::Money(cents), Value::Int32(multiplier), ArithOp::Mul) => {
            money_integer_mul(*cents, i64::from(*multiplier)).map(Some)
        }
        (Value::Money(cents), Value::Int64(multiplier), ArithOp::Mul) => {
            money_integer_mul(*cents, *multiplier).map(Some)
        }
        (Value::Int16(multiplier), Value::Money(cents), ArithOp::Mul) => {
            money_integer_mul(*cents, i64::from(*multiplier)).map(Some)
        }
        (Value::Int32(multiplier), Value::Money(cents), ArithOp::Mul) => {
            money_integer_mul(*cents, i64::from(*multiplier)).map(Some)
        }
        (Value::Int64(multiplier), Value::Money(cents), ArithOp::Mul) => {
            money_integer_mul(*cents, *multiplier).map(Some)
        }
        (Value::Money(cents), Value::Float32(multiplier), ArithOp::Mul) => {
            money_float_mul(*cents, f64::from(*multiplier)).map(Some)
        }
        (Value::Money(cents), Value::Float64(multiplier), ArithOp::Mul) => {
            money_float_mul(*cents, *multiplier).map(Some)
        }
        (Value::Float32(multiplier), Value::Money(cents), ArithOp::Mul) => {
            money_float_mul(*cents, f64::from(*multiplier)).map(Some)
        }
        (Value::Float64(multiplier), Value::Money(cents), ArithOp::Mul) => {
            money_float_mul(*cents, *multiplier).map(Some)
        }
        (Value::Money(_), Value::Money(_), _) => Err(EvalError::Type(
            "money arithmetic supports addition, subtraction, multiplication, and division"
                .to_owned(),
        )),
        _ => Ok(None),
    }
}

pub(crate) fn money_ratio(left_cents: i64, right_cents: i64) -> Result<Value, EvalError> {
    if right_cents == 0 {
        return Err(EvalError::DivByZero);
    }
    Ok(Value::Float64(
        cents_to_f64(left_cents)? / cents_to_f64(right_cents)?,
    ))
}

pub(crate) fn money_integer_div(cents: i64, divisor: i64) -> Result<Value, EvalError> {
    if divisor == 0 {
        return Err(EvalError::DivByZero);
    }
    cents
        .checked_div(divisor)
        .map(Value::Money)
        .ok_or(EvalError::Overflow)
}

pub(crate) fn money_integer_mul(cents: i64, multiplier: i64) -> Result<Value, EvalError> {
    cents
        .checked_mul(multiplier)
        .map(Value::Money)
        .ok_or(EvalError::Overflow)
}

pub(crate) fn money_float_mul(cents: i64, multiplier: f64) -> Result<Value, EvalError> {
    rounded_money_from_f64(cents_to_f64(cents)? * multiplier)
}

pub(crate) fn money_float_div(cents: i64, divisor: f64) -> Result<Value, EvalError> {
    if divisor == 0.0 {
        return Err(EvalError::DivByZero);
    }
    rounded_money_from_f64(cents_to_f64(cents)? / divisor)
}

pub(crate) fn rounded_money_from_f64(cents: f64) -> Result<Value, EvalError> {
    cents
        .round()
        .to_i64()
        .map(Value::Money)
        .ok_or(EvalError::Overflow)
}

pub(crate) fn cents_to_f64(cents: i64) -> Result<f64, EvalError> {
    cents.to_f64().ok_or(EvalError::Overflow)
}

pub(crate) fn decimal_float_operands(left: &Value, right: &Value) -> Option<(f64, f64)> {
    match (left, right) {
        (Value::Decimal { value, scale }, Value::Float32(r)) => {
            Some((decimal_value_to_f64(*value, *scale)?, f64::from(*r)))
        }
        (Value::Decimal { value, scale }, Value::Float64(r)) => {
            Some((decimal_value_to_f64(*value, *scale)?, *r))
        }
        (Value::Float32(l), Value::Decimal { value, scale }) => {
            Some((f64::from(*l), decimal_value_to_f64(*value, *scale)?))
        }
        (Value::Float64(l), Value::Decimal { value, scale }) => {
            Some((*l, decimal_value_to_f64(*value, *scale)?))
        }
        _ => None,
    }
}

pub(crate) fn decimal_value_to_f64(value: i128, scale: i32) -> Option<f64> {
    value.to_f64().map(|raw| raw / 10_f64.powi(scale))
}

pub(crate) fn numeric_to_decimal(value: &Value) -> Result<Option<(i128, i32)>, EvalError> {
    match value {
        Value::Decimal { value, scale } => Ok(Some((*value, *scale))),
        Value::Int16(v) => Ok(Some((i128::from(*v), 0))),
        Value::Int32(v) => Ok(Some((i128::from(*v), 0))),
        Value::Int64(v) => Ok(Some((i128::from(*v), 0))),
        Value::Float32(v) => decimal_from_f64(f64::from(*v)).map(Some),
        Value::Float64(v) => decimal_from_f64(*v).map(Some),
        _ => Ok(None),
    }
}

pub(crate) fn decimal_from_f64(value: f64) -> Result<(i128, i32), EvalError> {
    if !value.is_finite() {
        return Err(EvalError::Type(
            "cannot coerce non-finite float to decimal".to_owned(),
        ));
    }
    let text = value.to_string();
    decimal_from_text(&text)
        .ok_or_else(|| EvalError::Type(format!("cannot coerce float literal `{text}` to decimal")))
}

pub(crate) fn decimal_from_text(text: &str) -> Option<(i128, i32)> {
    if text.contains('e') || text.contains('E') {
        return None;
    }
    let (negative, unsigned) = text
        .strip_prefix('-')
        .map_or((false, text), |stripped| (true, stripped));
    let (whole, frac) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    let scale = i32::try_from(frac.len()).ok()?;
    let mut digits = String::with_capacity(whole.len() + frac.len());
    digits.push_str(if whole.is_empty() { "0" } else { whole });
    digits.push_str(frac);
    let mut value = digits.parse::<i128>().ok()?;
    if negative {
        value = value.checked_neg()?;
    }
    Some((value, scale))
}

pub(crate) fn int16_arith(l: i16, r: i16, op: ArithOp) -> Result<Value, EvalError> {
    let result = match op {
        ArithOp::Add => l.checked_add(r),
        ArithOp::Sub => l.checked_sub(r),
        ArithOp::Mul => l.checked_mul(r),
        ArithOp::Div => {
            if r == 0 {
                return Err(EvalError::DivByZero);
            }
            l.checked_div(r)
        }
        ArithOp::Mod => {
            if r == 0 {
                return Err(EvalError::DivByZero);
            }
            l.checked_rem(r)
        }
        ArithOp::Pow => {
            let base = i64::from(l);
            let exp = i64::from(r);
            if exp < 0 {
                return Err(EvalError::Type(
                    "negative exponent not supported for integer types".to_owned(),
                ));
            }
            let exp_u32 = u32::try_from(exp).map_err(|_| EvalError::Overflow)?;
            let result = base.checked_pow(exp_u32).ok_or(EvalError::Overflow)?;
            let result_i16 = i16::try_from(result).map_err(|_| EvalError::Overflow)?;
            return Ok(Value::Int16(result_i16));
        }
    };
    result.map(Value::Int16).ok_or(EvalError::Overflow)
}

pub(crate) fn int32_arith(l: i32, r: i32, op: ArithOp) -> Result<Value, EvalError> {
    let result = match op {
        ArithOp::Add => l.checked_add(r),
        ArithOp::Sub => l.checked_sub(r),
        ArithOp::Mul => l.checked_mul(r),
        ArithOp::Div => {
            if r == 0 {
                return Err(EvalError::DivByZero);
            }
            l.checked_div(r)
        }
        ArithOp::Mod => {
            if r == 0 {
                return Err(EvalError::DivByZero);
            }
            l.checked_rem(r)
        }
        ArithOp::Pow => {
            let base = i64::from(l);
            let exp = i64::from(r);
            if exp < 0 {
                return Err(EvalError::Type(
                    "negative exponent not supported for integer types".to_owned(),
                ));
            }
            let exp_u32 = u32::try_from(exp).map_err(|_| EvalError::Overflow)?;
            let result = base.checked_pow(exp_u32).ok_or(EvalError::Overflow)?;
            let result_i32 = i32::try_from(result).map_err(|_| EvalError::Overflow)?;
            return Ok(Value::Int32(result_i32));
        }
    };
    result.map(Value::Int32).ok_or(EvalError::Overflow)
}

pub(crate) fn int64_arith(l: i64, r: i64, op: ArithOp) -> Result<Value, EvalError> {
    let result = match op {
        ArithOp::Add => l.checked_add(r),
        ArithOp::Sub => l.checked_sub(r),
        ArithOp::Mul => l.checked_mul(r),
        ArithOp::Div => {
            if r == 0 {
                return Err(EvalError::DivByZero);
            }
            l.checked_div(r)
        }
        ArithOp::Mod => {
            if r == 0 {
                return Err(EvalError::DivByZero);
            }
            l.checked_rem(r)
        }
        ArithOp::Pow => {
            if r < 0 {
                return Err(EvalError::Type(
                    "negative exponent not supported for integer types".to_owned(),
                ));
            }
            let exp_u32 = u32::try_from(r).map_err(|_| EvalError::Overflow)?;
            return l
                .checked_pow(exp_u32)
                .map(Value::Int64)
                .ok_or(EvalError::Overflow);
        }
    };
    result.map(Value::Int64).ok_or(EvalError::Overflow)
}

pub(crate) fn decimal_arith(
    left_value: i128,
    left_scale: i32,
    right_value: i128,
    right_scale: i32,
    op: ArithOp,
) -> Result<Value, EvalError> {
    match op {
        ArithOp::Add => {
            let common_scale = left_scale.max(right_scale);
            let left = rescale_decimal_value(left_value, left_scale, common_scale)?;
            let right = rescale_decimal_value(right_value, right_scale, common_scale)?;
            let value = left.checked_add(right).ok_or(EvalError::Overflow)?;
            Ok(Value::Decimal {
                value,
                scale: common_scale,
            })
        }
        ArithOp::Sub => {
            let common_scale = left_scale.max(right_scale);
            let left = rescale_decimal_value(left_value, left_scale, common_scale)?;
            let right = rescale_decimal_value(right_value, right_scale, common_scale)?;
            let value = left.checked_sub(right).ok_or(EvalError::Overflow)?;
            Ok(Value::Decimal {
                value,
                scale: common_scale,
            })
        }
        ArithOp::Mod => {
            let common_scale = left_scale.max(right_scale);
            let left = rescale_decimal_value(left_value, left_scale, common_scale)?;
            let right = rescale_decimal_value(right_value, right_scale, common_scale)?;
            if right == 0 {
                return Err(EvalError::DivByZero);
            }
            Ok(Value::Decimal {
                value: left % right,
                scale: common_scale,
            })
        }
        ArithOp::Mul => {
            let scale = left_scale
                .checked_add(right_scale)
                .ok_or(EvalError::Overflow)?;
            let value = left_value
                .checked_mul(right_value)
                .ok_or(EvalError::Overflow)?;
            Ok(Value::Decimal { value, scale })
        }
        ArithOp::Div => {
            if right_value == 0 {
                return Err(EvalError::DivByZero);
            }
            let result_scale = left_scale.max(right_scale).max(6);
            let exponent = right_scale
                .checked_add(result_scale)
                .and_then(|v| v.checked_sub(left_scale))
                .ok_or(EvalError::Overflow)?;
            let factor = pow10_i128(u32::try_from(exponent).map_err(|_| EvalError::Overflow)?)
                .ok_or(EvalError::Overflow)?;
            let numerator = left_value.checked_mul(factor).ok_or(EvalError::Overflow)?;
            let denominator = right_value;
            let mut quotient = numerator / denominator;
            let remainder = numerator % denominator;
            if remainder != 0 {
                let twice_remainder = remainder
                    .checked_abs()
                    .and_then(|r| r.checked_mul(2))
                    .ok_or(EvalError::Overflow)?;
                let divisor = denominator.checked_abs().ok_or(EvalError::Overflow)?;
                if twice_remainder >= divisor {
                    let adjustment = if (numerator >= 0) == (denominator >= 0) {
                        1
                    } else {
                        -1
                    };
                    quotient = quotient
                        .checked_add(adjustment)
                        .ok_or(EvalError::Overflow)?;
                }
            }
            Ok(Value::Decimal {
                value: quotient,
                scale: result_scale,
            })
        }
        ArithOp::Pow => Err(EvalError::Type(
            "decimal exponentiation not supported".to_owned(),
        )),
    }
}

pub(crate) fn float32_arith(l: f32, r: f32, op: ArithOp) -> Result<Value, EvalError> {
    let result = match op {
        ArithOp::Add => l + r,
        ArithOp::Sub => l - r,
        ArithOp::Mul => l * r,
        ArithOp::Div => {
            if r == 0.0 {
                return Err(EvalError::DivByZero);
            }
            l / r
        }
        ArithOp::Mod => {
            if r == 0.0 {
                return Err(EvalError::DivByZero);
            }
            l % r
        }
        ArithOp::Pow => l.powf(r),
    };
    Ok(Value::Float32(result))
}

pub(crate) fn float64_arith(l: f64, r: f64, op: ArithOp) -> Result<Value, EvalError> {
    let result = match op {
        ArithOp::Add => l + r,
        ArithOp::Sub => l - r,
        ArithOp::Mul => l * r,
        ArithOp::Div => {
            if r == 0.0 {
                return Err(EvalError::DivByZero);
            }
            l / r
        }
        ArithOp::Mod => {
            if r == 0.0 {
                return Err(EvalError::DivByZero);
            }
            l % r
        }
        ArithOp::Pow => l.powf(r),
    };
    Ok(Value::Float64(result))
}
