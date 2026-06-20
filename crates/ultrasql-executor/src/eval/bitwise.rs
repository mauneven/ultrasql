//! Bit-string and integer bitwise helpers.
//!
//! Extracted verbatim from the original `eval.rs`; pure code motion.

use super::*;

// ---------------------------------------------------------------------------
// Bitwise helpers
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
pub(crate) enum BitStringOp {
    And,
    Or,
    Xor,
}

pub(crate) fn bit_string_arg<'a>(
    function: &str,
    args: &'a [Value],
    idx: usize,
) -> Result<Option<&'a ultrasql_core::BitString>, EvalError> {
    match args.get(idx) {
        Some(Value::BitString(bits)) => Ok(Some(bits)),
        Some(Value::Null) => Ok(None),
        Some(other) => Err(EvalError::Type(format!(
            "{function}: argument {} must be bit string, got {:?}",
            idx + 1,
            other.data_type()
        ))),
        None => Err(EvalError::Type(format!(
            "{function}: missing argument {}",
            idx + 1
        ))),
    }
}

pub(crate) fn integer_arg_as_usize(
    function: &str,
    args: &[Value],
    idx: usize,
) -> Result<Option<usize>, EvalError> {
    let value = match args.get(idx) {
        Some(Value::Int16(v)) => i64::from(*v),
        Some(Value::Int32(v)) => i64::from(*v),
        Some(Value::Int64(v)) => *v,
        Some(Value::Null) => return Ok(None),
        Some(other) => {
            return Err(EvalError::Type(format!(
                "{function}: argument {} must be integer, got {:?}",
                idx + 1,
                other.data_type()
            )));
        }
        None => {
            return Err(EvalError::Type(format!(
                "{function}: missing argument {}",
                idx + 1
            )));
        }
    };
    if value < 0 {
        return Err(EvalError::Type(format!(
            "{function}: argument {} must be non-negative",
            idx + 1
        )));
    }
    usize::try_from(value)
        .map(Some)
        .map_err(|_| EvalError::Type(format!("{function}: integer argument out of range")))
}

pub(crate) fn bitwise_or_integer(
    lv: Value,
    rv: Value,
    bit_op: BitStringOp,
    int_op: impl Fn(i64, i64) -> i64,
) -> Result<Value, EvalError> {
    match (lv, rv) {
        (Value::BitString(left), Value::BitString(right)) => {
            let result = match bit_op {
                BitStringOp::And => left.bit_and(&right),
                BitStringOp::Or => left.bit_or(&right),
                BitStringOp::Xor => left.bit_xor(&right),
            };
            result.map(Value::BitString).ok_or_else(|| {
                EvalError::Type("bitwise operation requires equal-length bit strings".to_owned())
            })
        }
        (Value::Network(left), Value::Network(right)) => left
            .bitwise(right, |a, b| match bit_op {
                BitStringOp::And => a & b,
                BitStringOp::Or => a | b,
                BitStringOp::Xor => a ^ b,
            })
            .map(Value::Network)
            .ok_or_else(|| {
                EvalError::Type(
                    "network bitwise operation requires matching address families".to_owned(),
                )
            }),
        (left, right) => integer_bitwise(left, right, int_op),
    }
}

pub(crate) fn shift_bit_string_or_integer(lv: Value, rv: Value, left_shift: bool) -> Result<Value, EvalError> {
    match (&lv, &rv) {
        (Value::Network(_), Value::Network(_)) => network_containment(lv, rv, !left_shift, false),
        (Value::BitString(bits), _) => {
            let amount = shift_amount(&rv)?;
            if left_shift {
                bits.shift_left(amount)
            } else {
                bits.shift_right(amount)
            }
            .map(Value::BitString)
            .ok_or(EvalError::Overflow)
        }
        _ => {
            if left_shift {
                integer_bitwise(lv, rv, |a, b| a << (b & 63))
            } else {
                integer_bitwise(lv, rv, |a, b| a >> (b & 63))
            }
        }
    }
}

pub(crate) fn network_containment(
    lv: Value,
    rv: Value,
    left_contains_right: bool,
    allow_equal: bool,
) -> Result<Value, EvalError> {
    let (Value::Network(left), Value::Network(right)) = (lv, rv) else {
        return Err(EvalError::Type(
            "network containment requires inet/cidr operands".to_owned(),
        ));
    };
    let left = left
        .inet_addr()
        .ok_or_else(|| EvalError::Type("network containment requires inet/cidr".to_owned()))?;
    let right = right
        .inet_addr()
        .ok_or_else(|| EvalError::Type("network containment requires inet/cidr".to_owned()))?;
    let result = if left_contains_right {
        if allow_equal {
            left.contains_or_equal(right)
        } else {
            left.contains_strict(right)
        }
    } else if allow_equal {
        right.contains_or_equal(left)
    } else {
        right.contains_strict(left)
    };
    Ok(Value::Bool(result))
}

pub(crate) fn shift_amount(value: &Value) -> Result<usize, EvalError> {
    let raw = match value {
        Value::Int16(v) => i64::from(*v),
        Value::Int32(v) => i64::from(*v),
        Value::Int64(v) => *v,
        other => {
            return Err(EvalError::Type(format!(
                "bit shift requires integer shift count, got {other:?}"
            )));
        }
    };
    if raw < 0 {
        return Err(EvalError::Type(
            "bit shift requires non-negative shift count".to_owned(),
        ));
    }
    usize::try_from(raw).map_err(|_| EvalError::Overflow)
}

/// Evaluate a bitwise binary operation on integer operands.
///
/// Only `Int16`, `Int32`, and `Int64` value pairs are accepted. The
/// `op` closure receives `i64`-promoted operands so a single closure
/// form covers all widths; the result is narrowed back to the input
/// width.
pub(crate) fn integer_bitwise(lv: Value, rv: Value, op: impl Fn(i64, i64) -> i64) -> Result<Value, EvalError> {
    match (lv, rv) {
        (Value::Int16(l), Value::Int16(r)) => {
            let result = op(i64::from(l), i64::from(r));
            i16::try_from(result)
                .map(Value::Int16)
                .map_err(|_| EvalError::Overflow)
        }
        (Value::Int32(l), Value::Int32(r)) => {
            let result = op(i64::from(l), i64::from(r));
            i32::try_from(result)
                .map(Value::Int32)
                .map_err(|_| EvalError::Overflow)
        }
        (Value::Int64(l), Value::Int64(r)) => Ok(Value::Int64(op(l, r))),
        (l, r) => Err(EvalError::Type(format!(
            "bitwise operation requires matching integer operands, got {l:?} and {r:?}"
        ))),
    }
}

