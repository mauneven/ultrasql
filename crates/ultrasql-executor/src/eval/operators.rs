//! Operator dispatch: Kleene AND/OR, unary, and binary.
//!
//! Extracted verbatim from the original `eval.rs`; pure code motion.

use super::*;

// ---------------------------------------------------------------------------
// Kleene short-circuit AND / OR
// ---------------------------------------------------------------------------

/// Kleene three-valued AND:
/// - `FALSE AND anything = FALSE`
/// - `TRUE AND x = x`
/// - `NULL AND FALSE = FALSE`, `NULL AND TRUE = NULL`
pub(crate) fn eval_and(
    left: &ScalarExpr,
    right: &ScalarExpr,
    row: &[Value],
    params: &[Value],
) -> Result<Value, EvalError> {
    let lv = eval_expr(left, row, params)?;
    // FALSE short-circuits regardless of the right operand.
    if matches!(lv, Value::Bool(false)) {
        return Ok(Value::Bool(false));
    }
    let rv = eval_expr(right, row, params)?;
    match (lv, rv) {
        (Value::Bool(false), _) | (_, Value::Bool(false)) => Ok(Value::Bool(false)),
        (Value::Bool(true), Value::Bool(true)) => Ok(Value::Bool(true)),
        (Value::Null, Value::Bool(true) | Value::Null) | (Value::Bool(true), Value::Null) => {
            Ok(Value::Null)
        }
        (l, r) => Err(EvalError::Type(format!(
            "AND requires boolean operands, got {l:?} AND {r:?}"
        ))),
    }
}

/// Kleene three-valued OR:
/// - `TRUE OR anything = TRUE`
/// - `FALSE OR x = x`
/// - `NULL OR TRUE = TRUE`, `NULL OR FALSE = NULL`
pub(crate) fn eval_or(
    left: &ScalarExpr,
    right: &ScalarExpr,
    row: &[Value],
    params: &[Value],
) -> Result<Value, EvalError> {
    let lv = eval_expr(left, row, params)?;
    // TRUE short-circuits regardless of the right operand.
    if matches!(lv, Value::Bool(true)) {
        return Ok(Value::Bool(true));
    }
    let rv = eval_expr(right, row, params)?;
    match (lv, rv) {
        (Value::Bool(true), _) | (_, Value::Bool(true)) => Ok(Value::Bool(true)),
        (Value::Bool(false), Value::Bool(false)) => Ok(Value::Bool(false)),
        (Value::Null, Value::Bool(false) | Value::Null) | (Value::Bool(false), Value::Null) => {
            Ok(Value::Null)
        }
        (l, r) => Err(EvalError::Type(format!(
            "OR requires boolean operands, got {l:?} OR {r:?}"
        ))),
    }
}

// ---------------------------------------------------------------------------
// Unary operators
// ---------------------------------------------------------------------------

pub(crate) fn apply_unary(op: UnaryOp, val: Value) -> Result<Value, EvalError> {
    match op {
        UnaryOp::Pos => {
            // `+x` is a no-op for all numeric types; propagates NULL.
            Ok(val)
        }

        UnaryOp::Neg => match val {
            Value::Null => Ok(Value::Null),
            Value::Int16(v) => v.checked_neg().map(Value::Int16).ok_or(EvalError::Overflow),
            Value::Int32(v) => v.checked_neg().map(Value::Int32).ok_or(EvalError::Overflow),
            Value::Int64(v) => v.checked_neg().map(Value::Int64).ok_or(EvalError::Overflow),
            Value::Float32(v) => Ok(Value::Float32(-v)),
            Value::Float64(v) => Ok(Value::Float64(-v)),
            Value::Money(v) => v.checked_neg().map(Value::Money).ok_or(EvalError::Overflow),
            other => Err(EvalError::Type(format!(
                "unary negation not defined for {other:?}"
            ))),
        },

        UnaryOp::Not => match val {
            Value::Null => Ok(Value::Null),
            Value::Bool(b) => Ok(Value::Bool(!b)),
            other => Err(EvalError::Type(format!(
                "NOT requires boolean operand, got {other:?}"
            ))),
        },

        UnaryOp::BitNot => match val {
            Value::Null => Ok(Value::Null),
            Value::Int16(v) => Ok(Value::Int16(!v)),
            Value::Int32(v) => Ok(Value::Int32(!v)),
            Value::Int64(v) => Ok(Value::Int64(!v)),
            Value::BitString(bits) => Ok(Value::BitString(bits.bit_not())),
            Value::Network(network) => Ok(Value::Network(network.bit_not())),
            other => Err(EvalError::Type(format!(
                "bitwise NOT (~) requires integer, bit string, or network operand, got {other:?}"
            ))),
        },
    }
}

// ---------------------------------------------------------------------------
// Binary operators
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_lines)]
pub(crate) fn apply_binary(op: BinaryOp, lv: Value, rv: Value) -> Result<Value, EvalError> {
    // NULL propagation for arithmetic and comparison ops.
    if matches!((&lv, &rv), (Value::Null, _) | (_, Value::Null)) {
        match op {
            BinaryOp::Add
            | BinaryOp::Sub
            | BinaryOp::Mul
            | BinaryOp::Div
            | BinaryOp::Mod
            | BinaryOp::Pow
            | BinaryOp::Eq
            | BinaryOp::NotEq
            | BinaryOp::Lt
            | BinaryOp::LtEq
            | BinaryOp::Gt
            | BinaryOp::GtEq
            | BinaryOp::VectorL2Distance
            | BinaryOp::VectorNegativeInnerProduct
            | BinaryOp::VectorCosineDistance
            | BinaryOp::VectorL1Distance
            | BinaryOp::Concat
            | BinaryOp::Like
            | BinaryOp::NotLike
            | BinaryOp::Ilike
            | BinaryOp::NotIlike
            | BinaryOp::RegexMatch
            | BinaryOp::RegexIMatch
            | BinaryOp::RegexNotMatch
            | BinaryOp::RegexNotIMatch
            | BinaryOp::BitAnd
            | BinaryOp::BitOr
            | BinaryOp::BitXor
            | BinaryOp::ShiftLeft
            | BinaryOp::ShiftRight
            | BinaryOp::NetworkContainedEq
            | BinaryOp::NetworkContainsEq
            | BinaryOp::JsonGet
            | BinaryOp::JsonGetText
            | BinaryOp::JsonGetPath
            | BinaryOp::JsonGetPathText
            | BinaryOp::JsonContains
            | BinaryOp::JsonContained
            | BinaryOp::Overlap
            | BinaryOp::JsonHasKey
            | BinaryOp::JsonHasAnyKey
            | BinaryOp::JsonHasAllKeys
            | BinaryOp::TextSearchMatch => return Ok(Value::Null),
            BinaryOp::And | BinaryOp::Or => return logical_op_dispatch_error(op),
        }
    }

    match op {
        // ------------------------------------------------------------------
        // Arithmetic
        // ------------------------------------------------------------------
        BinaryOp::Add => network_or_numeric_arith(lv, rv, ArithOp::Add),
        BinaryOp::Sub => network_or_numeric_arith(lv, rv, ArithOp::Sub),
        BinaryOp::Mul => numeric_arith(lv, rv, ArithOp::Mul),
        BinaryOp::Div => numeric_arith(lv, rv, ArithOp::Div),
        BinaryOp::Mod => numeric_arith(lv, rv, ArithOp::Mod),
        BinaryOp::Pow => numeric_arith(lv, rv, ArithOp::Pow),

        // ------------------------------------------------------------------
        // Comparison
        // ------------------------------------------------------------------
        BinaryOp::Eq => value_eq(&lv, &rv),
        BinaryOp::NotEq => value_not_eq(&lv, &rv),
        BinaryOp::Lt => value_compare(&lv, &rv, |c| c == std::cmp::Ordering::Less),
        BinaryOp::LtEq => value_compare(&lv, &rv, |c| {
            matches!(c, std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
        }),
        BinaryOp::Gt => value_compare(&lv, &rv, |c| c == std::cmp::Ordering::Greater),
        BinaryOp::GtEq => value_compare(&lv, &rv, |c| {
            matches!(c, std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
        }),

        // ------------------------------------------------------------------
        // String concatenation
        // ------------------------------------------------------------------
        BinaryOp::Concat => match (lv, rv) {
            (Value::BitString(l), Value::BitString(r)) => l
                .concat(&r)
                .map(Value::BitString)
                .ok_or(EvalError::Overflow),
            (Value::Text(l) | Value::Char(l), Value::Text(r) | Value::Char(r)) => {
                let mut s = l;
                s.push_str(&r);
                Ok(Value::Text(s))
            }
            (l, r) => Err(EvalError::Type(format!(
                "|| requires Text operands, got {l:?} and {r:?}"
            ))),
        },

        // ------------------------------------------------------------------
        // LIKE / NOT LIKE / ILIKE / NOT ILIKE
        // ------------------------------------------------------------------
        BinaryOp::Like | BinaryOp::NotLike | BinaryOp::Ilike | BinaryOp::NotIlike => {
            let case_insensitive = matches!(op, BinaryOp::Ilike | BinaryOp::NotIlike);
            let negated = matches!(op, BinaryOp::NotLike | BinaryOp::NotIlike);
            match (lv, rv) {
                (
                    Value::Text(haystack) | Value::Char(haystack),
                    Value::Text(pattern) | Value::Char(pattern),
                ) => {
                    let matched = like_match(&haystack, &pattern, case_insensitive);
                    Ok(Value::Bool(matched ^ negated))
                }
                (l, r) => Err(EvalError::Type(format!(
                    "LIKE requires Text operands, got {l:?} and {r:?}"
                ))),
            }
        }

        // ------------------------------------------------------------------
        // Bitwise integer operators
        // ------------------------------------------------------------------
        BinaryOp::BitAnd => bitwise_or_integer(lv, rv, BitStringOp::And, |a, b| a & b),
        BinaryOp::BitOr => bitwise_or_integer(lv, rv, BitStringOp::Or, |a, b| a | b),
        BinaryOp::BitXor => bitwise_or_integer(lv, rv, BitStringOp::Xor, |a, b| a ^ b),
        BinaryOp::ShiftLeft => shift_bit_string_or_integer(lv, rv, true),
        BinaryOp::ShiftRight => shift_bit_string_or_integer(lv, rv, false),
        BinaryOp::NetworkContainedEq => network_containment(lv, rv, false, true),
        BinaryOp::NetworkContainsEq => network_containment(lv, rv, true, true),

        // ------------------------------------------------------------------
        // Vector distance operators
        // ------------------------------------------------------------------
        BinaryOp::VectorL2Distance => vector_distance(&lv, &rv, VectorDistanceOp::L2),
        BinaryOp::VectorNegativeInnerProduct => {
            vector_distance(&lv, &rv, VectorDistanceOp::NegativeInnerProduct)
        }
        BinaryOp::VectorCosineDistance => vector_distance(&lv, &rv, VectorDistanceOp::Cosine),
        BinaryOp::VectorL1Distance => vector_distance(&lv, &rv, VectorDistanceOp::L1),

        // ------------------------------------------------------------------
        // Regex operators
        // ------------------------------------------------------------------
        BinaryOp::RegexMatch
        | BinaryOp::RegexIMatch
        | BinaryOp::RegexNotMatch
        | BinaryOp::RegexNotIMatch => {
            let case_insensitive = matches!(op, BinaryOp::RegexIMatch | BinaryOp::RegexNotIMatch);
            let negated = matches!(op, BinaryOp::RegexNotMatch | BinaryOp::RegexNotIMatch);
            match (lv, rv) {
                (
                    Value::Text(haystack) | Value::Char(haystack),
                    Value::Text(pattern) | Value::Char(pattern),
                ) => regex_match(&haystack, &pattern, case_insensitive)
                    .map(|matched| Value::Bool(matched ^ negated)),
                (l, r) => Err(EvalError::Type(format!(
                    "regex operators require Text operands, got {l:?} and {r:?}"
                ))),
            }
        }

        BinaryOp::JsonGet | BinaryOp::JsonGetPath => json_get(&lv, &rv, false),
        BinaryOp::JsonGetText | BinaryOp::JsonGetPathText => json_get(&lv, &rv, true),
        BinaryOp::JsonHasKey => json_has_key(&lv, &rv).map(Value::Bool),
        BinaryOp::JsonHasAnyKey => json_has_key_set(&lv, &rv, false).map(Value::Bool),
        BinaryOp::JsonHasAllKeys => json_has_key_set(&lv, &rv, true).map(Value::Bool),

        BinaryOp::JsonContains => contains_values(&lv, &rv)
            .map(Value::Bool)
            .ok_or_else(|| EvalError::Type(format!("@> not defined for {lv:?} and {rv:?}"))),
        BinaryOp::JsonContained => contains_values(&rv, &lv)
            .map(Value::Bool)
            .ok_or_else(|| EvalError::Type(format!("<@ not defined for {lv:?} and {rv:?}"))),
        BinaryOp::Overlap => overlaps_values(&lv, &rv)
            .map(Value::Bool)
            .ok_or_else(|| EvalError::Type(format!("&& not defined for {lv:?} and {rv:?}"))),
        BinaryOp::TextSearchMatch => text_search_match(&lv, &rv).map(Value::Bool),

        BinaryOp::And | BinaryOp::Or => logical_op_dispatch_error(op),
    }
}

pub(crate) fn logical_op_dispatch_error(op: BinaryOp) -> Result<Value, EvalError> {
    Err(EvalError::Type(format!(
        "{op:?} must be evaluated by the short-circuit path"
    )))
}
