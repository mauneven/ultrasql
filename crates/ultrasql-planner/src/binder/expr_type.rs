//! Type-inference helpers for scalar expressions.
//! Extracted from `expr_bind.rs` to keep each file under the 600-line ceiling.

use ultrasql_core::DataType;
use ultrasql_parser::ast::{BinaryOp, UnaryOp};

use super::PlanError;

/// Compute the result type of a binary operator applied to two operand types.
#[allow(clippy::too_many_lines)]
pub(super) fn binary_result_type(
    op: BinaryOp,
    lt: DataType,
    rt: DataType,
) -> Result<DataType, PlanError> {
    match op {
        BinaryOp::Add
        | BinaryOp::Sub
        | BinaryOp::Mul
        | BinaryOp::Div
        | BinaryOp::Mod
        | BinaryOp::Pow => {
            if matches!(lt, DataType::Null) {
                Ok(rt)
            } else if matches!(rt, DataType::Null) {
                Ok(lt)
            } else if let Some(decimal_type) = decimal_arithmetic_type(op, &lt, &rt) {
                Ok(decimal_type)
            } else {
                lt.numeric_join(&rt).map_err(|_| {
                    PlanError::TypeMismatch(format!(
                        "arithmetic operator {} on incompatible types {lt} and {rt}",
                        display_binary(op)
                    ))
                })
            }
        }
        BinaryOp::Concat => {
            if (lt.is_textlike() || matches!(lt, DataType::Null))
                && (rt.is_textlike() || matches!(rt, DataType::Null))
            {
                Ok(DataType::Text { max_len: None })
            } else {
                Err(PlanError::TypeMismatch(format!(
                    "string concatenation requires text operands, got {lt} and {rt}"
                )))
            }
        }
        BinaryOp::Eq
        | BinaryOp::NotEq
        | BinaryOp::Lt
        | BinaryOp::LtEq
        | BinaryOp::Gt
        | BinaryOp::GtEq => {
            if comparable(&lt, &rt) {
                Ok(DataType::Bool)
            } else {
                Err(PlanError::TypeMismatch(format!(
                    "cannot compare {lt} and {rt}"
                )))
            }
        }
        BinaryOp::And | BinaryOp::Or => {
            if matches!(lt, DataType::Bool | DataType::Null)
                && matches!(rt, DataType::Bool | DataType::Null)
            {
                Ok(DataType::Bool)
            } else {
                Err(PlanError::TypeMismatch(format!(
                    "{} requires boolean operands, got {lt} and {rt}",
                    display_binary(op)
                )))
            }
        }
        BinaryOp::Like
        | BinaryOp::NotLike
        | BinaryOp::Ilike
        | BinaryOp::NotIlike
        | BinaryOp::RegexMatch
        | BinaryOp::RegexIMatch
        | BinaryOp::RegexNotMatch
        | BinaryOp::RegexNotIMatch => {
            if (lt.is_textlike() || matches!(lt, DataType::Null))
                && (rt.is_textlike() || matches!(rt, DataType::Null))
            {
                Ok(DataType::Bool)
            } else {
                Err(PlanError::TypeMismatch(format!(
                    "{} requires text operands, got {lt} and {rt}",
                    display_binary(op)
                )))
            }
        }
        BinaryOp::BitAnd
        | BinaryOp::BitOr
        | BinaryOp::BitXor
        | BinaryOp::ShiftLeft
        | BinaryOp::ShiftRight => {
            if matches!(lt, DataType::Null) {
                Ok(rt)
            } else if matches!(rt, DataType::Null) {
                Ok(lt)
            } else if lt.is_integer() && rt.is_integer() {
                lt.numeric_join(&rt).map_err(|_| {
                    PlanError::TypeMismatch(format!(
                        "bitwise operator {} on incompatible types {lt} and {rt}",
                        display_binary(op)
                    ))
                })
            } else {
                Err(PlanError::TypeMismatch(format!(
                    "bitwise operator {} requires integer operands, got {lt} and {rt}",
                    display_binary(op)
                )))
            }
        }
        BinaryOp::JsonGet | BinaryOp::JsonGetPath => Ok(DataType::Jsonb),
        BinaryOp::JsonGetText | BinaryOp::JsonGetPathText => Ok(DataType::Text { max_len: None }),
        BinaryOp::JsonContains
        | BinaryOp::JsonContained
        | BinaryOp::Overlap
        | BinaryOp::JsonHasKey
        | BinaryOp::JsonHasAnyKey
        | BinaryOp::JsonHasAllKeys => Ok(DataType::Bool),
    }
}

fn decimal_arithmetic_type(op: BinaryOp, lt: &DataType, rt: &DataType) -> Option<DataType> {
    if !matches!(lt, DataType::Decimal { .. }) && !matches!(rt, DataType::Decimal { .. }) {
        return None;
    }
    if !lt.is_numeric() || !rt.is_numeric() {
        return None;
    }
    if lt.is_float() || rt.is_float() {
        return Some(DataType::Float64);
    }
    let scale = match op {
        BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mod => {
            max_optional_scale(decimal_operand_scale(lt), decimal_operand_scale(rt))
        }
        BinaryOp::Mul => add_optional_scale(decimal_operand_scale(lt), decimal_operand_scale(rt)),
        BinaryOp::Div => max_optional_scale(
            max_optional_scale(decimal_operand_scale(lt), decimal_operand_scale(rt)),
            Some(6),
        ),
        BinaryOp::Pow => None,
        _ => None,
    };
    Some(DataType::Decimal {
        precision: None,
        scale,
    })
}

fn decimal_operand_scale(data_type: &DataType) -> Option<i32> {
    match data_type {
        DataType::Decimal { scale, .. } => *scale,
        ty if ty.is_integer() => Some(0),
        _ => None,
    }
}

fn max_optional_scale(left: Option<i32>, right: Option<i32>) -> Option<i32> {
    match (left, right) {
        (Some(l), Some(r)) => Some(l.max(r)),
        _ => None,
    }
}

fn add_optional_scale(left: Option<i32>, right: Option<i32>) -> Option<i32> {
    match (left, right) {
        (Some(l), Some(r)) => l.checked_add(r),
        _ => None,
    }
}

pub(super) fn comparable(a: &DataType, b: &DataType) -> bool {
    if matches!(a, DataType::Null) || matches!(b, DataType::Null) {
        return true;
    }
    if a == b {
        return true;
    }
    if a.is_numeric() && b.is_numeric() {
        return true;
    }
    if a.is_textlike() && b.is_textlike() {
        return true;
    }
    if a.is_temporal() && b.is_temporal() {
        return true;
    }
    false
}

pub(super) const fn display_unary(op: UnaryOp) -> &'static str {
    match op {
        UnaryOp::Neg => "-",
        UnaryOp::Pos => "+",
        UnaryOp::Not => "NOT",
        UnaryOp::BitNot => "~",
    }
}

pub(super) const fn display_binary(op: BinaryOp) -> &'static str {
    match op {
        BinaryOp::Add => "+",
        BinaryOp::Sub => "-",
        BinaryOp::Mul => "*",
        BinaryOp::Div => "/",
        BinaryOp::Mod => "%",
        BinaryOp::Pow => "^",
        BinaryOp::Concat => "||",
        BinaryOp::Eq => "=",
        BinaryOp::NotEq => "<>",
        BinaryOp::Lt => "<",
        BinaryOp::LtEq => "<=",
        BinaryOp::Gt => ">",
        BinaryOp::GtEq => ">=",
        BinaryOp::And => "AND",
        BinaryOp::Or => "OR",
        BinaryOp::Like => "LIKE",
        BinaryOp::NotLike => "NOT LIKE",
        BinaryOp::Ilike => "ILIKE",
        BinaryOp::NotIlike => "NOT ILIKE",
        BinaryOp::RegexMatch => "~",
        BinaryOp::RegexIMatch => "~*",
        BinaryOp::RegexNotMatch => "!~",
        BinaryOp::RegexNotIMatch => "!~*",
        BinaryOp::BitAnd => "&",
        BinaryOp::BitOr => "|",
        BinaryOp::BitXor => "#",
        BinaryOp::ShiftLeft => "<<",
        BinaryOp::ShiftRight => ">>",
        BinaryOp::JsonGet => "->",
        BinaryOp::JsonGetText => "->>",
        BinaryOp::JsonGetPath => "#>",
        BinaryOp::JsonGetPathText => "#>>",
        BinaryOp::JsonContains => "@>",
        BinaryOp::JsonContained => "<@",
        BinaryOp::Overlap => "&&",
        BinaryOp::JsonHasKey => "?",
        BinaryOp::JsonHasAnyKey => "?|",
        BinaryOp::JsonHasAllKeys => "?&",
    }
}
