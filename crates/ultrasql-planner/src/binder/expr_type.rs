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
        BinaryOp::Add | BinaryOp::Mul | BinaryOp::Div | BinaryOp::Mod | BinaryOp::Pow => {
            if matches!(op, BinaryOp::Add) && network_integer_add_type(&lt, &rt).is_some() {
                return Ok(DataType::Inet);
            }
            if let Some(money_type) = money_arithmetic_type(op, &lt, &rt) {
                return Ok(money_type);
            }
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
        BinaryOp::Sub => {
            if lt.is_ip_network() && rt.is_integer() {
                Ok(DataType::Inet)
            } else if lt.is_ip_network() && rt.is_ip_network() {
                Ok(DataType::Int64)
            } else if let Some(money_type) = money_arithmetic_type(op, &lt, &rt) {
                Ok(money_type)
            } else if matches!(lt, DataType::Null) {
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
            if (lt.is_bit_string() || matches!(lt, DataType::Null))
                && (rt.is_bit_string() || matches!(rt, DataType::Null))
            {
                Ok(DataType::VarBit { max_len: None })
            } else if (lt.is_textlike() || matches!(lt, DataType::Null))
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
            if is_vector_comparison_operand(&lt, &rt) {
                return Err(PlanError::TypeMismatch(format!(
                    "vector comparison operator {} is not supported; use <->, <#>, <=>, <+>, or vector distance functions",
                    display_binary(op)
                )));
            }
            if comparable(&lt, &rt) {
                Ok(DataType::Bool)
            } else {
                Err(PlanError::TypeMismatch(format!(
                    "cannot compare {lt} and {rt}"
                )))
            }
        }
        BinaryOp::VectorL2Distance
        | BinaryOp::VectorNegativeInnerProduct
        | BinaryOp::VectorCosineDistance
        | BinaryOp::VectorL1Distance => {
            if vector_operands_compatible(&lt, &rt) {
                Ok(DataType::Float64)
            } else {
                Err(PlanError::TypeMismatch(format!(
                    "vector operator {} requires compatible vector operands, got {lt} and {rt}",
                    display_binary(op)
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
        BinaryOp::BitAnd | BinaryOp::BitOr | BinaryOp::BitXor => {
            if matches!(lt, DataType::Null) {
                Ok(rt)
            } else if matches!(rt, DataType::Null) {
                Ok(lt)
            } else if lt.is_network_address() && rt.is_network_address() {
                if lt.is_ip_network() && rt.is_ip_network() {
                    Ok(DataType::Inet)
                } else if lt == rt {
                    Ok(lt)
                } else {
                    Err(PlanError::TypeMismatch(format!(
                        "network bitwise operator {} requires matching address families, got {lt} and {rt}",
                        display_binary(op)
                    )))
                }
            } else if lt.is_bit_string() && rt.is_bit_string() {
                Ok(DataType::VarBit { max_len: None })
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
        BinaryOp::ShiftLeft | BinaryOp::ShiftRight => {
            if matches!(lt, DataType::Null) {
                Ok(rt)
            } else if matches!(rt, DataType::Null) {
                Ok(lt)
            } else if lt.is_ip_network() && rt.is_ip_network() {
                Ok(DataType::Bool)
            } else if lt.is_bit_string() && rt.is_integer() {
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
                    "bitwise operator {} requires bit string/integer operands, got {lt} and {rt}",
                    display_binary(op)
                )))
            }
        }
        BinaryOp::NetworkContainedEq | BinaryOp::NetworkContainsEq => {
            if (lt.is_ip_network() || matches!(lt, DataType::Null))
                && (rt.is_ip_network() || matches!(rt, DataType::Null))
            {
                Ok(DataType::Bool)
            } else {
                Err(PlanError::TypeMismatch(format!(
                    "network operator {} requires inet/cidr operands, got {lt} and {rt}",
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
        | BinaryOp::JsonHasAllKeys
        | BinaryOp::TextSearchMatch => Ok(DataType::Bool),
    }
}

fn money_arithmetic_type(op: BinaryOp, left: &DataType, right: &DataType) -> Option<DataType> {
    match (op, left, right) {
        (BinaryOp::Add | BinaryOp::Sub, DataType::Money, DataType::Money) => Some(DataType::Money),
        (BinaryOp::Div, DataType::Money, DataType::Money) => Some(DataType::Float64),
        (BinaryOp::Div, DataType::Money, ty) if ty.is_integer() => Some(DataType::Money),
        _ => None,
    }
}

fn network_integer_add_type(left: &DataType, right: &DataType) -> Option<DataType> {
    ((left.is_ip_network() && right.is_integer()) || (left.is_integer() && right.is_ip_network()))
        .then_some(DataType::Inet)
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
    if a.is_oid_alias() && b.is_oid_alias() {
        return true;
    }
    if (a.is_oid_alias() && b.is_integer()) || (a.is_integer() && b.is_oid_alias()) {
        return true;
    }
    if a.is_textlike() && b.is_textlike() {
        return true;
    }
    if a.is_bit_string() && b.is_bit_string() {
        return true;
    }
    if a.is_network_address() && b.is_network_address() {
        return (a.is_ip_network() && b.is_ip_network()) || a == b;
    }
    if a.is_temporal() && b.is_temporal() {
        return true;
    }
    false
}

const fn is_vector_comparison_operand(left: &DataType, right: &DataType) -> bool {
    left.is_vector_family() || right.is_vector_family()
}

fn vector_operands_compatible(left: &DataType, right: &DataType) -> bool {
    match (left, right) {
        (DataType::Null, right) | (right, DataType::Null)
            if vector_metric_family_kind(right).is_some() =>
        {
            true
        }
        (left, right)
            if vector_metric_family_kind(left).is_some()
                && vector_metric_family_kind(right).is_some() =>
        {
            vector_metric_family_kind(left) == vector_metric_family_kind(right)
                && dims_compatible(left.vector_dims().flatten(), right.vector_dims().flatten())
        }
        _ => false,
    }
}

fn vector_metric_family_kind(data_type: &DataType) -> Option<u8> {
    match data_type {
        DataType::Vector { .. } => Some(0),
        DataType::HalfVec { .. } => Some(1),
        DataType::SparseVec { .. } => Some(2),
        DataType::BitVec { .. } => None,
        _ => None,
    }
}

const fn dims_compatible(left: Option<u32>, right: Option<u32>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => left == right,
        _ => true,
    }
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
        BinaryOp::VectorL2Distance => "<->",
        BinaryOp::VectorNegativeInnerProduct => "<#>",
        BinaryOp::VectorCosineDistance => "<=>",
        BinaryOp::VectorL1Distance => "<+>",
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
        BinaryOp::NetworkContainedEq => "<<=",
        BinaryOp::NetworkContainsEq => ">>=",
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
        BinaryOp::TextSearchMatch => "@@",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decimal(scale: Option<i32>) -> DataType {
        DataType::Decimal {
            precision: None,
            scale,
        }
    }

    #[test]
    fn binary_result_type_covers_numeric_decimal_and_network_arithmetic() {
        assert_eq!(
            binary_result_type(BinaryOp::Add, DataType::Int32, DataType::Int64).expect("int add"),
            DataType::Int64
        );
        assert_eq!(
            binary_result_type(BinaryOp::Add, DataType::Inet, DataType::Int32).expect("inet add"),
            DataType::Inet
        );
        assert_eq!(
            binary_result_type(BinaryOp::Sub, DataType::Inet, DataType::Cidr).expect("inet diff"),
            DataType::Int64
        );
        assert_eq!(
            binary_result_type(BinaryOp::Add, decimal(Some(2)), DataType::Int32)
                .expect("decimal add"),
            decimal(Some(2))
        );
        assert_eq!(
            binary_result_type(BinaryOp::Mul, decimal(Some(2)), decimal(Some(3)))
                .expect("decimal mul"),
            decimal(Some(5))
        );
        assert_eq!(
            binary_result_type(BinaryOp::Div, decimal(Some(2)), decimal(Some(3)))
                .expect("decimal div"),
            decimal(Some(6))
        );
        assert_eq!(
            binary_result_type(BinaryOp::Pow, decimal(Some(2)), DataType::Int32)
                .expect("decimal pow"),
            decimal(None)
        );
        assert_eq!(
            binary_result_type(BinaryOp::Add, decimal(Some(2)), DataType::Float64)
                .expect("decimal float"),
            DataType::Float64
        );
        assert!(
            binary_result_type(
                BinaryOp::Add,
                DataType::Text { max_len: None },
                DataType::Int32
            )
            .is_err()
        );
    }

    #[test]
    fn binary_result_type_covers_text_bit_bool_and_comparison_families() {
        assert_eq!(
            binary_result_type(
                BinaryOp::Concat,
                DataType::Text { max_len: None },
                DataType::Char { len: Some(3) },
            )
            .expect("text concat"),
            DataType::Text { max_len: None }
        );
        assert_eq!(
            binary_result_type(
                BinaryOp::Concat,
                DataType::Bit { len: Some(3) },
                DataType::VarBit { max_len: None },
            )
            .expect("bit concat"),
            DataType::VarBit { max_len: None }
        );
        assert!(
            binary_result_type(
                BinaryOp::Concat,
                DataType::Int32,
                DataType::Text { max_len: None }
            )
            .is_err()
        );

        for op in [
            BinaryOp::Eq,
            BinaryOp::NotEq,
            BinaryOp::Lt,
            BinaryOp::LtEq,
            BinaryOp::Gt,
            BinaryOp::GtEq,
        ] {
            assert_eq!(
                binary_result_type(op, DataType::Int32, DataType::Float64).expect("compare"),
                DataType::Bool
            );
        }
        assert!(
            binary_result_type(
                BinaryOp::Eq,
                DataType::Vector { dims: Some(3) },
                DataType::Vector { dims: Some(3) },
            )
            .is_err()
        );
        assert!(binary_result_type(BinaryOp::Eq, DataType::Jsonb, DataType::Int32).is_err());

        assert_eq!(
            binary_result_type(BinaryOp::And, DataType::Bool, DataType::Null).expect("bool"),
            DataType::Bool
        );
        assert!(binary_result_type(BinaryOp::Or, DataType::Bool, DataType::Int32).is_err());
        assert_eq!(
            binary_result_type(
                BinaryOp::Like,
                DataType::Text { max_len: None },
                DataType::Text { max_len: None },
            )
            .expect("like"),
            DataType::Bool
        );
        assert!(
            binary_result_type(
                BinaryOp::RegexMatch,
                DataType::Int32,
                DataType::Text { max_len: None }
            )
            .is_err()
        );
    }

    #[test]
    fn binary_result_type_covers_vector_bitwise_shift_json_and_search_ops() {
        assert_eq!(
            binary_result_type(
                BinaryOp::VectorL2Distance,
                DataType::Vector { dims: Some(3) },
                DataType::Vector { dims: Some(3) },
            )
            .expect("vector l2"),
            DataType::Float64
        );
        assert!(
            binary_result_type(
                BinaryOp::VectorCosineDistance,
                DataType::Vector { dims: Some(3) },
                DataType::HalfVec { dims: Some(3) },
            )
            .is_err()
        );
        assert!(
            binary_result_type(
                BinaryOp::VectorL1Distance,
                DataType::SparseVec { dims: Some(3) },
                DataType::SparseVec { dims: Some(4) },
            )
            .is_err()
        );

        assert_eq!(
            binary_result_type(BinaryOp::BitAnd, DataType::Inet, DataType::Cidr)
                .expect("network and"),
            DataType::Inet
        );
        assert_eq!(
            binary_result_type(BinaryOp::BitOr, DataType::MacAddr, DataType::MacAddr)
                .expect("mac or"),
            DataType::MacAddr
        );
        assert!(
            binary_result_type(BinaryOp::BitXor, DataType::MacAddr, DataType::MacAddr8).is_err()
        );
        assert_eq!(
            binary_result_type(
                BinaryOp::ShiftLeft,
                DataType::VarBit { max_len: Some(8) },
                DataType::Int32,
            )
            .expect("bit shift"),
            DataType::VarBit { max_len: Some(8) }
        );
        assert_eq!(
            binary_result_type(BinaryOp::ShiftRight, DataType::Inet, DataType::Cidr)
                .expect("network shift"),
            DataType::Bool
        );
        assert!(
            binary_result_type(
                BinaryOp::ShiftLeft,
                DataType::Text { max_len: None },
                DataType::Int32
            )
            .is_err()
        );

        assert_eq!(
            binary_result_type(BinaryOp::NetworkContainedEq, DataType::Inet, DataType::Cidr)
                .expect("network contains"),
            DataType::Bool
        );
        assert!(
            binary_result_type(
                BinaryOp::NetworkContainsEq,
                DataType::MacAddr,
                DataType::Inet
            )
            .is_err()
        );
        assert_eq!(
            binary_result_type(
                BinaryOp::JsonGet,
                DataType::Jsonb,
                DataType::Text { max_len: None }
            )
            .expect("json get"),
            DataType::Jsonb
        );
        assert_eq!(
            binary_result_type(
                BinaryOp::JsonGetText,
                DataType::Jsonb,
                DataType::Text { max_len: None },
            )
            .expect("json text"),
            DataType::Text { max_len: None }
        );
        for op in [
            BinaryOp::JsonContains,
            BinaryOp::JsonContained,
            BinaryOp::Overlap,
            BinaryOp::JsonHasKey,
            BinaryOp::JsonHasAnyKey,
            BinaryOp::JsonHasAllKeys,
            BinaryOp::TextSearchMatch,
        ] {
            assert_eq!(
                binary_result_type(op, DataType::Jsonb, DataType::Jsonb).expect("bool op"),
                DataType::Bool
            );
        }
    }

    #[test]
    fn displays_every_operator_token() {
        assert_eq!(display_unary(UnaryOp::Neg), "-");
        assert_eq!(display_unary(UnaryOp::Pos), "+");
        assert_eq!(display_unary(UnaryOp::Not), "NOT");
        assert_eq!(display_unary(UnaryOp::BitNot), "~");

        for (op, token) in [
            (BinaryOp::Add, "+"),
            (BinaryOp::Sub, "-"),
            (BinaryOp::Mul, "*"),
            (BinaryOp::Div, "/"),
            (BinaryOp::Mod, "%"),
            (BinaryOp::Pow, "^"),
            (BinaryOp::Concat, "||"),
            (BinaryOp::Eq, "="),
            (BinaryOp::NotEq, "<>"),
            (BinaryOp::Lt, "<"),
            (BinaryOp::LtEq, "<="),
            (BinaryOp::Gt, ">"),
            (BinaryOp::GtEq, ">="),
            (BinaryOp::VectorL2Distance, "<->"),
            (BinaryOp::VectorNegativeInnerProduct, "<#>"),
            (BinaryOp::VectorCosineDistance, "<=>"),
            (BinaryOp::VectorL1Distance, "<+>"),
            (BinaryOp::And, "AND"),
            (BinaryOp::Or, "OR"),
            (BinaryOp::Like, "LIKE"),
            (BinaryOp::NotLike, "NOT LIKE"),
            (BinaryOp::Ilike, "ILIKE"),
            (BinaryOp::NotIlike, "NOT ILIKE"),
            (BinaryOp::RegexMatch, "~"),
            (BinaryOp::RegexIMatch, "~*"),
            (BinaryOp::RegexNotMatch, "!~"),
            (BinaryOp::RegexNotIMatch, "!~*"),
            (BinaryOp::BitAnd, "&"),
            (BinaryOp::BitOr, "|"),
            (BinaryOp::BitXor, "#"),
            (BinaryOp::ShiftLeft, "<<"),
            (BinaryOp::ShiftRight, ">>"),
            (BinaryOp::NetworkContainedEq, "<<="),
            (BinaryOp::NetworkContainsEq, ">>="),
            (BinaryOp::JsonGet, "->"),
            (BinaryOp::JsonGetText, "->>"),
            (BinaryOp::JsonGetPath, "#>"),
            (BinaryOp::JsonGetPathText, "#>>"),
            (BinaryOp::JsonContains, "@>"),
            (BinaryOp::JsonContained, "<@"),
            (BinaryOp::Overlap, "&&"),
            (BinaryOp::JsonHasKey, "?"),
            (BinaryOp::JsonHasAnyKey, "?|"),
            (BinaryOp::JsonHasAllKeys, "?&"),
            (BinaryOp::TextSearchMatch, "@@"),
        ] {
            assert_eq!(display_binary(op), token);
        }
    }
}
