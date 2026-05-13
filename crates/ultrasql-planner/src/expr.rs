//! Bound scalar expressions.
//!
//! [`ScalarExpr`] is the post-binding mirror of the parser's `Expr`. Every
//! node carries its inferred [`DataType`] so downstream consumers (the
//! optimizer, the executor, EXPLAIN) never re-derive types. Column
//! references are resolved to 0-based indices into the producing
//! operator's schema; the original column name is retained for display.
//!
//! The unary and binary operator enums are re-exported from
//! [`ultrasql_parser::ast`] verbatim — the parser already produced the
//! right discrimination and we have nothing to add at this layer.

use std::fmt;

use ultrasql_core::{DataType, Value};

pub use ultrasql_parser::ast::{BinaryOp, UnaryOp};

/// A typed, bound scalar expression.
///
/// Each node carries a `DataType` that names the result type. The
/// binder enforces this invariant; downstream code may rely on it
/// without re-checking.
#[derive(Clone, Debug, PartialEq)]
pub enum ScalarExpr {
    /// Reference to a column in the input operator's output schema.
    Column {
        /// Output name as printed by EXPLAIN. Case-folded except where
        /// the source identifier was double-quoted.
        name: String,
        /// 0-based index into the input schema.
        index: usize,
        /// Inferred type.
        data_type: DataType,
    },

    /// Constant value.
    Literal {
        /// Owned runtime value.
        value: Value,
        /// The literal's logical type (NOT necessarily the value's
        /// dynamic type; NULL literals carry `DataType::Null`).
        data_type: DataType,
    },

    /// Positional `$N` parameter. The type is `Null` until a
    /// `BindParameter` rewrite assigns one; we keep the field explicit
    /// so the variant has the same shape as its peers.
    Parameter {
        /// 1-based parameter index, copied from the parser.
        index: u32,
        /// Placeholder type — `DataType::Null` until bound.
        data_type: DataType,
    },

    /// Unary operator application.
    Unary {
        /// Operator.
        op: UnaryOp,
        /// Operand.
        expr: Box<Self>,
        /// Result type.
        data_type: DataType,
    },

    /// Binary operator application.
    Binary {
        /// Operator.
        op: BinaryOp,
        /// Left operand.
        left: Box<Self>,
        /// Right operand.
        right: Box<Self>,
        /// Result type.
        data_type: DataType,
    },

    /// `expr IS [NOT] NULL`. The result is always `Bool`, so we do not
    /// carry a `data_type` field here.
    IsNull {
        /// Operand.
        expr: Box<Self>,
        /// `true` for `IS NOT NULL`.
        negated: bool,
    },
}

impl ScalarExpr {
    /// The static result type of this expression. `IsNull` always
    /// reports `Bool`.
    #[must_use]
    pub fn data_type(&self) -> DataType {
        match self {
            Self::Column { data_type, .. }
            | Self::Literal { data_type, .. }
            | Self::Parameter { data_type, .. }
            | Self::Unary { data_type, .. }
            | Self::Binary { data_type, .. } => data_type.clone(),
            Self::IsNull { .. } => DataType::Bool,
        }
    }
}

impl fmt::Display for ScalarExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Column { name, .. } => f.write_str(name),
            Self::Literal { value, .. } => write!(f, "{value}"),
            Self::Parameter { index, .. } => write!(f, "${index}"),
            Self::Unary { op, expr, .. } => write!(f, "{}{expr}", display_unary(*op)),
            Self::Binary {
                op, left, right, ..
            } => write!(f, "({left} {} {right})", display_binary(*op)),
            Self::IsNull { expr, negated } => {
                let kw = if *negated { "IS NOT NULL" } else { "IS NULL" };
                write!(f, "({expr} {kw})")
            }
        }
    }
}

const fn display_unary(op: UnaryOp) -> &'static str {
    match op {
        UnaryOp::Neg => "-",
        UnaryOp::Pos => "+",
        UnaryOp::Not => "NOT ",
        UnaryOp::BitNot => "~",
    }
}

#[allow(clippy::too_many_lines)]
const fn display_binary(op: BinaryOp) -> &'static str {
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
        BinaryOp::JsonHasKey => "?",
        BinaryOp::JsonHasAnyKey => "?|",
        BinaryOp::JsonHasAllKeys => "?&",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn data_type_accessor_returns_carried_type() {
        let lit = ScalarExpr::Literal {
            value: Value::Int32(7),
            data_type: DataType::Int32,
        };
        assert_eq!(lit.data_type(), DataType::Int32);

        let isnull = ScalarExpr::IsNull {
            expr: Box::new(lit),
            negated: false,
        };
        assert_eq!(isnull.data_type(), DataType::Bool);
    }

    #[test]
    fn display_renders_binary_op() {
        let e = ScalarExpr::Binary {
            op: BinaryOp::Add,
            left: Box::new(ScalarExpr::Literal {
                value: Value::Int32(1),
                data_type: DataType::Int32,
            }),
            right: Box::new(ScalarExpr::Literal {
                value: Value::Int32(2),
                data_type: DataType::Int32,
            }),
            data_type: DataType::Int32,
        };
        assert_eq!(e.to_string(), "(1 + 2)");
    }
}
