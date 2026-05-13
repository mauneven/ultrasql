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
//!
//! ## Subquery variants
//!
//! Three variants represent subqueries that remain in the expression tree
//! until the optimizer's decorrelation pass lowers them to joins:
//!
//! - [`ScalarExpr::ScalarSubquery`] — a `(SELECT single_col …)` used as a
//!   scalar value.  Must project exactly one column (enforced by the binder).
//! - [`ScalarExpr::Exists`] — `EXISTS (SELECT …)` / `NOT EXISTS (…)`.
//!   Result type is always `Bool`.
//! - [`ScalarExpr::InSubquery`] — `expr [NOT] IN (SELECT …)`.  Result type
//!   is always `Bool`.
//!
//! Correlated subqueries additionally reference outer columns via
//! [`ScalarExpr::OuterColumn`], whose `frame_depth` tells the optimizer how
//! many scope levels outward the reference escapes.

use std::fmt;

use ultrasql_core::{DataType, Value};

use crate::plan::LogicalPlan;

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

    /// A reference to a column in an *outer* query scope.
    ///
    /// Produced when column resolution inside a subquery fails against the
    /// subquery's own FROM clause but succeeds in one of the enclosing
    /// outer scopes.  The optimizer's decorrelation pass consumes these
    /// references when lifting the subquery into a join.
    ///
    /// `frame_depth` is 1-based: 1 means the immediately enclosing query,
    /// 2 means one level further out, etc.
    OuterColumn {
        /// Column name retained for EXPLAIN readability.
        name: String,
        /// How many scope levels outward this reference escapes.
        frame_depth: usize,
        /// 0-based index within the outer frame's schema.
        column_index: usize,
        /// Inferred type of the outer column.
        data_type: DataType,
    },

    /// A scalar subquery: `(SELECT single_column …)`.
    ///
    /// The binder enforces that the inner plan projects exactly one column.
    /// The result type equals that column's type.
    ///
    /// `correlated = true` when the subplan contains at least one
    /// [`ScalarExpr::OuterColumn`] reference.  Uncorrelated scalar
    /// subqueries may be evaluated once and memoised by the executor.
    ScalarSubquery {
        /// The bound inner plan (arity exactly 1).
        subplan: Box<LogicalPlan>,
        /// `true` when the subplan references at least one outer column.
        correlated: bool,
        /// Result type — identical to the single column's type in `subplan`.
        data_type: DataType,
    },

    /// `[NOT] EXISTS (SELECT …)`.
    ///
    /// Result type is always `Bool`.  `correlated = true` when the subplan
    /// contains at least one [`ScalarExpr::OuterColumn`].
    Exists {
        /// The bound inner plan.
        subplan: Box<LogicalPlan>,
        /// `true` for `NOT EXISTS`.
        negated: bool,
        /// `true` when the subplan references at least one outer column.
        correlated: bool,
    },

    /// `expr [NOT] IN (SELECT single_column …)`.
    ///
    /// The binder enforces that the inner plan projects exactly one column
    /// and that its type is comparable to `expr`'s type.  Result type is
    /// always `Bool`.
    InSubquery {
        /// The left-hand test expression (bound against the *outer* scope).
        expr: Box<Self>,
        /// The bound inner plan (arity exactly 1).
        subplan: Box<LogicalPlan>,
        /// `true` for `NOT IN`.
        negated: bool,
        /// `true` when the subplan references at least one outer column.
        correlated: bool,
        /// Type of the inner column — retained for the optimizer's type
        /// checking when building the join condition.
        data_type: DataType,
    },
}

impl ScalarExpr {
    /// The static result type of this expression.
    ///
    /// - `IsNull` / `Exists` / `InSubquery` always return `Bool`.
    /// - `ScalarSubquery` returns the type of the single projected column.
    /// - `OuterColumn` returns the outer column's type.
    #[must_use]
    pub fn data_type(&self) -> DataType {
        match self {
            Self::Column { data_type, .. }
            | Self::Literal { data_type, .. }
            | Self::Parameter { data_type, .. }
            | Self::Unary { data_type, .. }
            | Self::Binary { data_type, .. }
            | Self::OuterColumn { data_type, .. }
            | Self::ScalarSubquery { data_type, .. } => data_type.clone(),
            // IN/EXISTS/IS NULL always produce Bool.
            Self::IsNull { .. } | Self::Exists { .. } | Self::InSubquery { .. } => DataType::Bool,
        }
    }

    /// Returns `true` when this expression or any sub-expression is an
    /// [`ScalarExpr::OuterColumn`].
    ///
    /// Used by the binder to detect whether a subquery is correlated.
    #[must_use]
    pub fn contains_outer_column(&self) -> bool {
        match self {
            Self::OuterColumn { .. } => true,
            // Subquery variants: correlation of the *enclosing* query is
            // tracked independently; we do not recurse into nested subplans.
            Self::Column { .. }
            | Self::Literal { .. }
            | Self::Parameter { .. }
            | Self::ScalarSubquery { .. }
            | Self::Exists { .. }
            | Self::InSubquery { .. } => false,
            Self::Unary { expr, .. } | Self::IsNull { expr, .. } => expr.contains_outer_column(),
            Self::Binary { left, right, .. } => {
                left.contains_outer_column() || right.contains_outer_column()
            }
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
            Self::OuterColumn {
                name, frame_depth, ..
            } => write!(f, "outer[{frame_depth}].{name}"),
            Self::ScalarSubquery { correlated, .. } => {
                let tag = if *correlated { "correlated" } else { "scalar" };
                write!(f, "(SUBQUERY[{tag}])")
            }
            Self::Exists {
                negated,
                correlated,
                ..
            } => {
                let not = if *negated { "NOT " } else { "" };
                let tag = if *correlated {
                    "correlated"
                } else {
                    "uncorrelated"
                };
                write!(f, "{not}EXISTS[{tag}]")
            }
            Self::InSubquery {
                expr,
                negated,
                correlated,
                ..
            } => {
                let not = if *negated { " NOT" } else { "" };
                let tag = if *correlated {
                    "correlated"
                } else {
                    "uncorrelated"
                };
                write!(f, "({expr}{not} IN SUBQUERY[{tag}])")
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
