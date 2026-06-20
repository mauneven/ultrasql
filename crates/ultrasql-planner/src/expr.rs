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
        /// 1-based parameter index carried from the parser.
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

    /// Builtin scalar function call: `extract(unit, date)`,
    /// `substring(text, from, for)`, etc. The binder resolves the
    /// function name to one of the variants the executor knows
    /// about; unknown names fail at bind time.
    FunctionCall {
        /// Normalised lower-case function name.
        name: String,
        /// Bound argument list.
        args: Vec<Self>,
        /// Statically inferred return type.
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
            | Self::ScalarSubquery { data_type, .. }
            | Self::FunctionCall { data_type, .. } => data_type.clone(),
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
            Self::FunctionCall { args, .. } => args.iter().any(Self::contains_outer_column),
        }
    }

    /// Returns `true` if this expression contains a scalar subquery, `EXISTS`,
    /// or `IN`-subquery anywhere in its scalar tree (it does not recurse into
    /// the nested subplans). The binder uses this to detect a non-order-
    /// preserving projection: decorrelation rewrites such a subquery into a
    /// join, so a `Sort` pushed *below* the projection would be discarded and
    /// `ORDER BY` silently violated.
    #[must_use]
    pub fn contains_subquery(&self) -> bool {
        match self {
            Self::ScalarSubquery { .. } | Self::Exists { .. } | Self::InSubquery { .. } => true,
            Self::Column { .. }
            | Self::Literal { .. }
            | Self::Parameter { .. }
            | Self::OuterColumn { .. } => false,
            Self::Unary { expr, .. } | Self::IsNull { expr, .. } => expr.contains_subquery(),
            Self::Binary { left, right, .. } => {
                left.contains_subquery() || right.contains_subquery()
            }
            Self::FunctionCall { args, .. } => args.iter().any(Self::contains_subquery),
        }
    }
}

impl fmt::Display for ScalarExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Column { name, .. } => f.write_str(name),
            Self::Literal { value, .. } => write!(f, "{value}"),
            Self::Parameter { index, .. } => write!(f, "${index}"),
            Self::Unary { op, expr, .. } => {
                // `display_unary` returns the bare keyword; word operators
                // (`NOT`) need a separating space before the operand, prefix
                // operators (`-`, `+`, `~`) attach directly.
                let sep = if matches!(op, UnaryOp::Not) { " " } else { "" };
                write!(f, "{}{sep}{expr}", display_unary(*op))
            }
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
            Self::FunctionCall { name, args, .. } => {
                write!(f, "{name}(")?;
                for (i, arg) in args.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{arg}")?;
                }
                write!(f, ")")
            }
        }
    }
}

/// Renders a [`UnaryOp`] as its bare SQL keyword/symbol.
///
/// Word operators (`NOT`) carry no trailing separator; callers that splice
/// the operator before an operand are responsible for inserting the space.
pub(crate) const fn display_unary(op: UnaryOp) -> &'static str {
    match op {
        UnaryOp::Neg => "-",
        UnaryOp::Pos => "+",
        UnaryOp::Not => "NOT",
        UnaryOp::BitNot => "~",
    }
}

/// Renders a [`BinaryOp`] as its SQL operator token.
#[allow(clippy::too_many_lines)]
pub(crate) const fn display_binary(op: BinaryOp) -> &'static str {
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

    fn lit(value: i32) -> ScalarExpr {
        ScalarExpr::Literal {
            value: Value::Int32(value),
            data_type: DataType::Int32,
        }
    }

    fn empty_plan_with_field(name: &str, data_type: DataType) -> LogicalPlan {
        LogicalPlan::Empty {
            schema: ultrasql_core::Schema::new([ultrasql_core::Field::nullable(name, data_type)])
                .expect("schema"),
        }
    }

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

    #[test]
    fn data_type_accessor_covers_subquery_and_function_variants() {
        let subplan = empty_plan_with_field("v", DataType::Float64);
        let variants = [
            ScalarExpr::Column {
                name: "v".into(),
                index: 0,
                data_type: DataType::Float64,
            },
            ScalarExpr::Parameter {
                index: 1,
                data_type: DataType::Text { max_len: None },
            },
            ScalarExpr::Unary {
                op: UnaryOp::Neg,
                expr: Box::new(lit(1)),
                data_type: DataType::Int32,
            },
            ScalarExpr::OuterColumn {
                name: "outer_v".into(),
                frame_depth: 1,
                column_index: 0,
                data_type: DataType::Int64,
            },
            ScalarExpr::ScalarSubquery {
                subplan: Box::new(subplan.clone()),
                correlated: false,
                data_type: DataType::Float64,
            },
            ScalarExpr::FunctionCall {
                name: "lower".into(),
                args: vec![],
                data_type: DataType::Text { max_len: None },
            },
        ];
        assert_eq!(variants[0].data_type(), DataType::Float64);
        assert_eq!(variants[1].data_type(), DataType::Text { max_len: None });
        assert_eq!(variants[2].data_type(), DataType::Int32);
        assert_eq!(variants[3].data_type(), DataType::Int64);
        assert_eq!(variants[4].data_type(), DataType::Float64);
        assert_eq!(variants[5].data_type(), DataType::Text { max_len: None });

        assert_eq!(
            ScalarExpr::Exists {
                subplan: Box::new(subplan.clone()),
                negated: false,
                correlated: false,
            }
            .data_type(),
            DataType::Bool
        );
        assert_eq!(
            ScalarExpr::InSubquery {
                expr: Box::new(lit(1)),
                subplan: Box::new(subplan),
                negated: false,
                correlated: false,
                data_type: DataType::Int32,
            }
            .data_type(),
            DataType::Bool
        );
    }

    #[test]
    fn contains_outer_column_recurses_through_scalar_children_only() {
        let outer = ScalarExpr::OuterColumn {
            name: "id".into(),
            frame_depth: 1,
            column_index: 0,
            data_type: DataType::Int32,
        };
        assert!(
            ScalarExpr::Unary {
                op: UnaryOp::Neg,
                expr: Box::new(outer.clone()),
                data_type: DataType::Int32,
            }
            .contains_outer_column()
        );
        assert!(
            ScalarExpr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(lit(1)),
                right: Box::new(outer.clone()),
                data_type: DataType::Bool,
            }
            .contains_outer_column()
        );
        assert!(
            ScalarExpr::FunctionCall {
                name: "abs".into(),
                args: vec![outer],
                data_type: DataType::Int32,
            }
            .contains_outer_column()
        );
        assert!(
            !ScalarExpr::ScalarSubquery {
                subplan: Box::new(empty_plan_with_field("id", DataType::Int32)),
                correlated: true,
                data_type: DataType::Int32,
            }
            .contains_outer_column()
        );
    }

    #[test]
    fn display_renders_every_expression_shape() {
        let plan = empty_plan_with_field("v", DataType::Int32);
        assert_eq!(
            ScalarExpr::Column {
                name: "id".into(),
                index: 0,
                data_type: DataType::Int32,
            }
            .to_string(),
            "id"
        );
        assert_eq!(
            ScalarExpr::Parameter {
                index: 3,
                data_type: DataType::Int32,
            }
            .to_string(),
            "$3"
        );
        assert_eq!(
            ScalarExpr::Unary {
                op: UnaryOp::Not,
                expr: Box::new(ScalarExpr::IsNull {
                    expr: Box::new(lit(1)),
                    negated: true,
                }),
                data_type: DataType::Bool,
            }
            .to_string(),
            "NOT (1 IS NOT NULL)"
        );
        assert_eq!(
            ScalarExpr::OuterColumn {
                name: "id".into(),
                frame_depth: 2,
                column_index: 0,
                data_type: DataType::Int32,
            }
            .to_string(),
            "outer[2].id"
        );
        assert_eq!(
            ScalarExpr::ScalarSubquery {
                subplan: Box::new(plan.clone()),
                correlated: true,
                data_type: DataType::Int32,
            }
            .to_string(),
            "(SUBQUERY[correlated])"
        );
        assert_eq!(
            ScalarExpr::Exists {
                subplan: Box::new(plan.clone()),
                negated: true,
                correlated: true,
            }
            .to_string(),
            "NOT EXISTS[correlated]"
        );
        assert_eq!(
            ScalarExpr::InSubquery {
                expr: Box::new(lit(1)),
                subplan: Box::new(plan),
                negated: true,
                correlated: false,
                data_type: DataType::Int32,
            }
            .to_string(),
            "(1 NOT IN SUBQUERY[uncorrelated])"
        );
        assert_eq!(
            ScalarExpr::FunctionCall {
                name: "coalesce".into(),
                args: vec![lit(1), lit(2)],
                data_type: DataType::Int32,
            }
            .to_string(),
            "coalesce(1, 2)"
        );
    }

    #[test]
    fn display_renders_all_binary_operator_tokens() {
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
            let expr = ScalarExpr::Binary {
                op,
                left: Box::new(lit(1)),
                right: Box::new(lit(2)),
                data_type: DataType::Bool,
            };
            assert_eq!(expr.to_string(), format!("(1 {token} 2)"));
        }
    }
}
