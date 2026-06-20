//! Legacy test-only lowering helpers.
//!
//! [`SubqueryKind`] and [`rewrite_filter`] implement the original
//! `LeftOuter`-join + `IS [NOT] NULL` lowering convention. Production code no
//! longer uses them; they survive only so unit tests can exercise that
//! convention directly via [`make_exists_filter`] / [`make_in_subquery_filter`]
//! without going through the binder.

#![cfg(test)]

use ultrasql_core::DataType;
use ultrasql_planner::{BinaryOp, LogicalJoinCondition, LogicalJoinType, LogicalPlan, ScalarExpr};

use super::helpers::concat_schemas;

/// The shape of a subquery predicate extracted from a `Filter` node.
///
/// Variants are constructed by legacy unit-test helpers to exercise the
/// original lowering convention directly. Production rewrites consume real
/// [`ScalarExpr::Exists`], [`ScalarExpr::InSubquery`], and
/// [`ScalarExpr::ScalarSubquery`] nodes.
#[derive(Debug)]
#[cfg(test)]
enum SubqueryKind {
    /// `EXISTS(sub)` — semi-join semantics.
    Exists {
        sub: Box<LogicalPlan>,
        negated: bool,
    },
    /// `expr IN (SELECT col FROM sub)` — semi-join on equality.
    InSubquery {
        outer_expr: Box<ScalarExpr>,
        inner_col: Box<ScalarExpr>,
        sub: Box<LogicalPlan>,
        negated: bool,
    },
}

/// Given a `Filter(input, subquery_pred)`, rewrite to a `LeftOuter` join
/// followed by an `IS [NOT] NULL` filter.
#[cfg(test)]
fn rewrite_filter(outer: &LogicalPlan, kind: SubqueryKind) -> Option<LogicalPlan> {
    match kind {
        SubqueryKind::Exists { sub, negated } => {
            // Pick the first column of the subquery schema as the sentinel.
            let sub_schema = sub.schema();
            if sub_schema.is_empty() {
                return None;
            }
            let sub_col_dt = sub_schema.field_at(0).data_type.clone();
            let outer_width = outer.schema().len();

            // Build join schema: outer columns ++ sub columns.
            let join_schema = concat_schemas(outer.schema(), sub_schema)?;

            // Join condition: no explicit predicate (correlated predicate is
            // assumed to already be embedded in the sub plan as a Filter).
            let join = LogicalPlan::Join {
                left: Box::new(outer.clone()),
                right: sub,
                join_type: LogicalJoinType::LeftOuter,
                condition: LogicalJoinCondition::None,
                schema: join_schema,
            };

            // Filter: rhs_col IS NULL (AntiJoin) or IS NOT NULL (SemiJoin).
            let rhs_sentinel = ScalarExpr::Column {
                name: sub_schema_col_name(outer_width),
                index: outer_width,
                data_type: sub_col_dt,
            };
            let filter_pred = ScalarExpr::IsNull {
                expr: Box::new(rhs_sentinel),
                negated: !negated, // EXISTS => IS NOT NULL; NOT EXISTS => IS NULL
            };
            Some(LogicalPlan::Filter {
                input: Box::new(join),
                predicate: filter_pred,
            })
        }

        SubqueryKind::InSubquery {
            outer_expr,
            inner_col,
            sub,
            negated,
        } => {
            let sub_schema = sub.schema();
            let outer_width = outer.schema().len();

            // Build join schema: outer columns ++ sub columns.
            let join_schema = concat_schemas(outer.schema(), sub_schema)?;

            // Join condition: outer_expr = inner_col.
            let inner_col_in_join = shift_column_index(&inner_col, outer_width);
            let eq_pred = ScalarExpr::Binary {
                op: BinaryOp::Eq,
                left: outer_expr,
                right: Box::new(inner_col_in_join.clone()),
                data_type: DataType::Bool,
            };

            let join = LogicalPlan::Join {
                left: Box::new(outer.clone()),
                right: sub,
                join_type: LogicalJoinType::LeftOuter,
                condition: LogicalJoinCondition::On(eq_pred),
                schema: join_schema,
            };

            // Filter: inner_col IS NULL (NOT IN) or IS NOT NULL (IN).
            let filter_pred = ScalarExpr::IsNull {
                expr: Box::new(inner_col_in_join),
                negated: !negated, // IN => IS NOT NULL; NOT IN => IS NULL
            };
            Some(LogicalPlan::Filter {
                input: Box::new(join),
                predicate: filter_pred,
            })
        }
    }
}

/// Generate a synthetic column name for a right-side schema column at `idx`.
#[cfg(test)]
fn sub_schema_col_name(idx: usize) -> String {
    format!("__sub{idx}")
}

/// Shift a `ScalarExpr::Column` index by `offset`.
#[cfg(test)]
fn shift_column_index(expr: &ScalarExpr, offset: usize) -> ScalarExpr {
    match expr {
        ScalarExpr::Column {
            name,
            index,
            data_type,
        } => ScalarExpr::Column {
            name: name.clone(),
            index: index + offset,
            data_type: data_type.clone(),
        },
        other => other.clone(),
    }
}

/// Build an `EXISTS`-subquery `Filter(outer, EXISTS(sub))` using the
/// decorrelation lowering convention. In tests we construct the plan
/// directly rather than going through the binder.
#[cfg(test)]
pub(crate) fn make_exists_filter(
    outer: &LogicalPlan,
    sub: LogicalPlan,
    negated: bool,
) -> LogicalPlan {
    // We use a non-standard approach: directly call `rewrite_filter` to
    // produce the decorrelated form.
    let kind = SubqueryKind::Exists {
        sub: Box::new(sub),
        negated,
    };
    rewrite_filter(outer, kind).expect("rewrite_filter always produces Some in tests")
}

/// Build an `IN`-subquery `Filter(outer, outer_expr IN (SELECT inner_col FROM
/// sub))` using the decorrelation lowering convention.
#[cfg(test)]
pub(crate) fn make_in_subquery_filter(
    outer: &LogicalPlan,
    outer_expr: ScalarExpr,
    inner_col: ScalarExpr,
    sub: LogicalPlan,
    negated: bool,
) -> LogicalPlan {
    let kind = SubqueryKind::InSubquery {
        outer_expr: Box::new(outer_expr),
        inner_col: Box::new(inner_col),
        sub: Box::new(sub),
        negated,
    };
    rewrite_filter(outer, kind).expect("rewrite_filter always produces Some in tests")
}
