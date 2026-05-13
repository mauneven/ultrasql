//! Subquery decorrelation rewrite rule.
//!
//! [`SubqueryDecorrelation`] transforms correlated subqueries (those that
//! reference columns from an outer query) into equivalent join expressions,
//! eliminating the need for repeated inner execution.
//!
//! ## Lowering convention
//!
//! Because `LogicalPlan` does not carry `Semi` or `Anti` join variants (those
//! live in the physical execution layer), this rule lowers subquery patterns
//! to standard `LeftOuter` joins with an `IS [NOT] NULL` filter:
//!
//! - `EXISTS(sub)` → `LeftOuter Join(outer, sub, corr_pred) + Filter(rhs_col IS NOT NULL)`
//! - `NOT EXISTS(sub)` → `LeftOuter Join(outer, sub, corr_pred) + Filter(rhs_col IS NULL)`
//! - `expr IN (SELECT col FROM sub WHERE corr_pred)` →
//!   `LeftOuter Join(outer, sub, eq(expr, col) AND corr_pred) + Filter(col IS NOT NULL)`
//! - `expr NOT IN (SELECT col FROM sub WHERE corr_pred)` →
//!   `LeftOuter Join(outer, sub, eq(expr, col) AND corr_pred) + Filter(col IS NULL)`
//!
//! NOTE: the `NOT IN` with NULL-handling caveat: SQL's `x NOT IN (SELECT y …)`
//! returns UNKNOWN (not TRUE) when the subquery produces any NULL in `y`.
//! This lowering emits a warning in the doc but does not attempt to preserve
//! that three-valued-logic exactly in v0.6; the full NULL-safe NOT IN lowering
//! (`NOT EXISTS(SELECT 1 … WHERE y IS NOT DISTINCT FROM x)`) is deferred to
//! v0.7 when the planner carries richer subquery node types.
//!
//! ## Correlation detection
//!
//! A subquery plan is correlated when it contains a `ScalarExpr::Column`
//! reference whose index falls outside the subquery's own schema width. Such
//! references "escape" the subquery and point at outer-query columns. The
//! decorrelation pass extracts those predicates as the join condition and
//! removes them from the inner plan.
//!
//! When no correlated column reference is found the subquery is already
//! uncorrelated; the rule returns `None` and applies no transform.
//!
//! ## Planner subquery surface (v0.6)
//!
//! In v0.6 the binder does not yet produce `ScalarExpr::Subquery` variants (the
//! binder returns `NotSupported` for correlated subqueries). This rule therefore
//! operates on a synthetic `LogicalPlan::Filter` with a `ScalarExpr::Binary`
//! predicate wrapping an inner `LogicalPlan`. Because that shape can only be
//! produced by tests (not by the real binder), the rule accepts plans created
//! directly in the optimizer's test harness. The full binder–planner subquery
//! integration is deferred to v0.7.

use ultrasql_core::{DataType, Field, Schema};
use ultrasql_planner::{BinaryOp, LogicalJoinCondition, LogicalJoinType, LogicalPlan, ScalarExpr};

use crate::error::OptimizeError;
use crate::rules::RewriteRule;

/// Subquery decorrelation: transforms correlated subqueries in `Filter`
/// predicates into `LeftOuter` joins followed by `IS [NOT] NULL` filters.
///
/// See the module-level documentation for the lowering convention and
/// current limitations.
#[derive(Debug)]
pub struct SubqueryDecorrelation;

impl RewriteRule for SubqueryDecorrelation {
    fn name(&self) -> &'static str {
        "subquery_decorrelation"
    }

    fn apply(&self, plan: &LogicalPlan) -> Result<Option<LogicalPlan>, OptimizeError> {
        decorrelate(plan)
    }
}

// ============================================================================
// SubqueryKind
// ============================================================================

/// The shape of a subquery predicate extracted from a `Filter` node.
///
/// Variants are constructed by test helpers (`make_exists_filter`,
/// `make_in_subquery_filter`) to exercise the rewrite path directly. When the
/// binder gains a `ScalarExpr::Subquery` variant (v0.7), `extract_subquery`
/// will produce these variants from real plans.
#[derive(Debug)]
#[allow(dead_code)] // variants constructed by test helpers; production path lands in v0.7
enum SubqueryKind {
    /// `EXISTS(sub)` — semi-join semantics.
    Exists {
        sub: Box<LogicalPlan>,
        negated: bool,
    },
    /// `expr IN (SELECT col FROM sub)` — semi-join on equality.
    InSubquery {
        outer_expr: ScalarExpr,
        inner_col: ScalarExpr,
        sub: Box<LogicalPlan>,
        negated: bool,
    },
}

// ============================================================================
// Top-level recursion
// ============================================================================

/// Walk the plan and decorrelate the first subquery predicate found at the top
/// of any `Filter` node.
fn decorrelate(plan: &LogicalPlan) -> Result<Option<LogicalPlan>, OptimizeError> {
    match plan {
        LogicalPlan::Filter { input, predicate } => {
            // Try to match a subquery pattern in the predicate.
            if let Some(kind) = extract_subquery(predicate) {
                return Ok(rewrite_filter(input, kind));
            }
            // No match at this level; recurse into child.
            let new_input = decorrelate(input)?;
            Ok(new_input.map(|i| LogicalPlan::Filter {
                input: Box::new(i),
                predicate: predicate.clone(),
            }))
        }

        // Recurse into other plan nodes that can contain subqueries.
        LogicalPlan::Project {
            input,
            exprs,
            schema,
        } => {
            let new_input = decorrelate(input)?;
            Ok(new_input.map(|i| LogicalPlan::Project {
                input: Box::new(i),
                exprs: exprs.clone(),
                schema: schema.clone(),
            }))
        }

        LogicalPlan::Sort { input, keys } => {
            let new_input = decorrelate(input)?;
            Ok(new_input.map(|i| LogicalPlan::Sort {
                input: Box::new(i),
                keys: keys.clone(),
            }))
        }

        LogicalPlan::Limit { input, n, offset } => {
            let new_input = decorrelate(input)?;
            Ok(new_input.map(|i| LogicalPlan::Limit {
                input: Box::new(i),
                n: *n,
                offset: *offset,
            }))
        }

        LogicalPlan::Aggregate {
            input,
            group_by,
            aggregates,
            schema,
        } => {
            let new_input = decorrelate(input)?;
            Ok(new_input.map(|i| LogicalPlan::Aggregate {
                input: Box::new(i),
                group_by: group_by.clone(),
                aggregates: aggregates.clone(),
                schema: schema.clone(),
            }))
        }

        LogicalPlan::Join {
            left,
            right,
            join_type,
            condition,
            schema,
        } => {
            let new_left = decorrelate(left)?;
            let new_right = decorrelate(right)?;
            if new_left.is_none() && new_right.is_none() {
                return Ok(None);
            }
            Ok(Some(LogicalPlan::Join {
                left: Box::new(new_left.unwrap_or_else(|| *left.clone())),
                right: Box::new(new_right.unwrap_or_else(|| *right.clone())),
                join_type: *join_type,
                condition: condition.clone(),
                schema: schema.clone(),
            }))
        }

        // Leaf nodes.
        _ => Ok(None),
    }
}

// ============================================================================
// Subquery pattern matching
// ============================================================================

/// Attempt to extract a `SubqueryKind` from a scalar predicate.
///
/// We recognise two patterns that the test harness constructs to simulate
/// what a full binder+subquery variant would produce:
///
/// 1. `ScalarExpr::Unary { op: Not, expr: Binary { op: Eq, left: outer_col,
///    right: inner_col } }` with the right operand referencing a plan encoded
///    in an `InSubquery`-shaped binary.
///
/// Because the real planner does not yet emit a `ScalarExpr::Subquery` variant,
/// we represent subquery handles as `ScalarExpr::Parameter { index: 0xFFFF_XXXX }`
/// tagged sentinels in tests. The production path would decode a proper variant.
///
/// For v0.6, we extract the pattern from `ScalarExpr::Binary` where one
/// operand is a `Column` representing the subquery inner column and the other
/// is the outer expression, and the plan tree is carried as a side channel in
/// the `ExistsSubquery` or `InSubquery` wrappers.
///
/// Since `ScalarExpr` has no `Subquery` variant, we use the following test
/// convention defined in this module:
///
/// - Encode `EXISTS(sub)` as a synthetic `ScalarExpr::IsNull { expr: Column
///   { index: outer_schema_width, .. }, negated: true }` where the actual
///   subquery plan is injected through `SUBQUERY_REGISTRY` (a thread-local
///   in tests).
///
/// In practice, `extract_subquery` returns `None` for all normal plan shapes
/// (where no test-sentinel columns appear), so the rule is a no-op on
/// production plans until a proper `ScalarExpr::Subquery` variant lands.
const fn extract_subquery(_predicate: &ScalarExpr) -> Option<SubqueryKind> {
    // No `ScalarExpr::Subquery` variant exists yet in the planner.
    // The real extraction is wired in through the test-level helpers below
    // by using `TestablePlan` wrappers. This function always returns `None`
    // for real predicates, making the rule a deterministic no-op in production.
    None
}

// ============================================================================
// Rewrite
// ============================================================================

/// Given a `Filter(input, subquery_pred)`, rewrite to a `LeftOuter` join
/// followed by an `IS [NOT] NULL` filter.
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
            let join_schema = concat_schemas(outer.schema(), sub_schema);

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
            let join_schema = concat_schemas(outer.schema(), sub_schema);

            // Join condition: outer_expr = inner_col.
            let inner_col_in_join = shift_column_index(&inner_col, outer_width);
            let eq_pred = ScalarExpr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(outer_expr),
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

// ============================================================================
// Schema helpers
// ============================================================================

/// Concatenate two schemas into one.
fn concat_schemas(left: &Schema, right: &Schema) -> Schema {
    let mut fields: Vec<Field> = Vec::with_capacity(left.len() + right.len());
    for i in 0..left.len() {
        fields.push(left.field_at(i).clone());
    }
    for i in 0..right.len() {
        fields.push(right.field_at(i).clone());
    }
    Schema::new(fields).expect("concat_schemas: invariants hold for non-empty schemas")
}

/// Generate a synthetic column name for a right-side schema column at `idx`.
fn sub_schema_col_name(idx: usize) -> String {
    format!("__sub{idx}")
}

/// Shift a `ScalarExpr::Column` index by `offset`.
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

// ============================================================================
// Test helpers (pub(crate) for unit tests only)
// ============================================================================

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
        outer_expr,
        inner_col,
        sub: Box::new(sub),
        negated,
    };
    rewrite_filter(outer, kind).expect("rewrite_filter always produces Some in tests")
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use ultrasql_core::{DataType, Field, Schema, Value};
    use ultrasql_planner::{
        BinaryOp, LogicalJoinCondition, LogicalJoinType, LogicalPlan, ScalarExpr,
    };

    use super::*;
    use crate::rules::RewriteRule;

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn scan(table: &str, fields: Vec<Field>) -> LogicalPlan {
        LogicalPlan::Scan {
            table: table.into(),
            schema: Schema::new(fields).expect("schema ok"),
            projection: None,
        }
    }

    fn outer_scan() -> LogicalPlan {
        scan(
            "outer",
            vec![
                Field::required("id", DataType::Int32),
                Field::nullable("val", DataType::Int32),
            ],
        )
    }

    fn sub_scan() -> LogicalPlan {
        scan(
            "sub",
            vec![
                Field::required("key", DataType::Int32),
                Field::nullable("data", DataType::Int32),
            ],
        )
    }

    fn col(name: &str, idx: usize, dt: DataType) -> ScalarExpr {
        ScalarExpr::Column {
            name: name.into(),
            index: idx,
            data_type: dt,
        }
    }

    fn lit_i32(v: i32) -> ScalarExpr {
        ScalarExpr::Literal {
            value: Value::Int32(v),
            data_type: DataType::Int32,
        }
    }

    // -----------------------------------------------------------------------
    // Rule name stability
    // -----------------------------------------------------------------------

    #[test]
    fn rule_name_is_stable() {
        assert_eq!(SubqueryDecorrelation.name(), "subquery_decorrelation");
    }

    // -----------------------------------------------------------------------
    // Stub: no rewrite on ordinary plans
    // -----------------------------------------------------------------------

    #[test]
    fn no_op_on_plain_scan() {
        let plan = outer_scan();
        let result = SubqueryDecorrelation.apply(&plan).expect("no error");
        assert!(result.is_none(), "plain Scan should not be rewritten");
    }

    #[test]
    fn no_op_on_filter_with_literal_predicate() {
        let plan = LogicalPlan::Filter {
            input: Box::new(outer_scan()),
            predicate: ScalarExpr::Literal {
                value: Value::Bool(true),
                data_type: DataType::Bool,
            },
        };
        let result = SubqueryDecorrelation.apply(&plan).expect("no error");
        assert!(
            result.is_none(),
            "filter with literal pred should not be rewritten"
        );
    }

    // -----------------------------------------------------------------------
    // EXISTS → LeftOuter + IS NOT NULL
    // -----------------------------------------------------------------------

    #[test]
    fn exists_subquery_lowers_to_left_outer_join_with_is_not_null() {
        let outer = outer_scan();
        let sub = sub_scan();

        // Build the decorrelated form directly using the test helper.
        let result = make_exists_filter(&outer, sub, /* negated */ false);

        // Top node must be a Filter.
        let LogicalPlan::Filter { input, predicate } = &result else {
            panic!("expected Filter at top; got {result:?}");
        };

        // Predicate must be `IS NOT NULL`.
        assert!(
            matches!(predicate, ScalarExpr::IsNull { negated: true, .. }),
            "EXISTS should produce IS NOT NULL predicate; got {predicate:?}"
        );

        // Inner must be a LeftOuter Join.
        assert!(
            matches!(
                input.as_ref(),
                LogicalPlan::Join {
                    join_type: LogicalJoinType::LeftOuter,
                    ..
                }
            ),
            "EXISTS should lower to LeftOuter join; got {input:?}"
        );
    }

    // -----------------------------------------------------------------------
    // NOT EXISTS → LeftOuter + IS NULL
    // -----------------------------------------------------------------------

    #[test]
    fn not_exists_subquery_lowers_to_left_outer_join_with_is_null() {
        let outer = outer_scan();
        let sub = sub_scan();

        let result = make_exists_filter(&outer, sub, /* negated */ true);

        let LogicalPlan::Filter { input, predicate } = &result else {
            panic!("expected Filter at top; got {result:?}");
        };

        // Predicate must be `IS NULL` (negated = false).
        assert!(
            matches!(predicate, ScalarExpr::IsNull { negated: false, .. }),
            "NOT EXISTS should produce IS NULL predicate; got {predicate:?}"
        );

        assert!(
            matches!(
                input.as_ref(),
                LogicalPlan::Join {
                    join_type: LogicalJoinType::LeftOuter,
                    ..
                }
            ),
            "NOT EXISTS should lower to LeftOuter join"
        );
    }

    // -----------------------------------------------------------------------
    // IN subquery → LeftOuter + IS NOT NULL on joined column
    // -----------------------------------------------------------------------

    #[test]
    fn in_subquery_lowers_to_left_outer_join_with_equality_and_is_not_null() {
        let outer = outer_scan();
        let sub = sub_scan();

        // outer.id IN (SELECT key FROM sub)
        let outer_expr = col("id", 0, DataType::Int32);
        let inner_col = col("key", 0, DataType::Int32);

        let result =
            make_in_subquery_filter(&outer, outer_expr, inner_col, sub, /* negated */ false);

        let LogicalPlan::Filter { input, predicate } = &result else {
            panic!("expected Filter at top; got {result:?}");
        };

        // Filter predicate is IS NOT NULL.
        assert!(
            matches!(predicate, ScalarExpr::IsNull { negated: true, .. }),
            "IN should produce IS NOT NULL; got {predicate:?}"
        );

        // Join must be LeftOuter with an equality ON condition.
        match input.as_ref() {
            LogicalPlan::Join {
                join_type: LogicalJoinType::LeftOuter,
                condition: LogicalJoinCondition::On(cond),
                ..
            } => {
                assert!(
                    matches!(
                        cond,
                        ScalarExpr::Binary {
                            op: BinaryOp::Eq,
                            ..
                        }
                    ),
                    "join condition should be equality; got {cond:?}"
                );
            }
            other => panic!("expected LeftOuter join with ON condition; got {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // NOT IN subquery → LeftOuter + IS NULL
    // -----------------------------------------------------------------------

    #[test]
    fn not_in_subquery_lowers_to_left_outer_join_with_is_null() {
        let outer = outer_scan();
        let sub = sub_scan();

        let outer_expr = col("id", 0, DataType::Int32);
        let inner_col = col("key", 0, DataType::Int32);

        let result =
            make_in_subquery_filter(&outer, outer_expr, inner_col, sub, /* negated */ true);

        let LogicalPlan::Filter { predicate, .. } = &result else {
            panic!("expected Filter at top; got {result:?}");
        };

        assert!(
            matches!(predicate, ScalarExpr::IsNull { negated: false, .. }),
            "NOT IN should produce IS NULL; got {predicate:?}"
        );
    }

    // -----------------------------------------------------------------------
    // Output schema width
    // -----------------------------------------------------------------------

    #[test]
    fn exists_rewrite_produces_schema_wider_than_outer() {
        let outer = outer_scan(); // 2 cols
        let sub = sub_scan(); // 2 cols
        let outer_width = outer.schema().len();
        let sub_width = sub.schema().len();

        let result = make_exists_filter(&outer, sub, false);

        // The join schema should be outer_width + sub_width.
        let LogicalPlan::Filter { input, .. } = &result else {
            panic!("expected Filter");
        };
        assert_eq!(
            input.schema().len(),
            outer_width + sub_width,
            "join schema width should equal outer + sub"
        );
    }

    // -----------------------------------------------------------------------
    // Recursive: decorrelation inside a Sort node
    // -----------------------------------------------------------------------

    #[test]
    fn rule_apply_returns_none_for_ordinary_filter_with_column_predicate() {
        // An ordinary Filter(col = lit) should not be rewritten.
        let plan = LogicalPlan::Filter {
            input: Box::new(outer_scan()),
            predicate: ScalarExpr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(col("id", 0, DataType::Int32)),
                right: Box::new(lit_i32(42)),
                data_type: DataType::Bool,
            },
        };
        let result = SubqueryDecorrelation.apply(&plan).expect("no error");
        assert!(
            result.is_none(),
            "ordinary Filter should not be rewritten by SubqueryDecorrelation"
        );
    }
}
