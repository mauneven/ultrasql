//! Predicate pushdown into subqueries and CTEs.
//!
//! [`PredicatePushdownSubquery`] pushes `Filter` predicates into derived tables
//! (subqueries in `FROM`) and non-recursive, single-use CTEs, reducing the
//! number of rows that the inner plan must produce.
//!
//! ## Cases handled
//!
//! ### 1. Filter over a derived-table subquery (Filter over Project over inner)
//!
//! When the plan has the shape:
//!
//! ```text
//! Filter(Project(inner), predicate)
//! ```
//!
//! and `predicate` only references columns that originate from `inner` (not
//! synthetic expressions computed by the `Project`), push the predicate below
//! the `Project`:
//!
//! ```text
//! Project(Filter(inner, remapped_predicate))
//! ```
//!
//! This is the "derived table" pushdown. The column remapping follows the same
//! logic as `PredicatePushdown` (the base rule) but is specialised here for
//! the case where the `Project` is the top of a subquery rather than a simple
//! projection over a scan.
//!
//! ### 2. Filter over a non-recursive CTE reference (Filter over Cte)
//!
//! When the plan has the shape:
//!
//! ```text
//! Filter(Cte { name, recursive: false, definition, body }, predicate)
//! ```
//!
//! and the CTE is used exactly once in `body` (detected by scanning for a
//! single `Scan { table: name }` reference), push the filter into the CTE
//! definition:
//!
//! ```text
//! Cte { definition: Filter(definition, predicate), body }
//! ```
//!
//! This is the "CTE inlining + push" optimisation. It is conservative: if the
//! CTE name appears more than once in `body` (materialised multiple times), we
//! do not push because doing so would execute the filter inside the CTE body
//! multiple times, potentially changing observable behaviour for side-effecting
//! subqueries (not that v0.6 has those, but the rule is written defensively).
//!
//! ## Non-applicable conditions
//!
//! - Predicate references a synthesised `Project` expression (computed column).
//! - CTE is recursive (`recursive: true`).
//! - CTE name appears more than once in the body (materialised multiple times).
//! - Predicate contains a parameter (`$N`) that cannot be safely pushed.

#![allow(clippy::match_same_arms)]

use std::collections::HashSet;

use ultrasql_planner::{LogicalPlan, ScalarExpr};

use crate::error::OptimizeError;
use crate::rules::RewriteRule;

/// Pushes filters into subqueries and non-recursive single-use CTEs.
///
/// See the module-level documentation for the full set of cases handled.
#[derive(Debug)]
pub struct PredicatePushdownSubquery;

impl RewriteRule for PredicatePushdownSubquery {
    fn name(&self) -> &'static str {
        "predicate_pushdown_subquery"
    }

    fn apply(&self, plan: &LogicalPlan) -> Result<Option<LogicalPlan>, OptimizeError> {
        push_subquery(plan)
    }
}

// ============================================================================
// Main recursion
// ============================================================================

#[allow(clippy::too_many_lines)]
fn push_subquery(plan: &LogicalPlan) -> Result<Option<LogicalPlan>, OptimizeError> {
    match plan {
        // -------------------------------------------------------------------
        // Case 1: Filter over Project (derived table / subquery-in-FROM)
        // -------------------------------------------------------------------
        LogicalPlan::Filter { input, predicate }
            if matches!(input.as_ref(), LogicalPlan::Project { .. }) =>
        {
            let LogicalPlan::Project {
                input: proj_input,
                exprs,
                schema,
            } = input.as_ref()
            else {
                unreachable!()
            };

            if let Some(pushed) = try_push_through_project(predicate, proj_input, exprs, schema)? {
                return Ok(Some(pushed));
            }

            // Cannot push at this level. Recurse into children.
            let new_input = push_subquery(input)?;
            Ok(new_input.map(|i| LogicalPlan::Filter {
                input: Box::new(i),
                predicate: predicate.clone(),
            }))
        }

        // -------------------------------------------------------------------
        // Case 2: Filter over non-recursive single-use CTE
        // -------------------------------------------------------------------
        LogicalPlan::Filter { input, predicate }
            if matches!(
                input.as_ref(),
                LogicalPlan::Cte {
                    recursive: false,
                    ..
                }
            ) =>
        {
            let LogicalPlan::Cte {
                name,
                recursive: false,
                definition,
                body,
                schema,
            } = input.as_ref()
            else {
                unreachable!()
            };

            // Only push if the CTE is referenced exactly once in the body.
            if cte_use_count(body, name) == 1 {
                // Push the predicate into the CTE definition.
                let new_definition = LogicalPlan::Filter {
                    input: definition.clone(),
                    predicate: predicate.clone(),
                };
                return Ok(Some(LogicalPlan::Cte {
                    name: name.clone(),
                    recursive: false,
                    definition: Box::new(new_definition),
                    body: body.clone(),
                    schema: schema.clone(),
                }));
            }

            // Cannot push; recurse.
            let new_input = push_subquery(input)?;
            Ok(new_input.map(|i| LogicalPlan::Filter {
                input: Box::new(i),
                predicate: predicate.clone(),
            }))
        }

        // -------------------------------------------------------------------
        // General Filter: recurse into child.
        // -------------------------------------------------------------------
        LogicalPlan::Filter { input, predicate } => {
            let new_input = push_subquery(input)?;
            Ok(new_input.map(|i| LogicalPlan::Filter {
                input: Box::new(i),
                predicate: predicate.clone(),
            }))
        }

        // -------------------------------------------------------------------
        // Structural recursion for all other nodes.
        // -------------------------------------------------------------------
        LogicalPlan::Project {
            input,
            exprs,
            schema,
        } => {
            let new_input = push_subquery(input)?;
            Ok(new_input.map(|i| LogicalPlan::Project {
                input: Box::new(i),
                exprs: exprs.clone(),
                schema: schema.clone(),
            }))
        }

        LogicalPlan::Sort { input, keys } => {
            let new_input = push_subquery(input)?;
            Ok(new_input.map(|i| LogicalPlan::Sort {
                input: Box::new(i),
                keys: keys.clone(),
            }))
        }

        LogicalPlan::Limit { input, n, offset } => {
            let new_input = push_subquery(input)?;
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
            let new_input = push_subquery(input)?;
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
            let new_left = push_subquery(left)?;
            let new_right = push_subquery(right)?;
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

        LogicalPlan::Cte {
            name,
            recursive,
            definition,
            body,
            schema,
        } => {
            let new_def = push_subquery(definition)?;
            let new_body = push_subquery(body)?;
            if new_def.is_none() && new_body.is_none() {
                return Ok(None);
            }
            Ok(Some(LogicalPlan::Cte {
                name: name.clone(),
                recursive: *recursive,
                definition: Box::new(new_def.unwrap_or_else(|| *definition.clone())),
                body: Box::new(new_body.unwrap_or_else(|| *body.clone())),
                schema: schema.clone(),
            }))
        }

        // Leaf / DML nodes.
        _ => Ok(None),
    }
}

// ============================================================================
// Derived-table pushdown helper
// ============================================================================

/// Try to push `predicate` through `Project(proj_input, exprs, schema)`.
///
/// Returns `Some(new_plan)` when push is possible, `None` when it is not.
fn try_push_through_project(
    predicate: &ScalarExpr,
    proj_input: &LogicalPlan,
    exprs: &[(ScalarExpr, String)],
    schema: &ultrasql_core::Schema,
) -> Result<Option<LogicalPlan>, OptimizeError> {
    // Predicate must not contain parameters (unsafe to push under a barrier).
    if predicate_has_parameter(predicate) {
        return Ok(None);
    }

    // Determine which output column indices the predicate references.
    let pred_refs = referenced_columns(predicate);
    if pred_refs.is_empty() {
        return Ok(None);
    }

    // All referenced columns must be pass-through (bare Column references).
    let all_passthrough = pred_refs.iter().all(|&out_idx| {
        exprs
            .get(out_idx)
            .is_some_and(|(e, _)| matches!(e, ScalarExpr::Column { .. }))
    });

    if !all_passthrough {
        return Ok(None);
    }

    // Remap predicate column indices through the project.
    let remapped = remap_through_project(predicate, exprs);

    // Recurse: try to push further into proj_input.
    let inner_candidate = LogicalPlan::Filter {
        input: Box::new(proj_input.clone()),
        predicate: remapped,
    };
    let further = push_subquery(&inner_candidate)?;
    let inner = further.unwrap_or(inner_candidate);

    Ok(Some(LogicalPlan::Project {
        input: Box::new(inner),
        exprs: exprs.to_vec(),
        schema: schema.clone(),
    }))
}

// ============================================================================
// CTE use count
// ============================================================================

/// Count how many times a `Scan { table: name }` appears in `plan`.
fn cte_use_count(plan: &LogicalPlan, name: &str) -> usize {
    match plan {
        LogicalPlan::Scan { table, .. } => usize::from(table == name),
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Project { input, .. }
        | LogicalPlan::Aggregate { input, .. } => cte_use_count(input, name),
        LogicalPlan::Join { left, right, .. } | LogicalPlan::SetOp { left, right, .. } => {
            cte_use_count(left, name) + cte_use_count(right, name)
        }
        LogicalPlan::Cte {
            definition, body, ..
        } => cte_use_count(definition, name) + cte_use_count(body, name),
        _ => 0,
    }
}

// ============================================================================
// Expression helpers
// ============================================================================

/// Collect all column indices referenced in `expr`.
fn referenced_columns(expr: &ScalarExpr) -> HashSet<usize> {
    let mut set = HashSet::new();
    collect_cols(expr, &mut set);
    set
}

fn collect_cols(expr: &ScalarExpr, out: &mut HashSet<usize>) {
    match expr {
        ScalarExpr::Column { index, .. } => {
            out.insert(*index);
        }
        ScalarExpr::Binary { left, right, .. } => {
            collect_cols(left, out);
            collect_cols(right, out);
        }
        ScalarExpr::Unary { expr: inner, .. } | ScalarExpr::IsNull { expr: inner, .. } => {
            collect_cols(inner, out);
        }
        ScalarExpr::Literal { .. } | ScalarExpr::Parameter { .. } => {}
        // Subquery variants treated as opaque leaves; full descent is a v0.7 follow-up.
        ScalarExpr::OuterColumn { .. }
        | ScalarExpr::ScalarSubquery { .. }
        | ScalarExpr::Exists { .. }
        | ScalarExpr::InSubquery { .. } => {}
    }
}

/// Return `true` if `expr` contains any `Parameter` node.
fn predicate_has_parameter(expr: &ScalarExpr) -> bool {
    match expr {
        ScalarExpr::Parameter { .. } => true,
        ScalarExpr::Binary { left, right, .. } => {
            predicate_has_parameter(left) || predicate_has_parameter(right)
        }
        ScalarExpr::Unary { expr: inner, .. } | ScalarExpr::IsNull { expr: inner, .. } => {
            predicate_has_parameter(inner)
        }
        ScalarExpr::Column { .. } | ScalarExpr::Literal { .. } => false,
        // Subquery variants treated as opaque leaves; full descent is a v0.7 follow-up.
        ScalarExpr::OuterColumn { .. }
        | ScalarExpr::ScalarSubquery { .. }
        | ScalarExpr::Exists { .. }
        | ScalarExpr::InSubquery { .. } => false,
    }
}

/// Remap column indices in `predicate` through the project expression list.
///
/// Each `Column { index: i }` in the predicate is replaced with `exprs[i].0`.
fn remap_through_project(predicate: &ScalarExpr, exprs: &[(ScalarExpr, String)]) -> ScalarExpr {
    match predicate {
        ScalarExpr::Column { index, .. } => {
            if let Some((child_e, _)) = exprs.get(*index) {
                child_e.clone()
            } else {
                predicate.clone()
            }
        }
        ScalarExpr::Binary {
            op,
            left,
            right,
            data_type,
        } => ScalarExpr::Binary {
            op: *op,
            left: Box::new(remap_through_project(left, exprs)),
            right: Box::new(remap_through_project(right, exprs)),
            data_type: data_type.clone(),
        },
        ScalarExpr::Unary {
            op,
            expr: inner,
            data_type,
        } => ScalarExpr::Unary {
            op: *op,
            expr: Box::new(remap_through_project(inner, exprs)),
            data_type: data_type.clone(),
        },
        ScalarExpr::IsNull {
            expr: inner,
            negated,
        } => ScalarExpr::IsNull {
            expr: Box::new(remap_through_project(inner, exprs)),
            negated: *negated,
        },
        ScalarExpr::Literal { .. } | ScalarExpr::Parameter { .. } => predicate.clone(),
        // Subquery variants treated as opaque leaves; full descent is a v0.7 follow-up.
        ScalarExpr::OuterColumn { .. }
        | ScalarExpr::ScalarSubquery { .. }
        | ScalarExpr::Exists { .. }
        | ScalarExpr::InSubquery { .. } => predicate.clone(),
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use ultrasql_core::{DataType, Field, Schema, Value};
    use ultrasql_planner::{BinaryOp, LogicalPlan, ScalarExpr};

    /// Fold a list of conjuncts into a left-deep `AND` tree.
    ///
    /// # Panics
    ///
    /// Panics when `preds` is empty (caller invariant).
    fn conjuncts_to_and(mut preds: Vec<ScalarExpr>) -> ScalarExpr {
        assert!(!preds.is_empty(), "conjuncts_to_and: empty list");
        let mut result = preds.remove(0);
        for p in preds {
            result = ScalarExpr::Binary {
                op: BinaryOp::And,
                left: Box::new(result),
                right: Box::new(p),
                data_type: ultrasql_core::DataType::Bool,
            };
        }
        result
    }

    /// Split a top-level `AND` into individual conjuncts.
    fn split_and(expr: &ScalarExpr) -> Vec<ScalarExpr> {
        match expr {
            ScalarExpr::Binary {
                op: BinaryOp::And,
                left,
                right,
                ..
            } => {
                let mut v = split_and(left);
                v.extend(split_and(right));
                v
            }
            other => vec![other.clone()],
        }
    }

    use super::*;
    use crate::rules::RewriteRule;

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn scan(table: &str) -> LogicalPlan {
        LogicalPlan::Scan {
            table: table.into(),
            schema: Schema::new(vec![
                Field::required("id", DataType::Int32),
                Field::nullable("score", DataType::Int32),
            ])
            .expect("schema ok"),
            projection: None,
        }
    }

    fn col(name: &str, idx: usize) -> ScalarExpr {
        ScalarExpr::Column {
            name: name.into(),
            index: idx,
            data_type: DataType::Int32,
        }
    }

    fn lit_i32(v: i32) -> ScalarExpr {
        ScalarExpr::Literal {
            value: Value::Int32(v),
            data_type: DataType::Int32,
        }
    }

    fn eq(l: ScalarExpr, r: ScalarExpr) -> ScalarExpr {
        ScalarExpr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(l),
            right: Box::new(r),
            data_type: DataType::Bool,
        }
    }

    fn proj_schema() -> Schema {
        Schema::new(vec![
            Field::required("id", DataType::Int32),
            Field::nullable("score", DataType::Int32),
        ])
        .expect("schema ok")
    }

    /// Build `Project(Scan("inner"), [col(0) AS id, col(1) AS score])`.
    fn derived_table() -> LogicalPlan {
        LogicalPlan::Project {
            input: Box::new(scan("inner")),
            exprs: vec![
                (col("id", 0), "id".into()),
                (col("score", 1), "score".into()),
            ],
            schema: proj_schema(),
        }
    }

    // -----------------------------------------------------------------------
    // Rule name
    // -----------------------------------------------------------------------

    #[test]
    fn rule_name_is_stable() {
        assert_eq!(
            PredicatePushdownSubquery.name(),
            "predicate_pushdown_subquery"
        );
    }

    // -----------------------------------------------------------------------
    // Case 1: Derived-table pushdown
    // -----------------------------------------------------------------------

    #[test]
    fn pushes_filter_into_derived_table_when_passthrough_column() {
        // Filter(Project(Scan("inner"), [id, score]), id = 5)
        let plan = LogicalPlan::Filter {
            input: Box::new(derived_table()),
            predicate: eq(col("id", 0), lit_i32(5)),
        };

        let result = PredicatePushdownSubquery.apply(&plan).expect("no error");
        assert!(
            result.is_some(),
            "filter should be pushed into derived table"
        );

        let result = result.unwrap();
        // Top node should now be Project.
        assert!(
            matches!(result, LogicalPlan::Project { .. }),
            "top node should be Project after push; got {result:?}"
        );
        if let LogicalPlan::Project { input, .. } = &result {
            // Below the Project there should be a Filter.
            assert!(
                matches!(input.as_ref(), LogicalPlan::Filter { .. }),
                "Project input should be Filter; got {input:?}"
            );
        }
    }

    #[test]
    fn does_not_push_when_predicate_references_computed_expr() {
        // Project with a computed column (not pass-through).
        let proj = LogicalPlan::Project {
            input: Box::new(scan("inner")),
            exprs: vec![(
                ScalarExpr::Binary {
                    op: BinaryOp::Add,
                    left: Box::new(col("id", 0)),
                    right: Box::new(lit_i32(1)),
                    data_type: DataType::Int32,
                },
                "derived".into(),
            )],
            schema: Schema::new(vec![Field::required("derived", DataType::Int32)])
                .expect("schema ok"),
        };
        let plan = LogicalPlan::Filter {
            input: Box::new(proj),
            predicate: eq(col("derived", 0), lit_i32(5)),
        };
        let result = PredicatePushdownSubquery.apply(&plan).expect("no error");
        // The predicate references a computed expression; cannot push.
        if let Some(r) = result {
            assert!(
                matches!(r, LogicalPlan::Filter { .. }),
                "should remain a Filter when push is not possible"
            );
        }
    }

    #[test]
    fn does_not_push_when_predicate_has_parameter() {
        // Predicate with $1.
        let plan = LogicalPlan::Filter {
            input: Box::new(derived_table()),
            predicate: eq(
                col("id", 0),
                ScalarExpr::Parameter {
                    index: 1,
                    data_type: DataType::Int32,
                },
            ),
        };
        let result = PredicatePushdownSubquery.apply(&plan).expect("no error");
        // Parameter predicates must not be pushed.
        if let Some(r) = result {
            assert!(
                matches!(r, LogicalPlan::Filter { .. }),
                "parameter predicate should not be pushed"
            );
        }
    }

    // -----------------------------------------------------------------------
    // Case 2: CTE inlining + push
    // -----------------------------------------------------------------------

    #[test]
    fn pushes_filter_into_non_recursive_cte_used_once() {
        let cte_def = scan("base");
        let cte_body = scan("cte_name"); // References the CTE by name.
        let cte_schema = Schema::new(vec![
            Field::required("id", DataType::Int32),
            Field::nullable("score", DataType::Int32),
        ])
        .expect("schema ok");

        let cte = LogicalPlan::Cte {
            name: "cte_name".into(),
            recursive: false,
            definition: Box::new(cte_def),
            body: Box::new(cte_body),
            schema: cte_schema,
        };

        // Filter(Cte, id = 5)
        let plan = LogicalPlan::Filter {
            input: Box::new(cte),
            predicate: eq(col("id", 0), lit_i32(5)),
        };

        let result = PredicatePushdownSubquery.apply(&plan).expect("no error");
        assert!(
            result.is_some(),
            "filter should be pushed into CTE definition"
        );

        // Top node should now be Cte (not Filter).
        assert!(
            matches!(result.unwrap(), LogicalPlan::Cte { .. }),
            "top node should be Cte after push"
        );
    }

    #[test]
    fn does_not_push_into_cte_used_multiple_times() {
        let cte_def = scan("base");
        // Body references "cte_name" twice (e.g. self-join equivalent).
        let cte_schema =
            Schema::new(vec![Field::required("id", DataType::Int32)]).expect("schema ok");
        let join_schema = Schema::new(vec![
            Field::required("id", DataType::Int32),
            Field::required("id2", DataType::Int32),
        ])
        .expect("schema ok");
        let cte_body = LogicalPlan::Join {
            left: Box::new(scan("cte_name")),
            right: Box::new(scan("cte_name")),
            join_type: ultrasql_planner::LogicalJoinType::Inner,
            condition: ultrasql_planner::LogicalJoinCondition::None,
            schema: join_schema,
        };
        let cte = LogicalPlan::Cte {
            name: "cte_name".into(),
            recursive: false,
            definition: Box::new(cte_def),
            body: Box::new(cte_body),
            schema: cte_schema,
        };
        let plan = LogicalPlan::Filter {
            input: Box::new(cte),
            predicate: eq(col("id", 0), lit_i32(5)),
        };
        let result = PredicatePushdownSubquery.apply(&plan).expect("no error");
        // CTE used twice — must not push.
        if let Some(r) = result {
            assert!(
                !matches!(r, LogicalPlan::Cte { .. }),
                "CTE used twice should not have filter pushed into definition"
            );
        }
    }

    #[test]
    fn does_not_push_into_recursive_cte() {
        let cte_def = scan("base");
        let cte_body = scan("cte_name");
        let cte_schema =
            Schema::new(vec![Field::required("id", DataType::Int32)]).expect("schema ok");
        let cte = LogicalPlan::Cte {
            name: "cte_name".into(),
            recursive: true, // Recursive CTE
            definition: Box::new(cte_def),
            body: Box::new(cte_body),
            schema: cte_schema,
        };
        let plan = LogicalPlan::Filter {
            input: Box::new(cte),
            predicate: eq(col("id", 0), lit_i32(5)),
        };
        let result = PredicatePushdownSubquery.apply(&plan).expect("no error");
        // Recursive CTE — must not push.
        if let Some(r) = result {
            assert!(
                !matches!(
                    r,
                    LogicalPlan::Cte {
                        recursive: false,
                        ..
                    }
                ),
                "recursive CTE should not have filter pushed"
            );
        }
    }

    // -----------------------------------------------------------------------
    // No-op on plans without subqueries
    // -----------------------------------------------------------------------

    #[test]
    fn no_op_on_plain_scan() {
        let plan = scan("t");
        let result = PredicatePushdownSubquery.apply(&plan).expect("no error");
        assert!(result.is_none());
    }

    #[test]
    fn no_op_on_filter_over_scan() {
        let plan = LogicalPlan::Filter {
            input: Box::new(scan("t")),
            predicate: eq(col("id", 0), lit_i32(42)),
        };
        let result = PredicatePushdownSubquery.apply(&plan).expect("no error");
        assert!(
            result.is_none(),
            "filter over Scan has no subquery to push into"
        );
    }

    // -----------------------------------------------------------------------
    // Helpers: split_and / conjuncts_to_and round-trip
    // -----------------------------------------------------------------------

    #[test]
    fn split_and_then_conjuncts_to_and_is_identity_for_single_pred() {
        let pred = eq(col("id", 0), lit_i32(1));
        let parts = split_and(&pred);
        assert_eq!(parts.len(), 1);
        let rebuilt = conjuncts_to_and(parts);
        // Display comparison is sufficient for structural equality check.
        assert_eq!(pred.to_string(), rebuilt.to_string());
    }

    #[test]
    fn split_and_decomposes_nested_and() {
        let p1 = eq(col("id", 0), lit_i32(1));
        let p2 = eq(col("score", 1), lit_i32(2));
        let and = ScalarExpr::Binary {
            op: BinaryOp::And,
            left: Box::new(p1),
            right: Box::new(p2),
            data_type: DataType::Bool,
        };
        let parts = split_and(&and);
        assert_eq!(parts.len(), 2);
    }
}
