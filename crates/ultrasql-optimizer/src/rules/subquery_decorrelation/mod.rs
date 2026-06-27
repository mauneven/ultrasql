//! Subquery decorrelation rewrite rule.
//!
//! [`SubqueryDecorrelation`] transforms correlated subqueries (those that
//! reference columns from an outer query) into equivalent join expressions,
//! eliminating the need for repeated inner execution.
//!
//! ## Lowering convention
//!
//! This rule lowers supported subquery patterns to logical `Semi` / `Anti`
//! joins where possible:
//!
//! - equality-correlated `EXISTS(sub)` → distinct correlated keys from `sub`,
//!   logical `Semi` join against outer.
//! - equality-correlated `NOT EXISTS(sub)` → distinct correlated keys from
//!   `sub`, logical `Anti` join against outer.
//! - uncorrelated `expr IN (SELECT col FROM sub)` → distinct subquery values,
//!   logical `Semi` join.
//! - uncorrelated `expr NOT IN (SELECT col FROM sub)` → distinct non-NULL
//!   subquery values, logical `Anti` joins, and NULL-presence probes that
//!   preserve SQL three-valued logic.
//! - uncorrelated scalar subquery in a predicate → cross join the scalar
//!   subplan, replace the subquery expression with its joined column, filter,
//!   then project outer columns.
//! - equality-correlated scalar aggregate subquery in a predicate → group the
//!   inner aggregate by the correlated key, left-join it to the outer input,
//!   replace the scalar subquery with the joined aggregate column, filter, then
//!   project outer columns.
//!
//! `NOT IN` needs extra care because SQL returns UNKNOWN, not TRUE, when the
//! subquery produces a NULL. The rule handles supported `NOT IN` shapes by
//! separating non-NULL value matching from NULL-presence probes; if a matching
//! correlated group (or an uncorrelated subquery) contains NULL, no row passes.
//!
//! ## Correlation detection
//!
//! A subquery plan is correlated when it contains a [`ScalarExpr::OuterColumn`]
//! reference produced by the binder. The production path currently handles
//! `inner_col = outer_col` equality correlations. The decorrelation pass
//! extracts those equality predicates as join keys and leaves local predicates
//! inside the inner plan.
//!
//! When no correlated column reference is found the subquery is already
//! uncorrelated; the rule returns `None` and applies no transform.
//!
//! ## Current limits
//!
//! Non-equality correlations are not lowered yet. They stay as explicit
//! roadmap debt rather than being hidden by benchmark-query rewrites.

use ultrasql_planner::{LogicalPlan, ScalarExpr};

use crate::error::OptimizeError;
use crate::rules::RewriteRule;

mod correlation;
mod exists_in;
mod helpers;
mod scalar;

#[cfg(test)]
mod legacy;
#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) use legacy::{make_exists_filter, make_in_subquery_filter};

use exists_in::{rewrite_exists_filter_expr, rewrite_in_subquery_filter_expr};
use scalar::{rewrite_project_with_scalar_subquery, rewrite_scalar_subquery_filter};

/// Subquery decorrelation: transforms correlated subqueries in `Filter`
/// predicates into `Semi` / `Anti` joins.
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
// Top-level recursion
// ============================================================================

/// Walk the plan and decorrelate the first subquery predicate found at the top
/// of any `Filter` node.
fn decorrelate(plan: &LogicalPlan) -> Result<Option<LogicalPlan>, OptimizeError> {
    match plan {
        LogicalPlan::Filter { input, predicate } => {
            if let Some(rewritten) = rewrite_filter_with_real_subquery_expr(input, predicate) {
                return Ok(Some(rewritten));
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
            if let Some(rewritten) = rewrite_project_with_scalar_subquery(input, exprs, schema) {
                return Ok(Some(rewritten));
            }
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

        // Recurse through the single-row guard so a scalar subquery whose
        // own body contains another subquery (nested decorrelation) still
        // gets rewritten on a later fixpoint iteration.
        LogicalPlan::SingleRowAssert { input } => {
            let new_input = decorrelate(input)?;
            Ok(new_input.map(|i| LogicalPlan::SingleRowAssert { input: Box::new(i) }))
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
// Production subquery expression dispatch
// ============================================================================

fn rewrite_filter_with_real_subquery_expr(
    outer: &LogicalPlan,
    predicate: &ScalarExpr,
) -> Option<LogicalPlan> {
    rewrite_scalar_subquery_filter(outer, predicate)
        .or_else(|| rewrite_exists_filter_expr(outer, predicate))
        .or_else(|| rewrite_in_subquery_filter_expr(outer, predicate))
}
