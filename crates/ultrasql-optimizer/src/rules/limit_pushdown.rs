//! Limit pushdown rewrite rule.
//!
//! [`LimitPushdown`] moves `LIMIT` nodes closer to their data sources where
//! doing so is semantics-preserving. In v0.6 two patterns are handled:
//!
//! 1. **`Limit(Sort(child), n, offset)` — push through Sort.**
//!    A sort followed by a limit can be fused into a top-N operation.
//!    Because [`LogicalPlan::Sort`] does not yet carry a `top_n` field,
//!    v0.6 leaves this case as a comment placeholder. The rule annotates the
//!    plan to enable the v0.7 `TopN` operator by detecting the pattern and
//!    re-expressing it as `Sort(child) → Limit`. The plan shape is unchanged
//!    here; the rewrite is a no-op producing `None`.
//!
//! 2. **`Limit(Project(child, exprs), n, offset)` — push through Project.**
//!    Because `Project` does not change the row count, the limit can move
//!    below the project. The result is `Project(Limit(child, n, offset))`.
//!    This avoids materialising full rows that the limit would later discard.

use ultrasql_planner::LogicalPlan;

use crate::error::OptimizeError;
use crate::rules::RewriteRule;

/// Pushes limit nodes through projections and (in a future wave) into sort
/// operators.
#[derive(Debug)]
pub struct LimitPushdown;

impl RewriteRule for LimitPushdown {
    fn name(&self) -> &'static str {
        "limit_pushdown"
    }

    fn apply(&self, plan: &LogicalPlan) -> Result<Option<LogicalPlan>, OptimizeError> {
        push_limit(plan)
    }
}

#[allow(clippy::too_many_lines)]
fn push_limit(plan: &LogicalPlan) -> Result<Option<LogicalPlan>, OptimizeError> {
    match plan {
        // ---------------------------------------------------------------
        // Case 2: Limit over Project — push the limit below the Project.
        // ---------------------------------------------------------------
        LogicalPlan::Limit { input, n, offset }
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

            Ok(Some(LogicalPlan::Project {
                input: Box::new(LogicalPlan::Limit {
                    input: proj_input.clone(),
                    n: *n,
                    offset: *offset,
                }),
                exprs: exprs.clone(),
                schema: schema.clone(),
            }))
        }

        // ---------------------------------------------------------------
        // Case 1: Limit over Sort — top-N fusion (v0.7 placeholder).
        //
        // TODO(v0.7): when LogicalPlan::Sort gains a `top_n: Option<u64>`
        //   field, rewrite `Limit(Sort(child, keys), n, offset)` →
        //   `Sort(child, keys, top_n = Some(n + offset))` and drop the
        //   outer Limit node.
        // ---------------------------------------------------------------
        LogicalPlan::Limit { input, n, offset }
            if matches!(input.as_ref(), LogicalPlan::Sort { .. }) =>
        {
            let new_input = push_limit(input)?;
            Ok(new_input.map(|i| LogicalPlan::Limit {
                input: Box::new(i),
                n: *n,
                offset: *offset,
            }))
        }

        // ---------------------------------------------------------------
        // General recursion
        // ---------------------------------------------------------------
        LogicalPlan::Filter { input, predicate } => {
            let new_input = push_limit(input)?;
            Ok(new_input.map(|i| LogicalPlan::Filter {
                input: Box::new(i),
                predicate: predicate.clone(),
            }))
        }

        LogicalPlan::Project {
            input,
            exprs,
            schema,
        } => {
            let new_input = push_limit(input)?;
            Ok(new_input.map(|i| LogicalPlan::Project {
                input: Box::new(i),
                exprs: exprs.clone(),
                schema: schema.clone(),
            }))
        }

        LogicalPlan::Limit { input, n, offset } => {
            let new_input = push_limit(input)?;
            Ok(new_input.map(|i| LogicalPlan::Limit {
                input: Box::new(i),
                n: *n,
                offset: *offset,
            }))
        }

        LogicalPlan::Sort { input, keys } => {
            let new_input = push_limit(input)?;
            Ok(new_input.map(|i| LogicalPlan::Sort {
                input: Box::new(i),
                keys: keys.clone(),
            }))
        }

        LogicalPlan::Join {
            left,
            right,
            join_type,
            condition,
            schema,
        } => {
            let new_left = push_limit(left)?;
            let new_right = push_limit(right)?;
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

        LogicalPlan::Aggregate {
            input,
            group_by,
            aggregates,
            schema,
        } => {
            let new_input = push_limit(input)?;
            Ok(new_input.map(|i| LogicalPlan::Aggregate {
                input: Box::new(i),
                group_by: group_by.clone(),
                aggregates: aggregates.clone(),
                schema: schema.clone(),
            }))
        }

        _ => Ok(None),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use ultrasql_core::{DataType, Field, Schema, Value};
    use ultrasql_planner::{LogicalPlan, ScalarExpr, SortKey};

    use super::*;
    use crate::rules::RewriteRule;

    fn one_col_schema() -> Schema {
        Schema::new([Field::required("id", DataType::Int32)]).expect("schema ok")
    }

    fn col(name: &str, idx: usize) -> ScalarExpr {
        ScalarExpr::Column {
            name: name.to_owned(),
            index: idx,
            data_type: DataType::Int32,
        }
    }

    fn scan(name: &str) -> LogicalPlan {
        LogicalPlan::Scan {
            table: name.into(),
            schema: one_col_schema(),
            projection: None,
        }
    }

    // --- Happy-path: Limit over Project ---

    #[test]
    fn pushes_limit_below_project() {
        let project = LogicalPlan::Project {
            input: Box::new(scan("t")),
            exprs: vec![(col("id", 0), "id".into())],
            schema: one_col_schema(),
        };
        let plan = LogicalPlan::Limit {
            input: Box::new(project),
            n: 10,
            offset: 0,
        };
        let rule = LimitPushdown;
        let result = rule.apply(&plan).expect("no error").expect("should push");
        // Result: Project(Limit(Scan))
        let LogicalPlan::Project { input, .. } = result else {
            panic!("expected Project at top");
        };
        assert!(
            matches!(
                *input,
                LogicalPlan::Limit {
                    n: 10,
                    offset: 0,
                    ..
                }
            ),
            "Limit should be below Project"
        );
    }

    // --- Edge-case: Limit over Sort (no structural change in v0.6) ---

    #[test]
    fn limit_over_sort_does_not_crash() {
        let sort = LogicalPlan::Sort {
            input: Box::new(scan("t")),
            keys: vec![SortKey {
                expr: col("id", 0),
                asc: true,
                nulls_first: false,
            }],
        };
        let plan = LogicalPlan::Limit {
            input: Box::new(sort),
            n: 5,
            offset: 0,
        };
        let rule = LimitPushdown;
        // Must not panic; result may be None (no change) since Sort has no top_n.
        let result = rule.apply(&plan).expect("no error");
        drop(result);
    }

    // --- No-op: Limit over Filter (not transformed) ---

    #[test]
    fn no_push_limit_over_filter() {
        let filter = LogicalPlan::Filter {
            input: Box::new(scan("t")),
            predicate: ScalarExpr::Literal {
                value: Value::Bool(true),
                data_type: DataType::Bool,
            },
        };
        let plan = LogicalPlan::Limit {
            input: Box::new(filter),
            n: 5,
            offset: 0,
        };
        let rule = LimitPushdown;
        // Filter is not a pushdown target; no structural change.
        let result = rule.apply(&plan).expect("no error");
        assert!(result.is_none(), "should not transform Limit over Filter");
    }
}
