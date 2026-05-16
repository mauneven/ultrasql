//! Projection pushdown rewrite rule.
//!
//! [`ProjectionPushdown`] prunes columns from table scans so the storage
//! layer reads only the columns actually needed by the query. In v0.6 the
//! rule handles:
//!
//! - `Project(Scan, exprs)` — computes the set of column indices that
//!   `exprs` reference; if that set is smaller than the scan's full schema,
//!   rewrites the `Scan` to carry a `projection: Some(used_indices)` list and
//!   re-indexes the project expressions to match the narrowed schema.
//!
//! Correctness invariant: after the rewrite the project expressions reference
//! the same logical columns as before, addressed through their new consecutive
//! positions in the projected scan schema.

#![allow(clippy::match_same_arms)]

use std::collections::HashSet;

use ultrasql_core::{Field, Schema};
use ultrasql_planner::{LogicalPlan, ScalarExpr};

use crate::error::OptimizeError;
use crate::rules::RewriteRule;

/// Pushes projection column lists into scan nodes to prune unused columns.
#[derive(Debug)]
pub struct ProjectionPushdown;

impl RewriteRule for ProjectionPushdown {
    fn name(&self) -> &'static str {
        "projection_pushdown"
    }

    fn apply(&self, plan: &LogicalPlan) -> Result<Option<LogicalPlan>, OptimizeError> {
        push_proj(plan)
    }
}

#[allow(clippy::too_many_lines)]
fn push_proj(plan: &LogicalPlan) -> Result<Option<LogicalPlan>, OptimizeError> {
    match plan {
        // ---------------------------------------------------------------
        // Core case: Project directly over Scan
        // ---------------------------------------------------------------
        LogicalPlan::Project {
            input,
            exprs,
            schema: proj_schema,
        } if matches!(input.as_ref(), LogicalPlan::Scan { .. }) => {
            let LogicalPlan::Scan {
                table,
                schema: scan_schema,
                projection,
            } = input.as_ref()
            else {
                unreachable!()
            };

            // If the scan already has a projection, do not double-push for now.
            if projection.is_some() {
                return Ok(None);
            }

            // Collect all column indices referenced in the project expressions.
            let used: HashSet<usize> = exprs
                .iter()
                .flat_map(|(e, _)| column_indices_referenced(e))
                .collect();

            let total_columns = scan_schema.len();

            // If all columns are used there is nothing to prune.
            if used.len() >= total_columns {
                return Ok(None);
            }

            // Build the ordered list of used indices.
            let mut ordered: Vec<usize> = used.into_iter().collect();
            ordered.sort_unstable();

            // Build the new (narrowed) scan schema.
            let new_scan_fields: Vec<Field> = ordered
                .iter()
                .map(|&i| scan_schema.field_at(i).clone())
                .collect();
            let new_scan_schema =
                Schema::new(new_scan_fields).map_err(|e| OptimizeError::RuleFailed {
                    rule: "projection_pushdown",
                    detail: e.to_string(),
                })?;

            // Build a remapping: old index → new index in the projected scan.
            let mut remap = vec![usize::MAX; total_columns];
            for (new_idx, &old_idx) in ordered.iter().enumerate() {
                remap[old_idx] = new_idx;
            }

            // Rewrite project expressions to use new indices.
            let new_exprs: Vec<(ScalarExpr, String)> = exprs
                .iter()
                .map(|(e, name)| (reindex_expr(e, &remap), name.clone()))
                .collect();

            Ok(Some(LogicalPlan::Project {
                input: Box::new(LogicalPlan::Scan {
                    table: table.clone(),
                    schema: new_scan_schema,
                    projection: Some(ordered),
                }),
                exprs: new_exprs,
                schema: proj_schema.clone(),
            }))
        }

        // ---------------------------------------------------------------
        // General recursion
        // ---------------------------------------------------------------
        LogicalPlan::Filter { input, predicate } => {
            let new_input = push_proj(input)?;
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
            let new_input = push_proj(input)?;
            Ok(new_input.map(|i| LogicalPlan::Project {
                input: Box::new(i),
                exprs: exprs.clone(),
                schema: schema.clone(),
            }))
        }

        LogicalPlan::Limit { input, n, offset } => {
            let new_input = push_proj(input)?;
            Ok(new_input.map(|i| LogicalPlan::Limit {
                input: Box::new(i),
                n: *n,
                offset: *offset,
            }))
        }

        LogicalPlan::Sort { input, keys } => {
            let new_input = push_proj(input)?;
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
            let new_left = push_proj(left)?;
            let new_right = push_proj(right)?;
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
            let new_input = push_proj(input)?;
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
// Helpers
// ---------------------------------------------------------------------------

/// Collect all column indices referenced by `expr`.
pub(crate) fn column_indices_referenced(expr: &ScalarExpr) -> HashSet<usize> {
    let mut set = HashSet::new();
    collect_refs(expr, &mut set);
    set
}

fn collect_refs(expr: &ScalarExpr, out: &mut HashSet<usize>) {
    match expr {
        ScalarExpr::Column { index, .. } => {
            out.insert(*index);
        }
        ScalarExpr::Binary { left, right, .. } => {
            collect_refs(left, out);
            collect_refs(right, out);
        }
        ScalarExpr::Unary { expr, .. } | ScalarExpr::IsNull { expr, .. } => {
            collect_refs(expr, out);
        }
        ScalarExpr::FunctionCall { args, .. } => {
            for a in args {
                collect_refs(a, out);
            }
        }
        ScalarExpr::Literal { .. } | ScalarExpr::Parameter { .. } => {}
        // Subquery variants treated as opaque leaves; full descent is a v0.7 follow-up.
        ScalarExpr::OuterColumn { .. }
        | ScalarExpr::ScalarSubquery { .. }
        | ScalarExpr::Exists { .. }
        | ScalarExpr::InSubquery { .. } => {}
    }
}

/// Rewrite column indices in `expr` using the provided remapping table.
/// `remap[old_index] = new_index`.
fn reindex_expr(expr: &ScalarExpr, remap: &[usize]) -> ScalarExpr {
    match expr {
        ScalarExpr::Column {
            name,
            index,
            data_type,
        } => ScalarExpr::Column {
            name: name.clone(),
            index: remap[*index],
            data_type: data_type.clone(),
        },
        ScalarExpr::Binary {
            op,
            left,
            right,
            data_type,
        } => ScalarExpr::Binary {
            op: *op,
            left: Box::new(reindex_expr(left, remap)),
            right: Box::new(reindex_expr(right, remap)),
            data_type: data_type.clone(),
        },
        ScalarExpr::Unary {
            op,
            expr: inner,
            data_type,
        } => ScalarExpr::Unary {
            op: *op,
            expr: Box::new(reindex_expr(inner, remap)),
            data_type: data_type.clone(),
        },
        ScalarExpr::IsNull {
            expr: inner,
            negated,
        } => ScalarExpr::IsNull {
            expr: Box::new(reindex_expr(inner, remap)),
            negated: *negated,
        },
        ScalarExpr::Literal { .. } | ScalarExpr::Parameter { .. } => expr.clone(),
        ScalarExpr::FunctionCall {
            name,
            args,
            data_type,
        } => ScalarExpr::FunctionCall {
            name: name.clone(),
            args: args.iter().map(|a| reindex_expr(a, remap)).collect(),
            data_type: data_type.clone(),
        },
        // Subquery variants treated as opaque leaves; full descent is a v0.7 follow-up.
        ScalarExpr::OuterColumn { .. }
        | ScalarExpr::ScalarSubquery { .. }
        | ScalarExpr::Exists { .. }
        | ScalarExpr::InSubquery { .. } => expr.clone(),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use ultrasql_core::{DataType, Field, Schema};
    use ultrasql_planner::{LogicalPlan, ScalarExpr};

    use super::*;
    use crate::rules::RewriteRule;

    fn three_col_schema() -> Schema {
        Schema::new([
            Field::required("a", DataType::Int32),
            Field::required("b", DataType::Int32),
            Field::required("c", DataType::Int32),
        ])
        .expect("schema ok")
    }

    fn col(name: &str, idx: usize) -> ScalarExpr {
        ScalarExpr::Column {
            name: name.to_owned(),
            index: idx,
            data_type: DataType::Int32,
        }
    }

    // --- Happy-path ---

    #[test]
    fn pushes_projection_into_scan_prunes_unused_column() {
        let full_schema = three_col_schema();
        let scan = LogicalPlan::Scan {
            table: "t".into(),
            schema: full_schema,
            projection: None,
        };
        // Project only uses col[0] (a) and col[2] (c) — col[1] (b) is unused.
        let proj_schema = Schema::new([
            Field::required("a", DataType::Int32),
            Field::required("c", DataType::Int32),
        ])
        .expect("schema ok");
        let plan = LogicalPlan::Project {
            input: Box::new(scan),
            exprs: vec![(col("a", 0), "a".into()), (col("c", 2), "c".into())],
            schema: proj_schema,
        };

        let rule = ProjectionPushdown;
        let result = rule.apply(&plan).expect("no error").expect("should push");

        // The result is still a Project...
        let LogicalPlan::Project { input, exprs, .. } = result else {
            panic!("expected Project");
        };
        // ...over a Scan with projection [0, 2].
        let LogicalPlan::Scan {
            projection,
            schema: scan_schema,
            ..
        } = *input
        else {
            panic!("expected Scan");
        };
        assert_eq!(projection, Some(vec![0, 2]));
        assert_eq!(scan_schema.len(), 2);
        // The project expressions should be reindexed: a→0, c→1.
        assert_eq!(exprs[0].0, col("a", 0));
        assert_eq!(exprs[1].0, col("c", 1));
    }

    // --- Edge-case: scan already projected ---

    #[test]
    fn does_not_double_push_already_projected_scan() {
        let scan = LogicalPlan::Scan {
            table: "t".into(),
            schema: Schema::new([Field::required("a", DataType::Int32)]).expect("schema ok"),
            projection: Some(vec![0]),
        };
        let proj_schema = Schema::new([Field::required("a", DataType::Int32)]).expect("schema ok");
        let plan = LogicalPlan::Project {
            input: Box::new(scan),
            exprs: vec![(col("a", 0), "a".into())],
            schema: proj_schema,
        };
        let rule = ProjectionPushdown;
        assert!(rule.apply(&plan).expect("no error").is_none());
    }

    // --- No-op: all columns used ---

    #[test]
    fn no_push_when_all_columns_used() {
        let schema = Schema::new([
            Field::required("a", DataType::Int32),
            Field::required("b", DataType::Int32),
        ])
        .expect("schema ok");
        let scan = LogicalPlan::Scan {
            table: "t".into(),
            schema: schema.clone(),
            projection: None,
        };
        let plan = LogicalPlan::Project {
            input: Box::new(scan),
            exprs: vec![(col("a", 0), "a".into()), (col("b", 1), "b".into())],
            schema,
        };
        let rule = ProjectionPushdown;
        assert!(rule.apply(&plan).expect("no error").is_none());
    }

    // --- No-op: scan not under project directly ---

    #[test]
    fn no_op_on_scan_alone() {
        let plan = LogicalPlan::Scan {
            table: "t".into(),
            schema: three_col_schema(),
            projection: None,
        };
        let rule = ProjectionPushdown;
        assert!(rule.apply(&plan).expect("no error").is_none());
    }
}
