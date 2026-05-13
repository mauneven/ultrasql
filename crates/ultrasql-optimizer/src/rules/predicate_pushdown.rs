//! Predicate pushdown rewrite rule.
//!
//! [`PredicatePushdown`] moves filter predicates closer to their data sources.
//! In v0.6 the rule handles two cases:
//!
//! 1. **Filter over Project.** When the filter predicate only references
//!    columns that originate from the child of the Project (not synthetic
//!    projection expressions), push the filter below the Project so it runs on
//!    fewer rows before any expression evaluation.
//!
//! 2. **Filter over Join.** Split the filter's top-level AND conjuncts; push
//!    each conjunct that references only the left side into a new
//!    `Filter(left)`, and each conjunct that references only the right side
//!    into a new `Filter(right)`. Conjuncts that reference both sides are
//!    merged into the join's ON condition via AND.

use std::collections::HashSet;

use ultrasql_planner::{BinaryOp, LogicalJoinCondition, LogicalPlan, ScalarExpr};

use crate::error::OptimizeError;
use crate::rules::RewriteRule;

/// Pushes filter predicates through projections and into join sides.
///
/// The rule is safe: it only moves predicates that cannot reference
/// synthesised projection columns. The correctness invariant is that
/// `column_index` references in the predicate must resolve to the same field
/// both before and after the rewrite.
#[derive(Debug)]
pub struct PredicatePushdown;

impl RewriteRule for PredicatePushdown {
    fn name(&self) -> &'static str {
        "predicate_pushdown"
    }

    fn apply(&self, plan: &LogicalPlan) -> Result<Option<LogicalPlan>, OptimizeError> {
        push_down(plan)
    }
}

#[allow(clippy::too_many_lines)]
fn push_down(plan: &LogicalPlan) -> Result<Option<LogicalPlan>, OptimizeError> {
    match plan {
        // ---------------------------------------------------------------
        // Case 1: Filter over Project
        // ---------------------------------------------------------------
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

            // Determine which column indices the predicate references.
            let pred_refs = referenced_columns(predicate);

            // Determine which indices in the project output are "pass-through"
            // columns from the child — i.e. `ScalarExpr::Column { index, .. }`
            // with no further computation. Build a mapping: project output idx
            // → child input idx.
            let passthrough: HashSet<usize> = exprs
                .iter()
                .enumerate()
                .filter_map(|(out_idx, (e, _))| {
                    if matches!(e, ScalarExpr::Column { .. }) && pred_refs.contains(&out_idx) {
                        Some(out_idx)
                    } else {
                        None
                    }
                })
                .collect();

            // All references must map to pass-through columns.
            if pred_refs.is_empty() || pred_refs != passthrough {
                // Cannot push; recurse into children instead.
                let new_input = push_down(input)?;
                return Ok(new_input.map(|i| LogicalPlan::Filter {
                    input: Box::new(i),
                    predicate: predicate.clone(),
                }));
            }

            // Rewrite the predicate: replace column indices from the project
            // output to the corresponding child column indices.
            let remapped = remap_predicate(predicate, exprs);

            // Recursively push the remapped predicate into the project's child.
            let new_inner = push_down(&LogicalPlan::Filter {
                input: proj_input.clone(),
                predicate: remapped.clone(),
            })?;

            let filter_below = new_inner.unwrap_or_else(|| LogicalPlan::Filter {
                input: proj_input.clone(),
                predicate: remapped,
            });

            Ok(Some(LogicalPlan::Project {
                input: Box::new(filter_below),
                exprs: exprs.clone(),
                schema: schema.clone(),
            }))
        }

        // ---------------------------------------------------------------
        // Case 2: Filter over Join
        // ---------------------------------------------------------------
        LogicalPlan::Filter { input, predicate }
            if matches!(input.as_ref(), LogicalPlan::Join { .. }) =>
        {
            let LogicalPlan::Join {
                left,
                right,
                join_type,
                condition,
                schema,
            } = input.as_ref()
            else {
                unreachable!()
            };

            let left_width = left.schema().len();

            // Split the predicate into top-level AND conjuncts.
            let conjuncts = split_and(predicate);

            let mut left_preds: Vec<ScalarExpr> = Vec::new();
            let mut right_preds: Vec<ScalarExpr> = Vec::new();
            let mut join_preds: Vec<ScalarExpr> = Vec::new();

            for c in conjuncts {
                let refs = referenced_columns(&c);
                let touches_left = refs.iter().any(|&i| i < left_width);
                let touches_right = refs.iter().any(|&i| i >= left_width);
                match (touches_left, touches_right) {
                    (true, false) => left_preds.push(c),
                    (false, true) => {
                        // Remap right-side indices to 0-based within the right
                        // child's schema.
                        let remapped = remap_right_indices(&c, left_width);
                        right_preds.push(remapped);
                    }
                    _ => join_preds.push(c),
                }
            }

            let made_progress =
                !left_preds.is_empty() || !right_preds.is_empty() || !join_preds.is_empty();
            // If everything ended up in join_preds with no pushdown, recurse
            // into children only.
            if !left_preds.is_empty() || !right_preds.is_empty() {
                let new_left = if left_preds.is_empty() {
                    *left.clone()
                } else {
                    LogicalPlan::Filter {
                        input: left.clone(),
                        predicate: conjuncts_to_and(left_preds),
                    }
                };

                let new_right = if right_preds.is_empty() {
                    *right.clone()
                } else {
                    LogicalPlan::Filter {
                        input: right.clone(),
                        predicate: conjuncts_to_and(right_preds),
                    }
                };

                // Merge join_preds into the existing join condition.
                let new_condition = if join_preds.is_empty() {
                    condition.clone()
                } else {
                    let extra = conjuncts_to_and(join_preds);
                    match condition {
                        LogicalJoinCondition::On(existing) => {
                            LogicalJoinCondition::On(ScalarExpr::Binary {
                                op: BinaryOp::And,
                                left: Box::new(existing.clone()),
                                right: Box::new(extra),
                                data_type: ultrasql_core::DataType::Bool,
                            })
                        }
                        LogicalJoinCondition::None | LogicalJoinCondition::Using(_) => {
                            LogicalJoinCondition::On(extra)
                        }
                    }
                };

                return Ok(Some(LogicalPlan::Join {
                    left: Box::new(new_left),
                    right: Box::new(new_right),
                    join_type: *join_type,
                    condition: new_condition,
                    schema: schema.clone(),
                }));
            }

            // Nothing pushed — recurse into the join children.
            let _ = made_progress; // used only for the branch above
            let new_left = push_down(left)?;
            let new_right = push_down(right)?;
            if new_left.is_none() && new_right.is_none() {
                return Ok(None);
            }
            Ok(Some(LogicalPlan::Filter {
                input: Box::new(LogicalPlan::Join {
                    left: Box::new(new_left.unwrap_or_else(|| *left.clone())),
                    right: Box::new(new_right.unwrap_or_else(|| *right.clone())),
                    join_type: *join_type,
                    condition: condition.clone(),
                    schema: schema.clone(),
                }),
                predicate: predicate.clone(),
            }))
        }

        // ---------------------------------------------------------------
        // General recursion
        // ---------------------------------------------------------------
        LogicalPlan::Filter { input, predicate } => {
            let new_input = push_down(input)?;
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
            let new_input = push_down(input)?;
            Ok(new_input.map(|i| LogicalPlan::Project {
                input: Box::new(i),
                exprs: exprs.clone(),
                schema: schema.clone(),
            }))
        }

        LogicalPlan::Limit { input, n, offset } => {
            let new_input = push_down(input)?;
            Ok(new_input.map(|i| LogicalPlan::Limit {
                input: Box::new(i),
                n: *n,
                offset: *offset,
            }))
        }

        LogicalPlan::Sort { input, keys } => {
            let new_input = push_down(input)?;
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
            let new_left = push_down(left)?;
            let new_right = push_down(right)?;
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
            let new_input = push_down(input)?;
            Ok(new_input.map(|i| LogicalPlan::Aggregate {
                input: Box::new(i),
                group_by: group_by.clone(),
                aggregates: aggregates.clone(),
                schema: schema.clone(),
            }))
        }

        // Leaf/mutation nodes: no push-down.
        _ => Ok(None),
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Collect all column indices referenced by `expr`.
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
        ScalarExpr::Unary { expr, .. } | ScalarExpr::IsNull { expr, .. } => {
            collect_cols(expr, out);
        }
        ScalarExpr::Literal { .. } | ScalarExpr::Parameter { .. } => {}
    }
}

/// Replace column indices in `predicate` according to the project's expression
/// list. Each output column index `i` that is a bare column reference maps to
/// `exprs[i].0`'s inner index.
fn remap_predicate(predicate: &ScalarExpr, exprs: &[(ScalarExpr, String)]) -> ScalarExpr {
    remap_expr(predicate, exprs)
}

fn remap_expr(expr: &ScalarExpr, exprs: &[(ScalarExpr, String)]) -> ScalarExpr {
    match expr {
        ScalarExpr::Column { index, .. } => {
            // Replace this column with the underlying child expression.
            if let Some((child_e, _)) = exprs.get(*index) {
                child_e.clone()
            } else {
                expr.clone()
            }
        }
        ScalarExpr::Binary {
            op,
            left,
            right,
            data_type,
        } => ScalarExpr::Binary {
            op: *op,
            left: Box::new(remap_expr(left, exprs)),
            right: Box::new(remap_expr(right, exprs)),
            data_type: data_type.clone(),
        },
        ScalarExpr::Unary {
            op,
            expr: inner,
            data_type,
        } => ScalarExpr::Unary {
            op: *op,
            expr: Box::new(remap_expr(inner, exprs)),
            data_type: data_type.clone(),
        },
        ScalarExpr::IsNull {
            expr: inner,
            negated,
        } => ScalarExpr::IsNull {
            expr: Box::new(remap_expr(inner, exprs)),
            negated: *negated,
        },
        ScalarExpr::Literal { .. } | ScalarExpr::Parameter { .. } => expr.clone(),
    }
}

/// Remap right-side column indices in a predicate.
/// After a join the right side starts at `left_width`; when we push a
/// predicate to the right child we subtract `left_width` from each index.
fn remap_right_indices(expr: &ScalarExpr, left_width: usize) -> ScalarExpr {
    match expr {
        ScalarExpr::Column {
            name,
            index,
            data_type,
        } => ScalarExpr::Column {
            name: name.clone(),
            index: index.saturating_sub(left_width),
            data_type: data_type.clone(),
        },
        ScalarExpr::Binary {
            op,
            left,
            right,
            data_type,
        } => ScalarExpr::Binary {
            op: *op,
            left: Box::new(remap_right_indices(left, left_width)),
            right: Box::new(remap_right_indices(right, left_width)),
            data_type: data_type.clone(),
        },
        ScalarExpr::Unary {
            op,
            expr: inner,
            data_type,
        } => ScalarExpr::Unary {
            op: *op,
            expr: Box::new(remap_right_indices(inner, left_width)),
            data_type: data_type.clone(),
        },
        ScalarExpr::IsNull {
            expr: inner,
            negated,
        } => ScalarExpr::IsNull {
            expr: Box::new(remap_right_indices(inner, left_width)),
            negated: *negated,
        },
        ScalarExpr::Literal { .. } | ScalarExpr::Parameter { .. } => expr.clone(),
    }
}

/// Split a tree of top-level `AND` nodes into individual conjuncts.
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

/// Fold a slice of conjuncts back into a left-deep AND tree.
///
/// Panics if the slice is empty (the rule never produces an empty list).
fn conjuncts_to_and(mut preds: Vec<ScalarExpr>) -> ScalarExpr {
    assert!(!preds.is_empty(), "conjuncts_to_and called with empty list");
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use ultrasql_core::{DataType, Field, Schema, Value};
    use ultrasql_planner::{
        BinaryOp, LogicalJoinCondition, LogicalJoinType, LogicalPlan, ScalarExpr,
    };

    use super::*;
    use crate::rules::RewriteRule;

    fn lit_i32(v: i32) -> ScalarExpr {
        ScalarExpr::Literal {
            value: Value::Int32(v),
            data_type: DataType::Int32,
        }
    }

    fn col(name: &str, idx: usize) -> ScalarExpr {
        ScalarExpr::Column {
            name: name.to_owned(),
            index: idx,
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

    fn users_schema() -> Schema {
        Schema::new([
            Field::required("id", DataType::Int32),
            Field::nullable("name", DataType::Text { max_len: None }),
        ])
        .expect("schema ok")
    }

    fn orders_schema() -> Schema {
        Schema::new([
            Field::required("order_id", DataType::Int32),
            Field::required("user_id", DataType::Int32),
        ])
        .expect("schema ok")
    }

    // --- Happy-path: Filter over Project ---

    #[test]
    fn pushes_filter_below_project_when_passthrough_column() {
        let scan = LogicalPlan::Scan {
            table: "users".into(),
            schema: users_schema(),
            projection: None,
        };
        // Project col[0] (id) as "id"
        let proj_schema = Schema::new([Field::required("id", DataType::Int32)]).expect("schema ok");
        let project = LogicalPlan::Project {
            input: Box::new(scan),
            exprs: vec![(col("id", 0), "id".into())],
            schema: proj_schema,
        };
        // Filter: output col[0] = 42  (project output index 0)
        let filter = LogicalPlan::Filter {
            input: Box::new(project),
            predicate: eq(col("id", 0), lit_i32(42)),
        };

        let rule = PredicatePushdown;
        let result = rule.apply(&filter).expect("no error");
        assert!(result.is_some(), "should have pushed down");
        // The result should be Project(Filter(Scan))
        let result = result.unwrap();
        assert!(
            matches!(result, LogicalPlan::Project { .. }),
            "top node should be Project"
        );
        if let LogicalPlan::Project { input, .. } = &result {
            assert!(
                matches!(input.as_ref(), LogicalPlan::Filter { .. }),
                "Project input should be Filter, got {input:?}"
            );
        }
    }

    // --- Happy-path: Filter over Join ---

    #[test]
    fn pushes_left_only_predicate_into_join_left() {
        let u_scan = LogicalPlan::Scan {
            table: "users".into(),
            schema: users_schema(),
            projection: None,
        };
        let o_scan = LogicalPlan::Scan {
            table: "orders".into(),
            schema: orders_schema(),
            projection: None,
        };
        let join_schema = Schema::new([
            Field::required("id", DataType::Int32),
            Field::nullable("name", DataType::Text { max_len: None }),
            Field::required("order_id", DataType::Int32),
            Field::required("user_id", DataType::Int32),
        ])
        .expect("schema ok");
        let join = LogicalPlan::Join {
            left: Box::new(u_scan),
            right: Box::new(o_scan),
            join_type: LogicalJoinType::Inner,
            condition: LogicalJoinCondition::On(eq(col("id", 0), col("user_id", 3))),
            schema: join_schema,
        };
        // Filter: col[0] (users.id) = 5 — only touches left side.
        let filter = LogicalPlan::Filter {
            input: Box::new(join),
            predicate: eq(col("id", 0), lit_i32(5)),
        };

        let rule = PredicatePushdown;
        let result = rule.apply(&filter).expect("no error");
        assert!(result.is_some());
        // Result should be a Join (no outer Filter remaining).
        assert!(
            matches!(result.unwrap(), LogicalPlan::Join { .. }),
            "result should be Join with filter pushed in"
        );
    }

    // --- Edge-case: predicate references synthesised expression ---

    #[test]
    fn does_not_push_when_predicate_references_computed_expr() {
        let scan = LogicalPlan::Scan {
            table: "users".into(),
            schema: users_schema(),
            projection: None,
        };
        // Project: output col[0] = col[0] + col[1] (computed, not pass-through)
        let proj_schema =
            Schema::new([Field::nullable("computed", DataType::Int32)]).expect("schema ok");
        let project = LogicalPlan::Project {
            input: Box::new(scan),
            exprs: vec![(
                ScalarExpr::Binary {
                    op: BinaryOp::Add,
                    left: Box::new(col("id", 0)),
                    right: Box::new(col("id", 1)),
                    data_type: DataType::Int32,
                },
                "computed".into(),
            )],
            schema: proj_schema,
        };
        let filter = LogicalPlan::Filter {
            input: Box::new(project),
            predicate: eq(col("computed", 0), lit_i32(5)),
        };
        let rule = PredicatePushdown;
        let result = rule.apply(&filter).expect("no error");
        // Should not push below — the rule should return None or an unchanged Filter.
        if let Some(r) = result {
            // If a rewrite happened, the top node should still be Filter.
            assert!(matches!(r, LogicalPlan::Filter { .. }));
        }
    }

    // --- No-op test ---

    #[test]
    fn no_op_on_scan() {
        let schema = users_schema();
        let plan = LogicalPlan::Scan {
            table: "users".into(),
            schema,
            projection: None,
        };
        let rule = PredicatePushdown;
        assert!(rule.apply(&plan).expect("no error").is_none());
    }
}
