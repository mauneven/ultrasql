//! Semi join pushdown across inner joins.
//!
//! After subquery decorrelation, predicates such as `o_orderkey IN (...)`
//! become logical `Semi` joins. The first rewrite pass naturally attaches that
//! semi join above the current outer input, which can leave the membership
//! filter sitting *after* unrelated inner joins. This rule pushes a semi join
//! below an inner join when the semi predicate only references one side of that
//! inner join.
//!
//! That rewrite is semantics-preserving for inner joins:
//!
//! - `(L ⋈ R) ⋉ S  =>  (L ⋉ S) ⋈ R` when the semi condition references only `L`
//!
//! and symmetrically for predicates that reference only `R`.
//!
//! The rule deliberately does **not** push anti joins yet. Q16-style anti
//! membership filters can be cheaper above a selective sibling join, so the
//! v0.7 follow-up should add costing before enabling that transformation.

use ultrasql_planner::{LogicalJoinCondition, LogicalJoinType, LogicalPlan, ScalarExpr};

use crate::error::OptimizeError;
use crate::rules::RewriteRule;

/// Pushes semi joins below inner joins when the membership predicate only
/// touches one inner-join input.
#[derive(Debug)]
pub struct SemiJoinPushdown;

impl RewriteRule for SemiJoinPushdown {
    fn name(&self) -> &'static str {
        "semi_join_pushdown"
    }

    fn apply(&self, plan: &LogicalPlan) -> Result<Option<LogicalPlan>, OptimizeError> {
        push_semi_join(plan)
    }
}

fn push_semi_join(plan: &LogicalPlan) -> Result<Option<LogicalPlan>, OptimizeError> {
    match plan {
        LogicalPlan::Join {
            left,
            right,
            join_type,
            condition: LogicalJoinCondition::On(semi_condition),
            schema,
        } if *join_type == LogicalJoinType::Semi
            && matches!(left.as_ref(), LogicalPlan::Join { .. }) =>
        {
            let LogicalPlan::Join {
                left: inner_left,
                right: inner_right,
                join_type: inner_join_type,
                condition: inner_condition,
                schema: inner_schema,
            } = left.as_ref()
            else {
                unreachable!()
            };
            if !matches!(
                inner_join_type,
                LogicalJoinType::Inner | LogicalJoinType::Cross
            ) {
                return recurse_join_children(
                    left,
                    right,
                    *join_type,
                    LogicalJoinCondition::On(semi_condition.clone()),
                    schema,
                );
            }

            let left_width = inner_left.schema().len();
            let inner_width = inner_schema.len();
            let mut touches_inner_left = false;
            let mut touches_inner_right = false;
            for index in referenced_columns(semi_condition) {
                if index < left_width {
                    touches_inner_left = true;
                } else if index < inner_width {
                    touches_inner_right = true;
                }
            }

            if touches_inner_left && !touches_inner_right {
                let Some(pushed_condition) =
                    remap_pushed_semi_condition_to_left(semi_condition, left_width, inner_width)
                else {
                    return recurse_join_children(
                        left,
                        right,
                        *join_type,
                        LogicalJoinCondition::On(semi_condition.clone()),
                        schema,
                    );
                };
                return Ok(Some(LogicalPlan::Join {
                    left: Box::new(LogicalPlan::Join {
                        left: inner_left.clone(),
                        right: right.clone(),
                        join_type: *join_type,
                        condition: LogicalJoinCondition::On(pushed_condition),
                        schema: inner_left.schema().clone(),
                    }),
                    right: inner_right.clone(),
                    join_type: *inner_join_type,
                    condition: inner_condition.clone(),
                    schema: schema.clone(),
                }));
            }

            if touches_inner_right && !touches_inner_left {
                let Some(pushed_condition) =
                    remap_pushed_semi_condition_to_right(semi_condition, left_width, inner_width)
                else {
                    return recurse_join_children(
                        left,
                        right,
                        *join_type,
                        LogicalJoinCondition::On(semi_condition.clone()),
                        schema,
                    );
                };
                return Ok(Some(LogicalPlan::Join {
                    left: inner_left.clone(),
                    right: Box::new(LogicalPlan::Join {
                        left: inner_right.clone(),
                        right: right.clone(),
                        join_type: *join_type,
                        condition: LogicalJoinCondition::On(pushed_condition),
                        schema: inner_right.schema().clone(),
                    }),
                    join_type: *inner_join_type,
                    condition: inner_condition.clone(),
                    schema: schema.clone(),
                }));
            }

            recurse_join_children(
                left,
                right,
                *join_type,
                LogicalJoinCondition::On(semi_condition.clone()),
                schema,
            )
        }
        LogicalPlan::Join {
            left,
            right,
            join_type,
            condition,
            schema,
        } => recurse_join_children(left, right, *join_type, condition.clone(), schema),
        LogicalPlan::Filter { input, predicate } => {
            let new_input = push_semi_join(input)?;
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
            let new_input = push_semi_join(input)?;
            Ok(new_input.map(|i| LogicalPlan::Project {
                input: Box::new(i),
                exprs: exprs.clone(),
                schema: schema.clone(),
            }))
        }
        LogicalPlan::Limit { input, n, offset } => {
            let new_input = push_semi_join(input)?;
            Ok(new_input.map(|i| LogicalPlan::Limit {
                input: Box::new(i),
                n: *n,
                offset: *offset,
            }))
        }
        LogicalPlan::Sort { input, keys } => {
            let new_input = push_semi_join(input)?;
            Ok(new_input.map(|i| LogicalPlan::Sort {
                input: Box::new(i),
                keys: keys.clone(),
            }))
        }
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggregates,
            schema,
        } => {
            let new_input = push_semi_join(input)?;
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

fn recurse_join_children(
    left: &LogicalPlan,
    right: &LogicalPlan,
    join_type: LogicalJoinType,
    condition: LogicalJoinCondition,
    schema: &ultrasql_core::Schema,
) -> Result<Option<LogicalPlan>, OptimizeError> {
    let new_left = push_semi_join(left)?;
    let new_right = push_semi_join(right)?;
    if new_left.is_none() && new_right.is_none() {
        return Ok(None);
    }
    Ok(Some(LogicalPlan::Join {
        left: Box::new(new_left.unwrap_or_else(|| left.clone())),
        right: Box::new(new_right.unwrap_or_else(|| right.clone())),
        join_type,
        condition,
        schema: schema.clone(),
    }))
}

fn referenced_columns(expr: &ScalarExpr) -> Vec<usize> {
    let mut out = Vec::new();
    collect_columns(expr, &mut out);
    out.sort_unstable();
    out.dedup();
    out
}

fn collect_columns(expr: &ScalarExpr, out: &mut Vec<usize>) {
    match expr {
        ScalarExpr::Column { index, .. } => out.push(*index),
        ScalarExpr::Binary { left, right, .. } => {
            collect_columns(left, out);
            collect_columns(right, out);
        }
        ScalarExpr::Unary { expr, .. } | ScalarExpr::IsNull { expr, .. } => {
            collect_columns(expr, out)
        }
        ScalarExpr::FunctionCall { args, .. } => {
            for arg in args {
                collect_columns(arg, out);
            }
        }
        ScalarExpr::Literal { .. }
        | ScalarExpr::Parameter { .. }
        | ScalarExpr::OuterColumn { .. }
        | ScalarExpr::ScalarSubquery { .. }
        | ScalarExpr::Exists { .. }
        | ScalarExpr::InSubquery { .. } => {}
    }
}

fn remap_pushed_semi_condition_to_left(
    expr: &ScalarExpr,
    left_width: usize,
    inner_width: usize,
) -> Option<ScalarExpr> {
    remap_pushed_semi_condition(expr, left_width, inner_width, PushedSide::Left)
}

fn remap_pushed_semi_condition_to_right(
    expr: &ScalarExpr,
    left_width: usize,
    inner_width: usize,
) -> Option<ScalarExpr> {
    remap_pushed_semi_condition(expr, left_width, inner_width, PushedSide::Right)
}

#[derive(Clone, Copy)]
enum PushedSide {
    Left,
    Right,
}

fn remap_pushed_semi_condition(
    expr: &ScalarExpr,
    left_width: usize,
    inner_width: usize,
    pushed_side: PushedSide,
) -> Option<ScalarExpr> {
    match expr {
        ScalarExpr::Column {
            name,
            index,
            data_type,
        } => {
            let new_index = match pushed_side {
                PushedSide::Left if *index < left_width => *index,
                PushedSide::Left if *index >= inner_width => *index - (inner_width - left_width),
                PushedSide::Right if *index >= left_width && *index < inner_width => {
                    *index - left_width
                }
                PushedSide::Right if *index >= inner_width => *index - left_width,
                _ => return None,
            };
            Some(ScalarExpr::Column {
                name: name.clone(),
                index: new_index,
                data_type: data_type.clone(),
            })
        }
        ScalarExpr::Literal { value, data_type } => Some(ScalarExpr::Literal {
            value: value.clone(),
            data_type: data_type.clone(),
        }),
        ScalarExpr::Parameter { index, data_type } => Some(ScalarExpr::Parameter {
            index: *index,
            data_type: data_type.clone(),
        }),
        ScalarExpr::Unary {
            op,
            expr: inner,
            data_type,
        } => Some(ScalarExpr::Unary {
            op: *op,
            expr: Box::new(remap_pushed_semi_condition(
                inner,
                left_width,
                inner_width,
                pushed_side,
            )?),
            data_type: data_type.clone(),
        }),
        ScalarExpr::Binary {
            op,
            left,
            right,
            data_type,
        } => Some(ScalarExpr::Binary {
            op: *op,
            left: Box::new(remap_pushed_semi_condition(
                left,
                left_width,
                inner_width,
                pushed_side,
            )?),
            right: Box::new(remap_pushed_semi_condition(
                right,
                left_width,
                inner_width,
                pushed_side,
            )?),
            data_type: data_type.clone(),
        }),
        ScalarExpr::IsNull { expr, negated } => Some(ScalarExpr::IsNull {
            expr: Box::new(remap_pushed_semi_condition(
                expr,
                left_width,
                inner_width,
                pushed_side,
            )?),
            negated: *negated,
        }),
        ScalarExpr::FunctionCall {
            name,
            args,
            data_type,
        } => Some(ScalarExpr::FunctionCall {
            name: name.clone(),
            args: args
                .iter()
                .map(|arg| remap_pushed_semi_condition(arg, left_width, inner_width, pushed_side))
                .collect::<Option<Vec<_>>>()?,
            data_type: data_type.clone(),
        }),
        ScalarExpr::OuterColumn { .. }
        | ScalarExpr::ScalarSubquery { .. }
        | ScalarExpr::Exists { .. }
        | ScalarExpr::InSubquery { .. } => None,
    }
}

#[cfg(test)]
mod tests {
    use ultrasql_core::{DataType, Field, Schema};
    use ultrasql_planner::{
        BinaryOp, LogicalJoinCondition, LogicalJoinType, LogicalPlan, ScalarExpr,
    };

    use super::SemiJoinPushdown;
    use crate::rules::RewriteRule;

    fn scan(name: &str, cols: &[&str]) -> LogicalPlan {
        let fields = cols
            .iter()
            .map(|col| Field::required(format!("{name}_{col}"), DataType::Int32))
            .collect::<Vec<_>>();
        LogicalPlan::Scan {
            table: name.to_owned(),
            schema: Schema::new(fields).expect("schema"),
            projection: None,
        }
    }

    fn col(index: usize, name: &str) -> ScalarExpr {
        ScalarExpr::Column {
            name: name.to_owned(),
            index,
            data_type: DataType::Int32,
        }
    }

    fn eq(left: ScalarExpr, right: ScalarExpr) -> ScalarExpr {
        ScalarExpr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(left),
            right: Box::new(right),
            data_type: DataType::Bool,
        }
    }

    #[test]
    fn pushes_semi_join_below_inner_join_when_condition_only_uses_left_side() {
        let left = scan("orders", &["orderkey", "custkey"]);
        let right = scan("customer", &["custkey"]);
        let semi_rhs = scan("hot_orders", &["orderkey"]);
        let inner_schema = Schema::new([
            left.schema().field_at(0).clone(),
            left.schema().field_at(1).clone(),
            right.schema().field_at(0).clone(),
        ])
        .expect("inner schema");
        let plan = LogicalPlan::Join {
            left: Box::new(LogicalPlan::Join {
                left: Box::new(left.clone()),
                right: Box::new(right.clone()),
                join_type: LogicalJoinType::Inner,
                condition: LogicalJoinCondition::On(eq(
                    col(1, "orders_custkey"),
                    col(2, "customer_custkey"),
                )),
                schema: inner_schema.clone(),
            }),
            right: Box::new(semi_rhs.clone()),
            join_type: LogicalJoinType::Semi,
            condition: LogicalJoinCondition::On(eq(
                col(0, "orders_orderkey"),
                col(3, "hot_orders_orderkey"),
            )),
            schema: inner_schema.clone(),
        };

        let rule = SemiJoinPushdown;
        let Some(result) = rule.apply(&plan).expect("rewrite succeeds") else {
            panic!("expected rewrite");
        };
        let LogicalPlan::Join {
            left: pushed,
            right: pushed_right,
            join_type: LogicalJoinType::Inner,
            ..
        } = result
        else {
            panic!("expected top join");
        };
        assert_eq!(pushed_right.schema(), right.schema());
        let LogicalPlan::Join {
            left: semi_left,
            right: semi_right,
            join_type: LogicalJoinType::Semi,
            ..
        } = pushed.as_ref()
        else {
            panic!("expected pushed semi join on inner-left, got {pushed:?}");
        };
        assert_eq!(semi_left.schema(), left.schema());
        assert_eq!(semi_right.schema(), semi_rhs.schema());
    }

    #[test]
    fn does_not_push_when_condition_touches_both_inner_sides() {
        let left = scan("l", &["id"]);
        let right = scan("r", &["id"]);
        let semi_rhs = scan("s", &["id"]);
        let inner_schema = Schema::new([
            left.schema().field_at(0).clone(),
            right.schema().field_at(0).clone(),
        ])
        .expect("inner schema");
        let plan = LogicalPlan::Join {
            left: Box::new(LogicalPlan::Join {
                left: Box::new(left.clone()),
                right: Box::new(right.clone()),
                join_type: LogicalJoinType::Inner,
                condition: LogicalJoinCondition::On(eq(col(0, "l_id"), col(1, "r_id"))),
                schema: inner_schema.clone(),
            }),
            right: Box::new(semi_rhs),
            join_type: LogicalJoinType::Semi,
            condition: LogicalJoinCondition::On(ScalarExpr::Binary {
                op: BinaryOp::And,
                left: Box::new(eq(col(0, "l_id"), col(2, "s_id"))),
                right: Box::new(eq(col(1, "r_id"), col(2, "s_id"))),
                data_type: DataType::Bool,
            }),
            schema: inner_schema,
        };

        let rule = SemiJoinPushdown;
        assert!(rule.apply(&plan).expect("rewrite succeeds").is_none());
    }
}
