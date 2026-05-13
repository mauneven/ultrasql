//! Outer-join elimination rewrite rule.
//!
//! [`OuterJoinElimination`] converts a `LEFT OUTER JOIN` to an `INNER JOIN`
//! when there is a wrapping filter that provably requires columns from the
//! right side to be non-NULL. In that case the outer join's NULL-padding
//! extension can never satisfy the filter, so the rows it generates are
//! eliminated anyway — which means the join might as well be inner.
//!
//! ## Conservative detection (v0.6)
//!
//! The rule detects the conservative subset of null-rejecting predicates:
//!
//! - `col IS NOT NULL` where `col` references a right-side column.
//! - `col = literal` (or any comparison) where `col` references a right-side
//!   column; comparisons with NULL return NULL (not TRUE), so they reject
//!   NULL-padded rows.
//!
//! Only `LeftOuter` → `Inner` is implemented. `RightOuter` and `FullOuter`
//! are left for a future wave.

use ultrasql_planner::{BinaryOp, LogicalJoinType, LogicalPlan, ScalarExpr};

use crate::error::OptimizeError;
use crate::rules::RewriteRule;

/// Converts LEFT OUTER JOINs to INNER JOINs when filter predicates imply that
/// NULL-padded rows would be rejected.
#[derive(Debug)]
pub struct OuterJoinElimination;

impl RewriteRule for OuterJoinElimination {
    fn name(&self) -> &'static str {
        "outer_join_elimination"
    }

    fn apply(&self, plan: &LogicalPlan) -> Result<Option<LogicalPlan>, OptimizeError> {
        eliminate(plan)
    }
}

#[allow(clippy::too_many_lines)]
fn eliminate(plan: &LogicalPlan) -> Result<Option<LogicalPlan>, OptimizeError> {
    match plan {
        // ---------------------------------------------------------------
        // Core case: Filter over LeftOuter Join
        // ---------------------------------------------------------------
        LogicalPlan::Filter { input, predicate }
            if matches!(
                input.as_ref(),
                LogicalPlan::Join {
                    join_type: LogicalJoinType::LeftOuter,
                    ..
                }
            ) =>
        {
            let LogicalPlan::Join {
                left,
                right,
                join_type: LogicalJoinType::LeftOuter,
                condition,
                schema,
            } = input.as_ref()
            else {
                unreachable!()
            };

            let left_width = left.schema().len();

            if predicate_rejects_null_on_right(predicate, left_width) {
                // Upgrade LEFT OUTER → INNER.
                return Ok(Some(LogicalPlan::Filter {
                    input: Box::new(LogicalPlan::Join {
                        left: left.clone(),
                        right: right.clone(),
                        join_type: LogicalJoinType::Inner,
                        condition: condition.clone(),
                        schema: schema.clone(),
                    }),
                    predicate: predicate.clone(),
                }));
            }

            // No elimination — recurse into children.
            let new_input = eliminate(input)?;
            Ok(new_input.map(|i| LogicalPlan::Filter {
                input: Box::new(i),
                predicate: predicate.clone(),
            }))
        }

        // ---------------------------------------------------------------
        // General recursion
        // ---------------------------------------------------------------
        LogicalPlan::Filter { input, predicate } => {
            let new_input = eliminate(input)?;
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
            let new_input = eliminate(input)?;
            Ok(new_input.map(|i| LogicalPlan::Project {
                input: Box::new(i),
                exprs: exprs.clone(),
                schema: schema.clone(),
            }))
        }

        LogicalPlan::Limit { input, n, offset } => {
            let new_input = eliminate(input)?;
            Ok(new_input.map(|i| LogicalPlan::Limit {
                input: Box::new(i),
                n: *n,
                offset: *offset,
            }))
        }

        LogicalPlan::Sort { input, keys } => {
            let new_input = eliminate(input)?;
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
            let new_left = eliminate(left)?;
            let new_right = eliminate(right)?;
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
            let new_input = eliminate(input)?;
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
// Predicate analysis
// ---------------------------------------------------------------------------

/// Returns `true` when `pred` provably requires at least one right-side
/// column (index ≥ `left_width`) to be non-NULL.
///
/// The conservative check recognises:
/// - `IS NOT NULL` directly on a right-side column.
/// - Any comparison (`=`, `<>`, `<`, `<=`, `>`, `>=`) whose left or right
///   operand is a right-side column reference; comparisons with NULL produce
///   NULL (not TRUE) under three-valued logic, so they act as null-rejectors.
fn predicate_rejects_null_on_right(pred: &ScalarExpr, left_width: usize) -> bool {
    match pred {
        ScalarExpr::IsNull {
            expr,
            negated: true,
        } => is_right_column(expr, left_width),

        ScalarExpr::Binary {
            op, left, right, ..
        } => {
            let is_cmp = matches!(
                op,
                BinaryOp::Eq
                    | BinaryOp::NotEq
                    | BinaryOp::Lt
                    | BinaryOp::LtEq
                    | BinaryOp::Gt
                    | BinaryOp::GtEq
            );
            if is_cmp {
                return is_right_column(left, left_width) || is_right_column(right, left_width);
            }
            // AND: either conjunct can reject.
            if matches!(op, BinaryOp::And) {
                return predicate_rejects_null_on_right(left, left_width)
                    || predicate_rejects_null_on_right(right, left_width);
            }
            false
        }

        _ => false,
    }
}

/// Returns `true` when `expr` is a column reference whose index falls in the
/// right side of a join (i.e. `index >= left_width`).
const fn is_right_column(expr: &ScalarExpr, left_width: usize) -> bool {
    matches!(expr, ScalarExpr::Column { index, .. } if *index >= left_width)
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

    fn col(name: &str, idx: usize) -> ScalarExpr {
        ScalarExpr::Column {
            name: name.to_owned(),
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

    fn two_col_schema(name_a: &str, name_b: &str) -> Schema {
        Schema::new([
            Field::required(name_a, DataType::Int32),
            Field::required(name_b, DataType::Int32),
        ])
        .expect("schema ok")
    }

    fn join_schema() -> Schema {
        Schema::new([
            Field::required("left_id", DataType::Int32),
            Field::required("left_val", DataType::Int32),
            Field::nullable("right_id", DataType::Int32),
            Field::nullable("right_val", DataType::Int32),
        ])
        .expect("schema ok")
    }

    fn left_outer_join() -> LogicalPlan {
        let l = LogicalPlan::Scan {
            table: "left_t".into(),
            schema: two_col_schema("left_id", "left_val"),
            projection: None,
        };
        let r = LogicalPlan::Scan {
            table: "right_t".into(),
            schema: two_col_schema("right_id", "right_val"),
            projection: None,
        };
        LogicalPlan::Join {
            left: Box::new(l),
            right: Box::new(r),
            join_type: LogicalJoinType::LeftOuter,
            condition: LogicalJoinCondition::On(eq(col("left_id", 0), col("right_id", 2))),
            schema: join_schema(),
        }
    }

    // --- Happy-path: IS NOT NULL on right column ---

    #[test]
    fn eliminates_left_outer_when_right_is_not_null_predicate() {
        let join = left_outer_join();
        let filter = LogicalPlan::Filter {
            input: Box::new(join),
            predicate: ScalarExpr::IsNull {
                expr: Box::new(col("right_id", 2)),
                negated: true,
            },
        };
        let rule = OuterJoinElimination;
        let result = rule.apply(&filter).expect("no error");
        assert!(result.is_some(), "should eliminate outer join");
        if let Some(LogicalPlan::Filter { input, .. }) = result {
            if let LogicalPlan::Join { join_type, .. } = *input {
                assert_eq!(join_type, LogicalJoinType::Inner);
            } else {
                panic!("expected Join inside Filter");
            }
        }
    }

    // --- Happy-path: comparison on right column ---

    #[test]
    fn eliminates_left_outer_when_right_column_compared_to_literal() {
        let join = left_outer_join();
        let filter = LogicalPlan::Filter {
            input: Box::new(join),
            predicate: eq(col("right_val", 3), lit_i32(42)),
        };
        let rule = OuterJoinElimination;
        let result = rule.apply(&filter).expect("no error");
        assert!(
            result.is_some(),
            "comparison on right side should eliminate outer join"
        );
        if let Some(LogicalPlan::Filter { input, .. }) = result {
            if let LogicalPlan::Join { join_type, .. } = *input {
                assert_eq!(join_type, LogicalJoinType::Inner);
            } else {
                panic!("expected Join inside Filter");
            }
        }
    }

    // --- Edge-case: predicate only references left side — no elimination ---

    #[test]
    fn does_not_eliminate_when_predicate_only_touches_left() {
        let join = left_outer_join();
        let filter = LogicalPlan::Filter {
            input: Box::new(join),
            predicate: eq(col("left_id", 0), lit_i32(1)),
        };
        let rule = OuterJoinElimination;
        let result = rule.apply(&filter).expect("no error");
        // May return Some (from child recursion) but any Join must still be LeftOuter.
        if let Some(LogicalPlan::Filter { input, .. }) = result {
            if let LogicalPlan::Join { join_type, .. } = *input {
                assert_eq!(
                    join_type,
                    LogicalJoinType::LeftOuter,
                    "join should remain LeftOuter"
                );
            }
        }
    }

    // --- No-op: inner join already ---

    #[test]
    fn no_op_on_inner_join() {
        let l = LogicalPlan::Scan {
            table: "l".into(),
            schema: two_col_schema("a", "b"),
            projection: None,
        };
        let r = LogicalPlan::Scan {
            table: "r".into(),
            schema: two_col_schema("c", "d"),
            projection: None,
        };
        let join_sch = Schema::new([
            Field::required("a", DataType::Int32),
            Field::required("b", DataType::Int32),
            Field::required("c", DataType::Int32),
            Field::required("d", DataType::Int32),
        ])
        .expect("schema ok");
        let plan = LogicalPlan::Filter {
            input: Box::new(LogicalPlan::Join {
                left: Box::new(l),
                right: Box::new(r),
                join_type: LogicalJoinType::Inner,
                condition: LogicalJoinCondition::None,
                schema: join_sch,
            }),
            predicate: eq(col("c", 2), lit_i32(5)),
        };
        let rule = OuterJoinElimination;
        // No elimination on inner join; may recurse but result is None or
        // unchanged join type.
        let result = rule.apply(&plan).expect("no error");
        if let Some(LogicalPlan::Filter { input, .. }) = result {
            if let LogicalPlan::Join { join_type, .. } = *input {
                assert_eq!(join_type, LogicalJoinType::Inner);
            }
        }
    }
}
