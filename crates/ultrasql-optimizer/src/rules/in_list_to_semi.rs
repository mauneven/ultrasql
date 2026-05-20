//! IN-list rewrite rule.
//!
//! [`InListToSemi`] collapses a disjunction of equality comparisons on the
//! same column into a more efficient representation.
//!
//! ## OR-list collapse (implemented in v0.6)
//!
//! A filter predicate of the form:
//!
//! ```text
//! col = lit1 OR col = lit2 OR col = lit3 [OR ...]
//! ```
//!
//! with at least 3 equality disjuncts on the same column is rewritten to a
//! `Binary(Or, ...)` tree where each leaf is a direct `Eq(col, lit)`.  In a
//! future execution wave this can be lowered to a hash-set membership check.
//!
//! The current rewrite is structure-preserving: the OR tree is rebuilt with
//! the original disjuncts. This makes the transformation a no-op on the AST
//! level but marks the pattern as recognized for the executor or a later pass.
//!
//! > **Note** — because [`ultrasql_planner::ScalarExpr`] has no `InList`
//! > variant yet, the rule stores the collapsed list back as an OR-tree. A
//! > future RFC that adds `ScalarExpr::InList` will change the lowering here.
//!
//! ## IN-subquery to semi-join
//!
//! `IN (subquery)` lowering now lives in [`super::SubqueryDecorrelation`],
//! which emits logical `Semi` / `Anti` joins once the binder has produced an
//! explicit subquery expression. This rule remains focused on literal OR-list
//! membership shapes.

use ultrasql_planner::{BinaryOp, LogicalPlan, ScalarExpr};

use crate::error::OptimizeError;
use crate::rules::RewriteRule;

/// Collapses OR-trees of equality comparisons on the same column when there
/// are at least 3 disjuncts.
#[derive(Debug)]
pub struct InListToSemi;

/// Minimum number of equality disjuncts required before the rule fires.
const MIN_DISJUNCTS: usize = 3;

impl RewriteRule for InListToSemi {
    fn name(&self) -> &'static str {
        "in_list_to_semi"
    }

    fn apply(&self, plan: &LogicalPlan) -> Result<Option<LogicalPlan>, OptimizeError> {
        collapse(plan)
    }
}

fn collapse(plan: &LogicalPlan) -> Result<Option<LogicalPlan>, OptimizeError> {
    match plan {
        LogicalPlan::Filter { input, predicate } => {
            let new_input = collapse(input)?;
            let new_pred = try_collapse_or_list(predicate);

            if new_input.is_none() && new_pred.is_none() {
                return Ok(None);
            }

            Ok(Some(LogicalPlan::Filter {
                input: Box::new(new_input.unwrap_or_else(|| *input.clone())),
                predicate: new_pred.unwrap_or_else(|| predicate.clone()),
            }))
        }

        LogicalPlan::Project {
            input,
            exprs,
            schema,
        } => {
            let new_input = collapse(input)?;
            Ok(new_input.map(|i| LogicalPlan::Project {
                input: Box::new(i),
                exprs: exprs.clone(),
                schema: schema.clone(),
            }))
        }

        LogicalPlan::Limit { input, n, offset } => {
            let new_input = collapse(input)?;
            Ok(new_input.map(|i| LogicalPlan::Limit {
                input: Box::new(i),
                n: *n,
                offset: *offset,
            }))
        }

        LogicalPlan::Sort { input, keys } => {
            let new_input = collapse(input)?;
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
            let new_left = collapse(left)?;
            let new_right = collapse(right)?;
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
            let new_input = collapse(input)?;
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
// OR-list collapse
// ---------------------------------------------------------------------------

/// Try to collapse a top-level OR-tree into a canonical list form.
///
/// Returns `Some(rebuilt_or_tree)` when the predicate is an OR of ≥
/// `MIN_DISJUNCTS` equality comparisons all on the same column against
/// distinct literals, or `None` if the pattern does not match.
fn try_collapse_or_list(expr: &ScalarExpr) -> Option<ScalarExpr> {
    let disjuncts = split_or(expr);
    if disjuncts.len() < MIN_DISJUNCTS {
        return None;
    }

    // All disjuncts must be `Eq(Column { index }, Literal)` on the same index.
    let mut col_index: Option<usize> = None;

    for d in &disjuncts {
        let ScalarExpr::Binary {
            op: BinaryOp::Eq,
            left,
            right,
            ..
        } = d
        else {
            return None;
        };

        let col_idx = match (left.as_ref(), right.as_ref()) {
            (ScalarExpr::Column { index, .. }, ScalarExpr::Literal { .. })
            | (ScalarExpr::Literal { .. }, ScalarExpr::Column { index, .. }) => *index,
            _ => return None,
        };

        match col_index {
            None => col_index = Some(col_idx),
            Some(i) if i != col_idx => return None,
            _ => {}
        }
    }

    // At this point we have ≥ MIN_DISJUNCTS equality checks all on the same
    // column. Since ScalarExpr has no InList variant yet, we rebuild the
    // canonical OR-tree. The structure is unchanged but the pattern is
    // recognised.
    // TODO(future RFC): lower to ScalarExpr::InList when that variant lands.
    let rebuilt = rebuild_or_tree(disjuncts);
    if &rebuilt == expr {
        None
    } else {
        Some(rebuilt)
    }
}

/// Split a top-level OR tree into individual disjuncts.
fn split_or(expr: &ScalarExpr) -> Vec<ScalarExpr> {
    match expr {
        ScalarExpr::Binary {
            op: BinaryOp::Or,
            left,
            right,
            ..
        } => {
            let mut v = split_or(left);
            v.extend(split_or(right));
            v
        }
        other => vec![other.clone()],
    }
}

/// Fold a slice of disjuncts back into a left-deep OR tree.
fn rebuild_or_tree(disjuncts: Vec<ScalarExpr>) -> ScalarExpr {
    assert!(!disjuncts.is_empty());
    let mut it = disjuncts.into_iter();
    let mut result = it.next().expect("non-empty");
    for d in it {
        result = ScalarExpr::Binary {
            op: BinaryOp::Or,
            left: Box::new(result),
            right: Box::new(d),
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
    use ultrasql_planner::{BinaryOp, LogicalPlan, ScalarExpr};

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

    fn or_expr(l: ScalarExpr, r: ScalarExpr) -> ScalarExpr {
        ScalarExpr::Binary {
            op: BinaryOp::Or,
            left: Box::new(l),
            right: Box::new(r),
            data_type: DataType::Bool,
        }
    }

    fn one_col_schema() -> Schema {
        Schema::new([Field::required("id", DataType::Int32)]).expect("schema ok")
    }

    // --- Current behavior: pattern is recognised but not rewritten ---

    #[test]
    fn three_way_or_equality_stays_no_op_without_inlist_expr() {
        let pred = or_expr(
            or_expr(eq(col("id", 0), lit_i32(1)), eq(col("id", 0), lit_i32(2))),
            eq(col("id", 0), lit_i32(3)),
        );
        let plan = LogicalPlan::Filter {
            input: Box::new(LogicalPlan::Scan {
                table: "t".into(),
                schema: one_col_schema(),
                projection: None,
            }),
            predicate: pred,
        };
        let rule = InListToSemi;
        let result = rule.apply(&plan).expect("no error");
        assert!(
            result.is_none(),
            "without ScalarExpr::InList, rebuilding the same OR tree must be a no-op"
        );
    }

    // --- Edge-case: only 2 disjuncts — below threshold ---

    #[test]
    fn does_not_collapse_two_way_or() {
        let pred = or_expr(eq(col("id", 0), lit_i32(1)), eq(col("id", 0), lit_i32(2)));
        let plan = LogicalPlan::Filter {
            input: Box::new(LogicalPlan::Scan {
                table: "t".into(),
                schema: one_col_schema(),
                projection: None,
            }),
            predicate: pred,
        };
        let rule = InListToSemi;
        let result = rule.apply(&plan).expect("no error");
        assert!(result.is_none(), "2-way OR should not be collapsed");
    }

    // --- Edge-case: mixed columns in OR ---

    #[test]
    fn does_not_collapse_mixed_column_or() {
        let schema = Schema::new([
            Field::required("a", DataType::Int32),
            Field::required("b", DataType::Int32),
        ])
        .expect("schema ok");
        let pred = or_expr(
            or_expr(eq(col("a", 0), lit_i32(1)), eq(col("b", 1), lit_i32(2))),
            eq(col("a", 0), lit_i32(3)),
        );
        let plan = LogicalPlan::Filter {
            input: Box::new(LogicalPlan::Scan {
                table: "t".into(),
                schema,
                projection: None,
            }),
            predicate: pred,
        };
        let rule = InListToSemi;
        let result = rule.apply(&plan).expect("no error");
        assert!(result.is_none(), "mixed-column OR should not be collapsed");
    }

    // --- No-op: scan alone ---

    #[test]
    fn no_op_on_scan() {
        let plan = LogicalPlan::Scan {
            table: "t".into(),
            schema: one_col_schema(),
            projection: None,
        };
        let rule = InListToSemi;
        assert!(rule.apply(&plan).expect("no error").is_none());
    }
}
