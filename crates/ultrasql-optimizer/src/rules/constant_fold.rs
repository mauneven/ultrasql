//! Constant-folding rewrite rule.
//!
//! [`ConstantFold`] walks every [`ScalarExpr`] reachable from the input plan
//! and evaluates sub-expressions whose operands are all literals. The rule
//! covers:
//!
//! - Arithmetic: `Literal op Literal` for `Add`, `Sub`, `Mul`, `Div`, `Mod`
//!   on `Int32`, `Int64`, `Float32`, `Float64`.
//! - Boolean short-circuit: `And`/`Or` with a literal operand.
//! - Comparison: `Literal cmp Literal` for the six comparison operators.
//! - Unary `Not` on boolean literals.
//! - `IsNull` on a literal null or literal non-null value.
//!
//! The rule returns `None` when no sub-expression changed, allowing the driver
//! to detect a fixed point without re-hashing the plan tree.

use ultrasql_core::{DataType, Value};
use ultrasql_planner::{
    BinaryOp, LogicalMergeAction, LogicalMergeClause, LogicalPlan, ScalarExpr, SortKey, UnaryOp,
};

use crate::error::OptimizeError;
use crate::rules::RewriteRule;

/// Folds constant sub-expressions to literals.
///
/// The rule is always safe to apply: it never changes the logical semantics of
/// the plan, only reduces work. It is registered first in the default rule set
/// because later rules (predicate pushdown, outer-join elimination) may unlock
/// additional constant folds.
#[derive(Debug)]
pub struct ConstantFold;

impl RewriteRule for ConstantFold {
    fn name(&self) -> &'static str {
        "constant_fold"
    }

    fn apply(&self, plan: &LogicalPlan) -> Result<Option<LogicalPlan>, OptimizeError> {
        fold_plan(plan)
    }
}

// ---------------------------------------------------------------------------
// Plan-level recursion helpers
// ---------------------------------------------------------------------------

/// Fold expressions in a `(ScalarExpr, String)` pair list. Returns the new
/// list and a `changed` flag. Both outputs are computed in one pass to avoid
/// double iteration.
fn fold_expr_pairs(pairs: &[(ScalarExpr, String)]) -> (Vec<(ScalarExpr, String)>, bool) {
    let mut changed = false;
    let new_pairs = pairs
        .iter()
        .map(|(e, n)| {
            fold_expr(e).map_or_else(
                || (e.clone(), n.clone()),
                |fe| {
                    changed = true;
                    (fe, n.clone())
                },
            )
        })
        .collect();
    (new_pairs, changed)
}

/// Fold expressions in a `(usize, ScalarExpr)` pair list (for assignments).
fn fold_idx_expr_pairs(pairs: &[(usize, ScalarExpr)]) -> (Vec<(usize, ScalarExpr)>, bool) {
    let mut changed = false;
    let new_pairs = pairs
        .iter()
        .map(|(idx, e)| {
            fold_expr(e).map_or_else(
                || (*idx, e.clone()),
                |fe| {
                    changed = true;
                    (*idx, fe)
                },
            )
        })
        .collect();
    (new_pairs, changed)
}

/// Fold a list of standalone `ScalarExpr`s. Returns the new list and a
/// `changed` flag.
fn fold_expr_list(exprs: &[ScalarExpr]) -> (Vec<ScalarExpr>, bool) {
    let mut changed = false;
    let new_exprs = exprs
        .iter()
        .map(|e| {
            fold_expr(e).map_or_else(
                || e.clone(),
                |fe| {
                    changed = true;
                    fe
                },
            )
        })
        .collect();
    (new_exprs, changed)
}

fn fold_sort_keys(keys: &[SortKey]) -> (Vec<SortKey>, bool) {
    let mut changed = false;
    let new_keys = keys
        .iter()
        .map(|k| {
            fold_expr(&k.expr).map_or_else(
                || k.clone(),
                |fe| {
                    changed = true;
                    SortKey {
                        expr: fe,
                        asc: k.asc,
                        nulls_first: k.nulls_first,
                    }
                },
            )
        })
        .collect();
    (new_keys, changed)
}

fn fold_join_condition(
    cond: &ultrasql_planner::LogicalJoinCondition,
) -> Option<ultrasql_planner::LogicalJoinCondition> {
    use ultrasql_planner::LogicalJoinCondition;
    if let LogicalJoinCondition::On(pred) = cond {
        fold_expr(pred).map(LogicalJoinCondition::On)
    } else {
        None
    }
}

fn fold_window_func(
    func: &ultrasql_planner::LogicalWindowFunc,
) -> (ultrasql_planner::LogicalWindowFunc, bool) {
    use ultrasql_planner::LogicalWindowFunc;

    match func {
        LogicalWindowFunc::Lag {
            expr,
            offset,
            default,
        } => fold_expr(expr).map_or_else(
            || (func.clone(), false),
            |expr| {
                (
                    LogicalWindowFunc::Lag {
                        expr,
                        offset: *offset,
                        default: default.clone(),
                    },
                    true,
                )
            },
        ),
        LogicalWindowFunc::Lead {
            expr,
            offset,
            default,
        } => fold_expr(expr).map_or_else(
            || (func.clone(), false),
            |expr| {
                (
                    LogicalWindowFunc::Lead {
                        expr,
                        offset: *offset,
                        default: default.clone(),
                    },
                    true,
                )
            },
        ),
        LogicalWindowFunc::FirstValue(expr) => fold_expr(expr).map_or_else(
            || (func.clone(), false),
            |expr| (LogicalWindowFunc::FirstValue(expr), true),
        ),
        LogicalWindowFunc::LastValue(expr) => fold_expr(expr).map_or_else(
            || (func.clone(), false),
            |expr| (LogicalWindowFunc::LastValue(expr), true),
        ),
        LogicalWindowFunc::NthValue { expr, n } => fold_expr(expr).map_or_else(
            || (func.clone(), false),
            |expr| (LogicalWindowFunc::NthValue { expr, n: *n }, true),
        ),
        LogicalWindowFunc::RowNumber
        | LogicalWindowFunc::Rank
        | LogicalWindowFunc::DenseRank
        | LogicalWindowFunc::Ntile(_) => (func.clone(), false),
    }
}

// ---------------------------------------------------------------------------
// Plan-level fold dispatch
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_lines)]
fn fold_plan(plan: &LogicalPlan) -> Result<Option<LogicalPlan>, OptimizeError> {
    match plan {
        LogicalPlan::Filter { input, predicate } => {
            let new_input = fold_plan(input)?;
            let new_pred = fold_expr(predicate);
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
            let new_input = fold_plan(input)?;
            let (new_exprs, exprs_changed) = fold_expr_pairs(exprs);
            if new_input.is_none() && !exprs_changed {
                return Ok(None);
            }
            Ok(Some(LogicalPlan::Project {
                input: Box::new(new_input.unwrap_or_else(|| *input.clone())),
                exprs: new_exprs,
                schema: schema.clone(),
            }))
        }

        LogicalPlan::Limit { input, n, offset } => {
            fold_plan(input)?.map(|i| LogicalPlan::Limit {
                input: Box::new(i),
                n: *n,
                offset: *offset,
            });
            Ok(fold_plan(input)?.map(|i| LogicalPlan::Limit {
                input: Box::new(i),
                n: *n,
                offset: *offset,
            }))
        }

        LogicalPlan::Sort { input, keys } => {
            let new_input = fold_plan(input)?;
            let (new_keys, keys_changed) = fold_sort_keys(keys);
            if new_input.is_none() && !keys_changed {
                return Ok(None);
            }
            Ok(Some(LogicalPlan::Sort {
                input: Box::new(new_input.unwrap_or_else(|| *input.clone())),
                keys: new_keys,
            }))
        }

        LogicalPlan::Window {
            input,
            partition_by,
            order_by,
            func,
            output_name,
            schema,
        } => {
            // Window: fold the partition-by exprs, the order-by exprs,
            // and the function-internal exprs. The output column shape
            // is fixed by the binder so the schema never changes.
            let new_input = fold_plan(input)?;
            let (new_partition, partition_changed) = fold_expr_list(partition_by);
            let (new_order_by, order_changed) = fold_sort_keys(order_by);
            let (new_func, func_changed) = fold_window_func(func);
            if new_input.is_none() && !partition_changed && !order_changed && !func_changed {
                return Ok(None);
            }
            Ok(Some(LogicalPlan::Window {
                input: Box::new(new_input.unwrap_or_else(|| *input.clone())),
                partition_by: new_partition,
                order_by: new_order_by,
                func: new_func,
                output_name: output_name.clone(),
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
            let new_left = fold_plan(left)?;
            let new_right = fold_plan(right)?;
            let new_cond = fold_join_condition(condition);
            if new_left.is_none() && new_right.is_none() && new_cond.is_none() {
                return Ok(None);
            }
            Ok(Some(LogicalPlan::Join {
                left: Box::new(new_left.unwrap_or_else(|| *left.clone())),
                right: Box::new(new_right.unwrap_or_else(|| *right.clone())),
                join_type: *join_type,
                condition: new_cond.unwrap_or_else(|| condition.clone()),
                schema: schema.clone(),
            }))
        }

        LogicalPlan::Aggregate {
            input,
            group_by,
            aggregates,
            schema,
        } => {
            let new_input = fold_plan(input)?;
            let (new_group, group_changed) = fold_expr_list(group_by);
            let mut aggs_changed = false;
            let new_aggs = aggregates
                .iter()
                .map(|agg| {
                    let new_arg = agg.arg.as_ref().and_then(fold_expr);
                    let new_direct_arg = agg.direct_arg.as_ref().and_then(fold_expr);
                    let new_order_by_expr = agg.order_by.as_ref().and_then(|key| {
                        fold_expr(&key.expr).map(|expr| SortKey {
                            expr,
                            asc: key.asc,
                            nulls_first: key.nulls_first,
                        })
                    });
                    if new_arg.is_some() || new_direct_arg.is_some() || new_order_by_expr.is_some()
                    {
                        aggs_changed = true;
                        ultrasql_planner::LogicalAggregateExpr {
                            func: agg.func,
                            arg: new_arg.or_else(|| agg.arg.clone()),
                            direct_arg: new_direct_arg.or_else(|| agg.direct_arg.clone()),
                            order_by: new_order_by_expr.or_else(|| agg.order_by.clone()),
                            distinct: agg.distinct,
                            output_name: agg.output_name.clone(),
                            data_type: agg.data_type.clone(),
                        }
                    } else {
                        agg.clone()
                    }
                })
                .collect();
            if new_input.is_none() && !group_changed && !aggs_changed {
                return Ok(None);
            }
            Ok(Some(LogicalPlan::Aggregate {
                input: Box::new(new_input.unwrap_or_else(|| *input.clone())),
                group_by: new_group,
                aggregates: new_aggs,
                schema: schema.clone(),
            }))
        }

        LogicalPlan::Pivot {
            input,
            group_columns,
            pivot_column,
            aggregate,
            pivot_values,
            schema,
        } => {
            let new_input = fold_plan(input)?;
            let new_arg = aggregate.arg.as_ref().and_then(fold_expr);
            if new_input.is_none() && new_arg.is_none() {
                return Ok(None);
            }
            let mut aggregate = aggregate.clone();
            if let Some(arg) = new_arg {
                aggregate.arg = Some(arg);
            }
            Ok(Some(LogicalPlan::Pivot {
                input: Box::new(new_input.unwrap_or_else(|| *input.clone())),
                group_columns: group_columns.clone(),
                pivot_column: *pivot_column,
                aggregate,
                pivot_values: pivot_values.clone(),
                schema: schema.clone(),
            }))
        }

        LogicalPlan::Unpivot {
            input,
            passthrough_columns,
            columns,
            name_column,
            value_column,
            include_nulls,
            schema,
        } => fold_plan(input)?
            .map(|new_input| LogicalPlan::Unpivot {
                input: Box::new(new_input),
                passthrough_columns: passthrough_columns.clone(),
                columns: columns.clone(),
                name_column: name_column.clone(),
                value_column: value_column.clone(),
                include_nulls: *include_nulls,
                schema: schema.clone(),
            })
            .map_or(Ok(None), |plan| Ok(Some(plan))),

        LogicalPlan::Insert {
            table,
            columns,
            source,
            on_conflict,
            returning,
            schema,
        } => {
            let new_source = fold_plan(source)?;
            let (new_ret, ret_changed) = fold_expr_pairs(returning);
            if new_source.is_none() && !ret_changed {
                return Ok(None);
            }
            Ok(Some(LogicalPlan::Insert {
                table: table.clone(),
                columns: columns.clone(),
                source: Box::new(new_source.unwrap_or_else(|| *source.clone())),
                on_conflict: on_conflict.clone(),
                returning: new_ret,
                schema: schema.clone(),
            }))
        }

        LogicalPlan::Update {
            table,
            assignments,
            input,
            returning,
            schema,
        } => {
            let new_input = fold_plan(input)?;
            let (new_assignments, assign_changed) = fold_idx_expr_pairs(assignments);
            let (new_ret, ret_changed) = fold_expr_pairs(returning);
            if new_input.is_none() && !assign_changed && !ret_changed {
                return Ok(None);
            }
            Ok(Some(LogicalPlan::Update {
                table: table.clone(),
                assignments: new_assignments,
                input: Box::new(new_input.unwrap_or_else(|| *input.clone())),
                returning: new_ret,
                schema: schema.clone(),
            }))
        }

        LogicalPlan::Delete {
            table,
            input,
            returning,
            schema,
        } => {
            let new_input = fold_plan(input)?;
            let (new_ret, ret_changed) = fold_expr_pairs(returning);
            if new_input.is_none() && !ret_changed {
                return Ok(None);
            }
            Ok(Some(LogicalPlan::Delete {
                table: table.clone(),
                input: Box::new(new_input.unwrap_or_else(|| *input.clone())),
                returning: new_ret,
                schema: schema.clone(),
            }))
        }

        LogicalPlan::Merge {
            target,
            target_alias,
            target_schema,
            source,
            on,
            clauses,
            schema,
        } => {
            let new_source = fold_plan(source)?;
            let new_on = fold_expr(on);
            let mut clauses_changed = false;
            let new_clauses = clauses
                .iter()
                .map(|clause| {
                    let new_condition = clause.condition.as_ref().and_then(fold_expr);
                    if new_condition.is_some() {
                        clauses_changed = true;
                    }
                    let action = match &clause.action {
                        LogicalMergeAction::Update { assignments } => {
                            let (assignments, changed) = fold_idx_expr_pairs(assignments);
                            clauses_changed |= changed;
                            LogicalMergeAction::Update { assignments }
                        }
                        LogicalMergeAction::Delete => LogicalMergeAction::Delete,
                        LogicalMergeAction::Insert { columns, values } => {
                            let (values, changed) = fold_expr_list(values);
                            clauses_changed |= changed;
                            LogicalMergeAction::Insert {
                                columns: columns.clone(),
                                values,
                            }
                        }
                    };
                    LogicalMergeClause {
                        kind: clause.kind,
                        condition: new_condition.or_else(|| clause.condition.clone()),
                        action,
                    }
                })
                .collect();
            if new_source.is_none() && new_on.is_none() && !clauses_changed {
                return Ok(None);
            }
            Ok(Some(LogicalPlan::Merge {
                target: target.clone(),
                target_alias: target_alias.clone(),
                target_schema: target_schema.clone(),
                source: Box::new(new_source.unwrap_or_else(|| *source.clone())),
                on: new_on.unwrap_or_else(|| on.clone()),
                clauses: new_clauses,
                schema: schema.clone(),
            }))
        }

        LogicalPlan::SetOp {
            op,
            quantifier,
            left,
            right,
            schema,
        } => {
            let new_left = fold_plan(left)?;
            let new_right = fold_plan(right)?;
            if new_left.is_none() && new_right.is_none() {
                return Ok(None);
            }
            Ok(Some(LogicalPlan::SetOp {
                op: *op,
                quantifier: *quantifier,
                left: Box::new(new_left.unwrap_or_else(|| *left.clone())),
                right: Box::new(new_right.unwrap_or_else(|| *right.clone())),
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
            let new_def = fold_plan(definition)?;
            let new_body = fold_plan(body)?;
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

        LogicalPlan::LockRows {
            input,
            strength,
            wait_policy,
            schema,
        } => fold_plan(input)?
            .map(|i| {
                Ok(Some(LogicalPlan::LockRows {
                    input: Box::new(i),
                    strength: *strength,
                    wait_policy: *wait_policy,
                    schema: schema.clone(),
                }))
            })
            .unwrap_or(Ok(None)),

        // Leaf nodes with no reachable expressions to fold.
        LogicalPlan::Scan { .. }
        | LogicalPlan::Empty { .. }
        | LogicalPlan::Values { .. }
        | LogicalPlan::Truncate { .. }
        | LogicalPlan::CreateTable { .. }
        | LogicalPlan::CreateMaterializedView { .. }
        | LogicalPlan::CreateView { .. }
        | LogicalPlan::CreateTypeEnum { .. }
        | LogicalPlan::CreateTypeComposite { .. }
        | LogicalPlan::CreateDomain { .. }
        | LogicalPlan::CreateOperator { .. }
        | LogicalPlan::CreateIndex { .. }
        | LogicalPlan::CreatePolicy { .. }
        | LogicalPlan::CreateRole { .. }
        | LogicalPlan::AlterRole { .. }
        | LogicalPlan::DropRole { .. }
        | LogicalPlan::GrantPrivileges { .. }
        | LogicalPlan::RevokePrivileges { .. }
        | LogicalPlan::AlterDefaultPrivileges { .. }
        | LogicalPlan::GrantRole { .. }
        | LogicalPlan::RevokeRole { .. }
        | LogicalPlan::DropTable { .. }
        | LogicalPlan::AlterTable { .. }
        | LogicalPlan::AlterView { .. }
        | LogicalPlan::CreateSequence { .. }
        | LogicalPlan::AlterSequence { .. }
        | LogicalPlan::DropSequence { .. }
        | LogicalPlan::CreateSchema { .. }
        | LogicalPlan::DropSchema { .. }
        | LogicalPlan::DropIndex { .. }
        | LogicalPlan::Comment { .. }
        | LogicalPlan::Begin { .. }
        | LogicalPlan::Commit { .. }
        | LogicalPlan::Rollback { .. }
        | LogicalPlan::Savepoint { .. }
        | LogicalPlan::RollbackToSavepoint { .. }
        | LogicalPlan::ReleaseSavepoint { .. }
        | LogicalPlan::PrepareTransaction { .. }
        | LogicalPlan::CommitPrepared { .. }
        | LogicalPlan::RollbackPrepared { .. }
        | LogicalPlan::SetTransaction { .. }
        | LogicalPlan::SetVariable { .. }
        | LogicalPlan::Describe { .. }
        | LogicalPlan::Summarize { .. }
        | LogicalPlan::Checkpoint { .. }
        | LogicalPlan::ExportDatabase { .. }
        | LogicalPlan::ImportDatabase { .. }
        | LogicalPlan::SetRole { .. }
        | LogicalPlan::Listen { .. }
        | LogicalPlan::Notify { .. }
        | LogicalPlan::Unlisten { .. }
        | LogicalPlan::Explain { .. }
        | LogicalPlan::Copy { .. }
        | LogicalPlan::FunctionScan { .. } => Ok(None),
    }
}

// ---------------------------------------------------------------------------
// Expression-level folding
// ---------------------------------------------------------------------------

/// Attempt to fold `expr` into a simpler expression.
///
/// Returns `Some(new_expr)` when any sub-expression changed; `None` when the
/// expression is already in its simplest form.
pub(crate) fn fold_expr(expr: &ScalarExpr) -> Option<ScalarExpr> {
    match expr {
        ScalarExpr::Binary {
            op,
            left,
            right,
            data_type,
        } => fold_binary(*op, left, right, data_type),

        ScalarExpr::Unary {
            op,
            expr: inner,
            data_type,
        } => fold_unary(*op, inner, data_type),

        ScalarExpr::IsNull {
            expr: inner,
            negated,
        } => fold_is_null(inner, *negated),

        _ => None,
    }
}

fn fold_binary(
    op: BinaryOp,
    left: &ScalarExpr,
    right: &ScalarExpr,
    result_type: &DataType,
) -> Option<ScalarExpr> {
    let left_folded_opt = fold_expr(left);
    let right_folded_opt = fold_expr(right);
    let left_changed = left_folded_opt.is_some();
    let right_changed = right_folded_opt.is_some();
    let children_changed = left_changed || right_changed;
    let left_folded = left_folded_opt.unwrap_or_else(|| left.clone());
    let right_folded = right_folded_opt.unwrap_or_else(|| right.clone());

    let lit_left = as_literal(&left_folded).cloned();
    let lit_right = as_literal(&right_folded).cloned();

    match op {
        BinaryOp::And => fold_and(
            left_folded,
            right_folded,
            lit_left.as_ref(),
            lit_right.as_ref(),
            result_type,
            children_changed,
        ),
        BinaryOp::Or => fold_or(
            left_folded,
            right_folded,
            lit_left.as_ref(),
            lit_right.as_ref(),
            result_type,
            children_changed,
        ),
        BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div | BinaryOp::Mod => {
            match (lit_left.as_ref(), lit_right.as_ref()) {
                (Some(lv), Some(rv)) => eval_arith(op, lv, rv, result_type).or_else(|| {
                    rebuild_binary_if_changed(
                        op,
                        left_folded,
                        right_folded,
                        result_type,
                        children_changed,
                    )
                }),
                _ => rebuild_binary_if_changed(
                    op,
                    left_folded,
                    right_folded,
                    result_type,
                    children_changed,
                ),
            }
        }
        BinaryOp::Eq
        | BinaryOp::NotEq
        | BinaryOp::Lt
        | BinaryOp::LtEq
        | BinaryOp::Gt
        | BinaryOp::GtEq => match (lit_left.as_ref(), lit_right.as_ref()) {
            (Some(lv), Some(rv)) => eval_cmp(op, lv, rv).or_else(|| {
                rebuild_binary_if_changed(
                    op,
                    left_folded,
                    right_folded,
                    result_type,
                    children_changed,
                )
            }),
            _ => rebuild_binary_if_changed(
                op,
                left_folded,
                right_folded,
                result_type,
                children_changed,
            ),
        },
        _ => {
            rebuild_binary_if_changed(op, left_folded, right_folded, result_type, children_changed)
        }
    }
}

fn fold_and(
    left: ScalarExpr,
    right: ScalarExpr,
    lit_left: Option<&Value>,
    lit_right: Option<&Value>,
    result_type: &DataType,
    children_changed: bool,
) -> Option<ScalarExpr> {
    match (lit_left, lit_right) {
        (Some(Value::Bool(false)), _) | (_, Some(Value::Bool(false))) => Some(bool_literal(false)),
        (Some(Value::Bool(true)), _) => Some(right),
        (_, Some(Value::Bool(true))) => Some(left),
        _ => rebuild_binary_if_changed(BinaryOp::And, left, right, result_type, children_changed),
    }
}

fn fold_or(
    left: ScalarExpr,
    right: ScalarExpr,
    lit_left: Option<&Value>,
    lit_right: Option<&Value>,
    result_type: &DataType,
    children_changed: bool,
) -> Option<ScalarExpr> {
    match (lit_left, lit_right) {
        (Some(Value::Bool(true)), _) | (_, Some(Value::Bool(true))) => Some(bool_literal(true)),
        (Some(Value::Bool(false)), _) => Some(right),
        (_, Some(Value::Bool(false))) => Some(left),
        _ => rebuild_binary_if_changed(BinaryOp::Or, left, right, result_type, children_changed),
    }
}

fn rebuild_binary_if_changed(
    op: BinaryOp,
    left: ScalarExpr,
    right: ScalarExpr,
    result_type: &DataType,
    changed: bool,
) -> Option<ScalarExpr> {
    if changed {
        Some(ScalarExpr::Binary {
            op,
            left: Box::new(left),
            right: Box::new(right),
            data_type: result_type.clone(),
        })
    } else {
        None
    }
}

fn fold_unary(op: UnaryOp, inner: &ScalarExpr, result_type: &DataType) -> Option<ScalarExpr> {
    let inner_folded_opt = fold_expr(inner);
    let inner_changed = inner_folded_opt.is_some();
    let inner_folded = inner_folded_opt.unwrap_or_else(|| inner.clone());
    if op == UnaryOp::Not {
        if let Some(Value::Bool(b)) = as_literal(&inner_folded) {
            return Some(bool_literal(!b));
        }
    }
    if inner_changed {
        Some(ScalarExpr::Unary {
            op,
            expr: Box::new(inner_folded),
            data_type: result_type.clone(),
        })
    } else {
        None
    }
}

fn fold_is_null(inner: &ScalarExpr, negated: bool) -> Option<ScalarExpr> {
    let inner_folded_opt = fold_expr(inner);
    let inner_changed = inner_folded_opt.is_some();
    let inner_folded = inner_folded_opt.unwrap_or_else(|| inner.clone());
    if let Some(v) = as_literal(&inner_folded) {
        let result = if v.is_null() { !negated } else { negated };
        return Some(bool_literal(result));
    }
    inner_changed.then(|| ScalarExpr::IsNull {
        expr: Box::new(inner_folded),
        negated,
    })
}

// ---------------------------------------------------------------------------
// Arithmetic evaluation helpers
// ---------------------------------------------------------------------------

fn eval_arith(op: BinaryOp, lv: &Value, rv: &Value, result_type: &DataType) -> Option<ScalarExpr> {
    let result: Option<Value> = match (lv, rv) {
        (Value::Int32(a), Value::Int32(b)) => eval_arith_i32(op, *a, *b),
        (Value::Int64(a), Value::Int64(b)) => eval_arith_i64(op, *a, *b),
        (Value::Float32(a), Value::Float32(b)) => eval_arith_f32(op, *a, *b),
        (Value::Float64(a), Value::Float64(b)) => eval_arith_f64(op, *a, *b),
        (a, b) => match (a.as_i64(), b.as_i64()) {
            (Some(a), Some(b)) => eval_arith_i64(op, a, b),
            _ => match (a.as_f64(), b.as_f64()) {
                (Some(a), Some(b)) => eval_arith_f64(op, a, b),
                _ => None,
            },
        },
    };
    result.map(|v| ScalarExpr::Literal {
        value: v,
        data_type: result_type.clone(),
    })
}

fn eval_arith_i32(op: BinaryOp, a: i32, b: i32) -> Option<Value> {
    match op {
        BinaryOp::Add => a.checked_add(b).map(Value::Int32),
        BinaryOp::Sub => a.checked_sub(b).map(Value::Int32),
        BinaryOp::Mul => a.checked_mul(b).map(Value::Int32),
        BinaryOp::Div => {
            if b == 0 {
                None
            } else {
                a.checked_div(b).map(Value::Int32)
            }
        }
        BinaryOp::Mod => {
            if b == 0 {
                None
            } else {
                a.checked_rem(b).map(Value::Int32)
            }
        }
        _ => None,
    }
}

fn eval_arith_i64(op: BinaryOp, a: i64, b: i64) -> Option<Value> {
    match op {
        BinaryOp::Add => a.checked_add(b).map(Value::Int64),
        BinaryOp::Sub => a.checked_sub(b).map(Value::Int64),
        BinaryOp::Mul => a.checked_mul(b).map(Value::Int64),
        BinaryOp::Div => {
            if b == 0 {
                None
            } else {
                a.checked_div(b).map(Value::Int64)
            }
        }
        BinaryOp::Mod => {
            if b == 0 {
                None
            } else {
                a.checked_rem(b).map(Value::Int64)
            }
        }
        _ => None,
    }
}

fn eval_arith_f32(op: BinaryOp, a: f32, b: f32) -> Option<Value> {
    match op {
        BinaryOp::Add => Some(Value::Float32(a + b)),
        BinaryOp::Sub => Some(Value::Float32(a - b)),
        BinaryOp::Mul => Some(Value::Float32(a * b)),
        BinaryOp::Div => Some(Value::Float32(a / b)),
        BinaryOp::Mod => Some(Value::Float32(a % b)),
        _ => None,
    }
}

fn eval_arith_f64(op: BinaryOp, a: f64, b: f64) -> Option<Value> {
    match op {
        BinaryOp::Add => Some(Value::Float64(a + b)),
        BinaryOp::Sub => Some(Value::Float64(a - b)),
        BinaryOp::Mul => Some(Value::Float64(a * b)),
        BinaryOp::Div => Some(Value::Float64(a / b)),
        BinaryOp::Mod => Some(Value::Float64(a % b)),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Comparison evaluation
// ---------------------------------------------------------------------------

fn eval_cmp(op: BinaryOp, lv: &Value, rv: &Value) -> Option<ScalarExpr> {
    use std::cmp::Ordering;
    let ord = cmp_values(lv, rv)?;
    let result = match op {
        BinaryOp::Eq => ord == Ordering::Equal,
        BinaryOp::NotEq => ord != Ordering::Equal,
        BinaryOp::Lt => ord == Ordering::Less,
        BinaryOp::LtEq => ord != Ordering::Greater,
        BinaryOp::Gt => ord == Ordering::Greater,
        BinaryOp::GtEq => ord != Ordering::Less,
        _ => return None,
    };
    Some(bool_literal(result))
}

fn cmp_values(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (Value::Int32(x), Value::Int32(y)) => x.partial_cmp(y),
        (Value::Int64(x), Value::Int64(y)) => x.partial_cmp(y),
        (Value::Float32(x), Value::Float32(y)) => x.partial_cmp(y),
        (Value::Float64(x), Value::Float64(y)) => x.partial_cmp(y),
        (a, b) => match (a.as_i64(), b.as_i64()) {
            (Some(a), Some(b)) => a.partial_cmp(&b),
            _ => match (a.as_f64(), b.as_f64()) {
                (Some(a), Some(b)) => a.partial_cmp(&b),
                _ => None,
            },
        },
    }
}

// ---------------------------------------------------------------------------
// Small utilities
// ---------------------------------------------------------------------------

const fn as_literal(expr: &ScalarExpr) -> Option<&Value> {
    match expr {
        ScalarExpr::Literal { value, .. } => Some(value),
        _ => None,
    }
}

const fn bool_literal(b: bool) -> ScalarExpr {
    ScalarExpr::Literal {
        value: Value::Bool(b),
        data_type: DataType::Bool,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use ultrasql_core::{DataType, Value};
    use ultrasql_planner::{BinaryOp, LogicalWindowFunc, ScalarExpr, SortKey, UnaryOp};

    use super::*;
    use crate::rules::RewriteRule;

    fn lit_i32(v: i32) -> ScalarExpr {
        ScalarExpr::Literal {
            value: Value::Int32(v),
            data_type: DataType::Int32,
        }
    }

    fn lit_i64(v: i64) -> ScalarExpr {
        ScalarExpr::Literal {
            value: Value::Int64(v),
            data_type: DataType::Int64,
        }
    }

    fn lit_f64(v: f64) -> ScalarExpr {
        ScalarExpr::Literal {
            value: Value::Float64(v),
            data_type: DataType::Float64,
        }
    }

    fn lit_bool(b: bool) -> ScalarExpr {
        ScalarExpr::Literal {
            value: Value::Bool(b),
            data_type: DataType::Bool,
        }
    }

    fn lit_null() -> ScalarExpr {
        ScalarExpr::Literal {
            value: Value::Null,
            data_type: DataType::Null,
        }
    }

    fn col_expr(name: &str, idx: usize) -> ScalarExpr {
        ScalarExpr::Column {
            name: name.to_owned(),
            index: idx,
            data_type: DataType::Int32,
        }
    }

    fn bin(op: BinaryOp, l: ScalarExpr, r: ScalarExpr, dt: DataType) -> ScalarExpr {
        ScalarExpr::Binary {
            op,
            left: Box::new(l),
            right: Box::new(r),
            data_type: dt,
        }
    }

    fn add_i32(l: ScalarExpr, r: ScalarExpr) -> ScalarExpr {
        bin(BinaryOp::Add, l, r, DataType::Int32)
    }

    fn and_expr(l: ScalarExpr, r: ScalarExpr) -> ScalarExpr {
        bin(BinaryOp::And, l, r, DataType::Bool)
    }

    fn or_expr(l: ScalarExpr, r: ScalarExpr) -> ScalarExpr {
        bin(BinaryOp::Or, l, r, DataType::Bool)
    }

    fn eq_expr(l: ScalarExpr, r: ScalarExpr) -> ScalarExpr {
        bin(BinaryOp::Eq, l, r, DataType::Bool)
    }

    // --- Happy-path tests ---

    #[test]
    fn folds_i32_add() {
        let e = add_i32(lit_i32(3), lit_i32(4));
        let folded = fold_expr(&e).expect("should fold");
        assert_eq!(
            folded,
            ScalarExpr::Literal {
                value: Value::Int32(7),
                data_type: DataType::Int32,
            }
        );
    }

    #[test]
    fn folds_f64_mul() {
        let e = bin(BinaryOp::Mul, lit_f64(2.0), lit_f64(3.5), DataType::Float64);
        let folded = fold_expr(&e).expect("should fold");
        assert_eq!(
            folded,
            ScalarExpr::Literal {
                value: Value::Float64(7.0),
                data_type: DataType::Float64,
            }
        );
    }

    #[test]
    fn folds_and_with_true_left() {
        let e = and_expr(lit_bool(true), col_expr("x", 0));
        let folded = fold_expr(&e).expect("should fold");
        assert_eq!(folded, col_expr("x", 0));
    }

    #[test]
    fn folds_and_with_false_short_circuits() {
        let e = and_expr(lit_bool(false), col_expr("x", 0));
        let folded = fold_expr(&e).expect("should fold");
        assert_eq!(folded, lit_bool(false));
    }

    #[test]
    fn folds_or_with_true_short_circuits() {
        let e = or_expr(lit_bool(true), col_expr("x", 0));
        let folded = fold_expr(&e).expect("should fold");
        assert_eq!(folded, lit_bool(true));
    }

    #[test]
    fn folds_or_with_false_left() {
        let e = or_expr(lit_bool(false), col_expr("x", 0));
        let folded = fold_expr(&e).expect("should fold");
        assert_eq!(folded, col_expr("x", 0));
    }

    #[test]
    fn folds_eq_comparison() {
        let e = eq_expr(lit_i32(5), lit_i32(5));
        let folded = fold_expr(&e).expect("should fold");
        assert_eq!(folded, lit_bool(true));
    }

    #[test]
    fn folds_lt_comparison() {
        let e = bin(BinaryOp::Lt, lit_i32(3), lit_i32(7), DataType::Bool);
        let folded = fold_expr(&e).expect("should fold");
        assert_eq!(folded, lit_bool(true));
    }

    #[test]
    fn folds_not_true() {
        let e = ScalarExpr::Unary {
            op: UnaryOp::Not,
            expr: Box::new(lit_bool(true)),
            data_type: DataType::Bool,
        };
        let folded = fold_expr(&e).expect("should fold");
        assert_eq!(folded, lit_bool(false));
    }

    #[test]
    fn folds_not_false() {
        let e = ScalarExpr::Unary {
            op: UnaryOp::Not,
            expr: Box::new(lit_bool(false)),
            data_type: DataType::Bool,
        };
        let folded = fold_expr(&e).expect("should fold");
        assert_eq!(folded, lit_bool(true));
    }

    #[test]
    fn folds_is_null_on_null_literal() {
        let e = ScalarExpr::IsNull {
            expr: Box::new(lit_null()),
            negated: false,
        };
        let folded = fold_expr(&e).expect("should fold");
        assert_eq!(folded, lit_bool(true));
    }

    #[test]
    fn folds_is_not_null_on_null_literal() {
        let e = ScalarExpr::IsNull {
            expr: Box::new(lit_null()),
            negated: true,
        };
        let folded = fold_expr(&e).expect("should fold");
        assert_eq!(folded, lit_bool(false));
    }

    #[test]
    fn folds_is_null_on_non_null_literal() {
        let e = ScalarExpr::IsNull {
            expr: Box::new(lit_i32(42)),
            negated: false,
        };
        let folded = fold_expr(&e).expect("should fold");
        assert_eq!(folded, lit_bool(false));
    }

    // --- Edge-case tests ---

    #[test]
    fn division_by_zero_is_not_folded_to_literal() {
        let e = bin(BinaryOp::Div, lit_i32(10), lit_i32(0), DataType::Int32);
        // Should not produce a Literal (division by zero is unrepresentable).
        if let Some(result) = fold_expr(&e) {
            assert!(!matches!(result, ScalarExpr::Literal { .. }));
        }
    }

    #[test]
    fn i64_arithmetic_folds_correctly() {
        let e = bin(BinaryOp::Mul, lit_i64(100), lit_i64(200), DataType::Int64);
        let folded = fold_expr(&e).expect("should fold");
        assert_eq!(
            folded,
            ScalarExpr::Literal {
                value: Value::Int64(20_000),
                data_type: DataType::Int64,
            }
        );
    }

    // --- No-op tests ---

    #[test]
    fn no_fold_for_column_reference() {
        let e = col_expr("id", 0);
        assert!(fold_expr(&e).is_none(), "column should not fold");
    }

    #[test]
    fn no_fold_for_column_plus_literal_is_not_a_literal() {
        let e = add_i32(col_expr("x", 0), lit_i32(1));
        // May return Some (rebuilt) or None, but must NOT produce a Literal.
        if let Some(result) = fold_expr(&e) {
            assert!(!matches!(result, ScalarExpr::Literal { .. }));
        }
    }

    #[test]
    fn rule_apply_on_filter_plan() {
        use ultrasql_core::{Field, Schema};

        let predicate = eq_expr(lit_i32(1), lit_i32(1));
        let plan = LogicalPlan::Filter {
            input: Box::new(LogicalPlan::Scan {
                table: "t".into(),
                schema: Schema::new([Field::required("id", DataType::Int32)]).expect("schema ok"),
                projection: None,
            }),
            predicate,
        };
        let rule = ConstantFold;
        let result = rule.apply(&plan).expect("no error");
        assert!(result.is_some(), "filter predicate should fold");
        if let Some(LogicalPlan::Filter { predicate, .. }) = result {
            assert_eq!(predicate, lit_bool(true));
        }
    }

    #[test]
    fn rule_folds_window_partition_order_and_function_exprs() {
        use ultrasql_core::{Field, Schema};

        let input_schema =
            Schema::new([Field::required("id", DataType::Int32)]).expect("input schema ok");
        let output_schema = Schema::new([
            Field::required("id", DataType::Int32),
            Field::required("first_value", DataType::Int32),
        ])
        .expect("output schema ok");
        let plan = LogicalPlan::Window {
            input: Box::new(LogicalPlan::Scan {
                table: "t".into(),
                schema: input_schema,
                projection: None,
            }),
            partition_by: vec![add_i32(lit_i32(1), lit_i32(2))],
            order_by: vec![SortKey {
                expr: add_i32(lit_i32(3), lit_i32(4)),
                asc: true,
                nulls_first: false,
            }],
            func: LogicalWindowFunc::FirstValue(add_i32(lit_i32(5), lit_i32(6))),
            output_name: "first_value".into(),
            schema: output_schema,
        };

        let result = ConstantFold.apply(&plan).expect("constant fold succeeds");
        let Some(LogicalPlan::Window {
            partition_by,
            order_by,
            func,
            ..
        }) = result
        else {
            panic!("window expressions should fold");
        };

        assert_eq!(partition_by, vec![lit_i32(3)]);
        assert_eq!(order_by[0].expr, lit_i32(7));
        assert_eq!(func, LogicalWindowFunc::FirstValue(lit_i32(11)));
    }

    #[test]
    fn rule_returns_none_for_scan() {
        use ultrasql_core::{Field, Schema};

        let schema = Schema::new([Field::required("id", DataType::Int32)]).expect("schema ok");
        let plan = LogicalPlan::Scan {
            table: "t".into(),
            schema,
            projection: None,
        };
        let rule = ConstantFold;
        assert!(rule.apply(&plan).expect("no error").is_none());
    }
}
