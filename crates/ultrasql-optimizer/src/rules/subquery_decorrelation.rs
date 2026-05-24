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
//! - uncorrelated `expr NOT IN (SELECT col FROM sub)` → distinct subquery
//!   values, logical `Anti` join.
//! - uncorrelated scalar subquery in a predicate → cross join the scalar
//!   subplan, replace the subquery expression with its joined column, filter,
//!   then project outer columns.
//! - equality-correlated scalar aggregate subquery in a predicate → group the
//!   inner aggregate by the correlated key, left-join it to the outer input,
//!   replace the scalar subquery with the joined aggregate column, filter, then
//!   project outer columns.
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

use ultrasql_core::{DataType, Field, Schema};
use ultrasql_planner::{BinaryOp, LogicalJoinCondition, LogicalJoinType, LogicalPlan, ScalarExpr};

use crate::error::OptimizeError;
use crate::rules::RewriteRule;

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
// SubqueryKind
// ============================================================================

/// The shape of a subquery predicate extracted from a `Filter` node.
///
/// Variants are constructed by legacy unit-test helpers
/// (`make_exists_filter`, `make_in_subquery_filter`) to exercise the original
/// lowering convention directly. Production rewrites now consume real
/// [`ScalarExpr::Exists`], [`ScalarExpr::InSubquery`], and
/// [`ScalarExpr::ScalarSubquery`] nodes before this compatibility path runs.
#[derive(Debug)]
#[allow(dead_code)] // variants constructed by legacy test helpers
enum SubqueryKind {
    /// `EXISTS(sub)` — semi-join semantics.
    Exists {
        sub: Box<LogicalPlan>,
        negated: bool,
    },
    /// `expr IN (SELECT col FROM sub)` — semi-join on equality.
    InSubquery {
        outer_expr: Box<ScalarExpr>,
        inner_col: Box<ScalarExpr>,
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
            if let Some(rewritten) = rewrite_filter_with_real_subquery_expr(input, predicate) {
                return Ok(Some(rewritten));
            }
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
// Production subquery expression rewrites
// ============================================================================

#[derive(Clone, Debug)]
struct CorrPair {
    inner_name: String,
    inner_index: usize,
    inner_type: DataType,
    outer_name: String,
    outer_index: usize,
    outer_type: DataType,
}

#[derive(Debug)]
struct CorrelatedScalarRight {
    plan: LogicalPlan,
    corr_pairs: Vec<CorrPair>,
    scalar_index: usize,
    scalar_name: String,
}

#[derive(Debug)]
struct CorrelatedExistsInput {
    clean_subplan: LogicalPlan,
    corr_pairs: Vec<CorrPair>,
    residual_predicates: Vec<ScalarExpr>,
}

fn rewrite_filter_with_real_subquery_expr(
    outer: &LogicalPlan,
    predicate: &ScalarExpr,
) -> Option<LogicalPlan> {
    rewrite_scalar_subquery_filter(outer, predicate)
        .or_else(|| rewrite_exists_filter_expr(outer, predicate))
        .or_else(|| rewrite_in_subquery_filter_expr(outer, predicate))
}

fn rewrite_scalar_subquery_filter(
    outer: &LogicalPlan,
    predicate: &ScalarExpr,
) -> Option<LogicalPlan> {
    if let Some((subplan, data_type)) = find_first_correlated_scalar_subquery(predicate) {
        let right = build_correlated_scalar_aggregate_right(*subplan)?;
        let outer_width = outer.schema().len();
        let replacement = ScalarExpr::Column {
            name: right.scalar_name.clone(),
            index: outer_width + right.scalar_index,
            data_type,
        };
        let rewritten_predicate = replace_first_correlated_scalar_subquery(predicate, replacement)?;
        let (outer_predicates, post_join_predicates) =
            split_outer_only_conjuncts(&rewritten_predicate, outer_width);
        let outer_input = filter_with_conjuncts(outer.clone(), outer_predicates);
        let join_condition = build_correlation_condition(&right.corr_pairs, outer_width);
        let join_schema = concat_schemas(outer_input.schema(), right.plan.schema());
        let join = LogicalPlan::Join {
            left: Box::new(outer_input),
            right: Box::new(right.plan),
            join_type: LogicalJoinType::LeftOuter,
            condition: LogicalJoinCondition::On(join_condition),
            schema: join_schema,
        };
        let filtered = filter_with_conjuncts(join, post_join_predicates);
        return Some(project_left(filtered, outer.schema()));
    }

    let outer_width = outer.schema().len();
    let (rewritten_predicate, subplan) = replace_first_uncorrelated_scalar_subquery(
        predicate,
        outer_width,
        "__scalar_subquery".to_owned(),
    )?;
    let right = alias_first_column(*subplan, "__scalar_subquery")?;
    let join_schema = concat_schemas(outer.schema(), right.schema());
    let join = LogicalPlan::Join {
        left: Box::new(outer.clone()),
        right: Box::new(right),
        join_type: LogicalJoinType::Cross,
        condition: LogicalJoinCondition::None,
        schema: join_schema,
    };
    let filtered = LogicalPlan::Filter {
        input: Box::new(join),
        predicate: rewritten_predicate,
    };
    Some(project_left(filtered, outer.schema()))
}

fn build_correlated_scalar_aggregate_right(plan: LogicalPlan) -> Option<CorrelatedScalarRight> {
    match plan {
        LogicalPlan::Project {
            input,
            exprs,
            schema,
        } => {
            if schema.len() != 1 || exprs.len() != 1 {
                return None;
            }
            let (aggregate, corr_pairs) = build_grouped_correlated_aggregate(*input)?;
            let corr_len = corr_pairs.len();
            let scalar_field = schema.field_at(0);
            let scalar_name = "__scalar_subquery".to_owned();
            let mut fields = corr_fields(&corr_pairs);
            fields.push(Field::nullable(
                scalar_name.clone(),
                scalar_field.data_type.clone(),
            ));
            let project_schema = Schema::new(fields).ok()?;

            let mut project_exprs = Vec::with_capacity(corr_len + 1);
            for idx in 0..corr_len {
                let field = aggregate.schema().field_at(idx);
                project_exprs.push((
                    ScalarExpr::Column {
                        name: field.name.clone(),
                        index: idx,
                        data_type: field.data_type.clone(),
                    },
                    field.name.clone(),
                ));
            }
            let shifted_scalar = shift_column_indices_by(&exprs[0].0, corr_len);
            project_exprs.push((shifted_scalar, scalar_name.clone()));

            Some(CorrelatedScalarRight {
                plan: LogicalPlan::Project {
                    input: Box::new(aggregate),
                    exprs: project_exprs,
                    schema: project_schema,
                },
                corr_pairs,
                scalar_index: corr_len,
                scalar_name,
            })
        }
        LogicalPlan::Aggregate { .. } => {
            let (aggregate, corr_pairs) = build_grouped_correlated_aggregate(plan)?;
            let scalar_index = corr_pairs.len();
            if aggregate.schema().len() != scalar_index + 1 {
                return None;
            }
            let scalar_name = aggregate.schema().field_at(scalar_index).name.clone();
            Some(CorrelatedScalarRight {
                plan: aggregate,
                corr_pairs,
                scalar_index,
                scalar_name,
            })
        }
        _ => None,
    }
}

fn build_grouped_correlated_aggregate(plan: LogicalPlan) -> Option<(LogicalPlan, Vec<CorrPair>)> {
    let LogicalPlan::Aggregate {
        input,
        group_by,
        aggregates,
        schema,
    } = plan
    else {
        return None;
    };
    if !group_by.is_empty() || aggregates.is_empty() {
        return None;
    }
    let (clean_input, corr_pairs) = extract_correlated_scalar_input(*input)?;
    if corr_pairs.is_empty() {
        return None;
    }

    let new_group_by = corr_pairs
        .iter()
        .map(|pair| ScalarExpr::Column {
            name: pair.inner_name.clone(),
            index: pair.inner_index,
            data_type: pair.inner_type.clone(),
        })
        .collect::<Vec<_>>();
    let mut fields = corr_fields(&corr_pairs);
    fields.extend(schema.fields().iter().cloned());
    let grouped_schema = Schema::new(fields).ok()?;

    Some((
        LogicalPlan::Aggregate {
            input: Box::new(clean_input),
            group_by: new_group_by,
            aggregates,
            schema: grouped_schema,
        },
        corr_pairs,
    ))
}

fn extract_correlated_scalar_input(plan: LogicalPlan) -> Option<(LogicalPlan, Vec<CorrPair>)> {
    let LogicalPlan::Filter { input, predicate } = plan else {
        return None;
    };
    let mut local = Vec::new();
    let mut corr = Vec::new();
    for conjunct in split_and(&predicate) {
        if conjunct.contains_outer_column() {
            corr.push(parse_correlation_equality(&conjunct)?);
        } else {
            local.push(conjunct);
        }
    }
    let clean = filter_with_conjuncts(*input, local);
    Some((clean, corr))
}

fn corr_fields(pairs: &[CorrPair]) -> Vec<Field> {
    pairs
        .iter()
        .enumerate()
        .map(|(idx, pair)| Field::nullable(format!("__corr_{idx}"), pair.inner_type.clone()))
        .collect()
}

fn split_outer_only_conjuncts(
    predicate: &ScalarExpr,
    outer_width: usize,
) -> (Vec<ScalarExpr>, Vec<ScalarExpr>) {
    let mut outer_only = Vec::new();
    let mut post_join = Vec::new();
    for conjunct in split_and(predicate) {
        let mut refs = Vec::new();
        collect_column_refs(&conjunct, &mut refs);
        if !expr_contains_subquery(&conjunct)
            && !refs.is_empty()
            && refs.iter().all(|idx| *idx < outer_width)
        {
            outer_only.push(conjunct);
        } else {
            post_join.push(conjunct);
        }
    }
    (outer_only, post_join)
}

fn collect_column_refs(expr: &ScalarExpr, refs: &mut Vec<usize>) {
    match expr {
        ScalarExpr::Column { index, .. } => refs.push(*index),
        ScalarExpr::Unary { expr, .. } | ScalarExpr::IsNull { expr, .. } => {
            collect_column_refs(expr, refs);
        }
        ScalarExpr::Binary { left, right, .. } => {
            collect_column_refs(left, refs);
            collect_column_refs(right, refs);
        }
        ScalarExpr::FunctionCall { args, .. } => {
            for arg in args {
                collect_column_refs(arg, refs);
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

fn expr_contains_subquery(expr: &ScalarExpr) -> bool {
    match expr {
        ScalarExpr::ScalarSubquery { .. }
        | ScalarExpr::Exists { .. }
        | ScalarExpr::InSubquery { .. } => true,
        ScalarExpr::Unary { expr, .. } | ScalarExpr::IsNull { expr, .. } => {
            expr_contains_subquery(expr)
        }
        ScalarExpr::Binary { left, right, .. } => {
            expr_contains_subquery(left) || expr_contains_subquery(right)
        }
        ScalarExpr::FunctionCall { args, .. } => args.iter().any(expr_contains_subquery),
        ScalarExpr::Column { .. }
        | ScalarExpr::Literal { .. }
        | ScalarExpr::Parameter { .. }
        | ScalarExpr::OuterColumn { .. } => false,
    }
}

fn rewrite_exists_filter_expr(outer: &LogicalPlan, predicate: &ScalarExpr) -> Option<LogicalPlan> {
    let conjuncts = split_and(predicate);
    let (idx, exists_expr) = conjuncts.iter().enumerate().find(|(_, c)| {
        matches!(
            c,
            ScalarExpr::Exists {
                correlated: true,
                ..
            }
        )
    })?;
    let ScalarExpr::Exists {
        subplan,
        negated,
        correlated: true,
    } = exists_expr
    else {
        return None;
    };
    let rest = conjuncts
        .iter()
        .enumerate()
        .filter_map(|(i, c)| if i == idx { None } else { Some(c.clone()) })
        .collect::<Vec<_>>();
    let outer = filter_with_conjuncts(outer.clone(), rest);
    let outer_width = outer.schema().len();
    let exists_input = strip_exists_projection(*subplan.clone());
    let exists_input = extract_correlated_exists_input(exists_input, outer_width)?;
    if exists_input.corr_pairs.is_empty() {
        return None;
    }

    if exists_input.residual_predicates.is_empty() {
        let right =
            distinct_correlation_keys(exists_input.clean_subplan, &exists_input.corr_pairs)?;
        let join_condition = build_correlation_condition(&exists_input.corr_pairs, outer_width);
        let join = LogicalPlan::Join {
            left: Box::new(outer.clone()),
            right: Box::new(right),
            join_type: if *negated {
                LogicalJoinType::Anti
            } else {
                LogicalJoinType::Semi
            },
            condition: LogicalJoinCondition::On(join_condition),
            schema: outer.schema().clone(),
        };
        return Some(join);
    }

    let (right, corr_pairs, residual_predicates) = project_exists_right_for_residual(
        exists_input.clean_subplan,
        &exists_input.corr_pairs,
        &exists_input.residual_predicates,
        outer_width,
    )?;
    let mut join_predicates = vec![build_correlation_condition_against_right_schema(
        &corr_pairs,
        outer_width,
    )];
    join_predicates.extend(residual_predicates);
    let join_condition = conjuncts_to_and(join_predicates);
    let join = LogicalPlan::Join {
        left: Box::new(outer.clone()),
        right: Box::new(right),
        join_type: if *negated {
            LogicalJoinType::Anti
        } else {
            LogicalJoinType::Semi
        },
        condition: LogicalJoinCondition::On(join_condition),
        schema: outer.schema().clone(),
    };
    Some(join)
}

fn project_exists_right_for_residual(
    input: LogicalPlan,
    corr_pairs: &[CorrPair],
    residual_predicates: &[ScalarExpr],
    outer_width: usize,
) -> Option<(LogicalPlan, Vec<CorrPair>, Vec<ScalarExpr>)> {
    let mut needed = Vec::with_capacity(corr_pairs.len() + residual_predicates.len());
    for pair in corr_pairs {
        push_unique_index(&mut needed, pair.inner_index);
    }
    for predicate in residual_predicates {
        collect_join_right_column_indices(predicate, outer_width, &mut needed);
    }
    if needed.is_empty() {
        return Some((input, corr_pairs.to_vec(), residual_predicates.to_vec()));
    }

    let input_schema = input.schema().clone();
    let mut fields = Vec::with_capacity(needed.len());
    let mut exprs = Vec::with_capacity(needed.len());
    for &idx in &needed {
        let field = input_schema.fields().get(idx)?;
        fields.push(field.clone());
        exprs.push((
            ScalarExpr::Column {
                name: field.name.clone(),
                index: idx,
                data_type: field.data_type.clone(),
            },
            field.name.clone(),
        ));
    }
    let projected_schema = Schema::new(fields).ok()?;
    let projected = LogicalPlan::Project {
        input: Box::new(input),
        exprs,
        schema: projected_schema,
    };

    let mut projected_pairs = Vec::with_capacity(corr_pairs.len());
    for pair in corr_pairs {
        let projected_idx = needed.iter().position(|&idx| idx == pair.inner_index)?;
        let mut projected_pair = pair.clone();
        projected_pair.inner_index = projected_idx;
        projected_pairs.push(projected_pair);
    }
    let projected_residuals = residual_predicates
        .iter()
        .map(|predicate| rebase_projected_exists_residual(predicate, outer_width, &needed))
        .collect::<Option<Vec<_>>>()?;

    Some((projected, projected_pairs, projected_residuals))
}

fn push_unique_index(indices: &mut Vec<usize>, index: usize) {
    if !indices.contains(&index) {
        indices.push(index);
    }
}

fn collect_join_right_column_indices(expr: &ScalarExpr, outer_width: usize, out: &mut Vec<usize>) {
    match expr {
        ScalarExpr::Column { index, .. } => {
            if *index >= outer_width {
                push_unique_index(out, *index - outer_width);
            }
        }
        ScalarExpr::Unary { expr, .. } | ScalarExpr::IsNull { expr, .. } => {
            collect_join_right_column_indices(expr, outer_width, out);
        }
        ScalarExpr::Binary { left, right, .. } => {
            collect_join_right_column_indices(left, outer_width, out);
            collect_join_right_column_indices(right, outer_width, out);
        }
        ScalarExpr::FunctionCall { args, .. } => {
            for arg in args {
                collect_join_right_column_indices(arg, outer_width, out);
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

fn rebase_projected_exists_residual(
    expr: &ScalarExpr,
    outer_width: usize,
    projected_indices: &[usize],
) -> Option<ScalarExpr> {
    match expr {
        ScalarExpr::Column {
            name,
            index,
            data_type,
        } => {
            if *index < outer_width {
                return Some(ScalarExpr::Column {
                    name: name.clone(),
                    index: *index,
                    data_type: data_type.clone(),
                });
            }
            let original_inner_idx = *index - outer_width;
            let projected_idx = projected_indices
                .iter()
                .position(|&idx| idx == original_inner_idx)?;
            Some(ScalarExpr::Column {
                name: name.clone(),
                index: outer_width + projected_idx,
                data_type: data_type.clone(),
            })
        }
        ScalarExpr::OuterColumn { .. } => None,
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
            expr: Box::new(rebase_projected_exists_residual(
                inner,
                outer_width,
                projected_indices,
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
            left: Box::new(rebase_projected_exists_residual(
                left,
                outer_width,
                projected_indices,
            )?),
            right: Box::new(rebase_projected_exists_residual(
                right,
                outer_width,
                projected_indices,
            )?),
            data_type: data_type.clone(),
        }),
        ScalarExpr::IsNull { expr, negated } => Some(ScalarExpr::IsNull {
            expr: Box::new(rebase_projected_exists_residual(
                expr,
                outer_width,
                projected_indices,
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
                .map(|arg| rebase_projected_exists_residual(arg, outer_width, projected_indices))
                .collect::<Option<Vec<_>>>()?,
            data_type: data_type.clone(),
        }),
        ScalarExpr::ScalarSubquery { .. }
        | ScalarExpr::Exists { .. }
        | ScalarExpr::InSubquery { .. } => None,
    }
}

fn rewrite_in_subquery_filter_expr(
    outer: &LogicalPlan,
    predicate: &ScalarExpr,
) -> Option<LogicalPlan> {
    let conjuncts = split_and(predicate);
    let (idx, in_expr) = conjuncts
        .iter()
        .enumerate()
        .find(|(_, c)| matches!(c, ScalarExpr::InSubquery { .. }))?;
    let ScalarExpr::InSubquery {
        expr,
        subplan,
        negated,
        correlated,
        data_type,
    } = in_expr
    else {
        return None;
    };
    let rest = conjuncts
        .iter()
        .enumerate()
        .filter_map(|(i, c)| if i == idx { None } else { Some(c.clone()) })
        .collect::<Vec<_>>();
    let outer = filter_with_conjuncts(outer.clone(), rest);
    if *correlated {
        return rewrite_correlated_in_subquery(&outer, expr, *subplan.clone(), *negated, data_type);
    }
    if subplan.schema().len() != 1 {
        return None;
    }
    let right = distinct_single_column(*subplan.clone())?;
    let outer_width = outer.schema().len();
    let right_col = ScalarExpr::Column {
        name: right.schema().field_at(0).name.clone(),
        index: outer_width,
        data_type: data_type.clone(),
    };
    let join_condition = ScalarExpr::Binary {
        op: BinaryOp::Eq,
        left: expr.clone(),
        right: Box::new(right_col.clone()),
        data_type: DataType::Bool,
    };
    let join = LogicalPlan::Join {
        left: Box::new(outer.clone()),
        right: Box::new(right),
        join_type: if *negated {
            LogicalJoinType::Anti
        } else {
            LogicalJoinType::Semi
        },
        condition: LogicalJoinCondition::On(join_condition),
        schema: outer.schema().clone(),
    };
    Some(join)
}

fn rewrite_correlated_in_subquery(
    outer: &LogicalPlan,
    outer_expr: &ScalarExpr,
    subplan: LogicalPlan,
    negated: bool,
    data_type: &DataType,
) -> Option<LogicalPlan> {
    let (right, corr_pairs, value_index, value_name) =
        build_correlated_in_right(subplan, data_type)?;
    let outer_width = outer.schema().len();
    let mut predicates = vec![build_correlation_condition_against_right_schema(
        &corr_pairs,
        outer_width,
    )];
    predicates.push(ScalarExpr::Binary {
        op: BinaryOp::Eq,
        left: Box::new(outer_expr.clone()),
        right: Box::new(ScalarExpr::Column {
            name: value_name,
            index: outer_width + value_index,
            data_type: data_type.clone(),
        }),
        data_type: DataType::Bool,
    });
    Some(LogicalPlan::Join {
        left: Box::new(outer.clone()),
        right: Box::new(right),
        join_type: if negated {
            LogicalJoinType::Anti
        } else {
            LogicalJoinType::Semi
        },
        condition: LogicalJoinCondition::On(conjuncts_to_and(predicates)),
        schema: outer.schema().clone(),
    })
}

fn build_correlated_in_right(
    plan: LogicalPlan,
    data_type: &DataType,
) -> Option<(LogicalPlan, Vec<CorrPair>, usize, String)> {
    let LogicalPlan::Project {
        input,
        exprs,
        schema,
    } = plan
    else {
        return None;
    };
    if schema.len() != 1 || exprs.len() != 1 || exprs[0].0.contains_outer_column() {
        return None;
    }
    let (clean_input, corr_pairs) = extract_correlated_scalar_input(*input)?;
    if corr_pairs.is_empty() {
        return None;
    }

    let value_name = "__in_subquery".to_owned();
    let mut fields = corr_fields(&corr_pairs);
    fields.push(Field::nullable(value_name.clone(), data_type.clone()));
    let project_schema = Schema::new(fields).ok()?;

    let mut project_exprs = Vec::with_capacity(corr_pairs.len() + 1);
    let mut projected_pairs = Vec::with_capacity(corr_pairs.len());
    for (idx, pair) in corr_pairs.iter().enumerate() {
        project_exprs.push((
            ScalarExpr::Column {
                name: pair.inner_name.clone(),
                index: pair.inner_index,
                data_type: pair.inner_type.clone(),
            },
            format!("__corr_{idx}"),
        ));
        let mut projected_pair = pair.clone();
        projected_pair.inner_index = idx;
        projected_pairs.push(projected_pair);
    }
    project_exprs.push((exprs[0].0.clone(), value_name.clone()));
    let project = LogicalPlan::Project {
        input: Box::new(clean_input),
        exprs: project_exprs,
        schema: project_schema.clone(),
    };
    let group_by = project_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, field)| ScalarExpr::Column {
            name: field.name.clone(),
            index: idx,
            data_type: field.data_type.clone(),
        })
        .collect::<Vec<_>>();
    let right = LogicalPlan::Aggregate {
        input: Box::new(project),
        group_by,
        aggregates: Vec::new(),
        schema: project_schema,
    };
    let value_index = projected_pairs.len();
    Some((right, projected_pairs, value_index, value_name))
}

fn strip_exists_projection(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Project { input, .. } => *input,
        other => other,
    }
}

fn extract_correlated_exists_input(
    plan: LogicalPlan,
    outer_width: usize,
) -> Option<CorrelatedExistsInput> {
    match plan {
        LogicalPlan::Filter { input, predicate } => {
            let mut local = Vec::new();
            let mut corr = Vec::new();
            let mut residual = Vec::new();
            for conjunct in split_and(&predicate) {
                if conjunct.contains_outer_column() {
                    if let Some(pair) = parse_correlation_equality(&conjunct) {
                        corr.push(pair);
                    } else {
                        residual.push(rebase_correlated_predicate(&conjunct, outer_width)?);
                    }
                } else {
                    local.push(conjunct);
                }
            }
            let clean = filter_with_conjuncts(*input, local);
            Some(CorrelatedExistsInput {
                clean_subplan: clean,
                corr_pairs: corr,
                residual_predicates: residual,
            })
        }
        _ => None,
    }
}

fn rebase_correlated_predicate(expr: &ScalarExpr, outer_width: usize) -> Option<ScalarExpr> {
    match expr {
        ScalarExpr::Column {
            name,
            index,
            data_type,
        } => Some(ScalarExpr::Column {
            name: name.clone(),
            index: outer_width + index,
            data_type: data_type.clone(),
        }),
        ScalarExpr::OuterColumn {
            name,
            frame_depth: 1,
            column_index,
            data_type,
        } => Some(ScalarExpr::Column {
            name: name.clone(),
            index: *column_index,
            data_type: data_type.clone(),
        }),
        ScalarExpr::OuterColumn { .. } => None,
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
            expr: Box::new(rebase_correlated_predicate(inner, outer_width)?),
            data_type: data_type.clone(),
        }),
        ScalarExpr::Binary {
            op,
            left,
            right,
            data_type,
        } => Some(ScalarExpr::Binary {
            op: *op,
            left: Box::new(rebase_correlated_predicate(left, outer_width)?),
            right: Box::new(rebase_correlated_predicate(right, outer_width)?),
            data_type: data_type.clone(),
        }),
        ScalarExpr::IsNull { expr, negated } => Some(ScalarExpr::IsNull {
            expr: Box::new(rebase_correlated_predicate(expr, outer_width)?),
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
                .map(|arg| rebase_correlated_predicate(arg, outer_width))
                .collect::<Option<Vec<_>>>()?,
            data_type: data_type.clone(),
        }),
        ScalarExpr::ScalarSubquery { .. }
        | ScalarExpr::Exists { .. }
        | ScalarExpr::InSubquery { .. } => None,
    }
}

fn parse_correlation_equality(expr: &ScalarExpr) -> Option<CorrPair> {
    let ScalarExpr::Binary {
        op: BinaryOp::Eq,
        left,
        right,
        ..
    } = expr
    else {
        return None;
    };
    match (left.as_ref(), right.as_ref()) {
        (
            ScalarExpr::Column {
                name,
                index,
                data_type,
            },
            ScalarExpr::OuterColumn {
                name: outer_name,
                frame_depth: 1,
                column_index,
                data_type: outer_type,
            },
        )
        | (
            ScalarExpr::OuterColumn {
                name: outer_name,
                frame_depth: 1,
                column_index,
                data_type: outer_type,
            },
            ScalarExpr::Column {
                name,
                index,
                data_type,
            },
        ) => Some(CorrPair {
            inner_name: name.clone(),
            inner_index: *index,
            inner_type: data_type.clone(),
            outer_name: outer_name.clone(),
            outer_index: *column_index,
            outer_type: outer_type.clone(),
        }),
        _ => None,
    }
}

fn distinct_correlation_keys(input: LogicalPlan, pairs: &[CorrPair]) -> Option<LogicalPlan> {
    let mut exprs = Vec::with_capacity(pairs.len());
    let mut fields = Vec::with_capacity(pairs.len());
    for (idx, pair) in pairs.iter().enumerate() {
        let name = format!("__corr_{idx}");
        exprs.push((
            ScalarExpr::Column {
                name: pair.inner_name.clone(),
                index: pair.inner_index,
                data_type: pair.inner_type.clone(),
            },
            name.clone(),
        ));
        fields.push(Field::nullable(name, pair.inner_type.clone()));
    }
    let schema = Schema::new(fields).ok()?;
    let project = LogicalPlan::Project {
        input: Box::new(input),
        exprs,
        schema: schema.clone(),
    };
    let group_by = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, field)| ScalarExpr::Column {
            name: field.name.clone(),
            index: idx,
            data_type: field.data_type.clone(),
        })
        .collect();
    Some(LogicalPlan::Aggregate {
        input: Box::new(project),
        group_by,
        aggregates: Vec::new(),
        schema,
    })
}

fn distinct_single_column(input: LogicalPlan) -> Option<LogicalPlan> {
    let schema = input.schema().clone();
    if schema.len() != 1 {
        return None;
    }
    let field = schema.field_at(0);
    let group_by = vec![ScalarExpr::Column {
        name: field.name.clone(),
        index: 0,
        data_type: field.data_type.clone(),
    }];
    Some(LogicalPlan::Aggregate {
        input: Box::new(input),
        group_by,
        aggregates: Vec::new(),
        schema,
    })
}

fn alias_first_column(input: LogicalPlan, name: &str) -> Option<LogicalPlan> {
    if input.schema().len() != 1 {
        return None;
    }
    let field = input.schema().field_at(0);
    let field_name = field.name.clone();
    let field_type = field.data_type.clone();
    let schema = Schema::new([Field::nullable(name, field.data_type.clone())]).ok()?;
    Some(LogicalPlan::Project {
        input: Box::new(input),
        exprs: vec![(
            ScalarExpr::Column {
                name: field_name,
                index: 0,
                data_type: field_type,
            },
            name.to_owned(),
        )],
        schema,
    })
}

fn build_correlation_condition(pairs: &[CorrPair], outer_width: usize) -> ScalarExpr {
    let mut predicates = pairs
        .iter()
        .enumerate()
        .map(|(idx, pair)| ScalarExpr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(ScalarExpr::Column {
                name: pair.outer_name.clone(),
                index: pair.outer_index,
                data_type: pair.outer_type.clone(),
            }),
            right: Box::new(ScalarExpr::Column {
                name: format!("__corr_{idx}"),
                index: outer_width + idx,
                data_type: pair.inner_type.clone(),
            }),
            data_type: DataType::Bool,
        });
    let first = predicates.next().expect("at least one correlation pair");
    predicates.fold(first, |left, right| ScalarExpr::Binary {
        op: BinaryOp::And,
        left: Box::new(left),
        right: Box::new(right),
        data_type: DataType::Bool,
    })
}

fn build_correlation_condition_against_right_schema(
    pairs: &[CorrPair],
    outer_width: usize,
) -> ScalarExpr {
    let mut predicates = pairs.iter().map(|pair| ScalarExpr::Binary {
        op: BinaryOp::Eq,
        left: Box::new(ScalarExpr::Column {
            name: pair.outer_name.clone(),
            index: pair.outer_index,
            data_type: pair.outer_type.clone(),
        }),
        right: Box::new(ScalarExpr::Column {
            name: pair.inner_name.clone(),
            index: outer_width + pair.inner_index,
            data_type: pair.inner_type.clone(),
        }),
        data_type: DataType::Bool,
    });
    let first = predicates.next().expect("at least one correlation pair");
    predicates.fold(first, |left, right| ScalarExpr::Binary {
        op: BinaryOp::And,
        left: Box::new(left),
        right: Box::new(right),
        data_type: DataType::Bool,
    })
}

fn filter_with_conjuncts(input: LogicalPlan, conjuncts: Vec<ScalarExpr>) -> LogicalPlan {
    if conjuncts.is_empty() {
        input
    } else {
        LogicalPlan::Filter {
            input: Box::new(input),
            predicate: conjuncts_to_and(conjuncts),
        }
    }
}

fn project_left(input: LogicalPlan, schema: &Schema) -> LogicalPlan {
    let exprs = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, field)| {
            (
                ScalarExpr::Column {
                    name: field.name.clone(),
                    index: idx,
                    data_type: field.data_type.clone(),
                },
                field.name.clone(),
            )
        })
        .collect();
    LogicalPlan::Project {
        input: Box::new(input),
        exprs,
        schema: schema.clone(),
    }
}

fn replace_first_uncorrelated_scalar_subquery(
    expr: &ScalarExpr,
    replacement_index: usize,
    replacement_name: String,
) -> Option<(ScalarExpr, Box<LogicalPlan>)> {
    match expr {
        ScalarExpr::ScalarSubquery {
            subplan,
            correlated: false,
            data_type,
        } => Some((
            ScalarExpr::Column {
                name: replacement_name,
                index: replacement_index,
                data_type: data_type.clone(),
            },
            subplan.clone(),
        )),
        ScalarExpr::Binary {
            op,
            left,
            right,
            data_type,
        } => {
            if let Some((new_left, subplan)) = replace_first_uncorrelated_scalar_subquery(
                left,
                replacement_index,
                replacement_name.clone(),
            ) {
                return Some((
                    ScalarExpr::Binary {
                        op: *op,
                        left: Box::new(new_left),
                        right: right.clone(),
                        data_type: data_type.clone(),
                    },
                    subplan,
                ));
            }
            replace_first_uncorrelated_scalar_subquery(right, replacement_index, replacement_name)
                .map(|(new_right, subplan)| {
                    (
                        ScalarExpr::Binary {
                            op: *op,
                            left: left.clone(),
                            right: Box::new(new_right),
                            data_type: data_type.clone(),
                        },
                        subplan,
                    )
                })
        }
        ScalarExpr::Unary {
            op,
            expr: inner,
            data_type,
        } => replace_first_uncorrelated_scalar_subquery(inner, replacement_index, replacement_name)
            .map(|(new_inner, subplan)| {
                (
                    ScalarExpr::Unary {
                        op: *op,
                        expr: Box::new(new_inner),
                        data_type: data_type.clone(),
                    },
                    subplan,
                )
            }),
        ScalarExpr::IsNull {
            expr: inner,
            negated,
        } => replace_first_uncorrelated_scalar_subquery(inner, replacement_index, replacement_name)
            .map(|(new_inner, subplan)| {
                (
                    ScalarExpr::IsNull {
                        expr: Box::new(new_inner),
                        negated: *negated,
                    },
                    subplan,
                )
            }),
        ScalarExpr::FunctionCall {
            name,
            args,
            data_type,
        } => {
            for (idx, arg) in args.iter().enumerate() {
                if let Some((new_arg, subplan)) = replace_first_uncorrelated_scalar_subquery(
                    arg,
                    replacement_index,
                    replacement_name.clone(),
                ) {
                    let mut new_args = args.clone();
                    new_args[idx] = new_arg;
                    return Some((
                        ScalarExpr::FunctionCall {
                            name: name.clone(),
                            args: new_args,
                            data_type: data_type.clone(),
                        },
                        subplan,
                    ));
                }
            }
            None
        }
        ScalarExpr::Column { .. }
        | ScalarExpr::Literal { .. }
        | ScalarExpr::Parameter { .. }
        | ScalarExpr::OuterColumn { .. }
        | ScalarExpr::ScalarSubquery {
            correlated: true, ..
        }
        | ScalarExpr::Exists { .. }
        | ScalarExpr::InSubquery { .. } => None,
    }
}

fn find_first_correlated_scalar_subquery(
    expr: &ScalarExpr,
) -> Option<(Box<LogicalPlan>, DataType)> {
    match expr {
        ScalarExpr::ScalarSubquery {
            subplan,
            correlated: true,
            data_type,
        } => Some((subplan.clone(), data_type.clone())),
        ScalarExpr::Binary { left, right, .. } => find_first_correlated_scalar_subquery(left)
            .or_else(|| find_first_correlated_scalar_subquery(right)),
        ScalarExpr::Unary { expr: inner, .. } | ScalarExpr::IsNull { expr: inner, .. } => {
            find_first_correlated_scalar_subquery(inner)
        }
        ScalarExpr::FunctionCall { args, .. } => {
            args.iter().find_map(find_first_correlated_scalar_subquery)
        }
        ScalarExpr::Column { .. }
        | ScalarExpr::Literal { .. }
        | ScalarExpr::Parameter { .. }
        | ScalarExpr::OuterColumn { .. }
        | ScalarExpr::ScalarSubquery {
            correlated: false, ..
        }
        | ScalarExpr::Exists { .. }
        | ScalarExpr::InSubquery { .. } => None,
    }
}

fn replace_first_correlated_scalar_subquery(
    expr: &ScalarExpr,
    replacement: ScalarExpr,
) -> Option<ScalarExpr> {
    match expr {
        ScalarExpr::ScalarSubquery {
            correlated: true, ..
        } => Some(replacement),
        ScalarExpr::Binary {
            op,
            left,
            right,
            data_type,
        } => {
            if let Some(new_left) =
                replace_first_correlated_scalar_subquery(left, replacement.clone())
            {
                return Some(ScalarExpr::Binary {
                    op: *op,
                    left: Box::new(new_left),
                    right: right.clone(),
                    data_type: data_type.clone(),
                });
            }
            replace_first_correlated_scalar_subquery(right, replacement).map(|new_right| {
                ScalarExpr::Binary {
                    op: *op,
                    left: left.clone(),
                    right: Box::new(new_right),
                    data_type: data_type.clone(),
                }
            })
        }
        ScalarExpr::Unary {
            op,
            expr: inner,
            data_type,
        } => replace_first_correlated_scalar_subquery(inner, replacement).map(|new_inner| {
            ScalarExpr::Unary {
                op: *op,
                expr: Box::new(new_inner),
                data_type: data_type.clone(),
            }
        }),
        ScalarExpr::IsNull {
            expr: inner,
            negated,
        } => replace_first_correlated_scalar_subquery(inner, replacement).map(|new_inner| {
            ScalarExpr::IsNull {
                expr: Box::new(new_inner),
                negated: *negated,
            }
        }),
        ScalarExpr::FunctionCall {
            name,
            args,
            data_type,
        } => {
            for (idx, arg) in args.iter().enumerate() {
                if let Some(new_arg) =
                    replace_first_correlated_scalar_subquery(arg, replacement.clone())
                {
                    let mut new_args = args.clone();
                    new_args[idx] = new_arg;
                    return Some(ScalarExpr::FunctionCall {
                        name: name.clone(),
                        args: new_args,
                        data_type: data_type.clone(),
                    });
                }
            }
            None
        }
        ScalarExpr::Column { .. }
        | ScalarExpr::Literal { .. }
        | ScalarExpr::Parameter { .. }
        | ScalarExpr::OuterColumn { .. }
        | ScalarExpr::ScalarSubquery {
            correlated: false, ..
        }
        | ScalarExpr::Exists { .. }
        | ScalarExpr::InSubquery { .. } => None,
    }
}

fn shift_column_indices_by(expr: &ScalarExpr, offset: usize) -> ScalarExpr {
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
        ScalarExpr::Literal { value, data_type } => ScalarExpr::Literal {
            value: value.clone(),
            data_type: data_type.clone(),
        },
        ScalarExpr::Parameter { index, data_type } => ScalarExpr::Parameter {
            index: *index,
            data_type: data_type.clone(),
        },
        ScalarExpr::Unary {
            op,
            expr: inner,
            data_type,
        } => ScalarExpr::Unary {
            op: *op,
            expr: Box::new(shift_column_indices_by(inner, offset)),
            data_type: data_type.clone(),
        },
        ScalarExpr::Binary {
            op,
            left,
            right,
            data_type,
        } => ScalarExpr::Binary {
            op: *op,
            left: Box::new(shift_column_indices_by(left, offset)),
            right: Box::new(shift_column_indices_by(right, offset)),
            data_type: data_type.clone(),
        },
        ScalarExpr::IsNull {
            expr: inner,
            negated,
        } => ScalarExpr::IsNull {
            expr: Box::new(shift_column_indices_by(inner, offset)),
            negated: *negated,
        },
        ScalarExpr::FunctionCall {
            name,
            args,
            data_type,
        } => ScalarExpr::FunctionCall {
            name: name.clone(),
            args: args
                .iter()
                .map(|arg| shift_column_indices_by(arg, offset))
                .collect(),
            data_type: data_type.clone(),
        },
        ScalarExpr::OuterColumn { .. }
        | ScalarExpr::ScalarSubquery { .. }
        | ScalarExpr::Exists { .. }
        | ScalarExpr::InSubquery { .. } => expr.clone(),
    }
}

fn split_and(expr: &ScalarExpr) -> Vec<ScalarExpr> {
    match expr {
        ScalarExpr::Binary {
            op: BinaryOp::And,
            left,
            right,
            ..
        } => {
            let mut out = split_and(left);
            out.extend(split_and(right));
            out
        }
        other => vec![other.clone()],
    }
}

fn conjuncts_to_and(mut predicates: Vec<ScalarExpr>) -> ScalarExpr {
    assert!(
        !predicates.is_empty(),
        "conjuncts_to_and called with empty list"
    );
    let mut result = predicates.remove(0);
    for predicate in predicates {
        result = ScalarExpr::Binary {
            op: BinaryOp::And,
            left: Box::new(result),
            right: Box::new(predicate),
            data_type: DataType::Bool,
        };
    }
    result
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
                left: outer_expr,
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
    let mut names = std::collections::HashSet::with_capacity(left.len() + right.len());
    for i in 0..left.len() {
        let field = left.field_at(i).clone();
        names.insert(field.name.to_ascii_lowercase());
        fields.push(field);
    }
    for i in 0..right.len() {
        let field = right.field_at(i);
        let name = if names.contains(&field.name.to_ascii_lowercase()) {
            format!("{}_1", field.name)
        } else {
            field.name.clone()
        };
        names.insert(name.to_ascii_lowercase());
        fields.push(Field {
            name,
            data_type: field.data_type.clone(),
            nullable: field.nullable,
        });
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
        outer_expr: Box::new(outer_expr),
        inner_col: Box::new(inner_col),
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
        AggregateFunc, BinaryOp, LogicalAggregateExpr, LogicalJoinCondition, LogicalJoinType,
        LogicalPlan, ScalarExpr,
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

    fn outer_col(name: &str, idx: usize, dt: DataType) -> ScalarExpr {
        ScalarExpr::OuterColumn {
            name: name.into(),
            frame_depth: 1,
            column_index: idx,
            data_type: dt,
        }
    }

    fn sub_key_project() -> LogicalPlan {
        let input = sub_scan();
        let schema = Schema::new([Field::required("key", DataType::Int32)]).expect("schema ok");
        LogicalPlan::Project {
            input: Box::new(input),
            exprs: vec![(col("key", 0, DataType::Int32), "key".into())],
            schema,
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

    #[test]
    fn real_correlated_exists_rewrites_to_semi_join() {
        let sub = LogicalPlan::Filter {
            input: Box::new(sub_scan()),
            predicate: ScalarExpr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(col("key", 0, DataType::Int32)),
                right: Box::new(outer_col("id", 0, DataType::Int32)),
                data_type: DataType::Bool,
            },
        };
        let plan = LogicalPlan::Filter {
            input: Box::new(outer_scan()),
            predicate: ScalarExpr::Exists {
                subplan: Box::new(sub),
                negated: false,
                correlated: true,
            },
        };

        let result = SubqueryDecorrelation
            .apply(&plan)
            .expect("no error")
            .expect("rewrite");
        assert_eq!(result.schema().len(), 2);
        assert!(
            matches!(
                result,
                LogicalPlan::Join {
                    join_type: LogicalJoinType::Semi,
                    condition: LogicalJoinCondition::On(_),
                    ..
                }
            ),
            "EXISTS should become Semi join, got {result:?}"
        );
    }

    #[test]
    fn real_correlated_not_exists_rewrites_to_anti_join() {
        let sub = LogicalPlan::Filter {
            input: Box::new(sub_scan()),
            predicate: ScalarExpr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(col("key", 0, DataType::Int32)),
                right: Box::new(outer_col("id", 0, DataType::Int32)),
                data_type: DataType::Bool,
            },
        };
        let plan = LogicalPlan::Filter {
            input: Box::new(outer_scan()),
            predicate: ScalarExpr::Exists {
                subplan: Box::new(sub),
                negated: true,
                correlated: true,
            },
        };

        let result = SubqueryDecorrelation
            .apply(&plan)
            .expect("no error")
            .expect("rewrite");
        assert_eq!(result.schema().len(), 2);
        assert!(
            matches!(
                result,
                LogicalPlan::Join {
                    join_type: LogicalJoinType::Anti,
                    condition: LogicalJoinCondition::On(_),
                    ..
                }
            ),
            "NOT EXISTS should become Anti join, got {result:?}"
        );
    }

    #[test]
    fn real_correlated_exists_with_residual_projects_inner_columns() {
        let corr = ScalarExpr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(col("key", 0, DataType::Int32)),
            right: Box::new(outer_col("id", 0, DataType::Int32)),
            data_type: DataType::Bool,
        };
        let residual = ScalarExpr::Binary {
            op: BinaryOp::NotEq,
            left: Box::new(col("data", 1, DataType::Int32)),
            right: Box::new(outer_col("val", 1, DataType::Int32)),
            data_type: DataType::Bool,
        };
        let sub = LogicalPlan::Filter {
            input: Box::new(sub_scan()),
            predicate: ScalarExpr::Binary {
                op: BinaryOp::And,
                left: Box::new(corr),
                right: Box::new(residual),
                data_type: DataType::Bool,
            },
        };
        let plan = LogicalPlan::Filter {
            input: Box::new(outer_scan()),
            predicate: ScalarExpr::Exists {
                subplan: Box::new(sub),
                negated: false,
                correlated: true,
            },
        };

        let result = SubqueryDecorrelation
            .apply(&plan)
            .expect("no error")
            .expect("rewrite");
        let LogicalPlan::Join {
            right,
            join_type,
            condition,
            ..
        } = result
        else {
            panic!("EXISTS should become join");
        };
        assert_eq!(join_type, LogicalJoinType::Semi);
        assert_eq!(right.schema().len(), 2);
        assert!(matches!(right.as_ref(), LogicalPlan::Project { .. },));
        let LogicalJoinCondition::On(predicate) = condition else {
            panic!("expected ON predicate");
        };
        let dump = predicate.to_string();
        assert!(
            dump.contains("data") && dump.contains("val"),
            "residual should survive after right projection, got {dump}"
        );
    }

    #[test]
    fn real_uncorrelated_in_rewrites_to_semi_join() {
        let plan = LogicalPlan::Filter {
            input: Box::new(outer_scan()),
            predicate: ScalarExpr::InSubquery {
                expr: Box::new(col("id", 0, DataType::Int32)),
                subplan: Box::new(sub_key_project()),
                negated: false,
                correlated: false,
                data_type: DataType::Int32,
            },
        };

        let result = SubqueryDecorrelation
            .apply(&plan)
            .expect("no error")
            .expect("rewrite");
        assert_eq!(result.schema().len(), 2);
        assert!(matches!(
            result,
            LogicalPlan::Join {
                join_type: LogicalJoinType::Semi,
                ..
            }
        ));
    }

    #[test]
    fn real_uncorrelated_not_in_rewrites_to_anti_join() {
        let plan = LogicalPlan::Filter {
            input: Box::new(outer_scan()),
            predicate: ScalarExpr::InSubquery {
                expr: Box::new(col("id", 0, DataType::Int32)),
                subplan: Box::new(sub_key_project()),
                negated: true,
                correlated: false,
                data_type: DataType::Int32,
            },
        };

        let result = SubqueryDecorrelation
            .apply(&plan)
            .expect("no error")
            .expect("rewrite");
        assert_eq!(result.schema().len(), 2);
        assert!(matches!(
            result,
            LogicalPlan::Join {
                join_type: LogicalJoinType::Anti,
                ..
            }
        ));
    }

    #[test]
    fn real_correlated_in_rewrites_to_semi_join() {
        let sub = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Filter {
                input: Box::new(sub_scan()),
                predicate: ScalarExpr::Binary {
                    op: BinaryOp::Eq,
                    left: Box::new(col("data", 1, DataType::Int32)),
                    right: Box::new(outer_col("val", 1, DataType::Int32)),
                    data_type: DataType::Bool,
                },
            }),
            exprs: vec![(col("key", 0, DataType::Int32), "key".into())],
            schema: Schema::new([Field::required("key", DataType::Int32)]).expect("schema ok"),
        };
        let plan = LogicalPlan::Filter {
            input: Box::new(outer_scan()),
            predicate: ScalarExpr::InSubquery {
                expr: Box::new(col("id", 0, DataType::Int32)),
                subplan: Box::new(sub),
                negated: false,
                correlated: true,
                data_type: DataType::Int32,
            },
        };

        let result = SubqueryDecorrelation
            .apply(&plan)
            .expect("no error")
            .expect("rewrite");
        let LogicalPlan::Join {
            right,
            join_type,
            condition,
            ..
        } = result
        else {
            panic!("correlated IN should become join");
        };
        assert_eq!(join_type, LogicalJoinType::Semi);
        assert!(matches!(right.as_ref(), LogicalPlan::Aggregate { .. }));
        let LogicalJoinCondition::On(predicate) = condition else {
            panic!("expected ON predicate");
        };
        let dump = predicate.to_string();
        assert!(
            dump.contains("val") && dump.contains("__in_subquery"),
            "correlated IN predicate should match both correlation key and projected value, got {dump}"
        );
    }

    #[test]
    fn real_correlated_not_in_rewrites_to_anti_join() {
        let sub = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Filter {
                input: Box::new(sub_scan()),
                predicate: ScalarExpr::Binary {
                    op: BinaryOp::Eq,
                    left: Box::new(col("data", 1, DataType::Int32)),
                    right: Box::new(outer_col("val", 1, DataType::Int32)),
                    data_type: DataType::Bool,
                },
            }),
            exprs: vec![(col("key", 0, DataType::Int32), "key".into())],
            schema: Schema::new([Field::required("key", DataType::Int32)]).expect("schema ok"),
        };
        let plan = LogicalPlan::Filter {
            input: Box::new(outer_scan()),
            predicate: ScalarExpr::InSubquery {
                expr: Box::new(col("id", 0, DataType::Int32)),
                subplan: Box::new(sub),
                negated: true,
                correlated: true,
                data_type: DataType::Int32,
            },
        };

        let result = SubqueryDecorrelation
            .apply(&plan)
            .expect("no error")
            .expect("rewrite");
        assert!(matches!(
            result,
            LogicalPlan::Join {
                join_type: LogicalJoinType::Anti,
                ..
            }
        ));
    }

    #[test]
    fn real_uncorrelated_scalar_subquery_rewrites_to_cross_join_filter() {
        let plan = LogicalPlan::Filter {
            input: Box::new(outer_scan()),
            predicate: ScalarExpr::Binary {
                op: BinaryOp::Gt,
                left: Box::new(col("id", 0, DataType::Int32)),
                right: Box::new(ScalarExpr::ScalarSubquery {
                    subplan: Box::new(sub_key_project()),
                    correlated: false,
                    data_type: DataType::Int32,
                }),
                data_type: DataType::Bool,
            },
        };

        let result = SubqueryDecorrelation
            .apply(&plan)
            .expect("no error")
            .expect("rewrite");
        let LogicalPlan::Project { input, schema, .. } = &result else {
            panic!("expected Project, got {result:?}");
        };
        assert_eq!(schema.len(), 2);
        assert!(
            matches!(
                input.as_ref(),
                LogicalPlan::Filter {
                    input,
                    ..
                } if matches!(
                    input.as_ref(),
                    LogicalPlan::Join {
                        join_type: LogicalJoinType::Cross,
                        ..
                    }
                )
            ),
            "scalar subquery should become Cross Join + Filter, got {input:?}"
        );
    }

    #[test]
    fn real_correlated_scalar_aggregate_rewrites_to_left_join_filter() {
        let sub_filter = LogicalPlan::Filter {
            input: Box::new(sub_scan()),
            predicate: ScalarExpr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(col("key", 0, DataType::Int32)),
                right: Box::new(outer_col("id", 0, DataType::Int32)),
                data_type: DataType::Bool,
            },
        };
        let agg_schema =
            Schema::new([Field::nullable("avg", DataType::Float64)]).expect("schema ok");
        let aggregate = LogicalPlan::Aggregate {
            input: Box::new(sub_filter),
            group_by: Vec::new(),
            aggregates: vec![LogicalAggregateExpr {
                func: AggregateFunc::Avg,
                arg: Some(col("data", 1, DataType::Int32)),
                direct_arg: None,
                order_by: None,
                distinct: false,
                output_name: "avg".to_owned(),
                data_type: DataType::Float64,
            }],
            schema: agg_schema.clone(),
        };
        let subquery = LogicalPlan::Project {
            input: Box::new(aggregate),
            exprs: vec![(col("avg", 0, DataType::Float64), "avg".to_owned())],
            schema: agg_schema,
        };
        let plan = LogicalPlan::Filter {
            input: Box::new(outer_scan()),
            predicate: ScalarExpr::Binary {
                op: BinaryOp::Lt,
                left: Box::new(col("val", 1, DataType::Int32)),
                right: Box::new(ScalarExpr::ScalarSubquery {
                    subplan: Box::new(subquery),
                    correlated: true,
                    data_type: DataType::Float64,
                }),
                data_type: DataType::Bool,
            },
        };

        let result = SubqueryDecorrelation
            .apply(&plan)
            .expect("no error")
            .expect("rewrite");
        let LogicalPlan::Project { input, schema, .. } = &result else {
            panic!("expected Project, got {result:?}");
        };
        assert_eq!(schema.len(), 2);
        assert!(
            matches!(
                input.as_ref(),
                LogicalPlan::Filter {
                    input,
                    ..
                } if matches!(
                    input.as_ref(),
                    LogicalPlan::Join {
                        join_type: LogicalJoinType::LeftOuter,
                        condition: LogicalJoinCondition::On(_),
                        ..
                    }
                )
            ),
            "correlated scalar aggregate should become LeftOuter Join + Filter, got {input:?}"
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
