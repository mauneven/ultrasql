//! EXISTS / NOT EXISTS and IN / NOT IN subquery decorrelation.
//!
//! Lowers correlated `EXISTS` to Semi/Anti joins (projecting inner residual
//! columns when needed) and the various `IN`/`NOT IN` shapes — uncorrelated and
//! correlated, negated and not — into Semi/Anti joins and NULL-presence anti
//! probes that preserve SQL three-valued logic.

use ultrasql_core::{DataType, Field, Schema};
use ultrasql_planner::{BinaryOp, LogicalJoinCondition, LogicalJoinType, LogicalPlan, ScalarExpr};

use super::correlation::{
    CorrPair,
    build_correlation_condition,
    build_correlation_condition_against_right_schema,
    collect_join_right_column_indices,
    corr_fields,
    distinct_correlation_keys,
    extract_correlated_exists_input,
    extract_correlated_scalar_input,
    push_unique_index,
    rebase_projected_exists_residual,
};
use super::helpers::{
    anti_join,
    conjuncts_to_and,
    distinct_single_column,
    filter_column_null,
    filter_with_conjuncts,
    split_and,
};

pub(crate) fn rewrite_exists_filter_expr(outer: &LogicalPlan, predicate: &ScalarExpr) -> Option<LogicalPlan> {
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
        let join_condition = build_correlation_condition(&exists_input.corr_pairs, outer_width)?;
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
    )?];
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

pub(crate) fn project_exists_right_for_residual(
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

pub(crate) fn rewrite_in_subquery_filter_expr(
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
    if *negated {
        return rewrite_uncorrelated_not_in_subquery(&outer, expr, *subplan.clone(), data_type);
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

pub(crate) fn rewrite_uncorrelated_not_in_subquery(
    outer: &LogicalPlan,
    outer_expr: &ScalarExpr,
    subplan: LogicalPlan,
    data_type: &DataType,
) -> Option<LogicalPlan> {
    if subplan.schema().len() != 1 {
        return None;
    }

    let non_null_values = distinct_single_column(filter_column_null(subplan.clone(), 0, true)?)?;
    let null_probe = filter_column_null(subplan, 0, false)?;
    let outer_width = outer.schema().len();
    let right_col = ScalarExpr::Column {
        name: non_null_values.schema().field_at(0).name.clone(),
        index: outer_width,
        data_type: data_type.clone(),
    };
    let value_miss = anti_join(
        outer.clone(),
        non_null_values.clone(),
        LogicalJoinCondition::On(ScalarExpr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(outer_expr.clone()),
            right: Box::new(right_col),
            data_type: DataType::Bool,
        }),
        outer.schema(),
    );
    let left_not_null_when_subquery_nonempty = anti_join(
        value_miss,
        non_null_values,
        LogicalJoinCondition::On(ScalarExpr::IsNull {
            expr: Box::new(outer_expr.clone()),
            negated: false,
        }),
        outer.schema(),
    );
    Some(anti_join(
        left_not_null_when_subquery_nonempty,
        null_probe,
        LogicalJoinCondition::None,
        outer.schema(),
    ))
}

pub(crate) fn rewrite_correlated_in_subquery(
    outer: &LogicalPlan,
    outer_expr: &ScalarExpr,
    subplan: LogicalPlan,
    negated: bool,
    data_type: &DataType,
) -> Option<LogicalPlan> {
    if negated {
        return rewrite_correlated_not_in_subquery(outer, outer_expr, subplan, data_type);
    }
    let (right, corr_pairs, value_index, value_name) =
        build_correlated_in_right(subplan, data_type)?;
    let outer_width = outer.schema().len();
    let mut predicates = vec![build_correlation_condition_against_right_schema(
        &corr_pairs,
        outer_width,
    )?];
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
        join_type: LogicalJoinType::Semi,
        condition: LogicalJoinCondition::On(conjuncts_to_and(predicates)),
        schema: outer.schema().clone(),
    })
}

pub(crate) fn rewrite_correlated_not_in_subquery(
    outer: &LogicalPlan,
    outer_expr: &ScalarExpr,
    subplan: LogicalPlan,
    data_type: &DataType,
) -> Option<LogicalPlan> {
    let (right, corr_pairs, value_index, value_name) =
        build_correlated_in_right(subplan, data_type)?;
    let non_null_values = filter_column_null(right.clone(), value_index, true)?;
    let null_values = filter_column_null(right, value_index, false)?;
    let outer_width = outer.schema().len();

    let mut value_predicates = vec![build_correlation_condition_against_right_schema(
        &corr_pairs,
        outer_width,
    )?];
    value_predicates.push(ScalarExpr::Binary {
        op: BinaryOp::Eq,
        left: Box::new(outer_expr.clone()),
        right: Box::new(ScalarExpr::Column {
            name: value_name,
            index: outer_width + value_index,
            data_type: data_type.clone(),
        }),
        data_type: DataType::Bool,
    });
    let value_miss = anti_join(
        outer.clone(),
        non_null_values.clone(),
        LogicalJoinCondition::On(conjuncts_to_and(value_predicates)),
        outer.schema(),
    );

    let mut left_null_predicates = vec![build_correlation_condition_against_right_schema(
        &corr_pairs,
        outer_width,
    )?];
    left_null_predicates.push(ScalarExpr::IsNull {
        expr: Box::new(outer_expr.clone()),
        negated: false,
    });
    let left_not_null_when_group_nonempty = anti_join(
        value_miss,
        non_null_values,
        LogicalJoinCondition::On(conjuncts_to_and(left_null_predicates)),
        outer.schema(),
    );

    Some(anti_join(
        left_not_null_when_group_nonempty,
        null_values,
        LogicalJoinCondition::On(build_correlation_condition_against_right_schema(
            &corr_pairs,
            outer_width,
        )?),
        outer.schema(),
    ))
}

pub(crate) fn build_correlated_in_right(
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

pub(crate) fn strip_exists_projection(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Project { input, .. } => *input,
        other => other,
    }
}
