//! Scalar-subquery decorrelation.
//!
//! Lowers correlated and uncorrelated `ScalarSubquery` nodes appearing in
//! `Filter` predicates and `Project` expressions into LEFT OUTER / CROSS joins,
//! including the grouped-aggregate and non-aggregated single-row shapes and the
//! `COUNT`-specific `COALESCE(col, 0)` empty-set fix-up.

use ultrasql_core::{Field, Schema, Value};
use ultrasql_planner::{
    AggregateFunc, LogicalAggregateExpr, LogicalJoinCondition, LogicalJoinType, LogicalPlan,
    ScalarExpr,
};

use super::correlation::{
    CorrPair, CorrelatedExistsInput, build_correlation_condition,
    build_correlation_condition_against_right_schema, collect_join_right_column_indices,
    corr_fields, extract_correlated_exists_input, extract_correlated_scalar_input,
    push_unique_index, rebase_projected_exists_residual,
};
use super::helpers::{
    alias_first_column, concat_schemas, conjuncts_to_and, filter_with_conjuncts,
    find_first_correlated_scalar_subquery, project_left, replace_first_correlated_scalar_subquery,
    replace_first_uncorrelated_scalar_subquery, shift_column_indices_by,
    split_outer_only_conjuncts,
};

#[derive(Debug)]
pub(crate) struct CorrelatedScalarRight {
    plan: LogicalPlan,
    corr_pairs: Vec<CorrPair>,
    scalar_index: usize,
    scalar_name: String,
    /// `true` when the decorrelated scalar is a `COUNT(*)`/`COUNT(x)` whose
    /// empty-input value is `0` rather than NULL. After the LEFT OUTER JOIN the
    /// substituted column is NULL for outer keys with no inner match, so the
    /// caller must wrap the replacement in `COALESCE(col, 0)` to restore the
    /// SQL-correct count of `0`. `SUM`/`MIN`/`MAX`/`AVG` legitimately yield NULL
    /// on empty input and leave this `false`.
    empty_set_is_zero: bool,
}

pub(crate) fn rewrite_scalar_subquery_filter(
    outer: &LogicalPlan,
    predicate: &ScalarExpr,
) -> Option<LogicalPlan> {
    if let Some((subplan, data_type)) = find_first_correlated_scalar_subquery(predicate) {
        let right = build_correlated_scalar_aggregate_right(*subplan)?;
        let outer_width = outer.schema().len();
        let joined_column = ScalarExpr::Column {
            name: right.scalar_name.clone(),
            index: outer_width + right.scalar_index,
            data_type,
        };
        // A correlated COUNT has empty-set value 0, but the LEFT OUTER JOIN
        // leaves the joined column NULL for outer keys with no inner match;
        // restore the SQL-correct 0 via COALESCE. SUM/MIN/MAX/AVG keep NULL.
        let replacement = if right.empty_set_is_zero {
            coalesce_count_with_zero(joined_column)
        } else {
            joined_column
        };
        let rewritten_predicate = replace_first_correlated_scalar_subquery(predicate, replacement)?;
        let (outer_predicates, post_join_predicates) =
            split_outer_only_conjuncts(&rewritten_predicate, outer_width);
        let outer_input = filter_with_conjuncts(outer.clone(), outer_predicates);
        let join_condition = build_correlation_condition(&right.corr_pairs, outer_width)?;
        let join_schema = concat_schemas(outer_input.schema(), right.plan.schema())?;
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
    let join_schema = concat_schemas(outer.schema(), right.schema())?;
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

pub(crate) fn rewrite_project_with_scalar_subquery(
    input: &LogicalPlan,
    exprs: &[(ScalarExpr, String)],
    schema: &Schema,
) -> Option<LogicalPlan> {
    let outer_width = input.schema().len();
    for (expr_idx, (expr, _alias)) in exprs.iter().enumerate() {
        if let Some((subplan, data_type)) = find_first_correlated_scalar_subquery(expr) {
            // Prefer the aggregate shape: a single grouped aggregate yields at
            // most one row per correlated key, so a LEFT OUTER JOIN is
            // cardinality-safe. This also covers correlated `COUNT(*)`/`COUNT(x)`
            // projections, which must report 0 (not NULL) for outer keys with no
            // inner match — see `coalesce_count_with_zero`.
            if let Some(right) = build_correlated_scalar_aggregate_right((*subplan).clone()) {
                let join_condition = build_correlation_condition(&right.corr_pairs, outer_width)?;
                let join_schema = concat_schemas(input.schema(), right.plan.schema())?;
                let join = LogicalPlan::Join {
                    left: Box::new(input.clone()),
                    right: Box::new(right.plan),
                    join_type: LogicalJoinType::LeftOuter,
                    condition: LogicalJoinCondition::On(join_condition),
                    schema: join_schema,
                };
                let joined_column = ScalarExpr::Column {
                    name: right.scalar_name,
                    index: outer_width + right.scalar_index,
                    data_type,
                };
                let replacement = if right.empty_set_is_zero {
                    coalesce_count_with_zero(joined_column)
                } else {
                    joined_column
                };
                let mut new_exprs = exprs.to_vec();
                new_exprs[expr_idx].0 =
                    replace_first_correlated_scalar_subquery(expr, replacement)?;
                return Some(LogicalPlan::Project {
                    input: Box::new(join),
                    exprs: new_exprs,
                    schema: schema.clone(),
                });
            }

            // Non-aggregated correlated scalar subquery (e.g.
            // `(SELECT o.amount FROM orders o WHERE o.uid = u.id)`). Decorrelate
            // via a LEFT OUTER JOIN against the projected inner rows. KNOWN
            // LIMITATION: if such a subquery matches more than one inner row per
            // outer key, SQL requires raising "more than one row returned by a
            // subquery used as an expression", but this rewrite instead
            // duplicates the outer row. This engine has no runtime single-row
            // assertion operator to enforce that here, so the multi-row case is
            // a pre-existing limitation; the common single-row case (the inner
            // key is unique — e.g. catalog probes like psql's `\du`) is handled
            // correctly and must keep working. The Filter-predicate path is
            // unaffected: it routes un-aggregated correlated scalars through an
            // aggregate/DISTINCT guard.
            if let Some(right) = build_correlated_scalar_project_right(*subplan, outer_width) {
                let mut predicates = vec![build_correlation_condition_against_right_schema(
                    &right.corr_pairs,
                    outer_width,
                )?];
                predicates.extend(right.residual_predicates);
                let join_schema = concat_schemas(input.schema(), right.plan.schema())?;
                let join = LogicalPlan::Join {
                    left: Box::new(input.clone()),
                    right: Box::new(right.plan),
                    join_type: LogicalJoinType::LeftOuter,
                    condition: LogicalJoinCondition::On(conjuncts_to_and(predicates)),
                    schema: join_schema,
                };
                let replacement = ScalarExpr::Column {
                    name: right.scalar_name,
                    index: outer_width + right.scalar_index,
                    data_type,
                };
                let mut new_exprs = exprs.to_vec();
                new_exprs[expr_idx].0 =
                    replace_first_correlated_scalar_subquery(expr, replacement)?;
                return Some(LogicalPlan::Project {
                    input: Box::new(join),
                    exprs: new_exprs,
                    schema: schema.clone(),
                });
            }
            return None;
        }

        if let Some((new_expr, subplan)) = replace_first_uncorrelated_scalar_subquery(
            expr,
            outer_width,
            "__scalar_subquery".to_owned(),
        ) {
            let right = alias_first_column(*subplan, "__scalar_subquery")?;
            let join_schema = concat_schemas(input.schema(), right.schema())?;
            let join = LogicalPlan::Join {
                left: Box::new(input.clone()),
                right: Box::new(right),
                join_type: LogicalJoinType::Cross,
                condition: LogicalJoinCondition::None,
                schema: join_schema,
            };
            let mut new_exprs = exprs.to_vec();
            new_exprs[expr_idx].0 = new_expr;
            return Some(LogicalPlan::Project {
                input: Box::new(join),
                exprs: new_exprs,
                schema: schema.clone(),
            });
        }
    }
    None
}

/// Projected right side for a non-aggregated correlated scalar subquery: the
/// inner plan reduced to the correlation key columns plus the scalar value,
/// ready to LEFT OUTER JOIN against the outer rows on the correlation keys.
pub(crate) struct CorrelatedScalarProjectRight {
    plan: LogicalPlan,
    corr_pairs: Vec<CorrPair>,
    residual_predicates: Vec<ScalarExpr>,
    scalar_index: usize,
    scalar_name: String,
}

pub(crate) fn build_correlated_scalar_project_right(
    plan: LogicalPlan,
    outer_width: usize,
) -> Option<CorrelatedScalarProjectRight> {
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
    let CorrelatedExistsInput {
        clean_subplan,
        corr_pairs,
        residual_predicates,
    } = extract_correlated_exists_input(*input, outer_width)?;
    if corr_pairs.is_empty() {
        return None;
    }

    let mut needed = Vec::new();
    for pair in &corr_pairs {
        push_unique_index(&mut needed, pair.inner_index);
    }
    for predicate in &residual_predicates {
        collect_join_right_column_indices(predicate, outer_width, &mut needed);
    }

    let input_schema = clean_subplan.schema().clone();
    let mut fields = Vec::with_capacity(needed.len() + 1);
    let mut project_exprs = Vec::with_capacity(needed.len() + 1);
    for &idx in &needed {
        let field = input_schema.fields().get(idx)?;
        fields.push(field.clone());
        project_exprs.push((
            ScalarExpr::Column {
                name: field.name.clone(),
                index: idx,
                data_type: field.data_type.clone(),
            },
            field.name.clone(),
        ));
    }

    let scalar_name = "__scalar_subquery".to_owned();
    let scalar_index = needed.len();
    fields.push(Field::nullable(
        scalar_name.clone(),
        schema.field_at(0).data_type.clone(),
    ));
    project_exprs.push((exprs[0].0.clone(), scalar_name.clone()));
    let project_schema = Schema::new(fields).ok()?;
    let projected = LogicalPlan::Project {
        input: Box::new(clean_subplan),
        exprs: project_exprs,
        schema: project_schema,
    };

    let mut projected_pairs = Vec::with_capacity(corr_pairs.len());
    for pair in &corr_pairs {
        let projected_idx = needed.iter().position(|&idx| idx == pair.inner_index)?;
        let mut projected_pair = pair.clone();
        projected_pair.inner_index = projected_idx;
        projected_pairs.push(projected_pair);
    }
    let projected_residuals = residual_predicates
        .iter()
        .map(|predicate| rebase_projected_exists_residual(predicate, outer_width, &needed))
        .collect::<Option<Vec<_>>>()?;

    Some(CorrelatedScalarProjectRight {
        plan: projected,
        corr_pairs: projected_pairs,
        residual_predicates: projected_residuals,
        scalar_index,
        scalar_name,
    })
}

pub(crate) fn build_correlated_scalar_aggregate_right(
    plan: LogicalPlan,
) -> Option<CorrelatedScalarRight> {
    match plan {
        LogicalPlan::Project {
            input,
            exprs,
            schema,
        } => {
            if schema.len() != 1 || exprs.len() != 1 {
                return None;
            }
            // Capture the inner aggregate before it is moved so we can tell
            // whether the projected scalar passes a COUNT through unchanged.
            // A bare passthrough (`Column { index: 0 }`) of a single
            // `COUNT(*)`/`COUNT(x)` has empty-set value 0; any wrapping
            // expression (e.g. `COUNT(*) + 5`) does not, so we only flag the
            // bare-passthrough shape.
            let inner_aggregate_funcs = aggregate_funcs_of(&input);
            let scalar_is_bare_count_passthrough =
                matches!(&exprs[0].0, ScalarExpr::Column { index: 0, .. })
                    && inner_aggregate_funcs.is_some_and(aggregate_empty_set_is_zero);

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
                empty_set_is_zero: scalar_is_bare_count_passthrough,
            })
        }
        LogicalPlan::Aggregate { .. } => {
            // The scalar is the single aggregate output directly, so its
            // empty-set value is 0 exactly when that aggregate is a COUNT.
            let empty_set_is_zero =
                aggregate_funcs_of(&plan).is_some_and(aggregate_empty_set_is_zero);
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
                empty_set_is_zero,
            })
        }
        _ => None,
    }
}

/// Borrow the aggregate calls of an `Aggregate` node, or `None` for any other
/// shape. Used to inspect a correlated scalar subquery's aggregate before the
/// node is consumed by [`build_grouped_correlated_aggregate`].
pub(crate) fn aggregate_funcs_of(plan: &LogicalPlan) -> Option<&[LogicalAggregateExpr]> {
    match plan {
        LogicalPlan::Aggregate { aggregates, .. } => Some(aggregates),
        _ => None,
    }
}

/// Whether a scalar aggregate's value over an empty input is `0` rather than
/// NULL. Only a single `COUNT(*)`/`COUNT(x)` qualifies; `SUM`/`MIN`/`MAX`/`AVG`
/// (and any other aggregate) yield NULL on empty input and must NOT be COALESCEd.
pub(crate) fn aggregate_empty_set_is_zero(aggregates: &[LogicalAggregateExpr]) -> bool {
    matches!(
        aggregates,
        [LogicalAggregateExpr {
            func: AggregateFunc::CountStar | AggregateFunc::Count,
            ..
        }]
    )
}

/// Wrap a decorrelated COUNT scalar column in `COALESCE(col, 0)` so that outer
/// keys with no matching inner rows report `0` (the SQL count of the empty set)
/// instead of the NULL produced by the LEFT OUTER JOIN. COUNT results are always
/// `Int64` in this engine, so the literal `0` is built as `Int64`.
pub(crate) fn coalesce_count_with_zero(column: ScalarExpr) -> ScalarExpr {
    let data_type = column.data_type();
    ScalarExpr::FunctionCall {
        name: "coalesce".to_owned(),
        args: vec![
            column,
            ScalarExpr::Literal {
                value: Value::Int64(0),
                data_type: data_type.clone(),
            },
        ],
        data_type,
    }
}

pub(crate) fn build_grouped_correlated_aggregate(
    plan: LogicalPlan,
) -> Option<(LogicalPlan, Vec<CorrPair>)> {
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
