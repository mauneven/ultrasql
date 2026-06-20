//! Correlation extraction and join-condition construction.
//!
//! Shared between the EXISTS/IN and scalar-subquery decorrelation paths: the
//! [`CorrPair`] equality descriptor, the cleaned-subplan extractors that pull
//! `inner = outer` correlations out of a `Filter`, and the helpers that rebuild
//! the corresponding join `ON` conditions against the decorrelated schemas.

use ultrasql_core::{DataType, Field, Schema};
use ultrasql_planner::{BinaryOp, LogicalPlan, ScalarExpr};

use super::helpers::{filter_with_conjuncts, split_and};

#[derive(Clone, Debug)]
pub(crate) struct CorrPair {
    pub(crate) inner_name: String,
    pub(crate) inner_index: usize,
    pub(crate) inner_type: DataType,
    pub(crate) outer_name: String,
    pub(crate) outer_index: usize,
    pub(crate) outer_type: DataType,
}

#[derive(Debug)]
pub(crate) struct CorrelatedExistsInput {
    pub(crate) clean_subplan: LogicalPlan,
    pub(crate) corr_pairs: Vec<CorrPair>,
    pub(crate) residual_predicates: Vec<ScalarExpr>,
}

pub(crate) fn extract_correlated_scalar_input(plan: LogicalPlan) -> Option<(LogicalPlan, Vec<CorrPair>)> {
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

pub(crate) fn corr_fields(pairs: &[CorrPair]) -> Vec<Field> {
    pairs
        .iter()
        .enumerate()
        .map(|(idx, pair)| Field::nullable(format!("__corr_{idx}"), pair.inner_type.clone()))
        .collect()
}

pub(crate) fn push_unique_index(indices: &mut Vec<usize>, index: usize) {
    if !indices.contains(&index) {
        indices.push(index);
    }
}

pub(crate) fn collect_join_right_column_indices(expr: &ScalarExpr, outer_width: usize, out: &mut Vec<usize>) {
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

pub(crate) fn rebase_projected_exists_residual(
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

pub(crate) fn extract_correlated_exists_input(
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

pub(crate) fn rebase_correlated_predicate(expr: &ScalarExpr, outer_width: usize) -> Option<ScalarExpr> {
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

pub(crate) fn parse_correlation_equality(expr: &ScalarExpr) -> Option<CorrPair> {
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

pub(crate) fn distinct_correlation_keys(input: LogicalPlan, pairs: &[CorrPair]) -> Option<LogicalPlan> {
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

pub(crate) fn build_correlation_condition(pairs: &[CorrPair], outer_width: usize) -> Option<ScalarExpr> {
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
    let first = predicates.next()?;
    Some(predicates.fold(first, |left, right| ScalarExpr::Binary {
        op: BinaryOp::And,
        left: Box::new(left),
        right: Box::new(right),
        data_type: DataType::Bool,
    }))
}

pub(crate) fn build_correlation_condition_against_right_schema(
    pairs: &[CorrPair],
    outer_width: usize,
) -> Option<ScalarExpr> {
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
    let first = predicates.next()?;
    Some(predicates.fold(first, |left, right| ScalarExpr::Binary {
        op: BinaryOp::And,
        left: Box::new(left),
        right: Box::new(right),
        data_type: DataType::Bool,
    }))
}
