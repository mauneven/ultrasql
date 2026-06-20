//! Generic plan, expression, and schema utilities.
//!
//! Provider-agnostic building blocks shared across the decorrelation paths:
//! conjunct splitting/joining, column-reference collection, scalar-subquery
//! location/replacement, single-column DISTINCT and NULL-filter constructors,
//! anti-join and projection builders, and schema concatenation.

use ultrasql_core::{DataType, Field, Schema};
use ultrasql_planner::{BinaryOp, LogicalJoinCondition, LogicalJoinType, LogicalPlan, ScalarExpr};

pub(crate) fn split_outer_only_conjuncts(
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

pub(crate) fn collect_column_refs(expr: &ScalarExpr, refs: &mut Vec<usize>) {
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

pub(crate) fn expr_contains_subquery(expr: &ScalarExpr) -> bool {
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

pub(crate) fn distinct_single_column(input: LogicalPlan) -> Option<LogicalPlan> {
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

pub(crate) fn filter_column_null(
    input: LogicalPlan,
    column_index: usize,
    negated: bool,
) -> Option<LogicalPlan> {
    let field = input.schema().fields().get(column_index)?.clone();
    Some(LogicalPlan::Filter {
        input: Box::new(input),
        predicate: ScalarExpr::IsNull {
            expr: Box::new(ScalarExpr::Column {
                name: field.name,
                index: column_index,
                data_type: field.data_type,
            }),
            negated,
        },
    })
}

pub(crate) fn anti_join(
    left: LogicalPlan,
    right: LogicalPlan,
    condition: LogicalJoinCondition,
    schema: &Schema,
) -> LogicalPlan {
    LogicalPlan::Join {
        left: Box::new(left),
        right: Box::new(right),
        join_type: LogicalJoinType::Anti,
        condition,
        schema: schema.clone(),
    }
}

pub(crate) fn alias_first_column(input: LogicalPlan, name: &str) -> Option<LogicalPlan> {
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

pub(crate) fn filter_with_conjuncts(input: LogicalPlan, conjuncts: Vec<ScalarExpr>) -> LogicalPlan {
    if conjuncts.is_empty() {
        input
    } else {
        LogicalPlan::Filter {
            input: Box::new(input),
            predicate: conjuncts_to_and(conjuncts),
        }
    }
}

pub(crate) fn project_left(input: LogicalPlan, schema: &Schema) -> LogicalPlan {
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

pub(crate) fn replace_first_uncorrelated_scalar_subquery(
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

pub(crate) fn find_first_correlated_scalar_subquery(
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

pub(crate) fn replace_first_correlated_scalar_subquery(
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

pub(crate) fn shift_column_indices_by(expr: &ScalarExpr, offset: usize) -> ScalarExpr {
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

pub(crate) fn split_and(expr: &ScalarExpr) -> Vec<ScalarExpr> {
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

pub(crate) fn conjuncts_to_and(mut predicates: Vec<ScalarExpr>) -> ScalarExpr {
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

/// Concatenate two schemas into one.
pub(crate) fn concat_schemas(left: &Schema, right: &Schema) -> Option<Schema> {
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
    Schema::new(fields).ok()
}
