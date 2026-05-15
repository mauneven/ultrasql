//! Aggregate detection, classification, and binding helpers.
//! Split out of `binder/mod.rs` to keep each file under the 600-line ceiling.

use ultrasql_core::{DataType, Field, Schema};
use ultrasql_parser::ast::{Expr, SelectItem};

use super::{
    AggregateFunc, Catalog, LogicalAggregateExpr, LogicalPlan, PlanError, ScalarExpr, ScopeEntry,
    ScopeStack, bind_expr, derive_output_name,
};

pub(super) fn projection_item_has_aggregate(item: &SelectItem) -> bool {
    match item {
        SelectItem::Expr { expr, .. } => expr_has_aggregate(expr),
        SelectItem::Wildcard { .. } | SelectItem::QualifiedWildcard { .. } => false,
    }
}

pub(super) fn expr_has_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::Call { name, .. } => {
            is_aggregate_name(name.parts.last().map_or("", |p| p.value.as_str()))
        }
        Expr::Unary { expr: inner, .. }
        | Expr::Paren { expr: inner, .. }
        | Expr::IsNull { expr: inner, .. } => expr_has_aggregate(inner),
        Expr::Binary { left, right, .. } => expr_has_aggregate(left) || expr_has_aggregate(right),
        _ => false,
    }
}

#[inline]
pub(super) fn is_aggregate_name(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "count"
            | "sum"
            | "avg"
            | "min"
            | "max"
            | "bool_and"
            | "bool_or"
            | "string_agg"
            | "array_agg"
    )
}

fn classify_aggregate(name: &str, args_empty: bool) -> Option<AggregateFunc> {
    match name.to_ascii_lowercase().as_str() {
        "count" if args_empty => Some(AggregateFunc::CountStar),
        "count" => Some(AggregateFunc::Count),
        "sum" => Some(AggregateFunc::Sum),
        "avg" => Some(AggregateFunc::Avg),
        "min" => Some(AggregateFunc::Min),
        "max" => Some(AggregateFunc::Max),
        "bool_and" => Some(AggregateFunc::BoolAnd),
        "bool_or" => Some(AggregateFunc::BoolOr),
        "string_agg" => Some(AggregateFunc::StringAgg),
        "array_agg" => Some(AggregateFunc::ArrayAgg),
        _ => None,
    }
}

fn aggregate_return_type(func: AggregateFunc, arg_type: DataType) -> DataType {
    match func {
        AggregateFunc::CountStar | AggregateFunc::Count => DataType::Int64,
        AggregateFunc::Sum => match arg_type {
            DataType::Int16 | DataType::Int32 | DataType::Int64 => DataType::Int64,
            DataType::Float32 | DataType::Float64 => DataType::Float64,
            other if other.is_numeric() => other,
            _ => DataType::Null,
        },
        AggregateFunc::Avg => {
            if arg_type.is_numeric() {
                DataType::Float64
            } else {
                DataType::Null
            }
        }
        AggregateFunc::Min | AggregateFunc::Max => arg_type,
        AggregateFunc::BoolAnd | AggregateFunc::BoolOr => DataType::Bool,
        AggregateFunc::StringAgg => DataType::Text { max_len: None },
        AggregateFunc::ArrayAgg => DataType::Array(Box::new(arg_type)),
    }
}

pub(super) fn bind_aggregate(
    input: LogicalPlan,
    select: &ultrasql_parser::ast::SelectStmt,
    _from_scope: &[ScopeEntry],
    catalog: &dyn Catalog,
    scope: &mut ScopeStack,
) -> Result<LogicalPlan, PlanError> {
    let input_schema = input.schema().clone();

    let mut group_by: Vec<ScalarExpr> = Vec::with_capacity(select.group_by.len());
    for e in &select.group_by {
        group_by.push(bind_expr(e, &input_schema, catalog, scope)?);
    }

    let mut aggregates: Vec<LogicalAggregateExpr> = Vec::new();
    for item in &select.projection {
        if let SelectItem::Expr { expr, alias, .. } = item {
            collect_aggregates(
                expr,
                alias.as_ref(),
                &input_schema,
                &mut aggregates,
                catalog,
                scope,
            )?;
        }
    }
    if let Some(having) = &select.having {
        collect_aggregates(having, None, &input_schema, &mut aggregates, catalog, scope)?;
    }

    let mut out_fields: Vec<Field> = Vec::new();
    for (i, g) in group_by.iter().enumerate() {
        let name = match g {
            ScalarExpr::Column { name, .. } => name.clone(),
            _ => format!("group{i}"),
        };
        out_fields.push(Field::nullable(name, g.data_type()));
    }
    for agg in &aggregates {
        out_fields.push(Field::nullable(
            agg.output_name.clone(),
            agg.data_type.clone(),
        ));
    }
    let agg_schema = build_unique_schema(out_fields)?;

    Ok(LogicalPlan::Aggregate {
        input: Box::new(input),
        group_by,
        aggregates,
        schema: agg_schema,
    })
}

fn build_unique_schema(mut fields: Vec<Field>) -> Result<Schema, PlanError> {
    let mut seen: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    for f in &mut fields {
        let lower = f.name.to_ascii_lowercase();
        let count = seen.entry(lower).or_insert(0);
        if *count > 0 {
            f.name = format!("{}_{}", f.name, *count);
        }
        *count += 1;
    }
    Schema::new(fields).map_err(|e| PlanError::TypeMismatch(format!("aggregate schema: {e}")))
}

fn collect_aggregates(
    expr: &Expr,
    alias: Option<&ultrasql_parser::ast::Identifier>,
    input_schema: &Schema,
    out: &mut Vec<LogicalAggregateExpr>,
    catalog: &dyn Catalog,
    scope: &mut ScopeStack,
) -> Result<(), PlanError> {
    match expr {
        Expr::Call {
            name,
            args,
            distinct,
            ..
        } => {
            let func_name = name
                .parts
                .last()
                .map_or("", |p| p.value.as_str())
                .to_ascii_lowercase();
            let is_star_arg = args.len() == 1
                && matches!(&args[0], Expr::Column { name: n }
                    if n.parts.len() == 1 && n.parts[0].value == "*");
            let args_empty_or_star = args.is_empty() || is_star_arg;
            if let Some(func) = classify_aggregate(&func_name, args_empty_or_star) {
                let (arg_expr, arg_ty) = if args_empty_or_star {
                    (None, DataType::Null)
                } else {
                    let bound = bind_expr(&args[0], input_schema, catalog, scope)?;
                    let ty = bound.data_type();
                    (Some(bound), ty)
                };
                let ret_ty = aggregate_return_type(func, arg_ty);
                let output_name = alias.map_or_else(
                    || derive_agg_output_name(&func_name, args),
                    |a| a.value.clone(),
                );
                let already = out.iter().any(|a| {
                    a.output_name == output_name
                        && std::mem::discriminant(&a.func) == std::mem::discriminant(&func)
                });
                if !already {
                    out.push(LogicalAggregateExpr {
                        func,
                        arg: arg_expr,
                        distinct: *distinct,
                        output_name,
                        data_type: ret_ty,
                    });
                }
                Ok(())
            } else {
                Err(PlanError::NotSupported(
                    "non-aggregate function calls in aggregation context",
                ))
            }
        }
        Expr::Paren { expr: inner, .. } | Expr::Unary { expr: inner, .. } => {
            collect_aggregates(inner, alias, input_schema, out, catalog, scope)
        }
        Expr::Binary { left, right, .. } => {
            collect_aggregates(left, None, input_schema, out, catalog, scope)?;
            collect_aggregates(right, None, input_schema, out, catalog, scope)
        }
        _ => Ok(()),
    }
}

pub(super) fn derive_agg_output_name(func_name: &str, _args: &[Expr]) -> String {
    func_name.to_string()
}

pub(super) fn bind_projection_agg(
    items: &[SelectItem],
    agg_schema: &Schema,
    catalog: &dyn Catalog,
    scope: &mut ScopeStack,
) -> Result<Vec<(ScalarExpr, String)>, PlanError> {
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        match item {
            SelectItem::Wildcard { .. } | SelectItem::QualifiedWildcard { .. } => {
                for (i, f) in agg_schema.fields().iter().enumerate() {
                    out.push((
                        ScalarExpr::Column {
                            name: f.name.clone(),
                            index: i,
                            data_type: f.data_type.clone(),
                        },
                        f.name.clone(),
                    ));
                }
            }
            SelectItem::Expr { expr, alias, .. } => {
                let bound = bind_expr_or_agg_ref(expr, agg_schema, catalog, scope)?;
                let name = alias
                    .as_ref()
                    .map_or_else(|| derive_output_name(expr, &bound), |a| a.value.clone());
                out.push((bound, name));
            }
        }
    }
    Ok(out)
}

fn bind_expr_or_agg_ref(
    expr: &Expr,
    agg_schema: &Schema,
    catalog: &dyn Catalog,
    scope: &mut ScopeStack,
) -> Result<ScalarExpr, PlanError> {
    match expr {
        Expr::Call { name, args, .. } => {
            let func_name = name
                .parts
                .last()
                .map_or("", |p| p.value.as_str())
                .to_ascii_lowercase();
            if is_aggregate_name(&func_name) {
                let agg_name = derive_agg_output_name(&func_name, args);
                if let Some((i, f)) = agg_schema.find(&agg_name) {
                    return Ok(ScalarExpr::Column {
                        name: f.name.clone(),
                        index: i,
                        data_type: f.data_type.clone(),
                    });
                }
            }
            bind_expr(expr, agg_schema, catalog, scope)
        }
        _ => bind_expr(expr, agg_schema, catalog, scope),
    }
}

pub(super) fn bind_projection_with_scope(
    items: &[SelectItem],
    input: &Schema,
    from_scope: &[ScopeEntry],
    catalog: &dyn Catalog,
    outer_scope: &mut ScopeStack,
) -> Result<Vec<(ScalarExpr, String)>, PlanError> {
    let mut out = Vec::new();
    for item in items {
        match item {
            SelectItem::Wildcard { .. } => {
                if from_scope.is_empty() {
                    for (i, f) in input.fields().iter().enumerate() {
                        out.push((
                            ScalarExpr::Column {
                                name: f.name.clone(),
                                index: i,
                                data_type: f.data_type.clone(),
                            },
                            f.name.clone(),
                        ));
                    }
                } else {
                    for entry in from_scope {
                        out.push((
                            ScalarExpr::Column {
                                name: entry.field.name.clone(),
                                index: entry.field_index,
                                data_type: entry.field.data_type.clone(),
                            },
                            entry.field.name.clone(),
                        ));
                    }
                }
            }
            SelectItem::QualifiedWildcard { qualifier, .. } => {
                let q = &qualifier.value;
                let matching: Vec<_> = from_scope
                    .iter()
                    .filter(|e| e.qualifier.eq_ignore_ascii_case(q))
                    .collect();
                if matching.is_empty() {
                    return Err(PlanError::TableNotFound(q.clone()));
                }
                for entry in matching {
                    out.push((
                        ScalarExpr::Column {
                            name: entry.field.name.clone(),
                            index: entry.field_index,
                            data_type: entry.field.data_type.clone(),
                        },
                        entry.field.name.clone(),
                    ));
                }
            }
            SelectItem::Expr { expr, alias, .. } => {
                let bound = bind_expr(expr, input, catalog, outer_scope)?;
                let name = alias
                    .as_ref()
                    .map_or_else(|| derive_output_name(expr, &bound), |a| a.value.clone());
                out.push((bound, name));
            }
        }
    }
    Ok(out)
}
