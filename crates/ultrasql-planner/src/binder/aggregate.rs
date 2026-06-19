//! Aggregate detection, classification, and binding helpers.
//! Split out of `binder/mod.rs` to keep each file under the 600-line ceiling.

use ultrasql_core::{DataType, Field, Schema};
use ultrasql_parser::ast::{Expr, NullsOrder, OrderItem, SelectItem, SortDirection};

use super::expr_bind::{
    bind_collated_expr, builtin_return_type, coerce_args_to_common_type,
    coerce_common_builtin_args, coerce_literal_to_match, common_scalar_return_type,
    is_supported_builtin, validate_builtin_args,
};
use super::expr_type::{binary_result_type, comparable};
use super::util::{ordinal_index, plain_select_exprs, positional_ordinal};
use super::{
    AggregateFunc, Catalog, LogicalAggregateExpr, LogicalPlan, PlanError, ScalarExpr, ScopeEntry,
    ScopeStack, SortKey, bind_expr_with_ctes, derive_output_name, schema_for_qualified_binding,
};

struct BoundAggregateInput {
    arg: Option<ScalarExpr>,
    direct_arg: Option<ScalarExpr>,
    order_by: Option<SortKey>,
    arg_type: DataType,
}

pub(super) fn projection_item_has_aggregate(item: &SelectItem) -> bool {
    match item {
        SelectItem::Expr { expr, .. } => expr_has_aggregate(expr),
        SelectItem::Wildcard { .. } | SelectItem::QualifiedWildcard { .. } => false,
    }
}

pub(super) fn expr_has_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::Call {
            name,
            args,
            distinct,
            within_group,
            over,
            ..
        } => {
            let func_name = name.parts.last().map_or("", |p| p.value.as_str());
            let is_current_aggregate = is_aggregate_name(func_name)
                && !is_scalar_min_max_call(
                    func_name,
                    args.len(),
                    *distinct,
                    within_group.is_some(),
                    over.is_some(),
                );
            is_current_aggregate || args.iter().any(expr_has_aggregate)
        }
        Expr::Unary { expr: inner, .. }
        | Expr::Paren { expr: inner, .. }
        | Expr::IsNull { expr: inner, .. }
        | Expr::Collate { expr: inner, .. } => expr_has_aggregate(inner),
        Expr::Coalesce { args, .. } | Expr::Greatest { args, .. } | Expr::Least { args, .. } => {
            args.iter().any(expr_has_aggregate)
        }
        Expr::NullIf { a, b, .. } => expr_has_aggregate(a) || expr_has_aggregate(b),
        Expr::Binary { left, right, .. } => expr_has_aggregate(left) || expr_has_aggregate(right),
        _ => false,
    }
}

pub(super) fn is_scalar_min_max_call(
    func_name: &str,
    arg_count: usize,
    distinct: bool,
    has_within_group: bool,
    has_over: bool,
) -> bool {
    matches!(func_name.to_ascii_lowercase().as_str(), "min" | "max")
        && arg_count > 1
        && !distinct
        && !has_within_group
        && !has_over
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
            | "json_agg"
            | "stddev"
            | "stddev_samp"
            | "stddev_pop"
            | "variance"
            | "var_samp"
            | "var_pop"
            | "corr"
            | "percentile_cont"
            | "percentile_disc"
    )
}

pub(super) fn classify_aggregate(name: &str, args_empty: bool) -> Option<AggregateFunc> {
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
        "json_agg" => Some(AggregateFunc::JsonAgg),
        // PostgreSQL: STDDEV is an alias for STDDEV_SAMP, VARIANCE
        // for VAR_SAMP. Match that.
        "stddev" | "stddev_samp" => Some(AggregateFunc::StddevSamp),
        "stddev_pop" => Some(AggregateFunc::StddevPop),
        "variance" | "var_samp" => Some(AggregateFunc::VarSamp),
        "var_pop" => Some(AggregateFunc::VarPop),
        "corr" => Some(AggregateFunc::Corr),
        "percentile_cont" => Some(AggregateFunc::PercentileCont),
        "percentile_disc" => Some(AggregateFunc::PercentileDisc),
        _ => None,
    }
}

pub(super) fn aggregate_return_type(func: AggregateFunc, arg_type: DataType) -> DataType {
    match func {
        AggregateFunc::CountStar | AggregateFunc::Count => DataType::Int64,
        AggregateFunc::Sum => match arg_type {
            DataType::Int16 | DataType::Int32 | DataType::Int64 => DataType::Int64,
            DataType::Float32 | DataType::Float64 => DataType::Float64,
            DataType::Vector { .. } | DataType::HalfVec { .. } => arg_type,
            other if other.is_numeric() => other,
            _ => DataType::Null,
        },
        AggregateFunc::Avg => {
            if arg_type.is_numeric() {
                DataType::Float64
            } else if matches!(arg_type, DataType::Vector { .. } | DataType::HalfVec { .. }) {
                arg_type
            } else {
                DataType::Null
            }
        }
        AggregateFunc::Min | AggregateFunc::Max => arg_type,
        AggregateFunc::BoolAnd | AggregateFunc::BoolOr => DataType::Bool,
        AggregateFunc::StringAgg => DataType::Text { max_len: None },
        AggregateFunc::ArrayAgg => DataType::Array(Box::new(arg_type)),
        AggregateFunc::JsonAgg => DataType::Jsonb,
        // STDDEV / VARIANCE always return double precision.
        AggregateFunc::StddevSamp
        | AggregateFunc::StddevPop
        | AggregateFunc::VarSamp
        | AggregateFunc::VarPop
        | AggregateFunc::Corr
        | AggregateFunc::PercentileCont => DataType::Float64,
        AggregateFunc::PercentileDisc => arg_type,
    }
}

pub(super) fn bind_aggregate(
    input: LogicalPlan,
    select: &ultrasql_parser::ast::SelectStmt,
    from_scope: &[ScopeEntry],
    catalog: &dyn Catalog,
    cte_catalog: &[(String, Schema)],
    scope: &mut ScopeStack,
) -> Result<LogicalPlan, PlanError> {
    let input_schema = input.schema().clone();
    let binding_schema = schema_for_qualified_binding(&input_schema, from_scope)?;

    let mut group_by: Vec<ScalarExpr> = Vec::with_capacity(select.group_by.len());
    for e in &select.group_by {
        // A bare integer in GROUP BY is a 1-based positional reference to the
        // SELECT list (`GROUP BY 1` groups by the first output expression),
        // not a constant — binding it as a literal would collapse every row
        // into a single group.
        let group_expr = if let Some(n) = positional_ordinal(e) {
            let outputs = plain_select_exprs(&select.projection).ok_or(PlanError::NotSupported(
                "positional GROUP BY with a wildcard in the SELECT list",
            ))?;
            let target = outputs[ordinal_index(n, outputs.len(), "GROUP BY")?];
            if expr_has_aggregate(target) {
                return Err(PlanError::TypeMismatch(format!(
                    "GROUP BY position {n} refers to an aggregate expression, \
                     which is not allowed"
                )));
            }
            target
        } else {
            e
        };
        group_by.push(bind_expr_with_ctes(
            group_expr,
            &binding_schema,
            catalog,
            cte_catalog,
            scope,
        )?);
    }

    let mut aggregates: Vec<LogicalAggregateExpr> = Vec::new();
    for item in &select.projection {
        if let SelectItem::Expr { expr, alias, .. } = item {
            collect_aggregates(
                expr,
                alias.as_ref(),
                &binding_schema,
                &mut aggregates,
                catalog,
                cte_catalog,
                scope,
            )?;
        }
    }
    if let Some(having) = &select.having {
        collect_aggregates(
            having,
            None,
            &binding_schema,
            &mut aggregates,
            catalog,
            cte_catalog,
            scope,
        )?;
    }

    let group_aliases = group_projection_aliases(
        &group_by,
        select,
        &binding_schema,
        catalog,
        cte_catalog,
        scope,
    )?;

    let mut out_fields: Vec<Field> = Vec::new();
    for (i, g) in group_by.iter().enumerate() {
        let name = group_aliases[i].clone().unwrap_or_else(|| match g {
            ScalarExpr::Column { name, .. } => name.clone(),
            _ => format!("group{i}"),
        });
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

fn group_projection_aliases(
    group_by: &[ScalarExpr],
    select: &ultrasql_parser::ast::SelectStmt,
    binding_schema: &Schema,
    catalog: &dyn Catalog,
    cte_catalog: &[(String, Schema)],
    scope: &mut ScopeStack,
) -> Result<Vec<Option<String>>, PlanError> {
    let mut aliases = vec![None; group_by.len()];
    for item in &select.projection {
        let SelectItem::Expr {
            expr,
            alias: Some(alias),
            ..
        } = item
        else {
            continue;
        };
        if expr_has_aggregate(expr) {
            continue;
        }
        let bound = bind_expr_with_ctes(expr, binding_schema, catalog, cte_catalog, scope)?;
        for (idx, group_expr) in group_by.iter().enumerate() {
            if matches!(group_expr, ScalarExpr::Column { .. }) {
                continue;
            }
            if aliases[idx].is_none() && &bound == group_expr {
                aliases[idx] = Some(alias.value.clone());
            }
        }
    }
    Ok(aliases)
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
    cte_catalog: &[(String, Schema)],
    scope: &mut ScopeStack,
) -> Result<(), PlanError> {
    match expr {
        Expr::Call {
            name,
            args,
            distinct,
            within_group,
            over,
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
            let scalar_min_max = is_scalar_min_max_call(
                &func_name,
                args.len(),
                *distinct,
                within_group.is_some(),
                over.is_some(),
            );
            if scalar_min_max {
                return collect_non_aggregate_function_args(
                    &func_name,
                    args,
                    input_schema,
                    out,
                    catalog,
                    cte_catalog,
                    scope,
                );
            }
            if let Some(func) = classify_aggregate(&func_name, args_empty_or_star) {
                let bound = if matches!(
                    func,
                    AggregateFunc::PercentileCont | AggregateFunc::PercentileDisc
                ) {
                    bind_ordered_set_percentile(
                        args,
                        within_group.as_deref(),
                        *distinct,
                        input_schema,
                        catalog,
                        cte_catalog,
                        scope,
                    )?
                } else if within_group.is_some() {
                    return Err(PlanError::NotSupported(
                        "WITHIN GROUP is supported for percentile aggregates",
                    ));
                } else if args_empty_or_star {
                    BoundAggregateInput {
                        arg: None,
                        direct_arg: None,
                        order_by: None,
                        arg_type: DataType::Null,
                    }
                } else if func == AggregateFunc::StringAgg {
                    bind_string_agg(args, input_schema, catalog, cte_catalog, scope)?
                } else if func == AggregateFunc::Corr {
                    if args.len() != 2 {
                        return Err(PlanError::TypeMismatch(format!(
                            "corr: expected 2 arguments, got {}",
                            args.len()
                        )));
                    }
                    let y =
                        bind_expr_with_ctes(&args[0], input_schema, catalog, cte_catalog, scope)?;
                    let x =
                        bind_expr_with_ctes(&args[1], input_schema, catalog, cte_catalog, scope)?;
                    let row_type = DataType::Record(vec![
                        ("f1".to_owned(), y.data_type()),
                        ("f2".to_owned(), x.data_type()),
                    ]);
                    BoundAggregateInput {
                        arg: Some(ScalarExpr::FunctionCall {
                            name: "row".to_owned(),
                            args: vec![y, x],
                            data_type: row_type.clone(),
                        }),
                        direct_arg: None,
                        order_by: None,
                        arg_type: row_type,
                    }
                } else {
                    let bound =
                        bind_expr_with_ctes(&args[0], input_schema, catalog, cte_catalog, scope)?;
                    let ty = bound.data_type();
                    BoundAggregateInput {
                        arg: Some(bound),
                        direct_arg: None,
                        order_by: None,
                        arg_type: ty,
                    }
                };
                let ret_ty = aggregate_return_type(func, bound.arg_type.clone());
                let output_name = alias.map_or_else(
                    || derive_agg_output_name(&func_name, args, within_group.as_deref()),
                    |a| a.value.clone(),
                );
                let already = out.iter().any(|a| {
                    a.output_name == output_name
                        && std::mem::discriminant(&a.func) == std::mem::discriminant(&func)
                });
                if !already {
                    out.push(LogicalAggregateExpr {
                        func,
                        arg: bound.arg,
                        direct_arg: bound.direct_arg,
                        order_by: bound.order_by,
                        distinct: *distinct,
                        output_name,
                        data_type: ret_ty,
                    });
                }
                Ok(())
            } else {
                collect_non_aggregate_function_args(
                    &func_name,
                    args,
                    input_schema,
                    out,
                    catalog,
                    cte_catalog,
                    scope,
                )
            }
        }
        Expr::Paren { expr: inner, .. } | Expr::Unary { expr: inner, .. } => {
            collect_aggregates(inner, alias, input_schema, out, catalog, cte_catalog, scope)
        }
        Expr::Collate { expr: inner, .. } => {
            collect_aggregates(inner, alias, input_schema, out, catalog, cte_catalog, scope)
        }
        Expr::Coalesce { args, .. } | Expr::Greatest { args, .. } | Expr::Least { args, .. } => {
            for arg in args {
                collect_aggregates(arg, None, input_schema, out, catalog, cte_catalog, scope)?;
            }
            Ok(())
        }
        Expr::NullIf { a, b, .. } => {
            collect_aggregates(a, None, input_schema, out, catalog, cte_catalog, scope)?;
            collect_aggregates(b, None, input_schema, out, catalog, cte_catalog, scope)
        }
        Expr::Binary { left, right, .. } => {
            collect_aggregates(left, None, input_schema, out, catalog, cte_catalog, scope)?;
            collect_aggregates(right, None, input_schema, out, catalog, cte_catalog, scope)
        }
        _ => Ok(()),
    }
}

fn collect_non_aggregate_function_args(
    func_name: &str,
    args: &[Expr],
    input_schema: &Schema,
    out: &mut Vec<LogicalAggregateExpr>,
    catalog: &dyn Catalog,
    cte_catalog: &[(String, Schema)],
    scope: &mut ScopeStack,
) -> Result<(), PlanError> {
    if !is_supported_builtin(func_name) {
        return Err(PlanError::NotSupported(
            "non-aggregate function calls in aggregation context",
        ));
    }
    for arg in args {
        collect_aggregates(arg, None, input_schema, out, catalog, cte_catalog, scope)?;
    }
    Ok(())
}

/// Bind `STRING_AGG(value, delimiter)`.
///
/// PostgreSQL requires the delimiter argument; we bind it as a constant
/// text expression and stash it in `direct_arg` so the executor can join
/// the accumulated parts with it. The delimiter must be text-like (or a
/// NULL literal, which PostgreSQL treats as no separator).
fn bind_string_agg(
    args: &[Expr],
    input_schema: &Schema,
    catalog: &dyn Catalog,
    cte_catalog: &[(String, Schema)],
    scope: &mut ScopeStack,
) -> Result<BoundAggregateInput, PlanError> {
    if args.len() != 2 {
        return Err(PlanError::TypeMismatch(format!(
            "string_agg: expected 2 arguments (value, delimiter), got {}",
            args.len()
        )));
    }
    let value = bind_expr_with_ctes(&args[0], input_schema, catalog, cte_catalog, scope)?;
    let delimiter = bind_expr_with_ctes(&args[1], input_schema, catalog, cte_catalog, scope)?;
    let delim_type = delimiter.data_type();
    if !delim_type.is_textlike() && !matches!(delim_type, DataType::Null) {
        return Err(PlanError::TypeMismatch(format!(
            "string_agg: delimiter must be text, got {delim_type}"
        )));
    }
    let arg_type = value.data_type();
    Ok(BoundAggregateInput {
        arg: Some(value),
        direct_arg: Some(delimiter),
        order_by: None,
        arg_type,
    })
}

fn bind_ordered_set_percentile(
    args: &[Expr],
    within_group: Option<&[OrderItem]>,
    distinct: bool,
    input_schema: &Schema,
    catalog: &dyn Catalog,
    cte_catalog: &[(String, Schema)],
    scope: &mut ScopeStack,
) -> Result<BoundAggregateInput, PlanError> {
    if distinct {
        return Err(PlanError::NotSupported(
            "DISTINCT is not supported for ordered-set percentiles",
        ));
    }
    if args.len() != 1 {
        return Err(PlanError::TypeMismatch(format!(
            "ordered-set percentile: expected 1 direct argument, got {}",
            args.len()
        )));
    }
    let Some([item]) = within_group else {
        return Err(PlanError::NotSupported(
            "ordered-set percentile requires one WITHIN GROUP order key",
        ));
    };
    let direct_arg = bind_expr_with_ctes(&args[0], input_schema, catalog, cte_catalog, scope)?;
    let order_expr = bind_expr_with_ctes(&item.expr, input_schema, catalog, cte_catalog, scope)?;
    let order_type = order_expr.data_type();
    let asc = matches!(item.direction, SortDirection::Asc);
    let nulls_first = match item.nulls {
        NullsOrder::First => true,
        NullsOrder::Last => false,
        NullsOrder::Default => !asc,
    };
    let order_by = SortKey {
        expr: order_expr.clone(),
        asc,
        nulls_first,
    };
    Ok(BoundAggregateInput {
        arg: Some(order_expr),
        direct_arg: Some(direct_arg),
        order_by: Some(order_by),
        arg_type: order_type,
    })
}

pub(super) fn derive_agg_output_name(
    func_name: &str,
    args: &[Expr],
    within_group: Option<&[OrderItem]>,
) -> String {
    let is_star_arg = args.len() == 1
        && matches!(&args[0], Expr::Column { name }
            if name.parts.len() == 1 && name.parts[0].value == "*");
    if args.is_empty() || is_star_arg {
        return func_name.to_string();
    }
    let arg_key = args
        .iter()
        .map(|arg| format!("{arg:?}"))
        .collect::<Vec<_>>()
        .join(",");
    let base = format!("{func_name}({arg_key})");
    if let Some(items) = within_group {
        let order_key = items
            .iter()
            .map(|item| format!("{item:?}"))
            .collect::<Vec<_>>()
            .join(",");
        format!("{base} within group ({order_key})")
    } else {
        base
    }
}

pub(super) fn bind_projection_agg(
    items: &[SelectItem],
    agg_schema: &Schema,
    catalog: &dyn Catalog,
    cte_catalog: &[(String, Schema)],
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
                let alias_name = alias.as_ref().map(|a| a.value.as_str());
                let bound = bind_expr_or_agg_ref(
                    expr,
                    alias_name,
                    agg_schema,
                    catalog,
                    cte_catalog,
                    scope,
                )?;
                let name = alias
                    .as_ref()
                    .map_or_else(|| derive_output_name(expr, &bound), |a| a.value.clone());
                out.push((bound, name));
            }
        }
    }
    Ok(out)
}

/// Rebind a projection expression under an aggregation context.
///
/// `alias_name` is the projection's `AS alias` if any; it lets us
/// resolve an aggregate call whose materialised column in
/// `agg_schema` was named after that alias (the path
/// `collect_aggregates` takes when the SELECT item carries an alias).
/// Without this hint, `SUM(l_quantity) AS sum_qty` would fail to
/// resolve because the binder would look up the generic `"sum"`
/// derived name while the agg schema holds `"sum_qty"`.
fn bind_expr_or_agg_ref(
    expr: &Expr,
    alias_name: Option<&str>,
    agg_schema: &Schema,
    catalog: &dyn Catalog,
    cte_catalog: &[(String, Schema)],
    scope: &mut ScopeStack,
) -> Result<ScalarExpr, PlanError> {
    match expr {
        Expr::Call {
            name,
            args,
            distinct,
            within_group,
            over,
            ..
        } => {
            if let Some(alias) = alias_name {
                if let Some((i, f)) = agg_schema.find(alias) {
                    return Ok(ScalarExpr::Column {
                        name: f.name.clone(),
                        index: i,
                        data_type: f.data_type.clone(),
                    });
                }
            }
            let func_name = name
                .parts
                .last()
                .map_or("", |p| p.value.as_str())
                .to_ascii_lowercase();
            if is_aggregate_name(&func_name)
                && !is_scalar_min_max_call(
                    &func_name,
                    args.len(),
                    *distinct,
                    within_group.is_some(),
                    over.is_some(),
                )
            {
                let agg_name = derive_agg_output_name(&func_name, args, within_group.as_deref());
                if let Some((i, f)) = agg_schema.find(&agg_name) {
                    return Ok(ScalarExpr::Column {
                        name: f.name.clone(),
                        index: i,
                        data_type: f.data_type.clone(),
                    });
                }
            }
            let mut bound_args: Vec<ScalarExpr> = args
                .iter()
                .map(|arg| bind_expr_or_agg_ref(arg, None, agg_schema, catalog, cte_catalog, scope))
                .collect::<Result<_, _>>()?;
            validate_builtin_args(&func_name, &mut bound_args)?;
            let return_type = builtin_return_type(&func_name, &bound_args)?;
            coerce_common_builtin_args(&func_name, &mut bound_args, &return_type);
            Ok(ScalarExpr::FunctionCall {
                name: func_name,
                args: bound_args,
                data_type: return_type,
            })
        }
        // Composite expressions: walk into binary / unary / paren so
        // nested aggregate calls (`100.00 * SUM(l_extendedprice * …)
        // / SUM(l_quantity)`, etc.) re-bind via the column-reference
        // path rather than re-entering the standalone-aggregate
        // rejector in `bind_expr`.
        Expr::Binary {
            op, left, right, ..
        } => {
            let mut l = bind_expr_or_agg_ref(left, None, agg_schema, catalog, cte_catalog, scope)?;
            let mut r = bind_expr_or_agg_ref(right, None, agg_schema, catalog, cte_catalog, scope)?;
            coerce_literal_to_match(&mut l, &mut r);
            let data_type = binary_result_type(*op, l.data_type(), r.data_type())?;
            Ok(ScalarExpr::Binary {
                op: *op,
                left: Box::new(l),
                right: Box::new(r),
                data_type,
            })
        }
        Expr::Unary {
            op, expr: inner, ..
        } => {
            let bound = bind_expr_or_agg_ref(inner, None, agg_schema, catalog, cte_catalog, scope)?;
            let data_type = bound.data_type();
            Ok(ScalarExpr::Unary {
                op: *op,
                expr: Box::new(bound),
                data_type,
            })
        }
        Expr::Paren { expr: inner, .. } => {
            bind_expr_or_agg_ref(inner, alias_name, agg_schema, catalog, cte_catalog, scope)
        }
        Expr::Collate {
            expr: inner,
            collation,
            ..
        } => {
            let bound =
                bind_expr_or_agg_ref(inner, alias_name, agg_schema, catalog, cte_catalog, scope)?;
            bind_collated_expr(collation, bound)
        }
        Expr::Coalesce { args, .. } => {
            bind_common_scalar_wrapper("coalesce", args, agg_schema, catalog, cte_catalog, scope)
        }
        Expr::Greatest { args, .. } => {
            bind_common_scalar_wrapper("greatest", args, agg_schema, catalog, cte_catalog, scope)
        }
        Expr::Least { args, .. } => {
            bind_common_scalar_wrapper("least", args, agg_schema, catalog, cte_catalog, scope)
        }
        Expr::NullIf { a, b, .. } => {
            let mut left = bind_expr_or_agg_ref(a, None, agg_schema, catalog, cte_catalog, scope)?;
            let mut right = bind_expr_or_agg_ref(b, None, agg_schema, catalog, cte_catalog, scope)?;
            coerce_literal_to_match(&mut left, &mut right);
            let left_type = left.data_type();
            let right_type = right.data_type();
            if !comparable(&left_type, &right_type) {
                return Err(PlanError::TypeMismatch(format!(
                    "nullif: cannot compare {left_type} and {right_type}"
                )));
            }
            Ok(ScalarExpr::FunctionCall {
                name: "nullif".to_owned(),
                args: vec![left, right],
                data_type: left_type,
            })
        }
        _ => bind_expr_with_ctes(expr, agg_schema, catalog, cte_catalog, scope),
    }
}

fn bind_common_scalar_wrapper(
    func_name: &str,
    args: &[Expr],
    agg_schema: &Schema,
    catalog: &dyn Catalog,
    cte_catalog: &[(String, Schema)],
    scope: &mut ScopeStack,
) -> Result<ScalarExpr, PlanError> {
    let mut bound_args: Vec<ScalarExpr> = args
        .iter()
        .map(|arg| bind_expr_or_agg_ref(arg, None, agg_schema, catalog, cte_catalog, scope))
        .collect::<Result<_, _>>()?;
    let return_type = common_scalar_return_type(func_name, &bound_args)?;
    coerce_args_to_common_type(&mut bound_args, &return_type);
    Ok(ScalarExpr::FunctionCall {
        name: func_name.to_owned(),
        args: bound_args,
        data_type: return_type,
    })
}

pub(super) fn bind_projection_with_scope(
    items: &[SelectItem],
    input: &Schema,
    from_scope: &[ScopeEntry],
    catalog: &dyn Catalog,
    cte_catalog: &[(String, Schema)],
    outer_scope: &mut ScopeStack,
) -> Result<Vec<(ScalarExpr, String)>, PlanError> {
    let mut out = Vec::new();
    let binding_schema = schema_for_qualified_binding(input, from_scope)?;
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
                let bound = bind_projection_expr_with_scope(
                    expr,
                    &binding_schema,
                    from_scope,
                    catalog,
                    cte_catalog,
                    outer_scope,
                )?;
                let name = alias
                    .as_ref()
                    .map_or_else(|| derive_output_name(expr, &bound), |a| a.value.clone());
                out.push((bound, name));
            }
        }
    }
    Ok(out)
}

fn bind_projection_expr_with_scope(
    expr: &Expr,
    binding_schema: &Schema,
    from_scope: &[ScopeEntry],
    catalog: &dyn Catalog,
    cte_catalog: &[(String, Schema)],
    scope: &mut ScopeStack,
) -> Result<ScalarExpr, PlanError> {
    if let Some(expr) = bind_row_to_json_whole_row(expr, from_scope) {
        return Ok(expr);
    }
    bind_expr_with_ctes(expr, binding_schema, catalog, cte_catalog, scope)
}

fn bind_row_to_json_whole_row(expr: &Expr, from_scope: &[ScopeEntry]) -> Option<ScalarExpr> {
    let Expr::Call { name, args, .. } = expr else {
        return None;
    };
    let func_name = name
        .parts
        .last()
        .map_or("", |part| part.value.as_str())
        .to_ascii_lowercase();
    if func_name != "row_to_json" || args.len() != 1 {
        return None;
    }
    let Expr::Column { name: arg_name } = &args[0] else {
        return None;
    };
    if arg_name.parts.len() != 1 {
        return None;
    }
    let qualifier = &arg_name.parts[0].value;
    let entries = from_scope
        .iter()
        .filter(|entry| entry.qualifier.eq_ignore_ascii_case(qualifier))
        .collect::<Vec<_>>();
    if entries.is_empty() {
        return None;
    }
    let fields = entries
        .iter()
        .map(|entry| (entry.field.name.clone(), entry.field.data_type.clone()))
        .collect::<Vec<_>>();
    let args = entries
        .iter()
        .map(|entry| ScalarExpr::Column {
            name: entry.field.name.clone(),
            index: entry.field_index,
            data_type: entry.field.data_type.clone(),
        })
        .collect::<Vec<_>>();
    let record_type = DataType::Record(fields);
    Some(ScalarExpr::FunctionCall {
        name: "row_to_json".to_owned(),
        args: vec![ScalarExpr::FunctionCall {
            name: "row".to_owned(),
            args,
            data_type: record_type,
        }],
        data_type: DataType::Jsonb,
    })
}
