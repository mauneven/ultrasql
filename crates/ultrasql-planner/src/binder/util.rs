//! Shared utility helpers used by multiple binder submodules.
//! Split out of `binder/mod.rs` to keep each file under the 600-line ceiling.

use ultrasql_core::{Field, Schema};
use ultrasql_parser::ast::{
    Expr, Literal, NullsOrder, ObjectName, OrderItem, SelectItem, SortDirection,
};

use super::{
    Catalog, LogicalOnConflict, LogicalPlan, PlanError, ScalarExpr, ScopeStack, SortKey, bind_expr,
    bind_expr_with_ctes,
};
use crate::catalog::TableMeta;

/// Build the `RETURNING` schema from the resolved `(expr, name)` pairs.
pub(super) fn build_returning_schema(
    returning: &[(ScalarExpr, String)],
) -> Result<Schema, PlanError> {
    if returning.is_empty() {
        return Ok(Schema::empty());
    }
    let fields: Vec<Field> = returning
        .iter()
        .map(|(e, n)| Field::nullable(n, e.data_type()))
        .collect();
    Ok(Schema::new_with_duplicate_names(fields))
}

/// Bind a `RETURNING` projection list against `table_schema`.
pub(super) fn bind_returning(
    items: &[SelectItem],
    table_schema: &Schema,
    catalog: &dyn Catalog,
    scope: &mut ScopeStack,
) -> Result<Vec<(ScalarExpr, String)>, PlanError> {
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        match item {
            SelectItem::Wildcard { .. } | SelectItem::QualifiedWildcard { .. } => {
                return Err(PlanError::NotSupported("wildcard in RETURNING clause"));
            }
            SelectItem::Expr { expr, alias, .. } => {
                let bound = bind_expr(expr, table_schema, catalog, scope)?;
                let name = alias
                    .as_ref()
                    .map_or_else(|| derive_output_name(expr, &bound), |a| a.value.clone());
                out.push((bound, name));
            }
        }
    }
    Ok(out)
}

/// Extract the last identifier of an `ObjectName` as a lowercase string.
#[inline]
pub(super) fn object_name_simple(name: &ObjectName) -> String {
    name.parts
        .last()
        .map_or_else(String::new, |p| p.value.to_ascii_lowercase())
}

/// Extract the explicit namespace in a table reference, if present.
#[inline]
pub(super) fn object_name_explicit_namespace(name: &ObjectName) -> Option<String> {
    (name.parts.len() >= 2).then(|| {
        let namespace_index = name.parts.len() - 2;
        name.parts[namespace_index].value.to_ascii_lowercase()
    })
}

/// Validate that a catalog hit is visible through the requested SQL name.
pub(super) fn validate_table_reference_namespace(
    catalog: &dyn Catalog,
    name: &ObjectName,
    table_name: &str,
    meta: &TableMeta,
) -> Result<(), PlanError> {
    if let Some(namespace) = object_name_explicit_namespace(name) {
        if meta.schema_name.eq_ignore_ascii_case(&namespace) {
            return Ok(());
        }
        return Err(PlanError::TableNotFound(format!(
            "{namespace}.{table_name}"
        )));
    }

    if catalog.table_schema_visible_without_qualification(&meta.schema_name) {
        return Ok(());
    }

    Err(PlanError::TableNotFound(table_name.to_owned()))
}

/// Derive an output column name from an expression.
pub(super) fn derive_output_name(ast: &Expr, bound: &ScalarExpr) -> String {
    match ast {
        Expr::Column { name } => name
            .parts
            .last()
            .map_or_else(String::new, |p| p.value.clone()),
        Expr::Call { name, .. } => name
            .parts
            .last()
            .map_or_else(String::new, |p| p.value.clone()),
        Expr::Collate { expr, .. } => derive_output_name(expr, bound),
        _ => bound.to_string(),
    }
}

pub(super) fn bind_order_by(
    items: &[OrderItem],
    input: &Schema,
    catalog: &dyn Catalog,
    cte_catalog: &[(String, Schema)],
    scope: &mut ScopeStack,
) -> Result<Vec<SortKey>, PlanError> {
    let mut keys = Vec::with_capacity(items.len());
    for item in items {
        let expr = bind_expr_with_ctes(&item.expr, input, catalog, cte_catalog, scope)?;
        let asc = matches!(item.direction, SortDirection::Asc);
        let nulls_first = match item.nulls {
            NullsOrder::First => true,
            NullsOrder::Last => false,
            NullsOrder::Default => !asc,
        };
        keys.push(SortKey {
            expr,
            asc,
            nulls_first,
        });
    }
    Ok(keys)
}

pub(super) fn bind_unsigned_literal(expr: &Expr, label: &'static str) -> Result<u64, PlanError> {
    match expr {
        Expr::Literal(Literal::Integer { text, .. }) => text.parse::<u64>().map_err(|_| {
            PlanError::TypeMismatch(format!(
                "{label} must be a non-negative integer, got '{text}'"
            ))
        }),
        Expr::Paren { expr, .. } => bind_unsigned_literal(expr, label),
        _ => Err(PlanError::NotSupported(
            "non-literal LIMIT/OFFSET expressions",
        )),
    }
}

/// Walk a bound logical plan and return `true` if any expression node
/// anywhere in the tree is a [`crate::expr::ScalarExpr::OuterColumn`].
pub(super) fn plan_contains_outer_column(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Scan { .. }
        | LogicalPlan::Empty { .. }
        | LogicalPlan::Truncate { .. }
        | LogicalPlan::CreateTable { .. }
        | LogicalPlan::CreateMaterializedView { .. }
        | LogicalPlan::CreateTypeEnum { .. }
        | LogicalPlan::CreateTypeComposite { .. }
        | LogicalPlan::CreateDomain { .. }
        | LogicalPlan::CreateOperator { .. }
        | LogicalPlan::CreateIndex { .. }
        | LogicalPlan::DropIndex { .. }
        | LogicalPlan::CreatePolicy { .. }
        | LogicalPlan::CreateRole { .. }
        | LogicalPlan::AlterRole { .. }
        | LogicalPlan::DropRole { .. }
        | LogicalPlan::GrantPrivileges { .. }
        | LogicalPlan::RevokePrivileges { .. }
        | LogicalPlan::AlterDefaultPrivileges { .. }
        | LogicalPlan::GrantRole { .. }
        | LogicalPlan::RevokeRole { .. }
        | LogicalPlan::CreateSchema { .. }
        | LogicalPlan::DropSchema { .. }
        | LogicalPlan::DropTable { .. }
        | LogicalPlan::AlterTable { .. }
        | LogicalPlan::CreateSequence { .. }
        | LogicalPlan::AlterSequence { .. }
        | LogicalPlan::DropSequence { .. }
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
        | LogicalPlan::SetRole { .. }
        | LogicalPlan::Listen { .. }
        | LogicalPlan::Notify { .. }
        | LogicalPlan::Unlisten { .. }
        | LogicalPlan::FunctionScan { .. } => false,
        LogicalPlan::Filter { input, predicate } => {
            expr_contains_outer(predicate) || plan_contains_outer_column(input)
        }
        LogicalPlan::Project { input, exprs, .. } => {
            exprs.iter().any(|(e, _)| expr_contains_outer(e)) || plan_contains_outer_column(input)
        }
        LogicalPlan::Sort { input, keys } => {
            keys.iter().any(|k| expr_contains_outer(&k.expr)) || plan_contains_outer_column(input)
        }
        LogicalPlan::Limit { input, .. } | LogicalPlan::LockRows { input, .. } => {
            plan_contains_outer_column(input)
        }
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggregates,
            ..
        } => {
            group_by.iter().any(expr_contains_outer)
                || aggregates.iter().any(|a| {
                    a.arg.as_ref().is_some_and(expr_contains_outer)
                        || a.direct_arg.as_ref().is_some_and(expr_contains_outer)
                        || a.order_by
                            .as_ref()
                            .is_some_and(|key| expr_contains_outer(&key.expr))
                })
                || plan_contains_outer_column(input)
        }
        LogicalPlan::Join { left, right, .. } | LogicalPlan::SetOp { left, right, .. } => {
            plan_contains_outer_column(left) || plan_contains_outer_column(right)
        }
        LogicalPlan::Cte {
            definition, body, ..
        } => plan_contains_outer_column(definition) || plan_contains_outer_column(body),
        LogicalPlan::Values { rows, .. } => {
            rows.iter().flat_map(|r| r.iter()).any(expr_contains_outer)
        }
        LogicalPlan::Insert {
            source,
            on_conflict,
            returning,
            ..
        } => {
            plan_contains_outer_column(source)
                || on_conflict.as_ref().is_some_and(|oc| match oc {
                    LogicalOnConflict::DoNothing { .. } => false,
                    LogicalOnConflict::DoUpdate {
                        assignments,
                        r#where,
                        ..
                    } => {
                        assignments.iter().any(|(_, e)| expr_contains_outer(e))
                            || r#where.as_ref().is_some_and(expr_contains_outer)
                    }
                })
                || returning.iter().any(|(e, _)| expr_contains_outer(e))
        }
        LogicalPlan::Update {
            assignments,
            input,
            returning,
            ..
        } => {
            assignments.iter().any(|(_, e)| expr_contains_outer(e))
                || plan_contains_outer_column(input)
                || returning.iter().any(|(e, _)| expr_contains_outer(e))
        }
        LogicalPlan::Delete {
            input, returning, ..
        } => {
            plan_contains_outer_column(input)
                || returning.iter().any(|(e, _)| expr_contains_outer(e))
        }
        LogicalPlan::Explain { input, .. } => plan_contains_outer_column(input),
        LogicalPlan::Copy { .. } => false,
        LogicalPlan::Window {
            input,
            partition_by,
            order_by,
            func,
            ..
        } => {
            partition_by.iter().any(expr_contains_outer)
                || order_by.iter().any(|k| expr_contains_outer(&k.expr))
                || match func {
                    crate::LogicalWindowFunc::Lag { expr, .. }
                    | crate::LogicalWindowFunc::Lead { expr, .. }
                    | crate::LogicalWindowFunc::FirstValue(expr)
                    | crate::LogicalWindowFunc::LastValue(expr)
                    | crate::LogicalWindowFunc::NthValue { expr, .. } => expr_contains_outer(expr),
                    _ => false,
                }
                || plan_contains_outer_column(input)
        }
    }
}

fn expr_contains_outer(expr: &ScalarExpr) -> bool {
    expr.contains_outer_column()
}
