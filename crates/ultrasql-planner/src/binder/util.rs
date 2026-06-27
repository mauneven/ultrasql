//! Shared utility helpers used by multiple binder submodules.
//! Split out of `binder/mod.rs` to keep each file under the 600-line ceiling.

use ultrasql_core::{Field, Schema};
use ultrasql_parser::ast::{
    Expr, Literal, NullsOrder, ObjectName, OrderItem, SelectItem, SortDirection,
};

use super::{
    Catalog, LogicalMergeAction, LogicalOnConflict, LogicalPlan, PlanError, ScalarExpr, ScopeStack,
    SortKey, bind_expr, bind_expr_with_ctes,
};
use crate::catalog::TableMeta;

pub(super) struct ResolvedTableRef {
    pub(super) plan_name: String,
    pub(super) meta: TableMeta,
}

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

pub(super) fn canonical_table_plan_name(meta: &TableMeta, table_name: &str) -> String {
    ultrasql_catalog::table_lookup_key(&meta.schema_name, table_name)
}

pub(super) fn parse_pg_identifier_path(text: &str) -> Option<Vec<String>> {
    let mut parts = Vec::new();
    let mut chars = text.chars().peekable();
    loop {
        match chars.peek().copied()? {
            '"' => {
                chars.next();
                let mut part = String::new();
                loop {
                    match chars.next()? {
                        '"' if chars.peek() == Some(&'"') => {
                            chars.next();
                            part.push('"');
                        }
                        '"' => break,
                        ch => part.push(ch),
                    }
                }
                parts.push(part);
            }
            _ => {
                let mut part = String::new();
                while let Some(ch) = chars.peek().copied() {
                    if ch == '.' {
                        break;
                    }
                    part.push(ch);
                    chars.next();
                }
                if part.is_empty() {
                    return None;
                }
                parts.push(part);
            }
        }
        match chars.next() {
            Some('.') => continue,
            None => return Some(parts),
            Some(_) => return None,
        }
    }
}

pub(super) fn lookup_table_reference(
    catalog: &dyn Catalog,
    name: &ObjectName,
) -> Result<ResolvedTableRef, PlanError> {
    let table_name = object_name_simple(name);
    if let Some(namespace) = object_name_explicit_namespace(name) {
        let meta = catalog
            .lookup_table_in_schema(&namespace, &table_name)
            .ok_or_else(|| PlanError::TableNotFound(format!("{namespace}.{table_name}")))?;
        let plan_name = canonical_table_plan_name(&meta, &table_name);
        return Ok(ResolvedTableRef { plan_name, meta });
    }

    let meta = catalog
        .lookup_table(&table_name)
        .ok_or_else(|| PlanError::TableNotFound(table_name.clone()))?;
    if !catalog.table_schema_visible_without_qualification(&meta.schema_name) {
        return Err(PlanError::TableNotFound(table_name));
    }
    let plan_name = canonical_table_plan_name(&meta, &table_name);
    Ok(ResolvedTableRef { plan_name, meta })
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

/// A bare, unqualified single-identifier `ORDER BY` / `DISTINCT ON` item — the
/// only shape that prefers a SELECT-list *output* alias over an input column
/// (PostgreSQL: `ORDER BY <name>` resolves to an output column first; qualified
/// names `t.c` and any expression `a+1` resolve against input columns only).
/// Returns the (case-folded comparison) identifier text when `expr` is such a
/// reference.
fn bare_output_name(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Column { name } if name.parts.len() == 1 => Some(name.parts[0].value.as_str()),
        _ => None,
    }
}

/// Resolve a bare `ORDER BY` / `DISTINCT ON` name against the SELECT-list output
/// columns (`proj_exprs`, the bound projection expressions paired with their
/// output names), mirroring PostgreSQL's output-alias-first rule.
///
/// * Exactly one output column matches → that projection expression (already in
///   the sort input's schema terms).
/// * More than one output column shares the name → `ORDER BY "x" is ambiguous`
///   (PostgreSQL raises this only for output/output collisions, never for an
///   output column that merely shadows an input column of the same name).
/// * No output column matches → `None`, and the caller falls back to binding the
///   name against the input schema.
fn resolve_output_alias(
    name: &str,
    proj_exprs: &[(ScalarExpr, String)],
) -> Result<Option<ScalarExpr>, PlanError> {
    let mut hit: Option<&ScalarExpr> = None;
    for (expr, out_name) in proj_exprs {
        if out_name.eq_ignore_ascii_case(name) {
            if hit.is_some() {
                return Err(PlanError::Ambiguous(format!(
                    "ORDER BY \"{name}\" is ambiguous"
                )));
            }
            hit = Some(expr);
        }
    }
    Ok(hit.cloned())
}

/// Bind an `ORDER BY` list into sort keys.
///
/// `proj_exprs`, when `Some`, are the bound projection output expressions in
/// the sort input's schema (the `Sort` sits *below* the projection). It is the
/// resolution target for positional ordinals (`ORDER BY 1`): see
/// [`positional_ordinal`]. It is *also* the SELECT-list-alias resolution target:
/// a bare unqualified name prefers a matching output column over an input column
/// (PostgreSQL semantics). When `None`, the sort input *is* the output relation
/// (a set-operation `ORDER BY`, or a `Sort` lifted above the projection), so an
/// ordinal resolves to a column reference into `input` and there are no aliases
/// to prefer.
pub(super) fn bind_order_by(
    items: &[OrderItem],
    input: &Schema,
    proj_exprs: Option<&[(ScalarExpr, String)]>,
    catalog: &dyn Catalog,
    cte_catalog: &[(String, Schema)],
    scope: &mut ScopeStack,
) -> Result<Vec<SortKey>, PlanError> {
    let mut keys = Vec::with_capacity(items.len());
    for item in items {
        let expr = match positional_ordinal(&item.expr) {
            Some(n) => resolve_order_ordinal(n, input, proj_exprs)?,
            None => {
                // A bare unqualified name prefers a SELECT-list output alias
                // over an input column (PostgreSQL). Try output resolution
                // first; fall back to the input schema when no alias matches.
                match (bare_output_name(&item.expr), proj_exprs) {
                    (Some(name), Some(exprs)) => match resolve_output_alias(name, exprs)? {
                        Some(expr) => expr,
                        None => {
                            bind_expr_with_ctes(&item.expr, input, catalog, cte_catalog, scope)?
                        }
                    },
                    _ => bind_expr_with_ctes(&item.expr, input, catalog, cte_catalog, scope)?,
                }
            }
        };
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

/// A bare unsigned integer literal in `ORDER BY` / `GROUP BY` is a 1-based
/// positional reference to the SELECT output list (PostgreSQL semantics):
/// `ORDER BY 1` sorts by the first output column, `GROUP BY 1` groups by the
/// first select-list expression. Returns the ordinal when `expr` is such a
/// reference. Only an unadorned integer literal is positional — `(1)`,
/// `1 + 0`, `-1`, and non-integer constants are ordinary expressions, matching
/// PostgreSQL (which sorts/groups by the constant value, a no-op / single
/// group, rather than treating them as positions).
pub(super) fn positional_ordinal(expr: &Expr) -> Option<u64> {
    match expr {
        Expr::Literal(Literal::Integer { text, .. }) => text.parse::<u64>().ok(),
        _ => None,
    }
}

/// Validate a 1-based positional ordinal against the number of output columns
/// (`len`) and return the 0-based index, mirroring PostgreSQL's
/// `<clause> position N is not in select list` error.
pub(super) fn ordinal_index(n: u64, len: usize, clause: &'static str) -> Result<usize, PlanError> {
    usize::try_from(n)
        .ok()
        .filter(|&i| i >= 1 && i <= len)
        .map(|i| i - 1)
        .ok_or_else(|| {
            PlanError::TypeMismatch(format!(
                "{clause} position {n} is not in select list (valid range is 1..={len})"
            ))
        })
}

/// Resolve a positional `ORDER BY` ordinal to its sort-key expression.
///
/// With `proj_exprs` (the `Sort` sits below the projection) the ordinal maps to
/// the already-bound projection expression, valid in the sort input's schema —
/// this also handles wildcards, since the projection list is already expanded.
/// Without it the ordinal maps to a column reference into `output`, whose schema
/// is the sort input.
fn resolve_order_ordinal(
    n: u64,
    output: &Schema,
    proj_exprs: Option<&[(ScalarExpr, String)]>,
) -> Result<ScalarExpr, PlanError> {
    let len = proj_exprs.map_or(output.fields().len(), <[(ScalarExpr, String)]>::len);
    let idx = ordinal_index(n, len, "ORDER BY")?;
    Ok(match proj_exprs {
        Some(exprs) => exprs[idx].0.clone(),
        None => {
            let field = &output.fields()[idx];
            ScalarExpr::Column {
                name: field.name.clone(),
                index: idx,
                data_type: field.data_type.clone(),
            }
        }
    })
}

/// The SELECT output expressions a positional `GROUP BY` ordinal references,
/// when the projection is a plain expression list. Returns `None` if the
/// projection contains a wildcard (`*` / `t.*`): the projection is bound after
/// `GROUP BY`, so the expanded column count is not yet known and ordinal
/// counting would be ambiguous — the caller rejects positional `GROUP BY` in
/// that (rare) case rather than risk grouping by the wrong column.
pub(super) fn plain_select_exprs(projection: &[SelectItem]) -> Option<Vec<&Expr>> {
    let mut out = Vec::with_capacity(projection.len());
    for item in projection {
        match item {
            SelectItem::Expr { expr, .. } => out.push(expr),
            SelectItem::Wildcard { .. } | SelectItem::QualifiedWildcard { .. } => return None,
        }
    }
    Some(out)
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
        | LogicalPlan::CreateView { .. }
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
        | LogicalPlan::AlterView { .. }
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
        | LogicalPlan::Describe { .. }
        | LogicalPlan::Summarize { .. }
        | LogicalPlan::Checkpoint { .. }
        | LogicalPlan::ExportDatabase { .. }
        | LogicalPlan::ImportDatabase { .. }
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
        LogicalPlan::DistinctOn { input, on_keys } => {
            on_keys.iter().any(expr_contains_outer) || plan_contains_outer_column(input)
        }
        LogicalPlan::Limit { input, .. }
        | LogicalPlan::LockRows { input, .. }
        | LogicalPlan::SingleRowAssert { input, .. } => plan_contains_outer_column(input),
        LogicalPlan::Pivot {
            input, aggregate, ..
        } => {
            aggregate.arg.as_ref().is_some_and(expr_contains_outer)
                || plan_contains_outer_column(input)
        }
        LogicalPlan::Unpivot { input, .. } => plan_contains_outer_column(input),
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
        LogicalPlan::Merge {
            source,
            on,
            clauses,
            ..
        } => {
            plan_contains_outer_column(source)
                || expr_contains_outer(on)
                || clauses.iter().any(|clause| {
                    clause.condition.as_ref().is_some_and(expr_contains_outer)
                        || match &clause.action {
                            LogicalMergeAction::Update { assignments } => {
                                assignments.iter().any(|(_, e)| expr_contains_outer(e))
                            }
                            LogicalMergeAction::Delete => false,
                            LogicalMergeAction::Insert { values, .. } => {
                                values.iter().any(expr_contains_outer)
                            }
                        }
                })
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
