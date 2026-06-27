//! Binder — turn a parser AST into a typed logical plan.
//!
//! The binder is a single pass over the AST. For a `SELECT` statement it:
//!
//! 1. Resolves the `FROM` clause into a join tree. A single named table
//!    becomes a [`crate::plan::LogicalPlan::Scan`]; explicit joins produce
//!    [`crate::plan::LogicalPlan::Join`]; subqueries become inner scopes.
//! 2. Expands `SELECT *` and `SELECT t.*` by walking the FROM scope.
//! 3. Detects aggregate calls in the projection / HAVING; wraps the plan
//!    in [`crate::plan::LogicalPlan::Aggregate`] when needed.
//! 4. Folds `UNION` / `INTERSECT` / `EXCEPT` tails into
//!    [`crate::plan::LogicalPlan::SetOp`].
//! 5. Binds leading CTEs and wraps the body in
//!    [`crate::plan::LogicalPlan::Cte`] nodes.
//! 6. Resolves column references against the producing operator's schema;
//!    bare column names become [`crate::expr::ScalarExpr::Column`] nodes
//!    with a 0-based index.
//! 7. Type-checks expressions, using
//!    [`ultrasql_core::DataType::numeric_join`] for arithmetic and a
//!    simple shape rule for comparisons and boolean operators.
//! 8. Wraps the plan in `Filter` / `Project` / `Sort` / `Limit` in the
//!    canonical SQL evaluation order.
//!
//! For DML statements the binder produces the corresponding plan nodes:
//!
//! - `INSERT` → [`crate::plan::LogicalPlan::Insert`] with a `Values` or
//!   bound-`Select` child for the row source.
//! - `UPDATE` → [`crate::plan::LogicalPlan::Update`] over a `Scan` /
//!   `Filter` child. `UPDATE … FROM other_table` returns
//!   [`crate::error::PlanError::NotSupported`].
//! - `DELETE` → [`crate::plan::LogicalPlan::Delete`] over a `Scan` /
//!   `Filter` child. `DELETE … USING other_table` similarly returns
//!   `NotSupported`.
//! - `TRUNCATE` → [`crate::plan::LogicalPlan::Truncate`]; every table
//!   name is validated against the catalog.
//!
//! `EXCLUDED` column references in `ON CONFLICT DO UPDATE` are not
//! supported in v0.2; the binder returns `NotSupported` for that form.
//!
//! ### Recursive CTEs
//!
//! `WITH RECURSIVE` is parsed and the `recursive` flag is recorded on the
//! produced [`crate::plan::LogicalPlan::Cte`] node. The recursion fixpoint
//! is **not** evaluated at this layer; that is deferred to a future executor
//! wave. Until then a recursive CTE binding resolves the CTE's definition
//! non-recursively.

use ultrasql_core::{DataType, Field, Schema, Value};
use ultrasql_parser::ast::{
    DescribeObjectKind as AstDescribeObjectKind, DescribeStmt, DescribeTarget as AstDescribeTarget,
    Distinct, ExplainFormat as AstExplainFormat, ExplainStmt, ExportDatabaseStmt, Expr as AstExpr,
    ImportDatabaseStmt, Literal, LockStrength as AstLockStrength,
    LockWaitPolicy as AstLockWaitPolicy, NullsOrder, SelectStmt, SetOp, SetQuantifier, SetRoleStmt,
    SetScope, SetValue, SetVarStmt, SortDirection, Statement, SummarizeStmt,
};
use ultrasql_parser::span::Span;

use crate::catalog::Catalog;
use crate::error::PlanError;
use crate::expr::ScalarExpr;
use crate::plan::{
    AggregateFunc, ConflictTarget, ExplainFormat, LockStrength, LockWaitPolicy,
    LogicalAggregateExpr, LogicalAlterTableAction, LogicalAlterViewAction,
    LogicalDescribeObjectKind, LogicalDescribeTarget, LogicalJoinCondition, LogicalJoinType,
    LogicalMergeAction, LogicalMergeClause, LogicalMergeMatchKind, LogicalOnConflict,
    LogicalPivotAggregate, LogicalPivotValue, LogicalPlan, LogicalSetOp, LogicalSetQuantifier,
    LogicalSetVariableAction, LogicalUnpivotColumn, SortKey, TxnIsolationLevel,
};
use crate::scope::{ScopeFrame, ScopeStack};

// Submodules — each file stays under the 600-line ceiling.
mod aggregate;
mod ddl;
mod dml;
mod expr_bind;
mod expr_type;
mod from;
mod privilege;
mod util;
mod window;

use self::aggregate::{
    bind_aggregate, bind_projection_agg, bind_projection_with_scope, derive_agg_output_name,
    expr_has_aggregate, is_aggregate_name, is_scalar_min_max_call, projection_item_has_aggregate,
};
use self::ddl::{
    bind_alter_role, bind_alter_sequence, bind_alter_table, bind_alter_view, bind_comment,
    bind_copy, bind_create_domain, bind_create_index, bind_create_materialized_view,
    bind_create_operator, bind_create_policy, bind_create_role, bind_create_schema,
    bind_create_sequence, bind_create_table, bind_create_type, bind_create_view, bind_drop_index,
    bind_drop_role, bind_drop_schema, bind_drop_sequence, bind_drop_table, bind_truncate,
};
use self::dml::{bind_delete, bind_insert, bind_merge, bind_update};
use self::expr_bind::{bind_expr, bind_expr_with_ctes};
use self::from::bind_from;
use self::privilege::{
    bind_alter_default_privileges, bind_grant_privileges, bind_grant_role, bind_revoke_privileges,
    bind_revoke_role,
};
use self::util::{
    bind_order_by, bind_returning, bind_unsigned_literal, build_returning_schema,
    derive_output_name, lookup_table_reference, object_name_simple, parse_pg_identifier_path,
    plan_contains_outer_column,
};

#[cfg(test)]
mod tests;

/// Bind a [`Statement`] against the supplied catalog and produce a
/// typed logical plan.
///
/// # Errors
///
/// Returns a [`PlanError`] for any of:
/// - missing table or column,
/// - ambiguous column reference,
/// - a type mismatch in an operator,
/// - a construct the binder does not yet implement.
pub fn bind(stmt: &Statement, catalog: &dyn Catalog) -> Result<LogicalPlan, PlanError> {
    let mut scope = ScopeStack::new();
    match stmt {
        Statement::Select(s) => bind_select(s, catalog, &mut scope),
        Statement::Insert(s) => bind_insert(s, catalog, &mut scope),
        Statement::Update(s) => bind_update(s, catalog, &mut scope),
        Statement::Delete(s) => bind_delete(s, catalog, &mut scope),
        Statement::Merge(s) => bind_merge(s, catalog, &mut scope),
        Statement::Truncate(s) => bind_truncate(s, catalog),
        Statement::Describe(s) => bind_describe(s, catalog, &mut scope),
        Statement::Summarize(s) => bind_summarize(s, catalog),
        Statement::ExportDatabase(s) => bind_export_database(s),
        Statement::ImportDatabase(s) => bind_import_database(s),
        Statement::Checkpoint { .. } => Ok(LogicalPlan::Checkpoint {
            schema: Schema::empty(),
        }),
        Statement::CreateTable(s) => bind_create_table(s, catalog),
        Statement::CreateMaterializedView(s) => bind_create_materialized_view(s, catalog),
        Statement::CreateView(s) => bind_create_view(s, catalog),
        Statement::CreateType(s) => bind_create_type(s, catalog),
        Statement::CreateDomain(s) => bind_create_domain(s, catalog),
        Statement::CreateOperator(s) => bind_create_operator(s, catalog),
        Statement::CreatePolicy(s) => bind_create_policy(s, catalog),
        Statement::CreateRole(s) => bind_create_role(s),
        Statement::CreateSchema(s) => bind_create_schema(s),
        Statement::Grant(s) => bind_grant_privileges(s),
        Statement::Revoke(s) => bind_revoke_privileges(s),
        Statement::AlterDefaultPrivileges(s) => bind_alter_default_privileges(s),
        Statement::GrantRole(s) => bind_grant_role(s),
        Statement::RevokeRole(s) => bind_revoke_role(s),
        Statement::CreateIndex(s) => bind_create_index(s, catalog),
        Statement::DropIndex(s) => bind_drop_index(s, catalog),
        Statement::CreateSequence(s) => bind_create_sequence(s),
        Statement::AlterSequence(s) => bind_alter_sequence(s),
        Statement::DropSequence(s) => bind_drop_sequence(s),
        Statement::DropSchema(s) => bind_drop_schema(s),
        Statement::AlterRole(s) => bind_alter_role(s),
        Statement::DropRole(s) => bind_drop_role(s),
        Statement::Comment(s) => bind_comment(s, catalog),
        Statement::DropTable(s) => bind_drop_table(s, catalog),
        Statement::AlterTable(s) => bind_alter_table(s, catalog),
        Statement::AlterView(s) => bind_alter_view(s, catalog),
        Statement::Copy(s) => bind_copy(s, catalog),
        Statement::Explain(s) => bind_explain(s, catalog, &mut scope),
        // Transaction-control statements have no catalog dependency: the
        // server inspects the per-session TxnState and dispatches
        // accordingly. The binder emits the corresponding LogicalPlan
        // variants so the Simple- and Extended-Query paths share a single
        // dispatch surface.
        Statement::Begin {
            isolation_level,
            access_mode,
            ..
        } => {
            use ultrasql_parser::ast::AstIsolationLevel as AL;
            let level = isolation_level.map(|l| match l {
                AL::ReadCommitted => TxnIsolationLevel::ReadCommitted,
                AL::RepeatableRead => TxnIsolationLevel::RepeatableRead,
                AL::Serializable => TxnIsolationLevel::Serializable,
            });
            Ok(LogicalPlan::Begin {
                isolation_level: level,
                read_only: access_mode
                    .map(|m| matches!(m, ultrasql_parser::ast::TransactionAccessMode::ReadOnly)),
                schema: Schema::empty(),
            })
        }
        Statement::Commit { .. } => Ok(LogicalPlan::Commit {
            schema: Schema::empty(),
        }),
        Statement::Rollback { .. } => Ok(LogicalPlan::Rollback {
            schema: Schema::empty(),
        }),
        Statement::Savepoint(s) => Ok(LogicalPlan::Savepoint {
            name: s.name.value.to_ascii_lowercase(),
            schema: Schema::empty(),
        }),
        Statement::RollbackToSavepoint(s) => Ok(LogicalPlan::RollbackToSavepoint {
            name: s.name.value.to_ascii_lowercase(),
            schema: Schema::empty(),
        }),
        Statement::ReleaseSavepoint(s) => Ok(LogicalPlan::ReleaseSavepoint {
            name: s.name.value.to_ascii_lowercase(),
            schema: Schema::empty(),
        }),
        Statement::PrepareTransaction { gid, .. } => Ok(LogicalPlan::PrepareTransaction {
            gid: gid.clone(),
            schema: Schema::empty(),
        }),
        Statement::CommitPrepared { gid, .. } => Ok(LogicalPlan::CommitPrepared {
            gid: gid.clone(),
            schema: Schema::empty(),
        }),
        Statement::RollbackPrepared { gid, .. } => Ok(LogicalPlan::RollbackPrepared {
            gid: gid.clone(),
            schema: Schema::empty(),
        }),
        Statement::SetTransaction {
            isolation_level,
            access_mode,
            ..
        } => {
            use ultrasql_parser::ast::AstIsolationLevel as AL;
            let level = isolation_level.map(|l| match l {
                AL::ReadCommitted => TxnIsolationLevel::ReadCommitted,
                AL::RepeatableRead => TxnIsolationLevel::RepeatableRead,
                AL::Serializable => TxnIsolationLevel::Serializable,
            });
            Ok(LogicalPlan::SetTransaction {
                isolation_level: level,
                read_only: access_mode
                    .map(|m| matches!(m, ultrasql_parser::ast::TransactionAccessMode::ReadOnly)),
                schema: Schema::empty(),
            })
        }
        Statement::SetVar(s) => bind_set_var(s),
        Statement::SetRole(s) => bind_set_role(s),
        Statement::Listen { channel, .. } => Ok(LogicalPlan::Listen {
            // Unquoted identifiers are case-folded at parse time, so the
            // value already lower-cases. Quoted identifiers retain their
            // source case; we trust the parser's policy.
            channel: channel.value.clone(),
            schema: Schema::empty(),
        }),
        Statement::Notify {
            channel, payload, ..
        } => Ok(LogicalPlan::Notify {
            channel: channel.value.clone(),
            payload: payload.clone(),
            schema: Schema::empty(),
        }),
        Statement::Unlisten { channel, .. } => Ok(LogicalPlan::Unlisten {
            channel: channel.as_ref().map(|id| id.value.clone()),
            schema: Schema::empty(),
        }),
        _ => Err(PlanError::NotSupported("statement variant")),
    }
}

fn bind_describe(
    stmt: &DescribeStmt,
    catalog: &dyn Catalog,
    scope: &mut ScopeStack,
) -> Result<LogicalPlan, PlanError> {
    let target = match &stmt.target {
        AstDescribeTarget::Query(query) => {
            let plan = bind_select(query, catalog, scope)?;
            LogicalDescribeTarget::Query {
                query_schema: plan.schema().clone(),
            }
        }
        AstDescribeTarget::Object { kind, name } => {
            let table_name = object_name_simple(name);
            let resolved = lookup_table_reference(catalog, name)?;
            let kind = match kind {
                AstDescribeObjectKind::Any => LogicalDescribeObjectKind::Any,
                AstDescribeObjectKind::Table => LogicalDescribeObjectKind::Table,
                AstDescribeObjectKind::View => LogicalDescribeObjectKind::View,
            };
            LogicalDescribeTarget::Object {
                name: table_name,
                namespace: resolved.meta.schema_name,
                kind,
                object_schema: resolved.meta.schema,
            }
        }
    };

    Ok(LogicalPlan::Describe {
        target,
        schema: describe_output_schema()?,
    })
}

fn describe_output_schema() -> Result<Schema, PlanError> {
    Schema::new([
        Field::required("column_name", DataType::Text { max_len: None }),
        Field::required("data_type", DataType::Text { max_len: None }),
        Field::required("nullable", DataType::Bool),
        Field::required("source_schema", DataType::Text { max_len: None }),
        Field::required("source_object", DataType::Text { max_len: None }),
        Field::required("source_kind", DataType::Text { max_len: None }),
    ])
    .map_err(|err| PlanError::TypeMismatch(format!("DESCRIBE schema: {err}")))
}

fn bind_summarize(stmt: &SummarizeStmt, catalog: &dyn Catalog) -> Result<LogicalPlan, PlanError> {
    let table = object_name_simple(&stmt.name);
    let resolved = lookup_table_reference(catalog, &stmt.name)?;
    Ok(LogicalPlan::Summarize {
        table,
        namespace: resolved.meta.schema_name,
        target_schema: resolved.meta.schema,
        schema: summarize_output_schema()?,
    })
}

fn bind_export_database(stmt: &ExportDatabaseStmt) -> Result<LogicalPlan, PlanError> {
    if stmt.path.trim().is_empty() {
        return Err(PlanError::TypeMismatch(
            "EXPORT DATABASE path cannot be empty".to_owned(),
        ));
    }
    Ok(LogicalPlan::ExportDatabase {
        path: stmt.path.clone(),
        schema: Schema::empty(),
    })
}

fn bind_import_database(stmt: &ImportDatabaseStmt) -> Result<LogicalPlan, PlanError> {
    if stmt.path.trim().is_empty() {
        return Err(PlanError::TypeMismatch(
            "IMPORT DATABASE path cannot be empty".to_owned(),
        ));
    }
    Ok(LogicalPlan::ImportDatabase {
        path: stmt.path.clone(),
        schema: Schema::empty(),
    })
}

fn summarize_output_schema() -> Result<Schema, PlanError> {
    Schema::new([
        Field::required("column_name", DataType::Text { max_len: None }),
        Field::required("data_type", DataType::Text { max_len: None }),
        Field::required("row_count", DataType::Int64),
        Field::required("null_count", DataType::Int64),
        Field::nullable("min", DataType::Text { max_len: None }),
        Field::nullable("max", DataType::Text { max_len: None }),
        Field::required("unique_count", DataType::Int64),
        Field::nullable("avg", DataType::Float64),
        Field::nullable("stddev", DataType::Float64),
    ])
    .map_err(|err| PlanError::TypeMismatch(format!("SUMMARIZE schema: {err}")))
}

fn bind_set_var(stmt: &SetVarStmt) -> Result<LogicalPlan, PlanError> {
    let name = stmt.name.value.to_ascii_lowercase();
    let action = match stmt.scope {
        SetScope::Session => LogicalSetVariableAction::Set,
        SetScope::Local => LogicalSetVariableAction::SetLocal,
        SetScope::Show => LogicalSetVariableAction::Show,
        SetScope::Reset => LogicalSetVariableAction::Reset,
    };
    let value = match (&action, &stmt.value) {
        (LogicalSetVariableAction::Set | LogicalSetVariableAction::SetLocal, SetValue::Default)
        | (LogicalSetVariableAction::Reset, _) => None,
        (LogicalSetVariableAction::Show, _) => None,
        (
            LogicalSetVariableAction::Set | LogicalSetVariableAction::SetLocal,
            SetValue::Values(v),
        ) => {
            if v.len() == 1 {
                Some(set_value_to_string(&v[0])?)
            } else if name == "search_path" || name == "datestyle" {
                Some(
                    v.iter()
                        .map(set_search_path_value_to_string)
                        .collect::<Result<Vec<_>, _>>()?
                        .join(", "),
                )
            } else {
                return Err(PlanError::NotSupported("SET with multiple values"));
            }
        }
    };
    let schema = if action == LogicalSetVariableAction::Show {
        Schema::new([Field::required(
            name.clone(),
            DataType::Text { max_len: None },
        )])
        .map_err(|err| PlanError::TypeMismatch(format!("SHOW schema: {err}")))?
    } else {
        Schema::empty()
    };
    Ok(LogicalPlan::SetVariable {
        name,
        action,
        value,
        schema,
    })
}

fn bind_set_role(stmt: &SetRoleStmt) -> Result<LogicalPlan, PlanError> {
    Ok(LogicalPlan::SetRole {
        role_name: stmt
            .role
            .as_ref()
            .map(|role| role.value.to_ascii_lowercase()),
        schema: Schema::empty(),
    })
}

fn set_value_to_string(expr: &AstExpr) -> Result<String, PlanError> {
    match expr {
        AstExpr::Literal(Literal::Bool { value, .. }) => {
            Ok(if *value { "on" } else { "off" }.to_owned())
        }
        AstExpr::Literal(Literal::Integer { text, .. })
        | AstExpr::Literal(Literal::Float { text, .. }) => Ok(text.clone()),
        AstExpr::Literal(Literal::String { value, .. })
        | AstExpr::Literal(Literal::Typed { value, .. }) => Ok(value.clone()),
        AstExpr::Column { name } if name.parts.len() == 1 => Ok(name.parts[0].value.clone()),
        _ => Err(PlanError::NotSupported("SET value expression")),
    }
}

fn set_search_path_value_to_string(expr: &AstExpr) -> Result<String, PlanError> {
    match expr {
        AstExpr::Column { name } if name.parts.len() == 1 && name.parts[0].quoted => {
            Ok(quote_identifier(&name.parts[0].value))
        }
        _ => set_value_to_string(expr),
    }
}

fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

/// Bind an `EXPLAIN [ANALYZE] [(FORMAT TEXT|JSON)] stmt`.
fn bind_explain(
    stmt: &ExplainStmt,
    catalog: &dyn Catalog,
    scope: &mut ScopeStack,
) -> Result<LogicalPlan, PlanError> {
    let inner = match &*stmt.statement {
        Statement::Select(s) => bind_select(s, catalog, scope)?,
        Statement::Insert(s) => bind_insert(s, catalog, scope)?,
        Statement::Update(s) => bind_update(s, catalog, scope)?,
        Statement::Delete(s) => bind_delete(s, catalog, scope)?,
        _ => return Err(PlanError::NotSupported("EXPLAIN of this statement kind")),
    };
    let format = match stmt.format {
        AstExplainFormat::Text => ExplainFormat::Text,
        AstExplainFormat::Json => ExplainFormat::Json,
    };
    let schema = Schema::new([Field::nullable(
        "QUERY PLAN",
        DataType::Text { max_len: None },
    )])
    .map_err(|e| PlanError::TypeMismatch(format!("EXPLAIN schema: {e}")))?;
    Ok(LogicalPlan::Explain {
        analyze: stmt.analyze,
        format,
        input: Box::new(inner),
        schema,
    })
}

// ---------------------------------------------------------------------------
// SELECT
// ---------------------------------------------------------------------------

/// A per-column scope entry used for wildcard expansion and qualified
/// column resolution.
///
/// Each entry tracks which table qualifier (alias or table name) owns the
/// field, along with the field's position in the combined FROM schema.
struct ScopeEntry {
    /// Table qualifier (alias or lowercased table name). Empty string
    /// for anonymous derived tables without a qualifier.
    qualifier: String,
    /// 0-based index into the full FROM schema.
    field_index: usize,
    /// The field itself (type + name).
    field: Field,
}

fn schema_for_qualified_binding(
    input: &Schema,
    from_scope: &[ScopeEntry],
) -> Result<Schema, PlanError> {
    if from_scope.is_empty() {
        return Ok(input.clone());
    }

    let mut fields = input.fields().to_vec();
    for entry in from_scope {
        if entry.qualifier.is_empty() {
            continue;
        }
        let Some(field) = fields.get_mut(entry.field_index) else {
            continue;
        };
        field.name = format!("{}.{}", entry.qualifier, entry.field.name);
    }

    Schema::new(fields)
        .map_err(|e| PlanError::TypeMismatch(format!("qualified binding schema: {e}")))
}

/// Bind a `SELECT` statement.
///
/// Handles: CTEs, FROM clause (single tables, explicit joins, subqueries),
/// wildcard expansion, GROUP BY + aggregates, HAVING, set operations,
/// ORDER BY, LIMIT / OFFSET.
pub(super) fn bind_select(
    select: &SelectStmt,
    catalog: &dyn Catalog,
    scope: &mut ScopeStack,
) -> Result<LogicalPlan, PlanError> {
    let mut cte_catalog: Vec<(String, Schema)> = Vec::new();
    let mut cte_plans: Vec<(String, bool, LogicalPlan)> = Vec::new();
    for cte in &select.ctes {
        let cte_name = cte.name.value.to_ascii_lowercase();
        let cte_plan = if cte.recursive {
            bind_recursive_cte(
                &cte_name,
                &cte.query,
                &cte.column_aliases,
                catalog,
                &cte_catalog,
                scope,
            )?
        } else {
            bind_select_with_ctes(&cte.query, catalog, &cte_catalog, scope)?
        };
        let cte_schema = cte_plan.schema().clone();
        let cte_schema = if cte.column_aliases.is_empty() {
            cte_schema
        } else {
            apply_column_aliases(&cte_schema, &cte.column_aliases)?
        };
        cte_catalog.push((cte_name.clone(), cte_schema));
        cte_plans.push((cte_name, cte.recursive, cte_plan));
    }

    let body_select;
    let body_select_ref = if select.set_ops.is_empty() {
        select
    } else {
        body_select = select_without_set_tail_modifiers(select);
        &body_select
    };
    let mut plan = bind_select_body(body_select_ref, catalog, &cte_catalog, scope)?;
    plan = bind_set_ops_and_modifiers(plan, select, catalog, &cte_catalog, scope)?;

    for (cte_name, recursive, def_plan) in cte_plans.into_iter().rev() {
        let body_schema = plan.schema().clone();
        plan = LogicalPlan::Cte {
            name: cte_name,
            recursive,
            definition: Box::new(def_plan),
            body: Box::new(plan),
            schema: body_schema,
        };
    }

    // FOR UPDATE / FOR SHARE / FOR NO KEY UPDATE / FOR KEY SHARE
    // When multiple locking clauses are present, only the first is used
    // (PostgreSQL applies the strongest one; for v0.4 we take the first).
    if let Some(locking) = select.locking.first() {
        let strength = match locking.strength {
            AstLockStrength::Update => LockStrength::Update,
            AstLockStrength::NoKeyUpdate => LockStrength::NoKeyUpdate,
            AstLockStrength::Share => LockStrength::Share,
            AstLockStrength::KeyShare => LockStrength::KeyShare,
        };
        let wait_policy = match locking.wait_policy {
            AstLockWaitPolicy::Wait => LockWaitPolicy::Wait,
            AstLockWaitPolicy::NoWait => LockWaitPolicy::NoWait,
            AstLockWaitPolicy::SkipLocked => LockWaitPolicy::SkipLocked,
        };
        let schema = plan.schema().clone();
        plan = LogicalPlan::LockRows {
            input: Box::new(plan),
            strength,
            wait_policy,
            schema,
        };
    }

    Ok(plan)
}

/// Bind a `WITH RECURSIVE` CTE definition.
///
/// PostgreSQL/SQL semantics: the CTE's body must be a top-level
/// `UNION` (or `UNION ALL`) of an *anchor* (which cannot reference
/// the CTE itself) and a *recursive term* (which may). The binder
/// here enforces that shape, binds the anchor first to derive a
/// schema for the CTE, then binds the recursive term against an
/// augmented catalog that exposes the CTE name with the anchor's
/// schema. Both halves are joined back into a single
/// `LogicalPlan::SetOp` so the lowerer's recursive-fixpoint code
/// sees the same `Cte { definition: SetOp { left, right }, .. }`
/// shape it consumes.
pub(super) fn bind_recursive_cte(
    cte_name: &str,
    query: &SelectStmt,
    column_aliases: &[ultrasql_parser::ast::Identifier],
    catalog: &dyn Catalog,
    cte_catalog: &[(String, Schema)],
    scope: &mut ScopeStack,
) -> Result<LogicalPlan, PlanError> {
    if query.set_ops.is_empty() {
        return Err(PlanError::TypeMismatch(format!(
            "WITH RECURSIVE \"{cte_name}\" must be `anchor UNION [ALL] recursive_term`"
        )));
    }
    let tail = &query.set_ops[0];
    if !matches!(tail.op, ultrasql_parser::ast::SetOp::Union) {
        return Err(PlanError::TypeMismatch(format!(
            "WITH RECURSIVE \"{cte_name}\" requires UNION (not INTERSECT or EXCEPT)"
        )));
    }
    if query.set_ops.len() != 1 {
        return Err(PlanError::TypeMismatch(format!(
            "WITH RECURSIVE \"{cte_name}\" must have exactly one UNION between anchor and recursive term"
        )));
    }

    // Bind the anchor. The anchor cannot reference the CTE itself,
    // so the catalog stays untouched.
    let anchor_select = SelectStmt {
        ctes: query.ctes.clone(),
        projection: query.projection.clone(),
        from: query.from.clone(),
        r#where: query.r#where.clone(),
        group_by: query.group_by.clone(),
        having: query.having.clone(),
        order_by: query.order_by.clone(),
        limit: query.limit.clone(),
        offset: query.offset.clone(),
        distinct: query.distinct.clone(),
        set_ops: Vec::new(),
        locking: query.locking.clone(),
        span: query.span,
    };
    let anchor_plan = bind_select_with_ctes(&anchor_select, catalog, cte_catalog, scope)?;
    let anchor_schema = anchor_plan.schema().clone();

    // Apply column aliases (if any) to the schema the recursive term
    // will see for `cte_name`.
    let exposed_schema = if column_aliases.is_empty() {
        anchor_schema.clone()
    } else {
        apply_column_aliases(&anchor_schema, column_aliases)?
    };
    let mut augmented_catalog: Vec<(String, Schema)> = cte_catalog.to_vec();
    augmented_catalog.push((cte_name.to_owned(), exposed_schema));

    // Bind the recursive term against the augmented catalog.
    let recursive_term_plan =
        bind_select_with_ctes(&tail.right, catalog, &augmented_catalog, scope)?;

    // Stitch them back into the same Cte-definition shape the
    // non-recursive path produces: a `SetOp` of anchor + recursive
    // term. The fixpoint loop in the lowerer pattern-matches on this
    // shape.
    bind_set_op(anchor_plan, tail.op, tail.quantifier, recursive_term_plan)
}

/// Bind a `SelectStmt` that may reference CTEs in `cte_catalog` plus the
/// regular catalog.
pub(super) fn bind_select_with_ctes(
    select: &SelectStmt,
    catalog: &dyn Catalog,
    cte_catalog: &[(String, Schema)],
    scope: &mut ScopeStack,
) -> Result<LogicalPlan, PlanError> {
    let mut nested_cte_catalog: Vec<(String, Schema)> = cte_catalog.to_vec();
    let mut nested_cte_plans: Vec<(String, bool, LogicalPlan)> = Vec::new();
    for cte in &select.ctes {
        let cte_name = cte.name.value.to_ascii_lowercase();
        let cte_plan = if cte.recursive {
            bind_recursive_cte(
                &cte_name,
                &cte.query,
                &cte.column_aliases,
                catalog,
                &nested_cte_catalog,
                scope,
            )?
        } else {
            bind_select_with_ctes(&cte.query, catalog, &nested_cte_catalog, scope)?
        };
        let cte_schema = cte_plan.schema().clone();
        let cte_schema = if cte.column_aliases.is_empty() {
            cte_schema
        } else {
            apply_column_aliases(&cte_schema, &cte.column_aliases)?
        };
        nested_cte_catalog.push((cte_name.clone(), cte_schema));
        nested_cte_plans.push((cte_name, cte.recursive, cte_plan));
    }

    let body_select;
    let body_select_ref = if select.set_ops.is_empty() {
        select
    } else {
        body_select = select_without_set_tail_modifiers(select);
        &body_select
    };
    let mut plan = bind_select_body(body_select_ref, catalog, &nested_cte_catalog, scope)?;
    plan = bind_set_ops_and_modifiers(plan, select, catalog, &nested_cte_catalog, scope)?;

    for (cte_name, recursive, def_plan) in nested_cte_plans.into_iter().rev() {
        let body_schema = plan.schema().clone();
        plan = LogicalPlan::Cte {
            name: cte_name,
            recursive,
            definition: Box::new(def_plan),
            body: Box::new(plan),
            schema: body_schema,
        };
    }

    Ok(plan)
}

fn select_without_set_tail_modifiers(select: &SelectStmt) -> SelectStmt {
    let mut stripped = select.clone();
    stripped.order_by.clear();
    stripped.limit = None;
    stripped.offset = None;
    stripped
}

fn bind_set_ops_and_modifiers(
    mut plan: LogicalPlan,
    select: &SelectStmt,
    catalog: &dyn Catalog,
    cte_catalog: &[(String, Schema)],
    scope: &mut ScopeStack,
) -> Result<LogicalPlan, PlanError> {
    if select.set_ops.is_empty() {
        return Ok(plan);
    }

    let mut final_tail_order_by = Vec::new();
    let mut final_tail_limit = None;
    let mut final_tail_offset = None;
    let tail_count = select.set_ops.len();
    for (idx, tail) in select.set_ops.iter().enumerate() {
        let mut right_select = (*tail.right).clone();
        if idx + 1 == tail_count {
            final_tail_order_by = std::mem::take(&mut right_select.order_by);
            final_tail_limit = right_select.limit.take();
            final_tail_offset = right_select.offset.take();
        }
        let right_plan = bind_select_with_ctes(&right_select, catalog, cte_catalog, scope)?;
        plan = bind_set_op(plan, tail.op, tail.quantifier, right_plan)?;
    }

    let order_by = if select.order_by.is_empty() {
        &final_tail_order_by
    } else {
        &select.order_by
    };
    let limit = select.limit.as_ref().or(final_tail_limit.as_ref());
    let offset = select.offset.as_ref().or(final_tail_offset.as_ref());
    bind_set_result_modifiers(plan, order_by, limit, offset, catalog, cte_catalog, scope)
}

fn bind_set_result_modifiers(
    mut plan: LogicalPlan,
    order_by: &[ultrasql_parser::ast::OrderItem],
    limit: Option<&AstExpr>,
    offset: Option<&AstExpr>,
    catalog: &dyn Catalog,
    cte_catalog: &[(String, Schema)],
    scope: &mut ScopeStack,
) -> Result<LogicalPlan, PlanError> {
    if !order_by.is_empty() {
        let keys = bind_order_by(order_by, plan.schema(), None, catalog, cte_catalog, scope)?;
        plan = LogicalPlan::Sort {
            input: Box::new(plan),
            keys,
        };
    }

    let limit_val = limit
        .map(|expr| bind_unsigned_literal(expr, "LIMIT"))
        .transpose()?;
    let offset_val = offset
        .map(|expr| bind_unsigned_literal(expr, "OFFSET"))
        .transpose()?
        .unwrap_or(0);
    if let Some(n) = limit_val {
        plan = LogicalPlan::Limit {
            input: Box::new(plan),
            n,
            offset: offset_val,
        };
    } else if offset_val != 0 {
        plan = LogicalPlan::Limit {
            input: Box::new(plan),
            n: u64::MAX,
            offset: offset_val,
        };
    }
    Ok(plan)
}

/// Apply a list of column alias overrides to a schema.
fn apply_column_aliases(
    schema: &Schema,
    aliases: &[ultrasql_parser::ast::Identifier],
) -> Result<Schema, PlanError> {
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| {
            let name = aliases
                .get(i)
                .map_or_else(|| f.name.clone(), |a| a.value.clone());
            Field {
                name,
                data_type: f.data_type.clone(),
                nullable: f.nullable,
            }
        })
        .collect();
    Schema::new(fields).map_err(|e| PlanError::TypeMismatch(format!("CTE column aliases: {e}")))
}

/// Resolve the common supertype of two corresponding set-op branch column
/// types, following PostgreSQL's `UNION`/`INTERSECT`/`EXCEPT` type
/// resolution (the `select_common_type` algorithm).
///
/// The pair must belong to the same type *category*. The supertype is the
/// preferred type of that category, or the one to which the other branch's
/// type implicitly coerces. Every branch column is later cast to this type
/// (see [`cast_set_op_side`]) so deduplication / matching happens in the
/// common physical type — without this, e.g. `DATE INTERSECT TIMESTAMP`
/// compares `Date(d)` against `Timestamp(us)` and never matches, silently
/// dropping rows.
///
/// # Rules
///
/// * Identical types resolve to themselves (unchanged).
/// * `NULL` / untyped-literal columns adopt the other branch's type.
/// * NUMERIC: int2/int4/int8/numeric/float resolve to the common numeric
///   supertype (decimal > float > wider integer).
/// * TEMPORAL: `date`+`timestamp` → `timestamp`; `date`/`timestamp`
///   +`timestamptz` → `timestamptz`; `date`+`timestamptz` → `timestamptz`.
///   `time`/`timetz`/`interval` only unify with themselves.
/// * STRING: `char(n)` / `varchar(n)` / `text` (any mix) → `text`, the
///   preferred string type. Comparison then follows PG `text` semantics.
/// * NETWORK: `inet`+`cidr` → `inet` (cidr implicitly casts to inet).
///   `macaddr`/`macaddr8` only unify with themselves.
///
/// Returns [`PlanError::TypeMismatch`] (SQLSTATE 42804, datatype_mismatch)
/// when the two types share no common supertype, mirroring PostgreSQL's
/// `UNION/INTERSECT/EXCEPT types <a> and <b> cannot be matched` error.
fn setop_common_type(left: &DataType, right: &DataType) -> Result<DataType, PlanError> {
    // 1. Identical types — including identical typmods — pass through.
    if left == right {
        return Ok(left.clone());
    }
    // 2. An untyped NULL / unknown-literal column adopts the other side.
    if matches!(left, DataType::Null) {
        return Ok(right.clone());
    }
    if matches!(right, DataType::Null) {
        return Ok(left.clone());
    }

    // 3. NUMERIC: preserve the mixed-width behaviour (decimal > float >
    //    wider integer).
    if left.is_numeric() && right.is_numeric() {
        return left
            .numeric_join(right)
            .map_err(|_| setop_mismatch(left, right));
    }

    // 4. TEMPORAL: date/timestamp/timestamptz promote to the widest member.
    if let Some(ty) = temporal_common_type(left, right) {
        return Ok(ty);
    }

    // 5. STRING: char/varchar/text collapse to text (the preferred type).
    if left.is_textlike() && right.is_textlike() {
        return Ok(DataType::Text { max_len: None });
    }

    // 6. NETWORK: inet + cidr promote to inet (cidr -> inet is implicit).
    if left.is_ip_network() && right.is_ip_network() {
        return Ok(DataType::Inet);
    }

    // 7. OID aliases (oid/regclass/regtype) and oid-alias/integer mixes
    //    already compare same-width as oid; keep the left (oid-alias) type.
    if left.is_oid_alias() && (right.is_oid_alias() || right.is_integer()) {
        return Ok(left.clone());
    }
    if right.is_oid_alias() && left.is_integer() {
        return Ok(right.clone());
    }

    // 8. Otherwise the two branches have no common supertype: PG raises
    //    `... types <a> and <b> cannot be matched` (datatype_mismatch).
    Err(setop_mismatch(left, right))
}

/// The PostgreSQL `cannot be matched` error for two un-unifiable set-op
/// branch column types (SQLSTATE 42804 via [`PlanError::TypeMismatch`]).
fn setop_mismatch(left: &DataType, right: &DataType) -> PlanError {
    PlanError::TypeMismatch(format!(
        "UNION/INTERSECT/EXCEPT types {left} and {right} cannot be matched"
    ))
}

/// Common supertype for two temporal types, or `None` when they do not
/// promote (e.g. `time` vs `date`). Implements PG's date/timestamp
/// promotion lattice: `date` < `timestamp` < `timestamptz`, with
/// `timestamptz` absorbing either narrower instant type.
fn temporal_common_type(left: &DataType, right: &DataType) -> Option<DataType> {
    use DataType::{Date, Timestamp, TimestampTz};
    match (left, right) {
        (TimestampTz, Date | Timestamp | TimestampTz) | (Date | Timestamp, TimestampTz) => {
            Some(TimestampTz)
        }
        (Timestamp, Date | Timestamp) | (Date, Timestamp) => Some(Timestamp),
        _ => None,
    }
}

fn bind_set_op(
    left: LogicalPlan,
    op: SetOp,
    quantifier: SetQuantifier,
    right: LogicalPlan,
) -> Result<LogicalPlan, PlanError> {
    let left_arity = left.schema().len();
    let right_arity = right.schema().len();
    if left_arity != right_arity {
        return Err(PlanError::TypeMismatch(format!(
            "set operation: left side has {left_arity} columns, right side has {right_arity}"
        )));
    }

    let fields: Result<Vec<Field>, PlanError> = left
        .schema()
        .fields()
        .iter()
        .zip(right.schema().fields().iter())
        .map(|(lf, rf)| {
            let out_ty = setop_common_type(&lf.data_type, &rf.data_type)?;
            Ok(Field::nullable(lf.name.clone(), out_ty))
        })
        .collect();
    let schema = Schema::new_with_duplicate_names(fields?);

    // Each side may carry a column whose physical width differs from the
    // unified `out_ty` (e.g. an `int4` column unified with `int8` into
    // `int8`). Wrap such a side in a `Project` that casts every differing
    // column to `out_ty`. Without this both correctness paths break:
    //   * `UNION`/`UNION ALL` ERROR in the columnar batch builder, which
    //     refuses to stack two children of different physical width
    //     ("expected Int64, got Int32"); and
    //   * `INTERSECT`/`EXCEPT` SILENTLY RETURN WRONG ROWS, because the
    //     executor's `RowKey`/`Value` equality has no cross-width arm
    //     (`Int32(1) != Int64(1)`), so equal values across widths never
    //     match and rows are dropped with no error.
    // Casting both sides to `out_ty` makes every comparison same-width.
    let left = cast_set_op_side(left, &schema);
    let right = cast_set_op_side(right, &schema);

    let logical_op = match op {
        SetOp::Union => LogicalSetOp::Union,
        SetOp::Intersect => LogicalSetOp::Intersect,
        SetOp::Except => LogicalSetOp::Except,
    };
    let logical_q = match quantifier {
        SetQuantifier::All => LogicalSetQuantifier::All,
        SetQuantifier::Distinct => LogicalSetQuantifier::Distinct,
    };

    Ok(LogicalPlan::SetOp {
        op: logical_op,
        quantifier: logical_q,
        left: Box::new(left),
        right: Box::new(right),
        schema,
    })
}

/// Wrap `side` in a `Project` that casts every column whose type differs
/// from the matching column in the unified set-op `target` schema.
///
/// When every column already matches `target` the side is returned
/// unchanged, so same-width set operations (the common case) gain no
/// extra `Project` node and the plan does not bloat.
fn cast_set_op_side(side: LogicalPlan, target: &Schema) -> LogicalPlan {
    let side_schema = side.schema();
    let needs_cast = side_schema
        .fields()
        .iter()
        .zip(target.fields().iter())
        .any(|(sf, tf)| sf.data_type != tf.data_type);
    if !needs_cast {
        return side;
    }

    let exprs: Vec<(ScalarExpr, String)> = side_schema
        .fields()
        .iter()
        .zip(target.fields().iter())
        .enumerate()
        .map(|(index, (sf, tf))| {
            let column = ScalarExpr::Column {
                name: sf.name.clone(),
                index,
                data_type: sf.data_type.clone(),
            };
            // Keep the side's own column name; only the type is unified.
            let out_name = sf.name.clone();
            if sf.data_type == tf.data_type {
                return (column, out_name);
            }
            // A `Null`-typed column carries only NULLs, which compare and
            // hash identically at any width, so `bind_runtime_cast`
            // declines it. Re-type the column reference to the unified
            // type so the columnar batch builder still aligns widths.
            let casted = expr_bind::bind_runtime_cast(column.clone(), &tf.data_type, &sf.data_type)
                .unwrap_or(ScalarExpr::Column {
                    name: sf.name.clone(),
                    index,
                    data_type: tf.data_type.clone(),
                });
            (casted, out_name)
        })
        .collect();

    let projected_schema = Schema::new_with_duplicate_names(
        side_schema
            .fields()
            .iter()
            .zip(target.fields().iter())
            .map(|(sf, tf)| Field::nullable(sf.name.clone(), tf.data_type.clone())),
    );

    LogicalPlan::Project {
        input: Box::new(side),
        exprs,
        schema: projected_schema,
    }
}

/// The core `SELECT` body binding: FROM → WHERE → GROUP BY → HAVING →
/// SELECT list → ORDER BY → LIMIT/OFFSET.
#[allow(clippy::too_many_lines)]
fn bind_select_body(
    select: &SelectStmt,
    catalog: &dyn Catalog,
    cte_catalog: &[(String, Schema)],
    scope: &mut ScopeStack,
) -> Result<LogicalPlan, PlanError> {
    let is_distinct = matches!(select.distinct, Distinct::Distinct);
    let distinct_on = match &select.distinct {
        Distinct::DistinctOn(exprs) => Some(exprs.as_slice()),
        _ => None,
    };

    let (mut plan, from_scope) = bind_from(&select.from, catalog, cte_catalog, scope)?;

    if let Some(pred_ast) = &select.r#where {
        let binding_schema = schema_for_qualified_binding(plan.schema(), &from_scope)?;
        let pred = bind_expr_with_ctes(pred_ast, &binding_schema, catalog, cte_catalog, scope)?;
        let pred_ty = pred.data_type();
        if pred_ty != DataType::Bool && pred_ty != DataType::Null {
            return Err(PlanError::TypeMismatch(format!(
                "WHERE predicate must be boolean, got {pred_ty}"
            )));
        }
        plan = LogicalPlan::Filter {
            input: Box::new(plan),
            predicate: pred,
        };
    }

    // Window-function lift: pull every `Expr::Call { over: Some(_) }`
    // out of the projection, wrap the FROM/WHERE plan in one
    // [`LogicalPlan::Window`] per call, and rewrite the projection so
    // each call site becomes a [`Expr::Column`] reference to the
    // synthetic `"$wn_N"` column. Pre-aggregate / pre-projection so
    // the existing aggregate detection and projection binding paths
    // see a normal scalar projection.
    let (projection_after_window, window_extractions) =
        window::extract_window_calls(&select.projection)?;
    let select_for_binding = if window_extractions.is_empty() {
        select.clone()
    } else {
        let mut cloned = select.clone();
        cloned.projection = projection_after_window;
        plan = window::apply_window_extractions(
            plan,
            window_extractions,
            catalog,
            cte_catalog,
            scope,
        )?;
        cloned
    };
    let select = &select_for_binding;

    let has_group_by = !select.group_by.is_empty();
    let has_aggregates = select.projection.iter().any(projection_item_has_aggregate);
    let having_has_agg = select.having.as_ref().is_some_and(expr_has_aggregate);

    if has_group_by || has_aggregates || having_has_agg {
        plan = bind_aggregate(plan, select, &from_scope, catalog, cte_catalog, scope)?;
        if let Some(having_ast) = &select.having {
            let pred = bind_expr_with_ctes(having_ast, plan.schema(), catalog, cte_catalog, scope)?;
            let pred_ty = pred.data_type();
            if pred_ty != DataType::Bool && pred_ty != DataType::Null {
                return Err(PlanError::TypeMismatch(format!(
                    "HAVING predicate must be boolean, got {pred_ty}"
                )));
            }
            plan = LogicalPlan::Filter {
                input: Box::new(plan),
                predicate: pred,
            };
        }
        let projected = bind_projection_agg(
            &select.projection,
            plan.schema(),
            catalog,
            cte_catalog,
            scope,
        )?;
        let projection_input_schema = plan.schema().clone();
        let proj_fields: Vec<Field> = projected
            .iter()
            .map(|(e, name)| projection_field(e, name, &projection_input_schema))
            .collect();
        let proj_schema = Schema::new_with_duplicate_names(proj_fields);

        plan = LogicalPlan::Project {
            input: Box::new(plan),
            exprs: projected,
            schema: proj_schema,
        };

        let order_input_schema = match &plan {
            LogicalPlan::Project { input, .. } => input.schema().clone(),
            _ => plan.schema().clone(),
        };
        if let Some(on_exprs) = distinct_on {
            plan = bind_distinct_on(
                plan,
                on_exprs,
                &select.order_by,
                &order_input_schema,
                catalog,
                cte_catalog,
                scope,
            )?;
        } else {
            plan = bind_order_by_around_projection(
                plan,
                &select.order_by,
                catalog,
                cte_catalog,
                scope,
            )?;
        }
    } else {
        let projection_input_schema = plan.schema().clone();
        let projected = bind_projection_with_scope(
            &select.projection,
            &projection_input_schema,
            &from_scope,
            catalog,
            cte_catalog,
            scope,
        )?;
        let proj_fields: Vec<Field> = projected
            .iter()
            .map(|(e, name)| projection_field(e, name, &projection_input_schema))
            .collect();
        let proj_schema = Schema::new_with_duplicate_names(proj_fields);

        plan = LogicalPlan::Project {
            input: Box::new(plan),
            exprs: projected,
            schema: proj_schema,
        };

        let order_input_schema = schema_for_qualified_binding(
            match &plan {
                LogicalPlan::Project { input, .. } => input.schema(),
                _ => plan.schema(),
            },
            &from_scope,
        )?;
        if let Some(on_exprs) = distinct_on {
            plan = bind_distinct_on(
                plan,
                on_exprs,
                &select.order_by,
                &order_input_schema,
                catalog,
                cte_catalog,
                scope,
            )?;
        } else {
            plan = bind_order_by_around_projection_with_input_schema(
                plan,
                &select.order_by,
                &order_input_schema,
                catalog,
                cte_catalog,
                scope,
            )?;
        }
    }

    if is_distinct {
        let proj_schema = plan.schema().clone();
        let group_by: Vec<ScalarExpr> = proj_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(idx, field)| ScalarExpr::Column {
                name: field.name.clone(),
                index: idx,
                data_type: field.data_type.clone(),
            })
            .collect();
        plan = LogicalPlan::Aggregate {
            input: Box::new(plan),
            group_by,
            aggregates: Vec::new(),
            schema: proj_schema,
        };
    }

    let limit_val = match &select.limit {
        Some(e) => Some(bind_unsigned_literal(e, "LIMIT")?),
        None => None,
    };
    let offset_val = match &select.offset {
        Some(e) => bind_unsigned_literal(e, "OFFSET")?,
        None => 0,
    };
    if let Some(n) = limit_val {
        plan = LogicalPlan::Limit {
            input: Box::new(plan),
            n,
            offset: offset_val,
        };
    } else if offset_val != 0 {
        plan = LogicalPlan::Limit {
            input: Box::new(plan),
            n: u64::MAX,
            offset: offset_val,
        };
    }

    Ok(plan)
}

fn projection_field(expr: &ScalarExpr, name: &str, input: &Schema) -> Field {
    let nullable = match expr {
        ScalarExpr::Column { index, .. } => input
            .fields()
            .get(*index)
            .is_none_or(|field| field.nullable),
        ScalarExpr::Literal { value, .. } => matches!(value, Value::Null),
        ScalarExpr::IsNull { .. } => false,
        _ => true,
    };
    Field {
        name: name.to_owned(),
        data_type: expr.data_type(),
        nullable,
    }
}

fn bind_order_by_around_projection(
    plan: LogicalPlan,
    order_by: &[ultrasql_parser::ast::OrderItem],
    catalog: &dyn Catalog,
    cte_catalog: &[(String, Schema)],
    scope: &mut ScopeStack,
) -> Result<LogicalPlan, PlanError> {
    let input_schema = match &plan {
        LogicalPlan::Project { input, .. } => input.schema().clone(),
        _ => plan.schema().clone(),
    };
    bind_order_by_around_projection_with_input_schema(
        plan,
        order_by,
        &input_schema,
        catalog,
        cte_catalog,
        scope,
    )
}

fn bind_order_by_around_projection_with_input_schema(
    plan: LogicalPlan,
    order_by: &[ultrasql_parser::ast::OrderItem],
    input_schema: &Schema,
    catalog: &dyn Catalog,
    cte_catalog: &[(String, Schema)],
    scope: &mut ScopeStack,
) -> Result<LogicalPlan, PlanError> {
    if order_by.is_empty() {
        return Ok(plan);
    }
    let LogicalPlan::Project {
        input,
        exprs,
        schema,
    } = plan
    else {
        let sort_keys = bind_order_by(order_by, input_schema, None, catalog, cte_catalog, scope)?;
        return Ok(LogicalPlan::Sort {
            input: Box::new(plan),
            keys: sort_keys,
        });
    };

    match bind_order_by(
        order_by,
        input_schema,
        Some(exprs.as_slice()),
        catalog,
        cte_catalog,
        scope,
    ) {
        Ok(sort_keys) => {
            // A projection containing a subquery is NOT order-preserving:
            // decorrelation rewrites the subquery into a (hash) join that
            // discards input row order, so a `Sort` pushed *below* the
            // projection would be silently dropped and `ORDER BY` violated.
            // Lift the `Sort` above the projection instead, binding the keys
            // against the OUTPUT schema. If a sort key is not an output column
            // we cannot lift it, so we keep the below-projection form — a rare
            // residual; the common ORDER-BY-a-selected-column case is fixed.
            if exprs.iter().any(|(e, _)| e.contains_subquery()) {
                if let Ok(output_keys) =
                    bind_order_by(order_by, &schema, None, catalog, cte_catalog, scope)
                {
                    let projected = LogicalPlan::Project {
                        input,
                        exprs,
                        schema,
                    };
                    return Ok(LogicalPlan::Sort {
                        input: Box::new(projected),
                        keys: output_keys,
                    });
                }
            }
            let sorted_input = if sort_keys.is_empty() {
                input
            } else {
                Box::new(LogicalPlan::Sort {
                    input,
                    keys: sort_keys,
                })
            };
            Ok(LogicalPlan::Project {
                input: sorted_input,
                exprs,
                schema,
            })
        }
        Err(PlanError::ColumnNotFound(_) | PlanError::Ambiguous(_)) => {
            let projected = LogicalPlan::Project {
                input,
                exprs,
                schema: schema.clone(),
            };
            let sort_keys = bind_order_by(order_by, &schema, None, catalog, cte_catalog, scope)?;
            Ok(LogicalPlan::Sort {
                input: Box::new(projected),
                keys: sort_keys,
            })
        }
        Err(error) => Err(error),
    }
}

/// Bind `SELECT DISTINCT ON (e1, …) … [ORDER BY …]`.
///
/// PostgreSQL semantics:
///
/// * Keep the **first** row of each group of rows sharing the same values for
///   the ON keys `(e1, …)`, where "first" is fixed by `ORDER BY`.
/// * The ON keys must be a **prefix** of `ORDER BY`; otherwise raise
///   `SELECT DISTINCT ON expressions must match initial ORDER BY expressions`
///   (SQLSTATE 42P10).
/// * Without `ORDER BY` the chosen row per group is implementation-defined; we
///   sort by the ON keys alone (ascending, NULLs last) so the result is
///   deterministic.
///
/// `plan` is the already-projected query (a [`LogicalPlan::Project`] in the
/// common case). The ON keys and `ORDER BY` keys resolve against
/// `input_schema` — the pre-projection schema, matching how `ORDER BY` keys
/// not in the select list are bound. The resulting shape is
/// `Project(DistinctOn(Sort(input)))`: the sort orders rows so the per-group
/// first row is well-defined, the [`LogicalPlan::DistinctOn`] dedup emits one
/// row per ON-key group, and the projection sits on top so an ON key need not
/// appear in the select list.
fn bind_distinct_on(
    plan: LogicalPlan,
    on_exprs: &[AstExpr],
    order_by: &[ultrasql_parser::ast::OrderItem],
    input_schema: &Schema,
    catalog: &dyn Catalog,
    cte_catalog: &[(String, Schema)],
    scope: &mut ScopeStack,
) -> Result<LogicalPlan, PlanError> {
    // Split off the top projection (the common case) so the dedup can sit
    // beneath it; `None` covers the rare non-projected shape.
    let (sort_input, proj) = match plan {
        LogicalPlan::Project {
            input,
            exprs,
            schema,
        } => (input, Some(ProjectParts { exprs, schema })),
        other => (Box::new(other), None),
    };
    let proj_exprs = proj.as_ref().map(|p| p.exprs.as_slice());

    // Bind the ON keys. Route them through the ORDER-BY binder (as synthetic
    // ascending items) so positional and projection-alias references resolve
    // exactly as they do in `ORDER BY` — this lets `DISTINCT ON (1)` match
    // `ORDER BY 1`.
    let on_items: Vec<ultrasql_parser::ast::OrderItem> = on_exprs
        .iter()
        .map(|expr| ultrasql_parser::ast::OrderItem {
            expr: expr.clone(),
            direction: SortDirection::Asc,
            nulls: NullsOrder::Default,
            span: Span::default(),
        })
        .collect();
    let on_keys: Vec<ScalarExpr> = bind_order_by(
        &on_items,
        input_schema,
        proj_exprs,
        catalog,
        cte_catalog,
        scope,
    )?
    .into_iter()
    .map(|key| key.expr)
    .collect();

    // Bind the real ORDER BY and enforce the prefix rule.
    let order_keys = bind_order_by(
        order_by,
        input_schema,
        proj_exprs,
        catalog,
        cte_catalog,
        scope,
    )?;
    if !order_keys.is_empty() {
        let prefix_matches = order_keys.len() >= on_keys.len()
            && on_keys
                .iter()
                .zip(order_keys.iter())
                .all(|(on, key)| *on == key.expr);
        if !prefix_matches {
            return Err(PlanError::DistinctOnOrderByMismatch(
                "SELECT DISTINCT ON expressions must match initial ORDER BY expressions".to_owned(),
            ));
        }
    }

    // Sort keys: the full ORDER BY (which begins with the ON keys when present)
    // or, without ORDER BY, the ON keys alone (ascending, NULLs last) so the
    // per-group first row is deterministic.
    let sort_keys = if order_keys.is_empty() {
        on_keys
            .iter()
            .map(|expr| SortKey {
                expr: expr.clone(),
                asc: true,
                nulls_first: false,
            })
            .collect()
    } else {
        order_keys
    };

    let mut current = if sort_keys.is_empty() {
        sort_input
    } else {
        Box::new(LogicalPlan::Sort {
            input: sort_input,
            keys: sort_keys,
        })
    };
    current = Box::new(LogicalPlan::DistinctOn {
        input: current,
        on_keys,
    });

    Ok(match proj {
        Some(ProjectParts { exprs, schema }) => LogicalPlan::Project {
            input: current,
            exprs,
            schema,
        },
        None => *current,
    })
}

/// The pieces of a [`LogicalPlan::Project`] lifted off so a `DISTINCT ON`
/// dedup can be spliced beneath it.
struct ProjectParts {
    exprs: Vec<(ScalarExpr, String)>,
    schema: Schema,
}
