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

#[allow(unused_imports)] // Value is used in binder/tests.rs via `use super::*`
use ultrasql_core::{DataType, Field, Schema, Value};
#[allow(unused_imports)] // BinaryOp and UnaryOp are used in binder/tests.rs via `use super::*`
use ultrasql_parser::ast::{
    BinaryOp, Distinct, ExplainFormat as AstExplainFormat, ExplainStmt, Expr as AstExpr, Literal,
    LockStrength as AstLockStrength, LockWaitPolicy as AstLockWaitPolicy, SelectStmt, SetOp,
    SetQuantifier, SetScope, SetValue, SetVarStmt, Statement, UnaryOp,
};

use crate::catalog::Catalog;
use crate::error::PlanError;
use crate::expr::ScalarExpr;
use crate::plan::{
    AggregateFunc, ConflictTarget, ExplainFormat, LockStrength, LockWaitPolicy,
    LogicalAggregateExpr, LogicalAlterTableAction, LogicalJoinCondition, LogicalJoinType,
    LogicalOnConflict, LogicalPlan, LogicalSetOp, LogicalSetQuantifier, LogicalSetVariableAction,
    SortKey, TxnIsolationLevel,
};
use crate::scope::{ScopeFrame, ScopeStack};

// Submodules — each file stays under the 600-line ceiling.
mod aggregate;
mod ddl;
mod dml;
mod expr_bind;
mod expr_type;
mod from;
mod util;
mod window;

use self::aggregate::{
    bind_aggregate, bind_projection_agg, bind_projection_with_scope, derive_agg_output_name,
    expr_has_aggregate, is_aggregate_name, projection_item_has_aggregate,
};
use self::ddl::{
    bind_alter_sequence, bind_alter_table, bind_comment, bind_copy, bind_create_domain,
    bind_create_index, bind_create_materialized_view, bind_create_policy, bind_create_sequence,
    bind_create_table, bind_create_type, bind_drop_sequence, bind_drop_table, bind_truncate,
};
use self::dml::{bind_delete, bind_insert, bind_update};
use self::expr_bind::{bind_expr, bind_expr_with_ctes};
use self::from::bind_from;
use self::util::{
    bind_order_by, bind_returning, bind_unsigned_literal, build_returning_schema,
    derive_output_name, object_name_simple, plan_contains_outer_column,
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
        Statement::Truncate(s) => bind_truncate(s, catalog),
        Statement::CreateTable(s) => bind_create_table(s, catalog),
        Statement::CreateMaterializedView(s) => bind_create_materialized_view(s, catalog),
        Statement::CreateType(s) => bind_create_type(s, catalog),
        Statement::CreateDomain(s) => bind_create_domain(s, catalog),
        Statement::CreatePolicy(s) => bind_create_policy(s, catalog),
        Statement::CreateIndex(s) => bind_create_index(s, catalog),
        Statement::CreateSequence(s) => bind_create_sequence(s),
        Statement::AlterSequence(s) => bind_alter_sequence(s),
        Statement::DropSequence(s) => bind_drop_sequence(s),
        Statement::Comment(s) => bind_comment(s, catalog),
        Statement::DropTable(s) => bind_drop_table(s, catalog),
        Statement::AlterTable(s) => bind_alter_table(s, catalog),
        Statement::Copy(s) => bind_copy(s, catalog),
        Statement::Explain(s) => bind_explain(s, catalog, &mut scope),
        // Transaction-control statements have no catalog dependency: the
        // server inspects the per-session TxnState and dispatches
        // accordingly. The binder emits the corresponding LogicalPlan
        // variants so the Simple- and Extended-Query paths share a single
        // dispatch surface.
        Statement::Begin {
            isolation_level, ..
        } => {
            use ultrasql_parser::ast::AstIsolationLevel as AL;
            let level = isolation_level.map(|l| match l {
                AL::ReadCommitted => TxnIsolationLevel::ReadCommitted,
                AL::RepeatableRead => TxnIsolationLevel::RepeatableRead,
                AL::Serializable => TxnIsolationLevel::Serializable,
            });
            Ok(LogicalPlan::Begin {
                isolation_level: level,
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
            isolation_level, ..
        } => {
            use ultrasql_parser::ast::AstIsolationLevel as AL;
            let level = match isolation_level {
                AL::ReadCommitted => TxnIsolationLevel::ReadCommitted,
                AL::RepeatableRead => TxnIsolationLevel::RepeatableRead,
                AL::Serializable => TxnIsolationLevel::Serializable,
            };
            Ok(LogicalPlan::SetTransaction {
                isolation_level: level,
                schema: Schema::empty(),
            })
        }
        Statement::SetVar(s) => bind_set_var(s),
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
            if v.len() != 1 {
                return Err(PlanError::NotSupported("SET with multiple values"));
            }
            Some(set_value_to_string(&v[0])?)
        }
    };
    let schema = if action == LogicalSetVariableAction::Show {
        Schema::new([Field::required(
            name.clone(),
            DataType::Text { max_len: None },
        )])
        .expect("SHOW schema is well-formed")
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
    if matches!(select.distinct, Distinct::DistinctOn(_)) {
        return Err(PlanError::NotSupported("SELECT DISTINCT ON (...)"));
    }

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

    let mut plan = bind_select_body(select, catalog, &cte_catalog, scope)?;

    for tail in &select.set_ops {
        let right_plan = bind_select_with_ctes(&tail.right, catalog, &cte_catalog, scope)?;
        plan = bind_set_op(plan, tail.op, tail.quantifier, right_plan)?;
    }

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

    let mut plan = bind_select_body(select, catalog, &nested_cte_catalog, scope)?;

    for tail in &select.set_ops {
        let right_plan = bind_select_with_ctes(&tail.right, catalog, &nested_cte_catalog, scope)?;
        plan = bind_set_op(plan, tail.op, tail.quantifier, right_plan)?;
    }

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
            let out_ty = if matches!(lf.data_type, DataType::Null) {
                rf.data_type.clone()
            } else if matches!(rf.data_type, DataType::Null) {
                lf.data_type.clone()
            } else if lf.data_type.is_numeric() && rf.data_type.is_numeric() {
                lf.data_type.numeric_join(&rf.data_type).map_err(|_| {
                    PlanError::TypeMismatch(format!(
                        "set operation column type mismatch: {} vs {}",
                        lf.data_type, rf.data_type
                    ))
                })?
            } else {
                lf.data_type.clone()
            };
            Ok(Field::nullable(lf.name.clone(), out_ty))
        })
        .collect();
    let schema =
        Schema::new(fields?).map_err(|e| PlanError::TypeMismatch(format!("set op schema: {e}")))?;

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

/// The core `SELECT` body binding: FROM → WHERE → GROUP BY → HAVING →
/// SELECT list → ORDER BY → LIMIT/OFFSET.
#[allow(clippy::too_many_lines)]
fn bind_select_body(
    select: &SelectStmt,
    catalog: &dyn Catalog,
    cte_catalog: &[(String, Schema)],
    scope: &mut ScopeStack,
) -> Result<LogicalPlan, PlanError> {
    if matches!(select.distinct, Distinct::DistinctOn(_)) {
        return Err(PlanError::NotSupported("SELECT DISTINCT ON (...)"));
    }
    let is_distinct = matches!(select.distinct, Distinct::Distinct);

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
        window::extract_window_calls(&select.projection);
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
        let proj_fields: Vec<Field> = projected
            .iter()
            .map(|(e, name)| Field::nullable(name, e.data_type()))
            .collect();
        let proj_schema = Schema::new(proj_fields)
            .map_err(|e| PlanError::TypeMismatch(format!("projection: {e}")))?;

        plan = LogicalPlan::Project {
            input: Box::new(plan),
            exprs: projected,
            schema: proj_schema,
        };

        plan =
            bind_order_by_around_projection(plan, &select.order_by, catalog, cte_catalog, scope)?;
    } else {
        let projected = bind_projection_with_scope(
            &select.projection,
            plan.schema(),
            &from_scope,
            catalog,
            cte_catalog,
            scope,
        )?;
        let proj_fields: Vec<Field> = projected
            .iter()
            .map(|(e, name)| Field::nullable(name, e.data_type()))
            .collect();
        let proj_schema = Schema::new(proj_fields)
            .map_err(|e| PlanError::TypeMismatch(format!("projection: {e}")))?;

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
        plan = bind_order_by_around_projection_with_input_schema(
            plan,
            &select.order_by,
            &order_input_schema,
            catalog,
            cte_catalog,
            scope,
        )?;
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
        let sort_keys = bind_order_by(order_by, input_schema, catalog, cte_catalog, scope)?;
        return Ok(LogicalPlan::Sort {
            input: Box::new(plan),
            keys: sort_keys,
        });
    };

    match bind_order_by(order_by, input_schema, catalog, cte_catalog, scope) {
        Ok(sort_keys) => {
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
            let sort_keys = bind_order_by(order_by, &schema, catalog, cte_catalog, scope)?;
            Ok(LogicalPlan::Sort {
                input: Box::new(projected),
                keys: sort_keys,
            })
        }
        Err(error) => Err(error),
    }
}
