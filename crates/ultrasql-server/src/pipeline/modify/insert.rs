//! INSERT lowering: `lower_real_insert`, row-level-security check
//! builders, sequence defaults, and the fused `(Int32, Int32)` insert
//! fast path.

use std::sync::Arc;

use ultrasql_catalog::TableEntry;
use ultrasql_core::{DataType, RelationId, Schema, Value};
use ultrasql_executor::fused_insert::FusedInsertInt32Pair;
use ultrasql_executor::{
    Eval, InsertConflictAction, ModifyKind, ModifyTable, ModifyTableStamps, Operator,
    SequenceDefault, ValuesScan,
};
use ultrasql_planner::{
    BinaryOp, INSERT_DEFAULT_SENTINEL, LogicalOnConflict, LogicalPlan, ScalarExpr,
};

use crate::auth::pg_authid::AuthCatalog;
use crate::error::ServerError;
use crate::pipeline::LowerCtx;
use crate::pipeline::lower_query::lower_query;

use super::constraints::{
    build_exclusion_insert_checks, build_exclusion_update_checks, build_foreign_key_checks,
};
use super::indexes::{build_insert_index_maintainers, build_vector_index_maintainers};
use super::referential::build_referenced_by_update_checks;

pub(crate) fn lower_real_insert(
    table: &str,
    columns: &[usize],
    source: &LogicalPlan,
    on_conflict: Option<&ultrasql_planner::LogicalOnConflict>,
    returning: &[(ScalarExpr, String)],
    returning_schema: &Schema,
    ctx: &LowerCtx<'_>,
) -> Result<Box<dyn Operator>, ServerError> {
    let entry = ctx
        .catalog_snapshot
        .tables
        .get(&table.to_ascii_lowercase())
        .ok_or_else(|| {
            ServerError::Plan(ultrasql_planner::PlanError::TableNotFound(
                table.to_string(),
            ))
        })?;
    let insert_columns =
        resolve_insert_columns(columns, source.schema().len(), entry.schema.len())?;
    let rls_insert_checks = build_rls_insert_checks(entry, ctx);
    if let Some(partition) = ctx.time_partitions.get(&table.to_ascii_lowercase()) {
        if !rls_insert_checks.is_empty() {
            return Err(ServerError::Unsupported(
                "partitioned INSERT does not yet support row-level security checks",
            ));
        }
        if on_conflict.is_some() {
            return Err(ServerError::Unsupported(
                "partitioned INSERT does not yet support ON CONFLICT",
            ));
        }
        if !returning.is_empty() {
            return Err(ServerError::Unsupported(
                "partitioned INSERT does not yet support RETURNING",
            ));
        }
        if insert_column_map_needed(&insert_columns, entry.schema.len()) {
            return Err(ServerError::Unsupported(
                "partitioned INSERT requires full-width target columns in table order",
            ));
        }
        if ctx.table_constraints.contains_key(&entry.oid)
            || ctx
                .catalog_snapshot
                .indexes_by_table
                .get(&entry.oid)
                .is_some_and(|indexes| !indexes.is_empty())
        {
            return Err(ServerError::Unsupported(
                "partitioned INSERT does not yet support defaults, CHECK, FK, UNIQUE, or indexes",
            ));
        }
        let child: Box<dyn Operator> = match source {
            LogicalPlan::Values { rows, schema } => {
                // Partitioned tables reject constraints/defaults above, so a
                // `DEFAULT` cell here can only resolve to NULL.
                let rows = rewrite_insert_default_cells(rows, &insert_columns, None)?;
                Box::new(ValuesScan::new(rows, schema.clone()))
            }
            other => lower_query(other, ctx)?,
        };
        let insert = crate::time_partition::TimePartitionInsert::new(
            partition.clone(),
            Arc::clone(&ctx.persistent_catalog),
            Arc::clone(&ctx.heap),
            Arc::clone(&ctx.vm),
            child,
            ctx.xid,
            ctx.command_id,
        )
        .with_wal(ctx.heap.wal_sink().cloned());
        return Ok(Box::new(insert));
    }
    if rls_insert_checks.is_empty() {
        if let Some(fused) = try_build_fused_insert_int32_pair(
            entry,
            &insert_columns,
            source,
            on_conflict,
            returning,
            ctx,
        ) {
            return Ok(fused);
        }
    }
    let child: Box<dyn Operator> = match source {
        LogicalPlan::Values { rows, schema } => {
            // Substitute any `DEFAULT` cells with the target column's
            // default expression (or NULL when the column has none).
            let constraints = ctx.table_constraints.get(&entry.oid).map(|c| c.clone());
            let rows = rewrite_insert_default_cells(rows, &insert_columns, constraints.as_deref())?;
            Box::new(ValuesScan::new(rows, schema.clone()))
        }
        // `INSERT INTO t SELECT ...` — drive the destination through the
        // same `ModifyTable` shape we use for `VALUES`, but with a
        // lowered query plan as the row source. The binder enforced
        // arity, types, and named-column matching when it built the
        // `Insert` plan; if its schema differs from the target table's
        // declared schema, refuse here so a silent encoding mismatch
        // never lands rows into the heap with the wrong layout.
        other => {
            let source_schema = other.schema();
            if source_schema.len() != insert_columns.len() {
                return Err(ServerError::Unsupported(
                    "INSERT ... SELECT with arity mismatch",
                ));
            }
            for (idx, src) in source_schema.fields().iter().enumerate() {
                let dst_idx = insert_columns[idx];
                let dst = entry.schema.field_at(dst_idx);
                if src.data_type != dst.data_type
                    && !matches!(src.data_type, ultrasql_core::DataType::Null)
                {
                    return Err(ServerError::Plan(
                        ultrasql_planner::PlanError::TypeMismatch(format!(
                            "INSERT ... SELECT column {idx} type mismatch: source {src} vs target {dst}",
                        )),
                    ));
                }
            }
            lower_query(other, ctx)?
        }
    };
    let rel = RelationId(entry.oid);
    crate::aggregating_index::mark_aggregating_indexes_dirty(entry, ctx);
    let insert_indexes = build_insert_index_maintainers(entry, ctx)?;
    let insert_vector_indexes = build_vector_index_maintainers(entry, ctx)?;
    let update_indexes = if matches!(on_conflict, Some(LogicalOnConflict::DoUpdate { .. })) {
        build_insert_index_maintainers(entry, ctx)?
    } else {
        Vec::new()
    };
    let update_vector_indexes = if matches!(on_conflict, Some(LogicalOnConflict::DoUpdate { .. })) {
        build_vector_index_maintainers(entry, ctx)?
    } else {
        Vec::new()
    };
    let conflict_action = build_insert_conflict_action(on_conflict);
    let constraints = ctx.table_constraints.get(&entry.oid).map(|c| c.clone());
    let modify = ModifyTable::new(
        Arc::clone(&ctx.heap),
        rel,
        entry.schema.clone(),
        ModifyKind::Insert,
        ModifyTableStamps::new(ctx.xid, ctx.command_id, ctx.xid, ctx.command_id),
        ctx.heap.wal_sink().cloned(),
        child,
    )
    .with_visibility_map(Arc::clone(&ctx.vm))
    .with_uniqueness_recheck(
        ctx.snapshot.clone(),
        Arc::clone(&ctx.oracle) as Arc<dyn ultrasql_mvcc::XidStatusOracle>,
    )
    .with_insert_indexes(insert_indexes)
    .with_update_indexes(update_indexes)
    .with_insert_vector_indexes(insert_vector_indexes)
    .with_update_vector_indexes(update_vector_indexes);
    let modify = if let Some(action) = conflict_action {
        modify.with_insert_conflict_action(action)
    } else {
        modify
    };
    let mut check_constraints = rls_insert_checks;
    if let Some(constraints) = &constraints {
        check_constraints.extend(
            constraints
                .checks
                .iter()
                .map(|check| (check.name.clone(), check.expr.clone())),
        );
    }
    let modify = if !check_constraints.is_empty() {
        modify.with_check_constraints(check_constraints)
    } else {
        modify
    };
    let modify = if let Some(constraints) = constraints {
        modify
            .with_column_defaults(constraints.defaults.clone())
            .with_sequence_defaults(build_sequence_defaults(
                &constraints.sequence_defaults,
                ctx,
            )?)
            .with_identity_always(constraints.identity_always.clone())
            .with_generated_stored(constraints.generated_stored.clone())
            .with_foreign_key_checks(build_foreign_key_checks(&constraints.foreign_keys, ctx)?)
            .with_exclusion_checks(build_exclusion_insert_checks(
                entry,
                &constraints.exclusion_constraints,
                ctx,
            )?)
            .with_exclusion_update_checks(build_exclusion_update_checks(
                entry,
                &constraints.exclusion_constraints,
                ctx,
            )?)
    } else {
        modify
    };
    let modify =
        modify.with_referenced_by_update_checks(build_referenced_by_update_checks(entry.oid, ctx)?);
    let modify = if insert_column_map_needed(&insert_columns, entry.schema.len()) {
        modify.with_insert_column_map(insert_columns)
    } else {
        modify
    };
    let modify = if returning.is_empty() {
        modify
    } else {
        modify.with_returning(
            returning.iter().map(|(expr, _name)| expr.clone()).collect(),
            returning_schema.clone(),
        )
    };
    Ok(Box::new(modify))
}

pub(super) fn build_rls_insert_checks(
    entry: &TableEntry,
    ctx: &LowerCtx<'_>,
) -> Vec<(String, ScalarExpr)> {
    build_rls_mutation_checks(entry, ctx, crate::RuntimeRlsCommand::Insert)
}

pub(super) fn build_rls_update_checks(
    entry: &TableEntry,
    ctx: &LowerCtx<'_>,
) -> Vec<(String, ScalarExpr)> {
    build_rls_mutation_checks(entry, ctx, crate::RuntimeRlsCommand::Update)
}

fn build_rls_mutation_checks(
    entry: &TableEntry,
    ctx: &LowerCtx<'_>,
    command: crate::RuntimeRlsCommand,
) -> Vec<(String, ScalarExpr)> {
    let Some(runtime_ref) = ctx.row_security.get(&entry.oid) else {
        return Vec::new();
    };
    let runtime = Arc::clone(runtime_ref.value());
    if !runtime.enabled || bypasses_row_security(runtime.as_ref(), ctx) {
        return Vec::new();
    }

    let inherited_roles = ctx.role_catalog.inherited_role_names(&ctx.current_user);
    let mut permissive = Vec::new();
    let mut restrictive = Vec::new();
    for policy in runtime.policies.iter().filter(|policy| {
        policy.command.applies_to(command) && policy.applies_to_roles(&inherited_roles)
    }) {
        let Some(expr) = policy.with_check.as_ref().or(policy.using.as_ref()) else {
            continue;
        };
        let predicate = rls_tenant_check_predicate(entry, expr, ctx);
        match policy.permissiveness {
            crate::RuntimeRlsPermissiveness::Permissive => permissive.push(predicate),
            crate::RuntimeRlsPermissiveness::Restrictive => restrictive.push(predicate),
        }
    }

    let Some(mut predicate) = combine_rls_check_predicates(permissive, BinaryOp::Or) else {
        return vec![("row-level security policy".to_owned(), bool_literal(false))];
    };
    if let Some(restrictive) = combine_rls_check_predicates(restrictive, BinaryOp::And) {
        predicate = ScalarExpr::Binary {
            op: BinaryOp::And,
            left: Box::new(predicate),
            right: Box::new(restrictive),
            data_type: DataType::Bool,
        };
    }
    vec![("row-level security policy".to_owned(), predicate)]
}

fn bypasses_row_security(runtime: &crate::TableRowSecurity, ctx: &LowerCtx<'_>) -> bool {
    let current_user = ctx.current_user.to_ascii_lowercase();
    let Some(role) = ctx.role_catalog.lookup_role(&current_user) else {
        return false;
    };
    role.is_superuser
        || role.bypass_rls
        || (!runtime.owner_role.is_empty()
            && runtime.owner_role.eq_ignore_ascii_case(&current_user))
}

fn rls_tenant_check_predicate(
    _entry: &TableEntry,
    expr: &crate::RuntimeTenantPolicyExpr,
    ctx: &LowerCtx<'_>,
) -> ScalarExpr {
    let Some(value) = ctx
        .session_settings
        .get(&expr.setting_name.to_ascii_lowercase())
    else {
        return bool_literal(false);
    };
    ScalarExpr::Binary {
        op: BinaryOp::Eq,
        left: Box::new(ScalarExpr::Column {
            name: expr.column_name.clone(),
            index: expr.column_index,
            data_type: DataType::Text { max_len: None },
        }),
        right: Box::new(ScalarExpr::Literal {
            value: Value::Text(value.clone()),
            data_type: DataType::Text { max_len: None },
        }),
        data_type: DataType::Bool,
    }
}

fn combine_rls_check_predicates(
    mut predicates: Vec<ScalarExpr>,
    op: BinaryOp,
) -> Option<ScalarExpr> {
    let mut current = predicates.pop()?;
    while let Some(next) = predicates.pop() {
        current = ScalarExpr::Binary {
            op,
            left: Box::new(next),
            right: Box::new(current),
            data_type: DataType::Bool,
        };
    }
    Some(current)
}

fn bool_literal(value: bool) -> ScalarExpr {
    ScalarExpr::Literal {
        value: Value::Bool(value),
        data_type: DataType::Bool,
    }
}

fn try_build_fused_insert_int32_pair(
    entry: &TableEntry,
    insert_columns: &[usize],
    source: &LogicalPlan,
    on_conflict: Option<&LogicalOnConflict>,
    returning: &[(ScalarExpr, String)],
    ctx: &LowerCtx<'_>,
) -> Option<Box<dyn Operator>> {
    if on_conflict.is_some()
        || !returning.is_empty()
        || insert_column_map_needed(insert_columns, entry.schema.len())
        || ctx.table_constraints.contains_key(&entry.oid)
        || ctx
            .catalog_snapshot
            .indexes_by_table
            .get(&entry.oid)
            .is_some_and(|indexes| !indexes.is_empty())
    {
        return None;
    }
    let fields = entry.schema.fields();
    if fields.len() != 2
        || fields[0].data_type != DataType::Int32
        || fields[1].data_type != DataType::Int32
    {
        return None;
    }
    let LogicalPlan::Values { rows, schema } = source else {
        return None;
    };
    if schema.len() != 2
        || schema.field_at(0).data_type != DataType::Int32
        || schema.field_at(1).data_type != DataType::Int32
    {
        return None;
    }
    let mut literal_rows = Vec::with_capacity(rows.len());
    for row in rows {
        let [
            ScalarExpr::Literal {
                value: Value::Int32(id),
                ..
            },
            ScalarExpr::Literal {
                value: Value::Int32(val),
                ..
            },
        ] = row.as_slice()
        else {
            return None;
        };
        literal_rows.push((*id, *val));
    }
    let op = FusedInsertInt32Pair::new(
        Arc::clone(&ctx.heap),
        RelationId(entry.oid),
        literal_rows,
        ctx.xid,
        ctx.command_id,
        ctx.heap.wal_sink().cloned(),
        Some(Arc::clone(&ctx.vm)),
    );
    Some(Box::new(op))
}

fn build_insert_conflict_action(
    on_conflict: Option<&LogicalOnConflict>,
) -> Option<InsertConflictAction> {
    match on_conflict? {
        LogicalOnConflict::DoNothing { target } => Some(InsertConflictAction::DoNothing {
            target: target.as_ref().map(|target| target.columns.clone()),
        }),
        LogicalOnConflict::DoUpdate {
            target,
            assignments,
            r#where,
        } => Some(InsertConflictAction::DoUpdate {
            target: target.columns.clone(),
            assignments: assignments
                .iter()
                .map(|(column, expr)| (*column, Eval::new(expr.clone())))
                .collect(),
            predicate: r#where.clone().map(Eval::new),
        }),
    }
}

fn resolve_insert_columns(
    columns: &[usize],
    source_arity: usize,
    target_width: usize,
) -> Result<Vec<usize>, ServerError> {
    if source_arity == 0 {
        return Ok(Vec::new());
    }
    let resolved: Vec<usize> = if columns.is_empty() {
        (0..target_width).collect()
    } else {
        columns.to_vec()
    };
    if resolved.len() != source_arity {
        return Err(ServerError::Unsupported("INSERT source arity mismatch"));
    }
    for target_idx in &resolved {
        if *target_idx >= target_width {
            return Err(ServerError::Unsupported(
                "INSERT target column out of range",
            ));
        }
    }
    Ok(resolved)
}

fn insert_column_map_needed(columns: &[usize], target_width: usize) -> bool {
    columns.len() != target_width
        || columns
            .iter()
            .copied()
            .enumerate()
            .any(|(idx, target_idx)| idx != target_idx)
}

/// Replace every `DEFAULT` sentinel cell in a `VALUES` source with the
/// target column's declared default expression, or `NULL` when the column
/// has no plain default.
///
/// `insert_columns[i]` is the table-column index that source position `i`
/// feeds. A `DEFAULT` against a column whose default is provided by a
/// sequence (`SERIAL`) or `GENERATED ... AS IDENTITY` is not yet rewritten
/// here and is reported as unsupported rather than silently producing a
/// wrong value.
fn rewrite_insert_default_cells(
    rows: &[Vec<ScalarExpr>],
    insert_columns: &[usize],
    constraints: Option<&crate::TableRuntimeConstraints>,
) -> Result<Vec<Vec<ScalarExpr>>, ServerError> {
    // Fast path: no `DEFAULT` cell anywhere — clone through unchanged.
    let has_default = rows
        .iter()
        .flatten()
        .any(|cell| matches!(cell, ScalarExpr::FunctionCall { name, .. } if name == INSERT_DEFAULT_SENTINEL));
    if !has_default {
        return Ok(rows.to_vec());
    }

    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let mut new_row = Vec::with_capacity(row.len());
        for (pos, cell) in row.iter().enumerate() {
            let is_default = matches!(
                cell,
                ScalarExpr::FunctionCall { name, .. } if name == INSERT_DEFAULT_SENTINEL
            );
            if !is_default {
                new_row.push(cell.clone());
                continue;
            }
            let table_col = insert_columns.get(pos).copied().unwrap_or(pos);
            // A column whose default is a sequence (`SERIAL`) or an identity
            // column needs nextval/identity machinery we do not reproduce at
            // this rewrite point; refuse rather than insert a wrong value.
            if let Some(constraints) = constraints {
                if constraints
                    .sequence_defaults
                    .get(table_col)
                    .is_some_and(Option::is_some)
                    || constraints.identity_always.get(table_col).copied() == Some(true)
                {
                    return Err(ServerError::Unsupported(
                        "DEFAULT in VALUES for a SERIAL/identity column is not yet supported",
                    ));
                }
            }
            let replacement = constraints
                .and_then(|c| c.defaults.get(table_col))
                .and_then(Option::as_ref)
                .cloned()
                .unwrap_or(ScalarExpr::Literal {
                    value: Value::Null,
                    data_type: DataType::Null,
                });
            new_row.push(replacement);
        }
        out.push(new_row);
    }
    Ok(out)
}

pub(super) fn build_sequence_defaults(
    defaults: &[Option<String>],
    ctx: &LowerCtx<'_>,
) -> Result<Vec<Option<SequenceDefault>>, ServerError> {
    let observer = ctx.sequence_state.as_ref().map(|state| {
        let state = state.clone();
        Arc::new(move |name: &str, value: i64| state.record_nextval(name, value))
            as Arc<dyn Fn(&str, i64) + Send + Sync>
    });
    let wal = ctx.heap.wal_sink().cloned();
    defaults
        .iter()
        .map(|name| {
            let Some(name) = name else {
                return Ok(None);
            };
            let sequence = ctx
                .sequences
                .get(name)
                .map(|seq| seq.clone())
                .ok_or_else(|| {
                    ServerError::Catalog(ultrasql_catalog::CatalogError::not_found(name.clone()))
                })?;
            let mut default = SequenceDefault::new(name.clone(), sequence);
            default = default.with_wal(wal.clone(), ctx.xid, ultrasql_core::RelationId::INVALID);
            if let Some(observer) = &observer {
                default = default.with_observer(Arc::clone(observer));
            }
            Ok(Some(default))
        })
        .collect()
}
