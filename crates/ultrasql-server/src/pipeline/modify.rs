//! INSERT/UPDATE/DELETE lowering plus the fused-kernel fast paths.

use std::collections::HashMap;
use std::sync::Arc;

use ultrasql_catalog::{IndexEntry, TableEntry};
use ultrasql_core::{CommandId, DataType, RelationId, TupleId, Value, Xid};
use ultrasql_executor::fused_delete::FusedDeleteInt32Pair;
use ultrasql_executor::fused_update::{FusedCmp, FusedPredicate, FusedUpdateInt32Add};
use ultrasql_executor::{
    Eval, Filter, InsertIndexEncoder, InsertIndexMaintainer, ModifyKind, ModifyTable, Operator,
    Project, RowCodec, RowConstraintCheck, RowUpdateConstraintCheck, SeqScan, SequenceDefault,
    ValuesScan,
};
use ultrasql_planner::{BinaryOp, LogicalPlan, LogicalReferentialAction, ScalarExpr};
use ultrasql_storage::btree::BTree;
use ultrasql_storage::heap::{DeleteOptions, UpdateOptions, UpdatePayload};

use crate::error::ServerError;
use crate::index_key::IndexKeyEncoding;

use super::LowerCtx;
use super::agg_fuse::{extract_int32_col_op_lit, shift_column_indices};
use super::lower_query::lower_query;

type CascadeIndexDeletes = Vec<(BTree<crate::BlankPageLoader>, Vec<(i64, TupleId)>)>;
type ReferentialIndexUpdates = Vec<(
    String,
    BTree<crate::BlankPageLoader>,
    Vec<ReferentialIndexUpdate>,
)>;

#[derive(Clone, Copy)]
struct ReferentialIndexUpdate {
    old_tid: TupleId,
    old_key: Option<i64>,
    new_key: Option<i64>,
}

pub(super) fn lower_real_insert(
    table: &str,
    columns: &[usize],
    source: &LogicalPlan,
    on_conflict: Option<&ultrasql_planner::LogicalOnConflict>,
    returning: &[(ScalarExpr, String)],
    ctx: &LowerCtx<'_>,
) -> Result<Box<dyn Operator>, ServerError> {
    if on_conflict.is_some() {
        return Err(ServerError::Unsupported("INSERT ... ON CONFLICT"));
    }
    if !returning.is_empty() {
        return Err(ServerError::Unsupported("INSERT ... RETURNING"));
    }
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
    let child: Box<dyn Operator> = match source {
        LogicalPlan::Values { rows, schema } => {
            Box::new(ValuesScan::new(rows.clone(), schema.clone()))
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
    let insert_indexes = build_insert_index_maintainers(entry, ctx)?;
    let constraints = ctx.table_constraints.get(&entry.oid).map(|c| c.clone());
    let modify = ModifyTable::new(
        Arc::clone(&ctx.heap),
        rel,
        entry.schema.clone(),
        ModifyKind::Insert,
        ctx.xid,
        ctx.command_id,
        Xid::new(0),
        CommandId::FIRST,
        ctx.heap.wal_sink().cloned(),
        child,
    )
    .with_visibility_map(Arc::clone(&ctx.vm))
    .with_insert_indexes(insert_indexes);
    let modify = if let Some(constraints) = constraints {
        modify
            .with_column_defaults(constraints.defaults.clone())
            .with_sequence_defaults(build_sequence_defaults(
                &constraints.sequence_defaults,
                ctx,
            )?)
            .with_identity_always(constraints.identity_always.clone())
            .with_generated_stored(constraints.generated_stored.clone())
            .with_check_constraints(
                constraints
                    .checks
                    .iter()
                    .map(|check| (check.name.clone(), check.expr.clone()))
                    .collect(),
            )
            .with_foreign_key_checks(build_foreign_key_checks(&constraints.foreign_keys, ctx)?)
    } else {
        modify
    };
    let modify = if insert_column_map_needed(&insert_columns, entry.schema.len()) {
        modify.with_insert_column_map(insert_columns)
    } else {
        modify
    };
    Ok(Box::new(modify))
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

fn build_sequence_defaults(
    defaults: &[Option<String>],
    ctx: &LowerCtx<'_>,
) -> Result<Vec<Option<SequenceDefault>>, ServerError> {
    let observer = ctx.sequence_state.as_ref().map(|state| {
        let state = state.clone();
        Arc::new(move |name: &str, value: i64| state.record_nextval(name, value))
            as Arc<dyn Fn(&str, i64) + Send + Sync>
    });
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
            if let Some(observer) = &observer {
                default = default.with_observer(Arc::clone(observer));
            }
            Ok(Some(default))
        })
        .collect()
}

fn build_foreign_key_checks(
    foreign_keys: &[crate::RuntimeForeignKeyConstraint],
    ctx: &LowerCtx<'_>,
) -> Result<Vec<RowConstraintCheck>, ServerError> {
    let mut out = Vec::with_capacity(foreign_keys.len());
    for fk in foreign_keys {
        let parent = ctx
            .catalog_snapshot
            .tables
            .get(&fk.target_table)
            .cloned()
            .ok_or_else(|| {
                ServerError::Plan(ultrasql_planner::PlanError::TableNotFound(
                    fk.target_table.clone(),
                ))
            })?;
        let heap = Arc::clone(&ctx.heap);
        let snapshot = ctx.snapshot.clone();
        let oracle = Arc::clone(&ctx.oracle);
        let name = fk.name.clone();
        let columns = fk.columns.clone();
        let target_columns = fk.target_columns.clone();
        out.push(Arc::new(move |row: &[Value]| {
            let Some(key) = row_key(row, &columns) else {
                return Ok(());
            };
            if relation_has_key(&heap, &parent, &target_columns, &key, &snapshot, &oracle)? {
                Ok(())
            } else {
                Err(ultrasql_executor::ExecError::ForeignKeyViolation(
                    name.clone(),
                ))
            }
        }) as RowConstraintCheck);
    }
    Ok(out)
}

fn build_referenced_by_delete_checks(
    parent_oid: ultrasql_core::Oid,
    ctx: &LowerCtx<'_>,
) -> Result<Vec<RowConstraintCheck>, ServerError> {
    let mut out = Vec::new();
    for item in ctx.table_constraints.iter() {
        let child_oid = *item.key();
        let child = table_entry_by_oid(ctx, child_oid)?;
        for fk in &item.value().foreign_keys {
            if fk.target_oid != parent_oid {
                continue;
            }
            let heap = Arc::clone(&ctx.heap);
            let snapshot = ctx.snapshot.clone();
            let oracle = Arc::clone(&ctx.oracle);
            let child = child.clone();
            let name = fk.name.clone();
            let child_columns = fk.columns.clone();
            let target_columns = fk.target_columns.clone();
            let on_delete = fk.on_delete;
            let child_constraints = Arc::clone(item.value());
            let child_indexes = ctx
                .catalog_snapshot
                .indexes_by_table
                .get(&child_oid)
                .cloned()
                .unwrap_or_default();
            let sequences = Arc::clone(&ctx.sequences);
            let sequence_state = ctx.sequence_state.clone();
            let xid = ctx.xid;
            let command_id = ctx.command_id;
            let vm = Arc::clone(&ctx.vm);
            out.push(Arc::new(move |parent_row: &[Value]| {
                let Some(key) = row_key(parent_row, &target_columns) else {
                    return Ok(());
                };
                let child_rows =
                    matching_child_rows(&heap, &child, &child_columns, &key, &snapshot, &oracle)?;
                if child_rows.is_empty() {
                    return Ok(());
                }
                match on_delete {
                    LogicalReferentialAction::NoAction | LogicalReferentialAction::Restrict => Err(
                        ultrasql_executor::ExecError::ForeignKeyViolation(name.clone()),
                    ),
                    LogicalReferentialAction::Cascade => {
                        cascade_delete_child_rows(
                            &heap,
                            &child,
                            &child_indexes,
                            &child_rows,
                            xid,
                            command_id,
                            &vm,
                        )?;
                        Ok(())
                    }
                    LogicalReferentialAction::SetNull | LogicalReferentialAction::SetDefault => {
                        update_child_rows_for_delete_action(
                            &heap,
                            &child,
                            &child_indexes,
                            &child_rows,
                            &child_columns,
                            on_delete,
                            &child_constraints,
                            &sequences,
                            sequence_state.as_ref(),
                            xid,
                            command_id,
                            &vm,
                        )
                    }
                }
            }) as RowConstraintCheck);
        }
    }
    Ok(out)
}

fn build_referenced_by_update_checks(
    parent_oid: ultrasql_core::Oid,
    ctx: &LowerCtx<'_>,
) -> Result<Vec<RowUpdateConstraintCheck>, ServerError> {
    let mut out = Vec::new();
    for item in ctx.table_constraints.iter() {
        let child_oid = *item.key();
        let child = table_entry_by_oid(ctx, child_oid)?;
        for fk in &item.value().foreign_keys {
            if fk.target_oid != parent_oid {
                continue;
            }
            let heap = Arc::clone(&ctx.heap);
            let snapshot = ctx.snapshot.clone();
            let oracle = Arc::clone(&ctx.oracle);
            let child = child.clone();
            let name = fk.name.clone();
            let child_columns = fk.columns.clone();
            let target_columns = fk.target_columns.clone();
            let on_update = fk.on_update;
            let child_constraints = Arc::clone(item.value());
            let child_indexes = ctx
                .catalog_snapshot
                .indexes_by_table
                .get(&child_oid)
                .cloned()
                .unwrap_or_default();
            let sequences = Arc::clone(&ctx.sequences);
            let sequence_state = ctx.sequence_state.clone();
            let xid = ctx.xid;
            let command_id = ctx.command_id;
            let vm = Arc::clone(&ctx.vm);
            out.push(Arc::new(move |old_row: &[Value], new_row: &[Value]| {
                let old_key = row_key(old_row, &target_columns);
                let new_key = row_key(new_row, &target_columns);
                if old_key == new_key {
                    return Ok(());
                }
                let Some(key) = old_key else {
                    return Ok(());
                };
                let child_rows =
                    matching_child_rows(&heap, &child, &child_columns, &key, &snapshot, &oracle)?;
                if child_rows.is_empty() {
                    return Ok(());
                }
                match on_update {
                    LogicalReferentialAction::NoAction | LogicalReferentialAction::Restrict => Err(
                        ultrasql_executor::ExecError::ForeignKeyViolation(name.clone()),
                    ),
                    LogicalReferentialAction::Cascade => cascade_update_child_rows(
                        &heap,
                        &child,
                        &child_indexes,
                        &child_rows,
                        &child_columns,
                        &target_columns,
                        new_row,
                        &child_constraints,
                        xid,
                        command_id,
                        &vm,
                    ),
                    LogicalReferentialAction::SetNull | LogicalReferentialAction::SetDefault => {
                        update_child_rows_for_delete_action(
                            &heap,
                            &child,
                            &child_indexes,
                            &child_rows,
                            &child_columns,
                            on_update,
                            &child_constraints,
                            &sequences,
                            sequence_state.as_ref(),
                            xid,
                            command_id,
                            &vm,
                        )
                    }
                }
            }) as RowUpdateConstraintCheck);
        }
    }
    Ok(out)
}

fn table_entry_by_oid(
    ctx: &LowerCtx<'_>,
    oid: ultrasql_core::Oid,
) -> Result<TableEntry, ServerError> {
    ctx.catalog_snapshot
        .tables
        .values()
        .find(|entry| entry.oid == oid)
        .cloned()
        .ok_or_else(|| {
            ServerError::Catalog(ultrasql_catalog::CatalogError::not_found(format!(
                "oid {}",
                oid.raw()
            )))
        })
}

fn row_key(row: &[Value], columns: &[usize]) -> Option<Vec<Value>> {
    let mut key = Vec::with_capacity(columns.len());
    for &idx in columns {
        let value = row.get(idx)?;
        if matches!(value, Value::Null) {
            return None;
        }
        key.push(value.clone());
    }
    Some(key)
}

fn relation_has_key(
    heap: &ultrasql_storage::heap::HeapAccess<crate::BlankPageLoader>,
    table: &TableEntry,
    columns: &[usize],
    key: &[Value],
    snapshot: &ultrasql_mvcc::Snapshot,
    oracle: &ultrasql_txn::TransactionManager,
) -> Result<bool, ultrasql_executor::ExecError> {
    let relation = RelationId(table.oid);
    let block_count = heap.block_count(relation).max(table.n_blocks);
    let codec = RowCodec::new(table.schema.clone());
    for tuple in heap.scan_visible(relation, block_count, snapshot, oracle) {
        let tuple = tuple.map_err(|e| ultrasql_executor::ExecError::TypeMismatch(e.to_string()))?;
        let row = codec
            .decode(&tuple.data)
            .map_err(|e| ultrasql_executor::ExecError::TypeMismatch(e.to_string()))?;
        if row_key(&row, columns).as_deref() == Some(key) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn matching_child_rows(
    heap: &ultrasql_storage::heap::HeapAccess<crate::BlankPageLoader>,
    table: &TableEntry,
    columns: &[usize],
    key: &[Value],
    snapshot: &ultrasql_mvcc::Snapshot,
    oracle: &ultrasql_txn::TransactionManager,
) -> Result<Vec<(TupleId, Vec<Value>)>, ultrasql_executor::ExecError> {
    let relation = RelationId(table.oid);
    let block_count = heap.block_count(relation).max(table.n_blocks);
    let codec = RowCodec::new(table.schema.clone());
    let mut out = Vec::new();
    for tuple in heap.scan_visible(relation, block_count, snapshot, oracle) {
        let tuple = tuple.map_err(|e| ultrasql_executor::ExecError::TypeMismatch(e.to_string()))?;
        let row = codec
            .decode(&tuple.data)
            .map_err(|e| ultrasql_executor::ExecError::TypeMismatch(e.to_string()))?;
        if row_key(&row, columns).as_deref() == Some(key) {
            out.push((tuple.tid, row));
        }
    }
    Ok(out)
}

fn cascade_delete_child_rows(
    heap: &ultrasql_storage::heap::HeapAccess<crate::BlankPageLoader>,
    child: &TableEntry,
    indexes: &[IndexEntry],
    rows: &[(TupleId, Vec<Value>)],
    xid: Xid,
    command_id: CommandId,
    vm: &ultrasql_storage::vm::VisibilityMap,
) -> Result<(), ultrasql_executor::ExecError> {
    if rows.is_empty() {
        return Ok(());
    }
    let mut index_deletes: CascadeIndexDeletes = Vec::with_capacity(indexes.len());
    for index in indexes {
        let columns: Vec<usize> = index.columns.iter().map(|col| usize::from(*col)).collect();
        let encoding = IndexKeyEncoding::for_columns(&child.schema, &columns)
            .map_err(|e| ultrasql_executor::ExecError::TypeMismatch(e.to_string()))?;
        let tree = BTree::open(
            Arc::clone(heap.buffer_pool()),
            RelationId::new(index.oid.raw()),
            index.root_block,
        );
        let mut keys = Vec::new();
        for (tid, row) in rows {
            let key = match columns.as_slice() {
                [col] => encoding
                    .encode_value(row.get(*col).ok_or_else(|| {
                        ultrasql_executor::ExecError::TypeMismatch(format!(
                            "index {}: row missing key column {col}",
                            index.name
                        ))
                    })?)
                    .map_err(|e| ultrasql_executor::ExecError::TypeMismatch(e.to_string()))?,
                _ => encoding
                    .encode_row(row)
                    .map_err(|e| ultrasql_executor::ExecError::TypeMismatch(e.to_string()))?,
            };
            if let Some(key) = key {
                keys.push((key, *tid));
            }
        }
        index_deletes.push((tree, keys));
    }

    let tids = rows.iter().map(|(tid, _)| *tid).collect::<Vec<_>>();
    let wal = heap.wal_sink().cloned();
    let wal_ref = wal.as_deref();
    heap.delete_many(
        tids,
        DeleteOptions {
            xmax: xid,
            cmax: command_id,
            wal: wal_ref,
            fsm: None,
            vm: Some(vm),
        },
    )
    .map_err(|e| ultrasql_executor::ExecError::TypeMismatch(e.to_string()))?;

    for (mut tree, keys) in index_deletes {
        for (key, tid) in keys {
            tree.delete_logged::<i64>(key, tid, xid, wal_ref)
                .map_err(|e| ultrasql_executor::ExecError::TypeMismatch(e.to_string()))?;
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn update_child_rows_for_delete_action(
    heap: &ultrasql_storage::heap::HeapAccess<crate::BlankPageLoader>,
    child: &TableEntry,
    indexes: &[IndexEntry],
    rows: &[(TupleId, Vec<Value>)],
    child_columns: &[usize],
    action: LogicalReferentialAction,
    constraints: &crate::TableRuntimeConstraints,
    sequences: &dashmap::DashMap<String, Arc<ultrasql_storage::sequence::Sequence>>,
    sequence_state: Option<&crate::SequenceSessionState>,
    xid: Xid,
    command_id: CommandId,
    vm: &ultrasql_storage::vm::VisibilityMap,
) -> Result<(), ultrasql_executor::ExecError> {
    if rows.is_empty() {
        return Ok(());
    }
    let codec = RowCodec::new(child.schema.clone());
    let mut edits: Vec<(TupleId, UpdatePayload)> = Vec::with_capacity(rows.len());
    let mut index_updates = build_referential_index_updates(heap, child, indexes, rows)?;

    for (row_idx, (tid, old_row)) in rows.iter().enumerate() {
        let mut new_row = old_row.clone();
        for &col in child_columns {
            if col >= new_row.len() {
                return Err(ultrasql_executor::ExecError::TypeMismatch(format!(
                    "referential action column {col} out of range for {}",
                    child.name
                )));
            }
            new_row[col] = match action {
                LogicalReferentialAction::SetNull => Value::Null,
                LogicalReferentialAction::SetDefault => {
                    referential_default_value(child, col, constraints, sequences, sequence_state)?
                }
                LogicalReferentialAction::Cascade
                | LogicalReferentialAction::NoAction
                | LogicalReferentialAction::Restrict => {
                    return Err(ultrasql_executor::ExecError::Unsupported(
                        "unexpected referential update action",
                    ));
                }
            };
        }
        validate_referential_action_row(child, constraints, &new_row)?;
        update_new_index_keys(child, indexes, &mut index_updates, row_idx, &new_row)?;
        let payload = codec
            .encode(&new_row)
            .map_err(|e| ultrasql_executor::ExecError::TypeMismatch(e.to_string()))?;
        edits.push((*tid, UpdatePayload::from_vec(payload)));
    }

    precheck_referential_index_updates(&index_updates)?;
    let wal = heap.wal_sink().cloned();
    let wal_ref = wal.as_deref();
    let outcomes = heap
        .update_many_with_outcomes(
            edits,
            UpdateOptions {
                xid,
                command_id,
                hot_eligible: indexes.is_empty(),
                wal: wal_ref,
                vm: Some(vm),
            },
        )
        .map_err(|e| ultrasql_executor::ExecError::TypeMismatch(e.to_string()))?;
    apply_referential_index_updates(index_updates, &outcomes, xid, wal_ref)?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn cascade_update_child_rows(
    heap: &ultrasql_storage::heap::HeapAccess<crate::BlankPageLoader>,
    child: &TableEntry,
    indexes: &[IndexEntry],
    rows: &[(TupleId, Vec<Value>)],
    child_columns: &[usize],
    target_columns: &[usize],
    new_parent_row: &[Value],
    constraints: &crate::TableRuntimeConstraints,
    xid: Xid,
    command_id: CommandId,
    vm: &ultrasql_storage::vm::VisibilityMap,
) -> Result<(), ultrasql_executor::ExecError> {
    if child_columns.len() != target_columns.len() {
        return Err(ultrasql_executor::ExecError::TypeMismatch(
            "foreign key column count mismatch during ON UPDATE CASCADE".to_owned(),
        ));
    }
    let codec = RowCodec::new(child.schema.clone());
    let mut edits: Vec<(TupleId, UpdatePayload)> = Vec::with_capacity(rows.len());
    let mut index_updates = build_referential_index_updates(heap, child, indexes, rows)?;

    for (row_idx, (tid, old_row)) in rows.iter().enumerate() {
        let mut new_row = old_row.clone();
        for (&child_col, &target_col) in child_columns.iter().zip(target_columns) {
            if child_col >= new_row.len() || target_col >= new_parent_row.len() {
                return Err(ultrasql_executor::ExecError::TypeMismatch(
                    "foreign key column out of range during ON UPDATE CASCADE".to_owned(),
                ));
            }
            new_row[child_col] = new_parent_row[target_col].clone();
        }
        validate_referential_action_row(child, constraints, &new_row)?;
        update_new_index_keys(child, indexes, &mut index_updates, row_idx, &new_row)?;
        let payload = codec
            .encode(&new_row)
            .map_err(|e| ultrasql_executor::ExecError::TypeMismatch(e.to_string()))?;
        edits.push((*tid, UpdatePayload::from_vec(payload)));
    }

    precheck_referential_index_updates(&index_updates)?;
    let wal = heap.wal_sink().cloned();
    let wal_ref = wal.as_deref();
    let outcomes = heap
        .update_many_with_outcomes(
            edits,
            UpdateOptions {
                xid,
                command_id,
                hot_eligible: indexes.is_empty(),
                wal: wal_ref,
                vm: Some(vm),
            },
        )
        .map_err(|e| ultrasql_executor::ExecError::TypeMismatch(e.to_string()))?;
    apply_referential_index_updates(index_updates, &outcomes, xid, wal_ref)?;
    Ok(())
}

fn referential_default_value(
    child: &TableEntry,
    col: usize,
    constraints: &crate::TableRuntimeConstraints,
    sequences: &dashmap::DashMap<String, Arc<ultrasql_storage::sequence::Sequence>>,
    sequence_state: Option<&crate::SequenceSessionState>,
) -> Result<Value, ultrasql_executor::ExecError> {
    let field = child.schema.field_at(col);
    if let Some(seq_name) = constraints
        .sequence_defaults
        .get(col)
        .and_then(Option::as_ref)
    {
        let sequence = sequences.get(seq_name).ok_or_else(|| {
            ultrasql_executor::ExecError::TypeMismatch(format!(
                "sequence default {seq_name} not found"
            ))
        })?;
        let raw = sequence.nextval().map_err(|e| {
            ultrasql_executor::ExecError::TypeMismatch(format!("sequence default {seq_name}: {e}"))
        })?;
        if let Some(state) = sequence_state {
            state.record_nextval(seq_name, raw);
        }
        return match field.data_type {
            DataType::Int16 => i16::try_from(raw).map(Value::Int16).map_err(|_| {
                ultrasql_executor::ExecError::TypeMismatch(format!(
                    "sequence default {seq_name} value {raw} out of range for Int16"
                ))
            }),
            DataType::Int32 => i32::try_from(raw).map(Value::Int32).map_err(|_| {
                ultrasql_executor::ExecError::TypeMismatch(format!(
                    "sequence default {seq_name} value {raw} out of range for Int32"
                ))
            }),
            DataType::Int64 => Ok(Value::Int64(raw)),
            ref other => Err(ultrasql_executor::ExecError::TypeMismatch(format!(
                "sequence default {seq_name} cannot populate {:?}",
                other
            ))),
        };
    }
    let Some(default) = constraints.defaults.get(col).and_then(Option::as_ref) else {
        return Ok(Value::Null);
    };
    Eval::new(default.clone())
        .eval(&[])
        .map_err(|e| ultrasql_executor::ExecError::TypeMismatch(e.to_string()))
}

fn validate_referential_action_row(
    child: &TableEntry,
    constraints: &crate::TableRuntimeConstraints,
    row: &[Value],
) -> Result<(), ultrasql_executor::ExecError> {
    for (idx, field) in child.schema.fields().iter().enumerate() {
        if !field.nullable && matches!(row.get(idx), Some(Value::Null) | None) {
            return Err(ultrasql_executor::ExecError::NotNullViolation(
                field.name.clone(),
            ));
        }
    }
    for check in &constraints.checks {
        match Eval::new(check.expr.clone())
            .eval(row)
            .map_err(|e| ultrasql_executor::ExecError::TypeMismatch(e.to_string()))?
        {
            Value::Bool(true) | Value::Null => {}
            Value::Bool(false) => {
                return Err(ultrasql_executor::ExecError::CheckViolation(
                    check.name.clone(),
                ));
            }
            other => {
                return Err(ultrasql_executor::ExecError::TypeMismatch(format!(
                    "CHECK {} evaluated to {:?}, expected bool",
                    check.name,
                    other.data_type()
                )));
            }
        }
    }
    Ok(())
}

fn build_referential_index_updates(
    heap: &ultrasql_storage::heap::HeapAccess<crate::BlankPageLoader>,
    child: &TableEntry,
    indexes: &[IndexEntry],
    rows: &[(TupleId, Vec<Value>)],
) -> Result<ReferentialIndexUpdates, ultrasql_executor::ExecError> {
    let mut out = Vec::with_capacity(indexes.len());
    for index in indexes {
        let columns: Vec<usize> = index.columns.iter().map(|col| usize::from(*col)).collect();
        let encoding = IndexKeyEncoding::for_columns(&child.schema, &columns)
            .map_err(|e| ultrasql_executor::ExecError::TypeMismatch(e.to_string()))?;
        let tree = BTree::open(
            Arc::clone(heap.buffer_pool()),
            RelationId::new(index.oid.raw()),
            index.root_block,
        );
        let mut changes = Vec::with_capacity(rows.len());
        for (tid, row) in rows {
            let old_key = encode_index_key(&encoding, &columns, row, &index.name)?;
            changes.push(ReferentialIndexUpdate {
                old_tid: *tid,
                old_key,
                new_key: None,
            });
        }
        out.push((index.name.clone(), tree, changes));
    }
    Ok(out)
}

fn update_new_index_keys(
    child: &TableEntry,
    indexes: &[IndexEntry],
    updates: &mut ReferentialIndexUpdates,
    row_idx: usize,
    row: &[Value],
) -> Result<(), ultrasql_executor::ExecError> {
    for (idx, index) in indexes.iter().enumerate() {
        let columns: Vec<usize> = index.columns.iter().map(|col| usize::from(*col)).collect();
        let encoding = IndexKeyEncoding::for_columns(&child.schema, &columns)
            .map_err(|e| ultrasql_executor::ExecError::TypeMismatch(e.to_string()))?;
        let new_key = encode_index_key(&encoding, &columns, row, &index.name)?;
        let Some((_name, _tree, changes)) = updates.get_mut(idx) else {
            return Err(ultrasql_executor::ExecError::Internal(
                "referential index update missing index slot",
            ));
        };
        let Some(change) = changes.get_mut(row_idx) else {
            return Err(ultrasql_executor::ExecError::Internal(
                "referential index update missing row slot",
            ));
        };
        change.new_key = new_key;
    }
    Ok(())
}

fn encode_index_key(
    encoding: &IndexKeyEncoding,
    columns: &[usize],
    row: &[Value],
    index_name: &str,
) -> Result<Option<i64>, ultrasql_executor::ExecError> {
    match columns {
        [col] => {
            let value = row.get(*col).ok_or_else(|| {
                ultrasql_executor::ExecError::TypeMismatch(format!(
                    "index {index_name}: row missing key column {col}"
                ))
            })?;
            encoding
                .encode_value(value)
                .map_err(|e| ultrasql_executor::ExecError::TypeMismatch(e.to_string()))
        }
        _ => encoding
            .encode_row(row)
            .map_err(|e| ultrasql_executor::ExecError::TypeMismatch(e.to_string())),
    }
}

fn precheck_referential_index_updates(
    updates: &ReferentialIndexUpdates,
) -> Result<(), ultrasql_executor::ExecError> {
    for (name, tree, changes) in updates {
        for change in changes {
            let Some(new_key) = change.new_key else {
                continue;
            };
            if change.old_key == Some(new_key) {
                continue;
            }
            if tree
                .lookup::<i64>(new_key)
                .map_err(|e| ultrasql_executor::ExecError::TypeMismatch(e.to_string()))?
                .is_some()
            {
                return Err(ultrasql_executor::ExecError::UniqueViolation(name.clone()));
            }
        }
    }
    Ok(())
}

fn apply_referential_index_updates(
    updates: ReferentialIndexUpdates,
    outcomes: &[ultrasql_storage::heap::UpdateOutcome],
    xid: Xid,
    wal: Option<&dyn ultrasql_storage::WalSink>,
) -> Result<(), ultrasql_executor::ExecError> {
    let new_tid_by_old: HashMap<TupleId, TupleId> = outcomes
        .iter()
        .map(|outcome| (outcome.old_tid, outcome.new_tid))
        .collect();
    for (name, mut tree, changes) in updates {
        for change in changes {
            let Some(new_tid) = new_tid_by_old.get(&change.old_tid).copied() else {
                return Err(ultrasql_executor::ExecError::Internal(
                    "heap update_many_with_outcomes omitted referential action TID",
                ));
            };
            if let Some(old_key) = change.old_key {
                tree.delete_logged::<i64>(old_key, change.old_tid, xid, wal)
                    .map_err(|e| {
                        ultrasql_executor::ExecError::TypeMismatch(format!(
                            "index delete {name}: {e}"
                        ))
                    })?;
            }
            if let Some(new_key) = change.new_key {
                tree.insert::<i64>(new_key, new_tid, xid, wal)
                    .map_err(|e| {
                        ultrasql_executor::ExecError::TypeMismatch(format!(
                            "index insert {name}: {e}"
                        ))
                    })?;
            }
        }
    }
    Ok(())
}

fn build_insert_index_maintainers(
    entry: &TableEntry,
    ctx: &LowerCtx<'_>,
) -> Result<Vec<InsertIndexMaintainer<crate::BlankPageLoader>>, ServerError> {
    let Some(indexes) = ctx.catalog_snapshot.indexes_by_table.get(&entry.oid) else {
        return Ok(Vec::new());
    };
    let mut out = Vec::with_capacity(indexes.len());
    for index in indexes {
        out.push(build_one_insert_index_maintainer(entry, index, ctx)?);
    }
    Ok(out)
}

fn build_one_insert_index_maintainer(
    entry: &TableEntry,
    index: &IndexEntry,
    ctx: &LowerCtx<'_>,
) -> Result<InsertIndexMaintainer<crate::BlankPageLoader>, ServerError> {
    let columns: Vec<usize> = index
        .columns
        .iter()
        .map(|attnum| usize::from(*attnum))
        .collect();
    let encoding = crate::index_key::IndexKeyEncoding::for_columns(&entry.schema, &columns)?;
    let index_rel = RelationId::new(index.oid.raw());
    let tree = BTree::open(
        Arc::clone(ctx.heap.buffer_pool()),
        index_rel,
        index.root_block,
    );
    let index_name = index.name.clone();
    let encoder: InsertIndexEncoder = Arc::new(move |row: &[Value]| {
        let encoded = match columns.as_slice() {
            [col] => {
                let value = row.get(*col).ok_or_else(|| {
                    ultrasql_executor::ExecError::TypeMismatch(format!(
                        "index {index_name}: row missing key column {col}"
                    ))
                })?;
                encoding.encode_value(value).map_err(|e| {
                    ultrasql_executor::ExecError::TypeMismatch(format!("index {index_name}: {e}"))
                })?
            }
            _ => encoding.encode_row(row).map_err(|e| {
                ultrasql_executor::ExecError::TypeMismatch(format!("index {index_name}: {e}"))
            })?,
        };
        Ok(encoded)
    });
    Ok(InsertIndexMaintainer::new(
        index.name.clone(),
        tree,
        encoder,
        index.is_unique,
    ))
}

/// Build a TID-emitting [`SeqScan`] over a persistent relation.
///
/// The resulting operator emits rows shaped
/// `[tid_block: Int32, tid_slot: Int32, ...payload_cols]`, which is the
/// contract [`ModifyTable`] expects for UPDATE and DELETE.
pub(super) fn build_tid_seq_scan(entry: &TableEntry, ctx: &LowerCtx<'_>) -> Box<dyn Operator> {
    let rel = RelationId(entry.oid);
    let block_count = ctx.heap.block_count(rel).max(entry.n_blocks);
    let codec = RowCodec::new(entry.schema.clone());
    let scan = SeqScan::new_with_tids_and_vm(
        Arc::clone(&ctx.heap),
        rel,
        block_count,
        ctx.snapshot.clone(),
        Arc::clone(&ctx.oracle),
        Arc::clone(&ctx.vm),
        codec,
    );
    Box::new(scan)
}

/// Recursively rebuild `expr`, adding `by` to every
/// [`ScalarExpr::Column`] index. Used by UPDATE / DELETE lowering: the
/// scan now emits `[tid_block, tid_slot, ...orig_cols]`, but the
/// binder produced column indices against the un-prefixed schema, so
/// every reference must shift by +2 to remain correct.
///
/// Subquery-bearing variants (`ScalarSubquery`, `Exists`,
/// `InSubquery`, `OuterColumn`) are not shifted — those would require
/// recursing into a `LogicalPlan` and rewriting the outer-column
/// references, which is out of scope for the basic UPDATE/DELETE path
/// in this commit. The helper returns those variants verbatim; if a

pub(super) fn try_build_fused_update(
    target_table: &str,
    entry: &TableEntry,
    assignments: &[(usize, ScalarExpr)],
    input: &LogicalPlan,
    ctx: &LowerCtx<'_>,
) -> Result<Option<Box<dyn Operator>>, ServerError> {
    // Schema must be exactly (Int32, Int32). No extra columns, no
    // NULLability change — `FusedUpdateInt32Add` reads a fixed
    // 9-byte payload layout.
    let fields = entry.schema.fields();
    if fields.len() != 2
        || fields[0].data_type != DataType::Int32
        || fields[1].data_type != DataType::Int32
    {
        return Ok(None);
    }

    if assignments.len() != 1 {
        return Ok(None);
    }
    let (target_col_usize, assign_expr) = &assignments[0];
    if *target_col_usize > 1 {
        return Ok(None);
    }
    let target_col = u8::try_from(*target_col_usize).expect("target_col fits in u8");

    // The assignment body must read the target column and add (or
    // subtract) an Int32 literal. Subtraction is normalised to
    // `delta = -literal`.
    let (op, left, right) = match assign_expr {
        ScalarExpr::Binary {
            op, left, right, ..
        } => (*op, left.as_ref(), right.as_ref()),
        _ => return Ok(None),
    };
    let read_col_idx = |e: &ScalarExpr| -> Option<usize> {
        match e {
            ScalarExpr::Column {
                index,
                data_type: DataType::Int32,
                ..
            } => Some(*index),
            _ => None,
        }
    };
    let read_lit_i32 = |e: &ScalarExpr| -> Option<i32> {
        match e {
            ScalarExpr::Literal {
                value: Value::Int32(v),
                ..
            } => Some(*v),
            _ => None,
        }
    };
    let delta: i32 = match op {
        BinaryOp::Add => {
            if let (Some(c), Some(l)) = (read_col_idx(left), read_lit_i32(right)) {
                if c != *target_col_usize {
                    return Ok(None);
                }
                l
            } else if let (Some(l), Some(c)) = (read_lit_i32(left), read_col_idx(right)) {
                if c != *target_col_usize {
                    return Ok(None);
                }
                l
            } else {
                return Ok(None);
            }
        }
        BinaryOp::Sub => {
            // Only `col - lit` is well-defined as `+ (-lit)` —
            // `lit - col` does not decompose to a single add.
            if let (Some(c), Some(l)) = (read_col_idx(left), read_lit_i32(right)) {
                if c != *target_col_usize {
                    return Ok(None);
                }
                l.checked_neg().ok_or(ServerError::Plan(
                    ultrasql_planner::PlanError::TypeMismatch(
                        "UPDATE delta overflows i32 negation".to_owned(),
                    ),
                ))?
            } else {
                return Ok(None);
            }
        }
        _ => return Ok(None),
    };

    // Validate input shape and extract the optional predicate. The
    // shape contract mirrors `build_filtered_tid_scan`'s contract
    // (Scan or Filter(Scan) over the same target table).
    let predicate: Option<FusedPredicate> = match input {
        LogicalPlan::Scan { table, .. } => {
            if !table.eq_ignore_ascii_case(target_table) {
                return Ok(None);
            }
            None
        }
        LogicalPlan::Filter {
            input: filter_input,
            predicate,
        } => {
            let LogicalPlan::Scan { table, .. } = filter_input.as_ref() else {
                return Ok(None);
            };
            if !table.eq_ignore_ascii_case(target_table) {
                return Ok(None);
            }
            let Some((pred_col_idx, cmp, lit)) = extract_int32_col_op_lit(predicate) else {
                return Ok(None);
            };
            if pred_col_idx > 1 {
                return Ok(None);
            }
            let fused_cmp = match cmp {
                ultrasql_vec::kernels::CmpOp::Eq => FusedCmp::Eq,
                ultrasql_vec::kernels::CmpOp::Ne => FusedCmp::Ne,
                ultrasql_vec::kernels::CmpOp::Lt => FusedCmp::Lt,
                ultrasql_vec::kernels::CmpOp::Le => FusedCmp::Le,
                ultrasql_vec::kernels::CmpOp::Gt => FusedCmp::Gt,
                ultrasql_vec::kernels::CmpOp::Ge => FusedCmp::Ge,
            };
            Some(FusedPredicate {
                col_index: u8::try_from(pred_col_idx).expect("col idx fits in u8"),
                op: fused_cmp,
                literal: lit,
            })
        }
        _ => return Ok(None),
    };

    let rel = RelationId(entry.oid);
    let block_count = ctx.heap.block_count(rel).max(entry.n_blocks);
    let op = FusedUpdateInt32Add::new(
        Arc::clone(&ctx.heap),
        rel,
        ctx.snapshot.clone(),
        Arc::clone(&ctx.oracle),
        block_count,
        predicate,
        target_col,
        delta,
        ctx.xid,
        ctx.command_id,
    )
    .with_visibility_map(Arc::clone(&ctx.vm));
    Ok(Some(Box::new(op)))
}

pub(super) fn lower_real_update(
    table: &str,
    assignments: &[(usize, ScalarExpr)],
    input: &LogicalPlan,
    returning: &[(ScalarExpr, String)],
    ctx: &LowerCtx<'_>,
) -> Result<Box<dyn Operator>, ServerError> {
    if !returning.is_empty() {
        return Err(ServerError::Unsupported("UPDATE ... RETURNING"));
    }
    let entry = ctx
        .catalog_snapshot
        .tables
        .get(&table.to_ascii_lowercase())
        .ok_or_else(|| {
            ServerError::Plan(ultrasql_planner::PlanError::TableNotFound(
                table.to_string(),
            ))
        })?;
    let has_indexes = ctx
        .catalog_snapshot
        .indexes_by_table
        .get(&entry.oid)
        .is_some_and(|indexes| !indexes.is_empty());
    let has_child_constraints = ctx.table_constraints.get(&entry.oid).is_some_and(|c| {
        c.generated_stored.iter().any(Option::is_some)
            || !c.checks.is_empty()
            || !c.foreign_keys.is_empty()
    });
    let has_parent_constraints = !build_referenced_by_update_checks(entry.oid, ctx)?.is_empty();

    // Fast-path: when the relation, assignment, and optional filter
    // all match the `(Int32, Int32) WHERE col cmp lit SET col_i =
    // col_i ± lit` shape, bypass the SeqScan + Filter + ModifyTable
    // chain entirely and lower to the single `FusedUpdateInt32Add`
    // operator. Saves ~150 µs / 10 000-row UPDATE on the bench shape
    // — see the operator's module header for the full motivation.
    if !has_indexes && !has_child_constraints && !has_parent_constraints {
        if let Some(fused) = try_build_fused_update(table, entry, assignments, input, ctx)? {
            return Ok(fused);
        }
    }

    let child = build_filtered_tid_scan(table, entry, input, ctx)?;

    // Assignment value expressions stay unshifted: `apply_update`
    // strips the leading [tid_block, tid_slot] pair before passing the
    // row to `Eval::eval`, so the value expression sees the relation's
    // natural column layout. Likewise, the assignment's *target*
    // column index addresses the relation schema directly.
    let assignments: Vec<(usize, ScalarExpr)> = assignments.to_vec();

    let rel = RelationId(entry.oid);
    let constraints = ctx.table_constraints.get(&entry.oid).map(|c| c.clone());
    let modify = ModifyTable::new(
        Arc::clone(&ctx.heap),
        rel,
        entry.schema.clone(),
        ModifyKind::Update { assignments },
        ctx.xid,
        ctx.command_id,
        ctx.xid,
        ctx.command_id,
        ctx.heap.wal_sink().cloned(),
        child,
    )
    .with_visibility_map(Arc::clone(&ctx.vm));
    let modify = if let Some(constraints) = constraints {
        modify
            .with_generated_stored(constraints.generated_stored.clone())
            .with_check_constraints(
                constraints
                    .checks
                    .iter()
                    .map(|check| (check.name.clone(), check.expr.clone()))
                    .collect(),
            )
            .with_foreign_key_checks(build_foreign_key_checks(&constraints.foreign_keys, ctx)?)
    } else {
        modify
    };
    let modify =
        modify.with_referenced_by_update_checks(build_referenced_by_update_checks(entry.oid, ctx)?);
    let modify = if has_indexes {
        modify.with_update_indexes(build_insert_index_maintainers(entry, ctx)?)
    } else {
        modify
    };
    Ok(Box::new(modify))
}

/// Try to detect the `(Int32, Int32) [WHERE col cmp lit]` DELETE
/// shape and lower it to [`FusedDeleteInt32Pair`]. Mirrors

pub(super) fn try_build_fused_delete(
    target_table: &str,
    entry: &TableEntry,
    input: &LogicalPlan,
    ctx: &LowerCtx<'_>,
) -> Result<Option<Box<dyn Operator>>, ServerError> {
    let fields = entry.schema.fields();
    if fields.len() != 2
        || fields[0].data_type != DataType::Int32
        || fields[1].data_type != DataType::Int32
    {
        return Ok(None);
    }

    let predicate: Option<FusedPredicate> = match input {
        LogicalPlan::Scan { table, .. } => {
            if !table.eq_ignore_ascii_case(target_table) {
                return Ok(None);
            }
            None
        }
        LogicalPlan::Filter {
            input: filter_input,
            predicate,
        } => {
            let LogicalPlan::Scan { table, .. } = filter_input.as_ref() else {
                return Ok(None);
            };
            if !table.eq_ignore_ascii_case(target_table) {
                return Ok(None);
            }
            let Some((pred_col_idx, cmp, lit)) = extract_int32_col_op_lit(predicate) else {
                return Ok(None);
            };
            if pred_col_idx > 1 {
                return Ok(None);
            }
            let fused_cmp = match cmp {
                ultrasql_vec::kernels::CmpOp::Eq => FusedCmp::Eq,
                ultrasql_vec::kernels::CmpOp::Ne => FusedCmp::Ne,
                ultrasql_vec::kernels::CmpOp::Lt => FusedCmp::Lt,
                ultrasql_vec::kernels::CmpOp::Le => FusedCmp::Le,
                ultrasql_vec::kernels::CmpOp::Gt => FusedCmp::Gt,
                ultrasql_vec::kernels::CmpOp::Ge => FusedCmp::Ge,
            };
            Some(FusedPredicate {
                col_index: u8::try_from(pred_col_idx).expect("col idx fits in u8"),
                op: fused_cmp,
                literal: lit,
            })
        }
        _ => return Ok(None),
    };

    let rel = RelationId(entry.oid);
    let block_count = ctx.heap.block_count(rel).max(entry.n_blocks);
    let op = FusedDeleteInt32Pair::new(
        Arc::clone(&ctx.heap),
        rel,
        ctx.snapshot.clone(),
        Arc::clone(&ctx.oracle),
        block_count,
        predicate,
        ctx.xid,
        ctx.command_id,
    )
    .with_visibility_map(Arc::clone(&ctx.vm));
    Ok(Some(Box::new(op)))
}

/// Lower a `DELETE` plan into a [`ModifyTable`] with `ModifyKind::Delete`.
///

pub(super) fn lower_real_delete(
    table: &str,
    input: &LogicalPlan,
    returning: &[(ScalarExpr, String)],
    ctx: &LowerCtx<'_>,
) -> Result<Box<dyn Operator>, ServerError> {
    if !returning.is_empty() {
        return Err(ServerError::Unsupported("DELETE ... RETURNING"));
    }
    let entry = ctx
        .catalog_snapshot
        .tables
        .get(&table.to_ascii_lowercase())
        .ok_or_else(|| {
            ServerError::Plan(ultrasql_planner::PlanError::TableNotFound(
                table.to_string(),
            ))
        })?;
    let has_indexes = ctx
        .catalog_snapshot
        .indexes_by_table
        .get(&entry.oid)
        .is_some_and(|indexes| !indexes.is_empty());

    // Fast-path: when the relation matches the `(Int32, Int32)` shape
    // and the optional filter is `Int32 col cmp Int32 lit`, bypass
    // the SeqScan + Filter + ModifyTable chain and lower to the
    // single-pass `FusedDeleteInt32Pair` operator.
    if !has_indexes && build_referenced_by_delete_checks(entry.oid, ctx)?.is_empty() {
        if let Some(fused) = try_build_fused_delete(table, entry, input, ctx)? {
            return Ok(fused);
        }
    }

    let child = build_filtered_tid_scan(table, entry, input, ctx)?;

    let rel = RelationId(entry.oid);
    let modify = ModifyTable::new(
        Arc::clone(&ctx.heap),
        rel,
        entry.schema.clone(),
        ModifyKind::Delete,
        ctx.xid,
        ctx.command_id,
        ctx.xid,
        ctx.command_id,
        ctx.heap.wal_sink().cloned(),
        child,
    )
    .with_visibility_map(Arc::clone(&ctx.vm));
    let modify = if has_indexes {
        modify.with_delete_indexes(build_insert_index_maintainers(entry, ctx)?)
    } else {
        modify
    };
    let modify =
        modify.with_referenced_by_delete_checks(build_referenced_by_delete_checks(entry.oid, ctx)?);
    Ok(Box::new(modify))
}

/// Build the TID-emitting child operator for an UPDATE / DELETE.
///
/// Recognises the binder's `Scan` / `Filter(Scan)` shapes:
///
/// - bare `Scan { table }` → TID-emitting `SeqScan`.
/// - `Filter { Scan { table }, predicate }` → `Filter`(`SeqScan`),
///   with every `Column { index }` in `predicate` shifted by +2 to
///   re-target the TID-prefixed batch.
///
/// Any other input shape — the planner does not produce it for UPDATE

pub(super) fn build_filtered_tid_scan(
    target_table: &str,
    entry: &TableEntry,
    input: &LogicalPlan,
    ctx: &LowerCtx<'_>,
) -> Result<Box<dyn Operator>, ServerError> {
    match input {
        LogicalPlan::Scan { table, .. } => {
            if !table.eq_ignore_ascii_case(target_table) {
                return Err(ServerError::Unsupported(
                    "UPDATE / DELETE child scan references a different table",
                ));
            }
            Ok(build_tid_seq_scan(entry, ctx))
        }
        LogicalPlan::Filter {
            input: filter_input,
            predicate,
        } => {
            let LogicalPlan::Scan { table, .. } = filter_input.as_ref() else {
                return Err(ServerError::Unsupported(
                    "UPDATE / DELETE WHERE input must be a base-table scan",
                ));
            };
            if !table.eq_ignore_ascii_case(target_table) {
                return Err(ServerError::Unsupported(
                    "UPDATE / DELETE child scan references a different table",
                ));
            }
            let scan = build_tid_seq_scan(entry, ctx);
            let shifted = shift_column_indices(predicate, 2);
            Ok(Box::new(Filter::new(scan, shifted)))
        }
        _ => Err(ServerError::Unsupported(
            "UPDATE / DELETE input shape; expected Scan or Filter(Scan)",
        )),
    }
}

pub(super) fn lower_project_columns(
    child: Box<dyn Operator>,
    exprs: &[(ScalarExpr, String)],
) -> Result<Box<dyn Operator>, ServerError> {
    // Fast path: every projection item is a bare column reference.
    // The downstream pipeline can then short-circuit through the
    // index-only `Project` operator and (when the indices match the
    // child schema) skip the projection wrapper entirely.
    //
    // When any item carries an expression (function call, arithmetic,
    // CASE, …) we route through the general `ProjectExprs` operator
    // that evaluates each `ScalarExpr` per row.
    let all_bare_columns = exprs
        .iter()
        .all(|(e, _)| matches!(e, ScalarExpr::Column { .. }));
    if !all_bare_columns {
        // Build the output schema before handing to the operator;
        // each projection's output type is the bound expression's
        // declared type, named after the alias / derived label.
        let mut fields: Vec<ultrasql_core::Field> = Vec::with_capacity(exprs.len());
        for (e, name) in exprs {
            fields.push(ultrasql_core::Field::nullable(name.clone(), e.data_type()));
        }
        let output_schema = ultrasql_core::Schema::new(fields).map_err(|e| {
            ServerError::Plan(ultrasql_planner::PlanError::TypeMismatch(format!(
                "projection schema: {e}"
            )))
        })?;
        return ultrasql_executor::ProjectExprs::new(child, exprs, output_schema)
            .map(|op| Box::new(op) as Box<dyn Operator>)
            .map_err(|e| {
                ServerError::Plan(ultrasql_planner::PlanError::TypeMismatch(format!(
                    "projection: {e}"
                )))
            });
    }
    let mut indices: Vec<usize> = Vec::with_capacity(exprs.len());
    for (expr, _name) in exprs {
        match expr {
            ScalarExpr::Column { index, .. } => indices.push(*index),
            _ => unreachable!("filtered to bare columns above"),
        }
    }
    // Identity-projection elision: if the requested indices exactly
    // mirror the child's column order (`[0, 1, .., n-1]`) and cover
    // every child column **with the same output names**, the
    // [`Project`] wrapper would just clone each `Column` into a fresh
    // `Vec<Column>` on every batch — a per-batch `Vec<i32>` deep-copy
    // for narrow-int relations. Hand the child back to the caller
    // unchanged so the wire-encoder sees the scan's own batches
    // without an extra layer of clones.
    //
    // `SELECT id, val FROM t` over a two-column `(id INT NOT NULL,
    // val INT)` relation matches this shape — eliminating the
    // wrapper drops ~16 KiB/batch of memcpy on the `select_scan_10k`
    // workload. The name check guards against `SELECT id AS my_id
    // FROM t`, which keeps the same data flow but rebinds the wire
    // `RowDescription` column name and so must build a wrapping
    // projection to carry the alias.
    let child_schema = child.schema();
    let child_width = child_schema.len();
    let is_identity_indices =
        indices.len() == child_width && indices.iter().enumerate().all(|(i, &idx)| i == idx);
    let names_match = is_identity_indices
        && exprs
            .iter()
            .enumerate()
            .all(|(i, (_, name))| child_schema.field_at(i).name == *name);
    if names_match {
        return Ok(child);
    }
    let fields: Vec<ultrasql_core::Field> = exprs
        .iter()
        .map(|(expr, name)| ultrasql_core::Field::nullable(name.clone(), expr.data_type()))
        .collect();
    let output_schema = ultrasql_core::Schema::new(fields).map_err(|e| {
        ServerError::Plan(ultrasql_planner::PlanError::TypeMismatch(format!(
            "projection schema: {e}"
        )))
    })?;
    Ok(Box::new(Project::with_schema(
        child,
        indices,
        output_schema,
    )?))
}
