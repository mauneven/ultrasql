//! Referential-action machinery: cascade/set-null/set-default handling
//! for parent-side UPDATE and DELETE, plus the secondary-index
//! maintenance those actions require.

use std::collections::HashMap;
use std::sync::Arc;

use ultrasql_catalog::{IndexEntry, TableEntry};
use ultrasql_core::{BlockNumber, CommandId, DataType, RelationId, TupleId, Value, Xid};
use ultrasql_executor::{Eval, RowCodec, RowUpdateConstraintCheck, eval_error_to_exec_error};
use ultrasql_planner::LogicalReferentialAction;
use ultrasql_storage::btree::BTree;
use ultrasql_storage::heap::{DeleteOptions, UpdateOptions, UpdatePayload};

use crate::error::ServerError;
use crate::index_key::IndexKeyEncoding;
use crate::pipeline::LowerCtx;

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

pub(super) fn build_referenced_by_update_checks(
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
            if fk.deferrable
                && fk.initially_deferred
                && fk.on_update == LogicalReferentialAction::NoAction
            {
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
                    LogicalReferentialAction::Cascade => {
                        cascade_update_child_rows(CascadeUpdateChildRowsArgs {
                            heap: &heap,
                            child: &child,
                            indexes: &child_indexes,
                            rows: &child_rows,
                            child_columns: &child_columns,
                            target_columns: &target_columns,
                            new_parent_row: new_row,
                            constraints: &child_constraints,
                            xid,
                            command_id,
                            vm: &vm,
                        })
                    }
                    LogicalReferentialAction::SetNull | LogicalReferentialAction::SetDefault => {
                        update_child_rows_for_delete_action(UpdateChildRowsForDeleteActionArgs {
                            heap: &heap,
                            child: &child,
                            indexes: &child_indexes,
                            rows: &child_rows,
                            child_columns: &child_columns,
                            action: on_update,
                            constraints: &child_constraints,
                            sequences: &sequences,
                            sequence_state: sequence_state.as_ref(),
                            xid,
                            command_id,
                            vm: &vm,
                        })
                    }
                }
            }) as RowUpdateConstraintCheck);
        }
    }
    Ok(out)
}

pub(super) fn table_entry_by_oid(
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

pub(super) fn row_key(row: &[Value], columns: &[usize]) -> Option<Vec<Value>> {
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

pub(super) fn relation_has_key(
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

pub(super) fn matching_child_rows(
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

pub(super) fn cascade_delete_child_rows(
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
        if index.root_block == BlockNumber::INVALID {
            continue;
        }
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

pub(super) struct UpdateChildRowsForDeleteActionArgs<'a> {
    pub(super) heap: &'a ultrasql_storage::heap::HeapAccess<crate::BlankPageLoader>,
    pub(super) child: &'a TableEntry,
    pub(super) indexes: &'a [IndexEntry],
    pub(super) rows: &'a [(TupleId, Vec<Value>)],
    pub(super) child_columns: &'a [usize],
    pub(super) action: LogicalReferentialAction,
    pub(super) constraints: &'a crate::TableRuntimeConstraints,
    pub(super) sequences: &'a dashmap::DashMap<String, Arc<ultrasql_storage::sequence::Sequence>>,
    pub(super) sequence_state: Option<&'a crate::SequenceSessionState>,
    pub(super) xid: Xid,
    pub(super) command_id: CommandId,
    pub(super) vm: &'a ultrasql_storage::vm::VisibilityMap,
}

pub(super) fn update_child_rows_for_delete_action(
    args: UpdateChildRowsForDeleteActionArgs<'_>,
) -> Result<(), ultrasql_executor::ExecError> {
    let UpdateChildRowsForDeleteActionArgs {
        heap,
        child,
        indexes,
        rows,
        child_columns,
        action,
        constraints,
        sequences,
        sequence_state,
        xid,
        command_id,
        vm,
    } = args;
    if rows.is_empty() {
        return Ok(());
    }
    let codec = RowCodec::new(child.schema.clone());
    let mut edits: Vec<(TupleId, UpdatePayload)> = Vec::with_capacity(rows.len());
    let mut index_updates = build_referential_index_updates(heap, child, indexes, rows)?;
    let wal = heap.wal_sink().cloned();
    let wal_ref = wal.as_deref();

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
                LogicalReferentialAction::SetDefault => referential_default_value(
                    child,
                    col,
                    constraints,
                    sequences,
                    sequence_state,
                    wal_ref,
                    xid,
                )?,
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

struct CascadeUpdateChildRowsArgs<'a> {
    heap: &'a ultrasql_storage::heap::HeapAccess<crate::BlankPageLoader>,
    child: &'a TableEntry,
    indexes: &'a [IndexEntry],
    rows: &'a [(TupleId, Vec<Value>)],
    child_columns: &'a [usize],
    target_columns: &'a [usize],
    new_parent_row: &'a [Value],
    constraints: &'a crate::TableRuntimeConstraints,
    xid: Xid,
    command_id: CommandId,
    vm: &'a ultrasql_storage::vm::VisibilityMap,
}

fn cascade_update_child_rows(
    args: CascadeUpdateChildRowsArgs<'_>,
) -> Result<(), ultrasql_executor::ExecError> {
    let CascadeUpdateChildRowsArgs {
        heap,
        child,
        indexes,
        rows,
        child_columns,
        target_columns,
        new_parent_row,
        constraints,
        xid,
        command_id,
        vm,
    } = args;
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
    wal: Option<&dyn ultrasql_storage::WalSink>,
    xid: Xid,
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
        let raw = sequence
            .nextval_logged(seq_name, ultrasql_core::RelationId::INVALID, xid, wal)
            .map_err(|e| {
                ultrasql_executor::ExecError::TypeMismatch(format!(
                    "sequence default {seq_name}: {e}"
                ))
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
        .map_err(eval_error_to_exec_error)
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
            .map_err(eval_error_to_exec_error)?
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
        if index.root_block == BlockNumber::INVALID {
            continue;
        }
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
