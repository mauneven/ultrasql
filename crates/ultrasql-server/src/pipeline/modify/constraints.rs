//! Constraint check builders for mutation lowering: foreign keys,
//! exclusion constraints, and the referenced-by-DELETE cascade/restrict
//! checks.

use std::sync::Arc;

use ultrasql_catalog::TableEntry;
use ultrasql_core::{RelationId, Value};
use ultrasql_executor::{RowCodec, RowConstraintCheck, RowUpdateConstraintCheck};
use ultrasql_planner::{BinaryOp, LogicalReferentialAction};

use crate::error::ServerError;
use crate::pipeline::LowerCtx;
use crate::pipeline::modify::ConstraintCheckDeps;

use super::referential::{
    UpdateChildRowsForDeleteActionArgs, cascade_delete_child_rows, matching_child_rows,
    relation_has_key, row_key, table_entry_by_oid, update_child_rows_for_delete_action,
};

pub(super) fn build_foreign_key_checks(
    foreign_keys: &[crate::RuntimeForeignKeyConstraint],
    ctx: &LowerCtx<'_>,
) -> Result<Vec<RowConstraintCheck>, ServerError> {
    build_foreign_key_checks_from_deps(foreign_keys, &ConstraintCheckDeps::from_lower_ctx(ctx))
}

/// Build non-deferred FOREIGN KEY row checks from explicit dependencies.
///
/// The reuse seam shared by INSERT lowering and `COPY FROM`. Deferred FKs are
/// validated at COMMIT via the session-txn pending-modifications path, so they
/// are skipped here for both callers.
pub(crate) fn build_foreign_key_checks_from_deps(
    foreign_keys: &[crate::RuntimeForeignKeyConstraint],
    deps: &ConstraintCheckDeps<'_>,
) -> Result<Vec<RowConstraintCheck>, ServerError> {
    let mut out = Vec::with_capacity(foreign_keys.len());
    for fk in foreign_keys {
        if fk.deferrable && fk.initially_deferred {
            continue;
        }
        let parent = deps
            .catalog_snapshot
            .tables
            .get(&fk.target_table)
            .cloned()
            .ok_or_else(|| {
                ServerError::Plan(ultrasql_planner::PlanError::TableNotFound(
                    fk.target_table.clone(),
                ))
            })?;
        let heap = Arc::clone(deps.heap);
        let snapshot = deps.snapshot.clone();
        let oracle = Arc::clone(deps.oracle);
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

pub(super) fn build_exclusion_insert_checks(
    table: &TableEntry,
    exclusions: &[crate::RuntimeExclusionConstraint],
    ctx: &LowerCtx<'_>,
) -> Result<Vec<RowConstraintCheck>, ServerError> {
    build_exclusion_insert_checks_from_deps(
        table,
        exclusions,
        &ConstraintCheckDeps::from_lower_ctx(ctx),
    )
}

/// Build EXCLUDE insert row checks from explicit dependencies.
///
/// The reuse seam shared by INSERT lowering and `COPY FROM`. The closure
/// dedups within the current batch (the `pending` set) and heap-rechecks
/// committed/own-write rows under the snapshot — so two conflicting rows in
/// one COPY are caught just as two rows in one INSERT are.
pub(crate) fn build_exclusion_insert_checks_from_deps(
    table: &TableEntry,
    exclusions: &[crate::RuntimeExclusionConstraint],
    deps: &ConstraintCheckDeps<'_>,
) -> Result<Vec<RowConstraintCheck>, ServerError> {
    let mut out = Vec::with_capacity(exclusions.len());
    for exclusion in exclusions {
        let heap = Arc::clone(deps.heap);
        let snapshot = deps.snapshot.clone();
        let oracle = Arc::clone(deps.oracle);
        let table = table.clone();
        let constraint = exclusion.clone();
        let pending = Arc::new(parking_lot::Mutex::new(Vec::<Vec<Value>>::new()));
        out.push(Arc::new(move |row: &[Value]| {
            {
                let pending_rows = pending.lock();
                for existing in pending_rows.iter() {
                    if exclusion_rows_conflict(&constraint, row, existing)? {
                        return Err(ultrasql_executor::ExecError::ExclusionViolation(
                            constraint.name.clone(),
                        ));
                    }
                }
            }
            if relation_has_exclusion_conflict(
                &heap,
                &table,
                &constraint,
                row,
                None,
                &snapshot,
                &oracle,
            )? {
                return Err(ultrasql_executor::ExecError::ExclusionViolation(
                    constraint.name.clone(),
                ));
            }
            pending.lock().push(row.to_vec());
            Ok(())
        }) as RowConstraintCheck);
    }
    Ok(out)
}

pub(super) fn build_exclusion_update_checks(
    table: &TableEntry,
    exclusions: &[crate::RuntimeExclusionConstraint],
    ctx: &LowerCtx<'_>,
) -> Result<Vec<RowUpdateConstraintCheck>, ServerError> {
    let mut out = Vec::with_capacity(exclusions.len());
    for exclusion in exclusions {
        let heap = Arc::clone(&ctx.heap);
        let snapshot = ctx.snapshot.clone();
        let oracle = Arc::clone(&ctx.oracle);
        let table = table.clone();
        let constraint = exclusion.clone();
        out.push(Arc::new(move |old_row: &[Value], new_row: &[Value]| {
            if exclusion_key_unchanged(&constraint, old_row, new_row) {
                return Ok(());
            }
            if relation_has_exclusion_conflict(
                &heap,
                &table,
                &constraint,
                new_row,
                Some(old_row),
                &snapshot,
                &oracle,
            )? {
                return Err(ultrasql_executor::ExecError::ExclusionViolation(
                    constraint.name.clone(),
                ));
            }
            Ok(())
        }) as RowUpdateConstraintCheck);
    }
    Ok(out)
}

pub(super) fn build_referenced_by_delete_checks(
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
            if fk.deferrable
                && fk.initially_deferred
                && fk.on_delete == LogicalReferentialAction::NoAction
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
                        update_child_rows_for_delete_action(UpdateChildRowsForDeleteActionArgs {
                            heap: &heap,
                            child: &child,
                            indexes: &child_indexes,
                            rows: &child_rows,
                            child_columns: &child_columns,
                            action: on_delete,
                            constraints: &child_constraints,
                            sequences: &sequences,
                            sequence_state: sequence_state.as_ref(),
                            xid,
                            command_id,
                            vm: &vm,
                        })
                    }
                }
            }) as RowConstraintCheck);
        }
    }
    Ok(out)
}

pub(super) fn has_referenced_by_delete_checks(
    parent_oid: ultrasql_core::Oid,
    constraints: &dashmap::DashMap<ultrasql_core::Oid, Arc<crate::TableRuntimeConstraints>>,
) -> bool {
    constraints.iter().any(|item| {
        item.value().foreign_keys.iter().any(|fk| {
            fk.target_oid == parent_oid
                && !(fk.deferrable
                    && fk.initially_deferred
                    && fk.on_delete == LogicalReferentialAction::NoAction)
        })
    })
}

fn relation_has_exclusion_conflict(
    heap: &ultrasql_storage::heap::HeapAccess<crate::BlankPageLoader>,
    table: &TableEntry,
    constraint: &crate::RuntimeExclusionConstraint,
    row: &[Value],
    skip_row: Option<&[Value]>,
    snapshot: &ultrasql_mvcc::Snapshot,
    oracle: &ultrasql_txn::TransactionManager,
) -> Result<bool, ultrasql_executor::ExecError> {
    let relation = RelationId(table.oid);
    let block_count = heap.block_count(relation).max(table.n_blocks);
    let codec = RowCodec::new(table.schema.clone());
    for tuple in heap.scan_visible(relation, block_count, snapshot, oracle) {
        let tuple = tuple.map_err(|e| ultrasql_executor::ExecError::TypeMismatch(e.to_string()))?;
        let existing = codec
            .decode(&tuple.data)
            .map_err(|e| ultrasql_executor::ExecError::TypeMismatch(e.to_string()))?;
        if skip_row.is_some_and(|skip| skip == existing.as_slice()) {
            continue;
        }
        if exclusion_rows_conflict(constraint, row, &existing)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn exclusion_key_unchanged(
    constraint: &crate::RuntimeExclusionConstraint,
    old_row: &[Value],
    new_row: &[Value],
) -> bool {
    constraint
        .elements
        .iter()
        .all(|element| old_row.get(element.column) == new_row.get(element.column))
}

fn exclusion_rows_conflict(
    constraint: &crate::RuntimeExclusionConstraint,
    left: &[Value],
    right: &[Value],
) -> Result<bool, ultrasql_executor::ExecError> {
    for element in &constraint.elements {
        let Some(left_value) = left.get(element.column) else {
            return Err(ultrasql_executor::ExecError::TypeMismatch(format!(
                "exclusion constraint {} references missing column {}",
                constraint.name, element.column
            )));
        };
        let Some(right_value) = right.get(element.column) else {
            return Err(ultrasql_executor::ExecError::TypeMismatch(format!(
                "exclusion constraint {} references missing column {}",
                constraint.name, element.column
            )));
        };
        if matches!(
            (left_value, right_value),
            (Value::Null, _) | (_, Value::Null)
        ) {
            return Ok(false);
        }
        if !exclusion_operator_matches(element.op, left_value, right_value)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn exclusion_operator_matches(
    op: BinaryOp,
    left: &Value,
    right: &Value,
) -> Result<bool, ultrasql_executor::ExecError> {
    match op {
        BinaryOp::Eq => Ok(left == right),
        BinaryOp::Overlap => match (left, right) {
            (Value::Range(l), Value::Range(r)) => Ok(l.overlaps(r)),
            (Value::Geometry(l), Value::Geometry(r)) => Ok(l.overlaps(r)),
            _ => Ok(false),
        },
        BinaryOp::JsonContains => match (left, right) {
            (Value::Range(l), Value::Range(r)) => Ok(l.contains_range(r)),
            (Value::Geometry(l), Value::Geometry(r)) => Ok(l.contains_geometry(r)),
            _ => Ok(false),
        },
        BinaryOp::JsonContained => match (left, right) {
            (Value::Range(l), Value::Range(r)) => Ok(r.contains_range(l)),
            (Value::Geometry(l), Value::Geometry(r)) => Ok(r.contains_geometry(l)),
            _ => Ok(false),
        },
        other => Err(ultrasql_executor::ExecError::TypeMismatch(format!(
            "unsupported exclusion operator {other:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use dashmap::DashMap;
    use ultrasql_core::Oid;

    use super::*;

    fn fk_to_parent(
        target_oid: Oid,
        on_delete: LogicalReferentialAction,
        deferrable: bool,
        initially_deferred: bool,
    ) -> crate::RuntimeForeignKeyConstraint {
        crate::RuntimeForeignKeyConstraint {
            name: "fk_child_parent".to_owned(),
            columns: vec![0],
            target_table: "parent".to_owned(),
            target_oid,
            target_columns: vec![0],
            on_delete,
            on_update: LogicalReferentialAction::NoAction,
            deferrable,
            initially_deferred,
        }
    }

    #[test]
    fn referenced_by_delete_metadata_detects_only_eager_delete_actions() {
        let parent = Oid::new(10);
        let other_parent = Oid::new(11);
        let child = Oid::new(20);
        let constraints = DashMap::new();
        constraints.insert(
            child,
            Arc::new(crate::TableRuntimeConstraints {
                foreign_keys: vec![fk_to_parent(
                    parent,
                    LogicalReferentialAction::NoAction,
                    true,
                    true,
                )],
                ..crate::TableRuntimeConstraints::default()
            }),
        );

        assert!(!has_referenced_by_delete_checks(parent, &constraints));

        constraints.insert(
            child,
            Arc::new(crate::TableRuntimeConstraints {
                foreign_keys: vec![fk_to_parent(
                    parent,
                    LogicalReferentialAction::Restrict,
                    false,
                    false,
                )],
                ..crate::TableRuntimeConstraints::default()
            }),
        );
        assert!(has_referenced_by_delete_checks(parent, &constraints));

        constraints.insert(
            child,
            Arc::new(crate::TableRuntimeConstraints {
                foreign_keys: vec![fk_to_parent(
                    other_parent,
                    LogicalReferentialAction::Restrict,
                    false,
                    false,
                )],
                ..crate::TableRuntimeConstraints::default()
            }),
        );
        assert!(!has_referenced_by_delete_checks(parent, &constraints));
    }
}
