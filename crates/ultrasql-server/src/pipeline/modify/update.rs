//! UPDATE lowering: `lower_real_update`, the fused `(Int32, Int32)`
//! add/subtract fast path, and indexed-update target-TID probing.

use std::sync::Arc;

use ultrasql_catalog::TableEntry;
use ultrasql_core::{DataType, RelationId, Schema, TupleId, Value, Xid};
use ultrasql_executor::fused_update::{
    FusedCmp, FusedPredicate, FusedUpdateInt32Add, FusedUpdateInt32AddConfig,
};
use ultrasql_executor::{ModifyKind, ModifyTable, ModifyTableStamps, Operator};
use ultrasql_planner::{BinaryOp, LogicalPlan, ScalarExpr};

use crate::error::ServerError;
use crate::pipeline::LowerCtx;
use crate::pipeline::agg_fuse::extract_int32_col_op_lit;
use crate::pipeline::index_scan::{
    find_single_column_index, key_type_for_btree, match_indexable_predicate,
    probe_index_entries_ordered,
};

use super::constraints::{build_exclusion_update_checks, build_foreign_key_checks};
use super::indexes::{build_insert_index_maintainers, build_vector_index_maintainers};
use super::insert::build_rls_update_checks;
use super::lowering::{build_eval_plan_qual, build_filtered_tid_scan};
use super::referential::build_referenced_by_update_checks;

/// Sentinel `SET col = col + <delta>` value that triggers the debug-only
/// post-row-lock panic used by the lock-leak isolation test. Chosen to be a
/// value no ordinary test or workload would update by. Compiled out of
/// release/ship binaries (the trigger is `#[cfg(debug_assertions)]`).
#[cfg(debug_assertions)]
const TEST_PANIC_AFTER_ROW_LOCK_DELTA: i32 = 0x7654_3210;

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

fn try_build_fused_update(
    target_table: &str,
    entry: &TableEntry,
    assignments: &[(usize, ScalarExpr)],
    input: &LogicalPlan,
    ctx: &LowerCtx<'_>,
) -> Result<Option<Box<dyn Operator>>, ServerError> {
    let fields = entry.schema.fields();
    let exact_int32_pair = fields.len() == 2
        && fields[0].data_type == DataType::Int32
        && fields[1].data_type == DataType::Int32;

    if assignments.len() != 1 {
        return Ok(None);
    }
    let (target_col_usize, assign_expr) = &assignments[0];
    let Some(target_field) = fields.get(*target_col_usize) else {
        return Ok(None);
    };
    if target_field.data_type.storage_type() != &DataType::Int32 {
        return Ok(None);
    }

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
            if exact_int32_pair && pred_col_idx > 1 {
                return Ok(None);
            }
            let Ok(pred_col_u8) = u8::try_from(pred_col_idx) else {
                return Ok(None);
            };
            let fused_cmp = match cmp {
                ultrasql_vec::kernels::CmpOp::Eq => FusedCmp::Eq,
                ultrasql_vec::kernels::CmpOp::Ne => FusedCmp::Ne,
                ultrasql_vec::kernels::CmpOp::Lt => FusedCmp::Lt,
                ultrasql_vec::kernels::CmpOp::Le => FusedCmp::Le,
                ultrasql_vec::kernels::CmpOp::Gt => FusedCmp::Gt,
                ultrasql_vec::kernels::CmpOp::Ge => FusedCmp::Ge,
            };
            Some(FusedPredicate {
                col_index: pred_col_u8,
                op: fused_cmp,
                literal: lit,
            })
        }
        _ => return Ok(None),
    };

    let target_tids = if let LogicalPlan::Filter { predicate, .. } = input {
        try_indexed_update_target_tids(entry, predicate, ctx)?
    } else {
        None
    };

    let rel = RelationId(entry.oid);
    if exact_int32_pair {
        let Ok(target_col) = u8::try_from(*target_col_usize) else {
            return Ok(None);
        };
        let block_count = ctx.heap.block_count(rel).max(entry.n_blocks);
        let op = FusedUpdateInt32Add::new(FusedUpdateInt32AddConfig {
            heap: Arc::clone(&ctx.heap),
            relation: rel,
            snapshot: ctx.snapshot.clone(),
            oracle: Arc::clone(&ctx.oracle),
            block_count,
            predicate,
            target_col,
            delta,
            xid: ctx.xid,
            command_id: ctx.command_id,
        });
        let op = if let Some(target_tids) = target_tids {
            let refresh_after_lock = ctx.isolation == ultrasql_txn::IsolationLevel::ReadCommitted;
            let lock_manager = Arc::clone(&ctx.oracle.lock_manager);
            let xid = ctx.xid;
            op.with_target_tids(target_tids).with_target_tid_lock(
                move |tid| {
                    let acquired = acquire_indexed_update_row_lock(&lock_manager, xid, tid)?;
                    // Debug-only panic AFTER the per-tuple Exclusive lock is held,
                    // for the lock-leak isolation test. Keyed off the sentinel
                    // delta so it fires only for `SET col = col + <SENTINEL>`;
                    // ordinary updates are untouched. Driven entirely by the SQL
                    // the test issues (no shared flag), so it is deterministic and
                    // thread-safe. Compiled out of release/ship binaries.
                    #[cfg(debug_assertions)]
                    if delta == TEST_PANIC_AFTER_ROW_LOCK_DELTA {
                        #[allow(clippy::panic)]
                        {
                            panic!("ultrasql test panic (debug-only, after row lock acquired)");
                        }
                    }
                    Ok(acquired)
                },
                refresh_after_lock,
            )
        } else {
            op
        }
        .with_visibility_map(Arc::clone(&ctx.vm));
        return Ok(Some(Box::new(op)));
    }

    Ok(None)
}

fn try_indexed_update_target_tids(
    entry: &TableEntry,
    predicate: &ScalarExpr,
    ctx: &LowerCtx<'_>,
) -> Result<Option<Vec<TupleId>>, ServerError> {
    let Some((col_idx, range)) = match_indexable_predicate(predicate) else {
        return Ok(None);
    };
    if range.low != range.high || key_type_for_btree(entry, col_idx).is_none() {
        return Ok(None);
    }
    let Some(index_entry) = find_single_column_index(&ctx.catalog_snapshot, entry, col_idx, ctx)
    else {
        return Ok(None);
    };
    let entries = probe_index_entries_ordered(index_entry, range, true, ctx)?;
    Ok(Some(entries.into_iter().map(|(_, tid)| tid).collect()))
}

fn update_requires_index_maintenance(
    entry: &TableEntry,
    assignments: &[(usize, ScalarExpr)],
    ctx: &LowerCtx<'_>,
) -> bool {
    let Some(indexes) = ctx.catalog_snapshot.indexes_by_table.get(&entry.oid) else {
        return false;
    };
    if indexes.is_empty() {
        return false;
    }

    let target_matches = |column: usize| assignments.iter().any(|(target, _)| *target == column);
    let constraints = ctx.table_constraints.get(&entry.oid);
    for index in indexes {
        if index
            .columns
            .iter()
            .any(|attnum| target_matches(usize::from(*attnum)))
        {
            return true;
        }

        let metadata = constraints
            .as_ref()
            .and_then(|constraints| constraints.indexes.get(&index.oid));
        if let Some(metadata) = metadata {
            if !metadata.key_exprs.is_empty()
                || metadata.predicate.is_some()
                || metadata.aggregating.is_some()
            {
                return true;
            }
            if metadata
                .include_columns
                .iter()
                .any(|column| target_matches(*column))
            {
                return true;
            }
        }
    }

    false
}

pub(super) fn acquire_indexed_update_row_lock(
    lock_manager: &ultrasql_txn::LockManager,
    xid: Xid,
    tid: TupleId,
) -> Result<bool, String> {
    let req = ultrasql_txn::LockRequest {
        xid,
        tag: ultrasql_txn::LockTag::Tuple(tid),
        mode: ultrasql_txn::LockMode::Exclusive,
    };
    match lock_manager.try_acquire(req) {
        Ok(true) => return Ok(false),
        Ok(false) => {}
        Err(e) => return Err(e.to_string()),
    }
    let acquire = || lock_manager.acquire(req).map_err(|e| e.to_string());
    if matches!(
        tokio::runtime::Handle::try_current().map(|handle| handle.runtime_flavor()),
        Ok(tokio::runtime::RuntimeFlavor::MultiThread)
    ) {
        tokio::task::block_in_place(acquire)?;
    } else {
        acquire()?;
    }
    Ok(true)
}

/// Acquire the blocking, deadlock-aware Exclusive tuple lock for the general
/// UPDATE / DELETE EvalPlanQual path, classifying the outcome so the server
/// surfaces the right SQLSTATE: a lock-wait cycle victim becomes
/// [`ExecError::DeadlockDetected`] (→ 40P01) and every other lock-manager
/// failure becomes [`ExecError::SerializationFailure`] (→ 40001). Reuses the
/// same `LockManager` `try_acquire` / blocking `acquire` path the fused fast
/// path and `SELECT ... FOR UPDATE` use, so all three serialize on the same
/// `LockTag::Tuple` grants.
pub(super) fn acquire_eval_plan_qual_row_lock(
    lock_manager: &ultrasql_txn::LockManager,
    xid: Xid,
    tid: TupleId,
) -> Result<bool, ultrasql_executor::ExecError> {
    use ultrasql_executor::ExecError;
    use ultrasql_txn::LockError;

    let req = ultrasql_txn::LockRequest {
        xid,
        tag: ultrasql_txn::LockTag::Tuple(tid),
        mode: ultrasql_txn::LockMode::Exclusive,
    };
    let classify = |e: LockError| -> ExecError {
        match e {
            LockError::Deadlock { .. } => ExecError::DeadlockDetected(e.to_string()),
            other => ExecError::SerializationFailure(other.to_string()),
        }
    };
    match lock_manager.try_acquire(req) {
        Ok(true) => return Ok(false),
        Ok(false) => {}
        Err(e) => return Err(classify(e)),
    }
    let acquire = || lock_manager.acquire(req).map_err(classify);
    if matches!(
        tokio::runtime::Handle::try_current().map(|handle| handle.runtime_flavor()),
        Ok(tokio::runtime::RuntimeFlavor::MultiThread)
    ) {
        tokio::task::block_in_place(acquire)?;
    } else {
        acquire()?;
    }
    Ok(true)
}

pub(crate) fn lower_real_update(
    table: &str,
    assignments: &[(usize, ScalarExpr)],
    input: &LogicalPlan,
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
    let has_indexes = ctx
        .catalog_snapshot
        .indexes_by_table
        .get(&entry.oid)
        .is_some_and(|indexes| !indexes.is_empty());
    let has_child_constraints = ctx.table_constraints.get(&entry.oid).is_some_and(|c| {
        c.generated_stored.iter().any(Option::is_some)
            || !c.checks.is_empty()
            || !c.foreign_keys.is_empty()
            || !c.exclusion_constraints.is_empty()
    });
    let has_parent_constraints = !build_referenced_by_update_checks(entry.oid, ctx)?.is_empty();
    let rls_update_checks = build_rls_update_checks(entry, ctx);

    // Fast-path: when the relation, assignment, and optional filter all
    // match the `(Int32, Int32) WHERE col cmp lit SET col_i = col_i ±
    // lit` shape, bypass the SeqScan + Filter + ModifyTable chain. This
    // is also safe for indexed tables when the update is in-place and no
    // maintained index state can depend on the assigned column.
    let index_maintenance_needed = update_requires_index_maintenance(entry, assignments, ctx);
    if returning.is_empty()
        && !index_maintenance_needed
        && !has_child_constraints
        && !has_parent_constraints
        && rls_update_checks.is_empty()
    {
        if let Some(fused) = try_build_fused_update(table, entry, assignments, input, ctx)? {
            return Ok(fused);
        }
    }

    let child = build_filtered_tid_scan(table, entry, input, ctx)?;
    crate::aggregating_index::mark_aggregating_indexes_dirty(entry, ctx);

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
        ModifyTableStamps::new(ctx.xid, ctx.command_id, ctx.xid, ctx.command_id),
        ctx.heap.wal_sink().cloned(),
        child,
    )
    .with_visibility_map(Arc::clone(&ctx.vm))
    .with_uniqueness_recheck(
        ctx.snapshot.clone(),
        Arc::clone(&ctx.oracle) as Arc<dyn ultrasql_mvcc::XidStatusOracle>,
    )
    // General write-path locking discipline: every targeted row is locked
    // (blocking, deadlock-aware) and re-checked against the latest committed
    // version before its new version is written, so a concurrent FOR UPDATE /
    // UPDATE / DELETE serializes and no update is lost.
    .with_eval_plan_qual(build_eval_plan_qual(entry, input, ctx));
    let mut check_constraints = rls_update_checks;
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
            .with_generated_stored(constraints.generated_stored.clone())
            .with_foreign_key_checks(build_foreign_key_checks(&constraints.foreign_keys, ctx)?)
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
    let modify = if has_indexes {
        modify
            .with_update_indexes(build_insert_index_maintainers(entry, ctx)?)
            .with_update_vector_indexes(build_vector_index_maintainers(entry, ctx)?)
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
