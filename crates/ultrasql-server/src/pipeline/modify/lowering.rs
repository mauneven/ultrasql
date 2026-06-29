//! Shared lowering helpers: the TID-emitting child scan for UPDATE /
//! DELETE and the projection-elision logic for SELECT lists.

use std::sync::Arc;

use ultrasql_executor::modify::eval_plan_qual::{EvalPlanQual, EvalPlanQualConfig, make_epq_fetch};
use ultrasql_executor::{Eval, Filter, Operator, Project, RowCodec};
use ultrasql_planner::{LogicalPlan, ScalarExpr};

use ultrasql_catalog::TableEntry;

use crate::error::ServerError;
use crate::pipeline::LowerCtx;
use crate::pipeline::agg_fuse::shift_column_indices;

use super::indexes::build_tid_seq_scan;
use super::update::acquire_eval_plan_qual_row_lock;

/// Build the TID-emitting child operator for an UPDATE / DELETE.
///
/// Recognises the binder's `Scan` / nested `Filter` shapes:
///
/// - bare `Scan { table }` → TID-emitting `SeqScan`.
/// - `Filter { input, predicate }` → `Filter`(lowered `input`),
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
            let child = build_filtered_tid_scan(target_table, entry, filter_input, ctx)?;
            let shifted = shift_column_indices(predicate, 2);
            Ok(Box::new(Filter::new(child, shifted)))
        }
        _ => Err(ServerError::Unsupported(
            "UPDATE / DELETE input shape; expected Scan or Filter(Scan)",
        )),
    }
}

/// Build the per-row Exclusive tuple lock + EvalPlanQual latest-version
/// re-check for a general UPDATE / DELETE on `entry`.
///
/// Reuses the **same** lock-manager path the fused fast path and
/// `SELECT ... FOR UPDATE` use (`acquire_eval_plan_qual_row_lock`, the typed
/// twin of the fused path's `acquire_indexed_update_row_lock`): the
/// blocking, deadlock-aware Exclusive `LockTag::Tuple` acquisition under the
/// session xid, so a concurrent FOR UPDATE / UPDATE / DELETE of the same row
/// serializes. After the lock is granted the EvalPlanQual re-reads the latest
/// committed version (READ COMMITTED) or aborts on a concurrent committed
/// write (REPEATABLE READ / SERIALIZABLE → 40001), exactly as the fused path
/// does via `refresh_after_lock` + the `WriteConflict` → 40001 relabel.
///
/// `input` is the binder's `Scan` / `Filter(Scan)` shape; its `Filter`
/// predicate (un-shifted, addressing the relation schema directly) drives the
/// READ COMMITTED predicate re-check against the latest row image.
pub(super) fn build_eval_plan_qual(
    entry: &TableEntry,
    input: &LogicalPlan,
    ctx: &LowerCtx<'_>,
) -> EvalPlanQual {
    let lock_manager = Arc::clone(&ctx.oracle.lock_manager);
    // Lock under the TOP-LEVEL xid: the lock manager releases by xid only at
    // txn end, so a lock taken inside a savepoint that is later rolled back
    // must still be owned by the (stable) top-level xid — otherwise a re-lock
    // of the same row in a later statement self-blocks behind the dead subxid.
    // The acquiring subxid (`ctx.xid`) rides along as the grant's *owner* so a
    // `ROLLBACK TO` releases exactly the row locks taken since the savepoint
    // (the lock manager keys conflict/`release_all` on `lock_xid`, rollback
    // release on `owner`). When no savepoint is open the two are equal.
    let lock_xid = ctx.lock_xid;
    let lock_owner = ctx.xid;
    let lock = Arc::new(move |tid| {
        acquire_eval_plan_qual_row_lock(&lock_manager, lock_xid, lock_owner, tid)
    });

    let oracle_for_snapshot = Arc::clone(&ctx.oracle);
    let snapshot_xid = ctx.xid;
    let snapshot_command = ctx.command_id;
    let fresh_snapshot =
        Arc::new(move || oracle_for_snapshot.statement_snapshot(snapshot_xid, snapshot_command));

    let fetch = make_epq_fetch(Arc::clone(&ctx.heap));

    let oracle = Arc::clone(&ctx.oracle) as Arc<dyn ultrasql_mvcc::XidStatusOracle>;

    // The UPDATE/DELETE WHERE predicate lives in the child `Filter`. Its
    // column indices address the relation schema directly (the +2 TID shift
    // is applied only to the *scan* child), so it can be evaluated against a
    // decoded relation row as-is for the READ COMMITTED predicate re-check.
    let predicate = match input {
        LogicalPlan::Filter { predicate, .. } => Some(Eval::new(predicate.clone())),
        _ => None,
    };

    let codec = RowCodec::new(entry.schema.clone());

    EvalPlanQual::new(EvalPlanQualConfig {
        lock,
        fresh_snapshot,
        fetch,
        oracle,
        snapshot: ctx.snapshot.clone(),
        isolation: ctx.isolation,
        predicate,
        codec,
    })
}

pub(crate) fn lower_project_columns(
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
        let output_schema = ultrasql_core::Schema::new_with_duplicate_names(fields);
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
    let output_schema = ultrasql_core::Schema::new_with_duplicate_names(fields);
    Ok(Box::new(Project::with_schema(
        child,
        indices,
        output_schema,
    )?))
}
