//! DELETE lowering: `lower_real_delete` plus the fused `(Int32, Int32)`
//! delete fast path.

use std::sync::Arc;

use ultrasql_catalog::TableEntry;
use ultrasql_core::{DataType, RelationId, Schema};
use ultrasql_executor::fused_delete::{FusedDeleteInt32Pair, FusedDeleteInt32PairConfig};
use ultrasql_executor::fused_update::{FusedCmp, FusedPredicate};
use ultrasql_executor::{ModifyKind, ModifyTable, ModifyTableStamps, Operator};
use ultrasql_planner::{LogicalPlan, ScalarExpr};

use crate::error::ServerError;
use crate::pipeline::LowerCtx;
use crate::pipeline::agg_fuse::extract_int32_col_op_lit;

use super::constraints::{build_referenced_by_delete_checks, has_referenced_by_delete_checks};
use super::indexes::{build_insert_index_maintainers, build_vector_index_maintainers};
use super::lowering::{build_eval_plan_qual, build_filtered_tid_scan};

/// Try to detect the `(Int32, Int32) [WHERE col cmp lit]` DELETE
/// shape and lower it to [`FusedDeleteInt32Pair`]. Mirrors

fn try_build_fused_delete(
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
                col_index: match u8::try_from(pred_col_idx) {
                    Ok(col_index) => col_index,
                    Err(_) => return Ok(None),
                },
                op: fused_cmp,
                literal: lit,
            })
        }
        _ => return Ok(None),
    };

    let rel = RelationId(entry.oid);
    let block_count = ctx.heap.block_count(rel).max(entry.n_blocks);
    let op = FusedDeleteInt32Pair::new(FusedDeleteInt32PairConfig {
        heap: Arc::clone(&ctx.heap),
        relation: rel,
        snapshot: ctx.snapshot.clone(),
        oracle: Arc::clone(&ctx.oracle),
        block_count,
        predicate,
        xid: ctx.xid,
        command_id: ctx.command_id,
    })
    .with_visibility_map(Arc::clone(&ctx.vm));
    Ok(Some(Box::new(op)))
}

/// Lower a `DELETE` plan into a [`ModifyTable`] with `ModifyKind::Delete`.
///

pub(crate) fn lower_real_delete(
    table: &str,
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

    // Fast-path: when the relation matches the `(Int32, Int32)` shape
    // and the optional filter is `Int32 col cmp Int32 lit`, bypass
    // the SeqScan + Filter + ModifyTable chain and lower to the
    // single-pass `FusedDeleteInt32Pair` operator.
    if returning.is_empty()
        && !has_indexes
        && !has_referenced_by_delete_checks(entry.oid, &ctx.table_constraints)
    {
        if let Some(fused) = try_build_fused_delete(table, entry, input, ctx)? {
            return Ok(fused);
        }
    }

    let child = build_filtered_tid_scan(table, entry, input, ctx)?;
    crate::aggregating_index::mark_aggregating_indexes_dirty(entry, ctx);

    let rel = RelationId(entry.oid);
    let modify = ModifyTable::new(
        Arc::clone(&ctx.heap),
        rel,
        entry.schema.clone(),
        ModifyKind::Delete,
        ModifyTableStamps::new(ctx.xid, ctx.command_id, ctx.xid, ctx.command_id),
        ctx.heap.wal_sink().cloned(),
        child,
    )
    .with_visibility_map(Arc::clone(&ctx.vm))
    // General write-path locking discipline: every targeted row is locked
    // (blocking, deadlock-aware) and re-checked against the latest committed
    // version before it is marked deleted, so a concurrent FOR UPDATE /
    // UPDATE / DELETE serializes and a concurrently-deleted row is skipped.
    .with_eval_plan_qual(build_eval_plan_qual(entry, input, ctx));
    let modify = if has_indexes {
        modify
            .with_delete_indexes(build_insert_index_maintainers(entry, ctx)?)
            .with_delete_vector_indexes(build_vector_index_maintainers(entry, ctx)?)
    } else {
        modify
    };
    let modify =
        modify.with_referenced_by_delete_checks(build_referenced_by_delete_checks(entry.oid, ctx)?);
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
