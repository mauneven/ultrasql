//! Scan-lowering helpers and table-function scan.

use std::sync::Arc;

use ultrasql_catalog::TableEntry;
use ultrasql_core::{RelationId, Value};
use ultrasql_executor::{
    CteScan, MemTableScan, Operator, ParallelSeqScan, Project, RowCodec, SeqScan,
    choose_parallel_seq_scan_workers,
};
use ultrasql_planner::ScalarExpr;

use crate::error::ServerError;

use super::LowerCtx;
use super::catalog_views::try_virtual_catalog_scan;

pub(super) fn lower_catalog_or_sample_scan(
    table: &str,
    projection: Option<&[usize]>,
    ctx: &LowerCtx<'_>,
) -> Result<Box<dyn Operator>, ServerError> {
    let folded = table.to_ascii_lowercase();
    if let Some(buffer) = ctx.cte_buffers.get(&folded) {
        let scan: Box<dyn Operator> = Box::new(CteScan::new(
            Arc::clone(&buffer.batches),
            buffer.schema.clone(),
        ));
        return apply_projection(scan, projection);
    }
    if let Some(scan) = try_virtual_catalog_scan(table, ctx)? {
        return apply_projection(scan, projection);
    }
    if let Some(entry) = ctx.catalog_snapshot.tables.get(&folded) {
        return lower_heap_scan(entry, projection, ctx);
    }
    // Legacy path: sample tables.
    let sample = ctx.tables.lookup(table).ok_or_else(|| {
        ServerError::Plan(ultrasql_planner::PlanError::TableNotFound(
            table.to_string(),
        ))
    })?;
    let scan: Box<dyn Operator> = Box::new(MemTableScan::new(
        sample.schema.clone(),
        sample.batches.clone(),
    ));
    apply_projection(scan, projection)
}

/// Construct a [`SeqScan`] for a real persistent relation.
pub(super) fn lower_heap_scan(
    entry: &TableEntry,
    projection: Option<&[usize]>,
    ctx: &LowerCtx<'_>,
) -> Result<Box<dyn Operator>, ServerError> {
    let rel = RelationId(entry.oid);
    // The catalog's `n_blocks` stat is an estimate; the heap's
    // counter is the truth. Take the larger of the two so a freshly
    // created table (entry.n_blocks = 0) still scans the blocks that
    // the heap has actually allocated through `insert`.
    let block_count = ctx.heap.block_count(rel).max(entry.n_blocks);
    let codec = RowCodec::new(entry.schema.clone());
    let workers = choose_parallel_seq_scan_workers(block_count, entry.schema.len());
    if workers > 1 && projection.is_none() {
        let scan = ParallelSeqScan::new_with_cancel(
            Arc::clone(&ctx.heap),
            rel,
            block_count,
            ctx.snapshot.clone(),
            Arc::clone(&ctx.oracle),
            Arc::clone(&ctx.vm),
            codec,
            ctx.cancel_flag.clone(),
            workers,
        );
        return Ok(Box::new(scan));
    }
    let mut scan = SeqScan::new_with_vm(
        Arc::clone(&ctx.heap),
        rel,
        block_count,
        ctx.snapshot.clone(),
        Arc::clone(&ctx.oracle),
        Arc::clone(&ctx.vm),
        codec,
    );
    // Thread the session's cancel flag through so `next_batch` returns
    // `ExecError::Cancelled` (→ SQLSTATE 57014) when a peer
    // `CancelRequest` flips it.
    if let Some(flag) = &ctx.cancel_flag {
        scan = scan.with_cancel_flag(flag.clone());
    }
    let scan: Box<dyn Operator> = Box::new(scan);
    apply_projection(scan, projection)
}

/// Construct a cache-building single-threaded [`SeqScan`] for a real
/// persistent relation.
///
/// Used by scalar aggregate miss paths where paying one serial scan to
/// populate the relation's `ColumnCache` is cheaper over repeated
/// executions than repeatedly choosing `ParallelSeqScan`, which does
/// not publish cache entries.
pub(super) fn lower_heap_seq_scan(
    entry: &TableEntry,
    ctx: &LowerCtx<'_>,
) -> Result<Box<dyn Operator>, ServerError> {
    let rel = RelationId(entry.oid);
    let block_count = ctx.heap.block_count(rel).max(entry.n_blocks);
    let codec = RowCodec::new(entry.schema.clone());
    let mut scan = SeqScan::new_with_vm(
        Arc::clone(&ctx.heap),
        rel,
        block_count,
        ctx.snapshot.clone(),
        Arc::clone(&ctx.oracle),
        Arc::clone(&ctx.vm),
        codec,
    );
    if let Some(flag) = &ctx.cancel_flag {
        scan = scan.with_cancel_flag(flag.clone());
    }
    Ok(Box::new(scan))
}

fn apply_projection(
    scan: Box<dyn Operator>,
    projection: Option<&[usize]>,
) -> Result<Box<dyn Operator>, ServerError> {
    if let Some(indices) = projection {
        Ok(Box::new(Project::new(scan, indices.to_vec())?))
    } else {
        Ok(scan)
    }
}

/// Lower a `LogicalPlan::FunctionScan { name, args, .. }` into the
/// matching set-returning-function operator. v0.5 supports
/// `generate_series(start, stop[, step])`.
pub(super) fn lower_function_scan(
    name: &str,
    args: &[ScalarExpr],
) -> Result<Box<dyn Operator>, ServerError> {
    if name != "generate_series" {
        return Err(ServerError::Unsupported(
            "table function (only generate_series in v0.5)",
        ));
    }
    if args.len() < 2 || args.len() > 3 {
        return Err(ServerError::Unsupported(
            "generate_series: expected (start, stop) or (start, stop, step)",
        ));
    }
    fn to_i64(e: &ScalarExpr) -> Result<i64, ServerError> {
        match e {
            ScalarExpr::Literal {
                value: Value::Int64(v),
                ..
            } => Ok(*v),
            ScalarExpr::Literal {
                value: Value::Int32(v),
                ..
            } => Ok(i64::from(*v)),
            ScalarExpr::Unary {
                op: ultrasql_planner::UnaryOp::Neg,
                expr,
                ..
            } => to_i64(expr).map(i64::wrapping_neg),
            _ => Err(ServerError::Unsupported(
                "generate_series: arguments must be integer literals",
            )),
        }
    }
    let start = to_i64(&args[0])?;
    let stop = to_i64(&args[1])?;
    let step = if args.len() == 3 {
        to_i64(&args[2])?
    } else {
        1
    };
    Ok(Box::new(ultrasql_executor::FunctionScan::generate_series(
        start, stop, step,
    )))
}
