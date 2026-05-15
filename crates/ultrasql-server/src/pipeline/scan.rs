//! Scan-lowering helpers and table-function scan.

#![allow(unused_imports, dead_code)]

use std::collections::HashMap;
use std::sync::Arc;

use ultrasql_catalog::{CatalogSnapshot, IndexEntry, TableEntry};
use ultrasql_core::{CommandId, DataType, Field, RelationId, Schema, Value, Xid};
use ultrasql_executor::filter_sum_op::{
    CachedAvgI32Scan, CachedFilterSumI32Scan, CachedSumI32Scan, FilterSumI32Scan,
};
use ultrasql_executor::fused_delete::FusedDeleteInt32Pair;
use ultrasql_executor::fused_update::{FusedCmp, FusedPredicate, FusedUpdateInt32Add};
use ultrasql_executor::physical::{BuildError, DataSource};
use ultrasql_executor::{
    CteScan, Filter, FilterEqI32, HashAggregate, HashJoin, IndexScan, Limit, MemTableScan,
    ModifyKind, ModifyTable, NestedLoopJoin, Operator, Project, ResultOp, RightFactory, RowCodec,
    SeqScan, SetOp, Sort, ValuesScan,
};
use ultrasql_mvcc::{Snapshot, Visibility, is_visible};
use ultrasql_planner::{
    BinaryOp, InMemoryCatalog, LogicalJoinCondition, LogicalJoinType, LogicalPlan, ScalarExpr,
    TableMeta,
};
use ultrasql_storage::btree::BTree;
use ultrasql_storage::heap::HeapAccess;
use ultrasql_txn::TransactionManager;
use ultrasql_vec::Batch;
use ultrasql_vec::column::{Column, NumericColumn, StringColumn};

use crate::BlankPageLoader;
use crate::error::ServerError;

use super::LowerCtx;

pub(super) fn lower_catalog_or_sample_scan(
    table: &str,
    plan_schema: &Schema,
    ctx: &LowerCtx<'_>,
) -> Result<Box<dyn Operator>, ServerError> {
    let folded = table.to_ascii_lowercase();
    if let Some(buffer) = ctx.cte_buffers.get(&folded) {
        return Ok(Box::new(CteScan::new(
            Arc::clone(&buffer.batches),
            plan_schema.clone(),
        )));
    }
    if let Some(entry) = ctx.catalog_snapshot.tables.get(&folded) {
        return Ok(lower_heap_scan(entry, ctx));
    }
    // Legacy path: sample tables.
    let sample = ctx.tables.lookup(table).ok_or_else(|| {
        ServerError::Plan(ultrasql_planner::PlanError::TableNotFound(
            table.to_string(),
        ))
    })?;
    Ok(Box::new(MemTableScan::new(
        sample.schema.clone(),
        sample.batches.clone(),
    )))
}

/// Construct a [`SeqScan`] for a real persistent relation.
pub(super) fn lower_heap_scan(entry: &TableEntry, ctx: &LowerCtx<'_>) -> Box<dyn Operator> {
    let rel = RelationId(entry.oid);
    // The catalog's `n_blocks` stat is an estimate; the heap's
    // counter is the truth. Take the larger of the two so a freshly
    // created table (entry.n_blocks = 0) still scans the blocks that
    // the heap has actually allocated through `insert`.
    let block_count = ctx.heap.block_count(rel).max(entry.n_blocks);
    let codec = RowCodec::new(entry.schema.clone());
    let mut scan = SeqScan::new(
        Arc::clone(&ctx.heap),
        rel,
        block_count,
        ctx.snapshot.clone(),
        Arc::clone(&ctx.oracle),
        codec,
    );
    // Thread the session's cancel flag through so `next_batch` returns
    // `ExecError::Cancelled` (→ SQLSTATE 57014) when a peer
    // `CancelRequest` flips it.
    if let Some(flag) = &ctx.cancel_flag {
        scan = scan.with_cancel_flag(flag.clone());
    }
    Box::new(scan)
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
