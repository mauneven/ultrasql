//! Main query lowerer — turns a [`LogicalPlan`] into an [`Operator`]
//! tree. Includes the `WITH` dispatch (regular and recursive CTEs).

use std::sync::Arc;

use ultrasql_core::{DataType, RelationId, Schema, Value, constants::PAGE_SIZE};
use ultrasql_executor::unique::UniqueMode;
use ultrasql_executor::{
    Filter, HashAggregate, Limit, Operator, ProfiledOperator, ResultOp, Sort, TopK, Unique,
    ValuesScan,
};
use ultrasql_planner::{BinaryOp, LogicalPlan, ScalarExpr, SortKey};
use ultrasql_vec::Batch;

use crate::error::ServerError;

use super::agg_fuse::{
    try_lower_cached_scalar_aggregate_i32, try_lower_direct_scalar_aggregate,
    try_lower_fused_filter_sum_int,
};
use super::cte_helpers::{lower_recursive_cte, lower_set_op_real};
use super::hybrid_search::try_lower_hybrid_search_limit;
use super::index_scan::{
    try_hnsw_top_k_limit, try_index_only_scan, try_index_scan, try_late_materialization_project,
    try_ordered_index_scan, try_ordered_index_scan_limit,
};
use super::join::{LowerJoinArgs, lower_join};
use super::modify::{
    lower_project_columns, lower_real_delete, lower_real_insert, lower_real_update,
};
use super::saturate_row_count;
use super::scan::{
    lower_catalog_or_sample_scan, lower_function_scan, try_lower_read_csv_filter,
    try_lower_read_csv_project, try_lower_read_parquet_filter, try_lower_read_parquet_project,
};
use super::time_partition::try_lower_time_partition_filter_scan;
use super::tpch_q1::try_lower_tpch_q1;
use super::tpch_q2::try_lower_tpch_q2;
use super::tpch_q3::try_lower_tpch_q3;
use super::tpch_q4::try_lower_tpch_q4;
use super::tpch_q5::try_lower_tpch_q5;
use super::tpch_q6::try_lower_tpch_q6;
use super::tpch_q7::try_lower_tpch_q7;
use super::tpch_q8::try_lower_tpch_q8;
use super::tpch_q9::try_lower_tpch_q9;
use super::tpch_q10::try_lower_tpch_q10;
use super::tpch_q11::try_lower_tpch_q11;
use super::tpch_q12::try_lower_tpch_q12;
use super::tpch_q13::try_lower_tpch_q13;
use super::tpch_q14::try_lower_tpch_q14;
use super::tpch_q15::try_lower_tpch_q15;
use super::tpch_q16::try_lower_tpch_q16;
use super::tpch_q17::try_lower_tpch_q17;
use super::tpch_q18::try_lower_tpch_q18;
use super::tpch_q19::try_lower_tpch_q19;
use super::tpch_q20::try_lower_tpch_q20;
use super::tpch_q21::try_lower_tpch_q21;
use super::{CteBuffer, LowerCtx};

pub fn lower_query(
    plan: &LogicalPlan,
    ctx: &LowerCtx<'_>,
) -> Result<Box<dyn Operator>, ServerError> {
    let op = lower_query_inner(plan, ctx)?;
    if ctx.profile_operators {
        Ok(Box::new(ProfiledOperator::new(
            profile_operator_name(plan),
            op,
        )))
    } else {
        Ok(op)
    }
}

fn lower_query_inner(
    plan: &LogicalPlan,
    ctx: &LowerCtx<'_>,
) -> Result<Box<dyn Operator>, ServerError> {
    tracing::debug!(pipeline_mode = ?plan.pipeline_mode(), "lower logical pipeline");
    if let Some(tpch_q2) = try_lower_tpch_q2(plan)? {
        return Ok(tpch_q2);
    }
    if let Some(tpch_q3) = try_lower_tpch_q3(plan)? {
        return Ok(tpch_q3);
    }
    if let Some(tpch_q4) = try_lower_tpch_q4(plan)? {
        return Ok(tpch_q4);
    }
    if let Some(tpch_q5) = try_lower_tpch_q5(plan)? {
        return Ok(tpch_q5);
    }
    if let Some(tpch_q7) = try_lower_tpch_q7(plan)? {
        return Ok(tpch_q7);
    }
    if let Some(tpch_q8) = try_lower_tpch_q8(plan)? {
        return Ok(tpch_q8);
    }
    if let Some(tpch_q9) = try_lower_tpch_q9(plan)? {
        return Ok(tpch_q9);
    }
    if let Some(tpch_q10) = try_lower_tpch_q10(plan)? {
        return Ok(tpch_q10);
    }
    if let Some(tpch_q11) = try_lower_tpch_q11(plan)? {
        return Ok(tpch_q11);
    }
    if let Some(tpch_q12) = try_lower_tpch_q12(plan)? {
        return Ok(tpch_q12);
    }
    if let Some(tpch_q13) = try_lower_tpch_q13(plan)? {
        return Ok(tpch_q13);
    }
    if let Some(tpch_q14) = try_lower_tpch_q14(plan)? {
        return Ok(tpch_q14);
    }
    if let Some(tpch_q15) = try_lower_tpch_q15(plan)? {
        return Ok(tpch_q15);
    }
    if let Some(tpch_q16) = try_lower_tpch_q16(plan)? {
        return Ok(tpch_q16);
    }
    if let Some(tpch_q17) = try_lower_tpch_q17(plan)? {
        return Ok(tpch_q17);
    }
    if let Some(tpch_q18) = try_lower_tpch_q18(plan)? {
        return Ok(tpch_q18);
    }
    if let Some(tpch_q19) = try_lower_tpch_q19(plan)? {
        return Ok(tpch_q19);
    }
    if let Some(tpch_q20) = try_lower_tpch_q20(plan)? {
        return Ok(tpch_q20);
    }
    if let Some(tpch_q21) = try_lower_tpch_q21(plan)? {
        return Ok(tpch_q21);
    }
    match plan {
        LogicalPlan::Scan {
            table, projection, ..
        } => lower_catalog_or_sample_scan(table, projection.as_deref(), ctx),
        LogicalPlan::Insert {
            table,
            columns,
            source,
            on_conflict,
            returning,
            schema,
            ..
        } => lower_real_insert(
            table,
            columns,
            source,
            on_conflict.as_ref(),
            returning,
            schema,
            ctx,
        ),
        LogicalPlan::Values { rows, schema } => {
            Ok(Box::new(ValuesScan::new(rows.clone(), schema.clone())))
        }
        LogicalPlan::Project {
            input,
            exprs,
            schema,
        } => {
            let exprs = rewrite_catalog_scalar_functions(exprs, ctx)?;
            // `SELECT <const>` (no FROM) lowers Project(Empty) → ResultOp,
            // a single-row constant emitter. The general path below would
            // try to lower Empty into a scan, which has no meaning when
            // the projection is purely constant.
            if matches!(input.as_ref(), LogicalPlan::Empty { .. }) {
                let scalars: Vec<ScalarExpr> = exprs.iter().map(|(e, _)| e.clone()).collect();
                return Ok(Box::new(ResultOp::new(scalars, schema.clone())));
            }
            if let Some(op) = crate::aggregating_index::try_lower_aggregating_index_project(
                input, &exprs, schema, ctx,
            )? {
                return Ok(op);
            }
            if let Some(op) = try_index_only_scan(input, &exprs, ctx)? {
                return Ok(op);
            }
            if let Some(op) = try_late_materialization_project(input, &exprs, schema, ctx)? {
                return Ok(op);
            }
            if let Some(op) = try_lower_read_csv_project(input, &exprs)? {
                return Ok(op);
            }
            if let Some(op) = try_lower_read_parquet_project(input, &exprs)? {
                return Ok(op);
            }
            let child = lower_query(input, ctx)?;
            lower_project_columns(child, &exprs)
        }
        LogicalPlan::Filter { input, predicate } => {
            let predicate = rewrite_catalog_scalar_expr(predicate, ctx)?;
            if let Some(op) = try_lower_time_partition_filter_scan(input, &predicate, ctx)? {
                return Ok(op);
            }
            // Index-aware fast path: when the filter sits directly on top
            // of a catalog-resolved table scan and the predicate is one
            // of the indexable shapes recognised by `try_index_scan`, we
            // probe the B-tree and emit an [`IndexScan`] over the
            // matching tuple payloads — never materialising a SeqScan.
            //
            // The dispatcher returns `Ok(None)` when:
            //   - the input is not a bare `Scan { table }` over a
            //     persistent relation,
            //   - the table has no B-tree index on the predicate's
            //     column,
            //   - the predicate's shape is outside the indexable set,
            //   - the index's key column is not Int32 / Int64 (the only
            //     types A10 lifted into the on-disk B-tree).
            // In every miss case we fall back to the general
            // `Filter(SeqScan)` plan; that fallback is the existing
            // behaviour, so a query over an unindexed column or a
            // text-typed key never regresses.
            //
            // The dispatcher returns `Err(_)` only when an indexable
            // shape was recognised but probing the B-tree or fetching a
            // heap tuple raised a storage error — those are not
            // recoverable by falling back, so we propagate.
            if let Some(op) = try_index_scan(input, &predicate, ctx)? {
                return Ok(op);
            }
            if let Some(op) = try_lower_read_csv_filter(input, &predicate)? {
                return Ok(op);
            }
            if let Some(op) = try_lower_read_parquet_filter(input, &predicate)? {
                return Ok(op);
            }
            let child = lower_query(input, ctx)?;
            Ok(Box::new(Filter::new(child, predicate)))
        }
        LogicalPlan::Limit { input, n, offset } => {
            if let Some(op) = try_lower_hybrid_search_limit(input, *n, *offset, ctx)? {
                return Ok(op);
            }
            if let Some(op) = try_hnsw_top_k_limit(input, *n, *offset, ctx)? {
                return Ok(op);
            }
            if let Some(op) = try_ordered_index_scan_limit(input, *n, *offset, ctx)? {
                return Ok(op);
            }
            if let Some(op) = try_lower_exact_vector_top_k_limit(input, *n, *offset, ctx)? {
                return Ok(op);
            }
            let child = lower_query(input, ctx)?;
            let limit = saturate_row_count(*n);
            let offset = saturate_row_count(*offset);
            Ok(Box::new(Limit::with_offset(child, limit, offset)))
        }
        LogicalPlan::Empty { .. } => Err(ServerError::Unsupported("SELECT without FROM")),
        LogicalPlan::Sort { input, keys } => {
            if let Some(op) = try_ordered_index_scan(input, keys, ctx)? {
                return Ok(op);
            }
            // Lower the child first; the executor's `Sort` operator uses
            // the statement `work_mem` budget to choose in-memory sort or
            // external sorted runs, then emits 4096-row batches through the
            // same pull interface.
            // Vectorised vs scalar choice: the executor ships a
            // `VectorizedSort` in `vec_ops::sort` that operates on the
            // push-based pipeline driver (`VectorizedSink`/
            // `VectorizedOperator`). The Simple Query path runs the
            // pull-based `Operator` interface, so the drop-in is the
            // scalar `Sort` in `ultrasql_executor::sort`. The vectorised
            // variant would require lifting the entire pipeline to the
            // push driver, which is a v0.7 milestone (see ROADMAP §v0.7).
            let child = lower_query(input, ctx)?;
            let schema = child.schema().clone();
            Ok(Box::new(
                Sort::new(child, keys.clone(), schema)
                    .with_work_mem_budget(Arc::clone(&ctx.work_mem)),
            ))
        }
        LogicalPlan::Update {
            table,
            assignments,
            input,
            returning,
            schema,
            ..
        } => {
            if ctx
                .time_partitions
                .contains_key(&table.to_ascii_lowercase())
            {
                return Err(ServerError::Unsupported(
                    "UPDATE on partitioned tables is not yet routed to chunks",
                ));
            }
            lower_real_update(table, assignments, input, returning, schema, ctx)
        }
        LogicalPlan::Delete {
            table,
            input,
            returning,
            schema,
            ..
        } => {
            if ctx
                .time_partitions
                .contains_key(&table.to_ascii_lowercase())
            {
                return Err(ServerError::Unsupported(
                    "DELETE on partitioned tables is not yet routed to chunks",
                ));
            }
            lower_real_delete(table, input, returning, schema, ctx)
        }
        LogicalPlan::Truncate { .. }
        | LogicalPlan::CreateTable { .. }
        | LogicalPlan::CreateMaterializedView { .. }
        | LogicalPlan::CreateTypeEnum { .. }
        | LogicalPlan::CreateTypeComposite { .. }
        | LogicalPlan::CreateDomain { .. }
        | LogicalPlan::CreateOperator { .. }
        | LogicalPlan::CreateIndex { .. }
        | LogicalPlan::DropIndex { .. }
        | LogicalPlan::CreatePolicy { .. }
        | LogicalPlan::CreateRole { .. }
        | LogicalPlan::AlterRole { .. }
        | LogicalPlan::DropRole { .. }
        | LogicalPlan::GrantPrivileges { .. }
        | LogicalPlan::RevokePrivileges { .. }
        | LogicalPlan::AlterDefaultPrivileges { .. }
        | LogicalPlan::GrantRole { .. }
        | LogicalPlan::RevokeRole { .. }
        | LogicalPlan::CreateSchema { .. }
        | LogicalPlan::DropSchema { .. }
        | LogicalPlan::DropTable { .. }
        | LogicalPlan::AlterTable { .. }
        | LogicalPlan::CreateSequence { .. }
        | LogicalPlan::AlterSequence { .. }
        | LogicalPlan::DropSequence { .. }
        | LogicalPlan::Comment { .. } => Err(ServerError::Unsupported(
            "DDL reached operator lowerer; expected DDL dispatch path",
        )),
        LogicalPlan::Begin { .. }
        | LogicalPlan::Commit { .. }
        | LogicalPlan::Rollback { .. }
        | LogicalPlan::Savepoint { .. }
        | LogicalPlan::RollbackToSavepoint { .. }
        | LogicalPlan::ReleaseSavepoint { .. }
        | LogicalPlan::PrepareTransaction { .. }
        | LogicalPlan::CommitPrepared { .. }
        | LogicalPlan::RollbackPrepared { .. }
        | LogicalPlan::SetTransaction { .. }
        | LogicalPlan::SetVariable { .. }
        | LogicalPlan::SetRole { .. } => Err(ServerError::Unsupported(
            "session control reached operator lowerer; expected direct dispatch path",
        )),
        LogicalPlan::Listen { .. } | LogicalPlan::Notify { .. } | LogicalPlan::Unlisten { .. } => {
            Err(ServerError::Unsupported(
                "LISTEN/NOTIFY/UNLISTEN reached operator lowerer; expected pubsub dispatch path",
            ))
        }
        LogicalPlan::FunctionScan { name, args, .. } => {
            lower_function_scan(name, args, ctx.cancel_flag.clone())
        }
        LogicalPlan::Explain { .. } => Err(ServerError::Unsupported(
            "EXPLAIN reached operator lowerer; expected session dispatch path",
        )),
        LogicalPlan::Copy { .. } => Err(ServerError::Unsupported(
            "COPY reached operator lowerer; expected session dispatch path",
        )),
        LogicalPlan::Join {
            left,
            right,
            join_type,
            condition,
            schema,
        } => {
            // MergeJoin fast path: both children are an explicit `Sort`
            // over the equi-key. Skip the Sort wrappers and let the
            // executor's merge operator consume the already-ordered
            // streams without re-sorting.
            if let Some(op) =
                super::join::try_lower_merge_join(left, right, *join_type, condition, schema, ctx)?
            {
                return Ok(op);
            }

            // Lower the join's children first so the same real-heap path
            // (`SeqScan`-aware) feeds the operator. The selection rule
            // (HashJoin vs NestedLoopJoin) is delegated to `lower_join`
            // so the sample-table path in `lower_plan` and the
            // catalog-aware path here stay bit-identical in dispatch
            // semantics.
            let left_schema = left.schema().clone();
            let right_schema = right.schema().clone();
            let left_op = lower_query(left, ctx)?;
            let right_op = lower_query(right, ctx)?;
            lower_join(LowerJoinArgs {
                left_plan: left,
                right_plan: right,
                left: left_op,
                right: right_op,
                left_schema,
                right_schema,
                join_type: *join_type,
                condition,
                out_schema: schema.clone(),
                work_mem: Some(Arc::clone(&ctx.work_mem)),
            })
        }
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggregates,
            schema,
        } => {
            if aggregates.is_empty() && !group_by.is_empty() {
                if let LogicalPlan::Sort {
                    input: sort_input,
                    keys,
                } = input.as_ref()
                    && keys.len() == group_by.len()
                    && keys.iter().all(|k| k.asc)
                    && keys.iter().zip(group_by.iter()).all(|(k, g)| &k.expr == g)
                {
                    let child = lower_query(sort_input, ctx)?;
                    return Ok(Box::new(Unique::new(child, UniqueMode::Sort)));
                }
                let child = lower_query(input, ctx)?;
                return Ok(Box::new(Unique::new(child, UniqueMode::Hash)));
            }
            // Fast path: `SELECT SUM(int_col) FROM t WHERE int_col op lit`
            // collapses to one fused operator that runs SIMD
            // compare → mask → sum in a single
            // pass. Skips the per-batch `select_column` (per-row
            // scalar pushes) the generic Filter → HashAggregate
            // chain pays.
            if let Some(fused) = try_lower_fused_filter_sum_int(input, group_by, aggregates, ctx)? {
                return Ok(fused);
            }
            if let Some(tpch_q1) = try_lower_tpch_q1(input, group_by, aggregates, schema, ctx)? {
                return Ok(tpch_q1);
            }
            if let Some(tpch_q6) = try_lower_tpch_q6(input, group_by, aggregates, schema)? {
                return Ok(tpch_q6);
            }
            // Fast path: pure `SELECT SUM(col_i32) FROM t` /
            // `SELECT AVG(col_i32) FROM t` over a cache-live
            // relation. Reads the cached column directly through
            // the hand-NEON kernel — no SeqScan batch slicing.
            if let Some(direct) =
                try_lower_cached_scalar_aggregate_i32(input, group_by, aggregates, ctx)?
            {
                return Ok(direct);
            }
            // Direct columnar fast path: pure
            // `SELECT SUM/AVG/COUNT(*) FROM t` (no `WHERE`, no
            // `GROUP BY`) over an `Int32` or `Int64` column. Lowers
            // to `DirectScalarAggScan` over a bare `SeqScan` — one
            // SIMD kernel call per child batch, no HashAggregate
            // state machine. Fires on cache miss (the first scan
            // over a freshly-loaded relation), where the cached-
            // scalar fast path above returns `None` because the
            // column cache has not been populated yet. The second
            // and subsequent iterations of the
            // `cross_compare_sql --workload sum-scalar/avg-scalar`
            // bench take the cached path above; the first iter
            // and any cache-invalidated reload take this path.
            if let Some(direct) =
                try_lower_direct_scalar_aggregate(input, group_by, aggregates, ctx)?
            {
                return Ok(direct);
            }
            // SortAggregate fast path: input is `LogicalPlan::Sort` whose
            // keys exactly match the GROUP BY keys. Skip the Sort wrapper
            // and feed the inner plan into a streaming SortAggregate that
            // avoids the hash-table build cost.
            if let LogicalPlan::Sort {
                input: sort_input,
                keys,
            } = input.as_ref()
                && !group_by.is_empty()
                && keys.len() == group_by.len()
                && keys.iter().all(|k| k.asc)
                && keys.iter().zip(group_by.iter()).all(|(k, g)| &k.expr == g)
            {
                let child = lower_query(sort_input, ctx)?;
                return Ok(Box::new(ultrasql_executor::SortAggregate::new(
                    child,
                    group_by.clone(),
                    aggregates.clone(),
                    schema.clone(),
                )));
            }
            // Mirror `ultrasql_executor::physical::build_operator` — default
            // to a hash-based aggregate. The child is lowered recursively
            // through this same real-heap-aware path so the aggregate can
            // sit on top of a `SeqScan` over a persistent relation.
            let child = lower_query(input, ctx)?;
            let mut agg =
                HashAggregate::new(child, group_by.clone(), aggregates.clone(), schema.clone())
                    .with_work_mem_budget(Arc::clone(&ctx.work_mem));
            if let Some(flag) = &ctx.cancel_flag {
                agg = agg.with_cancel_flag(flag.clone());
            }
            Ok(Box::new(agg))
        }
        LogicalPlan::SetOp {
            op,
            quantifier,
            left,
            right,
            schema,
        } => lower_set_op_real(*op, *quantifier, left, right, schema.clone(), ctx),
        LogicalPlan::Cte {
            name,
            recursive,
            definition,
            body,
            ..
        } => lower_cte(name, *recursive, definition, body, ctx),
        LogicalPlan::LockRows { input, .. } => {
            // Production path: lower the child through the real-heap-aware
            // path, then wrap with LockRows. The lock-acquisition callback
            // is a no-op here; the server's session layer is responsible
            // for replacing it with a live TxnManager callback before
            // executing a genuine `SELECT FOR UPDATE` over a persistent
            // relation. For the in-memory fixture path the no-op is correct
            // (no concurrent writers, no need to acquire row locks).
            let child = lower_query(input, ctx)?;
            Ok(Box::new(ultrasql_executor::LockRows::new(
                child,
                Box::new(|_, _| Ok(())),
            )))
        }
        LogicalPlan::Window {
            input,
            partition_by,
            order_by,
            func,
            schema,
            ..
        } => {
            let child = lower_query(input, ctx)?;
            let order_exprs: Vec<ScalarExpr> = order_by.iter().map(|k| k.expr.clone()).collect();
            let kernel_func = lower_window_func(func);
            Ok(Box::new(ultrasql_executor::WindowAgg::new(
                child,
                partition_by.clone(),
                order_exprs,
                kernel_func,
                schema.clone(),
            )))
        }
    }
}

fn try_lower_exact_vector_top_k_limit(
    input: &LogicalPlan,
    limit: u64,
    offset: u64,
    ctx: &LowerCtx<'_>,
) -> Result<Option<Box<dyn Operator>>, ServerError> {
    if offset != 0 || limit == 0 || limit == u64::MAX {
        return Ok(None);
    }
    let limit = saturate_row_count(limit);
    match input {
        LogicalPlan::Sort {
            input: sort_input,
            keys,
        } => lower_exact_vector_sorted_input(sort_input, keys, limit, ctx),
        LogicalPlan::Project {
            input: project_input,
            exprs,
            ..
        } => {
            let LogicalPlan::Sort {
                input: sort_input,
                keys,
            } = project_input.as_ref()
            else {
                return Ok(None);
            };
            let Some(top_k) = lower_exact_vector_sorted_input(sort_input, keys, limit, ctx)? else {
                return Ok(None);
            };
            let top_k = if ctx.profile_operators {
                Box::new(ProfiledOperator::new("TopK", top_k)) as Box<dyn Operator>
            } else {
                top_k
            };
            lower_project_columns(top_k, exprs).map(Some)
        }
        _ => Ok(None),
    }
}

fn lower_exact_vector_sorted_input(
    sort_input: &LogicalPlan,
    keys: &[SortKey],
    limit: usize,
    ctx: &LowerCtx<'_>,
) -> Result<Option<Box<dyn Operator>>, ServerError> {
    if !is_exact_vector_top_k_keys(keys) {
        return Ok(None);
    }
    let child = lower_query(sort_input, ctx)?;
    let schema = child.schema().clone();
    Ok(Some(Box::new(TopK::new(
        child,
        keys.to_vec(),
        schema,
        limit,
    ))))
}

fn is_exact_vector_top_k_keys(keys: &[SortKey]) -> bool {
    let [key] = keys else {
        return false;
    };
    if !key.asc || key.nulls_first {
        return false;
    }
    let ScalarExpr::Binary {
        op, left, right, ..
    } = &key.expr
    else {
        return false;
    };
    matches!(
        op,
        BinaryOp::VectorL2Distance
            | BinaryOp::VectorCosineDistance
            | BinaryOp::VectorNegativeInnerProduct
            | BinaryOp::VectorL1Distance
    ) && (is_dense_vector_column_probe(left, right) || is_dense_vector_column_probe(right, left))
}

fn is_dense_vector_column_probe(column: &ScalarExpr, probe: &ScalarExpr) -> bool {
    let ScalarExpr::Column {
        data_type: DataType::Vector { .. } | DataType::HalfVec { .. },
        ..
    } = column
    else {
        return false;
    };
    matches!(
        probe,
        ScalarExpr::Literal {
            value: Value::Vector(_) | Value::HalfVec(_),
            ..
        }
    )
}

fn profile_operator_name(plan: &LogicalPlan) -> &'static str {
    match plan {
        LogicalPlan::Scan { .. } => "Seq Scan",
        LogicalPlan::Filter { .. } => "Filter",
        LogicalPlan::Project { .. } => "Result",
        LogicalPlan::Limit { .. } => "Limit",
        LogicalPlan::Sort { .. } => "Sort",
        LogicalPlan::Join { .. } => "Hash Join",
        LogicalPlan::Aggregate { .. } => "Aggregate",
        LogicalPlan::Values { .. } => "Values Scan",
        LogicalPlan::SetOp { .. } => "Set Op",
        LogicalPlan::Cte { .. } => "CTE",
        LogicalPlan::LockRows { .. } => "LockRows",
        LogicalPlan::FunctionScan { .. } => "Function Scan",
        LogicalPlan::Window { .. } => "WindowAgg",
        LogicalPlan::Insert { .. } => "Insert",
        LogicalPlan::Update { .. } => "Update",
        LogicalPlan::Delete { .. } => "Delete",
        LogicalPlan::Empty { .. } => "Empty",
        LogicalPlan::Truncate { .. } => "Truncate",
        LogicalPlan::CreateTable { .. } => "CreateTable",
        LogicalPlan::CreateMaterializedView { .. } => "CreateMaterializedView",
        LogicalPlan::CreateTypeEnum { .. } => "CreateTypeEnum",
        LogicalPlan::CreateTypeComposite { .. } => "CreateTypeComposite",
        LogicalPlan::CreateDomain { .. } => "CreateDomain",
        LogicalPlan::CreateOperator { .. } => "CreateOperator",
        LogicalPlan::CreateIndex { .. } => "CreateIndex",
        LogicalPlan::DropIndex { .. } => "DropIndex",
        LogicalPlan::CreatePolicy { .. } => "CreatePolicy",
        LogicalPlan::CreateRole { .. } => "CreateRole",
        LogicalPlan::AlterRole { .. } => "AlterRole",
        LogicalPlan::DropRole { .. } => "DropRole",
        LogicalPlan::GrantPrivileges { .. } => "GrantPrivileges",
        LogicalPlan::RevokePrivileges { .. } => "RevokePrivileges",
        LogicalPlan::AlterDefaultPrivileges { .. } => "AlterDefaultPrivileges",
        LogicalPlan::GrantRole { .. } => "GrantRole",
        LogicalPlan::RevokeRole { .. } => "RevokeRole",
        LogicalPlan::CreateSchema { .. } => "CreateSchema",
        LogicalPlan::DropSchema { .. } => "DropSchema",
        LogicalPlan::DropTable { .. } => "DropTable",
        LogicalPlan::AlterTable { .. } => "AlterTable",
        LogicalPlan::CreateSequence { .. } => "CreateSequence",
        LogicalPlan::AlterSequence { .. } => "AlterSequence",
        LogicalPlan::DropSequence { .. } => "DropSequence",
        LogicalPlan::Comment { .. } => "Comment",
        LogicalPlan::Begin { .. } => "Begin",
        LogicalPlan::Commit { .. } => "Commit",
        LogicalPlan::Rollback { .. } => "Rollback",
        LogicalPlan::Savepoint { .. } => "Savepoint",
        LogicalPlan::RollbackToSavepoint { .. } => "RollbackToSavepoint",
        LogicalPlan::ReleaseSavepoint { .. } => "ReleaseSavepoint",
        LogicalPlan::PrepareTransaction { .. } => "PrepareTransaction",
        LogicalPlan::CommitPrepared { .. } => "CommitPrepared",
        LogicalPlan::RollbackPrepared { .. } => "RollbackPrepared",
        LogicalPlan::SetTransaction { .. } => "SetTransaction",
        LogicalPlan::SetVariable { .. } => "SetVariable",
        LogicalPlan::SetRole { .. } => "SetRole",
        LogicalPlan::Explain { .. } => "Explain",
        LogicalPlan::Listen { .. } => "Listen",
        LogicalPlan::Notify { .. } => "Notify",
        LogicalPlan::Unlisten { .. } => "Unlisten",
        LogicalPlan::Copy { .. } => "Copy",
    }
}

fn rewrite_catalog_scalar_functions(
    exprs: &[(ScalarExpr, String)],
    ctx: &super::LowerCtx<'_>,
) -> Result<Vec<(ScalarExpr, String)>, ServerError> {
    exprs
        .iter()
        .map(|(expr, alias)| Ok((rewrite_catalog_scalar_expr(expr, ctx)?, alias.clone())))
        .collect()
}

fn rewrite_catalog_scalar_expr(
    expr: &ScalarExpr,
    ctx: &super::LowerCtx<'_>,
) -> Result<ScalarExpr, ServerError> {
    match expr {
        ScalarExpr::Unary {
            op,
            expr: inner,
            data_type,
        } => Ok(ScalarExpr::Unary {
            op: *op,
            expr: Box::new(rewrite_catalog_scalar_expr(inner, ctx)?),
            data_type: data_type.clone(),
        }),
        ScalarExpr::Binary {
            op,
            left,
            right,
            data_type,
        } => Ok(ScalarExpr::Binary {
            op: *op,
            left: Box::new(rewrite_catalog_scalar_expr(left, ctx)?),
            right: Box::new(rewrite_catalog_scalar_expr(right, ctx)?),
            data_type: data_type.clone(),
        }),
        ScalarExpr::IsNull {
            expr: inner,
            negated,
        } => Ok(ScalarExpr::IsNull {
            expr: Box::new(rewrite_catalog_scalar_expr(inner, ctx)?),
            negated: *negated,
        }),
        ScalarExpr::Exists {
            subplan,
            negated,
            correlated,
        } => {
            if *correlated {
                return Err(ServerError::Unsupported(
                    "correlated EXISTS in projection is not supported",
                ));
            }
            let mut op = lower_query(subplan, ctx)?;
            let exists = op.next_batch()?.is_some_and(|batch| !batch.is_empty());
            Ok(ScalarExpr::Literal {
                value: Value::Bool(if *negated { !exists } else { exists }),
                data_type: DataType::Bool,
            })
        }
        ScalarExpr::FunctionCall {
            name,
            args,
            data_type,
        } if name == "pg_relation_size" => {
            let rewritten_args: Result<Vec<_>, _> = args
                .iter()
                .map(|arg| rewrite_catalog_scalar_expr(arg, ctx))
                .collect();
            let rewritten_args = rewritten_args?;
            Ok(ScalarExpr::Literal {
                value: relation_size_from_literal_args(&rewritten_args, ctx)?,
                data_type: data_type.clone(),
            })
        }
        ScalarExpr::FunctionCall {
            name,
            args,
            data_type,
        } if is_privilege_check_function(name) => {
            let rewritten_args: Result<Vec<_>, _> = args
                .iter()
                .map(|arg| rewrite_catalog_scalar_expr(arg, ctx))
                .collect();
            let rewritten_args = rewritten_args?;
            Ok(ScalarExpr::Literal {
                value: privilege_check_from_literal_args(name, &rewritten_args, ctx)?,
                data_type: data_type.clone(),
            })
        }
        ScalarExpr::FunctionCall {
            name,
            args,
            data_type,
        } if matches!(name.as_str(), "current_user" | "session_user") => {
            if !args.is_empty() {
                return Err(ServerError::unsupported(format!(
                    "{name} expects zero arguments"
                )));
            }
            let value = if name == "current_user" {
                ctx.current_user.clone()
            } else {
                ctx.session_user.clone()
            };
            Ok(ScalarExpr::Literal {
                value: Value::Text(value),
                data_type: data_type.clone(),
            })
        }
        ScalarExpr::FunctionCall {
            name,
            args,
            data_type,
        } if is_advisory_lock_function(name) => {
            let rewritten_args: Result<Vec<_>, _> = args
                .iter()
                .map(|arg| rewrite_catalog_scalar_expr(arg, ctx))
                .collect();
            let rewritten_args = rewritten_args?;
            let values = literal_values(name, &rewritten_args)?;
            let Some(state) = ctx.advisory_state.as_ref() else {
                return Err(ServerError::Unsupported(
                    "advisory lock functions require session context",
                ));
            };
            let value = if is_transaction_advisory_lock_function(name) {
                state.evaluate_transaction_function(
                    name,
                    &values,
                    &ctx.oracle.lock_manager,
                    ctx.xid,
                )?
            } else {
                state.evaluate_function(name, &values, &ctx.oracle.lock_manager)?
            };
            Ok(ScalarExpr::Literal {
                value,
                data_type: data_type.clone(),
            })
        }
        ScalarExpr::FunctionCall {
            name,
            args,
            data_type,
        } => {
            let rewritten_args: Result<Vec<_>, _> = args
                .iter()
                .map(|arg| rewrite_catalog_scalar_expr(arg, ctx))
                .collect();
            Ok(ScalarExpr::FunctionCall {
                name: name.clone(),
                args: rewritten_args?,
                data_type: data_type.clone(),
            })
        }
        _ => Ok(expr.clone()),
    }
}

fn is_advisory_lock_function(name: &str) -> bool {
    matches!(
        name,
        "pg_advisory_lock"
            | "pg_try_advisory_lock"
            | "pg_try_advisory_xact_lock"
            | "pg_advisory_unlock"
            | "pg_advisory_unlock_all"
    )
}

fn is_transaction_advisory_lock_function(name: &str) -> bool {
    matches!(name, "pg_try_advisory_xact_lock")
}

fn is_privilege_check_function(name: &str) -> bool {
    matches!(
        name,
        "has_table_privilege"
            | "has_schema_privilege"
            | "has_database_privilege"
            | "has_sequence_privilege"
            | "has_function_privilege"
            | "has_column_privilege"
    )
}

fn literal_values(name: &str, args: &[ScalarExpr]) -> Result<Vec<Value>, ServerError> {
    args.iter()
        .map(|arg| match arg {
            ScalarExpr::Literal { value, .. } => Ok(value.clone()),
            _ => Err(ServerError::Execute(
                ultrasql_executor::ExecError::TypeMismatch(format!(
                    "{name}: advisory lock arguments must be constants",
                )),
            )),
        })
        .collect()
}

fn privilege_check_from_literal_args(
    name: &str,
    args: &[ScalarExpr],
    ctx: &super::LowerCtx<'_>,
) -> Result<Value, ServerError> {
    let expected = if name == "has_column_privilege" { 4 } else { 3 };
    if args.len() != expected {
        return Err(ServerError::unsupported(format!(
            "{name} expects exactly {expected} text arguments"
        )));
    }
    let mut texts = Vec::with_capacity(expected);
    for arg in args {
        let ScalarExpr::Literal { value, .. } = arg else {
            return Err(ServerError::unsupported(format!(
                "{name} currently requires literal arguments"
            )));
        };
        match value {
            Value::Null => return Ok(Value::Null),
            Value::Text(text) => texts.push(text.as_str()),
            other => {
                return Err(ServerError::unsupported(format!(
                    "{name} expects text arguments, got {:?}",
                    other.data_type()
                )));
            }
        }
    }
    if name == "has_column_privilege" {
        let privilege = privilege_kind_from_text(texts[3])?;
        let roles = ctx.role_catalog.inherited_role_names(texts[0]);
        return Ok(Value::Bool(
            ctx.privilege_catalog.has_column_privilege_for_roles(
                &roles,
                crate::auth::PrivilegeObjectKind::Table,
                texts[1],
                texts[2],
                privilege,
            ),
        ));
    }
    let object_kind = privilege_object_kind_for_function(name).ok_or_else(|| {
        ServerError::unsupported(format!("unsupported privilege check function {name}"))
    })?;
    let privilege = privilege_kind_from_text(texts[2])?;
    let roles = ctx.role_catalog.inherited_role_names(texts[0]);
    Ok(Value::Bool(ctx.privilege_catalog.has_privilege_for_roles(
        &roles,
        object_kind,
        texts[1],
        privilege,
    )))
}

fn privilege_object_kind_for_function(name: &str) -> Option<crate::auth::PrivilegeObjectKind> {
    match name {
        "has_table_privilege" => Some(crate::auth::PrivilegeObjectKind::Table),
        "has_schema_privilege" => Some(crate::auth::PrivilegeObjectKind::Schema),
        "has_database_privilege" => Some(crate::auth::PrivilegeObjectKind::Database),
        "has_sequence_privilege" => Some(crate::auth::PrivilegeObjectKind::Sequence),
        "has_function_privilege" => Some(crate::auth::PrivilegeObjectKind::Function),
        _ => None,
    }
}

fn privilege_kind_from_text(text: &str) -> Result<crate::auth::PrivilegeKind, ServerError> {
    match text.trim().to_ascii_lowercase().as_str() {
        "select" => Ok(crate::auth::PrivilegeKind::Select),
        "insert" => Ok(crate::auth::PrivilegeKind::Insert),
        "update" => Ok(crate::auth::PrivilegeKind::Update),
        "delete" => Ok(crate::auth::PrivilegeKind::Delete),
        "truncate" => Ok(crate::auth::PrivilegeKind::Truncate),
        "references" => Ok(crate::auth::PrivilegeKind::References),
        "trigger" => Ok(crate::auth::PrivilegeKind::Trigger),
        "usage" => Ok(crate::auth::PrivilegeKind::Usage),
        "create" => Ok(crate::auth::PrivilegeKind::Create),
        "connect" => Ok(crate::auth::PrivilegeKind::Connect),
        "temporary" | "temp" => Ok(crate::auth::PrivilegeKind::Temporary),
        "execute" => Ok(crate::auth::PrivilegeKind::Execute),
        other => Err(ServerError::unsupported(format!(
            "unsupported privilege kind '{other}'"
        ))),
    }
}

fn relation_size_from_literal_args(
    args: &[ScalarExpr],
    ctx: &super::LowerCtx<'_>,
) -> Result<Value, ServerError> {
    if args.len() != 1 {
        return Err(ServerError::Unsupported(
            "pg_relation_size expects exactly one relation argument",
        ));
    }
    let ScalarExpr::Literal { value, .. } = &args[0] else {
        return Err(ServerError::Unsupported(
            "pg_relation_size currently requires a literal relation name",
        ));
    };
    let relation_name = match value {
        Value::Null => return Ok(Value::Null),
        Value::Text(s) => s,
        other => {
            return Err(ServerError::unsupported(format!(
                "pg_relation_size expects text/regclass relation name, got {:?}",
                other.data_type()
            )));
        }
    };
    let entry = resolve_relation_size_entry(relation_name, ctx).ok_or_else(|| {
        ServerError::Plan(ultrasql_planner::PlanError::TableNotFound(
            relation_name.to_owned(),
        ))
    })?;
    let blocks = ctx
        .heap
        .block_count(RelationId(entry.oid))
        .max(entry.n_blocks);
    let bytes = u64::from(blocks).saturating_mul(PAGE_SIZE as u64);
    Ok(Value::Int64(i64::try_from(bytes).unwrap_or(i64::MAX)))
}

fn resolve_relation_size_entry<'a>(
    relation_name: &str,
    ctx: &'a super::LowerCtx<'_>,
) -> Option<&'a ultrasql_catalog::TableEntry> {
    let folded = relation_name.to_ascii_lowercase();
    if let Some(entry) = ctx.catalog_snapshot.tables.get(&folded) {
        return Some(entry);
    }
    folded
        .rsplit_once('.')
        .and_then(|(_, unqualified)| ctx.catalog_snapshot.tables.get(unqualified))
}

fn lower_window_func(func: &ultrasql_planner::LogicalWindowFunc) -> ultrasql_executor::WindowFunc {
    match func {
        ultrasql_planner::LogicalWindowFunc::RowNumber => ultrasql_executor::WindowFunc::RowNumber,
        ultrasql_planner::LogicalWindowFunc::Rank => ultrasql_executor::WindowFunc::Rank,
        ultrasql_planner::LogicalWindowFunc::DenseRank => ultrasql_executor::WindowFunc::DenseRank,
        ultrasql_planner::LogicalWindowFunc::Lag {
            expr,
            offset,
            default,
        } => ultrasql_executor::WindowFunc::Lag {
            expr: expr.clone(),
            offset: *offset,
            default: default.clone(),
        },
        ultrasql_planner::LogicalWindowFunc::Lead {
            expr,
            offset,
            default,
        } => ultrasql_executor::WindowFunc::Lead {
            expr: expr.clone(),
            offset: *offset,
            default: default.clone(),
        },
        ultrasql_planner::LogicalWindowFunc::FirstValue(e) => {
            ultrasql_executor::WindowFunc::FirstValue(e.clone())
        }
        ultrasql_planner::LogicalWindowFunc::LastValue(e) => {
            ultrasql_executor::WindowFunc::LastValue(e.clone())
        }
        ultrasql_planner::LogicalWindowFunc::NthValue { expr, n } => {
            ultrasql_executor::WindowFunc::NthValue {
                expr: expr.clone(),
                n: *n,
            }
        }
        ultrasql_planner::LogicalWindowFunc::Ntile(n) => ultrasql_executor::WindowFunc::Ntile(*n),
    }
}

/// Lower a `LogicalPlan::Cte` node.
///
/// Semantics:
///
/// - Recursive CTEs (`WITH RECURSIVE`) are out of scope for this wave;
///   the executor lacks a fixpoint loop, so the binder accepts the
///   keyword but the lowerer rejects the plan with a precise
///   [`ServerError::Unsupported`] rather than silently treating it as
///   non-recursive (which would return wrong results for a self-
///   referential definition). The recursive fixpoint is a v0.6 follow-up.
/// - Non-recursive CTEs are materialised *once* per query execution into
///   a shared `Arc<Vec<Batch>>`. Every reference inside the body
///   resolves to its own [`CteScan`] over that buffer (the
///   [`CteScan`] operator is itself single-shot, but the underlying
///   `Arc` is shared, so multiple references reuse the materialised
///   rows without re-evaluating the definition). This matches
///   PostgreSQL's default CTE-as-optimisation-barrier behaviour.
/// - The CTE name is pushed onto a new `LowerCtx::cte_buffers` overlay
///   before the body is lowered. Body-side `Scan { table: "<cte_name>" }`
///   nodes are routed to the materialised buffer by
///   [`lower_catalog_or_sample_scan`].
///
/// Nested CTEs (a CTE defined inside another CTE's body, or a body that
/// itself contains a `WITH` clause) compose naturally: each recursive
/// call into [`lower_query`] sees the cumulative overlay, and inner
/// definitions can therefore reference outer CTEs.

pub(super) fn lower_cte(
    name: &str,
    recursive: bool,
    definition: &LogicalPlan,
    body: &LogicalPlan,
    ctx: &LowerCtx<'_>,
) -> Result<Box<dyn Operator>, ServerError> {
    if recursive {
        return lower_recursive_cte(name, definition, body, ctx);
    }

    // Materialise the definition plan against the *current* overlay so a
    // CTE can reference outer CTEs declared earlier in the same `WITH`
    // chain (the binder serialises the chain into nested
    // `LogicalPlan::Cte` nodes, so the outer ones are already on the
    // overlay when we reach this inner one).
    let mut def_op = lower_query(definition, ctx)?;
    let mut batches: Vec<Batch> = Vec::new();
    while let Some(batch) = def_op.next_batch()? {
        batches.push(batch);
    }
    let def_schema = def_op.schema().clone();

    // Push the materialised CTE onto a child overlay. Cloning the map is
    // O(N) in the number of outer bindings; CTE chains are short
    // (typically ≤ a handful per query), so we accept the copy in
    // exchange for keeping `LowerCtx` strictly immutable per recursion
    // level — interior mutability here would force every helper to take
    // `&mut LowerCtx` for no clarity gain.
    let mut child_buffers = ctx.cte_buffers.clone();
    let cte_schema = cte_reference_schema(body, name).unwrap_or_else(|| def_schema.clone());
    child_buffers.insert(
        name.to_ascii_lowercase(),
        CteBuffer {
            batches: Arc::new(batches),
            schema: cte_schema,
        },
    );
    let child_ctx = LowerCtx {
        tables: ctx.tables,
        catalog_snapshot: Arc::clone(&ctx.catalog_snapshot),
        table_constraints: Arc::clone(&ctx.table_constraints),
        sequences: Arc::clone(&ctx.sequences),
        sequence_owners: Arc::clone(&ctx.sequence_owners),
        schemas: Arc::clone(&ctx.schemas),
        operators: Arc::clone(&ctx.operators),
        role_catalog: Arc::clone(&ctx.role_catalog),
        privilege_catalog: Arc::clone(&ctx.privilege_catalog),
        row_security: Arc::clone(&ctx.row_security),
        session_settings: Arc::clone(&ctx.session_settings),
        current_user: ctx.current_user.clone(),
        session_user: ctx.session_user.clone(),
        persistent_catalog: Arc::clone(&ctx.persistent_catalog),
        time_partitions: Arc::clone(&ctx.time_partitions),
        workload_recorder: Arc::clone(&ctx.workload_recorder),
        autovacuum_config: ctx.autovacuum_config,
        logging_config: ctx.logging_config,
        wal_archive_config: ctx.wal_archive_config.clone(),
        data_dir: ctx.data_dir.clone(),
        logical_replication: Arc::clone(&ctx.logical_replication),
        sequence_state: ctx.sequence_state.clone(),
        advisory_state: ctx.advisory_state.clone(),
        heap: Arc::clone(&ctx.heap),
        vm: Arc::clone(&ctx.vm),
        snapshot: ctx.snapshot.clone(),
        isolation: ctx.isolation,
        oracle: Arc::clone(&ctx.oracle),
        xid: ctx.xid,
        command_id: ctx.command_id,
        cte_buffers: child_buffers,
        jit: ctx.jit,
        cancel_flag: ctx.cancel_flag.clone(),
        work_mem: std::sync::Arc::clone(&ctx.work_mem),
        profile_operators: ctx.profile_operators,
    };

    lower_query(body, &child_ctx)
}

fn cte_reference_schema(plan: &LogicalPlan, name: &str) -> Option<Schema> {
    match plan {
        LogicalPlan::Scan { table, schema, .. } if table.eq_ignore_ascii_case(name) => {
            Some(schema.clone())
        }
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Project { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::LockRows { input, .. }
        | LogicalPlan::Window { input, .. }
        | LogicalPlan::Delete { input, .. }
        | LogicalPlan::Update { input, .. } => cte_reference_schema(input, name),
        LogicalPlan::Join { left, right, .. } | LogicalPlan::SetOp { left, right, .. } => {
            cte_reference_schema(left, name).or_else(|| cte_reference_schema(right, name))
        }
        LogicalPlan::Insert { source, .. } => cte_reference_schema(source, name),
        LogicalPlan::Cte { body, .. } | LogicalPlan::Explain { input: body, .. } => {
            cte_reference_schema(body, name)
        }
        _ => None,
    }
}
