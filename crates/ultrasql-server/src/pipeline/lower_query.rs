//! Main query lowerer — turns a [`LogicalPlan`] into an [`Operator`]
//! tree. Includes the `WITH` dispatch (regular and recursive CTEs).

use std::sync::Arc;

use ultrasql_core::Schema;
use ultrasql_executor::{Filter, HashAggregate, Limit, Operator, ResultOp, Sort, ValuesScan};
use ultrasql_planner::{LogicalPlan, ScalarExpr};
use ultrasql_vec::Batch;

use crate::error::ServerError;

use super::agg_fuse::{
    try_lower_cached_scalar_aggregate_i32, try_lower_direct_scalar_aggregate,
    try_lower_fused_filter_sum_int,
};
use super::cte_helpers::{lower_recursive_cte, lower_set_op_real};
use super::index_scan::{try_index_only_scan, try_index_scan, try_ordered_index_scan};
use super::join::lower_join;
use super::modify::{
    lower_project_columns, lower_real_delete, lower_real_insert, lower_real_update,
};
use super::saturate_row_count;
use super::scan::{lower_catalog_or_sample_scan, lower_function_scan};
use super::{CteBuffer, LowerCtx};

pub fn lower_query(
    plan: &LogicalPlan,
    ctx: &LowerCtx<'_>,
) -> Result<Box<dyn Operator>, ServerError> {
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
            ..
        } => lower_real_insert(table, columns, source, on_conflict.as_ref(), returning, ctx),
        LogicalPlan::Values { rows, schema } => {
            Ok(Box::new(ValuesScan::new(rows.clone(), schema.clone())))
        }
        LogicalPlan::Project {
            input,
            exprs,
            schema,
        } => {
            // `SELECT <const>` (no FROM) lowers Project(Empty) → ResultOp,
            // a single-row constant emitter. The general path below would
            // try to lower Empty into a scan, which has no meaning when
            // the projection is purely constant.
            if matches!(input.as_ref(), LogicalPlan::Empty { .. }) {
                let scalars: Vec<ScalarExpr> = exprs.iter().map(|(e, _)| e.clone()).collect();
                return Ok(Box::new(ResultOp::new(scalars, schema.clone())));
            }
            if let Some(op) = try_index_only_scan(input, exprs, ctx)? {
                return Ok(op);
            }
            let child = lower_query(input, ctx)?;
            lower_project_columns(child, exprs)
        }
        LogicalPlan::Filter { input, predicate } => {
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
            if let Some(op) = try_index_scan(input, predicate, ctx)? {
                return Ok(op);
            }
            let child = lower_query(input, ctx)?;
            Ok(Box::new(Filter::new(child, predicate.clone())))
        }
        LogicalPlan::Limit { input, n, offset } => {
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
            // Lower the child first; the executor's `Sort` operator drains
            // it on the first `next_batch()` call and emits sorted rows in
            // 4096-row chunks thereafter, so the wire encoder treats it
            // exactly like any other scalar source.
            //
            // v0.5 limitation: `Sort` materialises the entire input in
            // memory before emitting the first row. Spill-to-disk is on
            // the v0.6 work_mem track. Bounded by `IN_MEMORY_POOL_FRAMES *
            // PAGE_SIZE` plus working-set headroom (see
            // `crate::IN_MEMORY_POOL_FRAMES`); a query whose input
            // exceeds that will OOM the connection task rather than spill.
            //
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
            Ok(Box::new(Sort::new(child, keys.clone(), schema)))
        }
        LogicalPlan::Update {
            table,
            assignments,
            input,
            returning,
            ..
        } => lower_real_update(table, assignments, input, returning, ctx),
        LogicalPlan::Delete {
            table,
            input,
            returning,
            ..
        } => lower_real_delete(table, input, returning, ctx),
        LogicalPlan::Truncate { .. }
        | LogicalPlan::CreateTable { .. }
        | LogicalPlan::CreateIndex { .. }
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
        | LogicalPlan::SetVariable { .. } => Err(ServerError::Unsupported(
            "session control reached operator lowerer; expected direct dispatch path",
        )),
        LogicalPlan::Listen { .. } | LogicalPlan::Notify { .. } | LogicalPlan::Unlisten { .. } => {
            Err(ServerError::Unsupported(
                "LISTEN/NOTIFY/UNLISTEN reached operator lowerer; expected pubsub dispatch path",
            ))
        }
        LogicalPlan::FunctionScan { name, args, .. } => lower_function_scan(name, args),
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
            lower_join(
                left_op,
                right_op,
                left_schema,
                right_schema,
                *join_type,
                condition,
                schema.clone(),
            )
        }
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggregates,
            schema,
        } => {
            // Fast path: `SELECT SUM(int_col) FROM t WHERE int_col op lit`
            // collapses to one fused operator that runs SIMD
            // compare → mask → sum in a single
            // pass. Skips the per-batch `select_column` (per-row
            // scalar pushes) the generic Filter → HashAggregate
            // chain pays.
            if let Some(fused) = try_lower_fused_filter_sum_int(input, group_by, aggregates, ctx)? {
                return Ok(fused);
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
                HashAggregate::new(child, group_by.clone(), aggregates.clone(), schema.clone());
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
        sequence_state: ctx.sequence_state.clone(),
        heap: Arc::clone(&ctx.heap),
        vm: Arc::clone(&ctx.vm),
        snapshot: ctx.snapshot.clone(),
        oracle: Arc::clone(&ctx.oracle),
        xid: ctx.xid,
        command_id: ctx.command_id,
        cte_buffers: child_buffers,
        jit: ctx.jit,
        cancel_flag: ctx.cancel_flag.clone(),
        work_mem: std::sync::Arc::clone(&ctx.work_mem),
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
