//! Fused-kernel lowerers — `SUM(c) WHERE c <op> lit` and the cached
//! scalar-aggregate fast path.

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
use super::scan::lower_heap_scan;

pub(super) fn shift_column_indices(expr: &ScalarExpr, by: usize) -> ScalarExpr {
    match expr {
        ScalarExpr::Column {
            name,
            index,
            data_type,
        } => ScalarExpr::Column {
            name: name.clone(),
            index: index + by,
            data_type: data_type.clone(),
        },
        ScalarExpr::Literal { value, data_type } => ScalarExpr::Literal {
            value: value.clone(),
            data_type: data_type.clone(),
        },
        ScalarExpr::Parameter { index, data_type } => ScalarExpr::Parameter {
            index: *index,
            data_type: data_type.clone(),
        },
        ScalarExpr::Unary {
            op,
            expr,
            data_type,
        } => ScalarExpr::Unary {
            op: *op,
            expr: Box::new(shift_column_indices(expr, by)),
            data_type: data_type.clone(),
        },
        ScalarExpr::Binary {
            op,
            left,
            right,
            data_type,
        } => ScalarExpr::Binary {
            op: *op,
            left: Box::new(shift_column_indices(left, by)),
            right: Box::new(shift_column_indices(right, by)),
            data_type: data_type.clone(),
        },
        ScalarExpr::IsNull { expr, negated } => ScalarExpr::IsNull {
            expr: Box::new(shift_column_indices(expr, by)),
            negated: *negated,
        },
        // Subquery-bearing and outer-frame variants are returned
        // unchanged. They cannot appear in a v0.5 UPDATE / DELETE
        // predicate (the binder produces them only for SELECTs), so we
        // would never observe them here in practice.
        ScalarExpr::OuterColumn { .. }
        | ScalarExpr::ScalarSubquery { .. }
        | ScalarExpr::Exists { .. }
        | ScalarExpr::InSubquery { .. } => expr.clone(),
    }
}

/// Lower an `UPDATE` plan into a [`ModifyTable`] with `ModifyKind::Update`.
///
/// The child operator is a TID-emitting [`SeqScan`] (optionally wrapped
/// in [`Filter`] when the planner produced a `WHERE`). Predicate column
/// indices are shifted by +2 to account for the leading TID columns;
/// assignment **target** column indices stay un-shifted because
/// `apply_update` re-indexes them against the relation schema, not the
/// child batch shape.

/// Try to lower pure-scalar SUM or AVG over an `Int32` column on a
/// cache-live relation into [`CachedSumI32Scan`] /
/// [`CachedAvgI32Scan`].
///
/// Matches:
///
/// ```text
///     Aggregate { group_by: [], aggregates: [Sum(Column { col, Int32 })] }
///       └── Scan { table }
/// ```
///
/// (or `Avg` instead of `Sum`) and the relation already has a
/// live entry in `HeapAccess::column_cache`. Returns `Ok(None)`
/// when the shape does not match or the cache is empty — caller
/// falls through to the generic `HashAggregate(SeqScan)` chain

pub(super) fn try_lower_cached_scalar_aggregate_i32(
    input: &LogicalPlan,
    group_by: &[ScalarExpr],
    aggregates: &[ultrasql_planner::LogicalAggregateExpr],
    ctx: &LowerCtx<'_>,
) -> Result<Option<Box<dyn Operator>>, ServerError> {
    use ultrasql_planner::AggregateFunc;

    if !group_by.is_empty() || aggregates.len() != 1 {
        return Ok(None);
    }
    let agg = &aggregates[0];
    if agg.distinct {
        return Ok(None);
    }
    let target_col = match &agg.arg {
        Some(ScalarExpr::Column {
            index,
            data_type: ultrasql_core::DataType::Int32,
            ..
        }) => *index,
        _ => return Ok(None),
    };

    let LogicalPlan::Scan { table, .. } = input else {
        return Ok(None);
    };
    let folded = table.to_ascii_lowercase();
    let entry = match ctx.catalog_snapshot.tables.get(&folded) {
        Some(entry) => entry,
        None => return Ok(None),
    };
    if target_col >= entry.schema.len()
        || !matches!(
            entry.schema.field_at(target_col).data_type,
            ultrasql_core::DataType::Int32
        )
    {
        return Ok(None);
    }

    let rel_id = RelationId(entry.oid);
    let Some(columns) = ctx.heap.column_cache.get(rel_id) else {
        return Ok(None);
    };

    let op: Box<dyn Operator> = match agg.func {
        AggregateFunc::Sum => Box::new(CachedSumI32Scan::new(
            columns,
            target_col,
            agg.output_name.clone(),
        )),
        AggregateFunc::Avg => Box::new(CachedAvgI32Scan::new(
            columns,
            target_col,
            agg.output_name.clone(),
        )),
        _ => return Ok(None),
    };
    Ok(Some(op))
}

/// Try to lower
///
/// ```text
///     Aggregate { group_by: [], aggregates: [Sum(Column { col_sum, Int32 })] }
///       └── Filter { predicate: Column { col_pred, Int32 } op Literal(Int32) }
///             └── Scan { table }
/// ```
///
/// into [`FilterSumI32Scan`] over a [`SeqScan`].
///
/// Returns `Ok(Some(_))` on a successful match, `Ok(None)` when the
/// plan tree does not match the fused shape (caller falls through
/// to `HashAggregate`), and `Err(_)` on a lowering failure of the

pub(super) fn try_lower_fused_filter_sum_i32(
    input: &LogicalPlan,
    group_by: &[ScalarExpr],
    aggregates: &[ultrasql_planner::LogicalAggregateExpr],
    ctx: &LowerCtx<'_>,
) -> Result<Option<Box<dyn Operator>>, ServerError> {
    use ultrasql_planner::AggregateFunc;

    // Shape: scalar SUM aggregate (no GROUP BY, one Sum entry,
    // non-DISTINCT, Int32 column argument).
    if !group_by.is_empty() || aggregates.len() != 1 {
        return Ok(None);
    }
    let agg = &aggregates[0];
    if agg.func != AggregateFunc::Sum || agg.distinct {
        return Ok(None);
    }
    let sum_col = match &agg.arg {
        Some(ScalarExpr::Column {
            index,
            data_type: ultrasql_core::DataType::Int32,
            ..
        }) => *index,
        _ => return Ok(None),
    };

    // Shape: Filter over Scan with Int32 predicate `col op lit`.
    let LogicalPlan::Filter {
        input: filter_input,
        predicate,
    } = input
    else {
        return Ok(None);
    };
    let LogicalPlan::Scan { table, .. } = filter_input.as_ref() else {
        return Ok(None);
    };
    let (pred_col, pred_op, pred_lit) = match extract_int32_col_op_lit(predicate) {
        Some(x) => x,
        None => return Ok(None),
    };

    // The scan target must be a real heap relation (we only built
    // the column cache for the heap path). Sample / CTE / memtable
    // sources never benefit from the column-cache fast path and
    // would not provide the `Int32` columns the fused operator
    // requires anyway.
    let folded = table.to_ascii_lowercase();
    let entry = match ctx.catalog_snapshot.tables.get(&folded) {
        Some(entry) => entry,
        None => return Ok(None),
    };

    // Schema validation: both `pred_col` and `sum_col` must be
    // Int32 in the relation's catalog schema.
    let schema = &entry.schema;
    if pred_col >= schema.len() || sum_col >= schema.len() {
        return Ok(None);
    }
    if !matches!(
        schema.field_at(pred_col).data_type,
        ultrasql_core::DataType::Int32
    ) || !matches!(
        schema.field_at(sum_col).data_type,
        ultrasql_core::DataType::Int32
    ) {
        return Ok(None);
    }

    // Cache-driven fast path: when the relation already has a
    // live column-cache entry, skip the SeqScan layer entirely
    // and run the fused SIMD kernel directly over the cached
    // `Arc<CachedColumns>`. The cache-driving `SeqScan` would
    // otherwise copy each column out via `slice_column` (one
    // ~4 MB memcpy per 1 M-row Int32 column) before passing the
    // batch through the operator pipeline.
    let rel_id = RelationId(entry.oid);
    if let Some(columns) = ctx.heap.column_cache.get(rel_id) {
        let fused = CachedFilterSumI32Scan::new(
            columns,
            pred_col,
            pred_lit,
            pred_op,
            sum_col,
            agg.output_name.clone(),
        );
        return Ok(Some(Box::new(fused)));
    }

    // Cache miss — drive the regular SeqScan path. The first
    // SeqScan over a relation populates the column cache as a
    // side effect of its walk, so subsequent queries hit the
    // direct-from-cache branch above.
    let scan = lower_heap_scan(entry, ctx);
    let fused = FilterSumI32Scan::new(
        scan,
        pred_col,
        pred_lit,
        pred_op,
        sum_col,
        agg.output_name.clone(),
    );
    Ok(Some(Box::new(fused)))
}

/// Match a predicate of shape `Column { Int32 } op Literal(Int32)`
/// (or its mirror `Literal(Int32) op Column { Int32 }`) and return
/// the `(col_index, cmp_op, threshold)` tuple. Returns `None` for
/// any other shape.

pub(super) fn extract_int32_col_op_lit(
    expr: &ScalarExpr,
) -> Option<(usize, ultrasql_vec::kernels::CmpOp, i32)> {
    use ultrasql_core::Value;
    use ultrasql_vec::kernels::CmpOp;

    let ScalarExpr::Binary {
        op, left, right, ..
    } = expr
    else {
        return None;
    };
    let cmp_op = match op {
        BinaryOp::Lt => CmpOp::Lt,
        BinaryOp::LtEq => CmpOp::Le,
        BinaryOp::Gt => CmpOp::Gt,
        BinaryOp::GtEq => CmpOp::Ge,
        BinaryOp::Eq => CmpOp::Eq,
        BinaryOp::NotEq => CmpOp::Ne,
        _ => return None,
    };

    let col_idx_from = |e: &ScalarExpr| match e {
        ScalarExpr::Column {
            index,
            data_type: ultrasql_core::DataType::Int32,
            ..
        } => Some(*index),
        _ => None,
    };
    let lit_from = |e: &ScalarExpr| match e {
        ScalarExpr::Literal {
            value: Value::Int32(v),
            ..
        } => Some(*v),
        _ => None,
    };

    if let (Some(col), Some(lit)) = (col_idx_from(left), lit_from(right)) {
        Some((col, cmp_op, lit))
    } else if let (Some(lit), Some(col)) = (lit_from(left), col_idx_from(right)) {
        // Mirror: swap op so `lit op col` becomes `col mirror_op lit`.
        let mirrored = match cmp_op {
            CmpOp::Lt => CmpOp::Gt,
            CmpOp::Le => CmpOp::Ge,
            CmpOp::Gt => CmpOp::Lt,
            CmpOp::Ge => CmpOp::Le,
            CmpOp::Eq => CmpOp::Eq,
            CmpOp::Ne => CmpOp::Ne,
        };
        Some((col, mirrored, lit))
    } else {
        None
    }
}
