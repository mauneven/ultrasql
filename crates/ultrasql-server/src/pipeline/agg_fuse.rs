//! Fused-kernel lowerers — `SUM(c) WHERE c <op> lit` and the cached
//! scalar-aggregate fast path.

use ultrasql_core::RelationId;
use ultrasql_executor::Operator;
use ultrasql_executor::filter_sum_op::{
    CachedAvgI32Scan, CachedFilterSumI32Scan, CachedFilterSumI64Scan, CachedSumI32Scan,
    FilterSumI32Scan, FilterSumI64Scan,
};
use ultrasql_planner::{BinaryOp, LogicalPlan, ScalarExpr};

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
        ScalarExpr::FunctionCall {
            name,
            args,
            data_type,
        } => ScalarExpr::FunctionCall {
            name: name.clone(),
            args: args.iter().map(|a| shift_column_indices(a, by)).collect(),
            data_type: data_type.clone(),
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
///     Aggregate { group_by: [], aggregates: [Sum|Avg|CountStar] }
///       └── Scan { table }
/// ```
///
/// into a [`DirectScalarAggScan`] wrapping a [`SeqScan`].
///
/// Matched shapes:
///
/// * `SUM(col)` over a non-NULL `Int32` or `Int64` column.
/// * `AVG(col)` over a non-NULL `Int32` or `Int64` column.
/// * `COUNT(*)` over any relation.
///
/// Returns `Ok(None)` when:
///
/// * the plan envelope is not a bare scalar aggregate over a bare scan,
/// * the aggregate function is anything other than `Sum`, `Avg`, or
///   `CountStar`,
/// * `DISTINCT` was requested (the kernels do not deduplicate),
/// * the aggregate argument is anything other than a direct column
///   reference (e.g. `SUM(x + 1)` or `SUM(2 * x)`),
/// * the targeted column is not `Int32` or `Int64`,
/// * the table is not registered in the catalog snapshot (sample-
///   table-only scans miss the fast path; that branch is the
///   in-memory legacy path documented in
///   [`crate::pipeline::scan::lower_catalog_or_sample_scan`]).
///
/// The caller falls through to the generic `HashAggregate(SeqScan)`
/// chain on `Ok(None)`. Errors propagate; today the only error path
/// would be the (unreachable) failure of [`super::scan::lower_heap_scan`]
/// itself.
///
/// NULL fallback: the constructed [`DirectScalarAggScan`] returns
/// [`ultrasql_executor::ExecError::Unsupported`] when an upstream batch
/// carries a column with a validity bitmap. This is a runtime guard;
/// the bench data is non-null so the runtime path stays on the SIMD
/// kernel.

pub(super) fn try_lower_direct_scalar_aggregate(
    input: &LogicalPlan,
    group_by: &[ScalarExpr],
    aggregates: &[ultrasql_planner::LogicalAggregateExpr],
    ctx: &LowerCtx<'_>,
) -> Result<Option<Box<dyn Operator>>, ServerError> {
    use ultrasql_executor::DirectScalarAggScan;
    use ultrasql_planner::AggregateFunc;

    if !group_by.is_empty() || aggregates.len() != 1 {
        return Ok(None);
    }
    let agg = &aggregates[0];
    if agg.distinct {
        return Ok(None);
    }

    // Outer shape must be a bare `Scan` over a real heap relation. The
    // catalog snapshot is consulted to confirm the relation is
    // persistent — sample/CTE/memtable sources fall through to the
    // generic lowerer (the in-memory data path has no
    // `Int32`/`Int64`-specialised heap walk to optimise).
    let LogicalPlan::Scan { table, .. } = input else {
        return Ok(None);
    };
    let folded = table.to_ascii_lowercase();
    let entry = match ctx.catalog_snapshot.tables.get(&folded) {
        Some(entry) => entry,
        None => return Ok(None),
    };
    let schema = &entry.schema;

    // Aggregate-function dispatch. `CountStar` has no column argument
    // and never inspects a column type; `Sum`/`Avg` require a direct
    // column reference with `Int32` or `Int64` data type.
    let op: Box<dyn Operator> = match agg.func {
        AggregateFunc::CountStar => {
            let child = super::scan::lower_heap_scan(entry, None, ctx)?;
            Box::new(DirectScalarAggScan::count_star(
                child,
                agg.output_name.clone(),
            ))
        }
        AggregateFunc::Sum => {
            let (col_idx, data_type) = match &agg.arg {
                Some(ScalarExpr::Column {
                    index, data_type, ..
                }) => (*index, data_type.clone()),
                _ => return Ok(None),
            };
            if col_idx >= schema.len() {
                return Ok(None);
            }
            let child = super::scan::lower_heap_scan(entry, None, ctx)?;
            match data_type {
                ultrasql_core::DataType::Int32 => Box::new(DirectScalarAggScan::sum_int32(
                    child,
                    col_idx,
                    agg.output_name.clone(),
                )),
                ultrasql_core::DataType::Int64 => Box::new(DirectScalarAggScan::sum_int64(
                    child,
                    col_idx,
                    agg.output_name.clone(),
                )),
                _ => return Ok(None),
            }
        }
        AggregateFunc::Avg => {
            let (col_idx, data_type) = match &agg.arg {
                Some(ScalarExpr::Column {
                    index, data_type, ..
                }) => (*index, data_type.clone()),
                _ => return Ok(None),
            };
            if col_idx >= schema.len() {
                return Ok(None);
            }
            let child = super::scan::lower_heap_scan(entry, None, ctx)?;
            match data_type {
                ultrasql_core::DataType::Int32 => Box::new(DirectScalarAggScan::avg_int32(
                    child,
                    col_idx,
                    agg.output_name.clone(),
                )),
                ultrasql_core::DataType::Int64 => Box::new(DirectScalarAggScan::avg_int64(
                    child,
                    col_idx,
                    agg.output_name.clone(),
                )),
                _ => return Ok(None),
            }
        }
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

pub(super) fn try_lower_fused_filter_sum_int(
    input: &LogicalPlan,
    group_by: &[ScalarExpr],
    aggregates: &[ultrasql_planner::LogicalAggregateExpr],
    ctx: &LowerCtx<'_>,
) -> Result<Option<Box<dyn Operator>>, ServerError> {
    use ultrasql_planner::AggregateFunc;

    // Shape: scalar SUM aggregate (no GROUP BY, one Sum entry,
    // non-DISTINCT, Int32 or Int64 column argument).
    if !group_by.is_empty() || aggregates.len() != 1 {
        return Ok(None);
    }
    let agg = &aggregates[0];
    if agg.func != AggregateFunc::Sum || agg.distinct {
        return Ok(None);
    }
    let (sum_col, sum_type) = match &agg.arg {
        Some(ScalarExpr::Column {
            index,
            data_type: ultrasql_core::DataType::Int32,
            ..
        }) => (*index, ultrasql_core::DataType::Int32),
        Some(ScalarExpr::Column {
            index,
            data_type: ultrasql_core::DataType::Int64,
            ..
        }) => (*index, ultrasql_core::DataType::Int64),
        _ => return Ok(None),
    };

    // Shape: Filter over Scan with integer predicate `col op lit`.
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
    if sum_col >= schema.len() {
        return Ok(None);
    }
    let rel_id = RelationId(entry.oid);

    match sum_type {
        ultrasql_core::DataType::Int32 => {
            let (pred_col, pred_op, pred_lit) = match extract_int32_col_op_lit(predicate) {
                Some(x) => x,
                None => return Ok(None),
            };
            if pred_col >= schema.len()
                || !matches!(
                    schema.field_at(pred_col).data_type,
                    ultrasql_core::DataType::Int32
                )
                || !matches!(
                    schema.field_at(sum_col).data_type,
                    ultrasql_core::DataType::Int32
                )
            {
                return Ok(None);
            }
            if let Some(columns) = ctx.heap.column_cache.get(rel_id) {
                let fused = CachedFilterSumI32Scan::new(
                    columns,
                    pred_col,
                    pred_lit,
                    pred_op,
                    sum_col,
                    agg.output_name.clone(),
                )
                .with_jit(ctx.jit);
                return Ok(Some(Box::new(fused)));
            }
            let scan = lower_heap_scan(entry, None, ctx)?;
            let fused = FilterSumI32Scan::new(
                scan,
                pred_col,
                pred_lit,
                pred_op,
                sum_col,
                agg.output_name.clone(),
            )
            .with_jit(ctx.jit);
            Ok(Some(Box::new(fused)))
        }
        ultrasql_core::DataType::Int64 => {
            let (pred_col, pred_op, pred_lit) = match extract_int64_col_op_lit(predicate) {
                Some(x) => x,
                None => return Ok(None),
            };
            if pred_col >= schema.len()
                || !matches!(
                    schema.field_at(pred_col).data_type,
                    ultrasql_core::DataType::Int64
                )
                || !matches!(
                    schema.field_at(sum_col).data_type,
                    ultrasql_core::DataType::Int64
                )
            {
                return Ok(None);
            }
            if let Some(columns) = ctx.heap.column_cache.get(rel_id) {
                let fused = CachedFilterSumI64Scan::new(
                    columns,
                    pred_col,
                    pred_lit,
                    pred_op,
                    sum_col,
                    agg.output_name.clone(),
                )
                .with_jit(ctx.jit);
                return Ok(Some(Box::new(fused)));
            }
            let scan = lower_heap_scan(entry, None, ctx)?;
            let fused = FilterSumI64Scan::new(
                scan,
                pred_col,
                pred_lit,
                pred_op,
                sum_col,
                agg.output_name.clone(),
            )
            .with_jit(ctx.jit);
            Ok(Some(Box::new(fused)))
        }
        _ => Ok(None),
    }
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

/// Match a predicate of shape `Column { Int64 } op Literal(Int64)` and
/// return the `(col_index, cmp_op, threshold)` tuple.
pub(super) fn extract_int64_col_op_lit(
    expr: &ScalarExpr,
) -> Option<(usize, ultrasql_vec::kernels::CmpOp, i64)> {
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
            data_type: ultrasql_core::DataType::Int64,
            ..
        } => Some(*index),
        _ => None,
    };
    let lit_from = |e: &ScalarExpr| match e {
        ScalarExpr::Literal {
            value: Value::Int64(v),
            ..
        } => Some(*v),
        _ => None,
    };

    if let (Some(col), Some(lit)) = (col_idx_from(left), lit_from(right)) {
        Some((col, cmp_op, lit))
    } else if let (Some(lit), Some(col)) = (lit_from(left), col_idx_from(right)) {
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
