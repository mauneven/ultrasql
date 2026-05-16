//! INSERT/UPDATE/DELETE lowering plus the fused-kernel fast paths.

use std::sync::Arc;

use ultrasql_catalog::TableEntry;
use ultrasql_core::{CommandId, DataType, RelationId, Value, Xid};
use ultrasql_executor::fused_delete::FusedDeleteInt32Pair;
use ultrasql_executor::fused_update::{FusedCmp, FusedPredicate, FusedUpdateInt32Add};
use ultrasql_executor::{
    Filter,
    ModifyKind, ModifyTable, Operator, Project, RowCodec,
    SeqScan, ValuesScan,
};
use ultrasql_planner::{
    BinaryOp, LogicalPlan, ScalarExpr,
};

use crate::error::ServerError;

use super::LowerCtx;
use super::agg_fuse::{extract_int32_col_op_lit, shift_column_indices};
use super::lower_query::lower_query;

pub(super) fn lower_real_insert(
    table: &str,
    columns: &[usize],
    source: &LogicalPlan,
    on_conflict: Option<&ultrasql_planner::LogicalOnConflict>,
    returning: &[(ScalarExpr, String)],
    ctx: &LowerCtx<'_>,
) -> Result<Box<dyn Operator>, ServerError> {
    if on_conflict.is_some() {
        return Err(ServerError::Unsupported("INSERT ... ON CONFLICT"));
    }
    if !returning.is_empty() {
        return Err(ServerError::Unsupported("INSERT ... RETURNING"));
    }
    let entry = ctx
        .catalog_snapshot
        .tables
        .get(&table.to_ascii_lowercase())
        .ok_or_else(|| {
            ServerError::Plan(ultrasql_planner::PlanError::TableNotFound(
                table.to_string(),
            ))
        })?;
    if !columns.is_empty() && columns.len() != entry.schema.len() {
        return Err(ServerError::Unsupported(
            "INSERT with column list narrower than table; v0.5 requires every column",
        ));
    }
    let child: Box<dyn Operator> = match source {
        LogicalPlan::Values { rows, schema } => {
            Box::new(ValuesScan::new(rows.clone(), schema.clone()))
        }
        // `INSERT INTO t SELECT ...` — drive the destination through the
        // same `ModifyTable` shape we use for `VALUES`, but with a
        // lowered query plan as the row source. The binder enforced
        // arity, types, and named-column matching when it built the
        // `Insert` plan; if its schema differs from the target table's
        // declared schema, refuse here so a silent encoding mismatch
        // never lands rows into the heap with the wrong layout.
        other => {
            let source_schema = other.schema();
            if source_schema.len() != entry.schema.len() {
                return Err(ServerError::Unsupported(
                    "INSERT ... SELECT with arity mismatch",
                ));
            }
            for (idx, (src, dst)) in source_schema
                .fields()
                .iter()
                .zip(entry.schema.fields().iter())
                .enumerate()
            {
                if src.data_type != dst.data_type
                    && !matches!(src.data_type, ultrasql_core::DataType::Null)
                {
                    return Err(ServerError::Plan(
                        ultrasql_planner::PlanError::TypeMismatch(format!(
                            "INSERT ... SELECT column {idx} type mismatch: source {src} vs target {dst}",
                        )),
                    ));
                }
            }
            lower_query(other, ctx)?
        }
    };
    let rel = RelationId(entry.oid);
    let modify = ModifyTable::new(
        Arc::clone(&ctx.heap),
        rel,
        entry.schema.clone(),
        ModifyKind::Insert,
        ctx.xid,
        ctx.command_id,
        Xid::new(0),
        CommandId::FIRST,
        None,
        child,
    );
    Ok(Box::new(modify))
}

/// Build a TID-emitting [`SeqScan`] over a persistent relation.
///
/// The resulting operator emits rows shaped
/// `[tid_block: Int32, tid_slot: Int32, ...payload_cols]`, which is the
/// contract [`ModifyTable`] expects for UPDATE and DELETE.
pub(super) fn build_tid_seq_scan(entry: &TableEntry, ctx: &LowerCtx<'_>) -> Box<dyn Operator> {
    let rel = RelationId(entry.oid);
    let block_count = ctx.heap.block_count(rel).max(entry.n_blocks);
    let codec = RowCodec::new(entry.schema.clone());
    let scan = SeqScan::new_with_tids(
        Arc::clone(&ctx.heap),
        rel,
        block_count,
        ctx.snapshot.clone(),
        Arc::clone(&ctx.oracle),
        codec,
    );
    Box::new(scan)
}

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

pub(super) fn try_build_fused_update(
    target_table: &str,
    entry: &TableEntry,
    assignments: &[(usize, ScalarExpr)],
    input: &LogicalPlan,
    ctx: &LowerCtx<'_>,
) -> Result<Option<Box<dyn Operator>>, ServerError> {
    // Schema must be exactly (Int32, Int32). No extra columns, no
    // NULLability change — `FusedUpdateInt32Add` reads a fixed
    // 9-byte payload layout.
    let fields = entry.schema.fields();
    if fields.len() != 2
        || fields[0].data_type != DataType::Int32
        || fields[1].data_type != DataType::Int32
    {
        return Ok(None);
    }

    if assignments.len() != 1 {
        return Ok(None);
    }
    let (target_col_usize, assign_expr) = &assignments[0];
    if *target_col_usize > 1 {
        return Ok(None);
    }
    let target_col = u8::try_from(*target_col_usize).expect("target_col fits in u8");

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
                col_index: u8::try_from(pred_col_idx).expect("col idx fits in u8"),
                op: fused_cmp,
                literal: lit,
            })
        }
        _ => return Ok(None),
    };

    let rel = RelationId(entry.oid);
    let block_count = ctx.heap.block_count(rel).max(entry.n_blocks);
    let op = FusedUpdateInt32Add::new(
        Arc::clone(&ctx.heap),
        rel,
        ctx.snapshot.clone(),
        Arc::clone(&ctx.oracle),
        block_count,
        predicate,
        target_col,
        delta,
        ctx.xid,
        ctx.command_id,
    );
    Ok(Some(Box::new(op)))
}

pub(super) fn lower_real_update(
    table: &str,
    assignments: &[(usize, ScalarExpr)],
    input: &LogicalPlan,
    returning: &[(ScalarExpr, String)],
    ctx: &LowerCtx<'_>,
) -> Result<Box<dyn Operator>, ServerError> {
    if !returning.is_empty() {
        return Err(ServerError::Unsupported("UPDATE ... RETURNING"));
    }
    let entry = ctx
        .catalog_snapshot
        .tables
        .get(&table.to_ascii_lowercase())
        .ok_or_else(|| {
            ServerError::Plan(ultrasql_planner::PlanError::TableNotFound(
                table.to_string(),
            ))
        })?;

    // Fast-path: when the relation, assignment, and optional filter
    // all match the `(Int32, Int32) WHERE col cmp lit SET col_i =
    // col_i ± lit` shape, bypass the SeqScan + Filter + ModifyTable
    // chain entirely and lower to the single `FusedUpdateInt32Add`
    // operator. Saves ~150 µs / 10 000-row UPDATE on the bench shape
    // — see the operator's module header for the full motivation.
    if let Some(fused) = try_build_fused_update(table, entry, assignments, input, ctx)? {
        return Ok(fused);
    }

    let child = build_filtered_tid_scan(table, entry, input, ctx)?;

    // Assignment value expressions stay unshifted: `apply_update`
    // strips the leading [tid_block, tid_slot] pair before passing the
    // row to `Eval::eval`, so the value expression sees the relation's
    // natural column layout. Likewise, the assignment's *target*
    // column index addresses the relation schema directly.
    let assignments: Vec<(usize, ScalarExpr)> = assignments.to_vec();

    let rel = RelationId(entry.oid);
    let modify = ModifyTable::new(
        Arc::clone(&ctx.heap),
        rel,
        entry.schema.clone(),
        ModifyKind::Update { assignments },
        ctx.xid,
        ctx.command_id,
        ctx.xid,
        ctx.command_id,
        None,
        child,
    );
    Ok(Box::new(modify))
}

/// Try to detect the `(Int32, Int32) [WHERE col cmp lit]` DELETE
/// shape and lower it to [`FusedDeleteInt32Pair`]. Mirrors

pub(super) fn try_build_fused_delete(
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
                col_index: u8::try_from(pred_col_idx).expect("col idx fits in u8"),
                op: fused_cmp,
                literal: lit,
            })
        }
        _ => return Ok(None),
    };

    let rel = RelationId(entry.oid);
    let block_count = ctx.heap.block_count(rel).max(entry.n_blocks);
    let op = FusedDeleteInt32Pair::new(
        Arc::clone(&ctx.heap),
        rel,
        ctx.snapshot.clone(),
        Arc::clone(&ctx.oracle),
        block_count,
        predicate,
        ctx.xid,
        ctx.command_id,
    );
    Ok(Some(Box::new(op)))
}

/// Lower a `DELETE` plan into a [`ModifyTable`] with `ModifyKind::Delete`.
///

pub(super) fn lower_real_delete(
    table: &str,
    input: &LogicalPlan,
    returning: &[(ScalarExpr, String)],
    ctx: &LowerCtx<'_>,
) -> Result<Box<dyn Operator>, ServerError> {
    if !returning.is_empty() {
        return Err(ServerError::Unsupported("DELETE ... RETURNING"));
    }
    let entry = ctx
        .catalog_snapshot
        .tables
        .get(&table.to_ascii_lowercase())
        .ok_or_else(|| {
            ServerError::Plan(ultrasql_planner::PlanError::TableNotFound(
                table.to_string(),
            ))
        })?;

    // Fast-path: when the relation matches the `(Int32, Int32)` shape
    // and the optional filter is `Int32 col cmp Int32 lit`, bypass
    // the SeqScan + Filter + ModifyTable chain and lower to the
    // single-pass `FusedDeleteInt32Pair` operator.
    if let Some(fused) = try_build_fused_delete(table, entry, input, ctx)? {
        return Ok(fused);
    }

    let child = build_filtered_tid_scan(table, entry, input, ctx)?;

    let rel = RelationId(entry.oid);
    let modify = ModifyTable::new(
        Arc::clone(&ctx.heap),
        rel,
        entry.schema.clone(),
        ModifyKind::Delete,
        ctx.xid,
        ctx.command_id,
        ctx.xid,
        ctx.command_id,
        None,
        child,
    );
    Ok(Box::new(modify))
}

/// Build the TID-emitting child operator for an UPDATE / DELETE.
///
/// Recognises the binder's `Scan` / `Filter(Scan)` shapes:
///
/// - bare `Scan { table }` → TID-emitting `SeqScan`.
/// - `Filter { Scan { table }, predicate }` → `Filter`(`SeqScan`),
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
            let LogicalPlan::Scan { table, .. } = filter_input.as_ref() else {
                return Err(ServerError::Unsupported(
                    "UPDATE / DELETE WHERE input must be a base-table scan",
                ));
            };
            if !table.eq_ignore_ascii_case(target_table) {
                return Err(ServerError::Unsupported(
                    "UPDATE / DELETE child scan references a different table",
                ));
            }
            let scan = build_tid_seq_scan(entry, ctx);
            let shifted = shift_column_indices(predicate, 2);
            Ok(Box::new(Filter::new(scan, shifted)))
        }
        _ => Err(ServerError::Unsupported(
            "UPDATE / DELETE input shape; expected Scan or Filter(Scan)",
        )),
    }
}

pub(super) fn lower_project_columns(
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
        let output_schema = ultrasql_core::Schema::new(fields)
            .map_err(|e| ServerError::Plan(ultrasql_planner::PlanError::TypeMismatch(format!(
                "projection schema: {e}"
            ))))?;
        return ultrasql_executor::ProjectExprs::new(child, exprs, output_schema)
            .map(|op| Box::new(op) as Box<dyn Operator>)
            .map_err(|e| ServerError::Plan(ultrasql_planner::PlanError::TypeMismatch(format!(
                "projection: {e}"
            ))));
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
    let is_identity_indices = indices.len() == child_width
        && indices.iter().enumerate().all(|(i, &idx)| i == idx);
    let names_match = is_identity_indices
        && exprs.iter().enumerate().all(|(i, (_, name))| {
            child_schema.field_at(i).name == *name
        });
    if names_match {
        return Ok(child);
    }
    Ok(Box::new(Project::new(child, indices)?))
}
