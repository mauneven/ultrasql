//! Helpers for recursive CTEs and `UNION`/`INTERSECT`/`EXCEPT` lowering.

use std::sync::Arc;

use ultrasql_core::Schema;
use ultrasql_executor::{Operator, SetOp};
use ultrasql_planner::LogicalPlan;
use ultrasql_vec::column::Column;
use ultrasql_vec::{Batch, DictionaryEncodingPolicy, StringEncoding, encode_strings_auto};

use crate::error::ServerError;

use super::lower_query::lower_query;
use super::{CteBuffer, LowerCtx};

pub(super) fn lower_recursive_cte(
    name: &str,
    definition: &LogicalPlan,
    body: &LogicalPlan,
    ctx: &LowerCtx<'_>,
) -> Result<Box<dyn Operator>, ServerError> {
    use ultrasql_planner::LogicalSetOp;

    let (op, quantifier, anchor, recursive_term, schema) = match definition {
        LogicalPlan::SetOp {
            op,
            quantifier,
            left,
            right,
            schema,
        } => (
            *op,
            *quantifier,
            left.as_ref(),
            right.as_ref(),
            schema.clone(),
        ),
        _ => {
            return Err(ServerError::Unsupported(
                "WITH RECURSIVE definition must be a UNION of an anchor + recursive term",
            ));
        }
    };
    if op != LogicalSetOp::Union {
        return Err(ServerError::Unsupported(
            "WITH RECURSIVE supports only UNION (not INTERSECT or EXCEPT)",
        ));
    }

    // Cap on iterations matches PostgreSQL's recommendation for
    // non-terminating queries (`max_recursive_iterations` GUC). 1024
    // is comfortable for graph traversals while still bounding a
    // runaway plan.
    const MAX_ITERATIONS: usize = 1024;

    let _ = schema; // SetOp's schema is identical to the anchor's after binding.
    let mut accumulator: Vec<Batch> = Vec::new();
    let mut working: Vec<Batch> = Vec::new();

    // Step 1 — lower and drain the anchor. Anchor sees the parent
    // overlay (it cannot reference the CTE itself by name).
    let mut anchor_op = lower_query(anchor, ctx)?;
    let def_schema = anchor_op.schema().clone();
    while let Some(b) = anchor_op.next_batch()? {
        if b.rows() > 0 {
            working.push(b.clone());
            accumulator.push(b);
        }
    }

    // Step 2 — fixpoint loop.
    let dedup = matches!(quantifier, ultrasql_planner::LogicalSetQuantifier::Distinct);
    let mut seen_keys: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
    if dedup {
        for b in &accumulator {
            for k in batch_row_keys(b) {
                seen_keys.insert(k);
            }
        }
    }
    for _ in 0..MAX_ITERATIONS {
        if working.is_empty() {
            break;
        }
        let mut child_buffers = ctx.cte_buffers.clone();
        child_buffers.insert(
            name.to_ascii_lowercase(),
            CteBuffer {
                batches: Arc::new(std::mem::take(&mut working)),
                schema: def_schema.clone(),
            },
        );
        let child_ctx = LowerCtx {
            tables: ctx.tables,
            catalog_snapshot: Arc::clone(&ctx.catalog_snapshot),
            table_constraints: Arc::clone(&ctx.table_constraints),
            sequences: Arc::clone(&ctx.sequences),
            persistent_catalog: Arc::clone(&ctx.persistent_catalog),
            time_partitions: Arc::clone(&ctx.time_partitions),
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

        let mut term_op = lower_query(recursive_term, &child_ctx)?;
        let mut new_batches: Vec<Batch> = Vec::new();
        while let Some(b) = term_op.next_batch()? {
            if b.rows() == 0 {
                continue;
            }
            if dedup {
                let kept = filter_unseen_rows(&b, &mut seen_keys)?;
                if let Some(kept) = kept {
                    if kept.rows() > 0 {
                        new_batches.push(kept);
                    }
                }
            } else {
                new_batches.push(b);
            }
        }
        if new_batches.is_empty() {
            break;
        }
        for b in &new_batches {
            accumulator.push(b.clone());
        }
        working = new_batches;
    }

    // Step 3 — bind body with the full accumulator as the CTE
    // buffer. From the body's perspective the CTE is a single
    // materialised relation.
    let mut body_buffers = ctx.cte_buffers.clone();
    body_buffers.insert(
        name.to_ascii_lowercase(),
        CteBuffer {
            batches: Arc::new(accumulator),
            schema: def_schema,
        },
    );
    let body_ctx = LowerCtx {
        tables: ctx.tables,
        catalog_snapshot: Arc::clone(&ctx.catalog_snapshot),
        table_constraints: Arc::clone(&ctx.table_constraints),
        sequences: Arc::clone(&ctx.sequences),
        persistent_catalog: Arc::clone(&ctx.persistent_catalog),
        time_partitions: Arc::clone(&ctx.time_partitions),
        sequence_state: ctx.sequence_state.clone(),
        heap: Arc::clone(&ctx.heap),
        vm: Arc::clone(&ctx.vm),
        snapshot: ctx.snapshot.clone(),
        oracle: Arc::clone(&ctx.oracle),
        xid: ctx.xid,
        command_id: ctx.command_id,
        cte_buffers: body_buffers,
        jit: ctx.jit,
        cancel_flag: ctx.cancel_flag.clone(),
        work_mem: std::sync::Arc::clone(&ctx.work_mem),
    };
    lower_query(body, &body_ctx)
}

/// Encode every row of `batch` into a flat byte key for set-membership
/// dedup in the recursive UNION fixpoint. Keys must compare equal
/// when rows are equal under SQL semantics; for the v0.5 type set
/// (Int32, Int64, Float32, Float64, Bool, Text) the encoding is the

pub(super) fn batch_row_keys(batch: &Batch) -> Vec<Vec<u8>> {
    let n_rows = batch.rows();
    let mut keys: Vec<Vec<u8>> = (0..n_rows).map(|_| Vec::with_capacity(64)).collect();
    for col in batch.columns() {
        for (row_idx, key) in keys.iter_mut().enumerate() {
            match col {
                Column::Int32(c) => {
                    if c.nulls().is_some_and(|n| !n.get(row_idx)) {
                        key.push(0xFF);
                    } else {
                        key.push(0x00);
                        key.extend_from_slice(&c.data()[row_idx].to_le_bytes());
                    }
                }
                Column::Int64(c) => {
                    if c.nulls().is_some_and(|n| !n.get(row_idx)) {
                        key.push(0xFF);
                    } else {
                        key.push(0x00);
                        key.extend_from_slice(&c.data()[row_idx].to_le_bytes());
                    }
                }
                Column::Utf8(c) => {
                    if c.nulls().is_some_and(|n| !n.get(row_idx)) {
                        key.push(0xFF);
                    } else {
                        key.push(0x00);
                        let s = c.value(row_idx);
                        key.extend_from_slice(
                            &u32::try_from(s.len())
                                .expect("CTE distinct key string length under u32::MAX")
                                .to_le_bytes(),
                        );
                        key.extend_from_slice(s.as_bytes());
                    }
                }
                Column::DictionaryUtf8(c) => {
                    if c.codes.nulls().is_some_and(|n| !n.get(row_idx)) {
                        key.push(0xFF);
                    } else {
                        key.push(0x00);
                        let s = c.decode_at(row_idx);
                        key.extend_from_slice(
                            &u32::try_from(s.len())
                                .expect("CTE distinct key string length under u32::MAX")
                                .to_le_bytes(),
                        );
                        key.extend_from_slice(s.as_bytes());
                    }
                }
                Column::Bool(c) => {
                    if c.nulls().is_some_and(|n| !n.get(row_idx)) {
                        key.push(0xFF);
                    } else {
                        key.push(if c.value(row_idx) { 0x01 } else { 0x00 });
                    }
                }
                Column::Float32(c) => {
                    if c.nulls().is_some_and(|n| !n.get(row_idx)) {
                        key.push(0xFF);
                    } else {
                        key.push(0x00);
                        key.extend_from_slice(&c.data()[row_idx].to_le_bytes());
                    }
                }
                Column::Float64(c) => {
                    if c.nulls().is_some_and(|n| !n.get(row_idx)) {
                        key.push(0xFF);
                    } else {
                        key.push(0x00);
                        key.extend_from_slice(&c.data()[row_idx].to_le_bytes());
                    }
                }
            }
        }
    }
    keys
}

/// Return a sub-batch of `batch` containing only rows whose encoded
/// key is not already in `seen`. Rows that survive get added to
/// `seen`.
pub(super) fn filter_unseen_rows(
    batch: &Batch,
    seen: &mut std::collections::HashSet<Vec<u8>>,
) -> Result<Option<Batch>, ServerError> {
    let keys = batch_row_keys(batch);
    let mut keep_mask = Vec::with_capacity(keys.len());
    for k in keys {
        if seen.insert(k) {
            keep_mask.push(true);
        } else {
            keep_mask.push(false);
        }
    }
    if !keep_mask.iter().any(|&b| b) {
        return Ok(None);
    }
    if keep_mask.iter().all(|&b| b) {
        return Ok(Some(batch.clone()));
    }
    // Rebuild the batch keeping only the marked rows.
    let mut cols: Vec<Column> = Vec::with_capacity(batch.columns().len());
    for col in batch.columns() {
        let new_col = match col {
            Column::Int32(c) => Column::Int32(filter_numeric(c, &keep_mask)),
            Column::Int64(c) => Column::Int64(filter_numeric(c, &keep_mask)),
            Column::Float32(c) => Column::Float32(filter_numeric(c, &keep_mask)),
            Column::Float64(c) => Column::Float64(filter_numeric(c, &keep_mask)),
            Column::Bool(c) => {
                let data: Vec<bool> = c
                    .data()
                    .iter()
                    .zip(keep_mask.iter())
                    .filter_map(|(v, k)| k.then_some(*v != 0))
                    .collect();
                Column::Bool(ultrasql_vec::column::BoolColumn::from_data(data))
            }
            Column::Utf8(_) | Column::DictionaryUtf8(_) => {
                let strings: Vec<Option<String>> = (0..keep_mask.len())
                    .filter(|&i| keep_mask[i])
                    .map(|i| col.text_value(i).map(str::to_owned))
                    .collect();
                match encode_strings_auto(
                    strings.iter().map(|v| v.as_deref()),
                    DictionaryEncodingPolicy::default(),
                ) {
                    StringEncoding::Raw(c) => Column::Utf8(c),
                    StringEncoding::Dictionary(c) => Column::DictionaryUtf8(c),
                }
            }
        };
        cols.push(new_col);
    }
    Batch::new(cols).map(Some).map_err(|e| {
        ServerError::Unsupported(Box::leak(
            format!("recursive CTE filter: {e}").into_boxed_str(),
        ))
    })
}

/// Filter helper for numeric columns — drops rows whose mask bit is 0.
pub(super) fn filter_numeric<T: Copy>(
    col: &ultrasql_vec::column::NumericColumn<T>,
    keep_mask: &[bool],
) -> ultrasql_vec::column::NumericColumn<T> {
    let data: Vec<T> = col
        .data()
        .iter()
        .zip(keep_mask.iter())
        .filter_map(|(v, k)| k.then_some(*v))
        .collect();
    ultrasql_vec::column::NumericColumn::from_data(data)
}

/// Re-check the contract `bind_set_op` enforces: both inputs must have
/// the same arity. Per-column type-compatibility is the binder's job;
/// we only catch the arity mismatch here so a hand-built plan that
/// skipped binding fails with a precise error instead of crashing the

pub(super) fn check_set_op_schemas(left: &Schema, right: &Schema) -> Result<(), ServerError> {
    if left.len() != right.len() {
        return Err(ServerError::Unsupported(
            "set operation: left and right sides must have the same number of columns",
        ));
    }
    Ok(())
}

/// Build a [`SetOp`] over the catalog-aware [`lower_query`] path.
///
/// The two children are lowered through the same real-heap-aware path
/// so a set-op can sit on top of `SeqScan` over a persistent relation,
/// an in-memory `Values`/`MemTableScan`, or any other supported source.
/// The executor's `SetOp` kernel
/// (`crates/ultrasql-executor/src/set_op.rs`) implements all six SQL
/// shapes (UNION / INTERSECT / EXCEPT × ALL / DISTINCT) with a
/// hash-counting algorithm, treating two NULLs as equal (matching
/// PostgreSQL `DISTINCT` semantics). The kernel is fully materialising:
/// it drains both inputs before emitting its first row, so the operator
/// is a pipeline breaker bounded by the same in-memory footprint as
/// `HashAggregate` / `Sort` until the v0.7 `work_mem` spill lands.
///
/// Schema-compatibility: the binder enforces arity and per-column
/// `numeric_join` compatibility (see `binder::bind_set_op`). We re-check
/// arity through [`check_set_op_schemas`] so a hand-built plan that
/// bypassed the binder still surfaces a precise error rather than
/// producing wrong rows.
pub(super) fn lower_set_op_real(
    op: ultrasql_planner::LogicalSetOp,
    quantifier: ultrasql_planner::LogicalSetQuantifier,
    left: &LogicalPlan,
    right: &LogicalPlan,
    out_schema: Schema,
    ctx: &LowerCtx<'_>,
) -> Result<Box<dyn Operator>, ServerError> {
    check_set_op_schemas(left.schema(), right.schema())?;
    let left_op = lower_query(left, ctx)?;
    let right_op = lower_query(right, ctx)?;
    Ok(Box::new(SetOp::new(
        left_op, right_op, op, quantifier, out_schema,
    )))
}
