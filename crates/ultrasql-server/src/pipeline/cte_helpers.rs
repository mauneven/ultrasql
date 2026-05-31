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
            for k in batch_row_keys(b)? {
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
            sequence_owners: Arc::clone(&ctx.sequence_owners),
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
        sequence_owners: Arc::clone(&ctx.sequence_owners),
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
        cte_buffers: body_buffers,
        jit: ctx.jit,
        cancel_flag: ctx.cancel_flag.clone(),
        work_mem: std::sync::Arc::clone(&ctx.work_mem),
        profile_operators: ctx.profile_operators,
    };
    lower_query(body, &body_ctx)
}

/// Encode every row of `batch` into a flat byte key for set-membership
/// dedup in the recursive UNION fixpoint. Keys must compare equal
/// when rows are equal under SQL semantics; for the v0.5 type set
/// (Int32, Int64, Float32, Float64, Bool, Text) the encoding is the

pub(super) fn batch_row_keys(batch: &Batch) -> Result<Vec<Vec<u8>>, ServerError> {
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
                        key.extend_from_slice(&cte_key_text_len(s)?.to_le_bytes());
                        key.extend_from_slice(s.as_bytes());
                    }
                }
                Column::DictionaryUtf8(c) => {
                    if c.codes.nulls().is_some_and(|n| !n.get(row_idx)) {
                        key.push(0xFF);
                    } else {
                        key.push(0x00);
                        let s = c.decode_at(row_idx);
                        key.extend_from_slice(&cte_key_text_len(s)?.to_le_bytes());
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
    Ok(keys)
}

fn cte_key_text_len(text: &str) -> Result<u32, ServerError> {
    u32::try_from(text.len())
        .map_err(|_| ServerError::unsupported("recursive CTE distinct key text too large"))
}

/// Return a sub-batch of `batch` containing only rows whose encoded
/// key is not already in `seen`. Rows that survive get added to
/// `seen`.
pub(super) fn filter_unseen_rows(
    batch: &Batch,
    seen: &mut std::collections::HashSet<Vec<u8>>,
) -> Result<Option<Batch>, ServerError> {
    let keys = batch_row_keys(batch)?;
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
            Column::Int32(c) => Column::Int32(filter_numeric(c, &keep_mask)?),
            Column::Int64(c) => Column::Int64(filter_numeric(c, &keep_mask)?),
            Column::Float32(c) => Column::Float32(filter_numeric(c, &keep_mask)?),
            Column::Float64(c) => Column::Float64(filter_numeric(c, &keep_mask)?),
            Column::Bool(c) => {
                let data: Vec<bool> = c
                    .data()
                    .iter()
                    .zip(keep_mask.iter())
                    .filter_map(|(v, k)| k.then_some(*v != 0))
                    .collect();
                let nulls = c.nulls().map(|source| filter_bitmap(source, &keep_mask));
                let col = match nulls {
                    Some(nulls) => ultrasql_vec::column::BoolColumn::with_nulls(data, nulls)
                        .map_err(cte_filter_error)?,
                    None => ultrasql_vec::column::BoolColumn::from_data(data),
                };
                Column::Bool(col)
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
    Batch::new(cols)
        .map(Some)
        .map_err(|e| ServerError::unsupported(format!("recursive CTE filter: {e}")))
}

/// Filter helper for numeric columns — drops rows whose mask bit is 0.
pub(super) fn filter_numeric<T: Copy>(
    col: &ultrasql_vec::column::NumericColumn<T>,
    keep_mask: &[bool],
) -> Result<ultrasql_vec::column::NumericColumn<T>, ServerError> {
    let data: Vec<T> = col
        .data()
        .iter()
        .zip(keep_mask.iter())
        .filter_map(|(v, k)| k.then_some(*v))
        .collect();
    match col.nulls().map(|source| filter_bitmap(source, keep_mask)) {
        Some(nulls) => {
            ultrasql_vec::column::NumericColumn::with_nulls(data, nulls).map_err(cte_filter_error)
        }
        None => Ok(ultrasql_vec::column::NumericColumn::from_data(data)),
    }
}

fn cte_filter_error(err: ultrasql_vec::column::ColumnError) -> ServerError {
    ServerError::unsupported(format!("recursive CTE filter: {err}"))
}

fn filter_bitmap(source: &ultrasql_vec::Bitmap, keep_mask: &[bool]) -> ultrasql_vec::Bitmap {
    let kept = keep_mask.iter().filter(|&&keep| keep).count();
    let mut out = ultrasql_vec::Bitmap::new(kept, true);
    let mut out_idx = 0;
    for (idx, keep) in keep_mask.iter().copied().enumerate() {
        if keep {
            out.set(out_idx, source.get(idx));
            out_idx += 1;
        }
    }
    out
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
/// is a pipeline breaker with its own in-memory footprint. `Sort` and
/// grouped `HashAggregate` now have dedicated `work_mem` spill paths;
/// `SetOp` spill remains separate follow-up work.
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

#[cfg(test)]
mod tests {
    use super::*;
    use ultrasql_core::{DataType, Field};
    use ultrasql_vec::Bitmap;
    use ultrasql_vec::column::{BoolColumn, Column, NumericColumn, StringColumn};
    use ultrasql_vec::dict::DictionaryColumn;

    fn validity(bits: &[bool]) -> Bitmap {
        let mut bitmap = Bitmap::new(bits.len(), true);
        for (idx, valid) in bits.iter().copied().enumerate() {
            bitmap.set(idx, valid);
        }
        bitmap
    }

    fn mixed_batch() -> Batch {
        Batch::new(vec![
            Column::Int32(
                NumericColumn::with_nulls(vec![1, 99, 1], validity(&[true, false, true]))
                    .expect("i32 nulls"),
            ),
            Column::Int64(NumericColumn::from_data(vec![10, 20, 10])),
            Column::Float32(NumericColumn::from_data(vec![1.0, 2.0, 1.0])),
            Column::Float64(NumericColumn::from_data(vec![3.0, 4.0, 3.0])),
            Column::Bool(
                BoolColumn::with_nulls(vec![true, false, true], validity(&[true, false, true]))
                    .expect("bool nulls"),
            ),
            Column::Utf8(
                StringColumn::with_nulls(
                    ["same".to_owned(), String::new(), "same".to_owned()],
                    validity(&[true, false, true]),
                )
                .expect("utf8 nulls"),
            ),
            Column::DictionaryUtf8(
                DictionaryColumn::from_strings([Some("dict"), None, Some("dict")])
                    .expect("test dictionary should fit u32 codes"),
            ),
        ])
        .expect("mixed batch")
    }

    #[test]
    fn row_keys_and_unseen_filter_preserve_nulls_across_column_kinds() {
        let batch = mixed_batch();
        let keys = batch_row_keys(&batch).expect("keys");
        assert_eq!(keys.len(), 3);
        assert_eq!(keys[0], keys[2]);
        assert_ne!(keys[0], keys[1]);

        let mut seen = std::collections::HashSet::new();
        seen.insert(keys[0].clone());
        let kept = filter_unseen_rows(&batch, &mut seen)
            .expect("filter")
            .expect("one null row survives");
        assert_eq!(kept.rows(), 1);
        let Column::Int32(i32s) = &kept.columns()[0] else {
            panic!("i32 column");
        };
        assert!(!i32s.nulls().expect("i32 nulls").get(0));
        let Column::Bool(flags) = &kept.columns()[4] else {
            panic!("bool column");
        };
        assert!(!flags.nulls().expect("bool nulls").get(0));
        assert_eq!(kept.columns()[5].text_value(0), None);
        assert_eq!(kept.columns()[6].text_value(0), None);

        assert!(
            filter_unseen_rows(&batch, &mut seen)
                .expect("all seen")
                .is_none()
        );
    }

    #[test]
    fn set_op_schema_check_reports_arity_mismatch() {
        let left = Schema::new([Field::required("id", DataType::Int32)]).expect("left");
        let right = Schema::new([
            Field::required("id", DataType::Int32),
            Field::required("name", DataType::Text { max_len: None }),
        ])
        .expect("right");
        assert!(check_set_op_schemas(&left, &right).is_err());
        assert!(check_set_op_schemas(&left, &left).is_ok());
    }
}
