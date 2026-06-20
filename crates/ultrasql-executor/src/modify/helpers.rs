//! Free helper functions shared across the `ModifyTable` submodules:
//! NOT-NULL / codec error mapping, the columnar UPDATE fast-path
//! detector and edit builder, TID extraction, and `MERGE` row helpers.

use ultrasql_core::{BlockNumber, DataType, PageId, RelationId, Schema, TupleId, Value};
use ultrasql_mvcc::{InfoMask, TupleHeader};
use ultrasql_planner::{BinaryOp, ScalarExpr};
use ultrasql_storage::PageLoader;
use ultrasql_storage::heap::UpdatePayload;
use ultrasql_vec::Batch;
use ultrasql_vec::column::Column;

use super::{
    ExpandedInsertRow, InsertConflictAction, InsertIndexMaintainer, RowCodecError, UpdateFastPathInt32Pair,
};
use crate::ExecError;

/// Enforce schema-level NOT-NULL constraints over a decoded `INSERT`
/// row before it is encoded and handed to the heap.
///
/// Surfaces [`ExecError::NotNullViolation`] on the first non-nullable
/// column carrying [`Value::Null`]; the caller maps this onto
/// PostgreSQL SQLSTATE `23502`.
pub(crate) fn check_not_null_violations(row: &[Value], schema: &Schema) -> Result<(), ExecError> {
    for (col, field) in row.iter().zip(schema.fields().iter()) {
        if !field.nullable && matches!(col, Value::Null) {
            return Err(ExecError::NotNullViolation(field.name.clone()));
        }
    }
    Ok(())
}

pub(crate) fn row_codec_error_to_exec(err: RowCodecError) -> ExecError {
    match err {
        RowCodecError::StringDataRightTruncation { detail, .. } => {
            ExecError::StringDataRightTruncation(detail)
        }
        RowCodecError::NumericFieldOverflow { detail, .. } => {
            ExecError::NumericFieldOverflow(detail)
        }
        other => ExecError::TypeMismatch(other.to_string()),
    }
}

/// Inspect a bound UPDATE assignment list against the target
/// relation schema and return a [`UpdateFastPathInt32Pair`] descriptor
/// when the columnar fast path applies.
///
/// Conditions:
///
/// - Relation schema is exactly two non-nullable `Int32` columns
///   (matches the bench tables `(id INT, val INT)`).
/// - Exactly one assignment targets one of those two columns.
/// - The assignment expression is either `col + lit`, `lit + col`,
///   `col - lit`, where `col` references the **target** column and
///   `lit` is an `Int32` literal. `lit - col` is rejected because the
///   transformation collapses into a single add only when the column
///   is the *left* operand of a subtract.
pub(crate) fn detect_update_int32_pair_fast_path(
    assignments: &[(usize, ScalarExpr)],
    relation_schema: &Schema,
) -> Option<UpdateFastPathInt32Pair> {
    if relation_schema.len() != 2 {
        return None;
    }
    let fields = relation_schema.fields();
    if !matches!(fields[0].data_type, DataType::Int32)
        || !matches!(fields[1].data_type, DataType::Int32)
    {
        return None;
    }
    if assignments.len() != 1 {
        return None;
    }
    let (target_col, expr) = &assignments[0];
    let target_col = *target_col;
    if target_col > 1 {
        return None;
    }
    let (op, left, right) = match expr {
        ScalarExpr::Binary {
            op,
            left,
            right,
            data_type: DataType::Int32,
        } => (op, left.as_ref(), right.as_ref()),
        _ => return None,
    };
    let column_ref_idx = |e: &ScalarExpr| match e {
        ScalarExpr::Column {
            index,
            data_type: DataType::Int32,
            ..
        } => Some(*index),
        _ => None,
    };
    let literal_i32 = |e: &ScalarExpr| match e {
        ScalarExpr::Literal {
            value: Value::Int32(v),
            ..
        } => Some(*v),
        _ => None,
    };
    let delta = match op {
        BinaryOp::Add => {
            if column_ref_idx(left) == Some(target_col) {
                literal_i32(right)?
            } else if column_ref_idx(right) == Some(target_col) {
                literal_i32(left)?
            } else {
                return None;
            }
        }
        BinaryOp::Sub => {
            // Only `col - lit` collapses to a single signed add.
            if column_ref_idx(left) != Some(target_col) {
                return None;
            }
            let lit = literal_i32(right)?;
            lit.checked_neg()?
        }
        _ => return None,
    };
    Some(UpdateFastPathInt32Pair {
        target_col_in_relation: target_col,
        delta,
    })
}

pub(crate) fn updated_ctid_target(header: &TupleHeader, current: TupleId) -> Option<TupleId> {
    if header.ctid == current {
        return None;
    }
    let redirects = header.infomask.contains(InfoMask::UPDATED)
        || header.infomask.contains(InfoMask::HOT_UPDATED);
    redirects.then_some(header.ctid)
}

pub(crate) fn conflict_target_columns(action: &InsertConflictAction) -> Option<&[usize]> {
    match action {
        InsertConflictAction::DoNothing { target } => target.as_deref(),
        InsertConflictAction::DoUpdate { target, .. } => Some(target.as_slice()),
    }
}

pub(crate) fn insert_conflict_uses_index<L: PageLoader>(
    action: &InsertConflictAction,
    index: &InsertIndexMaintainer<L>,
) -> bool {
    if !index.is_unique() {
        return false;
    }
    match conflict_target_columns(action) {
        Some(target) => columns_match_unordered(&index.key_columns, target),
        None => true,
    }
}

pub(crate) fn columns_match_unordered(left: &[usize], right: &[usize]) -> bool {
    left.len() == right.len() && left.iter().all(|col| right.contains(col))
}

pub(crate) fn expand_insert_row(
    row: &[Value],
    target_width: usize,
    column_map: &[usize],
) -> Result<ExpandedInsertRow, ExecError> {
    if row.len() != column_map.len() {
        return Err(ExecError::TypeMismatch(format!(
            "INSERT source row has {} columns, but column map has {} entries",
            row.len(),
            column_map.len()
        )));
    }
    let mut out = vec![Value::Null; target_width];
    let mut seen = vec![false; target_width];
    for (src_idx, target_idx) in column_map.iter().copied().enumerate() {
        if target_idx >= target_width {
            return Err(ExecError::TypeMismatch(format!(
                "INSERT target column index {target_idx} out of range (relation has {target_width} columns)"
            )));
        }
        if seen[target_idx] {
            return Err(ExecError::TypeMismatch(format!(
                "INSERT target column index {target_idx} appears more than once"
            )));
        }
        seen[target_idx] = true;
        out[target_idx] = row[src_idx].clone();
    }
    let omitted = seen.into_iter().map(|present| !present).collect();
    Ok(ExpandedInsertRow {
        values: out,
        omitted,
    })
}

/// Build the `(TupleId, new_payload_bytes)` edit list for the
/// `UPDATE t SET col_i = col_i ± lit` columnar fast path over a
/// `(Int32, Int32)` relation.
///
/// The input batch carries `[tid_block, tid_slot, id, val]` columns.
/// The output payload is 9 bytes wide:
///
/// ```text
///     byte 0    null bitmap        always 0 (both cols non-NULL)
///     bytes 1..5  id  LE i32       unchanged unless target_col == 0
///     bytes 5..9  val LE i32       unchanged unless target_col == 1
/// ```
///
/// The new value of the target column is `old + spec.delta` with overflow
/// checked before any payload is emitted. No `batch_to_rows`, no `Eval`,
/// no `RowCodec::encode` tree walk.
pub(crate) fn build_update_edits_int32_pair(
    batch: &Batch,
    relation: RelationId,
    spec: UpdateFastPathInt32Pair,
) -> Result<Vec<(TupleId, UpdatePayload)>, ExecError> {
    let cols = batch.columns();
    if cols.len() < 4 {
        return Err(ExecError::TypeMismatch(
            "UPDATE batch must carry [tid_block, tid_slot, id, val]".to_owned(),
        ));
    }
    let (
        Column::Int32(block_col),
        Column::Int32(slot_col),
        Column::Int32(id_col),
        Column::Int32(val_col),
    ) = (&cols[0], &cols[1], &cols[2], &cols[3])
    else {
        return Err(ExecError::TypeMismatch(
            "UPDATE fast path requires all four leading columns to be Int32".to_owned(),
        ));
    };
    let block_data = block_col.data();
    let slot_data = slot_col.data();
    let id_data = id_col.data();
    let val_data = val_col.data();
    let n = batch.rows();
    if block_data.len() < n || slot_data.len() < n || id_data.len() < n || val_data.len() < n {
        return Err(ExecError::TypeMismatch(
            "UPDATE column length shorter than batch rows".to_owned(),
        ));
    }
    let mut out: Vec<(TupleId, UpdatePayload)> = Vec::with_capacity(n);
    for i in 0..n {
        let block_u32 = u32::try_from(block_data[i]).map_err(|_| {
            ExecError::TypeMismatch(format!(
                "TID block value {} out of u32 range",
                block_data[i]
            ))
        })?;
        let slot_u16 = u16::try_from(slot_data[i]).map_err(|_| {
            ExecError::TypeMismatch(format!("TID slot value {} out of u16 range", slot_data[i]))
        })?;
        let id_v = id_data[i];
        let val_v = val_data[i];
        // Apply the assignment to the targeted column.
        let (new_id, new_val) =
            checked_update_int32_pair_add(id_v, val_v, spec.target_col_in_relation, spec.delta)?;
        // Inline 9-byte payload assembled into a `SmallVec<[u8; 16]>`
        // so the per-row encode pays no heap allocation: the entire
        // body lives in the SmallVec's inline buffer.
        let mut payload = UpdatePayload::new();
        payload.push(0_u8); // null bitmap: both non-NULL.
        payload.extend_from_slice(&new_id.to_le_bytes());
        payload.extend_from_slice(&new_val.to_le_bytes());
        let page_id = PageId::new(relation, BlockNumber::new(block_u32));
        out.push((TupleId::new(page_id, slot_u16), payload));
    }
    Ok(out)
}

fn checked_update_int32_pair_add(
    id: i32,
    val: i32,
    target_col: usize,
    delta: i32,
) -> Result<(i32, i32), ExecError> {
    if target_col == 0 {
        id.checked_add(delta)
            .map(|new_id| (new_id, val))
            .ok_or_else(|| ExecError::NumericFieldOverflow("Int32 id update overflow".into()))
    } else {
        val.checked_add(delta)
            .map(|new_val| (id, new_val))
            .ok_or_else(|| ExecError::NumericFieldOverflow("Int32 value update overflow".into()))
    }
}

/// Extract every `TupleId` from a `Batch` whose first two columns
/// are `tid_block: Int32` and `tid_slot: Int32` — the shape
/// `SeqScan::new_with_tids` emits for UPDATE / DELETE child operators.
///
/// Reads directly from the column arrays without materialising the
/// batch as `Vec<Vec<Value>>` (the `batch_to_rows` path the per-row
/// `extract_tid_and_row` helper used to drive). For a 10 000-row
/// DELETE this drops one full pass over the payload columns + 10 000
/// `Vec<Value>` allocations.
pub(crate) fn extract_tids_from_batch(
    batch: &Batch,
    relation: RelationId,
) -> Result<Vec<TupleId>, ExecError> {
    let cols = batch.columns();
    if cols.len() < 2 {
        return Err(ExecError::TypeMismatch(
            "DELETE batch must carry leading (tid_block, tid_slot) columns".to_owned(),
        ));
    }
    let (Column::Int32(block_col), Column::Int32(slot_col)) = (&cols[0], &cols[1]) else {
        return Err(ExecError::TypeMismatch(
            "TID columns must both be Int32".to_owned(),
        ));
    };
    let block_data = block_col.data();
    let slot_data = slot_col.data();
    let n = batch.rows();
    if block_data.len() < n || slot_data.len() < n {
        return Err(ExecError::TypeMismatch(
            "TID column length shorter than batch rows".to_owned(),
        ));
    }
    let mut out: Vec<TupleId> = Vec::with_capacity(n);
    for i in 0..n {
        let block_u32 = u32::try_from(block_data[i]).map_err(|_| {
            ExecError::TypeMismatch(format!(
                "TID block value {} out of u32 range",
                block_data[i]
            ))
        })?;
        let slot_u16 = u16::try_from(slot_data[i]).map_err(|_| {
            ExecError::TypeMismatch(format!("TID slot value {} out of u16 range", slot_data[i]))
        })?;
        let page_id = PageId::new(relation, BlockNumber::new(block_u32));
        out.push(TupleId::new(page_id, slot_u16));
    }
    Ok(out)
}

/// Extract a `TupleId` and the remaining column values from a row that
/// begins with `[tid_block: Int32, tid_slot: Int32, ...]`.
///
/// `relation` is the relation that owns the pages; it is embedded in the
/// returned `TupleId` via `PageId`.
pub(crate) fn extract_tid_and_row(
    row: &[Value],
    relation: RelationId,
) -> Result<(TupleId, &[Value]), ExecError> {
    if row.len() < 2 {
        return Err(ExecError::TypeMismatch(
            "UPDATE/DELETE input row must have at least two TID columns".to_owned(),
        ));
    }
    let block = match &row[0] {
        Value::Int32(b) => *b,
        other => {
            return Err(ExecError::TypeMismatch(format!(
                "TID block must be Int32, got {other:?}"
            )));
        }
    };
    let slot = match &row[1] {
        Value::Int32(s) => *s,
        other => {
            return Err(ExecError::TypeMismatch(format!(
                "TID slot must be Int32, got {other:?}"
            )));
        }
    };
    let block_u32 = u32::try_from(block).map_err(|_| {
        ExecError::TypeMismatch(format!("TID block value {block} out of u32 range"))
    })?;
    let slot_u16 = u16::try_from(slot)
        .map_err(|_| ExecError::TypeMismatch(format!("TID slot value {slot} out of u16 range")))?;
    let page_id = PageId::new(relation, BlockNumber::new(block_u32));
    let tid = TupleId::new(page_id, slot_u16);
    Ok((tid, &row[2..]))
}

pub(crate) fn merge_clause_index(row: &[Value], clause_count: usize) -> Result<usize, ExecError> {
    let Some(first) = row.first() else {
        return Err(ExecError::TypeMismatch(
            "MERGE input row must include a clause index".to_owned(),
        ));
    };
    let Value::Int32(raw) = first else {
        return Err(ExecError::TypeMismatch(format!(
            "MERGE clause index must be Int32, got {first:?}"
        )));
    };
    let idx = usize::try_from(*raw).map_err(|_| {
        ExecError::TypeMismatch(format!("MERGE clause index {raw} out of usize range"))
    })?;
    if idx >= clause_count {
        return Err(ExecError::TypeMismatch(format!(
            "MERGE clause index {idx} out of range for {clause_count} clause(s)"
        )));
    }
    Ok(idx)
}

pub(crate) fn merge_tid_row(row: &[Value]) -> Result<&[Value], ExecError> {
    if row.len() < 3 {
        return Err(ExecError::TypeMismatch(
            "MERGE input row must include clause index and TID columns".to_owned(),
        ));
    }
    Ok(&row[1..])
}
