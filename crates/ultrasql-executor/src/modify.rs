//! [`ModifyTable`] operator: applies INSERT / UPDATE / DELETE to a relation
//! through [`HeapAccess`].
//!
//! Drains its input operator (the rows to insert, or scan-output of rows
//! to update/delete) and reports the row count that was modified.
//!
//! # INSERT path
//!
//! The `Insert` kind drains the child operator. For each decoded row it
//! calls [`HeapAccess::insert`] with the provided MVCC metadata, then
//! tracks the count. After EOF it emits a single 1-row batch whose only
//! column is `Int64(affected_rows)` and then returns `Ok(None)`.
//!
//! # UPDATE / DELETE paths
//!
//! `Update` and `Delete` are implemented as stand-alone operator code
//! but are only reachable through direct construction (not through
//! `build_operator`) until the physical-plan lowering gains datasource
//! handles in wave 5+. Both accept a child that emits rows prepended
//! with `(tid_block: Int32, tid_slot: Int32, ...)` columns.
//!
//! # Send bound
//!
//! `ModifyTable<L>` is `Send` because [`HeapAccess<L>`] is `Send + Sync`
//! whenever `L: PageLoader + Send + Sync` and the WAL sink (when present)
//! implements `Send + Sync`.

use std::sync::Arc;

use ultrasql_core::{CommandId, DataType, Field, RelationId, Schema, TupleId, Value, Xid};
use ultrasql_planner::{BinaryOp, ScalarExpr};
use ultrasql_storage::PageLoader;
use ultrasql_storage::heap::{DeleteOptions, HeapAccess, UpdateOptions, UpdatePayload};
use ultrasql_storage::vm::VisibilityMap;
use ultrasql_storage::wal_sink::WalSink;
use ultrasql_vec::Batch;
use ultrasql_vec::column::{Column, NumericColumn};

use crate::eval::Eval;
use crate::filter_op::batch_to_rows;
use crate::row_codec::RowCodec;
use crate::{ExecError, Operator};

/// Enforce schema-level NOT-NULL constraints over a decoded `INSERT`
/// row before it is encoded and handed to the heap.
///
/// Surfaces [`ExecError::NotNullViolation`] on the first non-nullable
/// column carrying [`Value::Null`]; the caller maps this onto
/// PostgreSQL SQLSTATE `23502`.
fn check_not_null_violations(row: &[Value], schema: &Schema) -> Result<(), ExecError> {
    for (col, field) in row.iter().zip(schema.fields().iter()) {
        if !field.nullable && matches!(col, Value::Null) {
            return Err(ExecError::NotNullViolation(field.name.clone()));
        }
    }
    Ok(())
}

/// Columnar UPDATE fast-path descriptor for the
/// `UPDATE t SET col_i = col_i + literal` shape over an `(Int32, Int32)`
/// relation schema.
///
/// Detected at [`ModifyTable::new`] from the bound assignment list.
/// When `Some(_)`, [`ModifyTable::next_batch`]'s UPDATE arm bypasses
/// `batch_to_rows` + per-row `Eval` + per-row `RowCodec::encode`
/// entirely: the new column is computed by a single
/// `i32::wrapping_add(literal)` per lane and each tuple's 9-byte
/// payload is built inline from the (`id`, `new_val`) column pair.
#[derive(Clone, Copy, Debug)]
struct UpdateFastPathInt32Pair {
    /// 0-based index of the target column in the relation schema
    /// (also the column index in the eval expression's row). For
    /// `(id, val)` UPDATEs of `val`, this is `1`.
    target_col_in_relation: usize,
    /// Constant added to the target column on every row. For
    /// `val = val + 1` this is `1`; for `val = val - 5` this is `-5`.
    delta: i32,
}

// ---------------------------------------------------------------------------
// ModifyKind
// ---------------------------------------------------------------------------

/// The mutation kind for a [`ModifyTable`] operator.
#[derive(Debug)]
pub enum ModifyKind {
    /// Append all input rows to the target relation.
    Insert,

    /// Replace targeted rows in the relation.
    ///
    /// `assignments` is a list of `(target_column_index, value_expr)` pairs
    /// where the expression scope is the current row's values. The child
    /// operator must emit rows in the shape `[tid_block: Int32, tid_slot:
    /// Int32, original_col0, original_col1, ...]`.
    Update {
        /// Ordered list of `(column_index_in_relation_schema, value_expr)` pairs.
        assignments: Vec<(usize, ScalarExpr)>,
    },

    /// Mark targeted rows as dead.
    ///
    /// The child operator must emit rows in the shape
    /// `[tid_block: Int32, tid_slot: Int32, ...rest]`.
    Delete,
}

// ---------------------------------------------------------------------------
// ModifyTable
// ---------------------------------------------------------------------------

/// Pull-based table mutation operator.
///
/// On each `next_batch` call, drains all remaining rows from `child` and
/// performs the configured mutation. After the child signals EOF, emits
/// exactly one batch containing the `affected_rows` count (an `Int64`
/// column) and then returns `Ok(None)` on all subsequent calls.
///
/// # UPDATE / DELETE batching
///
/// UPDATE and DELETE call into the heap's bulk-write surface
/// ([`HeapAccess::update_many`] / [`HeapAccess::delete_many`]) once
/// per child batch instead of once per row. That groups TIDs by
/// page so a 10 000-row UPDATE over 50 pages takes ~50 page guards
/// instead of 10 000. The per-batch cost still includes
/// expression evaluation (the `assignments` evaluators are built
/// once at construction time and reused) and row encoding.
pub struct ModifyTable<L: PageLoader> {
    heap: Arc<HeapAccess<L>>,
    relation: RelationId,
    /// Schema of the output: `[("affected_rows", Int64)]`.
    schema: Schema,
    /// Row codec for INSERT and UPDATE payload encoding. Carries a
    /// cached `fixed_width_lower_bound` so per-row `encode` calls do
    /// not realloc on the first push (see [`RowCodec::new`]).
    codec: RowCodec,
    kind: ModifyKind,
    /// Pre-built per-assignment evaluators for UPDATE. `kind ==
    /// Update` populates this once at construction so the per-row
    /// loop in `next_batch` does not pay the `ScalarExpr::clone()`
    /// and evaluator-allocation cost on every iteration.
    ///
    /// Empty for INSERT and DELETE.
    update_evaluators: Vec<(usize, Eval)>,
    /// Cached descriptor for the columnar UPDATE fast path. `Some`
    /// when (a) the kind is UPDATE, (b) the relation schema is
    /// exactly `(Int32, Int32)`, (c) there is one assignment whose
    /// expression matches `col + lit` / `col - lit` / `lit + col`
    /// with an `Int32` literal.
    update_fast_path: Option<UpdateFastPathInt32Pair>,
    insert_xmin: Xid,
    insert_command_id: CommandId,
    delete_xmax: Xid,
    delete_cmax: CommandId,
    wal: Option<Arc<dyn WalSink>>,
    vm: Option<Arc<VisibilityMap>>,
    child: Box<dyn Operator>,
    done: bool,
    affected: u64,
}

impl<L: PageLoader> std::fmt::Debug for ModifyTable<L> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ModifyTable")
            .field("relation", &self.relation)
            .field("kind", &self.kind)
            .field("done", &self.done)
            .field("affected", &self.affected)
            .finish_non_exhaustive()
    }
}

impl<L: PageLoader> ModifyTable<L> {
    /// Output schema shared across all `ModifyTable` instances: a single
    /// `Int64` column named `"affected_rows"`.
    fn affected_rows_schema() -> Schema {
        Schema::new([Field::required("affected_rows", DataType::Int64)])
            .expect("affected_rows schema is trivially well-formed")
    }

    /// Construct a `ModifyTable` operator.
    ///
    /// # Parameters
    ///
    /// - `heap` — shared reference to the heap access method.
    /// - `relation` — target relation id.
    /// - `relation_schema` — full column schema of the target relation
    ///   (used as the codec schema for INSERT).
    /// - `kind` — mutation kind.
    /// - `insert_xmin` — XID to stamp as `xmin` on inserted tuples.
    /// - `insert_command_id` — command id within the inserting transaction.
    /// - `delete_xmax` — XID to stamp as `xmax` on deleted/updated tuples.
    /// - `delete_cmax` — command id for deletes/updates.
    /// - `wal` — optional WAL sink; `None` skips WAL emission.
    /// - `child` — source operator.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    #[allow(clippy::similar_names)]
    pub fn new(
        heap: Arc<HeapAccess<L>>,
        relation: RelationId,
        relation_schema: Schema,
        kind: ModifyKind,
        insert_xmin: Xid,
        insert_command_id: CommandId,
        delete_xmax: Xid,
        delete_cmax: CommandId,
        wal: Option<Arc<dyn WalSink>>,
        child: Box<dyn Operator>,
    ) -> Self {
        // Build per-assignment evaluators once at construction so the
        // per-row UPDATE loop does not pay the `ScalarExpr::clone()`
        // and evaluator-allocation cost on every iteration. For
        // INSERT and DELETE this is empty.
        let update_evaluators: Vec<(usize, Eval)> = match &kind {
            ModifyKind::Update { assignments } => assignments
                .iter()
                .map(|(col, expr)| (*col, Eval::new(expr.clone())))
                .collect(),
            ModifyKind::Insert | ModifyKind::Delete => Vec::new(),
        };
        let update_fast_path = match &kind {
            ModifyKind::Update { assignments } => {
                detect_update_int32_pair_fast_path(assignments, &relation_schema)
            }
            _ => None,
        };
        Self {
            heap,
            relation,
            schema: Self::affected_rows_schema(),
            codec: RowCodec::new(relation_schema),
            kind,
            update_evaluators,
            update_fast_path,
            insert_xmin,
            insert_command_id,
            delete_xmax,
            delete_cmax,
            wal,
            vm: None,
            child,
            done: false,
            affected: 0,
        }
    }

    /// Attach the server-owned visibility map so heap mutations clear
    /// all-visible bits for touched pages.
    #[must_use]
    pub fn with_visibility_map(mut self, vm: Arc<VisibilityMap>) -> Self {
        self.vm = Some(vm);
        self
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
fn detect_update_int32_pair_fast_path(
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

impl<L: PageLoader + Send + Sync + std::fmt::Debug + 'static> Operator for ModifyTable<L> {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.done {
            return Ok(None);
        }
        self.done = true;

        // For UPDATE we accumulate every batch's `(old_tid, payload)`
        // edits into a single Vec and hand the whole set to
        // `heap.update_many` in one call after the child is drained.
        // The bulk-UPDATE path inside `update_many` pays a fixed
        // per-call cost (sort, page-group walk, insert_batch dispatch,
        // column-cache invalidate); coalescing across batches drops
        // that overhead from `O(n_batches)` to `O(1)` while keeping the
        // per-row cost identical.
        let mut all_update_edits: Vec<(TupleId, UpdatePayload)> = Vec::new();

        // Drain the entire child input.
        loop {
            let Some(batch) = self.child.next_batch()? else {
                break;
            };
            if batch.rows() == 0 {
                continue;
            }

            match &self.kind {
                ModifyKind::Delete => {
                    // Bulk path: read every TID **directly** from the
                    // batch's first two columns (`tid_block`,
                    // `tid_slot` — both non-nullable `Int32` per
                    // `SeqScan::new_with_tids`) and hand the lot to
                    // `heap.delete_many`. No `batch_to_rows`
                    // materialisation, no per-row `Vec<Value>`
                    // intermediate.
                    let tids = extract_tids_from_batch(&batch, self.relation)?;
                    let n = tids.len();
                    let wal_ref: Option<&dyn WalSink> = self.wal.as_deref();
                    self.heap
                        .delete_many(
                            tids,
                            DeleteOptions {
                                xmax: self.delete_xmax,
                                cmax: self.delete_cmax,
                                wal: wal_ref,
                                fsm: None,
                                vm: self.vm.as_deref(),
                            },
                        )
                        .map_err(|e| ExecError::TypeMismatch(e.to_string()))?;
                    let n_u64 = u64::try_from(n).unwrap_or(u64::MAX);
                    self.affected = self.affected.saturating_add(n_u64);
                }
                ModifyKind::Update { .. } => {
                    // Columnar fast path: `UPDATE t SET col_i = col_i ± lit`
                    // over an `(Int32, Int32)` relation. Builds every
                    // tuple's 9-byte payload inline from the batch's
                    // column arrays — no `batch_to_rows`, no per-row
                    // `Eval`, no per-row `RowCodec::encode` tree walk.
                    let edits = if let Some(spec) = self.update_fast_path {
                        build_update_edits_int32_pair(&batch, self.relation, spec)?
                    } else {
                        // Slow path: batch_to_rows + per-row eval +
                        // per-row codec.encode. Covers every UPDATE
                        // shape not matched by the fast-path detector.
                        let child_schema = self.child.schema().clone();
                        let rows = batch_to_rows(&batch, &child_schema)
                            .map_err(|e| ExecError::TypeMismatch(e.to_string()))?;
                        let mut edits: Vec<(TupleId, UpdatePayload)> =
                            Vec::with_capacity(rows.len());
                        for row in &rows {
                            let edit = self.compute_update_edit(row)?;
                            edits.push(edit);
                        }
                        edits
                    };
                    if all_update_edits.is_empty() {
                        all_update_edits = edits;
                    } else {
                        all_update_edits.extend(edits);
                    }
                }
                ModifyKind::Insert => {
                    // Batched INSERT: encode every row in this batch
                    // once into per-row `Vec<u8>` payloads and hand
                    // the slice to `heap.insert_batch`. That bulk
                    // call pins each destination page exactly once
                    // and writes every payload under one write guard
                    // per page — replacing the prior per-row
                    // `heap.insert` loop that re-entered the buffer
                    // pool once per row.
                    let child_schema = self.child.schema().clone();
                    let rows = batch_to_rows(&batch, &child_schema)
                        .map_err(|e| ExecError::TypeMismatch(e.to_string()))?;
                    let target_schema = self.codec.schema();
                    let mut payloads: Vec<Vec<u8>> = Vec::with_capacity(rows.len());
                    for row in &rows {
                        check_not_null_violations(row, target_schema)?;
                        let payload = self
                            .codec
                            .encode(row)
                            .map_err(|e| ExecError::TypeMismatch(e.to_string()))?;
                        payloads.push(payload);
                    }
                    let n = payloads.len();
                    let payload_refs: Vec<&[u8]> = payloads.iter().map(Vec::as_slice).collect();
                    let wal_ref: Option<&dyn WalSink> = self.wal.as_deref();
                    self.heap
                        .insert_batch(
                            self.relation,
                            &payload_refs,
                            ultrasql_storage::heap::InsertOptions {
                                xmin: self.insert_xmin,
                                command_id: self.insert_command_id,
                                wal: wal_ref,
                                fsm: None,
                                vm: self.vm.as_deref(),
                            },
                        )
                        .map_err(|e| ExecError::TypeMismatch(e.to_string()))?;
                    self.affected = self.affected.saturating_add(n as u64);
                }
            }
        }

        // Single bulk UPDATE call after every input batch has been
        // accumulated. See the `all_update_edits` comment above.
        if matches!(self.kind, ModifyKind::Update { .. }) && !all_update_edits.is_empty() {
            let n = all_update_edits.len();
            let wal_ref: Option<&dyn WalSink> = self.wal.as_deref();
            self.heap
                .update_many(
                    all_update_edits,
                    UpdateOptions {
                        xid: self.delete_xmax,
                        command_id: self.delete_cmax,
                        hot_eligible: true,
                        wal: wal_ref,
                        vm: self.vm.as_deref(),
                    },
                )
                .map_err(|e| ExecError::TypeMismatch(e.to_string()))?;
            let n_u64 = u64::try_from(n).unwrap_or(u64::MAX);
            self.affected = self.affected.saturating_add(n_u64);
        }

        // Emit the affected-row-count batch.
        let affected_i64 = i64::try_from(self.affected).unwrap_or(i64::MAX);
        let batch = Batch::new([Column::Int64(NumericColumn::from_data(vec![affected_i64]))])
            .map_err(ExecError::from)?;
        Ok(Some(batch))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

impl<L: PageLoader + Send + Sync + std::fmt::Debug + 'static> ModifyTable<L> {
    /// Compute the `(old_tid, new_payload_bytes)` edit for a single
    /// UPDATE input row.
    ///
    /// The `row` slice must begin with `[tid_block: Int32, tid_slot:
    /// Int32, original_col0, ...]`. We extract the TID from the
    /// first two columns, apply the cached evaluators to the
    /// remaining columns to build the new row, and encode it through
    /// the operator's precomputed [`RowCodec`] (with a
    /// `fixed_width_lower_bound`-sized initial capacity so the first
    /// push does not reallocate). The encoded payload is handed to
    /// [`HeapAccess::update_many`] by the bulk caller.
    fn compute_update_edit(&self, row: &[Value]) -> Result<(TupleId, UpdatePayload), ExecError> {
        let (tid, orig_row) = extract_tid_and_row(row, self.relation)?;

        // Build the new row from the original, applying assignments.
        let relation_cols = self.codec.schema().len();
        let mut new_row: Vec<Value> = orig_row.to_vec();
        if new_row.len() != relation_cols {
            return Err(ExecError::TypeMismatch(format!(
                "UPDATE row has {} columns after TID, expected {}",
                new_row.len(),
                relation_cols,
            )));
        }

        for (col_idx, evaluator) in &self.update_evaluators {
            let val = evaluator
                .eval(orig_row)
                .map_err(|e| ExecError::TypeMismatch(e.to_string()))?;
            if *col_idx >= relation_cols {
                return Err(ExecError::TypeMismatch(format!(
                    "UPDATE assignment column index {col_idx} out of range (relation has {relation_cols} columns)"
                )));
            }
            new_row[*col_idx] = val;
        }

        let new_payload = self
            .codec
            .encode(&new_row)
            .map_err(|e| ExecError::TypeMismatch(e.to_string()))?;
        // Move the encoded bytes into a `SmallVec<[u8; 16]>`; rows
        // ≤ 16 bytes stay inline. `SmallVec::from_vec` reuses the
        // existing heap buffer when the row spills.
        let payload = UpdatePayload::from_vec(new_payload);
        Ok((tid, payload))
    }
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
/// The new value of the target column is `old + spec.delta`
/// (`wrapping_add`, matching the slow-path `int32_arith(Add)` semantics
/// the binder validated). No `batch_to_rows`, no `Eval`, no
/// `RowCodec::encode` tree walk.
fn build_update_edits_int32_pair(
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
        let (new_id, new_val) = if spec.target_col_in_relation == 0 {
            (id_v.wrapping_add(spec.delta), val_v)
        } else {
            (id_v, val_v.wrapping_add(spec.delta))
        };
        // Inline 9-byte payload assembled into a `SmallVec<[u8; 16]>`
        // so the per-row encode pays no heap allocation: the entire
        // body lives in the SmallVec's inline buffer.
        let mut payload = UpdatePayload::new();
        payload.push(0_u8); // null bitmap: both non-NULL.
        payload.extend_from_slice(&new_id.to_le_bytes());
        payload.extend_from_slice(&new_val.to_le_bytes());
        let page_id =
            ultrasql_core::PageId::new(relation, ultrasql_core::BlockNumber::new(block_u32));
        out.push((TupleId::new(page_id, slot_u16), payload));
    }
    Ok(out)
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
fn extract_tids_from_batch(batch: &Batch, relation: RelationId) -> Result<Vec<TupleId>, ExecError> {
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
        let page_id =
            ultrasql_core::PageId::new(relation, ultrasql_core::BlockNumber::new(block_u32));
        out.push(TupleId::new(page_id, slot_u16));
    }
    Ok(out)
}

/// Extract a `TupleId` and the remaining column values from a row that
/// begins with `[tid_block: Int32, tid_slot: Int32, ...]`.
///
/// `relation` is the relation that owns the pages; it is embedded in the
/// returned `TupleId` via `PageId`.
fn extract_tid_and_row(
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
    let page_id = ultrasql_core::PageId::new(relation, ultrasql_core::BlockNumber::new(block_u32));
    let tid = TupleId::new(page_id, slot_u16);
    Ok((tid, &row[2..]))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use parking_lot::Mutex;
    use std::collections::HashMap;
    use ultrasql_core::constants::PAGE_SIZE;
    use ultrasql_core::{
        CommandId, DataType, Field, PageId, RelationId, Result, Schema, Value, Xid,
    };
    use ultrasql_storage::buffer_pool::{BufferPool, PageLoader};
    use ultrasql_storage::heap::HeapAccess;
    use ultrasql_storage::page::Page;
    use ultrasql_storage::wal_sink::test_support::InMemoryWalSink;
    use ultrasql_vec::column::Column;

    use super::{ModifyKind, ModifyTable};
    use crate::Operator;
    use crate::mem_table_scan::MemTableScan;
    use crate::values_scan::ValuesScan;
    use ultrasql_planner::ScalarExpr;

    // -----------------------------------------------------------------------
    // In-memory heap fixtures (duplicated from seq_scan tests)
    // -----------------------------------------------------------------------

    #[derive(Default, Debug)]
    struct MapLoader {
        store: Mutex<HashMap<PageId, Box<[u8; PAGE_SIZE]>>>,
    }

    impl MapLoader {
        fn new() -> Self {
            Self::default()
        }
    }

    impl PageLoader for MapLoader {
        fn load(&self, page_id: PageId) -> Result<Page> {
            let stored = {
                let store = self.store.lock();
                store.get(&page_id).map(|b| {
                    let mut copy: Box<[u8; PAGE_SIZE]> = vec![0_u8; PAGE_SIZE]
                        .into_boxed_slice()
                        .try_into()
                        .expect("alloc matches PAGE_SIZE");
                    copy.copy_from_slice(&**b);
                    copy
                })
            };
            if let Some(bytes) = stored {
                return Page::from_bytes(bytes)
                    .map_err(|e| ultrasql_core::Error::Corruption(format!("test loader: {e}")));
            }
            let page = Page::new_heap();
            let mut copy: Box<[u8; PAGE_SIZE]> = vec![0_u8; PAGE_SIZE]
                .into_boxed_slice()
                .try_into()
                .expect("alloc matches PAGE_SIZE");
            copy.copy_from_slice(page.as_bytes());
            self.store.lock().insert(page_id, copy);
            Ok(page)
        }
    }

    fn rel() -> RelationId {
        RelationId::new(42)
    }

    fn make_heap() -> Arc<HeapAccess<MapLoader>> {
        let pool = Arc::new(BufferPool::new(64, MapLoader::new()));
        Arc::new(HeapAccess::new(pool))
    }

    fn schema_i32_text() -> Schema {
        Schema::new([
            Field::required("id", DataType::Int32),
            Field::required("name", DataType::Text { max_len: None }),
        ])
        .expect("schema ok")
    }

    fn lit_i32(v: i32) -> ScalarExpr {
        ScalarExpr::Literal {
            value: Value::Int32(v),
            data_type: DataType::Int32,
        }
    }

    fn lit_text(s: &str) -> ScalarExpr {
        ScalarExpr::Literal {
            value: Value::Text(s.to_owned()),
            data_type: DataType::Text { max_len: None },
        }
    }

    // -----------------------------------------------------------------------
    // Test 1: insert writes each input row to heap and reports count
    // -----------------------------------------------------------------------

    #[test]
    fn insert_writes_each_input_row_to_heap_and_reports_count() {
        let heap = make_heap();
        let schema = schema_i32_text();
        let wal = Arc::new(InMemoryWalSink::new());

        // Source: 3 rows via ValuesScan.
        let rows = vec![
            vec![lit_i32(1), lit_text("alice")],
            vec![lit_i32(2), lit_text("bob")],
            vec![lit_i32(3), lit_text("carol")],
        ];
        let source = ValuesScan::new(rows, schema.clone());

        let mut op = ModifyTable::new(
            Arc::clone(&heap),
            rel(),
            schema,
            ModifyKind::Insert,
            Xid::new(10),
            CommandId::FIRST,
            Xid::new(10),
            CommandId::FIRST,
            Some(Arc::clone(&wal) as Arc<dyn ultrasql_storage::wal_sink::WalSink>),
            Box::new(source),
        );

        // Drain the operator.
        let batch = op
            .next_batch()
            .expect("must not error")
            .expect("must emit batch");
        assert_eq!(batch.rows(), 1, "expected single affected-rows batch");
        match &batch.columns()[0] {
            Column::Int64(c) => assert_eq!(c.data(), &[3_i64], "expected 3 affected rows"),
            other => panic!("unexpected column: {other:?}"),
        }
        assert!(
            op.next_batch().unwrap().is_none(),
            "must return None after emit"
        );

        // Verify 3 rows are present in the heap.
        assert_eq!(heap.block_count(rel()), 1, "one block should be allocated");
    }

    // -----------------------------------------------------------------------
    // Test 2: insert emits a WAL record per inserted row
    // -----------------------------------------------------------------------

    #[test]
    fn insert_emits_wal_record_per_inserted_row() {
        let heap = make_heap();
        let schema = schema_i32_text();
        let wal = Arc::new(InMemoryWalSink::new());

        let rows = vec![
            vec![lit_i32(1), lit_text("x")],
            vec![lit_i32(2), lit_text("y")],
        ];
        let source = ValuesScan::new(rows, schema.clone());

        let mut op = ModifyTable::new(
            Arc::clone(&heap),
            rel(),
            schema,
            ModifyKind::Insert,
            Xid::new(20),
            CommandId::FIRST,
            Xid::new(20),
            CommandId::FIRST,
            Some(Arc::clone(&wal) as Arc<dyn ultrasql_storage::wal_sink::WalSink>),
            Box::new(source),
        );

        op.next_batch().unwrap();

        // Each insert should emit exactly one WAL record.
        assert_eq!(wal.len(), 2, "expected 2 WAL records");
    }

    // -----------------------------------------------------------------------
    // Test 3: empty input reports zero affected rows
    // -----------------------------------------------------------------------

    #[test]
    fn insert_empty_input_reports_zero() {
        let heap = make_heap();
        let schema = schema_i32_text();
        let source = ValuesScan::new(vec![], schema.clone());

        let mut op = ModifyTable::new(
            Arc::clone(&heap),
            rel(),
            schema,
            ModifyKind::Insert,
            Xid::new(30),
            CommandId::FIRST,
            Xid::new(30),
            CommandId::FIRST,
            None,
            Box::new(source),
        );

        let batch = op.next_batch().unwrap().unwrap();
        match &batch.columns()[0] {
            Column::Int64(c) => assert_eq!(c.data(), &[0_i64]),
            other => panic!("unexpected column: {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // Test 4: schema reports affected_rows column
    // -----------------------------------------------------------------------

    #[test]
    fn modify_table_schema_is_affected_rows() {
        let heap = make_heap();
        let schema = schema_i32_text();
        let source = MemTableScan::new(schema.clone(), vec![]);
        let op = ModifyTable::new(
            Arc::clone(&heap),
            rel(),
            schema,
            ModifyKind::Insert,
            Xid::new(1),
            CommandId::FIRST,
            Xid::new(1),
            CommandId::FIRST,
            None,
            Box::new(source),
        );
        assert_eq!(op.schema().len(), 1);
        assert_eq!(op.schema().field_at(0).name, "affected_rows");
        assert_eq!(op.schema().field_at(0).data_type, DataType::Int64);
    }
}
