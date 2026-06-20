//! The [`Operator`] implementation for [`ModifyTable`]: `next_batch`
//! drains the child, dispatches per [`ModifyKind`], coalesces bulk heap
//! writes, and emits either the `RETURNING` projection or the
//! affected-row count.

use std::collections::HashSet;

use ultrasql_core::{Schema, TupleId, Value};
use ultrasql_storage::PageLoader;
use ultrasql_storage::heap::{DeleteOptions, UpdateOptions, UpdatePayload};
use ultrasql_storage::wal_sink::WalSink;
use ultrasql_vec::Batch;
use ultrasql_vec::column::{Column, NumericColumn};

use super::helpers::{
    build_update_edits_int32_pair, check_not_null_violations, extract_tids_from_batch,
    merge_clause_index, merge_tid_row, row_codec_error_to_exec,
};
use super::{
    DeleteIndexChange, InsertConflict, InsertConflictAction, MergeAction, ModifyKind, ModifyTable,
    UpdateIndexChange, VectorDeleteIndexChange, VectorUpdateIndexChange,
};
use crate::filter_op::batch_to_rows;
use crate::seq_scan::build_batch;
use crate::{ExecError, Operator};

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
        let mut all_update_index_changes: Vec<UpdateIndexChange> = Vec::new();
        let mut all_update_vector_index_changes: Vec<VectorUpdateIndexChange> = Vec::new();
        let mut all_delete_tids: Vec<TupleId> = Vec::new();
        let mut all_delete_index_changes: Vec<DeleteIndexChange> = Vec::new();
        let mut all_delete_vector_index_changes: Vec<VectorDeleteIndexChange> = Vec::new();
        let mut all_insert_payloads: Vec<Vec<u8>> = Vec::new();
        let mut all_insert_index_keys: Vec<Vec<Option<i64>>> =
            self.insert_indexes.iter().map(|_| Vec::new()).collect();
        let mut all_insert_vector_index_keys: Vec<Vec<Option<Vec<f32>>>> = self
            .insert_vector_indexes
            .iter()
            .map(|_| Vec::new())
            .collect();
        let mut merge_seen_insert_keys: Vec<HashSet<i64>> =
            self.insert_indexes.iter().map(|_| HashSet::new()).collect();
        let mut returning_rows: Vec<Vec<Value>> = Vec::new();
        let returning_active = !self.returning_evaluators.is_empty();

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
                    let (tids, delete_index_changes, delete_vector_index_changes, deleted_rows) =
                        if !returning_active
                            && self.delete_indexes.is_empty()
                            && self.delete_vector_indexes.is_empty()
                            && self.referenced_by_delete_checks.is_empty()
                        {
                            (
                                extract_tids_from_batch(&batch, self.relation)?,
                                Vec::new(),
                                Vec::new(),
                                Vec::new(),
                            )
                        } else {
                            self.extract_delete_tids_and_index_changes(&batch, returning_active)?
                        };
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
                    self.apply_delete_index_changes(&delete_index_changes)?;
                    self.apply_delete_vector_index_changes(&delete_vector_index_changes)?;
                    if returning_active {
                        for row in deleted_rows {
                            returning_rows.push(self.evaluate_returning_row(&row)?);
                        }
                    }
                    self.add_affected_rows(n)?;
                }
                ModifyKind::Update { .. } => {
                    // Columnar fast path: `UPDATE t SET col_i = col_i ± lit`
                    // over an `(Int32, Int32)` relation. Builds every
                    // tuple's 9-byte payload inline from the batch's
                    // column arrays — no `batch_to_rows`, no per-row
                    // `Eval`, no per-row `RowCodec::encode` tree walk.
                    let edits = if let Some(spec) = self.update_fast_path.filter(|_| {
                        !returning_active
                            && self.check_constraints.is_empty()
                            && self.foreign_key_checks.is_empty()
                            && self.exclusion_update_checks.is_empty()
                            && self.referenced_by_update_checks.is_empty()
                            && self.update_indexes.is_empty()
                            && self.update_vector_indexes.is_empty()
                    }) {
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
                            let computed = self.compute_update_edit(row, returning_active)?;
                            if let Some(index_change) = computed.index_change {
                                all_update_index_changes.push(index_change);
                            }
                            if let Some(index_change) = computed.vector_index_change {
                                all_update_vector_index_changes.push(index_change);
                            }
                            if let Some(returning_row) = computed.returning_row {
                                returning_rows.push(self.evaluate_returning_row(&returning_row)?);
                            }
                            edits.push((computed.tid, computed.payload));
                        }
                        edits
                    };
                    if all_update_edits.is_empty() {
                        all_update_edits = edits;
                    } else {
                        all_update_edits.extend(edits);
                    }
                }
                ModifyKind::Merge { clauses } => {
                    let child_schema = self.child.schema().clone();
                    let rows = batch_to_rows(&batch, &child_schema)
                        .map_err(|e| ExecError::TypeMismatch(e.to_string()))?;
                    for row in &rows {
                        let clause_idx = merge_clause_index(row, clauses.len())?;
                        let clause = &clauses[clause_idx];
                        match &clause.action {
                            MergeAction::Update { assignments } => {
                                let computed = self.compute_update_edit_with_evaluators(
                                    merge_tid_row(row)?,
                                    assignments,
                                    false,
                                )?;
                                if let Some(index_change) = computed.index_change {
                                    all_update_index_changes.push(index_change);
                                }
                                if let Some(index_change) = computed.vector_index_change {
                                    all_update_vector_index_changes.push(index_change);
                                }
                                all_update_edits.push((computed.tid, computed.payload));
                            }
                            MergeAction::Delete => {
                                let deleted =
                                    self.compute_delete_change_from_row(merge_tid_row(row)?)?;
                                all_delete_tids.push(deleted.tid);
                                if let Some(index_change) = deleted.index_change {
                                    all_delete_index_changes.push(index_change);
                                }
                                if let Some(index_change) = deleted.vector_index_change {
                                    all_delete_vector_index_changes.push(index_change);
                                }
                            }
                            MergeAction::Insert { columns, values } => {
                                let prepared = self.prepare_merge_insert_row(
                                    row,
                                    columns,
                                    values,
                                    &mut merge_seen_insert_keys,
                                )?;
                                for (idx, key) in prepared.index_keys.into_iter().enumerate() {
                                    let Some(keys) = all_insert_index_keys.get_mut(idx) else {
                                        return Err(ExecError::Internal(
                                            "merge insert index key vector width mismatch",
                                        ));
                                    };
                                    keys.push(key);
                                }
                                for (idx, key) in prepared.vector_index_keys.into_iter().enumerate()
                                {
                                    let Some(keys) = all_insert_vector_index_keys.get_mut(idx)
                                    else {
                                        return Err(ExecError::Internal(
                                            "merge insert vector index key vector width mismatch",
                                        ));
                                    };
                                    keys.push(key);
                                }
                                all_insert_payloads.push(prepared.payload);
                            }
                        }
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
                    let mut index_keys: Vec<Vec<Option<i64>>> = self
                        .insert_indexes
                        .iter()
                        .map(|_| Vec::with_capacity(rows.len()))
                        .collect();
                    let mut vector_index_keys: Vec<Vec<Option<Vec<f32>>>> = self
                        .insert_vector_indexes
                        .iter()
                        .map(|_| Vec::with_capacity(rows.len()))
                        .collect();
                    let mut seen_keys: Vec<HashSet<i64>> =
                        self.insert_indexes.iter().map(|_| HashSet::new()).collect();
                    let conflict_action = self.insert_conflict_action.clone();
                    self.validate_insert_conflict_arbiter(conflict_action.as_ref())?;
                    for row in &rows {
                        let mut expanded_row;
                        let omitted;
                        let target_row = if self.insert_column_map.is_some()
                            || !self.column_defaults.is_empty()
                            || !self.sequence_defaults.is_empty()
                            || !self.identity_always.is_empty()
                            || !self.generated_stored.is_empty()
                            || !self.check_constraints.is_empty()
                            || !self.exclusion_checks.is_empty()
                        {
                            if let Some(column_map) = &self.insert_column_map {
                                let expanded = super::helpers::expand_insert_row(
                                    row,
                                    target_schema.len(),
                                    column_map,
                                )?;
                                expanded_row = expanded.values;
                                omitted = expanded.omitted;
                            } else {
                                expanded_row = row.clone();
                                omitted = vec![false; target_schema.len()];
                            }
                            self.check_identity_explicit_values(&omitted)?;
                            self.apply_insert_defaults(&mut expanded_row, &omitted)?;
                            self.check_generated_stored_explicit_values(&omitted)?;
                            self.apply_generated_stored(&mut expanded_row)?;
                            self.check_row_constraints(&expanded_row)?;
                            self.check_foreign_keys(&expanded_row)?;
                            self.check_exclusions(&expanded_row)?;
                            expanded_row.as_slice()
                        } else {
                            row.as_slice()
                        };
                        if self.insert_column_map.is_none()
                            && self.column_defaults.is_empty()
                            && self.sequence_defaults.is_empty()
                            && self.identity_always.is_empty()
                            && self.generated_stored.is_empty()
                            && self.check_constraints.is_empty()
                            && self.exclusion_checks.is_empty()
                        {
                            self.check_foreign_keys(target_row)?;
                        }
                        check_not_null_violations(target_row, target_schema)?;
                        let row_index_keys = self
                            .insert_indexes
                            .iter()
                            .map(|index| index.encode_key(target_row))
                            .collect::<Result<Vec<_>, _>>()?;
                        if let Some(action) = &conflict_action {
                            if let Some(conflict) =
                                self.find_insert_conflict(action, &row_index_keys, &seen_keys)?
                            {
                                match action {
                                    InsertConflictAction::DoNothing { .. } => continue,
                                    InsertConflictAction::DoUpdate {
                                        assignments,
                                        predicate,
                                        ..
                                    } => {
                                        let InsertConflict::Existing(tid) = conflict else {
                                            return Err(ExecError::TypeMismatch(
                                                "ON CONFLICT DO UPDATE cannot affect the same row twice"
                                                    .to_owned(),
                                            ));
                                        };
                                        let (current_tid, old_row) =
                                            self.fetch_conflict_current_row(tid)?;
                                        if let Some(computed) = self.compute_conflict_update_edit(
                                            current_tid,
                                            &old_row,
                                            target_row,
                                            assignments,
                                            predicate.as_ref(),
                                            returning_active,
                                        )? {
                                            if let Some(index_change) = computed.index_change {
                                                all_update_index_changes.push(index_change);
                                            }
                                            if let Some(index_change) = computed.vector_index_change
                                            {
                                                all_update_vector_index_changes.push(index_change);
                                            }
                                            if let Some(returning_row) = computed.returning_row {
                                                returning_rows.push(
                                                    self.evaluate_returning_row(&returning_row)?,
                                                );
                                            }
                                            all_update_edits.push((computed.tid, computed.payload));
                                        }
                                        continue;
                                    }
                                }
                            }
                            self.remember_insert_keys(&row_index_keys, &mut seen_keys);
                        } else {
                            self.reject_duplicate_insert_keys(&row_index_keys, &mut seen_keys)?;
                        }
                        for (idx, key) in row_index_keys.iter().copied().enumerate() {
                            if idx >= index_keys.len() {
                                return Err(ExecError::Internal(
                                    "insert index key vector width mismatch",
                                ));
                            }
                            index_keys[idx].push(key);
                        }
                        let row_vector_index_keys = self
                            .insert_vector_indexes
                            .iter()
                            .map(|index| index.encode_key(target_row))
                            .collect::<Result<Vec<_>, _>>()?;
                        for (idx, key) in row_vector_index_keys.into_iter().enumerate() {
                            if idx >= vector_index_keys.len() {
                                return Err(ExecError::Internal(
                                    "insert vector index key vector width mismatch",
                                ));
                            }
                            vector_index_keys[idx].push(key);
                        }
                        if returning_active {
                            returning_rows.push(self.evaluate_returning_row(target_row)?);
                        }
                        let payload = self
                            .codec
                            .encode(target_row)
                            .map_err(row_codec_error_to_exec)?;
                        payloads.push(payload);
                    }
                    let n = payloads.len();
                    let payload_refs: Vec<&[u8]> = payloads.iter().map(Vec::as_slice).collect();
                    let wal_ref: Option<&dyn WalSink> = self.wal.as_deref();
                    let n_atts = u16::try_from(target_schema.len()).map_err(|_| {
                        ExecError::Internal("target schema column count exceeds u16")
                    })?;
                    let tids = self
                        .heap
                        .insert_batch(
                            self.relation,
                            &payload_refs,
                            ultrasql_storage::heap::InsertOptions {
                                xmin: self.insert_xmin,
                                command_id: self.insert_command_id,
                                n_atts,
                                wal: wal_ref,
                                fsm: None,
                                vm: self.vm.as_deref(),
                            },
                        )
                        .map_err(|e| ExecError::TypeMismatch(e.to_string()))?;
                    debug_assert_eq!(tids.len(), payloads.len());
                    for (idx, index) in self.insert_indexes.iter_mut().enumerate() {
                        for (row_idx, key) in index_keys[idx].iter().enumerate() {
                            if let Some(k) = key {
                                let Some(tid) = tids.get(row_idx).copied() else {
                                    return Err(ExecError::Internal(
                                        "heap insert_batch returned fewer TIDs than payloads",
                                    ));
                                };
                                index.insert_key(*k, tid, self.insert_xmin, wal_ref)?;
                            }
                        }
                    }
                    for (idx, index) in self.insert_vector_indexes.iter().enumerate() {
                        for (row_idx, key) in vector_index_keys[idx].iter().enumerate() {
                            if let Some(vector) = key {
                                let Some(tid) = tids.get(row_idx).copied() else {
                                    return Err(ExecError::Internal(
                                        "heap insert_batch returned fewer TIDs than payloads",
                                    ));
                                };
                                index.insert_vector(vector, tid)?;
                            }
                        }
                    }
                    self.add_affected_rows(n)?;
                }
            }
        }

        // Single bulk UPDATE call after every input batch has been
        // accumulated. See the `all_update_edits` comment above.
        if !all_update_edits.is_empty() {
            let n = all_update_edits.len();
            let wal = self.wal.clone();
            let wal_ref: Option<&dyn WalSink> = wal.as_deref();
            let index_keys_unchanged = self.update_indexes.is_empty()
                || all_update_index_changes
                    .iter()
                    .all(|change| change.old_keys == change.new_keys);
            let vector_index_keys_unchanged = self.update_vector_indexes.is_empty()
                || all_update_vector_index_changes
                    .iter()
                    .all(|change| change.old_keys == change.new_keys);
            let update_opts = UpdateOptions {
                xid: self.delete_xmax,
                command_id: self.delete_cmax,
                hot_eligible: index_keys_unchanged && vector_index_keys_unchanged,
                wal: wal_ref,
                vm: self.vm.as_deref(),
            };
            if self.update_indexes.is_empty() && self.update_vector_indexes.is_empty() {
                self.heap
                    .update_many(all_update_edits, update_opts)
                    .map_err(|e| ExecError::TypeMismatch(e.to_string()))?;
            } else {
                self.precheck_update_index_changes(&all_update_index_changes)?;
                let outcomes = self
                    .heap
                    .update_many_with_outcomes(all_update_edits, update_opts)
                    .map_err(|e| ExecError::TypeMismatch(e.to_string()))?;
                self.apply_update_index_changes(&all_update_index_changes, &outcomes, wal_ref)?;
                self.apply_update_vector_index_changes(
                    &all_update_vector_index_changes,
                    &outcomes,
                )?;
            }
            self.add_affected_rows(n)?;
        }

        if !all_delete_tids.is_empty() {
            let n = all_delete_tids.len();
            let wal_ref: Option<&dyn WalSink> = self.wal.as_deref();
            self.heap
                .delete_many(
                    all_delete_tids,
                    DeleteOptions {
                        xmax: self.delete_xmax,
                        cmax: self.delete_cmax,
                        wal: wal_ref,
                        fsm: None,
                        vm: self.vm.as_deref(),
                    },
                )
                .map_err(|e| ExecError::TypeMismatch(e.to_string()))?;
            self.apply_delete_index_changes(&all_delete_index_changes)?;
            self.apply_delete_vector_index_changes(&all_delete_vector_index_changes)?;
            self.add_affected_rows(n)?;
        }

        if !all_insert_payloads.is_empty() {
            let n = all_insert_payloads.len();
            let payload_refs: Vec<&[u8]> = all_insert_payloads.iter().map(Vec::as_slice).collect();
            let wal_ref: Option<&dyn WalSink> = self.wal.as_deref();
            let n_atts = u16::try_from(self.codec.schema().len())
                .map_err(|_| ExecError::Internal("target schema column count exceeds u16"))?;
            let tids = self
                .heap
                .insert_batch(
                    self.relation,
                    &payload_refs,
                    ultrasql_storage::heap::InsertOptions {
                        xmin: self.insert_xmin,
                        command_id: self.insert_command_id,
                        n_atts,
                        wal: wal_ref,
                        fsm: None,
                        vm: self.vm.as_deref(),
                    },
                )
                .map_err(|e| ExecError::TypeMismatch(e.to_string()))?;
            debug_assert_eq!(tids.len(), all_insert_payloads.len());
            for (idx, index) in self.insert_indexes.iter_mut().enumerate() {
                for (row_idx, key) in all_insert_index_keys[idx].iter().enumerate() {
                    if let Some(k) = key {
                        let Some(tid) = tids.get(row_idx).copied() else {
                            return Err(ExecError::Internal(
                                "heap insert_batch returned fewer TIDs than payloads",
                            ));
                        };
                        index.insert_key(*k, tid, self.insert_xmin, wal_ref)?;
                    }
                }
            }
            for (idx, index) in self.insert_vector_indexes.iter().enumerate() {
                for (row_idx, key) in all_insert_vector_index_keys[idx].iter().enumerate() {
                    if let Some(vector) = key {
                        let Some(tid) = tids.get(row_idx).copied() else {
                            return Err(ExecError::Internal(
                                "heap insert_batch returned fewer TIDs than payloads",
                            ));
                        };
                        index.insert_vector(vector, tid)?;
                    }
                }
            }
            self.add_affected_rows(n)?;
        }

        if returning_active {
            return build_batch(&returning_rows, &self.schema).map(Some);
        }

        // Emit the affected-row-count batch.
        let batch = Batch::new([Column::Int64(NumericColumn::from_data(vec![self.affected]))])
            .map_err(ExecError::from)?;
        Ok(Some(batch))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}
