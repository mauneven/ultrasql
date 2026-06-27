//! Index-key encoding, ON CONFLICT detection, INSERT duplicate-key
//! rejection, DELETE/MERGE row preparation, and B-tree / vector index
//! change application for [`ModifyTable`].

use std::collections::HashSet;

use ultrasql_core::{TupleId, Value};
use ultrasql_storage::PageLoader;
use ultrasql_storage::wal_sink::WalSink;
use ultrasql_vec::Batch;

use super::helpers::{
    check_not_null_violations, columns_match_unordered, conflict_target_columns, expand_insert_row,
    extract_tid_and_row, insert_conflict_uses_index, row_codec_error_to_exec,
};
use super::{
    ComputedDelete, DeleteExtraction, DeleteIndexChange, InsertConflict, InsertConflictAction,
    ModifyTable, PreparedInsert, UpdateIndexChange, VectorDeleteIndexChange,
    VectorUpdateIndexChange,
};
use crate::eval::Eval;
use crate::{ExecError, eval_error_to_exec_error};

impl<L: PageLoader + Send + Sync + std::fmt::Debug + 'static> ModifyTable<L> {
    pub(crate) fn encode_update_index_keys(
        &self,
        row: &[Value],
    ) -> Result<Vec<Option<i64>>, ExecError> {
        self.update_indexes
            .iter()
            .map(|index| index.encode_key(row))
            .collect()
    }

    pub(crate) fn encode_update_vector_index_keys(
        &self,
        row: &[Value],
    ) -> Result<Vec<Option<Vec<f32>>>, ExecError> {
        self.update_vector_indexes
            .iter()
            .map(|index| index.encode_key(row))
            .collect()
    }

    pub(crate) fn validate_insert_conflict_arbiter(
        &self,
        action: Option<&InsertConflictAction>,
    ) -> Result<(), ExecError> {
        let Some(action) = action else {
            return Ok(());
        };
        let Some(target) = conflict_target_columns(action) else {
            return Ok(());
        };
        if self
            .insert_indexes
            .iter()
            .any(|index| index.is_unique() && columns_match_unordered(&index.key_columns, target))
        {
            return Ok(());
        }
        Err(ExecError::TypeMismatch(format!(
            "ON CONFLICT target {:?} does not match a unique index",
            target
        )))
    }

    pub(crate) fn find_insert_conflict(
        &self,
        action: &InsertConflictAction,
        row_keys: &[Option<i64>],
        seen_keys: &[HashSet<i64>],
    ) -> Result<Option<InsertConflict>, ExecError> {
        for (idx, index) in self.insert_indexes.iter().enumerate() {
            if !insert_conflict_uses_index(action, index) {
                continue;
            }
            let Some(key) = row_keys.get(idx).copied().flatten() else {
                continue;
            };
            if seen_keys.get(idx).is_some_and(|seen| seen.contains(&key)) {
                return Ok(Some(InsertConflict::InBatch));
            }
            if let Some(tid) = index.lookup_tid(key)? {
                return Ok(Some(InsertConflict::Existing(tid)));
            }
        }
        Ok(None)
    }

    pub(crate) fn reject_duplicate_insert_keys(
        &self,
        row_keys: &[Option<i64>],
        seen_keys: &mut [HashSet<i64>],
    ) -> Result<(), ExecError> {
        for (idx, index) in self.insert_indexes.iter().enumerate() {
            let Some(key) = row_keys.get(idx).copied().flatten() else {
                continue;
            };
            if !index.is_unique() {
                continue;
            }
            // In-batch duplicate: two rows in this statement carry the same
            // unique key — always a violation regardless of heap state.
            if !seen_keys[idx].insert(key) {
                return Err(ExecError::UniqueViolation(index.name.clone()));
            }
            // Against the existing index: under Option-A a stale leaf entry
            // pointing at a dead tuple is NOT a conflict. Heap-recheck when a
            // uniqueness snapshot is wired; otherwise fall back to the
            // index-only probe.
            if self.insert_key_conflicts_live(idx, key)? {
                return Err(ExecError::UniqueViolation(index.name.clone()));
            }
        }
        Ok(())
    }

    /// `true` iff inserting `key` into unique index `idx` conflicts with a
    /// **live** existing row (heap-rechecking when a uniqueness snapshot is
    /// wired; index-only otherwise).
    pub(crate) fn insert_key_conflicts_live(
        &self,
        idx: usize,
        key: i64,
    ) -> Result<bool, ExecError> {
        let index = &self.insert_indexes[idx];
        match (&self.uniqueness_snapshot, &self.uniqueness_oracle) {
            (Some(snapshot), Some(oracle)) => Ok(matches!(
                index.classify_unique_conflict(
                    key,
                    self.heap.as_ref(),
                    snapshot,
                    oracle.as_ref(),
                )?,
                super::index_maintainer::UniqueConflict::Live
            )),
            _ => index.contains_key(key),
        }
    }

    /// The dead TID (if any) to physically replace before inserting `key`
    /// into unique index `idx` (Option-A targeted-dead-replace). `None`
    /// when there is no entry, when the entry is live (caller already
    /// rejected), when the index is non-unique, or when no recheck is wired.
    pub(crate) fn insert_dead_replace_tid(
        &self,
        idx: usize,
        key: i64,
    ) -> Result<Option<TupleId>, ExecError> {
        let index = &self.insert_indexes[idx];
        if !index.is_unique() {
            return Ok(None);
        }
        match (&self.uniqueness_snapshot, &self.uniqueness_oracle) {
            (Some(snapshot), Some(oracle)) => {
                match index.classify_unique_conflict(
                    key,
                    self.heap.as_ref(),
                    snapshot,
                    oracle.as_ref(),
                )? {
                    super::index_maintainer::UniqueConflict::Dead(tid) => Ok(Some(tid)),
                    _ => Ok(None),
                }
            }
            _ => Ok(None),
        }
    }

    pub(crate) fn remember_insert_keys(
        &self,
        row_keys: &[Option<i64>],
        seen_keys: &mut [HashSet<i64>],
    ) {
        for (idx, index) in self.insert_indexes.iter().enumerate() {
            let Some(key) = row_keys.get(idx).copied().flatten() else {
                continue;
            };
            if index.is_unique() {
                seen_keys[idx].insert(key);
            }
        }
    }

    pub(crate) fn apply_update_index_changes(
        &mut self,
        changes: &[UpdateIndexChange],
        outcomes: &[ultrasql_storage::heap::UpdateOutcome],
        wal: Option<&dyn WalSink>,
    ) -> Result<(), ExecError> {
        let outcome_by_old: std::collections::HashMap<
            TupleId,
            ultrasql_storage::heap::UpdateOutcome,
        > = outcomes
            .iter()
            .map(|outcome| (outcome.old_tid, *outcome))
            .collect();
        for change in changes {
            let Some(outcome) = outcome_by_old.get(&change.old_tid).copied() else {
                return Err(ExecError::Internal(
                    "heap update_many_with_outcomes omitted an updated TID",
                ));
            };
            let new_tid = outcome.new_tid;
            for idx in 0..self.update_indexes.len() {
                let old_key = change.old_keys[idx];
                let new_key = change.new_keys[idx];
                if old_key == new_key {
                    continue;
                }
                // Option-A (design §1 A2): do NOT physically remove the old
                // key's leaf entry. The old TID's tuple now carries
                // `xmax = current_xid`, so the heap recheck filters it; the
                // new entry below is the live one. Leaving the old entry
                // keeps the index coherent under `ROLLBACK TO` and defers
                // reclamation to VACUUM — same model as the DELETE arm.
                if let Some(key) = new_key {
                    // Heap-rechecking uniqueness: a stale leaf entry pointing
                    // at a dead tuple is not a conflict (and is replaced).
                    let dead_tid = self.classify_update_unique_conflict(idx, key, new_tid)?;
                    self.update_indexes[idx].insert_key_replacing_dead(
                        key,
                        new_tid,
                        dead_tid,
                        self.delete_xmax,
                        wal,
                    )?;
                }
            }
        }
        Ok(())
    }

    /// Resolve a unique-conflict for a key-changing UPDATE's new key.
    ///
    /// Returns the dead TID to replace (Option-A targeted-dead-replace) or
    /// `None` when there is nothing to replace; errors with
    /// `UniqueViolation` on a *live* conflict. When no uniqueness snapshot
    /// is wired, or the index is non-unique, falls back to the index-only
    /// probe so behaviour is unchanged for callers that did not opt in.
    fn classify_update_unique_conflict(
        &self,
        idx: usize,
        key: i64,
        new_tid: TupleId,
    ) -> Result<Option<TupleId>, ExecError> {
        let index = &self.update_indexes[idx];
        if !index.is_unique() {
            return Ok(None);
        }
        match (&self.uniqueness_snapshot, &self.uniqueness_oracle) {
            (Some(snapshot), Some(oracle)) => {
                match index.classify_unique_conflict(
                    key,
                    self.heap.as_ref(),
                    snapshot,
                    oracle.as_ref(),
                )? {
                    super::index_maintainer::UniqueConflict::Live => {
                        Err(ExecError::UniqueViolation(index.name.clone()))
                    }
                    super::index_maintainer::UniqueConflict::Dead(dead_tid) => {
                        // A dead entry pointing at the row we just produced
                        // (a HOT-style same-TID rewrite) is not a stale
                        // duplicate to remove.
                        Ok((dead_tid != new_tid).then_some(dead_tid))
                    }
                    super::index_maintainer::UniqueConflict::None => Ok(None),
                }
            }
            // No recheck wired: preserve the index-only behaviour.
            _ => {
                if index.contains_key(key)? {
                    Err(ExecError::UniqueViolation(index.name.clone()))
                } else {
                    Ok(None)
                }
            }
        }
    }

    pub(crate) fn precheck_update_index_changes(
        &self,
        changes: &[UpdateIndexChange],
    ) -> Result<(), ExecError> {
        for change in changes {
            for idx in 0..self.update_indexes.len() {
                let Some(new_key) = change.new_keys[idx] else {
                    continue;
                };
                if change.old_keys[idx] == Some(new_key) {
                    continue;
                }
                if !self.update_indexes[idx].is_unique() {
                    continue;
                }
                // Heap-rechecking precheck: a stale leaf entry pointing at a
                // dead tuple is not a conflict (Option-A). Falls back to the
                // index-only probe when no uniqueness snapshot is wired.
                if self.update_key_conflicts_live(idx, new_key)? {
                    return Err(ExecError::UniqueViolation(
                        self.update_indexes[idx].name.clone(),
                    ));
                }
            }
        }
        Ok(())
    }

    /// `true` iff `new_key` conflicts with a **live** row in unique update
    /// index `idx` (heap-rechecking when wired; index-only otherwise).
    fn update_key_conflicts_live(&self, idx: usize, new_key: i64) -> Result<bool, ExecError> {
        let index = &self.update_indexes[idx];
        match (&self.uniqueness_snapshot, &self.uniqueness_oracle) {
            (Some(snapshot), Some(oracle)) => Ok(matches!(
                index.classify_unique_conflict(
                    new_key,
                    self.heap.as_ref(),
                    snapshot,
                    oracle.as_ref(),
                )?,
                super::index_maintainer::UniqueConflict::Live
            )),
            _ => index.contains_key(new_key),
        }
    }

    pub(crate) fn extract_delete_tids_and_index_changes(
        &self,
        batch: &Batch,
        capture_deleted_rows: bool,
    ) -> Result<DeleteExtraction, ExecError> {
        let child_schema = self.child.schema().clone();
        let rows = crate::filter_op::batch_to_rows(batch, &child_schema)
            .map_err(|e| ExecError::TypeMismatch(e.to_string()))?;
        self.extract_delete_tids_from_rows(rows, capture_deleted_rows)
    }

    /// EvalPlanQual DELETE extraction: lock + re-check every targeted row,
    /// then build the heap-delete TIDs / index changes / deleted-row images
    /// from the surviving (still-matching, not-concurrently-deleted) rows at
    /// their latest base TID. See
    /// [`ModifyTable::apply_eval_plan_qual`](super::ModifyTable::apply_eval_plan_qual).
    pub(crate) fn extract_delete_tids_and_index_changes_epq(
        &self,
        batch: &Batch,
        capture_deleted_rows: bool,
    ) -> Result<DeleteExtraction, ExecError> {
        let child_schema = self.child.schema().clone();
        let rows = crate::filter_op::batch_to_rows(batch, &child_schema)
            .map_err(|e| ExecError::TypeMismatch(e.to_string()))?;
        let rows = self.apply_eval_plan_qual(rows)?;
        self.extract_delete_tids_from_rows(rows, capture_deleted_rows)
    }

    fn extract_delete_tids_from_rows(
        &self,
        rows: Vec<Vec<Value>>,
        capture_deleted_rows: bool,
    ) -> Result<DeleteExtraction, ExecError> {
        let mut tids: Vec<TupleId> = Vec::with_capacity(rows.len());
        let mut changes: Vec<DeleteIndexChange> = Vec::with_capacity(rows.len());
        let mut vector_changes: Vec<VectorDeleteIndexChange> = Vec::with_capacity(rows.len());
        let mut deleted_rows: Vec<Vec<Value>> = if capture_deleted_rows {
            Vec::with_capacity(rows.len())
        } else {
            Vec::new()
        };
        for row in &rows {
            let (tid, orig_row) = extract_tid_and_row(row, self.relation)?;
            let relation_cols = self.codec.schema().len();
            if orig_row.len() < relation_cols {
                return Err(ExecError::TypeMismatch(format!(
                    "DELETE row has {} columns after TID, expected at least {}",
                    orig_row.len(),
                    relation_cols,
                )));
            }
            let target_row = &orig_row[..relation_cols];
            self.check_referenced_by_delete(target_row)?;
            let keys = self
                .delete_indexes
                .iter()
                .map(|index| index.encode_key(target_row))
                .collect::<Result<Vec<_>, _>>()?;
            let vector_keys = self
                .delete_vector_indexes
                .iter()
                .map(|index| index.encode_key(target_row))
                .collect::<Result<Vec<_>, _>>()?;
            tids.push(tid);
            changes.push(DeleteIndexChange { tid, keys });
            vector_changes.push(VectorDeleteIndexChange {
                tid,
                keys: vector_keys,
            });
            if capture_deleted_rows {
                deleted_rows.push(target_row.to_vec());
            }
        }
        Ok((tids, changes, vector_changes, deleted_rows))
    }

    pub(crate) fn compute_delete_change_from_row(
        &self,
        row: &[Value],
    ) -> Result<ComputedDelete, ExecError> {
        let (tid, orig_row) = extract_tid_and_row(row, self.relation)?;
        let relation_cols = self.codec.schema().len();
        if orig_row.len() < relation_cols {
            return Err(ExecError::TypeMismatch(format!(
                "MERGE DELETE row has {} columns after TID, expected at least {}",
                orig_row.len(),
                relation_cols,
            )));
        }
        let target_row = &orig_row[..relation_cols];
        self.check_referenced_by_delete(target_row)?;
        let keys = self
            .delete_indexes
            .iter()
            .map(|index| index.encode_key(target_row))
            .collect::<Result<Vec<_>, _>>()?;
        let vector_keys = self
            .delete_vector_indexes
            .iter()
            .map(|index| index.encode_key(target_row))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ComputedDelete {
            tid,
            index_change: (!self.delete_indexes.is_empty())
                .then_some(DeleteIndexChange { tid, keys }),
            vector_index_change: (!self.delete_vector_indexes.is_empty()).then_some(
                VectorDeleteIndexChange {
                    tid,
                    keys: vector_keys,
                },
            ),
        })
    }

    pub(crate) fn prepare_merge_insert_row(
        &self,
        row: &[Value],
        columns: &[usize],
        values: &[Eval],
        seen_keys: &mut [HashSet<i64>],
    ) -> Result<PreparedInsert, ExecError> {
        let relation_cols = self.codec.schema().len();
        if row.len() < 3 + relation_cols {
            return Err(ExecError::TypeMismatch(format!(
                "MERGE INSERT row has {} columns, expected at least {}",
                row.len(),
                3 + relation_cols,
            )));
        }
        let eval_row = &row[3..];
        let source_values = values
            .iter()
            .map(|eval| eval.eval(eval_row).map_err(eval_error_to_exec_error))
            .collect::<Result<Vec<_>, _>>()?;
        let expanded = expand_insert_row(&source_values, relation_cols, columns)?;
        let mut target_row = expanded.values;
        self.check_identity_explicit_values(&expanded.omitted)?;
        self.apply_insert_defaults(&mut target_row, &expanded.omitted)?;
        self.check_generated_stored_explicit_values(&expanded.omitted)?;
        self.apply_generated_stored(&mut target_row)?;
        self.check_row_constraints(&target_row)?;
        self.check_foreign_keys(&target_row)?;
        self.check_exclusions(&target_row)?;
        check_not_null_violations(&target_row, self.codec.schema())?;
        let index_keys = self
            .insert_indexes
            .iter()
            .map(|index| index.encode_key(&target_row))
            .collect::<Result<Vec<_>, _>>()?;
        self.reject_duplicate_insert_keys(&index_keys, seen_keys)?;
        let vector_index_keys = self
            .insert_vector_indexes
            .iter()
            .map(|index| index.encode_key(&target_row))
            .collect::<Result<Vec<_>, _>>()?;
        let payload = self
            .codec
            .encode(&target_row)
            .map_err(row_codec_error_to_exec)?;
        Ok(PreparedInsert {
            payload,
            index_keys,
            vector_index_keys,
        })
    }

    pub(crate) fn apply_delete_index_changes(
        &mut self,
        _changes: &[DeleteIndexChange],
    ) -> Result<(), ExecError> {
        // Option-A (design §1 A1): MVCC DELETE no longer physically removes
        // the B-tree leaf entry. The deleted tuple's `xmax` stamp is the
        // sole authority — both index read paths (`btree_probe` /
        // `late_materialize`) re-fetch the heap tuple and drop the candidate
        // when the delete is visible, so a stale leaf entry can never
        // surface a dead row. Leaving the entry in place makes `ROLLBACK TO`
        // index-coherent for free (there is nothing to restore) and lets
        // VACUUM reclaim the leaf once the tuple is dead to every snapshot.
        //
        // `extract_delete_tids_and_index_changes` still builds the
        // `DeleteIndexChange` records (kept deliberately — see the struct's
        // doc); only the B-tree *apply* becomes a no-op. The sibling
        // `VectorDeleteIndexChange` path still applies, since vector indexes
        // keep an explicit tombstone model rather than heap-recheck.
        Ok(())
    }

    pub(crate) fn apply_delete_vector_index_changes(
        &self,
        changes: &[VectorDeleteIndexChange],
    ) -> Result<(), ExecError> {
        for change in changes {
            for idx in 0..self.delete_vector_indexes.len() {
                if change.keys[idx].is_some() {
                    self.delete_vector_indexes[idx].delete_tid(change.tid)?;
                }
            }
        }
        Ok(())
    }

    pub(crate) fn apply_update_vector_index_changes(
        &self,
        changes: &[VectorUpdateIndexChange],
        outcomes: &[ultrasql_storage::heap::UpdateOutcome],
    ) -> Result<(), ExecError> {
        let outcome_by_old: std::collections::HashMap<
            TupleId,
            ultrasql_storage::heap::UpdateOutcome,
        > = outcomes
            .iter()
            .map(|outcome| (outcome.old_tid, *outcome))
            .collect();
        for change in changes {
            let Some(outcome) = outcome_by_old.get(&change.old_tid).copied() else {
                return Err(ExecError::Internal(
                    "heap update_many_with_outcomes omitted an updated TID",
                ));
            };
            let new_tid = outcome.new_tid;
            for idx in 0..self.update_vector_indexes.len() {
                if outcome.hot && change.old_keys[idx] == change.new_keys[idx] {
                    continue;
                }
                if change.old_keys[idx].is_some() {
                    self.update_vector_indexes[idx].delete_tid(change.old_tid)?;
                }
                if let Some(vector) = &change.new_keys[idx] {
                    self.update_vector_indexes[idx].insert_vector(vector, new_tid)?;
                }
            }
        }
        Ok(())
    }
}
