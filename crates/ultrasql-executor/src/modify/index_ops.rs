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
            if !seen_keys[idx].insert(key) || index.contains_key(key)? {
                return Err(ExecError::UniqueViolation(index.name.clone()));
            }
        }
        Ok(())
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
                if let Some(key) = old_key {
                    let _ = self.update_indexes[idx].delete_key(
                        key,
                        change.old_tid,
                        self.delete_xmax,
                        wal,
                    )?;
                }
                if let Some(key) = new_key {
                    if self.update_indexes[idx].is_unique()
                        && self.update_indexes[idx].contains_key(key)?
                    {
                        return Err(ExecError::UniqueViolation(
                            self.update_indexes[idx].name.clone(),
                        ));
                    }
                    self.update_indexes[idx].insert_key(key, new_tid, self.delete_xmax, wal)?;
                }
            }
        }
        Ok(())
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
                if self.update_indexes[idx].is_unique()
                    && self.update_indexes[idx].contains_key(new_key)?
                {
                    return Err(ExecError::UniqueViolation(
                        self.update_indexes[idx].name.clone(),
                    ));
                }
            }
        }
        Ok(())
    }

    pub(crate) fn extract_delete_tids_and_index_changes(
        &self,
        batch: &Batch,
        capture_deleted_rows: bool,
    ) -> Result<DeleteExtraction, ExecError> {
        let child_schema = self.child.schema().clone();
        let rows = crate::filter_op::batch_to_rows(batch, &child_schema)
            .map_err(|e| ExecError::TypeMismatch(e.to_string()))?;
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
        changes: &[DeleteIndexChange],
    ) -> Result<(), ExecError> {
        let wal = self.wal.clone();
        let wal_ref = wal.as_deref();
        for change in changes {
            for idx in 0..self.delete_indexes.len() {
                if let Some(key) = change.keys[idx] {
                    let _ = self.delete_indexes[idx].delete_key(
                        key,
                        change.tid,
                        self.delete_xmax,
                        wal_ref,
                    )?;
                }
            }
        }
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
