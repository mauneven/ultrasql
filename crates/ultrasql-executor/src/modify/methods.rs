//! Per-row UPDATE edit construction for [`ModifyTable`]: the slow-path
//! UPDATE/MERGE edit computation, ON CONFLICT resolution, RETURNING
//! evaluation, and INSERT default / generated / constraint application.

use ultrasql_core::{DataType, TupleId, Value};
use ultrasql_storage::PageLoader;
use ultrasql_storage::heap::UpdatePayload;

use super::helpers::{check_not_null_violations, extract_tid_and_row, updated_ctid_target};
use super::{
    ComputedUpdate, ModifyTable, SequenceDefault, UpdateIndexChange, VectorUpdateIndexChange,
};
use crate::eval::Eval;
use crate::{ExecError, eval_error_to_exec_error};

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
    pub(crate) fn compute_update_edit(
        &self,
        row: &[Value],
        capture_returning_row: bool,
    ) -> Result<ComputedUpdate, ExecError> {
        self.compute_update_edit_with_evaluators(
            row,
            &self.update_evaluators,
            capture_returning_row,
        )
    }

    pub(crate) fn compute_update_edit_with_evaluators(
        &self,
        row: &[Value],
        assignments: &[(usize, Eval)],
        capture_returning_row: bool,
    ) -> Result<ComputedUpdate, ExecError> {
        let (tid, eval_row) = extract_tid_and_row(row, self.relation)?;

        // Build the new row from the original, applying assignments.
        let relation_cols = self.codec.schema().len();
        if eval_row.len() < relation_cols
            || (!self.update_extra_eval_columns && eval_row.len() != relation_cols)
        {
            return Err(ExecError::TypeMismatch(format!(
                "UPDATE row has {} columns after TID, expected {}",
                eval_row.len(),
                relation_cols,
            )));
        }
        let orig_row = &eval_row[..relation_cols];
        let mut new_row: Vec<Value> = orig_row.to_vec();
        let old_keys = self.encode_update_index_keys(orig_row)?;
        let old_vector_keys = self.encode_update_vector_index_keys(orig_row)?;

        for (col_idx, evaluator) in assignments {
            if self
                .generated_stored
                .get(*col_idx)
                .is_some_and(Option::is_some)
            {
                return Err(ExecError::GeneratedAlwaysViolation(
                    self.codec.schema().field_at(*col_idx).name.clone(),
                ));
            }
            let val = evaluator.eval(eval_row).map_err(eval_error_to_exec_error)?;
            if *col_idx >= relation_cols {
                return Err(ExecError::TypeMismatch(format!(
                    "UPDATE assignment column index {col_idx} out of range (relation has {relation_cols} columns)"
                )));
            }
            new_row[*col_idx] = val;
        }
        self.apply_generated_stored(&mut new_row)?;
        check_not_null_violations(&new_row, self.codec.schema())?;
        self.check_row_constraints(&new_row)?;
        self.check_foreign_keys(&new_row)?;
        self.check_exclusion_update(orig_row, &new_row)?;
        self.check_referenced_by_update(orig_row, &new_row)?;
        let new_keys = self.encode_update_index_keys(&new_row)?;
        let new_vector_keys = self.encode_update_vector_index_keys(&new_row)?;

        let new_payload = self
            .codec
            .encode(&new_row)
            .map_err(|e| ExecError::TypeMismatch(e.to_string()))?;
        // Move the encoded bytes into a `SmallVec<[u8; 16]>`; rows
        // ≤ 16 bytes stay inline. `SmallVec::from_vec` reuses the
        // existing heap buffer when the row spills.
        let payload = UpdatePayload::from_vec(new_payload);
        let index_change = if self.update_indexes.is_empty() {
            None
        } else {
            Some(UpdateIndexChange {
                old_tid: tid,
                old_keys,
                new_keys,
            })
        };
        let vector_index_change = if self.update_vector_indexes.is_empty() {
            None
        } else {
            Some(VectorUpdateIndexChange {
                old_tid: tid,
                old_keys: old_vector_keys,
                new_keys: new_vector_keys,
            })
        };
        Ok(ComputedUpdate {
            tid,
            payload,
            index_change,
            vector_index_change,
            returning_row: capture_returning_row.then_some(new_row),
        })
    }

    pub(crate) fn compute_conflict_update_edit(
        &self,
        tid: TupleId,
        orig_row: &[Value],
        excluded_row: &[Value],
        assignments: &[(usize, Eval)],
        predicate: Option<&Eval>,
        capture_returning_row: bool,
    ) -> Result<Option<ComputedUpdate>, ExecError> {
        let mut eval_row = Vec::with_capacity(orig_row.len().saturating_add(excluded_row.len()));
        eval_row.extend_from_slice(orig_row);
        eval_row.extend_from_slice(excluded_row);
        if let Some(predicate) = predicate {
            match predicate
                .eval(&eval_row)
                .map_err(eval_error_to_exec_error)?
            {
                Value::Bool(true) => {}
                Value::Bool(false) | Value::Null => return Ok(None),
                other => {
                    return Err(ExecError::TypeMismatch(format!(
                        "ON CONFLICT DO UPDATE WHERE returned {:?}, expected Bool",
                        other.data_type()
                    )));
                }
            }
        }

        let relation_cols = self.codec.schema().len();
        let mut new_row: Vec<Value> = orig_row.to_vec();
        if new_row.len() != relation_cols {
            return Err(ExecError::TypeMismatch(format!(
                "ON CONFLICT row has {} columns, expected {}",
                new_row.len(),
                relation_cols,
            )));
        }
        let old_keys = self.encode_update_index_keys(orig_row)?;
        let old_vector_keys = self.encode_update_vector_index_keys(orig_row)?;

        for (col_idx, evaluator) in assignments {
            if self
                .generated_stored
                .get(*col_idx)
                .is_some_and(Option::is_some)
            {
                return Err(ExecError::GeneratedAlwaysViolation(
                    self.codec.schema().field_at(*col_idx).name.clone(),
                ));
            }
            if *col_idx >= relation_cols {
                return Err(ExecError::TypeMismatch(format!(
                    "ON CONFLICT assignment column index {col_idx} out of range (relation has {relation_cols} columns)"
                )));
            }
            new_row[*col_idx] = evaluator
                .eval(&eval_row)
                .map_err(eval_error_to_exec_error)?;
        }
        self.apply_generated_stored(&mut new_row)?;
        check_not_null_violations(&new_row, self.codec.schema())?;
        self.check_row_constraints(&new_row)?;
        self.check_foreign_keys(&new_row)?;
        self.check_exclusion_update(orig_row, &new_row)?;
        self.check_referenced_by_update(orig_row, &new_row)?;
        let new_keys = self.encode_update_index_keys(&new_row)?;
        let new_vector_keys = self.encode_update_vector_index_keys(&new_row)?;

        let new_payload = self
            .codec
            .encode(&new_row)
            .map_err(|e| ExecError::TypeMismatch(e.to_string()))?;
        let payload = UpdatePayload::from_vec(new_payload);
        let index_change = if self.update_indexes.is_empty() {
            None
        } else {
            Some(UpdateIndexChange {
                old_tid: tid,
                old_keys,
                new_keys,
            })
        };
        let vector_index_change = if self.update_vector_indexes.is_empty() {
            None
        } else {
            Some(VectorUpdateIndexChange {
                old_tid: tid,
                old_keys: old_vector_keys,
                new_keys: new_vector_keys,
            })
        };
        Ok(Some(ComputedUpdate {
            tid,
            payload,
            index_change,
            vector_index_change,
            returning_row: capture_returning_row.then_some(new_row),
        }))
    }

    pub(crate) fn fetch_conflict_current_row(
        &self,
        tid: TupleId,
    ) -> Result<(TupleId, Vec<Value>), ExecError> {
        let mut current = tid;
        for _ in 0..64 {
            let tuple = self.heap.fetch(current).map_err(|e| {
                ExecError::TypeMismatch(format!("ON CONFLICT fetch existing tuple: {e}"))
            })?;
            if let Some(next) = updated_ctid_target(&tuple.header, current) {
                current = next;
                continue;
            }
            let row = self.codec.decode(&tuple.data).map_err(|e| {
                ExecError::TypeMismatch(format!("ON CONFLICT decode existing tuple: {e}"))
            })?;
            return Ok((current, row));
        }
        Err(ExecError::Internal(
            "ON CONFLICT update ctid chain exceeded 64 hops",
        ))
    }

    pub(crate) fn evaluate_returning_row(&self, row: &[Value]) -> Result<Vec<Value>, ExecError> {
        self.returning_evaluators
            .iter()
            .map(|eval| eval.eval(row).map_err(eval_error_to_exec_error))
            .collect()
    }

    pub(crate) fn apply_insert_defaults(
        &self,
        row: &mut [Value],
        omitted: &[bool],
    ) -> Result<(), ExecError> {
        if self.column_defaults.is_empty() && self.sequence_defaults.is_empty() {
            return Ok(());
        }
        if (!self.column_defaults.is_empty() && self.column_defaults.len() != row.len())
            || (!self.sequence_defaults.is_empty() && self.sequence_defaults.len() != row.len())
            || omitted.len() != row.len()
        {
            return Err(ExecError::TypeMismatch(
                "INSERT default metadata width does not match target row".to_owned(),
            ));
        }
        for idx in 0..row.len() {
            if !omitted[idx] {
                continue;
            }
            if let Some(default) = self.sequence_defaults.get(idx).and_then(Option::as_ref) {
                row[idx] = self.next_sequence_default_value(idx, default)?;
                continue;
            }
            let Some(default) = self.column_defaults.get(idx) else {
                continue;
            };
            if let Some(evaluator) = default {
                row[idx] = evaluator.eval(&[]).map_err(eval_error_to_exec_error)?;
            }
        }
        Ok(())
    }

    pub(crate) fn check_identity_explicit_values(&self, omitted: &[bool]) -> Result<(), ExecError> {
        if self.identity_always.is_empty() {
            return Ok(());
        }
        if self.identity_always.len() != omitted.len() {
            return Err(ExecError::TypeMismatch(
                "INSERT identity metadata width does not match target row".to_owned(),
            ));
        }
        for (idx, always) in self.identity_always.iter().copied().enumerate() {
            if always && !omitted[idx] {
                return Err(ExecError::GeneratedAlwaysViolation(
                    self.codec.schema().field_at(idx).name.clone(),
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn check_generated_stored_explicit_values(
        &self,
        omitted: &[bool],
    ) -> Result<(), ExecError> {
        if self.generated_stored.is_empty() {
            return Ok(());
        }
        if self.generated_stored.len() != omitted.len() {
            return Err(ExecError::TypeMismatch(
                "INSERT generated-column metadata width does not match target row".to_owned(),
            ));
        }
        for (idx, generated) in self.generated_stored.iter().enumerate() {
            if generated.is_some() && !omitted[idx] {
                return Err(ExecError::GeneratedAlwaysViolation(
                    self.codec.schema().field_at(idx).name.clone(),
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn apply_generated_stored(&self, row: &mut [Value]) -> Result<(), ExecError> {
        if self.generated_stored.is_empty() {
            return Ok(());
        }
        if self.generated_stored.len() != row.len() {
            return Err(ExecError::TypeMismatch(
                "generated-column metadata width does not match target row".to_owned(),
            ));
        }
        for idx in 0..row.len() {
            let Some(evaluator) = self.generated_stored.get(idx).and_then(Option::as_ref) else {
                continue;
            };
            row[idx] = evaluator.eval(row).map_err(eval_error_to_exec_error)?;
        }
        Ok(())
    }

    pub(crate) fn check_foreign_keys(&self, row: &[Value]) -> Result<(), ExecError> {
        for check in &self.foreign_key_checks {
            check(row)?;
        }
        Ok(())
    }

    pub(crate) fn check_exclusions(&self, row: &[Value]) -> Result<(), ExecError> {
        for check in &self.exclusion_checks {
            check(row)?;
        }
        Ok(())
    }

    pub(crate) fn check_exclusion_update(
        &self,
        old_row: &[Value],
        new_row: &[Value],
    ) -> Result<(), ExecError> {
        for check in &self.exclusion_update_checks {
            check(old_row, new_row)?;
        }
        Ok(())
    }

    pub(crate) fn check_referenced_by_delete(&self, row: &[Value]) -> Result<(), ExecError> {
        for check in &self.referenced_by_delete_checks {
            check(row)?;
        }
        Ok(())
    }

    pub(crate) fn check_referenced_by_update(
        &self,
        old_row: &[Value],
        new_row: &[Value],
    ) -> Result<(), ExecError> {
        for check in &self.referenced_by_update_checks {
            check(old_row, new_row)?;
        }
        Ok(())
    }

    pub(crate) fn next_sequence_default_value(
        &self,
        idx: usize,
        default: &SequenceDefault,
    ) -> Result<Value, ExecError> {
        let raw = if let Some(wal) = &default.wal {
            default.sequence.nextval_logged(
                &default.name,
                default.seqrelid,
                default.xid,
                Some(wal.as_ref()),
            )
        } else {
            default.sequence.nextval()
        }
        .map_err(|e| ExecError::TypeMismatch(format!("sequence default {}: {e}", default.name)))?;
        if let Some(on_nextval) = &default.on_nextval {
            on_nextval(&default.name, raw);
        }
        let field = self.codec.schema().field_at(idx);
        match field.data_type {
            DataType::Int16 => i16::try_from(raw).map(Value::Int16).map_err(|_| {
                ExecError::TypeMismatch(format!(
                    "sequence default {} value {raw} out of range for Int16",
                    default.name
                ))
            }),
            DataType::Int32 => i32::try_from(raw).map(Value::Int32).map_err(|_| {
                ExecError::TypeMismatch(format!(
                    "sequence default {} value {raw} out of range for Int32",
                    default.name
                ))
            }),
            DataType::Int64 => Ok(Value::Int64(raw)),
            ref other => Err(ExecError::TypeMismatch(format!(
                "sequence default {} cannot populate {:?}",
                default.name, other
            ))),
        }
    }

    pub(crate) fn check_row_constraints(&self, row: &[Value]) -> Result<(), ExecError> {
        for check in &self.check_constraints {
            match check
                .evaluator
                .eval(row)
                .map_err(eval_error_to_exec_error)?
            {
                Value::Bool(true) | Value::Null => {}
                Value::Bool(false) => return Err(ExecError::CheckViolation(check.name.clone())),
                other => {
                    return Err(ExecError::TypeMismatch(format!(
                        "CHECK constraint {} returned {:?}, expected Bool",
                        check.name,
                        other.data_type()
                    )));
                }
            }
        }
        Ok(())
    }
}
