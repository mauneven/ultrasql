//! DML/SELECT execution wrapper and transaction finalisation / rollback helpers.

use super::*;

impl<RW> Session<RW>
where
    RW: AsyncRead + AsyncWrite + Unpin,
{
    /// Run a DML/SELECT plan against the session's current [`TxnState`].
    ///
    /// - `Idle` → open a fresh autocommit txn, run, commit on success
    ///   (or abort on error); state stays `Idle`.
    /// - `InTransaction` → refresh the per-statement snapshot, run
    ///   inside the existing txn, don't commit. On success state stays
    ///   `InTransaction`; on error transitions to `Failed`.
    /// - `Failed` → unreachable (the caller guarded).
    pub(crate) fn run_dml_or_select(
        &mut self,
        plan: &LogicalPlan,
        catalog_snapshot: &Arc<CatalogSnapshot>,
        owner: Option<&Arc<LogicalPlan>>,
    ) -> Result<SelectResult, ServerError> {
        // `owner`, when present, must be the `Arc` that `plan` derefs from:
        // the precheck cache pins it by identity, so a mismatch would let a
        // skipped check apply to the wrong plan. Only the pointer-stable
        // `stmt_cache` path passes `Some`; cold / view-rewrite plans pass
        // `None` and always run the full checks.
        debug_assert!(
            owner.is_none_or(|arc| std::ptr::eq(&**arc, plan)),
            "run_dml_or_select owner Arc must point to the plan passed by value",
        );
        let prechecked = self.fast_dml_prechecked(owner);
        let rls_plan = if prechecked {
            None
        } else {
            self.apply_row_security(plan, catalog_snapshot, crate::RuntimeRlsCommand::Select)?
        };
        let plan = rls_plan.as_ref().unwrap_or(plan);
        if !prechecked {
            self.check_rls_insert_values(plan, catalog_snapshot)?;
            self.enforce_column_privileges(plan, catalog_snapshot)?;
        }
        let _operator_span =
            tracing::debug_span!("sql.operator", plan = ?std::mem::discriminant(plan)).entered();
        // The cached `(Int32, Int32)` full-scan fast path is already
        // answered from the version-stamped column cache and does not
        // consult txn-local visibility state. In autocommit `Idle`
        // mode there is therefore no user-visible work for `begin()` /
        // `commit()` to do; skipping them avoids one XID allocation,
        // one snapshot build, and one CLOG transition on the
        // `select_scan_10k` hot path. Explicit transaction blocks keep
        // the normal machinery so `ReadyForQuery` state and command-id
        // progression stay unchanged there.
        if matches!(self.txn_state, TxnState::Idle) {
            if let Some(result) = try_run_cached_int32_pair_select(
                plan,
                catalog_snapshot,
                self.state.heap.as_ref(),
                &mut self.write_buf,
            ) {
                return Ok(result);
            }
            if let Some(result) = try_run_cached_scalar_aggregate_select(
                plan,
                catalog_snapshot,
                self.state.heap.as_ref(),
                &mut self.write_buf,
            ) {
                return Ok(result);
            }
            if let Some(result) =
                crate::projection_summary::try_run_cached_grouped_projection_select(
                    plan,
                    catalog_snapshot,
                    self.state.heap.as_ref(),
                    &mut self.write_buf,
                )
            {
                return Ok(result);
            }
        }
        if self.can_use_cached_scalar_aggregate_in_explicit_txn(plan)
            && let Some(result) = try_run_cached_scalar_aggregate_select(
                plan,
                catalog_snapshot,
                self.state.heap.as_ref(),
                &mut self.write_buf,
            )
        {
            return Ok(result);
        }
        if !prechecked {
            self.reject_non_append_materialized_view_source_write(plan)?;
            if rls_plan.is_none()
                && let Some(arc) = owner
                && let Some(key) = Self::prechecked_fast_dml_key(arc)
                && self.fast_dml_checks_cacheable(plan)
            {
                // Store the `Arc`, not just `key`: the strong reference pins
                // the allocation so its address can't be recycled, and the
                // value is what `fast_dml_prechecked` verifies with
                // `Arc::ptr_eq`.
                self.prechecked_fast_dml
                    .borrow_mut()
                    .insert(key, Arc::clone(arc));
            }
        }

        match std::mem::replace(&mut self.txn_state, TxnState::Idle) {
            TxnState::Idle => {
                let txn = self.state.txn_manager.begin(IsolationLevel::ReadCommitted);
                let outcome = run_plan_in_txn(RunPlanInTxnArgs {
                    plan,
                    txn: &txn,
                    catalog_snapshot: Arc::clone(catalog_snapshot),
                    table_constraints: Arc::clone(&self.state.table_constraints),
                    sequences: Arc::clone(&self.state.sequences),
                    sequence_owners: Arc::clone(&self.state.sequence_owners),
                    sequence_namespaces: Arc::clone(&self.state.sequence_namespaces),
                    schemas: Arc::clone(&self.state.schemas),
                    operators: Arc::clone(&self.state.operators),
                    role_catalog: Arc::clone(&self.state.role_catalog),
                    privilege_catalog: Arc::clone(&self.state.privilege_catalog),
                    row_security: Arc::clone(&self.state.row_security),
                    session_settings: Arc::new(self.session_settings.clone()),
                    current_user: self.current_user.clone(),
                    session_user: self.auth_user.clone(),
                    persistent_catalog: Arc::clone(&self.state.persistent_catalog),
                    time_partitions: Arc::clone(&self.state.time_partitions),
                    workload_recorder: Arc::clone(&self.state.workload_recorder),
                    autovacuum_config: self.state.autovacuum_config(),
                    logging_config: self.state.logging_config(),
                    wal_archive_config: self.state.wal_archive_config(),
                    data_dir: self.state.data_dir.clone(),
                    logical_replication: Arc::clone(&self.state.logical_replication),
                    sequence_state: Some(self.sequence_state.clone()),
                    advisory_state: Some(self.advisory_state.clone()),
                    tables: &self.state.tables,
                    heap: Arc::clone(&self.state.heap),
                    vm: Arc::clone(&self.state.vm),
                    oracle: Arc::clone(&self.state.txn_manager),
                    jit: self.jit_config(),
                    cancel_flag: Some(self.cancel_flag.clone()),
                    stream_buf: &mut self.write_buf,
                    allow_streaming: true,
                    // Clone the autocommit txn into the would-be streaming
                    // handle: the clone shares the same XID, so the drive
                    // loop commits the statement after the drain while the
                    // original `txn` here is dropped uncommitted (a no-op
                    // for an `InProgress` CLOG entry). Only consumed on the
                    // streaming SELECT branch; the buffered/DML paths drop
                    // the clone and `finalise_autocommit` commits `txn`.
                    streaming_commit_txn: Some(txn.clone()),
                });
                self.finalise_autocommit(plan, txn, outcome)
            }
            TxnState::InTransaction(mut txn) => {
                self.state.txn_manager.refresh_snapshot(&mut txn);
                if let Some(outcome) =
                    self.try_run_fused_delete_in_explicit_txn(plan, catalog_snapshot, &txn)?
                {
                    let outcome = match outcome {
                        Ok(result) => {
                            self.note_dml_effect(plan, result.rows)?;
                            match self.flush_dirty_heap_pages_after_dml_if_needed(plan, result.rows)
                            {
                                Ok(()) => Ok(result),
                                Err(err) => Err(err),
                            }
                        }
                        Err(err) => Err(err),
                    };
                    self.txn_state = if outcome.is_ok() {
                        TxnState::InTransaction(txn)
                    } else {
                        TxnState::Failed(txn)
                    };
                    return outcome;
                }
                let outcome = run_plan_in_txn(RunPlanInTxnArgs {
                    plan,
                    txn: &txn,
                    catalog_snapshot: Arc::clone(catalog_snapshot),
                    table_constraints: Arc::clone(&self.state.table_constraints),
                    sequences: Arc::clone(&self.state.sequences),
                    sequence_owners: Arc::clone(&self.state.sequence_owners),
                    sequence_namespaces: Arc::clone(&self.state.sequence_namespaces),
                    schemas: Arc::clone(&self.state.schemas),
                    operators: Arc::clone(&self.state.operators),
                    role_catalog: Arc::clone(&self.state.role_catalog),
                    privilege_catalog: Arc::clone(&self.state.privilege_catalog),
                    row_security: Arc::clone(&self.state.row_security),
                    session_settings: Arc::new(self.session_settings.clone()),
                    current_user: self.current_user.clone(),
                    session_user: self.auth_user.clone(),
                    persistent_catalog: Arc::clone(&self.state.persistent_catalog),
                    time_partitions: Arc::clone(&self.state.time_partitions),
                    workload_recorder: Arc::clone(&self.state.workload_recorder),
                    autovacuum_config: self.state.autovacuum_config(),
                    logging_config: self.state.logging_config(),
                    wal_archive_config: self.state.wal_archive_config(),
                    data_dir: self.state.data_dir.clone(),
                    logical_replication: Arc::clone(&self.state.logical_replication),
                    sequence_state: Some(self.sequence_state.clone()),
                    advisory_state: Some(self.advisory_state.clone()),
                    tables: &self.state.tables,
                    heap: Arc::clone(&self.state.heap),
                    vm: Arc::clone(&self.state.vm),
                    oracle: Arc::clone(&self.state.txn_manager),
                    jit: self.jit_config(),
                    cancel_flag: Some(self.cancel_flag.clone()),
                    stream_buf: &mut self.write_buf,
                    allow_streaming: true,
                    // Inside an explicit transaction block the handle stays
                    // in `self.txn_state` and is committed later by COMMIT,
                    // so there is no per-statement txn to hand to the drive
                    // loop. The streaming operator drains independently
                    // against its cloned MVCC snapshot.
                    streaming_commit_txn: None,
                });
                let outcome = match outcome {
                    Ok(result) => {
                        self.note_dml_effect(plan, result.rows)?;
                        match self.flush_dirty_heap_pages_after_dml_if_needed(plan, result.rows) {
                            Ok(()) => Ok(result),
                            Err(err) => Err(err),
                        }
                    }
                    Err(err) => Err(err),
                };
                // Transition: Ok → InTransaction; Err → Failed. The txn
                // remains alive in the CLOG (InProgress) until the user
                // issues COMMIT/ROLLBACK.
                self.txn_state = if outcome.is_ok() {
                    TxnState::InTransaction(txn)
                } else {
                    TxnState::Failed(txn)
                };
                outcome
            }
            TxnState::Failed(txn) => {
                // Should be guarded by the caller; restore state.
                self.txn_state = TxnState::Failed(txn);
                Err(ServerError::TransactionAborted)
            }
        }
    }

    /// Commit-on-success / abort-on-error for the autocommit path.
    /// Surfaces cleanup/finalization failures so the client never sees
    /// success for a transaction the server could not close cleanly.
    pub(crate) fn finalise_autocommit(
        &mut self,
        plan: &LogicalPlan,
        txn: Transaction,
        outcome: Result<SelectResult, ServerError>,
    ) -> Result<SelectResult, ServerError> {
        let is_dml = Self::dml_target_table(plan).is_some();
        match outcome {
            Ok(result) if result.streaming.is_some() => {
                // Large streaming SELECT: the txn-clone carried inside the
                // handle is committed by the async drive loop *after* the
                // result drains (cursor semantics). Drop the original `txn`
                // here uncommitted — it shares the handle's XID, so the
                // post-drain commit finalises the statement exactly once.
                // A streaming result is always a read-only SELECT, so the
                // DML-finalize machinery above does not apply.
                debug_assert!(!is_dml, "streaming result must be a read-only SELECT");
                drop(txn);
                Ok(result)
            }
            Ok(result) => {
                if is_dml {
                    if let Err(e) = self.state.validate_deferred_foreign_keys(&txn) {
                        return Err(self.rollback_transaction_after_error_with_abort_marker(
                            txn,
                            e,
                            "autocommit rollback after deferred FK violation",
                            true,
                        ));
                    }
                    if let Err(e) =
                        self.flush_dirty_heap_pages_after_dml_if_needed(plan, result.rows)
                    {
                        return Err(self.rollback_transaction_after_error_with_abort_marker(
                            txn,
                            e,
                            "autocommit rollback after dirty-page flush error",
                            true,
                        ));
                    }
                }
                if let Err(e) = self
                    .state
                    .commit_transaction(txn, is_dml, "autocommit statement")
                {
                    return Err(e);
                } else {
                    self.pending_post_commit_maintenance = true;
                    let rows = result.rows;
                    let modified_table = (rows > 0)
                        .then(|| Self::dml_target_table(plan))
                        .flatten()
                        .map(str::to_ascii_lowercase);
                    self.note_dml_effect(plan, rows)?;
                    if let Some(table) = &modified_table {
                        self.maintain_aggregating_indexes_for_tables_after_commit(
                            std::slice::from_ref(table),
                        )?;
                    }
                    self.maintain_append_only_materialized_views_after_commit(plan)?;
                    self.flush_pending_dml_effects()?;
                }
                Ok(result)
            }
            Err(e) => Err(self.rollback_transaction_after_error_with_abort_marker(
                txn,
                e,
                "autocommit rollback after statement error",
                is_dml,
            )),
        }
    }

    pub(crate) fn rollback_transaction_after_error(
        &self,
        txn: Transaction,
        original: ServerError,
        context: &'static str,
    ) -> ServerError {
        let durable_abort_marker = !self.pending_table_modifications.is_empty();
        self.rollback_transaction_after_error_with_abort_marker(
            txn,
            original,
            context,
            durable_abort_marker,
        )
    }

    pub(crate) fn rollback_transaction_after_error_with_abort_marker(
        &self,
        txn: Transaction,
        original: ServerError,
        context: &'static str,
        durable_abort_marker: bool,
    ) -> ServerError {
        // Roll back any in-place UPDATE writes by this txn before
        // terminating the CLOG entry, so the undo walker still sees
        // the writer's XID. Surface cleanup failure to the client;
        // otherwise callers could miss that autocommit rollback did
        // not actually finish.
        let original_text = original.to_string();
        let xid = txn.xid;
        let rollback_err = self
            .state
            .heap
            .rollback_in_place_updates(xid)
            .err()
            .map(|err| err.to_string());
        let abort_err = self
            .state
            .abort_transaction(txn, durable_abort_marker, context)
            .err()
            .map(|err| err.to_string());
        match (rollback_err, abort_err) {
            (None, None) => original,
            (Some(rollback), None) => ServerError::Ddl(format!(
                "{context}: {original_text}; in-place update rollback failed: {rollback}"
            )),
            (None, Some(abort)) => ServerError::Ddl(format!(
                "{context}: {original_text}; transaction abort failed: {abort}"
            )),
            (Some(rollback), Some(abort)) => ServerError::Ddl(format!(
                "{context}: {original_text}; in-place update rollback failed: {rollback}; transaction abort failed: {abort}"
            )),
        }
    }

    pub(crate) fn rollback_catalog_transaction_after_error(
        &self,
        txn: Transaction,
        original: ServerError,
        context: &'static str,
    ) -> ServerError {
        self.rollback_transaction_after_error(txn, original, context)
    }

    pub(crate) fn rollback_materialized_view_maintenance_after_error(
        &self,
        txn: Transaction,
        original: ServerError,
        context: &'static str,
    ) -> ServerError {
        self.rollback_transaction_after_error(txn, original, context)
    }

    pub(crate) fn finalise_read_transaction(
        &self,
        txn: Transaction,
        context: &'static str,
    ) -> Result<(), ServerError> {
        self.state
            .txn_manager
            .commit(txn)
            .map_err(|err| ServerError::Ddl(format!("{context}: {err}")))
    }

    pub(crate) fn finalise_read_maintenance_transaction(
        &self,
        txn: Transaction,
        outcome: Result<(), ServerError>,
        commit_context: &'static str,
        rollback_context: &'static str,
    ) -> Result<(), ServerError> {
        match outcome {
            Ok(()) => self.finalise_read_transaction(txn, commit_context),
            Err(err) => Err(self.rollback_transaction_after_error(txn, err, rollback_context)),
        }
    }
}
