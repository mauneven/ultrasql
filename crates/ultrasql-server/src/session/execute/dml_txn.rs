//! DML/SELECT execution wrapper and transaction finalisation / rollback helpers.

use super::*;
use crate::session::AutocommitAbortGuard;

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
    ///
    /// `allow_streaming` is supplied by the *dispatch context*, never
    /// hardwired here: only the single-statement Simple-Query network path
    /// (`handle_query` → `send_query_result_with_ready` →
    /// `drive_streaming_select`) can actually drive a streaming handle, so
    /// only it passes `true`. The multi-statement batch path, the embedded
    /// API, import, and any nested/local caller pass `false` and receive a
    /// fully buffered body (the pre-streaming behaviour) — a streaming
    /// `SelectResult` they could not drive would otherwise ship only
    /// window 0 and leak the XID held by the dropped handle.
    pub(crate) fn run_dml_or_select(
        &mut self,
        plan: &LogicalPlan,
        catalog_snapshot: &Arc<CatalogSnapshot>,
        owner: Option<&Arc<LogicalPlan>>,
        allow_streaming: bool,
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
        // Read-only transaction enforcement (SQLSTATE 25006). A
        // data-modifying statement inside a READ ONLY transaction is
        // rejected and, like any in-transaction error, aborts the block.
        // Checked before the prechecked fast path so cached fused
        // INSERT/UPDATE/DELETE shapes are caught too.
        if let TxnState::InTransaction(txn) = &self.txn_state
            && txn.read_only
            && let Some(command) = Self::read_only_violation_command(plan)
        {
            if let TxnState::InTransaction(txn) =
                std::mem::replace(&mut self.txn_state, TxnState::Idle)
            {
                self.txn_state = TxnState::Failed(txn);
            }
            return Err(ServerError::ReadOnlyTransaction(command));
        }
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
        // The cached `(Int32, Int32)` full-scan fast path is answered from
        // the version-stamped column cache. It is shared and replayed RAW,
        // so it may only serve a snapshot that reflects exactly the
        // committed state at the cache's version (see
        // `ColumnCache::is_snapshot_coherent`). In autocommit `Idle` mode we
        // still skip `begin()` / `commit()` (no XID allocation, no CLOG
        // transition), but we build a lightweight read-only snapshot for the
        // gate. For the quiescent `select_scan_10k` hot path the in-progress
        // set is empty, so this is one atomic load plus an empty walk — the
        // gate passes and the fast path fires unchanged. Under concurrency
        // the gate fails and we fall through to the normal txn path, which
        // walks the heap. Explicit transaction blocks reuse `txn.snapshot`.
        if matches!(self.txn_state, TxnState::Idle) {
            let autocommit_snapshot = self
                .state
                .txn_manager
                .statement_snapshot(ultrasql_core::Xid::INVALID, ultrasql_core::CommandId::FIRST);
            if let Some(result) = try_run_cached_int32_pair_select(
                plan,
                catalog_snapshot,
                self.state.heap.as_ref(),
                &autocommit_snapshot,
                self.state.txn_manager.as_ref(),
                &mut self.write_buf,
            ) {
                return Ok(result);
            }
            if let Some(result) = try_run_cached_scalar_aggregate_select(
                plan,
                catalog_snapshot,
                self.state.heap.as_ref(),
                &autocommit_snapshot,
                self.state.txn_manager.as_ref(),
                &mut self.write_buf,
            ) {
                return Ok(result);
            }
            if let Some(result) =
                crate::projection_summary::try_run_cached_grouped_projection_select(
                    plan,
                    catalog_snapshot,
                    self.state.heap.as_ref(),
                    &autocommit_snapshot,
                    self.state.txn_manager.as_ref(),
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
                &self.current_txn_snapshot(),
                self.state.txn_manager.as_ref(),
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
                // Arm an unwind guard for the autocommit XID. If
                // `run_plan_in_txn` (executor-grade code over user data, full of
                // panic sites) or `finalise_autocommit` panics, this guard's
                // Drop aborts the XID — releasing the per-tuple locks the
                // statement acquired, which `Transaction`'s own Drop (it has
                // none) cannot. On every NORMAL return below the guard is
                // disarmed: `finalise_autocommit` either commits (buffered Ok),
                // aborts (Err), or hands the XID to `drive_streaming_select`
                // (streaming Ok, which installs its own guard), so the guard
                // must only fire on a panic between here and that return.
                let mut abort_guard =
                    AutocommitAbortGuard::arm(Arc::clone(&self.state.txn_manager), txn.xid);
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
                    allow_streaming,
                    // Clone the autocommit txn into the would-be streaming
                    // handle: the clone shares the same XID, so the drive
                    // loop commits the statement after the drain while the
                    // original `txn` here is dropped uncommitted (a no-op
                    // for an `InProgress` CLOG entry). Only consumed on the
                    // streaming SELECT branch; the buffered/DML paths drop
                    // the clone and `finalise_autocommit` commits `txn`.
                    // Skip the clone entirely when streaming is disallowed:
                    // the SELECT arm cannot produce a handle, so no consumer
                    // exists for it.
                    streaming_commit_txn: allow_streaming.then(|| txn.clone()),
                });
                let result = self.finalise_autocommit(plan, txn, outcome);
                // Reached only on a NORMAL return from `finalise_autocommit`
                // (it committed, aborted, or handed off to the streaming drive
                // loop). Disarm so the guard does not also abort. A panic inside
                // `finalise_autocommit` skips this and the guard fires.
                abort_guard.disarm();
                result
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
                    allow_streaming,
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
        // Evict the shared column cache for every table this aborting txn
        // modified. A plain in-txn INSERT/COPY bumps the cache version at
        // physical insert time but is not undone by `rollback_in_place_updates`
        // (that only reverts in-place-UPDATE / DELETE-stamp writes), so a
        // projection the writer published from its own uncommitted read-after-
        // write snapshot — and the COUNT(*) scalar-aggregate / single-column
        // wire bodies derived from it — would otherwise linger and be served to
        // a fresh reader once this xid aborts. Bumping with the parent xid
        // forces a heap rebuild that hides the rolled-back rows. No-op when no
        // in-txn write was recorded (the common autocommit single-statement
        // error). Mirrors the explicit-ROLLBACK / ROLLBACK TO SAVEPOINT paths.
        self.invalidate_modified_table_column_caches(xid);
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
            .map(|_committed_subxids| ())
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
