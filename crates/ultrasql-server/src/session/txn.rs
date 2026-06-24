//! Part of the `session` module split. The
//! `impl<RW> Session<RW>` block is reopened here to add a handful
//! of methods to the type defined in `session/mod.rs`. Splitting
//! across files keeps every unit under the 600-line ceiling without
//! changing semantics.

use tokio::io::{AsyncRead, AsyncWrite};
use ultrasql_planner::{LogicalPlan, TxnIsolationLevel};
use ultrasql_protocol::BackendMessage;
use ultrasql_txn::IsolationLevel;

use super::Session;
use crate::error::ServerError;
use crate::result_encoder::SelectResult;
use crate::{TxnState, notice_warning};

impl<RW> Session<RW>
where
    RW: AsyncRead + AsyncWrite + Unpin,
{
    /// Dispatch a transaction-control statement (BEGIN / COMMIT /
    /// ROLLBACK / SAVEPOINT / ROLLBACK TO / RELEASE) against the
    /// session's [`TxnState`].
    ///
    /// PostgreSQL semantics:
    ///
    /// - `BEGIN` inside an open transaction emits a `NoticeResponse`
    ///   `WARNING: there is already a transaction in progress` and
    ///   leaves the state unchanged.
    /// - `COMMIT` / `ROLLBACK` outside a transaction emits a
    ///   `NoticeResponse` `WARNING: there is no transaction in progress`
    ///   and emits `COMMIT` / `ROLLBACK` as the command tag.
    /// - `COMMIT` while in the `Failed` state aborts the transaction and
    ///   returns the `ROLLBACK` tag — *not* `COMMIT` — matching
    ///   PostgreSQL's behaviour of treating a failed-block commit as a
    ///   rollback so the application's "did the COMMIT really land?"
    ///   check still works.
    pub(crate) fn execute_txn_control(
        &mut self,
        plan: &LogicalPlan,
    ) -> Result<SelectResult, ServerError> {
        match plan {
            LogicalPlan::Begin {
                isolation_level,
                read_only,
                ..
            } => self.execute_begin(*isolation_level, *read_only),
            LogicalPlan::Commit { .. } => self.execute_commit(),
            LogicalPlan::Rollback { .. } => self.execute_rollback(),
            LogicalPlan::Savepoint { name, .. } => self.execute_savepoint(name),
            LogicalPlan::RollbackToSavepoint { name, .. } => {
                self.execute_rollback_to_savepoint(name)
            }
            LogicalPlan::ReleaseSavepoint { name, .. } => self.execute_release_savepoint(name),
            LogicalPlan::PrepareTransaction { gid, .. } => self.execute_prepare_transaction(gid),
            LogicalPlan::CommitPrepared { gid, .. } => self.execute_commit_prepared(gid),
            LogicalPlan::RollbackPrepared { gid, .. } => self.execute_rollback_prepared(gid),
            LogicalPlan::SetTransaction {
                isolation_level,
                read_only,
                ..
            } => self.execute_set_transaction(*isolation_level, *read_only),
            _ => Err(ServerError::Unsupported(
                "execute_txn_control called with non-txn-control plan",
            )),
        }
    }

    /// `PREPARE TRANSACTION 'gid'` — phase 1 of two-phase commit.
    ///
    /// Disassociates the current transaction from the session and
    /// hands its `xid` to the [`TwoPhaseCoordinator`] under `gid`.
    /// The CLOG entry stays `InProgress` until phase 2 finalises it.
    /// PostgreSQL rules:
    /// - Outside a transaction: error `25P01`.
    /// - Inside a failed block: phase-1 prepare aborts the txn and
    ///   returns a rollback tag, mirroring failed-block COMMIT.
    pub(crate) fn execute_prepare_transaction(
        &mut self,
        gid: &str,
    ) -> Result<SelectResult, ServerError> {
        match std::mem::replace(&mut self.txn_state, TxnState::Idle) {
            TxnState::Idle => Ok(SelectResult {
                messages: vec![
                    notice_warning("25P01", "PREPARE TRANSACTION outside a transaction"),
                    BackendMessage::CommandComplete {
                        tag: "PREPARE TRANSACTION".to_string(),
                    },
                ],
                streamed_body: None,
                shared_streamed_body: None,
                streaming: None,
                rows: 0,
            }),
            TxnState::InTransaction(mut txn) => {
                self.state
                    .workload_recorder
                    .clear_session_transaction_start(self.pid);
                self.state.txn_manager.refresh_snapshot(&mut txn);
                if !self.pending_table_modifications.is_empty()
                    && let Err(e) = self.state.validate_deferred_foreign_keys(&txn)
                {
                    let err = self.rollback_transaction_after_error_with_abort_marker(
                        txn,
                        e,
                        "PREPARE TRANSACTION rollback after deferred FK violation",
                        true,
                    );
                    self.clear_pending_dml_effects();
                    return Err(err);
                }
                if let Err(e) = self.state.txn_manager.prepare_transaction(
                    gid,
                    txn,
                    self.state.two_phase.as_ref(),
                ) {
                    return Err(ServerError::Ddl(format!("prepare_transaction({gid}): {e}")));
                }
                // Prepared transactions leave this session's state.
                // Keep local modification counters from leaking into
                // subsequent unrelated transactions on this connection.
                self.clear_pending_dml_effects();
                Ok(SelectResult {
                    messages: vec![BackendMessage::CommandComplete {
                        tag: "PREPARE TRANSACTION".to_string(),
                    }],
                    streamed_body: None,
                    shared_streamed_body: None,
                    streaming: None,
                    rows: 0,
                })
            }
            TxnState::Failed(txn) => {
                self.state
                    .workload_recorder
                    .clear_session_transaction_start(self.pid);
                let xid = txn.xid;
                if let Err(e) = self.state.heap.rollback_in_place_updates(xid) {
                    self.txn_state = TxnState::Failed(txn);
                    return Err(ServerError::Ddl(format!(
                        "PREPARE TRANSACTION rollback in-place updates: {e}"
                    )));
                }
                let durable_abort_marker = !self.pending_table_modifications.is_empty();
                if let Err(e) = self.state.abort_transaction(
                    txn,
                    durable_abort_marker,
                    "PREPARE TRANSACTION failed-block",
                ) {
                    self.clear_pending_dml_effects();
                    return Err(e);
                }
                self.clear_pending_dml_effects();
                Ok(SelectResult {
                    messages: vec![BackendMessage::CommandComplete {
                        tag: "ROLLBACK".to_string(),
                    }],
                    streamed_body: None,
                    shared_streamed_body: None,
                    streaming: None,
                    rows: 0,
                })
            }
        }
    }

    /// `COMMIT PREPARED 'gid'` — phase 2 commit of a prepared txn.
    ///
    /// Resolves the gid via the coordinator, finalises the CLOG
    /// entry as Committed, and returns the standard
    /// `COMMIT PREPARED` command tag. A missing gid surfaces as
    /// `ServerError::Internal` carrying the coordinator's error
    /// message.
    pub(crate) fn execute_commit_prepared(
        &mut self,
        gid: &str,
    ) -> Result<SelectResult, ServerError> {
        let prepared = self
            .state
            .two_phase
            .begin_resolution(gid)
            .map_err(|e| ServerError::Ddl(format!("commit_prepared({gid}): {e}")))?;
        let xid = prepared.xid;
        let result = (|| -> Result<(), ServerError> {
            self.state
                .txn_manager
                .validate_prepared(xid)
                .map_err(|e| ServerError::Ddl(format!("validate_prepared({gid}): {e}")))?;
            // The prepared transaction's committed-subxid family (released and
            // open-at-prepare savepoint subxids) was captured at PREPARE and
            // carried durably in the 2PC state file. Embed it in this single
            // Commit WAL record — exactly as single-phase COMMIT does — so a
            // pure-WAL restart after COMMIT PREPARED marks the whole family
            // Committed and rows written under a savepoint do not vanish.
            let committed_subxids = prepared.committed_subxids.clone();
            if let Some(commit_lsn) = self
                .state
                .append_commit_record(xid, committed_subxids.clone())?
            {
                self.state.wait_for_wal_durable(commit_lsn)?;
            }
            self.state
                .txn_manager
                .finalise_prepared(xid, &committed_subxids, ultrasql_mvcc::XidStatus::Committed)
                .map_err(|e| {
                    ServerError::Ddl(format!("finalise_prepared({gid} committed): {e}"))
                })?;
            self.state.note_commit_for_gc();
            Ok(())
        })();
        if let Err(err) = result {
            self.state.two_phase.abort_resolution(&prepared);
            return Err(err);
        }
        self.state
            .two_phase
            .finish_resolution(&prepared)
            .map_err(|e| ServerError::Ddl(format!("commit_prepared({gid}): {e}")))?;
        Ok(SelectResult {
            messages: vec![BackendMessage::CommandComplete {
                tag: "COMMIT PREPARED".to_string(),
            }],
            streamed_body: None,
            shared_streamed_body: None,
            streaming: None,
            rows: 0,
        })
    }

    /// `ROLLBACK PREPARED 'gid'` — phase 2 abort of a prepared txn.
    ///
    /// Symmetric counterpart to [`Self::execute_commit_prepared`].
    /// Drains any pending in-place undo for the prepared xid before
    /// terminating the CLOG entry so a concurrent reader observes
    /// the right post-rollback state.
    pub(crate) fn execute_rollback_prepared(
        &mut self,
        gid: &str,
    ) -> Result<SelectResult, ServerError> {
        let prepared = self
            .state
            .two_phase
            .begin_resolution(gid)
            .map_err(|e| ServerError::Ddl(format!("rollback_prepared({gid}): {e}")))?;
        let xid = prepared.xid;
        let result = (|| -> Result<(), ServerError> {
            self.state
                .txn_manager
                .validate_prepared(xid)
                .map_err(|e| ServerError::Ddl(format!("validate_prepared({gid}): {e}")))?;
            self.state
                .heap
                .rollback_in_place_updates(xid)
                .map_err(|e| {
                    ServerError::Ddl(format!("rollback prepared in-place updates({gid}): {e}"))
                })?;
            if let Some(abort_lsn) = self.state.append_abort_record(xid)? {
                self.state.wait_for_wal_durable(abort_lsn)?;
            }
            // A rolled-back prepared transaction's savepoint family is aborted:
            // it appears in no committed list, so recovery's default-abort sweep
            // discards their rows durably. Force-abort the family in memory too
            // so the same live process agrees with that durable outcome — in the
            // same-process happy path (no restart) the subxids are still
            // InProgress and must be folded to Aborted; after a prepare-restart
            // they are already Aborted and force-abort is idempotent.
            let family = prepared.committed_subxids.clone();
            self.state
                .txn_manager
                .finalise_prepared(xid, &family, ultrasql_mvcc::XidStatus::Aborted)
                .map_err(|e| ServerError::Ddl(format!("finalise_prepared({gid} aborted): {e}")))?;
            Ok(())
        })();
        if let Err(err) = result {
            self.state.two_phase.abort_resolution(&prepared);
            return Err(err);
        }
        self.state
            .two_phase
            .finish_resolution(&prepared)
            .map_err(|e| ServerError::Ddl(format!("rollback_prepared({gid}): {e}")))?;
        Ok(SelectResult {
            messages: vec![BackendMessage::CommandComplete {
                tag: "ROLLBACK PREPARED".to_string(),
            }],
            streamed_body: None,
            shared_streamed_body: None,
            streaming: None,
            rows: 0,
        })
    }

    pub(crate) fn execute_begin(
        &mut self,
        level: Option<TxnIsolationLevel>,
        read_only: Option<bool>,
    ) -> Result<SelectResult, ServerError> {
        let iso = match level {
            None | Some(TxnIsolationLevel::ReadCommitted) => IsolationLevel::ReadCommitted,
            Some(TxnIsolationLevel::RepeatableRead) => IsolationLevel::RepeatableRead,
            Some(TxnIsolationLevel::Serializable) => IsolationLevel::Serializable,
        };
        let warn = match &self.txn_state {
            TxnState::Idle => {
                let mut txn = self.state.txn_manager.begin(iso);
                // `READ WRITE` and an unspecified access mode both default
                // to read-write; only `READ ONLY` flips the flag.
                txn.read_only = read_only.unwrap_or(false);
                self.txn_state = TxnState::InTransaction(txn);
                self.state
                    .workload_recorder
                    .set_session_transaction_start(self.pid);
                None
            }
            TxnState::InTransaction(_) | TxnState::Failed(_) => {
                Some("there is already a transaction in progress")
            }
        };
        let mut messages: Vec<BackendMessage> = Vec::with_capacity(2);
        if let Some(msg) = warn {
            messages.push(notice_warning("25001", msg));
        }
        messages.push(BackendMessage::CommandComplete {
            tag: "BEGIN".to_string(),
        });
        Ok(SelectResult {
            messages,
            streamed_body: None,
            shared_streamed_body: None,
            streaming: None,
            rows: 0,
        })
    }

    /// `SET TRANSACTION ISOLATION LEVEL …` — change the *current*
    /// transaction's isolation level.
    ///
    /// PostgreSQL semantics:
    /// - Outside a transaction: SQLSTATE `25P01`
    ///   (`no_active_sql_transaction`).
    /// - In a failed block: rejected with the standard `25P02`
    ///   (handled by the failed-block guard upstream of this method).
    /// - Inside a healthy transaction: updates `Transaction::isolation`
    ///   in place. If the new level is `Serializable` and an
    ///   [`SsiManager`] is installed, the txn is registered for
    ///   conflict tracking.
    pub(crate) fn execute_set_transaction(
        &mut self,
        level: Option<TxnIsolationLevel>,
        read_only: Option<bool>,
    ) -> Result<SelectResult, ServerError> {
        let iso = level.map(|l| match l {
            TxnIsolationLevel::ReadCommitted => IsolationLevel::ReadCommitted,
            TxnIsolationLevel::RepeatableRead => IsolationLevel::RepeatableRead,
            TxnIsolationLevel::Serializable => IsolationLevel::Serializable,
        });
        let mut messages: Vec<BackendMessage> = Vec::with_capacity(2);
        match &mut self.txn_state {
            TxnState::Idle => {
                messages.push(notice_warning(
                    "25P01",
                    "SET TRANSACTION outside a transaction",
                ));
            }
            TxnState::InTransaction(txn) => {
                if let Some(iso) = iso {
                    txn.isolation = iso;
                    if iso == IsolationLevel::Serializable {
                        self.state.txn_manager.register_serializable(txn.xid);
                    }
                }
                // `None` leaves the access mode unchanged (e.g. a plain
                // `SET TRANSACTION ISOLATION LEVEL …`).
                if let Some(ro) = read_only {
                    txn.read_only = ro;
                }
            }
            TxnState::Failed(_) => {
                // The failed-block 25P02 path is handled at the dispatch
                // layer; if we somehow reach here just leave the txn
                // alone and emit nothing extra.
            }
        }
        messages.push(BackendMessage::CommandComplete {
            tag: "SET".to_string(),
        });
        Ok(SelectResult {
            messages,
            streamed_body: None,
            shared_streamed_body: None,
            streaming: None,
            rows: 0,
        })
    }

    pub(crate) fn execute_commit(&mut self) -> Result<SelectResult, ServerError> {
        match std::mem::replace(&mut self.txn_state, TxnState::Idle) {
            TxnState::Idle => Ok(SelectResult {
                messages: vec![
                    notice_warning("25P01", "there is no transaction in progress"),
                    BackendMessage::CommandComplete {
                        tag: "COMMIT".to_string(),
                    },
                ],
                streamed_body: None,
                shared_streamed_body: None,
                streaming: None,
                rows: 0,
            }),
            TxnState::InTransaction(mut txn) => {
                self.state
                    .workload_recorder
                    .clear_session_transaction_start(self.pid);
                self.state.txn_manager.refresh_snapshot(&mut txn);
                let modified_tables = self
                    .pending_table_modifications
                    .keys()
                    .cloned()
                    .collect::<Vec<_>>();
                if !self.pending_table_modifications.is_empty() {
                    if let Err(e) = self.state.validate_deferred_foreign_keys(&txn) {
                        let err = self.rollback_transaction_after_error_with_abort_marker(
                            txn,
                            e,
                            "COMMIT rollback after deferred FK violation",
                            true,
                        );
                        self.clear_pending_dml_effects();
                        return Err(err);
                    }
                }
                if let Err(e) = self.state.commit_transaction(
                    txn,
                    !modified_tables.is_empty(),
                    "explicit COMMIT",
                ) {
                    tracing::warn!(error = %e, "explicit COMMIT failed to finalise");
                    self.clear_pending_dml_effects();
                    return Err(e);
                } else {
                    self.state.note_commit_for_gc();
                    if let Err(e) =
                        self.maintain_aggregating_indexes_for_tables_after_commit(&modified_tables)
                    {
                        let _ = self.flush_pending_dml_effects();
                        return Err(e);
                    }
                    if let Err(e) =
                        self.maintain_materialized_views_for_tables_after_commit(&modified_tables)
                    {
                        let _ = self.flush_pending_dml_effects();
                        return Err(e);
                    }
                    if let Err(e) = self.flush_pending_materialized_view_rows() {
                        let _ = self.flush_pending_dml_effects();
                        return Err(e);
                    }
                    self.flush_pending_dml_effects()?;
                }
                Ok(SelectResult {
                    messages: vec![BackendMessage::CommandComplete {
                        tag: "COMMIT".to_string(),
                    }],
                    streamed_body: None,
                    shared_streamed_body: None,
                    streaming: None,
                    rows: 0,
                })
            }
            TxnState::Failed(txn) => {
                self.state
                    .workload_recorder
                    .clear_session_transaction_start(self.pid);
                let xid = txn.xid;
                if let Err(e) = self.state.heap.rollback_in_place_updates(xid) {
                    self.txn_state = TxnState::Failed(txn);
                    return Err(ServerError::Ddl(format!(
                        "explicit COMMIT rollback in-place updates: {e}"
                    )));
                }
                let durable_abort_marker = !self.pending_table_modifications.is_empty();
                if let Err(e) = self.state.abort_transaction(
                    txn,
                    durable_abort_marker,
                    "explicit COMMIT rollback",
                ) {
                    self.clear_pending_dml_effects();
                    return Err(e);
                }
                self.clear_pending_dml_effects();
                // PostgreSQL emits the ROLLBACK tag here, not COMMIT.
                Ok(SelectResult {
                    messages: vec![BackendMessage::CommandComplete {
                        tag: "ROLLBACK".to_string(),
                    }],
                    streamed_body: None,
                    shared_streamed_body: None,
                    streaming: None,
                    rows: 0,
                })
            }
        }
    }

    pub(crate) fn execute_rollback(&mut self) -> Result<SelectResult, ServerError> {
        match std::mem::replace(&mut self.txn_state, TxnState::Idle) {
            TxnState::Idle => Ok(SelectResult {
                messages: vec![
                    notice_warning("25P01", "there is no transaction in progress"),
                    BackendMessage::CommandComplete {
                        tag: "ROLLBACK".to_string(),
                    },
                ],
                streamed_body: None,
                shared_streamed_body: None,
                streaming: None,
                rows: 0,
            }),
            TxnState::InTransaction(txn) | TxnState::Failed(txn) => {
                self.state
                    .workload_recorder
                    .clear_session_transaction_start(self.pid);
                let xid = txn.xid;
                if let Err(e) = self.state.heap.rollback_in_place_updates(xid) {
                    self.txn_state = TxnState::Failed(txn);
                    return Err(ServerError::Ddl(format!(
                        "explicit ROLLBACK in-place updates: {e}"
                    )));
                }
                // Recovery treats WAL-observed, non-prepared XIDs with no
                // commit record as aborted, so ordinary explicit rollback does
                // not need a synchronous abort marker.
                if let Err(e) = self
                    .state
                    .abort_transaction(txn, false, "explicit ROLLBACK")
                {
                    self.clear_pending_dml_effects();
                    return Err(e);
                }
                self.clear_pending_dml_effects();
                Ok(SelectResult {
                    messages: vec![BackendMessage::CommandComplete {
                        tag: "ROLLBACK".to_string(),
                    }],
                    streamed_body: None,
                    shared_streamed_body: None,
                    streaming: None,
                    rows: 0,
                })
            }
        }
    }

    /// `SAVEPOINT name` — set a savepoint inside the current
    /// transaction block. Outside a transaction returns SQLSTATE
    /// `25P01` (`no_active_sql_transaction`).
    pub(crate) fn execute_savepoint(&mut self, name: &str) -> Result<SelectResult, ServerError> {
        match &mut self.txn_state {
            TxnState::Idle => Err(ServerError::Savepoint(
                "SAVEPOINT can only be used in transaction blocks",
            )),
            TxnState::Failed(_) => Err(ServerError::TransactionAborted),
            TxnState::InTransaction(txn) => {
                self.state.txn_manager.begin_savepoint(txn, name);
                Ok(SelectResult {
                    messages: vec![BackendMessage::CommandComplete {
                        tag: "SAVEPOINT".to_string(),
                    }],
                    streamed_body: None,
                    shared_streamed_body: None,
                    streaming: None,
                    rows: 0,
                })
            }
        }
    }

    /// `ROLLBACK TO [SAVEPOINT] name` — roll back to the named
    /// savepoint. The transaction remains alive; subsequent statements
    /// run inside the same xid. If the current state is `Failed`, a
    /// successful `ROLLBACK TO` clears the failure flag (matching
    /// PostgreSQL behaviour).
    ///
    /// Errors:
    ///
    /// - Outside a transaction: SQLSTATE `25P01`
    ///   (`no_active_sql_transaction`).
    /// - Unknown savepoint name: SQLSTATE `3B001`
    ///   (`invalid_savepoint_specification`).
    pub(crate) fn execute_rollback_to_savepoint(
        &mut self,
        name: &str,
    ) -> Result<SelectResult, ServerError> {
        // We need to take ownership of the inner txn to mutate it, then
        // put it back in the correct state variant.
        let prior_failed = matches!(self.txn_state, TxnState::Failed(_));
        let state = std::mem::replace(&mut self.txn_state, TxnState::Idle);
        match state {
            TxnState::Idle => {
                // `TxnState::Idle` is the default left behind by the
                // replace; nothing to restore.
                Err(ServerError::Savepoint(
                    "ROLLBACK TO SAVEPOINT can only be used in transaction blocks",
                ))
            }
            TxnState::InTransaction(mut txn) | TxnState::Failed(mut txn) => {
                match self.state.txn_manager.rollback_to_savepoint(&mut txn, name) {
                    Ok(aborted_subxids) => {
                        // Physically undo every write the rolled-back
                        // subtransactions made: restore in-place-UPDATE
                        // pre-images and clear DELETE stamps. With the
                        // Phase-1 visibility predicate these are no longer
                        // *required* for own-visibility (the snapshot's
                        // rolled-back set already reverts them), but they
                        // reclaim heap bytes, keep the seq-scan path and the
                        // undo log consistent, and invalidate the column
                        // cache for the touched relations (the heap undo
                        // helpers bump the cache version internally). This
                        // mirrors the full-abort path, scoped to the aborted
                        // subxids.
                        for sub_xid in aborted_subxids {
                            if let Err(e) = self.state.heap.rollback_in_place_updates(sub_xid) {
                                // Heap undo failed: the transaction is now in
                                // an indeterminate physical state. Mark the
                                // block failed so the user must ROLLBACK.
                                self.txn_state = TxnState::Failed(txn);
                                return Err(ServerError::Ddl(format!(
                                    "ROLLBACK TO SAVEPOINT physical undo (subxid {sub_xid}): {e}"
                                )));
                            }
                        }
                        // Invalidate the shared column cache for every table
                        // modified in this transaction (design §3 R8). The
                        // cache is version-keyed and replayed RAW; a rolled-
                        // back INSERT does not flow through the heap undo
                        // helpers (which only bump the cache for in-place /
                        // delete-stamp undo), so its row could otherwise
                        // linger in a cached projection whose coherence gate
                        // — keyed on the now-aborted subxid — wrongly passes.
                        // Bumping with the parent xid evicts the stale entry
                        // and forces the next scan to rebuild from the heap,
                        // where the Phase-1 predicate hides the rolled-back
                        // rows.
                        self.invalidate_modified_table_column_caches(txn.xid);
                        // Clear the failure flag: the rolled-back work is
                        // undone so the user can continue.
                        self.txn_state = TxnState::InTransaction(txn);
                        Ok(SelectResult {
                            messages: vec![BackendMessage::CommandComplete {
                                tag: "ROLLBACK".to_string(),
                            }],
                            streamed_body: None,
                            shared_streamed_body: None,
                            streaming: None,
                            rows: 0,
                        })
                    }
                    Err(_) => {
                        // Unknown savepoint name. Restore the prior state
                        // (the rollback did not fire so the txn is in the
                        // same shape as before this call).
                        self.txn_state = if prior_failed {
                            TxnState::Failed(txn)
                        } else {
                            TxnState::InTransaction(txn)
                        };
                        Err(ServerError::SavepointNotFound(name.to_owned()))
                    }
                }
            }
        }
    }

    /// Evict the shared column-cache projection for every table this
    /// transaction has modified, recording `writer_xid` as the new
    /// last-writer so the current transaction can immediately rebuild a
    /// fresh projection from the heap.
    ///
    /// Used by `ROLLBACK TO SAVEPOINT` so a rolled-back write cannot be
    /// served from a stale cached projection (design §3 R8). The set of
    /// modified tables is the session's `pending_table_modifications` keys;
    /// invalidating a superset (all tables touched this txn, not just under
    /// the rolled-back subxids) is safe — it only forces a heap rebuild.
    fn invalidate_modified_table_column_caches(&self, writer_xid: ultrasql_core::Xid) {
        if self.pending_table_modifications.is_empty() {
            return;
        }
        let snapshot = self.state.catalog_snapshot();
        for table in self.pending_table_modifications.keys() {
            if let Some(entry) = snapshot.tables.get(&table.to_ascii_lowercase()) {
                self.state
                    .heap
                    .column_cache
                    .bump_version(ultrasql_core::RelationId(entry.oid), writer_xid);
            }
        }
    }

    /// `RELEASE [SAVEPOINT] name` — destroy a savepoint. Subsequent
    /// `ROLLBACK TO` of the same name will fail.
    ///
    /// A savepoint-not-found error inside an explicit transaction
    /// transitions the session to `Failed` (matching PostgreSQL: any
    /// statement that errors inside a transaction block aborts the
    /// block until COMMIT/ROLLBACK).
    pub(crate) fn execute_release_savepoint(
        &mut self,
        name: &str,
    ) -> Result<SelectResult, ServerError> {
        let release_ok = match &mut self.txn_state {
            TxnState::Idle => {
                return Err(ServerError::Savepoint(
                    "RELEASE SAVEPOINT can only be used in transaction blocks",
                ));
            }
            TxnState::Failed(_) => return Err(ServerError::TransactionAborted),
            TxnState::InTransaction(txn) => {
                self.state.txn_manager.release_savepoint(txn, name).is_ok()
            }
        };
        if release_ok {
            Ok(SelectResult {
                messages: vec![BackendMessage::CommandComplete {
                    tag: "RELEASE".to_string(),
                }],
                streamed_body: None,
                shared_streamed_body: None,
                streaming: None,
                rows: 0,
            })
        } else {
            Err(self.fail_if_in_transaction(ServerError::SavepointNotFound(name.to_owned())))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use tokio::io::{DuplexStream, duplex};

    use crate::Server;

    fn test_session() -> Session<DuplexStream> {
        let (io, _peer) = duplex(64);
        Session::new(io, Arc::new(Server::with_sample_database()), None)
    }

    fn empty_plan() -> LogicalPlan {
        LogicalPlan::Empty {
            schema: ultrasql_core::Schema::empty(),
        }
    }

    fn last_tag(result: &SelectResult) -> &str {
        let Some(BackendMessage::CommandComplete { tag }) = result.messages.last() else {
            panic!("missing command tag");
        };
        tag
    }

    #[test]
    fn txn_control_unit_paths_cover_idle_warnings_and_failed_blocks() {
        let mut session = test_session();
        assert!(session.execute_txn_control(&empty_plan()).is_err());

        let commit = session.execute_commit().expect("idle commit warning");
        assert_eq!(last_tag(&commit), "COMMIT");
        assert_eq!(commit.messages.len(), 2);
        let rollback = session.execute_rollback().expect("idle rollback warning");
        assert_eq!(last_tag(&rollback), "ROLLBACK");
        assert_eq!(rollback.messages.len(), 2);

        let prepare = session
            .execute_prepare_transaction("idle-gid")
            .expect("idle prepare warning");
        assert_eq!(last_tag(&prepare), "PREPARE TRANSACTION");
        assert_eq!(prepare.messages.len(), 2);

        session
            .execute_begin(Some(TxnIsolationLevel::Serializable), None)
            .expect("begin");
        assert!(matches!(session.txn_state, TxnState::InTransaction(_)));
        let nested = session
            .execute_begin(None, None)
            .expect("nested begin warning");
        assert_eq!(last_tag(&nested), "BEGIN");
        assert_eq!(nested.messages.len(), 2);
        let set = session
            .execute_set_transaction(Some(TxnIsolationLevel::RepeatableRead), None)
            .expect("set transaction");
        assert_eq!(last_tag(&set), "SET");

        let txn = match std::mem::replace(&mut session.txn_state, TxnState::Idle) {
            TxnState::InTransaction(txn) => txn,
            other => panic!("expected in transaction, got {other:?}"),
        };
        session.txn_state = TxnState::Failed(txn);
        let failed_commit = session.execute_commit().expect("failed commit rolls back");
        assert_eq!(last_tag(&failed_commit), "ROLLBACK");

        let txn = session
            .state
            .txn_manager
            .begin(IsolationLevel::ReadCommitted);
        session.txn_state = TxnState::Failed(txn);
        let failed_prepare = session
            .execute_prepare_transaction("failed-gid")
            .expect("failed prepare rolls back");
        assert_eq!(last_tag(&failed_prepare), "ROLLBACK");
    }

    #[test]
    fn two_phase_and_savepoint_paths_cover_success_and_errors() {
        let mut session = test_session();
        assert!(session.execute_commit_prepared("missing").is_err());
        assert!(session.execute_rollback_prepared("missing").is_err());
        assert!(session.execute_savepoint("s").is_err());
        assert!(session.execute_rollback_to_savepoint("s").is_err());
        assert!(session.execute_release_savepoint("s").is_err());

        session.execute_begin(None, None).expect("begin");
        session.execute_savepoint("s1").expect("savepoint");
        assert!(session.execute_rollback_to_savepoint("missing").is_err());
        assert!(matches!(session.txn_state, TxnState::InTransaction(_)));
        session
            .execute_savepoint("s2")
            .expect("savepoint after error");
        session
            .execute_rollback_to_savepoint("s2")
            .expect("rollback to savepoint");
        session
            .execute_release_savepoint("s1")
            .expect("release savepoint");
        session.execute_rollback().expect("rollback");

        session
            .execute_begin(None, None)
            .expect("begin failed release");
        session.execute_savepoint("bad_release").expect("savepoint");
        assert!(session.execute_release_savepoint("missing").is_err());
        assert!(matches!(session.txn_state, TxnState::Failed(_)));
        assert!(session.execute_savepoint("after_failed").is_err());
        session.execute_rollback().expect("rollback failed block");

        session
            .execute_begin(None, None)
            .expect("begin prepare commit");
        session
            .execute_prepare_transaction("commit-gid")
            .expect("prepare commit");
        let committed = session
            .execute_commit_prepared("commit-gid")
            .expect("commit prepared");
        assert_eq!(last_tag(&committed), "COMMIT PREPARED");

        session
            .execute_begin(None, None)
            .expect("begin prepare rollback");
        session
            .execute_prepare_transaction("rollback-gid")
            .expect("prepare rollback");
        let rolled_back = session
            .execute_rollback_prepared("rollback-gid")
            .expect("rollback prepared");
        assert_eq!(last_tag(&rolled_back), "ROLLBACK PREPARED");
    }

    #[test]
    fn commit_prepared_keeps_state_when_finalise_fails() {
        let mut session = test_session();
        session
            .state
            .two_phase
            .prepare("orphan-commit", ultrasql_core::Xid::new(99_001), &[])
            .expect("prepare orphan");

        let err = session
            .execute_commit_prepared("orphan-commit")
            .expect_err("missing CLOG entry must fail");
        assert!(
            err.to_string().contains("validate_prepared"),
            "unexpected error: {err}"
        );
        assert_eq!(session.state.two_phase.list_prepared().len(), 1);
    }

    #[test]
    fn rollback_prepared_keeps_state_when_finalise_fails() {
        let mut session = test_session();
        session
            .state
            .two_phase
            .prepare("orphan-rollback", ultrasql_core::Xid::new(99_002), &[])
            .expect("prepare orphan");

        let err = session
            .execute_rollback_prepared("orphan-rollback")
            .expect_err("missing CLOG entry must fail");
        assert!(
            err.to_string().contains("validate_prepared"),
            "unexpected error: {err}"
        );
        assert_eq!(session.state.two_phase.list_prepared().len(), 1);
    }

    #[test]
    fn rollback_reports_abort_failure_instead_of_success_tag() {
        let mut session = test_session();
        let txn = session
            .state
            .txn_manager
            .begin(IsolationLevel::ReadCommitted);
        let stale = txn.clone();
        session.state.txn_manager.abort(txn).expect("pre-abort");
        session.txn_state = TxnState::InTransaction(stale);

        let err = session
            .execute_rollback()
            .expect_err("stale rollback must fail");
        assert!(
            err.to_string().contains("ROLLBACK"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn explicit_rollback_does_not_append_abort_marker_for_unprepared_transaction() {
        let data_dir = tempfile::TempDir::new().expect("temp data dir");
        let server = Arc::new(Server::init(data_dir.path()).expect("persistent server"));
        let (io, _peer) = duplex(64);
        let mut session = Session::new(io, Arc::clone(&server), None);
        session.execute_begin(None, None).expect("begin");
        session
            .pending_table_modifications
            .insert("t".to_owned(), 1);

        session.execute_rollback().expect("rollback");
        drop(session);
        drop(server);

        let mut abort_records = 0usize;
        ultrasql_wal::recover(data_dir.path().join("pg_wal"), |record| {
            if record.header.record_type == ultrasql_wal::RecordType::Abort {
                abort_records += 1;
            }
            Ok(())
        })
        .expect("recover WAL");
        assert_eq!(abort_records, 0);
    }

    #[test]
    fn failed_commit_reports_abort_failure_instead_of_success_tag() {
        let mut session = test_session();
        let txn = session
            .state
            .txn_manager
            .begin(IsolationLevel::ReadCommitted);
        let stale = txn.clone();
        session.state.txn_manager.abort(txn).expect("pre-abort");
        session.txn_state = TxnState::Failed(stale);

        let err = session
            .execute_commit()
            .expect_err("stale failed-block commit must fail");
        assert!(
            err.to_string().contains("COMMIT"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn failed_prepare_reports_abort_failure_instead_of_success_tag() {
        let mut session = test_session();
        let txn = session
            .state
            .txn_manager
            .begin(IsolationLevel::ReadCommitted);
        let stale = txn.clone();
        session.state.txn_manager.abort(txn).expect("pre-abort");
        session.txn_state = TxnState::Failed(stale);

        let err = session
            .execute_prepare_transaction("stale-prepare")
            .expect_err("stale failed-block prepare must fail");
        assert!(
            err.to_string().contains("PREPARE TRANSACTION"),
            "unexpected error: {err}"
        );
    }
}
