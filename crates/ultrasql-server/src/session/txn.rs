//! Part of the `session` module split. The
//! `impl<RW> Session<RW>` block is reopened here to add a handful
//! of methods to the type defined in `session/mod.rs`. Splitting
//! across files keeps every unit under the 600-line ceiling without
//! changing semantics.

#![allow(unused_imports)]

use std::sync::Arc;

use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tracing::{debug, error, info, warn};
use ultrasql_catalog::{
    CatalogSnapshot, IndexEntry, MutableCatalog, PersistentCatalog, TableEntry,
};
use ultrasql_core::{DataType, PageId, RelationId, Value};
use ultrasql_optimizer::{NoStats, PlanCache, PlanCacheConfig, PlanCacheKey, StatsSource};
use ultrasql_parser::Parser;
use ultrasql_planner::{
    Catalog as PlannerCatalog, InMemoryCatalog, LogicalAlterTableAction, LogicalPlan, TableMeta,
    TxnIsolationLevel, bind,
};
use ultrasql_protocol::{BackendMessage, FrontendMessage, decode_frontend, encode_backend};
use ultrasql_storage::btree::BTree;
use ultrasql_storage::buffer_pool::{BufferPool, PageLoader};
use ultrasql_storage::heap::{DeleteOptions, HeapAccess, UpdateOptions};
use ultrasql_storage::page::Page;
use ultrasql_txn::{IsolationLevel, Transaction, TransactionManager};

use super::Session;
use crate::error::ServerError;
use crate::extended;
use crate::pipeline::{self, LowerCtx, SampleTables};
use crate::result_encoder::{
    self, SelectResult, run_ddl_command, run_modify_command, run_select, run_select_streamed,
};
use crate::{
    BlankPageLoader, CombinedCatalog, Server, TxnState, decode_key_column, notice_warning,
    run_plan_in_txn,
};

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
                isolation_level, ..
            } => self.execute_begin(*isolation_level),
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
                isolation_level, ..
            } => self.execute_set_transaction(*isolation_level),
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
                    let xid = txn.xid;
                    if let Err(rollback_err) = self.state.heap.rollback_in_place_updates(xid) {
                        tracing::warn!(
                            error = %rollback_err,
                            "in-place update rollback failed after deferred FK violation",
                        );
                    }
                    if let Err(abort_err) = self.state.txn_manager.abort(txn) {
                        tracing::warn!(
                            error = %abort_err,
                            "PREPARE TRANSACTION rollback failed after deferred FK violation",
                        );
                    }
                    self.clear_pending_dml_effects();
                    return Err(e);
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
                    rows: 0,
                })
            }
            TxnState::Failed(txn) => {
                self.state
                    .workload_recorder
                    .clear_session_transaction_start(self.pid);
                let xid = txn.xid;
                if let Err(e) = self.state.heap.rollback_in_place_updates(xid) {
                    tracing::warn!(error = %e, "in-place update rollback failed");
                }
                if let Err(e) = self.state.txn_manager.abort(txn) {
                    tracing::warn!(error = %e, "PREPARE TRANSACTION on failed block — abort failed");
                }
                self.clear_pending_dml_effects();
                Ok(SelectResult {
                    messages: vec![BackendMessage::CommandComplete {
                        tag: "ROLLBACK".to_string(),
                    }],
                    streamed_body: None,
                    shared_streamed_body: None,
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
        let xid = self
            .state
            .two_phase
            .commit_prepared(gid)
            .map_err(|e| ServerError::Ddl(format!("commit_prepared({gid}): {e}")))?;
        if let Err(e) = self
            .state
            .txn_manager
            .finalise_prepared(xid, ultrasql_mvcc::XidStatus::Committed)
        {
            tracing::warn!(error = %e, "finalise_prepared (committed) failed");
        } else {
            if let Some(commit_lsn) = self.state.append_commit_record(xid)? {
                self.state.wait_for_wal_durable(commit_lsn)?;
            }
            self.state.note_commit_for_gc();
        }
        Ok(SelectResult {
            messages: vec![BackendMessage::CommandComplete {
                tag: "COMMIT PREPARED".to_string(),
            }],
            streamed_body: None,
            shared_streamed_body: None,
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
        let xid = self
            .state
            .two_phase
            .rollback_prepared(gid)
            .map_err(|e| ServerError::Ddl(format!("rollback_prepared({gid}): {e}")))?;
        if let Err(e) = self.state.heap.rollback_in_place_updates(xid) {
            tracing::warn!(error = %e, "in-place update rollback failed for prepared txn");
        }
        if let Err(e) = self
            .state
            .txn_manager
            .finalise_prepared(xid, ultrasql_mvcc::XidStatus::Aborted)
        {
            tracing::warn!(error = %e, "finalise_prepared (aborted) failed");
        }
        Ok(SelectResult {
            messages: vec![BackendMessage::CommandComplete {
                tag: "ROLLBACK PREPARED".to_string(),
            }],
            streamed_body: None,
            shared_streamed_body: None,
            rows: 0,
        })
    }

    pub(crate) fn execute_begin(
        &mut self,
        level: Option<TxnIsolationLevel>,
    ) -> Result<SelectResult, ServerError> {
        let iso = match level {
            None | Some(TxnIsolationLevel::ReadCommitted) => IsolationLevel::ReadCommitted,
            Some(TxnIsolationLevel::RepeatableRead) => IsolationLevel::RepeatableRead,
            Some(TxnIsolationLevel::Serializable) => IsolationLevel::Serializable,
        };
        let warn = match &self.txn_state {
            TxnState::Idle => {
                let txn = self.state.txn_manager.begin(iso);
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
        level: TxnIsolationLevel,
    ) -> Result<SelectResult, ServerError> {
        let iso = match level {
            TxnIsolationLevel::ReadCommitted => IsolationLevel::ReadCommitted,
            TxnIsolationLevel::RepeatableRead => IsolationLevel::RepeatableRead,
            TxnIsolationLevel::Serializable => IsolationLevel::Serializable,
        };
        let mut messages: Vec<BackendMessage> = Vec::with_capacity(2);
        match &mut self.txn_state {
            TxnState::Idle => {
                messages.push(notice_warning(
                    "25P01",
                    "SET TRANSACTION ISOLATION LEVEL outside a transaction",
                ));
            }
            TxnState::InTransaction(txn) => {
                txn.isolation = iso;
                if iso == IsolationLevel::Serializable {
                    self.state.txn_manager.register_serializable(txn.xid);
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
                        let xid = txn.xid;
                        if let Err(rollback_err) = self.state.heap.rollback_in_place_updates(xid) {
                            tracing::warn!(
                                error = %rollback_err,
                                "in-place update rollback failed after deferred FK violation",
                            );
                        }
                        if let Err(abort_err) = self.state.txn_manager.abort(txn) {
                            tracing::warn!(
                                error = %abort_err,
                                "COMMIT rollback failed after deferred FK violation",
                            );
                        }
                        self.clear_pending_dml_effects();
                        return Err(e);
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
                        self.flush_pending_dml_effects();
                        return Err(e);
                    }
                    if let Err(e) =
                        self.maintain_materialized_views_for_tables_after_commit(&modified_tables)
                    {
                        self.flush_pending_dml_effects();
                        return Err(e);
                    }
                    self.flush_pending_materialized_view_rows();
                    self.flush_pending_dml_effects();
                }
                Ok(SelectResult {
                    messages: vec![BackendMessage::CommandComplete {
                        tag: "COMMIT".to_string(),
                    }],
                    streamed_body: None,
                    shared_streamed_body: None,
                    rows: 0,
                })
            }
            TxnState::Failed(txn) => {
                self.state
                    .workload_recorder
                    .clear_session_transaction_start(self.pid);
                let xid = txn.xid;
                if let Err(e) = self.state.heap.rollback_in_place_updates(xid) {
                    tracing::warn!(error = %e, "in-place update rollback failed");
                }
                if let Err(e) = self.state.txn_manager.abort(txn) {
                    tracing::warn!(error = %e, "explicit COMMIT (treated as rollback) failed");
                }
                self.clear_pending_dml_effects();
                // PostgreSQL emits the ROLLBACK tag here, not COMMIT.
                Ok(SelectResult {
                    messages: vec![BackendMessage::CommandComplete {
                        tag: "ROLLBACK".to_string(),
                    }],
                    streamed_body: None,
                    shared_streamed_body: None,
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
                rows: 0,
            }),
            TxnState::InTransaction(txn) | TxnState::Failed(txn) => {
                self.state
                    .workload_recorder
                    .clear_session_transaction_start(self.pid);
                let xid = txn.xid;
                if let Err(e) = self.state.heap.rollback_in_place_updates(xid) {
                    tracing::warn!(error = %e, "in-place update rollback failed");
                }
                if let Err(e) = self.state.txn_manager.abort(txn) {
                    tracing::warn!(error = %e, "explicit ROLLBACK failed");
                }
                self.clear_pending_dml_effects();
                Ok(SelectResult {
                    messages: vec![BackendMessage::CommandComplete {
                        tag: "ROLLBACK".to_string(),
                    }],
                    streamed_body: None,
                    shared_streamed_body: None,
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
                if self
                    .state
                    .txn_manager
                    .rollback_to_savepoint(&mut txn, name)
                    .is_ok()
                {
                    // Clear the failure flag: the rolled-back work is
                    // undone so the user can continue.
                    self.txn_state = TxnState::InTransaction(txn);
                    Ok(SelectResult {
                        messages: vec![BackendMessage::CommandComplete {
                            tag: "ROLLBACK".to_string(),
                        }],
                        streamed_body: None,
                        shared_streamed_body: None,
                        rows: 0,
                    })
                } else {
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
                rows: 0,
            })
        } else {
            Err(self.fail_if_in_transaction(ServerError::SavepointNotFound(name.to_owned())))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{DuplexStream, duplex};

    fn test_session() -> Session<DuplexStream> {
        let (io, _peer) = duplex(64);
        Session::new(io, Arc::new(Server::with_sample_database()))
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
            .execute_begin(Some(TxnIsolationLevel::Serializable))
            .expect("begin");
        assert!(matches!(session.txn_state, TxnState::InTransaction(_)));
        let nested = session.execute_begin(None).expect("nested begin warning");
        assert_eq!(last_tag(&nested), "BEGIN");
        assert_eq!(nested.messages.len(), 2);
        let set = session
            .execute_set_transaction(TxnIsolationLevel::RepeatableRead)
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

        session.execute_begin(None).expect("begin");
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

        session.execute_begin(None).expect("begin failed release");
        session.execute_savepoint("bad_release").expect("savepoint");
        assert!(session.execute_release_savepoint("missing").is_err());
        assert!(matches!(session.txn_state, TxnState::Failed(_)));
        assert!(session.execute_savepoint("after_failed").is_err());
        session.execute_rollback().expect("rollback failed block");

        session.execute_begin(None).expect("begin prepare commit");
        session
            .execute_prepare_transaction("commit-gid")
            .expect("prepare commit");
        let committed = session
            .execute_commit_prepared("commit-gid")
            .expect("commit prepared");
        assert_eq!(last_tag(&committed), "COMMIT PREPARED");

        session.execute_begin(None).expect("begin prepare rollback");
        session
            .execute_prepare_transaction("rollback-gid")
            .expect("prepare rollback");
        let rolled_back = session
            .execute_rollback_prepared("rollback-gid")
            .expect("rollback prepared");
        assert_eq!(last_tag(&rolled_back), "ROLLBACK PREPARED");
    }
}
