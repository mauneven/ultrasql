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
    bind,
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
            LogicalPlan::Begin { .. } => self.execute_begin(),
            LogicalPlan::Commit { .. } => self.execute_commit(),
            LogicalPlan::Rollback { .. } => self.execute_rollback(),
            LogicalPlan::Savepoint { name, .. } => self.execute_savepoint(name),
            LogicalPlan::RollbackToSavepoint { name, .. } => {
                self.execute_rollback_to_savepoint(name)
            }
            LogicalPlan::ReleaseSavepoint { name, .. } => self.execute_release_savepoint(name),
            _ => Err(ServerError::Unsupported(
                "execute_txn_control called with non-txn-control plan",
            )),
        }
    }

    pub(crate) fn execute_begin(&mut self) -> Result<SelectResult, ServerError> {
        let warn = match &self.txn_state {
            TxnState::Idle => {
                let txn = self.state.txn_manager.begin(IsolationLevel::ReadCommitted);
                self.txn_state = TxnState::InTransaction(txn);
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
                rows: 0,
            }),
            TxnState::InTransaction(txn) => {
                if let Err(e) = self.state.txn_manager.commit(txn) {
                    tracing::warn!(error = %e, "explicit COMMIT failed to finalise");
                } else {
                    self.state.note_commit_for_gc();
                }
                Ok(SelectResult {
                    messages: vec![BackendMessage::CommandComplete {
                        tag: "COMMIT".to_string(),
                    }],
                    streamed_body: None,
                    rows: 0,
                })
            }
            TxnState::Failed(txn) => {
                let xid = txn.xid;
                if let Err(e) = self.state.heap.rollback_in_place_updates(xid) {
                    tracing::warn!(error = %e, "in-place update rollback failed");
                }
                if let Err(e) = self.state.txn_manager.abort(txn) {
                    tracing::warn!(error = %e, "explicit COMMIT (treated as rollback) failed");
                }
                // PostgreSQL emits the ROLLBACK tag here, not COMMIT.
                Ok(SelectResult {
                    messages: vec![BackendMessage::CommandComplete {
                        tag: "ROLLBACK".to_string(),
                    }],
                    streamed_body: None,
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
                rows: 0,
            }),
            TxnState::InTransaction(txn) | TxnState::Failed(txn) => {
                let xid = txn.xid;
                if let Err(e) = self.state.heap.rollback_in_place_updates(xid) {
                    tracing::warn!(error = %e, "in-place update rollback failed");
                }
                if let Err(e) = self.state.txn_manager.abort(txn) {
                    tracing::warn!(error = %e, "explicit ROLLBACK failed");
                }
                Ok(SelectResult {
                    messages: vec![BackendMessage::CommandComplete {
                        tag: "ROLLBACK".to_string(),
                    }],
                    streamed_body: None,
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
                rows: 0,
            })
        } else {
            Err(self.fail_if_in_transaction(ServerError::SavepointNotFound(name.to_owned())))
        }
    }
}
