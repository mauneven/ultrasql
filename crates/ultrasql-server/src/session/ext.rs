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

use crate::error::ServerError;
use crate::extended;
use crate::pipeline::{self, LowerCtx, SampleTables};
use crate::result_encoder::{
    self, SelectResult, run_ddl_command, run_modify_command, run_select, run_select_streamed,
};
use crate::{
    BlankPageLoader, CombinedCatalog, Server, TxnState, notice_warning, run_plan_in_txn,
    decode_key_column,
};
use super::Session;

impl<RW> Session<RW>
where
    RW: AsyncRead + AsyncWrite + Unpin,
{
    /// Handle `Parse(name, sql, param_types)`.
    ///
    /// After [`crate::extended::handle_parse`] stores the bound plan, the same
    /// cost-based optimizer the Simple Query path runs is applied here
    /// so a subsequent `Execute` does not have to re-optimise. The
    /// optimised plan replaces the stored plan in `state.statements`.
    /// Parameter (`$N`) placeholders survive optimisation — rule-based
    /// rewrites are placeholder-aware (e.g., `ConstantFold` skips
    /// `ScalarExpr::Parameter`).
    ///
    /// The plan cache is shared with Simple Query: a Parse whose SQL
    /// text is already cached by a previous Simple Query hits the cache
    /// and skips the rule-rewrite loop.
    pub(crate) async fn handle_parse(
        &mut self,
        name: String,
        sql: String,
        param_types: Vec<u32>,
    ) -> Result<(), ServerError> {
        if self.extended.pipeline_failed {
            return Ok(());
        }
        // Capture a per-statement catalog snapshot — identical pattern
        // to `execute_query` so binding observes the same catalog the
        // forthcoming `Execute` will use. Plans are stored bound, not
        // re-bound at Execute time, so concurrent DDL between Parse and
        // Execute is invisible to the prepared statement (PostgreSQL
        // exhibits the same behaviour with `pg_proc` snapshotting).
        let catalog_snapshot: Arc<CatalogSnapshot> = self.state.catalog_snapshot();
        let combined = CombinedCatalog {
            snapshot: &catalog_snapshot,
            fallback: &self.state.catalog,
        };
        let parse_sql = sql.clone();
        let parse_name = name.clone();
        match crate::extended::handle_parse(&mut self.extended, name, sql, param_types, &combined) {
            Ok(msg) => {
                if let Err(e) =
                    self.optimize_parsed_plan(&parse_name, &parse_sql, &catalog_snapshot)
                {
                    if !e.is_query_scoped() {
                        return Err(e);
                    }
                    let e = self.fail_if_in_transaction(e);
                    self.extended.mark_failed();
                    return self.send_error(&e.to_string(), e.sqlstate()).await;
                }
                self.send(&msg).await
            }
            Err(e) => {
                if !e.is_query_scoped() {
                    return Err(e);
                }
                let e = self.fail_if_in_transaction(e);
                self.extended.mark_failed();
                self.send_error(&e.to_string(), e.sqlstate()).await
            }
        }
    }

    /// Run the optimizer + plan cache over the bound plan stored under
    /// `name`, replacing it with the optimised plan.
    ///
    /// DDL and transaction-control statements are skipped: those reach
    /// `Execute` through the direct-dispatch path in
    /// [`Self::handle_execute`] and the optimizer's rule pipeline does
    /// not target them.
    ///
    /// The SQL text drives the cache key so a `Parse` whose text already
    /// has a cached entry — primed by a prior Simple Query or a prior
    /// `Parse` of the same SQL — reuses the cached plan.
    ///
    /// # Errors
    ///
    /// Propagates errors from [`ultrasql_optimizer::optimize`] wrapped as
    /// [`ServerError::Plan`]. A query-scoped error fails just this
    /// Parse; an unrecoverable error propagates and the caller closes
    /// the session.
    pub(crate) fn optimize_parsed_plan(
        &mut self,
        name: &str,
        sql: &str,
        catalog_snapshot: &Arc<CatalogSnapshot>,
    ) -> Result<(), ServerError> {
        let bound_plan = match self.extended.statements.get(name) {
            Some(stmt) => match &stmt.plan {
                Some(p) => p.clone(),
                None => return Ok(()), // empty statement
            },
            None => return Ok(()),
        };
        let is_optimizable = matches!(
            &bound_plan,
            LogicalPlan::Scan { .. }
                | LogicalPlan::Filter { .. }
                | LogicalPlan::Project { .. }
                | LogicalPlan::Limit { .. }
                | LogicalPlan::Sort { .. }
                | LogicalPlan::Join { .. }
                | LogicalPlan::Aggregate { .. }
                | LogicalPlan::SetOp { .. }
                | LogicalPlan::Cte { .. }
                | LogicalPlan::Values { .. }
                | LogicalPlan::Insert { .. }
                | LogicalPlan::Update { .. }
                | LogicalPlan::Delete { .. }
                | LogicalPlan::Empty { .. }
        );
        if !is_optimizable {
            // DDL / transaction-control: the optimizer's rules do not
            // target these and the Execute path dispatches them around
            // the operator pipeline.
            return Ok(());
        }
        let optimised = self.optimize_dml_plan(sql, bound_plan, catalog_snapshot)?;
        if let Some(stmt) = self.extended.statements.get_mut(name) {
            stmt.plan = Some(optimised);
        }
        Ok(())
    }

    /// Handle `Bind(portal, statement, param_formats, params, result_formats)`.
    pub(crate) async fn handle_bind(
        &mut self,
        portal_name: String,
        statement_name: String,
        param_formats: Vec<i16>,
        params: Vec<Option<Vec<u8>>>,
        result_formats: Vec<i16>,
    ) -> Result<(), ServerError> {
        if self.extended.pipeline_failed {
            return Ok(());
        }
        let catalog_snapshot: Arc<CatalogSnapshot> = self.state.catalog_snapshot();
        let combined = CombinedCatalog {
            snapshot: &catalog_snapshot,
            fallback: &self.state.catalog,
        };
        match crate::extended::handle_bind(
            &mut self.extended,
            portal_name,
            &statement_name,
            &param_formats,
            &params,
            result_formats,
            Some(&combined),
        ) {
            Ok(msg) => self.send(&msg).await,
            Err(e) => {
                if !e.is_query_scoped() {
                    return Err(e);
                }
                let e = self.fail_if_in_transaction(e);
                self.extended.mark_failed();
                self.send_error(&e.to_string(), e.sqlstate()).await
            }
        }
    }

    /// Handle `Describe(kind, name)`.
    pub(crate) async fn handle_describe(
        &mut self,
        kind: ultrasql_protocol::DescribeKind,
        name: &str,
    ) -> Result<(), ServerError> {
        if self.extended.pipeline_failed {
            return Ok(());
        }
        let catalog_snapshot: Arc<CatalogSnapshot> = self.state.catalog_snapshot();
        let combined = CombinedCatalog {
            snapshot: &catalog_snapshot,
            fallback: &self.state.catalog,
        };
        let result = match kind {
            ultrasql_protocol::DescribeKind::Statement => {
                crate::extended::handle_describe_statement(&self.extended, name, Some(&combined))
            }
            ultrasql_protocol::DescribeKind::Portal => {
                crate::extended::handle_describe_portal(&self.extended, name).map(|m| vec![m])
            }
        };
        match result {
            Ok(msgs) => {
                for m in &msgs {
                    self.send(m).await?;
                }
                Ok(())
            }
            Err(e) => {
                if !e.is_query_scoped() {
                    return Err(e);
                }
                let e = self.fail_if_in_transaction(e);
                self.extended.mark_failed();
                self.send_error(&e.to_string(), e.sqlstate()).await
            }
        }
    }

    /// Handle `Execute(portal, max_rows)`. Runs the portal end-to-end
    /// using the same `lower_query` / executor path Simple Query uses,
    /// and routes the plan through the session's [`TxnState`] so an
    /// explicit BEGIN issued via Simple Query (or via a prior Extended
    /// Execute) keeps subsequent Executes inside the same transaction.
    ///
    /// Transaction-control plans (BEGIN / COMMIT / ROLLBACK / SAVEPOINT
    /// / ROLLBACK TO / RELEASE) are dispatched directly against the
    /// session's [`TxnState`] via [`Self::execute_txn_control`] —
    /// `execute_portal` never sees them.
    pub(crate) async fn handle_execute(&mut self, portal: &str, max_rows: i32) -> Result<(), ServerError> {
        if self.extended.pipeline_failed {
            return Ok(());
        }

        // Peek at the portal's plan up front: txn-control plans skip
        // `execute_portal` entirely so the session's TxnState owns the
        // transition. Cloning is cheap because the txn-control variants
        // carry only a `Schema::empty()` (and an optional savepoint name).
        let plan_clone = if let Some(p) = self.extended.portals.get(portal) {
            p.plan.clone()
        } else {
            let err = ServerError::Unsupported("Execute: portal not found");
            let err = self.fail_if_in_transaction(err);
            self.extended.mark_failed();
            return self.send_error(&err.to_string(), err.sqlstate()).await;
        };

        // Transaction-control plans take the dedicated TxnState dispatch.
        if let Some(ref plan) = plan_clone {
            if matches!(
                plan,
                LogicalPlan::Begin { .. }
                    | LogicalPlan::Commit { .. }
                    | LogicalPlan::Rollback { .. }
                    | LogicalPlan::Savepoint { .. }
                    | LogicalPlan::RollbackToSavepoint { .. }
                    | LogicalPlan::ReleaseSavepoint { .. }
            ) {
                match self.execute_txn_control(plan) {
                    Ok(result) => {
                        for m in &result.messages {
                            self.send(m).await?;
                        }
                        return Ok(());
                    }
                    Err(e) => {
                        if !e.is_query_scoped() {
                            return Err(e);
                        }
                        self.extended.mark_failed();
                        return self.send_error(&e.to_string(), e.sqlstate()).await;
                    }
                }
            }
        }

        // A statement inside a failed transaction block is rejected
        // before we open any new resources.
        if matches!(self.txn_state, TxnState::Failed(_)) {
            let err = ServerError::TransactionAborted;
            self.extended.mark_failed();
            return self.send_error(&err.to_string(), err.sqlstate()).await;
        }

        // Non-txn-control path: route through TxnState.
        let outcome = self.run_portal_routed(portal, max_rows);

        match outcome {
            Ok(out) => {
                for m in &out.messages {
                    self.send(m).await?;
                }
                Ok(())
            }
            Err(e) => {
                if !e.is_query_scoped() {
                    return Err(e);
                }
                self.extended.mark_failed();
                self.send_error(&e.to_string(), e.sqlstate()).await
            }
        }
    }

    /// Run a named portal under the current [`TxnState`].
    ///
    /// Mirrors [`Self::run_dml_or_select`] but drives the executor
    /// through `crate::extended::execute_portal` so the result-format codes
    /// the client supplied at Bind time are honoured.
    pub(crate) fn run_portal_routed(
        &mut self,
        portal: &str,
        max_rows: i32,
    ) -> Result<crate::extended::ExecuteOutcome, ServerError> {
        let catalog_snapshot: Arc<CatalogSnapshot> = self.state.catalog_snapshot();
        match std::mem::replace(&mut self.txn_state, TxnState::Idle) {
            TxnState::Idle => {
                let txn = self.state.txn_manager.begin(IsolationLevel::ReadCommitted);
                let ctx = pipeline::LowerCtx {
                    tables: &self.state.tables,
                    catalog_snapshot: Arc::clone(&catalog_snapshot),
                    heap: Arc::clone(&self.state.heap),
                    snapshot: txn.snapshot.clone(),
                    oracle: Arc::clone(&self.state.txn_manager),
                    xid: txn.xid,
                    command_id: txn.current_command,
                    cte_buffers: std::collections::HashMap::new(),
                };
                let res = crate::extended::execute_portal(&mut self.extended, portal, max_rows, &ctx);
                if res.is_ok() {
                    if let Err(e) = self.state.txn_manager.commit(txn) {
                        tracing::warn!(
                            error = %e,
                            "autocommit failed to finalise (Extended Execute)",
                        );
                    }
                } else if let Err(e) = self.state.txn_manager.abort(txn) {
                    tracing::warn!(
                        error = %e,
                        "autocommit rollback failed (Extended Execute)",
                    );
                }
                // txn_state stays Idle.
                res
            }
            TxnState::InTransaction(mut txn) => {
                self.state.txn_manager.refresh_snapshot(&mut txn);
                let ctx = pipeline::LowerCtx {
                    tables: &self.state.tables,
                    catalog_snapshot: Arc::clone(&catalog_snapshot),
                    heap: Arc::clone(&self.state.heap),
                    snapshot: txn.snapshot.clone(),
                    oracle: Arc::clone(&self.state.txn_manager),
                    xid: txn.xid,
                    command_id: txn.current_command,
                    cte_buffers: std::collections::HashMap::new(),
                };
                let res = crate::extended::execute_portal(&mut self.extended, portal, max_rows, &ctx);
                self.txn_state = if res.is_ok() {
                    TxnState::InTransaction(txn)
                } else {
                    TxnState::Failed(txn)
                };
                res
            }
            TxnState::Failed(txn) => {
                self.txn_state = TxnState::Failed(txn);
                Err(ServerError::TransactionAborted)
            }
        }
    }

    /// Handle `Sync`. Emits a `ReadyForQuery` carrying the session's
    /// current transaction state byte (`'I'` idle, `'T'` in a
    /// transaction block, `'E'` in a failed transaction block).
    pub(crate) async fn handle_sync(&mut self) -> Result<(), ServerError> {
        self.extended.reset_on_sync();
        self.send(&BackendMessage::ReadyForQuery {
            status: self.txn_state.ready_for_query_status(),
        })
        .await
    }

    /// Handle `Close(kind, name)`. Always emits `CloseComplete` even
    /// when the named object does not exist (per spec).
    pub(crate) async fn handle_extended_close(
        &mut self,
        kind: ultrasql_protocol::DescribeKind,
        name: &str,
    ) -> Result<(), ServerError> {
        if self.extended.pipeline_failed {
            return Ok(());
        }
        let msg = crate::extended::handle_close(&mut self.extended, kind, name);
        self.send(&msg).await
    }

    /// Handle `Flush`. Flush already happens inside `send`; this is a
    /// no-op on top of that.
    pub(crate) async fn handle_flush(&mut self) -> Result<(), ServerError> {
        self.io.flush().await?;
        Ok(())
    }

}
