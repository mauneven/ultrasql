//! Part of the `session` module split. The
//! `impl<RW> Session<RW>` block is reopened here to add a handful
//! of methods to the type defined in `session/mod.rs`. Splitting
//! across files keeps every unit under the 600-line ceiling without
//! changing semantics.

#![allow(unused_imports)]

use std::sync::Arc;
use std::time::Instant;

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
use super::timeout::StatementTimeoutGuard;
use crate::error::ServerError;
use crate::extended;
use crate::pipeline::{self, LowerCtx, SampleTables};
use crate::result_encoder::{
    self, SelectResult, run_ddl_command, run_modify_command, run_select, run_select_streamed,
};
use crate::workload::{WorkloadQueryRecord, plan_hash_for_plan};
use crate::{
    BlankPageLoader, CombinedCatalog, Server, TxnState, decode_key_column, notice_warning,
    record_serializable_predicate_locks, record_serializable_write_conflicts, run_plan_in_txn,
};

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
        if self
            .extended
            .statements
            .get(name)
            .is_some_and(|stmt| !stmt.limit_offset_param_indexes.is_empty())
        {
            return Ok(());
        }
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
            stmt.plan_hash = plan_hash_for_plan(&optimised);
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
    pub(crate) async fn handle_execute(
        &mut self,
        portal: &str,
        max_rows: i32,
    ) -> Result<(), ServerError> {
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
        let workload_meta = self.extended.portals.get(portal).map(|p| {
            (
                p.sql.clone(),
                p.plan_hash,
                p.bind_param_count,
                p.bind_params_redacted,
            )
        });

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
                    | LogicalPlan::PrepareTransaction { .. }
                    | LogicalPlan::CommitPrepared { .. }
                    | LogicalPlan::RollbackPrepared { .. }
                    | LogicalPlan::SetTransaction { .. }
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
            // Pub-sub plans bypass the transaction system entirely.
            if matches!(
                plan,
                LogicalPlan::Listen { .. }
                    | LogicalPlan::Notify { .. }
                    | LogicalPlan::Unlisten { .. }
            ) {
                match self.execute_pubsub(plan) {
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
            if matches!(plan, LogicalPlan::SetVariable { .. }) {
                match self.execute_set_variable(plan, false) {
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
            if matches!(plan, LogicalPlan::SetRole { .. }) {
                match self.execute_set_role(plan) {
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
            // EXPLAIN: render the wrapped plan tree. Drop the leading
            // RowDescription because Extended Query delivers it via a
            // separate Describe message.
            if matches!(plan, LogicalPlan::Explain { .. }) {
                let catalog_snapshot = self.state.catalog_snapshot();
                match self.execute_explain(plan, &catalog_snapshot) {
                    Ok(result) => {
                        for m in &result.messages {
                            if matches!(m, ultrasql_protocol::BackendMessage::RowDescription { .. })
                            {
                                continue;
                            }
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
            // COPY needs the async wire flow (CopyData stream + CopyDone /
            // CopyFail). Route through the dedicated Extended dispatcher
            // which suppresses the trailing `ReadyForQuery` — the pipeline's
            // own `Sync` will emit one.
            if matches!(plan, LogicalPlan::Copy { .. }) {
                return self.handle_copy_statement_extended(plan).await;
            }
        }

        // A statement inside a failed transaction block is rejected
        // before we open any new resources.
        if matches!(self.txn_state, TxnState::Failed(_)) {
            let err = ServerError::TransactionAborted;
            self.extended.mark_failed();
            return self.send_error(&err.to_string(), err.sqlstate()).await;
        }

        if let Some(ref plan) = plan_clone
            && Self::is_ddl_plan(plan)
        {
            let catalog_snapshot = self.state.catalog_snapshot();
            let result = if matches!(self.txn_state, TxnState::InTransaction(_)) {
                Err(self.fail_if_in_transaction(ServerError::Unsupported(
                    "DDL inside an explicit transaction block is not yet supported",
                )))
            } else {
                self.execute_ddl_plan(plan, &catalog_snapshot)
            };
            match result {
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

        // Non-txn-control path: route through TxnState.
        let started = Instant::now();
        let timeout_guard =
            StatementTimeoutGuard::arm(self.statement_timeout_ms, self.cancel_flag.clone());
        let outcome = self.run_portal_routed(portal, max_rows);
        drop(timeout_guard);
        let elapsed = started.elapsed();
        if let Some((query, plan_hash, bind_param_count, bind_params_redacted)) = workload_meta {
            let rows = outcome
                .as_ref()
                .map_or(0, |out| Self::parse_command_rows_tag(&out.messages));
            let error = outcome.as_ref().err().map(ToString::to_string);
            self.log_completed_statement(&query, elapsed, rows, error.as_deref());
            self.state.workload_recorder.record(WorkloadQueryRecord {
                query,
                plan_hash,
                elapsed,
                rows,
                error,
                bind_param_count,
                bind_params_redacted,
            });
        }

        match outcome {
            Ok(out) => {
                for m in &out.messages {
                    self.send(m).await?;
                }
                if matches!(self.txn_state, TxnState::Idle) {
                    self.run_post_response_maintenance();
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
        let portal_plan = self
            .extended
            .portals
            .get(portal)
            .and_then(|p| p.plan.clone());
        match std::mem::replace(&mut self.txn_state, TxnState::Idle) {
            TxnState::Idle => {
                if let Some(plan) = portal_plan.as_ref() {
                    self.enforce_column_privileges(plan, &catalog_snapshot)?;
                }
                let txn = self.state.txn_manager.begin(IsolationLevel::ReadCommitted);
                let ctx = pipeline::LowerCtx {
                    tables: &self.state.tables,
                    catalog_snapshot: Arc::clone(&catalog_snapshot),
                    table_constraints: Arc::clone(&self.state.table_constraints),
                    sequences: Arc::clone(&self.state.sequences),
                    role_catalog: Arc::clone(&self.state.role_catalog),
                    privilege_catalog: Arc::clone(&self.state.privilege_catalog),
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
                    heap: Arc::clone(&self.state.heap),
                    vm: Arc::clone(&self.state.vm),
                    snapshot: txn.snapshot.clone(),
                    oracle: Arc::clone(&self.state.txn_manager),
                    xid: txn.current_xid(),
                    command_id: txn.current_command,
                    cte_buffers: std::collections::HashMap::new(),
                    jit: self.jit_config(),
                    cancel_flag: Some(self.cancel_flag.clone()),
                    work_mem: std::sync::Arc::new(ultrasql_executor::work_mem::WorkMemBudget::new(
                        u64::MAX,
                    )),
                    profile_operators: false,
                };
                if let Some(plan) = portal_plan.as_ref() {
                    record_serializable_predicate_locks(
                        plan,
                        &txn,
                        &catalog_snapshot,
                        self.state.txn_manager.as_ref(),
                    );
                    record_serializable_write_conflicts(
                        plan,
                        &txn,
                        &catalog_snapshot,
                        self.state.txn_manager.as_ref(),
                    );
                }
                let res =
                    crate::extended::execute_portal(&mut self.extended, portal, max_rows, &ctx);
                if res.is_ok() {
                    if portal_plan
                        .as_ref()
                        .and_then(|plan| Self::dml_target_table(plan))
                        .is_some()
                    {
                        if let Err(e) = self.state.validate_deferred_foreign_keys(&txn) {
                            let xid = txn.xid;
                            if let Err(rollback_err) =
                                self.state.heap.rollback_in_place_updates(xid)
                            {
                                tracing::warn!(
                                    error = %rollback_err,
                                    "in-place update rollback failed after deferred FK violation",
                                );
                            }
                            if let Err(abort_err) = self.state.txn_manager.abort(txn) {
                                tracing::warn!(
                                    error = %abort_err,
                                    "autocommit rollback failed after deferred FK violation (Extended Execute)",
                                );
                            }
                            return Err(e);
                        }
                    }
                    let is_dml = portal_plan
                        .as_ref()
                        .and_then(Self::dml_target_table)
                        .is_some();
                    if let Err(e) =
                        self.state
                            .commit_transaction(txn, is_dml, "Extended Execute autocommit")
                    {
                        if is_dml {
                            return Err(e);
                        }
                        tracing::warn!(error = %e, "autocommit failed to finalise (Extended Execute)");
                    } else {
                        self.state.note_commit_for_gc();
                        if let (Some(plan), Ok(outcome)) = (portal_plan.as_ref(), res.as_ref()) {
                            let rows = Self::parse_affected_rows_tag(&outcome.messages);
                            self.note_committed_dml_effect(plan, rows);
                            if rows > 0
                                && let Some(table) = Self::dml_target_table(plan)
                            {
                                self.maintain_aggregating_indexes_for_tables_after_commit(&[
                                    table.to_ascii_lowercase(),
                                ])?;
                            }
                        }
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
                if let Some(plan) = portal_plan.as_ref()
                    && let Err(e) = self.enforce_column_privileges(plan, &catalog_snapshot)
                {
                    self.txn_state = TxnState::Failed(txn);
                    return Err(e);
                }
                let ctx = pipeline::LowerCtx {
                    tables: &self.state.tables,
                    catalog_snapshot: Arc::clone(&catalog_snapshot),
                    table_constraints: Arc::clone(&self.state.table_constraints),
                    sequences: Arc::clone(&self.state.sequences),
                    role_catalog: Arc::clone(&self.state.role_catalog),
                    privilege_catalog: Arc::clone(&self.state.privilege_catalog),
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
                    heap: Arc::clone(&self.state.heap),
                    vm: Arc::clone(&self.state.vm),
                    snapshot: txn.snapshot.clone(),
                    oracle: Arc::clone(&self.state.txn_manager),
                    // Stamp writes with the *current* effective xid so
                    // SAVEPOINT-scoped INSERT/UPDATE/DELETE carry the
                    // subxact xid in xmin/xmax. ROLLBACK TO can then
                    // hide them by aborting that subxid in the CLOG.
                    xid: txn.current_xid(),
                    command_id: txn.current_command,
                    cte_buffers: std::collections::HashMap::new(),
                    jit: self.jit_config(),
                    cancel_flag: Some(self.cancel_flag.clone()),
                    work_mem: std::sync::Arc::new(ultrasql_executor::work_mem::WorkMemBudget::new(
                        u64::MAX,
                    )),
                    profile_operators: false,
                };
                if let Some(plan) = portal_plan.as_ref() {
                    record_serializable_predicate_locks(
                        plan,
                        &txn,
                        &catalog_snapshot,
                        self.state.txn_manager.as_ref(),
                    );
                    record_serializable_write_conflicts(
                        plan,
                        &txn,
                        &catalog_snapshot,
                        self.state.txn_manager.as_ref(),
                    );
                }
                let res =
                    crate::extended::execute_portal(&mut self.extended, portal, max_rows, &ctx);
                if let (Some(plan), Ok(outcome)) = (portal_plan.as_ref(), res.as_ref()) {
                    let rows = Self::parse_affected_rows_tag(&outcome.messages);
                    self.note_dml_effect(plan, rows);
                }
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

    /// Handle `Sync`. Drains any queued `LISTEN` notifications onto the
    /// wire and emits a `ReadyForQuery` carrying the session's
    /// current transaction state byte (`'I'` idle, `'T'` in a
    /// transaction block, `'E'` in a failed transaction block).
    pub(crate) async fn handle_sync(&mut self) -> Result<(), ServerError> {
        self.extended.reset_on_sync();
        // Compose the wire payload in a scratch buffer borrowed from the
        // session: notifications first, then `ReadyForQuery`, then a
        // single `write_all` + `flush`. Taking the buffer breaks the
        // mutable-borrow conflict between `self.write_buf` and the
        // `self.notify_rx.try_recv()` calls below.
        let mut scratch = std::mem::take(&mut self.write_buf);
        scratch.clear();
        // Notifications that arrived while the session was mid-pipeline
        // precede `ReadyForQuery` per the PostgreSQL convention so
        // libpq-style drivers route them via the async notification
        // callback before the next query is dispatched.
        self.drain_pending_notifications_into(&mut scratch);
        ultrasql_protocol::encode_backend(
            &BackendMessage::ReadyForQuery {
                status: self.txn_state.ready_for_query_status(),
            },
            &mut scratch,
        );
        let res = self.io.write_all(&scratch).await;
        scratch.clear();
        self.write_buf = scratch;
        res?;
        self.io.flush().await?;
        Ok(())
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
