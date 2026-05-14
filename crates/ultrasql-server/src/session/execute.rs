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
    /// Synchronous core of query execution: parse, bind, lower, run.
    ///
    /// Kept synchronous because none of the steps perform I/O. The
    /// async handler invokes this from the connection task; the
    /// executor's reactor stays responsive because the sample tables
    /// have a bounded fixed size.
    ///
    /// A [`CatalogSnapshot`] is acquired at the very start of execution
    /// via a wait-free `ArcSwap::load_full`.  All catalog lookups for the
    /// duration of this statement go through the snapshot so concurrent
    /// DDL cannot perturb an in-flight query.
    ///
    /// ## Transaction routing
    ///
    /// The session's [`TxnState`] determines how the statement is wrapped:
    ///
    /// - `Idle` — a fresh autocommit transaction is allocated, the
    ///   statement runs, and the transaction is committed on success
    ///   (or aborted on error). This is the legacy path.
    /// - `InTransaction(txn)` — the statement uses the existing
    ///   transaction. The session's `command_id` is advanced and the
    ///   `ReadCommitted` snapshot is refreshed. On error the session
    ///   transitions to `Failed(txn)`; subsequent statements until
    ///   `COMMIT`/`ROLLBACK` return SQLSTATE `25P02`.
    /// - `Failed(_)` — every non-transaction-control statement is
    ///   rejected with SQLSTATE `25P02`.
    ///
    /// Transaction-control statements (`BEGIN` / `COMMIT` / `ROLLBACK` /
    /// `SAVEPOINT` / `ROLLBACK TO` / `RELEASE`) are dispatched separately
    /// in [`Self::execute_txn_control`] so they can manipulate the
    /// session's `txn_state` directly.
    #[inline]
    pub(crate) fn execute_query(&mut self, sql: &str) -> Result<SelectResult, ServerError> {
        // Capture a per-statement catalog snapshot — wait-free arc-swap load.
        // The binder reads this snapshot first; if a name is not found there
        // (a runtime CREATE TABLE never landed it), the in-memory sample
        // catalog provides the legacy fallback.
        let catalog_snapshot: Arc<CatalogSnapshot> = self.state.catalog_snapshot();
        let combined = CombinedCatalog {
            snapshot: &catalog_snapshot,
            fallback: &self.state.catalog,
        };

        // Parser / binder errors inside an explicit transaction must
        // also transition us to `Failed` — PostgreSQL marks the block
        // as aborted regardless of whether the failure was at parse,
        // plan, or execute time. Handle that uniformly here.
        let stmt = match Parser::new(sql).parse_statement() {
            Ok(s) => s,
            Err(e) => return Err(self.fail_if_in_transaction(e.into())),
        };
        let plan = match bind(&stmt, &combined) {
            Ok(p) => p,
            Err(e) => return Err(self.fail_if_in_transaction(e.into())),
        };

        // Transaction-control statements own the session's TxnState.
        match &plan {
            LogicalPlan::Begin { .. }
            | LogicalPlan::Commit { .. }
            | LogicalPlan::Rollback { .. }
            | LogicalPlan::Savepoint { .. }
            | LogicalPlan::RollbackToSavepoint { .. }
            | LogicalPlan::ReleaseSavepoint { .. } => {
                return self.execute_txn_control(&plan);
            }
            _ => {}
        }

        // A statement issued while the explicit transaction has already
        // errored must be rejected with the standard PostgreSQL SQLSTATE
        // `25P02` until the user issues COMMIT/ROLLBACK.
        if matches!(self.txn_state, TxnState::Failed(_)) {
            return Err(ServerError::TransactionAborted);
        }

        // DDL is dispatched ahead of operator lowering: it never produces
        // rows, so the lowerer would only round-trip it through an
        // unreachable arm. DDL inside an explicit transaction is
        // rejected today because the catalog mutations are not
        // transactional under the v0.5 catalog (see AGENTS.md §11; a
        // follow-up RFC will add transactional DDL). The rejection
        // transitions the txn to `Failed` so subsequent statements get
        // SQLSTATE `25P02` until COMMIT/ROLLBACK.
        let is_ddl = matches!(
            &plan,
            LogicalPlan::CreateTable { .. }
                | LogicalPlan::CreateIndex { .. }
                | LogicalPlan::DropTable { .. }
                | LogicalPlan::AlterTable { .. }
                | LogicalPlan::Truncate { .. }
        );
        if is_ddl && matches!(self.txn_state, TxnState::InTransaction(_)) {
            return Err(self.fail_if_in_transaction(ServerError::Unsupported(
                "DDL inside an explicit transaction block is not yet supported",
            )));
        }
        match &plan {
            LogicalPlan::CreateTable { .. } => {
                return self.execute_create_table(&plan, &catalog_snapshot);
            }
            LogicalPlan::CreateIndex { .. } => {
                return self.execute_create_index(&plan, &catalog_snapshot);
            }
            LogicalPlan::DropTable { .. } => {
                return self.execute_drop_table(&plan);
            }
            LogicalPlan::AlterTable { .. } => {
                return self.execute_alter_table(&plan, &catalog_snapshot);
            }
            LogicalPlan::Truncate { .. } => {
                return self.execute_truncate(&plan, &catalog_snapshot);
            }
            _ => {}
        }

        // DML / SELECT path: route through the cost-based optimizer
        // before lowering. The cache key is the raw SQL text so a repeat
        // Simple Query — or an Extended Query Parse over the same string
        // — reuses the already-optimised plan. See
        // [`Self::optimize_dml_plan`] for the cache + invalidation
        // contract.
        //
        // Behaviour depends on TxnState. The `run_dml_or_select` helper
        // already transitions `InTransaction → Failed` on any execution
        // error, so no explicit `fail_if_in_transaction` is needed here.
        // Skip the optimizer + plan cache for trivial `INSERT VALUES`
        // plans. The cost-based optimizer has no rewrites that
        // apply to a leaf `Insert { source: Values }` shape, and
        // the plan-cache lookup hashes the entire SQL text — for a
        // 10 000-row bulk INSERT that is a ~150 KB hash on every
        // iteration (cross_compare_sql uses a unique table name
        // per iter so the cache always misses). Bypass is
        // INSERT-only — UPDATE / DELETE need the optimizer's
        // canonicalisation passes for the lowerer's
        // `build_filtered_tid_scan` shape contract.
        let optimised_plan = if Self::is_trivial_insert_values(&plan)
            || Self::is_fused_update_shape(&plan)
        {
            plan
        } else {
            match self.optimize_dml_plan(sql, plan, &catalog_snapshot) {
                Ok(p) => p,
                Err(e) => return Err(self.fail_if_in_transaction(e)),
            }
        };
        self.run_dml_or_select(&optimised_plan, &catalog_snapshot)
    }

    /// `true` iff `plan` is an `Update` whose source is a bare `Scan` or
    /// `Filter(Scan)` shape — the exact set of inputs that the fused
    /// UPDATE path (`try_build_fused_update`) recognises. The fused
    /// path does its own structural matching on the bound plan and
    /// does not depend on any optimizer rewrites, so when this
    /// predicate fires the optimizer's full pass over the plan is
    /// pure overhead and the per-iter plan-cache miss (the
    /// `cross_compare_sql` bench uses a unique table name per iter,
    /// so the SQL-text key never repeats) is also wasted.
    ///
    /// We deliberately keep this predicate loose: we test only the
    /// *outer* `Update`-over-(Scan | Filter(Scan)) structure here.
    /// `try_build_fused_update` re-validates every fine-grained
    /// precondition (schema is `(Int32, Int32)`, assignment is a
    /// linear `Column ± Int32 literal`, predicate is an Int32 column
    /// + Int32 literal compare) and falls back to the default
    /// `ModifyTable(Filter(SeqScan))` plan when any of them fails.
    /// The cost of the redundant validation is negligible compared
    /// to a missed optimizer pass.
    pub(crate) fn is_fused_update_shape(plan: &LogicalPlan) -> bool {
        let LogicalPlan::Update {
            input, returning, ..
        } = plan
        else {
            return false;
        };
        if !returning.is_empty() {
            return false;
        }
        matches!(
            input.as_ref(),
            LogicalPlan::Scan { .. }
                | LogicalPlan::Filter {
                    input: _,
                    predicate: _,
                }
        )
    }

    /// `true` iff `plan` is `Insert { source: Values { .. }, .. }`
    /// with no `ON CONFLICT` / `RETURNING` — see the call site for
    /// why this bypasses the optimizer + plan-cache lookup.
    pub(crate) fn is_trivial_insert_values(plan: &LogicalPlan) -> bool {
        let LogicalPlan::Insert {
            source,
            on_conflict,
            returning,
            ..
        } = plan
        else {
            return false;
        };
        if on_conflict.is_some() || !returning.is_empty() {
            return false;
        }
        matches!(source.as_ref(), LogicalPlan::Values { .. })
    }

    /// Apply the cost-based optimizer to a DML/SELECT plan and return
    /// the result.
    ///
    /// The optimised plan is cached in [`Server::plan_cache`] keyed on
    /// the raw `sql` text. A cache hit skips the rule-rewrite loop and
    /// returns the previously-optimised plan; a cache miss runs
    /// [`ultrasql_optimizer::optimize`] against the bound plan and
    /// stores the result. The cache is cleared whole-cloth by every DDL
    /// path (see [`Self::plan_cache_invalidate`]), so concurrent DDL
    /// cannot serve a stale plan.
    ///
    /// # Errors
    ///
    /// Wraps [`OptimizeError`] into [`ServerError::Plan`] via a synthetic
    /// `PlanError::Type` message because the optimizer's failure modes
    /// are all bind-time-quality (the binder already type-checked the
    /// plan, so a rule failure is an internal-invariant violation). The
    /// caller forwards the wrapped error through the normal
    /// `fail_if_in_transaction` machinery.
    pub(crate) fn optimize_dml_plan(
        &self,
        sql: &str,
        plan: LogicalPlan,
        catalog_snapshot: &Arc<CatalogSnapshot>,
    ) -> Result<LogicalPlan, ServerError> {
        let key = PlanCacheKey::named(sql.to_owned());
        let stats: NoStats = NoStats;
        let snapshot = Arc::clone(catalog_snapshot);
        // The closure is invoked only on cache miss; on a hit the cached
        // plan is returned and the plan we received here is dropped.
        // The closure consumes the plan via move because `FnOnce` does
        // not require `Clone` even though the underlying signature of
        // `PlanCache::get_or_plan` declares `FnOnce(&[Value])`.
        self.state
            .plan_cache
            .get_or_plan(&key, &[], move |_params| {
                ultrasql_optimizer::optimize(plan, &snapshot, &stats as &dyn StatsSource)
            })
            .map_err(|e| {
                ServerError::Plan(ultrasql_planner::PlanError::TypeMismatch(format!(
                    "optimizer failed: {e}"
                )))
            })
    }

    /// Clear the shared plan cache.
    ///
    /// Called from every DDL path after a successful catalog mutation
    /// so the next DML/SELECT statement re-plans against the new schema.
    /// The cache is keyed on SQL text, which has no relationship to the
    /// OIDs the DDL touched, so we invalidate everything; a finer-grained
    /// per-relation invalidation is a v0.7 follow-up.
    pub(crate) fn plan_cache_invalidate(&self) {
        self.state.plan_cache.invalidate_all();
    }

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
    ) -> Result<SelectResult, ServerError> {
        match std::mem::replace(&mut self.txn_state, TxnState::Idle) {
            TxnState::Idle => {
                let txn = self.state.txn_manager.begin(IsolationLevel::ReadCommitted);
                let outcome = run_plan_in_txn(
                    plan,
                    &txn,
                    Arc::clone(catalog_snapshot),
                    &self.state.tables,
                    Arc::clone(&self.state.heap),
                    Arc::clone(&self.state.txn_manager),
                );
                self.finalise_autocommit(txn, outcome)
            }
            TxnState::InTransaction(mut txn) => {
                self.state.txn_manager.refresh_snapshot(&mut txn);
                let outcome = run_plan_in_txn(
                    plan,
                    &txn,
                    Arc::clone(catalog_snapshot),
                    &self.state.tables,
                    Arc::clone(&self.state.heap),
                    Arc::clone(&self.state.txn_manager),
                );
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
    /// Logs (does not surface) txn manager errors so the original
    /// outcome reaches the client.
    pub(crate) fn finalise_autocommit(
        &self,
        txn: Transaction,
        outcome: Result<SelectResult, ServerError>,
    ) -> Result<SelectResult, ServerError> {
        match &outcome {
            Ok(_) => {
                if let Err(e) = self.state.txn_manager.commit(txn) {
                    tracing::warn!(error = %e, "autocommit failed to finalise");
                }
            }
            Err(_) => {
                // Roll back any in-place UPDATE writes by this txn
                // *before* terminating the CLOG entry, so the undo
                // log walker still sees the writer's XID.
                let xid = txn.xid;
                if let Err(e) = self.state.heap.rollback_in_place_updates(xid) {
                    tracing::warn!(error = %e, "in-place update rollback failed");
                }
                if let Err(abort_err) = self.state.txn_manager.abort(txn) {
                    tracing::warn!(error = %abort_err, "autocommit rollback failed");
                }
            }
        }
        outcome
    }

    /// If the session is currently `InTransaction`, transition to
    /// `Failed` so subsequent statements get the `25P02` rejection
    /// until COMMIT/ROLLBACK. This mirrors PostgreSQL: any failure
    /// inside a transaction block — including parser errors, bind
    /// errors, executor errors, and DDL-inside-tx rejections —
    /// aborts the block.
    ///
    /// Statements outside a transaction (Idle) and statements while
    /// already in a Failed block leave the state unchanged.
    ///
    /// Returns the original error verbatim so callers can `return`
    /// with a single line.
    pub(crate) fn fail_if_in_transaction(&mut self, err: ServerError) -> ServerError {
        if matches!(self.txn_state, TxnState::InTransaction(_)) {
            // Replace+match avoids needing to clone the Transaction
            // handle out of the variant.
            let prev = std::mem::replace(&mut self.txn_state, TxnState::Idle);
            if let TxnState::InTransaction(txn) = prev {
                self.txn_state = TxnState::Failed(txn);
            }
        }
        err
    }

}
