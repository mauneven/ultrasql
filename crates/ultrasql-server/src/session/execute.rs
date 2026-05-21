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
    CatalogSnapshot, IndexEntry, MutableCatalog, PersistentCatalog, StatisticExtRow, TableEntry,
};
use ultrasql_core::{BlockNumber, DataType, PageId, RelationId, Value, Xid};
use ultrasql_mvcc::XidStatusOracle;
use ultrasql_optimizer::{
    InMemoryStatsCatalog, PlanCache, PlanCacheConfig, PlanCacheKey, StatsCatalog, StatsSource,
};
use ultrasql_parser::Parser;
use ultrasql_planner::{
    BinaryOp, Catalog as PlannerCatalog, InMemoryCatalog, LogicalAlterTableAction, LogicalPlan,
    LogicalSetVariableAction, ScalarExpr, TableMeta, bind,
};
use ultrasql_protocol::{
    BackendMessage, FieldDescription, FrontendMessage, decode_frontend, encode_backend,
};
use ultrasql_storage::access_method::{AccessMethod, BrinIndex};
use ultrasql_storage::btree::BTree;
use ultrasql_storage::buffer_pool::{BufferPool, PageLoader};
use ultrasql_storage::heap::{DeleteOptions, HeapAccess, UpdateOptions};
use ultrasql_storage::page::Page;
use ultrasql_txn::{IsolationLevel, Transaction, TransactionManager};

use super::{PendingLogicalChange, Session};
use crate::error::ServerError;
use crate::extended;
use crate::pipeline::{self, LowerCtx, SampleTables};
use crate::replication::LogicalChangeKind;
use crate::result_encoder::{
    self, SelectResult, run_ddl_command, run_modify_command, run_select, run_select_streamed,
};
use crate::{
    BlankPageLoader, CombinedCatalog, Server, TxnState, decode_key_column, notice_warning,
    run_plan_in_txn, try_run_cached_int32_pair_select, try_run_cached_scalar_aggregate_select,
};

#[derive(Debug)]
struct CreateStatisticsSpec {
    name: String,
    table: String,
    columns: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum LogicalReplicationDdl {
    CreatePublication { name: String, tables: Vec<String> },
    DropPublication { name: String, if_exists: bool },
    CreateSubscription,
    DropSubscription,
}

trait RlsPlanOptionExt {
    fn transpose_ok(self) -> Result<Option<LogicalPlan>, ServerError>;
}

impl RlsPlanOptionExt for Option<LogicalPlan> {
    fn transpose_ok(self) -> Result<Option<LogicalPlan>, ServerError> {
        Ok(self)
    }
}

fn bool_literal(value: bool) -> ScalarExpr {
    ScalarExpr::Literal {
        value: Value::Bool(value),
        data_type: DataType::Bool,
    }
}

fn combine_rls_predicates(mut predicates: Vec<ScalarExpr>, op: BinaryOp) -> Option<ScalarExpr> {
    let first = predicates.pop()?;
    Some(
        predicates
            .into_iter()
            .fold(first, |left, right| ScalarExpr::Binary {
                op,
                left: Box::new(left),
                right: Box::new(right),
                data_type: DataType::Bool,
            }),
    )
}

fn parse_bool_guc(value: &str) -> Result<bool, ServerError> {
    match value.to_ascii_lowercase().as_str() {
        "on" | "true" | "1" | "yes" => Ok(true),
        "off" | "false" | "0" | "no" => Ok(false),
        _ => Err(ServerError::Unsupported(
            "invalid boolean runtime parameter",
        )),
    }
}

fn starts_with_keyword_pair(sql: &str, first: &str, second: &str) -> bool {
    let mut words = sql.split_whitespace();
    words
        .next()
        .is_some_and(|word| word.eq_ignore_ascii_case(first))
        && words
            .next()
            .is_some_and(|word| word.eq_ignore_ascii_case(second))
}

fn split_first_token(input: &str) -> Result<(&str, &str), ServerError> {
    let input = input.trim();
    if input.is_empty() {
        return Err(ServerError::ddl("logical replication DDL requires a name"));
    }
    let end = input.find(char::is_whitespace).unwrap_or(input.len());
    Ok((&input[..end], input[end..].trim()))
}

fn parse_publication_tables(input: &str) -> Result<Vec<String>, ServerError> {
    let input = input.trim();
    if !input
        .get(.."FOR TABLE".len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("FOR TABLE"))
    {
        return Err(ServerError::ddl(
            "CREATE PUBLICATION currently supports FOR TABLE only",
        ));
    }
    let table_list = input["FOR TABLE".len()..].trim();
    let tables = table_list
        .split(',')
        .map(|table| table.trim().trim_matches('"').to_string())
        .filter(|table| !table.is_empty())
        .collect::<Vec<_>>();
    if tables.is_empty() {
        return Err(ServerError::ddl(
            "CREATE PUBLICATION requires at least one table",
        ));
    }
    Ok(tables)
}

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

        // Wire-level statement no-ops kept for PostgreSQL compatibility
        // while the real plumbing lands behind the same names:
        //
        // - `VACUUM [table]` (§3.2-aligned): manual vacuum surface for
        //   ORMs / migration tools; the autovacuum trigger is a
        //   follow-up.
        //
        // Each shim short-circuits before the parser to avoid forcing
        // every layer to grow new exhaustive arms today.
        let trimmed = sql.trim_start();
        let _query_span = tracing::info_span!(
            "sql.query",
            bytes = sql.len(),
            standby = self.state.is_standby_mode()
        )
        .entered();
        if let Some(function_name) = Self::try_parse_backup_function(trimmed) {
            return self.execute_backup_function(function_name);
        }
        if self.state.is_standby_mode() && !Self::hot_standby_allows(trimmed) {
            return Err(ServerError::Unsupported("hot standby is read-only"));
        }
        if let Some(table) = self.try_parse_analyze_target(trimmed) {
            return self.execute_analyze(table.as_deref());
        }
        if let Some(table) = self.try_parse_vacuum_target(trimmed) {
            return self.execute_vacuum(table.as_deref());
        }
        if let Some(result) = self.try_execute_logical_replication_ddl(trimmed)? {
            return Ok(result);
        }

        if let Some(spec) = Self::try_parse_create_statistics(trimmed)? {
            return self.execute_create_statistics(&catalog_snapshot, spec);
        }

        // Parse + bind cache lookup. The cache stores fully bound
        // [`LogicalPlan`] values keyed by the trimmed SQL text. A hit
        // skips both `Parser::parse_statement` and `bind(...)`. The
        // cache is flushed by every DDL hook (see
        // [`Self::plan_cache_invalidate`]) so a catalog change cannot
        // resurrect a stale binding.
        let cache_key = trimmed; // already trimmed at function entry
        let cached_plan = self.stmt_cache.borrow().get(cache_key).cloned();
        if let Some(plan_arc) = cached_plan {
            if matches!(self.txn_state, TxnState::Failed(_)) {
                return Err(ServerError::TransactionAborted);
            }
            // Fast path: plans that bypass the optimizer never mutate
            // the bound plan, so we can run them straight from the
            // shared `Arc<LogicalPlan>` without paying
            // `Arc::unwrap_or_clone`'s deep clone. The shared-OLAP
            // workloads on `cross_compare_sql` hit this branch every
            // iteration (the SQL key repeats and the lowered shape is
            // always `is_scalar_aggregate_shape`); the legacy clone
            // walked the entire `LogicalPlan` tree once per query.
            if Self::is_trivial_insert_values(&plan_arc)
                || Self::is_fused_update_shape(&plan_arc)
                || Self::is_scalar_aggregate_shape(&plan_arc)
            {
                return self.run_dml_or_select(&plan_arc, &catalog_snapshot);
            }
            let plan = Arc::unwrap_or_clone(plan_arc);
            return self.execute_bound_plan(plan, sql, catalog_snapshot);
        }

        // Parser / binder errors inside an explicit transaction must
        // also transition us to `Failed` — PostgreSQL marks the block
        // as aborted regardless of whether the failure was at parse,
        // plan, or execute time. Handle that uniformly here.
        let stmt = match Parser::new(sql).parse_statement() {
            Ok(s) => s,
            Err(e) => return Err(self.fail_if_in_transaction(e.into())),
        };

        // PREPARE / EXECUTE / DEALLOCATE manipulate the per-session
        // prepared-statement cache (the same `ExtendedConnState` the
        // Extended Query path owns). Dispatched here so the bind step
        // never sees them; the binder rejects these AST variants.
        if let Some(result) =
            self.try_dispatch_meta_statement(&stmt, Arc::clone(&catalog_snapshot))?
        {
            return Ok(result);
        }
        if let Some(result) = self.try_dispatch_sequence_select(&stmt)? {
            return Ok(result);
        }

        let combined = CombinedCatalog {
            snapshot: &catalog_snapshot,
            fallback: &self.state.catalog,
        };
        let plan = match bind(&stmt, &combined) {
            Ok(p) => p,
            Err(e) => return Err(self.fail_if_in_transaction(e.into())),
        };

        // Cache the bound plan for repeated identical SQL. Only true
        // DML / SELECT shapes are eligible. Txn-control, DDL, and
        // meta variants need to flow through the dispatchers that
        // own their state transitions and would mis-execute through
        // the cache-hit `execute_bound_plan` fast path. `INSERT` is
        // also skipped — its bound plan embeds the literal value
        // tuple, so a 10 000-row bulk INSERT would dump ~150 KB into
        // the cache per statement (and the bench harness uses a
        // unique table per iter, so the entry would never repeat).
        // Every remaining shape — including UPDATE / DELETE — is
        // cached because the entry is `Arc<LogicalPlan>` and the
        // hit-path clone is a cheap refcount bump.
        let cacheable = matches!(
            &plan,
            LogicalPlan::Project { .. }
                | LogicalPlan::Scan { .. }
                | LogicalPlan::Filter { .. }
                | LogicalPlan::Aggregate { .. }
                | LogicalPlan::Sort { .. }
                | LogicalPlan::Limit { .. }
                | LogicalPlan::Join { .. }
                | LogicalPlan::Window { .. }
                | LogicalPlan::Cte { .. }
                | LogicalPlan::SetOp { .. }
                | LogicalPlan::Values { .. }
                | LogicalPlan::Update { .. }
                | LogicalPlan::Delete { .. }
                | LogicalPlan::LockRows { .. }
                | LogicalPlan::FunctionScan { .. }
        );
        if cacheable {
            self.stmt_cache
                .borrow_mut()
                .insert(cache_key.to_string(), Arc::new(plan.clone()));
        }

        // Transaction-control statements own the session's TxnState.
        match &plan {
            LogicalPlan::Begin { .. }
            | LogicalPlan::Commit { .. }
            | LogicalPlan::Rollback { .. }
            | LogicalPlan::Savepoint { .. }
            | LogicalPlan::RollbackToSavepoint { .. }
            | LogicalPlan::ReleaseSavepoint { .. }
            | LogicalPlan::PrepareTransaction { .. }
            | LogicalPlan::CommitPrepared { .. }
            | LogicalPlan::RollbackPrepared { .. }
            | LogicalPlan::SetTransaction { .. } => {
                return self.execute_txn_control(&plan);
            }
            // LISTEN / NOTIFY / UNLISTEN are dispatched against the
            // shared `NotifyHub`; they do not touch the transaction
            // system. See `session/notify.rs`.
            LogicalPlan::Listen { .. }
            | LogicalPlan::Notify { .. }
            | LogicalPlan::Unlisten { .. } => {
                return self.execute_pubsub(&plan);
            }
            _ => {}
        }

        // A statement issued while the explicit transaction has already
        // errored must be rejected with the standard PostgreSQL SQLSTATE
        // `25P02` until the user issues COMMIT/ROLLBACK.
        if matches!(self.txn_state, TxnState::Failed(_)) {
            return Err(ServerError::TransactionAborted);
        }

        if matches!(&plan, LogicalPlan::SetVariable { .. }) {
            return self.execute_set_variable(&plan, true);
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
                | LogicalPlan::CreateMaterializedView { .. }
                | LogicalPlan::CreateIndex { .. }
                | LogicalPlan::CreatePolicy { .. }
                | LogicalPlan::CreateSequence { .. }
                | LogicalPlan::AlterSequence { .. }
                | LogicalPlan::DropSequence { .. }
                | LogicalPlan::Comment { .. }
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
            LogicalPlan::CreateMaterializedView { .. } => {
                return self.execute_create_materialized_view(&plan, &catalog_snapshot);
            }
            LogicalPlan::CreateIndex { .. } => {
                return self.execute_create_index(&plan, &catalog_snapshot);
            }
            LogicalPlan::CreatePolicy { .. } => {
                return self.execute_create_policy(&plan, &catalog_snapshot);
            }
            LogicalPlan::CreateSequence { .. } => {
                return self.execute_create_sequence(&plan);
            }
            LogicalPlan::AlterSequence { .. } => {
                return self.execute_alter_sequence(&plan);
            }
            LogicalPlan::DropSequence { .. } => {
                return self.execute_drop_sequence(&plan);
            }
            LogicalPlan::Comment { .. } => {
                return self.execute_comment(&plan, &catalog_snapshot);
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
            LogicalPlan::Explain { .. } => {
                return self.execute_explain(&plan, &catalog_snapshot);
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
        //
        // The `is_scalar_aggregate_shape` bypass mirrors the same
        // reasoning for `SELECT SUM/AVG/COUNT(*) FROM t [WHERE ...]`:
        // the optimizer's rewrite set has no rule that improves a leaf
        // scalar-aggregate plan, and the lowerer's
        // `try_lower_cached_scalar_aggregate_i32` / `try_lower_fused_filter_sum_int`
        // fast paths run directly against the bound shape. Bypassing
        // the optimizer drops the DashMap lookup + `LogicalPlan::clone`
        // pair from every iteration of `cross_compare_sql --workload
        // sum-scalar/avg-scalar/filter-sum`.
        let optimised_plan = if Self::is_trivial_insert_values(&plan)
            || Self::is_fused_update_shape(&plan)
            || Self::is_scalar_aggregate_shape(&plan)
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

    pub(crate) fn execute_set_variable(
        &mut self,
        plan: &LogicalPlan,
        include_row_description: bool,
    ) -> Result<SelectResult, ServerError> {
        let LogicalPlan::SetVariable {
            name,
            action,
            value,
            ..
        } = plan
        else {
            return Err(ServerError::Unsupported("execute_set_variable: wrong plan"));
        };
        match action {
            LogicalSetVariableAction::Set | LogicalSetVariableAction::SetLocal => {
                let Some(v) = value.as_deref() else {
                    return self.execute_set_variable_reset(name);
                };
                self.apply_session_variable(name, v)?;
                Ok(result_encoder::run_ddl_command("SET"))
            }
            LogicalSetVariableAction::Reset => self.execute_set_variable_reset(name),
            LogicalSetVariableAction::Show => {
                Ok(self.show_session_variable(name, include_row_description)?)
            }
        }
    }

    fn execute_set_variable_reset(&mut self, name: &str) -> Result<SelectResult, ServerError> {
        match name {
            "jit" => {
                self.jit_enabled = false;
                Ok(result_encoder::run_ddl_command("RESET"))
            }
            "jit_above_cost" => {
                self.jit_above_rows = ultrasql_vec::jit::DEFAULT_JIT_ABOVE_ROWS;
                Ok(result_encoder::run_ddl_command("RESET"))
            }
            "synchronous_commit" => Ok(result_encoder::run_ddl_command("RESET")),
            _ if name.contains('.') => {
                self.session_settings.remove(&name.to_ascii_lowercase());
                Ok(result_encoder::run_ddl_command("RESET"))
            }
            _ => Err(ServerError::Unsupported("unsupported runtime parameter")),
        }
    }

    fn apply_session_variable(&mut self, name: &str, value: &str) -> Result<(), ServerError> {
        match name {
            "jit" => {
                self.jit_enabled = parse_bool_guc(value)?;
                Ok(())
            }
            "jit_above_cost" => {
                let parsed = value
                    .parse::<usize>()
                    .map_err(|_| ServerError::Unsupported("invalid jit_above_cost"))?;
                self.jit_above_rows = parsed;
                Ok(())
            }
            "synchronous_commit" => match value.to_ascii_lowercase().as_str() {
                "on" | "off" | "local" | "remote_write" | "remote_apply" => Ok(()),
                _ => Err(ServerError::Unsupported("invalid synchronous_commit")),
            },
            _ if name.contains('.') => {
                self.session_settings
                    .insert(name.to_ascii_lowercase(), value.to_owned());
                Ok(())
            }
            _ => Err(ServerError::Unsupported("unsupported runtime parameter")),
        }
    }

    fn show_session_variable(
        &self,
        name: &str,
        include_row_description: bool,
    ) -> Result<SelectResult, ServerError> {
        let shown = match name {
            "jit" => {
                if self.jit_enabled {
                    "on".to_owned()
                } else {
                    "off".to_owned()
                }
            }
            "jit_above_cost" => self.jit_above_rows.to_string(),
            "synchronous_commit" => "on".to_owned(),
            _ if name.contains('.') => self
                .session_settings
                .get(&name.to_ascii_lowercase())
                .cloned()
                .unwrap_or_default(),
            _ => return Err(ServerError::Unsupported("unsupported runtime parameter")),
        };
        let mut messages = Vec::with_capacity(3);
        if include_row_description {
            messages.push(BackendMessage::RowDescription {
                fields: vec![FieldDescription {
                    name: name.to_owned(),
                    table_oid: 0,
                    col_attnum: 0,
                    type_oid: 25,
                    type_size: -1,
                    type_modifier: -1,
                    format_code: 0,
                }],
            });
        }
        messages.push(BackendMessage::DataRow {
            columns: vec![Some(shown.into_bytes())],
        });
        messages.push(BackendMessage::CommandComplete {
            tag: "SHOW".to_owned(),
        });
        Ok(SelectResult {
            messages,
            streamed_body: None,
            shared_streamed_body: None,
            rows: 1,
        })
    }

    /// Hot-path entry for a SQL string that has already been parsed +
    /// bound by an earlier `execute_query` call. The bound plan was
    /// cached in [`Self::stmt_cache`] so we skip the parser, binder,
    /// and (for the DML/SELECT shapes that survive the cache filter)
    /// the meta-statement and DDL dispatchers. The optimizer + lowerer
    /// run as usual; the optimizer's own `PlanCache` provides the
    /// second layer of memoisation.
    fn execute_bound_plan(
        &mut self,
        plan: LogicalPlan,
        sql: &str,
        catalog_snapshot: Arc<CatalogSnapshot>,
    ) -> Result<SelectResult, ServerError> {
        if matches!(self.txn_state, TxnState::Failed(_)) {
            return Err(ServerError::TransactionAborted);
        }
        let optimised_plan = if Self::is_trivial_insert_values(&plan)
            || Self::is_fused_update_shape(&plan)
            || Self::is_scalar_aggregate_shape(&plan)
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

    /// `true` iff `plan` is a trivial scalar aggregate over a bare
    /// `Scan` or `Filter(Scan)` shape — exactly the shapes that the
    /// pipeline lowerer routes through the column-cache fast path
    /// (`try_lower_cached_scalar_aggregate_i32` for pure SUM/AVG over
    /// an `Int32` column, `try_lower_fused_filter_sum_int` for the
    /// filtered SUM variant). The cost-based optimizer has no rule
    /// that rewrites a leaf scalar-aggregate plan into a cheaper
    /// equivalent, so the per-iter optimizer pass + plan-cache lookup
    /// pair is pure overhead on the
    /// `cross_compare_sql --workload sum-scalar/avg-scalar/filter-sum`
    /// hot path. The lowerer re-validates every fine-grained
    /// precondition before producing the fused operator; the
    /// predicate here only checks the outer envelope so we can bypass
    /// the optimizer cleanly.
    ///
    /// The binder wraps the aggregate node in an outer
    /// `LogicalPlan::Project` whose expressions are pure column
    /// references into the aggregate's output (one per aggregate output
    /// column — see `bind_select_body`). We accept that envelope so the
    /// fast path catches the `SELECT SUM(x) FROM t` plan as written.
    pub(crate) fn is_scalar_aggregate_shape(plan: &LogicalPlan) -> bool {
        // Strip an outer pass-through `Project` whose expressions are
        // column references into the aggregate's output. The binder
        // emits this envelope for every aggregate query (see
        // `bind_select_body`); peeling it lets the predicate match the
        // canonical bound shape directly.
        let agg_plan = match plan {
            LogicalPlan::Project { input, exprs, .. } => {
                let all_columns = exprs
                    .iter()
                    .all(|(e, _)| matches!(e, ultrasql_planner::ScalarExpr::Column { .. }));
                if !all_columns {
                    return false;
                }
                input.as_ref()
            }
            other => other,
        };

        let LogicalPlan::Aggregate {
            input,
            group_by,
            aggregates,
            ..
        } = agg_plan
        else {
            return false;
        };
        if !group_by.is_empty() || aggregates.len() != 1 {
            return false;
        }
        let agg = &aggregates[0];
        if agg.distinct {
            return false;
        }
        // Outer shape: bare Scan or Filter(Scan).
        match input.as_ref() {
            LogicalPlan::Scan { .. } => true,
            LogicalPlan::Filter {
                input: filter_input,
                ..
            } => matches!(filter_input.as_ref(), LogicalPlan::Scan { .. }),
            _ => false,
        }
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
        let stats = ServerStatsSource {
            stats_catalog: &self.state.stats_catalog,
        };
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
        self.stmt_cache.borrow_mut().clear();
    }

    fn apply_row_security(
        &self,
        plan: &LogicalPlan,
        catalog_snapshot: &CatalogSnapshot,
        command: crate::RuntimeRlsCommand,
    ) -> Result<Option<LogicalPlan>, ServerError> {
        match plan {
            LogicalPlan::Scan {
                table,
                schema,
                projection,
            } => self.rls_scan_plan(
                table,
                schema,
                projection.as_deref(),
                catalog_snapshot,
                command,
            ),
            LogicalPlan::Filter { input, predicate } => self
                .apply_row_security(input, catalog_snapshot, command)?
                .map(|input| LogicalPlan::Filter {
                    input: Box::new(input),
                    predicate: predicate.clone(),
                })
                .transpose_ok(),
            LogicalPlan::Project {
                input,
                exprs,
                schema,
            } => self
                .apply_row_security(input, catalog_snapshot, command)?
                .map(|input| LogicalPlan::Project {
                    input: Box::new(input),
                    exprs: exprs.clone(),
                    schema: schema.clone(),
                })
                .transpose_ok(),
            LogicalPlan::Limit { input, n, offset } => self
                .apply_row_security(input, catalog_snapshot, command)?
                .map(|input| LogicalPlan::Limit {
                    input: Box::new(input),
                    n: *n,
                    offset: *offset,
                })
                .transpose_ok(),
            LogicalPlan::Sort { input, keys } => self
                .apply_row_security(input, catalog_snapshot, command)?
                .map(|input| LogicalPlan::Sort {
                    input: Box::new(input),
                    keys: keys.clone(),
                })
                .transpose_ok(),
            LogicalPlan::Window {
                input,
                partition_by,
                order_by,
                func,
                output_name,
                schema,
            } => self
                .apply_row_security(input, catalog_snapshot, command)?
                .map(|input| LogicalPlan::Window {
                    input: Box::new(input),
                    partition_by: partition_by.clone(),
                    order_by: order_by.clone(),
                    func: func.clone(),
                    output_name: output_name.clone(),
                    schema: schema.clone(),
                })
                .transpose_ok(),
            LogicalPlan::Aggregate {
                input,
                group_by,
                aggregates,
                schema,
            } => self
                .apply_row_security(input, catalog_snapshot, command)?
                .map(|input| LogicalPlan::Aggregate {
                    input: Box::new(input),
                    group_by: group_by.clone(),
                    aggregates: aggregates.clone(),
                    schema: schema.clone(),
                })
                .transpose_ok(),
            LogicalPlan::Join {
                left,
                right,
                join_type,
                condition,
                schema,
            } => {
                let new_left = self.apply_row_security(left, catalog_snapshot, command)?;
                let new_right = self.apply_row_security(right, catalog_snapshot, command)?;
                if new_left.is_none() && new_right.is_none() {
                    return Ok(None);
                }
                Ok(Some(LogicalPlan::Join {
                    left: Box::new(new_left.unwrap_or_else(|| left.as_ref().clone())),
                    right: Box::new(new_right.unwrap_or_else(|| right.as_ref().clone())),
                    join_type: *join_type,
                    condition: condition.clone(),
                    schema: schema.clone(),
                }))
            }
            LogicalPlan::SetOp {
                op,
                quantifier,
                left,
                right,
                schema,
            } => {
                let new_left = self.apply_row_security(left, catalog_snapshot, command)?;
                let new_right = self.apply_row_security(right, catalog_snapshot, command)?;
                if new_left.is_none() && new_right.is_none() {
                    return Ok(None);
                }
                Ok(Some(LogicalPlan::SetOp {
                    op: *op,
                    quantifier: *quantifier,
                    left: Box::new(new_left.unwrap_or_else(|| left.as_ref().clone())),
                    right: Box::new(new_right.unwrap_or_else(|| right.as_ref().clone())),
                    schema: schema.clone(),
                }))
            }
            LogicalPlan::Cte {
                name,
                recursive,
                definition,
                body,
                schema,
            } => {
                let new_definition =
                    self.apply_row_security(definition, catalog_snapshot, command)?;
                let new_body = self.apply_row_security(body, catalog_snapshot, command)?;
                if new_definition.is_none() && new_body.is_none() {
                    return Ok(None);
                }
                Ok(Some(LogicalPlan::Cte {
                    name: name.clone(),
                    recursive: *recursive,
                    definition: Box::new(
                        new_definition.unwrap_or_else(|| definition.as_ref().clone()),
                    ),
                    body: Box::new(new_body.unwrap_or_else(|| body.as_ref().clone())),
                    schema: schema.clone(),
                }))
            }
            LogicalPlan::LockRows {
                input,
                strength,
                wait_policy,
                schema,
            } => self
                .apply_row_security(input, catalog_snapshot, command)?
                .map(|input| LogicalPlan::LockRows {
                    input: Box::new(input),
                    strength: *strength,
                    wait_policy: *wait_policy,
                    schema: schema.clone(),
                })
                .transpose_ok(),
            LogicalPlan::Insert {
                table,
                columns,
                source,
                on_conflict,
                returning,
                schema,
            } => self
                .apply_row_security(source, catalog_snapshot, crate::RuntimeRlsCommand::Select)?
                .map(|source| LogicalPlan::Insert {
                    table: table.clone(),
                    columns: columns.clone(),
                    source: Box::new(source),
                    on_conflict: on_conflict.clone(),
                    returning: returning.clone(),
                    schema: schema.clone(),
                })
                .transpose_ok(),
            LogicalPlan::Update {
                table,
                assignments,
                input,
                returning,
                schema,
            } => self
                .apply_row_security(input, catalog_snapshot, crate::RuntimeRlsCommand::Update)?
                .map(|input| LogicalPlan::Update {
                    table: table.clone(),
                    assignments: assignments.clone(),
                    input: Box::new(input),
                    returning: returning.clone(),
                    schema: schema.clone(),
                })
                .transpose_ok(),
            LogicalPlan::Delete {
                table,
                input,
                returning,
                schema,
            } => self
                .apply_row_security(input, catalog_snapshot, crate::RuntimeRlsCommand::Delete)?
                .map(|input| LogicalPlan::Delete {
                    table: table.clone(),
                    input: Box::new(input),
                    returning: returning.clone(),
                    schema: schema.clone(),
                })
                .transpose_ok(),
            LogicalPlan::Explain {
                analyze,
                format,
                input,
                schema,
            } => self
                .apply_row_security(input, catalog_snapshot, command)?
                .map(|input| LogicalPlan::Explain {
                    analyze: *analyze,
                    format: *format,
                    input: Box::new(input),
                    schema: schema.clone(),
                })
                .transpose_ok(),
            LogicalPlan::Copy {
                relation,
                input,
                columns,
                direction,
                source,
                format,
                delimiter,
                null_str,
                header,
                auto_detect,
                ignore_errors,
                max_errors,
                reject_table,
                schema,
            } => {
                let Some(input) = input else {
                    return Ok(None);
                };
                self.apply_row_security(input, catalog_snapshot, command)?
                    .map(|input| LogicalPlan::Copy {
                        relation: relation.clone(),
                        input: Some(Box::new(input)),
                        columns: columns.clone(),
                        direction: *direction,
                        source: source.clone(),
                        format: *format,
                        delimiter: *delimiter,
                        null_str: null_str.clone(),
                        header: *header,
                        auto_detect: *auto_detect,
                        ignore_errors: *ignore_errors,
                        max_errors: *max_errors,
                        reject_table: reject_table.clone(),
                        schema: schema.clone(),
                    })
                    .transpose_ok()
            }
            _ => Ok(None),
        }
    }

    fn rls_scan_plan(
        &self,
        table: &str,
        schema: &ultrasql_core::Schema,
        projection: Option<&[usize]>,
        catalog_snapshot: &CatalogSnapshot,
        command: crate::RuntimeRlsCommand,
    ) -> Result<Option<LogicalPlan>, ServerError> {
        let Some(entry) = catalog_snapshot.tables.get(table) else {
            return Ok(None);
        };
        let Some(runtime) = self.enabled_row_security(entry.oid) else {
            return Ok(None);
        };
        let predicate = self.rls_using_predicate(&runtime, command)?;
        let full_scan = LogicalPlan::Scan {
            table: table.to_owned(),
            schema: entry.schema.clone(),
            projection: None,
        };
        let filtered = LogicalPlan::Filter {
            input: Box::new(full_scan),
            predicate,
        };
        let Some(projection) = projection else {
            return Ok(Some(filtered));
        };
        let exprs = projection
            .iter()
            .map(|idx| {
                let field = entry.schema.fields().get(*idx).ok_or_else(|| {
                    ServerError::ddl(format!("RLS projection index {idx} out of bounds"))
                })?;
                Ok((
                    ScalarExpr::Column {
                        name: field.name.clone(),
                        index: *idx,
                        data_type: field.data_type.clone(),
                    },
                    field.name.clone(),
                ))
            })
            .collect::<Result<Vec<_>, ServerError>>()?;
        Ok(Some(LogicalPlan::Project {
            input: Box::new(filtered),
            exprs,
            schema: schema.clone(),
        }))
    }

    fn enabled_row_security(
        &self,
        table_oid: ultrasql_core::Oid,
    ) -> Option<Arc<crate::TableRowSecurity>> {
        let guard = self.state.row_security.get(&table_oid)?;
        let runtime = Arc::clone(guard.value());
        if runtime.enabled { Some(runtime) } else { None }
    }

    fn rls_using_predicate(
        &self,
        runtime: &crate::TableRowSecurity,
        command: crate::RuntimeRlsCommand,
    ) -> Result<ScalarExpr, ServerError> {
        let mut permissive = Vec::new();
        let mut restrictive = Vec::new();
        for policy in runtime
            .policies
            .iter()
            .filter(|policy| policy.command.applies_to(command))
        {
            let Some(expr) = policy.using.as_ref() else {
                continue;
            };
            match policy.permissiveness {
                crate::RuntimeRlsPermissiveness::Permissive => {
                    permissive.push(self.rls_tenant_predicate(expr)?);
                }
                crate::RuntimeRlsPermissiveness::Restrictive => {
                    restrictive.push(self.rls_tenant_predicate(expr)?);
                }
            }
        }
        let Some(mut predicate) = combine_rls_predicates(permissive, BinaryOp::Or) else {
            return Ok(bool_literal(false));
        };
        if let Some(restrictive) = combine_rls_predicates(restrictive, BinaryOp::And) {
            predicate = ScalarExpr::Binary {
                op: BinaryOp::And,
                left: Box::new(predicate),
                right: Box::new(restrictive),
                data_type: DataType::Bool,
            };
        }
        Ok(predicate)
    }

    fn rls_tenant_predicate(
        &self,
        expr: &crate::RuntimeTenantPolicyExpr,
    ) -> Result<ScalarExpr, ServerError> {
        let Some(value) = self
            .session_settings
            .get(&expr.setting_name.to_ascii_lowercase())
        else {
            return Ok(bool_literal(false));
        };
        Ok(ScalarExpr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(ScalarExpr::Column {
                name: expr.column_name.clone(),
                index: expr.column_index,
                data_type: DataType::Text { max_len: None },
            }),
            right: Box::new(ScalarExpr::Literal {
                value: Value::Text(value.clone()),
                data_type: DataType::Text { max_len: None },
            }),
            data_type: DataType::Bool,
        })
    }

    fn check_rls_insert_values(
        &self,
        plan: &LogicalPlan,
        catalog_snapshot: &CatalogSnapshot,
    ) -> Result<(), ServerError> {
        let LogicalPlan::Insert {
            table,
            columns,
            source,
            ..
        } = plan
        else {
            return Ok(());
        };
        let Some(entry) = catalog_snapshot.tables.get(table) else {
            return Ok(());
        };
        let Some(runtime) = self.enabled_row_security(entry.oid) else {
            return Ok(());
        };
        let mut permissive_checks = Vec::new();
        let mut restrictive_checks = Vec::new();
        for policy in runtime
            .policies
            .iter()
            .filter(|policy| policy.command.applies_to(crate::RuntimeRlsCommand::Insert))
        {
            let Some(check) = policy.with_check.as_ref().or(policy.using.as_ref()) else {
                continue;
            };
            match policy.permissiveness {
                crate::RuntimeRlsPermissiveness::Permissive => permissive_checks.push(check),
                crate::RuntimeRlsPermissiveness::Restrictive => restrictive_checks.push(check),
            }
        }
        let LogicalPlan::Values { rows, .. } = source.as_ref() else {
            return Err(ServerError::Unsupported(
                "RLS WITH CHECK for INSERT currently requires VALUES input",
            ));
        };
        for row in rows {
            let mut accepted = false;
            for check in &permissive_checks {
                if self.rls_insert_row_matches(check, columns, row)? {
                    accepted = true;
                    break;
                }
            }
            if accepted {
                for check in &restrictive_checks {
                    if !self.rls_insert_row_matches(check, columns, row)? {
                        accepted = false;
                        break;
                    }
                }
            }
            if !accepted {
                return Err(ultrasql_executor::ExecError::CheckViolation(
                    "row-level security policy".to_owned(),
                )
                .into());
            }
        }
        Ok(())
    }

    fn rls_insert_row_matches(
        &self,
        check: &crate::RuntimeTenantPolicyExpr,
        columns: &[usize],
        row: &[ScalarExpr],
    ) -> Result<bool, ServerError> {
        let Some(expected) = self
            .session_settings
            .get(&check.setting_name.to_ascii_lowercase())
        else {
            return Ok(false);
        };
        let row_idx = if columns.is_empty() {
            check.column_index
        } else {
            let Some(idx) = columns.iter().position(|col| *col == check.column_index) else {
                return Ok(false);
            };
            idx
        };
        let Some(expr) = row.get(row_idx) else {
            return Ok(false);
        };
        match expr {
            ScalarExpr::Literal {
                value: Value::Text(actual),
                ..
            } => Ok(actual == expected),
            ScalarExpr::Literal {
                value: Value::Null, ..
            } => Ok(false),
            _ => Err(ServerError::Unsupported(
                "RLS WITH CHECK currently requires literal tenant values",
            )),
        }
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
        let rls_plan =
            self.apply_row_security(plan, catalog_snapshot, crate::RuntimeRlsCommand::Select)?;
        let plan = rls_plan.as_ref().unwrap_or(plan);
        self.check_rls_insert_values(plan, catalog_snapshot)?;
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
        self.reject_non_append_materialized_view_source_write(plan)?;

        match std::mem::replace(&mut self.txn_state, TxnState::Idle) {
            TxnState::Idle => {
                let txn = self.state.txn_manager.begin(IsolationLevel::ReadCommitted);
                let outcome = run_plan_in_txn(
                    plan,
                    &txn,
                    Arc::clone(catalog_snapshot),
                    Arc::clone(&self.state.table_constraints),
                    Arc::clone(&self.state.sequences),
                    Arc::clone(&self.state.persistent_catalog),
                    Arc::clone(&self.state.time_partitions),
                    Arc::clone(&self.state.workload_recorder),
                    Some(self.sequence_state.clone()),
                    &self.state.tables,
                    Arc::clone(&self.state.heap),
                    Arc::clone(&self.state.vm),
                    Arc::clone(&self.state.txn_manager),
                    self.jit_config(),
                    Some(self.cancel_flag.clone()),
                    &mut self.write_buf,
                );
                self.finalise_autocommit(plan, txn, outcome)
            }
            TxnState::InTransaction(mut txn) => {
                self.state.txn_manager.refresh_snapshot(&mut txn);
                let outcome = run_plan_in_txn(
                    plan,
                    &txn,
                    Arc::clone(catalog_snapshot),
                    Arc::clone(&self.state.table_constraints),
                    Arc::clone(&self.state.sequences),
                    Arc::clone(&self.state.persistent_catalog),
                    Arc::clone(&self.state.time_partitions),
                    Arc::clone(&self.state.workload_recorder),
                    Some(self.sequence_state.clone()),
                    &self.state.tables,
                    Arc::clone(&self.state.heap),
                    Arc::clone(&self.state.vm),
                    Arc::clone(&self.state.txn_manager),
                    self.jit_config(),
                    Some(self.cancel_flag.clone()),
                    &mut self.write_buf,
                );
                if let Ok(result) = &outcome {
                    self.note_dml_effect(plan, result.rows);
                }
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
        &mut self,
        plan: &LogicalPlan,
        txn: Transaction,
        outcome: Result<SelectResult, ServerError>,
    ) -> Result<SelectResult, ServerError> {
        match outcome {
            Ok(result) => {
                let xid = txn.xid;
                let is_dml = Self::dml_target_table(plan).is_some();
                if is_dml {
                    if let Err(e) = self.state.validate_deferred_foreign_keys(&txn) {
                        if let Err(rollback_err) = self.state.heap.rollback_in_place_updates(xid) {
                            tracing::warn!(
                                error = %rollback_err,
                                "in-place update rollback failed after deferred FK violation",
                            );
                        }
                        if let Err(abort_err) = self.state.txn_manager.abort(txn) {
                            tracing::warn!(
                                error = %abort_err,
                                "autocommit rollback failed after deferred FK violation",
                            );
                        }
                        return Err(e);
                    }
                }
                if let Err(e) = self
                    .state
                    .commit_transaction(txn, is_dml, "autocommit statement")
                {
                    if is_dml {
                        return Err(e);
                    }
                    tracing::warn!(error = %e, "autocommit failed to finalise");
                } else {
                    self.pending_post_commit_maintenance = true;
                    let rows = result.rows;
                    let modified_table = (rows > 0)
                        .then(|| Self::dml_target_table(plan))
                        .flatten()
                        .map(str::to_ascii_lowercase);
                    self.note_dml_effect(plan, rows);
                    if let Some(table) = &modified_table {
                        self.maintain_aggregating_indexes_for_tables_after_commit(
                            std::slice::from_ref(table),
                        )?;
                    }
                    self.maintain_append_only_materialized_views_after_commit(plan)?;
                }
                Ok(result)
            }
            Err(e) => {
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
                Err(e)
            }
        }
    }

    fn try_parse_analyze_target(&self, trimmed_sql: &str) -> Option<Option<String>> {
        if trimmed_sql.len() < "analyze".len() || !trimmed_sql[..7].eq_ignore_ascii_case("analyze")
        {
            return None;
        }
        let rest = trimmed_sql[7..].trim();
        if rest.is_empty() || rest == ";" {
            return Some(None);
        }
        let ident = rest.trim_end_matches(';').trim();
        if ident.is_empty() {
            return Some(None);
        }
        // v0.6: support `ANALYZE` and `ANALYZE table_name`.
        if ident.split_whitespace().count() == 1 {
            return Some(Some(ident.trim_matches('"').to_ascii_lowercase()));
        }
        None
    }

    fn try_parse_vacuum_target(&self, trimmed_sql: &str) -> Option<Option<String>> {
        if trimmed_sql.len() < "vacuum".len() || !trimmed_sql[..6].eq_ignore_ascii_case("vacuum") {
            return None;
        }
        let rest = trimmed_sql[6..].trim();
        if rest.is_empty() || rest == ";" {
            return Some(None);
        }
        let ident = rest.trim_end_matches(';').trim();
        if ident.is_empty() {
            return Some(None);
        }
        if ident.split_whitespace().count() == 1 {
            return Some(Some(ident.trim_matches('"').to_ascii_lowercase()));
        }
        None
    }

    fn try_parse_create_statistics(
        trimmed_sql: &str,
    ) -> Result<Option<CreateStatisticsSpec>, ServerError> {
        let head = "create statistics";
        if trimmed_sql.len() < head.len() || !trimmed_sql[..head.len()].eq_ignore_ascii_case(head) {
            return Ok(None);
        }
        let rest = trimmed_sql[head.len()..].trim();
        let rest = rest.strip_suffix(';').unwrap_or(rest).trim();
        if rest.is_empty() {
            return Err(ServerError::ddl("malformed CREATE STATISTICS"));
        }
        let normalized = rest.replace(',', " , ");
        let tokens: Vec<&str> = normalized.split_whitespace().collect();
        if tokens.len() < 5 || !tokens[1].eq_ignore_ascii_case("on") {
            return Err(ServerError::ddl("malformed CREATE STATISTICS"));
        }
        let mut columns = Vec::new();
        let mut idx = 2;
        while idx < tokens.len() && !tokens[idx].eq_ignore_ascii_case("from") {
            if tokens[idx] != "," {
                columns.push(Self::fold_statistics_identifier(tokens[idx]));
            }
            idx += 1;
        }
        if columns.is_empty()
            || idx + 2 != tokens.len()
            || !tokens[idx].eq_ignore_ascii_case("from")
        {
            return Err(ServerError::ddl("malformed CREATE STATISTICS"));
        }
        Ok(Some(CreateStatisticsSpec {
            name: Self::fold_statistics_identifier(tokens[0]),
            table: Self::fold_statistics_identifier(tokens[idx + 1]),
            columns,
        }))
    }

    fn fold_statistics_identifier(ident: &str) -> String {
        ident.trim_matches('"').to_ascii_lowercase()
    }

    fn execute_create_statistics(
        &mut self,
        snapshot: &CatalogSnapshot,
        spec: CreateStatisticsSpec,
    ) -> Result<SelectResult, ServerError> {
        let table = snapshot.tables.get(&spec.table).ok_or_else(|| {
            self.fail_if_in_transaction(ServerError::Plan(
                ultrasql_planner::PlanError::TableNotFound(spec.table.clone()),
            ))
        })?;
        let mut stxkeys = Vec::with_capacity(spec.columns.len());
        for column in &spec.columns {
            let position = table
                .schema
                .fields()
                .iter()
                .position(|field| field.name.eq_ignore_ascii_case(column))
                .ok_or_else(|| {
                    self.fail_if_in_transaction(ServerError::Plan(
                        ultrasql_planner::PlanError::ColumnNotFound(column.clone()),
                    ))
                })?;
            stxkeys.push(
                i16::try_from(position.saturating_add(1)).map_err(|_| {
                    ServerError::ddl("CREATE STATISTICS table has too many columns")
                })?,
            );
        }
        let row = StatisticExtRow {
            oid: self.state.persistent_catalog.next_oid(),
            stxname: spec.name,
            stxrelid: table.oid,
            stxkeys,
            stxkind: vec!['d', 'f', 'm'],
        };
        self.state
            .persistent_catalog
            .create_statistic_ext(row.clone())
            .map_err(ServerError::Catalog)?;
        let catalog_txn = self
            .state
            .txn_manager
            .begin(ultrasql_txn::IsolationLevel::ReadCommitted);
        if let Err(e) = self.state.persistent_catalog.persist_statistic_ext_row(
            &row,
            self.state.heap.as_ref(),
            catalog_txn.xid,
            catalog_txn.current_command,
        ) {
            if let Err(abort_err) = self.state.txn_manager.abort(catalog_txn) {
                tracing::warn!(
                    error = %abort_err,
                    "CREATE STATISTICS catalog transaction abort failed",
                );
            }
            return Err(ServerError::Catalog(e));
        }
        self.state.commit_transaction(
            catalog_txn,
            true,
            "CREATE STATISTICS catalog transaction",
        )?;
        self.plan_cache_invalidate();
        Ok(result_encoder::SelectResult {
            messages: vec![BackendMessage::CommandComplete {
                tag: "CREATE STATISTICS".to_string(),
            }],
            streamed_body: None,
            shared_streamed_body: None,
            rows: 0,
        })
    }

    fn execute_vacuum(&mut self, table: Option<&str>) -> Result<SelectResult, ServerError> {
        let snapshot = self.state.catalog_snapshot();
        let tables: Vec<TableEntry> = match table {
            Some(name) => vec![snapshot.tables.get(name).cloned().ok_or_else(|| {
                self.fail_if_in_transaction(ServerError::Plan(
                    ultrasql_planner::PlanError::TableNotFound(name.to_string()),
                ))
            })?],
            None => snapshot.tables.values().cloned().collect(),
        };
        let oldest = self.state.txn_manager.oldest_in_progress();
        for entry in tables {
            self.vacuum_one_table_indexes(&snapshot, &entry, oldest)?;
            let rel = RelationId(entry.oid);
            self.state
                .heap
                .vacuum_heap(rel, oldest, self.state.txn_manager.as_ref())
                .map_err(|e| ServerError::ddl(format!("VACUUM heap: {e}")))?;
            self.state.vacuum_mark_visible_pages(oldest);
            self.resummarize_brin_indexes(&snapshot, &entry)?;
            self.maintain_aggregating_indexes_for_tables_after_commit(std::slice::from_ref(
                &entry.name,
            ))?;
        }
        Ok(result_encoder::SelectResult {
            messages: vec![BackendMessage::CommandComplete {
                tag: "VACUUM".to_string(),
            }],
            streamed_body: None,
            shared_streamed_body: None,
            rows: 0,
        })
    }

    fn vacuum_one_table_indexes(
        &self,
        snapshot: &CatalogSnapshot,
        entry: &TableEntry,
        oldest: Xid,
    ) -> Result<(), ServerError> {
        let Some(indexes) = snapshot.indexes_by_table.get(&entry.oid) else {
            return Ok(());
        };
        for index in indexes {
            if let Some(hnsw) =
                self.state
                    .table_constraints
                    .get(&entry.oid)
                    .and_then(|constraints| {
                        let metadata = constraints.indexes.get(&index.oid)?;
                        (metadata.method == ultrasql_planner::LogicalIndexMethod::Hnsw)
                            .then(|| metadata.hnsw.clone())
                            .flatten()
                    })
            {
                hnsw.vacuum_deleted_logged(oldest, self.state.heap.wal_sink().map(Arc::as_ref))
                    .map_err(|e| ServerError::ddl(format!("VACUUM HNSW {}: {e}", index.name)))?;
                continue;
            }
            if let Some(ivfflat) =
                self.state
                    .table_constraints
                    .get(&entry.oid)
                    .and_then(|constraints| {
                        let metadata = constraints.indexes.get(&index.oid)?;
                        (metadata.method == ultrasql_planner::LogicalIndexMethod::IvfFlat)
                            .then(|| metadata.ivfflat.clone())
                            .flatten()
                    })
            {
                ivfflat
                    .compact_deleted_logged(oldest, self.state.heap.wal_sink().map(Arc::as_ref))
                    .map_err(|e| ServerError::ddl(format!("VACUUM IVFFlat {}: {e}", index.name)))?;
                continue;
            }
            if index.root_block == BlockNumber::INVALID {
                continue;
            }
            let btree = BTree::open(
                Arc::clone(self.state.heap.buffer_pool()),
                RelationId::new(index.oid.raw()),
                index.root_block,
            );
            btree
                .vacuum(|tid| {
                    let Ok(tuple) = self.state.heap.fetch(tid) else {
                        return true;
                    };
                    let xmax = tuple.header.xmax;
                    !xmax.is_invalid() && xmax < oldest && self.state.txn_manager.is_committed(xmax)
                })
                .map_err(|e| ServerError::ddl(format!("VACUUM index {}: {e}", index.name)))?;
        }
        Ok(())
    }

    fn resummarize_brin_indexes(
        &self,
        snapshot: &CatalogSnapshot,
        entry: &TableEntry,
    ) -> Result<(), ServerError> {
        let Some(indexes) = snapshot.indexes_by_table.get(&entry.oid) else {
            return Ok(());
        };
        let Some(constraints) = self.state.table_constraints.get(&entry.oid) else {
            return Ok(());
        };
        let brin_indexes: Vec<_> = indexes
            .iter()
            .filter_map(|index| {
                let metadata = constraints.indexes.get(&index.oid)?;
                if metadata.method != ultrasql_planner::LogicalIndexMethod::Brin {
                    return None;
                }
                let brin = metadata.brin.clone()?;
                Some((index.clone(), metadata.clone(), brin))
            })
            .collect();
        drop(constraints);
        if brin_indexes.is_empty() {
            return Ok(());
        }

        let rel = RelationId(entry.oid);
        let block_count = self.state.heap.block_count(rel).max(entry.n_blocks);
        if block_count == 0 {
            for (_, _, brin) in brin_indexes {
                brin.clear_summaries();
            }
            return Ok(());
        }

        let txn = self
            .state
            .txn_manager
            .begin(ultrasql_txn::IsolationLevel::ReadCommitted);
        let result = (|| -> Result<(), ServerError> {
            for (index, metadata, brin) in &brin_indexes {
                let columns: Vec<usize> = index
                    .columns
                    .iter()
                    .map(|attnum| usize::from(*attnum))
                    .collect();
                let encoding = if metadata.key_exprs.is_empty() {
                    crate::index_key::IndexKeyEncoding::for_columns(&entry.schema, &columns)?
                } else {
                    let [expr] = metadata.key_exprs.as_slice() else {
                        return Err(ServerError::Unsupported(
                            "CREATE INDEX: expression indexes support exactly one key in this wave",
                        ));
                    };
                    crate::index_key::IndexKeyEncoding::for_data_type(&expr.data_type())?
                };
                brin.clear_summaries();
                let scan = self.state.heap.scan_visible(
                    rel,
                    block_count,
                    &txn.snapshot,
                    self.state.txn_manager.as_ref(),
                );
                for tuple in scan {
                    let tuple = tuple
                        .map_err(|e| ServerError::ddl(format!("VACUUM BRIN heap scan: {e}")))?;
                    let key = crate::decode_key_column(
                        &tuple.data,
                        &entry.schema,
                        columns.first().copied(),
                        &metadata.key_exprs,
                        metadata.predicate.as_ref(),
                        metadata.method,
                        &encoding,
                    )?;
                    if let Some(key) = key {
                        let brin_key = BrinIndex::encode_i64_key(key);
                        brin.insert(&brin_key, tuple.tid).map_err(|e| {
                            ServerError::ddl(format!("VACUUM BRIN summarize {}: {e}", index.name))
                        })?;
                    }
                }
            }
            Ok(())
        })();
        if let Err(e) = self.state.txn_manager.commit(txn) {
            tracing::warn!(error = %e, "autocommit (VACUUM BRIN summarize) failed to finalise");
        }
        result
    }

    fn execute_analyze(&mut self, table: Option<&str>) -> Result<SelectResult, ServerError> {
        match table {
            Some(t) => {
                if !self.state.analyze_table(t)? {
                    return Err(self.fail_if_in_transaction(ServerError::Plan(
                        ultrasql_planner::PlanError::TableNotFound(t.to_string()),
                    )));
                }
            }
            None => {
                let snapshot = self.state.catalog_snapshot();
                let tables: Vec<String> = snapshot.tables.keys().map(|k| k.to_string()).collect();
                for name in tables {
                    let _ = self.state.analyze_table(&name);
                }
            }
        }
        Ok(result_encoder::SelectResult {
            messages: vec![BackendMessage::CommandComplete {
                tag: "ANALYZE".to_string(),
            }],
            streamed_body: None,
            shared_streamed_body: None,
            rows: 0,
        })
    }

    pub(crate) fn maintain_append_only_materialized_views(
        &mut self,
        plan: &LogicalPlan,
        txn: &Transaction,
    ) -> Result<Vec<(Arc<crate::MaterializedViewRuntime>, u64)>, ServerError> {
        let LogicalPlan::Insert { table, .. } = plan else {
            return Ok(Vec::new());
        };
        let views = self.materialized_views_for_source(table);
        self.materialize_view_deltas(views, txn)
    }

    fn materialized_views_for_source(
        &self,
        table: &str,
    ) -> Vec<Arc<crate::MaterializedViewRuntime>> {
        let folded = table.to_ascii_lowercase();
        self.state
            .materialized_views
            .iter()
            .filter_map(|entry| {
                let view = entry.value();
                if view.source_table == folded {
                    Some(Arc::clone(view))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
    }

    fn reject_non_append_materialized_view_source_write(
        &self,
        plan: &LogicalPlan,
    ) -> Result<(), ServerError> {
        let table = match plan {
            LogicalPlan::Update { table, .. } | LogicalPlan::Delete { table, .. } => table,
            _ => return Ok(()),
        };
        if self.materialized_views_for_source(table).is_empty() {
            return Ok(());
        }
        Err(ServerError::Unsupported(
            "UPDATE/DELETE on append-only materialized view source is not supported",
        ))
    }

    fn materialize_view_deltas(
        &mut self,
        views: Vec<Arc<crate::MaterializedViewRuntime>>,
        txn: &Transaction,
    ) -> Result<Vec<(Arc<crate::MaterializedViewRuntime>, u64)>, ServerError> {
        let mut materialized_rows = Vec::with_capacity(views.len());
        for view in views {
            let rows = self.materialize_view_delta(&view, txn)?;
            if rows > 0 {
                materialized_rows.push((view, rows));
            }
        }
        Ok(materialized_rows)
    }

    pub(crate) fn maintain_append_only_materialized_views_after_commit(
        &mut self,
        plan: &LogicalPlan,
    ) -> Result<(), ServerError> {
        let LogicalPlan::Insert { .. } = plan else {
            return Ok(());
        };
        let txn = self.state.txn_manager.begin(IsolationLevel::ReadCommitted);
        let rows = match self.maintain_append_only_materialized_views(plan, &txn) {
            Ok(rows) => rows,
            Err(e) => {
                if let Err(abort_err) = self.state.txn_manager.abort(txn) {
                    tracing::warn!(
                        error = %abort_err,
                        "materialized-view maintenance rollback failed",
                    );
                }
                return Err(e);
            }
        };
        if let Err(commit_err) = self.state.txn_manager.commit(txn) {
            tracing::warn!(
                error = %commit_err,
                "materialized-view maintenance commit failed",
            );
        }
        self.pending_materialized_view_rows.extend(rows);
        self.flush_pending_materialized_view_rows();
        Ok(())
    }

    pub(crate) fn maintain_materialized_views_for_tables_after_commit(
        &mut self,
        tables: &[String],
    ) -> Result<(), ServerError> {
        if tables.is_empty() {
            return Ok(());
        }
        let txn = self.state.txn_manager.begin(IsolationLevel::ReadCommitted);
        let mut rows = Vec::new();
        for table in tables {
            let views = self.materialized_views_for_source(table);
            match self.materialize_view_deltas(views, &txn) {
                Ok(mut view_rows) => rows.append(&mut view_rows),
                Err(e) => {
                    if let Err(abort_err) = self.state.txn_manager.abort(txn) {
                        tracing::warn!(
                            error = %abort_err,
                            "materialized-view maintenance rollback failed",
                        );
                    }
                    return Err(e);
                }
            }
        }
        if let Err(commit_err) = self.state.txn_manager.commit(txn) {
            tracing::warn!(
                error = %commit_err,
                "materialized-view maintenance commit failed",
            );
        }
        self.pending_materialized_view_rows.extend(rows);
        self.flush_pending_materialized_view_rows();
        Ok(())
    }

    pub(crate) fn maintain_aggregating_indexes_for_tables_after_commit(
        &mut self,
        tables: &[String],
    ) -> Result<(), ServerError> {
        if tables.is_empty() {
            return Ok(());
        }
        let snapshot = self.state.catalog_snapshot();
        let entries = tables
            .iter()
            .filter_map(|table| {
                let entry = snapshot.tables.get(&table.to_ascii_lowercase()).cloned()?;
                let has_aggregating_index = self
                    .state
                    .table_constraints
                    .get(&entry.oid)
                    .is_some_and(|constraints| {
                        constraints
                            .indexes
                            .values()
                            .any(|metadata| metadata.aggregating.is_some())
                    });
                has_aggregating_index.then_some(entry)
            })
            .collect::<Vec<_>>();
        if entries.is_empty() {
            return Ok(());
        }
        let txn = self
            .state
            .txn_manager
            .begin(ultrasql_txn::IsolationLevel::ReadCommitted);
        let result = (|| -> Result<(), ServerError> {
            for entry in &entries {
                crate::aggregating_index::refresh_dirty_aggregating_indexes(
                    entry,
                    &self.state.table_constraints,
                    self.state.heap.as_ref(),
                    &txn.snapshot,
                    self.state.txn_manager.as_ref(),
                )?;
            }
            Ok(())
        })();
        if let Err(commit_err) = self.state.txn_manager.commit(txn) {
            tracing::warn!(
                error = %commit_err,
                "aggregating-index maintenance commit failed",
            );
        }
        result
    }

    pub(crate) fn materialize_view_delta(
        &mut self,
        view: &Arc<crate::MaterializedViewRuntime>,
        txn: &Transaction,
    ) -> Result<u64, ServerError> {
        let committed = view
            .materialized_rows
            .load(std::sync::atomic::Ordering::Acquire);
        let pending = self
            .pending_materialized_view_rows
            .iter()
            .filter(|(pending_view, _)| pending_view.view_table == view.view_table)
            .map(|(_, rows)| *rows)
            .fold(0_u64, u64::saturating_add);
        let offset = committed.saturating_add(pending);
        let source = LogicalPlan::Limit {
            input: Box::new(view.source.clone()),
            n: u64::MAX,
            offset,
        };
        let insert = LogicalPlan::Insert {
            table: view.view_table.clone(),
            columns: Vec::new(),
            source: Box::new(source),
            on_conflict: None,
            returning: Vec::new(),
            schema: ultrasql_core::Schema::empty(),
        };
        let catalog_snapshot = self.state.catalog_snapshot();
        let result = run_plan_in_txn(
            &insert,
            txn,
            catalog_snapshot,
            Arc::clone(&self.state.table_constraints),
            Arc::clone(&self.state.sequences),
            Arc::clone(&self.state.persistent_catalog),
            Arc::clone(&self.state.time_partitions),
            Arc::clone(&self.state.workload_recorder),
            Some(self.sequence_state.clone()),
            &self.state.tables,
            Arc::clone(&self.state.heap),
            Arc::clone(&self.state.vm),
            Arc::clone(&self.state.txn_manager),
            self.jit_config(),
            Some(self.cancel_flag.clone()),
            &mut self.write_buf,
        )?;
        Ok(result.rows)
    }

    pub(crate) fn note_dml_effect(&mut self, plan: &LogicalPlan, rows: u64) {
        if rows == 0 {
            return;
        }
        let Some(table) = Self::dml_target_table(plan) else {
            return;
        };
        if let Some(kind) = Self::dml_change_kind(plan) {
            self.pending_logical_changes.push(PendingLogicalChange {
                table: table.to_ascii_lowercase(),
                kind,
                rows_affected: rows,
            });
        }
        let entry = self
            .pending_table_modifications
            .entry(table.to_ascii_lowercase())
            .or_insert(0);
        *entry = entry.saturating_add(rows);
    }

    pub(crate) fn parse_affected_rows_tag(messages: &[BackendMessage]) -> u64 {
        let Some(BackendMessage::CommandComplete { tag }) = messages
            .iter()
            .find(|m| matches!(m, BackendMessage::CommandComplete { .. }))
        else {
            return 0;
        };
        let mut parts = tag.split_whitespace();
        let Some(cmd) = parts.next() else {
            return 0;
        };
        if !matches!(cmd, "INSERT" | "UPDATE" | "DELETE") {
            return 0;
        }
        // INSERT tag shape is `INSERT 0 <rows>`, UPDATE/DELETE is
        // `<CMD> <rows>`.
        let last = parts.next_back().unwrap_or_default();
        last.parse::<u64>().unwrap_or(0)
    }

    pub(crate) fn parse_command_rows_tag(messages: &[BackendMessage]) -> u64 {
        let Some(BackendMessage::CommandComplete { tag }) = messages
            .iter()
            .find(|m| matches!(m, BackendMessage::CommandComplete { .. }))
        else {
            return 0;
        };
        tag.split_whitespace()
            .next_back()
            .and_then(|rows| rows.parse::<u64>().ok())
            .unwrap_or(0)
    }

    pub(crate) fn note_committed_dml_effect(&self, plan: &LogicalPlan, rows: u64) {
        if rows == 0 {
            return;
        }
        let Some(table) = Self::dml_target_table(plan) else {
            return;
        };
        if let Some(kind) = Self::dml_change_kind(plan) {
            self.state
                .logical_replication
                .record_committed_dml(table, kind, rows);
        }
        self.state.note_table_modifications(table, rows);
    }

    pub(crate) fn flush_pending_dml_effects(&mut self) {
        let logical = std::mem::take(&mut self.pending_logical_changes);
        for change in logical {
            self.state.logical_replication.record_committed_dml(
                &change.table,
                change.kind,
                change.rows_affected,
            );
        }
        let drained = std::mem::take(&mut self.pending_table_modifications);
        for (table, rows) in drained {
            self.state.note_table_modifications(&table, rows);
        }
    }

    pub(crate) fn flush_pending_materialized_view_rows(&mut self) {
        let drained = std::mem::take(&mut self.pending_materialized_view_rows);
        for (view, rows) in drained {
            if rows == 0 {
                continue;
            }
            view.materialized_rows
                .fetch_add(rows, std::sync::atomic::Ordering::AcqRel);
            self.state.note_table_modifications(&view.view_table, rows);
        }
    }

    pub(crate) fn run_post_response_maintenance(&mut self) {
        if self.pending_post_commit_maintenance {
            self.pending_post_commit_maintenance = false;
            self.state.note_commit_for_gc();
        }
        self.flush_pending_dml_effects();
    }

    pub(crate) fn clear_pending_dml_effects(&mut self) {
        self.pending_table_modifications.clear();
        self.pending_logical_changes.clear();
        self.pending_materialized_view_rows.clear();
    }

    pub(crate) fn dml_target_table(plan: &LogicalPlan) -> Option<&str> {
        match plan {
            LogicalPlan::Insert { table, .. }
            | LogicalPlan::Update { table, .. }
            | LogicalPlan::Delete { table, .. } => Some(table.as_str()),
            _ => None,
        }
    }

    pub(crate) fn dml_change_kind(plan: &LogicalPlan) -> Option<LogicalChangeKind> {
        match plan {
            LogicalPlan::Insert { .. } => Some(LogicalChangeKind::Insert),
            LogicalPlan::Update { .. } => Some(LogicalChangeKind::Update),
            LogicalPlan::Delete { .. } => Some(LogicalChangeKind::Delete),
            _ => None,
        }
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

    fn try_execute_logical_replication_ddl(
        &self,
        trimmed_sql: &str,
    ) -> Result<Option<SelectResult>, ServerError> {
        let Some(ddl) = Self::try_parse_logical_replication_ddl(trimmed_sql)? else {
            return Ok(None);
        };
        match ddl {
            LogicalReplicationDdl::CreatePublication { name, tables } => {
                self.state
                    .logical_replication
                    .create_publication(&name, tables)?;
                Ok(Some(run_ddl_command("CREATE PUBLICATION")))
            }
            LogicalReplicationDdl::DropPublication { name, if_exists } => {
                if !self.state.logical_replication.drop_publication(&name) && !if_exists {
                    return Err(ServerError::ddl(format!(
                        "publication \"{}\" does not exist",
                        name.to_ascii_lowercase()
                    )));
                }
                Ok(Some(run_ddl_command("DROP PUBLICATION")))
            }
            LogicalReplicationDdl::CreateSubscription => {
                Err(ServerError::Unsupported("CREATE SUBSCRIPTION"))
            }
            LogicalReplicationDdl::DropSubscription => {
                Err(ServerError::Unsupported("DROP SUBSCRIPTION"))
            }
        }
    }

    fn try_parse_logical_replication_ddl(
        trimmed_sql: &str,
    ) -> Result<Option<LogicalReplicationDdl>, ServerError> {
        let sql = trimmed_sql.trim().trim_end_matches(';').trim();
        if starts_with_keyword_pair(sql, "CREATE", "PUBLICATION") {
            let rest = sql["CREATE PUBLICATION".len()..].trim();
            let (name, after_name) = split_first_token(rest)?;
            let tables = parse_publication_tables(after_name)?;
            return Ok(Some(LogicalReplicationDdl::CreatePublication {
                name: name.to_string(),
                tables,
            }));
        }
        if starts_with_keyword_pair(sql, "DROP", "PUBLICATION") {
            let rest = sql["DROP PUBLICATION".len()..].trim();
            let (if_exists, rest) = if rest
                .get(.."IF EXISTS".len())
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case("IF EXISTS"))
            {
                (true, rest["IF EXISTS".len()..].trim())
            } else {
                (false, rest)
            };
            let (name, _) = split_first_token(rest)?;
            return Ok(Some(LogicalReplicationDdl::DropPublication {
                name: name.to_string(),
                if_exists,
            }));
        }
        if starts_with_keyword_pair(sql, "CREATE", "SUBSCRIPTION") {
            return Ok(Some(LogicalReplicationDdl::CreateSubscription));
        }
        if starts_with_keyword_pair(sql, "DROP", "SUBSCRIPTION") {
            return Ok(Some(LogicalReplicationDdl::DropSubscription));
        }
        Ok(None)
    }

    fn try_parse_backup_function(trimmed_sql: &str) -> Option<&'static str> {
        let normalized = trimmed_sql
            .trim_end_matches(';')
            .trim()
            .to_ascii_lowercase();
        if normalized.starts_with("select pg_start_backup(")
            || normalized.starts_with("select pg_backup_start(")
        {
            return Some("pg_start_backup");
        }
        if normalized == "select pg_stop_backup()"
            || normalized == "select pg_backup_stop()"
            || normalized.starts_with("select pg_stop_backup(")
            || normalized.starts_with("select pg_backup_stop(")
        {
            return Some("pg_stop_backup");
        }
        None
    }

    fn execute_backup_function(
        &self,
        function_name: &'static str,
    ) -> Result<SelectResult, ServerError> {
        let lsn = self.state.record_backup_marker(function_name)?;
        Ok(Self::single_text_select(function_name, &lsn))
    }

    fn single_text_select(name: &str, value: &str) -> SelectResult {
        SelectResult {
            messages: vec![
                BackendMessage::RowDescription {
                    fields: vec![FieldDescription {
                        name: name.to_owned(),
                        table_oid: 0,
                        col_attnum: 0,
                        type_oid: 25,
                        type_size: -1,
                        type_modifier: -1,
                        format_code: 0,
                    }],
                },
                BackendMessage::DataRow {
                    columns: vec![Some(value.as_bytes().to_vec())],
                },
                BackendMessage::CommandComplete {
                    tag: "SELECT 1".to_owned(),
                },
            ],
            streamed_body: None,
            shared_streamed_body: None,
            rows: 1,
        }
    }

    fn hot_standby_allows(trimmed_sql: &str) -> bool {
        let normalized = trimmed_sql.trim();
        if normalized.is_empty() {
            return true;
        }
        let upper = normalized.to_ascii_uppercase();
        upper.starts_with("SELECT")
            || upper.starts_with("SHOW")
            || upper.starts_with("EXPLAIN")
            || upper.starts_with("WITH")
            || upper.starts_with("VALUES")
            || (upper.starts_with("COPY") && upper.contains(" TO "))
    }
}

struct ServerStatsSource<'a> {
    stats_catalog: &'a parking_lot::RwLock<InMemoryStatsCatalog>,
}

impl StatsSource for ServerStatsSource<'_> {
    fn row_count(&self, table: &str) -> u64 {
        self.stats_catalog
            .read()
            .lookup_relation(table)
            .map_or(0, |s| s.row_count)
    }

    fn page_count(&self, table: &str) -> u64 {
        self.stats_catalog
            .read()
            .lookup_relation(table)
            .map_or(0, |s| s.page_count)
    }

    fn null_frac(&self, table: &str, column: usize) -> f64 {
        self.stats_catalog
            .read()
            .lookup_relation(table)
            .and_then(|s| s.columns.get(column).map(|c| c.null_frac))
            .unwrap_or(0.0)
    }

    fn n_distinct(&self, table: &str, column: usize) -> f64 {
        self.stats_catalog
            .read()
            .lookup_relation(table)
            .and_then(|s| s.columns.get(column).map(|c| c.n_distinct))
            .unwrap_or(0.0)
    }
}
