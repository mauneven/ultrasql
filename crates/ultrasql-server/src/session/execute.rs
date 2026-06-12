//! Part of the `session` module split. The
//! `impl<RW> Session<RW>` block is reopened here to add a handful
//! of methods to the type defined in `session/mod.rs`. Splitting
//! across files keeps every unit under the 600-line ceiling without
//! changing semantics.

use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncWrite};
use ultrasql_catalog::{CatalogSnapshot, StatisticExtRow, TableEntry};
use ultrasql_core::{
    BlockNumber, DataType, RelationId, Value, Xid, timestamptz_display_in_timezone,
};
use ultrasql_mvcc::XidStatusOracle;
use ultrasql_optimizer::{InMemoryStatsCatalog, PlanCacheKey, StatsCatalog, StatsSource};
use ultrasql_parser::Parser;
use ultrasql_planner::{BinaryOp, LogicalPlan, LogicalSetVariableAction, ScalarExpr, bind};
use ultrasql_protocol::{BackendMessage, FieldDescription};
use ultrasql_storage::access_method::{AccessMethod, BrinIndex};
use ultrasql_storage::btree::BTree;
use ultrasql_txn::{IsolationLevel, Transaction};

use super::{PendingLogicalChange, Session};
use crate::auth::AuthCatalog;
use crate::error::ServerError;
use crate::replication::LogicalChangeKind;
use crate::result_encoder::{self, SelectResult, run_ddl_command};
use crate::{
    CombinedCatalog, RunPlanInTxnArgs, TxnState, run_plan_in_txn, try_run_cached_int32_pair_select,
    try_run_cached_scalar_aggregate_select,
};

#[derive(Debug)]
struct CreateStatisticsSpec {
    name: String,
    table: String,
    columns: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum LogicalReplicationDdl {
    CreatePublication {
        name: String,
        tables: Vec<String>,
    },
    DropPublication {
        name: String,
        if_exists: bool,
    },
    CreateSubscription {
        name: String,
        conninfo: String,
        publications: Vec<String>,
        slot_name: Option<String>,
    },
    DropSubscription {
        name: String,
        if_exists: bool,
    },
}

trait RlsPlanOptionExt {
    fn transpose_ok(self) -> Result<Option<LogicalPlan>, ServerError>;
}

impl RlsPlanOptionExt for Option<LogicalPlan> {
    fn transpose_ok(self) -> Result<Option<LogicalPlan>, ServerError> {
        Ok(self)
    }
}

fn materialized_view_row_count_overflow() -> ServerError {
    ServerError::Execute(ultrasql_executor::ExecError::NumericFieldOverflow(
        "materialized view row count overflow".to_owned(),
    ))
}

fn checked_materialized_view_row_add(left: u64, right: u64) -> Result<u64, ServerError> {
    left.checked_add(right)
        .ok_or_else(materialized_view_row_count_overflow)
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

fn isolation_level_name(isolation: IsolationLevel) -> &'static str {
    match isolation {
        IsolationLevel::ReadCommitted => "read committed",
        IsolationLevel::RepeatableRead => "repeatable read",
        IsolationLevel::Serializable => "serializable",
    }
}

fn shown_transaction_isolation(txn_state: &TxnState) -> &'static str {
    match txn_state {
        TxnState::Idle => isolation_level_name(IsolationLevel::ReadCommitted),
        TxnState::InTransaction(txn) | TxnState::Failed(txn) => isolation_level_name(txn.isolation),
    }
}

fn parse_statement_timeout_ms(value: &str) -> Result<u64, ServerError> {
    let trimmed = value.trim();
    if let Some(stripped) = trimmed.strip_prefix('-') {
        if !stripped.is_empty() {
            return Err(ServerError::Unsupported("invalid statement_timeout"));
        }
    }
    trimmed
        .parse::<u64>()
        .map_err(|_| ServerError::Unsupported("invalid statement_timeout"))
}

fn normalize_datestyle(value: &str) -> Result<String, ServerError> {
    let mut style = None;
    let mut order = None;
    for part in value
        .split(|ch: char| ch == ',' || ch.is_ascii_whitespace())
        .filter(|part| !part.is_empty())
    {
        match part.to_ascii_lowercase().as_str() {
            "iso" => style = Some("ISO"),
            "postgres" => style = Some("Postgres"),
            "sql" => style = Some("SQL"),
            "german" => style = Some("German"),
            "mdy" => order = Some("MDY"),
            "dmy" => order = Some("DMY"),
            "ymd" => order = Some("YMD"),
            _ => return Err(ServerError::Unsupported("invalid datestyle")),
        }
    }
    let style = style.unwrap_or("ISO");
    let order = order.unwrap_or(if style == "German" { "DMY" } else { "MDY" });
    Ok(format!("{style}, {order}"))
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

fn parse_quoted_literal(input: &str) -> Result<(&str, &str), ServerError> {
    let input = input.trim_start();
    let Some(rest) = input.strip_prefix('\'') else {
        return Err(ServerError::ddl(
            "CREATE SUBSCRIPTION requires a quoted literal",
        ));
    };
    let Some(end) = rest.find('\'') else {
        return Err(ServerError::ddl("unterminated quoted literal"));
    };
    Ok((&rest[..end], rest[end + 1..].trim()))
}

fn parse_subscription_publications(input: &str) -> Result<Vec<String>, ServerError> {
    let mut rest = input.trim();
    if !rest
        .get(.."PUBLICATION".len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("PUBLICATION"))
    {
        return Err(ServerError::ddl("CREATE SUBSCRIPTION requires PUBLICATION"));
    }
    rest = rest["PUBLICATION".len()..].trim();
    let publication_part = rest
        .split_once("WITH")
        .map_or(rest, |(publications, _)| publications)
        .trim();
    let publications = publication_part
        .split(',')
        .map(|publication| publication.trim().trim_matches('"').to_string())
        .filter(|publication| !publication.is_empty())
        .collect::<Vec<_>>();
    if publications.is_empty() {
        return Err(ServerError::ddl(
            "CREATE SUBSCRIPTION requires at least one publication",
        ));
    }
    Ok(publications)
}

fn parse_subscription_slot_name(input: &str) -> Result<Option<String>, ServerError> {
    let Some((_, options)) = input.split_once("WITH") else {
        return Ok(None);
    };
    let options = options.trim();
    let options = options
        .strip_prefix('(')
        .and_then(|text| text.strip_suffix(')'))
        .unwrap_or(options);
    for option in options.split(',') {
        let Some((name, value)) = option.split_once('=') else {
            continue;
        };
        if name.trim().eq_ignore_ascii_case("slot_name") {
            return Ok(Some(
                value
                    .trim()
                    .trim_matches('\'')
                    .trim_matches('"')
                    .to_string(),
            ));
        }
    }
    Ok(None)
}

fn parse_create_subscription(input: &str) -> Result<LogicalReplicationDdl, ServerError> {
    let (name, rest) = split_first_token(input)?;
    let rest = rest.trim();
    if !rest
        .get(.."CONNECTION".len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("CONNECTION"))
    {
        return Err(ServerError::ddl("CREATE SUBSCRIPTION requires CONNECTION"));
    }
    let (conninfo, after_conninfo) = parse_quoted_literal(&rest["CONNECTION".len()..])?;
    let publications = parse_subscription_publications(after_conninfo)?;
    let slot_name = parse_subscription_slot_name(after_conninfo)?;
    Ok(LogicalReplicationDdl::CreateSubscription {
        name: name.to_string(),
        conninfo: conninfo.to_string(),
        publications,
        slot_name,
    })
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

        // Wire-level statement no-ops kept for SQL tooling
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
        if let Some(result) = self.try_dispatch_advisory_lock_select(&stmt)? {
            return Ok(result);
        }
        if let Some(result) = self.try_dispatch_sequence_select(&stmt)? {
            return Ok(result);
        }

        let combined = CombinedCatalog {
            snapshot: &catalog_snapshot,
            fallback: &self.state.catalog,
            search_path: self.session_settings.get("search_path").map(String::as_str),
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
        if matches!(&plan, LogicalPlan::SetRole { .. }) {
            return self.execute_set_role(&plan);
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
                | LogicalPlan::CreateTypeEnum { .. }
                | LogicalPlan::CreateTypeComposite { .. }
                | LogicalPlan::CreateDomain { .. }
                | LogicalPlan::CreateOperator { .. }
                | LogicalPlan::CreateIndex { .. }
                | LogicalPlan::DropIndex { .. }
                | LogicalPlan::CreatePolicy { .. }
                | LogicalPlan::CreateRole { .. }
                | LogicalPlan::AlterRole { .. }
                | LogicalPlan::DropRole { .. }
                | LogicalPlan::GrantPrivileges { .. }
                | LogicalPlan::RevokePrivileges { .. }
                | LogicalPlan::AlterDefaultPrivileges { .. }
                | LogicalPlan::GrantRole { .. }
                | LogicalPlan::RevokeRole { .. }
                | LogicalPlan::CreateSchema { .. }
                | LogicalPlan::DropSchema { .. }
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
            LogicalPlan::CreateTypeEnum { .. } => {
                return self.execute_create_type_enum(&plan, &catalog_snapshot);
            }
            LogicalPlan::CreateTypeComposite { .. } => {
                return self.execute_create_type_composite(&plan, &catalog_snapshot);
            }
            LogicalPlan::CreateDomain { .. } => {
                return self.execute_create_domain(&plan, &catalog_snapshot);
            }
            LogicalPlan::CreateOperator { .. } => {
                return self.execute_create_operator(&plan);
            }
            LogicalPlan::CreateIndex { .. } => {
                return self.execute_create_index(&plan, &catalog_snapshot);
            }
            LogicalPlan::DropIndex { .. } => {
                return self.execute_drop_index(&plan);
            }
            LogicalPlan::CreatePolicy { .. } => {
                return self.execute_create_policy(&plan, &catalog_snapshot);
            }
            LogicalPlan::CreateRole { .. } => {
                return self.execute_create_role(&plan);
            }
            LogicalPlan::AlterRole { .. } => {
                return self.execute_alter_role(&plan);
            }
            LogicalPlan::DropRole { .. } => {
                return self.execute_drop_role(&plan);
            }
            LogicalPlan::GrantPrivileges { .. } => {
                return self.execute_grant_privileges(&plan);
            }
            LogicalPlan::RevokePrivileges { .. } => {
                return self.execute_revoke_privileges(&plan);
            }
            LogicalPlan::AlterDefaultPrivileges { .. } => {
                return self.execute_alter_default_privileges(&plan);
            }
            LogicalPlan::GrantRole { .. } => {
                return self.execute_grant_role(&plan);
            }
            LogicalPlan::RevokeRole { .. } => {
                return self.execute_revoke_role(&plan);
            }
            LogicalPlan::CreateSchema { .. } => {
                return self.execute_create_schema(&plan);
            }
            LogicalPlan::DropSchema { .. } => {
                return self.execute_drop_schema(&plan);
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

    pub(crate) fn is_ddl_plan(plan: &LogicalPlan) -> bool {
        matches!(
            plan,
            LogicalPlan::CreateTable { .. }
                | LogicalPlan::CreateMaterializedView { .. }
                | LogicalPlan::CreateTypeEnum { .. }
                | LogicalPlan::CreateTypeComposite { .. }
                | LogicalPlan::CreateDomain { .. }
                | LogicalPlan::CreateOperator { .. }
                | LogicalPlan::CreateIndex { .. }
                | LogicalPlan::DropIndex { .. }
                | LogicalPlan::CreatePolicy { .. }
                | LogicalPlan::CreateRole { .. }
                | LogicalPlan::AlterRole { .. }
                | LogicalPlan::DropRole { .. }
                | LogicalPlan::GrantPrivileges { .. }
                | LogicalPlan::RevokePrivileges { .. }
                | LogicalPlan::AlterDefaultPrivileges { .. }
                | LogicalPlan::GrantRole { .. }
                | LogicalPlan::RevokeRole { .. }
                | LogicalPlan::CreateSchema { .. }
                | LogicalPlan::DropSchema { .. }
                | LogicalPlan::CreateSequence { .. }
                | LogicalPlan::AlterSequence { .. }
                | LogicalPlan::DropSequence { .. }
                | LogicalPlan::Comment { .. }
                | LogicalPlan::DropTable { .. }
                | LogicalPlan::AlterTable { .. }
                | LogicalPlan::Truncate { .. }
        )
    }

    pub(crate) fn execute_ddl_plan(
        &mut self,
        plan: &LogicalPlan,
        catalog_snapshot: &Arc<CatalogSnapshot>,
    ) -> Result<SelectResult, ServerError> {
        match plan {
            LogicalPlan::CreateTable { .. } => self.execute_create_table(plan, catalog_snapshot),
            LogicalPlan::CreateMaterializedView { .. } => {
                self.execute_create_materialized_view(plan, catalog_snapshot)
            }
            LogicalPlan::CreateTypeEnum { .. } => {
                self.execute_create_type_enum(plan, catalog_snapshot)
            }
            LogicalPlan::CreateTypeComposite { .. } => {
                self.execute_create_type_composite(plan, catalog_snapshot)
            }
            LogicalPlan::CreateDomain { .. } => self.execute_create_domain(plan, catalog_snapshot),
            LogicalPlan::CreateOperator { .. } => self.execute_create_operator(plan),
            LogicalPlan::CreateIndex { .. } => self.execute_create_index(plan, catalog_snapshot),
            LogicalPlan::DropIndex { .. } => self.execute_drop_index(plan),
            LogicalPlan::CreatePolicy { .. } => self.execute_create_policy(plan, catalog_snapshot),
            LogicalPlan::CreateRole { .. } => self.execute_create_role(plan),
            LogicalPlan::AlterRole { .. } => self.execute_alter_role(plan),
            LogicalPlan::DropRole { .. } => self.execute_drop_role(plan),
            LogicalPlan::GrantPrivileges { .. } => self.execute_grant_privileges(plan),
            LogicalPlan::RevokePrivileges { .. } => self.execute_revoke_privileges(plan),
            LogicalPlan::AlterDefaultPrivileges { .. } => {
                self.execute_alter_default_privileges(plan)
            }
            LogicalPlan::GrantRole { .. } => self.execute_grant_role(plan),
            LogicalPlan::RevokeRole { .. } => self.execute_revoke_role(plan),
            LogicalPlan::CreateSchema { .. } => self.execute_create_schema(plan),
            LogicalPlan::DropSchema { .. } => self.execute_drop_schema(plan),
            LogicalPlan::CreateSequence { .. } => self.execute_create_sequence(plan),
            LogicalPlan::AlterSequence { .. } => self.execute_alter_sequence(plan),
            LogicalPlan::DropSequence { .. } => self.execute_drop_sequence(plan),
            LogicalPlan::Comment { .. } => self.execute_comment(plan, catalog_snapshot),
            LogicalPlan::DropTable { .. } => self.execute_drop_table(plan),
            LogicalPlan::AlterTable { .. } => self.execute_alter_table(plan, catalog_snapshot),
            LogicalPlan::Truncate { .. } => self.execute_truncate(plan, catalog_snapshot),
            _ => Err(ServerError::Unsupported("execute_ddl_plan: wrong plan")),
        }
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
            "statement_timeout" => {
                self.statement_timeout_ms = 0;
                self.session_settings.remove("statement_timeout");
                Ok(result_encoder::run_ddl_command("RESET"))
            }
            "extra_float_digits" => {
                self.session_settings.remove("extra_float_digits");
                Ok(result_encoder::run_ddl_command("RESET"))
            }
            "application_name" => {
                self.session_settings.remove("application_name");
                self.state
                    .workload_recorder
                    .update_session_application_name(self.pid, None);
                Ok(result_encoder::run_ddl_command("RESET"))
            }
            "client_min_messages" => {
                self.session_settings.remove("client_min_messages");
                Ok(result_encoder::run_ddl_command("RESET"))
            }
            "client_encoding" => Ok(result_encoder::run_ddl_command("RESET")),
            "datestyle" => {
                self.session_settings.remove("datestyle");
                Ok(result_encoder::run_ddl_command("RESET"))
            }
            "search_path" => {
                self.session_settings.remove("search_path");
                self.plan_cache_invalidate();
                Ok(result_encoder::run_ddl_command("RESET"))
            }
            "intervalstyle" => {
                self.session_settings.remove("intervalstyle");
                Ok(result_encoder::run_ddl_command("RESET"))
            }
            "lc_monetary" => {
                self.session_settings.remove("lc_monetary");
                Ok(result_encoder::run_ddl_command("RESET"))
            }
            "timezone" => {
                self.session_settings.remove("timezone");
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
            "statement_timeout" => {
                let parsed = parse_statement_timeout_ms(value)?;
                self.statement_timeout_ms = parsed;
                self.session_settings
                    .insert("statement_timeout".to_owned(), parsed.to_string());
                Ok(())
            }
            "extra_float_digits" => {
                let parsed = value
                    .parse::<i32>()
                    .map_err(|_| ServerError::Unsupported("invalid extra_float_digits"))?;
                if !(-15..=3).contains(&parsed) {
                    return Err(ServerError::Unsupported("invalid extra_float_digits"));
                }
                self.session_settings
                    .insert("extra_float_digits".to_owned(), parsed.to_string());
                Ok(())
            }
            "application_name" => {
                self.session_settings
                    .insert("application_name".to_owned(), value.to_owned());
                self.state
                    .workload_recorder
                    .update_session_application_name(self.pid, Some(value.to_owned()));
                Ok(())
            }
            "client_min_messages" => match value.to_ascii_lowercase().as_str() {
                "debug5" | "debug4" | "debug3" | "debug2" | "debug1" | "log" | "notice"
                | "warning" | "error" => {
                    self.session_settings
                        .insert("client_min_messages".to_owned(), value.to_ascii_lowercase());
                    Ok(())
                }
                _ => Err(ServerError::Unsupported("invalid client_min_messages")),
            },
            "client_encoding" => match value.to_ascii_lowercase().as_str() {
                "utf8" | "utf-8" | "unicode" => Ok(()),
                _ => Err(ServerError::Unsupported("invalid client_encoding")),
            },
            "datestyle" => {
                let normalized = normalize_datestyle(value)?;
                self.session_settings
                    .insert("datestyle".to_owned(), normalized);
                Ok(())
            }
            "search_path" => {
                self.session_settings
                    .insert("search_path".to_owned(), value.to_owned());
                self.plan_cache_invalidate();
                Ok(())
            }
            "intervalstyle" => match value.to_ascii_lowercase().as_str() {
                "postgres" | "postgres_verbose" | "sql_standard" | "iso_8601" => {
                    self.session_settings
                        .insert("intervalstyle".to_owned(), value.to_ascii_lowercase());
                    Ok(())
                }
                _ => Err(ServerError::Unsupported("invalid intervalstyle")),
            },
            "lc_monetary" => {
                self.session_settings
                    .insert("lc_monetary".to_owned(), value.to_owned());
                Ok(())
            }
            "timezone" => {
                let normalized = value.trim();
                if normalized.is_empty() || timestamptz_display_in_timezone(0, normalized).is_none()
                {
                    return Err(ServerError::Unsupported("invalid timezone"));
                }
                self.session_settings
                    .insert("timezone".to_owned(), normalized.to_owned());
                Ok(())
            }
            "standard_conforming_strings" => match value.to_ascii_lowercase().as_str() {
                "on" => Ok(()),
                _ => Err(ServerError::Unsupported(
                    "invalid standard_conforming_strings",
                )),
            },
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
            "statement_timeout" => self.statement_timeout_ms.to_string(),
            "extra_float_digits" => self
                .session_settings
                .get("extra_float_digits")
                .cloned()
                .unwrap_or_else(|| "1".to_owned()),
            "application_name" => self
                .session_settings
                .get("application_name")
                .cloned()
                .unwrap_or_default(),
            "client_encoding" => "UTF8".to_owned(),
            "client_min_messages" => self
                .session_settings
                .get("client_min_messages")
                .cloned()
                .unwrap_or_else(|| "notice".to_owned()),
            "datestyle" => self
                .session_settings
                .get("datestyle")
                .cloned()
                .unwrap_or_else(|| "ISO, MDY".to_owned()),
            "intervalstyle" => self
                .session_settings
                .get("intervalstyle")
                .cloned()
                .unwrap_or_else(|| "postgres".to_owned()),
            "lc_monetary" => self
                .session_settings
                .get("lc_monetary")
                .cloned()
                .unwrap_or_else(|| "C".to_owned()),
            "max_identifier_length" => "63".to_owned(),
            "server_version" => crate::REPORTED_SERVER_VERSION.to_owned(),
            "server_version_num" => "140000".to_owned(),
            "search_path" => self
                .session_settings
                .get("search_path")
                .cloned()
                .unwrap_or_else(|| "\"$user\", public".to_owned()),
            "timezone" | "TimeZone" => self
                .session_settings
                .get("timezone")
                .cloned()
                .unwrap_or_else(|| "UTC".to_owned()),
            "transaction_isolation" => shown_transaction_isolation(&self.txn_state).to_owned(),
            "standard_conforming_strings" => "on".to_owned(),
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

    fn scalar_aggregate_source_table(plan: &LogicalPlan) -> Option<String> {
        let agg_plan = match plan {
            LogicalPlan::Project { input, exprs, .. } => {
                let passthrough = exprs
                    .iter()
                    .all(|(expr, _)| matches!(expr, ScalarExpr::Column { .. }));
                if !passthrough {
                    return None;
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
            return None;
        };
        if !group_by.is_empty() || aggregates.len() != 1 || aggregates[0].distinct {
            return None;
        }

        let table = match input.as_ref() {
            LogicalPlan::Scan { table, .. } => table,
            LogicalPlan::Filter {
                input: filter_input,
                ..
            } => {
                let LogicalPlan::Scan { table, .. } = filter_input.as_ref() else {
                    return None;
                };
                table
            }
            _ => return None,
        };
        Some(table.to_ascii_lowercase())
    }

    fn can_use_cached_scalar_aggregate_in_explicit_txn(&self, plan: &LogicalPlan) -> bool {
        let TxnState::InTransaction(txn) = &self.txn_state else {
            return false;
        };
        if txn.isolation != IsolationLevel::ReadCommitted {
            return false;
        }
        let Some(table) = Self::scalar_aggregate_source_table(plan) else {
            return false;
        };
        !self.pending_table_modifications.contains_key(&table)
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
        if runtime.enabled && !self.bypasses_row_security(&runtime) {
            Some(runtime)
        } else {
            None
        }
    }

    fn bypasses_row_security(&self, runtime: &crate::TableRowSecurity) -> bool {
        let current_user = self.current_user.to_ascii_lowercase();
        let Some(role) = self.state.role_catalog.lookup_role(&current_user) else {
            return false;
        };
        role.is_superuser
            || role.bypass_rls
            || (!runtime.owner_role.is_empty()
                && runtime.owner_role.eq_ignore_ascii_case(&current_user))
    }

    fn rls_using_predicate(
        &self,
        runtime: &crate::TableRowSecurity,
        command: crate::RuntimeRlsCommand,
    ) -> Result<ScalarExpr, ServerError> {
        let inherited_roles = self
            .state
            .role_catalog
            .inherited_role_names(&self.current_user);
        let mut permissive = Vec::new();
        let mut restrictive = Vec::new();
        for policy in runtime.policies.iter().filter(|policy| {
            policy.command.applies_to(command) && policy.applies_to_roles(&inherited_roles)
        }) {
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
        let inherited_roles = self
            .state
            .role_catalog
            .inherited_role_names(&self.current_user);
        let mut permissive_checks = Vec::new();
        let mut restrictive_checks = Vec::new();
        for policy in runtime.policies.iter().filter(|policy| {
            policy.command.applies_to(crate::RuntimeRlsCommand::Insert)
                && policy.applies_to_roles(&inherited_roles)
        }) {
            let Some(check) = policy.with_check.as_ref().or(policy.using.as_ref()) else {
                continue;
            };
            match policy.permissiveness {
                crate::RuntimeRlsPermissiveness::Permissive => permissive_checks.push(check),
                crate::RuntimeRlsPermissiveness::Restrictive => restrictive_checks.push(check),
            }
        }
        let LogicalPlan::Values { rows, .. } = source.as_ref() else {
            return Ok(());
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
        self.enforce_column_privileges(plan, catalog_snapshot)?;
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
        self.reject_non_append_materialized_view_source_write(plan)?;

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
                });
                self.finalise_autocommit(plan, txn, outcome)
            }
            TxnState::InTransaction(mut txn) => {
                self.state.txn_manager.refresh_snapshot(&mut txn);
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
        match outcome {
            Ok(result) => {
                let is_dml = Self::dml_target_table(plan).is_some();
                if is_dml {
                    if let Err(e) = self.state.validate_deferred_foreign_keys(&txn) {
                        return Err(self.rollback_transaction_after_error(
                            txn,
                            e,
                            "autocommit rollback after deferred FK violation",
                        ));
                    }
                    if let Err(e) =
                        self.flush_dirty_heap_pages_after_dml_if_needed(plan, result.rows)
                    {
                        return Err(self.rollback_transaction_after_error(
                            txn,
                            e,
                            "autocommit rollback after dirty-page flush error",
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
                }
                Ok(result)
            }
            Err(e) => Err(self.rollback_transaction_after_error(
                txn,
                e,
                "autocommit rollback after statement error",
            )),
        }
    }

    pub(crate) fn rollback_transaction_after_error(
        &self,
        txn: Transaction,
        original: ServerError,
        context: &'static str,
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
            .txn_manager
            .abort(txn)
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
            return Err(self.rollback_catalog_transaction_after_error(
                catalog_txn,
                ServerError::Catalog(e),
                "CREATE STATISTICS catalog rollback after persist error",
            ));
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
            let rel = RelationId(entry.oid);
            let block_count = self.state.heap.block_count(rel).max(entry.n_blocks);
            self.state
                .workload_recorder
                .begin_vacuum(self.pid, entry.oid.raw(), block_count);
            let result = (|| -> Result<(), ServerError> {
                self.state
                    .workload_recorder
                    .update_vacuum(self.pid, "vacuuming indexes", 0, 0);
                self.vacuum_one_table_indexes(&snapshot, &entry, oldest)?;
                self.state.workload_recorder.update_vacuum(
                    self.pid,
                    "vacuuming heap",
                    block_count,
                    0,
                );
                self.state
                    .heap
                    .vacuum_heap(rel, oldest, self.state.txn_manager.as_ref())
                    .map_err(|e| ServerError::ddl(format!("VACUUM heap: {e}")))?;
                self.state.workload_recorder.update_vacuum(
                    self.pid,
                    "performing final cleanup",
                    block_count,
                    block_count,
                );
                self.state.vacuum_mark_visible_pages(oldest);
                self.resummarize_brin_indexes(&snapshot, &entry)?;
                self.maintain_aggregating_indexes_for_tables_after_commit(std::slice::from_ref(
                    &entry.name,
                ))?;
                Ok(())
            })();
            self.state.workload_recorder.finish_vacuum(self.pid);
            result?;
            self.state
                .workload_recorder
                .record_table_vacuum(entry.oid.raw());
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
        self.finalise_read_maintenance_transaction(
            txn,
            result,
            "VACUUM BRIN summarize commit",
            "VACUUM BRIN summarize rollback after rebuild error",
        )
    }

    fn execute_analyze(&mut self, table: Option<&str>) -> Result<SelectResult, ServerError> {
        match table {
            Some(t) => {
                if !self.state.analyze_table_with_pid(t, self.pid)? {
                    return Err(self.fail_if_in_transaction(ServerError::Plan(
                        ultrasql_planner::PlanError::TableNotFound(t.to_string()),
                    )));
                }
            }
            None => {
                let snapshot = self.state.catalog_snapshot();
                let tables: Vec<String> = snapshot.tables.keys().map(|k| k.to_string()).collect();
                for name in tables {
                    let _ = self.state.analyze_table_with_pid(&name, self.pid);
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
                return Err(self.rollback_materialized_view_maintenance_after_error(
                    txn,
                    e,
                    "materialized-view insert maintenance rollback after delta error",
                ));
            }
        };
        self.state.commit_transaction(
            txn,
            true,
            "materialized-view insert maintenance transaction",
        )?;
        self.pending_materialized_view_rows.extend(rows);
        self.flush_pending_materialized_view_rows()?;
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
                    return Err(self.rollback_materialized_view_maintenance_after_error(
                        txn,
                        e,
                        "materialized-view table maintenance rollback after delta error",
                    ));
                }
            }
        }
        self.state.commit_transaction(
            txn,
            true,
            "materialized-view table maintenance transaction",
        )?;
        self.pending_materialized_view_rows.extend(rows);
        self.flush_pending_materialized_view_rows()?;
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
        self.finalise_read_maintenance_transaction(
            txn,
            result,
            "aggregating-index maintenance commit",
            "aggregating-index maintenance rollback after refresh error",
        )
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
            .try_fold(0_u64, checked_materialized_view_row_add)?;
        let offset = checked_materialized_view_row_add(committed, pending)?;
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
        let result = run_plan_in_txn(RunPlanInTxnArgs {
            plan: &insert,
            txn,
            catalog_snapshot,
            table_constraints: Arc::clone(&self.state.table_constraints),
            sequences: Arc::clone(&self.state.sequences),
            sequence_owners: Arc::clone(&self.state.sequence_owners),
            sequence_namespaces: Arc::clone(&self.state.sequence_namespaces),
            schemas: Arc::clone(&self.state.schemas),
            operators: Arc::clone(&self.state.operators),
            role_catalog: Arc::clone(&self.state.role_catalog),
            privilege_catalog: Arc::clone(&self.state.privilege_catalog),
            row_security: Arc::clone(&self.state.row_security),
            session_settings: Arc::new(std::collections::HashMap::new()),
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
        })?;
        Ok(result.rows)
    }

    pub(crate) fn note_dml_effect(
        &mut self,
        plan: &LogicalPlan,
        rows: u64,
    ) -> Result<(), ServerError> {
        if rows == 0 {
            return Ok(());
        }
        let Some(table) = Self::dml_target_table(plan) else {
            return Ok(());
        };
        let table = table.to_ascii_lowercase();
        let current = self
            .pending_table_modifications
            .get(&table)
            .copied()
            .unwrap_or(0);
        let total = current.checked_add(rows).ok_or_else(|| {
            ServerError::Execute(ultrasql_executor::ExecError::NumericFieldOverflow(
                "pending DML row count overflow".to_owned(),
            ))
        })?;
        if let Some(kind) = Self::dml_change_kind(plan) {
            self.pending_logical_changes.push(PendingLogicalChange {
                table: table.clone(),
                kind,
                rows_affected: rows,
            });
        }
        self.pending_table_modifications.insert(table, total);
        Ok(())
    }

    pub(crate) fn flush_dirty_heap_pages_after_dml_if_needed(
        &self,
        plan: &LogicalPlan,
        rows: u64,
    ) -> Result<(), ServerError> {
        if rows > 0 && matches!(plan, LogicalPlan::Insert { .. }) {
            self.state.flush_dirty_heap_pages_if_needed()?;
        }
        Ok(())
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

    pub(crate) fn flush_pending_materialized_view_rows(&mut self) -> Result<(), ServerError> {
        let drained = std::mem::take(&mut self.pending_materialized_view_rows);
        for (view, rows) in drained {
            if rows == 0 {
                continue;
            }
            let previous = view
                .materialized_rows
                .fetch_update(
                    std::sync::atomic::Ordering::AcqRel,
                    std::sync::atomic::Ordering::Acquire,
                    |current| current.checked_add(rows),
                )
                .map_err(|_| materialized_view_row_count_overflow())?;
            let total = checked_materialized_view_row_add(previous, rows)?;
            if let Err(err) = self
                .state
                .persist_materialized_view_runtime_metadata(&view, total)
            {
                tracing::warn!(
                    error = %err,
                    view = %view.view_table,
                    "persist materialized-view runtime metadata failed",
                );
            }
            self.state.note_table_modifications(&view.view_table, rows);
        }
        Ok(())
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
                let dropped = self.state.logical_replication.drop_publication(&name)?;
                if !dropped && !if_exists {
                    return Err(ServerError::ddl(format!(
                        "publication \"{}\" does not exist",
                        name.to_ascii_lowercase()
                    )));
                }
                Ok(Some(run_ddl_command("DROP PUBLICATION")))
            }
            LogicalReplicationDdl::CreateSubscription {
                name,
                conninfo,
                publications,
                slot_name,
            } => {
                self.state.logical_replication.create_subscription(
                    &name,
                    &conninfo,
                    publications,
                    slot_name,
                )?;
                Ok(Some(run_ddl_command("CREATE SUBSCRIPTION")))
            }
            LogicalReplicationDdl::DropSubscription { name, if_exists } => {
                let dropped = self.state.logical_replication.drop_subscription(&name)?;
                if !dropped && !if_exists {
                    return Err(ServerError::ddl(format!(
                        "subscription \"{}\" does not exist",
                        name.to_ascii_lowercase()
                    )));
                }
                Ok(Some(run_ddl_command("DROP SUBSCRIPTION")))
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
            let rest = sql["CREATE SUBSCRIPTION".len()..].trim();
            return Ok(Some(parse_create_subscription(rest)?));
        }
        if starts_with_keyword_pair(sql, "DROP", "SUBSCRIPTION") {
            let rest = sql["DROP SUBSCRIPTION".len()..].trim();
            let (if_exists, rest) = if rest
                .get(.."IF EXISTS".len())
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case("IF EXISTS"))
            {
                (true, rest["IF EXISTS".len()..].trim())
            } else {
                (false, rest)
            };
            let (name, _) = split_first_token(rest)?;
            return Ok(Some(LogicalReplicationDdl::DropSubscription {
                name: name.to_string(),
                if_exists,
            }));
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use tokio::io::duplex;
    use ultrasql_core::{Field, Schema};
    use ultrasql_planner::{AggregateFunc, LogicalAggregateExpr, LogicalSetVariableAction};

    use crate::Server;

    fn test_schema() -> Schema {
        Schema::new([
            Field::required("id", DataType::Int32),
            Field::required("value", DataType::Int32),
        ])
        .expect("test schema")
    }

    fn scan_plan() -> LogicalPlan {
        LogicalPlan::Scan {
            table: "t".to_owned(),
            schema: test_schema(),
            projection: None,
        }
    }

    fn int_column(name: &str, index: usize) -> ScalarExpr {
        ScalarExpr::Column {
            name: name.to_owned(),
            index,
            data_type: DataType::Int32,
        }
    }

    fn test_session() -> Session<tokio::io::DuplexStream> {
        let (io, _peer) = duplex(64);
        Session::new(io, Arc::new(Server::with_sample_database()))
    }

    fn first_data_row_text(result: &SelectResult) -> String {
        let Some(BackendMessage::DataRow { columns }) = result
            .messages
            .iter()
            .find(|msg| matches!(msg, BackendMessage::DataRow { .. }))
        else {
            panic!("missing data row");
        };
        String::from_utf8(columns[0].clone().expect("value")).expect("utf8")
    }

    #[test]
    fn logical_replication_and_guc_parsers_cover_success_and_errors() {
        for value in ["on", "TRUE", "1", "yes"] {
            assert!(parse_bool_guc(value).expect("true guc"));
        }
        for value in ["off", "FALSE", "0", "no"] {
            assert!(!parse_bool_guc(value).expect("false guc"));
        }
        assert!(parse_bool_guc("maybe").is_err());
        assert_eq!(parse_statement_timeout_ms(" 250 ").expect("timeout"), 250);
        assert!(parse_statement_timeout_ms("-1").is_err());
        assert!(parse_statement_timeout_ms("abc").is_err());

        assert!(starts_with_keyword_pair(
            "create publication pub for table t",
            "CREATE",
            "PUBLICATION",
        ));
        assert!(!starts_with_keyword_pair("create", "CREATE", "PUBLICATION"));
        assert_eq!(
            split_first_token("  name rest ").expect("token"),
            ("name", "rest")
        );
        assert!(split_first_token("   ").is_err());
        assert_eq!(
            parse_publication_tables("FOR TABLE users, \"Orders\"").expect("tables"),
            vec!["users".to_owned(), "Orders".to_owned()]
        );
        assert!(parse_publication_tables("FOR ALL TABLES").is_err());
        assert!(parse_publication_tables("FOR TABLE ,").is_err());
        assert_eq!(
            parse_quoted_literal(" 'conn info' PUBLICATION pub").expect("literal"),
            ("conn info", "PUBLICATION pub")
        );
        assert!(parse_quoted_literal("conn").is_err());
        assert!(parse_quoted_literal("'unterminated").is_err());
        assert_eq!(
            parse_subscription_publications("PUBLICATION pub1, \"Pub2\" WITH (slot_name='s')")
                .expect("publications"),
            vec!["pub1".to_owned(), "Pub2".to_owned()]
        );
        assert!(parse_subscription_publications("WITH ()").is_err());
        assert_eq!(
            parse_subscription_slot_name(
                "PUBLICATION pub WITH (copy_data=false, slot_name='slot_a')"
            )
            .expect("slot"),
            Some("slot_a".to_owned())
        );
        assert_eq!(
            parse_subscription_slot_name("PUBLICATION pub").expect("no slot"),
            None
        );

        let subscription = parse_create_subscription(
            "sub CONNECTION 'host=localhost' PUBLICATION pub WITH (slot_name = \"slot_b\")",
        )
        .expect("subscription");
        assert_eq!(
            subscription,
            LogicalReplicationDdl::CreateSubscription {
                name: "sub".to_owned(),
                conninfo: "host=localhost".to_owned(),
                publications: vec!["pub".to_owned()],
                slot_name: Some("slot_b".to_owned()),
            }
        );
        assert!(parse_create_subscription("sub PUBLICATION pub").is_err());

        assert_eq!(
            Session::<tokio::io::DuplexStream>::try_parse_logical_replication_ddl(
                "CREATE PUBLICATION pub FOR TABLE users;"
            )
            .expect("parse publication"),
            Some(LogicalReplicationDdl::CreatePublication {
                name: "pub".to_owned(),
                tables: vec!["users".to_owned()],
            })
        );
        assert_eq!(
            Session::<tokio::io::DuplexStream>::try_parse_logical_replication_ddl(
                "DROP PUBLICATION IF EXISTS pub"
            )
            .expect("drop publication"),
            Some(LogicalReplicationDdl::DropPublication {
                name: "pub".to_owned(),
                if_exists: true,
            })
        );
        assert_eq!(
            Session::<tokio::io::DuplexStream>::try_parse_logical_replication_ddl(
                "DROP SUBSCRIPTION sub"
            )
            .expect("drop subscription"),
            Some(LogicalReplicationDdl::DropSubscription {
                name: "sub".to_owned(),
                if_exists: false,
            })
        );
        assert!(
            Session::<tokio::io::DuplexStream>::try_parse_logical_replication_ddl("SELECT 1")
                .expect("not ddl")
                .is_none()
        );
    }

    #[test]
    fn session_variable_surface_sets_shows_and_resets_supported_gucs() {
        let mut session = test_session();
        session
            .apply_session_variable("jit", "on")
            .expect("set jit");
        assert!(session.jit_enabled);
        session
            .apply_session_variable("jit_above_cost", "123")
            .expect("set jit threshold");
        assert_eq!(session.jit_above_rows, 123);
        session
            .apply_session_variable("statement_timeout", "50")
            .expect("set timeout");
        assert_eq!(session.statement_timeout_ms, 50);
        session
            .apply_session_variable("extra_float_digits", "3")
            .expect("extra_float_digits");
        session
            .apply_session_variable("application_name", "cert")
            .expect("application name");
        session
            .apply_session_variable("client_min_messages", "WARNING")
            .expect("client min messages");
        session
            .apply_session_variable("client_encoding", "UTF8")
            .expect("encoding");
        session
            .apply_session_variable("datestyle", "SQL, DMY")
            .expect("datestyle");
        session
            .apply_session_variable("search_path", "app, public")
            .expect("search path");
        session
            .apply_session_variable("intervalstyle", "iso_8601")
            .expect("intervalstyle");
        session
            .apply_session_variable("lc_monetary", "C")
            .expect("lc_monetary");
        session
            .apply_session_variable("timezone", "America/Bogota")
            .expect("timezone");
        session
            .apply_session_variable("timezone", "+02:30")
            .expect("fixed timezone");
        session
            .apply_session_variable("standard_conforming_strings", "on")
            .expect("strings");
        session
            .apply_session_variable("synchronous_commit", "remote_write")
            .expect("sync commit");
        session
            .apply_session_variable("ultrasql.tenant", "acme")
            .expect("custom guc");

        assert_eq!(
            first_data_row_text(
                &session
                    .show_session_variable("jit", true)
                    .expect("show jit")
            ),
            "on"
        );
        assert_eq!(
            first_data_row_text(
                &session
                    .show_session_variable("timezone", false)
                    .expect("show timezone")
            ),
            "+02:30"
        );
        assert_eq!(
            first_data_row_text(
                &session
                    .show_session_variable("ultrasql.tenant", false)
                    .expect("show custom")
            ),
            "acme"
        );
        assert_eq!(
            first_data_row_text(
                &session
                    .show_session_variable("lc_monetary", false)
                    .expect("show lc_monetary")
            ),
            "C"
        );
        assert_eq!(
            first_data_row_text(
                &session
                    .show_session_variable("datestyle", false)
                    .expect("show datestyle")
            ),
            "SQL, DMY"
        );
        assert_eq!(
            first_data_row_text(
                &session
                    .show_session_variable("server_version", true)
                    .expect("show version")
            ),
            crate::REPORTED_SERVER_VERSION
        );

        assert!(session.apply_session_variable("jit", "maybe").is_err());
        assert!(
            session
                .apply_session_variable("jit_above_cost", "bad")
                .is_err()
        );
        assert!(
            session
                .apply_session_variable("extra_float_digits", "4")
                .is_err()
        );
        assert!(
            session
                .apply_session_variable("client_min_messages", "loud")
                .is_err()
        );
        assert!(
            session
                .apply_session_variable("client_encoding", "LATIN1")
                .is_err()
        );
        assert!(session.apply_session_variable("datestyle", "moon").is_err());
        assert!(
            session
                .apply_session_variable("timezone", "No/SuchZone")
                .is_err()
        );
        assert!(
            session
                .apply_session_variable("intervalstyle", "bad")
                .is_err()
        );
        assert!(
            session
                .apply_session_variable("standard_conforming_strings", "off")
                .is_err()
        );
        assert!(
            session
                .apply_session_variable("synchronous_commit", "bad")
                .is_err()
        );
        assert!(session.apply_session_variable("unknown", "x").is_err());

        for name in [
            "jit",
            "jit_above_cost",
            "statement_timeout",
            "extra_float_digits",
            "application_name",
            "client_min_messages",
            "client_encoding",
            "datestyle",
            "search_path",
            "intervalstyle",
            "lc_monetary",
            "timezone",
            "synchronous_commit",
            "ultrasql.tenant",
        ] {
            session
                .execute_set_variable_reset(name)
                .unwrap_or_else(|_| panic!("reset {name}"));
        }
        assert!(!session.jit_enabled);
        assert_eq!(session.statement_timeout_ms, 0);
        assert!(!session.session_settings.contains_key("ultrasql.tenant"));
        assert!(session.execute_set_variable_reset("unsupported").is_err());

        let show_plan = LogicalPlan::SetVariable {
            name: "client_encoding".to_owned(),
            action: LogicalSetVariableAction::Show,
            value: None,
            schema: Schema::new([Field::required(
                "client_encoding",
                DataType::Text { max_len: None },
            )])
            .expect("show schema"),
        };
        assert_eq!(
            first_data_row_text(
                &session
                    .execute_set_variable(&show_plan, true)
                    .expect("execute show")
            ),
            "UTF8"
        );
        let wrong = LogicalPlan::Values {
            rows: Vec::new(),
            schema: Schema::empty(),
        };
        assert!(session.execute_set_variable(&wrong, true).is_err());
    }

    #[test]
    fn plan_shape_predicates_and_command_tags_cover_dml_edges() {
        let scan = scan_plan();
        let filtered = LogicalPlan::Filter {
            input: Box::new(scan.clone()),
            predicate: bool_literal(true),
        };
        let update = LogicalPlan::Update {
            table: "t".to_owned(),
            assignments: vec![(1, int_column("value", 1))],
            input: Box::new(filtered.clone()),
            returning: Vec::new(),
            schema: Schema::empty(),
        };
        assert!(Session::<tokio::io::DuplexStream>::is_fused_update_shape(
            &update
        ));
        let mut returning_update = update.clone();
        if let LogicalPlan::Update { returning, .. } = &mut returning_update {
            returning.push((int_column("id", 0), "id".to_owned()));
        }
        assert!(!Session::<tokio::io::DuplexStream>::is_fused_update_shape(
            &returning_update
        ));

        let aggregate = LogicalPlan::Aggregate {
            input: Box::new(filtered),
            group_by: Vec::new(),
            aggregates: vec![LogicalAggregateExpr {
                func: AggregateFunc::Sum,
                arg: Some(int_column("value", 1)),
                direct_arg: None,
                order_by: None,
                distinct: false,
                output_name: "sum".to_owned(),
                data_type: DataType::Int64,
            }],
            schema: Schema::new([Field::required("sum", DataType::Int64)]).expect("agg schema"),
        };
        let projected_aggregate = LogicalPlan::Project {
            input: Box::new(aggregate.clone()),
            exprs: vec![(int_column("sum", 0), "sum".to_owned())],
            schema: Schema::new([Field::required("sum", DataType::Int64)]).expect("project schema"),
        };
        assert!(
            Session::<tokio::io::DuplexStream>::is_scalar_aggregate_shape(&projected_aggregate)
        );
        assert_eq!(
            Session::<tokio::io::DuplexStream>::scalar_aggregate_source_table(&projected_aggregate),
            Some("t".to_owned())
        );
        let mut grouped = aggregate.clone();
        if let LogicalPlan::Aggregate { group_by, .. } = &mut grouped {
            group_by.push(int_column("id", 0));
        }
        assert!(!Session::<tokio::io::DuplexStream>::is_scalar_aggregate_shape(&grouped));

        let insert_values = LogicalPlan::Insert {
            table: "t".to_owned(),
            columns: Vec::new(),
            source: Box::new(LogicalPlan::Values {
                rows: vec![vec![ScalarExpr::Literal {
                    value: Value::Int32(1),
                    data_type: DataType::Int32,
                }]],
                schema: Schema::new([Field::required("id", DataType::Int32)])
                    .expect("values schema"),
            }),
            on_conflict: None,
            returning: Vec::new(),
            schema: Schema::empty(),
        };
        assert!(Session::<tokio::io::DuplexStream>::is_trivial_insert_values(&insert_values));
        assert_eq!(
            Session::<tokio::io::DuplexStream>::dml_target_table(&insert_values),
            Some("t")
        );
        let mut session = test_session();
        session
            .pending_table_modifications
            .insert("t".to_owned(), u64::MAX);
        let err = session
            .note_dml_effect(&insert_values, 1)
            .expect_err("pending DML counter overflow must not saturate");
        assert_eq!(err.sqlstate(), "22003");
        assert!(session.pending_logical_changes.is_empty());
        assert_eq!(
            Session::<tokio::io::DuplexStream>::dml_change_kind(&insert_values),
            Some(LogicalChangeKind::Insert)
        );
        assert_eq!(
            Session::<tokio::io::DuplexStream>::dml_change_kind(&update),
            Some(LogicalChangeKind::Update)
        );
        let delete = LogicalPlan::Delete {
            table: "t".to_owned(),
            input: Box::new(scan),
            returning: Vec::new(),
            schema: Schema::empty(),
        };
        assert_eq!(
            Session::<tokio::io::DuplexStream>::dml_change_kind(&delete),
            Some(LogicalChangeKind::Delete)
        );
        assert_eq!(
            Session::<tokio::io::DuplexStream>::dml_target_table(&LogicalPlan::Empty {
                schema: Schema::empty(),
            }),
            None
        );

        let messages = vec![BackendMessage::CommandComplete {
            tag: "INSERT 0 9".to_owned(),
        }];
        assert_eq!(
            Session::<tokio::io::DuplexStream>::parse_affected_rows_tag(&messages),
            9
        );
        assert_eq!(
            Session::<tokio::io::DuplexStream>::parse_command_rows_tag(&messages),
            9
        );
        assert_eq!(
            Session::<tokio::io::DuplexStream>::parse_affected_rows_tag(&[
                BackendMessage::CommandComplete {
                    tag: "SELECT 9".to_owned(),
                }
            ]),
            0
        );
        assert_eq!(
            Session::<tokio::io::DuplexStream>::parse_command_rows_tag(&[]),
            0
        );
    }

    #[test]
    fn materialized_view_row_flush_rejects_counter_overflow() {
        let mut session = test_session();
        let runtime = Arc::new(crate::MaterializedViewRuntime {
            view_table: "mv_t".to_owned(),
            source_table: "t".to_owned(),
            source: scan_plan(),
            materialized_rows: std::sync::atomic::AtomicU64::new(u64::MAX),
        });
        session
            .pending_materialized_view_rows
            .push((Arc::clone(&runtime), 1));

        let err = session
            .flush_pending_materialized_view_rows()
            .expect_err("materialized view row counter overflow must not wrap");

        assert_eq!(err.sqlstate(), "22003");
        assert_eq!(
            runtime
                .materialized_rows
                .load(std::sync::atomic::Ordering::Acquire),
            u64::MAX
        );
    }

    #[test]
    fn finalise_autocommit_reports_abort_failure_with_original_error() {
        let mut session = test_session();
        let txn = session
            .state
            .txn_manager
            .begin(IsolationLevel::ReadCommitted);
        let stale = txn.clone();
        session.state.txn_manager.abort(txn).expect("pre-abort");

        let err = session
            .finalise_autocommit(
                &scan_plan(),
                stale,
                Err(ServerError::Unsupported("executor boom")),
            )
            .expect_err("autocommit cleanup failure must be visible");
        let msg = err.to_string();
        assert!(
            msg.contains("autocommit rollback after statement error"),
            "unexpected error: {err}"
        );
        assert!(msg.contains("executor boom"), "original error lost: {err}");
        assert!(
            msg.contains("transaction abort failed"),
            "abort failure hidden: {err}"
        );
    }

    #[test]
    fn finalise_autocommit_reports_read_commit_failure() {
        let mut session = test_session();
        let txn = session
            .state
            .txn_manager
            .begin(IsolationLevel::ReadCommitted);
        let stale = txn.clone();
        session.state.txn_manager.commit(txn).expect("pre-commit");

        let err = session
            .finalise_autocommit(
                &scan_plan(),
                stale,
                Ok(SelectResult {
                    messages: Vec::new(),
                    streamed_body: None,
                    shared_streamed_body: None,
                    rows: 0,
                }),
            )
            .expect_err("read autocommit commit failure must be visible");
        let msg = err.to_string();
        assert!(
            msg.contains("autocommit statement commit"),
            "context missing: {err}"
        );
        assert!(msg.contains("commit"), "commit failure hidden: {err}");
    }

    #[test]
    fn catalog_rollback_reports_abort_failure_with_original_error() {
        let session = test_session();
        let txn = session
            .state
            .txn_manager
            .begin(IsolationLevel::ReadCommitted);
        let stale = txn.clone();
        session.state.txn_manager.abort(txn).expect("pre-abort");

        let err = session.rollback_catalog_transaction_after_error(
            stale,
            ServerError::ddl("catalog boom"),
            "CREATE TABLE catalog rollback after persist error",
        );
        let msg = err.to_string();
        assert!(
            msg.contains("CREATE TABLE catalog rollback after persist error"),
            "unexpected error: {err}"
        );
        assert!(msg.contains("catalog boom"), "original error lost: {err}");
        assert!(
            msg.contains("transaction abort failed"),
            "abort failure hidden: {err}"
        );
    }

    #[test]
    fn materialized_view_maintenance_rollback_reports_abort_failure_with_original_error() {
        let session = test_session();
        let txn = session
            .state
            .txn_manager
            .begin(IsolationLevel::ReadCommitted);
        let stale = txn.clone();
        session.state.txn_manager.abort(txn).expect("pre-abort");

        let err = session.rollback_materialized_view_maintenance_after_error(
            stale,
            ServerError::ddl("maintenance boom"),
            "materialized-view maintenance rollback after delta error",
        );
        let msg = err.to_string();
        assert!(
            msg.contains("materialized-view maintenance rollback after delta error"),
            "unexpected error: {err}"
        );
        assert!(
            msg.contains("maintenance boom"),
            "original error lost: {err}"
        );
        assert!(
            msg.contains("transaction abort failed"),
            "abort failure hidden: {err}"
        );
    }

    #[test]
    fn read_transaction_commit_reports_commit_failure_with_context() {
        let session = test_session();
        let txn = session
            .state
            .txn_manager
            .begin(IsolationLevel::ReadCommitted);
        let stale = txn.clone();
        session.state.txn_manager.commit(txn).expect("pre-commit");

        let err = session
            .finalise_read_transaction(stale, "read cleanup commit")
            .expect_err("stale read commit must fail");
        let msg = err.to_string();
        assert!(
            msg.contains("read cleanup commit"),
            "context missing: {err}"
        );
        assert!(msg.contains("commit"), "commit failure hidden: {err}");
    }

    #[test]
    fn read_maintenance_transaction_reports_commit_failure() {
        let session = test_session();
        let txn = session
            .state
            .txn_manager
            .begin(IsolationLevel::ReadCommitted);
        let stale = txn.clone();
        session.state.txn_manager.commit(txn).expect("pre-commit");

        let err = session
            .finalise_read_maintenance_transaction(
                stale,
                Ok(()),
                "maintenance commit",
                "maintenance rollback",
            )
            .expect_err("maintenance commit failure must be visible");
        let msg = err.to_string();
        assert!(msg.contains("maintenance commit"), "context missing: {err}");
        assert!(msg.contains("commit"), "commit failure hidden: {err}");
    }

    #[test]
    fn read_maintenance_transaction_reports_abort_failure_with_original_error() {
        let session = test_session();
        let txn = session
            .state
            .txn_manager
            .begin(IsolationLevel::ReadCommitted);
        let stale = txn.clone();
        session.state.txn_manager.abort(txn).expect("pre-abort");

        let err = session
            .finalise_read_maintenance_transaction(
                stale,
                Err(ServerError::ddl("maintenance boom")),
                "maintenance commit",
                "maintenance rollback",
            )
            .expect_err("maintenance rollback failure must be visible");
        let msg = err.to_string();
        assert!(
            msg.contains("maintenance rollback"),
            "context missing: {err}"
        );
        assert!(
            msg.contains("maintenance boom"),
            "original error lost: {err}"
        );
        assert!(
            msg.contains("transaction abort failed"),
            "abort failure hidden: {err}"
        );
    }

    #[test]
    fn backup_hot_standby_and_single_text_helpers_cover_admin_edges() {
        assert_eq!(
            Session::<tokio::io::DuplexStream>::try_parse_backup_function(
                "SELECT pg_start_backup('label');"
            ),
            Some("pg_start_backup")
        );
        assert_eq!(
            Session::<tokio::io::DuplexStream>::try_parse_backup_function(
                "select pg_backup_stop()"
            ),
            Some("pg_stop_backup")
        );
        assert_eq!(
            Session::<tokio::io::DuplexStream>::try_parse_backup_function("SELECT 1"),
            None
        );

        for sql in [
            "",
            "SELECT 1",
            "SHOW client_encoding",
            "EXPLAIN SELECT 1",
            "WITH x AS (SELECT 1) SELECT * FROM x",
            "VALUES (1)",
            "COPY t TO STDOUT",
        ] {
            assert!(Session::<tokio::io::DuplexStream>::hot_standby_allows(sql));
        }
        for sql in ["INSERT INTO t VALUES (1)", "COPY t FROM STDIN"] {
            assert!(!Session::<tokio::io::DuplexStream>::hot_standby_allows(sql));
        }

        let result = Session::<tokio::io::DuplexStream>::single_text_select("answer", "42");
        assert_eq!(result.rows, 1);
        assert_eq!(first_data_row_text(&result), "42");

        let mut session = test_session();
        let txn = session
            .state
            .txn_manager
            .begin(IsolationLevel::ReadCommitted);
        session.txn_state = TxnState::InTransaction(txn);
        let err = session.fail_if_in_transaction(ServerError::Unsupported("boom"));
        assert!(matches!(err, ServerError::Unsupported("boom")));
        assert!(matches!(session.txn_state, TxnState::Failed(_)));
    }
}
