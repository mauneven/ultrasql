//! Query execution dispatch: parse/bind/lower entrypoint, DDL/checkpoint/SET dispatch.

use super::*;

impl<RW> Session<RW>
where
    RW: AsyncRead + AsyncWrite + Unpin,
{
    /// Dispatch and execute a single statement, returning its
    /// [`SelectResult`].
    ///
    /// `allow_streaming` is propagated unchanged to every SELECT-producing
    /// sub-dispatch (`run_dml_or_select`, the EXECUTE meta path,
    /// `execute_bound_plan`). It is `true` only on the single-statement
    /// Simple-Query network path that can drive a streaming handle; every
    /// other consumer (multi-statement batch, embedded API, import) passes
    /// `false` and gets a fully buffered body. See
    /// [`Self::run_dml_or_select`] for why a streaming handle that the
    /// caller cannot drive corrupts the wire and leaks the XID.
    pub(crate) fn execute_query(
        &mut self,
        sql: &str,
        allow_streaming: bool,
    ) -> Result<SelectResult, ServerError> {
        // Pin the stable date/time builtins (`now()` / `current_timestamp` /
        // `current_date`) to the transaction-start instant and
        // `statement_timestamp()` to this statement's start, for the whole
        // dispatch. The guard restores the live-wall-clock fallback on drop.
        // This is the single convergence point for the Simple Query path
        // (single statement, multi-statement batch, and the embedded API).
        let _eval_clock = self.install_statement_eval_clock();
        // Capture a per-statement catalog snapshot — wait-free arc-swap load
        // when no transactional-DDL overlay is pending, else the committed
        // snapshot with this session's in-transaction-created relation
        // overlaid (self-yes / others-no). The binder reads this snapshot
        // first; if a name is not found there (a runtime CREATE TABLE never
        // landed it), the in-memory sample catalog provides the legacy
        // fallback.
        let catalog_snapshot: Arc<CatalogSnapshot> = self.effective_catalog_snapshot();

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

        if let Some(result) =
            self.try_execute_fast_insert_int32_pair_sql(trimmed, &catalog_snapshot)?
        {
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
                || Self::is_fused_delete_shape(&plan_arc)
                || Self::is_scalar_aggregate_shape(&plan_arc)
            {
                if !self.state.regular_views.is_empty() {
                    let prepared = self.prepare_regular_view_plan(&plan_arc, &catalog_snapshot)?;
                    if Self::is_trivial_insert_values(&prepared)
                        || Self::is_fused_update_shape(&prepared)
                        || Self::is_fused_delete_shape(&prepared)
                        || Self::is_scalar_aggregate_shape(&prepared)
                    {
                        // The view-rewrite produces a fresh `prepared` plan
                        // every call, so it has no stable identity to cache
                        // against — pass `None` and run the full checks.
                        return self.run_dml_or_select(
                            &prepared,
                            &catalog_snapshot,
                            None,
                            allow_streaming,
                        );
                    }
                    return self.execute_bound_plan(
                        prepared,
                        sql,
                        catalog_snapshot,
                        allow_streaming,
                    );
                }
                // Stable path: `plan_arc` is the pointer-stable `stmt_cache`
                // entry, so it can key the precheck cache by Arc identity.
                return self.run_dml_or_select(
                    &plan_arc,
                    &catalog_snapshot,
                    Some(&plan_arc),
                    allow_streaming,
                );
            }
            let plan = Arc::unwrap_or_clone(plan_arc);
            return self.execute_bound_plan(plan, sql, catalog_snapshot, allow_streaming);
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
            self.try_dispatch_meta_statement(&stmt, Arc::clone(&catalog_snapshot), allow_streaming)?
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
        let plan = match self.prepare_regular_view_plan(&plan, &catalog_snapshot) {
            Ok(p) => p,
            Err(e) => return Err(self.fail_if_in_transaction(e)),
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

        if matches!(&plan, LogicalPlan::Checkpoint { .. }) {
            return self.execute_checkpoint(&plan);
        }
        if matches!(&plan, LogicalPlan::SetVariable { .. }) {
            return self.execute_set_variable(&plan, true);
        }
        if matches!(&plan, LogicalPlan::SetRole { .. }) {
            return self.execute_set_role(&plan);
        }
        if matches!(&plan, LogicalPlan::Describe { .. }) {
            return self.execute_describe(&plan, true, &[]);
        }
        if matches!(&plan, LogicalPlan::ExportDatabase { .. }) {
            return self.execute_export_database(&plan);
        }
        if matches!(&plan, LogicalPlan::ImportDatabase { .. }) {
            return self.execute_import_database(&plan);
        }
        // DDL is dispatched ahead of operator lowering: it never produces
        // rows, so the lowerer would only round-trip it through an
        // unreachable arm. DDL inside an explicit transaction is
        // rejected today because the catalog mutations are not
        // transactional under the v0.5 catalog (see AGENTS.md §11 and
        // `docs/transactional-ddl-design.md`; lifting this gate without
        // the catalog-overlay work is silent schema corruption). The
        // rejection returns SQLSTATE `0A000` (feature_not_supported) with
        // an autocommit HINT, and transitions the txn to `Failed` so
        // subsequent statements get SQLSTATE `25P02` until COMMIT/ROLLBACK.
        let is_ddl = matches!(
            &plan,
            LogicalPlan::CreateTable { .. }
                | LogicalPlan::CreateMaterializedView { .. }
                | LogicalPlan::CreateView { .. }
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
                | LogicalPlan::Checkpoint { .. }
                | LogicalPlan::ExportDatabase { .. }
                | LogicalPlan::ImportDatabase { .. }
                | LogicalPlan::DropTable { .. }
                | LogicalPlan::AlterTable { .. }
                | LogicalPlan::AlterView { .. }
                | LogicalPlan::Truncate { .. }
        );
        if is_ddl
            && !Self::is_transactional_ddl_supported(&plan)
            && matches!(self.txn_state, TxnState::InTransaction(_))
        {
            return Err(self.fail_if_in_transaction(ServerError::DdlInTransaction));
        }
        match &plan {
            LogicalPlan::CreateTable { .. } => {
                return self.execute_create_table(&plan, &catalog_snapshot);
            }
            LogicalPlan::CreateMaterializedView { .. } => {
                return self.execute_create_materialized_view(&plan, &catalog_snapshot);
            }
            LogicalPlan::CreateView { .. } => {
                return self.execute_create_view(&plan, &catalog_snapshot);
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
            LogicalPlan::Checkpoint { .. } => {
                return self.execute_checkpoint(&plan);
            }
            LogicalPlan::ExportDatabase { .. } => {
                return self.execute_export_database(&plan);
            }
            LogicalPlan::ImportDatabase { .. } => {
                return self.execute_import_database(&plan);
            }
            LogicalPlan::DropTable { .. } => {
                return self.execute_drop_table(&plan);
            }
            LogicalPlan::AlterTable { .. } => {
                return self.execute_alter_table(&plan, &catalog_snapshot);
            }
            LogicalPlan::AlterView { .. } => {
                return self.execute_alter_view(&plan, &catalog_snapshot);
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
        let executable_plan = match self.prepare_regular_view_plan(&plan, &catalog_snapshot) {
            Ok(plan) => plan,
            Err(e) => return Err(self.fail_if_in_transaction(e)),
        };
        let optimised_plan = if Self::is_trivial_insert_values(&executable_plan)
            || Self::is_fused_update_shape(&executable_plan)
            || Self::is_fused_delete_shape(&executable_plan)
            || Self::is_scalar_aggregate_shape(&executable_plan)
        {
            executable_plan
        } else {
            match self.optimize_dml_plan(sql, executable_plan, &catalog_snapshot) {
                Ok(p) => p,
                Err(e) => return Err(self.fail_if_in_transaction(e)),
            }
        };
        // Cold path: `optimised_plan` is a freshly-allocated local with no
        // stable identity to cache against.
        self.run_dml_or_select(&optimised_plan, &catalog_snapshot, None, allow_streaming)
    }

    /// Whether `plan` is a DDL statement that transactional DDL supports
    /// running inside an explicit transaction block.
    ///
    /// Covers `CREATE TABLE` (milestone 1/2) and a `txn-safe` `CREATE INDEX`
    /// (milestone 3), and only when neither creates a non-MVCC sidecar that
    /// cannot be transactionally rolled back. The per-handler in-transaction
    /// path applies the final scoping check (e.g. a serial/sequence-bearing
    /// `CREATE TABLE` is still rejected because the sequence-create WAL is
    /// replayed unconditionally on restart; a `CREATE INDEX` on a same-txn
    /// created table or while a SAVEPOINT is active is rejected there too).
    /// Every other DDL stays rejected-in-transaction with SQLSTATE `0A000`.
    pub(crate) fn is_transactional_ddl_supported(plan: &LogicalPlan) -> bool {
        match plan {
            LogicalPlan::CreateTable { .. } => true,
            LogicalPlan::CreateIndex { .. } => Self::create_index_is_txn_safe(plan),
            LogicalPlan::AlterTable { .. } => Self::alter_table_is_txn_safe(plan),
            LogicalPlan::DropTable { .. } => Self::drop_table_is_txn_safe(plan),
            _ => false,
        }
    }

    /// Whether a `DROP TABLE` plan is a shape transactional DDL can stage in the
    /// session overlay as a negative mask and publish at COMMIT (milestone 5).
    ///
    /// Only the SYNTACTIC reject lives here: `CASCADE` expands one DROP into a
    /// transitive closure of dependent views / matviews / chunks and severs FK
    /// edges on SURVIVING tables in place — non-MVCC cross-table mutation the
    /// negative-mask overlay cannot model — so it keeps the gate's `0A000` and
    /// stays fully supported in autocommit. A plain `RESTRICT` (non-CASCADE)
    /// `DROP TABLE` (with or without `IF EXISTS`) is admitted here; the in-txn
    /// handler ([`Session::execute_drop_table_in_txn`]) applies the full
    /// STATE-dependent reject predicate against the resolved target (owned
    /// sequence, RLS, dependent view, time-partition parent/chunk, inbound /
    /// outbound FK, columnar shadow / custom stats / comments, a system table,
    /// or an active SAVEPOINT) — each of which keeps `0A000`.
    pub(crate) fn drop_table_is_txn_safe(plan: &LogicalPlan) -> bool {
        let LogicalPlan::DropTable { cascade, .. } = plan else {
            return false;
        };
        !*cascade
    }

    /// Whether an `ALTER TABLE` plan is a sub-action transactional DDL can stage
    /// in the session overlay and publish at COMMIT (milestone 4).
    ///
    /// Only the CATALOG-ONLY subset qualifies — the sub-actions whose entire
    /// durable effect is a `pg_class` / `pg_attribute` row (plus an in-memory
    /// runtime-default / privilege-grant / time-partition side map the overlay
    /// can revert from a before-image):
    ///
    /// - `RENAME TO` / `RENAME COLUMN` (name-only catalog edits),
    /// - `ALTER COLUMN SET/DROP DEFAULT` (runtime default + `atthasdef`),
    /// - `ALTER COLUMN SET/DROP NOT NULL` (a nullability flag; `SET NOT NULL`
    ///   validate-scans existing rows under the user snapshot),
    /// - `SET (...)` storage options.
    ///
    /// Every other action — `ADD`/`DROP COLUMN` (heap rewrite),
    /// `ADD`/`DROP CONSTRAINT` (a backing index / runtime predicate that is a
    /// separate durable artifact), `ENABLE ROW LEVEL SECURITY`, and any future
    /// `ALTER TYPE` — writes state the whole-transaction overlay cannot
    /// transactionally undo, so it keeps the gate's `0A000` in-transaction and
    /// stays fully supported in autocommit.
    ///
    /// Syntactic shape check only; the in-txn handler applies the final scoping
    /// check (an active SAVEPOINT and a time-partitioned target table are
    /// rejected there too).
    pub(crate) fn alter_table_is_txn_safe(plan: &LogicalPlan) -> bool {
        let LogicalPlan::AlterTable { action, .. } = plan else {
            return false;
        };
        matches!(
            action,
            ultrasql_planner::LogicalAlterTableAction::RenameTable { .. }
                | ultrasql_planner::LogicalAlterTableAction::RenameColumn { .. }
                | ultrasql_planner::LogicalAlterTableAction::AlterColumnSetDefault { .. }
                | ultrasql_planner::LogicalAlterTableAction::AlterColumnDropDefault { .. }
                | ultrasql_planner::LogicalAlterTableAction::AlterColumnSetNotNull { .. }
                | ultrasql_planner::LogicalAlterTableAction::AlterColumnDropNotNull { .. }
                | ultrasql_planner::LogicalAlterTableAction::SetOptions { .. }
        )
    }

    /// Whether a `CREATE INDEX` plan is a shape transactional DDL can stage in
    /// the session overlay and build at COMMIT (milestone 3).
    ///
    /// Only a plain B-tree index — no expression keys, no partial predicate, no
    /// `INCLUDE`, no `CONCURRENTLY` — qualifies. Every other shape writes a
    /// durable artifact the whole-transaction overlay cannot roll back:
    /// expression / partial / `INCLUDE` indexes and the non-B-tree access
    /// methods (Hash / BRIN / Aggregating / IvfFlat / Hnsw) install the
    /// non-MVCC `RuntimeIndexMetadata` sidecar, and `CONCURRENTLY` is a
    /// multi-transaction protocol that cannot run inside one. Those keep the
    /// gate's `0A000` in-transaction and remain fully supported in autocommit.
    ///
    /// This is a syntactic shape check only; the in-txn handler additionally
    /// rejects a target table created earlier in the same transaction and an
    /// active SAVEPOINT (mirroring the `CREATE TABLE` reject).
    pub(crate) fn create_index_is_txn_safe(plan: &LogicalPlan) -> bool {
        let LogicalPlan::CreateIndex {
            columns,
            include_columns,
            predicate,
            method,
            concurrently,
            ..
        } = plan
        else {
            return false;
        };
        // A bare-column B-tree carries its key columns in `columns`; an
        // expression index leaves `columns` empty and stores `key_exprs`.
        !columns.is_empty()
            && include_columns.is_empty()
            && predicate.is_none()
            && !*concurrently
            && matches!(method, ultrasql_planner::LogicalIndexMethod::Btree)
    }

    pub(crate) fn is_ddl_plan(plan: &LogicalPlan) -> bool {
        matches!(
            plan,
            LogicalPlan::CreateTable { .. }
                | LogicalPlan::CreateMaterializedView { .. }
                | LogicalPlan::CreateView { .. }
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
                | LogicalPlan::Checkpoint { .. }
                | LogicalPlan::ExportDatabase { .. }
                | LogicalPlan::ImportDatabase { .. }
                | LogicalPlan::DropTable { .. }
                | LogicalPlan::AlterTable { .. }
                | LogicalPlan::AlterView { .. }
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
            LogicalPlan::CreateView { .. } => self.execute_create_view(plan, catalog_snapshot),
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
            LogicalPlan::Checkpoint { .. } => self.execute_checkpoint(plan),
            LogicalPlan::ExportDatabase { .. } => self.execute_export_database(plan),
            LogicalPlan::ImportDatabase { .. } => self.execute_import_database(plan),
            LogicalPlan::DropTable { .. } => self.execute_drop_table(plan),
            LogicalPlan::AlterTable { .. } => self.execute_alter_table(plan, catalog_snapshot),
            LogicalPlan::AlterView { .. } => self.execute_alter_view(plan, catalog_snapshot),
            LogicalPlan::Truncate { .. } => self.execute_truncate(plan, catalog_snapshot),
            _ => Err(ServerError::Unsupported("execute_ddl_plan: wrong plan")),
        }
    }

    pub(crate) fn execute_checkpoint(
        &mut self,
        plan: &LogicalPlan,
    ) -> Result<SelectResult, ServerError> {
        let LogicalPlan::Checkpoint { .. } = plan else {
            return Err(ServerError::Unsupported("execute_checkpoint: wrong plan"));
        };
        match self.txn_state {
            TxnState::Idle => {
                self.state.perform_checkpoint()?;
                Ok(run_ddl_command("CHECKPOINT"))
            }
            TxnState::InTransaction(_) => Err(self.fail_if_in_transaction(
                ServerError::Unsupported("CHECKPOINT inside an explicit transaction block"),
            )),
            TxnState::Failed(_) => Err(ServerError::TransactionAborted),
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
}
