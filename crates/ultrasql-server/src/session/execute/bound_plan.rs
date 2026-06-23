//! Cached bound-plan execution and fast-path shape detection (fused DML, scalar aggregate, fast insert).

use super::*;

impl<RW> Session<RW>
where
    RW: AsyncRead + AsyncWrite + Unpin,
{
    /// Hot-path entry for a SQL string that has already been parsed +
    /// bound by an earlier `execute_query` call. The bound plan was
    /// cached in [`Self::stmt_cache`] so we skip the parser, binder,
    /// and (for the DML/SELECT shapes that survive the cache filter)
    /// the meta-statement and DDL dispatchers. The optimizer + lowerer
    /// run as usual; the optimizer's own `PlanCache` provides the
    /// second layer of memoisation.
    pub(crate) fn execute_bound_plan(
        &mut self,
        plan: LogicalPlan,
        sql: &str,
        catalog_snapshot: Arc<CatalogSnapshot>,
        allow_streaming: bool,
    ) -> Result<SelectResult, ServerError> {
        if matches!(self.txn_state, TxnState::Failed(_)) {
            return Err(ServerError::TransactionAborted);
        }
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
        // `optimised_plan` is rebuilt from the bound plan each call, so it
        // carries no stable identity for the precheck cache.
        self.run_dml_or_select(&optimised_plan, &catalog_snapshot, None, allow_streaming)
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

    /// `true` iff `plan` is a `Delete` whose source is a bare `Scan` or
    /// `Filter(Scan)` shape — the exact envelope that the fused DELETE
    /// path (`try_build_fused_delete`) recognises before it revalidates
    /// the table schema, predicate type, indexes, and FK restrictions.
    ///
    /// This mirrors [`Self::is_fused_update_shape`]. The benchmark's
    /// bulk DELETE uses a fresh table name per sample, so the
    /// SQL-text optimizer cache cannot hit; bypassing the optimizer for
    /// this already-specialized leaf shape removes planner overhead
    /// without changing the correctness gate in the lowerer.
    pub(crate) fn is_fused_delete_shape(plan: &LogicalPlan) -> bool {
        let LogicalPlan::Delete {
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

    pub(crate) fn scalar_aggregate_source_table(plan: &LogicalPlan) -> Option<String> {
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

    pub(crate) fn can_use_cached_scalar_aggregate_in_explicit_txn(
        &self,
        plan: &LogicalPlan,
    ) -> bool {
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

    /// MVCC snapshot to gate the cached-aggregate fast path inside an
    /// explicit transaction.
    ///
    /// `can_use_cached_scalar_aggregate_in_explicit_txn` only returns `true`
    /// for a `ReadCommitted` `InTransaction` state, so a fresh statement
    /// snapshot (committed state now, plus this txn's own writes via its
    /// xid) is the correct read-committed view to test against
    /// [`ColumnCache::is_snapshot_coherent`]. Falls back to an empty
    /// read-only snapshot if somehow called outside a live transaction; the
    /// gate then simply governs admission like the autocommit path.
    pub(crate) fn current_txn_snapshot(&self) -> ultrasql_mvcc::Snapshot {
        match &self.txn_state {
            TxnState::InTransaction(txn) => self
                .state
                .txn_manager
                .statement_snapshot(txn.xid, txn.current_command),
            _ => self
                .state
                .txn_manager
                .statement_snapshot(ultrasql_core::Xid::INVALID, ultrasql_core::CommandId::FIRST),
        }
    }

    pub(crate) fn parse_fast_insert_int32_pair_sql(
        sql: &str,
    ) -> Option<FastInsertInt32PairSql<'_>> {
        let bytes = sql.as_bytes();
        let mut pos = skip_ascii_ws(bytes, 0);
        pos = consume_keyword(bytes, pos, b"insert")?;
        let after_insert = skip_ascii_ws(bytes, pos);
        if after_insert == pos {
            return None;
        }
        pos = consume_keyword(bytes, after_insert, b"into")?;
        let after_into = skip_ascii_ws(bytes, pos);
        if after_into == pos {
            return None;
        }
        let (table, mut pos) = parse_simple_identifier(sql, after_into)?;
        let after_table = skip_ascii_ws(bytes, pos);
        if after_table == pos {
            return None;
        }
        pos = consume_keyword(bytes, after_table, b"values")?;
        let mut rows = Vec::new();
        loop {
            pos = skip_ascii_ws(bytes, pos);
            if bytes.get(pos).copied()? != b'(' {
                return None;
            }
            pos += 1;
            pos = skip_ascii_ws(bytes, pos);
            let (id, next) = parse_i32_literal(bytes, pos)?;
            pos = skip_ascii_ws(bytes, next);
            if bytes.get(pos).copied()? != b',' {
                return None;
            }
            pos += 1;
            pos = skip_ascii_ws(bytes, pos);
            let (val, next) = parse_i32_literal(bytes, pos)?;
            pos = skip_ascii_ws(bytes, next);
            if bytes.get(pos).copied()? != b')' {
                return None;
            }
            pos += 1;
            rows.push((id, val));
            pos = skip_ascii_ws(bytes, pos);
            match bytes.get(pos).copied() {
                Some(b',') => {
                    pos += 1;
                }
                Some(b';') => {
                    pos = skip_ascii_ws(bytes, pos + 1);
                    if pos == bytes.len() {
                        break;
                    }
                    return None;
                }
                None => break,
                Some(_) => return None,
            }
        }
        if rows.is_empty() {
            return None;
        }
        Some(FastInsertInt32PairSql { table, rows })
    }

    pub(crate) fn try_execute_fast_insert_int32_pair_sql(
        &mut self,
        sql: &str,
        catalog_snapshot: &CatalogSnapshot,
    ) -> Result<Option<SelectResult>, ServerError> {
        let Some(parsed) = Self::parse_fast_insert_int32_pair_sql(sql) else {
            return Ok(None);
        };
        let table_name = parsed.table.to_ascii_lowercase();
        let Some(entry) = self.lookup_fast_insert_table(&table_name, catalog_snapshot) else {
            return Ok(None);
        };
        let table_key = ultrasql_catalog::table_lookup_key(&entry.schema_name, &table_name);
        let fields = entry.schema.fields();
        if crate::is_regular_view_entry(entry)
            || fields.len() != 2
            || fields[0].data_type != DataType::Int32
            || fields[1].data_type != DataType::Int32
            || self.state.table_constraints.contains_key(&entry.oid)
            || catalog_snapshot
                .indexes_by_table
                .get(&entry.oid)
                .is_some_and(|indexes| !indexes.is_empty())
            || self.enabled_row_security(entry.oid).is_some()
            || !self.materialized_views_for_source(&table_key).is_empty()
        {
            return Ok(None);
        }
        if matches!(self.txn_state, TxnState::Failed(_)) {
            return Err(ServerError::TransactionAborted);
        }

        match std::mem::replace(&mut self.txn_state, TxnState::Idle) {
            TxnState::Idle => {
                let txn = self.state.txn_manager.begin(IsolationLevel::ReadCommitted);
                let rows = match self.fast_insert_int32_pair_rows(entry, &parsed.rows, &txn) {
                    Ok(rows) => rows,
                    Err(err) => {
                        return Err(self.rollback_transaction_after_error_with_abort_marker(
                            txn,
                            err,
                            "fast INSERT rollback after statement error",
                            true,
                        ));
                    }
                };
                let rows = match u64::try_from(rows).map_err(|_| {
                    ServerError::Execute(ultrasql_executor::ExecError::NumericFieldOverflow(
                        "INSERT affected row count overflow".to_owned(),
                    ))
                }) {
                    Ok(rows) => rows,
                    Err(err) => {
                        return Err(self.rollback_transaction_after_error_with_abort_marker(
                            txn,
                            err,
                            "fast INSERT rollback after statement error",
                            true,
                        ));
                    }
                };
                if rows > 0
                    && let Err(err) = self.state.flush_dirty_heap_pages_if_needed()
                {
                    return Err(self.rollback_transaction_after_error_with_abort_marker(
                        txn,
                        err,
                        "fast INSERT rollback after statement error",
                        true,
                    ));
                }
                self.state
                    .commit_transaction(txn, true, "fast INSERT statement")?;
                self.pending_post_commit_maintenance = true;
                self.note_fast_insert_committed_effect(&table_key, rows)?;
                Ok(Some(fast_insert_result(rows)))
            }
            TxnState::InTransaction(mut txn) => {
                self.state.txn_manager.refresh_snapshot(&mut txn);
                let outcome = self
                    .fast_insert_int32_pair_rows(entry, &parsed.rows, &txn)
                    .and_then(|rows| {
                        let rows = u64::try_from(rows).map_err(|_| {
                            ServerError::Execute(
                                ultrasql_executor::ExecError::NumericFieldOverflow(
                                    "INSERT affected row count overflow".to_owned(),
                                ),
                            )
                        })?;
                        self.note_fast_insert_pending_effect(&table_key, rows)?;
                        if rows > 0 {
                            self.state.flush_dirty_heap_pages_if_needed()?;
                        }
                        Ok(fast_insert_result(rows))
                    });
                self.txn_state = if outcome.is_ok() {
                    TxnState::InTransaction(txn)
                } else {
                    TxnState::Failed(txn)
                };
                outcome.map(Some)
            }
            TxnState::Failed(txn) => {
                self.txn_state = TxnState::Failed(txn);
                Err(ServerError::TransactionAborted)
            }
        }
    }

    fn lookup_fast_insert_table<'a>(
        &self,
        table_name: &str,
        catalog_snapshot: &'a CatalogSnapshot,
    ) -> Option<&'a TableEntry> {
        if let Some(entry) = catalog_snapshot.tables.get(table_name) {
            return Some(entry);
        }
        for schema_name in crate::search_path_schema_names(
            self.session_settings.get("search_path").map(String::as_str),
        ) {
            let table_key = ultrasql_catalog::table_lookup_key(&schema_name, table_name);
            if let Some(entry) = catalog_snapshot.tables.get(&table_key) {
                return Some(entry);
            }
        }
        None
    }

    fn fast_insert_int32_pair_rows(
        &self,
        entry: &TableEntry,
        rows: &[(i32, i32)],
        txn: &Transaction,
    ) -> Result<usize, ServerError> {
        let mut payloads = Vec::with_capacity(rows.len());
        for &(id, val) in rows {
            let mut payload = [0_u8; 9];
            payload[1..5].copy_from_slice(&id.to_le_bytes());
            payload[5..9].copy_from_slice(&val.to_le_bytes());
            payloads.push(payload);
        }
        let payload_refs = payloads
            .iter()
            .map(|payload| payload.as_slice())
            .collect::<Vec<_>>();
        let xmin = txn.write_xid();
        txn.debug_assert_stamp(xmin);
        let wal_sink_arc = self.state.heap.wal_sink().cloned();
        let tids = self
            .state
            .heap
            .insert_batch(
                RelationId(entry.oid),
                &payload_refs,
                InsertOptions {
                    // Stamp the active subtransaction XID (parent when no
                    // savepoint is open). MUST be `write_xid()`, not
                    // `txn.xid` — see `Transaction::write_xid`.
                    xmin,
                    command_id: txn.current_command,
                    n_atts: 2,
                    wal: wal_sink_arc.as_deref(),
                    fsm: None,
                    vm: Some(self.state.vm.as_ref()),
                },
            )
            .map_err(|err| {
                ServerError::Execute(ultrasql_executor::ExecError::TypeMismatch(err.to_string()))
            })?;
        Ok(tids.len())
    }

    fn note_fast_insert_pending_effect(
        &mut self,
        table: &str,
        rows: u64,
    ) -> Result<(), ServerError> {
        if rows == 0 {
            return Ok(());
        }
        let current = self
            .pending_table_modifications
            .get(table)
            .copied()
            .unwrap_or(0);
        let total = current.checked_add(rows).ok_or_else(|| {
            ServerError::Execute(ultrasql_executor::ExecError::NumericFieldOverflow(
                "pending DML row count overflow".to_owned(),
            ))
        })?;
        if self.state.logical_replication.has_publications() {
            self.pending_logical_changes.push(PendingLogicalChange {
                table: table.to_owned(),
                kind: LogicalChangeKind::Insert,
                rows_affected: rows,
            });
        }
        self.pending_table_modifications
            .insert(table.to_owned(), total);
        Ok(())
    }

    fn note_fast_insert_committed_effect(&self, table: &str, rows: u64) -> Result<(), ServerError> {
        if rows == 0 {
            return Ok(());
        }
        if self.state.logical_replication.has_publications() {
            self.state.logical_replication.record_committed_dml(
                table,
                LogicalChangeKind::Insert,
                rows,
            )?;
        }
        self.state.note_table_modifications(table, rows);
        Ok(())
    }
}
