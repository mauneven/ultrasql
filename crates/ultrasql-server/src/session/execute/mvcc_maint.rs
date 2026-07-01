//! Materialized-view maintenance and fused-delete fast path.

use super::*;

impl<RW> Session<RW>
where
    RW: AsyncRead + AsyncWrite + Unpin,
{
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

    pub(crate) fn materialized_views_for_source(
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

    pub(crate) fn reject_non_append_materialized_view_source_write(
        &self,
        plan: &LogicalPlan,
    ) -> Result<(), ServerError> {
        let table = match plan {
            LogicalPlan::Update { table, .. } | LogicalPlan::Delete { table, .. } => table,
            LogicalPlan::Merge { target, .. } => target,
            _ => return Ok(()),
        };
        if self.materialized_views_for_source(table).is_empty() {
            return Ok(());
        }
        Err(ServerError::Unsupported(
            "UPDATE/DELETE/MERGE on append-only materialized view source is not supported",
        ))
    }

    /// Hash-bucket key for `plan` in [`Self::prechecked_fast_dml`], or
    /// `None` when the shape is not eligible for the precheck cache.
    ///
    /// The key is the `Arc`'s allocation address, used only to index the
    /// map; identity is settled by [`Arc::ptr_eq`] against the stored
    /// `Arc` (see [`Self::fast_dml_prechecked`]), so the address is never
    /// trusted on its own.
    pub(crate) fn prechecked_fast_dml_key(plan: &Arc<LogicalPlan>) -> Option<usize> {
        if matches!(**plan, LogicalPlan::Delete { .. }) {
            Some(Arc::as_ptr(plan).cast::<()>() as usize)
        } else {
            None
        }
    }

    /// `true` iff `owner`'s static DML checks were already cached.
    ///
    /// `owner` is the pointer-stable `stmt_cache` `Arc` driving this
    /// execution (or `None` for the cold / view-rewrite paths, which never
    /// cache). A hit requires both an address match *and* [`Arc::ptr_eq`]
    /// against the stored `Arc`, so a recycled heap address belonging to a
    /// different plan can never be mistaken for a cached one.
    pub(crate) fn fast_dml_prechecked(&self, owner: Option<&Arc<LogicalPlan>>) -> bool {
        owner.is_some_and(|arc| {
            Self::prechecked_fast_dml_key(arc).is_some_and(|key| {
                self.prechecked_fast_dml
                    .borrow()
                    .get(&key)
                    .is_some_and(|cached| Arc::ptr_eq(cached, arc))
            })
        })
    }

    pub(crate) fn fast_dml_checks_cacheable(&self, plan: &LogicalPlan) -> bool {
        let LogicalPlan::Delete {
            table,
            input,
            returning,
            ..
        } = plan
        else {
            return false;
        };
        returning.is_empty()
            && self.state.row_security.is_empty()
            && self.materialized_views_for_source(table).is_empty()
            && Self::fused_delete_int32_pair_predicate(table, input).is_some()
    }

    pub(crate) fn try_run_fused_delete_in_explicit_txn(
        &self,
        plan: &LogicalPlan,
        catalog_snapshot: &CatalogSnapshot,
        txn: &Transaction,
    ) -> Result<Option<Result<SelectResult, ServerError>>, ServerError> {
        let LogicalPlan::Delete {
            table,
            input,
            returning,
            ..
        } = plan
        else {
            return Ok(None);
        };
        if !returning.is_empty() {
            return Ok(None);
        }
        // SERIALIZABLE gate: the fused in-place DELETE bypasses
        // `run_plan_in_txn`, which is where `record_serializable_predicate_locks`
        // and `record_serializable_write_conflicts` run. Short-circuiting here
        // under SERIALIZABLE would drop this DELETE's SIREAD predicate lock (for
        // its WHERE-scan read) and its write-conflict registration — an SSI
        // serialization hole: a concurrent read-write dependency would go
        // undetected. Fall through to the general MVCC DELETE path, which
        // records both. READ COMMITTED / REPEATABLE READ record no predicate
        // locks, so the fast path stays safe there. Mirrors the SERIALIZABLE
        // guard on the cached int32-pair SELECT fast path in `run_plan_in_txn`.
        if txn.isolation == IsolationLevel::Serializable {
            return Ok(None);
        }
        // No-savepoint gate: when a savepoint is open, fall through to the
        // general MVCC DELETE path. The fused in-place DELETE writes its
        // `xmax` stamp directly to the page; routing it through the general
        // path (which stamps via the operator's `ctx.xid == current_xid()`
        // and records per-relation undo) keeps `ROLLBACK TO` able to revert
        // the delete cleanly and guarantees no corruption even if a future
        // refactor mis-stamps the fused path. The stamp itself is already
        // correct (`write_xid()`, fixed in Phase 0); this gate is the
        // belt-and-suspenders "safe milestone" guard from the design.
        if txn.subtxn_stack.depth() > 0 {
            return Ok(None);
        }
        let entry = catalog_snapshot
            .tables
            .get(&table.to_ascii_lowercase())
            .ok_or_else(|| {
                ServerError::Plan(ultrasql_planner::PlanError::TableNotFound(
                    table.to_string(),
                ))
            })?;
        let fields = entry.schema.fields();
        if fields.len() != 2
            || fields[0].data_type != DataType::Int32
            || fields[1].data_type != DataType::Int32
        {
            return Ok(None);
        }
        if catalog_snapshot
            .indexes_by_table
            .get(&entry.oid)
            .is_some_and(|indexes| !indexes.is_empty())
            || self.has_referenced_by_delete_checks(entry.oid)
        {
            return Ok(None);
        }
        let Some(predicate) = Self::fused_delete_int32_pair_predicate(table, input) else {
            return Ok(None);
        };

        let rel = RelationId(entry.oid);
        let block_count = self.state.heap.block_count(rel).max(entry.n_blocks);
        let wal_sink_arc = self.state.heap.wal_sink().cloned();
        let wal_sink = wal_sink_arc.as_deref();
        let scan = DeleteInt32PairScan {
            rel,
            block_count,
            snapshot: &txn.snapshot,
            oracle: self.state.txn_manager.as_ref(),
            predicate,
        };
        let write_xid = txn.write_xid();
        txn.debug_assert_stamp(write_xid);
        let stamp = DeleteInt32PairStamp {
            // Stamp the active subtransaction XID (parent when no savepoint
            // is open). MUST be `write_xid()`, not `txn.xid` — stamping the
            // parent here is the fused-DELETE corruption root the first
            // attempt was reverted for. See `Transaction::write_xid`.
            xid: write_xid,
            command_id: txn.current_command,
        };
        let deleted = if let Some(wal_sink) = wal_sink {
            self.state.heap.delete_int32_pair_inplace_parallel_wal(
                scan,
                stamp,
                wal_sink,
                Some(self.state.vm.as_ref()),
            )
        } else {
            self.state.heap.delete_int32_pair_inplace_parallel_no_wal(
                scan,
                stamp,
                Some(self.state.vm.as_ref()),
            )
        }
        .map_err(|err| {
            ServerError::Execute(ultrasql_executor::ExecError::TypeMismatch(err.to_string()))
        });

        let result = deleted.and_then(|rows| {
            let rows = u64::try_from(rows).map_err(|_| {
                ServerError::Execute(ultrasql_executor::ExecError::NumericFieldOverflow(
                    "DELETE affected row count overflow".to_owned(),
                ))
            })?;
            Ok(SelectResult {
                messages: vec![BackendMessage::CommandComplete {
                    tag: format!("DELETE {rows}"),
                }],
                streamed_body: None,
                shared_streamed_body: None,
                streaming: None,
                rows,
            })
        });
        Ok(Some(result))
    }

    fn has_referenced_by_delete_checks(&self, parent_oid: Oid) -> bool {
        if self.state.table_constraints.is_empty() {
            return false;
        }
        self.state.table_constraints.iter().any(|item| {
            item.value().foreign_keys.iter().any(|fk| {
                fk.target_oid == parent_oid
                    && !(fk.deferrable
                        && fk.initially_deferred
                        && fk.on_delete == LogicalReferentialAction::NoAction)
            })
        })
    }

    pub(crate) fn fused_delete_int32_pair_predicate(
        target_table: &str,
        input: &LogicalPlan,
    ) -> Option<Int32PairPredicate> {
        match input {
            LogicalPlan::Scan { table, .. } if table.eq_ignore_ascii_case(target_table) => {
                Some(Int32PairPredicate::All)
            }
            LogicalPlan::Filter { input, predicate } => {
                let LogicalPlan::Scan { table, .. } = input.as_ref() else {
                    return None;
                };
                if !table.eq_ignore_ascii_case(target_table) {
                    return None;
                }
                let (col_index, op, literal) = Self::extract_int32_col_cmp_lit(predicate)?;
                if col_index > 1 {
                    return None;
                }
                Some(Int32PairPredicate::ColumnCmp {
                    col_index: u8::try_from(col_index).ok()?,
                    op,
                    literal,
                })
            }
            _ => None,
        }
    }

    fn extract_int32_col_cmp_lit(expr: &ScalarExpr) -> Option<(usize, Int32PairCmp, i32)> {
        let ScalarExpr::Binary {
            op, left, right, ..
        } = expr
        else {
            return None;
        };
        let cmp = Self::binary_cmp_to_int32_pair_cmp(*op)?;
        let col_idx_from = |expr: &ScalarExpr| match expr {
            ScalarExpr::Column {
                index,
                data_type: DataType::Int32,
                ..
            } => Some(*index),
            _ => None,
        };
        let lit_from = |expr: &ScalarExpr| match expr {
            ScalarExpr::Literal {
                value: Value::Int32(value),
                ..
            } => Some(*value),
            _ => None,
        };

        if let (Some(col), Some(lit)) = (col_idx_from(left), lit_from(right)) {
            Some((col, cmp, lit))
        } else if let (Some(lit), Some(col)) = (lit_from(left), col_idx_from(right)) {
            Some((col, mirror_int32_pair_cmp(cmp), lit))
        } else {
            None
        }
    }

    fn binary_cmp_to_int32_pair_cmp(op: BinaryOp) -> Option<Int32PairCmp> {
        match op {
            BinaryOp::Eq => Some(Int32PairCmp::Eq),
            BinaryOp::NotEq => Some(Int32PairCmp::Ne),
            BinaryOp::Lt => Some(Int32PairCmp::Lt),
            BinaryOp::LtEq => Some(Int32PairCmp::Le),
            BinaryOp::Gt => Some(Int32PairCmp::Gt),
            BinaryOp::GtEq => Some(Int32PairCmp::Ge),
            _ => None,
        }
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
            // Materialized-view maintenance INSERT reads only the row
            // count and needs a complete body; never stream.
            allow_streaming: false,
            streaming_commit_txn: None,
        })?;
        Ok(result.rows)
    }
}
