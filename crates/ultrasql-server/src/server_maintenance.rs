//! `impl Server` methods (split out of the crate root): maintenance.
//!
//! Pure code motion from `lib.rs`; behavior unchanged.
use super::*;

impl Server {
    /// Record committed tuple modifications for `table` and trigger
    /// autovacuum ANALYZE when the threshold is crossed.
    pub fn note_table_modifications(&self, table: &str, modified_rows: u64) {
        if modified_rows == 0 {
            return;
        }

        let folded = table.to_ascii_lowercase();
        self.columnar_storage.mark_dirty(&folded);
        {
            let mut entry = self.table_modifications.entry(folded.clone()).or_insert(0);
            *entry = entry.saturating_add(modified_rows);
        }
        let analyze_current = {
            let mut entry = self
                .table_analyze_modifications
                .entry(folded.clone())
                .or_insert(0);
            *entry = entry.saturating_add(modified_rows);
            *entry
        };
        let threshold = self.auto_analyze_threshold(&folded);
        if analyze_current < threshold {
            return;
        }

        // Reset counter first so concurrent DML can accumulate for the
        // next cycle while the maintenance pass drains this table.
        self.table_analyze_modifications.insert(folded.clone(), 0);
        self.pending_analyze_tables.insert(folded, ());
    }

    /// Rebuild every pending same-table columnar shadow.
    ///
    /// The heap remains authoritative. This maintenance pass drains
    /// queued table names and warms `HeapAccess::column_cache` from an
    /// MVCC snapshot so subsequent OLAP scans can use the columnar
    /// secondary layout without first paying the row-store decode cost.
    pub fn run_columnarization_cycle(&self) {
        while self.run_one_pending_columnarization() {}
    }

    pub(crate) fn run_one_pending_columnarization(&self) -> bool {
        let Some(table) = self.columnar_storage.pop_pending() else {
            return false;
        };
        match self.columnarize_table(&table) {
            Ok(true) => {
                tracing::debug!(table = %table, "columnar shadow rebuilt");
            }
            Ok(false) => {
                tracing::debug!(table = %table, "columnar shadow skipped");
            }
            Err(e) => {
                tracing::warn!(table = %table, error = %e, "columnar shadow rebuild failed");
            }
        }
        true
    }

    /// Rebuild one table's columnar shadow from the row-store heap.
    pub fn columnarize_table(&self, table: &str) -> Result<bool, ServerError> {
        let folded = table.to_ascii_lowercase();
        let snapshot = self.catalog_snapshot();
        let Some(entry) = snapshot.tables.get(&folded).cloned() else {
            self.columnar_storage.remove(&folded);
            return Ok(false);
        };
        drop(snapshot);

        let rel = RelationId(entry.oid);
        if let Some(cached) = self.heap.column_cache.get(rel) {
            self.columnar_storage.record_rebuild(
                folded,
                rel,
                cached.version,
                cached.row_count(),
                cached.segment_count(),
            );
            return Ok(true);
        }

        let block_count = self.heap.block_count(rel).max(entry.n_blocks);
        if block_count == 0 {
            return Ok(false);
        }

        let scan_txn = self.txn_manager.begin(IsolationLevel::ReadCommitted);
        let scan_result = (|| -> Result<(), ServerError> {
            let mut scan = SeqScan::new_with_vm(
                Arc::clone(&self.heap),
                rel,
                block_count,
                scan_txn.snapshot.clone(),
                Arc::clone(&self.txn_manager),
                Arc::clone(&self.vm),
                RowCodec::new(entry.schema.clone()),
            );
            while scan
                .next_batch()
                .map_err(|e| ServerError::Ddl(format!("columnarization scan failed: {e}")))?
                .is_some()
            {}
            Ok(())
        })();
        self.finalise_scan_transaction(
            scan_txn,
            scan_result,
            "columnarization scan transaction abort",
            "columnarization scan rollback after scan error",
        )?;

        let Some(cached) = self.heap.column_cache.get(rel) else {
            return Ok(false);
        };
        self.columnar_storage.record_rebuild(
            folded,
            rel,
            cached.version,
            cached.row_count(),
            cached.segment_count(),
        );
        Ok(true)
    }

    /// Run `ANALYZE` for one table: refresh block-count hint and
    /// rebuild relation stats from MVCC-visible rows.
    pub fn analyze_table(&self, table: &str) -> Result<bool, ServerError> {
        self.analyze_table_with_pid(table, 0)
    }

    /// Run `ANALYZE` for one table and publish progress under `pid`.
    ///
    /// Resolves the table via the GLOBAL committed catalog. A SESSION holding a
    /// transactional-DDL overlay must NOT use this entry point for an
    /// in-txn-created table — `Server` has no access to the overlay — and
    /// instead resolves the [`TableEntry`] from its own
    /// `effective_catalog_snapshot()` and calls
    /// [`Self::analyze_table_entry_with_pid`] directly.
    pub fn analyze_table_with_pid(&self, table: &str, pid: u32) -> Result<bool, ServerError> {
        let folded = table.to_ascii_lowercase();
        let snapshot = self.catalog_snapshot();
        let Some(entry) = snapshot.tables.get(&folded) else {
            // A missing table still clears any pending-analyze flag for the
            // name, matching the original entry-resolution ordering.
            self.pending_analyze_tables.remove(&folded);
            return Ok(false);
        };
        let entry = entry.clone();
        drop(snapshot);
        // Resolved from the committed catalog, so the OID is published and the
        // durable OID-keyed writes are valid.
        self.analyze_table_entry_with_pid(&folded, &entry, pid, true)
    }

    /// Run `ANALYZE` for one already-resolved [`TableEntry`] and publish
    /// progress under `pid`.
    ///
    /// Factored out of [`Self::analyze_table_with_pid`] so a session can supply
    /// an entry resolved through its per-txn catalog overlay (an
    /// in-txn-created relation that the global catalog does not yet carry)
    /// without giving `Server` access to the overlay. `folded` is the
    /// case-folded lookup name used for the pending-analyze flag and the
    /// AnalyzeRunner label.
    ///
    /// `persist_durably` MUST be `false` when `entry` is an overlay-only
    /// (in-txn-created) relation whose OID is not yet published to the
    /// committed persistent catalog: the OID-keyed durable writes
    /// (`update_table_size` / `persist_statistic_rows` / `replace_statistics`)
    /// would fail "not found" against the uncommitted OID. With it `false`,
    /// ANALYZE still scans the heap and registers the computed stats in the
    /// in-memory `stats_catalog` (keyed by folded name) so the issuing
    /// session's optimizer sees them, but skips the durable OID-keyed writes —
    /// a later autocommit `ANALYZE` after COMMIT persists them. Committed
    /// tables (autocommit, or in-txn ALTER of an already-committed table) pass
    /// `true`, preserving the original behavior byte-for-byte.
    pub fn analyze_table_entry_with_pid(
        &self,
        folded: &str,
        entry: &TableEntry,
        pid: u32,
        persist_durably: bool,
    ) -> Result<bool, ServerError> {
        self.pending_analyze_tables.remove(folded);
        let rel = RelationId(entry.oid);
        let block_count = self.heap.block_count(rel).max(entry.n_blocks);
        if persist_durably {
            self.persistent_catalog
                .update_table_size(entry.oid, block_count)
                .map_err(ServerError::Catalog)?;
        }

        self.workload_recorder
            .begin_analyze(pid, entry.oid.raw(), block_count);
        let result = (|| -> Result<bool, ServerError> {
            self.workload_recorder
                .update_analyze(pid, "scanning table", 0);

            let scan_txn = self.txn_manager.begin(IsolationLevel::ReadCommitted);
            let scan_snapshot = scan_txn.snapshot.clone();
            let mut payloads: Vec<Vec<u8>> = Vec::new();
            let scan_result = self
                .heap
                .for_each_visible(
                    rel,
                    block_count,
                    &scan_snapshot,
                    self.txn_manager.as_ref(),
                    |_tid, _hdr, payload| {
                        payloads.push(payload.to_vec());
                        Ok(())
                    },
                )
                .map_err(|e| ServerError::Ddl(format!("ANALYZE scan failed: {e}")));
            self.finalise_scan_transaction(
                scan_txn,
                scan_result,
                "ANALYZE scan transaction abort",
                "ANALYZE scan rollback after scan error",
            )?;

            self.workload_recorder
                .update_analyze(pid, "computing statistics", block_count);
            let codec = RowCodec::new(entry.schema.clone());
            let mut rows: Vec<Vec<ultrasql_core::Value>> = Vec::with_capacity(payloads.len());
            for payload in payloads {
                match codec.decode(&payload) {
                    Ok(row) => rows.push(row),
                    Err(e) => {
                        tracing::warn!(table = %folded, error = %e, "ANALYZE skipped malformed tuple");
                    }
                }
            }
            let stats = AnalyzeRunner::new(AnalyzeOptions::default())
                .run(folded, &entry.schema, rows.into_iter())
                .map_err(|e| ServerError::Ddl(format!("ANALYZE statistics failed: {e}")))?;
            let mut stat_rows = Vec::with_capacity(stats.columns.len());
            for col in &stats.columns {
                let staattnum =
                    i16::try_from(col.column_index.saturating_add(1)).map_err(|_| {
                        ServerError::Ddl("ANALYZE table has too many columns".to_owned())
                    })?;
                let pg_row = PgStatisticRow::from_column_stats(
                    entry.oid.raw(),
                    u16::try_from(staattnum).map_err(|_| {
                        ServerError::Ddl("ANALYZE invalid attribute number".to_owned())
                    })?,
                    col,
                );
                stat_rows.push(StatisticRow {
                    starelid: entry.oid,
                    staattnum,
                    stanullfrac: pg_row.stanullfrac,
                    stadistinct: pg_row.stadistinct,
                });
            }
            self.workload_recorder
                .update_analyze(pid, "writing statistics", block_count);
            // Durable, OID-keyed persistence is skipped for an overlay-only
            // (uncommitted-OID) relation: the committed persistent catalog has
            // no row for `entry.oid` yet, so these writes would fail. The
            // in-memory `stats_catalog.register` below still runs so the
            // session's optimizer sees the fresh stats.
            if persist_durably {
                let catalog_txn = self.txn_manager.begin(IsolationLevel::ReadCommitted);
                if let Err(e) = self.persistent_catalog.persist_statistic_rows(
                    &stat_rows,
                    self.heap.as_ref(),
                    catalog_txn.xid,
                    catalog_txn.current_command,
                ) {
                    return Err(self.abort_analyze_catalog_statistics_transaction(catalog_txn, e));
                }
                self.commit_transaction(
                    catalog_txn,
                    true,
                    "ANALYZE catalog statistics transaction",
                )?;
            }
            self.stats_catalog.write().register(stats);
            if persist_durably {
                self.persistent_catalog
                    .replace_statistics(entry.oid, stat_rows);
            }
            self.plan_cache.invalidate_all();
            Ok(true)
        })();
        self.workload_recorder.finish_analyze(pid);
        if matches!(result, Ok(true)) {
            if pid == 0 {
                self.workload_recorder
                    .record_table_autoanalyze(entry.oid.raw());
            } else {
                self.workload_recorder.record_table_analyze(entry.oid.raw());
            }
        }
        result
    }

    pub(crate) fn abort_analyze_catalog_statistics_transaction(
        &self,
        txn: Transaction,
        err: ultrasql_catalog::CatalogError,
    ) -> ServerError {
        match self.txn_manager.abort(txn) {
            Ok(()) => ServerError::Catalog(err),
            Err(abort_err) => ServerError::ddl(format!(
                "ANALYZE catalog statistics transaction abort: {err}; \
                 transaction abort failed: {abort_err}"
            )),
        }
    }

    pub(crate) fn finalise_scan_transaction<T>(
        &self,
        txn: Transaction,
        outcome: Result<T, ServerError>,
        success_context: &'static str,
        rollback_context: &'static str,
    ) -> Result<T, ServerError> {
        match self.txn_manager.abort(txn) {
            Ok(()) => outcome,
            Err(abort_err) => match outcome {
                Ok(_) => Err(ServerError::ddl(format!("{success_context}: {abort_err}"))),
                Err(err) => Err(ServerError::ddl(format!(
                    "{rollback_context}: {err}; transaction abort failed: {abort_err}"
                ))),
            },
        }
    }

    pub(crate) fn auto_analyze_threshold(&self, table: &str) -> u64 {
        let snapshot = self.catalog_snapshot();
        let Some(entry) = snapshot.tables.get(table) else {
            return self.autovacuum_config.analyze_threshold;
        };
        let rel = RelationId(entry.oid);
        let blocks = u64::from(self.heap.block_count(rel).max(entry.n_blocks));
        let estimated_rows = blocks.saturating_mul(64);
        autovacuum_config_for_table(self.autovacuum_config, entry)
            .analyze_threshold_for_rows(estimated_rows)
    }

    pub(crate) fn run_one_pending_analyze(&self) {
        let Some(table) = self
            .pending_analyze_tables
            .iter()
            .next()
            .map(|entry| entry.key().clone())
        else {
            return;
        };

        match self.analyze_table(&table) {
            Ok(true) => {
                tracing::debug!(table = %table, "autovacuum analyze completed");
            }
            Ok(false) => {
                tracing::debug!(table = %table, "autovacuum analyze skipped missing table");
            }
            Err(e) => {
                tracing::warn!(table = %table, error = %e, "autovacuum analyze failed");
            }
        }
    }
}
