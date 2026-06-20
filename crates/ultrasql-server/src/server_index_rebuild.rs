//! `impl Server` methods (split out of the crate root): index_rebuild.
//!
//! Pure code motion from `lib.rs`; behavior unchanged.
use super::*;

impl Server {
    pub(crate) fn rebuild_aggregating_index_rows(
        &self,
        table: &TableEntry,
        spec: &ultrasql_planner::LogicalAggregatingIndex,
    ) -> Result<Vec<Vec<Value>>, ServerError> {
        let txn = self.txn_manager.begin(IsolationLevel::ReadCommitted);
        let rows = crate::aggregating_index::build_aggregating_index_rows(
            table,
            spec,
            self.heap.as_ref(),
            &txn.snapshot,
            self.txn_manager.as_ref(),
        );
        self.finalise_restart_rebuild_transaction(
            txn,
            rows,
            "restart aggregating-index rebuild transaction commit",
            "restart aggregating-index rebuild transaction rollback",
        )
    }

    pub(crate) fn finalise_restart_rebuild_transaction<T>(
        &self,
        txn: Transaction,
        outcome: Result<T, ServerError>,
        commit_context: &'static str,
        rollback_context: &'static str,
    ) -> Result<T, ServerError> {
        match outcome {
            Ok(value) => self
                .txn_manager
                .commit(txn)
                .map(|()| value)
                .map_err(|err| ServerError::ddl(format!("{commit_context}: {err}"))),
            Err(err) => match self.txn_manager.abort(txn) {
                Ok(()) => Err(err),
                Err(abort_err) => Err(ServerError::ddl(format!(
                    "{rollback_context}: {err}; transaction abort failed: {abort_err}"
                ))),
            },
        }
    }

    pub(crate) fn replay_vector_index_wal_into(
        &self,
        hnsw_indexes: &[Arc<PageBackedHnswIndex>],
        ivfflat_indexes: &[Arc<PageBackedIvfFlatIndex>],
    ) -> Result<(), ServerError> {
        if hnsw_indexes.is_empty() && ivfflat_indexes.is_empty() {
            return Ok(());
        }
        let Some(data_dir) = &self.data_dir else {
            return Ok(());
        };
        let wal_dir = data_dir.join("pg_wal");
        let recovery_replay_target = recovery_replay_target_from_data_dir(data_dir)?;
        // Track each record's WAL LSN (cumulative byte offset, exactly as the
        // heap recovery pass does) so HNSW replay can be LSN-bounded: a record
        // whose LSN is already covered by a loaded snapshot's meta.lsn
        // high-water mark is skipped by `redo_covered`. Without a snapshot the
        // arena starts at meta.lsn == 0 and every record applies (full rebuild).
        // Seed from the WAL recovery floor so LSNs stay absolute after any
        // truncation (matches recover_with_target's own cursor; absent = 0).
        let mut record_lsn = ultrasql_wal::read_floor(&wal_dir)
            .map_err(|e| ServerError::ddl(format!("read WAL recovery floor: {e}")))?
            .floor_lsn;
        ultrasql_wal::recover_with_target(&wal_dir, recovery_replay_target, |record| {
            let current_lsn = record_lsn;
            record_lsn = record_lsn
                .checked_advance(u64::from(record.header.total_length))
                .ok_or(ultrasql_wal::RecoveryError::Record(
                    ultrasql_wal::WalRecordError::Malformed("vector replay lsn overflow"),
                ))?;
            if record.header.record_type == RecordType::HnswOp {
                for hnsw in hnsw_indexes {
                    if !hnsw.is_valid() {
                        continue;
                    }
                    if let Err(e) = hnsw.apply_wal_record_at(current_lsn, record) {
                        hnsw.invalidate();
                        tracing::warn!(
                            error = %e,
                            "HNSW WAL replay failed; marking index unavailable"
                        );
                    }
                }
            }
            if record.header.record_type == RecordType::IvfFlatOp {
                for ivfflat in ivfflat_indexes {
                    if !ivfflat.is_valid() {
                        continue;
                    }
                    // Pass the record's LSN so a loaded snapshot's redo gate skips
                    // records it already covers (mirrors the HNSW path above).
                    if let Err(e) = ivfflat.apply_wal_record_at(current_lsn, record) {
                        ivfflat.invalidate();
                        tracing::warn!(
                            error = %e,
                            "IVFFlat WAL replay failed; marking index unavailable"
                        );
                    }
                }
            }
            Ok(())
        })
        .map(|_| ())
        .map_err(|e| ServerError::ddl(format!("recover vector index WAL: {e}")))
    }

    /// Allocate the next per-connection process id.
    ///
    /// Counter is monotonic; wraps after 2^32 connections. The PostgreSQL
    /// wire layer treats the value opaquely.
    pub fn allocate_pid(&self) -> u32 {
        self.next_pid
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    /// Acquire a per-statement catalog snapshot.
    ///
    /// The returned [`Arc<CatalogSnapshot>`] is immutable and stable for the
    /// caller's lifetime; concurrent DDL atomically swaps in a new pointer
    /// without invalidating this reference.
    ///
    /// This is the primary entry point for the binder and the optimizer.
    /// The call is wait-free — it performs a single `ArcSwap::load_full`.
    #[must_use]
    pub fn catalog_snapshot(&self) -> Arc<CatalogSnapshot> {
        self.persistent_catalog.snapshot()
    }

    /// Return live WAL writer counters when WAL-backed storage is enabled.
    #[must_use]
    pub fn wal_writer_stats(&self) -> Option<ultrasql_wal::WalWriterStats> {
        self.wal_writer.as_ref().map(ultrasql_wal::WalWriter::stats)
    }

    /// Return runtime autovacuum thresholds.
    #[must_use]
    pub const fn autovacuum_config(&self) -> AutovacuumConfig {
        self.autovacuum_config
    }

    /// Replace runtime autovacuum thresholds before the launcher starts.
    pub fn set_autovacuum_config(&mut self, config: AutovacuumConfig) {
        self.autovacuum_config = config;
    }

    /// Return runtime statement logging settings.
    #[must_use]
    pub const fn logging_config(&self) -> LoggingConfig {
        self.logging_config
    }

    /// Replace runtime statement logging settings before the listener starts.
    pub fn set_logging_config(&mut self, config: LoggingConfig) {
        self.logging_config = config;
    }

    /// Return the idle-session timeout in milliseconds.
    #[must_use]
    pub const fn idle_session_timeout_ms(&self) -> u64 {
        self.idle_session_timeout_ms
    }

    /// Replace the idle-session timeout before the listener starts.
    pub const fn set_idle_session_timeout_ms(&mut self, timeout_ms: u64) {
        self.idle_session_timeout_ms = timeout_ms;
    }

    /// Return runtime WAL archive settings.
    #[must_use]
    pub fn wal_archive_config(&self) -> WalArchiveConfig {
        self.wal_archive_config.clone()
    }

    /// Replace runtime WAL archive settings before the listener starts.
    pub fn set_wal_archive_config(&mut self, config: WalArchiveConfig) {
        self.wal_archive_config = config;
    }

    /// Return process-local ANN/vector-index counters for ops metrics.
    #[must_use]
    pub fn ann_system_metrics(&self) -> AnnSystemMetrics {
        let mut metrics = AnnSystemMetrics::default();
        for entry in self.table_constraints.iter() {
            for runtime in entry.value().indexes.values() {
                if let Some(hnsw) = &runtime.hnsw {
                    let stats = hnsw.page_stats();
                    metrics.hnsw_indexes = metrics.hnsw_indexes.saturating_add(1);
                    metrics.candidates = metrics
                        .candidates
                        .saturating_add(usize_to_u64_saturated(stats.live_nodes));
                    metrics.tombstones = metrics
                        .tombstones
                        .saturating_add(usize_to_u64_saturated(stats.tombstones));
                    let pages = stats
                        .meta_pages
                        .saturating_add(stats.node_pages)
                        .saturating_add(stats.overflow_pages)
                        .saturating_add(stats.free_list_pages);
                    metrics.vector_index_memory_bytes = metrics
                        .vector_index_memory_bytes
                        .saturating_add(pages_to_bytes_saturated(pages));
                }
                if let Some(ivfflat) = &runtime.ivfflat {
                    let stats = ivfflat.page_stats();
                    metrics.ivfflat_indexes = metrics.ivfflat_indexes.saturating_add(1);
                    metrics.candidates = metrics
                        .candidates
                        .saturating_add(usize_to_u64_saturated(stats.live_entries));
                    metrics.tombstones = metrics
                        .tombstones
                        .saturating_add(usize_to_u64_saturated(stats.tombstones));
                    let pages = stats
                        .meta_pages
                        .saturating_add(stats.centroid_pages)
                        .saturating_add(stats.list_pages)
                        .saturating_add(stats.entry_pages);
                    metrics.vector_index_memory_bytes = metrics
                        .vector_index_memory_bytes
                        .saturating_add(pages_to_bytes_saturated(pages));
                }
            }
        }
        metrics
    }

    /// Run offline admin validation over catalog, indexes, WAL, heap visibility, and ANN tombstones.
    #[must_use]
    pub fn validate(&self) -> ValidationReport {
        ValidationReport {
            checks: vec![
                self.validate_catalog_check(),
                self.validate_indexes_check(),
                self.validate_wal_check(),
                self.validate_heap_visibility_check(),
                self.validate_ann_tombstones_check(),
            ],
        }
    }

    pub(crate) fn validate_catalog_check(&self) -> ValidationCheck {
        let snapshot = self.catalog_snapshot();
        let mut errors = Vec::new();
        for (folded, table) in &snapshot.tables {
            if !snapshot.tables_by_oid.contains_key(&table.oid) {
                errors.push(format!(
                    "table {} oid {} missing from oid map",
                    table.name,
                    table.oid.raw()
                ));
            }
            let expected_key = ultrasql_catalog::table_lookup_key(&table.schema_name, &table.name);
            if folded != &expected_key {
                errors.push(format!(
                    "table {} stored under non-canonical key {}",
                    table.name, folded
                ));
            }
        }
        for (oid, table) in &snapshot.tables_by_oid {
            if !snapshot
                .tables
                .values()
                .any(|named_table| named_table.oid == *oid)
            {
                errors.push(format!(
                    "oid map table {} oid {} missing from name map",
                    table.name,
                    oid.raw()
                ));
            }
        }
        validation_check(
            "catalog",
            errors,
            format!(
                "{} table(s), {} oid entry(s), {} index(es)",
                snapshot.tables.len(),
                snapshot.tables_by_oid.len(),
                snapshot.indexes.len()
            ),
        )
    }

    pub(crate) fn validate_indexes_check(&self) -> ValidationCheck {
        let snapshot = self.catalog_snapshot();
        let mut errors = Vec::new();
        for index in snapshot.indexes.values() {
            let Some(table) = snapshot.tables_by_oid.get(&index.table_oid) else {
                errors.push(format!(
                    "index {} references missing table oid {}",
                    index.name,
                    index.table_oid.raw()
                ));
                continue;
            };
            if !snapshot
                .indexes_by_table
                .get(&index.table_oid)
                .is_some_and(|indexes| indexes.iter().any(|entry| entry.oid == index.oid))
            {
                errors.push(format!(
                    "index {} oid {} missing from table index map",
                    index.name,
                    index.oid.raw()
                ));
            }
            for column in &index.columns {
                let idx = usize::from(*column);
                if idx >= table.schema.len() {
                    errors.push(format!(
                        "index {} column {} out of range for table {}",
                        index.name, column, table.name
                    ));
                }
            }
            let method = index.access_method.to_ascii_lowercase();
            if method == "hnsw" || method == "ivfflat" {
                let runtime = self
                    .table_constraints
                    .get(&index.table_oid)
                    .and_then(|constraints| constraints.value().indexes.get(&index.oid).cloned());
                match (method.as_str(), runtime) {
                    ("hnsw", Some(runtime)) => match runtime.hnsw {
                        Some(hnsw) if hnsw.is_valid() => {}
                        Some(_) => errors.push(format!("hnsw index {} is invalid", index.name)),
                        None => errors.push(format!(
                            "hnsw index {} missing page-backed sidecar",
                            index.name
                        )),
                    },
                    ("ivfflat", Some(runtime)) => match runtime.ivfflat {
                        Some(ivfflat) if ivfflat.is_valid() => {}
                        Some(_) => errors.push(format!("ivfflat index {} is invalid", index.name)),
                        None => errors.push(format!(
                            "ivfflat index {} missing page-backed sidecar",
                            index.name
                        )),
                    },
                    _ => errors.push(format!(
                        "{} index {} missing runtime metadata",
                        method, index.name
                    )),
                }
            }
        }
        validation_check(
            "indexes",
            errors,
            format!(
                "{} index(es), {} indexed table bucket(s)",
                snapshot.indexes.len(),
                snapshot.indexes_by_table.len()
            ),
        )
    }

    pub(crate) fn validate_wal_check(&self) -> ValidationCheck {
        let Some(data_dir) = &self.data_dir else {
            return validation_check(
                "wal",
                Vec::new(),
                "in-memory server; no WAL directory configured".to_owned(),
            );
        };
        let wal_dir = data_dir.join("pg_wal");
        match ultrasql_wal::recover(&wal_dir, |_| Ok(())) {
            Ok(lsn) => validation_check(
                "wal",
                Vec::new(),
                format!("decoded WAL through lsn {}", lsn.raw()),
            ),
            Err(err) => validation_check("wal", vec![err.to_string()], String::new()),
        }
    }

    pub(crate) fn validate_heap_visibility_check(&self) -> ValidationCheck {
        let snapshot = self.catalog_snapshot();
        let scan_txn = self.txn_manager.begin(IsolationLevel::ReadCommitted);
        let scan_snapshot = scan_txn.snapshot.clone();
        let mut errors = Vec::new();
        let mut visible_rows = 0_u64;
        let mut checked_tables = 0_u64;
        let mut skipped_catalog_tables = 0_u64;
        for table in snapshot.tables.values() {
            if table.schema_name == "pg_catalog" {
                skipped_catalog_tables = skipped_catalog_tables.saturating_add(1);
                continue;
            }
            checked_tables = checked_tables.saturating_add(1);
            let rel = RelationId(table.oid);
            let block_count = self.heap.block_count(rel).max(table.n_blocks);
            let codec = RowCodec::new(table.schema.clone());
            let mut decode_error: Option<String> = None;
            let mut table_rows = 0_u64;
            let scan_result = self.heap.for_each_visible(
                rel,
                block_count,
                &scan_snapshot,
                self.txn_manager.as_ref(),
                |_tid, _hdr, payload| {
                    if decode_error.is_none() {
                        if let Err(err) = codec.decode(payload) {
                            decode_error = Some(err.to_string());
                        }
                    }
                    table_rows = table_rows.saturating_add(1);
                    Ok(())
                },
            );
            if let Err(err) = scan_result {
                errors.push(format!("table {} heap scan failed: {err}", table.name));
            }
            if let Some(err) = decode_error {
                errors.push(format!("table {} row decode failed: {err}", table.name));
            }
            visible_rows = visible_rows.saturating_add(table_rows);
        }
        if let Err(err) = self.txn_manager.abort(scan_txn) {
            errors.push(format!("validation scan transaction abort failed: {err}"));
        }
        validation_check(
            "heap_visibility",
            errors,
            format!(
                "{} user table(s), {} catalog table(s) skipped, {} visible row(s)",
                checked_tables, skipped_catalog_tables, visible_rows
            ),
        )
    }

    pub(crate) fn validate_ann_tombstones_check(&self) -> ValidationCheck {
        let mut errors = Vec::new();
        let mut hnsw_indexes = 0_u64;
        let mut ivfflat_indexes = 0_u64;
        let mut tombstones = 0_u64;
        for entry in self.table_constraints.iter() {
            for runtime in entry.value().indexes.values() {
                if let Some(hnsw) = &runtime.hnsw {
                    hnsw_indexes = hnsw_indexes.saturating_add(1);
                    let stats = hnsw.page_stats();
                    tombstones =
                        tombstones.saturating_add(usize_to_u64_saturated(stats.tombstones));
                    if !hnsw.is_valid() {
                        errors.push("hnsw sidecar is invalid".to_owned());
                    }
                }
                if let Some(ivfflat) = &runtime.ivfflat {
                    ivfflat_indexes = ivfflat_indexes.saturating_add(1);
                    let stats = ivfflat.page_stats();
                    tombstones =
                        tombstones.saturating_add(usize_to_u64_saturated(stats.tombstones));
                    if !ivfflat.is_valid() {
                        errors.push("ivfflat sidecar is invalid".to_owned());
                    }
                }
            }
        }
        validation_check(
            "ann_tombstones",
            errors,
            format!(
                "{} hnsw index(es), {} ivfflat index(es), {} tombstone(s)",
                hnsw_indexes, ivfflat_indexes, tombstones
            ),
        )
    }

    /// Validate foreign keys that were declared `DEFERRABLE INITIALLY DEFERRED`.
    ///
    /// The check is deliberately table-scanning: v0.8 favours correctness over
    /// an incremental deferred-trigger queue. Immediate checks still run in the
    /// executor for non-deferred constraints.
    pub(crate) fn validate_deferred_foreign_keys(
        &self,
        txn: &Transaction,
    ) -> Result<(), ServerError> {
        let catalog = self.catalog_snapshot();
        for item in self.table_constraints.iter() {
            let child_oid = *item.key();
            let constraints = item.value();
            if !constraints
                .foreign_keys
                .iter()
                .any(|fk| fk.deferrable && fk.initially_deferred)
            {
                continue;
            }
            let Some(child) = catalog.tables_by_oid.get(&child_oid).cloned() else {
                continue;
            };
            let child_rel = RelationId(child.oid);
            let child_blocks = self.heap.block_count(child_rel).max(child.n_blocks);
            if child_blocks == 0 {
                continue;
            }
            let child_codec = RowCodec::new(child.schema.clone());
            for fk in constraints
                .foreign_keys
                .iter()
                .filter(|fk| fk.deferrable && fk.initially_deferred)
            {
                let parent = catalog
                    .tables_by_oid
                    .get(&fk.target_oid)
                    .or_else(|| catalog.tables.get(&fk.target_table))
                    .ok_or_else(|| {
                        ServerError::Catalog(ultrasql_catalog::CatalogError::not_found(
                            fk.target_table.clone(),
                        ))
                    })?;
                for tuple in self.heap.scan_visible(
                    child_rel,
                    child_blocks,
                    &txn.snapshot,
                    self.txn_manager.as_ref(),
                ) {
                    let tuple = tuple
                        .map_err(|e| ServerError::Ddl(format!("deferred FK scan failed: {e}")))?;
                    let row = child_codec.decode(&tuple.data).map_err(|e| {
                        ServerError::Ddl(format!("deferred FK row decode failed: {e}"))
                    })?;
                    let Some(key) = deferred_fk_key(&row, &fk.columns) else {
                        continue;
                    };
                    if !self.deferred_relation_has_key(parent, &fk.target_columns, &key, txn)? {
                        return Err(ultrasql_executor::ExecError::ForeignKeyViolation(
                            fk.name.clone(),
                        )
                        .into());
                    }
                }
            }
        }
        Ok(())
    }

    pub(crate) fn deferred_relation_has_key(
        &self,
        table: &TableEntry,
        columns: &[usize],
        key: &[Value],
        txn: &Transaction,
    ) -> Result<bool, ServerError> {
        let relation = RelationId(table.oid);
        let block_count = self.heap.block_count(relation).max(table.n_blocks);
        let codec = RowCodec::new(table.schema.clone());
        for tuple in self.heap.scan_visible(
            relation,
            block_count,
            &txn.snapshot,
            self.txn_manager.as_ref(),
        ) {
            let tuple = tuple
                .map_err(|e| ServerError::Ddl(format!("deferred FK parent scan failed: {e}")))?;
            let row = codec.decode(&tuple.data).map_err(|e| {
                ServerError::Ddl(format!("deferred FK parent row decode failed: {e}"))
            })?;
            if deferred_fk_key(&row, columns).as_deref() == Some(key) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Lookup optimizer statistics for `table` from the in-memory
    /// stats catalog.
    #[must_use]
    pub fn lookup_relation_stats(&self, table: &str) -> Option<ultrasql_optimizer::RelationStats> {
        self.stats_catalog.read().lookup_relation(table)
    }
}
