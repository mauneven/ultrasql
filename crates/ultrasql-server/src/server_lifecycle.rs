//! `impl Server` methods (split out of the crate root): lifecycle.
//!
//! Pure code motion from `lib.rs`; behavior unchanged.
use super::*;

impl Server {
    /// Build an empty in-memory server.
    ///
    /// This is the embedded `:memory:` entry point: no TCP listener, no WAL,
    /// no preloaded sample relations. DDL and DML still use the same heap,
    /// catalog, MVCC, and executor paths as a normal session.
    #[must_use]
    pub fn with_empty_database() -> Self {
        Self::with_in_memory_catalog(
            InMemoryCatalog::new(),
            SampleTables::new(),
            IN_MEMORY_POOL_FRAMES,
        )
    }

    /// Execute one read-only SQL query without opening a server socket.
    ///
    /// The local path deliberately reuses the normal parser, binder, and
    /// physical lowerer so file table functions behave the same as they
    /// do over the PostgreSQL wire protocol. It materialises text rows
    /// for CLI-style display instead of encoding wire frames.
    pub fn execute_local_query(
        self: &Arc<Self>,
        sql: &str,
    ) -> Result<LocalQueryOutput, ServerError> {
        let stmt = Parser::new(sql).parse_statement()?;
        let catalog_snapshot = self.catalog_snapshot();
        let combined = CombinedCatalog {
            snapshot: &catalog_snapshot,
            fallback: &self.catalog,
            search_path: None,
        };
        let plan = bind(&stmt, &combined)?;
        if !is_local_read_plan(&plan) {
            return Err(ServerError::Unsupported(
                "ultrasql-local supports read-only SELECT queries",
            ));
        }

        let txn = self.txn_manager.begin(IsolationLevel::ReadCommitted);
        let ctx = LowerCtx {
            tables: &self.tables,
            catalog_snapshot,
            table_constraints: Arc::clone(&self.table_constraints),
            sequences: Arc::clone(&self.sequences),
            sequence_owners: Arc::clone(&self.sequence_owners),
            sequence_namespaces: Arc::clone(&self.sequence_namespaces),
            schemas: Arc::clone(&self.schemas),
            operators: Arc::clone(&self.operators),
            role_catalog: Arc::clone(&self.role_catalog),
            privilege_catalog: Arc::clone(&self.privilege_catalog),
            row_security: Arc::clone(&self.row_security),
            session_settings: Arc::new(std::collections::HashMap::new()),
            current_user: "ultrasql".to_owned(),
            session_user: "ultrasql".to_owned(),
            persistent_catalog: Arc::clone(&self.persistent_catalog),
            time_partitions: Arc::clone(&self.time_partitions),
            workload_recorder: Arc::clone(&self.workload_recorder),
            autovacuum_config: self.autovacuum_config(),
            logging_config: self.logging_config(),
            wal_archive_config: self.wal_archive_config(),
            data_dir: self.data_dir.clone(),
            logical_replication: Arc::clone(&self.logical_replication),
            sequence_state: Some(SequenceSessionState::default()),
            advisory_state: None,
            heap: Arc::clone(&self.heap),
            vm: Arc::clone(&self.vm),
            snapshot: txn.snapshot.clone(),
            isolation: txn.isolation,
            oracle: Arc::clone(&self.txn_manager),
            xid: txn.current_xid(),
            lock_xid: txn.xid,
            command_id: txn.current_command,
            cte_buffers: std::collections::HashMap::new(),
            jit: ultrasql_vec::jit::JitConfig {
                enabled: false,
                above_rows: ultrasql_vec::jit::DEFAULT_JIT_ABOVE_ROWS,
            },
            cancel_flag: None,
            // Embedded `ultrasql-local` has no session GUCs; arm the default
            // 64 MiB budget (capped by the process-wide memory-admission
            // share) so large sorts / GROUP BY / hash-joins spill to disk
            // rather than growing the heap without bound.
            work_mem: Arc::new(ultrasql_executor::work_mem::WorkMemBudget::new(
                crate::memory_admission::effective_work_mem_bytes(
                    crate::session::DEFAULT_WORK_MEM_BYTES,
                    self.memory_admission.per_statement_cap_bytes(),
                ),
            )),
            profile_operators: false,
            // Embedded `ultrasql-local` runs as the bootstrap superuser
            // `ultrasql`, so server-local file reads are permitted.
            allow_server_files: true,
        };
        let outcome = (|| {
            let mut op = pipeline::lower_query(&plan, &ctx)?;
            local_output_from_select_result(run_select(op.as_mut())?)
        })();
        self.finalise_local_query_transaction(txn, outcome)
    }

    pub(crate) fn finalise_local_query_transaction(
        &self,
        txn: Transaction,
        outcome: Result<LocalQueryOutput, ServerError>,
    ) -> Result<LocalQueryOutput, ServerError> {
        match outcome {
            Ok(output) => self
                .txn_manager
                .commit(txn)
                .map(|_committed_subxids| output)
                .map_err(|err| {
                    ServerError::ddl(format!("ultrasql-local read transaction commit: {err}"))
                }),
            Err(err) => match self.txn_manager.abort(txn) {
                Ok(()) => Err(err),
                Err(abort_err) => Err(ServerError::ddl(format!(
                    "ultrasql-local read transaction rollback: {err}; transaction abort failed: {abort_err}"
                ))),
            },
        }
    }

    /// Build a server pre-loaded with the canonical sample database.
    ///
    /// The persistent catalog is bootstrapped from an in-memory buffer pool
    /// (no disk I/O). On a fresh in-memory database the bootstrap detects an
    /// empty heap and installs the hard-coded initial snapshot.
    #[must_use]
    pub fn with_sample_database() -> Self {
        Self::with_sample_database_pool_frames(IN_MEMORY_POOL_FRAMES)
    }

    /// Build a server pre-loaded with the canonical sample database and a
    /// caller-provided in-memory buffer-pool size.
    ///
    /// Intended for large in-process benchmarks such as TPC-H, where the
    /// default development pool can be too small for the loaded dataset.
    #[must_use]
    pub fn with_sample_database_pool_frames(pool_frames: usize) -> Self {
        let mut catalog = InMemoryCatalog::new();
        let tables = build_sample_database(&mut catalog);
        Self::with_in_memory_catalog(catalog, tables, pool_frames)
    }

    pub(crate) fn with_in_memory_catalog(
        catalog: InMemoryCatalog,
        tables: SampleTables,
        pool_frames: usize,
    ) -> Self {
        let persistent_catalog = Arc::new(PersistentCatalog::new());
        // One in-memory buffer pool for both catalog bootstrap and
        // user-table DML so every connection observes the same heap.
        let page_loader = BlankPageLoader::new();
        let pool = Arc::new(BufferPool::new(pool_frames, page_loader.clone()));
        // Eviction relief with no WAL force: a sink-less pool treats the durable
        // LSN as `u64::MAX`, so Phase A always flushes and Phase B never fires.
        Server::install_eviction_relief(&pool, &page_loader, None);
        let heap = Arc::new(HeapAccess::new(Arc::clone(&pool)));
        let vm = Arc::new(VisibilityMap::new());
        match persistent_catalog.bootstrap_from_heap(heap.as_ref()) {
            Ok(stats) => {
                tracing::info!(?stats, "persistent catalog bootstrapped");
            }
            Err(e) => {
                // Bootstrap must not fail on a fresh in-memory database.
                // If it does, log the error but do not panic so tests and
                // development builds can still start.  The fallback is an
                // empty persistent catalog.
                tracing::warn!(error = %e, "persistent catalog bootstrap failed; using empty catalog");
            }
        }

        let ssi = Arc::new(SsiManager::new());
        let txn_manager = Arc::new(TransactionManager::new_with_ssi(ssi));
        let plan_cache = Arc::new(PlanCache::new(PlanCacheConfig::default()));

        // Per-process tempdir for the 2PC coordinator. Production
        // wiring (`Server::init`) replaces this with `<data_dir>/pg_twophase`.
        let two_phase_dir =
            std::env::temp_dir().join(format!("ultrasql-twophase-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&two_phase_dir);
        let two_phase = Arc::new(ultrasql_txn::two_phase::TwoPhaseCoordinator::new(
            two_phase_dir,
        ));
        Self {
            catalog,
            tables,
            data_dir: None,
            persistent_catalog,
            heap,
            page_loader,
            vm,
            txn_manager,
            plan_cache,
            vacuum_commit_counter: std::sync::atomic::AtomicU64::new(0),
            stats_catalog: parking_lot::RwLock::new(InMemoryStatsCatalog::new()),
            table_constraints: Arc::new(dashmap::DashMap::new()),
            domain_constraints: Arc::new(dashmap::DashMap::new()),
            row_security: Arc::new(dashmap::DashMap::new()),
            sequences: Arc::new(dashmap::DashMap::new()),
            sequence_owners: Arc::new(dashmap::DashMap::new()),
            sequence_namespaces: Arc::new(dashmap::DashMap::new()),
            schemas: Arc::new(dashmap::DashMap::new()),
            operators: Arc::new(dashmap::DashMap::new()),
            materialized_views: Arc::new(dashmap::DashMap::new()),
            regular_views: Arc::new(dashmap::DashMap::new()),
            columnar_storage: Arc::new(columnar_storage::ColumnarSecondaryStore::new()),
            time_partitions: Arc::new(dashmap::DashMap::new()),
            logical_replication: Arc::new(replication::LogicalReplicationRuntime::new()),
            workload_recorder: Arc::new(workload::WorkloadRecorder::new()),
            table_modifications: dashmap::DashMap::new(),
            table_analyze_modifications: dashmap::DashMap::new(),
            pending_analyze_tables: dashmap::DashMap::new(),
            autovacuum_config: AutovacuumConfig::default(),
            logging_config: LoggingConfig::default(),
            idle_session_timeout_ms: 0,
            default_statement_timeout_ms: crate::DEFAULT_STATEMENT_TIMEOUT_MS,
            memory_admission: crate::MemoryAdmission::from_env_or_auto(),
            wal_archive_config: WalArchiveConfig::default(),
            two_phase,
            auth: AuthConfig::Trust,
            tls_server_config: None,
            role_catalog: Arc::new(auth::InMemoryAuthCatalog::with_bootstrap_superuser()),
            role_connection_limiter: Arc::new(auth::RoleConnectionLimiter::new()),
            privilege_catalog: sample_privilege_catalog(),
            notify_hub: Arc::new(notify::NotifyHub::new()),
            cancel_registry: Arc::new(cancel::CancelRegistry::new()),
            next_pid: std::sync::atomic::AtomicU32::new(1),
            standby_mode: std::sync::atomic::AtomicBool::new(false),
            standby_apply_lsn: std::sync::atomic::AtomicU64::new(0),
            checkpointer: None,
            wal_writer: None,
            wal_buffer_sink: None,
            wal_dir: None,
        }
    }

    /// Enable or disable hot-standby read-only query mode.
    pub fn set_standby_mode(&self, enabled: bool) {
        self.standby_mode
            .store(enabled, std::sync::atomic::Ordering::Release);
    }

    /// Return whether hot-standby read-only mode is active.
    #[must_use]
    pub fn is_standby_mode(&self) -> bool {
        self.standby_mode.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Raw hot-standby WAL-apply cursor (next LSN not yet applied by
    /// continuous replay). Used by the standby walreceiver loop to skip
    /// redundant apply calls and by diagnostics.
    #[must_use]
    pub fn standby_apply_lsn_raw(&self) -> u64 {
        self.standby_apply_lsn
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// LSN through which the runtime WAL writer has fsynced.
    ///
    /// Returns `None` for in-memory sample servers because those instances do
    /// not own an on-disk WAL writer.
    #[must_use]
    pub fn runtime_wal_flushed_lsn(&self) -> Option<ultrasql_core::Lsn> {
        self.wal_writer
            .as_ref()
            .map(ultrasql_wal::WalWriter::flushed_lsn)
    }

    /// Hot-standby WAL-apply cursor: the next LSN not yet replayed. Seeded at
    /// recovery and advanced by [`Server::apply_landed_wal`]; on a primary it
    /// stays at the recovered LSN.
    #[must_use]
    pub fn standby_apply_cursor_lsn(&self) -> ultrasql_core::Lsn {
        ultrasql_core::Lsn::new(
            self.standby_apply_lsn
                .load(std::sync::atomic::Ordering::Acquire),
        )
    }

    /// Append a commit marker for WAL-backed SQL recovery.
    ///
    /// `committed_subxids` are the subtransaction XIDs that committed atomically
    /// with the parent (released and implicitly-released-at-commit subxids). They
    /// ride inside this single Commit record so recovery marks the whole family
    /// `Committed` together — a row inserted under a released savepoint must not
    /// vanish after a pure-WAL restart. The list excludes `ROLLBACK TO`-aborted
    /// subxids, which correctly default to `Aborted` on recovery.
    pub(crate) fn append_commit_record(
        &self,
        xid: Xid,
        committed_subxids: Vec<Xid>,
    ) -> Result<Option<Lsn>, ServerError> {
        let Some(wal) = self.heap.wal_sink() else {
            return Ok(None);
        };
        let payload = CommitPayload {
            commit_lsn: Lsn::ZERO,
            commit_timestamp_micros: unix_timestamp_micros(),
            committed_subxids,
        };
        let encoded = payload
            .encode()
            .map_err(|e| ServerError::ddl(format!("commit WAL payload encode: {e}")))?;
        let record = WalRecord::new(RecordType::Commit, xid, Lsn::ZERO, 0, encoded)
            .map_err(|e| ServerError::ddl(format!("commit WAL record encode: {e}")))?;
        wal.append(record)
            .map(Some)
            .map_err(|e| ServerError::ddl(format!("commit WAL append: {e}")))
    }

    /// Append an abort marker for WAL-backed SQL recovery.
    pub(crate) fn append_abort_record(&self, xid: Xid) -> Result<Option<Lsn>, ServerError> {
        let Some(wal) = self.heap.wal_sink() else {
            return Ok(None);
        };
        let payload = AbortPayload {
            abort_lsn: Lsn::ZERO,
        };
        let record = WalRecord::new(RecordType::Abort, xid, Lsn::ZERO, 0, payload.encode())
            .map_err(|e| ServerError::ddl(format!("abort WAL record encode: {e}")))?;
        wal.append(record)
            .map(Some)
            .map_err(|e| ServerError::ddl(format!("abort WAL append: {e}")))
    }

    /// Force a WAL durability barrier, flush eligible heap pages, then append
    /// and fsync a checkpoint record.
    pub(crate) fn perform_checkpoint(&self) -> Result<(), ServerError> {
        let Some(wal) = self.heap.wal_sink() else {
            self.flush_dirty_heap_pages()?;
            return Ok(());
        };

        let barrier = WalRecord::new(RecordType::Nop, Xid::INVALID, Lsn::ZERO, 0, Vec::new())
            .map_err(|e| ServerError::ddl(format!("checkpoint barrier WAL record encode: {e}")))?;
        let barrier_lsn = wal
            .append(barrier)
            .map_err(|e| ServerError::ddl(format!("checkpoint barrier WAL append: {e}")))?;
        self.wait_for_wal_durable(barrier_lsn)?;

        self.flush_dirty_heap_pages()?;

        // Make the flushed pages durable on disk BEFORE recording the
        // checkpoint LSN, so the checkpoint is a true durability barrier: every
        // heap mutation up to checkpoint_lsn is on disk, not merely in the OS
        // page cache. This is a prerequisite for recycling WAL segments below
        // the checkpoint — removing those WAL records is only safe once the
        // pages they would otherwise replay are themselves durable.
        self.page_loader
            .fsync_all()
            .map_err(|e| ServerError::ddl(format!("checkpoint fsync data segments: {e}")))?;

        let redo_from = self
            .heap
            .buffer_pool()
            .oldest_dirty_lsn()
            .filter(|oldest| oldest.raw() < barrier_lsn.raw())
            .unwrap_or(barrier_lsn);
        let payload = CheckpointPayload {
            redo_from,
            oldest_in_progress: self.txn_manager.oldest_in_progress(),
            next_xid: self.txn_manager.next_xid(),
        };
        let checkpoint = WalRecord::new(
            RecordType::Checkpoint,
            Xid::INVALID,
            Lsn::ZERO,
            0,
            payload.encode(),
        )
        .map_err(|e| ServerError::ddl(format!("checkpoint WAL record encode: {e}")))?;
        let checkpoint_lsn = wal
            .append(checkpoint)
            .map_err(|e| ServerError::ddl(format!("checkpoint WAL append: {e}")))?;
        self.wait_for_wal_durable(checkpoint_lsn)?;
        self.heap
            .last_checkpoint_lsn
            .fetch_max(checkpoint_lsn.raw(), std::sync::atomic::Ordering::AcqRel);

        // Write a durable per-index vector snapshot so the next restart can load
        // it and replay only the WAL above its meta.lsn instead of rebuilding
        // the whole HNSW graph. Best-effort: a snapshot is an optimization (the
        // WAL is the source of truth and a stale/corrupt snapshot is rejected on
        // load), so a write failure is logged and the checkpoint still succeeds.
        if let Some(data_dir) = &self.data_dir {
            let mut all_snapshots_ok = true;
            // The lowest snapshot LSN per vector-index family, captured *before*
            // each encode. meta.lsn is monotone, so the value a snapshot actually
            // embeds is >= this; the truncation floor stays at or below every
            // snapshot's real coverage. `ZERO` (no logged mutation) is excluded.
            let mut min_hnsw_snapshot_lsn: Option<Lsn> = None;
            let mut min_ivfflat_snapshot_lsn: Option<Lsn> = None;
            for table in self.table_constraints.iter() {
                for (oid, index_meta) in &table.value().indexes {
                    if let Some(hnsw) = &index_meta.hnsw {
                        let snap_lsn = hnsw.snapshot_lsn();
                        let bytes = hnsw.encode_snapshot();
                        match write_vector_snapshot(data_dir, *oid, &bytes) {
                            Ok(()) => {
                                min_hnsw_snapshot_lsn =
                                    fold_min_nonzero_lsn(min_hnsw_snapshot_lsn, snap_lsn);
                            }
                            Err(e) => {
                                all_snapshots_ok = false;
                                tracing::warn!(
                                    error = %e,
                                    oid = oid.raw(),
                                    "vector index snapshot write failed; full replay on next restart"
                                );
                            }
                        }
                    }
                    if let Some(ivfflat) = &index_meta.ivfflat {
                        let snap_lsn = ivfflat.snapshot_lsn();
                        let bytes = ivfflat.encode_snapshot();
                        match write_vector_snapshot(data_dir, *oid, &bytes) {
                            Ok(()) => {
                                min_ivfflat_snapshot_lsn =
                                    fold_min_nonzero_lsn(min_ivfflat_snapshot_lsn, snap_lsn);
                            }
                            Err(e) => {
                                all_snapshots_ok = false;
                                tracing::warn!(
                                    error = %e,
                                    oid = oid.raw(),
                                    "vector index snapshot write failed; full replay on next restart"
                                );
                            }
                        }
                    }
                }
            }

            // Write a durable commit-log snapshot stamped with the checkpoint
            // LSN so the WAL Commit/Abort records below the checkpoint can later
            // be recycled without losing the status of transactions that
            // resolved before it. Best-effort, same contract as above: a missing
            // or corrupt snapshot is rejected on load and recovery falls back to
            // a full WAL commit-status rebuild.
            let (next_xid, clog_entries) = self.txn_manager.export_clog();
            let clog_bytes = encode_clog_snapshot(checkpoint_lsn, next_xid, &clog_entries);
            let clog_ok = match write_clog_snapshot(data_dir, &clog_bytes) {
                Ok(()) => true,
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "commit-log snapshot write failed; full WAL rebuild on next restart"
                    );
                    false
                }
            };

            // With every secondary structure durably snapshotted, recycle the WAL
            // segments below the safe floor.
            self.maybe_recycle_wal(
                redo_from,
                min_hnsw_snapshot_lsn,
                min_ivfflat_snapshot_lsn,
                all_snapshots_ok && clog_ok,
            );
        }
        Ok(())
    }

    /// Run one full checkpoint cycle for a background timer, logging and
    /// swallowing any error so a transient failure never tears down the loop.
    ///
    /// A full checkpoint flushes dirty pages, fsyncs the data segments, writes
    /// the per-index and commit-log snapshots, and recycles WAL segments below
    /// the safe floor. In in-memory (no-WAL) mode this is a cheap no-op, since
    /// `perform_checkpoint` returns early without a WAL sink.
    pub fn run_checkpoint_cycle(&self) {
        if let Err(e) = self.perform_checkpoint() {
            warn!(error = %e, "automatic checkpoint cycle failed; will retry next interval");
        }
    }

    /// Recycle WAL segments that lie entirely below the safe recovery floor.
    ///
    /// The floor is the most conservative of three bounds, so recovery can still
    /// reconstruct every committed byte and resolve every transaction:
    /// * `redo_from` — the checkpoint redo point; the heap is durable up to it.
    /// * the oldest in-progress transaction's first written LSN — its records
    ///   must survive or recovery cannot mark it aborted, and an unknown XID
    ///   defaults to `InProgress` forever (wrong visibility, spurious conflicts).
    /// * every HNSW and IVFFlat index's snapshot LSN — each rebuilds from its
    ///   snapshot plus the retained WAL above that LSN.
    ///
    /// Recycling is skipped entirely when any required snapshot failed to become
    /// durable. An index with no logged mutation (snapshot LSN `ZERO`) has no WAL
    /// records of its own and imposes no floor — `fold_min_nonzero_lsn` excludes
    /// it, so its `None` bound here simply does not constrain the floor.
    /// Minimum `restart_lsn` across all persisted physical replication slots,
    /// or `None` when there are none with a parseable LSN (in-memory mode, no
    /// slots, or only archive-filename slots). Used to floor WAL recycling so a
    /// held slot prevents losing WAL a standby still needs.
    pub(crate) fn min_physical_slot_restart_lsn(&self) -> Option<Lsn> {
        let dir = self.data_dir.as_ref()?.join("pg_replslot");
        let store = crate::replication::ReplicationSlotStore::open(dir).ok()?;
        let slots = store.list().ok()?;
        crate::replication::min_parseable_restart_lsn(&slots)
    }

    pub(crate) fn maybe_recycle_wal(
        &self,
        redo_from: Lsn,
        min_hnsw_snapshot_lsn: Option<Lsn>,
        min_ivfflat_snapshot_lsn: Option<Lsn>,
        snapshots_ok: bool,
    ) {
        let (Some(sink), Some(wal_dir)) = (&self.wal_buffer_sink, &self.wal_dir) else {
            return;
        };
        // Prune resolved transactions from the first-LSN map (bounding its growth
        // even when we cannot truncate) and learn the oldest still-active one.
        let oldest_active = sink
            .prune_terminal_and_oldest_active_first_lsn(|xid| self.txn_manager.is_in_progress(xid));
        if !snapshots_ok {
            return;
        }
        let mut floor = redo_from.raw();
        if let Some(active) = oldest_active {
            floor = floor.min(active.raw());
        }
        if let Some(hnsw_lsn) = min_hnsw_snapshot_lsn {
            floor = floor.min(hnsw_lsn.raw());
        }
        if let Some(ivfflat_lsn) = min_ivfflat_snapshot_lsn {
            floor = floor.min(ivfflat_lsn.raw());
        }
        // A held physical replication slot pins WAL: never recycle below the
        // minimum `restart_lsn` across all slots, or a lagging standby would
        // silently lose segments it still needs to catch up. See
        // `session/replication.rs` and `docs/streaming-replication-design.md`.
        if let Some(slot_floor) = self.min_physical_slot_restart_lsn() {
            floor = floor.min(slot_floor.raw());
        }
        match ultrasql_wal::truncate_below(wal_dir, Lsn::new(floor)) {
            Ok(outcome) if !outcome.is_noop() => tracing::info!(
                removed = outcome.removed_segments.len(),
                reclaimed_bytes = outcome.reclaimed_bytes,
                floor_segment = outcome.floor.segment_index,
                floor_lsn = outcome.floor.floor_lsn.raw(),
                "recycled WAL segments below checkpoint floor"
            ),
            Ok(_) => {}
            Err(e) => {
                tracing::warn!(error = %e, "WAL segment recycling failed; segments retained")
            }
        }
    }

    /// Wait until the runtime WAL writer has fsynced at least `lsn`.
    ///
    /// A merely *slow* fsync (a busy or sluggish disk) is NOT a failure: the
    /// writer thread is alive and will eventually advance `flushed_lsn`, so we
    /// keep polling. A 5-second timeout that abandoned a still-pending durable
    /// write would be wrong on the commit path — the Commit record may yet flush
    /// — so this wait does not give up on slowness alone.
    ///
    /// It DOES return promptly with an error in exactly one case: the writer
    /// thread has terminated with a hard error
    /// ([`WalWriter::has_fatal_error`](ultrasql_wal::WalWriter::has_fatal_error)).
    /// Then `flushed_lsn` can never reach the target and waiting longer is
    /// pointless. A last-resort upper bound ([`WAL_DURABILITY_HARD_CAP`]) guards
    /// against a writer somehow wedged without flipping the fatal flag; reaching
    /// it is reported as an error too. Callers on the commit path treat *any*
    /// error returned here — fatal flag or cap — as an unrecoverable durability
    /// failure (the Commit record was already appended and may still flush), so
    /// the precise reason only affects diagnostics.
    pub(crate) fn wait_for_wal_durable(&self, lsn: Lsn) -> Result<(), ServerError> {
        let Some(writer) = &self.wal_writer else {
            return Ok(());
        };
        if lsn == Lsn::ZERO {
            return Ok(());
        }

        // A generous last-resort bound: a healthy writer fsyncs within
        // milliseconds, so any wait approaching this is a writer that is wedged
        // (not merely slow) yet did not flip the fatal flag. Far above any
        // plausible fsync latency, it exists only so a waiter cannot hang
        // forever; normal slow-disk commits resolve long before it.
        const WAL_DURABILITY_HARD_CAP: std::time::Duration = std::time::Duration::from_secs(120);
        const WAL_DURABILITY_POLL: std::time::Duration = std::time::Duration::from_micros(50);

        let started = std::time::Instant::now();
        loop {
            let flushed = writer.flushed_lsn();
            if flushed.raw() >= lsn.raw() {
                return Ok(());
            }
            // Dead writer: it will never advance again. Surface a hard error so
            // the caller can escalate (the commit path turns this fatal).
            if writer.has_fatal_error() {
                return Err(ServerError::Io(std::io::Error::other(format!(
                    "WAL writer terminated with a hard error before reaching durability: \
                     flushed_lsn={} target_lsn={}",
                    flushed.raw(),
                    lsn.raw()
                ))));
            }
            if started.elapsed() >= WAL_DURABILITY_HARD_CAP {
                return Err(ServerError::Io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    format!(
                        "WAL durability wait exceeded hard cap at flushed_lsn={} target_lsn={}",
                        flushed.raw(),
                        lsn.raw()
                    ),
                )));
            }
            writer.notify();
            std::thread::sleep(WAL_DURABILITY_POLL);
        }
    }

    /// Force every data WAL record written by `xid` durable (fsynced).
    ///
    /// Used by 2PC `PREPARE TRANSACTION`: before the prepared-state file is
    /// written and the PREPARE is acked, ALL of the transaction's data WAL
    /// records must be on disk. Otherwise a crash after the PREPARE ack but
    /// before the WAL tail is fsynced would leave the txn in-doubt (the
    /// prepared-state file is durable) while its row changes were torn off the
    /// un-fsynced WAL tail and discarded — a later `COMMIT PREPARED` would then
    /// mark it committed but replay nothing, silently losing an acknowledged
    /// transaction.
    ///
    /// Captures the txn's last appended data WAL LSN via the sink's per-xid
    /// `last_lsn_for` and waits until it is flushed. A no-WAL sink, or an xid
    /// that appended no data records, resolves to `Lsn::ZERO` and returns
    /// immediately.
    pub(crate) fn wait_for_txn_data_wal_durable(&self, xid: Xid) -> Result<(), ServerError> {
        let Some(wal) = self.heap.wal_sink() else {
            return Ok(());
        };
        let last_lsn = wal.last_lsn_for(xid);
        self.wait_for_wal_durable(last_lsn)
    }

    /// Resolve the durability of a commit whose Commit record has *already been
    /// appended* to the durable WAL pipeline, returning the only two outcomes
    /// that path may produce.
    ///
    /// Once `append_commit_record` returns `Ok(Some(commit_lsn))` the Commit
    /// record is in the shared WAL buffer and the background writer will fsync
    /// it on its own schedule — the live process can neither remove it nor
    /// reliably observe whether it became durable. So the **only** correct
    /// outcomes here are:
    ///
    /// * [`CommitDurability::Durable`] — `wait_for_wal_durable` confirmed the
    ///   record is on disk; the commit may now be finalized visible.
    /// * [`CommitDurability::Fatal`] — the wait failed (the writer died, or the
    ///   hard cap fired). The record may STILL flush a moment later, so we may
    ///   NOT abort-in-memory and tell the client the commit failed: a crash
    ///   would then let recovery find the durable Commit record and resurrect a
    ///   txn the client was told failed (a phantom commit). The caller must take
    ///   the process down so recovery decides the true outcome from the WAL.
    ///
    /// Crucially, there is no "abort in memory and return a normal error"
    /// outcome — this is exactly the torn-durability contract the gate flagged.
    /// Pulled out as a pure function so the decision is unit-testable without
    /// actually aborting the process.
    pub(crate) fn resolve_commit_durability_after_append(
        &self,
        commit_lsn: Lsn,
    ) -> CommitDurability {
        match self.wait_for_wal_durable(commit_lsn) {
            Ok(()) => CommitDurability::Durable,
            Err(e) => CommitDurability::Fatal(format!(
                "WAL could not be made durable after the Commit record was appended \
                 (commit_lsn={}): {e}",
                commit_lsn.raw()
            )),
        }
    }

    /// Escalate an unrecoverable commit-durability failure to a controlled, hard
    /// process termination.
    ///
    /// The Commit record is in the durable WAL pipeline and may still flush, so
    /// the live process can no longer give the client a truthful answer. Per
    /// PostgreSQL's PANIC-on-commit-flush-failure contract, the backend goes
    /// down and recovery on restart decides the true outcome from the durable
    /// WAL — the single source of truth. The client sees a connection drop,
    /// i.e. an AMBIGUOUS (correct) outcome for an indeterminate commit.
    ///
    /// We use [`std::process::abort`] (SIGABRT) rather than `panic!`: a panic
    /// would UNWIND and be swallowed by the per-statement `catch_unwind` panic
    /// isolation (FIX #2), leaving the process alive and the phantom-commit
    /// window open. `abort()` cannot be caught, so the process really
    /// terminates and recovery runs on restart.
    ///
    /// In test builds the abort is replaced by a recorded flag (the
    /// `record_fatal_commit_abort` test seam) so the decision can be asserted
    /// without killing the test process.
    pub(crate) fn fatal_commit_durability_failure(
        &self,
        xid: Xid,
        context: &str,
        reason: &str,
    ) -> ! {
        error!(
            xid = xid.raw(),
            context,
            reason,
            "FATAL: WAL durability could not be guaranteed for a committing transaction; \
             its Commit record is already in the durable WAL pipeline and may yet flush. \
             Durability cannot be guaranteed in-process, so the server is aborting so that \
             crash recovery decides the transaction's true outcome from the durable WAL on \
             restart. The client's commit outcome is AMBIGUOUS (connection drop)."
        );
        #[cfg(test)]
        {
            crate::tests::record_fatal_commit_abort(xid, context, reason);
            // In tests we must not actually abort. Park the (logically dead)
            // caller forever on a thread the test never joins: returning would
            // violate the `-> !` contract and let the phantom-commit path
            // continue, which is exactly what we are proving cannot happen.
            loop {
                std::thread::sleep(std::time::Duration::from_secs(3600));
            }
        }
        #[cfg(not(test))]
        {
            std::process::abort();
        }
    }

    /// Commit a transaction. When it changed persistent heap/index state, the
    /// commit is made **durable before it is made visible** (PostgreSQL's commit
    /// protocol): append the Commit WAL record, fsync it, and only then flip the
    /// CLOG to `Committed` and release the txn's locks.
    ///
    /// Ordering (durable path, `durable_commit_marker == true`):
    ///
    /// 1. `commit_check_serializable` — run the SSI dangerous-structure check and
    ///    compute the committed-subxid family. The CLOG is still `InProgress`, so
    ///    the txn is MVCC-invisible and holds its locks. A serialization anomaly
    ///    aborts here (no Commit record written — in-memory abort is safe).
    /// 2. `append_commit_record` — *before* the record is appended, an append
    ///    failure means NOTHING entered the durable pipeline, so the txn is
    ///    aborted in memory (CLOG `Aborted`, locks released) and a normal error
    ///    returned. *After* a successful append, the Commit record is in the
    ///    durable WAL pipeline and the background writer may fsync it at any
    ///    moment. From here the live process can neither undo nor reliably
    ///    observe that record, so `wait_for_wal_durable` has exactly two
    ///    outcomes (see [`Self::resolve_commit_durability_after_append`]):
    ///    durable -> finalize visible; failure -> FATAL process abort (NOT an
    ///    in-memory abort — that would risk a phantom commit recovery later
    ///    resurrects).
    /// 3. `commit_finalize_visible` — flip the CLOG to `Committed` and release
    ///    locks. Only now can any other backend observe the commit.
    ///
    /// This closes the window where a committed-but-not-yet-durable txn was
    /// visible (and its locks gone): a concurrent reader can never see a commit
    /// that a subsequent crash would roll back. It also closes the inverse
    /// window the gate flagged: a client is NEVER told a commit failed (via
    /// in-memory abort) while its Commit record could still flush to disk.
    ///
    /// When `durable_commit_marker == false` (read-only / no-WAL txns) there is
    /// nothing to make durable, so the check and the visibility flip run
    /// back-to-back via the single-shot `commit`.
    pub(crate) fn commit_transaction(
        &self,
        txn: ultrasql_txn::Transaction,
        durable_commit_marker: bool,
        context: &str,
    ) -> Result<(), ServerError> {
        let xid = txn.xid;

        if !durable_commit_marker {
            // No persistent state changed: nothing to fsync, so flip visible
            // immediately (the single-shot path runs the SSI check + flip).
            self.txn_manager.commit(txn).map_err(|e| match e {
                TxnError::SerializationFailure { detail, .. } => {
                    ServerError::SerializationFailure(detail)
                }
                other => ServerError::ddl(format!("{context} commit: {other}")),
            })?;
            return Ok(());
        }

        // Phase 1: run the serializable check and compute the committed-subxid
        // family WITHOUT making the txn visible. On a serialization anomaly the
        // txn is still InProgress and invisible; abort it and surface the error.
        let committed_subxids = match self.txn_manager.commit_check_serializable(&txn) {
            Ok(family) => family,
            Err(TxnError::SerializationFailure { detail, .. }) => {
                // The CLOG was never flipped; abort to release locks and mark
                // the txn Aborted, then report the serialization failure.
                let _ = self.txn_manager.abort(txn);
                return Err(ServerError::SerializationFailure(detail));
            }
            Err(other) => {
                let _ = self.txn_manager.abort(txn);
                return Err(ServerError::ddl(format!("{context} commit: {other}")));
            }
        };

        // Phase 2a: append the Commit record. The txn is STILL invisible (CLOG
        // InProgress) and STILL holds its locks, so no other backend can observe
        // it yet. If the APPEND itself fails the record never entered the durable
        // pipeline — nothing can flush — so an in-memory abort here is safe and
        // correct (CLOG Aborted, locks released, normal error to the client).
        let commit_lsn = match self.append_commit_record(xid, committed_subxids.clone()) {
            Ok(Some(commit_lsn)) => commit_lsn,
            Ok(None) => {
                // No WAL sink (in-memory): nothing to fsync, so the commit is
                // trivially "durable". Fall straight through to finalize.
                return self.finalize_commit_visible(&txn, &committed_subxids, context);
            }
            Err(e) => {
                // Append failed BEFORE the record entered the pipeline: safe to
                // abort in memory and surface the error.
                let _ = self.txn_manager.abort(txn);
                return Err(e);
            }
        };

        // Phase 2b: the Commit record is NOW in the durable WAL pipeline. From
        // here we MUST NOT abort-in-memory-and-return-an-error on a durability
        // failure: the record may still flush, and a crash would let recovery
        // resurrect a txn we told the client had failed (a phantom commit). The
        // only outcomes are durable (-> finalize) or fatal (-> process abort so
        // recovery decides from the durable WAL).
        match self.resolve_commit_durability_after_append(commit_lsn) {
            CommitDurability::Durable => {
                self.finalize_commit_visible(&txn, &committed_subxids, context)
            }
            CommitDurability::Fatal(reason) => {
                // Does NOT call txn_manager.abort: that is the phantom-commit
                // window. Takes the process down (bypassing catch_unwind) so the
                // durable WAL is the single source of truth on restart.
                self.fatal_commit_durability_failure(xid, context, &reason)
            }
        }
    }

    /// Phase 3 of the durable commit: flip the CLOG to `Committed` and release
    /// the txn's locks. Only reached once the commit is durable (or there is no
    /// WAL to make durable), so the txn becoming visible can never be rolled
    /// back by a later crash. The lifecycle is driven linearly (phase 1 just
    /// succeeded), so this only fails on a mis-driven double-commit, surfaced
    /// unchanged.
    fn finalize_commit_visible(
        &self,
        txn: &ultrasql_txn::Transaction,
        committed_subxids: &[Xid],
        context: &str,
    ) -> Result<(), ServerError> {
        self.txn_manager
            .commit_finalize_visible(txn, committed_subxids)
            .map_err(|e| ServerError::ddl(format!("{context} commit finalize: {e}")))
    }

    /// Abort a transaction and, when it changed persistent heap/index state,
    /// force its abort marker durable before reporting rollback success.
    pub(crate) fn abort_transaction(
        &self,
        txn: ultrasql_txn::Transaction,
        durable_abort_marker: bool,
        context: &str,
    ) -> Result<(), ServerError> {
        let xid = txn.xid;
        self.txn_manager
            .abort(txn)
            .map_err(|e| ServerError::ddl(format!("{context} abort: {e}")))?;
        if durable_abort_marker && let Some(abort_lsn) = self.append_abort_record(xid)? {
            self.wait_for_wal_durable(abort_lsn)?;
        }
        Ok(())
    }

    /// Flush dirty heap pages into the sample server's spill store.
    pub fn flush_dirty_heap_pages(&self) -> Result<usize, ServerError> {
        let loader = self.page_loader.clone();
        self.heap
            .buffer_pool()
            .try_flush_dirty(|page_id, page| loader.store(page_id, page))
            .map_err(|e| ServerError::ddl(format!("flush dirty heap pages: {e}")))
    }

    /// Flush dirty heap pages only when bulk loads put real pressure on frames.
    ///
    /// COPY batches call this after insert. A full flush after every 4096 rows
    /// turns SF10 loads into repeated whole-pool scans; pressure gating keeps
    /// the eviction invariant while avoiding O(pool_frames × batches) work.
    pub fn flush_dirty_heap_pages_if_needed(&self) -> Result<Option<usize>, ServerError> {
        let pool = self.heap.buffer_pool();
        let capacity = pool.capacity();
        let resident_threshold = capacity.saturating_mul(3) / 4;
        let dirty_threshold = capacity.saturating_mul(1) / 8;

        // Cheap precheck: this runs after EVERY row-producing INSERT
        // statement, so it must not sweep the frame array. The O(1)
        // dirty counter + page-table length decide "no pressure" without
        // touching the full stats() sweep.
        let dirty = pool.dirty_pages();
        let resident = pool.resident_pages();
        if capacity == 0 || dirty == 0 || resident < resident_threshold || dirty < dirty_threshold {
            return Ok(None);
        }

        let before = pool.stats();
        let flushed = self.flush_dirty_heap_pages()?;
        let after = pool.stats();
        info!(
            capacity,
            resident_before = before.resident,
            dirty_before = before.dirty,
            pinned_before = before.pinned,
            flushed,
            resident_after = after.resident,
            dirty_after = after.dirty,
            pinned_after = after.pinned,
            "bulk load buffer-pool pressure flush"
        );
        Ok(Some(flushed))
    }
}

/// Force-and-wait closure type used by [`ServerEvictionRelief`] to advance the
/// durable WAL position before re-flushing a gated frame.
///
/// Returns `Ok(())` once the WAL is durable through the target LSN, or an
/// `std::io::Error` (e.g. a durability timeout) on failure. The closure must
/// be invoked with **no pool/frame latch held** — it busy-polls the WAL writer
/// and would convoy every concurrent miss behind WAL I/O otherwise (see
/// [`EvictionRelief`] and ARCHITECTURE.md §14).
type WalForceFn = Arc<dyn Fn(Lsn) -> std::io::Result<()> + Send + Sync>;

/// Server-side [`EvictionRelief`] implementation.
///
/// Reuses the single existing write-back site
/// [`BufferPool::try_flush_dirty`](ultrasql_storage::BufferPool::try_flush_dirty)
/// and, when every dirty victim is ahead of the durable WAL, the WAL
/// force-and-wait primitive — both invoked from `relieve` with no pool/frame
/// latch held (the buffer pool calls `relieve` only after `get_page` returned
/// `Exhausted` and released its latches). It adds no new write-back path: the
/// pool's loader stays read-only and the WAL-before-data gate is enforced
/// entirely by `try_flush_dirty`.
struct ServerEvictionRelief {
    /// The buffer pool whose dirty pages this hook flushes.
    pool: Arc<BufferPool<BlankPageLoader>>,
    /// Writer side-channel: persists a page so its frame becomes evictable.
    page_loader: BlankPageLoader,
    /// Force-and-wait the WAL durable to a target LSN. `None` in WAL-less /
    /// in-memory mode, where `durable_lsn` is treated as `u64::MAX` so Phase A
    /// always flushes and Phase B never fires.
    force_wal_durable: Option<WalForceFn>,
}

impl ServerEvictionRelief {
    /// Phase A flush: write back every dirty, unpinned frame that is already
    /// at or below the durable WAL position. Returns the number flushed.
    fn flush_durable(&self) -> Result<usize, BufferPoolError> {
        let loader = self.page_loader.clone();
        self.pool
            .try_flush_dirty(move |page_id, page| loader.store(page_id, page))
            .map_err(BufferPoolError::Loader)
    }
}

impl EvictionRelief for ServerEvictionRelief {
    fn relieve(&self) -> Result<(), BufferPoolError> {
        // A poisoned pool must not be flushed; surface it like get_page would.
        if self.pool.is_poisoned() {
            return Err(BufferPoolError::Poisoned);
        }

        // Phase A — flush what is already durable. No WAL force, no latch.
        let flushed = self.flush_durable()?;
        if flushed > 0 {
            return Ok(());
        }

        // Phase B — every dirty unpinned victim is ahead of the durable WAL.
        // Force the WAL durable to the lowest such page-LSN (the minimum that
        // unblocks at least one frame) WITH NO LATCH HELD, then re-flush.
        if let Some(target) = self.pool.oldest_unflushable_dirty_lsn() {
            if let Some(force) = self.force_wal_durable.as_ref() {
                warn!(
                    target_lsn = target.raw(),
                    "eviction relief forcing WAL durable (buffer pool too small for dirty working set)"
                );
                force(target).map_err(|e| BufferPoolError::Loader(ultrasql_core::Error::Io(e)))?;
                let flushed = self.flush_durable()?;
                if flushed == 0 {
                    // Phase C — made no progress this round (e.g. frames got
                    // re-dirtied above the new durable LSN by a concurrent
                    // writer). The bounded loop in get_page_relieved decides
                    // whether to retry or surface Exhausted.
                    warn!(
                        target_lsn = target.raw(),
                        "eviction relief flushed nothing after WAL force; pool may be over-committed"
                    );
                }
            } else {
                // No WAL force available (in-memory mode). With no sink the
                // pool treats durable as u64::MAX, so this branch is
                // unreachable in practice; surface no progress gracefully.
                warn!("eviction relief: dirty frames blocked by WAL gate but no force available");
            }
        }
        Ok(())
    }
}

impl Server {
    /// Build and install the [`EvictionRelief`] hook on the buffer pool.
    ///
    /// Called once during construction, after the WAL writer exists (the force
    /// closure captures the writer's durability handle). `wal_writer` is
    /// `None` in in-memory / sample mode, where the force is a no-op because
    /// the pool's `durable_lsn` is `u64::MAX` and Phase A always flushes.
    pub(crate) fn install_eviction_relief(
        pool: &Arc<BufferPool<BlankPageLoader>>,
        page_loader: &BlankPageLoader,
        wal_writer: Option<&ultrasql_wal::WalWriter>,
    ) {
        let force_wal_durable: Option<WalForceFn> = wal_writer.map(|writer| {
            let durability = writer.durability_handle();
            let force: WalForceFn =
                Arc::new(move |lsn: Lsn| force_wal_durable_to(&durability, lsn));
            force
        });
        let relief = Arc::new(ServerEvictionRelief {
            pool: Arc::clone(pool),
            page_loader: page_loader.clone(),
            force_wal_durable,
        });
        pool.set_eviction_relief(relief);
    }
}

/// Busy-poll `durability` until it has fsynced through `lsn`, forcing fsyncs
/// with `notify`. Mirrors [`Server::wait_for_wal_durable`] but operates on a
/// cloneable [`WalDurabilityHandle`](ultrasql_wal::WalDurabilityHandle) so it
/// can run inside the eviction-relief closure (no `&Server` needed, no latch
/// held).
fn force_wal_durable_to(
    durability: &ultrasql_wal::WalDurabilityHandle,
    lsn: Lsn,
) -> std::io::Result<()> {
    if lsn == Lsn::ZERO {
        return Ok(());
    }

    const WAL_DURABILITY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
    const WAL_DURABILITY_POLL: std::time::Duration = std::time::Duration::from_micros(50);

    let started = std::time::Instant::now();
    loop {
        let flushed = durability.flushed_lsn();
        if flushed.raw() >= lsn.raw() {
            return Ok(());
        }
        if started.elapsed() >= WAL_DURABILITY_TIMEOUT {
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!(
                    "eviction-relief WAL durability wait timed out at flushed_lsn={} target_lsn={}",
                    flushed.raw(),
                    lsn.raw()
                ),
            ));
        }
        durability.notify();
        std::thread::sleep(WAL_DURABILITY_POLL);
    }
}

/// Outcome of waiting for a commit to become durable *after* its Commit record
/// has been appended to the durable WAL pipeline.
///
/// Deliberately has no "abort in memory" variant: once the Commit record is in
/// the pipeline the live process can neither undo nor reliably observe it, so a
/// durability failure can only be escalated to a fatal process abort (recovery
/// then decides the true outcome from the durable WAL). Telling the client the
/// commit failed while the record could still flush is the phantom-commit bug
/// this type exists to make unrepresentable.
#[derive(Debug)]
pub(crate) enum CommitDurability {
    /// The Commit record is on disk; the commit may be finalized visible.
    Durable,
    /// The durable-wait failed (dead WAL writer, or the hard cap fired). The
    /// record may still flush, so the process must abort and let recovery
    /// decide. Carries a human-readable reason for the FATAL log.
    Fatal(String),
}
