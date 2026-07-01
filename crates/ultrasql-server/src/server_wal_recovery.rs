//! `impl Server` methods (split out of the crate root): wal_recovery.
//!
//! Pure code motion from `lib.rs`; behavior unchanged.
use super::*;

impl Server {
    /// Append pre-encoded rows directly into heap pages for in-process
    /// benchmark setup.
    ///
    /// This bypasses the PostgreSQL wire COPY path and normal buffer-pool
    /// insert path, but preserves the heap page/tuple format used by scans.
    pub fn bulk_load_encoded_rows(
        &self,
        relation: RelationId,
        payloads: &[Vec<u8>],
        txn: &Transaction,
    ) -> Result<u64, ServerError> {
        let table = self
            .catalog_snapshot()
            .tables_by_oid
            .get(&relation.oid())
            .cloned()
            .ok_or_else(|| {
                ServerError::ddl(format!("bulk load relation {} not found", relation.oid()))
            })?;
        let n_atts = u16::try_from(table.schema.len())
            .map_err(|_| ServerError::ddl("bulk load schema column count exceeds u16"))?;
        let insert_opts = InsertOptions {
            xmin: txn.current_xid(),
            command_id: txn.current_command,
            n_atts,
            wal: None,
            fsm: None,
            vm: Some(self.vm.as_ref()),
        };
        let loader = self.page_loader.clone();
        self.heap
            .bulk_load_encoded_batch(relation, payloads, insert_opts, |page_id, page| {
                loader.store(page_id, page)
            })
            .map_err(|e| ServerError::ddl(format!("bulk load encoded rows: {e}")))
    }

    /// Record a backup marker in the data directory.
    ///
    /// Returns the current backup LSN surface. UltraSQL v0.9 does not expose a
    /// stable public LSN accessor yet, so the marker records wall-clock time
    /// and the SQL function returns the PostgreSQL-shaped zero LSN used by the
    /// existing recovery CLI placeholders.
    pub fn record_backup_marker(&self, function_name: &str) -> Result<String, ServerError> {
        let Some(data_dir) = &self.data_dir else {
            return Ok("0/0".to_owned());
        };
        let file_name = if function_name.eq_ignore_ascii_case("pg_start_backup") {
            "backup_label"
        } else {
            "backup_stop"
        };
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_secs())
            .unwrap_or(0);
        let payload = format!("function={function_name}\nlsn=0/0\nunix_seconds={now}\n");
        write_backup_marker_file(&data_dir.join(file_name), &payload)?;
        Ok("0/0".to_owned())
    }

    /// Builder: switch the server to MD5 password auth.
    ///
    /// Every incoming connection must present a `Password` response
    /// matching `MD5(MD5(password + username) || salt)`. Used by
    /// integration tests and as the configuration entry point for
    /// production deployments that wire MD5 in front of the real
    /// `pg_authid` table.
    #[must_use]
    pub fn require_md5_password(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.auth = AuthConfig::Md5 {
            username: username.into(),
            password: password.into(),
        };
        self
    }

    /// Builder: set the authentication policy directly.
    ///
    /// Used by the CLI (which resolves [`AuthConfig::Scram`] by deriving the
    /// verifier from a password file) and by integration tests that exercise
    /// the SCRAM handshake.
    #[must_use]
    pub fn with_auth(mut self, auth: AuthConfig) -> Self {
        self.auth = auth;
        self
    }

    /// Builder: enable TLS with a prepared rustls server configuration.
    ///
    /// Build the config from a [`crate::tls::TlsConfig`] via
    /// [`crate::tls::TlsHandshake::build_server_config`]. Once set, a client
    /// `SSLRequest` is answered with `'S'` and the connection is upgraded to
    /// TLS before the startup handshake.
    #[must_use]
    pub fn with_tls(mut self, config: Arc<rustls::ServerConfig>) -> Self {
        self.tls_server_config = Some(config);
        self
    }

    /// Record a successful commit and, every
    /// [`UNDO_GC_INTERVAL_COMMITS`] commits, run maintenance:
    /// undo-log GC plus one pending auto-analyze task.
    ///
    /// Bump-and-check is one atomic add plus a modulo; the heavier
    /// maintenance work is deferred out of the per-commit fast path.
    /// Errors from the maintenance pass are logged and swallowed so a
    /// transient failure cannot mask the underlying commit's success.
    pub fn note_commit_for_gc(&self) {
        use std::sync::atomic::Ordering;
        let n = self
            .vacuum_commit_counter
            .fetch_add(1, Ordering::Relaxed)
            .wrapping_add(1);
        if n % UNDO_GC_INTERVAL_COMMITS != 0 {
            return;
        }
        let oldest = self.txn_manager.oldest_in_progress();
        match self.heap.vacuum_undo_log(oldest) {
            Ok(trimmed) => {
                if trimmed > 0 {
                    tracing::debug!(
                        trimmed,
                        oldest_xid = oldest.raw(),
                        "undo-log GC trimmed entries"
                    );
                }
            }
            Err(e) => tracing::warn!(error = %e, "undo-log GC failed"),
        }
        self.vacuum_mark_visible_pages(oldest);
        // Retire committed SSI entries whose concurrent transactions have all
        // finished. Bounds the rw-conflict map and prevents long-committed
        // serializable transactions from fabricating spurious 40001 failures.
        let ssi_retired = self.txn_manager.collect_ssi_garbage(oldest);
        if ssi_retired > 0 {
            tracing::debug!(
                retired = ssi_retired,
                oldest_xid = oldest.raw(),
                "SSI committed-entry GC retired entries"
            );
        }
        self.run_one_pending_analyze();
    }

    /// Run one background autovacuum cycle across tables that crossed
    /// modification thresholds.
    pub fn run_autovacuum_cycle(&self) {
        let oldest = self.txn_manager.oldest_in_progress();
        if let Err(e) = self.heap.vacuum_undo_log(oldest) {
            tracing::warn!(error = %e, "autovacuum undo-log GC failed");
        }
        let snapshot = self.catalog_snapshot();
        for entry in snapshot.tables.values() {
            let table_name = table_entry_lookup_key(entry);
            let modified = self
                .table_modifications
                .get(&table_name)
                .map(|v| *v)
                .unwrap_or(0);
            let blocks = self
                .heap
                .block_count(RelationId(entry.oid))
                .max(entry.n_blocks);
            let estimated_rows = u64::from(blocks).saturating_mul(64);
            let threshold = autovacuum_config_for_table(self.autovacuum_config, entry)
                .vacuum_threshold_for_rows(estimated_rows);
            if modified < threshold {
                continue;
            }
            match self
                .heap
                .vacuum_heap(RelationId(entry.oid), oldest, self.txn_manager.as_ref())
            {
                Ok(stats) => {
                    self.workload_recorder
                        .record_table_autovacuum(entry.oid.raw());
                    if stats.tuples_reclaimed > 0 {
                        tracing::debug!(
                            table = %entry.name,
                            reclaimed = stats.tuples_reclaimed,
                            "autovacuum reclaimed heap tuples",
                        );
                    }
                }
                Err(e) => tracing::warn!(table = %entry.name, error = %e, "autovacuum heap failed"),
            }
            self.pending_analyze_tables.insert(table_name.clone(), ());
            self.table_modifications.insert(table_name, 0);
        }
        self.vacuum_mark_visible_pages(oldest);
        self.run_one_pending_analyze();
        self.run_one_pending_columnarization();
    }

    pub(crate) fn vacuum_mark_visible_pages(&self, oldest: ultrasql_core::Xid) {
        let snapshot = self.catalog_snapshot();
        for entry in snapshot.tables.values() {
            let rel = RelationId(entry.oid);
            let block_count = self.heap.block_count(rel).max(entry.n_blocks);
            if block_count == 0 {
                continue;
            }
            match self.heap.vacuum_mark_all_visible(
                rel,
                block_count,
                oldest,
                self.txn_manager.as_ref(),
                self.vm.as_ref(),
            ) {
                Ok(marked) => {
                    if marked > 0 {
                        tracing::debug!(
                            table = %entry.name,
                            marked,
                            "vacuum marked pages all-visible"
                        );
                    }
                }
                Err(e) => tracing::warn!(
                    table = %entry.name,
                    error = %e,
                    "vacuum all-visible certification failed"
                ),
            }
        }
    }

    /// Initialize a server that boots from `data_dir`.
    ///
    /// Brings up a buffer pool wired to an on-disk WAL writer that persists
    /// every heap mutation.  The WAL segments are written under
    /// `data_dir/pg_wal`.  On a fresh directory the catalog heap is empty
    /// and the initial snapshot is installed.
    ///
    /// This is the production entry point.  `with_sample_database` is the
    /// test/REPL entry point (no WAL, fully in-memory).
    ///
    /// # Errors
    ///
    /// Returns [`ServerError::Io`] when `data_dir` cannot be opened, when
    /// the WAL writer thread cannot be spawned, or when the heap bootstrap
    /// fails for a reason other than an empty heap. Returns
    /// [`ServerError::Ddl`] when the data directory itself is a symlink or
    /// is not owned by the effective user on Unix.
    pub fn init(data_dir: &Path) -> Result<Self, ServerError> {
        Self::init_with_wal_writer_config(data_dir, ultrasql_wal::WalWriterConfig::default())
    }

    /// [`Self::init`] with a custom WAL segment size in bytes.
    ///
    /// Smaller segments make checkpoint-driven segment recycling observable
    /// without writing tens of MiB — used by the crash-recovery drill — and are
    /// a legitimate operational knob for finer WAL-retention granularity. A
    /// `segment_size_bytes` of `0` falls back to the built-in default.
    pub fn init_with_wal_segment_size(
        data_dir: &Path,
        segment_size_bytes: u64,
    ) -> Result<Self, ServerError> {
        let default = ultrasql_wal::WalWriterConfig::default();
        let config = ultrasql_wal::WalWriterConfig {
            segment_size_bytes: if segment_size_bytes == 0 {
                default.segment_size_bytes
            } else {
                segment_size_bytes
            },
            ..default
        };
        Self::init_with_wal_writer_config(data_dir, config)
    }

    /// [`Self::init`] with an explicit WAL writer configuration. Used by tests
    /// that need a small segment size to exercise multi-segment recycling
    /// without writing tens of MiB.
    pub(crate) fn init_with_wal_writer_config(
        data_dir: &Path,
        wal_writer_config: ultrasql_wal::WalWriterConfig,
    ) -> Result<Self, ServerError> {
        use std::sync::Arc;
        use ultrasql_wal::{WalBuffer, WalWriter};
        use wal_sink::WalBufferSink;

        let data_dir = prepare_secure_data_dir(data_dir)?;
        let data_dir = data_dir.as_path();
        let catalog_version = catalog_version::ensure_catalog_version(data_dir)?;
        tracing::info!(
            version = catalog_version.observed_version,
            created = catalog_version.created,
            "catalog version marker checked"
        );

        // 1. WAL buffer — 8 MiB ring.
        const WAL_BUFFER_BYTES: usize = 8 * 1024 * 1024;
        let wal_buffer = Arc::new(WalBuffer::new(WAL_BUFFER_BYTES, ultrasql_core::Lsn::ZERO));
        let wal_dir = data_dir.join("pg_wal");

        // 2. Sink adapter bridges WalBuffer ↔ storage's WalSink trait. Keep a
        // typed clone so the checkpoint can read per-transaction first-LSNs that
        // the narrow `WalSink` trait does not expose.
        let buffer_sink = Arc::new(WalBufferSink::new(Arc::clone(&wal_buffer)));
        let sink: Arc<dyn ultrasql_storage::WalSink> = buffer_sink.clone();
        let last_checkpoint_lsn = Arc::new(std::sync::atomic::AtomicU64::new(0));

        // 3. Buffer pool with WAL.
        let page_loader = BlankPageLoader::persistent(data_dir.join("base")).map_err(|e| {
            ServerError::Io(std::io::Error::other(format!("heap segment store: {e}")))
        })?;
        let pool = Arc::new(BufferPool::with_wal(
            IN_MEMORY_POOL_FRAMES,
            page_loader.clone(),
            Arc::clone(&sink),
        ));
        let heap = Arc::new(HeapAccess::with_checkpoint_lsn(
            Arc::clone(&pool),
            Arc::clone(&last_checkpoint_lsn),
        ));
        let vm = Arc::new(VisibilityMap::new());
        let sequences = Arc::new(dashmap::DashMap::new());
        let sequence_owners = Arc::new(dashmap::DashMap::new());
        let sequence_namespaces = Arc::new(dashmap::DashMap::new());
        let schemas = Arc::new(dashmap::DashMap::new());

        // 4. Replay existing WAL before accepting new appends. The recovery
        // target restores heap/index pages through `HeapAccess` and sequence
        // state through the shared registry.
        let recovery_apply_target = ServerRecoveryTarget {
            heap: Arc::clone(&heap),
            sequences: Arc::clone(&sequences),
        };
        let recovery_replay_target = recovery_replay_target_from_data_dir(data_dir)?;
        // Seed the byte cursor from the WAL recovery floor (the start LSN of the
        // first surviving segment after any truncation). recover_with_target
        // seeds its own cursor from the same floor; both read the manifest, so
        // the LSNs they reconstruct agree. An absent manifest is LSN 0.
        let recovery_floor = ultrasql_wal::read_floor(&wal_dir)
            .map_err(|e| ServerError::Ddl(format!("read WAL recovery floor: {e}")))?;
        // WAL has been recycled iff the surviving stream no longer starts at the
        // origin. Only then is replay-from-floor missing early relation-extend
        // records and block counts must be re-seeded from the durable heap; with
        // a complete origin stream, WAL replay already reconstructs them exactly,
        // so seeding would be redundant (and would wrongly resurface a torn,
        // never-committed DDL row that the truncated WAL leaves uncommitted).
        let wal_was_recycled = recovery_floor.floor_lsn != ultrasql_core::Lsn::ZERO;
        let mut record_lsn = recovery_floor.floor_lsn;
        let recovered_lsn =
            ultrasql_wal::recover_with_target(&wal_dir, recovery_replay_target, |record| {
                let current_lsn = record_lsn;
                record_lsn = record_lsn
                    .checked_advance(u64::from(record.header.total_length))
                    .ok_or(ultrasql_wal::RecoveryError::Record(
                        ultrasql_wal::WalRecordError::Malformed("replay lsn overflow"),
                    ))?;
                ultrasql_wal::dispatch_record_at_lsn(&recovery_apply_target, record, current_lsn)
                    .map_err(|e| ultrasql_wal::RecoveryError::Applier(e.to_string()))
            })
            .map_err(|e| ServerError::Ddl(format!("WAL recovery: {e}")))?;
        wal_buffer.advance_to_lsn(recovered_lsn);
        tracing::info!(lsn = recovered_lsn.raw(), "WAL recovery complete");

        // Seed relation block counts from the durable on-disk heap. WAL replay
        // alone rebuilds these counters, but once low WAL segments are recycled
        // the replayed stream no longer starts at LSN 0, so the early
        // relation-extend records are gone even though the blocks they created
        // are durably present. Without this seeding a post-recycle restart would
        // under-count blocks and heap scans would stop short of durable pages —
        // silently dropping rows and even whole catalog tables. Seeding is
        // monotonic, so it never undoes a larger count WAL replay just set.
        //
        // Only HEAP-scanned relations are seeded: the system/catalog relations
        // here (so the catalog bootstrap below sees every user table), and the
        // user *tables* later once the catalog identifies them. Index relations
        // are rebuilt from the table scan, not their own block count, and seeding
        // a torn/partial index's durable pages would make it wrongly look present.
        let durable_block_counts = if wal_was_recycled {
            page_loader.durable_relation_block_counts().map_err(|e| {
                ServerError::Io(std::io::Error::other(format!(
                    "discover durable relation block counts: {e}"
                )))
            })?
        } else {
            Vec::new()
        };
        for (rel, blocks) in &durable_block_counts {
            if rel.oid().raw() < ultrasql_catalog::FIRST_USER_OID {
                heap.seed_block_count(*rel, *blocks);
            }
        }

        // 5. Background writer thread draining the buffer to disk.
        let wal_writer = WalWriter::open(&wal_dir, Arc::clone(&wal_buffer), wal_writer_config)
            .map_err(|e| ServerError::Io(std::io::Error::other(format!("WAL writer: {e}"))))?;

        // 5a. Install the LSN-gated eviction-relief hook now that the WAL
        // writer exists (the relief's WAL force closure captures its durability
        // handle). This converts a hard `BufferPoolError::Exhausted` on the
        // user-facing heap/index/TOAST read path into a bounded, latch-safe,
        // WAL-correct flush-on-evict relief.
        Server::install_eviction_relief(&pool, &page_loader, Some(&wal_writer));

        let checkpointer_loader = page_loader.clone();
        let checkpointer = Some(ultrasql_storage::Checkpointer::spawn(
            &pool,
            Some(Arc::clone(&sink)),
            Some(Arc::clone(&last_checkpoint_lsn)),
            move |page_id, page| checkpointer_loader.store(page_id, page),
            ultrasql_storage::CheckpointerConfig::default(),
        ));

        // Build the transaction manager and rebuild its commit-status oracle
        // (the CLOG) BEFORE the catalog bootstrap. A naive catalog bootstrap
        // reads the newest heap row per catalog OID regardless of whether the
        // writing `ddl_txn` committed: a crash between a DDL's catalog rows
        // becoming durable (WAL-applied) and its commit marker becoming durable
        // leaves those rows on disk with the `ddl_txn` unresolved, and a raw
        // scan would resurrect the uncommitted table/index/sequence as live
        // schema. To prevent that the bootstrap must scan with a fully
        // reconstructed commit-status oracle and skip rows whose writer did not
        // commit — so the oracle has to exist first.
        let ssi = Arc::new(SsiManager::new());
        let txn_manager = Arc::new(TransactionManager::new_with_ssi(ssi));

        // 2PC recovery seeds prepared (still-InProgress) XIDs into the CLOG
        // before the commit-status rebuild's uncommitted->aborted sweep, so a
        // prepared transaction is not wrongly swept to Aborted. A prepared (not
        // yet committed) DDL is correctly invisible to the bootstrap snapshot.
        let two_phase_dir = data_dir.join("pg_twophase");
        std::fs::create_dir_all(&two_phase_dir).map_err(ServerError::Io)?;
        let two_phase_coord = ultrasql_txn::two_phase::TwoPhaseCoordinator::new(two_phase_dir);
        let recovered_state_files = two_phase_coord
            .recover_from_disk()
            .map_err(|e| ServerError::Ddl(format!("2PC recovery: {e}")))?;
        let mut recovered_prepared = 0usize;
        let mut cleaned_resolved = 0usize;
        for prepared in two_phase_coord.list_prepared() {
            match txn_manager.recover_prepared(prepared.xid) {
                Ok(()) => recovered_prepared += 1,
                Err(TxnError::AlreadyTerminated {
                    status: ultrasql_mvcc::XidStatus::Committed | ultrasql_mvcc::XidStatus::Aborted,
                    ..
                }) => {
                    two_phase_coord.finish_resolution(&prepared).map_err(|e| {
                        ServerError::Ddl(format!("2PC resolved state cleanup: {e}"))
                    })?;
                    cleaned_resolved += 1;
                }
                Err(e) => return Err(ServerError::Ddl(format!("2PC CLOG recovery: {e}"))),
            }
        }
        tracing::info!(
            state_files = recovered_state_files,
            prepared = recovered_prepared,
            cleaned_resolved,
            "2PC state recovery complete"
        );
        let two_phase = Arc::new(two_phase_coord);

        // Restore the commit log from a durable snapshot before scanning the
        // WAL, so transactions whose Commit/Abort records were recycled keep
        // their status. Used only if it decodes cleanly AND its snapshot LSN is
        // within the durable WAL end (a snapshot ahead of the recovered WAL
        // would assert statuses for records that did not survive). The
        // commit-status WAL scan below then runs idempotently on top (retained
        // WAL is authoritative; import seeds only the truncated tail).
        if let Some(bytes) = read_clog_snapshot(data_dir) {
            match decode_clog_snapshot(&bytes) {
                Ok((snapshot_lsn, next_xid, entries)) if snapshot_lsn <= recovered_lsn => {
                    txn_manager.import_clog(next_xid, &entries);
                }
                Ok((snapshot_lsn, _, _)) => tracing::warn!(
                    snapshot_lsn = snapshot_lsn.raw(),
                    recovered_lsn = recovered_lsn.raw(),
                    "commit-log snapshot ahead of recovered WAL; ignoring"
                ),
                Err(e) => tracing::warn!(
                    error = %e,
                    "commit-log snapshot unreadable; full WAL commit-status rebuild"
                ),
            }
        }
        // The authoritative commit-status pass: mark Commit/Abort from the
        // retained WAL, then sweep every observed-but-unresolved XID to Aborted.
        // After this the CLOG reports exactly the committed-as-of-recovery set.
        crate::server_recovery_rebuild::rebuild_commit_status_from_wal(
            txn_manager.as_ref(),
            data_dir,
        )?;

        // Bootstrap snapshot: a "committed-as-of-recovery" snapshot under which
        // NO real transaction is in progress (`xmin == xmax == next_xid`, empty
        // `xip`, `current_xid` a sentinel above every observed XID). Combined
        // with the rebuilt oracle, the visibility predicate reduces — for the
        // strictly append-only catalog heap (every tuple has an INVALID `xmax`)
        // — to "the writer committed". A committed catalog row is therefore
        // never hidden; an aborted / never-committed one is always hidden.
        let bootstrap_xid = txn_manager.next_xid();
        let bootstrap_snapshot = ultrasql_mvcc::Snapshot::new(
            bootstrap_xid,
            bootstrap_xid,
            bootstrap_xid,
            ultrasql_core::CommandId::FIRST,
            std::iter::empty(),
        );

        let persistent_catalog = Arc::new(PersistentCatalog::new());
        let stats =
            require_wal_backed_catalog_bootstrap(persistent_catalog.bootstrap_from_heap_visible(
                heap.as_ref(),
                &bootstrap_snapshot,
                txn_manager.as_ref(),
            ))?;
        tracing::info!(
            ?stats,
            "persistent catalog bootstrapped (WAL-backed, visibility-filtered)"
        );

        let mut catalog = InMemoryCatalog::new();
        let tables = build_sample_database(&mut catalog);
        let plan_cache = Arc::new(PlanCache::new(PlanCacheConfig::default()));

        let server = Self {
            catalog,
            tables,
            data_dir: Some(data_dir.to_path_buf()),
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
            sequences,
            sequence_owners,
            sequence_namespaces,
            schemas,
            operators: Arc::new(dashmap::DashMap::new()),
            materialized_views: Arc::new(dashmap::DashMap::new()),
            regular_views: Arc::new(dashmap::DashMap::new()),
            columnar_storage: Arc::new(columnar_storage::ColumnarSecondaryStore::new()),
            time_partitions: Arc::new(dashmap::DashMap::new()),
            logical_replication: Arc::new(replication::LogicalReplicationRuntime::open_metadata(
                data_dir.join("pg_logical"),
            )?),
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
            standby_apply_lsn: std::sync::atomic::AtomicU64::new(recovered_lsn.raw()),
            checkpointer,
            wal_writer: Some(wal_writer),
            wal_buffer_sink: Some(buffer_sink),
            wal_dir: Some(wal_dir.clone()),
        };
        // The commit-status oracle (durable CLOG import + WAL commit-status
        // rebuild + uncommitted->aborted sweep) was fully reconstructed BEFORE
        // the catalog bootstrap above, against the bare transaction manager, so
        // the bootstrap's visibility filter could skip uncommitted DDL rows. No
        // re-run is needed here.

        // Phase two of durable block-count seeding (see WAL-recovery comment
        // above): now that the catalog knows which relations are user *tables*,
        // seed their durable block counts so post-recycle scans — and the index
        // rebuilds below, which scan the table heap — see every durable page.
        // User indexes are deliberately excluded.
        {
            let snapshot = server.catalog_snapshot();
            for (rel, blocks) in &durable_block_counts {
                if rel.oid().raw() >= ultrasql_catalog::FIRST_USER_OID
                    && snapshot.tables_by_oid.contains_key(&rel.oid())
                {
                    server.heap.seed_block_count(*rel, *blocks);
                }
            }
        }
        server.rebuild_domain_runtime_constraint_sidecars()?;
        server.rebuild_role_metadata()?;
        server.rebuild_privilege_metadata()?;
        server.rebuild_schema_metadata()?;
        server.refresh_persistent_catalog_schema_names();
        server.rebuild_table_runtime_constraint_sidecars()?;
        server.rebuild_persistent_index_sidecars(recovered_lsn)?;
        let stats_catalog = hydrate_optimizer_stats_from_catalog(
            &server.catalog_snapshot(),
            server.heap.as_ref(),
            server.txn_manager.as_ref(),
        );
        *server.stats_catalog.write() = stats_catalog;
        server.rebuild_sequence_owner_metadata()?;
        server.rebuild_operator_metadata()?;
        server.rebuild_row_security_sidecars()?;
        server.rebuild_materialized_view_runtime_sidecars()?;
        server.rebuild_regular_view_runtime_sidecars()?;
        server.rebuild_time_partition_runtime_sidecars()?;
        Ok(server)
    }

    pub(crate) fn domain_runtime_metadata_path(&self) -> Option<std::path::PathBuf> {
        self.data_dir
            .as_ref()
            .map(|dir| dir.join("pg_domain_runtime.meta"))
    }
}
