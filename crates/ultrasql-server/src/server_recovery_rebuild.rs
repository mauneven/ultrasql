//! `impl Server` methods (split out of the crate root): recovery_rebuild.
//!
//! Pure code motion from `lib.rs`; behavior unchanged.
use super::*;

impl Server {
    pub(crate) fn persist_regular_view_runtime_metadata(
        &self,
        runtime: &RegularViewRuntime,
    ) -> Result<(), ServerError> {
        if self.regular_view_metadata_path().is_none() {
            return Ok(());
        }
        let Some(view_entry) = self.persistent_catalog.lookup_table(&runtime.view_table) else {
            return Ok(());
        };
        let mut records = self.load_regular_view_metadata()?;
        records.retain(|record| {
            record.view_table != runtime.view_table && record.view_oid != view_entry.oid
        });
        records.push(RegularViewMetadataRecord {
            view_table: runtime.view_table.clone(),
            view_oid: view_entry.oid,
            source_sql: runtime.source_sql.clone(),
            search_path: runtime.search_path.clone(),
        });
        self.write_regular_view_metadata(&records)
    }

    pub(crate) fn ensure_regular_view_runtime_metadata_slots_persistable(
        &self,
    ) -> Result<(), ServerError> {
        ensure_optional_runtime_metadata_write_slots(self.regular_view_metadata_path())
    }

    pub(crate) fn remove_regular_view_runtime_metadata(
        &self,
        dropped_tables: &[String],
    ) -> Result<(), ServerError> {
        if dropped_tables.is_empty() {
            return Ok(());
        }
        let mut records = self.load_regular_view_metadata()?;
        let before = records.len();
        records.retain(|record| {
            !dropped_tables
                .iter()
                .any(|table| record.view_table.eq_ignore_ascii_case(table))
        });
        if records.len() != before {
            self.write_regular_view_metadata(&records)?;
        }
        Ok(())
    }

    pub(crate) fn rebuild_regular_view_runtime_sidecars(&self) -> Result<(), ServerError> {
        self.regular_views.clear();
        let catalog_snapshot = self.catalog_snapshot();
        for record in self.load_regular_view_metadata()? {
            let view_entry = self
                .persistent_catalog
                .lookup_table(&record.view_table)
                .ok_or_else(|| {
                    ServerError::Ddl(format!("invalid view metadata for '{}'", record.view_table))
                })?;
            if view_entry.oid != record.view_oid || !is_regular_view_entry(&view_entry) {
                return Err(ServerError::Ddl(format!(
                    "invalid view metadata for '{}'",
                    record.view_table
                )));
            }
            let combined = CombinedCatalog {
                snapshot: &catalog_snapshot,
                fallback: &self.catalog,
                search_path: record.search_path.as_deref(),
            };
            let source = bind_regular_view_source_sql(&record.source_sql, &combined)?;
            if !view_source_shape_matches(source.schema(), &view_entry.schema) {
                return Err(ServerError::Ddl(format!(
                    "view metadata for '{}' no longer matches catalog schema",
                    record.view_table
                )));
            }
            self.regular_views.insert(
                record.view_table.clone(),
                Arc::new(RegularViewRuntime {
                    view_table: record.view_table,
                    source_sql: record.source_sql,
                    search_path: record.search_path,
                    source,
                    columns: view_entry.schema,
                }),
            );
        }
        Ok(())
    }

    pub(crate) fn rebuild_time_partition_runtime_sidecars(&self) -> Result<(), ServerError> {
        self.time_partitions.clear();
        let snapshot = self.catalog_snapshot();
        let mut parents = Vec::new();
        let mut chunks = Vec::new();
        for (key, entry) in &snapshot.tables {
            if let Some(options) =
                time_partition::parent_options_from_entry(entry).map_err(ServerError::Ddl)?
            {
                parents.push((entry.clone(), options));
            }
            if let Some(options) =
                time_partition::chunk_options_from_entry(entry).map_err(ServerError::Ddl)?
            {
                chunks.push((key.clone(), entry.clone(), options));
            }
        }
        parents.sort_by_key(|(entry, _)| entry.oid.raw());
        chunks.sort_by_key(|(_, entry, _)| entry.oid.raw());

        for (entry, options) in parents {
            let partition_column_index = entry
                .schema
                .fields()
                .iter()
                .position(|field| field.name.eq_ignore_ascii_case(&options.column))
                .ok_or_else(|| {
                    ServerError::Ddl(format!(
                        "time partition table '{}' references missing column '{}'",
                        entry.name, options.column
                    ))
                })?;
            let partition_column = entry.schema.field(partition_column_index).ok_or_else(|| {
                ServerError::Ddl(format!(
                    "time partition table '{}' column index is invalid",
                    entry.name
                ))
            })?;
            match &partition_column.data_type {
                DataType::Timestamp | DataType::TimestampTz => {}
                other => {
                    return Err(ServerError::Ddl(format!(
                        "time partition table '{}' column '{}' has unsupported type {other}",
                        entry.name, partition_column.name
                    )));
                }
            }

            let mut runtime = time_partition::TimePartitionRuntime::daily(
                entry.schema_name.clone(),
                entry.name.clone(),
                entry.oid,
                entry.schema.clone(),
                partition_column.name.clone(),
                partition_column_index,
            );
            runtime.chunk_interval_us = options.interval_us;
            for (chunk_key, chunk_entry, chunk_options) in &chunks {
                if chunk_options.parent_oid != entry.oid {
                    continue;
                }
                if chunk_entry.schema.len() != entry.schema.len() {
                    return Err(ServerError::Ddl(format!(
                        "time partition chunk '{}' has schema width {} but parent '{}' has width {}",
                        chunk_entry.name,
                        chunk_entry.schema.len(),
                        entry.name,
                        entry.schema.len()
                    )));
                }
                runtime.chunks.insert(
                    chunk_options.start_us,
                    time_partition::TimeChunkRuntime {
                        start_us: chunk_options.start_us,
                        end_us: chunk_options.end_us,
                        table_name: chunk_key.clone(),
                        oid: chunk_entry.oid,
                    },
                );
            }
            self.time_partitions
                .insert(table_entry_lookup_key(&entry), Arc::new(runtime));
        }
        Ok(())
    }

    pub(crate) fn rebuild_persistent_index_sidecars(
        &self,
        recovered_lsn: Lsn,
    ) -> Result<(), ServerError> {
        let snapshot = self.catalog_snapshot();
        let mut hnsw_indexes = Vec::new();
        let mut ivfflat_indexes = Vec::new();

        for (table_oid, indexes) in &snapshot.indexes_by_table {
            let Some(table) = snapshot.tables_by_oid.get(table_oid) else {
                continue;
            };
            let mut constraints = self
                .table_constraints
                .get(table_oid)
                .map(|entry| entry.value().as_ref().clone())
                .unwrap_or_default();
            let mut changed = false;

            for index in indexes {
                let method = logical_index_method_from_name(&index.access_method);
                match method {
                    LogicalIndexMethod::Btree | LogicalIndexMethod::Hash => {
                        let rows = self.rebuild_btree_index_pages(table, index, method)?;
                        tracing::info!(
                            table = %table.name,
                            index = %index.name,
                            rows,
                            "rebuilt persistent btree index pages"
                        );
                    }
                    LogicalIndexMethod::Brin => {
                        let (brin, rows) = self.rebuild_brin_summary(table, index)?;
                        constraints.indexes.insert(
                            index.oid,
                            RuntimeIndexMetadata {
                                key_exprs: Vec::new(),
                                predicate: None,
                                include_columns: Vec::new(),
                                method,
                                brin: Some(brin),
                                hnsw: None,
                                ivfflat: None,
                                aggregating: None,
                            },
                        );
                        changed = true;
                        tracing::info!(
                            table = %table.name,
                            index = %index.name,
                            rows,
                            "rebuilt persistent brin summaries"
                        );
                    }
                    LogicalIndexMethod::Hnsw => {
                        let [attnum] = index.columns.as_slice() else {
                            continue;
                        };
                        let col = usize::from(*attnum);
                        let Some(field) = table.schema.field(col) else {
                            continue;
                        };
                        let Some((dims, default_payload)) =
                            ann_dims_and_default_payload(&field.data_type)
                        else {
                            continue;
                        };
                        let metric = hnsw_metric_for_opclass_name(
                            index.opclasses.first().and_then(Option::as_deref),
                        )?;
                        let payload = ann_payload_option_from_catalog(&index.options)?
                            .unwrap_or(default_payload);
                        let rel = RelationId::new(index.oid.raw());
                        // Prefer a durable snapshot, replaying only the WAL
                        // records above its meta.lsn high-water mark instead of
                        // rebuilding the whole graph (an O(N^2) insert sweep).
                        // The snapshot is trusted ONLY if from_snapshot_bytes
                        // accepts it (crc32c + format + relation), its dims and
                        // metric match this catalog index, AND its meta.lsn does
                        // not exceed the durable WAL end (recovered_lsn): a
                        // snapshot carrying ops beyond the durable WAL would
                        // diverge from the heap on restart, so it is rejected and
                        // we fall back to a full replay. Every failure is safe —
                        // the WAL remains the source of truth.
                        let snapshot_loaded = self
                            .data_dir
                            .as_ref()
                            .and_then(|dd| read_vector_snapshot(dd, index.oid))
                            .and_then(|bytes| {
                                PageBackedHnswIndex::from_snapshot_bytes(rel, &bytes).ok()
                            })
                            .filter(|idx| {
                                usize::try_from(dims).is_ok_and(|d| idx.dims() == d)
                                    && idx.metric() == metric
                                    && idx.snapshot_lsn() <= recovered_lsn
                            });
                        let hnsw = Arc::new(match snapshot_loaded {
                            Some(idx) => idx,
                            None => PageBackedHnswIndex::new_with_payload_kind(
                                rel, dims, metric, 16, 64, payload,
                            )
                            .map_err(|e| {
                                ServerError::ddl(format!(
                                    "rebuild HNSW {} from catalog: {e}",
                                    index.name
                                ))
                            })?,
                        });
                        hnsw_indexes.push(Arc::clone(&hnsw));
                        constraints.indexes.insert(
                            index.oid,
                            RuntimeIndexMetadata {
                                key_exprs: Vec::new(),
                                predicate: None,
                                include_columns: Vec::new(),
                                method,
                                brin: None,
                                hnsw: Some(hnsw),
                                ivfflat: None,
                                aggregating: None,
                            },
                        );
                        changed = true;
                    }
                    LogicalIndexMethod::IvfFlat => {
                        let [attnum] = index.columns.as_slice() else {
                            continue;
                        };
                        let col = usize::from(*attnum);
                        let Some(field) = table.schema.field(col) else {
                            continue;
                        };
                        let Some((dims, default_payload)) =
                            ann_dims_and_default_payload(&field.data_type)
                        else {
                            continue;
                        };
                        let metric = hnsw_metric_for_opclass_name(
                            index.opclasses.first().and_then(Option::as_deref),
                        )?;
                        let (lists, probes, payload) =
                            ivfflat_options_from_catalog(&index.options)?;
                        let payload = payload.unwrap_or(default_payload);
                        let rel = RelationId::new(index.oid.raw());
                        // Prefer a durable snapshot, replaying only the WAL above
                        // its meta.lsn high-water mark instead of rebuilding the
                        // lists from a full from-zero replay. Trusted ONLY if
                        // from_snapshot_bytes accepts it (crc32c + format +
                        // relation), its dims and metric match this catalog index,
                        // AND its meta.lsn does not exceed the durable WAL end —
                        // identical contract to the HNSW path above.
                        let snapshot_loaded = self
                            .data_dir
                            .as_ref()
                            .and_then(|dd| read_vector_snapshot(dd, index.oid))
                            .and_then(|bytes| {
                                PageBackedIvfFlatIndex::from_snapshot_bytes(rel, &bytes).ok()
                            })
                            .filter(|idx| {
                                usize::try_from(dims).is_ok_and(|d| idx.dims() == d)
                                    && idx.metric() == metric
                                    && idx.snapshot_lsn() <= recovered_lsn
                            });
                        let ivfflat = Arc::new(match snapshot_loaded {
                            Some(idx) => idx,
                            None => PageBackedIvfFlatIndex::new_with_payload_kind(
                                rel, dims, metric, lists, probes, payload,
                            )
                            .map_err(|e| {
                                ServerError::ddl(format!(
                                    "rebuild IVFFlat {} from catalog: {e}",
                                    index.name
                                ))
                            })?,
                        });
                        ivfflat_indexes.push(Arc::clone(&ivfflat));
                        constraints.indexes.insert(
                            index.oid,
                            RuntimeIndexMetadata {
                                key_exprs: Vec::new(),
                                predicate: None,
                                include_columns: Vec::new(),
                                method,
                                brin: None,
                                hnsw: None,
                                ivfflat: Some(ivfflat),
                                aggregating: None,
                            },
                        );
                        changed = true;
                    }
                    LogicalIndexMethod::Aggregating => {
                        let Some(spec) =
                            crate::aggregating_index::aggregating_index_spec_from_catalog(
                                table, index,
                            )?
                        else {
                            continue;
                        };
                        let rows = self.rebuild_aggregating_index_rows(table, &spec)?;
                        constraints.indexes.insert(
                            index.oid,
                            RuntimeIndexMetadata {
                                key_exprs: aggregating_group_key_exprs(table, &spec)?,
                                predicate: None,
                                include_columns: Vec::new(),
                                method,
                                brin: None,
                                hnsw: None,
                                ivfflat: None,
                                aggregating: Some(Arc::new(RuntimeAggregatingIndex::new(
                                    spec, rows,
                                ))),
                            },
                        );
                        changed = true;
                    }
                    _ => {}
                }
            }

            if changed {
                self.table_constraints
                    .insert(*table_oid, Arc::new(constraints));
            }
        }

        self.replay_vector_index_wal_into(&hnsw_indexes, &ivfflat_indexes)
    }

    pub(crate) fn rebuild_btree_index_pages(
        &self,
        table: &TableEntry,
        index: &IndexEntry,
        method: LogicalIndexMethod,
    ) -> Result<u64, ServerError> {
        if index.root_block == BlockNumber::INVALID {
            return Ok(0);
        }
        let columns: Vec<usize> = index
            .columns
            .iter()
            .map(|attnum| usize::from(*attnum))
            .collect();
        let runtime_metadata = self
            .table_constraints
            .get(&table.oid)
            .and_then(|constraints| constraints.indexes.get(&index.oid).cloned());
        let expression_key_exprs = runtime_metadata
            .as_ref()
            .map_or_else(Vec::new, |metadata| metadata.key_exprs.clone());
        let predicate = runtime_metadata
            .as_ref()
            .and_then(|metadata| metadata.predicate.clone());
        if columns.is_empty() && expression_key_exprs.is_empty() {
            return Ok(0);
        }
        let encoding = if method == LogicalIndexMethod::Hash {
            crate::index_key::IndexKeyEncoding::Int64
        } else if columns.is_empty() && expression_key_exprs.len() == 1 {
            crate::index_key::IndexKeyEncoding::for_data_type(&expression_key_exprs[0].data_type())?
        } else {
            crate::index_key::IndexKeyEncoding::for_columns(&table.schema, &columns)?
        };
        let key_col_idx = columns.first().copied();
        let index_rel = RelationId::new(index.oid.raw());
        let mut btree = BTree::create(Arc::clone(self.heap.buffer_pool()), index_rel)
            .map_err(|e| ServerError::ddl(format!("restart rebuild {}: {e}", index.name)))?;
        let txn = self.txn_manager.begin(IsolationLevel::ReadCommitted);
        let table_rel = RelationId(table.oid);
        let block_count = self.heap.block_count(table_rel).max(table.n_blocks);
        let scan = self.heap.scan_visible(
            table_rel,
            block_count,
            &txn.snapshot,
            self.txn_manager.as_ref(),
        );
        let result = (|| -> Result<u64, ServerError> {
            let mut inserted = 0_u64;
            for tuple in scan {
                let tuple = tuple.map_err(|e| {
                    ServerError::ddl(format!(
                        "restart rebuild {} heap scan failed: {e}",
                        index.name
                    ))
                })?;
                let Some(key) = decode_key_column(
                    &tuple.data,
                    &table.schema,
                    key_col_idx,
                    &expression_key_exprs,
                    predicate.as_ref(),
                    method,
                    &encoding,
                )?
                else {
                    continue;
                };
                if index.is_unique {
                    btree.insert(key, tuple.tid, txn.xid, None).map_err(|e| {
                        ServerError::ddl(format!("restart rebuild {}: {e}", index.name))
                    })?;
                } else {
                    btree
                        .insert_non_unique(key, tuple.tid, txn.xid, None)
                        .map_err(|e| {
                            ServerError::ddl(format!("restart rebuild {}: {e}", index.name))
                        })?;
                }
                inserted = inserted.saturating_add(1);
            }
            Ok(inserted)
        })();
        self.finalise_restart_rebuild_transaction(
            txn,
            result,
            "restart btree rebuild transaction commit",
            "restart btree rebuild transaction rollback",
        )
    }

    /// Repopulate the persistent btree/hash index pages of every surviving
    /// plain-column index of `table` from the heap, after an `ALTER TABLE ...
    /// DROP COLUMN` has rewritten that heap in place.
    ///
    /// DROP COLUMN re-encodes every visible tuple to the narrower row via a
    /// direct `heap.update`, which re-stamps each surviving row as a new tuple
    /// version (the pre-drop version is left dead). The existing index leaves
    /// still point at those now-dead pre-images, so a UNIQUE / PRIMARY KEY index
    /// silently stops enforcing 23505: the duplicate-key recheck fetches the
    /// dead pre-image, classifies the slot as free, and lets a duplicate land.
    /// That duplicate then aborts the next restart's index rebuild
    /// (`restart rebuild <idx>: duplicate key in index`) and the server fails to
    /// boot. Re-running exactly the startup btree rebuild here — but scoped to
    /// this one table and its freshly committed heap — rebuilds every leaf to
    /// point at the live tuple versions, so enforcement resumes immediately and
    /// no duplicate can ever land. Must be called AFTER the rewrite has
    /// committed and the in-memory catalog survivors have been re-pointed
    /// ([`Self::rebuild_btree_index_pages`] reads the post-drop schema/columns).
    ///
    /// Expression / partial indexes are intentionally skipped: their runtime
    /// key / predicate metadata still references the pre-drop column positions
    /// (re-indexing that metadata is a separate concern), so evaluating them
    /// against the rewritten rows here could key on the wrong slot. They are
    /// rebuilt from re-bound metadata at the next restart.
    pub(crate) fn repopulate_table_btree_indexes_after_drop_column(&self, table: &TableEntry) {
        let snapshot = self.catalog_snapshot();
        let Some(indexes) = snapshot.indexes_by_table.get(&table.oid) else {
            return;
        };
        for index in indexes {
            let method = logical_index_method_from_name(&index.access_method);
            if !matches!(method, LogicalIndexMethod::Btree | LogicalIndexMethod::Hash) {
                continue;
            }
            // Expression / partial index runtime metadata is not column-shifted
            // by DROP COLUMN, so a rebuild here would evaluate stale positions;
            // leave it to the restart rebuild, which re-binds against the schema.
            let runtime = self
                .table_constraints
                .get(&table.oid)
                .and_then(|constraints| constraints.indexes.get(&index.oid).cloned());
            if runtime.as_ref().is_some_and(|metadata| {
                !metadata.key_exprs.is_empty() || metadata.predicate.is_some()
            }) {
                tracing::warn!(
                    table = %table.name,
                    index = %index.name,
                    "ALTER TABLE DROP COLUMN: skipping in-place rebuild of expression/partial \
                     index; it will be rebuilt at the next restart"
                );
                continue;
            }
            match self.rebuild_btree_index_pages(table, index, method) {
                Ok(rows) => tracing::info!(
                    table = %table.name,
                    index = %index.name,
                    rows,
                    unique = index.is_unique,
                    "ALTER TABLE DROP COLUMN: repopulated index from rewritten heap"
                ),
                Err(e) => tracing::error!(
                    error = %e,
                    table = %table.name,
                    index = %index.name,
                    "ALTER TABLE DROP COLUMN: repopulating index after column drop failed; \
                     the heap is authoritative and a restart rebuilds it"
                ),
            }
        }
    }

    pub(crate) fn rebuild_brin_summary(
        &self,
        table: &TableEntry,
        index: &IndexEntry,
    ) -> Result<(Arc<BrinIndex>, u64), ServerError> {
        if index.columns.is_empty() {
            return Ok((Arc::new(BrinIndex::new(128)), 0));
        }
        let columns: Vec<usize> = index
            .columns
            .iter()
            .map(|attnum| usize::from(*attnum))
            .collect();
        let encoding = crate::index_key::IndexKeyEncoding::for_columns(&table.schema, &columns)?;
        let key_col_idx = columns.first().copied();
        let brin = Arc::new(BrinIndex::new(128));
        let txn = self.txn_manager.begin(IsolationLevel::ReadCommitted);
        let table_rel = RelationId(table.oid);
        let block_count = self.heap.block_count(table_rel).max(table.n_blocks);
        let scan = self.heap.scan_visible(
            table_rel,
            block_count,
            &txn.snapshot,
            self.txn_manager.as_ref(),
        );
        let result = (|| -> Result<u64, ServerError> {
            let mut inserted = 0_u64;
            for tuple in scan {
                let tuple = tuple.map_err(|e| {
                    ServerError::ddl(format!(
                        "restart rebuild {} BRIN heap scan failed: {e}",
                        index.name
                    ))
                })?;
                let Some(key) = decode_key_column(
                    &tuple.data,
                    &table.schema,
                    key_col_idx,
                    &[],
                    None,
                    LogicalIndexMethod::Brin,
                    &encoding,
                )?
                else {
                    continue;
                };
                let brin_key = BrinIndex::encode_i64_key(key);
                brin.insert(&brin_key, tuple.tid).map_err(|e| {
                    ServerError::ddl(format!("restart rebuild {} BRIN: {e}", index.name))
                })?;
                inserted = inserted.saturating_add(1);
            }
            Ok(inserted)
        })();
        let rows = self.finalise_restart_rebuild_transaction(
            txn,
            result,
            "restart brin rebuild transaction commit",
            "restart brin rebuild transaction rollback",
        )?;
        Ok((brin, rows))
    }
}

/// Rebuild the commit-status oracle (CLOG) of `txn_manager` by scanning the WAL
/// under `data_dir`.
///
/// This is the authoritative crash-recovery pass: it marks every WAL-observed
/// transaction Committed / Aborted from its terminal record, then sweeps every
/// observed-but-unresolved XID to Aborted. It depends on **nothing** but the
/// transaction manager and the data directory, so it can run against a bare
/// [`TransactionManager`] **before** the `Server` and its catalog are
/// assembled — which is required so the catalog bootstrap can scan the heap
/// with a fully-populated commit-status oracle and skip uncommitted DDL rows.
///
/// Idempotent: re-running it (e.g. the `&self` wrapper after `Server`
/// construction) only re-asserts the same terminal statuses.
pub(crate) fn rebuild_commit_status_from_wal(
    txn_manager: &TransactionManager,
    data_dir: &Path,
) -> Result<(), ServerError> {
    let wal_dir = data_dir.join("pg_wal");
    let recovery_replay_target = recovery_replay_target_from_data_dir(data_dir)?;
    let mut observed_xids = std::collections::BTreeSet::new();
    ultrasql_wal::recover_with_target(&wal_dir, recovery_replay_target, |record| {
        observed_xids.insert(record.header.xid);
        update_commit_status_for_record(txn_manager, record);
        Ok(())
    })
    .map_err(|e| ServerError::ddl(format!("recover commit status: {e}")))?;
    for xid in observed_xids {
        txn_manager.recover_uncommitted_as_aborted(xid);
    }
    Ok(())
}

/// Apply one WAL record's effect on the commit-status oracle: mark the record's
/// XID observed (advancing the allocator floor), and on a terminal
/// Commit/Abort record flip the XID — and, for a commit, every subxid released
/// atomically with it — to its final status.
///
/// This is the per-record half of commit-status recovery, shared by the
/// full-WAL crash rebuild ([`rebuild_commit_status_from_wal`]) and incremental
/// hot-standby apply ([`apply_wal_range_with_commit_status`]) so the two paths
/// cannot drift.
///
/// It deliberately performs **no** default-abort sweep. An XID observed but not
/// yet terminal stays [`ultrasql_mvcc::XidStatus::InProgress`]. The crash
/// rebuild adds the sweep after a *complete* scan (every XID's fate is known);
/// incremental standby apply must not, because a commit for an in-progress XID
/// may still arrive in a later WAL record that has not been applied yet —
/// aborting it now would permanently hide rows it will legitimately commit.
fn update_commit_status_for_record(txn_manager: &TransactionManager, record: &WalRecord) {
    txn_manager.recover_observed_xid(record.header.xid);
    match record.header.record_type {
        RecordType::Commit => {
            txn_manager.recover_committed(record.header.xid);
            // The parent's single Commit record carries the subxids that
            // committed atomically with it (released + implicitly
            // released-at-commit savepoints). Mark each Committed so a row
            // inserted under a released savepoint keeps a committed xmin.
            if let Ok(payload) = CommitPayload::decode(&record.payload) {
                for subxid in payload.committed_subxids {
                    txn_manager.recover_committed(subxid);
                }
            }
        }
        RecordType::Abort => txn_manager.recover_aborted(record.header.xid),
        _ => {}
    }
}

/// Incrementally apply a bounded WAL range `[from_lsn, to_lsn)` on a hot
/// standby: replay heap and sequence changes into `heap_target` and advance
/// transaction commit status in `txn_manager`, in a single pass. Returns the
/// next unapplied LSN (the cursor to resume from on the next call).
///
/// Unlike crash recovery this performs **no** default-abort sweep (see
/// [`update_commit_status_for_record`]); commit status converges as later
/// ranges are applied. Heap replay is idempotent under the page-LSN rule and
/// commit-status restoration is idempotent, so re-applying an already-applied
/// prefix is harmless.
pub(crate) fn apply_wal_range_with_commit_status(
    txn_manager: &TransactionManager,
    heap_target: &dyn HeapTarget,
    wal_dir: &Path,
    from_lsn: Lsn,
    to_lsn: Lsn,
) -> Result<Lsn, ServerError> {
    let stream = ultrasql_wal::reader::read_wal_range(wal_dir, from_lsn, to_lsn)
        .map_err(|e| ServerError::ddl(format!("standby apply: read WAL range: {e}")))?;
    for record in &stream.records {
        let (decoded, _used) = WalRecord::decode(&record.bytes)
            .map_err(|e| ServerError::ddl(format!("standby apply: decode record: {e}")))?;
        ultrasql_wal::dispatch_record_at_lsn(heap_target, &decoded, record.lsn)
            .map_err(|e| ServerError::ddl(format!("standby apply: dispatch record: {e}")))?;
        update_commit_status_for_record(txn_manager, &decoded);
    }
    Ok(stream.next_lsn)
}

impl Server {
    /// Apply landed-but-unapplied WAL up to `up_to` on a hot standby, replaying
    /// heap/sequence changes and transaction commit status so this standby's
    /// snapshots observe freshly-streamed commits, and advancing the apply
    /// cursor. Returns the new cursor (the next unapplied LSN).
    ///
    /// A no-op when `up_to` is at or behind the cursor. Idempotent under the
    /// WAL page-LSN and commit-status recovery rules, so a redundant call over
    /// an already-applied range is safe.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] if the server has no persistent WAL directory
    /// (an in-memory instance) or if reading/replaying the WAL range fails.
    pub fn apply_landed_wal(&self, up_to: Lsn) -> Result<Lsn, ServerError> {
        let wal_dir = self.wal_dir.as_ref().ok_or_else(|| {
            ServerError::ddl("apply_landed_wal requires a persistent WAL directory")
        })?;
        let from_lsn = Lsn::new(
            self.standby_apply_lsn
                .load(std::sync::atomic::Ordering::Acquire),
        );
        if up_to.raw() <= from_lsn.raw() {
            return Ok(from_lsn);
        }
        let target = ServerRecoveryTarget {
            heap: Arc::clone(&self.heap),
            sequences: Arc::clone(&self.sequences),
        };
        let next_lsn = apply_wal_range_with_commit_status(
            self.txn_manager.as_ref(),
            &target,
            wal_dir,
            from_lsn,
            up_to,
        )?;
        self.standby_apply_lsn
            .store(next_lsn.raw(), std::sync::atomic::Ordering::Release);
        Ok(next_lsn)
    }
}

#[cfg(test)]
mod standby_apply_tests {
    use super::*;
    use ultrasql_mvcc::{XidStatus, XidStatusOracle};

    /// Incremental commit-status apply marks commits (and their released
    /// subxids) terminal, but leaves an observed-yet-uncommitted XID
    /// `InProgress` — the crash-recovery default-abort sweep must NOT run
    /// incrementally, or a commit arriving in a later range would be lost.
    #[test]
    fn incremental_apply_marks_commits_without_aborting_open_xids() {
        let txn_manager = TransactionManager::new();

        // An INSERT by xid 10 whose Commit record has not been applied yet.
        let insert = WalRecord::new(
            RecordType::HeapInsert,
            Xid::new(10),
            Lsn::ZERO,
            0,
            Vec::new(),
        )
        .expect("build insert record");
        update_commit_status_for_record(&txn_manager, &insert);

        // A committed transaction (xid 11) that released a subxid (12).
        let commit_payload = CommitPayload {
            commit_lsn: Lsn::new(64),
            commit_timestamp_micros: 0,
            committed_subxids: vec![Xid::new(12)],
        }
        .encode()
        .expect("encode commit payload");
        let commit = WalRecord::new(
            RecordType::Commit,
            Xid::new(11),
            Lsn::ZERO,
            0,
            commit_payload,
        )
        .expect("build commit record");
        update_commit_status_for_record(&txn_manager, &commit);

        assert_eq!(txn_manager.status(Xid::new(11)), XidStatus::Committed);
        assert_eq!(txn_manager.status(Xid::new(12)), XidStatus::Committed);
        // The open xid is still resolvable by a later commit — not aborted.
        assert_eq!(txn_manager.status(Xid::new(10)), XidStatus::InProgress);
    }
}
