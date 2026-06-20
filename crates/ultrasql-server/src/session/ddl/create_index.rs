//! `CREATE INDEX` DDL handler. Part of the `session::ddl` module split;
//! reopens the `impl<RW> Session<RW>` block defined in `session/mod.rs`.
//!
//! `execute_create_index` is a single, indivisible handler that exceeds
//! the 700-line target; it is kept whole here per the split policy.

use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncWrite};
use ultrasql_catalog::{CatalogSnapshot, IndexEntry, MutableCatalog};
use ultrasql_core::{RelationId, Value};
use ultrasql_planner::{LogicalIndexMethod, LogicalPlan};
use ultrasql_storage::access_method::{
    AccessMethod, BrinIndex, PageBackedHnswIndex, PageBackedIvfFlatIndex,
};
use ultrasql_storage::btree::BTree;
use ultrasql_txn::IsolationLevel;

use super::super::Session;
use super::index_options::{
    ann_dims_and_default_payload, hnsw_metric_for_opclass, hnsw_payload_option,
    index_options_as_pairs, ivfflat_options, logical_index_method_name,
};
use super::{CreateIndexProgressGuard, log_failed_ddl_rollback};
use crate::decode_key_column;
use crate::error::ServerError;
use crate::result_encoder::{SelectResult, run_ddl_command};

impl<RW> Session<RW>
where
    RW: AsyncRead + AsyncWrite + Unpin,
{
    /// Build a B+ tree index over the supplied table and register it
    /// in `pg_index`.
    ///
    /// The kernel work is split into four steps:
    ///
    /// 1. Validate the request against the current catalog snapshot —
    ///    `IF NOT EXISTS`, presence of the parent table, and key-column
    ///    type compatibility with the B-tree (the v0.5 tree stores
    ///    fixed-size 8-byte keys, so every supported column type is
    ///    mapped into an `i64` by the
    ///    [`crate::index_key::IndexKeyEncoding`] this method picks).
    /// 2. Allocate a fresh OID for the index and instantiate a new
    ///    [`BTree`] over a relation id derived from that OID. The
    ///    buffer pool's blank-page loader hands out empty heap pages
    ///    which `BTree::create` then initialises as B-tree leaves.
    /// 3. Scan every visible row of the parent table under an
    ///    autocommit snapshot, decode the key column(s), and call
    ///    [`BTree::insert`] with the row's [`ultrasql_core::TupleId`].
    /// 4. Build an [`IndexEntry`] carrying the root block plus the
    ///    requested attnums, register it with the persistent catalog,
    ///    and let the catalog's snapshot rotation publish the entry to
    ///    subsequent statements.
    ///
    /// # Supported key shapes
    ///
    /// - Single column of `Int16`, `Int32`, `Int64`, `Bool`,
    ///   `Timestamp`, `TimestampTz`, `Float32`, `Float64`, or `Text`.
    ///   See [`crate::index_key::IndexKeyEncoding`] for the per-type
    ///   mapping. `Text` columns are truncated to their first 8 UTF-8
    ///   bytes; collisions are resolved by a heap-side recheck during
    ///   index probes.
    /// - Two columns of `Bool` / `Int16` / `Int32` packed into a single
    ///   `i64` (`hi << 32 | lo`). Composite probes are recheck-filtered
    ///   to drop bit-pattern collisions.
    /// - Indexes over three or more columns, over wider integer halves,
    ///   and over float / text composites still return
    ///   [`ServerError::Unsupported`] — they require a `Vec<u8>`-keyed
    ///   B-tree, scheduled for the v0.7 wave.
    ///
    /// # Other gaps
    ///
    /// - `UNIQUE` is honoured at the catalog level — the
    ///   [`IndexEntry::is_unique`] flag is propagated — but the
    ///   B-tree's existing duplicate-key rejection is the only
    ///   enforcement. Non-unique indexes that happen to have unique
    ///   data still build correctly; non-unique indexes with
    ///   duplicates would error here, which is a known limitation we
    ///   accept until the B-tree gains a non-unique mode.
    pub(crate) fn execute_create_index(
        &self,
        plan: &LogicalPlan,
        snapshot: &CatalogSnapshot,
    ) -> Result<SelectResult, ServerError> {
        let LogicalPlan::CreateIndex {
            index_name,
            index_namespace,
            table_name,
            columns,
            key_exprs,
            opclasses,
            index_options,
            include_columns,
            predicate,
            method,
            aggregating,
            unique,
            primary_key,
            if_not_exists,
            ..
        } = plan
        else {
            return Err(ServerError::Unsupported(
                "execute_create_index called with non-CreateIndex plan",
            ));
        };

        // 1a. IF NOT EXISTS short-circuit.
        let index_key = ultrasql_catalog::index_lookup_key(index_namespace, index_name);
        if snapshot.indexes.contains_key(&index_key) {
            if *if_not_exists {
                return Ok(run_ddl_command("CREATE INDEX"));
            }
            return Err(ServerError::Catalog(
                ultrasql_catalog::CatalogError::already_exists(index_key),
            ));
        }

        // 1b. Resolve the parent table.
        let table = snapshot.tables.get(table_name).ok_or_else(|| {
            ServerError::Plan(ultrasql_planner::PlanError::TableNotFound(
                table_name.clone(),
            ))
        })?;
        self.ensure_table_owner_or_superuser(table.oid, table_name)?;

        if *method == LogicalIndexMethod::Aggregating {
            if *unique {
                return Err(ServerError::Unsupported(
                    "CREATE UNIQUE AGGREGATING INDEX is not supported",
                ));
            }
            let Some(spec) = aggregating.clone() else {
                return Err(ServerError::ddl(
                    "CREATE AGGREGATING INDEX missing aggregating metadata",
                ));
            };
            let index_oid = self.state.persistent_catalog.next_oid();
            self.state
                .ensure_table_runtime_constraints_metadata_slots_persistable()?;
            let block_count = self
                .state
                .heap
                .block_count(RelationId(table.oid))
                .max(table.n_blocks);
            let progress = CreateIndexProgressGuard::new(
                self.state.workload_recorder.as_ref(),
                self.pid,
                table.oid.raw(),
                index_oid.raw(),
                block_count,
            );
            progress.update("building index", 0);
            let txn = self.state.txn_manager.begin(IsolationLevel::ReadCommitted);
            let build_result = crate::aggregating_index::build_aggregating_index_rows(
                table,
                &spec,
                self.state.heap.as_ref(),
                &txn.snapshot,
                self.state.txn_manager.as_ref(),
            );
            self.state
                .commit_transaction(txn, false, "CREATE AGGREGATING INDEX scan")?;
            let rows = build_result?;
            progress.update("writing catalog", block_count);
            let attnums = columns
                .iter()
                .map(|col| {
                    u16::try_from(*col).map_err(|_| {
                        ServerError::Unsupported(
                            "CREATE AGGREGATING INDEX: column index does not fit in u16 attnum field",
                        )
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            let entry = IndexEntry::new(index_oid, index_name.clone(), table.oid, attnums, false)
                .with_schema_name(index_namespace.clone())
                .with_access_method("aggregating", vec![None; spec.group_columns.len()])
                .with_options(
                    crate::aggregating_index::catalog_options_for_aggregating_index(
                        &spec, table.oid, index_oid,
                    ),
                );
            self.state.persistent_catalog.create_index(entry.clone())?;
            let ddl_txn = self
                .state
                .txn_manager
                .begin(ultrasql_txn::IsolationLevel::ReadCommitted);
            if let Err(e) = self.state.persistent_catalog.persist_index_rows(
                &entry,
                self.state.heap.as_ref(),
                ddl_txn.xid,
                ddl_txn.current_command,
            ) {
                log_failed_ddl_rollback(
                    self.state.persistent_catalog.drop_index(&index_key),
                    "drop index",
                );
                return Err(self.rollback_catalog_transaction_after_error(
                    ddl_txn,
                    e.into(),
                    "CREATE AGGREGATING INDEX catalog rollback after persist error",
                ));
            }
            self.state.commit_transaction(
                ddl_txn,
                true,
                "CREATE AGGREGATING INDEX catalog transaction",
            )?;
            let mut constraints = self
                .state
                .table_constraints
                .get(&table.oid)
                .map(|entry| entry.value().as_ref().clone())
                .unwrap_or_default();
            constraints.indexes.insert(
                index_oid,
                crate::RuntimeIndexMetadata {
                    key_exprs: key_exprs.clone(),
                    predicate: None,
                    include_columns: Vec::new(),
                    method: *method,
                    brin: None,
                    hnsw: None,
                    ivfflat: None,
                    aggregating: Some(Arc::new(crate::RuntimeAggregatingIndex::new(spec, rows))),
                },
            );
            self.state
                .table_constraints
                .insert(table.oid, Arc::new(constraints));
            self.state.persist_table_runtime_constraints_metadata()?;
            self.plan_cache_invalidate();

            return Ok(run_ddl_command("CREATE INDEX"));
        }

        if *method == LogicalIndexMethod::IvfFlat {
            if *unique {
                return Err(ServerError::Unsupported(
                    "CREATE UNIQUE INDEX USING ivfflat: ivfflat indexes do not enforce uniqueness",
                ));
            }
            if columns.len() != 1 || key_exprs.len() != 1 || !include_columns.is_empty() {
                return Err(ServerError::Unsupported(
                    "CREATE INDEX USING ivfflat: exactly one vector column key is supported",
                ));
            }
            if predicate.is_some() {
                return Err(ServerError::Unsupported(
                    "CREATE INDEX USING ivfflat: partial indexes are not supported in this wave",
                ));
            }
            let vector_col = columns[0];
            let field = table.schema.field(vector_col).ok_or_else(|| {
                ServerError::ddl(format!(
                    "CREATE INDEX USING ivfflat: key column {vector_col} missing"
                ))
            })?;
            let (dims, default_payload) =
                ann_dims_and_default_payload("CREATE INDEX USING ivfflat", &field.data_type)?;
            let metric = hnsw_metric_for_opclass(opclasses.first().and_then(Option::as_deref))?;
            let (lists, probes, payload) = ivfflat_options(index_options)?;
            let payload = payload.unwrap_or(default_payload);
            let index_oid = self.state.persistent_catalog.next_oid();
            self.state
                .ensure_table_runtime_constraints_metadata_slots_persistable()?;
            let ivfflat = Arc::new(
                PageBackedIvfFlatIndex::new_with_payload_kind(
                    RelationId::new(index_oid.raw()),
                    dims,
                    metric,
                    lists,
                    probes,
                    payload,
                )
                .map_err(|e| ServerError::ddl(format!("CREATE INDEX ivfflat init: {e}")))?,
            );
            let txn = self.state.txn_manager.begin(IsolationLevel::ReadCommitted);
            let table_rel = RelationId(table.oid);
            let block_count = self.state.heap.block_count(table_rel).max(table.n_blocks);
            let progress = CreateIndexProgressGuard::new(
                self.state.workload_recorder.as_ref(),
                self.pid,
                table.oid.raw(),
                index_oid.raw(),
                block_count,
            );
            progress.update("scanning table", 0);
            let codec = ultrasql_executor::RowCodec::new(table.schema.clone());
            let scan = self.state.heap.scan_visible(
                table_rel,
                block_count,
                &txn.snapshot,
                self.state.txn_manager.as_ref(),
            );
            let build_result = (|| -> Result<(), ServerError> {
                let mut rows = Vec::new();
                let mut last_progress_block = 0;
                for result in scan {
                    let tuple = result.map_err(|e| {
                        ServerError::ddl(format!("CREATE INDEX ivfflat heap scan: {e}"))
                    })?;
                    let blocks_done = tuple
                        .tid
                        .page
                        .block
                        .raw()
                        .saturating_add(1)
                        .min(block_count);
                    if blocks_done != last_progress_block {
                        progress.update("scanning table", blocks_done);
                        last_progress_block = blocks_done;
                    }
                    let row = codec.decode(&tuple.data).map_err(|e| {
                        ServerError::ddl(format!("CREATE INDEX ivfflat decode: {e}"))
                    })?;
                    let vector = match row.get(vector_col) {
                        Some(Value::Vector(vector) | Value::HalfVec(vector)) => vector.clone(),
                        Some(Value::Null) => continue,
                        _ => {
                            return Err(ServerError::ddl(
                                "CREATE INDEX ivfflat: key column did not decode as vector or halfvec",
                            ));
                        }
                    };
                    rows.push((vector, tuple.tid));
                }
                progress.update("loading index", block_count);
                ivfflat
                    .bulk_load_logged(rows, txn.xid, self.state.heap.wal_sink().map(Arc::as_ref))
                    .map_err(|e| ServerError::ddl(format!("CREATE INDEX ivfflat bulk load: {e}")))
            })();
            self.state
                .commit_transaction(txn, true, "CREATE INDEX ivfflat build")?;
            build_result?;
            progress.update("writing catalog", block_count);
            let attnum = u16::try_from(vector_col).map_err(|_| {
                ServerError::Unsupported(
                    "CREATE INDEX: column index does not fit in u16 attnum field",
                )
            })?;
            let entry = IndexEntry::new(
                index_oid,
                index_name.clone(),
                table.oid,
                vec![attnum],
                false,
            )
            .with_schema_name(index_namespace.clone())
            .with_access_method("ivfflat", opclasses.clone())
            .with_options(index_options_as_pairs(index_options));
            self.state.persistent_catalog.create_index(entry.clone())?;
            let ddl_txn = self
                .state
                .txn_manager
                .begin(ultrasql_txn::IsolationLevel::ReadCommitted);
            if let Err(e) = self.state.persistent_catalog.persist_index_rows(
                &entry,
                self.state.heap.as_ref(),
                ddl_txn.xid,
                ddl_txn.current_command,
            ) {
                log_failed_ddl_rollback(
                    self.state.persistent_catalog.drop_index(&index_key),
                    "drop index",
                );
                return Err(self.rollback_catalog_transaction_after_error(
                    ddl_txn,
                    e.into(),
                    "CREATE IVFFLAT INDEX catalog rollback after persist error",
                ));
            }
            self.state.commit_transaction(
                ddl_txn,
                true,
                "CREATE IVFFLAT INDEX catalog transaction",
            )?;
            let mut constraints = self
                .state
                .table_constraints
                .get(&table.oid)
                .map(|entry| entry.value().as_ref().clone())
                .unwrap_or_default();
            constraints.indexes.insert(
                index_oid,
                crate::RuntimeIndexMetadata {
                    key_exprs: Vec::new(),
                    predicate: None,
                    include_columns: Vec::new(),
                    method: *method,
                    brin: None,
                    hnsw: None,
                    ivfflat: Some(ivfflat),
                    aggregating: None,
                },
            );
            self.state
                .table_constraints
                .insert(table.oid, Arc::new(constraints));
            self.state.persist_table_runtime_constraints_metadata()?;
            self.plan_cache_invalidate();

            return Ok(run_ddl_command("CREATE INDEX"));
        }

        if *method == LogicalIndexMethod::Hnsw {
            if *unique {
                return Err(ServerError::Unsupported(
                    "CREATE UNIQUE INDEX USING hnsw: hnsw indexes do not enforce uniqueness",
                ));
            }
            if columns.len() != 1 || key_exprs.len() != 1 || !include_columns.is_empty() {
                return Err(ServerError::Unsupported(
                    "CREATE INDEX USING hnsw: exactly one vector column key is supported",
                ));
            }
            if predicate.is_some() {
                return Err(ServerError::Unsupported(
                    "CREATE INDEX USING hnsw: partial indexes are not supported in this wave",
                ));
            }
            let vector_col = columns[0];
            let field = table.schema.field(vector_col).ok_or_else(|| {
                ServerError::ddl(format!(
                    "CREATE INDEX USING hnsw: key column {vector_col} missing"
                ))
            })?;
            let (dims, default_payload) =
                ann_dims_and_default_payload("CREATE INDEX USING hnsw", &field.data_type)?;

            let metric = hnsw_metric_for_opclass(opclasses.first().and_then(Option::as_deref))?;
            let payload = hnsw_payload_option(index_options)?.unwrap_or(default_payload);
            let index_oid = self.state.persistent_catalog.next_oid();
            self.state
                .ensure_table_runtime_constraints_metadata_slots_persistable()?;
            let index_rel = RelationId::new(index_oid.raw());
            let hnsw = Arc::new(
                PageBackedHnswIndex::new_with_payload_kind(
                    index_rel, dims, metric, 16, 64, payload,
                )
                .map_err(|e| ServerError::ddl(format!("CREATE INDEX hnsw init: {e}")))?,
            );
            let txn = self.state.txn_manager.begin(IsolationLevel::ReadCommitted);
            let table_rel = RelationId(table.oid);
            let block_count = self.state.heap.block_count(table_rel).max(table.n_blocks);
            let progress = CreateIndexProgressGuard::new(
                self.state.workload_recorder.as_ref(),
                self.pid,
                table.oid.raw(),
                index_oid.raw(),
                block_count,
            );
            progress.update("building index", 0);
            let codec = ultrasql_executor::RowCodec::new(table.schema.clone());
            let scan = self.state.heap.scan_visible(
                table_rel,
                block_count,
                &txn.snapshot,
                self.state.txn_manager.as_ref(),
            );
            let build_result = (|| -> Result<(), ServerError> {
                let mut last_progress_block = 0;
                for result in scan {
                    let tuple = result.map_err(|e| {
                        ServerError::ddl(format!("CREATE INDEX hnsw heap scan: {e}"))
                    })?;
                    let blocks_done = tuple
                        .tid
                        .page
                        .block
                        .raw()
                        .saturating_add(1)
                        .min(block_count);
                    if blocks_done != last_progress_block {
                        progress.update("building index", blocks_done);
                        last_progress_block = blocks_done;
                    }
                    let row = codec
                        .decode(&tuple.data)
                        .map_err(|e| ServerError::ddl(format!("CREATE INDEX hnsw decode: {e}")))?;
                    let vector = match row.get(vector_col) {
                        Some(Value::Vector(vector) | Value::HalfVec(vector)) => vector,
                        Some(Value::Null) => continue,
                        _ => {
                            return Err(ServerError::ddl(
                                "CREATE INDEX hnsw: key column did not decode as vector or halfvec",
                            ));
                        }
                    };
                    hnsw.insert_vector_logged(
                        vector,
                        tuple.tid,
                        txn.xid,
                        self.state.heap.wal_sink().map(Arc::as_ref),
                    )
                    .map_err(|e| ServerError::ddl(format!("CREATE INDEX hnsw insert: {e}")))?;
                }
                Ok(())
            })();
            self.state
                .commit_transaction(txn, true, "CREATE INDEX hnsw build")?;
            build_result?;
            progress.update("writing catalog", block_count);
            let attnum = u16::try_from(vector_col).map_err(|_| {
                ServerError::Unsupported(
                    "CREATE INDEX: column index does not fit in u16 attnum field",
                )
            })?;
            let entry = IndexEntry::new(
                index_oid,
                index_name.clone(),
                table.oid,
                vec![attnum],
                false,
            )
            .with_schema_name(index_namespace.clone())
            .with_access_method("hnsw", opclasses.clone())
            .with_options(index_options_as_pairs(index_options));
            self.state.persistent_catalog.create_index(entry.clone())?;
            let ddl_txn = self
                .state
                .txn_manager
                .begin(ultrasql_txn::IsolationLevel::ReadCommitted);
            if let Err(e) = self.state.persistent_catalog.persist_index_rows(
                &entry,
                self.state.heap.as_ref(),
                ddl_txn.xid,
                ddl_txn.current_command,
            ) {
                log_failed_ddl_rollback(
                    self.state.persistent_catalog.drop_index(&index_key),
                    "drop index",
                );
                return Err(self.rollback_catalog_transaction_after_error(
                    ddl_txn,
                    e.into(),
                    "CREATE HNSW INDEX catalog rollback after persist error",
                ));
            }
            self.state.commit_transaction(
                ddl_txn,
                true,
                "CREATE HNSW INDEX catalog transaction",
            )?;
            let mut constraints = self
                .state
                .table_constraints
                .get(&table.oid)
                .map(|entry| entry.value().as_ref().clone())
                .unwrap_or_default();
            constraints.indexes.insert(
                index_oid,
                crate::RuntimeIndexMetadata {
                    key_exprs: Vec::new(),
                    predicate: None,
                    include_columns: Vec::new(),
                    method: *method,
                    brin: None,
                    hnsw: Some(hnsw),
                    ivfflat: None,
                    aggregating: None,
                },
            );
            self.state
                .table_constraints
                .insert(table.oid, Arc::new(constraints));
            self.state.persist_table_runtime_constraints_metadata()?;
            self.plan_cache_invalidate();

            return Ok(run_ddl_command("CREATE INDEX"));
        }

        // 1c. Pick an i64 encoding for the requested key shape. The
        //     encoding is shared with the IndexScan probe path via
        //     `pipeline::key_encoding_for_btree` — keep the two
        //     resolutions consistent or a freshly built index will be
        //     unprobe-able.
        let expression_key_exprs = if columns.is_empty() {
            let [expr] = key_exprs.as_slice() else {
                return Err(ServerError::Unsupported(
                    "CREATE INDEX: expression indexes support exactly one key in this wave",
                ));
            };
            let _ = expr;
            key_exprs.clone()
        } else {
            Vec::new()
        };
        let encoding = if *method == ultrasql_planner::LogicalIndexMethod::Hash {
            crate::index_key::IndexKeyEncoding::Int64
        } else if expression_key_exprs.is_empty() {
            crate::index_key::IndexKeyEncoding::for_columns(&table.schema, columns)?
        } else {
            crate::index_key::IndexKeyEncoding::for_data_type(&expression_key_exprs[0].data_type())?
        };
        let key_col_idx = columns.first().copied();

        // 2. Allocate an OID and instantiate the B-tree.
        let index_oid = self.state.persistent_catalog.next_oid();
        let index_rel = RelationId::new(index_oid.raw());
        let pool = self.state.heap.buffer_pool();
        let mut btree = BTree::create(Arc::clone(pool), index_rel)
            .map_err(|e| ServerError::ddl(format!("BTree::create failed: {e}")))?;
        let root_block = btree.root_block();
        let brin_summary = if *method == ultrasql_planner::LogicalIndexMethod::Brin {
            Some(Arc::new(BrinIndex::new(128)))
        } else {
            None
        };
        let writes_runtime_index_metadata = !expression_key_exprs.is_empty()
            || predicate.is_some()
            || !include_columns.is_empty()
            || *method != ultrasql_planner::LogicalIndexMethod::Btree;
        if writes_runtime_index_metadata {
            self.state
                .ensure_table_runtime_constraints_metadata_slots_persistable()?;
        }

        // 3. Scan the heap and populate the tree.
        let mut attnums: Vec<u16> = Vec::with_capacity(columns.len());
        for &col in columns {
            let attnum = u16::try_from(col).map_err(|_| {
                ServerError::Unsupported(
                    "CREATE INDEX: column index does not fit in u16 attnum field",
                )
            })?;
            attnums.push(attnum);
        }
        let txn = self.state.txn_manager.begin(IsolationLevel::ReadCommitted);
        let table_rel = RelationId(table.oid);
        let block_count = self.state.heap.block_count(table_rel).max(table.n_blocks);
        let progress = CreateIndexProgressGuard::new(
            self.state.workload_recorder.as_ref(),
            self.pid,
            table.oid.raw(),
            index_oid.raw(),
            block_count,
        );
        progress.update("building index", 0);
        let scan = self.state.heap.scan_visible(
            table_rel,
            block_count,
            &txn.snapshot,
            self.state.txn_manager.as_ref(),
        );
        let insert_result = (|| -> Result<u64, ServerError> {
            let mut inserted: u64 = 0;
            let mut last_progress_block = 0;
            for result in scan {
                let tup =
                    result.map_err(|e| ServerError::ddl(format!("CREATE INDEX heap scan: {e}")))?;
                let blocks_done = tup.tid.page.block.raw().saturating_add(1).min(block_count);
                if blocks_done != last_progress_block {
                    progress.update("building index", blocks_done);
                    last_progress_block = blocks_done;
                }
                let row = decode_key_column(
                    &tup.data,
                    &table.schema,
                    key_col_idx,
                    &expression_key_exprs,
                    predicate.as_ref(),
                    *method,
                    &encoding,
                )?;
                if let Some(key) = row {
                    if *unique {
                        btree.insert(key, tup.tid, txn.xid, None).map_err(|e| {
                            ServerError::ddl(format!("CREATE INDEX btree insert: {e}"))
                        })?;
                    } else {
                        btree
                            .insert_non_unique(key, tup.tid, txn.xid, None)
                            .map_err(|e| {
                                ServerError::ddl(format!("CREATE INDEX btree insert: {e}"))
                            })?;
                    }
                    if let Some(brin) = &brin_summary {
                        let brin_key = BrinIndex::encode_i64_key(key);
                        brin.insert(&brin_key, tup.tid).map_err(|e| {
                            ServerError::ddl(format!("CREATE INDEX brin summarize: {e}"))
                        })?;
                    }
                    inserted += 1;
                }
                // NULL key — skip; PostgreSQL's btree omits NULL keys
                // from the index unless `INCLUDE` adds them, and our
                // BTree::insert lacks a NULL marker.
            }
            Ok(inserted)
        })();

        // Commit the txn regardless of build outcome so the XID does
        // not leak as in-progress forever.
        self.state
            .commit_transaction(txn, true, "CREATE INDEX build")?;
        let _ = insert_result?;
        progress.update("writing catalog", block_count);

        // 4. Register the index entry. The columns vector uses the
        //    1-based attnum convention shared with `pg_attribute`; the
        //    `IndexEntry` stores 0-based positions internally, so the
        //    cast is direct. We override `root_block` to match the
        //    freshly built tree.
        let mut entry = IndexEntry::new(index_oid, index_name.clone(), table.oid, attnums, *unique)
            .with_schema_name(index_namespace.clone())
            .with_primary(*primary_key)
            .with_access_method(logical_index_method_name(*method), opclasses.clone())
            .with_options(index_options_as_pairs(index_options));
        entry.root_block = root_block;
        self.state.persistent_catalog.create_index(entry.clone())?;
        let ddl_txn = self
            .state
            .txn_manager
            .begin(ultrasql_txn::IsolationLevel::ReadCommitted);
        if let Err(e) = self.state.persistent_catalog.persist_index_rows(
            &entry,
            self.state.heap.as_ref(),
            ddl_txn.xid,
            ddl_txn.current_command,
        ) {
            log_failed_ddl_rollback(
                self.state.persistent_catalog.drop_index(&index_key),
                "drop index",
            );
            return Err(self.rollback_catalog_transaction_after_error(
                ddl_txn,
                e.into(),
                "CREATE INDEX catalog rollback after persist error",
            ));
        }
        self.state
            .commit_transaction(ddl_txn, true, "CREATE INDEX catalog transaction")?;
        if writes_runtime_index_metadata {
            let mut constraints = self
                .state
                .table_constraints
                .get(&table.oid)
                .map(|entry| entry.value().as_ref().clone())
                .unwrap_or_default();
            constraints.indexes.insert(
                index_oid,
                crate::RuntimeIndexMetadata {
                    key_exprs: expression_key_exprs,
                    predicate: predicate.clone(),
                    include_columns: include_columns.clone(),
                    method: *method,
                    brin: brin_summary.clone(),
                    hnsw: None,
                    ivfflat: None,
                    aggregating: None,
                },
            );
            self.state
                .table_constraints
                .insert(table.oid, Arc::new(constraints));
            self.state.persist_table_runtime_constraints_metadata()?;
        }
        // A new index can flip an existing cached plan from
        // `Filter(SeqScan)` to `IndexScan`; clear the cache so the next
        // statement re-plans against the post-CREATE INDEX catalog.
        self.plan_cache_invalidate();

        Ok(run_ddl_command("CREATE INDEX"))
    }
}
