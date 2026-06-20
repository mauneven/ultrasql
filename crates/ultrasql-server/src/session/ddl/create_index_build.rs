//! Per-access-method index builders for `CREATE INDEX`. Part of the
//! `session::ddl` module split; reopens the `impl<RW> Session<RW>` block
//! defined in `session/mod.rs`.
//!
//! Each method here owns the build + catalog-registration kernel for one
//! ANN / aggregating access method (aggregating, IVFFlat, HNSW). The
//! B-tree / BRIN / Hash builder lives in `create_index_build_btree.rs`.
//! The orchestration entry point [`Session::execute_create_index`] in
//! `create_index.rs` validates the request and dispatches to the right
//! builder. Logic is moved verbatim from the original single-method file;
//! see that module for the design notes covering supported key shapes.

use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncWrite};
use ultrasql_catalog::{IndexEntry, MutableCatalog, TableEntry};
use ultrasql_core::{RelationId, Value};
use ultrasql_planner::{
    LogicalAggregatingIndex, LogicalIndexMethod, LogicalIndexOption, ScalarExpr,
};
use ultrasql_storage::access_method::{PageBackedHnswIndex, PageBackedIvfFlatIndex};
use ultrasql_txn::IsolationLevel;

use super::super::Session;
use super::index_options::{
    ann_dims_and_default_payload, hnsw_metric_for_opclass, hnsw_payload_option,
    index_options_as_pairs, ivfflat_options,
};
use super::{CreateIndexProgressGuard, log_failed_ddl_rollback};
use crate::error::ServerError;
use crate::result_encoder::{SelectResult, run_ddl_command};

impl<RW> Session<RW>
where
    RW: AsyncRead + AsyncWrite + Unpin,
{
    /// Build an `AGGREGATING` index and register it in `pg_index`.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn build_aggregating_index(
        &self,
        table: &TableEntry,
        index_name: &str,
        index_namespace: &str,
        columns: &[usize],
        key_exprs: &[ScalarExpr],
        method: LogicalIndexMethod,
        aggregating: &Option<LogicalAggregatingIndex>,
        unique: bool,
        index_key: &str,
    ) -> Result<SelectResult, ServerError> {
        if unique {
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
        let entry = IndexEntry::new(index_oid, index_name.to_string(), table.oid, attnums, false)
            .with_schema_name(index_namespace.to_string())
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
                self.state.persistent_catalog.drop_index(index_key),
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
                key_exprs: key_exprs.to_vec(),
                predicate: None,
                include_columns: Vec::new(),
                method,
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

        Ok(run_ddl_command("CREATE INDEX"))
    }

    /// Build an IVFFlat (inverted-file) ANN index and register it in
    /// `pg_index`.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn build_ivfflat_index(
        &self,
        table: &TableEntry,
        index_name: &str,
        index_namespace: &str,
        columns: &[usize],
        key_exprs: &[ScalarExpr],
        opclasses: &[Option<String>],
        index_options: &[LogicalIndexOption],
        include_columns: &[usize],
        predicate: &Option<ScalarExpr>,
        method: LogicalIndexMethod,
        unique: bool,
        index_key: &str,
    ) -> Result<SelectResult, ServerError> {
        if unique {
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
            index_name.to_string(),
            table.oid,
            vec![attnum],
            false,
        )
        .with_schema_name(index_namespace.to_string())
        .with_access_method("ivfflat", opclasses.to_vec())
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
                self.state.persistent_catalog.drop_index(index_key),
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
                method,
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

        Ok(run_ddl_command("CREATE INDEX"))
    }

    /// Build an HNSW (graph) ANN index and register it in `pg_index`.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn build_hnsw_index(
        &self,
        table: &TableEntry,
        index_name: &str,
        index_namespace: &str,
        columns: &[usize],
        key_exprs: &[ScalarExpr],
        opclasses: &[Option<String>],
        index_options: &[LogicalIndexOption],
        include_columns: &[usize],
        predicate: &Option<ScalarExpr>,
        method: LogicalIndexMethod,
        unique: bool,
        index_key: &str,
    ) -> Result<SelectResult, ServerError> {
        if unique {
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
            index_name.to_string(),
            table.oid,
            vec![attnum],
            false,
        )
        .with_schema_name(index_namespace.to_string())
        .with_access_method("hnsw", opclasses.to_vec())
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
                self.state.persistent_catalog.drop_index(index_key),
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
                method,
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

        Ok(run_ddl_command("CREATE INDEX"))
    }
}
