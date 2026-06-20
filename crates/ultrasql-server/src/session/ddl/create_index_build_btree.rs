//! B-tree (and BRIN / Hash) index builder for `CREATE INDEX`. Part of
//! the `session::ddl` module split; reopens the `impl<RW> Session<RW>`
//! block defined in `session/mod.rs`.
//!
//! Split out from `create_index_build.rs` to keep each file under the
//! 700-line ceiling. Logic is moved verbatim; see
//! [`Session::execute_create_index`] for the supported key shapes and
//! gaps.

use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncWrite};
use ultrasql_catalog::{IndexEntry, MutableCatalog, TableEntry};
use ultrasql_core::RelationId;
use ultrasql_planner::{LogicalIndexMethod, LogicalIndexOption, ScalarExpr};
use ultrasql_storage::access_method::{AccessMethod, BrinIndex};
use ultrasql_storage::btree::BTree;
use ultrasql_txn::IsolationLevel;

use super::super::Session;
use super::index_options::{index_options_as_pairs, logical_index_method_name};
use super::{CreateIndexProgressGuard, log_failed_ddl_rollback};
use crate::decode_key_column;
use crate::error::ServerError;
use crate::result_encoder::{SelectResult, run_ddl_command};

impl<RW> Session<RW>
where
    RW: AsyncRead + AsyncWrite + Unpin,
{
    /// Build a B+ tree index (optionally also a BRIN summary, or under
    /// the Hash access method) and register it in `pg_index`. See the
    /// module-level docs on [`Session::execute_create_index`] for the
    /// supported key shapes and gaps.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn build_btree_index(
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
        primary_key: bool,
        index_key: &str,
    ) -> Result<SelectResult, ServerError> {
        // 1c. Pick an i64 encoding for the requested key shape. The
        //     encoding is shared with the IndexScan probe path via
        //     `pipeline::key_encoding_for_btree` — keep the two
        //     resolutions consistent or a freshly built index will be
        //     unprobe-able.
        let expression_key_exprs = if columns.is_empty() {
            let [expr] = key_exprs else {
                return Err(ServerError::Unsupported(
                    "CREATE INDEX: expression indexes support exactly one key in this wave",
                ));
            };
            let _ = expr;
            key_exprs.to_vec()
        } else {
            Vec::new()
        };
        let encoding = if method == ultrasql_planner::LogicalIndexMethod::Hash {
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
        let brin_summary = if method == ultrasql_planner::LogicalIndexMethod::Brin {
            Some(Arc::new(BrinIndex::new(128)))
        } else {
            None
        };
        let writes_runtime_index_metadata = !expression_key_exprs.is_empty()
            || predicate.is_some()
            || !include_columns.is_empty()
            || method != ultrasql_planner::LogicalIndexMethod::Btree;
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
                    method,
                    &encoding,
                )?;
                if let Some(key) = row {
                    if unique {
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
        let mut entry =
            IndexEntry::new(index_oid, index_name.to_string(), table.oid, attnums, unique)
                .with_schema_name(index_namespace.to_string())
                .with_primary(primary_key)
                .with_access_method(logical_index_method_name(method), opclasses.to_vec())
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
                self.state.persistent_catalog.drop_index(index_key),
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
                    include_columns: include_columns.to_vec(),
                    method,
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
