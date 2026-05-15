//! Part of the `session` module split. The
//! `impl<RW> Session<RW>` block is reopened here to add a handful
//! of methods to the type defined in `session/mod.rs`. Splitting
//! across files keeps every unit under the 600-line ceiling without
//! changing semantics.

#![allow(unused_imports)]

use std::sync::Arc;

use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tracing::{debug, error, info, warn};
use ultrasql_catalog::{
    CatalogSnapshot, IndexEntry, MutableCatalog, PersistentCatalog, TableEntry,
};
use ultrasql_core::{DataType, PageId, RelationId, Value};
use ultrasql_optimizer::{NoStats, PlanCache, PlanCacheConfig, PlanCacheKey, StatsSource};
use ultrasql_parser::Parser;
use ultrasql_planner::{
    Catalog as PlannerCatalog, InMemoryCatalog, LogicalAlterTableAction, LogicalPlan, TableMeta,
    bind,
};
use ultrasql_protocol::{BackendMessage, FrontendMessage, decode_frontend, encode_backend};
use ultrasql_storage::btree::BTree;
use ultrasql_storage::buffer_pool::{BufferPool, PageLoader};
use ultrasql_storage::heap::{DeleteOptions, HeapAccess, UpdateOptions};
use ultrasql_storage::page::Page;
use ultrasql_txn::{IsolationLevel, Transaction, TransactionManager};

use super::Session;
use crate::error::ServerError;
use crate::extended;
use crate::pipeline::{self, LowerCtx, SampleTables};
use crate::result_encoder::{
    self, SelectResult, run_ddl_command, run_modify_command, run_select, run_select_streamed,
};
use crate::{
    BlankPageLoader, CombinedCatalog, Server, TxnState, decode_key_column, notice_warning,
    run_plan_in_txn,
};

impl<RW> Session<RW>
where
    RW: AsyncRead + AsyncWrite + Unpin,
{
    /// Persist a `CREATE TABLE` into the catalog.
    ///
    /// Honors `IF NOT EXISTS` by short-circuiting when the relation
    /// already exists in either the persistent snapshot or the
    /// in-memory sample catalog. The resolved column [`Schema`] from
    /// the binder is stored verbatim, so a subsequent statement that
    /// captures a fresh snapshot will see the new relation.
    ///
    /// Currently a metadata-only operation: the segment file and the
    /// `pg_class.relfilenode` block are allocated lazily on the first
    /// `INSERT`. This matches PostgreSQL's `RelationSetNewRelfilenode`
    /// timing closely enough that subsequent `INSERT` wiring (in a
    /// follow-up commit) can stamp the right block number then.
    pub(crate) fn execute_create_table(
        &self,
        plan: &LogicalPlan,
        snapshot: &CatalogSnapshot,
    ) -> Result<SelectResult, ServerError> {
        let LogicalPlan::CreateTable {
            table_name,
            namespace,
            columns,
            if_not_exists,
            ..
        } = plan
        else {
            return Err(ServerError::Unsupported(
                "execute_create_table called with non-CreateTable plan",
            ));
        };
        let exists_persistent = snapshot.tables.contains_key(table_name);
        let exists_fallback = self.state.catalog.lookup_table(table_name).is_some();
        if exists_persistent || exists_fallback {
            if *if_not_exists {
                return Ok(run_ddl_command("CREATE TABLE"));
            }
            return Err(ServerError::Catalog(
                ultrasql_catalog::CatalogError::already_exists(table_name.clone()),
            ));
        }
        let oid = self.state.persistent_catalog.next_oid();
        let entry = TableEntry::new(oid, table_name.clone(), namespace.clone(), columns.clone());
        self.state.persistent_catalog.create_table(entry.clone())?;
        // Persist the typed pg_class + pg_attribute rows so a restart
        // can rebuild this `TableEntry` via
        // `PersistentCatalog::bootstrap_from_heap`. The DDL runs in an
        // autocommit transaction allocated on the spot; the rows are
        // stamped with that xid so MVCC visibility lines up with the
        // user-table relations created in the same statement.
        let ddl_txn = self
            .state
            .txn_manager
            .begin(ultrasql_txn::IsolationLevel::ReadCommitted);
        let ddl_xid = ddl_txn.xid;
        let ddl_command_id = ddl_txn.current_command;
        if let Err(e) = self.state.persistent_catalog.persist_table_rows(
            &entry,
            self.state.heap.as_ref(),
            ddl_xid,
            ddl_command_id,
        ) {
            // Abort the catalog-write txn before surfacing the error so
            // the CLOG entry is closed and the rollback path cleans
            // any partial in-place undo entries (there are none for
            // pg_class inserts, but symmetry matters for future
            // expansion).
            if let Err(abort_err) = self.state.txn_manager.abort(ddl_txn) {
                tracing::warn!(
                    error = %abort_err,
                    "abort of catalog-write txn failed after persist_table_rows error",
                );
            }
            return Err(e.into());
        }
        if let Err(commit_err) = self.state.txn_manager.commit(ddl_txn) {
            tracing::warn!(
                error = %commit_err,
                "catalog-write txn failed to commit; restart visibility may differ",
            );
        }
        // A new relation can shadow names a cached plan rewrote against
        // the previous snapshot; clear the cache so the next statement
        // re-plans.
        self.plan_cache_invalidate();
        Ok(run_ddl_command("CREATE TABLE"))
    }

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
            table_name,
            columns,
            unique,
            if_not_exists,
            ..
        } = plan
        else {
            return Err(ServerError::Unsupported(
                "execute_create_index called with non-CreateIndex plan",
            ));
        };

        // 1a. IF NOT EXISTS short-circuit.
        if snapshot.indexes.contains_key(index_name) {
            if *if_not_exists {
                return Ok(run_ddl_command("CREATE INDEX"));
            }
            return Err(ServerError::Catalog(
                ultrasql_catalog::CatalogError::already_exists(index_name.clone()),
            ));
        }

        // 1b. Resolve the parent table.
        let table = snapshot.tables.get(table_name).ok_or_else(|| {
            ServerError::Plan(ultrasql_planner::PlanError::TableNotFound(
                table_name.clone(),
            ))
        })?;

        // 1c. Pick an i64 encoding for the requested key shape. The
        //     encoding is shared with the IndexScan probe path via
        //     `pipeline::key_encoding_for_btree` — keep the two
        //     resolutions consistent or a freshly built index will be
        //     unprobe-able.
        let encoding = crate::index_key::IndexKeyEncoding::for_columns(&table.schema, columns)?;
        let key_col_idx = columns[0];

        // 2. Allocate an OID and instantiate the B-tree.
        let index_oid = self.state.persistent_catalog.next_oid();
        let index_rel = RelationId::new(index_oid.raw());
        let pool = self.state.heap.buffer_pool();
        let mut btree = BTree::create(Arc::clone(pool), index_rel)
            .map_err(|e| ServerError::ddl(format!("BTree::create failed: {e}")))?;
        let root_block = btree.root_block();

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
        let scan = self.state.heap.scan_visible(
            table_rel,
            block_count,
            &txn.snapshot,
            self.state.txn_manager.as_ref(),
        );
        let insert_result = (|| -> Result<u64, ServerError> {
            let mut inserted: u64 = 0;
            for result in scan {
                let tup =
                    result.map_err(|e| ServerError::ddl(format!("CREATE INDEX heap scan: {e}")))?;
                let row = decode_key_column(&tup.data, &table.schema, key_col_idx, &encoding)?;
                if let Some(key) = row {
                    btree
                        .insert(key, tup.tid, txn.xid, None)
                        .map_err(|e| ServerError::ddl(format!("CREATE INDEX btree insert: {e}")))?;
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
        if let Err(e) = self.state.txn_manager.commit(txn) {
            tracing::warn!(error = %e, "autocommit (CREATE INDEX) failed to finalise");
        }
        let _ = insert_result?;

        // 4. Register the index entry. The columns vector uses the
        //    1-based attnum convention shared with `pg_attribute`; the
        //    `IndexEntry` stores 0-based positions internally, so the
        //    cast is direct. We override `root_block` to match the
        //    freshly built tree.
        let mut entry = IndexEntry::new(index_oid, index_name.clone(), table.oid, attnums, *unique);
        entry.root_block = root_block;
        self.state.persistent_catalog.create_index(entry)?;
        // A new index can flip an existing cached plan from
        // `Filter(SeqScan)` to `IndexScan`; clear the cache so the next
        // statement re-plans against the post-CREATE INDEX catalog.
        self.plan_cache_invalidate();

        Ok(run_ddl_command("CREATE INDEX"))
    }

    /// Drop one or more tables.
    ///
    /// The binder has already filtered names through the catalog —
    /// see [`ultrasql_planner::bind`] — so the only failure surface
    /// here is `CatalogError::NotFound`, which can fire only when a
    /// concurrent DDL deleted the relation between the binder and the
    /// dispatcher. Associated indexes are removed by
    /// [`MutableCatalog::drop_table`] in a single atomic snapshot
    /// rotation.
    ///
    /// Heap pages backing the dropped relation are *not* reclaimed in
    /// this wave: the in-memory buffer pool grows on demand and the
    /// segment manager has not yet landed. The dropped name becomes
    /// available immediately for reuse via `CREATE TABLE` — subsequent
    /// inserts will reuse the relation-id space without colliding
    /// because OIDs are monotonic.
    pub(crate) fn execute_drop_table(
        &self,
        plan: &LogicalPlan,
    ) -> Result<SelectResult, ServerError> {
        let LogicalPlan::DropTable { tables, .. } = plan else {
            return Err(ServerError::Unsupported(
                "execute_drop_table called with non-DropTable plan",
            ));
        };
        for name in tables {
            self.state.persistent_catalog.drop_table(name)?;
        }
        // Any cached plan that referenced this name is now invalid;
        // clear the cache so subsequent statements re-plan.
        self.plan_cache_invalidate();
        Ok(run_ddl_command("DROP TABLE"))
    }
}
