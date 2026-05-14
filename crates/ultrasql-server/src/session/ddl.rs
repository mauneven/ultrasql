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

use crate::error::ServerError;
use crate::extended;
use crate::pipeline::{self, LowerCtx, SampleTables};
use crate::result_encoder::{
    self, SelectResult, run_ddl_command, run_modify_command, run_select, run_select_streamed,
};
use crate::{
    BlankPageLoader, CombinedCatalog, Server, TxnState, notice_warning, run_plan_in_txn,
    decode_key_column,
};
use super::Session;

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
        self.state.persistent_catalog.create_table(entry)?;
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
    ///    `IF NOT EXISTS`, presence of the parent table, key-column
    ///    type compatibility with the B-tree (currently only fixed-size
    ///    8-byte keys are stored, so `Int64` is the natural domain;
    ///    `Int32` keys are widened to `i64` before insertion).
    /// 2. Allocate a fresh OID for the index and instantiate a new
    ///    [`BTree`] over a relation id derived from that OID. The
    ///    buffer pool's blank-page loader hands out empty heap pages
    ///    which `BTree::create` then initialises as B-tree leaves.
    /// 3. Scan every visible row of the parent table under an
    ///    autocommit snapshot, decode the key column, and call
    ///    [`BTree::insert`] with the row's [`ultrasql_core::TupleId`].
    /// 4. Build an [`IndexEntry`] carrying the root block plus the
    ///    requested attnums, register it with the persistent catalog,
    ///    and let the catalog's snapshot rotation publish the entry to
    ///    subsequent statements.
    ///
    /// # Sub-shape gaps documented for reviewers
    ///
    /// - Only single-column indexes are built today. The binder
    ///   accepts multi-column lists for completeness (so a follow-up
    ///   can flip the kernel restriction without re-binding) but the
    ///   server rejects them here.
    /// - Only `Int32` / `Int64` key types are supported. Other types
    ///   (text, float, bool) would require a richer [`BTree`] key
    ///   trait; the build returns
    ///   [`ServerError::Unsupported`] for them.
    /// - `UNIQUE` is honoured at the catalog level — the
    ///   [`IndexEntry::is_unique`] flag is propagated — but the
    ///   B-tree's existing duplicate-key rejection is the only
    ///   enforcement. Non-unique indexes that happen to have unique
    ///   data still build correctly; non-unique indexes with
    ///   duplicates would error here, which is a known limitation
    ///   we accept until the B-tree gains a non-unique mode.
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

        // 1c. Validate the key columns. Only one column, only Int32 /
        //     Int64 — see the doc comment for the rationale.
        if columns.len() != 1 {
            return Err(ServerError::Unsupported(
                "CREATE INDEX: only single-column indexes are supported in this wave",
            ));
        }
        let key_col_idx = columns[0];
        let key_field = table.schema.field(key_col_idx).ok_or_else(|| {
            ServerError::Plan(ultrasql_planner::PlanError::ColumnNotFound(format!(
                "column index {key_col_idx} in table {table_name}"
            )))
        })?;
        let widen_i32 = match key_field.data_type {
            DataType::Int32 => true,
            DataType::Int64 => false,
            _ => {
                return Err(ServerError::Unsupported(
                    "CREATE INDEX: only Int32 / Int64 key columns are supported in this wave",
                ));
            }
        };

        // 2. Allocate an OID and instantiate the B-tree.
        let index_oid = self.state.persistent_catalog.next_oid();
        let index_rel = RelationId::new(index_oid.raw());
        let pool = self.state.heap.buffer_pool();
        let mut btree = BTree::create(Arc::clone(pool), index_rel)
            .map_err(|e| ServerError::ddl(format!("BTree::create failed: {e}")))?;
        let root_block = btree.root_block();

        // 3. Scan the heap and populate the tree.
        let key_attnum = u16::try_from(key_col_idx).map_err(|_| {
            ServerError::Unsupported("CREATE INDEX: column index does not fit in u16 attnum field")
        })?;
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
                let row = decode_key_column(&tup.data, &table.schema, key_col_idx, widen_i32)?;
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
        let attnums: Vec<u16> = vec![key_attnum];
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
    pub(crate) fn execute_drop_table(&self, plan: &LogicalPlan) -> Result<SelectResult, ServerError> {
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
