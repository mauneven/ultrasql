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
    /// Apply an `ALTER TABLE` action.
    ///
    /// The only supported action in this wave is `ADD COLUMN`. For
    /// `ADD COLUMN` we
    ///
    /// 1. take a per-statement MVCC snapshot,
    /// 2. scan every visible tuple under the *old* schema and rewrite
    ///    it back through `HeapAccess::update` with a payload encoded
    ///    against the *new* schema (the appended column carries
    ///    [`Value::Null`] for every pre-existing row),
    /// 3. swap the catalog entry to the new schema via
    ///    [`MutableCatalog::alter_table_add_column`].
    ///
    /// Steps 2 and 3 are wrapped in a single autocommit transaction so
    /// the rewrite and the catalog swap commit (or abort) together;
    /// concurrent readers either see the old schema with old tuples or
    /// the new schema with rewritten tuples — never a torn state.
    ///
    /// # Sub-shape gaps documented for reviewers
    ///
    /// - `DROP COLUMN`, `RENAME COLUMN`, `RENAME TO`, and
    ///   `ADD/DROP CONSTRAINT` are not yet bindable in
    ///   [`ultrasql_planner::bind`]; the binder returns
    ///   `NotSupported` for them so they never reach this arm.
    /// - The rewrite is online-unsafe today: there is no per-relation
    ///   exclusive lock taken across steps 2 and 3, so a concurrent
    ///   INSERT during the rewrite may produce a tuple that scans see
    ///   under the new schema but was encoded against the old one. We
    ///   ship this anyway because v0.5 dispatches Simple Query
    ///   statements serially per connection and the README workload
    ///   does not concurrently mutate the relation under test. A
    ///   follow-up will route DDL through the lock manager
    ///   (`AccessExclusiveLock`).
    pub(crate) fn execute_alter_table(
        &self,
        plan: &LogicalPlan,
        snapshot: &CatalogSnapshot,
    ) -> Result<SelectResult, ServerError> {
        let LogicalPlan::AlterTable {
            table_name, action, ..
        } = plan
        else {
            return Err(ServerError::Unsupported(
                "execute_alter_table called with non-AlterTable plan",
            ));
        };
        match action {
            LogicalAlterTableAction::AddColumn { column } => {
                self.execute_alter_add_column(table_name, column.clone(), snapshot)
            }
            LogicalAlterTableAction::DropColumn {
                column_index,
                column_name,
            } => self.execute_alter_drop_column(table_name, *column_index, column_name, snapshot),
            LogicalAlterTableAction::RenameColumn {
                column_index,
                new_name,
                ..
            } => self.execute_alter_rename_column(table_name, *column_index, new_name, snapshot),
            LogicalAlterTableAction::RenameTable { new_name } => {
                self.execute_alter_rename_table(table_name, new_name)
            }
        }
    }

    /// Execute `ALTER TABLE t DROP COLUMN c`: rewrite every visible
    /// tuple without that slot, then publish the narrower schema.
    pub(crate) fn execute_alter_drop_column(
        &self,
        table_name: &str,
        column_index: usize,
        column_name: &str,
        snapshot: &CatalogSnapshot,
    ) -> Result<SelectResult, ServerError> {
        let entry = snapshot.tables.get(table_name).ok_or_else(|| {
            ServerError::Plan(ultrasql_planner::PlanError::TableNotFound(
                table_name.to_owned(),
            ))
        })?;
        let mut new_fields: Vec<ultrasql_core::Field> = entry.schema.fields().to_vec();
        if column_index >= new_fields.len() {
            return Err(ServerError::ddl(format!(
                "ALTER TABLE DROP COLUMN: index {column_index} out of bounds for {table_name}"
            )));
        }
        new_fields.remove(column_index);
        let new_schema = ultrasql_core::Schema::new(new_fields).map_err(|e| {
            ServerError::Catalog(ultrasql_catalog::CatalogError::schema_conflict(format!(
                "ALTER TABLE DROP COLUMN: {e}"
            )))
        })?;

        let txn = self.state.txn_manager.begin(IsolationLevel::ReadCommitted);
        let rel = RelationId(entry.oid);
        let block_count = self.state.heap.block_count(rel).max(entry.n_blocks);
        let old_codec = ultrasql_executor::RowCodec::new(entry.schema.clone());
        let new_codec = ultrasql_executor::RowCodec::new(new_schema.clone());

        let rewrite_result: Result<(), ServerError> = (|| {
            let mut to_rewrite: Vec<(ultrasql_core::TupleId, Vec<Value>)> = Vec::new();
            {
                let scan = self.state.heap.scan_visible(
                    rel,
                    block_count,
                    &txn.snapshot,
                    self.state.txn_manager.as_ref(),
                );
                for result in scan {
                    let tup = result.map_err(|e| {
                        ServerError::ddl(format!("ALTER TABLE DROP COLUMN scan: {e}"))
                    })?;
                    let row = old_codec.decode(&tup.data).map_err(|e| {
                        ServerError::ddl(format!("ALTER TABLE DROP COLUMN decode: {e}"))
                    })?;
                    to_rewrite.push((tup.tid, row));
                }
            }
            for (tid, mut old_row) in to_rewrite {
                old_row.remove(column_index);
                let new_payload = new_codec.encode(&old_row).map_err(|e| {
                    ServerError::ddl(format!("ALTER TABLE DROP COLUMN encode: {e}"))
                })?;
                self.state
                    .heap
                    .update(
                        tid,
                        &new_payload,
                        UpdateOptions {
                            xid: txn.xid,
                            command_id: ultrasql_core::CommandId::FIRST,
                            wal: None,
                            vm: None,
                            hot_eligible: true,
                        },
                    )
                    .map_err(|e| {
                        ServerError::ddl(format!("ALTER TABLE DROP COLUMN heap update: {e}"))
                    })?;
            }
            Ok(())
        })();

        match rewrite_result {
            Ok(()) => {
                self.state
                    .persistent_catalog
                    .alter_table_replace_schema(table_name, new_schema)
                    .map_err(ServerError::Catalog)?;
                self.state
                    .txn_manager
                    .commit(txn)
                    .map_err(|e| ServerError::ddl(format!("ALTER TABLE DROP COLUMN commit: {e}")))?;
                self.state.plan_cache.invalidate_all();
                Ok(run_ddl_command(&format!(
                    "ALTER TABLE DROP COLUMN {column_name}"
                )))
            }
            Err(e) => {
                let _ = self.state.txn_manager.abort(txn);
                Err(e)
            }
        }
    }

    /// Execute `ALTER TABLE t RENAME COLUMN old TO new`: catalog-only
    /// (the heap's row codec is positional so no rewrite is needed).
    pub(crate) fn execute_alter_rename_column(
        &self,
        table_name: &str,
        column_index: usize,
        new_name: &str,
        snapshot: &CatalogSnapshot,
    ) -> Result<SelectResult, ServerError> {
        let entry = snapshot.tables.get(table_name).ok_or_else(|| {
            ServerError::Plan(ultrasql_planner::PlanError::TableNotFound(
                table_name.to_owned(),
            ))
        })?;
        let mut new_fields: Vec<ultrasql_core::Field> = entry.schema.fields().to_vec();
        if column_index >= new_fields.len() {
            return Err(ServerError::ddl(format!(
                "ALTER TABLE RENAME COLUMN: index {column_index} out of bounds for {table_name}"
            )));
        }
        let renamed = ultrasql_core::Field {
            name: new_name.to_string(),
            ..new_fields[column_index].clone()
        };
        new_fields[column_index] = renamed;
        let new_schema = ultrasql_core::Schema::new(new_fields).map_err(|e| {
            ServerError::Catalog(ultrasql_catalog::CatalogError::schema_conflict(format!(
                "ALTER TABLE RENAME COLUMN: {e}"
            )))
        })?;
        self.state
            .persistent_catalog
            .alter_table_replace_schema(table_name, new_schema)
            .map_err(ServerError::Catalog)?;
        self.state.plan_cache.invalidate_all();
        Ok(run_ddl_command(&format!(
            "ALTER TABLE RENAME COLUMN TO {new_name}"
        )))
    }

    /// Execute `ALTER TABLE t RENAME TO new`: catalog-only (relations
    /// are OID-addressed; the rename only updates the by-name index).
    pub(crate) fn execute_alter_rename_table(
        &self,
        old_name: &str,
        new_name: &str,
    ) -> Result<SelectResult, ServerError> {
        self.state
            .persistent_catalog
            .alter_table_rename(old_name, new_name)
            .map_err(ServerError::Catalog)?;
        self.state.plan_cache.invalidate_all();
        Ok(run_ddl_command(&format!("ALTER TABLE RENAME TO {new_name}")))
    }

    /// Execute the `ALTER TABLE t ADD COLUMN c TYPE [NULL | NOT NULL]`
    /// path.
    ///
    /// Decoded from the dispatch arm so `execute_alter_table` stays
    /// a thin shape-match. See [`Self::execute_alter_table`] for the
    /// design notes that apply to the rewrite ordering, MVCC, and the
    /// known online-DDL gap.
    pub(crate) fn execute_alter_add_column(
        &self,
        table_name: &str,
        column: ultrasql_core::Field,
        snapshot: &CatalogSnapshot,
    ) -> Result<SelectResult, ServerError> {
        // 1. Resolve the existing entry and build the new schema.
        let entry = snapshot.tables.get(table_name).ok_or_else(|| {
            ServerError::Plan(ultrasql_planner::PlanError::TableNotFound(
                table_name.to_owned(),
            ))
        })?;
        let mut new_fields: Vec<ultrasql_core::Field> = entry.schema.fields().to_vec();
        new_fields.push(column.clone());
        let new_schema = ultrasql_core::Schema::new(new_fields).map_err(|e| {
            ServerError::Catalog(ultrasql_catalog::CatalogError::schema_conflict(format!(
                "ALTER TABLE ADD COLUMN: {e}"
            )))
        })?;

        // 2. Rewrite existing tuples — outside the catalog swap so
        //    the snapshot scan still observes the old schema.
        let txn = self.state.txn_manager.begin(IsolationLevel::ReadCommitted);
        let rel = RelationId(entry.oid);
        let block_count = self.state.heap.block_count(rel).max(entry.n_blocks);
        let old_codec = ultrasql_executor::RowCodec::new(entry.schema.clone());
        let new_codec = ultrasql_executor::RowCodec::new(new_schema);

        let rewrite_result: Result<(), ServerError> = (|| {
            // Collect the visible tuples up front so the heap iterator
            // is fully drained before any update lands — otherwise the
            // iterator could revisit a row that the update has just
            // copied into a new slot. The relations we ALTER in v0.5
            // fit comfortably in memory.
            let mut to_rewrite: Vec<(ultrasql_core::TupleId, Vec<Value>)> = Vec::new();
            {
                let scan = self.state.heap.scan_visible(
                    rel,
                    block_count,
                    &txn.snapshot,
                    self.state.txn_manager.as_ref(),
                );
                for result in scan {
                    let tup = result
                        .map_err(|e| ServerError::ddl(format!("ALTER TABLE heap scan: {e}")))?;
                    let row = old_codec
                        .decode(&tup.data)
                        .map_err(|e| ServerError::ddl(format!("ALTER TABLE row decode: {e}")))?;
                    to_rewrite.push((tup.tid, row));
                }
            }

            // Now perform the updates.
            for (tid, old_row) in to_rewrite {
                let mut new_row = old_row;
                new_row.push(Value::Null);
                let new_payload = new_codec
                    .encode(&new_row)
                    .map_err(|e| ServerError::ddl(format!("ALTER TABLE row encode: {e}")))?;
                self.state
                    .heap
                    .update(
                        tid,
                        &new_payload,
                        UpdateOptions {
                            xid: txn.xid,
                            command_id: ultrasql_core::CommandId::FIRST,
                            wal: None,
                            vm: None,
                            hot_eligible: true,
                        },
                    )
                    .map_err(|e| ServerError::ddl(format!("ALTER TABLE heap update: {e}")))?;
            }
            Ok(())
        })();

        // 3. Swap the catalog entry only if the rewrite succeeded;
        //    otherwise abort the transaction so the half-rewritten
        //    tuples become dead (their xmin matches our xid, which we
        //    will mark aborted on rollback).
        match rewrite_result {
            Ok(()) => {
                self.state
                    .persistent_catalog
                    .alter_table_add_column(table_name, column)?;
                if let Err(e) = self.state.txn_manager.commit(txn) {
                    tracing::warn!(error = %e, "autocommit (ALTER TABLE) failed to finalise");
                }
                // A schema change can invalidate any cached projection-
                // pushdown / predicate-pushdown decision; clear all.
                self.plan_cache_invalidate();
                Ok(run_ddl_command("ALTER TABLE"))
            }
            Err(e) => {
                if let Err(abort_err) = self.state.txn_manager.abort(txn) {
                    tracing::warn!(
                        error = %abort_err,
                        "autocommit (ALTER TABLE rollback) failed to abort"
                    );
                }
                Err(e)
            }
        }
    }

    /// Empty every relation named in the `TRUNCATE` statement.
    ///
    /// PostgreSQL's `TRUNCATE` takes `ACCESS EXCLUSIVE` and reclaims the
    /// relfilenode in a single fast-path: drop the segment files, then
    /// allocate a fresh empty heap on commit. UltraSQL's v0.5 in-memory
    /// runtime has no segment manager wired into the server's
    /// `BufferPool<BlankPageLoader>`, so the fast-path "swap the
    /// relfilenode" hook does not yet exist on this path. Instead, we
    /// open an autocommit MVCC transaction and stamp `xmax` on every
    /// row visible to the txn's own snapshot by calling
    /// [`HeapAccess::delete`] for each visible TID.
    ///
    /// Correctness notes:
    ///
    /// - The result is MVCC-correct under our snapshot model: a
    ///   concurrent snapshot that pre-dates the truncate's commit
    ///   continues to see every row (its `xmax` is committed-after
    ///   from the older snapshot's POV); a snapshot taken after the
    ///   commit sees the relation as empty.
    /// - Dead-tuple pages stay on the heap. A subsequent `INSERT` will
    ///   reuse free space inside them as it would after any DELETE,
    ///   and `n_blocks` stays unchanged so future scans still cover
    ///   the dead-tuple block range (necessary because a row inserted
    ///   into one of those reused slots must still be discovered).
    /// - The path is `O(rows visible to txn)` rather than O(1). For
    ///   the wire-completion gate this is acceptable: TRUNCATE is no
    ///   longer rejected, and a future segment-manager wiring can
    ///   replace this body with the proper fast-path without touching
    ///   any caller.
    ///
    /// `RESTART IDENTITY` and `CASCADE` are accepted by the parser and
    /// the binder but currently have no effect at execution time:
    ///
    /// - `RESTART IDENTITY` reseeds owned sequences. UltraSQL does not
    ///   yet implement `SERIAL` / sequence catalogs (see ROADMAP P1
    ///   v0.6), so there are no sequences to reseed. The keyword is
    ///   accept-and-ignore until that lands.
    /// - `CASCADE` truncates dependent foreign-key children. UltraSQL
    ///   does not yet enforce foreign keys at the catalog level, so
    ///   there are no dependent relations to find. The keyword is
    ///   accept-and-ignore until the foreign-key wave lands.
    ///
    /// Multi-table `TRUNCATE` truncates every table inside a single
    /// autocommit transaction so the operation is atomic — either all
    /// listed relations become empty in the next snapshot or none do.
    pub(crate) fn execute_truncate(
        &self,
        plan: &LogicalPlan,
        snapshot: &CatalogSnapshot,
    ) -> Result<SelectResult, ServerError> {
        let LogicalPlan::Truncate { tables, .. } = plan else {
            return Err(ServerError::Unsupported(
                "execute_truncate called with non-Truncate plan",
            ));
        };

        // Single autocommit txn so the multi-table case is atomic. A
        // partial failure aborts the txn and every delete it stamped
        // becomes invisible to subsequent snapshots.
        let txn = self.state.txn_manager.begin(IsolationLevel::ReadCommitted);

        let truncate_result: Result<(), ServerError> = (|| {
            for name in tables {
                let entry = snapshot.tables.get(name).ok_or_else(|| {
                    ServerError::Plan(ultrasql_planner::PlanError::TableNotFound(name.clone()))
                })?;
                let rel = RelationId(entry.oid);
                // The heap's resident block count is the source of
                // truth for "how many blocks must I scan." We OR with
                // the catalog's hint so a relation extended on a
                // previous connection still gets a complete scan.
                let block_count = self.state.heap.block_count(rel).max(entry.n_blocks);

                // Snapshot every visible TID up front, then issue the
                // deletes in a second pass. Holding the heap iterator
                // open across delete calls would let the iterator
                // revisit a tuple whose xmax we just stamped; flushing
                // to a vector first avoids that race.
                let mut tids: Vec<ultrasql_core::TupleId> = Vec::new();
                {
                    let scan = self.state.heap.scan_visible(
                        rel,
                        block_count,
                        &txn.snapshot,
                        self.state.txn_manager.as_ref(),
                    );
                    for result in scan {
                        let tup = result
                            .map_err(|e| ServerError::ddl(format!("TRUNCATE heap scan: {e}")))?;
                        tids.push(tup.tid);
                    }
                }

                for tid in tids {
                    self.state
                        .heap
                        .delete(
                            tid,
                            DeleteOptions {
                                xmax: txn.xid,
                                cmax: ultrasql_core::CommandId::FIRST,
                                wal: None,
                                fsm: None,
                                vm: None,
                            },
                        )
                        .map_err(|e| ServerError::ddl(format!("TRUNCATE heap delete: {e}")))?;
                }
            }
            Ok(())
        })();

        match truncate_result {
            Ok(()) => {
                if let Err(e) = self.state.txn_manager.commit(txn) {
                    tracing::warn!(error = %e, "autocommit (TRUNCATE) failed to finalise");
                }
                // Row counts changed beyond recognition; clear the cache
                // so any cardinality-aware plan re-runs.
                self.plan_cache_invalidate();
                Ok(run_ddl_command("TRUNCATE TABLE"))
            }
            Err(e) => {
                if let Err(abort_err) = self.state.txn_manager.abort(txn) {
                    tracing::warn!(
                        error = %abort_err,
                        "autocommit (TRUNCATE rollback) failed to abort"
                    );
                }
                Err(e)
            }
        }
    }
}
