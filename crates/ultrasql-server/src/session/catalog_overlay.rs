//! Per-transaction catalog overlay for transactional `CREATE TABLE`
//! (transactional-DDL milestone 1).
//!
//! When a `CREATE TABLE` runs inside an explicit `BEGIN … COMMIT` block the
//! global, copy-on-write `ArcSwap<CatalogSnapshot>` must NOT be mutated: a
//! concurrent session would observe a half-created (and possibly
//! never-committed) relation — a dirty schema read — and a `ROLLBACK` could
//! not undo it. Instead the created entries live here, session-locally,
//! bound to the active transaction's xid. The issuing session resolves the
//! relation through [`crate::CatalogSnapshot::with_overlay`]; every other
//! session keeps reading the unmodified committed snapshot.
//!
//! Durable side: the `pg_class` / `pg_attribute` / `pg_index` /
//! `pg_constraint` heap rows are written stamped with the **user** xid and
//! are NOT committed mid-statement — the user's `COMMIT` / `ROLLBACK` (and,
//! after a crash, the visibility-filtered catalog bootstrap) decides their
//! fate via MVCC. See `docs/transactional-ddl-design.md` §4.
//!
//! On `COMMIT` the overlay is merged into the global catalog with a single
//! `rebuild_snapshot()` publish; on `ROLLBACK` it is discarded and the
//! staged (non-MVCC) in-memory side effects are reverted.

use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncWrite};
use ultrasql_catalog::persistent::ConstraintRow;
use ultrasql_catalog::{CatalogSnapshot, IndexEntry, MutableCatalog, TableEntry};
use ultrasql_core::{BlockNumber, Oid, RelationId, Xid};
use ultrasql_executor::ExecError;
use ultrasql_planner::LogicalIndexMethod;
use ultrasql_storage::btree::{BTree, BTreeError};
use ultrasql_txn::Transaction;

use super::Session;
use crate::TxnState;
use crate::auth::{DefaultPrivilegeGrant, PrivilegeGrant};
use crate::error::ServerError;
use crate::{TableRowSecurity, TableRuntimeConstraints};

/// A session-local, transaction-scoped record of the in-progress
/// transactional-DDL catalog effects for one transaction.
///
/// Milestone 1/2 stages a single created table per overlay (a `BEGIN; CREATE
/// TABLE t; CREATE TABLE u` second statement is rejected — see
/// [`crate::session`]'s in-txn create-table guard).
///
/// Milestone 3 (transactional `CREATE INDEX` on an EXISTING table) reuses the
/// same overlay with `table == None`: the target table is already in the
/// global catalog, so only the new index's [`IndexEntry`] (and any unique
/// `pg_constraint` row) is staged, in `extra_indexes` / `extra_index_constraints`.
/// An overlay can therefore carry a created table, one-or-more extra indexes on
/// existing tables, or both (the same-txn table+index combo is deferred to a
/// follow-up and rejected for now).
pub(crate) struct CatalogOverlay {
    /// The transaction this overlay belongs to. Cross-checked against the
    /// active transaction's xid before the overlay is read, so a stale
    /// overlay can never leak into a different transaction.
    pub(crate) xid: Xid,
    /// The created table, if this overlay stages an in-txn `CREATE TABLE`.
    /// `None` for a pure `CREATE INDEX` overlay (the target table is already
    /// committed in the global catalog).
    pub(crate) table: Option<TableEntry>,
    /// Implicit unique / primary-key indexes created with the table.
    pub(crate) indexes: Vec<IndexEntry>,
    /// `pg_constraint` rows for the created table (unique / PK / check / FK /
    /// exclusion).
    pub(crate) constraints: Vec<ConstraintRow>,
    /// Indexes created in-txn over an EXISTING table (milestone 3 `CREATE
    /// INDEX`). Staged UNBUILT (`root_block == INVALID`) and built at COMMIT
    /// by [`Session::build_pending_catalog_ddl_indexes`], identically to the
    /// implicit constraint indexes in `indexes`.
    pub(crate) extra_indexes: Vec<IndexEntry>,
    /// `pg_constraint` rows for a `CREATE UNIQUE INDEX` over an existing table.
    pub(crate) extra_index_constraints: Vec<ConstraintRow>,
    /// Staged in-memory side effects of an in-txn `CREATE TABLE` to apply at
    /// COMMIT-merge and revert at ROLLBACK. `None` for a pure `CREATE INDEX`
    /// overlay, which stages no non-MVCC in-memory side map.
    pub(crate) staged: Option<StagedSideEffects>,
}

/// Non-MVCC, in-memory side effects of a `CREATE TABLE` that were applied
/// immediately for self-visibility inside the transaction and must be
/// reverted if it rolls back.
pub(crate) struct StagedSideEffects {
    /// OID of the created relation (key into the side maps below).
    pub(crate) oid: Oid,
    /// Folded `table_lookup_key` of the relation (key into time partitions).
    pub(crate) table_key: String,
    /// Runtime constraints inserted into `Server::table_constraints`, if any.
    pub(crate) runtime_constraints: Option<Arc<TableRuntimeConstraints>>,
    /// Whether `Server::row_security` previously had an entry for `oid`
    /// (it never should for a fresh OID, but we capture the prior value so
    /// the revert is exact rather than assumed).
    pub(crate) row_security_before: Option<Arc<TableRowSecurity>>,
    /// Whether a `time_partitions` entry was inserted under `table_key`.
    pub(crate) time_partition_inserted: bool,
    /// Privilege-catalog snapshot captured before `apply_default_privileges`,
    /// restored verbatim on rollback.
    pub(crate) privilege_grants_before: Vec<PrivilegeGrant>,
    /// Default-privilege snapshot captured before `apply_default_privileges`.
    pub(crate) privilege_default_grants_before: Vec<DefaultPrivilegeGrant>,
    /// Whether any privilege state actually changed (so commit/rollback can
    /// skip the persist round-trip when nothing moved).
    pub(crate) privileges_changed: bool,
}

impl<RW> Session<RW>
where
    RW: AsyncRead + AsyncWrite + Unpin,
{
    /// The catalog snapshot the binder / planner must read for THIS session
    /// this statement.
    ///
    /// Returns the committed `ArcSwap` snapshot unchanged (a single
    /// wait-free `load_full`) when there is no transactional-DDL overlay —
    /// preserving the zero-cost hot read path. Only when an overlay is
    /// present (and bound to the active transaction) does it pay for a
    /// snapshot clone with the in-transaction-created relation overlaid, so
    /// the issuing session can resolve the table it just created before
    /// COMMIT. Other sessions never call this session's overlay, which is
    /// how the others-no isolation guarantee holds.
    pub(crate) fn effective_catalog_snapshot(&self) -> Arc<CatalogSnapshot> {
        let base = self.state.catalog_snapshot();
        let Some(overlay) = self.pending_catalog_ddl.as_ref() else {
            return base;
        };
        // The overlay is only valid for the transaction that created it.
        // If the active transaction's xid does not match (which should not
        // happen — the overlay is cleared at COMMIT/ROLLBACK), fall back to
        // the committed snapshot rather than leak a foreign transaction's
        // pending schema.
        let active_xid = match &self.txn_state {
            TxnState::InTransaction(txn) | TxnState::Failed(txn) => Some(txn.xid),
            TxnState::Idle => None,
        };
        if active_xid != Some(overlay.xid) {
            return base;
        }
        Arc::new(base.with_overlay(
            overlay.table.as_ref(),
            &overlay.indexes,
            &overlay.constraints,
            &overlay.extra_indexes,
            &overlay.extra_index_constraints,
        ))
    }

    /// Build the deferred constraint-index B-trees for a pending
    /// transactional-DDL overlay at COMMIT (transactional-DDL milestone 2).
    ///
    /// An in-transaction `CREATE TABLE … PRIMARY KEY / UNIQUE` stages its
    /// implicit constraint index UNBUILT — `IndexEntry::root_block ==
    /// BlockNumber::INVALID` — so in-txn INSERTs skip its maintenance (the
    /// INVALID-root skip in `build_one_insert_index_maintainer`) and a ROLLBACK
    /// (or crash before COMMIT) leaks no durable index segment. This method
    /// builds each such tree exactly once, here, BEFORE the user xid is
    /// durably committed:
    ///
    /// 1. allocate the durable B-tree segment ([`BTree::create`]);
    /// 2. scan the new table's rows under the USER transaction snapshot
    ///    (`txn.snapshot`) — every row this transaction inserted is visible to
    ///    itself and nothing committed concurrently can be — using NO inner
    ///    transaction, stamping each B-tree insert with `txn.xid`;
    /// 3. encode each row's key with the same `IndexKeyEncoding` /
    ///    `decode_key_column` path the insert-time maintainer uses, so the
    ///    freshly built tree is byte-for-byte probe-compatible;
    /// 4. on a [`BTreeError::DuplicateKey`] return
    ///    [`ExecError::UniqueViolation`] (SQLSTATE `23505`) — the caller
    ///    (`execute_commit`) takes this BEFORE `commit_transaction`, so a
    ///    duplicate aborts the WHOLE transaction (rows + table + index all
    ///    gone) rather than half-committing the table;
    /// 5. on success, write the real `root_block` back into the overlay's
    ///    `IndexEntry` IN PLACE so the subsequent in-memory publish
    ///    (`commit_pending_catalog_ddl`, which clones the overlay entries)
    ///    registers a probe-able tree;
    /// 6. and — because an index's durable `root_block` lives in its
    ///    `pg_class.relfilenode`, which was written INVALID when the unbuilt
    ///    index's catalog rows were persisted in
    ///    `persist_create_table_rows_under_xid` — RE-PERSIST the index rows
    ///    (`persist_index_rows`) under the user xid. The catalog heap is
    ///    append-only and bootstrap keeps the latest `pg_class` row per OID, so
    ///    this fresh row (carrying the real `relfilenode`) supersedes the
    ///    INVALID one after a restart; both rows ride the still-uncommitted user
    ///    xid, so a ROLLBACK discards them and the index never resurrects.
    ///    Without this re-persist a committed index would rebuild UNBUILT on
    ///    restart (silently losing uniqueness enforcement) — the corruption
    ///    class this milestone must not introduce.
    ///
    /// Entries already built (`root_block != INVALID`, e.g. an autocommit path
    /// that should never reach here, or an empty overlay) are skipped.
    ///
    /// Milestone 3: the same deferred-build machinery also drives an in-txn
    /// `CREATE INDEX` on an EXISTING table (`overlay.extra_indexes`). The only
    /// difference is the source of the table schema/blocks — the implicit
    /// constraint indexes target the overlay's freshly created `table`, while a
    /// `CREATE INDEX` targets a table already in the committed snapshot
    /// (resolved here by `table_oid`). For an existing table `txn.snapshot`
    /// correctly sees all committed rows PLUS the issuing transaction's own
    /// writes, so the freshly built tree indexes exactly the rows that COMMIT
    /// will make durable.
    pub(crate) fn build_pending_catalog_ddl_indexes(
        &mut self,
        txn: &Transaction,
    ) -> Result<(), ServerError> {
        // Take ownership of the staged index lists so the per-index build can
        // borrow `&self.state` while it mutates each entry's `root_block` in
        // place; the entries are written back into the overlay afterwards.
        let Some(overlay) = self.pending_catalog_ddl.as_mut() else {
            return Ok(());
        };
        // The implicit constraint indexes are plain unique B-trees over the
        // overlay's freshly created table; gather its schema once.
        let created_table = overlay.table.clone();
        let mut indexes = std::mem::take(&mut overlay.indexes);
        let mut extra_indexes = std::mem::take(&mut overlay.extra_indexes);

        let build_result =
            (|| -> Result<(), ServerError> {
                if let Some(table) = created_table.as_ref() {
                    for index in indexes.iter_mut() {
                        self.build_one_deferred_index(
                            index,
                            &table.schema,
                            RelationId(table.oid),
                            table.n_blocks,
                            txn,
                        )?;
                    }
                }
                // Milestone 3: each `CREATE INDEX` targets a table already in the
                // committed snapshot. Resolve its schema by OID (the issuing
                // transaction never altered it — same-txn-created tables are
                // rejected by the in-txn CREATE INDEX guard).
                for index in extra_indexes.iter_mut() {
                    let snapshot = self.state.catalog_snapshot();
                    let target = snapshot.tables_by_oid.get(&index.table_oid).ok_or_else(|| {
                    ServerError::ddl(format!(
                        "COMMIT index build: target table for index '{}' not found in catalog",
                        index.name
                    ))
                })?;
                    let schema = target.schema.clone();
                    let table_rel = RelationId(target.oid);
                    let n_blocks = target.n_blocks;
                    drop(snapshot);
                    self.build_one_deferred_index(index, &schema, table_rel, n_blocks, txn)?;
                }
                Ok(())
            })();

        // Restore the (now-built) entries into the overlay regardless of
        // outcome so a later publish / discard sees the same set. On error the
        // caller (`execute_commit`) immediately discards the overlay.
        if let Some(overlay) = self.pending_catalog_ddl.as_mut() {
            overlay.indexes = indexes;
            overlay.extra_indexes = extra_indexes;
        }
        build_result
    }

    /// Build one deferred B-tree index over `table_rel`'s visible rows under
    /// the user `txn` snapshot, stamp the real `root_block` into `index` in
    /// place, and re-persist its `pg_index` / `pg_class` rows under the user
    /// xid so the durable `relfilenode` is corrected from INVALID to the built
    /// root. A duplicate key surfaces as `23505` BEFORE the commit marker is
    /// written, aborting the whole transaction. Shared by the implicit
    /// constraint-index path (milestone 2) and the `CREATE INDEX` path
    /// (milestone 3).
    fn build_one_deferred_index(
        &self,
        index: &mut IndexEntry,
        table_schema: &ultrasql_core::Schema,
        table_rel: RelationId,
        table_n_blocks: u32,
        txn: &Transaction,
    ) -> Result<(), ServerError> {
        if index.root_block != BlockNumber::INVALID {
            // Already built (defensive: the in-txn path stages every entry
            // UNBUILT, so this only guards re-entry / non-deferred entries).
            return Ok(());
        }
        let block_count = self.state.heap.block_count(table_rel).max(table_n_blocks);
        let columns: Vec<usize> = index.columns.iter().map(|c| usize::from(*c)).collect();
        let encoding = crate::index_key::IndexKeyEncoding::for_columns(table_schema, &columns)?;
        let key_col_idx = columns.first().copied();
        let index_rel = RelationId::new(index.oid.raw());
        let mut btree = BTree::create(Arc::clone(self.state.heap.buffer_pool()), index_rel)
            .map_err(|e| ServerError::ddl(format!("COMMIT index create: {e}")))?;
        let root_block = btree.root_block();
        // Scan the table's rows under the user snapshot with NO inner
        // transaction: for a freshly created table the rows this transaction
        // inserted are self-visible; for an EXISTING table the snapshot also
        // sees every committed row. The build runs before the commit marker is
        // written so a duplicate aborts the whole txn.
        let scan = self.state.heap.scan_visible(
            table_rel,
            block_count,
            &txn.snapshot,
            self.state.txn_manager.as_ref(),
        );
        for result in scan {
            let tup = result
                .map_err(|e| ServerError::ddl(format!("COMMIT index build heap scan: {e}")))?;
            let key = crate::decode_key_column(
                &tup.data,
                table_schema,
                key_col_idx,
                &[],
                None,
                LogicalIndexMethod::Btree,
                &encoding,
            )?;
            // A NULL key is omitted from the index (PostgreSQL's btree omits
            // NULL keys; uniqueness does not constrain them), so it never
            // collides — mirroring the insert-time maintainer.
            if let Some(key) = key {
                let inserted = if index.is_unique {
                    btree.insert(key, tup.tid, txn.xid, None)
                } else {
                    // A non-unique `CREATE INDEX` over existing rows must not
                    // reject duplicate keys (mirrors the autocommit builder's
                    // `insert_non_unique`).
                    btree.insert_non_unique(key, tup.tid, txn.xid, None)
                };
                match inserted {
                    Ok(()) => {}
                    Err(BTreeError::DuplicateKey) => {
                        return Err(ServerError::Execute(ExecError::UniqueViolation(
                            index.name.clone(),
                        )));
                    }
                    Err(e) => {
                        return Err(ServerError::ddl(format!("COMMIT index build insert: {e}")));
                    }
                }
            }
        }
        // Write the real root back into the overlay entry IN PLACE so the
        // in-memory publish (which clones these entries) references the
        // freshly built tree.
        index.root_block = root_block;
        // Re-persist the index rows so the durable pg_class.relfilenode (the
        // index's root_block on disk) is corrected from INVALID to the real
        // root under the user xid. The append-only catalog + latest-per-OID
        // bootstrap make this fresh row win after a restart; on ROLLBACK both
        // rows are aborted-xid and discarded.
        self.state
            .persistent_catalog
            .persist_index_rows(
                index,
                self.state.heap.as_ref(),
                txn.xid,
                txn.current_command,
            )
            .map_err(|e| ServerError::ddl(format!("COMMIT index re-persist: {e}")))?;
        Ok(())
    }

    /// Merge a pending transactional-DDL overlay into the global catalog on
    /// COMMIT.
    ///
    /// Called from `execute_commit` AFTER `commit_transaction` has written
    /// the durable commit marker for the user xid (which makes the catalog
    /// heap rows — already on disk under that xid — visible on restart). This
    /// publishes the in-memory catalog state: the table, its indexes, and its
    /// constraint rows go into the live DashMaps with a single
    /// `rebuild_snapshot` per `create_*` / `install_constraint_rows` call, and
    /// the deferred runtime-constraint / RLS / privilege metadata sidecars are
    /// persisted now that the table is in the global snapshot. The staged
    /// in-memory side effects (runtime constraints, RLS, time partitions,
    /// privileges) were already applied at create time and simply stay.
    ///
    /// A failure here is logged, not propagated: the transaction is already
    /// durably committed, so the heap side is authoritative and a fresh
    /// `bootstrap_from_heap` (e.g. on the next restart) reconstructs the same
    /// in-memory state. We still clear the overlay so a later statement does
    /// not re-merge it.
    pub(crate) fn commit_pending_catalog_ddl(&mut self) {
        let Some(overlay) = self.pending_catalog_ddl.take() else {
            return;
        };
        let catalog = &self.state.persistent_catalog;
        // An in-txn `CREATE TABLE` overlay publishes the table first; a pure
        // `CREATE INDEX` overlay (`table == None`) skips this — the target
        // table is already in the global catalog.
        if let Some(table) = overlay.table.as_ref() {
            if let Err(e) = catalog.create_table(table.clone()) {
                tracing::error!(
                    error = %e,
                    table = %table.name,
                    "transactional CREATE TABLE commit: publishing table to global catalog failed; \
                     heap rows are durable and will be rebuilt on restart"
                );
            }
            for index in &overlay.indexes {
                if let Err(e) = catalog.create_index(index.clone()) {
                    tracing::error!(
                        error = %e,
                        index = %index.name,
                        "transactional CREATE TABLE commit: publishing index to global catalog failed"
                    );
                }
            }
        }
        // Milestone 3: publish each in-txn `CREATE INDEX` over an existing
        // table. The entry now carries the real (built) `root_block` from the
        // deferred build, so it is immediately probe-able.
        for index in &overlay.extra_indexes {
            if let Err(e) = catalog.create_index(index.clone()) {
                tracing::error!(
                    error = %e,
                    index = %index.name,
                    "transactional CREATE INDEX commit: publishing index to global catalog failed"
                );
            }
        }
        // Publishes the constraint rows (the created table's, plus any unique
        // `CREATE INDEX` constraint) and issues the final `rebuild_snapshot`
        // so the committed snapshot reflects the new relation/index atomically.
        let mut constraint_rows = overlay.constraints.clone();
        constraint_rows.extend(overlay.extra_index_constraints.iter().cloned());
        catalog.install_constraint_rows(constraint_rows);

        // The table is now in the global snapshot, so the metadata sidecars
        // (deferred at create time) can be written including it. A pure
        // `CREATE INDEX` overlay staged no in-memory side maps, so these are
        // no-ops there (and `staged` is `None`).
        if overlay.staged.is_some() {
            if let Err(e) = self.state.persist_table_runtime_constraints_metadata() {
                tracing::error!(error = %e, "transactional CREATE TABLE commit: persist runtime-constraints metadata failed");
            }
            if let Err(e) = self.state.persist_row_security_metadata() {
                tracing::error!(error = %e, "transactional CREATE TABLE commit: persist row-security metadata failed");
            }
        }
        if overlay
            .staged
            .as_ref()
            .is_some_and(|staged| staged.privileges_changed)
            && let Err(e) = self.state.persist_privilege_metadata()
        {
            tracing::error!(error = %e, "transactional CREATE TABLE commit: persist privilege metadata failed");
        }

        // The committed table/index can shadow names a cached plan rewrote
        // against the previous snapshot; clear the cache so the next statement
        // re-plans against the now-committed catalog.
        self.plan_cache_invalidate();
    }

    /// Discard a pending transactional-DDL overlay on ROLLBACK (or a failed
    /// COMMIT that rolls back), reverting the staged in-memory, non-MVCC side
    /// effects.
    ///
    /// The global catalog was never mutated for the in-txn path, so there is
    /// nothing to undo there. The durable catalog heap rows survive on disk
    /// stamped with the aborted user xid — MVCC-invisible at runtime and
    /// hidden by the visibility-filtered bootstrap on restart. Only the
    /// session-applied side maps need reverting:
    ///
    /// - runtime constraints inserted into `table_constraints`,
    /// - the `row_security` entry,
    /// - any `time_partitions` entry,
    /// - the privilege-catalog grants (restored from the captured snapshot).
    ///
    /// None of these were persisted to their metadata files for the in-txn
    /// table (those writes are deferred to COMMIT), so no file revert is
    /// needed.
    pub(crate) fn discard_pending_catalog_ddl(&mut self) {
        if self.pending_catalog_ddl.is_none() {
            return;
        }
        self.revert_staged_catalog_ddl_side_effects();
        // The session's bind cache may hold plans bound against the overlay
        // (which resolved the now-gone table); clear it so the next statement
        // re-binds against the committed catalog.
        self.plan_cache_invalidate();
    }
}

impl<RW> Session<RW> {
    /// Revert the non-MVCC, global in-memory side effects a pending
    /// transactional-DDL overlay staged, and clear the overlay.
    ///
    /// Free of the `AsyncRead + AsyncWrite` bounds so it can be invoked from
    /// both [`Session::discard_pending_catalog_ddl`] and the `Drop` impl
    /// (a client that disconnects mid-transaction, after an in-txn
    /// `CREATE TABLE` but before `COMMIT`/`ROLLBACK`, must not leak the staged
    /// side maps for the lifetime of the process).
    pub(super) fn revert_staged_catalog_ddl_side_effects(&mut self) {
        let Some(overlay) = self.pending_catalog_ddl.take() else {
            return;
        };
        // A pure `CREATE INDEX` overlay (milestone 3) staged no in-memory side
        // map — the durable unbuilt `pg_index` rows ride the aborted user xid
        // and the segment was never built — so there is nothing to revert here.
        let Some(staged) = overlay.staged else {
            return;
        };

        if staged.runtime_constraints.is_some() {
            self.state.table_constraints.remove(&staged.oid);
        }
        match staged.row_security_before {
            Some(prev) => {
                self.state.row_security.insert(staged.oid, prev);
            }
            None => {
                self.state.row_security.remove(&staged.oid);
            }
        }
        if staged.time_partition_inserted {
            self.state.time_partitions.remove(&staged.table_key);
        }
        if staged.privileges_changed {
            self.state.privilege_catalog.install_snapshot(
                staged.privilege_grants_before,
                staged.privilege_default_grants_before,
            );
        }
    }
}
