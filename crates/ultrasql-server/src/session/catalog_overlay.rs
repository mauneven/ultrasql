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
use ultrasql_core::{Oid, Xid};

use super::Session;
use crate::TxnState;
use crate::auth::{DefaultPrivilegeGrant, PrivilegeGrant};
use crate::{TableRowSecurity, TableRuntimeConstraints};

/// A session-local, transaction-scoped record of one in-progress
/// `CREATE TABLE` and its catalog effects.
///
/// Milestone 1 stages a single created table per overlay (a `BEGIN; CREATE
/// TABLE t; CREATE TABLE u` second statement is rejected — see
/// [`crate::session`]'s in-txn create-table guard).
pub(crate) struct CatalogOverlay {
    /// The transaction this overlay belongs to. Cross-checked against the
    /// active transaction's xid before the overlay is read, so a stale
    /// overlay can never leak into a different transaction.
    pub(crate) xid: Xid,
    /// The created table.
    pub(crate) table: TableEntry,
    /// Implicit unique / primary-key indexes created with the table.
    pub(crate) indexes: Vec<IndexEntry>,
    /// `pg_constraint` rows (unique / PK / check / FK / exclusion).
    pub(crate) constraints: Vec<ConstraintRow>,
    /// Staged side effects to apply at COMMIT-merge and revert at ROLLBACK.
    pub(crate) staged: StagedSideEffects,
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
        Arc::new(base.with_overlay(&overlay.table, &overlay.indexes, &overlay.constraints))
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
        if let Err(e) = catalog.create_table(overlay.table.clone()) {
            tracing::error!(
                error = %e,
                table = %overlay.table.name,
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
        // Publishes the constraint rows and issues the final `rebuild_snapshot`
        // so the committed snapshot reflects the new relation atomically.
        catalog.install_constraint_rows(overlay.constraints.clone());

        // The table is now in the global snapshot, so the metadata sidecars
        // (deferred at create time) can be written including it.
        if let Err(e) = self.state.persist_table_runtime_constraints_metadata() {
            tracing::error!(error = %e, "transactional CREATE TABLE commit: persist runtime-constraints metadata failed");
        }
        if let Err(e) = self.state.persist_row_security_metadata() {
            tracing::error!(error = %e, "transactional CREATE TABLE commit: persist row-security metadata failed");
        }
        if overlay.staged.privileges_changed
            && let Err(e) = self.state.persist_privilege_metadata()
        {
            tracing::error!(error = %e, "transactional CREATE TABLE commit: persist privilege metadata failed");
        }

        // The committed table can shadow names a cached plan rewrote against
        // the previous snapshot; clear the cache so the next statement re-plans
        // against the now-committed catalog.
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
        let staged = overlay.staged;

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
