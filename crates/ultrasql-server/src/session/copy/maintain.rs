//! Index + CHECK/UNIQUE maintenance for `COPY FROM`.
//!
//! `COPY FROM` decodes every row to a wire payload and historically handed
//! the batch straight to `heap.insert_batch` — no secondary-index
//! maintenance and no constraint enforcement beyond NOT NULL. That left a
//! secondary/unique index STALE after a COPY (an index scan misses the
//! COPYed rows), admitted duplicate keys into a UNIQUE index, and skipped
//! CHECK constraints.
//!
//! This module closes that gap by routing a COPY batch through the SAME
//! [`ModifyTable`] INSERT operator a normal `INSERT` uses, so COPY reaches
//! parity with INSERT: per-row CHECK (23514), secondary + unique index
//! maintenance, UNIQUE enforcement (23505) — against committed rows, the
//! txn's own prior rows, AND duplicates within the same COPY batch — plus
//! FOREIGN KEY (23503) and EXCLUDE (23P01) checks, all reusing the executor's
//! INSERT machinery rather than reimplementing it.
//!
//! The maintained path is taken only when the target table actually has a
//! secondary/unique index or a CHECK/FK/EXCLUDE constraint
//! ([`Session::copy_table_needs_maintained_insert`]); a plain table keeps
//! the historical bulk fast path in [`Session::flush_copy_insert_batch`],
//! including the autocommit `mark_all_visible` optimisation.
//!
//! ## Atomicity
//!
//! The operator runs under the SAME [`Transaction`] the COPY rows ride: the
//! heap tuples and the index entries are stamped with `txn.current_xid()`
//! and share its abort fate. Under the Option-A no-index-undo model a leaf
//! entry left pointing at an aborted (now-dead) tuple is harmless — the
//! uniqueness recheck heap-rechecks under the snapshot+oracle and classifies
//! it as no-conflict. So `BEGIN; COPY …; ROLLBACK` discards the COPY rows
//! AND neutralises their index entries atomically, exactly as a rolled-back
//! INSERT does; `COMMIT` makes both durable. A mid-COPY violation surfaces as
//! an error that the COPY dispatcher turns into an all-or-nothing abort
//! (autocommit rolls the implicit txn back; an explicit block parks Failed
//! and the user's ROLLBACK discards everything).

use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncWrite};
use ultrasql_catalog::TableEntry;
use ultrasql_core::RelationId;
use ultrasql_executor::{
    MemTableScan, ModifyKind, ModifyTable, ModifyTableStamps, Operator, RowCodec, build_batch,
};
use ultrasql_txn::Transaction;

use super::super::Session;
use crate::error::ServerError;
use crate::pipeline::modify::{
    ConstraintCheckDeps, IndexMaintainerDeps, build_exclusion_insert_checks_from_deps,
    build_foreign_key_checks_from_deps, build_insert_index_maintainers_from_deps,
    build_vector_index_maintainers_from_deps,
};

impl<RW> Session<RW>
where
    RW: AsyncRead + AsyncWrite + Unpin,
{
    /// Whether a `COPY FROM` into `entry` must take the maintained INSERT
    /// path instead of the bulk `heap.insert_batch` fast path.
    ///
    /// `true` when the table has any secondary/unique index (B-tree, hash,
    /// or vector) or any CHECK / FOREIGN KEY / EXCLUDE constraint. NOT NULL
    /// alone does not force the maintained path — it is enforced at decode
    /// (and re-enforced by the operator when the maintained path is taken).
    ///
    /// `false` lets COPY keep the historical bulk fast path, including the
    /// autocommit `mark_all_visible` visibility-map optimisation, with no
    /// per-row overhead for an unconstrained table.
    pub(in crate::session) fn copy_table_needs_maintained_insert(
        &self,
        entry: &TableEntry,
    ) -> bool {
        let catalog = self.effective_catalog_snapshot();
        let has_index = catalog
            .indexes_by_table
            .get(&entry.oid)
            .is_some_and(|indexes| !indexes.is_empty());
        if has_index {
            return true;
        }
        self.state
            .table_constraints
            .get(&entry.oid)
            .is_some_and(|constraints| {
                !constraints.checks.is_empty()
                    || !constraints.foreign_keys.is_empty()
                    || !constraints.exclusion_constraints.is_empty()
            })
    }

    /// Insert one COPY batch through the [`ModifyTable`] INSERT operator so
    /// every secondary/unique index is maintained and CHECK/UNIQUE/FK/EXCLUDE
    /// are enforced exactly as a normal INSERT — reusing the executor's INSERT
    /// loop rather than reimplementing the maintenance.
    ///
    /// The decoded wire payloads are turned back into typed `Value` rows via
    /// the table's [`RowCodec`] and replayed through a [`MemTableScan`] child;
    /// the operator then drives uniqueness recheck (against committed rows and
    /// the txn's own prior rows), within-batch duplicate rejection, CHECK
    /// evaluation, FK/EXCLUDE checks, NOT NULL, the heap write, and B-tree +
    /// vector index maintenance — all stamped with `txn.current_xid()` so they
    /// roll back atomically with the COPY rows.
    ///
    /// A constraint violation returns the operator's [`ExecError`] wrapped in
    /// [`ServerError::Execute`], which carries the correct SQLSTATE (23514 /
    /// 23505 / 23503 / 23P01 / 23502). The whole batch is rejected before any
    /// row in it lands (the operator validates the entire batch's keys before
    /// the heap write), so a failed batch never leaves a half-maintained index.
    pub(in crate::session) fn flush_copy_insert_batch_maintained(
        &self,
        entry: &TableEntry,
        payloads: &[Vec<u8>],
        txn: &Transaction,
    ) -> Result<(), ServerError> {
        if payloads.is_empty() {
            return Ok(());
        }
        let codec = RowCodec::new(entry.schema.clone());
        let mut rows = Vec::with_capacity(payloads.len());
        for payload in payloads {
            let row = codec.decode(payload).map_err(|e| {
                ServerError::ddl(format!("COPY FROM row decode for maintenance: {e}"))
            })?;
            rows.push(row);
        }
        let batch = build_batch(&rows, &entry.schema)?;
        let child: Box<dyn Operator> =
            Box::new(MemTableScan::new(entry.schema.clone(), vec![batch]));

        let xid = txn.current_xid();
        let command_id = txn.current_command;

        // The same dependency views the INSERT-lowering path feeds the shared
        // maintainer/check builders — assembled here off the Session and the
        // governing COPY transaction so COPY reuses INSERT's machinery verbatim.
        let catalog = self.effective_catalog_snapshot();
        let index_deps = IndexMaintainerDeps {
            catalog_snapshot: &catalog,
            table_constraints: &self.state.table_constraints,
            heap: &self.state.heap,
            xid,
        };
        let insert_indexes = build_insert_index_maintainers_from_deps(entry, index_deps)?;
        let vector_deps = IndexMaintainerDeps {
            catalog_snapshot: &catalog,
            table_constraints: &self.state.table_constraints,
            heap: &self.state.heap,
            xid,
        };
        let insert_vector_indexes = build_vector_index_maintainers_from_deps(entry, vector_deps)?;

        let constraints = self
            .state
            .table_constraints
            .get(&entry.oid)
            .map(|c| c.clone());

        let mut modify = ModifyTable::new(
            Arc::clone(&self.state.heap),
            RelationId(entry.oid),
            entry.schema.clone(),
            ModifyKind::Insert,
            ModifyTableStamps::new(xid, command_id, xid, command_id),
            self.state.heap.wal_sink().cloned(),
            child,
        )
        .with_visibility_map(Arc::clone(&self.state.vm))
        .with_uniqueness_recheck(
            txn.snapshot.clone(),
            Arc::clone(&self.state.txn_manager) as Arc<dyn ultrasql_mvcc::XidStatusOracle>,
        )
        .with_insert_indexes(insert_indexes)
        .with_insert_vector_indexes(insert_vector_indexes);

        if let Some(constraints) = &constraints {
            if !constraints.checks.is_empty() {
                modify = modify.with_check_constraints(
                    constraints
                        .checks
                        .iter()
                        .map(|check| (check.name.clone(), check.expr.clone()))
                        .collect(),
                );
            }
            let check_deps = ConstraintCheckDeps {
                catalog_snapshot: &catalog,
                heap: &self.state.heap,
                snapshot: &txn.snapshot,
                oracle: &self.state.txn_manager,
            };
            let fk_checks =
                build_foreign_key_checks_from_deps(&constraints.foreign_keys, &check_deps)?;
            if !fk_checks.is_empty() {
                modify = modify.with_foreign_key_checks(fk_checks);
            }
            let exclusion_checks = build_exclusion_insert_checks_from_deps(
                entry,
                &constraints.exclusion_constraints,
                &check_deps,
            )?;
            if !exclusion_checks.is_empty() {
                modify = modify.with_exclusion_checks(exclusion_checks);
            }
        }

        let mut op: Box<dyn Operator> = Box::new(modify);
        while op.next_batch()?.is_some() {}
        self.state.flush_dirty_heap_pages_if_needed()?;
        Ok(())
    }
}
