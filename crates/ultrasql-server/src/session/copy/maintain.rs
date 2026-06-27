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
use ultrasql_core::{RelationId, Schema};
use ultrasql_executor::{
    MemTableScan, ModifyKind, ModifyTable, ModifyTableStamps, Operator, RowCodec, SequenceDefault,
    SequenceNextvalObserver, build_batch,
};
use ultrasql_txn::Transaction;

use crate::BlankPageLoader;

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
        // Full-width payloads (no COPY column list, or a column list that omits
        // no defaulted column): decode straight into the table schema and feed
        // the operator with no column map or default metadata. The decode layer
        // already filled omitted columns with NULL and enforced NOT NULL.
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
        let modify = self.build_copy_modify(entry, txn, child)?;
        let mut op: Box<dyn Operator> = Box::new(modify);
        while op.next_batch()?.is_some() {}
        self.state.flush_dirty_heap_pages_if_needed()?;
        Ok(())
    }

    /// Whether a `COPY t(col-list) FROM` must take the default-applying insert
    /// path because at least one OMITTED table column carries a DEFAULT,
    /// sequence (`SERIAL`), `GENERATED ... AS IDENTITY`, or `GENERATED ... AS
    /// (expr) STORED` — matching PostgreSQL, which fills every omitted column
    /// from its default machinery rather than NULL.
    ///
    /// `false` (no column list, or every omitted column has no default) keeps
    /// the existing decode-fills-NULL path: an omitted column there becomes
    /// NULL, and a NOT NULL omitted column with no default raises 23502 — both
    /// already PostgreSQL-correct.
    pub(in crate::session) fn copy_column_list_applies_defaults(
        &self,
        entry: &TableEntry,
        columns: &[usize],
    ) -> bool {
        if columns.is_empty() || columns.len() == entry.schema.len() {
            return false;
        }
        let Some(constraints) = self.state.table_constraints.get(&entry.oid) else {
            return false;
        };
        (0..entry.schema.len())
            .filter(|idx| !columns.contains(idx))
            .any(|idx| {
                constraints.defaults.get(idx).is_some_and(Option::is_some)
                    || constraints
                        .sequence_defaults
                        .get(idx)
                        .is_some_and(Option::is_some)
                    || constraints.identity_always.get(idx).copied() == Some(true)
                    || constraints
                        .generated_stored
                        .get(idx)
                        .is_some_and(Option::is_some)
            })
    }

    /// Insert a COPY batch decoded over the NARROW stream schema (only the
    /// columns named in the COPY column list, in stream order), applying the
    /// target table's DEFAULT / sequence / identity / generated-stored
    /// machinery to omitted columns exactly as a normal `INSERT t(col-list)`
    /// does — via [`ModifyTable::with_insert_column_map`] and the default
    /// metadata setters.
    ///
    /// `stream_schema` is the projected schema of `columns`; `payloads` are
    /// narrow rows encoded against it. The operator expands each narrow row to
    /// full width, fills omitted cells from the default metadata, evaluates
    /// generated columns, then runs NOT NULL / CHECK / FK / UNIQUE / EXCLUDE
    /// and index maintenance — identical to INSERT.
    pub(in crate::session) fn flush_copy_insert_batch_with_defaults(
        &self,
        entry: &TableEntry,
        columns: &[usize],
        stream_schema: &Schema,
        payloads: &[Vec<u8>],
        txn: &Transaction,
    ) -> Result<(), ServerError> {
        if payloads.is_empty() {
            return Ok(());
        }
        let stream_codec = RowCodec::new(stream_schema.clone());
        let mut rows = Vec::with_capacity(payloads.len());
        for payload in payloads {
            let row = stream_codec.decode(payload).map_err(|e| {
                ServerError::ddl(format!("COPY FROM narrow row decode for defaults: {e}"))
            })?;
            rows.push(row);
        }
        let batch = build_batch(&rows, stream_schema)?;
        let child: Box<dyn Operator> =
            Box::new(MemTableScan::new(stream_schema.clone(), vec![batch]));

        let mut modify = self
            .build_copy_modify(entry, txn, child)?
            .with_insert_column_map(columns.to_vec());

        // Same default metadata the INSERT lowering attaches (see
        // `lower_real_insert`): the operator applies these to columns the COPY
        // column list omitted, keyed off the column map's `omitted` mask.
        if let Some(constraints) = self.state.table_constraints.get(&entry.oid) {
            let constraints = constraints.clone();
            modify = modify
                .with_column_defaults(constraints.defaults.clone())
                .with_sequence_defaults(
                    self.build_copy_sequence_defaults(&constraints.sequence_defaults, txn)?,
                )
                .with_identity_always(constraints.identity_always.clone())
                .with_generated_stored(constraints.generated_stored.clone());
        }

        let mut op: Box<dyn Operator> = Box::new(modify);
        while op.next_batch()?.is_some() {}
        self.state.flush_dirty_heap_pages_if_needed()?;
        Ok(())
    }

    /// Build the shared INSERT [`ModifyTable`] for a COPY batch: visibility
    /// map, uniqueness recheck, secondary/vector index maintenance, and
    /// CHECK / FOREIGN KEY / EXCLUDE constraints — the same machinery a normal
    /// INSERT uses. Callers attach the child operator and (for the column-list
    /// path) the column map plus default metadata.
    fn build_copy_modify(
        &self,
        entry: &TableEntry,
        txn: &Transaction,
        child: Box<dyn Operator>,
    ) -> Result<ModifyTable<BlankPageLoader>, ServerError> {
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

        Ok(modify)
    }

    /// Build the per-column [`SequenceDefault`] descriptors for a COPY default
    /// batch, mirroring the INSERT lowering's `build_sequence_defaults`: each
    /// named sequence is looked up in the session's sequence map, WAL-stamped
    /// with the COPY xid, and given the session observer that records the
    /// generated value for `currval`/`lastval`.
    fn build_copy_sequence_defaults(
        &self,
        defaults: &[Option<String>],
        txn: &Transaction,
    ) -> Result<Vec<Option<SequenceDefault>>, ServerError> {
        let sequence_state = self.sequence_state.clone();
        let observer: SequenceNextvalObserver = Arc::new(move |name: &str, value| {
            sequence_state.record_nextval(name, value);
        });
        let wal = self.state.heap.wal_sink().cloned();
        let xid = txn.current_xid();
        defaults
            .iter()
            .map(|name| {
                let Some(name) = name else {
                    return Ok(None);
                };
                let sequence = self
                    .state
                    .sequences
                    .get(name)
                    .map(|seq| Arc::clone(seq.value()))
                    .ok_or_else(|| {
                        ServerError::Catalog(ultrasql_catalog::CatalogError::not_found(
                            name.clone(),
                        ))
                    })?;
                let default = SequenceDefault::new(name.clone(), sequence)
                    .with_wal(wal.clone(), xid, ultrasql_core::RelationId::INVALID)
                    .with_observer(Arc::clone(&observer));
                Ok(Some(default))
            })
            .collect()
    }
}
