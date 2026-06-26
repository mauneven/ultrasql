//! Runtime index/aggregating-index metadata, validation report types, and
//! runtime constraint descriptors.
//!
//! Moved verbatim from the crate root; behavior unchanged.
use super::*;

/// Runtime metadata for one index beyond plain attnum keys.
#[derive(Clone, Debug, Default)]
pub struct RuntimeIndexMetadata {
    /// Bound key expressions. Empty means use the catalog entry's key columns.
    pub key_exprs: Vec<ScalarExpr>,
    /// Bound partial-index predicate, if any.
    pub predicate: Option<ScalarExpr>,
    /// 0-based table columns listed in `INCLUDE (...)`.
    pub include_columns: Vec<usize>,
    /// Access method requested by `USING`.
    pub method: LogicalIndexMethod,
    /// In-memory BRIN min/max summaries for block-range pruning.
    pub brin: Option<Arc<ultrasql_storage::access_method::BrinIndex>>,
    /// Page-backed HNSW graph for vector top-k scans.
    pub hnsw: Option<Arc<ultrasql_storage::access_method::PageBackedHnswIndex>>,
    /// Page-backed IVFFlat inverted lists for vector top-k scans.
    pub ivfflat: Option<Arc<ultrasql_storage::access_method::PageBackedIvfFlatIndex>>,
    /// Runtime aggregating-index summary for dashboard-style GROUP BY scans.
    pub aggregating: Option<Arc<RuntimeAggregatingIndex>>,
}

/// Process-wide ANN/vector-index counters for ops metrics.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AnnSystemMetrics {
    /// Approximate candidate count available across runtime ANN sidecars.
    pub candidates: u64,
    /// Tombstoned ANN entries waiting for VACUUM cleanup.
    pub tombstones: u64,
    /// Approximate memory footprint of page-backed vector-index pages.
    pub vector_index_memory_bytes: u64,
    /// Number of runtime HNSW indexes.
    pub hnsw_indexes: u64,
    /// Number of runtime IVFFlat indexes.
    pub ivfflat_indexes: u64,
}

/// One admin validation check result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidationCheck {
    /// Stable machine-readable check name.
    pub name: &'static str,
    /// Check outcome.
    pub status: ValidationStatus,
    /// Human-readable evidence for the outcome.
    pub detail: String,
}

/// Admin validation status.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ValidationStatus {
    /// Check passed.
    Ok,
    /// Check failed and should make `ultrasql validate` exit non-zero.
    Failed,
}

impl ValidationStatus {
    /// Lowercase status for CLI output.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Failed => "failed",
        }
    }
}

/// Full admin validation report.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ValidationReport {
    /// Ordered checks run by the validator.
    pub checks: Vec<ValidationCheck>,
}

impl ValidationReport {
    /// Return true when every check passed.
    #[must_use]
    pub fn is_ok(&self) -> bool {
        self.checks
            .iter()
            .all(|check| check.status == ValidationStatus::Ok)
    }
}

/// Runtime sidecar for `CREATE AGGREGATING INDEX`.
#[derive(Debug)]
pub struct RuntimeAggregatingIndex {
    /// Bound aggregating-index metadata.
    pub spec: ultrasql_planner::LogicalAggregatingIndex,
    /// Materialized summary rows in `group columns + aggregates` order.
    pub rows: std::sync::RwLock<Vec<Vec<Value>>>,
    /// Set when DML touched the base table after the last summary build.
    pub dirty: std::sync::atomic::AtomicBool,
    /// XID of the transaction whose write last dirtied this summary, recorded
    /// at `mark_dirty` time (before that writer terminates). Mirrors the
    /// column cache's `last_writer_xid`: the serve gate consults it together
    /// with the same [`ultrasql_mvcc::XidStatusOracle`] the heap visibility
    /// path uses, so a summary rebuilt from an ABORTED writer's own
    /// uncommitted snapshot is never served to a fresh reader as a phantom
    /// aggregate. `Xid::INVALID` means "no recorded in-txn writer" (clean /
    /// restart-rebuilt / committed-maintenance state), which the gate treats
    /// as always servable.
    last_writer_xid: std::sync::atomic::AtomicU64,
    pub(crate) explain_stats: RuntimeAggregatingIndexExplainStats,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct AggregatingIndexExplainStats {
    pub aggregating_index_used: bool,
    pub stale_rebuild_used: bool,
    pub summary_rows_read: u64,
    pub base_rows_skipped: u64,
}

#[derive(Debug, Default)]
pub(crate) struct RuntimeAggregatingIndexExplainStats {
    aggregating_index_used: std::sync::atomic::AtomicBool,
    stale_rebuild_used: std::sync::atomic::AtomicBool,
    summary_rows_read: std::sync::atomic::AtomicU64,
    base_rows_skipped: std::sync::atomic::AtomicU64,
}

impl RuntimeAggregatingIndex {
    /// Build a clean runtime summary.
    #[must_use]
    pub fn new(spec: ultrasql_planner::LogicalAggregatingIndex, rows: Vec<Vec<Value>>) -> Self {
        Self {
            spec,
            rows: std::sync::RwLock::new(rows),
            dirty: std::sync::atomic::AtomicBool::new(false),
            last_writer_xid: std::sync::atomic::AtomicU64::new(Xid::INVALID.raw()),
            explain_stats: RuntimeAggregatingIndexExplainStats::default(),
        }
    }

    /// Mark summary rows stale. Next matching read rebuilds lazily.
    ///
    /// Records no in-txn writer (`Xid::INVALID`): the serve gate then treats
    /// any rebuilt summary as servable. Used by maintenance/EXPLAIN paths and
    /// tests that only want to force a stale rebuild. The DML write path uses
    /// [`Self::mark_dirty_with_writer`] so the aborted-writer gate can reject
    /// a phantom summary.
    pub fn mark_dirty(&self) {
        self.mark_dirty_with_writer(Xid::INVALID);
    }

    /// Mark summary rows stale and record `writer` as the transaction whose
    /// write dirtied it.
    ///
    /// Called at physical-mutation (`mark_dirty`) time from the DML lowerer,
    /// before the writer terminates — exactly like the column cache stamps
    /// `last_writer_xid`. The serve gate (`summary_servable_to`) later refuses
    /// to hand a non-self reader a summary that was rebuilt from `writer`'s own
    /// uncommitted snapshot unless `writer` is committed per the shared status
    /// oracle.
    ///
    /// The recorded writer monotonically advances to the newest dirtying XID;
    /// recording `Xid::INVALID` never clobbers a real writer (a no-arg
    /// [`Self::mark_dirty`] from a maintenance path must not erase the abort
    /// guard for an in-flight DML writer).
    pub fn mark_dirty_with_writer(&self, writer: Xid) {
        use std::sync::atomic::Ordering;
        if writer != Xid::INVALID {
            // Advance to the newest writer; never move backwards.
            let _ = self
                .last_writer_xid
                .fetch_max(writer.raw(), Ordering::AcqRel);
        }
        // Publish the writer before the dirty flag so a concurrent reader that
        // observes `dirty == true` also observes the matching writer.
        self.dirty.store(true, Ordering::Release);
    }

    /// The transaction whose write last dirtied this summary, or
    /// `Xid::INVALID` if none recorded.
    pub(crate) fn last_writer_xid(&self) -> Xid {
        Xid::new(
            self.last_writer_xid
                .load(std::sync::atomic::Ordering::Acquire),
        )
    }

    /// Clear the recorded in-txn writer once the summary reflects committed
    /// heap truth (a fresh rebuild from a non-writer snapshot). After this the
    /// gate treats the summary as unconditionally servable.
    pub(crate) fn clear_last_writer(&self) {
        self.last_writer_xid
            .store(Xid::INVALID.raw(), std::sync::atomic::Ordering::Release);
    }

    /// Whether the current (clean) summary may be served to `snapshot`,
    /// consulting `oracle` for the recorded writer's commit status.
    ///
    /// Mirrors `ColumnCache::is_snapshot_coherent`'s writer predicate exactly:
    /// a summary whose `last_writer == W` is servable iff
    ///
    /// ```text
    /// W == Xid::INVALID
    ///   || snapshot.is_current_xid(W)                       // own read-after-write
    ///   || ( !snapshot.xid_in_progress(W)                   // within the reader's horizon
    ///        && oracle.is_committed(W) )                     // and really committed
    /// ```
    ///
    /// The `is_committed` half is the abort backstop: a writer that warmed the
    /// summary from its own read-after-write view and then ABORTED (plain
    /// `ROLLBACK`, `ROLLBACK PREPARED`, savepoint rollback, or an SSI
    /// force-abort) is no longer in progress, so `!xid_in_progress` alone
    /// would wrongly admit its phantom aggregate. Requiring the writer to be
    /// committed per the **same** [`ultrasql_mvcc::XidStatusOracle`] the heap
    /// `scan_visible` path consults means the summary and the heap can never
    /// disagree about the writer's fate; an aborted writer forces a fresh
    /// rebuild from committed heap truth (which itself skips the aborted
    /// tuples). The `!xid_in_progress` half is the read-side guard preserved
    /// from the column cache: a snapshot frozen before the writer committed
    /// must not consume a summary that includes rows committed after it.
    pub(crate) fn summary_servable_to(
        &self,
        snapshot: &ultrasql_mvcc::Snapshot,
        oracle: &dyn ultrasql_mvcc::XidStatusOracle,
    ) -> bool {
        let writer = self.last_writer_xid();
        writer == Xid::INVALID
            || snapshot.is_current_xid(writer)
            || (!snapshot.xid_in_progress(writer) && oracle.is_committed(writer))
    }

    pub(crate) fn record_explain_read(
        &self,
        stale_rebuild_used: bool,
        summary_rows_read: usize,
        base_rows_skipped: u64,
    ) {
        self.explain_stats
            .aggregating_index_used
            .store(true, std::sync::atomic::Ordering::Release);
        self.explain_stats
            .stale_rebuild_used
            .store(stale_rebuild_used, std::sync::atomic::Ordering::Release);
        self.explain_stats.summary_rows_read.store(
            u64::try_from(summary_rows_read).unwrap_or(u64::MAX),
            std::sync::atomic::Ordering::Release,
        );
        self.explain_stats
            .base_rows_skipped
            .store(base_rows_skipped, std::sync::atomic::Ordering::Release);
    }

    pub(crate) fn explain_stats_snapshot(&self) -> AggregatingIndexExplainStats {
        AggregatingIndexExplainStats {
            aggregating_index_used: self
                .explain_stats
                .aggregating_index_used
                .load(std::sync::atomic::Ordering::Acquire),
            stale_rebuild_used: self
                .explain_stats
                .stale_rebuild_used
                .load(std::sync::atomic::Ordering::Acquire),
            summary_rows_read: self
                .explain_stats
                .summary_rows_read
                .load(std::sync::atomic::Ordering::Acquire),
            base_rows_skipped: self
                .explain_stats
                .base_rows_skipped
                .load(std::sync::atomic::Ordering::Acquire),
        }
    }
}

/// One runtime CHECK constraint.
#[derive(Clone, Debug)]
pub struct RuntimeCheckConstraint {
    /// Constraint name reported on violation.
    pub name: String,
    /// Boolean expression bound against the table row schema.
    pub expr: ScalarExpr,
}

/// One runtime FOREIGN KEY constraint.
#[derive(Clone, Debug)]
pub struct RuntimeForeignKeyConstraint {
    /// Constraint name reported on violation.
    pub name: String,
    /// Referencing table column indices.
    pub columns: Vec<usize>,
    /// Referenced table name.
    pub target_table: String,
    /// Referenced table OID.
    pub target_oid: ultrasql_core::Oid,
    /// Referenced table column indices.
    pub target_columns: Vec<usize>,
    /// Action when a referenced row is deleted.
    pub on_delete: ultrasql_planner::LogicalReferentialAction,
    /// Action when a referenced key is updated.
    pub on_update: ultrasql_planner::LogicalReferentialAction,
    /// Whether this constraint may be checked at transaction commit.
    pub deferrable: bool,
    /// Whether this deferrable constraint starts in deferred mode.
    pub initially_deferred: bool,
}

/// One runtime EXCLUDE constraint.
#[derive(Clone, Debug)]
pub struct RuntimeExclusionConstraint {
    /// Constraint name reported on violation.
    pub name: String,
    /// Access method requested by `USING`.
    pub method: LogicalIndexMethod,
    /// 0-based table column indices plus operators.
    pub elements: Vec<RuntimeExclusionElement>,
}

/// One runtime EXCLUDE element.
#[derive(Clone, Debug)]
pub struct RuntimeExclusionElement {
    /// Table column index.
    pub column: usize,
    /// Operator applied to `(new_value, existing_value)`.
    pub op: BinaryOp,
}

#[cfg(test)]
mod aggregating_index_gate_tests {
    //! Gate-level unit tests for the aggregating-index serve predicate
    //! [`RuntimeAggregatingIndex::summary_servable_to`], mirroring
    //! `column_cache`'s `aborted_writer_projection_is_rejected` family (commit
    //! b4d3e302). These prove the own-read / committed / aborted / in-progress
    //! matrix directly, independent of *how* a writer reached its CLOG status
    //! — so the SSI force-abort flavor (which lands here as
    //! `XidStatus::Aborted`) is covered without driving SSI over the wire.

    use std::collections::HashMap;

    use parking_lot::RwLock;
    use ultrasql_core::{CommandId, Xid};
    use ultrasql_mvcc::{Snapshot, XidStatus, XidStatusOracle};
    use ultrasql_planner::LogicalAggregatingIndex;

    use super::RuntimeAggregatingIndex;

    /// In-crate stand-in for the CLOG-backed oracle. Defaults unset XIDs to
    /// `InProgress`, exactly like `TransactionManager` for an XID with no
    /// terminal CLOG entry.
    #[derive(Default)]
    struct MapOracle {
        states: RwLock<HashMap<Xid, XidStatus>>,
    }

    impl MapOracle {
        fn new() -> Self {
            Self::default()
        }

        fn set_committed(&self, xid: Xid) {
            self.states.write().insert(xid, XidStatus::Committed);
        }

        fn set_aborted(&self, xid: Xid) {
            self.states.write().insert(xid, XidStatus::Aborted);
        }

        fn set_in_progress(&self, xid: Xid) {
            self.states.write().insert(xid, XidStatus::InProgress);
        }
    }

    impl XidStatusOracle for MapOracle {
        fn status(&self, xid: Xid) -> XidStatus {
            *self
                .states
                .read()
                .get(&xid)
                .unwrap_or(&XidStatus::InProgress)
        }
    }

    fn empty_index(writer: Xid) -> RuntimeAggregatingIndex {
        let runtime = RuntimeAggregatingIndex::new(
            LogicalAggregatingIndex {
                group_columns: vec![0],
                aggregates: vec![],
            },
            Vec::new(),
        );
        if writer != Xid::INVALID {
            runtime.mark_dirty_with_writer(writer);
        }
        runtime
    }

    /// Reader snapshot with an empty in-progress set (the quiescent case the
    /// summary targets) and `current_xid`.
    fn reader_snapshot(current_xid: Xid) -> Snapshot {
        Snapshot::new(
            Xid::new(1),
            Xid::new(1_000),
            current_xid,
            CommandId::FIRST,
            std::iter::empty(),
        )
    }

    /// Preserved case (b): a COMMITTED writer's summary is still servable to a
    /// fresh (non-self) reader — no over-rejection.
    #[test]
    fn committed_writer_summary_is_served() {
        let writer = Xid::new(100);
        let runtime = empty_index(writer);
        let oracle = MapOracle::new();
        oracle.set_committed(writer);

        let snap = reader_snapshot(Xid::INVALID);
        assert!(
            runtime.summary_servable_to(&snap, &oracle),
            "a committed writer's summary must be servable"
        );
    }

    /// The NEW rejection: an ABORTED writer's summary is NEVER servable to a
    /// fresh reader, regardless of how it aborted (plain ROLLBACK, ROLLBACK
    /// PREPARED, savepoint rollback, or SSI force-abort all land here as
    /// `XidStatus::Aborted` in the same CLOG oracle the gate consults).
    #[test]
    fn aborted_writer_summary_is_rejected() {
        let writer = Xid::new(100);
        let runtime = empty_index(writer);
        let oracle = MapOracle::new();
        oracle.set_aborted(writer);

        let snap = reader_snapshot(Xid::INVALID);
        assert!(
            !runtime.summary_servable_to(&snap, &oracle),
            "an aborted writer's summary must not be servable (phantom-aggregate hole)"
        );
    }

    /// Preserved case (a): the writer reading its OWN uncommitted (still
    /// in-progress) summary is served via `is_current_xid`, without consulting
    /// commit status.
    #[test]
    fn own_in_progress_writer_reads_its_summary() {
        let writer = Xid::new(100);
        let runtime = empty_index(writer);
        let oracle = MapOracle::new();
        oracle.set_in_progress(writer);

        // The reader IS the writer: current_xid == writer.
        let snap = reader_snapshot(writer);
        assert!(
            runtime.summary_servable_to(&snap, &oracle),
            "the writer must see its own in-progress summary (read-after-write)"
        );
    }

    /// Preserved case (c): an in-progress OTHER writer is rejected. With an
    /// empty in-progress set a non-self reader's snapshot was taken when no
    /// other txn was running, so an in-progress writer is not committed and
    /// the committed-status check rejects it.
    #[test]
    fn in_progress_other_writer_is_rejected() {
        let writer = Xid::new(100);
        let runtime = empty_index(writer);
        let oracle = MapOracle::new();
        oracle.set_in_progress(writer);

        let snap = reader_snapshot(Xid::INVALID);
        assert!(
            !runtime.summary_servable_to(&snap, &oracle),
            "an in-progress other writer must not be servable"
        );
    }

    /// A snapshot frozen *before* a now-committed writer must NOT consume that
    /// writer's summary: the `!xid_in_progress` half of the gate survives the
    /// committed-status addition (read-side guard for a frozen RR reader).
    #[test]
    fn committed_writer_newer_than_snapshot_is_rejected() {
        let writer = Xid::new(500);
        let runtime = empty_index(writer);
        let oracle = MapOracle::new();
        oracle.set_committed(writer);

        // Snapshot frozen with xmax=200: writer 500 is newer than the horizon.
        let snap = Snapshot::new(
            Xid::new(1),
            Xid::new(200),
            Xid::INVALID,
            CommandId::FIRST,
            std::iter::empty(),
        );
        assert!(
            !runtime.summary_servable_to(&snap, &oracle),
            "a snapshot frozen before a committed writer must not consume its summary"
        );
    }

    /// A summary with no recorded writer (`Xid::INVALID`) is always servable —
    /// the oracle is not consulted. Covers the clean / restart-rebuilt /
    /// maintenance-rebuilt state.
    #[test]
    fn no_recorded_writer_is_always_servable() {
        let runtime = empty_index(Xid::INVALID);
        let oracle = MapOracle::new();
        let snap = reader_snapshot(Xid::INVALID);
        assert!(runtime.summary_servable_to(&snap, &oracle));
    }

    /// `mark_dirty_with_writer` advances to the newest writer and never moves
    /// backwards, and a no-arg `mark_dirty` (INVALID) does not erase a real
    /// recorded writer's abort guard.
    #[test]
    fn recorded_writer_advances_monotonically() {
        let runtime = empty_index(Xid::INVALID);
        runtime.mark_dirty_with_writer(Xid::new(10));
        assert_eq!(runtime.last_writer_xid(), Xid::new(10));
        // Older writer does not clobber the newer one.
        runtime.mark_dirty_with_writer(Xid::new(5));
        assert_eq!(runtime.last_writer_xid(), Xid::new(10));
        // Newer writer advances it.
        runtime.mark_dirty_with_writer(Xid::new(20));
        assert_eq!(runtime.last_writer_xid(), Xid::new(20));
        // No-arg mark_dirty (INVALID) must not erase the guard.
        runtime.mark_dirty();
        assert_eq!(runtime.last_writer_xid(), Xid::new(20));
        // Explicit clear resets to INVALID.
        runtime.clear_last_writer();
        assert_eq!(runtime.last_writer_xid(), Xid::INVALID);
    }
}
