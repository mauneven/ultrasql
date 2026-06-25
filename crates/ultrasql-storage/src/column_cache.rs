//! Per-relation columnar projection cache.
//!
//! The row-store heap is the authoritative source of truth (MVCC,
//! WAL, durability all live there). For analytical SELECTs over an
//! unchanged relation that source pays per-tuple decode on every
//! `SeqScan` — proportional to row count and dominant in the
//! 1M-row OLAP bench. A column store (DuckDB, ClickHouse) avoids
//! this by physically arranging data column-by-column on disk.
//!
//! [`ColumnCache`] interposes a lazy in-memory columnar projection
//! between the heap and the executor:
//!
//! - First `SeqScan` over a relation walks the heap normally **and**
//!   accumulates the decoded columns into an [`Arc<CachedColumns>`]
//!   entry keyed on the relation's monotonically-increasing
//!   relation version.
//! - Server maintenance can also build the same entry after committed
//!   DML, warming the columnar layout before the next OLAP query.
//! - Subsequent `SeqScan`s over the **same** relation, **same**
//!   version, skip the heap walk entirely and stream batches
//!   directly from the cached columns.
//! - Any mutation on the relation (`heap.insert` / `heap.update` /
//!   `heap.delete` and their bulk variants) bumps the version,
//!   which makes existing cache entries unreachable; the next read
//!   re-builds from heap.
//!
//! # Correctness under MVCC
//!
//! The cache stores the **set of live tuples observed by the
//! cache-building scan at its snapshot**. A subsequent scan with a
//! newer snapshot must see at least those rows (the older snapshot's
//! visible tuples remain visible to any later snapshot under MVCC
//! semantics) **provided** no mutation has happened since cache
//! build. The version stamp encodes that "no mutation" predicate:
//! we increment it on any insert/update/delete, so cache hits only
//! return data the heap has not modified since the build, and the
//! subsequent snapshot sees the same row set the build snapshot saw.
//!
//! Concurrent writes during a build are handled by the same
//! mechanism: the writer bumps the version, the build's `put` is
//! rejected (or overwrites a stale entry which will be invalidated
//! again on the next read). Worst case the cache is rebuilt; never
//! wrong.

use std::sync::Arc;

use ahash::AHashMap;
use parking_lot::RwLock;
use ultrasql_core::{DataType, RelationId, Schema, Value, Xid};
use ultrasql_mvcc::{Snapshot, XidStatusOracle};
use ultrasql_vec::column::Column;

/// Target row count per in-memory columnar segment.
pub const DEFAULT_COLUMNAR_SEGMENT_ROWS: usize = 65_536;

/// Version-scoped key for cached scalar aggregate wire bodies.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum CachedScalarAggregateWireKey {
    /// `SELECT SUM(col)` over an unchanged cached relation.
    Sum {
        output_name: String,
        input_type_tag: u8,
        sum_col: usize,
    },
    /// `SELECT AVG(col)` over an unchanged cached relation.
    Avg {
        output_name: String,
        input_type_tag: u8,
        sum_col: usize,
    },
    /// `SELECT SUM(sum_col) FROM t WHERE pred_col op lit`.
    FilterSum {
        output_name: String,
        input_type_tag: u8,
        sum_col: usize,
        predicate_col: usize,
        predicate_op_tag: u8,
        predicate_lit: i64,
    },
}

/// Version-scoped key for cached physical projection summaries.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct CachedGroupedProjectionWireKey {
    /// Source-table column indices used as GROUP BY keys.
    pub group_columns: Vec<usize>,
    /// Aggregate slots computed after the group columns.
    pub aggregates: Vec<CachedGroupedProjectionAggregateKey>,
    /// Output schema signature. Names matter because the cached body includes
    /// the wire RowDescription.
    pub output_fields: Vec<CachedGroupedProjectionFieldKey>,
    /// ORDER BY columns resolved against the projected output.
    pub order_by: Vec<CachedGroupedProjectionOrderKey>,
}

/// Output-field identity for projection summary wire reuse.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct CachedGroupedProjectionFieldKey {
    /// Output column name.
    pub name: String,
    /// Output column type.
    pub data_type: DataType,
    /// Whether the output column is nullable.
    pub nullable: bool,
}

/// One aggregate slot in a cached projection summary.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum CachedGroupedProjectionAggregateKey {
    /// `COUNT(*)`.
    CountStar,
    /// `COUNT(col)`.
    Count {
        /// Source column index.
        column: usize,
        /// Source column type.
        data_type: DataType,
    },
    /// `SUM(col)`.
    Sum {
        /// Source column index.
        column: usize,
        /// Source column type.
        data_type: DataType,
    },
}

/// ORDER BY column in a cached projection summary.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct CachedGroupedProjectionOrderKey {
    /// Output column index.
    pub output_index: usize,
    /// `true` for ascending order.
    pub asc: bool,
    /// `true` when NULL sorts first.
    pub nulls_first: bool,
}

/// Cached rows and optional Simple Query wire body for a physical projection
/// summary.
#[derive(Debug)]
pub struct CachedGroupedProjectionWire {
    /// Grouped output rows in logical value form. Extended Query can reuse
    /// these rows while honoring per-column text/binary format codes.
    pub rows: Arc<[Vec<Value>]>,
    /// RowDescription + DataRow* + CommandComplete for Simple Query reuse.
    pub text_body: RwLock<Option<Arc<[u8]>>>,
}

/// Cached column projection for a relation at a specific version.
#[derive(Debug)]
pub struct CachedColumns {
    /// Version of the relation when this entry was built. Bumping
    /// the relation's version through [`ColumnCache::bump_version`]
    /// makes subsequent [`ColumnCache::get`] calls miss.
    pub version: u64,
    /// Logical schema of the cached columns (relation schema, no
    /// TID prefix).
    pub schema: Schema,
    /// Per-column values for **every live tuple** observed by the
    /// cache-building scan. `columns[i]` has the same length for
    /// every `i` and equals the row count of the cached projection.
    pub columns: Vec<Column>,
    /// Logical row counts for each columnar segment.
    ///
    /// The current v1.0 slice stores each column as one contiguous
    /// typed buffer and records segment boundaries here. Scan replay
    /// can keep slicing from the contiguous buffers while maintenance,
    /// stats, and future spill-to-disk code see a real segmented
    /// layout contract.
    pub segment_row_counts: Vec<usize>,
    /// Lazily-populated pre-encoded wire body for the exact
    /// `(Int32, Int32)` identity full-scan shape over this schema
    /// and relation version. Kept separate from `columns` so other
    /// scan shapes pay no extra work.
    pub cached_int32_pair_select_wire: RwLock<Option<Arc<[u8]>>>,
    /// Lazily-populated pre-encoded wire bodies for scalar aggregate
    /// queries answered directly from these cached columns. Keyed by
    /// output shape + predicate so repeated Simple Query executions on
    /// an unchanged relation can skip both heap access and aggregate
    /// recomputation.
    pub cached_scalar_aggregate_wire: RwLock<AHashMap<CachedScalarAggregateWireKey, Arc<[u8]>>>,
    /// Lazily-populated pre-encoded wire bodies for grouped aggregate
    /// summaries. Each entry is scoped by this relation's version through the
    /// surrounding [`CachedColumns`] object.
    pub cached_grouped_projection_wire:
        RwLock<AHashMap<CachedGroupedProjectionWireKey, Arc<CachedGroupedProjectionWire>>>,
}

impl CachedColumns {
    /// Build a cached column projection with an empty lazy wire-body cache.
    #[must_use]
    pub fn new(version: u64, schema: Schema, columns: Vec<Column>) -> Self {
        let row_count = columns.first().map(Column::len).unwrap_or(0);
        Self::new_segmented(
            version,
            schema,
            columns,
            columnar_segment_row_counts(row_count, DEFAULT_COLUMNAR_SEGMENT_ROWS),
        )
    }

    /// Build a cached column projection with caller-supplied segment
    /// boundaries.
    #[must_use]
    pub fn new_segmented(
        version: u64,
        schema: Schema,
        columns: Vec<Column>,
        segment_row_counts: Vec<usize>,
    ) -> Self {
        Self {
            version,
            schema,
            columns,
            segment_row_counts,
            cached_int32_pair_select_wire: RwLock::new(None),
            cached_scalar_aggregate_wire: RwLock::new(AHashMap::new()),
            cached_grouped_projection_wire: RwLock::new(AHashMap::new()),
        }
    }

    /// Return number of rows stored in this columnar projection.
    #[must_use]
    pub fn row_count(&self) -> usize {
        self.columns.first().map(Column::len).unwrap_or(0)
    }

    /// Return number of columnar segments backing this projection.
    #[must_use]
    pub fn segment_count(&self) -> usize {
        self.segment_row_counts.len()
    }
}

fn columnar_segment_row_counts(row_count: usize, segment_rows: usize) -> Vec<usize> {
    if row_count == 0 {
        return Vec::new();
    }
    let segment_rows = segment_rows.max(1);
    let mut out = Vec::with_capacity(row_count.div_ceil(segment_rows));
    let mut remaining = row_count;
    while remaining > 0 {
        let n = remaining.min(segment_rows);
        out.push(n);
        remaining -= n;
    }
    out
}

/// Per-`HeapAccess` columnar cache.
///
/// Holds two pieces of state:
///
/// - A monotone per-relation version counter (`AtomicU64` behind a
///   sharded map). Read on cache lookup; bumped on every mutation
///   the heap performs against that relation.
/// - A version-stamped entry per relation. Cache lookups validate
///   the entry's `version` against the relation's current version
///   and miss when they differ.
#[derive(Debug, Default)]
pub struct ColumnCache {
    inner: RwLock<ColumnCacheInner>,
}

#[derive(Debug, Default)]
struct ColumnCacheInner {
    versions: AHashMap<RelationId, u64>,
    entries: AHashMap<RelationId, Arc<CachedColumns>>,
    /// Highest XID that has mutated each relation since process start
    /// (monotone per relation). Used as the column-cache **coherence
    /// witness**: a scan may only publish a shared projection when its
    /// building snapshot can see this writer. See
    /// [`ColumnCache::last_writer_xid`].
    last_writer_xid: AHashMap<RelationId, Xid>,
}

impl ColumnCache {
    /// Construct an empty cache.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Return the current monotonic version for `rel`. Starts at
    /// zero for a relation that has never been mutated.
    #[must_use]
    pub fn relation_version(&self, rel: RelationId) -> u64 {
        let g = self.inner.read();
        *g.versions.get(&rel).unwrap_or(&0)
    }

    /// Bump the version of `rel` and drop any cached entry for it.
    ///
    /// Called by every heap mutation path
    /// (`insert` / `update` / `delete` / bulk variants) with the
    /// `writer_xid` performing the mutation. The version is bumped at
    /// **physical mutation time**, which is *before* the writer
    /// commits; `writer_xid` is recorded as the relation's
    /// [`last_writer_xid`](Self::last_writer_xid) so a later scan can
    /// refuse to publish a projection from a snapshot that cannot yet
    /// see this writer (see the coherence guard in `SeqScan::build`).
    pub fn bump_version(&self, rel: RelationId, writer_xid: Xid) {
        let mut g = self.inner.write();
        let v = g.versions.entry(rel).or_insert(0);
        *v = v.saturating_add(1);
        g.entries.remove(&rel);
        let last = g.last_writer_xid.entry(rel).or_insert(Xid::INVALID);
        if writer_xid > *last {
            *last = writer_xid;
        }
    }

    /// Highest XID that has mutated `rel` since process start, or
    /// [`Xid::INVALID`] if `rel` has never been mutated.
    ///
    /// A scan may only **publish** a shared columnar projection for
    /// `rel` when its building snapshot can see this writer: otherwise
    /// the relation's version already reflects rows (or deletions) the
    /// snapshot cannot observe, so the projection would be incoherent
    /// for the newer readers the version-keyed cache serves it to.
    #[must_use]
    pub fn last_writer_xid(&self, rel: RelationId) -> Xid {
        let g = self.inner.read();
        g.last_writer_xid.get(&rel).copied().unwrap_or(Xid::INVALID)
    }

    /// Look up the cached projection for `rel` at the current
    /// version. Returns `None` on cache miss (no entry or stale
    /// version).
    ///
    /// # Coherence
    ///
    /// This accessor performs **no MVCC visibility check**: the returned
    /// projection is the raw set of live tuples one building scan observed
    /// at the relation's current version, with no per-snapshot qualifier.
    /// It is sound only for a caller that has *already* established its
    /// operating snapshot may consume the cache for `rel` (see
    /// [`Self::is_snapshot_coherent`]), or for a caller that does not serve
    /// MVCC results from it (maintenance / warming / stats).
    ///
    /// Any path that returns rows to a client **must** use
    /// [`Self::get_for_snapshot`] instead, which folds the coherence gate
    /// in. Replaying this entry RAW for a snapshot that does not reflect
    /// exactly the committed state at this version dirty-reads or hides
    /// rows (see the module docs and `SeqScan`'s coherence guard).
    #[must_use]
    pub fn get(&self, rel: RelationId) -> Option<Arc<CachedColumns>> {
        let g = self.inner.read();
        let current = *g.versions.get(&rel).unwrap_or(&0);
        g.entries
            .get(&rel)
            .filter(|e| e.version == current)
            .cloned()
    }

    /// Whether `snapshot` may both **publish** to and **consume** from the
    /// shared, version-keyed projection for `rel`.
    ///
    /// The cache stores one projection per relation version and is replayed
    /// RAW — no per-tuple visibility re-filtering. A snapshot may therefore
    /// touch the cache only when it provably reflects **exactly the
    /// committed state** at that version. The sufficient condition is:
    ///
    /// ```text
    /// snapshot.xip().is_empty()
    ///   && ( last_writer == Xid::INVALID
    ///        || snapshot.is_current_xid(last_writer)
    ///        || ( !snapshot.xid_in_progress(last_writer)
    ///             && oracle.is_committed(last_writer) ) )
    /// ```
    ///
    /// - `xip().is_empty()` — no *other* transaction was in progress when
    ///   this snapshot was taken. No in-progress writer can be silently
    ///   missed, including a writer with a *lower* xid than the recorded
    ///   (max) `last_writer_xid` (the multi-writer hole). It also closes the
    ///   dirty-read hole: while a writer X is in progress, every *other*
    ///   reader has X in its in-progress set, so every other reader's gate
    ///   fails and it walks the heap rather than consuming a projection X
    ///   published from its own read-after-write snapshot.
    /// - `!xid_in_progress(last_writer)` — the snapshot is not *behind* the
    ///   latest writer reflected in the version: a reader frozen before that
    ///   writer committed has it in-progress (or newer than `xmax`), so the
    ///   gate fails and the reader walks the heap — never consuming a
    ///   projection that includes rows committed after its snapshot.
    /// - `oracle.is_committed(last_writer)` — the **abort backstop**. The
    ///   relation's version is bumped at *physical* mutation time — before
    ///   the writer terminates — and the recorded `last_writer_xid` reflects
    ///   that writer. A writer that warmed the cache from its own
    ///   read-after-write uncommitted view and then **aborted** (plain
    ///   `ROLLBACK`, `ROLLBACK PREPARED`, or an SSI force-abort) is no longer
    ///   in progress, so `!xid_in_progress` alone would wrongly admit its
    ///   phantom rows. Requiring the writer to be **committed** per the same
    ///   [`XidStatusOracle`] the heap visibility path (`scan_visible` /
    ///   [`ultrasql_mvcc::is_visible`]) consults means the cache and the heap
    ///   can never disagree about the writer's fate; an aborted writer forces
    ///   a fresh heap scan, which itself skips the aborted tuples.
    ///
    /// The two writer conditions are **AND**-ed (not either/or): both that
    /// the snapshot is within the writer's horizon *and* that the writer
    /// committed must hold for a non-self reader.
    ///
    /// Together they admit the cache only when the relation is effectively
    /// quiescent for this snapshot (the read-mostly workload the cache
    /// targets) and fall back to a correct heap scan under any concurrency.
    ///
    /// `oracle` must be the transaction-status oracle backing this process's
    /// CLOG (in production the `TransactionManager`); it costs one status
    /// lookup per gate hit, which is negligible on the read-mostly path.
    #[must_use]
    pub fn is_snapshot_coherent(
        &self,
        rel: RelationId,
        snapshot: &Snapshot,
        oracle: &dyn XidStatusOracle,
    ) -> bool {
        if !snapshot.xip().is_empty() {
            return false;
        }
        let writer = self.last_writer_xid(rel);
        writer == Xid::INVALID
            || snapshot.is_current_xid(writer)
            || (!snapshot.xid_in_progress(writer) && oracle.is_committed(writer))
    }

    /// Coherence-gated [`Self::get`]: return the cached projection for `rel`
    /// only when `snapshot` provably reflects exactly the committed state at
    /// the cache's version (see [`Self::is_snapshot_coherent`]). Every path
    /// that serves MVCC results to a client must go through this accessor;
    /// `None` means the caller must fall back to a heap scan.
    #[must_use]
    pub fn get_for_snapshot(
        &self,
        rel: RelationId,
        snapshot: &Snapshot,
        oracle: &dyn XidStatusOracle,
    ) -> Option<Arc<CachedColumns>> {
        // Read everything under one lock so the version, last writer, and
        // entry are mutually consistent — a concurrent `bump_version` either
        // happens entirely before (gate sees the new writer / version) or
        // entirely after (we return the pre-bump entry, which `get`'s
        // version filter still matched).
        let g = self.inner.read();
        if !snapshot.xip().is_empty() {
            return None;
        }
        let writer = g.last_writer_xid.get(&rel).copied().unwrap_or(Xid::INVALID);
        // Serve a non-self reader only a writer whose effect is both within
        // this snapshot's horizon (`!xid_in_progress` — the snapshot is not
        // frozen *before* the writer, which would hide rows committed after
        // it) AND real (`is_committed` per the same oracle the heap
        // visibility path consults — never merely "no longer in progress",
        // which an ABORTED writer also satisfies, the phantom-row hole). A
        // reader that IS the writer is served its own in-progress projection.
        let snapshot_sees_writer = writer == Xid::INVALID
            || snapshot.is_current_xid(writer)
            || (!snapshot.xid_in_progress(writer) && oracle.is_committed(writer));
        if !snapshot_sees_writer {
            return None;
        }
        let current = *g.versions.get(&rel).unwrap_or(&0);
        g.entries
            .get(&rel)
            .filter(|e| e.version == current)
            .cloned()
    }

    /// Store a cache entry. The entry's `version` is compared
    /// against the relation's current version on insert: a stale
    /// build is dropped on the floor so a writer-during-build race
    /// cannot resurrect outdated columns.
    pub fn put(&self, rel: RelationId, entry: CachedColumns) {
        let mut g = self.inner.write();
        let current = *g.versions.get(&rel).unwrap_or(&0);
        if entry.version != current {
            // A writer bumped the version while we were building;
            // drop this entry and let the next reader rebuild from
            // the new heap state.
            return;
        }
        g.entries.insert(rel, Arc::new(entry));
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use parking_lot::RwLock;
    use ultrasql_core::{CommandId, Field, RelationId, Schema, Xid};
    use ultrasql_mvcc::{Snapshot, XidStatus, XidStatusOracle};
    use ultrasql_vec::column::{Column, NumericColumn};

    use super::{CachedColumns, ColumnCache};

    const REL: RelationId = RelationId::new(7);

    /// In-crate stand-in for the CLOG-backed oracle. Defaults unset XIDs to
    /// `InProgress`, exactly like the production `TransactionManager` for an
    /// XID with no terminal CLOG entry.
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

    fn schema() -> Schema {
        Schema::new([
            Field::required("id", ultrasql_core::DataType::Int32),
            Field::required("val", ultrasql_core::DataType::Int32),
        ])
        .expect("schema")
    }

    fn cols() -> Vec<Column> {
        vec![
            Column::Int32(NumericColumn::from_data(vec![1, 2, 3])),
            Column::Int32(NumericColumn::from_data(vec![10, 20, 30])),
        ]
    }

    /// Build a cache where `writer` is the relation's last writer and a
    /// 3-row entry is published at the current version.
    fn warm(writer: Xid) -> ColumnCache {
        let cache = ColumnCache::new();
        cache.bump_version(REL, writer);
        let version = cache.relation_version(REL);
        cache.put(REL, CachedColumns::new(version, schema(), cols()));
        cache
    }

    /// Reader snapshot with an empty in-progress set (the quiescent case the
    /// cache targets) and `current_xid`.
    fn reader_snapshot(current_xid: Xid) -> Snapshot {
        Snapshot::new(
            Xid::new(1),
            Xid::new(1_000),
            current_xid,
            CommandId::FIRST,
            std::iter::empty(),
        )
    }

    /// Preserved case (b): a COMMITTED writer's projection is still served to
    /// a fresh (non-self) reader.
    #[test]
    fn committed_writer_projection_is_served() {
        let writer = Xid::new(100);
        let cache = warm(writer);
        let oracle = MapOracle::new();
        oracle.set_committed(writer);

        let snap = reader_snapshot(Xid::INVALID);
        assert!(cache.is_snapshot_coherent(REL, &snap, &oracle));
        assert!(
            cache.get_for_snapshot(REL, &snap, &oracle).is_some(),
            "a committed writer's cached projection must be served"
        );
    }

    /// The NEW rejection — preserved case (d): an ABORTED writer's projection
    /// is NEVER served to a fresh reader, regardless of *how* it aborted
    /// (plain ROLLBACK, ROLLBACK PREPARED, or SSI force-abort all land here
    /// as `XidStatus::Aborted` in the same CLOG oracle the gate consults).
    #[test]
    fn aborted_writer_projection_is_rejected() {
        let writer = Xid::new(100);
        let cache = warm(writer);
        let oracle = MapOracle::new();
        oracle.set_aborted(writer);

        let snap = reader_snapshot(Xid::INVALID);
        assert!(
            !cache.is_snapshot_coherent(REL, &snap, &oracle),
            "an aborted writer must not pass the coherence gate"
        );
        assert!(
            cache.get_for_snapshot(REL, &snap, &oracle).is_none(),
            "an aborted writer's cached projection must never be served"
        );
    }

    /// Preserved case (a): the writer reading its OWN uncommitted (still
    /// in-progress) projection is served via `is_current_xid`, without
    /// consulting the oracle's commit status.
    #[test]
    fn own_in_progress_writer_reads_its_projection() {
        let writer = Xid::new(100);
        let cache = warm(writer);
        let oracle = MapOracle::new();
        oracle.set_in_progress(writer);

        // The reader IS the writer: current_xid == writer.
        let snap = reader_snapshot(writer);
        assert!(cache.is_snapshot_coherent(REL, &snap, &oracle));
        assert!(
            cache.get_for_snapshot(REL, &snap, &oracle).is_some(),
            "the writer must see its own in-progress projection (read-after-write)"
        );
    }

    /// Preserved case (c): an in-progress OTHER writer is rejected. With an
    /// empty in-progress set a non-self reader's snapshot was taken when no
    /// other txn was running, so an in-progress writer is not committed and
    /// the committed-status check rejects it.
    #[test]
    fn in_progress_other_writer_is_rejected() {
        let writer = Xid::new(100);
        let cache = warm(writer);
        let oracle = MapOracle::new();
        oracle.set_in_progress(writer);

        let snap = reader_snapshot(Xid::INVALID);
        assert!(
            !cache.is_snapshot_coherent(REL, &snap, &oracle),
            "an in-progress other writer must not pass the gate"
        );
        assert!(cache.get_for_snapshot(REL, &snap, &oracle).is_none());
    }

    /// A snapshot frozen *before* a now-committed writer must NOT consume
    /// that writer's projection: the `!xid_in_progress` half of the gate
    /// must survive the committed-status addition (the regression guard for
    /// the read-side HOLE — a frozen RR reader). Here the writer xid is
    /// >= the snapshot's `xmax`, so `xid_in_progress` is true even though the
    /// oracle reports the writer committed.
    #[test]
    fn committed_writer_newer_than_snapshot_is_rejected() {
        let writer = Xid::new(500);
        let cache = warm(writer);
        let oracle = MapOracle::new();
        oracle.set_committed(writer);

        // Snapshot frozen with xmax=200: writer 500 is newer than the
        // snapshot horizon (xid_in_progress == true).
        let snap = Snapshot::new(
            Xid::new(1),
            Xid::new(200),
            Xid::INVALID,
            CommandId::FIRST,
            std::iter::empty(),
        );
        assert!(snap.xip().is_empty());
        assert!(
            !cache.is_snapshot_coherent(REL, &snap, &oracle),
            "a snapshot frozen before a committed writer must not consume its projection"
        );
        assert!(cache.get_for_snapshot(REL, &snap, &oracle).is_none());
    }

    /// A non-empty in-progress set fails the gate up front (concurrency
    /// fallback), independent of writer status.
    #[test]
    fn non_empty_in_progress_set_fails_gate() {
        let writer = Xid::new(100);
        let cache = warm(writer);
        let oracle = MapOracle::new();
        oracle.set_committed(writer);

        let snap = Snapshot::new(
            Xid::new(1),
            Xid::new(1_000),
            Xid::INVALID,
            CommandId::FIRST,
            [Xid::new(50)],
        );
        assert!(!cache.is_snapshot_coherent(REL, &snap, &oracle));
        assert!(cache.get_for_snapshot(REL, &snap, &oracle).is_none());
    }

    /// A relation never written (INVALID last writer) is always coherent —
    /// the oracle is not consulted for `Xid::INVALID`.
    #[test]
    fn never_written_relation_is_coherent() {
        let cache = ColumnCache::new();
        let oracle = MapOracle::new();
        let snap = reader_snapshot(Xid::INVALID);
        assert!(cache.is_snapshot_coherent(REL, &snap, &oracle));
    }
}
