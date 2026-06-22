//! Sequential heap scan operator backed by the storage subsystem.
//!
//! Drives [`HeapAccess::scan_visible`] and decodes each tuple's
//! payload through a [`RowCodec`] directly into typed column
//! builders. Batches are capped at 4096 rows per `ARCHITECTURE.md`
//! §9.
//!
//! # Streaming model
//!
//! Each `next_batch` call drains the underlying [`VisibleHeapScan`]
//! iterator until 4096 visible tuples have landed in the per-column
//! [`ColumnBuilder`]s, then emits the [`Batch`](ultrasql_vec::Batch) and reseeds a fresh
//! set of builders. Memory usage is O(batch), not O(relation) — the
//! v0.5 "materialise everything into `Vec<Vec<Value>>` before
//! yielding the first batch" hack is gone.
//!
//! # Walker lifetime model
//!
//! [`VisibleHeapWalker`] borrows from [`HeapAccess`], [`Snapshot`], and
//! [`XidStatusOracle`]. `SeqScan` avoids a self-referential struct by
//! storing only the next `(block, slot)` resume position. Each
//! [`Operator::next_batch`] call creates a short-lived walker borrowing
//! from `self`, streams up to one output batch, then stores the walker's
//! resume position before the borrow ends.
//!
//! # Module layout
//!
//! This file declares the operator's public data types and
//! constructors; the rest of the behaviour is split across sibling
//! submodules:
//!
//! - [`operator`] — the [`Operator`] / `next_batch` implementation.
//! - [`cache`] — the column-cache fast path (read + populate).
//! - [`build_batch`] — the legacy `Vec<Vec<Value>>` → [`Batch`](ultrasql_vec::Batch) path.

use std::sync::Arc;

use ultrasql_core::{DataType, Field, RelationId, Schema};
use ultrasql_mvcc::{Snapshot, XidStatusOracle};
use ultrasql_storage::PageLoader;
use ultrasql_storage::heap::HeapAccess;
use ultrasql_storage::vm::VisibilityMap;

use crate::row_codec::{ColumnBuilder, RowCodec};
use crate::{CancelFlag, ExecError};

mod build_batch;
mod cache;
mod operator;

#[cfg(test)]
mod tests;

pub use build_batch::build_batch;

use cache::{CacheBuildState, CacheReadState, schema_all_fixed_numeric};

/// Maximum rows per batch, matching the `ARCHITECTURE.md` §9 contract.
const BATCH_TARGET_ROWS: usize = 4096;

/// Sequential heap scan operator.
///
/// Reads every MVCC-visible tuple from `rel` and decodes each payload
/// directly into typed column builders, emitting 4096-row [`Batch`](ultrasql_vec::Batch)es.
///
/// `L` is the [`PageLoader`] implementation (in production: the segment
/// loader; in tests: an in-memory map). `O` is the [`XidStatusOracle`]
/// implementation (in production: the CLOG-backed oracle; in tests:
/// `ultrasql_mvcc::status::test_support::MapOracle`).
///
/// # Send bound
///
/// The operator is `Send` because every owned field —
/// `Arc<HeapAccess<L>>`, `Box<Snapshot>`, `Arc<O>`, and the column
/// builders — is `Send + Sync`. The heap walker is never stored across
/// calls, so no lifetime erasure is required.
pub struct SeqScan<L: PageLoader + 'static, O: XidStatusOracle + ?Sized + 'static> {
    /// Reusable typed column builders. Sized to
    /// [`BATCH_TARGET_ROWS`] capacity on every fresh allocation and
    /// swapped out wholesale when a batch is emitted.
    builders: Vec<ColumnBuilder>,
    /// When `Some`, the scan is replaying a cached columnar
    /// projection of the relation; `next_batch` slices the columns
    /// into BATCH_TARGET_ROWS-sized output batches and never
    /// touches the heap. Set by [`Self::build`] when
    /// `HeapAccess::column_cache` already holds a live entry for
    /// this relation; left `None` when the scan is responsible for
    /// either populating the cache or operating outside the cache
    /// (e.g. TID-prefixed scans).
    cache_read: Option<CacheReadState>,
    /// Deferred constructor error for public constructors that cannot
    /// return `Result`. `next_batch` surfaces it before heap access.
    init_error: Option<String>,
    /// When `Some`, the scan is **populating** the column cache as
    /// it walks the heap: every decoded row is appended to these
    /// per-column accumulators **in addition** to the per-batch
    /// `builders`. On EOF the accumulators are finalised into a
    /// [`CachedColumns`] entry and stored in
    /// `HeapAccess::column_cache` so the next scan over the same
    /// relation can short-circuit via `cache_read`.
    cache_build: Option<CacheBuildState>,
    /// Shared heap access. The iterator borrows the inner
    /// `HeapAccess<L>` via this Arc.
    heap: Arc<HeapAccess<L>>,
    /// MVCC snapshot. Heap-allocated so its address is stable across
    /// moves of `Self`; the iterator carries a `'static`-extended
    /// borrow pointing here.
    snapshot: Box<Snapshot>,
    /// Transaction-status oracle. Same stability argument as
    /// `snapshot`.
    oracle: Arc<O>,
    /// Optional server-owned visibility map. When present and the
    /// relation has VM-certified pages, the heap walker skips per-tuple
    /// transaction-status probes on those pages.
    vm: Option<Arc<VisibilityMap>>,
    /// Static metadata captured at construction.
    relation: RelationId,
    /// Exclusive end block for this scan.
    block_count: u32,
    /// Block where the next short-lived walker should resume.
    next_block: u32,
    /// Slot where the next short-lived walker should resume.
    next_slot: u16,
    /// Row codec; owns the schema and drives `decode_into_builders`.
    codec: RowCodec,
    /// `true` if the operator should prepend `tid_block` / `tid_slot`
    /// columns to every decoded row. UPDATE / DELETE rely on this
    /// shape (see [`crate::modify::ModifyTable`]).
    with_tids: bool,
    /// Output schema. Equals `codec.schema()` when `with_tids` is
    /// false, or `[tid_block, tid_slot, ...codec.schema()]` when
    /// `with_tids` is true.
    output_schema: Schema,
    /// `true` after the iterator has been exhausted and the final
    /// (possibly partial) batch has been emitted.
    eof: bool,
    /// Per-query cancel signal. Polled at the top of every
    /// `next_batch`; when set, the operator returns
    /// [`ExecError::Cancelled`] without producing further batches.
    /// `None` for tests and bench harnesses that do not need
    /// cancellation.
    cancel_flag: Option<CancelFlag>,
}

/// Construction inputs for a VM-backed range [`SeqScan`].
///
/// Parallel scan workers use this shape to scan a disjoint block interval
/// without TID columns or column-cache participation.
pub struct SeqScanRangeWithVmConfig<L: PageLoader + 'static, O: XidStatusOracle + ?Sized + 'static>
{
    /// Shared heap access method for the target relation.
    pub heap: Arc<HeapAccess<L>>,
    /// Relation to scan.
    pub relation: RelationId,
    /// First heap block assigned to this worker.
    pub start_block: u32,
    /// Exclusive end heap block assigned to this worker.
    pub end_block: u32,
    /// MVCC snapshot used for tuple visibility.
    pub snapshot: Snapshot,
    /// Transaction-status oracle used for MVCC checks.
    pub oracle: Arc<O>,
    /// Visibility map used to skip per-tuple status probes.
    pub vm: Arc<VisibilityMap>,
    /// Row codec for the relation payload schema.
    pub codec: RowCodec,
}

impl<L: PageLoader + 'static, O: XidStatusOracle + ?Sized + 'static> std::fmt::Debug
    for SeqScanRangeWithVmConfig<L, O>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SeqScanRangeWithVmConfig")
            .field("relation", &self.relation)
            .field("start_block", &self.start_block)
            .field("end_block", &self.end_block)
            .field("schema", self.codec.schema())
            .finish_non_exhaustive()
    }
}

struct SeqScanBuildConfig<L: PageLoader + 'static, O: XidStatusOracle + ?Sized + 'static> {
    heap: Arc<HeapAccess<L>>,
    relation: RelationId,
    start_block: u32,
    block_count: u32,
    snapshot: Snapshot,
    oracle: Arc<O>,
    vm: Option<Arc<VisibilityMap>>,
    codec: RowCodec,
    with_tids: bool,
    allow_cache: bool,
    output_schema: Schema,
}

impl<L: PageLoader + 'static, O: XidStatusOracle + ?Sized + 'static> std::fmt::Debug
    for SeqScan<L, O>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SeqScan")
            .field("relation", &self.relation)
            .field("block_count", &self.block_count)
            .field("eof", &self.eof)
            .field("schema", self.codec.schema())
            .finish_non_exhaustive()
    }
}

impl<L, O> SeqScan<L, O>
where
    L: PageLoader + Send + Sync + 'static,
    O: XidStatusOracle + Send + Sync + 'static,
{
    /// Construct a `SeqScan`.
    ///
    /// - `heap` — shared reference to the heap access method.
    /// - `relation` — relation id to scan.
    /// - `block_count` — number of allocated blocks in `relation`
    ///   (from the catalog or `HeapAccess::block_count`).
    /// - `snapshot` — MVCC snapshot for visibility filtering.
    /// - `oracle` — transaction-status oracle.
    /// - `codec` — row codec whose schema matches the relation's
    ///   column layout.
    #[must_use]
    pub fn new(
        heap: Arc<HeapAccess<L>>,
        relation: RelationId,
        block_count: u32,
        snapshot: Snapshot,
        oracle: Arc<O>,
        codec: RowCodec,
    ) -> Self {
        let output_schema = codec.schema().clone();
        Self::build(SeqScanBuildConfig {
            heap,
            relation,
            start_block: 0,
            block_count,
            snapshot,
            oracle,
            vm: None,
            codec,
            with_tids: false,
            allow_cache: true,
            output_schema,
        })
    }

    /// Construct a `SeqScan` that uses a server-owned visibility map.
    #[must_use]
    pub fn new_with_vm(
        heap: Arc<HeapAccess<L>>,
        relation: RelationId,
        block_count: u32,
        snapshot: Snapshot,
        oracle: Arc<O>,
        vm: Arc<VisibilityMap>,
        codec: RowCodec,
    ) -> Self {
        let output_schema = codec.schema().clone();
        Self::build(SeqScanBuildConfig {
            heap,
            relation,
            start_block: 0,
            block_count,
            snapshot,
            oracle,
            vm: Some(vm),
            codec,
            with_tids: false,
            allow_cache: true,
            output_schema,
        })
    }

    /// Construct a `SeqScan` that emits two leading `Int32` columns
    /// (`tid_block`, `tid_slot`) before every payload column.
    ///
    /// Required by UPDATE / DELETE lowering: the
    /// [`crate::modify::ModifyTable`] operator extracts the tuple's
    /// `TupleId` from those columns to address the heap. The rest of
    /// the fields match [`SeqScan::new`].
    #[must_use]
    pub fn new_with_tids(
        heap: Arc<HeapAccess<L>>,
        relation: RelationId,
        block_count: u32,
        snapshot: Snapshot,
        oracle: Arc<O>,
        codec: RowCodec,
    ) -> Self {
        let output_schema = tid_prefixed_schema(&codec);
        Self::build(SeqScanBuildConfig {
            heap,
            relation,
            start_block: 0,
            block_count,
            snapshot,
            oracle,
            vm: None,
            codec,
            with_tids: true,
            allow_cache: true,
            output_schema,
        })
    }

    /// Construct a TID-prefixed `SeqScan` that uses a visibility map.
    #[must_use]
    pub fn new_with_tids_and_vm(
        heap: Arc<HeapAccess<L>>,
        relation: RelationId,
        block_count: u32,
        snapshot: Snapshot,
        oracle: Arc<O>,
        vm: Arc<VisibilityMap>,
        codec: RowCodec,
    ) -> Self {
        let output_schema = tid_prefixed_schema(&codec);
        Self::build(SeqScanBuildConfig {
            heap,
            relation,
            start_block: 0,
            block_count,
            snapshot,
            oracle,
            vm: Some(vm),
            codec,
            with_tids: true,
            allow_cache: true,
            output_schema,
        })
    }

    /// Construct a non-TID range scan for one parallel worker.
    #[must_use]
    pub fn new_range_with_vm(config: SeqScanRangeWithVmConfig<L, O>) -> Self {
        let SeqScanRangeWithVmConfig {
            heap,
            relation,
            start_block,
            end_block,
            snapshot,
            oracle,
            vm,
            codec,
        } = config;
        let output_schema = codec.schema().clone();
        Self::build(SeqScanBuildConfig {
            heap,
            relation,
            start_block,
            block_count: end_block,
            snapshot,
            oracle,
            vm: Some(vm),
            codec,
            with_tids: false,
            allow_cache: false,
            output_schema,
        })
    }

    /// Shared helper that builds the operator and the
    /// lifetime-extended iterator. Both `new` and `new_with_tids`
    /// funnel through here.
    fn build(config: SeqScanBuildConfig<L, O>) -> Self {
        let SeqScanBuildConfig {
            heap,
            relation,
            start_block,
            block_count,
            snapshot,
            oracle,
            vm,
            codec,
            with_tids,
            allow_cache,
            output_schema,
        } = config;
        let snapshot_box: Box<Snapshot> = Box::new(snapshot);

        // Column-cache eligibility:
        // - Non-TID-prefixed scan only (UPDATE / DELETE always
        //   reruns fresh state; caching its TID-augmented output
        //   never pays off).
        // - Only relations whose schema is exclusively numeric
        //   fixed-width types (Int16/32/64, Float32/64). Bool and
        //   Utf8 columns lack the `with_nulls` / `from_parts`
        //   constructors `slice_column` would need, and the bench
        //   workloads never hit them on a cached path anyway.
        let cache_eligible = allow_cache
            && start_block == 0
            && !with_tids
            && schema_all_fixed_numeric(codec.schema());
        // Column-cache **read** coherence guard. The cache is shared and
        // replayed RAW: `next_batch_from_cache` slices the cached columns
        // with no per-tuple visibility re-filtering, so a snapshot may only
        // consume a published entry when it provably reflects exactly the
        // committed state at that version.
        // [`ColumnCache::get_for_snapshot`] folds that gate in (see
        // [`ColumnCache::is_snapshot_coherent`]); a non-coherent snapshot
        // misses the cache and walks the heap.
        let cache_read = if cache_eligible {
            heap.column_cache
                .get_for_snapshot(relation, &snapshot_box)
                .map(|columns| CacheReadState { columns, cursor: 0 })
        } else {
            None
        };
        let cache_hit = cache_read.is_some();

        // Build typed builders sized for one full batch only when
        // we are going to walk the heap. The cache-read path never
        // touches `self.builders`, so allocating a fresh per-column
        // `Vec<T>` with `BATCH_TARGET_ROWS` capacity is pure
        // overhead — on the `select_scan_10k` hot path the relation
        // schema is `(Int32, Int32)` and each call to
        // `SeqScan::new` would otherwise spend ~32 KiB on builders
        // that are dropped unused.
        //
        // The TID-prefixed scan keeps its builders unconditionally:
        // `with_tids` is incompatible with cache reads (it is
        // explicitly excluded from `cache_eligible`).
        let mut init_error: Option<String> = None;
        let builders = if cache_hit {
            Vec::new()
        } else {
            match build_initial_builders(&codec, with_tids) {
                Ok(builders) => builders,
                Err(err) => {
                    tracing::error!(error = %err, "seq scan builder initialisation failed");
                    init_error = Some(err.to_string());
                    Vec::new()
                }
            }
        };

        // Decide whether this scan should populate the cache as a
        // side effect. Skip the build when (a) the scan is reading
        // from the cache already, (b) the scan is TID-augmented,
        // (c) the relation is empty (no point caching nothing), or
        // (d) this snapshot cannot see the relation's latest writer.
        //
        // Condition (d) is the column-cache **coherence guard**. The
        // cache is shared and keyed only on the relation's mutation
        // version, with no per-snapshot qualifier; whatever projection
        // we publish is served to *every* later scan at the same
        // version, and is replayed RAW (no visibility re-filtering). A
        // relation's version is bumped at physical insert/update/delete
        // time, which is *before* the writer commits — so the version
        // can already reflect a writer's rows (or deletions) while a
        // frozen snapshot (REPEATABLE READ / SERIALIZABLE, or any
        // snapshot taken before that writer committed) still cannot see
        // them. Conversely a read-after-write snapshot can see its own
        // uncommitted rows, and a multi-writer version reflects writers
        // whose individual visibility differs per reader.
        //
        // The sound condition (applied identically to the publish side
        // here and the read side above, via
        // [`ColumnCache::is_snapshot_coherent`]) is that the operating
        // snapshot provably reflects exactly the committed state at that
        // version. The cache is then used only when the relation is
        // effectively quiescent for this snapshot — the read-mostly
        // workload the cache targets — and any concurrency falls back to
        // a correct heap scan. The common autocommit reader takes a
        // fresh snapshot after the writer committed (empty in-progress
        // set, writer visible), so it still publishes and reuses the
        // cache; the hot path is unchanged.
        //
        // Read order matters: sample `target_version` *before* the
        // writer state inside the gate. A concurrent mutation between the
        // two reads then either advances the recorded writer to one this
        // snapshot cannot see (the gate rejects the build) or advances
        // the version past `target_version` (the version guard in
        // `ColumnCache::put` drops the finalised entry). Sampling the
        // version first leaves no window in which a freshly-bumped writer
        // is missed by *both* checks.
        let cache_build = if cache_eligible && !cache_hit && block_count > 0 {
            let target_version = heap.column_cache.relation_version(relation);
            if heap
                .column_cache
                .is_snapshot_coherent(relation, &snapshot_box)
            {
                match build_initial_builders(&codec, false) {
                    Ok(builders) => Some(CacheBuildState {
                        builders,
                        target_version,
                    }),
                    Err(err) => {
                        tracing::warn!(error = %err, "seq scan column-cache build disabled");
                        None
                    }
                }
            } else {
                None
            }
        } else {
            None
        };

        Self {
            builders,
            cache_read,
            init_error,
            cache_build,
            heap,
            snapshot: snapshot_box,
            oracle,
            vm,
            relation,
            block_count,
            next_block: start_block.min(block_count),
            next_slot: 0,
            codec,
            with_tids,
            output_schema,
            eof: false,
            cancel_flag: None,
        }
    }

    /// Attach a [`CancelFlag`] to this scan.
    ///
    /// Once set, `next_batch` polls the flag at every entry and
    /// returns [`ExecError::Cancelled`] without producing further
    /// batches. Returns `self` so callers can chain immediately after
    /// construction:
    ///
    /// ```ignore
    /// let scan = SeqScan::new(...).with_cancel_flag(flag);
    /// ```
    #[must_use]
    pub fn with_cancel_flag(mut self, flag: CancelFlag) -> Self {
        self.cancel_flag = Some(flag);
        self
    }
}

fn tid_prefixed_schema(codec: &RowCodec) -> Schema {
    let mut fields: Vec<Field> = Vec::with_capacity(codec.schema().len() + 2);
    fields.push(Field::required("tid_block", DataType::Int32));
    fields.push(Field::required("tid_slot", DataType::Int32));
    for i in 0..codec.schema().len() {
        fields.push(codec.schema().field_at(i).clone());
    }
    Schema::new_with_duplicate_names(fields)
}

fn builder_init_error(error: impl std::fmt::Display) -> ExecError {
    ExecError::TypeMismatch(format!("seq scan builder initialisation failed: {error}"))
}

/// Build a fresh `Vec<ColumnBuilder>` matching the codec's schema,
/// optionally prepending two `Int32` builders for `tid_block` /
/// `tid_slot`. Sized to [`BATCH_TARGET_ROWS`] capacity.
fn build_initial_builders(
    codec: &RowCodec,
    with_tids: bool,
) -> Result<Vec<ColumnBuilder>, ExecError> {
    let mut out: Vec<ColumnBuilder> = Vec::new();
    if with_tids {
        let tid_schema = Schema::new_with_duplicate_names([
            Field::required("tid_block", DataType::Int32),
            Field::required("tid_slot", DataType::Int32),
        ]);
        let tid_codec = RowCodec::new(tid_schema);
        out.extend(
            tid_codec
                .new_builders(BATCH_TARGET_ROWS)
                .map_err(builder_init_error)?,
        );
    }
    out.extend(
        codec
            .new_builders(BATCH_TARGET_ROWS)
            .map_err(builder_init_error)?,
    );
    Ok(out)
}
