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
//! [`ColumnBuilder`]s, then emits the [`Batch`] and reseeds a fresh
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

use std::sync::Arc;

use ultrasql_core::{DataType, Field, RelationId, Schema, Value, coerce_bpchar_text, pack_timetz};
use ultrasql_mvcc::{Snapshot, XidStatusOracle};
use ultrasql_storage::PageLoader;
use ultrasql_storage::heap::HeapAccess;
use ultrasql_storage::vm::VisibilityMap;
use ultrasql_vec::bitmap::Bitmap;
use ultrasql_vec::column::{BoolColumn, Column, NumericColumn};
use ultrasql_vec::{Batch, DictionaryEncodingPolicy, StringEncoding, encode_strings_auto};

use crate::row_codec::{ColumnBuilder, RowCodec};
use crate::{CancelFlag, ExecError, Operator};

/// Maximum rows per batch, matching the `ARCHITECTURE.md` §9 contract.
const BATCH_TARGET_ROWS: usize = 4096;

/// Sequential heap scan operator.
///
/// Reads every MVCC-visible tuple from `rel` and decodes each payload
/// directly into typed column builders, emitting 4096-row [`Batch`]es.
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
    #[allow(dead_code)]
    heap: Arc<HeapAccess<L>>,
    /// MVCC snapshot. Heap-allocated so its address is stable across
    /// moves of `Self`; the iterator carries a `'static`-extended
    /// borrow pointing here.
    #[allow(dead_code)]
    snapshot: Box<Snapshot>,
    /// Transaction-status oracle. Same stability argument as
    /// `snapshot`.
    #[allow(dead_code)]
    oracle: Arc<O>,
    /// Optional server-owned visibility map. When present and the
    /// relation has VM-certified pages, the heap walker skips per-tuple
    /// transaction-status probes on those pages.
    #[allow(dead_code)]
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

/// Cache-read state: a snapshot of cached columns for a relation,
/// plus the row cursor we are streaming from.
struct CacheReadState {
    columns: std::sync::Arc<ultrasql_storage::column_cache::CachedColumns>,
    cursor: usize,
}

impl std::fmt::Debug for CacheReadState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CacheReadState")
            .field("cursor", &self.cursor)
            .field("rows", &self.columns.columns.first().map(Column::len))
            .finish()
    }
}

/// Cache-build state: parallel column builders that accumulate
/// **every** decoded row across the whole scan (in contrast to the
/// per-batch `builders` field which is swapped out on every batch
/// emit). Finalised and stored in the column cache on EOF.
struct CacheBuildState {
    builders: Vec<ColumnBuilder>,
    /// Version of the relation when the build started, captured
    /// from `HeapAccess::column_cache.relation_version`. Re-checked
    /// at `put` time so a writer-during-build race drops the entry
    /// on the floor instead of resurrecting stale columns.
    target_version: u64,
}

impl std::fmt::Debug for CacheBuildState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CacheBuildState")
            .field("target_version", &self.target_version)
            .finish()
    }
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
        Self::build(
            heap,
            relation,
            0,
            block_count,
            snapshot,
            oracle,
            None,
            codec,
            false,
            true,
            output_schema,
        )
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
        Self::build(
            heap,
            relation,
            0,
            block_count,
            snapshot,
            oracle,
            Some(vm),
            codec,
            false,
            true,
            output_schema,
        )
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
        let mut fields: Vec<Field> = Vec::with_capacity(codec.schema().len() + 2);
        fields.push(Field::required("tid_block", DataType::Int32));
        fields.push(Field::required("tid_slot", DataType::Int32));
        for i in 0..codec.schema().len() {
            fields.push(codec.schema().field_at(i).clone());
        }
        let output_schema = Schema::new(fields).expect("TID-prefixed schema is well-formed");
        Self::build(
            heap,
            relation,
            0,
            block_count,
            snapshot,
            oracle,
            None,
            codec,
            true,
            true,
            output_schema,
        )
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
        let mut fields: Vec<Field> = Vec::with_capacity(codec.schema().len() + 2);
        fields.push(Field::required("tid_block", DataType::Int32));
        fields.push(Field::required("tid_slot", DataType::Int32));
        for i in 0..codec.schema().len() {
            fields.push(codec.schema().field_at(i).clone());
        }
        let output_schema = Schema::new(fields).expect("TID-prefixed schema is well-formed");
        Self::build(
            heap,
            relation,
            0,
            block_count,
            snapshot,
            oracle,
            Some(vm),
            codec,
            true,
            true,
            output_schema,
        )
    }

    /// Construct a non-TID range scan for one parallel worker.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new_range_with_vm(
        heap: Arc<HeapAccess<L>>,
        relation: RelationId,
        start_block: u32,
        end_block: u32,
        snapshot: Snapshot,
        oracle: Arc<O>,
        vm: Arc<VisibilityMap>,
        codec: RowCodec,
    ) -> Self {
        let output_schema = codec.schema().clone();
        Self::build(
            heap,
            relation,
            start_block,
            end_block,
            snapshot,
            oracle,
            Some(vm),
            codec,
            false,
            false,
            output_schema,
        )
    }

    /// Shared helper that builds the operator and the
    /// lifetime-extended iterator. Both `new` and `new_with_tids`
    /// funnel through here.
    #[allow(clippy::too_many_arguments)]
    fn build(
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
    ) -> Self {
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
        let cache_read = if cache_eligible {
            heap.column_cache
                .get(relation)
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
        let builders = if cache_hit {
            Vec::new()
        } else {
            build_initial_builders(&codec, with_tids)
        };

        // Decide whether this scan should populate the cache as a
        // side effect. Skip the build when (a) the scan is reading
        // from the cache already, (b) the scan is TID-augmented, or
        // (c) the relation is empty (no point caching nothing).
        let cache_build = if cache_eligible && !cache_hit && block_count > 0 {
            let target_version = heap.column_cache.relation_version(relation);
            Some(CacheBuildState {
                builders: build_initial_builders(&codec, false),
                target_version,
            })
        } else {
            None
        };

        Self {
            builders,
            cache_read,
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

/// Build a fresh `Vec<ColumnBuilder>` matching the codec's schema,
/// optionally prepending two `Int32` builders for `tid_block` /
/// `tid_slot`. Sized to [`BATCH_TARGET_ROWS`] capacity.
fn build_initial_builders(codec: &RowCodec, with_tids: bool) -> Vec<ColumnBuilder> {
    let mut out: Vec<ColumnBuilder> = Vec::new();
    if with_tids {
        let tid_schema = Schema::new([
            Field::required("tid_block", DataType::Int32),
            Field::required("tid_slot", DataType::Int32),
        ])
        .expect("tid schema is well-formed");
        let tid_codec = RowCodec::new(tid_schema);
        out.extend(
            tid_codec
                .new_builders(BATCH_TARGET_ROWS)
                .expect("Int32 is supported"),
        );
    }
    out.extend(
        codec
            .new_builders(BATCH_TARGET_ROWS)
            .expect("codec schema types are supported"),
    );
    out
}

impl<L, O> Operator for SeqScan<L, O>
where
    L: PageLoader + Send + Sync + std::fmt::Debug + 'static,
    O: XidStatusOracle + Send + Sync + std::fmt::Debug + 'static,
{
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.eof {
            return Ok(None);
        }
        // Cancellation poll at batch boundary. A `SeqScan` over a
        // large heap is typically the longest-running operator in the
        // pipeline; checking the flag here is the cheapest place to
        // observe a CancelRequest.
        if let Some(flag) = self.cancel_flag.as_ref()
            && flag.is_set()
        {
            return Err(ExecError::Cancelled);
        }

        // Fast path: replay from cached columnar projection. Skips
        // the heap walk + per-tuple decode entirely. See
        // `CacheReadState`.
        if self.cache_read.is_some() {
            return self.next_batch_from_cache();
        }

        let tid_offset = usize::from(self.with_tids) * 2;
        let mut rows_buffered: usize = 0;
        let mut iter_exhausted = true;

        if self.next_block < self.block_count {
            let mut walker = if let Some(vm) = self.vm.as_deref() {
                self.heap.scan_visible_walker_range_from_position_with_vm(
                    self.relation,
                    (self.next_block, self.next_slot),
                    self.block_count,
                    &self.snapshot,
                    self.oracle.as_ref(),
                    vm,
                )
            } else {
                self.heap.scan_visible_walker_range_from_position(
                    self.relation,
                    (self.next_block, self.next_slot),
                    self.block_count,
                    &self.snapshot,
                    self.oracle.as_ref(),
                )
            };
            while rows_buffered < BATCH_TARGET_ROWS {
                let item = walker.try_next().map_err(|e| {
                    tracing::warn!(error = %e, "heap scan error");
                    ExecError::Internal("heap scan failed")
                })?;
                let Some((tid, _header, payload)) = item else {
                    break;
                };
                if self.with_tids {
                    // PostgreSQL's `BlockNumber` is u32; the TID
                    // columns are i32 (matching the v0.5 `ModifyTable`
                    // extractor).
                    let block_i32 = i32::try_from(tid.page.block.raw()).map_err(|_| {
                        ExecError::Internal("BlockNumber exceeds i32 range; TID column overflow")
                    })?;
                    let slot_i32 = i32::from(tid.slot);
                    RowCodec::push_i32_into(&mut self.builders, 0, block_i32);
                    RowCodec::push_i32_into(&mut self.builders, 1, slot_i32);
                }
                self.codec
                    .decode_into_builders(payload, &mut self.builders[tid_offset..])
                    .map_err(|e| {
                        ExecError::TypeMismatch(format!(
                            "row decode failed: relation={:?}, schema={:?}, payload_len={}, payload_prefix={}, error={}",
                            self.relation,
                            self.codec.schema(),
                            payload.len(),
                            payload_prefix(payload),
                            e
                        ))
                    })?;
                // Mirror the decoded row into the cache-build
                // accumulator when populating the column cache.
                // Skipped on the TID-prefixed scan (cache_build is
                // `None` there).
                if let Some(build) = self.cache_build.as_mut() {
                    self.codec
                        .decode_into_builders(payload, &mut build.builders)
                        .map_err(|e| {
                            ExecError::TypeMismatch(format!(
                                "row cache decode failed: relation={:?}, schema={:?}, payload_len={}, payload_prefix={}, error={}",
                                self.relation,
                                self.codec.schema(),
                                payload.len(),
                                payload_prefix(payload),
                                e
                            ))
                        })?;
                }
                rows_buffered += 1;
            }
            // Mark "not exhausted" only when we hit the row cap (the
            // walker may still hold more rows for the next call).
            if rows_buffered >= BATCH_TARGET_ROWS {
                iter_exhausted = false;
            }
            let (next_block, next_slot) = walker.resume_position();
            self.next_block = next_block;
            self.next_slot = next_slot;
        }

        if rows_buffered == 0 {
            self.eof = true;
            // Finalise the cache build, if any. The walker is
            // exhausted: we have every visible row in
            // `cache_build.builders`. Store the result and let the
            // next scan over this relation reach `cache_read`.
            self.finalise_cache_build();
            return Ok(None);
        }

        // Swap out the current builders so we can finish them into a
        // batch; the replacement builders' Vec<T> allocations are
        // fresh — see report below. This is the only per-batch
        // allocation the streaming path performs (excluding the
        // backing batch itself).
        let replacement = build_initial_builders(&self.codec, self.with_tids);
        let finished = std::mem::replace(&mut self.builders, replacement);
        let batch = RowCodec::finish_batch(finished).map_err(ExecError::from)?;

        if iter_exhausted {
            self.eof = true;
            // Walker is done — finalise the cache build before the
            // operator emits its EOF marker on the next call.
            self.finalise_cache_build();
        }
        Ok(Some(batch))
    }

    fn schema(&self) -> &Schema {
        &self.output_schema
    }

    fn estimated_row_count(&self) -> Option<usize> {
        // Cache-read path knows the relation's total cardinality
        // up front; advertise it so the wire-encoder can pre-reserve
        // the response buffer and skip mid-stream `BytesMut::reserve`
        // reallocations.
        self.cache_read.as_ref().and_then(|state| {
            state
                .columns
                .columns
                .first()
                .map(ultrasql_vec::column::Column::len)
        })
    }
}

impl<L, O> SeqScan<L, O>
where
    L: PageLoader + Send + Sync + std::fmt::Debug + 'static,
    O: XidStatusOracle + Send + Sync + std::fmt::Debug + 'static,
{
    /// Slice the next batch out of the cached columnar projection.
    ///
    /// Replaces the heap walk + decode loop entirely when the
    /// relation's `ColumnCache` entry is live. Emits wider batches
    /// than the heap-walk path
    /// (`CACHE_REPLAY_BATCH_ROWS`) because the cache already holds
    /// the full columnar projection in memory: larger batches
    /// amortise per-batch operator overhead (filter / select_column
    /// / aggregate). Downstream operators handle variable batch
    /// sizes — the 4096-cap is a soft target for the streaming
    /// heap-scan path, not a hard invariant of the executor.
    fn next_batch_from_cache(&mut self) -> Result<Option<Batch>, ExecError> {
        const CACHE_REPLAY_BATCH_ROWS: usize = 1_048_576;

        let Some(state) = self.cache_read.as_mut() else {
            // Should not happen: caller checked `is_some` before
            // calling, but stay defensive.
            return Ok(None);
        };
        let total_rows = state.columns.columns.first().map(Column::len).unwrap_or(0);
        if state.cursor >= total_rows {
            self.eof = true;
            self.cache_read = None;
            return Ok(None);
        }
        let end = (state.cursor + CACHE_REPLAY_BATCH_ROWS).min(total_rows);
        let mut batch_cols: Vec<Column> = Vec::with_capacity(state.columns.columns.len());
        for col in &state.columns.columns {
            batch_cols.push(slice_column(col, state.cursor, end));
        }
        state.cursor = end;
        let batch = Batch::new(batch_cols).map_err(ExecError::from)?;
        Ok(Some(batch))
    }

    /// Move the accumulator builders into the relation's
    /// [`ColumnCache`] entry. No-op when `cache_build` is `None`
    /// (TID-prefixed scan or a scan that already started from a
    /// live cache entry).
    fn finalise_cache_build(&mut self) {
        let Some(build) = self.cache_build.take() else {
            return;
        };
        // Build the final `Vec<Column>` from the accumulator
        // builders. Any per-builder finish error means the cache
        // is unbuildable for this scan — drop the build silently;
        // the next scan over the same relation will retry.
        let finished_batch = match RowCodec::finish_batch(build.builders) {
            Ok(b) => b,
            Err(_) => return,
        };
        let columns: Vec<Column> = finished_batch.columns().to_vec();
        let entry = ultrasql_storage::column_cache::CachedColumns::new(
            build.target_version,
            self.codec.schema().clone(),
            columns,
        );
        self.heap.column_cache.put(self.relation, entry);
    }
}

fn payload_prefix(payload: &[u8]) -> String {
    let mut out = String::with_capacity(payload.len().min(32) * 2);
    for byte in payload.iter().take(32) {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

/// Slice rows `[start, end)` out of `col` into an owned [`Column`].
///
/// This is a zero-conditional clone of the underlying typed
/// `Vec<T>` for fixed-width columns and an offsets+values clone
/// for the variable-width `Utf8` arm. Used by the column-cache
/// fast path to materialise a batch from cached data without
/// re-decoding from heap bytes.
fn slice_column(col: &Column, start: usize, end: usize) -> Column {
    use ultrasql_vec::bitmap::Bitmap;
    use ultrasql_vec::column::NumericColumn;

    fn slice_nulls(nulls: Option<&Bitmap>, start: usize, end: usize) -> Option<Bitmap> {
        nulls.map(|b| {
            let mut out = Bitmap::new(end - start, false);
            for (i, src) in (start..end).enumerate() {
                out.set(i, b.get(src));
            }
            out
        })
    }

    match col {
        Column::Int32(c) => {
            let data = c.data()[start..end].to_vec();
            let nulls = slice_nulls(c.nulls(), start, end);
            match nulls {
                Some(n) => {
                    Column::Int32(NumericColumn::with_nulls(data, n).expect("matching lengths"))
                }
                None => Column::Int32(NumericColumn::from_data(data)),
            }
        }
        Column::Int64(c) => {
            let data = c.data()[start..end].to_vec();
            let nulls = slice_nulls(c.nulls(), start, end);
            match nulls {
                Some(n) => {
                    Column::Int64(NumericColumn::with_nulls(data, n).expect("matching lengths"))
                }
                None => Column::Int64(NumericColumn::from_data(data)),
            }
        }
        Column::Float32(c) => {
            let data = c.data()[start..end].to_vec();
            let nulls = slice_nulls(c.nulls(), start, end);
            match nulls {
                Some(n) => {
                    Column::Float32(NumericColumn::with_nulls(data, n).expect("matching lengths"))
                }
                None => Column::Float32(NumericColumn::from_data(data)),
            }
        }
        Column::Float64(c) => {
            let data = c.data()[start..end].to_vec();
            let nulls = slice_nulls(c.nulls(), start, end);
            match nulls {
                Some(n) => {
                    Column::Float64(NumericColumn::with_nulls(data, n).expect("matching lengths"))
                }
                None => Column::Float64(NumericColumn::from_data(data)),
            }
        }
        // Bool / Utf8 cache slicing is intentionally not
        // implemented: `schema_all_fixed_numeric` keeps these out of
        // the cache-eligible set, so this arm is unreachable in
        // practice. Surfacing it as a panic catches a future
        // regression where the eligibility check is loosened
        // without finishing the slice paths.
        Column::Bool(_) | Column::Utf8(_) | Column::DictionaryUtf8(_) => {
            unreachable!(
                "column cache does not yet support Bool / Utf8 — gated by schema_all_fixed_numeric"
            )
        }
    }
}

/// `true` iff every column in `schema` is a fixed-width numeric
/// type. Used to gate column-cache eligibility — the slice path
/// only supports `Int16` / `Int32` / `Int64` / `Float32` / `Float64`
/// at the moment.
fn schema_all_fixed_numeric(schema: &Schema) -> bool {
    schema.fields().iter().all(|f| {
        matches!(
            f.data_type.storage_type(),
            DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Oid
                | DataType::RegClass
                | DataType::RegType
                | DataType::Float32
                | DataType::Float64
        )
    })
}

/// Convert a slice of decoded rows into a [`Batch`] matching `schema`.
///
/// Kept for backwards compatibility with callers that still want the
/// `Vec<Vec<Value>>` → [`Batch`] path. The streaming [`SeqScan`] no
/// longer uses this function.
#[allow(clippy::too_many_lines)]
pub fn build_batch(rows: &[Vec<Value>], schema: &Schema) -> Result<Batch, ExecError> {
    if rows.is_empty() {
        return Batch::new(std::iter::empty::<Column>()).map_err(ExecError::from);
    }

    let n_cols = schema.len();
    let n_rows = rows.len();

    // Helper closure: scan column `col_idx` once and build a validity
    // bitmap (1 = valid, 0 = null per Arrow convention). Returns
    // `None` when no row in this column is null — the column is
    // emitted without a bitmap so kernels keep their fast path.
    let build_validity = |col_idx: usize| -> Option<Bitmap> {
        let mut any_null = false;
        let mut bitmap = Bitmap::new(n_rows, true);
        for (row_idx, row) in rows.iter().enumerate() {
            if matches!(row[col_idx], Value::Null) {
                bitmap.set(row_idx, false);
                any_null = true;
            }
        }
        any_null.then_some(bitmap)
    };

    let mut columns: Vec<Column> = Vec::with_capacity(n_cols);

    for col_idx in 0..n_cols {
        let field = schema.field_at(col_idx);
        let storage_type = field.data_type.storage_type();
        let col = match storage_type {
            DataType::Null => {
                for (row_idx, row) in rows.iter().enumerate() {
                    if !matches!(row[col_idx], Value::Null) {
                        return Err(ExecError::TypeMismatch(format!(
                            "expected NULL at row {row_idx} col {col_idx}, got {:?}",
                            row[col_idx].data_type()
                        )));
                    }
                }
                Column::Int32(
                    NumericColumn::with_nulls(vec![0_i32; n_rows], Bitmap::new(n_rows, false))
                        .map_err(|e| ExecError::TypeMismatch(e.to_string()))?,
                )
            }
            DataType::Bool => {
                let mut data: Vec<bool> = Vec::with_capacity(n_rows);
                for (row_idx, row) in rows.iter().enumerate() {
                    match &row[col_idx] {
                        Value::Bool(v) => data.push(*v),
                        Value::Null => data.push(false),
                        other => {
                            return Err(ExecError::TypeMismatch(format!(
                                "expected Bool at row {row_idx} col {col_idx}, got {:?}",
                                other.data_type()
                            )));
                        }
                    }
                }
                let col = if let Some(nulls) = build_validity(col_idx) {
                    BoolColumn::with_nulls(data, nulls)
                        .map_err(|e| ExecError::TypeMismatch(e.to_string()))?
                } else {
                    BoolColumn::from_data(data)
                };
                Column::Bool(col)
            }
            DataType::Int16 => {
                let mut data: Vec<i32> = Vec::with_capacity(n_rows);
                for (row_idx, row) in rows.iter().enumerate() {
                    match &row[col_idx] {
                        Value::Int16(v) => data.push(i32::from(*v)),
                        Value::Null => data.push(0),
                        other => {
                            return Err(ExecError::TypeMismatch(format!(
                                "expected Int16 at row {row_idx} col {col_idx}, got {:?}",
                                other.data_type()
                            )));
                        }
                    }
                }
                let col = if let Some(nulls) = build_validity(col_idx) {
                    NumericColumn::with_nulls(data, nulls)
                        .map_err(|e| ExecError::TypeMismatch(e.to_string()))?
                } else {
                    NumericColumn::from_data(data)
                };
                Column::Int32(col)
            }
            DataType::Int32 => {
                let mut data: Vec<i32> = Vec::with_capacity(n_rows);
                for (row_idx, row) in rows.iter().enumerate() {
                    match &row[col_idx] {
                        Value::Int32(v) => data.push(*v),
                        Value::Null => data.push(0),
                        other => {
                            return Err(ExecError::TypeMismatch(format!(
                                "expected Int32 at row {row_idx} col {col_idx}, got {:?}",
                                other.data_type()
                            )));
                        }
                    }
                }
                let col = if let Some(nulls) = build_validity(col_idx) {
                    NumericColumn::with_nulls(data, nulls)
                        .map_err(|e| ExecError::TypeMismatch(e.to_string()))?
                } else {
                    NumericColumn::from_data(data)
                };
                Column::Int32(col)
            }
            DataType::Int64 => {
                let mut data: Vec<i64> = Vec::with_capacity(n_rows);
                for (row_idx, row) in rows.iter().enumerate() {
                    match &row[col_idx] {
                        Value::Int64(v) => data.push(*v),
                        Value::Null => data.push(0),
                        other => {
                            return Err(ExecError::TypeMismatch(format!(
                                "expected Int64 at row {row_idx} col {col_idx}, got {:?}",
                                other.data_type()
                            )));
                        }
                    }
                }
                let col = if let Some(nulls) = build_validity(col_idx) {
                    NumericColumn::with_nulls(data, nulls)
                        .map_err(|e| ExecError::TypeMismatch(e.to_string()))?
                } else {
                    NumericColumn::from_data(data)
                };
                Column::Int64(col)
            }
            DataType::Oid | DataType::RegClass | DataType::RegType => {
                let mut data: Vec<i64> = Vec::with_capacity(n_rows);
                for (row_idx, row) in rows.iter().enumerate() {
                    let raw = match (&storage_type, &row[col_idx]) {
                        (DataType::Oid, Value::Oid(v))
                        | (DataType::RegClass, Value::RegClass(v))
                        | (DataType::RegType, Value::RegType(v)) => i64::from(v.raw()),
                        (_, Value::Null) => 0,
                        (_, other) => {
                            return Err(ExecError::TypeMismatch(format!(
                                "expected {storage_type} at row {row_idx} col {col_idx}, got {:?}",
                                other.data_type()
                            )));
                        }
                    };
                    data.push(raw);
                }
                let col = if let Some(nulls) = build_validity(col_idx) {
                    NumericColumn::with_nulls(data, nulls)
                        .map_err(|e| ExecError::TypeMismatch(e.to_string()))?
                } else {
                    NumericColumn::from_data(data)
                };
                Column::Int64(col)
            }
            DataType::Float32 => {
                let mut data: Vec<f32> = Vec::with_capacity(n_rows);
                for (row_idx, row) in rows.iter().enumerate() {
                    match &row[col_idx] {
                        Value::Float32(v) => data.push(*v),
                        Value::Null => data.push(0.0),
                        other => {
                            return Err(ExecError::TypeMismatch(format!(
                                "expected Float32 at row {row_idx} col {col_idx}, got {:?}",
                                other.data_type()
                            )));
                        }
                    }
                }
                let col = if let Some(nulls) = build_validity(col_idx) {
                    NumericColumn::with_nulls(data, nulls)
                        .map_err(|e| ExecError::TypeMismatch(e.to_string()))?
                } else {
                    NumericColumn::from_data(data)
                };
                Column::Float32(col)
            }
            DataType::Float64 => {
                let mut data: Vec<f64> = Vec::with_capacity(n_rows);
                for (row_idx, row) in rows.iter().enumerate() {
                    match &row[col_idx] {
                        Value::Float64(v) => data.push(*v),
                        Value::Null => data.push(0.0),
                        other => {
                            return Err(ExecError::TypeMismatch(format!(
                                "expected Float64 at row {row_idx} col {col_idx}, got {:?}",
                                other.data_type()
                            )));
                        }
                    }
                }
                let col = if let Some(nulls) = build_validity(col_idx) {
                    NumericColumn::with_nulls(data, nulls)
                        .map_err(|e| ExecError::TypeMismatch(e.to_string()))?
                } else {
                    NumericColumn::from_data(data)
                };
                Column::Float64(col)
            }
            DataType::Text { .. }
            | DataType::Enum { .. }
            | DataType::Composite { .. }
            | DataType::Char { .. }
            | DataType::Bit { .. }
            | DataType::VarBit { .. }
            | DataType::Inet
            | DataType::Cidr
            | DataType::MacAddr
            | DataType::MacAddr8
            | DataType::Json
            | DataType::Jsonb
            | DataType::Xml
            | DataType::PgLsn
            | DataType::Vector { .. }
            | DataType::HalfVec { .. }
            | DataType::SparseVec { .. }
            | DataType::BitVec { .. }
            | DataType::Range(_)
            | DataType::Geometry(_)
            | DataType::Array(_)
            | DataType::Uuid
            | DataType::Bytea => {
                let mut strings: Vec<Option<String>> = Vec::with_capacity(n_rows);
                for (row_idx, row) in rows.iter().enumerate() {
                    match (&field.data_type, &row[col_idx]) {
                        (DataType::Text { .. }, Value::Text(s)) => strings.push(Some(s.clone())),
                        (DataType::Enum { labels, .. }, Value::Text(s))
                            if labels.iter().any(|label| label == s) =>
                        {
                            strings.push(Some(s.clone()));
                        }
                        (DataType::Composite { .. }, Value::Text(s)) => {
                            strings.push(Some(s.clone()));
                        }
                        (DataType::Char { .. }, Value::Char(s)) => strings.push(Some(s.clone())),
                        (DataType::Char { len }, Value::Text(s)) => {
                            let coerced = coerce_bpchar_text(s, *len, false).map_err(|err| {
                                ExecError::StringDataRightTruncation(err.to_string())
                            })?;
                            strings.push(Some(coerced));
                        }
                        (DataType::Json, Value::Json(s))
                        | (DataType::Jsonb, Value::Jsonb(s))
                        | (DataType::Xml, Value::Xml(s)) => strings.push(Some(s.clone())),
                        (DataType::PgLsn, Value::PgLsn(lsn)) => {
                            strings.push(Some(lsn.to_string()));
                        }
                        (
                            DataType::Bit { .. } | DataType::VarBit { .. },
                            Value::BitString(bits),
                        ) if bits.matches_type(&field.data_type) => {
                            strings.push(Some(bits.to_string()));
                        }
                        (
                            DataType::Bit { .. } | DataType::VarBit { .. },
                            Value::BitString(bits),
                        ) => {
                            return Err(ExecError::StringDataRightTruncation(format!(
                                "bit string length {} does not match type {}",
                                bits.len(),
                                field.data_type
                            )));
                        }
                        (
                            DataType::Inet
                            | DataType::Cidr
                            | DataType::MacAddr
                            | DataType::MacAddr8,
                            Value::Network(network),
                        ) if network.data_type() == field.data_type => {
                            strings.push(Some(network.to_string()));
                        }
                        (DataType::Vector { dims }, Value::Vector(values))
                            if dims.is_none() || u32::try_from(values.len()).ok() == *dims =>
                        {
                            strings.push(Some(row[col_idx].to_string()));
                        }
                        (expected, value)
                            if expected.is_vector_family()
                                && vector_family_value_matches(expected, value) =>
                        {
                            strings.push(Some(row[col_idx].to_string()));
                        }
                        (DataType::Range(expected), Value::Range(v))
                            if expected == &v.range_type =>
                        {
                            strings.push(Some(v.to_string()));
                        }
                        (DataType::Geometry(expected), Value::Geometry(v))
                            if expected == &v.geometry_type =>
                        {
                            strings.push(Some(v.to_string()));
                        }
                        (DataType::Array(expected), Value::Array { element_type, .. })
                            if expected.as_ref() == element_type =>
                        {
                            strings.push(Some(row[col_idx].to_string()));
                        }
                        (DataType::Uuid, Value::Uuid(v)) => {
                            strings.push(Some(Value::Uuid(*v).to_string()));
                        }
                        (DataType::Bytea, Value::Bytea(v)) => {
                            strings.push(Some(Value::Bytea(v.clone()).to_string()));
                        }
                        (_, Value::Null) => strings.push(None),
                        (_, other) => {
                            return Err(ExecError::TypeMismatch(format!(
                                "expected {} at row {row_idx} col {col_idx}, got {:?}",
                                field.data_type,
                                other.data_type()
                            )));
                        }
                    }
                }
                match encode_strings_auto(
                    strings.iter().map(|v| v.as_deref()),
                    DictionaryEncodingPolicy::default(),
                ) {
                    StringEncoding::Raw(c) => Column::Utf8(c),
                    StringEncoding::Dictionary(c) => Column::DictionaryUtf8(c),
                }
            }
            DataType::Date => {
                // Date values share the Int32 batch column: the
                // storage is the same 4-byte little-endian payload
                // (days since 2000-01-01). The schema field still
                // reports `DataType::Date` so downstream operators
                // that care about date semantics keep the type tag.
                let mut data: Vec<i32> = Vec::with_capacity(n_rows);
                for (row_idx, row) in rows.iter().enumerate() {
                    match &row[col_idx] {
                        Value::Date(v) => data.push(*v),
                        Value::Null => data.push(0),
                        other => {
                            return Err(ExecError::TypeMismatch(format!(
                                "expected Date at row {row_idx} col {col_idx}, got {:?}",
                                other.data_type()
                            )));
                        }
                    }
                }
                let col = if let Some(nulls) = build_validity(col_idx) {
                    NumericColumn::with_nulls(data, nulls)
                        .map_err(|e| ExecError::TypeMismatch(e.to_string()))?
                } else {
                    NumericColumn::from_data(data)
                };
                Column::Int32(col)
            }
            DataType::Decimal { .. }
            | DataType::Money
            | DataType::Timestamp
            | DataType::TimestampTz
            | DataType::Time
            | DataType::TimeTz => {
                // Decimal / Money / Timestamp / Time values share the Int64
                // batch column. Schema field carries the semantic
                // tag (and scale, for Decimal).
                let mut data: Vec<i64> = Vec::with_capacity(n_rows);
                for (row_idx, row) in rows.iter().enumerate() {
                    let v_i64 = match &row[col_idx] {
                        Value::Decimal { value, .. } => *value,
                        Value::Money(v) => *v,
                        Value::Timestamp(v) | Value::TimestampTz(v) | Value::Time(v) => *v,
                        Value::TimeTz {
                            micros,
                            offset_seconds,
                        } => pack_timetz(*micros, *offset_seconds).ok_or_else(|| {
                            ExecError::TypeMismatch(format!(
                                "invalid TimeTz at row {row_idx} col {col_idx}"
                            ))
                        })?,
                        Value::Int16(v) => i64::from(*v),
                        Value::Int32(v) => i64::from(*v),
                        Value::Int64(v) => *v,
                        Value::Null => 0,
                        other => {
                            return Err(ExecError::TypeMismatch(format!(
                                "expected Decimal/Money/Timestamp/Time at row {row_idx} col {col_idx}, got {:?}",
                                other.data_type()
                            )));
                        }
                    };
                    data.push(v_i64);
                }
                let col = if let Some(nulls) = build_validity(col_idx) {
                    NumericColumn::with_nulls(data, nulls)
                        .map_err(|e| ExecError::TypeMismatch(e.to_string()))?
                } else {
                    NumericColumn::from_data(data)
                };
                Column::Int64(col)
            }
            other => {
                return Err(ExecError::TypeMismatch(format!(
                    "SeqScan: unsupported column type {other} for batch building"
                )));
            }
        };
        columns.push(col);
    }

    Batch::new(columns).map_err(ExecError::from)
}

fn vector_family_value_matches(expected: &DataType, value: &Value) -> bool {
    let actual = value.data_type();
    vector_family_kind(expected) == vector_family_kind(&actual)
        && dims_compatible(
            expected.vector_dims().flatten(),
            actual.vector_dims().flatten(),
        )
}

fn vector_family_kind(data_type: &DataType) -> Option<u8> {
    match data_type {
        DataType::Vector { .. } => Some(0),
        DataType::HalfVec { .. } => Some(1),
        DataType::SparseVec { .. } => Some(2),
        DataType::BitVec { .. } => Some(3),
        _ => None,
    }
}

const fn dims_compatible(left: Option<u32>, right: Option<u32>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => left == right,
        _ => true,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    reason = "tests: index arithmetic against compile-time-known loop bounds"
)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use parking_lot::Mutex;
    use ultrasql_core::constants::PAGE_SIZE;
    use ultrasql_core::{
        BitString, CommandId, DataType, Field, GeometryType, GeometryValue, Lsn, NetworkValue, Oid,
        PageId, RangeType, RangeValue, RelationId, Result, Schema, SparseVector, Value, Xid,
    };
    use ultrasql_mvcc::Snapshot;
    use ultrasql_mvcc::status::test_support::MapOracle;
    use ultrasql_storage::buffer_pool::{BufferPool, PageLoader};
    use ultrasql_storage::heap::{HeapAccess, InsertOptions};
    use ultrasql_storage::page::Page;
    use ultrasql_vec::bitmap::Bitmap;
    use ultrasql_vec::column::{Column, NumericColumn};

    use super::{SeqScan, build_batch, payload_prefix, schema_all_fixed_numeric, slice_column};
    use crate::row_codec::RowCodec;
    use crate::{ExecError, Operator};

    // -----------------------------------------------------------------------
    // Test fixtures
    // -----------------------------------------------------------------------

    /// In-memory page loader that materialises blank heap pages on first miss
    /// and persists them across evictions.
    #[derive(Default, Debug)]
    struct MapLoader {
        store: Mutex<HashMap<PageId, Box<[u8; PAGE_SIZE]>>>,
    }

    impl MapLoader {
        fn new() -> Self {
            Self::default()
        }
    }

    impl PageLoader for MapLoader {
        fn load(&self, page_id: PageId) -> Result<Page> {
            let stored = {
                let store = self.store.lock();
                store.get(&page_id).map(|b| {
                    let mut copy: Box<[u8; PAGE_SIZE]> = vec![0_u8; PAGE_SIZE]
                        .into_boxed_slice()
                        .try_into()
                        .expect("alloc matches PAGE_SIZE");
                    copy.copy_from_slice(&**b);
                    copy
                })
            };
            if let Some(bytes) = stored {
                return Page::from_bytes(bytes)
                    .map_err(|e| ultrasql_core::Error::Corruption(format!("test loader: {e}")));
            }
            let page = Page::new_heap();
            let mut copy: Box<[u8; PAGE_SIZE]> = vec![0_u8; PAGE_SIZE]
                .into_boxed_slice()
                .try_into()
                .expect("alloc matches PAGE_SIZE");
            copy.copy_from_slice(page.as_bytes());
            self.store.lock().insert(page_id, copy);
            Ok(page)
        }
    }

    fn rel() -> RelationId {
        RelationId::new(1)
    }

    fn make_heap() -> Arc<HeapAccess<MapLoader>> {
        let pool = Arc::new(BufferPool::new(64, MapLoader::new()));
        Arc::new(HeapAccess::new(pool))
    }

    fn snap_for(xid: u64) -> Snapshot {
        Snapshot::new(
            Xid::new(xid + 1),
            Xid::new(xid + 2),
            Xid::new(xid + 1),
            CommandId::FIRST,
            [],
        )
    }

    fn insert_opts(xid: u64) -> InsertOptions<'static> {
        InsertOptions {
            xmin: Xid::new(xid),
            command_id: CommandId::FIRST,
            wal: None,
            fsm: None,
            vm: None,
        }
    }

    fn schema_i32_text() -> Schema {
        Schema::new([
            Field::required("id", DataType::Int32),
            Field::required("name", DataType::Text { max_len: None }),
        ])
        .expect("schema ok")
    }

    fn schema_i32_only() -> Schema {
        Schema::new([Field::required("id", DataType::Int32)]).expect("schema ok")
    }

    #[test]
    fn build_batch_covers_sql_storage_families_and_null_bitmaps() {
        let enum_ty = DataType::Enum {
            oid: Oid::new(100),
            name: Arc::from("mood"),
            labels: Arc::from(vec!["ok".to_owned()].into_boxed_slice()),
        };
        let composite_ty = DataType::Composite {
            oid: Oid::new(101),
            name: Arc::from("pair"),
            fields: Arc::from(
                vec![
                    ("x".to_owned(), DataType::Int32),
                    ("y".to_owned(), DataType::Int32),
                ]
                .into_boxed_slice(),
            ),
        };
        let schema = Schema::new([
            Field::nullable("n", DataType::Null),
            Field::nullable("b", DataType::Bool),
            Field::nullable("i2", DataType::Int16),
            Field::nullable("i4", DataType::Int32),
            Field::nullable("i8", DataType::Int64),
            Field::nullable("oid", DataType::Oid),
            Field::nullable("regclass", DataType::RegClass),
            Field::nullable("regtype", DataType::RegType),
            Field::nullable("f4", DataType::Float32),
            Field::nullable("f8", DataType::Float64),
            Field::nullable("txt", DataType::Text { max_len: None }),
            Field::nullable("enum", enum_ty),
            Field::nullable("composite", composite_ty),
            Field::nullable("char", DataType::Char { len: Some(4) }),
            Field::nullable("bit", DataType::Bit { len: Some(4) }),
            Field::nullable("varbit", DataType::VarBit { max_len: Some(8) }),
            Field::nullable("inet", DataType::Inet),
            Field::nullable("cidr", DataType::Cidr),
            Field::nullable("mac", DataType::MacAddr),
            Field::nullable("mac8", DataType::MacAddr8),
            Field::nullable("json", DataType::Json),
            Field::nullable("jsonb", DataType::Jsonb),
            Field::nullable("xml", DataType::Xml),
            Field::nullable("lsn", DataType::PgLsn),
            Field::nullable("vec", DataType::Vector { dims: Some(2) }),
            Field::nullable("half", DataType::HalfVec { dims: Some(2) }),
            Field::nullable("sparse", DataType::SparseVec { dims: Some(5) }),
            Field::nullable("bitvec", DataType::BitVec { dims: Some(4) }),
            Field::nullable("range", DataType::Range(RangeType::Int4)),
            Field::nullable("array", DataType::Array(Box::new(DataType::Int32))),
            Field::nullable("geom", DataType::Geometry(GeometryType::Box)),
            Field::nullable("uuid", DataType::Uuid),
            Field::nullable("bytea", DataType::Bytea),
            Field::nullable("date", DataType::Date),
            Field::nullable(
                "dec",
                DataType::Decimal {
                    precision: None,
                    scale: Some(2),
                },
            ),
            Field::nullable("money", DataType::Money),
            Field::nullable("ts", DataType::Timestamp),
            Field::nullable("tstz", DataType::TimestampTz),
            Field::nullable("time", DataType::Time),
            Field::nullable("timetz", DataType::TimeTz),
        ])
        .expect("schema");
        let row = vec![
            Value::Null,
            Value::Bool(true),
            Value::Int16(7),
            Value::Int32(8),
            Value::Int64(9),
            Value::Oid(Oid::new(10)),
            Value::RegClass(Oid::new(11)),
            Value::RegType(Oid::new(12)),
            Value::Float32(1.5),
            Value::Float64(2.5),
            Value::Text("txt".into()),
            Value::Text("ok".into()),
            Value::Text("(1,2)".into()),
            Value::Text("xy".into()),
            Value::BitString(BitString::parse("1010").expect("bit")),
            Value::BitString(BitString::parse("101011").expect("varbit")),
            Value::Network(
                NetworkValue::parse_for_type(&DataType::Inet, "127.0.0.1").expect("inet"),
            ),
            Value::Network(
                NetworkValue::parse_for_type(&DataType::Cidr, "10.0.0.0/24").expect("cidr"),
            ),
            Value::Network(
                NetworkValue::parse_for_type(&DataType::MacAddr, "08:00:2b:01:02:03").expect("mac"),
            ),
            Value::Network(
                NetworkValue::parse_for_type(&DataType::MacAddr8, "08:00:2b:ff:fe:01:02:03")
                    .expect("mac8"),
            ),
            Value::Json(r#"{"a":1}"#.into()),
            Value::Jsonb(r#"{"a":1}"#.into()),
            Value::Xml("<r/>".into()),
            Value::PgLsn(Lsn::new(0x1_0000_0002)),
            Value::Vector(vec![1.0, 2.0]),
            Value::HalfVec(vec![3.0, 4.0]),
            Value::SparseVec(SparseVector {
                dims: 5,
                entries: vec![(1, 1.0), (3, 2.0)],
            }),
            Value::BitVec {
                dims: 4,
                bytes: vec![0b1010_0000],
            },
            Value::Range(RangeValue::parse(RangeType::Int4, "[1,4)").expect("range")),
            Value::Array {
                element_type: DataType::Int32,
                elements: vec![Value::Int32(1), Value::Null],
            },
            Value::Geometry(GeometryValue::parse(GeometryType::Box, "((0,0),(1,1))").expect("box")),
            Value::Uuid([7; 16]),
            Value::Bytea(vec![0xde, 0xad]),
            Value::Date(42),
            Value::Decimal {
                value: 1234,
                scale: 2,
            },
            Value::Money(5678),
            Value::Timestamp(111),
            Value::TimestampTz(222),
            Value::Time(333),
            Value::TimeTz {
                micros: 1_000,
                offset_seconds: -3_600,
            },
        ];
        let null_row = vec![Value::Null; schema.len()];
        let batch = build_batch(&[row, null_row], &schema).expect("batch");

        assert_eq!(batch.rows(), 2);
        assert_eq!(batch.width(), schema.len());
        match &batch.columns()[1] {
            Column::Bool(c) => assert!(c.nulls().is_some_and(|n| n.get(0) && !n.get(1))),
            other => panic!("expected bool column, got {other:?}"),
        }
        match &batch.columns()[34] {
            Column::Int64(c) => assert_eq!(c.data()[0], 1234),
            other => panic!("expected decimal int64 column, got {other:?}"),
        }

        let bad = build_batch(&[vec![Value::Text("bad".into())]], &schema_i32_only())
            .expect_err("type mismatch");
        assert!(matches!(bad, ExecError::TypeMismatch(_)));
    }

    #[test]
    fn cache_helpers_slice_numeric_columns_and_gate_supported_schemas() {
        let mut nulls = Bitmap::new(5, true);
        nulls.set(2, false);
        let int_col = Column::Int32(
            NumericColumn::with_nulls(vec![1, 2, 3, 4, 5], nulls.clone()).expect("int col"),
        );
        match slice_column(&int_col, 1, 4) {
            Column::Int32(c) => {
                assert_eq!(c.data(), &[2, 3, 4]);
                assert!(c.nulls().is_some_and(|n| n.get(0) && !n.get(1) && n.get(2)));
            }
            other => panic!("expected int32 column, got {other:?}"),
        }
        match slice_column(
            &Column::Float64(NumericColumn::from_data(vec![1.0, 2.0, 3.0, 4.0])),
            1,
            3,
        ) {
            Column::Float64(c) => assert_eq!(c.data(), &[2.0, 3.0]),
            other => panic!("expected float64 column, got {other:?}"),
        }

        let numeric_schema = Schema::new([
            Field::required("i2", DataType::Int16),
            Field::required("oid", DataType::Oid),
            Field::required("f8", DataType::Float64),
        ])
        .expect("schema");
        let text_schema =
            Schema::new([Field::required("t", DataType::Text { max_len: None })]).expect("schema");
        assert!(schema_all_fixed_numeric(&numeric_schema));
        assert!(!schema_all_fixed_numeric(&text_schema));
        assert_eq!(payload_prefix(&[0, 1, 0xab, 0xff]), "0001abff");
    }

    fn drain_rows(scan: &mut dyn Operator) -> Vec<(i32, String)> {
        let mut out = Vec::new();
        while let Some(batch) = scan.next_batch().expect("operator must not error") {
            let cols = batch.columns();
            assert_eq!(cols.len(), 2);
            let ids = match &cols[0] {
                Column::Int32(c) => c.data().to_vec(),
                other => panic!("expected Int32, got {other:?}"),
            };
            let names: Vec<String> = match &cols[1] {
                col @ (Column::Utf8(_) | Column::DictionaryUtf8(_)) => (0..col.len())
                    .map(|i| {
                        col.text_value(i)
                            .expect("test scan text column should be non-null")
                            .to_owned()
                    })
                    .collect(),
                other => panic!("expected Utf8, got {other:?}"),
            };
            assert_eq!(ids.len(), names.len());
            for (id, name) in ids.into_iter().zip(names) {
                out.push((id, name));
            }
        }
        out
    }

    #[test]
    fn scan_returns_inserted_rows_in_insert_order() {
        let heap = make_heap();
        let codec = RowCodec::new(schema_i32_text());
        let xid: u64 = 10;
        let oracle = Arc::new(MapOracle::new());
        oracle.set_committed(Xid::new(xid));

        let expected: Vec<(i32, String)> = (0_i32..10).map(|i| (i, format!("row_{i}"))).collect();
        for (id, name) in &expected {
            let row = vec![Value::Int32(*id), Value::Text(name.clone())];
            let payload = codec.encode(&row).expect("encode");
            heap.insert(rel(), &payload, insert_opts(xid))
                .expect("insert");
        }

        let block_count = heap.block_count(rel());
        let snapshot = snap_for(xid);
        let mut scan = SeqScan::new(
            Arc::clone(&heap),
            rel(),
            block_count,
            snapshot,
            Arc::clone(&oracle),
            codec,
        );

        let rows = drain_rows(&mut scan);
        assert_eq!(rows, expected, "scan returned rows in wrong order");
    }

    #[test]
    fn scan_filters_invisible_rows() {
        let heap = make_heap();
        let codec = RowCodec::new(schema_i32_text());
        let xid_committed: u64 = 20;
        let xid_aborted: u64 = 21;
        let oracle = Arc::new(MapOracle::new());
        oracle.set_committed(Xid::new(xid_committed));
        oracle.set_aborted(Xid::new(xid_aborted));

        let committed_rows: Vec<(i32, String)> =
            (0_i32..5).map(|i| (i, format!("committed_{i}"))).collect();
        let aborted_rows: Vec<(i32, String)> = (100_i32..105)
            .map(|i| (i, format!("aborted_{i}")))
            .collect();

        for (id, name) in &committed_rows {
            let row = vec![Value::Int32(*id), Value::Text(name.clone())];
            let payload = codec.encode(&row).expect("encode");
            heap.insert(rel(), &payload, insert_opts(xid_committed))
                .expect("insert");
        }
        for (id, name) in &aborted_rows {
            let row = vec![Value::Int32(*id), Value::Text(name.clone())];
            let payload = codec.encode(&row).expect("encode");
            heap.insert(rel(), &payload, insert_opts(xid_aborted))
                .expect("insert");
        }

        let snapshot = Snapshot::new(
            Xid::new(xid_aborted + 1),
            Xid::new(xid_aborted + 2),
            Xid::new(xid_aborted + 1),
            CommandId::FIRST,
            [],
        );
        let block_count = heap.block_count(rel());
        let mut scan = SeqScan::new(
            Arc::clone(&heap),
            rel(),
            block_count,
            snapshot,
            Arc::clone(&oracle),
            codec,
        );

        let rows = drain_rows(&mut scan);
        assert_eq!(
            rows, committed_rows,
            "scan should only return committed rows"
        );
    }

    #[test]
    fn scan_chunks_into_4096_row_batches() {
        let heap = make_heap();
        let codec = RowCodec::new(schema_i32_text());
        let xid: u64 = 30;
        let oracle = Arc::new(MapOracle::new());
        oracle.set_committed(Xid::new(xid));

        let total = 4100_usize;
        for i in 0_i32..i32::try_from(total).expect("fits i32") {
            let row = vec![Value::Int32(i), Value::Text(format!("r{i}"))];
            let payload = codec.encode(&row).expect("encode");
            heap.insert(rel(), &payload, insert_opts(xid))
                .expect("insert");
        }

        let block_count = heap.block_count(rel());
        let snapshot = snap_for(xid);
        let mut scan = SeqScan::new(
            Arc::clone(&heap),
            rel(),
            block_count,
            snapshot,
            Arc::clone(&oracle),
            codec,
        );

        let mut batch_sizes: Vec<usize> = Vec::new();
        while let Some(batch) = scan.next_batch().expect("operator must not error") {
            batch_sizes.push(batch.rows());
        }

        let total_scanned: usize = batch_sizes.iter().sum();
        assert_eq!(total_scanned, total, "total rows mismatch");
        assert!(
            batch_sizes.contains(&4096),
            "expected at least one full 4096-row batch, got {batch_sizes:?}"
        );
        assert_eq!(
            *batch_sizes.last().expect("at least one batch"),
            total % 4096,
            "remainder batch size mismatch"
        );
    }

    #[test]
    fn scan_empty_relation_returns_none() {
        let heap = make_heap();
        let codec = RowCodec::new(schema_i32_text());
        let oracle = Arc::new(MapOracle::new());
        let snapshot = snap_for(1);
        let mut scan = SeqScan::new(
            Arc::clone(&heap),
            rel(),
            0,
            snapshot,
            Arc::clone(&oracle),
            codec,
        );

        let result = scan.next_batch().expect("operator must not error");
        assert!(
            result.is_none(),
            "empty relation must return None immediately"
        );
    }

    #[test]
    fn tid_scan_prepends_block_and_slot_columns() {
        let heap = make_heap();
        let codec = RowCodec::new(schema_i32_text());
        let xid: u64 = 50;
        let oracle = Arc::new(MapOracle::new());
        oracle.set_committed(Xid::new(xid));

        let inputs: Vec<(i32, String)> = (0_i32..3).map(|i| (i, format!("row_{i}"))).collect();
        for (id, name) in &inputs {
            let row = vec![Value::Int32(*id), Value::Text(name.clone())];
            let payload = codec.encode(&row).expect("encode");
            heap.insert(rel(), &payload, insert_opts(xid))
                .expect("insert");
        }

        let block_count = heap.block_count(rel());
        let snapshot = snap_for(xid);
        let mut scan = SeqScan::new_with_tids(
            Arc::clone(&heap),
            rel(),
            block_count,
            snapshot,
            Arc::clone(&oracle),
            codec,
        );

        let schema = scan.schema().clone();
        assert_eq!(schema.len(), 4, "TID schema must have 4 columns");
        assert_eq!(schema.field_at(0).name, "tid_block");
        assert_eq!(schema.field_at(0).data_type, DataType::Int32);
        assert_eq!(schema.field_at(1).name, "tid_slot");
        assert_eq!(schema.field_at(1).data_type, DataType::Int32);

        let batch = scan
            .next_batch()
            .expect("must not error")
            .expect("first batch");
        assert_eq!(batch.rows(), 3);
        assert_eq!(batch.width(), 4);
        let block_col = match &batch.columns()[0] {
            Column::Int32(c) => c.data().to_vec(),
            other => panic!("expected Int32 for tid_block, got {other:?}"),
        };
        assert_eq!(block_col, vec![0_i32, 0, 0]);
        let slot_col = match &batch.columns()[1] {
            Column::Int32(c) => c.data().to_vec(),
            other => panic!("expected Int32 for tid_slot, got {other:?}"),
        };
        assert_eq!(slot_col, vec![0_i32, 1, 2]);
        let id_col = match &batch.columns()[2] {
            Column::Int32(c) => c.data().to_vec(),
            other => panic!("expected Int32 for id, got {other:?}"),
        };
        assert_eq!(id_col, vec![0_i32, 1, 2]);
    }

    #[test]
    fn scan_propagates_codec_errors_as_type_mismatch() {
        let heap = make_heap();
        let codec = RowCodec::new(schema_i32_text());
        let xid: u64 = 40;
        let oracle = Arc::new(MapOracle::new());
        oracle.set_committed(Xid::new(xid));

        let corrupt_payload = vec![0xDE, 0xAD];
        heap.insert(rel(), &corrupt_payload, insert_opts(xid))
            .expect("insert corrupt payload");

        let block_count = heap.block_count(rel());
        let snapshot = snap_for(xid);
        let mut scan = SeqScan::new(
            Arc::clone(&heap),
            rel(),
            block_count,
            snapshot,
            Arc::clone(&oracle),
            codec,
        );

        let err = scan.next_batch().expect_err("corrupt payload must error");
        assert!(
            matches!(err, ExecError::TypeMismatch(_)),
            "expected TypeMismatch, got {err:?}"
        );
    }

    // -----------------------------------------------------------------------
    // New streaming tests
    // -----------------------------------------------------------------------

    /// Verify that an 8200-row heap streams out as batches of 4096,
    /// 4096 and 8 — confirming the operator no longer pre-materialises
    /// every row before yielding the first batch.
    #[test]
    fn streaming_seq_scan_emits_4096_chunks() {
        let heap = make_heap();
        let codec = RowCodec::new(schema_i32_only());
        let xid: u64 = 60;
        let oracle = Arc::new(MapOracle::new());
        oracle.set_committed(Xid::new(xid));

        let total = 8200_usize;
        for i in 0_i32..i32::try_from(total).expect("fits i32") {
            let row = vec![Value::Int32(i)];
            let payload = codec.encode(&row).expect("encode");
            heap.insert(rel(), &payload, insert_opts(xid))
                .expect("insert");
        }

        let block_count = heap.block_count(rel());
        let snapshot = snap_for(xid);
        let mut scan = SeqScan::new(
            Arc::clone(&heap),
            rel(),
            block_count,
            snapshot,
            Arc::clone(&oracle),
            codec,
        );

        let mut sizes: Vec<usize> = Vec::new();
        while let Some(batch) = scan.next_batch().expect("operator must not error") {
            sizes.push(batch.rows());
        }
        assert_eq!(
            sizes,
            vec![4096, 4096, 8],
            "streaming scan must emit 4096 + 4096 + 8, got {sizes:?}"
        );
    }

    /// Verify content equality with the legacy output: streamed rows
    /// preserve insertion order over a 10k-row heap.
    #[test]
    fn streaming_seq_scan_matches_old_output() {
        let heap = make_heap();
        let codec = RowCodec::new(schema_i32_only());
        let xid: u64 = 70;
        let oracle = Arc::new(MapOracle::new());
        oracle.set_committed(Xid::new(xid));

        let total = 10_000_i32;
        for i in 0..total {
            let row = vec![Value::Int32(i)];
            let payload = codec.encode(&row).expect("encode");
            heap.insert(rel(), &payload, insert_opts(xid))
                .expect("insert");
        }

        let block_count = heap.block_count(rel());
        let snapshot = snap_for(xid);
        let mut scan = SeqScan::new(
            Arc::clone(&heap),
            rel(),
            block_count,
            snapshot,
            Arc::clone(&oracle),
            codec,
        );

        let mut streamed: Vec<i32> = Vec::with_capacity(total as usize);
        while let Some(batch) = scan.next_batch().expect("operator must not error") {
            match &batch.columns()[0] {
                Column::Int32(c) => streamed.extend_from_slice(c.data()),
                other => panic!("expected Int32 column, got {other:?}"),
            }
        }

        let expected: Vec<i32> = (0..total).collect();
        assert_eq!(
            streamed, expected,
            "streaming output diverges from insertion order"
        );
    }

    /// Smoke test the null-bitmap routing: alternate rows have NULL
    /// in column 1 and the resulting column's bitmap matches.
    #[test]
    fn streaming_seq_scan_routes_nulls_into_bitmap() {
        let heap = make_heap();
        let schema = Schema::new([
            Field::required("id", DataType::Int32),
            Field::nullable("score", DataType::Int64),
        ])
        .expect("schema ok");
        let codec = RowCodec::new(schema);
        let xid: u64 = 80;
        let oracle = Arc::new(MapOracle::new());
        oracle.set_committed(Xid::new(xid));

        let total: i32 = 32;
        for i in 0..total {
            let row = if i % 2 == 0 {
                vec![Value::Int32(i), Value::Null]
            } else {
                vec![Value::Int32(i), Value::Int64(i64::from(i) * 10)]
            };
            let payload = codec.encode(&row).expect("encode");
            heap.insert(rel(), &payload, insert_opts(xid))
                .expect("insert");
        }

        let block_count = heap.block_count(rel());
        let snapshot = snap_for(xid);
        let mut scan = SeqScan::new(
            Arc::clone(&heap),
            rel(),
            block_count,
            snapshot,
            Arc::clone(&oracle),
            codec,
        );

        let batch = scan
            .next_batch()
            .expect("operator must not error")
            .expect("first batch");
        let score_col = match &batch.columns()[1] {
            Column::Int64(c) => c,
            other => panic!("expected Int64 score, got {other:?}"),
        };
        let nulls = score_col
            .nulls()
            .expect("null bitmap must be present after observing nulls");
        for i in 0..(total as usize) {
            let is_valid_expected = i % 2 == 1;
            assert_eq!(
                nulls.get(i),
                is_valid_expected,
                "row {i}: expected valid={is_valid_expected}, got bit={}",
                nulls.get(i)
            );
        }
        for (i, &v) in score_col.data().iter().enumerate() {
            if i % 2 == 0 {
                assert_eq!(v, 0, "row {i}: null placeholder must be 0");
            } else {
                assert_eq!(
                    v,
                    i64::from(i32::try_from(i).expect("fits i32")) * 10,
                    "row {i}: non-null value must round-trip"
                );
            }
        }
    }
}
