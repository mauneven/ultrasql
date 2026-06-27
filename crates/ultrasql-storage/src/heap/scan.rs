//! See `crate::heap` for the public API.
//!
//! Part of the `heap` module split — each `impl<L: PageLoader>
//! HeapAccess<L>` block here adds methods to the type defined in
//! `heap/mod.rs`. Splitting across files keeps each unit under the
//! 600-line ceiling without changing semantics.

use dashmap::DashMap;
use std::sync::Arc;
use ultrasql_core::{BlockNumber, PageId, RelationId, TupleId, Xid};
use ultrasql_mvcc::tuple_header::TUPLE_HEADER_SIZE;
use ultrasql_mvcc::{Snapshot, TupleHeader, Visibility, XidStatusOracle, is_visible};

use crate::buffer_pool::{BufferPool, PageGuard, PageLoader};
use crate::page::PageError;
use crate::vm::VisibilityMap;

use super::walker::VisibleHeapWalker;
use super::{HeapAccess, HeapError, HeapTuple, UndoRelationLog, undo_pre_image_from_log};

impl<L: PageLoader> HeapAccess<L> {
    /// Read a tuple by id. Visibility is not enforced — callers running
    /// a scan should consult [`ultrasql_mvcc::is_visible`] before
    /// surfacing the tuple to user code.
    pub fn fetch(&self, tid: TupleId) -> Result<HeapTuple, HeapError> {
        let guard = self.get_page_relieved(tid.page)?;
        let owned = Self::copy_slot_bytes(&guard, tid.slot)?;
        Self::decode_tuple(tid, &owned)
    }

    /// Fetch the pre-image payload bytes a reader should observe for a
    /// tuple whose visibility resolved to
    /// [`Visibility::VisiblePreImage`] — i.e. the slot currently holds the
    /// post-image of an in-place UPDATE the reader's snapshot does not yet
    /// see committed (or which a rolled-back subxid wrote).
    ///
    /// This is the same per-relation undo-log lookup the visibility-
    /// filtered sequential scan performs (`for_each_visible` /
    /// `VisibleHeapScan::next`), exposed so the **index** read paths can
    /// surface the identical pre-image row a seq scan does (design §3 R6).
    /// Returns the pre-image payload (header stripped) or `None` when the
    /// undo entry has been trimmed by VACUUM — in which case the caller
    /// treats the tuple as not visible (the safe direction).
    pub fn fetch_visible_pre_image<O: XidStatusOracle + ?Sized>(
        &self,
        tid: TupleId,
        snapshot: &Snapshot,
        oracle: &O,
    ) -> Result<Option<Vec<u8>>, HeapError> {
        let tuple = self.fetch(tid)?;
        Ok(Self::lookup_undo_pre_image(
            &self.undo_log,
            tid.page.relation,
            tid,
            &tuple.header,
            &tuple.data,
            snapshot,
            oracle,
        ))
    }

    /// Visibility-filtered sequential scan that yields **borrowed**
    /// slot bytes — zero `Vec<u8>` allocations per tuple.
    ///
    /// [`Self::scan_visible`] yields a fully-owned `HeapTuple` whose
    /// `data: Vec<u8>` is a fresh allocation per slot. On a 1 M-row
    /// analytic scan that path pays ~1 M allocator dispatches + 1 M
    /// `Vec::drop` calls — measurable wall time even on a hot
    /// allocator.
    ///
    /// `scan_visible_walker` returns a [`VisibleHeapWalker`] whose
    /// `try_next` writes the slot bytes into a reusable internal
    /// buffer (preallocated to one tuple's worth) and hands the
    /// caller a `&[u8]` view into that buffer. The borrow is valid
    /// until the next `try_next` call.
    /// Callback-style visitor over every MVCC-visible tuple in `rel`.
    ///
    /// Faster than [`Self::scan_visible_walker`] when the caller does
    /// not need to escape with a `&[u8]` reference between yields. The
    /// walker variant memcpys each 8 KiB page into a scratch buffer so
    /// the page lock is released between slot reads; for relations
    /// scanned end-to-end inside one operator there is no other lock
    /// contender, and the memcpy is pure overhead. `for_each_visible`
    /// holds the page-read guard for the whole slot loop on one page
    /// and invokes `f(tid, payload)` per visible slot directly off the
    /// page bytes — no per-page memcpy, no per-slot tuple-copy.
    ///
    /// Visibility uses the same path as `scan_visible_walker`
    /// (`is_visible` against `snapshot` / `oracle`, with the
    /// per-`xmin` cache).
    ///
    /// # Errors
    ///
    /// Buffer-pool pin failures or malformed page metadata return
    /// [`HeapError`]; the visitor stops on the first error.
    pub fn for_each_visible<O, F>(
        &self,
        rel: RelationId,
        block_count: u32,
        snapshot: &Snapshot,
        oracle: &O,
        mut f: F,
    ) -> Result<(), HeapError>
    where
        O: XidStatusOracle + ?Sized,
        F: FnMut(TupleId, &TupleHeader, &[u8]) -> Result<(), HeapError>,
    {
        // Visibility-cache key: `(xmin, infomask_bits)` → visible.
        // The on-disk tuple-header layout (see [`TupleHeader::encode`])
        // packs xmin at bytes 0..8, xmax at 8..16, infomask at 24..26.
        // Reading those three fields covers everything the cached
        // visibility decision depends on, so the full
        // [`TupleHeader::decode`] (which also decodes `cmin`, `cmax`,
        // `n_atts`, `data_offset`, `ctid`) is pure waste on the hot
        // path: those four/five extra fields are never consulted by
        // `is_visible` and not read by the callback in the bench
        // shape. Skipping them drops ~30 bytes of per-tuple decode
        // work; the slow `is_visible` path on a cache miss still
        // pays the full `TupleHeader::decode` to materialise a
        // `&TupleHeader` for the oracle and the callback.
        let mut xmin_cache: Option<(Xid, u16, bool)> = None;
        for block in 0..block_count {
            let page_id = PageId::new(rel, BlockNumber::new(block));
            let guard = self.get_page_relieved(page_id)?;
            let page = guard.read();
            let page_bytes = page.as_bytes();
            let slot_count = page.header().slot_count();
            for slot in 0..slot_count {
                let item_id_off =
                    crate::page::PAGE_HEADER_SIZE + usize::from(slot) * crate::page::ITEMID_SIZE;
                let raw = u32::from_le_bytes([
                    page_bytes[item_id_off],
                    page_bytes[item_id_off + 1],
                    page_bytes[item_id_off + 2],
                    page_bytes[item_id_off + 3],
                ]);
                let flags = raw & 0b11;
                if flags != 1 {
                    // ItemIdFlags::Normal == 1; skip Unused / Dead / Redirect.
                    continue;
                }
                let length = usize::try_from((raw >> 2) & 0x7FFF)
                    .map_err(|_| HeapError::MalformedHeader("slot length overflow"))?;
                let offset = usize::try_from((raw >> 17) & 0x7FFF)
                    .map_err(|_| HeapError::MalformedHeader("slot offset overflow"))?;
                if length < TUPLE_HEADER_SIZE
                    || offset
                        .checked_add(length)
                        .is_none_or(|end| end > page_bytes.len())
                {
                    return Err(HeapError::MalformedHeader("slot shorter than header"));
                }
                let slot_bytes = &page_bytes[offset..offset + length];

                // Minimal-decode visibility cache lookup. Reads only
                // `xmin` (bytes 0..8), `xmax` (8..16), `infomask`
                // (24..26) — see the comment above the loop.
                let xmin_raw = u64::from_le_bytes([
                    slot_bytes[0],
                    slot_bytes[1],
                    slot_bytes[2],
                    slot_bytes[3],
                    slot_bytes[4],
                    slot_bytes[5],
                    slot_bytes[6],
                    slot_bytes[7],
                ]);
                let xmax_raw = u64::from_le_bytes([
                    slot_bytes[8],
                    slot_bytes[9],
                    slot_bytes[10],
                    slot_bytes[11],
                    slot_bytes[12],
                    slot_bytes[13],
                    slot_bytes[14],
                    slot_bytes[15],
                ]);
                let infomask_bits = u16::from_le_bytes([slot_bytes[24], slot_bytes[25]]);
                let xmin_xid = Xid::new(xmin_raw);

                // Fast path: xmax invalid (alive tuple) AND cached
                // `(xmin, infomask)` says visible. Avoids
                // `TupleHeader::decode` and `is_visible` entirely.
                if xmax_raw == 0 {
                    if let Some((cxmin, cinfo, true)) = xmin_cache {
                        if cxmin == xmin_xid && cinfo == infomask_bits {
                            let tid = TupleId::new(page_id, slot);
                            // Reconstruct a minimal `TupleHeader`
                            // view for the callback. The callback in
                            // the fused-UPDATE path only reads the
                            // payload; constructing the header with
                            // the three fields we already decoded
                            // keeps the existing `FnMut(tid, header,
                            // payload)` contract intact without
                            // re-paying the full header decode.
                            let header =
                                TupleHeader::minimal_for_visible_cache_hit(xmin_xid, infomask_bits);
                            f(tid, &header, &slot_bytes[TUPLE_HEADER_SIZE..])?;
                            continue;
                        }
                    }
                }

                // Slow path: cache miss, or xmax set. Pay the full
                // header decode so `is_visible` can examine every
                // field it needs.
                let (header, _) = TupleHeader::decode(&slot_bytes[..TUPLE_HEADER_SIZE])
                    .ok_or(HeapError::MalformedHeader("header decode failed"))?;
                let outcome = is_visible(&header, snapshot, oracle);
                if header.xmax.is_invalid() {
                    let visible = matches!(outcome, Visibility::Visible);
                    xmin_cache = Some((header.xmin, header.infomask.bits(), visible));
                }
                let tid = TupleId::new(page_id, slot);
                match outcome {
                    Visibility::Visible => {
                        f(tid, &header, &slot_bytes[TUPLE_HEADER_SIZE..])?;
                    }
                    Visibility::VisiblePreImage => {
                        // The slot's payload is the post-image of an
                        // in-place UPDATE the reader's snapshot does
                        // not yet see committed. Substitute the
                        // pre-image from the per-relation undo log.
                        // Multiple in-place updates on the same slot
                        // form a chain; we reverse every writer the
                        // snapshot cannot see (full-payload: take the
                        // oldest invisible writer's pre-image; compact:
                        // subtract the sum of all invisible deltas). A
                        // missing entry means VACUUM trimmed it after we
                        // lost the right to see the pre-image — treat as
                        // invisible (the safe direction).
                        if let Some(pre) = Self::lookup_undo_pre_image(
                            &self.undo_log,
                            rel,
                            tid,
                            &header,
                            &slot_bytes[TUPLE_HEADER_SIZE..],
                            snapshot,
                            oracle,
                        ) {
                            f(tid, &header, &pre)?;
                        }
                    }
                    Visibility::Invisible | Visibility::DeletedByOwn => {}
                }
            }
            drop(page);
            drop(guard);
        }
        Ok(())
    }

    /// Reconstruct the pre-image this `snapshot` must observe for an
    /// in-place-updated slot, by reversing every undo record whose
    /// `writer_xid` is invisible to the snapshot.
    ///
    /// Full-payload entries store the payload *before* their writer
    /// applied, so the correct pre-image is the **oldest** invisible
    /// writer's `old_payload` (the last state committed before the
    /// snapshot). Compact int32-pair batches store a signed delta per
    /// writer, so the pre-image is `current − sum(invisible deltas)`
    /// per affected column. Writers the snapshot *can* see are already
    /// reflected in the current payload and must NOT be reversed.
    /// Returns owned bytes so the scan callback can keep its borrow
    /// across iterations.
    ///
    /// Lock order: caller has already dropped (or never acquired)
    /// the page-write guard; this only takes the per-relation
    /// `RwLock<UndoRelationLog>` for a read.
    fn lookup_undo_pre_image<O: XidStatusOracle + ?Sized>(
        undo_log: &Arc<DashMap<RelationId, parking_lot::RwLock<UndoRelationLog>>>,
        rel: RelationId,
        tid: TupleId,
        _header: &TupleHeader,
        current_payload: &[u8],
        snapshot: &Snapshot,
        oracle: &O,
    ) -> Option<Vec<u8>> {
        let log = undo_log.get(&rel)?;
        let log = log.read();
        undo_pre_image_from_log(&log, tid, current_payload, snapshot, oracle)
    }

    pub fn scan_visible_walker<'a, O: XidStatusOracle + ?Sized>(
        &'a self,
        rel: RelationId,
        block_count: u32,
        snapshot: &'a Snapshot,
        oracle: &'a O,
    ) -> VisibleHeapWalker<'a, L, O> {
        self.scan_visible_walker_inner(rel, (0, 0), block_count, snapshot, oracle, None)
    }

    /// Visibility-filtered scan over a half-open block range, resuming
    /// from `(block, slot)`.
    ///
    /// Used by executor operators that persist scan position between
    /// output batches without storing a self-referential walker.
    pub fn scan_visible_walker_range_from_position<'a, O: XidStatusOracle + ?Sized>(
        &'a self,
        rel: RelationId,
        start: (u32, u16),
        end_block: u32,
        snapshot: &'a Snapshot,
        oracle: &'a O,
    ) -> VisibleHeapWalker<'a, L, O> {
        self.scan_visible_walker_inner(rel, start, end_block, snapshot, oracle, None)
    }

    /// Visibility-filtered sequential scan with a visibility-map fast path.
    ///
    /// For pages whose VM bit is all-visible, the walker yields normal
    /// tuples without per-tuple `is_visible` / transaction-status checks.
    /// The caller must only pass a VM maintained by the same heap mutation
    /// path: inserts, updates, and deletes must clear touched pages before
    /// vacuum marks them visible again.
    pub fn scan_visible_walker_with_vm<'a, O: XidStatusOracle + ?Sized>(
        &'a self,
        rel: RelationId,
        block_count: u32,
        snapshot: &'a Snapshot,
        oracle: &'a O,
        vm: &'a VisibilityMap,
    ) -> VisibleHeapWalker<'a, L, O> {
        self.scan_visible_walker_inner(rel, (0, 0), block_count, snapshot, oracle, Some(vm))
    }

    /// Visibility-map fast path over a half-open block range.
    pub fn scan_visible_walker_range_with_vm<'a, O: XidStatusOracle + ?Sized>(
        &'a self,
        rel: RelationId,
        start_block: u32,
        end_block: u32,
        snapshot: &'a Snapshot,
        oracle: &'a O,
        vm: &'a VisibilityMap,
    ) -> VisibleHeapWalker<'a, L, O> {
        self.scan_visible_walker_inner(rel, (start_block, 0), end_block, snapshot, oracle, Some(vm))
    }

    /// Visibility-map fast path over a half-open block range, resuming
    /// from `(block, slot)`.
    pub fn scan_visible_walker_range_from_position_with_vm<'a, O: XidStatusOracle + ?Sized>(
        &'a self,
        rel: RelationId,
        start: (u32, u16),
        end_block: u32,
        snapshot: &'a Snapshot,
        oracle: &'a O,
        vm: &'a VisibilityMap,
    ) -> VisibleHeapWalker<'a, L, O> {
        self.scan_visible_walker_inner(rel, start, end_block, snapshot, oracle, Some(vm))
    }

    fn scan_visible_walker_inner<'a, O: XidStatusOracle + ?Sized>(
        &'a self,
        rel: RelationId,
        start: (u32, u16),
        end_block: u32,
        snapshot: &'a Snapshot,
        oracle: &'a O,
        vm: Option<&'a VisibilityMap>,
    ) -> VisibleHeapWalker<'a, L, O> {
        let (start_block, start_slot) = start;
        VisibleHeapWalker {
            pool: &self.pool,
            rel,
            block_count: end_block,
            current_block: start_block.min(end_block),
            current_slot: start_slot,
            slot_count: 0,
            vm,
            current_block_all_visible: false,
            // Per-block bulk page copy: PAGE_SIZE bytes preallocated
            // once so the per-block `extend_from_slice(page_bytes)`
            // never reallocates. The previous per-slot read-lock /
            // tuple-copy cycle is replaced with one read-lock /
            // 8 KiB memcpy per block.
            page_scratch: Vec::with_capacity(ultrasql_core::constants::PAGE_SIZE),
            snapshot,
            oracle,
            xmin_cache: None,
            undo_log: Arc::clone(&self.undo_log),
            pre_image_scratch: Vec::new(),
        }
    }
}

/// Iterator yielded by [`HeapAccess::scan`].
///
/// Walks the relation block-by-block, pinning each page **exactly
/// once** for the duration of every slot read on that page, then
/// dropping the pin at the block boundary.
///
/// # Pin amortisation
///
/// The previous design re-pinned the page (i.e. one
/// `BufferPool::get_page` `DashMap` probe + one atomic-refcount bump per
/// frame) on every slot read. On a 1 M-row sequential scan over
/// ~3 000 pages with ~300 slots/page that paid ~1 M pin/unpin pairs
/// when only ~3 000 are strictly necessary. With the pin held across
/// the block, the per-slot cost drops to a single per-frame
/// `RwLock<Page>::read` acquire (uncontended, lock-free path under
/// `parking_lot`) — measurably ~50× cheaper.
///
/// The yielded `HeapTuple` still owns its `data: Vec<u8>` (the slot's
/// payload bytes are copied out under the per-frame read lock), so
/// the guard is safe to drop at the block boundary and no caller
/// lifetime escapes onto the page.
pub struct HeapScan<'a, L: PageLoader> {
    pub(super) pool: &'a Arc<BufferPool<L>>,
    pub(super) rel: RelationId,
    pub(super) block_count: u32,
    pub(super) current_block: u32,
    pub(super) current_slot: u16,
    pub(super) slot_cap: u16,
    /// Pinned page for `current_block`. `Some` once `current_slot`
    /// has been initialised from the page header; `None` between
    /// blocks. Dropped on block boundary so the buffer-pool frame
    /// becomes eligible for eviction again.
    pub(super) current_guard: Option<PageGuard<L>>,
}

impl<L: PageLoader> std::fmt::Debug for HeapScan<'_, L> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HeapScan")
            .field("rel", &self.rel)
            .field("block_count", &self.block_count)
            .field("current_block", &self.current_block)
            .field("current_slot", &self.current_slot)
            .finish_non_exhaustive()
    }
}

impl<L: PageLoader> Iterator for HeapScan<'_, L> {
    type Item = Result<HeapTuple, HeapError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.current_block >= self.block_count {
                self.current_guard = None;
                return None;
            }

            let page_id = PageId::new(self.rel, BlockNumber::new(self.current_block));

            // Lazy-pin the block on first entry; the pin is held for
            // every slot read on this page and dropped on the block
            // boundary below.
            if self.current_guard.is_none() {
                let guard = match self.pool.get_page_relieved(page_id) {
                    Ok(g) => g,
                    Err(e) => {
                        self.current_block = self.current_block.saturating_add(1);
                        self.current_slot = 0;
                        return Some(Err(HeapError::from(e)));
                    }
                };
                self.slot_cap = guard.read().header().slot_count();
                self.current_slot = 0;
                self.current_guard = Some(guard);
            }

            if self.current_slot >= self.slot_cap {
                // Drop the pin before advancing so the frame is
                // immediately eligible for eviction.
                self.current_guard = None;
                self.current_block = self.current_block.saturating_add(1);
                self.current_slot = 0;
                continue;
            }

            let slot = self.current_slot;
            self.current_slot += 1;

            // Read this slot through the held pin. `copy_slot_bytes`
            // acquires + releases the per-frame `RwLock<Page>` read
            // path under `parking_lot` (a CAS on the fast path); no
            // DashMap probe and no atomic-refcount bump per slot.
            let Some(guard) = self.current_guard.as_ref() else {
                return Some(Err(HeapError::MalformedHeader(
                    "heap scan guard missing before slot read",
                )));
            };
            let owned = match HeapAccess::<L>::copy_slot_bytes(guard, slot) {
                Ok(v) => v,
                // Skip non-normal slots (Unused/Dead/Redirect).
                Err(HeapError::Page(PageError::DeadSlot(_) | PageError::InvalidSlot { .. })) => {
                    continue;
                }
                Err(e) => return Some(Err(e)),
            };
            let tid = TupleId::new(page_id, slot);
            return Some(HeapAccess::<L>::decode_tuple(tid, &owned));
        }
    }
}

// ---------------------------------------------------------------------------
// Visibility-aware scan
// ---------------------------------------------------------------------------

/// Iterator yielded by [`HeapAccess::scan_visible`].
///
/// Wraps [`HeapScan`] and applies `is_visible` to each tuple before
/// yielding it. Tuples that are `Invisible` or `DeletedByOwn` are
/// silently skipped; only [`Visibility::Visible`] tuples reach the
/// caller.  I/O and decode errors are still propagated as
/// `Err(HeapError)`.
pub struct VisibleHeapScan<'a, L: PageLoader, O: XidStatusOracle + ?Sized> {
    pub(super) inner: HeapScan<'a, L>,
    pub(super) undo_log: &'a Arc<DashMap<RelationId, parking_lot::RwLock<UndoRelationLog>>>,
    pub(super) snapshot: &'a Snapshot,
    pub(super) oracle: &'a O,
    /// One-entry cache of `(xmin, infomask_bits) → visibility` valid
    /// only when the tuple has `xmax == Xid::INVALID`.
    ///
    /// For analytic scans the overwhelmingly common case is a long
    /// run of tuples sharing the same `xmin` (the preload's
    /// transaction) with default `infomask` and no deleter. The
    /// MVCC `is_visible` decision then depends only on whether
    /// `xmin` committed before the current snapshot — identical
    /// for every tuple in the run. We cache the boolean answer for
    /// the most-recent `(xmin, infomask)` key and short-circuit
    /// the full decision (including the per-tuple oracle.status
    /// `DashMap` probe) on match. Any deviation (non-invalid
    /// `xmax`, different infomask) falls through to the slow
    /// `is_visible` path without consulting the cache, and a fresh
    /// cache entry is recorded after the slow path completes.
    pub(super) xmin_cache: Option<(Xid, u16, bool)>,
}

impl<L: PageLoader, O: XidStatusOracle + ?Sized> std::fmt::Debug for VisibleHeapScan<'_, L, O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VisibleHeapScan")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------
// Zero-alloc visibility walker — see `HeapAccess::scan_visible_walker`.
// ---------------------------------------------------------------------------

impl<L: PageLoader, O: XidStatusOracle + ?Sized> Iterator for VisibleHeapScan<'_, L, O> {
    type Item = Result<HeapTuple, HeapError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.inner.next()? {
                Err(e) => return Some(Err(e)),
                Ok(tup) => {
                    // Fast path: same `(xmin, infomask)` as the last
                    // visibility decision *and* the tuple has no
                    // deleter (`xmax == Xid::INVALID`). On a hit we
                    // reuse the cached verdict and skip the
                    // oracle.status `DashMap` probe entirely. For
                    // the `select_avg_1m` / `filter_sum_1m` bench
                    // shape (one preload transaction, no deletes)
                    // this turns ~1 M oracle probes into one.
                    if tup.header.xmax.is_invalid() {
                        let infomask_bits = tup.header.infomask.bits();
                        if let Some((cached_xmin, cached_infomask, cached_visible)) =
                            self.xmin_cache
                        {
                            if cached_xmin == tup.header.xmin && cached_infomask == infomask_bits {
                                if cached_visible {
                                    return Some(Ok(tup));
                                }
                                continue;
                            }
                        }
                        // Cache miss: compute the full visibility
                        // decision once and stash the verdict for
                        // subsequent matching tuples.
                        let visible = matches!(
                            is_visible(&tup.header, self.snapshot, self.oracle),
                            Visibility::Visible,
                        );
                        self.xmin_cache = Some((tup.header.xmin, infomask_bits, visible));
                        if visible {
                            return Some(Ok(tup));
                        }
                        continue;
                    }
                    // Slow path for tuples with a non-invalid
                    // `xmax`: the visibility verdict depends on
                    // both `xmin` and `xmax` status; the
                    // single-key cache cannot model that without
                    // false positives, so we go through the full
                    // `is_visible` rules without touching the
                    // cache.
                    match is_visible(&tup.header, self.snapshot, self.oracle) {
                        Visibility::Visible => return Some(Ok(tup)),
                        Visibility::VisiblePreImage => {
                            let rel = tup.tid.page.relation;
                            if let Some(pre) = HeapAccess::<L>::lookup_undo_pre_image(
                                self.undo_log,
                                rel,
                                tup.tid,
                                &tup.header,
                                &tup.data,
                                self.snapshot,
                                self.oracle,
                            ) {
                                let mut tup = tup;
                                tup.data = pre;
                                return Some(Ok(tup));
                            }
                        }
                        Visibility::Invisible | Visibility::DeletedByOwn => {}
                    }
                    // Invisible (other txn in-progress, aborted, deleted
                    // before our snapshot) or DeletedByOwn — skip and
                    // continue the loop.
                }
            }
        }
    }
}
