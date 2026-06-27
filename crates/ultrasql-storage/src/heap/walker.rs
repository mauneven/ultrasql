//! See `crate::heap` for the public API.
//!
//! Part of the `heap` module split — each `impl<L: PageLoader>
//! HeapAccess<L>` block here adds methods to the type defined in
//! `heap/mod.rs`. Splitting across files keeps each unit under the
//! 600-line ceiling without changing semantics.

use std::sync::Arc;

use dashmap::DashMap;
use ultrasql_core::{BlockNumber, PageId, RelationId, TupleId, Xid};
use ultrasql_mvcc::tuple_header::TUPLE_HEADER_SIZE;
use ultrasql_mvcc::{Snapshot, TupleHeader, Visibility, XidStatusOracle, is_visible};

use crate::buffer_pool::{BufferPool, PageLoader};
use crate::page::ItemId;
use crate::vm::VisibilityMap;

use super::{HeapError, UndoRelationLog, undo_pre_image_from_log};

/// Tuple yielded by [`VisibleHeapWalker::try_next`].
///
/// The payload slice borrows from the walker's internal scratch buffer and is
/// invalidated by the next `try_next` call.
pub type VisibleTuple<'a> = (TupleId, TupleHeader, &'a [u8]);

/// Visibility-filtered sequential scan that yields borrowed slot
/// payload slices.
///
/// Constructed via [`crate::heap::HeapAccess::scan_visible_walker`]. The walker
/// owns one [`crate::buffer_pool::PageGuard`] at a time (released at block boundaries)
/// and one [`Vec<u8>`] scratch buffer reused across every slot read;
/// per-tuple work is zero allocation.
///
/// The borrow returned by [`Self::try_next`] is valid until the
/// next `try_next` call — the `&mut self` receiver prevents
/// overlapping borrows.
pub struct VisibleHeapWalker<'a, L: PageLoader, O: XidStatusOracle + ?Sized> {
    pub(super) pool: &'a Arc<BufferPool<L>>,
    pub(super) rel: RelationId,
    pub(super) block_count: u32,
    pub(super) current_block: u32,
    pub(super) current_slot: u16,
    pub(super) slot_count: u16,
    /// Optional VM consulted at block boundaries. When the current
    /// page is certified all-visible, every normal tuple can skip
    /// `is_visible` / CLOG probes because heap mutations clear this
    /// bit before the page can be trusted again.
    pub(super) vm: Option<&'a VisibilityMap>,
    pub(super) current_block_all_visible: bool,
    /// `PAGE_SIZE` (8 KiB) buffer holding the most-recent **whole**
    /// block's bytes. On block transition the walker pins the page
    /// once, acquires the per-frame read lock once, memcpys the 8 KiB
    /// page into this scratch, then drops the lock and the pin. Every
    /// per-slot read then walks the slot directory inside this
    /// buffer with no further lock acquires.
    ///
    /// The bulk copy is semantically equivalent to per-slot reads
    /// under a fixed snapshot: visibility decisions depend on
    /// `(header, snapshot, oracle.status(xid))`, all of which are
    /// monotone or fixed across the scan. A writer that mutates the
    /// page after our copy is seen by subsequent blocks but not by
    /// the current one — the same point-in-time view a per-slot
    /// reader would observe at its read time.
    pub(super) page_scratch: Vec<u8>,
    pub(super) snapshot: &'a Snapshot,
    pub(super) oracle: &'a O,
    /// Same `(xmin, infomask, visibility)` cache as `VisibleHeapScan`.
    pub(super) xmin_cache: Option<(Xid, u16, bool)>,
    /// Per-relation undo log, shared with the heap. Consulted when
    /// the visibility predicate returns
    /// [`Visibility::VisiblePreImage`] for an `UPDATED_IN_PLACE`
    /// tuple whose writer xmax is not visible to this snapshot.
    pub(super) undo_log: Arc<DashMap<RelationId, parking_lot::RwLock<UndoRelationLog>>>,
    /// Scratch buffer the walker copies a pre-image payload into
    /// when [`Visibility::VisiblePreImage`] fires. The returned
    /// `&[u8]` borrows from here; the borrow is invalidated by the
    /// next `try_next` call exactly like the page-scratch slot
    /// borrow.
    pub(super) pre_image_scratch: Vec<u8>,
}

impl<L: PageLoader, O: XidStatusOracle + ?Sized> std::fmt::Debug for VisibleHeapWalker<'_, L, O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VisibleHeapWalker")
            .field("rel", &self.rel)
            .field("current_block", &self.current_block)
            .field("current_slot", &self.current_slot)
            .finish_non_exhaustive()
    }
}

impl<L: PageLoader, O: XidStatusOracle + ?Sized> VisibleHeapWalker<'_, L, O> {
    /// Return the block/slot position where a future walker should resume.
    ///
    /// After [`Self::try_next`] yields a tuple, this position points to
    /// the next slot after that tuple. After EOF, `block >= block_count`.
    #[must_use]
    pub const fn resume_position(&self) -> (u32, u16) {
        (self.current_block, self.current_slot)
    }

    /// Advance to the next MVCC-visible tuple and return a borrowed
    /// view of its `(TupleId, TupleHeader, payload_bytes)`.
    ///
    /// Returns `Ok(None)` when the relation is exhausted, `Err(_)` on
    /// I/O / decode failure. The `payload_bytes` slice borrows from
    /// the walker's internal scratch buffer; the borrow is
    /// invalidated by the next call.
    pub fn try_next(&mut self) -> Result<Option<VisibleTuple<'_>>, HeapError> {
        loop {
            if self.current_block >= self.block_count {
                return Ok(None);
            }

            let page_id = PageId::new(self.rel, BlockNumber::new(self.current_block));

            // Block transition: pin + read-lock + memcpy 8 KiB to
            // scratch, then drop both lock and pin. Subsequent slot
            // reads work entirely off the local scratch buffer with
            // no further lock acquires.
            if self.page_scratch.is_empty() {
                let guard = match self.pool.get_page_relieved(page_id) {
                    Ok(g) => g,
                    Err(e) => {
                        self.current_block = self.current_block.saturating_add(1);
                        self.current_slot = 0;
                        return Err(HeapError::from(e));
                    }
                };
                {
                    let page = guard.read();
                    self.slot_count = page.header().slot_count();
                    self.page_scratch.clear();
                    self.page_scratch
                        .extend_from_slice(page.as_bytes().as_slice());
                }
                self.current_block_all_visible = self
                    .vm
                    .is_some_and(|vm| vm.is_all_visible(self.rel, page_id.block));
                drop(guard);
            }

            if self.current_slot >= self.slot_count {
                // Free the page buffer for the next block's memcpy.
                self.page_scratch.clear();
                self.current_block = self.current_block.saturating_add(1);
                self.current_slot = 0;
                continue;
            }

            let slot = self.current_slot;
            self.current_slot += 1;

            // Parse the slot directory entry from the cached page
            // bytes. The item-id layout matches `page::ItemId`:
            //   bits 0..2   flags (1 = Normal)
            //   bits 2..17  length (15 bits)
            //   bits 17..32 offset (15 bits)
            let item_id_off = ultrasql_storage_page_item_id_offset(slot);
            // `item_id_off + 4 <= PAGE_HEADER_SIZE + slot_count * 4 <= PAGE_SIZE`
            // because `slot < slot_count` guards the high bound and
            // `page_scratch` always holds a full page.
            let raw = u32::from_le_bytes([
                self.page_scratch[item_id_off],
                self.page_scratch[item_id_off + 1],
                self.page_scratch[item_id_off + 2],
                self.page_scratch[item_id_off + 3],
            ]);
            let item_id = ItemId::from_raw(raw);
            if !item_id.is_normal() {
                continue;
            }
            let length = usize::try_from(item_id.length())
                .map_err(|_| HeapError::MalformedHeader("slot length out of range"))?;
            let offset = usize::try_from(item_id.offset())
                .map_err(|_| HeapError::MalformedHeader("slot offset out of range"))?;
            if length < TUPLE_HEADER_SIZE
                || offset
                    .checked_add(length)
                    .is_none_or(|end| end > self.page_scratch.len())
            {
                return Err(HeapError::MalformedHeader("slot shorter than header"));
            }
            let slot_bytes = &self.page_scratch[offset..offset + length];
            let (header, _) = TupleHeader::decode(&slot_bytes[..TUPLE_HEADER_SIZE])
                .ok_or(HeapError::MalformedHeader("header decode failed"))?;

            if self.current_block_all_visible {
                let tid = TupleId::new(page_id, slot);
                let payload = &self.page_scratch[offset + TUPLE_HEADER_SIZE..offset + length];
                return Ok(Some((tid, header, payload)));
            }

            // Run the full visibility predicate. The cache below
            // only short-circuits the `Visible` ⇄ `Invisible` axis
            // for tuples with `xmax == INVALID`; tuples whose xmax
            // is set (including the `UPDATED_IN_PLACE` case that
            // returns `VisiblePreImage`) always pay the full
            // `is_visible` call.
            let outcome = if header.xmax.is_invalid() {
                let infomask_bits = header.infomask.bits();
                let cache_hit = self
                    .xmin_cache
                    .filter(|(cxmin, cinfo, _)| *cxmin == header.xmin && *cinfo == infomask_bits)
                    .map(|(_, _, v)| {
                        if v {
                            Visibility::Visible
                        } else {
                            Visibility::Invisible
                        }
                    });
                cache_hit.unwrap_or_else(|| {
                    let o = is_visible(&header, self.snapshot, self.oracle);
                    let v = matches!(o, Visibility::Visible);
                    self.xmin_cache = Some((header.xmin, infomask_bits, v));
                    o
                })
            } else {
                is_visible(&header, self.snapshot, self.oracle)
            };

            let tid = TupleId::new(page_id, slot);
            match outcome {
                Visibility::Visible => {
                    let payload = &self.page_scratch[offset + TUPLE_HEADER_SIZE..offset + length];
                    return Ok(Some((tid, header, payload)));
                }
                Visibility::VisiblePreImage => {
                    // Substitute the pre-image from the per-relation
                    // undo log. Reverse every writer this snapshot
                    // cannot see (full-payload: oldest invisible
                    // writer's pre-image; compact: subtract the sum of
                    // all invisible deltas). Missing entry → VACUUM
                    // trimmed it; treat as invisible (the safe
                    // direction).
                    self.pre_image_scratch.clear();
                    if let Some(payload) = lookup_undo_pre_image_owned(
                        &self.undo_log,
                        self.rel,
                        tid,
                        &self.page_scratch[offset + TUPLE_HEADER_SIZE..offset + length],
                        self.snapshot,
                        self.oracle,
                    ) {
                        self.pre_image_scratch.extend_from_slice(&payload);
                        return Ok(Some((tid, header, &self.pre_image_scratch[..])));
                    }
                    // Fall through to next slot.
                }
                Visibility::Invisible | Visibility::DeletedByOwn => {}
            }
        }
    }
}

/// Shared helper — same contract as
/// [`super::HeapAccess::lookup_undo_pre_image`] but accessible to the
/// free walker struct without borrowing `self`.
fn lookup_undo_pre_image_owned<O: XidStatusOracle + ?Sized>(
    undo_log: &DashMap<RelationId, parking_lot::RwLock<UndoRelationLog>>,
    rel: RelationId,
    tid: TupleId,
    current_payload: &[u8],
    snapshot: &Snapshot,
    oracle: &O,
) -> Option<Vec<u8>> {
    let log_handle = undo_log.get(&rel)?;
    let log = log_handle.read();
    undo_pre_image_from_log(&log, tid, current_payload, snapshot, oracle)
}

/// `PAGE_HEADER_SIZE + slot * ITEMID_SIZE` — mirrors
/// `crate::page::Page::item_id_offset` which is currently
/// `pub(crate)`-private and so unreachable from the walker's
/// inline slot-dir parse.
#[inline]
fn ultrasql_storage_page_item_id_offset(slot: u16) -> usize {
    crate::page::PAGE_HEADER_SIZE + usize::from(slot) * crate::page::ITEMID_SIZE
}
