//! See `crate::heap` for the public API.
//!
//! Part of the `heap` module split — each `impl<L: PageLoader>
//! HeapAccess<L>` block here adds methods to the type defined in
//! `heap/mod.rs`. Splitting across files keeps each unit under the
//! 600-line ceiling without changing semantics.

#![allow(unused_imports)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use ahash::AHashMap;
use ultrasql_core::{BlockNumber, CommandId, Lsn, PageId, RelationId, TupleId, Xid};
use ultrasql_mvcc::tuple_header::{InfoMask, TUPLE_HEADER_SIZE};
use ultrasql_mvcc::{Snapshot, TupleHeader, Visibility, XidStatusOracle, is_visible};
use ultrasql_wal::WalRecord;
use ultrasql_wal::payload::{
    FullPageWritePayload, HeapDeletePayload, HeapInsertPayload, HeapUpdatePayload,
};
use ultrasql_wal::record::RecordType;

use crate::buffer_pool::{BufferPool, PageGuard, PageLoader};
use crate::page::PageError;
use crate::wal_sink::WalSink;

use super::{
    DeleteOptions, HeapAccess, HeapError, HeapTuple, InsertOptions, UndoEntry,
    UndoRelationLog, UpdateOptions, UpdateOutcome, UpdatePayload,
};


/// Visibility-filtered sequential scan that yields borrowed slot
/// payload slices.
///
/// Constructed via [`HeapAccess::scan_visible_walker`]. The walker
/// owns one [`PageGuard`] at a time (released at block boundaries)
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
    /// Advance to the next MVCC-visible tuple and return a borrowed
    /// view of its `(TupleId, TupleHeader, payload_bytes)`.
    ///
    /// Returns `Ok(None)` when the relation is exhausted, `Err(_)` on
    /// I/O / decode failure. The `payload_bytes` slice borrows from
    /// the walker's internal scratch buffer; the borrow is
    /// invalidated by the next call.
    #[allow(clippy::type_complexity)]
    pub fn try_next(&mut self) -> Result<Option<(TupleId, TupleHeader, &[u8])>, HeapError> {
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
                let guard = match self.pool.get_page(page_id) {
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
                self.current_slot = 0;
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
            let flags = raw & 0b11;
            // ItemIdFlags::Normal == 1; skip Unused / Dead / Redirect.
            if flags != 1 {
                continue;
            }
            let length = ((raw >> 2) & 0x7FFF) as usize;
            let offset = ((raw >> 17) & 0x7FFF) as usize;
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

            let visible = if header.xmax.is_invalid() {
                let infomask_bits = header.infomask.bits();
                if let Some((cxmin, cinfo, cv)) = self.xmin_cache {
                    if cxmin == header.xmin && cinfo == infomask_bits {
                        cv
                    } else {
                        let v = matches!(
                            is_visible(&header, self.snapshot, self.oracle),
                            Visibility::Visible,
                        );
                        self.xmin_cache = Some((header.xmin, infomask_bits, v));
                        v
                    }
                } else {
                    let v = matches!(
                        is_visible(&header, self.snapshot, self.oracle),
                        Visibility::Visible,
                    );
                    self.xmin_cache = Some((header.xmin, infomask_bits, v));
                    v
                }
            } else {
                matches!(
                    is_visible(&header, self.snapshot, self.oracle),
                    Visibility::Visible,
                )
            };

            if !visible {
                continue;
            }

            let tid = TupleId::new(page_id, slot);
            // Payload is the bytes after the tuple header within the
            // slot window we already validated above.
            let payload = &self.page_scratch[offset + TUPLE_HEADER_SIZE..offset + length];
            return Ok(Some((tid, header, payload)));
        }
    }
}

/// `PAGE_HEADER_SIZE + slot * ITEMID_SIZE` — mirrors
/// `crate::page::Page::item_id_offset` which is currently
/// `pub(crate)`-private and so unreachable from the walker's
/// inline slot-dir parse.
#[inline]
const fn ultrasql_storage_page_item_id_offset(slot: u16) -> usize {
    crate::page::PAGE_HEADER_SIZE + (slot as usize) * crate::page::ITEMID_SIZE
}

