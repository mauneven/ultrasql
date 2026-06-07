//! See `crate::heap` for the public API.
//!
//! Part of the `heap` module split — each `impl<L: PageLoader>
//! HeapAccess<L>` block here adds methods to the type defined in
//! `heap/mod.rs`. Splitting across files keeps each unit under the
//! 600-line ceiling without changing semantics.

use std::sync::Arc;
use std::sync::atomic::AtomicU32;

use ultrasql_core::{BlockNumber, CommandId, PageId, RelationId, TupleId, Xid};
use ultrasql_mvcc::TupleHeader;
use ultrasql_mvcc::tuple_header::{InfoMask, TUPLE_HEADER_SIZE};

use crate::buffer_pool::{BufferPool, PageGuard, PageLoader, PageWrite};
use crate::page::PageError;

use super::{DeleteOptions, HeapAccess, HeapError, HeapTuple, InsertOptions, UpdateOptions};

impl<L: PageLoader> HeapAccess<L> {
    /// Mark a page as all-visible in the visibility map.
    ///
    /// Called by vacuum after verifying that every live tuple on `block`
    /// is visible to the oldest active snapshot. Callers must ensure that
    /// no concurrent mutation is in progress on the page; stamping a page
    /// all-visible while a writer holds a pin on it is a visibility error.
    ///
    /// This is a thin wrapper over `VisibilityMap::mark_all_visible`
    /// provided here so the executor does not need to import the VM type
    /// directly when it calls into the heap.
    pub fn vacuum_set_all_visible(
        &self,
        rel: RelationId,
        block: BlockNumber,
        vm: &crate::vm::VisibilityMap,
    ) {
        vm.mark_all_visible(rel, block);
    }

    /// Get or create the block counter for `rel`. `pub(crate)` so the WAL
    /// applier can call `advance_counter` without re-introducing a public API.
    pub(crate) fn counter_for(&self, rel: RelationId) -> Arc<AtomicU32> {
        if let Some(existing) = self.block_counters.get(&rel) {
            return Arc::clone(&existing);
        }
        let counter = Arc::new(AtomicU32::new(0));
        match self.block_counters.entry(rel) {
            dashmap::mapref::entry::Entry::Occupied(o) => Arc::clone(o.get()),
            dashmap::mapref::entry::Entry::Vacant(v) => {
                v.insert(Arc::clone(&counter));
                counter
            }
        }
    }

    /// Get or create the insertion cursor for `rel`. The cursor stores
    /// the block number the heap last successfully inserted into; it is
    /// used as the starting point for the linear-scan fallback in
    /// [`Self::insert`].
    ///
    /// The cursor is purely advisory — a stale value (e.g. the page is
    /// now full) causes one wasted attempt and falls through to the
    /// existing linear scan from the cursor forward.
    pub(super) fn cursor_for(&self, rel: RelationId) -> Arc<AtomicU32> {
        if let Some(existing) = self.insert_cursor.get(&rel) {
            return Arc::clone(&existing);
        }
        let cursor = Arc::new(AtomicU32::new(0));
        match self.insert_cursor.entry(rel) {
            dashmap::mapref::entry::Entry::Occupied(o) => Arc::clone(o.get()),
            dashmap::mapref::entry::Entry::Vacant(v) => {
                v.insert(Arc::clone(&cursor));
                cursor
            }
        }
    }

    /// Attempt a HOT (same-page) update.
    ///
    /// Returns `Ok(Some(new_tid))` if the new version fit on the same
    /// page as `old_tid`, `Ok(None)` if the page lacks room (the
    /// caller should fall back to the non-HOT path).
    ///
    /// When this function succeeds it has already patched the old
    /// tuple's header in place.
    ///
    pub(super) fn try_hot_update(
        guard: &PageGuard<L>,
        old_tid: TupleId,
        new_payload: &[u8],
        opts: UpdateOptions<'_>,
        n_atts: u16,
        new_tuple_size: usize,
    ) -> Result<Option<TupleId>, HeapError> {
        let new_tid = {
            let mut page = guard.write();

            // Verify the old tuple is alive before touching anything.
            {
                let bytes = page.read_tuple(old_tid.slot)?;
                if bytes.len() < TUPLE_HEADER_SIZE {
                    return Err(HeapError::MalformedHeader("slot shorter than header"));
                }
                let (hdr, _) = TupleHeader::decode(&bytes[..TUPLE_HEADER_SIZE])
                    .ok_or(HeapError::MalformedHeader("header decode failed"))?;
                if !hdr.is_alive() {
                    return Err(HeapError::MalformedHeader("update on deleted tuple"));
                }
            }

            // Check whether there is room for the new version on this page.
            let free = page.header().free_space();
            if free < new_tuple_size + crate::page::ITEMID_SIZE {
                // Not enough room — signal fallback to caller.
                return Ok(None);
            }

            // Build the new tuple bytes (header + payload). We don't know
            // the final slot yet so we use a tentative tid; it will be
            // patched after insert_tuple returns.
            let tentative_tid = TupleId::new(old_tid.page, 0);
            let mut new_hdr = TupleHeader::fresh(opts.xid, opts.command_id, tentative_tid, n_atts);
            // New version is marked HOT_UPDATED | UPDATED so index scans
            // know the chain does not cross page boundaries.
            new_hdr
                .infomask
                .set(InfoMask::HOT_UPDATED | InfoMask::UPDATED);

            let mut new_tuple_bytes = Vec::with_capacity(new_tuple_size);
            new_tuple_bytes.resize(TUPLE_HEADER_SIZE, 0);
            new_hdr.encode(&mut new_tuple_bytes[..TUPLE_HEADER_SIZE]);
            new_tuple_bytes.extend_from_slice(new_payload);

            // HOT updates only ever create new tuple versions — never
            // reuse a previously-deleted slot. Skip
            // `Page::insert_tuple`'s O(slot_count) find-reusable-slot
            // linear scan via the appended variant. For a 200-slot page
            // with 200 HOT inserts this drops the scan cost from
            // O(slot_count²) total to O(slot_count).
            let new_slot = page.insert_tuple_appended(&new_tuple_bytes)?;
            let new_tid = TupleId::new(old_tid.page, new_slot);

            // Patch the new tuple's ctid to point at itself (terminal
            // version in the chain).
            let mut patched_new_hdr = new_hdr;
            patched_new_hdr.ctid = new_tid;
            let new_hdr_bytes = Self::collect_header_bytes(&patched_new_hdr);
            let page_bytes = page.as_bytes_mut();
            let (new_off, _) = Self::slot_window(page_bytes, new_slot)?;
            page_bytes[new_off..new_off + TUPLE_HEADER_SIZE].copy_from_slice(&new_hdr_bytes);

            // Patch the old tuple's header in place: set xmax/cmax,
            // HOT_UPDATED flag, and redirect ctid to the new version.
            let (old_off, old_len) = Self::slot_window(page_bytes, old_tid.slot)?;
            if old_len < TUPLE_HEADER_SIZE {
                return Err(HeapError::MalformedHeader("old slot shorter than header"));
            }
            let (mut old_hdr, _) =
                TupleHeader::decode(&page_bytes[old_off..old_off + TUPLE_HEADER_SIZE])
                    .ok_or(HeapError::MalformedHeader("old header decode failed"))?;
            old_hdr.xmax = opts.xid;
            old_hdr.cmax = opts.command_id;
            old_hdr.infomask.set(InfoMask::HOT_UPDATED);
            old_hdr.ctid = new_tid;
            let old_hdr_bytes = Self::collect_header_bytes(&old_hdr);
            page_bytes[old_off..old_off + TUPLE_HEADER_SIZE].copy_from_slice(&old_hdr_bytes);

            new_tid
        };
        Ok(Some(new_tid))
    }

    /// HOT-update variant that operates on an already-held [`PageWrite`].
    ///
    /// Caller owns the page write guard for the duration of an entire
    /// page-run (the bulk-UPDATE `update_many` path acquires the write
    /// lock once per source page and drives every row on that page
    /// through this helper before releasing). This drops the
    /// per-row `PageGuard::write()` acquire/release pair the
    /// per-row `try_hot_update` used to pay (~50 ns × N rows on
    /// `parking_lot::RwLock`).
    ///
    /// The new tuple bytes (40-byte header + caller payload) are
    /// assembled into `scratch` which the caller reuses across rows —
    /// one heap allocation per page-run instead of one per row.
    ///
    /// Old + new headers are stamped via direct byte writes at known
    /// field offsets (mirroring [`Self::stamp_updated_old_inline`]);
    /// no `TupleHeader::decode` / `encode` round trip on the per-row
    /// path.
    #[allow(clippy::too_many_arguments)]
    #[inline]
    pub(super) fn try_hot_update_inplace(
        page: &mut PageWrite<'_>,
        old_tid: TupleId,
        new_payload: &[u8],
        opts: UpdateOptions<'_>,
        n_atts: u16,
        new_tuple_size: usize,
        scratch: &mut Vec<u8>,
    ) -> Result<Option<TupleId>, HeapError> {
        // Free-space precheck — avoid building the new tuple and
        // entering `insert_tuple_appended` only to bounce back.
        let free = page.header().free_space();
        if free < new_tuple_size + crate::page::ITEMID_SIZE {
            return Ok(None);
        }

        // Verify old tuple alive by reading xmax (bytes 8..16) directly.
        let (old_off, old_len) = Self::slot_window(page.as_bytes_mut(), old_tid.slot)?;
        if old_len < TUPLE_HEADER_SIZE {
            return Err(HeapError::MalformedHeader("old slot shorter than header"));
        }
        {
            let bytes = page.as_bytes_mut();
            let xmax_at = old_off + 8;
            let existing_xmax = read_u64_le(bytes, xmax_at)?;
            if existing_xmax != 0 {
                return Err(HeapError::MalformedHeader("update on deleted tuple"));
            }
        }

        // Build the new tuple bytes into the caller-provided scratch.
        // `Vec::clear` keeps the existing allocation so subsequent rows
        // in the page-run pay no allocator traffic.
        let tentative_tid = TupleId::new(old_tid.page, 0);
        let mut new_hdr = TupleHeader::fresh(opts.xid, opts.command_id, tentative_tid, n_atts);
        new_hdr
            .infomask
            .set(InfoMask::HOT_UPDATED | InfoMask::UPDATED);
        scratch.clear();
        scratch.resize(TUPLE_HEADER_SIZE, 0);
        new_hdr.encode(&mut scratch[..TUPLE_HEADER_SIZE]);
        scratch.extend_from_slice(new_payload);

        // Insert the new tuple at the appended slot.
        let new_slot = page.insert_tuple_appended(scratch)?;
        let new_tid = TupleId::new(old_tid.page, new_slot);

        // Patch the new tuple's ctid (self-reference) and the old
        // tuple's header in place. Both writes hit known byte offsets
        // — no `TupleHeader::decode` + `encode` round trip.
        let page_bytes = page.as_bytes_mut();
        let (new_off, _) = Self::slot_window(page_bytes, new_slot)?;

        // New tuple ctid (bytes 32..40 relative to slot).
        let ctid_rel_at = new_off + 32;
        page_bytes[ctid_rel_at..ctid_rel_at + 4]
            .copy_from_slice(&new_tid.page.relation.0.raw().to_le_bytes());
        let block_slot_packed =
            (new_tid.page.block.raw() & 0x00FF_FFFF) | ((u32::from(new_tid.slot)) << 24);
        page_bytes[ctid_rel_at + 4..ctid_rel_at + 8]
            .copy_from_slice(&block_slot_packed.to_le_bytes());

        // Old tuple stamps: xmax | cmax | infomask |= HOT_UPDATED | ctid.
        let xmax_at = old_off + 8;
        page_bytes[xmax_at..xmax_at + 8].copy_from_slice(&opts.xid.raw().to_le_bytes());
        let cmax_at = old_off + 20;
        page_bytes[cmax_at..cmax_at + 4].copy_from_slice(&opts.command_id.raw().to_le_bytes());
        let infomask_at = old_off + 24;
        let cur_infomask =
            u16::from_le_bytes([page_bytes[infomask_at], page_bytes[infomask_at + 1]]);
        let new_infomask = cur_infomask | InfoMask::HOT_UPDATED;
        page_bytes[infomask_at..infomask_at + 2].copy_from_slice(&new_infomask.to_le_bytes());
        let old_ctid_at = old_off + 32;
        page_bytes[old_ctid_at..old_ctid_at + 4]
            .copy_from_slice(&new_tid.page.relation.0.raw().to_le_bytes());
        page_bytes[old_ctid_at + 4..old_ctid_at + 8]
            .copy_from_slice(&block_slot_packed.to_le_bytes());

        Ok(Some(new_tid))
    }

    /// Stamp the old tuple's header for the non-HOT update case.
    ///
    /// Sets `xmax`, `cmax`, `infomask |= UPDATED`, and `ctid = new_tid`
    /// on the old tuple identified by `old_tid`.
    ///
    /// Tight inline variant of [`Self::stamp_updated_old`] that
    /// writes only the four header fields a non-HOT UPDATE actually
    /// changes (`xmax` / `cmax` / `infomask | UPDATED` / `ctid`) at
    /// known fixed offsets within the slot, without paying a full
    /// `TupleHeader::decode` + re-`encode` per row.
    ///
    /// Caller is responsible for holding a `PageWrite` over the
    /// page; this helper performs no locking. Used by
    /// `update_many`'s bulk-non-HOT fallback to stamp every entry
    /// on a source page under a single page guard.
    ///
    /// Layout (mirrors `TupleHeader::encode`):
    ///   bytes  0..8   xmin   (read-only here)
    ///   bytes  8..16  xmax       ← stamped
    ///   bytes 16..20  cmin   (read-only)
    ///   bytes 20..24  cmax       ← stamped
    ///   bytes 24..26  infomask   ← OR-ed with `UPDATED`
    ///   bytes 26..28  n_atts (read-only)
    ///   bytes 28..30  data_offset (read-only)
    ///   bytes 30..32  reserved
    ///   bytes 32..36  ctid relation   ← stamped
    ///   bytes 36..40  ctid block+slot ← stamped
    #[inline]
    pub(super) fn stamp_updated_old_inline(
        page_bytes: &mut [u8; ultrasql_core::constants::PAGE_SIZE],
        old_slot: u16,
        new_tid: TupleId,
        xid: Xid,
        command_id: CommandId,
    ) -> Result<(), HeapError> {
        let (slot_offset, slot_length) = Self::slot_window(page_bytes, old_slot)?;
        if slot_length < TUPLE_HEADER_SIZE {
            return Err(HeapError::MalformedHeader("slot shorter than header"));
        }

        // Check tuple is alive — `is_alive == xmax.is_invalid()`,
        // and `Xid::INVALID == 0`, so read the 8-byte xmax field
        // and compare to zero. Eight bytes — one cache-line touch.
        let xmax_at = slot_offset + 8;
        let existing_xmax = read_u64_le(page_bytes, xmax_at)?;
        if existing_xmax != 0 {
            return Err(HeapError::MalformedHeader("update on deleted tuple"));
        }

        // Stamp xmax (bytes 8..16).
        page_bytes[xmax_at..xmax_at + 8].copy_from_slice(&xid.raw().to_le_bytes());

        // Stamp cmax (bytes 20..24).
        let cmax_at = slot_offset + 20;
        page_bytes[cmax_at..cmax_at + 4].copy_from_slice(&command_id.raw().to_le_bytes());

        // OR `UPDATED` into infomask (bytes 24..26).
        let infomask_at = slot_offset + 24;
        let cur_infomask =
            u16::from_le_bytes([page_bytes[infomask_at], page_bytes[infomask_at + 1]]);
        let new_infomask = cur_infomask | InfoMask::UPDATED;
        page_bytes[infomask_at..infomask_at + 2].copy_from_slice(&new_infomask.to_le_bytes());

        // Stamp ctid relation (bytes 32..36).
        let ctid_rel_at = slot_offset + 32;
        page_bytes[ctid_rel_at..ctid_rel_at + 4]
            .copy_from_slice(&new_tid.page.relation.0.raw().to_le_bytes());

        // Stamp ctid block+slot packing (bytes 36..40).
        let block_slot_packed =
            (new_tid.page.block.raw() & 0x00FF_FFFF) | ((u32::from(new_tid.slot)) << 24);
        page_bytes[ctid_rel_at + 4..ctid_rel_at + 8]
            .copy_from_slice(&block_slot_packed.to_le_bytes());

        Ok(())
    }

    pub(super) fn stamp_updated_old(
        guard: &PageGuard<L>,
        old_tid: TupleId,
        new_tid: TupleId,
        opts: UpdateOptions<'_>,
    ) -> Result<(), HeapError> {
        let mut page = guard.write();
        let bytes = page.read_tuple(old_tid.slot)?;
        if bytes.len() < TUPLE_HEADER_SIZE {
            return Err(HeapError::MalformedHeader("slot shorter than header"));
        }
        let (mut hdr, _) = TupleHeader::decode(&bytes[..TUPLE_HEADER_SIZE])
            .ok_or(HeapError::MalformedHeader("header decode failed"))?;
        if !hdr.is_alive() {
            return Err(HeapError::MalformedHeader("update on deleted tuple"));
        }
        hdr.xmax = opts.xid;
        hdr.cmax = opts.command_id;
        hdr.infomask.set(InfoMask::UPDATED);
        hdr.ctid = new_tid;
        let hdr_bytes = Self::collect_header_bytes(&hdr);

        let page_bytes = page.as_bytes_mut();
        let (slot_offset, slot_length) = Self::slot_window(page_bytes, old_tid.slot)?;
        if slot_length < TUPLE_HEADER_SIZE {
            return Err(HeapError::MalformedHeader("slot shorter than header"));
        }
        page_bytes[slot_offset..slot_offset + TUPLE_HEADER_SIZE].copy_from_slice(&hdr_bytes);
        Ok(())
    }

    /// Read a slot under shared lock into an owned byte buffer.
    /// Releases the per-frame read lock before returning.
    pub(super) fn copy_slot_bytes(guard: &PageGuard<L>, slot: u16) -> Result<Vec<u8>, HeapError> {
        let page = guard.read();
        Ok(page.read_tuple(slot)?.to_vec())
    }

    pub(super) fn decode_tuple(tid: TupleId, bytes: &[u8]) -> Result<HeapTuple, HeapError> {
        if bytes.len() < TUPLE_HEADER_SIZE {
            return Err(HeapError::MalformedHeader("slot shorter than header"));
        }
        let (header, _) = TupleHeader::decode(&bytes[..TUPLE_HEADER_SIZE])
            .ok_or(HeapError::MalformedHeader("header decode failed"))?;
        let data = bytes[TUPLE_HEADER_SIZE..].to_vec();
        Ok(HeapTuple { tid, header, data })
    }

    pub(super) fn collect_header_bytes(header: &TupleHeader) -> [u8; TUPLE_HEADER_SIZE] {
        let mut buf = [0_u8; TUPLE_HEADER_SIZE];
        header.encode(&mut buf);
        buf
    }

    /// Read the free space of `page_id` from the buffer pool and return it as
    /// a `u32`, clamping to `u32::MAX` if the `usize` does not fit.
    pub(super) fn page_free_space(pool: &Arc<BufferPool<L>>, page_id: PageId) -> u32 {
        // A pool miss here is non-fatal for FSM accuracy; we simply return 0
        // to cause the FSM to record the block as full, which is conservative
        // and safe.
        pool.get_page(page_id).ok().map_or(0, |guard| {
            let free = guard.read().header().free_space();
            u32::try_from(free).unwrap_or(u32::MAX)
        })
    }

    /// Update FSM and clear VM bits after a successful insert.
    ///
    /// Called after the WAL record has been appended (if any) and the page
    /// guard has been dropped.  Both hooks are best-effort: a failure to pin
    /// the page for the FSM read is treated as "no free space known" (the FSM
    /// records 0, which is conservative).
    pub(super) fn post_insert_fsm_vm(
        pool: &Arc<BufferPool<L>>,
        page_id: PageId,
        opts: InsertOptions<'_>,
    ) {
        if let Some(fsm) = opts.fsm {
            let free = Self::page_free_space(pool, page_id);
            fsm.record_free_space(page_id.relation, page_id.block, free);
        }
        if let Some(vm) = opts.vm {
            vm.clear(page_id.relation, page_id.block);
        }
    }

    /// Update FSM and clear VM bits after a successful delete.
    ///
    /// The FSM update is optimistic: we record the dead tuple's space as free
    /// immediately so future inserters see the block as a candidate. Vacuum
    /// will eventually reclaim the space; until then the insert will discover
    /// (via `NoSpace`) that the category was too optimistic and fall back.
    pub(super) fn post_delete_fsm_vm(
        pool: &Arc<BufferPool<L>>,
        page_id: PageId,
        opts: DeleteOptions<'_>,
    ) {
        if let Some(fsm) = opts.fsm {
            let free = Self::page_free_space(pool, page_id);
            fsm.record_free_space(page_id.relation, page_id.block, free);
        }
        if let Some(vm) = opts.vm {
            vm.clear(page_id.relation, page_id.block);
        }
    }

    /// Extract `(offset, length)` of slot `slot` by reading its
    /// `ItemId` bytes directly out of the page buffer. The page-module
    /// helpers `read_item_id` / `item_id_offset` are private, so we
    /// inline the same arithmetic here.
    ///
    /// If the page-module helpers become `pub(crate)` we should switch
    /// to those.
    #[inline]
    pub(super) fn slot_window(
        page_bytes: &[u8; ultrasql_core::constants::PAGE_SIZE],
        slot: u16,
    ) -> Result<(usize, usize), HeapError> {
        use crate::page::{ITEMID_SIZE, ItemId, PAGE_HEADER_SIZE};

        let off = PAGE_HEADER_SIZE + usize::from(slot) * ITEMID_SIZE;
        if off + ITEMID_SIZE > page_bytes.len() {
            return Err(HeapError::Page(PageError::InvalidSlot {
                index: slot,
                len: 0,
            }));
        }
        let raw = u32::from_le_bytes(
            page_bytes[off..off + ITEMID_SIZE]
                .try_into()
                .map_err(|_| HeapError::MalformedHeader("itemid slice"))?,
        );
        let id = ItemId::from_raw(raw);
        let offset = usize::try_from(id.offset())
            .map_err(|_| HeapError::MalformedHeader("itemid offset overflow"))?;
        let length = usize::try_from(id.length())
            .map_err(|_| HeapError::MalformedHeader("itemid length overflow"))?;
        Ok((offset, length))
    }
}

#[inline]
fn read_u64_le(bytes: &[u8], offset: usize) -> Result<u64, HeapError> {
    let end = offset
        .checked_add(8)
        .ok_or(HeapError::MalformedHeader("u64 field offset overflow"))?;
    let window = bytes
        .get(offset..end)
        .ok_or(HeapError::MalformedHeader("u64 field outside tuple"))?;
    Ok(u64::from_le_bytes([
        window[0], window[1], window[2], window[3], window[4], window[5], window[6], window[7],
    ]))
}
