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

use super::{
    DeleteOptions, HeapAccess, HeapError, HeapTuple, InsertOptions, UpdateOptions,
    checked_tuple_space_needed,
};

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
        new_tuple_size: usize,
    ) -> Result<Option<TupleId>, HeapError> {
        let new_tid = {
            let mut page = guard.write();
            let n_atts;

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
                n_atts = hdr.n_atts;
            }

            // Check whether there is room for the new version on this page.
            let free = page.header().free_space();
            if free < checked_tuple_space_needed(new_tuple_size)? {
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
            Self::tuple_header_bytes_mut(page_bytes, new_off, "new header outside page")?
                .copy_from_slice(&new_hdr_bytes);

            // Patch the old tuple's header in place: set xmax/cmax,
            // HOT_UPDATED flag, and redirect ctid to the new version.
            let (old_off, old_len) = Self::slot_window(page_bytes, old_tid.slot)?;
            if old_len < TUPLE_HEADER_SIZE {
                return Err(HeapError::MalformedHeader("old slot shorter than header"));
            }
            let old_header =
                Self::tuple_header_bytes(page_bytes, old_off, "old header outside page")?;
            let (mut old_hdr, _) = TupleHeader::decode(old_header)
                .ok_or(HeapError::MalformedHeader("old header decode failed"))?;
            old_hdr.xmax = opts.xid;
            old_hdr.cmax = opts.command_id;
            old_hdr.infomask.set(InfoMask::HOT_UPDATED);
            old_hdr.ctid = new_tid;
            let old_hdr_bytes = Self::collect_header_bytes(&old_hdr);
            Self::tuple_header_bytes_mut(page_bytes, old_off, "old header outside page")?
                .copy_from_slice(&old_hdr_bytes);

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
    #[inline]
    pub(super) fn try_hot_update_inplace(
        page: &mut PageWrite<'_>,
        old_tid: TupleId,
        new_payload: &[u8],
        opts: UpdateOptions<'_>,
        new_tuple_size: usize,
        scratch: &mut Vec<u8>,
    ) -> Result<Option<TupleId>, HeapError> {
        // Verify old tuple alive by reading xmax (bytes 8..16) directly.
        let (old_off, old_len) = Self::slot_window(page.as_bytes_mut(), old_tid.slot)?;
        if old_len < TUPLE_HEADER_SIZE {
            return Err(HeapError::MalformedHeader("old slot shorter than header"));
        }
        let n_atts;
        {
            let bytes = page.as_bytes_mut();
            let existing_xmax = Self::read_xmax(bytes, old_off)?;
            if existing_xmax != 0 {
                return Err(HeapError::MalformedHeader("update on deleted tuple"));
            }
            n_atts = Self::read_n_atts(bytes, old_off)?;
        }

        // Free-space precheck — avoid building the new tuple and
        // entering `insert_tuple_appended` only to bounce back.
        let free = page.header().free_space();
        if free < checked_tuple_space_needed(new_tuple_size)? {
            return Ok(None);
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
        Self::write_ctid(page_bytes, new_off, new_tid)?;

        // Old tuple stamps: xmax | cmax | infomask |= HOT_UPDATED | ctid.
        Self::write_xmax(page_bytes, old_off, opts.xid)?;
        Self::write_cmax(page_bytes, old_off, opts.command_id)?;
        let cur_infomask = Self::read_infomask(page_bytes, old_off)?;
        let new_infomask = cur_infomask | InfoMask::HOT_UPDATED;
        Self::write_infomask(page_bytes, old_off, new_infomask)?;
        Self::write_ctid(page_bytes, old_off, new_tid)?;

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
        let existing_xmax = Self::read_xmax(page_bytes, slot_offset)?;
        if existing_xmax != 0 {
            return Err(HeapError::MalformedHeader("update on deleted tuple"));
        }

        // Stamp xmax (bytes 8..16).
        Self::write_xmax(page_bytes, slot_offset, xid)?;

        // Stamp cmax (bytes 20..24).
        Self::write_cmax(page_bytes, slot_offset, command_id)?;

        // OR `UPDATED` into infomask (bytes 24..26).
        let cur_infomask = Self::read_infomask(page_bytes, slot_offset)?;
        let new_infomask = cur_infomask | InfoMask::UPDATED;
        Self::write_infomask(page_bytes, slot_offset, new_infomask)?;

        // Stamp ctid relation (bytes 32..36).
        Self::write_ctid(page_bytes, slot_offset, new_tid)?;

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
        Self::tuple_header_bytes_mut(page_bytes, slot_offset, "slot header outside page")?
            .copy_from_slice(&hdr_bytes);
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
    /// `ItemId` bytes directly out of the page buffer. Bounds-sensitive
    /// byte ranges go through the page module's checked slot-directory
    /// offset helper.
    #[inline]
    pub(super) fn slot_window(
        page_bytes: &[u8; ultrasql_core::constants::PAGE_SIZE],
        slot: u16,
    ) -> Result<(usize, usize), HeapError> {
        use crate::page::{ITEMID_SIZE, ItemId, Page};

        let off = Page::try_item_id_offset(slot).map_err(|_| {
            HeapError::Page(PageError::InvalidSlot {
                index: slot,
                len: 0,
            })
        })?;
        let end = off
            .checked_add(ITEMID_SIZE)
            .ok_or(HeapError::MalformedHeader("itemid range overflow"))?;
        let item_bytes =
            page_bytes
                .get(off..end)
                .ok_or(HeapError::Page(PageError::InvalidSlot {
                    index: slot,
                    len: 0,
                }))?;
        let raw = u32::from_le_bytes(
            item_bytes
                .try_into()
                .map_err(|_| HeapError::MalformedHeader("itemid slice"))?,
        );
        let id = ItemId::from_raw(raw);
        let offset = usize::try_from(id.offset())
            .map_err(|_| HeapError::MalformedHeader("itemid offset overflow"))?;
        let length = usize::try_from(id.length())
            .map_err(|_| HeapError::MalformedHeader("itemid length overflow"))?;
        let end = offset
            .checked_add(length)
            .ok_or(HeapError::MalformedHeader("itemid tuple range overflow"))?;
        if page_bytes.get(offset..end).is_none() {
            return Err(HeapError::MalformedHeader(
                "itemid tuple range outside page",
            ));
        }
        Ok((offset, length))
    }
}
