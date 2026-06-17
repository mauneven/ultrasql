//! WAL replay implementation for the heap access method.
//!
//! Implements [`ultrasql_wal::HeapTarget`] for [`HeapAccess<L>`] so the WAL
//! recovery path can drive page-level redo directly through the heap's buffer
//! pool, bypassing WAL emission (which would cause an infinite loop).
//!
//! # Contract
//!
//! Each `apply_*` method is idempotent with respect to the on-page state: if
//! the page already reflects the mutation (page LSN ≥ record LSN) the method
//! is a no-op. This property is required for crash recovery when some pages
//! were flushed to disk before the crash but others were not.
//!
//! # Block counter maintenance
//!
//! [`HeapAccess`] maintains an internal per-relation block counter for v0.5.
//! The applier advances the counter whenever it writes to a block whose number
//! equals or exceeds the current counter value. This ensures that a
//! post-recovery scan driven by `block_count()` covers all replayed blocks.
//!
//! # Visibility
//!
//! The applier does not enforce MVCC visibility — it replays every WAL record
//! regardless of transaction outcome. The caller is responsible for filtering
//! aborted transactions, either by using the commit/abort records to drive a
//! CLOG update and then running a visibility filter, or by replaying only
//! records from committed transactions.

use std::sync::atomic::Ordering;

use ultrasql_core::endian::{read_i64_le, read_u16_le, read_u32_le};
use ultrasql_core::{BlockNumber, Lsn, PageId, RelationId, TupleId, Xid};
use ultrasql_mvcc::TupleHeader;
use ultrasql_mvcc::tuple_header::{InfoMask, TUPLE_HEADER_SIZE};
use ultrasql_wal::applier::{ApplyError, HeapTarget};
use ultrasql_wal::payload::{
    BTreeOpKind, BTreeOpPayload, FullPageWritePayload, HeapDeleteInPlaceBatchPayload,
    HeapDeleteInPlacePayload, HeapDeleteInPlaceRangeBatchPayload, HeapDeletePayload,
    HeapInsertBatchPayload, HeapInsertPayload, HeapUpdateInPlaceBatchPayload,
    HeapUpdateInPlacePayload, HeapUpdateInt32PairDeltaBatchPayload,
    HeapUpdateInt32PairDeltaRangeBatchPayload, HeapUpdatePayload,
};

use crate::btree::{BTree, BTreeError};
use crate::buffer_pool::PageLoader;
use crate::heap::{HeapAccess, UndoEntry, UndoRelationLog};
use crate::page::{ItemId, PageError};

fn should_skip_redo(page: &crate::page::Page, record_lsn: Lsn) -> bool {
    let raw = record_lsn.raw();
    let page_lsn = page.header().lsn;
    page_lsn > raw || (raw != 0 && page_lsn == raw)
}

fn stamp_replayed_lsn(page: &mut crate::page::Page, record_lsn: Lsn) {
    let raw = record_lsn.raw();
    if raw != 0 && page.header().lsn < raw {
        page.set_lsn(raw);
    }
}

fn refused(operation: &'static str, detail: impl Into<String>) -> ApplyError {
    ApplyError::Refused {
        operation,
        detail: detail.into(),
    }
}

fn item_id_from_page_bytes(
    page_bytes: &[u8],
    slot: u16,
    operation: &'static str,
) -> Result<ItemId, ApplyError> {
    let item_id_off = crate::page::PAGE_HEADER_SIZE + usize::from(slot) * crate::page::ITEMID_SIZE;
    let item_id_end = item_id_off
        .checked_add(crate::page::ITEMID_SIZE)
        .ok_or_else(|| refused(operation, "itemid offset overflow"))?;
    let item_bytes = page_bytes
        .get(item_id_off..item_id_end)
        .ok_or_else(|| refused(operation, "itemid slice out of bounds"))?;
    let raw = u32::from_le_bytes(
        item_bytes
            .try_into()
            .map_err(|_| refused(operation, "itemid slice"))?,
    );
    Ok(ItemId::from_raw(raw))
}

fn item_offset_usize(item: ItemId, operation: &'static str) -> Result<usize, ApplyError> {
    usize::try_from(item.offset()).map_err(|_| refused(operation, "item offset overflow"))
}

fn item_length_usize(item: ItemId, operation: &'static str) -> Result<usize, ApplyError> {
    usize::try_from(item.length()).map_err(|_| refused(operation, "item length overflow"))
}

impl<L: PageLoader + 'static> HeapTarget for HeapAccess<L> {
    /// Apply a heap-insert record by writing the tuple bytes into the correct
    /// slot on the target page.
    ///
    /// The method calls `insert_tuple` on the page, which appends to the next
    /// available slot. During a clean forward replay the slot number assigned
    /// by the page will match `payload.tid.slot`. If the slot already has
    /// valid data (the page was already flushed past this LSN) the insertion
    /// is skipped.
    fn apply_insert(&self, payload: &HeapInsertPayload) -> Result<(), ApplyError> {
        self.apply_insert_at_lsn(payload, Lsn::ZERO)
    }

    fn apply_insert_at_lsn(
        &self,
        payload: &HeapInsertPayload,
        record_lsn: Lsn,
    ) -> Result<(), ApplyError> {
        let page_id = payload.tid.page;
        let rel = page_id.relation;

        // Ensure the block counter reflects this block.
        self.advance_counter(rel, page_id.block)?;

        {
            // Obtain an exclusive pin on the page.
            let guard = self
                .pool
                .get_page(page_id)
                .map_err(|e| ApplyError::Refused {
                    operation: "heap_insert",
                    detail: format!("buffer pool: {e}"),
                })?;

            let mut page = guard.write();
            if should_skip_redo(&page, record_lsn) {
                return Ok(());
            }

            // Idempotency check: if the page already has a slot at `tid.slot`
            // with data, skip the insert (the page was flushed past this record).
            let slot_count = page.header().slot_count();
            if payload.tid.slot < slot_count {
                if let Ok(existing) = page.read_tuple(payload.tid.slot) {
                    if !existing.is_empty() {
                        // Slot already exists and has content; skip.
                        stamp_replayed_lsn(&mut page, record_lsn);
                        return Ok(());
                    }
                }
            }

            // Write the tuple bytes into the page.
            page.insert_tuple(&payload.tuple_bytes)
                .map_err(|e| ApplyError::Refused {
                    operation: "heap_insert",
                    detail: format!("insert_tuple: {e}"),
                })?;
            stamp_replayed_lsn(&mut page, record_lsn);
        }
        Ok(())
    }

    fn apply_insert_batch(&self, payload: &HeapInsertBatchPayload) -> Result<(), ApplyError> {
        self.apply_insert_batch_at_lsn(payload, Lsn::ZERO)
    }

    fn apply_insert_batch_at_lsn(
        &self,
        payload: &HeapInsertBatchPayload,
        record_lsn: Lsn,
    ) -> Result<(), ApplyError> {
        let page_id = payload.page;
        self.advance_counter(page_id.relation, page_id.block)?;

        let guard = self
            .pool
            .get_page(page_id)
            .map_err(|e| ApplyError::Refused {
                operation: "heap_insert_batch",
                detail: format!("buffer pool: {e}"),
            })?;
        let mut page = guard.write();
        if should_skip_redo(&page, record_lsn) {
            return Ok(());
        }

        for entry in &payload.entries {
            let slot_count = page.header().slot_count();
            if entry.slot < slot_count
                && page
                    .read_tuple(entry.slot)
                    .is_ok_and(|existing| !existing.is_empty())
            {
                continue;
            }

            let actual_slot =
                page.insert_tuple(&entry.tuple_bytes)
                    .map_err(|e| ApplyError::Refused {
                        operation: "heap_insert_batch",
                        detail: format!("insert_tuple: {e}"),
                    })?;
            if actual_slot != entry.slot {
                return Err(ApplyError::Refused {
                    operation: "heap_insert_batch",
                    detail: format!(
                        "slot mismatch: expected {}, inserted {actual_slot}",
                        entry.slot
                    ),
                });
            }
        }
        stamp_replayed_lsn(&mut page, record_lsn);
        Ok(())
    }

    /// Apply a heap-update record by writing the new tuple bytes and stamping
    /// the old tuple's header with `xmax`/`cmax`.
    ///
    /// If the old and new tids are on the same page the update is performed
    /// under a single exclusive pin. If they are on different pages the new
    /// page is written first and then the old page is stamped.
    fn apply_update(&self, payload: &HeapUpdatePayload) -> Result<(), ApplyError> {
        self.apply_update_at_lsn(payload, Lsn::ZERO)
    }

    fn apply_update_at_lsn(
        &self,
        payload: &HeapUpdatePayload,
        record_lsn: Lsn,
    ) -> Result<(), ApplyError> {
        let new_page_id = payload.new_tid.page;
        let old_page_id = payload.old_tid.page;

        // Ensure block counters cover both pages.
        self.advance_counter(new_page_id.relation, new_page_id.block)?;
        if old_page_id != new_page_id {
            self.advance_counter(old_page_id.relation, old_page_id.block)?;
        }

        // Write the new tuple onto its page.
        {
            let guard = self
                .pool
                .get_page(new_page_id)
                .map_err(|e| ApplyError::Refused {
                    operation: "heap_update_new",
                    detail: format!("buffer pool: {e}"),
                })?;
            let mut page = guard.write();
            if !should_skip_redo(&page, record_lsn) {
                let slot_count = page.header().slot_count();
                if payload.new_tid.slot >= slot_count
                    || page
                        .read_tuple(payload.new_tid.slot)
                        .map_or(true, <[u8]>::is_empty)
                {
                    page.insert_tuple(&payload.new_tuple_bytes).map_err(|e| {
                        ApplyError::Refused {
                            operation: "heap_update_new",
                            detail: format!("insert_tuple: {e}"),
                        }
                    })?;
                }
                stamp_replayed_lsn(&mut page, record_lsn);
            }
        }

        // Stamp the old tuple's header: read it, set xmax/cmax, write back.
        {
            let guard = self
                .pool
                .get_page(old_page_id)
                .map_err(|e| ApplyError::Refused {
                    operation: "heap_update_old",
                    detail: format!("buffer pool: {e}"),
                })?;
            let mut page = guard.write();
            if should_skip_redo(&page, record_lsn) {
                return Ok(());
            }
            // Read the existing header bytes.
            let existing =
                page.read_tuple(payload.old_tid.slot)
                    .map_err(|e| ApplyError::Refused {
                        operation: "heap_update_old",
                        detail: format!("read slot: {e}"),
                    })?;
            if existing.len() < TUPLE_HEADER_SIZE {
                return Err(ApplyError::Refused {
                    operation: "heap_update_old",
                    detail: String::from("slot shorter than tuple header"),
                });
            }
            let (mut hdr, _) =
                TupleHeader::decode(&existing[..TUPLE_HEADER_SIZE]).ok_or_else(|| {
                    ApplyError::Refused {
                        operation: "heap_update_old",
                        detail: String::from("header decode failed"),
                    }
                })?;

            // Decode xmax from the new tuple header (the new version's xmin
            // is the updating transaction's xid, which becomes the old
            // version's xmax).
            let (new_hdr, _) = TupleHeader::decode(&payload.new_tuple_bytes[..TUPLE_HEADER_SIZE])
                .ok_or_else(|| ApplyError::Refused {
                operation: "heap_update_old",
                detail: String::from("new header decode failed"),
            })?;
            hdr.xmax = new_hdr.xmin;
            hdr.cmax = new_hdr.cmin;
            hdr.ctid = payload.new_tid;

            // Write the patched header back into the page's raw bytes.
            let mut hdr_bytes = [0_u8; TUPLE_HEADER_SIZE];
            hdr.encode(&mut hdr_bytes);
            let page_bytes = page.as_bytes_mut();
            // Re-read item-id to get the slot offset.
            let item =
                item_id_from_page_bytes(page_bytes, payload.old_tid.slot, "heap_update_old")?;
            let slot_off = item_offset_usize(item, "heap_update_old")?;
            page_bytes[slot_off..slot_off + TUPLE_HEADER_SIZE].copy_from_slice(&hdr_bytes);
            stamp_replayed_lsn(&mut page, record_lsn);
        }

        Ok(())
    }

    /// Apply a heap-delete record by stamping `xmax`/`cmax` into the tuple
    /// header at `payload.tid`.
    fn apply_delete(&self, payload: &HeapDeletePayload) -> Result<(), ApplyError> {
        self.apply_delete_at_lsn(payload, Lsn::ZERO)
    }

    fn apply_delete_at_lsn(
        &self,
        payload: &HeapDeletePayload,
        record_lsn: Lsn,
    ) -> Result<(), ApplyError> {
        let page_id = payload.tid.page;
        self.advance_counter(page_id.relation, page_id.block)?;

        {
            let guard = self
                .pool
                .get_page(page_id)
                .map_err(|e| ApplyError::Refused {
                    operation: "heap_delete",
                    detail: format!("buffer pool: {e}"),
                })?;

            let mut page = guard.write();
            if should_skip_redo(&page, record_lsn) {
                return Ok(());
            }
            let existing = page.read_tuple(payload.tid.slot).map_err(|e| match e {
                // If the slot doesn't exist, the record is already beyond
                // what's on this page — treat as idempotent no-op.
                PageError::InvalidSlot { .. } | PageError::DeadSlot(_) => ApplyError::Refused {
                    operation: "heap_delete",
                    detail: format!("read slot: {e}"),
                },
                other => ApplyError::Refused {
                    operation: "heap_delete",
                    detail: format!("read slot: {other}"),
                },
            })?;
            if existing.len() < TUPLE_HEADER_SIZE {
                return Err(ApplyError::Refused {
                    operation: "heap_delete",
                    detail: String::from("slot shorter than tuple header"),
                });
            }
            let (mut hdr, _) =
                TupleHeader::decode(&existing[..TUPLE_HEADER_SIZE]).ok_or_else(|| {
                    ApplyError::Refused {
                        operation: "heap_delete",
                        detail: String::from("header decode failed"),
                    }
                })?;

            // Idempotency: if xmax is already set to the same xid, skip.
            if hdr.xmax == payload.xmax {
                stamp_replayed_lsn(&mut page, record_lsn);
                return Ok(());
            }
            hdr.mark_deleted(payload.xmax, payload.cmax);
            let mut hdr_bytes = [0_u8; TUPLE_HEADER_SIZE];
            hdr.encode(&mut hdr_bytes);

            let page_bytes = page.as_bytes_mut();
            let item = item_id_from_page_bytes(page_bytes, payload.tid.slot, "heap_delete")?;
            let slot_off = item_offset_usize(item, "heap_delete")?;
            page_bytes[slot_off..slot_off + TUPLE_HEADER_SIZE].copy_from_slice(&hdr_bytes);
            stamp_replayed_lsn(&mut page, record_lsn);
        }

        Ok(())
    }

    /// Apply an in-place UPDATE record. Rewrites the slot's payload
    /// with `post_image_bytes`, stamps `xmax`/`cmax`/
    /// `infomask | UPDATED | UPDATED_IN_PLACE` on the header, and
    /// rebuilds the in-memory `(tid, writer_xid, pre_image_bytes)`
    /// undo entry so post-recovery cross-snapshot readers can still
    /// resolve the pre-image. Idempotent: a replayed record whose
    /// post-image already matches the slot bytes and whose xmax
    /// already equals `writer_xid` is a no-op.
    fn apply_update_in_place(&self, payload: &HeapUpdateInPlacePayload) -> Result<(), ApplyError> {
        self.apply_update_in_place_at_lsn(payload, Lsn::ZERO)
    }

    fn apply_update_in_place_at_lsn(
        &self,
        payload: &HeapUpdateInPlacePayload,
        record_lsn: Lsn,
    ) -> Result<(), ApplyError> {
        let page_id = payload.tid.page;
        let rel = page_id.relation;
        self.advance_counter(rel, page_id.block)?;

        {
            let guard = self
                .pool
                .get_page(page_id)
                .map_err(|e| ApplyError::Refused {
                    operation: "heap_update_in_place",
                    detail: format!("buffer pool: {e}"),
                })?;
            let mut page = guard.write();
            if should_skip_redo(&page, record_lsn) {
                return Ok(());
            }
            let existing = page
                .read_tuple(payload.tid.slot)
                .map_err(|e| ApplyError::Refused {
                    operation: "heap_update_in_place",
                    detail: format!("read slot: {e}"),
                })?;
            if existing.len() < TUPLE_HEADER_SIZE {
                return Err(ApplyError::Refused {
                    operation: "heap_update_in_place",
                    detail: String::from("slot shorter than tuple header"),
                });
            }
            let (mut hdr, _) =
                TupleHeader::decode(&existing[..TUPLE_HEADER_SIZE]).ok_or_else(|| {
                    ApplyError::Refused {
                        operation: "heap_update_in_place",
                        detail: String::from("header decode failed"),
                    }
                })?;

            // Idempotency: if xmax + UPDATED_IN_PLACE bit are already set
            // to this writer the record was already replayed. Confirm by
            // also checking the slot bytes match the post image so a
            // stale matching xmax across distinct cmax values still falls
            // through to a full rewrite.
            let post_matches = existing.len() == TUPLE_HEADER_SIZE + payload.post_image_bytes.len()
                && &existing[TUPLE_HEADER_SIZE..] == payload.post_image_bytes.as_slice();
            if post_matches
                && hdr.xmax == payload.writer_xid
                && hdr.infomask.contains(InfoMask::UPDATED_IN_PLACE)
            {
                stamp_replayed_lsn(&mut page, record_lsn);
                return Ok(());
            }

            hdr.xmax = payload.writer_xid;
            hdr.cmax = payload.command_id;
            hdr.infomask.set(InfoMask::UPDATED);
            hdr.infomask.set(InfoMask::UPDATED_IN_PLACE);
            let mut hdr_bytes = [0_u8; TUPLE_HEADER_SIZE];
            hdr.encode(&mut hdr_bytes);

            let page_bytes = page.as_bytes_mut();
            let item =
                item_id_from_page_bytes(page_bytes, payload.tid.slot, "heap_update_in_place")?;
            let slot_off = item_offset_usize(item, "heap_update_in_place")?;
            let slot_len = item_length_usize(item, "heap_update_in_place")?;
            if slot_len < TUPLE_HEADER_SIZE + payload.post_image_bytes.len() {
                return Err(ApplyError::Refused {
                    operation: "heap_update_in_place",
                    detail: format!("slot length {slot_len} too small for header + post-image"),
                });
            }
            page_bytes[slot_off..slot_off + TUPLE_HEADER_SIZE].copy_from_slice(&hdr_bytes);
            let payload_off = slot_off + TUPLE_HEADER_SIZE;
            page_bytes[payload_off..payload_off + payload.post_image_bytes.len()]
                .copy_from_slice(&payload.post_image_bytes);
            stamp_replayed_lsn(&mut page, record_lsn);
        }

        // Rebuild the in-memory undo entry. Push idempotently: if an
        // entry for the same `(tid, writer_xid, pre_image)` triple
        // already exists, skip — recovery may walk the same record
        // twice across restart cycles.
        {
            let log_handle = self
                .undo_log
                .entry(rel)
                .or_insert_with(|| parking_lot::RwLock::new(UndoRelationLog::default()));
            let mut log = log_handle.write();
            let already = log.entries.iter().rev().any(|e| {
                e.tid == payload.tid
                    && e.writer_xid == payload.writer_xid
                    && e.old_payload.as_slice() == payload.pre_image_bytes.as_slice()
            });
            if !already {
                if payload.pre_image_bytes.len() != 9 {
                    return Err(ApplyError::Refused {
                        operation: "heap_update_in_place",
                        detail: format!(
                            "invalid pre-image width: expected 9 bytes, got {}",
                            payload.pre_image_bytes.len()
                        ),
                    });
                }
                let mut pre = [0_u8; 9];
                pre.copy_from_slice(&payload.pre_image_bytes);
                log.entries.push(UndoEntry {
                    tid: payload.tid,
                    writer_xid: payload.writer_xid,
                    old_payload: pre,
                });
            }
        }

        self.column_cache.bump_version(rel);
        Ok(())
    }

    fn apply_update_in_place_batch_at_lsn(
        &self,
        payload: &HeapUpdateInPlaceBatchPayload,
        record_lsn: Lsn,
    ) -> Result<(), ApplyError> {
        let page_id = payload.page;
        let rel = page_id.relation;
        self.advance_counter(rel, page_id.block)?;

        {
            let guard = self
                .pool
                .get_page(page_id)
                .map_err(|e| ApplyError::Refused {
                    operation: "heap_update_in_place_batch",
                    detail: format!("buffer pool: {e}"),
                })?;
            let mut page = guard.write();
            if should_skip_redo(&page, record_lsn) {
                return Ok(());
            }

            for entry in &payload.entries {
                let existing = page
                    .read_tuple(entry.slot)
                    .map_err(|e| ApplyError::Refused {
                        operation: "heap_update_in_place_batch",
                        detail: format!("read slot: {e}"),
                    })?;
                if existing.len() < TUPLE_HEADER_SIZE {
                    return Err(ApplyError::Refused {
                        operation: "heap_update_in_place_batch",
                        detail: String::from("slot shorter than tuple header"),
                    });
                }
                let (mut hdr, _) =
                    TupleHeader::decode(&existing[..TUPLE_HEADER_SIZE]).ok_or_else(|| {
                        ApplyError::Refused {
                            operation: "heap_update_in_place_batch",
                            detail: String::from("header decode failed"),
                        }
                    })?;

                hdr.xmax = payload.writer_xid;
                hdr.cmax = payload.command_id;
                hdr.infomask.set(InfoMask::UPDATED);
                hdr.infomask.set(InfoMask::UPDATED_IN_PLACE);
                let mut hdr_bytes = [0_u8; TUPLE_HEADER_SIZE];
                hdr.encode(&mut hdr_bytes);

                let page_bytes = page.as_bytes_mut();
                let item =
                    item_id_from_page_bytes(page_bytes, entry.slot, "heap_update_in_place_batch")?;
                let slot_off = item_offset_usize(item, "heap_update_in_place_batch")?;
                let slot_len = item_length_usize(item, "heap_update_in_place_batch")?;
                if slot_len < TUPLE_HEADER_SIZE + entry.post_image.len() {
                    return Err(ApplyError::Refused {
                        operation: "heap_update_in_place_batch",
                        detail: format!("slot length {slot_len} too small for header + post-image"),
                    });
                }
                page_bytes[slot_off..slot_off + TUPLE_HEADER_SIZE].copy_from_slice(&hdr_bytes);
                let payload_off = slot_off + TUPLE_HEADER_SIZE;
                page_bytes[payload_off..payload_off + entry.post_image.len()]
                    .copy_from_slice(&entry.post_image);
            }
            stamp_replayed_lsn(&mut page, record_lsn);
        }

        {
            let log_handle = self
                .undo_log
                .entry(rel)
                .or_insert_with(|| parking_lot::RwLock::new(UndoRelationLog::default()));
            let mut log = log_handle.write();
            for entry in &payload.entries {
                let tid = TupleId::new(page_id, entry.slot);
                let already = log.entries.iter().rev().any(|existing| {
                    existing.tid == tid
                        && existing.writer_xid == payload.writer_xid
                        && existing.old_payload == entry.pre_image
                });
                if !already {
                    log.entries.push(UndoEntry {
                        tid,
                        writer_xid: payload.writer_xid,
                        old_payload: entry.pre_image,
                    });
                }
            }
        }

        self.column_cache.bump_version(rel);
        Ok(())
    }

    fn apply_update_int32_pair_delta_batch(
        &self,
        payload: &HeapUpdateInt32PairDeltaBatchPayload,
    ) -> Result<(), ApplyError> {
        self.apply_update_int32_pair_delta_batch_at_lsn(payload, Lsn::ZERO)
    }

    fn apply_update_int32_pair_delta_batch_at_lsn(
        &self,
        payload: &HeapUpdateInt32PairDeltaBatchPayload,
        record_lsn: Lsn,
    ) -> Result<(), ApplyError> {
        let page_id = payload.page;
        let rel = page_id.relation;
        self.advance_counter(rel, page_id.block)?;

        let mut undo_entries: Vec<(u16, [u8; 9])> = Vec::with_capacity(payload.slots.len());
        {
            let guard = self
                .pool
                .get_page(page_id)
                .map_err(|e| ApplyError::Refused {
                    operation: "heap_update_int32_pair_delta_batch",
                    detail: format!("buffer pool: {e}"),
                })?;
            let mut page = guard.write();
            if should_skip_redo(&page, record_lsn) {
                return Ok(());
            }
            let page_bytes = page.as_bytes_mut();
            for &slot in &payload.slots {
                let item = item_id_from_page_bytes(
                    page_bytes,
                    slot,
                    "heap_update_int32_pair_delta_batch",
                )?;
                let slot_off = item_offset_usize(item, "heap_update_int32_pair_delta_batch")?;
                let slot_len = item_length_usize(item, "heap_update_int32_pair_delta_batch")?;
                if slot_len < TUPLE_HEADER_SIZE + 9 {
                    return Err(ApplyError::Refused {
                        operation: "heap_update_int32_pair_delta_batch",
                        detail: format!("slot length {slot_len} too small for int32 pair update"),
                    });
                }

                let (mut hdr, _) =
                    TupleHeader::decode(&page_bytes[slot_off..slot_off + TUPLE_HEADER_SIZE])
                        .ok_or_else(|| ApplyError::Refused {
                            operation: "heap_update_int32_pair_delta_batch",
                            detail: String::from("header decode failed"),
                        })?;
                let payload_off = slot_off + TUPLE_HEADER_SIZE;
                let mut pre_image = [0_u8; 9];
                pre_image.copy_from_slice(&page_bytes[payload_off..payload_off + 9]);
                let target_off = payload_off
                    + match payload.target_col {
                        0 => 1,
                        1 => 5,
                        _ => {
                            return Err(ApplyError::Refused {
                                operation: "heap_update_int32_pair_delta_batch",
                                detail: format!("target_col {} out of range", payload.target_col),
                            });
                        }
                    };
                let current = i32::from_le_bytes([
                    page_bytes[target_off],
                    page_bytes[target_off + 1],
                    page_bytes[target_off + 2],
                    page_bytes[target_off + 3],
                ]);
                let already_applied = hdr.xmax == payload.writer_xid
                    && hdr.infomask.contains(InfoMask::UPDATED_IN_PLACE);
                if already_applied {
                    let restored =
                        current
                            .checked_sub(payload.delta)
                            .ok_or_else(|| ApplyError::Refused {
                                operation: "heap_update_int32_pair_delta_batch",
                                detail: String::from("pre-image delta subtraction overflow"),
                            })?;
                    pre_image[target_off - payload_off..target_off - payload_off + 4]
                        .copy_from_slice(&restored.to_le_bytes());
                } else {
                    let updated =
                        current
                            .checked_add(payload.delta)
                            .ok_or_else(|| ApplyError::Refused {
                                operation: "heap_update_int32_pair_delta_batch",
                                detail: String::from("post-image delta addition overflow"),
                            })?;
                    page_bytes[target_off..target_off + 4].copy_from_slice(&updated.to_le_bytes());
                    hdr.xmax = payload.writer_xid;
                    hdr.cmax = payload.command_id;
                    hdr.infomask.set(InfoMask::UPDATED);
                    hdr.infomask.set(InfoMask::UPDATED_IN_PLACE);
                    let mut hdr_bytes = [0_u8; TUPLE_HEADER_SIZE];
                    hdr.encode(&mut hdr_bytes);
                    page_bytes[slot_off..slot_off + TUPLE_HEADER_SIZE].copy_from_slice(&hdr_bytes);
                }
                undo_entries.push((slot, pre_image));
            }
            stamp_replayed_lsn(&mut page, record_lsn);
        }

        if !undo_entries.is_empty() {
            let log_handle = self
                .undo_log
                .entry(rel)
                .or_insert_with(|| parking_lot::RwLock::new(UndoRelationLog::default()));
            let mut log = log_handle.write();
            for (slot, pre_image) in undo_entries {
                let tid = TupleId::new(page_id, slot);
                let already = log.entries.iter().rev().any(|existing| {
                    existing.tid == tid
                        && existing.writer_xid == payload.writer_xid
                        && existing.old_payload == pre_image
                });
                if !already {
                    log.entries.push(UndoEntry {
                        tid,
                        writer_xid: payload.writer_xid,
                        old_payload: pre_image,
                    });
                }
            }
            self.column_cache.bump_version(rel);
        }
        Ok(())
    }

    fn apply_update_int32_pair_delta_range_batch(
        &self,
        payload: &HeapUpdateInt32PairDeltaRangeBatchPayload,
    ) -> Result<(), ApplyError> {
        self.apply_update_int32_pair_delta_range_batch_at_lsn(payload, Lsn::ZERO)
    }

    fn apply_update_int32_pair_delta_range_batch_at_lsn(
        &self,
        payload: &HeapUpdateInt32PairDeltaRangeBatchPayload,
        record_lsn: Lsn,
    ) -> Result<(), ApplyError> {
        if payload.slot_count == 0 {
            return Err(refused(
                "heap_update_int32_pair_delta_range_batch",
                "slot_count must be nonzero",
            ));
        }
        let mut slots = Vec::with_capacity(usize::from(payload.slot_count));
        for delta in 0..payload.slot_count {
            let slot = payload.first_slot.checked_add(delta).ok_or_else(|| {
                refused(
                    "heap_update_int32_pair_delta_range_batch",
                    "slot range overflow",
                )
            })?;
            slots.push(slot);
        }
        let expanded = HeapUpdateInt32PairDeltaBatchPayload {
            page: payload.page,
            writer_xid: payload.writer_xid,
            command_id: payload.command_id,
            target_col: payload.target_col,
            delta: payload.delta,
            slots,
        };
        self.apply_update_int32_pair_delta_batch_at_lsn(&expanded, record_lsn)
    }

    /// Apply an in-place DELETE record. Stamps `xmax`/`cmax`/
    /// `infomask | UPDATED` on the tuple header. Idempotent: an
    /// already-stamped slot with the same `xmax` is a no-op.
    fn apply_delete_in_place(&self, payload: &HeapDeleteInPlacePayload) -> Result<(), ApplyError> {
        self.apply_delete_in_place_at_lsn(payload, Lsn::ZERO)
    }

    fn apply_delete_in_place_at_lsn(
        &self,
        payload: &HeapDeleteInPlacePayload,
        record_lsn: Lsn,
    ) -> Result<(), ApplyError> {
        let page_id = payload.tid.page;
        self.advance_counter(page_id.relation, page_id.block)?;

        {
            let guard = self
                .pool
                .get_page(page_id)
                .map_err(|e| ApplyError::Refused {
                    operation: "heap_delete_in_place",
                    detail: format!("buffer pool: {e}"),
                })?;
            let mut page = guard.write();
            if should_skip_redo(&page, record_lsn) {
                return Ok(());
            }
            let existing = page
                .read_tuple(payload.tid.slot)
                .map_err(|e| ApplyError::Refused {
                    operation: "heap_delete_in_place",
                    detail: format!("read slot: {e}"),
                })?;
            if existing.len() < TUPLE_HEADER_SIZE {
                return Err(ApplyError::Refused {
                    operation: "heap_delete_in_place",
                    detail: String::from("slot shorter than tuple header"),
                });
            }
            let (mut hdr, _) =
                TupleHeader::decode(&existing[..TUPLE_HEADER_SIZE]).ok_or_else(|| {
                    ApplyError::Refused {
                        operation: "heap_delete_in_place",
                        detail: String::from("header decode failed"),
                    }
                })?;

            if hdr.xmax == payload.xmax {
                stamp_replayed_lsn(&mut page, record_lsn);
                return Ok(());
            }
            hdr.mark_deleted(payload.xmax, payload.cmax);
            let mut hdr_bytes = [0_u8; TUPLE_HEADER_SIZE];
            hdr.encode(&mut hdr_bytes);

            let page_bytes = page.as_bytes_mut();
            let item =
                item_id_from_page_bytes(page_bytes, payload.tid.slot, "heap_delete_in_place")?;
            let slot_off = item_offset_usize(item, "heap_delete_in_place")?;
            page_bytes[slot_off..slot_off + TUPLE_HEADER_SIZE].copy_from_slice(&hdr_bytes);
            stamp_replayed_lsn(&mut page, record_lsn);
        }

        self.column_cache.bump_version(page_id.relation);
        Ok(())
    }

    fn apply_delete_in_place_batch(
        &self,
        payload: &HeapDeleteInPlaceBatchPayload,
    ) -> Result<(), ApplyError> {
        self.apply_delete_in_place_batch_at_lsn(payload, Lsn::ZERO)
    }

    fn apply_delete_in_place_batch_at_lsn(
        &self,
        payload: &HeapDeleteInPlaceBatchPayload,
        record_lsn: Lsn,
    ) -> Result<(), ApplyError> {
        let page_id = payload.page;
        self.advance_counter(page_id.relation, page_id.block)?;
        let mut applied = false;

        {
            let guard = self
                .pool
                .get_page(page_id)
                .map_err(|e| ApplyError::Refused {
                    operation: "heap_delete_in_place_batch",
                    detail: format!("buffer pool: {e}"),
                })?;
            let mut page = guard.write();
            if should_skip_redo(&page, record_lsn) {
                return Ok(());
            }

            for entry in &payload.entries {
                let existing = page
                    .read_tuple(entry.slot)
                    .map_err(|e| ApplyError::Refused {
                        operation: "heap_delete_in_place_batch",
                        detail: format!("read slot: {e}"),
                    })?;
                if existing.len() < TUPLE_HEADER_SIZE {
                    return Err(ApplyError::Refused {
                        operation: "heap_delete_in_place_batch",
                        detail: String::from("slot shorter than tuple header"),
                    });
                }
                let (mut hdr, _) =
                    TupleHeader::decode(&existing[..TUPLE_HEADER_SIZE]).ok_or_else(|| {
                        ApplyError::Refused {
                            operation: "heap_delete_in_place_batch",
                            detail: String::from("header decode failed"),
                        }
                    })?;
                if hdr.xmax == payload.xmax {
                    continue;
                }

                hdr.mark_deleted(payload.xmax, payload.cmax);
                let mut hdr_bytes = [0_u8; TUPLE_HEADER_SIZE];
                hdr.encode(&mut hdr_bytes);

                let page_bytes = page.as_bytes_mut();
                let item =
                    item_id_from_page_bytes(page_bytes, entry.slot, "heap_delete_in_place_batch")?;
                let slot_off = item_offset_usize(item, "heap_delete_in_place_batch")?;
                page_bytes[slot_off..slot_off + TUPLE_HEADER_SIZE].copy_from_slice(&hdr_bytes);
                applied = true;
            }
            stamp_replayed_lsn(&mut page, record_lsn);
        }

        if applied {
            self.column_cache.bump_version(page_id.relation);
        }
        Ok(())
    }

    fn apply_delete_in_place_range_batch(
        &self,
        payload: &HeapDeleteInPlaceRangeBatchPayload,
    ) -> Result<(), ApplyError> {
        self.apply_delete_in_place_range_batch_at_lsn(payload, Lsn::ZERO)
    }

    fn apply_delete_in_place_range_batch_at_lsn(
        &self,
        payload: &HeapDeleteInPlaceRangeBatchPayload,
        record_lsn: Lsn,
    ) -> Result<(), ApplyError> {
        let page_id = payload.page;
        if payload.slot_count == 0 {
            return Err(ApplyError::Refused {
                operation: "heap_delete_in_place_range_batch",
                detail: String::from("slot_count must be nonzero"),
            });
        }
        self.advance_counter(page_id.relation, page_id.block)?;
        let mut applied = false;

        {
            let guard = self
                .pool
                .get_page(page_id)
                .map_err(|e| ApplyError::Refused {
                    operation: "heap_delete_in_place_range_batch",
                    detail: format!("buffer pool: {e}"),
                })?;
            let mut page = guard.write();
            if should_skip_redo(&page, record_lsn) {
                return Ok(());
            }

            let page_bytes = page.as_bytes_mut();
            for delta in 0..payload.slot_count {
                let slot =
                    payload
                        .first_slot
                        .checked_add(delta)
                        .ok_or_else(|| ApplyError::Refused {
                            operation: "heap_delete_in_place_range_batch",
                            detail: String::from("slot range overflow"),
                        })?;
                let item =
                    item_id_from_page_bytes(page_bytes, slot, "heap_delete_in_place_range_batch")?;
                let slot_off = item_offset_usize(item, "heap_delete_in_place_range_batch")?;
                let slot_len = item_length_usize(item, "heap_delete_in_place_range_batch")?;
                if slot_len < TUPLE_HEADER_SIZE {
                    return Err(ApplyError::Refused {
                        operation: "heap_delete_in_place_range_batch",
                        detail: String::from("slot shorter than tuple header"),
                    });
                }
                let (mut hdr, _) =
                    TupleHeader::decode(&page_bytes[slot_off..slot_off + TUPLE_HEADER_SIZE])
                        .ok_or_else(|| ApplyError::Refused {
                            operation: "heap_delete_in_place_range_batch",
                            detail: String::from("header decode failed"),
                        })?;
                if hdr.xmax == payload.xmax {
                    continue;
                }

                hdr.mark_deleted(payload.xmax, payload.cmax);
                let mut hdr_bytes = [0_u8; TUPLE_HEADER_SIZE];
                hdr.encode(&mut hdr_bytes);
                page_bytes[slot_off..slot_off + TUPLE_HEADER_SIZE].copy_from_slice(&hdr_bytes);
                applied = true;
            }
            stamp_replayed_lsn(&mut page, record_lsn);
        }

        if applied {
            self.column_cache.bump_version(page_id.relation);
        }
        Ok(())
    }

    /// Apply a full-page-write record by restoring the page image verbatim.
    ///
    /// The entire 8 KiB page image is written into the buffer pool's frame.
    /// This is the crash-recovery path for torn-write protection: if the page
    /// on disk was partially written at the time of the crash, the FPW record
    /// carries the consistent pre-mutation image so subsequent mutation records
    /// can be re-applied correctly.
    fn apply_full_page_write(&self, payload: &FullPageWritePayload) -> Result<(), ApplyError> {
        self.apply_full_page_write_at_lsn(payload, Lsn::ZERO)
    }

    fn apply_full_page_write_at_lsn(
        &self,
        payload: &FullPageWritePayload,
        record_lsn: Lsn,
    ) -> Result<(), ApplyError> {
        use ultrasql_core::constants::PAGE_SIZE;

        let page_id = payload.page;
        self.advance_counter(page_id.relation, page_id.block)?;

        {
            let guard = self
                .pool
                .get_page(page_id)
                .map_err(|e| ApplyError::Refused {
                    operation: "fpw",
                    detail: format!("buffer pool: {e}"),
                })?;

            let mut page = guard.write();
            if should_skip_redo(&page, record_lsn) {
                return Ok(());
            }
            if payload.page_bytes.len() != PAGE_SIZE {
                return Err(ApplyError::Refused {
                    operation: "fpw",
                    detail: format!(
                        "page_bytes length {} != PAGE_SIZE {}",
                        payload.page_bytes.len(),
                        PAGE_SIZE
                    ),
                });
            }
            page.as_bytes_mut()
                .copy_from_slice(&payload.page_bytes[..PAGE_SIZE]);
            stamp_replayed_lsn(&mut page, record_lsn);
        }

        Ok(())
    }

    /// Replay one B-tree operation into the shared buffer pool.
    ///
    /// Insert/delete records carry the logical key and heap TID, so redo can
    /// rebuild the leaf entry through the normal B-tree API. Split records are
    /// redundant in this replay model: the preceding insert redo will split
    /// deterministically when the leaf is full, and full-page writes restore
    /// any already-flushed split pages before logical redo continues.
    fn apply_btree_op(&self, payload: &BTreeOpPayload) -> Result<(), ApplyError> {
        match payload.op {
            BTreeOpKind::Insert => {
                let key = decode_btree_key(payload)?;
                let tid = decode_btree_tid(payload)?;
                let mut tree = open_or_create_btree(self, payload.index_rel)?;
                match tree.insert_non_unique::<i64>(key, tid, Xid::INVALID, None) {
                    Ok(()) | Err(BTreeError::DuplicateKey) => Ok(()),
                    Err(e) => Err(ApplyError::Refused {
                        operation: "btree_insert",
                        detail: e.to_string(),
                    }),
                }
            }
            BTreeOpKind::Delete => {
                let key = decode_btree_key(payload)?;
                let tid = decode_btree_tid(payload)?;
                let mut tree = open_or_create_btree(self, payload.index_rel)?;
                tree.delete::<i64>(key, tid)
                    .map(|_| ())
                    .map_err(|e| ApplyError::Refused {
                        operation: "btree_delete",
                        detail: e.to_string(),
                    })
            }
            BTreeOpKind::Split => Ok(()),
        }
    }
}

fn open_or_create_btree<L: PageLoader + 'static>(
    heap: &HeapAccess<L>,
    rel: RelationId,
) -> Result<BTree<L>, ApplyError> {
    let tree = BTree::open(std::sync::Arc::clone(&heap.pool), rel, BlockNumber::new(0));
    match tree.lookup::<i64>(i64::MIN) {
        Ok(_) => Ok(tree),
        Err(BTreeError::MalformedNode(_)) | Err(BTreeError::Page(_)) => {
            BTree::create(std::sync::Arc::clone(&heap.pool), rel).map_err(|e| ApplyError::Refused {
                operation: "btree_create",
                detail: e.to_string(),
            })
        }
        Err(e) => Err(ApplyError::Refused {
            operation: "btree_open",
            detail: e.to_string(),
        }),
    }
}

fn decode_btree_key(payload: &BTreeOpPayload) -> Result<i64, ApplyError> {
    if payload.key_bytes.len() != 8 {
        return Err(ApplyError::Refused {
            operation: "btree_decode_key",
            detail: format!("expected 8 key bytes, got {}", payload.key_bytes.len()),
        });
    }
    read_i64_le(&payload.key_bytes).map_err(|e| ApplyError::Refused {
        operation: "btree_decode_key",
        detail: e.to_string(),
    })
}

fn decode_btree_tid(payload: &BTreeOpPayload) -> Result<TupleId, ApplyError> {
    if payload.child_or_value.len() != 12 {
        return Err(ApplyError::Refused {
            operation: "btree_decode_tid",
            detail: format!(
                "expected 12 child/value bytes, got {}",
                payload.child_or_value.len()
            ),
        });
    }
    let rel = RelationId::new(read_u32_le(&payload.child_or_value[0..4]).map_err(|e| {
        ApplyError::Refused {
            operation: "btree_decode_tid",
            detail: e.to_string(),
        }
    })?);
    let block = BlockNumber::new(read_u32_le(&payload.child_or_value[4..8]).map_err(|e| {
        ApplyError::Refused {
            operation: "btree_decode_tid",
            detail: e.to_string(),
        }
    })?);
    let slot = read_u16_le(&payload.child_or_value[8..10]).map_err(|e| ApplyError::Refused {
        operation: "btree_decode_tid",
        detail: e.to_string(),
    })?;
    Ok(TupleId::new(PageId::new(rel, block), slot))
}

impl<L: PageLoader> HeapAccess<L> {
    /// Advance the per-relation block counter to at least `block + 1`.
    ///
    /// Called by the applier when it writes to a block during recovery to
    /// ensure that post-recovery scans driven by `block_count()` cover all
    /// replayed blocks.
    pub(crate) fn advance_counter(
        &self,
        rel: ultrasql_core::RelationId,
        block: BlockNumber,
    ) -> Result<(), ApplyError> {
        let counter = self.counter_for(rel);
        let needed = block
            .raw()
            .checked_add(1)
            .ok_or_else(|| ApplyError::Refused {
                operation: "advance_counter",
                detail: format!(
                    "block {} cannot be represented as exclusive block_count",
                    block.raw()
                ),
            })?;
        // CAS loop: advance only if the current value is less than `needed`.
        let mut current = counter.load(Ordering::Acquire);
        while current < needed {
            match counter.compare_exchange_weak(
                current,
                needed,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use parking_lot::Mutex;
    use ultrasql_core::constants::PAGE_SIZE;
    use ultrasql_core::{BlockNumber, CommandId, Lsn, PageId, RelationId, Result, TupleId, Xid};
    use ultrasql_mvcc::TupleHeader;
    use ultrasql_mvcc::tuple_header::TUPLE_HEADER_SIZE;
    use ultrasql_wal::applier::{ApplyError, HeapTarget};
    use ultrasql_wal::payload::{
        BTreeOpKind, BTreeOpPayload, FullPageWritePayload, HeapDeletePayload, HeapInsertBatchEntry,
        HeapInsertBatchPayload, HeapInsertPayload,
    };

    use crate::btree::BTree;
    use crate::buffer_pool::{BufferPool, PageLoader};
    use crate::heap::{HeapAccess, InsertOptions};
    use crate::page::Page;

    /// In-memory page loader that persists pages in a `HashMap` so writes
    /// survive across pin/unpin cycles.
    #[derive(Default)]
    struct MapLoader {
        store: Mutex<HashMap<PageId, Box<[u8; PAGE_SIZE]>>>,
    }

    impl MapLoader {
        fn new() -> Self {
            Self::default()
        }

        fn with_page(page_id: PageId, page: &Page) -> Self {
            let loader = Self::new();
            let mut copy: Box<[u8; PAGE_SIZE]> = vec![0_u8; PAGE_SIZE]
                .into_boxed_slice()
                .try_into()
                .expect("alloc matches PAGE_SIZE");
            copy.copy_from_slice(page.as_bytes());
            loader.store.lock().insert(page_id, copy);
            loader
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
                    .map_err(|e| ultrasql_core::Error::Corruption(format!("map loader: {e}")));
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
        RelationId::new(77)
    }

    fn page_id(block: u32) -> PageId {
        PageId::new(rel(), BlockNumber::new(block))
    }

    fn tuple_id(block: u32, slot: u16) -> TupleId {
        TupleId::new(page_id(block), slot)
    }

    fn btree_tid_bytes(tid: TupleId) -> Vec<u8> {
        let mut out = vec![0_u8; 12];
        out[0..4].copy_from_slice(&tid.page.relation.oid().raw().to_le_bytes());
        out[4..8].copy_from_slice(&tid.page.block.raw().to_le_bytes());
        out[8..10].copy_from_slice(&tid.slot.to_le_bytes());
        out
    }

    /// Build a minimal tuple byte vector: a fresh header with no payload.
    fn minimal_tuple(xmin: u64, tid: TupleId) -> Vec<u8> {
        let hdr = TupleHeader::fresh(Xid::new(xmin), CommandId::FIRST, tid, 0);
        let mut bytes = vec![0_u8; TUPLE_HEADER_SIZE];
        hdr.encode(&mut bytes[..TUPLE_HEADER_SIZE]);
        bytes
    }

    fn make_heap() -> HeapAccess<MapLoader> {
        let pool = Arc::new(BufferPool::new(64, MapLoader::new()));
        HeapAccess::new(pool)
    }

    #[test]
    fn apply_insert_writes_tuple_to_page() {
        let heap = make_heap();
        let tid = tuple_id(0, 0);
        let tuple_bytes = minimal_tuple(1, tid);

        let payload = HeapInsertPayload { tid, tuple_bytes };
        heap.apply_insert(&payload).unwrap();

        // Verify the tuple is readable via fetch.
        let fetched = heap.fetch(tid).unwrap();
        assert_eq!(fetched.tid, tid);
        assert_eq!(fetched.header.xmin, Xid::new(1));
    }

    #[test]
    fn apply_insert_is_idempotent() {
        let heap = make_heap();
        let tid = tuple_id(0, 0);
        let tuple_bytes = minimal_tuple(1, tid);
        let payload = HeapInsertPayload { tid, tuple_bytes };
        heap.apply_insert(&payload).unwrap();
        // Second application of the same record must not fail.
        heap.apply_insert(&payload).unwrap();
        // Only one slot should exist.
        assert_eq!(heap.block_count(rel()), 1);
    }

    #[test]
    fn apply_insert_batch_writes_tuples_to_page() {
        let heap = make_heap();
        let tid0 = tuple_id(0, 0);
        let tid1 = tuple_id(0, 1);
        let payload = HeapInsertBatchPayload {
            page: page_id(0),
            entries: vec![
                HeapInsertBatchEntry {
                    slot: 0,
                    tuple_bytes: minimal_tuple(1, tid0),
                },
                HeapInsertBatchEntry {
                    slot: 1,
                    tuple_bytes: minimal_tuple(1, tid1),
                },
            ],
        };

        heap.apply_insert_batch(&payload).unwrap();
        heap.apply_insert_batch(&payload).unwrap();

        assert_eq!(heap.fetch(tid0).unwrap().tid, tid0);
        assert_eq!(heap.fetch(tid1).unwrap().tid, tid1);
        assert_eq!(heap.block_count(rel()), 1);
    }

    #[test]
    fn apply_delete_stamps_xmax() {
        let heap = make_heap();
        let tid = tuple_id(0, 0);

        // Insert via the normal path so the slot is valid.
        heap.insert(
            rel(),
            b"hello",
            InsertOptions {
                xmin: Xid::new(1),
                command_id: CommandId::FIRST,
                n_atts: 0,
                wal: None,
                fsm: None,
                vm: None,
            },
        )
        .unwrap();

        let payload = HeapDeletePayload {
            tid,
            xmax: Xid::new(2),
            cmax: CommandId::new(1),
        };
        heap.apply_delete(&payload).unwrap();

        let fetched = heap.fetch(tid).unwrap();
        assert_eq!(fetched.header.xmax, Xid::new(2));
    }

    #[test]
    fn apply_delete_at_lsn_skips_when_page_lsn_is_newer() {
        let heap = make_heap();
        let tid = tuple_id(0, 0);
        heap.insert(
            rel(),
            b"hello",
            InsertOptions {
                xmin: Xid::new(1),
                command_id: CommandId::FIRST,
                n_atts: 0,
                wal: None,
                fsm: None,
                vm: None,
            },
        )
        .unwrap();
        {
            let guard = heap.pool.get_page(page_id(0)).unwrap();
            guard.write().set_lsn(200);
        }

        let payload = HeapDeletePayload {
            tid,
            xmax: Xid::new(2),
            cmax: CommandId::new(1),
        };
        heap.apply_delete_at_lsn(&payload, Lsn::new(100)).unwrap();

        let fetched = heap.fetch(tid).unwrap();
        assert_eq!(
            fetched.header.xmax,
            Xid::INVALID,
            "old delete redo must not overwrite newer page image"
        );
        let guard = heap.pool.get_page(page_id(0)).unwrap();
        assert_eq!(guard.read().header().lsn, 200);
    }

    #[test]
    fn apply_delete_at_lsn_skips_when_page_lsn_covers_record() {
        let heap = make_heap();
        let tid = tuple_id(0, 0);
        heap.insert(
            rel(),
            b"hello",
            InsertOptions {
                xmin: Xid::new(1),
                command_id: CommandId::FIRST,
                n_atts: 0,
                wal: None,
                fsm: None,
                vm: None,
            },
        )
        .unwrap();
        {
            let guard = heap.pool.get_page(page_id(0)).unwrap();
            guard.write().set_lsn(100);
        }

        let payload = HeapDeletePayload {
            tid,
            xmax: Xid::new(2),
            cmax: CommandId::new(1),
        };
        heap.apply_delete_at_lsn(&payload, Lsn::new(100)).unwrap();

        let fetched = heap.fetch(tid).unwrap();
        assert_eq!(
            fetched.header.xmax,
            Xid::INVALID,
            "equal page LSN means redo is already reflected"
        );
    }

    #[test]
    fn apply_fpw_restores_page_image() {
        let heap = make_heap();
        // Insert something to create page 0 for this relation.
        heap.insert(
            rel(),
            b"data",
            InsertOptions {
                xmin: Xid::new(1),
                command_id: CommandId::FIRST,
                n_atts: 0,
                wal: None,
                fsm: None,
                vm: None,
            },
        )
        .unwrap();

        // Build a zeroed-out page image (simulates a restored checkpoint image).
        let zeroed = crate::page::Page::new_heap();
        let page_bytes = zeroed.as_bytes().to_vec();
        assert_eq!(page_bytes.len(), PAGE_SIZE);

        let pid = page_id(0);
        let payload = FullPageWritePayload {
            page: pid,
            page_bytes: page_bytes.clone(),
        };
        heap.apply_full_page_write(&payload).unwrap();

        // After FPW the page bytes must match the payload.
        let guard = heap.pool.get_page(pid).unwrap();
        let actual = guard.read().as_bytes().to_vec();
        assert_eq!(actual, page_bytes, "FPW should restore page verbatim");
    }

    #[test]
    fn apply_fpw_repairs_torn_page_before_tuple_redo() {
        let tid = tuple_id(0, 0);
        let mut torn_page = Page::new_heap();
        torn_page.insert_tuple(&minimal_tuple(99, tid)).unwrap();
        torn_page.set_lsn(1);
        let loader = MapLoader::with_page(page_id(0), &torn_page);
        let heap = HeapAccess::new(Arc::new(BufferPool::new(64, loader)));

        let torn_tuple = heap.fetch(tid).unwrap();
        assert_eq!(
            torn_tuple.header.xmin,
            Xid::new(99),
            "test setup must expose the wrong torn-page row before FPW"
        );

        let checkpoint_image = Page::new_heap();
        let fpw = FullPageWritePayload {
            page: page_id(0),
            page_bytes: checkpoint_image.as_bytes().to_vec(),
        };
        heap.apply_full_page_write_at_lsn(&fpw, Lsn::new(100))
            .unwrap();

        let good_tuple = minimal_tuple(7, tid);
        heap.apply_insert_at_lsn(
            &HeapInsertPayload {
                tid,
                tuple_bytes: good_tuple,
            },
            Lsn::new(128),
        )
        .unwrap();

        let recovered = heap.fetch(tid).unwrap();
        assert_eq!(
            recovered.header.xmin,
            Xid::new(7),
            "FPW replay must repair torn bytes before row redo"
        );
    }

    #[test]
    fn apply_fpw_at_lsn_skips_when_page_lsn_is_newer() {
        let heap = make_heap();
        heap.insert(
            rel(),
            b"data",
            InsertOptions {
                xmin: Xid::new(1),
                command_id: CommandId::FIRST,
                n_atts: 0,
                wal: None,
                fsm: None,
                vm: None,
            },
        )
        .unwrap();
        let pid = page_id(0);
        let original = {
            let guard = heap.pool.get_page(pid).unwrap();
            let mut page = guard.write();
            page.set_lsn(200);
            page.as_bytes().to_vec()
        };
        let zeroed = crate::page::Page::new_heap();
        let payload = FullPageWritePayload {
            page: pid,
            page_bytes: zeroed.as_bytes().to_vec(),
        };

        heap.apply_full_page_write_at_lsn(&payload, Lsn::new(100))
            .unwrap();

        let guard = heap.pool.get_page(pid).unwrap();
        assert_eq!(
            guard.read().as_bytes(),
            original.as_slice(),
            "old FPW redo must not replace newer page image"
        );
    }

    #[test]
    fn apply_btree_op_replays_insert_and_delete() {
        let heap = make_heap();
        let index_rel = RelationId::new(88);
        let tid = tuple_id(3, 2);
        let insert = BTreeOpPayload {
            op: BTreeOpKind::Insert,
            index_rel,
            page: PageId::new(index_rel, BlockNumber::new(0)),
            key_bytes: 42_i64.to_le_bytes().to_vec(),
            child_or_value: btree_tid_bytes(tid),
        };

        heap.apply_btree_op(&insert).unwrap();
        heap.apply_btree_op(&insert).unwrap();

        let tree = BTree::open(
            Arc::clone(heap.buffer_pool()),
            index_rel,
            BlockNumber::new(0),
        );
        assert_eq!(tree.lookup_all::<i64>(42).unwrap(), vec![tid]);

        let delete = BTreeOpPayload {
            op: BTreeOpKind::Delete,
            ..insert
        };
        heap.apply_btree_op(&delete).unwrap();
        let tree = BTree::open(
            Arc::clone(heap.buffer_pool()),
            index_rel,
            BlockNumber::new(0),
        );
        assert!(tree.lookup_all::<i64>(42).unwrap().is_empty());
    }

    #[test]
    fn advance_counter_updates_block_count() {
        let heap = make_heap();
        heap.advance_counter(rel(), BlockNumber::new(4)).unwrap();
        assert_eq!(heap.block_count(rel()), 5);
        // Advancing to a lower block must not decrease the counter.
        heap.advance_counter(rel(), BlockNumber::new(2)).unwrap();
        assert_eq!(heap.block_count(rel()), 5);
    }

    #[test]
    fn apply_insert_rejects_unrepresentable_block_count_without_panic() {
        let heap = make_heap();
        let tid = tuple_id(u32::MAX, 0);
        let payload = HeapInsertPayload {
            tid,
            tuple_bytes: minimal_tuple(1, tid),
        };

        let result = heap.apply_insert(&payload);

        assert!(matches!(
            result,
            Err(ApplyError::Refused {
                operation: "advance_counter",
                ..
            })
        ));
    }
}
