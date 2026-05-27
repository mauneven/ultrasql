//! See `crate::heap` for the public API.
//!
//! Part of the `heap` module split — each `impl<L: PageLoader>
//! HeapAccess<L>` block here adds methods to the type defined in
//! `heap/mod.rs`. Splitting across files keeps each unit under the
//! 600-line ceiling without changing semantics.

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::cast_lossless,
    reason = "on-disk format / fixed-width packing; narrowings bounded by PAGE_SIZE / relation size"
)]
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
    DeleteOptions, HeapAccess, HeapError, HeapTuple, InsertOptions, Int32PairUndoBatch, UndoEntry,
    UndoRelationLog, UpdateOptions, UpdateOutcome, UpdatePayload,
};

struct Int32PairRangeUpdate {
    total_updated: usize,
    compact_undo: Vec<Int32PairUndoBatch>,
}

impl<L: PageLoader> HeapAccess<L> {
    /// Roll back every in-place UPDATE performed by `xid` by
    /// restoring the slot's pre-image from the undo log and clearing
    /// the `xmax / cmax / UPDATED / UPDATED_IN_PLACE` header bits.
    ///
    /// Walks every relation's undo log, splits its entries into
    /// (kept, rolled-back), and rewrites each rolled-back slot's
    /// payload + header. The kept entries are written back to the
    /// per-relation log so future rollbacks of other XIDs can still
    /// find them.
    ///
    /// Called by the server's transaction abort path
    /// (`finalise_autocommit` on Err, explicit ROLLBACK, failed-
    /// transaction COMMIT). Idempotent: a second call for the same
    /// `xid` finds nothing to do.
    pub fn rollback_in_place_updates(&self, xid: Xid) -> Result<usize, HeapError> {
        let mut total_restored: usize = 0;
        // Snapshot the keys upfront so we don't hold a `DashMap`
        // shard read lock across the per-relation work.
        let rels: Vec<RelationId> = self.undo_log.iter().map(|e| *e.key()).collect();
        for rel in rels {
            let Some(log_handle) = self.undo_log.get(&rel) else {
                continue;
            };
            let mut log = log_handle.write();
            if log.entries.is_empty() && log.int32_pair_batches.is_empty() {
                continue;
            }
            // Partition entries: keep everything not written by `xid`;
            // collect the rolled-back set so we can apply them under
            // page guards.
            let mut to_apply: Vec<UndoEntry> = Vec::new();
            let mut compact_to_apply: Vec<Int32PairUndoBatch> = Vec::new();
            let mut kept: Vec<UndoEntry> = Vec::with_capacity(log.entries.len());
            for e in std::mem::take(&mut log.entries) {
                if e.writer_xid == xid {
                    to_apply.push(e);
                } else {
                    kept.push(e);
                }
            }
            log.entries = kept;
            let mut kept_batches: Vec<Int32PairUndoBatch> =
                Vec::with_capacity(log.int32_pair_batches.len());
            for batch in std::mem::take(&mut log.int32_pair_batches) {
                if batch.writer_xid == xid {
                    compact_to_apply.push(batch);
                } else {
                    kept_batches.push(batch);
                }
            }
            log.int32_pair_batches = kept_batches;
            drop(log);

            if to_apply.is_empty() && compact_to_apply.is_empty() {
                continue;
            }

            let restored_before = total_restored;
            // Process per-page so each affected page is pinned once.
            // Entries are sorted by tid (appended in (page, slot)
            // order); `to_apply` therefore is sorted as well.
            let mut i = 0;
            while i < to_apply.len() {
                let page_id = to_apply[i].tid.page;
                let mut j = i + 1;
                while j < to_apply.len() && to_apply[j].tid.page == page_id {
                    j += 1;
                }
                let guard = self.pool.get_page(page_id)?;
                let mut page = guard.write();
                let bytes = page.as_bytes_mut();
                for entry in &to_apply[i..j] {
                    // Locate the slot via item-id.
                    let item_id_off = crate::page::PAGE_HEADER_SIZE
                        + usize::from(entry.tid.slot) * crate::page::ITEMID_SIZE;
                    let item_raw = u32::from_le_bytes([
                        bytes[item_id_off],
                        bytes[item_id_off + 1],
                        bytes[item_id_off + 2],
                        bytes[item_id_off + 3],
                    ]);
                    if item_raw & 0b11 != 1 {
                        continue;
                    }
                    let length = ((item_raw >> 2) & 0x7FFF) as usize;
                    let offset = ((item_raw >> 17) & 0x7FFF) as usize;
                    if length < TUPLE_HEADER_SIZE
                        || offset.checked_add(length).is_none_or(|e| e > bytes.len())
                    {
                        return Err(HeapError::MalformedHeader("slot shorter than header"));
                    }
                    // Restore the payload bytes (pre-image is the
                    // full payload; offset+TUPLE_HEADER_SIZE..end).
                    let payload_off = offset + TUPLE_HEADER_SIZE;
                    let pre = &entry.old_payload;
                    let copy_len = pre.len().min(length - TUPLE_HEADER_SIZE);
                    bytes[payload_off..payload_off + copy_len].copy_from_slice(&pre[..copy_len]);
                    // Clear xmax (bytes 8..16), cmax (20..24), and
                    // the UPDATED + UPDATED_IN_PLACE bits in
                    // infomask (24..26). Leave xmin / other
                    // header fields untouched.
                    bytes[offset + 8..offset + 16].copy_from_slice(&[0u8; 8]);
                    bytes[offset + 20..offset + 24].copy_from_slice(&[0u8; 4]);
                    let cur_im = u16::from_le_bytes([bytes[offset + 24], bytes[offset + 25]]);
                    let new_im = cur_im & !(InfoMask::UPDATED | InfoMask::UPDATED_IN_PLACE);
                    bytes[offset + 24..offset + 26].copy_from_slice(&new_im.to_le_bytes());
                    total_restored += 1;
                }
                drop(page);
                drop(guard);
                i = j;
            }

            for batch in compact_to_apply.iter().rev() {
                let guard = self.pool.get_page(batch.page)?;
                let mut page = guard.write();
                let bytes = page.as_bytes_mut();
                for slot in batch_slots(batch) {
                    let item_id_off = crate::page::PAGE_HEADER_SIZE
                        + usize::from(slot) * crate::page::ITEMID_SIZE;
                    let item_raw = u32::from_le_bytes([
                        bytes[item_id_off],
                        bytes[item_id_off + 1],
                        bytes[item_id_off + 2],
                        bytes[item_id_off + 3],
                    ]);
                    if item_raw & 0b11 != 1 {
                        continue;
                    }
                    let length = ((item_raw >> 2) & 0x7FFF) as usize;
                    let offset = ((item_raw >> 17) & 0x7FFF) as usize;
                    if length < TUPLE_HEADER_SIZE
                        || offset.checked_add(length).is_none_or(|e| e > bytes.len())
                    {
                        return Err(HeapError::MalformedHeader("slot shorter than header"));
                    }
                    let payload_off = offset + TUPLE_HEADER_SIZE;
                    if payload_off + 9 > offset + length {
                        return Err(HeapError::MalformedHeader(
                            "payload shorter than (Int32, Int32)",
                        ));
                    }
                    let target_off = if batch.target_col == 0 {
                        payload_off + 1
                    } else {
                        payload_off + 5
                    };
                    let current = i32::from_le_bytes(
                        bytes[target_off..target_off + 4]
                            .try_into()
                            .expect("4-byte slice"),
                    );
                    let restored = current.wrapping_sub(batch.delta);
                    bytes[target_off..target_off + 4].copy_from_slice(&restored.to_le_bytes());
                    bytes[offset + 8..offset + 16].copy_from_slice(&[0u8; 8]);
                    bytes[offset + 20..offset + 24].copy_from_slice(&[0u8; 4]);
                    let cur_im = u16::from_le_bytes([bytes[offset + 24], bytes[offset + 25]]);
                    let new_im = cur_im & !(InfoMask::UPDATED | InfoMask::UPDATED_IN_PLACE);
                    bytes[offset + 24..offset + 26].copy_from_slice(&new_im.to_le_bytes());
                    total_restored += 1;
                }
                drop(page);
                drop(guard);
            }
            if total_restored > restored_before {
                self.column_cache.bump_version(rel);
            }
        }
        Ok(total_restored)
    }

    /// **In-place** MVCC-correct UPDATE for the narrow
    /// `(Int32, Int32) SET col_i = col_i ± delta [WHERE col_j cmp lit]`
    /// shape.
    ///
    /// Architectural shift versus the classical out-of-place
    /// new-tuple-version path: every UPDATE writes the *new* payload
    /// directly into the existing slot's payload region (preserving
    /// the same `ctid`) and stamps the source header with
    /// `xmax / cmax / infomask | UPDATED | UPDATED_IN_PLACE`. The
    /// *old* payload is appended to the per-relation
    /// [`HeapAccess::undo_log`] keyed by `TupleId`, so a concurrent
    /// reader whose snapshot does not yet see this UPDATE as
    /// committed can recover the pre-image from the side log
    /// (handled in `Self::for_each_visible_with_undo`).
    ///
    /// What in-place wins versus the out-of-place plan:
    ///
    /// - Zero destination-page allocations and zero destination-page
    ///   writes (the prior plan grew the relation by ~65 fresh pages
    ///   on a 10 000-row bench UPDATE, each paying a `Page::new_heap`
    ///   zero-fill plus per-row header / payload / item-id writes).
    /// - Per-tuple write budget drops to ~22 bytes (8 B xmax + 4 B
    ///   cmax + 2 B infomask + 8 B payload) from ~70 bytes (40 B
    ///   header + 9 B payload + 4 B item-id at dest, plus 22 B stamp
    ///   at source).
    /// - The per-relation `block_counter` no longer grows on UPDATE;
    ///   sequential scans cover the same block range they did before.
    ///
    /// What in-place pays:
    ///
    /// - One `Vec::push` per qualifying tuple into a per-source-page
    ///   scratch undo buffer (~5 ns), and one bulk-append per source
    ///   page into the per-relation undo log under a single
    ///   `RwLock::write` (~50 ns + memcpy of ~9 bytes × tuples).
    ///
    /// # MVCC correctness
    ///
    /// Tuples updated in place carry the
    /// [`InfoMask::UPDATED_IN_PLACE`] bit on top of the existing
    /// `UPDATED` bit. Readers using `Self::for_each_visible_with_undo`
    /// (or the standard `is_visible`-driven scan paths once the
    /// visibility predicate is taught about `UPDATED_IN_PLACE`) check
    /// whether the writer's xmax is visible in their snapshot:
    /// - If yes, the slot's current bytes are the right payload.
    /// - If no, they consult the undo log for the pre-image.
    ///
    /// VACUUM is responsible for trimming undo entries whose
    /// `writer_xid` predates every live snapshot's `xmin`.
    ///
    /// # Concurrency
    ///
    /// Holds **one** write-exclusive page guard at a time — the source
    /// page being updated. No destination guard is acquired because
    /// no destination page exists.
    ///
    /// # Durability
    ///
    /// When `wal` is `Some`, the inner loop emits one
    /// page-batched in-place UPDATE record per touched page
    /// (carrying pre + post-image bytes for every slot) after the
    /// per-page write guard is dropped, and stamps the page LSN with
    /// the assigned LSN. A
    /// [`ultrasql_wal::RecordType::FullPageWrite`]
    /// record is emitted first when the page has not been touched
    /// since the previous checkpoint, mirroring the
    /// [`HeapAccess::update_many`] / [`HeapAccess::delete_many`]
    /// contract. Recovery rebuilds both the post-image and the
    /// in-memory `UndoRelationLog` entry through
    /// [`HeapTarget::apply_update_in_place`](ultrasql_wal::HeapTarget::apply_update_in_place).
    ///
    /// When `wal` is `None`, no record is emitted — the configuration
    /// used for unit tests and any future explicit `--no-wal` mode.
    /// The buffer pool decides which mode applies via its configured
    /// [`crate::wal_sink::WalSink`]; fused executor callers
    /// pull the sink from [`HeapAccess::wal_sink`].
    #[allow(clippy::too_many_arguments)]
    #[inline]
    pub fn update_int32_pair_inplace_undo<O, P>(
        &self,
        rel: RelationId,
        block_count: u32,
        snapshot: &Snapshot,
        oracle: &O,
        predicate: P,
        target_col: u8,
        delta: i32,
        xid: Xid,
        command_id: CommandId,
        wal: Option<&dyn WalSink>,
        vm: Option<&crate::vm::VisibilityMap>,
    ) -> Result<usize, HeapError>
    where
        O: XidStatusOracle + ?Sized,
        P: Fn(i32, i32) -> bool,
    {
        use crate::page::{ITEMID_SIZE, PAGE_HEADER_SIZE};

        let mut total_updated: usize = 0;
        let mut xmin_cache: Option<(Xid, u16, bool)> = None;

        // Local scratch buffers for the compact undo log. This path
        // changes one fixed-width int32 column by one literal delta,
        // so a page id + slot list + delta is enough to reconstruct
        // the pre-image for old snapshots.
        let mut compact_undo_scratch: Vec<Int32PairUndoBatch> =
            Vec::with_capacity(block_count as usize);
        let mut page_undo_slots: Vec<u16> = Vec::with_capacity(256);

        // When a WAL sink is wired, collect per-row `(slot,
        // pre_image, post_image)` triples *during* the page write
        // and emit them with the page write guard dropped. Holding
        // the per-frame `RwLock<Page>` write across WAL I/O would
        // pin the buffer-pool frame for the duration of an fsync.
        // Reusing one Vec across pages avoids allocator churn.
        let mut wal_scratch: Vec<(u16, [u8; 9], [u8; 9])> = if wal.is_some() {
            Vec::with_capacity(256)
        } else {
            Vec::new()
        };

        let xid_bytes = xid.raw().to_le_bytes();
        let cmd_bytes = command_id.raw().to_le_bytes();

        for src_block in 0..block_count {
            let src_page_id = PageId::new(rel, BlockNumber::new(src_block));
            let mut page_updated = false;

            // FPW: if the page has not been mutated since the last
            // checkpoint, emit a full-page-write record first so
            // recovery has the canonical image to apply per-row
            // post-images on top of. The FPW guard is on a shared
            // read lock; emission completes before we acquire the
            // exclusive write lock for the mutation.
            if let Some(sink) = wal {
                Self::maybe_emit_fpw(
                    &self.pool,
                    src_page_id,
                    sink,
                    &self.last_checkpoint_lsn,
                    xid,
                )?;
            }

            let src_guard = self.pool.get_page(src_page_id)?;
            let mut src_page = src_guard.write();
            let src_bytes = src_page.as_bytes_mut();
            let src_slot_count = {
                let hdr = crate::page::PageHeader::decode(src_bytes).map_err(HeapError::Page)?;
                hdr.slot_count()
            };

            for src_slot in 0..src_slot_count {
                // ItemId decode.
                let item_id_off = PAGE_HEADER_SIZE + usize::from(src_slot) * ITEMID_SIZE;
                let item_raw = u32::from_le_bytes([
                    src_bytes[item_id_off],
                    src_bytes[item_id_off + 1],
                    src_bytes[item_id_off + 2],
                    src_bytes[item_id_off + 3],
                ]);
                if item_raw & 0b11 != 1 {
                    continue;
                }
                let length = ((item_raw >> 2) & 0x7FFF) as usize;
                let offset = ((item_raw >> 17) & 0x7FFF) as usize;
                if length < TUPLE_HEADER_SIZE
                    || offset
                        .checked_add(length)
                        .is_none_or(|e| e > src_bytes.len())
                {
                    return Err(HeapError::MalformedHeader("slot shorter than header"));
                }

                // Minimal-decode visibility check.
                let xmin_raw = u64::from_le_bytes(
                    src_bytes[offset..offset + 8]
                        .try_into()
                        .expect("8-byte slice"),
                );
                let xmax_raw = u64::from_le_bytes(
                    src_bytes[offset + 8..offset + 16]
                        .try_into()
                        .expect("8-byte slice"),
                );
                let infomask_bits = u16::from_le_bytes(
                    src_bytes[offset + 24..offset + 26]
                        .try_into()
                        .expect("2-byte slice"),
                );
                let xmin_xid = Xid::new(xmin_raw);

                let visibility = if xmax_raw == 0 {
                    match xmin_cache {
                        Some((cxmin, cinfo, cv)) if cxmin == xmin_xid && cinfo == infomask_bits => {
                            if cv {
                                Visibility::Visible
                            } else {
                                Visibility::Invisible
                            }
                        }
                        _ => {
                            let (h, _) =
                                TupleHeader::decode(&src_bytes[offset..offset + TUPLE_HEADER_SIZE])
                                    .ok_or(HeapError::MalformedHeader("header decode failed"))?;
                            let v =
                                matches!(is_visible(&h, snapshot, oracle), Visibility::Visible,);
                            xmin_cache = Some((h.xmin, h.infomask.bits(), v));
                            if v {
                                Visibility::Visible
                            } else {
                                Visibility::Invisible
                            }
                        }
                    }
                } else {
                    let (h, _) =
                        TupleHeader::decode(&src_bytes[offset..offset + TUPLE_HEADER_SIZE])
                            .ok_or(HeapError::MalformedHeader("header decode failed"))?;
                    is_visible(&h, snapshot, oracle)
                };
                // Decode (id, val) from payload [null_byte, id_le, val_le].
                let payload_off = offset + TUPLE_HEADER_SIZE;
                if payload_off + 9 > offset + length {
                    return Err(HeapError::MalformedHeader(
                        "payload shorter than (Int32, Int32)",
                    ));
                }
                let pair = u64::from_le_bytes(
                    src_bytes[payload_off + 1..payload_off + 9]
                        .try_into()
                        .expect("8-byte payload"),
                );
                let id = i32::from_ne_bytes((pair as u32).to_ne_bytes());
                let val = i32::from_ne_bytes(((pair >> 32) as u32).to_ne_bytes());

                match visibility {
                    Visibility::Visible => {}
                    Visibility::VisiblePreImage => {
                        if predicate(id, val) {
                            return Err(HeapError::WriteConflict(
                                "in-place tuple has an unresolved writer",
                            ));
                        }
                        continue;
                    }
                    Visibility::Invisible | Visibility::DeletedByOwn => continue,
                }

                if !predicate(id, val) {
                    continue;
                }

                let (new_id, new_val) = if target_col == 0 {
                    (id.wrapping_add(delta), val)
                } else {
                    (id, val.wrapping_add(delta))
                };

                if wal.is_some() {
                    let mut pre_bytes = [0u8; 9];
                    pre_bytes.copy_from_slice(&src_bytes[payload_off..payload_off + 9]);
                    // Post bytes are filled in below before the loop tail.
                    wal_scratch.push((src_slot, pre_bytes, [0u8; 9]));
                }
                page_undo_slots.push(src_slot);

                // Stamp the source slot's header in place:
                //   bytes  8..16  xmax
                //   bytes 20..24  cmax
                //   bytes 24..26  infomask | UPDATED | UPDATED_IN_PLACE
                src_bytes[offset + 8..offset + 16].copy_from_slice(&xid_bytes);
                src_bytes[offset + 20..offset + 24].copy_from_slice(&cmd_bytes);
                let new_infomask = infomask_bits | InfoMask::UPDATED | InfoMask::UPDATED_IN_PLACE;
                src_bytes[offset + 24..offset + 26].copy_from_slice(&new_infomask.to_le_bytes());

                // Overwrite the payload with the new (id, val) — same
                // 8-byte region the prior values occupied. The
                // null-bitmap byte stays zero. Packed as one u64 store.
                let new_pair = ((u32::from_ne_bytes(new_val.to_ne_bytes()) as u64) << 32)
                    | u32::from_ne_bytes(new_id.to_ne_bytes()) as u64;
                src_bytes[payload_off + 1..payload_off + 9]
                    .copy_from_slice(&new_pair.to_le_bytes());

                if wal.is_some() {
                    let last = wal_scratch.last_mut().expect("just pushed");
                    last.2
                        .copy_from_slice(&src_bytes[payload_off..payload_off + 9]);
                }

                total_updated += 1;
                page_updated = true;
            }

            // Drop the source-page write guard before touching the
            // shared undo log; lock order is `page → undo`.
            drop(src_page);
            drop(src_guard);

            // Emit one WAL record for the applied rows on this page, with
            // the page guard dropped (no buffer-pool pin held during
            // WAL I/O). Stamp the page LSN with the page-batch
            // record's assigned LSN so recovery's redo-skip check
            // sees the page as covered by every entry in it.
            if let Some(sink) = wal {
                if !wal_scratch.is_empty() {
                    let lsn = Self::emit_update_in_place_batch_wal(
                        sink,
                        src_page_id,
                        xid,
                        command_id,
                        &wal_scratch,
                    )?;
                    Self::stamp_page_lsn(&self.pool, src_page_id, lsn)?;
                }
                wal_scratch.clear();
            }
            if let Some(&first_slot) = page_undo_slots.first() {
                let slot_count = u16::try_from(page_undo_slots.len())
                    .map_err(|_| HeapError::MalformedHeader("too many updated slots"))?;
                let last_slot = *page_undo_slots
                    .last()
                    .expect("first slot exists, so last slot exists");
                let contiguous =
                    usize::from(last_slot.saturating_sub(first_slot)) + 1 == page_undo_slots.len();
                let slots = if contiguous {
                    Vec::new()
                } else {
                    std::mem::take(&mut page_undo_slots)
                };
                compact_undo_scratch.push(Int32PairUndoBatch {
                    page: src_page_id,
                    writer_xid: xid,
                    target_col,
                    delta,
                    first_slot,
                    slot_count,
                    slots,
                });
                page_undo_slots.clear();
            }
            if page_updated && let Some(vm) = vm {
                vm.clear(src_page_id.relation, src_page_id.block);
            }

            // Defer undo append: keep accumulating compact per-page
            // batches and bulk-move once after the entire UPDATE
            // finishes. Saves one log write-lock per source page and
            // avoids per-row pre-image allocation.
        }

        // Single append of every compact pre-image batch into the
        // per-relation undo log under one write-lock acquire.
        if !compact_undo_scratch.is_empty() {
            let log_handle = self
                .undo_log
                .entry(rel)
                .or_insert_with(|| parking_lot::RwLock::new(UndoRelationLog::default()));
            let mut log = log_handle.write();
            if log.int32_pair_batches.is_empty() {
                log.int32_pair_batches.reserve(block_count as usize);
            }
            log.int32_pair_batches.append(&mut compact_undo_scratch);
        }

        if total_updated > 0 {
            self.column_cache.bump_version(rel);
        }

        Ok(total_updated)
    }

    /// Parallel no-WAL variant for large in-memory fused `(Int32, Int32)`
    /// UPDATEs.
    ///
    /// The WAL-backed path stays sequential so per-transaction WAL chain
    /// ordering remains unchanged. For the in-memory server mode used by the
    /// DB-vs-DB benchmark, source pages are independent: each worker owns a
    /// disjoint block range, records compact undo locally, and the caller
    /// appends all undo batches under one relation-log lock after workers
    /// finish.
    #[allow(clippy::too_many_arguments)]
    pub fn update_int32_pair_inplace_undo_parallel_no_wal<O, P>(
        &self,
        rel: RelationId,
        block_count: u32,
        snapshot: &Snapshot,
        oracle: &O,
        predicate: P,
        target_col: u8,
        delta: i32,
        xid: Xid,
        command_id: CommandId,
        vm: Option<&crate::vm::VisibilityMap>,
    ) -> Result<usize, HeapError>
    where
        O: XidStatusOracle + Sync + ?Sized,
        P: Fn(i32, i32) -> bool + Sync,
    {
        let available_workers = std::thread::available_parallelism().map_or(1, |n| n.get());
        if block_count < 2_048 || available_workers <= 1 {
            return self.update_int32_pair_inplace_undo(
                rel,
                block_count,
                snapshot,
                oracle,
                predicate,
                target_col,
                delta,
                xid,
                command_id,
                None,
                vm,
            );
        }

        let block_count_usize = block_count as usize;
        let workers = available_workers
            .min(block_count_usize.div_ceil(512))
            .min(block_count_usize)
            .max(1);
        if workers <= 1 {
            return self.update_int32_pair_inplace_undo(
                rel,
                block_count,
                snapshot,
                oracle,
                predicate,
                target_col,
                delta,
                xid,
                command_id,
                None,
                vm,
            );
        }

        let workers_u32 =
            u32::try_from(workers).map_err(|_| HeapError::MalformedHeader("worker overflow"))?;
        let chunk_blocks = block_count.div_ceil(workers_u32).max(1);
        let predicate_ref = &predicate;
        let mut updates = Vec::with_capacity(workers);

        std::thread::scope(|scope| {
            let mut handles = Vec::with_capacity(workers);
            let mut start_block = 0_u32;
            while start_block < block_count {
                let end_block = start_block.saturating_add(chunk_blocks).min(block_count);
                handles.push(scope.spawn(move || {
                    self.update_int32_pair_range_no_wal(
                        rel,
                        start_block,
                        end_block,
                        snapshot,
                        oracle,
                        predicate_ref,
                        target_col,
                        delta,
                        xid,
                        command_id,
                        vm,
                    )
                }));
                start_block = end_block;
            }

            for handle in handles {
                let update = handle
                    .join()
                    .map_err(|_| HeapError::MalformedHeader("parallel update worker panicked"))??;
                updates.push(update);
            }
            Ok::<(), HeapError>(())
        })?;

        let total_updated = updates.iter().fold(0_usize, |total, update| {
            total.saturating_add(update.total_updated)
        });
        let mut compact_undo_scratch = Vec::with_capacity(block_count as usize);
        for mut update in updates {
            compact_undo_scratch.append(&mut update.compact_undo);
        }

        if !compact_undo_scratch.is_empty() {
            let log_handle = self
                .undo_log
                .entry(rel)
                .or_insert_with(|| parking_lot::RwLock::new(UndoRelationLog::default()));
            let mut log = log_handle.write();
            if log.int32_pair_batches.is_empty() {
                log.int32_pair_batches.reserve(block_count as usize);
            }
            log.int32_pair_batches.append(&mut compact_undo_scratch);
        }

        if total_updated > 0 {
            self.column_cache.bump_version(rel);
        }

        Ok(total_updated)
    }

    #[allow(clippy::too_many_arguments)]
    fn update_int32_pair_range_no_wal<O, P>(
        &self,
        rel: RelationId,
        start_block: u32,
        end_block: u32,
        snapshot: &Snapshot,
        oracle: &O,
        predicate: &P,
        target_col: u8,
        delta: i32,
        xid: Xid,
        command_id: CommandId,
        vm: Option<&crate::vm::VisibilityMap>,
    ) -> Result<Int32PairRangeUpdate, HeapError>
    where
        O: XidStatusOracle + ?Sized,
        P: Fn(i32, i32) -> bool,
    {
        use crate::page::{ITEMID_SIZE, PAGE_HEADER_SIZE};

        let range_len = end_block.saturating_sub(start_block) as usize;
        let mut total_updated: usize = 0;
        let mut xmin_cache: Option<(Xid, u16, bool)> = None;
        let mut compact_undo_scratch: Vec<Int32PairUndoBatch> = Vec::with_capacity(range_len);
        let mut page_undo_slots: Vec<u16> = Vec::with_capacity(256);
        let xid_bytes = xid.raw().to_le_bytes();
        let cmd_bytes = command_id.raw().to_le_bytes();

        for src_block in start_block..end_block {
            let src_page_id = PageId::new(rel, BlockNumber::new(src_block));
            let mut page_updated = false;

            let src_guard = self.pool.get_page(src_page_id)?;
            let mut src_page = src_guard.write();
            let src_bytes = src_page.as_bytes_mut();
            let src_slot_count = {
                let hdr = crate::page::PageHeader::decode(src_bytes).map_err(HeapError::Page)?;
                hdr.slot_count()
            };

            for src_slot in 0..src_slot_count {
                let item_id_off = PAGE_HEADER_SIZE + usize::from(src_slot) * ITEMID_SIZE;
                let item_raw = u32::from_le_bytes([
                    src_bytes[item_id_off],
                    src_bytes[item_id_off + 1],
                    src_bytes[item_id_off + 2],
                    src_bytes[item_id_off + 3],
                ]);
                if item_raw & 0b11 != 1 {
                    continue;
                }
                let length = ((item_raw >> 2) & 0x7FFF) as usize;
                let offset = ((item_raw >> 17) & 0x7FFF) as usize;
                if length < TUPLE_HEADER_SIZE
                    || offset
                        .checked_add(length)
                        .is_none_or(|e| e > src_bytes.len())
                {
                    return Err(HeapError::MalformedHeader("slot shorter than header"));
                }

                let xmin_raw = u64::from_le_bytes(
                    src_bytes[offset..offset + 8]
                        .try_into()
                        .expect("8-byte slice"),
                );
                let xmax_raw = u64::from_le_bytes(
                    src_bytes[offset + 8..offset + 16]
                        .try_into()
                        .expect("8-byte slice"),
                );
                let infomask_bits = u16::from_le_bytes(
                    src_bytes[offset + 24..offset + 26]
                        .try_into()
                        .expect("2-byte slice"),
                );
                let xmin_xid = Xid::new(xmin_raw);

                let visibility = if xmax_raw == 0 {
                    match xmin_cache {
                        Some((cxmin, cinfo, cv)) if cxmin == xmin_xid && cinfo == infomask_bits => {
                            if cv {
                                Visibility::Visible
                            } else {
                                Visibility::Invisible
                            }
                        }
                        _ => {
                            let (h, _) =
                                TupleHeader::decode(&src_bytes[offset..offset + TUPLE_HEADER_SIZE])
                                    .ok_or(HeapError::MalformedHeader("header decode failed"))?;
                            let v = matches!(is_visible(&h, snapshot, oracle), Visibility::Visible);
                            xmin_cache = Some((h.xmin, h.infomask.bits(), v));
                            if v {
                                Visibility::Visible
                            } else {
                                Visibility::Invisible
                            }
                        }
                    }
                } else {
                    let (h, _) =
                        TupleHeader::decode(&src_bytes[offset..offset + TUPLE_HEADER_SIZE])
                            .ok_or(HeapError::MalformedHeader("header decode failed"))?;
                    is_visible(&h, snapshot, oracle)
                };

                let payload_off = offset + TUPLE_HEADER_SIZE;
                if payload_off + 9 > offset + length {
                    return Err(HeapError::MalformedHeader(
                        "payload shorter than (Int32, Int32)",
                    ));
                }
                let pair = u64::from_le_bytes(
                    src_bytes[payload_off + 1..payload_off + 9]
                        .try_into()
                        .expect("8-byte payload"),
                );
                let id = i32::from_ne_bytes((pair as u32).to_ne_bytes());
                let val = i32::from_ne_bytes(((pair >> 32) as u32).to_ne_bytes());

                match visibility {
                    Visibility::Visible => {}
                    Visibility::VisiblePreImage => {
                        if predicate(id, val) {
                            return Err(HeapError::WriteConflict(
                                "in-place tuple has an unresolved writer",
                            ));
                        }
                        continue;
                    }
                    Visibility::Invisible | Visibility::DeletedByOwn => continue,
                }

                if !predicate(id, val) {
                    continue;
                }

                let (new_id, new_val) = if target_col == 0 {
                    (id.wrapping_add(delta), val)
                } else {
                    (id, val.wrapping_add(delta))
                };

                page_undo_slots.push(src_slot);
                src_bytes[offset + 8..offset + 16].copy_from_slice(&xid_bytes);
                src_bytes[offset + 20..offset + 24].copy_from_slice(&cmd_bytes);
                let new_infomask = infomask_bits | InfoMask::UPDATED | InfoMask::UPDATED_IN_PLACE;
                src_bytes[offset + 24..offset + 26].copy_from_slice(&new_infomask.to_le_bytes());

                let new_pair = ((u32::from_ne_bytes(new_val.to_ne_bytes()) as u64) << 32)
                    | u32::from_ne_bytes(new_id.to_ne_bytes()) as u64;
                src_bytes[payload_off + 1..payload_off + 9]
                    .copy_from_slice(&new_pair.to_le_bytes());

                total_updated += 1;
                page_updated = true;
            }

            drop(src_page);
            drop(src_guard);

            if let Some(&first_slot) = page_undo_slots.first() {
                let slot_count = u16::try_from(page_undo_slots.len())
                    .map_err(|_| HeapError::MalformedHeader("too many updated slots"))?;
                let last_slot = *page_undo_slots
                    .last()
                    .expect("first slot exists, so last slot exists");
                let contiguous =
                    usize::from(last_slot.saturating_sub(first_slot)) + 1 == page_undo_slots.len();
                let slots = if contiguous {
                    Vec::new()
                } else {
                    std::mem::take(&mut page_undo_slots)
                };
                compact_undo_scratch.push(Int32PairUndoBatch {
                    page: src_page_id,
                    writer_xid: xid,
                    target_col,
                    delta,
                    first_slot,
                    slot_count,
                    slots,
                });
                page_undo_slots.clear();
            }
            if page_updated && let Some(vm) = vm {
                vm.clear(src_page_id.relation, src_page_id.block);
            }
        }

        Ok(Int32PairRangeUpdate {
            total_updated,
            compact_undo: compact_undo_scratch,
        })
    }

    /// Point form of [`Self::update_int32_pair_inplace_undo`].
    ///
    /// The caller already found candidate TIDs through a secondary
    /// index. This method rechecks MVCC visibility and the predicate
    /// against the heap slot before mutating, so stale or invisible
    /// index entries remain correctness-neutral.
    #[allow(clippy::too_many_arguments)]
    pub fn update_int32_pair_tid_inplace_undo<O, P>(
        &self,
        tid: TupleId,
        snapshot: &Snapshot,
        oracle: &O,
        predicate: P,
        target_col: u8,
        delta: i32,
        xid: Xid,
        command_id: CommandId,
        wal: Option<&dyn WalSink>,
        vm: Option<&crate::vm::VisibilityMap>,
    ) -> Result<usize, HeapError>
    where
        O: XidStatusOracle + ?Sized,
        P: Fn(i32, i32) -> bool,
    {
        use crate::page::{ITEMID_SIZE, PAGE_HEADER_SIZE};

        let rel = tid.page.relation;
        let xid_bytes = xid.raw().to_le_bytes();
        let cmd_bytes = command_id.raw().to_le_bytes();

        if let Some(sink) = wal {
            Self::maybe_emit_fpw(&self.pool, tid.page, sink, &self.last_checkpoint_lsn, xid)?;
        }

        let mut pre_image = [0_u8; 9];
        let mut post_image = [0_u8; 9];

        {
            let guard = self.pool.get_page(tid.page)?;
            let mut page = guard.write();
            let bytes = page.as_bytes_mut();
            let slot_count = {
                let hdr = crate::page::PageHeader::decode(bytes).map_err(HeapError::Page)?;
                hdr.slot_count()
            };
            if tid.slot >= slot_count {
                return Ok(0);
            }

            let item_id_off = PAGE_HEADER_SIZE + usize::from(tid.slot) * ITEMID_SIZE;
            let item_raw = u32::from_le_bytes([
                bytes[item_id_off],
                bytes[item_id_off + 1],
                bytes[item_id_off + 2],
                bytes[item_id_off + 3],
            ]);
            if item_raw & 0b11 != 1 {
                return Ok(0);
            }
            let length = ((item_raw >> 2) & 0x7FFF) as usize;
            let offset = ((item_raw >> 17) & 0x7FFF) as usize;
            if length < TUPLE_HEADER_SIZE
                || offset.checked_add(length).is_none_or(|e| e > bytes.len())
            {
                return Err(HeapError::MalformedHeader("slot shorter than header"));
            }

            let (header, _) = TupleHeader::decode(&bytes[offset..offset + TUPLE_HEADER_SIZE])
                .ok_or(HeapError::MalformedHeader("header decode failed"))?;
            let payload_off = offset + TUPLE_HEADER_SIZE;
            if payload_off + 9 > offset + length {
                return Err(HeapError::MalformedHeader(
                    "payload shorter than (Int32, Int32)",
                ));
            }

            let id = i32::from_le_bytes([
                bytes[payload_off + 1],
                bytes[payload_off + 2],
                bytes[payload_off + 3],
                bytes[payload_off + 4],
            ]);
            let val = i32::from_le_bytes([
                bytes[payload_off + 5],
                bytes[payload_off + 6],
                bytes[payload_off + 7],
                bytes[payload_off + 8],
            ]);

            match is_visible(&header, snapshot, oracle) {
                Visibility::Visible => {}
                Visibility::VisiblePreImage => {
                    if predicate(id, val) {
                        return Err(HeapError::WriteConflict(
                            "in-place tuple has an unresolved writer",
                        ));
                    }
                    return Ok(0);
                }
                Visibility::Invisible | Visibility::DeletedByOwn => return Ok(0),
            }

            if !predicate(id, val) {
                return Ok(0);
            }

            let (new_id, new_val) = if target_col == 0 {
                (id.wrapping_add(delta), val)
            } else {
                (id, val.wrapping_add(delta))
            };

            pre_image.copy_from_slice(&bytes[payload_off..payload_off + 9]);
            bytes[offset + 8..offset + 16].copy_from_slice(&xid_bytes);
            bytes[offset + 20..offset + 24].copy_from_slice(&cmd_bytes);
            let new_infomask =
                header.infomask.bits() | InfoMask::UPDATED | InfoMask::UPDATED_IN_PLACE;
            bytes[offset + 24..offset + 26].copy_from_slice(&new_infomask.to_le_bytes());

            let payload_u64 = ((u32::from_ne_bytes(new_val.to_ne_bytes()) as u64) << 32)
                | u32::from_ne_bytes(new_id.to_ne_bytes()) as u64;
            bytes[payload_off + 1..payload_off + 9].copy_from_slice(&payload_u64.to_le_bytes());
            post_image.copy_from_slice(&bytes[payload_off..payload_off + 9]);
        }

        if let Some(sink) = wal {
            let lsn = Self::emit_update_in_place_wal(
                sink,
                tid,
                xid,
                command_id,
                &pre_image,
                &post_image,
            )?;
            Self::stamp_page_lsn(&self.pool, tid.page, lsn)?;
        }
        if let Some(vm) = vm {
            vm.clear(tid.page.relation, tid.page.block);
        }

        let log_handle = self
            .undo_log
            .entry(rel)
            .or_insert_with(|| parking_lot::RwLock::new(UndoRelationLog::default()));
        let mut log = log_handle.write();
        log.entries.push(UndoEntry {
            tid,
            writer_xid: xid,
            old_payload: pre_image,
        });
        drop(log);

        self.column_cache.bump_version(rel);
        Ok(1)
    }
}

fn batch_slots(batch: &Int32PairUndoBatch) -> Vec<u16> {
    if !batch.slots.is_empty() {
        return batch.slots.clone();
    }
    (0..batch.slot_count)
        .map(|offset| batch.first_slot.saturating_add(offset))
        .collect()
}
