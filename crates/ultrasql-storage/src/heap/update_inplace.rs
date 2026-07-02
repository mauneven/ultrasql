//! See `crate::heap` for the public API.
//!
//! Part of the `heap` module split — each `impl<L: PageLoader>
//! HeapAccess<L>` block here adds methods to the type defined in
//! `heap/mod.rs`. Splitting across files keeps each unit under the
//! 600-line ceiling without changing semantics.

use ultrasql_core::{BlockNumber, CommandId, PageId, RelationId, TupleId, Xid};
use ultrasql_mvcc::tuple_header::{InfoMask, TUPLE_HEADER_SIZE};
use ultrasql_mvcc::{Snapshot, TupleHeader, Visibility, XidStatusOracle, is_visible};

use crate::buffer_pool::PageLoader;
use crate::wal_sink::WalSink;

use super::{
    HeapAccess, HeapError, Int32PairUndoBatch, UndoEntry, UndoRelationLog, checked_heap_count_add,
};

struct Int32PairRangeUpdate {
    total_updated: usize,
    compact_undo: Vec<Int32PairUndoBatch>,
}

struct UpdateInt32PairRange<'a, O: ?Sized, P: ?Sized> {
    rel: RelationId,
    start_block: u32,
    end_block: u32,
    snapshot: &'a Snapshot,
    oracle: &'a O,
    predicate: &'a P,
    target_col: u8,
    delta: i32,
    xid: Xid,
    command_id: CommandId,
    vm: Option<&'a crate::vm::VisibilityMap>,
}

/// Page-major scan request for fused `(Int32, Int32)` UPDATE.
///
/// The predicate receives decoded `(id, value)` payload values after
/// MVCC visibility checks pass for the supplied snapshot and oracle.
pub struct UpdateInt32PairScan<'a, O: ?Sized, P> {
    /// Relation to scan.
    pub rel: RelationId,
    /// Number of blocks to visit in `rel`.
    pub block_count: u32,
    /// MVCC snapshot used for tuple visibility.
    pub snapshot: &'a Snapshot,
    /// Commit-status oracle backing visibility checks.
    pub oracle: &'a O,
    /// Predicate over decoded `(Int32, Int32)` payload values.
    pub predicate: P,
}

impl<O: ?Sized, P> std::fmt::Debug for UpdateInt32PairScan<'_, O, P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UpdateInt32PairScan")
            .field("rel", &self.rel)
            .field("block_count", &self.block_count)
            .finish_non_exhaustive()
    }
}

/// Point-update request for a candidate `(Int32, Int32)` tuple.
///
/// The heap rechecks visibility and the predicate before writing, so
/// stale secondary-index candidates are skipped safely.
pub struct UpdateInt32PairTid<'a, O: ?Sized, P> {
    /// Candidate tuple id to recheck and update.
    pub tid: TupleId,
    /// MVCC snapshot used for tuple visibility.
    pub snapshot: &'a Snapshot,
    /// Commit-status oracle backing visibility checks.
    pub oracle: &'a O,
    /// Predicate over decoded `(Int32, Int32)` payload values.
    pub predicate: P,
}

impl<O: ?Sized, P> std::fmt::Debug for UpdateInt32PairTid<'_, O, P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UpdateInt32PairTid")
            .field("tid", &self.tid)
            .finish_non_exhaustive()
    }
}

/// Arithmetic edit applied by fused `(Int32, Int32)` UPDATE helpers.
#[derive(Clone, Copy, Debug)]
pub struct UpdateInt32PairEdit {
    /// Target column: `0` for id, `1` for value.
    pub target_col: u8,
    /// Signed delta added to the target column.
    pub delta: i32,
}

/// MVCC stamp written by fused in-place UPDATE helpers.
#[derive(Clone, Copy, Debug)]
pub struct UpdateInt32PairStamp {
    /// XID stamped as `xmax` on updated tuple versions.
    pub xid: Xid,
    /// Command id stamped as `cmax` on updated tuple versions.
    pub command_id: CommandId,
}

#[derive(Debug)]
struct PageUndoSlots {
    first_slot: u16,
    last_slot: u16,
    slot_count: u16,
    contiguous: bool,
    slots: Vec<u16>,
}

#[derive(Clone, Copy, Debug)]
struct UpdateInt32PairMutation {
    offset: usize,
    payload_off: usize,
    infomask_bits: u16,
    new_pair: u64,
}

impl PageUndoSlots {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            first_slot: 0,
            last_slot: 0,
            slot_count: 0,
            contiguous: true,
            slots: Vec::with_capacity(capacity),
        }
    }

    #[inline]
    fn push(&mut self, slot: u16) -> Result<(), HeapError> {
        if self.slot_count == 0 {
            self.first_slot = slot;
            self.last_slot = slot;
            self.slot_count = 1;
            self.contiguous = true;
            return Ok(());
        }

        if self.contiguous && slot == self.last_slot.saturating_add(1) {
            self.last_slot = slot;
            self.slot_count = self
                .slot_count
                .checked_add(1)
                .ok_or(HeapError::MalformedHeader("too many updated slots"))?;
            return Ok(());
        }

        if self.contiguous {
            self.slots.extend(self.first_slot..=self.last_slot);
            self.contiguous = false;
        }
        self.slots.push(slot);
        self.last_slot = slot;
        self.slot_count = self
            .slot_count
            .checked_add(1)
            .ok_or(HeapError::MalformedHeader("too many updated slots"))?;
        Ok(())
    }

    #[inline]
    fn take_batch(
        &mut self,
        page: PageId,
        writer_xid: Xid,
        command_id: CommandId,
        target_col: u8,
        delta: i32,
    ) -> Option<Int32PairUndoBatch> {
        if self.slot_count == 0 {
            return None;
        }
        let slots = if self.contiguous {
            Vec::new()
        } else {
            std::mem::take(&mut self.slots)
        };
        let batch = Int32PairUndoBatch {
            page,
            writer_xid,
            command_id,
            target_col,
            delta,
            first_slot: self.first_slot,
            slot_count: self.slot_count,
            slots,
        };
        self.first_slot = 0;
        self.last_slot = 0;
        self.slot_count = 0;
        self.contiguous = true;
        Some(batch)
    }
}

#[inline]
fn read_le_u16(bytes: &[u8], start: usize, error: &'static str) -> Result<u16, HeapError> {
    let end = start
        .checked_add(2)
        .ok_or(HeapError::MalformedHeader(error))?;
    if end > bytes.len() {
        return Err(HeapError::MalformedHeader(error));
    }
    // SAFETY: The range check above proves two bytes are readable from
    // `start`. Heap tuple fields are byte-aligned, so use `read_unaligned`.
    let word = unsafe { bytes.as_ptr().add(start).cast::<u16>().read_unaligned() };
    Ok(u16::from_le(word))
}

#[inline]
fn read_le_u32(bytes: &[u8], start: usize, error: &'static str) -> Result<u32, HeapError> {
    let end = start
        .checked_add(4)
        .ok_or(HeapError::MalformedHeader(error))?;
    if end > bytes.len() {
        return Err(HeapError::MalformedHeader(error));
    }
    // SAFETY: The range check above proves four bytes are readable from
    // `start`. Heap tuple fields are byte-aligned, so use `read_unaligned`.
    let word = unsafe { bytes.as_ptr().add(start).cast::<u32>().read_unaligned() };
    Ok(u32::from_le(word))
}

#[inline]
fn read_le_u64(bytes: &[u8], start: usize, error: &'static str) -> Result<u64, HeapError> {
    let end = start
        .checked_add(8)
        .ok_or(HeapError::MalformedHeader(error))?;
    if end > bytes.len() {
        return Err(HeapError::MalformedHeader(error));
    }
    // SAFETY: The range check above proves eight bytes are readable from
    // `start`. Heap tuple fields are byte-aligned, so use `read_unaligned`.
    let word = unsafe { bytes.as_ptr().add(start).cast::<u64>().read_unaligned() };
    Ok(u64::from_le(word))
}

#[inline]
fn read_le_i32(bytes: &[u8], start: usize, error: &'static str) -> Result<i32, HeapError> {
    Ok(i32::from_le_bytes(
        read_le_u32(bytes, start, error)?.to_le_bytes(),
    ))
}

#[inline]
fn itemid_window(item_raw: u32) -> Result<(usize, usize), HeapError> {
    let length = u16::try_from((item_raw >> 2) & 0x7FFF)
        .map_err(|_| HeapError::MalformedHeader("item length overflow"))?;
    let offset = u16::try_from((item_raw >> 17) & 0x7FFF)
        .map_err(|_| HeapError::MalformedHeader("item offset overflow"))?;
    Ok((usize::from(length), usize::from(offset)))
}

#[inline]
fn usize_from_u32(value: u32, error: &'static str) -> Result<usize, HeapError> {
    usize::try_from(value).map_err(|_| HeapError::MalformedHeader(error))
}

#[inline]
fn decode_int32_pair(pair: u64) -> (i32, i32) {
    let bytes = pair.to_le_bytes();
    (
        i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
        i32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]),
    )
}

#[inline]
fn encode_int32_pair(id: i32, val: i32) -> u64 {
    let id_bits = u32::from_ne_bytes(id.to_ne_bytes());
    let val_bits = u32::from_ne_bytes(val.to_ne_bytes());
    (u64::from(val_bits) << 32) | u64::from(id_bits)
}

impl<L: PageLoader> HeapAccess<L> {
    /// Roll back every in-place UPDATE performed by `xid` by
    /// restoring the slot's pre-image from the undo log and clearing
    /// the `xmax / cmax / UPDATED / UPDATED_IN_PLACE` header bits.
    /// Also clears DELETE stamps written by the same aborted xid so a
    /// later statement can update the restored row.
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
            if log.is_empty() {
                continue;
            }
            // Partition: remove everything written by `xid` (returned for
            // page-guard application below), keep other writers' records.
            let (to_apply, compact_to_apply) = log.take_written_by(xid);
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
                let guard = self.get_page_relieved(page_id)?;
                let mut page = guard.write();
                let bytes = page.as_bytes_mut();
                for entry in &to_apply[i..j] {
                    // Locate the slot via item-id.
                    let item_id_off = crate::page::PAGE_HEADER_SIZE
                        + usize::from(entry.tid.slot) * crate::page::ITEMID_SIZE;
                    let item_raw = read_le_u32(bytes, item_id_off, "item id out of bounds")?;
                    if item_raw & 0b11 != 1 {
                        continue;
                    }
                    let (length, offset) = itemid_window(item_raw)?;
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
                let guard = self.get_page_relieved(batch.page)?;
                let mut page = guard.write();
                let bytes = page.as_bytes_mut();
                for slot in batch_slots(batch) {
                    let item_id_off = crate::page::PAGE_HEADER_SIZE
                        + usize::from(slot) * crate::page::ITEMID_SIZE;
                    let item_raw = read_le_u32(bytes, item_id_off, "item id out of bounds")?;
                    if item_raw & 0b11 != 1 {
                        continue;
                    }
                    let (length, offset) = itemid_window(item_raw)?;
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
                    let current = read_le_i32(bytes, target_off, "target int32 out of bounds")?;
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
                self.invalidate_int32_pair_payload_stats_relation(rel);
                self.column_cache.bump_version(rel, xid);
            }
        }
        total_restored += self.rollback_delete_stamps(xid)?;
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
    #[inline]
    pub fn update_int32_pair_inplace_undo<O, P>(
        &self,
        scan: UpdateInt32PairScan<'_, O, P>,
        edit: UpdateInt32PairEdit,
        stamp: UpdateInt32PairStamp,
        wal: Option<&dyn WalSink>,
        vm: Option<&crate::vm::VisibilityMap>,
    ) -> Result<usize, HeapError>
    where
        O: XidStatusOracle + ?Sized,
        P: Fn(i32, i32) -> bool,
    {
        use crate::page::{ITEMID_SIZE, PAGE_HEADER_SIZE};

        let UpdateInt32PairScan {
            rel,
            block_count,
            snapshot,
            oracle,
            predicate,
        } = scan;
        let UpdateInt32PairEdit { target_col, delta } = edit;
        let UpdateInt32PairStamp { xid, command_id } = stamp;
        let mut total_updated: usize = 0;
        let mut xmin_cache: Option<(Xid, u16, bool)> = None;
        let vm = vm.filter(|vm| vm.contains_relation(rel));

        // Local scratch buffers for the compact undo log. This path
        // changes one fixed-width int32 column by one literal delta,
        // so a page id + slot list + delta is enough to reconstruct
        // the pre-image for old snapshots.
        let mut compact_undo_scratch: Vec<Int32PairUndoBatch> =
            Vec::with_capacity(usize_from_u32(block_count, "block count overflow")?);
        let mut page_undo_slots = PageUndoSlots::with_capacity(256);

        // When a WAL sink is wired, collect page-local slots. Every row on
        // this fused path applies the same `target_col += delta` edit, so the
        // WAL record stores that delta once instead of pre/post images for
        // every row. Reusing one Vec across pages avoids allocator churn.
        let mut wal_scratch: Vec<u16> = if wal.is_some() {
            Vec::with_capacity(256)
        } else {
            Vec::new()
        };
        let mut wal_payload_buf: Vec<u8> = if wal.is_some() {
            Vec::with_capacity(512)
        } else {
            Vec::new()
        };
        let wal_before_page_mutation =
            matches!(wal, Some(sink) if sink.appends_without_blocking_io());
        let mut update_prev_lsn = match wal {
            Some(sink)
                if wal_before_page_mutation
                    && self
                        .last_checkpoint_lsn
                        .load(std::sync::atomic::Ordering::Acquire)
                        == 0 =>
            {
                Some(sink.last_lsn_for(xid))
            }
            _ => None,
        };
        let mut page_mutations: Vec<UpdateInt32PairMutation> =
            Vec::with_capacity(if wal_before_page_mutation { 256 } else { 0 });
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

            let src_guard = self.get_page_relieved(src_page_id)?;
            let mut src_page = src_guard.write();
            let src_bytes = src_page.as_bytes_mut();
            let src_slot_count = {
                let hdr = crate::page::PageHeader::decode(src_bytes).map_err(HeapError::Page)?;
                hdr.slot_count()
            };

            for src_slot in 0..src_slot_count {
                // ItemId decode.
                let item_id_off = PAGE_HEADER_SIZE + usize::from(src_slot) * ITEMID_SIZE;
                let item_raw = read_le_u32(src_bytes, item_id_off, "item id out of bounds")?;
                if item_raw & 0b11 != 1 {
                    continue;
                }
                let (length, offset) = itemid_window(item_raw)?;
                if length < TUPLE_HEADER_SIZE
                    || offset
                        .checked_add(length)
                        .is_none_or(|e| e > src_bytes.len())
                {
                    return Err(HeapError::MalformedHeader("slot shorter than header"));
                }

                // Minimal-decode visibility check.
                let xmin_raw = read_le_u64(src_bytes, offset, "xmin out of bounds")?;
                let xmax_raw = read_le_u64(src_bytes, offset + 8, "xmax out of bounds")?;
                let infomask_bits = read_le_u16(src_bytes, offset + 24, "infomask out of bounds")?;
                let xmin_xid = Xid::new(xmin_raw);

                let visibility = if xmax_raw == 0 && infomask_bits & InfoMask::INPLACE_HISTORY == 0
                {
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
                let pair = read_le_u64(
                    src_bytes,
                    payload_off + 1,
                    "int32 pair payload out of bounds",
                )?;
                let (id, val) = decode_int32_pair(pair);

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
                    Visibility::VisibleMaybePreImage => {
                        // Visible with in-place undo history. When every
                        // recorded writer is visible to this snapshot the
                        // slot bytes are current and the row mutates like
                        // any visible row; otherwise acting on them would
                        // use a payload this snapshot must not observe —
                        // raise the same retryable conflict as a pending
                        // in-place update.
                        let tid = TupleId::new(src_page_id, src_slot);
                        if !self.undo_slot_state_current(
                            rel,
                            tid,
                            &src_bytes[payload_off..payload_off + 9],
                            snapshot,
                            oracle,
                        ) {
                            if predicate(id, val) {
                                return Err(HeapError::WriteConflict(
                                    "in-place tuple has an unresolved writer",
                                ));
                            }
                            continue;
                        }
                    }
                    Visibility::Invisible | Visibility::DeletedByOwn => continue,
                }

                if !predicate(id, val) {
                    continue;
                }

                let (new_id, new_val) = checked_int32_pair_add(id, val, target_col, delta)?;

                if wal.is_some() {
                    wal_scratch.push(src_slot);
                }
                page_undo_slots.push(src_slot)?;

                let new_pair = encode_int32_pair(new_id, new_val);
                if wal_before_page_mutation {
                    page_mutations.push(UpdateInt32PairMutation {
                        offset,
                        payload_off,
                        infomask_bits,
                        new_pair,
                    });
                } else {
                    // Stamp the source slot's header in place:
                    //   bytes  8..16  xmax
                    //   bytes 20..24  cmax
                    //   bytes 24..26  infomask | UPDATED | UPDATED_IN_PLACE
                    src_bytes[offset + 8..offset + 16].copy_from_slice(&xid_bytes);
                    src_bytes[offset + 20..offset + 24].copy_from_slice(&cmd_bytes);
                    let new_infomask =
                        infomask_bits | InfoMask::UPDATED | InfoMask::UPDATED_IN_PLACE;
                    src_bytes[offset + 24..offset + 26]
                        .copy_from_slice(&new_infomask.to_le_bytes());

                    // Overwrite the payload with the new (id, val) — same
                    // 8-byte region the prior values occupied. The
                    // null-bitmap byte stays zero. Packed as one u64 store.
                    src_bytes[payload_off + 1..payload_off + 9]
                        .copy_from_slice(&new_pair.to_le_bytes());
                }

                total_updated += 1;
                page_updated = true;
            }

            let mut guard_appended_lsn = None;
            if let Some(sink) = wal
                && wal_before_page_mutation
                && !wal_scratch.is_empty()
            {
                let prev_lsn = update_prev_lsn.unwrap_or_else(|| sink.last_lsn_for(xid));
                let lsn = Self::emit_update_int32_pair_delta_batch_wal_before_reuse(
                    sink,
                    src_page_id,
                    xid,
                    command_id,
                    target_col,
                    delta,
                    &wal_scratch,
                    prev_lsn,
                    &mut wal_payload_buf,
                )?;
                if update_prev_lsn.is_some() {
                    update_prev_lsn = Some(lsn);
                }
                guard_appended_lsn = Some(lsn);
            }

            if !page_mutations.is_empty() {
                let src_bytes = src_page.as_bytes_mut();
                for mutation in &page_mutations {
                    let offset = mutation.offset;
                    src_bytes[offset + 8..offset + 16].copy_from_slice(&xid_bytes);
                    src_bytes[offset + 20..offset + 24].copy_from_slice(&cmd_bytes);
                    let new_infomask =
                        mutation.infomask_bits | InfoMask::UPDATED | InfoMask::UPDATED_IN_PLACE;
                    src_bytes[offset + 24..offset + 26]
                        .copy_from_slice(&new_infomask.to_le_bytes());
                    src_bytes[mutation.payload_off + 1..mutation.payload_off + 9]
                        .copy_from_slice(&mutation.new_pair.to_le_bytes());
                }
                if let Some(lsn) = guard_appended_lsn {
                    src_page.set_lsn(lsn.raw());
                    wal_scratch.clear();
                }
                page_mutations.clear();
            }

            // Drop the source-page write guard before touching the
            // shared undo log; lock order is `page → undo`.
            drop(src_page);
            drop(src_guard);

            // Emit one WAL record for the applied rows on this page with the
            // page guard dropped.
            if let Some(sink) = wal {
                if !wal_scratch.is_empty() {
                    let lsn = Self::emit_update_int32_pair_delta_batch_wal_reuse(
                        &self.pool,
                        sink,
                        src_page_id,
                        xid,
                        command_id,
                        target_col,
                        delta,
                        &wal_scratch,
                        &mut wal_payload_buf,
                    )?;
                    Self::stamp_page_lsn(&self.pool, src_page_id, lsn)?;
                }
                wal_scratch.clear();
            }
            if let Some(batch) =
                page_undo_slots.take_batch(src_page_id, xid, command_id, target_col, delta)
            {
                compact_undo_scratch.push(batch);
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
            log.append_int32_pair_batches(&mut compact_undo_scratch);
        }

        if total_updated > 0 {
            self.invalidate_int32_pair_payload_stats_relation(rel);
            self.column_cache.bump_version(rel, xid);
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
    pub fn update_int32_pair_inplace_undo_parallel_no_wal<O, P>(
        &self,
        scan: UpdateInt32PairScan<'_, O, P>,
        edit: UpdateInt32PairEdit,
        stamp: UpdateInt32PairStamp,
        vm: Option<&crate::vm::VisibilityMap>,
    ) -> Result<usize, HeapError>
    where
        O: XidStatusOracle + Sync + ?Sized,
        P: Fn(i32, i32) -> bool + Sync,
    {
        let available_workers = std::thread::available_parallelism().map_or(1, |n| n.get());
        let UpdateInt32PairScan {
            rel,
            block_count,
            snapshot,
            oracle,
            predicate,
        } = scan;
        if block_count < 2_048 || available_workers <= 1 {
            return self.update_int32_pair_inplace_undo(
                UpdateInt32PairScan {
                    rel,
                    block_count,
                    snapshot,
                    oracle,
                    predicate,
                },
                edit,
                stamp,
                None,
                vm,
            );
        }
        let UpdateInt32PairEdit { target_col, delta } = edit;
        let UpdateInt32PairStamp { xid, command_id } = stamp;

        let block_count_usize = usize_from_u32(block_count, "block count overflow")?;
        let workers = available_workers
            .min(block_count_usize.div_ceil(512))
            .min(block_count_usize)
            // The update loop is memory-bandwidth bound and spawns
            // scoped workers per statement. Four workers saturate the
            // 1m-row hot path without paying extra thread-start cost.
            .clamp(1, 4);
        if workers <= 1 {
            return self.update_int32_pair_inplace_undo(
                UpdateInt32PairScan {
                    rel,
                    block_count,
                    snapshot,
                    oracle,
                    predicate,
                },
                edit,
                stamp,
                None,
                vm,
            );
        }

        let predicate_ref = &predicate;
        let mut updates = Vec::with_capacity(workers);
        // Work-stealing chunks (see the parallel WAL paths): fast cores take
        // proportionally more chunks so the slowest core never gates the
        // statement.
        let chunk_blocks = 512_u32;
        let next_chunk = std::sync::atomic::AtomicU32::new(0);

        std::thread::scope(|scope| {
            let mut handles = Vec::with_capacity(workers);
            for _ in 0..workers {
                let next_chunk = &next_chunk;
                handles.push(scope.spawn(move || {
                    let mut merged = Int32PairRangeUpdate {
                        total_updated: 0,
                        compact_undo: Vec::new(),
                    };
                    loop {
                        let start_block = next_chunk
                            .fetch_add(chunk_blocks, std::sync::atomic::Ordering::Relaxed);
                        if start_block >= block_count {
                            return Ok::<Int32PairRangeUpdate, HeapError>(merged);
                        }
                        let end_block = start_block.saturating_add(chunk_blocks).min(block_count);
                        let mut chunk =
                            self.update_int32_pair_range_no_wal(UpdateInt32PairRange {
                                rel,
                                start_block,
                                end_block,
                                snapshot,
                                oracle,
                                predicate: predicate_ref,
                                target_col,
                                delta,
                                xid,
                                command_id,
                                vm,
                            })?;
                        merged.total_updated = checked_heap_count_add(
                            merged.total_updated,
                            chunk.total_updated,
                            "updated tuple count overflow",
                        )?;
                        merged.compact_undo.append(&mut chunk.compact_undo);
                    }
                }));
            }

            for handle in handles {
                let update = handle
                    .join()
                    .map_err(|_| HeapError::MalformedHeader("parallel update worker panicked"))??;
                updates.push(update);
            }
            Ok::<(), HeapError>(())
        })?;

        let total_updated = updates.iter().try_fold(0_usize, |total, update| {
            checked_heap_count_add(total, update.total_updated, "updated tuple count overflow")
        })?;
        let mut compact_undo_scratch =
            Vec::with_capacity(usize_from_u32(block_count, "block count overflow")?);
        for mut update in updates {
            compact_undo_scratch.append(&mut update.compact_undo);
        }

        if !compact_undo_scratch.is_empty() {
            let log_handle = self
                .undo_log
                .entry(rel)
                .or_insert_with(|| parking_lot::RwLock::new(UndoRelationLog::default()));
            let mut log = log_handle.write();
            log.append_int32_pair_batches(&mut compact_undo_scratch);
        }

        if total_updated > 0 {
            self.invalidate_int32_pair_payload_stats_relation(rel);
            self.column_cache.bump_version(rel, xid);
        }

        Ok(total_updated)
    }

    /// Parallel WAL-backed variant for large fused `(Int32, Int32)` UPDATEs.
    ///
    /// Each worker owns a disjoint block range; per-page delta records are
    /// appended with the per-transaction chain link resolved atomically
    /// inside the sink (no chain mutex), and the first post-checkpoint touch
    /// of a page logs a full page image first (torn-page protection).
    /// Requires a nonblocking WAL sink; smaller relations and single-core
    /// hosts fall back to the sequential path unchanged.
    pub fn update_int32_pair_inplace_undo_parallel_wal<O, P>(
        &self,
        scan: UpdateInt32PairScan<'_, O, P>,
        edit: UpdateInt32PairEdit,
        stamp: UpdateInt32PairStamp,
        wal: &dyn WalSink,
        vm: Option<&crate::vm::VisibilityMap>,
    ) -> Result<usize, HeapError>
    where
        O: XidStatusOracle + Sync + ?Sized,
        P: Fn(i32, i32) -> bool + Sync,
    {
        const PARALLEL_WAL_UPDATE_MIN_BLOCKS: u32 = 128;
        const PARALLEL_WAL_UPDATE_BLOCKS_PER_WORKER: u32 = 256;

        let available_workers = std::thread::available_parallelism().map_or(1, |n| n.get());
        let UpdateInt32PairScan {
            rel,
            block_count,
            snapshot,
            oracle,
            predicate,
        } = scan;
        if block_count < PARALLEL_WAL_UPDATE_MIN_BLOCKS
            || available_workers <= 1
            || !wal.appends_without_blocking_io()
        {
            return self.update_int32_pair_inplace_undo(
                UpdateInt32PairScan {
                    rel,
                    block_count,
                    snapshot,
                    oracle,
                    predicate,
                },
                edit,
                stamp,
                Some(wal),
                vm,
            );
        }
        let UpdateInt32PairEdit { target_col, delta } = edit;
        let UpdateInt32PairStamp { xid, command_id } = stamp;

        let block_count_usize = usize_from_u32(block_count, "block count overflow")?;
        let blocks_per_worker = usize_from_u32(
            PARALLEL_WAL_UPDATE_BLOCKS_PER_WORKER,
            "blocks per worker overflow",
        )?;
        let workers = available_workers
            .min(block_count_usize.div_ceil(blocks_per_worker))
            .min(block_count_usize)
            .max(1);
        if workers <= 1 {
            return self.update_int32_pair_inplace_undo(
                UpdateInt32PairScan {
                    rel,
                    block_count,
                    snapshot,
                    oracle,
                    predicate,
                },
                edit,
                stamp,
                Some(wal),
                vm,
            );
        }

        let predicate_ref = &predicate;
        let chain = std::sync::atomic::AtomicU64::new(wal.last_lsn_for(xid).raw());
        let mut updates = Vec::with_capacity(workers);
        // Work-stealing chunks: on asymmetric cores an equal split gates the
        // whole statement on the slowest core; small chunks claimed via
        // fetch_add let fast cores take proportionally more work.
        let chunk_blocks = PARALLEL_WAL_UPDATE_BLOCKS_PER_WORKER.max(1);
        let next_chunk = std::sync::atomic::AtomicU32::new(0);

        std::thread::scope(|scope| {
            let mut handles = Vec::with_capacity(workers);
            for _ in 0..workers {
                handles.push(scope.spawn({
                    let chain = &chain;
                    let next_chunk = &next_chunk;
                    move || {
                        let mut merged = Int32PairRangeUpdate {
                            total_updated: 0,
                            compact_undo: Vec::new(),
                        };
                        loop {
                            let start_block = next_chunk
                                .fetch_add(chunk_blocks, std::sync::atomic::Ordering::Relaxed);
                            if start_block >= block_count {
                                return Ok::<Int32PairRangeUpdate, HeapError>(merged);
                            }
                            let end_block =
                                start_block.saturating_add(chunk_blocks).min(block_count);
                            let mut chunk = self.update_int32_pair_range_wal(
                                UpdateInt32PairRange {
                                    rel,
                                    start_block,
                                    end_block,
                                    snapshot,
                                    oracle,
                                    predicate: predicate_ref,
                                    target_col,
                                    delta,
                                    xid,
                                    command_id,
                                    vm,
                                },
                                wal,
                                chain,
                            )?;
                            merged.total_updated = checked_heap_count_add(
                                merged.total_updated,
                                chunk.total_updated,
                                "updated tuple count overflow",
                            )?;
                            merged.compact_undo.append(&mut chunk.compact_undo);
                        }
                    }
                }));
            }

            for handle in handles {
                let update = handle.join().map_err(|_| {
                    HeapError::MalformedHeader("parallel WAL update worker panicked")
                })??;
                updates.push(update);
            }
            Ok::<(), HeapError>(())
        })?;

        let total_updated = updates.iter().try_fold(0_usize, |total, update| {
            checked_heap_count_add(total, update.total_updated, "updated tuple count overflow")
        })?;
        let mut compact_undo_scratch = Vec::with_capacity(block_count_usize);
        for mut update in updates {
            compact_undo_scratch.append(&mut update.compact_undo);
        }

        if !compact_undo_scratch.is_empty() {
            let log_handle = self
                .undo_log
                .entry(rel)
                .or_insert_with(|| parking_lot::RwLock::new(UndoRelationLog::default()));
            let mut log = log_handle.write();
            log.append_int32_pair_batches(&mut compact_undo_scratch);
        }

        if total_updated > 0 {
            self.invalidate_int32_pair_payload_stats_relation(rel);
            self.column_cache.bump_version(rel, xid);
        }

        Ok(total_updated)
    }

    /// WAL-backed range worker for the parallel fused UPDATE: the no-WAL
    /// range body plus WAL-before-mutation ordering. Per page: emit an FPW
    /// for the first post-checkpoint touch (image captured under the held
    /// exclusive guard), collect matching slots WITHOUT mutating, append the
    /// page's delta record through the linked chain, then apply the header
    /// stamps + payload writes and set the page LSN — all before the guard
    /// drops, so the record is in the durable pipeline before the mutated
    /// page can ever be flushed.
    fn update_int32_pair_range_wal<O, P>(
        &self,
        request: UpdateInt32PairRange<'_, O, P>,
        wal: &dyn WalSink,
        chain: &std::sync::atomic::AtomicU64,
    ) -> Result<Int32PairRangeUpdate, HeapError>
    where
        O: XidStatusOracle + ?Sized,
        P: Fn(i32, i32) -> bool + ?Sized,
    {
        use crate::page::{ITEMID_SIZE, PAGE_HEADER_SIZE};

        let UpdateInt32PairRange {
            rel,
            start_block,
            end_block,
            snapshot,
            oracle,
            predicate,
            target_col,
            delta,
            xid,
            command_id,
            vm,
        } = request;
        let range_len = usize_from_u32(
            end_block.saturating_sub(start_block),
            "block range overflow",
        )?;
        let mut total_updated: usize = 0;
        let mut xmin_cache: Option<(Xid, u16, bool)> = None;
        let vm = vm.filter(|vm| vm.contains_relation(rel));
        let mut compact_undo_scratch: Vec<Int32PairUndoBatch> = Vec::with_capacity(range_len);
        let mut wal_scratch: Vec<u16> = Vec::with_capacity(256);
        let mut wal_payload_buf: Vec<u8> = Vec::with_capacity(512);
        let mut page_mutations: Vec<UpdateInt32PairMutation> = Vec::with_capacity(256);
        let xid_bytes = xid.raw().to_le_bytes();
        let cmd_bytes = command_id.raw().to_le_bytes();

        for src_block in start_block..end_block {
            let src_page_id = PageId::new(rel, BlockNumber::new(src_block));
            let mut page_updated = false;
            wal_scratch.clear();
            page_mutations.clear();

            let src_guard = self.get_page_relieved(src_page_id)?;
            let mut src_page = src_guard.write();
            // Torn-page protection under parallelism (see the parallel DELETE
            // worker): first post-checkpoint touch logs the pre-mutation
            // image through the same linked chain.
            let checkpoint_lsn = self
                .last_checkpoint_lsn
                .load(std::sync::atomic::Ordering::Acquire);
            if checkpoint_lsn != 0 && src_page.header().lsn < checkpoint_lsn {
                let payload = ultrasql_wal::payload::FullPageWritePayload {
                    page: src_page_id,
                    page_bytes: src_page.as_bytes().to_vec(),
                };
                let fpw_lsn = wal.append_borrowed_linked(
                    ultrasql_wal::record::RecordType::FullPageWrite,
                    xid,
                    0,
                    &payload.encode()?,
                    chain,
                )?;
                src_page.set_lsn(fpw_lsn.raw());
            }
            let src_bytes = src_page.as_bytes_mut();
            let src_slot_count = {
                let hdr = crate::page::PageHeader::decode(src_bytes).map_err(HeapError::Page)?;
                hdr.slot_count()
            };

            for src_slot in 0..src_slot_count {
                let item_id_off = PAGE_HEADER_SIZE + usize::from(src_slot) * ITEMID_SIZE;
                let item_raw = read_le_u32(src_bytes, item_id_off, "item id out of bounds")?;
                if item_raw & 0b11 != 1 {
                    continue;
                }
                let (length, offset) = itemid_window(item_raw)?;
                if length < TUPLE_HEADER_SIZE
                    || offset
                        .checked_add(length)
                        .is_none_or(|e| e > src_bytes.len())
                {
                    return Err(HeapError::MalformedHeader("slot shorter than header"));
                }

                let xmin_raw = read_le_u64(src_bytes, offset, "xmin out of bounds")?;
                let xmax_raw = read_le_u64(src_bytes, offset + 8, "xmax out of bounds")?;
                let infomask_bits = read_le_u16(src_bytes, offset + 24, "infomask out of bounds")?;
                let xmin_xid = Xid::new(xmin_raw);

                let visibility = if xmax_raw == 0 && infomask_bits & InfoMask::INPLACE_HISTORY == 0
                {
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
                let pair = read_le_u64(
                    src_bytes,
                    payload_off + 1,
                    "int32 pair payload out of bounds",
                )?;
                let (id, val) = decode_int32_pair(pair);

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
                    Visibility::VisibleMaybePreImage => {
                        // Visible with in-place undo history. When every
                        // recorded writer is visible to this snapshot the
                        // slot bytes are current and the row mutates like
                        // any visible row; otherwise acting on them would
                        // use a payload this snapshot must not observe —
                        // raise the same retryable conflict as a pending
                        // in-place update.
                        let tid = TupleId::new(src_page_id, src_slot);
                        if !self.undo_slot_state_current(
                            rel,
                            tid,
                            &src_bytes[payload_off..payload_off + 9],
                            snapshot,
                            oracle,
                        ) {
                            if predicate(id, val) {
                                return Err(HeapError::WriteConflict(
                                    "in-place tuple has an unresolved writer",
                                ));
                            }
                            continue;
                        }
                    }
                    Visibility::Invisible | Visibility::DeletedByOwn => continue,
                }

                if !predicate(id, val) {
                    continue;
                }

                let (new_id, new_val) = checked_int32_pair_add(id, val, target_col, delta)?;

                wal_scratch.push(src_slot);
                page_mutations.push(UpdateInt32PairMutation {
                    offset,
                    payload_off,
                    infomask_bits,
                    new_pair: encode_int32_pair(new_id, new_val),
                });

                total_updated += 1;
                page_updated = true;
            }

            if !wal_scratch.is_empty() {
                let lsn = Self::emit_update_int32_pair_delta_batch_wal_linked(
                    wal,
                    src_page_id,
                    xid,
                    command_id,
                    target_col,
                    delta,
                    &wal_scratch,
                    chain,
                    &mut wal_payload_buf,
                )?;
                let src_bytes = src_page.as_bytes_mut();
                for mutation in &page_mutations {
                    let offset = mutation.offset;
                    src_bytes[offset + 8..offset + 16].copy_from_slice(&xid_bytes);
                    src_bytes[offset + 20..offset + 24].copy_from_slice(&cmd_bytes);
                    let new_infomask =
                        mutation.infomask_bits | InfoMask::UPDATED | InfoMask::UPDATED_IN_PLACE;
                    src_bytes[offset + 24..offset + 26]
                        .copy_from_slice(&new_infomask.to_le_bytes());
                    src_bytes[mutation.payload_off + 1..mutation.payload_off + 9]
                        .copy_from_slice(&mutation.new_pair.to_le_bytes());
                }
                src_page.set_lsn(lsn.raw());
            }

            drop(src_page);
            drop(src_guard);

            // The WAL slot list IS the undo slot list: derive the compact
            // pre-image batch from it once per page instead of paying a
            // second per-row push. Slots are collected in ascending order,
            // so contiguity is a single O(1) check.
            if let (Some(&first), Some(&last)) = (wal_scratch.first(), wal_scratch.last()) {
                let slot_count = u16::try_from(wal_scratch.len())
                    .map_err(|_| HeapError::MalformedHeader("undo slot count overflow"))?;
                let contiguous = usize::from(last - first) + 1 == wal_scratch.len();
                compact_undo_scratch.push(Int32PairUndoBatch {
                    page: src_page_id,
                    writer_xid: xid,
                    command_id,
                    target_col,
                    delta,
                    first_slot: first,
                    slot_count,
                    slots: if contiguous {
                        Vec::new()
                    } else {
                        wal_scratch.clone()
                    },
                });
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

    fn update_int32_pair_range_no_wal<O, P>(
        &self,
        request: UpdateInt32PairRange<'_, O, P>,
    ) -> Result<Int32PairRangeUpdate, HeapError>
    where
        O: XidStatusOracle + ?Sized,
        P: Fn(i32, i32) -> bool + ?Sized,
    {
        use crate::page::{ITEMID_SIZE, PAGE_HEADER_SIZE};

        let UpdateInt32PairRange {
            rel,
            start_block,
            end_block,
            snapshot,
            oracle,
            predicate,
            target_col,
            delta,
            xid,
            command_id,
            vm,
        } = request;
        let range_len = usize_from_u32(
            end_block.saturating_sub(start_block),
            "block range overflow",
        )?;
        let mut total_updated: usize = 0;
        let mut xmin_cache: Option<(Xid, u16, bool)> = None;
        let vm = vm.filter(|vm| vm.contains_relation(rel));
        let mut compact_undo_scratch: Vec<Int32PairUndoBatch> = Vec::with_capacity(range_len);
        let mut page_undo_slots = PageUndoSlots::with_capacity(256);
        let xid_bytes = xid.raw().to_le_bytes();
        let cmd_bytes = command_id.raw().to_le_bytes();

        for src_block in start_block..end_block {
            let src_page_id = PageId::new(rel, BlockNumber::new(src_block));
            let mut page_updated = false;

            let src_guard = self.get_page_relieved(src_page_id)?;
            let mut src_page = src_guard.write();
            let src_bytes = src_page.as_bytes_mut();
            let src_slot_count = {
                let hdr = crate::page::PageHeader::decode(src_bytes).map_err(HeapError::Page)?;
                hdr.slot_count()
            };

            for src_slot in 0..src_slot_count {
                let item_id_off = PAGE_HEADER_SIZE + usize::from(src_slot) * ITEMID_SIZE;
                let item_raw = read_le_u32(src_bytes, item_id_off, "item id out of bounds")?;
                if item_raw & 0b11 != 1 {
                    continue;
                }
                let (length, offset) = itemid_window(item_raw)?;
                if length < TUPLE_HEADER_SIZE
                    || offset
                        .checked_add(length)
                        .is_none_or(|e| e > src_bytes.len())
                {
                    return Err(HeapError::MalformedHeader("slot shorter than header"));
                }

                let xmin_raw = read_le_u64(src_bytes, offset, "xmin out of bounds")?;
                let xmax_raw = read_le_u64(src_bytes, offset + 8, "xmax out of bounds")?;
                let infomask_bits = read_le_u16(src_bytes, offset + 24, "infomask out of bounds")?;
                let xmin_xid = Xid::new(xmin_raw);

                let visibility = if xmax_raw == 0 && infomask_bits & InfoMask::INPLACE_HISTORY == 0
                {
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
                let pair = read_le_u64(
                    src_bytes,
                    payload_off + 1,
                    "int32 pair payload out of bounds",
                )?;
                let (id, val) = decode_int32_pair(pair);

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
                    Visibility::VisibleMaybePreImage => {
                        // Visible with in-place undo history. When every
                        // recorded writer is visible to this snapshot the
                        // slot bytes are current and the row mutates like
                        // any visible row; otherwise acting on them would
                        // use a payload this snapshot must not observe —
                        // raise the same retryable conflict as a pending
                        // in-place update.
                        let tid = TupleId::new(src_page_id, src_slot);
                        if !self.undo_slot_state_current(
                            rel,
                            tid,
                            &src_bytes[payload_off..payload_off + 9],
                            snapshot,
                            oracle,
                        ) {
                            if predicate(id, val) {
                                return Err(HeapError::WriteConflict(
                                    "in-place tuple has an unresolved writer",
                                ));
                            }
                            continue;
                        }
                    }
                    Visibility::Invisible | Visibility::DeletedByOwn => continue,
                }

                if !predicate(id, val) {
                    continue;
                }

                let (new_id, new_val) = checked_int32_pair_add(id, val, target_col, delta)?;

                page_undo_slots.push(src_slot)?;
                src_bytes[offset + 8..offset + 16].copy_from_slice(&xid_bytes);
                src_bytes[offset + 20..offset + 24].copy_from_slice(&cmd_bytes);
                let new_infomask = infomask_bits | InfoMask::UPDATED | InfoMask::UPDATED_IN_PLACE;
                src_bytes[offset + 24..offset + 26].copy_from_slice(&new_infomask.to_le_bytes());

                let new_pair = encode_int32_pair(new_id, new_val);
                src_bytes[payload_off + 1..payload_off + 9]
                    .copy_from_slice(&new_pair.to_le_bytes());

                total_updated += 1;
                page_updated = true;
            }

            drop(src_page);
            drop(src_guard);

            if let Some(batch) =
                page_undo_slots.take_batch(src_page_id, xid, command_id, target_col, delta)
            {
                compact_undo_scratch.push(batch);
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
    pub fn update_int32_pair_tid_inplace_undo<O, P>(
        &self,
        target: UpdateInt32PairTid<'_, O, P>,
        edit: UpdateInt32PairEdit,
        stamp: UpdateInt32PairStamp,
        wal: Option<&dyn WalSink>,
        vm: Option<&crate::vm::VisibilityMap>,
    ) -> Result<usize, HeapError>
    where
        O: XidStatusOracle + ?Sized,
        P: Fn(i32, i32) -> bool,
    {
        use crate::page::{ITEMID_SIZE, PAGE_HEADER_SIZE};

        let UpdateInt32PairTid {
            tid,
            snapshot,
            oracle,
            predicate,
        } = target;
        let UpdateInt32PairEdit { target_col, delta } = edit;
        let UpdateInt32PairStamp { xid, command_id } = stamp;
        let rel = tid.page.relation;
        let vm = vm.filter(|vm| vm.contains_relation(rel));
        let xid_bytes = xid.raw().to_le_bytes();
        let cmd_bytes = command_id.raw().to_le_bytes();

        if let Some(sink) = wal {
            Self::maybe_emit_fpw(&self.pool, tid.page, sink, &self.last_checkpoint_lsn, xid)?;
        }

        let mut pre_image = [0_u8; 9];
        let mut post_image = [0_u8; 9];

        {
            let guard = self.get_page_relieved(tid.page)?;
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
            let item_raw = read_le_u32(bytes, item_id_off, "item id out of bounds")?;
            if item_raw & 0b11 != 1 {
                return Ok(0);
            }
            let (length, offset) = itemid_window(item_raw)?;
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
                Visibility::VisibleMaybePreImage => {
                    // Visible with in-place undo history (see the range
                    // loops): the slot bytes are mutable only when every
                    // recorded writer is visible to this snapshot.
                    if !self.undo_slot_state_current(
                        tid.page.relation,
                        tid,
                        &bytes[payload_off..payload_off + 9],
                        snapshot,
                        oracle,
                    ) {
                        if predicate(id, val) {
                            return Err(HeapError::WriteConflict(
                                "in-place tuple has an unresolved writer",
                            ));
                        }
                        return Ok(0);
                    }
                }
                Visibility::Invisible | Visibility::DeletedByOwn => return Ok(0),
            }

            if !predicate(id, val) {
                return Ok(0);
            }

            let (new_id, new_val) = checked_int32_pair_add(id, val, target_col, delta)?;

            pre_image.copy_from_slice(&bytes[payload_off..payload_off + 9]);
            bytes[offset + 8..offset + 16].copy_from_slice(&xid_bytes);
            bytes[offset + 20..offset + 24].copy_from_slice(&cmd_bytes);
            let new_infomask =
                header.infomask.bits() | InfoMask::UPDATED | InfoMask::UPDATED_IN_PLACE;
            bytes[offset + 24..offset + 26].copy_from_slice(&new_infomask.to_le_bytes());

            let payload_u64 = encode_int32_pair(new_id, new_val);
            bytes[payload_off + 1..payload_off + 9].copy_from_slice(&payload_u64.to_le_bytes());
            post_image.copy_from_slice(&bytes[payload_off..payload_off + 9]);
        }

        if let Some(sink) = wal {
            let lsn = Self::emit_update_in_place_wal(
                &self.pool,
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
        log.push_entry(UndoEntry {
            tid,
            writer_xid: xid,
            old_payload: pre_image,
        });
        drop(log);

        self.invalidate_int32_pair_payload_stats_relation(rel);
        self.column_cache.bump_version(rel, xid);
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

fn checked_int32_pair_add(
    id: i32,
    val: i32,
    target_col: u8,
    delta: i32,
) -> Result<(i32, i32), HeapError> {
    if target_col == 0 {
        id.checked_add(delta)
            .map(|new_id| (new_id, val))
            .ok_or(HeapError::NumericOverflow("Int32 id update overflow"))
    } else {
        val.checked_add(delta)
            .map(|new_val| (id, new_val))
            .ok_or(HeapError::NumericOverflow("Int32 value update overflow"))
    }
}
