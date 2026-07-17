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
use crate::page::{ITEMID_SIZE, ItemId, ItemIdFlags, PAGE_HEADER_SIZE};
use crate::wal_sink::WalSink;

use super::{
    HeapAccess, HeapError, Int32PairUndoBatch, UndoEntry, UndoRelationLog, checked_heap_count_add,
    undo_pre_image_from_log,
};

struct Int32PairRangeUpdate {
    total_updated: usize,
}

struct Int32PairWorkerResult {
    update: Int32PairRangeUpdate,
    error: Option<HeapError>,
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

#[derive(Clone, Copy, Debug)]
enum RollbackPayload {
    Full([u8; 9]),
    Delta { target_off: usize, delta: i32 },
}

#[derive(Clone, Copy, Debug)]
struct RollbackMutation {
    sequence: u64,
    offset: usize,
    payload_off: usize,
    retain_history: bool,
    payload: RollbackPayload,
}

const INT32_PAIR_PAYLOAD_SIZE: usize = 9;
const INT32_PAIR_TUPLE_SIZE: usize = TUPLE_HEADER_SIZE + INT32_PAIR_PAYLOAD_SIZE;

/// Recognize the dense fixed-width layout produced by append-only loading of
/// an `(Int32, Int32)` relation.
///
/// Validation is deliberately page-local and exact: every slot must be
/// `Normal`, have the expected tuple length, and point to the next tuple-sized
/// region below the preceding slot. Legitimate pages with holes, redirects,
/// variable-width rows, or compacted slot order fall back to the general
/// ItemId-decoding path. Once this returns `Some`, the update loop can derive
/// tuple offsets arithmetically and avoid materializing a mutation descriptor
/// for every qualifying row.
#[inline]
fn regular_int32_pair_first_offset(bytes: &[u8], slot_count: u16) -> Option<usize> {
    if slot_count == 0 {
        return None;
    }
    let tuple_size = u32::try_from(INT32_PAIR_TUPLE_SIZE).ok()?;
    let first_raw = read_le_u32(bytes, PAGE_HEADER_SIZE, "item id out of bounds").ok()?;
    let (first_length, first_offset) = itemid_window(first_raw).ok()?;
    if first_length != INT32_PAIR_TUPLE_SIZE {
        return None;
    }

    for slot in 0..slot_count {
        let item_id_off =
            PAGE_HEADER_SIZE.checked_add(usize::from(slot).checked_mul(ITEMID_SIZE)?)?;
        let raw = read_le_u32(bytes, item_id_off, "item id out of bounds").ok()?;
        let byte_delta = usize::from(slot).checked_mul(INT32_PAIR_TUPLE_SIZE)?;
        let expected_offset = first_offset.checked_sub(byte_delta)?;
        if expected_offset
            .checked_add(INT32_PAIR_TUPLE_SIZE)
            .is_none_or(|end| end > bytes.len())
        {
            return None;
        }
        let expected = ItemId::new(
            u32::try_from(expected_offset).ok()?,
            tuple_size,
            ItemIdFlags::Normal,
        );
        if raw != expected.into_raw() {
            return None;
        }
    }
    Some(first_offset)
}

#[inline]
fn regular_int32_pair_offset(first_offset: usize, slot: u16) -> usize {
    // INVARIANT: `regular_int32_pair_first_offset` validated this exact
    // subtraction for every slot on the page. Wrapping arithmetic keeps the
    // hot loop branch-free without weakening that validation.
    first_offset.wrapping_sub(usize::from(slot).wrapping_mul(INT32_PAIR_TUPLE_SIZE))
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

fn int32_pair_undo_batch_from_slots(
    page: PageId,
    writer_xid: Xid,
    command_id: CommandId,
    target_col: u8,
    delta: i32,
    slots: &[u16],
) -> Result<Int32PairUndoBatch, HeapError> {
    let first_slot = *slots
        .first()
        .ok_or(HeapError::MalformedHeader("empty undo slot batch"))?;
    let last_slot = *slots
        .last()
        .ok_or(HeapError::MalformedHeader("empty undo slot batch"))?;
    let slot_count = u16::try_from(slots.len())
        .map_err(|_| HeapError::MalformedHeader("undo slot count overflow"))?;
    let contiguous = usize::from(last_slot.saturating_sub(first_slot)) + 1 == slots.len();
    Ok(Int32PairUndoBatch {
        page,
        writer_xid,
        command_id,
        target_col,
        delta,
        first_slot,
        slot_count,
        slots: if contiguous {
            Vec::new()
        } else {
            slots.to_vec()
        },
    })
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
fn itemid_window(item_raw: u32) -> Result<(usize, usize), HeapError> {
    let length = u16::try_from((item_raw >> 2) & 0x7FFF)
        .map_err(|_| HeapError::MalformedHeader("item length overflow"))?;
    let offset = u16::try_from((item_raw >> 17) & 0x7FFF)
        .map_err(|_| HeapError::MalformedHeader("item offset overflow"))?;
    Ok((usize::from(length), usize::from(offset)))
}

fn rollback_int32_pair_window(bytes: &[u8], slot: u16) -> Result<(usize, usize), HeapError> {
    let item_id_delta = usize::from(slot)
        .checked_mul(ITEMID_SIZE)
        .ok_or(HeapError::MalformedHeader("item id offset overflow"))?;
    let item_id_off = PAGE_HEADER_SIZE
        .checked_add(item_id_delta)
        .ok_or(HeapError::MalformedHeader("item id offset overflow"))?;
    let item_raw = read_le_u32(bytes, item_id_off, "item id out of bounds")?;
    if item_raw & 0b11 != 1 {
        return Err(HeapError::MalformedHeader(
            "rollback target slot is not normal",
        ));
    }
    let (length, offset) = itemid_window(item_raw)?;
    let tuple_end = offset
        .checked_add(length)
        .ok_or(HeapError::MalformedHeader("slot length overflow"))?;
    if length < TUPLE_HEADER_SIZE + INT32_PAIR_PAYLOAD_SIZE || tuple_end > bytes.len() {
        return Err(HeapError::MalformedHeader(
            "rollback target shorter than int32 pair",
        ));
    }
    let payload_off = offset
        .checked_add(TUPLE_HEADER_SIZE)
        .ok_or(HeapError::MalformedHeader("payload offset overflow"))?;
    Ok((offset, payload_off))
}

fn undo_pre_image_predicate_matches<O, P>(
    log: &parking_lot::RwLock<UndoRelationLog>,
    tid: TupleId,
    current_payload: &[u8],
    snapshot: &Snapshot,
    oracle: &O,
    predicate: &P,
) -> Result<Option<bool>, HeapError>
where
    O: XidStatusOracle + ?Sized,
    P: Fn(i32, i32) -> bool + ?Sized,
{
    let Some(pre_image) =
        undo_pre_image_from_log(&log.read(), tid, current_payload, snapshot, oracle)
    else {
        return Ok(None);
    };
    let pair = read_le_u64(&pre_image, 1, "int32 pair undo pre-image out of bounds")?;
    let (id, val) = decode_int32_pair(pair);
    Ok(Some(predicate(id, val)))
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
    /// Each page transitions atomically under `page write → undo write`:
    /// validate every inverse, apply records newest-first across both undo
    /// representations, then mark those records restored before releasing the
    /// page. Applied records remain reader-visible until undo vacuum because a
    /// walker may still hold a stamped post-image copied before rollback.
    ///
    /// Called by the server's transaction abort path
    /// (`finalise_autocommit` on Err, explicit ROLLBACK, failed-
    /// transaction COMMIT). Idempotent: a second call for the same
    /// `xid` finds nothing to do.
    pub fn rollback_in_place_updates(&self, xid: Xid) -> Result<usize, HeapError> {
        let mut total_restored: usize = 0;
        // Clone each Arc while visiting the map, then drop every DashMap guard
        // before acquiring a page lock. Update publication uses the same
        // `page → undo` order and never nests a DashMap shard guard.
        let logs: Vec<_> = self
            .undo_log
            .iter()
            .map(|entry| (*entry.key(), std::sync::Arc::clone(entry.value())))
            .collect();
        for (rel, log_handle) in logs {
            let pages = log_handle.read().pages_written_by(xid);
            if pages.is_empty() {
                continue;
            }

            let restored_before = total_restored;
            for page_id in pages {
                let guard = self.get_page_relieved(page_id)?;
                let mut page = guard.write();
                let bytes = page.as_bytes_mut();
                let mut log = log_handle.write();
                let full = log.entries_written_by_on_page(xid, page_id);
                let compact = log.batches_written_by_on_page(xid, page_id);
                let mut mutations = Vec::with_capacity(
                    full.len()
                        + compact
                            .iter()
                            .map(|(_, batch)| batch.slot_len())
                            .sum::<usize>(),
                );

                // Complete every fallible page/slot validation before the
                // first inverse write. On error the page is unchanged and its
                // undo remains unapplied, so a later rollback retry is safe.
                for (sequence, entry) in full {
                    let (offset, payload_off) = rollback_int32_pair_window(bytes, entry.tid.slot)?;
                    mutations.push(RollbackMutation {
                        sequence,
                        offset,
                        payload_off,
                        retain_history: log.has_active_history_for_tid_excluding(entry.tid, xid),
                        payload: RollbackPayload::Full(entry.old_payload),
                    });
                }
                for (sequence, batch) in compact {
                    for slot in batch_slots(&batch) {
                        let (offset, payload_off) = rollback_int32_pair_window(bytes, slot)?;
                        let target_off = if batch.target_col == 0 {
                            payload_off + 1
                        } else {
                            payload_off + 5
                        };
                        let tid = TupleId::new(page_id, slot);
                        mutations.push(RollbackMutation {
                            sequence,
                            offset,
                            payload_off,
                            retain_history: log.has_active_history_for_tid_excluding(tid, xid),
                            payload: RollbackPayload::Delta {
                                target_off,
                                delta: batch.delta,
                            },
                        });
                    }
                }

                mutations.sort_by_key(|mutation| std::cmp::Reverse(mutation.sequence));
                let next_total = checked_heap_count_add(
                    total_restored,
                    mutations.len(),
                    "rollback tuple count overflow",
                )?;
                for mutation in &mutations {
                    match mutation.payload {
                        RollbackPayload::Full(pre_image) => {
                            bytes[mutation.payload_off..mutation.payload_off + pre_image.len()]
                                .copy_from_slice(&pre_image);
                        }
                        RollbackPayload::Delta { target_off, delta } => {
                            // INVARIANT: `rollback_int32_pair_window` proved
                            // this fixed-width target range exists. The forward
                            // update checked addition, so wrapping subtraction
                            // exactly recovers the prior value.
                            let current = i32::from_le_bytes([
                                bytes[target_off],
                                bytes[target_off + 1],
                                bytes[target_off + 2],
                                bytes[target_off + 3],
                            ]);
                            bytes[target_off..target_off + 4]
                                .copy_from_slice(&current.wrapping_sub(delta).to_le_bytes());
                        }
                    }
                    bytes[mutation.offset + 8..mutation.offset + 16].fill(0);
                    bytes[mutation.offset + 20..mutation.offset + 24].fill(0);
                    let cur_im = u16::from_le_bytes([
                        bytes[mutation.offset + 24],
                        bytes[mutation.offset + 25],
                    ]);
                    let mut new_im = cur_im & !(InfoMask::UPDATED | InfoMask::UPDATED_IN_PLACE);
                    if mutation.retain_history {
                        new_im |= InfoMask::INPLACE_HISTORY;
                    }
                    bytes[mutation.offset + 24..mutation.offset + 26]
                        .copy_from_slice(&new_im.to_le_bytes());
                }
                log.deactivate_written_by_on_page(xid, page_id);
                total_restored = next_total;
            }
            log_handle.write().compact_inactive_records();
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
        let undo_log_handle = self.undo_log_handle(rel);

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
        let mut update_prev_lsn = match wal {
            Some(sink)
                if self
                    .last_checkpoint_lsn
                    .load(std::sync::atomic::Ordering::Acquire)
                    == 0 =>
            {
                Some(sink.last_lsn_for(xid))
            }
            _ => None,
        };
        let mut page_mutations: Vec<UpdateInt32PairMutation> = Vec::with_capacity(256);
        let xid_bytes = xid.raw().to_le_bytes();
        let cmd_bytes = command_id.raw().to_le_bytes();

        for src_block in 0..block_count {
            let src_page_id = PageId::new(rel, BlockNumber::new(src_block));

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
                        let tid = TupleId::new(src_page_id, src_slot);
                        let logical_match = undo_pre_image_predicate_matches(
                            &undo_log_handle,
                            tid,
                            &src_bytes[payload_off..payload_off + 9],
                            snapshot,
                            oracle,
                            &predicate,
                        )?;
                        if logical_match.unwrap_or(true) {
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
                        if let Some(logical_match) = undo_pre_image_predicate_matches(
                            &undo_log_handle,
                            tid,
                            &src_bytes[payload_off..payload_off + 9],
                            snapshot,
                            oracle,
                            &predicate,
                        )? {
                            if logical_match {
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
                page_mutations.push(UpdateInt32PairMutation {
                    offset,
                    payload_off,
                    infomask_bits,
                    new_pair,
                });

                total_updated += 1;
            }

            let mut guard_appended_lsn = None;
            if let Some(sink) = wal
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

            let undo_batch =
                page_undo_slots.take_batch(src_page_id, xid, command_id, target_col, delta);
            if let Some(batch) = undo_batch {
                // Publish while the page remains write-locked and before the
                // first byte changes. A reader can never observe the stamped
                // post-image without this pre-image being lookup-visible.
                undo_log_handle.write().push_int32_pair_batch(batch);
                if let Some(vm) = vm {
                    vm.clear(src_page_id.relation, src_page_id.block);
                }

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
                }
            }
            page_mutations.clear();
            wal_scratch.clear();

            drop(src_page);
            drop(src_guard);
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
        let mut first_error: Option<HeapError> = None;
        // Work-stealing chunks (see the parallel WAL paths): fast cores take
        // proportionally more chunks so the slowest core never gates the
        // statement.
        let chunk_blocks = 512_u32;
        let next_chunk = std::sync::atomic::AtomicU32::new(0);
        let undo_log_handle = self.undo_log_handle(rel);

        std::thread::scope(|scope| {
            let mut handles = Vec::with_capacity(workers);
            for _ in 0..workers {
                let next_chunk = &next_chunk;
                let undo_log_handle = &undo_log_handle;
                handles.push(scope.spawn(move || {
                    let mut merged = Int32PairRangeUpdate { total_updated: 0 };
                    let work = std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                        || -> Result<(), HeapError> {
                            loop {
                                let start_block = next_chunk
                                    .fetch_add(chunk_blocks, std::sync::atomic::Ordering::Relaxed);
                                if start_block >= block_count {
                                    return Ok(());
                                }
                                let end_block =
                                    start_block.saturating_add(chunk_blocks).min(block_count);
                                self.update_int32_pair_range_no_wal(
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
                                    undo_log_handle,
                                    &mut merged,
                                )?;
                            }
                        },
                    ));
                    let error = match work {
                        Ok(Ok(())) => None,
                        Ok(Err(error)) => Some(error),
                        Err(_) => Some(HeapError::ParallelWorkerPanic),
                    };
                    Int32PairWorkerResult {
                        update: merged,
                        error,
                    }
                }));
            }

            for handle in handles {
                match handle.join() {
                    Ok(mut worker) => {
                        if first_error.is_none() {
                            first_error = worker.error.take();
                        }
                        updates.push(worker.update);
                    }
                    Err(_) => {
                        if first_error.is_none() {
                            first_error = Some(HeapError::ParallelWorkerPanic);
                        }
                    }
                }
            }
        });

        if let Some(error) = first_error {
            return Err(error);
        }

        let total_updated = updates.iter().try_fold(0_usize, |total, update| {
            checked_heap_count_add(total, update.total_updated, "updated tuple count overflow")
        })?;

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
            || !wal.supports_concurrent_linked_appends()
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
        let mut first_error: Option<HeapError> = None;
        // Work-stealing chunks: on asymmetric cores an equal split gates the
        // whole statement on the slowest core; small chunks claimed via
        // fetch_add let fast cores take proportionally more work.
        let chunk_blocks = PARALLEL_WAL_UPDATE_BLOCKS_PER_WORKER.max(1);
        let next_chunk = std::sync::atomic::AtomicU32::new(0);
        let undo_log_handle = self.undo_log_handle(rel);

        std::thread::scope(|scope| {
            let mut handles = Vec::with_capacity(workers);
            for _ in 0..workers {
                handles.push(scope.spawn({
                    let chain = &chain;
                    let next_chunk = &next_chunk;
                    let undo_log_handle = &undo_log_handle;
                    move || {
                        let mut merged = Int32PairRangeUpdate { total_updated: 0 };
                        let work = std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                            || -> Result<(), HeapError> {
                                loop {
                                    let start_block = next_chunk.fetch_add(
                                        chunk_blocks,
                                        std::sync::atomic::Ordering::Relaxed,
                                    );
                                    if start_block >= block_count {
                                        return Ok(());
                                    }
                                    let end_block =
                                        start_block.saturating_add(chunk_blocks).min(block_count);
                                    self.update_int32_pair_range_wal(
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
                                        undo_log_handle,
                                        &mut merged,
                                    )?;
                                }
                            },
                        ));
                        let error = match work {
                            Ok(Ok(())) => None,
                            Ok(Err(error)) => Some(error),
                            Err(_) => Some(HeapError::ParallelWorkerPanic),
                        };
                        Int32PairWorkerResult {
                            update: merged,
                            error,
                        }
                    }
                }));
            }

            for handle in handles {
                match handle.join() {
                    Ok(mut worker) => {
                        if first_error.is_none() {
                            first_error = worker.error.take();
                        }
                        updates.push(worker.update);
                    }
                    Err(_) => {
                        if first_error.is_none() {
                            first_error = Some(HeapError::ParallelWorkerPanic);
                        }
                    }
                }
            }
        });

        if let Some(error) = first_error {
            return Err(error);
        }

        let total_updated = updates.iter().try_fold(0_usize, |total, update| {
            checked_heap_count_add(total, update.total_updated, "updated tuple count overflow")
        })?;

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
    /// page can ever be flushed. Completed page batches append directly to
    /// the caller-owned `update` accumulator so a caught worker error or
    /// panic cannot discard pre-images from earlier pages in the same chunk.
    fn update_int32_pair_range_wal<O, P>(
        &self,
        request: UpdateInt32PairRange<'_, O, P>,
        wal: &dyn WalSink,
        chain: &std::sync::atomic::AtomicU64,
        undo_log: &parking_lot::RwLock<UndoRelationLog>,
        update: &mut Int32PairRangeUpdate,
    ) -> Result<(), HeapError>
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
        let mut xmin_cache: Option<(Xid, u16, bool)> = None;
        let vm = vm.filter(|vm| vm.contains_relation(rel));
        let mut wal_scratch: Vec<u16> = Vec::with_capacity(256);
        let mut wal_payload_buf: Vec<u8> = Vec::with_capacity(512);
        let mut page_mutations: Vec<UpdateInt32PairMutation> = Vec::with_capacity(256);
        let xid_bytes = xid.raw().to_le_bytes();
        let cmd_bytes = command_id.raw().to_le_bytes();

        for src_block in start_block..end_block {
            let src_page_id = PageId::new(rel, BlockNumber::new(src_block));
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
            let regular_first_offset = regular_int32_pair_first_offset(src_bytes, src_slot_count);

            for src_slot in 0..src_slot_count {
                let (length, offset) = if let Some(first_offset) = regular_first_offset {
                    (
                        INT32_PAIR_TUPLE_SIZE,
                        regular_int32_pair_offset(first_offset, src_slot),
                    )
                } else {
                    let item_id_off = PAGE_HEADER_SIZE + usize::from(src_slot) * ITEMID_SIZE;
                    let item_raw = read_le_u32(src_bytes, item_id_off, "item id out of bounds")?;
                    if item_raw & 0b11 != 1 {
                        continue;
                    }
                    itemid_window(item_raw)?
                };
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
                        let tid = TupleId::new(src_page_id, src_slot);
                        let logical_match = undo_pre_image_predicate_matches(
                            undo_log,
                            tid,
                            &src_bytes[payload_off..payload_off + 9],
                            snapshot,
                            oracle,
                            predicate,
                        )?;
                        if logical_match.unwrap_or(true) {
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
                        if let Some(logical_match) = undo_pre_image_predicate_matches(
                            undo_log,
                            tid,
                            &src_bytes[payload_off..payload_off + 9],
                            snapshot,
                            oracle,
                            predicate,
                        )? {
                            if logical_match {
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
                if regular_first_offset.is_none() {
                    page_mutations.push(UpdateInt32PairMutation {
                        offset,
                        payload_off,
                        infomask_bits,
                        new_pair: encode_int32_pair(new_id, new_val),
                    });
                }

                update.total_updated += 1;
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
                let batch = int32_pair_undo_batch_from_slots(
                    src_page_id,
                    xid,
                    command_id,
                    target_col,
                    delta,
                    &wal_scratch,
                )?;
                // WAL and all page-local validation are complete. Publish the
                // pre-image before the infallible byte writes while retaining
                // the page guard, so no reader can observe an unlogged stamp.
                undo_log.write().push_int32_pair_batch(batch);
                if let Some(vm) = vm {
                    vm.clear(src_page_id.relation, src_page_id.block);
                }

                let src_bytes = src_page.as_bytes_mut();
                if let Some(first_offset) = regular_first_offset {
                    let target_payload_offset = if target_col == 0 { 1 } else { 5 };
                    for &slot in &wal_scratch {
                        let offset = regular_int32_pair_offset(first_offset, slot);
                        let payload_off = offset + TUPLE_HEADER_SIZE;
                        let infomask_bits =
                            u16::from_le_bytes([src_bytes[offset + 24], src_bytes[offset + 25]]);
                        let target_off = payload_off + target_payload_offset;
                        let current = i32::from_le_bytes([
                            src_bytes[target_off],
                            src_bytes[target_off + 1],
                            src_bytes[target_off + 2],
                            src_bytes[target_off + 3],
                        ]);

                        src_bytes[offset + 8..offset + 16].copy_from_slice(&xid_bytes);
                        src_bytes[offset + 20..offset + 24].copy_from_slice(&cmd_bytes);
                        let new_infomask =
                            infomask_bits | InfoMask::UPDATED | InfoMask::UPDATED_IN_PLACE;
                        src_bytes[offset + 24..offset + 26]
                            .copy_from_slice(&new_infomask.to_le_bytes());
                        // The first pass checked this exact addition while the
                        // page guard remained held and the bytes were still
                        // unchanged. Because that checked pass succeeded,
                        // wrapping addition produces the same in-range value.
                        let updated = current.wrapping_add(delta);
                        src_bytes[target_off..target_off + 4]
                            .copy_from_slice(&updated.to_le_bytes());
                    }
                } else {
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
                }
                src_page.set_lsn(lsn.raw());
            }

            drop(src_page);
            drop(src_guard);
        }

        Ok(())
    }

    fn update_int32_pair_range_no_wal<O, P>(
        &self,
        request: UpdateInt32PairRange<'_, O, P>,
        undo_log: &parking_lot::RwLock<UndoRelationLog>,
        update: &mut Int32PairRangeUpdate,
    ) -> Result<(), HeapError>
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
        let mut xmin_cache: Option<(Xid, u16, bool)> = None;
        let vm = vm.filter(|vm| vm.contains_relation(rel));
        let mut page_slots = Vec::with_capacity(256);
        let mut page_mutations = Vec::with_capacity(256);
        let xid_bytes = xid.raw().to_le_bytes();
        let cmd_bytes = command_id.raw().to_le_bytes();

        for src_block in start_block..end_block {
            let src_page_id = PageId::new(rel, BlockNumber::new(src_block));
            page_slots.clear();
            page_mutations.clear();

            let src_guard = self.get_page_relieved(src_page_id)?;
            let mut src_page = src_guard.write();
            let src_bytes = src_page.as_bytes_mut();
            let src_slot_count = {
                let hdr = crate::page::PageHeader::decode(src_bytes).map_err(HeapError::Page)?;
                hdr.slot_count()
            };
            let regular_first_offset = regular_int32_pair_first_offset(src_bytes, src_slot_count);

            for src_slot in 0..src_slot_count {
                let (length, offset) = if let Some(first_offset) = regular_first_offset {
                    (
                        INT32_PAIR_TUPLE_SIZE,
                        regular_int32_pair_offset(first_offset, src_slot),
                    )
                } else {
                    let item_id_off = PAGE_HEADER_SIZE + usize::from(src_slot) * ITEMID_SIZE;
                    let item_raw = read_le_u32(src_bytes, item_id_off, "item id out of bounds")?;
                    if item_raw & 0b11 != 1 {
                        continue;
                    }
                    itemid_window(item_raw)?
                };
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
                        let tid = TupleId::new(src_page_id, src_slot);
                        let logical_match = undo_pre_image_predicate_matches(
                            undo_log,
                            tid,
                            &src_bytes[payload_off..payload_off + 9],
                            snapshot,
                            oracle,
                            predicate,
                        )?;
                        if logical_match.unwrap_or(true) {
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
                        if let Some(logical_match) = undo_pre_image_predicate_matches(
                            undo_log,
                            tid,
                            &src_bytes[payload_off..payload_off + 9],
                            snapshot,
                            oracle,
                            predicate,
                        )? {
                            if logical_match {
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

                page_slots.push(src_slot);
                if regular_first_offset.is_none() {
                    page_mutations.push(UpdateInt32PairMutation {
                        offset,
                        payload_off,
                        infomask_bits,
                        new_pair: encode_int32_pair(new_id, new_val),
                    });
                }

                update.total_updated += 1;
            }

            if !page_slots.is_empty() {
                let batch = int32_pair_undo_batch_from_slots(
                    src_page_id,
                    xid,
                    command_id,
                    target_col,
                    delta,
                    &page_slots,
                )?;
                // Every predicate, visibility check, arithmetic operation,
                // and bound check completed before this point. Publish before
                // the proven-infallible writes while the page remains locked.
                undo_log.write().push_int32_pair_batch(batch);
                if let Some(vm) = vm {
                    vm.clear(src_page_id.relation, src_page_id.block);
                }

                let src_bytes = src_page.as_bytes_mut();
                if let Some(first_offset) = regular_first_offset {
                    let target_payload_offset = if target_col == 0 { 1 } else { 5 };
                    for &slot in &page_slots {
                        let offset = regular_int32_pair_offset(first_offset, slot);
                        let payload_off = offset + TUPLE_HEADER_SIZE;
                        let infomask_bits =
                            u16::from_le_bytes([src_bytes[offset + 24], src_bytes[offset + 25]]);
                        let target_off = payload_off + target_payload_offset;
                        let current = i32::from_le_bytes([
                            src_bytes[target_off],
                            src_bytes[target_off + 1],
                            src_bytes[target_off + 2],
                            src_bytes[target_off + 3],
                        ]);
                        src_bytes[offset + 8..offset + 16].copy_from_slice(&xid_bytes);
                        src_bytes[offset + 20..offset + 24].copy_from_slice(&cmd_bytes);
                        let new_infomask =
                            infomask_bits | InfoMask::UPDATED | InfoMask::UPDATED_IN_PLACE;
                        src_bytes[offset + 24..offset + 26]
                            .copy_from_slice(&new_infomask.to_le_bytes());
                        src_bytes[target_off..target_off + 4]
                            .copy_from_slice(&current.wrapping_add(delta).to_le_bytes());
                    }
                } else {
                    for mutation in &page_mutations {
                        src_bytes[mutation.offset + 8..mutation.offset + 16]
                            .copy_from_slice(&xid_bytes);
                        src_bytes[mutation.offset + 20..mutation.offset + 24]
                            .copy_from_slice(&cmd_bytes);
                        let new_infomask =
                            mutation.infomask_bits | InfoMask::UPDATED | InfoMask::UPDATED_IN_PLACE;
                        src_bytes[mutation.offset + 24..mutation.offset + 26]
                            .copy_from_slice(&new_infomask.to_le_bytes());
                        src_bytes[mutation.payload_off + 1..mutation.payload_off + 9]
                            .copy_from_slice(&mutation.new_pair.to_le_bytes());
                    }
                }
            }

            drop(src_page);
            drop(src_guard);
        }

        Ok(())
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
        let undo_log_handle = self.undo_log_handle(rel);

        if let Some(sink) = wal {
            Self::maybe_emit_fpw(&self.pool, tid.page, sink, &self.last_checkpoint_lsn, xid)?;
        }

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
                    let logical_match = undo_pre_image_predicate_matches(
                        &undo_log_handle,
                        tid,
                        &bytes[payload_off..payload_off + 9],
                        snapshot,
                        oracle,
                        &predicate,
                    )?;
                    if logical_match.unwrap_or(true) {
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
                    if let Some(logical_match) = undo_pre_image_predicate_matches(
                        &undo_log_handle,
                        tid,
                        &bytes[payload_off..payload_off + 9],
                        snapshot,
                        oracle,
                        &predicate,
                    )? {
                        if logical_match {
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

            let mut pre_image = [0_u8; 9];
            pre_image.copy_from_slice(&bytes[payload_off..payload_off + 9]);
            let mut post_image = pre_image;
            post_image[1..9].copy_from_slice(&encode_int32_pair(new_id, new_val).to_le_bytes());
            let appended_lsn = if let Some(sink) = wal {
                Some(Self::emit_update_in_place_wal(
                    sink,
                    tid,
                    xid,
                    command_id,
                    &pre_image,
                    &post_image,
                )?)
            } else {
                None
            };

            // Publish before mutating and while retaining the page guard.
            // The following fixed ranges were validated above, so no
            // fallible operation remains between publication and unlock.
            undo_log_handle.write().push_entry(UndoEntry {
                tid,
                writer_xid: xid,
                command_id,
                old_payload: pre_image,
            });
            if let Some(vm) = vm {
                vm.clear(tid.page.relation, tid.page.block);
            }
            bytes[offset + 8..offset + 16].copy_from_slice(&xid_bytes);
            bytes[offset + 20..offset + 24].copy_from_slice(&cmd_bytes);
            let new_infomask =
                header.infomask.bits() | InfoMask::UPDATED | InfoMask::UPDATED_IN_PLACE;
            bytes[offset + 24..offset + 26].copy_from_slice(&new_infomask.to_le_bytes());

            let payload_u64 = encode_int32_pair(new_id, new_val);
            bytes[payload_off + 1..payload_off + 9].copy_from_slice(&payload_u64.to_le_bytes());
            if let Some(lsn) = appended_lsn {
                page.set_lsn(lsn.raw());
            }
        }

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

#[cfg(test)]
mod regular_page_layout_tests {
    use ultrasql_core::constants::PAGE_SIZE;

    use super::{
        INT32_PAIR_TUPLE_SIZE, regular_int32_pair_first_offset, regular_int32_pair_offset,
    };
    use crate::page::Page;

    #[test]
    fn recognizes_only_dense_fixed_width_normal_slots() {
        let mut page = Page::new_heap();
        let tuple = [0_u8; INT32_PAIR_TUPLE_SIZE];
        for _ in 0..3 {
            page.insert_tuple_appended(&tuple).unwrap();
        }

        let first = regular_int32_pair_first_offset(page.as_bytes(), page.header().slot_count())
            .expect("dense fixed-width page");
        assert_eq!(first, PAGE_SIZE - INT32_PAIR_TUPLE_SIZE);
        assert_eq!(
            regular_int32_pair_offset(first, 2),
            PAGE_SIZE - 3 * INT32_PAIR_TUPLE_SIZE
        );

        page.delete_tuple(1).unwrap();
        assert_eq!(
            regular_int32_pair_first_offset(page.as_bytes(), page.header().slot_count()),
            None,
            "a dead-slot hole must use the general ItemId path"
        );

        let mut variable = Page::new_heap();
        variable.insert_tuple_appended(&tuple).unwrap();
        variable
            .insert_tuple_appended(&tuple[..INT32_PAIR_TUPLE_SIZE - 1])
            .unwrap();
        assert_eq!(
            regular_int32_pair_first_offset(variable.as_bytes(), variable.header().slot_count()),
            None,
            "a variable-width page must use the general ItemId path"
        );
    }
}
