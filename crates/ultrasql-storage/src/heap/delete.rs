//! See `crate::heap` for the public API.
//!
//! Part of the `heap` module split — each `impl<L: PageLoader>
//! HeapAccess<L>` block here adds methods to the type defined in
//! `heap/mod.rs`. Splitting across files keeps each unit under the
//! 600-line ceiling without changing semantics.

use std::sync::atomic::Ordering;

use ultrasql_core::{BlockNumber, CommandId, Lsn, PageId, RelationId, TupleId, Xid};
use ultrasql_mvcc::tuple_header::{InfoMask, TUPLE_HEADER_SIZE};
use ultrasql_mvcc::{Snapshot, TupleHeader, Visibility, XidStatusOracle, is_visible};
use ultrasql_wal::WalRecord;
use ultrasql_wal::payload::HeapDeletePayload;
use ultrasql_wal::record::RecordType;

use crate::buffer_pool::{PageGuard, PageLoader};
use crate::wal_sink::WalSink;

use super::{
    DeleteOptions, HeapAccess, HeapError, Int32PairPagePayloadStats, checked_heap_count_add,
};

#[inline]
fn itemid_window(item_raw: u32) -> Result<(usize, usize), HeapError> {
    let length = u16::try_from((item_raw >> 2) & 0x7FFF)
        .map_err(|_| HeapError::MalformedHeader("item length overflow"))?;
    let offset = u16::try_from((item_raw >> 17) & 0x7FFF)
        .map_err(|_| HeapError::MalformedHeader("item offset overflow"))?;
    Ok((usize::from(length), usize::from(offset)))
}

/// Int32 comparison used by storage-native fused delete predicates.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Int32PairCmp {
    /// `lhs = rhs`.
    Eq,
    /// `lhs <> rhs`.
    Ne,
    /// `lhs < rhs`.
    Lt,
    /// `lhs <= rhs`.
    Le,
    /// `lhs > rhs`.
    Gt,
    /// `lhs >= rhs`.
    Ge,
}

impl Int32PairCmp {
    #[inline]
    fn check(self, lhs: i32, rhs: i32) -> bool {
        match self {
            Self::Eq => lhs == rhs,
            Self::Ne => lhs != rhs,
            Self::Lt => lhs < rhs,
            Self::Le => lhs <= rhs,
            Self::Gt => lhs > rhs,
            Self::Ge => lhs >= rhs,
        }
    }
}

/// Predicate descriptor for fused `(Int32, Int32)` DELETE scans.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Int32PairPredicate {
    /// Match every visible tuple without decoding the fixed-width payload.
    All,
    /// Compare one payload column to an Int32 literal.
    ColumnCmp {
        /// Column index: `0` for `id`, `1` for `value`.
        col_index: u8,
        /// Comparison operator.
        op: Int32PairCmp,
        /// Literal right-hand side.
        literal: i32,
    },
}

impl Int32PairPredicate {
    /// Return the single payload column needed to evaluate this predicate.
    #[must_use]
    pub const fn required_column(self) -> Option<u8> {
        match self {
            Self::All => None,
            Self::ColumnCmp { col_index, .. } if col_index < 2 => Some(col_index),
            Self::ColumnCmp { .. } => None,
        }
    }

    #[inline]
    fn matches_column(self, col_index: u8, value: i32) -> bool {
        match self {
            Self::All => true,
            Self::ColumnCmp {
                col_index: expected,
                op,
                literal,
            } if expected == col_index => op.check(value, literal),
            Self::ColumnCmp { .. } => false,
        }
    }

    #[inline]
    fn matches_pair(self, id: i32, val: i32) -> bool {
        match self {
            Self::All => true,
            Self::ColumnCmp {
                col_index: 0,
                op,
                literal,
            } => op.check(id, literal),
            Self::ColumnCmp {
                col_index: 1,
                op,
                literal,
            } => op.check(val, literal),
            Self::ColumnCmp { .. } => false,
        }
    }
}

/// Evaluation contract for fused `(Int32, Int32)` DELETE predicates.
pub trait Int32PairPredicateEval {
    /// Return `true` when every visible tuple matches without payload decode.
    fn matches_all(&self) -> bool {
        false
    }

    /// Return a simple one-column comparison when this predicate has one.
    fn column_cmp(&self) -> Option<(u8, Int32PairCmp, i32)> {
        None
    }

    /// Return `Some(0)` or `Some(1)` when this predicate can be evaluated from
    /// one payload column. Returning `None` makes the heap decode both columns
    /// and call [`Self::matches_pair`].
    fn required_column(&self) -> Option<u8> {
        None
    }

    /// Evaluate the predicate from both decoded payload columns.
    fn matches_pair(&self, id: i32, val: i32) -> bool;

    /// Evaluate the predicate from one decoded payload column.
    fn matches_column(&self, col_index: u8, value: i32) -> bool {
        let _ = (col_index, value);
        false
    }
}

impl<F> Int32PairPredicateEval for F
where
    F: Fn(i32, i32) -> bool,
{
    #[inline]
    fn matches_pair(&self, id: i32, val: i32) -> bool {
        self(id, val)
    }
}

impl Int32PairPredicateEval for Int32PairPredicate {
    #[inline]
    fn matches_all(&self) -> bool {
        matches!(*self, Self::All)
    }

    #[inline]
    fn column_cmp(&self) -> Option<(u8, Int32PairCmp, i32)> {
        match *self {
            Self::ColumnCmp {
                col_index,
                op,
                literal,
            } => Some((col_index, op, literal)),
            Self::All => None,
        }
    }

    #[inline]
    fn required_column(&self) -> Option<u8> {
        (*self).required_column()
    }

    #[inline]
    fn matches_pair(&self, id: i32, val: i32) -> bool {
        (*self).matches_pair(id, val)
    }

    #[inline]
    fn matches_column(&self, col_index: u8, value: i32) -> bool {
        (*self).matches_column(col_index, value)
    }
}

#[derive(Clone, Copy, Debug)]
enum DeletePredicatePlan {
    All,
    ColumnCmp {
        col_index: u8,
        op: Int32PairCmp,
        literal: i32,
    },
    Pair {
        required_col: Option<u8>,
    },
}

#[inline]
fn delete_predicate_plan<P: Int32PairPredicateEval + ?Sized>(
    predicate: &P,
) -> Result<DeletePredicatePlan, HeapError> {
    if predicate.matches_all() {
        return Ok(DeletePredicatePlan::All);
    }
    if let Some((col_index, op, literal)) = predicate.column_cmp() {
        return match col_index {
            0 | 1 => Ok(DeletePredicatePlan::ColumnCmp {
                col_index,
                op,
                literal,
            }),
            _ => Err(HeapError::MalformedHeader(
                "int32 pair predicate column out of range",
            )),
        };
    }
    match predicate.required_column() {
        Some(col @ (0 | 1)) => Ok(DeletePredicatePlan::Pair {
            required_col: Some(col),
        }),
        Some(_) => Err(HeapError::MalformedHeader(
            "int32 pair predicate column out of range",
        )),
        None => Ok(DeletePredicatePlan::Pair { required_col: None }),
    }
}

#[inline]
fn read_u16_at(bytes: &[u8], start: usize) -> u16 {
    debug_assert!(start.checked_add(2).is_some_and(|end| end <= bytes.len()));
    // SAFETY: Callers validate the tuple or payload window before reading
    // fixed-width fields. `read_unaligned` permits heap tuple byte alignment.
    let word = unsafe { bytes.as_ptr().add(start).cast::<u16>().read_unaligned() };
    u16::from_le(word)
}

#[inline]
fn read_u32_at(bytes: &[u8], start: usize) -> u32 {
    debug_assert!(start.checked_add(4).is_some_and(|end| end <= bytes.len()));
    // SAFETY: Callers validate the tuple or payload window before reading
    // fixed-width fields. `read_unaligned` permits heap tuple byte alignment.
    let word = unsafe { bytes.as_ptr().add(start).cast::<u32>().read_unaligned() };
    u32::from_le(word)
}

#[inline]
fn read_i32_at(bytes: &[u8], start: usize) -> i32 {
    let word = read_u32_at(bytes, start);
    i32::from_le_bytes(word.to_le_bytes())
}

#[inline]
fn read_u64_at(bytes: &[u8], start: usize) -> u64 {
    debug_assert!(start.checked_add(8).is_some_and(|end| end <= bytes.len()));
    // SAFETY: Callers validate the tuple window before reading fixed-width
    // header fields. `read_unaligned` permits heap tuple byte alignment.
    let word = unsafe { bytes.as_ptr().add(start).cast::<u64>().read_unaligned() };
    u64::from_le(word)
}

#[inline]
fn int32_pair_delete_predicate_matches_planned<P: Int32PairPredicateEval + ?Sized>(
    bytes: &[u8],
    payload_off: usize,
    tuple_end: usize,
    plan: DeletePredicatePlan,
    predicate: &P,
) -> Result<bool, HeapError> {
    if matches!(plan, DeletePredicatePlan::All) {
        return Ok(true);
    }

    let payload_end = payload_off
        .checked_add(9)
        .ok_or(HeapError::MalformedHeader("int32 pair payload overflow"))?;
    if payload_end > tuple_end {
        return Err(HeapError::MalformedHeader(
            "payload shorter than (Int32, Int32)",
        ));
    }

    match plan {
        DeletePredicatePlan::All => unreachable!("handled before payload validation"),
        DeletePredicatePlan::ColumnCmp {
            col_index: 0,
            op,
            literal,
        } => {
            let id = read_i32_at(bytes, payload_off + 1);
            Ok(op.check(id, literal))
        }
        DeletePredicatePlan::ColumnCmp {
            col_index: 1,
            op,
            literal,
        } => {
            let val = read_i32_at(bytes, payload_off + 5);
            Ok(op.check(val, literal))
        }
        DeletePredicatePlan::ColumnCmp { .. } => {
            unreachable!("predicate column validated before scan")
        }
        DeletePredicatePlan::Pair {
            required_col: Some(0),
        } => {
            let id = read_i32_at(bytes, payload_off + 1);
            Ok(predicate.matches_column(0, id))
        }
        DeletePredicatePlan::Pair {
            required_col: Some(1),
        } => {
            let val = read_i32_at(bytes, payload_off + 5);
            Ok(predicate.matches_column(1, val))
        }
        DeletePredicatePlan::Pair {
            required_col: Some(_),
        } => unreachable!("predicate column validated before scan"),
        DeletePredicatePlan::Pair { required_col: None } => {
            let id = read_i32_at(bytes, payload_off + 1);
            let val = read_i32_at(bytes, payload_off + 5);
            Ok(predicate.matches_pair(id, val))
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct DeleteVisibilityCache {
    xmin_raw: u64,
    xmax_raw: u64,
    command_raw: u64,
    infomask_bits: u16,
    visibility: Visibility,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DeleteSlotWalView<'a> {
    Empty,
    Range { first_slot: u16, slot_count: u16 },
    Sparse(&'a [u16]),
}

#[derive(Debug)]
pub(super) struct DeleteSlotWalScratch {
    sparse_slots: Vec<u16>,
    first_slot: u16,
    slot_count: u16,
    contiguous: bool,
    has_any: bool,
}

impl DeleteSlotWalScratch {
    pub(super) fn with_capacity(capacity: usize) -> Self {
        Self {
            sparse_slots: Vec::with_capacity(capacity),
            first_slot: 0,
            slot_count: 0,
            contiguous: true,
            has_any: false,
        }
    }

    pub(super) fn clear(&mut self) {
        self.sparse_slots.clear();
        self.first_slot = 0;
        self.slot_count = 0;
        self.contiguous = true;
        self.has_any = false;
    }

    pub(super) fn is_empty(&self) -> bool {
        !self.has_any
    }

    pub(super) fn push(&mut self, slot: u16) -> Result<(), HeapError> {
        if !self.has_any {
            self.first_slot = slot;
            self.slot_count = 1;
            self.contiguous = true;
            self.has_any = true;
            return Ok(());
        }

        if self.contiguous {
            let expected = self
                .first_slot
                .checked_add(self.slot_count)
                .ok_or(HeapError::MalformedHeader("delete slot range overflow"))?;
            if slot == expected {
                self.slot_count = self
                    .slot_count
                    .checked_add(1)
                    .ok_or(HeapError::MalformedHeader("delete slot range overflow"))?;
                return Ok(());
            }
            self.sparse_slots.clear();
            self.sparse_slots.reserve(usize::from(self.slot_count) + 1);
            for delta in 0..self.slot_count {
                let range_slot = self
                    .first_slot
                    .checked_add(delta)
                    .ok_or(HeapError::MalformedHeader("delete slot range overflow"))?;
                self.sparse_slots.push(range_slot);
            }
            self.sparse_slots.push(slot);
            self.contiguous = false;
            return Ok(());
        }

        self.sparse_slots.push(slot);
        Ok(())
    }

    pub(super) fn view(&self) -> DeleteSlotWalView<'_> {
        if !self.has_any {
            DeleteSlotWalView::Empty
        } else if self.contiguous {
            DeleteSlotWalView::Range {
                first_slot: self.first_slot,
                slot_count: self.slot_count,
            }
        } else {
            DeleteSlotWalView::Sparse(&self.sparse_slots)
        }
    }
}

#[derive(Debug)]
struct Int32PairPagePayloadStatsBuilder {
    normal_slots: u16,
    min0: i32,
    max0: i32,
    min1: i32,
    max1: i32,
}

impl Int32PairPagePayloadStatsBuilder {
    const fn new() -> Self {
        Self {
            normal_slots: 0,
            min0: 0,
            max0: 0,
            min1: 0,
            max1: 0,
        }
    }

    fn observe(&mut self, id: i32, val: i32) -> Result<(), HeapError> {
        if self.normal_slots == 0 {
            self.min0 = id;
            self.max0 = id;
            self.min1 = val;
            self.max1 = val;
        } else {
            self.min0 = self.min0.min(id);
            self.max0 = self.max0.max(id);
            self.min1 = self.min1.min(val);
            self.max1 = self.max1.max(val);
        }
        self.normal_slots = self
            .normal_slots
            .checked_add(1)
            .ok_or(HeapError::MalformedHeader("int32 pair stats slot overflow"))?;
        Ok(())
    }

    fn finish(self, slot_count: u16) -> Option<Int32PairPagePayloadStats> {
        (self.normal_slots > 0).then_some(Int32PairPagePayloadStats {
            slot_count,
            normal_slots: self.normal_slots,
            min0: self.min0,
            max0: self.max0,
            min1: self.min1,
            max1: self.max1,
        })
    }
}

#[inline]
fn int32_pair_stats_prove_all_match(
    stats: Int32PairPagePayloadStats,
    slot_count: u16,
    plan: DeletePredicatePlan,
) -> bool {
    if stats.slot_count != slot_count || stats.normal_slots == 0 {
        return false;
    }
    let DeletePredicatePlan::ColumnCmp {
        col_index,
        op,
        literal,
    } = plan
    else {
        return matches!(plan, DeletePredicatePlan::All);
    };
    let (min, max) = if col_index == 0 {
        (stats.min0, stats.max0)
    } else {
        (stats.min1, stats.max1)
    };
    match op {
        Int32PairCmp::Eq => min == literal && max == literal,
        Int32PairCmp::Ne => literal < min || literal > max,
        Int32PairCmp::Lt => max < literal,
        Int32PairCmp::Le => max <= literal,
        Int32PairCmp::Gt => min > literal,
        Int32PairCmp::Ge => min >= literal,
    }
}

impl DeleteVisibilityCache {
    #[inline]
    fn matches(self, xmin_raw: u64, xmax_raw: u64, command_raw: u64, infomask_bits: u16) -> bool {
        self.xmin_raw == xmin_raw
            && self.xmax_raw == xmax_raw
            && self.command_raw == command_raw
            && self.infomask_bits == infomask_bits
    }
}

#[inline]
fn stamp_delete_int32_pair_header(
    bytes: &mut [u8],
    offset: usize,
    infomask_bits: u16,
    xid_bytes: &[u8; 8],
    cmd_bytes: &[u8; 4],
) {
    bytes[offset + 8..offset + 16].copy_from_slice(xid_bytes);
    bytes[offset + 20..offset + 24].copy_from_slice(cmd_bytes);
    let new_infomask = infomask_bits | InfoMask::UPDATED;
    bytes[offset + 24..offset + 26].copy_from_slice(&new_infomask.to_le_bytes());
}

struct DeleteInt32PairRange<'a, O: ?Sized, P: ?Sized> {
    rel: RelationId,
    start_block: u32,
    end_block: u32,
    snapshot: &'a Snapshot,
    oracle: &'a O,
    predicate: &'a P,
    xid: Xid,
    command_id: CommandId,
    vm: Option<&'a crate::vm::VisibilityMap>,
}

struct DeleteInt32PairWalRange<'a, O: ?Sized, P: ?Sized> {
    rel: RelationId,
    start_block: u32,
    end_block: u32,
    snapshot: &'a Snapshot,
    oracle: &'a O,
    predicate: &'a P,
    xid: Xid,
    command_id: CommandId,
    wal: &'a dyn WalSink,
    prev_lsn: &'a parking_lot::Mutex<Lsn>,
    vm: Option<&'a crate::vm::VisibilityMap>,
}

/// Page-major scan request for fused `(Int32, Int32)` DELETE.
///
/// Closure predicates receive decoded `(id, value)` payloads. Typed
/// [`Int32PairPredicate`] values can advertise a single required column so the
/// heap avoids decoding payload bytes the predicate cannot inspect.
pub struct DeleteInt32PairScan<'a, O: ?Sized, P> {
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

impl<O: ?Sized, P> std::fmt::Debug for DeleteInt32PairScan<'_, O, P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeleteInt32PairScan")
            .field("rel", &self.rel)
            .field("block_count", &self.block_count)
            .finish_non_exhaustive()
    }
}

/// MVCC stamp written by fused in-place DELETE helpers.
#[derive(Clone, Copy, Debug)]
pub struct DeleteInt32PairStamp {
    /// XID stamped as `xmax` on deleted tuple versions.
    pub xid: Xid,
    /// Command id stamped as `cmax` on deleted tuple versions.
    pub command_id: CommandId,
}

impl<L: PageLoader> HeapAccess<L> {
    const PARALLEL_WAL_DELETE_MIN_BLOCKS: u32 = 128;
    const PARALLEL_WAL_DELETE_BLOCKS_PER_WORKER: u32 = 256;

    /// Clear `xmax` stamps for an aborted transaction.
    ///
    /// Regular MVCC visibility can treat an aborted `xmax` as visible,
    /// but the heap update helpers must also see the slot as physically
    /// alive before stamping a new `xmax`. Abort cleanup therefore clears
    /// `xmax`/`cmax` for DELETE stamps and aborted classical UPDATE old
    /// versions. In-place UPDATEs are skipped here because their payload
    /// must be restored from the undo log before their header is cleared.
    ///
    /// Public so the server's `ROLLBACK TO SAVEPOINT` path can clear a
    /// rolled-back subtransaction's DELETE stamps directly. (The full-abort
    /// path reaches this via [`Self::rollback_in_place_updates`], which
    /// calls it after restoring in-place pre-images.)
    pub fn rollback_delete_stamps(&self, xid: Xid) -> Result<usize, HeapError> {
        use crate::page::{ITEMID_SIZE, PAGE_HEADER_SIZE, PageHeader};

        let mut total_restored = 0_usize;
        let pages = self.take_rollback_stamp_pages(xid);
        let mut restored_relations: Vec<RelationId> = Vec::new();
        for page_id in pages {
            let mut page_restored = false;
            let guard = self.get_page_relieved(page_id)?;
            let mut page = guard.write();
            let bytes = page.as_bytes_mut();
            let slot_count = PageHeader::decode(bytes)
                .map_err(HeapError::Page)?
                .slot_count();

            for slot in 0..slot_count {
                let item_id_off = PAGE_HEADER_SIZE + usize::from(slot) * ITEMID_SIZE;
                let item_raw = u32::from_le_bytes([
                    bytes[item_id_off],
                    bytes[item_id_off + 1],
                    bytes[item_id_off + 2],
                    bytes[item_id_off + 3],
                ]);
                if item_raw & 0b11 != 1 {
                    continue;
                }
                let (length, offset) = itemid_window(item_raw)?;
                if length < TUPLE_HEADER_SIZE
                    || offset.checked_add(length).is_none_or(|e| e > bytes.len())
                {
                    return Err(HeapError::MalformedHeader("slot shorter than header"));
                }
                let (header, _) = TupleHeader::decode(&bytes[offset..offset + TUPLE_HEADER_SIZE])
                    .ok_or(HeapError::MalformedHeader("header decode failed"))?;
                if header.xmax != xid || header.infomask.contains(InfoMask::UPDATED_IN_PLACE) {
                    continue;
                }

                let tid = TupleId::new(page_id, slot);
                let mut restored = header;
                restored.xmax = Xid::INVALID;
                restored.cmax = CommandId::FIRST;
                restored.ctid = tid;
                restored.infomask.clear(
                    InfoMask::UPDATED
                        | InfoMask::HOT_UPDATED
                        | InfoMask::UPDATED_IN_PLACE
                        | InfoMask::XMAX_COMMITTED
                        | InfoMask::XMAX_INVALID,
                );
                let mut header_bytes = [0_u8; TUPLE_HEADER_SIZE];
                restored.encode(&mut header_bytes);
                bytes[offset..offset + TUPLE_HEADER_SIZE].copy_from_slice(&header_bytes);
                page_restored = true;
                total_restored += 1;
            }
            if page_restored && !restored_relations.contains(&page_id.relation) {
                restored_relations.push(page_id.relation);
            }
        }

        for rel in restored_relations {
            self.column_cache.bump_version(rel, xid);
        }

        Ok(total_restored)
    }

    /// Mark a tuple deleted.
    ///
    /// The slot stays allocated and the payload is left untouched; only
    /// the header's `xmax`/`cmax` fields move. A later visibility check
    /// will hide the tuple from snapshots that observe `xmax` as committed.
    ///
    /// If `opts.wal` is `Some`, a `RecordType::HeapDelete` record is appended
    /// to the sink after the in-place stamp succeeds. The page guard is
    /// dropped before the WAL append so a future blocking WAL writer cannot
    /// starve buffer-pool eviction.
    ///
    /// Payload encoding runs before the page mutation so an encode failure
    /// short-circuits without touching the page. If encoding succeeds but
    /// the WAL append later fails, the buffer pool is poisoned and
    /// [`HeapError::Wal`] is returned; callers must restart from WAL before
    /// accepting more work.
    pub fn delete(&self, tid: TupleId, opts: DeleteOptions<'_>) -> Result<(), HeapError> {
        // Encode the WAL payload BEFORE the page mutation so that an encode
        // failure cleanly aborts without touching the page.
        let wal_record = if let Some(sink) = opts.wal {
            // Emit a full-page-write record if this is the first mutation of
            // the page since the last checkpoint. FPW must precede the mutation
            // record so recovery can restore the page before applying the delete.
            Self::maybe_emit_fpw(
                &self.pool,
                tid.page,
                sink,
                &self.last_checkpoint_lsn,
                opts.xmax,
            )?;
            let prev_lsn = sink.last_lsn_for(opts.xmax);
            let payload_bytes = HeapDeletePayload {
                tid,
                xmax: opts.xmax,
                cmax: opts.cmax,
            }
            .encode()?;
            let record = WalRecord::new(
                RecordType::HeapDelete,
                opts.xmax,
                prev_lsn,
                0,
                payload_bytes,
            )?;
            Some((sink, record))
        } else {
            None
        };

        // Mutate the page. Guard is dropped at the end of this block so
        // the pin is released before WAL I/O begins.
        {
            let guard = self.get_page_relieved(tid.page)?;
            Self::delete_in_place(&guard, tid, opts.xmax, opts.cmax)?;
            self.remember_rollback_stamp_page(opts.xmax, tid.page);
            // guard drops here — pin released before WAL append
        }

        // Append the WAL record outside the pin scope. If append returns
        // Err the page has already been mutated; poison the buffer pool and
        // return a fatal WAL error so the service can restart from WAL.
        if let Some((sink, record)) = wal_record {
            let lsn: Lsn = Self::append_after_page_mutation(&self.pool, sink, record)?;
            // Stamp the page LSN so recovery knows the on-page state was
            // logged at this LSN. WAL append completes before stamp so the
            // page LSN is never ahead of the WAL.
            Self::stamp_page_lsn(&self.pool, tid.page, lsn)?;
        }
        // Update FSM (optimistically record the dead tuple's space as free so
        // future inserters can find this block) and clear the VM all-visible
        // bit (the page now has a deleted tuple invisible to future snapshots).
        Self::post_delete_fsm_vm(&self.pool, tid.page, opts);
        // Invalidate the columnar projection cache for this
        // relation — a mutated row makes any cached `Vec<Column>`
        // stale until the next `SeqScan` re-builds it.
        self.column_cache.bump_version(tid.page.relation, opts.xmax);
        Ok(())
    }

    /// Bulk-delete every tuple in `tids`, grouped by page so each
    /// affected page is pinned and write-locked **exactly once**.
    ///
    /// [`Self::delete`] pins, write-locks, mutates and releases a
    /// page on every row. For a bulk DELETE over `N` rows on `P`
    /// pages that is `N` `DashMap` shard probes + `N` pin/unpin
    /// pairs + `N` write-lock acquisitions when only `P` are
    /// strictly necessary. `delete_many` groups the input by
    /// `page_id`, takes **one** write guard per page, stamps every
    /// slot on that page under that single guard, then drops the
    /// guard before its WAL append / FSM hook batch.
    ///
    /// Semantics are equivalent to invoking [`Self::delete`] N
    /// times in order: each tuple's header is stamped with
    /// `opts.xmax` / `opts.cmax`; WAL emission, when configured,
    /// emits one `HeapDelete` record per stamped slot (the WAL
    /// applier replays them identically to `delete`); FSM hints
    /// and VM clears, when `opts.fsm` / `opts.vm` are configured,
    /// run **once per page touched** (record the final free-space
    /// after every delete on the page lands).
    ///
    /// Slots within a page are stamped in ascending slot order; the
    /// between-page order is the iteration order of the
    /// page-grouping `AHashMap`, which is non-deterministic. Per-
    /// tuple deletes have no ordering-dependent semantics so this is
    /// safe.
    ///
    /// Returns the number of slots successfully stamped.
    ///
    /// # Errors
    ///
    /// - [`HeapError::BufferPool`] on pin failure for any affected page.
    /// - [`HeapError::Page`] / [`HeapError::MalformedHeader`] on slot
    ///   decode failure.
    /// - [`HeapError::WalPayload`] on WAL encode failure (encode happens
    ///   before the page is mutated, so the page is left untouched).
    ///
    /// # Concurrency
    ///
    /// At most one [`PageGuard`] is held at any instant. The guard is
    /// dropped before WAL I/O begins, so a concurrent reader on
    /// another page is never blocked by this method's pin.
    pub fn delete_many<I>(&self, tids: I, opts: DeleteOptions<'_>) -> Result<usize, HeapError>
    where
        I: IntoIterator<Item = TupleId>,
    {
        // Group TIDs by page. `ahash::AHashMap` is the workspace
        // default hash table; `PageId` already hashes well.
        let mut by_page: ahash::AHashMap<PageId, Vec<u16>> = ahash::AHashMap::new();
        for tid in tids {
            by_page.entry(tid.page).or_default().push(tid.slot);
        }
        if by_page.is_empty() {
            return Ok(0);
        }

        let mut total = 0_usize;
        for (page_id, mut slots) in by_page {
            // Sort within a page so the slot directory is touched in
            // ascending order — keeps page cache lines hot.
            slots.sort_unstable();

            // Pre-encode WAL payloads BEFORE mutating the page so an
            // encode failure aborts cleanly (the contract `delete`
            // upholds for the single-tuple case).
            let wal_payloads: Option<Vec<Vec<u8>>> = if let Some(sink) = opts.wal {
                Self::maybe_emit_fpw(
                    &self.pool,
                    page_id,
                    sink,
                    &self.last_checkpoint_lsn,
                    opts.xmax,
                )?;
                let mut payloads = Vec::with_capacity(slots.len());
                for &slot in &slots {
                    let tid = TupleId::new(page_id, slot);
                    let bytes = HeapDeletePayload {
                        tid,
                        xmax: opts.xmax,
                        cmax: opts.cmax,
                    }
                    .encode()?;
                    payloads.push(bytes);
                }
                Some(payloads)
            } else {
                None
            };

            // Mutate every slot on this page under one write guard.
            {
                let guard = self.get_page_relieved(page_id)?;
                for &slot in &slots {
                    let tid = TupleId::new(page_id, slot);
                    Self::delete_in_place(&guard, tid, opts.xmax, opts.cmax)?;
                }
                self.remember_rollback_stamp_page(opts.xmax, page_id);
                // guard drops here — pin released before WAL append.
            }

            // Append every per-tuple WAL record outside the pin scope.
            // The page LSN is stamped once at the final LSN of the
            // batch (recovery replays records in append order, so the
            // final per-slot stamp is the only state recovery needs).
            if let (Some(sink), Some(payloads)) = (opts.wal, wal_payloads) {
                let mut last_lsn: Lsn = Lsn::ZERO;
                for payload in payloads {
                    let prev_lsn = sink.last_lsn_for(opts.xmax);
                    let record =
                        WalRecord::new(RecordType::HeapDelete, opts.xmax, prev_lsn, 0, payload)?;
                    last_lsn = Self::append_after_page_mutation(&self.pool, sink, record)?;
                }
                Self::stamp_page_lsn(&self.pool, page_id, last_lsn)?;
            }

            // FSM/VM hooks fire once per page touched.
            Self::post_delete_fsm_vm(&self.pool, page_id, opts);
            // Column-cache invalidation: bump the relation's version
            // for every page we touch. The first bump invalidates the
            // entry; subsequent bumps just move the version forward.
            self.column_cache.bump_version(page_id.relation, opts.xmax);
            total += slots.len();
        }
        Ok(total)
    }

    /// Single-pass MVCC-correct DELETE for the narrow
    /// `(Int32, Int32) [WHERE col_j cmp lit]` shape.
    ///
    /// Mirrors [`Self::update_int32_pair_inplace_undo`]: page-major
    /// traversal, one source-page write guard at a time, ItemId +
    /// minimal-visibility + payload decode inline. The slot's
    /// payload is unchanged (DELETE leaves the bytes intact and uses
    /// `xmax` to hide the tuple); only the header's `xmax / cmax /
    /// infomask` triple is stamped.
    ///
    /// What this saves versus `ModifyTable(Filter(SeqScan))` →
    /// `delete_many`:
    /// - No intermediate `Vec<TupleId>` of qualifying TIDs.
    /// - No per-page `AHashMap<PageId, Vec<u16>>` grouping pass.
    /// - One write-pin per source page; the prior plan paid one for
    ///   the scan's visibility walker and one for the stamp pass.
    ///
    /// # Concurrency
    ///
    /// Holds **one** write-exclusive page guard at a time.
    ///
    /// # Durability
    ///
    /// When `wal` is `Some`, one
    /// [`RecordType::HeapDeleteInPlaceBatch`] record is appended per
    /// mutated page after the page guard is dropped and the page LSN is
    /// stamped with that batch LSN, mirroring the FPW + page-batch +
    /// page-LSN pattern in [`Self::update_int32_pair_inplace_undo`].
    /// A `None` value
    /// retains the non-durable benchmark path for the executor's
    /// fused operator (the pipeline lowerer threads the live sink in
    /// when present).
    #[inline]
    pub fn delete_int32_pair_inplace<O, P>(
        &self,
        scan: DeleteInt32PairScan<'_, O, P>,
        stamp: DeleteInt32PairStamp,
        wal: Option<&dyn WalSink>,
        vm: Option<&crate::vm::VisibilityMap>,
    ) -> Result<usize, HeapError>
    where
        O: XidStatusOracle + ?Sized,
        P: Int32PairPredicateEval,
    {
        use crate::page::{ITEMID_SIZE, PAGE_HEADER_SIZE};

        let DeleteInt32PairScan {
            rel,
            block_count,
            snapshot,
            oracle,
            predicate,
        } = scan;
        let DeleteInt32PairStamp { xid, command_id } = stamp;
        let mut total_deleted: usize = 0;
        let mut visibility_cache: Option<DeleteVisibilityCache> = None;
        let predicate_plan = delete_predicate_plan(&predicate)?;
        let xid_bytes = xid.raw().to_le_bytes();
        let cmd_bytes = command_id.raw().to_le_bytes();
        let vm = vm.filter(|vm| vm.contains_relation(rel));

        // Per-page slot scratch: collect under the write guard, emit
        // WAL once the guard is dropped, same shape as the update
        // path. Reused across pages.
        let mut wal_scratch =
            DeleteSlotWalScratch::with_capacity(if wal.is_some() { 256 } else { 0 });
        let mut wal_payload_buf: Vec<u8> = if wal.is_some() {
            Vec::with_capacity(512)
        } else {
            Vec::new()
        };
        let mut delete_prev_lsn = match wal {
            Some(sink) if self.last_checkpoint_lsn.load(Ordering::Acquire) == 0 => {
                Some(sink.last_lsn_for(xid))
            }
            _ => None,
        };
        let wal_appends_before_stamp = wal.is_some_and(WalSink::appends_without_blocking_io);
        let mut stamp_offsets: Vec<u16> = if wal_appends_before_stamp {
            Vec::with_capacity(256)
        } else {
            Vec::new()
        };

        for src_block in 0..block_count {
            let src_page_id = PageId::new(rel, BlockNumber::new(src_block));
            let mut page_deleted = false;
            wal_scratch.clear();
            stamp_offsets.clear();

            // FPW: emit the canonical page image first if this is
            // the first mutation since the last checkpoint. Matches
            // the contract used by `delete_many` and
            // `update_int32_pair_inplace_undo`.
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
            {
                let src_bytes = src_page.as_bytes_mut();
                let src_slot_count = {
                    let hdr =
                        crate::page::PageHeader::decode(src_bytes).map_err(HeapError::Page)?;
                    hdr.slot_count()
                };
                let page_predicate_all_matches = self
                    .int32_pair_payload_stats
                    .get(&src_page_id)
                    .is_some_and(|stats| {
                        int32_pair_stats_prove_all_match(*stats, src_slot_count, predicate_plan)
                    });
                let mut stats_builder =
                    if matches!(predicate_plan, DeletePredicatePlan::ColumnCmp { .. })
                        && !page_predicate_all_matches
                    {
                        Some(Int32PairPagePayloadStatsBuilder::new())
                    } else {
                        None
                    };

                for src_slot in 0..src_slot_count {
                    let item_id_off = PAGE_HEADER_SIZE + usize::from(src_slot) * ITEMID_SIZE;
                    let item_raw = read_u32_at(src_bytes, item_id_off);
                    if item_raw & 0b11 != 1 {
                        continue;
                    }
                    let (length, offset) = itemid_window(item_raw)?;
                    let tuple_end = offset + length;
                    if length < TUPLE_HEADER_SIZE || tuple_end > src_bytes.len() {
                        return Err(HeapError::MalformedHeader("slot shorter than header"));
                    }

                    let xmin_raw = read_u64_at(src_bytes, offset);
                    let xmax_raw = read_u64_at(src_bytes, offset + 8);
                    let command_field_raw = read_u64_at(src_bytes, offset + 16);
                    let infomask_bits = read_u16_at(src_bytes, offset + 24);

                    let visibility = match visibility_cache {
                        Some(cache)
                            if cache.matches(
                                xmin_raw,
                                xmax_raw,
                                command_field_raw,
                                infomask_bits,
                            ) =>
                        {
                            cache.visibility
                        }
                        _ => {
                            let (h, _) =
                                TupleHeader::decode(&src_bytes[offset..offset + TUPLE_HEADER_SIZE])
                                    .ok_or(HeapError::MalformedHeader("header decode failed"))?;
                            let visibility = is_visible(&h, snapshot, oracle);
                            visibility_cache = Some(DeleteVisibilityCache {
                                xmin_raw: h.xmin.raw(),
                                xmax_raw: h.xmax.raw(),
                                command_raw: command_field_raw,
                                infomask_bits: h.infomask.bits(),
                                visibility,
                            });
                            visibility
                        }
                    };
                    if !matches!(visibility, Visibility::Visible) {
                        continue;
                    }

                    let payload_off = offset + TUPLE_HEADER_SIZE;
                    if let Some(builder) = stats_builder.as_mut() {
                        let payload_end = payload_off
                            .checked_add(9)
                            .ok_or(HeapError::MalformedHeader("int32 pair payload overflow"))?;
                        if payload_end > tuple_end {
                            return Err(HeapError::MalformedHeader(
                                "payload shorter than (Int32, Int32)",
                            ));
                        }
                        let id = read_i32_at(src_bytes, payload_off + 1);
                        let val = read_i32_at(src_bytes, payload_off + 5);
                        builder.observe(id, val)?;
                    }
                    if !page_predicate_all_matches
                        && !int32_pair_delete_predicate_matches_planned(
                            src_bytes,
                            payload_off,
                            tuple_end,
                            predicate_plan,
                            &predicate,
                        )?
                    {
                        continue;
                    }

                    // Write-write conflict: the row is visible to us but a
                    // concurrent in-flight transaction has already stamped xmax
                    // (an unresolved delete/update). Stamping over it would lose
                    // that writer's mark (a lost delete). The fused DELETE path
                    // does not wait+recheck, so surface a retryable serialization
                    // failure (SQLSTATE 40001), mirroring the fused UPDATE path.
                    // An aborted xmax is not in progress, so a delete over a
                    // rolled-back deleter still proceeds.
                    let prior_xmax = Xid::new(xmax_raw);
                    if !prior_xmax.is_invalid()
                        && prior_xmax != xid
                        && oracle.is_in_progress(prior_xmax)
                    {
                        return Err(HeapError::WriteConflict(
                            "in-place tuple has an unresolved writer",
                        ));
                    }

                    if wal.is_some() {
                        wal_scratch.push(src_slot)?;
                    }
                    if wal_appends_before_stamp {
                        let offset_u16 = u16::try_from(offset)
                            .map_err(|_| HeapError::MalformedHeader("tuple offset overflow"))?;
                        stamp_offsets.push(offset_u16);
                    } else {
                        stamp_delete_int32_pair_header(
                            src_bytes,
                            offset,
                            infomask_bits,
                            &xid_bytes,
                            &cmd_bytes,
                        );
                    }

                    total_deleted += 1;
                    page_deleted = true;
                }
                if let Some(builder) = stats_builder
                    && let Some(stats) = builder.finish(src_slot_count)
                {
                    self.int32_pair_payload_stats.insert(src_page_id, stats);
                }
            }

            let mut guard_appended_lsn = None;
            if let Some(sink) = wal
                && wal_appends_before_stamp
                && !wal_scratch.is_empty()
            {
                let prev_lsn = delete_prev_lsn.unwrap_or_else(|| sink.last_lsn_for(xid));
                let lsn = match wal_scratch.view() {
                    DeleteSlotWalView::Range {
                        first_slot,
                        slot_count,
                    } => Self::emit_delete_in_place_range_batch_wal_before_reuse(
                        sink,
                        src_page_id,
                        xid,
                        command_id,
                        first_slot,
                        slot_count,
                        &mut wal_payload_buf,
                        prev_lsn,
                    )?,
                    DeleteSlotWalView::Sparse(slots) => {
                        Self::emit_delete_in_place_batch_wal_before_reuse(
                            sink,
                            src_page_id,
                            xid,
                            command_id,
                            slots,
                            &mut wal_payload_buf,
                            prev_lsn,
                        )?
                    }
                    DeleteSlotWalView::Empty => continue,
                };
                delete_prev_lsn = Some(lsn);
                guard_appended_lsn = Some(lsn);
            }

            if let Some(lsn) = guard_appended_lsn {
                let src_bytes = src_page.as_bytes_mut();
                for &offset in &stamp_offsets {
                    let offset = usize::from(offset);
                    let infomask_bits = read_u16_at(src_bytes, offset + 24);
                    stamp_delete_int32_pair_header(
                        src_bytes,
                        offset,
                        infomask_bits,
                        &xid_bytes,
                        &cmd_bytes,
                    );
                }
                src_page.set_lsn(lsn.raw());
                wal_scratch.clear();
                stamp_offsets.clear();
            }

            drop(src_page);
            drop(src_guard);

            // Emit one WAL record for every stamped slot on this page with the
            // page guard dropped when the sink can block.
            if let Some(sink) = wal {
                if !wal_scratch.is_empty() {
                    let prev_lsn = delete_prev_lsn.unwrap_or_else(|| sink.last_lsn_for(xid));
                    let lsn = match wal_scratch.view() {
                        DeleteSlotWalView::Range {
                            first_slot,
                            slot_count,
                        } => Self::emit_delete_in_place_range_batch_wal_reuse_after(
                            &self.pool,
                            sink,
                            src_page_id,
                            xid,
                            command_id,
                            first_slot,
                            slot_count,
                            &mut wal_payload_buf,
                            prev_lsn,
                        )?,
                        DeleteSlotWalView::Sparse(slots) => {
                            Self::emit_delete_in_place_batch_wal_reuse_after(
                                &self.pool,
                                sink,
                                src_page_id,
                                xid,
                                command_id,
                                slots,
                                &mut wal_payload_buf,
                                prev_lsn,
                            )?
                        }
                        DeleteSlotWalView::Empty => continue,
                    };
                    delete_prev_lsn = Some(lsn);
                    Self::stamp_page_lsn(&self.pool, src_page_id, lsn)?;
                }
                wal_scratch.clear();
            }
            if page_deleted && let Some(vm) = vm {
                vm.clear(src_page_id.relation, src_page_id.block);
            }
            if page_deleted {
                self.remember_rollback_stamp_page(xid, src_page_id);
            }
        }

        if total_deleted > 0 {
            self.column_cache.bump_version(rel, xid);
        }

        Ok(total_deleted)
    }

    /// Parallel WAL-backed variant for large fused `(Int32, Int32)` DELETEs.
    ///
    /// Each worker owns a disjoint block range. WAL appends are serialized so
    /// `prev_lsn` remains a real per-transaction chain; page scans and tuple
    /// stamping run in parallel. The method uses this path only for
    /// nonblocking WAL sinks before the first checkpoint, where each page
    /// mutation can append its compact page-local record while holding the page
    /// write guard and then stamp the page with the returned LSN. Other cases
    /// fall back to the sequential WAL path.
    pub fn delete_int32_pair_inplace_parallel_wal<O, P>(
        &self,
        scan: DeleteInt32PairScan<'_, O, P>,
        stamp: DeleteInt32PairStamp,
        wal: &dyn WalSink,
        vm: Option<&crate::vm::VisibilityMap>,
    ) -> Result<usize, HeapError>
    where
        O: XidStatusOracle + Sync + ?Sized,
        P: Int32PairPredicateEval + Sync,
    {
        let available_workers = std::thread::available_parallelism().map_or(1, |n| n.get());
        let DeleteInt32PairScan {
            rel,
            block_count,
            snapshot,
            oracle,
            predicate,
        } = scan;
        if block_count < Self::PARALLEL_WAL_DELETE_MIN_BLOCKS
            || available_workers <= 1
            || !wal.appends_without_blocking_io()
            || self.last_checkpoint_lsn.load(Ordering::Acquire) != 0
        {
            return self.delete_int32_pair_inplace(
                DeleteInt32PairScan {
                    rel,
                    block_count,
                    snapshot,
                    oracle,
                    predicate,
                },
                stamp,
                Some(wal),
                vm,
            );
        }
        let DeleteInt32PairStamp { xid, command_id } = stamp;

        let block_count_usize = usize::try_from(block_count)
            .map_err(|_| HeapError::MalformedHeader("block count overflow"))?;
        let blocks_per_worker = usize::try_from(Self::PARALLEL_WAL_DELETE_BLOCKS_PER_WORKER)
            .map_err(|_| HeapError::MalformedHeader("delete worker block overflow"))?;
        let workers = available_workers
            .min(block_count_usize.div_ceil(blocks_per_worker))
            .min(block_count_usize)
            .max(1);
        if workers <= 1 {
            return self.delete_int32_pair_inplace(
                DeleteInt32PairScan {
                    rel,
                    block_count,
                    snapshot,
                    oracle,
                    predicate,
                },
                stamp,
                Some(wal),
                vm,
            );
        }

        let workers_u32 =
            u32::try_from(workers).map_err(|_| HeapError::MalformedHeader("worker overflow"))?;
        let chunk_blocks = block_count.div_ceil(workers_u32).max(1);
        let predicate_ref = &predicate;
        let prev_lsn = parking_lot::Mutex::new(wal.last_lsn_for(xid));
        let mut total_deleted = 0_usize;

        std::thread::scope(|scope| {
            let mut handles = Vec::with_capacity(workers);
            let mut start_block = 0_u32;
            while start_block < block_count {
                let end_block = start_block.saturating_add(chunk_blocks).min(block_count);
                handles.push(scope.spawn({
                    let prev_lsn = &prev_lsn;
                    move || {
                        self.delete_int32_pair_range_wal(DeleteInt32PairWalRange {
                            rel,
                            start_block,
                            end_block,
                            snapshot,
                            oracle,
                            predicate: predicate_ref,
                            xid,
                            command_id,
                            wal,
                            prev_lsn,
                            vm,
                        })
                    }
                }));
                start_block = end_block;
            }

            for handle in handles {
                let deleted = handle.join().map_err(|_| {
                    HeapError::MalformedHeader("parallel WAL delete worker panicked")
                })??;
                total_deleted =
                    checked_heap_count_add(total_deleted, deleted, "deleted tuple count overflow")?;
            }
            Ok::<(), HeapError>(())
        })?;

        if total_deleted > 0 {
            self.column_cache.bump_version(rel, xid);
        }

        Ok(total_deleted)
    }

    /// Parallel no-WAL variant for large in-memory fused `(Int32, Int32)`
    /// DELETEs.
    ///
    /// WAL-backed deletes stay sequential so per-transaction WAL chain ordering
    /// remains unchanged. Without WAL, each worker owns a disjoint page range
    /// and stamps matching visible tuples under the same MVCC rules as
    /// [`Self::delete_int32_pair_inplace`].
    pub fn delete_int32_pair_inplace_parallel_no_wal<O, P>(
        &self,
        scan: DeleteInt32PairScan<'_, O, P>,
        stamp: DeleteInt32PairStamp,
        vm: Option<&crate::vm::VisibilityMap>,
    ) -> Result<usize, HeapError>
    where
        O: XidStatusOracle + Sync + ?Sized,
        P: Int32PairPredicateEval + Sync,
    {
        let available_workers = std::thread::available_parallelism().map_or(1, |n| n.get());
        let DeleteInt32PairScan {
            rel,
            block_count,
            snapshot,
            oracle,
            predicate,
        } = scan;
        if block_count < 2_048 || available_workers <= 1 {
            return self.delete_int32_pair_inplace(
                DeleteInt32PairScan {
                    rel,
                    block_count,
                    snapshot,
                    oracle,
                    predicate,
                },
                stamp,
                None,
                vm,
            );
        }
        let DeleteInt32PairStamp { xid, command_id } = stamp;

        let block_count_usize = usize::try_from(block_count)
            .map_err(|_| HeapError::MalformedHeader("block count overflow"))?;
        let workers = available_workers
            .min(block_count_usize.div_ceil(512))
            .min(block_count_usize)
            .max(1);
        if workers <= 1 {
            return self.delete_int32_pair_inplace(
                DeleteInt32PairScan {
                    rel,
                    block_count,
                    snapshot,
                    oracle,
                    predicate,
                },
                stamp,
                None,
                vm,
            );
        }

        let workers_u32 =
            u32::try_from(workers).map_err(|_| HeapError::MalformedHeader("worker overflow"))?;
        let chunk_blocks = block_count.div_ceil(workers_u32).max(1);
        let predicate_ref = &predicate;
        let mut total_deleted = 0_usize;

        std::thread::scope(|scope| {
            let mut handles = Vec::with_capacity(workers);
            let mut start_block = 0_u32;
            while start_block < block_count {
                let end_block = start_block.saturating_add(chunk_blocks).min(block_count);
                handles.push(scope.spawn(move || {
                    self.delete_int32_pair_range_no_wal(DeleteInt32PairRange {
                        rel,
                        start_block,
                        end_block,
                        snapshot,
                        oracle,
                        predicate: predicate_ref,
                        xid,
                        command_id,
                        vm,
                    })
                }));
                start_block = end_block;
            }

            for handle in handles {
                let deleted = handle
                    .join()
                    .map_err(|_| HeapError::MalformedHeader("parallel delete worker panicked"))??;
                total_deleted =
                    checked_heap_count_add(total_deleted, deleted, "deleted tuple count overflow")?;
            }
            Ok::<(), HeapError>(())
        })?;

        if total_deleted > 0 {
            self.column_cache.bump_version(rel, xid);
        }

        Ok(total_deleted)
    }

    fn delete_int32_pair_range_wal<O, P>(
        &self,
        request: DeleteInt32PairWalRange<'_, O, P>,
    ) -> Result<usize, HeapError>
    where
        O: XidStatusOracle + ?Sized,
        P: Int32PairPredicateEval + ?Sized,
    {
        use crate::page::{ITEMID_SIZE, PAGE_HEADER_SIZE};

        let DeleteInt32PairWalRange {
            rel,
            start_block,
            end_block,
            snapshot,
            oracle,
            predicate,
            xid,
            command_id,
            wal,
            prev_lsn,
            vm,
        } = request;
        let mut total_deleted: usize = 0;
        let mut visibility_cache: Option<DeleteVisibilityCache> = None;
        let predicate_plan = delete_predicate_plan(predicate)?;
        let xid_bytes = xid.raw().to_le_bytes();
        let cmd_bytes = command_id.raw().to_le_bytes();
        let vm = vm.filter(|vm| vm.contains_relation(rel));
        let mut wal_scratch = DeleteSlotWalScratch::with_capacity(256);
        let mut wal_payload_buf = Vec::with_capacity(512);
        let mut stamp_offsets = Vec::with_capacity(256);

        for src_block in start_block..end_block {
            let src_page_id = PageId::new(rel, BlockNumber::new(src_block));
            let mut page_deleted = false;
            wal_scratch.clear();
            stamp_offsets.clear();

            let src_guard = self.get_page_relieved(src_page_id)?;
            let mut src_page = src_guard.write();
            {
                let src_bytes = src_page.as_bytes_mut();
                let src_slot_count = {
                    let hdr =
                        crate::page::PageHeader::decode(src_bytes).map_err(HeapError::Page)?;
                    hdr.slot_count()
                };
                let page_predicate_all_matches = self
                    .int32_pair_payload_stats
                    .get(&src_page_id)
                    .is_some_and(|stats| {
                        int32_pair_stats_prove_all_match(*stats, src_slot_count, predicate_plan)
                    });
                let mut stats_builder =
                    if matches!(predicate_plan, DeletePredicatePlan::ColumnCmp { .. })
                        && !page_predicate_all_matches
                    {
                        Some(Int32PairPagePayloadStatsBuilder::new())
                    } else {
                        None
                    };

                for src_slot in 0..src_slot_count {
                    let item_id_off = PAGE_HEADER_SIZE + usize::from(src_slot) * ITEMID_SIZE;
                    let item_raw = read_u32_at(src_bytes, item_id_off);
                    if item_raw & 0b11 != 1 {
                        continue;
                    }
                    let (length, offset) = itemid_window(item_raw)?;
                    let tuple_end = offset + length;
                    if length < TUPLE_HEADER_SIZE || tuple_end > src_bytes.len() {
                        return Err(HeapError::MalformedHeader("slot shorter than header"));
                    }

                    let xmin_raw = read_u64_at(src_bytes, offset);
                    let xmax_raw = read_u64_at(src_bytes, offset + 8);
                    let command_field_raw = read_u64_at(src_bytes, offset + 16);
                    let infomask_bits = read_u16_at(src_bytes, offset + 24);

                    let visibility = match visibility_cache {
                        Some(cache)
                            if cache.matches(
                                xmin_raw,
                                xmax_raw,
                                command_field_raw,
                                infomask_bits,
                            ) =>
                        {
                            cache.visibility
                        }
                        _ => {
                            let (h, _) =
                                TupleHeader::decode(&src_bytes[offset..offset + TUPLE_HEADER_SIZE])
                                    .ok_or(HeapError::MalformedHeader("header decode failed"))?;
                            let visibility = is_visible(&h, snapshot, oracle);
                            visibility_cache = Some(DeleteVisibilityCache {
                                xmin_raw: h.xmin.raw(),
                                xmax_raw: h.xmax.raw(),
                                command_raw: command_field_raw,
                                infomask_bits: h.infomask.bits(),
                                visibility,
                            });
                            visibility
                        }
                    };
                    if !matches!(visibility, Visibility::Visible) {
                        continue;
                    }

                    let payload_off = offset + TUPLE_HEADER_SIZE;
                    if let Some(builder) = stats_builder.as_mut() {
                        let payload_end = payload_off
                            .checked_add(9)
                            .ok_or(HeapError::MalformedHeader("int32 pair payload overflow"))?;
                        if payload_end > tuple_end {
                            return Err(HeapError::MalformedHeader(
                                "payload shorter than (Int32, Int32)",
                            ));
                        }
                        let id = read_i32_at(src_bytes, payload_off + 1);
                        let val = read_i32_at(src_bytes, payload_off + 5);
                        builder.observe(id, val)?;
                    }
                    if !page_predicate_all_matches
                        && !int32_pair_delete_predicate_matches_planned(
                            src_bytes,
                            payload_off,
                            tuple_end,
                            predicate_plan,
                            predicate,
                        )?
                    {
                        continue;
                    }

                    // Write-write conflict (see delete_int32_pair_inplace): a
                    // visible row whose xmax names a foreign in-flight writer
                    // must not be stamped over (lost delete); raise a retryable
                    // 40001 instead of waiting/recheck.
                    let prior_xmax = Xid::new(xmax_raw);
                    if !prior_xmax.is_invalid()
                        && prior_xmax != xid
                        && oracle.is_in_progress(prior_xmax)
                    {
                        return Err(HeapError::WriteConflict(
                            "in-place tuple has an unresolved writer",
                        ));
                    }

                    wal_scratch.push(src_slot)?;
                    let offset_u16 = u16::try_from(offset)
                        .map_err(|_| HeapError::MalformedHeader("tuple offset overflow"))?;
                    stamp_offsets.push(offset_u16);

                    total_deleted += 1;
                    page_deleted = true;
                }
                if let Some(builder) = stats_builder
                    && let Some(stats) = builder.finish(src_slot_count)
                {
                    self.int32_pair_payload_stats.insert(src_page_id, stats);
                }
            }

            if !wal_scratch.is_empty() {
                let lsn = {
                    let mut prev_lsn_guard = prev_lsn.lock();
                    let lsn = match wal_scratch.view() {
                        DeleteSlotWalView::Range {
                            first_slot,
                            slot_count,
                        } => Self::emit_delete_in_place_range_batch_wal_before_reuse(
                            wal,
                            src_page_id,
                            xid,
                            command_id,
                            first_slot,
                            slot_count,
                            &mut wal_payload_buf,
                            *prev_lsn_guard,
                        )?,
                        DeleteSlotWalView::Sparse(slots) => {
                            Self::emit_delete_in_place_batch_wal_before_reuse(
                                wal,
                                src_page_id,
                                xid,
                                command_id,
                                slots,
                                &mut wal_payload_buf,
                                *prev_lsn_guard,
                            )?
                        }
                        DeleteSlotWalView::Empty => continue,
                    };
                    *prev_lsn_guard = lsn;
                    lsn
                };

                let src_bytes = src_page.as_bytes_mut();
                for &offset in &stamp_offsets {
                    let offset = usize::from(offset);
                    let infomask_bits = read_u16_at(src_bytes, offset + 24);
                    stamp_delete_int32_pair_header(
                        src_bytes,
                        offset,
                        infomask_bits,
                        &xid_bytes,
                        &cmd_bytes,
                    );
                }
                src_page.set_lsn(lsn.raw());
            }

            drop(src_page);
            drop(src_guard);

            if page_deleted && let Some(vm) = vm {
                vm.clear(src_page_id.relation, src_page_id.block);
            }
            if page_deleted {
                self.remember_rollback_stamp_page(xid, src_page_id);
            }
        }

        Ok(total_deleted)
    }

    fn delete_int32_pair_range_no_wal<O, P>(
        &self,
        request: DeleteInt32PairRange<'_, O, P>,
    ) -> Result<usize, HeapError>
    where
        O: XidStatusOracle + ?Sized,
        P: Int32PairPredicateEval + ?Sized,
    {
        use crate::page::{ITEMID_SIZE, PAGE_HEADER_SIZE};

        let DeleteInt32PairRange {
            rel,
            start_block,
            end_block,
            snapshot,
            oracle,
            predicate,
            xid,
            command_id,
            vm,
        } = request;
        let mut total_deleted: usize = 0;
        let mut visibility_cache: Option<DeleteVisibilityCache> = None;
        let predicate_plan = delete_predicate_plan(predicate)?;
        let xid_bytes = xid.raw().to_le_bytes();
        let cmd_bytes = command_id.raw().to_le_bytes();
        let vm = vm.filter(|vm| vm.contains_relation(rel));
        for src_block in start_block..end_block {
            let src_page_id = PageId::new(rel, BlockNumber::new(src_block));
            let mut page_deleted = false;

            let src_guard = self.get_page_relieved(src_page_id)?;
            let mut src_page = src_guard.write();
            let src_bytes = src_page.as_bytes_mut();
            let src_slot_count = {
                let hdr = crate::page::PageHeader::decode(src_bytes).map_err(HeapError::Page)?;
                hdr.slot_count()
            };

            for src_slot in 0..src_slot_count {
                let item_id_off = PAGE_HEADER_SIZE + usize::from(src_slot) * ITEMID_SIZE;
                let item_raw = read_u32_at(src_bytes, item_id_off);
                if item_raw & 0b11 != 1 {
                    continue;
                }
                let (length, offset) = itemid_window(item_raw)?;
                let tuple_end = offset + length;
                if length < TUPLE_HEADER_SIZE || tuple_end > src_bytes.len() {
                    return Err(HeapError::MalformedHeader("slot shorter than header"));
                }

                let xmin_raw = read_u64_at(src_bytes, offset);
                let xmax_raw = read_u64_at(src_bytes, offset + 8);
                let command_field_raw = read_u64_at(src_bytes, offset + 16);
                let infomask_bits = read_u16_at(src_bytes, offset + 24);

                let visibility = match visibility_cache {
                    Some(cache)
                        if cache.matches(xmin_raw, xmax_raw, command_field_raw, infomask_bits) =>
                    {
                        cache.visibility
                    }
                    _ => {
                        let (h, _) =
                            TupleHeader::decode(&src_bytes[offset..offset + TUPLE_HEADER_SIZE])
                                .ok_or(HeapError::MalformedHeader("header decode failed"))?;
                        let visibility = is_visible(&h, snapshot, oracle);
                        visibility_cache = Some(DeleteVisibilityCache {
                            xmin_raw: h.xmin.raw(),
                            xmax_raw: h.xmax.raw(),
                            command_raw: command_field_raw,
                            infomask_bits: h.infomask.bits(),
                            visibility,
                        });
                        visibility
                    }
                };
                if !matches!(visibility, Visibility::Visible) {
                    continue;
                }

                let payload_off = offset + TUPLE_HEADER_SIZE;
                if !int32_pair_delete_predicate_matches_planned(
                    src_bytes,
                    payload_off,
                    tuple_end,
                    predicate_plan,
                    predicate,
                )? {
                    continue;
                }

                // Write-write conflict (see delete_int32_pair_inplace): a
                // visible row whose xmax names a foreign in-flight writer must
                // not be stamped over (lost delete); raise a retryable 40001
                // instead of waiting/recheck.
                let prior_xmax = Xid::new(xmax_raw);
                if !prior_xmax.is_invalid()
                    && prior_xmax != xid
                    && oracle.is_in_progress(prior_xmax)
                {
                    return Err(HeapError::WriteConflict(
                        "in-place tuple has an unresolved writer",
                    ));
                }

                src_bytes[offset + 8..offset + 16].copy_from_slice(&xid_bytes);
                src_bytes[offset + 20..offset + 24].copy_from_slice(&cmd_bytes);
                let new_infomask = infomask_bits | InfoMask::UPDATED;
                src_bytes[offset + 24..offset + 26].copy_from_slice(&new_infomask.to_le_bytes());

                total_deleted += 1;
                page_deleted = true;
            }

            drop(src_page);
            drop(src_guard);

            if page_deleted && let Some(vm) = vm {
                vm.clear(src_page_id.relation, src_page_id.block);
            }
            if page_deleted {
                self.remember_rollback_stamp_page(xid, src_page_id);
            }
        }

        Ok(total_deleted)
    }

    /// Apply a deletion stamp to the tuple identified by `tid` while
    /// holding `guard`'s exclusive write lock.
    ///
    /// The buffer pool exposes only `read_tuple` (immutable) for
    /// payload access; we re-encode the header into a fresh buffer
    /// and overwrite the slot via the page's mutable bytes.
    /// `Page::insert_tuple` allocates a *new* slot — we want to
    /// overwrite the existing one in place. This is safe because:
    ///
    /// - The new header has the same size as the old one
    ///   ([`TUPLE_HEADER_SIZE`]).
    /// - The slot's `ItemId` offset/length is unchanged.
    /// - The payload trailing the header is untouched.
    ///
    /// If the page module grows an in-place `update_tuple_header`
    /// helper, we should migrate to it.
    ///
    pub(super) fn delete_in_place(
        guard: &PageGuard<L>,
        tid: TupleId,
        xmax: Xid,
        cmax: CommandId,
    ) -> Result<(), HeapError> {
        {
            let mut page = guard.write();
            let bytes = page.read_tuple(tid.slot)?;
            if bytes.len() < TUPLE_HEADER_SIZE {
                return Err(HeapError::MalformedHeader("slot shorter than header"));
            }
            let (mut header, _) = TupleHeader::decode(&bytes[..TUPLE_HEADER_SIZE])
                .ok_or(HeapError::MalformedHeader("header decode failed"))?;
            header.mark_deleted(xmax, cmax);
            let header_bytes = Self::collect_header_bytes(&header);

            let page_bytes = page.as_bytes_mut();
            let (slot_offset, slot_length) = Self::slot_window(page_bytes, tid.slot)?;
            if slot_length < TUPLE_HEADER_SIZE {
                return Err(HeapError::MalformedHeader("slot shorter than header"));
            }
            page_bytes[slot_offset..slot_offset + TUPLE_HEADER_SIZE].copy_from_slice(&header_bytes);
        }
        Ok(())
    }
}
