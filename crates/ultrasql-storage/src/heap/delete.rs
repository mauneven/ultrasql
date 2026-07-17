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

use crate::buffer_pool::{PageGuard, PageLoader, PageWrite};
use crate::wal_sink::WalSink;

use super::{
    DeleteOptions, HeapAccess, HeapError, Int32PairPagePayloadStats, UndoRelationLog,
    checked_heap_count_add, undo_pre_image_from_log,
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
    if payload_end <= tuple_end && bytes[payload_off] == 0 {
        return match plan {
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
        };
    }

    let (id, val) = int32_pair_nullable_payload_values(bytes, payload_off, tuple_end)?;
    Ok(match plan {
        DeletePredicatePlan::All => unreachable!("handled before payload validation"),
        DeletePredicatePlan::ColumnCmp {
            col_index: 0,
            op,
            literal,
        } => id.is_some_and(|id| op.check(id, literal)),
        DeletePredicatePlan::ColumnCmp {
            col_index: 1,
            op,
            literal,
        } => val.is_some_and(|val| op.check(val, literal)),
        DeletePredicatePlan::ColumnCmp { .. } => {
            unreachable!("predicate column validated before scan")
        }
        DeletePredicatePlan::Pair {
            required_col: Some(0),
        } => id.is_some_and(|id| predicate.matches_column(0, id)),
        DeletePredicatePlan::Pair {
            required_col: Some(1),
        } => val.is_some_and(|val| predicate.matches_column(1, val)),
        DeletePredicatePlan::Pair {
            required_col: Some(_),
        } => unreachable!("predicate column validated before scan"),
        DeletePredicatePlan::Pair { required_col: None } => id
            .zip(val)
            .is_some_and(|(id, val)| predicate.matches_pair(id, val)),
    })
}

#[inline]
fn int32_pair_nullable_payload_values(
    bytes: &[u8],
    payload_off: usize,
    tuple_end: usize,
) -> Result<(Option<i32>, Option<i32>), HeapError> {
    if payload_off >= tuple_end || tuple_end > bytes.len() {
        return Err(HeapError::MalformedHeader(
            "int32 pair payload is missing its null bitmap",
        ));
    }
    let null_bitmap = bytes[payload_off];
    let id_null = null_bitmap & 1 != 0;
    let val_null = null_bitmap & 2 != 0;
    let id_off = payload_off
        .checked_add(1)
        .ok_or(HeapError::MalformedHeader("int32 pair payload overflow"))?;
    let id = if id_null {
        None
    } else {
        Some(read_i32_payload_value(bytes, id_off, tuple_end)?)
    };
    let val_off = if id_null {
        id_off
    } else {
        id_off
            .checked_add(4)
            .ok_or(HeapError::MalformedHeader("int32 pair payload overflow"))?
    };
    let val = if val_null {
        None
    } else {
        Some(read_i32_payload_value(bytes, val_off, tuple_end)?)
    };
    Ok((id, val))
}

#[inline]
fn read_i32_payload_value(
    bytes: &[u8],
    value_off: usize,
    tuple_end: usize,
) -> Result<i32, HeapError> {
    let value_end = value_off
        .checked_add(4)
        .ok_or(HeapError::MalformedHeader("int32 pair payload overflow"))?;
    if value_end > tuple_end || value_end > bytes.len() {
        return Err(HeapError::MalformedHeader(
            "int32 pair payload is truncated",
        ));
    }
    Ok(read_i32_at(bytes, value_off))
}

fn delete_visibility_allows_current_mutation<O, P>(
    visibility: Visibility,
    undo_log: &parking_lot::RwLock<UndoRelationLog>,
    tid: TupleId,
    current_payload: &[u8],
    mvcc: (&Snapshot, &O),
    predicate_plan: DeletePredicatePlan,
    predicate: &P,
) -> Result<bool, HeapError>
where
    O: XidStatusOracle + ?Sized,
    P: Int32PairPredicateEval + ?Sized,
{
    let (snapshot, oracle) = mvcc;
    match visibility {
        Visibility::Visible => Ok(true),
        Visibility::Invisible | Visibility::DeletedByOwn => Ok(false),
        Visibility::VisiblePreImage | Visibility::VisibleMaybePreImage => {
            let pre_image =
                undo_pre_image_from_log(&undo_log.read(), tid, current_payload, snapshot, oracle);
            let Some(pre_image) = pre_image else {
                // `VisibleMaybePreImage` also covers the common case where all
                // historical writers are visible and the physical bytes are
                // the logical row. `VisiblePreImage` promises the opposite;
                // missing undo there is conservatively a serialization
                // conflict instead of silently evaluating the post-image.
                return if matches!(visibility, Visibility::VisibleMaybePreImage) {
                    Ok(true)
                } else {
                    Err(HeapError::WriteConflict(
                        "visible tuple pre-image is unavailable",
                    ))
                };
            };
            if int32_pair_delete_predicate_matches_planned(
                &pre_image,
                0,
                pre_image.len(),
                predicate_plan,
                predicate,
            )? {
                Err(HeapError::WriteConflict(
                    "in-place tuple has an unresolved writer",
                ))
            } else {
                Ok(false)
            }
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
    // A partial scan (for example, because another snapshot-visible version
    // had to be reconstructed from undo) cannot prove a predicate for slots
    // it did not observe. Requiring every ItemId to contribute keeps this
    // shortcut conservative; pages with dead/unused slots simply take the
    // regular per-row predicate path.
    if stats.slot_count != slot_count || stats.normal_slots != slot_count {
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
    // Mirror TupleHeader::mark_deleted: deleting an in-place-update
    // post-image ends the in-place chain (classical deleter xmax) but keeps
    // the undo linkage alive via INPLACE_HISTORY, so the delete is never
    // mistaken for another in-place update and silently lost.
    let mut new_infomask = infomask_bits | InfoMask::UPDATED;
    if new_infomask & InfoMask::UPDATED_IN_PLACE != 0 {
        new_infomask = (new_infomask & !InfoMask::UPDATED_IN_PLACE) | InfoMask::INPLACE_HISTORY;
    }
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
    /// Per-transaction WAL chain link (raw LSN of the txn's previous
    /// record). Resolved atomically with each append inside the sink, so
    /// concurrent workers keep the chain strictly linear with no extra lock.
    chain: &'a std::sync::atomic::AtomicU64,
    vm: Option<&'a crate::vm::VisibilityMap>,
}

struct ParallelDeleteWorkerOutput {
    deleted: Result<usize, HeapError>,
    rollback_pages: Vec<PageId>,
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

    fn finish_parallel_delete(
        &self,
        xid: Xid,
        mut worker_outputs: Vec<ParallelDeleteWorkerOutput>,
        worker_panicked: bool,
    ) -> Result<usize, HeapError> {
        // Every worker owns disjoint page ranges and records touched pages
        // locally. Merge all completed work before propagating a sibling's
        // error so transaction abort can still clear stamps written by workers
        // that finished successfully.
        self.remember_rollback_stamp_pages(
            xid,
            worker_outputs
                .iter_mut()
                .flat_map(|output| output.rollback_pages.drain(..)),
        );
        if worker_panicked {
            return Err(HeapError::ParallelWorkerPanic);
        }
        worker_outputs
            .into_iter()
            .try_fold(0_usize, |total, output| {
                checked_heap_count_add(total, output.deleted?, "deleted tuple count overflow")
            })
    }

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
    ///
    /// Each page is validated completely before any header is changed. Its
    /// registry entry is removed only after the page restoration succeeds, so
    /// a malformed or temporarily unavailable later page remains retryable.
    pub fn rollback_delete_stamps(&self, xid: Xid) -> Result<usize, HeapError> {
        use crate::page::{ITEMID_SIZE, PAGE_HEADER_SIZE, PageHeader};

        let mut total_restored = 0_usize;
        let pages = self.rollback_stamp_pages_snapshot(xid);
        let mut restored_relations: Vec<RelationId> = Vec::new();
        for page_id in pages {
            let guard = self.get_page_relieved(page_id)?;
            let mut page = guard.write();
            let bytes = page.as_bytes_mut();
            let slot_count = PageHeader::decode(bytes)
                .map_err(HeapError::Page)?
                .slot_count();
            let mut restored_headers = Vec::new();

            // Complete every fallible decode and bounds check before writing
            // the first header. A failure therefore leaves both the page and
            // its rollback registry membership unchanged.
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
                restored_headers.push((offset, header_bytes));
            }

            let next_total = checked_heap_count_add(
                total_restored,
                restored_headers.len(),
                "rollback tuple count overflow",
            )?;
            for (offset, header_bytes) in &restored_headers {
                bytes[*offset..*offset + TUPLE_HEADER_SIZE].copy_from_slice(header_bytes);
            }
            // Keep the page latch through registry retirement. Mutators use
            // the same page → registry order, so no observer can see a
            // restored page that still races with a newly published stamp.
            self.mark_rollback_stamp_page_restored(xid, page_id);
            total_restored = next_total;

            if !restored_headers.is_empty() && !restored_relations.contains(&page_id.relation) {
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
    /// after the in-place stamp succeeds. The page latch is released first so
    /// readers remain live, but the original frame pin is retained through
    /// append and LSN publication so eviction/checkpoint cannot expose the
    /// post-delete bytes under their previous WAL dependency.
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

        if let Some((sink, record)) = wal_record {
            // Keep this original pin from the first dirty byte through WAL
            // append and page-LSN publication. The page write latch is released
            // after the header stamp, so readers remain live, but the
            // checkpointer cannot flush this frame with its previous LSN while
            // a WAL sink is blocked.
            let guard = self.get_page_relieved(tid.page)?;
            if let Some(vm) = opts.vm {
                Self::delete_in_place(&guard, tid, opts.xmax, opts.cmax, Some(vm))?;
            } else {
                Self::delete_in_place_no_vm(&guard, tid, opts.xmax, opts.cmax)?;
            }
            self.remember_rollback_stamp_page(opts.xmax, tid.page);

            // Append outside the page-latch scope but while the frame remains
            // pinned. If append returns Err, poison before releasing that pin.
            let lsn: Lsn = Self::append_after_page_mutation(&self.pool, sink, record)?;
            Self::stamp_pinned_page_lsn(&guard, lsn);
        } else {
            // The in-memory benchmark/embedded path has no WAL ordering
            // dependency. Release its pin immediately after the page mutation
            // so the common no-WAL call keeps the same short critical lifetime
            // as a plain buffer-pool write.
            {
                let guard = self.get_page_relieved(tid.page)?;
                if let Some(vm) = opts.vm {
                    Self::delete_in_place(&guard, tid, opts.xmax, opts.cmax, Some(vm))?;
                } else {
                    Self::delete_in_place_no_vm(&guard, tid, opts.xmax, opts.cmax)?;
                }
            }
            self.remember_rollback_stamp_page(opts.xmax, tid.page);
        }
        // Update FSM optimistically so future inserters can find this block.
        // VM was already cleared atomically with the page mutation above.
        Self::post_delete_fsm(&self.pool, tid.page, opts);
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
    /// guard before its WAL append and FSM hook.
    ///
    /// Semantics are equivalent to invoking [`Self::delete`] N
    /// times in order: each tuple's header is stamped with
    /// `opts.xmax` / `opts.cmax`; WAL emission, when configured,
    /// emits one `HeapDelete` record per stamped slot (the WAL
    /// applier replays them identically to `delete`); FSM hints
    /// and VM clears, when `opts.fsm` / `opts.vm` are configured,
    /// run **once per page touched**. VM is cleared under the page
    /// write latch before the first stamp; FSM records final free
    /// space after every delete on the page lands.
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
    /// At most one [`PageGuard`] is held at any instant. Its page latch is
    /// dropped before WAL I/O begins, so concurrent page readers remain live;
    /// its frame pin is retained until the batch's final LSN is published.
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

            // Mutate every slot on this page under one write guard. Retain the
            // original page pin until every per-tuple record is appended and
            // the page receives their maximum LSN.
            let guard = self.get_page_relieved(page_id)?;
            {
                let mut page = guard.write();
                let mut clear_vm = opts.vm;
                let mut mutated = false;
                for &slot in &slots {
                    let tid = TupleId::new(page_id, slot);
                    if let Err(err) =
                        Self::delete_in_place_locked(&mut page, tid, opts.xmax, opts.cmax, clear_vm)
                    {
                        if mutated && opts.wal.is_some() {
                            self.pool.poison_after_wal_error();
                        }
                        return Err(err);
                    }
                    if !mutated {
                        // Register while the first successful stamp is still
                        // protected by the page latch. A malformed later slot
                        // must not strand this page outside abort rollback.
                        self.remember_rollback_stamp_page(opts.xmax, page_id);
                        mutated = true;
                    }
                    // The first successful stamp cleared this page while its
                    // write latch was held; avoid repeating the VM lookup for
                    // every remaining tuple in the page-local batch.
                    clear_vm = None;
                }
                // The page write latch drops here; the pin stays live.
            }

            // Append every per-tuple WAL record outside the page-latch scope.
            // The page LSN is stamped once at the final LSN of the
            // batch (recovery replays records in append order, so the
            // final per-slot stamp is the only state recovery needs).
            if let (Some(sink), Some(payloads)) = (opts.wal, wal_payloads) {
                let mut last_lsn: Lsn = Lsn::ZERO;
                for payload in payloads {
                    let prev_lsn = sink.last_lsn_for(opts.xmax);
                    let record = match WalRecord::new(
                        RecordType::HeapDelete,
                        opts.xmax,
                        prev_lsn,
                        0,
                        payload,
                    ) {
                        Ok(record) => record,
                        Err(err) => {
                            self.pool.poison_after_wal_error();
                            return Err(HeapError::WalRecord(err));
                        }
                    };
                    last_lsn = Self::append_after_page_mutation(&self.pool, sink, record)?;
                }
                Self::stamp_pinned_page_lsn(&guard, last_lsn);
            }
            drop(guard);

            // The FSM hook fires once per page touched. VM was cleared under
            // the page write latch before the first tuple stamp.
            Self::post_delete_fsm(&self.pool, page_id, opts);
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
    /// mutated page after validation but before VM or tuple bytes change.
    /// The page LSN is stamped with that batch LSN under the same write
    /// guard, mirroring the FPW + page-batch + page-LSN pattern in
    /// [`Self::update_int32_pair_inplace_undo`].
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
        let undo_log_handle = self.undo_log_handle(rel);

        // Per-page slot scratch is reused across pages. The compact WAL record
        // is emitted after the page scan has validated every candidate and
        // before VM state or tuple bytes are changed.
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
        // Stage page-local tuple offsets so every predicate, visibility,
        // conflict, bounds, and WAL operation finishes before the first page
        // byte changes. This also lets the VM bit be cleared under the same
        // write guard immediately before the stamps become visible.
        let mut stamp_offsets: Vec<u16> = Vec::with_capacity(256);

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
                    let payload_off = offset + TUPLE_HEADER_SIZE;
                    let payload_end = payload_off
                        .checked_add(9)
                        .ok_or(HeapError::MalformedHeader("int32 pair payload overflow"))?;
                    if !delete_visibility_allows_current_mutation(
                        visibility,
                        &undo_log_handle,
                        TupleId::new(src_page_id, src_slot),
                        &src_bytes[payload_off..tuple_end],
                        (snapshot, oracle),
                        predicate_plan,
                        &predicate,
                    )? {
                        continue;
                    }
                    if let Some(builder) = stats_builder.as_mut()
                        && payload_end <= tuple_end
                        && src_bytes[payload_off] == 0
                    {
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

            let mut guard_appended_lsn = None;
            if let Some(sink) = wal
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

            if page_deleted {
                // Register abort cleanup and clear VM while the page is still
                // write-locked, immediately before the infallible byte stamps.
                // A coherent walker therefore cannot copy post-delete bytes
                // while still trusting a stale all-visible bit.
                self.remember_rollback_stamp_page(xid, src_page_id);
                if let Some(vm) = vm {
                    vm.clear(src_page_id.relation, src_page_id.block);
                }
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
                if let Some(lsn) = guard_appended_lsn {
                    src_page.set_lsn(lsn.raw());
                }
                wal_scratch.clear();
                stamp_offsets.clear();
            }

            drop(src_page);
            drop(src_guard);
        }

        if total_deleted > 0 {
            self.column_cache.bump_version(rel, xid);
        }

        Ok(total_deleted)
    }

    /// Parallel WAL-backed variant for large fused `(Int32, Int32)` DELETEs.
    ///
    /// Each worker owns disjoint block ranges. The sink resolves the shared
    /// `prev_lsn` link atomically with each append so the transaction keeps one
    /// linear WAL chain while page scans and tuple stamping run in parallel.
    /// The method requires a nonblocking WAL sink; each page mutation appends
    /// its compact page-local record while holding the page write guard and
    /// stamps the page with the returned LSN. The first post-checkpoint touch
    /// logs a full page image first through the same linked append path.
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
            || !wal.supports_concurrent_linked_appends()
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

        let predicate_ref = &predicate;
        let chain = std::sync::atomic::AtomicU64::new(wal.last_lsn_for(xid).raw());
        let mut worker_outputs = Vec::with_capacity(workers);
        let mut worker_panicked = false;

        // Work-stealing chunks instead of one equal slice per worker: on
        // asymmetric cores (performance + efficiency) an equal split gates
        // the whole statement on the slowest core; small chunks claimed via
        // fetch_add let fast cores take proportionally more work.
        let chunk_blocks = Self::PARALLEL_WAL_DELETE_BLOCKS_PER_WORKER.max(1);
        let next_chunk = std::sync::atomic::AtomicU32::new(0);

        std::thread::scope(|scope| {
            let mut handles = Vec::with_capacity(workers);
            for _ in 0..workers {
                handles.push(scope.spawn({
                    let chain = &chain;
                    let next_chunk = &next_chunk;
                    move || {
                        let mut rollback_pages = Vec::with_capacity(blocks_per_worker);
                        let deleted =
                            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                let mut deleted = 0_usize;
                                loop {
                                    let start_block = next_chunk.fetch_add(
                                        chunk_blocks,
                                        std::sync::atomic::Ordering::Relaxed,
                                    );
                                    if start_block >= block_count {
                                        return Ok::<usize, HeapError>(deleted);
                                    }
                                    let end_block =
                                        start_block.saturating_add(chunk_blocks).min(block_count);
                                    deleted = checked_heap_count_add(
                                        deleted,
                                        self.delete_int32_pair_range_wal(
                                            DeleteInt32PairWalRange {
                                                rel,
                                                start_block,
                                                end_block,
                                                snapshot,
                                                oracle,
                                                predicate: predicate_ref,
                                                xid,
                                                command_id,
                                                wal,
                                                chain,
                                                vm,
                                            },
                                            &mut rollback_pages,
                                        )?,
                                        "deleted tuple count overflow",
                                    )?;
                                }
                            }))
                            .unwrap_or(Err(HeapError::ParallelWorkerPanic));
                        ParallelDeleteWorkerOutput {
                            deleted,
                            rollback_pages,
                        }
                    }
                }));
            }

            for handle in handles {
                match handle.join() {
                    Ok(output) => worker_outputs.push(output),
                    Err(_) => worker_panicked = true,
                }
            }
        });

        let total_deleted = self.finish_parallel_delete(xid, worker_outputs, worker_panicked)?;
        if total_deleted > 0 {
            self.column_cache.bump_version(rel, xid);
        }

        Ok(total_deleted)
    }

    /// Parallel no-WAL variant for large in-memory fused `(Int32, Int32)`
    /// DELETEs.
    ///
    /// Each worker owns a disjoint page range and stamps matching visible
    /// tuples under the same MVCC rules as
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
        let mut worker_outputs = Vec::with_capacity(workers);
        let mut worker_panicked = false;

        std::thread::scope(|scope| {
            let mut handles = Vec::with_capacity(workers);
            let mut start_block = 0_u32;
            while start_block < block_count {
                let end_block = start_block.saturating_add(chunk_blocks).min(block_count);
                handles.push(scope.spawn(move || {
                    let mut rollback_pages = Vec::with_capacity(512);
                    let deleted = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        self.delete_int32_pair_range_no_wal(
                            DeleteInt32PairRange {
                                rel,
                                start_block,
                                end_block,
                                snapshot,
                                oracle,
                                predicate: predicate_ref,
                                xid,
                                command_id,
                                vm,
                            },
                            &mut rollback_pages,
                        )
                    }))
                    .unwrap_or(Err(HeapError::ParallelWorkerPanic));
                    ParallelDeleteWorkerOutput {
                        deleted,
                        rollback_pages,
                    }
                }));
                start_block = end_block;
            }

            for handle in handles {
                match handle.join() {
                    Ok(output) => worker_outputs.push(output),
                    Err(_) => worker_panicked = true,
                }
            }
        });

        let total_deleted = self.finish_parallel_delete(xid, worker_outputs, worker_panicked)?;
        if total_deleted > 0 {
            self.column_cache.bump_version(rel, xid);
        }

        Ok(total_deleted)
    }

    fn delete_int32_pair_range_wal<O, P>(
        &self,
        request: DeleteInt32PairWalRange<'_, O, P>,
        rollback_pages: &mut Vec<PageId>,
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
            chain,
            vm,
        } = request;
        let mut total_deleted: usize = 0;
        let mut visibility_cache: Option<DeleteVisibilityCache> = None;
        let predicate_plan = delete_predicate_plan(predicate)?;
        let xid_bytes = xid.raw().to_le_bytes();
        let cmd_bytes = command_id.raw().to_le_bytes();
        let vm = vm.filter(|vm| vm.contains_relation(rel));
        let undo_log_handle = self.undo_log_handle(rel);
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
            // Torn-page protection under parallelism: the first
            // post-checkpoint touch of this page logs its full pre-mutation
            // image BEFORE the delta record, exactly like the sequential
            // path's maybe_emit_fpw. The exclusive guard is already held, so
            // the image is read in place; the sink resolves the shared
            // transaction chain link atomically with the append, and page
            // ownership is disjoint per worker so no page can race its own
            // FPW. Appending under the page guard is deadlock-free (WAL
            // backpressure never takes page latches; see WalBuffer docs).
            let checkpoint_lsn = self.last_checkpoint_lsn.load(Ordering::Acquire);
            if checkpoint_lsn != 0 && src_page.header().lsn < checkpoint_lsn {
                let payload = ultrasql_wal::payload::FullPageWritePayload {
                    page: src_page_id,
                    page_bytes: src_page.as_bytes().to_vec(),
                };
                let fpw_lsn = wal.append_borrowed_linked(
                    RecordType::FullPageWrite,
                    xid,
                    0,
                    &payload.encode()?,
                    chain,
                )?;
                src_page.set_lsn(fpw_lsn.raw());
            }
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
                    let payload_off = offset + TUPLE_HEADER_SIZE;
                    let payload_end = payload_off
                        .checked_add(9)
                        .ok_or(HeapError::MalformedHeader("int32 pair payload overflow"))?;
                    if !delete_visibility_allows_current_mutation(
                        visibility,
                        &undo_log_handle,
                        TupleId::new(src_page_id, src_slot),
                        &src_bytes[payload_off..tuple_end],
                        (snapshot, oracle),
                        predicate_plan,
                        predicate,
                    )? {
                        continue;
                    }
                    if let Some(builder) = stats_builder.as_mut()
                        && payload_end <= tuple_end
                        && src_bytes[payload_off] == 0
                    {
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
                let lsn = match wal_scratch.view() {
                    DeleteSlotWalView::Range {
                        first_slot,
                        slot_count,
                    } => Self::emit_delete_in_place_range_batch_wal_linked(
                        wal,
                        src_page_id,
                        xid,
                        command_id,
                        first_slot,
                        slot_count,
                        &mut wal_payload_buf,
                        chain,
                    )?,
                    DeleteSlotWalView::Sparse(slots) => {
                        Self::emit_delete_in_place_batch_wal_linked(
                            wal,
                            src_page_id,
                            xid,
                            command_id,
                            slots,
                            &mut wal_payload_buf,
                            chain,
                        )?
                    }
                    DeleteSlotWalView::Empty => continue,
                };

                // All validation and the linked WAL append completed without
                // mutating the page. Publish abort ownership and clear VM under
                // the held write guard before the first header stamp.
                rollback_pages.push(src_page_id);
                if let Some(vm) = vm {
                    vm.clear(src_page_id.relation, src_page_id.block);
                }
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

            debug_assert_eq!(page_deleted, !stamp_offsets.is_empty());
        }

        Ok(total_deleted)
    }

    fn delete_int32_pair_range_no_wal<O, P>(
        &self,
        request: DeleteInt32PairRange<'_, O, P>,
        rollback_pages: &mut Vec<PageId>,
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
        let undo_log_handle = self.undo_log_handle(rel);
        let mut stamp_offsets = Vec::with_capacity(256);
        for src_block in start_block..end_block {
            let src_page_id = PageId::new(rel, BlockNumber::new(src_block));
            let mut page_deleted = false;
            stamp_offsets.clear();

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
                let payload_off = offset + TUPLE_HEADER_SIZE;
                if !delete_visibility_allows_current_mutation(
                    visibility,
                    &undo_log_handle,
                    TupleId::new(src_page_id, src_slot),
                    &src_bytes[payload_off..tuple_end],
                    (snapshot, oracle),
                    predicate_plan,
                    predicate,
                )? {
                    continue;
                }
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

                let offset_u16 = u16::try_from(offset)
                    .map_err(|_| HeapError::MalformedHeader("tuple offset overflow"))?;
                stamp_offsets.push(offset_u16);
                page_deleted = true;

                total_deleted += 1;
            }

            if page_deleted {
                // The full page scan completed, so no fallible predicate,
                // visibility, conflict, or bounds work remains. Record abort
                // ownership and clear VM under the write guard immediately
                // before applying the proven-infallible stamps.
                rollback_pages.push(src_page_id);
                if let Some(vm) = vm {
                    vm.clear(src_page_id.relation, src_page_id.block);
                }
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
            }

            drop(src_page);
            drop(src_guard);

            debug_assert_eq!(page_deleted, !stamp_offsets.is_empty());
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
        vm: Option<&crate::vm::VisibilityMap>,
    ) -> Result<(), HeapError> {
        let mut page = guard.write();
        Self::delete_in_place_locked(&mut page, tid, xmax, cmax, vm)
    }

    /// Header-only fast path when the caller has no visibility map.
    ///
    /// Keeping the no-VM path branch-free matters for embedded and benchmark
    /// workloads that stamp rows one at a time. It has the same mutation
    /// contract as [`Self::delete_in_place`], minus the VM-clear operation
    /// that cannot apply when no map was supplied.
    #[inline]
    fn delete_in_place_no_vm(
        guard: &PageGuard<L>,
        tid: TupleId,
        xmax: Xid,
        cmax: CommandId,
    ) -> Result<(), HeapError> {
        let mut page = guard.write();
        let page_bytes = page.as_bytes_mut();
        let (slot_offset, slot_length) = Self::slot_window(page_bytes, tid.slot)?;
        if slot_length < TUPLE_HEADER_SIZE {
            return Err(HeapError::MalformedHeader("slot shorter than header"));
        }
        let header_end = slot_offset + TUPLE_HEADER_SIZE;
        let (mut header, _) = TupleHeader::decode(&page_bytes[slot_offset..header_end])
            .ok_or(HeapError::MalformedHeader("header decode failed"))?;
        header.mark_deleted(xmax, cmax);
        let header_bytes = Self::collect_header_bytes(&header);
        page_bytes[slot_offset..header_end].copy_from_slice(&header_bytes);
        Ok(())
    }

    fn delete_in_place_locked(
        page: &mut PageWrite<'_>,
        tid: TupleId,
        xmax: Xid,
        cmax: CommandId,
        vm: Option<&crate::vm::VisibilityMap>,
    ) -> Result<(), HeapError> {
        let page_bytes = page.as_bytes_mut();
        let (slot_offset, slot_length) = Self::slot_window(page_bytes, tid.slot)?;
        if slot_length < TUPLE_HEADER_SIZE {
            return Err(HeapError::MalformedHeader("slot shorter than header"));
        }
        let header_end = slot_offset + TUPLE_HEADER_SIZE;
        let (mut header, _) = TupleHeader::decode(&page_bytes[slot_offset..header_end])
            .ok_or(HeapError::MalformedHeader("header decode failed"))?;
        header.mark_deleted(xmax, cmax);
        let header_bytes = Self::collect_header_bytes(&header);

        // This is the first externally observable state change. Keep it under
        // the same exclusive page latch as the header copy so a coherent
        // walker can see either old bytes + all-visible or new bytes + VM
        // clear, never new bytes + stale all-visible.
        if let Some(vm) = vm {
            vm.clear(tid.page.relation, tid.page.block);
        }
        page_bytes[slot_offset..header_end].copy_from_slice(&header_bytes);
        Ok(())
    }
}
