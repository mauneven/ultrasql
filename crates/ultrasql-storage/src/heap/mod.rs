//! Heap access method.
//!
//! The heap is the simplest access method: a relation's tuples live in
//! its pages without any sort order, identified by a `(block, slot)`
//! [`TupleId`]. Tuples carry an [`MVCC header`](TupleHeader) followed
//! by the user payload; visibility is the caller's responsibility,
//! pair this with [`ultrasql_mvcc::is_visible`] when scanning.
//!
//! Wire-up
//! -------
//!
//! [`HeapAccess`] sits on top of a [`BufferPool`] and provides six
//! operations:
//!
//! - [`HeapAccess::insert`] — append a tuple to a relation, growing the
//!   relation's block count if no existing page has room.
//! - [`HeapAccess::fetch`] — read a tuple by [`TupleId`], ignoring
//!   visibility.
//! - [`HeapAccess::delete`] — stamp `xmax`/`cmax` into the in-place
//!   header so a subsequent visibility check returns `Invisible`.
//! - [`HeapAccess::update`] — replace a tuple's payload, attempting an
//!   in-page HOT update before falling back to a cross-page insert.
//! - [`HeapAccess::scan`] — iterate every normal slot of every page in a
//!   relation, in `(block, slot)` order, without any visibility filter.
//! - [`HeapAccess::scan_visible`] — like `scan` but applies MVCC
//!   visibility inline via a `Snapshot` and an `XidStatusOracle`
//!   (see `ultrasql-mvcc`).
//!
//! Block allocation
//! ----------------
//!
//! The heap owns an internal per-relation atomic counter that grows
//! whenever an insert fails to find free space in an existing block.
//! The persistent catalog stores `n_blocks`/`relpages` as a durable size
//! hint; server scan paths use the larger of the resident heap counter
//! and the catalog hint so newly inserted rows and restart metadata are
//! both covered.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use dashmap::DashMap;
use smallvec::SmallVec;
use ultrasql_core::{BlockNumber, CommandId, PageId, RelationId, TupleId, Xid};
use ultrasql_mvcc::TupleHeader;
use ultrasql_wal::payload::PayloadError;

use crate::buffer_pool::{BufferPool, BufferPoolError, PageLoader};
use crate::page::PageError;
use crate::wal_sink::{WalSink, WalSinkError};

/// Inline storage for an UPDATE's new-tuple payload.
///
/// `(Int32, Int32)` columnar UPDATEs encode a 9-byte body; most narrow
/// row shapes fit in ≤ 16 bytes. The 16-byte inline buffer eliminates
/// the per-row `Vec::with_capacity(9)` heap allocation that otherwise
/// fires once per affected tuple on the bulk-UPDATE path (10 000 rows ⇒
/// 10 000 tiny `mimalloc` calls). Wider rows spill to the heap exactly
/// like a regular `Vec<u8>`, so the slow path is unchanged.
pub type UpdatePayload = SmallVec<[u8; 16]>;

/// Counts page-space releases for one relation and remembers the count at
/// which a full free-space sweep last found no room.
///
/// Both start so that the first insert after construction (or restart) may
/// sweep once. The only consequence of a missed release is that an insert
/// extends the relation instead of reusing space, never a wrong result.
#[derive(Debug)]
pub(crate) struct FreeSpaceEpoch {
    freed: AtomicU64,
    swept_clean_at: AtomicU64,
}

impl FreeSpaceEpoch {
    fn new() -> Self {
        Self {
            freed: AtomicU64::new(1),
            swept_clean_at: AtomicU64::new(0),
        }
    }
}

/// Errors raised by the heap access method.
#[derive(Debug, thiserror::Error)]
pub enum HeapError {
    /// Underlying buffer-pool failure (load miss, contention, etc.).
    #[error("buffer pool: {0}")]
    BufferPool(#[from] BufferPoolError),

    /// Page-level operation failed (slot out of range, dead slot, no
    /// free space within a page, etc.).
    #[error("page: {0}")]
    Page(#[from] PageError),

    /// Direct page writer failed during bulk load.
    #[error("page writer: {0}")]
    PageWriter(#[from] ultrasql_core::Error),

    /// The decoded slot is too short to hold a full [`TupleHeader`], or
    /// the header bytes failed to parse.
    #[error("malformed tuple header: {0}")]
    MalformedHeader(&'static str),

    /// A scoped parallel heap worker unwound before returning its result.
    #[error("parallel heap worker panicked")]
    ParallelWorkerPanic,

    /// Tuple write could not proceed because another transaction's
    /// in-place update is still visible as a pre-image to this snapshot.
    #[error("write conflict: {0}")]
    WriteConflict(&'static str),

    /// The relation's block counter has been exhausted. A relation
    /// would have to grow past [`u32::MAX`] blocks for this to fire.
    #[error("relation is out of blocks")]
    OutOfBlocks,

    /// Fixed-width numeric fast path overflowed while computing a new value.
    #[error("numeric overflow: {0}")]
    NumericOverflow(&'static str),

    /// The [`WalSink`] rejected a record.
    #[error("wal sink: {0}")]
    Wal(#[from] WalSinkError),

    /// Encoding a typed WAL payload failed.
    #[error("wal payload encoding: {0}")]
    WalPayload(#[from] PayloadError),

    /// Encoding a full WAL record failed.
    #[error("wal record encoding: {0}")]
    WalRecord(#[from] ultrasql_wal::WalRecordError),
}

/// Options threaded into an insert.
///
/// The caller knows its transaction id and the current command id within
/// that transaction; the heap stamps both into the tuple header before
/// writing the slot.
///
/// `n_atts` is the physical attribute count encoded in the tuple body.
/// Callers that already have a row/schema descriptor should pass that
/// count so future tuple decoders can distinguish intentionally-missing
/// trailing attributes from unknown metadata.
///
/// With a `wal` sink, the heap first serializes any required checkpoint FPW
/// with the page, writes the tuple, then appends `HeapInsert` and raises the
/// page LSN while retaining the original frame pin. The page latch is released
/// during ordinary WAL backpressure, so readers remain live, but a checkpointer
/// cannot flush the post-insert bytes under their previous LSN. Pass `None` to
/// skip WAL emission (e.g. during recovery or WAL-less tests).
///
/// The optional `fsm` reference, when present, is consulted to locate an
/// existing block with sufficient free space before allocating a new block,
/// and is updated after the insert to reflect the page's new free space.
///
/// The optional `vm` reference is cleared under the same exclusive page latch
/// as the first tuple write, before post-insert bytes can be observed.
#[derive(Clone, Copy)]
pub struct InsertOptions<'a> {
    /// XID of the inserting transaction.
    pub xmin: Xid,
    /// Command id within `xmin` that issued the insert.
    pub command_id: CommandId,
    /// Number of attributes physically present in the tuple body.
    pub n_atts: u16,
    /// Optional WAL sink. When `Some`, the heap retains the dirty page's
    /// original pin through `HeapInsert` append and page-LSN publication.
    pub wal: Option<&'a dyn WalSink>,
    /// Optional free-space map. When `Some`, the heap uses the FSM to
    /// locate a target block before the linear scan, and updates the FSM
    /// after a successful insert.
    pub fsm: Option<&'a crate::fsm::FreeSpaceMap>,
    /// Optional visibility map. When `Some`, the heap clears the page's
    /// all-visible bit atomically with the page mutation.
    pub vm: Option<&'a crate::vm::VisibilityMap>,
}

impl std::fmt::Debug for InsertOptions<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InsertOptions")
            .field("xmin", &self.xmin)
            .field("command_id", &self.command_id)
            .field("n_atts", &self.n_atts)
            .field("wal", &self.wal.is_some())
            .field("fsm", &self.fsm.is_some())
            .field("vm", &self.vm.is_some())
            .finish()
    }
}

/// Options threaded into an update.
///
/// The caller supplies the XID and command id of the updating transaction.
/// `hot_eligible` signals that no indexed column changed in this update, so
/// an in-page HOT chain is safe; the heap will try to satisfy that hint when
/// there is enough room on the same page.
///
/// With a `wal` sink, required FPWs precede the page mutations. The heap then
/// retains the original source/destination frame pins through `HeapUpdate`
/// append and monotonic page-LSN publication, so neither dirty page is
/// flushable in the post-mutation/pre-LSN interval. The record's flags have
/// [`ultrasql_wal::payload::HEAP_UPDATE_HOT`] set when the update was
/// performed as HOT.
///
/// The optional `vm` reference is cleared on both old and new pages while
/// their respective mutation latch is still exclusive.
#[derive(Clone, Copy)]
pub struct UpdateOptions<'a> {
    /// XID performing the update (stamped as `xmax` on the old version
    /// and `xmin` on the new version).
    pub xid: Xid,
    /// Command id within `xid`.
    pub command_id: CommandId,
    /// `true` if no indexed column changed — a HOT update is allowed.
    pub hot_eligible: bool,
    /// Optional WAL sink. When `Some`, the heap keeps every affected frame
    /// pinned through `HeapUpdate` append and page-LSN publication.
    pub wal: Option<&'a dyn WalSink>,
    /// Optional visibility map. When `Some`, the heap clears both affected
    /// pages' all-visible bits atomically with their mutations.
    pub vm: Option<&'a crate::vm::VisibilityMap>,
}

impl std::fmt::Debug for UpdateOptions<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UpdateOptions")
            .field("xid", &self.xid)
            .field("command_id", &self.command_id)
            .field("hot_eligible", &self.hot_eligible)
            .field("wal", &self.wal.is_some())
            .field("vm", &self.vm.is_some())
            .finish()
    }
}

/// Options threaded into a delete.
///
/// The caller supplies the XID and command id of the deleting transaction.
///
/// With a `wal` sink, any checkpoint FPW is serialized first. The heap stamps
/// the tuple, appends `HeapDelete`, and publishes the monotonic page LSN while
/// retaining the original frame pin, preventing a checkpointer from flushing
/// the post-delete page under its previous WAL dependency.
///
/// The optional `fsm` reference, when present, is updated with the page's
/// new free space after the delete (the space is not immediately reclaimed
/// until VACUUM, but we optimistically record the dead-tuple size as free
/// so future inserters see the block as a candidate).
///
/// The optional `vm` reference is cleared under the same exclusive page latch
/// as the delete stamp.
#[derive(Clone, Copy)]
pub struct DeleteOptions<'a> {
    /// XID performing the delete (stamped as `xmax` in the tuple header).
    pub xmax: Xid,
    /// Command id within `xmax` that issued the delete.
    pub cmax: CommandId,
    /// Optional WAL sink. When `Some`, the heap retains the dirty frame's
    /// original pin through `HeapDelete` append and page-LSN publication.
    pub wal: Option<&'a dyn WalSink>,
    /// Optional free-space map. When `Some`, the heap records the page's
    /// post-delete free space so future inserters can find the block.
    pub fsm: Option<&'a crate::fsm::FreeSpaceMap>,
    /// Optional visibility map. When `Some`, the heap clears the page's
    /// all-visible bit atomically with the delete stamp.
    pub vm: Option<&'a crate::vm::VisibilityMap>,
}

impl std::fmt::Debug for DeleteOptions<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeleteOptions")
            .field("xmax", &self.xmax)
            .field("cmax", &self.cmax)
            .field("wal", &self.wal.is_some())
            .finish()
    }
}

/// Result of a successful update.
#[derive(Clone, Copy, Debug)]
pub struct UpdateOutcome {
    /// [`TupleId`] of the old version (unchanged from the caller's
    /// input).
    pub old_tid: TupleId,
    /// [`TupleId`] of the newly-written version.
    pub new_tid: TupleId,
    /// `true` when the update was performed as HOT — old and new
    /// versions live on the same page and are linked via `ctid`.
    pub hot: bool,
}

/// A heap tuple as returned by [`HeapAccess::fetch`] and the scan
/// iterator. The header decodes the MVCC fields; `data` is the user
/// payload bytes following the header.
#[derive(Clone, Debug)]
pub struct HeapTuple {
    /// Identifier of the slot this tuple lives in.
    pub tid: TupleId,
    /// Decoded MVCC header.
    pub header: TupleHeader,
    /// User payload following the header.
    pub data: Vec<u8>,
}

/// Stubbed catalog surface.
///
/// The heap needs to know "how many blocks does this relation have?"
/// to bound its sequential scan, and "give me a new block" to grow on
/// insert. In v0.5 the heap supplies its own implementation by
/// counting blocks it has allocated; once the catalog crate lands,
/// callers will hand a real catalog implementation in.
///
/// This trait is intentionally minimal — the catalog crate will own
/// the production version with richer metadata (column types,
/// statistics, free-space-map handles).
pub trait Catalog: Send + Sync {
    /// Number of blocks currently allocated to `rel`.
    fn block_count(&self, rel: RelationId) -> u32;

    /// Allocate a fresh block for `rel` and return its number. The
    /// implementation is responsible for ensuring concurrent callers
    /// receive distinct block numbers.
    fn extend(&self, rel: RelationId) -> Result<BlockNumber, HeapError>;
}

/// Heap access method.
///
/// Insertion-ordered set of pages stamped with a transaction's `xmax`.
///
/// The membership set exists purely to dedupe: the page-major DELETE and
/// classic-UPDATE walkers report every touched page once per page, but a
/// bare `Vec::contains` dedupe made
/// [`HeapAccess::remember_rollback_stamp_page`] quadratic in the number of
/// touched pages (a 1M-row / ~6 500-page bulk DELETE spent more time in the
/// dedupe scan than in the delete itself).
#[derive(Debug, Default)]
struct RollbackStampPages {
    /// Unique stamped pages in first-touch order.
    pages: Vec<PageId>,
    /// O(1) membership index over `pages`.
    seen: std::collections::HashSet<PageId>,
}

impl RollbackStampPages {
    fn insert(&mut self, page_id: PageId) {
        if self.seen.insert(page_id) {
            self.pages.push(page_id);
        }
    }

    fn extend(&mut self, page_ids: impl IntoIterator<Item = PageId>) {
        let page_ids = page_ids.into_iter();
        let (lower_bound, _) = page_ids.size_hint();
        self.pages.reserve(lower_bound);
        self.seen.reserve(lower_bound);
        for page_id in page_ids {
            self.insert(page_id);
        }
    }

    fn snapshot(&self) -> Vec<PageId> {
        self.pages
            .iter()
            .copied()
            .filter(|page_id| self.seen.contains(page_id))
            .collect()
    }

    fn mark_restored(&mut self, page_id: PageId) {
        self.seen.remove(&page_id);
    }

    fn is_empty(&self) -> bool {
        self.seen.is_empty()
    }
}

/// One [`HeapAccess`] instance is shared across the executor; it does
/// not own any per-statement state, so a single value can serve every
/// concurrent query against the same buffer pool.
pub struct HeapAccess<L: PageLoader> {
    /// Buffer pool. `pub(crate)` so the WAL applier in `wal_applier.rs`
    /// can pin pages directly during recovery without going through the
    /// public `fetch`/`insert`/`delete` methods (which would re-emit WAL).
    pub(crate) pool: Arc<BufferPool<L>>,
    /// Per-relation block counters. Maintained internally for v0.5
    /// because the catalog crate is not yet wired; once the catalog
    /// arrives, this field will be replaced with a `&dyn Catalog`.
    block_counters: DashMap<RelationId, Arc<AtomicU32>>,
    /// Per-relation insertion cursor hint: block number known to have
    /// had free space the last time we inserted there.
    ///
    /// `insert` consults this hint before its linear-scan fallback so
    /// the common case ("there is room on the tail page") is O(1)
    /// instead of O(N) in the number of allocated blocks. The hint may
    /// be stale (a concurrent insert may have filled the page); the
    /// caller handles that by retrying with a linear scan starting at
    /// the hint. The cursor is an `Arc<AtomicU32>` so reads/writes are
    /// lock-free and shared safely across threads.
    insert_cursor: DashMap<RelationId, Arc<AtomicU32>>,
    /// Per-relation record of freed page space, so an insert that finds the
    /// tail page full only sweeps the earlier pages when some of them may
    /// have gained room since the last sweep came up empty.
    free_space_epochs: DashMap<RelationId, Arc<FreeSpaceEpoch>>,
    /// Raw LSN (as `u64`) of the most recent checkpoint. Shared with the
    /// checkpointer so both can read and update it under the same `Arc`.
    ///
    /// Before a page mutation, if a WAL sink is present, the heap checks
    /// whether the page's on-disk LSN is less than `last_checkpoint_lsn`.
    /// If so, it emits a `RecordType::FullPageWrite` record carrying the
    /// entire page image before the mutation record. This ensures that
    /// recovery after a torn partial-page write can restore the page to a
    /// consistent state.
    pub last_checkpoint_lsn: Arc<AtomicU64>,
    /// Per-relation columnar projection cache. Populated lazily by the
    /// first `SeqScan` (no-TID mode) over a relation; invalidated by
    /// every `insert` / `update` / `delete` (and their bulk variants)
    /// through the version-bump mechanism. See
    /// [`crate::column_cache::ColumnCache`].
    pub column_cache: Arc<crate::column_cache::ColumnCache>,
    /// Side-channel undo log for the in-place UPDATE path.
    ///
    /// When an in-place UPDATE rewrites a slot's payload, the
    /// *pre-update* bytes are appended here keyed by relation. A scan
    /// whose snapshot does not yet see the updater's `xmax` as
    /// committed (because the updater is in `xip` or `xmax` is in the
    /// reader's future) consults this log to recover the payload it
    /// should logically observe, preserving MVCC semantics for any
    /// concurrent reader. When no reader exists with such a snapshot
    /// (the common case for autocommit OLTP workloads) the undo
    /// entries are written but never read — the scan path's
    /// visibility check returns the post-update payload from the
    /// slot directly.
    ///
    /// Entries are appended in `(PageId, SlotIndex)` order by the
    /// page-major UPDATE walker. Lookup goes through the log's per-tid /
    /// per-page hash indices, so reconstructing one slot's pre-image costs
    /// O(writers-for-that-slot), independent of the log's total size.
    ///
    /// VACUUM is responsible for trimming entries whose `writer_xid`
    /// is older than every live snapshot's `xmin` (no live reader
    /// could need that pre-image any more); v0.7+ work.
    pub undo_log: Arc<DashMap<RelationId, Arc<parking_lot::RwLock<UndoRelationLog>>>>,
    /// Pages whose tuple headers were stamped with `xmax` by a transaction.
    ///
    /// Rollback uses this to clear aborted DELETE/classic-UPDATE stamps without
    /// scanning every block of every relation. In-place UPDATE rollback uses
    /// `undo_log` instead because it must restore payload bytes too.
    rollback_stamp_pages: Arc<DashMap<u64, parking_lot::Mutex<RollbackStampPages>>>,
    /// Payload-only min/max cache for page-local `(Int32, Int32)` rows.
    ///
    /// Header-only DELETE stamps do not invalidate this cache because tuple
    /// payload bytes and slot layout stay stable. INSERT and UPDATE paths must
    /// invalidate entries for their relation before cached stats can be used to
    /// prove a page-level predicate match.
    pub(crate) int32_pair_payload_stats: Arc<DashMap<PageId, Int32PairPagePayloadStats>>,
    /// Visibility map shared with online WAL replay.
    ///
    /// Crash recovery starts with an empty in-memory VM, but hot-standby
    /// replay runs while read-only queries are active. When attached, every
    /// redo mutation clears the affected VM entry under the same exclusive
    /// page latch as the page write. This recovery-only field stays after the
    /// OLTP-facing caches so adding it does not displace the fields touched by
    /// insert, update, and delete operations.
    replay_visibility_map: parking_lot::RwLock<Option<Arc<crate::vm::VisibilityMap>>>,
}

/// Payload min/max stats for one heap page containing fixed `(Int32, Int32)` rows.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Int32PairPagePayloadStats {
    /// Page header slot count when the stats were built.
    pub slot_count: u16,
    /// Number of normal item slots included in the min/max range.
    pub normal_slots: u16,
    /// Minimum value observed for payload column 0.
    pub min0: i32,
    /// Maximum value observed for payload column 0.
    pub max0: i32,
    /// Minimum value observed for payload column 1.
    pub min1: i32,
    /// Maximum value observed for payload column 1.
    pub max1: i32,
}

/// Per-relation in-place-update undo log with O(1) slot-scoped lookup.
///
/// The log stores two record kinds (full-payload [`UndoEntry`]s and compact
/// [`Int32PairUndoBatch`]es) in append order, plus hash indices keyed by
/// [`TupleId`] / [`PageId`]. Every reader that reconstructs one slot's
/// pre-image touches only that slot's writers — NOT the whole log. The old
/// representation was two bare `Vec`s that the pre-image reconstruction
/// (`undo_pre_image_from_log`) scanned END TO END per lookup: a background
/// scan over a table with `B` live undo batches paid `O(rows x B)`, which at
/// 1M rows turned maintenance scans into core-burning quadratics.
///
/// Fields are private so every mutation path keeps the indices coherent;
/// mutate through the methods below.
#[derive(Clone, Copy, Debug)]
struct UndoRecordMetadata {
    sequence: u64,
    active: bool,
}

#[derive(Debug, Default)]
pub struct UndoRelationLog {
    /// Sequence assigned to the next record appended to either record kind.
    ///
    /// A single sequence across both vectors preserves mutation order for
    /// snapshot reconstruction and rollback.
    next_sequence: u64,
    /// Internal ordering/rollback state parallel to `entries`.
    entry_metadata: Vec<UndoRecordMetadata>,
    /// Internal ordering/rollback state parallel to `int32_pair_batches`.
    batch_metadata: Vec<UndoRecordMetadata>,
    /// Full-payload entries in append order (per slot: oldest first).
    entries: Vec<UndoEntry>,
    /// Compact fixed-width in-place UPDATE batches, in append order. These
    /// cover the `(Int32, Int32) SET col = col ± literal` path and avoid one
    /// full [`UndoEntry`] per row on bulk updates.
    int32_pair_batches: Vec<Int32PairUndoBatch>,
    /// `tid` → ascending indices into `entries`.
    entries_by_tid: std::collections::HashMap<TupleId, Vec<usize>>,
    /// `page` → ascending indices into `int32_pair_batches`.
    batches_by_page: std::collections::HashMap<PageId, Vec<usize>>,
}

impl UndoRelationLog {
    /// Append a full-payload pre-image record.
    pub fn push_entry(&mut self, entry: UndoEntry) {
        self.entry_metadata.push(UndoRecordMetadata {
            sequence: self.next_sequence,
            active: true,
        });
        self.next_sequence = self.next_sequence.saturating_add(1);
        self.entries_by_tid
            .entry(entry.tid)
            .or_default()
            .push(self.entries.len());
        self.entries.push(entry);
    }

    /// Append one compact int32-pair batch.
    pub fn push_int32_pair_batch(&mut self, batch: Int32PairUndoBatch) {
        self.batch_metadata.push(UndoRecordMetadata {
            sequence: self.next_sequence,
            active: true,
        });
        self.next_sequence = self.next_sequence.saturating_add(1);
        self.batches_by_page
            .entry(batch.page)
            .or_default()
            .push(self.int32_pair_batches.len());
        self.int32_pair_batches.push(batch);
    }

    /// Drain `scratch` into the log (bulk-update append path).
    pub fn append_int32_pair_batches(&mut self, scratch: &mut Vec<Int32PairUndoBatch>) {
        self.int32_pair_batches.reserve(scratch.len());
        for batch in scratch.drain(..) {
            self.push_int32_pair_batch(batch);
        }
    }

    /// Full-payload entries recorded for `tid`, oldest first.
    pub fn entries_for_tid(
        &self,
        tid: TupleId,
    ) -> impl DoubleEndedIterator<Item = &UndoEntry> + '_ {
        self.entries_by_tid
            .get(&tid)
            .into_iter()
            .flatten()
            .filter_map(|&index| {
                self.entry_metadata
                    .get(index)
                    .is_some_and(|metadata| metadata.active)
                    .then(|| self.entries.get(index))
                    .flatten()
            })
    }

    fn ordered_entries_for_tid(
        &self,
        tid: TupleId,
    ) -> impl DoubleEndedIterator<Item = (&UndoEntry, UndoRecordMetadata)> + '_ {
        self.entries_by_tid
            .get(&tid)
            .into_iter()
            .flatten()
            .filter_map(|&index| {
                let metadata = *self.entry_metadata.get(index)?;
                if !metadata.active {
                    return None;
                }
                Some((self.entries.get(index)?, metadata))
            })
    }

    /// Compact batches recorded for `page`, oldest first.
    pub fn batches_for_page(
        &self,
        page: PageId,
    ) -> impl DoubleEndedIterator<Item = &Int32PairUndoBatch> + '_ {
        self.batches_by_page
            .get(&page)
            .into_iter()
            .flatten()
            .filter_map(|&index| {
                self.batch_metadata
                    .get(index)
                    .is_some_and(|metadata| metadata.active)
                    .then(|| self.int32_pair_batches.get(index))
                    .flatten()
            })
    }

    fn ordered_batches_for_page(
        &self,
        page: PageId,
    ) -> impl DoubleEndedIterator<Item = (&Int32PairUndoBatch, UndoRecordMetadata)> + '_ {
        self.batches_by_page
            .get(&page)
            .into_iter()
            .flatten()
            .filter_map(|&index| {
                let metadata = *self.batch_metadata.get(index)?;
                if !metadata.active {
                    return None;
                }
                Some((self.int32_pair_batches.get(index)?, metadata))
            })
    }

    /// All full-payload entries, in append order (vacuum/tests).
    #[must_use]
    pub fn entries(&self) -> &[UndoEntry] {
        &self.entries
    }

    /// All compact batches, in append order (vacuum/tests).
    #[must_use]
    pub fn int32_pair_batches(&self) -> &[Int32PairUndoBatch] {
        &self.int32_pair_batches
    }

    /// `true` when neither record kind holds anything.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty() && self.int32_pair_batches.is_empty()
    }

    /// Number of full-payload entries currently retained.
    #[must_use]
    pub fn entries_len(&self) -> usize {
        self.entry_metadata
            .iter()
            .filter(|metadata| metadata.active)
            .count()
    }

    /// Number of compact batches currently retained.
    #[must_use]
    pub fn int32_pair_batches_len(&self) -> usize {
        self.batch_metadata
            .iter()
            .filter(|metadata| metadata.active)
            .count()
    }

    /// Return every page with active undo written by `xid`.
    pub(crate) fn pages_written_by(&self, xid: Xid) -> Vec<PageId> {
        let mut pages = std::collections::HashSet::new();
        for (tid, indices) in &self.entries_by_tid {
            if indices.iter().any(|&index| {
                self.entries
                    .get(index)
                    .zip(self.entry_metadata.get(index))
                    .is_some_and(|(entry, metadata)| entry.writer_xid == xid && metadata.active)
            }) {
                pages.insert(tid.page);
            }
        }
        for (page, indices) in &self.batches_by_page {
            if indices.iter().any(|&index| {
                self.int32_pair_batches
                    .get(index)
                    .zip(self.batch_metadata.get(index))
                    .is_some_and(|(batch, metadata)| batch.writer_xid == xid && metadata.active)
            }) {
                pages.insert(*page);
            }
        }
        let mut pages: Vec<_> = pages.into_iter().collect();
        pages.sort_unstable_by_key(|page| (page.relation.0.raw(), page.block.raw()));
        pages
    }

    /// Clone active full-payload records for `xid` on `page`, oldest first.
    pub(crate) fn entries_written_by_on_page(
        &self,
        xid: Xid,
        page: PageId,
    ) -> Vec<(u64, UndoEntry)> {
        let mut entries = Vec::new();
        for (tid, indices) in &self.entries_by_tid {
            if tid.page != page {
                continue;
            }
            for &index in indices {
                if let Some((entry, metadata)) = self
                    .entries
                    .get(index)
                    .zip(self.entry_metadata.get(index))
                    .filter(|(entry, metadata)| entry.writer_xid == xid && metadata.active)
                {
                    entries.push((index, metadata.sequence, entry.clone()));
                }
            }
        }
        entries.sort_unstable_by_key(|(index, _, _)| *index);
        entries
            .into_iter()
            .map(|(_, sequence, entry)| (sequence, entry))
            .collect()
    }

    /// Clone active compact records for `xid` on `page`, oldest first.
    pub(crate) fn batches_written_by_on_page(
        &self,
        xid: Xid,
        page: PageId,
    ) -> Vec<(u64, Int32PairUndoBatch)> {
        self.batches_by_page
            .get(&page)
            .into_iter()
            .flatten()
            .filter_map(|&index| {
                let batch = self.int32_pair_batches.get(index)?;
                let metadata = self.batch_metadata.get(index)?;
                (batch.writer_xid == xid && metadata.active)
                    .then(|| (metadata.sequence, batch.clone()))
            })
            .collect()
    }

    /// Return whether `tid` retains active undo written by another
    /// transaction.
    ///
    /// Rollback of the newest in-place writer clears its live
    /// `UPDATED_IN_PLACE` stamp. If an older writer remains in this log, the
    /// tuple header must retain `INPLACE_HISTORY` so snapshots predating that
    /// writer still consult the surviving undo chain.
    pub(crate) fn has_active_history_for_tid_excluding(&self, tid: TupleId, xid: Xid) -> bool {
        self.entries_for_tid(tid)
            .any(|entry| entry.writer_xid != xid)
            || self
                .batches_for_page(tid.page)
                .any(|batch| batch.writer_xid != xid && batch.contains_slot(tid.slot))
    }

    /// Deactivate `xid`'s records for `page` after rollback restored it.
    ///
    /// Walkers snapshot page-scoped undo while holding the page read guard, so
    /// an older walker retains its private copy while new readers atomically
    /// observe the restored page and no active global record.
    pub(crate) fn deactivate_written_by_on_page(&mut self, xid: Xid, page: PageId) {
        for (tid, indices) in &self.entries_by_tid {
            if tid.page != page {
                continue;
            }
            for &index in indices {
                if self
                    .entries
                    .get(index)
                    .is_some_and(|entry| entry.writer_xid == xid)
                    && let Some(metadata) = self.entry_metadata.get_mut(index)
                {
                    metadata.active = false;
                }
            }
        }
        if let Some(indices) = self.batches_by_page.get(&page) {
            for &index in indices {
                if self
                    .int32_pair_batches
                    .get(index)
                    .is_some_and(|batch| batch.writer_xid == xid)
                    && let Some(metadata) = self.batch_metadata.get_mut(index)
                {
                    metadata.active = false;
                }
            }
        }
    }

    /// Remove deactivated backing records and rebuild indices once.
    pub(crate) fn compact_inactive_records(&mut self) {
        let mut kept_entries = Vec::with_capacity(self.entries.len());
        let mut kept_entry_metadata = Vec::with_capacity(self.entry_metadata.len());
        for (entry, metadata) in self.entries.drain(..).zip(self.entry_metadata.drain(..)) {
            if metadata.active {
                kept_entries.push(entry);
                kept_entry_metadata.push(metadata);
            }
        }
        self.entries = kept_entries;
        self.entry_metadata = kept_entry_metadata;

        let mut kept_batches = Vec::with_capacity(self.int32_pair_batches.len());
        let mut kept_batch_metadata = Vec::with_capacity(self.batch_metadata.len());
        for (batch, metadata) in self
            .int32_pair_batches
            .drain(..)
            .zip(self.batch_metadata.drain(..))
        {
            if metadata.active {
                kept_batches.push(batch);
                kept_batch_metadata.push(metadata);
            }
        }
        self.int32_pair_batches = kept_batches;
        self.batch_metadata = kept_batch_metadata;
        self.rebuild_indices();
    }

    /// Clone active undo for `page` into an immutable walker-local log.
    pub(crate) fn snapshot_page(&self, page: PageId) -> Self {
        let mut snapshot = Self::default();
        let mut entries = Vec::new();
        for (tid, indices) in &self.entries_by_tid {
            if tid.page != page {
                continue;
            }
            for &index in indices {
                if let Some((entry, metadata)) = self
                    .entries
                    .get(index)
                    .zip(self.entry_metadata.get(index))
                    .filter(|(_, metadata)| metadata.active)
                {
                    entries.push((metadata.sequence, entry.clone()));
                }
            }
        }
        entries.sort_unstable_by_key(|(sequence, _)| *sequence);

        let mut batches = self
            .batches_by_page
            .get(&page)
            .into_iter()
            .flatten()
            .filter_map(|&index| {
                let batch = self.int32_pair_batches.get(index)?;
                let metadata = self.batch_metadata.get(index)?;
                metadata.active.then(|| (metadata.sequence, batch.clone()))
            })
            .collect::<Vec<_>>();
        batches.sort_unstable_by_key(|(sequence, _)| *sequence);

        let mut entry_index = 0;
        let mut batch_index = 0;
        while entry_index < entries.len() || batch_index < batches.len() {
            let take_entry = match (entries.get(entry_index), batches.get(batch_index)) {
                (Some((entry_sequence, _)), Some((batch_sequence, _))) => {
                    entry_sequence < batch_sequence
                }
                (Some(_), None) => true,
                (None, Some(_)) => false,
                (None, None) => break,
            };
            if take_entry {
                snapshot.push_entry(entries[entry_index].1.clone());
                entry_index += 1;
            } else {
                snapshot.push_int32_pair_batch(batches[batch_index].1.clone());
                batch_index += 1;
            }
        }
        snapshot
    }

    /// Remove and return every record written by `xid` (rollback), keeping
    /// all other writers' records and their relative order.
    pub fn take_written_by(&mut self, xid: Xid) -> (Vec<UndoEntry>, Vec<Int32PairUndoBatch>) {
        let mut taken_entries = Vec::new();
        let mut kept_entries = Vec::with_capacity(self.entries.len());
        let mut kept_entry_metadata = Vec::with_capacity(self.entry_metadata.len());
        for (entry, metadata) in self.entries.drain(..).zip(self.entry_metadata.drain(..)) {
            if entry.writer_xid == xid {
                taken_entries.push(entry);
            } else {
                kept_entries.push(entry);
                kept_entry_metadata.push(metadata);
            }
        }
        self.entries = kept_entries;
        self.entry_metadata = kept_entry_metadata;

        let mut taken_batches = Vec::new();
        let mut kept_batches = Vec::with_capacity(self.int32_pair_batches.len());
        let mut kept_batch_metadata = Vec::with_capacity(self.batch_metadata.len());
        for (batch, metadata) in self
            .int32_pair_batches
            .drain(..)
            .zip(self.batch_metadata.drain(..))
        {
            if batch.writer_xid == xid {
                taken_batches.push(batch);
            } else {
                kept_batches.push(batch);
                kept_batch_metadata.push(metadata);
            }
        }
        self.int32_pair_batches = kept_batches;
        self.batch_metadata = kept_batch_metadata;

        self.rebuild_indices();
        (taken_entries, taken_batches)
    }

    /// Drop every record whose writer is older than `oldest_active_xid`
    /// (vacuum trim: those writers are terminal and visible to every
    /// possible snapshot). Returns `(entries_trimmed, batches_trimmed)`.
    pub fn trim_below(&mut self, oldest_active_xid: Xid) -> (usize, usize) {
        let entries_before = self.entries.len();
        let mut kept_entries = Vec::with_capacity(self.entries.len());
        let mut kept_entry_metadata = Vec::with_capacity(self.entry_metadata.len());
        for (entry, metadata) in self.entries.drain(..).zip(self.entry_metadata.drain(..)) {
            if metadata.active && entry.writer_xid >= oldest_active_xid {
                kept_entries.push(entry);
                kept_entry_metadata.push(metadata);
            }
        }
        self.entries = kept_entries;
        self.entry_metadata = kept_entry_metadata;
        let batches_before = self.int32_pair_batches.len();
        let mut kept_batches = Vec::with_capacity(self.int32_pair_batches.len());
        let mut kept_batch_metadata = Vec::with_capacity(self.batch_metadata.len());
        for (batch, metadata) in self
            .int32_pair_batches
            .drain(..)
            .zip(self.batch_metadata.drain(..))
        {
            if metadata.active && batch.writer_xid >= oldest_active_xid {
                kept_batches.push(batch);
                kept_batch_metadata.push(metadata);
            }
        }
        self.int32_pair_batches = kept_batches;
        self.batch_metadata = kept_batch_metadata;
        let trimmed = (
            entries_before - self.entries.len(),
            batches_before - self.int32_pair_batches.len(),
        );
        if trimmed != (0, 0) {
            self.rebuild_indices();
        }
        trimmed
    }

    /// Recompute both hash indices from the record vectors.
    fn rebuild_indices(&mut self) {
        self.entries_by_tid.clear();
        for (idx, entry) in self.entries.iter().enumerate() {
            if self
                .entry_metadata
                .get(idx)
                .is_some_and(|metadata| metadata.active)
            {
                self.entries_by_tid.entry(entry.tid).or_default().push(idx);
            }
        }
        self.batches_by_page.clear();
        for (idx, batch) in self.int32_pair_batches.iter().enumerate() {
            if self
                .batch_metadata
                .get(idx)
                .is_some_and(|metadata| metadata.active)
            {
                self.batches_by_page
                    .entry(batch.page)
                    .or_default()
                    .push(idx);
            }
        }
    }
}

/// One pre-image record carried by the in-place-update undo log.
#[derive(Clone, Debug)]
pub struct UndoEntry {
    /// `TupleId` of the slot whose pre-update payload this entry
    /// holds.
    pub tid: TupleId,
    /// XID of the transaction that wrote the *new* in-place payload.
    /// Used by readers to decide whether their snapshot sees the
    /// update — if not, the pre-image stored in this entry is what
    /// they should observe.
    pub writer_xid: Xid,
    /// Command within `writer_xid` that wrote the post-image.
    ///
    /// Own writes become visible only to later commands, so readers need this
    /// boundary to reconstruct statement-level snapshots within one
    /// transaction.
    pub command_id: CommandId,
    /// The pre-update payload bytes (no tuple header). The current
    /// in-place fast path stores exactly the 9-byte `(null, id, val)`
    /// body for `(Int32, Int32)` rows.
    pub old_payload: [u8; 9],
}

/// Compact pre-image metadata for one page of fixed-width `(Int32, Int32)`
/// in-place updates.
#[derive(Clone, Debug)]
pub struct Int32PairUndoBatch {
    /// Page whose slots were updated.
    pub page: PageId,
    /// XID of the transaction that wrote the post-image.
    pub writer_xid: Xid,
    /// Command within `writer_xid` that wrote the post-image. Together
    /// with `writer_xid` this uniquely identifies the originating
    /// in-place UPDATE command, distinguishing two distinct same-shape
    /// commands of one transaction (same page/col/delta/slots, different
    /// `command_id`) from a single record re-replayed during recovery.
    /// It is a dedup discriminator only — it does NOT participate in the
    /// pre-image delta sum (`undo_pre_image_from_log`), so two distinct
    /// commands still both contribute their delta.
    pub command_id: CommandId,
    /// Updated column: `0` for `id`, `1` for `val`.
    pub target_col: u8,
    /// Delta applied to the target column.
    pub delta: i32,
    /// First updated slot when the batch is a contiguous range.
    pub first_slot: u16,
    /// Number of slots in the contiguous range starting at
    /// [`Self::first_slot`]. When [`Self::slots`] is non-empty this
    /// mirrors `slots.len()` for observability.
    pub slot_count: u16,
    /// Updated slots on `page`, in ascending slot order. Empty means
    /// the batch is represented by `first_slot..first_slot+slot_count`.
    pub slots: Vec<u16>,
}

impl Int32PairUndoBatch {
    /// Number of row pre-images represented by this batch.
    #[must_use]
    pub fn slot_len(&self) -> usize {
        if self.slots.is_empty() {
            usize::from(self.slot_count)
        } else {
            self.slots.len()
        }
    }

    /// Return `true` when this batch contains `slot`.
    #[must_use]
    pub fn contains_slot(&self, slot: u16) -> bool {
        if self.slots.is_empty() {
            let end = self.first_slot.saturating_add(self.slot_count);
            slot >= self.first_slot && slot < end
        } else {
            self.slots.binary_search(&slot).is_ok()
        }
    }
}

/// Reconstruct the pre-image an in-place-updated slot must show to
/// `snapshot`, by reversing every undo record whose `writer_xid` the
/// snapshot cannot see. Shared by the closure scan (`scan.rs`) and the
/// free walker (`walker.rs`) so both paths use identical logic.
///
/// Full-payload and compact records share one monotonic
/// internal sequence. Reconstruction merges both per-slot streams newest-first
/// and applies each invisible writer's exact inverse in temporal order. This
/// is required when point and bulk updates alternate: a full pre-image
/// replaces the payload at its position in history, while a compact record
/// subtracts its delta.
pub(crate) fn undo_pre_image_from_log<O>(
    log: &UndoRelationLog,
    tid: TupleId,
    current_payload: &[u8],
    snapshot: &ultrasql_mvcc::Snapshot,
    oracle: &O,
) -> Option<Vec<u8>>
where
    O: ultrasql_mvcc::XidStatusOracle + ?Sized,
{
    if current_payload.len() < 9 {
        return None;
    }

    let mut full = log.ordered_entries_for_tid(tid).rev().peekable();
    let mut compact = log
        .ordered_batches_for_page(tid.page)
        .rev()
        .filter(|(batch, _)| batch.contains_slot(tid.slot))
        .peekable();
    let mut pre_image: Option<Vec<u8>> = None;

    loop {
        let take_full = match (full.peek(), compact.peek()) {
            (Some((_, entry_meta)), Some((_, batch_meta))) => {
                entry_meta.sequence > batch_meta.sequence
            }
            (Some(_), None) => true,
            (None, Some(_)) => false,
            (None, None) => break,
        };
        if take_full {
            let (entry, _) = full.next()?;
            if !undo_writer_visible_to_snapshot(
                entry.writer_xid,
                entry.command_id,
                snapshot,
                oracle,
            ) {
                let payload = pre_image.get_or_insert_with(|| current_payload[..9].to_vec());
                payload.copy_from_slice(&entry.old_payload);
            }
        } else {
            let (batch, _) = compact.next()?;
            if !undo_writer_visible_to_snapshot(
                batch.writer_xid,
                batch.command_id,
                snapshot,
                oracle,
            ) {
                let payload = pre_image.get_or_insert_with(|| current_payload[..9].to_vec());
                reverse_one_compact_delta(payload, batch.target_col, batch.delta)?;
            }
        }
    }

    pre_image
}

#[inline]
fn undo_writer_visible_to_snapshot<O>(
    writer: Xid,
    command_id: CommandId,
    snapshot: &ultrasql_mvcc::Snapshot,
    oracle: &O,
) -> bool
where
    O: ultrasql_mvcc::XidStatusOracle + ?Sized,
{
    if snapshot.own_subxid_rolled_back(writer) {
        false
    } else if snapshot.is_current_xid(writer) {
        command_id < snapshot.current_command
    } else if snapshot.xid_in_progress(writer) {
        false
    } else {
        matches!(
            oracle.status(writer),
            ultrasql_mvcc::status::XidStatus::Committed | ultrasql_mvcc::status::XidStatus::Frozen,
        )
    }
}

/// Reverse one compact delta in-place.
fn reverse_one_compact_delta(payload: &mut [u8], target_col: u8, delta: i32) -> Option<()> {
    let offset = if target_col == 0 { 1 } else { 5 };
    let current = i32::from_le_bytes(payload.get(offset..offset + 4)?.try_into().ok()?);
    let restored = i64::from(current).checked_sub(i64::from(delta))?;
    let restored = i32::try_from(restored).ok()?;
    payload
        .get_mut(offset..offset + 4)?
        .copy_from_slice(&restored.to_le_bytes());
    Some(())
}

#[inline]
fn checked_heap_count_add(
    current: usize,
    delta: usize,
    error: &'static str,
) -> Result<usize, HeapError> {
    current
        .checked_add(delta)
        .ok_or(HeapError::MalformedHeader(error))
}

#[inline]
fn checked_heap_u32_count_add(
    current: u32,
    delta: u32,
    error: &'static str,
) -> Result<u32, HeapError> {
    current
        .checked_add(delta)
        .ok_or(HeapError::MalformedHeader(error))
}

#[inline]
fn checked_heap_u64_count_add(
    current: u64,
    delta: usize,
    error: &'static str,
) -> Result<u64, HeapError> {
    let delta = u64::try_from(delta).map_err(|_| HeapError::MalformedHeader(error))?;
    current
        .checked_add(delta)
        .ok_or(HeapError::MalformedHeader(error))
}

#[inline]
fn checked_tuple_space_needed(tuple_size: usize) -> Result<usize, HeapError> {
    tuple_size
        .checked_add(crate::page::ITEMID_SIZE)
        .ok_or(HeapError::MalformedHeader("tuple size overflow"))
}

impl<L: PageLoader> std::fmt::Debug for HeapAccess<L> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HeapAccess")
            .field("relation_count", &self.block_counters.len())
            .finish_non_exhaustive()
    }
}

mod delete;
mod helpers;
mod insert;
mod scan;
#[cfg(test)]
mod tests;
mod tuple_fields;
mod update;
mod update_inplace;
mod vacuum;
mod wal_emit;
mod walker;

pub use delete::{
    DeleteInt32PairScan, DeleteInt32PairStamp, Int32PairCmp, Int32PairPredicate,
    Int32PairPredicateEval,
};
pub use update_inplace::{
    UpdateInt32PairEdit, UpdateInt32PairScan, UpdateInt32PairStamp, UpdateInt32PairTid,
};

impl<L: PageLoader> HeapAccess<L> {
    /// Build a new heap access bound to `pool`.
    ///
    /// The `last_checkpoint_lsn` is an optional shared atomic that tracks the
    /// LSN of the most recent checkpoint. Pass `None` to create a standalone
    /// `HeapAccess` that never emits full-page-write records (suitable for
    /// tests or WAL-less configurations). Pass `Some(Arc<AtomicU64>)` from
    /// the same `Arc` used by the checkpointer to enable FPW emission.
    #[must_use]
    pub fn new(pool: Arc<BufferPool<L>>) -> Self {
        Self {
            pool,
            block_counters: DashMap::new(),
            insert_cursor: DashMap::new(),
            free_space_epochs: DashMap::new(),
            last_checkpoint_lsn: Arc::new(AtomicU64::new(0)),
            replay_visibility_map: parking_lot::RwLock::new(None),
            column_cache: Arc::new(crate::column_cache::ColumnCache::new()),
            undo_log: Arc::new(DashMap::new()),
            rollback_stamp_pages: Arc::new(DashMap::new()),
            int32_pair_payload_stats: Arc::new(DashMap::new()),
        }
    }

    /// Build a new heap access that shares `last_checkpoint_lsn` with the
    /// checkpointer (or any other writer that advances the checkpoint LSN).
    ///
    /// Prefer this constructor in production; use [`Self::new`] in tests
    /// that do not care about FPW emission.
    #[must_use]
    pub fn with_checkpoint_lsn(
        pool: Arc<BufferPool<L>>,
        last_checkpoint_lsn: Arc<AtomicU64>,
    ) -> Self {
        Self {
            pool,
            block_counters: DashMap::new(),
            insert_cursor: DashMap::new(),
            free_space_epochs: DashMap::new(),
            last_checkpoint_lsn,
            replay_visibility_map: parking_lot::RwLock::new(None),
            column_cache: Arc::new(crate::column_cache::ColumnCache::new()),
            undo_log: Arc::new(DashMap::new()),
            rollback_stamp_pages: Arc::new(DashMap::new()),
            int32_pair_payload_stats: Arc::new(DashMap::new()),
        }
    }

    /// Acquire a page guard, relieving buffer-pool exhaustion on the way.
    ///
    /// This is the user-facing read/mutate entry point: heap, index, and TOAST
    /// page accesses route through it (recovery and checkpoint-internal paths
    /// use the raw `self.pool.get_page` because they are single-threaded and
    /// pre-WAL-writer and must NOT trigger relief).
    ///
    /// It delegates to [`BufferPool::get_page_relieved`], which on
    /// [`BufferPoolError::Exhausted`] invokes the pool's installed
    /// [`EvictionRelief`] hook to flush dirty pages — LSN-gated, forcing the
    /// WAL durable when every dirty victim is ahead of the durable position —
    /// and retries within a bounded budget.
    ///
    /// # Errors
    ///
    /// Propagates any [`BufferPoolError`] (including `Exhausted` after the
    /// relief budget is spent) as a [`HeapError`].
    pub(crate) fn get_page_relieved(
        &self,
        page_id: PageId,
    ) -> Result<crate::buffer_pool::PageGuard<L>, HeapError> {
        self.pool
            .get_page_relieved(page_id)
            .map_err(HeapError::BufferPool)
    }

    pub(crate) fn invalidate_int32_pair_payload_stats_relation(&self, rel: RelationId) {
        self.int32_pair_payload_stats
            .retain(|page_id, _| page_id.relation != rel);
    }

    /// Invalidate cached fixed-width payload statistics for one mutated page.
    ///
    /// Single-row INSERT and UPDATE operations know every page whose payload
    /// changed. Removing those exact keys avoids a relation-wide `DashMap`
    /// shard scan on every OLTP mutation while preserving the same cache
    /// coherence contract.
    pub(crate) fn invalidate_int32_pair_payload_stats_page(&self, page_id: PageId) {
        self.int32_pair_payload_stats.remove(&page_id);
    }

    /// Clone a relation undo handle without retaining a DashMap shard guard.
    ///
    /// Callers obtain this before page locking, then follow `page → undo`.
    pub(crate) fn undo_log_handle(
        &self,
        rel: RelationId,
    ) -> Arc<parking_lot::RwLock<UndoRelationLog>> {
        let entry = self
            .undo_log
            .entry(rel)
            .or_insert_with(|| Arc::new(parking_lot::RwLock::new(UndoRelationLog::default())));
        Arc::clone(entry.value())
    }

    /// Attach the visibility map used by online WAL replay.
    ///
    /// The map must be the same instance consulted by visible scans and
    /// updated by vacuum. Attach it before replay begins; redo then clears a
    /// page's VM bits while retaining that page's exclusive latch, preventing
    /// a hot-standby reader from observing replayed bytes through a stale
    /// all-visible shortcut.
    pub fn attach_replay_visibility_map(&self, vm: Arc<crate::vm::VisibilityMap>) {
        *self.replay_visibility_map.write() = Some(vm);
    }

    /// Clear VM state for a page while the caller retains its write latch.
    pub(crate) fn clear_replay_visibility(&self, page_id: PageId) {
        let vm = self.replay_visibility_map.read().as_ref().map(Arc::clone);
        if let Some(vm) = vm {
            vm.clear(page_id.relation, page_id.block);
        }
    }

    /// `true` when every undo writer recorded for `tid` is visible to
    /// `snapshot` — i.e. the slot's current bytes are exactly the payload
    /// this snapshot should observe and a row flagged
    /// [`ultrasql_mvcc::tuple_header::InfoMask::INPLACE_HISTORY`] can be
    /// treated as plainly [`ultrasql_mvcc::Visibility::Visible`]. When it
    /// returns `false`, some earlier in-place update is invisible to the
    /// snapshot: readers must substitute the undo pre-image, and mutators
    /// must raise the same retryable conflict as for a pending in-place
    /// update instead of acting on bytes the snapshot cannot see.
    pub fn undo_slot_state_current<O>(
        &self,
        rel: RelationId,
        tid: TupleId,
        current_payload: &[u8],
        snapshot: &ultrasql_mvcc::Snapshot,
        oracle: &O,
    ) -> bool
    where
        O: ultrasql_mvcc::XidStatusOracle + ?Sized,
    {
        let Some(log_handle) = self
            .undo_log
            .get(&rel)
            .map(|handle| Arc::clone(handle.value()))
        else {
            return true;
        };
        let log = log_handle.read();
        undo_pre_image_from_log(&log, tid, current_payload, snapshot, oracle).is_none()
    }

    pub(crate) fn remember_rollback_stamp_page(&self, xid: Xid, page_id: PageId) {
        let xid_raw = xid.raw();
        if xid_raw == 0 {
            return;
        }
        let entry = self
            .rollback_stamp_pages
            .entry(xid_raw)
            .or_insert_with(|| parking_lot::Mutex::new(RollbackStampPages::default()));
        let mut pages = entry.lock();
        pages.insert(page_id);
    }

    /// Remember a batch of pages stamped by one transaction under one map/lock
    /// acquisition.
    ///
    /// The iterator's first-seen order is preserved and duplicates are ignored,
    /// matching repeated calls to [`Self::remember_rollback_stamp_page`].
    pub(crate) fn remember_rollback_stamp_pages(
        &self,
        xid: Xid,
        page_ids: impl IntoIterator<Item = PageId>,
    ) {
        let xid_raw = xid.raw();
        if xid_raw == 0 {
            return;
        }
        let mut page_ids = page_ids.into_iter();
        let Some(first_page) = page_ids.next() else {
            return;
        };
        let entry = self
            .rollback_stamp_pages
            .entry(xid_raw)
            .or_insert_with(|| parking_lot::Mutex::new(RollbackStampPages::default()));
        let mut pages = entry.lock();
        pages.insert(first_page);
        pages.extend(page_ids);
    }

    #[cfg(test)]
    pub(crate) fn take_rollback_stamp_pages(&self, xid: Xid) -> Vec<PageId> {
        self.rollback_stamp_pages
            .remove(&xid.raw())
            .map_or_else(Vec::new, |(_, pages)| pages.into_inner().snapshot())
    }

    /// Snapshot pages still requiring DELETE/classic-UPDATE stamp rollback.
    ///
    /// Unlike [`Self::take_rollback_stamp_pages`], this leaves the registry
    /// intact. Rollback removes one page only after its full validation and
    /// restoration succeed, making a later retry safe after a page error.
    pub(crate) fn rollback_stamp_pages_snapshot(&self, xid: Xid) -> Vec<PageId> {
        self.rollback_stamp_pages
            .get(&xid.raw())
            .map_or_else(Vec::new, |pages| pages.lock().snapshot())
    }

    /// Forget page stamps retained only for possible transaction rollback.
    ///
    /// Recovery registers DELETE and classical UPDATE source pages before it
    /// knows whether their writer committed. Once commit status is rebuilt, a
    /// committed writer no longer needs physical rollback bookkeeping; its
    /// MVCC headers remain authoritative on the page.
    pub fn discard_rollback_stamp_pages(&self, xid: Xid) {
        self.rollback_stamp_pages.remove(&xid.raw());
    }

    /// Mark one registered page restored and remove the transaction entry
    /// atomically when no page remains.
    pub(crate) fn mark_rollback_stamp_page_restored(&self, xid: Xid, page_id: PageId) {
        let xid_raw = xid.raw();
        self.rollback_stamp_pages.remove_if(&xid_raw, |_, pages| {
            let mut pages = pages.lock();
            pages.mark_restored(page_id);
            pages.is_empty()
        });
    }

    /// Borrow the buffer pool's WAL sink, if any.
    ///
    /// Convenience accessor for callers (fused executor paths, the
    /// pipeline lowerer) that want to thread the same sink they hold
    /// for the rest of the statement into the in-place UPDATE /
    /// DELETE entry points without reaching through the pool field
    /// directly.
    #[must_use]
    pub fn wal_sink(&self) -> Option<&Arc<dyn crate::wal_sink::WalSink>> {
        self.pool.wal_sink()
    }

    /// Number of blocks the heap has allocated to `rel`.
    ///
    /// This is the resident block count. Callers that drive a scan from
    /// catalog metadata should use the larger of this value and the
    /// catalog's durable `n_blocks` hint.
    #[must_use]
    pub fn block_count(&self, rel: RelationId) -> u32 {
        self.block_counters
            .get(&rel)
            .map_or(0, |c| c.load(Ordering::Acquire))
    }

    /// Seed `rel`'s in-memory block counter to at least `blocks`.
    ///
    /// Monotonic (fetch-max): never lowers a counter WAL replay has already
    /// advanced further. Recovery calls this from the durable on-disk segment
    /// sizes so relation sizes survive even when low WAL segments — including the
    /// relation-extend records — have been recycled and the replayed stream no
    /// longer starts at LSN 0. Without it, a scan would stop short of pages that
    /// are durably present, silently dropping rows (and whole catalog tables).
    pub fn seed_block_count(&self, rel: RelationId, blocks: u32) {
        if blocks == 0 {
            return;
        }
        let counter = self.counter_for(rel);
        let mut current = counter.load(Ordering::Acquire);
        while current < blocks {
            match counter.compare_exchange_weak(
                current,
                blocks,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
    }

    /// Borrow the underlying buffer pool.
    ///
    /// Exposed so subsystems that need raw page access against the
    /// same pool — notably the server's `CREATE INDEX` path, which
    /// instantiates a [`crate::btree::BTree`] over the same pool used
    /// by the heap — can clone the inner `Arc` without going through
    /// `HeapAccess`'s tuple-oriented API. Returning a `&Arc<...>`
    /// keeps the call non-allocating; callers `Arc::clone` if they
    /// need a fresh owned handle.
    #[must_use]
    pub const fn buffer_pool(&self) -> &Arc<BufferPool<L>> {
        &self.pool
    }
}

pub use scan::{HeapScan, VisibleHeapScan};
pub use vacuum::VacuumStats;
pub use walker::{VisibleHeapWalker, VisibleTuple};
