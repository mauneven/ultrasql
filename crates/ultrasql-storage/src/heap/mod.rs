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
//! For v0.5 the heap owns an internal per-relation atomic counter that
//! grows whenever an insert fails to find free space in an existing
//! block. The [`Catalog`] trait is stubbed here for a future
//! `ultrasql-catalog` agent to implement; once wired, the catalog
//! becomes the authoritative source of block counts and the internal
//! counter goes away.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use dashmap::DashMap;
use smallvec::SmallVec;
use ultrasql_core::{BlockNumber, CommandId, RelationId, TupleId, Xid};
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

    /// The decoded slot is too short to hold a full [`TupleHeader`], or
    /// the header bytes failed to parse.
    #[error("malformed tuple header: {0}")]
    MalformedHeader(&'static str),

    /// The relation's block counter has been exhausted. A relation
    /// would have to grow past [`u32::MAX`] blocks for this to fire.
    #[error("relation is out of blocks")]
    OutOfBlocks,

    /// The [`WalSink`] rejected a record.
    #[error("wal sink: {0}")]
    Wal(#[from] WalSinkError),

    /// Encoding a typed WAL payload failed.
    #[error("wal payload encoding: {0}")]
    WalPayload(#[from] PayloadError),
}

/// Options threaded into an insert.
///
/// The caller knows its transaction id and the current command id within
/// that transaction; the heap stamps both into the tuple header before
/// writing the slot.
///
/// The optional `wal` sink, when present, receives a fully-formed
/// `HeapInsert` WAL record after the tuple has been written to the page.
/// Pass `None` to skip WAL emission (e.g. during recovery or in tests
/// that do not care about WAL output).
///
/// The optional `fsm` reference, when present, is consulted to locate an
/// existing block with sufficient free space before allocating a new block,
/// and is updated after the insert to reflect the page's new free space.
///
/// The optional `vm` reference, when present, has the affected page's VM
/// bits cleared after each insert to indicate the page is no longer
/// all-visible.
#[derive(Clone, Copy)]
pub struct InsertOptions<'a> {
    /// XID of the inserting transaction.
    pub xmin: Xid,
    /// Command id within `xmin` that issued the insert.
    pub command_id: CommandId,
    /// Optional WAL sink. When `Some`, the heap appends a
    /// `RecordType::HeapInsert` record after a successful insert.
    pub wal: Option<&'a dyn WalSink>,
    /// Optional free-space map. When `Some`, the heap uses the FSM to
    /// locate a target block before the linear scan, and updates the FSM
    /// after a successful insert.
    pub fsm: Option<&'a crate::fsm::FreeSpaceMap>,
    /// Optional visibility map. When `Some`, the heap clears the page's
    /// all-visible bit after a successful insert.
    pub vm: Option<&'a crate::vm::VisibilityMap>,
}

impl std::fmt::Debug for InsertOptions<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InsertOptions")
            .field("xmin", &self.xmin)
            .field("command_id", &self.command_id)
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
/// The optional `wal` sink, when present, receives a fully-formed
/// `HeapUpdate` WAL record after the new version has been written and the
/// old tuple's header has been stamped. The record's flags will have
/// [`ultrasql_wal::payload::HEAP_UPDATE_HOT`] set when the update was
/// performed as HOT.
///
/// The optional `vm` reference, when present, has both the old and new
/// pages' VM bits cleared after the update.
#[derive(Clone, Copy)]
pub struct UpdateOptions<'a> {
    /// XID performing the update (stamped as `xmax` on the old version
    /// and `xmin` on the new version).
    pub xid: Xid,
    /// Command id within `xid`.
    pub command_id: CommandId,
    /// `true` if no indexed column changed — a HOT update is allowed.
    pub hot_eligible: bool,
    /// Optional WAL sink. When `Some`, the heap appends a
    /// `RecordType::HeapUpdate` record after a successful update.
    pub wal: Option<&'a dyn WalSink>,
    /// Optional visibility map. When `Some`, the heap clears both the old
    /// and new pages' all-visible bits after a successful update.
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
/// The optional `wal` sink, when present, receives a fully-formed
/// `HeapDelete` WAL record after the tuple's header has been stamped.
///
/// The optional `fsm` reference, when present, is updated with the page's
/// new free space after the delete (the space is not immediately reclaimed
/// until VACUUM, but we optimistically record the dead-tuple size as free
/// so future inserters see the block as a candidate).
///
/// The optional `vm` reference, when present, has the affected page's VM
/// bits cleared after a successful delete.
#[derive(Clone, Copy)]
pub struct DeleteOptions<'a> {
    /// XID performing the delete (stamped as `xmax` in the tuple header).
    pub xmax: Xid,
    /// Command id within `xmax` that issued the delete.
    pub cmax: CommandId,
    /// Optional WAL sink. When `Some`, the heap appends a
    /// `RecordType::HeapDelete` record after a successful delete.
    pub wal: Option<&'a dyn WalSink>,
    /// Optional free-space map. When `Some`, the heap records the page's
    /// post-delete free space so future inserters can find the block.
    pub fsm: Option<&'a crate::fsm::FreeSpaceMap>,
    /// Optional visibility map. When `Some`, the heap clears the page's
    /// all-visible bit after a successful delete.
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
    /// page-major UPDATE walker, which trivially yields entries
    /// sorted by `tid`. Lookup is a single binary search across the
    /// relation's Vec.
    ///
    /// VACUUM is responsible for trimming entries whose `writer_xid`
    /// is older than every live snapshot's `xmin` (no live reader
    /// could need that pre-image any more); v0.7+ work.
    pub undo_log: Arc<DashMap<RelationId, parking_lot::RwLock<UndoRelationLog>>>,
}

/// Per-relation undo log entries, sorted by `tid` (ascending).
#[derive(Debug, Default)]
pub struct UndoRelationLog {
    /// Entries in ascending `tid` order. Appenders pushing
    /// monotonically-increasing TIDs preserve the sort. Readers
    /// binary-search.
    pub entries: Vec<UndoEntry>,
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
    /// The pre-update payload bytes (no tuple header). The current
    /// in-place fast path stores exactly the 9-byte `(null, id, val)`
    /// body for `(Int32, Int32)` rows.
    pub old_payload: [u8; 9],
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
mod update;
mod update_inplace;
mod vacuum;
mod wal_emit;
mod walker;

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
            last_checkpoint_lsn: Arc::new(AtomicU64::new(0)),
            column_cache: Arc::new(crate::column_cache::ColumnCache::new()),
            undo_log: Arc::new(DashMap::new()),
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
            last_checkpoint_lsn,
            column_cache: Arc::new(crate::column_cache::ColumnCache::new()),
            undo_log: Arc::new(DashMap::new()),
        }
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
    /// This is the v0.5 stand-in for a catalog query. Callers that need
    /// to drive a scan should pass this value to [`Self::scan`].
    #[must_use]
    pub fn block_count(&self, rel: RelationId) -> u32 {
        self.block_counters
            .get(&rel)
            .map_or(0, |c| c.load(Ordering::Acquire))
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
pub use walker::VisibleHeapWalker;
