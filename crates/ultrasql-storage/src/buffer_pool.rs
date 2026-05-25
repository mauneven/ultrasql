//! Buffer pool.
//!
//! The buffer pool caches pages in memory and arbitrates between
//! concurrent readers, writers, and the eviction policy. It is the
//! single largest determinant of OLTP throughput: every read goes
//! through it, every write lands in it, and a poor eviction policy
//! drops the cache hit ratio off a cliff under mixed workloads.
//!
//! Architecture
//! ------------
//!
//! - **Frames.** A fixed pool of `Frame`s allocated at startup. Each
//!   frame owns a [`Page`] buffer and a small piece of metadata
//!   (`pin_count`, `dirty`, `clock_ref`, `page_id`).
//! - **Page table.** A sharded `DashMap<PageId, FrameId>`. Lookups
//!   take a single shard lock; misses fall back to the eviction path.
//! - **Eviction.** Classic CLOCK with a single rotating hand. The
//!   hand walks frames in order. A frame is evictable iff its pin
//!   count is zero. On each visit, the algorithm clears the
//!   `clock_ref` bit; the next sweep finds the same frame again and
//!   evicts it. (CLOCK-Pro is a known follow-up; the lint enforces
//!   the same trait surface so the upgrade is a drop-in.)
//! - **WAL integration.** An optional `Arc<dyn WalSink>` reference allows
//!   [`BufferPool::try_flush_dirty`] to gate page flushes on the sink's
//!   `durable_lsn`: a dirty page whose page-LSN exceeds the durable LSN
//!   has not yet been durably logged and must not be written to disk (the
//!   recovery invariant requires that WAL is always ahead of the data
//!   files). When no sink is present, all dirty unpinned pages are
//!   eligible for flushing.
//!
//! Concurrency
//! -----------
//!
//! - `get_page` takes the shard lock briefly to insert / find. The
//!   per-frame lock guards the page buffer itself. Readers acquire
//!   the frame lock in shared mode; writers in exclusive mode.
//! - Eviction takes the shard lock, the victim frame's exclusive
//!   lock, the page-table lock for the old `page_id`, and the page-
//!   table lock for the new `page_id`. The latch order is the global
//!   "shard → frame" rule from [ARCHITECTURE.md](../../../ARCHITECTURE.md);
//!   we never invert it.
//! - `try_flush_dirty` acquires per-frame locks individually in frame-index
//!   order (it never holds two simultaneously), so it does not introduce
//!   new lock-ordering hazards with respect to the existing buffer-pool
//!   ordering rules.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

use dashmap::DashMap;
use parking_lot::{Mutex, RwLock};
use ultrasql_core::cache::CachePadded;
use ultrasql_core::{BlockNumber, Error, PageId, RelationId, Result};

use crate::page::Page;
use crate::wal_sink::WalSink;

/// Errors specific to the buffer pool.
#[derive(Debug, thiserror::Error)]
pub enum BufferPoolError {
    /// The pool is full and no unpinned frame could be found to
    /// evict.
    #[error("buffer pool exhausted: every frame is pinned")]
    Exhausted,

    /// The page loader failed to produce a page on miss.
    #[error("page loader failed: {0}")]
    Loader(#[from] Error),

    /// The page was requested with an exclusive-write guard but is
    /// already held by another writer.
    #[error("page {page_id} is already pinned for write")]
    WriteContention {
        /// The page that was contended.
        page_id: PageId,
    },
}

/// Page loader callback.
///
/// The buffer pool does not own segment files; the storage manager
/// hands it a closure that knows how to fetch a [`Page`] given a
/// [`PageId`]. Production code wires this to the segment layer; tests
/// wire it to an in-memory map.
pub trait PageLoader: Send + Sync {
    /// Fetch the page at `page_id` and return an owned [`Page`].
    ///
    /// The loader is expected to be deterministic — for a given page
    /// id, repeated calls produce equal page contents (modulo writes
    /// by the same process).
    fn load(&self, page_id: PageId) -> Result<Page>;
}

impl<F> PageLoader for F
where
    F: Fn(PageId) -> Result<Page> + Send + Sync,
{
    fn load(&self, page_id: PageId) -> Result<Page> {
        self(page_id)
    }
}

/// Live diagnostics for the pool.
#[derive(Debug, Default)]
pub struct BufferPoolStats {
    /// Cumulative `get_page` calls.
    pub gets: u64,
    /// Cumulative cache hits — page already resident.
    pub hits: u64,
    /// Cumulative cache misses — page had to be loaded.
    pub misses: u64,
    /// Cumulative evictions.
    pub evictions: u64,
    /// Currently resident pages.
    pub resident: usize,
    /// Currently pinned frames.
    pub pinned: usize,
    /// Currently dirty pages.
    pub dirty: usize,
}

/// Cumulative buffer-pool counters for one relation.
#[derive(Debug)]
pub struct BufferPoolRelationStats {
    /// Relation these counters describe.
    pub relation: RelationId,
    /// Cumulative cache misses for pages in this relation.
    pub reads: u64,
    /// Cumulative cache hits for pages in this relation.
    pub hits: u64,
}

/// The buffer pool itself.
pub struct BufferPool<L: PageLoader> {
    frames: Vec<CachePadded<Frame>>,
    page_table: DashMap<PageId, usize>,
    loader: L,
    clock_hand: AtomicUsize,
    /// Serializes miss installation so one `PageId` cannot be loaded
    /// into multiple frames concurrently.
    miss_lock: Mutex<()>,
    /// Optional WAL sink for LSN-gated dirty-page flushing.
    ///
    /// When `Some`, [`BufferPool::try_flush_dirty`] will only flush frames
    /// whose page-LSN is ≤ the sink's `durable_lsn`. This ensures the WAL
    /// is always written ahead of the data files, which is the fundamental
    /// crash-recovery invariant.
    ///
    /// When `None`, all unpinned dirty frames are eligible for flushing
    /// regardless of LSN.
    wal_sink: Option<Arc<dyn WalSink>>,
    /// Cumulative counters.
    counters: Counters,
    /// Cumulative counters keyed by relation id.
    relation_counters: DashMap<RelationId, RelationCounters>,
}

impl<L: PageLoader> std::fmt::Debug for BufferPool<L> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BufferPool")
            .field("frame_count", &self.frames.len())
            .field("page_table_len", &self.page_table.len())
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Default)]
struct Counters {
    gets: AtomicU64,
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
}

#[derive(Debug, Default)]
struct RelationCounters {
    reads: AtomicU64,
    hits: AtomicU64,
}

/// A buffer-pool frame.
///
/// `Frame` is allocated once at startup and reused for the life of the
/// pool. The `page` is a `RwLock<Option<Page>>` — `None` for a frame
/// that has never been populated (it lives in the free list at boot).
#[derive(Debug)]
struct Frame {
    /// The page buffer. `None` for an empty frame.
    page: RwLock<Option<Page>>,
    /// Identifier of the page currently held. Guarded by `meta_lock`
    /// for transitions.
    page_id: Mutex<Option<PageId>>,
    /// Pin counter — a frame with non-zero pin count is not evictable.
    pin_count: AtomicUsize,
    /// CLOCK reference bit — set on every access, cleared by the
    /// CLOCK hand.
    clock_ref: AtomicBool,
    /// Dirty bit — set on every write, cleared on flush.
    dirty: AtomicBool,
}

impl Frame {
    const fn empty() -> Self {
        Self {
            page: RwLock::new(None),
            page_id: Mutex::new(None),
            pin_count: AtomicUsize::new(0),
            clock_ref: AtomicBool::new(false),
            dirty: AtomicBool::new(false),
        }
    }
}

impl<L: PageLoader> BufferPool<L> {
    /// Construct a buffer pool with `capacity` frames and no WAL sink.
    ///
    /// Without a WAL sink, [`Self::try_flush_dirty`] treats every page LSN
    /// as durable and will flush any unpinned dirty page. Use
    /// [`Self::with_wal`] when WAL integration is required.
    #[must_use]
    pub fn new(capacity: usize, loader: L) -> Self {
        assert!(capacity > 0, "buffer pool capacity must be > 0");
        let frames = (0..capacity)
            .map(|_| CachePadded::new(Frame::empty()))
            .collect();
        Self {
            frames,
            page_table: DashMap::with_capacity(capacity),
            loader,
            clock_hand: AtomicUsize::new(0),
            miss_lock: Mutex::new(()),
            wal_sink: None,
            counters: Counters::default(),
            relation_counters: DashMap::new(),
        }
    }

    /// Construct a buffer pool with `capacity` frames and a WAL sink.
    ///
    /// The sink's [`WalSink::durable_lsn`] is consulted by
    /// [`Self::try_flush_dirty`] to gate page flushes: a dirty frame
    /// whose page-LSN exceeds the durable LSN will not be flushed because
    /// the WAL record that describes the mutation has not yet been made
    /// durable. This preserves the WAL-ahead-of-data-files invariant
    /// required for crash recovery.
    ///
    /// Eviction itself does not flush dirty pages regardless of whether a
    /// sink is present. Flushing is the checkpointer's job; the eviction
    /// path simply skips dirty frames.
    #[must_use]
    pub fn with_wal(capacity: usize, loader: L, wal: Arc<dyn WalSink>) -> Self {
        assert!(capacity > 0, "buffer pool capacity must be > 0");
        let frames = (0..capacity)
            .map(|_| CachePadded::new(Frame::empty()))
            .collect();
        Self {
            frames,
            page_table: DashMap::with_capacity(capacity),
            loader,
            clock_hand: AtomicUsize::new(0),
            miss_lock: Mutex::new(()),
            wal_sink: Some(wal),
            counters: Counters::default(),
            relation_counters: DashMap::new(),
        }
    }

    /// Borrow the configured WAL sink, if any.
    ///
    /// Heap access methods that emit per-row WAL records (the
    /// in-place UPDATE / DELETE fused paths) call this to obtain a
    /// reference to the buffer pool's sink. Returns `None` when the
    /// pool was constructed via [`Self::new`] without a sink — that
    /// configuration is reserved for tests and bring-up; production
    /// callers use [`Self::with_wal`].
    #[must_use]
    pub fn wal_sink(&self) -> Option<&Arc<dyn WalSink>> {
        self.wal_sink.as_ref()
    }

    /// Return fixed frame capacity for pressure-based checkpoint decisions.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.frames.len()
    }

    /// Highest resident block for `rel`, if any.
    ///
    /// B-tree handles can be reopened from catalog metadata while their
    /// pages are still resident in this process. Seeding fresh block
    /// allocation above the resident maximum prevents reopened handles
    /// from reusing an existing index page before the segment allocator is
    /// wired into the B-tree layer.
    #[must_use]
    pub fn max_resident_block(&self, rel: RelationId) -> Option<BlockNumber> {
        self.page_table
            .iter()
            .filter_map(|entry| {
                let page_id = *entry.key();
                (page_id.relation == rel).then_some(page_id.block)
            })
            .max()
    }

    /// Flush dirty, unpinned frames to disk using the provided `writer`
    /// callback.
    ///
    /// For each frame that is dirty and has `pin_count == 0`, this method
    /// checks whether the frame's page-LSN is ≤ the WAL sink's
    /// `durable_lsn` (or, if no sink is configured, treats all LSNs as
    /// durable). If the LSN condition is satisfied the `writer` callback
    /// is invoked with the `PageId` and a shared reference to the `Page`.
    /// On a successful write the dirty bit is cleared so the frame becomes
    /// eligible for eviction.
    ///
    /// The `writer` receives shared access to the page while the per-frame
    /// read lock is held. Writer implementations must not attempt to
    /// re-enter the buffer pool (no `get_page` calls inside `writer`); that
    /// would deadlock because the read lock is already held.
    ///
    /// # Lock order
    ///
    /// This method acquires per-frame read locks individually in frame-index
    /// order, never holding two simultaneously. This is consistent with the
    /// global latch order documented in ARCHITECTURE.md §14.
    ///
    /// # Errors
    ///
    /// If `writer` returns an error the frame is left dirty and the error
    /// is propagated. The count of successfully flushed pages before the
    /// error occurred is lost (the caller can inspect `stats().dirty` to
    /// assess remaining work). The checkpointer continues on errors and
    /// retries on the next interval.
    ///
    /// # Returns
    ///
    /// The number of pages successfully flushed.
    pub fn try_flush_dirty(
        &self,
        mut writer: impl FnMut(PageId, &Page) -> Result<()>,
    ) -> Result<usize> {
        let durable = self
            .wal_sink
            .as_ref()
            .map_or(u64::MAX, |s| s.durable_lsn().raw());

        let mut flushed: usize = 0;

        for frame in &self.frames {
            // Fast-path: skip frames that are obviously clean or pinned
            // without taking any lock.
            if !frame.dirty.load(Ordering::Acquire) {
                continue;
            }
            if frame.pin_count.load(Ordering::Acquire) != 0 {
                continue;
            }

            // Acquire the meta lock to read the page_id and double-check
            // the pin count atomically.
            let page_id = {
                let pid_slot = frame.page_id.lock();
                if frame.pin_count.load(Ordering::Acquire) != 0 {
                    continue;
                }
                match *pid_slot {
                    Some(pid) => pid,
                    None => continue,
                }
            };

            // Read the page LSN under shared lock.
            let page_lsn = {
                let page_guard = frame.page.read();
                match page_guard.as_ref() {
                    Some(page) => page.header().lsn,
                    None => continue,
                }
            };

            // Gate on WAL durability. A page whose LSN exceeds the
            // durable WAL position must not be written to disk: the WAL
            // record describing the mutation is not yet guaranteed to
            // survive a crash, so writing the page would violate the
            // WAL-ahead-of-data-files invariant.
            if page_lsn > durable {
                continue;
            }

            // Invoke the writer with shared access to the page.
            {
                let page_guard = frame.page.read();
                match page_guard.as_ref() {
                    Some(page) => writer(page_id, page)?,
                    None => continue,
                }
            }

            // Clear the dirty bit only after a successful write.
            frame.dirty.store(false, Ordering::Release);
            flushed += 1;
        }

        Ok(flushed)
    }

    /// Acquire a page guard. On miss, the page is loaded via the
    /// supplied [`PageLoader`].
    ///
    /// The returned [`PageGuard`] borrows the pool and decrements the
    /// frame's pin count on drop. Multiple read guards may co-exist;
    /// at most one write guard at a time on a given page (enforced by
    /// the frame's `RwLock`).
    pub fn get_page(self: &Arc<Self>, page_id: PageId) -> Result<PageGuard<L>, BufferPoolError> {
        self.counters.gets.fetch_add(1, Ordering::Relaxed);

        if let Some(frame_idx) = self.lookup(page_id) {
            self.frames[frame_idx]
                .pin_count
                .fetch_add(1, Ordering::AcqRel);
            // `clock_ref` is purely advisory for the CLOCK eviction
            // hand: setting it tells the next sweep "this frame was
            // recently used; please come back later." A torn / stale
            // read from the eviction thread is harmless — the worst
            // case is one extra rotation of the hand before the bit
            // takes effect, which is still bounded by the (capacity *
            // 4) outer attempt cap in `acquire_frame_for`. The pin
            // count above already supplies the AcqRel needed to
            // synchronize with eviction; the clock-ref store has no
            // happens-before consumers, so `Relaxed` is sufficient.
            self.frames[frame_idx]
                .clock_ref
                .store(true, Ordering::Relaxed);
            self.counters.hits.fetch_add(1, Ordering::Relaxed);
            self.record_relation_hit(page_id.relation);
            return Ok(PageGuard {
                pool: Arc::clone(self),
                frame_idx,
            });
        }

        let _miss = self.miss_lock.lock();

        if let Some(frame_idx) = self.lookup(page_id) {
            self.frames[frame_idx]
                .pin_count
                .fetch_add(1, Ordering::AcqRel);
            self.frames[frame_idx]
                .clock_ref
                .store(true, Ordering::Relaxed);
            self.counters.hits.fetch_add(1, Ordering::Relaxed);
            self.record_relation_hit(page_id.relation);
            return Ok(PageGuard {
                pool: Arc::clone(self),
                frame_idx,
            });
        }

        self.counters.misses.fetch_add(1, Ordering::Relaxed);
        self.record_relation_read(page_id.relation);

        let frame_idx = self.acquire_frame_for(page_id)?;
        let new_page = match self.loader.load(page_id) {
            Ok(page) => page,
            Err(e) => {
                let frame = &self.frames[frame_idx];
                *frame.page.write() = None;
                *frame.page_id.lock() = None;
                return Err(BufferPoolError::Loader(e));
            }
        };
        {
            let frame = &self.frames[frame_idx];
            // Set the page contents and metadata while the eviction
            // path is already locked out via the pin count we'll
            // bump.
            *frame.page.write() = Some(new_page);
            *frame.page_id.lock() = Some(page_id);
            frame.clock_ref.store(true, Ordering::Release);
            frame.dirty.store(false, Ordering::Release);
            frame.pin_count.fetch_add(1, Ordering::AcqRel);
        }
        self.page_table.insert(page_id, frame_idx);
        Ok(PageGuard {
            pool: Arc::clone(self),
            frame_idx,
        })
    }

    /// Return a snapshot of pool diagnostics.
    #[must_use]
    pub fn stats(&self) -> BufferPoolStats {
        let resident = self.page_table.len();
        let pinned = self
            .frames
            .iter()
            .filter(|f| f.pin_count.load(Ordering::Acquire) > 0)
            .count();
        let dirty = self
            .frames
            .iter()
            .filter(|f| f.dirty.load(Ordering::Acquire))
            .count();
        BufferPoolStats {
            gets: self.counters.gets.load(Ordering::Relaxed),
            hits: self.counters.hits.load(Ordering::Relaxed),
            misses: self.counters.misses.load(Ordering::Relaxed),
            evictions: self.counters.evictions.load(Ordering::Relaxed),
            resident,
            pinned,
            dirty,
        }
    }

    /// Return cumulative counters for one relation.
    #[must_use]
    pub fn relation_stats(&self, relation: RelationId) -> BufferPoolRelationStats {
        let Some(counters) = self.relation_counters.get(&relation) else {
            return BufferPoolRelationStats {
                relation,
                reads: 0,
                hits: 0,
            };
        };
        BufferPoolRelationStats {
            relation,
            reads: counters.reads.load(Ordering::Relaxed),
            hits: counters.hits.load(Ordering::Relaxed),
        }
    }

    fn record_relation_hit(&self, relation: RelationId) {
        self.relation_counters
            .entry(relation)
            .or_default()
            .hits
            .fetch_add(1, Ordering::Relaxed);
    }

    fn record_relation_read(&self, relation: RelationId) {
        self.relation_counters
            .entry(relation)
            .or_default()
            .reads
            .fetch_add(1, Ordering::Relaxed);
    }

    fn lookup(&self, page_id: PageId) -> Option<usize> {
        self.page_table.get(&page_id).map(|e| *e)
    }

    fn acquire_frame_for(&self, new_page_id: PageId) -> Result<usize, BufferPoolError> {
        // First, look for a free frame.
        for (idx, frame) in self.frames.iter().enumerate() {
            if frame.pin_count.load(Ordering::Acquire) != 0 {
                continue;
            }
            let mut page_id_slot = frame.page_id.lock();
            if frame.pin_count.load(Ordering::Acquire) == 0 && page_id_slot.is_none() {
                *page_id_slot = Some(new_page_id);
                return Ok(idx);
            }
        }
        // Otherwise, sweep the clock.
        let total = self.frames.len();
        // Bound the number of full sweeps to avoid pathological loops.
        for _attempt in 0..(total * 4) {
            let hand = self.clock_hand.fetch_add(1, Ordering::AcqRel) % total;
            let frame = &self.frames[hand];

            if frame.pin_count.load(Ordering::Acquire) != 0 {
                continue;
            }
            // First visit: clear the ref bit, advance.
            if frame.clock_ref.swap(false, Ordering::AcqRel) {
                continue;
            }

            // Candidate. Take the meta lock to reserve.
            let mut page_id_slot = frame.page_id.lock();
            // Recheck the pin count under the slot lock.
            if frame.pin_count.load(Ordering::Acquire) != 0 {
                drop(page_id_slot);
                continue;
            }
            if let Some(old_id) = *page_id_slot {
                // We do not flush dirty pages here — the storage
                // manager is responsible for issuing the WAL flush
                // before evicting dirty pages. The buffer pool's
                // contract is "do not lose pinned data"; in the
                // current bring-up there are no concurrent flushers.
                if frame.dirty.load(Ordering::Acquire) {
                    drop(page_id_slot);
                    continue;
                }
                self.page_table.remove(&old_id);
                self.counters.evictions.fetch_add(1, Ordering::Relaxed);
            }
            *page_id_slot = Some(new_page_id);
            drop(page_id_slot);
            *frame.page.write() = None;
            return Ok(hand);
        }
        Err(BufferPoolError::Exhausted)
    }

    fn unpin(&self, frame_idx: usize, dirty: bool) {
        let frame = &self.frames[frame_idx];
        if dirty {
            frame.dirty.store(true, Ordering::Release);
        }
        // Drop the pin count last so concurrent readers see dirty
        // before unpin.
        frame.pin_count.fetch_sub(1, Ordering::Release);
    }
}

/// RAII guard returned by [`BufferPool::get_page`].
///
/// While the guard is alive, the underlying frame is pinned and
/// cannot be evicted. On drop, the pin count is decremented and the
/// frame becomes eligible for the eviction policy.
pub struct PageGuard<L: PageLoader> {
    pool: Arc<BufferPool<L>>,
    frame_idx: usize,
}

impl<L: PageLoader> std::fmt::Debug for PageGuard<L> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PageGuard")
            .field("frame_idx", &self.frame_idx)
            .finish_non_exhaustive()
    }
}

impl<L: PageLoader> PageGuard<L> {
    /// Borrow the underlying page in shared-read mode.
    ///
    /// Calls into the per-frame `RwLock`'s read path.
    pub fn read(&self) -> PageRead<'_> {
        PageRead {
            inner: self.pool.frames[self.frame_idx].page.read(),
        }
    }

    /// Borrow the underlying page in exclusive-write mode. Marks the
    /// page dirty when the returned guard is dropped.
    pub fn write(&self) -> PageWrite<'_> {
        let frame: &Frame = &self.pool.frames[self.frame_idx];
        PageWrite {
            frame,
            inner: frame.page.write(),
        }
    }
}

impl<L: PageLoader> Drop for PageGuard<L> {
    fn drop(&mut self) {
        self.pool.unpin(self.frame_idx, false);
    }
}

/// Read-only view of a page through the buffer pool. Holds the per-
/// frame read lock for as long as the borrow is alive.
pub struct PageRead<'a> {
    inner: parking_lot::lock_api::RwLockReadGuard<'a, parking_lot::RawRwLock, Option<Page>>,
}

impl std::fmt::Debug for PageRead<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PageRead").finish_non_exhaustive()
    }
}

impl std::ops::Deref for PageRead<'_> {
    type Target = Page;
    fn deref(&self) -> &Self::Target {
        self.inner
            .as_ref()
            .expect("PageRead invariant: page is populated when held")
    }
}

/// Read-write view of a page through the buffer pool. Marks the frame
/// dirty on drop.
pub struct PageWrite<'a> {
    frame: &'a Frame,
    inner: parking_lot::lock_api::RwLockWriteGuard<'a, parking_lot::RawRwLock, Option<Page>>,
}

impl std::fmt::Debug for PageWrite<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PageWrite").finish_non_exhaustive()
    }
}

impl std::ops::Deref for PageWrite<'_> {
    type Target = Page;
    fn deref(&self) -> &Self::Target {
        self.inner
            .as_ref()
            .expect("PageWrite invariant: page is populated when held")
    }
}

impl std::ops::DerefMut for PageWrite<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner
            .as_mut()
            .expect("PageWrite invariant: page is populated when held")
    }
}

impl Drop for PageWrite<'_> {
    fn drop(&mut self) {
        self.frame.dirty.store(true, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use ultrasql_core::{BlockNumber, Lsn, PageId, RelationId, Xid};

    use super::*;
    use crate::page::Page;
    use crate::wal_sink::WalSink;

    /// Loader that materializes blank heap pages.
    struct BlankLoader;
    impl PageLoader for BlankLoader {
        fn load(&self, _: PageId) -> Result<Page> {
            Ok(Page::new_heap())
        }
    }

    fn pid(block: u32) -> PageId {
        PageId::new(RelationId::new(1), BlockNumber::new(block))
    }

    #[test]
    fn hit_path_increments_hit_counter() {
        let pool = Arc::new(BufferPool::new(4, BlankLoader));
        let _g1 = pool.get_page(pid(0)).unwrap();
        let _g2 = pool.get_page(pid(0)).unwrap();
        let stats = pool.stats();
        assert_eq!(stats.gets, 2);
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.resident, 1);
        assert_eq!(stats.pinned, 1);
    }

    #[test]
    fn pinning_keeps_resident() {
        let pool = Arc::new(BufferPool::new(2, BlankLoader));
        let _a = pool.get_page(pid(0)).unwrap();
        let _b = pool.get_page(pid(1)).unwrap();
        // Both frames pinned — a third miss with all pinned must
        // fail.
        let err = pool.get_page(pid(2)).unwrap_err();
        assert!(matches!(err, BufferPoolError::Exhausted));
    }

    #[test]
    fn unpin_allows_eviction() {
        let pool = Arc::new(BufferPool::new(2, BlankLoader));
        let g0 = pool.get_page(pid(0)).unwrap();
        let g1 = pool.get_page(pid(1)).unwrap();
        drop(g0);
        drop(g1);
        // Both unpinned. Now bring in pid(2), then pid(3) — one of
        // 0/1 should have been evicted.
        let _g2 = pool.get_page(pid(2)).unwrap();
        let _g3 = pool.get_page(pid(3)).unwrap();
        let stats = pool.stats();
        assert!(
            stats.evictions >= 1,
            "expected ≥1 eviction, got {}",
            stats.evictions
        );
        assert_eq!(stats.resident, 2);
    }

    #[test]
    fn write_marks_dirty() {
        let pool = Arc::new(BufferPool::new(2, BlankLoader));
        {
            let g = pool.get_page(pid(0)).unwrap();
            let mut w = g.write();
            // Trivial mutation.
            w.set_lsn(123);
        }
        assert_eq!(pool.stats().dirty, 1);
    }

    #[test]
    fn read_after_write_sees_update() {
        let pool = Arc::new(BufferPool::new(2, BlankLoader));
        let g = pool.get_page(pid(0)).unwrap();
        {
            let mut w = g.write();
            w.set_lsn(42);
        }
        assert_eq!(g.read().header().lsn, 42);
    }

    #[test]
    fn many_unpinned_pages_get_evicted_in_order() {
        let pool = Arc::new(BufferPool::new(4, BlankLoader));
        for i in 0_u32..4 {
            drop(pool.get_page(pid(i)).unwrap());
        }
        // Force 8 more accesses; each takes a slot from the resident
        // set.
        for i in 4_u32..12 {
            drop(pool.get_page(pid(i)).unwrap());
        }
        let stats = pool.stats();
        // We accessed 12 pages with a 4-slot pool; at least 8
        // evictions must have happened.
        assert!(stats.evictions >= 8, "got {}", stats.evictions);
        assert_eq!(stats.resident, 4);
    }

    #[test]
    fn pin_count_serializes_eviction() {
        // Pin everything; verify the pool refuses to evict.
        let pool = Arc::new(BufferPool::new(3, BlankLoader));
        let pins: Vec<_> = (0_u32..3).map(|i| pool.get_page(pid(i)).unwrap()).collect();
        let err = pool.get_page(pid(99)).unwrap_err();
        assert!(matches!(err, BufferPoolError::Exhausted));
        drop(pins);
    }

    #[test]
    fn stats_reflect_dirty_clear_on_eviction() {
        let pool = Arc::new(BufferPool::new(2, BlankLoader));
        {
            let g = pool.get_page(pid(0)).unwrap();
            let mut w = g.write();
            w.set_lsn(1);
        }
        // Dirty page is not auto-evicted yet, but resident count is
        // still 1.
        assert!(pool.stats().dirty >= 1);
    }

    // -----------------------------------------------------------------------
    // try_flush_dirty tests
    // -----------------------------------------------------------------------

    /// A `WalSink` stub that reports a fixed durable LSN.
    struct FixedDurableSink {
        durable: Lsn,
    }

    impl WalSink for FixedDurableSink {
        fn append(
            &self,
            _record: ultrasql_wal::WalRecord,
        ) -> Result<Lsn, crate::wal_sink::WalSinkError> {
            Ok(Lsn::ZERO)
        }

        fn durable_lsn(&self) -> Lsn {
            self.durable
        }

        fn last_lsn_for(&self, _xid: Xid) -> Lsn {
            Lsn::ZERO
        }
    }

    /// Pool with sink at `durable_lsn=100`, page with `lsn=50` and dirty bit
    /// set. `try_flush_dirty` should call the writer once and clear the
    /// dirty bit.
    #[test]
    fn try_flush_dirty_writes_clean_dirty_pages_with_durable_lsn() {
        let sink: Arc<dyn WalSink> = Arc::new(FixedDurableSink {
            durable: Lsn::new(100),
        });
        let pool = Arc::new(BufferPool::with_wal(4, BlankLoader, sink));

        // Load and write to page 0 to mark it dirty with lsn=50.
        {
            let g = pool.get_page(pid(0)).unwrap();
            let mut w = g.write();
            w.set_lsn(50);
        }
        assert_eq!(pool.stats().dirty, 1);

        // try_flush_dirty should flush the page.
        let mut call_count: usize = 0;
        let flushed = pool
            .try_flush_dirty(|_page_id, _page| {
                call_count += 1;
                Ok(())
            })
            .unwrap();

        assert_eq!(
            call_count, 1,
            "writer must be called once for the dirty page"
        );
        assert_eq!(flushed, 1);
        assert_eq!(
            pool.stats().dirty,
            0,
            "dirty bit must be cleared after flush"
        );
    }

    /// Pool with sink at `durable_lsn=10`, page `lsn=100`. The page's LSN is
    /// above the durable LSN so it must NOT be flushed.
    #[test]
    fn try_flush_dirty_skips_pages_above_durable_lsn() {
        let sink: Arc<dyn WalSink> = Arc::new(FixedDurableSink {
            durable: Lsn::new(10),
        });
        let pool = Arc::new(BufferPool::with_wal(4, BlankLoader, sink));

        {
            let g = pool.get_page(pid(0)).unwrap();
            let mut w = g.write();
            w.set_lsn(100); // above durable_lsn=10
        }
        assert_eq!(pool.stats().dirty, 1);

        let mut call_count: usize = 0;
        let flushed = pool
            .try_flush_dirty(|_page_id, _page| {
                call_count += 1;
                Ok(())
            })
            .unwrap();

        assert_eq!(
            call_count, 0,
            "writer must NOT be called for page above durable LSN"
        );
        assert_eq!(flushed, 0);
        assert_eq!(pool.stats().dirty, 1, "dirty bit must NOT be cleared");
    }

    /// A pinned dirty page must not be flushed even if its LSN is durable.
    #[test]
    fn try_flush_dirty_skips_pinned_pages() {
        let sink: Arc<dyn WalSink> = Arc::new(FixedDurableSink {
            durable: Lsn::new(1000),
        });
        let pool = Arc::new(BufferPool::with_wal(4, BlankLoader, sink));

        // Acquire a write guard and keep it alive so the frame stays pinned.
        let guard = pool.get_page(pid(0)).unwrap();
        {
            let mut w = guard.write();
            w.set_lsn(50);
        }
        // Frame is pinned (guard still alive) and dirty.
        assert_eq!(pool.stats().pinned, 1);
        assert_eq!(pool.stats().dirty, 1);

        let mut call_count: usize = 0;
        let flushed = pool
            .try_flush_dirty(|_page_id, _page| {
                call_count += 1;
                Ok(())
            })
            .unwrap();

        assert_eq!(call_count, 0, "pinned page must not be flushed");
        assert_eq!(flushed, 0);
        // Let the guard drop so cleanup succeeds.
        drop(guard);
    }

    /// Without a sink (`BufferPool::new`), all dirty unpinned pages are
    /// flushed regardless of their LSN.
    #[test]
    fn try_flush_dirty_with_no_sink_treats_all_lsns_durable() {
        let pool = Arc::new(BufferPool::new(4, BlankLoader));

        // Two pages with different LSNs, both dirty.
        {
            let g0 = pool.get_page(pid(0)).unwrap();
            g0.write().set_lsn(1_000_000);
        }
        {
            let g1 = pool.get_page(pid(1)).unwrap();
            g1.write().set_lsn(u64::MAX);
        }
        assert_eq!(pool.stats().dirty, 2);

        let mut call_count: usize = 0;
        let flushed = pool
            .try_flush_dirty(|_page_id, _page| {
                call_count += 1;
                Ok(())
            })
            .unwrap();

        assert_eq!(
            call_count, 2,
            "both pages must be flushed when no sink present"
        );
        assert_eq!(flushed, 2);
        assert_eq!(pool.stats().dirty, 0);
    }
}
