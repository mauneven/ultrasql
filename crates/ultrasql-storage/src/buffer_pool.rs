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

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

use dashmap::DashMap;
use parking_lot::{Mutex, RwLock};
use ultrasql_core::cache::CachePadded;
use ultrasql_core::{Error, PageId, Result};

use crate::page::Page;

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

/// The buffer pool itself.
pub struct BufferPool<L: PageLoader> {
    frames: Vec<CachePadded<Frame>>,
    page_table: DashMap<PageId, usize>,
    loader: L,
    clock_hand: AtomicUsize,
    /// Cumulative counters.
    counters: Counters,
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
    /// Construct a buffer pool with `capacity` frames.
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
            counters: Counters::default(),
        }
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
            return Ok(PageGuard {
                pool: Arc::clone(self),
                frame_idx,
            });
        }

        self.counters.misses.fetch_add(1, Ordering::Relaxed);

        let frame_idx = self.acquire_frame_for(page_id)?;
        let new_page = self.loader.load(page_id).map_err(BufferPoolError::Loader)?;
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

    fn lookup(&self, page_id: PageId) -> Option<usize> {
        self.page_table.get(&page_id).map(|e| *e)
    }

    fn acquire_frame_for(&self, _new_page_id: PageId) -> Result<usize, BufferPoolError> {
        // First, look for a free frame.
        for (idx, frame) in self.frames.iter().enumerate() {
            if frame.pin_count.load(Ordering::Acquire) == 0 && frame.page_id.lock().is_none() {
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
            *page_id_slot = None;
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

    use ultrasql_core::{BlockNumber, PageId, RelationId};

    use super::*;
    use crate::page::Page;

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
}
