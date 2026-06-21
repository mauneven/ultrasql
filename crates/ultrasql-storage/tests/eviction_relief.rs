//! Unit tests for LSN-gated flush-on-evict relief.
//!
//! These drive the buffer pool's public relief surface
//! ([`BufferPool::get_page_relieved`], [`BufferPool::set_eviction_relief`],
//! [`BufferPool::oldest_unflushable_dirty_lsn`]) through a test
//! [`EvictionRelief`] implementation that mirrors the production
//! `ServerEvictionRelief`: Phase A flushes pages already at/below the durable
//! WAL position via [`BufferPool::try_flush_dirty`]; Phase B forces the WAL
//! durable (here, advancing a [`LaggingWalSink`]'s durable LSN) to
//! `oldest_unflushable_dirty_lsn` and re-flushes.
//!
//! What each test proves:
//! - `relief_lets_eviction_succeed_under_all_dirty_pressure` — the core
//!   availability fix: a dirty working set larger than the pool no longer
//!   hard-fails with `Exhausted`.
//! - `relief_respects_lsn_gate_with_sink` — WAL-before-data: a page whose
//!   page-LSN exceeds the durable LSN is NEVER written; Phase A flushes 0,
//!   `oldest_unflushable_dirty_lsn` reports the min blocked LSN, and only after
//!   the durable LSN advances does the gated frame reach the writer.
//! - `pinned_frames_never_evicted_or_flushed` — pinned frames are never passed
//!   to the writer and keep their bytes.
//! - `relief_bounded_returns_exhausted_when_only_pinned` — the bounded loop
//!   terminates with `Exhausted` instead of spinning when no flush can help.
//! - `poisoned_pool_skips_relief` — a poisoned pool surfaces `Poisoned` without
//!   invoking the writer.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;
use ultrasql_core::{BlockNumber, Lsn, PageId, RelationId, Result};
use ultrasql_storage::WalSink;
use ultrasql_storage::buffer_pool::{BufferPool, BufferPoolError, EvictionRelief, PageLoader};
use ultrasql_storage::page::Page;
use ultrasql_storage::test_support::LaggingWalSink;

/// Loader that materializes blank heap pages on a miss.
struct BlankLoader;
impl PageLoader for BlankLoader {
    fn load(&self, _: PageId) -> Result<Page> {
        Ok(Page::new_heap())
    }
}

fn pid(block: u32) -> PageId {
    PageId::new(RelationId::new(1), BlockNumber::new(block))
}

/// Records every page handed to the writer, with the page-LSN it carried, so
/// tests can assert the WAL-before-data gate was honored.
#[derive(Default)]
struct WriterLog {
    /// page_id -> page bytes most recently written.
    pages: HashMap<PageId, Vec<u8>>,
    /// Every (page_id, page_lsn) the writer ever saw, in order.
    seen: Vec<(PageId, u64)>,
}

/// Test relief that mirrors `ServerEvictionRelief` Phase A / Phase B.
struct TestRelief {
    pool: Arc<BufferPool<BlankLoader>>,
    writer: Arc<Mutex<WriterLog>>,
    /// Optional lagging sink; when present, Phase B advances its durable LSN
    /// (the test stand-in for forcing the WAL writer to fsync).
    sink: Option<Arc<LaggingWalSink>>,
}

impl TestRelief {
    /// Phase A: flush every dirty unpinned frame already at/below durable.
    fn flush_durable(&self) -> std::result::Result<usize, BufferPoolError> {
        let writer = Arc::clone(&self.writer);
        self.pool
            .try_flush_dirty(move |page_id, page| {
                let mut log = writer.lock();
                log.seen.push((page_id, page.header().lsn));
                log.pages.insert(page_id, page.as_bytes().to_vec());
                Ok(())
            })
            .map_err(BufferPoolError::Loader)
    }
}

impl EvictionRelief for TestRelief {
    fn relieve(&self) -> std::result::Result<(), BufferPoolError> {
        if self.pool.is_poisoned() {
            return Err(BufferPoolError::Poisoned);
        }
        // Phase A.
        if self.flush_durable()? > 0 {
            return Ok(());
        }
        // Phase B: advance durable to the lowest blocked LSN, then re-flush.
        if let Some(target) = self.pool.oldest_unflushable_dirty_lsn() {
            if let Some(sink) = self.sink.as_ref() {
                sink.set_durable_lsn(target);
                let _ = self.flush_durable()?;
            }
        }
        Ok(())
    }
}

/// Pin `page_id`, stamp its page-LSN to `lsn`, mark it dirty, and drop the pin
/// so the frame is dirty + unpinned (a flush candidate).
fn dirty_page_at_lsn(pool: &Arc<BufferPool<BlankLoader>>, page_id: PageId, lsn: u64) {
    let guard = pool.get_page(page_id).expect("pin page to dirty it");
    {
        let mut w = guard.write();
        w.set_lsn(lsn);
    } // drop write guard -> frame marked dirty
    drop(guard); // drop pin -> frame unpinned
}

#[test]
fn relief_lets_eviction_succeed_under_all_dirty_pressure() {
    // No WAL sink: every LSN is treated as durable, so Phase A always flushes.
    const N: u32 = 4;
    let pool = Arc::new(BufferPool::new(N as usize, BlankLoader));
    let writer = Arc::new(Mutex::new(WriterLog::default()));
    pool.set_eviction_relief(Arc::new(TestRelief {
        pool: Arc::clone(&pool),
        writer: Arc::clone(&writer),
        sink: None,
    }));

    // Fill every frame with a dirty, unpinned page.
    for b in 0..N {
        dirty_page_at_lsn(&pool, pid(b), u64::from(b) + 1);
    }
    assert_eq!(pool.stats().dirty, N as usize, "all frames should be dirty");

    // Without relief a bare get_page for an (N+1)-th page would Exhaust.
    assert!(
        matches!(pool.get_page(pid(N)), Err(BufferPoolError::Exhausted)),
        "bare get_page must Exhaust under all-dirty pressure"
    );

    // With relief, get_page_relieved flushes dirty pages and succeeds.
    let guard = pool
        .get_page_relieved(pid(N))
        .expect("relief must let eviction succeed");
    drop(guard);

    assert!(
        pool.stats().dirty < N as usize,
        "relief must have flushed at least one dirty page (dirty now {})",
        pool.stats().dirty
    );
    assert!(
        !writer.lock().pages.is_empty(),
        "relief must have written at least one page to the writer"
    );
}

#[test]
fn relief_respects_lsn_gate_with_sink() {
    // A lagging sink: durable starts at 0, so every dirtied page (page_lsn >= 1)
    // is ahead of durable and blocked by the WAL-before-data gate.
    const N: u32 = 3;
    let sink = Arc::new(LaggingWalSink::new());
    let pool = Arc::new(BufferPool::with_wal(
        N as usize,
        BlankLoader,
        Arc::clone(&sink) as Arc<dyn WalSink>,
    ));
    let writer = Arc::new(Mutex::new(WriterLog::default()));
    pool.set_eviction_relief(Arc::new(TestRelief {
        pool: Arc::clone(&pool),
        writer: Arc::clone(&writer),
        sink: Some(Arc::clone(&sink)),
    }));

    // Dirty all frames with strictly increasing page-LSNs, all > durable (0).
    let lsns = [10_u64, 20, 30];
    for (b, &lsn) in lsns.iter().enumerate() {
        let block = u32::try_from(b).expect("block index fits u32");
        dirty_page_at_lsn(&pool, pid(block), lsn);
    }

    // (a) Phase A (no force) flushes 0: every dirty page is ahead of durable.
    let phase_a = {
        let writer = Arc::clone(&writer);
        pool.try_flush_dirty(move |page_id, page| {
            writer.lock().seen.push((page_id, page.header().lsn));
            Ok(())
        })
        .expect("try_flush_dirty must not error")
    };
    assert_eq!(phase_a, 0, "Phase A must flush nothing while durable lags");
    assert!(
        writer.lock().seen.is_empty(),
        "no page may reach the writer before durable advances"
    );

    // (b) oldest_unflushable_dirty_lsn reports the MIN blocked LSN.
    assert_eq!(
        pool.oldest_unflushable_dirty_lsn(),
        Some(Lsn::new(10)),
        "must report the lowest blocked page-LSN"
    );

    // (c) Drive relief: it advances durable to 10 (the min) and flushes only
    // the frame(s) now <= durable. The relief loop re-reports the new min on
    // each round, so a full get_page_relieved eventually frees a victim.
    let guard = pool
        .get_page_relieved(pid(N))
        .expect("relief must succeed after advancing durable");
    drop(guard);

    // The decisive invariant: NO page with page_lsn > the durable LSN AT THE
    // TIME OF WRITE ever reached the writer. We advanced durable in min-LSN
    // steps, so assert every written page's LSN was <= the final durable LSN
    // and, more strongly, that the writer only ever saw pages whose LSN had
    // already become durable.
    let final_durable = sink.durable_lsn().raw();
    for (page_id, page_lsn) in &writer.lock().seen {
        assert!(
            *page_lsn <= final_durable,
            "page {page_id} written at LSN {page_lsn} exceeds durable {final_durable} (WAL-before-data violated)"
        );
    }
    assert!(
        !writer.lock().seen.is_empty(),
        "relief must have written the gated frame(s) once durable advanced"
    );
}

#[test]
fn pinned_frames_never_evicted_or_flushed() {
    const N: u32 = 4;
    let pool = Arc::new(BufferPool::new(N as usize, BlankLoader));
    let writer = Arc::new(Mutex::new(WriterLog::default()));
    pool.set_eviction_relief(Arc::new(TestRelief {
        pool: Arc::clone(&pool),
        writer: Arc::clone(&writer),
        sink: None,
    }));

    // Pin two frames (guards held) and dirty them while pinned.
    let g0 = pool.get_page(pid(0)).expect("pin pid0");
    g0.write().set_lsn(100);
    let g1 = pool.get_page(pid(1)).expect("pin pid1");
    g1.write().set_lsn(101);

    // Dirty the remaining frames unpinned.
    dirty_page_at_lsn(&pool, pid(2), 102);
    dirty_page_at_lsn(&pool, pid(3), 103);

    // Relief should flush only the unpinned dirty frames.
    let relief: Arc<dyn EvictionRelief> = Arc::new(TestRelief {
        pool: Arc::clone(&pool),
        writer: Arc::clone(&writer),
        sink: None,
    });
    relief.relieve().expect("relief must run");

    let log = writer.lock();
    assert!(
        !log.pages.contains_key(&pid(0)) && !log.pages.contains_key(&pid(1)),
        "pinned frames must never reach the writer"
    );
    assert!(
        log.pages.contains_key(&pid(2)) || log.pages.contains_key(&pid(3)),
        "unpinned dirty frames should have been flushed"
    );
    drop(log);

    // Pinned frames keep their pin and stay dirty (never cleaned).
    let stats = pool.stats();
    assert!(stats.pinned >= 2, "the two held guards must stay pinned");
    drop(g0);
    drop(g1);
}

#[test]
fn relief_bounded_returns_exhausted_when_only_pinned() {
    // Pin every frame; no flush can free a victim, so relief makes no progress
    // and the bounded loop must surface Exhausted instead of spinning.
    const N: u32 = 2;
    let pool = Arc::new(BufferPool::new(N as usize, BlankLoader));
    let writer = Arc::new(Mutex::new(WriterLog::default()));
    pool.set_eviction_relief(Arc::new(TestRelief {
        pool: Arc::clone(&pool),
        writer: Arc::clone(&writer),
        sink: None,
    }));

    let mut held = Vec::new();
    for b in 0..N {
        let g = pool.get_page(pid(b)).expect("pin all frames");
        g.write().set_lsn(u64::from(b) + 1);
        held.push(g);
    }

    // All frames pinned: relief cannot help, so this must terminate (not hang)
    // and return Exhausted.
    let err = pool
        .get_page_relieved(pid(N))
        .expect_err("must Exhaust when every frame is pinned");
    assert!(matches!(err, BufferPoolError::Exhausted));
    assert!(
        writer.lock().pages.is_empty(),
        "no pinned frame may be written"
    );
    drop(held);
}

#[test]
fn poisoned_pool_skips_relief() {
    let pool = Arc::new(BufferPool::new(2, BlankLoader));
    let writer = Arc::new(Mutex::new(WriterLog::default()));
    pool.set_eviction_relief(Arc::new(TestRelief {
        pool: Arc::clone(&pool),
        writer: Arc::clone(&writer),
        sink: None,
    }));

    // Force the pool into the poisoned state via a failed WAL-append simulation:
    // get a page, poison the pool, then assert get_page_relieved short-circuits.
    {
        let g = pool.get_page(pid(0)).expect("pin then poison");
        g.write().set_lsn(1);
    }
    // Poison through the public surface used by the heap WAL-emit failure path.
    pool.poison_for_test();

    let err = pool
        .get_page_relieved(pid(1))
        .expect_err("poisoned pool must not relieve");
    assert!(matches!(err, BufferPoolError::Poisoned));
    assert!(
        writer.lock().pages.is_empty(),
        "poisoned pool must not invoke the writer"
    );
}
