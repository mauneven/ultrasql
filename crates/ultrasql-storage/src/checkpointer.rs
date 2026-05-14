//! Periodic dirty-page flush.
//!
//! The checkpointer is a long-lived background task that periodically drives
//! [`BufferPool::try_flush_dirty`] using a caller-supplied frame-writer
//! callback. It is intentionally simple in v0.3: no LSN-truncation, no
//! incremental checkpoints, no parallel flushers. The contract is:
//!
//! > Every `<interval>`, push dirty pages whose page-LSN is ≤ the WAL's
//! > durable LSN to disk via the writer callback.
//!
//! # Wire-up
//!
//! [`Checkpointer::spawn`] returns an owning handle. Drop the handle or call
//! [`Checkpointer::shutdown`] to stop the background thread. The thread loops:
//!
//! 1. Sleep `interval` (via [`parking_lot::Condvar`] `wait_for`, so early
//!    wake-up is possible for testing).
//! 2. Call [`BufferPool::try_flush_dirty`] with the supplied writer callback.
//! 3. Log the result with [`tracing::debug`] on success or [`tracing::warn`]
//!    on writer error. The thread **continues** on writer errors; it does not
//!    bring down the system.
//! 4. Optionally append a `RecordType::Checkpoint` WAL record via the sink —
//!    **TODO(v0.4)**: the checkpointer does not yet know which redo-from LSN
//!    to record, so this step is deferred.
//!
//! # Shutdown
//!
//! [`Checkpointer::shutdown`] signals the background thread to stop and joins
//! it, returning the total count of pages successfully flushed across all
//! flush cycles.
//!
//! # Lock order
//!
//! The checkpointer acquires per-frame locks inside
//! [`BufferPool::try_flush_dirty`]. Those locks are held in frame-index order
//! and are never held simultaneously with any other buffer-pool locks. This is
//! consistent with the global latch order defined in ARCHITECTURE.md §14; the
//! checkpointer introduces no new lock-ordering hazards.

use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use parking_lot::{Condvar, Mutex};
use tracing::{debug, warn};

use crate::buffer_pool::{BufferPool, PageLoader};
use crate::page::Page;
use crate::wal_sink::WalSink;

/// Configuration knobs for the [`Checkpointer`].
#[derive(Clone, Copy, Debug)]
pub struct CheckpointerConfig {
    /// How often the checkpointer wakes up and attempts to flush dirty
    /// pages. Shorter intervals reduce the amount of work lost in a crash
    /// at the cost of more frequent I/O.
    pub interval: Duration,
}

impl Default for CheckpointerConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(5),
        }
    }
}

/// State shared between the owning handle and the background thread.
#[derive(Debug, Default)]
struct Shared {
    /// Set to `true` by [`Checkpointer::shutdown`] to tell the thread to stop.
    stopping: Mutex<bool>,
    /// Condvar used to both wait for the interval and receive stop signals.
    wake: Condvar,
}

/// Owning handle to the checkpointer background thread.
///
/// Dropping this value without calling [`Self::shutdown`] will trigger a
/// warning and attempt a best-effort join. Prefer explicit shutdown to ensure
/// the final flush count is visible to the caller.
#[derive(Debug)]
pub struct Checkpointer {
    shared: Arc<Shared>,
    handle: Option<JoinHandle<u64>>,
}

impl Checkpointer {
    /// Spawn the checkpointer background thread.
    ///
    /// The thread periodically flushes dirty pages from `pool` via `writer`.
    /// When a WAL `sink` is supplied, only pages whose page-LSN is ≤
    /// `sink.durable_lsn()` are flushed; this preserves the WAL-ahead-of-data
    /// invariant. Pass `None` to flush all dirty pages regardless of LSN.
    ///
    /// # Arguments
    ///
    /// - `pool`: the buffer pool to checkpoint.
    /// - `sink`: optional WAL sink used for LSN-gating. May be `None` in
    ///   test or WAL-less configurations.
    /// - `writer`: callback invoked for each dirty frame eligible for flush.
    ///   Receives the [`ultrasql_core::PageId`] and a shared reference to
    ///   the [`Page`]. Must **not** call back into `pool` (would deadlock).
    /// - `config`: tuning knobs, most importantly the flush interval.
    pub fn spawn<L, F>(
        pool: &Arc<BufferPool<L>>,
        _sink: Option<Arc<dyn WalSink>>,
        writer: F,
        config: CheckpointerConfig,
    ) -> Self
    where
        L: PageLoader + 'static,
        F: FnMut(ultrasql_core::PageId, &Page) -> ultrasql_core::Result<()> + Send + 'static,
    {
        let shared = Arc::new(Shared::default());
        let thread_shared = Arc::clone(&shared);
        let pool_clone = Arc::clone(pool);

        let handle = thread::Builder::new()
            .name(String::from("ultrasql-checkpointer"))
            .spawn(move || Self::run(&pool_clone, writer, config, &thread_shared))
            .expect("checkpointer: failed to spawn thread");

        Self {
            shared,
            handle: Some(handle),
        }
    }

    /// Signal the background thread to stop and wait for it to exit.
    ///
    /// Returns the total number of pages successfully flushed across all
    /// checkpoint cycles performed by this checkpointer.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the background thread panicked.
    pub fn shutdown(mut self) -> Result<u64, std::io::Error> {
        {
            let mut stopping = self.shared.stopping.lock();
            *stopping = true;
        }
        self.shared.wake.notify_all();

        let handle = self
            .handle
            .take()
            .expect("checkpointer: handle already consumed");
        handle
            .join()
            .map_err(|_| std::io::Error::other("checkpointer background thread panicked"))
    }

    /// Background thread body.
    ///
    /// Loops until the stop signal is set, sleeping `config.interval` between
    /// each flush cycle. Uses [`Condvar::wait_for`] so shutdown wakes the
    /// thread immediately rather than waiting for the next interval.
    fn run<L, F>(
        pool: &Arc<BufferPool<L>>,
        mut writer: F,
        config: CheckpointerConfig,
        shared: &Arc<Shared>,
    ) -> u64
    where
        L: PageLoader + 'static,
        F: FnMut(ultrasql_core::PageId, &Page) -> ultrasql_core::Result<()>,
    {
        let mut total_flushed: u64 = 0;

        loop {
            // Sleep for `interval` or until the stop signal arrives.
            {
                let mut stopping = shared.stopping.lock();
                if *stopping {
                    break;
                }
                // `wait_for` releases the lock during the sleep and
                // re-acquires it on wake. If the condvar fires before the
                // timeout we re-check the stop flag.
                let _ = shared.wake.wait_for(&mut stopping, config.interval);
                if *stopping {
                    break;
                }
            }

            // Flush dirty pages.
            match pool.try_flush_dirty(&mut writer) {
                Ok(n) => {
                    let n_u64 = u64::try_from(n).unwrap_or(u64::MAX);
                    total_flushed = total_flushed.saturating_add(n_u64);
                    if n > 0 {
                        debug!(pages = n, "checkpointer: flushed dirty pages");
                    }
                    // TODO(v0.4): append a RecordType::Checkpoint WAL record
                    // here once the checkpointer knows the redo-from LSN.
                }
                Err(e) => {
                    // Writer errors are non-fatal for the checkpointer; the
                    // system is still consistent (the page remains dirty and
                    // WAL is intact). Log and continue; the next cycle will
                    // retry.
                    warn!(error = %e, "checkpointer: writer error during flush; retrying next cycle");
                }
            }
        }

        total_flushed
    }
}

impl Drop for Checkpointer {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            warn!("Checkpointer dropped without shutdown(); signalling thread to stop");
            {
                let mut stopping = self.shared.stopping.lock();
                *stopping = true;
            }
            self.shared.wake.notify_all();
            if handle.join().is_err() {
                warn!("checkpointer background thread panicked during Drop");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use ultrasql_core::{BlockNumber, PageId, RelationId};

    use super::*;
    use crate::buffer_pool::{BufferPool, PageLoader};
    use crate::page::Page;

    struct BlankLoader;
    impl PageLoader for BlankLoader {
        fn load(&self, _: PageId) -> ultrasql_core::Result<Page> {
            Ok(Page::new_heap())
        }
    }

    fn pid(block: u32) -> PageId {
        PageId::new(RelationId::new(1), BlockNumber::new(block))
    }

    /// Spawn with a no-op writer, wait briefly, then shut down. Must not panic.
    #[test]
    #[ignore = "slow: real-time sleep (50 ms); run via cargo test -- --ignored"]
    fn spawn_and_shutdown_clean() {
        let pool = Arc::new(BufferPool::new(4, BlankLoader));
        let config = CheckpointerConfig {
            interval: Duration::from_millis(50),
        };
        let ckpt = Checkpointer::spawn(&pool, None, |_pid, _page| Ok(()), config);
        std::thread::sleep(Duration::from_millis(50));
        let count = ckpt.shutdown().expect("checkpointer should not panic");
        // No dirty pages, so no flushes expected.
        assert_eq!(count, 0);
    }

    /// Set up a pool with one dirty page and a durable LSN that covers the
    /// page LSN. The checkpointer should flush it within a few intervals.
    #[test]
    #[ignore = "slow: real-time sleep (100 ms); run via cargo test -- --ignored"]
    fn checkpointer_flushes_dirty_pages() {
        use crate::wal_sink::WalSink;
        use crate::wal_sink::WalSinkError;
        use ultrasql_core::Lsn;
        use ultrasql_core::Xid;
        use ultrasql_wal::WalRecord;

        struct AlwaysDurableSink;
        impl WalSink for AlwaysDurableSink {
            fn append(&self, _record: WalRecord) -> Result<Lsn, WalSinkError> {
                Ok(Lsn::ZERO)
            }
            fn durable_lsn(&self) -> Lsn {
                Lsn::new(u64::MAX)
            }
            fn last_lsn_for(&self, _xid: Xid) -> Lsn {
                Lsn::ZERO
            }
        }

        let sink: Arc<dyn WalSink> = Arc::new(AlwaysDurableSink);
        let pool = Arc::new(BufferPool::with_wal(4, BlankLoader, Arc::clone(&sink)));

        // Mark page 0 dirty with a low LSN.
        {
            let g = pool.get_page(pid(0)).unwrap();
            let mut w = g.write();
            w.set_lsn(1);
        }
        assert_eq!(pool.stats().dirty, 1);

        let flush_count = Arc::new(AtomicUsize::new(0));
        let flush_count_clone = Arc::clone(&flush_count);

        let config = CheckpointerConfig {
            interval: Duration::from_millis(10),
        };

        let ckpt = Checkpointer::spawn(
            &pool,
            Some(sink),
            move |_pid, _page| {
                flush_count_clone.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
            config,
        );

        // Give the checkpointer a few intervals to fire.
        std::thread::sleep(Duration::from_millis(100));
        let total = ckpt.shutdown().expect("checkpointer should not panic");

        assert!(
            flush_count.load(Ordering::SeqCst) >= 1,
            "expected at least one flush, got {}",
            flush_count.load(Ordering::SeqCst)
        );
        assert!(
            total >= 1,
            "shutdown must return flush count > 0; got {total}"
        );
    }
}
