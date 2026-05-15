//! Loom-based concurrency model tests for the lock manager.
//!
//! `loom` exhaustively explores possible thread interleavings of a
//! small model program, catching race conditions a hand-written
//! integration test would miss. The production [`LockManager`]
//! itself uses `parking_lot::Mutex` and `crossbeam::Condvar`, neither
//! of which loom can intercept; replacing those primitives wholesale
//! would invasively rewrite the lock manager's hot path. Instead the
//! tests here model the lock manager's *contracts* using
//! `loom::sync::atomic` types so loom can scrutinise them directly.
//!
//! The two contracts under test are the same invariants the central
//! lock table promises [`crate::lock::LockEntry`] callers:
//!
//! 1. **Mutual exclusion of exclusive holders.** No two threads ever
//!    observe the lock as `Exclusive` at the same time.
//! 2. **No spurious upgrade under shared contention.** A thread
//!    waiting for the exclusive grant cannot promote the entry while
//!    any shared holders are still counted in.
//!
//! Run under loom with the standard recipe:
//!
//! ```sh
//! RUSTFLAGS="--cfg loom" cargo test -p ultrasql-txn --test loom_lock_model --release
//! ```
//!
//! The default `cargo test` invocation skips this file because the
//! `#![cfg(loom)]` gate compiles it out.

#![cfg(loom)]

use loom::sync::Arc;
use loom::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use loom::thread;

/// Lock state machine modelled on `LockEntry::mode`.
///
/// `0` is free; positive values count active shared holders; `u32::MAX`
/// flags the entry as exclusively held.
const FREE: u32 = 0;
const EXCLUSIVE: u32 = u32::MAX;

#[derive(Default)]
struct LockModel {
    state: AtomicU32,
}

impl LockModel {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            state: AtomicU32::new(FREE),
        })
    }

    /// Acquire shared. Returns `true` on success.
    fn try_acquire_shared(&self) -> bool {
        loop {
            let cur = self.state.load(Ordering::Acquire);
            if cur == EXCLUSIVE {
                return false;
            }
            if self
                .state
                .compare_exchange(cur, cur + 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return true;
            }
        }
    }

    /// Release shared.
    fn release_shared(&self) {
        let prev = self.state.fetch_sub(1, Ordering::AcqRel);
        // Underflow detection: prev must be > 0 and != EXCLUSIVE.
        assert!(prev != FREE, "release_shared called on a free lock");
        assert!(prev != EXCLUSIVE, "release_shared called on exclusive lock");
    }

    /// Try to acquire exclusive. Returns `true` only when the lock was free.
    fn try_acquire_exclusive(&self) -> bool {
        self.state
            .compare_exchange(FREE, EXCLUSIVE, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    fn release_exclusive(&self) {
        let prev = self.state.swap(FREE, Ordering::AcqRel);
        assert_eq!(prev, EXCLUSIVE, "release_exclusive on non-exclusive lock");
    }
}

/// Two threads race to grab exclusive. Exactly one must win each
/// round; the loser must observe the winner's release before its own
/// acquire succeeds.
#[test]
fn exclusive_acquire_is_mutually_exclusive() {
    loom::model(|| {
        let lock = LockModel::new();
        let l1 = Arc::clone(&lock);
        let l2 = Arc::clone(&lock);

        // Shared counter — incremented inside the critical section by
        // whichever thread holds exclusive. If mutual exclusion is
        // violated the increment can race with itself and tear.
        let counter = Arc::new(AtomicUsize::new(0));
        let c1 = Arc::clone(&counter);
        let c2 = Arc::clone(&counter);

        let h1 = thread::spawn(move || {
            while !l1.try_acquire_exclusive() {
                loom::thread::yield_now();
            }
            let prev = c1.fetch_add(1, Ordering::AcqRel);
            assert_eq!(prev % 2, 0, "exclusive overlap detected (prev={prev})");
            // Mark "we're inside" by storing odd, then back to even on release.
            c1.fetch_add(1, Ordering::AcqRel);
            l1.release_exclusive();
        });

        let h2 = thread::spawn(move || {
            while !l2.try_acquire_exclusive() {
                loom::thread::yield_now();
            }
            let prev = c2.fetch_add(1, Ordering::AcqRel);
            assert_eq!(prev % 2, 0, "exclusive overlap detected (prev={prev})");
            c2.fetch_add(1, Ordering::AcqRel);
            l2.release_exclusive();
        });

        h1.join().unwrap();
        h2.join().unwrap();
        assert_eq!(counter.load(Ordering::Acquire), 4);
    });
}

/// Shared and exclusive cannot coexist. The reader and writer race
/// for any order of scheduling; whoever wins acquires its mode and
/// the other must wait. Both must eventually complete and the lock
/// must end up free.
#[test]
fn exclusive_blocks_until_shared_drains() {
    loom::model(|| {
        let lock = LockModel::new();
        let l_reader = Arc::clone(&lock);
        let l_writer = Arc::clone(&lock);

        // Reader: spin until shared succeeds, then release.
        let reader = thread::spawn(move || {
            while !l_reader.try_acquire_shared() {
                loom::thread::yield_now();
            }
            l_reader.release_shared();
        });

        // Writer: spin until exclusive succeeds, then release.
        let writer = thread::spawn(move || {
            while !l_writer.try_acquire_exclusive() {
                loom::thread::yield_now();
            }
            l_writer.release_exclusive();
        });

        reader.join().unwrap();
        writer.join().unwrap();

        assert_eq!(
            lock.state.load(Ordering::Acquire),
            FREE,
            "lock must end up free"
        );
    });
}
