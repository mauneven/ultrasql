//! Row-level locking API for `SELECT FOR UPDATE / FOR SHARE`.
//!
//! Provides [`RowLockMode`] and [`RowLockRequest`] to describe the
//! row-locking intent from an executor, and wires those onto the
//! existing [`LockManager`] using the appropriate [`LockMode`] mapping.
//!
//! # Mode mapping
//!
//! | SQL clause              | [`RowLockMode`]     | [`LockMode`]          |
//! |-------------------------|---------------------|-----------------------|
//! | `FOR UPDATE`            | `ForUpdate`         | `Exclusive`           |
//! | `FOR NO KEY UPDATE`     | `ForNoKeyUpdate`    | `RowExclusive`        |
//! | `FOR SHARE`             | `ForShare`          | `Share`               |
//! | `FOR KEY SHARE`         | `ForKeyShare`       | `RowShare`            |
//!
//! Multiple `FOR SHARE` / `FOR KEY SHARE` holders on the same tuple are
//! allowed; a `FOR UPDATE` blocks any other `FOR UPDATE` or
//! `FOR NO KEY UPDATE` on the same tuple.
//!
//! # Concurrency
//!
//! All row locks go through [`LockManager::acquire`], which uses the
//! central lock table with blocking semantics and deadlock detection.
//! No fastpath is used for tuple-level locks — the fastpath is reserved
//! for `AccessShare` on relations.

use ultrasql_core::{TupleId, Xid};

use crate::lock::{LockError, LockManager, LockMode, LockRequest, LockTag};

/// Row-locking mode corresponding to a `SELECT … FOR` clause.
///
/// The four modes match PostgreSQL's row-locking strength order (weakest
/// to strongest): `ForKeyShare < ForShare < ForNoKeyUpdate < ForUpdate`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RowLockMode {
    /// `SELECT FOR UPDATE` — strongest row lock.
    ///
    /// Acquires [`LockMode::Exclusive`] on the tuple. Blocks all other
    /// row-lock modes except `AccessShare`.
    ForUpdate,
    /// `SELECT FOR NO KEY UPDATE` — blocks updates that change indexed key
    /// columns but allows `ForKeyShare`.
    ///
    /// Acquires [`LockMode::RowExclusive`] on the tuple.
    ForNoKeyUpdate,
    /// `SELECT FOR SHARE` — allows multiple concurrent readers.
    ///
    /// Acquires [`LockMode::Share`] on the tuple.
    ForShare,
    /// `SELECT FOR KEY SHARE` — weakest row lock, compatible with
    /// `ForShare` and `ForNoKeyUpdate`.
    ///
    /// Acquires [`LockMode::RowShare`] on the tuple.
    ForKeyShare,
}

impl RowLockMode {
    /// Map this row-lock mode to the underlying [`LockMode`] used by the
    /// central lock table.
    #[must_use]
    pub const fn to_lock_mode(self) -> LockMode {
        match self {
            Self::ForUpdate => LockMode::Exclusive,
            Self::ForNoKeyUpdate => LockMode::RowExclusive,
            Self::ForShare => LockMode::Share,
            Self::ForKeyShare => LockMode::RowShare,
        }
    }
}

/// A request to acquire a row-level lock on a specific tuple.
///
/// Built by the executor when it encounters a `SELECT FOR UPDATE / SHARE`
/// clause and handed to [`LockManager::acquire_row_lock`].
#[derive(Clone, Copy, Debug)]
pub struct RowLockRequest {
    /// The transaction requesting the lock.
    pub xid: Xid,
    /// The tuple to lock.
    pub tid: TupleId,
    /// The locking mode.
    pub mode: RowLockMode,
}

/// Extension trait adding row-locking helpers to [`LockManager`].
///
/// Implemented directly on [`LockManager`] so that callers can use
/// `lock_manager.acquire_row_lock(req)` without importing an extra trait.
pub trait RowLockExt {
    /// Acquire a row-level lock described by `req`.
    ///
    /// Blocks until the lock is granted or until the deadlock detector
    /// marks `req.xid` as a victim.
    ///
    /// # Errors
    ///
    /// - [`LockError::Deadlock`] — this XID was chosen as a deadlock victim.
    /// - [`LockError::Timeout`] — acquisition timed out (not raised by the
    ///   current blocking implementation; reserved for future use).
    fn acquire_row_lock(&self, req: RowLockRequest) -> Result<(), LockError>;

    /// Release the row-level lock on `tid` held by `xid` in `mode`.
    ///
    /// If the lock was not held (e.g. already released) this is a no-op.
    fn release_row_lock(&self, xid: Xid, tid: TupleId, mode: RowLockMode);
}

impl RowLockExt for LockManager {
    fn acquire_row_lock(&self, req: RowLockRequest) -> Result<(), LockError> {
        let lock_req = LockRequest {
            xid: req.xid,
            tag: LockTag::Tuple(req.tid),
            mode: req.mode.to_lock_mode(),
        };
        self.acquire(lock_req)
    }

    fn release_row_lock(&self, xid: Xid, tid: TupleId, mode: RowLockMode) {
        self.release(xid, LockTag::Tuple(tid), mode.to_lock_mode());
    }
}

// ─── tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use ultrasql_core::{BlockNumber, PageId, RelationId, TupleId, Xid};

    use super::*;
    use crate::lock::LockManager;

    fn xid(n: u64) -> Xid {
        Xid::new(n)
    }

    fn tid(rel: u32, block: u32, slot: u16) -> TupleId {
        TupleId::new(
            PageId::new(RelationId::new(rel), BlockNumber::new(block)),
            slot,
        )
    }

    // ── ForUpdate blocks a second ForUpdate ──────────────────────────────────

    #[test]
    fn for_update_blocks_second_for_update() {
        let mgr = Arc::new(LockManager::new());
        let t = tid(1, 0, 0);

        // T1 acquires ForUpdate.
        mgr.acquire_row_lock(RowLockRequest {
            xid: xid(1),
            tid: t,
            mode: RowLockMode::ForUpdate,
        })
        .unwrap();

        // T2 attempting ForUpdate should block. Use a short try_acquire-style
        // test via the central table inspection.
        let tag = LockTag::Tuple(t);
        let snap = mgr.inspect(tag).expect("entry must exist");
        assert!(
            snap.grants.iter().any(|(x, _)| *x == xid(1)),
            "T1 should hold the ForUpdate grant"
        );

        // A non-blocking check: try_acquire should return false (conflict).
        let got = mgr
            .try_acquire(crate::lock::LockRequest {
                xid: xid(2),
                tag,
                mode: RowLockMode::ForUpdate.to_lock_mode(),
            })
            .unwrap();
        assert!(!got, "ForUpdate must conflict with another ForUpdate");
    }

    // ── ForShare allows multiple concurrent holders ───────────────────────────

    #[test]
    fn for_share_allows_multiple_readers() {
        let mgr = LockManager::with_deadlock_interval(Duration::from_millis(50));
        let t = tid(2, 0, 0);

        mgr.acquire_row_lock(RowLockRequest {
            xid: xid(10),
            tid: t,
            mode: RowLockMode::ForShare,
        })
        .unwrap();

        // A second ForShare on the same tuple must be granted immediately.
        mgr.acquire_row_lock(RowLockRequest {
            xid: xid(11),
            tid: t,
            mode: RowLockMode::ForShare,
        })
        .unwrap();

        let tag = LockTag::Tuple(t);
        let snap = mgr.inspect(tag).expect("entry must exist");
        assert_eq!(
            snap.grants.len(),
            2,
            "both ForShare holders must be in the grant set"
        );
        assert!(snap.waiters.is_empty(), "no waiters expected");
    }

    // ── ForUpdate blocks ForShare ─────────────────────────────────────────────

    #[test]
    fn for_update_blocks_for_share() {
        let mgr = LockManager::new();
        let t = tid(3, 0, 0);

        mgr.acquire_row_lock(RowLockRequest {
            xid: xid(1),
            tid: t,
            mode: RowLockMode::ForUpdate,
        })
        .unwrap();

        let tag = LockTag::Tuple(t);
        // A ForShare on the same tuple must conflict (Exclusive vs Share).
        let got = mgr
            .try_acquire(crate::lock::LockRequest {
                xid: xid(2),
                tag,
                mode: RowLockMode::ForShare.to_lock_mode(),
            })
            .unwrap();
        assert!(!got, "ForUpdate must block ForShare");
    }

    // ── release_row_lock removes the grant ───────────────────────────────────

    #[test]
    fn release_row_lock_removes_grant() {
        let mgr = LockManager::new();
        let t = tid(4, 0, 0);

        mgr.acquire_row_lock(RowLockRequest {
            xid: xid(5),
            tid: t,
            mode: RowLockMode::ForUpdate,
        })
        .unwrap();

        mgr.release_row_lock(xid(5), t, RowLockMode::ForUpdate);

        // After release the entry is pruned from the central table.
        assert!(
            mgr.inspect(LockTag::Tuple(t)).is_none(),
            "lock entry should be pruned after release"
        );
    }

    // ── mode mapping ─────────────────────────────────────────────────────────

    #[test]
    fn mode_mapping_is_correct() {
        assert_eq!(RowLockMode::ForUpdate.to_lock_mode(), LockMode::Exclusive);
        assert_eq!(
            RowLockMode::ForNoKeyUpdate.to_lock_mode(),
            LockMode::RowExclusive
        );
        assert_eq!(RowLockMode::ForShare.to_lock_mode(), LockMode::Share);
        assert_eq!(RowLockMode::ForKeyShare.to_lock_mode(), LockMode::RowShare);
    }
}
