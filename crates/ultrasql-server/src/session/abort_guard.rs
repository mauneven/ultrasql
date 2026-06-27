//! Unwind guard that aborts an autocommit transaction's XID on a panic.

use std::sync::Arc;

use ultrasql_core::Xid;
use ultrasql_txn::TransactionManager;

/// RAII guard that aborts an *autocommit* transaction's XID if it is dropped
/// while still armed — i.e. on an unwind out of the statement-execution path.
///
/// `Transaction` has no `Drop` impl (it is `Clone` and holds no manager
/// handle), so a panic that unwinds past the normal `commit`/`abort` finalisers
/// drops the autocommit handle **without** releasing the per-tuple Exclusive
/// locks it acquired — they leak permanently (the XID is stuck `InProgress`,
/// there is no orphan-lock reaper), and a later writer to the same row blocks
/// until `statement_timeout`. This guard closes that hole: on any non-normal
/// exit it calls [`TransactionManager::abort_in_progress_by_xid`], which
/// releases the locks and marks the CLOG entry `Aborted`.
///
/// The normal commit/abort paths call [`Self::disarm`] once the handle has been
/// finalised (or handed off to the streaming drive loop), so the guard's `Drop`
/// is a no-op on the happy/error paths and only fires on an unwind. The abort is
/// additionally idempotent (a no-op once the XID is terminated), so even a
/// missed `disarm` cannot double-abort.
///
/// Explicit `BEGIN` blocks do NOT use this guard: their handle lives in the
/// session state and is aborted by the client's `ROLLBACK`/`COMMIT`.
pub(crate) struct AutocommitAbortGuard {
    manager: Arc<TransactionManager>,
    xid: Xid,
    armed: bool,
}

impl AutocommitAbortGuard {
    /// Arm a guard for `xid` against `manager`.
    pub(crate) fn arm(manager: Arc<TransactionManager>, xid: Xid) -> Self {
        Self {
            manager,
            xid,
            armed: true,
        }
    }

    /// Disarm the guard: the transaction reached a normal commit/abort (or was
    /// handed off to another guard), so this guard must not also abort. Call on
    /// every non-unwind exit.
    pub(crate) fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for AutocommitAbortGuard {
    fn drop(&mut self) {
        if self.armed {
            // Only fires on an unwind (or a forgotten `disarm`). Idempotent:
            // a no-op if the XID was already committed/aborted.
            self.manager.abort_in_progress_by_xid(self.xid);
        }
    }
}
