//! Transaction status oracle.
//!
//! Visibility decisions depend on knowing, for a given XID, whether the
//! transaction has committed, aborted, or is still in progress. The
//! authoritative answer lives in CLOG (commit log) and PROC array
//! managed by `ultrasql-txn`; the MVCC crate consumes a small trait so
//! it can be tested in isolation.
//!
//! In production, `ultrasql-txn` provides an implementation backed by a
//! sharded CLOG cache. In tests, an in-memory `HashMap` stand-in is
//! perfectly sufficient.

use ultrasql_core::Xid;

/// Authoritative status of a transaction at the moment a snapshot was
/// taken (or earlier — committed/aborted are terminal states).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum XidStatus {
    /// The transaction is still running. `xmin`-running tuples are
    /// invisible to snapshots taken before its commit.
    InProgress,
    /// The transaction committed durably.
    Committed,
    /// The transaction aborted. Tuples it wrote are skipped.
    Aborted,
    /// The XID is older than the oldest known transaction; its state
    /// has been frozen and treated as committed (vacuum-frozen).
    Frozen,
}

/// Look up the status of a transaction.
///
/// The oracle must be cheap; visibility runs in the inner loop of
/// every executor scan. Production implementations cache CLOG pages in
/// the buffer pool and serve from RAM for the common case.
pub trait XidStatusOracle: Send + Sync {
    /// Look up `xid`'s status. The status of `Xid::INVALID` is
    /// undefined; callers should not query it.
    fn status(&self, xid: Xid) -> XidStatus;

    /// Convenience: `true` iff `xid` has committed.
    fn is_committed(&self, xid: Xid) -> bool {
        matches!(self.status(xid), XidStatus::Committed | XidStatus::Frozen)
    }

    /// Convenience: `true` iff `xid` has aborted.
    fn is_aborted(&self, xid: Xid) -> bool {
        matches!(self.status(xid), XidStatus::Aborted)
    }

    /// Convenience: `true` iff `xid` is still running.
    fn is_in_progress(&self, xid: Xid) -> bool {
        matches!(self.status(xid), XidStatus::InProgress)
    }
}

#[cfg(any(test, feature = "testing"))]
pub mod test_support {
    //! In-memory oracle for unit tests.

    use std::collections::HashMap;

    use parking_lot::RwLock;
    use ultrasql_core::Xid;

    use super::{XidStatus, XidStatusOracle};

    /// Trivial in-memory oracle. Defaults unset XIDs to
    /// [`XidStatus::InProgress`] so test authors can opt in
    /// transactions to committed/aborted explicitly.
    #[derive(Debug, Default)]
    pub struct MapOracle {
        states: RwLock<HashMap<Xid, XidStatus>>,
    }

    impl MapOracle {
        /// Construct an oracle with no recorded XIDs.
        #[must_use]
        pub fn new() -> Self {
            Self::default()
        }

        /// Record `xid` as committed.
        pub fn set_committed(&self, xid: Xid) {
            self.states.write().insert(xid, XidStatus::Committed);
        }

        /// Record `xid` as aborted.
        pub fn set_aborted(&self, xid: Xid) {
            self.states.write().insert(xid, XidStatus::Aborted);
        }

        /// Record `xid` as in progress.
        pub fn set_in_progress(&self, xid: Xid) {
            self.states.write().insert(xid, XidStatus::InProgress);
        }
    }

    impl XidStatusOracle for MapOracle {
        fn status(&self, xid: Xid) -> XidStatus {
            if xid == Xid::FROZEN {
                return XidStatus::Frozen;
            }
            *self.states.read().get(&xid).unwrap_or(&XidStatus::InProgress)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::MapOracle;
    use super::*;

    #[test]
    fn frozen_xid_is_committed() {
        let oracle = MapOracle::new();
        assert!(oracle.is_committed(Xid::FROZEN));
    }

    #[test]
    fn unset_xid_is_in_progress() {
        let oracle = MapOracle::new();
        assert!(oracle.is_in_progress(Xid::new(100)));
    }

    #[test]
    fn record_commit_changes_status() {
        let oracle = MapOracle::new();
        let x = Xid::new(42);
        oracle.set_committed(x);
        assert!(oracle.is_committed(x));
        assert!(!oracle.is_aborted(x));
        oracle.set_aborted(x);
        assert!(oracle.is_aborted(x));
    }
}
