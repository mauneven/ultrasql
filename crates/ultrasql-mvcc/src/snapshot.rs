//! Transaction snapshot.
//!
//! A snapshot freezes the visible-transactions universe at a single
//! point in time. It is constructed at statement start (READ COMMITTED)
//! or at transaction start (REPEATABLE READ / SERIALIZABLE) by the
//! transaction manager, then handed to every executor operator that
//! reads tuples.
//!
//! A snapshot defines:
//!
//! - `xmin` — the lowest XID still considered in-progress.
//! - `xmax` — one past the highest XID assigned at snapshot time.
//! - `xip` — the set of XIDs in `[xmin, xmax)` that were in progress
//!   when the snapshot was taken.
//! - `current_xid` and `current_command` — identifies the requester so
//!   the visibility predicate can show the transaction its own writes.

use smallvec::SmallVec;
use ultrasql_core::{CommandId, Xid};

/// Lightweight container for the set of in-progress XIDs. For typical
/// workloads this set is tiny (a handful of concurrent writers), so we
/// inline up to 8 entries to avoid heap allocation in the common case.
type ActiveXids = SmallVec<[Xid; 8]>;

/// MVCC snapshot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Snapshot {
    /// Lowest XID still considered in progress. All XIDs strictly less
    /// than `xmin` are either committed or aborted.
    pub xmin: Xid,
    /// One past the largest XID known at snapshot construction. Any
    /// XID `>= xmax` is implicitly in-the-future and therefore
    /// invisible.
    pub xmax: Xid,
    /// The transaction's own XID. Tuples written by `current_xid` are
    /// visible at commands strictly less than `current_command`.
    pub current_xid: Xid,
    /// Current command id within `current_xid`. The first statement
    /// in a transaction starts at [`CommandId::FIRST`].
    pub current_command: CommandId,
    /// Snapshot-time in-progress XIDs in `[xmin, xmax)`. Sorted
    /// ascending; we exploit the ordering for `binary_search`.
    pub xip: ActiveXids,
}

impl Snapshot {
    /// Build a snapshot. `xip` need not be sorted; the constructor
    /// sorts it.
    #[must_use]
    pub fn new<I: IntoIterator<Item = Xid>>(
        xmin: Xid,
        xmax: Xid,
        current_xid: Xid,
        current_command: CommandId,
        xip: I,
    ) -> Self {
        let mut xip: ActiveXids = xip.into_iter().collect();
        xip.sort_unstable();
        Self {
            xmin,
            xmax,
            current_xid,
            current_command,
            xip,
        }
    }

    /// Whether `xid` was in progress when this snapshot was taken.
    ///
    /// Three regions:
    ///
    /// - `xid < xmin`: fully resolved (not in progress).
    /// - `xid >= xmax`: newer than the snapshot — implicitly in
    ///   progress for visibility purposes.
    /// - `xmin <= xid < xmax`: check the explicit in-progress list.
    #[must_use]
    pub fn xid_in_progress(&self, xid: Xid) -> bool {
        if xid < self.xmin {
            return false;
        }
        if xid >= self.xmax {
            return true;
        }
        self.xip.binary_search(&xid).is_ok()
    }

    /// `true` iff `xid` is the transaction requesting this snapshot.
    #[must_use]
    pub fn is_current_xid(&self, xid: Xid) -> bool {
        xid == self.current_xid
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snap(xmin: u64, xmax: u64, cur: u64, cmd: u32, in_progress: &[u64]) -> Snapshot {
        Snapshot::new(
            Xid::new(xmin),
            Xid::new(xmax),
            Xid::new(cur),
            CommandId::new(cmd),
            in_progress.iter().map(|&x| Xid::new(x)),
        )
    }

    #[test]
    fn xid_below_xmin_not_in_progress() {
        let s = snap(10, 20, 15, 0, &[12, 14, 16]);
        assert!(!s.xid_in_progress(Xid::new(5)));
        assert!(!s.xid_in_progress(Xid::new(9)));
    }

    #[test]
    fn xid_at_or_above_xmax_in_progress() {
        let s = snap(10, 20, 15, 0, &[12, 14, 16]);
        assert!(s.xid_in_progress(Xid::new(20)));
        assert!(s.xid_in_progress(Xid::new(50)));
    }

    #[test]
    fn xid_in_active_list_is_in_progress() {
        let s = snap(10, 20, 15, 0, &[12, 14, 16]);
        assert!(s.xid_in_progress(Xid::new(12)));
        assert!(s.xid_in_progress(Xid::new(14)));
        assert!(s.xid_in_progress(Xid::new(16)));
    }

    #[test]
    fn xid_between_xmin_and_xmax_but_not_active_is_resolved() {
        let s = snap(10, 20, 15, 0, &[12, 14, 16]);
        assert!(!s.xid_in_progress(Xid::new(11)));
        assert!(!s.xid_in_progress(Xid::new(13)));
        assert!(!s.xid_in_progress(Xid::new(15)));
        assert!(!s.xid_in_progress(Xid::new(19)));
    }

    #[test]
    fn xip_is_sorted_after_construction() {
        let s = snap(10, 20, 15, 0, &[16, 12, 14]);
        let xips: Vec<u64> = s.xip.iter().map(|x| x.raw()).collect();
        assert_eq!(xips, vec![12, 14, 16]);
    }

    #[test]
    fn is_current_xid() {
        let s = snap(10, 20, 15, 0, &[]);
        assert!(s.is_current_xid(Xid::new(15)));
        assert!(!s.is_current_xid(Xid::new(14)));
    }
}
