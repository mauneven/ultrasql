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

/// Lightweight container for the set of own subtransaction XIDs. A
/// transaction rarely has more than a handful of open / released / rolled
/// back savepoints, so we inline up to 8 entries.
type OwnSubxidVec = SmallVec<[Xid; 8]>;

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
    ///
    /// Private to protect the sorted invariant that
    /// [`Snapshot::xid_in_progress`]'s `binary_search` depends on.
    /// Read access is via [`Snapshot::xip`]; the only way to populate
    /// it is [`Snapshot::new`], which sorts.
    xip: ActiveXids,
    /// Own subtransaction XIDs that count as **self** for visibility:
    /// every live (un-released) savepoint XID *plus* every released-but-
    /// parent-still-open ("merged up") savepoint XID. Sorted ascending so
    /// [`Self::is_current_xid`] can `binary_search`.
    ///
    /// A row whose `xmin` is in this set is a write the transaction made
    /// under one of its own savepoints; the predicate treats it exactly
    /// like a row stamped with `current_xid`. Empty for any transaction
    /// with no open/merged savepoints, which collapses
    /// [`Self::is_current_xid`] to the single-`Xid` compare.
    own_live_subxids: OwnSubxidVec,
    /// Own subtransaction XIDs that were rolled back via `ROLLBACK TO`.
    /// A row whose `xmin` (insert) or `xmax` (delete / in-place update)
    /// is in this set is forced invisible / reverted **independently of
    /// the CLOG**, so visibility is correct even before (or without) any
    /// physical heap undo. Sorted ascending for `binary_search`.
    own_rolled_back_subxids: OwnSubxidVec,
    /// Lower bound for both subxid sets: the smallest XID present in
    /// either set, or [`Xid::INVALID`]-as-max when both are empty. Lets
    /// the predicate skip the `binary_search` entirely for any XID below
    /// the range (the overwhelmingly common case: foreign XIDs and the
    /// parent's own pre-savepoint writes).
    own_subxid_lo: Xid,
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
            own_live_subxids: OwnSubxidVec::new(),
            own_rolled_back_subxids: OwnSubxidVec::new(),
            // Both sets empty → lo = max sentinel, so every real XID is
            // strictly below it and short-circuits the subxid lookups.
            own_subxid_lo: Xid::new(u64::MAX),
        }
    }

    /// Replace **only** the own-subtransaction sets, recomputing the
    /// range bound. `xmin` / `xmax` / `xip` / `current_xid` /
    /// `current_command` are left untouched.
    ///
    /// This is how a frozen REPEATABLE READ / SERIALIZABLE snapshot stays
    /// coherent across `SAVEPOINT` / `RELEASE` / `ROLLBACK TO`: those
    /// operations change *which of the transaction's own writes count as
    /// self vs reverted*, but they must not perturb the concurrent-writer
    /// universe the snapshot froze at begin. Both inputs are sorted here
    /// so [`Self::is_current_xid`] / [`Self::own_subxid_rolled_back`] can
    /// `binary_search`.
    pub fn set_own_subxids<L, R>(&mut self, live: L, rolled_back: R)
    where
        L: IntoIterator<Item = Xid>,
        R: IntoIterator<Item = Xid>,
    {
        let mut live: OwnSubxidVec = live.into_iter().collect();
        live.sort_unstable();
        let mut rolled_back: OwnSubxidVec = rolled_back.into_iter().collect();
        rolled_back.sort_unstable();

        // Range bound is the smallest XID present in either set. The sets
        // are sorted so their first element is their minimum.
        let lo = match (live.first(), rolled_back.first()) {
            (Some(&a), Some(&b)) => a.min(b),
            (Some(&a), None) => a,
            (None, Some(&b)) => b,
            (None, None) => Xid::new(u64::MAX),
        };

        self.own_live_subxids = live;
        self.own_rolled_back_subxids = rolled_back;
        self.own_subxid_lo = lo;
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

    /// `true` iff `xid` is the transaction requesting this snapshot — its
    /// top-level XID **or** one of its own live / merged-up subtransaction
    /// XIDs.
    ///
    /// The subxid branch short-circuits to the single-`Xid` compare
    /// whenever the live set is empty (no open or merged savepoints) or
    /// `xid` is below the subxid range. A row stamped with a live/merged
    /// subxid is one of *our own* writes and is treated identically to a
    /// `current_xid` write by the visibility predicate.
    #[must_use]
    pub fn is_current_xid(&self, xid: Xid) -> bool {
        xid == self.current_xid
            || (!self.own_live_subxids.is_empty()
                && xid >= self.own_subxid_lo
                && self.own_live_subxids.binary_search(&xid).is_ok())
    }

    /// `true` iff `xid` is one of this transaction's own subtransactions
    /// that was rolled back via `ROLLBACK TO`.
    ///
    /// A row whose `xmin` is in this set was inserted under a rolled-back
    /// savepoint and must be invisible even when the CLOG would report it
    /// committed; a row whose `xmax` is in this set had its delete /
    /// in-place update reverted, so the row "did not change". The
    /// predicate consults this so visibility is correct **independently of
    /// whether physical heap undo has run** — closing the unsound masking
    /// the reverted first attempt depended on.
    ///
    /// Short-circuits when the rolled-back set is empty or `xid` is below
    /// the subxid range.
    #[must_use]
    pub fn own_subxid_rolled_back(&self, xid: Xid) -> bool {
        !self.own_rolled_back_subxids.is_empty()
            && xid >= self.own_subxid_lo
            && self.own_rolled_back_subxids.binary_search(&xid).is_ok()
    }

    /// The snapshot-time in-progress XIDs in `[xmin, xmax)`, sorted
    /// ascending.
    ///
    /// Returned as a shared slice so callers cannot perturb the sorted
    /// invariant that [`Self::xid_in_progress`] relies on.
    #[must_use]
    pub fn xip(&self) -> &[Xid] {
        &self.xip
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
        let xips: Vec<u64> = s.xip().iter().map(|x| x.raw()).collect();
        assert_eq!(xips, vec![12, 14, 16]);
    }

    #[test]
    fn is_current_xid() {
        let s = snap(10, 20, 15, 0, &[]);
        assert!(s.is_current_xid(Xid::new(15)));
        assert!(!s.is_current_xid(Xid::new(14)));
    }

    // ── R1: own-subtransaction sets ───────────────────────────────────────

    #[test]
    fn is_current_xid_includes_own_live_subxids() {
        let mut s = snap(10, 30, 15, 0, &[]);
        s.set_own_subxids([Xid::new(20), Xid::new(25)], std::iter::empty());
        // Parent and both live subxids are "current".
        assert!(s.is_current_xid(Xid::new(15)));
        assert!(s.is_current_xid(Xid::new(20)));
        assert!(s.is_current_xid(Xid::new(25)));
        // An unrelated XID is not.
        assert!(!s.is_current_xid(Xid::new(22)));
        assert!(!s.is_current_xid(Xid::new(14)));
    }

    #[test]
    fn rolled_back_subxid_is_not_current_but_is_flagged() {
        let mut s = snap(10, 30, 15, 0, &[]);
        s.set_own_subxids([Xid::new(20)], [Xid::new(25)]);
        // 25 was rolled back: it is NOT current, but IS flagged.
        assert!(!s.is_current_xid(Xid::new(25)));
        assert!(s.own_subxid_rolled_back(Xid::new(25)));
        // 20 is live: current, not rolled back.
        assert!(s.is_current_xid(Xid::new(20)));
        assert!(!s.own_subxid_rolled_back(Xid::new(20)));
        // The parent is current and never "rolled back".
        assert!(s.is_current_xid(Xid::new(15)));
        assert!(!s.own_subxid_rolled_back(Xid::new(15)));
    }

    #[test]
    fn empty_subxid_sets_short_circuit() {
        // Fresh snapshot: both sets empty, lo is the max sentinel.
        let s = snap(10, 30, 15, 0, &[]);
        assert!(s.is_current_xid(Xid::new(15)));
        assert!(!s.is_current_xid(Xid::new(20)));
        // Nothing is ever flagged rolled-back with empty sets.
        assert!(!s.own_subxid_rolled_back(Xid::new(20)));
        assert!(!s.own_subxid_rolled_back(Xid::new(15)));
    }

    #[test]
    fn set_own_subxids_patches_only_subxid_sets() {
        let mut s = snap(10, 30, 15, 7, &[12, 14]);
        let xmin_before = s.xmin;
        let xmax_before = s.xmax;
        let xip_before: Vec<Xid> = s.xip().to_vec();
        let cur_before = s.current_xid;
        let cmd_before = s.current_command;

        s.set_own_subxids([Xid::new(25), Xid::new(20)], [Xid::new(28)]);

        // Snapshot stability: nothing but the subxid sets changed.
        assert_eq!(s.xmin, xmin_before);
        assert_eq!(s.xmax, xmax_before);
        assert_eq!(s.xip().to_vec(), xip_before);
        assert_eq!(s.current_xid, cur_before);
        assert_eq!(s.current_command, cmd_before);

        // And the sets took effect (including sorting the inputs).
        assert!(s.is_current_xid(Xid::new(20)));
        assert!(s.is_current_xid(Xid::new(25)));
        assert!(s.own_subxid_rolled_back(Xid::new(28)));

        // Re-patching with empty sets restores the short-circuit.
        s.set_own_subxids(std::iter::empty(), std::iter::empty());
        assert!(!s.is_current_xid(Xid::new(20)));
        assert!(!s.own_subxid_rolled_back(Xid::new(28)));
    }
}
