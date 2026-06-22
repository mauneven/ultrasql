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
//! - `own_live_subxids` / `own_rolled_back_subxids` — this backend's own
//!   subtransaction (savepoint) XIDs, partitioned into "still live or
//!   merged-up" (treated as *self* by the visibility predicate) and
//!   "rolled back" (forced invisible). Both are empty for the
//!   overwhelming majority of snapshots; an empty set short-circuits to
//!   zero added per-tuple cost.

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
    ///
    /// Private to protect the sorted invariant that
    /// [`Snapshot::xid_in_progress`]'s `binary_search` depends on.
    /// Read access is via [`Snapshot::xip`]; the only way to populate
    /// it is [`Snapshot::new`], which sorts.
    xip: ActiveXids,
    /// This backend's own *live* (not-yet-released-or-merged, not
    /// rolled-back) subtransaction XIDs, plus released-but-parent-still-
    /// open subxids, sorted ascending. Treated as *self* by
    /// [`Snapshot::is_current_xid`], so the owning transaction sees its
    /// own writes made under an active or released-but-uncommitted
    /// savepoint.
    ///
    /// Empty for the overwhelming majority of snapshots (no savepoint
    /// open in this backend). An empty set lets `is_current_xid`
    /// short-circuit to the exact single-`Xid` compare it performed
    /// before subtransaction support, so the no-savepoint hot path pays
    /// zero added cost.
    ///
    /// Private to protect the sorted invariant `binary_search` relies
    /// on. Populated only by [`Snapshot::new_with_subxids`] (which
    /// sorts) or patched in place by [`Snapshot::set_own_subxids`].
    own_live_subxids: ActiveXids,
    /// This backend's own *rolled-back* subtransaction XIDs, sorted
    /// ascending. A tuple whose `xmin` (insert) or `xmax` (delete /
    /// in-place update) is in this set is forced invisible / reverted
    /// regardless of CLOG state — the "my rolled-back savepoint is
    /// always invisible to me" invariant, independent of isolation
    /// level. Empty in the common case; an empty set short-circuits
    /// [`Snapshot::own_subxid_rolled_back`].
    own_rolled_back_subxids: ActiveXids,
    /// Smallest XID that could be one of this backend's own subxids
    /// (live, merged-up, or rolled-back). A per-tuple `xid` strictly
    /// below this bound cannot be an own subxid, so the subxid
    /// `binary_search`es are skipped entirely. Equal to `xmax` (i.e.
    /// "no own subxid range") when both subxid sets are empty.
    own_subxid_lo: Xid,
}

impl Snapshot {
    /// Build a snapshot with no own subtransactions. `xip` need not be
    /// sorted; the constructor sorts it.
    ///
    /// Equivalent to [`Self::new_with_subxids`] with empty subxid sets.
    /// This is the constructor for the common, no-savepoint case (and
    /// every pre-subtransaction caller / test).
    #[must_use]
    pub fn new<I: IntoIterator<Item = Xid>>(
        xmin: Xid,
        xmax: Xid,
        current_xid: Xid,
        current_command: CommandId,
        xip: I,
    ) -> Self {
        Self::new_with_subxids(
            xmin,
            xmax,
            current_xid,
            current_command,
            xip,
            std::iter::empty(),
            std::iter::empty(),
        )
    }

    /// Build a snapshot carrying this backend's own subtransaction
    /// (savepoint) context. `xip`, `own_live_subxids`, and
    /// `own_rolled_back_subxids` need not be sorted; the constructor
    /// sorts all three.
    ///
    /// - `own_live_subxids` — own subxids treated as *self* (live or
    ///   released-but-parent-still-open). The owning transaction sees
    ///   their writes.
    /// - `own_rolled_back_subxids` — own subxids forced invisible
    ///   regardless of CLOG state.
    ///
    /// In the common case both subxid iterators are empty; the snapshot
    /// then behaves bit-identically to [`Self::new`].
    #[must_use]
    pub fn new_with_subxids<I, L, R>(
        xmin: Xid,
        xmax: Xid,
        current_xid: Xid,
        current_command: CommandId,
        xip: I,
        own_live_subxids: L,
        own_rolled_back_subxids: R,
    ) -> Self
    where
        I: IntoIterator<Item = Xid>,
        L: IntoIterator<Item = Xid>,
        R: IntoIterator<Item = Xid>,
    {
        let mut xip: ActiveXids = xip.into_iter().collect();
        xip.sort_unstable();
        let mut own_live_subxids: ActiveXids = own_live_subxids.into_iter().collect();
        own_live_subxids.sort_unstable();
        let mut own_rolled_back_subxids: ActiveXids = own_rolled_back_subxids.into_iter().collect();
        own_rolled_back_subxids.sort_unstable();
        let own_subxid_lo =
            Self::compute_own_subxid_lo(xmax, &own_live_subxids, &own_rolled_back_subxids);
        Self {
            xmin,
            xmax,
            current_xid,
            current_command,
            xip,
            own_live_subxids,
            own_rolled_back_subxids,
            own_subxid_lo,
        }
    }

    /// Lowest XID that could be an own subxid, given the two (sorted)
    /// subxid sets. When both are empty this is `xmax`, so every real
    /// per-tuple `xid` (which is `< xmax`) sorts below the bound and the
    /// subxid searches are skipped.
    fn compute_own_subxid_lo(
        xmax: Xid,
        own_live_subxids: &[Xid],
        own_rolled_back_subxids: &[Xid],
    ) -> Xid {
        let live_lo = own_live_subxids.first().copied();
        let rb_lo = own_rolled_back_subxids.first().copied();
        match (live_lo, rb_lo) {
            (Some(a), Some(b)) => a.min(b),
            (Some(a), None) => a,
            (None, Some(b)) => b,
            (None, None) => xmax,
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

    /// `true` iff `xid` is the transaction requesting this snapshot —
    /// the parent top-level XID, **or** one of this backend's own live
    /// (or released-but-uncommitted) subtransaction XIDs.
    ///
    /// The parent equality is tested first because it is by far the
    /// hottest case. When no savepoint is open in this backend
    /// (`own_live_subxids` empty — the universal case) the `is_empty`
    /// guard short-circuits before the range check and the
    /// `binary_search`, making this the exact single-`Xid` compare it
    /// was before subtransaction support.
    #[must_use]
    pub fn is_current_xid(&self, xid: Xid) -> bool {
        if xid == self.current_xid {
            return true;
        }
        // Own live subxids: empty (and thus `lo == xmax`) in the common
        // case, so any real `xid < xmax` fails the range pre-filter and
        // we never touch the search.
        !self.own_live_subxids.is_empty()
            && xid >= self.own_subxid_lo
            && self.own_live_subxids.binary_search(&xid).is_ok()
    }

    /// `true` iff `xid` is one of this backend's own *rolled-back*
    /// subtransaction XIDs.
    ///
    /// A tuple whose `xmin` is rolled back is invisible to the owning
    /// transaction even before the CLOG abort is observed; a tuple whose
    /// `xmax` is rolled back has its delete / in-place update reverted.
    /// Short-circuits on an empty set and on the own-subxid range
    /// pre-filter, so the no-rolled-back-savepoint path pays nothing.
    #[must_use]
    pub fn own_subxid_rolled_back(&self, xid: Xid) -> bool {
        !self.own_rolled_back_subxids.is_empty()
            && xid >= self.own_subxid_lo
            && self.own_rolled_back_subxids.binary_search(&xid).is_ok()
    }

    /// Replace this snapshot's own-subtransaction sets in place.
    ///
    /// Used by the transaction manager to keep a **frozen** snapshot
    /// (REPEATABLE READ / SERIALIZABLE, which is not rebuilt at every
    /// statement) coherent with the savepoint stack after
    /// `SAVEPOINT` / `RELEASE` / `ROLLBACK TO`. Only the two subxid
    /// `SmallVec`s and the derived range bound are touched — `xmin`,
    /// `xmax`, `xip`, `current_xid`, and `current_command` are left
    /// untouched, preserving snapshot stability.
    ///
    /// Both iterators need not be sorted; this method sorts them.
    pub fn set_own_subxids<L, R>(&mut self, live: L, rolled_back: R)
    where
        L: IntoIterator<Item = Xid>,
        R: IntoIterator<Item = Xid>,
    {
        let mut live: ActiveXids = live.into_iter().collect();
        live.sort_unstable();
        let mut rolled_back: ActiveXids = rolled_back.into_iter().collect();
        rolled_back.sort_unstable();
        self.own_subxid_lo = Self::compute_own_subxid_lo(self.xmax, &live, &rolled_back);
        self.own_live_subxids = live;
        self.own_rolled_back_subxids = rolled_back;
    }

    /// This backend's own live (or merged-up) subtransaction XIDs,
    /// sorted ascending. Empty when no savepoint is open.
    #[must_use]
    pub fn own_live_subxids(&self) -> &[Xid] {
        &self.own_live_subxids
    }

    /// This backend's own rolled-back subtransaction XIDs, sorted
    /// ascending. Empty when no savepoint was rolled back.
    #[must_use]
    pub fn own_rolled_back_subxids(&self) -> &[Xid] {
        &self.own_rolled_back_subxids
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

    /// Build a snapshot with explicit own-subxid sets.
    fn snap_sub(
        xmin: u64,
        xmax: u64,
        cur: u64,
        cmd: u32,
        in_progress: &[u64],
        live: &[u64],
        rolled_back: &[u64],
    ) -> Snapshot {
        Snapshot::new_with_subxids(
            Xid::new(xmin),
            Xid::new(xmax),
            Xid::new(cur),
            CommandId::new(cmd),
            in_progress.iter().map(|&x| Xid::new(x)),
            live.iter().map(|&x| Xid::new(x)),
            rolled_back.iter().map(|&x| Xid::new(x)),
        )
    }

    #[test]
    fn is_current_xid_includes_own_live_subxids() {
        // parent = 15, two live subxids 30 and 31.
        let s = snap_sub(10, 40, 15, 0, &[], &[31, 30], &[]);
        // Parent is current.
        assert!(s.is_current_xid(Xid::new(15)));
        // Each own live subxid is current (set is sorted on build).
        assert!(s.is_current_xid(Xid::new(30)));
        assert!(s.is_current_xid(Xid::new(31)));
        // A foreign / unrelated xid is not.
        assert!(!s.is_current_xid(Xid::new(14)));
        assert!(!s.is_current_xid(Xid::new(32)));
    }

    #[test]
    fn rolled_back_subxid_is_not_current_but_is_flagged() {
        // 30 live (self), 31 rolled back (not self, flagged invisible).
        let s = snap_sub(10, 40, 15, 0, &[], &[30], &[31]);
        assert!(s.is_current_xid(Xid::new(30)));
        assert!(!s.is_current_xid(Xid::new(31)));
        assert!(s.own_subxid_rolled_back(Xid::new(31)));
        assert!(!s.own_subxid_rolled_back(Xid::new(30)));
    }

    #[test]
    fn empty_subxid_sets_short_circuit() {
        // No savepoint: only the parent is current, nothing rolled back.
        let s = snap_sub(10, 40, 15, 0, &[], &[], &[]);
        assert!(s.is_current_xid(Xid::new(15)));
        assert!(!s.is_current_xid(Xid::new(30)));
        assert!(!s.own_subxid_rolled_back(Xid::new(30)));
        // The range bound collapses to xmax so every real xid < xmax is
        // below it (the pre-filter never enters the search).
        assert!(!s.own_subxid_rolled_back(Xid::new(39)));
        assert_eq!(s.own_live_subxids(), &[] as &[Xid]);
        assert_eq!(s.own_rolled_back_subxids(), &[] as &[Xid]);
    }

    #[test]
    fn set_own_subxids_patches_only_subxid_sets() {
        let mut s = snap_sub(10, 40, 15, 3, &[12, 14], &[], &[]);
        let xmin = s.xmin;
        let xmax = s.xmax;
        let xip: Vec<Xid> = s.xip().to_vec();
        let cur = s.current_xid;
        let cmd = s.current_command;

        s.set_own_subxids([Xid::new(31), Xid::new(30)], [Xid::new(32)]);

        // The frozen, isolation-bearing fields are untouched.
        assert_eq!(s.xmin, xmin);
        assert_eq!(s.xmax, xmax);
        assert_eq!(s.xip().to_vec(), xip);
        assert_eq!(s.current_xid, cur);
        assert_eq!(s.current_command, cmd);

        // The subxid sets are now populated and sorted.
        assert_eq!(s.own_live_subxids(), &[Xid::new(30), Xid::new(31)]);
        assert_eq!(s.own_rolled_back_subxids(), &[Xid::new(32)]);
        assert!(s.is_current_xid(Xid::new(30)));
        assert!(s.is_current_xid(Xid::new(31)));
        assert!(s.own_subxid_rolled_back(Xid::new(32)));

        // Clearing them restores the no-savepoint short-circuit.
        s.set_own_subxids(std::iter::empty(), std::iter::empty());
        assert!(!s.is_current_xid(Xid::new(30)));
        assert!(!s.own_subxid_rolled_back(Xid::new(32)));
    }

    #[test]
    fn new_delegates_to_empty_subxids() {
        let s = snap(10, 20, 15, 0, &[12, 14]);
        assert_eq!(s.own_live_subxids(), &[] as &[Xid]);
        assert_eq!(s.own_rolled_back_subxids(), &[] as &[Xid]);
    }
}
