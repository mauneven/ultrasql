//! Tuple visibility predicate.
//!
//! The predicate matches PostgreSQL's `HeapTupleSatisfiesMVCC` exactly
//! for the cases UltraSQL supports today. The full PostgreSQL rule set
//! has a long tail of subtleties (multixact members, key-share locks,
//! infomask hint-bit advancement) that this module deliberately omits
//! at this stage; the omissions are tracked in the roadmap.

use ultrasql_core::Xid;

use crate::snapshot::Snapshot;
use crate::status::{XidStatus, XidStatusOracle};
use crate::tuple_header::{InfoMask, TupleHeader};

/// Oracle consulted when a tuple has the [`InfoMask::SUBXACT`] bit set.
///
/// Implementations return `true` if the given subtransaction XID was
/// rolled back within its parent transaction.  A tuple with `SUBXACT` set
/// whose `xmin` returns `true` here is invisible even to the parent
/// transaction — it was written under a savepoint that was subsequently
/// rolled back.
///
/// A no-op implementation (always returns `false`) is correct when
/// subtransactions are not in use or when the CLOG already marks the subxid
/// as `Aborted` (visibility will then reject it via the normal oracle path).
/// The dedicated oracle exists for the case where the parent transaction is
/// still in progress and the CLOG entry for the subxid is `Aborted` but
/// the caller wants a fast local check without a CLOG lookup.
pub trait SubxactOracle: Send + Sync {
    /// Return `true` iff `subxid` was rolled back within its parent
    /// transaction.
    fn is_rolled_back(&self, subxid: Xid) -> bool;
}

/// A [`SubxactOracle`] that always returns `false`.
///
/// Use this when no subtransaction tracking is available or when the
/// normal XID-status oracle already handles aborted subtransactions via
/// CLOG entries.
/// A no-op [`SubxactOracle`] for use when subtransactions are not tracked.
///
/// See module-level docs for usage.
#[derive(Debug)]
pub struct NoSubxacts;

impl SubxactOracle for NoSubxacts {
    fn is_rolled_back(&self, _subxid: Xid) -> bool {
        false
    }
}

/// Outcome of a visibility check.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Visibility {
    /// The tuple is visible to the snapshot.
    Visible,
    /// The tuple is not visible — and the executor should skip past
    /// it without further processing.
    Invisible,
    /// The tuple is *visible-with-update-conflict*: the requester has
    /// already deleted it within the same transaction at a later
    /// command. Returned for completeness; an UPDATE statement uses
    /// this to short-circuit.
    DeletedByOwn,
}

/// Decide whether `header` is visible to `snapshot`, consulting the
/// `oracle` for transaction status when needed.
///
/// This is the core MVCC predicate used by heap scans. For scenarios
/// involving subtransaction rollback tracking, prefer
/// [`is_visible_ext`] which accepts an additional [`SubxactOracle`]
/// parameter.
///
/// Decision tree (mirroring HeapTupleSatisfiesMVCC):
///
/// 1. **Frozen tuples** are visible to everyone.
/// 2. If the inserter is the current transaction:
///    - if the insert command is **after** the snapshot's current
///      command, the tuple is invisible (a statement does not see
///      writes from its own future).
///    - if the deleter is also the current transaction at an earlier
///      command, the tuple is **[`Visibility::DeletedByOwn`]**.
///    - otherwise the tuple is visible.
/// 3. If the inserter is **not** committed (in progress or aborted)
///    according to the snapshot, the tuple is invisible.
/// 4. If the deleter is committed before the snapshot, the tuple is
///    invisible.
/// 5. Otherwise the tuple is visible.
///
/// Note: tuples with the [`InfoMask::SUBXACT`] bit set are handled
/// correctly when the CLOG already marks the subtransaction `Aborted`.
/// For the in-transaction case where the parent is still in-progress,
/// use [`is_visible_ext`] with a proper [`SubxactOracle`].
#[must_use]
pub fn is_visible<O: XidStatusOracle + ?Sized>(
    header: &TupleHeader,
    snapshot: &Snapshot,
    oracle: &O,
) -> Visibility {
    is_visible_ext(header, snapshot, oracle, &NoSubxacts)
}

/// Decide whether `header` is visible, with subtransaction rollback
/// awareness.
///
/// Extends [`is_visible`] with an additional [`SubxactOracle`] consulted
/// when the tuple has the [`InfoMask::SUBXACT`] bit set. A tuple written
/// by a rolled-back savepoint is invisible even to the parent transaction.
///
/// Decision tree (mirroring `HeapTupleSatisfiesMVCC` with subtxn extension):
///
/// 1. **Frozen tuples** are visible to everyone.
/// 2. If the tuple has the [`InfoMask::SUBXACT`] bit set and `subxact`
///    reports the `xmin` subtransaction XID as rolled back, the tuple is
///    invisible — it was written under a savepoint that was rolled back.
/// 3. If the inserter is the current transaction:
///    - if the insert command is **after** the snapshot's current
///      command, the tuple is invisible.
///    - if the deleter is also the current transaction at an earlier
///      command, the tuple is **[`Visibility::DeletedByOwn`]**.
///    - otherwise the tuple is visible.
/// 4. If the inserter is **not** committed (in progress or aborted)
///    according to the snapshot, the tuple is invisible.
/// 5. If the deleter is committed before the snapshot, the tuple is
///    invisible.
/// 6. Otherwise the tuple is visible.
#[must_use]
pub fn is_visible_ext<O, S>(
    header: &TupleHeader,
    snapshot: &Snapshot,
    oracle: &O,
    subxact: &S,
) -> Visibility
where
    O: XidStatusOracle + ?Sized,
    S: SubxactOracle + ?Sized,
{
    if header.infomask.is_frozen() {
        return Visibility::Visible;
    }

    // --- subtransaction rollback check ----------------------------------

    if header.infomask.contains(InfoMask::SUBXACT) && subxact.is_rolled_back(header.xmin) {
        // Written under a savepoint that was subsequently rolled back.
        // Invisible even to the parent transaction.
        return Visibility::Invisible;
    }

    // --- check xmin -----------------------------------------------------

    if header.xmin.is_invalid() {
        // No inserter recorded — likely a fresh slot or stale storage.
        return Visibility::Invisible;
    }

    if snapshot.is_current_xid(header.xmin) {
        // Inserted by us. Visible only at commands strictly later than
        // when the insert happened.
        if header.cmin >= snapshot.current_command {
            return Visibility::Invisible;
        }

        // If we then deleted it in a subsequent command, surface the
        // distinct DeletedByOwn outcome.
        if snapshot.is_current_xid(header.xmax) && header.cmax < snapshot.current_command {
            return Visibility::DeletedByOwn;
        }

        // Otherwise visible — regardless of whether some other still-
        // running transaction is also trying to delete it.
        return Visibility::Visible;
    }

    // Inserter is some other transaction. It must have committed
    // *before* this snapshot to be visible.
    if !is_committed_before_snapshot(header.xmin, snapshot, oracle) {
        return Visibility::Invisible;
    }

    // --- check xmax -----------------------------------------------------

    if header.xmax.is_invalid() {
        return Visibility::Visible;
    }

    if snapshot.is_current_xid(header.xmax) {
        // We deleted it ourselves. Hidden at the deleting command and
        // every later command in the same transaction.
        if header.cmax < snapshot.current_command {
            return Visibility::Invisible;
        }
        return Visibility::Visible;
    }

    if is_committed_before_snapshot(header.xmax, snapshot, oracle) {
        return Visibility::Invisible;
    }

    Visibility::Visible
}

/// Helper: `xid` committed *and* it committed before this snapshot.
/// The "before this snapshot" half is encoded by the snapshot's
/// in-progress predicate.
fn is_committed_before_snapshot<O: XidStatusOracle + ?Sized>(
    xid: Xid,
    snapshot: &Snapshot,
    oracle: &O,
) -> bool {
    if snapshot.xid_in_progress(xid) {
        return false;
    }
    matches!(oracle.status(xid), XidStatus::Committed | XidStatus::Frozen)
}

#[cfg(test)]
mod tests {
    use ultrasql_core::{BlockNumber, CommandId, PageId, RelationId, TupleId};

    use super::*;
    use crate::status::test_support::MapOracle;
    use crate::tuple_header::InfoMask;

    fn tid() -> TupleId {
        TupleId::new(PageId::new(RelationId::new(1), BlockNumber::new(0)), 0)
    }

    fn snap(xmin: u64, xmax: u64, cur: u64, cmd: u32) -> Snapshot {
        Snapshot::new(
            Xid::new(xmin),
            Xid::new(xmax),
            Xid::new(cur),
            CommandId::new(cmd),
            std::iter::empty(),
        )
    }

    fn h(xmin: u64, cmin: u32, xmax: u64, cmax: u32) -> TupleHeader {
        let mut h = TupleHeader::fresh(Xid::new(xmin), CommandId::new(cmin), tid(), 1);
        if xmax != 0 {
            h.mark_deleted(Xid::new(xmax), CommandId::new(cmax));
        }
        h
    }

    #[test]
    fn frozen_tuple_visible_unconditionally() {
        let mut header = h(0, 0, 0, 0);
        header.infomask.set(InfoMask::FROZEN);
        let snap = snap(10, 20, 50, 0);
        let oracle = MapOracle::new();
        assert_eq!(is_visible(&header, &snap, &oracle), Visibility::Visible);
    }

    #[test]
    fn tuple_inserted_by_committed_other_txn_visible() {
        let header = h(5, 0, 0, 0);
        let snap = snap(10, 20, 50, 0);
        let oracle = MapOracle::new();
        oracle.set_committed(Xid::new(5));
        assert_eq!(is_visible(&header, &snap, &oracle), Visibility::Visible);
    }

    #[test]
    fn tuple_inserted_by_in_progress_other_txn_invisible() {
        let header = h(15, 0, 0, 0);
        // 15 is in progress because it's between xmin and xmax and we
        // include it as such.
        let snap = Snapshot::new(
            Xid::new(10),
            Xid::new(20),
            Xid::new(50),
            CommandId::new(0),
            [Xid::new(15)],
        );
        let oracle = MapOracle::new();
        oracle.set_in_progress(Xid::new(15));
        assert_eq!(is_visible(&header, &snap, &oracle), Visibility::Invisible);
    }

    #[test]
    fn tuple_inserted_by_aborted_other_txn_invisible() {
        let header = h(5, 0, 0, 0);
        let snap = snap(10, 20, 50, 0);
        let oracle = MapOracle::new();
        oracle.set_aborted(Xid::new(5));
        assert_eq!(is_visible(&header, &snap, &oracle), Visibility::Invisible);
    }

    #[test]
    fn tuple_deleted_by_committed_other_txn_invisible() {
        let header = h(5, 0, 7, 0);
        let snap = snap(10, 20, 50, 0);
        let oracle = MapOracle::new();
        oracle.set_committed(Xid::new(5));
        oracle.set_committed(Xid::new(7));
        assert_eq!(is_visible(&header, &snap, &oracle), Visibility::Invisible);
    }

    #[test]
    fn tuple_deleted_by_aborted_other_txn_remains_visible() {
        let header = h(5, 0, 7, 0);
        let snap = snap(10, 20, 50, 0);
        let oracle = MapOracle::new();
        oracle.set_committed(Xid::new(5));
        oracle.set_aborted(Xid::new(7));
        assert_eq!(is_visible(&header, &snap, &oracle), Visibility::Visible);
    }

    #[test]
    fn own_insert_visible_at_later_command() {
        let header = h(50, 0, 0, 0); // inserted at cmd 0
        let snap = snap(10, 60, 50, 1);
        let oracle = MapOracle::new();
        oracle.set_in_progress(Xid::new(50));
        assert_eq!(is_visible(&header, &snap, &oracle), Visibility::Visible);
    }

    #[test]
    fn own_insert_invisible_at_same_command() {
        let header = h(50, 1, 0, 0); // inserted at cmd 1
        let snap = snap(10, 60, 50, 1);
        let oracle = MapOracle::new();
        oracle.set_in_progress(Xid::new(50));
        assert_eq!(is_visible(&header, &snap, &oracle), Visibility::Invisible);
    }

    #[test]
    fn own_insert_then_own_delete_yields_deleted_by_own() {
        let header = h(50, 0, 50, 1); // insert at 0, delete at 1
        let snap = snap(10, 60, 50, 2); // observing at command 2
        let oracle = MapOracle::new();
        oracle.set_in_progress(Xid::new(50));
        assert_eq!(
            is_visible(&header, &snap, &oracle),
            Visibility::DeletedByOwn
        );
    }

    #[test]
    fn invalid_xmin_invisible() {
        let header = h(0, 0, 0, 0);
        let snap = snap(10, 20, 50, 0);
        let oracle = MapOracle::new();
        assert_eq!(is_visible(&header, &snap, &oracle), Visibility::Invisible);
    }

    #[test]
    fn tuple_deleted_by_concurrent_in_progress_remains_visible() {
        // Insert long ago, deletion attempt by an in-flight txn. The
        // snapshot does not see the delete yet.
        let header = h(5, 0, 18, 0);
        let snap = Snapshot::new(
            Xid::new(10),
            Xid::new(20),
            Xid::new(50),
            CommandId::new(0),
            [Xid::new(18)],
        );
        let oracle = MapOracle::new();
        oracle.set_committed(Xid::new(5));
        oracle.set_in_progress(Xid::new(18));
        assert_eq!(is_visible(&header, &snap, &oracle), Visibility::Visible);
    }

    // ── subtransaction visibility ─────────────────────────────────────────────

    /// A tuple with SUBXACT set whose subxid is in the rolled-back set
    /// must be invisible, even when the CLOG would say committed.
    #[test]
    fn subxact_rolled_back_tuple_is_invisible() {
        use std::collections::HashSet;

        struct RolledBackOracle(HashSet<u64>);
        impl SubxactOracle for RolledBackOracle {
            fn is_rolled_back(&self, subxid: Xid) -> bool {
                self.0.contains(&subxid.raw())
            }
        }

        let mut header = h(42, 0, 0, 0);
        header.infomask.set(InfoMask::SUBXACT);

        let snap = snap(10, 60, 50, 1);
        let oracle = MapOracle::new();
        oracle.set_committed(Xid::new(42));

        let rolled = RolledBackOracle(std::iter::once(42).collect());
        assert_eq!(
            is_visible_ext(&header, &snap, &oracle, &rolled),
            Visibility::Invisible,
            "rolled-back savepoint tuple must be invisible"
        );
    }

    /// A tuple with SUBXACT set whose subxid is NOT rolled back follows
    /// normal MVCC rules.
    #[test]
    fn subxact_not_rolled_back_follows_normal_rules() {
        let mut header = h(5, 0, 0, 0);
        header.infomask.set(InfoMask::SUBXACT);

        let snap = snap(10, 20, 50, 0);
        let oracle = MapOracle::new();
        oracle.set_committed(Xid::new(5));

        assert_eq!(
            is_visible_ext(&header, &snap, &oracle, &NoSubxacts),
            Visibility::Visible,
            "non-rolled-back savepoint tuple must be visible when committed"
        );
    }
}
