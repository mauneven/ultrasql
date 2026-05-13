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
use crate::tuple_header::TupleHeader;

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
#[must_use]
pub fn is_visible<O: XidStatusOracle + ?Sized>(
    header: &TupleHeader,
    snapshot: &Snapshot,
    oracle: &O,
) -> Visibility {
    if header.infomask.is_frozen() {
        return Visibility::Visible;
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
        assert_eq!(is_visible(&header, &snap, &oracle), Visibility::DeletedByOwn);
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
}
