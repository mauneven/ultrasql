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

/// Outcome of a visibility check.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Visibility {
    /// The tuple is visible to the snapshot — read the slot's
    /// payload bytes directly.
    Visible,
    /// The tuple is not visible — and the executor should skip past
    /// it without further processing.
    Invisible,
    /// The tuple is *visible-with-update-conflict*: the requester has
    /// already deleted it within the same transaction at a later
    /// command. Returned for completeness; an UPDATE statement uses
    /// this to short-circuit.
    DeletedByOwn,
    /// The tuple is visible **but the slot's current bytes are the
    /// post-image of an in-place UPDATE the reader's snapshot does
    /// not yet see committed**. The caller must consult the
    /// per-relation undo log keyed by `(tid, writer_xid)` to recover
    /// the pre-update payload bytes the reader should observe.
    ///
    /// Emitted only for tuples carrying [`InfoMask::UPDATED_IN_PLACE`]
    /// whose `xmax` is either still in-progress, aborted, or
    /// committed *after* the reader's snapshot. The reader's
    /// snapshot therefore predates the UPDATE; without this signal
    /// the caller would return the post-image, silently violating
    /// snapshot isolation.
    VisiblePreImage,
}

/// Decide whether `header` is visible to `snapshot`, consulting the
/// `oracle` for transaction status when needed.
///
/// This is the single core MVCC predicate used by every heap and index
/// read path. Subtransaction-rollback awareness is carried **in the
/// snapshot** (via [`Snapshot::own_subxid_rolled_back`] /
/// [`Snapshot::is_current_xid`]), not through a separate oracle: a row's
/// savepoint-ness is never recorded on disk, so the snapshot's own-subxid
/// sets are the sole authority. This keeps savepoint state off the heap
/// and makes the predicate correct **independent of whether physical heap
/// undo has run** after a `ROLLBACK TO`.
///
/// Decision tree (mirroring `HeapTupleSatisfiesMVCC` with the subxid
/// extension):
///
/// 1. **Frozen tuples** are visible to everyone.
/// 2. **Own rolled-back insert guard:** if `xmin` is one of our own
///    rolled-back subtransactions, the row is invisible — its insert
///    "did not count" — beating any CLOG "committed" hint at any
///    isolation level.
/// 3. If the inserter is the current transaction (parent or a live /
///    merged-up subxid):
///    - if the insert command is **after** the snapshot's current
///      command, the tuple is invisible.
///    - **DEFECT-3 own rolled-back xmax guard:** if `xmax` is one of our
///      own rolled-back subtransactions, the delete / in-place update
///      "did not count" — the row reverts to its live / pre-image form.
///    - if we in-place-updated it ourselves, surface the right
///      pre-/post-image view.
///    - if we deleted it ourselves at an earlier command, surface
///      [`Visibility::DeletedByOwn`].
///    - otherwise the tuple is visible.
/// 4. If the inserter is **not** committed (in progress or aborted)
///    according to the snapshot, the tuple is invisible.
/// 5. If `xmax` names one of our own rolled-back subxids, the foreign-
///    inserted row's delete is reverted: it stays visible.
/// 6. If the deleter is committed before the snapshot, the tuple is
///    invisible.
/// 7. Otherwise the tuple is visible.
#[must_use]
pub fn is_visible<O: XidStatusOracle + ?Sized>(
    header: &TupleHeader,
    snapshot: &Snapshot,
    oracle: &O,
) -> Visibility {
    if header.infomask.is_frozen() {
        return Visibility::Visible;
    }

    // --- own rolled-back insert guard -----------------------------------
    //
    // A row inserted under one of our own savepoints that we later rolled
    // back is invisible — even if the subxid's CLOG entry says committed
    // (it never will under the keep-InProgress release model, but a
    // belt-and-suspenders check here keeps visibility self-sufficient).
    if snapshot.own_subxid_rolled_back(header.xmin) {
        return Visibility::Invisible;
    }

    // --- check xmin -----------------------------------------------------

    if header.xmin.is_invalid() {
        // No inserter recorded — likely a fresh slot or stale storage.
        return Visibility::Invisible;
    }

    if snapshot.is_current_xid(header.xmin) {
        // Inserted by us (parent or a live / merged-up subxid). Visible
        // only at commands strictly later than when the insert happened.
        if header.cmin >= snapshot.current_command {
            return Visibility::Invisible;
        }

        // DEFECT-3 FIX (the masked self-inserter gap): our own row was
        // deleted / in-place-updated by a subxid we rolled back. The
        // delete / update "did not count" — revert independently of any
        // physical undo. This must run *before* the live-xmax handling
        // below, which would otherwise treat the reverted xmax as a real
        // own-delete / own-update.
        if !header.xmax.is_invalid() && snapshot.own_subxid_rolled_back(header.xmax) {
            return if header.infomask.contains(InfoMask::UPDATED_IN_PLACE) {
                // The slot bytes are the rolled-back post-image; the
                // caller must substitute the undo-log pre-image.
                Visibility::VisiblePreImage
            } else {
                Visibility::Visible
            };
        }

        // In-place UPDATE we performed ourselves at a prior command:
        // the slot's current bytes are the right view. The classical
        // "DeletedByOwn" branch below treats `xmax == current_xid` as
        // a delete; for in-place updates that is wrong — the slot
        // still represents a live tuple, just with a newer payload.
        if header.infomask.contains(InfoMask::UPDATED_IN_PLACE)
            && snapshot.is_current_xid(header.xmax)
        {
            if header.cmax >= snapshot.current_command {
                // Own UPDATE issued at a future command — this command
                // index should still see the pre-image (own pending
                // write is not visible to commands ≤ cmax).
                return Visibility::VisiblePreImage;
            }
            return Visibility::Visible;
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

    // Foreign-inserted row whose delete / in-place update was performed by
    // one of our own rolled-back subxids: the deletion "did not count".
    if snapshot.own_subxid_rolled_back(header.xmax) {
        return if header.infomask.contains(InfoMask::UPDATED_IN_PLACE) {
            Visibility::VisiblePreImage
        } else {
            Visibility::Visible
        };
    }

    // In-place UPDATE semantics: a non-`INVALID` xmax with the
    // `UPDATED_IN_PLACE` bit means the slot's payload is the
    // *post-update* version. The pre-update version is held in the
    // heap's side-channel undo log keyed by `TupleId`. The tuple is
    // therefore always *visible* under MVCC — both the pre- and
    // post-update views exist for some snapshot — but which view a
    // reader observes depends on whether xmax is committed-before-
    // snapshot:
    //   - If yes (or xmax == current_xid, our own write), the slot
    //     bytes are the right payload: return `Visible`.
    //   - If no (xmax in-progress, aborted, or in the snapshot's
    //     future), the reader's snapshot predates the update: return
    //     `VisiblePreImage` so the caller substitutes the undo-log
    //     entry's pre-image bytes.
    if header.infomask.contains(InfoMask::UPDATED_IN_PLACE) {
        if snapshot.is_current_xid(header.xmax) {
            // Own write. Visible to commands strictly after `cmax`.
            if header.cmax < snapshot.current_command {
                return Visibility::Visible;
            }
            // Own write at a future command — pre-image is the right
            // view for this command index.
            return Visibility::VisiblePreImage;
        }
        if is_committed_before_snapshot(header.xmax, snapshot, oracle) {
            return Visibility::Visible;
        }
        return Visibility::VisiblePreImage;
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

    // ── subtransaction visibility (snapshot-driven) ───────────────────────────

    /// A row inserted under one of our own rolled-back subxids is
    /// invisible even when the CLOG would report it committed — the
    /// snapshot's rolled-back set beats the oracle.
    #[test]
    fn own_rolled_back_insert_is_invisible() {
        let header = h(42, 0, 0, 0); // inserted by subxid 42
        let mut snap = snap(10, 60, 50, 1);
        // Parent is 50; subxid 42 was rolled back.
        snap.set_own_subxids(std::iter::empty(), [Xid::new(42)]);
        let oracle = MapOracle::new();
        oracle.set_committed(Xid::new(42)); // CLOG lies "committed".
        assert_eq!(
            is_visible(&header, &snap, &oracle),
            Visibility::Invisible,
            "rolled-back savepoint insert must be invisible despite CLOG"
        );
    }

    /// A row inserted under a *live* (not rolled-back) own subxid is
    /// visible to the same transaction.
    #[test]
    fn own_live_subxid_insert_is_visible() {
        let header = h(42, 0, 0, 0); // inserted by subxid 42 at cmd 0
        let mut snap = snap(10, 60, 50, 1);
        snap.set_own_subxids([Xid::new(42)], std::iter::empty());
        let oracle = MapOracle::new();
        oracle.set_in_progress(Xid::new(42));
        assert_eq!(
            is_visible(&header, &snap, &oracle),
            Visibility::Visible,
            "live savepoint insert must be visible to its own transaction"
        );
    }

    /// DEFECT-3: our own row (inserted by the parent) was DELETEd by a
    /// subxid we rolled back. The delete "did not count" — the row must
    /// be visible again, independent of physical undo.
    #[test]
    fn own_row_deleted_by_rolled_back_subxid_reverts_to_visible() {
        // Parent 50 inserted at cmd 0; subxid 42 deleted it at cmd 1.
        let header = h(50, 0, 42, 1);
        let mut snap = snap(10, 60, 50, 5);
        snap.set_own_subxids(std::iter::empty(), [Xid::new(42)]);
        let oracle = MapOracle::new();
        assert_eq!(
            is_visible(&header, &snap, &oracle),
            Visibility::Visible,
            "row deleted by a rolled-back subxid must revert to visible"
        );
    }

    /// DEFECT-3 (in-place update variant): our own row was in-place
    /// UPDATEd by a subxid we rolled back. The slot bytes are the
    /// rolled-back post-image, so the caller must see the pre-image.
    #[test]
    fn own_row_in_place_updated_by_rolled_back_subxid_yields_pre_image() {
        let mut header = h(50, 0, 42, 1);
        header.infomask.set(InfoMask::UPDATED_IN_PLACE);
        let mut snap = snap(10, 60, 50, 5);
        snap.set_own_subxids(std::iter::empty(), [Xid::new(42)]);
        let oracle = MapOracle::new();
        assert_eq!(
            is_visible(&header, &snap, &oracle),
            Visibility::VisiblePreImage,
            "in-place update by a rolled-back subxid must surface the pre-image"
        );
    }

    /// A foreign-committed row whose delete was performed by one of our
    /// own rolled-back subxids stays visible (the delete is reverted).
    #[test]
    fn foreign_row_deleted_by_rolled_back_subxid_stays_visible() {
        // Inserted by committed foreign txn 5; deleted by our subxid 42.
        let header = h(5, 0, 42, 0);
        let mut snap = snap(10, 60, 50, 5);
        snap.set_own_subxids(std::iter::empty(), [Xid::new(42)]);
        let oracle = MapOracle::new();
        oracle.set_committed(Xid::new(5));
        assert_eq!(
            is_visible(&header, &snap, &oracle),
            Visibility::Visible,
            "foreign row whose delete was rolled back stays visible"
        );
    }

    /// A savepoint write that was NOT rolled back follows normal MVCC
    /// rules (visible when committed).
    #[test]
    fn non_rolled_back_subxact_follows_normal_rules() {
        let header = h(5, 0, 0, 0);
        let snap = snap(10, 20, 50, 0);
        let oracle = MapOracle::new();
        oracle.set_committed(Xid::new(5));
        assert_eq!(
            is_visible(&header, &snap, &oracle),
            Visibility::Visible,
            "non-rolled-back committed tuple must be visible"
        );
    }

    // ---------------------------------------------------------------
    // UPDATED_IN_PLACE — cross-snapshot pre-image disclosure
    // ---------------------------------------------------------------

    #[test]
    fn in_place_update_post_image_when_writer_committed_before_snapshot() {
        // Writer xid 7 committed; reader's snapshot xmax = 20 (so
        // writer is visible). Reader should see the slot's post-
        // image — `Visibility::Visible`.
        let mut header = h(5, 0, 7, 0);
        header.infomask.set(InfoMask::UPDATED_IN_PLACE);
        let snap = snap(10, 20, 50, 0);
        let oracle = MapOracle::new();
        oracle.set_committed(Xid::new(5));
        oracle.set_committed(Xid::new(7));
        assert_eq!(is_visible(&header, &snap, &oracle), Visibility::Visible);
    }

    #[test]
    fn in_place_update_pre_image_when_writer_in_progress() {
        // Writer xid 15 still in-progress; reader's snapshot does
        // NOT see it. Reader must observe the pre-image via the
        // undo log — `Visibility::VisiblePreImage`.
        let mut header = h(5, 0, 15, 0);
        header.infomask.set(InfoMask::UPDATED_IN_PLACE);
        let snap = Snapshot::new(
            Xid::new(10),
            Xid::new(20),
            Xid::new(50),
            CommandId::new(0),
            [Xid::new(15)],
        );
        let oracle = MapOracle::new();
        oracle.set_committed(Xid::new(5));
        oracle.set_in_progress(Xid::new(15));
        assert_eq!(
            is_visible(&header, &snap, &oracle),
            Visibility::VisiblePreImage,
            "concurrent in-place UPDATE must surface the pre-image to a snapshot that pre-dates the writer's commit",
        );
    }

    #[test]
    fn in_place_update_pre_image_when_writer_committed_after_snapshot() {
        // Writer xid 25 committed but with xid ≥ snapshot's xmax —
        // committed *after* this snapshot was taken. Reader must
        // see the pre-image.
        let mut header = h(5, 0, 25, 0);
        header.infomask.set(InfoMask::UPDATED_IN_PLACE);
        let snap = snap(10, 20, 50, 0);
        let oracle = MapOracle::new();
        oracle.set_committed(Xid::new(5));
        oracle.set_committed(Xid::new(25));
        assert_eq!(
            is_visible(&header, &snap, &oracle),
            Visibility::VisiblePreImage,
            "writer committed after the reader's snapshot was taken: pre-image is the right view",
        );
    }

    #[test]
    fn in_place_update_own_write_post_image_after_command_boundary() {
        // We performed the in-place UPDATE ourselves at command 0;
        // we're now running at command 3 — our own post-image is
        // the right view.
        let mut header = h(50, 0, 50, 0);
        header.infomask.set(InfoMask::UPDATED_IN_PLACE);
        let snap = snap(10, 60, 50, 3);
        let oracle = MapOracle::new();
        assert_eq!(is_visible(&header, &snap, &oracle), Visibility::Visible);
    }
}
