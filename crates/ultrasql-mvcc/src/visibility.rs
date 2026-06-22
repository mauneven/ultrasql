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
/// This is the single core MVCC predicate used by every heap, index, and
/// catalog scan. Subtransaction (savepoint) awareness is carried entirely
/// by the [`Snapshot`]: its `own_live_subxids` are treated as *self* by
/// [`Snapshot::is_current_xid`], and its `own_rolled_back_subxids` are
/// rejected by [`Snapshot::own_subxid_rolled_back`]. Both sets are empty
/// for the common, no-savepoint case, so this predicate behaves exactly
/// as it did before subtransaction support and pays no added per-tuple
/// cost.
///
/// Decision tree (mirroring `HeapTupleSatisfiesMVCC`):
///
/// 1. **Frozen tuples** are visible to everyone.
/// 2. If the inserter (`xmin`) is one of this backend's own
///    **rolled-back** subtransactions, the tuple is invisible — it was
///    inserted under a savepoint that was rolled back. This overrides
///    any "committed" CLOG hint and applies regardless of isolation
///    level.
/// 3. If the inserter is the current transaction (the parent **or** an
///    own live subxid):
///    - if the insert command is **after** the snapshot's current
///      command, the tuple is invisible (a statement does not see writes
///      from its own future).
///    - if the deleter is also the current transaction at an earlier
///      command, the tuple is **[`Visibility::DeletedByOwn`]**.
///    - otherwise the tuple is visible.
/// 4. If the inserter is **not** committed (in progress or aborted)
///    according to the snapshot, the tuple is invisible.
/// 5. If the deleter (`xmax`) is one of this backend's own
///    **rolled-back** subtransactions, the delete / in-place update did
///    not happen: the row reappears ([`Visibility::Visible`]) or, for an
///    in-place update, its pre-image is surfaced
///    ([`Visibility::VisiblePreImage`]).
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

    // --- own rolled-back subtransaction insert --------------------------
    //
    // A tuple inserted under a savepoint that this backend later rolled
    // back is invisible even before the CLOG abort is observed, and
    // overrides any "committed" hint. O(1) when no savepoint was rolled
    // back in this backend (the common case: the set is empty).
    if snapshot.own_subxid_rolled_back(header.xmin) {
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

    // Own rolled-back subtransaction delete / in-place update: the
    // savepoint that performed it was rolled back, so the operation never
    // happened. The row reappears, or — for an in-place update whose
    // post-image bytes are still physically in the slot (ROLLBACK TO does
    // not undo the heap) — its pre-image is surfaced from the undo log.
    // Gated by an empty-set short-circuit, so the common case pays
    // nothing.
    if snapshot.own_subxid_rolled_back(header.xmax) {
        if header.infomask.contains(InfoMask::UPDATED_IN_PLACE) {
            return Visibility::VisiblePreImage;
        }
        return Visibility::Visible;
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

    // ── subtransaction (savepoint) visibility ─────────────────────────────────
    //
    // Subtransaction awareness lives entirely in the `Snapshot`: own live
    // subxids are *self* (via `is_current_xid`), own rolled-back subxids
    // are rejected (via `own_subxid_rolled_back`). These tests drive the
    // predicate directly through `Snapshot::new_with_subxids`.

    /// Build a snapshot whose `current_xid` is `parent`, carrying explicit
    /// own live and rolled-back subxid sets.
    fn snap_sub(
        xmin: u64,
        xmax: u64,
        parent: u64,
        cmd: u32,
        in_progress: &[u64],
        live: &[u64],
        rolled_back: &[u64],
    ) -> Snapshot {
        Snapshot::new_with_subxids(
            Xid::new(xmin),
            Xid::new(xmax),
            Xid::new(parent),
            CommandId::new(cmd),
            in_progress.iter().map(|&x| Xid::new(x)),
            live.iter().map(|&x| Xid::new(x)),
            rolled_back.iter().map(|&x| Xid::new(x)),
        )
    }

    /// A1: a row inserted by an own *live* subxid is visible to the
    /// owning transaction at a later command — the headline repro at the
    /// predicate level. parent=50, subxid=55, insert at command 0,
    /// observing at command 1.
    #[test]
    fn own_live_subxid_insert_visible_at_later_command() {
        let header = h(55, 0, 0, 0); // xmin = subxid 55, cmin = 0
        let snap = snap_sub(10, 60, 50, 1, &[], &[55], &[]);
        let oracle = MapOracle::new();
        oracle.set_in_progress(Xid::new(55));
        assert_eq!(
            is_visible(&header, &snap, &oracle),
            Visibility::Visible,
            "a txn must see its own write made under an active savepoint",
        );
    }

    /// A2: own live subxid insert is invisible at the same command
    /// (Halloween guard still applies — the cmin/cmax logic is shared
    /// with top-level own writes).
    #[test]
    fn own_live_subxid_insert_invisible_at_same_command() {
        let header = h(55, 1, 0, 0); // inserted at command 1
        let snap = snap_sub(10, 60, 50, 1, &[], &[55], &[]);
        let oracle = MapOracle::new();
        oracle.set_in_progress(Xid::new(55));
        assert_eq!(is_visible(&header, &snap, &oracle), Visibility::Invisible);
    }

    /// A3: a row inserted by a *rolled-back* subxid is invisible to the
    /// owning transaction even when the CLOG (oracle) says Committed —
    /// the "aborted wins over committed for own reads" invariant.
    #[test]
    fn rolled_back_subxid_insert_is_invisible() {
        let header = h(55, 0, 0, 0); // xmin = rolled-back subxid 55
        let snap = snap_sub(10, 60, 50, 1, &[], &[], &[55]);
        let oracle = MapOracle::new();
        oracle.set_committed(Xid::new(55)); // oracle lies "committed"
        assert_eq!(
            is_visible(&header, &snap, &oracle),
            Visibility::Invisible,
            "rolled-back savepoint insert must be invisible regardless of CLOG",
        );
    }

    /// A4: a row *deleted* by a rolled-back subxid reappears. xmin is a
    /// committed other txn, xmax is the rolled-back subxid.
    #[test]
    fn rolled_back_subxid_delete_makes_row_reappear() {
        let header = h(5, 0, 55, 0); // inserted by 5, deleted by subxid 55
        let snap = snap_sub(10, 60, 50, 1, &[], &[], &[55]);
        let oracle = MapOracle::new();
        oracle.set_committed(Xid::new(5));
        oracle.set_committed(Xid::new(55)); // CLOG may even say committed
        assert_eq!(
            is_visible(&header, &snap, &oracle),
            Visibility::Visible,
            "a delete performed under a rolled-back savepoint must not count",
        );
    }

    /// A5: an in-place UPDATE by a rolled-back subxid surfaces the
    /// pre-image (the post-image bytes are stale: ROLLBACK TO does not
    /// physically undo the heap).
    #[test]
    fn rolled_back_subxid_in_place_update_surfaces_pre_image() {
        let mut header = h(5, 0, 55, 0);
        header.infomask.set(InfoMask::UPDATED_IN_PLACE);
        let snap = snap_sub(10, 60, 50, 1, &[], &[], &[55]);
        let oracle = MapOracle::new();
        oracle.set_committed(Xid::new(5));
        oracle.set_committed(Xid::new(55));
        assert_eq!(
            is_visible(&header, &snap, &oracle),
            Visibility::VisiblePreImage,
            "rolled-back in-place update must surface the pre-image",
        );
    }

    /// A6: a released-but-parent-still-open subxid is carried in
    /// `own_live_subxids` (merged_up) so its writes remain self-visible;
    /// after fold-up (not in any set, CLOG committed, removed from xip)
    /// it is visible via the normal committed-before-snapshot path.
    #[test]
    fn released_subxid_visible_via_merged_up_and_after_fold_up() {
        // Released, parent still open: subxid 55 in own_live_subxids.
        let header = h(55, 0, 0, 0);
        let merged = snap_sub(10, 60, 50, 1, &[], &[55], &[]);
        let oracle = MapOracle::new();
        oracle.set_committed(Xid::new(55));
        assert_eq!(
            is_visible(&header, &merged, &oracle),
            Visibility::Visible,
            "released-but-uncommitted subxid stays self-visible",
        );

        // After top-level fold-up: not in any own set, committed in CLOG,
        // not in xip, xmin < xmax. Visible to everyone.
        let folded = snap_sub(10, 60, 99, 1, &[], &[], &[]);
        assert_eq!(
            is_visible(&header, &folded, &oracle),
            Visibility::Visible,
            "folded-up committed subxid is visible via the normal path",
        );
    }

    /// A7: a *foreign* backend's live subxid is invisible to this one. It
    /// sits in `xip` (in-progress), NOT in our `own_live_subxids`, so we
    /// must not over-broaden "self".
    #[test]
    fn foreign_backend_live_subxid_is_invisible() {
        let header = h(15, 0, 0, 0); // foreign subxid 15
        // 15 is in our xip (foreign in-progress); our own sets are empty.
        let snap = snap_sub(10, 60, 50, 1, &[15], &[], &[]);
        let oracle = MapOracle::new();
        oracle.set_in_progress(Xid::new(15));
        assert_eq!(
            is_visible(&header, &snap, &oracle),
            Visibility::Invisible,
            "another backend's uncommitted subxid must remain invisible",
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
