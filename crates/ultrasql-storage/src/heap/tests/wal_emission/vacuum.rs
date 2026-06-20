//! `vacuum_heap` behavior: reclaiming committed dead tuples, keeping
//! committed in-place-update slots, and skipping in-progress/alive rows.

use ultrasql_core::{CommandId, Xid};
use ultrasql_mvcc::status::test_support::MapOracle;
use ultrasql_mvcc::Snapshot;

use super::rel;
use crate::heap::tests::{del_opts, int32_pair_payload, make_heap, opts, update_int32_edit, update_int32_stamp};
use crate::heap::{UpdateInt32PairTid};

// ------------------------------------------------------------------
// vacuum_heap tests
// ------------------------------------------------------------------

#[test]
fn vacuum_heap_reclaims_committed_dead_tuples() {
    let heap = make_heap(16);
    let r = rel();

    // Insert two tuples under different XIDs.
    let t1 = heap.insert(r, b"live", opts(10)).unwrap();
    let t2 = heap.insert(r, b"dead", opts(20)).unwrap();

    // Delete t2 under XID 30.
    heap.delete(t2, del_opts(30, 0)).unwrap();

    // Build an oracle that says XIDs 10, 20, 30 are all committed.
    let oracle = MapOracle::default();
    oracle.set_committed(Xid::new(10));
    oracle.set_committed(Xid::new(20));
    oracle.set_committed(Xid::new(30));

    // oldest_active_xid > 30, so XID 30 is eligible for vacuum.
    let stats = heap.vacuum_heap(r, Xid::new(100), &oracle).unwrap();
    assert_eq!(stats.tuples_reclaimed, 1, "one dead tuple expected");
    assert_eq!(
        stats.pages_compacted, 1,
        "one page should have been compacted"
    );

    // t1 must still be fetchable; t2 must be gone (slot is now dead/unused).
    let live = heap.fetch(t1).unwrap();
    assert_eq!(live.data, b"live");
}

#[test]
fn vacuum_heap_keeps_committed_in_place_update_slot() {
    let heap = make_heap(16);
    let r = rel();
    let tid = heap
        .insert(r, &int32_pair_payload(1, 10), opts(10))
        .unwrap();

    let oracle = MapOracle::new();
    oracle.set_committed(Xid::new(10));
    let writer_20 = Snapshot::new(
        Xid::new(10),
        Xid::new(100),
        Xid::new(20),
        CommandId::FIRST,
        std::iter::empty(),
    );
    let updated = heap
        .update_int32_pair_tid_inplace_undo(
            UpdateInt32PairTid {
                tid,
                snapshot: &writer_20,
                oracle: &oracle,
                predicate: |id, _val| id == 1,
            },
            update_int32_edit(1, 5),
            update_int32_stamp(20),
            None,
            None,
        )
        .unwrap();
    assert_eq!(updated, 1);
    assert_eq!(heap.fetch(tid).unwrap().data, int32_pair_payload(1, 15));

    oracle.set_committed(Xid::new(20));
    let stats = heap.vacuum_heap(r, Xid::new(100), &oracle).unwrap();
    assert_eq!(stats.tuples_reclaimed, 0);
    assert_eq!(stats.pages_compacted, 0);
    assert_eq!(heap.fetch(tid).unwrap().data, int32_pair_payload(1, 15));
}

#[test]
fn vacuum_heap_skips_in_progress_deleters() {
    let heap = make_heap(16);
    let r = rel();

    let t = heap.insert(r, b"row", opts(10)).unwrap();
    heap.delete(t, del_opts(50, 0)).unwrap();

    let oracle = MapOracle::default();
    oracle.set_committed(Xid::new(10));
    // XID 50 is NOT committed in the oracle.

    // oldest_active_xid = 40 < 50, so XID 50 is still "in progress".
    let stats = heap.vacuum_heap(r, Xid::new(40), &oracle).unwrap();
    assert_eq!(
        stats.tuples_reclaimed, 0,
        "in-progress delete must not be vacuumed"
    );
}

#[test]
fn vacuum_heap_skips_alive_tuples() {
    let heap = make_heap(16);
    let r = rel();

    heap.insert(r, b"still alive", opts(10)).unwrap();

    let oracle = MapOracle::default();
    oracle.set_committed(Xid::new(10));

    let stats = heap.vacuum_heap(r, Xid::new(100), &oracle).unwrap();
    assert_eq!(
        stats.tuples_reclaimed, 0,
        "alive tuple must not be reclaimed"
    );
    assert_eq!(stats.pages_compacted, 0);
}
