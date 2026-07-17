//! `vacuum_heap` behavior: reclaiming committed dead tuples, keeping
//! committed in-place-update slots, and skipping in-progress/alive rows.

use std::sync::Arc;

use ultrasql_core::{CommandId, Xid};
use ultrasql_mvcc::Snapshot;
use ultrasql_mvcc::status::test_support::MapOracle;

use super::{MapLoader, rel};
use crate::buffer_pool::BufferPool;
use crate::heap::tests::{
    del_opts, int32_pair_payload, make_heap, opts, update_int32_edit, update_int32_stamp,
};
use crate::heap::{UpdateInt32PairTid, UpdateOptions};
use crate::wal_sink::{WalSink, test_support::InMemoryWalSink};

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
fn vacuum_heap_keeps_classic_update_redirect_chain() {
    let heap = make_heap(16);
    let r = rel();
    let old_tid = heap.insert(r, b"before", opts(10)).unwrap();
    let outcome = heap
        .update(
            old_tid,
            b"after",
            UpdateOptions {
                xid: Xid::new(20),
                command_id: CommandId::FIRST,
                hot_eligible: true,
                wal: None,
                vm: None,
            },
        )
        .unwrap();

    let oracle = MapOracle::new();
    oracle.set_committed(Xid::new(10));
    oracle.set_committed(Xid::new(20));
    let stats = heap.vacuum_heap(r, Xid::new(100), &oracle).unwrap();

    assert_eq!(stats.tuples_reclaimed, 0);
    assert_eq!(heap.fetch(old_tid).unwrap().header.ctid, outcome.new_tid);
    assert_eq!(heap.fetch(outcome.new_tid).unwrap().data, b"after");
}

#[test]
fn wal_backed_vacuum_defers_physical_reclamation() {
    let sink = Arc::new(InMemoryWalSink::new());
    let pool = Arc::new(BufferPool::with_wal(
        16,
        MapLoader::new(),
        Arc::clone(&sink) as Arc<dyn WalSink>,
    ));
    let heap = crate::heap::HeapAccess::new(pool);
    let r = rel();
    let tid = heap.insert(r, b"dead", opts(10)).unwrap();
    heap.delete(tid, del_opts(20, 0)).unwrap();

    let oracle = MapOracle::new();
    oracle.set_committed(Xid::new(10));
    oracle.set_committed(Xid::new(20));
    let stats = heap.vacuum_heap(r, Xid::new(100), &oracle).unwrap();

    assert_eq!(stats.tuples_reclaimed, 0);
    assert_eq!(stats.pages_compacted, 0);
    assert_eq!(heap.fetch(tid).unwrap().data, b"dead");
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

#[test]
fn vacuum_heap_keeps_row_visible_to_snapshot_that_predates_deleters_commit() {
    use crate::heap::HeapTuple;

    let heap = make_heap(16);
    let r = rel();

    // Insert under XID 10, committed long ago.
    let t = heap.insert(r, b"row", opts(10)).unwrap();

    let oracle = MapOracle::new();
    oracle.set_committed(Xid::new(10));

    // Deleter D (XID 90) stamps the row but has NOT committed yet.
    oracle.set_in_progress(Xid::new(90));
    heap.delete(t, del_opts(90, 0)).unwrap();

    // Reader R (XID 100) begins while D is still in progress: R's
    // snapshot lists 90 as active, so D's delete is invisible to R and
    // the row must stay visible to R for R's entire lifetime.
    oracle.set_in_progress(Xid::new(100));
    let reader_100 = Snapshot::new(
        Xid::new(90),
        Xid::new(101),
        Xid::new(100),
        CommandId::FIRST,
        [Xid::new(90)],
    );
    let before: Vec<HeapTuple> = heap
        .scan_visible(r, heap.block_count(r), &reader_100, &oracle)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        before.len(),
        1,
        "setup: R's snapshot must see the row while D is still in progress"
    );

    // D commits. The oldest transaction still in progress is now R
    // (XID 100), but R's live snapshot still treats 90 as in-progress —
    // so the horizon VACUUM receives must be R's snapshot xmin (90), not
    // R's own XID. `TransactionManager::vacuum_horizon()` computes exactly
    // that min-over-live-snapshot-xmins floor (see its unit tests); the
    // server's VACUUM path (session/execute/maintenance.rs) passes it
    // here. With the correct horizon the slot must survive.
    oracle.set_committed(Xid::new(90));
    let stats = heap.vacuum_heap(r, Xid::new(90), &oracle).unwrap();
    assert_eq!(
        stats.tuples_reclaimed, 0,
        "a horizon at the live snapshot's xmin must protect the slot"
    );

    // R's ORIGINAL snapshot must still see the row after VACUUM.
    let after: Vec<HeapTuple> = heap
        .scan_visible(r, heap.block_count(r), &reader_100, &oracle)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        after.len(),
        1,
        "VACUUM ({stats:?}) must not reclaim a row that reader XID 100's live \
         snapshot (taken before deleter XID 90 committed) is entitled to see"
    );
    assert_eq!(after[0].data, b"row");

    // Once no live snapshot needs the pre-image (reader gone, horizon
    // past both XIDs), the same slot is reclaimable — vacuum still works.
    let stats = heap.vacuum_heap(r, Xid::new(101), &oracle).unwrap();
    assert_eq!(
        stats.tuples_reclaimed, 1,
        "with no live snapshot at or below the deleter, the slot must be reclaimed"
    );
}
