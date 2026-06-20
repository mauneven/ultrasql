//! Deliverable B: visibility-aware scans, the VM-backed visible walker,
//! and `vacuum_mark_all_visible` certification.

use proptest::prelude::*;
use ultrasql_core::{CommandId, Xid};
use ultrasql_mvcc::status::test_support::MapOracle;
use ultrasql_mvcc::Snapshot;

use super::*;

#[test]
fn visibility_scan_filters_aborted_inserts() {
    let heap = make_heap(16);
    let committed_tid = heap.insert(rel(), b"committed", opts(10)).unwrap();
    let _aborted_tid = heap.insert(rel(), b"aborted", opts(20)).unwrap();

    let oracle = MapOracle::new();
    oracle.set_committed(Xid::new(10));
    oracle.set_aborted(Xid::new(20));

    let snap = committed_snap(999);
    let blocks = heap.block_count(rel());
    let visible: Vec<HeapTuple> = heap
        .scan_visible(rel(), blocks, &snap, &oracle)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0].tid, committed_tid);
    assert_eq!(visible[0].data, b"committed");
}

#[test]
fn visibility_scan_filters_uncommitted_other_txn_inserts() {
    let heap = make_heap(16);
    let _in_progress_tid = heap.insert(rel(), b"in-progress", opts(300)).unwrap();

    let oracle = MapOracle::new();
    oracle.set_in_progress(Xid::new(300));

    // Snapshot taken with 300 in-progress: xmin=50, xmax=500,
    // current_xid=999 (different from 300).
    let snap = Snapshot::new(
        Xid::new(50),
        Xid::new(500),
        Xid::new(999),
        CommandId::FIRST,
        [Xid::new(300)],
    );

    let blocks = heap.block_count(rel());
    let visible: Vec<HeapTuple> = heap
        .scan_visible(rel(), blocks, &snap, &oracle)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert!(
        visible.is_empty(),
        "in-progress insert from another txn must be invisible"
    );
}

#[test]
fn visibility_scan_includes_own_uncommitted_writes() {
    let heap = make_heap(16);
    // Insert with the same xid that will be the snapshot's
    // current_xid, at command_id 0.
    let own_tid = heap
        .insert(
            rel(),
            b"own-write",
            InsertOptions {
                xmin: Xid::new(42),
                command_id: CommandId::FIRST,
                n_atts: 0,
                fsm: None,
                vm: None,
                wal: None,
            },
        )
        .unwrap();

    let oracle = MapOracle::new();
    oracle.set_in_progress(Xid::new(42));

    // Snapshot at command 1: own write at command 0 is visible.
    let snap = Snapshot::new(
        Xid::new(10),
        Xid::new(100),
        Xid::new(42),
        CommandId::new(1), // later than cmin=0
        std::iter::empty(),
    );

    let blocks = heap.block_count(rel());
    let visible: Vec<HeapTuple> = heap
        .scan_visible(rel(), blocks, &snap, &oracle)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0].tid, own_tid);
}

#[test]
fn visible_walker_vm_all_visible_skips_oracle_status() {
    let heap = make_heap(16);
    let first_tid = heap.insert(rel(), b"first", opts(100)).unwrap();
    let second_tid = heap.insert(rel(), b"second", opts(100)).unwrap();

    let vm = crate::vm::VisibilityMap::new();
    heap.vacuum_set_all_visible(rel(), first_tid.page.block, &vm);

    let oracle = CountingOracle::new();
    oracle.set_committed(Xid::new(100));
    let snap = committed_snap(999);
    let blocks = heap.block_count(rel());
    let mut walker = heap.scan_visible_walker_with_vm(rel(), blocks, &snap, &oracle, &vm);

    let mut got = Vec::new();
    while let Some((tid, _header, payload)) = walker.try_next().unwrap() {
        got.push((tid, payload.to_vec()));
    }

    assert_eq!(
        got,
        vec![
            (first_tid, b"first".to_vec()),
            (second_tid, b"second".to_vec())
        ]
    );
    assert_eq!(oracle.calls(), 0);
}

#[test]
fn visible_walker_vm_clear_after_delete_restores_visibility_checks() {
    let heap = make_heap(16);
    let tid = heap.insert(rel(), b"gone", opts(100)).unwrap();

    let vm = crate::vm::VisibilityMap::new();
    heap.vacuum_set_all_visible(rel(), tid.page.block, &vm);
    assert!(vm.is_all_visible(rel(), tid.page.block));

    heap.delete(
        tid,
        DeleteOptions {
            xmax: Xid::new(200),
            cmax: CommandId::FIRST,
            wal: None,
            fsm: None,
            vm: Some(&vm),
        },
    )
    .unwrap();

    assert!(!vm.is_all_visible(rel(), tid.page.block));

    let oracle = CountingOracle::new();
    oracle.set_committed(Xid::new(100));
    oracle.set_committed(Xid::new(200));
    let snap = committed_snap(999);
    let blocks = heap.block_count(rel());
    let mut walker = heap.scan_visible_walker_with_vm(rel(), blocks, &snap, &oracle, &vm);

    assert!(walker.try_next().unwrap().is_none());
    assert!(oracle.calls() > 0);
}

#[test]
fn vacuum_mark_all_visible_certifies_only_old_committed_pages() {
    let heap = make_heap(16);
    let committed_tid = heap.insert(rel(), b"committed", opts(100)).unwrap();
    let _young_tid = heap.insert(rel(), b"young", opts(300)).unwrap();

    let vm = crate::vm::VisibilityMap::new();
    let oracle = MapOracle::new();
    oracle.set_committed(Xid::new(100));
    oracle.set_committed(Xid::new(300));

    let marked = heap
        .vacuum_mark_all_visible(rel(), heap.block_count(rel()), Xid::new(200), &oracle, &vm)
        .unwrap();

    assert_eq!(marked, 0);
    assert!(!vm.is_all_visible(rel(), committed_tid.page.block));

    let marked = heap
        .vacuum_mark_all_visible(rel(), heap.block_count(rel()), Xid::new(400), &oracle, &vm)
        .unwrap();

    assert_eq!(marked, 1);
    assert!(vm.is_all_visible(rel(), committed_tid.page.block));
}

// Property test: for any set of inserts + random deletes, the
// visibility-aware scan returns exactly the non-deleted tuples when
// all xids are committed.
proptest! {
    #[test]
    fn prop_visible_scan_matches_non_deleted(
        payloads in proptest::collection::vec(proptest::collection::vec(0u8..=255, 1..=100), 1..=30),
        delete_mask in proptest::collection::vec(proptest::bool::ANY, 1..=30),
    ) {
        let heap = make_heap(256);
        let insert_xid = Xid::new(1);

        let oracle = MapOracle::new();
        oracle.set_committed(insert_xid);

        let mut tids = Vec::new();
        for p in &payloads {
            let tid = heap
                .insert(rel(), p, InsertOptions {
                    xmin: insert_xid,
                    command_id: CommandId::FIRST,
                    n_atts: 0,
                    fsm: None,
                    vm: None,
                    wal: None,
                })
                .unwrap();
            tids.push(tid);
        }

        let mut expected_count: usize = 0;
        let delete_xid = Xid::new(2);
        oracle.set_committed(delete_xid);

        for (i, &should_delete) in delete_mask.iter().enumerate() {
            if i >= tids.len() {
                break;
            }
            if should_delete {
                heap.delete(
                    tids[i],
                    DeleteOptions {
                        xmax: delete_xid,
                        cmax: CommandId::FIRST,
                        fsm: None,
                        vm: None,
                        wal: None,
                    },
                )
                .unwrap();
            } else {
                expected_count += 1;
            }
        }
        // Tuples beyond the delete_mask length are never deleted.
        expected_count += tids.len().saturating_sub(delete_mask.len());

        let snap = Snapshot::new(
            Xid::new(0),
            Xid::new(100),
            Xid::new(999),
            CommandId::FIRST,
            std::iter::empty(),
        );

        let blocks = heap.block_count(rel());
        let visible: Vec<HeapTuple> = heap
            .scan_visible(rel(), blocks, &snap, &oracle)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        prop_assert_eq!(
            visible.len(),
            expected_count,
            "scan_visible returned {} tuples, expected {}",
            visible.len(),
            expected_count
        );
    }
}
