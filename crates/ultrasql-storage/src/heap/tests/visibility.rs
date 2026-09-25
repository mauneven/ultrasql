//! Deliverable B: visibility-aware scans, the VM-backed visible walker,
//! and `vacuum_mark_all_visible` certification.

use std::sync::{Arc, Barrier};

use proptest::prelude::*;
use ultrasql_core::{CommandId, Lsn, Xid};
use ultrasql_mvcc::Snapshot;
use ultrasql_mvcc::status::test_support::MapOracle;
use ultrasql_mvcc::tuple_header::TUPLE_HEADER_SIZE;
use ultrasql_wal::WalRecord;

use super::*;
use crate::wal_sink::{WalSink, WalSinkError};

#[derive(Debug)]
struct BlockingWalSink {
    append_entered: Barrier,
    release_append: Barrier,
}

impl BlockingWalSink {
    fn new() -> Self {
        Self {
            append_entered: Barrier::new(2),
            release_append: Barrier::new(2),
        }
    }
}

impl WalSink for BlockingWalSink {
    fn append(&self, _record: WalRecord) -> Result<Lsn, WalSinkError> {
        self.append_entered.wait();
        self.release_append.wait();
        Ok(Lsn::new(1))
    }

    fn durable_lsn(&self) -> Lsn {
        Lsn::new(2)
    }

    fn last_lsn_for(&self, _xid: Xid) -> Lsn {
        Lsn::ZERO
    }
}

fn assert_vm_cleared_before_post_page_wal_append(bulk: bool) {
    let heap = Arc::new(make_heap(16));
    let tid = heap.insert(rel(), b"gone", opts(100)).unwrap();
    let vm = Arc::new(crate::vm::VisibilityMap::new());
    heap.vacuum_set_all_visible(rel(), tid.page.block, &vm);
    let sink = Arc::new(BlockingWalSink::new());

    let worker_heap = Arc::clone(&heap);
    let worker_vm = Arc::clone(&vm);
    let worker_sink = Arc::clone(&sink);
    let worker = std::thread::spawn(move || {
        let delete_opts = DeleteOptions {
            xmax: Xid::new(200),
            cmax: CommandId::FIRST,
            wal: Some(worker_sink.as_ref()),
            fsm: None,
            vm: Some(worker_vm.as_ref()),
        };
        if bulk {
            worker_heap.delete_many([tid], delete_opts).map(|_| ())
        } else {
            worker_heap.delete(tid, delete_opts)
        }
    });

    // Classic DELETE appends WAL after releasing the page latch. Pausing the
    // append exposes the exact old race window deterministically: a coherent
    // reader can inspect the post-delete header while WAL is blocked.
    sink.append_entered.wait();
    let mut flushed_while_append_blocked = false;
    let flushed = heap
        .pool
        .try_flush_dirty(|_, _| {
            flushed_while_append_blocked = true;
            Ok(())
        })
        .unwrap();
    assert_eq!(flushed, 0, "the WAL-pending page must remain pinned");
    assert!(
        !flushed_while_append_blocked,
        "checkpointer must not write post-delete bytes before their LSN stamp"
    );
    let (observed_xmax, observed_all_visible) = {
        let guard = heap.pool.get_page(tid.page).unwrap();
        let page = guard.read();
        let tuple = page.read_tuple(tid.slot).unwrap();
        let (header, _) = TupleHeader::decode(&tuple[..TUPLE_HEADER_SIZE]).unwrap();
        let all_visible = vm.is_all_visible(rel(), tid.page.block);
        (header.xmax, all_visible)
    };
    sink.release_append.wait();
    worker.join().unwrap().unwrap();

    assert_eq!(observed_xmax, Xid::new(200));
    assert!(
        !observed_all_visible,
        "post-delete bytes must never coexist with stale all-visible state"
    );
}

fn assert_insert_vm_cleared_before_post_page_wal_append(bulk: bool) {
    let heap = Arc::new(make_heap(16));
    let seed = heap.insert(rel(), b"seed", opts(100)).unwrap();
    let vm = Arc::new(crate::vm::VisibilityMap::new());
    heap.vacuum_set_all_visible(rel(), seed.page.block, &vm);
    let sink = Arc::new(BlockingWalSink::new());

    let worker_heap = Arc::clone(&heap);
    let worker_vm = Arc::clone(&vm);
    let worker_sink = Arc::clone(&sink);
    let worker = std::thread::spawn(move || {
        let insert_opts = InsertOptions {
            xmin: Xid::new(200),
            command_id: CommandId::FIRST,
            n_atts: 0,
            wal: Some(worker_sink.as_ref()),
            fsm: None,
            vm: Some(worker_vm.as_ref()),
        };
        if bulk {
            worker_heap
                .insert_batch(rel(), &[b"new".as_slice()], insert_opts)
                .map(|_| ())
        } else {
            worker_heap.insert(rel(), b"new", insert_opts).map(|_| ())
        }
    });

    // INSERT also appends WAL after releasing its page latch. While that
    // append is paused, the newly allocated slot is already visible in the
    // page image, so VM must already have been cleared under the earlier latch.
    sink.append_entered.wait();
    let mut flushed_while_append_blocked = false;
    let flushed = heap
        .pool
        .try_flush_dirty(|_, _| {
            flushed_while_append_blocked = true;
            Ok(())
        })
        .unwrap();
    assert_eq!(flushed, 0, "the WAL-pending page must remain pinned");
    assert!(
        !flushed_while_append_blocked,
        "checkpointer must not write post-insert bytes before their LSN stamp"
    );
    let (observed_slot_count, observed_all_visible) = {
        let guard = heap.pool.get_page(seed.page).unwrap();
        let page = guard.read();
        let all_visible = vm.is_all_visible(rel(), seed.page.block);
        (page.header().slot_count(), all_visible)
    };
    sink.release_append.wait();
    worker.join().unwrap().unwrap();

    assert_eq!(observed_slot_count, 2);
    assert!(
        !observed_all_visible,
        "post-insert bytes must never coexist with stale all-visible state"
    );
}

fn assert_update_vm_cleared_before_post_page_wal_append() {
    let heap = Arc::new(make_heap(16));
    let old_tid = heap.insert(rel(), b"old", opts(100)).unwrap();
    let vm = Arc::new(crate::vm::VisibilityMap::new());
    heap.vacuum_set_all_visible(rel(), old_tid.page.block, &vm);
    let sink = Arc::new(BlockingWalSink::new());

    let worker_heap = Arc::clone(&heap);
    let worker_vm = Arc::clone(&vm);
    let worker_sink = Arc::clone(&sink);
    let worker = std::thread::spawn(move || {
        worker_heap.update(
            old_tid,
            b"new",
            UpdateOptions {
                xid: Xid::new(200),
                command_id: CommandId::FIRST,
                hot_eligible: true,
                wal: Some(worker_sink.as_ref()),
                vm: Some(worker_vm.as_ref()),
            },
        )
    });

    sink.append_entered.wait();
    let mut flushed_while_append_blocked = false;
    let flushed = heap
        .pool
        .try_flush_dirty(|_, _| {
            flushed_while_append_blocked = true;
            Ok(())
        })
        .unwrap();
    assert_eq!(flushed, 0, "the WAL-pending page must remain pinned");
    assert!(!flushed_while_append_blocked);
    assert!(
        !vm.is_all_visible(rel(), old_tid.page.block),
        "HOT update must clear VM before releasing its mutation latch"
    );
    let old_header = heap.fetch(old_tid).unwrap().header;
    assert_eq!(old_header.xmax, Xid::new(200));

    sink.release_append.wait();
    let outcome = worker.join().unwrap().unwrap();
    assert!(outcome.hot);
}

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
fn delete_clears_vm_before_releasing_page_latch() {
    assert_vm_cleared_before_post_page_wal_append(false);
}

#[test]
fn delete_many_clears_vm_before_releasing_page_latch() {
    assert_vm_cleared_before_post_page_wal_append(true);
}

#[test]
fn insert_clears_vm_before_releasing_page_latch() {
    assert_insert_vm_cleared_before_post_page_wal_append(false);
}

#[test]
fn insert_batch_clears_vm_before_releasing_page_latch() {
    assert_insert_vm_cleared_before_post_page_wal_append(true);
}

#[test]
fn update_clears_vm_and_keeps_page_unflushable_during_wal_append() {
    assert_update_vm_cleared_before_post_page_wal_append();
}

#[test]
fn page_lsn_stamp_is_monotonic_when_appenders_finish_out_of_order() {
    let heap = make_heap(4);
    let tid = heap.insert(rel(), b"row", opts(100)).unwrap();
    let guard = heap.pool.get_page(tid.page).unwrap();

    HeapAccess::stamp_pinned_page_lsn(&guard, Lsn::new(20));
    HeapAccess::stamp_pinned_page_lsn(&guard, Lsn::new(10));

    assert_eq!(guard.read().header().lsn, 20);
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

#[test]
fn update_many_non_hot_fallback_clears_destination_page_vm() {
    let heap = make_heap(16);
    let payload = [3_u8; 1000];
    let mut tids = Vec::new();
    while heap.block_count(rel()) < 2 {
        tids.push(heap.insert(rel(), &payload, opts(100)).unwrap());
    }
    let old_tid = tids[0];
    assert_eq!(old_tid.page.block, BlockNumber::new(0));

    let vm = crate::vm::VisibilityMap::new();
    let oracle = MapOracle::new();
    oracle.set_committed(Xid::new(100));
    let marked = heap
        .vacuum_mark_all_visible(rel(), heap.block_count(rel()), Xid::new(200), &oracle, &vm)
        .unwrap();
    assert_eq!(marked, 2);

    let new_payload: UpdatePayload = payload.iter().copied().collect();
    let outcomes = heap
        .update_many_with_outcomes(
            [(old_tid, new_payload)],
            UpdateOptions {
                xid: Xid::new(300),
                command_id: CommandId::FIRST,
                hot_eligible: false,
                wal: None,
                vm: Some(&vm),
            },
        )
        .unwrap();
    assert_eq!(outcomes.len(), 1);
    let new_tid = outcomes[0].new_tid;
    assert_ne!(new_tid.page, old_tid.page);
    assert!(!vm.is_all_visible(rel(), old_tid.page.block));
    assert!(
        !vm.is_all_visible(rel(), new_tid.page.block),
        "the page that received the uncommitted new version must lose its all-visible bit"
    );
}

#[test]
fn vacuum_marks_old_committed_in_place_update_all_visible() {
    let heap = make_heap(16);
    let tid = heap
        .insert(rel(), &int32_pair_payload(1, 10), opts(10))
        .unwrap();
    let oracle = MapOracle::new();
    oracle.set_committed(Xid::new(10));
    let writer = Snapshot::new(
        Xid::new(10),
        Xid::new(100),
        Xid::new(20),
        CommandId::FIRST,
        std::iter::empty(),
    );
    assert_eq!(
        heap.update_int32_pair_tid_inplace_undo(
            UpdateInt32PairTid {
                tid,
                snapshot: &writer,
                oracle: &oracle,
                predicate: |id, _| id == 1,
            },
            update_int32_edit(1, 5),
            update_int32_stamp(20),
            None,
            None,
        )
        .unwrap(),
        1
    );
    oracle.set_committed(Xid::new(20));

    let vm = crate::vm::VisibilityMap::new();
    let marked = heap
        .vacuum_mark_all_visible(rel(), heap.block_count(rel()), Xid::new(100), &oracle, &vm)
        .unwrap();

    assert_eq!(marked, 1);
    assert!(vm.is_all_visible(rel(), tid.page.block));
}

#[test]
fn vacuum_all_visible_publication_holds_page_latch_against_writer() {
    let heap = std::sync::Arc::new(make_heap(16));
    let tid = heap.insert(rel(), b"committed", opts(100)).unwrap();
    let vm = std::sync::Arc::new(crate::vm::VisibilityMap::new());
    let hook = std::sync::Arc::new(crate::vm::MarkAllVisibleHook::new());
    vm.install_mark_all_visible_hook(std::sync::Arc::clone(&hook));

    let oracle = std::sync::Arc::new(MapOracle::new());
    oracle.set_committed(Xid::new(100));
    let vacuum_heap = std::sync::Arc::clone(&heap);
    let vacuum_vm = std::sync::Arc::clone(&vm);
    let vacuum_oracle = std::sync::Arc::clone(&oracle);
    let vacuum = std::thread::spawn(move || {
        vacuum_heap.vacuum_mark_all_visible(
            rel(),
            vacuum_heap.block_count(rel()),
            Xid::new(200),
            vacuum_oracle.as_ref(),
            vacuum_vm.as_ref(),
        )
    });

    // Vacuum has certified the tuple and is paused immediately before the VM
    // bit write. The page read latch must still be held at this point.
    hook.wait_until_mark_attempt();
    let (writer_ready_tx, writer_ready_rx) = std::sync::mpsc::channel();
    let (writer_done_tx, writer_done_rx) = std::sync::mpsc::channel();
    let writer_heap = std::sync::Arc::clone(&heap);
    let writer_vm = std::sync::Arc::clone(&vm);
    let writer = std::thread::spawn(move || {
        let guard = writer_heap.get_page_relieved(tid.page).unwrap();
        writer_ready_tx.send(()).unwrap();
        let result = HeapAccess::<MapLoader>::delete_in_place(
            &guard,
            tid,
            Xid::new(300),
            CommandId::FIRST,
            Some(writer_vm.as_ref()),
        );
        writer_done_tx.send(()).unwrap();
        result
    });
    writer_ready_rx.recv().unwrap();

    // With the certification latch intact, the writer cannot acquire its
    // exclusive latch and clear/mutate before vacuum publishes the bit.
    let writer_finished_before_mark = writer_done_rx
        .recv_timeout(std::time::Duration::from_millis(100))
        .is_ok();
    hook.release_mark();

    assert_eq!(vacuum.join().unwrap().unwrap(), 1);
    writer.join().unwrap().unwrap();
    assert!(
        !writer_finished_before_mark,
        "writer acquired the page between certification and VM publication"
    );
    assert!(
        !vm.is_all_visible(rel(), tid.page.block),
        "the later writer must clear vacuum's earlier all-visible mark"
    );
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
