//! Deliverable A: HOT-chain updates and in-place int32-pair update tests.

use ultrasql_core::{CommandId, Xid};
use ultrasql_mvcc::Snapshot;
use ultrasql_mvcc::status::test_support::MapOracle;
use ultrasql_mvcc::tuple_header::InfoMask;

use super::*;

fn fill_parallel_update_pages(
    heap: &HeapAccess<MapLoader>,
    populated_pages: u32,
    special_page: u32,
    special_value: i32,
) -> (Vec<(TupleId, i32, i32)>, i32) {
    let mut rows = Vec::new();
    let mut id = 0_i32;
    while heap.block_count(rel()) < special_page.saturating_add(1) {
        let value = id * 10;
        let tid = heap
            .insert(rel(), &int32_pair_payload(id, value), opts(10))
            .unwrap();
        rows.push((tid, id, value));
        id += 1;
    }
    let special_id = id;
    let special_tid = heap
        .insert(
            rel(),
            &int32_pair_payload(special_id, special_value),
            opts(10),
        )
        .unwrap();
    assert!(
        special_tid.page.block.raw() >= special_page,
        "special row must land on or after the requested page"
    );
    rows.push((special_tid, special_id, special_value));
    id += 1;
    while heap.block_count(rel()) < populated_pages {
        let value = id * 10;
        let tid = heap
            .insert(rel(), &int32_pair_payload(id, value), opts(10))
            .unwrap();
        rows.push((tid, id, value));
        id += 1;
    }
    (rows, special_id)
}

#[derive(Debug)]
struct BlockingOracle {
    inner: MapOracle,
    target: Xid,
    entered: std::sync::Arc<std::sync::Barrier>,
    release: std::sync::Arc<std::sync::Barrier>,
    blocked: std::sync::atomic::AtomicBool,
}

impl BlockingOracle {
    fn new(
        target: Xid,
        entered: std::sync::Arc<std::sync::Barrier>,
        release: std::sync::Arc<std::sync::Barrier>,
    ) -> Self {
        Self {
            inner: MapOracle::new(),
            target,
            entered,
            release,
            blocked: std::sync::atomic::AtomicBool::new(false),
        }
    }
}

impl XidStatusOracle for BlockingOracle {
    fn status(&self, xid: Xid) -> XidStatus {
        if xid == self.target && !self.blocked.swap(true, std::sync::atomic::Ordering::AcqRel) {
            self.entered.wait();
            self.release.wait();
        }
        self.inner.status(xid)
    }
}

fn heap_with_in_progress_int32_pair_update() -> (HeapAccess<MapLoader>, MapOracle, TupleId) {
    let heap = make_heap(2_048);
    let tid = heap
        .insert(rel(), &int32_pair_payload(1, 10), opts(10))
        .unwrap();
    let oracle = MapOracle::new();
    oracle.set_committed(Xid::new(10));
    oracle.set_in_progress(Xid::new(20));
    let writer = Snapshot::new(
        Xid::new(10),
        Xid::new(100),
        Xid::new(20),
        CommandId::FIRST,
        std::iter::empty(),
    );
    assert_eq!(
        heap.update_int32_pair_inplace_undo(
            update_int32_scan(
                rel(),
                heap.block_count(rel()),
                &writer,
                &oracle,
                |_id, _val| true,
            ),
            update_int32_edit(1, 10),
            update_int32_stamp(20),
            None,
            None,
        )
        .unwrap(),
        1
    );
    assert_eq!(
        int32_pair_from_payload(&heap.fetch(tid).unwrap().data),
        (1, 20)
    );
    (heap, oracle, tid)
}

#[test]
fn update_creates_hot_chain_when_eligible_and_room() {
    let heap = make_heap(16);

    // Insert a small tuple that leaves plenty of room on the page.
    let tid = heap.insert(rel(), b"original", opts(100)).unwrap();

    let uo = update_opts(200);
    let outcome = heap.update(tid, b"updated-payload", uo).unwrap();

    assert!(outcome.hot, "expected HOT update when page has room");
    assert_eq!(outcome.old_tid, tid);
    // Both tids must live on the same page (same block).
    assert_eq!(
        outcome.old_tid.page.block, outcome.new_tid.page.block,
        "HOT: old and new must be on the same block"
    );

    // Old version: xmax stamped, ctid redirects to new.
    let old = heap.fetch(tid).unwrap();
    assert_eq!(old.header.xmax, Xid::new(200));
    assert_eq!(old.header.ctid, outcome.new_tid);
    assert!(
        old.header.infomask.contains(InfoMask::HOT_UPDATED),
        "old tuple must have HOT_UPDATED bit set"
    );

    // New version: xmin set, ctid self-referential (terminal).
    let new_tup = heap.fetch(outcome.new_tid).unwrap();
    assert_eq!(new_tup.header.xmin, Xid::new(200));
    assert_eq!(new_tup.header.ctid, outcome.new_tid);
    assert!(
        new_tup.header.infomask.contains(InfoMask::HOT_UPDATED),
        "new tuple must have HOT_UPDATED bit set"
    );
    assert_eq!(new_tup.data, b"updated-payload");
}

#[test]
fn update_falls_back_to_non_hot_when_page_full() {
    let heap = make_heap(32);
    // Fill the page with big tuples so there is < (header + 1 byte) left.
    // 7000 bytes per tuple: fits once with room for header but not for a
    // second same-size write.
    let big = [0xAA_u8; 7000];
    let tid = heap.insert(rel(), &big, opts(100)).unwrap();
    // Insert another large tuple; this should spill to block 1.
    let _ = heap.insert(rel(), &big, opts(100)).unwrap();

    // Now update the first tuple on block 0.  The page is too full for
    // another 7000-byte tuple in-place.
    let uo = UpdateOptions {
        xid: Xid::new(200),
        command_id: CommandId::FIRST,
        hot_eligible: true, // we ask for HOT but the page is full
        wal: None,
        vm: None,
    };
    let outcome = heap.update(tid, &big, uo).unwrap();
    assert!(!outcome.hot, "expected non-HOT when page is full");

    // New version lands on a different block.
    assert_ne!(
        outcome.old_tid.page.block, outcome.new_tid.page.block,
        "non-HOT: old and new must be on different blocks"
    );

    // Old tuple has xmax stamped.
    let old = heap.fetch(tid).unwrap();
    assert_eq!(old.header.xmax, Xid::new(200));
}

#[test]
fn update_rejected_on_already_deleted_tuple() {
    let heap = make_heap(8);
    let tid = heap.insert(rel(), b"to-delete", opts(100)).unwrap();
    heap.delete(
        tid,
        DeleteOptions {
            xmax: Xid::new(150),
            cmax: CommandId::FIRST,
            fsm: None,
            vm: None,
            wal: None,
        },
    )
    .unwrap();

    let uo = update_opts(200);
    let err = heap.update(tid, b"should-fail", uo).unwrap_err();
    assert!(
        matches!(err, HeapError::MalformedHeader(_)),
        "expected MalformedHeader on update of deleted tuple, got {err:?}"
    );
}

#[test]
fn inplace_int32_update_conflicts_with_in_progress_writer() {
    let heap = make_heap(8);
    let _tid = heap
        .insert(rel(), &int32_pair_payload(1, 10), opts(10))
        .unwrap();

    let oracle = MapOracle::new();
    oracle.set_committed(Xid::new(10));
    oracle.set_in_progress(Xid::new(20));

    let writer_20 = Snapshot::new(
        Xid::new(10),
        Xid::new(100),
        Xid::new(20),
        CommandId::FIRST,
        std::iter::empty(),
    );
    let updated = heap
        .update_int32_pair_inplace_undo(
            update_int32_scan(
                rel(),
                heap.block_count(rel()),
                &writer_20,
                &oracle,
                |id, _val| id == 1,
            ),
            update_int32_edit(1, 5),
            update_int32_stamp(20),
            None,
            None,
        )
        .unwrap();
    assert_eq!(updated, 1);

    let writer_30 = Snapshot::new(
        Xid::new(10),
        Xid::new(100),
        Xid::new(30),
        CommandId::FIRST,
        [Xid::new(20)],
    );
    let err = heap
        .update_int32_pair_inplace_undo(
            update_int32_scan(
                rel(),
                heap.block_count(rel()),
                &writer_30,
                &oracle,
                |id, _val| id == 1,
            ),
            update_int32_edit(1, 7),
            update_int32_stamp(30),
            None,
            None,
        )
        .unwrap_err();

    assert!(
        matches!(err, HeapError::WriteConflict(_)),
        "expected write conflict for invisible in-place writer, got {err:?}"
    );
}

#[test]
fn inplace_int32_update_skips_unrelated_in_progress_writer() {
    let heap = make_heap(8);
    let _first = heap
        .insert(rel(), &int32_pair_payload(1, 10), opts(10))
        .unwrap();
    let _second = heap
        .insert(rel(), &int32_pair_payload(2, 20), opts(10))
        .unwrap();

    let oracle = MapOracle::new();
    oracle.set_committed(Xid::new(10));
    oracle.set_in_progress(Xid::new(20));

    let writer_20 = Snapshot::new(
        Xid::new(10),
        Xid::new(100),
        Xid::new(20),
        CommandId::FIRST,
        std::iter::empty(),
    );
    heap.update_int32_pair_inplace_undo(
        update_int32_scan(
            rel(),
            heap.block_count(rel()),
            &writer_20,
            &oracle,
            |id, _val| id == 1,
        ),
        update_int32_edit(1, 5),
        update_int32_stamp(20),
        None,
        None,
    )
    .unwrap();

    let writer_30 = Snapshot::new(
        Xid::new(10),
        Xid::new(100),
        Xid::new(30),
        CommandId::FIRST,
        [Xid::new(20)],
    );
    let updated = heap
        .update_int32_pair_inplace_undo(
            update_int32_scan(
                rel(),
                heap.block_count(rel()),
                &writer_30,
                &oracle,
                |id, _val| id == 2,
            ),
            update_int32_edit(1, 7),
            update_int32_stamp(30),
            None,
            None,
        )
        .unwrap();

    assert_eq!(updated, 1);
}

#[test]
fn inplace_int32_update_records_compact_undo_batch() {
    let heap = make_heap(8);
    for id in 0_i32..4 {
        heap.insert(rel(), &int32_pair_payload(id, id * 10), opts(10))
            .unwrap();
    }

    let oracle = MapOracle::new();
    oracle.set_committed(Xid::new(10));
    oracle.set_in_progress(Xid::new(20));
    let writer_20 = Snapshot::new(
        Xid::new(10),
        Xid::new(100),
        Xid::new(20),
        CommandId::FIRST,
        std::iter::empty(),
    );

    let updated = heap
        .update_int32_pair_inplace_undo(
            update_int32_scan(
                rel(),
                heap.block_count(rel()),
                &writer_20,
                &oracle,
                |_id, _val| true,
            ),
            update_int32_edit(1, 5),
            update_int32_stamp(20),
            None,
            None,
        )
        .unwrap();

    assert_eq!(updated, 4);
    assert_eq!(
        heap.undo_log_len(rel()),
        0,
        "bulk int32 updates must not allocate one full undo entry per row",
    );
    assert_eq!(heap.int32_pair_undo_batch_len(rel()), 1);
    let log = heap.undo_log.get(&rel()).unwrap();
    let log = log.read();
    let batch = &log.int32_pair_batches[0];
    assert_eq!(batch.first_slot, 0);
    assert_eq!(usize::from(batch.slot_count), 4);
    assert!(
        batch.slots.is_empty(),
        "contiguous slot updates must not allocate a slot list"
    );
}

#[test]
fn invisible_inplace_int32_update_reads_compact_preimage() {
    let heap = make_heap(8);
    heap.insert(rel(), &int32_pair_payload(1, 10), opts(10))
        .unwrap();

    let oracle = MapOracle::new();
    oracle.set_committed(Xid::new(10));
    oracle.set_in_progress(Xid::new(20));
    let writer_20 = Snapshot::new(
        Xid::new(10),
        Xid::new(100),
        Xid::new(20),
        CommandId::FIRST,
        std::iter::empty(),
    );
    heap.update_int32_pair_inplace_undo(
        update_int32_scan(
            rel(),
            heap.block_count(rel()),
            &writer_20,
            &oracle,
            |id, _val| id == 1,
        ),
        update_int32_edit(1, 5),
        update_int32_stamp(20),
        None,
        None,
    )
    .unwrap();

    let reader = Snapshot::new(
        Xid::new(10),
        Xid::new(100),
        Xid::new(30),
        CommandId::FIRST,
        [Xid::new(20)],
    );
    let visible: Vec<HeapTuple> = heap
        .scan_visible(rel(), heap.block_count(rel()), &reader, &oracle)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(visible.len(), 1);
    assert_eq!(int32_pair_from_payload(&visible[0].data), (1, 10));
}

#[test]
fn walker_page_undo_snapshot_ignores_later_publications() {
    let heap = std::sync::Arc::new(make_heap(8));
    let tid = heap
        .insert(rel(), &int32_pair_payload(1, 10), opts(10))
        .unwrap();
    let update_oracle = MapOracle::new();
    update_oracle.set_committed(Xid::new(10));
    let writer_20 = Snapshot::new(
        Xid::new(10),
        Xid::new(100),
        Xid::new(20),
        CommandId::FIRST,
        std::iter::empty(),
    );
    heap.update_int32_pair_inplace_undo(
        update_int32_scan(
            rel(),
            heap.block_count(rel()),
            &writer_20,
            &update_oracle,
            |_id, _value| true,
        ),
        update_int32_edit(1, 5),
        update_int32_stamp(20),
        None,
        None,
    )
    .unwrap();

    let entered = std::sync::Arc::new(std::sync::Barrier::new(2));
    let release = std::sync::Arc::new(std::sync::Barrier::new(2));
    let reader_oracle = std::sync::Arc::new(BlockingOracle::new(
        Xid::new(20),
        std::sync::Arc::clone(&entered),
        std::sync::Arc::clone(&release),
    ));
    reader_oracle.inner.set_committed(Xid::new(10));
    reader_oracle.inner.set_in_progress(Xid::new(20));
    let reader_heap = std::sync::Arc::clone(&heap);
    let reader = std::thread::spawn(move || {
        let snapshot = Snapshot::new(
            Xid::new(30),
            Xid::new(100),
            Xid::new(40),
            CommandId::FIRST,
            std::iter::empty(),
        );
        let mut walker = reader_heap.scan_visible_walker(
            rel(),
            reader_heap.block_count(rel()),
            &snapshot,
            reader_oracle.as_ref(),
        );
        let (_, _, payload) = walker.try_next().unwrap().unwrap();
        int32_pair_from_payload(payload)
    });

    entered.wait();
    update_oracle.set_committed(Xid::new(20));
    let writer_30 = Snapshot::new(
        Xid::new(10),
        Xid::new(100),
        Xid::new(30),
        CommandId::FIRST,
        std::iter::empty(),
    );
    heap.update_int32_pair_tid_inplace_undo(
        UpdateInt32PairTid {
            tid,
            snapshot: &writer_30,
            oracle: &update_oracle,
            predicate: |_id, _value| true,
        },
        update_int32_edit(1, 7),
        update_int32_stamp(30),
        None,
        None,
    )
    .unwrap();
    release.wait();

    assert_eq!(reader.join().unwrap(), (1, 10));
}

#[test]
fn walker_keeps_preimage_snapshot_across_concurrent_rollback() {
    let heap = std::sync::Arc::new(make_heap(8));
    heap.insert(rel(), &int32_pair_payload(1, 10), opts(10))
        .unwrap();
    let update_oracle = MapOracle::new();
    update_oracle.set_committed(Xid::new(10));
    let writer = Snapshot::new(
        Xid::new(10),
        Xid::new(100),
        Xid::new(20),
        CommandId::FIRST,
        std::iter::empty(),
    );
    heap.update_int32_pair_inplace_undo(
        update_int32_scan(
            rel(),
            heap.block_count(rel()),
            &writer,
            &update_oracle,
            |_id, _value| true,
        ),
        update_int32_edit(1, 5),
        update_int32_stamp(20),
        None,
        None,
    )
    .unwrap();

    let entered = std::sync::Arc::new(std::sync::Barrier::new(2));
    let release = std::sync::Arc::new(std::sync::Barrier::new(2));
    let reader_oracle = std::sync::Arc::new(BlockingOracle::new(
        Xid::new(20),
        std::sync::Arc::clone(&entered),
        std::sync::Arc::clone(&release),
    ));
    reader_oracle.inner.set_committed(Xid::new(10));
    reader_oracle.inner.set_in_progress(Xid::new(20));
    let reader_heap = std::sync::Arc::clone(&heap);
    let reader = std::thread::spawn(move || {
        let snapshot = Snapshot::new(
            Xid::new(30),
            Xid::new(100),
            Xid::new(40),
            CommandId::FIRST,
            std::iter::empty(),
        );
        let mut walker = reader_heap.scan_visible_walker(
            rel(),
            reader_heap.block_count(rel()),
            &snapshot,
            reader_oracle.as_ref(),
        );
        let (_, _, payload) = walker.try_next().unwrap().unwrap();
        int32_pair_from_payload(payload)
    });

    entered.wait();
    assert_eq!(heap.rollback_in_place_updates(Xid::new(20)).unwrap(), 1);
    assert_eq!(heap.int32_pair_undo_batch_len(rel()), 0);
    release.wait();
    assert_eq!(reader.join().unwrap(), (1, 10));

    update_oracle.set_aborted(Xid::new(20));
    let post_rollback = Snapshot::new(
        Xid::new(10),
        Xid::new(100),
        Xid::new(50),
        CommandId::FIRST,
        std::iter::empty(),
    );
    let rows = heap
        .scan_visible(
            rel(),
            heap.block_count(rel()),
            &post_rollback,
            &update_oracle,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(int32_pair_from_payload(&rows[0].data), (1, 10));
}

#[test]
fn rollback_inplace_int32_update_restores_compact_undo_batch() {
    let heap = make_heap(8);
    let tid = heap
        .insert(rel(), &int32_pair_payload(1, 10), opts(10))
        .unwrap();

    let oracle = MapOracle::new();
    oracle.set_committed(Xid::new(10));
    oracle.set_in_progress(Xid::new(20));
    let writer_20 = Snapshot::new(
        Xid::new(10),
        Xid::new(100),
        Xid::new(20),
        CommandId::FIRST,
        std::iter::empty(),
    );
    heap.update_int32_pair_inplace_undo(
        update_int32_scan(
            rel(),
            heap.block_count(rel()),
            &writer_20,
            &oracle,
            |id, _val| id == 1,
        ),
        update_int32_edit(1, 5),
        update_int32_stamp(20),
        None,
        None,
    )
    .unwrap();

    assert_eq!(heap.fetch(tid).unwrap().data, int32_pair_payload(1, 15));
    assert_eq!(heap.rollback_in_place_updates(Xid::new(20)).unwrap(), 1);
    assert_eq!(heap.fetch(tid).unwrap().data, int32_pair_payload(1, 10));
    assert_eq!(heap.int32_pair_undo_batch_len(rel()), 0);
}

#[test]
fn rollback_later_inplace_writer_preserves_prior_history_linkage() {
    let heap = make_heap(8);
    let tid = heap
        .insert(rel(), &int32_pair_payload(1, 10), opts(10))
        .unwrap();

    let oracle = MapOracle::new();
    oracle.set_committed(Xid::new(10));
    oracle.set_in_progress(Xid::new(20));
    let writer_20 = Snapshot::new(
        Xid::new(10),
        Xid::new(100),
        Xid::new(20),
        CommandId::FIRST,
        std::iter::empty(),
    );
    heap.update_int32_pair_inplace_undo(
        update_int32_scan(
            rel(),
            heap.block_count(rel()),
            &writer_20,
            &oracle,
            |id, _val| id == 1,
        ),
        update_int32_edit(1, 5),
        update_int32_stamp(20),
        None,
        None,
    )
    .unwrap();
    oracle.set_committed(Xid::new(20));

    oracle.set_in_progress(Xid::new(30));
    let writer_30 = Snapshot::new(
        Xid::new(10),
        Xid::new(100),
        Xid::new(30),
        CommandId::FIRST,
        std::iter::empty(),
    );
    heap.update_int32_pair_inplace_undo(
        update_int32_scan(
            rel(),
            heap.block_count(rel()),
            &writer_30,
            &oracle,
            |id, _val| id == 1,
        ),
        update_int32_edit(1, 7),
        update_int32_stamp(30),
        None,
        None,
    )
    .unwrap();
    assert_eq!(heap.fetch(tid).unwrap().data, int32_pair_payload(1, 22));

    assert_eq!(heap.rollback_in_place_updates(Xid::new(30)).unwrap(), 1);
    let restored = heap.fetch(tid).unwrap();
    assert_eq!(restored.data, int32_pair_payload(1, 15));
    assert!(
        restored.header.infomask.contains(InfoMask::INPLACE_HISTORY),
        "rollback must keep surviving xid 20 undo reachable"
    );

    oracle.set_aborted(Xid::new(30));
    let before_xid_20 = Snapshot::new(
        Xid::new(10),
        Xid::new(100),
        Xid::new(15),
        CommandId::FIRST,
        [Xid::new(20)],
    );
    let old_rows = heap
        .scan_visible(rel(), heap.block_count(rel()), &before_xid_20, &oracle)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(old_rows.len(), 1);
    assert_eq!(
        int32_pair_from_payload(&old_rows[0].data),
        (1, 10),
        "a snapshot predating xid 20 must still reconstruct its pre-image"
    );

    let after_xid_20 = Snapshot::new(
        Xid::new(10),
        Xid::new(100),
        Xid::new(40),
        CommandId::FIRST,
        std::iter::empty(),
    );
    let current_rows = heap
        .scan_visible(rel(), heap.block_count(rel()), &after_xid_20, &oracle)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(current_rows.len(), 1);
    assert_eq!(int32_pair_from_payload(&current_rows[0].data), (1, 15));
}

#[test]
fn rollback_page_error_preserves_that_pages_undo_for_retry() {
    let heap = make_heap(16);
    let mut rows = Vec::new();
    let mut id = 0_i32;
    while heap.block_count(rel()) < 2 {
        let tid = heap
            .insert(rel(), &int32_pair_payload(id, id * 10), opts(10))
            .unwrap();
        rows.push((tid, id));
        id += 1;
    }
    let second_page = PageId::new(rel(), BlockNumber::new(1));

    let oracle = MapOracle::new();
    oracle.set_committed(Xid::new(10));
    let writer = Snapshot::new(
        Xid::new(10),
        Xid::new(100),
        Xid::new(20),
        CommandId::FIRST,
        std::iter::empty(),
    );
    let updated = heap
        .update_int32_pair_inplace_undo(
            update_int32_scan(
                rel(),
                heap.block_count(rel()),
                &writer,
                &oracle,
                |_id, _value| true,
            ),
            update_int32_edit(1, 1),
            update_int32_stamp(20),
            None,
            None,
        )
        .unwrap();

    let original_item_id = {
        let guard = heap.get_page_relieved(second_page).unwrap();
        let mut page = guard.write();
        let bytes = page.as_bytes_mut();
        let offset = crate::page::PAGE_HEADER_SIZE;
        let original: [u8; 4] = bytes[offset..offset + 4].try_into().unwrap();
        bytes[offset..offset + 4].fill(0);
        original
    };
    let error = heap.rollback_in_place_updates(Xid::new(20)).unwrap_err();
    assert!(matches!(error, HeapError::MalformedHeader(_)));

    for (tid, row_id) in &rows {
        if tid.page.block == BlockNumber::new(0) {
            assert_eq!(
                heap.fetch(*tid).unwrap().data,
                int32_pair_payload(*row_id, *row_id * 10),
                "the first page must have completed its atomic rollback"
            );
        }
    }

    {
        let guard = heap.get_page_relieved(second_page).unwrap();
        let mut page = guard.write();
        let bytes = page.as_bytes_mut();
        let offset = crate::page::PAGE_HEADER_SIZE;
        bytes[offset..offset + 4].copy_from_slice(&original_item_id);
    }
    let second_page_rows = rows
        .iter()
        .filter(|(tid, _)| tid.page == second_page)
        .count();
    assert_eq!(
        heap.rollback_in_place_updates(Xid::new(20)).unwrap(),
        second_page_rows
    );
    assert_eq!(updated, rows.len());
    for (tid, row_id) in rows {
        assert_eq!(
            heap.fetch(tid).unwrap().data,
            int32_pair_payload(row_id, row_id * 10)
        );
    }
}

#[test]
fn rollback_delete_stamp_restores_logged_page() {
    let heap = make_heap(8);
    let tid = heap.insert(rel(), b"alive", opts(10)).unwrap();

    heap.delete(tid, del_opts(20, 0)).unwrap();
    assert_eq!(heap.fetch(tid).unwrap().header.xmax, Xid::new(20));

    assert_eq!(heap.rollback_in_place_updates(Xid::new(20)).unwrap(), 1);
    let restored = heap.fetch(tid).unwrap();
    assert_eq!(restored.header.xmax, Xid::INVALID);
    assert_eq!(restored.header.cmax, CommandId::FIRST);
}

#[test]
fn parallel_no_wal_inplace_int32_update_records_undo_and_rolls_back() {
    let heap = make_heap(4096);
    let tids = (0_i32..4)
        .map(|id| {
            heap.insert(rel(), &int32_pair_payload(id, id * 10), opts(10))
                .unwrap()
        })
        .collect::<Vec<_>>();

    let oracle = MapOracle::new();
    oracle.set_committed(Xid::new(10));
    oracle.set_in_progress(Xid::new(20));
    let writer_20 = Snapshot::new(
        Xid::new(10),
        Xid::new(100),
        Xid::new(20),
        CommandId::FIRST,
        std::iter::empty(),
    );
    let updated = heap
        .update_int32_pair_inplace_undo_parallel_no_wal(
            update_int32_scan(rel(), 2_048, &writer_20, &oracle, |_id, _val| true),
            update_int32_edit(1, 7),
            update_int32_stamp(20),
            None,
        )
        .unwrap();

    assert_eq!(updated, 4);
    assert_eq!(heap.int32_pair_undo_batch_len(rel()), 1);
    for (idx, tid) in tids.iter().enumerate() {
        let id = i32::try_from(idx).unwrap();
        assert_eq!(
            heap.fetch(*tid).unwrap().data,
            int32_pair_payload(id, id * 10 + 7)
        );
    }
    assert_eq!(heap.rollback_in_place_updates(Xid::new(20)).unwrap(), 4);
    for (idx, tid) in tids.iter().enumerate() {
        let id = i32::try_from(idx).unwrap();
        assert_eq!(
            heap.fetch(*tid).unwrap().data,
            int32_pair_payload(id, id * 10)
        );
    }
}

#[test]
fn parallel_no_wal_overflow_keeps_completed_pages_rollbackable() {
    if std::thread::available_parallelism().map_or(1, |workers| workers.get()) <= 1 {
        return;
    }

    let heap = make_heap(2_048);
    let (rows, overflow_id) = fill_parallel_update_pages(&heap, 525, 520, i32::MAX);
    let oracle = MapOracle::new();
    oracle.set_committed(Xid::new(10));
    let writer = Snapshot::new(
        Xid::new(10),
        Xid::new(100),
        Xid::new(20),
        CommandId::FIRST,
        std::iter::empty(),
    );

    let error = heap
        .update_int32_pair_inplace_undo_parallel_no_wal(
            update_int32_scan(rel(), 2_048, &writer, &oracle, |_id, _value| true),
            update_int32_edit(1, 1),
            update_int32_stamp(20),
            None,
        )
        .unwrap_err();
    assert!(
        matches!(error, HeapError::NumericOverflow(_)),
        "the worker's original overflow must be preserved: {error:?}"
    );
    let overflow_tid = rows.iter().find(|(_, id, _)| *id == overflow_id).unwrap().0;
    assert_eq!(
        heap.fetch(overflow_tid).unwrap().data,
        int32_pair_payload(overflow_id, i32::MAX)
    );

    assert!(
        heap.rollback_in_place_updates(Xid::new(20)).unwrap() > 0,
        "other chunks must have completed mutations to restore"
    );
    for (tid, id, value) in rows {
        assert_eq!(
            heap.fetch(tid).unwrap().data,
            int32_pair_payload(id, value),
            "rollback must restore every row after a late overflow"
        );
    }
}

#[test]
fn parallel_no_wal_predicate_panic_keeps_completed_pages_rollbackable() {
    if std::thread::available_parallelism().map_or(1, |workers| workers.get()) <= 1 {
        return;
    }

    let heap = make_heap(2_048);
    let (rows, panic_id) = fill_parallel_update_pages(&heap, 525, 520, 0);
    let oracle = MapOracle::new();
    oracle.set_committed(Xid::new(10));
    let writer = Snapshot::new(
        Xid::new(10),
        Xid::new(100),
        Xid::new(20),
        CommandId::FIRST,
        std::iter::empty(),
    );

    let error = heap
        .update_int32_pair_inplace_undo_parallel_no_wal(
            update_int32_scan(rel(), 2_048, &writer, &oracle, move |id, _value| {
                assert_ne!(id, panic_id, "injected no-WAL predicate panic");
                true
            }),
            update_int32_edit(1, 1),
            update_int32_stamp(20),
            None,
        )
        .unwrap_err();
    assert!(matches!(error, HeapError::ParallelWorkerPanic));

    assert!(
        heap.rollback_in_place_updates(Xid::new(20)).unwrap() > 0,
        "successful chunks must remain rollbackable after a sibling panic"
    );
    for (tid, id, value) in rows {
        assert_eq!(
            heap.fetch(tid).unwrap().data,
            int32_pair_payload(id, value),
            "rollback must restore every row after a late predicate panic"
        );
    }
}

// -----------------------------------------------------------------------
// Multi-writer in-place-update pre-image (snapshot isolation).
//
// A row updated IN PLACE by several committed writers AFTER a reader's
// snapshot must reconstruct the value as of *before the first writer
// the reader cannot see*, not an intermediate version. Writers use xids
// in [xmin, xmax) of `committed_snap` so each writer sees its
// predecessor committed; readers use a snapshot whose `xmax` lands below
// the writer xids so they fall in the implicit in-progress region.
// -----------------------------------------------------------------------

/// FULL-PAYLOAD path (`update_int32_pair_tid_inplace_undo`, one
/// [`UndoEntry`] per write): three committed writers stack V0→V1→V2→V3.
/// A reader whose snapshot predates all three must observe V0, not an
/// intermediate V1/V2.
#[test]
fn full_payload_multi_writer_reader_sees_oldest_pre_image() {
    let heap = make_heap(8);
    let tid = heap
        .insert(rel(), &int32_pair_payload(1, 100), opts(10))
        .unwrap();

    let oracle = MapOracle::new();
    oracle.set_committed(Xid::new(10));
    oracle.set_committed(Xid::new(100));
    oracle.set_committed(Xid::new(200));
    oracle.set_committed(Xid::new(300));

    // V0 (val=100) -> V1 (105) -> V2 (108) -> V3 (114), each a committed
    // writer that sees its predecessor committed.
    for (writer, delta) in [(100_u64, 5_i32), (200, 3), (300, 6)] {
        let snap = committed_snap(writer);
        let updated = heap
            .update_int32_pair_tid_inplace_undo(
                UpdateInt32PairTid {
                    tid,
                    snapshot: &snap,
                    oracle: &oracle,
                    predicate: |id, _val| id == 1,
                },
                update_int32_edit(1, delta),
                update_int32_stamp(writer),
                None,
                None,
            )
            .unwrap();
        assert_eq!(updated, 1);
    }
    // Physical slot is the post-image V3.
    assert_eq!(heap.fetch(tid).unwrap().data, int32_pair_payload(1, 114));

    // Reader whose snapshot predates all three writers (xmax = 50, so
    // 100/200/300 are implicitly in progress) must see V0 = 100, NOT an
    // intermediate.
    let reader = Snapshot::new(
        Xid::new(20),
        Xid::new(50),
        Xid::new(30),
        CommandId::FIRST,
        std::iter::empty(),
    );
    let visible: Vec<HeapTuple> = heap
        .scan_visible(rel(), heap.block_count(rel()), &reader, &oracle)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(visible.len(), 1);
    assert_eq!(
        int32_pair_from_payload(&visible[0].data),
        (1, 100),
        "reader predating all writers must see V0, not an intermediate"
    );

    // Single-writer case is still correct: a reader predating only the
    // first writer sees V0 too (sanity that the oldest-pick is also
    // right with one entry).
    let single = Snapshot::new(
        Xid::new(20),
        Xid::new(50),
        Xid::new(30),
        CommandId::FIRST,
        std::iter::empty(),
    );
    let walked: Vec<(i32, i32)> = collect_walker_pairs(&heap, &single, &oracle);
    assert_eq!(walked, vec![(1, 100)]);
}

/// FULL-PAYLOAD path with a MIX of visible and invisible writers: T100
/// is visible to the reader, T200/T300 are not. The reader must see V1
/// (the state after the last visible writer), reversing only the two
/// invisible writers.
#[test]
fn full_payload_mixed_visibility_reader_sees_after_last_visible() {
    let heap = make_heap(8);
    let tid = heap
        .insert(rel(), &int32_pair_payload(1, 100), opts(10))
        .unwrap();

    let oracle = MapOracle::new();
    oracle.set_committed(Xid::new(10));
    oracle.set_committed(Xid::new(100));
    oracle.set_committed(Xid::new(200));
    oracle.set_committed(Xid::new(300));

    for (writer, delta) in [(100_u64, 5_i32), (200, 3), (300, 6)] {
        let snap = committed_snap(writer);
        heap.update_int32_pair_tid_inplace_undo(
            UpdateInt32PairTid {
                tid,
                snapshot: &snap,
                oracle: &oracle,
                predicate: |id, _val| id == 1,
            },
            update_int32_edit(1, delta),
            update_int32_stamp(writer),
            None,
            None,
        )
        .unwrap();
    }

    // Reader with xmax = 150: T100 < 150 and committed -> visible;
    // T200/T300 >= 150 -> implicitly in progress -> invisible. Correct
    // view is V1 = 105 (after the last visible writer T100).
    let reader = Snapshot::new(
        Xid::new(50),
        Xid::new(150),
        Xid::new(60),
        CommandId::FIRST,
        std::iter::empty(),
    );
    let visible: Vec<HeapTuple> = heap
        .scan_visible(rel(), heap.block_count(rel()), &reader, &oracle)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(visible.len(), 1);
    assert_eq!(
        int32_pair_from_payload(&visible[0].data),
        (1, 105),
        "reader must see state after last visible writer (V1), not V0 or V2"
    );
}

/// COMPACT DELTA path (`update_int32_pair_inplace_undo`, one
/// [`Int32PairUndoBatch`] per write): `val += 5` then `val += 3` by two
/// committed writers. A reader predating both must see `current − 8`
/// (the base), not `current − 3` (reversing only the newest delta).
#[test]
fn compact_delta_multi_writer_reverses_all_invisible_deltas() {
    let heap = make_heap(8);
    let tid = heap
        .insert(rel(), &int32_pair_payload(1, 100), opts(10))
        .unwrap();

    let oracle = MapOracle::new();
    oracle.set_committed(Xid::new(10));
    oracle.set_committed(Xid::new(100));
    oracle.set_committed(Xid::new(200));

    for (writer, delta) in [(100_u64, 5_i32), (200, 3)] {
        let snap = committed_snap(writer);
        let updated = heap
            .update_int32_pair_inplace_undo(
                update_int32_scan(
                    rel(),
                    heap.block_count(rel()),
                    &snap,
                    &oracle,
                    |id, _val| id == 1,
                ),
                update_int32_edit(1, delta),
                update_int32_stamp(writer),
                None,
                None,
            )
            .unwrap();
        assert_eq!(updated, 1);
    }
    // Current slot is base + 8 = 108.
    assert_eq!(
        int32_pair_from_payload(&heap.fetch(tid).unwrap().data),
        (1, 108)
    );

    let reader = Snapshot::new(
        Xid::new(20),
        Xid::new(50),
        Xid::new(30),
        CommandId::FIRST,
        std::iter::empty(),
    );
    let visible: Vec<HeapTuple> = heap
        .scan_visible(rel(), heap.block_count(rel()), &reader, &oracle)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(visible.len(), 1);
    assert_eq!(
        int32_pair_from_payload(&visible[0].data),
        (1, 100),
        "reader must reverse both invisible deltas (current - 8), not just the newest"
    );
}

/// COMPACT DELTA path, per-column correctness: two writers touch
/// different target columns (`id += 7`, then `val += 4`). A reader
/// predating both must reverse each column's delta independently.
#[test]
fn compact_delta_per_column_reverses_correctly() {
    let heap = make_heap(8);
    let tid = heap
        .insert(rel(), &int32_pair_payload(1, 100), opts(10))
        .unwrap();

    let oracle = MapOracle::new();
    oracle.set_committed(Xid::new(10));
    oracle.set_committed(Xid::new(100));
    oracle.set_committed(Xid::new(200));

    // T100: id += 7  -> (8, 100). Re-target by id == 1 first.
    let snap_100 = committed_snap(100);
    heap.update_int32_pair_inplace_undo(
        update_int32_scan(
            rel(),
            heap.block_count(rel()),
            &snap_100,
            &oracle,
            |id, _val| id == 1,
        ),
        update_int32_edit(0, 7),
        update_int32_stamp(100),
        None,
        None,
    )
    .unwrap();
    // T200: val += 4 -> (8, 104).
    let snap_200 = committed_snap(200);
    heap.update_int32_pair_inplace_undo(
        update_int32_scan(
            rel(),
            heap.block_count(rel()),
            &snap_200,
            &oracle,
            |id, _val| id == 8,
        ),
        update_int32_edit(1, 4),
        update_int32_stamp(200),
        None,
        None,
    )
    .unwrap();
    assert_eq!(
        int32_pair_from_payload(&heap.fetch(tid).unwrap().data),
        (8, 104)
    );

    let reader = Snapshot::new(
        Xid::new(20),
        Xid::new(50),
        Xid::new(30),
        CommandId::FIRST,
        std::iter::empty(),
    );
    let visible: Vec<HeapTuple> = heap
        .scan_visible(rel(), heap.block_count(rel()), &reader, &oracle)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(visible.len(), 1);
    assert_eq!(
        int32_pair_from_payload(&visible[0].data),
        (1, 100),
        "per-column reversal must restore id and val independently"
    );
}

#[test]
fn point_inplace_int32_update_rechecks_tid_predicate() {
    let heap = make_heap(8);
    let first = heap
        .insert(rel(), &int32_pair_payload(1, 10), opts(10))
        .unwrap();
    let second = heap
        .insert(rel(), &int32_pair_payload(2, 20), opts(10))
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
                tid: second,
                snapshot: &writer_20,
                oracle: &oracle,
                predicate: |id, _val| id == 2,
            },
            update_int32_edit(1, 7),
            update_int32_stamp(20),
            None,
            None,
        )
        .unwrap();
    assert_eq!(updated, 1);
    assert_eq!(heap.fetch(second).unwrap().data, int32_pair_payload(2, 27));

    let skipped = heap
        .update_int32_pair_tid_inplace_undo(
            UpdateInt32PairTid {
                tid: first,
                snapshot: &writer_20,
                oracle: &oracle,
                predicate: |id, _val| id == 2,
            },
            update_int32_edit(1, 100),
            update_int32_stamp(20),
            None,
            None,
        )
        .unwrap();
    assert_eq!(skipped, 0);
    assert_eq!(heap.fetch(first).unwrap().data, int32_pair_payload(1, 10));
}

#[test]
fn undo_log_indices_scope_lookups_and_survive_trim_and_rollback_partition() {
    use super::super::{Int32PairUndoBatch, UndoEntry, UndoRelationLog};

    let mut log = UndoRelationLog::default();
    let tid_a = TupleId::new(PageId::new(RelationId::new(1), BlockNumber::new(1)), 0);
    let tid_b = TupleId::new(PageId::new(RelationId::new(1), BlockNumber::new(2)), 3);
    let entry = |tid: TupleId, xid: u64, tag: u8| UndoEntry {
        tid,
        writer_xid: Xid::new(xid),
        command_id: CommandId::FIRST,
        old_payload: [tag; 9],
    };

    // Interleaved appends across two slots: per-tid iteration yields only
    // that slot's writers, oldest first.
    log.push_entry(entry(tid_a, 10, 1));
    log.push_entry(entry(tid_b, 11, 2));
    log.push_entry(entry(tid_a, 12, 3));
    let a_writers: Vec<u64> = log
        .entries_for_tid(tid_a)
        .map(|e| e.writer_xid.raw())
        .collect();
    assert_eq!(a_writers, vec![10, 12]);
    let b_writers: Vec<u64> = log
        .entries_for_tid(tid_b)
        .map(|e| e.writer_xid.raw())
        .collect();
    assert_eq!(b_writers, vec![11]);

    // Batches are scoped per page the same way.
    let batch = |block: u32, xid: u64, delta: i32| Int32PairUndoBatch {
        page: PageId::new(RelationId::new(1), BlockNumber::new(block)),
        writer_xid: Xid::new(xid),
        command_id: CommandId::new(0),
        target_col: 1,
        delta,
        first_slot: 0,
        slot_count: 4,
        slots: Vec::new(),
    };
    log.push_int32_pair_batch(batch(1, 10, 7));
    log.push_int32_pair_batch(batch(2, 11, -2));
    log.push_int32_pair_batch(batch(1, 12, 5));
    let page1 = PageId::new(RelationId::new(1), BlockNumber::new(1));
    let deltas: Vec<i32> = log.batches_for_page(page1).map(|b| b.delta).collect();
    assert_eq!(deltas, vec![7, 5]);

    // Rollback partition removes exactly one writer's records everywhere
    // and the indices stay coherent for the survivors.
    let (taken_entries, taken_batches) = log.take_written_by(Xid::new(12));
    assert_eq!(taken_entries.len(), 1);
    assert_eq!(taken_batches.len(), 1);
    let a_writers: Vec<u64> = log
        .entries_for_tid(tid_a)
        .map(|e| e.writer_xid.raw())
        .collect();
    assert_eq!(a_writers, vec![10]);
    let deltas: Vec<i32> = log.batches_for_page(page1).map(|b| b.delta).collect();
    assert_eq!(deltas, vec![7]);

    // Vacuum trim below xid 11 drops writer 10 from both kinds; the
    // indices reflect the survivors only.
    let (trimmed_entries, trimmed_batches) = log.trim_below(Xid::new(11));
    assert_eq!((trimmed_entries, trimmed_batches), (1, 1));
    assert!(log.entries_for_tid(tid_a).next().is_none());
    assert!(log.batches_for_page(page1).next().is_none());
    let b_writers: Vec<u64> = log
        .entries_for_tid(tid_b)
        .map(|e| e.writer_xid.raw())
        .collect();
    assert_eq!(b_writers, vec![11]);
    assert_eq!(log.entries_len(), 1);
    assert_eq!(log.int32_pair_batches_len(), 1);
}

#[test]
fn mixed_point_and_bulk_undo_reconstructs_in_temporal_order() {
    for point_first in [true, false] {
        let heap = make_heap(8);
        let tid = heap
            .insert(rel(), &int32_pair_payload(1, 10), opts(10))
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
        if point_first {
            heap.update_int32_pair_tid_inplace_undo(
                UpdateInt32PairTid {
                    tid,
                    snapshot: &writer_20,
                    oracle: &oracle,
                    predicate: |_id, _value| true,
                },
                update_int32_edit(1, 5),
                update_int32_stamp(20),
                None,
                None,
            )
            .unwrap();
        } else {
            heap.update_int32_pair_inplace_undo(
                update_int32_scan(
                    rel(),
                    heap.block_count(rel()),
                    &writer_20,
                    &oracle,
                    |_id, _value| true,
                ),
                update_int32_edit(1, 5),
                update_int32_stamp(20),
                None,
                None,
            )
            .unwrap();
        }
        oracle.set_committed(Xid::new(20));

        let writer_30 = Snapshot::new(
            Xid::new(10),
            Xid::new(100),
            Xid::new(30),
            CommandId::FIRST,
            std::iter::empty(),
        );
        if point_first {
            heap.update_int32_pair_inplace_undo(
                update_int32_scan(
                    rel(),
                    heap.block_count(rel()),
                    &writer_30,
                    &oracle,
                    |_id, _value| true,
                ),
                update_int32_edit(1, 7),
                update_int32_stamp(30),
                None,
                None,
            )
            .unwrap();
        } else {
            heap.update_int32_pair_tid_inplace_undo(
                UpdateInt32PairTid {
                    tid,
                    snapshot: &writer_30,
                    oracle: &oracle,
                    predicate: |_id, _value| true,
                },
                update_int32_edit(1, 7),
                update_int32_stamp(30),
                None,
                None,
            )
            .unwrap();
        }
        oracle.set_committed(Xid::new(30));

        let old_reader = Snapshot::new(
            Xid::new(10),
            Xid::new(20),
            Xid::new(40),
            CommandId::FIRST,
            std::iter::empty(),
        );
        let rows = heap
            .scan_visible(rel(), heap.block_count(rel()), &old_reader, &oracle)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            int32_pair_from_payload(&rows[0].data),
            (1, 10),
            "point_first={point_first}"
        );
    }
}

#[test]
fn same_xid_undo_respects_command_boundaries_across_record_forms() {
    for point_first in [true, false] {
        let heap = make_heap(8);
        let tid = heap
            .insert(rel(), &int32_pair_payload(1, 10), opts(10))
            .unwrap();
        let oracle = MapOracle::new();
        oracle.set_committed(Xid::new(10));
        oracle.set_in_progress(Xid::new(20));

        let apply = |point: bool, command: u32, delta: i32| {
            let snapshot = Snapshot::new(
                Xid::new(10),
                Xid::new(100),
                Xid::new(20),
                CommandId::new(command),
                std::iter::empty(),
            );
            let stamp = UpdateInt32PairStamp {
                xid: Xid::new(20),
                command_id: CommandId::new(command),
            };
            if point {
                heap.update_int32_pair_tid_inplace_undo(
                    UpdateInt32PairTid {
                        tid,
                        snapshot: &snapshot,
                        oracle: &oracle,
                        predicate: |_id, _value| true,
                    },
                    update_int32_edit(1, delta),
                    stamp,
                    None,
                    None,
                )
            } else {
                heap.update_int32_pair_inplace_undo(
                    update_int32_scan(
                        rel(),
                        heap.block_count(rel()),
                        &snapshot,
                        &oracle,
                        |_id, _value| true,
                    ),
                    update_int32_edit(1, delta),
                    stamp,
                    None,
                    None,
                )
            }
        };

        assert_eq!(apply(point_first, 1, 5).unwrap(), 1);
        assert_eq!(apply(!point_first, 2, 7).unwrap(), 1);
        assert_eq!(
            int32_pair_from_payload(&heap.fetch(tid).unwrap().data),
            (1, 22)
        );

        let read_at = |command: u32| {
            let snapshot = Snapshot::new(
                Xid::new(10),
                Xid::new(100),
                Xid::new(20),
                CommandId::new(command),
                std::iter::empty(),
            );
            let rows = heap
                .scan_visible(rel(), heap.block_count(rel()), &snapshot, &oracle)
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(rows.len(), 1);
            int32_pair_from_payload(&rows[0].data)
        };

        assert_eq!(read_at(0), (1, 10), "shape point_first={point_first}");
        assert_eq!(read_at(1), (1, 10), "shape point_first={point_first}");
        assert_eq!(read_at(2), (1, 15), "shape point_first={point_first}");
        assert_eq!(read_at(3), (1, 22), "shape point_first={point_first}");
    }
}

#[test]
fn rolled_back_own_subxid_undo_overrides_commit_status() {
    let heap = make_heap(8);
    let tid = heap
        .insert(rel(), &int32_pair_payload(1, 10), opts(10))
        .unwrap();
    let oracle = MapOracle::new();
    oracle.set_committed(Xid::new(10));
    oracle.set_committed(Xid::new(21));

    let mut writer = Snapshot::new(
        Xid::new(10),
        Xid::new(100),
        Xid::new(20),
        CommandId::new(1),
        std::iter::empty(),
    );
    writer.set_own_subxids([Xid::new(21)], std::iter::empty());
    assert_eq!(
        heap.update_int32_pair_tid_inplace_undo(
            UpdateInt32PairTid {
                tid,
                snapshot: &writer,
                oracle: &oracle,
                predicate: |_id, _value| true,
            },
            update_int32_edit(1, 5),
            UpdateInt32PairStamp {
                xid: Xid::new(21),
                command_id: CommandId::new(1),
            },
            None,
            None,
        )
        .unwrap(),
        1
    );
    assert_eq!(
        int32_pair_from_payload(&heap.fetch(tid).unwrap().data),
        (1, 15)
    );

    // Before physical rollback, the snapshot's rolled-back-subxid set must
    // force the undo pre-image even if CLOG incorrectly/temporarily reports
    // the subxid committed.
    let mut after_rollback_to = Snapshot::new(
        Xid::new(10),
        Xid::new(100),
        Xid::new(20),
        CommandId::new(2),
        std::iter::empty(),
    );
    after_rollback_to.set_own_subxids(std::iter::empty(), [Xid::new(21)]);
    let rows = heap
        .scan_visible(rel(), heap.block_count(rel()), &after_rollback_to, &oracle)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(int32_pair_from_payload(&rows[0].data), (1, 10));
}

#[test]
fn fused_update_predicate_uses_visible_pre_image() {
    let (heap, oracle, tid) = heap_with_in_progress_int32_pair_update();
    let contender = Snapshot::new(
        Xid::new(10),
        Xid::new(100),
        Xid::new(30),
        CommandId::FIRST,
        [Xid::new(20)],
    );

    // The physical post-image matches, but this snapshot's logical row does
    // not. The writer must be ignored without a false conflict or mutation.
    assert_eq!(
        heap.update_int32_pair_inplace_undo(
            update_int32_scan(
                rel(),
                heap.block_count(rel()),
                &contender,
                &oracle,
                |_id, val| val == 20,
            ),
            update_int32_edit(1, 1),
            update_int32_stamp(30),
            None,
            None,
        )
        .unwrap(),
        0
    );

    // The logical pre-image matches while the physical post-image does not.
    // Mutating the post-image would update a version this snapshot cannot
    // see, so surface a retryable write conflict instead of affecting zero.
    let error = heap
        .update_int32_pair_inplace_undo(
            update_int32_scan(
                rel(),
                heap.block_count(rel()),
                &contender,
                &oracle,
                |_id, val| val == 10,
            ),
            update_int32_edit(1, 1),
            update_int32_stamp(30),
            None,
            None,
        )
        .unwrap_err();
    assert!(matches!(error, HeapError::WriteConflict(_)));
    let tuple = heap.fetch(tid).unwrap();
    assert_eq!(tuple.header.xmax, Xid::new(20));
    assert_eq!(int32_pair_from_payload(&tuple.data), (1, 20));
}

#[test]
fn parallel_fused_update_predicate_uses_visible_pre_image() {
    let (heap, oracle, tid) = heap_with_in_progress_int32_pair_update();
    let contender = Snapshot::new(
        Xid::new(10),
        Xid::new(100),
        Xid::new(30),
        CommandId::FIRST,
        [Xid::new(20)],
    );

    assert_eq!(
        heap.update_int32_pair_inplace_undo_parallel_no_wal(
            update_int32_scan(rel(), 2_048, &contender, &oracle, |_id, val| val == 20),
            update_int32_edit(1, 1),
            update_int32_stamp(30),
            None,
        )
        .unwrap(),
        0
    );
    let error = heap
        .update_int32_pair_inplace_undo_parallel_no_wal(
            update_int32_scan(rel(), 2_048, &contender, &oracle, |_id, val| val == 10),
            update_int32_edit(1, 1),
            update_int32_stamp(30),
            None,
        )
        .unwrap_err();
    assert!(matches!(error, HeapError::WriteConflict(_)));
    let tuple = heap.fetch(tid).unwrap();
    assert_eq!(tuple.header.xmax, Xid::new(20));
    assert_eq!(int32_pair_from_payload(&tuple.data), (1, 20));
}

#[test]
fn fused_delete_predicate_uses_visible_pre_image() {
    let (heap, oracle, tid) = heap_with_in_progress_int32_pair_update();
    let contender = Snapshot::new(
        Xid::new(10),
        Xid::new(100),
        Xid::new(30),
        CommandId::FIRST,
        [Xid::new(20)],
    );
    let stamp = DeleteInt32PairStamp {
        xid: Xid::new(30),
        command_id: CommandId::FIRST,
    };

    assert_eq!(
        heap.delete_int32_pair_inplace(
            DeleteInt32PairScan {
                rel: rel(),
                block_count: heap.block_count(rel()),
                snapshot: &contender,
                oracle: &oracle,
                predicate: |_id: i32, val: i32| val == 20,
            },
            stamp,
            None,
            None,
        )
        .unwrap(),
        0
    );
    let error = heap
        .delete_int32_pair_inplace(
            DeleteInt32PairScan {
                rel: rel(),
                block_count: heap.block_count(rel()),
                snapshot: &contender,
                oracle: &oracle,
                predicate: |_id: i32, val: i32| val == 10,
            },
            stamp,
            None,
            None,
        )
        .unwrap_err();
    assert!(matches!(error, HeapError::WriteConflict(_)));
    let tuple = heap.fetch(tid).unwrap();
    assert_eq!(tuple.header.xmax, Xid::new(20));
    assert_eq!(int32_pair_from_payload(&tuple.data), (1, 20));
}

#[test]
fn delete_payload_stats_never_prove_unobserved_pre_image_slots() {
    let (heap, oracle, updated_tid) = heap_with_in_progress_int32_pair_update();
    let other_tid = heap
        .insert(rel(), &int32_pair_payload(2, 15), opts(10))
        .unwrap();
    let old_snapshot = Snapshot::new(
        Xid::new(10),
        Xid::new(100),
        Xid::new(30),
        CommandId::FIRST,
        [Xid::new(20)],
    );

    // Slot 0 is a pre-image row whose logical value is 10, so this typed
    // predicate skips it. Slot 1 matches and is deleted. A stats cache built
    // from only slot 1 must not later claim that every physical slot is 15.
    assert_eq!(
        heap.delete_int32_pair_inplace(
            DeleteInt32PairScan {
                rel: rel(),
                block_count: heap.block_count(rel()),
                snapshot: &old_snapshot,
                oracle: &oracle,
                predicate: Int32PairPredicate::ColumnCmp {
                    col_index: 1,
                    op: Int32PairCmp::Eq,
                    literal: 15,
                },
            },
            DeleteInt32PairStamp {
                xid: Xid::new(30),
                command_id: CommandId::FIRST,
            },
            None,
            None,
        )
        .unwrap(),
        1
    );
    assert_eq!(heap.fetch(other_tid).unwrap().header.xmax, Xid::new(30));

    oracle.set_committed(Xid::new(20));
    oracle.set_committed(Xid::new(30));
    let new_snapshot = Snapshot::new(
        Xid::new(10),
        Xid::new(100),
        Xid::new(40),
        CommandId::FIRST,
        std::iter::empty(),
    );
    assert_eq!(
        heap.delete_int32_pair_inplace(
            DeleteInt32PairScan {
                rel: rel(),
                block_count: heap.block_count(rel()),
                snapshot: &new_snapshot,
                oracle: &oracle,
                predicate: Int32PairPredicate::ColumnCmp {
                    col_index: 1,
                    op: Int32PairCmp::Eq,
                    literal: 15,
                },
            },
            DeleteInt32PairStamp {
                xid: Xid::new(40),
                command_id: CommandId::FIRST,
            },
            None,
            None,
        )
        .unwrap(),
        0
    );
    let updated = heap.fetch(updated_tid).unwrap();
    assert_eq!(updated.header.xmax, Xid::new(20));
    assert_eq!(int32_pair_from_payload(&updated.data), (1, 20));
}

#[test]
fn parallel_fused_delete_predicate_uses_visible_pre_image() {
    let (heap, oracle, tid) = heap_with_in_progress_int32_pair_update();
    let contender = Snapshot::new(
        Xid::new(10),
        Xid::new(100),
        Xid::new(30),
        CommandId::FIRST,
        [Xid::new(20)],
    );
    let stamp = DeleteInt32PairStamp {
        xid: Xid::new(30),
        command_id: CommandId::FIRST,
    };

    assert_eq!(
        heap.delete_int32_pair_inplace_parallel_no_wal(
            DeleteInt32PairScan {
                rel: rel(),
                block_count: 2_048,
                snapshot: &contender,
                oracle: &oracle,
                predicate: |_id: i32, val: i32| val == 20,
            },
            stamp,
            None,
        )
        .unwrap(),
        0
    );
    let error = heap
        .delete_int32_pair_inplace_parallel_no_wal(
            DeleteInt32PairScan {
                rel: rel(),
                block_count: 2_048,
                snapshot: &contender,
                oracle: &oracle,
                predicate: |_id: i32, val: i32| val == 10,
            },
            stamp,
            None,
        )
        .unwrap_err();
    assert!(matches!(error, HeapError::WriteConflict(_)));
    let tuple = heap.fetch(tid).unwrap();
    assert_eq!(tuple.header.xmax, Xid::new(20));
    assert_eq!(int32_pair_from_payload(&tuple.data), (1, 20));
}

#[test]
fn fused_mutations_honor_rolled_back_subxid_pre_image() {
    for mutation in ["update", "delete"] {
        let heap = make_heap(8);
        let tid = heap
            .insert(rel(), &int32_pair_payload(1, 10), opts(10))
            .unwrap();
        let oracle = MapOracle::new();
        oracle.set_committed(Xid::new(10));
        // Deliberately report the subxid committed: the snapshot's rolled-back
        // set is authoritative and must still reconstruct its pre-image.
        oracle.set_committed(Xid::new(21));

        let mut writer = Snapshot::new(
            Xid::new(10),
            Xid::new(100),
            Xid::new(20),
            CommandId::new(1),
            std::iter::empty(),
        );
        writer.set_own_subxids([Xid::new(21)], std::iter::empty());
        assert_eq!(
            heap.update_int32_pair_tid_inplace_undo(
                UpdateInt32PairTid {
                    tid,
                    snapshot: &writer,
                    oracle: &oracle,
                    predicate: |_id, _val| true,
                },
                update_int32_edit(1, 10),
                UpdateInt32PairStamp {
                    xid: Xid::new(21),
                    command_id: CommandId::new(1),
                },
                None,
                None,
            )
            .unwrap(),
            1
        );

        let mut after_rollback = Snapshot::new(
            Xid::new(10),
            Xid::new(100),
            Xid::new(20),
            CommandId::new(2),
            std::iter::empty(),
        );
        after_rollback.set_own_subxids(std::iter::empty(), [Xid::new(21)]);
        let result = match mutation {
            "update" => heap.update_int32_pair_inplace_undo(
                update_int32_scan(
                    rel(),
                    heap.block_count(rel()),
                    &after_rollback,
                    &oracle,
                    |_id, val| val == 10,
                ),
                update_int32_edit(1, 1),
                UpdateInt32PairStamp {
                    xid: Xid::new(20),
                    command_id: CommandId::new(2),
                },
                None,
                None,
            ),
            "delete" => heap.delete_int32_pair_inplace(
                DeleteInt32PairScan {
                    rel: rel(),
                    block_count: heap.block_count(rel()),
                    snapshot: &after_rollback,
                    oracle: &oracle,
                    predicate: |_id: i32, val: i32| val == 10,
                },
                DeleteInt32PairStamp {
                    xid: Xid::new(20),
                    command_id: CommandId::new(2),
                },
                None,
                None,
            ),
            _ => unreachable!(),
        };
        assert!(
            matches!(result, Err(HeapError::WriteConflict(_))),
            "mutation={mutation}"
        );
        let tuple = heap.fetch(tid).unwrap();
        assert_eq!(tuple.header.xmax, Xid::new(21), "mutation={mutation}");
        assert_eq!(
            int32_pair_from_payload(&tuple.data),
            (1, 20),
            "mutation={mutation}"
        );
    }
}

#[test]
fn same_xid_point_and_mixed_updates_rollback_in_reverse_sequence() {
    for shape in ["point-point", "point-bulk", "bulk-point"] {
        let heap = make_heap(8);
        let tid = heap
            .insert(rel(), &int32_pair_payload(1, 10), opts(10))
            .unwrap();
        let oracle = MapOracle::new();
        oracle.set_committed(Xid::new(10));
        let first = Snapshot::new(
            Xid::new(10),
            Xid::new(100),
            Xid::new(20),
            CommandId::FIRST,
            std::iter::empty(),
        );
        let second = Snapshot::new(
            Xid::new(10),
            Xid::new(100),
            Xid::new(20),
            CommandId::new(1),
            std::iter::empty(),
        );
        let point = |snapshot: &Snapshot, command_id: u32| {
            heap.update_int32_pair_tid_inplace_undo(
                UpdateInt32PairTid {
                    tid,
                    snapshot,
                    oracle: &oracle,
                    predicate: |_id, _value| true,
                },
                update_int32_edit(1, 1),
                UpdateInt32PairStamp {
                    xid: Xid::new(20),
                    command_id: CommandId::new(command_id),
                },
                None,
                None,
            )
        };
        let bulk = |snapshot: &Snapshot, command_id: u32| {
            heap.update_int32_pair_inplace_undo(
                update_int32_scan(
                    rel(),
                    heap.block_count(rel()),
                    snapshot,
                    &oracle,
                    |_id, _value| true,
                ),
                update_int32_edit(1, 1),
                UpdateInt32PairStamp {
                    xid: Xid::new(20),
                    command_id: CommandId::new(command_id),
                },
                None,
                None,
            )
        };

        match shape {
            "point-point" => {
                assert_eq!(point(&first, 0).unwrap(), 1);
                assert_eq!(point(&second, 1).unwrap(), 1);
            }
            "point-bulk" => {
                assert_eq!(point(&first, 0).unwrap(), 1);
                assert_eq!(bulk(&second, 1).unwrap(), 1);
            }
            "bulk-point" => {
                assert_eq!(bulk(&first, 0).unwrap(), 1);
                assert_eq!(point(&second, 1).unwrap(), 1);
            }
            _ => unreachable!(),
        }
        assert_eq!(heap.fetch(tid).unwrap().data, int32_pair_payload(1, 12));
        assert_eq!(heap.rollback_in_place_updates(Xid::new(20)).unwrap(), 2);
        assert_eq!(
            heap.fetch(tid).unwrap().data,
            int32_pair_payload(1, 10),
            "shape={shape}"
        );
    }
}

#[test]
fn delete_after_committed_inplace_update_is_not_lost_and_preserves_old_snapshots() {
    // Regression for a silent LOST DELETE: deleting a row whose slot bytes
    // are an in-place-update post-image used to leave UPDATED_IN_PLACE set,
    // so the deleter's xmax read as "just another in-place update" and every
    // snapshot kept seeing the row forever. The delete stamp now swaps the
    // flag for INPLACE_HISTORY: new snapshots see the delete; snapshots that
    // predate the UPDATE still observe the pre-update payload via undo.
    let heap = make_heap(8);
    heap.insert(rel(), &int32_pair_payload(1, 10), opts(10))
        .unwrap();

    let oracle = MapOracle::new();
    oracle.set_committed(Xid::new(10));

    // In-place UPDATE by xid 20 (val 10 -> 15), then commit it.
    oracle.set_in_progress(Xid::new(20));
    let writer_20 = Snapshot::new(
        Xid::new(10),
        Xid::new(100),
        Xid::new(20),
        CommandId::FIRST,
        std::iter::empty(),
    );
    heap.update_int32_pair_inplace_undo(
        update_int32_scan(
            rel(),
            heap.block_count(rel()),
            &writer_20,
            &oracle,
            |id, _val| id == 1,
        ),
        update_int32_edit(1, 5),
        update_int32_stamp(20),
        None,
        None,
    )
    .unwrap();
    oracle.set_committed(Xid::new(20));

    // DELETE by xid 30 through the fused path, then commit it.
    oracle.set_in_progress(Xid::new(30));
    let deleter_30 = Snapshot::new(
        Xid::new(10),
        Xid::new(100),
        Xid::new(30),
        CommandId::FIRST,
        std::iter::empty(),
    );
    let deleted = heap
        .delete_int32_pair_inplace(
            DeleteInt32PairScan {
                rel: rel(),
                block_count: heap.block_count(rel()),
                snapshot: &deleter_30,
                oracle: &oracle,
                predicate: |id: i32, _val: i32| id == 1,
            },
            DeleteInt32PairStamp {
                xid: Xid::new(30),
                command_id: CommandId::FIRST,
            },
            None,
            None,
        )
        .unwrap();
    assert_eq!(deleted, 1, "the fused delete must find the updated row");
    oracle.set_committed(Xid::new(30));

    // A NEW snapshot (sees both commits) must observe the delete.
    let after_all = Snapshot::new(
        Xid::new(10),
        Xid::new(100),
        Xid::new(40),
        CommandId::FIRST,
        std::iter::empty(),
    );
    let visible: Vec<HeapTuple> = heap
        .scan_visible(rel(), heap.block_count(rel()), &after_all, &oracle)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(visible.is_empty(), "the delete must not be silently lost");

    // A snapshot that predates BOTH the update and the delete still sees
    // the ORIGINAL payload (undo pre-image through INPLACE_HISTORY).
    let before_update = Snapshot::new(
        Xid::new(10),
        Xid::new(100),
        Xid::new(15),
        CommandId::FIRST,
        [Xid::new(20), Xid::new(30)],
    );
    let visible: Vec<HeapTuple> = heap
        .scan_visible(rel(), heap.block_count(rel()), &before_update, &oracle)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(visible.len(), 1, "pre-update snapshot still sees the row");
    assert_eq!(
        int32_pair_from_payload(&visible[0].data),
        (1, 10),
        "pre-update snapshot must observe the pre-update payload"
    );

    // A snapshot between the two commits (sees the update, not the delete)
    // observes the post-update payload.
    let between = Snapshot::new(
        Xid::new(10),
        Xid::new(100),
        Xid::new(25),
        CommandId::FIRST,
        [Xid::new(30)],
    );
    let visible: Vec<HeapTuple> = heap
        .scan_visible(rel(), heap.block_count(rel()), &between, &oracle)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(visible.len(), 1);
    assert_eq!(
        int32_pair_from_payload(&visible[0].data),
        (1, 15),
        "between-commits snapshot must observe the post-update payload"
    );
}

#[test]
fn update_after_committed_inplace_update_succeeds_and_new_value_is_visible() {
    // Sibling scenario of the lost-DELETE regression above, UPDATE flavor:
    // a committed in-place UPDATE permanently leaves the writer's stamp in
    // `xmax` (with UPDATED_IN_PLACE marking the slot as the CURRENT
    // version — vacuum keeps the slot for exactly that reason). A later
    // general (non-fused) UPDATE of that same, visible row must treat the
    // stale writer stamp as "alive current version", not as "deleted
    // tuple". The executor reaches this path whenever the fused int32-pair
    // rewrite does not apply, with the TID coming from a visible scan.
    let heap = make_heap(8);
    let tid = heap
        .insert(rel(), &int32_pair_payload(1, 10), opts(10))
        .unwrap();

    let oracle = MapOracle::new();
    oracle.set_committed(Xid::new(10));

    // In-place UPDATE by xid 20 (val 10 -> 15), then commit it.
    oracle.set_in_progress(Xid::new(20));
    let writer_20 = Snapshot::new(
        Xid::new(10),
        Xid::new(100),
        Xid::new(20),
        CommandId::FIRST,
        std::iter::empty(),
    );
    heap.update_int32_pair_inplace_undo(
        update_int32_scan(
            rel(),
            heap.block_count(rel()),
            &writer_20,
            &oracle,
            |id, _val| id == 1,
        ),
        update_int32_edit(1, 5),
        update_int32_stamp(20),
        None,
        None,
    )
    .unwrap();
    oracle.set_committed(Xid::new(20));

    // General UPDATE by xid 30 on the same slot: (1, 15) -> (1, 99).
    oracle.set_in_progress(Xid::new(30));
    let outcome = heap
        .update(tid, &int32_pair_payload(1, 99), update_opts(30))
        .expect("general update over a committed in-place update must succeed");
    assert_eq!(outcome.old_tid, tid);
    oracle.set_committed(Xid::new(30));

    // A snapshot that sees all three commits observes exactly one row,
    // carrying the general update's new value.
    let after_all = Snapshot::new(
        Xid::new(10),
        Xid::new(100),
        Xid::new(40),
        CommandId::FIRST,
        std::iter::empty(),
    );
    let visible: Vec<HeapTuple> = heap
        .scan_visible(rel(), heap.block_count(rel()), &after_all, &oracle)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        visible.len(),
        1,
        "exactly one version must be visible after the general update"
    );
    assert_eq!(
        int32_pair_from_payload(&visible[0].data),
        (1, 99),
        "the general update's new value must be visible"
    );
}
