//! Deliverable A: HOT-chain updates and in-place int32-pair update tests.

use ultrasql_core::{CommandId, Xid};
use ultrasql_mvcc::Snapshot;
use ultrasql_mvcc::status::test_support::MapOracle;
use ultrasql_mvcc::tuple_header::InfoMask;

use super::*;

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
