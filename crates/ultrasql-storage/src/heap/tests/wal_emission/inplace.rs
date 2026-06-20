//! WAL emission for in-place int32-pair batch updates and deletes,
//! including sparse vs. range encodings, parallel WAL chaining, and
//! payload-stats interaction.

use ultrasql_core::{BlockNumber, CommandId, Lsn, PageId, Xid};
use ultrasql_mvcc::Snapshot;
use ultrasql_mvcc::status::test_support::MapOracle;
use ultrasql_wal::payload::{
    HeapDeleteInPlaceBatchPayload, HeapDeleteInPlaceRangeBatchPayload,
    HeapUpdateInt32PairDeltaRangeBatchPayload,
};
use ultrasql_wal::record::RecordType;

use super::{make_heap_with_sink, rel};
use crate::heap::delete::{DeleteSlotWalScratch, DeleteSlotWalView};
use crate::heap::tests::{
    int32_pair_payload, make_heap, opts, update_int32_edit, update_int32_scan, update_int32_stamp,
};
use crate::heap::{
    DeleteInt32PairScan, DeleteInt32PairStamp, Int32PairCmp, Int32PairPredicate,
    UpdateInt32PairEdit, UpdateInt32PairScan, UpdateInt32PairStamp,
};

#[test]
fn inplace_int32_update_emits_one_batch_record_per_page() {
    let (heap, sink) = make_heap_with_sink(8);
    for id in 0_i32..3 {
        heap.insert(rel(), &int32_pair_payload(id, id * 10), opts(10))
            .unwrap();
    }

    let oracle = MapOracle::new();
    oracle.set_committed(Xid::new(10));
    let snapshot = Snapshot::new(
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
                &snapshot,
                &oracle,
                |_id, _val| true,
            ),
            update_int32_edit(1, 1),
            update_int32_stamp(20),
            Some(sink.as_ref()),
            None,
        )
        .unwrap();

    assert_eq!(updated, 3);
    assert_eq!(sink.len(), 1, "one page should emit one batch record");
    let records = sink.records();
    let (_lsn, record) = &records[0];
    assert_eq!(
        record.header.record_type,
        RecordType::HeapUpdateInt32PairDeltaRangeBatch
    );
    let payload = HeapUpdateInt32PairDeltaRangeBatchPayload::decode(&record.payload).unwrap();
    assert_eq!(payload.page, PageId::new(rel(), BlockNumber::new(0)));
    assert_eq!(payload.writer_xid, Xid::new(20));
    assert_eq!(payload.command_id, CommandId::FIRST);
    assert_eq!(payload.target_col, 1);
    assert_eq!(payload.delta, 1);
    assert_eq!(payload.first_slot, 0);
    assert_eq!(payload.slot_count, 3);
}

#[test]
fn inplace_int32_delete_emits_one_batch_record_per_page() {
    let (heap, sink) = make_heap_with_sink(8);
    for id in 0_i32..3 {
        heap.insert(rel(), &int32_pair_payload(id, id * 10), opts(10))
            .unwrap();
    }

    let oracle = MapOracle::new();
    oracle.set_committed(Xid::new(10));
    let snapshot = Snapshot::new(
        Xid::new(10),
        Xid::new(100),
        Xid::new(20),
        CommandId::FIRST,
        std::iter::empty(),
    );
    let deleted = heap
        .delete_int32_pair_inplace(
            DeleteInt32PairScan {
                rel: rel(),
                block_count: heap.block_count(rel()),
                snapshot: &snapshot,
                oracle: &oracle,
                predicate: |_id, _val| true,
            },
            DeleteInt32PairStamp {
                xid: Xid::new(20),
                command_id: CommandId::FIRST,
            },
            Some(sink.as_ref()),
            None,
        )
        .unwrap();

    assert_eq!(deleted, 3);
    assert_eq!(sink.len(), 1, "one page should emit one batch record");
    let records = sink.records();
    let (_lsn, record) = &records[0];
    assert_eq!(
        record.header.record_type,
        RecordType::HeapDeleteInPlaceRangeBatch
    );
    let payload = HeapDeleteInPlaceRangeBatchPayload::decode(&record.payload).unwrap();
    assert_eq!(payload.page, PageId::new(rel(), BlockNumber::new(0)));
    assert_eq!(payload.xmax, Xid::new(20));
    assert_eq!(payload.cmax, CommandId::FIRST);
    assert_eq!(payload.first_slot, 0);
    assert_eq!(payload.slot_count, 3);
}

#[test]
fn inplace_int32_delete_keeps_sparse_batch_record() {
    let (heap, sink) = make_heap_with_sink(8);
    for id in 0_i32..3 {
        heap.insert(rel(), &int32_pair_payload(id, id * 10), opts(10))
            .unwrap();
    }

    let oracle = MapOracle::new();
    oracle.set_committed(Xid::new(10));
    let snapshot = Snapshot::new(
        Xid::new(10),
        Xid::new(100),
        Xid::new(20),
        CommandId::FIRST,
        std::iter::empty(),
    );
    let deleted = heap
        .delete_int32_pair_inplace(
            DeleteInt32PairScan {
                rel: rel(),
                block_count: heap.block_count(rel()),
                snapshot: &snapshot,
                oracle: &oracle,
                predicate: |id, _val| id != 1,
            },
            DeleteInt32PairStamp {
                xid: Xid::new(20),
                command_id: CommandId::FIRST,
            },
            Some(sink.as_ref()),
            None,
        )
        .unwrap();

    assert_eq!(deleted, 2);
    let records = sink.records();
    let (_lsn, record) = &records[0];
    assert_eq!(
        record.header.record_type,
        RecordType::HeapDeleteInPlaceBatch
    );
    let payload = HeapDeleteInPlaceBatchPayload::decode(&record.payload).unwrap();
    let slots: Vec<u16> = payload.entries.iter().map(|entry| entry.slot).collect();
    assert_eq!(slots, vec![0, 2]);
}

#[test]
fn parallel_wal_backed_int32_delete_preserves_wal_chain() {
    let (heap, sink) = make_heap_with_sink(512);
    let mut inserted = 0_i32;
    while heap.block_count(rel()) < 140 {
        heap.insert(rel(), &int32_pair_payload(inserted, inserted), opts(10))
            .unwrap();
        inserted += 1;
    }
    let block_count = heap.block_count(rel());

    let oracle = MapOracle::new();
    oracle.set_committed(Xid::new(10));
    let snapshot = Snapshot::new(
        Xid::new(10),
        Xid::new(100),
        Xid::new(20),
        CommandId::FIRST,
        std::iter::empty(),
    );
    let deleted = heap
        .delete_int32_pair_inplace_parallel_wal(
            DeleteInt32PairScan {
                rel: rel(),
                block_count,
                snapshot: &snapshot,
                oracle: &oracle,
                predicate: |_id, _val| true,
            },
            DeleteInt32PairStamp {
                xid: Xid::new(20),
                command_id: CommandId::FIRST,
            },
            sink.as_ref(),
            None,
        )
        .unwrap();

    assert_eq!(deleted, usize::try_from(inserted).unwrap());
    let records = sink.records();
    assert_eq!(records.len(), usize::try_from(block_count).unwrap());
    let mut prev_lsn = Lsn::ZERO;
    for (lsn, record) in records {
        assert_eq!(record.header.prev_lsn, prev_lsn);
        assert_eq!(
            record.header.record_type,
            RecordType::HeapDeleteInPlaceRangeBatch
        );
        prev_lsn = lsn;
    }
}

#[test]
fn delete_wal_slot_scratch_stays_range_until_sparse_break() {
    let mut scratch = DeleteSlotWalScratch::with_capacity(4);
    scratch.push(2).unwrap();
    scratch.push(3).unwrap();
    scratch.push(4).unwrap();
    assert_eq!(
        scratch.view(),
        DeleteSlotWalView::Range {
            first_slot: 2,
            slot_count: 3
        }
    );

    scratch.push(6).unwrap();
    assert_eq!(scratch.view(), DeleteSlotWalView::Sparse(&[2, 3, 4, 6]));

    scratch.clear();
    assert_eq!(scratch.view(), DeleteSlotWalView::Empty);
}

#[test]
fn inplace_int32_delete_keeps_payload_stats_across_header_only_delete() {
    let (heap, sink) = make_heap_with_sink(8);
    for id in 0_i32..3 {
        heap.insert(rel(), &int32_pair_payload(id, id * 10), opts(10))
            .unwrap();
    }

    let oracle = MapOracle::new();
    oracle.set_committed(Xid::new(10));
    oracle.set_aborted(Xid::new(20));
    let snapshot = Snapshot::new(
        Xid::new(10),
        Xid::new(100),
        Xid::new(20),
        CommandId::FIRST,
        std::iter::empty(),
    );
    let stamp = DeleteInt32PairStamp {
        xid: Xid::new(20),
        command_id: CommandId::FIRST,
    };

    assert_eq!(
        heap.delete_int32_pair_inplace(
            DeleteInt32PairScan {
                rel: rel(),
                block_count: heap.block_count(rel()),
                snapshot: &snapshot,
                oracle: &oracle,
                predicate: Int32PairPredicate::ColumnCmp {
                    col_index: 0,
                    op: Int32PairCmp::Lt,
                    literal: 3,
                },
            },
            stamp,
            Some(sink.as_ref()),
            None
        )
        .unwrap(),
        3
    );
    let page_id = PageId::new(rel(), BlockNumber::new(0));
    assert!(heap.int32_pair_payload_stats.contains_key(&page_id));

    assert_eq!(
        heap.delete_int32_pair_inplace(
            DeleteInt32PairScan {
                rel: rel(),
                block_count: heap.block_count(rel()),
                snapshot: &snapshot,
                oracle: &oracle,
                predicate: Int32PairPredicate::ColumnCmp {
                    col_index: 0,
                    op: Int32PairCmp::Lt,
                    literal: 3,
                },
            },
            stamp,
            Some(sink.as_ref()),
            None
        )
        .unwrap(),
        3
    );
    assert!(heap.int32_pair_payload_stats.contains_key(&page_id));
}

#[test]
fn inplace_int32_update_invalidates_payload_stats() {
    let (heap, sink) = make_heap_with_sink(8);
    for id in 0_i32..3 {
        heap.insert(rel(), &int32_pair_payload(id, id * 10), opts(10))
            .unwrap();
    }

    let oracle = MapOracle::new();
    oracle.set_committed(Xid::new(10));
    let snapshot = Snapshot::new(
        Xid::new(10),
        Xid::new(100),
        Xid::new(20),
        CommandId::FIRST,
        std::iter::empty(),
    );
    heap.delete_int32_pair_inplace(
        DeleteInt32PairScan {
            rel: rel(),
            block_count: heap.block_count(rel()),
            snapshot: &snapshot,
            oracle: &oracle,
            predicate: Int32PairPredicate::ColumnCmp {
                col_index: 0,
                op: Int32PairCmp::Lt,
                literal: 3,
            },
        },
        DeleteInt32PairStamp {
            xid: Xid::new(20),
            command_id: CommandId::FIRST,
        },
        Some(sink.as_ref()),
        None,
    )
    .unwrap();
    let page_id = PageId::new(rel(), BlockNumber::new(0));
    assert!(heap.int32_pair_payload_stats.contains_key(&page_id));

    heap.update_int32_pair_inplace_undo(
        UpdateInt32PairScan {
            rel: rel(),
            block_count: heap.block_count(rel()),
            snapshot: &snapshot,
            oracle: &oracle,
            predicate: |id, _val| id < 3,
        },
        UpdateInt32PairEdit {
            target_col: 1,
            delta: 1,
        },
        UpdateInt32PairStamp {
            xid: Xid::new(30),
            command_id: CommandId::FIRST,
        },
        Some(sink.as_ref()),
        None,
    )
    .unwrap();

    assert!(!heap.int32_pair_payload_stats.contains_key(&page_id));
}

#[test]
fn inplace_int32_delete_accepts_single_column_predicate() {
    let heap = make_heap(8);
    for id in 0_i32..4 {
        heap.insert(rel(), &int32_pair_payload(id, id * 10), opts(10))
            .unwrap();
    }

    let oracle = MapOracle::new();
    oracle.set_committed(Xid::new(10));
    let snapshot = Snapshot::new(
        Xid::new(10),
        Xid::new(100),
        Xid::new(20),
        CommandId::FIRST,
        std::iter::empty(),
    );
    let predicate = Int32PairPredicate::ColumnCmp {
        col_index: 0,
        op: Int32PairCmp::Lt,
        literal: 2,
    };
    assert_eq!(predicate.required_column(), Some(0));
    let deleted = heap
        .delete_int32_pair_inplace(
            DeleteInt32PairScan {
                rel: rel(),
                block_count: heap.block_count(rel()),
                snapshot: &snapshot,
                oracle: &oracle,
                predicate,
            },
            DeleteInt32PairStamp {
                xid: Xid::new(20),
                command_id: CommandId::FIRST,
            },
            None,
            None,
        )
        .unwrap();

    assert_eq!(deleted, 2);
}
