//! WAL record emission for heap insert, batch-insert, update (HOT and
//! non-HOT), delete, and the null/absent sink cases.

use std::sync::Arc;

use ultrasql_core::{CommandId, PageId, BlockNumber, Xid};
use ultrasql_mvcc::tuple_header::TUPLE_HEADER_SIZE;
use ultrasql_wal::payload::{
    HEAP_UPDATE_HOT, HeapDeletePayload, HeapInsertBatchPayload, HeapInsertPayload,
    HeapUpdatePayload,
};
use ultrasql_wal::record::RecordType;

use super::{make_heap_with_sink, rel};
use crate::heap::tests::make_heap;
use crate::heap::{DeleteOptions, InsertOptions, UpdateOptions};
use crate::wal_sink::{NullWalSink, test_support::InMemoryWalSink};

// -------------------------------------------------------------------
// 1. insert emits HeapInsert with expected payload
// -------------------------------------------------------------------

#[test]
fn insert_emits_heap_insert_record_with_expected_payload() {
    let (heap, sink) = make_heap_with_sink(8);

    let tid = heap
        .insert(
            rel(),
            b"hello wal",
            InsertOptions {
                xmin: Xid::new(10),
                command_id: CommandId::FIRST,
                n_atts: 0,
                fsm: None,
                vm: None,
                wal: Some(sink.as_ref()),
            },
        )
        .unwrap();

    assert_eq!(sink.len(), 1, "expected one WAL record");
    let records = sink.records();
    let (_lsn, record) = &records[0];
    assert_eq!(record.header.record_type, RecordType::HeapInsert);
    assert_eq!(record.header.xid, Xid::new(10));

    // Decode the payload and verify tid.
    let payload = HeapInsertPayload::decode(&record.payload).unwrap();
    assert_eq!(payload.tid, tid, "WAL payload tid must match returned tid");

    // tuple_bytes must match what heap.fetch returns.
    let fetched = heap.fetch(tid).unwrap();
    let mut expected_bytes = vec![0_u8; TUPLE_HEADER_SIZE + fetched.data.len()];
    fetched
        .header
        .encode(&mut expected_bytes[..TUPLE_HEADER_SIZE]);
    expected_bytes[TUPLE_HEADER_SIZE..].copy_from_slice(&fetched.data);

    assert_eq!(
        payload.tuple_bytes, expected_bytes,
        "WAL tuple_bytes must match on-page canonical bytes"
    );
}

#[test]
fn insert_batch_emits_one_batch_record_per_page() {
    let (heap, sink) = make_heap_with_sink(8);
    let rows = [b"a".as_slice(), b"bb".as_slice(), b"ccc".as_slice()];
    let tids = heap
        .insert_batch(
            rel(),
            &rows,
            InsertOptions {
                xmin: Xid::new(10),
                command_id: CommandId::FIRST,
                n_atts: 0,
                fsm: None,
                vm: None,
                wal: Some(sink.as_ref()),
            },
        )
        .unwrap();

    assert_eq!(tids.len(), rows.len());
    assert_eq!(sink.len(), 1, "one heap page should emit one batch record");
    let records = sink.records();
    let (_lsn, record) = &records[0];
    assert_eq!(record.header.record_type, RecordType::HeapInsertBatch);
    assert_eq!(record.header.xid, Xid::new(10));

    let payload = HeapInsertBatchPayload::decode(&record.payload).unwrap();
    assert_eq!(payload.page, PageId::new(rel(), BlockNumber::new(0)));
    assert_eq!(payload.entries.len(), rows.len());
    for (entry, tid) in payload.entries.iter().zip(&tids) {
        assert_eq!(entry.slot, tid.slot);
        let fetched = heap.fetch(*tid).unwrap();
        let mut expected_bytes = vec![0_u8; TUPLE_HEADER_SIZE + fetched.data.len()];
        fetched
            .header
            .encode(&mut expected_bytes[..TUPLE_HEADER_SIZE]);
        expected_bytes[TUPLE_HEADER_SIZE..].copy_from_slice(&fetched.data);
        assert_eq!(entry.tuple_bytes, expected_bytes);
    }
}

// -------------------------------------------------------------------
// 2. HOT update emits HeapUpdate with HOT flag set
// -------------------------------------------------------------------

#[test]
fn update_emits_heap_update_record_with_hot_flag() {
    let (heap, sink) = make_heap_with_sink(16);

    let old_tid = heap
        .insert(
            rel(),
            b"original",
            InsertOptions {
                xmin: Xid::new(1),
                command_id: CommandId::FIRST,
                n_atts: 0,
                fsm: None,
                vm: None,
                wal: Some(sink.as_ref()),
            },
        )
        .unwrap();

    // Use a fresh sink so only the update record appears.
    let sink2 = Arc::new(InMemoryWalSink::new());

    let outcome = heap
        .update(
            old_tid,
            b"updated",
            UpdateOptions {
                xid: Xid::new(2),
                command_id: CommandId::FIRST,
                hot_eligible: true,
                wal: Some(sink2.as_ref()),
                vm: None,
            },
        )
        .unwrap();

    assert!(
        outcome.hot,
        "expected HOT update for small tuple on fresh page"
    );
    assert_eq!(sink2.len(), 1, "expected exactly one update record");

    let records = sink2.records();
    let (_lsn, record) = &records[0];
    assert_eq!(record.header.record_type, RecordType::HeapUpdate);

    let payload = HeapUpdatePayload::decode(&record.payload).unwrap();
    assert_eq!(payload.old_tid, outcome.old_tid);
    assert_eq!(payload.new_tid, outcome.new_tid);
    assert_ne!(payload.flags & HEAP_UPDATE_HOT, 0, "HOT flag must be set");
}

// -------------------------------------------------------------------
// 3. Non-HOT update does not have HOT flag
// -------------------------------------------------------------------

#[test]
fn update_emits_heap_update_record_without_hot_flag_when_falling_back() {
    let (heap, sink) = make_heap_with_sink(32);

    // Fill the page with a large tuple so there is no room for another.
    let big = [0xBB_u8; 7000];
    let old_tid = heap
        .insert(
            rel(),
            &big,
            InsertOptions {
                xmin: Xid::new(1),
                command_id: CommandId::FIRST,
                n_atts: 0,
                fsm: None,
                vm: None,
                wal: None,
            },
        )
        .unwrap();
    // Second large insert forces block 1 to be allocated.
    let _ = heap
        .insert(
            rel(),
            &big,
            InsertOptions {
                xmin: Xid::new(1),
                command_id: CommandId::FIRST,
                n_atts: 0,
                fsm: None,
                vm: None,
                wal: None,
            },
        )
        .unwrap();

    // Update the first tuple; page is full so it falls back to non-HOT.
    let outcome = heap
        .update(
            old_tid,
            &big,
            UpdateOptions {
                xid: Xid::new(2),
                command_id: CommandId::FIRST,
                hot_eligible: true, // asked for HOT but page is full
                wal: Some(sink.as_ref()),
                vm: None,
            },
        )
        .unwrap();

    assert!(!outcome.hot, "expected non-HOT fall-back when page is full");
    assert_ne!(
        outcome.old_tid.page.block, outcome.new_tid.page.block,
        "new version must be on a different block"
    );

    assert_eq!(sink.len(), 1);
    let records = sink.records();
    let (_lsn, record) = &records[0];
    let payload = HeapUpdatePayload::decode(&record.payload).unwrap();
    assert_eq!(
        payload.flags & HEAP_UPDATE_HOT,
        0,
        "HOT flag must NOT be set"
    );
}

// -------------------------------------------------------------------
// 4. delete emits HeapDelete
// -------------------------------------------------------------------

#[test]
fn delete_emits_heap_delete_record() {
    let (heap, sink) = make_heap_with_sink(8);

    let tid = heap
        .insert(
            rel(),
            b"to-delete",
            InsertOptions {
                xmin: Xid::new(10),
                command_id: CommandId::FIRST,
                n_atts: 0,
                fsm: None,
                vm: None,
                wal: None,
            },
        )
        .unwrap();

    heap.delete(
        tid,
        DeleteOptions {
            xmax: Xid::new(20),
            cmax: CommandId::new(3),
            fsm: None,
            vm: None,
            wal: Some(sink.as_ref()),
        },
    )
    .unwrap();

    assert_eq!(sink.len(), 1, "expected one delete record");
    let records = sink.records();
    let (_lsn, record) = &records[0];
    assert_eq!(record.header.record_type, RecordType::HeapDelete);
    assert_eq!(record.header.xid, Xid::new(20));

    let payload = HeapDeletePayload::decode(&record.payload).unwrap();
    assert_eq!(payload.tid, tid);
    assert_eq!(payload.xmax, Xid::new(20));
    assert_eq!(payload.cmax, CommandId::new(3));
}

// -------------------------------------------------------------------
// 5. NullWalSink drops records silently
// -------------------------------------------------------------------

#[test]
fn null_sink_drops_records_silently() {
    let heap = make_heap(8);
    let null = NullWalSink;

    // Should not panic; NullWalSink always returns Ok(Lsn::ZERO).
    let tid = heap
        .insert(
            rel(),
            b"test",
            InsertOptions {
                xmin: Xid::new(1),
                command_id: CommandId::FIRST,
                n_atts: 0,
                fsm: None,
                vm: None,
                wal: Some(&null),
            },
        )
        .unwrap();

    // The tuple must be readable even when the sink discards the record.
    let got = heap.fetch(tid).unwrap();
    assert_eq!(got.data, b"test");
}

// -------------------------------------------------------------------
// 6. wal: None emits nothing
// -------------------------------------------------------------------

#[test]
fn wal_sink_none_emits_nothing() {
    let (heap, sink) = make_heap_with_sink(8);

    // Insert without WAL — the provided sink should receive zero records.
    let tid = heap
        .insert(
            rel(),
            b"no-wal",
            InsertOptions {
                xmin: Xid::new(5),
                command_id: CommandId::FIRST,
                n_atts: 0,
                fsm: None,
                vm: None,
                wal: None,
            },
        )
        .unwrap();

    // Delete with a *separate* sink to confirm it gets one record
    // while the insert-side sink got zero.
    let del_sink = Arc::new(InMemoryWalSink::new());
    heap.delete(
        tid,
        DeleteOptions {
            xmax: Xid::new(6),
            cmax: CommandId::FIRST,
            fsm: None,
            vm: None,
            wal: Some(del_sink.as_ref()),
        },
    )
    .unwrap();

    assert_eq!(sink.len(), 0, "no-WAL insert must emit zero records");
    assert_eq!(del_sink.len(), 1, "delete with sink must emit one record");
}
