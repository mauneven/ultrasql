//! Deliverable B: page-LSN stamping after WAL-backed insert, update
//! (HOT and cross-page non-HOT), and delete.

use ultrasql_core::{CommandId, Xid};

use super::{make_heap_with_sink, rel};
use crate::heap::{InsertOptions, DeleteOptions, UpdateOptions};

// -------------------------------------------------------------------
// LSN stamping tests (Deliverable B)
// -------------------------------------------------------------------

/// After a heap insert with a WAL sink, the page's `header.lsn`
/// must equal the LSN returned by the sink's `append`.
#[test]
fn insert_stamps_page_lsn_to_wal_append_lsn() {
    let (heap, sink) = make_heap_with_sink(8);
    let xid = Xid::new(10);

    let tid = heap
        .insert(
            rel(),
            b"lsn-test",
            InsertOptions {
                xmin: xid,
                command_id: CommandId::FIRST,
                n_atts: 0,
                fsm: None,
                vm: None,
                wal: Some(sink.as_ref()),
            },
        )
        .unwrap();

    // The sink assigned LSN 1 to the first record.
    let records = sink.records();
    let (expected_lsn, _) = records[0];

    // Read the page directly from the pool and check the header LSN.
    let guard = heap.pool.get_page(tid.page).unwrap();
    let page_lsn = guard.read().header().lsn;
    assert_eq!(
        page_lsn,
        expected_lsn.raw(),
        "page LSN must equal WAL append LSN after insert"
    );
}

/// For a HOT update, both the old and new tuples live on the same
/// page. That page's LSN must equal the LSN from the update's WAL
/// append.
///
/// For a non-HOT update, both the old page and the new page must
/// be stamped with the same WAL append LSN.
#[test]
fn update_stamps_new_and_old_pages_when_different() {
    // Use a large payload to force non-HOT (cross-page) update.
    let (heap, sink) = make_heap_with_sink(32);
    let big = [0xCC_u8; 7000];

    // Insert the first tuple with no WAL to keep the sink clean.
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
    // Force block 1 to exist so the update has a non-HOT destination.
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

    let outcome = heap
        .update(
            old_tid,
            &big,
            UpdateOptions {
                xid: Xid::new(2),
                command_id: CommandId::FIRST,
                hot_eligible: true, // hot requested but page is full
                wal: Some(sink.as_ref()),
                vm: None,
            },
        )
        .unwrap();

    assert!(
        !outcome.hot,
        "expected non-HOT update; old and new should be on different pages"
    );
    assert_ne!(outcome.old_tid.page, outcome.new_tid.page);

    let records = sink.records();
    let (expected_lsn, _) = records[0];

    // Both pages must be stamped with the same LSN.
    let old_guard = heap.pool.get_page(outcome.old_tid.page).unwrap();
    let old_lsn = old_guard.read().header().lsn;
    let new_guard = heap.pool.get_page(outcome.new_tid.page).unwrap();
    let new_lsn = new_guard.read().header().lsn;

    assert_eq!(
        old_lsn,
        expected_lsn.raw(),
        "old page LSN must equal WAL update LSN"
    );
    assert_eq!(
        new_lsn,
        expected_lsn.raw(),
        "new page LSN must equal WAL update LSN"
    );
}

/// After a heap delete with a WAL sink, the page's `header.lsn`
/// must equal the LSN returned by the sink's `append`.
#[test]
fn delete_stamps_page_lsn() {
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
                wal: None, // no WAL for insert; clean sink for delete
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

    let records = sink.records();
    let (expected_lsn, _) = records[0];

    let guard = heap.pool.get_page(tid.page).unwrap();
    let page_lsn = guard.read().header().lsn;
    assert_eq!(
        page_lsn,
        expected_lsn.raw(),
        "page LSN must equal WAL delete LSN after delete"
    );
}
