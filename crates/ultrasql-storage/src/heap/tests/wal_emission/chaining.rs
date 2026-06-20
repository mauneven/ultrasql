//! prev_lsn chaining within a transaction, WAL append-failure handling,
//! and the monotonic-chain property test.

use proptest::prelude::*;
use ultrasql_core::{CommandId, Lsn, Xid};
use ultrasql_mvcc::status::test_support::MapOracle;
use ultrasql_mvcc::Snapshot;
use ultrasql_wal::WalRecord;

use super::{make_heap_with_sink, rel};
use crate::buffer_pool::BufferPoolError;
use crate::heap::tests::{int32_pair_from_payload, int32_pair_payload, make_heap, opts, usize_to_u8};
use crate::heap::{DeleteInt32PairScan, DeleteInt32PairStamp, HeapError, HeapTuple, InsertOptions};
use crate::wal_sink::{WalSink, WalSinkError};

// -------------------------------------------------------------------
// 7. prev_lsn chains within a xid
// -------------------------------------------------------------------

#[test]
fn last_lsn_chains_within_xid() {
    let (heap, sink) = make_heap_with_sink(8);
    let xid = Xid::new(77);

    // First insert: prev_lsn should be Lsn::ZERO (no prior record).
    let _t1 = heap
        .insert(
            rel(),
            b"first",
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

    let records_snapshot = sink.records();
    let (lsn1, rec1) = &records_snapshot[0];
    assert_eq!(
        rec1.header.prev_lsn,
        ultrasql_core::Lsn::ZERO,
        "first record prev_lsn must be ZERO"
    );

    // Second insert for the same xid: prev_lsn must equal lsn1.
    let _t2 = heap
        .insert(
            rel(),
            b"second",
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

    let records = sink.records();
    let (_lsn2, rec2) = &records[1];
    assert_eq!(
        rec2.header.prev_lsn, *lsn1,
        "second record prev_lsn must equal first record lsn"
    );
}

// -------------------------------------------------------------------
// 8. Property test: prev_lsn chain is monotonic for a fixed xid
// -------------------------------------------------------------------

// -------------------------------------------------------------------
// 9. WAL append failure after a committed page mutation is reported
// -------------------------------------------------------------------

/// A WAL sink that always rejects every record. Used to verify that
/// the heap propagates WAL sink failure instead of panicking.
struct RejectingWalSink;

impl WalSink for RejectingWalSink {
    fn append(&self, _record: WalRecord) -> Result<Lsn, WalSinkError> {
        Err(WalSinkError::Rejected(
            "test: sink intentionally rejects all records".into(),
        ))
    }

    fn durable_lsn(&self) -> Lsn {
        Lsn::ZERO
    }

    fn last_lsn_for(&self, _xid: Xid) -> Lsn {
        Lsn::ZERO
    }
}

struct NonBlockingRejectingWalSink;

impl WalSink for NonBlockingRejectingWalSink {
    fn append(&self, _record: WalRecord) -> Result<Lsn, WalSinkError> {
        Err(WalSinkError::Rejected(
            "test: buffered sink intentionally rejects all records".into(),
        ))
    }

    fn appends_without_blocking_io(&self) -> bool {
        true
    }

    fn durable_lsn(&self) -> Lsn {
        Lsn::ZERO
    }

    fn last_lsn_for(&self, _xid: Xid) -> Lsn {
        Lsn::ZERO
    }
}

#[test]
fn wal_append_failure_during_insert_returns_wal_error() {
    let heap = make_heap(8);
    let sink = RejectingWalSink;

    let err = heap
        .insert(
            rel(),
            b"will-write-then-wal-fail",
            InsertOptions {
                xmin: Xid::new(42),
                command_id: CommandId::FIRST,
                n_atts: 0,
                fsm: None,
                vm: None,
                wal: Some(&sink),
            },
        )
        .unwrap_err();
    assert!(
        matches!(err, HeapError::Wal(WalSinkError::Rejected(_))),
        "heap insert should return Wal error, got {err:?}"
    );

    let poisoned = heap.insert(rel(), b"blocked-after-wal-failure", opts(43));
    assert!(
        matches!(
            poisoned,
            Err(HeapError::BufferPool(BufferPoolError::Poisoned))
        ),
        "heap should reject later page access after WAL failure, got {poisoned:?}"
    );
}

#[test]
fn buffered_delete_wal_failure_before_stamp_keeps_heap_usable() {
    let heap = make_heap(8);
    heap.insert(rel(), &int32_pair_payload(1, 10), opts(10))
        .unwrap();

    let oracle = MapOracle::new();
    oracle.set_committed(Xid::new(10));
    let snapshot = Snapshot::new(
        Xid::new(10),
        Xid::new(100),
        Xid::new(20),
        CommandId::FIRST,
        std::iter::empty(),
    );
    let sink = NonBlockingRejectingWalSink;
    let err = heap
        .delete_int32_pair_inplace(
            DeleteInt32PairScan {
                rel: rel(),
                block_count: heap.block_count(rel()),
                snapshot: &snapshot,
                oracle: &oracle,
                predicate: |id, _val| id == 1,
            },
            DeleteInt32PairStamp {
                xid: Xid::new(20),
                command_id: CommandId::FIRST,
            },
            Some(&sink),
            None,
        )
        .unwrap_err();
    assert!(matches!(err, HeapError::Wal(WalSinkError::Rejected(_))));

    let alive: Vec<HeapTuple> = heap
        .scan_visible(rel(), heap.block_count(rel()), &snapshot, &oracle)
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(alive.len(), 1);
    assert_eq!(int32_pair_from_payload(&alive[0].data), (1, 10));

    heap.insert(rel(), &int32_pair_payload(2, 20), opts(30))
        .expect("before-mutation WAL failure must not poison heap");
}

proptest! {
    #[test]
    fn prop_prev_lsn_chain_monotonic(
        n in 2_usize..=20,
    ) {
        let (heap, sink) = make_heap_with_sink(256);
        let xid = Xid::new(42);

        for i in 0..n {
            let payload = usize_to_u8(i).to_le_bytes();
            heap.insert(
                rel(),
                &payload,
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
        }

        let records = sink.records();
        prop_assert_eq!(records.len(), n);

        // For each record after the first, prev_lsn must equal the
        // LSN assigned to the immediately preceding record.
        for i in 1..n {
            let j = i - 1;
            let (prev_lsn, _) = &records[j];
            let (_, cur_rec) = &records[i];
            prop_assert_eq!(
                cur_rec.header.prev_lsn,
                *prev_lsn,
                "record[{}].prev_lsn must equal records[{}].lsn",
                i, j
            );
        }
    }
}
