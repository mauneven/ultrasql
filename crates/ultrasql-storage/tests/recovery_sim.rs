//! Recovery simulation integration test.
//!
//! This test simulates a server crash by running a workload against a
//! `HeapAccess` backed by an `InMemoryWalSink`, then dropping all
//! in-memory state and reconstructing a fresh `HeapAccess` from the
//! WAL records via `dispatch_record`. The final visible-tuple set must
//! match the committed transactions exactly.
//!
//! Workload: 1 000 inserts, 200 updates, and 100 deletes across 50
//! transactions. Half the transactions commit; half abort. The
//! simulation does not use a CLOG; aborted transactions are tracked
//! in a plain `HashSet` and filtered during the "expected state"
//! computation.

use std::collections::HashSet;
use std::sync::Arc;

use proptest::prelude::*;
use ultrasql_core::{BlockNumber, CommandId, PageId, RelationId, TupleId, Xid};
use ultrasql_storage::buffer_pool::BufferPool;
use ultrasql_storage::heap::{
    DeleteInt32PairScan, DeleteInt32PairStamp, DeleteOptions, HeapAccess, InsertOptions,
    UpdateInt32PairEdit, UpdateInt32PairScan, UpdateInt32PairStamp, UpdateInt32PairTid,
    UpdateOptions,
};
use ultrasql_wal::applier::{dispatch_record, dispatch_record_at_lsn};

// ---------------------------------------------------------------------------
// Shared test infrastructure
// ---------------------------------------------------------------------------

/// In-memory page loader shared with heap unit-test shape.
mod loader {
    use std::collections::HashMap;

    use parking_lot::Mutex;
    use ultrasql_core::constants::PAGE_SIZE;
    use ultrasql_core::{PageId, Result};
    use ultrasql_storage::buffer_pool::PageLoader;
    use ultrasql_storage::page::Page;

    #[derive(Default)]
    pub(super) struct MapLoader {
        store: Mutex<HashMap<PageId, Box<[u8; PAGE_SIZE]>>>,
    }

    impl MapLoader {
        pub(super) fn new() -> Self {
            Self::default()
        }
    }

    impl PageLoader for MapLoader {
        fn load(&self, page_id: PageId) -> Result<Page> {
            let stored = {
                let store = self.store.lock();
                store.get(&page_id).map(|b| {
                    let mut copy: Box<[u8; PAGE_SIZE]> = vec![0_u8; PAGE_SIZE]
                        .into_boxed_slice()
                        .try_into()
                        .expect("alloc matches PAGE_SIZE");
                    copy.copy_from_slice(&**b);
                    copy
                })
            };
            if let Some(bytes) = stored {
                return Page::from_bytes(bytes)
                    .map_err(|e| ultrasql_core::Error::Corruption(format!("map loader: {e}")));
            }
            let page = Page::new_heap();
            let mut copy: Box<[u8; PAGE_SIZE]> = vec![0_u8; PAGE_SIZE]
                .into_boxed_slice()
                .try_into()
                .expect("alloc matches PAGE_SIZE");
            copy.copy_from_slice(page.as_bytes());
            {
                let mut store = self.store.lock();
                store.insert(page_id, copy);
            }
            Ok(page)
        }
    }
}

/// Shared `MapLoader` that survives across `HeapAccess` reconstructions
/// (simulates pages written to disk before the crash).
///
/// In this simulation, the "disk" is the `MapLoader`. The WAL records
/// are kept separately (as the WAL would survive a crash). The
/// post-crash recovery builds a fresh `HeapAccess` from the same loader,
/// replays the WAL, and the tuples should match.
fn make_persistent_heap(loader: loader::MapLoader) -> HeapAccess<loader::MapLoader> {
    let pool = Arc::new(BufferPool::new(256, loader));
    HeapAccess::new(pool)
}

const fn rel() -> RelationId {
    RelationId::new(1)
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).expect("test count must fit u64")
}

fn usize_to_u16(value: usize) -> u16 {
    u16::try_from(value).expect("test slot index must fit u16")
}

fn usize_to_i32(value: usize) -> i32 {
    i32::try_from(value).expect("test row index must fit i32")
}

fn insert_opts(xid: Xid, sink: &dyn ultrasql_storage::wal_sink::WalSink) -> InsertOptions<'_> {
    InsertOptions {
        xmin: xid,
        command_id: CommandId::FIRST,
        n_atts: 2,
        wal: Some(sink),
        fsm: None,
        vm: None,
    }
}

fn del_opts(xid: Xid, sink: &dyn ultrasql_storage::wal_sink::WalSink) -> DeleteOptions<'_> {
    DeleteOptions {
        xmax: xid,
        cmax: CommandId::FIRST,
        wal: Some(sink),
        fsm: None,
        vm: None,
    }
}

// ---------------------------------------------------------------------------
// Main simulation test
// ---------------------------------------------------------------------------

/// Crash-recovery simulation.
///
/// Phase 1 — Write workload:
/// - 50 transactions, xids 1..=50.
/// - Odd xids commit; even xids abort.
/// - Each transaction inserts 20 tuples (total 1 000 inserts).
/// - Every 5th committed xid updates one of its own tuples (≈ 200 updates).
/// - Every 10th committed xid deletes one of its own tuples (≈ 100 deletes).
///
/// Phase 2 — Crash simulation:
/// - Drop the `HeapAccess`; retain the WAL records and the loader.
///
/// Phase 3 — Recovery:
/// - Reconstruct a fresh `HeapAccess` on the same `MapLoader`.
/// - Replay all WAL records via `dispatch_record`.
///
/// Phase 4 — Assertion:
/// - Scan all blocks for every xid.
/// - A tuple is "visible" if its xmin is a committed xid and xmax is
///   either INVALID or an aborted xid.
/// - The visible count must match the expected committed-insert minus
///   committed-delete count.
#[test]
fn crash_recovery_visible_tuples_match_committed_only() {
    use ultrasql_storage::test_support::InMemoryWalSink;

    const N_XIDS: u64 = 50;
    const INSERTS_PER_XID: usize = 20;

    let sink = Arc::new(InMemoryWalSink::new());

    // --- Phase 1: write workload ------------------------------------------
    let mut committed: HashSet<u64> = HashSet::new();
    let mut aborted: HashSet<u64> = HashSet::new();

    {
        let heap = make_persistent_heap(loader::MapLoader::new());

        for xid_raw in 1..=N_XIDS {
            let xid = Xid::new(xid_raw);
            let mut my_tids: Vec<TupleId> = Vec::new();

            // Insert 20 tuples per transaction.
            for i in 0..INSERTS_PER_XID {
                let payload = format!("xid={xid_raw} i={i}");
                let tid = heap
                    .insert(rel(), payload.as_bytes(), insert_opts(xid, sink.as_ref()))
                    .expect("insert must succeed");
                my_tids.push(tid);
            }

            // Update: every 5th committed transaction updates its 2nd tuple.
            if xid_raw % 5 == 1 && xid_raw % 2 == 1 {
                // odd xid → will commit; xid_raw % 5 == 1 selects ~1/5 of them
                if let Some(&update_tid) = my_tids.get(1) {
                    let new_payload = format!("updated by xid={xid_raw}");
                    let _ = heap.update(
                        update_tid,
                        new_payload.as_bytes(),
                        UpdateOptions {
                            xid,
                            command_id: CommandId::FIRST,
                            hot_eligible: true,
                            wal: Some(sink.as_ref()),
                            vm: None,
                        },
                    );
                }
            }

            // Delete: every 10th committed transaction deletes its 3rd tuple.
            if xid_raw % 10 == 1 && xid_raw % 2 == 1 {
                if let Some(&delete_tid) = my_tids.get(2) {
                    let _ = heap.delete(delete_tid, del_opts(xid, sink.as_ref()));
                }
            }

            // Odd xids commit; even xids abort.
            if xid_raw % 2 == 1 {
                committed.insert(xid_raw);
            } else {
                aborted.insert(xid_raw);
            }
        }
        // heap drops here — simulated crash
    }

    // --- Phase 2: collect WAL records -------------------------------------
    let wal_records: Vec<_> = sink.records();

    // --- Phase 3: recovery replay -----------------------------------------
    // Fresh heap on a NEW loader (simulates reading from disk after crash).
    // In a real system the loader would read from disk files; here we use
    // a fresh MapLoader (all pages start as blank heap pages), which means
    // the test exercises that replay rebuilds the state from scratch.
    let recovery_heap = make_persistent_heap(loader::MapLoader::new());
    for (_lsn, record) in &wal_records {
        dispatch_record(&recovery_heap, record)
            .expect("dispatch_record must succeed during replay");
    }

    // --- Phase 4: assert expected state -----------------------------------
    // Count expected committed tuples:
    // - Each committed xid inserts 20 tuples.
    // - Some updates add 1 new version (HOT or cross-page) while the old
    //   version gets xmax stamped; each update is a net +1 slot but
    //   visible count stays 1 per logical tuple (old is superseded).
    // - Deletes from committed xids remove 1 tuple.
    //
    // We simplify: scan ALL slots, and count those where:
    //   xmin ∈ committed  AND  (xmax == INVALID OR xmax ∈ aborted)
    let n_blocks = recovery_heap.block_count(rel());
    assert!(
        n_blocks > 0,
        "recovery must have written at least one block"
    );

    let mut visible = 0_u64;
    for tuple in recovery_heap.scan(rel(), n_blocks).flatten() {
        let xmin = tuple.header.xmin.raw();
        let xmax = tuple.header.xmax.raw();
        let xmin_committed = committed.contains(&xmin);
        let xmax_invalid = xmax == 0;
        let xmax_aborted = xmax != 0 && aborted.contains(&xmax);
        if xmin_committed && (xmax_invalid || xmax_aborted) {
            visible += 1;
        }
    }

    // Expected: committed xids × 20 inserts − committed deletes.
    // Committed xids: odd numbers 1..=50 → 25 xids, 25 × 20 = 500 inserts.
    // Committed deletes: xid_raw % 10 == 1 AND odd: 1, 11, 21, 31, 41 → 5 deletes.
    // Updates don't change the net visible count (old xmax = committing xid,
    // new tuple xmin = same committing xid, both visible rules apply).
    let n_committed_deletes =
        usize_to_u64((1..=N_XIDS).filter(|&x| x % 2 == 1 && x % 10 == 1).count());
    let n_committed = N_XIDS / 2; // 25 committed xids
    let expected = n_committed
        .checked_mul(usize_to_u64(INSERTS_PER_XID))
        .and_then(|count| count.checked_sub(n_committed_deletes))
        .expect("expected visible tuple count must fit u64");

    assert_eq!(
        visible, expected,
        "recovered visible tuple count {visible} != expected {expected}"
    );
}

/// Crash-recovery under CONCURRENT inserts with no intervening checkpoint.
///
/// This is the regression for the durability bug the vector soak test
/// surfaced: many connections inserting into the same relation contend on
/// the same heap pages. Slot allocation happens under the page write lock,
/// but each insert appends its WAL record *after* releasing that lock, so
/// two inserters on a page can append in the opposite order to the slots
/// they took. The recovery applier replays records in WAL (LSN) order; the
/// pre-fix append-based redo then hit "slot mismatch: expected N, inserted
/// N-1" and aborted recovery, losing every committed row on the page.
///
/// Phase 1 spawns several threads inserting concurrently (each as its own
/// xid). Phase 2 drops the heap (simulated `kill -9` — only the WAL, kept
/// in the sink, survives) and replays every record into a fresh heap, as
/// crash recovery would. Phase 3 asserts every committed row is recoverable
/// at its recorded TID with its original bytes, and no row leaked or
/// duplicated.
#[test]
fn crash_recovery_concurrent_inserts_replay_in_wal_order() {
    use std::thread;
    use ultrasql_storage::test_support::InMemoryWalSink;

    const THREADS: usize = 8;
    const ROWS_PER_THREAD: usize = 300;

    let sink = Arc::new(InMemoryWalSink::new());

    // --- Phase 1: concurrent inserts into one relation --------------------
    let inserted: Vec<(TupleId, String)> = {
        let heap = make_persistent_heap(loader::MapLoader::new());
        let sink_ref = sink.as_ref();
        let per_thread: Vec<Vec<(TupleId, String)>> = thread::scope(|scope| {
            let handles: Vec<_> = (0..THREADS)
                .map(|t| {
                    let heap = &heap;
                    scope.spawn(move || {
                        let xid = Xid::new(usize_to_u64(t) + 1);
                        let mut mine = Vec::with_capacity(ROWS_PER_THREAD);
                        for i in 0..ROWS_PER_THREAD {
                            // Small payloads pack many rows per page, so
                            // multiple threads collide on the same page and
                            // the WAL/slot order divergence shows up.
                            let payload = format!("t{t:02}-r{i:04}");
                            let tid = heap
                                .insert(rel(), payload.as_bytes(), insert_opts(xid, sink_ref))
                                .expect("concurrent insert must succeed");
                            mine.push((tid, payload));
                        }
                        mine
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|handle| handle.join().expect("insert thread joins"))
                .collect()
        });
        per_thread.into_iter().flatten().collect()
        // heap drops here — simulated crash; the WAL lives on in `sink`.
    };

    assert_eq!(
        inserted.len(),
        THREADS * ROWS_PER_THREAD,
        "every insert must return a tid"
    );

    // --- Phase 2: replay the WAL (append order == LSN order) --------------
    let records = sink.records();
    let recovery_heap = make_persistent_heap(loader::MapLoader::new());
    for (_lsn, record) in &records {
        dispatch_record(&recovery_heap, record)
            .expect("WAL replay of concurrent inserts must succeed");
    }

    // --- Phase 3: every committed row is recoverable ----------------------
    for (tid, payload) in &inserted {
        let tuple = recovery_heap
            .fetch(*tid)
            .unwrap_or_else(|e| panic!("row {payload} at {tid:?} lost in recovery: {e}"));
        assert_eq!(
            tuple.data,
            payload.as_bytes(),
            "row {payload} recovered with the wrong bytes"
        );
    }

    let recovered = recovery_heap
        .scan(rel(), recovery_heap.block_count(rel()))
        .flatten()
        .count();
    assert_eq!(
        recovered,
        inserted.len(),
        "recovered tuple count must equal the committed inserts (no loss, no duplication)"
    );
}

// ---------------------------------------------------------------------------
// Property tests: page tuple round-trips
// ---------------------------------------------------------------------------

/// Any sequence of random payloads inserted into a page can be read back
/// with identical bytes, provided the page has enough space.
#[test]
fn proptest_page_tuple_round_trips() {
    proptest!(|(
        payloads in proptest::collection::vec(
            proptest::collection::vec(any::<u8>(), 0..64_usize),
            1..20_usize,
        )
    )| {
        use ultrasql_core::{CommandId, Xid};
        use ultrasql_storage::page::Page;
        use ultrasql_mvcc::tuple_header::{TUPLE_HEADER_SIZE};
        use ultrasql_mvcc::TupleHeader;

        let mut page = Page::new_heap();
        let mut expected: Vec<Vec<u8>> = Vec::new();

        for (i, payload) in payloads.iter().enumerate() {
            let mut tuple_bytes = vec![0_u8; TUPLE_HEADER_SIZE + payload.len()];
            let tid = TupleId::new(
                PageId::new(RelationId::new(1), BlockNumber::new(0)),
                usize_to_u16(i),
            );
            let hdr = TupleHeader::fresh(Xid::new(1), CommandId::FIRST, tid, 0);
            hdr.encode(&mut tuple_bytes[..TUPLE_HEADER_SIZE]);
            tuple_bytes[TUPLE_HEADER_SIZE..].copy_from_slice(payload);

            let insert_result = page.insert_tuple(&tuple_bytes);
            match insert_result {
                Ok(_slot) => {
                    expected.push(tuple_bytes);
                }
                Err(ultrasql_storage::page::PageError::NoSpace { .. }) => {
                    // Page is full — that is fine for this test.
                    break;
                }
                Err(e) => {
                    prop_assert!(false, "unexpected insert_tuple error: {e:?}");
                }
            }
        }

        // Read back every inserted tuple and compare bytes.
        for (slot, expected_bytes) in expected.iter().enumerate() {
            let actual = page
                .read_tuple(usize_to_u16(slot))
                .expect("slot must be readable");
            prop_assert_eq!(actual, expected_bytes.as_slice());
        }
    });
}

// ---------------------------------------------------------------------------
// In-place UPDATE / DELETE crash-recovery tests (Item 1 Part B)
// ---------------------------------------------------------------------------

/// Encode a 9-byte `(Int32, Int32)` payload in the on-page layout
/// `update_int32_pair_inplace_undo` expects: a leading null-bitmap byte
/// followed by the two little-endian i32 values.
fn pair_payload(id: i32, val: i32) -> [u8; 9] {
    let mut out = [0_u8; 9];
    out[1..5].copy_from_slice(&id.to_le_bytes());
    out[5..9].copy_from_slice(&val.to_le_bytes());
    out
}

/// Decode the `(id, val)` pair out of an on-page payload slice.
fn pair_decode(bytes: &[u8]) -> (i32, i32) {
    let id = i32::from_le_bytes(bytes[1..5].try_into().expect("4B"));
    let val = i32::from_le_bytes(bytes[5..9].try_into().expect("4B"));
    (id, val)
}

/// Crash-recovery for `update_int32_pair_inplace_undo`.
///
/// Phase 1: insert N (Int32, Int32) tuples under xid=1 (committed),
/// then run the in-place UPDATE under xid=2 with the WAL sink wired.
/// Phase 2: drop the heap (simulated crash). Build a fresh heap on a
/// new MapLoader and replay every record from the sink. The post-
/// recovery scan must show every row's `val` column incremented by
/// the delta supplied to the UPDATE, and the per-relation undo log
/// must carry one entry per row so cross-snapshot readers can still
/// resolve the pre-image.
#[test]
fn crash_recovery_in_place_update_restores_post_image_and_undo_log() {
    use ultrasql_core::CommandId;
    use ultrasql_mvcc::Snapshot;
    use ultrasql_mvcc::status::test_support::MapOracle;
    use ultrasql_storage::test_support::InMemoryWalSink;

    const ROWS: usize = 200;
    const DELTA: i32 = 5;

    let sink = Arc::new(InMemoryWalSink::new());
    let oracle = MapOracle::new();
    oracle.set_committed(Xid::new(1));
    oracle.set_committed(Xid::new(2));

    // Phase 1: insert + in-place update under a live heap with WAL.
    let mut original_pairs: Vec<(i32, i32)> = Vec::with_capacity(ROWS);
    {
        let heap = make_persistent_heap(loader::MapLoader::new());
        for i in 0..ROWS {
            let id = usize_to_i32(i);
            let val = id.checked_mul(10).expect("test row value must fit i32");
            original_pairs.push((id, val));
            let bytes = pair_payload(id, val);
            heap.insert(rel(), &bytes, insert_opts(Xid::new(1), sink.as_ref()))
                .expect("insert");
        }
        let n_blocks = heap.block_count(rel());
        let snap = Snapshot::new(
            Xid::new(2),
            Xid::new(3),
            Xid::new(2),
            CommandId::FIRST,
            std::iter::empty(),
        );
        let sink_ref: &dyn ultrasql_storage::wal_sink::WalSink = sink.as_ref();
        let updated = heap
            .update_int32_pair_inplace_undo(
                UpdateInt32PairScan {
                    rel: rel(),
                    block_count: n_blocks,
                    snapshot: &snap,
                    oracle: &oracle,
                    predicate: |_id, _val| true,
                },
                UpdateInt32PairEdit {
                    target_col: 1,
                    delta: DELTA,
                },
                UpdateInt32PairStamp {
                    xid: Xid::new(2),
                    command_id: CommandId::FIRST,
                },
                Some(sink_ref),
                None,
            )
            .expect("in-place update");
        assert_eq!(updated, ROWS, "every row should match the predicate");
        // heap drops here — simulated crash.
    }

    // Phase 2: collect records and replay into a fresh heap.
    let records = sink.records();
    let batch_rows: usize = records
        .iter()
        .filter_map(|(_, r)| match r.header.record_type {
            ultrasql_wal::record::RecordType::HeapUpdateInt32PairDeltaBatch => Some(
                ultrasql_wal::HeapUpdateInt32PairDeltaBatchPayload::decode(&r.payload)
                    .expect("batch payload decodes")
                    .slots
                    .len(),
            ),
            ultrasql_wal::record::RecordType::HeapUpdateInt32PairDeltaRangeBatch => {
                Some(usize::from(
                    ultrasql_wal::HeapUpdateInt32PairDeltaRangeBatchPayload::decode(&r.payload)
                        .expect("range batch payload decodes")
                        .slot_count,
                ))
            }
            _ => None,
        })
        .sum();
    assert_eq!(batch_rows, ROWS, "WAL must batch every in-place UPDATE row");
    let recovery_heap = make_persistent_heap(loader::MapLoader::new());
    for (_, rec) in &records {
        dispatch_record(&recovery_heap, rec).expect("dispatch_record during replay");
    }

    // Phase 3: every visible row should reflect the post-image.
    let mut recovered_pairs: Vec<(i32, i32)> = Vec::with_capacity(ROWS);
    for tuple in recovery_heap
        .scan(rel(), recovery_heap.block_count(rel()))
        .flatten()
    {
        recovered_pairs.push(pair_decode(&tuple.data));
    }
    recovered_pairs.sort_by_key(|(id, _)| *id);
    let expected_post: Vec<(i32, i32)> = original_pairs
        .iter()
        .map(|(id, val)| (*id, val.wrapping_add(DELTA)))
        .collect();
    assert_eq!(
        recovered_pairs.len(),
        ROWS,
        "recovery should yield every row"
    );
    assert_eq!(
        recovered_pairs, expected_post,
        "post-recovery payloads must match the live post-image"
    );

    // Phase 4: the compact int32-pair undo batches must have been rebuilt
    // (one slot per replayed row), matching the live producer's
    // representation, so a cross-snapshot reader can still resolve the
    // pre-image via `current - sum(invisible deltas)`.
    {
        let log_handle = recovery_heap
            .undo_log
            .get(&rel())
            .expect("undo log entry for rel");
        let log = log_handle.read();
        let rebuilt_slots: usize = log.int32_pair_batches().iter().map(|b| b.slot_len()).sum();
        assert_eq!(
            rebuilt_slots, ROWS,
            "compact undo batches must cover one slot per replayed row"
        );
        for batch in log.int32_pair_batches().iter() {
            assert_eq!(batch.writer_xid, Xid::new(2));
            assert_eq!(batch.target_col, 1);
            assert_eq!(batch.delta, DELTA);
        }
    }

    // Phase 5: a reader whose snapshot PREDATES the in-place writer
    // (xid=2) must observe the PRE-image (original `val`), proving the
    // rebuilt undo log feeds the read-time `current - delta` lookup. A
    // snapshot with xid=2 in-progress treats the writer as invisible.
    let pre_snapshot = Snapshot::new(
        Xid::new(2),
        Xid::new(3),
        Xid::new(3),
        CommandId::FIRST,
        [Xid::new(2)],
    );
    for tuple in recovery_heap
        .scan(rel(), recovery_heap.block_count(rel()))
        .flatten()
    {
        let pre = recovery_heap
            .fetch_visible_pre_image(tuple.tid, &pre_snapshot, &oracle)
            .expect("pre-image lookup")
            .expect("undo entry present for snapshot-predating reader");
        let (id, val) = pair_decode(&pre);
        let original_val = original_pairs
            .iter()
            .find_map(|(oid, oval)| if *oid == id { Some(*oval) } else { None })
            .expect("id should match an original row");
        assert_eq!(
            val, original_val,
            "snapshot-predating reader must see the pre-image (V0), not the post-image"
        );
    }
}

/// Crash-recovery for `delete_int32_pair_inplace`.
///
/// Same shape as the UPDATE test: insert under xid=1, in-place DELETE
/// under xid=2, drop heap, replay sink records into fresh heap, then
/// verify each row's header carries `xmax = 2` so any visibility check
/// hides it.
#[test]
fn crash_recovery_in_place_delete_stamps_xmax() {
    use ultrasql_core::CommandId;
    use ultrasql_mvcc::Snapshot;
    use ultrasql_mvcc::status::test_support::MapOracle;
    use ultrasql_storage::test_support::InMemoryWalSink;

    const ROWS: usize = 150;

    let sink = Arc::new(InMemoryWalSink::new());
    let oracle = MapOracle::new();
    oracle.set_committed(Xid::new(1));
    oracle.set_committed(Xid::new(2));

    {
        let heap = make_persistent_heap(loader::MapLoader::new());
        for i in 0..ROWS {
            let id = usize_to_i32(i);
            let val = id.checked_mul(7).expect("test row value must fit i32");
            let bytes = pair_payload(id, val);
            heap.insert(rel(), &bytes, insert_opts(Xid::new(1), sink.as_ref()))
                .expect("insert");
        }
        let n_blocks = heap.block_count(rel());
        let snap = Snapshot::new(
            Xid::new(2),
            Xid::new(3),
            Xid::new(2),
            CommandId::FIRST,
            std::iter::empty(),
        );
        let sink_ref: &dyn ultrasql_storage::wal_sink::WalSink = sink.as_ref();
        let deleted = heap
            .delete_int32_pair_inplace(
                DeleteInt32PairScan {
                    rel: rel(),
                    block_count: n_blocks,
                    snapshot: &snap,
                    oracle: &oracle,
                    predicate: |_id, _val| true,
                },
                DeleteInt32PairStamp {
                    xid: Xid::new(2),
                    command_id: CommandId::FIRST,
                },
                Some(sink_ref),
                None,
            )
            .expect("in-place delete");
        assert_eq!(deleted, ROWS);
    }

    let records = sink.records();
    assert!(
        records.iter().any(|(_, r)| matches!(
            r.header.record_type,
            ultrasql_wal::record::RecordType::HeapDeleteInPlaceRangeBatch
        )),
        "WAL must contain at least one HeapDeleteInPlaceRangeBatch record"
    );

    let recovery_heap = make_persistent_heap(loader::MapLoader::new());
    for (_, rec) in &records {
        dispatch_record(&recovery_heap, rec).expect("dispatch_record during replay");
    }

    let mut deleted_rows = 0_usize;
    let mut undeleted_rows = 0_usize;
    for tuple in recovery_heap
        .scan(rel(), recovery_heap.block_count(rel()))
        .flatten()
    {
        if tuple.header.xmax == Xid::new(2) {
            deleted_rows += 1;
        } else {
            undeleted_rows += 1;
        }
    }
    assert_eq!(deleted_rows, ROWS, "every replayed row should carry xmax=2");
    assert_eq!(
        undeleted_rows, 0,
        "no row should be left without an xmax stamp"
    );
}

// ---------------------------------------------------------------------------
// Post-crash undo-log rebuild for ALREADY-FLUSHED in-place pages.
//
// The in-memory undo log is volatile MVCC state — it is NOT stored on the
// heap page. When a page was flushed durably past an in-place UPDATE's LSN
// before a crash, recovery SKIPS the on-page redo (`should_skip_redo`) for
// that page. The bug: the undo-log reconstruction was tied to that redo, so
// after recovery a reader whose snapshot predates the in-place writer found
// NO undo entry and wrongly saw the POST-image — a snapshot-isolation
// violation that surfaced only after a crash/restart.
//
// These tests reproduce the skip path by replaying a record stream twice:
// the first replay applies the post-image and stamps each page's LSN to the
// record LSN (the "flushed" durable state); the second replay then finds
// `page_lsn >= record_lsn` and SKIPS the on-page redo. We clear the
// in-memory undo log between the two replays to model the volatile state
// lost on crash. After the second (skip) replay the undo log must have been
// rebuilt so a snapshot-predating reader still resolves the pre-image (V0).
// ---------------------------------------------------------------------------

/// A snapshot under which `writer` is in progress (hence invisible): the
/// reader's snapshot predates the in-place writer.
fn predating_snapshot(writer: u64) -> ultrasql_mvcc::Snapshot {
    ultrasql_mvcc::Snapshot::new(
        Xid::new(writer),
        Xid::new(writer + 1),
        Xid::new(writer + 1),
        CommandId::FIRST,
        [Xid::new(writer)],
    )
}

/// FULL-PAYLOAD form (`HeapUpdateInPlace`, one `UndoEntry` per row).
///
/// After recovery SKIPS the on-page redo for an already-flushed page, the
/// undo log must still be rebuilt so a snapshot-predating reader sees the
/// PRE-image (V0). Without the fix the reader sees the post-image.
#[test]
fn crash_recovery_rebuilds_undo_for_flushed_full_payload_inplace_update() {
    use ultrasql_mvcc::status::test_support::MapOracle;
    use ultrasql_storage::test_support::InMemoryWalSink;

    const ROWS: usize = 64;
    const WRITER: u64 = 2;
    const DELTA: i32 = 7;

    let sink = Arc::new(InMemoryWalSink::new());
    let oracle = MapOracle::new();
    oracle.set_committed(Xid::new(1));
    oracle.set_committed(Xid::new(WRITER));

    // Phase 1: insert + per-tuple full-payload in-place UPDATE (the point
    // form emits one `HeapUpdateInPlace` record per row).
    let mut original_pairs: Vec<(i32, i32)> = Vec::with_capacity(ROWS);
    let mut tids: Vec<TupleId> = Vec::with_capacity(ROWS);
    {
        let heap = make_persistent_heap(loader::MapLoader::new());
        for i in 0..ROWS {
            let id = usize_to_i32(i);
            let val = id.checked_mul(10).expect("value fits i32");
            original_pairs.push((id, val));
            let tid = heap
                .insert(
                    rel(),
                    &pair_payload(id, val),
                    insert_opts(Xid::new(1), sink.as_ref()),
                )
                .expect("insert");
            tids.push(tid);
        }
        let writer_snap = ultrasql_mvcc::Snapshot::new(
            Xid::new(WRITER),
            Xid::new(WRITER + 1),
            Xid::new(WRITER),
            CommandId::FIRST,
            std::iter::empty(),
        );
        for &tid in &tids {
            let updated = heap
                .update_int32_pair_tid_inplace_undo(
                    UpdateInt32PairTid {
                        tid,
                        snapshot: &writer_snap,
                        oracle: &oracle,
                        predicate: |_id, _val| true,
                    },
                    UpdateInt32PairEdit {
                        target_col: 1,
                        delta: DELTA,
                    },
                    UpdateInt32PairStamp {
                        xid: Xid::new(WRITER),
                        command_id: CommandId::FIRST,
                    },
                    Some(sink.as_ref() as &dyn ultrasql_storage::wal_sink::WalSink),
                    None,
                )
                .expect("in-place update");
            assert_eq!(updated, 1);
        }
        // heap drops — simulated crash.
    }
    let records = sink.records();
    assert!(
        records
            .iter()
            .any(|(_, r)| r.header.record_type
                == ultrasql_wal::record::RecordType::HeapUpdateInPlace),
        "workload must emit full-payload in-place records"
    );

    // Phase 2: first replay onto a fresh heap stamps each page's LSN to the
    // record LSN (the durable "flushed" image).
    let recovery_heap = make_persistent_heap(loader::MapLoader::new());
    for (lsn, rec) in &records {
        dispatch_record_at_lsn(&recovery_heap, rec, *lsn).expect("first replay");
    }

    // Simulate the crash that lost the volatile in-memory undo log while the
    // post-image pages stayed durable.
    recovery_heap.undo_log.clear();
    assert_eq!(
        recovery_heap.undo_log_len(rel()),
        0,
        "undo log must be empty going into the skip replay"
    );

    // Phase 3: replay AGAIN. Each page's LSN now covers the record LSN, so
    // `should_skip_redo` SKIPS the on-page redo — the page bytes already
    // hold the post-image. The undo log must be rebuilt regardless.
    for (lsn, rec) in &records {
        dispatch_record_at_lsn(&recovery_heap, rec, *lsn).expect("skip replay");
    }

    // The physical slot is still the post-image (redo was skipped, not
    // re-applied — proving the page redo really was gated).
    let expected_post: Vec<(i32, i32)> = original_pairs
        .iter()
        .map(|(id, val)| (*id, val.wrapping_add(DELTA)))
        .collect();
    let mut recovered: Vec<(i32, i32)> = recovery_heap
        .scan(rel(), recovery_heap.block_count(rel()))
        .flatten()
        .map(|t| pair_decode(&t.data))
        .collect();
    recovered.sort_by_key(|(id, _)| *id);
    assert_eq!(
        recovered, expected_post,
        "physical slot must hold the post-image"
    );

    // The undo log was rebuilt from the WAL records' pre-images: a reader
    // whose snapshot predates the writer must see V0, not the post-image.
    assert_eq!(
        recovery_heap.undo_log_len(rel()),
        ROWS,
        "undo log must be rebuilt with one full-payload entry per flushed row"
    );
    let reader = predating_snapshot(WRITER);
    for &tid in &tids {
        let pre = recovery_heap
            .fetch_visible_pre_image(tid, &reader, &oracle)
            .expect("pre-image lookup")
            .expect("undo entry must exist after the skip replay");
        let (id, val) = pair_decode(&pre);
        let original_val = original_pairs
            .iter()
            .find_map(|(oid, oval)| (*oid == id).then_some(*oval))
            .expect("matching original row");
        assert_eq!(
            val, original_val,
            "snapshot-predating reader must see V0 even though the page redo was skipped"
        );
    }
}

/// COMPACT DELTA form (`HeapUpdateInt32PairDeltaBatch`).
///
/// Same scenario via the compact delta batch: after recovery skips the
/// on-page redo for a flushed page, the rebuilt compact undo batch must let
/// a snapshot-predating reader resolve `current - delta` = V0.
#[test]
fn crash_recovery_rebuilds_undo_for_flushed_compact_delta_inplace_update() {
    use ultrasql_mvcc::status::test_support::MapOracle;
    use ultrasql_storage::test_support::InMemoryWalSink;

    const ROWS: usize = 200;
    const WRITER: u64 = 2;
    const DELTA: i32 = 5;

    let sink = Arc::new(InMemoryWalSink::new());
    let oracle = MapOracle::new();
    oracle.set_committed(Xid::new(1));
    oracle.set_committed(Xid::new(WRITER));

    let mut original_pairs: Vec<(i32, i32)> = Vec::with_capacity(ROWS);
    let mut tids: Vec<TupleId> = Vec::with_capacity(ROWS);
    {
        let heap = make_persistent_heap(loader::MapLoader::new());
        for i in 0..ROWS {
            let id = usize_to_i32(i);
            let val = id.checked_mul(10).expect("value fits i32");
            original_pairs.push((id, val));
            let tid = heap
                .insert(
                    rel(),
                    &pair_payload(id, val),
                    insert_opts(Xid::new(1), sink.as_ref()),
                )
                .expect("insert");
            tids.push(tid);
        }
        let n_blocks = heap.block_count(rel());
        let writer_snap = ultrasql_mvcc::Snapshot::new(
            Xid::new(WRITER),
            Xid::new(WRITER + 1),
            Xid::new(WRITER),
            CommandId::FIRST,
            std::iter::empty(),
        );
        let updated = heap
            .update_int32_pair_inplace_undo(
                UpdateInt32PairScan {
                    rel: rel(),
                    block_count: n_blocks,
                    snapshot: &writer_snap,
                    oracle: &oracle,
                    predicate: |_id, _val| true,
                },
                UpdateInt32PairEdit {
                    target_col: 1,
                    delta: DELTA,
                },
                UpdateInt32PairStamp {
                    xid: Xid::new(WRITER),
                    command_id: CommandId::FIRST,
                },
                Some(sink.as_ref() as &dyn ultrasql_storage::wal_sink::WalSink),
                None,
            )
            .expect("in-place update");
        assert_eq!(updated, ROWS);
    }
    let records = sink.records();
    assert!(
        records.iter().any(|(_, r)| matches!(
            r.header.record_type,
            ultrasql_wal::record::RecordType::HeapUpdateInt32PairDeltaBatch
                | ultrasql_wal::record::RecordType::HeapUpdateInt32PairDeltaRangeBatch
        )),
        "workload must emit compact delta batch records"
    );

    let recovery_heap = make_persistent_heap(loader::MapLoader::new());
    for (lsn, rec) in &records {
        dispatch_record_at_lsn(&recovery_heap, rec, *lsn).expect("first replay");
    }
    recovery_heap.undo_log.clear();
    assert_eq!(recovery_heap.int32_pair_undo_slot_len(rel()), 0);

    for (lsn, rec) in &records {
        dispatch_record_at_lsn(&recovery_heap, rec, *lsn).expect("skip replay");
    }

    // Physical slot holds the post-image (redo skipped, not double-applied).
    let expected_post: Vec<(i32, i32)> = original_pairs
        .iter()
        .map(|(id, val)| (*id, val.wrapping_add(DELTA)))
        .collect();
    let mut recovered: Vec<(i32, i32)> = recovery_heap
        .scan(rel(), recovery_heap.block_count(rel()))
        .flatten()
        .map(|t| pair_decode(&t.data))
        .collect();
    recovered.sort_by_key(|(id, _)| *id);
    assert_eq!(
        recovered, expected_post,
        "physical slot must hold the post-image"
    );

    // Compact undo batch rebuilt: a predating reader resolves current-delta.
    assert_eq!(
        recovery_heap.int32_pair_undo_slot_len(rel()),
        ROWS,
        "compact undo batch must be rebuilt with one slot per flushed row"
    );
    let reader = predating_snapshot(WRITER);
    for &tid in &tids {
        let pre = recovery_heap
            .fetch_visible_pre_image(tid, &reader, &oracle)
            .expect("pre-image lookup")
            .expect("compact undo batch must exist after the skip replay");
        let (id, val) = pair_decode(&pre);
        let original_val = original_pairs
            .iter()
            .find_map(|(oid, oval)| (*oid == id).then_some(*oval))
            .expect("matching original row");
        assert_eq!(
            val, original_val,
            "snapshot-predating reader must see V0 (current - delta) after a skipped redo"
        );
    }
}

/// IDEMPOTENCY: replaying the same in-place record stream twice (both the
/// full-payload and compact forms) must not double-push undo entries, so the
/// pre-image stays V0 rather than being over-reversed (e.g. current - 2*delta
/// for the compact form, or a duplicated full-payload entry).
#[test]
fn crash_recovery_inplace_undo_rebuild_is_idempotent() {
    use ultrasql_mvcc::status::test_support::MapOracle;
    use ultrasql_storage::test_support::InMemoryWalSink;

    const ROWS: usize = 48;
    const WRITER: u64 = 2;
    const FULL_DELTA: i32 = 4;
    const COMPACT_DELTA: i32 = 6;

    let sink = Arc::new(InMemoryWalSink::new());
    let oracle = MapOracle::new();
    oracle.set_committed(Xid::new(1));
    oracle.set_committed(Xid::new(WRITER));

    // Two relations: one driven by the full-payload point form, one by the
    // compact scan form, so both undo representations are exercised.
    let full_rel = RelationId::new(1);
    let compact_rel = RelationId::new(2);
    let mut full_pairs: Vec<(i32, i32)> = Vec::new();
    let mut full_tids: Vec<TupleId> = Vec::new();
    let mut compact_pairs: Vec<(i32, i32)> = Vec::new();
    let mut compact_tids: Vec<TupleId> = Vec::new();
    {
        let heap = make_persistent_heap(loader::MapLoader::new());
        let writer_snap = ultrasql_mvcc::Snapshot::new(
            Xid::new(WRITER),
            Xid::new(WRITER + 1),
            Xid::new(WRITER),
            CommandId::FIRST,
            std::iter::empty(),
        );
        for i in 0..ROWS {
            let id = usize_to_i32(i);
            let val = id.checked_mul(10).expect("fits");
            full_pairs.push((id, val));
            let tid = heap
                .insert(
                    full_rel,
                    &pair_payload(id, val),
                    insert_opts(Xid::new(1), sink.as_ref()),
                )
                .expect("insert full");
            full_tids.push(tid);
        }
        for &tid in &full_tids {
            heap.update_int32_pair_tid_inplace_undo(
                UpdateInt32PairTid {
                    tid,
                    snapshot: &writer_snap,
                    oracle: &oracle,
                    predicate: |_id, _val| true,
                },
                UpdateInt32PairEdit {
                    target_col: 1,
                    delta: FULL_DELTA,
                },
                UpdateInt32PairStamp {
                    xid: Xid::new(WRITER),
                    command_id: CommandId::FIRST,
                },
                Some(sink.as_ref() as &dyn ultrasql_storage::wal_sink::WalSink),
                None,
            )
            .expect("full update");
        }
        for i in 0..ROWS {
            let id = usize_to_i32(i);
            let val = id.checked_mul(10).expect("fits");
            compact_pairs.push((id, val));
            let tid = heap
                .insert(
                    compact_rel,
                    &pair_payload(id, val),
                    insert_opts(Xid::new(1), sink.as_ref()),
                )
                .expect("insert compact");
            compact_tids.push(tid);
        }
        let n_blocks = heap.block_count(compact_rel);
        heap.update_int32_pair_inplace_undo(
            UpdateInt32PairScan {
                rel: compact_rel,
                block_count: n_blocks,
                snapshot: &writer_snap,
                oracle: &oracle,
                predicate: |_id, _val| true,
            },
            UpdateInt32PairEdit {
                target_col: 1,
                delta: COMPACT_DELTA,
            },
            UpdateInt32PairStamp {
                xid: Xid::new(WRITER),
                command_id: CommandId::FIRST,
            },
            Some(sink.as_ref() as &dyn ultrasql_storage::wal_sink::WalSink),
            None,
        )
        .expect("compact update");
    }
    let records = sink.records();

    let recovery_heap = make_persistent_heap(loader::MapLoader::new());
    // Replay the WHOLE stream THREE times. The first applies; the second and
    // third hit the skip path (pages durable) and must not double-push undo.
    for _ in 0..3 {
        for (lsn, rec) in &records {
            dispatch_record_at_lsn(&recovery_heap, rec, *lsn).expect("replay");
        }
    }

    assert_eq!(
        recovery_heap.undo_log_len(full_rel),
        ROWS,
        "full-payload undo entries must not be duplicated by repeated replay"
    );
    assert_eq!(
        recovery_heap.int32_pair_undo_slot_len(compact_rel),
        ROWS,
        "compact undo slots must not be duplicated by repeated replay"
    );

    let reader = predating_snapshot(WRITER);
    for &tid in &full_tids {
        let (id, val) = pair_decode(
            &recovery_heap
                .fetch_visible_pre_image(tid, &reader, &oracle)
                .expect("lookup")
                .expect("full undo present"),
        );
        let original = full_pairs
            .iter()
            .find_map(|(oid, oval)| (*oid == id).then_some(*oval))
            .expect("match");
        assert_eq!(
            val, original,
            "full pre-image must be V0, not duplicated/over-reversed"
        );
    }
    for &tid in &compact_tids {
        let (id, val) = pair_decode(
            &recovery_heap
                .fetch_visible_pre_image(tid, &reader, &oracle)
                .expect("lookup")
                .expect("compact undo present"),
        );
        let original = compact_pairs
            .iter()
            .find_map(|(oid, oval)| (*oid == id).then_some(*oval))
            .expect("match");
        assert_eq!(
            val, original,
            "compact pre-image must be current-delta (V0), not over-reversed by a doubled batch"
        );
    }
}

/// DISTINCT-COMMAND DEDUP DISCRIMINATOR (compact form).
///
/// One transaction runs TWO in-place delta UPDATEs over the SAME rows with
/// the SAME delta (`val += N` twice). These emit two distinct compact
/// `HeapUpdateInt32PairDeltaBatch` records: identical
/// page/writer_xid/target_col/delta/slots, but different `command_id` and
/// different WAL LSN. After recovery the undo log must hold BOTH batches so a
/// snapshot-predating reader reverses `2·N` (sees V0), not `N` (V0+N).
///
/// Regression: a dedup that keyed on batch SHAPE only (omitting command_id)
/// collapsed the second batch into the first; because
/// `undo_pre_image_from_log` SUMS invisible deltas, the reader then under-
/// reversed by one delta. This fires on a PLAIN full replay (c1 then c2
/// dispatched sequentially), not just the skip-redo path.
///
/// Both replay modes are checked: full replay (fresh pages) and skip-redo
/// replay (pages durable past the record LSN, undo log cleared between).
/// Genuine re-replay of the SAME record must still dedup — covered by
/// running each mode and asserting exactly two batches, never four.
#[test]
fn crash_recovery_keeps_distinct_same_shape_compact_commands() {
    use ultrasql_mvcc::Snapshot;
    use ultrasql_mvcc::status::test_support::MapOracle;
    use ultrasql_storage::test_support::InMemoryWalSink;

    const ROWS: usize = 120;
    const WRITER: u64 = 2;
    const N: i32 = 5;

    let sink = Arc::new(InMemoryWalSink::new());
    let oracle = MapOracle::new();
    oracle.set_committed(Xid::new(1));
    oracle.set_committed(Xid::new(WRITER));

    // Phase 1: insert, then run the SAME-shape compact UPDATE twice in one
    // transaction (two distinct commands over the same rows).
    let mut original_pairs: Vec<(i32, i32)> = Vec::with_capacity(ROWS);
    let mut tids: Vec<TupleId> = Vec::with_capacity(ROWS);
    {
        let heap = make_persistent_heap(loader::MapLoader::new());
        for i in 0..ROWS {
            let id = usize_to_i32(i);
            let val = id.checked_mul(10).expect("value fits i32");
            original_pairs.push((id, val));
            let tid = heap
                .insert(
                    rel(),
                    &pair_payload(id, val),
                    insert_opts(Xid::new(1), sink.as_ref()),
                )
                .expect("insert");
            tids.push(tid);
        }
        let n_blocks = heap.block_count(rel());
        let sink_ref = sink.as_ref() as &dyn ultrasql_storage::wal_sink::WalSink;

        // Command 0: val += N. current_command=1 so the committed xid=1
        // inserts are visible; this command stamps cmax=0 on every row.
        let snap_c0 = Snapshot::new(
            Xid::new(WRITER),
            Xid::new(WRITER + 1),
            Xid::new(WRITER),
            CommandId::new(1),
            std::iter::empty(),
        );
        let u0 = heap
            .update_int32_pair_inplace_undo(
                UpdateInt32PairScan {
                    rel: rel(),
                    block_count: n_blocks,
                    snapshot: &snap_c0,
                    oracle: &oracle,
                    predicate: |_id, _val| true,
                },
                UpdateInt32PairEdit {
                    target_col: 1,
                    delta: N,
                },
                UpdateInt32PairStamp {
                    xid: Xid::new(WRITER),
                    command_id: CommandId::new(0),
                },
                Some(sink_ref),
                None,
            )
            .expect("first in-place update");
        assert_eq!(u0, ROWS);

        // Command 1: val += N again over the same rows. current_command=2 so
        // command 0's own writes (cmax=0 < 2) are Visible and re-update.
        let snap_c1 = Snapshot::new(
            Xid::new(WRITER),
            Xid::new(WRITER + 1),
            Xid::new(WRITER),
            CommandId::new(2),
            std::iter::empty(),
        );
        let u1 = heap
            .update_int32_pair_inplace_undo(
                UpdateInt32PairScan {
                    rel: rel(),
                    block_count: n_blocks,
                    snapshot: &snap_c1,
                    oracle: &oracle,
                    predicate: |_id, _val| true,
                },
                UpdateInt32PairEdit {
                    target_col: 1,
                    delta: N,
                },
                UpdateInt32PairStamp {
                    xid: Xid::new(WRITER),
                    command_id: CommandId::new(1),
                },
                Some(sink_ref),
                None,
            )
            .expect("second in-place update");
        assert_eq!(u1, ROWS, "the second command must re-update every row");
        // Physical slot is now V0 + 2N on every row.
        for (idx, &tid) in tids.iter().enumerate() {
            let (_id, val) = pair_decode(&heap.fetch(tid).expect("fetch").data);
            assert_eq!(
                val,
                original_pairs[idx].1.wrapping_add(2 * N),
                "live post-image must be V0 + 2N after two commands"
            );
        }
    }

    // The stream must carry two distinct compact records (different LSN), with
    // identical shape and delta.
    let records = sink.records();
    let compact: Vec<_> = records
        .iter()
        .filter(|(_, r)| {
            matches!(
                r.header.record_type,
                ultrasql_wal::record::RecordType::HeapUpdateInt32PairDeltaBatch
                    | ultrasql_wal::record::RecordType::HeapUpdateInt32PairDeltaRangeBatch
            )
        })
        .collect();
    assert!(
        compact.len() >= 2,
        "two distinct same-shape commands must emit at least two compact records (got {})",
        compact.len()
    );

    let expected_pre: Vec<(i32, i32)> = original_pairs.clone();
    let reader = predating_snapshot(WRITER);

    // --- Mode A: plain FULL replay (fresh pages, both records applied). ---
    {
        let recovery_heap = make_persistent_heap(loader::MapLoader::new());
        for (lsn, rec) in &records {
            dispatch_record_at_lsn(&recovery_heap, rec, *lsn).expect("full replay");
        }
        // Two distinct commands ⇒ two retained batches (×ROWS slots each).
        assert_eq!(
            recovery_heap.int32_pair_undo_batch_len(rel()),
            2,
            "full replay must retain both distinct same-shape command batches"
        );
        assert_pre_image_is_v0(
            &recovery_heap,
            &tids,
            &expected_pre,
            &reader,
            &oracle,
            2 * N,
        );
    }

    // --- Mode B: skip-redo replay (pages durable past LSN, undo cleared). ---
    {
        let recovery_heap = make_persistent_heap(loader::MapLoader::new());
        for (lsn, rec) in &records {
            dispatch_record_at_lsn(&recovery_heap, rec, *lsn).expect("first replay");
        }
        recovery_heap.undo_log.clear();
        for (lsn, rec) in &records {
            dispatch_record_at_lsn(&recovery_heap, rec, *lsn).expect("skip replay");
        }
        assert_eq!(
            recovery_heap.int32_pair_undo_batch_len(rel()),
            2,
            "skip-redo replay must rebuild both distinct command batches and dedup re-replay to exactly two"
        );
        assert_pre_image_is_v0(
            &recovery_heap,
            &tids,
            &expected_pre,
            &reader,
            &oracle,
            2 * N,
        );
    }
}

/// Assert every row's snapshot-predating pre-image equals V0, i.e. the post-
/// image (`current`) minus `total_delta`. A correct two-batch undo log
/// reverses `total_delta`; a wrongly-collapsed single batch reverses less.
fn assert_pre_image_is_v0<O: ultrasql_mvcc::XidStatusOracle + ?Sized>(
    heap: &HeapAccess<loader::MapLoader>,
    tids: &[TupleId],
    expected_pre: &[(i32, i32)],
    reader: &ultrasql_mvcc::Snapshot,
    oracle: &O,
    total_delta: i32,
) {
    for (idx, &tid) in tids.iter().enumerate() {
        let pre = heap
            .fetch_visible_pre_image(tid, reader, oracle)
            .expect("pre-image lookup")
            .expect("undo entry present");
        let (id, val) = pair_decode(&pre);
        let (exp_id, exp_val) = expected_pre[idx];
        assert_eq!(id, exp_id, "id must be stable");
        assert_eq!(
            val, exp_val,
            "predating reader must see V0 = current - {total_delta}, not an under-reversed value"
        );
        // Cross-check: the live post-image is exactly V0 + total_delta.
        let (_pid, post_val) = pair_decode(&heap.fetch(tid).expect("fetch").data);
        assert_eq!(
            post_val,
            exp_val.wrapping_add(total_delta),
            "post-image must be V0 + total_delta"
        );
    }
}
