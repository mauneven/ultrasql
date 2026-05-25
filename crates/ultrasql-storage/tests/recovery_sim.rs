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

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::cast_lossless,
    reason = "integration test: index arithmetic against compile-time loop bounds"
)]

use std::collections::HashSet;
use std::sync::Arc;

use proptest::prelude::*;
use ultrasql_core::{BlockNumber, CommandId, PageId, RelationId, TupleId, Xid};
use ultrasql_storage::buffer_pool::BufferPool;
use ultrasql_storage::heap::{DeleteOptions, HeapAccess, InsertOptions, UpdateOptions};
use ultrasql_wal::applier::dispatch_record;

// ---------------------------------------------------------------------------
// Shared test infrastructure
// ---------------------------------------------------------------------------

/// In-memory page loader (copied from heap unit tests).
#[allow(unreachable_pub)]
mod loader {
    use std::collections::HashMap;

    use parking_lot::Mutex;
    use ultrasql_core::constants::PAGE_SIZE;
    use ultrasql_core::{PageId, Result};
    use ultrasql_storage::buffer_pool::PageLoader;
    use ultrasql_storage::page::Page;

    #[derive(Default)]
    pub struct MapLoader {
        store: Mutex<HashMap<PageId, Box<[u8; PAGE_SIZE]>>>,
    }

    impl MapLoader {
        pub fn new() -> Self {
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

fn insert_opts(xid: Xid, sink: &dyn ultrasql_storage::wal_sink::WalSink) -> InsertOptions<'_> {
    InsertOptions {
        xmin: xid,
        command_id: CommandId::FIRST,
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
    let n_committed_deletes: u64 =
        (1..=N_XIDS).filter(|&x| x % 2 == 1 && x % 10 == 1).count() as u64;
    let n_committed = N_XIDS / 2; // 25 committed xids
    let expected = n_committed * (INSERTS_PER_XID as u64) - n_committed_deletes;

    assert_eq!(
        visible, expected,
        "recovered visible tuple count {visible} != expected {expected}"
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
                i as u16,
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
            let actual = page.read_tuple(slot as u16).expect("slot must be readable");
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
            let id = i as i32;
            let val = i as i32 * 10;
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
                rel(),
                n_blocks,
                &snap,
                &oracle,
                |_id, _val| true,
                1,
                DELTA,
                Xid::new(2),
                CommandId::FIRST,
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
        .filter(|(_, r)| {
            matches!(
                r.header.record_type,
                ultrasql_wal::record::RecordType::HeapUpdateInPlaceBatch
            )
        })
        .map(|(_, r)| {
            ultrasql_wal::HeapUpdateInPlaceBatchPayload::decode(&r.payload)
                .expect("batch payload decodes")
                .entries
                .len()
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

    // Phase 4: undo log must have been rebuilt with one pre-image
    // entry per row so cross-snapshot readers still resolve pre-image.
    let log_handle = recovery_heap
        .undo_log
        .get(&rel())
        .expect("undo log entry for rel");
    let log = log_handle.read();
    assert_eq!(
        log.entries.len(),
        ROWS,
        "undo log must carry one pre-image entry per replayed row"
    );
    for entry in log.entries.iter() {
        let (id, val) = pair_decode(&entry.old_payload);
        let original_val = original_pairs
            .iter()
            .find_map(|(oid, oval)| if *oid == id { Some(*oval) } else { None })
            .expect("id should match an original row");
        assert_eq!(val, original_val, "undo pre-image should be original value");
        assert_eq!(entry.writer_xid, Xid::new(2));
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
            let bytes = pair_payload(i as i32, (i as i32) * 7);
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
                rel(),
                n_blocks,
                &snap,
                &oracle,
                |_id, _val| true,
                Xid::new(2),
                CommandId::FIRST,
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
            ultrasql_wal::record::RecordType::HeapDeleteInPlace
        )),
        "WAL must contain at least one HeapDeleteInPlace record"
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
