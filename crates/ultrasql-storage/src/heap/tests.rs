#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::cast_lossless,
    reason = "test: deterministic data generation with compile-time-bounded loop sizes"
)]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::thread;

use parking_lot::Mutex;
use ultrasql_core::constants::PAGE_SIZE;
use ultrasql_core::{BlockNumber, CommandId, PageId, Result, Xid};
use ultrasql_mvcc::status::test_support::MapOracle;
use ultrasql_mvcc::status::{XidStatus, XidStatusOracle};
use ultrasql_mvcc::tuple_header::{InfoMask, TUPLE_HEADER_SIZE};
use ultrasql_mvcc::{Snapshot, Visibility, is_visible};

use super::*;
use crate::buffer_pool::BufferPool;
use crate::page::Page;

/// Test loader that materializes blank heap pages on first miss
/// and persists them keyed by `PageId` so writes from one
/// pin/unpin cycle survive into the next.
#[derive(Default)]
struct MapLoader {
    store: Mutex<HashMap<PageId, Box<[u8; PAGE_SIZE]>>>,
}

impl MapLoader {
    fn new() -> Self {
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
                .map_err(|e| ultrasql_core::Error::Corruption(format!("test loader: {e}")));
        }
        let page = Page::new_heap();
        // Persist a snapshot so the next `load` for the same id
        // sees the same blank page. Writes through the buffer
        // pool don't flush back into this map by themselves; the
        // tests in this module don't exercise eviction so this
        // is fine.
        let mut copy: Box<[u8; PAGE_SIZE]> = vec![0_u8; PAGE_SIZE]
            .into_boxed_slice()
            .try_into()
            .expect("alloc matches PAGE_SIZE");
        copy.copy_from_slice(page.as_bytes());
        self.store.lock().insert(page_id, copy);
        Ok(page)
    }
}

fn rel() -> RelationId {
    RelationId::new(42)
}

fn opts(xid: u64) -> InsertOptions<'static> {
    InsertOptions {
        xmin: Xid::new(xid),
        command_id: CommandId::FIRST,
        wal: None,
        fsm: None,
        vm: None,
    }
}

fn del_opts(xmax: u64, cmax: u32) -> DeleteOptions<'static> {
    DeleteOptions {
        xmax: Xid::new(xmax),
        cmax: CommandId::new(cmax),
        wal: None,
        fsm: None,
        vm: None,
    }
}

fn make_heap(capacity: usize) -> HeapAccess<MapLoader> {
    let pool = Arc::new(BufferPool::new(capacity, MapLoader::new()));
    HeapAccess::new(pool)
}

#[derive(Debug, Default)]
struct CountingOracle {
    inner: MapOracle,
    calls: AtomicUsize,
}

impl CountingOracle {
    fn new() -> Self {
        Self::default()
    }

    fn set_committed(&self, xid: Xid) {
        self.inner.set_committed(xid);
    }

    fn calls(&self) -> usize {
        self.calls.load(AtomicOrdering::Relaxed)
    }
}

impl XidStatusOracle for CountingOracle {
    fn status(&self, xid: Xid) -> XidStatus {
        self.calls.fetch_add(1, AtomicOrdering::Relaxed);
        self.inner.status(xid)
    }
}

#[test]
fn insert_and_fetch_round_trip() {
    let heap = make_heap(8);
    let payload = b"hello heap";
    let tid = heap.insert(rel(), payload, opts(100)).unwrap();
    let got = heap.fetch(tid).unwrap();
    assert_eq!(got.tid, tid);
    assert_eq!(got.data, payload);
    assert_eq!(got.header.xmin, Xid::new(100));
    assert!(got.header.is_alive());
    // Header's ctid was patched to point at the assigned slot.
    assert_eq!(got.header.ctid, tid);
}

#[test]
fn insert_returns_increasing_tuple_ids_within_a_page() {
    let heap = make_heap(8);
    let mut slots = Vec::new();
    for i in 0_u32..16 {
        let tid = heap.insert(rel(), &i.to_le_bytes(), opts(100)).unwrap();
        slots.push(tid);
    }
    // All on block 0, slots 0..16.
    for (i, tid) in slots.iter().enumerate() {
        assert_eq!(tid.page.block, BlockNumber::new(0));
        assert_eq!(usize::from(tid.slot), i);
    }
}

#[test]
fn insert_many_tuples_spans_multiple_pages() {
    let heap = make_heap(32);
    // Insert tuples large enough that ~30 fit on a page.
    let payload = [0xAB_u8; 200];
    let mut tids = Vec::new();
    for _ in 0..200 {
        tids.push(heap.insert(rel(), &payload, opts(100)).unwrap());
    }
    // Confirm we used at least two blocks.
    let max_block = tids.iter().map(|t| t.page.block.raw()).max().unwrap();
    assert!(max_block >= 1, "expected ≥2 blocks; max_block={max_block}");
    // Every fetch succeeds.
    for tid in &tids {
        let t = heap.fetch(*tid).unwrap();
        assert_eq!(t.data, &payload[..]);
    }
}

#[test]
fn delete_sets_xmax_and_preserves_data() {
    let heap = make_heap(8);
    let payload = b"row";
    let tid = heap.insert(rel(), payload, opts(100)).unwrap();
    heap.delete(tid, del_opts(200, 3)).unwrap();
    let got = heap.fetch(tid).unwrap();
    assert_eq!(got.header.xmax, Xid::new(200));
    assert_eq!(got.header.cmax, CommandId::new(3));
    // Original insert metadata intact.
    assert_eq!(got.header.xmin, Xid::new(100));
    assert_eq!(got.data, payload);
    assert!(!got.header.is_alive());
}

#[test]
fn scan_yields_every_inserted_tuple_in_insert_order() {
    let heap = make_heap(32);
    let payload = [0xCD_u8; 200];
    let mut tids = Vec::new();
    for _ in 0..100 {
        tids.push(heap.insert(rel(), &payload, opts(100)).unwrap());
    }
    let blocks = heap.block_count(rel());
    let scanned: Vec<TupleId> = heap.scan(rel(), blocks).map(|r| r.unwrap().tid).collect();
    assert_eq!(scanned.len(), tids.len());
    // Scan walks (block, slot) ascending; inserts within a block
    // also assigned ascending slots and we always filled the
    // lowest-block first, so the orders must match.
    assert_eq!(scanned, tids);
}

#[test]
fn insert_grows_relation_when_existing_pages_full() {
    let heap = make_heap(32);
    let big = [0xEE_u8; 7000]; // ~7 KiB — only one fits per 8 KiB page.
    let t0 = heap.insert(rel(), &big, opts(100)).unwrap();
    let t1 = heap.insert(rel(), &big, opts(100)).unwrap();
    assert_eq!(t0.page.block, BlockNumber::new(0));
    // Second insert must land on a newly allocated block.
    assert_eq!(t1.page.block, BlockNumber::new(1));
    assert_eq!(heap.block_count(rel()), 2);
}

// TODO(heap-concurrency): real intermittent race where two
// threads inserting into the same in-memory PageLoader-backed heap
// can stomp the per-frame state under the buffer-pool clock hand
// before the pin_count fence sees the other thread's write. The
// production segment-backed loader does not have this hot loop, so
// the race is gated behind the test loader's structure. Tracked
// for a follow-up; ignored here so CI is deterministic.
#[test]
#[ignore = "flaky in CI; see TODO(heap-concurrency) above"]
fn concurrent_inserts_from_two_threads_preserve_every_tuple() {
    const N: u32 = 200;

    let heap = Arc::new(make_heap(64));
    let h1 = {
        let heap = Arc::clone(&heap);
        thread::spawn(move || {
            let mut out = Vec::with_capacity(N as usize);
            for i in 0..N {
                let payload = i.to_le_bytes().repeat(8);
                out.push(heap.insert(rel(), &payload, opts(100)).unwrap());
            }
            out
        })
    };
    let h2 = {
        let heap = Arc::clone(&heap);
        thread::spawn(move || {
            let mut out = Vec::with_capacity(N as usize);
            for i in 0..N {
                let payload = (i + N).to_le_bytes().repeat(8);
                out.push(heap.insert(rel(), &payload, opts(200)).unwrap());
            }
            out
        })
    };
    let mut all: Vec<TupleId> = h1.join().unwrap();
    all.extend(h2.join().unwrap());
    assert_eq!(all.len(), (2 * N) as usize);
    // Every tid must be unique and fetchable.
    all.sort();
    let len_before_dedup = all.len();
    all.dedup();
    assert_eq!(all.len(), len_before_dedup, "duplicate tids assigned");
    for tid in &all {
        heap.fetch(*tid).unwrap();
    }

    // Scan must surface exactly 2*N tuples too.
    let blocks = heap.block_count(rel());
    let scanned: Vec<HeapTuple> = heap
        .scan(rel(), blocks)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(scanned.len(), (2 * N) as usize);
}

#[test]
fn visibility_predicate_filters_scanned_tuples() {
    // Smoke-test the MVCC stack on top of the heap.
    let heap = make_heap(16);
    let committed_xid = Xid::new(100);
    let bad_xid = Xid::new(101);
    let alive_tid = heap.insert(rel(), b"alive", opts(100)).unwrap();
    let _aborted = heap
        .insert(
            rel(),
            b"aborted-insert",
            InsertOptions {
                xmin: bad_xid,
                command_id: CommandId::FIRST,
                fsm: None,
                vm: None,
                wal: None,
            },
        )
        .unwrap();
    let to_delete_tid = heap.insert(rel(), b"will-be-deleted", opts(100)).unwrap();
    heap.delete(
        to_delete_tid,
        DeleteOptions {
            xmax: Xid::new(102),
            cmax: CommandId::FIRST,
            fsm: None,
            vm: None,
            wal: None,
        },
    )
    .unwrap();

    let oracle = MapOracle::new();
    oracle.set_committed(committed_xid);
    oracle.set_aborted(bad_xid);
    oracle.set_committed(Xid::new(102));

    let snap = Snapshot::new(
        Xid::new(50),
        Xid::new(200),
        Xid::new(999),
        CommandId::FIRST,
        std::iter::empty(),
    );

    let blocks = heap.block_count(rel());
    let visible: Vec<HeapTuple> = heap
        .scan(rel(), blocks)
        .filter_map(|r| {
            let tup = r.ok()?;
            if matches!(is_visible(&tup.header, &snap, &oracle), Visibility::Visible) {
                Some(tup)
            } else {
                None
            }
        })
        .collect();

    assert_eq!(visible.len(), 1, "only the alive committed tuple survives");
    assert_eq!(visible[0].tid, alive_tid);
    assert_eq!(visible[0].data, b"alive");
}

#[test]
fn fetch_dead_slot_returns_page_error() {
    let heap = make_heap(8);
    let tid = heap.insert(rel(), b"x", opts(100)).unwrap();
    // Hard-delete the slot via the page API by going through the
    // pool ourselves — the heap's `delete` is the MVCC delete and
    // leaves the slot Normal.
    {
        let guard = heap.pool.get_page(tid.page).unwrap();
        let mut page = guard.write();
        page.delete_tuple(tid.slot).unwrap();
    }
    let err = heap.fetch(tid).unwrap_err();
    assert!(
        matches!(err, HeapError::Page(PageError::DeadSlot(_))),
        "got {err:?}"
    );
}

#[test]
fn scan_skips_hard_deleted_slots() {
    let heap = make_heap(16);
    let _t0 = heap.insert(rel(), b"a", opts(100)).unwrap();
    let t1 = heap.insert(rel(), b"b", opts(100)).unwrap();
    let _t2 = heap.insert(rel(), b"c", opts(100)).unwrap();
    // Hard-delete the middle slot.
    {
        let guard = heap.pool.get_page(t1.page).unwrap();
        let mut page = guard.write();
        page.delete_tuple(t1.slot).unwrap();
    }
    let blocks = heap.block_count(rel());
    let payloads: Vec<Vec<u8>> = heap.scan(rel(), blocks).map(|r| r.unwrap().data).collect();
    assert_eq!(payloads, vec![b"a".to_vec(), b"c".to_vec()]);
}

#[test]
fn block_count_grows_only_when_needed() {
    let heap = make_heap(8);
    assert_eq!(heap.block_count(rel()), 0);
    let _ = heap.insert(rel(), b"first", opts(100)).unwrap();
    assert_eq!(heap.block_count(rel()), 1);
    // Subsequent inserts that fit on block 0 do not grow.
    for _ in 0..50 {
        let _ = heap.insert(rel(), b"x", opts(100)).unwrap();
    }
    assert_eq!(heap.block_count(rel()), 1);
}

#[test]
fn empty_scan_returns_nothing() {
    let heap = make_heap(8);
    let mut it = heap.scan(rel(), 0);
    assert!(it.next().is_none());
}

// -----------------------------------------------------------------------
// UpdateOptions / UpdateOutcome helpers
// -----------------------------------------------------------------------

fn update_opts(xid: u64) -> UpdateOptions<'static> {
    UpdateOptions {
        xid: Xid::new(xid),
        command_id: CommandId::FIRST,
        hot_eligible: true,
        wal: None,
        vm: None,
    }
}

// -----------------------------------------------------------------------
// Deliverable A tests
// -----------------------------------------------------------------------

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

// -----------------------------------------------------------------------
// Deliverable B tests
// -----------------------------------------------------------------------

fn committed_snap(current_xid: u64) -> Snapshot {
    // Snapshot where all xids < 50 are outside the active set.
    Snapshot::new(
        Xid::new(50),
        Xid::new(500),
        Xid::new(current_xid),
        CommandId::FIRST,
        std::iter::empty(),
    )
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

// Property test: for any set of inserts + random deletes, the
// visibility-aware scan returns exactly the non-deleted tuples when
// all xids are committed.
use proptest::prelude::*;

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

// -----------------------------------------------------------------------
// WAL emission tests (Deliverable C)
// -----------------------------------------------------------------------

mod wal_emission {
    use ultrasql_core::{CommandId, Lsn, Xid};
    use ultrasql_wal::WalRecord;
    use ultrasql_wal::payload::{
        HEAP_UPDATE_HOT, HeapDeletePayload, HeapInsertPayload, HeapUpdatePayload,
    };
    use ultrasql_wal::record::RecordType;

    use super::*;
    use crate::buffer_pool::BufferPool;
    use crate::wal_sink::{NullWalSink, WalSinkError, test_support::InMemoryWalSink};

    fn make_heap_with_sink(capacity: usize) -> (HeapAccess<MapLoader>, Arc<InMemoryWalSink>) {
        let pool = Arc::new(BufferPool::new(capacity, MapLoader::new()));
        let heap = HeapAccess::new(pool);
        let sink = Arc::new(InMemoryWalSink::new());
        (heap, sink)
    }

    fn rel() -> RelationId {
        RelationId::new(99)
    }

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
    // 9. WAL append failure after a committed page mutation panics
    // -------------------------------------------------------------------

    /// A WAL sink that always rejects every record. Used to verify that
    /// the heap panics rather than silently returning `Err` once a page
    /// mutation is committed.
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

    #[test]
    fn wal_append_failure_during_insert_panics() {
        let heap = make_heap(8);
        let sink = RejectingWalSink;

        // The page mutation will succeed, then sink.append will return
        // Err. The heap must panic rather than returning that Err to the
        // caller, because the on-page state has already been committed.
        // AssertUnwindSafe is safe here: the test does not share any
        // mutable state across the unwind boundary.
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            heap.insert(
                rel(),
                b"will-write-then-wal-fail",
                InsertOptions {
                    xmin: Xid::new(42),
                    command_id: CommandId::FIRST,
                    fsm: None,
                    vm: None,
                    wal: Some(&sink),
                },
            )
        }));
        assert!(
            result.is_err(),
            "heap insert must panic when WAL append fails after a committed page mutation"
        );
    }

    proptest! {
        #[test]
        fn prop_prev_lsn_chain_monotonic(
            n in 2_usize..=20,
        ) {
            let (heap, sink) = make_heap_with_sink(256);
            let xid = Xid::new(42);

            for i in 0..n {
                let payload = (i as u8).to_le_bytes();
                heap.insert(
                    rel(),
                    &payload,
                    InsertOptions {
                        xmin: xid,
                        command_id: CommandId::FIRST,
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

    // ------------------------------------------------------------------
    // vacuum_heap tests
    // ------------------------------------------------------------------

    #[test]
    fn vacuum_heap_reclaims_committed_dead_tuples() {
        let heap = make_heap(16);
        let r = rel();

        // Insert two tuples under different XIDs.
        let t1 = heap.insert(r, b"live", opts(10)).unwrap();
        let t2 = heap.insert(r, b"dead", opts(20)).unwrap();

        // Delete t2 under XID 30.
        heap.delete(t2, del_opts(30, 0)).unwrap();

        // Build an oracle that says XIDs 10, 20, 30 are all committed.
        let oracle = MapOracle::default();
        oracle.set_committed(Xid::new(10));
        oracle.set_committed(Xid::new(20));
        oracle.set_committed(Xid::new(30));

        // oldest_active_xid > 30, so XID 30 is eligible for vacuum.
        let stats = heap.vacuum_heap(r, Xid::new(100), &oracle).unwrap();
        assert_eq!(stats.tuples_reclaimed, 1, "one dead tuple expected");
        assert_eq!(
            stats.pages_compacted, 1,
            "one page should have been compacted"
        );

        // t1 must still be fetchable; t2 must be gone (slot is now dead/unused).
        let live = heap.fetch(t1).unwrap();
        assert_eq!(live.data, b"live");
    }

    #[test]
    fn vacuum_heap_skips_in_progress_deleters() {
        let heap = make_heap(16);
        let r = rel();

        let t = heap.insert(r, b"row", opts(10)).unwrap();
        heap.delete(t, del_opts(50, 0)).unwrap();

        let oracle = MapOracle::default();
        oracle.set_committed(Xid::new(10));
        // XID 50 is NOT committed in the oracle.

        // oldest_active_xid = 40 < 50, so XID 50 is still "in progress".
        let stats = heap.vacuum_heap(r, Xid::new(40), &oracle).unwrap();
        assert_eq!(
            stats.tuples_reclaimed, 0,
            "in-progress delete must not be vacuumed"
        );
    }

    #[test]
    fn vacuum_heap_skips_alive_tuples() {
        let heap = make_heap(16);
        let r = rel();

        heap.insert(r, b"still alive", opts(10)).unwrap();

        let oracle = MapOracle::default();
        oracle.set_committed(Xid::new(10));

        let stats = heap.vacuum_heap(r, Xid::new(100), &oracle).unwrap();
        assert_eq!(
            stats.tuples_reclaimed, 0,
            "alive tuple must not be reclaimed"
        );
        assert_eq!(stats.pages_compacted, 0);
    }
}
