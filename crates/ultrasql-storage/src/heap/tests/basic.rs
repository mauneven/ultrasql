//! Basic heap access tests: insert, fetch, delete, scan, overflow guards,
//! and attribute-count preservation.

use std::sync::Arc;
use std::sync::atomic::Ordering as AtomicOrdering;
use std::thread;

use ultrasql_core::constants::PAGE_SIZE;
use ultrasql_core::{BlockNumber, CommandId, Result, Xid};
use ultrasql_mvcc::status::test_support::MapOracle;
use ultrasql_mvcc::{Snapshot, Visibility, is_visible};

use super::*;
use crate::page::{ITEMID_SIZE, ItemId, ItemIdFlags, Page};

#[test]
fn seed_block_count_advances_monotonically() {
    let heap = make_heap(8);
    let rel = ultrasql_core::RelationId::new(42);
    assert_eq!(heap.block_count(rel), 0);

    // Seeding raises the counter to the durable on-disk size...
    heap.seed_block_count(rel, 5);
    assert_eq!(heap.block_count(rel), 5);

    // ...but never lowers it (a smaller durable figure must not undo a larger
    // count that WAL replay already advanced past).
    heap.seed_block_count(rel, 3);
    assert_eq!(heap.block_count(rel), 5);

    // A larger durable figure does advance it.
    heap.seed_block_count(rel, 9);
    assert_eq!(heap.block_count(rel), 9);

    // Zero is a no-op (no durable blocks discovered for the relation).
    heap.seed_block_count(rel, 0);
    assert_eq!(heap.block_count(rel), 9);
}

#[test]
fn heap_count_add_rejects_overflow() {
    let err = checked_heap_count_add(usize::MAX, 1, "updated tuple count overflow").unwrap_err();
    assert!(matches!(
        err,
        HeapError::MalformedHeader("updated tuple count overflow")
    ));
}

#[test]
fn heap_tuple_space_needed_rejects_overflow() {
    let err = checked_tuple_space_needed(usize::MAX).unwrap_err();
    assert!(matches!(
        err,
        HeapError::MalformedHeader("tuple size overflow")
    ));
}

#[test]
fn heap_u32_count_add_rejects_overflow() {
    let err = checked_heap_u32_count_add(u32::MAX, 1, "vacuum stat overflow").unwrap_err();
    assert!(matches!(
        err,
        HeapError::MalformedHeader("vacuum stat overflow")
    ));
}

#[test]
fn heap_u64_count_add_rejects_overflow() {
    let err = checked_heap_u64_count_add(u64::MAX, 1, "bulk load count overflow").unwrap_err();
    assert!(matches!(
        err,
        HeapError::MalformedHeader("bulk load count overflow")
    ));
}

#[test]
fn slot_window_rejects_itemid_range_outside_page() {
    let mut page = Page::new_heap();
    let slot = page.insert_tuple(b"ok").unwrap();
    let item_off = Page::item_id_offset(slot);
    let malformed = ItemId::new(8_000, 500, ItemIdFlags::Normal);
    page.as_bytes_mut()[item_off..item_off + ITEMID_SIZE]
        .copy_from_slice(&malformed.into_raw().to_le_bytes());

    let err = HeapAccess::<MapLoader>::slot_window(page.as_bytes(), slot).unwrap_err();

    assert!(matches!(
        err,
        HeapError::MalformedHeader("itemid tuple range outside page")
    ));
}

#[test]
fn tuple_header_range_rejects_offset_overflow() {
    let err = HeapAccess::<MapLoader>::tuple_header_range(usize::MAX).unwrap_err();

    assert!(matches!(
        err,
        HeapError::MalformedHeader("tuple header range overflow")
    ));
}

#[test]
fn tuple_field_range_rejects_offset_overflow() {
    let err = HeapAccess::<MapLoader>::tuple_field_range(usize::MAX, 8, 8, "tuple field overflow")
        .unwrap_err();

    assert!(matches!(
        err,
        HeapError::MalformedHeader("tuple field overflow")
    ));
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

#[test]
fn concurrent_inserts_from_two_threads_preserve_every_tuple() {
    const N: u32 = 200;

    let heap = Arc::new(make_heap(64));
    let thread_count = u32_to_usize(N);
    let expected_count = u32_to_usize(
        N.checked_mul(2)
            .expect("test tuple count must not overflow"),
    );
    let h1 = {
        let heap = Arc::clone(&heap);
        thread::spawn(move || {
            let mut out = Vec::with_capacity(thread_count);
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
            let mut out = Vec::with_capacity(thread_count);
            for i in 0..N {
                let payload = (i + N).to_le_bytes().repeat(8);
                out.push(heap.insert(rel(), &payload, opts(200)).unwrap());
            }
            out
        })
    };
    let mut all: Vec<TupleId> = h1.join().unwrap();
    all.extend(h2.join().unwrap());
    assert_eq!(all.len(), expected_count);
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
    assert_eq!(scanned.len(), expected_count);
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
                n_atts: 0,
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
fn insert_batch_rejects_oversized_row_without_extending() {
    let heap = make_heap(8);
    let counter = heap.counter_for(rel());
    let cursor = heap.cursor_for(rel());
    counter.store(u32::MAX - 1, AtomicOrdering::Release);
    cursor.store(u32::MAX - 2, AtomicOrdering::Release);
    let oversized = vec![0xAB_u8; PAGE_SIZE];

    let err = heap
        .insert_batch(rel(), &[oversized.as_slice()], opts(100))
        .unwrap_err();

    assert!(matches!(err, HeapError::Page(PageError::NoSpace { .. })));
    assert_eq!(heap.block_count(rel()), u32::MAX - 1);
}

#[test]
fn empty_scan_returns_nothing() {
    let heap = make_heap(8);
    let mut it = heap.scan(rel(), 0);
    assert!(it.next().is_none());
}

#[test]
fn insert_and_update_preserve_attribute_count() {
    let heap = make_heap(16);

    let tid = heap
        .insert(rel(), b"two-column-row", opts_with_n_atts(100, 2))
        .unwrap();
    assert_eq!(heap.fetch(tid).unwrap().header.n_atts, 2);

    let batch_tids = heap
        .insert_batch(
            rel(),
            &[b"batch-a".as_slice(), b"batch-b".as_slice()],
            opts_with_n_atts(101, 3),
        )
        .unwrap();
    for tid in batch_tids {
        assert_eq!(heap.fetch(tid).unwrap().header.n_atts, 3);
    }

    let hot = heap
        .update(tid, b"hot-row", update_opts(200))
        .expect("small update should be HOT");
    assert!(hot.hot);
    assert_eq!(heap.fetch(hot.new_tid).unwrap().header.n_atts, 2);

    let full_heap = make_heap(16);
    let big = [0xAB_u8; 7000];
    let old_tid = full_heap
        .insert(rel(), &big, opts_with_n_atts(300, 4))
        .unwrap();
    let _other = full_heap
        .insert(rel(), &big, opts_with_n_atts(300, 4))
        .unwrap();
    let non_hot = full_heap
        .update(old_tid, &big, update_opts(400))
        .expect("full page should fall back to non-HOT");
    assert!(!non_hot.hot);
    assert_eq!(full_heap.fetch(non_hot.new_tid).unwrap().header.n_atts, 4);
}
