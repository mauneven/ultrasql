//! Unit tests for the B+ tree.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering as AtomicOrdering};
use std::thread;

use ultrasql_core::endian::write_i64_le;
use ultrasql_core::{BlockNumber, PageId, RelationId, TupleId, Xid};
use ultrasql_wal::WalRecord;

use super::node::{NodeMeta, init_btree_page};
use super::*;
use crate::buffer_pool::{BufferPool, BufferPoolError, PageLoader};
use crate::page::Page;
use crate::wal_sink::{WalSink, WalSinkError};

/// In-memory loader for B-tree tests.
///
/// The B-tree reinitialises every page it allocates, so the loader
/// only needs to hand back blank heap pages on cache miss. The
/// counter is purely diagnostic.
#[derive(Default, Debug)]
struct MapLoader {
    misses: AtomicU64,
}

impl MapLoader {
    const fn new() -> Self {
        Self {
            misses: AtomicU64::new(0),
        }
    }
}

impl PageLoader for MapLoader {
    fn load(&self, _page_id: PageId) -> ultrasql_core::Result<Page> {
        self.misses.fetch_add(1, AtomicOrdering::Relaxed);
        // The buffer pool keeps modifications in its own frame
        // memory while pinned/resident; the loader only services
        // misses. For tests, a blank page is fine because the
        // tree reinitialises freshly allocated blocks immediately.
        Ok(Page::new_heap())
    }
}

fn make_tree() -> BTree<MapLoader> {
    // Pool sized to comfortably hold the dirty-page set the tests
    // build. The buffer pool currently refuses to evict dirty
    // pages (the storage manager owns flushing), so we pre-size
    // the pool to fit the test workload.
    let pool = Arc::new(BufferPool::new(4096, MapLoader::new()));
    BTree::create(pool, RelationId::new(42)).expect("create btree")
}

fn tid(block: u32, slot: u16) -> TupleId {
    TupleId::new(
        PageId::new(RelationId::new(99), BlockNumber::new(block)),
        slot,
    )
}

#[test]
fn node_meta_rejects_reserved_flag_bits() {
    let mut page = Page::new_heap();
    init_btree_page(&mut page, NodeMeta::fresh_leaf()).unwrap();
    page.as_bytes_mut()[NODE_SPECIAL_OFFSET + 16] |= 1 << 2;

    let err = NodeMeta::read_from(&page).unwrap_err();

    assert!(matches!(
        err,
        BTreeError::MalformedNode("node flags reserved bits")
    ));
}

#[test]
fn node_meta_rejects_reserved_suffix_bytes() {
    let mut page = Page::new_heap();
    init_btree_page(&mut page, NodeMeta::fresh_leaf()).unwrap();
    page.as_bytes_mut()[NODE_SPECIAL_OFFSET + 18] = 1;

    let err = NodeMeta::read_from(&page).unwrap_err();

    assert!(matches!(
        err,
        BTreeError::MalformedNode("node reserved bytes")
    ));
}

#[test]
fn node_meta_write_clears_reserved_suffix_bytes() {
    let mut page = Page::new_heap();
    init_btree_page(&mut page, NodeMeta::fresh_leaf()).unwrap();
    page.as_bytes_mut()[NODE_SPECIAL_OFFSET + 18..NODE_SPECIAL_OFFSET + NODE_META_SIZE].fill(0xFF);

    NodeMeta::fresh_leaf().write_into(&mut page);

    assert!(
        page.as_bytes()[NODE_SPECIAL_OFFSET + 18..NODE_SPECIAL_OFFSET + NODE_META_SIZE]
            .iter()
            .all(|&b| b == 0)
    );
}

#[test]
fn empty_tree_lookup_returns_none() {
    let tree = make_tree();
    assert!(tree.lookup::<i64>(0).unwrap().is_none());
    assert!(tree.lookup::<i64>(100).unwrap().is_none());
    assert!(tree.lookup::<i64>(-100).unwrap().is_none());
}

#[test]
fn insert_then_lookup_returns_value() {
    let mut tree = make_tree();
    tree.insert::<i64>(42, tid(1, 2), Xid::new(1), None)
        .unwrap();
    assert_eq!(tree.lookup::<i64>(42).unwrap(), Some(tid(1, 2)));
    assert!(tree.lookup::<i64>(43).unwrap().is_none());
}

#[test]
fn insert_1000_sequential_keys() {
    let mut tree = make_tree();
    for i in 0_i64..1000 {
        let block = u32::try_from(i).expect("fits in u32");
        let slot = u16::try_from(i & 0xFFFF).expect("fits in u16");
        tree.insert::<i64>(i, tid(block, slot), Xid::new(1), None)
            .unwrap();
    }
    for i in 0_i64..1000 {
        let block = u32::try_from(i).expect("fits in u32");
        let slot = u16::try_from(i & 0xFFFF).expect("fits in u16");
        assert_eq!(
            tree.lookup::<i64>(i).unwrap(),
            Some(tid(block, slot)),
            "lookup({i}) failed",
        );
    }
    assert!(tree.lookup::<i64>(1000).unwrap().is_none());
    assert!(tree.lookup::<i64>(-1).unwrap().is_none());
}

#[test]
fn insert_1000_shuffled_keys() {
    let mut tree = make_tree();
    let mut keys: Vec<i64> = (0_i64..1000).collect();
    // Deterministic xorshift permutation.
    let mut s: u64 = 0x9E37_79B9_7F4A_7C15;
    for i in (1..keys.len()).rev() {
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        let upper = u64::try_from(i + 1).expect("shuffle bound must fit u64");
        let j = usize::try_from(s % upper).expect("shuffle index must fit usize");
        keys.swap(i, j);
    }
    for &k in &keys {
        let block = u32::try_from(k).expect("fits in u32");
        let slot = u16::try_from((k * 7) & 0xFFFF).expect("fits in u16");
        tree.insert::<i64>(k, tid(block, slot), Xid::new(1), None)
            .unwrap();
    }
    for &k in &keys {
        let block = u32::try_from(k).expect("fits in u32");
        let slot = u16::try_from((k * 7) & 0xFFFF).expect("fits in u16");
        assert_eq!(
            tree.lookup::<i64>(k).unwrap(),
            Some(tid(block, slot)),
            "lookup({k}) failed",
        );
    }
}

#[test]
fn range_scan_visits_keys_in_order() {
    let mut tree = make_tree();
    for i in 0_i64..200 {
        let block = u32::try_from(i).expect("fits in u32");
        tree.insert::<i64>(i, tid(block, 0), Xid::new(1), None)
            .unwrap();
    }
    let collected: Vec<(i64, TupleId)> = tree
        .range_scan::<i64>(0, None)
        .map(Result::unwrap)
        .collect();
    assert_eq!(collected.len(), 200);
    for (i, (k, _)) in collected.iter().enumerate() {
        let expected = i64::try_from(i).expect("fits");
        assert_eq!(*k, expected, "out-of-order at slot {i}");
    }
}

#[test]
fn range_scan_with_end_bound_stops_at_right_place() {
    let mut tree = make_tree();
    for i in 0_i64..200 {
        let block = u32::try_from(i).expect("fits in u32");
        tree.insert::<i64>(i, tid(block, 0), Xid::new(1), None)
            .unwrap();
    }
    let collected: Vec<i64> = tree
        .range_scan::<i64>(50, Some(120))
        .map(|r| r.unwrap().0)
        .collect();
    assert_eq!(collected.len(), 70);
    assert_eq!(collected.first(), Some(&50));
    assert_eq!(collected.last(), Some(&119));
}

#[test]
fn duplicate_insert_returns_duplicate_key_error() {
    let mut tree = make_tree();
    tree.insert::<i64>(7, tid(1, 0), Xid::new(1), None).unwrap();
    let err = tree
        .insert::<i64>(7, tid(2, 0), Xid::new(1), None)
        .unwrap_err();
    assert!(matches!(err, BTreeError::DuplicateKey), "got {err:?}");
    assert_eq!(tree.lookup::<i64>(7).unwrap(), Some(tid(1, 0)));
}

#[test]
fn non_unique_insert_allows_duplicate_keys_and_lookup_all_returns_every_tid() {
    let mut tree = make_tree();
    for block in 1_u32..=40 {
        tree.insert_non_unique::<i64>(7, tid(block, 0), Xid::new(1), None)
            .unwrap();
    }

    let found = tree.lookup_all::<i64>(7).unwrap();
    let expected: Vec<_> = (1_u32..=40).map(|block| tid(block, 0)).collect();
    assert_eq!(found, expected);

    let range_found: Vec<_> = tree
        .range_scan::<i64>(7, Some(8))
        .map(|entry| entry.unwrap().1)
        .collect();
    assert_eq!(range_found, expected);
}

#[test]
fn delete_removes_key_and_allows_reinsert() {
    let mut tree = make_tree();
    tree.insert::<i64>(7, tid(1, 0), Xid::new(1), None).unwrap();

    assert!(tree.delete::<i64>(7, tid(1, 0)).unwrap());
    assert!(tree.lookup::<i64>(7).unwrap().is_none());
    assert!(!tree.delete::<i64>(7, tid(1, 0)).unwrap());

    tree.insert::<i64>(7, tid(2, 0), Xid::new(2), None).unwrap();
    assert_eq!(tree.lookup::<i64>(7).unwrap(), Some(tid(2, 0)));
}

#[test]
fn delete_logged_emits_btree_delete_record() {
    use crate::wal_sink::test_support::InMemoryWalSink;
    use ultrasql_wal::record::RecordType;

    let mut tree = make_tree();
    let sink = InMemoryWalSink::new();
    tree.insert::<i64>(7, tid(1, 0), Xid::new(1), None).unwrap();

    assert!(
        tree.delete_logged::<i64>(7, tid(1, 0), Xid::new(2), Some(&sink))
            .unwrap()
    );
    let records = sink.records();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].1.header.record_type, RecordType::BTreeOp);
}

struct RejectingWalSink;

impl WalSink for RejectingWalSink {
    fn append(&self, _record: WalRecord) -> Result<ultrasql_core::Lsn, WalSinkError> {
        Err(WalSinkError::Rejected(
            "test: btree sink intentionally rejects records".into(),
        ))
    }

    fn durable_lsn(&self) -> ultrasql_core::Lsn {
        ultrasql_core::Lsn::ZERO
    }

    fn last_lsn_for(&self, _xid: Xid) -> ultrasql_core::Lsn {
        ultrasql_core::Lsn::ZERO
    }
}

struct RejectSecondWalSink {
    calls: AtomicUsize,
}

impl RejectSecondWalSink {
    const fn new() -> Self {
        Self {
            calls: AtomicUsize::new(0),
        }
    }
}

impl WalSink for RejectSecondWalSink {
    fn append(&self, _record: WalRecord) -> Result<ultrasql_core::Lsn, WalSinkError> {
        let call = self.calls.fetch_add(1, AtomicOrdering::Relaxed);
        if call == 0 {
            Ok(ultrasql_core::Lsn::new(1))
        } else {
            Err(WalSinkError::Rejected(
                "test: btree sink rejects second record".into(),
            ))
        }
    }

    fn durable_lsn(&self) -> ultrasql_core::Lsn {
        ultrasql_core::Lsn::ZERO
    }

    fn last_lsn_for(&self, _xid: Xid) -> ultrasql_core::Lsn {
        ultrasql_core::Lsn::ZERO
    }
}

#[test]
fn insert_wal_append_failure_returns_error_and_poisons_pool() {
    let mut tree = make_tree();
    let sink = RejectingWalSink;

    let err = tree
        .insert::<i64>(7, tid(1, 0), Xid::new(1), Some(&sink))
        .unwrap_err();
    assert!(
        matches!(err, BTreeError::Wal(WalSinkError::Rejected(_))),
        "btree insert should return Wal error, got {err:?}"
    );

    let lookup = tree.lookup::<i64>(7);
    assert!(
        matches!(
            lookup,
            Err(BTreeError::BufferPool(BufferPoolError::Poisoned))
        ),
        "btree should reject later page access after WAL failure, got {lookup:?}"
    );
}

#[test]
fn split_wal_append_failure_returns_error_and_poisons_pool() {
    let mut tree = make_tree();
    for key in 0_i64..i64::try_from(MAX_LEAF_ENTRIES).unwrap() {
        let block = u32::try_from(key).unwrap();
        tree.insert::<i64>(key, tid(block, 0), Xid::new(1), None)
            .unwrap();
    }
    let sink = RejectSecondWalSink::new();

    let split_key = i64::try_from(MAX_LEAF_ENTRIES).unwrap();
    let err = tree
        .insert::<i64>(
            split_key,
            tid(u32::try_from(split_key).unwrap(), 0),
            Xid::new(2),
            Some(&sink),
        )
        .unwrap_err();
    assert!(
        matches!(err, BTreeError::Wal(WalSinkError::Rejected(_))),
        "btree split should return Wal error, got {err:?}"
    );

    let lookup = tree.lookup::<i64>(split_key);
    assert!(
        matches!(
            lookup,
            Err(BTreeError::BufferPool(BufferPoolError::Poisoned))
        ),
        "btree should reject later page access after split WAL failure, got {lookup:?}"
    );
}

#[test]
fn delete_wal_append_failure_returns_error_and_poisons_pool() {
    let mut tree = make_tree();
    let sink = RejectingWalSink;
    tree.insert::<i64>(7, tid(1, 0), Xid::new(1), None).unwrap();

    let err = tree
        .delete_logged::<i64>(7, tid(1, 0), Xid::new(2), Some(&sink))
        .unwrap_err();
    assert!(
        matches!(err, BTreeError::Wal(WalSinkError::Rejected(_))),
        "btree delete should return Wal error, got {err:?}"
    );

    let lookup = tree.lookup::<i64>(7);
    assert!(
        matches!(
            lookup,
            Err(BTreeError::BufferPool(BufferPoolError::Poisoned))
        ),
        "btree should reject later page access after WAL failure, got {lookup:?}"
    );
}

#[test]
fn one_split_keeps_root_lookup_correct() {
    // MAX_LEAF_ENTRIES = 32; inserting 33+ keys forces a split.
    let mut tree = make_tree();
    for i in 0_i64..40 {
        let block = u32::try_from(i).expect("fits in u32");
        tree.insert::<i64>(i, tid(block, 0), Xid::new(1), None)
            .unwrap();
    }
    for i in 0_i64..40 {
        let block = u32::try_from(i).expect("fits in u32");
        assert_eq!(
            tree.lookup::<i64>(i).unwrap(),
            Some(tid(block, 0)),
            "lookup({i}) post-split failed",
        );
    }
}

#[test]
fn two_level_splits_force_inner_node_split() {
    // MAX_INTERNAL_ENTRIES = 16 + MAX_LEAF_ENTRIES = 32: 1000
    // inserts comfortably forces the root to become a level-2
    // internal node.
    let mut tree = make_tree();
    for i in 0_i64..1000 {
        let block = u32::try_from(i).expect("fits in u32");
        tree.insert::<i64>(i, tid(block, 0), Xid::new(1), None)
            .unwrap();
    }
    for i in 0_i64..1000 {
        let block = u32::try_from(i).expect("fits in u32");
        assert_eq!(
            tree.lookup::<i64>(i).unwrap(),
            Some(tid(block, 0)),
            "lookup({i}) failed",
        );
    }
    let n: usize = tree.range_scan::<i64>(0, None).count();
    assert_eq!(n, 1000);
}

#[test]
fn delete_reinsert_and_high_key_insert_preserve_low_key_lookup() {
    let mut tree = make_tree();
    let mut current_tids: Vec<TupleId> = Vec::with_capacity(1000);
    for i in 0_i64..1000 {
        let block = u32::try_from(i).expect("fits in u32");
        let tuple = tid(block, 0);
        tree.insert::<i64>(i, tuple, Xid::new(1), None).unwrap();
        current_tids.push(tuple);
    }

    let mut next_block = 2_000_u32;
    for (step, op) in [
        Ok(932_i64),
        Ok(323),
        Ok(485),
        Ok(396),
        Ok(873),
        Ok(283),
        Err(1_000_001_000_i64),
        Ok(299),
        Err(1_000_001_001),
        Ok(489),
        Err(1_000_001_002),
        Ok(213),
        Err(1_000_001_003),
        Err(1_000_001_004),
        Ok(454),
        Ok(108),
        Err(1_000_001_005),
        Ok(990),
        Ok(641),
        Err(1_000_001_006),
        Ok(190),
        Err(1_000_001_007),
        Err(1_000_001_008),
    ]
    .into_iter()
    .enumerate()
    {
        match op {
            Ok(key) => {
                let idx = usize::try_from(key).expect("positive test key");
                let old_tid = current_tids[idx];
                let new_tid = tid(next_block, 0);
                next_block += 1;
                assert!(
                    tree.delete_logged::<i64>(key, old_tid, Xid::new(2), None)
                        .unwrap(),
                    "delete old key {key} at step {}",
                    step + 1
                );
                tree.insert::<i64>(key, new_tid, Xid::new(2), None).unwrap();
                current_tids[idx] = new_tid;
            }
            Err(key) => {
                tree.insert::<i64>(key, tid(next_block, 0), Xid::new(3), None)
                    .unwrap();
                next_block += 1;
            }
        }
        assert_eq!(
            tree.lookup::<i64>(28).unwrap(),
            Some(current_tids[28]),
            "lookup(28) failed after step {}",
            step + 1
        );
    }
}

#[test]
fn concurrent_readers_all_succeed() {
    let mut tree = make_tree();
    for i in 0_i64..500 {
        let block = u32::try_from(i).expect("fits in u32");
        tree.insert::<i64>(i, tid(block, 7), Xid::new(1), None)
            .unwrap();
    }
    let tree = Arc::new(tree);
    let threads: Vec<_> = (0_i64..8)
        .map(|t| {
            let tree = Arc::clone(&tree);
            thread::spawn(move || {
                for round in 0_i64..50 {
                    let key = (round * 7 + t).rem_euclid(500);
                    let block = u32::try_from(key).expect("fits in u32");
                    let v = tree.lookup::<i64>(key).unwrap();
                    assert_eq!(v, Some(tid(block, 7)));
                }
            })
        })
        .collect();
    for t in threads {
        t.join().expect("reader thread");
    }
}

#[test]
fn negative_keys_round_trip_correctly() {
    let mut tree = make_tree();
    for i in -50_i64..50 {
        let block = u32::try_from(i + 100).expect("fits in u32");
        tree.insert::<i64>(i, tid(block, 0), Xid::new(1), None)
            .unwrap();
    }
    for i in -50_i64..50 {
        let block = u32::try_from(i + 100).expect("fits in u32");
        assert_eq!(
            tree.lookup::<i64>(i).unwrap(),
            Some(tid(block, 0)),
            "lookup({i}) failed",
        );
    }
    let keys: Vec<i64> = tree
        .range_scan::<i64>(-10, Some(10))
        .map(|r| r.unwrap().0)
        .collect();
    let expected: Vec<i64> = (-10..10).collect();
    assert_eq!(keys, expected);
}

#[test]
fn range_scan_from_middle_starts_at_correct_key() {
    let mut tree = make_tree();
    for i in 0_i64..100 {
        let block = u32::try_from(i).expect("fits in u32");
        tree.insert::<i64>(i * 2, tid(block, 0), Xid::new(1), None)
            .unwrap();
    }
    // Start at 49 which is *between* keys 48 and 50; expect 50 first.
    let first = tree.range_scan::<i64>(49, None).next().unwrap().unwrap();
    assert_eq!(first.0, 50);
}

#[test]
fn open_recovers_existing_root() {
    let pool = Arc::new(BufferPool::new(4096, MapLoader::new()));
    let mut tree = BTree::create(Arc::clone(&pool), RelationId::new(7)).unwrap();
    for i in 0_i64..50 {
        let block = u32::try_from(i).expect("fits in u32");
        tree.insert::<i64>(i, tid(block, 0), Xid::new(1), None)
            .unwrap();
    }
    let root = tree.root_block();
    drop(tree);
    let tree2 = BTree::open(pool, RelationId::new(7), root);
    for i in 0_i64..50 {
        let block = u32::try_from(i).expect("fits in u32");
        assert_eq!(tree2.lookup::<i64>(i).unwrap(), Some(tid(block, 0)));
    }
}

#[test]
fn reopened_handles_share_block_allocation_across_splits() {
    let pool = Arc::new(BufferPool::new(4096, MapLoader::new()));
    let rel = RelationId::new(8);
    let tree = BTree::create(Arc::clone(&pool), rel).unwrap();
    let root = tree.root_block();
    drop(tree);

    let mut first = BTree::open(Arc::clone(&pool), rel, root);
    let mut second = BTree::open(Arc::clone(&pool), rel, root);

    for i in 0_i64..96 {
        let block = u32::try_from(i).expect("fits in u32");
        first
            .insert::<i64>(i, tid(block, 0), Xid::new(1), None)
            .unwrap();
    }
    for i in 1_000_i64..1_096 {
        let block = u32::try_from(i).expect("fits in u32");
        second
            .insert::<i64>(i, tid(block, 0), Xid::new(2), None)
            .unwrap();
    }

    let probe = BTree::open(pool, rel, root);
    for i in 0_i64..96 {
        let block = u32::try_from(i).expect("fits in u32");
        assert_eq!(
            probe.lookup::<i64>(i).unwrap(),
            Some(tid(block, 0)),
            "first handle key {i} missing after second handle splits",
        );
    }
    for i in 1_000_i64..1_096 {
        let block = u32::try_from(i).expect("fits in u32");
        assert_eq!(
            probe.lookup::<i64>(i).unwrap(),
            Some(tid(block, 0)),
            "second handle key {i} missing after split",
        );
    }
}

#[test]
fn reopened_handles_high_key_splits_preserve_existing_low_keys() {
    let pool = Arc::new(BufferPool::new(4096, MapLoader::new()));
    let rel = RelationId::new(9);
    let mut tree = BTree::create(Arc::clone(&pool), rel).unwrap();
    for i in 0_i64..1000 {
        let block = u32::try_from(i).expect("fits in u32");
        tree.insert::<i64>(i, tid(block, 0), Xid::new(1), None)
            .unwrap();
    }
    let root = tree.root_block();
    drop(tree);

    let mut handles: Vec<_> = (0_u32..4)
        .map(|_| BTree::open(Arc::clone(&pool), rel, root))
        .collect();
    for round in 0_i64..250 {
        for client in 0_usize..handles.len() {
            let client_i64 = i64::try_from(client).expect("fits");
            let key = 1_000_001_000_i64 + client_i64 * 10_000_000_i64 + round;
            handles[client]
                .insert::<i64>(
                    key,
                    tid(
                        u32::try_from(round).expect("fits"),
                        u16::try_from(client).unwrap(),
                    ),
                    Xid::new(2),
                    None,
                )
                .unwrap();
        }
    }

    let probe = BTree::open(pool, rel, root);
    for i in 0_i64..1000 {
        let block = u32::try_from(i).expect("fits in u32");
        assert_eq!(
            probe.lookup::<i64>(i).unwrap(),
            Some(tid(block, 0)),
            "low key {i} missing after high-key split load",
        );
    }
}

// --- v0.8 additions ---

#[test]
fn backward_scan_returns_keys_in_descending_order() {
    let mut tree = make_tree();
    for i in 0_i64..50 {
        let block = u32::try_from(i).expect("fits");
        tree.insert::<i64>(i, tid(block, 0), Xid::new(1), None)
            .unwrap();
    }
    // Backward scan from 49 with no lower bound.
    let iter = tree.backward_scan::<i64>(49_i64, None).unwrap();
    let keys: Vec<i64> = iter.map(|r| r.unwrap().0).collect();
    assert!(!keys.is_empty());
    // Verify descending order.
    for w in keys.windows(2) {
        assert!(w[0] >= w[1], "not descending: {} < {}", w[0], w[1]);
    }
}

#[test]
fn backward_scan_with_bounds_respects_range() {
    let mut tree = make_tree();
    for i in 0_i64..20 {
        let block = u32::try_from(i).expect("fits");
        tree.insert::<i64>(i, tid(block, 0), Xid::new(1), None)
            .unwrap();
    }
    // Keys [5, 15] descending.
    let iter = tree.backward_scan::<i64>(15_i64, Some(5_i64)).unwrap();
    let keys: Vec<i64> = iter.map(|r| r.unwrap().0).collect();
    // Should contain keys in [5..=15].
    for &k in &keys {
        assert!((5..=15).contains(&k), "key {k} out of [5,15]");
    }
}

#[test]
fn composite_key_encode_decode_round_trip() {
    let k: CompositeKey<3> = CompositeKey::new([1, -7, 999]);
    let mut buf = [0_u8; 24];
    k.encode_into(&mut buf);
    let decoded = CompositeKey::<3>::decode_from(&buf);
    assert_eq!(k, decoded);
}

#[test]
fn composite_key_ordering_is_lexicographic() {
    let a: CompositeKey<2> = CompositeKey::new([1, 5]);
    let b: CompositeKey<2> = CompositeKey::new([1, 6]);
    let c: CompositeKey<2> = CompositeKey::new([2, 0]);
    assert!(a < b);
    assert!(b < c);
    assert!(a < c);
}

#[test]
fn expression_index_insert_and_lookup() {
    use crate::access_method::BTreeAccessMethod;
    use ultrasql_core::Value;

    let am = BTreeAccessMethod::new(false);
    let idx = ExprIndexAdapter::new(
        am,
        // Key: first Value as 8-byte LE i64.
        |row| {
            if let Some(Value::Int64(v)) = row.first() {
                let mut buf = [0_u8; 8];
                write_i64_le(&mut buf, *v);
                buf.to_vec()
            } else {
                vec![]
            }
        },
    );
    let row = vec![Value::Int64(42)];
    idx.insert_row(&row, tid(1, 0)).unwrap();
    let mut key_buf = [0_u8; 8];
    write_i64_le(&mut key_buf, 42);
    let results = idx.lookup_key(&key_buf).unwrap();
    assert!(results.contains(&tid(1, 0)));
}

#[test]
fn partial_index_skips_rows_not_matching_predicate() {
    use crate::access_method::BTreeAccessMethod;
    use ultrasql_core::Value;

    let am = BTreeAccessMethod::new(false);
    // Only index rows where col0 > 10.
    let idx = PartialIndexAdapter::new(
        am,
        |row| {
            if let Some(Value::Int64(v)) = row.first() {
                let mut buf = [0_u8; 8];
                write_i64_le(&mut buf, *v);
                buf.to_vec()
            } else {
                vec![]
            }
        },
        |row| matches!(row.first(), Some(Value::Int64(v)) if *v > 10),
    );
    // Row with 5 — should NOT be indexed.
    idx.insert_row(&[Value::Int64(5)], tid(1, 0)).unwrap();
    // Row with 20 — should be indexed.
    idx.insert_row(&[Value::Int64(20)], tid(2, 0)).unwrap();

    let mut key_buf = [0_u8; 8];
    write_i64_le(&mut key_buf, 5);
    assert!(
        idx.lookup_key(&key_buf).unwrap().is_empty(),
        "5 should not be indexed"
    );

    write_i64_le(&mut key_buf, 20);
    assert!(
        !idx.lookup_key(&key_buf).unwrap().is_empty(),
        "20 should be indexed"
    );
}

#[test]
fn covering_index_stores_include_payload() {
    use crate::access_method::BTreeAccessMethod;
    use ultrasql_core::Value;

    let am = BTreeAccessMethod::new(true);
    let idx = CoveringIndexAdapter::new(
        am,
        // Key = col0.
        |row| {
            if let Some(Value::Int64(v)) = row.first() {
                let mut buf = [0_u8; 8];
                write_i64_le(&mut buf, *v);
                buf.to_vec()
            } else {
                vec![]
            }
        },
        // INCLUDE = col1 as 8 bytes.
        |row| {
            if let Some(Value::Int64(v)) = row.get(1) {
                let mut buf = [0_u8; 8];
                write_i64_le(&mut buf, *v);
                buf.to_vec()
            } else {
                vec![]
            }
        },
    );
    let row = vec![Value::Int64(7), Value::Int64(999)];
    idx.insert_row(&row, tid(1, 0)).unwrap();
    let mut key_buf = [0_u8; 8];
    write_i64_le(&mut key_buf, 7);
    let entries = idx.lookup_covering(&key_buf).unwrap();
    assert_eq!(entries.len(), 1);
    let expected_payload = {
        let mut buf = [0_u8; 8];
        write_i64_le(&mut buf, 999);
        buf.to_vec()
    };
    assert_eq!(entries[0].include_payload, expected_payload);
}

#[test]
fn concurrent_index_build_two_pass() {
    use crate::access_method::BTreeAccessMethod;

    let am = BTreeAccessMethod::new(false);
    let builder = ConcurrentIndexBuilder::new(am);
    assert!(builder.status().is_none());

    // Pass 1: snapshot at xid 100.
    let pass1_rows = (0_i64..5).map(|i| {
        let mut buf = [0_u8; 8];
        write_i64_le(&mut buf, i);
        (buf.to_vec(), tid(u32::try_from(i).unwrap(), 0))
    });
    builder.build_pass1(pass1_rows, 100).unwrap();
    assert_eq!(
        builder.status(),
        Some(ConcurrentBuildStatus::Pass1Complete { snapshot_xid: 100 })
    );

    // Pass 2: delta rows [5..10).
    let pass2_rows = (5_i64..10).map(|i| {
        let mut buf = [0_u8; 8];
        write_i64_le(&mut buf, i);
        (buf.to_vec(), tid(u32::try_from(i).unwrap(), 0))
    });
    builder.build_pass2(pass2_rows).unwrap();
    assert_eq!(builder.status(), Some(ConcurrentBuildStatus::Ready));

    let finished = builder.finish();
    for i in 0_i64..10 {
        let mut buf = [0_u8; 8];
        write_i64_le(&mut buf, i);
        let results = finished.lookup(&buf).unwrap();
        assert!(!results.is_empty(), "key {i} missing after CIC build");
    }
}

#[test]
fn vacuum_removes_dead_entries() {
    let mut tree = make_tree();
    for i in 0_i64..20 {
        let block = u32::try_from(i).expect("fits");
        tree.insert::<i64>(i, tid(block, 0), Xid::new(1), None)
            .unwrap();
    }
    // Mark even-keyed TIDs as dead.
    let removed = tree
        .vacuum(|t| t.page.block.raw() % 2 == 0)
        .expect("vacuum");
    assert_eq!(removed, 10, "expected 10 dead entries removed");

    // Odd keys should still be present.
    for i in (1_i64..20).step_by(2) {
        let block = u32::try_from(i).expect("fits");
        assert_eq!(
            tree.lookup::<i64>(i).unwrap(),
            Some(tid(block, 0)),
            "odd key {i} missing"
        );
    }
    // Even keys should now be missing.
    for i in (0_i64..20).step_by(2) {
        assert!(
            tree.lookup::<i64>(i).unwrap().is_none(),
            "dead key {i} still present"
        );
    }
}
