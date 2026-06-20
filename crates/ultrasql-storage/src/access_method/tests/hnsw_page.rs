//! Page-backed HNSW persistence, replay, and snapshot unit tests.

use super::*;
use crate::wal_sink::test_support::InMemoryWalSink;
use proptest::prelude::*;
use ultrasql_core::{BlockNumber, Lsn, PageId, RelationId, TupleId, Xid};
use ultrasql_wal::record::{RecordType, WalRecord};

#[test]
fn page_backed_hnsw_mirror_stays_consistent_through_dml_and_reload() {
    // The in-memory mirror (the O(1) read accelerator behind traversal and
    // search) must stay byte-for-byte consistent with the durable pages
    // across insert, delete, vacuum, and snapshot reload — otherwise search
    // would silently diverge from the on-disk graph.
    const DIMS: usize = 12;
    const N: u16 = 400;
    let dims_u32 = u32::try_from(DIMS).expect("dims fit u32");
    let index_rel = RelationId::new(8824);
    let am = PageBackedHnswIndex::new(index_rel, dims_u32, HnswMetric::L2, 12, 32)
        .expect("page-backed hnsw config")
        .with_build_traversal_work_threshold(0);
    let mut rng = 0x0bad_c0de_1234_5678_u64;
    let mut next_unit = || {
        rng = rng
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        let bits = u16::try_from((rng >> 48) & 0xFFFF).expect("16 bits fit u16");
        f32::from(bits) / f32::from(u16::MAX)
    };
    let mut vectors: Vec<(TupleId, Vec<f32>)> = Vec::new();
    for i in 0..N {
        let v: Vec<f32> = (0..DIMS).map(|_| next_unit()).collect();
        am.insert_vector(&v, tid(1, i)).expect("insert");
        vectors.push((tid(1, i), v));
    }
    am.assert_mirror_consistent();

    // Tombstone every fifth vector, then vacuum it out.
    for i in (0..N).step_by(5) {
        am.mark_deleted(tid(1, i)).expect("delete");
    }
    am.assert_mirror_consistent();
    am.vacuum_deleted().expect("vacuum");
    am.assert_mirror_consistent();

    // Search after DML must find the true nearest among the live set (the
    // mirror is what search reads).
    let live: Vec<(TupleId, &Vec<f32>)> = vectors
        .iter()
        .enumerate()
        .filter(|(i, _)| i % 5 != 0)
        .map(|(_, (t, v))| (*t, v))
        .collect();
    let k = 10;
    let mut recall_sum = 0.0_f64;
    let trials = 20;
    for _ in 0..trials {
        let probe: Vec<f32> = (0..DIMS).map(|_| next_unit()).collect();
        let mut exact: Vec<(f32, TupleId)> = live
            .iter()
            .map(|(t, v)| (HnswMetric::L2.distance(&probe, v), *t))
            .collect();
        exact.sort_by(|a, b| a.0.total_cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
        let want: std::collections::BTreeSet<TupleId> =
            exact.iter().take(k).map(|(_, t)| *t).collect();
        let got: std::collections::BTreeSet<TupleId> = am
            .search_with_ef(&probe, k, 64)
            .expect("search")
            .into_iter()
            .map(|hit| hit.tid)
            .collect();
        // Every returned tid must be a live one (no tombstoned/vacuumed leak).
        for t in &got {
            assert!(
                live.iter().any(|(lt, _)| lt == t),
                "search returned a non-live tid after vacuum"
            );
        }
        let overlap = want.iter().filter(|t| got.contains(t)).count();
        recall_sum += f64::from(u16::try_from(overlap).expect("overlap fits u16"))
            / f64::from(u16::try_from(k).expect("k fits u16"));
    }
    assert!(
        recall_sum / f64::from(trials) >= 0.9,
        "post-vacuum recall@{k} too low"
    );

    // A snapshot reload rebuilds the mirror from pages alone; it must be
    // consistent and return identical search results.
    let probe: Vec<f32> = (0..DIMS).map(|_| next_unit()).collect();
    let before: Vec<TupleId> = am
        .search(&probe, k)
        .expect("search before reload")
        .into_iter()
        .map(|hit| hit.tid)
        .collect();
    let bytes = am.encode_snapshot();
    let restored =
        PageBackedHnswIndex::from_snapshot_bytes(index_rel, &bytes).expect("snapshot decodes");
    restored.assert_mirror_consistent();
    let after: Vec<TupleId> = restored
        .search(&probe, k)
        .expect("search after reload")
        .into_iter()
        .map(|hit| hit.tid)
        .collect();
    assert_eq!(before, after, "search results must survive snapshot reload");
}

#[test]
fn hnsw_node_page_round_trips_upper_layers() {
    // The v2 node format must round-trip a multi-layer node (per-layer
    // neighbor chains) exactly, even though the build does not yet produce
    // levels > 0 — this exercises the durable format in isolation.
    let rel = RelationId::new(9001);
    let block = BlockNumber::new(7);
    let node = HnswNodePage {
        page_id: PageId::new(rel, block),
        lsn: Lsn::new(42),
        node_id: 5,
        tid: tid(1, 9),
        vector_len: 4,
        vector_head: BlockNumber::new(8),
        neighbor_count: 3,
        neighbor_head: Some(BlockNumber::new(10)),
        level: 2,
        upper_levels: vec![
            HnswLevelNeighbors {
                head: Some(BlockNumber::new(11)),
                count: 2,
            },
            HnswLevelNeighbors {
                head: None,
                count: 0,
            },
        ],
        deleted: false,
    };
    let image = PageBackedHnswPageImage {
        page_id: PageId::new(rel, block),
        lsn: Lsn::new(42),
        page: HnswPersistentPage::Node(node),
    };
    let mut bytes = Vec::new();
    encode_hnsw_page_record(&mut bytes, &image);
    let mut cursor = SnapshotCursor::new(&bytes);
    let decoded =
        decode_hnsw_page_record(&mut cursor, rel, AnnPayloadKind::F32, HNSW_SNAPSHOT_VERSION)
            .expect("decode v2 node");
    assert!(cursor.is_empty(), "no trailing bytes after a node record");
    let HnswPersistentPage::Node(got) = decoded.page else {
        panic!("expected a node page");
    };
    assert_eq!(got.level, 2);
    assert_eq!(got.neighbor_head, Some(BlockNumber::new(10)));
    assert_eq!(got.neighbor_count, 3);
    assert_eq!(got.upper_levels.len(), 2);
    assert_eq!(got.upper_levels[0].head, Some(BlockNumber::new(11)));
    assert_eq!(got.upper_levels[0].count, 2);
    assert_eq!(got.upper_levels[1].head, None);
    assert_eq!(got.upper_levels[1].count, 0);
}

#[test]
fn hnsw_node_page_v1_decodes_as_base_only() {
    // A v1 node record has no upper-layer trailer. Decoding it under the
    // legacy version must yield a base-only (level 0) node and consume the
    // record exactly — proving backward compatibility with pre-hierarchical
    // on-disk snapshots.
    let rel = RelationId::new(9002);
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&BlockNumber::new(3).raw().to_le_bytes());
    bytes.extend_from_slice(&Lsn::new(7).raw().to_le_bytes());
    bytes.push(HNSW_PAGE_KIND_NODE);
    bytes.extend_from_slice(&12_u64.to_le_bytes());
    push_tuple_id(&mut bytes, tid(2, 4));
    push_len(&mut bytes, 4);
    bytes.extend_from_slice(&BlockNumber::new(5).raw().to_le_bytes());
    push_len(&mut bytes, 2);
    push_opt_block(&mut bytes, Some(BlockNumber::new(6)));
    bytes.push(0_u8); // not deleted; no v2 trailer follows
    let mut cursor = SnapshotCursor::new(&bytes);
    let decoded =
        decode_hnsw_page_record(&mut cursor, rel, AnnPayloadKind::F32, 1).expect("decode v1 node");
    assert!(cursor.is_empty(), "v1 record consumed with no trailer");
    let HnswPersistentPage::Node(got) = decoded.page else {
        panic!("expected a node page");
    };
    assert_eq!(got.level, 0);
    assert!(got.upper_levels.is_empty());
    assert_eq!(got.node_id, 12);
    assert_eq!(got.neighbor_count, 2);
    assert_eq!(got.neighbor_head, Some(BlockNumber::new(6)));
}

#[test]
fn page_backed_hnsw_build_is_multi_layer_and_recalls_well() {
    // The hierarchical build must actually create upper layers (m=16 gives
    // ~1/16 of nodes a level >= 1) and the layered navigation must answer
    // queries with high recall.
    const DIMS: usize = 16;
    const N: u16 = 3000;
    let dims_u32 = u32::try_from(DIMS).expect("dims fit u32");
    let am = PageBackedHnswIndex::new(RelationId::new(8830), dims_u32, HnswMetric::L2, 16, 64)
        .expect("page-backed hnsw config");
    let mut rng = 0xa5a5_5a5a_1234_9e37_u64;
    let mut next_unit = || {
        rng = rng
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        let bits = u16::try_from((rng >> 48) & 0xFFFF).expect("16 bits fit u16");
        f32::from(bits) / f32::from(u16::MAX)
    };
    let mut vectors: Vec<(TupleId, Vec<f32>)> = Vec::new();
    for i in 0..N {
        let v: Vec<f32> = (0..DIMS).map(|_| next_unit()).collect();
        am.insert_vector(&v, tid(1, i)).expect("insert");
        vectors.push((tid(1, i), v));
    }

    // The graph must be genuinely hierarchical.
    let levels: Vec<usize> = am
        .debug_neighbor_lists()
        .iter()
        .map(|(_, level, _)| *level)
        .collect();
    let max_level = levels.iter().copied().max().unwrap_or(0);
    let upper = levels.iter().filter(|level| **level >= 1).count();
    assert!(
        max_level >= 1,
        "hierarchical build should produce upper layers, got max level {max_level}"
    );
    assert!(
        upper >= 1,
        "expected some nodes promoted above the base layer, got {upper}"
    );

    let k = 10;
    let mut recall_sum = 0.0_f64;
    let trials = 30;
    for _ in 0..trials {
        let probe: Vec<f32> = (0..DIMS).map(|_| next_unit()).collect();
        let mut exact: Vec<(f32, TupleId)> = vectors
            .iter()
            .map(|(t, v)| (HnswMetric::L2.distance(&probe, v), *t))
            .collect();
        exact.sort_by(|a, b| a.0.total_cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
        let want: std::collections::BTreeSet<TupleId> =
            exact.iter().take(k).map(|(_, t)| *t).collect();
        let got: std::collections::BTreeSet<TupleId> = am
            .search_with_ef(&probe, k, 64)
            .expect("graph search")
            .into_iter()
            .map(|hit| hit.tid)
            .collect();
        let overlap = want.iter().filter(|t| got.contains(t)).count();
        recall_sum += f64::from(u16::try_from(overlap).expect("overlap fits u16"))
            / f64::from(u16::try_from(k).expect("k fits u16"));
    }
    let mean = recall_sum / f64::from(trials);
    assert!(mean >= 0.9, "multi-layer recall@{k} too low: {mean}");
}

#[test]
fn page_backed_hnsw_vacuum_reclaims_node_and_overflow_pages() {
    let am = PageBackedHnswIndex::new(RelationId::new(8801), 3, HnswMetric::L2, 2, 16)
        .expect("page-backed hnsw config");
    am.insert_vector(&[0.0, 0.0, 0.0], tid(1, 0))
        .expect("insert deleted row");
    am.insert_vector(&[1.0, 0.0, 0.0], tid(1, 1))
        .expect("insert live row");
    am.insert_vector(&[2.0, 0.0, 0.0], tid(1, 2))
        .expect("insert second live row");

    am.mark_deleted(tid(1, 0)).expect("tombstone row");
    assert_eq!(am.page_stats().tombstones, 1);

    let removed = am.vacuum_deleted().expect("vacuum hnsw pages");
    assert_eq!(removed, 1);
    let after_vacuum = am.page_stats();
    assert_eq!(after_vacuum.live_nodes, 2);
    assert_eq!(after_vacuum.tombstones, 0);
    assert!(after_vacuum.reusable_pages > 0);

    am.insert_vector(&[3.0, 0.0, 0.0], tid(1, 3))
        .expect("insert reuses free pages");
    let after_reuse = am.page_stats();
    assert_eq!(after_reuse.live_nodes, 3);
    assert!(after_reuse.next_block_number <= after_vacuum.next_block_number);
}

#[test]
fn page_backed_hnsw_replays_wal_into_recovered_pages() {
    let index_rel = RelationId::new(8802);
    let source =
        PageBackedHnswIndex::new(index_rel, 3, HnswMetric::L2, 4, 16).expect("source config");
    let sink = InMemoryWalSink::new();
    source
        .insert_vector_logged(&[0.0, 0.0, 0.0], tid(1, 0), Xid::new(12), Some(&sink))
        .expect("logged insert origin");
    source
        .insert_vector_logged(&[1.0, 0.0, 0.0], tid(1, 1), Xid::new(12), Some(&sink))
        .expect("logged insert live");
    source
        .mark_deleted_logged(tid(1, 0), Xid::new(12), Some(&sink))
        .expect("logged delete");
    source
        .vacuum_deleted_logged(Xid::new(12), Some(&sink))
        .expect("logged vacuum");

    let recovered =
        PageBackedHnswIndex::new(index_rel, 3, HnswMetric::L2, 4, 16).expect("recover config");
    let records = sink.records();
    for (_, record) in &records {
        recovered.apply_wal_record(record).expect("replay hnsw WAL");
    }
    for (_, record) in &records {
        recovered
            .apply_wal_record(record)
            .expect("replay hnsw WAL idempotently");
    }

    let stats = recovered.page_stats();
    assert_eq!(stats.live_nodes, 1);
    assert_eq!(stats.tombstones, 0);
    let hits = recovered.search(&[0.0, 0.0, 0.0], 2).expect("search");
    let tids: Vec<TupleId> = hits.into_iter().map(|hit| hit.tid).collect();
    assert_eq!(tids, vec![tid(1, 1)]);
}

#[test]
fn page_backed_hnsw_stamps_page_lsns_and_restores_page_images() {
    let index_rel = RelationId::new(8803);
    let am = PageBackedHnswIndex::new(index_rel, 3, HnswMetric::L2, 4, 16).expect("hnsw config");
    let sink = InMemoryWalSink::new();

    am.insert_vector_logged(&[0.0, 0.0, 0.0], tid(1, 0), Xid::new(13), Some(&sink))
        .expect("logged insert");

    let records = sink.records();
    let assigned_lsn = records[0].0;
    assert!(assigned_lsn > Lsn::ZERO);
    let images = am.page_images();
    assert!(images.len() >= 4);
    assert!(
        images
            .iter()
            .all(|image| image.page_id.relation == index_rel && image.lsn == assigned_lsn)
    );

    let restored =
        PageBackedHnswIndex::from_page_images(index_rel, 3, HnswMetric::L2, 4, 16, images)
            .expect("restore hnsw pages");
    assert_eq!(restored.page_stats().live_nodes, 1);
    let hits = restored.search(&[0.1, 0.0, 0.0], 1).expect("search");
    assert_eq!(hits[0].tid, tid(1, 0));
}

#[test]
fn page_backed_hnsw_restore_rejects_duplicate_node_ids() {
    let index_rel = RelationId::new(8813);
    let source =
        PageBackedHnswIndex::new(index_rel, 3, HnswMetric::L2, 4, 16).expect("hnsw config");
    source
        .insert_vector(&[0.0, 0.0, 0.0], tid(1, 0))
        .expect("insert node");

    let mut images = source.page_images();
    let mut duplicate = images
        .iter()
        .find(|image| matches!(image.page, HnswPersistentPage::Node(_)))
        .expect("node image exists")
        .clone();
    duplicate.page_id = PageId::new(index_rel, BlockNumber::new(99_999));
    let HnswPersistentPage::Node(node) = &mut duplicate.page else {
        unreachable!("selected node page");
    };
    node.page_id = duplicate.page_id;
    node.tid = tid(1, 1);
    images.push(duplicate);

    let err = PageBackedHnswIndex::from_page_images(index_rel, 3, HnswMetric::L2, 4, 16, images)
        .expect_err("duplicate node ids must be refused");

    assert!(format!("{err}").contains("duplicate node id"));
}

#[test]
fn page_backed_hnsw_redo_skips_records_covered_by_page_lsn() {
    let index_rel = RelationId::new(8804);
    let source =
        PageBackedHnswIndex::new(index_rel, 3, HnswMetric::L2, 4, 16).expect("source config");
    let sink = InMemoryWalSink::new();
    source
        .insert_vector_logged(&[0.0, 0.0, 0.0], tid(1, 0), Xid::new(14), Some(&sink))
        .expect("logged insert one");
    source
        .insert_vector_logged(&[1.0, 0.0, 0.0], tid(1, 1), Xid::new(14), Some(&sink))
        .expect("logged insert two");

    let images_after_second = source.page_images();
    let recovered = PageBackedHnswIndex::from_page_images(
        index_rel,
        3,
        HnswMetric::L2,
        4,
        16,
        images_after_second,
    )
    .expect("restore hnsw pages");
    let stats_before = recovered.page_stats();

    let records = sink.records();
    for (lsn, record) in records {
        recovered
            .apply_wal_record_at(lsn, &record)
            .expect("redo should skip covered LSN");
    }

    assert_eq!(recovered.page_stats(), stats_before);
    let hits = recovered.search(&[0.0, 0.0, 0.0], 2).expect("search");
    let tids: Vec<TupleId> = hits.into_iter().map(|hit| hit.tid).collect();
    assert_eq!(tids, vec![tid(1, 0), tid(1, 1)]);
}

proptest::proptest! {
    #[test]
    fn page_backed_hnsw_rejects_random_wal_payloads_without_panicking(
        payload in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..128_usize),
    ) {
        let index = PageBackedHnswIndex::new(RelationId::new(8805), 3, HnswMetric::L2, 4, 16)
            .expect("hnsw config");
        let record = WalRecord::new(RecordType::HnswOp, Xid::new(15), Lsn::ZERO, 0, payload)
            .expect("test WAL record should fit size limits");

        let replay = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            index.apply_wal_record(&record)
        }));

        prop_assert!(replay.is_ok(), "HNSW WAL replay panicked");
        if let Ok(Ok(())) = replay {
            prop_assert!(index.page_stats().live_nodes <= 1);
        }
    }
}

/// Build a 4-dim page-backed HNSW index with the given payload kind and
/// ~30 distinct vectors. `m = 2` with 30 inserts forces neighbor overflow
/// chains, so Node/Overflow(Vector)/Overflow(Neighbors)/FreeList page kinds
/// all appear in the snapshot.
fn build_snapshot_index(
    index_rel: RelationId,
    payload_kind: AnnPayloadKind,
) -> PageBackedHnswIndex {
    let am = PageBackedHnswIndex::new_with_payload_kind(
        index_rel,
        4,
        HnswMetric::L2,
        2,
        32,
        payload_kind,
    )
    .expect("snapshot index config");
    for i in 0..30_u32 {
        let f = i as f32;
        let vector = [f, f * 0.5 + 1.0, 10.0 - f, (i % 7) as f32];
        am.insert_vector(&vector, tid(7, u16::try_from(i).expect("slot fits u16")))
            .expect("insert snapshot vector");
    }
    am
}

#[test]
fn hnsw_snapshot_round_trips_search_results() {
    let query = [3.0_f32, 2.0, 7.0, 1.0];
    for (rel, kind) in [
        (9_910_u32, AnnPayloadKind::F32),
        (9_911, AnnPayloadKind::Bf16),
        (9_912, AnnPayloadKind::Int8),
    ] {
        let index_rel = RelationId::new(rel);
        let am = build_snapshot_index(index_rel, kind);

        // A node with more than `m` neighbors guarantees a neighbor overflow
        // chain; confirm overflow pages exist so the encoding is exercised.
        let stats = am.page_stats();
        assert!(
            stats.overflow_pages > 0,
            "expected overflow pages for kind {kind:?}"
        );

        let expected = am.search(&query, 5).expect("source search");
        let expected_tids: Vec<TupleId> = expected.iter().map(|hit| hit.tid).collect();
        assert!(!expected_tids.is_empty());
        let expected_pages = am.page_images().len();
        let expected_lsn = am.snapshot_lsn();

        let bytes = am.encode_snapshot();
        let restored =
            PageBackedHnswIndex::from_snapshot_bytes(index_rel, &bytes).expect("snapshot decodes");

        assert_eq!(restored.payload_kind(), kind, "payload kind preserved");
        assert_eq!(
            restored.page_images().len(),
            expected_pages,
            "page count preserved for kind {kind:?}"
        );
        assert_eq!(
            restored.snapshot_lsn(),
            expected_lsn,
            "snapshot lsn preserved for kind {kind:?}"
        );

        let restored_hits = restored.search(&query, 5).expect("restored search");
        let restored_tids: Vec<TupleId> = restored_hits.iter().map(|hit| hit.tid).collect();
        assert_eq!(
            restored_tids, expected_tids,
            "top-k tids preserved for kind {kind:?}"
        );
    }
}

#[test]
fn hnsw_snapshot_rejects_corruption() {
    let index_rel = RelationId::new(9_913);
    let am = build_snapshot_index(index_rel, AnnPayloadKind::Int8);
    let bytes = am.encode_snapshot();

    // Sanity: the pristine snapshot decodes.
    PageBackedHnswIndex::from_snapshot_bytes(index_rel, &bytes).expect("pristine snapshot decodes");

    // (a) Flip one byte in the middle of the buffer.
    let mut flipped = bytes.clone();
    let mid = flipped.len() / 2;
    flipped[mid] ^= 0xFF;
    assert!(
        PageBackedHnswIndex::from_snapshot_bytes(index_rel, &flipped).is_err(),
        "flipped byte must be rejected"
    );

    // (b) Truncate the buffer.
    let truncated = &bytes[..bytes.len() - 5];
    assert!(
        PageBackedHnswIndex::from_snapshot_bytes(index_rel, truncated).is_err(),
        "truncated buffer must be rejected"
    );

    // (c) Corrupt the magic header.
    let mut bad_magic = bytes.clone();
    bad_magic[0] ^= 0xFF;
    assert!(
        PageBackedHnswIndex::from_snapshot_bytes(index_rel, &bad_magic).is_err(),
        "corrupt magic must be rejected"
    );

    // A relation mismatch is also refused (defense in depth).
    assert!(
        PageBackedHnswIndex::from_snapshot_bytes(RelationId::new(1), &bytes).is_err(),
        "relation mismatch must be rejected"
    );
}
