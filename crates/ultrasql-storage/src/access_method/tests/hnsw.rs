//! Runtime and page-backed HNSW unit tests (build and search).

use ultrasql_core::{RelationId, TupleId, Xid};
use ultrasql_wal::payload::{HnswOpKind, HnswOpPayload};
use ultrasql_wal::record::{RecordType};
use super::*;
use crate::wal_sink::test_support::InMemoryWalSink;

// --- HnswIndex ---

#[test]
fn hnsw_insert_vector_and_search_returns_nearest_tids() {
    let am = HnswIndex::new(3, HnswMetric::L2, 4, 16).expect("hnsw config");
    am.insert_vector(&[0.0, 0.0, 0.0], tid(1, 0))
        .expect("insert origin");
    am.insert_vector(&[1.0, 0.0, 0.0], tid(1, 1))
        .expect("insert near");
    am.insert_vector(&[10.0, 0.0, 0.0], tid(1, 2))
        .expect("insert far");

    let hits = am.search(&[0.2, 0.0, 0.0], 2).expect("search");
    let tids: Vec<TupleId> = hits.into_iter().map(|hit| hit.tid).collect();
    assert_eq!(tids, vec![tid(1, 0), tid(1, 1)]);
}

#[test]
fn hnsw_search_with_ef_overrides_exploration_budget() {
    // A small index default ef_search keeps the graph search narrow; a
    // per-query ef that covers the whole live set makes the search exact.
    let am = HnswIndex::new(2, HnswMetric::L2, 4, 2).expect("hnsw config");
    for i in 0u16..20 {
        am.insert_vector(&[f32::from(i) * 2.0, 0.0], tid(1, i))
            .expect("insert");
    }
    let probe = [0.1, 0.0];
    // Default ef_search=2 explores at most two nodes, so it returns 2 hits.
    let narrow = am.search(&probe, 3).expect("default search");
    assert_eq!(narrow.len(), 2);
    // A per-query ef >= live count makes the search exact: the true 3
    // nearest to 0.1 are ids 0 (d=0.1), 1 (d=1.9), 2 (d=3.9).
    let exact = am.search_with_ef(&probe, 3, 100).expect("boosted search");
    let tids: Vec<TupleId> = exact.into_iter().map(|hit| hit.tid).collect();
    assert_eq!(tids, vec![tid(1, 0), tid(1, 1), tid(1, 2)]);
}

#[test]
fn hnsw_invalidate_makes_index_unavailable_for_search() {
    let am = HnswIndex::new(3, HnswMetric::L2, 4, 16).expect("hnsw config");
    am.insert_vector(&[0.0, 0.0, 0.0], tid(1, 0))
        .expect("insert origin");

    assert!(am.is_available());
    am.invalidate();
    assert!(!am.is_available());
    assert!(am.search(&[0.0, 0.0, 0.0], 1).expect("search").is_empty());
}

#[test]
fn hnsw_delete_tombstone_and_vacuum_compaction_preserve_search() {
    let am = HnswIndex::new(3, HnswMetric::L2, 4, 16).expect("hnsw config");
    am.insert_vector(&[0.0, 0.0, 0.0], tid(1, 0))
        .expect("insert deleted row");
    am.insert_vector(&[1.0, 0.0, 0.0], tid(1, 1))
        .expect("insert live row");
    am.insert_vector(&[2.0, 0.0, 0.0], tid(1, 2))
        .expect("insert second live row");

    am.mark_deleted(tid(1, 0)).expect("tombstone row");
    assert_eq!(am.tombstone_count(), 1);
    assert_eq!(am.live_len(), 2);
    let hits = am.search(&[0.0, 0.0, 0.0], 2).expect("search");
    let tids: Vec<TupleId> = hits.into_iter().map(|hit| hit.tid).collect();
    assert_eq!(tids, vec![tid(1, 1), tid(1, 2)]);

    let removed = am.compact_deleted().expect("compact tombstones");
    assert_eq!(removed, 1);
    assert_eq!(am.tombstone_count(), 0);
    assert_eq!(am.live_len(), 2);
    let hits = am.search(&[0.0, 0.0, 0.0], 2).expect("search after vacuum");
    let tids: Vec<TupleId> = hits.into_iter().map(|hit| hit.tid).collect();
    assert_eq!(tids, vec![tid(1, 1), tid(1, 2)]);
}

#[test]
fn hnsw_logged_insert_delete_and_compact_emit_wal_records() {
    let am = HnswIndex::new(3, HnswMetric::L2, 4, 16).expect("hnsw config");
    let sink = InMemoryWalSink::new();
    let index_rel = RelationId::new(777);
    let tuple = tid(9, 1);

    am.insert_vector_logged(
        index_rel,
        &[0.0, 1.0, 2.0],
        tuple,
        Xid::new(10),
        Some(&sink),
    )
    .expect("logged insert");
    am.mark_deleted_logged(index_rel, tuple, Xid::new(10), Some(&sink))
        .expect("logged delete");
    am.compact_deleted_logged(index_rel, Xid::new(10), Some(&sink))
        .expect("logged compact");

    let records = sink.records();
    assert_eq!(records.len(), 3);
    assert_eq!(records[0].1.header.record_type, RecordType::HnswOp);
    let insert = HnswOpPayload::decode(&records[0].1.payload).expect("decode hnsw insert");
    assert_eq!(insert.op, HnswOpKind::Insert);
    assert_eq!(insert.index_rel, index_rel);
    assert_eq!(insert.tid, tuple);
    assert_eq!(insert.vector, vec![0.0, 1.0, 2.0]);
    let delete = HnswOpPayload::decode(&records[1].1.payload).expect("decode hnsw delete");
    assert_eq!(delete.op, HnswOpKind::Delete);
    assert_eq!(delete.tid, tuple);
    let compact = HnswOpPayload::decode(&records[2].1.payload).expect("decode hnsw compact");
    assert_eq!(compact.op, HnswOpKind::Compact);
}

#[test]
fn page_backed_hnsw_allocates_meta_node_overflow_and_free_list_pages() {
    let am = PageBackedHnswIndex::new(RelationId::new(8800), 3, HnswMetric::L2, 4, 16)
        .expect("page-backed hnsw config");

    let initial = am.page_stats();
    assert_eq!(initial.meta_pages, 1);
    assert_eq!(initial.free_list_pages, 1);
    assert_eq!(initial.node_pages, 0);
    assert_eq!(initial.overflow_pages, 0);

    am.insert_vector(&[0.0, 0.0, 0.0], tid(1, 0))
        .expect("insert origin");
    am.insert_vector(&[1.0, 0.0, 0.0], tid(1, 1))
        .expect("insert near");
    am.insert_vector(&[10.0, 0.0, 0.0], tid(1, 2))
        .expect("insert far");

    let stats = am.page_stats();
    assert_eq!(stats.live_nodes, 3);
    assert_eq!(stats.tombstones, 0);
    assert_eq!(stats.meta_pages, 1);
    assert_eq!(stats.free_list_pages, 1);
    assert_eq!(stats.node_pages, 3);
    assert!(stats.overflow_pages >= 3);
    assert_eq!(stats.reusable_pages, 0);

    let hits = am.search(&[0.2, 0.0, 0.0], 2).expect("search");
    let tids: Vec<TupleId> = hits.into_iter().map(|hit| hit.tid).collect();
    assert_eq!(tids, vec![tid(1, 0), tid(1, 1)]);
}

#[test]
fn page_backed_hnsw_graph_search_is_approximate_and_exact_with_high_ef() {
    // 200 live nodes with ef_search=8: the persistent search must traverse
    // the graph (not exhaustively scan), and a per-query ef >= live count
    // must be exact.
    let am = PageBackedHnswIndex::new(RelationId::new(8810), 2, HnswMetric::L2, 16, 8)
        .expect("page-backed hnsw config");
    for i in 0u16..200 {
        am.insert_vector(&[f32::from(i), 0.0], tid(1, i))
            .expect("insert");
    }
    let probe = [50.3_f32, 0.0];
    let k = 5;

    // Boosted ef (>= live=200) is exact: the true 5 nearest to 50.3.
    let exact: Vec<TupleId> = am
        .search_with_ef(&probe, k, 1000)
        .expect("exact search")
        .into_iter()
        .map(|hit| hit.tid)
        .collect();
    assert_eq!(
        exact,
        vec![tid(1, 50), tid(1, 51), tid(1, 49), tid(1, 52), tid(1, 48)]
    );

    // Default ef=8 traverses the graph and recovers the true neighbors with
    // high recall (the line graph navigates cleanly).
    let approx: std::collections::BTreeSet<TupleId> = am
        .search(&probe, k)
        .expect("graph search")
        .into_iter()
        .map(|hit| hit.tid)
        .collect();
    assert_eq!(approx.len(), k, "graph search must return k results");
    let overlap = exact.iter().filter(|t| approx.contains(t)).count();
    let recall =
        f64::from(u16::try_from(overlap).unwrap()) / f64::from(u16::try_from(k).unwrap());
    assert!(recall >= 0.8, "graph recall@{k} too low: {recall}");
}

#[test]
fn page_backed_hnsw_diversity_heuristic_keeps_high_recall_in_high_dim() {
    // 16-dimensional pseudo-random vectors: a plain "connect to the m
    // nearest" graph navigates this poorly (greedy descent gets trapped in
    // local clusters, recall@10 ~0.66), while the HNSW diversity heuristic
    // preserves the long-range bridge edges that keep recall high. This test
    // would fail on the pre-heuristic build.
    const DIMS: usize = 16;
    const N: u16 = 600;
    let dims_u32 = u32::try_from(DIMS).expect("dims fit u32");
    let am = PageBackedHnswIndex::new(RelationId::new(8811), dims_u32, HnswMetric::L2, 16, 64)
        .expect("page-backed hnsw config");
    let mut rng = 0x1234_5678_9abc_def0_u64;
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
    assert!(
        mean >= 0.9,
        "diversity-heuristic recall@{k} too low: {mean} (pre-heuristic was ~0.66)"
    );
}

#[test]
fn page_backed_hnsw_graph_traversal_build_keeps_high_recall() {
    // Force the graph-traversal build path (`collect_construction_candidates`)
    // at small N via a zero work threshold — production crosses it at ~8k
    // vectors. The navigable graph the traversal produces must still answer
    // queries with high recall: the recall side of the O(N²)→sub-quadratic
    // build fix.
    const DIMS: usize = 16;
    const N: u16 = 1200;
    let dims_u32 = u32::try_from(DIMS).expect("dims fit u32");
    let am = PageBackedHnswIndex::new(RelationId::new(8821), dims_u32, HnswMetric::L2, 16, 64)
        .expect("page-backed hnsw config")
        .with_build_traversal_work_threshold(0);
    let mut rng = 0x0f1e_2d3c_4b5a_6978_u64;
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
            .search_with_ef(&probe, k, 128)
            .expect("graph search")
            .into_iter()
            .map(|hit| hit.tid)
            .collect();
        let overlap = want.iter().filter(|t| got.contains(t)).count();
        recall_sum += f64::from(u16::try_from(overlap).expect("overlap fits u16"))
            / f64::from(u16::try_from(k).expect("k fits u16"));
    }
    let mean = recall_sum / f64::from(trials);
    assert!(
        mean >= 0.95,
        "graph-traversal-build recall@{k} too low: {mean} (target >= 0.95 at ef<=128)"
    );
}

#[test]
fn page_backed_hnsw_traversal_build_is_deterministic_for_replay() {
    // The traversal build must be deterministic: replaying the same insert
    // sequence (e.g. during WAL recovery) has to reconstruct an identical
    // graph, or recovery would diverge from the durable index. Build two
    // indexes from the same vectors past the ef_construction threshold and
    // assert every node's neighbor list matches byte-for-byte.
    const DIMS: usize = 12;
    const N: u16 = 500;
    let dims_u32 = u32::try_from(DIMS).expect("dims fit u32");
    let build = || {
        let am =
            PageBackedHnswIndex::new(RelationId::new(8822), dims_u32, HnswMetric::L2, 12, 32)
                .expect("page-backed hnsw config")
                .with_build_traversal_work_threshold(0);
        let mut rng = 0xdead_beef_cafe_f00d_u64;
        let mut next_unit = move || {
            rng = rng
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let bits = u16::try_from((rng >> 48) & 0xFFFF).expect("16 bits fit u16");
            f32::from(bits) / f32::from(u16::MAX)
        };
        for i in 0..N {
            let v: Vec<f32> = (0..DIMS).map(|_| next_unit()).collect();
            am.insert_vector(&v, tid(1, i)).expect("insert");
        }
        am
    };
    let first = build();
    let second = build();
    assert_eq!(
        first.debug_neighbor_lists(),
        second.debug_neighbor_lists(),
        "traversal build must be deterministic for WAL replay"
    );
}
