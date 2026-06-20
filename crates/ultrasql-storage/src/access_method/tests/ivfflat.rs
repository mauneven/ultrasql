//! ivfflat access-method unit tests.

use super::*;
use crate::wal_sink::test_support::InMemoryWalSink;
use proptest::prelude::*;
use ultrasql_core::{Lsn, RelationId, TupleId, Xid};
use ultrasql_wal::payload::{IvfFlatOpKind, IvfFlatOpPayload};
use ultrasql_wal::record::{RecordType, WalRecord};

#[test]
fn page_backed_ivfflat_rejects_random_wal_payloads_without_panicking() {
    proptest!(|(payload in proptest::collection::vec(any::<u8>(), 0..128_usize))| {
        let index =
            PageBackedIvfFlatIndex::new(RelationId::new(9903), 3, HnswMetric::L2, 2, 1)
                .expect("ivfflat config");
        let record = WalRecord::new(RecordType::IvfFlatOp, Xid::new(16), Lsn::ZERO, 0, payload)
            .expect("test WAL record should fit size limits");

        let replay = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            index.apply_wal_record(&record)
        }));

        prop_assert!(replay.is_ok(), "IVFFlat WAL replay panicked");
        if let Ok(Ok(())) = replay {
            prop_assert!(index.page_stats().live_entries <= 1);
        }
    });
}

#[test]
fn ivfflat_bulk_load_trains_centroids_and_reranks_candidates() {
    let am = IvfFlatIndex::new(2, HnswMetric::L2, 2, 1).expect("ivfflat config");
    am.bulk_load(vec![
        (vec![0.0, 0.0], tid(1, 0)),
        (vec![0.5, 0.0], tid(1, 1)),
        (vec![10.0, 0.0], tid(2, 0)),
        (vec![9.0, 0.0], tid(2, 1)),
    ])
    .expect("bulk load ivfflat");

    assert_eq!(am.centroid_count(), 2);
    assert_eq!(am.list_count(), 2);
    assert_eq!(am.probes(), 1);
    let hits = am.search(&[9.2, 0.0], 2).expect("ivfflat search");
    let tids: Vec<TupleId> = hits.into_iter().map(|hit| hit.tid).collect();
    assert_eq!(tids, vec![tid(2, 1), tid(2, 0)]);
}

#[test]
fn ivfflat_insert_delete_and_compact_keep_lists_searchable() {
    let am = IvfFlatIndex::new(2, HnswMetric::L2, 2, 2).expect("ivfflat config");
    am.bulk_load(vec![
        (vec![0.0, 0.0], tid(1, 0)),
        (vec![10.0, 0.0], tid(2, 0)),
    ])
    .expect("bulk load ivfflat");
    am.insert_vector(&[1.0, 0.0], tid(1, 1))
        .expect("insert ivfflat");
    am.mark_deleted(tid(1, 0)).expect("delete ivfflat");

    assert_eq!(am.tombstone_count(), 1);
    let hits = am.search(&[0.0, 0.0], 2).expect("search after delete");
    let tids: Vec<TupleId> = hits.into_iter().map(|hit| hit.tid).collect();
    assert_eq!(tids, vec![tid(1, 1), tid(2, 0)]);

    assert_eq!(am.compact_deleted().expect("compact ivfflat"), 1);
    assert_eq!(am.tombstone_count(), 0);
    assert_eq!(am.live_len(), 2);
}

#[test]
fn ivfflat_rejects_duplicate_bulk_load_tids() {
    let am = IvfFlatIndex::new(2, HnswMetric::L2, 2, 1).expect("ivfflat config");

    let err = am
        .bulk_load(vec![
            (vec![0.0, 0.0], tid(1, 0)),
            (vec![1.0, 0.0], tid(1, 0)),
        ])
        .expect_err("duplicate tuple IDs should be rejected");

    assert!(matches!(err, AccessMethodError::DuplicateKey));
    assert!(!am.is_available());
    assert_eq!(am.live_len(), 0);
}

#[test]
fn page_backed_ivfflat_rejects_duplicate_bulk_load_tids_atomically() {
    let index_rel = RelationId::new(9899);
    let index =
        PageBackedIvfFlatIndex::new(index_rel, 2, HnswMetric::L2, 2, 1).expect("ivfflat config");

    index
        .bulk_load_logged(vec![(vec![0.0, 0.0], tid(1, 0))], Xid::new(29), None)
        .expect("initial bulk load");

    let err = index
        .bulk_load_logged(
            vec![(vec![10.0, 0.0], tid(2, 0)), (vec![11.0, 0.0], tid(2, 0))],
            Xid::new(30),
            None,
        )
        .expect_err("duplicate tuple IDs should be rejected before mutation");

    assert!(matches!(err, AccessMethodError::DuplicateKey));
    assert_eq!(index.page_stats().live_entries, 1);
    let hits = index.search(&[0.0, 0.0], 1).expect("search old index");
    let tids: Vec<TupleId> = hits.into_iter().map(|hit| hit.tid).collect();
    assert_eq!(tids, vec![tid(1, 0)]);
}

#[test]
fn page_backed_ivfflat_replays_centroids_lists_and_deletes() {
    let index_rel = RelationId::new(9900);
    let source =
        PageBackedIvfFlatIndex::new(index_rel, 2, HnswMetric::L2, 2, 1).expect("ivfflat config");
    let sink = InMemoryWalSink::new();

    source
        .bulk_load_logged(
            vec![
                (vec![0.0, 0.0], tid(1, 0)),
                (vec![1.0, 0.0], tid(1, 1)),
                (vec![9.0, 0.0], tid(2, 0)),
                (vec![10.0, 0.0], tid(2, 1)),
            ],
            Xid::new(30),
            Some(&sink),
        )
        .expect("bulk load logged");
    source
        .insert_vector_logged(&[9.5, 0.0], tid(2, 2), Xid::new(31), Some(&sink))
        .expect("logged insert");
    source
        .mark_deleted_logged(tid(1, 0), Xid::new(32), Some(&sink))
        .expect("logged delete");
    source
        .compact_deleted_logged(Xid::new(33), Some(&sink))
        .expect("logged compact");

    let records = sink.records();
    assert!(
        records
            .iter()
            .any(|(_, record)| record.header.record_type == RecordType::IvfFlatOp)
    );
    let first_payload =
        IvfFlatOpPayload::decode(&records[0].1.payload).expect("decode ivfflat WAL");
    assert_eq!(first_payload.op, IvfFlatOpKind::Centroid);
    assert_eq!(first_payload.index_rel, index_rel);

    let recovered = PageBackedIvfFlatIndex::new(index_rel, 2, HnswMetric::L2, 2, 1)
        .expect("recovered ivfflat config");
    for (lsn, record) in &records {
        recovered
            .apply_wal_record_at(*lsn, record)
            .expect("replay ivfflat WAL");
    }
    for (lsn, record) in &records {
        recovered
            .apply_wal_record_at(*lsn, record)
            .expect("replay ivfflat WAL idempotently");
    }

    let stats = recovered.page_stats();
    assert_eq!(stats.meta_pages, 1);
    assert_eq!(stats.centroid_pages, 2);
    assert_eq!(stats.list_pages, 2);
    assert_eq!(stats.live_entries, 4);
    assert_eq!(stats.tombstones, 0);
    assert!(stats.entry_pages >= 4);
    assert!(stats.next_block_number >= 5);

    let hits = recovered.search(&[9.4, 0.0], 3).expect("search");
    let tids: Vec<TupleId> = hits.into_iter().map(|hit| hit.tid).collect();
    assert_eq!(tids, vec![tid(2, 2), tid(2, 0), tid(2, 1)]);
}

#[test]
fn ann_quantized_payloads_keep_exact_f32_rerank_vectors() {
    let vector = vec![1.25, -2.5, 0.125];
    let bf16 = AnnVectorPayload::new(AnnPayloadKind::Bf16, &vector).expect("bf16 payload builds");
    assert_eq!(bf16.kind(), AnnPayloadKind::Bf16);
    assert_eq!(bf16.rerank_policy(), AnnRerankPolicy::ExactF32);
    assert_eq!(bf16.exact_f32(), vector.as_slice());
    assert_eq!(bf16.quantized_len_bytes(), vector.len() * 2);

    let int8 = AnnVectorPayload::new(AnnPayloadKind::Int8, &vector).expect("int8 payload builds");
    assert_eq!(int8.kind(), AnnPayloadKind::Int8);
    assert_eq!(int8.rerank_policy(), AnnRerankPolicy::ExactF32);
    assert_eq!(int8.exact_f32(), vector.as_slice());
    assert_eq!(int8.quantized_len_bytes(), vector.len());

    let hnsw = PageBackedHnswIndex::new_with_payload_kind(
        RelationId::new(9901),
        3,
        HnswMetric::L2,
        4,
        16,
        AnnPayloadKind::Bf16,
    )
    .expect("hnsw bf16 config");
    assert_eq!(hnsw.payload_kind(), AnnPayloadKind::Bf16);
    assert_eq!(hnsw.rerank_policy(), AnnRerankPolicy::ExactF32);

    let ivfflat = PageBackedIvfFlatIndex::new_with_payload_kind(
        RelationId::new(9902),
        3,
        HnswMetric::L2,
        2,
        1,
        AnnPayloadKind::Int8,
    )
    .expect("ivfflat int8 config");
    assert_eq!(ivfflat.payload_kind(), AnnPayloadKind::Int8);
    assert_eq!(ivfflat.rerank_policy(), AnnRerankPolicy::ExactF32);
}

/// Build a small logged IVFFlat index whose `snapshot_lsn` is advanced by real
/// WAL LSNs, leaving one tombstone so the snapshot exercises deleted entries.
fn build_ivfflat_snapshot_index(
    index_rel: RelationId,
    kind: AnnPayloadKind,
) -> PageBackedIvfFlatIndex {
    let index =
        PageBackedIvfFlatIndex::new_with_payload_kind(index_rel, 2, HnswMetric::L2, 2, 1, kind)
            .expect("ivfflat config");
    let sink = InMemoryWalSink::new();
    index
        .bulk_load_logged(
            vec![
                (vec![0.0, 0.0], tid(1, 0)),
                (vec![1.0, 0.0], tid(1, 1)),
                (vec![9.0, 0.0], tid(2, 0)),
                (vec![10.0, 0.0], tid(2, 1)),
            ],
            Xid::new(40),
            Some(&sink),
        )
        .expect("bulk load logged");
    index
        .insert_vector_logged(&[9.5, 0.0], tid(2, 2), Xid::new(41), Some(&sink))
        .expect("logged insert");
    index
        .mark_deleted_logged(tid(1, 0), Xid::new(42), Some(&sink))
        .expect("logged delete");
    index
}

#[test]
fn ivfflat_snapshot_round_trips_search_results() {
    let query = [9.4_f32, 0.0];
    for (rel, kind) in [
        (9_920_u32, AnnPayloadKind::F32),
        (9_921, AnnPayloadKind::Bf16),
        (9_922, AnnPayloadKind::Int8),
    ] {
        let index_rel = RelationId::new(rel);
        let source = build_ivfflat_snapshot_index(index_rel, kind);

        let expected = source.search(&query, 3).expect("source search");
        let expected_tids: Vec<TupleId> = expected.iter().map(|hit| hit.tid).collect();
        assert!(!expected_tids.is_empty());
        let expected_lsn = source.snapshot_lsn();
        assert!(
            expected_lsn.raw() != 0,
            "logged ops must advance the snapshot lsn for kind {kind:?}"
        );
        let expected_tombstones = source.tombstone_count();

        let bytes = source.encode_snapshot();
        let restored = PageBackedIvfFlatIndex::from_snapshot_bytes(index_rel, &bytes)
            .expect("snapshot decodes");

        assert_eq!(restored.payload_kind(), kind, "payload kind preserved");
        assert_eq!(restored.dims(), 2, "dims preserved");
        assert_eq!(restored.metric(), HnswMetric::L2, "metric preserved");
        assert_eq!(restored.probes(), 1, "probes preserved");
        assert_eq!(
            restored.snapshot_lsn(),
            expected_lsn,
            "snapshot lsn preserved for kind {kind:?}"
        );
        assert_eq!(
            restored.tombstone_count(),
            expected_tombstones,
            "tombstones survive for kind {kind:?}"
        );

        let restored_hits = restored.search(&query, 3).expect("restored search");
        let restored_tids: Vec<TupleId> = restored_hits.iter().map(|hit| hit.tid).collect();
        assert_eq!(
            restored_tids, expected_tids,
            "top-k tids preserved for kind {kind:?}"
        );
    }
}

#[test]
fn ivfflat_snapshot_rejects_corruption() {
    let index_rel = RelationId::new(9_923);
    let source = build_ivfflat_snapshot_index(index_rel, AnnPayloadKind::Int8);
    let bytes = source.encode_snapshot();

    PageBackedIvfFlatIndex::from_snapshot_bytes(index_rel, &bytes)
        .expect("pristine snapshot decodes");

    let mut flipped = bytes.clone();
    let mid = flipped.len() / 2;
    flipped[mid] ^= 0xFF;
    assert!(
        PageBackedIvfFlatIndex::from_snapshot_bytes(index_rel, &flipped).is_err(),
        "flipped byte must be rejected"
    );

    let truncated = &bytes[..bytes.len() - 5];
    assert!(
        PageBackedIvfFlatIndex::from_snapshot_bytes(index_rel, truncated).is_err(),
        "truncated buffer must be rejected"
    );

    let mut bad_magic = bytes.clone();
    bad_magic[0] ^= 0xFF;
    assert!(
        PageBackedIvfFlatIndex::from_snapshot_bytes(index_rel, &bad_magic).is_err(),
        "corrupt magic must be rejected"
    );

    assert!(
        PageBackedIvfFlatIndex::from_snapshot_bytes(RelationId::new(1), &bytes).is_err(),
        "relation mismatch must be rejected"
    );
}

#[test]
fn ivfflat_snapshot_replay_skips_covered_records_and_applies_newer() {
    let index_rel = RelationId::new(9_924);
    let source =
        PageBackedIvfFlatIndex::new(index_rel, 2, HnswMetric::L2, 2, 1).expect("ivfflat config");
    let sink = InMemoryWalSink::new();
    source
        .bulk_load_logged(
            vec![
                (vec![0.0, 0.0], tid(1, 0)),
                (vec![1.0, 0.0], tid(1, 1)),
                (vec![9.0, 0.0], tid(2, 0)),
                (vec![10.0, 0.0], tid(2, 1)),
            ],
            Xid::new(50),
            Some(&sink),
        )
        .expect("bulk load logged");
    // Delete + compact so the snapshot is taken POST-compaction — the case
    // where an ungated replay of the pre-compaction insert would resurrect
    // the removed tuple.
    source
        .mark_deleted_logged(tid(1, 0), Xid::new(51), Some(&sink))
        .expect("logged delete");
    source
        .compact_deleted_logged(Xid::new(52), Some(&sink))
        .expect("logged compact");

    let bytes = source.encode_snapshot();
    let snapshot_lsn = source.snapshot_lsn();
    let restored =
        PageBackedIvfFlatIndex::from_snapshot_bytes(index_rel, &bytes).expect("snapshot decodes");
    let live_before = restored.live_len();

    // Every emitted record is at or below the snapshot lsn, so the redo gate
    // must skip them all — state unchanged, and the compacted tuple stays gone.
    let records = sink.records();
    for (lsn, record) in &records {
        assert!(lsn.raw() <= snapshot_lsn.raw());
        restored
            .apply_wal_record_at(*lsn, record)
            .expect("gated replay of covered record");
    }
    assert_eq!(
        restored.live_len(),
        live_before,
        "covered records must be skipped, not re-applied"
    );
    assert!(
        restored
            .search(&[0.0, 0.0], 5)
            .expect("search")
            .iter()
            .all(|hit| hit.tid != tid(1, 0)),
        "the compacted-away tuple must not be resurrected by replay"
    );

    // A record ABOVE the snapshot lsn is genuinely applied.
    source
        .insert_vector_logged(&[5.0, 0.0], tid(3, 0), Xid::new(53), Some(&sink))
        .expect("post-snapshot insert");
    let all = sink.records();
    let (new_lsn, new_record) = all.last().expect("a new record exists");
    assert!(
        new_lsn.raw() > snapshot_lsn.raw(),
        "the new record is above the snapshot lsn"
    );
    restored
        .apply_wal_record_at(*new_lsn, new_record)
        .expect("apply post-snapshot record");
    assert_eq!(
        restored.live_len(),
        live_before + 1,
        "a record above the snapshot lsn must apply"
    );
    assert!(
        restored
            .search(&[5.0, 0.0], 5)
            .expect("search")
            .iter()
            .any(|hit| hit.tid == tid(3, 0)),
        "the post-snapshot insert is searchable"
    );
}

#[test]
fn nearest_vectors_skips_empty_centroid_slots_without_panicking() {
    // An empty interior centroid slot carries no vector. It must be skipped,
    // never fed to a distance kernel whose length-equality assert would panic.
    let centroids = vec![vec![1.0, 0.0], Vec::new(), vec![9.0, 0.0]];
    let got = nearest_vectors(&centroids, &[8.0, 0.0], HnswMetric::L2, 3);
    // The empty slot (index 1) is excluded; the populated slots rank by
    // distance to [8,0]: slot 2 ([9,0]) nearest, then slot 0 ([1,0]).
    assert_eq!(got, vec![2, 0]);
    assert_eq!(
        nearest_vector(&centroids, &[8.0, 0.0], HnswMetric::L2),
        Some(2)
    );
    // All-empty centroids yield nothing to probe — and still no panic.
    assert!(nearest_vectors(&[Vec::new(), Vec::new()], &[8.0, 0.0], HnswMetric::L2, 2).is_empty());
}

#[test]
fn ivfflat_snapshot_with_empty_centroid_slot_decodes_and_search_is_safe() {
    // Adversarial: a CRC-valid snapshot can carry an empty interior centroid
    // slot (unreachable via the public API, reachable via corruption). Decoding
    // it must never yield an index whose first search panics — the decode
    // contract forbids a corrupt-but-decodable buffer from crashing a query.
    let index_rel = RelationId::new(9_925);
    let mut body = Vec::new();
    body.extend_from_slice(b"USQLIFF1"); // magic
    body.extend_from_slice(&1u32.to_le_bytes()); // version
    body.extend_from_slice(&index_rel.oid().raw().to_le_bytes());
    body.extend_from_slice(&2u32.to_le_bytes()); // dims
    body.push(0); // metric = L2
    body.extend_from_slice(&2u32.to_le_bytes()); // lists
    body.extend_from_slice(&1u32.to_le_bytes()); // probes
    body.push(0); // payload_kind = F32
    body.extend_from_slice(&100u64.to_le_bytes()); // snapshot_lsn
    // Two centroid slots: slot 0 EMPTY (len 0), slot 1 = [10, 0].
    body.extend_from_slice(&2u32.to_le_bytes());
    body.extend_from_slice(&0u32.to_le_bytes());
    body.extend_from_slice(&2u32.to_le_bytes());
    body.extend_from_slice(&10.0f32.to_le_bytes());
    body.extend_from_slice(&0.0f32.to_le_bytes());
    // One entry [9, 0] assigned to the populated list 1.
    body.extend_from_slice(&1u32.to_le_bytes()); // entry_count
    body.extend_from_slice(&2u32.to_le_bytes()); // vector len
    body.extend_from_slice(&9.0f32.to_le_bytes());
    body.extend_from_slice(&0.0f32.to_le_bytes());
    body.extend_from_slice(&7u32.to_le_bytes()); // tid relation oid
    body.extend_from_slice(&1u32.to_le_bytes()); // tid block
    body.extend_from_slice(&0u16.to_le_bytes()); // tid slot
    body.extend_from_slice(&1u32.to_le_bytes()); // list_id = 1
    body.push(0); // not deleted
    let crc = crc32c::crc32c(&body);
    body.extend_from_slice(&crc.to_le_bytes());

    let restored = PageBackedIvfFlatIndex::from_snapshot_bytes(index_rel, &body)
        .expect("snapshot with an empty interior centroid slot must decode");
    // The empty slot must not crash the first search; the one entry is found.
    let hits = restored
        .search(&[9.4, 0.0], 1)
        .expect("search must not panic");
    assert_eq!(hits.len(), 1);
}
