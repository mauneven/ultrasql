//! Hash-index operation payload tests.

use super::*;

// ── HashOpPayload ─────────────────────────────────────────────────────

#[test]
fn hash_op_insert_round_trip() {
    let p = HashOpPayload {
        op: HashOpKind::Insert,
        index_rel: RelationId::new(42),
        bucket: 17,
        page: page_id(42, 3),
        key_hash: 0xDEAD_BEEF,
        key_bytes: b"hash-key".to_vec(),
        value_bytes: b"tuple-id-12b".to_vec(),
    };
    assert_eq!(HashOpPayload::decode(&p.encode().unwrap()).unwrap(), p);
}

#[test]
fn hash_op_overflow_link_round_trip() {
    let p = HashOpPayload {
        op: HashOpKind::OverflowLink,
        index_rel: RelationId::new(9),
        bucket: 4,
        page: page_id(9, 12),
        key_hash: 0,
        key_bytes: vec![],
        value_bytes: 13_u32.to_le_bytes().to_vec(),
    };
    assert_eq!(HashOpPayload::decode(&p.encode().unwrap()).unwrap(), p);
}

#[test]
fn hash_op_unknown_kind_rejected() {
    let p = HashOpPayload {
        op: HashOpKind::Delete,
        index_rel: RelationId::new(1),
        bucket: 0,
        page: page_id(1, 0),
        key_hash: 1,
        key_bytes: vec![1],
        value_bytes: vec![2],
    };
    let mut raw = p.encode().unwrap();
    raw[0] = 99;
    let err = HashOpPayload::decode(&raw).unwrap_err();
    assert!(
        matches!(err, PayloadError::Malformed(_)),
        "expected Malformed for unknown HashOpKind, got {err:?}"
    );
}

#[test]
fn hash_op_truncated_rejected() {
    let p = HashOpPayload {
        op: HashOpKind::Insert,
        index_rel: RelationId::new(1),
        bucket: 0,
        page: page_id(1, 0),
        key_hash: 1,
        key_bytes: vec![0; 8],
        value_bytes: vec![1, 2, 3],
    };
    let mut raw = p.encode().unwrap();
    raw.truncate(raw.len() - 1);
    let err = HashOpPayload::decode(&raw).unwrap_err();
    assert!(matches!(err, PayloadError::Truncated { .. }), "got {err:?}");
}

proptest! {
    #[test]
    fn proptest_hash_op_round_trip(
        op_raw in prop_oneof![Just(1_u8), Just(2_u8), Just(3_u8)],
        rel in 0_u32..u32::MAX,
        bucket in 0_u32..u32::MAX,
        block in 0_u32..u32::MAX,
        key_hash in any::<u64>(),
        key_bytes in proptest::collection::vec(any::<u8>(), 0..256_usize),
        value_bytes in proptest::collection::vec(any::<u8>(), 0..256_usize),
    ) {
        let op = HashOpKind::from_u8(op_raw).unwrap();
        let p = HashOpPayload {
            op,
            index_rel: RelationId::new(rel),
            bucket,
            page: page_id(rel, block),
            key_hash,
            key_bytes,
            value_bytes,
        };
        prop_assert_eq!(HashOpPayload::decode(&p.encode().unwrap()).unwrap(), p);
    }
}
