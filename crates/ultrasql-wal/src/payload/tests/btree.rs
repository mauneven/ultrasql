//! B-tree operation payload tests.

use super::*;

    // ── BTreeOpPayload ────────────────────────────────────────────────────

    #[test]
    fn btree_op_insert_round_trip() {
        let p = BTreeOpPayload {
            op: BTreeOpKind::Insert,
            index_rel: RelationId::new(42),
            page: page_id(42, 7),
            key_bytes: vec![0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08],
            child_or_value: b"tuple-id-12b".to_vec(),
        };
        assert_eq!(BTreeOpPayload::decode(&p.encode().unwrap()).unwrap(), p);
    }

    #[test]
    fn btree_op_split_round_trip() {
        let p = BTreeOpPayload {
            op: BTreeOpKind::Split,
            index_rel: RelationId::new(1),
            page: page_id(1, 0),
            key_bytes: 42_i64.to_le_bytes().to_vec(),
            child_or_value: 99_u32.to_le_bytes().to_vec(),
        };
        assert_eq!(BTreeOpPayload::decode(&p.encode().unwrap()).unwrap(), p);
    }

    #[test]
    fn btree_op_delete_round_trip() {
        let p = BTreeOpPayload {
            op: BTreeOpKind::Delete,
            index_rel: RelationId::new(5),
            page: page_id(5, 3),
            key_bytes: vec![0xFF; 8],
            child_or_value: vec![],
        };
        assert_eq!(BTreeOpPayload::decode(&p.encode().unwrap()).unwrap(), p);
    }

    #[test]
    fn btree_op_empty_key_and_value_round_trip() {
        let p = BTreeOpPayload {
            op: BTreeOpKind::Insert,
            index_rel: RelationId::new(0),
            page: page_id(0, 0),
            key_bytes: vec![],
            child_or_value: vec![],
        };
        assert_eq!(BTreeOpPayload::decode(&p.encode().unwrap()).unwrap(), p);
    }

    #[test]
    fn btree_op_unknown_kind_rejected() {
        // Build a valid Insert payload, then corrupt the op byte to 99.
        let p = BTreeOpPayload {
            op: BTreeOpKind::Insert,
            index_rel: RelationId::new(1),
            page: page_id(1, 0),
            key_bytes: vec![1, 2, 3, 4, 5, 6, 7, 8],
            child_or_value: vec![],
        };
        let mut raw = p.encode().unwrap();
        raw[0] = 99; // unknown kind
        let err = BTreeOpPayload::decode(&raw).unwrap_err();
        assert!(
            matches!(err, PayloadError::Malformed(_)),
            "expected Malformed for unknown BTreeOpKind, got {err:?}"
        );
    }

    #[test]
    fn btree_op_truncated_rejected() {
        let p = BTreeOpPayload {
            op: BTreeOpKind::Insert,
            index_rel: RelationId::new(1),
            page: page_id(1, 0),
            key_bytes: vec![0; 8],
            child_or_value: vec![1, 2, 3],
        };
        let mut raw = p.encode().unwrap();
        raw.truncate(raw.len() - 1);
        let err = BTreeOpPayload::decode(&raw).unwrap_err();
        assert!(matches!(err, PayloadError::Truncated { .. }), "got {err:?}");
    }

    proptest! {
        #[test]
        fn proptest_btree_op_round_trip(
            op_raw in prop_oneof![Just(1_u8), Just(2_u8), Just(3_u8)],
            rel in 0_u32..u32::MAX,
            block in 0_u32..u32::MAX,
            key_bytes in proptest::collection::vec(any::<u8>(), 0..256_usize),
            cv_bytes in proptest::collection::vec(any::<u8>(), 0..256_usize),
        ) {
            let op = BTreeOpKind::from_u8(op_raw).unwrap();
            let p = BTreeOpPayload {
                op,
                index_rel: RelationId::new(rel),
                page: page_id(rel, block),
                key_bytes,
                child_or_value: cv_bytes,
            };
            prop_assert_eq!(BTreeOpPayload::decode(&p.encode().unwrap()).unwrap(), p);
        }
    }
