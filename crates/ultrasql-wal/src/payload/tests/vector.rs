//! Vector-index (HNSW, IVFFlat) operation payload tests.

use super::*;

    // ── HnswOpPayload ────────────────────────────────────────────────────

    #[test]
    fn hnsw_op_insert_round_trip() {
        let p = HnswOpPayload {
            op: HnswOpKind::Insert,
            index_rel: RelationId::new(77),
            tid: tid(77, 7, 3),
            vector: vec![1.0, 2.0, 3.0],
        };
        assert_eq!(HnswOpPayload::decode(&p.encode().unwrap()).unwrap(), p);
    }

    #[test]
    fn hnsw_op_unknown_kind_rejected() {
        let p = HnswOpPayload {
            op: HnswOpKind::Delete,
            index_rel: RelationId::new(1),
            tid: tid(1, 1, 0),
            vector: Vec::new(),
        };
        let mut raw = p.encode().unwrap();
        raw[0] = 99;
        let err = HnswOpPayload::decode(&raw).unwrap_err();
        assert!(
            matches!(err, PayloadError::Malformed(_)),
            "expected Malformed for unknown HnswOpKind, got {err:?}"
        );
    }

    #[test]
    fn hnsw_op_truncated_rejected() {
        let p = HnswOpPayload {
            op: HnswOpKind::Insert,
            index_rel: RelationId::new(1),
            tid: tid(1, 1, 0),
            vector: vec![0.0, 1.0],
        };
        let mut raw = p.encode().unwrap();
        raw.truncate(raw.len() - 1);
        let err = HnswOpPayload::decode(&raw).unwrap_err();
        assert!(matches!(err, PayloadError::Truncated { .. }), "got {err:?}");
    }

    proptest! {
        #[test]
        fn proptest_hnsw_op_round_trip(
            op_raw in prop_oneof![Just(1_u8), Just(2_u8), Just(3_u8)],
            rel in 0_u32..u32::MAX,
            block in 0_u32..u32::MAX,
            slot in any::<u16>(),
            vector in finite_f32_vec(64),
        ) {
            let op = HnswOpKind::from_u8(op_raw).unwrap();
            let p = HnswOpPayload {
                op,
                index_rel: RelationId::new(rel),
                tid: tid(rel, block, slot),
                vector,
            };
            prop_assert_eq!(HnswOpPayload::decode(&p.encode().unwrap()).unwrap(), p);
        }

        #[test]
        fn proptest_hnsw_op_decode_random_bytes_never_panics(
            raw in proptest::collection::vec(any::<u8>(), 0..160_usize),
        ) {
            let _ = HnswOpPayload::decode(&raw);
        }
    }

    // ── IvfFlatOpPayload ─────────────────────────────────────────────────

    #[test]
    fn ivfflat_op_insert_round_trip() {
        let p = IvfFlatOpPayload {
            op: IvfFlatOpKind::Insert,
            index_rel: RelationId::new(77),
            tid: tid(77, 7, 3),
            list_id: 4,
            vector: vec![1.0, 2.0, 3.0],
        };
        assert_eq!(IvfFlatOpPayload::decode(&p.encode().unwrap()).unwrap(), p);
    }

    #[test]
    fn ivfflat_op_unknown_kind_rejected() {
        let p = IvfFlatOpPayload {
            op: IvfFlatOpKind::Delete,
            index_rel: RelationId::new(1),
            tid: tid(1, 1, 0),
            list_id: 0,
            vector: Vec::new(),
        };
        let mut raw = p.encode().unwrap();
        raw[0] = 99;
        let err = IvfFlatOpPayload::decode(&raw).unwrap_err();
        assert!(
            matches!(err, PayloadError::Malformed(_)),
            "expected Malformed for unknown IvfFlatOpKind, got {err:?}"
        );
    }

    #[test]
    fn ivfflat_op_truncated_rejected() {
        let p = IvfFlatOpPayload {
            op: IvfFlatOpKind::Centroid,
            index_rel: RelationId::new(1),
            tid: tid(1, 1, 0),
            list_id: 0,
            vector: vec![0.0, 1.0],
        };
        let mut raw = p.encode().unwrap();
        raw.truncate(raw.len() - 1);
        let err = IvfFlatOpPayload::decode(&raw).unwrap_err();
        assert!(matches!(err, PayloadError::Truncated { .. }), "got {err:?}");
    }

    proptest! {
        #[test]
        fn proptest_ivfflat_op_round_trip(
            op_raw in prop_oneof![Just(1_u8), Just(2_u8), Just(3_u8), Just(4_u8)],
            rel in 0_u32..u32::MAX,
            block in 0_u32..u32::MAX,
            slot in any::<u16>(),
            list_id in any::<u32>(),
            vector in finite_f32_vec(64),
        ) {
            let op = IvfFlatOpKind::from_u8(op_raw).unwrap();
            let p = IvfFlatOpPayload {
                op,
                index_rel: RelationId::new(rel),
                tid: tid(rel, block, slot),
                list_id,
                vector,
            };
            prop_assert_eq!(IvfFlatOpPayload::decode(&p.encode().unwrap()).unwrap(), p);
        }

        #[test]
        fn proptest_ivfflat_op_decode_random_bytes_never_panics(
            raw in proptest::collection::vec(any::<u8>(), 0..160_usize),
        ) {
            let _ = IvfFlatOpPayload::decode(&raw);
        }
    }
