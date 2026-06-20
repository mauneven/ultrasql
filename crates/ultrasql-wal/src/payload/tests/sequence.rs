//! Sequence operation payload tests.

use super::*;

    // ── SequenceOpPayload ─────────────────────────────────────────────────

    #[test]
    fn sequence_op_advance_round_trip() {
        let p = SequenceOpPayload {
            op: SequenceOpKind::Advance,
            seqrelid: RelationId::new(42),
            name: "orders_id_seq".to_owned(),
            start_value: 1,
            last_value: 7,
            min_value: 1,
            max_value: i64::MAX,
            increment: 1,
            cache_size: 1,
            is_called: true,
            cycle: false,
        };
        assert_eq!(SequenceOpPayload::decode(&p.encode().unwrap()).unwrap(), p);
    }

    #[test]
    fn sequence_op_unknown_kind_rejected() {
        let p = SequenceOpPayload {
            op: SequenceOpKind::Set,
            seqrelid: RelationId::new(9),
            name: "s".to_owned(),
            start_value: 10,
            last_value: 10,
            min_value: 1,
            max_value: 100,
            increment: 5,
            cache_size: 32,
            is_called: false,
            cycle: true,
        };
        let mut raw = p.encode().unwrap();
        raw[0] = 99;
        let err = SequenceOpPayload::decode(&raw).unwrap_err();
        assert!(matches!(err, PayloadError::Malformed(_)), "got {err:?}");
    }

    #[test]
    fn sequence_op_truncated_rejected() {
        let p = SequenceOpPayload {
            op: SequenceOpKind::Alter,
            seqrelid: RelationId::new(9),
            name: "s".to_owned(),
            start_value: 10,
            last_value: 10,
            min_value: 1,
            max_value: 100,
            increment: 5,
            cache_size: 32,
            is_called: false,
            cycle: true,
        };
        let mut raw = p.encode().unwrap();
        raw.truncate(raw.len() - 1);
        let err = SequenceOpPayload::decode(&raw).unwrap_err();
        assert!(matches!(err, PayloadError::Truncated { .. }), "got {err:?}");
    }
