//! Generic decoder rejection tests (trailing / reserved bytes).

use super::*;

#[test]
fn payload_decoders_reject_trailing_bytes() {
    assert_trailing_rejected(
        HeapInsertPayload {
            tid: tid(1, 2, 3),
            tuple_bytes: vec![1, 2, 3],
        }
        .encode()
        .expect("encode"),
        HeapInsertPayload::decode,
    );

    assert_trailing_rejected(
        HeapInsertBatchPayload {
            page: page_id(1, 2),
            entries: vec![HeapInsertBatchEntry {
                slot: 3,
                tuple_bytes: vec![1, 2, 3],
            }],
        }
        .encode()
        .expect("encode"),
        HeapInsertBatchPayload::decode,
    );

    assert_trailing_rejected(
        HeapUpdatePayload {
            old_tid: tid(1, 2, 3),
            new_tid: tid(1, 2, 4),
            flags: HEAP_UPDATE_HOT,
            new_tuple_bytes: vec![4, 5, 6],
        }
        .encode()
        .expect("encode"),
        HeapUpdatePayload::decode,
    );

    assert_trailing_rejected(
        HeapUpdateInPlacePayload {
            tid: tid(1, 2, 3),
            writer_xid: Xid::new(11),
            command_id: CommandId::new(2),
            pre_image_bytes: vec![1, 2],
            post_image_bytes: vec![3, 4],
        }
        .encode()
        .expect("encode"),
        HeapUpdateInPlacePayload::decode,
    );

    assert_trailing_rejected(
        HeapUpdateInPlaceBatchPayload {
            page: page_id(1, 2),
            writer_xid: Xid::new(11),
            command_id: CommandId::new(2),
            entries: vec![HeapUpdateInPlaceBatchEntry {
                slot: 3,
                pre_image: [1; HeapUpdateInPlaceBatchPayload::IMAGE_LEN],
                post_image: [2; HeapUpdateInPlaceBatchPayload::IMAGE_LEN],
            }],
        }
        .encode()
        .expect("encode"),
        HeapUpdateInPlaceBatchPayload::decode,
    );

    assert_trailing_rejected(
        HeapDeletePayload {
            tid: tid(1, 2, 3),
            xmax: Xid::new(11),
            cmax: CommandId::new(2),
        }
        .encode()
        .expect("encode"),
        HeapDeletePayload::decode,
    );

    assert_trailing_rejected(
        HeapDeleteInPlacePayload {
            tid: tid(1, 2, 3),
            xmax: Xid::new(11),
            cmax: CommandId::new(2),
        }
        .encode()
        .expect("encode"),
        HeapDeleteInPlacePayload::decode,
    );

    assert_trailing_rejected(
        HeapDeleteInPlaceBatchPayload {
            page: page_id(1, 2),
            xmax: Xid::new(11),
            cmax: CommandId::new(2),
            entries: vec![HeapDeleteInPlaceBatchEntry { slot: 3 }],
        }
        .encode()
        .expect("encode"),
        HeapDeleteInPlaceBatchPayload::decode,
    );

    assert_trailing_rejected(
        HeapDeleteInPlaceRangeBatchPayload {
            page: page_id(1, 2),
            xmax: Xid::new(11),
            cmax: CommandId::new(2),
            first_slot: 3,
            slot_count: 2,
        }
        .encode()
        .expect("encode"),
        HeapDeleteInPlaceRangeBatchPayload::decode,
    );

    assert_trailing_rejected(
        CommitPayload {
            commit_lsn: Lsn::new(123),
            commit_timestamp_micros: 456,
            committed_subxids: Vec::new(),
        }
        .encode()
        .expect("encode commit payload"),
        CommitPayload::decode,
    );

    assert_trailing_rejected(
        AbortPayload {
            abort_lsn: Lsn::new(123),
        }
        .encode(),
        AbortPayload::decode,
    );

    assert_trailing_rejected(
        CheckpointPayload {
            redo_from: Lsn::new(100),
            oldest_in_progress: Xid::new(10),
            next_xid: Xid::new(20),
        }
        .encode(),
        CheckpointPayload::decode,
    );

    assert_trailing_rejected(
        FullPageWritePayload {
            page: page_id(1, 2),
            page_bytes: full_page(),
        }
        .encode()
        .expect("encode"),
        FullPageWritePayload::decode,
    );

    assert_trailing_rejected(
        BTreeOpPayload {
            op: BTreeOpKind::Insert,
            index_rel: RelationId::new(7),
            page: page_id(7, 2),
            key_bytes: vec![1, 2],
            child_or_value: vec![3, 4],
        }
        .encode()
        .expect("encode"),
        BTreeOpPayload::decode,
    );

    assert_trailing_rejected(
        HashOpPayload {
            op: HashOpKind::Insert,
            index_rel: RelationId::new(7),
            bucket: 3,
            page: page_id(7, 2),
            key_hash: 99,
            key_bytes: vec![1, 2],
            value_bytes: vec![3, 4],
        }
        .encode()
        .expect("encode"),
        HashOpPayload::decode,
    );

    assert_trailing_rejected(
        HnswOpPayload {
            op: HnswOpKind::Insert,
            index_rel: RelationId::new(7),
            tid: tid(1, 2, 3),
            vector: vec![1.0, 2.0],
        }
        .encode()
        .expect("encode"),
        HnswOpPayload::decode,
    );

    assert_trailing_rejected(
        IvfFlatOpPayload {
            op: IvfFlatOpKind::Insert,
            index_rel: RelationId::new(7),
            tid: tid(1, 2, 3),
            list_id: 4,
            vector: vec![1.0, 2.0],
        }
        .encode()
        .expect("encode"),
        IvfFlatOpPayload::decode,
    );

    assert_trailing_rejected(
        SequenceOpPayload {
            op: SequenceOpKind::Advance,
            seqrelid: RelationId::new(9),
            name: "seq".to_owned(),
            start_value: 1,
            last_value: 2,
            min_value: 1,
            max_value: 10,
            increment: 1,
            cache_size: 1,
            is_called: true,
            cycle: false,
        }
        .encode()
        .expect("encode"),
        SequenceOpPayload::decode,
    );
}

#[test]
fn payload_decoders_reject_reserved_bytes() {
    let heap_insert = HeapInsertPayload {
        tid: tid(1, 2, 3),
        tuple_bytes: vec![1, 2, 3],
    }
    .encode()
    .expect("encode");
    assert_reserved_rejected(heap_insert.clone(), 7, HeapInsertPayload::decode);
    assert_reserved_rejected(heap_insert, 10, HeapInsertPayload::decode);

    assert_reserved_rejected(
        HeapUpdatePayload {
            old_tid: tid(1, 2, 3),
            new_tid: tid(1, 2, 4),
            flags: HEAP_UPDATE_HOT,
            new_tuple_bytes: vec![4, 5, 6],
        }
        .encode()
        .expect("encode"),
        25,
        HeapUpdatePayload::decode,
    );

    assert_reserved_rejected(
        HeapDeletePayload {
            tid: tid(1, 2, 3),
            xmax: Xid::new(11),
            cmax: CommandId::new(2),
        }
        .encode()
        .expect("encode"),
        TID_SIZE + 12,
        HeapDeletePayload::decode,
    );

    assert_reserved_rejected(
        BTreeOpPayload {
            op: BTreeOpKind::Insert,
            index_rel: RelationId::new(7),
            page: page_id(7, 2),
            key_bytes: vec![1, 2],
            child_or_value: vec![3, 4],
        }
        .encode()
        .expect("encode"),
        1,
        BTreeOpPayload::decode,
    );

    assert_reserved_rejected(
        HashOpPayload {
            op: HashOpKind::Insert,
            index_rel: RelationId::new(7),
            bucket: 3,
            page: page_id(7, 2),
            key_hash: 99,
            key_bytes: vec![1, 2],
            value_bytes: vec![3, 4],
        }
        .encode()
        .expect("encode"),
        1,
        HashOpPayload::decode,
    );
}
