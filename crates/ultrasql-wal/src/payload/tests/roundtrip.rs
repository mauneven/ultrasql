//! Round-trip and negative tests for heap, txn, and checkpoint payloads.

use super::*;

// ── HeapInsertPayload ─────────────────────────────────────────────────

#[test]
fn heap_insert_round_trip_empty_tuple() {
    let p = HeapInsertPayload {
        tid: tid(1, 0, 0),
        tuple_bytes: vec![],
    };
    assert_eq!(HeapInsertPayload::decode(&p.encode().unwrap()).unwrap(), p);
}

#[test]
fn heap_insert_round_trip_realistic() {
    let p = HeapInsertPayload {
        tid: tid(7, 42, 13),
        tuple_bytes: (0_u8..64).collect(),
    };
    assert_eq!(HeapInsertPayload::decode(&p.encode().unwrap()).unwrap(), p);
}

#[test]
fn heap_insert_batch_round_trip_realistic() {
    let p = HeapInsertBatchPayload {
        page: page_id(7, 42),
        entries: vec![
            HeapInsertBatchEntry {
                slot: 13,
                tuple_bytes: (0_u8..64).collect(),
            },
            HeapInsertBatchEntry {
                slot: 14,
                tuple_bytes: (64_u8..96).collect(),
            },
        ],
    };
    assert_eq!(
        HeapInsertBatchPayload::decode(&p.encode().unwrap()).unwrap(),
        p
    );
}

// ── HeapUpdatePayload ─────────────────────────────────────────────────

#[test]
fn heap_update_round_trip_no_hot() {
    let p = HeapUpdatePayload {
        old_tid: tid(1, 0, 0),
        new_tid: tid(1, 0, 1),
        flags: 0,
        new_tuple_bytes: vec![],
    };
    assert_eq!(HeapUpdatePayload::decode(&p.encode().unwrap()).unwrap(), p);
}

#[test]
fn heap_update_round_trip_hot() {
    let p = HeapUpdatePayload {
        old_tid: tid(5, 100, 3),
        new_tid: tid(5, 100, 4),
        flags: HEAP_UPDATE_HOT,
        new_tuple_bytes: (0_u8..=127).collect(),
    };
    assert_eq!(HeapUpdatePayload::decode(&p.encode().unwrap()).unwrap(), p);
}

#[test]
fn heap_update_in_place_batch_round_trip_two_slots() {
    let p = HeapUpdateInPlaceBatchPayload {
        page: page_id(9, 3),
        writer_xid: Xid::new(77),
        command_id: CommandId::new(4),
        entries: vec![
            HeapUpdateInPlaceBatchEntry {
                slot: 1,
                pre_image: [0, 1, 0, 0, 0, 10, 0, 0, 0],
                post_image: [0, 1, 0, 0, 0, 11, 0, 0, 0],
            },
            HeapUpdateInPlaceBatchEntry {
                slot: 2,
                pre_image: [0, 2, 0, 0, 0, 20, 0, 0, 0],
                post_image: [0, 2, 0, 0, 0, 21, 0, 0, 0],
            },
        ],
    };
    assert_eq!(
        HeapUpdateInPlaceBatchPayload::decode(&p.encode().unwrap()).unwrap(),
        p
    );
}

#[test]
fn heap_update_in_place_batch_encode_entries_matches_typed_encoder() {
    let page = page_id(9, 3);
    let writer_xid = Xid::new(77);
    let command_id = CommandId::new(4);
    let entries = [
        (
            1,
            [0, 1, 0, 0, 0, 10, 0, 0, 0],
            [0, 1, 0, 0, 0, 11, 0, 0, 0],
        ),
        (
            2,
            [0, 2, 0, 0, 0, 20, 0, 0, 0],
            [0, 2, 0, 0, 0, 21, 0, 0, 0],
        ),
    ];
    let typed = HeapUpdateInPlaceBatchPayload {
        page,
        writer_xid,
        command_id,
        entries: entries
            .iter()
            .map(
                |(slot, pre_image, post_image)| HeapUpdateInPlaceBatchEntry {
                    slot: *slot,
                    pre_image: *pre_image,
                    post_image: *post_image,
                },
            )
            .collect(),
    };

    assert_eq!(
        HeapUpdateInPlaceBatchPayload::encode_entries(page, writer_xid, command_id, &entries,)
            .unwrap(),
        typed.encode().unwrap()
    );
}

#[test]
fn heap_update_int32_pair_delta_batch_round_trip_two_slots() {
    let p = HeapUpdateInt32PairDeltaBatchPayload {
        page: page_id(9, 3),
        writer_xid: Xid::new(77),
        command_id: CommandId::new(4),
        target_col: 1,
        delta: -3,
        slots: vec![1, 2],
    };
    assert_eq!(
        HeapUpdateInt32PairDeltaBatchPayload::decode(&p.encode().unwrap()).unwrap(),
        p
    );
}

#[test]
fn heap_update_int32_pair_delta_batch_encode_slots_into_matches_vec_encoder() {
    let mut out = Vec::with_capacity(4);
    HeapUpdateInt32PairDeltaBatchPayload::encode_slots_into(
        page_id(9, 3),
        Xid::new(77),
        CommandId::new(4),
        1,
        -3,
        &[1, 2, 3],
        &mut out,
    )
    .unwrap();

    assert_eq!(
        out,
        HeapUpdateInt32PairDeltaBatchPayload::encode_slots(
            page_id(9, 3),
            Xid::new(77),
            CommandId::new(4),
            1,
            -3,
            &[1, 2, 3],
        )
        .unwrap()
    );
}

#[test]
fn heap_update_int32_pair_delta_range_batch_round_trip() {
    let p = HeapUpdateInt32PairDeltaRangeBatchPayload {
        page: page_id(9, 3),
        writer_xid: Xid::new(77),
        command_id: CommandId::new(4),
        target_col: 1,
        delta: -3,
        first_slot: 1,
        slot_count: 3,
    };
    assert_eq!(
        HeapUpdateInt32PairDeltaRangeBatchPayload::decode(&p.encode().unwrap()).unwrap(),
        p
    );
}

#[test]
fn heap_delete_in_place_batch_round_trip_two_slots() {
    let p = HeapDeleteInPlaceBatchPayload {
        page: page_id(9, 3),
        xmax: Xid::new(77),
        cmax: CommandId::new(4),
        entries: vec![
            HeapDeleteInPlaceBatchEntry { slot: 1 },
            HeapDeleteInPlaceBatchEntry { slot: 2 },
        ],
    };
    assert_eq!(
        HeapDeleteInPlaceBatchPayload::decode(&p.encode().unwrap()).unwrap(),
        p
    );
}

#[test]
fn heap_delete_in_place_batch_encode_slots_matches_typed_encoder() {
    let page = page_id(9, 3);
    let xmax = Xid::new(77);
    let cmax = CommandId::new(4);
    let slots = [1, 2];
    let typed = HeapDeleteInPlaceBatchPayload {
        page,
        xmax,
        cmax,
        entries: slots
            .iter()
            .map(|slot| HeapDeleteInPlaceBatchEntry { slot: *slot })
            .collect(),
    };

    assert_eq!(
        HeapDeleteInPlaceBatchPayload::encode_slots(page, xmax, cmax, &slots).unwrap(),
        typed.encode().unwrap()
    );
}

#[test]
fn heap_delete_in_place_range_batch_round_trip() {
    let p = HeapDeleteInPlaceRangeBatchPayload {
        page: page_id(9, 3),
        xmax: Xid::new(77),
        cmax: CommandId::new(4),
        first_slot: 1,
        slot_count: 2,
    };
    assert_eq!(
        HeapDeleteInPlaceRangeBatchPayload::decode(&p.encode().unwrap()).unwrap(),
        p
    );
}

// ── HeapDeletePayload ─────────────────────────────────────────────────

#[test]
fn heap_delete_round_trip_minimal() {
    let p = HeapDeletePayload {
        tid: tid(1, 0, 0),
        xmax: Xid::INVALID,
        cmax: CommandId::FIRST,
    };
    assert_eq!(HeapDeletePayload::decode(&p.encode().unwrap()).unwrap(), p);
}

#[test]
fn heap_delete_round_trip_realistic() {
    let p = HeapDeletePayload {
        tid: tid(3, 99, 7),
        xmax: Xid::new(1_234_567),
        cmax: CommandId::new(2),
    };
    assert_eq!(HeapDeletePayload::decode(&p.encode().unwrap()).unwrap(), p);
}

// ── CommitPayload ─────────────────────────────────────────────────────

#[test]
fn commit_round_trip_zero() {
    let p = CommitPayload {
        commit_lsn: Lsn::ZERO,
        commit_timestamp_micros: 0,
    };
    assert_eq!(CommitPayload::decode(&p.encode()).unwrap(), p);
}

#[test]
fn commit_round_trip_realistic() {
    let p = CommitPayload {
        commit_lsn: Lsn::new(0x0000_0001_0000_2000),
        commit_timestamp_micros: 1_715_000_000_000_000,
    };
    assert_eq!(CommitPayload::decode(&p.encode()).unwrap(), p);
}

// ── AbortPayload ──────────────────────────────────────────────────────

#[test]
fn abort_round_trip_zero() {
    let p = AbortPayload {
        abort_lsn: Lsn::ZERO,
    };
    assert_eq!(AbortPayload::decode(&p.encode()).unwrap(), p);
}

#[test]
fn abort_round_trip_nonzero() {
    let p = AbortPayload {
        abort_lsn: Lsn::new(0xDEAD_BEEF_CAFE_BABE),
    };
    assert_eq!(AbortPayload::decode(&p.encode()).unwrap(), p);
}

// ── CheckpointPayload ─────────────────────────────────────────────────

#[test]
fn checkpoint_round_trip_zeros() {
    let p = CheckpointPayload {
        redo_from: Lsn::ZERO,
        oldest_in_progress: Xid::INVALID,
        next_xid: Xid::FIRST_USER,
    };
    assert_eq!(CheckpointPayload::decode(&p.encode()).unwrap(), p);
}

#[test]
fn checkpoint_round_trip_realistic() {
    let p = CheckpointPayload {
        redo_from: Lsn::new(0x0001_0000),
        oldest_in_progress: Xid::new(42),
        next_xid: Xid::new(100),
    };
    assert_eq!(CheckpointPayload::decode(&p.encode()).unwrap(), p);
}

// ── FullPageWritePayload ──────────────────────────────────────────────

#[test]
fn full_page_write_round_trip_zeroed_page() {
    let p = FullPageWritePayload {
        page: page_id(1, 0),
        page_bytes: vec![0_u8; PAGE_SIZE],
    };
    assert_eq!(
        FullPageWritePayload::decode(&p.encode().unwrap()).unwrap(),
        p
    );
}

#[test]
fn full_page_write_round_trip_realistic() {
    let p = FullPageWritePayload {
        page: page_id(7, 255),
        page_bytes: full_page(),
    };
    assert_eq!(
        FullPageWritePayload::decode(&p.encode().unwrap()).unwrap(),
        p
    );
}

#[test]
fn encode_rejects_block_above_24_bit_field() {
    let p = HeapInsertPayload {
        tid: TupleId::new(
            PageId::new(RelationId::new(1), BlockNumber::new(0x0100_0000)),
            0,
        ),
        tuple_bytes: vec![],
    };
    let err = p.encode().unwrap_err();
    assert!(
        matches!(err, PayloadError::Malformed(_)),
        "expected Malformed for block > 24-bit, got {err:?}"
    );
}

// ── Negative tests ────────────────────────────────────────────────────

#[test]
fn heap_update_reserved_flags_rejected() {
    let p = HeapUpdatePayload {
        old_tid: tid(1, 0, 0),
        new_tid: tid(1, 0, 1),
        flags: 0b1000_0000,
        new_tuple_bytes: vec![],
    };
    // Encode by hand, bypassing the encode-time reserved-flag check is
    // not performed (encode trusts the caller at construction time).
    // Use decode on a manually crafted buffer instead.
    let mut raw = p.encode().unwrap(); // encode writes flags = 0b1000_0000 verbatim
    let err = HeapUpdatePayload::decode(&raw).unwrap_err();
    assert!(
        matches!(err, PayloadError::FlagsReserved(0b1000_0000)),
        "got {err:?}"
    );

    // Also test flags = 0b0000_0010 (another reserved bit).
    raw[TID_SIZE * 2] = 0b0000_0010;
    let err2 = HeapUpdatePayload::decode(&raw).unwrap_err();
    assert!(matches!(err2, PayloadError::FlagsReserved(_)));
}

#[test]
fn heap_insert_truncated_by_one_byte_rejected() {
    let p = HeapInsertPayload {
        tid: tid(1, 0, 0),
        tuple_bytes: b"hello world".to_vec(),
    };
    let mut raw = p.encode().unwrap();
    raw.truncate(raw.len() - 1);
    let err = HeapInsertPayload::decode(&raw).unwrap_err();
    assert!(matches!(err, PayloadError::Truncated { .. }), "got {err:?}");
}

#[test]
fn heap_update_truncated_by_one_byte_rejected() {
    let p = HeapUpdatePayload {
        old_tid: tid(1, 0, 0),
        new_tid: tid(1, 0, 1),
        flags: 0,
        new_tuple_bytes: b"hello".to_vec(),
    };
    let mut raw = p.encode().unwrap();
    raw.truncate(raw.len() - 1);
    let err = HeapUpdatePayload::decode(&raw).unwrap_err();
    assert!(matches!(err, PayloadError::Truncated { .. }), "got {err:?}");
}

#[test]
fn heap_delete_truncated_by_one_byte_rejected() {
    let p = HeapDeletePayload {
        tid: tid(1, 0, 0),
        xmax: Xid::new(99),
        cmax: CommandId::new(1),
    };
    let mut raw = p.encode().unwrap();
    raw.truncate(raw.len() - 1);
    let err = HeapDeletePayload::decode(&raw).unwrap_err();
    assert!(matches!(err, PayloadError::Truncated { .. }), "got {err:?}");
}

#[test]
fn commit_truncated_by_one_byte_rejected() {
    let p = CommitPayload {
        commit_lsn: Lsn::new(1),
        commit_timestamp_micros: 2,
    };
    let mut raw = p.encode();
    raw.truncate(raw.len() - 1);
    let err = CommitPayload::decode(&raw).unwrap_err();
    assert!(matches!(err, PayloadError::Truncated { .. }), "got {err:?}");
}

#[test]
fn abort_truncated_by_one_byte_rejected() {
    let p = AbortPayload {
        abort_lsn: Lsn::new(100),
    };
    let mut raw = p.encode();
    raw.truncate(raw.len() - 1);
    let err = AbortPayload::decode(&raw).unwrap_err();
    assert!(matches!(err, PayloadError::Truncated { .. }), "got {err:?}");
}

#[test]
fn checkpoint_truncated_by_one_byte_rejected() {
    let p = CheckpointPayload {
        redo_from: Lsn::new(1),
        oldest_in_progress: Xid::new(2),
        next_xid: Xid::new(3),
    };
    let mut raw = p.encode();
    raw.truncate(raw.len() - 1);
    let err = CheckpointPayload::decode(&raw).unwrap_err();
    assert!(matches!(err, PayloadError::Truncated { .. }), "got {err:?}");
}

#[test]
fn full_page_write_truncated_by_one_byte_rejected() {
    let p = FullPageWritePayload {
        page: page_id(1, 0),
        page_bytes: full_page(),
    };
    let mut raw = p.encode().unwrap();
    raw.truncate(raw.len() - 1);
    let err = FullPageWritePayload::decode(&raw).unwrap_err();
    assert!(matches!(err, PayloadError::Truncated { .. }), "got {err:?}");
}
// NOTE: FullPageWritePayload::encode does not encode a TupleId and uses
// PAGE_ID_SIZE (u32 fields without 24-bit restriction), so no block-limit
// test is needed here.

#[test]
fn heap_insert_gigantic_tuple_len_rejected() {
    // Craft a raw buffer whose tuple_len field claims 1 GiB.
    const FIXED: usize = TID_SIZE + 4;
    let mut raw = vec![0_u8; FIXED]; // no actual tuple bytes
    let gigabyte: u32 = 1_024 * 1_024 * 1_024;
    write_u32_le(&mut raw[TID_SIZE..TID_SIZE + 4], gigabyte);
    let err = HeapInsertPayload::decode(&raw).unwrap_err();
    assert!(
        matches!(err, PayloadError::Malformed(_)),
        "expected Malformed, got {err:?}"
    );
}

#[test]
fn full_page_write_wrong_page_size_rejected() {
    // Craft a FPW whose page_bytes_len is PAGE_SIZE - 1.
    const FIXED: usize = PAGE_ID_SIZE + 4;
    let wrong_len = u32::try_from(PAGE_SIZE - 1).unwrap();
    let mut raw = vec![0_u8; FIXED + PAGE_SIZE - 1];
    write_u32_le(&mut raw[PAGE_ID_SIZE..PAGE_ID_SIZE + 4], wrong_len);
    let err = FullPageWritePayload::decode(&raw).unwrap_err();
    assert!(
        matches!(err, PayloadError::Malformed(_)),
        "expected Malformed, got {err:?}"
    );

    // Also test with a page_bytes_len that is larger than PAGE_SIZE.
    let larger = u32::try_from(PAGE_SIZE + 1).unwrap();
    let mut raw2 = vec![0_u8; FIXED + PAGE_SIZE + 1];
    write_u32_le(&mut raw2[PAGE_ID_SIZE..PAGE_ID_SIZE + 4], larger);
    let err2 = FullPageWritePayload::decode(&raw2).unwrap_err();
    assert!(
        matches!(err2, PayloadError::Malformed(_)),
        "expected Malformed, got {err2:?}"
    );
}

// ── Proptest: HeapInsertPayload round-trip ────────────────────────────

proptest! {
    #[test]
    fn proptest_heap_insert_round_trip(
        rel in 0_u32..u32::MAX,
        block in 0_u32..0x00FF_FFFFu32,
        slot in 0_u16..u16::MAX,
        tuple_bytes in proptest::collection::vec(any::<u8>(), 0..16_384),
    ) {
        let p = HeapInsertPayload {
            tid: tid(rel, block, slot),
            tuple_bytes,
        };
        prop_assert_eq!(HeapInsertPayload::decode(&p.encode().unwrap()).unwrap(), p);
    }

    #[test]
    fn proptest_heap_update_round_trip(
        old_rel in 0_u32..u32::MAX,
        old_block in 0_u32..0x00FF_FFFFu32,
        old_slot in 0_u16..u16::MAX,
        new_rel in 0_u32..u32::MAX,
        new_block in 0_u32..0x00FF_FFFFu32,
        new_slot in 0_u16..u16::MAX,
        // Only valid flags: 0 or HEAP_UPDATE_HOT (1).
        flags in prop_oneof![Just(0_u8), Just(HEAP_UPDATE_HOT)],
        new_tuple_bytes in proptest::collection::vec(any::<u8>(), 0..16_384),
    ) {
        let p = HeapUpdatePayload {
            old_tid: tid(old_rel, old_block, old_slot),
            new_tid: tid(new_rel, new_block, new_slot),
            flags,
            new_tuple_bytes,
        };
        prop_assert_eq!(HeapUpdatePayload::decode(&p.encode().unwrap()).unwrap(), p);
    }
}
