//! Tests for the streaming [`SeqScan`](super::super::SeqScan) heap-walk
//! path: MVCC visibility, batch chunking, TID prefixing, error
//! propagation, and null-bitmap routing.

use std::sync::Arc;

use ultrasql_core::{CommandId, DataType, Field, Schema, Value, Xid};
use ultrasql_mvcc::Snapshot;
use ultrasql_mvcc::status::test_support::MapOracle;
use ultrasql_vec::column::Column;

use super::{drain_rows, insert_opts, make_heap, rel, schema_i32_only, schema_i32_text, snap_for};
use crate::row_codec::RowCodec;
use crate::seq_scan::SeqScan;
use crate::{ExecError, Operator};

#[test]
fn scan_returns_inserted_rows_in_insert_order() {
    let heap = make_heap();
    let codec = RowCodec::new(schema_i32_text());
    let xid: u64 = 10;
    let oracle = Arc::new(MapOracle::new());
    oracle.set_committed(Xid::new(xid));

    let expected: Vec<(i32, String)> = (0_i32..10).map(|i| (i, format!("row_{i}"))).collect();
    for (id, name) in &expected {
        let row = vec![Value::Int32(*id), Value::Text(name.clone())];
        let payload = codec.encode(&row).expect("encode");
        heap.insert(rel(), &payload, insert_opts(xid))
            .expect("insert");
    }

    let block_count = heap.block_count(rel());
    let snapshot = snap_for(xid);
    let mut scan = SeqScan::new(
        Arc::clone(&heap),
        rel(),
        block_count,
        snapshot,
        Arc::clone(&oracle),
        codec,
    );

    let rows = drain_rows(&mut scan);
    assert_eq!(rows, expected, "scan returned rows in wrong order");
}

#[test]
fn scan_filters_invisible_rows() {
    let heap = make_heap();
    let codec = RowCodec::new(schema_i32_text());
    let xid_committed: u64 = 20;
    let xid_aborted: u64 = 21;
    let oracle = Arc::new(MapOracle::new());
    oracle.set_committed(Xid::new(xid_committed));
    oracle.set_aborted(Xid::new(xid_aborted));

    let committed_rows: Vec<(i32, String)> =
        (0_i32..5).map(|i| (i, format!("committed_{i}"))).collect();
    let aborted_rows: Vec<(i32, String)> = (100_i32..105)
        .map(|i| (i, format!("aborted_{i}")))
        .collect();

    for (id, name) in &committed_rows {
        let row = vec![Value::Int32(*id), Value::Text(name.clone())];
        let payload = codec.encode(&row).expect("encode");
        heap.insert(rel(), &payload, insert_opts(xid_committed))
            .expect("insert");
    }
    for (id, name) in &aborted_rows {
        let row = vec![Value::Int32(*id), Value::Text(name.clone())];
        let payload = codec.encode(&row).expect("encode");
        heap.insert(rel(), &payload, insert_opts(xid_aborted))
            .expect("insert");
    }

    let snapshot = Snapshot::new(
        Xid::new(xid_aborted + 1),
        Xid::new(xid_aborted + 2),
        Xid::new(xid_aborted + 1),
        CommandId::FIRST,
        [],
    );
    let block_count = heap.block_count(rel());
    let mut scan = SeqScan::new(
        Arc::clone(&heap),
        rel(),
        block_count,
        snapshot,
        Arc::clone(&oracle),
        codec,
    );

    let rows = drain_rows(&mut scan);
    assert_eq!(
        rows, committed_rows,
        "scan should only return committed rows"
    );
}

#[test]
fn scan_chunks_into_4096_row_batches() {
    let heap = make_heap();
    let codec = RowCodec::new(schema_i32_text());
    let xid: u64 = 30;
    let oracle = Arc::new(MapOracle::new());
    oracle.set_committed(Xid::new(xid));

    let total = 4100_usize;
    for i in 0_i32..i32::try_from(total).expect("fits i32") {
        let row = vec![Value::Int32(i), Value::Text(format!("r{i}"))];
        let payload = codec.encode(&row).expect("encode");
        heap.insert(rel(), &payload, insert_opts(xid))
            .expect("insert");
    }

    let block_count = heap.block_count(rel());
    let snapshot = snap_for(xid);
    let mut scan = SeqScan::new(
        Arc::clone(&heap),
        rel(),
        block_count,
        snapshot,
        Arc::clone(&oracle),
        codec,
    );

    let mut batch_sizes: Vec<usize> = Vec::new();
    while let Some(batch) = scan.next_batch().expect("operator must not error") {
        batch_sizes.push(batch.rows());
    }

    let total_scanned: usize = batch_sizes.iter().sum();
    assert_eq!(total_scanned, total, "total rows mismatch");
    assert!(
        batch_sizes.contains(&4096),
        "expected at least one full 4096-row batch, got {batch_sizes:?}"
    );
    assert_eq!(
        *batch_sizes.last().expect("at least one batch"),
        total % 4096,
        "remainder batch size mismatch"
    );
}

#[test]
fn scan_empty_relation_returns_none() {
    let heap = make_heap();
    let codec = RowCodec::new(schema_i32_text());
    let oracle = Arc::new(MapOracle::new());
    let snapshot = snap_for(1);
    let mut scan = SeqScan::new(
        Arc::clone(&heap),
        rel(),
        0,
        snapshot,
        Arc::clone(&oracle),
        codec,
    );

    let result = scan.next_batch().expect("operator must not error");
    assert!(
        result.is_none(),
        "empty relation must return None immediately"
    );
}

#[test]
fn tid_scan_prepends_block_and_slot_columns() {
    let heap = make_heap();
    let codec = RowCodec::new(schema_i32_text());
    let xid: u64 = 50;
    let oracle = Arc::new(MapOracle::new());
    oracle.set_committed(Xid::new(xid));

    let inputs: Vec<(i32, String)> = (0_i32..3).map(|i| (i, format!("row_{i}"))).collect();
    for (id, name) in &inputs {
        let row = vec![Value::Int32(*id), Value::Text(name.clone())];
        let payload = codec.encode(&row).expect("encode");
        heap.insert(rel(), &payload, insert_opts(xid))
            .expect("insert");
    }

    let block_count = heap.block_count(rel());
    let snapshot = snap_for(xid);
    let mut scan = SeqScan::new_with_tids(
        Arc::clone(&heap),
        rel(),
        block_count,
        snapshot,
        Arc::clone(&oracle),
        codec,
    );

    let schema = scan.schema().clone();
    assert_eq!(schema.len(), 4, "TID schema must have 4 columns");
    assert_eq!(schema.field_at(0).name, "tid_block");
    assert_eq!(schema.field_at(0).data_type, DataType::Int32);
    assert_eq!(schema.field_at(1).name, "tid_slot");
    assert_eq!(schema.field_at(1).data_type, DataType::Int32);

    let batch = scan
        .next_batch()
        .expect("must not error")
        .expect("first batch");
    assert_eq!(batch.rows(), 3);
    assert_eq!(batch.width(), 4);
    let block_col = match &batch.columns()[0] {
        Column::Int32(c) => c.data().to_vec(),
        other => panic!("expected Int32 for tid_block, got {other:?}"),
    };
    assert_eq!(block_col, vec![0_i32, 0, 0]);
    let slot_col = match &batch.columns()[1] {
        Column::Int32(c) => c.data().to_vec(),
        other => panic!("expected Int32 for tid_slot, got {other:?}"),
    };
    assert_eq!(slot_col, vec![0_i32, 1, 2]);
    let id_col = match &batch.columns()[2] {
        Column::Int32(c) => c.data().to_vec(),
        other => panic!("expected Int32 for id, got {other:?}"),
    };
    assert_eq!(id_col, vec![0_i32, 1, 2]);
}

#[test]
fn scan_propagates_codec_errors_as_type_mismatch() {
    let heap = make_heap();
    let codec = RowCodec::new(schema_i32_text());
    let xid: u64 = 40;
    let oracle = Arc::new(MapOracle::new());
    oracle.set_committed(Xid::new(xid));

    let corrupt_payload = vec![0xDE, 0xAD];
    heap.insert(rel(), &corrupt_payload, insert_opts(xid))
        .expect("insert corrupt payload");

    let block_count = heap.block_count(rel());
    let snapshot = snap_for(xid);
    let mut scan = SeqScan::new(
        Arc::clone(&heap),
        rel(),
        block_count,
        snapshot,
        Arc::clone(&oracle),
        codec,
    );

    let err = scan.next_batch().expect_err("corrupt payload must error");
    assert!(
        matches!(err, ExecError::TypeMismatch(_)),
        "expected TypeMismatch, got {err:?}"
    );
}

// -----------------------------------------------------------------------
// New streaming tests
// -----------------------------------------------------------------------

/// Verify that an 8200-row heap streams out as batches of 4096,
/// 4096 and 8 — confirming the operator no longer pre-materialises
/// every row before yielding the first batch.
#[test]
fn streaming_seq_scan_emits_4096_chunks() {
    let heap = make_heap();
    let codec = RowCodec::new(schema_i32_only());
    let xid: u64 = 60;
    let oracle = Arc::new(MapOracle::new());
    oracle.set_committed(Xid::new(xid));

    let total = 8200_usize;
    for i in 0_i32..i32::try_from(total).expect("fits i32") {
        let row = vec![Value::Int32(i)];
        let payload = codec.encode(&row).expect("encode");
        heap.insert(rel(), &payload, insert_opts(xid))
            .expect("insert");
    }

    let block_count = heap.block_count(rel());
    let snapshot = snap_for(xid);
    let mut scan = SeqScan::new(
        Arc::clone(&heap),
        rel(),
        block_count,
        snapshot,
        Arc::clone(&oracle),
        codec,
    );

    let mut sizes: Vec<usize> = Vec::new();
    while let Some(batch) = scan.next_batch().expect("operator must not error") {
        sizes.push(batch.rows());
    }
    assert_eq!(
        sizes,
        vec![4096, 4096, 8],
        "streaming scan must emit 4096 + 4096 + 8, got {sizes:?}"
    );
}

/// Verify content equality with the legacy output: streamed rows
/// preserve insertion order over a 10k-row heap.
#[test]
fn streaming_seq_scan_matches_old_output() {
    let heap = make_heap();
    let codec = RowCodec::new(schema_i32_only());
    let xid: u64 = 70;
    let oracle = Arc::new(MapOracle::new());
    oracle.set_committed(Xid::new(xid));

    let total = 10_000_usize;
    let total_i32 = i32::try_from(total).expect("total fits i32");
    for i in 0..total_i32 {
        let row = vec![Value::Int32(i)];
        let payload = codec.encode(&row).expect("encode");
        heap.insert(rel(), &payload, insert_opts(xid))
            .expect("insert");
    }

    let block_count = heap.block_count(rel());
    let snapshot = snap_for(xid);
    let mut scan = SeqScan::new(
        Arc::clone(&heap),
        rel(),
        block_count,
        snapshot,
        Arc::clone(&oracle),
        codec,
    );

    let mut streamed: Vec<i32> = Vec::with_capacity(total);
    while let Some(batch) = scan.next_batch().expect("operator must not error") {
        match &batch.columns()[0] {
            Column::Int32(c) => streamed.extend_from_slice(c.data()),
            other => panic!("expected Int32 column, got {other:?}"),
        }
    }

    let expected: Vec<i32> = (0..total_i32).collect();
    assert_eq!(
        streamed, expected,
        "streaming output diverges from insertion order"
    );
}

/// Smoke test the null-bitmap routing: alternate rows have NULL
/// in column 1 and the resulting column's bitmap matches.
#[test]
fn streaming_seq_scan_routes_nulls_into_bitmap() {
    let heap = make_heap();
    let schema = Schema::new([
        Field::required("id", DataType::Int32),
        Field::nullable("score", DataType::Int64),
    ])
    .expect("schema ok");
    let codec = RowCodec::new(schema);
    let xid: u64 = 80;
    let oracle = Arc::new(MapOracle::new());
    oracle.set_committed(Xid::new(xid));

    let total = 32_usize;
    let total_i32 = i32::try_from(total).expect("total fits i32");
    for i in 0..total_i32 {
        let row = if i % 2 == 0 {
            vec![Value::Int32(i), Value::Null]
        } else {
            vec![Value::Int32(i), Value::Int64(i64::from(i) * 10)]
        };
        let payload = codec.encode(&row).expect("encode");
        heap.insert(rel(), &payload, insert_opts(xid))
            .expect("insert");
    }

    let block_count = heap.block_count(rel());
    let snapshot = snap_for(xid);
    let mut scan = SeqScan::new(
        Arc::clone(&heap),
        rel(),
        block_count,
        snapshot,
        Arc::clone(&oracle),
        codec,
    );

    let batch = scan
        .next_batch()
        .expect("operator must not error")
        .expect("first batch");
    let score_col = match &batch.columns()[1] {
        Column::Int64(c) => c,
        other => panic!("expected Int64 score, got {other:?}"),
    };
    let nulls = score_col
        .nulls()
        .expect("null bitmap must be present after observing nulls");
    for i in 0..total {
        let is_valid_expected = i % 2 == 1;
        assert_eq!(
            nulls.get(i),
            is_valid_expected,
            "row {i}: expected valid={is_valid_expected}, got bit={}",
            nulls.get(i)
        );
    }
    for (i, &v) in score_col.data().iter().enumerate() {
        if i % 2 == 0 {
            assert_eq!(v, 0, "row {i}: null placeholder must be 0");
        } else {
            assert_eq!(
                v,
                i64::from(i32::try_from(i).expect("fits i32")) * 10,
                "row {i}: non-null value must round-trip"
            );
        }
    }
}
