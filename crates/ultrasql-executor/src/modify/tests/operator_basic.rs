//! INSERT/affected-rows operator behavior, output schema, builder
//! descriptor wiring, and SQL error mapping for `ModifyTable`.

use super::*;

// -----------------------------------------------------------------------
// Test 1: insert writes each input row to heap and reports count
// -----------------------------------------------------------------------

#[test]
fn insert_writes_each_input_row_to_heap_and_reports_count() {
    let heap = make_heap();
    let schema = schema_i32_text();
    let wal = Arc::new(InMemoryWalSink::new());

    // Source: 3 rows via ValuesScan.
    let rows = vec![
        vec![lit_i32(1), lit_text("alice")],
        vec![lit_i32(2), lit_text("bob")],
        vec![lit_i32(3), lit_text("carol")],
    ];
    let source = ValuesScan::new(rows, schema.clone());

    let mut op = ModifyTable::new(
        Arc::clone(&heap),
        rel(),
        schema,
        ModifyKind::Insert,
        stamps(10),
        Some(Arc::clone(&wal) as Arc<dyn ultrasql_storage::wal_sink::WalSink>),
        Box::new(source),
    );

    // Drain the operator.
    let batch = op
        .next_batch()
        .expect("must not error")
        .expect("must emit batch");
    assert_eq!(batch.rows(), 1, "expected single affected-rows batch");
    match &batch.columns()[0] {
        Column::Int64(c) => assert_eq!(c.data(), &[3_i64], "expected 3 affected rows"),
        other => panic!("unexpected column: {other:?}"),
    }
    assert!(
        op.next_batch().unwrap().is_none(),
        "must return None after emit"
    );

    // Verify 3 rows are present in the heap.
    assert_eq!(heap.block_count(rel()), 1, "one block should be allocated");
}

#[test]
fn insert_rejects_affected_row_counter_overflow() {
    let heap = make_heap();
    let schema = schema_i32_text();
    let rows = vec![vec![lit_i32(1), lit_text("alice")]];
    let source = ValuesScan::new(rows, schema.clone());

    let mut op = ModifyTable::new(
        Arc::clone(&heap),
        rel(),
        schema,
        ModifyKind::Insert,
        stamps(10),
        None,
        Box::new(source),
    );
    op.affected = i64::MAX;

    let err = op
        .next_batch()
        .expect_err("affected row count overflow must not clamp");
    assert!(matches!(err, ExecError::NumericFieldOverflow(_)));
}

// -----------------------------------------------------------------------
// Test 2: insert emits one page-batched WAL record
// -----------------------------------------------------------------------

#[test]
fn insert_emits_page_batched_wal_record() {
    let heap = make_heap();
    let schema = schema_i32_text();
    let wal = Arc::new(InMemoryWalSink::new());

    let rows = vec![
        vec![lit_i32(1), lit_text("x")],
        vec![lit_i32(2), lit_text("y")],
    ];
    let source = ValuesScan::new(rows, schema.clone());

    let mut op = ModifyTable::new(
        Arc::clone(&heap),
        rel(),
        schema,
        ModifyKind::Insert,
        stamps(20),
        Some(Arc::clone(&wal) as Arc<dyn ultrasql_storage::wal_sink::WalSink>),
        Box::new(source),
    );

    op.next_batch().unwrap();

    let records = wal.records();
    assert_eq!(records.len(), 1, "expected one page-batched WAL record");
    assert_eq!(
        format!("{:?}", records[0].1.header.record_type),
        "HeapInsertBatch"
    );
}

// -----------------------------------------------------------------------
// Test 3: empty input reports zero affected rows
// -----------------------------------------------------------------------

#[test]
fn insert_empty_input_reports_zero() {
    let heap = make_heap();
    let schema = schema_i32_text();
    let source = ValuesScan::new(vec![], schema.clone());

    let mut op = ModifyTable::new(
        Arc::clone(&heap),
        rel(),
        schema,
        ModifyKind::Insert,
        stamps(30),
        None,
        Box::new(source),
    );

    let batch = op.next_batch().unwrap().unwrap();
    match &batch.columns()[0] {
        Column::Int64(c) => assert_eq!(c.data(), &[0_i64]),
        other => panic!("unexpected column: {other:?}"),
    }
}

// -----------------------------------------------------------------------
// Test 4: schema reports affected_rows column
// -----------------------------------------------------------------------

#[test]
fn modify_table_schema_is_affected_rows() {
    let heap = make_heap();
    let schema = schema_i32_text();
    let source = MemTableScan::new(schema.clone(), vec![]);
    let op = ModifyTable::new(
        Arc::clone(&heap),
        rel(),
        schema,
        ModifyKind::Insert,
        stamps(1),
        None,
        Box::new(source),
    );
    assert_eq!(op.schema().len(), 1);
    assert_eq!(op.schema().field_at(0).name, "affected_rows");
    assert_eq!(op.schema().field_at(0).data_type, DataType::Int64);
}

#[test]
fn modify_table_builder_methods_store_runtime_descriptors() {
    let heap = make_heap();
    let schema = schema_i32_text();
    let source = MemTableScan::new(schema.clone(), vec![]);
    let wal = Arc::new(InMemoryWalSink::new()) as Arc<dyn ultrasql_storage::wal_sink::WalSink>;
    let observed = Arc::new(Mutex::new(Vec::new()));
    let observed_clone = Arc::clone(&observed);
    let sequence = Arc::new(
        ultrasql_storage::sequence::Sequence::new(SequenceOptions::default()).expect("sequence"),
    );
    let sequence_default = super::SequenceDefault::new("users_id_seq", sequence)
        .with_observer(Arc::new(move |name, value| {
            observed_clone.lock().push((name.to_owned(), value));
        }))
        .with_wal(Some(Arc::clone(&wal)), Xid::new(9), rel());
    let returning_schema =
        Schema::new([Field::required("id", DataType::Int32)]).expect("returning");
    let row_check: RowCheck = Arc::new(|_| Ok(()));
    let update_check: UpdateCheck = Arc::new(|_, _| Ok(()));

    let op = ModifyTable::new(
        Arc::clone(&heap),
        rel(),
        schema,
        ModifyKind::Insert,
        stamps(1),
        None,
        Box::new(source),
    )
    .with_visibility_map(Arc::new(VisibilityMap::new()))
    .with_insert_conflict_action(InsertConflictAction::DoNothing {
        target: Some(vec![0]),
    })
    .with_insert_column_map(vec![1, 0])
    .with_column_defaults(vec![Some(lit_i32(7)), None])
    .with_sequence_defaults(vec![Some(sequence_default), None])
    .with_identity_always(vec![true, false])
    .with_generated_stored(vec![None, Some(lit_text("stored"))])
    .with_check_constraints(vec![("ck_true".to_owned(), lit_bool(true))])
    .with_foreign_key_checks(vec![Arc::clone(&row_check)])
    .with_exclusion_checks(vec![Arc::clone(&row_check)])
    .with_exclusion_update_checks(vec![Arc::clone(&update_check)])
    .with_referenced_by_delete_checks(vec![Arc::clone(&row_check)])
    .with_referenced_by_update_checks(vec![update_check])
    .with_returning(vec![col_i32("id", 0)], returning_schema);

    assert!(op.vm.is_some());
    assert!(matches!(
        op.insert_conflict_action,
        Some(InsertConflictAction::DoNothing { .. })
    ));
    assert_eq!(op.insert_column_map.as_deref(), Some(&[1, 0][..]));
    assert_eq!(op.column_defaults.len(), 2);
    assert_eq!(op.sequence_defaults.len(), 2);
    assert_eq!(op.identity_always, vec![true, false]);
    assert_eq!(op.generated_stored.len(), 2);
    assert_eq!(op.check_constraints[0].name, "ck_true");
    assert_eq!(op.foreign_key_checks.len(), 1);
    assert_eq!(op.exclusion_checks.len(), 1);
    assert_eq!(op.exclusion_update_checks.len(), 1);
    assert_eq!(op.referenced_by_delete_checks.len(), 1);
    assert_eq!(op.referenced_by_update_checks.len(), 1);
    assert_eq!(op.returning_evaluators.len(), 1);
    assert_eq!(op.schema.field_at(0).name, "id");
    assert!(observed.lock().is_empty());
}

#[test]
fn not_null_and_row_codec_errors_map_to_sql_errors() {
    let schema = schema_i32_text();
    let err =
        check_not_null_violations(&[Value::Int32(1), Value::Null], &schema).expect_err("not null");
    assert!(matches!(err, ExecError::NotNullViolation(ref col) if col == "name"));
    check_not_null_violations(&[Value::Int32(1), Value::Text("ok".to_owned())], &schema)
        .expect("valid row");

    let trunc = row_codec_error_to_exec(RowCodecError::StringDataRightTruncation {
        column: 1,
        ty: DataType::Char { len: Some(2) },
        detail: "too long".to_owned(),
    });
    assert!(
        matches!(trunc, ExecError::StringDataRightTruncation(ref detail) if detail == "too long")
    );

    let ty = row_codec_error_to_exec(RowCodecError::Arity { schema: 2, row: 1 });
    assert!(matches!(ty, ExecError::TypeMismatch(_)));
}
