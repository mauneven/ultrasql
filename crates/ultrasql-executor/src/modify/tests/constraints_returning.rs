//! INSERT column maps, sequence/generated defaults, constraints,
//! RETURNING, and the UPDATE/DELETE operator slow paths.

use super::*;

#[test]
fn insert_column_map_sequence_generated_constraints_and_returning() {
    let heap = make_heap();
    let target_schema = Schema::new([
        Field::required("id", DataType::Int32),
        Field::required("name", DataType::Text { max_len: None }),
        Field::required("stored", DataType::Text { max_len: None }),
    ])
    .expect("target schema");
    let source_schema = Schema::new([Field::required("name", DataType::Text { max_len: None })])
        .expect("source schema");
    let source = ValuesScan::new(vec![vec![lit_text("alpha")]], source_schema);

    let observed = Arc::new(Mutex::new(Vec::new()));
    let observed_clone = Arc::clone(&observed);
    let wal = Arc::new(InMemoryWalSink::new()) as Arc<dyn ultrasql_storage::wal_sink::WalSink>;
    let sequence = Arc::new(
        ultrasql_storage::sequence::Sequence::new(SequenceOptions::default()).expect("sequence"),
    );
    let sequence_default = super::SequenceDefault::new("users_id_seq", sequence)
        .with_observer(Arc::new(move |name, value| {
            observed_clone.lock().push((name.to_owned(), value));
        }))
        .with_wal(Some(Arc::clone(&wal)), Xid::new(11), rel());
    let fk_hits = Arc::new(Mutex::new(0_usize));
    let fk_hits_clone = Arc::clone(&fk_hits);
    let fk: RowCheck = Arc::new(move |row| {
        assert_eq!(row[1], Value::Text("alpha".to_owned()));
        *fk_hits_clone.lock() += 1;
        Ok(())
    });
    let exclusion_hits = Arc::new(Mutex::new(0_usize));
    let exclusion_hits_clone = Arc::clone(&exclusion_hits);
    let exclusion: RowCheck = Arc::new(move |row| {
        assert_eq!(row[2], Value::Text("stored".to_owned()));
        *exclusion_hits_clone.lock() += 1;
        Ok(())
    });
    let returning_schema = Schema::new([
        Field::required("id", DataType::Int32),
        Field::required("stored", DataType::Text { max_len: None }),
    ])
    .expect("returning schema");

    let mut op = ModifyTable::new(
        Arc::clone(&heap),
        rel(),
        target_schema,
        ModifyKind::Insert,
        stamps(11),
        Some(wal),
        Box::new(source),
    )
    .with_insert_column_map(vec![1])
    .with_sequence_defaults(vec![Some(sequence_default), None, None])
    .with_identity_always(vec![true, false, false])
    .with_generated_stored(vec![None, None, Some(lit_text("stored"))])
    .with_check_constraints(vec![("ck_true".to_owned(), lit_bool(true))])
    .with_foreign_key_checks(vec![fk])
    .with_exclusion_checks(vec![exclusion])
    .with_returning(
        vec![col_i32("id", 0), col_text("stored", 2)],
        returning_schema,
    );

    let batch = op.next_batch().expect("insert").expect("returning");
    assert_eq!(batch.rows(), 1);
    match &batch.columns()[0] {
        Column::Int32(c) => assert_eq!(c.data(), &[1]),
        other => panic!("unexpected id column {other:?}"),
    }
    assert_eq!(batch.columns()[1].text_value(0), Some("stored"));
    assert_eq!(&*observed.lock(), &[("users_id_seq".to_owned(), 1)]);
    assert_eq!(*fk_hits.lock(), 1);
    assert_eq!(*exclusion_hits.lock(), 1);
}

#[test]
fn update_and_delete_operator_paths_cover_slow_branches_and_returning() {
    let heap = make_heap();
    let schema = schema_i32_text();
    let old_tid = insert_payload(
        &heap,
        &schema,
        &[Value::Int32(1), Value::Text("old".to_owned())],
    );
    let child_schema = tid_row_schema(&schema);
    let update_source = ValuesScan::new(
        vec![vec![
            lit_i32(i32::try_from(old_tid.page.block.raw()).expect("block fits")),
            lit_i32(i32::from(old_tid.slot)),
            lit_i32(1),
            lit_text("old"),
        ]],
        child_schema.clone(),
    );
    let returning_schema = Schema::new([Field::required("name", DataType::Text { max_len: None })])
        .expect("returning");
    let mut update = ModifyTable::new(
        Arc::clone(&heap),
        rel(),
        schema.clone(),
        ModifyKind::Update {
            assignments: vec![(1, lit_text("new"))],
        },
        stamps(2),
        None,
        Box::new(update_source),
    )
    .with_returning(vec![col_text("name", 1)], returning_schema);

    let batch = update.next_batch().expect("update").expect("returning");
    assert_eq!(batch.rows(), 1);
    assert_eq!(batch.columns()[0].text_value(0), Some("new"));

    let delete_tid = insert_payload(
        &heap,
        &schema,
        &[Value::Int32(2), Value::Text("gone".to_owned())],
    );
    let delete_source = ValuesScan::new(
        vec![vec![
            lit_i32(i32::try_from(delete_tid.page.block.raw()).expect("block fits")),
            lit_i32(i32::from(delete_tid.slot)),
            lit_i32(2),
            lit_text("gone"),
        ]],
        child_schema,
    );
    let returning_schema =
        Schema::new([Field::required("id", DataType::Int32)]).expect("returning");
    let mut delete = ModifyTable::new(
        Arc::clone(&heap),
        rel(),
        schema,
        ModifyKind::Delete,
        stamps(3),
        None,
        Box::new(delete_source),
    )
    .with_returning(vec![col_i32("id", 0)], returning_schema);

    let batch = delete.next_batch().expect("delete").expect("returning");
    assert_eq!(batch.rows(), 1);
    match &batch.columns()[0] {
        Column::Int32(c) => assert_eq!(c.data(), &[2]),
        other => panic!("unexpected returning column {other:?}"),
    }
}
