//! ON CONFLICT resolution, default/constraint error edges, and
//! B-tree index conflict + maintenance helper coverage.

use super::*;

#[test]
fn update_conflict_defaults_and_constraint_helpers_cover_error_edges() {
    let heap = make_heap();
    let schema = schema_i32_text();
    let child = MemTableScan::new(tid_row_schema(&schema), vec![]);
    let update_op = ModifyTable::new(
        Arc::clone(&heap),
        rel(),
        schema.clone(),
        ModifyKind::Update {
            assignments: vec![(1, lit_text("computed"))],
        },
        stamps(4),
        None,
        Box::new(child),
    );
    let update_row = [
        Value::Int32(0),
        Value::Int32(7),
        Value::Int32(9),
        Value::Text("before".to_owned()),
    ];
    let computed = update_op
        .compute_update_edit(&update_row, true)
        .expect("computed update");
    assert_eq!(computed.tid, tid(0, 7));
    assert_eq!(
        computed.returning_row,
        Some(vec![Value::Int32(9), Value::Text("computed".to_owned())])
    );

    let conflict = update_op
        .compute_conflict_update_edit(
            tid(0, 8),
            &[Value::Int32(1), Value::Text("old".to_owned())],
            &[Value::Int32(1), Value::Text("excluded".to_owned())],
            &[(1, Eval::new(lit_text("merged")))],
            Some(&Eval::new(lit_bool(true))),
            true,
        )
        .expect("conflict update")
        .expect("updated");
    assert_eq!(
        conflict.returning_row,
        Some(vec![Value::Int32(1), Value::Text("merged".to_owned())])
    );
    assert!(
        update_op
            .compute_conflict_update_edit(
                tid(0, 8),
                &[Value::Int32(1), Value::Text("old".to_owned())],
                &[Value::Int32(1), Value::Text("excluded".to_owned())],
                &[(1, Eval::new(lit_text("merged")))],
                Some(&Eval::new(lit_bool(false))),
                false,
            )
            .expect("predicate false")
            .is_none()
    );
    assert!(
        update_op
            .compute_conflict_update_edit(
                tid(0, 8),
                &[Value::Int32(1), Value::Text("old".to_owned())],
                &[Value::Int32(1), Value::Text("excluded".to_owned())],
                &[(1, Eval::new(lit_text("merged")))],
                Some(&Eval::new(lit_i32(1))),
                false,
            )
            .is_err()
    );

    let insert_op = ModifyTable::new(
        Arc::clone(&heap),
        rel(),
        schema.clone(),
        ModifyKind::Insert,
        stamps(5),
        None,
        Box::new(MemTableScan::new(schema.clone(), vec![])),
    )
    .with_column_defaults(vec![Some(lit_i32(42)), None]);
    let mut row = vec![Value::Null, Value::Text("kept".to_owned())];
    insert_op
        .apply_insert_defaults(&mut row, &[true, false])
        .expect("defaults");
    assert_eq!(row[0], Value::Int32(42));
    assert!(insert_op.apply_insert_defaults(&mut row, &[true]).is_err());

    let generated_op = ModifyTable::new(
        Arc::clone(&heap),
        rel(),
        schema.clone(),
        ModifyKind::Insert,
        stamps(6),
        None,
        Box::new(MemTableScan::new(schema.clone(), vec![])),
    )
    .with_generated_stored(vec![None, Some(lit_text("stored"))])
    .with_identity_always(vec![true, false]);
    assert!(
        generated_op
            .check_identity_explicit_values(&[false, true])
            .is_err()
    );
    assert!(
        generated_op
            .check_generated_stored_explicit_values(&[true, false])
            .is_err()
    );
    let mut generated_row = vec![Value::Int32(1), Value::Null];
    generated_op
        .apply_generated_stored(&mut generated_row)
        .expect("generated");
    assert_eq!(generated_row[1], Value::Text("stored".to_owned()));
    assert!(
        generated_op
            .apply_generated_stored(&mut [Value::Int32(1)])
            .is_err()
    );

    let check_false = ModifyTable::new(
        Arc::clone(&heap),
        rel(),
        schema.clone(),
        ModifyKind::Insert,
        stamps(7),
        None,
        Box::new(MemTableScan::new(schema.clone(), vec![])),
    )
    .with_check_constraints(vec![("ck_false".to_owned(), lit_bool(false))]);
    assert!(matches!(
        check_false.check_row_constraints(&[Value::Int32(1), Value::Text("x".to_owned())]),
        Err(ExecError::CheckViolation(ref name)) if name == "ck_false"
    ));
    let check_type = ModifyTable::new(
        Arc::clone(&heap),
        rel(),
        schema.clone(),
        ModifyKind::Insert,
        stamps(8),
        None,
        Box::new(MemTableScan::new(schema, vec![])),
    )
    .with_check_constraints(vec![("ck_type".to_owned(), lit_i32(1))]);
    assert!(
        check_type
            .check_row_constraints(&[Value::Int32(1), Value::Text("x".to_owned())])
            .is_err()
    );

    let expanded = expand_insert_row(&[Value::Int32(3)], 2, &[1]).expect("expanded");
    assert_eq!(expanded.values, vec![Value::Null, Value::Int32(3)]);
    assert_eq!(expanded.omitted, vec![true, false]);
    assert!(expand_insert_row(&[Value::Int32(1)], 2, &[2]).is_err());
    assert!(expand_insert_row(&[Value::Int32(1)], 2, &[0, 0]).is_err());

    let int64_schema =
        Schema::new([Field::required("seq", DataType::Int64)]).expect("int64 schema");
    let seq_op = ModifyTable::new(
        heap,
        rel(),
        int64_schema,
        ModifyKind::Insert,
        stamps(9),
        None,
        Box::new(MemTableScan::new(
            Schema::new([Field::required("seq", DataType::Int64)]).expect("source"),
            vec![],
        )),
    );
    let seq = Arc::new(
        ultrasql_storage::sequence::Sequence::new(SequenceOptions {
            start: 9,
            ..SequenceOptions::default()
        })
        .expect("sequence"),
    );
    let default = super::SequenceDefault::new("s", seq);
    assert_eq!(
        seq_op
            .next_sequence_default_value(0, &default)
            .expect("seq"),
        Value::Int64(9)
    );
}

#[test]
fn btree_index_conflict_and_maintenance_helpers_cover_index_paths() {
    let heap = make_heap();
    let schema = schema_i32_text();
    let existing = tid(0, 1);
    let mut index = btree_index("idx_users_id", true);
    assert!(format!("{index:?}").contains("idx_users_id"));
    assert_eq!(
        index
            .encode_key(&[Value::Int32(7), Value::Text("x".to_owned())])
            .expect("key"),
        Some(7)
    );
    assert!(!index.contains_key(7).expect("missing"));
    index
        .insert_key(7, existing, Xid::new(10), None)
        .expect("insert key");
    assert!(index.contains_key(7).expect("present"));
    assert!(matches!(
        index.insert_key(7, tid(0, 2), Xid::new(10), None),
        Err(ExecError::UniqueViolation(ref name)) if name == "idx_users_id"
    ));
    assert!(
        index
            .delete_key(7, existing, Xid::new(11), None)
            .expect("delete key")
    );
    assert!(!index.contains_key(7).expect("deleted"));

    let mut conflict_index = btree_index("idx_conflict", true);
    conflict_index
        .insert_key(7, existing, Xid::new(12), None)
        .expect("seed conflict");
    let op = ModifyTable::new(
        Arc::clone(&heap),
        rel(),
        schema.clone(),
        ModifyKind::Insert,
        stamps(12),
        None,
        Box::new(MemTableScan::new(schema.clone(), vec![])),
    )
    .with_insert_indexes(vec![conflict_index]);
    let action = InsertConflictAction::DoNothing {
        target: Some(vec![0]),
    };
    op.validate_insert_conflict_arbiter(Some(&action))
        .expect("arbiter");
    assert!(
        op.validate_insert_conflict_arbiter(Some(&InsertConflictAction::DoNothing {
            target: Some(vec![1])
        }))
        .is_err()
    );
    assert!(matches!(
        op.find_insert_conflict(&action, &[Some(7)], &[HashSet::new()])
            .expect("existing"),
        Some(super::InsertConflict::Existing(t)) if t == existing
    ));
    let mut seen = vec![HashSet::new()];
    seen[0].insert(8);
    assert!(matches!(
        op.find_insert_conflict(&action, &[Some(8)], &seen)
            .expect("in batch"),
        Some(super::InsertConflict::InBatch)
    ));
    op.remember_insert_keys(&[Some(9)], &mut seen);
    assert!(seen[0].contains(&9));
    let mut duplicate_seen = vec![HashSet::new()];
    op.reject_duplicate_insert_keys(&[Some(10)], &mut duplicate_seen)
        .expect("first key");
    assert!(
        op.reject_duplicate_insert_keys(&[Some(10)], &mut duplicate_seen)
            .is_err()
    );

    let mut update_index = btree_index("idx_update", true);
    let old_tid = tid(0, 3);
    let new_tid = tid(0, 4);
    update_index
        .insert_key(1, old_tid, Xid::new(13), None)
        .expect("old key");
    let mut update_op = ModifyTable::new(
        Arc::clone(&heap),
        rel(),
        schema.clone(),
        ModifyKind::Update {
            assignments: vec![(0, lit_i32(2))],
        },
        stamps(13),
        None,
        Box::new(MemTableScan::new(tid_row_schema(&schema), vec![])),
    )
    .with_update_indexes(vec![update_index]);
    let changes = vec![UpdateIndexChange {
        old_tid,
        old_keys: vec![Some(1)],
        new_keys: vec![Some(2)],
    }];
    update_op
        .precheck_update_index_changes(&changes)
        .expect("precheck");
    update_op
        .apply_update_index_changes(
            &changes,
            &[UpdateOutcome {
                old_tid,
                new_tid,
                hot: false,
            }],
            None,
        )
        .expect("apply update index");
    assert!(
        !update_op.update_indexes[0]
            .contains_key(1)
            .expect("old gone")
    );
    assert!(
        update_op.update_indexes[0]
            .contains_key(2)
            .expect("new key")
    );

    let mut delete_index = btree_index("idx_delete", true);
    delete_index
        .insert_key(5, old_tid, Xid::new(14), None)
        .expect("delete seed");
    let mut delete_op = ModifyTable::new(
        Arc::clone(&heap),
        rel(),
        schema,
        ModifyKind::Delete,
        stamps(14),
        None,
        Box::new(MemTableScan::new(
            Schema::new([
                Field::required("tid_block", DataType::Int32),
                Field::required("tid_slot", DataType::Int32),
            ])
            .expect("delete source"),
            vec![],
        )),
    )
    .with_delete_indexes(vec![delete_index]);
    delete_op
        .apply_delete_index_changes(&[DeleteIndexChange {
            tid: old_tid,
            keys: vec![Some(5)],
        }])
        .expect("delete index");
    assert!(
        !delete_op.delete_indexes[0]
            .contains_key(5)
            .expect("delete gone")
    );
}