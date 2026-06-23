//! ON CONFLICT resolution, default/constraint error edges, and
//! B-tree index conflict + maintenance helper coverage.

use super::*;
use ultrasql_mvcc::Snapshot;
use ultrasql_mvcc::status::test_support::MapOracle;
use ultrasql_storage::heap::DeleteOptions;

/// Insert a single-column-`Int32` row with an explicit `xmin`, returning its
/// TID. Mirrors [`insert_payload`] but lets the test choose the inserter XID
/// so liveness classification can be exercised against a chosen oracle.
fn insert_row_with_xmin(
    heap: &HeapAccess<MapLoader>,
    schema: &Schema,
    row: &[Value],
    xmin: Xid,
) -> TupleId {
    let codec = RowCodec::new(schema.clone());
    let payload = codec.encode(row).expect("payload");
    let tids = heap
        .insert_batch(
            rel(),
            &[payload.as_slice()],
            InsertOptions {
                xmin,
                command_id: CommandId::FIRST,
                n_atts: u16::try_from(schema.len()).expect("test schema fits u16"),
                wal: None,
                fsm: None,
                vm: None,
            },
        )
        .expect("insert row");
    tids[0]
}

/// BUG-1 regression (aborted-deleter misclassification).
///
/// A row whose inserter committed and whose deleter ABORTED is STILL LIVE: the
/// aborted DELETE never happened. The unique-conflict classifier must report
/// `Live` for such a row so a second inserter of the same key is rejected and
/// does NOT physically replace the live leaf entry. Before the fix
/// `tuple_is_pending_live` only treated an `InProgress` deleter as live-keeping
/// and fell through (→ `Dead`) for an aborted deleter, corrupting the index.
#[test]
fn classify_unique_conflict_keeps_aborted_deleter_row_live() {
    let heap = make_heap();
    let schema = Schema::new([Field::required("id", DataType::Int32)]).expect("schema");

    let inserter = Xid::new(50); // committed
    let aborted_deleter = Xid::new(60); // aborted DELETE — did NOT happen
    let key = 7_i64;
    let tid = insert_row_with_xmin(&heap, &schema, &[Value::Int32(7)], inserter);

    // Stamp xmax = aborted_deleter (a DELETE that later aborts).
    heap.delete(
        tid,
        DeleteOptions {
            xmax: aborted_deleter,
            cmax: CommandId::FIRST,
            wal: None,
            fsm: None,
            vm: None,
        },
    )
    .expect("stamp xmax");

    let oracle = MapOracle::new();
    oracle.set_committed(inserter);
    oracle.set_aborted(aborted_deleter);

    // A snapshot taken before `inserter` committed: it lists `inserter` as
    // in-flight, so the row is `Invisible` and the classifier must fall to
    // the pending-live test — exactly the path the bug corrupted.
    let snapshot = Snapshot::new(
        inserter,
        Xid::new(100),
        Xid::new(99),
        CommandId::FIRST,
        [inserter],
    );

    let mut index = btree_index("idx_aborted_deleter", true);
    index
        .insert_key(key, tid, Xid::new(70), None)
        .expect("seed");

    let conflict = index
        .classify_unique_conflict(key, &heap, &snapshot, &oracle)
        .expect("classify");
    assert_eq!(
        conflict,
        crate::modify::index_maintainer::UniqueConflict::Live,
        "row with a committed inserter and an ABORTED deleter is still live; \
         classifying it Dead would let a second inserter replace the live entry"
    );
}

/// Companion to the above: a row whose delete actually COMMITTED is genuinely
/// dead, so its key is reusable (`Dead`). Guards against the fix over-correcting
/// into never reporting a reusable key.
#[test]
fn classify_unique_conflict_reports_committed_delete_as_dead() {
    let heap = make_heap();
    let schema = Schema::new([Field::required("id", DataType::Int32)]).expect("schema");

    let inserter = Xid::new(50); // committed
    let committed_deleter = Xid::new(60); // committed DELETE — row is dead
    let key = 7_i64;
    let tid = insert_row_with_xmin(&heap, &schema, &[Value::Int32(7)], inserter);
    heap.delete(
        tid,
        DeleteOptions {
            xmax: committed_deleter,
            cmax: CommandId::FIRST,
            wal: None,
            fsm: None,
            vm: None,
        },
    )
    .expect("stamp xmax");

    let oracle = MapOracle::new();
    oracle.set_committed(inserter);
    oracle.set_committed(committed_deleter);

    let snapshot = Snapshot::new(
        Xid::new(200),
        Xid::new(200),
        Xid::new(199),
        CommandId::FIRST,
        [],
    );

    let mut index = btree_index("idx_committed_delete", true);
    index
        .insert_key(key, tid, Xid::new(70), None)
        .expect("seed");

    let conflict = index
        .classify_unique_conflict(key, &heap, &snapshot, &oracle)
        .expect("classify");
    assert_eq!(
        conflict,
        crate::modify::index_maintainer::UniqueConflict::Dead(tid),
        "a committed DELETE makes the row dead; its key is reusable"
    );
}

/// A row whose inserter ABORTED is genuinely dead even with no deleter — the
/// insert never happened. Guards the `status(xmin) == Aborted` short-circuit.
#[test]
fn classify_unique_conflict_reports_aborted_inserter_as_dead() {
    let heap = make_heap();
    let schema = Schema::new([Field::required("id", DataType::Int32)]).expect("schema");

    let aborted_inserter = Xid::new(50); // aborted INSERT — never happened
    let key = 7_i64;
    let tid = insert_row_with_xmin(&heap, &schema, &[Value::Int32(7)], aborted_inserter);

    let oracle = MapOracle::new();
    oracle.set_aborted(aborted_inserter);

    let snapshot = Snapshot::new(
        Xid::new(200),
        Xid::new(200),
        Xid::new(199),
        CommandId::FIRST,
        [],
    );

    let mut index = btree_index("idx_aborted_inserter", true);
    index
        .insert_key(key, tid, Xid::new(70), None)
        .expect("seed");

    let conflict = index
        .classify_unique_conflict(key, &heap, &snapshot, &oracle)
        .expect("classify");
    assert_eq!(
        conflict,
        crate::modify::index_maintainer::UniqueConflict::Dead(tid),
        "an aborted inserter leaves a dead row; its key is reusable"
    );
}

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
    // Option-A (design §1 A2): the old key's leaf entry is intentionally
    // NOT physically removed on a key-changing UPDATE — it lingers for
    // VACUUM and is filtered by the read-side heap recheck. The new key's
    // entry is inserted as the live one.
    assert!(
        update_op.update_indexes[0]
            .contains_key(1)
            .expect("old key lingers under Option-A (filtered by heap recheck)")
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
    // Option-A (design §1 A1): MVCC DELETE no longer physically removes the
    // B-tree leaf entry. The entry lingers (filtered by the read-side heap
    // recheck) until VACUUM reclaims it; `apply_delete_index_changes` is a
    // B-tree no-op.
    assert!(
        delete_op.delete_indexes[0]
            .contains_key(5)
            .expect("delete-key entry lingers under Option-A (filtered by heap recheck)")
    );
}
