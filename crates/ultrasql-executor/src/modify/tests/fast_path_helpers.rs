//! Columnar UPDATE fast-path detection, TID extraction, overflow
//! rejection, and conflict-target / ctid-redirect helper coverage.

use super::*;

#[test]
fn update_int32_pair_fast_path_detection_covers_supported_shapes() {
    let schema = schema_i32_pair();
    let add_right = binary_i32(BinaryOp::Add, col_i32("val", 1), lit_i32(3));
    let spec = detect_update_int32_pair_fast_path(&[(1, add_right)], &schema).expect("add");
    assert_eq!(spec.target_col_in_relation, 1);
    assert_eq!(spec.delta, 3);

    let add_left = binary_i32(BinaryOp::Add, lit_i32(4), col_i32("id", 0));
    let spec = detect_update_int32_pair_fast_path(&[(0, add_left)], &schema).expect("add");
    assert_eq!(spec.target_col_in_relation, 0);
    assert_eq!(spec.delta, 4);

    let sub = binary_i32(BinaryOp::Sub, col_i32("val", 1), lit_i32(5));
    let spec = detect_update_int32_pair_fast_path(&[(1, sub)], &schema).expect("sub");
    assert_eq!(spec.delta, -5);

    let lit_minus_col = binary_i32(BinaryOp::Sub, lit_i32(5), col_i32("val", 1));
    assert!(detect_update_int32_pair_fast_path(&[(1, lit_minus_col)], &schema).is_none());
    assert!(
        detect_update_int32_pair_fast_path(
            &[(2, binary_i32(BinaryOp::Add, col_i32("val", 1), lit_i32(1)))],
            &schema
        )
        .is_none()
    );
    assert!(detect_update_int32_pair_fast_path(&[], &schema).is_none());
    assert!(
        detect_update_int32_pair_fast_path(
            &[(1, binary_i32(BinaryOp::Gt, col_i32("val", 1), lit_i32(1)))],
            &schema
        )
        .is_none()
    );
}

#[test]
fn tid_extraction_and_update_fast_payloads_validate_shapes() {
    let batch = Batch::new([
        Column::Int32(NumericColumn::from_data(vec![2, 3])),
        Column::Int32(NumericColumn::from_data(vec![7, 8])),
        Column::Int32(NumericColumn::from_data(vec![10, 20])),
        Column::Int32(NumericColumn::from_data(vec![100, 200])),
    ])
    .expect("batch");
    let edits = build_update_edits_int32_pair(
        &batch,
        rel(),
        UpdateFastPathInt32Pair {
            target_col_in_relation: 1,
            delta: 5,
        },
    )
    .expect("edits");
    assert_eq!(edits[0].0, tid(2, 7));
    assert_eq!(edits[0].1.as_slice(), &[0, 10, 0, 0, 0, 105, 0, 0, 0]);

    let tids = extract_tids_from_batch(&batch, rel()).expect("tids");
    assert_eq!(tids, vec![tid(2, 7), tid(3, 8)]);

    let tid_row = [Value::Int32(4), Value::Int32(9), Value::Text("x".into())];
    let (one_tid, row) = extract_tid_and_row(&tid_row, rel()).expect("tid row");
    assert_eq!(one_tid, tid(4, 9));
    assert_eq!(row, &[Value::Text("x".to_owned())]);

    let bad_short = Batch::new([Column::Int32(NumericColumn::from_data(vec![1]))]).expect("batch");
    assert!(extract_tids_from_batch(&bad_short, rel()).is_err());
    assert!(
        build_update_edits_int32_pair(
            &bad_short,
            rel(),
            UpdateFastPathInt32Pair {
                target_col_in_relation: 0,
                delta: 1,
            },
        )
        .is_err()
    );

    let bad_negative = Batch::new([
        Column::Int32(NumericColumn::from_data(vec![-1])),
        Column::Int32(NumericColumn::from_data(vec![1])),
    ])
    .expect("batch");
    assert!(extract_tids_from_batch(&bad_negative, rel()).is_err());

    assert!(extract_tid_and_row(&[Value::Text("bad".into())], rel()).is_err());
    assert!(extract_tid_and_row(&[Value::Int32(-1), Value::Int32(1)], rel()).is_err());
    assert!(extract_tid_and_row(&[Value::Int32(1), Value::Int32(70_000)], rel()).is_err());
}

#[test]
fn update_fast_payloads_reject_int32_overflow() {
    let batch = Batch::new([
        Column::Int32(NumericColumn::from_data(vec![2])),
        Column::Int32(NumericColumn::from_data(vec![7])),
        Column::Int32(NumericColumn::from_data(vec![10])),
        Column::Int32(NumericColumn::from_data(vec![i32::MAX])),
    ])
    .expect("batch");

    let err = build_update_edits_int32_pair(
        &batch,
        rel(),
        UpdateFastPathInt32Pair {
            target_col_in_relation: 1,
            delta: 1,
        },
    )
    .expect_err("overflow must reject fast update payload");
    assert!(matches!(err, ExecError::NumericFieldOverflow(_)), "{err:?}");
}

#[test]
fn conflict_targets_and_ctid_redirect_helpers_cover_edge_cases() {
    assert!(columns_match_unordered(&[2, 1], &[1, 2]));
    assert!(!columns_match_unordered(&[1, 2], &[1, 3]));

    let do_nothing = InsertConflictAction::DoNothing {
        target: Some(vec![1, 2]),
    };
    assert_eq!(conflict_target_columns(&do_nothing), Some(&[1, 2][..]));
    let do_nothing_any = InsertConflictAction::DoNothing { target: None };
    assert!(conflict_target_columns(&do_nothing_any).is_none());
    let do_update = InsertConflictAction::DoUpdate {
        target: vec![0],
        assignments: Vec::new(),
        predicate: None,
    };
    assert_eq!(conflict_target_columns(&do_update), Some(&[0][..]));

    let current = tid(1, 1);
    let next = tid(1, 2);
    let mut header = TupleHeader::fresh(Xid::new(1), CommandId::FIRST, current, 2);
    header.ctid = next;
    assert_eq!(updated_ctid_target(&header, current), None);
    header.infomask.set(InfoMask::UPDATED);
    assert_eq!(updated_ctid_target(&header, current), Some(next));
    header.ctid = current;
    assert_eq!(updated_ctid_target(&header, current), None);
}
