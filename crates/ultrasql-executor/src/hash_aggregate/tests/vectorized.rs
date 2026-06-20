//! Vectorised fast-path cross-validation, overflow, cancellation, and
//! identity-row tests.

use super::*;

/// Test 1: SUM(i64) over 4096 rows. The vectorised column path and the
/// row-at-a-time scalar path must produce bit-identical results on
/// dense (non-null) data. NULL-aware behaviour is exercised separately
/// because the v0.5 `batch_to_rows` decoder does not yet honour the
/// column validity bitmap, so the row-loop reference is only meaningful
/// when every row is valid.
#[test]
fn vectorized_sum_i64_matches_scalar() {
    let n = 4096_i64;
    // Deterministic values that exercise signed accumulation without
    // crossing the overflow boundary. Overflow behavior has a dedicated
    // typed-error test below.
    let values: Vec<i64> = (0..n).map(|i| (i % 257) - 128).collect();
    let (schema, batch) = make_i64_batch(values.clone(), None);
    let out_schema =
        Schema::new([Field::nullable("total", DataType::Int64)]).expect("schema ok");

    let sum_val = LogicalAggregateExpr {
        func: AggregateFunc::Sum,
        arg: Some(col("val", 0, DataType::Int64)),
        direct_arg: None,
        order_by: None,
        distinct: false,
        output_name: "total".into(),
        data_type: DataType::Int64,
    };

    // Vectorised path.
    let scan_vec = MemTableScan::new(schema.clone(), vec![batch.clone()]);
    let mut op_vec = HashAggregate::new(
        Box::new(scan_vec),
        vec![],
        vec![sum_val.clone()],
        out_schema.clone(),
    );
    let rows_vec = drain_all(&mut op_vec);

    // Scalar path (forced off the fast path).
    let scan_sca = MemTableScan::new(schema, vec![batch]);
    let mut op_sca = HashAggregate::new(Box::new(scan_sca), vec![], vec![sum_val], out_schema);
    op_sca.force_scalar_path();
    let rows_sca = drain_all(&mut op_sca);

    assert_eq!(rows_vec.len(), 1);
    assert_eq!(rows_sca.len(), 1);
    assert_eq!(
        rows_vec[0], rows_sca[0],
        "vectorised SUM must equal scalar SUM bit-for-bit"
    );

    // Independent reference.
    let want: i64 = values.iter().copied().sum();
    assert_eq!(rows_vec[0][0], Value::Int64(want));
}

#[test]
fn hash_aggregate_vectorized_sum_i64_overflow_returns_typed_error() {
    let (schema, batch) = make_i64_batch(vec![i64::MAX, 1], None);
    let out_schema =
        Schema::new([Field::nullable("total", DataType::Int64)]).expect("schema ok");
    let sum_val = LogicalAggregateExpr {
        func: AggregateFunc::Sum,
        arg: Some(col("val", 0, DataType::Int64)),
        direct_arg: None,
        order_by: None,
        distinct: false,
        output_name: "total".into(),
        data_type: DataType::Int64,
    };
    let scan = MemTableScan::new(schema, vec![batch]);
    let mut op = HashAggregate::new(Box::new(scan), vec![], vec![sum_val], out_schema);

    let err = op
        .next_batch()
        .expect_err("vectorized SUM(BIGINT) overflow must not wrap");

    assert!(
        matches!(err, crate::ExecError::NumericFieldOverflow(_)),
        "{err:?}"
    );
}

#[test]
fn hash_aggregate_scalar_sum_i64_overflow_returns_typed_error() {
    let (schema, batch) = make_i64_batch(vec![i64::MAX, 1], None);
    let out_schema =
        Schema::new([Field::nullable("total", DataType::Int64)]).expect("schema ok");
    let sum_val = LogicalAggregateExpr {
        func: AggregateFunc::Sum,
        arg: Some(col("val", 0, DataType::Int64)),
        direct_arg: None,
        order_by: None,
        distinct: false,
        output_name: "total".into(),
        data_type: DataType::Int64,
    };
    let scan = MemTableScan::new(schema, vec![batch]);
    let mut op = HashAggregate::new(Box::new(scan), vec![], vec![sum_val], out_schema);
    op.force_scalar_path();

    let err = op
        .next_batch()
        .expect_err("scalar SUM(BIGINT) overflow must not wrap");

    assert!(
        matches!(err, crate::ExecError::NumericFieldOverflow(_)),
        "{err:?}"
    );
}

#[test]
fn scalar_count_states_reject_i64_overflow() {
    let mut count_star = AggState::CountStar(i64::MAX);
    let err = accumulate_value(&mut count_star, None)
        .expect_err("COUNT(*) overflow must not saturate");
    assert!(matches!(err, crate::ExecError::NumericFieldOverflow(_)));

    let mut count = AggState::Count(i64::MAX);
    let err = accumulate_value(&mut count, Some(Value::Int32(1)))
        .expect_err("COUNT(expr) overflow must not saturate");
    assert!(matches!(err, crate::ExecError::NumericFieldOverflow(_)));

    let mut avg = AggState::Avg(Some(Value::Int64(1)), i64::MAX);
    let err = accumulate_value(&mut avg, Some(Value::Int64(1)))
        .expect_err("AVG count overflow must not saturate");
    assert!(matches!(err, crate::ExecError::NumericFieldOverflow(_)));
}

#[test]
fn vectorized_count_states_reject_i64_overflow() {
    let batch = make_batch_i32_i64(&[(1, 10)]);

    let mut count_star = [AggState::CountStar(i64::MAX)];
    let err = vectorized_step(&[VecAggSlot::CountStar], &batch, &mut count_star)
        .expect_err("vectorized COUNT(*) overflow must not saturate");
    assert!(matches!(err, crate::ExecError::NumericFieldOverflow(_)));

    let mut count = [AggState::Count(i64::MAX)];
    let err = vectorized_step(&[VecAggSlot::Count(0)], &batch, &mut count)
        .expect_err("vectorized COUNT(expr) overflow must not saturate");
    assert!(matches!(err, crate::ExecError::NumericFieldOverflow(_)));

    let mut avg = [AggState::Avg(Some(Value::Int64(10)), i64::MAX)];
    let err = vectorized_step(&[VecAggSlot::Avg(1)], &batch, &mut avg)
        .expect_err("vectorized AVG count overflow must not saturate");
    assert!(matches!(err, crate::ExecError::NumericFieldOverflow(_)));
}

/// Companion NULL-handling check for the vectorised SUM path. The row-
/// loop reference is computed in Rust directly because v0.5's
/// `batch_to_rows` does not yet honour the column validity bitmap. The
/// kernel under test must (a) skip NULL rows, (b) return `Value::Null`
/// when every row is NULL.
#[test]
fn vectorized_sum_i64_honours_nulls() {
    let n = 1024_usize;
    let values: Vec<i64> = (0..i64::try_from(n).expect("n fits i64")).collect();
    let nulls_pat: Vec<bool> = (0..n).map(|i| !i.is_multiple_of(17)).collect();
    let (schema, batch) = make_i64_batch(values.clone(), Some(nulls_pat.clone()));
    let out_schema =
        Schema::new([Field::nullable("total", DataType::Int64)]).expect("schema ok");

    let sum_val = LogicalAggregateExpr {
        func: AggregateFunc::Sum,
        arg: Some(col("val", 0, DataType::Int64)),
        direct_arg: None,
        order_by: None,
        distinct: false,
        output_name: "total".into(),
        data_type: DataType::Int64,
    };

    let scan = MemTableScan::new(schema, vec![batch]);
    let mut op = HashAggregate::new(Box::new(scan), vec![], vec![sum_val], out_schema);
    let rows = drain_all(&mut op);

    let want: i64 = values
        .iter()
        .zip(nulls_pat.iter())
        .filter_map(|(v, valid)| valid.then_some(*v))
        .fold(0_i64, i64::wrapping_add);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Int64(want));

    // Independently verify the internal NULL semantics by exercising
    // the SUM accumulator on an all-NULL column: the accumulator must
    // stay `None`, which `finalise` returns as `Value::Null`. (The
    // operator's outbound `build_batch` does not yet preserve nulls in
    // v0.5, so we observe the NULL via the kernel directly rather than
    // through the round-trip.)
    let nulls_all = ultrasql_vec::Bitmap::new(32, false);
    let all_null_col =
        Column::Int64(NumericColumn::with_nulls(vec![42_i64; 32], nulls_all).expect("col ok"));
    let mut acc: Option<Value> = None;
    accumulate_sum(&mut acc, &all_null_col).expect("sum ok");
    assert!(acc.is_none(), "all-NULL accumulator must stay None");
    let state = AggState::Sum(acc);
    assert_eq!(finalise(&state).expect("sum finalise"), Value::Null);
}

/// Test 2: AVG(i32) over 4096 rows. The vectorised path widens the i32
/// accumulator to i64 (matching the scalar `add_values` widening), and
/// the final divide produces Float64. Dense (non-null) so the v0.5
/// `batch_to_rows` row-loop reference is well-defined.
#[test]
fn vectorized_avg_i32_matches_scalar() {
    // Use a range that fits well in i32 to keep the i64 sum unambiguous.
    let values: Vec<i32> = (0_i32..4096).map(|i| i - 2048).collect();

    let (schema, batch) = make_i32_batch(values, None);
    let out_schema =
        Schema::new([Field::nullable("avg_v", DataType::Float64)]).expect("schema ok");

    let avg_v = LogicalAggregateExpr {
        func: AggregateFunc::Avg,
        arg: Some(col("v", 0, DataType::Int32)),
        direct_arg: None,
        order_by: None,
        distinct: false,
        output_name: "avg_v".into(),
        data_type: DataType::Float64,
    };

    // Vectorised path.
    let scan_vec = MemTableScan::new(schema.clone(), vec![batch.clone()]);
    let mut op_vec = HashAggregate::new(
        Box::new(scan_vec),
        vec![],
        vec![avg_v.clone()],
        out_schema.clone(),
    );
    let rows_vec = drain_all(&mut op_vec);

    // Scalar path.
    let scan_sca = MemTableScan::new(schema, vec![batch]);
    let mut op_sca = HashAggregate::new(Box::new(scan_sca), vec![], vec![avg_v], out_schema);
    op_sca.force_scalar_path();
    let rows_sca = drain_all(&mut op_sca);

    assert_eq!(rows_vec.len(), 1);
    assert_eq!(rows_sca.len(), 1);

    // The result is Float64; compare via bit pattern for exact equality.
    match (&rows_vec[0][0], &rows_sca[0][0]) {
        (Value::Float64(a), Value::Float64(b)) => {
            assert_eq!(
                a.to_bits(),
                b.to_bits(),
                "vectorised AVG bits must equal scalar AVG bits"
            );
        }
        other => panic!("expected Float64 results, got {other:?}"),
    }
}

#[test]
fn scalar_avg_vector_skips_nulls_and_returns_dense_vector() {
    let vector_type = DataType::Vector { dims: Some(3) };
    let schema =
        Schema::new([Field::nullable("embedding", vector_type.clone())]).expect("schema ok");
    let mut valid = Bitmap::new(3, true);
    valid.set(2, false);
    let batch = Batch::new([Column::Utf8(
        StringColumn::with_nulls(
            vec![
                "[1,2,3]".to_owned(),
                "[3,4,5]".to_owned(),
                "[99,99,99]".to_owned(),
            ],
            valid,
        )
        .expect("string column ok"),
    )])
    .expect("batch ok");
    let out_schema = Schema::new([Field::nullable("avg_embedding", vector_type.clone())])
        .expect("schema ok");
    let avg_embedding = LogicalAggregateExpr {
        func: AggregateFunc::Avg,
        arg: Some(col("embedding", 0, vector_type.clone())),
        direct_arg: None,
        order_by: None,
        distinct: false,
        output_name: "avg_embedding".into(),
        data_type: vector_type,
    };

    let scan = MemTableScan::new(schema, vec![batch]);
    let mut op = HashAggregate::new(Box::new(scan), vec![], vec![avg_embedding], out_schema);
    let rows = drain_all(&mut op);

    assert_eq!(rows, vec![vec![Value::Vector(vec![2.0, 3.0, 4.0])]]);
}

#[test]
fn scalar_avg_vector_dimension_mismatch_errors() {
    let vector_type = DataType::Vector { dims: None };
    let schema =
        Schema::new([Field::nullable("embedding", vector_type.clone())]).expect("schema ok");
    let batch = Batch::new([Column::Utf8(StringColumn::from_data(
        ["[1,2]", "[1,2,3]"].map(str::to_owned),
    ))])
    .expect("batch ok");
    let out_schema = Schema::new([Field::nullable("avg_embedding", vector_type.clone())])
        .expect("schema ok");
    let avg_embedding = LogicalAggregateExpr {
        func: AggregateFunc::Avg,
        arg: Some(col("embedding", 0, vector_type.clone())),
        direct_arg: None,
        order_by: None,
        distinct: false,
        output_name: "avg_embedding".into(),
        data_type: vector_type,
    };

    let scan = MemTableScan::new(schema, vec![batch]);
    let mut op = HashAggregate::new(Box::new(scan), vec![], vec![avg_embedding], out_schema);
    let err = op.next_batch().expect_err("dimension mismatch must fail");

    assert!(
        err.to_string().contains("dimension mismatch"),
        "unexpected error: {err}"
    );
}

#[test]
fn grouped_vectorized_sum_i64_matches_scalar() {
    let schema = schema_group_val();
    let batch = make_batch_i32_i64(&[(1, 10), (2, 20), (1, 7), (2, 3), (3, 9)]);
    let out_schema = Schema::new([
        Field::required("group", DataType::Int32),
        Field::nullable("total", DataType::Int64),
    ])
    .expect("schema ok");

    let scan_vec = MemTableScan::new(schema.clone(), vec![batch.clone()]);
    let mut op_vec = HashAggregate::new(
        Box::new(scan_vec),
        vec![col("group", 0, DataType::Int32)],
        vec![sum_agg("val", 1)],
        out_schema.clone(),
    );
    let mut rows_vec = drain_all(&mut op_vec);

    let scan_sca = MemTableScan::new(schema, vec![batch]);
    let mut op_sca = HashAggregate::new(
        Box::new(scan_sca),
        vec![col("group", 0, DataType::Int32)],
        vec![sum_agg("val", 1)],
        out_schema,
    );
    op_sca.force_scalar_path();
    let mut rows_sca = drain_all(&mut op_sca);

    rows_vec.sort_by_key(|row| match row[0] {
        Value::Int32(v) => v,
        ref other => panic!("expected Int32 group key, got {other:?}"),
    });
    rows_sca.sort_by_key(|row| match row[0] {
        Value::Int32(v) => v,
        ref other => panic!("expected Int32 group key, got {other:?}"),
    });
    assert_eq!(rows_vec, rows_sca);
}

#[test]
fn grouped_vectorized_sum_mul_matches_scalar() {
    let schema = Schema::new([
        Field::required("partkey", DataType::Int32),
        Field::required(
            "cost",
            DataType::Decimal {
                precision: Some(15),
                scale: Some(2),
            },
        ),
        Field::required("qty", DataType::Int32),
    ])
    .expect("schema ok");
    let batch = Batch::new([
        Column::Int32(NumericColumn::from_data(vec![1_i32, 2, 1, 3])),
        Column::Int64(NumericColumn::from_data(vec![150_i64, 200, 25, 400])),
        Column::Int32(NumericColumn::from_data(vec![2_i32, 5, 4, 1])),
    ])
    .expect("batch ok");
    let out_schema = Schema::new([
        Field::required("partkey", DataType::Int32),
        Field::nullable(
            "value",
            DataType::Decimal {
                precision: Some(15),
                scale: Some(2),
            },
        ),
    ])
    .expect("schema ok");

    let scan_vec = MemTableScan::new(schema.clone(), vec![batch.clone()]);
    let mut op_vec = HashAggregate::new(
        Box::new(scan_vec),
        vec![col("partkey", 0, DataType::Int32)],
        vec![sum_decimal_mul_i32_agg()],
        out_schema.clone(),
    );
    let mut rows_vec = drain_all(&mut op_vec);

    let scan_sca = MemTableScan::new(schema, vec![batch]);
    let mut op_sca = HashAggregate::new(
        Box::new(scan_sca),
        vec![col("partkey", 0, DataType::Int32)],
        vec![sum_decimal_mul_i32_agg()],
        out_schema,
    );
    op_sca.force_scalar_path();
    let mut rows_sca = drain_all(&mut op_sca);

    rows_vec.sort_by_key(|row| match row[0] {
        Value::Int32(v) => v,
        ref other => panic!("expected Int32 group key, got {other:?}"),
    });
    rows_sca.sort_by_key(|row| match row[0] {
        Value::Int32(v) => v,
        ref other => panic!("expected Int32 group key, got {other:?}"),
    });
    assert_eq!(rows_vec, rows_sca);
}

#[test]
fn grouped_vectorized_i64_and_null_keys_are_finalized_correctly() {
    let schema = Schema::new([
        Field::nullable("group", DataType::Int64),
        Field::required("val", DataType::Int64),
    ])
    .expect("schema ok");
    let mut key_validity = Bitmap::new(3, true);
    key_validity.set(1, false);
    let batch = Batch::new([
        Column::Int64(
            NumericColumn::with_nulls(vec![10_i64, 99, 10], key_validity).expect("keys"),
        ),
        Column::Int64(NumericColumn::from_data(vec![1_i64, 2, 3])),
    ])
    .expect("batch ok");
    let out_schema = Schema::new([
        Field::nullable("group", DataType::Int64),
        Field::nullable("total", DataType::Int64),
    ])
    .expect("schema ok");
    let mut op = HashAggregate::new(
        Box::new(MemTableScan::new(schema, vec![batch])),
        vec![col("group", 0, DataType::Int64)],
        vec![sum_agg("val", 1)],
        out_schema,
    );

    let mut rows = drain_all(&mut op);
    rows.sort_by_key(|row| match row[0] {
        Value::Int64(v) => v,
        Value::Null => i64::MAX,
        ref other => panic!("unexpected key: {other:?}"),
    });

    assert_eq!(
        rows,
        vec![
            vec![Value::Int64(10), Value::Int64(4)],
            vec![Value::Null, Value::Int64(2)],
        ]
    );
}

#[test]
fn hash_agg_cancel_flag_is_observed_across_build_paths() {
    let pre_cancel = CancelFlag::new();
    pre_cancel.cancel();
    let (schema, batch) = make_i64_batch(vec![1_i64], None);
    let mut op = HashAggregate::new(
        Box::new(MemTableScan::new(schema, vec![batch])),
        vec![],
        vec![count_star_agg()],
        Schema::new([Field::required("cnt", DataType::Int64)]).expect("schema ok"),
    )
    .with_cancel_flag(pre_cancel);
    assert!(matches!(op.next_batch(), Err(crate::ExecError::Cancelled)));

    let vector_flag = CancelFlag::new();
    let (schema, batch) = make_i64_batch(vec![1_i64, 2], None);
    let scan = CancellingScan {
        schema,
        batch: Some(batch),
        flag: vector_flag.clone(),
    };
    let mut op = HashAggregate::new(
        Box::new(scan),
        vec![],
        vec![count_star_agg()],
        Schema::new([Field::required("cnt", DataType::Int64)]).expect("schema ok"),
    )
    .with_cancel_flag(vector_flag);
    assert!(matches!(op.next_batch(), Err(crate::ExecError::Cancelled)));

    let scalar_flag = CancelFlag::new();
    let schema = schema_group_val();
    let batch = make_batch_i32_i64(&[(1, 10), (2, 20)]);
    let scan = CancellingScan {
        schema,
        batch: Some(batch),
        flag: scalar_flag.clone(),
    };
    let mut op = HashAggregate::new(
        Box::new(scan),
        vec![col("group", 0, DataType::Int32)],
        vec![count_star_agg()],
        Schema::new([
            Field::required("group", DataType::Int32),
            Field::required("cnt", DataType::Int64),
        ])
        .expect("schema ok"),
    )
    .with_cancel_flag(scalar_flag);
    assert!(matches!(op.next_batch(), Err(crate::ExecError::Cancelled)));

    let spill_flag = CancelFlag::new();
    let schema = schema_group_val();
    let batch = make_batch_i32_i64(&[(1, 10), (2, 20)]);
    let scan = CancellingScan {
        schema,
        batch: Some(batch),
        flag: spill_flag.clone(),
    };
    let mut op = HashAggregate::new(
        Box::new(scan),
        vec![col("group", 0, DataType::Int32)],
        vec![sum_agg("val", 1)],
        Schema::new([
            Field::required("group", DataType::Int32),
            Field::required("total", DataType::Int64),
        ])
        .expect("schema ok"),
    )
    .with_work_mem_budget(std::sync::Arc::new(WorkMemBudget::new(1)))
    .with_cancel_flag(spill_flag);
    assert!(matches!(op.next_batch(), Err(crate::ExecError::Cancelled)));
}

#[test]
fn hash_agg_scalar_identity_and_vectorized_empty_batch_paths() {
    let schema = schema_group_val();
    let empty = make_batch_i32_i64(&[]);
    let scan = MemTableScan::new(schema, vec![empty]);
    let mut op = HashAggregate::new(
        Box::new(scan),
        vec![],
        vec![count_star_agg()],
        Schema::new([Field::required("cnt", DataType::Int64)]).expect("schema ok"),
    );
    let rows = drain_all(&mut op);
    assert_eq!(rows, vec![vec![Value::Int64(0)]]);

    let schema = schema_group_val();
    let scan = MemTableScan::new(schema, vec![]);
    let mut op = HashAggregate::new(
        Box::new(scan),
        vec![],
        vec![sum_agg("val", 1)],
        Schema::new([Field::nullable("total", DataType::Int64)]).expect("schema ok"),
    );
    op.force_scalar_path();
    let rows = drain_all(&mut op);
    assert_eq!(rows, vec![vec![Value::Null]]);
}

/// Test 3: COUNT(*) over a 100-row batch returns exactly 100 via the
/// vectorised path (no rows skipped, no null handling involved).
#[test]
fn vectorized_count_star_returns_batch_rows() {
    let values: Vec<i64> = (0_i64..100).collect();
    let (schema, batch) = make_i64_batch(values, None);
    let out_schema = Schema::new([Field::required("cnt", DataType::Int64)]).expect("schema ok");

    let scan = MemTableScan::new(schema, vec![batch]);
    let mut op = HashAggregate::new(Box::new(scan), vec![], vec![count_star_agg()], out_schema);
    let rows = drain_all(&mut op);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Int64(100));
}
