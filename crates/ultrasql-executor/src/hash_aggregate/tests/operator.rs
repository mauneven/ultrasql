//! Operator-level behaviour tests: GROUP BY, scalar aggregates, error
//! propagation, spilling, and the aggregate finalisation paths.

use super::*;

#[test]
fn hash_agg_count_star_no_group() {
    let schema = schema_group_val();
    let scan = MemTableScan::new(
        schema,
        vec![make_batch_i32_i64(&[(1, 10), (2, 20), (3, 30)])],
    );
    let out_schema = Schema::new([Field::required("cnt", DataType::Int64)]).expect("schema ok");
    let mut op = HashAggregate::new(Box::new(scan), vec![], vec![count_star_agg()], out_schema);
    let rows = drain_all(&mut op);
    assert_eq!(rows.len(), 1, "scalar aggregate emits exactly one row");
    assert_eq!(rows[0][0], Value::Int64(3), "COUNT(*) = 3");
}

// -------------------------------------------------------------------------
// Test 2: empty input, no group keys → single COUNT=0 row
// -------------------------------------------------------------------------

#[test]
fn hash_agg_empty_input_no_group_emits_identity_row() {
    let schema = schema_group_val();
    let scan = MemTableScan::new(schema, vec![]);
    let out_schema = Schema::new([Field::required("cnt", DataType::Int64)]).expect("schema ok");
    let mut op = HashAggregate::new(Box::new(scan), vec![], vec![count_star_agg()], out_schema);
    let rows = drain_all(&mut op);
    assert_eq!(rows.len(), 1, "empty table + no group keys = one row");
    assert_eq!(rows[0][0], Value::Int64(0), "COUNT(*) = 0");
}

// -------------------------------------------------------------------------
// Test 3: empty input with group keys → no rows
// -------------------------------------------------------------------------

#[test]
fn hash_agg_empty_input_with_group_keys_emits_nothing() {
    let schema = schema_group_val();
    let scan = MemTableScan::new(schema, vec![]);
    let out_schema = Schema::new([
        Field::required("group", DataType::Int32),
        Field::required("cnt", DataType::Int64),
    ])
    .expect("schema ok");
    let mut op = HashAggregate::new(
        Box::new(scan),
        vec![col("group", 0, DataType::Int32)],
        vec![count_star_agg()],
        out_schema,
    );
    let rows = drain_all(&mut op);
    assert!(rows.is_empty(), "empty table + group keys = no rows");
}

// -------------------------------------------------------------------------
// Test 4: GROUP BY with SUM, MIN, MAX
// -------------------------------------------------------------------------

#[test]
fn hash_agg_group_by_sum_min_max() {
    let schema = schema_group_val();
    // group=1 has val: 10, 30; group=2 has val: 20
    let scan = MemTableScan::new(
        schema,
        vec![make_batch_i32_i64(&[(1, 10), (2, 20), (1, 30)])],
    );
    let out_schema = Schema::new([
        Field::required("group", DataType::Int32),
        Field::required("total", DataType::Int64),
        Field::required("mn", DataType::Int64),
        Field::required("mx", DataType::Int64),
    ])
    .expect("schema ok");
    let mut op = HashAggregate::new(
        Box::new(scan),
        vec![col("group", 0, DataType::Int32)],
        vec![sum_agg("val", 1), min_agg("val", 1), max_agg("val", 1)],
        out_schema,
    );
    let mut rows = drain_all(&mut op);
    // Sort by group key for deterministic comparison.
    rows.sort_by_key(|r| match &r[0] {
        Value::Int32(v) => *v,
        _ => i32::MAX,
    });
    assert_eq!(rows.len(), 2);
    // group=1: sum=40 (10+30), min=10, max=30
    assert_eq!(rows[0][0], Value::Int32(1));
    assert_eq!(rows[0][1], Value::Int64(40));
    assert_eq!(rows[0][2], Value::Int64(10));
    assert_eq!(rows[0][3], Value::Int64(30));
    // group=2: sum=20, min=20, max=20
    assert_eq!(rows[1][0], Value::Int32(2));
    assert_eq!(rows[1][1], Value::Int64(20));
}

#[test]
fn hash_agg_group_key_eval_error_propagates() {
    let scan = MemTableScan::new(
        schema_group_val(),
        vec![make_batch_i32_i64(&[(1, 10), (2, 20)])],
    );
    let out_schema = Schema::new([
        Field::required("group", DataType::Int32),
        Field::required("cnt", DataType::Int64),
    ])
    .expect("schema ok");
    let mut op = HashAggregate::new(
        Box::new(scan),
        vec![divide_i32_by_zero("group", 0)],
        vec![count_star_agg()],
        out_schema,
    );

    let err = op.next_batch().expect_err("group key division must error");
    assert!(
        err.to_string().contains("division by zero"),
        "unexpected error: {err}"
    );
}

#[test]
fn hash_agg_arg_eval_error_propagates() {
    let scan = MemTableScan::new(schema_group_val(), vec![make_batch_i32_i64(&[(1, 10)])]);
    let out_schema = Schema::new([Field::required("total", DataType::Int64)]).expect("schema ok");
    let agg = LogicalAggregateExpr {
        func: AggregateFunc::Sum,
        arg: Some(divide_i64_by_zero("val", 1)),
        direct_arg: None,
        order_by: None,
        distinct: false,
        output_name: "total".into(),
        data_type: DataType::Int64,
    };
    let mut op = HashAggregate::new(Box::new(scan), vec![], vec![agg], out_schema);

    let err = op
        .next_batch()
        .expect_err("aggregate arg division must error");
    assert!(
        err.to_string().contains("division by zero"),
        "unexpected error: {err}"
    );
}

/// `STRING_AGG(label, ',')` over the hash-aggregate path must join
/// parts with the bound delimiter, skip NULLs, and not emit a
/// trailing delimiter for single-row groups. Regression for the bug
/// where the delimiter was dropped and parts joined with `""`.
#[test]
fn hash_agg_string_agg_joins_with_delimiter() {
    let input_schema = Schema::new([
        Field::required("group", DataType::Int32),
        Field::nullable("label", DataType::Text { max_len: None }),
    ])
    .expect("schema ok");
    let mut valid = Bitmap::new(6, true);
    valid.set(4, false);
    let batch = Batch::new([
        Column::Int32(NumericColumn::from_data(vec![1, 1, 1, 2, 3, 3])),
        Column::Utf8(
            StringColumn::with_nulls(["a", "b", "c", "x", "", "y"].map(str::to_owned), valid)
                .expect("utf8"),
        ),
    ])
    .expect("batch ok");
    let scan = MemTableScan::new(input_schema, vec![batch]);
    let out_schema = Schema::new([
        Field::required("group", DataType::Int32),
        Field::nullable("joined", DataType::Text { max_len: None }),
    ])
    .expect("schema ok");
    let aggs = vec![LogicalAggregateExpr {
        func: AggregateFunc::StringAgg,
        arg: Some(col("label", 1, DataType::Text { max_len: None })),
        direct_arg: Some(ScalarExpr::Literal {
            value: Value::Text(",".to_owned()),
            data_type: DataType::Text { max_len: None },
        }),
        order_by: None,
        distinct: false,
        output_name: "joined".into(),
        data_type: DataType::Text { max_len: None },
    }];
    let mut op = HashAggregate::new(
        Box::new(scan),
        vec![col("group", 0, DataType::Int32)],
        aggs,
        out_schema,
    );
    let mut rows = drain_all(&mut op);
    rows.sort_by_key(|r| match &r[0] {
        Value::Int32(v) => *v,
        _ => i32::MAX,
    });

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][1], Value::Text("a,b,c".to_owned())); // multi-row join
    assert_eq!(rows[1][1], Value::Text("x".to_owned())); // single row, no delimiter
    assert_eq!(rows[2][1], Value::Text("y".to_owned())); // NULL skipped
}

/// `STRING_AGG(DISTINCT label, '-')` must dedupe inputs yet still join
/// the surviving parts with the delimiter — exercising the `Distinct`
/// wrapper around the separator-carrying state.
#[test]
fn hash_agg_string_agg_distinct_uses_delimiter() {
    let input_schema = Schema::new([
        Field::required("group", DataType::Int32),
        Field::required("label", DataType::Text { max_len: None }),
    ])
    .expect("schema ok");
    let batch = Batch::new([
        Column::Int32(NumericColumn::from_data(vec![1, 1, 1, 1])),
        Column::Utf8(StringColumn::from_data(
            ["a", "b", "a", "b"].map(str::to_owned),
        )),
    ])
    .expect("batch ok");
    let scan = MemTableScan::new(input_schema, vec![batch]);
    let out_schema = Schema::new([
        Field::required("group", DataType::Int32),
        Field::nullable("joined", DataType::Text { max_len: None }),
    ])
    .expect("schema ok");
    let aggs = vec![LogicalAggregateExpr {
        func: AggregateFunc::StringAgg,
        arg: Some(col("label", 1, DataType::Text { max_len: None })),
        direct_arg: Some(ScalarExpr::Literal {
            value: Value::Text("-".to_owned()),
            data_type: DataType::Text { max_len: None },
        }),
        order_by: None,
        distinct: true,
        output_name: "joined".into(),
        data_type: DataType::Text { max_len: None },
    }];
    let mut op = HashAggregate::new(
        Box::new(scan),
        vec![col("group", 0, DataType::Int32)],
        aggs,
        out_schema,
    );
    let rows = drain_all(&mut op);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1], Value::Text("a-b".to_owned()));
}

#[test]
fn hash_agg_percentile_fraction_eval_error_propagates() {
    let scan = MemTableScan::new(schema_group_val(), vec![make_batch_i32_i64(&[(1, 10)])]);
    let order_expr = col("val", 1, DataType::Int64);
    let out_schema = Schema::new([Field::required("p", DataType::Float64)]).expect("schema ok");
    let agg = LogicalAggregateExpr {
        func: AggregateFunc::PercentileCont,
        arg: Some(order_expr.clone()),
        direct_arg: Some(divide_i32_by_zero("group", 0)),
        order_by: Some(SortKey {
            expr: order_expr,
            asc: true,
            nulls_first: false,
        }),
        distinct: false,
        output_name: "p".into(),
        data_type: DataType::Float64,
    };
    let mut op = HashAggregate::new(Box::new(scan), vec![], vec![agg], out_schema);

    let err = op
        .next_batch()
        .expect_err("percentile fraction division must error");
    assert!(
        err.to_string().contains("division by zero"),
        "unexpected error: {err}"
    );
}

#[test]
fn hash_agg_percentile_order_eval_error_propagates() {
    let scan = MemTableScan::new(schema_group_val(), vec![make_batch_i32_i64(&[(1, 10)])]);
    let order_expr = divide_i64_by_zero("val", 1);
    let out_schema = Schema::new([Field::required("p", DataType::Float64)]).expect("schema ok");
    let agg = LogicalAggregateExpr {
        func: AggregateFunc::PercentileCont,
        arg: Some(col("val", 1, DataType::Int64)),
        direct_arg: Some(lit_f64(0.5)),
        order_by: Some(SortKey {
            expr: order_expr,
            asc: true,
            nulls_first: false,
        }),
        distinct: false,
        output_name: "p".into(),
        data_type: DataType::Float64,
    };
    let mut op = HashAggregate::new(Box::new(scan), vec![], vec![agg], out_schema);

    let err = op
        .next_batch()
        .expect_err("percentile order division must error");
    assert!(
        err.to_string().contains("division by zero"),
        "unexpected error: {err}"
    );
}

#[test]
fn hash_agg_percentile_cont_orders_any_nan_after_finite_values() {
    let state = AggState::PercentileCont {
        values: vec![f64::from_bits(0xfff8_0000_0000_0000), 1.0, 2.0],
        fraction: Some(0.0),
        asc: true,
    };

    let Value::Float64(value) = finalise(&state).expect("percentile finalises") else {
        panic!("expected percentile value");
    };
    assert_eq!(value, 1.0);
}

#[test]
fn hash_agg_percentile_disc_rejects_unsupported_order_value() {
    let scan = MemTableScan::new(
        schema_group_val(),
        vec![make_batch_i32_i64(&[(1, 10), (2, 20)])],
    );
    let array_expr = ScalarExpr::Literal {
        value: Value::Array {
            element_type: DataType::Int32,
            elements: vec![Value::Int32(1)],
        },
        data_type: DataType::Array(Box::new(DataType::Int32)),
    };
    let out_schema = Schema::new([Field::nullable(
        "p",
        DataType::Array(Box::new(DataType::Int32)),
    )])
    .expect("schema ok");
    let agg = LogicalAggregateExpr {
        func: AggregateFunc::PercentileDisc,
        arg: Some(array_expr.clone()),
        direct_arg: Some(lit_f64(0.5)),
        order_by: Some(SortKey {
            expr: array_expr,
            asc: true,
            nulls_first: false,
        }),
        distinct: false,
        output_name: "p".into(),
        data_type: DataType::Array(Box::new(DataType::Int32)),
    };
    let mut op = HashAggregate::new(Box::new(scan), vec![], vec![agg], out_schema);

    let err = op
        .next_batch()
        .expect_err("unsupported percentile_disc key must surface");
    assert!(
        err.to_string().contains("not orderable"),
        "unexpected error: {err}"
    );
}

#[test]
fn hash_agg_spills_grouped_input_when_work_mem_is_too_small() {
    let schema = schema_group_val();
    let scan = MemTableScan::new(
        schema,
        vec![
            make_batch_i32_i64(&[(1, 10), (2, 20), (1, 7)]),
            make_batch_i32_i64(&[(3, 30), (2, 5), (3, 4)]),
        ],
    );
    let out_schema = Schema::new([
        Field::required("group", DataType::Int32),
        Field::required("total", DataType::Int64),
    ])
    .expect("schema ok");
    let mut op = HashAggregate::new(
        Box::new(scan),
        vec![col("group", 0, DataType::Int32)],
        vec![sum_agg("val", 1)],
        out_schema,
    )
    .with_work_mem_budget(std::sync::Arc::new(WorkMemBudget::new(1)));

    let mut rows = drain_all(&mut op);
    rows.sort_by_key(|row| match row[0] {
        Value::Int32(v) => v,
        _ => panic!("unexpected group key"),
    });

    assert_eq!(
        rows,
        vec![
            vec![Value::Int32(1), Value::Int64(17)],
            vec![Value::Int32(2), Value::Int64(25)],
            vec![Value::Int32(3), Value::Int64(34)],
        ]
    );
    assert!(
        op.spilled_to_disk(),
        "grouped hash aggregate must partition-spill"
    );
    let profile = op.spill_profile();
    assert!(profile.spills > 0);
    assert!(profile.bytes > 0);
    assert_eq!(op.io_bytes(), profile.bytes.saturating_mul(2));
    assert_eq!(op.profile_children().len(), 1);
}

#[test]
fn hash_agg_count_distinct_per_group() {
    let schema = schema_group_val();
    let scan = MemTableScan::new(
        schema,
        vec![make_batch_i32_i64(&[
            (1, 10),
            (1, 10),
            (1, 20),
            (2, 30),
            (2, 30),
        ])],
    );
    let out_schema = Schema::new([
        Field::required("group", DataType::Int32),
        Field::required("distinct_count", DataType::Int64),
    ])
    .expect("schema ok");
    let mut op = HashAggregate::new(
        Box::new(scan),
        vec![col("group", 0, DataType::Int32)],
        vec![count_distinct_agg("val", 1, DataType::Int64)],
        out_schema,
    );
    let mut rows = drain_all(&mut op);
    rows.sort_by_key(|r| match &r[0] {
        Value::Int32(v) => *v,
        _ => i32::MAX,
    });
    assert_eq!(rows[0], vec![Value::Int32(1), Value::Int64(2)]);
    assert_eq!(rows[1], vec![Value::Int32(2), Value::Int64(1)]);
}

// -------------------------------------------------------------------------
// Test 5: COUNT(expr) counts non-null values
// -------------------------------------------------------------------------

#[test]
fn hash_agg_count_expr_counts_non_null_values() {
    let schema =
        Schema::new([Field::nullable("v", DataType::Text { max_len: None })]).expect("schema ok");
    let scan = MemTableScan::new(
        schema,
        vec![
            Batch::new([Column::Utf8(StringColumn::from_data(vec![
                "a".to_string(),
                "b".to_string(),
                "c".to_string(),
            ]))])
            .expect("batch ok"),
        ],
    );
    let out_schema = Schema::new([Field::required("cnt", DataType::Int64)]).expect("schema ok");
    let count_expr_agg = LogicalAggregateExpr {
        func: AggregateFunc::Count,
        arg: Some(col("v", 0, DataType::Text { max_len: None })),
        direct_arg: None,
        order_by: None,
        distinct: false,
        output_name: "cnt".into(),
        data_type: DataType::Int64,
    };
    let mut op = HashAggregate::new(Box::new(scan), vec![], vec![count_expr_agg], out_schema);
    let rows = drain_all(&mut op);
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0][0],
        Value::Int64(3),
        "COUNT(v) counts all non-null values"
    );
}

#[test]
fn hash_agg_sum_single_int32_row_widens_to_int64() {
    let schema = Schema::new([Field::required("v", DataType::Int32)]).expect("schema ok");
    let scan = MemTableScan::new(
        schema,
        vec![Batch::new([Column::Int32(NumericColumn::from_data(vec![7]))]).expect("batch ok")],
    );
    let out_schema = Schema::new([Field::required("total", DataType::Int64)]).expect("schema ok");
    let sum_expr = LogicalAggregateExpr {
        func: AggregateFunc::Sum,
        arg: Some(col("v", 0, DataType::Int32)),
        direct_arg: None,
        order_by: None,
        distinct: false,
        output_name: "total".into(),
        data_type: DataType::Int64,
    };

    let mut op = HashAggregate::new(Box::new(scan), vec![], vec![sum_expr], out_schema);
    let rows = drain_all(&mut op);

    assert_eq!(rows, vec![vec![Value::Int64(7)]]);
}

#[test]
fn hash_agg_grouped_sum_keeps_null_only_groups_on_fast_path() {
    let schema = Schema::new([
        Field::required("group", DataType::Int32),
        Field::nullable("amount", DataType::Int64),
    ])
    .expect("schema ok");
    let mut amount_validity = Bitmap::new(3, true);
    amount_validity.set(1, false);
    let batch = Batch::new([
        Column::Int32(NumericColumn::from_data(vec![1, 2, 3])),
        Column::Int64(
            NumericColumn::with_nulls(vec![10, 0, 30], amount_validity).expect("amount column ok"),
        ),
    ])
    .expect("batch ok");
    let scan = MemTableScan::new(schema, vec![batch]);
    let out_schema = Schema::new([
        Field::required("group", DataType::Int32),
        Field::nullable("total", DataType::Int64),
    ])
    .expect("schema ok");

    let mut op = HashAggregate::new(
        Box::new(scan),
        vec![col("group", 0, DataType::Int32)],
        vec![sum_agg("amount", 1)],
        out_schema,
    );
    let mut rows = drain_all(&mut op);
    rows.sort_by_key(|row| match row[0] {
        Value::Int32(v) => v,
        _ => i32::MAX,
    });

    assert_eq!(
        rows,
        vec![
            vec![Value::Int32(1), Value::Int64(10)],
            vec![Value::Int32(2), Value::Null],
            vec![Value::Int32(3), Value::Int64(30)],
        ]
    );
}

#[test]
fn hash_agg_array_agg_returns_native_array() {
    let schema = schema_group_val();
    let scan = MemTableScan::new(
        schema,
        vec![make_batch_i32_i64(&[(1, 10), (1, 20), (1, 30)])],
    );
    let agg = LogicalAggregateExpr {
        func: AggregateFunc::ArrayAgg,
        arg: Some(col("val", 1, DataType::Int64)),
        direct_arg: None,
        order_by: None,
        distinct: false,
        output_name: "vals".into(),
        data_type: DataType::Array(Box::new(DataType::Int64)),
    };
    let out_schema = Schema::new([Field::required(
        "vals",
        DataType::Array(Box::new(DataType::Int64)),
    )])
    .expect("schema ok");
    let mut op = HashAggregate::new(Box::new(scan), vec![], vec![agg], out_schema);
    let rows = drain_all(&mut op);
    assert_eq!(
        rows,
        vec![vec![Value::Array {
            element_type: DataType::Int64,
            elements: vec![Value::Int64(10), Value::Int64(20), Value::Int64(30)]
        }]]
    );
}

#[test]
fn hash_agg_array_agg_keeps_null_elements() {
    // PostgreSQL's array_agg keeps NULL elements; rows (1),(NULL),(3)
    // must produce {1,NULL,3} (length 3, element_type from first
    // non-null). Exercises the hash accumulation path.
    let (schema, batch) = make_i64_batch(vec![1, 0, 3], Some(vec![true, false, true]));
    let scan = MemTableScan::new(schema, vec![batch]);
    let agg = LogicalAggregateExpr {
        func: AggregateFunc::ArrayAgg,
        arg: Some(col("val", 0, DataType::Int64)),
        direct_arg: None,
        order_by: None,
        distinct: false,
        output_name: "vals".into(),
        data_type: DataType::Array(Box::new(DataType::Int64)),
    };
    let out_schema = Schema::new([Field::required(
        "vals",
        DataType::Array(Box::new(DataType::Int64)),
    )])
    .expect("schema ok");
    let mut op = HashAggregate::new(Box::new(scan), vec![], vec![agg], out_schema);
    let rows = drain_all(&mut op);
    assert_eq!(
        rows,
        vec![vec![Value::Array {
            element_type: DataType::Int64,
            elements: vec![Value::Int64(1), Value::Null, Value::Int64(3)],
        }]]
    );
}

// -------------------------------------------------------------------------
// Test 6: multi-row group (duplicate hash keys handled correctly)
// -------------------------------------------------------------------------

#[test]
fn hash_agg_many_groups_with_duplicates() {
    let schema = schema_group_val();
    // 100 rows, 10 groups of 10 rows each.
    let row_data: Vec<(i32, i64)> = (0_i32..100).map(|i| (i % 10, i64::from(i))).collect();
    let scan = MemTableScan::new(schema, vec![make_batch_i32_i64(&row_data)]);
    let out_schema = Schema::new([
        Field::required("group", DataType::Int32),
        Field::required("cnt", DataType::Int64),
    ])
    .expect("schema ok");
    let mut op = HashAggregate::new(
        Box::new(scan),
        vec![col("group", 0, DataType::Int32)],
        vec![count_star_agg()],
        out_schema,
    );
    let rows = drain_all(&mut op);
    assert_eq!(rows.len(), 10, "expected 10 groups");
    for row in &rows {
        assert_eq!(row[1], Value::Int64(10), "each group has 10 rows");
    }
}
