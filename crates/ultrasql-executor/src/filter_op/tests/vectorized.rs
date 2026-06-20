//! Vectorised fast-path tests: SIMD comparison kernels must agree
//! row-for-row with the scalar fallback, honour SQL NULL
//! semantics, and decline shapes the kernels do not cover.

use super::*;

// ---- vectorised fast-path tests ----

fn schema_x_i64() -> Schema {
    Schema::new([Field::required("x", DataType::Int64)]).expect("schema ok")
}

fn batch_i64(data: Vec<i64>) -> Batch {
    Batch::new([Column::Int64(NumericColumn::from_data(data))]).expect("batch ok")
}

/// 4096-row Int64 batch with `x > lit`: vectorised output must
/// agree row-for-row with a naive scalar reference.
#[test]
fn vectorized_gt_i64_matches_scalar() {
    let n = 4096_usize;
    let threshold = 1_000_000_i64;
    let data: Vec<i64> = (0..n)
        .map(|i| i64::try_from(i).expect("test index fits in i64") * 1_000 - 500_000)
        .collect();

    let pred = ScalarExpr::Binary {
        op: BinaryOp::Gt,
        left: Box::new(ScalarExpr::Column {
            name: "x".into(),
            index: 0,
            data_type: DataType::Int64,
        }),
        right: Box::new(ScalarExpr::Literal {
            value: Value::Int64(threshold),
            data_type: DataType::Int64,
        }),
        data_type: DataType::Bool,
    };
    let scan = MemTableScan::new(schema_x_i64(), vec![batch_i64(data.clone())]);
    let mut filter = Filter::new(Box::new(scan), pred);

    let out = filter.next_batch().unwrap().unwrap();
    let got: Vec<i64> = match &out.columns()[0] {
        Column::Int64(c) => c.data().to_vec(),
        other => panic!("unexpected column type: {other:?}"),
    };
    let want: Vec<i64> = data.iter().filter(|&&v| v > threshold).copied().collect();
    assert_eq!(got, want);
    assert!(filter.next_batch().unwrap().is_none());
}

/// Vectorised `col = lit` over an Int32 column whose validity
/// bitmap marks some rows NULL. NULL rows must NOT appear in the
/// output — SQL `WHERE` treats `UNKNOWN` as `false`, and the
/// kernel honours that by AND-ing the validity bitmap into the
/// data-compare mask.
#[test]
fn vectorized_eq_i32_with_nulls() {
    let len = 8_usize;
    let data: Vec<i32> = vec![42, 999, 42, 999, 42, 999, 42, 7];
    let mut validity = Bitmap::new(len, true);
    for &null_row in &[1_usize, 3, 5] {
        validity.set(null_row, false);
    }
    let column = NumericColumn::with_nulls(data, validity).expect("valid column");
    let batch = Batch::new([Column::Int32(column)]).expect("batch ok");

    let schema = Schema::new([Field::required("k", DataType::Int32)]).expect("schema ok");
    let pred = ScalarExpr::Binary {
        op: BinaryOp::Eq,
        left: Box::new(ScalarExpr::Column {
            name: "k".into(),
            index: 0,
            data_type: DataType::Int32,
        }),
        right: Box::new(ScalarExpr::Literal {
            value: Value::Int32(42),
            data_type: DataType::Int32,
        }),
        data_type: DataType::Bool,
    };
    let scan = MemTableScan::new(schema, vec![batch]);
    let mut filter = Filter::new(Box::new(scan), pred);

    let out = filter.next_batch().unwrap().unwrap();
    let got: Vec<i32> = match &out.columns()[0] {
        Column::Int32(c) => c.data().to_vec(),
        other => panic!("unexpected column type: {other:?}"),
    };
    // Rows {0, 2, 4, 6}: value 42, non-null. Rows 1/3/5: value 999
    // and NULL (validity = 0) — must be dropped. Row 7: 7.
    assert_eq!(got, vec![42, 42, 42, 42]);
}

/// TPC-H Q21 style predicate: `l_receiptdate > l_commitdate`.
/// Date columns are stored as Int32 day offsets, so this must use
/// the vectorised column-vs-column path instead of row decoding.
#[test]
fn vectorized_column_column_i32_date_gt() {
    let commit_dates = vec![10_i32, 20, 30, 40, 50, 60];
    let receipt_dates = vec![11_i32, 19, 30, 99, 49, 61];
    let batch = Batch::new([
        Column::Int32(NumericColumn::from_data(commit_dates)),
        Column::Int32(NumericColumn::from_data(receipt_dates)),
    ])
    .expect("batch ok");
    let schema = Schema::new([
        Field::required("l_commitdate", DataType::Date),
        Field::required("l_receiptdate", DataType::Date),
    ])
    .expect("schema ok");
    let pred = ScalarExpr::Binary {
        op: BinaryOp::Gt,
        left: Box::new(ScalarExpr::Column {
            name: "l_receiptdate".into(),
            index: 1,
            data_type: DataType::Date,
        }),
        right: Box::new(ScalarExpr::Column {
            name: "l_commitdate".into(),
            index: 0,
            data_type: DataType::Date,
        }),
        data_type: DataType::Bool,
    };
    let scan = MemTableScan::new(schema, vec![batch]);
    let mut filter = Filter::new(Box::new(scan), pred);

    let out = filter.next_batch().unwrap().unwrap();
    let got: Vec<(i32, i32)> = match (&out.columns()[0], &out.columns()[1]) {
        (Column::Int32(commit), Column::Int32(receipt)) => commit
            .data()
            .iter()
            .copied()
            .zip(receipt.data().iter().copied())
            .collect(),
        other => panic!("unexpected column types: {other:?}"),
    };
    assert_eq!(got, vec![(10, 11), (40, 99), (60, 61)]);
    assert!(filter.next_batch().unwrap().is_none());
}

/// Date literals use the same raw Int32 ordering as stored date
/// columns, so `col >= DATE '...'` should stay on the vectorized
/// column-vs-literal path.
#[test]
fn vectorized_date_literal_i32_matches_scalar() {
    let data = vec![10_i32, 20, 30, 40, 50, 60];
    let batch =
        Batch::new([Column::Int32(NumericColumn::from_data(data.clone()))]).expect("batch ok");
    let schema =
        Schema::new([Field::required("o_orderdate", DataType::Date)]).expect("schema ok");
    let threshold = 40_i32;
    let pred = ScalarExpr::Binary {
        op: BinaryOp::GtEq,
        left: Box::new(ScalarExpr::Column {
            name: "o_orderdate".into(),
            index: 0,
            data_type: DataType::Date,
        }),
        right: Box::new(ScalarExpr::Literal {
            value: Value::Date(threshold),
            data_type: DataType::Date,
        }),
        data_type: DataType::Bool,
    };
    let scan = MemTableScan::new(schema, vec![batch]);
    let mut filter = Filter::new(Box::new(scan), pred);

    let out = filter.next_batch().unwrap().unwrap();
    let got: Vec<i32> = match &out.columns()[0] {
        Column::Int32(c) => c.data().to_vec(),
        other => panic!("unexpected column type: {other:?}"),
    };
    assert_eq!(got, vec![40, 50, 60]);
    assert!(filter.next_batch().unwrap().is_none());
}

/// Timestamp literals use the raw Int64 ordering of timestamp
/// columns, so simple range predicates stay on the vectorized fast
/// path.
#[test]
fn vectorized_timestamp_literal_i64_matches_scalar() {
    let data = vec![10_i64, 20, 30, 40, 50, 60];
    let batch =
        Batch::new([Column::Int64(NumericColumn::from_data(data.clone()))]).expect("batch ok");
    let schema = Schema::new([Field::required("ts", DataType::Timestamp)]).expect("schema ok");
    let threshold = 25_i64;
    let pred = ScalarExpr::Binary {
        op: BinaryOp::Gt,
        left: Box::new(ScalarExpr::Column {
            name: "ts".into(),
            index: 0,
            data_type: DataType::Timestamp,
        }),
        right: Box::new(ScalarExpr::Literal {
            value: Value::Timestamp(threshold),
            data_type: DataType::Timestamp,
        }),
        data_type: DataType::Bool,
    };
    let scan = MemTableScan::new(schema, vec![batch]);
    let mut filter = Filter::new(Box::new(scan), pred);

    let out = filter.next_batch().unwrap().unwrap();
    let got: Vec<i64> = match &out.columns()[0] {
        Column::Int64(c) => c.data().to_vec(),
        other => panic!("unexpected column type: {other:?}"),
    };
    assert_eq!(got, vec![30, 40, 50, 60]);
    assert!(filter.next_batch().unwrap().is_none());
}

/// A conjunction of simple date comparisons should stay on the
/// vectorized path instead of forcing a full scalar fallback.
#[test]
fn vectorized_and_of_date_range_predicates_matches_scalar() {
    let data = vec![10_i32, 20, 30, 40, 50, 60];
    let batch =
        Batch::new([Column::Int32(NumericColumn::from_data(data.clone()))]).expect("batch ok");
    let schema =
        Schema::new([Field::required("o_orderdate", DataType::Date)]).expect("schema ok");
    let lower = ScalarExpr::Binary {
        op: BinaryOp::GtEq,
        left: Box::new(ScalarExpr::Column {
            name: "o_orderdate".into(),
            index: 0,
            data_type: DataType::Date,
        }),
        right: Box::new(ScalarExpr::Literal {
            value: Value::Date(20),
            data_type: DataType::Date,
        }),
        data_type: DataType::Bool,
    };
    let upper = ScalarExpr::Binary {
        op: BinaryOp::Lt,
        left: Box::new(ScalarExpr::Column {
            name: "o_orderdate".into(),
            index: 0,
            data_type: DataType::Date,
        }),
        right: Box::new(ScalarExpr::Literal {
            value: Value::Date(50),
            data_type: DataType::Date,
        }),
        data_type: DataType::Bool,
    };
    let pred = ScalarExpr::Binary {
        op: BinaryOp::And,
        left: Box::new(lower),
        right: Box::new(upper),
        data_type: DataType::Bool,
    };
    let scan = MemTableScan::new(schema, vec![batch]);
    let mut filter = Filter::new(Box::new(scan), pred);

    let out = filter.next_batch().unwrap().unwrap();
    let got: Vec<i32> = match &out.columns()[0] {
        Column::Int32(c) => c.data().to_vec(),
        other => panic!("unexpected column type: {other:?}"),
    };
    assert_eq!(got, vec![20, 30, 40]);
    assert!(filter.next_batch().unwrap().is_none());
}

/// Column-vs-column comparison must apply SQL NULL semantics:
/// NULL on either side means UNKNOWN and does not pass WHERE.
#[test]
fn vectorized_column_column_i32_merges_nulls() {
    let len = 5_usize;
    let left_values = vec![5_i32, 7, 9, 11, 13];
    let right_values = vec![1_i32, 2, 3, 20, 4];
    let mut left_validity = Bitmap::new(len, true);
    let mut right_validity = Bitmap::new(len, true);
    left_validity.set(1, false);
    right_validity.set(2, false);
    let left =
        NumericColumn::with_nulls(left_values, left_validity).expect("valid left column");
    let right =
        NumericColumn::with_nulls(right_values, right_validity).expect("valid right column");
    let batch = Batch::new([Column::Int32(left), Column::Int32(right)]).expect("batch ok");
    let schema = Schema::new([
        Field::required("left", DataType::Int32),
        Field::required("right", DataType::Int32),
    ])
    .expect("schema ok");
    let pred = ScalarExpr::Binary {
        op: BinaryOp::Gt,
        left: Box::new(ScalarExpr::Column {
            name: "left".into(),
            index: 0,
            data_type: DataType::Int32,
        }),
        right: Box::new(ScalarExpr::Column {
            name: "right".into(),
            index: 1,
            data_type: DataType::Int32,
        }),
        data_type: DataType::Bool,
    };
    let scan = MemTableScan::new(schema, vec![batch]);
    let mut filter = Filter::new(Box::new(scan), pred);

    let out = filter.next_batch().unwrap().unwrap();
    let got: Vec<i32> = match &out.columns()[0] {
        Column::Int32(c) => c.data().to_vec(),
        other => panic!("unexpected column type: {other:?}"),
    };
    // Rows 0 and 4 pass. Row 1 has NULL left, row 2 NULL right,
    // row 3 is 11 > 20 false.
    assert_eq!(got, vec![5, 13]);
}

/// Decimal columns store as Int64, but different scales do not
/// share raw integer ordering. The column-vs-column fast path must
/// decline so Eval can rescale before compare.
#[test]
fn decimal_column_column_different_scales_falls_back() {
    let left_values = vec![10_000_i64, 1_000, 500];
    let right_values = vec![200_000_i64, 2_000, 600];
    let batch = Batch::new([
        Column::Int64(NumericColumn::from_data(left_values)),
        Column::Int64(NumericColumn::from_data(right_values)),
    ])
    .expect("batch ok");
    let left_type = DataType::Decimal {
        precision: Some(12),
        scale: Some(2),
    };
    let right_type = DataType::Decimal {
        precision: Some(12),
        scale: Some(4),
    };
    let schema = Schema::new([
        Field::required("left_dec", left_type.clone()),
        Field::required("right_dec", right_type.clone()),
    ])
    .expect("schema ok");
    let pred = ScalarExpr::Binary {
        op: BinaryOp::Gt,
        left: Box::new(ScalarExpr::Column {
            name: "left_dec".into(),
            index: 0,
            data_type: left_type,
        }),
        right: Box::new(ScalarExpr::Column {
            name: "right_dec".into(),
            index: 1,
            data_type: right_type,
        }),
        data_type: DataType::Bool,
    };
    let scan = MemTableScan::new(schema, vec![batch]);
    let mut filter = Filter::new(Box::new(scan), pred);

    let out = filter.next_batch().unwrap().unwrap();
    let got: Vec<i64> = match &out.columns()[0] {
        Column::Int64(c) => c.data().to_vec(),
        other => panic!("unexpected column type: {other:?}"),
    };
    // Logical values: 100.00 > 20.0000, 10.00 > 0.2000,
    // 5.00 > 0.0600. Raw Int64 ordering would incorrectly drop
    // row 0 (10000 < 200000).
    assert_eq!(got, vec![10_000, 1_000, 500]);
}

/// `col + 1 > 5` does not match the col-op-literal shape (LHS is a
/// `Binary(Add, ...)`, not a `Column`). Fast path must decline;
/// the scalar fallback must produce the same answer.
#[test]
fn non_fast_path_falls_back() {
    let data: Vec<i32> = vec![3, 4, 5, 6, 7];
    let batch = Batch::new([Column::Int32(NumericColumn::from_data(data))]).expect("batch ok");
    let schema = Schema::new([Field::required("x", DataType::Int32)]).expect("schema ok");

    // (x + 1) > 5  → keeps rows where x > 4, i.e. {5, 6, 7}.
    let lhs = ScalarExpr::Binary {
        op: BinaryOp::Add,
        left: Box::new(ScalarExpr::Column {
            name: "x".into(),
            index: 0,
            data_type: DataType::Int32,
        }),
        right: Box::new(ScalarExpr::Literal {
            value: Value::Int32(1),
            data_type: DataType::Int32,
        }),
        data_type: DataType::Int32,
    };
    let pred = ScalarExpr::Binary {
        op: BinaryOp::Gt,
        left: Box::new(lhs),
        right: Box::new(ScalarExpr::Literal {
            value: Value::Int32(5),
            data_type: DataType::Int32,
        }),
        data_type: DataType::Bool,
    };
    let scan = MemTableScan::new(schema, vec![batch]);
    let mut filter = Filter::new(Box::new(scan), pred);

    let out = filter.next_batch().unwrap().unwrap();
    let got: Vec<i32> = match &out.columns()[0] {
        Column::Int32(c) => c.data().to_vec(),
        other => panic!("unexpected column type: {other:?}"),
    };
    assert_eq!(got, vec![5, 6, 7]);
}

/// `100 > col` is the swapped-operand variant: the matcher must
/// flip the operator so the kernel sees `col < 100`.
#[test]
fn vectorized_literal_on_left_is_flipped() {
    let data: Vec<i64> = (0..200_i64).collect();
    let schema = schema_x_i64();
    let pred = ScalarExpr::Binary {
        op: BinaryOp::Gt,
        left: Box::new(ScalarExpr::Literal {
            value: Value::Int64(100),
            data_type: DataType::Int64,
        }),
        right: Box::new(ScalarExpr::Column {
            name: "x".into(),
            index: 0,
            data_type: DataType::Int64,
        }),
        data_type: DataType::Bool,
    };
    let scan = MemTableScan::new(schema, vec![batch_i64(data.clone())]);
    let mut filter = Filter::new(Box::new(scan), pred);

    let out = filter.next_batch().unwrap().unwrap();
    let got: Vec<i64> = match &out.columns()[0] {
        Column::Int64(c) => c.data().to_vec(),
        other => panic!("unexpected column type: {other:?}"),
    };
    let want: Vec<i64> = data.iter().copied().filter(|&v| v < 100).collect();
    assert_eq!(got, want);
}
