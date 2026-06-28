//! Column-codec tests: `select_column` compaction, empty-batch
//! materialisation, and `batch_to_rows` decoding of every SQL
//! storage family.

use super::*;

#[test]
fn select_column_covers_non_integer_column_families() {
    let mut mask = Bitmap::new(4, false);
    mask.set(0, true);
    mask.set(2, true);

    match select_column(
        &Column::Float32(NumericColumn::from_data(vec![1.0, 2.0, 3.0, 4.0])),
        &mask,
        2,
    )
    .expect("select float32")
    {
        Column::Float32(c) => assert_eq!(c.data(), &[1.0, 3.0]),
        other => panic!("expected float32 column, got {other:?}"),
    }
    match select_column(
        &Column::Float64(NumericColumn::from_data(vec![1.0, 2.0, 3.0, 4.0])),
        &mask,
        2,
    )
    .expect("select float64")
    {
        Column::Float64(c) => assert_eq!(c.data(), &[1.0, 3.0]),
        other => panic!("expected float64 column, got {other:?}"),
    }
    match select_column(
        &Column::Bool(BoolColumn::from_data(vec![true, false, true, false])),
        &mask,
        2,
    )
    .expect("select bool")
    {
        Column::Bool(c) => {
            assert!(c.value(0));
            assert!(c.value(1));
        }
        other => panic!("expected bool column, got {other:?}"),
    }
    let selected_text = select_column(
        &Column::Utf8(StringColumn::from_data(
            ["a", "b", "a", "c"].into_iter().map(str::to_owned),
        )),
        &mask,
        2,
    )
    .expect("select text");
    match &selected_text {
        Column::Utf8(_) | Column::DictionaryUtf8(_) => {
            assert_eq!(selected_text.text_value(0), Some("a"));
            assert_eq!(selected_text.text_value(1), Some("a"));
        }
        other => panic!("expected text column, got {other:?}"),
    }
}

#[test]
fn select_column_preserves_selected_nulls() {
    let mut mask = Bitmap::new(4, false);
    mask.set(0, true);
    mask.set(1, true);
    mask.set(3, true);

    let mut nulls = Bitmap::new(4, true);
    nulls.set(1, false);
    nulls.set(2, false);
    let source =
        NumericColumn::with_nulls(vec![10_i32, 0, 0, 9], nulls).expect("source nullable column");

    match select_column(&Column::Int32(source), &mask, 3).expect("select nullable int") {
        Column::Int32(selected) => {
            assert_eq!(selected.data(), &[10, 0, 9]);
            let selected_nulls = selected.nulls().expect("selected stays nullable");
            assert!(selected_nulls.get(0));
            assert!(!selected_nulls.get(1));
            assert!(selected_nulls.get(2));
        }
        other => panic!("expected int32 column, got {other:?}"),
    }
}

#[test]
fn build_empty_batch_preserves_declared_column_families() {
    let schema = Schema::new([
        Field::required("b", DataType::Bool),
        Field::required("i2", DataType::Int16),
        Field::required("i4", DataType::Int32),
        Field::required("d", DataType::Date),
        Field::required("i8", DataType::Int64),
        Field::required("oid", DataType::Oid),
        Field::required(
            "n",
            DataType::Decimal {
                precision: None,
                scale: Some(2),
            },
        ),
        Field::required("f4", DataType::Float32),
        Field::required("f8", DataType::Float64),
        Field::required("t", DataType::Text { max_len: None }),
        Field::required("j", DataType::Json),
        Field::required("v", DataType::Vector { dims: Some(2) }),
    ])
    .expect("schema");

    let batch = build_empty_batch(&schema).expect("empty batch");
    assert_eq!(batch.rows(), 0);
    assert_eq!(batch.width(), schema.len());
    assert!(matches!(batch.columns()[0], Column::Bool(_)));
    assert!(matches!(batch.columns()[1], Column::Int32(_)));
    assert!(matches!(batch.columns()[4], Column::Int64(_)));
    assert!(matches!(batch.columns()[7], Column::Float32(_)));
    assert!(matches!(batch.columns()[8], Column::Float64(_)));
    assert!(matches!(batch.columns()[9], Column::Utf8(_)));
}

#[test]
fn batch_to_rows_decodes_sql_storage_families_and_reports_bad_shapes() {
    let enum_ty = DataType::Enum {
        oid: Oid::new(100),
        name: Arc::from("mood"),
        labels: Arc::from(vec!["ok".to_owned()].into_boxed_slice()),
    };
    let composite_ty = DataType::Composite {
        oid: Oid::new(101),
        name: Arc::from("pair"),
        fields: Arc::from(
            vec![
                ("x".to_owned(), DataType::Int32),
                ("y".to_owned(), DataType::Int32),
            ]
            .into_boxed_slice(),
        ),
    };
    let schema = Schema::new([
        Field::required("i2", DataType::Int16),
        Field::required("i4", DataType::Int32),
        Field::required("i8", DataType::Int64),
        Field::required("money", DataType::Money),
        Field::required("oid", DataType::Oid),
        Field::required("regclass", DataType::RegClass),
        Field::required("regtype", DataType::RegType),
        Field::required("f4", DataType::Float32),
        Field::required("f8", DataType::Float64),
        Field::required("b", DataType::Bool),
        Field::required("txt", DataType::Text { max_len: None }),
        Field::required("enum", enum_ty),
        Field::required("composite", composite_ty),
        Field::required("char", DataType::Char { len: Some(4) }),
        Field::required("bit", DataType::Bit { len: Some(4) }),
        Field::required("varbit", DataType::VarBit { max_len: Some(8) }),
        Field::required("inet", DataType::Inet),
        Field::required("cidr", DataType::Cidr),
        Field::required("mac", DataType::MacAddr),
        Field::required("mac8", DataType::MacAddr8),
        Field::required("json", DataType::Json),
        Field::required("jsonb", DataType::Jsonb),
        Field::required("xml", DataType::Xml),
        Field::required("lsn", DataType::PgLsn),
        Field::required("vec", DataType::Vector { dims: Some(2) }),
        Field::required("half", DataType::HalfVec { dims: Some(2) }),
        Field::required("sparse", DataType::SparseVec { dims: Some(5) }),
        Field::required("bitvec", DataType::BitVec { dims: Some(4) }),
        Field::required("range", DataType::Range(RangeType::Int4)),
        Field::required("array", DataType::Array(Box::new(DataType::Int32))),
        Field::required("geom", DataType::Geometry(GeometryType::Box)),
        Field::required("uuid", DataType::Uuid),
        Field::required("bytea", DataType::Bytea),
        Field::required("date", DataType::Date),
        Field::required(
            "dec",
            DataType::Decimal {
                precision: None,
                scale: Some(2),
            },
        ),
        Field::required("ts", DataType::Timestamp),
        Field::required("tstz", DataType::TimestampTz),
        Field::required("time", DataType::Time),
        Field::required("timetz", DataType::TimeTz),
        Field::required("interval", DataType::Interval),
    ])
    .expect("schema");
    let timetz = pack_timetz(1_000, -3_600).expect("timetz");
    let batch = Batch::new([
        Column::Int32(NumericColumn::from_data(vec![7])),
        Column::Int32(NumericColumn::from_data(vec![8])),
        Column::Int64(NumericColumn::from_data(vec![9])),
        Column::Int64(NumericColumn::from_data(vec![10])),
        Column::Int64(NumericColumn::from_data(vec![11])),
        Column::Int64(NumericColumn::from_data(vec![12])),
        Column::Int64(NumericColumn::from_data(vec![13])),
        Column::Float32(NumericColumn::from_data(vec![1.5])),
        Column::Float64(NumericColumn::from_data(vec![2.5])),
        Column::Bool(BoolColumn::from_data(vec![true])),
        Column::Utf8(StringColumn::from_data(["txt".to_owned()])),
        Column::Utf8(StringColumn::from_data(["ok".to_owned()])),
        Column::Utf8(StringColumn::from_data(["(1,2)".to_owned()])),
        Column::Utf8(StringColumn::from_data(["xy  ".to_owned()])),
        Column::Utf8(StringColumn::from_data(["1010".to_owned()])),
        Column::Utf8(StringColumn::from_data(["101011".to_owned()])),
        Column::Utf8(StringColumn::from_data(["127.0.0.1".to_owned()])),
        Column::Utf8(StringColumn::from_data(["10.0.0.0/24".to_owned()])),
        Column::Utf8(StringColumn::from_data(["08:00:2b:01:02:03".to_owned()])),
        Column::Utf8(StringColumn::from_data([
            "08:00:2b:ff:fe:01:02:03".to_owned()
        ])),
        Column::Utf8(StringColumn::from_data([r#"{"a":1}"#.to_owned()])),
        Column::Utf8(StringColumn::from_data([r#"{"a":1}"#.to_owned()])),
        Column::Utf8(StringColumn::from_data(["<r/>".to_owned()])),
        Column::Utf8(StringColumn::from_data(["1/2".to_owned()])),
        Column::Utf8(StringColumn::from_data(["[1,2]".to_owned()])),
        Column::Utf8(StringColumn::from_data(["[3,4]".to_owned()])),
        Column::Utf8(StringColumn::from_data(["{1:1,3:2}/5".to_owned()])),
        Column::Utf8(StringColumn::from_data(["1010".to_owned()])),
        Column::Utf8(StringColumn::from_data(["[1,4)".to_owned()])),
        Column::Utf8(StringColumn::from_data(["{1,2,NULL}".to_owned()])),
        Column::Utf8(StringColumn::from_data(["((0,0),(1,1))".to_owned()])),
        Column::Utf8(StringColumn::from_data([
            "12345678-9abc-def0-1234-56789abcdef0".to_owned(),
        ])),
        Column::Utf8(StringColumn::from_data(["\\xdeadbeef".to_owned()])),
        Column::Int32(NumericColumn::from_data(vec![42])),
        // Decimal columns now materialise as decimal text (i128-backed).
        Column::Utf8(StringColumn::from_data(["12.34".to_owned()])),
        Column::Int64(NumericColumn::from_data(vec![111])),
        Column::Int64(NumericColumn::from_data(vec![222])),
        Column::Int64(NumericColumn::from_data(vec![333])),
        Column::Int64(NumericColumn::from_data(vec![timetz])),
        // Interval columns materialise as PostgreSQL-canonical text and must
        // round-trip back into a real `Value::Interval`.
        Column::Utf8(StringColumn::from_data(
            ["2 mons 3 days 04:05:06".to_owned()],
        )),
    ])
    .expect("batch");

    let rows = batch_to_rows(&batch, &schema).expect("rows");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Int16(7));
    assert_eq!(rows[0][4], Value::Oid(Oid::new(11)));
    assert_eq!(rows[0][33], Value::Date(42));
    assert_eq!(
        rows[0][34],
        Value::Decimal {
            value: 1234,
            scale: 2
        }
    );
    assert!(matches!(
        rows[0][38],
        Value::TimeTz {
            micros: 1_000,
            offset_seconds: -3_600
        }
    ));
    assert_eq!(
        rows[0][39],
        Value::Interval {
            months: 2,
            days: 3,
            microseconds: 14_706_000_000,
        }
    );

    let bad_cols =
        Batch::new([Column::Int64(NumericColumn::from_data(vec![1]))]).expect("bad batch");
    assert!(batch_to_rows(&bad_cols, &schema).is_err());
}
