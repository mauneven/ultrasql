//! Tests for the [`build_batch`](super::super::build_batch) legacy
//! conversion path and the column-cache helper functions.

use std::sync::Arc;

use ultrasql_core::{
    BitString, DataType, Field, GeometryType, GeometryValue, Lsn, NetworkValue, Oid, RangeType,
    RangeValue, Schema, SparseVector, Value,
};
use ultrasql_vec::bitmap::Bitmap;
use ultrasql_vec::column::{BoolColumn, Column, NumericColumn};

use super::schema_i32_only;
use crate::ExecError;
use crate::seq_scan::build_batch;
use crate::seq_scan::cache::{schema_all_fixed_numeric, slice_column};
use crate::seq_scan::operator::payload_prefix;

#[test]
fn build_batch_covers_sql_storage_families_and_null_bitmaps() {
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
        Field::nullable("n", DataType::Null),
        Field::nullable("b", DataType::Bool),
        Field::nullable("i2", DataType::Int16),
        Field::nullable("i4", DataType::Int32),
        Field::nullable("i8", DataType::Int64),
        Field::nullable("oid", DataType::Oid),
        Field::nullable("regclass", DataType::RegClass),
        Field::nullable("regtype", DataType::RegType),
        Field::nullable("f4", DataType::Float32),
        Field::nullable("f8", DataType::Float64),
        Field::nullable("txt", DataType::Text { max_len: None }),
        Field::nullable("enum", enum_ty),
        Field::nullable("composite", composite_ty),
        Field::nullable("char", DataType::Char { len: Some(4) }),
        Field::nullable("bit", DataType::Bit { len: Some(4) }),
        Field::nullable("varbit", DataType::VarBit { max_len: Some(8) }),
        Field::nullable("inet", DataType::Inet),
        Field::nullable("cidr", DataType::Cidr),
        Field::nullable("mac", DataType::MacAddr),
        Field::nullable("mac8", DataType::MacAddr8),
        Field::nullable("json", DataType::Json),
        Field::nullable("jsonb", DataType::Jsonb),
        Field::nullable("xml", DataType::Xml),
        Field::nullable("lsn", DataType::PgLsn),
        Field::nullable("vec", DataType::Vector { dims: Some(2) }),
        Field::nullable("half", DataType::HalfVec { dims: Some(2) }),
        Field::nullable("sparse", DataType::SparseVec { dims: Some(5) }),
        Field::nullable("bitvec", DataType::BitVec { dims: Some(4) }),
        Field::nullable("range", DataType::Range(RangeType::Int4)),
        Field::nullable("array", DataType::Array(Box::new(DataType::Int32))),
        Field::nullable("geom", DataType::Geometry(GeometryType::Box)),
        Field::nullable("uuid", DataType::Uuid),
        Field::nullable("bytea", DataType::Bytea),
        Field::nullable("date", DataType::Date),
        Field::nullable(
            "dec",
            DataType::Decimal {
                precision: None,
                scale: Some(2),
            },
        ),
        Field::nullable(
            "dyn_dec",
            DataType::Decimal {
                precision: None,
                scale: None,
            },
        ),
        Field::nullable("money", DataType::Money),
        Field::nullable("ts", DataType::Timestamp),
        Field::nullable("tstz", DataType::TimestampTz),
        Field::nullable("time", DataType::Time),
        Field::nullable("timetz", DataType::TimeTz),
        Field::nullable("interval", DataType::Interval),
    ])
    .expect("schema");
    let row = vec![
        Value::Null,
        Value::Bool(true),
        Value::Int16(7),
        Value::Int32(8),
        Value::Int64(9),
        Value::Oid(Oid::new(10)),
        Value::RegClass(Oid::new(11)),
        Value::RegType(Oid::new(12)),
        Value::Float32(1.5),
        Value::Float64(2.5),
        Value::Text("txt".into()),
        Value::Text("ok".into()),
        Value::Text("(1,2)".into()),
        Value::Text("xy".into()),
        Value::BitString(BitString::parse("1010").expect("bit")),
        Value::BitString(BitString::parse("101011").expect("varbit")),
        Value::Network(NetworkValue::parse_for_type(&DataType::Inet, "127.0.0.1").expect("inet")),
        Value::Network(NetworkValue::parse_for_type(&DataType::Cidr, "10.0.0.0/24").expect("cidr")),
        Value::Network(
            NetworkValue::parse_for_type(&DataType::MacAddr, "08:00:2b:01:02:03").expect("mac"),
        ),
        Value::Network(
            NetworkValue::parse_for_type(&DataType::MacAddr8, "08:00:2b:ff:fe:01:02:03")
                .expect("mac8"),
        ),
        Value::Json(r#"{"a":1}"#.into()),
        Value::Jsonb(r#"{"a":1}"#.into()),
        Value::Xml("<r/>".into()),
        Value::PgLsn(Lsn::new(0x1_0000_0002)),
        Value::Vector(vec![1.0, 2.0]),
        Value::HalfVec(vec![3.0, 4.0]),
        Value::SparseVec(SparseVector {
            dims: 5,
            entries: vec![(1, 1.0), (3, 2.0)],
        }),
        Value::BitVec {
            dims: 4,
            bytes: vec![0b1010_0000],
        },
        Value::Range(RangeValue::parse(RangeType::Int4, "[1,4)").expect("range")),
        Value::Array {
            element_type: DataType::Int32,
            elements: vec![Value::Int32(1), Value::Null],
        },
        Value::Geometry(GeometryValue::parse(GeometryType::Box, "((0,0),(1,1))").expect("box")),
        Value::Uuid([7; 16]),
        Value::Bytea(vec![0xde, 0xad]),
        Value::Date(42),
        Value::Decimal {
            value: 1234,
            scale: 2,
        },
        Value::Decimal {
            value: 166_667,
            scale: 6,
        },
        Value::Money(5678),
        Value::Timestamp(111),
        Value::TimestampTz(222),
        Value::Time(333),
        Value::TimeTz {
            micros: 1_000,
            offset_seconds: -3_600,
        },
        Value::Interval {
            months: 2,
            days: 3,
            microseconds: 14_706_000_000,
        },
    ];
    let null_row = vec![Value::Null; schema.len()];
    let batch = build_batch(&[row, null_row], &schema).expect("batch");

    assert_eq!(batch.rows(), 2);
    assert_eq!(batch.width(), schema.len());
    match &batch.columns()[1] {
        Column::Bool(c) => assert!(c.nulls().is_some_and(|n| n.get(0) && !n.get(1))),
        other => panic!("expected bool column, got {other:?}"),
    }
    // Decimal columns now materialise as decimal text (i128-backed,
    // lossless) rather than a fixed-width Int64 batch column.
    assert_eq!(batch.columns()[34].text_value(0), Some("12.34"));
    assert_eq!(batch.columns()[35].text_value(0), Some("0.166667"));
    assert_eq!(batch.columns()[35].text_value(1), None);
    // Interval columns materialise as text (last column), mirroring the
    // streaming row-codec column builder; NULL stays NULL.
    let interval_col = batch.columns().len() - 1;
    assert_eq!(
        batch.columns()[interval_col].text_value(0),
        Some("2mon 3d 14706000000us")
    );
    assert_eq!(batch.columns()[interval_col].text_value(1), None);

    let bad = build_batch(&[vec![Value::Text("bad".into())]], &schema_i32_only())
        .expect_err("type mismatch");
    assert!(matches!(bad, ExecError::TypeMismatch(_)));
}

#[test]
fn cache_helpers_slice_numeric_columns_and_gate_supported_schemas() {
    let mut nulls = Bitmap::new(5, true);
    nulls.set(2, false);
    let int_col = Column::Int32(
        NumericColumn::with_nulls(vec![1, 2, 3, 4, 5], nulls.clone()).expect("int col"),
    );
    match slice_column(&int_col, 1, 4).expect("slice") {
        Column::Int32(c) => {
            assert_eq!(c.data(), &[2, 3, 4]);
            assert!(c.nulls().is_some_and(|n| n.get(0) && !n.get(1) && n.get(2)));
        }
        other => panic!("expected int32 column, got {other:?}"),
    }
    match slice_column(
        &Column::Float64(NumericColumn::from_data(vec![1.0, 2.0, 3.0, 4.0])),
        1,
        3,
    )
    .expect("slice")
    {
        Column::Float64(c) => assert_eq!(c.data(), &[2.0, 3.0]),
        other => panic!("expected float64 column, got {other:?}"),
    }
    let unsupported = slice_column(&Column::Bool(BoolColumn::from_data(vec![true])), 0, 1)
        .expect_err("bool cache slice must fail cleanly");
    assert!(matches!(unsupported, ExecError::TypeMismatch(_)));

    let numeric_schema = Schema::new([
        Field::required("i2", DataType::Int16),
        Field::required("oid", DataType::Oid),
        Field::required("f8", DataType::Float64),
    ])
    .expect("schema");
    let text_schema =
        Schema::new([Field::required("t", DataType::Text { max_len: None })]).expect("schema");
    assert!(schema_all_fixed_numeric(&numeric_schema));
    assert!(!schema_all_fixed_numeric(&text_schema));
    assert_eq!(payload_prefix(&[0, 1, 0xab, 0xff]), "0001abff");
}
