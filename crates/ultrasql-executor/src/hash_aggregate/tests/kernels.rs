//! Hash-key and vectorised-kernel unit tests.

use super::*;

#[test]
fn group_key_hashes_supported_value_families_with_sql_key_semantics() {
    let values = vec![
        Value::Null,
        Value::Bool(true),
        Value::Int16(1),
        Value::Int32(2),
        Value::Int64(3),
        Value::Money(4),
        Value::Oid(Oid::new(5)),
        Value::RegClass(Oid::new(6)),
        Value::RegType(Oid::new(7)),
        Value::PgLsn(Lsn::new(8)),
        Value::Float32(f32::from_bits(0x7fc0_0001)),
        Value::Float64(f64::from_bits(0x7ff8_0000_0000_0001)),
        Value::Text("text".into()),
        Value::Char("bpchar  ".into()),
        Value::Json(r#"{"a":1}"#.into()),
        Value::Jsonb(r#"{"a":1}"#.into()),
        Value::Xml("<r/>".into()),
        Value::Bytea(vec![0, 1, 255]),
        Value::Timestamp(9),
        Value::TimestampTz(10),
        Value::Time(11),
        Value::TimeTz {
            micros: 12,
            offset_seconds: 3_600,
        },
        Value::Date(13),
        Value::Uuid([14; 16]),
        Value::Decimal {
            value: 15,
            scale: 2,
        },
        Value::Interval {
            months: 1,
            days: 2,
            microseconds: 3,
        },
        Value::Range(RangeValue::parse(RangeType::Int4, "[1,3)").expect("range")),
        Value::Geometry(GeometryValue::parse(GeometryType::Box, "((0,0),(1,1))").expect("box")),
        Value::Array {
            element_type: DataType::Int32,
            elements: vec![Value::Int32(1), Value::Null],
        },
        Value::Vector(vec![1.0, f32::NAN]),
        Value::HalfVec(vec![2.0, f32::NAN]),
        Value::SparseVec(SparseVector {
            dims: 4,
            entries: vec![(2, 1.5)],
        }),
        Value::BitVec {
            dims: 9,
            bytes: vec![0b1010_0000, 0b1000_0000],
        },
        Value::BitString(BitString::parse("10101").expect("bits")),
        Value::Network(NetworkValue::parse_for_type(&DataType::Inet, "127.0.0.1").expect("inet")),
        Value::Record(vec![("x".into(), Value::Int32(1))]),
    ];

    let key = GroupKey::from_values(values.clone());
    let clone = GroupKey::from_values(values.clone());
    let mut seen = HashSet::new();
    assert!(seen.insert(key));
    assert!(!seen.insert(clone));
    assert_eq!(GroupKey::from_values(values).into_values().len(), 36);

    assert_eq!(
        GroupKey::from_values(vec![Value::Char("a".into())]),
        GroupKey::from_values(vec![Value::Char("a   ".into())])
    );
    let mut decimal_seen = HashSet::new();
    assert!(
        decimal_seen.insert(GroupKey::from_values(vec![Value::Decimal {
            value: 10,
            scale: 1,
        }]))
    );
    assert!(
        !decimal_seen.insert(GroupKey::from_values(vec![Value::Decimal {
            value: 1,
            scale: 0,
        }]))
    );
    assert_eq!(
        GroupKey::from_values(vec![Value::TimeTz {
            micros: 3_600_000_000,
            offset_seconds: 3_600,
        }]),
        GroupKey::from_values(vec![Value::TimeTz {
            micros: 0,
            offset_seconds: 0,
        }])
    );
}

#[test]
fn vectorized_helper_paths_cover_counts_extrema_and_grouped_errors() {
    let mut validity = Bitmap::new(4, true);
    validity.set(1, false);
    let int32 = Column::Int32(
        NumericColumn::with_nulls(vec![1, 2, 3, 4], validity.clone()).expect("int32"),
    );
    let int64 = Column::Int64(
        NumericColumn::with_nulls(vec![10, 20, 30, 40], validity.clone()).expect("int64"),
    );
    let float32 = Column::Float32(
        NumericColumn::with_nulls(vec![1.0, f32::NAN, 3.0, 4.0], validity.clone())
            .expect("float32"),
    );
    let float64 = Column::Float64(
        NumericColumn::with_nulls(vec![1.0, 2.0, 3.0, 4.0], validity.clone()).expect("float64"),
    );
    let bools = Column::Bool(
        BoolColumn::with_nulls(vec![true, false, true, false], validity.clone()).expect("bool"),
    );
    let utf8 = Column::Utf8(
        StringColumn::with_nulls(
            ["a", "b", "c", "d"].into_iter().map(str::to_owned),
            validity.clone(),
        )
        .expect("utf8"),
    );
    let dict = Column::DictionaryUtf8(
        DictionaryColumn::from_strings([Some("a"), None, Some("c"), Some("d")])
            .expect("test dictionary should fit u32 codes"),
    );
    assert_eq!(column_non_null_count(&int32).expect("int32 count"), 3);
    assert_eq!(column_non_null_count(&int64).expect("int64 count"), 3);
    assert_eq!(column_non_null_count(&float32).expect("float32 count"), 3);
    assert_eq!(column_non_null_count(&float64).expect("float64 count"), 3);
    assert_eq!(column_non_null_count(&bools).expect("bool count"), 3);
    assert_eq!(column_non_null_count(&utf8).expect("utf8 count"), 3);
    assert_eq!(column_non_null_count(&dict).expect("dict count"), 3);

    let batch = Batch::new([
        int32.clone(),
        int64.clone(),
        float32.clone(),
        float64.clone(),
    ])
    .expect("batch");
    let aggregates = vec![
        count_star_agg(),
        LogicalAggregateExpr {
            func: AggregateFunc::Count,
            arg: Some(col("a", 0, DataType::Int32)),
            direct_arg: None,
            order_by: None,
            distinct: false,
            output_name: "cnt".into(),
            data_type: DataType::Int64,
        },
        sum_agg("b", 1),
        LogicalAggregateExpr {
            func: AggregateFunc::Avg,
            arg: Some(col("c", 2, DataType::Float32)),
            direct_arg: None,
            order_by: None,
            distinct: false,
            output_name: "avg".into(),
            data_type: DataType::Float64,
        },
        min_agg("b", 1),
        max_agg("b", 1),
    ];
    let plan = build_vectorized_plan(&aggregates).expect("vectorized plan");
    let mut states = vec![
        AggState::CountStar(0),
        AggState::Count(0),
        AggState::Sum(None),
        AggState::Avg(None, 0),
        AggState::Min(None),
        AggState::Max(None),
    ];
    vectorized_step(&plan, &batch, &mut states).expect("vectorized step");
    assert_eq!(finalise(&states[0]).expect("count star"), Value::Int64(4));
    assert_eq!(finalise(&states[1]).expect("count"), Value::Int64(3));
    assert_eq!(finalise(&states[2]).expect("sum"), Value::Int64(80));
    assert_eq!(finalise(&states[4]).expect("min"), Value::Int64(10));
    assert_eq!(finalise(&states[5]).expect("max"), Value::Int64(40));

    assert!(
        build_vectorized_plan(&[LogicalAggregateExpr {
            func: AggregateFunc::Sum,
            arg: Some(ScalarExpr::Literal {
                value: Value::Int32(1),
                data_type: DataType::Int32,
            }),
            direct_arg: None,
            order_by: None,
            distinct: false,
            output_name: "bad".into(),
            data_type: DataType::Int64,
        }])
        .is_none()
    );

    let mut min_acc = None;
    update_extremum(&mut min_acc, &int32, true).expect("min");
    assert_eq!(min_acc, Some(Value::Int32(1)));
    let mut max_acc = None;
    update_extremum(&mut max_acc, &float64, false).expect("max");
    assert_eq!(max_acc, Some(Value::Float64(4.0)));
    let mut wrong_sum = Some(Value::Text("bad".into()));
    assert!(accumulate_sum(&mut wrong_sum, &int64).is_err());

    assert_eq!(read_i32_key(Some(&int32), 1).expect("null key"), None);
    assert_eq!(read_i64_key(Some(&int64), 2).expect("i64 key"), Some(30));
    assert_eq!(
        read_numeric_value(Some(&int32), 2).expect("numeric"),
        Some(3)
    );
    assert!(read_i32_key(None, 0).is_err());
    assert!(read_i64_key(Some(&int32), 0).is_err());
    assert!(read_numeric_value(Some(&float64), 0).is_err());

    let grouped_schema = Schema::new([
        Field::required("k", DataType::Int32),
        Field::required("v", DataType::Int64),
    ])
    .expect("schema");
    let grouped_plan = build_grouped_vectorized_plan(
        &[col("k", 0, DataType::Int32)],
        &[sum_agg("v", 1)],
        &grouped_schema,
    )
    .expect("grouped plan");
    let grouped_batch = Batch::new([int32, int64]).expect("grouped batch");
    let mut table = std::collections::HashMap::new();
    grouped_vectorized_step(&grouped_plan, &grouped_batch, &mut table).expect("grouped step");
    assert_eq!(table.get(&Some(1)), Some(&Some(10)));
    assert_eq!(table.get(&None), Some(&None));
    assert_eq!(table.get(&Some(3)), Some(&Some(30)));
    assert_eq!(
        finalize_grouped_sum(
            123,
            &DataType::Decimal {
                precision: None,
                scale: Some(2),
            },
        )
        .expect("decimal grouped sum"),
        Value::Decimal {
            value: 123,
            scale: 2
        }
    );
    let err = finalize_grouped_sum(i64::from(i32::MAX) + 1, &DataType::Int32)
        .expect_err("grouped INT sum overflow must not clamp");
    assert!(matches!(err, crate::ExecError::NumericFieldOverflow(_)));
}
