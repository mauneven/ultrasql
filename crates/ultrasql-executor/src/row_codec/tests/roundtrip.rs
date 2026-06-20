//! Round-trip encode/decode tests.

use super::*;

#[test]
fn round_trip_bool_true() {
    let codec = RowCodec::new(schema_bool());
    let row = vec![Value::Bool(true)];
    let bytes = codec.encode(&row).unwrap();
    assert_eq!(codec.decode(&bytes).unwrap(), row);
}
#[test]
fn round_trip_bool_false() {
    let codec = RowCodec::new(schema_bool());
    let row = vec![Value::Bool(false)];
    let bytes = codec.encode(&row).unwrap();
    assert_eq!(codec.decode(&bytes).unwrap(), row);
}
#[test]
fn round_trip_int16() {
    let codec = RowCodec::new(schema_i16());
    for v in [i16::MIN, -1, 0, 1, i16::MAX] {
        let row = vec![Value::Int16(v)];
        assert_eq!(codec.decode(&codec.encode(&row).unwrap()).unwrap(), row);
    }
}
#[test]
fn round_trip_int32() {
    let codec = RowCodec::new(schema_i32());
    for v in [i32::MIN, -42, 0, 42, i32::MAX] {
        let row = vec![Value::Int32(v)];
        assert_eq!(codec.decode(&codec.encode(&row).unwrap()).unwrap(), row);
    }
}
#[test]
fn round_trip_int64() {
    let codec = RowCodec::new(schema_i64());
    for v in [i64::MIN, -1, 0, 1, i64::MAX] {
        let row = vec![Value::Int64(v)];
        assert_eq!(codec.decode(&codec.encode(&row).unwrap()).unwrap(), row);
    }
}
#[test]
fn round_trip_float32() {
    let codec = RowCodec::new(schema_f32());
    for v in [f32::NEG_INFINITY, -1.5, 0.0, 1.5, f32::INFINITY] {
        let row = vec![Value::Float32(v)];
        assert_eq!(codec.decode(&codec.encode(&row).unwrap()).unwrap(), row);
    }
}
#[test]
fn round_trip_float64() {
    let codec = RowCodec::new(schema_f64());
    for v in [f64::NEG_INFINITY, -1.5, 0.0, 1.5, f64::INFINITY] {
        let row = vec![Value::Float64(v)];
        assert_eq!(codec.decode(&codec.encode(&row).unwrap()).unwrap(), row);
    }
}
#[test]
fn round_trip_text() {
    let codec = RowCodec::new(schema_text());
    for s in ["", "hello", "unicode: \u{1F600}", &"x".repeat(1024)] {
        let row = vec![Value::Text(s.to_owned())];
        assert_eq!(codec.decode(&codec.encode(&row).unwrap()).unwrap(), row);
    }
}

#[test]
fn bounded_varchar_rejects_overlength_text_assignment() {
    let codec = RowCodec::new(schema_varchar3());
    assert!(matches!(
        codec.encode(&[Value::Text("abcd".to_owned())]),
        Err(RowCodecError::StringDataRightTruncation { column: 0, .. })
    ));
}

#[test]
fn round_trip_bpchar_pads_text_assignment() {
    let codec = RowCodec::new(schema_char4());
    let encoded = codec.encode(&[Value::Text("ok".to_owned())]).unwrap();
    assert_eq!(
        codec.decode(&encoded).unwrap(),
        vec![Value::Char("ok  ".to_owned())]
    );
    assert!(matches!(
        codec.encode(&[Value::Text("toolong".to_owned())]),
        Err(RowCodecError::StringDataRightTruncation { column: 0, .. })
    ));
}

#[test]
fn round_trip_int_array() {
    let schema = Schema::new([Field::required(
        "xs",
        DataType::Array(Box::new(DataType::Int32)),
    )])
    .unwrap();
    let codec = RowCodec::new(schema);
    let row = vec![Value::Array {
        element_type: DataType::Int32,
        elements: vec![Value::Int32(1), Value::Int32(2), Value::Null],
    }];
    assert_eq!(codec.decode(&codec.encode(&row).unwrap()).unwrap(), row);
}

#[test]
fn round_trip_json_preserves_text() {
    let schema = Schema::new([Field::required("doc", DataType::Json)]).unwrap();
    let codec = RowCodec::new(schema);
    let row = vec![Value::Json(r#"{"b": 2, "a": 1}"#.into())];
    assert_eq!(codec.decode(&codec.encode(&row).unwrap()).unwrap(), row);
}

#[test]
fn round_trip_jsonb() {
    let schema = Schema::new([Field::required("doc", DataType::Jsonb)]).unwrap();
    let codec = RowCodec::new(schema);
    let row = vec![Value::Jsonb(r#"{"b":"x","a":1}"#.into())];
    assert_eq!(
        codec.decode(&codec.encode(&row).unwrap()).unwrap(),
        vec![Value::Jsonb(r#"{"a":1,"b":"x"}"#.into())]
    );
}

#[test]
fn round_trip_xml_preserves_text_and_rejects_unbalanced_input() {
    let schema = Schema::new([Field::required("doc", DataType::Xml)]).unwrap();
    let codec = RowCodec::new(schema);
    let row = vec![Value::Xml(
        r#"<root attr="v"><child>text</child></root>"#.into(),
    )];
    assert_eq!(codec.decode(&codec.encode(&row).unwrap()).unwrap(), row);
    assert!(codec.encode(&[Value::Xml("<root>".into())]).is_err());
}

#[test]
fn round_trip_vector() {
    let schema = Schema::new([Field::required(
        "embedding",
        DataType::Vector { dims: Some(3) },
    )])
    .unwrap();
    let codec = RowCodec::new(schema);
    let row = vec![Value::Vector(vec![1.0, 2.5, -3.0])];
    assert_eq!(codec.decode(&codec.encode(&row).unwrap()).unwrap(), row);
}

#[test]
fn vector_binary_layout_is_stable() {
    let schema = Schema::new([Field::required(
        "embedding",
        DataType::Vector { dims: Some(3) },
    )])
    .unwrap();
    let codec = RowCodec::new(schema);
    let encoded = codec
        .encode(&[Value::Vector(vec![1.0, 2.5, -3.0])])
        .unwrap();

    assert_eq!(
        encoded.len(),
        1 + VECTOR_DIMS_WIDTH + 3 * VECTOR_ELEMENT_WIDTH
    );
    assert_eq!(
        encoded,
        vec![
            0x00, // null bitmap: one non-null column
            0x03, 0x00, 0x00, 0x00, // dims: u32 little-endian
            0x00, 0x00, 0x80, 0x3f, // 1.0f32 little-endian
            0x00, 0x00, 0x20, 0x40, // 2.5f32 little-endian
            0x00, 0x00, 0x40, 0xc0, // -3.0f32 little-endian
        ]
    );
}

#[test]
fn vector_decode_rejects_truncated_payload() {
    let schema = Schema::new([Field::required(
        "embedding",
        DataType::Vector { dims: Some(3) },
    )])
    .unwrap();
    let codec = RowCodec::new(schema);
    let mut encoded = vec![0x00];
    encoded.extend_from_slice(&3_u32.to_le_bytes());
    encoded.extend_from_slice(&1.0_f32.to_le_bytes());

    let err = codec
        .decode(&encoded)
        .expect_err("truncated vector payload");
    assert!(matches!(
        err,
        RowCodecError::Truncated { needed, have }
            if needed == 1 + VECTOR_DIMS_WIDTH + 3 * VECTOR_ELEMENT_WIDTH
                && have == encoded.len()
    ));
}

#[test]
fn vector_decode_rejects_non_finite_payload() {
    let schema = Schema::new([Field::required(
        "embedding",
        DataType::Vector { dims: Some(1) },
    )])
    .unwrap();
    let codec = RowCodec::new(schema);
    let mut encoded = vec![0x00];
    encoded.extend_from_slice(&1_u32.to_le_bytes());
    encoded.extend_from_slice(&f32::NAN.to_le_bytes());

    let err = codec
        .decode(&encoded)
        .expect_err("non-finite vector payload");
    assert!(matches!(err, RowCodecError::Type { column: 0, .. }));
}

#[test]
fn round_trip_vector_family_values() {
    let schema = Schema::new([
        Field::required("h", DataType::HalfVec { dims: Some(3) }),
        Field::required("s", DataType::SparseVec { dims: Some(5) }),
        Field::required("b", DataType::BitVec { dims: Some(6) }),
    ])
    .unwrap();
    let codec = RowCodec::new(schema);
    let row = vec![
        Value::HalfVec(vec![1.0, 2.5, -3.0]),
        Value::SparseVec(SparseVector::new(5, vec![(1, 1.0), (3, 2.5)]).unwrap()),
        Value::BitVec {
            dims: 6,
            bytes: vec![0b1010_0100],
        },
    ];
    assert_eq!(codec.decode(&codec.encode(&row).unwrap()).unwrap(), row);
}

#[test]
fn round_trip_temporal_oid_binary_network_range_and_geometry_values() {
    let schema = Schema::new([
        Field::required("oid", DataType::Oid),
        Field::required("regclass", DataType::RegClass),
        Field::required("regtype", DataType::RegType),
        Field::required("lsn", DataType::PgLsn),
        Field::required("date", DataType::Date),
        Field::required("ts", DataType::Timestamp),
        Field::required("tstz", DataType::TimestampTz),
        Field::required("time", DataType::Time),
        Field::required("timetz", DataType::TimeTz),
        Field::required("interval", DataType::Interval),
        Field::required("uuid", DataType::Uuid),
        Field::required("bytea", DataType::Bytea),
        Field::required("bits", DataType::Bit { len: Some(4) }),
        Field::required("varbits", DataType::VarBit { max_len: Some(8) }),
        Field::required("inet", DataType::Inet),
        Field::required("range", DataType::Range(RangeType::Int4)),
        Field::required("geom", DataType::Geometry(GeometryType::Box)),
    ])
    .unwrap();
    let codec = RowCodec::new(schema.clone());
    let row = vec![
        Value::Oid(Oid::new(1)),
        Value::RegClass(Oid::new(2)),
        Value::RegType(Oid::new(3)),
        Value::PgLsn(Lsn::new(0x1_0000_0002)),
        Value::Date(42),
        Value::Timestamp(123),
        Value::TimestampTz(456),
        Value::Time(789),
        Value::TimeTz {
            micros: 1_000,
            offset_seconds: -18_000,
        },
        Value::Interval {
            months: 2,
            days: 3,
            microseconds: 4,
        },
        Value::Uuid([7; 16]),
        Value::Bytea(vec![0, 1, 2, 255]),
        Value::BitString(BitString::parse("1010").expect("bit")),
        Value::BitString(BitString::parse("101011").expect("varbit")),
        Value::Network(
            NetworkValue::parse_for_type(&DataType::Inet, "192.168.1.10").expect("inet"),
        ),
        Value::Range(RangeValue::parse(RangeType::Int4, "[1,4)").expect("range")),
        Value::Geometry(GeometryValue::parse(GeometryType::Box, "((0,0),(2,3))").expect("box")),
    ];
    let encoded = codec.encode(&row).expect("encode");
    assert_eq!(codec.decode(&encoded).expect("decode"), row);
    assert_eq!(
        codec
            .decode_projected(&encoded, &[14, 0, 12])
            .expect("project"),
        vec![
            Value::Network(
                NetworkValue::parse_for_type(&DataType::Inet, "192.168.1.10").expect("inet")
            ),
            Value::Oid(Oid::new(1)),
            Value::BitString(BitString::parse("1010").expect("bit")),
        ]
    );

    let mut builders = schema
        .fields()
        .iter()
        .map(|field| ColumnBuilder::new(&field.data_type, 1, 0).expect("builder"))
        .collect::<Vec<_>>();
    codec
        .decode_into_builders(&encoded, &mut builders)
        .expect("decode builders");
    let batch = RowCodec::finish_batch(builders).expect("finish");
    assert_eq!(batch.width(), schema.len());
    assert_eq!(batch.rows(), 1);
}

#[test]
fn decode_projected_covers_varlena_catalog_network_and_vector_families() {
    let enum_type = DataType::Enum {
        oid: Oid::new(8_001),
        name: Arc::<str>::from("mood"),
        labels: Arc::from(vec!["happy".to_owned(), "sad".to_owned()].into_boxed_slice()),
    };
    let composite_type = DataType::Composite {
        oid: Oid::new(8_002),
        name: Arc::<str>::from("pair"),
        fields: Arc::from(
            vec![
                ("id".to_owned(), DataType::Int32),
                ("name".to_owned(), DataType::Text { max_len: None }),
            ]
            .into_boxed_slice(),
        ),
    };
    let schema = Schema::new([
        Field::required("b", DataType::Bool),
        Field::required("s", DataType::Int16),
        Field::required("f4", DataType::Float32),
        Field::required("f8", DataType::Float64),
        Field::required("enumv", enum_type.clone()),
        Field::required("comp", composite_type.clone()),
        Field::required("charv", DataType::Char { len: Some(4) }),
        Field::required("cidr", DataType::Cidr),
        Field::required("mac", DataType::MacAddr),
        Field::required("mac8", DataType::MacAddr8),
        Field::required("json", DataType::Json),
        Field::required("jsonb", DataType::Jsonb),
        Field::required("xml", DataType::Xml),
        Field::required("bytea", DataType::Bytea),
        Field::required("vector", DataType::Vector { dims: Some(2) }),
        Field::required("halfvec", DataType::HalfVec { dims: Some(2) }),
        Field::required("sparse", DataType::SparseVec { dims: Some(4) }),
        Field::required("bitvec", DataType::BitVec { dims: Some(8) }),
        Field::required(
            "array",
            DataType::Array(Box::new(DataType::Text { max_len: None })),
        ),
        Field::required("geom", DataType::Geometry(GeometryType::Point)),
    ])
    .unwrap();
    let row = vec![
        Value::Bool(false),
        Value::Int16(12),
        Value::Float32(3.5),
        Value::Float64(-4.5),
        Value::Text("happy".to_owned()),
        Value::Text("(1,foo)".to_owned()),
        Value::Char("xy  ".to_owned()),
        Value::Network(
            NetworkValue::parse_for_type(&DataType::Cidr, "192.168.0.0/24").expect("cidr"),
        ),
        Value::Network(
            NetworkValue::parse_for_type(&DataType::MacAddr, "08:00:2b:01:02:03").expect("mac"),
        ),
        Value::Network(
            NetworkValue::parse_for_type(&DataType::MacAddr8, "08:00:2b:01:02:03:04:05")
                .expect("mac8"),
        ),
        Value::Json(r#"{"z":0}"#.to_owned()),
        Value::Jsonb(r#"{"a":1}"#.to_owned()),
        Value::Xml("<root/>".to_owned()),
        Value::Bytea(vec![1, 2, 3]),
        Value::Vector(vec![1.0, -1.0]),
        Value::HalfVec(vec![0.5, 2.0]),
        Value::SparseVec(SparseVector::new(4, vec![(1, 1.0), (3, -2.0)]).unwrap()),
        Value::BitVec {
            dims: 8,
            bytes: vec![0b1010_1100],
        },
        Value::Array {
            element_type: DataType::Text { max_len: None },
            elements: vec![Value::Text("a".to_owned()), Value::Text("b".to_owned())],
        },
        Value::Geometry(GeometryValue::parse(GeometryType::Point, "(1,2)").expect("point")),
    ];
    let codec = RowCodec::new(schema.clone());
    let encoded = codec.encode(&row).expect("encode");
    let all_columns = (0..schema.len()).collect::<Vec<_>>();
    assert_eq!(
        codec
            .decode_projected(&encoded, &all_columns)
            .expect("project all"),
        row
    );
    assert_eq!(
        codec.decode_projected(&encoded, &[19]).expect("skip all"),
        vec![Value::Geometry(
            GeometryValue::parse(GeometryType::Point, "(1,2)").expect("point")
        )]
    );
    assert!(codec.encode(&[Value::Text("angry".to_owned())]).is_err());
    let null_schema = Schema::new([Field::required("n", DataType::Null)]).expect("null");
    assert!(matches!(
        RowCodec::new(null_schema).encode(&[Value::Int32(1)]),
        Err(RowCodecError::Type { column: 0, .. })
            | Err(RowCodecError::UnsupportedType { column: 0, .. })
    ));
}

#[test]
fn vector_family_encode_rejects_wrong_dimension() {
    for (data_type, value) in [
        (
            DataType::HalfVec { dims: Some(3) },
            Value::HalfVec(vec![1.0, 2.0]),
        ),
        (
            DataType::SparseVec { dims: Some(5) },
            Value::SparseVec(SparseVector::new(4, vec![(1, 1.0)]).unwrap()),
        ),
        (
            DataType::BitVec { dims: Some(8) },
            Value::BitVec {
                dims: 7,
                bytes: vec![0b1010_1010],
            },
        ),
    ] {
        let schema = Schema::new([Field::required("v", data_type)]).unwrap();
        let codec = RowCodec::new(schema);
        let err = codec.encode(&[value]).expect_err("dimension mismatch");
        assert!(matches!(err, RowCodecError::Type { column: 0, .. }));
    }
}
