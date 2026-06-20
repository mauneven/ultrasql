use super::super::*;
use crate::parse_money_text;

#[test]
fn date_display_uses_iso_calendar_text() {
    assert_eq!(Value::Date(0).to_string(), "2000-01-01");
    assert_eq!(Value::Date(-1).to_string(), "1999-12-31");
    assert_eq!(Value::Date(8_766).to_string(), "2024-01-01");
}

#[test]
fn temporal_display_uses_postgres_iso_text() {
    assert_eq!(Value::Time(3_723_456_789).to_string(), "01:02:03.456789");
    assert_eq!(
        Value::Timestamp(90_245_006_789).to_string(),
        "2000-01-02 01:04:05.006789"
    );
    assert_eq!(
        Value::TimestampTz(90_245_000_000).to_string(),
        "2000-01-02 01:04:05+00"
    );
    assert_eq!(
        Value::TimeTz {
            micros: 14_706_789_000,
            offset_seconds: -28_800,
        }
        .to_string(),
        "04:05:06.789-08"
    );
}

#[test]
fn iso_date_and_timestamp_text_helpers_round_trip() {
    assert_eq!(parse_date_text(" 2000-01-02 "), Some(1));
    assert_eq!(format_date_days(1), "2000-01-02");
    let leap_day = parse_date_text("2024-02-29").unwrap();
    assert_eq!(format_date_days(leap_day), "2024-02-29");
    assert_eq!(parse_date_text("2023-02-29"), None);
    assert_eq!(parse_date_text("2023-04-31"), None);
    assert_eq!(
        parse_timestamp_text("2000-01-01T01:02:03.456789"),
        Some(3_723_456_789)
    );
    assert_eq!(
        parse_timestamp_text("2000-01-01 01:02:03.456789-08"),
        Some(3_723_456_789)
    );
    assert_eq!(parse_timestamp_text("2000-01-01"), None);
    assert_eq!(parse_timestamp_text("2000-01-01 bad"), None);
}

#[test]
fn timetz_equality_uses_utc_time_of_day() {
    assert_eq!(
        Value::TimeTz {
            micros: 64_800_000_000,
            offset_seconds: -25_200,
        },
        Value::TimeTz {
            micros: 61_200_000_000,
            offset_seconds: -28_800,
        }
    );
}

#[test]
fn null_is_null() {
    assert!(Value::Null.is_null());
    assert!(!Value::Int32(0).is_null());
}

#[test]
fn data_type_matches_variant() {
    assert_eq!(Value::Int32(1).data_type(), DataType::Int32);
    assert_eq!(Value::Int64(1).data_type(), DataType::Int64);
    assert_eq!(Value::Money(123).data_type(), DataType::Money);
    assert_eq!(Value::Bool(true).data_type(), DataType::Bool);
    assert_eq!(
        Value::Text("hi".into()).data_type(),
        DataType::Text { max_len: None }
    );
    assert_eq!(
        Value::Array {
            element_type: DataType::Int32,
            elements: vec![Value::Int32(1), Value::Int32(2)]
        }
        .data_type(),
        DataType::Array(Box::new(DataType::Int32))
    );
    assert_eq!(Value::Json(r#"{"a":1}"#.into()).data_type(), DataType::Json);
    assert_eq!(
        Value::Jsonb(r#"{"a":1}"#.into()).data_type(),
        DataType::Jsonb
    );
    assert_eq!(Value::Xml("<root/>".into()).data_type(), DataType::Xml);
    assert_eq!(Value::Null.data_type(), DataType::Null);
}

#[test]
fn range_values_cover_overlap_containment_and_empty_edges() {
    let left = RangeValue::parse(RangeType::Int4, "[1,10)").unwrap();
    let overlapping = RangeValue::parse(RangeType::Int4, "[9,12]").unwrap();
    let inside = RangeValue::parse(RangeType::Int4, "[2,3]").unwrap();
    let outside = RangeValue::parse(RangeType::Int4, "[10,12]").unwrap();
    let empty = RangeValue::parse(RangeType::Int4, "[5,5)").unwrap();

    assert!(left.overlaps(&overlapping));
    assert!(!left.overlaps(&outside));
    assert!(left.contains_range(&inside));
    assert!(left.contains_range(&empty));
    assert_eq!(empty.to_string(), "empty");
    assert_eq!(
        RangeValue::parse(RangeType::Num, "(1.5,2.25]")
            .unwrap()
            .to_string(),
        "(1.5,2.25]"
    );
    assert_eq!(
        RangeValue::parse(RangeType::Date, "[2000-01-01,2000-01-03)")
            .unwrap()
            .to_string(),
        "[0,2)"
    );
    assert!(RangeValue::parse(RangeType::Int4, "bad").is_none());
    assert!(!left.overlaps(&RangeValue::parse(RangeType::Int8, "[1,10)").unwrap()));
}

#[test]
fn geometry_values_use_bounding_boxes_for_gist_predicates() {
    let point = GeometryValue::parse(GeometryType::Point, "(1,2)").unwrap();
    let circle = GeometryValue::parse(GeometryType::Circle, "<(5,5),2>").unwrap();
    let container = GeometryValue::parse(GeometryType::Box, "((0,0),(10,10))").unwrap();
    let far = GeometryValue::parse(GeometryType::Polygon, "((20,20),(21,21),(22,20))").unwrap();

    assert_eq!(point.to_string(), "(1,2)");
    assert!(container.contains_geometry(&circle));
    assert!(container.overlaps(&circle));
    assert!(!container.overlaps(&far));
    assert!(GeometryValue::parse(GeometryType::Point, "(1)").is_none());
    assert!(GeometryValue::parse(GeometryType::Circle, "(1,2)").is_none());
}

#[test]
fn array_display_and_parse_round_trip() {
    let value = Value::Array {
        element_type: DataType::Text { max_len: None },
        elements: vec![
            Value::Text("red".into()),
            Value::Text("green,blue".into()),
            Value::Null,
        ],
    };
    assert_eq!(value.to_string(), r#"{red,"green,blue",NULL}"#);
    assert_eq!(
        Value::parse_array(DataType::Text { max_len: None }, &value.to_string()),
        Some(value)
    );

    let xml = Value::Array {
        element_type: DataType::Xml,
        elements: vec![
            Value::Xml(r#"<item id="1">a</item>"#.into()),
            Value::Xml("Ada Lovelace".into()),
        ],
    };
    assert_eq!(
        xml.to_string(),
        r#"{"<item id=\"1\">a</item>","Ada Lovelace"}"#
    );
}

#[test]
fn array_display_and_parse_multi_dimensional_round_trip() {
    let matrix_type = DataType::Array(Box::new(DataType::Int32));
    let value = Value::Array {
        element_type: matrix_type.clone(),
        elements: vec![
            Value::Array {
                element_type: DataType::Int32,
                elements: vec![Value::Int32(1), Value::Int32(2)],
            },
            Value::Array {
                element_type: DataType::Int32,
                elements: vec![Value::Int32(3), Value::Int32(4)],
            },
        ],
    };
    assert_eq!(value.to_string(), "{{1,2},{3,4}}");
    assert_eq!(value.array_dimensions(), Some(vec![2, 2]));
    assert_eq!(
        Value::parse_array(matrix_type.clone(), &value.to_string()),
        Some(value)
    );
    assert_eq!(Value::parse_array(matrix_type, "{{1,2},{3}}"), None);
}

#[test]
fn array_parser_covers_scalar_element_families_and_escaping() {
    assert_eq!(
        Value::parse_array(DataType::Bool, "{t,false,NULL}")
            .unwrap()
            .to_string(),
        "{true,false,NULL}"
    );
    assert_eq!(
        Value::parse_array(DataType::Int16, "{-1,2}")
            .unwrap()
            .to_string(),
        "{-1,2}"
    );
    assert_eq!(
        Value::parse_array(DataType::Float64, "{1.5,2.25}")
            .unwrap()
            .to_string(),
        "{1.5,2.25}"
    );
    assert_eq!(
        Value::parse_array(DataType::Oid, "{42}"),
        Some(Value::Array {
            element_type: DataType::Oid,
            elements: vec![Value::Oid(crate::Oid::new(42))]
        })
    );
    assert_eq!(
        Value::parse_array(DataType::RegClass, "{43}"),
        Some(Value::Array {
            element_type: DataType::RegClass,
            elements: vec![Value::RegClass(crate::Oid::new(43))]
        })
    );
    assert_eq!(
        Value::parse_array(DataType::RegType, "{44}"),
        Some(Value::Array {
            element_type: DataType::RegType,
            elements: vec![Value::RegType(crate::Oid::new(44))]
        })
    );
    assert_eq!(
        Value::parse_array(DataType::PgLsn, "{0/2A}"),
        Some(Value::Array {
            element_type: DataType::PgLsn,
            elements: vec![Value::PgLsn(crate::Lsn::new(42))]
        })
    );
    assert_eq!(
        Value::parse_array(DataType::Char { len: Some(3) }, r#"{"a"}"#)
            .unwrap()
            .to_string(),
        r#"{"a  "}"#
    );
    assert_eq!(
        Value::parse_array(DataType::Bytea, r#"{"\\xdead"}"#)
            .unwrap()
            .to_string(),
        r#"{"\\xdead"}"#
    );
    assert_eq!(
        Value::parse_array(DataType::Money, "{$1.25}")
            .unwrap()
            .to_string(),
        "{$1.25}"
    );
    assert!(Value::parse_array(DataType::Uuid, "{not-a-uuid}").is_none());
    assert!(Value::parse_array(DataType::Text { max_len: None }, r#"{"unterminated}"#).is_none());
    assert!(Value::parse_array(DataType::Vector { dims: None }, "{[1,2]}").is_none());
}

#[test]
fn integer_widening_accessors() {
    assert_eq!(Value::Int16(7).as_i64(), Some(7));
    assert_eq!(Value::Int32(7).as_i64(), Some(7));
    assert_eq!(Value::Int64(7).as_i64(), Some(7));
    assert_eq!(Value::Float32(7.0).as_i64(), None);
    assert_eq!(Value::Null.as_i64(), None);
}

#[test]
fn money_display_and_parse_use_pg_cash_cents() {
    assert_eq!(Value::Money(123_456).to_string(), "$1,234.56");
    assert_eq!(Value::Money(-123).to_string(), "-$1.23");
    assert_eq!(
        parse_money_text("$1,234.565").expect("money parses"),
        Value::Money(123_457)
    );
    assert_eq!(
        parse_money_text("($1.23)").expect("parenthesized negative parses"),
        Value::Money(-123)
    );
}

#[test]
fn decimal_display_handles_scales_beyond_u64_powers() {
    assert_eq!(
        Value::Decimal {
            value: 12,
            scale: 25
        }
        .to_string(),
        "0.0000000000000000000000012"
    );
    assert_eq!(
        Value::Decimal {
            value: -12,
            scale: -20
        }
        .to_string(),
        "-1200000000000000000000"
    );
}

#[test]
fn char_values_preserve_padding_but_compare_trimmed() {
    assert_eq!(Value::Char("ok  ".to_owned()).to_string(), "ok  ");
    assert_eq!(
        Value::Char("ok  ".to_owned()).data_type(),
        DataType::Char { len: Some(4) }
    );
    assert_eq!(Value::Char("ok  ".to_owned()), Value::Char("ok".to_owned()));
}

#[test]
fn float_widening_accessors() {
    assert_eq!(Value::Float32(1.5).as_f64(), Some(1.5));
    assert_eq!(Value::Float64(2.5).as_f64(), Some(2.5));
    assert_eq!(Value::Int32(1).as_f64(), None);
}

#[test]
fn text_and_bytes_accessors() {
    let t = Value::Text("hello".into());
    assert_eq!(t.as_text(), Some("hello"));
    assert_eq!(t.as_bytes(), None);
    let b = Value::Bytea(vec![0xde, 0xad]);
    assert_eq!(b.as_bytes(), Some(&[0xde, 0xad][..]));
    assert_eq!(b.as_text(), None);
}

#[test]
fn bytea_parse_accepts_hex_text() {
    assert_eq!(
        Value::parse_bytea("\\xdeadBEEF"),
        Some(vec![0xde, 0xad, 0xbe, 0xef])
    );
    assert_eq!(Value::parse_bytea("\\xabc"), None);
    assert_eq!(Value::parse_bytea("deadbeef"), None);
    assert_eq!(Value::parse_bytea("\\xzz"), None);
}

#[test]
fn display_round_trip_for_simple_values() {
    assert_eq!(Value::Null.to_string(), "NULL");
    assert_eq!(Value::Bool(true).to_string(), "true");
    assert_eq!(Value::Int64(-7).to_string(), "-7");
    assert_eq!(Value::Text("hi".into()).to_string(), "hi");
    assert_eq!(Value::Bytea(vec![0xde, 0xad]).to_string(), "\\xdead");
}

#[test]
fn uuid_display_is_canonical() {
    let bytes = [
        0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde,
        0xf0,
    ];
    assert_eq!(
        Value::Uuid(bytes).to_string(),
        "12345678-9abc-def0-1234-56789abcdef0"
    );
}

#[test]
fn vector_parse_rejects_non_finite_elements() {
    assert_eq!(
        Value::parse_vector("[1, 2.5, -3]").unwrap(),
        Value::Vector(vec![1.0, 2.5, -3.0])
    );
    assert_eq!(
        Value::Vector(vec![1.0, 2.5, -3.0]).to_string(),
        "[1,2.5,-3]"
    );
    assert!(Value::parse_vector("[]").is_none());
    assert!(Value::parse_vector("[NaN]").is_none());
    assert!(Value::parse_vector("[Infinity]").is_none());
}

#[test]
fn vector_family_literals_parse_and_render() {
    assert_eq!(
        Value::parse_halfvec("[1, 2.5, -3]").unwrap(),
        Value::HalfVec(vec![1.0, 2.5, -3.0])
    );
    assert_eq!(
        Value::HalfVec(vec![1.0, 2.5, -3.0]).to_string(),
        "[1,2.5,-3]"
    );

    assert_eq!(
        Value::parse_sparsevec("{1:1,3:2.5}/5").unwrap(),
        Value::SparseVec(SparseVector::new(5, vec![(1, 1.0), (3, 2.5)]).unwrap())
    );
    assert_eq!(
        Value::SparseVec(SparseVector::new(5, vec![(1, 1.0), (3, 2.5)]).unwrap()).to_string(),
        "{1:1,3:2.5}/5"
    );

    assert_eq!(
        Value::parse_bitvec("101001").unwrap(),
        Value::BitVec {
            dims: 6,
            bytes: vec![0b1010_0100]
        }
    );
    assert_eq!(
        Value::parse_bitvec("111111111").unwrap(),
        Value::BitVec {
            dims: 9,
            bytes: vec![0xff, 0b1000_0000]
        }
    );
    assert_eq!(
        (Value::BitVec {
            dims: 6,
            bytes: vec![0b1010_0100],
        })
        .to_string(),
        "101001"
    );

    assert!(Value::parse_halfvec("[NaN]").is_none());
    assert!(Value::parse_sparsevec("{0:1}/5").is_none());
    assert!(Value::parse_sparsevec("{1:1}/0").is_none());
    assert!(Value::parse_bitvec("102").is_none());
    assert!(Value::parse_bitvec("").is_none());
}

#[test]
fn uuid_parse_accepts_canonical_and_compact() {
    let expected = [
        0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde,
        0xf0,
    ];
    assert_eq!(
        Value::parse_uuid("12345678-9abc-def0-1234-56789abcdef0"),
        Some(expected)
    );
    assert_eq!(
        Value::parse_uuid("123456789ABCDEF0123456789ABCDEF0"),
        Some(expected)
    );
    assert_eq!(Value::parse_uuid("not-a-uuid"), None);
    assert_eq!(
        Value::parse_uuid("12345678-9abc-def0-1234-56789abcdef"),
        None
    );
    assert_eq!(
        Value::parse_uuid("12345678-9abc-def0-1234-56789abcdeg0"),
        None
    );
}

#[test]
fn time_text_parser_and_timetz_pack_reject_bad_edges() {
    assert_eq!(
        parse_time_text("2000-01-01 04:05:06.789 -08"),
        Some(14_706_789_000)
    );
    assert_eq!(
        parse_timestamptz_text("2000-01-01 00:00:00 America/New_York"),
        Some(18_000_000_000)
    );
    assert_eq!(
        parse_timestamptz_text("2000-07-01 00:00:00 America/New_York"),
        parse_timestamp_text("2000-07-01 04:00:00")
    );
    assert_eq!(
        parse_timetz_text("2000-01-01 04:05:06 America/New_York"),
        Some((14_706_000_000, -18_000))
    );
    assert_eq!(
        parse_timetz_text("2000-07-01 04:05:06 America/New_York"),
        Some((14_706_000_000, -14_400))
    );
    assert_eq!(parse_timetz_text("04:05 zulu"), Some((14_700_000_000, 0)));
    assert_eq!(
        parse_timetz_text("04:05:06+0530"),
        Some((14_706_000_000, 19_800))
    );
    assert_eq!(
        format_timetz(14_706_789_000, 19_830),
        "04:05:06.789+05:30:30"
    );
    assert_eq!(parse_time_text("24:00"), Some(MICROS_PER_DAY));
    assert_eq!(parse_time_text("24:00:00.000001"), None);
    assert_eq!(parse_timetz_text("04:05 +16"), None);

    let packed = pack_timetz(MICROS_PER_DAY, 86_400).unwrap();
    assert_eq!(unpack_timetz(packed), Some((MICROS_PER_DAY, 86_400)));
    assert_eq!(pack_timetz(-1, 0), None);
    assert_eq!(pack_timetz(0, 86_401), None);
    assert_eq!(unpack_timetz(-1), None);
    assert_eq!(
        unpack_timetz((MICROS_PER_DAY + 1) << TIMETZ_OFFSET_BITS),
        None
    );
}

#[test]
fn from_impls() {
    let v: Value = 7_i32.into();
    assert_eq!(v, Value::Int32(7));
    let v: Value = "abc".into();
    assert_eq!(v, Value::Text("abc".into()));
}
