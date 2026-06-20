//! Coverage-matrix tests: typed-literal storage families and
//! literal coercion to cast targets.

use super::*;

    #[test]
    #[allow(clippy::too_many_lines)]
    fn typed_literal_matrix_covers_storage_families() {
        let cases = [
            ("date", "2000-01-01", None, DataType::Date, Value::Date(0)),
            ("date", "2000-99-99", None, DataType::Date, Value::Null),
            (
                "time",
                "01:02:03.000004",
                None,
                DataType::Time,
                Value::Time(3_723_000_004),
            ),
            (
                "timetz",
                "01:02:03-05",
                None,
                DataType::TimeTz,
                Value::TimeTz {
                    micros: 3_723_000_000,
                    offset_seconds: -18_000,
                },
            ),
            (
                "timestamp",
                "2000-01-01 00:00:01",
                None,
                DataType::Timestamp,
                Value::Timestamp(1_000_000),
            ),
            (
                "timestamptz",
                "2000-01-01 00:00:01+00",
                None,
                DataType::TimestampTz,
                Value::TimestampTz(1_000_000),
            ),
            (
                "json",
                "{\"ok\":true}",
                None,
                DataType::Json,
                Value::Json("{\"ok\":true}".to_owned()),
            ),
            ("json", "{bad", None, DataType::Json, Value::Null),
            (
                "jsonb",
                "[1,2]",
                None,
                DataType::Jsonb,
                Value::Jsonb("[1,2]".to_owned()),
            ),
            (
                "xml",
                "<root/>",
                None,
                DataType::Xml,
                Value::Xml("<root/>".to_owned()),
            ),
            ("xml", "<root>", None, DataType::Xml, Value::Null),
            ("money", "12.34", None, DataType::Money, Value::Money(1234)),
            ("oid", "42", None, DataType::Oid, Value::Oid(Oid::new(42))),
            (
                "pg_lsn",
                "1/10",
                None,
                DataType::PgLsn,
                Value::PgLsn(ultrasql_core::Lsn::new(0x1_0000_0010)),
            ),
            (
                "tsvector",
                "hello",
                None,
                DataType::TsVector,
                Value::Text("hello".to_owned()),
            ),
            ("unknown_type", "x", None, DataType::Null, Value::Null),
        ];

        for (type_name, text, unit, data_type, value) in cases {
            let expr = typed(type_name, text, unit);
            assert_eq!(literal_type(&expr), data_type, "{type_name} {text}");
            assert_eq!(literal_value(&expr), value, "{type_name} {text}");
        }

        for (unit, expected) in [
            ("years", (24, 0, 0)),
            ("months", (2, 0, 0)),
            ("days", (0, 2, 0)),
            ("hours", (0, 0, 7_200_000_000)),
            ("minutes", (0, 0, 120_000_000)),
            ("seconds", (0, 0, 2_000_000)),
        ] {
            assert_eq!(
                parse_interval_literal("2", Some(unit)),
                Some(expected),
                "{unit}"
            );
        }
        assert!(parse_interval_literal("2", Some("fortnights")).is_none());
        assert!(parse_interval_literal("999999999999999999", Some("hours")).is_none());

        assert_eq!(
            literal_type(&typed("bit", "101", None)),
            DataType::Bit { len: Some(3) }
        );
        assert_eq!(
            literal_type(&typed("bit", "102", None)),
            DataType::Bit { len: None }
        );
        assert_eq!(
            literal_type(&typed("varbit", "1010", None)),
            DataType::VarBit { max_len: Some(4) }
        );
        assert_eq!(
            literal_type(&typed("bit varying", "1010", None)),
            DataType::VarBit { max_len: Some(4) }
        );

        for (type_name, value, data_type) in [
            ("inet", "127.0.0.1", DataType::Inet),
            ("cidr", "10.0.0.0/8", DataType::Cidr),
            ("macaddr", "08:00:2b:01:02:03", DataType::MacAddr),
            ("macaddr8", "08:00:2b:01:02:03:04:05", DataType::MacAddr8),
        ] {
            let expr = typed(type_name, value, None);
            assert_eq!(literal_type(&expr), data_type, "{type_name}");
        }

        for (type_name, value, data_type) in [
            ("halfvec(2)", "[1,2]", DataType::HalfVec { dims: Some(2) }),
            (
                "sparsevec(5)",
                "{1:1,5:2}/5",
                DataType::SparseVec { dims: Some(5) },
            ),
            ("bitvec(4)", "1010", DataType::BitVec { dims: Some(4) }),
        ] {
            let expr = typed(type_name, value, None);
            assert_eq!(literal_type(&expr), data_type, "{type_name}");
            assert!(!matches!(literal_value(&expr), Value::Null));
        }
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn literal_coercion_matrix_covers_cast_targets() {
        let enum_type = DataType::Enum {
            oid: Oid::new(70_001),
            name: Arc::from("mood"),
            labels: Arc::from(vec!["sad".to_owned(), "ok".to_owned()].into_boxed_slice()),
        };
        let composite_type = DataType::Composite {
            oid: Oid::new(70_002),
            name: Arc::from("pair_type"),
            fields: Arc::from(
                vec![
                    ("a".to_owned(), DataType::Int32),
                    ("b".to_owned(), DataType::Text { max_len: None }),
                ]
                .into_boxed_slice(),
            ),
        };
        let domain_type = DataType::Domain {
            oid: Oid::new(70_003),
            name: Arc::from("positive_int"),
            base_type: Box::new(DataType::Int32),
            not_null: true,
        };

        let scalar_cases = [
            (lit(Value::Int32(7)), DataType::Int16, Value::Int16(7)),
            (lit(Value::Int64(8)), DataType::Int16, Value::Int16(8)),
            (
                lit(Value::Text("9".to_owned())),
                DataType::Int16,
                Value::Int16(9),
            ),
            (lit(Value::Int16(10)), DataType::Int32, Value::Int32(10)),
            (lit(Value::Int64(11)), DataType::Int32, Value::Int32(11)),
            (
                lit(Value::Text("12".to_owned())),
                DataType::Int32,
                Value::Int32(12),
            ),
            (lit(Value::Int16(13)), DataType::Int64, Value::Int64(13)),
            (lit(Value::Int32(14)), DataType::Int64, Value::Int64(14)),
            (
                lit(Value::Text("15".to_owned())),
                DataType::Int64,
                Value::Int64(15),
            ),
            (
                lit(Value::Text("true".to_owned())),
                DataType::Bool,
                Value::Bool(true),
            ),
            (
                lit(Value::Text("off".to_owned())),
                DataType::Bool,
                Value::Bool(false),
            ),
            (
                lit(Value::Float32(1.25)),
                DataType::Float64,
                Value::Float64(1.25),
            ),
            (lit(Value::Int16(2)), DataType::Float64, Value::Float64(2.0)),
            (lit(Value::Int32(3)), DataType::Float64, Value::Float64(3.0)),
            (lit(Value::Int64(4)), DataType::Float64, Value::Float64(4.0)),
            (
                lit(Value::Decimal {
                    value: 125,
                    scale: 2,
                }),
                DataType::Float64,
                Value::Float64(1.25),
            ),
            (
                lit(Value::Float64(1.5)),
                DataType::Float32,
                Value::Float32(1.5),
            ),
            (lit(Value::Int16(2)), DataType::Float32, Value::Float32(2.0)),
            (lit(Value::Int32(3)), DataType::Float32, Value::Float32(3.0)),
            (lit(Value::Int64(4)), DataType::Float32, Value::Float32(4.0)),
            (
                lit(Value::Decimal {
                    value: 125,
                    scale: 2,
                }),
                DataType::Float32,
                Value::Float32(1.25),
            ),
            (
                lit(Value::Char("hi  ".to_owned())),
                DataType::Text { max_len: None },
                Value::Text("hi  ".to_owned()),
            ),
            (
                lit(Value::Timestamp(7)),
                DataType::TimestampTz,
                Value::TimestampTz(7),
            ),
            (
                lit(Value::TimestampTz(8)),
                DataType::Timestamp,
                Value::Timestamp(8),
            ),
            (
                lit(Value::Text("01:02:03".to_owned())),
                DataType::Time,
                Value::Time(3_723_000_000),
            ),
            (
                lit(Value::Text("01:02:03+02".to_owned())),
                DataType::TimeTz,
                Value::TimeTz {
                    micros: 3_723_000_000,
                    offset_seconds: 7_200,
                },
            ),
            (
                lit(Value::Text("2000-01-01 00:00:01".to_owned())),
                DataType::Timestamp,
                Value::Timestamp(1_000_000),
            ),
            (
                lit(Value::Text("2000-01-01 00:00:01+00".to_owned())),
                DataType::TimestampTz,
                Value::TimestampTz(1_000_000),
            ),
            (
                lit(Value::Text("12.34".to_owned())),
                DataType::Decimal {
                    precision: None,
                    scale: Some(2),
                },
                Value::Decimal {
                    value: 1234,
                    scale: 2,
                },
            ),
            (
                lit(Value::Int32(12)),
                DataType::Decimal {
                    precision: None,
                    scale: Some(2),
                },
                Value::Decimal {
                    value: 1200,
                    scale: 2,
                },
            ),
            (
                lit(Value::Text("12.34".to_owned())),
                DataType::Money,
                Value::Money(1234),
            ),
            (
                lit(Value::Text("[1,3)".to_owned())),
                DataType::Range(RangeType::Int4),
                Value::Range(
                    ultrasql_core::RangeValue::parse(RangeType::Int4, "[1,3)")
                        .expect("range parses"),
                ),
            ),
            (
                lit(Value::Text("(1,2)".to_owned())),
                DataType::Geometry(GeometryType::Point),
                Value::Geometry(
                    ultrasql_core::GeometryValue::parse(GeometryType::Point, "(1,2)")
                        .expect("point parses"),
                ),
            ),
            (
                lit(Value::Text("[1,2,3]".to_owned())),
                DataType::Vector { dims: Some(3) },
                Value::Vector(vec![1.0, 2.0, 3.0]),
            ),
            (
                lit(Value::Text(
                    "550e8400-e29b-41d4-a716-446655440000".to_owned(),
                )),
                DataType::Uuid,
                Value::Uuid(Value::parse_uuid("550e8400-e29b-41d4-a716-446655440000").unwrap()),
            ),
            (
                lit(Value::Text("\\x0aff".to_owned())),
                DataType::Bytea,
                Value::Bytea(vec![0x0a, 0xff]),
            ),
            (
                lit(Value::Text("{\"a\":1}".to_owned())),
                DataType::Json,
                Value::Json("{\"a\":1}".to_owned()),
            ),
            (
                lit(Value::Json("{\"a\":1}".to_owned())),
                DataType::Jsonb,
                Value::Jsonb("{\"a\":1}".to_owned()),
            ),
            (
                lit(Value::Jsonb("{\"a\":1}".to_owned())),
                DataType::Json,
                Value::Json("{\"a\":1}".to_owned()),
            ),
            (
                lit(Value::Text("<root/>".to_owned())),
                DataType::Xml,
                Value::Xml("<root/>".to_owned()),
            ),
        ];

        for (input, target, expected) in scalar_cases {
            let expr = coerce(input, &target);
            assert_eq!(literal_value(&expr), expected, "{target}");
        }

        let decimal_target = DataType::Decimal {
            precision: Some(8),
            scale: Some(2),
        };
        let decimal_expr = coerce(lit(Value::Text("12.34".to_owned())), &decimal_target);
        assert_eq!(literal_type(&decimal_expr), decimal_target);

        let enum_expr = coerce(lit(Value::Text("ok".to_owned())), &enum_type);
        assert_eq!(literal_type(&enum_expr), enum_type);

        assert!(composite_text_matches_arity("(1,two)", 2));
        let composite_expr = coerce(lit(Value::Text("(1,two)".to_owned())), &composite_type);
        assert_eq!(literal_type(&composite_expr), composite_type);

        let domain_expr = coerce(lit(Value::Text("42".to_owned())), &domain_type);
        assert_eq!(literal_value(&domain_expr), Value::Int32(42));
        assert_eq!(literal_type(&domain_expr), domain_type);

        let array_expr = coerce(
            lit(Value::Array {
                element_type: DataType::Int32,
                elements: vec![Value::Int32(1), Value::Null, Value::Int32(2)],
            }),
            &DataType::Array(Box::new(DataType::Int64)),
        );
        assert_eq!(
            literal_type(&array_expr),
            DataType::Array(Box::new(DataType::Int64))
        );
        let text_array_expr = coerce(
            lit(Value::Text("{1,2}".to_owned())),
            &DataType::Array(Box::new(DataType::Int32)),
        );
        assert_eq!(
            literal_type(&text_array_expr),
            DataType::Array(Box::new(DataType::Int32))
        );

        let bit_expr = coerce(
            lit(Value::Text("1010".to_owned())),
            &DataType::Bit { len: Some(4) },
        );
        assert_eq!(literal_type(&bit_expr), DataType::Bit { len: Some(4) });

        let bpchar_expr = coerce(
            lit(Value::Text("hi".to_owned())),
            &DataType::Char { len: Some(4) },
        );
        assert_eq!(literal_value(&bpchar_expr), Value::Char("hi  ".to_owned()));

        let inet_expr = coerce(lit(Value::Text("127.0.0.1".to_owned())), &DataType::Inet);
        assert_eq!(literal_type(&inet_expr), DataType::Inet);

        let regclass_expr = coerce(lit(Value::Text("42".to_owned())), &DataType::RegClass);
        assert_eq!(literal_value(&regclass_expr), Value::RegClass(Oid::new(42)));
    }

