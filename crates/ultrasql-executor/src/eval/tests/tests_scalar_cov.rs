//! Date/JSON/XML/bit/network and numeric/case dispatch coverage.
//!
//! Extracted verbatim from the original `eval.rs` test module; pure code motion.

use super::*;

#[test]
fn date_json_xml_bit_and_network_helpers_cover_scalar_edges() {
    let date = eval_fn(
        "make_date",
        vec![Value::Int32(2024), Value::Int32(2), Value::Int32(29)],
    );
    assert_eq!(
        eval_fn(
            "extract",
            vec![Value::Text("year".to_owned()), date.clone()]
        ),
        Value::Int64(2024)
    );
    assert_eq!(
        eval_fn(
            "extract",
            vec![Value::Text("month".to_owned()), date.clone()]
        ),
        Value::Int64(2)
    );
    assert_eq!(
        eval_fn("extract", vec![Value::Text("day".to_owned()), date]),
        Value::Int64(29)
    );
    assert_eq!(
        eval_fn(
            "extract",
            vec![Value::Text("hour".to_owned()), Value::Time(3_661_000_000)]
        ),
        Value::Int64(1)
    );
    assert_eq!(
        eval_fn(
            "extract",
            vec![
                Value::Text("minute".to_owned()),
                Value::Interval {
                    months: 14,
                    days: 2,
                    microseconds: 7_200_000_000,
                },
            ]
        ),
        Value::Int64(0)
    );
    assert_eq!(
        eval_fn(
            "date_trunc",
            vec![
                Value::Text("minute".to_owned()),
                Value::TimestampTz(123_456_789),
            ]
        ),
        Value::TimestampTz(120_000_000)
    );
    assert_eq!(
        eval_fn(
            "age",
            vec![
                Value::Timestamp(2 * 86_400_000_000 + 1_000_000),
                Value::Timestamp(0),
            ]
        ),
        Value::Interval {
            months: 0,
            days: 2,
            microseconds: 1_000_000,
        }
    );
    assert_eq!(
        eval_fn(
            "date_bin",
            vec![
                Value::Interval {
                    months: 0,
                    days: 0,
                    microseconds: 15 * 60_000_000,
                },
                Value::TimestampTz(46 * 60_000_000),
                Value::TimestampTz(0),
            ]
        ),
        Value::TimestampTz(45 * 60_000_000)
    );
    assert!(
        eval_fn_err(
            "make_date",
            vec![Value::Int32(2024), Value::Int32(2), Value::Int32(30)]
        )
        .contains("invalid date")
    );
    assert!(
        eval_fn_err(
            "date_bin",
            vec![
                Value::Interval {
                    months: 1,
                    days: 0,
                    microseconds: 0,
                },
                Value::TimestampTz(0),
                Value::TimestampTz(0),
            ],
        )
        .contains("month stride")
    );

    let local_noon = parse_timestamp_text("2000-07-01 12:00:00").expect("timestamp parses");
    let utc_from_new_york = parse_timestamptz_text("2000-07-01 12:00:00 America/New_York")
        .expect("timestamptz parses");
    assert_eq!(
        Eval::new(call(
            "timezone",
            vec![
                lit_text("America/New_York"),
                ScalarExpr::Literal {
                    value: Value::Timestamp(local_noon),
                    data_type: DataType::Timestamp,
                },
            ],
            DataType::TimestampTz,
        ))
        .eval(&[])
        .expect("timestamp AT TIME ZONE evaluates"),
        Value::TimestampTz(utc_from_new_york)
    );
    assert_eq!(
        Eval::new(call(
            "timezone",
            vec![
                lit_text("America/New_York"),
                ScalarExpr::Literal {
                    value: Value::TimestampTz(utc_from_new_york),
                    data_type: DataType::TimestampTz,
                },
            ],
            DataType::Timestamp,
        ))
        .eval(&[])
        .expect("timestamptz AT TIME ZONE evaluates"),
        Value::Timestamp(local_noon)
    );
    assert_eq!(
        Eval::new(call(
            "timezone",
            vec![
                lit_text("UTC"),
                ScalarExpr::Literal {
                    value: Value::TimeTz {
                        micros: 14_706_000_000,
                        offset_seconds: -18_000,
                    },
                    data_type: DataType::TimeTz,
                },
            ],
            DataType::TimeTz,
        ))
        .eval(&[])
        .expect("timetz AT TIME ZONE evaluates"),
        Value::TimeTz {
            micros: 32_706_000_000,
            offset_seconds: 0,
        }
    );

    let bits = Value::BitString(BitString::parse("1010").expect("bits"));
    assert_eq!(eval_fn("bit_count", vec![bits.clone()]), Value::Int64(2));
    assert_eq!(
        eval_fn("get_bit", vec![bits.clone(), Value::Int32(2)]),
        Value::Int32(1)
    );
    assert_eq!(
        eval_fn("set_bit", vec![bits, Value::Int32(1), Value::Int32(1)]),
        Value::BitString(BitString::parse("1110").expect("bits"))
    );
    assert!(
        eval_fn_err(
            "set_bit",
            vec![
                Value::BitString(BitString::parse("10").expect("bits")),
                Value::Int32(0),
                Value::Int32(2),
            ],
        )
        .contains("new value")
    );

    assert_eq!(
        eval_fn(
            "json_build_object",
            vec![
                Value::Text("a".to_owned()),
                Value::Int32(1),
                Value::Text("b".to_owned()),
                Value::Bool(true),
            ]
        ),
        Value::Jsonb(r#"{"a":1,"b":true}"#.to_owned())
    );
    assert_eq!(
        eval_fn(
            "jsonb_set",
            vec![
                Value::Jsonb(r#"{"a":{"b":1}}"#.to_owned()),
                Value::Array {
                    element_type: DataType::Text { max_len: None },
                    elements: vec![Value::Text("a".to_owned()), Value::Text("b".to_owned())],
                },
                Value::Int32(9),
                Value::Bool(true),
            ]
        ),
        Value::Jsonb(r#"{"a":{"b":9}}"#.to_owned())
    );
    assert_eq!(
        eval_fn(
            "jsonb_path_exists",
            vec![
                Value::Jsonb(r#"{"items":[{"score":12},{"score":25}]}"#.to_owned()),
                Value::Text("$.items[*] ? (@.score >= 20)".to_owned()),
            ]
        ),
        Value::Bool(true)
    );
    assert_eq!(
        eval_fn(
            "row_to_json",
            vec![Value::Record(vec![
                ("id".to_owned(), Value::Int32(1)),
                ("name".to_owned(), Value::Text("a".to_owned())),
            ])]
        ),
        Value::Jsonb(r#"{"id":1,"name":"a"}"#.to_owned())
    );

    assert_eq!(
        eval_fn(
            "xml_is_well_formed_document",
            vec![Value::Text(
                "<root><item id=\"2\">b</item></root>".to_owned()
            )]
        ),
        Value::Bool(true)
    );
    assert_eq!(
        eval_fn(
            "xpath_exists",
            vec![
                Value::Text("/root/item[@id=\"2\"]".to_owned()),
                Value::Xml("<root><item id=\"1\"/><item id=\"2\">b</item></root>".to_owned()),
            ]
        ),
        Value::Bool(true)
    );
    assert_eq!(
        eval_fn(
            "xpath",
            vec![
                Value::Text("/root/item".to_owned()),
                Value::Xml("<root><item>a</item><item>b</item></root>".to_owned()),
            ]
        ),
        Value::Array {
            element_type: DataType::Xml,
            elements: vec![
                Value::Xml("<item>a</item>".to_owned()),
                Value::Xml("<item>b</item>".to_owned()),
            ],
        }
    );

    let lit_inet = |text: &str| ScalarExpr::Literal {
        value: inet(text),
        data_type: DataType::Inet,
    };
    let network_add = ScalarExpr::Binary {
        op: BinaryOp::Add,
        left: Box::new(lit_inet("192.168.1.10")),
        right: Box::new(lit_i32(5)),
        data_type: DataType::Inet,
    };
    assert_eq!(
        Eval::new(network_add).eval(&[]).expect("network add"),
        inet("192.168.1.15")
    );
    let network_sub = ScalarExpr::Binary {
        op: BinaryOp::Sub,
        left: Box::new(lit_inet("192.168.1.15")),
        right: Box::new(lit_inet("192.168.1.10")),
        data_type: DataType::Int64,
    };
    assert_eq!(
        Eval::new(network_sub).eval(&[]).expect("network sub"),
        Value::Int64(5)
    );
}

#[test]
fn numeric_and_case_function_dispatch_covers_common_edges() {
    assert_float_close(eval_fn("ceil", vec![Value::Float64(1.2)]), 2.0);
    assert_float_close(eval_fn("floor", vec![Value::Float64(1.8)]), 1.0);
    assert_float_close(eval_fn("round", vec![Value::Float64(1.5)]), 2.0);
    assert_float_close(eval_fn("trunc", vec![Value::Float64(1.9)]), 1.0);
    assert_float_close(
        eval_fn("mod", vec![Value::Float64(7.0), Value::Float64(4.0)]),
        3.0,
    );
    assert_float_close(
        eval_fn("power", vec![Value::Float64(2.0), Value::Float64(3.0)]),
        8.0,
    );
    assert_float_close(eval_fn("sqrt", vec![Value::Float64(9.0)]), 3.0);
    assert_float_close(eval_fn("exp", vec![Value::Float64(0.0)]), 1.0);
    assert_float_close(
        eval_fn("ln", vec![Value::Float64(std::f64::consts::E)]),
        1.0,
    );
    assert_float_close(eval_fn("log", vec![Value::Float64(100.0)]), 2.0);
    assert_float_close(eval_fn("sin", vec![Value::Float64(0.0)]), 0.0);
    assert_float_close(eval_fn("cos", vec![Value::Float64(0.0)]), 1.0);
    assert_float_close(eval_fn("tan", vec![Value::Float64(0.0)]), 0.0);
    assert_float_close(
        eval_fn("asin", vec![Value::Float64(1.0)]),
        std::f64::consts::FRAC_PI_2,
    );
    assert_float_close(eval_fn("acos", vec![Value::Float64(1.0)]), 0.0);
    assert_float_close(
        eval_fn("atan", vec![Value::Float64(1.0)]),
        std::f64::consts::FRAC_PI_4,
    );
    assert!(eval_fn_err("sqrt", vec![Value::Text("bad".to_owned())]).contains("numeric"));

    assert_eq!(
        eval_fn(
            "case_searched",
            vec![
                Value::Bool(false),
                Value::Text("no".to_owned()),
                Value::Null,
                Value::Text("skip".to_owned()),
                Value::Bool(true),
                Value::Text("yes".to_owned()),
                Value::Text("else".to_owned()),
            ],
        ),
        Value::Text("yes".to_owned())
    );
    assert!(
        eval_fn_err(
            "case_searched",
            vec![Value::Int32(1), Value::Text("bad".to_owned()), Value::Null],
        )
        .contains("WHEN clause")
    );
    assert_eq!(
        eval_fn(
            "case_simple",
            vec![
                Value::Int32(2),
                Value::Int32(1),
                Value::Text("one".to_owned()),
                Value::Int32(2),
                Value::Text("two".to_owned()),
                Value::Text("else".to_owned()),
            ],
        ),
        Value::Text("two".to_owned())
    );
    assert_eq!(
        eval_fn(
            "case_simple",
            vec![
                Value::Null,
                Value::Null,
                Value::Text("null".to_owned()),
                Value::Text("else".to_owned()),
            ],
        ),
        Value::Text("else".to_owned())
    );
}

