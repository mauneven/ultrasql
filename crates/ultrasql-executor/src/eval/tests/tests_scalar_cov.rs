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
        Value::Decimal {
            value: 2024,
            scale: 0
        }
    );
    assert_eq!(
        eval_fn(
            "extract",
            vec![Value::Text("month".to_owned()), date.clone()]
        ),
        Value::Decimal { value: 2, scale: 0 }
    );
    assert_eq!(
        eval_fn("extract", vec![Value::Text("day".to_owned()), date]),
        Value::Decimal {
            value: 29,
            scale: 0
        }
    );
    assert_eq!(
        eval_fn(
            "extract",
            vec![Value::Text("hour".to_owned()), Value::Time(3_661_000_000)]
        ),
        Value::Decimal { value: 1, scale: 0 }
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
        Value::Decimal { value: 0, scale: 0 }
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
    let utc_from_new_york =
        parse_timestamptz_text("2000-07-01 12:00:00 America/New_York").expect("timestamptz parses");
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

#[test]
fn round_family_preserves_input_type_and_pg_rounding() {
    // numeric -> numeric, integer-valued result (scale 0).
    // round(1.5::numeric) = 2
    assert_eq!(
        eval_fn(
            "round",
            vec![Value::Decimal {
                value: 15,
                scale: 1
            }]
        ),
        Value::Decimal { value: 2, scale: 0 }
    );
    // numeric round is half AWAY from zero (not banker's): round(2.5) = 3.
    assert_eq!(
        eval_fn(
            "round",
            vec![Value::Decimal {
                value: 25,
                scale: 1
            }]
        ),
        Value::Decimal { value: 3, scale: 0 }
    );
    assert_eq!(
        eval_fn(
            "round",
            vec![Value::Decimal {
                value: 35,
                scale: 1
            }]
        ),
        Value::Decimal { value: 4, scale: 0 }
    );
    // round half away from zero for negatives: round(-2.5) = -3.
    assert_eq!(
        eval_fn(
            "round",
            vec![Value::Decimal {
                value: -25,
                scale: 1
            }]
        ),
        Value::Decimal {
            value: -3,
            scale: 0
        }
    );
    // floor/ceil/trunc on numeric -> numeric, directional.
    assert_eq!(
        eval_fn(
            "floor",
            vec![Value::Decimal {
                value: 17,
                scale: 1
            }]
        ),
        Value::Decimal { value: 1, scale: 0 }
    );
    assert_eq!(
        eval_fn(
            "floor",
            vec![Value::Decimal {
                value: -17,
                scale: 1
            }]
        ),
        Value::Decimal {
            value: -2,
            scale: 0
        }
    );
    assert_eq!(
        eval_fn(
            "ceil",
            vec![Value::Decimal {
                value: 12,
                scale: 1
            }]
        ),
        Value::Decimal { value: 2, scale: 0 }
    );
    assert_eq!(
        eval_fn(
            "ceil",
            vec![Value::Decimal {
                value: -12,
                scale: 1
            }]
        ),
        Value::Decimal {
            value: -1,
            scale: 0
        }
    );
    assert_eq!(
        eval_fn(
            "trunc",
            vec![Value::Decimal {
                value: 19,
                scale: 1
            }]
        ),
        Value::Decimal { value: 1, scale: 0 }
    );
    assert_eq!(
        eval_fn(
            "trunc",
            vec![Value::Decimal {
                value: -19,
                scale: 1
            }]
        ),
        Value::Decimal {
            value: -1,
            scale: 0
        }
    );

    // double precision -> double precision with banker's (ties-to-even)
    // rounding: round(2.5::float8) = 2, round(3.5::float8) = 4.
    assert_eq!(
        eval_fn("round", vec![Value::Float64(2.5)]),
        Value::Float64(2.0)
    );
    assert_eq!(
        eval_fn("round", vec![Value::Float64(3.5)]),
        Value::Float64(4.0)
    );
    assert_eq!(
        eval_fn("ceil", vec![Value::Float64(1.2)]),
        Value::Float64(2.0)
    );

    // integer -> numeric (PG casts int to numeric for these functions).
    assert_eq!(
        eval_fn("floor", vec![Value::Int32(1)]),
        Value::Decimal { value: 1, scale: 0 }
    );
    assert_eq!(
        eval_fn("round", vec![Value::Int64(7)]),
        Value::Decimal { value: 7, scale: 0 }
    );

    // NULL propagates.
    assert_eq!(eval_fn("round", vec![Value::Null]), Value::Null);
}

#[test]
fn abs_preserves_argument_numeric_type() {
    // Integer widths are preserved (matching the planner-declared type
    // and PostgreSQL, which keeps `abs(int4)` as `integer`).
    assert_eq!(eval_fn("abs", vec![Value::Int16(-5)]), Value::Int16(5));
    assert_eq!(eval_fn("abs", vec![Value::Int32(-2)]), Value::Int32(2));
    assert_eq!(eval_fn("abs", vec![Value::Int64(-7)]), Value::Int64(7));
    // Float widths are preserved.
    assert_eq!(
        eval_fn("abs", vec![Value::Float32(-1.5)]),
        Value::Float32(1.5)
    );
    assert_eq!(
        eval_fn("abs", vec![Value::Float64(-1.5)]),
        Value::Float64(1.5)
    );
    // Decimal preserves scale.
    assert_eq!(
        eval_fn(
            "abs",
            vec![Value::Decimal {
                value: -150,
                scale: 2,
            }],
        ),
        Value::Decimal {
            value: 150,
            scale: 2,
        }
    );
    // Money preserves the Money type.
    assert_eq!(eval_fn("abs", vec![Value::Money(-250)]), Value::Money(250));
    // NULL stays NULL.
    assert_eq!(eval_fn("abs", vec![Value::Null]), Value::Null);
}

#[test]
fn mod_integer_fast_path_is_exact_and_keeps_integer_type() {
    // f64 round-trip would lose the low bit and yield 0.0; the integer
    // fast path preserves it.
    assert_eq!(
        eval_fn(
            "mod",
            vec![Value::Int64(9_007_199_254_740_993), Value::Int64(2)]
        ),
        Value::Int64(1)
    );
    // Two int32s stay int32 (wider-integer rule).
    assert_eq!(
        eval_fn("mod", vec![Value::Int32(7), Value::Int32(3)]),
        Value::Int32(1)
    );
    // Mixed widths widen to the wider integer.
    assert_eq!(
        eval_fn("mod", vec![Value::Int16(7), Value::Int32(3)]),
        Value::Int32(1)
    );
    assert_eq!(
        eval_fn("mod", vec![Value::Int32(7), Value::Int64(3)]),
        Value::Int64(1)
    );
    // Two int16s stay int16.
    assert_eq!(
        eval_fn("mod", vec![Value::Int16(7), Value::Int16(3)]),
        Value::Int16(1)
    );
    // Zero divisor is a division-by-zero error.
    assert!(
        eval_fn_err("mod", vec![Value::Int32(7), Value::Int32(0)]).contains("division by zero")
    );
    // Float inputs still route through the f64 path and stay Float64.
    assert_eq!(
        eval_fn("mod", vec![Value::Float64(7.5), Value::Float64(2.0)]),
        Value::Float64(1.5)
    );
    // NULL operand yields NULL.
    assert_eq!(
        eval_fn("mod", vec![Value::Int32(7), Value::Null]),
        Value::Null
    );
}

#[test]
fn split_part_empty_delimiter_matches_postgres() {
    // PG: empty delimiter -> field 1 is the whole string.
    assert_eq!(
        eval_fn(
            "split_part",
            vec![
                Value::Text("abc".to_owned()),
                Value::Text(String::new()),
                Value::Int32(1),
            ],
        ),
        Value::Text("abc".to_owned())
    );
    // PG: any other field with an empty delimiter is empty.
    assert_eq!(
        eval_fn(
            "split_part",
            vec![
                Value::Text("abc".to_owned()),
                Value::Text(String::new()),
                Value::Int32(2),
            ],
        ),
        Value::Text(String::new())
    );
    // A real delimiter is unaffected.
    assert_eq!(
        eval_fn(
            "split_part",
            vec![
                Value::Text("a,b,c".to_owned()),
                Value::Text(",".to_owned()),
                Value::Int32(2),
            ],
        ),
        Value::Text("b".to_owned())
    );
}

// ---------------------------------------------------------------------------
// Short-circuit semantics for CASE / COALESCE.
//
// The whole-expression query path (`eval_expr`) must evaluate these lazily, so
// a fallible expression (here, division by zero) sitting in a branch / argument
// that PostgreSQL never reaches returns a value instead of raising. The
// *selected* branch is still evaluated, so genuine errors are not swallowed.
// ---------------------------------------------------------------------------

/// `1 / 0` — a well-typed expression that raises [`EvalError::DivByZero`] when
/// (and only when) it is actually evaluated.
fn div_by_zero() -> ScalarExpr {
    binop(BinaryOp::Div, lit_i32(1), lit_i32(0))
}

/// Evaluate a self-contained scalar (no columns) through the full `eval_expr`
/// query path, exercising the lazy short-circuit dispatch.
fn eval_scalar(expr: ScalarExpr) -> Result<Value, EvalError> {
    Eval::new(expr).eval(&[])
}

#[test]
fn case_searched_does_not_evaluate_non_taken_branch() {
    // CASE WHEN false THEN 1/0 ELSE 99 END -> 99 (THEN never evaluated).
    let expr = call(
        "case_searched",
        vec![lit_bool(false), div_by_zero(), lit_i32(99)],
        DataType::Int32,
    );
    assert_eq!(eval_scalar(expr).expect("no error"), Value::Int32(99));

    // CASE WHEN false THEN 1/0 WHEN true THEN 7 ELSE 1/0 END -> 7
    // The fallible first THEN and the fallible ELSE are both skipped.
    let expr = call(
        "case_searched",
        vec![
            lit_bool(false),
            div_by_zero(),
            lit_bool(true),
            lit_i32(7),
            div_by_zero(),
        ],
        DataType::Int32,
    );
    assert_eq!(eval_scalar(expr).expect("no error"), Value::Int32(7));

    // The selected branch is still evaluated: a fallible taken THEN must raise.
    let expr = call(
        "case_searched",
        vec![lit_bool(true), div_by_zero(), lit_i32(99)],
        DataType::Int32,
    );
    assert!(matches!(eval_scalar(expr), Err(EvalError::DivByZero)));
}

#[test]
fn case_simple_does_not_evaluate_non_taken_branch() {
    // CASE 1 WHEN 2 THEN 1/0 WHEN 1 THEN 5 ELSE 1/0 END -> 5
    let expr = call(
        "case_simple",
        vec![
            lit_i32(1),
            lit_i32(2),
            div_by_zero(),
            lit_i32(1),
            lit_i32(5),
            div_by_zero(),
        ],
        DataType::Int32,
    );
    assert_eq!(eval_scalar(expr).expect("no error"), Value::Int32(5));
}

#[test]
fn coalesce_stops_at_first_non_null() {
    // COALESCE(7, 1/0) -> 7 (second argument never evaluated).
    let expr = call("coalesce", vec![lit_i32(7), div_by_zero()], DataType::Int32);
    assert_eq!(eval_scalar(expr).expect("no error"), Value::Int32(7));

    // COALESCE(NULL, 1/0) evaluates the second argument (the first is NULL) and
    // raises — matching PostgreSQL, which does not skip it.
    let expr = call("coalesce", vec![lit_null(), div_by_zero()], DataType::Int32);
    assert!(matches!(eval_scalar(expr), Err(EvalError::DivByZero)));
}
