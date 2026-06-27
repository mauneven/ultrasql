//! Focused tests for date/time/interval/decimal/vector literal
//! parsing and constant folding.

use super::*;

#[test]
fn one_day_after_epoch() {
    assert_eq!(parse_date_literal("2000-01-02"), Some(1));
}

#[test]
fn pre_epoch_six_years_back() {
    // 1994-01-01: six 365-day years back plus one leap (1996),
    // so 6*365 + 1 = 2191 days before the epoch.
    assert_eq!(parse_date_literal("1994-01-01"), Some(-2191));
}

#[test]
fn one_year_forward_is_365_or_366() {
    let y2000 = parse_date_literal("2000-01-01").unwrap();
    let y2001 = parse_date_literal("2001-01-01").unwrap();
    assert_eq!(y2001 - y2000, 366, "2000 was a leap year");
    let y2002 = parse_date_literal("2002-01-01").unwrap();
    assert_eq!(y2002 - y2001, 365);
}

#[test]
fn rejects_malformed() {
    assert!(parse_date_literal("not-a-date").is_none());
    assert!(parse_date_literal("2000/01/01").is_none());
    assert!(parse_date_literal("2000-13-01").is_none());
    assert!(parse_date_literal("2000-01-32").is_none());
    assert!(parse_date_literal("2000-02-30").is_none());
}

#[test]
fn timestamp_literal_parses_microseconds_since_epoch() {
    assert_eq!(parse_timestamp_literal("2000-01-01 00:00:00"), Some(0));
    assert_eq!(
        parse_timestamp_literal("2000-01-02 00:00:00"),
        Some(86_400_000_000)
    );
    assert_eq!(
        parse_timestamp_literal("2000-01-01 01:02:03.456789"),
        Some(3_723_456_789)
    );
    assert_eq!(
        parse_timestamp_literal("2000-01-01 01:02:03.456789-08"),
        Some(3_723_456_789),
        "timestamp without time zone ignores input offset"
    );
}

#[test]
fn time_and_timetz_literals_parse_postgres_shapes() {
    assert_eq!(
        parse_time_of_day_micros("01:02:03.456789-08"),
        Some(3_723_456_789)
    );
    assert_eq!(
        parse_timetz_literal("04:05:06.789-08:00"),
        Some((14_706_789_000, -28_800))
    );
    assert_eq!(
        parse_timetz_literal("04:05:06.789 EST"),
        Some((14_706_789_000, -18_000))
    );
    assert_eq!(
        parse_timetz_literal("2000-07-01 04:05:06.789 America/New_York"),
        Some((14_706_789_000, -14_400))
    );
    assert_eq!(
        parse_timestamptz_literal("2000-01-02 03:04:05 EST"),
        Some(115_445_000_000)
    );
    assert_eq!(
        parse_timestamptz_literal("2000-07-01 00:00:00 America/New_York"),
        parse_timestamp_literal("2000-07-01 04:00:00")
    );
}

#[test]
fn algorithm_handles_leap_year_february() {
    let feb29 = days_since_epoch(2000, 2, 29).expect("valid leap day");
    let mar01 = days_since_epoch(2000, 3, 1).expect("valid March day");
    assert_eq!(mar01 - feb29, 1, "2000-02-29 → 2000-03-01 is one day");
}

#[test]
fn parses_interval_year_unit_into_months() {
    assert_eq!(parse_interval_literal("1", Some("year")), Some((12, 0, 0)));
    assert_eq!(parse_interval_literal("3", Some("month")), Some((3, 0, 0)));
    assert_eq!(parse_interval_literal("90", Some("day")), Some((0, 90, 0)));
}

#[test]
fn decimal_coercion_honors_target_scale() {
    let mut expr = ScalarExpr::Literal {
        value: Value::Float64(0.0001),
        data_type: DataType::Float64,
    };
    coerce_literal_to_type(
        &mut expr,
        &DataType::Decimal {
            precision: Some(15),
            scale: Some(2),
        },
    );
    let ScalarExpr::Literal { value, data_type } = expr else {
        panic!("expected literal");
    };
    assert_eq!(value, Value::Decimal { value: 0, scale: 2 });
    assert_eq!(
        data_type,
        DataType::Decimal {
            precision: Some(15),
            scale: Some(2)
        }
    );
}

#[test]
fn dotted_numeric_literal_binds_as_exact_decimal() {
    let expr = bind_literal(&Literal::Float {
        text: "0.0001".to_owned(),
        span: Span::default(),
    })
    .expect("decimal literal binds");
    let ScalarExpr::Literal { value, data_type } = expr else {
        panic!("expected literal");
    };
    assert_eq!(value, Value::Decimal { value: 1, scale: 4 });
    assert_eq!(
        data_type,
        DataType::Decimal {
            precision: None,
            scale: Some(4)
        }
    );
}

#[test]
fn decimal_literal_arithmetic_is_not_folded_through_float() {
    let left = ScalarExpr::Literal {
        value: Value::Decimal { value: 6, scale: 2 },
        data_type: DataType::Decimal {
            precision: None,
            scale: Some(2),
        },
    };
    let right = ScalarExpr::Literal {
        value: Value::Decimal { value: 1, scale: 2 },
        data_type: DataType::Decimal {
            precision: None,
            scale: Some(2),
        },
    };
    let folded = try_fold_literal_binary(BinaryOp::Sub, &left, &right)
        .expect("fold attempt should not error");
    assert!(folded.is_none(), "decimal arithmetic must stay exact");
}

#[test]
fn decimal_literal_coerces_to_float64_target() {
    let mut expr = bind_literal(&Literal::Float {
        text: "1.5".to_owned(),
        span: Span::default(),
    })
    .expect("float literal binds");
    coerce_literal_to_type(&mut expr, &DataType::Float64);
    let ScalarExpr::Literal { value, data_type } = expr else {
        panic!("expected literal");
    };
    assert_eq!(data_type, DataType::Float64);
    let Value::Float64(v) = value else {
        panic!("expected float64");
    };
    assert!((v - 1.5).abs() < f64::EPSILON);
}

#[test]
fn typed_vector_literal_binds_to_vector_value() {
    let expr = bind_literal(&Literal::Typed {
        type_name: "vector".to_owned(),
        value: "[1,2,3]".to_owned(),
        unit: None,
        span: Span::default(),
    })
    .expect("vector literal binds");
    let ScalarExpr::Literal { value, data_type } = expr else {
        panic!("expected literal");
    };
    assert_eq!(value, Value::Vector(vec![1.0, 2.0, 3.0]));
    assert_eq!(data_type, DataType::Vector { dims: Some(3) });
}

#[test]
fn typed_vector_literal_with_modifier_validates_dimension() {
    let expr = bind_literal(&Literal::Typed {
        type_name: "vector(3)".to_owned(),
        value: "[1,2,3]".to_owned(),
        unit: None,
        span: Span::default(),
    })
    .expect("vector(3) literal binds");
    let ScalarExpr::Literal { value, data_type } = expr else {
        panic!("expected literal");
    };
    assert_eq!(value, Value::Vector(vec![1.0, 2.0, 3.0]));
    assert_eq!(data_type, DataType::Vector { dims: Some(3) });
}

#[test]
fn typed_vector_literal_rejects_dimension_mismatch() {
    let expr = bind_literal(&Literal::Typed {
        type_name: "vector(3)".to_owned(),
        value: "[1,2]".to_owned(),
        unit: None,
        span: Span::default(),
    })
    .expect("vector(3) dimension-mismatch literal binds to NULL");
    let ScalarExpr::Literal { value, data_type } = expr else {
        panic!("expected literal");
    };
    assert_eq!(value, Value::Null);
    assert_eq!(data_type, DataType::Vector { dims: Some(3) });
}

#[test]
fn bind_time_and_timetz_literals_from_ast() {
    let time_expr = bind_literal(&Literal::Typed {
        type_name: "time".into(),
        value: "04:05:06-08".into(),
        unit: None,
        span: Span::new(0, 0),
    })
    .expect("time literal binds");
    let ScalarExpr::Literal { value, data_type } = time_expr else {
        panic!("expected time literal");
    };
    assert_eq!(data_type, DataType::Time);
    assert_eq!(value, Value::Time(14_706_000_000));

    let timetz_expr = bind_literal(&Literal::Typed {
        type_name: "time with time zone".into(),
        value: "04:05:06-08".into(),
        unit: None,
        span: Span::new(0, 0),
    })
    .expect("timetz literal binds");
    let ScalarExpr::Literal { value, data_type } = timetz_expr else {
        panic!("expected timetz literal");
    };
    assert_eq!(data_type, DataType::TimeTz);
    assert_eq!(
        value,
        Value::TimeTz {
            micros: 14_706_000_000,
            offset_seconds: -28_800,
        }
    );
}

#[test]
fn fold_date_interval_keeps_calendar_month_semantics() {
    let folded =
        fold_date_interval(days_since_epoch(2000, 1, 31).expect("valid date"), 1, 0, 0).unwrap();
    let super::ScalarExpr::Literal { value, data_type } = folded else {
        panic!("expected folded literal");
    };
    assert_eq!(data_type, DataType::Date);
    assert_eq!(
        value,
        Value::Date(days_since_epoch(2000, 2, 29).expect("valid leap day"))
    );
}

#[test]
fn negative_i64_boundary_literal_folds_exactly() {
    assert_eq!(
        parse_negative_i64_boundary("9223372036854775808"),
        Some(i64::MIN)
    );
    assert_eq!(
        parse_negative_i64_boundary("9_223_372_036_854_775_808"),
        Some(i64::MIN)
    );
    assert_eq!(parse_negative_i64_boundary("9223372036854775809"), None);
}

#[test]
fn folds_float_literal_subtraction() {
    let left = ScalarExpr::Literal {
        value: Value::Float64(0.06),
        data_type: DataType::Float64,
    };
    let right = ScalarExpr::Literal {
        value: Value::Float64(0.01),
        data_type: DataType::Float64,
    };

    let folded = try_fold_literal_binary(BinaryOp::Sub, &left, &right)
        .expect("fold succeeds")
        .expect("float literals should fold");
    let ScalarExpr::Literal {
        value: Value::Float64(value),
        data_type,
    } = folded
    else {
        panic!("expected float literal");
    };
    assert_eq!(data_type, DataType::Float64);
    assert!((value - 0.05).abs() < 1.0e-12, "expected 0.05, got {value}");
}

fn integer_literal(text: &str) -> ScalarExpr {
    bind_literal(&Literal::Integer {
        text: text.to_owned(),
        span: Span::default(),
    })
    .expect("integer literal binds")
}

#[test]
fn i64_max_integer_literal_binds_exactly() {
    // 9223372036854775807 is i64::MAX exactly and must round-trip.
    let ScalarExpr::Literal { value, data_type } = integer_literal("9223372036854775807") else {
        panic!("expected literal");
    };
    assert_eq!(value, Value::Int64(i64::MAX));
    assert_eq!(data_type, DataType::Int64);
}

#[test]
fn narrow_integer_literals_keep_their_natural_width() {
    let ScalarExpr::Literal { value, data_type } = integer_literal("42") else {
        panic!("expected literal");
    };
    assert_eq!(value, Value::Int32(42));
    assert_eq!(data_type, DataType::Int32);

    let ScalarExpr::Literal { value, data_type } = integer_literal("3000000000") else {
        panic!("expected literal");
    };
    assert_eq!(value, Value::Int64(3_000_000_000));
    assert_eq!(data_type, DataType::Int64);
}

#[test]
fn out_of_i64_integer_literal_binds_exact_numeric() {
    // 20-digit literal: previously parked at i64::MAX (silent corruption),
    // then the i64 stopgap errored. With the i128-backed NUMERIC it now
    // binds exactly to a `Decimal` (scale 0), matching PostgreSQL which
    // types large integer literals as `numeric`.
    let ScalarExpr::Literal { value, data_type } = bind_literal(&Literal::Integer {
        text: "99999999999999999999".to_owned(),
        span: Span::default(),
    })
    .expect("20-digit literal binds exactly") else {
        panic!("expected literal");
    };
    assert_eq!(
        value,
        Value::Decimal {
            value: 99_999_999_999_999_999_999,
            scale: 0,
        }
    );
    assert!(matches!(
        data_type,
        DataType::Decimal { scale: Some(0), .. }
    ));
}

#[test]
fn integer_literal_at_i64_boundary_plus_one_binds_numeric() {
    // i64::MAX + 1 = 9223372036854775808 (unsigned) is the first value
    // that no longer fits an i64; it now binds to an exact i128 NUMERIC.
    let ScalarExpr::Literal { value, .. } = bind_literal(&Literal::Integer {
        text: "9223372036854775808".to_owned(),
        span: Span::default(),
    })
    .expect("i64::MAX + 1 binds exactly") else {
        panic!("expected literal");
    };
    assert_eq!(
        value,
        Value::Decimal {
            value: 9_223_372_036_854_775_808,
            scale: 0,
        }
    );
}

#[test]
fn beyond_i128_integer_literal_errors() {
    // A 40-digit integer literal exceeds i128 (~38 digits) and must raise
    // numeric_value_out_of_range (22003) rather than truncate.
    let err = bind_literal(&Literal::Integer {
        text: "9999999999999999999999999999999999999999".to_owned(),
        span: Span::default(),
    })
    .expect_err("beyond-i128 literal must error");
    assert!(matches!(err, PlanError::NumericValueOutOfRange(_)));
}

#[test]
fn in_range_decimal_literal_beyond_i64_binds_exactly() {
    // 9999999999999999999.99 has an unscaled mantissa that overflows i64;
    // previously it silently fell back to a lossy Float64 (and later the
    // i64 stopgap errored). With i128 it now binds exactly.
    let ScalarExpr::Literal { value, .. } = bind_literal(&Literal::Float {
        text: "9999999999999999999.99".to_owned(),
        span: Span::default(),
    })
    .expect("beyond-i64 decimal binds exactly") else {
        panic!("expected literal");
    };
    assert_eq!(
        value,
        Value::Decimal {
            value: 999_999_999_999_999_999_999,
            scale: 2,
        }
    );
}

#[test]
fn out_of_range_decimal_literal_errors_instead_of_lossy_float() {
    // A mantissa beyond i128 (~38 digits) still raises
    // numeric_value_out_of_range (22003) rather than a lossy Float64.
    let err = bind_literal(&Literal::Float {
        text: "999999999999999999999999999999999999999.99".to_owned(),
        span: Span::default(),
    })
    .expect_err("out-of-range decimal must error");
    assert!(matches!(err, PlanError::NumericValueOutOfRange(_)));
}

#[test]
fn in_range_decimal_literal_binds_exactly() {
    let ScalarExpr::Literal { value, data_type } = bind_literal(&Literal::Float {
        text: "123.45".to_owned(),
        span: Span::default(),
    })
    .expect("in-range decimal binds") else {
        panic!("expected literal");
    };
    assert_eq!(
        value,
        Value::Decimal {
            value: 12_345,
            scale: 2
        }
    );
    assert_eq!(
        data_type,
        DataType::Decimal {
            precision: None,
            scale: Some(2),
        }
    );
}

#[test]
fn exponent_literal_still_falls_back_to_float64() {
    // Exponent notation is not an exact fixed-point decimal; it must
    // continue to bind as Float64, not error.
    let ScalarExpr::Literal { value, data_type } = bind_literal(&Literal::Float {
        text: "1.5e3".to_owned(),
        span: Span::default(),
    })
    .expect("exponent literal binds") else {
        panic!("expected literal");
    };
    assert_eq!(data_type, DataType::Float64);
    let Value::Float64(v) = value else {
        panic!("expected float64");
    };
    assert!((v - 1500.0).abs() < f64::EPSILON);
}
