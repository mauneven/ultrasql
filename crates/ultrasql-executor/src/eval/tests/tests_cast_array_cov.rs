//! Coverage tests: cast/size and array error-edge paths.
//!
//! Extracted verbatim from the original `eval.rs` test module; pure code motion.

use super::*;

#[test]
fn cast_size_and_array_error_edges_cover_scalar_compat_paths() {
    assert!(eval_fn_err("__ultrasql_cast_int2", vec![]).contains("expected 1 arg"));
    assert_eq!(
        eval_fn("__ultrasql_cast_int2", vec![Value::Null]),
        Value::Null
    );
    assert_eq!(
        eval_fn("__ultrasql_cast_int2", vec![Value::Int32(7)]),
        Value::Int16(7)
    );
    assert_eq!(
        eval_fn("__ultrasql_cast_int4", vec![Value::Text("42".into())]),
        Value::Int32(42)
    );
    assert!(
        eval_fn_err("__ultrasql_cast_int2", vec![Value::Int32(40_000)])
            .contains("out of range")
    );
    assert!(
        eval_fn_err("__ultrasql_cast_int4", vec![Value::Text("x".into())])
            .contains("invalid integer")
    );
    assert_eq!(
        eval_fn("__ultrasql_cast_int4", vec![Value::Int64(9)]),
        Value::Int32(9)
    );
    assert_eq!(
        eval_fn("__ultrasql_cast_int8", vec![Value::Int16(11)]),
        Value::Int64(11)
    );
    assert!(
        eval_fn_err("__ultrasql_cast_int8", vec![Value::Text("x".into())])
            .contains("invalid integer")
    );
    assert!(eval_fn_err("__ultrasql_cast_float4", vec![]).contains("expected 1 arg"));
    assert_eq!(
        eval_fn("__ultrasql_cast_float4", vec![Value::Null]),
        Value::Null
    );
    assert_eq!(
        eval_fn("__ultrasql_cast_float4", vec![Value::Int64(12)]),
        Value::Float32(12.0)
    );
    assert_eq!(
        eval_fn("__ultrasql_cast_float8", vec![Value::Float32(1.5)]),
        Value::Float64(1.5)
    );
    assert_eq!(
        eval_fn("__ultrasql_cast_float8", vec![Value::Text("3.5".into())]),
        Value::Float64(3.5)
    );
    assert_eq!(
        eval_fn(
            "__ultrasql_cast_float8",
            vec![Value::Decimal {
                value: 225,
                scale: 2
            }]
        ),
        Value::Float64(2.25)
    );
    assert!(
        eval_fn_err("__ultrasql_cast_float8", vec![Value::Text("x".into())])
            .contains("invalid numeric")
    );
    assert_eq!(
        eval_fn("__ultrasql_cast_bool", vec![Value::Text("yes".into())]),
        Value::Bool(true)
    );
    assert_eq!(
        eval_fn("__ultrasql_cast_bool", vec![Value::Char(" off ".into())]),
        Value::Bool(false)
    );
    assert!(
        eval_fn_err("__ultrasql_cast_bool", vec![Value::Text("maybe".into())])
            .contains("invalid syntax")
    );
    assert_eq!(
        eval_fn(
            "__ultrasql_cast_date",
            vec![Value::Text("2023-08-15".into())]
        ),
        Value::Date(parse_date_text("2023-08-15").expect("date parses"))
    );
    assert_eq!(
        eval_fn("__ultrasql_cast_time", vec![Value::Char("04:05:06".into())]),
        Value::Time(parse_time_text("04:05:06").expect("time parses"))
    );
    assert_eq!(
        eval_fn(
            "__ultrasql_cast_timestamp",
            vec![Value::Text("2023-08-15 04:05:06".into())]
        ),
        Value::Timestamp(
            parse_timestamp_text("2023-08-15 04:05:06").expect("timestamp parses")
        )
    );
    assert!(
        eval_fn_err("__ultrasql_cast_date", vec![Value::Text("bad".into())])
            .contains("invalid syntax")
    );
    assert_eq!(
        eval_fn(
            "__ultrasql_cast_timestamptz",
            vec![Value::Text("2023-08-15 04:05:06 UTC".into())]
        ),
        Value::TimestampTz(
            parse_timestamptz_text("2023-08-15 04:05:06 UTC").expect("timestamptz parses")
        )
    );
    let (timetz_micros, timetz_offset) =
        parse_timetz_text("04:05:06-05").expect("timetz parses");
    assert_eq!(
        eval_fn(
            "__ultrasql_cast_timetz",
            vec![Value::Text("04:05:06-05".into())]
        ),
        Value::TimeTz {
            micros: timetz_micros,
            offset_seconds: timetz_offset,
        }
    );
    assert_eq!(
        eval_fn(
            "__ultrasql_cast_uuid",
            vec![Value::Text("12345678-9abc-def0-1234-56789abcdef0".into())]
        ),
        Value::Uuid(
            Value::parse_uuid("12345678-9abc-def0-1234-56789abcdef0").expect("uuid parses")
        )
    );
    assert_eq!(
        eval_fn(
            "__ultrasql_cast_json",
            vec![Value::Text("{\"a\":1}".into())]
        ),
        Value::Json("{\"a\":1}".into())
    );
    assert_eq!(
        eval_fn(
            "__ultrasql_cast_jsonb",
            vec![Value::Text("{\"a\":1}".into())]
        ),
        Value::Jsonb("{\"a\":1}".into())
    );
    assert_eq!(
        eval_fn("__ultrasql_cast_xml", vec![Value::Text("<root/>".into())]),
        Value::Xml("<root/>".into())
    );
    assert_eq!(
        eval_fn("__ultrasql_cast_money", vec![Value::Text("$12.34".into())]),
        Value::Money(1234)
    );
    assert_eq!(
        eval_fn("__ultrasql_cast_numeric", vec![Value::Text("56.78".into())]),
        Value::Decimal {
            value: 5678,
            scale: 2
        }
    );
    assert_eq!(
        eval_fn("__ultrasql_cast_numeric", vec![Value::Int32(42)]),
        Value::Decimal {
            value: 42,
            scale: 0
        }
    );
    assert_eq!(
        eval_fn(
            "__ultrasql_cast_numeric",
            vec![
                Value::Text("12.345".into()),
                Value::Int32(5),
                Value::Int32(2)
            ]
        ),
        Value::Decimal {
            value: 1235,
            scale: 2
        }
    );
    assert_eq!(
        eval_fn(
            "__ultrasql_cast_numeric",
            vec![Value::Int32(7), Value::Int32(5), Value::Int32(2)]
        ),
        Value::Decimal {
            value: 700,
            scale: 2
        }
    );
    assert!(
        eval_fn_err(
            "__ultrasql_cast_numeric",
            vec![
                Value::Text("1234.56".into()),
                Value::Int32(5),
                Value::Int32(2)
            ]
        )
        .contains("numeric field overflow")
    );
    assert!(
        eval_fn_err("__ultrasql_cast_numeric", vec![Value::Text("bad".into())])
            .contains("invalid syntax")
    );
    assert!(
        eval_fn_err("__ultrasql_cast_jsonb", vec![Value::Text("{bad".into())])
            .contains("invalid JSON")
    );

    assert!(eval_fn_err("__ultrasql_cast_oid", vec![]).contains("expected 1 arg"));
    assert_eq!(
        eval_fn("__ultrasql_cast_oid", vec![Value::Null]),
        Value::Null
    );
    assert_eq!(
        eval_fn("__ultrasql_cast_oid", vec![Value::RegClass(Oid::new(42))]),
        Value::Oid(Oid::new(42))
    );
    assert_eq!(
        eval_fn("__ultrasql_cast_oid", vec![Value::Int16(42)]),
        Value::Oid(Oid::new(42))
    );
    assert!(eval_fn_err("__ultrasql_cast_oid", vec![Value::Text("x".into())]).contains("OID"));

    assert!(eval_fn_err("__ultrasql_cast_regclass", vec![]).contains("expected 1 arg"));
    assert_eq!(
        eval_fn("__ultrasql_cast_regclass", vec![Value::Null]),
        Value::Null
    );
    assert_eq!(
        eval_fn("__ultrasql_cast_regclass", vec![Value::Int16(7)]),
        Value::RegClass(Oid::new(7))
    );
    assert_eq!(
        eval_fn("__ultrasql_cast_regclass", vec![Value::Int32(8)]),
        Value::RegClass(Oid::new(8))
    );
    assert_eq!(
        eval_fn("__ultrasql_cast_regclass", vec![Value::Int64(9)]),
        Value::RegClass(Oid::new(9))
    );
    assert!(
        eval_fn_err("__ultrasql_cast_regclass", vec![Value::Text("x".into())]).contains("OID")
    );

    assert!(eval_fn_err("__ultrasql_cast_regtype", vec![]).contains("expected 1 arg"));
    assert_eq!(
        eval_fn("__ultrasql_cast_regtype", vec![Value::Null]),
        Value::Null
    );
    assert_eq!(
        eval_fn("__ultrasql_cast_regtype", vec![Value::Int16(7)]),
        Value::RegType(Oid::new(7))
    );
    assert_eq!(
        eval_fn("__ultrasql_cast_regtype", vec![Value::Int32(8)]),
        Value::RegType(Oid::new(8))
    );
    assert_eq!(
        eval_fn("__ultrasql_cast_regtype", vec![Value::Int64(9)]),
        Value::RegType(Oid::new(9))
    );
    assert!(
        eval_fn_err("__ultrasql_cast_regtype", vec![Value::Text("x".into())]).contains("OID")
    );

    assert!(eval_fn_err("__ultrasql_cast_text", vec![]).contains("expected 1 arg"));
    assert_eq!(
        eval_fn("__ultrasql_cast_text", vec![Value::Null]),
        Value::Null
    );
    assert_eq!(
        eval_fn("__ultrasql_cast_text", vec![Value::RegType(Oid::new(23))]),
        Value::Text("integer".into())
    );
    assert_eq!(
        eval_fn(
            "__ultrasql_cast_text",
            vec![Value::RegType(Oid::new(999_999))]
        ),
        Value::Text("999999".into())
    );
    assert_eq!(
        eval_fn("__ultrasql_cast_numeric", vec![Value::Money(1234)]),
        Value::Decimal {
            value: 1234,
            scale: 2
        }
    );
    assert_eq!(
        eval_fn("__ultrasql_cast_money", vec![Value::Int32(12)]),
        Value::Money(1200)
    );
    assert_eq!(
        eval_fn(
            "__ultrasql_cast_money",
            vec![Value::Decimal {
                value: 12_345,
                scale: 3
            }]
        ),
        Value::Money(1235)
    );
    assert!(eval_fn_err("pg_size_pretty", vec![]).contains("expected 1 arg"));
    assert_eq!(eval_fn("pg_size_pretty", vec![Value::Null]), Value::Null);
    assert!(eval_fn_err("pg_size_pretty", vec![Value::Text("x".into())]).contains("integer"));

    let array = Value::Array {
        element_type: DataType::Int32,
        elements: vec![Value::Int32(1)],
    };
    assert_eq!(
        eval_fn("array_length", vec![array.clone(), Value::Int32(0)]),
        Value::Null
    );
    assert!(eval_fn_err("__ultrasql_array_subscript", vec![]).contains("expected 2 args"));
    assert_eq!(
        eval_fn(
            "__ultrasql_array_subscript",
            vec![Value::Null, Value::Int32(1)]
        ),
        Value::Null
    );
    assert_eq!(
        eval_fn(
            "__ultrasql_array_subscript",
            vec![array.clone(), Value::Null]
        ),
        Value::Null
    );
    assert_eq!(
        eval_fn(
            "__ultrasql_array_subscript",
            vec![array.clone(), Value::Int32(0)]
        ),
        Value::Null
    );
    assert!(eval_fn_err("__ultrasql_eq_any_array", vec![]).contains("expected 2 args"));
    assert_eq!(
        eval_fn("__ultrasql_eq_any_array", vec![Value::Null, array.clone()]),
        Value::Null
    );
    assert_eq!(
        eval_fn("__ultrasql_eq_any_array", vec![Value::Int32(2), array]),
        Value::Bool(false)
    );
}

