//! Column/literal tests and catalog/cast/text helper coverage.
//!
//! Extracted verbatim from the original `eval.rs` test module; pure code motion.

use super::*;

#[test]
fn column_ref_returns_correct_value() {
    let ev = Eval::new(col(1));
    let row = [Value::Int32(10), Value::Int32(20)];
    assert_eq!(ev.eval(&row).unwrap(), Value::Int32(20));
}

#[test]
fn column_ref_out_of_range_returns_error() {
    let ev = Eval::new(col(5));
    let err = ev.eval(&[Value::Int32(1)]).unwrap_err();
    assert!(
        matches!(err, EvalError::ColumnIndex { index: 5, len: 1 }),
        "unexpected: {err}"
    );
}

// -----------------------------------------------------------------------
// Literal
// -----------------------------------------------------------------------

#[test]
fn literal_returns_its_value() {
    let ev = Eval::new(lit_i32(42));
    assert_eq!(ev.eval(&[]).unwrap(), Value::Int32(42));
}

#[test]
fn catalog_compatibility_functions_cover_visible_oid_and_description_paths() {
    assert_eq!(
        eval_fn("pg_typeof", vec![Value::Int32(1)]),
        Value::Text("integer".to_owned())
    );
    assert_eq!(
        eval_fn("current_schemas", vec![Value::Bool(true)]),
        Value::Array {
            element_type: DataType::Text { max_len: None },
            elements: vec![
                Value::Text("pg_catalog".to_owned()),
                Value::Text("public".to_owned())
            ],
        }
    );
    assert_eq!(
        eval_fn(
            "to_regtype",
            vec![Value::Text("pg_catalog.int4".to_owned())]
        ),
        Value::RegType(Oid::new(23))
    );
    assert_eq!(
        eval_fn("pg_table_is_visible", vec![Value::RegClass(Oid::new(1259))]),
        Value::Bool(true)
    );
    assert_eq!(
        eval_fn("pg_is_other_temp_schema", vec![Value::Oid(Oid::new(11))]),
        Value::Bool(false)
    );
    assert_eq!(
        eval_fn("pg_function_is_visible", vec![Value::Int64(42)]),
        Value::Bool(true)
    );
    assert_eq!(
        eval_fn("pg_relation_is_publishable", vec![Value::Null]),
        Value::Bool(false)
    );
    assert_eq!(
        eval_fn(
            "set_config",
            vec![
                Value::Text("search_path".to_owned()),
                Value::Text("public".to_owned()),
                Value::Bool(true),
            ],
        ),
        Value::Text("public".to_owned())
    );
    assert_eq!(
        eval_fn("format_type", vec![Value::Oid(Oid::new(1700)), Value::Null]),
        Value::Text("numeric".to_owned())
    );
    assert_eq!(
        eval_fn(
            "pg_get_expr",
            vec![Value::Text("a + b".to_owned()), Value::Oid(Oid::new(1))],
        ),
        Value::Text("a + b".to_owned())
    );
    assert_eq!(
        eval_fn("pg_get_indexdef", vec![Value::Oid(Oid::new(42))]),
        Value::Text("index 42".to_owned())
    );
    assert_eq!(
        eval_fn(
            "pg_get_constraintdef",
            vec![Value::Oid(Oid::new(7)), Value::Bool(true)],
        ),
        Value::Text("constraint 7".to_owned())
    );
    assert_eq!(
        eval_fn("pg_get_statisticsobjdef_columns", vec![Value::Int32(9)]),
        Value::Text(String::new())
    );
    assert_eq!(
        eval_fn("pg_get_function_result", vec![Value::RegType(Oid::new(10))]),
        Value::Text(String::new())
    );
    assert_eq!(
        eval_fn("pg_get_function_arguments", vec![Value::Int16(10)]),
        Value::Text(String::new())
    );
    assert_eq!(
        eval_fn("pg_encoding_to_char", vec![Value::Int32(6)]),
        Value::Text("UTF8".to_owned())
    );
    assert_eq!(
        eval_fn(
            "obj_description",
            vec![Value::Oid(Oid::new(1)), Value::Text("pg_class".to_owned())],
        ),
        Value::Null
    );
    assert_eq!(
        eval_fn(
            "col_description",
            vec![Value::Oid(Oid::new(1)), Value::Int32(2)]
        ),
        Value::Null
    );
    assert_eq!(
        eval_fn(
            "pg_get_serial_sequence",
            vec![Value::Text("t".to_owned()), Value::Text("id".to_owned()),],
        ),
        Value::Null
    );
}

#[test]
fn cast_and_size_helpers_cover_oid_reg_and_text_surfaces() {
    assert_eq!(
        eval_fn("__ultrasql_cast_oid", vec![Value::Int64(42)]),
        Value::Oid(Oid::new(42))
    );
    assert_eq!(
        eval_fn("__ultrasql_cast_regclass", vec![Value::Oid(Oid::new(43))]),
        Value::RegClass(Oid::new(43))
    );
    assert_eq!(
        eval_fn(
            "__ultrasql_cast_regtype",
            vec![Value::RegClass(Oid::new(44))]
        ),
        Value::RegType(Oid::new(44))
    );
    assert_eq!(
        eval_fn("__ultrasql_cast_text", vec![Value::Money(1234)]),
        Value::Text("$12.34".to_owned())
    );
    assert_eq!(
        eval_fn("pg_size_pretty", vec![Value::Int64(1536)]),
        Value::Text("1 kB".to_owned())
    );
    assert!(
        matches!(eval_fn("gen_random_uuid", vec![]), Value::Uuid(_)),
        "gen_random_uuid should emit uuid bytes"
    );
    assert!(
        eval_fn_err("__ultrasql_cast_oid", vec![Value::Int64(-1)]).contains("value out of range")
    );
}

#[test]
fn text_math_regex_and_format_helpers_cover_common_scalar_paths() {
    assert_eq!(eval_fn("abs", vec![Value::Int32(-7)]), Value::Int64(7));
    assert_eq!(
        eval_fn("lower", vec![Value::Text("MiXeD".to_owned())]),
        Value::Text("mixed".to_owned())
    );
    assert_eq!(
        eval_fn("upper", vec![Value::Text("MiXeD".to_owned())]),
        Value::Text("MIXED".to_owned())
    );
    assert_eq!(eval_fn("pi", vec![]), Value::Float64(std::f64::consts::PI));
    assert!(matches!(eval_fn("random", vec![]), Value::Float64(v) if (0.0..1.0).contains(&v)));
    assert_eq!(
        eval_fn("length", vec![Value::Text("abc".to_owned())]),
        Value::Int32(3)
    );
    assert_eq!(
        eval_fn(
            "bit_length",
            vec![Value::BitString(BitString::parse("10101").expect("bits"))]
        ),
        Value::Int32(5)
    );
    assert_eq!(
        eval_fn(
            "octet_length",
            vec![Value::BitString(BitString::parse("10101").expect("bits"))]
        ),
        Value::Int32(1)
    );
    assert_eq!(
        eval_fn("trim", vec![Value::Text("  hi  ".to_owned())]),
        Value::Text("hi".to_owned())
    );
    assert_eq!(
        eval_fn(
            "lpad",
            vec![
                Value::Text("7".to_owned()),
                Value::Int32(3),
                Value::Text("0".to_owned())
            ]
        ),
        Value::Text("007".to_owned())
    );
    assert_eq!(
        eval_fn(
            "rpad",
            vec![
                Value::Text("7".to_owned()),
                Value::Int32(3),
                Value::Text("0".to_owned())
            ]
        ),
        Value::Text("700".to_owned())
    );
    assert_eq!(
        eval_fn(
            "left",
            vec![Value::Text("abcdef".to_owned()), Value::Int32(2)]
        ),
        Value::Text("ab".to_owned())
    );
    assert_eq!(
        eval_fn(
            "right",
            vec![Value::Text("abcdef".to_owned()), Value::Int32(2)]
        ),
        Value::Text("ef".to_owned())
    );
    assert_eq!(
        eval_fn(
            "position",
            vec![
                Value::Text("cd".to_owned()),
                Value::Text("abcdef".to_owned())
            ]
        ),
        Value::Int32(3)
    );
    assert_eq!(
        eval_fn(
            "replace",
            vec![
                Value::Text("banana".to_owned()),
                Value::Text("na".to_owned()),
                Value::Text("NA".to_owned())
            ]
        ),
        Value::Text("baNANA".to_owned())
    );
    assert_eq!(
        eval_fn(
            "split_part",
            vec![
                Value::Text("a,b,c".to_owned()),
                Value::Text(",".to_owned()),
                Value::Int32(2)
            ]
        ),
        Value::Text("b".to_owned())
    );
    assert_eq!(
        eval_fn(
            "concat",
            vec![Value::Text("a".to_owned()), Value::Null, Value::Int32(7)]
        ),
        Value::Text("a7".to_owned())
    );
    assert_eq!(
        eval_fn(
            "concat_ws",
            vec![
                Value::Text("-".to_owned()),
                Value::Text("a".to_owned()),
                Value::Null,
                Value::Text("b".to_owned())
            ]
        ),
        Value::Text("a-b".to_owned())
    );
    assert_eq!(
        eval_fn(
            "repeat",
            vec![Value::Text("ha".to_owned()), Value::Int32(3)]
        ),
        Value::Text("hahaha".to_owned())
    );
    let too_large = Value::Int64(i64::try_from(MAX_EVAL_GENERATED_TEXT_CHARS).unwrap() + 1);
    assert!(
        eval_fn_err("lpad", vec![Value::Text("x".to_owned()), too_large.clone()])
            .contains("output length")
    );
    assert!(
        eval_fn_err("repeat", vec![Value::Text("x".to_owned()), too_large])
            .contains("output length")
    );
    assert_eq!(
        eval_fn("reverse", vec![Value::Text("abc".to_owned())]),
        Value::Text("cba".to_owned())
    );
    assert_eq!(
        eval_fn("quote_ident", vec![Value::Text("select".to_owned())]),
        Value::Text("\"select\"".to_owned())
    );
    assert_eq!(
        eval_fn("quote_literal", vec![Value::Text("a'b".to_owned())]),
        Value::Text("'a''b'".to_owned())
    );
    assert_eq!(
        eval_fn(
            "format",
            vec![
                Value::Text("hello %s %I".to_owned()),
                Value::Text("x".to_owned()),
                Value::Text("select".to_owned())
            ]
        ),
        Value::Text("hello x \"select\"".to_owned())
    );
    assert_eq!(
        eval_fn(
            "regexp_replace",
            vec![
                Value::Text("abc123".to_owned()),
                Value::Text("[0-9]+".to_owned()),
                Value::Text("!".to_owned())
            ]
        ),
        Value::Text("abc!".to_owned())
    );
}
