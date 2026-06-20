//! Coverage tests: catalog and array nulls/errors/fallbacks.
//!
//! Extracted verbatim from the original `eval.rs` test module; pure code motion.

use super::*;

#[test]
fn catalog_and_array_functions_cover_nulls_errors_and_fallbacks() {
    for name in [
        "version",
        "current_catalog",
        "current_database",
        "current_schema",
        "current_user",
    ] {
        assert!(eval_fn(name, vec![]).data_type() == DataType::Text { max_len: None });
        assert!(eval_fn_err(name, vec![Value::Int32(1)]).contains("expected 0 args"));
    }

    assert_eq!(
        eval_fn("current_schemas", vec![Value::Bool(true)]),
        Value::Array {
            element_type: DataType::Text { max_len: None },
            elements: vec![
                Value::Text("pg_catalog".into()),
                Value::Text("public".into())
            ],
        }
    );
    assert_eq!(
        eval_fn("current_schemas", vec![Value::Bool(false)]),
        Value::Array {
            element_type: DataType::Text { max_len: None },
            elements: vec![Value::Text("public".into())],
        }
    );
    assert_eq!(
        eval_fn("current_schemas", vec![Value::Null]),
        Value::Array {
            element_type: DataType::Text { max_len: None },
            elements: vec![Value::Text("public".into())],
        }
    );
    assert!(eval_fn_err("current_schemas", vec![]).contains("expected 1 arg"));
    assert!(eval_fn_err("current_schemas", vec![Value::Int32(1)]).contains("boolean"));

    assert_eq!(eval_fn("to_regtype", vec![Value::Null]), Value::Null);
    assert_eq!(
        eval_fn("to_regtype", vec![Value::RegType(Oid::new(23))]),
        Value::RegType(Oid::new(23))
    );
    assert_eq!(
        eval_fn("to_regtype", vec![Value::Text("int4".into())]),
        Value::RegType(Oid::new(23))
    );
    assert_eq!(
        eval_fn(
            "to_regtype",
            vec![Value::Text(r#"pg_catalog."int4""#.into())]
        ),
        Value::RegType(Oid::new(23))
    );
    assert!(eval_fn_err("to_regtype", vec![]).contains("expected 1 arg"));
    assert!(eval_fn_err("to_regtype", vec![Value::Int32(1)]).contains("text argument"));

    for name in [
        "pg_table_is_visible",
        "pg_is_other_temp_schema",
        "pg_function_is_visible",
        "pg_relation_is_publishable",
    ] {
        assert!(matches!(
            eval_fn(name, vec![Value::Null]),
            Value::Null | Value::Bool(false)
        ));
        assert!(matches!(
            eval_fn(name, vec![Value::Oid(Oid::new(1))]),
            Value::Bool(_)
        ));
        assert!(eval_fn_err(name, vec![]).contains("expected 1 arg"));
        if name != "pg_relation_is_publishable" {
            assert!(eval_fn_err(name, vec![Value::Text("bad".into())]).contains("OID"));
        }
    }

    assert_eq!(
        eval_fn(
            "set_config",
            vec![
                Value::Text("work_mem".into()),
                Value::Text("4MB".into()),
                Value::Bool(true),
            ],
        ),
        Value::Text("4MB".into())
    );
    assert_eq!(
        eval_fn(
            "set_config",
            vec![Value::Null, Value::Text("x".into()), Value::Bool(false)]
        ),
        Value::Null
    );
    assert!(eval_fn_err("set_config", vec![]).contains("expected 3 args"));
    assert!(
        eval_fn_err(
            "set_config",
            vec![Value::Int32(1), Value::Text("x".into()), Value::Bool(false)]
        )
        .contains("setting name")
    );
    assert!(
        eval_fn_err(
            "set_config",
            vec![Value::Text("x".into()), Value::Int32(1), Value::Bool(false)]
        )
        .contains("setting value")
    );
    assert!(
        eval_fn_err(
            "set_config",
            vec![
                Value::Text("x".into()),
                Value::Text("y".into()),
                Value::Int32(1)
            ]
        )
        .contains("local flag")
    );

    for (oid, name) in [
        (16, "boolean"),
        (17, "bytea"),
        (20, "bigint"),
        (23, "integer"),
        (25, "text"),
        (2950, "uuid"),
        (3802, "jsonb"),
        (999_999, "text"),
    ] {
        assert_eq!(
            eval_fn("format_type", vec![Value::Oid(Oid::new(oid)), Value::Null]),
            Value::Text(name.into())
        );
    }
    assert_eq!(
        eval_fn("format_type", vec![Value::Null, Value::Null]),
        Value::Null
    );
    let numeric_8_2_typmod = (8_i32 << 16) + 2 + 4;
    assert_eq!(
        eval_fn(
            "format_type",
            vec![Value::Oid(Oid::new(1700)), Value::Int32(numeric_8_2_typmod)]
        ),
        Value::Text("numeric(8,2)".into())
    );
    assert_eq!(
        eval_fn(
            "format_type",
            vec![Value::Oid(Oid::new(1042)), Value::Int32(9)]
        ),
        Value::Text("character(5)".into())
    );
    assert!(eval_fn_err("format_type", vec![]).contains("expected 2 args"));
    assert!(
        eval_fn_err("format_type", vec![Value::Text("bad".into()), Value::Null])
            .contains("oid")
    );

    assert_eq!(
        eval_fn(
            "pg_get_expr",
            vec![Value::Text("x + 1".into()), Value::Oid(Oid::new(1))]
        ),
        Value::Text("x + 1".into())
    );
    assert_eq!(
        eval_fn(
            "pg_get_expr",
            vec![
                Value::Text("x + 1".into()),
                Value::Oid(Oid::new(1)),
                Value::Bool(false)
            ]
        ),
        Value::Text("x + 1".into())
    );
    assert_eq!(
        eval_fn("pg_get_expr", vec![Value::Null, Value::Oid(Oid::new(1))]),
        Value::Null
    );
    assert!(eval_fn_err("pg_get_expr", vec![]).contains("expected 2 or 3 args"));
    assert!(
        eval_fn_err(
            "pg_get_expr",
            vec![Value::Int32(1), Value::Oid(Oid::new(1))]
        )
        .contains("expression text")
    );

    for name in [
        "pg_get_indexdef",
        "pg_get_constraintdef",
        "pg_get_statisticsobjdef_columns",
        "pg_get_function_result",
        "pg_get_function_arguments",
    ] {
        assert_eq!(eval_fn(name, vec![Value::Null]), Value::Null);
        assert!(eval_fn_err(name, vec![Value::Text("bad".into())]).contains("oid"));
    }
    assert_eq!(
        eval_fn("pg_encoding_to_char", vec![Value::Int32(6)]),
        Value::Text("UTF8".into())
    );
    assert_eq!(
        eval_fn(
            "obj_description",
            vec![Value::Oid(Oid::new(1)), Value::Text("pg_class".into())]
        ),
        Value::Null
    );
    assert_eq!(
        eval_fn(
            "col_description",
            vec![Value::Oid(Oid::new(1)), Value::Int32(1)]
        ),
        Value::Null
    );
    assert_eq!(
        eval_fn(
            "pg_get_serial_sequence",
            vec![Value::Text("public.t".into()), Value::Text("id".into())]
        ),
        Value::Null
    );

    let array = Value::Array {
        element_type: DataType::Int32,
        elements: vec![Value::Int32(10), Value::Null, Value::Int32(30)],
    };
    assert_eq!(
        eval_fn("array_length", vec![array.clone(), Value::Int32(1)]),
        Value::Int32(3)
    );
    assert_eq!(
        eval_fn("array_length", vec![Value::Null, Value::Int32(1)]),
        Value::Null
    );
    assert!(
        eval_fn_err("array_length", vec![Value::Int32(1), Value::Int32(1)]).contains("array")
    );
    assert!(
        eval_fn_err("array_length", vec![array.clone(), Value::Text("1".into())])
            .contains("dimension")
    );
    assert_eq!(
        eval_fn("array_position", vec![array.clone(), Value::Int32(30)]),
        Value::Int32(3)
    );
    assert_eq!(
        eval_fn(
            "array_position",
            vec![array.clone(), Value::Int32(30), Value::Int32(3)]
        ),
        Value::Int32(3)
    );
    assert_eq!(
        eval_fn("array_position", vec![array.clone(), Value::Int32(99)]),
        Value::Null
    );
    assert_eq!(
        eval_fn(
            "array_to_string",
            vec![
                array.clone(),
                Value::Text(",".into()),
                Value::Text("NULL".into())
            ]
        ),
        Value::Text("10,NULL,30".into())
    );
    assert_eq!(
        eval_fn(
            "string_to_array",
            vec![Value::Text("a,b".into()), Value::Text(",".into())]
        ),
        Value::Array {
            element_type: DataType::Text { max_len: None },
            elements: vec![Value::Text("a".into()), Value::Text("b".into())],
        }
    );
    assert_eq!(
        eval_fn(
            "__ultrasql_array_subscript",
            vec![array.clone(), Value::Int32(2)]
        ),
        Value::Null
    );
    assert_eq!(
        eval_fn(
            "__ultrasql_array_subscript",
            vec![array.clone(), Value::Int32(3)]
        ),
        Value::Int32(30)
    );
    assert_eq!(
        eval_fn(
            "__ultrasql_array_slice",
            vec![array.clone(), Value::Int32(1), Value::Int32(2)]
        ),
        Value::Array {
            element_type: DataType::Int32,
            elements: vec![Value::Int32(10), Value::Null],
        }
    );
    assert_eq!(
        eval_fn(
            "__ultrasql_array_slice",
            vec![array.clone(), Value::Int32(3), Value::Null]
        ),
        Value::Array {
            element_type: DataType::Int32,
            elements: vec![Value::Int32(30)],
        }
    );
    assert_eq!(
        eval_fn(
            "__ultrasql_eq_any_array",
            vec![Value::Int32(10), array.clone()]
        ),
        Value::Bool(true)
    );
    assert_eq!(
        eval_fn("__ultrasql_eq_any_array", vec![Value::Int32(20), array]),
        Value::Null
    );
}

// -----------------------------------------------------------------------
// Column reference
// -----------------------------------------------------------------------

