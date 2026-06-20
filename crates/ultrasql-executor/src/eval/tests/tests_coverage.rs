//! Coverage tests: subquery guards, null/extremum/xml helpers, catalog edges.
//!
//! Extracted verbatim from the original `eval.rs` test module; pure code motion.

use super::*;

#[test]
fn subquery_and_outer_scope_guards_return_unsupported() {
    let subplan = one_col_empty_plan();
    let exprs = vec![
        ScalarExpr::OuterColumn {
            name: "x".into(),
            frame_depth: 1,
            column_index: 0,
            data_type: DataType::Int32,
        },
        ScalarExpr::ScalarSubquery {
            subplan: Box::new(subplan.clone()),
            correlated: false,
            data_type: DataType::Int32,
        },
        ScalarExpr::Exists {
            subplan: Box::new(subplan.clone()),
            negated: false,
            correlated: false,
        },
        ScalarExpr::InSubquery {
            expr: Box::new(lit_i32(1)),
            subplan: Box::new(subplan),
            negated: false,
            correlated: false,
            data_type: DataType::Int32,
        },
    ];

    for expr in exprs {
        let err = Eval::new(expr).eval(&[]).expect_err("guard must reject");
        assert!(matches!(err, EvalError::Unsupported(_)), "got {err}");
    }
}

#[test]
fn null_helpers_extrema_xml_and_unknown_function_paths() {
    assert_eq!(
        eval_fn("coalesce", vec![Value::Null, Value::Text("x".into())]),
        Value::Text("x".into())
    );
    assert_eq!(eval_fn("coalesce", vec![Value::Null]), Value::Null);

    assert_eq!(
        eval_fn("ifnull", vec![Value::Null, Value::Int32(7)]),
        Value::Int32(7)
    );
    assert_eq!(
        eval_fn(
            "nvl",
            vec![Value::Text("a".into()), Value::Text("b".into())]
        ),
        Value::Text("a".into())
    );
    assert!(eval_fn_err("ifnull", vec![Value::Null]).contains("expected 2 args"));

    assert_eq!(
        eval_fn("nullif", vec![Value::Int32(7), Value::Int32(7)]),
        Value::Null
    );
    assert_eq!(
        eval_fn("nullif", vec![Value::Int32(7), Value::Int32(8)]),
        Value::Int32(7)
    );
    assert_eq!(
        eval_fn("nullif", vec![Value::Null, Value::Int32(8)]),
        Value::Null
    );
    assert!(eval_fn_err("nullif", vec![Value::Int32(1)]).contains("expected 2 args"));

    assert_eq!(
        eval_fn("least", vec![Value::Null, Value::Int32(8), Value::Int32(3)]),
        Value::Int32(3)
    );
    assert_eq!(
        eval_fn("greatest", vec![Value::Int32(8), Value::Int32(3)]),
        Value::Int32(8)
    );
    assert_eq!(eval_fn("least", vec![Value::Null]), Value::Null);
    assert_eq!(
        eval_fn("min", vec![Value::Int32(1), Value::Null]),
        Value::Null
    );
    assert!(eval_fn_err("greatest", vec![]).contains("expected at least 1 arg"));

    assert_eq!(
        eval_fn("xml_is_well_formed", vec![Value::Text("<a/><b/>".into())]),
        Value::Bool(true)
    );
    assert_eq!(
        eval_fn(
            "xml_is_well_formed_document",
            vec![Value::Text("<a/><b/>".into())]
        ),
        Value::Bool(false)
    );
    assert_eq!(
        eval_fn("xml_is_well_formed_content", vec![Value::Null]),
        Value::Null
    );
    assert!(eval_fn_err("xml_is_well_formed", vec![]).contains("expected 1 arg"));
    assert_eq!(
        eval_fn(
            "xmlparse",
            vec![
                Value::Text("document".into()),
                Value::Text("<root/>".into())
            ]
        ),
        Value::Xml("<root/>".into())
    );
    assert_eq!(
        eval_fn(
            "xmlparse",
            vec![
                Value::Text("content".into()),
                Value::Text("<a/><b/>".into())
            ]
        ),
        Value::Xml("<a/><b/>".into())
    );
    assert!(
        eval_fn_err(
            "xmlparse",
            vec![
                Value::Text("document".into()),
                Value::Text("<a/><b/>".into())
            ]
        )
        .contains("well-formed XML document")
    );
    assert_eq!(
        eval_fn(
            "xmlserialize",
            vec![
                Value::Text("content".into()),
                Value::Xml("<root/>".into()),
                Value::Text("text".into())
            ]
        ),
        Value::Text("<root/>".into())
    );

    assert!(eval_fn_err("does_not_exist", vec![]).contains("function not implemented"));
}

#[test]
fn catalog_edge_cases_cover_remaining_error_and_oid_paths() {
    for (oid, name) in [
        (21, "smallint"),
        (26, "oid"),
        (700, "real"),
        (701, "double precision"),
        (790, "money"),
        (114, "json"),
        (142, "xml"),
        (650, "cidr"),
        (829, "macaddr"),
        (869, "inet"),
        (1042, "character"),
        (1082, "date"),
        (1083, "time without time zone"),
        (1114, "timestamp without time zone"),
        (1184, "timestamp with time zone"),
        (1266, "time with time zone"),
        (1560, "bit"),
        (1562, "bit varying"),
        (3220, "pg_lsn"),
        (3614, "tsvector"),
        (3615, "tsquery"),
        (2205, "regclass"),
        (2206, "regtype"),
    ] {
        assert_eq!(
            eval_fn("format_type", vec![Value::Oid(Oid::new(oid)), Value::Null]),
            Value::Text(name.into())
        );
    }

    assert!(eval_fn_err("pg_typeof", vec![]).contains("expected 1 arg"));
    assert!(eval_fn_err("pg_get_indexdef", vec![]).contains("expected 1 to 3 args"));
    assert!(eval_fn_err("pg_get_constraintdef", vec![]).contains("expected 1 or 2 args"));
    assert_eq!(
        eval_fn("pg_get_constraintdef", vec![Value::Oid(Oid::new(0))]),
        Value::Null
    );
    assert!(eval_fn_err("pg_get_statisticsobjdef_columns", vec![]).contains("expected 1 arg"));
    assert!(eval_fn_err("pg_get_function_result", vec![]).contains("expected 1 arg"));
    assert!(eval_fn_err("pg_get_function_arguments", vec![]).contains("expected 1 arg"));
    assert!(eval_fn_err("pg_encoding_to_char", vec![]).contains("expected 1 arg"));
    assert_eq!(
        eval_fn("pg_encoding_to_char", vec![Value::Null]),
        Value::Null
    );
    assert!(
        eval_fn_err("pg_encoding_to_char", vec![Value::Text("UTF8".into())])
            .contains("integer argument")
    );

    assert!(eval_fn_err("obj_description", vec![]).contains("expected 2 args"));
    assert_eq!(
        eval_fn(
            "obj_description",
            vec![Value::Null, Value::Text("pg_class".into())]
        ),
        Value::Null
    );
    assert!(
        eval_fn_err(
            "obj_description",
            vec![Value::Text("bad".into()), Value::Text("pg_class".into())]
        )
        .contains("oid argument")
    );
    assert!(
        eval_fn_err(
            "obj_description",
            vec![Value::Oid(Oid::new(1)), Value::Int32(1)]
        )
        .contains("catalog name")
    );

    assert!(eval_fn_err("col_description", vec![]).contains("expected 2 args"));
    assert_eq!(
        eval_fn("col_description", vec![Value::Null, Value::Int32(1)]),
        Value::Null
    );
    assert!(
        eval_fn_err(
            "col_description",
            vec![Value::Text("bad".into()), Value::Text("bad".into())]
        )
        .contains("oid and integer")
    );

    assert!(eval_fn_err("pg_get_serial_sequence", vec![]).contains("expected 2 args"));
    assert_eq!(
        eval_fn(
            "pg_get_serial_sequence",
            vec![Value::Null, Value::Text("id".into())]
        ),
        Value::Null
    );
    assert!(
        eval_fn_err(
            "pg_get_serial_sequence",
            vec![Value::Int32(1), Value::Text("id".into())]
        )
        .contains("text arguments")
    );
}
