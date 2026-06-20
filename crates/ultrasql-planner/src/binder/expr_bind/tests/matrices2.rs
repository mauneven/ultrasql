//! Coverage-matrix tests: builtin validation / type inference and
//! cast-type / numeric helper edge paths.

use super::*;

#[test]
#[allow(clippy::too_many_lines)]
fn builtin_validation_and_type_matrix_covers_catalog_introspection_surface() {
    let text = DataType::Text { max_len: None };
    let vector3 = DataType::Vector { dims: Some(3) };
    let halfvec3 = DataType::HalfVec { dims: Some(3) };
    let sparse5 = DataType::SparseVec { dims: Some(5) };

    let return_cases = [
        (
            "ifnull",
            vec![null_arg(DataType::Null), null_arg(text.clone())],
            text.clone(),
        ),
        (
            "nullif",
            vec![null_arg(DataType::Int32), null_arg(DataType::Int32)],
            DataType::Int32,
        ),
        ("least", vec![null_arg(DataType::Int32)], DataType::Int32),
        (
            "greatest",
            vec![null_arg(DataType::Float64)],
            DataType::Float64,
        ),
        ("extract", Vec::new(), DataType::Int64),
        ("current_date", Vec::new(), DataType::Date),
        ("now", Vec::new(), DataType::TimestampTz),
        ("age", Vec::new(), DataType::Interval),
        ("abs", Vec::new(), DataType::Int64),
        ("sqrt", Vec::new(), DataType::Float64),
        ("length", Vec::new(), DataType::Int32),
        ("bit_count", Vec::new(), DataType::Int64),
        ("set_bit", Vec::new(), DataType::VarBit { max_len: None }),
        ("lower", Vec::new(), text.clone()),
        ("to_tsvector", Vec::new(), DataType::TsVector),
        ("to_tsquery", Vec::new(), DataType::TsQuery),
        ("plainto_tsquery", Vec::new(), DataType::TsQuery),
        ("ts_rank", Vec::new(), DataType::Float64),
        ("ts_rank_cd", Vec::new(), DataType::Float64),
        ("ts_headline", Vec::new(), text.clone()),
        ("numnode", Vec::new(), DataType::Int32),
        ("querytree", Vec::new(), text.clone()),
        ("row_to_json", Vec::new(), DataType::Jsonb),
        ("jsonb_path_exists", Vec::new(), DataType::Bool),
        (
            "xpath",
            Vec::new(),
            DataType::Array(Box::new(DataType::Xml)),
        ),
        ("pg_advisory_lock", Vec::new(), DataType::Null),
        ("pg_try_advisory_lock", Vec::new(), DataType::Bool),
        ("has_table_privilege", Vec::new(), DataType::Bool),
        ("pg_get_userbyid", Vec::new(), text.clone()),
        ("to_regtype", Vec::new(), DataType::RegType),
        ("gen_random_uuid", Vec::new(), DataType::Uuid),
        ("pg_relation_size", Vec::new(), DataType::Int64),
        (
            "current_schemas",
            Vec::new(),
            DataType::Array(Box::new(text.clone())),
        ),
        ("version", Vec::new(), text.clone()),
        ("array_length", Vec::new(), DataType::Int32),
        ("array_to_string", Vec::new(), text.clone()),
        (
            "string_to_array",
            Vec::new(),
            DataType::Array(Box::new(text.clone())),
        ),
        ("l2_distance", Vec::new(), DataType::Float64),
        ("hybrid_search", Vec::new(), DataType::Float64),
        ("vector_norm", Vec::new(), DataType::Float64),
        ("vector_dims", Vec::new(), DataType::Int32),
    ];
    for (name, args, expected) in return_cases {
        assert_eq!(
            builtin_return_type(name, &args).unwrap(),
            expected,
            "{name}"
        );
        assert!(is_supported_builtin(name), "{name}");
    }
    assert!(builtin_return_type("missing_builtin", &[]).is_err());
    assert!(!is_supported_builtin("missing_builtin"));

    assert!(validate_builtin_args("current_schemas", &mut [null_arg(DataType::Bool)]).is_ok());
    assert!(validate_builtin_args("current_schemas", &mut [null_arg(DataType::Int32)]).is_err());
    assert!(
        validate_builtin_args(
            "set_config",
            &mut [
                null_arg(text.clone()),
                null_arg(text.clone()),
                null_arg(DataType::Bool),
            ],
        )
        .is_ok()
    );
    assert!(
        validate_builtin_args(
            "set_config",
            &mut [
                null_arg(DataType::Int32),
                null_arg(text.clone()),
                null_arg(DataType::Bool),
            ],
        )
        .is_err()
    );
    assert!(
        validate_builtin_args("pg_table_is_visible", &mut [null_arg(DataType::RegClass)]).is_ok()
    );
    assert!(validate_builtin_args("pg_table_is_visible", &mut [null_arg(text.clone())]).is_err());
    assert!(validate_builtin_args("to_regtype", &mut [null_arg(text.clone())]).is_ok());
    assert!(validate_builtin_args("to_regtype", &mut [null_arg(DataType::Int32)]).is_err());
    assert!(validate_builtin_args("to_tsvector", &mut [null_arg(text.clone())]).is_ok());
    assert!(
        validate_builtin_args(
            "to_tsvector",
            &mut [null_arg(text.clone()), null_arg(text.clone())]
        )
        .is_ok()
    );
    assert!(validate_builtin_args("to_tsvector", &mut [null_arg(DataType::Int32)]).is_err());
    assert!(
        validate_builtin_args(
            "ts_rank",
            &mut [null_arg(DataType::TsVector), null_arg(DataType::TsQuery)]
        )
        .is_ok()
    );
    assert!(
        validate_builtin_args(
            "ts_rank",
            &mut [
                null_arg(text.clone()),
                null_arg(DataType::TsVector),
                null_arg(DataType::TsQuery),
            ]
        )
        .is_err()
    );
    assert!(validate_builtin_args("ts_rank", &mut [null_arg(text.clone())]).is_err());
    assert!(
        validate_builtin_args(
            "ts_headline",
            &mut [null_arg(text.clone()), null_arg(DataType::TsQuery)]
        )
        .is_ok()
    );
    assert!(validate_builtin_args("ts_headline", &mut [null_arg(text.clone())]).is_err());
    assert!(validate_builtin_args("numnode", &mut [null_arg(DataType::TsQuery)]).is_ok());
    assert!(validate_builtin_args("numnode", &mut [null_arg(text.clone())]).is_err());
    assert!(validate_builtin_args("querytree", &mut [null_arg(DataType::TsQuery)]).is_ok());
    assert!(validate_builtin_args("querytree", &mut [null_arg(text.clone())]).is_err());
    assert!(
        validate_builtin_args(
            "has_column_privilege",
            &mut [
                null_arg(text.clone()),
                null_arg(text.clone()),
                null_arg(text.clone()),
                null_arg(text.clone()),
            ],
        )
        .is_ok()
    );
    assert!(
        validate_builtin_args(
            "has_column_privilege",
            &mut [
                null_arg(text.clone()),
                null_arg(text.clone()),
                null_arg(text.clone()),
            ],
        )
        .is_err()
    );
    assert!(validate_builtin_args("jsonb_path_exists", &mut [null_arg(DataType::Jsonb)]).is_err());
    assert!(validate_builtin_args("xml_is_well_formed", &mut [null_arg(DataType::Xml)],).is_ok());
    assert!(validate_builtin_args("xml_is_well_formed", &mut [null_arg(DataType::Int32)]).is_err());
    assert!(
        validate_builtin_args(
            "xpath",
            &mut [null_arg(text.clone()), null_arg(DataType::Xml)],
        )
        .is_ok()
    );
    assert!(
        validate_builtin_args(
            "xpath",
            &mut [
                null_arg(text.clone()),
                null_arg(DataType::Xml),
                null_arg(DataType::Array(Box::new(DataType::Array(Box::new(
                    text.clone()
                ))))),
            ],
        )
        .is_ok()
    );

    assert!(
        validate_builtin_args(
            "l2_distance",
            &mut [null_arg(vector3.clone()), null_arg(vector3.clone())],
        )
        .is_ok()
    );
    assert!(
        validate_builtin_args(
            "cosine_distance",
            &mut [null_arg(halfvec3.clone()), null_arg(halfvec3.clone())],
        )
        .is_ok()
    );
    assert!(
        validate_builtin_args(
            "l1_distance",
            &mut [null_arg(sparse5.clone()), null_arg(sparse5.clone())],
        )
        .is_ok()
    );
    assert!(
        validate_builtin_args(
            "l2_distance",
            &mut [
                null_arg(DataType::Vector { dims: Some(2) }),
                null_arg(vector3.clone()),
            ],
        )
        .is_err()
    );
    assert!(validate_builtin_args("vector_norm", &mut [null_arg(halfvec3.clone())]).is_ok());
    assert!(
        validate_builtin_args(
            "vector_norm",
            &mut [null_arg(DataType::BitVec { dims: Some(3) })]
        )
        .is_err()
    );
    assert!(
        validate_builtin_args(
            "vector_dims",
            &mut [null_arg(DataType::BitVec { dims: Some(3) })]
        )
        .is_ok()
    );
    assert!(validate_builtin_args("vector_dims", &mut [null_arg(DataType::Int32)]).is_err());
    assert!(
        validate_builtin_args(
            "hybrid_search",
            &mut [
                null_arg(DataType::Jsonb),
                null_arg(text.clone()),
                null_arg(vector3.clone()),
                null_arg(vector3.clone()),
            ],
        )
        .is_ok()
    );
    assert!(
        validate_builtin_args(
            "hybrid_search",
            &mut [
                null_arg(DataType::Int32),
                null_arg(text.clone()),
                null_arg(vector3.clone()),
                null_arg(vector3),
            ],
        )
        .is_err()
    );

    assert_eq!(
        vector_metric_family_kind(&DataType::Vector { dims: None }),
        Some(0)
    );
    assert_eq!(
        vector_metric_family_kind(&DataType::HalfVec { dims: None }),
        Some(1)
    );
    assert_eq!(
        vector_metric_family_kind(&DataType::SparseVec { dims: None }),
        Some(2)
    );
    assert_eq!(
        vector_metric_family_kind(&DataType::BitVec { dims: None }),
        None
    );
    assert_eq!(
        dense_vector_family_kind(&DataType::Vector { dims: None }),
        Some(0)
    );
    assert_eq!(
        dense_vector_family_kind(&DataType::HalfVec { dims: None }),
        Some(1)
    );
    assert_eq!(
        dense_vector_family_kind(&DataType::SparseVec { dims: None }),
        None
    );
}

#[test]
#[allow(clippy::too_many_lines)]
fn cast_type_and_numeric_helpers_cover_edge_paths() {
    for (name, expected) in [
        ("int", DataType::Int32),
        ("bigint", DataType::Int64),
        ("smallint", DataType::Int16),
        ("boolean", DataType::Bool),
        ("real", DataType::Float32),
        ("double precision", DataType::Float64),
        ("text", DataType::Text { max_len: None }),
        ("bytea", DataType::Bytea),
        ("date", DataType::Date),
        ("time with time zone", DataType::TimeTz),
        ("timestamp without time zone", DataType::Timestamp),
        ("timestamp with time zone", DataType::TimestampTz),
        ("uuid", DataType::Uuid),
        ("json", DataType::Json),
        ("jsonb", DataType::Jsonb),
        ("xml", DataType::Xml),
        (
            "numeric",
            DataType::Decimal {
                precision: None,
                scale: None,
            },
        ),
        (
            "numeric(8,2)",
            DataType::Decimal {
                precision: Some(8),
                scale: Some(2),
            },
        ),
        (
            "decimal(8)",
            DataType::Decimal {
                precision: Some(8),
                scale: Some(0),
            },
        ),
        ("money", DataType::Money),
        ("regclass", DataType::RegClass),
        ("regtype", DataType::RegType),
        ("pg_lsn", DataType::PgLsn),
        ("int4range", DataType::Range(RangeType::Int4)),
        ("point", DataType::Geometry(GeometryType::Point)),
        ("polygon", DataType::Geometry(GeometryType::Polygon)),
        ("char(3)", DataType::Char { len: Some(3) }),
        ("varchar(12)", DataType::Text { max_len: Some(12) }),
        ("bit(4)", DataType::Bit { len: Some(4) }),
        ("varbit(4)", DataType::VarBit { max_len: Some(4) }),
        ("inet", DataType::Inet),
        ("vector(3)", DataType::Vector { dims: Some(3) }),
        ("halfvec", DataType::HalfVec { dims: None }),
    ] {
        assert_eq!(resolve_cast_type(name), Some(expected), "{name}");
    }
    assert_eq!(resolve_cast_type("vector(0)"), None);
    assert_eq!(resolve_cast_type("not_a_type"), None);

    assert_eq!(pow10_i64(3), Some(1000));
    assert_eq!(scaled_decimal_text_to_i64("-12.30"), Some(-1230));
    assert_eq!(scaled_decimal_text_to_i64("bad"), None);
    assert_eq!(parse_decimal_literal("12.30"), Some((1230, 2)));
    assert_eq!(parse_decimal_literal("1e2"), None);
    assert_eq!(
        decimal_from_numeric_value(&Value::Int32(12), Some(2)),
        Some((1200, 2))
    );
    assert_eq!(
        decimal_from_numeric_value(&Value::Float64(12.25), Some(2)),
        Some((1225, 2))
    );
    assert_eq!(
        decimal_from_numeric_value(&Value::Float64(f64::NAN), Some(2)),
        None
    );
    assert_eq!(
        literal_numeric_as_f64(&Value::Decimal {
            value: 123,
            scale: 2
        }),
        Some(1.23)
    );
    assert_eq!(literal_numeric_as_f64(&Value::Text("x".to_owned())), None);
    assert_eq!(
        money_from_literal_value(&Value::Text("12.34".to_owned())),
        Some(1234)
    );
    assert_eq!(money_from_literal_value(&Value::Float64(1.0)), None);

    assert_eq!(
        parse_pg_identifier_path(r#"public."weird.name""#),
        Some(vec!["public".to_owned(), "weird.name".to_owned()])
    );
    assert_eq!(parse_pg_identifier_path(".bad"), None);

    assert!(vector_family_cast_matches(
        &DataType::Vector { dims: Some(3) },
        &DataType::Vector { dims: Some(3) }
    ));
    assert!(!vector_family_cast_matches(
        &DataType::Vector { dims: Some(3) },
        &DataType::Vector { dims: Some(2) }
    ));
    assert!(cast_result_matches(
        &DataType::Vector { dims: None },
        &DataType::Vector { dims: Some(3) }
    ));
    assert!(cast_result_matches(
        &DataType::Decimal {
            precision: None,
            scale: None
        },
        &DataType::Decimal {
            precision: None,
            scale: Some(2)
        }
    ));

    let mut left = lit(Value::Text("12".to_owned()));
    let mut right = lit(Value::Int32(12));
    coerce_literal_to_match(&mut left, &mut right);
    assert_eq!(literal_value(&left), Value::Int32(12));
    assert_eq!(literal_value(&right), Value::Int32(12));

    let result = common_scalar_return_type(
        "coalesce",
        &[null_arg(DataType::Int32), null_arg(DataType::Float64)],
    )
    .expect("numeric common type");
    assert_eq!(result, DataType::Float64);
    assert!(
        common_scalar_return_type(
            "coalesce",
            &[null_arg(DataType::Int32), null_arg(DataType::Xml)]
        )
        .is_err()
    );

    let _non_literal = coerce(
        ScalarExpr::Unary {
            op: ultrasql_parser::ast::UnaryOp::Neg,
            expr: Box::new(lit(Value::Int32(1))),
            data_type: DataType::Int32,
        },
        &DataType::Int32,
    );
}
