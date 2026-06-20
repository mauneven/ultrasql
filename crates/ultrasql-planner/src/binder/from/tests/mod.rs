use std::fs;
use std::io::Cursor;

use arrow_schema::DataType as ArrowDataType;
use serde_json::{Map as JsonMap, json};
use ultrasql_core::csv::CsvParseOptions;
use ultrasql_core::{DataType, Field, Schema, Value};
use ultrasql_parser::Parser;
use ultrasql_parser::ast::{Statement, TableRef};

use super::csv_schema::{
    bind_read_csv_table_function, bind_sniff_csv_table_function, first_csv_record_with_options,
    infer_csv_header_from_first_record, read_csv_header_from_first_record,
    validate_read_csv_reject_path_arg,
};
use super::paths::{
    contains_wildcard, expand_file_path_specs, expand_file_paths, path_specs_use_object_store,
    read_file_path_specs, wildcard_match,
};
use super::json_reader::{
    JsonColumnKind, JsonFieldAccumulator, JsonInputKind, PlannerJsonRecordReader, json_value_kind,
    json_value_to_object, widen_json_kind,
};
use super::readers::{
    arrow_type_to_sql, planner_parquet_range_error, validate_planner_object_range,
};
use super::table_function::bind_json_table_function;
use super::{
    LogicalJoinCondition, LogicalJoinType, LogicalPlan, PlanError, READ_CSV_HEADER_SAMPLE_BYTES,
    PLANNER_JSON_RECORD_LIMIT_BYTES, ScalarExpr, ScopeEntry, ScopeStack, bind_from,
};
use crate::catalog::{InMemoryCatalog, TableMeta};

fn text_lit(value: impl Into<String>) -> ScalarExpr {
    ScalarExpr::Literal {
        value: Value::Text(value.into()),
        data_type: DataType::Text { max_len: None },
    }
}

fn text_array(values: &[String]) -> ScalarExpr {
    ScalarExpr::Literal {
        value: Value::Array {
            element_type: DataType::Text { max_len: None },
            elements: values.iter().cloned().map(Value::Text).collect(),
        },
        data_type: DataType::Array(Box::new(DataType::Text { max_len: None })),
    }
}

fn planner_test_catalog() -> InMemoryCatalog {
    let users = Schema::new([
        Field::required("id", DataType::Int32),
        Field::nullable("name", DataType::Text { max_len: None }),
    ])
    .expect("users schema");
    let orders = Schema::new([
        Field::required("id", DataType::Int32),
        Field::required("user_id", DataType::Int32),
    ])
    .expect("orders schema");
    let mut catalog = InMemoryCatalog::new();
    catalog.register("users", TableMeta::new(users.clone()));
    catalog.register("orders", TableMeta::new(orders));
    catalog.register("pg_class", TableMeta::with_schema_name("pg_catalog", users));
    catalog
}

fn parse_from(sql: &str) -> Vec<TableRef> {
    match Parser::new(sql).parse_statement().expect(sql) {
        Statement::Select(select) => select.from,
        other => panic!("expected select, got {other:?}"),
    }
}

fn bind_from_sql(sql: &str) -> (LogicalPlan, Vec<ScopeEntry>) {
    let catalog = planner_test_catalog();
    let from = parse_from(sql);
    bind_from(&from, &catalog, &[], &mut ScopeStack::new()).expect(sql)
}

#[test]
fn local_path_specs_globs_and_mixing_errors_are_explicit() {
    let dir = tempfile::tempdir().expect("tempdir");
    let a = dir.path().join("a.csv");
    let b = dir.path().join("b.csv");
    std::fs::write(&a, "id\n1\n").expect("write a");
    std::fs::write(&b, "id\n2\n").expect("write b");

    assert!(contains_wildcard("*.csv"));
    assert!(wildcard_match("?.csv", "a.csv"));
    assert!(!wildcard_match("?.csv", "ab.csv"));

    let pattern = dir.path().join("*.csv").display().to_string();
    let expanded = expand_file_path_specs("read_csv", &[pattern]).expect("expand glob");
    assert_eq!(expanded, vec![a, b]);

    let mixed =
        path_specs_use_object_store("read_csv", &["s3://bucket/a.csv".into(), "b.csv".into()])
            .expect_err("mixed path specs rejected");
    assert!(
        mixed
            .to_string()
            .contains("cannot mix local and object-store paths"),
        "{mixed}"
    );
}

#[test]
#[allow(clippy::too_many_lines)]
fn bind_from_covers_table_ref_families_and_join_scope_shapes() {
    let (empty, empty_scope) =
        bind_from(&[], &planner_test_catalog(), &[], &mut ScopeStack::new()).expect("empty");
    assert!(matches!(empty, LogicalPlan::Empty { .. }));
    assert!(empty_scope.is_empty());

    let (system_scan, system_scope) = bind_from_sql("SELECT * FROM pg_catalog.pg_class AS c");
    assert_eq!(system_scan.schema().field_at(0).name, "id");
    assert_eq!(system_scope[0].qualifier, "c");

    let catalog = planner_test_catalog();
    let cte_schema =
        Schema::new([Field::required("cte_id", DataType::Int64)]).expect("cte schema");
    let cte_from = parse_from("SELECT * FROM latest AS l");
    let (cte_scan, cte_scope) = bind_from(
        &cte_from,
        &catalog,
        &[("latest".to_owned(), cte_schema.clone())],
        &mut ScopeStack::new(),
    )
    .expect("cte from");
    assert_eq!(cte_scan.schema(), &cte_schema);
    assert_eq!(cte_scope[0].qualifier, "l");

    let (subquery, subquery_scope) =
        bind_from_sql("SELECT * FROM (SELECT id, name FROM users) AS q(user_id, username)");
    assert_eq!(subquery.schema().field_at(0).name, "user_id");
    assert_eq!(subquery_scope[1].field.name, "username");

    for (sql, expected_fields) in [
        (
            "SELECT * FROM generate_series(1, 3) AS g",
            vec![("generate_series", DataType::Int64)],
        ),
        (
            "SELECT * FROM unnest([[1, 2], [3, 4]]) AS u",
            vec![("unnest", DataType::Int32)],
        ),
        (
            "SELECT * FROM json_each(jsonb '{\"a\":1}') AS j",
            vec![
                ("key", DataType::Text { max_len: None }),
                ("value", DataType::Jsonb),
            ],
        ),
        (
            "SELECT * FROM jsonb_path_query(jsonb '{\"a\":1}', '$.a') AS p",
            vec![("value", DataType::Jsonb)],
        ),
        (
            "SELECT * FROM sniff_csv('/tmp/no-read-needed.csv') AS sniff",
            vec![
                ("Delimiter", DataType::Text { max_len: None }),
                ("Quote", DataType::Text { max_len: None }),
                ("Escape", DataType::Text { max_len: None }),
                ("NewLineDelimiter", DataType::Text { max_len: None }),
                ("SkipRows", DataType::Int64),
                ("HasHeader", DataType::Bool),
                ("Columns", DataType::Text { max_len: None }),
                ("DateFormat", DataType::Text { max_len: None }),
                ("TimestampFormat", DataType::Text { max_len: None }),
                ("UserArguments", DataType::Text { max_len: None }),
                ("Prompt", DataType::Text { max_len: None }),
            ],
        ),
        (
            "SELECT * FROM JSON_TABLE(\
             jsonb '[{\"id\":1,\"name\":\"Ada\"}]', \
             '$[*]' COLUMNS (\
                 ord FOR ORDINALITY, \
                 id bigint PATH '$.id', \
                 name text, \
                 has_name boolean EXISTS PATH '$.name'\
             )) jt",
            vec![
                ("ord", DataType::Int64),
                ("id", DataType::Int64),
                ("name", DataType::Text { max_len: None }),
                ("has_name", DataType::Bool),
            ],
        ),
        (
            "SELECT * FROM XMLTABLE(\
             '/root/item' PASSING XML '<root><item id=\"1\"><name>Ada</name></item></root>' \
             COLUMNS (\
                 ord FOR ORDINALITY, \
                 id bigint PATH '@id', \
                 name text PATH 'name/text()'\
             )) xt",
            vec![
                ("ord", DataType::Int64),
                ("id", DataType::Int64),
                ("name", DataType::Text { max_len: None }),
            ],
        ),
    ] {
        let (plan, scope) = bind_from_sql(sql);
        assert_eq!(plan.schema().len(), expected_fields.len(), "{sql}");
        assert_eq!(scope.len(), expected_fields.len(), "{sql}");
        for (idx, (name, data_type)) in expected_fields.into_iter().enumerate() {
            assert_eq!(plan.schema().field_at(idx).name, name, "{sql}");
            assert_eq!(plan.schema().field_at(idx).data_type, data_type, "{sql}");
        }
    }

    let (joined, joined_scope) =
        bind_from_sql("SELECT * FROM users u LEFT JOIN orders o ON u.id = o.user_id");
    let LogicalPlan::Join {
        join_type,
        condition,
        schema,
        ..
    } = joined
    else {
        panic!("expected join");
    };
    assert_eq!(join_type, LogicalJoinType::LeftOuter);
    assert!(matches!(condition, LogicalJoinCondition::On(_)));
    assert!(schema.field_at(2).nullable, "right side left-join nullable");
    assert_eq!(joined_scope[2].qualifier, "o");

    let (using_join, _) = bind_from_sql("SELECT * FROM users FULL JOIN orders USING (id)");
    let LogicalPlan::Join {
        join_type,
        condition,
        schema,
        ..
    } = using_join
    else {
        panic!("expected using join");
    };
    assert_eq!(join_type, LogicalJoinType::FullOuter);
    assert!(matches!(condition, LogicalJoinCondition::Using(_)));
    assert_eq!(schema.field_at(0).name, "id");
    assert!(schema.field_at(0).nullable);

    for sql in [
        "SELECT * FROM missing_table",
        "SELECT * FROM no_such_function()",
        "SELECT * FROM unnest(1)",
        "SELECT * FROM json_each()",
        "SELECT * FROM jsonb_path_query(jsonb '{\"a\":1}')",
        "SELECT * FROM users u JOIN orders o ON u.id",
        "SELECT * FROM users u JOIN orders o USING (missing)",
    ] {
        let catalog = planner_test_catalog();
        let from = parse_from(sql);
        let Err(err) = bind_from(&from, &catalog, &[], &mut ScopeStack::new()) else {
            panic!("expected bind error for {sql}");
        };
        assert!(
            matches!(
                err,
                PlanError::TableNotFound(_)
                    | PlanError::ColumnNotFound(_)
                    | PlanError::TypeMismatch(_)
                    | PlanError::NotSupported(_)
            ),
            "{sql}: {err:?}"
        );
    }
}

#[test]
fn path_argument_reader_accepts_text_arrays_and_rejects_bad_shapes() {
    let paths = vec!["a.csv".to_owned(), "b.csv".to_owned()];
    assert_eq!(
        read_file_path_specs("read_csv", &text_array(&paths)).expect("text array"),
        paths
    );
    let bad_array = ScalarExpr::Literal {
        value: Value::Array {
            element_type: DataType::Int32,
            elements: vec![Value::Int32(1)],
        },
        data_type: DataType::Array(Box::new(DataType::Int32)),
    };
    assert!(read_file_path_specs("read_csv", &bad_array).is_err());
    assert!(validate_read_csv_reject_path_arg(&text_lit("rejects.csv")).is_ok());
    assert!(validate_read_csv_reject_path_arg(&text_lit("")).is_err());
    assert!(validate_read_csv_reject_path_arg(&text_lit("s3://bucket/rejects.csv")).is_err());
    let bad_scalar = ScalarExpr::Literal {
        value: Value::Int32(1),
        data_type: DataType::Int32,
    };
    assert!(read_file_path_specs("read_csv", &bad_scalar).is_err());
    assert!(expand_file_path_specs("read_csv", &[]).is_err());
    assert!(expand_file_paths("read_csv", "/").is_err());
}

#[test]
fn csv_header_inference_handles_delimiters_and_multiline_quotes() {
    assert_eq!(
        infer_csv_header_from_first_record("comma.csv", "id,name\n1,alice\n")
            .expect("comma header"),
        vec!["id".to_owned(), "name".to_owned()]
    );
    assert_eq!(
        infer_csv_header_from_first_record("semi.csv", "id;name;score\n1;a;2\n")
            .expect("semicolon header"),
        vec!["id".to_owned(), "name".to_owned(), "score".to_owned()]
    );
    assert_eq!(
        infer_csv_header_from_first_record("quoted.csv", "\"id\npart\",name\n1,a\n")
            .expect("multiline header"),
        vec!["id\npart".to_owned(), "name".to_owned()]
    );
    assert!(infer_csv_header_from_first_record("bad.csv", ",name\n").is_err());
    assert!(
        first_csv_record_with_options(
            "empty.csv",
            "",
            CsvParseOptions {
                delimiter: ',',
                quote: Some('"'),
                escape: Some('"'),
            },
        )
        .is_err()
    );
    assert!(infer_csv_header_from_first_record("multi.csv", "a,b\n1,2\n").is_ok());
}

#[test]
fn csv_header_fallback_rejects_oversized_first_record() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("large-header.csv");
    fs::write(
        &path,
        format!(
            "{}\n1\n",
            "a".repeat(
                usize::try_from(READ_CSV_HEADER_SAMPLE_BYTES)
                    .expect("CSV header sample limit fits usize")
                    + 1,
            )
        ),
    )
    .expect("write csv");

    let err = read_csv_header_from_first_record(&[path.display().to_string()])
        .expect_err("oversized first record rejected");

    assert!(err.to_string().contains("exceeds sample limit"), "{err}");
}

#[test]
fn json_record_readers_stream_objects_and_report_malformed_rows() {
    let mut ndjson = PlannerJsonRecordReader::new(
        JsonInputKind::Ndjson,
        Box::new(Cursor::new(b"\n{\"id\":1}\n{\"id\":2}\n".to_vec())),
    );
    assert_eq!(
        ndjson
            .next_text("read_ndjson", "rows.ndjson")
            .expect("first ndjson"),
        Some((2, "{\"id\":1}".to_owned()))
    );
    assert_eq!(
        ndjson
            .next_text("read_ndjson", "rows.ndjson")
            .expect("second ndjson"),
        Some((3, "{\"id\":2}".to_owned()))
    );
    assert_eq!(
        ndjson
            .next_text("read_ndjson", "rows.ndjson")
            .expect("eof ndjson"),
        None
    );

    let mut json = PlannerJsonRecordReader::new(
        JsonInputKind::Json,
        Box::new(Cursor::new(br#"[{"id":1},{"id":2}]"#.to_vec())),
    );
    assert_eq!(
        json.next_text("read_json", "rows.json")
            .expect("first json row"),
        Some((1, "{\"id\":1}".to_owned()))
    );
    assert_eq!(
        json.next_text("read_json", "rows.json")
            .expect("second json row"),
        Some((2, "{\"id\":2}".to_owned()))
    );
    assert_eq!(
        json.next_text("read_json", "rows.json").expect("json eof"),
        None
    );

    assert!(json_value_to_object("read_json", "rows.json", 1, json!(["not-object"])).is_err());

    let mut object = PlannerJsonRecordReader::new(
        JsonInputKind::Json,
        Box::new(Cursor::new(br#"{"id":{"nested":true}}"#.to_vec())),
    );
    assert_eq!(
        object
            .next_text("read_json", "object.json")
            .expect("single object"),
        Some((1, "{\"id\":{\"nested\":true}}".to_owned()))
    );
    assert_eq!(
        object.next_text("read_json", "object.json").expect("done"),
        None
    );

    let mut scalar = PlannerJsonRecordReader::new(
        JsonInputKind::Json,
        Box::new(Cursor::new(b"42".to_vec())),
    );
    assert!(scalar.next_text("read_json", "scalar.json").is_err());

    let mut bad_array = PlannerJsonRecordReader::new(
        JsonInputKind::Json,
        Box::new(Cursor::new(b"[1]".to_vec())),
    );
    assert!(bad_array.next_text("read_json", "bad-array.json").is_err());

    let mut truncated_array = PlannerJsonRecordReader::new(
        JsonInputKind::Json,
        Box::new(Cursor::new(br#"[{"id":1}"#.to_vec())),
    );
    assert_eq!(
        truncated_array
            .next_text("read_json", "truncated-array.json")
            .expect("first object"),
        Some((1, "{\"id\":1}".to_owned()))
    );
    assert!(
        truncated_array
            .next_text("read_json", "truncated-array.json")
            .is_err()
    );

    let mut truncated_object = PlannerJsonRecordReader::new(
        JsonInputKind::Json,
        Box::new(Cursor::new(br#"{"id":"unterminated"#.to_vec())),
    );
    assert!(
        truncated_object
            .next_text("read_json", "truncated-object.json")
            .is_err()
    );
}

#[test]
fn json_record_readers_reject_oversized_records() {
    let payload = "x".repeat(PLANNER_JSON_RECORD_LIMIT_BYTES);
    let object = format!("{{\"payload\":\"{payload}\"}}");
    let mut ndjson = PlannerJsonRecordReader::new(
        JsonInputKind::Ndjson,
        Box::new(Cursor::new(format!("{object}\n").into_bytes())),
    );
    assert_json_record_limit(
        ndjson.next_text("read_ndjson", "large.ndjson"),
        "read_ndjson",
    );

    let mut json = PlannerJsonRecordReader::new(
        JsonInputKind::Json,
        Box::new(Cursor::new(format!("[{object}]").into_bytes())),
    );
    assert_json_record_limit(json.next_text("read_json", "large.json"), "read_json");
}

fn assert_json_record_limit(result: Result<Option<(usize, String)>, PlanError>, name: &str) {
    match result {
        Err(err) => assert!(err.to_string().contains("exceeds record limit"), "{err}"),
        Ok(_) => panic!("{name} oversized record accepted"),
    }
}

#[test]
fn json_field_accumulator_widens_and_marks_missing_values_nullable() {
    let mut acc = JsonFieldAccumulator::default();
    let first = JsonMap::from_iter([
        ("id".to_owned(), json!(1)),
        ("flag".to_owned(), json!(true)),
    ]);
    let second = JsonMap::from_iter([
        ("id".to_owned(), json!(2.5)),
        ("note".to_owned(), json!(null)),
    ]);
    acc.observe("read_json", &first).expect("first row");
    acc.observe("read_json", &second).expect("second row");
    let fields = acc.finish();

    let id = fields.iter().find(|f| f.name == "id").expect("id field");
    assert_eq!(id.data_type, DataType::Float64);
    assert!(!id.nullable);
    let flag = fields
        .iter()
        .find(|f| f.name == "flag")
        .expect("flag field");
    assert!(flag.nullable, "missing in second row marks nullable");
    let note = fields
        .iter()
        .find(|f| f.name == "note")
        .expect("note field");
    assert_eq!(note.data_type, DataType::Text { max_len: None });
    assert!(note.nullable);

    let empty_name = JsonMap::from_iter([("".to_owned(), json!(1))]);
    let mut bad = JsonFieldAccumulator::default();
    assert!(bad.observe("read_json", &empty_name).is_err());

    assert_eq!(json_value_kind(&json!(null)), JsonColumnKind::Unknown);
    assert_eq!(json_value_kind(&json!(true)), JsonColumnKind::Bool);
    assert_eq!(json_value_kind(&json!(1)), JsonColumnKind::Int64);
    assert_eq!(json_value_kind(&json!(1.5)), JsonColumnKind::Float64);
    assert_eq!(json_value_kind(&json!("x")), JsonColumnKind::Text);
    assert_eq!(
        widen_json_kind(JsonColumnKind::Bool, JsonColumnKind::Int64),
        JsonColumnKind::Text
    );
}

#[test]
fn arrow_and_range_helpers_cover_supported_and_error_paths() {
    for (arrow, sql) in [
        (ArrowDataType::Boolean, DataType::Bool),
        (ArrowDataType::Int32, DataType::Int32),
        (ArrowDataType::Int64, DataType::Int64),
        (ArrowDataType::Float32, DataType::Float32),
        (ArrowDataType::Float64, DataType::Float64),
        (ArrowDataType::Utf8, DataType::Text { max_len: None }),
        (ArrowDataType::LargeUtf8, DataType::Text { max_len: None }),
    ] {
        assert_eq!(arrow_type_to_sql("read_arrow", &arrow).unwrap(), sql);
    }
    assert!(arrow_type_to_sql("read_arrow", &ArrowDataType::Date32).is_err());

    assert_eq!(validate_planner_object_range("obj", 2, 3, 10).unwrap(), 3);
    assert!(validate_planner_object_range("obj", 8, 3, 10).is_err());
    assert!(validate_planner_object_range("obj", u64::MAX, 1, u64::MAX).is_err());

    let err = planner_parquet_range_error("bad range".to_owned());
    assert!(err.to_string().contains("bad range"));
}

#[test]
fn local_csv_and_json_table_functions_infer_scoped_schemas() {
    let dir = tempfile::tempdir().expect("tempdir");
    let csv = dir.path().join("rows.csv");
    std::fs::write(&csv, "id,name\n1,alice\n").expect("write csv");
    let json = dir.path().join("rows.json");
    std::fs::write(&json, r#"[{"id":1,"name":"alice"},{"id":2}]"#).expect("write json");
    let ndjson = dir.path().join("rows.ndjson");
    std::fs::write(&ndjson, "{\"id\":1}\n{\"id\":2,\"ok\":true}\n").expect("write ndjson");

    let (csv_schema, csv_scope) =
        bind_read_csv_table_function(&[text_lit(csv.display().to_string())], "c")
            .expect("csv schema");
    assert_eq!(csv_schema.field_at(0).name, "id");
    assert_eq!(csv_schema.field_at(1).name, "name");
    assert_eq!(csv_schema.field_at(2).name, "_filename");
    assert_eq!(csv_scope[0].qualifier, "c");

    let (json_schema, json_scope) = bind_json_table_function(
        "read_json",
        JsonInputKind::Json,
        &[text_lit(json.display().to_string())],
        "j",
    )
    .expect("json schema");
    assert_eq!(json_scope[0].qualifier, "j");
    assert!(json_schema.find("name").expect("name").1.nullable);

    let (ndjson_schema, _) = bind_json_table_function(
        "read_ndjson",
        JsonInputKind::Ndjson,
        &[text_lit(ndjson.display().to_string())],
        "n",
    )
    .expect("ndjson schema");
    assert_eq!(
        ndjson_schema.find("ok").expect("ok").1.data_type,
        DataType::Bool
    );

    let (sniff_schema, sniff_scope) =
        bind_sniff_csv_table_function(&[text_lit(csv.display().to_string())], "sniff")
            .expect("sniff schema");
    assert_eq!(sniff_schema.field_at(0).name, "Delimiter");
    assert_eq!(sniff_scope[0].qualifier, "sniff");

    assert!(bind_read_csv_table_function(&[], "c").is_err());
    assert!(
        bind_read_csv_table_function(&[text_lit(csv.display().to_string()), text_lit("")], "c")
            .is_err()
    );
    assert!(bind_json_table_function("read_json", JsonInputKind::Json, &[], "j").is_err());
    assert!(bind_sniff_csv_table_function(&[], "sniff").is_err());
    assert!(
        bind_sniff_csv_table_function(
            &[ScalarExpr::Literal {
                value: Value::Int32(1),
                data_type: DataType::Int32,
            }],
            "sniff",
        )
        .is_err()
    );
}
