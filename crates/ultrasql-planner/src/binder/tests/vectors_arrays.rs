//! Binder tests for array and vector column types, vector distance/metric
//! functions, array functions and JSON_TABLE.

use ultrasql_core::{DataType, Field, Schema};
use ultrasql_parser::ast::BinaryOp;

use super::*;
use crate::catalog::{InMemoryCatalog, TableMeta};

#[test]
fn binds_create_table_array_column_types() {
    let cat = InMemoryCatalog::new();
    let plan = parse_and_bind("CREATE TABLE t (ids INT[], tags TEXT[])", &cat).expect("bind ok");
    let LogicalPlan::CreateTable { columns, .. } = plan else {
        panic!("expected CreateTable");
    };
    assert_eq!(
        columns.fields()[0].data_type,
        DataType::Array(Box::new(DataType::Int32))
    );
    assert_eq!(
        columns.fields()[1].data_type,
        DataType::Array(Box::new(DataType::Text { max_len: None }))
    );
}

#[test]
fn binds_create_table_jsonb_column_type() {
    let cat = InMemoryCatalog::new();
    let plan = parse_and_bind("CREATE TABLE t (doc JSONB)", &cat).expect("bind ok");
    let LogicalPlan::CreateTable { columns, .. } = plan else {
        panic!("expected CreateTable");
    };
    assert_eq!(columns.fields()[0].data_type, DataType::Jsonb);
}

#[test]
fn binds_create_table_xml_column_type() {
    let cat = InMemoryCatalog::new();
    let plan = parse_and_bind("CREATE TABLE t (doc XML)", &cat).expect("bind ok");
    let LogicalPlan::CreateTable { columns, .. } = plan else {
        panic!("expected CreateTable");
    };
    assert_eq!(columns.fields()[0].data_type, DataType::Xml);
}

#[test]
fn binds_create_table_vector_column_type() {
    let cat = InMemoryCatalog::new();
    let plan = parse_and_bind("CREATE TABLE t (embedding VECTOR(1536))", &cat).expect("bind ok");
    let LogicalPlan::CreateTable { columns, .. } = plan else {
        panic!("expected CreateTable");
    };
    assert_eq!(
        columns.fields()[0].data_type,
        DataType::Vector { dims: Some(1536) }
    );
}

#[test]
fn binds_create_table_vector_family_column_types() {
    let cat = InMemoryCatalog::new();
    let plan = parse_and_bind(
        "CREATE TABLE t (h HALFVEC(3), s SPARSEVEC(5), b BITVEC(8))",
        &cat,
    )
    .expect("bind ok");
    let LogicalPlan::CreateTable { columns, .. } = plan else {
        panic!("expected CreateTable");
    };
    assert_eq!(
        columns.fields()[0].data_type,
        DataType::HalfVec { dims: Some(3) }
    );
    assert_eq!(
        columns.fields()[1].data_type,
        DataType::SparseVec { dims: Some(5) }
    );
    assert_eq!(
        columns.fields()[2].data_type,
        DataType::BitVec { dims: Some(8) }
    );
}

#[test]
fn binds_create_materialized_view_derives_and_aliases_columns() {
    let cat = users_catalog();
    let plan = parse_and_bind(
        "CREATE MATERIALIZED VIEW IF NOT EXISTS user_mv (user_id, username) AS \
         SELECT id, name FROM users",
        &cat,
    )
    .expect("bind ok");
    let LogicalPlan::CreateMaterializedView {
        table_name,
        namespace,
        columns,
        source,
        if_not_exists,
        schema,
    } = plan
    else {
        panic!("expected CreateMaterializedView");
    };
    assert_eq!(table_name, "user_mv");
    assert_eq!(namespace, "public");
    assert!(if_not_exists);
    assert_eq!(schema, Schema::empty());
    assert_eq!(columns.fields()[0].name, "user_id");
    assert_eq!(columns.fields()[0].data_type, DataType::Int32);
    assert_eq!(columns.fields()[1].name, "username");
    assert_eq!(
        columns.fields()[1].data_type,
        DataType::Text { max_len: None }
    );
    assert!(matches!(*source, LogicalPlan::Project { .. }));
}

#[test]
fn binds_create_view_derives_aliases_and_preserves_source_sql() {
    let cat = users_catalog();
    let plan = parse_and_bind(
        "CREATE VIEW user_v (user_id, username) AS SELECT id, name FROM users",
        &cat,
    )
    .expect("bind ok");
    let LogicalPlan::CreateView {
        table_name,
        namespace,
        columns,
        source,
        source_sql,
        or_replace,
        schema,
    } = plan
    else {
        panic!("expected CreateView");
    };
    assert_eq!(table_name, "user_v");
    assert_eq!(namespace, "public");
    assert!(!or_replace);
    assert_eq!(schema, Schema::empty());
    assert_eq!(source_sql, "SELECT id, name FROM users");
    assert_eq!(columns.fields()[0].name, "user_id");
    assert_eq!(columns.fields()[0].data_type, DataType::Int32);
    assert_eq!(columns.fields()[1].name, "username");
    assert!(matches!(*source, LogicalPlan::Project { .. }));
}

#[test]
fn binds_create_table_rejects_zero_dimensional_vector() {
    let cat = InMemoryCatalog::new();
    let err = parse_and_bind("CREATE TABLE t (embedding VECTOR(0))", &cat).unwrap_err();
    assert!(matches!(err, PlanError::TypeMismatch(_)), "got {err:?}");
}

#[test]
fn binds_create_table_rejects_zero_dimensional_vector_family() {
    let cat = InMemoryCatalog::new();
    for sql in [
        "CREATE TABLE t (embedding HALFVEC(0))",
        "CREATE TABLE t (embedding SPARSEVEC(0))",
        "CREATE TABLE t (embedding BITVEC(0))",
    ] {
        let err = parse_and_bind(sql, &cat).unwrap_err();
        assert!(matches!(err, PlanError::TypeMismatch(_)), "got {err:?}");
    }
}

#[test]
fn binds_vector_distance_expression_as_float64() {
    let cat = embeddings_catalog();
    let plan =
        parse_and_bind("SELECT embedding <-> '[1,2,4]' FROM embeddings", &cat).expect("bind ok");
    let LogicalPlan::Project { exprs, schema, .. } = &plan else {
        panic!("expected Project, got {plan:?}");
    };
    assert_eq!(exprs[0].0.data_type(), DataType::Float64);
    assert_eq!(schema.field_at(0).data_type, DataType::Float64);
    let ScalarExpr::Binary { op, right, .. } = &exprs[0].0 else {
        panic!("expected vector distance binary expression");
    };
    assert_eq!(*op, BinaryOp::VectorL2Distance);
    assert_eq!(
        right.data_type(),
        DataType::Vector { dims: Some(3) },
        "string literal should coerce to vector(3)"
    );
}

#[test]
fn binds_vector_distance_rejects_dimension_mismatch() {
    let cat = embeddings_catalog();
    let err = parse_and_bind("SELECT embedding <-> '[1,2]' FROM embeddings", &cat).unwrap_err();
    assert!(matches!(err, PlanError::TypeMismatch(_)), "got {err:?}");
}

#[test]
fn rejects_unsupported_vector_comparison_operators_explicitly() {
    let cat = embeddings_catalog();
    for (op, sql) in [
        ("=", "SELECT embedding = VECTOR '[1,2,3]' FROM embeddings"),
        ("<>", "SELECT embedding <> VECTOR '[1,2,3]' FROM embeddings"),
        ("<", "SELECT embedding < VECTOR '[1,2,3]' FROM embeddings"),
        ("<=", "SELECT embedding <= VECTOR '[1,2,3]' FROM embeddings"),
        (">", "SELECT embedding > VECTOR '[1,2,3]' FROM embeddings"),
        (">=", "SELECT embedding >= VECTOR '[1,2,3]' FROM embeddings"),
    ] {
        let err = parse_and_bind(sql, &cat).unwrap_err();
        let PlanError::TypeMismatch(message) = err else {
            panic!("expected TypeMismatch for {op}, got {err:?}");
        };
        assert!(
            message.contains("vector comparison operator"),
            "{op} should name vector comparison, got {message}"
        );
        assert!(
            message.contains(op),
            "{op} should appear in diagnostic, got {message}"
        );
    }
}

#[test]
fn rejects_vector_comparison_against_non_vector_explicitly() {
    let cat = embeddings_catalog();
    let err = parse_and_bind("SELECT embedding = 7 FROM embeddings", &cat).unwrap_err();
    let PlanError::TypeMismatch(message) = err else {
        panic!("expected TypeMismatch, got {err:?}");
    };
    assert!(
        message.contains("vector comparison operator ="),
        "diagnostic should name vector comparison, got {message}"
    );
}

#[test]
fn rejects_between_on_vector_with_comparison_diagnostic() {
    let cat = embeddings_catalog();
    let err = parse_and_bind(
        "SELECT embedding BETWEEN VECTOR '[1,2,3]' AND VECTOR '[4,5,6]' FROM embeddings",
        &cat,
    )
    .unwrap_err();
    let PlanError::TypeMismatch(message) = err else {
        panic!("expected TypeMismatch, got {err:?}");
    };
    assert!(
        message.contains("vector comparison operator >="),
        "BETWEEN should fail through rewritten comparison, got {message}"
    );
}

#[test]
fn binds_vector_inner_product_functions_as_float64() {
    let cat = embeddings_catalog();
    let plan = parse_and_bind(
        "SELECT inner_product(embedding, VECTOR '[4,5,6]'), \
                dot_product(embedding, VECTOR '[4,5,6]') \
         FROM embeddings",
        &cat,
    )
    .expect("bind ok");
    let LogicalPlan::Project { exprs, schema, .. } = &plan else {
        panic!("expected Project, got {plan:?}");
    };
    assert_eq!(exprs[0].0.data_type(), DataType::Float64);
    assert_eq!(exprs[1].0.data_type(), DataType::Float64);
    assert_eq!(schema.field_at(0).data_type, DataType::Float64);
    assert_eq!(schema.field_at(1).data_type, DataType::Float64);
}

#[test]
fn binds_vector_scalar_functions_with_pgvector_return_types() {
    let cat = embeddings_catalog();
    let plan = parse_and_bind(
        "SELECT l2_distance(embedding, VECTOR '[1,2,4]'), \
                cosine_distance(embedding, VECTOR '[3,-6,3]'), \
                dot_product(embedding, VECTOR '[4,5,6]'), \
                inner_product(embedding, VECTOR '[4,5,6]'), \
                l1_distance(embedding, VECTOR '[3,2,-1]'), \
                vector_norm(embedding), \
                vector_dims(embedding) \
         FROM embeddings",
        &cat,
    )
    .expect("bind ok");
    let LogicalPlan::Project { exprs, schema, .. } = &plan else {
        panic!("expected Project, got {plan:?}");
    };
    assert_eq!(exprs[0].0.data_type(), DataType::Float64);
    assert_eq!(exprs[1].0.data_type(), DataType::Float64);
    assert_eq!(exprs[2].0.data_type(), DataType::Float64);
    assert_eq!(exprs[3].0.data_type(), DataType::Float64);
    assert_eq!(exprs[4].0.data_type(), DataType::Float64);
    assert_eq!(exprs[5].0.data_type(), DataType::Float64);
    assert_eq!(exprs[6].0.data_type(), DataType::Int32);
    assert_eq!(schema.field_at(0).data_type, DataType::Float64);
    assert_eq!(schema.field_at(1).data_type, DataType::Float64);
    assert_eq!(schema.field_at(2).data_type, DataType::Float64);
    assert_eq!(schema.field_at(3).data_type, DataType::Float64);
    assert_eq!(schema.field_at(4).data_type, DataType::Float64);
    assert_eq!(schema.field_at(5).data_type, DataType::Float64);
    assert_eq!(schema.field_at(6).data_type, DataType::Int32);
}

#[test]
fn binds_halfvec_and_sparsevec_pgvector_metric_functions() {
    let cat = InMemoryCatalog::new();
    for sql in [
        "SELECT l2_distance(HALFVEC(3) '[1,2,3]', HALFVEC(3) '[1,2,4]')",
        "SELECT cosine_distance(SPARSEVEC(5) '{1:1,3:2}/5', SPARSEVEC(5) '{1:2,4:3}/5')",
        "SELECT inner_product(HALFVEC(3) '[1,2,3]', HALFVEC(3) '[4,5,6]')",
        "SELECT l1_distance(SPARSEVEC(5) '{1:1}/5', SPARSEVEC(5) '{5:2}/5')",
        "SELECT vector_norm(HALFVEC(2) '[3,4]')",
        "SELECT l2_norm(SPARSEVEC(4) '{1:3,4:4}/4')",
    ] {
        let plan = parse_and_bind(sql, &cat).expect(sql);
        let LogicalPlan::Project { exprs, schema, .. } = &plan else {
            panic!("expected Project, got {plan:?}");
        };
        assert_eq!(exprs[0].0.data_type(), DataType::Float64, "{sql}");
        assert_eq!(schema.field_at(0).data_type, DataType::Float64, "{sql}");
    }
}

#[test]
fn rejects_bitvec_float_metric_operators() {
    let cat = InMemoryCatalog::new();
    for sql in [
        "SELECT BITVEC(4) '1010' <-> BITVEC(4) '0101'",
        "SELECT l2_distance(BITVEC(4) '1010', BITVEC(4) '0101')",
        "SELECT vector_norm(BITVEC(4) '1010')",
    ] {
        let err = parse_and_bind(sql, &cat).unwrap_err();
        let PlanError::TypeMismatch(message) = err else {
            panic!("expected TypeMismatch for {sql}, got {err:?}");
        };
        assert!(
            message.contains("vector") || message.contains("metric"),
            "diagnostic should mention vector metric, got {message}"
        );
    }
}

#[test]
fn binds_vector_typed_literal_projection() {
    let cat = InMemoryCatalog::new();
    let plan = parse_and_bind("SELECT VECTOR '[1,2,3]'", &cat).expect("bind ok");
    let LogicalPlan::Project { exprs, schema, .. } = &plan else {
        panic!("expected Project, got {plan:?}");
    };
    assert_eq!(
        schema.field_at(0).data_type,
        DataType::Vector { dims: Some(3) }
    );
    let ScalarExpr::Literal { value, data_type } = &exprs[0].0 else {
        panic!("expected vector literal");
    };
    assert_eq!(*value, ultrasql_core::Value::Vector(vec![1.0, 2.0, 3.0]));
    assert_eq!(*data_type, DataType::Vector { dims: Some(3) });
}

#[test]
fn binds_vector_cast_with_dimension_modifier() {
    let cat = InMemoryCatalog::new();
    for sql in [
        "SELECT '[1,2,3]'::VECTOR(3)",
        "SELECT CAST('[1,2,3]' AS VECTOR(3))",
    ] {
        let plan = parse_and_bind(sql, &cat).expect("bind ok");
        let LogicalPlan::Project { exprs, schema, .. } = &plan else {
            panic!("expected Project, got {plan:?}");
        };
        assert_eq!(
            schema.field_at(0).data_type,
            DataType::Vector { dims: Some(3) },
            "schema type for {sql}"
        );
        assert!(matches!(
            &exprs[0].0,
            ScalarExpr::Literal {
                value: ultrasql_core::Value::Vector(values),
                data_type: DataType::Vector { dims: Some(3) },
            } if values == &vec![1.0, 2.0, 3.0]
        ));
    }
}

#[test]
fn binds_vector_family_casts_with_dimension_modifiers() {
    let cat = InMemoryCatalog::new();
    for (sql, expected_type) in [
        (
            "SELECT '[1,2,3]'::HALFVEC(3)",
            DataType::HalfVec { dims: Some(3) },
        ),
        (
            "SELECT '{1:1,3:2}/5'::SPARSEVEC(5)",
            DataType::SparseVec { dims: Some(5) },
        ),
        (
            "SELECT '1010'::BITVEC(4)",
            DataType::BitVec { dims: Some(4) },
        ),
    ] {
        let plan = parse_and_bind(sql, &cat).expect("bind ok");
        let LogicalPlan::Project { exprs, schema, .. } = &plan else {
            panic!("expected Project, got {plan:?}");
        };
        assert_eq!(
            schema.field_at(0).data_type,
            expected_type,
            "schema type for {sql}"
        );
        assert_eq!(exprs[0].0.data_type(), expected_type, "expr type for {sql}");
    }
}

#[test]
fn vector_family_casts_reject_dimension_mismatch() {
    let cat = InMemoryCatalog::new();
    for sql in [
        "SELECT '[1,2]'::HALFVEC(3)",
        "SELECT '{1:1}/4'::SPARSEVEC(5)",
        "SELECT '101'::BITVEC(4)",
    ] {
        let err = parse_and_bind(sql, &cat).unwrap_err();
        assert!(matches!(err, PlanError::TypeMismatch(_)), "got {err:?}");
    }
}

#[test]
fn vector_family_distance_rejects_dimension_mismatch() {
    let cat = InMemoryCatalog::new();
    for sql in [
        "SELECT HALFVEC(3) '[1,2,3]' <-> HALFVEC(2) '[1,2]'",
        "SELECT SPARSEVEC(5) '{1:1}/5' <-> SPARSEVEC(4) '{1:1}/4'",
        "SELECT BITVEC(4) '1010' <-> BITVEC(3) '101'",
    ] {
        let err = parse_and_bind(sql, &cat).unwrap_err();
        assert!(matches!(err, PlanError::TypeMismatch(_)), "got {err:?}");
    }
}

#[test]
fn binds_array_type_casts_for_common_element_types() {
    let cat = InMemoryCatalog::new();
    for (sql, expected) in [
        // Text-literal source: parsed element-wise via `Value::parse_array`.
        (
            "SELECT '{1,2,3}'::int[]",
            DataType::Array(Box::new(DataType::Int32)),
        ),
        (
            "SELECT CAST('{a,b}' AS text[])",
            DataType::Array(Box::new(DataType::Text { max_len: None })),
        ),
        (
            // Declared size is ignored, matching PostgreSQL.
            "SELECT '{1,2,3}'::int[3]",
            DataType::Array(Box::new(DataType::Int32)),
        ),
        (
            "SELECT '{1.5,2.5}'::float8[]",
            DataType::Array(Box::new(DataType::Float64)),
        ),
        (
            "SELECT '{1,2}'::bigint[]",
            DataType::Array(Box::new(DataType::Int64)),
        ),
        // ARRAY[...] source cast to the same element type needs no
        // element coercion, so it binds.
        (
            "SELECT ARRAY[1,2]::int[]",
            DataType::Array(Box::new(DataType::Int32)),
        ),
    ] {
        let plan = parse_and_bind(sql, &cat).expect("bind ok");
        let LogicalPlan::Project { schema, .. } = &plan else {
            panic!("expected Project for {sql}, got {plan:?}");
        };
        assert_eq!(
            schema.field_at(0).data_type,
            expected,
            "schema type for {sql}"
        );
    }
}

#[test]
fn array_cast_requiring_element_coercion_is_a_clean_feature_error() {
    // `ARRAY[1,2]::text[]` would need per-element int→text coercion, which
    // the literal-array coercion path does not perform. It must fail cleanly
    // (not a parse error, not a silent subscript misparse).
    let cat = InMemoryCatalog::new();
    let err = parse_and_bind("SELECT ARRAY[1,2]::text[]", &cat)
        .expect_err("element coercion is unsupported");
    assert!(
        matches!(err, PlanError::NotSupported(_)),
        "expected NotSupported, got {err:?}"
    );
}

#[test]
fn non_literal_array_cast_is_a_clean_feature_error() {
    // Casting a non-literal column expression to an array type is not yet
    // supported in the runtime cast path; it must be a clean error, never a
    // parse error or a silent subscript misparse.
    let mut cat = InMemoryCatalog::new();
    cat.register(
        "t",
        TableMeta::new(Schema::new([Field::nullable("id", DataType::Int32)]).expect("schema")),
    );
    let err = parse_and_bind("SELECT id::text[] FROM t", &cat).expect_err("should not bind");
    assert!(
        matches!(err, PlanError::NotSupported(_)),
        "expected NotSupported, got {err:?}"
    );
}
