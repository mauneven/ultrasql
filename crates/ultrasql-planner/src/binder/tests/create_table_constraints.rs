//! Binder tests for CREATE TABLE constraints, defaults, identity/serial
//! columns, sequences, exclusion/foreign keys and COMMENT ON.

use ultrasql_core::{DataType, Field, Schema, Value};
use ultrasql_parser::Parser;
use ultrasql_parser::ast::BinaryOp;

use super::super::*;
use super::*;
use crate::catalog::InMemoryCatalog;
use crate::LogicalIndexMethod;

#[test]
fn binds_unnest_table_function_from_text_array() {
    let cat = InMemoryCatalog::new();
    let plan = parse_and_bind(
        "SELECT * FROM unnest(string_to_array('red,green', ','))",
        &cat,
    )
    .expect("bind ok");
    assert_eq!(
        plan.schema().fields()[0].data_type,
        DataType::Text { max_len: None }
    );
}

#[test]
fn binds_unnest_multidimensional_array_to_base_element_type() {
    let cat = InMemoryCatalog::new();
    let plan = parse_and_bind("SELECT * FROM unnest([[1, 2], [3, 4]])", &cat).expect("bind ok");
    assert_eq!(plan.schema().fields()[0].data_type, DataType::Int32);
}

#[test]
fn binds_multidimensional_array_literal_and_numeric_common_type() {
    let cat = InMemoryCatalog::new();
    let plan =
        parse_and_bind("SELECT [[1, 2], [3, 4]], [1::smallint, 2::bigint]", &cat).expect("bind ok");
    let LogicalPlan::Project { exprs, schema, .. } = &plan else {
        panic!("expected Project, got {plan:?}");
    };
    let matrix_type = DataType::Array(Box::new(DataType::Array(Box::new(DataType::Int32))));
    assert_eq!(schema.field_at(0).data_type, matrix_type);
    let ScalarExpr::Literal { value, data_type } = &exprs[0].0 else {
        panic!("expected matrix literal, got {:?}", exprs[0].0);
    };
    assert_eq!(data_type, &matrix_type);
    assert_eq!(
        value,
        &Value::Array {
            element_type: DataType::Array(Box::new(DataType::Int32)),
            elements: vec![
                Value::Array {
                    element_type: DataType::Int32,
                    elements: vec![Value::Int32(1), Value::Int32(2)]
                },
                Value::Array {
                    element_type: DataType::Int32,
                    elements: vec![Value::Int32(3), Value::Int32(4)]
                }
            ]
        }
    );

    let widened_type = DataType::Array(Box::new(DataType::Int64));
    assert_eq!(schema.field_at(1).data_type, widened_type);
    let ScalarExpr::Literal { value, data_type } = &exprs[1].0 else {
        panic!("expected widened literal, got {:?}", exprs[1].0);
    };
    assert_eq!(data_type, &widened_type);
    assert_eq!(
        value,
        &Value::Array {
            element_type: DataType::Int64,
            elements: vec![Value::Int64(1), Value::Int64(2)]
        }
    );
}

#[test]
fn binds_array_slice_as_array_typed_function() {
    let cat = InMemoryCatalog::new();
    let plan = parse_and_bind("SELECT [10, 20, 30, 40][2:3]", &cat).expect("bind ok");
    let LogicalPlan::Project { exprs, schema, .. } = &plan else {
        panic!("expected Project, got {plan:?}");
    };
    assert_eq!(
        schema.field_at(0).data_type,
        DataType::Array(Box::new(DataType::Int32))
    );
    let ScalarExpr::FunctionCall {
        name, data_type, ..
    } = &exprs[0].0
    else {
        panic!("expected array slice function, got {:?}", exprs[0].0);
    };
    assert_eq!(name, "__ultrasql_array_slice");
    assert_eq!(data_type, &DataType::Array(Box::new(DataType::Int32)));
}

#[test]
fn binds_array_append_prepend_remove_return_array_type() {
    let cat = InMemoryCatalog::new();
    let plan = parse_and_bind(
        "SELECT array_append([1, 2], 3), \
                array_prepend(0, [1, 2]), \
                array_remove([1, 2, 1], 1)",
        &cat,
    )
    .expect("bind ok");
    let schema = plan.schema();
    for idx in 0..3 {
        assert_eq!(
            schema.field_at(idx).data_type,
            DataType::Array(Box::new(DataType::Int32))
        );
    }
}

#[test]
fn binds_array_metadata_functions_return_scalar_types() {
    let cat = InMemoryCatalog::new();
    let plan = parse_and_bind(
        "SELECT cardinality([[1, 2], [3, 4]]), \
                array_ndims([[1, 2], [3, 4]]), \
                array_lower([[1, 2], [3, 4]], 1), \
                array_upper([[1, 2], [3, 4]], 2), \
                array_dims([[1, 2], [3, 4]])",
        &cat,
    )
    .expect("bind ok");
    let schema = plan.schema();
    for idx in 0..4 {
        assert_eq!(schema.field_at(idx).data_type, DataType::Int32);
    }
    assert_eq!(
        schema.field_at(4).data_type,
        DataType::Text { max_len: None }
    );
}

#[test]
fn binds_array_replace_positions_trim_return_array_types() {
    let cat = InMemoryCatalog::new();
    let plan = parse_and_bind(
        "SELECT array_replace([1, 2, 1], 1, 9), \
                array_positions([1, 2, 1], 1), \
                trim_array([1, 2, 3], 1)",
        &cat,
    )
    .expect("bind ok");
    let schema = plan.schema();
    assert_eq!(
        schema.field_at(0).data_type,
        DataType::Array(Box::new(DataType::Int32))
    );
    assert_eq!(
        schema.field_at(1).data_type,
        DataType::Array(Box::new(DataType::Int32))
    );
    assert_eq!(
        schema.field_at(2).data_type,
        DataType::Array(Box::new(DataType::Int32))
    );
}

#[test]
fn rejects_ragged_multidimensional_array_literal() {
    let cat = InMemoryCatalog::new();
    let err = parse_and_bind("SELECT [[1, 2], [3]]", &cat).unwrap_err();
    assert!(matches!(err, PlanError::TypeMismatch(_)), "got {err:?}");
}

#[test]
fn binds_json_table_declared_columns() {
    let cat = InMemoryCatalog::new();
    let plan = parse_and_bind(
        "SELECT * FROM JSON_TABLE(\
         jsonb '[{\"id\":1,\"name\":\"Ada\"}]', \
         '$[*]' COLUMNS (\
             ord FOR ORDINALITY, \
             id bigint PATH '$.id', \
             name text, \
             has_score boolean EXISTS PATH '$.score'\
         )) jt",
        &cat,
    )
    .expect("bind json_table");

    assert_eq!(plan.schema().field_at(0).name, "ord");
    assert_eq!(plan.schema().field_at(0).data_type, DataType::Int64);
    assert_eq!(plan.schema().field_at(1).name, "id");
    assert_eq!(plan.schema().field_at(1).data_type, DataType::Int64);
    assert_eq!(plan.schema().field_at(2).name, "name");
    assert_eq!(
        plan.schema().field_at(2).data_type,
        DataType::Text { max_len: None }
    );
    assert_eq!(plan.schema().field_at(3).name, "has_score");
    assert_eq!(plan.schema().field_at(3).data_type, DataType::Bool);
}

#[test]
fn binds_create_table_primary_key_implies_not_null() {
    let cat = InMemoryCatalog::new();
    let plan = parse_and_bind("CREATE TABLE t (id INT PRIMARY KEY)", &cat).expect("bind ok");
    let LogicalPlan::CreateTable { columns, .. } = plan else {
        panic!("expected CreateTable");
    };
    assert!(!columns.fields()[0].nullable);
}

#[test]
fn binds_create_table_generated_stored_expression() {
    let cat = InMemoryCatalog::new();
    let plan = parse_and_bind(
        "CREATE TABLE t (a INT, b INT GENERATED ALWAYS AS (a + 1) STORED)",
        &cat,
    )
    .expect("bind ok");
    let LogicalPlan::CreateTable {
        generated_stored, ..
    } = plan
    else {
        panic!("expected CreateTable");
    };
    assert!(generated_stored[0].is_none());
    assert!(matches!(
        generated_stored[1],
        Some(ScalarExpr::Binary { .. })
    ));
}

#[test]
fn binds_create_table_duplicate_column_rejected() {
    let cat = InMemoryCatalog::new();
    let err = parse_and_bind("CREATE TABLE t (id INT, id INT)", &cat).unwrap_err();
    assert!(
        matches!(err, PlanError::DuplicateColumn(ref c) if c == "id"),
        "got {err:?}"
    );
}

#[test]
fn binds_create_table_existing_relation_rejected() {
    let cat = users_catalog();
    let err = parse_and_bind("CREATE TABLE users (id INT)", &cat).unwrap_err();
    assert!(
        matches!(err, PlanError::DuplicateTable(ref t) if t == "users"),
        "got {err:?}"
    );
}

#[test]
fn binds_create_table_if_not_exists_skips_existence_check() {
    let cat = users_catalog();
    let plan = parse_and_bind("CREATE TABLE IF NOT EXISTS users (id INT)", &cat).expect("bind ok");
    let LogicalPlan::CreateTable {
        if_not_exists,
        table_name,
        ..
    } = plan
    else {
        panic!("expected CreateTable");
    };
    assert!(if_not_exists);
    assert_eq!(table_name, "users");
}

#[test]
fn binds_create_table_with_qualified_namespace() {
    let cat = InMemoryCatalog::new();
    let plan = parse_and_bind("CREATE TABLE my_ns.events (id INT)", &cat).expect("bind ok");
    let LogicalPlan::CreateTable {
        table_name,
        namespace,
        ..
    } = plan
    else {
        panic!("expected CreateTable");
    };
    assert_eq!(namespace, "my_ns");
    assert_eq!(table_name, "events");
}

#[test]
fn binds_create_table_defaults_checks_and_unique_constraints() {
    let cat = InMemoryCatalog::new();
    let plan = parse_and_bind(
        "CREATE TABLE t (id INT DEFAULT 7 CHECK (id > 0), v INT, UNIQUE (id))",
        &cat,
    )
    .expect("bind ok");
    let LogicalPlan::CreateTable {
        defaults,
        checks,
        unique_constraints,
        ..
    } = plan
    else {
        panic!("expected CreateTable");
    };
    assert!(defaults[0].is_some());
    assert_eq!(checks.len(), 1);
    assert_eq!(unique_constraints.len(), 1);
    assert_eq!(unique_constraints[0].columns, vec![0]);
}

#[test]
fn binds_timestamp_default_now_with_timestamp_type() {
    let cat = InMemoryCatalog::new();
    let plan = parse_and_bind(
        "CREATE TABLE t (installed_on TIMESTAMP NOT NULL DEFAULT now())",
        &cat,
    )
    .expect("bind ok");
    let LogicalPlan::CreateTable { defaults, .. } = plan else {
        panic!("expected CreateTable");
    };
    let Some(ScalarExpr::FunctionCall {
        name, data_type, ..
    }) = defaults[0].as_ref()
    else {
        panic!("expected function default");
    };
    assert_eq!(name, "now");
    assert_eq!(*data_type, DataType::Timestamp);
}

#[test]
fn binds_serial_column_as_required_with_sequence_default() {
    let cat = InMemoryCatalog::new();
    let plan = parse_and_bind(
        "CREATE TABLE t (id SERIAL, v BIGSERIAL, s SMALLSERIAL)",
        &cat,
    )
    .expect("bind ok");
    let LogicalPlan::CreateTable {
        columns,
        sequence_defaults,
        ..
    } = plan
    else {
        panic!("expected CreateTable");
    };
    assert_eq!(columns.field_at(0).data_type, DataType::Int32);
    assert_eq!(columns.field_at(1).data_type, DataType::Int64);
    assert_eq!(columns.field_at(2).data_type, DataType::Int16);
    assert!(!columns.field_at(0).nullable);
    assert_eq!(sequence_defaults[0].as_deref(), Some("t_id_seq"));
    assert_eq!(sequence_defaults[1].as_deref(), Some("t_v_seq"));
    assert_eq!(sequence_defaults[2].as_deref(), Some("t_s_seq"));
}

#[test]
fn binds_identity_column_as_sequence_backed_required_column() {
    let cat = InMemoryCatalog::new();
    let plan = parse_and_bind(
        "CREATE TABLE t (id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 10 INCREMENT BY 5))",
        &cat,
    )
    .expect("bind ok");
    let LogicalPlan::CreateTable {
        columns,
        sequence_defaults,
        sequence_options,
        identity_always,
        ..
    } = plan
    else {
        panic!("expected CreateTable");
    };
    assert_eq!(columns.field_at(0).data_type, DataType::Int64);
    assert!(!columns.field_at(0).nullable);
    assert_eq!(sequence_defaults[0].as_deref(), Some("t_id_seq"));
    assert!(identity_always[0]);
    let opts = sequence_options[0].expect("identity sequence options");
    assert_eq!(opts.start, 10);
    assert_eq!(opts.increment, 5);
}

#[test]
fn binds_create_table_foreign_key_constraints() {
    let cat = users_catalog();
    let plan = parse_and_bind(
        "CREATE TABLE child (user_id INT REFERENCES users(id), CONSTRAINT child_user_fk FOREIGN KEY (user_id) REFERENCES users(id))",
        &cat,
    )
    .expect("foreign keys bind");
    let LogicalPlan::CreateTable { foreign_keys, .. } = plan else {
        panic!("expected CreateTable");
    };
    assert_eq!(foreign_keys.len(), 2);
    assert_eq!(foreign_keys[0].columns, vec![0]);
    assert_eq!(foreign_keys[0].target_table, "users");
    assert_eq!(foreign_keys[0].target_columns, vec![0]);
    assert_eq!(foreign_keys[1].name, "child_user_fk");
}

#[test]
fn binds_deferrable_foreign_key_flags() {
    let cat = users_catalog();
    let plan = parse_and_bind(
        "CREATE TABLE child (user_id INT REFERENCES users(id) DEFERRABLE INITIALLY DEFERRED)",
        &cat,
    )
    .expect("foreign key binds");
    let LogicalPlan::CreateTable { foreign_keys, .. } = plan else {
        panic!("expected CreateTable");
    };
    assert_eq!(foreign_keys.len(), 1);
    assert!(foreign_keys[0].deferrable);
    assert!(foreign_keys[0].initially_deferred);
}

#[test]
fn binds_create_table_exclusion_constraints() {
    let cat = InMemoryCatalog::new();
    let plan = parse_and_bind(
        "CREATE TABLE bookings (room INT, during INT4RANGE, \
         EXCLUDE USING gist (room WITH =, during WITH &&))",
        &cat,
    )
    .expect("exclusion constraint binds");
    let LogicalPlan::CreateTable {
        columns,
        exclusion_constraints,
        ..
    } = plan
    else {
        panic!("expected CreateTable");
    };
    assert_eq!(
        columns.field_at(1).data_type,
        DataType::Range(ultrasql_core::RangeType::Int4)
    );
    assert_eq!(exclusion_constraints.len(), 1);
    assert_eq!(exclusion_constraints[0].method, LogicalIndexMethod::Gist);
    assert_eq!(exclusion_constraints[0].elements[0].column, 0);
    assert_eq!(exclusion_constraints[0].elements[0].op, BinaryOp::Eq);
    assert_eq!(exclusion_constraints[0].elements[1].column, 1);
    assert_eq!(exclusion_constraints[0].elements[1].op, BinaryOp::Overlap);
}

#[test]
fn binds_sequence_ddl_options() {
    let cat = InMemoryCatalog::new();
    let create = parse_and_bind(
        "CREATE SEQUENCE IF NOT EXISTS s START WITH 10 INCREMENT BY 5 MINVALUE 1 MAXVALUE 100 CACHE 4 CYCLE",
        &cat,
    )
    .expect("create sequence binds");
    let LogicalPlan::CreateSequence {
        sequence_name,
        namespace,
        options,
        if_not_exists,
        ..
    } = create
    else {
        panic!("expected CreateSequence");
    };
    assert_eq!(sequence_name, "s");
    assert_eq!(namespace, "public");
    assert!(if_not_exists);
    assert_eq!(options.start, 10);
    assert_eq!(options.increment, 5);
    assert_eq!(options.min, Some(1));
    assert_eq!(options.max, Some(100));
    assert_eq!(options.cache, 4);
    assert!(options.cycle);

    let alter = parse_and_bind("ALTER SEQUENCE s START WITH 50 RESTART WITH 7", &cat)
        .expect("alter sequence binds");
    let LogicalPlan::AlterSequence { options, .. } = alter else {
        panic!("expected AlterSequence");
    };
    assert_eq!(options.start, Some(50));
    assert_eq!(options.restart, Some(Some(7)));
}

#[test]
fn binds_descending_sequence_default_start_to_maxvalue() {
    let cat = InMemoryCatalog::new();
    let plan = parse_and_bind("CREATE SEQUENCE s INCREMENT BY -1", &cat)
        .expect("descending sequence binds");
    let LogicalPlan::CreateSequence { options, .. } = plan else {
        panic!("expected CreateSequence");
    };
    assert_eq!(options.start, -1);
    assert_eq!(options.increment, -1);
}

#[test]
fn binds_create_sequence_rejects_restart() {
    let cat = InMemoryCatalog::new();
    let err = parse_and_bind("CREATE SEQUENCE s RESTART", &cat).unwrap_err();
    assert!(matches!(err, PlanError::NotSupported(_)), "got {err:?}");
}

#[test]
fn binds_create_table_rejects_unsupported_column_type() {
    let cat = InMemoryCatalog::new();
    let err = parse_and_bind("CREATE TABLE t (id XMLTYPE)", &cat).unwrap_err();
    assert!(matches!(err, PlanError::NotSupported(_)), "got {err:?}");
}

#[test]
fn binds_create_table_persistent_catalog_via_snapshot_adapter() {
    // CatalogSnapshot from ultrasql-catalog implements `Catalog`,
    // so the binder can consume a persistent snapshot directly
    // (the seam the server uses to bind against PersistentCatalog).
    use ultrasql_catalog::TableEntry;
    let snap = ultrasql_catalog::CatalogSnapshot {
        tables: {
            let mut m = std::collections::HashMap::new();
            let schema = Schema::new([Field::required("id", DataType::Int32)]).expect("schema ok");
            m.insert(
                "products".to_string(),
                TableEntry::new(ultrasql_core::Oid::new(100), "products", "public", schema),
            );
            m
        },
        tables_by_oid: std::collections::HashMap::new(),
        indexes: std::collections::HashMap::new(),
        indexes_by_table: std::collections::HashMap::new(),
        enum_types: std::collections::HashMap::new(),
        enum_types_by_oid: std::collections::HashMap::new(),
        composite_types: std::collections::HashMap::new(),
        composite_types_by_oid: std::collections::HashMap::new(),
        domain_types: std::collections::HashMap::new(),
        domain_types_by_oid: std::collections::HashMap::new(),
        constraints: std::collections::HashMap::new(),
        descriptions: std::collections::HashMap::new(),
        statistics: std::collections::HashMap::new(),
        statistic_ext: std::collections::HashMap::new(),
    };
    // Creating an already-existing relation through the snapshot
    // adapter surfaces DuplicateTable, proving the binder reaches
    // the snapshot.
    let stmt = Parser::new("CREATE TABLE products (id INT)")
        .parse_statement()
        .expect("parse ok");
    let err = bind(&stmt, &snap).unwrap_err();
    assert!(
        matches!(err, PlanError::DuplicateTable(ref t) if t == "products"),
        "got {err:?}"
    );
}

#[test]
fn binds_comment_on_table_and_column() {
    let cat = users_catalog();
    let table = parse_and_bind("COMMENT ON TABLE users IS 'hello'", &cat).expect("table comment");
    let LogicalPlan::Comment {
        target, comment, ..
    } = table
    else {
        panic!("expected comment plan");
    };
    assert_eq!(comment.as_deref(), Some("hello"));
    assert_eq!(
        target,
        crate::plan::LogicalCommentTarget::Table {
            table: "users".to_owned()
        }
    );

    let column =
        parse_and_bind("COMMENT ON COLUMN users.name IS NULL", &cat).expect("column comment");
    let LogicalPlan::Comment {
        target, comment, ..
    } = column
    else {
        panic!("expected comment plan");
    };
    assert!(comment.is_none());
    assert_eq!(
        target,
        crate::plan::LogicalCommentTarget::Column {
            table: "users".to_owned(),
            column: "name".to_owned(),
            attnum: 2,
        }
    );

    let index =
        parse_and_bind("COMMENT ON INDEX users_name_idx IS 'idx'", &cat).expect("index comment");
    let LogicalPlan::Comment { target, .. } = index else {
        panic!("expected comment plan");
    };
    assert_eq!(
        target,
        crate::plan::LogicalCommentTarget::Index {
            index: "users_name_idx".to_owned(),
            namespace: None,
        }
    );

    let qualified_index = parse_and_bind("COMMENT ON INDEX app.users_name_idx IS 'idx'", &cat)
        .expect("qualified index comment");
    let LogicalPlan::Comment { target, .. } = qualified_index else {
        panic!("expected comment plan");
    };
    assert_eq!(
        target,
        crate::plan::LogicalCommentTarget::Index {
            index: "users_name_idx".to_owned(),
            namespace: Some("app".to_owned()),
        }
    );
}
