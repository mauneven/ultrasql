use proptest::prelude::*;
use ultrasql_core::{DataType, Field, Schema, Value};
use ultrasql_parser::Parser;
use ultrasql_parser::ast::BinaryOp;

use super::*;
use crate::catalog::{InMemoryCatalog, TableMeta};
use crate::plan::PipelineMode;
use crate::{
    LogicalIndexMethod, LogicalPrivilegeKind, LogicalPrivilegeObjectKind, LogicalWindowFunc,
};

/// Catalog with a single `users` table: id INT, name TEXT, score FLOAT8.
fn users_catalog() -> InMemoryCatalog {
    let schema = Schema::new([
        Field::required("id", DataType::Int32),
        Field::nullable("name", DataType::Text { max_len: None }),
        Field::nullable("score", DataType::Float64),
    ])
    .expect("schema ok");
    let mut cat = InMemoryCatalog::new();
    cat.register("users", TableMeta::new(schema));
    cat
}

fn app_users_catalog() -> InMemoryCatalog {
    let schema = Schema::new([
        Field::required("id", DataType::Int32),
        Field::nullable("name", DataType::Text { max_len: None }),
    ])
    .expect("schema ok");
    let mut cat = InMemoryCatalog::new();
    cat.register("users", TableMeta::with_schema_name("app", schema));
    cat
}

fn users_index_catalog() -> InMemoryCatalog {
    let mut cat = users_catalog();
    cat.register_index("users_id_idx");
    cat
}

fn embeddings_catalog() -> InMemoryCatalog {
    let schema = Schema::new([
        Field::required("id", DataType::Int32),
        Field::nullable("embedding", DataType::Vector { dims: Some(3) }),
    ])
    .expect("schema ok");
    let mut cat = InMemoryCatalog::new();
    cat.register("embeddings", TableMeta::new(schema));
    cat
}

fn money_catalog() -> InMemoryCatalog {
    let schema = Schema::new([
        Field::required("amount", DataType::Money),
        Field::required("qty", DataType::Int32),
        Field::required(
            "price",
            DataType::Decimal {
                precision: Some(10),
                scale: Some(3),
            },
        ),
    ])
    .expect("schema ok");
    let mut cat = InMemoryCatalog::new();
    cat.register("ledger", TableMeta::new(schema));
    cat
}

fn fact_events_catalog() -> InMemoryCatalog {
    let schema = Schema::new([
        Field::required("tenant_id", DataType::Int32),
        Field::required("bucket", DataType::Int32),
        Field::required("amount", DataType::Int64),
    ])
    .expect("schema ok");
    let mut cat = InMemoryCatalog::new();
    cat.register("fact_events", TableMeta::new(schema));
    cat
}

fn timestamp_catalog() -> InMemoryCatalog {
    let schema = Schema::new([
        Field::required("id", DataType::Int32),
        Field::nullable("locked_at", DataType::Timestamp),
    ])
    .expect("schema ok");
    let mut cat = InMemoryCatalog::new();
    cat.register("locks", TableMeta::new(schema));
    cat
}

fn parse_and_bind(sql: &str, cat: &dyn Catalog) -> Result<LogicalPlan, PlanError> {
    let stmt = Parser::new(sql)
        .parse_statement()
        .expect("test SQL parses cleanly");
    bind(&stmt, cat)
}

fn parse_bind_ok(sql: &str) -> LogicalPlan {
    let cat = users_catalog();
    parse_and_bind(sql, &cat).expect("bind ok")
}

#[test]
fn binds_describe_table_output_schema() {
    let plan = parse_bind_ok("DESCRIBE TABLE users");
    let fields = plan.schema().fields();
    assert_eq!(
        fields
            .iter()
            .map(|field| field.name.as_str())
            .collect::<Vec<_>>(),
        vec![
            "column_name",
            "data_type",
            "nullable",
            "source_schema",
            "source_object",
            "source_kind",
        ]
    );
}

#[test]
fn binds_describe_query_output_schema() {
    let plan = parse_bind_ok("DESCRIBE SELECT id, name FROM users");
    let fields = plan.schema().fields();
    assert_eq!(fields.len(), 6);
    assert_eq!(fields[0].name, "column_name");
    let LogicalPlan::Describe {
        target: LogicalDescribeTarget::Query { query_schema },
        ..
    } = plan
    else {
        panic!("expected Describe query plan");
    };
    assert!(!query_schema.field_at(0).nullable);
    assert!(query_schema.field_at(1).nullable);
}

#[test]
fn describe_missing_object_is_table_not_found() {
    let cat = users_catalog();
    let err = parse_and_bind("DESCRIBE missing_users", &cat).expect_err("missing object");
    assert_eq!(err, PlanError::TableNotFound("missing_users".to_owned()));
}

#[test]
fn describe_view_reports_unsupported_until_view_catalog_metadata_exists() {
    let cat = users_catalog();
    let err = parse_and_bind("DESCRIBE VIEW users", &cat).expect_err("view describe unsupported");
    assert_eq!(
        err,
        PlanError::NotSupported("DESCRIBE VIEW requires view catalog metadata")
    );
}

#[test]
fn binds_checkpoint_as_empty_session_control_plan() {
    let plan = parse_bind_ok("CHECKPOINT");
    assert!(matches!(plan, LogicalPlan::Checkpoint { .. }));
    assert!(plan.schema().is_empty());
}

#[test]
fn binds_explicit_public_dotted_table_name() {
    let schema = Schema::new([Field::required("id", DataType::Int32)]).expect("schema ok");
    let mut cat = InMemoryCatalog::new();
    cat.register("events.log", TableMeta::new(schema));

    let plan = parse_and_bind("SELECT id FROM public.\"events.log\"", &cat).expect("bind ok");
    let LogicalPlan::Project { input, .. } = plan else {
        panic!("expected Project");
    };
    let LogicalPlan::Scan { table, .. } = input.as_ref() else {
        panic!("expected Scan");
    };
    assert_eq!(table, "6:public10:events.log");
}

#[test]
fn binds_column_privilege_specs() {
    let plan = parse_bind_ok("GRANT SELECT(id), UPDATE(name) ON TABLE users TO analyst");
    let LogicalPlan::GrantPrivileges {
        privileges,
        object_kind,
        objects,
        grantees,
        ..
    } = plan
    else {
        panic!("expected GrantPrivileges");
    };
    assert_eq!(object_kind, LogicalPrivilegeObjectKind::Table);
    assert_eq!(objects, vec!["users".to_owned()]);
    assert_eq!(grantees, vec!["analyst".to_owned()]);
    assert_eq!(privileges.len(), 2);
    assert_eq!(privileges[0].kind, LogicalPrivilegeKind::Select);
    assert_eq!(privileges[0].columns, vec!["id".to_owned()]);
    assert_eq!(privileges[1].kind, LogicalPrivilegeKind::Update);
    assert_eq!(privileges[1].columns, vec!["name".to_owned()]);
}

#[test]
fn binds_role_membership_grant_and_set_role() {
    let plan = parse_bind_ok("GRANT App_Group TO App_User WITH ADMIN OPTION");
    let LogicalPlan::GrantRole {
        roles,
        grantees,
        admin_option,
        ..
    } = plan
    else {
        panic!("expected GrantRole");
    };
    assert_eq!(roles, vec!["app_group".to_owned()]);
    assert_eq!(grantees, vec!["app_user".to_owned()]);
    assert!(admin_option);

    let plan = parse_bind_ok("SET ROLE App_Group");
    let LogicalPlan::SetRole { role_name, .. } = plan else {
        panic!("expected SetRole");
    };
    assert_eq!(role_name, Some("app_group".to_owned()));
}

#[test]
fn binds_search_path_list_set_local() {
    let plan = parse_bind_ok("SET LOCAL search_path TO public, \"$user\"");
    let LogicalPlan::SetVariable {
        name,
        action,
        value,
        ..
    } = plan
    else {
        panic!("expected SetVariable");
    };
    assert_eq!(name, "search_path");
    assert_eq!(action, LogicalSetVariableAction::SetLocal);
    assert_eq!(value.as_deref(), Some("public, \"$user\""));
}

#[test]
fn binds_datestyle_list_set() {
    let plan = parse_bind_ok("SET datestyle TO SQL, DMY");
    let LogicalPlan::SetVariable {
        name,
        action,
        value,
        ..
    } = plan
    else {
        panic!("expected SetVariable");
    };
    assert_eq!(name, "datestyle");
    assert_eq!(action, LogicalSetVariableAction::Set);
    assert_eq!(value.as_deref(), Some("sql, dmy"));
}

#[test]
fn rejects_invalid_column_privilege_specs() {
    let cat = users_catalog();
    let err = parse_and_bind("GRANT SELECT(id) ON DATABASE ultrasql TO analyst", &cat)
        .expect_err("column privileges are table-only");
    assert!(matches!(err, PlanError::NotSupported(_)));

    let err = parse_and_bind("GRANT DELETE(id) ON TABLE users TO analyst", &cat)
        .expect_err("DELETE has no column privilege form");
    assert!(matches!(err, PlanError::NotSupported(_)));
}

#[test]
fn binds_alter_default_privileges() {
    let cat = users_catalog();
    let plan = parse_and_bind(
        "ALTER DEFAULT PRIVILEGES FOR ROLE App_Owner IN SCHEMA App \
         GRANT SELECT, INSERT ON TABLES TO Analyst WITH GRANT OPTION",
        &cat,
    )
    .expect("default privilege grant binds");
    let LogicalPlan::AlterDefaultPrivileges {
        target_roles,
        schemas,
        operation,
        privileges,
        object_kind,
        grantees,
        grant_option,
        ..
    } = plan
    else {
        panic!("expected AlterDefaultPrivileges");
    };
    assert_eq!(target_roles, vec!["app_owner".to_owned()]);
    assert_eq!(schemas, vec!["app".to_owned()]);
    assert!(operation.is_grant());
    assert_eq!(object_kind, LogicalPrivilegeObjectKind::Table);
    assert_eq!(grantees, vec!["analyst".to_owned()]);
    assert_eq!(privileges.len(), 2);
    assert_eq!(privileges[0].kind, LogicalPrivilegeKind::Select);
    assert_eq!(privileges[1].kind, LogicalPrivilegeKind::Insert);
    assert!(grant_option);

    let err = parse_and_bind(
        "ALTER DEFAULT PRIVILEGES GRANT SELECT(id) ON TABLES TO analyst",
        &cat,
    )
    .expect_err("default privileges reject column lists");
    assert!(matches!(err, PlanError::NotSupported(_)));
}

#[test]
fn binds_row_to_json_whole_row_alias_to_named_record() {
    let plan = parse_bind_ok("SELECT row_to_json(u) FROM users u WHERE u.id = 1");
    let LogicalPlan::Project { exprs, input, .. } = &plan else {
        panic!("expected Project, got {plan:?}");
    };
    let LogicalPlan::Filter { predicate, .. } = input.as_ref() else {
        panic!("expected Filter input, got {input:?}");
    };
    let ScalarExpr::Binary { left, right, .. } = predicate else {
        panic!("expected binary predicate, got {predicate:?}");
    };
    assert!(
        matches!(
            left.as_ref(),
            ScalarExpr::Column {
                index: 0,
                data_type: DataType::Int32,
                ..
            }
        ),
        "unexpected predicate left: {left:?}"
    );
    assert!(
        matches!(
            right.as_ref(),
            ScalarExpr::Literal {
                value: ultrasql_core::Value::Int32(1),
                ..
            }
        ),
        "unexpected predicate right: {right:?}"
    );
    let ScalarExpr::FunctionCall { name, args, .. } = &exprs[0].0 else {
        panic!("expected row_to_json call, got {:?}", exprs[0].0);
    };
    assert_eq!(name, "row_to_json");
    let [
        ScalarExpr::FunctionCall {
            name: row_name,
            args: row_args,
            data_type: DataType::Record(fields),
        },
    ] = args.as_slice()
    else {
        panic!("expected row constructor record arg, got {args:?}");
    };
    assert_eq!(row_name, "row");
    assert_eq!(
        fields
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>(),
        vec!["id", "name", "score"]
    );
    assert_eq!(row_args.len(), 3);
}

#[test]
fn binds_xml_scalar_functions_with_precise_return_types() {
    let plan = parse_bind_ok(
        "SELECT \
            xml_is_well_formed_document('<root/>'), \
            xml_is_well_formed_content('<a/><b/>'), \
            xpath_exists('/root/item', XML '<root><item/></root>'), \
            xpath('/root/item', XML '<root><item/></root>'), \
            xpath('/r:root', XML '<root xmlns=\"urn:root\"/>', ARRAY[ARRAY['r','urn:root']]), \
            XMLPARSE(DOCUMENT '<root/>'), \
            XMLSERIALIZE(CONTENT XML '<root/>' AS TEXT)",
    );
    let LogicalPlan::Project { exprs, .. } = &plan else {
        panic!("expected Project, got {plan:?}");
    };
    assert_eq!(exprs[0].0.data_type(), DataType::Bool);
    assert_eq!(exprs[1].0.data_type(), DataType::Bool);
    assert_eq!(exprs[2].0.data_type(), DataType::Bool);
    assert_eq!(
        exprs[3].0.data_type(),
        DataType::Array(Box::new(DataType::Xml))
    );
    assert_eq!(
        exprs[4].0.data_type(),
        DataType::Array(Box::new(DataType::Xml))
    );
    assert_eq!(exprs[5].0.data_type(), DataType::Xml);
    assert_eq!(exprs[6].0.data_type(), DataType::Text { max_len: None });
}

#[test]
fn binds_at_time_zone_with_timestamp_return_types() {
    let plan = parse_bind_ok(
        "SELECT \
            TIMESTAMP '2000-07-01 00:00:00' AT TIME ZONE 'America/New_York', \
            TIMESTAMPTZ '2000-07-01 04:00:00+00' AT TIME ZONE 'America/New_York', \
            TIMETZ '04:05:06-05' AT TIME ZONE 'UTC'",
    );
    let LogicalPlan::Project { exprs, .. } = &plan else {
        panic!("expected Project, got {plan:?}");
    };
    assert_eq!(exprs[0].0.data_type(), DataType::TimestampTz);
    assert_eq!(exprs[1].0.data_type(), DataType::Timestamp);
    assert_eq!(exprs[2].0.data_type(), DataType::TimeTz);
}

// -----------------------------------------------------------------------
// INSERT — happy paths
// -----------------------------------------------------------------------

#[test]
fn binds_insert_with_column_list_resolves_indices() {
    let plan = parse_bind_ok("INSERT INTO users (name, score) VALUES ('alice', 1.0)");
    let LogicalPlan::Insert {
        table,
        columns,
        source,
        ..
    } = &plan
    else {
        panic!("expected Insert, got {plan:?}");
    };
    assert_eq!(table, "users");
    // name is index 1, score is index 2
    assert_eq!(columns, &[1_usize, 2_usize]);
    assert!(matches!(source.as_ref(), LogicalPlan::Values { .. }));
}

#[test]
fn binds_insert_default_values() {
    let plan = parse_bind_ok("INSERT INTO users DEFAULT VALUES");
    let LogicalPlan::Insert {
        source, columns, ..
    } = &plan
    else {
        panic!("expected Insert");
    };
    // Columns = all three (all-columns expansion)
    assert_eq!(columns.len(), 3);
    // Source is a Values with one zero-width row.
    let LogicalPlan::Values { rows, .. } = source.as_ref() else {
        panic!("expected Values source");
    };
    assert_eq!(rows.len(), 1);
    assert!(rows[0].is_empty());
}

#[test]
fn binds_insert_with_multi_row_values() {
    let plan =
        parse_bind_ok("INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')");
    let LogicalPlan::Insert { source, .. } = &plan else {
        panic!("expected Insert");
    };
    let LogicalPlan::Values { rows, .. } = source.as_ref() else {
        panic!("expected Values");
    };
    assert_eq!(rows.len(), 3);
    for r in rows {
        assert_eq!(r.len(), 2);
    }
}

#[test]
fn binds_insert_select() {
    // Must use a single-column select (id only) to match column count 1.
    let plan = parse_bind_ok("INSERT INTO users (id) SELECT id FROM users WHERE id > 0");
    let LogicalPlan::Insert {
        columns, source, ..
    } = &plan
    else {
        panic!("expected Insert");
    };
    assert_eq!(columns, &[0_usize]);
    // Source is a bound Select plan.
    assert!(
        matches!(
            source.as_ref(),
            LogicalPlan::Limit { .. }
                | LogicalPlan::Sort { .. }
                | LogicalPlan::Project { .. }
                | LogicalPlan::Filter { .. }
                | LogicalPlan::Scan { .. }
        ),
        "unexpected source: {source:?}"
    );
}

// -----------------------------------------------------------------------
// INSERT — error paths
// -----------------------------------------------------------------------

#[test]
fn binds_insert_rejects_ragged_value_rows() {
    let cat = users_catalog();
    let err = parse_and_bind(
        "INSERT INTO users (id, name) VALUES (1, 'alice', 99.0)",
        &cat,
    )
    .unwrap_err();
    // Row 1 has 3 cells but 2 columns expected.
    assert!(matches!(err, PlanError::TypeMismatch(_)), "got {err:?}");
}

#[test]
fn binds_insert_rejects_unknown_column() {
    let cat = users_catalog();
    let err = parse_and_bind("INSERT INTO users (bogus) VALUES (1)", &cat).unwrap_err();
    assert!(
        matches!(err, PlanError::ColumnNotFound(ref c) if c == "bogus"),
        "got {err:?}"
    );
}

#[test]
fn binds_insert_rejects_arity_mismatch_with_select_source() {
    // Column list has 2 entries, SELECT returns 3 columns.
    let cat = users_catalog();
    let err = parse_and_bind(
        "INSERT INTO users (id, name) SELECT id, name, score FROM users",
        &cat,
    )
    .unwrap_err();
    assert!(matches!(err, PlanError::TypeMismatch(_)), "got {err:?}");
}

// -----------------------------------------------------------------------
// INSERT — ON CONFLICT
// -----------------------------------------------------------------------

#[test]
fn binds_on_conflict_do_nothing() {
    let plan = parse_bind_ok("INSERT INTO users (id) VALUES (1) ON CONFLICT DO NOTHING");
    let LogicalPlan::Insert { on_conflict, .. } = &plan else {
        panic!("expected Insert");
    };
    assert!(matches!(
        on_conflict,
        Some(LogicalOnConflict::DoNothing { target: None })
    ));
}

#[test]
fn binds_on_conflict_do_update_targets() {
    let plan = parse_bind_ok(
        "INSERT INTO users (id, name) VALUES (1, 'x') ON CONFLICT (id) DO UPDATE SET name = 'y'",
    );
    let LogicalPlan::Insert { on_conflict, .. } = &plan else {
        panic!("expected Insert");
    };
    let Some(LogicalOnConflict::DoUpdate {
        target,
        assignments,
        ..
    }) = on_conflict
    else {
        panic!("expected DoUpdate, got {on_conflict:?}");
    };
    // Conflict target: column 'id' is at index 0
    assert_eq!(target.columns, vec![0_usize]);
    // Assignment: name (index 1) = literal 'y'
    assert_eq!(assignments.len(), 1);
    assert_eq!(assignments[0].0, 1);
}

// -----------------------------------------------------------------------
// UPDATE
// -----------------------------------------------------------------------

#[test]
fn binds_update_with_filter_and_assignments() {
    let plan = parse_bind_ok("UPDATE users SET score = 9.5 WHERE id = 1");
    let LogicalPlan::Update {
        table,
        assignments,
        input,
        ..
    } = &plan
    else {
        panic!("expected Update, got {plan:?}");
    };
    assert_eq!(table, "users");
    // score is column index 2
    assert_eq!(assignments.len(), 1);
    assert_eq!(assignments[0].0, 2);
    assert!(matches!(input.as_ref(), LogicalPlan::Filter { .. }));
}

#[test]
fn binds_update_now_assignment_to_timestamp_target() {
    let cat = timestamp_catalog();
    let plan =
        parse_and_bind("UPDATE locks SET locked_at = now() WHERE id = 1", &cat).expect("bind ok");
    let LogicalPlan::Update { assignments, .. } = plan else {
        panic!("expected Update");
    };
    assert_eq!(assignments.len(), 1);
    assert_eq!(assignments[0].0, 1);
    assert_eq!(assignments[0].1.data_type(), DataType::Timestamp);
}

#[test]
fn binds_update_rejects_unknown_target_column() {
    let cat = users_catalog();
    let err = parse_and_bind("UPDATE users SET bogus = 1", &cat).unwrap_err();
    assert!(
        matches!(err, PlanError::ColumnNotFound(ref c) if c == "bogus"),
        "got {err:?}"
    );
}

#[test]
fn binds_update_rejects_duplicate_target_column() {
    let cat = users_catalog();
    // PostgreSQL rejects `UPDATE t SET col=1, col=2` — mirror that.
    let err = parse_and_bind("UPDATE users SET score = 1.0, score = 2.0", &cat).unwrap_err();
    assert!(
        matches!(err, PlanError::DuplicateColumn(ref c) if c == "score"),
        "expected DuplicateColumn(score), got {err:?}"
    );
}

#[test]
fn binder_rejects_update_from_other_table_as_not_supported() {
    let cat = users_catalog();
    let err = parse_and_bind(
        "UPDATE users SET score = 1 FROM users AS u2 WHERE users.id = u2.id",
        &cat,
    )
    .unwrap_err();
    assert!(matches!(err, PlanError::NotSupported(_)), "got {err:?}");
}

// -----------------------------------------------------------------------
// DELETE
// -----------------------------------------------------------------------

#[test]
fn binds_delete_emits_scan_filter_delete() {
    let plan = parse_bind_ok("DELETE FROM users WHERE id = 42");
    let LogicalPlan::Delete { table, input, .. } = &plan else {
        panic!("expected Delete, got {plan:?}");
    };
    assert_eq!(table, "users");
    assert!(matches!(input.as_ref(), LogicalPlan::Filter { .. }));
}

#[test]
fn binder_rejects_delete_using_other_table_as_not_supported() {
    let cat = users_catalog();
    let err = parse_and_bind(
        "DELETE FROM users USING users AS u2 WHERE users.id = u2.id",
        &cat,
    )
    .unwrap_err();
    assert!(matches!(err, PlanError::NotSupported(_)), "got {err:?}");
}

// -----------------------------------------------------------------------
// TRUNCATE
// -----------------------------------------------------------------------

#[test]
fn binds_truncate_validates_table_existence() {
    let plan = parse_bind_ok("TRUNCATE TABLE users");
    let LogicalPlan::Truncate {
        tables,
        restart_identity,
        cascade,
        ..
    } = &plan
    else {
        panic!("expected Truncate, got {plan:?}");
    };
    assert_eq!(tables, &["users"]);
    assert!(!restart_identity);
    assert!(!cascade);
    assert!(plan.schema().is_empty());

    // Unknown table should fail.
    let cat = users_catalog();
    let err = parse_and_bind("TRUNCATE TABLE nope", &cat).unwrap_err();
    assert!(
        matches!(err, PlanError::TableNotFound(ref t) if t == "nope"),
        "got {err:?}"
    );
}

// -----------------------------------------------------------------------
// CREATE TABLE
// -----------------------------------------------------------------------

#[test]
fn binds_create_table_resolves_basic_column_types() {
    let cat = InMemoryCatalog::new();
    let plan = parse_and_bind(
        "CREATE TABLE accounts (id BIGINT NOT NULL, name TEXT, balance FLOAT8)",
        &cat,
    )
    .expect("bind ok");
    let LogicalPlan::CreateTable {
        table_name,
        namespace,
        columns,
        if_not_exists,
        schema,
        ..
    } = plan
    else {
        panic!("expected CreateTable, got other plan");
    };
    assert_eq!(table_name, "accounts");
    assert_eq!(namespace, "public");
    assert!(!if_not_exists);
    assert_eq!(schema, Schema::empty());
    assert_eq!(columns.len(), 3);
    assert_eq!(columns.fields()[0].name, "id");
    assert_eq!(columns.fields()[0].data_type, DataType::Int64);
    assert!(!columns.fields()[0].nullable, "NOT NULL honored");
    assert_eq!(
        columns.fields()[1].data_type,
        DataType::Text { max_len: None }
    );
    assert!(columns.fields()[1].nullable, "no constraint = nullable");
    assert_eq!(columns.fields()[2].data_type, DataType::Float64);
}

#[test]
fn binds_create_type_enum_and_uses_catalog_type_in_create_table() {
    let cat = InMemoryCatalog::new();
    let plan =
        parse_and_bind("CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')", &cat).expect("bind ok");
    let LogicalPlan::CreateTypeEnum {
        type_name,
        namespace,
        labels,
        ..
    } = plan
    else {
        panic!("expected CreateTypeEnum");
    };
    assert_eq!(type_name, "mood");
    assert_eq!(namespace, "public");
    assert_eq!(labels, ["sad", "ok", "happy"]);

    let mut cat = InMemoryCatalog::new();
    let enum_type = DataType::Enum {
        oid: ultrasql_core::Oid::new(42_000),
        name: "mood".into(),
        labels: vec!["sad".to_owned(), "ok".to_owned(), "happy".to_owned()].into(),
    };
    cat.register_type("mood", enum_type.clone());
    let plan =
        parse_and_bind("CREATE TABLE enum_probe (id INT, mood mood)", &cat).expect("bind ok");
    let LogicalPlan::CreateTable { columns, .. } = plan else {
        panic!("expected CreateTable");
    };
    assert_eq!(columns.fields()[1].data_type, enum_type);
}

#[test]
fn binds_create_type_composite_and_uses_catalog_type_in_create_table() {
    let cat = InMemoryCatalog::new();
    let plan = parse_and_bind("CREATE TYPE postal_address AS (street TEXT, zip INT)", &cat)
        .expect("bind ok");
    let LogicalPlan::CreateTypeComposite {
        type_name,
        namespace,
        attributes,
        ..
    } = plan
    else {
        panic!("expected CreateTypeComposite");
    };
    assert_eq!(type_name, "postal_address");
    assert_eq!(namespace, "public");
    assert_eq!(attributes.fields()[0].name, "street");
    assert_eq!(
        attributes.fields()[0].data_type,
        DataType::Text { max_len: None }
    );
    assert_eq!(attributes.fields()[1].name, "zip");
    assert_eq!(attributes.fields()[1].data_type, DataType::Int32);

    let mut cat = InMemoryCatalog::new();
    let composite_type = DataType::Composite {
        oid: ultrasql_core::Oid::new(42_001),
        name: "postal_address".into(),
        fields: vec![
            ("street".to_owned(), DataType::Text { max_len: None }),
            ("zip".to_owned(), DataType::Int32),
        ]
        .into(),
    };
    cat.register_type("postal_address", composite_type.clone());
    let plan = parse_and_bind(
        "CREATE TABLE contact_book (id INT, addr postal_address)",
        &cat,
    )
    .expect("bind ok");
    let LogicalPlan::CreateTable { columns, .. } = plan else {
        panic!("expected CreateTable");
    };
    assert_eq!(columns.fields()[1].data_type, composite_type);
}

#[test]
fn binds_create_domain_and_uses_catalog_type_in_create_table() {
    let cat = InMemoryCatalog::new();
    let plan = parse_and_bind(
        "CREATE DOMAIN positive_int AS INT NOT NULL CHECK (VALUE > 0)",
        &cat,
    )
    .expect("bind ok");
    let LogicalPlan::CreateDomain {
        domain_name,
        namespace,
        base_type,
        not_null,
        checks,
        ..
    } = plan
    else {
        panic!("expected CreateDomain");
    };
    assert_eq!(domain_name, "positive_int");
    assert_eq!(namespace, "public");
    assert_eq!(base_type, DataType::Int32);
    assert!(not_null);
    assert_eq!(checks.len(), 1);

    let mut cat = InMemoryCatalog::new();
    let domain_type = DataType::Domain {
        oid: ultrasql_core::Oid::new(42_002),
        name: "positive_int".into(),
        base_type: Box::new(DataType::Int32),
        not_null: true,
    };
    cat.register_type("positive_int", domain_type.clone());
    let plan = parse_and_bind(
        "CREATE TABLE domain_probe (id INT, score positive_int)",
        &cat,
    )
    .expect("bind ok");
    let LogicalPlan::CreateTable { columns, .. } = plan else {
        panic!("expected CreateTable");
    };
    assert_eq!(columns.fields()[1].data_type, domain_type);
    assert!(!columns.fields()[1].nullable);
}

#[test]
fn binds_create_table_range_partition_column() {
    let cat = InMemoryCatalog::new();
    let plan = parse_and_bind(
        "CREATE TABLE metrics (ts TIMESTAMP NOT NULL, v INT) PARTITION BY RANGE (ts)",
        &cat,
    )
    .expect("bind ok");
    let LogicalPlan::CreateTable { partition, .. } = plan else {
        panic!("expected CreateTable");
    };
    let partition = partition.expect("time partition spec");
    assert_eq!(partition.column, "ts");
    assert_eq!(partition.column_index, 0);
}

#[test]
fn binds_create_table_with_varchar_modifier() {
    let cat = InMemoryCatalog::new();
    let plan = parse_and_bind("CREATE TABLE t (s VARCHAR(255))", &cat).expect("bind ok");
    let LogicalPlan::CreateTable { columns, .. } = plan else {
        panic!("expected CreateTable");
    };
    assert_eq!(
        columns.fields()[0].data_type,
        DataType::Text { max_len: Some(255) }
    );
}

#[test]
fn binds_create_table_numeric_typmods_like_postgres() {
    let cat = InMemoryCatalog::new();
    let plan = parse_and_bind(
        "CREATE TABLE t (a NUMERIC, b NUMERIC(10), c DECIMAL(12,4))",
        &cat,
    )
    .expect("bind ok");
    let LogicalPlan::CreateTable { columns, .. } = plan else {
        panic!("expected CreateTable");
    };

    assert_eq!(
        columns.fields()[0].data_type,
        DataType::Decimal {
            precision: None,
            scale: None
        }
    );
    assert_eq!(
        columns.fields()[1].data_type,
        DataType::Decimal {
            precision: Some(10),
            scale: Some(0)
        }
    );
    assert_eq!(
        columns.fields()[2].data_type,
        DataType::Decimal {
            precision: Some(12),
            scale: Some(4)
        }
    );
}

#[test]
fn rejects_zero_numeric_precision() {
    let cat = InMemoryCatalog::new();
    let err = parse_and_bind("CREATE TABLE t (n NUMERIC(0,2))", &cat).expect_err("must reject");
    let PlanError::TypeMismatch(message) = err else {
        panic!("expected TypeMismatch, got {err:?}");
    };
    assert!(message.contains("NUMERIC precision"));
}

#[test]
fn binds_text_literal_cast_to_numeric_with_literal_scale() {
    let cat = InMemoryCatalog::new();
    let plan = parse_and_bind("SELECT '12.340'::numeric AS amount", &cat).expect("bind ok");
    let LogicalPlan::Project { exprs, schema, .. } = plan else {
        panic!("expected Project");
    };
    assert_eq!(
        schema.field_at(0).data_type,
        DataType::Decimal {
            precision: None,
            scale: Some(3)
        }
    );
    let ScalarExpr::Literal { value, data_type } = &exprs[0].0 else {
        panic!("expected folded decimal literal");
    };
    assert_eq!(
        value,
        &Value::Decimal {
            value: 12_340,
            scale: 3
        }
    );
    assert_eq!(
        data_type,
        &DataType::Decimal {
            precision: None,
            scale: Some(3)
        }
    );
}

#[test]
fn binds_create_table_money_type() {
    let cat = InMemoryCatalog::new();
    let plan = parse_and_bind("CREATE TABLE ledger (amount MONEY)", &cat).expect("bind ok");
    let LogicalPlan::CreateTable { columns, .. } = plan else {
        panic!("expected CreateTable");
    };

    assert_eq!(columns.fields()[0].data_type, DataType::Money);
}

#[test]
fn binds_text_literal_cast_to_money() {
    let cat = InMemoryCatalog::new();
    let plan = parse_and_bind("SELECT '$1,234.56'::money AS amount", &cat).expect("bind ok");
    let LogicalPlan::Project { exprs, schema, .. } = plan else {
        panic!("expected Project");
    };
    assert_eq!(schema.field_at(0).data_type, DataType::Money);
    let ScalarExpr::Literal { value, data_type } = &exprs[0].0 else {
        panic!("expected folded money literal");
    };
    assert_eq!(value, &Value::Money(123_456));
    assert_eq!(data_type, &DataType::Money);
}

#[test]
fn binds_money_addition_and_subtraction() {
    let cat = InMemoryCatalog::new();
    for sql in [
        "SELECT '$1.00'::money + '$2.50'::money",
        "SELECT '$3.00'::money - '$1.25'::money",
    ] {
        let plan = parse_and_bind(sql, &cat).expect("bind ok");
        assert_eq!(plan.schema().field_at(0).data_type, DataType::Money);
    }
}

#[test]
fn binds_money_division_matrix() {
    let cat = InMemoryCatalog::new();
    let ratio = parse_and_bind("SELECT '$5.00'::money / '$2.00'::money", &cat).expect("bind ok");
    assert_eq!(ratio.schema().field_at(0).data_type, DataType::Float64);

    let divided = parse_and_bind("SELECT '$5.01'::money / 2", &cat).expect("bind ok");
    assert_eq!(divided.schema().field_at(0).data_type, DataType::Money);

    let rounded = parse_and_bind("SELECT '$5.01'::money / 2.0::float8", &cat).expect("bind ok");
    assert_eq!(rounded.schema().field_at(0).data_type, DataType::Money);
}

#[test]
fn binds_money_scalar_multiplication() {
    let cat = InMemoryCatalog::new();
    for sql in [
        "SELECT '$1.25'::money * 3",
        "SELECT 3 * '$1.25'::money",
        "SELECT '$1.25'::money * 1.5::float8",
        "SELECT 1.5::float8 * '$1.25'::money",
    ] {
        let plan = parse_and_bind(sql, &cat).expect("bind ok");
        assert_eq!(plan.schema().field_at(0).data_type, DataType::Money);
    }
}

#[test]
fn binds_money_runtime_casts() {
    let cat = money_catalog();
    let plan = parse_and_bind(
        "SELECT amount::numeric, amount::text, qty::money, price::money FROM ledger",
        &cat,
    )
    .expect("bind ok");
    let schema = plan.schema();
    assert_eq!(
        schema.field_at(0).data_type,
        DataType::Decimal {
            precision: None,
            scale: Some(2)
        }
    );
    assert_eq!(
        schema.field_at(1).data_type,
        DataType::Text { max_len: None }
    );
    assert_eq!(schema.field_at(2).data_type, DataType::Money);
    assert_eq!(schema.field_at(3).data_type, DataType::Money);
}

#[test]
fn binds_money_unary_signs() {
    let cat = InMemoryCatalog::new();
    let plan = parse_and_bind("SELECT -('$1.25'::money), +'$2.00'::money", &cat).expect("bind ok");
    let LogicalPlan::Project { exprs, schema, .. } = plan else {
        panic!("expected Project");
    };
    assert_eq!(schema.field_at(0).data_type, DataType::Money);
    assert_eq!(schema.field_at(1).data_type, DataType::Money);
    let ScalarExpr::Literal { value, data_type } = &exprs[0].0 else {
        panic!("expected folded negative money literal");
    };
    assert_eq!(value, &Value::Money(-125));
    assert_eq!(data_type, &DataType::Money);
}

#[test]
fn binds_create_table_char_and_bpchar_types() {
    let cat = InMemoryCatalog::new();
    let plan = parse_and_bind(
        "CREATE TABLE codes (c CHAR(4), b BPCHAR(3), d CHARACTER)",
        &cat,
    )
    .expect("bind ok");
    let LogicalPlan::CreateTable { columns, .. } = plan else {
        panic!("expected CreateTable");
    };

    assert_eq!(
        columns.fields()[0].data_type,
        DataType::Char { len: Some(4) }
    );
    assert_eq!(
        columns.fields()[1].data_type,
        DataType::Char { len: Some(3) }
    );
    assert_eq!(
        columns.fields()[2].data_type,
        DataType::Char { len: Some(1) }
    );
}

#[test]
fn binds_text_literal_cast_to_char_with_padding() {
    let cat = InMemoryCatalog::new();
    let plan = parse_and_bind("SELECT CAST('abcdef' AS CHAR(3)) AS code", &cat).expect("bind ok");
    let LogicalPlan::Project { exprs, schema, .. } = plan else {
        panic!("expected Project");
    };
    assert_eq!(
        schema.field_at(0).data_type,
        DataType::Char { len: Some(3) }
    );
    let ScalarExpr::Literal { value, data_type } = &exprs[0].0 else {
        panic!("expected folded char literal");
    };
    assert_eq!(value, &Value::Char("abc".to_owned()));
    assert_eq!(data_type, &DataType::Char { len: Some(3) });
}

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

// -----------------------------------------------------------------------
// JOIN tests
// -----------------------------------------------------------------------

/// Build a two-table catalog: users (`id` INT, `name` TEXT) and orders (`oid` INT, `user_id` INT).
fn two_table_catalog() -> InMemoryCatalog {
    let users_schema = Schema::new([
        Field::required("id", DataType::Int32),
        Field::nullable("name", DataType::Text { max_len: None }),
    ])
    .expect("schema ok");
    let orders_schema = Schema::new([
        Field::required("oid", DataType::Int32),
        Field::required("user_id", DataType::Int32),
    ])
    .expect("schema ok");
    let mut cat = InMemoryCatalog::new();
    cat.register("users", TableMeta::new(users_schema));
    cat.register("orders", TableMeta::new(orders_schema));
    cat
}

fn duplicate_id_catalog() -> InMemoryCatalog {
    let schema_a = Schema::new([
        Field::required("id", DataType::Int32),
        Field::required("marker", DataType::Int32),
    ])
    .expect("schema ok");
    let schema_b = Schema::new([
        Field::required("id", DataType::Int32),
        Field::required("marker", DataType::Int32),
    ])
    .expect("schema ok");
    let mut cat = InMemoryCatalog::new();
    cat.register("a", TableMeta::new(schema_a));
    cat.register("b", TableMeta::new(schema_b));
    cat
}

#[test]
fn binds_inner_join_with_on_predicate() {
    let cat = two_table_catalog();
    let plan = parse_and_bind(
        "SELECT users.id FROM users INNER JOIN orders ON users.id = orders.user_id",
        &cat,
    )
    .expect("bind ok");
    // The top-level plan has a Project; find the Join underneath.
    fn find_join(plan: &LogicalPlan) -> Option<&LogicalPlan> {
        match plan {
            LogicalPlan::Join { .. } => Some(plan),
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Filter { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => find_join(input),
            _ => None,
        }
    }
    let join = find_join(&plan).expect("should contain a Join node");
    let LogicalPlan::Join {
        join_type,
        condition,
        schema,
        ..
    } = join
    else {
        panic!("expected Join");
    };
    assert_eq!(*join_type, LogicalJoinType::Inner);
    assert!(
        matches!(condition, LogicalJoinCondition::On(_)),
        "expected ON condition"
    );
    // Schema is concatenation: users(id, name) + orders(oid, user_id) = 4
    assert_eq!(schema.len(), 4, "join schema width should be 4");
}

#[test]
fn qualified_join_predicate_resolves_duplicate_right_column() {
    let cat = duplicate_id_catalog();
    let plan = parse_and_bind(
        "SELECT a.id AS aid, b.id AS bid \
         FROM a JOIN b ON a.id = b.id \
         ORDER BY b.id",
        &cat,
    )
    .expect("bind ok");

    let LogicalPlan::Project { input, exprs, .. } = &plan else {
        panic!("expected top Project, got {plan:?}");
    };
    assert!(matches!(&exprs[0].0, ScalarExpr::Column { index: 0, .. }));
    assert!(matches!(&exprs[1].0, ScalarExpr::Column { index: 2, .. }));

    let LogicalPlan::Sort { input, keys } = input.as_ref() else {
        panic!("expected Sort under Project");
    };
    assert!(matches!(&keys[0].expr, ScalarExpr::Column { index: 2, .. }));

    let LogicalPlan::Join { condition, .. } = input.as_ref() else {
        panic!("expected Join under Sort");
    };
    let LogicalJoinCondition::On(ScalarExpr::Binary { left, right, .. }) = condition else {
        panic!("expected binary ON predicate, got {condition:?}");
    };
    assert!(matches!(left.as_ref(), ScalarExpr::Column { index: 0, .. }));
    assert!(matches!(
        right.as_ref(),
        ScalarExpr::Column { index: 2, .. }
    ));
}

#[test]
fn order_by_can_reference_projection_alias() {
    let cat = users_catalog();
    let plan =
        parse_and_bind("SELECT id AS ident FROM users ORDER BY ident DESC", &cat).expect("bind ok");

    let LogicalPlan::Sort { input, keys } = &plan else {
        panic!("expected top Sort over projected alias, got {plan:?}");
    };
    assert_eq!(keys.len(), 1);
    assert!(!keys[0].asc);
    assert!(matches!(
        &keys[0].expr,
        ScalarExpr::Column { index: 0, name, .. } if name == "ident"
    ));
    assert!(
        matches!(input.as_ref(), LogicalPlan::Project { .. }),
        "alias ORDER BY should sort projected rows"
    );
}

#[test]
fn binds_duplicate_unaliased_function_output_labels() {
    let cat = users_catalog();
    let plan = parse_and_bind(
        "SELECT pg_get_expr(1, 1), pg_get_expr(2, 2) FROM users",
        &cat,
    )
    .expect("bind ok");

    let LogicalPlan::Project { schema, .. } = &plan else {
        panic!("expected Project, got {plan:?}");
    };
    assert_eq!(schema.field_at(0).name, "pg_get_expr");
    assert_eq!(schema.field_at(1).name, "pg_get_expr");
}

#[test]
fn qualified_where_predicate_resolves_duplicate_right_column() {
    let cat = duplicate_id_catalog();
    let plan = parse_and_bind("SELECT b.id FROM a, b WHERE a.id = b.id", &cat).expect("bind ok");

    let LogicalPlan::Project { input, exprs, .. } = &plan else {
        panic!("expected top Project, got {plan:?}");
    };
    assert!(matches!(&exprs[0].0, ScalarExpr::Column { index: 2, .. }));

    let LogicalPlan::Filter { input, predicate } = input.as_ref() else {
        panic!("expected Filter under Project");
    };
    let ScalarExpr::Binary { left, right, .. } = predicate else {
        panic!("expected binary WHERE predicate, got {predicate:?}");
    };
    assert!(matches!(left.as_ref(), ScalarExpr::Column { index: 0, .. }));
    assert!(matches!(
        right.as_ref(),
        ScalarExpr::Column { index: 2, .. }
    ));
    assert!(matches!(input.as_ref(), LogicalPlan::Join { .. }));
}

#[test]
fn binds_left_outer_join_makes_right_columns_nullable() {
    let cat = two_table_catalog();
    let plan = parse_and_bind(
        "SELECT users.id FROM users LEFT JOIN orders ON users.id = orders.user_id",
        &cat,
    )
    .expect("bind ok");

    fn find_join(plan: &LogicalPlan) -> Option<&LogicalPlan> {
        match plan {
            LogicalPlan::Join { .. } => Some(plan),
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Filter { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => find_join(input),
            _ => None,
        }
    }
    let join = find_join(&plan).expect("should contain a Join");
    let LogicalPlan::Join {
        join_type, schema, ..
    } = join
    else {
        panic!("expected Join");
    };
    assert_eq!(*join_type, LogicalJoinType::LeftOuter);
    // Left columns (users.id, users.name): id was required, stays required.
    assert!(
        !schema.field_at(0).nullable,
        "left.id should remain required"
    );
    // Right columns (orders.oid, orders.user_id) should be nullable in LEFT JOIN.
    assert!(schema.field_at(2).nullable, "right.oid should be nullable");
    assert!(
        schema.field_at(3).nullable,
        "right.user_id should be nullable"
    );
}

#[test]
fn binds_right_outer_join_makes_left_columns_nullable() {
    let cat = two_table_catalog();
    let plan = parse_and_bind(
        "SELECT users.id FROM users RIGHT JOIN orders ON users.id = orders.user_id",
        &cat,
    )
    .expect("bind ok");

    fn find_join(plan: &LogicalPlan) -> Option<&LogicalPlan> {
        match plan {
            LogicalPlan::Join { .. } => Some(plan),
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Filter { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => find_join(input),
            _ => None,
        }
    }
    let join = find_join(&plan).expect("should contain a Join");
    let LogicalPlan::Join {
        join_type, schema, ..
    } = join
    else {
        panic!("expected Join");
    };
    assert_eq!(*join_type, LogicalJoinType::RightOuter);
    // In RIGHT JOIN: left columns become nullable.
    assert!(
        schema.field_at(0).nullable,
        "left.id should be nullable in RIGHT JOIN"
    );
    // Right columns keep their original nullability (both were required).
    assert!(!schema.field_at(2).nullable, "right.oid stays required");
}

#[test]
fn binds_full_outer_join_makes_both_sides_nullable() {
    let cat = two_table_catalog();
    let plan = parse_and_bind(
        "SELECT users.id FROM users FULL OUTER JOIN orders ON users.id = orders.user_id",
        &cat,
    )
    .expect("bind ok");

    fn find_join(plan: &LogicalPlan) -> Option<&LogicalPlan> {
        match plan {
            LogicalPlan::Join { .. } => Some(plan),
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Filter { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => find_join(input),
            _ => None,
        }
    }
    let join = find_join(&plan).expect("should contain a Join");
    let LogicalPlan::Join {
        join_type, schema, ..
    } = join
    else {
        panic!("expected Join");
    };
    assert_eq!(*join_type, LogicalJoinType::FullOuter);
    // Both sides should be nullable.
    assert!(
        schema.field_at(0).nullable,
        "left.id should be nullable in FULL OUTER JOIN"
    );
    assert!(
        schema.field_at(2).nullable,
        "right.oid should be nullable in FULL OUTER JOIN"
    );
}

#[test]
fn binds_cross_join_has_no_predicate() {
    let cat = two_table_catalog();
    let plan =
        parse_and_bind("SELECT users.id FROM users CROSS JOIN orders", &cat).expect("bind ok");

    fn find_join(plan: &LogicalPlan) -> Option<&LogicalPlan> {
        match plan {
            LogicalPlan::Join { .. } => Some(plan),
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Filter { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => find_join(input),
            _ => None,
        }
    }
    let join = find_join(&plan).expect("should contain a Join");
    let LogicalPlan::Join {
        join_type,
        condition,
        ..
    } = join
    else {
        panic!("expected Join");
    };
    assert_eq!(*join_type, LogicalJoinType::Cross);
    assert!(
        matches!(condition, LogicalJoinCondition::None),
        "cross join should have no condition"
    );
}

fn join_chain_catalog(table_count: usize) -> InMemoryCatalog {
    let mut cat = InMemoryCatalog::new();
    let schema = Schema::new([Field::required("id", DataType::Int32)]).expect("schema ok");
    for idx in 0..table_count {
        cat.register(&format!("t{idx}"), TableMeta::new(schema.clone()));
    }
    cat
}

fn join_chain_sql(table_count: usize) -> String {
    let mut sql = String::from("SELECT t0.id FROM t0");
    for idx in 1..table_count {
        sql.push_str(&format!(" JOIN t{idx} ON t0.id = t{idx}.id"));
    }
    sql
}

#[test]
fn accepts_explicit_join_chain_at_depth_limit() {
    let cat = join_chain_catalog(65);
    let sql = join_chain_sql(65);

    parse_and_bind(&sql, &cat).expect("join depth at planner limit should bind");
}

#[test]
fn rejects_explicit_join_chain_above_depth_limit() {
    let cat = join_chain_catalog(66);
    let sql = join_chain_sql(66);

    let err = parse_and_bind(&sql, &cat).expect_err("join chain should exceed planner limit");

    assert!(
        err.to_string().contains("join depth"),
        "expected join-depth error, got {err:?}"
    );
}

#[test]
fn binds_using_join_folds_to_equality_and_collapses_columns() {
    // Build a catalog where both tables have a column named `id`.
    let schema_a = Schema::new([Field::required("id", DataType::Int32)]).expect("schema ok");
    let schema_b = Schema::new([
        Field::required("id", DataType::Int32),
        Field::nullable("val", DataType::Text { max_len: None }),
    ])
    .expect("schema ok");
    let mut cat = InMemoryCatalog::new();
    cat.register("a", TableMeta::new(schema_a));
    cat.register("b", TableMeta::new(schema_b));

    let plan = parse_and_bind("SELECT a.id FROM a JOIN b USING (id)", &cat).expect("bind ok");

    fn find_join(plan: &LogicalPlan) -> Option<&LogicalPlan> {
        match plan {
            LogicalPlan::Join { .. } => Some(plan),
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Filter { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => find_join(input),
            _ => None,
        }
    }
    let join = find_join(&plan).expect("should contain a Join");
    let LogicalPlan::Join {
        condition, schema, ..
    } = join
    else {
        panic!("expected Join");
    };
    assert!(
        matches!(condition, LogicalJoinCondition::Using(_)),
        "expected USING condition"
    );
    // USING(id) collapses: id once + val = 2 columns (not 3).
    assert_eq!(
        schema.len(),
        2,
        "USING join should collapse the shared column"
    );
}

#[test]
fn binds_natural_join_collapses_shared_columns_without_ambiguous_select() {
    let schema_a = Schema::new([
        Field::required("id", DataType::Int32),
        Field::nullable("left_name", DataType::Text { max_len: None }),
    ])
    .expect("schema ok");
    let schema_b = Schema::new([
        Field::required("id", DataType::Int32),
        Field::nullable("right_name", DataType::Text { max_len: None }),
    ])
    .expect("schema ok");
    let mut cat = InMemoryCatalog::new();
    cat.register("a", TableMeta::new(schema_a));
    cat.register("b", TableMeta::new(schema_b));

    let plan = parse_and_bind(
        "SELECT id, left_name, right_name FROM a NATURAL JOIN b",
        &cat,
    )
    .expect("bind ok");

    fn find_join(plan: &LogicalPlan) -> Option<&LogicalPlan> {
        match plan {
            LogicalPlan::Join { .. } => Some(plan),
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Filter { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => find_join(input),
            _ => None,
        }
    }
    let join = find_join(&plan).expect("should contain a Join");
    let LogicalPlan::Join {
        condition, schema, ..
    } = join
    else {
        panic!("expected Join");
    };
    assert!(
        matches!(condition, LogicalJoinCondition::Using(pairs) if pairs.as_slice() == [(0, 0)]),
        "natural join should bind as USING(id)"
    );
    assert_eq!(schema.len(), 3);
    assert_eq!(schema.field_at(0).name, "id");
    assert_eq!(schema.field_at(1).name, "left_name");
    assert_eq!(schema.field_at(2).name, "right_name");
}

// -----------------------------------------------------------------------
// GROUP BY / aggregate tests
// -----------------------------------------------------------------------

#[test]
fn binds_group_by_emits_aggregate_node() {
    let cat = users_catalog();
    let plan = parse_and_bind("SELECT id, count(*) FROM users GROUP BY id", &cat).expect("bind ok");

    fn find_agg(plan: &LogicalPlan) -> Option<&LogicalPlan> {
        match plan {
            LogicalPlan::Aggregate { .. } => Some(plan),
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Filter { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => find_agg(input),
            _ => None,
        }
    }
    let agg = find_agg(&plan).expect("should contain Aggregate node");
    let LogicalPlan::Aggregate {
        group_by,
        aggregates,
        schema,
        ..
    } = agg
    else {
        panic!("expected Aggregate");
    };
    assert_eq!(group_by.len(), 1, "one GROUP BY key");
    assert_eq!(aggregates.len(), 1, "one aggregate");
    assert_eq!(aggregates[0].func, AggregateFunc::CountStar);
    // Schema: [id, count]
    assert_eq!(schema.len(), 2);
    assert_eq!(schema.field_at(0).name, "id");
    assert_eq!(schema.field_at(1).name, "count");
}

#[test]
fn binds_group_by_scalar_function_projection_alias() {
    let schema = Schema::new([
        Field::required("order_date", DataType::Date),
        Field::required("amount", DataType::Int32),
    ])
    .expect("schema ok");
    let mut cat = InMemoryCatalog::new();
    cat.register("sales", TableMeta::new(schema));

    let plan = parse_and_bind(
        "SELECT EXTRACT(YEAR FROM order_date) AS o_year, SUM(amount) AS revenue \
         FROM sales GROUP BY EXTRACT(YEAR FROM order_date) ORDER BY o_year",
        &cat,
    )
    .expect("bind ok");

    assert_eq!(plan.schema().field_at(0).name, "o_year");
    assert_eq!(plan.schema().field_at(1).name, "revenue");
}

#[test]
fn binds_group_by_column_projection_alias() {
    let cat = users_catalog();
    let plan = parse_and_bind(
        "SELECT id AS ident, COUNT(*) AS row_count FROM users GROUP BY id ORDER BY ident",
        &cat,
    )
    .expect("bind ok");

    assert_eq!(plan.schema().field_at(0).name, "ident");
    assert_eq!(plan.schema().field_at(1).name, "row_count");
}

#[test]
fn binds_count_star() {
    let cat = users_catalog();
    let plan = parse_and_bind("SELECT count(*) FROM users", &cat).expect("bind ok");

    fn find_agg(plan: &LogicalPlan) -> Option<&LogicalPlan> {
        match plan {
            LogicalPlan::Aggregate { .. } => Some(plan),
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Filter { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => find_agg(input),
            _ => None,
        }
    }
    let agg = find_agg(&plan).expect("should contain Aggregate node");
    let LogicalPlan::Aggregate { aggregates, .. } = agg else {
        panic!("expected Aggregate");
    };
    assert_eq!(aggregates.len(), 1);
    assert_eq!(aggregates[0].func, AggregateFunc::CountStar);
    assert!(aggregates[0].arg.is_none(), "count(*) has no argument");
}

#[test]
fn binds_vector_sum_and_avg_with_vector_return_type() {
    let cat = embeddings_catalog();
    let plan = parse_and_bind(
        "SELECT sum(embedding), avg(embedding) FROM embeddings",
        &cat,
    )
    .expect("bind ok");

    fn find_agg(plan: &LogicalPlan) -> Option<&LogicalPlan> {
        match plan {
            LogicalPlan::Aggregate { .. } => Some(plan),
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Filter { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => find_agg(input),
            _ => None,
        }
    }

    let agg = find_agg(&plan).expect("should contain Aggregate node");
    let LogicalPlan::Aggregate { aggregates, .. } = agg else {
        panic!("expected Aggregate");
    };
    assert_eq!(aggregates.len(), 2);
    assert_eq!(aggregates[0].func, AggregateFunc::Sum);
    assert_eq!(aggregates[1].func, AggregateFunc::Avg);
    assert_eq!(aggregates[0].data_type, DataType::Vector { dims: Some(3) });
    assert_eq!(aggregates[1].data_type, DataType::Vector { dims: Some(3) });
}

#[test]
fn binds_having_filters_post_aggregate() {
    let cat = users_catalog();
    let plan = parse_and_bind(
        "SELECT id, count(*) FROM users GROUP BY id HAVING count(*) > 1",
        &cat,
    )
    .expect("bind ok");

    fn find_filter_above_agg(plan: &LogicalPlan) -> bool {
        match plan {
            LogicalPlan::Filter { input, .. } => {
                matches!(input.as_ref(), LogicalPlan::Aggregate { .. })
            }
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => find_filter_above_agg(input),
            _ => false,
        }
    }
    assert!(
        find_filter_above_agg(&plan),
        "should have Filter above Aggregate for HAVING"
    );
}

#[test]
fn binds_decimal_arithmetic_around_aggregate_with_decimal_type() {
    let schema = Schema::new([
        Field::required(
            "price",
            DataType::Decimal {
                precision: Some(15),
                scale: Some(2),
            },
        ),
        Field::required(
            "discount",
            DataType::Decimal {
                precision: Some(15),
                scale: Some(2),
            },
        ),
    ])
    .expect("schema ok");
    let mut cat = InMemoryCatalog::new();
    cat.register("lineitem", TableMeta::new(schema));

    let plan = parse_and_bind(
        "SELECT 100 * SUM(price * (1 - discount)) / SUM(price * (1 - discount)) AS ratio FROM lineitem",
        &cat,
    )
    .expect("bind ok");

    let LogicalPlan::Project { schema, exprs, .. } = &plan else {
        panic!("expected top-level Project, got {plan:?}");
    };
    assert_eq!(
        schema.field_at(0).data_type,
        DataType::Decimal {
            precision: None,
            scale: Some(8)
        }
    );
    assert_eq!(
        exprs[0].0.data_type(),
        DataType::Decimal {
            precision: None,
            scale: Some(8)
        }
    );
}

#[test]
fn binds_coalesce_around_aggregate_projection() {
    let schema = Schema::new([Field::required("amount", DataType::Int32)]).expect("schema ok");
    let mut cat = InMemoryCatalog::new();
    cat.register("sales", TableMeta::new(schema));

    let plan = parse_and_bind("SELECT COALESCE(SUM(amount), 0) AS total FROM sales", &cat)
        .expect("bind ok");

    let LogicalPlan::Project {
        input,
        exprs,
        schema,
    } = &plan
    else {
        panic!("expected top-level Project, got {plan:?}");
    };
    assert_eq!(schema.field_at(0).name, "total");
    assert_eq!(schema.field_at(0).data_type, DataType::Int64);

    let ScalarExpr::FunctionCall {
        name,
        args,
        data_type,
    } = &exprs[0].0
    else {
        panic!("expected coalesce projection, got {:?}", exprs[0].0);
    };
    assert_eq!(name, "coalesce");
    assert_eq!(*data_type, DataType::Int64);
    assert!(matches!(args[0], ScalarExpr::Column { index: 0, .. }));

    let LogicalPlan::Aggregate { aggregates, .. } = input.as_ref() else {
        panic!("expected Aggregate under Project");
    };
    assert_eq!(aggregates.len(), 1);
    assert_eq!(aggregates[0].func, AggregateFunc::Sum);

    let ifnull_plan = parse_and_bind("SELECT IFNULL(SUM(amount), 0) AS total FROM sales", &cat)
        .expect("bind generic scalar wrapper ok");
    assert_eq!(ifnull_plan.schema().field_at(0).data_type, DataType::Int64);
}

#[test]
fn binds_distinct_sum_arguments_to_distinct_aggregate_columns() {
    let schema = Schema::new([
        Field::required("volume", DataType::Float64),
        Field::required("nation", DataType::Text { max_len: None }),
    ])
    .expect("schema ok");
    let mut cat = InMemoryCatalog::new();
    cat.register("lineitem", TableMeta::new(schema));

    let plan = parse_and_bind(
        "SELECT \
             SUM(CASE WHEN nation = 'BRAZIL' THEN volume ELSE volume - volume END) / SUM(volume) \
             AS mkt_share \
         FROM lineitem",
        &cat,
    )
    .expect("bind ok");

    let LogicalPlan::Project { input, exprs, .. } = &plan else {
        panic!("expected top-level Project, got {plan:?}");
    };
    let ScalarExpr::Binary { left, right, .. } = &exprs[0].0 else {
        panic!("expected ratio expression, got {:?}", exprs[0].0);
    };
    assert!(matches!(left.as_ref(), ScalarExpr::Column { index: 0, .. }));
    assert!(matches!(
        right.as_ref(),
        ScalarExpr::Column { index: 1, .. }
    ));

    let LogicalPlan::Aggregate { aggregates, .. } = input.as_ref() else {
        panic!("expected Aggregate under Project");
    };
    assert_eq!(aggregates.len(), 2, "SUM calls have different arguments");
}

// -----------------------------------------------------------------------
// Set operations tests
// -----------------------------------------------------------------------

#[test]
fn binds_union_all_arity_match() {
    let cat = users_catalog();
    let plan = parse_and_bind("SELECT id FROM users UNION ALL SELECT id FROM users", &cat)
        .expect("bind ok");

    fn find_setop(plan: &LogicalPlan) -> Option<&LogicalPlan> {
        match plan {
            LogicalPlan::SetOp { .. } => Some(plan),
            LogicalPlan::Cte { body, .. } => find_setop(body),
            _ => None,
        }
    }
    // The SetOp may be wrapped in a Cte if there were CTEs, otherwise it's
    // at the top level.
    let setop = find_setop(&plan).unwrap_or(&plan);
    // Accept either SetOp at top or wrapped in project.
    let has_setop = matches!(plan, LogicalPlan::SetOp { .. })
        || matches!(&plan, LogicalPlan::Project { input, .. }
                if matches!(input.as_ref(), LogicalPlan::SetOp { .. }));
    // Or the plan IS the setop.
    let is_setop = matches!(&plan, LogicalPlan::SetOp { quantifier, .. }
            if *quantifier == LogicalSetQuantifier::All);
    // If it's not directly at top, it's wrapped by the outer structure.
    if !has_setop && !is_setop {
        // Find it anywhere in the tree.
        let _ = setop;
        // The schema should have 1 column.
        let final_schema = plan.schema();
        assert_eq!(
            final_schema.len(),
            1,
            "UNION ALL of single-column selects = 1 col"
        );
    } else {
        assert!(has_setop || is_setop);
    }
    let _ = setop;
}

#[test]
fn binds_set_operation_order_by_output_column() {
    let cat = users_catalog();
    let plan = parse_and_bind(
        "SELECT id FROM users UNION SELECT id FROM users ORDER BY id",
        &cat,
    )
    .expect("bind ok");

    let LogicalPlan::Sort { input, keys } = &plan else {
        panic!("expected Sort above set operation, got {plan:?}");
    };
    assert_eq!(keys.len(), 1);
    assert!(matches!(keys[0].expr, ScalarExpr::Column { index: 0, .. }));
    assert!(matches!(input.as_ref(), LogicalPlan::SetOp { .. }));
}

#[test]
fn binds_union_distinct_with_arity_mismatch_is_rejected() {
    let cat = users_catalog();
    // id (1 col) UNION id, name (2 cols) should fail.
    let err = parse_and_bind(
        "SELECT id FROM users UNION SELECT id, name FROM users",
        &cat,
    )
    .unwrap_err();
    assert!(matches!(err, PlanError::TypeMismatch(_)), "got {err:?}");
}

// -----------------------------------------------------------------------
// CTE tests
// -----------------------------------------------------------------------

#[test]
fn binds_cte_then_references_it_in_body() {
    let cat = users_catalog();
    let plan = parse_and_bind(
        "WITH active AS (SELECT id FROM users) SELECT id FROM active",
        &cat,
    )
    .expect("bind ok");

    // Top-level plan should be a Cte node.
    let LogicalPlan::Cte {
        name, recursive, ..
    } = &plan
    else {
        panic!("expected Cte at top, got {plan:?}");
    };
    assert_eq!(name, "active");
    assert!(!recursive, "non-recursive CTE should have recursive=false");
}

#[test]
fn binds_scalar_subquery_that_references_outer_cte() {
    let cat = users_catalog();
    let plan = parse_and_bind(
        "WITH revenue AS (SELECT id, score FROM users) SELECT id FROM revenue WHERE score = (SELECT MAX(score) FROM revenue)",
        &cat,
    )
    .expect("bind ok");

    let LogicalPlan::Cte { body, .. } = &plan else {
        panic!("expected Cte at top, got {plan:?}");
    };

    fn find_filter(plan: &LogicalPlan) -> Option<&LogicalPlan> {
        match plan {
            LogicalPlan::Filter { .. } => Some(plan),
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::Sort { input, .. } => find_filter(input),
            _ => None,
        }
    }

    let filter = find_filter(body).expect("should have Filter");
    let LogicalPlan::Filter { predicate, .. } = filter else {
        panic!("expected Filter");
    };
    let ScalarExpr::Binary { right, .. } = predicate else {
        panic!("expected binary predicate, got {predicate:?}");
    };
    assert!(matches!(
        right.as_ref(),
        ScalarExpr::ScalarSubquery {
            correlated: false,
            ..
        }
    ));
}

// -----------------------------------------------------------------------
// SELECT * wildcard tests
// -----------------------------------------------------------------------

#[test]
fn binds_select_star_expands_via_catalog() {
    let cat = users_catalog();
    let plan = parse_and_bind("SELECT * FROM users", &cat).expect("bind ok");
    let LogicalPlan::Project { schema, exprs, .. } = &plan else {
        panic!("expected Project, got {plan:?}");
    };
    // users has id, name, score = 3 columns
    assert_eq!(schema.len(), 3, "SELECT * should expand to 3 columns");
    assert_eq!(exprs.len(), 3);
}

#[test]
fn binds_qualified_wildcard_restricts_to_table_alias() {
    let cat = two_table_catalog();
    let plan = parse_and_bind(
        "SELECT u.* FROM users u JOIN orders o ON u.id = o.user_id",
        &cat,
    )
    .expect("bind ok");
    let LogicalPlan::Project { schema, .. } = &plan else {
        panic!("expected Project, got {plan:?}");
    };
    // users u has 2 columns; u.* should expand to those 2 only.
    assert_eq!(schema.len(), 2, "u.* should expand to users' 2 columns");
}

// -----------------------------------------------------------------------
// Error / unsupported
// -----------------------------------------------------------------------

#[test]
fn binder_rejects_unknown_aggregate_with_not_supported() {
    let cat = users_catalog();
    // `mode` is not a known aggregate; the binder should reject it.
    let err = parse_and_bind("SELECT mode(score) FROM users GROUP BY id", &cat).unwrap_err();
    assert!(
        matches!(err, PlanError::NotSupported(_)),
        "unknown aggregate should be NotSupported, got {err:?}"
    );
}

// -----------------------------------------------------------------------
// Property test
// -----------------------------------------------------------------------

// -----------------------------------------------------------------------
// Subquery tests
// -----------------------------------------------------------------------

/// A two-table catalog: `users (id INT, name TEXT, score FLOAT8)`
/// and `orders (oid INT, user_id INT)`.
fn subquery_catalog() -> InMemoryCatalog {
    let users_schema = Schema::new([
        Field::required("id", DataType::Int32),
        Field::nullable("name", DataType::Text { max_len: None }),
        Field::nullable("score", DataType::Float64),
    ])
    .expect("schema ok");
    let orders_schema = Schema::new([
        Field::required("oid", DataType::Int32),
        Field::required("user_id", DataType::Int32),
    ])
    .expect("schema ok");
    let mut cat = InMemoryCatalog::new();
    cat.register("users", TableMeta::new(users_schema));
    cat.register("orders", TableMeta::new(orders_schema));
    cat
}

#[test]
fn binds_uncorrelated_exists_subquery() {
    // `EXISTS (SELECT oid FROM orders)` has no outer column references.
    let cat = subquery_catalog();
    let plan = parse_and_bind(
        "SELECT id FROM users WHERE EXISTS (SELECT oid FROM orders)",
        &cat,
    )
    .expect("bind ok");
    // Walk to the Filter and check its predicate.
    fn find_filter(plan: &LogicalPlan) -> Option<&LogicalPlan> {
        match plan {
            LogicalPlan::Filter { .. } => Some(plan),
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::Sort { input, .. } => find_filter(input),
            _ => None,
        }
    }
    let filter = find_filter(&plan).expect("should have Filter");
    let LogicalPlan::Filter { predicate, .. } = filter else {
        panic!("expected Filter");
    };
    let ScalarExpr::Exists {
        negated,
        correlated,
        ..
    } = predicate
    else {
        panic!("expected Exists predicate, got {predicate:?}");
    };
    assert!(!negated, "should not be negated");
    assert!(!correlated, "no outer column reference → uncorrelated");
}

#[test]
fn binds_correlated_exists_subquery() {
    // `EXISTS (SELECT oid FROM orders WHERE user_id = id)` — `id` is not in
    // `orders`, so it resolves to the outer `users.id`.
    let cat = subquery_catalog();
    let plan = parse_and_bind(
        "SELECT id FROM users WHERE EXISTS (SELECT oid FROM orders WHERE user_id = id)",
        &cat,
    )
    .expect("bind ok");
    fn find_filter(plan: &LogicalPlan) -> Option<&LogicalPlan> {
        match plan {
            LogicalPlan::Filter { .. } => Some(plan),
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::Sort { input, .. } => find_filter(input),
            _ => None,
        }
    }
    let filter = find_filter(&plan).expect("should have Filter");
    let LogicalPlan::Filter { predicate, .. } = filter else {
        panic!("expected Filter");
    };
    let ScalarExpr::Exists { correlated, .. } = predicate else {
        panic!("expected Exists, got {predicate:?}");
    };
    assert!(correlated, "id resolves to outer users.id → correlated");
}

#[test]
fn binds_in_subquery_arity_1_check_rejects_multi_column() {
    // `id IN (SELECT oid, user_id FROM orders)` — 2-column subquery must fail.
    let cat = subquery_catalog();
    let err = parse_and_bind(
        "SELECT id FROM users WHERE id IN (SELECT oid, user_id FROM orders)",
        &cat,
    )
    .unwrap_err();
    assert!(
        matches!(err, PlanError::TypeMismatch(_)),
        "multi-column IN subquery should be TypeMismatch, got {err:?}"
    );
}

#[test]
fn binds_scalar_subquery_returns_scalar_subquery_expr() {
    // `(SELECT oid FROM orders LIMIT 1)` used as a scalar in the projection.
    let cat = subquery_catalog();
    let plan = parse_and_bind(
        "SELECT id, (SELECT oid FROM orders LIMIT 1) FROM users",
        &cat,
    )
    .expect("bind ok");
    let LogicalPlan::Project { exprs, .. } = &plan else {
        panic!("expected Project, got {plan:?}");
    };
    // Second expression should be a ScalarSubquery.
    let (second_expr, _) = &exprs[1];
    assert!(
        matches!(
            second_expr,
            ScalarExpr::ScalarSubquery {
                correlated: false,
                ..
            }
        ),
        "expected uncorrelated ScalarSubquery, got {second_expr:?}"
    );
}

#[test]
fn binds_not_in_subquery() {
    let cat = subquery_catalog();
    let plan = parse_and_bind(
        "SELECT id FROM users WHERE id NOT IN (SELECT user_id FROM orders)",
        &cat,
    )
    .expect("bind ok");
    fn find_filter(plan: &LogicalPlan) -> Option<&LogicalPlan> {
        match plan {
            LogicalPlan::Filter { .. } => Some(plan),
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::Sort { input, .. } => find_filter(input),
            _ => None,
        }
    }
    let filter = find_filter(&plan).expect("should have Filter");
    let LogicalPlan::Filter { predicate, .. } = filter else {
        panic!("expected Filter");
    };
    let ScalarExpr::InSubquery { negated, .. } = predicate else {
        panic!("expected InSubquery, got {predicate:?}");
    };
    assert!(negated, "NOT IN should produce negated=true");
}

#[test]
fn binds_any_eq_lowers_to_exists() {
    // `id = ANY (SELECT user_id FROM orders)` should bind as InSubquery with
    // negated=false (the same representation as `id IN (…)`).
    let cat = subquery_catalog();
    let plan = parse_and_bind(
        "SELECT id FROM users WHERE id = ANY (SELECT user_id FROM orders)",
        &cat,
    )
    .expect("bind ok");
    fn find_filter(plan: &LogicalPlan) -> Option<&LogicalPlan> {
        match plan {
            LogicalPlan::Filter { .. } => Some(plan),
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::Sort { input, .. } => find_filter(input),
            _ => None,
        }
    }
    let filter = find_filter(&plan).expect("should have Filter");
    let LogicalPlan::Filter { predicate, .. } = filter else {
        panic!("expected Filter");
    };
    assert!(
        matches!(predicate, ScalarExpr::InSubquery { negated: false, .. }),
        "= ANY should lower to InSubquery(negated=false), got {predicate:?}"
    );
}

#[test]
fn binds_any_with_lt_returns_not_supported() {
    let cat = subquery_catalog();
    let err = parse_and_bind(
        "SELECT id FROM users WHERE id < ANY (SELECT user_id FROM orders)",
        &cat,
    )
    .unwrap_err();
    assert!(
        matches!(err, PlanError::NotSupported(_)),
        "< ANY should be NotSupported, got {err:?}"
    );
}

#[test]
fn binder_rejects_scalar_subquery_with_multi_column_projection() {
    let cat = subquery_catalog();
    let err = parse_and_bind(
        "SELECT id, (SELECT oid, user_id FROM orders LIMIT 1) FROM users",
        &cat,
    )
    .unwrap_err();
    assert!(
        matches!(err, PlanError::TypeMismatch(_)),
        "multi-column scalar subquery should be TypeMismatch, got {err:?}"
    );
}

#[test]
fn outer_column_correctly_tracks_frame_depth_in_nested_subquery() {
    // Outer query scans `users`.  The subquery scans `orders`.  Inside the
    // subquery's WHERE, `id` is not in `orders` so it should resolve as
    // `OuterColumn { frame_depth: 1, … }`.
    let cat = subquery_catalog();
    let plan = parse_and_bind(
        "SELECT id FROM users WHERE EXISTS (SELECT oid FROM orders WHERE user_id = id)",
        &cat,
    )
    .expect("bind ok");
    // Navigate to the Exists predicate's inner plan.
    fn find_exists_pred(plan: &LogicalPlan) -> Option<&ScalarExpr> {
        match plan {
            LogicalPlan::Filter { predicate, .. } => {
                if matches!(predicate, ScalarExpr::Exists { .. }) {
                    Some(predicate)
                } else {
                    None
                }
            }
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => find_exists_pred(input),
            _ => None,
        }
    }
    let pred = find_exists_pred(&plan).expect("should find Exists predicate");
    let ScalarExpr::Exists { subplan, .. } = pred else {
        panic!("expected Exists");
    };
    // The inner plan should have a Filter with an outer-column reference.
    fn find_outer_col(plan: &LogicalPlan) -> Option<usize> {
        match plan {
            LogicalPlan::Filter { predicate, .. } => {
                // Predicate is `user_id = id` — a Binary with the right side
                // being an OuterColumn.
                if let ScalarExpr::Binary { right, .. } = predicate {
                    if let ScalarExpr::OuterColumn { frame_depth, .. } = right.as_ref() {
                        return Some(*frame_depth);
                    }
                }
                None
            }
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => find_outer_col(input),
            _ => None,
        }
    }
    let depth = find_outer_col(subplan).expect("should find OuterColumn in subplan");
    assert_eq!(depth, 1, "column is one level out → frame_depth = 1");
}

// -----------------------------------------------------------------------
// BETWEEN tests — the binder rewrites BETWEEN into a comparison tree
// -----------------------------------------------------------------------

/// Extract the bound WHERE predicate from a SELECT plan that the
/// binder shaped as `Project { Filter { Scan } }`.
fn predicate_of(plan: &LogicalPlan) -> &ScalarExpr {
    fn find_filter(plan: &LogicalPlan) -> &LogicalPlan {
        match plan {
            LogicalPlan::Filter { .. } => plan,
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => find_filter(input),
            _ => panic!("expected Filter under plan, got {plan:?}"),
        }
    }
    match find_filter(plan) {
        LogicalPlan::Filter { predicate, .. } => predicate,
        other => panic!("expected Filter, got {other:?}"),
    }
}

#[test]
fn binds_between_as_ge_and_le() {
    // The canonical rewrite: BETWEEN low AND high becomes
    // `expr >= low AND expr <= high`.
    let plan = parse_bind_ok("SELECT id FROM users WHERE id BETWEEN 5 AND 10");
    let pred = predicate_of(&plan);
    // Top-level: AND.
    let ScalarExpr::Binary {
        op: BinaryOp::And,
        left,
        right,
        data_type,
    } = pred
    else {
        panic!("expected AND at the root, got {pred:?}");
    };
    assert_eq!(*data_type, DataType::Bool);

    // Left arm: `id >= 5`.
    let ScalarExpr::Binary {
        op: BinaryOp::GtEq,
        left: lo_l,
        right: lo_r,
        ..
    } = left.as_ref()
    else {
        panic!("expected GtEq on left, got {left:?}");
    };
    assert!(matches!(lo_l.as_ref(), ScalarExpr::Column { name, .. } if name == "id"));
    assert!(matches!(
        lo_r.as_ref(),
        ScalarExpr::Literal {
            value: Value::Int32(5),
            ..
        }
    ));

    // Right arm: `id <= 10`.
    let ScalarExpr::Binary {
        op: BinaryOp::LtEq,
        left: hi_l,
        right: hi_r,
        ..
    } = right.as_ref()
    else {
        panic!("expected LtEq on right, got {right:?}");
    };
    assert!(matches!(hi_l.as_ref(), ScalarExpr::Column { name, .. } if name == "id"));
    assert!(matches!(
        hi_r.as_ref(),
        ScalarExpr::Literal {
            value: Value::Int32(10),
            ..
        }
    ));
}

#[test]
fn binds_not_between_as_lt_or_gt() {
    let plan = parse_bind_ok("SELECT id FROM users WHERE id NOT BETWEEN 5 AND 10");
    let pred = predicate_of(&plan);
    let ScalarExpr::Binary {
        op: BinaryOp::Or,
        left,
        right,
        ..
    } = pred
    else {
        panic!("expected OR at the root, got {pred:?}");
    };
    assert!(matches!(
        left.as_ref(),
        ScalarExpr::Binary {
            op: BinaryOp::Lt,
            ..
        }
    ));
    assert!(matches!(
        right.as_ref(),
        ScalarExpr::Binary {
            op: BinaryOp::Gt,
            ..
        }
    ));
}

#[test]
fn binds_between_mixed_numeric_types() {
    // `score` is FLOAT8 in the users catalog. A BETWEEN against an
    // integer pair must bind cleanly through the same numeric-join
    // promotion that the explicit comparison form uses.
    let plan = parse_bind_ok("SELECT id FROM users WHERE score BETWEEN 1 AND 100");
    let pred = predicate_of(&plan);
    assert!(matches!(
        pred,
        ScalarExpr::Binary {
            op: BinaryOp::And,
            ..
        }
    ));
}

#[test]
fn binds_between_symmetric_emits_or_of_two_ranges() {
    let plan = parse_bind_ok("SELECT id FROM users WHERE id BETWEEN SYMMETRIC 10 AND 5");
    let pred = predicate_of(&plan);
    // BETWEEN SYMMETRIC: (forward) OR (reversed).
    let ScalarExpr::Binary {
        op: BinaryOp::Or,
        left,
        right,
        ..
    } = pred
    else {
        panic!("expected OR at the root, got {pred:?}");
    };
    // Each arm is a `(>= AND <=)` tree.
    for (label, arm) in [("forward", left.as_ref()), ("reversed", right.as_ref())] {
        assert!(
            matches!(
                arm,
                ScalarExpr::Binary {
                    op: BinaryOp::And,
                    ..
                }
            ),
            "SYMMETRIC {label} arm should be AND, got {arm:?}"
        );
    }
}

#[test]
fn binds_not_between_symmetric_emits_and_of_two_ranges() {
    let plan = parse_bind_ok("SELECT id FROM users WHERE id NOT BETWEEN SYMMETRIC 10 AND 5");
    let pred = predicate_of(&plan);
    // NOT BETWEEN SYMMETRIC: (forward NOT) AND (reversed NOT).
    let ScalarExpr::Binary {
        op: BinaryOp::And,
        left,
        right,
        ..
    } = pred
    else {
        panic!("expected AND at the root, got {pred:?}");
    };
    for (label, arm) in [("forward", left.as_ref()), ("reversed", right.as_ref())] {
        assert!(
            matches!(
                arm,
                ScalarExpr::Binary {
                    op: BinaryOp::Or,
                    ..
                }
            ),
            "NOT SYMMETRIC {label} arm should be OR, got {arm:?}"
        );
    }
}

#[test]
fn binds_between_rewrite_renders_as_full_tree() {
    // Lock down the exact textual form of the rewrite so it surfaces
    // unambiguously in EXPLAIN-style output.
    let plan = parse_bind_ok("SELECT id FROM users WHERE id BETWEEN 5 AND 10");
    let pred = predicate_of(&plan);
    assert_eq!(pred.to_string(), "((id >= 5) AND (id <= 10))");
}

#[test]
fn binds_between_uses_existing_type_check_to_reject_incompatible_bounds() {
    // `name` is TEXT; bound by an integer literal is not comparable
    // — the binder must surface a TypeMismatch the same way it
    // would for the equivalent `name >= 1 AND name <= 10`.
    let cat = users_catalog();
    let err = parse_and_bind("SELECT id FROM users WHERE name BETWEEN 1 AND 10", &cat).unwrap_err();
    assert!(matches!(err, PlanError::TypeMismatch(_)), "got {err:?}");
}

proptest! {
    /// BETWEEN binds without error for every integer pair drawn from the
    /// supported i32 range — the rewrite never invents a type it does not
    /// already accept on plain comparisons.
    #[test]
    fn prop_between_int_pair_binds_ok(
        lo in -1_000_000_i32..=1_000_000_i32,
        hi in -1_000_000_i32..=1_000_000_i32,
    ) {
        let cat = users_catalog();
        let sql = format!("SELECT id FROM users WHERE id BETWEEN {lo} AND {hi}");
        let result = parse_and_bind(&sql, &cat);
        prop_assert!(result.is_ok(), "BETWEEN should bind, got {:?}", result);
    }

    /// NOT BETWEEN binds without error for every integer pair drawn from
    /// the supported i32 range.
    #[test]
    fn prop_not_between_int_pair_binds_ok(
        lo in -1_000_000_i32..=1_000_000_i32,
        hi in -1_000_000_i32..=1_000_000_i32,
    ) {
        let cat = users_catalog();
        let sql = format!("SELECT id FROM users WHERE id NOT BETWEEN {lo} AND {hi}");
        let result = parse_and_bind(&sql, &cat);
        prop_assert!(result.is_ok(), "NOT BETWEEN should bind, got {:?}", result);
    }
}

proptest! {
    /// Any random join tree over a fixed set of 3 tables binds without error.
    #[test]
    fn prop_join_tree_over_three_tables_binds_ok(
        // Choose join type index 0..4 for left join and right join.
        lj_type in 0_usize..2_usize,
        rj_type in 0_usize..2_usize,
    ) {
        // Catalog: a, b, c each with one column.
        let mut cat = InMemoryCatalog::new();
        let s = Schema::new([Field::required("x", DataType::Int32)]).expect("schema ok");
        cat.register("ta", TableMeta::new(s));
        let sb = Schema::new([Field::required("y", DataType::Int32)]).expect("schema ok");
        cat.register("tb", TableMeta::new(sb));
        let sc = Schema::new([Field::required("z", DataType::Int32)]).expect("schema ok");
        cat.register("tc", TableMeta::new(sc));

        let join_kw = ["INNER JOIN", "CROSS JOIN"];
        let lj = join_kw[lj_type % join_kw.len()];
        let rj = join_kw[rj_type % join_kw.len()];
        let on_lj = if lj == "CROSS JOIN" { "" } else { " ON ta.x = tb.y" };
        let on_rj = if rj == "CROSS JOIN" { "" } else { " ON ta.x = tc.z" };
        let sql = format!(
            "SELECT ta.x FROM ta {lj} tb{on_lj} {rj} tc{on_rj}"
        );
        let result = parse_and_bind(&sql, &cat);
        prop_assert!(result.is_ok(), "join tree should bind ok, got {:?}", result);
    }
}

proptest! {
    /// For any arity in 1..=6 and 1..=4 matching VALUES rows, the bound
    /// INSERT plan has a Values source with the same arity.
    #[test]
    fn prop_insert_values_arity_preserved(
        arity in 1_usize..=6_usize,
        nrows in 1_usize..=4_usize,
    ) {
        // Build a catalog with a table that has `arity` INT columns.
        let fields: Vec<Field> = (0..arity)
            .map(|i| Field::nullable(format!("c{i}"), DataType::Int32))
            .collect();
        let schema = Schema::new(fields).expect("schema ok");
        let mut cat = InMemoryCatalog::new();
        cat.register("t", TableMeta::new(schema));

        // Build SQL: INSERT INTO t (c0, c1, …) VALUES (0, 0, …), …
        let cols: Vec<String> = (0..arity).map(|i| format!("c{i}")).collect();
        let one_row = vec!["0"; arity].join(", ");
        let values_clause = std::iter::repeat_n(format!("({one_row})"), nrows)
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "INSERT INTO t ({}) VALUES {}",
            cols.join(", "),
            values_clause
        );

        let plan = parse_and_bind(&sql, &cat).expect("bind ok");
        let LogicalPlan::Insert { columns, source, .. } = &plan else {
            panic!("expected Insert");
        };
        prop_assert_eq!(columns.len(), arity);
        let LogicalPlan::Values { rows, .. } = source.as_ref() else {
            panic!("expected Values source");
        };
        prop_assert_eq!(rows.len(), nrows);
        for r in rows {
            prop_assert_eq!(r.len(), arity);
        }
    }
}

// -----------------------------------------------------------------------
// CREATE INDEX / DROP TABLE / ALTER TABLE — DDL binder tests
// -----------------------------------------------------------------------

#[test]
fn binds_create_index_resolves_column_index_and_synthesises_name() {
    let plan = parse_bind_ok("CREATE INDEX ON users (id)");
    let LogicalPlan::CreateIndex {
        index_name,
        table_name,
        columns,
        unique,
        if_not_exists,
        ..
    } = plan
    else {
        panic!("expected CreateIndex plan");
    };
    assert_eq!(table_name, "users");
    assert_eq!(columns, vec![0]);
    assert!(!unique);
    assert!(!if_not_exists);
    // Synthesised name uses the {table}_{cols}_idx convention.
    assert_eq!(index_name, "users_id_idx");
}

#[test]
fn binds_create_index_namespace_from_target_table() {
    let cat = app_users_catalog();
    let plan = parse_and_bind("CREATE INDEX same_idx ON app.users (id)", &cat)
        .expect("qualified app table binds");
    let LogicalPlan::CreateIndex {
        index_name,
        index_namespace,
        table_name,
        ..
    } = plan
    else {
        panic!("expected CreateIndex plan");
    };
    assert_eq!(index_name, "same_idx");
    assert_eq!(index_namespace, "app");
    assert_eq!(table_name, "app.users");
}

#[test]
fn binds_create_unique_index_honours_unique_flag_and_explicit_name() {
    let plan = parse_bind_ok("CREATE UNIQUE INDEX IF NOT EXISTS users_pk ON users (id)");
    let LogicalPlan::CreateIndex {
        index_name,
        unique,
        if_not_exists,
        ..
    } = plan
    else {
        panic!("expected CreateIndex plan");
    };
    assert!(unique);
    assert!(if_not_exists);
    assert_eq!(index_name, "users_pk");
}

#[test]
fn binds_create_index_concurrently_flag() {
    let plan = parse_bind_ok("CREATE INDEX CONCURRENTLY users_id_idx ON users (id)");
    let LogicalPlan::CreateIndex { concurrently, .. } = plan else {
        panic!("expected CreateIndex plan");
    };
    assert!(concurrently);
}

#[test]
fn binds_create_hash_index_method() {
    let plan = parse_bind_ok("CREATE INDEX users_id_hash_idx ON users USING hash (id)");
    let LogicalPlan::CreateIndex { method, .. } = plan else {
        panic!("expected CreateIndex plan");
    };
    assert_eq!(method, LogicalIndexMethod::Hash);
}

#[test]
fn binds_create_aggregating_index_metadata() {
    let cat = fact_events_catalog();
    let LogicalPlan::CreateIndex {
        method,
        columns,
        aggregating,
        ..
    } = parse_and_bind(
        "CREATE AGGREGATING INDEX fact_rollup ON fact_events \
         (tenant_id, bucket, sum(amount), count(*))",
        &cat,
    )
    .expect("bind aggregating index")
    else {
        panic!("expected CreateIndex plan");
    };
    assert_eq!(method, LogicalIndexMethod::Aggregating);
    assert_eq!(columns, vec![0, 1]);
    let aggregating = aggregating.expect("aggregating metadata");
    assert_eq!(aggregating.group_columns, vec![0, 1]);
    assert_eq!(aggregating.aggregates.len(), 2);
    assert_eq!(aggregating.aggregates[0].func, AggregateFunc::Sum);
    assert_eq!(aggregating.aggregates[0].arg_column, Some(2));
    assert_eq!(aggregating.aggregates[1].func, AggregateFunc::CountStar);
    assert_eq!(aggregating.aggregates[1].arg_column, None);
}

#[test]
fn binds_create_inverted_search_and_brin_index_methods() {
    for (sql, expected) in [
        (
            "CREATE INDEX users_name_gin_idx ON users USING gin (name)",
            LogicalIndexMethod::Gin,
        ),
        (
            "CREATE INDEX users_score_gist_idx ON users USING gist (score)",
            LogicalIndexMethod::Gist,
        ),
        (
            "CREATE INDEX users_id_brin_idx ON users USING brin (id)",
            LogicalIndexMethod::Brin,
        ),
    ] {
        let LogicalPlan::CreateIndex { method, .. } = parse_bind_ok(sql) else {
            panic!("expected CreateIndex plan");
        };
        assert_eq!(method, expected);
    }
}

#[test]
fn binds_create_hnsw_index_method_for_vector_column() {
    let cat = embeddings_catalog();
    let LogicalPlan::CreateIndex {
        method, columns, ..
    } = parse_and_bind(
        "CREATE INDEX embeddings_hnsw_idx ON embeddings USING hnsw (embedding)",
        &cat,
    )
    .expect("bind hnsw")
    else {
        panic!("expected CreateIndex plan");
    };
    assert_eq!(method, LogicalIndexMethod::Hnsw);
    assert_eq!(columns, vec![1]);
}

#[test]
fn binds_create_hnsw_vector_opclass() {
    let cat = embeddings_catalog();
    let LogicalPlan::CreateIndex {
        method,
        columns,
        opclasses,
        ..
    } = parse_and_bind(
        "CREATE INDEX embeddings_hnsw_idx ON embeddings USING hnsw (embedding vector_l2_ops)",
        &cat,
    )
    .expect("bind hnsw opclass")
    else {
        panic!("expected CreateIndex plan");
    };
    assert_eq!(method, LogicalIndexMethod::Hnsw);
    assert_eq!(columns, vec![1]);
    assert_eq!(opclasses, vec![Some("vector_l2_ops".to_owned())]);
}

#[test]
fn binds_create_ivfflat_with_lists_and_probes() {
    let cat = embeddings_catalog();
    let LogicalPlan::CreateIndex {
        method,
        columns,
        opclasses,
        index_options,
        ..
    } = parse_and_bind(
        "CREATE INDEX embeddings_ivf_idx ON embeddings \
         USING ivfflat (embedding vector_l2_ops) WITH (lists = 8, probes = 3)",
        &cat,
    )
    .expect("bind ivfflat")
    else {
        panic!("expected CreateIndex plan");
    };
    assert_eq!(method, LogicalIndexMethod::IvfFlat);
    assert_eq!(columns, vec![1]);
    assert_eq!(opclasses, vec![Some("vector_l2_ops".to_owned())]);
    assert_eq!(index_options.len(), 2);
    assert_eq!(index_options[0].name, "lists");
    assert_eq!(index_options[0].value, "8");
    assert_eq!(index_options[1].name, "probes");
    assert_eq!(index_options[1].value, "3");
}

#[test]
fn rejects_unique_inverted_search_and_brin_index_methods() {
    let cat = users_catalog();
    for sql in [
        "CREATE UNIQUE INDEX users_name_gin_idx ON users USING gin (name)",
        "CREATE UNIQUE INDEX users_score_gist_idx ON users USING gist (score)",
        "CREATE UNIQUE INDEX users_id_brin_idx ON users USING brin (id)",
    ] {
        let err = parse_and_bind(sql, &cat).unwrap_err();
        assert!(matches!(err, PlanError::NotSupported(_)), "got {err:?}");
    }
}

#[test]
fn rejects_unique_hnsw_index() {
    let cat = embeddings_catalog();
    let err = parse_and_bind(
        "CREATE UNIQUE INDEX embeddings_hnsw_idx ON embeddings USING hnsw (embedding)",
        &cat,
    )
    .unwrap_err();
    assert!(matches!(err, PlanError::NotSupported(_)), "got {err:?}");
}

#[test]
fn rejects_hnsw_index_on_non_vector_column() {
    let cat = users_catalog();
    let err =
        parse_and_bind("CREATE INDEX users_hnsw_idx ON users USING hnsw (id)", &cat).unwrap_err();
    assert!(matches!(err, PlanError::TypeMismatch(_)), "got {err:?}");
}

#[test]
fn binds_create_expression_index_key() {
    let plan = parse_bind_ok("CREATE INDEX users_lower_name_idx ON users (lower(name))");
    let LogicalPlan::CreateIndex {
        columns, key_exprs, ..
    } = plan
    else {
        panic!("expected CreateIndex plan");
    };
    assert!(columns.is_empty());
    assert_eq!(key_exprs.len(), 1);
    assert!(matches!(
        &key_exprs[0],
        ScalarExpr::FunctionCall { name, .. } if name == "lower"
    ));
}

#[test]
fn binds_create_partial_and_include_index_metadata() {
    let plan = parse_bind_ok(
        "CREATE INDEX users_name_active_idx ON users (name) INCLUDE (score) WHERE name IS NOT NULL",
    );
    let LogicalPlan::CreateIndex {
        columns,
        include_columns,
        predicate,
        ..
    } = plan
    else {
        panic!("expected CreateIndex plan");
    };
    assert_eq!(columns, vec![1]);
    assert_eq!(include_columns, vec![2]);
    assert!(matches!(
        predicate,
        Some(ScalarExpr::IsNull { negated: true, .. })
    ));
}

#[test]
fn create_index_rejects_unknown_column() {
    let cat = users_catalog();
    let err = parse_and_bind("CREATE INDEX bad_idx ON users (does_not_exist)", &cat).unwrap_err();
    assert!(matches!(err, PlanError::ColumnNotFound(_)), "got {err:?}");
}

#[test]
fn create_index_rejects_unknown_table() {
    let cat = users_catalog();
    let err = parse_and_bind("CREATE INDEX bad_idx ON nonexistent (id)", &cat).unwrap_err();
    assert!(matches!(err, PlanError::TableNotFound(_)), "got {err:?}");
}

#[test]
fn binds_drop_table_with_known_relation() {
    let plan = parse_bind_ok("DROP TABLE users");
    let LogicalPlan::DropTable {
        tables, if_exists, ..
    } = plan
    else {
        panic!("expected DropTable plan");
    };
    assert_eq!(tables, vec!["users".to_string()]);
    assert!(!if_exists);
}

#[test]
fn drop_table_if_exists_silently_omits_missing_relations() {
    let plan = parse_bind_ok("DROP TABLE IF EXISTS users, nope");
    let LogicalPlan::DropTable {
        tables, if_exists, ..
    } = plan
    else {
        panic!("expected DropTable plan");
    };
    assert!(if_exists);
    // `nope` is silently filtered; `users` remains.
    assert_eq!(tables, vec!["users".to_string()]);
}

#[test]
fn drop_table_without_if_exists_rejects_missing_relation() {
    let cat = users_catalog();
    let err = parse_and_bind("DROP TABLE nonexistent", &cat).unwrap_err();
    assert!(matches!(err, PlanError::TableNotFound(_)), "got {err:?}");
}

#[test]
fn binds_drop_index_with_known_index() {
    let cat = users_index_catalog();
    let plan = parse_and_bind("DROP INDEX users_id_idx", &cat).expect("drop index binds");
    let LogicalPlan::DropIndex {
        indexes, if_exists, ..
    } = plan
    else {
        panic!("expected DropIndex plan");
    };
    assert_eq!(indexes, vec!["users_id_idx".to_string()]);
    assert!(!if_exists);
}

#[test]
fn binds_drop_index_preserves_explicit_namespace() {
    let mut cat = users_index_catalog();
    cat.register_index_in_schema("app", "users_id_idx");
    let plan = parse_and_bind("DROP INDEX app.users_id_idx", &cat).expect("drop index binds");
    let LogicalPlan::DropIndex {
        indexes,
        index_namespaces,
        ..
    } = plan
    else {
        panic!("expected DropIndex plan");
    };
    assert_eq!(indexes, vec!["users_id_idx".to_string()]);
    assert_eq!(index_namespaces, vec![Some("app".to_string())]);
}

#[test]
fn binds_drop_index_preserves_quoted_dotted_public_name() {
    let mut cat = users_index_catalog();
    cat.register_index("idx.dotted");
    let plan = parse_and_bind("DROP INDEX \"idx.dotted\"", &cat).expect("drop index binds");
    let LogicalPlan::DropIndex {
        indexes,
        index_namespaces,
        ..
    } = plan
    else {
        panic!("expected DropIndex plan");
    };
    assert_eq!(indexes, vec!["idx.dotted".to_string()]);
    assert_eq!(index_namespaces, vec![None]);
}

#[test]
fn drop_index_if_exists_silently_omits_missing_indexes() {
    let cat = users_index_catalog();
    let plan = parse_and_bind("DROP INDEX IF EXISTS users_id_idx, nope", &cat)
        .expect("drop index if exists binds");
    let LogicalPlan::DropIndex {
        indexes, if_exists, ..
    } = plan
    else {
        panic!("expected DropIndex plan");
    };
    assert!(if_exists);
    assert_eq!(indexes, vec!["users_id_idx".to_string()]);
}

#[test]
fn drop_index_without_if_exists_rejects_missing_index() {
    let cat = users_catalog();
    let err = parse_and_bind("DROP INDEX nonexistent_idx", &cat).unwrap_err();
    assert!(matches!(err, PlanError::IndexNotFound(_)), "got {err:?}");
}

#[test]
fn binds_alter_table_add_column_resolves_field() {
    let plan = parse_bind_ok("ALTER TABLE users ADD COLUMN extra INTEGER");
    let LogicalPlan::AlterTable {
        table_name, action, ..
    } = plan
    else {
        panic!("expected AlterTable plan");
    };
    assert_eq!(table_name, "users");
    let LogicalAlterTableAction::AddColumn { column, default } = action else {
        panic!("expected AddColumn action");
    };
    assert_eq!(column.name, "extra");
    assert_eq!(column.data_type, DataType::Int32);
    assert!(column.nullable, "ADD COLUMN defaults to nullable");
    assert!(default.is_none());
}

#[test]
fn binds_alter_table_add_column_not_null() {
    let plan = parse_bind_ok("ALTER TABLE users ADD COLUMN flag BOOLEAN NOT NULL");
    let LogicalPlan::AlterTable { action, .. } = plan else {
        panic!("expected AlterTable plan");
    };
    let LogicalAlterTableAction::AddColumn { column, default } = action else {
        panic!("expected AddColumn action");
    };
    assert_eq!(column.data_type, DataType::Bool);
    assert!(!column.nullable);
    assert!(default.is_none());
}

#[test]
fn binds_alter_table_add_column_default() {
    let plan = parse_bind_ok("ALTER TABLE users ADD COLUMN extra INTEGER DEFAULT 7");
    let LogicalPlan::AlterTable { action, .. } = plan else {
        panic!("expected AlterTable plan");
    };
    let LogicalAlterTableAction::AddColumn { column, default } = action else {
        panic!("expected AddColumn action");
    };
    assert_eq!(column.name, "extra");
    assert_eq!(column.data_type, DataType::Int32);
    let Some(ScalarExpr::Literal {
        value: Value::Int32(7),
        ..
    }) = default
    else {
        panic!("expected integer literal default");
    };
}

#[test]
fn alter_table_add_column_rejects_duplicate_name() {
    let cat = users_catalog();
    let err = parse_and_bind("ALTER TABLE users ADD COLUMN id INTEGER", &cat).unwrap_err();
    assert!(
        matches!(err, PlanError::DuplicateColumn(ref c) if c == "id"),
        "got {err:?}"
    );
}

#[test]
fn binds_alter_table_drop_column_resolves_index() {
    let cat = users_catalog();
    let plan = parse_and_bind("ALTER TABLE users DROP COLUMN score", &cat).unwrap();
    let LogicalPlan::AlterTable { action, .. } = plan else {
        panic!("expected AlterTable plan");
    };
    let LogicalAlterTableAction::DropColumn {
        column_index,
        column_name,
    } = action
    else {
        panic!("expected DropColumn action");
    };
    assert_eq!(column_name, "score");
    assert_eq!(column_index, 2);
}

#[test]
fn binds_alter_table_rename_to_new_name() {
    let cat = users_catalog();
    let plan = parse_and_bind("ALTER TABLE users RENAME TO subscribers", &cat).unwrap();
    let LogicalPlan::AlterTable { action, .. } = plan else {
        panic!("expected AlterTable plan");
    };
    let LogicalAlterTableAction::RenameTable { new_name } = action else {
        panic!("expected RenameTable action");
    };
    assert_eq!(new_name, "subscribers");
}

fn collect_window_funcs<'a>(plan: &'a LogicalPlan, out: &mut Vec<&'a LogicalWindowFunc>) {
    match plan {
        LogicalPlan::Window { input, func, .. } => {
            out.push(func);
            collect_window_funcs(input, out);
        }
        LogicalPlan::Project { input, .. }
        | LogicalPlan::Filter { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::LockRows { input, .. }
        | LogicalPlan::Update { input, .. }
        | LogicalPlan::Delete { input, .. } => collect_window_funcs(input, out),
        LogicalPlan::Insert { source, .. } => collect_window_funcs(source, out),
        LogicalPlan::Join { left, right, .. } | LogicalPlan::SetOp { left, right, .. } => {
            collect_window_funcs(left, out);
            collect_window_funcs(right, out);
        }
        LogicalPlan::Cte {
            definition, body, ..
        } => {
            collect_window_funcs(definition, out);
            collect_window_funcs(body, out);
        }
        LogicalPlan::CreateMaterializedView { source, .. }
        | LogicalPlan::Explain { input: source, .. }
        | LogicalPlan::Copy {
            input: Some(source),
            ..
        } => collect_window_funcs(source, out),
        LogicalPlan::Scan { .. }
        | LogicalPlan::Empty { .. }
        | LogicalPlan::Values { .. }
        | LogicalPlan::FunctionScan { .. }
        | LogicalPlan::Truncate { .. }
        | LogicalPlan::CreateTable { .. }
        | LogicalPlan::CreateTypeEnum { .. }
        | LogicalPlan::CreateTypeComposite { .. }
        | LogicalPlan::CreateDomain { .. }
        | LogicalPlan::CreateOperator { .. }
        | LogicalPlan::CreateIndex { .. }
        | LogicalPlan::DropIndex { .. }
        | LogicalPlan::CreateSchema { .. }
        | LogicalPlan::DropSchema { .. }
        | LogicalPlan::CreatePolicy { .. }
        | LogicalPlan::CreateRole { .. }
        | LogicalPlan::AlterRole { .. }
        | LogicalPlan::DropRole { .. }
        | LogicalPlan::GrantPrivileges { .. }
        | LogicalPlan::RevokePrivileges { .. }
        | LogicalPlan::AlterDefaultPrivileges { .. }
        | LogicalPlan::GrantRole { .. }
        | LogicalPlan::RevokeRole { .. }
        | LogicalPlan::DropTable { .. }
        | LogicalPlan::AlterTable { .. }
        | LogicalPlan::CreateSequence { .. }
        | LogicalPlan::AlterSequence { .. }
        | LogicalPlan::DropSequence { .. }
        | LogicalPlan::Comment { .. }
        | LogicalPlan::Begin { .. }
        | LogicalPlan::Commit { .. }
        | LogicalPlan::Rollback { .. }
        | LogicalPlan::Savepoint { .. }
        | LogicalPlan::RollbackToSavepoint { .. }
        | LogicalPlan::ReleaseSavepoint { .. }
        | LogicalPlan::PrepareTransaction { .. }
        | LogicalPlan::CommitPrepared { .. }
        | LogicalPlan::RollbackPrepared { .. }
        | LogicalPlan::SetTransaction { .. }
        | LogicalPlan::SetVariable { .. }
        | LogicalPlan::Describe { .. }
        | LogicalPlan::Checkpoint { .. }
        | LogicalPlan::SetRole { .. }
        | LogicalPlan::Listen { .. }
        | LogicalPlan::Notify { .. }
        | LogicalPlan::Unlisten { .. }
        | LogicalPlan::Copy { input: None, .. } => {}
    }
}

#[test]
fn binds_window_functions_and_preserves_result_types() {
    let plan = parse_bind_ok(
        "SELECT id, row_number() OVER (PARTITION BY name ORDER BY score DESC NULLS FIRST) AS rn \
         FROM users",
    );
    let LogicalPlan::Project { schema, .. } = &plan else {
        panic!("expected Project");
    };
    assert_eq!(schema.field_at(1).name, "rn");
    assert_eq!(schema.field_at(1).data_type, DataType::Int64);

    let mut funcs = Vec::new();
    collect_window_funcs(&plan, &mut funcs);
    assert!(matches!(funcs.as_slice(), [LogicalWindowFunc::RowNumber]));
    let dump = plan.display(0);
    assert!(dump.contains("Window: $wn_0 = RowNumber"));
    assert!(dump.contains("PARTITION BY [name]"));
    assert!(dump.contains("ORDER BY [score DESC]"));
}

#[test]
fn binds_offset_and_value_window_function_arguments() {
    let plan = parse_bind_ok(
        "SELECT \
            lag(score, 2, -1.5) OVER (ORDER BY id) AS lag_score, \
            lead(name, 1, 'n/a') OVER (ORDER BY id) AS lead_name, \
            first_value(score) OVER (ORDER BY id) AS first_score, \
            last_value(score) OVER (ORDER BY id) AS last_score, \
            nth_value(score, 2) OVER (ORDER BY id) AS nth_score, \
            ntile(4) OVER (ORDER BY id) AS bucket \
         FROM users",
    );
    let mut funcs = Vec::new();
    collect_window_funcs(&plan, &mut funcs);
    assert_eq!(funcs.len(), 6);
    assert!(funcs.iter().any(|func| {
        matches!(
            func,
            LogicalWindowFunc::Lag {
                offset: 2,
                default: Value::Decimal {
                    value: -15,
                    scale: 1
                },
                ..
            }
        )
    }));
    assert!(funcs.iter().any(|func| {
        matches!(
            func,
            LogicalWindowFunc::Lead {
                offset: 1,
                default: Value::Text(v),
                ..
            } if v == "n/a"
        )
    }));
    assert!(
        funcs
            .iter()
            .any(|func| matches!(func, LogicalWindowFunc::FirstValue(_)))
    );
    assert!(
        funcs
            .iter()
            .any(|func| matches!(func, LogicalWindowFunc::LastValue(_)))
    );
    assert!(
        funcs
            .iter()
            .any(|func| matches!(func, LogicalWindowFunc::NthValue { n: 2, .. }))
    );
    assert!(
        funcs
            .iter()
            .any(|func| matches!(func, LogicalWindowFunc::Ntile(4)))
    );
}

#[test]
fn rejects_malformed_window_function_calls() {
    let cat = users_catalog();
    for sql in [
        "SELECT row_number(1) OVER () FROM users",
        "SELECT lag(score, score) OVER () FROM users",
        "SELECT lag(score, 1, score) OVER () FROM users",
        "SELECT nth_value(score, 0) OVER () FROM users",
        "SELECT ntile(0) OVER () FROM users",
        "SELECT mystery_window(score) OVER () FROM users",
    ] {
        let err = parse_and_bind(sql, &cat).expect_err(sql);
        assert!(
            matches!(err, PlanError::TypeMismatch(_)) || err.is_not_supported(),
            "{sql}: {err:?}"
        );
    }
}

#[test]
fn rejects_distinct_window_function_calls() {
    let cat = users_catalog();
    let sql = "SELECT first_value(DISTINCT score) OVER () FROM users";
    let err = parse_and_bind(sql, &cat).expect_err(sql);
    assert!(err.is_not_supported(), "{err:?}");
    assert!(
        err.to_string().contains("DISTINCT window function"),
        "{err:?}"
    );
}

#[test]
fn statement_family_display_schema_and_pipeline_modes_are_stable() {
    let cases = [
        (
            "SELECT id FROM users WHERE score > 1 ORDER BY id LIMIT 2",
            "Project:",
            PipelineMode::VectorizedOlap,
        ),
        (
            "EXPLAIN (FORMAT JSON) SELECT id FROM users",
            "Explain (JSON)",
            PipelineMode::ScalarOltp,
        ),
        (
            "COPY users (id, name) FROM STDIN WITH (FORMAT CSV, HEADER)",
            "Copy: users (0,1) FROM STDIN FORMAT=CSV",
            PipelineMode::ScalarOltp,
        ),
        (
            "COPY (SELECT id FROM users) TO STDOUT WITH (FORMAT PARQUET)",
            "Copy: <query> (*) TO STDOUT FORMAT=PARQUET",
            PipelineMode::ScalarOltp,
        ),
        (
            "CREATE TABLE IF NOT EXISTS accounts (id INT PRIMARY KEY, amount INT CHECK (amount > 0), UNIQUE (amount))",
            "CreateTable: public.accounts IF NOT EXISTS",
            PipelineMode::ScalarOltp,
        ),
        (
            "CREATE MATERIALIZED VIEW IF NOT EXISTS user_mv (user_id, username) AS SELECT id, name FROM users",
            "CreateMaterializedView: public.user_mv IF NOT EXISTS",
            PipelineMode::ScalarOltp,
        ),
        (
            "CREATE TYPE mood AS ENUM ('sad', 'ok')",
            "CreateTypeEnum: public.mood",
            PipelineMode::ScalarOltp,
        ),
        (
            "CREATE TYPE postal_address AS (street TEXT, zip INT)",
            "CreateTypeComposite: public.postal_address",
            PipelineMode::ScalarOltp,
        ),
        (
            "CREATE DOMAIN positive_int AS INT NOT NULL CHECK (VALUE > 0)",
            "CreateDomain: public.positive_int",
            PipelineMode::ScalarOltp,
        ),
        (
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS users_id_idx ON users USING hash (id)",
            "CreateIndex Concurrently IF NOT EXISTS: users_id_idx",
            PipelineMode::ScalarOltp,
        ),
        (
            "DROP TABLE IF EXISTS users CASCADE",
            "DropTable IF EXISTS: tables=[users] CASCADE",
            PipelineMode::ScalarOltp,
        ),
        (
            "ALTER TABLE users ADD COLUMN extra INTEGER",
            "AlterTable: users ADD COLUMN extra",
            PipelineMode::ScalarOltp,
        ),
        (
            "CREATE POLICY user_tenant ON users USING (name = current_setting('ultrasql.tenant_id', true))",
            "CreatePolicy: user_tenant ON users",
            PipelineMode::ScalarOltp,
        ),
        (
            "CREATE ROLE app_user LOGIN",
            "CreateRole: app_user",
            PipelineMode::ScalarOltp,
        ),
        (
            "ALTER ROLE app_user NOLOGIN",
            "AlterRole: app_user",
            PipelineMode::ScalarOltp,
        ),
        (
            "DROP ROLE IF EXISTS app_user",
            "DropRole IF EXISTS: roles=[app_user]",
            PipelineMode::ScalarOltp,
        ),
        (
            "GRANT SELECT ON TABLE users TO app_user WITH GRANT OPTION",
            "GrantPrivileges: Table objects=[users] grantees=[app_user] WITH GRANT OPTION",
            PipelineMode::ScalarOltp,
        ),
        (
            "REVOKE SELECT ON TABLE users FROM app_user CASCADE",
            "RevokePrivileges: Table objects=[users] grantees=[app_user] CASCADE",
            PipelineMode::ScalarOltp,
        ),
        (
            "ALTER DEFAULT PRIVILEGES GRANT SELECT ON TABLES TO app_user",
            "AlterDefaultPrivileges: Grant Table",
            PipelineMode::ScalarOltp,
        ),
        (
            "GRANT app_role TO app_user WITH ADMIN OPTION",
            "GrantRole: roles=[app_role] grantees=[app_user] WITH ADMIN OPTION",
            PipelineMode::ScalarOltp,
        ),
        (
            "REVOKE app_role FROM app_user CASCADE",
            "RevokeRole: roles=[app_role] grantees=[app_user] CASCADE",
            PipelineMode::ScalarOltp,
        ),
        (
            "CREATE SEQUENCE IF NOT EXISTS s START WITH 10",
            "CreateSequence IF NOT EXISTS: public.s",
            PipelineMode::ScalarOltp,
        ),
        (
            "ALTER SEQUENCE s INCREMENT BY 2",
            "AlterSequence: s",
            PipelineMode::ScalarOltp,
        ),
        (
            "DROP SEQUENCE IF EXISTS s CASCADE",
            "DropSequence IF EXISTS: sequences=[s] CASCADE",
            PipelineMode::ScalarOltp,
        ),
        (
            "COMMENT ON TABLE users IS 'hello'",
            "Comment: TABLE users SET",
            PipelineMode::ScalarOltp,
        ),
        (
            "BEGIN ISOLATION LEVEL SERIALIZABLE",
            "Begin",
            PipelineMode::ScalarOltp,
        ),
        ("COMMIT", "Commit", PipelineMode::ScalarOltp),
        ("ROLLBACK", "Rollback", PipelineMode::ScalarOltp),
        ("SAVEPOINT sp1", "Savepoint: sp1", PipelineMode::ScalarOltp),
        (
            "ROLLBACK TO SAVEPOINT sp1",
            "RollbackToSavepoint: sp1",
            PipelineMode::ScalarOltp,
        ),
        (
            "RELEASE SAVEPOINT sp1",
            "ReleaseSavepoint: sp1",
            PipelineMode::ScalarOltp,
        ),
        (
            "PREPARE TRANSACTION 'gid1'",
            "PrepareTransaction: gid1",
            PipelineMode::ScalarOltp,
        ),
        (
            "COMMIT PREPARED 'gid1'",
            "CommitPrepared: gid1",
            PipelineMode::ScalarOltp,
        ),
        (
            "ROLLBACK PREPARED 'gid1'",
            "RollbackPrepared: gid1",
            PipelineMode::ScalarOltp,
        ),
        (
            "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ",
            "SetTransaction: RepeatableRead",
            PipelineMode::ScalarOltp,
        ),
        (
            "SET search_path TO public",
            "SetVariable: Set search_path=public",
            PipelineMode::ScalarOltp,
        ),
        (
            "SET ROLE app_user",
            "SetRole: app_user",
            PipelineMode::ScalarOltp,
        ),
        (
            "LISTEN changes",
            "Listen: changes",
            PipelineMode::ScalarOltp,
        ),
        (
            "NOTIFY changes, 'payload'",
            "Notify: changes 'payload'",
            PipelineMode::ScalarOltp,
        ),
        ("UNLISTEN *", "Unlisten: *", PipelineMode::ScalarOltp),
    ];

    let cat = users_catalog();
    for (sql, expected_display, expected_mode) in cases {
        let plan = parse_and_bind(sql, &cat).expect(sql);
        assert_eq!(plan.pipeline_mode(), expected_mode, "{sql}");
        let _schema = plan.schema();
        let display = plan.display(0);
        assert!(
            display.contains(expected_display),
            "{sql}: expected {expected_display:?}, got {display:?}"
        );
        assert_eq!(format!("{plan}"), display);
    }
}
