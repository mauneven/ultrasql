//! Binder tests for assorted statement families: CASE/set-op typing, ORDER BY
//! placement, DESCRIBE/SUMMARIZE, admin statements, privileges and SET.

use ultrasql_core::{DataType, Field, Schema};

use super::*;
use crate::catalog::{InMemoryCatalog, TableMeta};
use crate::{LogicalPrivilegeKind, LogicalPrivilegeObjectKind};

#[test]
fn case_branches_with_incompatible_types_are_rejected() {
    let err = parse_and_bind(
        "SELECT CASE WHEN id > 0 THEN id ELSE name END AS c FROM users",
        &users_catalog(),
    )
    .expect_err("INT vs TEXT CASE branches must not bind");
    assert!(
        matches!(err, PlanError::TypeMismatch(_)),
        "expected TypeMismatch, got {err:?}"
    );
}

#[test]
fn case_branches_reconcile_to_common_numeric_type() {
    // THEN is INT (id), ELSE is FLOAT8 (score): the CASE output must reconcile
    // to the common type FLOAT8, not silently adopt the first branch's INT.
    let plan = parse_bind_ok("SELECT CASE WHEN id > 0 THEN id ELSE score END AS c FROM users");
    assert_eq!(plan.schema().field_at(0).data_type, DataType::Float64);
}

#[test]
fn set_op_with_incompatible_column_types_is_rejected() {
    let err = parse_and_bind(
        "SELECT id FROM users UNION SELECT name FROM users",
        &users_catalog(),
    )
    .expect_err("INT vs TEXT set-op columns must not bind");
    assert!(
        matches!(err, PlanError::TypeMismatch(_)),
        "expected TypeMismatch, got {err:?}"
    );
}

#[test]
fn set_op_with_compatible_column_types_binds() {
    // Sanity: a well-typed UNION must still bind.
    let _ = parse_bind_ok("SELECT id FROM users UNION SELECT id FROM users");
}

#[test]
fn order_by_with_subquery_in_projection_sorts_above_projection() {
    // A projection containing a subquery is not order-preserving (the subquery
    // is later decorrelated into a join that discards input order), so the
    // ORDER BY Sort must sit ABOVE the projection, not be pushed below it.
    let plan =
        parse_bind_ok("SELECT id, (SELECT max(score) FROM users) AS m FROM users ORDER BY id");
    match &plan {
        LogicalPlan::Sort { input, .. } => assert!(
            matches!(input.as_ref(), LogicalPlan::Project { .. }),
            "expected Sort over Project, got Sort over {input:?}"
        ),
        other => panic!("expected a top-level Sort, got {other:?}"),
    }
}

#[test]
fn order_by_without_subquery_keeps_sort_below_projection() {
    // Control: an order-preserving projection keeps the efficient
    // Sort-below-projection form (sort the input, then project).
    let plan = parse_bind_ok("SELECT id, name FROM users ORDER BY id");
    match &plan {
        LogicalPlan::Project { input, .. } => assert!(
            matches!(input.as_ref(), LogicalPlan::Sort { .. }),
            "expected Project over Sort, got Project over {input:?}"
        ),
        other => panic!("expected a top-level Project, got {other:?}"),
    }
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
fn describe_unqualified_object_preserves_any_kind() {
    let plan = parse_bind_ok("DESCRIBE users");
    let LogicalPlan::Describe { target, .. } = plan else {
        panic!("expected Describe plan");
    };
    let LogicalDescribeTarget::Object { kind, .. } = target else {
        panic!("expected Describe object target");
    };
    assert_eq!(kind, LogicalDescribeObjectKind::Any);
}

#[test]
fn describe_view_binds_view_target_metadata() {
    let plan = parse_bind_ok("DESCRIBE VIEW users");
    let LogicalPlan::Describe { target, .. } = plan else {
        panic!("expected Describe plan");
    };
    let LogicalDescribeTarget::Object {
        kind,
        object_schema,
        ..
    } = target
    else {
        panic!("expected Describe object target");
    };
    assert_eq!(kind, LogicalDescribeObjectKind::View);
    assert_eq!(object_schema.len(), 3);
}

#[test]
fn binds_summarize_table_output_schema() {
    let plan = parse_bind_ok("SUMMARIZE users");
    let fields = plan.schema().fields();
    assert_eq!(
        fields
            .iter()
            .map(|field| field.name.as_str())
            .collect::<Vec<_>>(),
        vec![
            "column_name",
            "data_type",
            "row_count",
            "null_count",
            "min",
            "max",
            "unique_count",
            "avg",
            "stddev",
        ]
    );

    let LogicalPlan::Summarize {
        table,
        namespace,
        target_schema,
        ..
    } = plan
    else {
        panic!("expected Summarize plan");
    };
    assert_eq!(table, "users");
    assert_eq!(namespace, "public");
    assert_eq!(target_schema.len(), 3);
}

#[test]
fn summarize_missing_table_errors() {
    let cat = users_catalog();
    let err = parse_and_bind("SUMMARIZE missing_users", &cat).expect_err("missing summarize table");
    assert_eq!(err, PlanError::TableNotFound("missing_users".to_owned()));
}

#[test]
fn binds_checkpoint_as_empty_session_control_plan() {
    let plan = parse_bind_ok("CHECKPOINT");
    assert!(matches!(plan, LogicalPlan::Checkpoint { .. }));
    assert!(plan.schema().is_empty());
}

#[test]
fn binds_export_database_as_empty_admin_plan() {
    let plan = parse_bind_ok("EXPORT DATABASE TO '/tmp/ultra-dump'");
    let LogicalPlan::ExportDatabase { path, schema } = plan else {
        panic!("expected ExportDatabase plan");
    };
    assert_eq!(path, "/tmp/ultra-dump");
    assert!(schema.is_empty());
}

#[test]
fn binds_import_database_as_empty_admin_plan() {
    let plan = parse_bind_ok("IMPORT DATABASE FROM '/tmp/ultra-dump'");
    let LogicalPlan::ImportDatabase { path, schema } = plan else {
        panic!("expected ImportDatabase plan");
    };
    assert_eq!(path, "/tmp/ultra-dump");
    assert!(schema.is_empty());
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
fn binds_set_variable_as_session_setting() {
    let plan = parse_bind_ok("SET VARIABLE ultrasql.tenant TO 'acme'");
    let LogicalPlan::SetVariable {
        name,
        action,
        value,
        ..
    } = plan
    else {
        panic!("expected SetVariable");
    };
    assert_eq!(name, "ultrasql.tenant");
    assert_eq!(action, LogicalSetVariableAction::Set);
    assert_eq!(value.as_deref(), Some("acme"));
}
