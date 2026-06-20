//! Binder tests for MERGE and the core DML statements (INSERT, UPDATE,
//! DELETE, TRUNCATE).

use ultrasql_core::DataType;

use super::*;
use crate::plan::{LogicalMergeAction, LogicalMergeMatchKind};
use crate::{LogicalPrivilegeKind, LogicalPrivilegeObjectKind};

#[test]
fn binds_merge_update_delete_insert_clauses() {
    let plan = parse_bind_ok(
        "MERGE INTO users AS u \
         USING users AS s \
         ON u.id = s.id \
         WHEN MATCHED AND s.score IS NULL THEN DELETE \
         WHEN MATCHED THEN UPDATE SET name = s.name, score = s.score \
         WHEN NOT MATCHED THEN INSERT (id, name, score) VALUES (s.id, s.name, s.score)",
    );
    let LogicalPlan::Merge {
        target,
        source,
        on,
        clauses,
        schema,
        ..
    } = plan
    else {
        panic!("expected Merge");
    };

    assert_eq!(target, "users");
    assert!(matches!(source.as_ref(), LogicalPlan::Scan { table, .. } if table == "users"));
    assert_eq!(on.data_type(), DataType::Bool);
    assert!(schema.is_empty());
    assert_eq!(clauses.len(), 3);
    assert_eq!(clauses[0].kind, LogicalMergeMatchKind::Matched);
    assert!(matches!(clauses[0].action, LogicalMergeAction::Delete));
    assert!(clauses[0].condition.is_some());
    assert_eq!(clauses[1].kind, LogicalMergeMatchKind::Matched);
    let LogicalMergeAction::Update { ref assignments } = clauses[1].action else {
        panic!("expected update action");
    };
    assert_eq!(
        assignments.iter().map(|(idx, _)| *idx).collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(clauses[2].kind, LogicalMergeMatchKind::NotMatched);
    let LogicalMergeAction::Insert {
        ref columns,
        ref values,
    } = clauses[2].action
    else {
        panic!("expected insert action");
    };
    assert_eq!(columns, &[0, 1, 2]);
    assert_eq!(values.len(), 3);
}

#[test]
fn merge_rejects_ambiguous_unqualified_column_references() {
    let cat = users_catalog();
    let err = parse_and_bind(
        "MERGE INTO users AS u \
         USING users AS s \
         ON id = s.id \
         WHEN MATCHED THEN UPDATE SET name = s.name",
        &cat,
    )
    .expect_err("unqualified id is ambiguous between target and source");
    assert!(matches!(err, PlanError::Ambiguous(ref c) if c == "id"));
}

#[test]
fn merge_rejects_non_boolean_on_condition() {
    let cat = users_catalog();
    let err = parse_and_bind(
        "MERGE INTO users AS u \
         USING users AS s \
         ON s.score \
         WHEN MATCHED THEN DELETE",
        &cat,
    )
    .expect_err("ON must be boolean");
    assert!(matches!(err, PlanError::TypeMismatch(msg) if msg.contains("MERGE ON")));
}

#[test]
fn merge_rejects_duplicate_update_target() {
    let cat = users_catalog();
    let err = parse_and_bind(
        "MERGE INTO users AS u \
         USING users AS s \
         ON u.id = s.id \
         WHEN MATCHED THEN UPDATE SET name = s.name, name = 'x'",
        &cat,
    )
    .expect_err("duplicate update target is rejected");
    assert!(matches!(err, PlanError::DuplicateColumn(ref c) if c == "name"));
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
