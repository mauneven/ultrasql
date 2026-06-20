//! Binder tests for window functions and positional GROUP BY / ORDER BY
//! ordinal resolution.

use ultrasql_core::{DataType, Value};

use super::*;
use crate::plan::PipelineMode;
use crate::LogicalWindowFunc;

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
        | LogicalPlan::Pivot { input, .. }
        | LogicalPlan::Unpivot { input, .. }
        | LogicalPlan::LockRows { input, .. }
        | LogicalPlan::Update { input, .. }
        | LogicalPlan::Delete { input, .. } => collect_window_funcs(input, out),
        LogicalPlan::Insert { source, .. } => collect_window_funcs(source, out),
        LogicalPlan::Merge { source, .. } => collect_window_funcs(source, out),
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
        | LogicalPlan::CreateView { source, .. }
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
        | LogicalPlan::AlterView { .. }
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
        | LogicalPlan::Summarize { .. }
        | LogicalPlan::Checkpoint { .. }
        | LogicalPlan::ExportDatabase { .. }
        | LogicalPlan::ImportDatabase { .. }
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

// -----------------------------------------------------------------------
// Positional ORDER BY / GROUP BY ordinals
//
// A bare integer in ORDER BY / GROUP BY is a 1-based reference to the Nth
// SELECT output column (PostgreSQL semantics), NOT an integer constant.
// Regression for the bug where `GROUP BY 1` bound to a constant literal
// (collapsing every row into one group) and `ORDER BY 1` became a no-op
// sort on a constant.
// -----------------------------------------------------------------------

fn positional_find_aggregate(plan: &LogicalPlan) -> Option<&LogicalPlan> {
    match plan {
        LogicalPlan::Aggregate { .. } => Some(plan),
        LogicalPlan::Project { input, .. }
        | LogicalPlan::Filter { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. } => positional_find_aggregate(input),
        _ => None,
    }
}

fn positional_find_sort(plan: &LogicalPlan) -> Option<&LogicalPlan> {
    match plan {
        LogicalPlan::Sort { .. } => Some(plan),
        LogicalPlan::Project { input, .. }
        | LogicalPlan::Filter { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::Limit { input, .. } => positional_find_sort(input),
        _ => None,
    }
}

#[test]
fn group_by_positional_ordinal_resolves_to_output_column() {
    let plan = parse_bind_ok("SELECT name, count(*) FROM users GROUP BY 1");
    let agg = positional_find_aggregate(&plan).expect("Aggregate node");
    let LogicalPlan::Aggregate { group_by, .. } = agg else {
        panic!("expected Aggregate, got {agg:?}");
    };
    assert_eq!(group_by.len(), 1, "GROUP BY 1 must produce exactly one key");
    assert!(
        matches!(&group_by[0], ScalarExpr::Column { name, .. } if name == "name"),
        "GROUP BY 1 must resolve to the first output column `name`, got {:?}",
        group_by[0]
    );
}

#[test]
fn group_by_positional_matches_named_column() {
    // `GROUP BY 1` must bind to exactly the same key as `GROUP BY name`.
    let by_pos = parse_bind_ok("SELECT name, count(*) FROM users GROUP BY 1");
    let by_name = parse_bind_ok("SELECT name, count(*) FROM users GROUP BY name");
    let key = |plan: &LogicalPlan| {
        let LogicalPlan::Aggregate { group_by, .. } =
            positional_find_aggregate(plan).expect("Aggregate")
        else {
            panic!("expected Aggregate");
        };
        group_by.clone()
    };
    assert_eq!(
        key(&by_pos),
        key(&by_name),
        "GROUP BY 1 and GROUP BY name must produce identical group keys"
    );
}

#[test]
fn order_by_positional_ordinal_resolves_to_output_column() {
    let plan = parse_bind_ok("SELECT id, name FROM users ORDER BY 2 DESC");
    let sort = positional_find_sort(&plan).expect("Sort node");
    let LogicalPlan::Sort { keys, .. } = sort else {
        panic!("expected Sort, got {sort:?}");
    };
    assert_eq!(keys.len(), 1, "one sort key");
    assert!(!keys[0].asc, "ORDER BY 2 DESC must sort descending");
    assert!(
        matches!(&keys[0].expr, ScalarExpr::Column { name, .. } if name == "name"),
        "ORDER BY 2 must resolve to the second output column `name`, got {:?}",
        keys[0].expr
    );
}

#[test]
fn order_by_positional_is_not_a_constant_literal() {
    // The core of the original bug: `ORDER BY 1` must NOT bind to a constant.
    let plan = parse_bind_ok("SELECT name, id FROM users ORDER BY 1");
    let sort = positional_find_sort(&plan).expect("Sort node");
    let LogicalPlan::Sort { keys, .. } = sort else {
        panic!("expected Sort, got {sort:?}");
    };
    assert!(
        !matches!(keys[0].expr, ScalarExpr::Literal { .. }),
        "ORDER BY 1 must not be a constant literal sort key, got {:?}",
        keys[0].expr
    );
    assert!(
        matches!(&keys[0].expr, ScalarExpr::Column { name, .. } if name == "name"),
        "ORDER BY 1 must resolve to the first output column `name`, got {:?}",
        keys[0].expr
    );
}

#[test]
fn group_by_and_order_by_positional_combined() {
    let plan = parse_bind_ok("SELECT name, count(*) AS c FROM users GROUP BY 1 ORDER BY 2 DESC");
    let LogicalPlan::Aggregate { group_by, .. } =
        positional_find_aggregate(&plan).expect("Aggregate")
    else {
        panic!("expected Aggregate");
    };
    assert!(
        matches!(&group_by[0], ScalarExpr::Column { name, .. } if name == "name"),
        "GROUP BY 1 must resolve to `name`, got {:?}",
        group_by[0]
    );
    let LogicalPlan::Sort { keys, .. } = positional_find_sort(&plan).expect("Sort") else {
        panic!("expected Sort");
    };
    // ORDER BY 2 -> the count aggregate output column, descending.
    assert!(!keys[0].asc, "ORDER BY 2 DESC");
    assert!(
        !matches!(keys[0].expr, ScalarExpr::Literal { .. }),
        "ORDER BY 2 must not be a constant literal, got {:?}",
        keys[0].expr
    );
}

#[test]
fn order_by_positional_in_set_operation() {
    // Set-operation ORDER BY also resolves ordinals against the output list.
    let plan = parse_bind_ok("SELECT id FROM users UNION SELECT id FROM users ORDER BY 1");
    let LogicalPlan::Sort { keys, .. } = positional_find_sort(&plan).expect("Sort") else {
        panic!("expected Sort");
    };
    assert!(
        matches!(&keys[0].expr, ScalarExpr::Column { .. }),
        "ORDER BY 1 over a UNION must resolve to a column reference, got {:?}",
        keys[0].expr
    );
}

#[test]
fn positional_ordinal_out_of_range_is_rejected() {
    let order_err = parse_and_bind("SELECT id, name FROM users ORDER BY 5", &users_catalog())
        .expect_err("ORDER BY position out of range must error");
    assert!(
        matches!(order_err, PlanError::TypeMismatch(_)),
        "expected TypeMismatch for out-of-range ORDER BY, got {order_err:?}"
    );
    let group_err = parse_and_bind(
        "SELECT name, count(*) FROM users GROUP BY 9",
        &users_catalog(),
    )
    .expect_err("GROUP BY position out of range must error");
    assert!(
        matches!(group_err, PlanError::TypeMismatch(_)),
        "expected TypeMismatch for out-of-range GROUP BY, got {group_err:?}"
    );
}

#[test]
fn group_by_positional_referencing_aggregate_is_rejected() {
    // `GROUP BY 2` where output column 2 is an aggregate is an error,
    // matching PostgreSQL.
    let err = parse_and_bind(
        "SELECT name, count(*) FROM users GROUP BY 2",
        &users_catalog(),
    )
    .expect_err("GROUP BY referencing an aggregate must error");
    assert!(
        matches!(err, PlanError::TypeMismatch(_)),
        "expected TypeMismatch for GROUP BY of an aggregate, got {err:?}"
    );
}
