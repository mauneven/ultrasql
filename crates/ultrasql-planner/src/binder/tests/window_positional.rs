//! Binder tests for window functions and positional GROUP BY / ORDER BY
//! ordinal resolution.

use ultrasql_core::{DataType, Value};

use super::*;
use crate::LogicalWindowFunc;
use crate::plan::PipelineMode;

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

/// Fetch the ORDER BY keys of the first (outermost) Window node.
fn first_window_order_by(plan: &LogicalPlan) -> Option<&[crate::plan::SortKey]> {
    match plan {
        LogicalPlan::Window { order_by, .. } => Some(order_by),
        LogicalPlan::Project { input, .. }
        | LogicalPlan::Filter { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. } => first_window_order_by(input),
        _ => None,
    }
}

#[test]
fn window_order_by_default_nulls_follows_direction() {
    // PostgreSQL: ASC default -> NULLS LAST (nulls_first=false), DESC default
    // -> NULLS FIRST (nulls_first=true). The window binder previously resolved
    // Default to NULLS LAST unconditionally, ignoring direction (BUG 1).
    let asc = parse_bind_ok("SELECT sum(id) OVER (ORDER BY score) FROM users");
    let keys = first_window_order_by(&asc).expect("window order by");
    assert_eq!(keys.len(), 1);
    assert!(keys[0].asc, "ASC default");
    assert!(
        !keys[0].nulls_first,
        "ASC default must be NULLS LAST (nulls_first=false)"
    );

    let desc = parse_bind_ok("SELECT sum(id) OVER (ORDER BY score DESC) FROM users");
    let keys = first_window_order_by(&desc).expect("window order by");
    assert_eq!(keys.len(), 1);
    assert!(!keys[0].asc, "DESC");
    assert!(
        keys[0].nulls_first,
        "DESC default must be NULLS FIRST (nulls_first=true)"
    );
}

#[test]
fn window_order_by_explicit_nulls_honored_under_desc() {
    // Explicit NULLS FIRST/LAST must override the direction default for DESC.
    let first = parse_bind_ok("SELECT sum(id) OVER (ORDER BY score DESC NULLS LAST) FROM users");
    let keys = first_window_order_by(&first).expect("window order by");
    assert!(!keys[0].asc);
    assert!(
        !keys[0].nulls_first,
        "explicit NULLS LAST under DESC must stay nulls_first=false"
    );

    let last = parse_bind_ok("SELECT sum(id) OVER (ORDER BY score ASC NULLS FIRST) FROM users");
    let keys = first_window_order_by(&last).expect("window order by");
    assert!(keys[0].asc);
    assert!(
        keys[0].nulls_first,
        "explicit NULLS FIRST under ASC must stay nulls_first=true"
    );
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

fn first_window_frame(plan: &LogicalPlan) -> Option<&crate::plan::LogicalWindowFrame> {
    match plan {
        LogicalPlan::Window { frame, .. } => Some(frame),
        LogicalPlan::Project { input, .. } => first_window_frame(input),
        _ => None,
    }
}

#[test]
fn binds_aggregate_window_functions_with_result_types() {
    use crate::plan::WindowAggKind;
    let plan = parse_bind_ok(
        "SELECT \
            sum(id)   OVER (ORDER BY id) AS s, \
            avg(id)   OVER (ORDER BY id) AS a, \
            count(id) OVER (ORDER BY id) AS c, \
            count(*)  OVER (ORDER BY id) AS cs, \
            min(score) OVER (ORDER BY id) AS mn, \
            max(score) OVER (ORDER BY id) AS mx \
         FROM users",
    );
    let LogicalPlan::Project { schema, .. } = &plan else {
        panic!("expected Project");
    };
    // sum(int) -> Int64, avg -> Float64, count/count(*) -> Int64,
    // min/max(float) -> Float64.
    assert_eq!(schema.field_at(0).data_type, DataType::Int64);
    assert_eq!(schema.field_at(1).data_type, DataType::Float64);
    assert_eq!(schema.field_at(2).data_type, DataType::Int64);
    assert_eq!(schema.field_at(3).data_type, DataType::Int64);
    assert_eq!(schema.field_at(4).data_type, DataType::Float64);
    assert_eq!(schema.field_at(5).data_type, DataType::Float64);

    let mut funcs = Vec::new();
    collect_window_funcs(&plan, &mut funcs);
    assert!(funcs.iter().any(|f| matches!(
        f,
        LogicalWindowFunc::Aggregate {
            kind: WindowAggKind::Sum,
            ..
        }
    )));
    assert!(
        funcs
            .iter()
            .any(|f| matches!(f, LogicalWindowFunc::CountStar))
    );
}

#[test]
fn default_frame_is_range_running_with_order_by() {
    use crate::plan::{BoundFrameBound, BoundFrameUnits};
    let plan = parse_bind_ok("SELECT sum(id) OVER (ORDER BY id) FROM users");
    let frame = first_window_frame(&plan).expect("frame");
    // PG default with ORDER BY: RANGE BETWEEN UNBOUNDED PRECEDING AND
    // CURRENT ROW (NOT ROWS — proving the stepped per-peer semantics).
    assert_eq!(frame.units, BoundFrameUnits::Range);
    assert_eq!(frame.start, BoundFrameBound::UnboundedPreceding);
    assert_eq!(frame.end, BoundFrameBound::CurrentRow);
}

#[test]
fn default_frame_is_whole_partition_without_order_by() {
    use crate::plan::{BoundFrameBound, BoundFrameUnits};
    let plan = parse_bind_ok("SELECT sum(id) OVER (PARTITION BY name) FROM users");
    let frame = first_window_frame(&plan).expect("frame");
    assert_eq!(frame.units, BoundFrameUnits::Range);
    assert_eq!(frame.start, BoundFrameBound::UnboundedPreceding);
    assert_eq!(frame.end, BoundFrameBound::UnboundedFollowing);
}

#[test]
fn ranking_functions_drop_user_frame() {
    // Ranking functions ignore the frame: the binder stores the
    // whole-partition frame regardless of the explicit ROWS frame.
    let plan = parse_bind_ok(
        "SELECT rank() OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM users",
    );
    let frame = first_window_frame(&plan).expect("frame");
    assert!(frame.is_whole_partition_default());
}

#[test]
fn explicit_rows_frame_binds_correctly() {
    use crate::plan::{BoundFrameBound, BoundFrameUnits};
    let plan = parse_bind_ok(
        "SELECT sum(id) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM users",
    );
    let frame = first_window_frame(&plan).expect("frame");
    assert_eq!(frame.units, BoundFrameUnits::Rows);
    assert!(matches!(frame.start, BoundFrameBound::Preceding(_)));
    assert!(matches!(frame.end, BoundFrameBound::Following(_)));
}

#[test]
fn frame_validation_errors_fire() {
    let cat = users_catalog();
    let cases: &[(&str, &str)] = &[
        (
            "SELECT sum(id) OVER (ORDER BY id, score RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM users",
            "exactly one ORDER BY column",
        ),
        (
            "SELECT sum(id) OVER (GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM users",
            "GROUPS mode requires an ORDER BY clause",
        ),
        (
            "SELECT sum(id) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED FOLLOWING AND CURRENT ROW) FROM users",
            "frame start cannot be UNBOUNDED FOLLOWING",
        ),
        (
            "SELECT sum(id) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND UNBOUNDED PRECEDING) FROM users",
            "frame end cannot be UNBOUNDED PRECEDING",
        ),
        (
            "SELECT sum(id) OVER (ORDER BY id ROWS BETWEEN 1 FOLLOWING AND CURRENT ROW) FROM users",
            "frame starting from following row cannot have preceding rows",
        ),
        (
            "SELECT sum(id) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND 1 PRECEDING) FROM users",
            "frame starting from current row cannot have preceding rows",
        ),
    ];
    for (sql, needle) in cases {
        let err = parse_and_bind(sql, &cat).expect_err(sql);
        assert!(
            matches!(&err, PlanError::InvalidWindowFrame(m) if m.contains(needle)),
            "{sql}: expected '{needle}', got {err:?}"
        );
    }
}

#[test]
fn range_offset_on_text_order_column_is_unsupported() {
    let cat = users_catalog();
    let sql =
        "SELECT sum(id) OVER (ORDER BY name RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM users";
    let err = parse_and_bind(sql, &cat).expect_err(sql);
    assert!(err.is_not_supported(), "{err:?}");
    assert!(
        err.to_string().contains("not supported for column type"),
        "{err:?}"
    );
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

// -----------------------------------------------------------------------
// Window calls nested inside value expressions (function args, CASE,
// COALESCE, cast, IN list, BETWEEN, ...) must be lifted into their own
// `$wn_N` column, exactly as a top-level window call is. PostgreSQL accepts
// a window function anywhere a value expression is allowed.
// -----------------------------------------------------------------------

/// Count how many `$wn_N` Window operators a plan stacks.
fn count_window_funcs(plan: &LogicalPlan) -> usize {
    let mut funcs = Vec::new();
    collect_window_funcs(plan, &mut funcs);
    funcs.len()
}

#[test]
fn window_call_nested_in_coalesce_is_lifted() {
    let plan = parse_bind_ok("SELECT COALESCE(sum(score) OVER (ORDER BY id), 0) FROM users");
    assert_eq!(
        count_window_funcs(&plan),
        1,
        "the window call inside COALESCE must be lifted into one Window node"
    );
    let dump = plan.display(0);
    assert!(dump.contains("$wn_0"), "synthetic column emitted: {dump}");
}

#[test]
fn window_call_nested_in_case_is_lifted() {
    let plan = parse_bind_ok(
        "SELECT CASE WHEN id > 1 THEN row_number() OVER (ORDER BY id) ELSE 0 END FROM users",
    );
    assert_eq!(count_window_funcs(&plan), 1);
    let mut funcs = Vec::new();
    collect_window_funcs(&plan, &mut funcs);
    assert!(matches!(funcs.as_slice(), [LogicalWindowFunc::RowNumber]));
}

#[test]
fn window_call_as_function_argument_is_lifted() {
    // pg_typeof(avg(v) OVER ()) — the avg-result type itself is a separate
    // known issue; we only assert the window call is lifted and the query
    // plans.
    let plan = parse_bind_ok("SELECT pg_typeof(avg(score) OVER ()) FROM users");
    assert_eq!(count_window_funcs(&plan), 1);
}

#[test]
fn multiple_distinct_window_calls_in_one_expression_each_lifted() {
    let plan = parse_bind_ok(
        "SELECT COALESCE(rank() OVER (ORDER BY id) + dense_rank() OVER (ORDER BY id), 0) \
         FROM users",
    );
    assert_eq!(
        count_window_funcs(&plan),
        2,
        "rank() and dense_rank() must each get their own $wn_N"
    );
    let mut funcs = Vec::new();
    collect_window_funcs(&plan, &mut funcs);
    assert!(funcs.iter().any(|f| matches!(f, LogicalWindowFunc::Rank)));
    assert!(
        funcs
            .iter()
            .any(|f| matches!(f, LogicalWindowFunc::DenseRank))
    );
    let dump = plan.display(0);
    assert!(dump.contains("$wn_0") && dump.contains("$wn_1"), "{dump}");
}

#[test]
fn window_call_inside_cast_is_lifted() {
    let plan = parse_bind_ok("SELECT CAST(row_number() OVER (ORDER BY id) AS BIGINT) FROM users");
    assert_eq!(count_window_funcs(&plan), 1);
}

#[test]
fn window_call_inside_in_list_is_lifted() {
    let plan = parse_bind_ok("SELECT row_number() OVER (ORDER BY id) IN (1, 2) FROM users");
    assert_eq!(count_window_funcs(&plan), 1);
}

#[test]
fn window_call_inside_between_is_lifted() {
    let plan =
        parse_bind_ok("SELECT id BETWEEN 0 AND sum(id) OVER (ORDER BY id) AS in_range FROM users");
    assert_eq!(count_window_funcs(&plan), 1);
}

#[test]
fn window_in_window_over_clause_is_rejected() {
    // row_number() OVER (ORDER BY (rank() OVER ())) — a window call inside
    // another window's ORDER BY is illegal (PG 42P20).
    let cat = users_catalog();
    let sql = "SELECT row_number() OVER (ORDER BY (rank() OVER ())) FROM users";
    let err = parse_and_bind(sql, &cat).expect_err(sql);
    assert!(
        matches!(&err, PlanError::InvalidWindowFrame(m) if m.contains("cannot be nested")),
        "expected window-in-window rejection, got {err:?}"
    );
}

#[test]
fn window_in_window_argument_is_rejected() {
    let cat = users_catalog();
    let sql = "SELECT sum(rank() OVER ()) OVER (ORDER BY id) FROM users";
    let err = parse_and_bind(sql, &cat).expect_err(sql);
    assert!(
        matches!(&err, PlanError::InvalidWindowFrame(m) if m.contains("cannot be nested")),
        "expected window-in-window rejection, got {err:?}"
    );
}

#[test]
fn aggregate_of_window_call_is_rejected() {
    // sum(count(*) OVER ()) — a plain aggregate cannot aggregate a window
    // result (PG 42P20).
    let cat = users_catalog();
    let sql = "SELECT sum(count(*) OVER ()) FROM users";
    let err = parse_and_bind(sql, &cat).expect_err(sql);
    assert!(
        matches!(
            &err,
            PlanError::InvalidWindowFrame(m)
                if m.contains("aggregate function calls cannot contain window")
        ),
        "expected aggregate-of-window rejection, got {err:?}"
    );
}

#[test]
fn window_call_in_where_is_still_rejected() {
    // The window-extraction pass only runs on the projection; a window call
    // in WHERE never lifts and must still error.
    let cat = users_catalog();
    let sql = "SELECT id FROM users WHERE row_number() OVER () = 1";
    let err = parse_and_bind(sql, &cat).expect_err(sql);
    // Any planning error is acceptable; the point is the query does NOT plan.
    let _ = err;
}

#[test]
fn window_call_inside_subquery_does_not_leak_to_outer_query() {
    // The window call belongs to the scalar subquery's own SELECT, so the
    // OUTER query must wrap zero Window nodes (the subquery is planned
    // separately and is not flattened into the outer plan here).
    let plan = parse_bind_ok(
        "SELECT (SELECT row_number() OVER (ORDER BY id) FROM users LIMIT 1) AS sub FROM users",
    );
    assert_eq!(
        count_window_funcs(&plan),
        0,
        "the subquery's window call must not be lifted into the outer query"
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
