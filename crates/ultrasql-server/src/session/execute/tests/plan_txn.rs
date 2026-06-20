//! Plan-shape predicate, command-tag, and transaction-finalisation test coverage.

use super::*;

#[test]
fn plan_shape_predicates_and_command_tags_cover_dml_edges() {
    let scan = scan_plan();
    let filtered = LogicalPlan::Filter {
        input: Box::new(scan.clone()),
        predicate: bool_literal(true),
    };
    let update = LogicalPlan::Update {
        table: "t".to_owned(),
        assignments: vec![(1, int_column("value", 1))],
        input: Box::new(filtered.clone()),
        returning: Vec::new(),
        schema: Schema::empty(),
    };
    assert!(Session::<tokio::io::DuplexStream>::is_fused_update_shape(
        &update
    ));
    let mut returning_update = update.clone();
    if let LogicalPlan::Update { returning, .. } = &mut returning_update {
        returning.push((int_column("id", 0), "id".to_owned()));
    }
    assert!(!Session::<tokio::io::DuplexStream>::is_fused_update_shape(
        &returning_update
    ));
    let delete = LogicalPlan::Delete {
        table: "t".to_owned(),
        input: Box::new(filtered.clone()),
        returning: Vec::new(),
        schema: Schema::empty(),
    };
    assert!(Session::<tokio::io::DuplexStream>::is_fused_delete_shape(
        &delete
    ));
    let mut returning_delete = delete.clone();
    if let LogicalPlan::Delete { returning, .. } = &mut returning_delete {
        returning.push((int_column("id", 0), "id".to_owned()));
    }
    assert!(!Session::<tokio::io::DuplexStream>::is_fused_delete_shape(
        &returning_delete
    ));
    let delete_predicate = ScalarExpr::Binary {
        op: BinaryOp::Lt,
        left: Box::new(int_column("id", 0)),
        right: Box::new(ScalarExpr::Literal {
            value: Value::Int32(100),
            data_type: DataType::Int32,
        }),
        data_type: DataType::Bool,
    };
    let delete_input = LogicalPlan::Filter {
        input: Box::new(scan.clone()),
        predicate: delete_predicate,
    };
    assert_eq!(
        Session::<tokio::io::DuplexStream>::fused_delete_int32_pair_predicate("t", &delete_input),
        Some(Int32PairPredicate::ColumnCmp {
            col_index: 0,
            op: Int32PairCmp::Lt,
            literal: 100,
        })
    );

    let aggregate = LogicalPlan::Aggregate {
        input: Box::new(filtered),
        group_by: Vec::new(),
        aggregates: vec![LogicalAggregateExpr {
            func: AggregateFunc::Sum,
            arg: Some(int_column("value", 1)),
            direct_arg: None,
            order_by: None,
            distinct: false,
            output_name: "sum".to_owned(),
            data_type: DataType::Int64,
        }],
        schema: Schema::new([Field::required("sum", DataType::Int64)]).expect("agg schema"),
    };
    let projected_aggregate = LogicalPlan::Project {
        input: Box::new(aggregate.clone()),
        exprs: vec![(int_column("sum", 0), "sum".to_owned())],
        schema: Schema::new([Field::required("sum", DataType::Int64)]).expect("project schema"),
    };
    assert!(Session::<tokio::io::DuplexStream>::is_scalar_aggregate_shape(&projected_aggregate));
    assert_eq!(
        Session::<tokio::io::DuplexStream>::scalar_aggregate_source_table(&projected_aggregate),
        Some("t".to_owned())
    );
    let mut grouped = aggregate.clone();
    if let LogicalPlan::Aggregate { group_by, .. } = &mut grouped {
        group_by.push(int_column("id", 0));
    }
    assert!(!Session::<tokio::io::DuplexStream>::is_scalar_aggregate_shape(&grouped));

    let insert_values = LogicalPlan::Insert {
        table: "t".to_owned(),
        columns: Vec::new(),
        source: Box::new(LogicalPlan::Values {
            rows: vec![vec![ScalarExpr::Literal {
                value: Value::Int32(1),
                data_type: DataType::Int32,
            }]],
            schema: Schema::new([Field::required("id", DataType::Int32)]).expect("values schema"),
        }),
        on_conflict: None,
        returning: Vec::new(),
        schema: Schema::empty(),
    };
    assert!(Session::<tokio::io::DuplexStream>::is_trivial_insert_values(&insert_values));
    assert_eq!(
        Session::<tokio::io::DuplexStream>::dml_target_table(&insert_values),
        Some("t")
    );
    let mut session = test_session();
    session
        .pending_table_modifications
        .insert("t".to_owned(), u64::MAX);
    let err = session
        .note_dml_effect(&insert_values, 1)
        .expect_err("pending DML counter overflow must not saturate");
    assert_eq!(err.sqlstate(), "22003");
    assert!(session.pending_logical_changes.is_empty());
    assert_eq!(
        Session::<tokio::io::DuplexStream>::dml_change_kind(&insert_values),
        Some(LogicalChangeKind::Insert)
    );
    assert_eq!(
        Session::<tokio::io::DuplexStream>::dml_change_kind(&update),
        Some(LogicalChangeKind::Update)
    );
    let delete = LogicalPlan::Delete {
        table: "t".to_owned(),
        input: Box::new(scan),
        returning: Vec::new(),
        schema: Schema::empty(),
    };
    assert_eq!(
        Session::<tokio::io::DuplexStream>::dml_change_kind(&delete),
        Some(LogicalChangeKind::Delete)
    );
    assert_eq!(
        Session::<tokio::io::DuplexStream>::dml_target_table(&LogicalPlan::Empty {
            schema: Schema::empty(),
        }),
        None
    );

    let messages = vec![BackendMessage::CommandComplete {
        tag: "INSERT 0 9".to_owned(),
    }];
    assert_eq!(
        Session::<tokio::io::DuplexStream>::parse_affected_rows_tag(&messages),
        9
    );
    assert_eq!(
        Session::<tokio::io::DuplexStream>::parse_command_rows_tag(&messages),
        9
    );
    assert_eq!(
        Session::<tokio::io::DuplexStream>::parse_affected_rows_tag(&[
            BackendMessage::CommandComplete {
                tag: "SELECT 9".to_owned(),
            }
        ]),
        0
    );
    assert_eq!(
        Session::<tokio::io::DuplexStream>::parse_command_rows_tag(&[]),
        0
    );
}

#[test]
fn materialized_view_row_flush_rejects_counter_overflow() {
    let mut session = test_session();
    let runtime = Arc::new(crate::MaterializedViewRuntime {
        view_table: "mv_t".to_owned(),
        source_table: "t".to_owned(),
        source: scan_plan(),
        materialized_rows: std::sync::atomic::AtomicU64::new(u64::MAX),
    });
    session
        .pending_materialized_view_rows
        .push((Arc::clone(&runtime), 1));

    let err = session
        .flush_pending_materialized_view_rows()
        .expect_err("materialized view row counter overflow must not wrap");

    assert_eq!(err.sqlstate(), "22003");
    assert_eq!(
        runtime
            .materialized_rows
            .load(std::sync::atomic::Ordering::Acquire),
        u64::MAX
    );
}

#[test]
fn finalise_autocommit_reports_abort_failure_with_original_error() {
    let mut session = test_session();
    let txn = session
        .state
        .txn_manager
        .begin(IsolationLevel::ReadCommitted);
    let stale = txn.clone();
    session.state.txn_manager.abort(txn).expect("pre-abort");

    let err = session
        .finalise_autocommit(
            &scan_plan(),
            stale,
            Err(ServerError::Unsupported("executor boom")),
        )
        .expect_err("autocommit cleanup failure must be visible");
    let msg = err.to_string();
    assert!(
        msg.contains("autocommit rollback after statement error"),
        "unexpected error: {err}"
    );
    assert!(msg.contains("executor boom"), "original error lost: {err}");
    assert!(
        msg.contains("transaction abort failed"),
        "abort failure hidden: {err}"
    );
}

#[test]
fn finalise_autocommit_reports_read_commit_failure() {
    let mut session = test_session();
    let txn = session
        .state
        .txn_manager
        .begin(IsolationLevel::ReadCommitted);
    let stale = txn.clone();
    session.state.txn_manager.commit(txn).expect("pre-commit");

    let err = session
        .finalise_autocommit(
            &scan_plan(),
            stale,
            Ok(SelectResult {
                messages: Vec::new(),
                streamed_body: None,
                shared_streamed_body: None,
                rows: 0,
            }),
        )
        .expect_err("read autocommit commit failure must be visible");
    let msg = err.to_string();
    assert!(
        msg.contains("autocommit statement commit"),
        "context missing: {err}"
    );
    assert!(msg.contains("commit"), "commit failure hidden: {err}");
}

#[test]
fn finalise_autocommit_reports_logical_replication_lsn_exhaustion() {
    let mut session = test_session();
    session
        .state
        .logical_replication
        .create_publication("pub_t", vec!["t".to_owned()])
        .expect("publication");
    session
        .state
        .logical_replication
        .set_next_lsn_for_test(u64::MAX);
    let txn = session
        .state
        .txn_manager
        .begin(IsolationLevel::ReadCommitted);
    let insert = LogicalPlan::Insert {
        table: "t".to_owned(),
        columns: Vec::new(),
        source: Box::new(LogicalPlan::Values {
            rows: vec![vec![ScalarExpr::Literal {
                value: Value::Int32(1),
                data_type: DataType::Int32,
            }]],
            schema: Schema::new([Field::required("id", DataType::Int32)]).expect("values schema"),
        }),
        on_conflict: None,
        returning: Vec::new(),
        schema: Schema::empty(),
    };

    let err = session
        .finalise_autocommit(
            &insert,
            txn,
            Ok(SelectResult {
                messages: Vec::new(),
                streamed_body: None,
                shared_streamed_body: None,
                rows: 1,
            }),
        )
        .expect_err("CDC finalization failure must be visible before success response");

    assert!(
        err.to_string()
            .contains("logical replication LSN space exhausted"),
        "unexpected error: {err}"
    );
    assert!(session.pending_logical_changes.is_empty());
    assert_eq!(session.state.logical_replication.changes_since(0).len(), 0);
}

#[test]
fn catalog_rollback_reports_abort_failure_with_original_error() {
    let session = test_session();
    let txn = session
        .state
        .txn_manager
        .begin(IsolationLevel::ReadCommitted);
    let stale = txn.clone();
    session.state.txn_manager.abort(txn).expect("pre-abort");

    let err = session.rollback_catalog_transaction_after_error(
        stale,
        ServerError::ddl("catalog boom"),
        "CREATE TABLE catalog rollback after persist error",
    );
    let msg = err.to_string();
    assert!(
        msg.contains("CREATE TABLE catalog rollback after persist error"),
        "unexpected error: {err}"
    );
    assert!(msg.contains("catalog boom"), "original error lost: {err}");
    assert!(
        msg.contains("transaction abort failed"),
        "abort failure hidden: {err}"
    );
}

#[test]
fn materialized_view_maintenance_rollback_reports_abort_failure_with_original_error() {
    let session = test_session();
    let txn = session
        .state
        .txn_manager
        .begin(IsolationLevel::ReadCommitted);
    let stale = txn.clone();
    session.state.txn_manager.abort(txn).expect("pre-abort");

    let err = session.rollback_materialized_view_maintenance_after_error(
        stale,
        ServerError::ddl("maintenance boom"),
        "materialized-view maintenance rollback after delta error",
    );
    let msg = err.to_string();
    assert!(
        msg.contains("materialized-view maintenance rollback after delta error"),
        "unexpected error: {err}"
    );
    assert!(
        msg.contains("maintenance boom"),
        "original error lost: {err}"
    );
    assert!(
        msg.contains("transaction abort failed"),
        "abort failure hidden: {err}"
    );
}

#[test]
fn read_transaction_commit_reports_commit_failure_with_context() {
    let session = test_session();
    let txn = session
        .state
        .txn_manager
        .begin(IsolationLevel::ReadCommitted);
    let stale = txn.clone();
    session.state.txn_manager.commit(txn).expect("pre-commit");

    let err = session
        .finalise_read_transaction(stale, "read cleanup commit")
        .expect_err("stale read commit must fail");
    let msg = err.to_string();
    assert!(
        msg.contains("read cleanup commit"),
        "context missing: {err}"
    );
    assert!(msg.contains("commit"), "commit failure hidden: {err}");
}

#[test]
fn read_maintenance_transaction_reports_commit_failure() {
    let session = test_session();
    let txn = session
        .state
        .txn_manager
        .begin(IsolationLevel::ReadCommitted);
    let stale = txn.clone();
    session.state.txn_manager.commit(txn).expect("pre-commit");

    let err = session
        .finalise_read_maintenance_transaction(
            stale,
            Ok(()),
            "maintenance commit",
            "maintenance rollback",
        )
        .expect_err("maintenance commit failure must be visible");
    let msg = err.to_string();
    assert!(msg.contains("maintenance commit"), "context missing: {err}");
    assert!(msg.contains("commit"), "commit failure hidden: {err}");
}

#[test]
fn read_maintenance_transaction_reports_abort_failure_with_original_error() {
    let session = test_session();
    let txn = session
        .state
        .txn_manager
        .begin(IsolationLevel::ReadCommitted);
    let stale = txn.clone();
    session.state.txn_manager.abort(txn).expect("pre-abort");

    let err = session
        .finalise_read_maintenance_transaction(
            stale,
            Err(ServerError::ddl("maintenance boom")),
            "maintenance commit",
            "maintenance rollback",
        )
        .expect_err("maintenance rollback failure must be visible");
    let msg = err.to_string();
    assert!(
        msg.contains("maintenance rollback"),
        "context missing: {err}"
    );
    assert!(
        msg.contains("maintenance boom"),
        "original error lost: {err}"
    );
    assert!(
        msg.contains("transaction abort failed"),
        "abort failure hidden: {err}"
    );
}

#[test]
fn backup_hot_standby_and_single_text_helpers_cover_admin_edges() {
    assert_eq!(
        Session::<tokio::io::DuplexStream>::try_parse_backup_function(
            "SELECT pg_start_backup('label');"
        ),
        Some("pg_start_backup")
    );
    assert_eq!(
        Session::<tokio::io::DuplexStream>::try_parse_backup_function("select pg_backup_stop()"),
        Some("pg_stop_backup")
    );
    assert_eq!(
        Session::<tokio::io::DuplexStream>::try_parse_backup_function("SELECT 1"),
        None
    );

    for sql in [
        "",
        "SELECT 1",
        "SHOW client_encoding",
        "EXPLAIN SELECT 1",
        "WITH x AS (SELECT 1) SELECT * FROM x",
        "VALUES (1)",
        "COPY t TO STDOUT",
    ] {
        assert!(Session::<tokio::io::DuplexStream>::hot_standby_allows(sql));
    }
    for sql in ["INSERT INTO t VALUES (1)", "COPY t FROM STDIN"] {
        assert!(!Session::<tokio::io::DuplexStream>::hot_standby_allows(sql));
    }

    let result = Session::<tokio::io::DuplexStream>::single_text_select("answer", "42");
    assert_eq!(result.rows, 1);
    assert_eq!(first_data_row_text(&result), "42");

    let mut session = test_session();
    let txn = session
        .state
        .txn_manager
        .begin(IsolationLevel::ReadCommitted);
    session.txn_state = TxnState::InTransaction(txn);
    let err = session.fail_if_in_transaction(ServerError::Unsupported("boom"));
    assert!(matches!(err, ServerError::Unsupported("boom")));
    assert!(matches!(session.txn_state, TxnState::Failed(_)));
}
