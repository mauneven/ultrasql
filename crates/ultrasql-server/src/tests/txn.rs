//! Test sub-module; see `tests/mod.rs` for shared helpers.

#![allow(unused_imports)]

use super::*;


/// `BEGIN; INSERT; INSERT; COMMIT;` — both rows visible after commit.
/// `BEGIN; INSERT; ROLLBACK;` — row not persisted.
/// `ReadyForQuery` status byte reflects state at every step.
#[tokio::test]
async fn begin_commit_persists_rows_rollback_discards() {
    let (mut client, server_side) = tokio::io::duplex(8192);
    let state = Arc::new(Server::with_sample_database());
    let handle = tokio::spawn(handle_connection(server_side, state));
    complete_startup(&mut client).await;

    // CREATE TABLE — outside any txn.
    send_frontend(
        &mut client,
        &FrontendMessage::Query {
            sql: "CREATE TABLE t (id INT NOT NULL, val INT)".to_string(),
        },
    )
    .await;
    let msgs = drain_until_ready(&mut client).await;
    assert_eq!(ready_status(&msgs), b'I');

    // BEGIN
    send_frontend(
        &mut client,
        &FrontendMessage::Query {
            sql: "BEGIN".to_string(),
        },
    )
    .await;
    let msgs = drain_until_ready(&mut client).await;
    assert_eq!(command_tag(&msgs).as_deref(), Some("BEGIN"));
    assert_eq!(ready_status(&msgs), b'T', "BEGIN → 'T' status");

    // INSERT — inside txn
    send_frontend(
        &mut client,
        &FrontendMessage::Query {
            sql: "INSERT INTO t VALUES (1, 100)".to_string(),
        },
    )
    .await;
    let msgs = drain_until_ready(&mut client).await;
    assert_eq!(command_tag(&msgs).as_deref(), Some("INSERT 0 1"));
    assert_eq!(ready_status(&msgs), b'T');

    // INSERT
    send_frontend(
        &mut client,
        &FrontendMessage::Query {
            sql: "INSERT INTO t VALUES (2, 200)".to_string(),
        },
    )
    .await;
    let msgs = drain_until_ready(&mut client).await;
    assert_eq!(ready_status(&msgs), b'T');

    // COMMIT
    send_frontend(
        &mut client,
        &FrontendMessage::Query {
            sql: "COMMIT".to_string(),
        },
    )
    .await;
    let msgs = drain_until_ready(&mut client).await;
    assert_eq!(command_tag(&msgs).as_deref(), Some("COMMIT"));
    assert_eq!(ready_status(&msgs), b'I', "COMMIT → 'I'");

    // SELECT — both rows visible.
    send_frontend(
        &mut client,
        &FrontendMessage::Query {
            sql: "SELECT id FROM t".to_string(),
        },
    )
    .await;
    let msgs = drain_until_ready(&mut client).await;
    let row_count = msgs
        .iter()
        .filter(|m| matches!(m, BackendMessage::DataRow { .. }))
        .count();
    assert_eq!(row_count, 2, "both committed rows visible");

    // BEGIN; INSERT; ROLLBACK — row 3 must not persist.
    for stmt in ["BEGIN", "INSERT INTO t VALUES (3, 300)", "ROLLBACK"] {
        send_frontend(
            &mut client,
            &FrontendMessage::Query {
                sql: stmt.to_string(),
            },
        )
        .await;
        let _ = drain_until_ready(&mut client).await;
    }

    send_frontend(
        &mut client,
        &FrontendMessage::Query {
            sql: "SELECT id FROM t".to_string(),
        },
    )
    .await;
    let msgs = drain_until_ready(&mut client).await;
    let row_count = msgs
        .iter()
        .filter(|m| matches!(m, BackendMessage::DataRow { .. }))
        .count();
    assert_eq!(row_count, 2, "rolled-back INSERT did not persist");
    assert_eq!(ready_status(&msgs), b'I');

    send_frontend(&mut client, &FrontendMessage::Terminate).await;
    drop(client);
    handle.await.expect("task joins").expect("clean exit");
}


/// `BEGIN; UPDATE; ROLLBACK;` — UPDATE is undone.
#[tokio::test]
async fn begin_update_rollback_reverts_value() {
    let (mut client, server_side) = tokio::io::duplex(8192);
    let state = Arc::new(Server::with_sample_database());
    let handle = tokio::spawn(handle_connection(server_side, state));
    complete_startup(&mut client).await;

    // Setup
    for sql in [
        "CREATE TABLE t (id INT NOT NULL, val INT)",
        "INSERT INTO t VALUES (1, 100)",
    ] {
        send_frontend(&mut client, &FrontendMessage::Query { sql: sql.into() }).await;
        let _ = drain_until_ready(&mut client).await;
    }

    // BEGIN; UPDATE; ROLLBACK
    for sql in [
        "BEGIN",
        "UPDATE t SET val = val + 999 WHERE id = 1",
        "ROLLBACK",
    ] {
        send_frontend(&mut client, &FrontendMessage::Query { sql: sql.into() }).await;
        let _ = drain_until_ready(&mut client).await;
    }

    // Verify val unchanged.
    send_frontend(
        &mut client,
        &FrontendMessage::Query {
            sql: "SELECT val FROM t WHERE id = 1".to_string(),
        },
    )
    .await;
    let msgs = drain_until_ready(&mut client).await;
    let rows: Vec<Vec<Option<Vec<u8>>>> = msgs
        .iter()
        .filter_map(|m| match m {
            BackendMessage::DataRow { columns } => Some(columns.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_deref(), Some(b"100".as_slice()));

    send_frontend(&mut client, &FrontendMessage::Terminate).await;
    drop(client);
    handle.await.expect("task joins").expect("clean exit");
}


/// A statement that errors inside a transaction transitions the
/// session to the `Failed` state. Subsequent statements (other than
/// COMMIT / ROLLBACK) return SQLSTATE `25P02`. COMMIT in `Failed`
/// state returns the `ROLLBACK` tag (PostgreSQL semantics).
#[tokio::test]
async fn failed_transaction_rejects_subsequent_statements_until_rollback() {
    let (mut client, server_side) = tokio::io::duplex(8192);
    let state = Arc::new(Server::with_sample_database());
    let handle = tokio::spawn(handle_connection(server_side, state));
    complete_startup(&mut client).await;

    // BEGIN
    send_frontend(
        &mut client,
        &FrontendMessage::Query {
            sql: "BEGIN".to_string(),
        },
    )
    .await;
    let msgs = drain_until_ready(&mut client).await;
    assert_eq!(ready_status(&msgs), b'T');

    // Cause an error: select from a non-existent table.
    send_frontend(
        &mut client,
        &FrontendMessage::Query {
            sql: "SELECT * FROM no_such_table".to_string(),
        },
    )
    .await;
    let msgs = drain_until_ready(&mut client).await;
    assert!(
        msgs.iter()
            .any(|m| matches!(m, BackendMessage::ErrorResponse { .. })),
        "expected ErrorResponse for missing table"
    );
    assert_eq!(ready_status(&msgs), b'E', "post-error status → 'E'");

    // A subsequent statement (a perfectly valid SELECT against the
    // sample table) is rejected with `25P02` while in `Failed`.
    send_frontend(
        &mut client,
        &FrontendMessage::Query {
            sql: "SELECT id FROM users".to_string(),
        },
    )
    .await;
    let msgs = drain_until_ready(&mut client).await;
    let err = msgs
        .iter()
        .find_map(|m| match m {
            BackendMessage::ErrorResponse { fields } => Some(fields.clone()),
            _ => None,
        })
        .expect("ErrorResponse in failed state");
    let sqlstate = err
        .iter()
        .find_map(|(c, v)| (*c == b'C').then(|| v.clone()))
        .expect("SQLSTATE field present");
    assert_eq!(sqlstate, "25P02", "failed-block SQLSTATE");
    assert_eq!(ready_status(&msgs), b'E');

    // COMMIT in failed state returns the `ROLLBACK` tag.
    send_frontend(
        &mut client,
        &FrontendMessage::Query {
            sql: "COMMIT".to_string(),
        },
    )
    .await;
    let msgs = drain_until_ready(&mut client).await;
    assert_eq!(
        command_tag(&msgs).as_deref(),
        Some("ROLLBACK"),
        "COMMIT in failed state returns ROLLBACK tag (PostgreSQL semantics)",
    );
    assert_eq!(ready_status(&msgs), b'I', "post-COMMIT status → 'I'");

    // Session is healthy again — the same query that errored under
    // `Failed` now runs normally.
    send_frontend(
        &mut client,
        &FrontendMessage::Query {
            sql: "SELECT id FROM users".to_string(),
        },
    )
    .await;
    let msgs = drain_until_ready(&mut client).await;
    assert!(
        !msgs
            .iter()
            .any(|m| matches!(m, BackendMessage::ErrorResponse { .. }))
    );

    send_frontend(&mut client, &FrontendMessage::Terminate).await;
    drop(client);
    handle.await.expect("task joins").expect("clean exit");
}


/// Implicit autocommit still works: `INSERT` outside any `BEGIN`
/// commits immediately and is visible to the next statement.
#[tokio::test]
async fn implicit_autocommit_still_persists_writes() {
    let (mut client, server_side) = tokio::io::duplex(8192);
    let state = Arc::new(Server::with_sample_database());
    let handle = tokio::spawn(handle_connection(server_side, state));
    complete_startup(&mut client).await;

    for sql in [
        "CREATE TABLE t (id INT NOT NULL)",
        "INSERT INTO t VALUES (1)",
        "INSERT INTO t VALUES (2)",
    ] {
        send_frontend(&mut client, &FrontendMessage::Query { sql: sql.into() }).await;
        let msgs = drain_until_ready(&mut client).await;
        assert_eq!(
            ready_status(&msgs),
            b'I',
            "autocommit always leaves status as 'I' after {sql}",
        );
    }

    send_frontend(
        &mut client,
        &FrontendMessage::Query {
            sql: "SELECT id FROM t".to_string(),
        },
    )
    .await;
    let msgs = drain_until_ready(&mut client).await;
    let row_count = msgs
        .iter()
        .filter(|m| matches!(m, BackendMessage::DataRow { .. }))
        .count();
    assert_eq!(row_count, 2);

    send_frontend(&mut client, &FrontendMessage::Terminate).await;
    drop(client);
    handle.await.expect("task joins").expect("clean exit");
}


/// `BEGIN` while a transaction is already open emits a
/// `NoticeResponse` (WARNING) and leaves the session in
/// `InTransaction`. The PostgreSQL behaviour we mirror.
#[tokio::test]
async fn nested_begin_emits_warning_but_keeps_session_in_tx() {
    let (mut client, server_side) = tokio::io::duplex(8192);
    let state = Arc::new(Server::with_sample_database());
    let handle = tokio::spawn(handle_connection(server_side, state));
    complete_startup(&mut client).await;

    // First BEGIN
    send_frontend(
        &mut client,
        &FrontendMessage::Query {
            sql: "BEGIN".to_string(),
        },
    )
    .await;
    let msgs = drain_until_ready(&mut client).await;
    assert_eq!(ready_status(&msgs), b'T');

    // Nested BEGIN
    send_frontend(
        &mut client,
        &FrontendMessage::Query {
            sql: "BEGIN".to_string(),
        },
    )
    .await;
    let msgs = drain_until_ready(&mut client).await;
    assert!(
        msgs.iter()
            .any(|m| matches!(m, BackendMessage::NoticeResponse { .. })),
        "expected NoticeResponse for nested BEGIN: {msgs:?}"
    );
    assert_eq!(command_tag(&msgs).as_deref(), Some("BEGIN"));
    assert_eq!(ready_status(&msgs), b'T', "nested BEGIN → still 'T'");

    send_frontend(
        &mut client,
        &FrontendMessage::Query {
            sql: "ROLLBACK".to_string(),
        },
    )
    .await;
    let _ = drain_until_ready(&mut client).await;

    send_frontend(&mut client, &FrontendMessage::Terminate).await;
    drop(client);
    handle.await.expect("task joins").expect("clean exit");
}


/// `COMMIT` / `ROLLBACK` outside a transaction emit a
/// `NoticeResponse` (WARNING) but still succeed with the
/// corresponding command tag.
#[tokio::test]
async fn commit_and_rollback_outside_tx_emit_warning() {
    let (mut client, server_side) = tokio::io::duplex(8192);
    let state = Arc::new(Server::with_sample_database());
    let handle = tokio::spawn(handle_connection(server_side, state));
    complete_startup(&mut client).await;

    for sql in ["COMMIT", "ROLLBACK"] {
        send_frontend(&mut client, &FrontendMessage::Query { sql: sql.into() }).await;
        let msgs = drain_until_ready(&mut client).await;
        assert!(
            msgs.iter()
                .any(|m| matches!(m, BackendMessage::NoticeResponse { .. })),
            "expected NoticeResponse for {sql} outside tx: {msgs:?}"
        );
        assert_eq!(command_tag(&msgs).as_deref(), Some(sql));
        assert_eq!(ready_status(&msgs), b'I');
    }

    send_frontend(&mut client, &FrontendMessage::Terminate).await;
    drop(client);
    handle.await.expect("task joins").expect("clean exit");
}

