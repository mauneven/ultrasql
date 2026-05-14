//! Test sub-module; see `tests/mod.rs` for shared helpers.

#![allow(unused_imports)]

use super::*;


/// Extended Query round-trip over the in-memory duplex transport.
///
/// `Parse → Bind → Describe(Portal) → Execute → Sync` against
/// `SELECT id FROM users` should return the same three rows the
/// Simple Query path produces. This is the duplex-level smoke test;
/// the real-driver test against `tokio-postgres` lives in
/// `crates/ultrasql-server/tests/extended_query_round_trip.rs`.
#[tokio::test]
async fn extended_query_round_trip_select() {
    let (mut client, server_side) = tokio::io::duplex(8192);
    let state = server();
    let handle = tokio::spawn(handle_connection(server_side, state));

    complete_startup(&mut client).await;

    // Parse
    send_frontend(
        &mut client,
        &FrontendMessage::Parse {
            name: "s1".to_string(),
            sql: "SELECT id FROM users".to_string(),
            param_types: vec![],
        },
    )
    .await;
    // Bind
    send_frontend(
        &mut client,
        &FrontendMessage::Bind {
            portal_name: "p1".to_string(),
            statement_name: "s1".to_string(),
            param_formats: vec![],
            params: vec![],
            result_formats: vec![],
        },
    )
    .await;
    // Describe(Portal)
    send_frontend(
        &mut client,
        &FrontendMessage::Describe {
            kind: ultrasql_protocol::DescribeKind::Portal,
            name: "p1".to_string(),
        },
    )
    .await;
    // Execute
    send_frontend(
        &mut client,
        &FrontendMessage::Execute {
            portal: "p1".to_string(),
            max_rows: 0,
        },
    )
    .await;
    // Sync — triggers ReadyForQuery.
    send_frontend(&mut client, &FrontendMessage::Sync).await;

    let msgs = drain_until_ready(&mut client).await;

    // ParseComplete and BindComplete are present.
    assert!(
        msgs.iter()
            .any(|m| matches!(m, BackendMessage::ParseComplete)),
        "missing ParseComplete: {msgs:?}"
    );
    assert!(
        msgs.iter()
            .any(|m| matches!(m, BackendMessage::BindComplete)),
        "missing BindComplete: {msgs:?}"
    );
    // RowDescription from Describe(Portal).
    assert!(
        msgs.iter()
            .any(|m| matches!(m, BackendMessage::RowDescription { .. })),
        "missing RowDescription: {msgs:?}"
    );
    // Three data rows.
    let n_rows = msgs
        .iter()
        .filter(|m| matches!(m, BackendMessage::DataRow { .. }))
        .count();
    assert_eq!(n_rows, 3, "expected 3 data rows: {msgs:?}");
    // CommandComplete + ReadyForQuery 'I' at the end.
    assert!(matches!(
        msgs.last().unwrap(),
        BackendMessage::ReadyForQuery { status: b'I' }
    ));

    send_frontend(&mut client, &FrontendMessage::Terminate).await;
    drop(client);
    handle.await.expect("task joins").expect("clean exit");
}


/// Parameter substitution end-to-end over the duplex transport.
///
/// `SELECT id FROM users WHERE id = $1` with `$1 = 2` should
/// return exactly one row.
#[tokio::test]
async fn extended_query_round_trip_with_parameter() {
    let (mut client, server_side) = tokio::io::duplex(8192);
    let state = server();
    let handle = tokio::spawn(handle_connection(server_side, state));

    complete_startup(&mut client).await;

    send_frontend(
        &mut client,
        &FrontendMessage::Parse {
            name: String::new(),
            sql: "SELECT id FROM users WHERE id = $1".to_string(),
            param_types: vec![23], // int4
        },
    )
    .await;
    send_frontend(
        &mut client,
        &FrontendMessage::Bind {
            portal_name: String::new(),
            statement_name: String::new(),
            param_formats: vec![1], // binary
            params: vec![Some(2_i32.to_be_bytes().to_vec())],
            result_formats: vec![],
        },
    )
    .await;
    send_frontend(
        &mut client,
        &FrontendMessage::Execute {
            portal: String::new(),
            max_rows: 0,
        },
    )
    .await;
    send_frontend(&mut client, &FrontendMessage::Sync).await;

    let msgs = drain_until_ready(&mut client).await;
    let rows: Vec<_> = msgs
        .iter()
        .filter_map(|m| match m {
            BackendMessage::DataRow { columns } => Some(columns.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(rows.len(), 1, "expected one matching row: {msgs:?}");
    assert_eq!(rows[0][0].as_deref(), Some(b"2".as_slice()));

    send_frontend(&mut client, &FrontendMessage::Terminate).await;
    drop(client);
    handle.await.expect("task joins").expect("clean exit");
}


/// Extended Query round-trip for BEGIN / INSERT / COMMIT — prepared
/// statements and unnamed portals.  Mirrors the Simple Query test
/// `begin_commit_persists_rows_rollback_discards` over the
/// `Parse/Bind/Execute/Sync` path.
#[tokio::test]
async fn extended_query_begin_insert_commit_round_trips() {
    let (mut client, server_side) = tokio::io::duplex(8192);
    let state = Arc::new(Server::with_sample_database());
    let handle = tokio::spawn(handle_connection(server_side, state));
    complete_startup(&mut client).await;

    // Setup CREATE TABLE via Simple Query (Extended doesn't accept
    // CREATE TABLE today; see execute_portal docs).
    send_frontend(
        &mut client,
        &FrontendMessage::Query {
            sql: "CREATE TABLE t (id INT NOT NULL, val INT)".to_string(),
        },
    )
    .await;
    let _ = drain_until_ready(&mut client).await;

    // BEGIN via Extended Query (unnamed statement + portal).
    for sql in ["BEGIN", "INSERT INTO t VALUES (1, 100)", "COMMIT"] {
        send_frontend(
            &mut client,
            &FrontendMessage::Parse {
                name: String::new(),
                sql: sql.into(),
                param_types: vec![],
            },
        )
        .await;
        send_frontend(
            &mut client,
            &FrontendMessage::Bind {
                portal_name: String::new(),
                statement_name: String::new(),
                param_formats: vec![],
                params: vec![],
                result_formats: vec![],
            },
        )
        .await;
        send_frontend(
            &mut client,
            &FrontendMessage::Execute {
                portal: String::new(),
                max_rows: 0,
            },
        )
        .await;
        send_frontend(&mut client, &FrontendMessage::Sync).await;
        let msgs = drain_until_ready(&mut client).await;
        // Status reflects post-statement TxnState.
        let expected_status = match sql {
            "BEGIN" | "INSERT INTO t VALUES (1, 100)" => b'T',
            "COMMIT" => b'I',
            _ => unreachable!(),
        };
        assert_eq!(
            ready_status(&msgs),
            expected_status,
            "Extended {sql} → status {} (got {:?})",
            expected_status as char,
            msgs
        );
    }

    // The inserted row is visible after COMMIT.
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
    assert_eq!(row_count, 1, "Extended BEGIN/INSERT/COMMIT persisted");

    send_frontend(&mut client, &FrontendMessage::Terminate).await;
    drop(client);
    handle.await.expect("task joins").expect("clean exit");
}


/// Extended Query ROLLBACK discards the in-flight write.
#[tokio::test]
async fn extended_query_begin_insert_rollback_discards() {
    let (mut client, server_side) = tokio::io::duplex(8192);
    let state = Arc::new(Server::with_sample_database());
    let handle = tokio::spawn(handle_connection(server_side, state));
    complete_startup(&mut client).await;

    send_frontend(
        &mut client,
        &FrontendMessage::Query {
            sql: "CREATE TABLE t (id INT NOT NULL)".to_string(),
        },
    )
    .await;
    let _ = drain_until_ready(&mut client).await;

    for sql in ["BEGIN", "INSERT INTO t VALUES (42)", "ROLLBACK"] {
        send_frontend(
            &mut client,
            &FrontendMessage::Parse {
                name: String::new(),
                sql: sql.into(),
                param_types: vec![],
            },
        )
        .await;
        send_frontend(
            &mut client,
            &FrontendMessage::Bind {
                portal_name: String::new(),
                statement_name: String::new(),
                param_formats: vec![],
                params: vec![],
                result_formats: vec![],
            },
        )
        .await;
        send_frontend(
            &mut client,
            &FrontendMessage::Execute {
                portal: String::new(),
                max_rows: 0,
            },
        )
        .await;
        send_frontend(&mut client, &FrontendMessage::Sync).await;
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
    assert_eq!(row_count, 0, "Extended ROLLBACK discarded");

    send_frontend(&mut client, &FrontendMessage::Terminate).await;
    drop(client);
    handle.await.expect("task joins").expect("clean exit");
}

