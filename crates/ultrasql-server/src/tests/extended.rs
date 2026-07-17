//! Test sub-module; see `tests/mod.rs` for shared helpers.

use std::time::Duration;

use super::*;
use tokio::io::AsyncRead;

async fn read_one_backend(io: &mut (impl AsyncRead + Unpin), buf: &mut BytesMut) -> BackendMessage {
    loop {
        if let Some(msg) = ultrasql_protocol::decode_backend(buf).expect("decode backend") {
            return msg;
        }
        let n = io.read_buf(buf).await.expect("read backend");
        assert_ne!(n, 0, "server closed before a complete backend message");
    }
}

async fn drain_extended_until_ready(
    io: &mut (impl AsyncRead + Unpin),
    buf: &mut BytesMut,
) -> Vec<BackendMessage> {
    let mut messages = Vec::new();
    loop {
        let msg = read_one_backend(io, buf).await;
        let is_ready = matches!(msg, BackendMessage::ReadyForQuery { .. });
        messages.push(msg);
        if is_ready {
            return messages;
        }
    }
}

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

#[tokio::test]
async fn extended_parse_waits_for_flush() {
    let (mut client, server_side) = tokio::io::duplex(8192);
    let handle = tokio::spawn(handle_connection(server_side, server()));
    complete_startup(&mut client).await;

    send_frontend(
        &mut client,
        &FrontendMessage::Parse {
            name: "queued".to_string(),
            sql: "SELECT 1".to_string(),
            param_types: vec![],
        },
    )
    .await;

    let mut wire = BytesMut::new();
    let early_read =
        tokio::time::timeout(Duration::from_millis(100), client.read_buf(&mut wire)).await;
    assert!(
        early_read.is_err(),
        "ParseComplete must remain queued until Flush or Sync"
    );

    send_frontend(&mut client, &FrontendMessage::Flush).await;
    let message = tokio::time::timeout(
        Duration::from_secs(2),
        read_one_backend(&mut client, &mut wire),
    )
    .await
    .expect("Flush delivers queued ParseComplete");
    assert!(matches!(message, BackendMessage::ParseComplete));

    send_frontend(&mut client, &FrontendMessage::Sync).await;
    let messages = drain_extended_until_ready(&mut client, &mut wire).await;
    assert_eq!(messages.len(), 1, "Flush must not duplicate queued bytes");
    assert!(matches!(
        messages[0],
        BackendMessage::ReadyForQuery { status: b'I' }
    ));

    send_frontend(&mut client, &FrontendMessage::Terminate).await;
    drop(client);
    handle.await.expect("task joins").expect("clean exit");
}

#[tokio::test]
async fn extended_sync_preserves_queued_response_order() {
    let (mut client, server_side) = tokio::io::duplex(8192);
    let handle = tokio::spawn(handle_connection(server_side, server()));
    complete_startup(&mut client).await;

    send_frontend(
        &mut client,
        &FrontendMessage::Parse {
            name: "ordered".to_string(),
            sql: "SELECT 1".to_string(),
            param_types: vec![],
        },
    )
    .await;
    send_frontend(
        &mut client,
        &FrontendMessage::Bind {
            portal_name: "ordered".to_string(),
            statement_name: "ordered".to_string(),
            param_formats: vec![],
            params: vec![],
            result_formats: vec![],
        },
    )
    .await;
    send_frontend(
        &mut client,
        &FrontendMessage::Close {
            kind: ultrasql_protocol::DescribeKind::Portal,
            name: "ordered".to_string(),
        },
    )
    .await;
    send_frontend(&mut client, &FrontendMessage::Sync).await;

    let messages = drain_until_ready(&mut client).await;
    assert!(
        matches!(
            messages.as_slice(),
            [
                BackendMessage::ParseComplete,
                BackendMessage::BindComplete,
                BackendMessage::CloseComplete,
                BackendMessage::ReadyForQuery { status: b'I' }
            ]
        ),
        "Sync must append ReadyForQuery after queued responses: {messages:?}"
    );

    send_frontend(&mut client, &FrontendMessage::Terminate).await;
    drop(client);
    handle.await.expect("task joins").expect("clean exit");
}

#[tokio::test]
async fn extended_large_result_crosses_bounded_write_window_before_sync() {
    let (mut client, server_side) = tokio::io::duplex(16 * 1024);
    let handle = tokio::spawn(handle_connection(server_side, server()));
    complete_startup(&mut client).await;

    send_frontend(
        &mut client,
        &FrontendMessage::Parse {
            name: "large".to_string(),
            sql: "SELECT * FROM generate_series(1, 10000)".to_string(),
            param_types: vec![],
        },
    )
    .await;
    send_frontend(
        &mut client,
        &FrontendMessage::Bind {
            portal_name: "large".to_string(),
            statement_name: "large".to_string(),
            param_formats: vec![],
            params: vec![],
            result_formats: vec![],
        },
    )
    .await;
    send_frontend(
        &mut client,
        &FrontendMessage::Execute {
            portal: "large".to_string(),
            max_rows: 0,
        },
    )
    .await;

    let mut wire = BytesMut::new();
    let bytes_read = tokio::time::timeout(Duration::from_secs(5), client.read_buf(&mut wire))
        .await
        .expect("bounded window sends a large response before Sync")
        .expect("read early large-result bytes");
    assert_ne!(bytes_read, 0);

    send_frontend(&mut client, &FrontendMessage::Sync).await;
    let messages = tokio::time::timeout(
        Duration::from_secs(10),
        drain_extended_until_ready(&mut client, &mut wire),
    )
    .await
    .expect("large Extended response completes");
    assert_eq!(
        messages
            .iter()
            .filter(|msg| matches!(msg, BackendMessage::DataRow { .. }))
            .count(),
        10_000
    );
    assert!(matches!(
        messages.last(),
        Some(BackendMessage::ReadyForQuery { status: b'I' })
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

/// Regression: an EMPTY result-format Bind on a float column must report
/// TEXT (format 0) in the RowDescription, matching the TEXT bytes the
/// DataRow actually ships.
///
/// The server previously special-cased `Float32`/`Float64` to BINARY
/// (format 1) when the Bind result-format list was empty, but the row
/// encoder ships text for an empty list — so the RowDescription lied and a
/// client decoding per the advertised format mis-read the bytes. This test
/// drives the exact frontend sequence libpq/JDBC use (empty
/// `result_formats`) and asserts the format code and the wire bytes agree.
#[tokio::test]
async fn empty_result_format_float_column_advertises_text_matching_data_row() {
    let (mut client, server_side) = tokio::io::duplex(8192);
    let state = server();
    let handle = tokio::spawn(handle_connection(server_side, state));

    complete_startup(&mut client).await;

    send_frontend(
        &mut client,
        &FrontendMessage::Parse {
            name: "sf".to_string(),
            sql: "SELECT 1.5::float8 AS d".to_string(),
            param_types: vec![],
        },
    )
    .await;
    // Bind with an EMPTY result-format list — "text for every column".
    send_frontend(
        &mut client,
        &FrontendMessage::Bind {
            portal_name: "pf".to_string(),
            statement_name: "sf".to_string(),
            param_formats: vec![],
            params: vec![],
            result_formats: vec![],
        },
    )
    .await;
    send_frontend(
        &mut client,
        &FrontendMessage::Describe {
            kind: ultrasql_protocol::DescribeKind::Portal,
            name: "pf".to_string(),
        },
    )
    .await;
    send_frontend(
        &mut client,
        &FrontendMessage::Execute {
            portal: "pf".to_string(),
            max_rows: 0,
        },
    )
    .await;
    send_frontend(&mut client, &FrontendMessage::Sync).await;

    let msgs = drain_until_ready(&mut client).await;

    // The RowDescription must advertise TEXT (format 0) for the float column.
    let fields = msgs
        .iter()
        .find_map(|m| match m {
            BackendMessage::RowDescription { fields } => Some(fields.clone()),
            _ => None,
        })
        .expect("RowDescription present");
    assert_eq!(fields.len(), 1, "one float column");
    assert_eq!(
        fields[0].format_code, 0,
        "empty result-format Bind must report TEXT (0) for the float column"
    );

    // And the DataRow must carry the TEXT encoding of 1.5 — proving the
    // advertised format matches the bytes on the wire.
    let rows: Vec<_> = msgs
        .iter()
        .filter_map(|m| match m {
            BackendMessage::DataRow { columns } => Some(columns.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(rows.len(), 1, "one row: {msgs:?}");
    assert_eq!(
        rows[0][0].as_deref(),
        Some(b"1.5".as_slice()),
        "float DataRow must be the text encoding, matching the advertised format"
    );

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
