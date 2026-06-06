//! Test sub-module; see `tests/mod.rs` for shared helpers.

use super::*;

#[tokio::test]
async fn startup_handshake_completes() {
    let (mut client, server_side) = tokio::io::duplex(8192);
    let state = server();
    let handle = tokio::spawn(handle_connection(server_side, state));

    complete_startup(&mut client).await;
    // Send Terminate to let the handler return cleanly.
    send_frontend(&mut client, &FrontendMessage::Terminate).await;
    drop(client);
    handle.await.expect("task joins").expect("clean exit");
}

#[tokio::test]
async fn startup_server_version_is_driver_parseable() {
    let (mut client, server_side) = tokio::io::duplex(8192);
    let state = server();
    let handle = tokio::spawn(handle_connection(server_side, state));

    send_frontend(
        &mut client,
        &FrontendMessage::StartupMessage {
            protocol_major: 3,
            protocol_minor: 0,
            params: vec![("user".to_string(), "tester".to_string())],
        },
    )
    .await;
    let msgs = drain_until_ready(&mut client).await;
    let server_version = msgs
        .iter()
        .find_map(|msg| match msg {
            BackendMessage::ParameterStatus { name, value } if name == "server_version" => {
                Some(value.as_str())
            }
            _ => None,
        })
        .expect("server_version ParameterStatus");
    assert!(
        server_version
            .split('.')
            .all(|part| !part.is_empty() && part.chars().all(|c| c.is_ascii_digit())),
        "server_version must be numeric for Npgsql, got {server_version:?}"
    );

    send_frontend(&mut client, &FrontendMessage::Terminate).await;
    drop(client);
    handle.await.expect("task joins").expect("clean exit");
}

#[tokio::test]
async fn simple_query_returns_three_data_rows() {
    let (mut client, server_side) = tokio::io::duplex(8192);
    let state = server();
    let handle = tokio::spawn(handle_connection(server_side, state));

    complete_startup(&mut client).await;
    send_frontend(
        &mut client,
        &FrontendMessage::Query {
            sql: "SELECT id FROM users".to_string(),
        },
    )
    .await;
    let msgs = drain_until_ready(&mut client).await;

    let row_desc = msgs
        .iter()
        .find(|m| matches!(m, BackendMessage::RowDescription { .. }))
        .expect("row description present");
    match row_desc {
        BackendMessage::RowDescription { fields } => {
            assert_eq!(fields.len(), 1);
            assert_eq!(fields[0].name, "id");
            assert_eq!(fields[0].type_oid, 23); // int4
        }
        _ => unreachable!(),
    }

    let rows: Vec<_> = msgs
        .iter()
        .filter(|m| matches!(m, BackendMessage::DataRow { .. }))
        .collect();
    assert_eq!(rows.len(), 3);
    match rows[0] {
        BackendMessage::DataRow { columns } => {
            assert_eq!(columns.len(), 1);
            assert_eq!(columns[0].as_deref(), Some(b"1".as_slice()));
        }
        _ => unreachable!(),
    }

    let cc = msgs
        .iter()
        .find(|m| matches!(m, BackendMessage::CommandComplete { .. }))
        .expect("command complete present");
    match cc {
        BackendMessage::CommandComplete { tag } => assert_eq!(tag, "SELECT 3"),
        _ => unreachable!(),
    }
    assert!(matches!(
        msgs.last().unwrap(),
        BackendMessage::ReadyForQuery { status: b'I' }
    ));

    send_frontend(&mut client, &FrontendMessage::Terminate).await;
    drop(client);
    handle.await.expect("task joins").expect("clean exit");
}

#[tokio::test]
async fn filter_and_limit_narrow_result_to_one_row() {
    let (mut client, server_side) = tokio::io::duplex(8192);
    let state = server();
    let handle = tokio::spawn(handle_connection(server_side, state));

    complete_startup(&mut client).await;
    send_frontend(
        &mut client,
        &FrontendMessage::Query {
            sql: "SELECT id FROM users WHERE id = 1 LIMIT 1".to_string(),
        },
    )
    .await;
    let msgs = drain_until_ready(&mut client).await;

    let rows: Vec<_> = msgs
        .iter()
        .filter(|m| matches!(m, BackendMessage::DataRow { .. }))
        .collect();
    assert_eq!(rows.len(), 1);
    match rows[0] {
        BackendMessage::DataRow { columns } => {
            assert_eq!(columns[0].as_deref(), Some(b"1".as_slice()));
        }
        _ => unreachable!(),
    }

    send_frontend(&mut client, &FrontendMessage::Terminate).await;
    drop(client);
    handle.await.expect("task joins").expect("clean exit");
}

#[tokio::test]
async fn unknown_table_reports_error_then_ready_idle() {
    let (mut client, server_side) = tokio::io::duplex(8192);
    let state = server();
    let handle = tokio::spawn(handle_connection(server_side, state));

    complete_startup(&mut client).await;
    send_frontend(
        &mut client,
        &FrontendMessage::Query {
            sql: "SELECT id FROM nope".to_string(),
        },
    )
    .await;
    let msgs = drain_until_ready(&mut client).await;

    assert!(
        msgs.iter()
            .any(|m| matches!(m, BackendMessage::ErrorResponse { .. }))
    );
    // The session continues — ready-for-query is 'I' (idle), not
    // 'E' (in failed transaction), because we are not in a tx.
    assert!(matches!(
        msgs.last().unwrap(),
        BackendMessage::ReadyForQuery { status: b'I' }
    ));

    send_frontend(&mut client, &FrontendMessage::Terminate).await;
    drop(client);
    handle.await.expect("task joins").expect("clean exit");
}

#[tokio::test]
async fn parse_error_reports_error_response() {
    let (mut client, server_side) = tokio::io::duplex(8192);
    let state = server();
    let handle = tokio::spawn(handle_connection(server_side, state));

    complete_startup(&mut client).await;
    send_frontend(
        &mut client,
        &FrontendMessage::Query {
            sql: "GIBBERISH NOT SQL".to_string(),
        },
    )
    .await;
    let msgs = drain_until_ready(&mut client).await;

    let err = msgs
        .iter()
        .find(|m| matches!(m, BackendMessage::ErrorResponse { .. }))
        .expect("error response present");
    match err {
        BackendMessage::ErrorResponse { fields } => {
            // Severity, code, and message fields are populated.
            let codes: Vec<u8> = fields.iter().map(|(c, _)| *c).collect();
            assert!(codes.contains(&b'S'));
            assert!(codes.contains(&b'C'));
            assert!(codes.contains(&b'M'));
        }
        _ => unreachable!(),
    }

    send_frontend(&mut client, &FrontendMessage::Terminate).await;
    drop(client);
    handle.await.expect("task joins").expect("clean exit");
}

#[tokio::test]
async fn terminate_ends_the_session_cleanly() {
    let (mut client, server_side) = tokio::io::duplex(8192);
    let state = server();
    let handle = tokio::spawn(handle_connection(server_side, state));

    complete_startup(&mut client).await;
    send_frontend(&mut client, &FrontendMessage::Terminate).await;
    // Closing the client confirms the server returns cleanly.
    drop(client);
    let result = handle.await.expect("task joins");
    result.expect("clean exit on Terminate");
}

#[tokio::test]
async fn empty_query_returns_empty_query_response() {
    let (mut client, server_side) = tokio::io::duplex(8192);
    let state = server();
    let handle = tokio::spawn(handle_connection(server_side, state));

    complete_startup(&mut client).await;
    send_frontend(&mut client, &FrontendMessage::Query { sql: String::new() }).await;
    let msgs = drain_until_ready(&mut client).await;
    assert!(
        msgs.iter()
            .any(|m| matches!(m, BackendMessage::EmptyQueryResponse))
    );
    assert!(matches!(
        msgs.last().unwrap(),
        BackendMessage::ReadyForQuery { status: b'I' }
    ));

    send_frontend(&mut client, &FrontendMessage::Terminate).await;
    drop(client);
    handle.await.expect("task joins").expect("clean exit");
}

#[tokio::test]
async fn comment_only_query_returns_empty_query_response() {
    let (mut client, server_side) = tokio::io::duplex(8192);
    let state = server();
    let handle = tokio::spawn(handle_connection(server_side, state));

    complete_startup(&mut client).await;
    send_frontend(
        &mut client,
        &FrontendMessage::Query {
            sql: "-- ping".to_owned(),
        },
    )
    .await;
    let msgs = drain_until_ready(&mut client).await;
    assert!(
        msgs.iter()
            .any(|m| matches!(m, BackendMessage::EmptyQueryResponse))
    );
    assert!(matches!(
        msgs.last().unwrap(),
        BackendMessage::ReadyForQuery { status: b'I' }
    ));

    send_frontend(&mut client, &FrontendMessage::Terminate).await;
    drop(client);
    handle.await.expect("task joins").expect("clean exit");
}

/// Adversarial input: a client that advertises `protocol_major =
/// 0xFFFF` (or any non-3 value, including the negotiated future
/// minor protocol number used by clients targeting newer servers)
/// must be rejected cleanly with an `ErrorResponse` carrying
/// SQLSTATE 08P01, followed by a clean connection close — not a
/// panic, not a silent EOF.
#[tokio::test]
async fn unsupported_protocol_major_returns_error_response() {
    let (mut client, server_side) = tokio::io::duplex(8192);
    let state = server();
    let handle = tokio::spawn(handle_connection(server_side, state));

    // Send a startup with a wildly future major.
    send_frontend(
        &mut client,
        &FrontendMessage::StartupMessage {
            protocol_major: 0xFFFF,
            protocol_minor: 0,
            params: vec![("user".to_string(), "anyone".to_string())],
        },
    )
    .await;

    // Drain whatever bytes the server sent back before closing.
    let mut buf = BytesMut::with_capacity(1024);
    let mut tmp = [0_u8; 1024];
    loop {
        let n = client.read(&mut tmp).await.expect("read");
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&tmp[..n]);
    }

    // The first decoded backend message must be an ErrorResponse
    // with SQLSTATE 08P01.
    let msg = ultrasql_protocol::decode_backend(&mut buf)
        .expect("decode")
        .expect("non-empty");
    match msg {
        BackendMessage::ErrorResponse { fields } => {
            let code = fields
                .iter()
                .find_map(|(c, v)| (*c == b'C').then(|| v.clone()))
                .expect("SQLSTATE field present");
            assert_eq!(code, "08P01");
        }
        other => panic!("expected ErrorResponse, got {other:?}"),
    }

    // The handler task must have returned with the
    // UnsupportedProtocol classification (not a panic).
    let result = handle.await.expect("task joins");
    assert!(matches!(
        result,
        Err(ServerError::UnsupportedProtocol { major: 0xFFFF, .. })
    ));
}

/// `TxnState::ready_for_query_status` maps each variant to the
/// correct PostgreSQL status byte. Unit test, no I/O.
#[test]
fn txn_state_ready_for_query_status_matches_postgres() {
    // The Failed and InTransaction arms hold a Transaction handle,
    // which we mint via a throwaway TxnManager.
    let mgr = TransactionManager::new();
    let txn1 = mgr.begin(IsolationLevel::ReadCommitted);
    let txn2 = mgr.begin(IsolationLevel::ReadCommitted);

    assert_eq!(TxnState::Idle.ready_for_query_status(), b'I');
    assert_eq!(TxnState::InTransaction(txn1).ready_for_query_status(), b'T');
    assert_eq!(TxnState::Failed(txn2).ready_for_query_status(), b'E');
}
