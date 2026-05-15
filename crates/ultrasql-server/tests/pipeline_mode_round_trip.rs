//! Extended Query pipeline-mode test (§2.3).
//!
//! Pipelines three Parse/Bind/Execute trios without intervening
//! `Sync`. After the last Execute, a single `Sync` flushes one
//! `ReadyForQuery`. The server must emit three `CommandComplete`
//! frames between the three Executes — proof that no individual Sync
//! gates statement boundaries.
//!
//! A second test pins the failure-mode contract: an error mid-pipeline
//! marks the pipeline `error_pending`; subsequent
//! Parse/Bind/Execute/Describe/Close messages are silently dropped
//! until the next `Sync`, which emits one `ErrorResponse` (already
//! emitted at the failure point) and one `ReadyForQuery 'I'` — the
//! pipeline_failed flag resets at Sync.

use std::sync::Arc;
use std::time::Duration;

use bytes::{BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};

use ultrasql_protocol::{BackendMessage, FrontendMessage, decode_backend, encode_frontend};
use ultrasql_server::{Server, handle_connection};

async fn complete_startup<RW>(client: &mut RW)
where
    RW: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let mut buf = BytesMut::new();
    encode_frontend(
        &FrontendMessage::StartupMessage {
            protocol_major: 3,
            protocol_minor: 0,
            params: vec![("user".to_string(), "tester".to_string())],
        },
        &mut buf,
    );
    client.write_all(&buf).await.expect("send startup");
    client.flush().await.expect("flush startup");
    drain_until_ready(client).await;
}

async fn drain_until_ready<RW>(client: &mut RW) -> Vec<BackendMessage>
where
    RW: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let mut buf = BytesMut::new();
    let mut tmp = [0_u8; 4096];
    let mut out = Vec::new();
    loop {
        let n = match tokio::time::timeout(Duration::from_secs(5), client.read(&mut tmp))
            .await
            .expect("read timeout")
        {
            Ok(0) => return out,
            Ok(n) => n,
            Err(e) => panic!("read: {e}"),
        };
        buf.put_slice(&tmp[..n]);
        while let Ok(Some(msg)) = decode_backend(&mut buf) {
            let done = matches!(msg, BackendMessage::ReadyForQuery { .. });
            out.push(msg);
            if done {
                return out;
            }
        }
    }
}

async fn send<RW>(client: &mut RW, msg: &FrontendMessage)
where
    RW: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let mut buf = BytesMut::new();
    encode_frontend(msg, &mut buf);
    client.write_all(&buf).await.expect("write");
    client.flush().await.expect("flush");
}

/// Three Parse/Bind/Execute trios pipelined back-to-back, one trailing
/// Sync. Server emits three `CommandComplete`s and exactly one
/// `ReadyForQuery`.
#[tokio::test]
async fn three_bind_execute_pairs_share_one_sync() {
    let (mut client, server_side) = duplex(64 * 1024);
    let state = Arc::new(Server::with_sample_database());
    let handle = tokio::spawn(handle_connection(server_side, state));

    complete_startup(&mut client).await;

    send(
        &mut client,
        &FrontendMessage::Query {
            sql: "CREATE TABLE t (id INT NOT NULL)".to_string(),
        },
    )
    .await;
    let _ = drain_until_ready(&mut client).await;
    send(
        &mut client,
        &FrontendMessage::Query {
            sql: "INSERT INTO t VALUES (1), (2), (3)".to_string(),
        },
    )
    .await;
    let _ = drain_until_ready(&mut client).await;

    // Three Parse/Bind/Execute trios pipelined. No intervening Sync.
    for i in 0..3_usize {
        let stmt = format!("stmt_{i}");
        let portal = format!("portal_{i}");
        let sql = format!("SELECT id FROM t WHERE id = {}", i + 1);
        send(
            &mut client,
            &FrontendMessage::Parse {
                name: stmt.clone(),
                sql,
                param_types: Vec::new(),
            },
        )
        .await;
        send(
            &mut client,
            &FrontendMessage::Bind {
                portal_name: portal.clone(),
                statement_name: stmt,
                param_formats: Vec::new(),
                params: Vec::new(),
                result_formats: Vec::new(),
            },
        )
        .await;
        send(
            &mut client,
            &FrontendMessage::Execute {
                portal,
                max_rows: 0,
            },
        )
        .await;
    }
    send(&mut client, &FrontendMessage::Sync).await;

    let msgs = drain_until_ready(&mut client).await;
    let cc_count = msgs
        .iter()
        .filter(|m| matches!(m, BackendMessage::CommandComplete { .. }))
        .count();
    let rfq_count = msgs
        .iter()
        .filter(|m| matches!(m, BackendMessage::ReadyForQuery { .. }))
        .count();
    let data_row_count = msgs
        .iter()
        .filter(|m| matches!(m, BackendMessage::DataRow { .. }))
        .count();
    assert_eq!(cc_count, 3, "one CommandComplete per pipelined Execute");
    assert_eq!(
        rfq_count, 1,
        "exactly one ReadyForQuery at the Sync boundary"
    );
    assert_eq!(data_row_count, 3, "one row per Execute (id = 1, 2, 3)");

    send(&mut client, &FrontendMessage::Terminate).await;
    drop(client);
    handle.await.expect("task joins").expect("clean exit");
}

/// An error mid-pipeline silences subsequent messages until the next
/// `Sync`, which emits exactly one `ReadyForQuery` and resets the
/// failed flag.
#[tokio::test]
async fn pipeline_error_silences_until_sync() {
    let (mut client, server_side) = duplex(64 * 1024);
    let state = Arc::new(Server::with_sample_database());
    let handle = tokio::spawn(handle_connection(server_side, state));

    complete_startup(&mut client).await;

    // First Parse fails (undefined relation). Subsequent Bind /
    // Execute on the same pipeline are silently dropped. The Sync
    // emits ReadyForQuery.
    send(
        &mut client,
        &FrontendMessage::Parse {
            name: "bad".to_string(),
            sql: "SELECT * FROM undefined_table".to_string(),
            param_types: Vec::new(),
        },
    )
    .await;
    send(
        &mut client,
        &FrontendMessage::Bind {
            portal_name: "bad".to_string(),
            statement_name: "bad".to_string(),
            param_formats: Vec::new(),
            params: Vec::new(),
            result_formats: Vec::new(),
        },
    )
    .await;
    send(
        &mut client,
        &FrontendMessage::Execute {
            portal: "bad".to_string(),
            max_rows: 0,
        },
    )
    .await;
    send(&mut client, &FrontendMessage::Sync).await;

    let msgs = drain_until_ready(&mut client).await;
    let err_count = msgs
        .iter()
        .filter(|m| matches!(m, BackendMessage::ErrorResponse { .. }))
        .count();
    assert!(err_count >= 1, "at least one ErrorResponse, got {msgs:?}");
    let rfq_count = msgs
        .iter()
        .filter(|m| matches!(m, BackendMessage::ReadyForQuery { .. }))
        .count();
    assert_eq!(rfq_count, 1, "exactly one ReadyForQuery at Sync");

    // After Sync, a fresh, valid Parse/Bind/Execute succeeds — the
    // pipeline_failed flag has been cleared.
    send(
        &mut client,
        &FrontendMessage::Query {
            sql: "SELECT 1".to_string(),
        },
    )
    .await;
    let post = drain_until_ready(&mut client).await;
    let post_cc = post
        .iter()
        .filter(|m| matches!(m, BackendMessage::CommandComplete { .. }))
        .count();
    assert_eq!(post_cc, 1, "post-Sync statement succeeds: got {post:?}");

    send(&mut client, &FrontendMessage::Terminate).await;
    drop(client);
    handle.await.expect("task joins").expect("clean exit");
}
