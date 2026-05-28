//! Wire protocol edge cases that need raw protocol control.

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
    let _ = drain_until_ready(client).await;
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

fn error_field(fields: &[(u8, String)], tag: u8) -> Option<&str> {
    fields
        .iter()
        .find_map(|(field_tag, value)| (*field_tag == tag).then_some(value.as_str()))
}

#[tokio::test]
async fn bind_rejects_unsupported_result_format_and_preserves_failed_txn_state() {
    let (mut client, server_side) = duplex(64 * 1024);
    let state = Arc::new(Server::with_sample_database());
    let handle = tokio::spawn(handle_connection(server_side, state));

    complete_startup(&mut client).await;

    send(
        &mut client,
        &FrontendMessage::Query {
            sql: "BEGIN".to_string(),
        },
    )
    .await;
    let begin = drain_until_ready(&mut client).await;
    assert!(
        matches!(
            begin.last(),
            Some(BackendMessage::ReadyForQuery { status: b'T' })
        ),
        "BEGIN must enter transaction state: {begin:?}"
    );

    send(
        &mut client,
        &FrontendMessage::Parse {
            name: "stmt_bad_format".to_string(),
            sql: "SELECT id FROM users".to_string(),
            param_types: Vec::new(),
        },
    )
    .await;
    send(
        &mut client,
        &FrontendMessage::Bind {
            portal_name: "portal_bad_format".to_string(),
            statement_name: "stmt_bad_format".to_string(),
            param_formats: Vec::new(),
            params: Vec::new(),
            result_formats: vec![2],
        },
    )
    .await;
    send(&mut client, &FrontendMessage::Sync).await;

    let msgs = drain_until_ready(&mut client).await;
    let err = msgs
        .iter()
        .find_map(|msg| match msg {
            BackendMessage::ErrorResponse { fields } => Some(fields),
            _ => None,
        })
        .expect("Bind must reject unsupported result format");
    assert_eq!(error_field(err, b'S'), Some("ERROR"));
    assert_eq!(error_field(err, b'C'), Some("0A000"));
    assert!(
        error_field(err, b'M').is_some_and(|msg| msg.contains("result format")),
        "message must name result format problem: {err:?}"
    );
    assert!(
        matches!(
            msgs.last(),
            Some(BackendMessage::ReadyForQuery { status: b'E' })
        ),
        "failed Bind inside BEGIN must report failed transaction: {msgs:?}"
    );

    send(
        &mut client,
        &FrontendMessage::Query {
            sql: "ROLLBACK".to_string(),
        },
    )
    .await;
    let rollback = drain_until_ready(&mut client).await;
    assert!(
        matches!(
            rollback.last(),
            Some(BackendMessage::ReadyForQuery { status: b'I' })
        ),
        "ROLLBACK must return to idle state: {rollback:?}"
    );

    send(&mut client, &FrontendMessage::Terminate).await;
    drop(client);
    handle.await.expect("task joins").expect("clean exit");
}
