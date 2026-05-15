//! End-to-end portal-resumption test (§2.4).
//!
//! Drives an Extended Query loop manually so each `Execute` carries a
//! non-zero `max_rows` cap. After the cap, the server emits
//! `PortalSuspended`; the next `Execute` on the same portal resumes
//! from the row that was about to be emitted, not from scratch.
//!
//! The test issues ten `Execute(portal, 10)` calls against a 100-row
//! `SELECT id FROM t ORDER BY id` and asserts:
//!
//! - Each `Execute` returns exactly ten `DataRow` messages.
//! - The aggregate row stream is the 100 distinct seeded `id`s in
//!   order (no gaps, no duplicates).
//! - All nine intermediate `Execute`s emit `PortalSuspended`.
//! - The tenth emits `CommandComplete "SELECT 100"`.

use std::sync::Arc;
use std::time::Duration;

use bytes::{BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};

use ultrasql_protocol::{
    BackendMessage, DescribeKind, FrontendMessage, decode_backend, encode_frontend,
};
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
    // Drain until ReadyForQuery.
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

#[tokio::test]
async fn portal_resumption_emits_remaining_rows_across_executes() {
    let (mut client, server_side) = duplex(64 * 1024);
    let state = Arc::new(Server::with_sample_database());
    let handle = tokio::spawn(handle_connection(server_side, state));

    complete_startup(&mut client).await;

    // Seed the table via Simple Query.
    send(
        &mut client,
        &FrontendMessage::Query {
            sql: "CREATE TABLE t (id INT NOT NULL)".to_string(),
        },
    )
    .await;
    let _ = drain_until_ready(&mut client).await;

    // Bulk insert 100 rows via a single multi-row VALUES.
    let mut values = String::from("INSERT INTO t VALUES ");
    for i in 0..100 {
        if i > 0 {
            values.push(',');
        }
        values.push_str(&format!("({i})"));
    }
    send(&mut client, &FrontendMessage::Query { sql: values }).await;
    let _ = drain_until_ready(&mut client).await;

    // Parse + Bind a single SELECT into a named portal.
    send(
        &mut client,
        &FrontendMessage::Parse {
            name: "stmt_one".to_string(),
            sql: "SELECT id FROM t ORDER BY id".to_string(),
            param_types: Vec::new(),
        },
    )
    .await;
    send(
        &mut client,
        &FrontendMessage::Bind {
            portal_name: "p_one".to_string(),
            statement_name: "stmt_one".to_string(),
            param_formats: Vec::new(),
            params: Vec::new(),
            result_formats: Vec::new(),
        },
    )
    .await;
    send(
        &mut client,
        &FrontendMessage::Describe {
            kind: DescribeKind::Portal,
            name: "p_one".to_string(),
        },
    )
    .await;

    let mut seen_rows: Vec<i32> = Vec::with_capacity(100);
    let mut suspended_count = 0_usize;
    let mut completed = false;
    for iter in 0..10 {
        send(
            &mut client,
            &FrontendMessage::Execute {
                portal: "p_one".to_string(),
                max_rows: 10,
            },
        )
        .await;
        send(&mut client, &FrontendMessage::Sync).await;
        let msgs = drain_until_ready(&mut client).await;
        let mut got_rows_this = 0;
        for m in &msgs {
            match m {
                BackendMessage::DataRow { columns } => {
                    let bytes = columns
                        .first()
                        .expect("one col")
                        .as_ref()
                        .expect("non-null id");
                    let text = std::str::from_utf8(bytes).expect("utf8");
                    let v: i32 = text.parse().expect("int");
                    seen_rows.push(v);
                    got_rows_this += 1;
                }
                BackendMessage::PortalSuspended => suspended_count += 1,
                BackendMessage::CommandComplete { tag } => {
                    completed = true;
                    assert_eq!(tag, "SELECT 100", "final tag at iter {iter}");
                }
                _ => {}
            }
        }
        assert_eq!(got_rows_this, 10, "iter {iter} emits 10 rows");
    }
    assert_eq!(seen_rows, (0_i32..100).collect::<Vec<_>>());
    assert_eq!(
        suspended_count, 9,
        "exactly 9 PortalSuspended messages across the 10 Executes"
    );
    assert!(completed, "final Execute must emit CommandComplete");

    send(&mut client, &FrontendMessage::Terminate).await;
    drop(client);
    handle.await.expect("task joins").expect("clean exit");
}
