//! `#[cfg(test)] mod tests` split across a `tests/` directory
//! so no single test file blows past the 600-line ceiling.
//!
//! Helpers live in this module's root; tests are grouped by
//! category into sub-modules.

#![allow(unused_imports, dead_code)]

use super::*;
use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use ultrasql_catalog::IndexEntry;
use ultrasql_protocol::{FrontendMessage, decode_frontend, encode_backend};
use ultrasql_txn::IsolationLevel;

/// Read every backend message currently buffered on `io`, stopping
/// once a `ReadyForQuery` is observed. Returns the collected
/// messages.
async fn drain_until_ready(io: &mut (impl AsyncRead + Unpin)) -> Vec<BackendMessage> {
    let mut buf = BytesMut::with_capacity(4096);
    let mut out = Vec::new();
    let mut tmp = [0_u8; 1024];
    loop {
        // Try to decode messages already in `buf`.
        while let Some(msg) = ultrasql_protocol::decode_backend(&mut buf).expect("decode") {
            let is_ready = matches!(msg, BackendMessage::ReadyForQuery { .. });
            out.push(msg);
            if is_ready {
                return out;
            }
        }
        let n = io.read(&mut tmp).await.expect("read");
        if n == 0 {
            return out;
        }
        buf.extend_from_slice(&tmp[..n]);
    }
}

/// Send a frontend message and flush.
async fn send_frontend(io: &mut (impl AsyncWrite + Unpin), msg: &FrontendMessage) {
    let mut buf = BytesMut::new();
    ultrasql_protocol::encode_frontend(msg, &mut buf);
    io.write_all(&buf).await.expect("write");
    io.flush().await.expect("flush");
}

fn server() -> Arc<Server> {
    Arc::new(Server::with_sample_database())
}

#[test]
fn validation_report_covers_required_admin_checks() {
    let server = Server::with_sample_database();
    let report = server.validate();

    assert!(report.is_ok(), "{report:?}");
    for name in [
        "catalog",
        "indexes",
        "wal",
        "heap_visibility",
        "ann_tombstones",
    ] {
        assert!(
            report.checks.iter().any(|check| check.name == name),
            "missing check {name}"
        );
    }
}

async fn complete_startup(client: &mut (impl AsyncRead + AsyncWrite + Unpin)) {
    send_frontend(
        client,
        &FrontendMessage::StartupMessage {
            protocol_major: 3,
            protocol_minor: 0,
            params: vec![("user".to_string(), "tester".to_string())],
        },
    )
    .await;
    let msgs = drain_until_ready(client).await;
    // Sanity-check the handshake shape: ends in ReadyForQuery 'I'.
    assert!(matches!(
        msgs.last().unwrap(),
        BackendMessage::ReadyForQuery { status: b'I' }
    ));
    // AuthenticationOk must appear at least once.
    assert!(
        msgs.iter()
            .any(|m| matches!(m, BackendMessage::AuthenticationOk))
    );
}

// -----------------------------------------------------------------------
// Transaction-control state machine — Simple Query duplex tests
// -----------------------------------------------------------------------

/// Helper: extract the trailing `ReadyForQuery` status byte from a
/// drained message sequence.
fn ready_status(msgs: &[BackendMessage]) -> u8 {
    match msgs.last().expect("non-empty msgs") {
        BackendMessage::ReadyForQuery { status } => *status,
        other => panic!("expected ReadyForQuery at end, got {other:?}"),
    }
}

/// Helper: extract the `CommandComplete` tag from a drained message
/// sequence.
fn command_tag(msgs: &[BackendMessage]) -> Option<String> {
    msgs.iter().find_map(|m| match m {
        BackendMessage::CommandComplete { tag } => Some(tag.clone()),
        _ => None,
    })
}

mod basic;
mod ddl_alter;
mod ddl_create;
mod extended;
mod plan_cache;
mod recovery;
mod txn;
