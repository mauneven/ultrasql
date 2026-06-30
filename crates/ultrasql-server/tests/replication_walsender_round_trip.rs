//! Phase 1 (control plane) walsender protocol round-trip.
//!
//! Drives a raw libpq replication connection against an in-process server and
//! exercises the handshake a standby performs before streaming: `IDENTIFY_SYSTEM`,
//! `CREATE_REPLICATION_SLOT … PHYSICAL` (+ on-disk persistence and duplicate
//! rejection), `DROP_REPLICATION_SLOT`, and the defined deferred-streaming error
//! for `START_REPLICATION`. See `docs/streaming-replication-design.md`.

pub mod support;

use bytes::BytesMut;
use support::{shutdown, start_persistent_server};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use ultrasql_protocol::{BackendMessage, FrontendMessage, decode_backend, encode_frontend};

async fn read_backend_message(stream: &mut TcpStream, buf: &mut BytesMut) -> BackendMessage {
    loop {
        if let Some(message) = decode_backend(buf).expect("backend message decodes") {
            return message;
        }
        let mut chunk = [0_u8; 8192];
        let n = stream.read(&mut chunk).await.expect("socket read");
        assert!(n > 0, "server closed connection mid-message");
        buf.extend_from_slice(&chunk[..n]);
    }
}

/// Collect backend messages up to and including the next `ReadyForQuery`.
async fn drain_to_ready(stream: &mut TcpStream, buf: &mut BytesMut) -> Vec<BackendMessage> {
    let mut out = Vec::new();
    loop {
        let msg = read_backend_message(stream, buf).await;
        let done = matches!(msg, BackendMessage::ReadyForQuery { .. });
        out.push(msg);
        if done {
            return out;
        }
    }
}

async fn send_query(stream: &mut TcpStream, sql: &str) {
    let mut out = BytesMut::new();
    encode_frontend(
        &FrontendMessage::Query {
            sql: sql.to_owned(),
        },
        &mut out,
    );
    stream.write_all(&out).await.expect("write query");
}

fn data_row(messages: &[BackendMessage]) -> &[Option<Vec<u8>>] {
    messages
        .iter()
        .find_map(|m| match m {
            BackendMessage::DataRow { columns } => Some(columns.as_slice()),
            _ => None,
        })
        .expect("a DataRow")
}

fn has_error(messages: &[BackendMessage]) -> bool {
    messages
        .iter()
        .any(|m| matches!(m, BackendMessage::ErrorResponse { .. }))
}

fn error_code(messages: &[BackendMessage]) -> Option<String> {
    messages.iter().find_map(|m| match m {
        BackendMessage::ErrorResponse { fields } => fields
            .iter()
            .find(|(tag, _)| *tag == b'C')
            .map(|(_, v)| v.clone()),
        _ => None,
    })
}

#[tokio::test]
async fn walsender_handshake_identify_system_and_slot_lifecycle() {
    let data_dir = tempfile::TempDir::new().expect("temp data dir");
    let running = start_persistent_server(data_dir.path(), "walsender_round_trip").await;
    let addr = running.bound;

    let mut stream = TcpStream::connect(addr).await.expect("raw wire connect");
    let mut buf = BytesMut::new();

    // Startup with the `replication` parameter routes us to the walsender loop.
    let mut out = BytesMut::new();
    encode_frontend(
        &FrontendMessage::StartupMessage {
            protocol_major: 3,
            protocol_minor: 0,
            params: vec![
                ("user".to_owned(), "tester".to_owned()),
                ("replication".to_owned(), "true".to_owned()),
                ("application_name".to_owned(), "walsender_probe".to_owned()),
            ],
        },
        &mut out,
    );
    stream.write_all(&out).await.expect("write startup");
    // Auth + ParameterStatus + BackendKeyData + initial ReadyForQuery.
    drain_to_ready(&mut stream, &mut buf).await;

    // ---- IDENTIFY_SYSTEM ----
    send_query(&mut stream, "IDENTIFY_SYSTEM").await;
    let ident = drain_to_ready(&mut stream, &mut buf).await;
    assert!(!has_error(&ident), "IDENTIFY_SYSTEM errored: {ident:?}");
    let fields = ident
        .iter()
        .find_map(|m| match m {
            BackendMessage::RowDescription { fields } => Some(fields),
            _ => None,
        })
        .expect("RowDescription");
    let names: Vec<&str> = fields.iter().map(|f| f.name.as_str()).collect();
    assert_eq!(names, ["systemid", "timeline", "xlogpos", "dbname"]);
    let row = data_row(&ident);
    assert_eq!(row.len(), 4);
    assert!(row[0].as_ref().is_some_and(|v| !v.is_empty()), "systemid");
    assert_eq!(row[1].as_deref(), Some(b"1".as_slice()), "timeline = 1");
    let xlogpos = String::from_utf8(row[2].clone().expect("xlogpos")).unwrap();
    assert!(xlogpos.contains('/'), "xlogpos is pg_lsn text: {xlogpos}");
    assert!(row[3].is_none(), "dbname is NULL for physical replication");

    // ---- CREATE_REPLICATION_SLOT ... PHYSICAL ----
    send_query(&mut stream, "CREATE_REPLICATION_SLOT phys_test PHYSICAL").await;
    let created = drain_to_ready(&mut stream, &mut buf).await;
    assert!(!has_error(&created), "CREATE slot errored: {created:?}");
    let crow = data_row(&created);
    assert_eq!(
        crow[0].as_deref(),
        Some(b"phys_test".as_slice()),
        "slot_name"
    );
    let consistent = String::from_utf8(crow[1].clone().expect("consistent_point")).unwrap();
    assert!(
        consistent.contains('/'),
        "consistent_point pg_lsn: {consistent}"
    );

    // The slot is persisted on disk under pg_replslot with a pg_lsn restart_lsn.
    let slot_file = data_dir.path().join("pg_replslot").join("phys_test.slot");
    assert!(slot_file.exists(), "slot file persisted at {slot_file:?}");
    let body = std::fs::read_to_string(&slot_file).expect("read slot file");
    assert!(
        body.contains("restart_lsn=") && body.contains('/'),
        "slot persists a pg_lsn restart_lsn: {body}"
    );

    // ---- duplicate create is rejected ----
    send_query(&mut stream, "CREATE_REPLICATION_SLOT phys_test PHYSICAL").await;
    let dup = drain_to_ready(&mut stream, &mut buf).await;
    assert_eq!(error_code(&dup).as_deref(), Some("42710"), "duplicate slot");

    // ---- START_REPLICATION for a missing slot errors before any streaming ----
    send_query(
        &mut stream,
        "START_REPLICATION SLOT no_such_slot PHYSICAL 0/0",
    )
    .await;
    let bad_start = drain_to_ready(&mut stream, &mut buf).await;
    assert_eq!(
        error_code(&bad_start).as_deref(),
        Some("42704"),
        "START_REPLICATION on a missing slot errors before CopyBoth"
    );

    // ---- DROP_REPLICATION_SLOT ----
    send_query(&mut stream, "DROP_REPLICATION_SLOT phys_test").await;
    let dropped = drain_to_ready(&mut stream, &mut buf).await;
    assert!(!has_error(&dropped), "DROP slot errored: {dropped:?}");
    assert!(
        dropped
            .iter()
            .any(|m| matches!(m, BackendMessage::CommandComplete { tag } if tag == "DROP_REPLICATION_SLOT")),
        "CommandComplete for DROP: {dropped:?}"
    );
    assert!(!slot_file.exists(), "slot file removed after DROP");

    // ---- dropping a missing slot is an error ----
    send_query(&mut stream, "DROP_REPLICATION_SLOT phys_test").await;
    let missing = drain_to_ready(&mut stream, &mut buf).await;
    assert_eq!(
        error_code(&missing).as_deref(),
        Some("42704"),
        "missing slot"
    );

    // Polite goodbye, then tear down.
    let mut term = BytesMut::new();
    encode_frontend(&FrontendMessage::Terminate, &mut term);
    let _ = stream.write_all(&term).await;
    drop(stream);
    shutdown(running).await;
}

#[tokio::test]
async fn non_replication_connection_is_unaffected() {
    // A normal (non-replication) connection still speaks SQL: a control check
    // that the startup routing only diverts when `replication` is truthy.
    let data_dir = tempfile::TempDir::new().expect("temp data dir");
    let running = start_persistent_server(data_dir.path(), "walsender_control").await;
    let rows = running
        .client
        .simple_query("SELECT 1")
        .await
        .expect("plain SQL still works on a non-replication connection");
    assert!(!rows.is_empty());
    shutdown(running).await;
}

#[tokio::test]
async fn walsender_start_replication_streams_durable_wal() {
    let data_dir = tempfile::TempDir::new().expect("temp data dir");
    let running = start_persistent_server(data_dir.path(), "walsender_stream").await;

    // Generate committed WAL on a normal SQL connection so there is durable WAL
    // to stream from the beginning of the log.
    running
        .client
        .batch_execute(
            "CREATE TABLE stream_t (id INT); INSERT INTO stream_t VALUES (1),(2),(3),(4),(5);",
        )
        .await
        .expect("generate WAL");

    let addr = running.bound;
    let mut stream = TcpStream::connect(addr).await.expect("raw wire connect");
    let mut buf = BytesMut::new();

    let mut out = BytesMut::new();
    encode_frontend(
        &FrontendMessage::StartupMessage {
            protocol_major: 3,
            protocol_minor: 0,
            params: vec![
                ("user".to_owned(), "tester".to_owned()),
                ("replication".to_owned(), "true".to_owned()),
            ],
        },
        &mut out,
    );
    stream.write_all(&out).await.expect("write startup");
    drain_to_ready(&mut stream, &mut buf).await;

    // Stream physical WAL from the start of the log.
    send_query(&mut stream, "START_REPLICATION PHYSICAL 0/0").await;

    // Expect CopyBothResponse, then XLogData ('w') CopyData frames. After the
    // first frame, ask the server to stop with CopyDone and drain to
    // ReadyForQuery (tolerating any further in-flight frames / keepalives).
    let mut saw_copyboth = false;
    let mut xlog_frames: Vec<Vec<u8>> = Vec::new();
    let mut sent_copydone = false;
    loop {
        match read_backend_message(&mut stream, &mut buf).await {
            BackendMessage::CopyBothResponse { .. } => saw_copyboth = true,
            BackendMessage::CopyData(payload) => {
                if payload.first() == Some(&b'w') {
                    xlog_frames.push(payload);
                }
                if !sent_copydone {
                    let mut done = BytesMut::new();
                    encode_frontend(&FrontendMessage::CopyDone, &mut done);
                    stream.write_all(&done).await.expect("write CopyDone");
                    sent_copydone = true;
                }
            }
            BackendMessage::ReadyForQuery { .. } => break,
            BackendMessage::ErrorResponse { fields } => panic!("stream errored: {fields:?}"),
            _ => {}
        }
    }

    assert!(saw_copyboth, "server initiated the CopyBoth stream");
    assert!(
        !xlog_frames.is_empty(),
        "received at least one XLogData frame"
    );

    // The first frame's WAL bytes (after the 25-byte XLogData header: 'w' +
    // 3×Int64) decode as a valid WAL record — proves end-to-end byte fidelity.
    let first = &xlog_frames[0];
    assert!(first.len() > 25, "XLogData frame carries WAL bytes");
    let (_record, used) = ultrasql_wal::record::WalRecord::decode(&first[25..])
        .expect("streamed bytes decode as a WAL record");
    assert!(used > 0, "decoded a non-empty WAL record");

    // Close the replication socket so the server's session task ends, otherwise
    // shutdown blocks on the parked walsender connection (2s timeout).
    drop(stream);
    shutdown(running).await;
}
