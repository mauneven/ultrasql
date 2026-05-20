//! End-to-end `COPY` round-trip tests against a real `tokio-postgres`
//! client.
//!
//! Closes the v0.5 wire-protocol gap "COPY wire dispatch" by driving an
//! in-process `ultrasqld` with a stock `tokio-postgres` client and
//! asserting the four canonical wire shapes round-trip:
//!
//! 1. `COPY t FROM STDIN` (text) — `Client::copy_in` streams rows; the
//!    server lands them in the heap; a subsequent `SELECT COUNT(*)`
//!    returns the expected count.
//! 2. `COPY t TO STDOUT` (text) — `Client::copy_out` streams the rows
//!    back; the test compares byte-for-byte against the payload it
//!    fed into `COPY FROM STDIN`.
//! 3. `COPY t FROM STDIN` followed by `COPY t TO STDOUT` round-trip —
//!    the assemblage of input lines re-emerges exactly.
//! 4. `COPY t FROM STDIN WITH (FORMAT CSV)` — the CSV variant lands
//!    rows correctly even with quoted strings.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::SinkExt;
use tokio_postgres::NoTls;
use ultrasql_server::{Server, bind_listener, serve_listener};

fn sql_string(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// Spin up an in-process server on an ephemeral TCP port and return a
/// connected `tokio-postgres` client plus the join handles so the test
/// can shut everything down cleanly.
async fn start_server_and_connect() -> (
    tokio_postgres::Client,
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
) {
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("addr parses");
    let (listener, bound) = bind_listener(addr).await.expect("bind");
    let server = Arc::new(Server::with_sample_database());
    let server_handle = tokio::spawn(serve_listener(listener, server));

    let conn_str = format!(
        "host={host} port={port} user=tester application_name=copy_test",
        host = bound.ip(),
        port = bound.port()
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .expect("tokio-postgres connect");
    let conn_handle = tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });
    (client, conn_handle, server_handle)
}

async fn shutdown(
    client: tokio_postgres::Client,
    server_handle: tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
) {
    drop(client);
    tokio::time::sleep(Duration::from_millis(20)).await;
    server_handle.abort();
}

/// Run `SELECT COUNT(*) FROM <table>` via simple-query and return the
/// integer payload.
async fn select_count(client: &tokio_postgres::Client, table: &str) -> i64 {
    let rows = client
        .simple_query(&format!("SELECT COUNT(*) FROM {table}"))
        .await
        .expect("count query");
    let mut answer: Option<i64> = None;
    for m in rows {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = m {
            answer = Some(
                row.get(0)
                    .expect("count column present")
                    .parse::<i64>()
                    .expect("count parses"),
            );
        }
    }
    answer.expect("COUNT(*) returned a row")
}

/// Drain a `tokio_postgres::CopyOutStream` to a single `Vec<u8>` so we
/// can compare it to the payload we fed into `COPY FROM STDIN`.
async fn collect_copy_out(stream: tokio_postgres::CopyOutStream) -> Vec<u8> {
    use futures::StreamExt;
    let mut stream = Box::pin(stream);
    let mut out = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.expect("CopyData chunk");
        out.extend_from_slice(&chunk);
    }
    out
}

/// Push `payload` into `COPY t FROM STDIN` and finish the COPY cleanly.
///
/// Returns the row count `tokio-postgres` extracts from the trailing
/// `CommandComplete` (e.g. `"COPY N"`).
async fn copy_in_payload(client: &tokio_postgres::Client, sql: &str, payload: &[u8]) -> u64 {
    let sink = client
        .copy_in::<_, Bytes>(sql)
        .await
        .expect("copy_in establishes COPY FROM STDIN");
    // `CopyInSink` implements `Sink + !Unpin` because it holds a pinned
    // futures-channel. The `Sink::send` trait method requires `self:
    // Pin<&mut Self>`; tokio-rs/postgres test code wraps the sink in
    // `futures::pin_mut!` to get a `Pin<&mut _>` without taking a fresh
    // allocation. `finish()` then consumes the unpinned sink directly.
    futures::pin_mut!(sink);
    sink.as_mut()
        .send(Bytes::from(payload.to_vec()))
        .await
        .expect("send CopyData");
    sink.finish().await.expect("finish copy_in")
}

/// `COPY t FROM STDIN` over a populated relation lands every row.
#[tokio::test]
async fn copy_from_stdin_text_lands_rows() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE copy_from_text (id INT, label TEXT)")
        .await
        .expect("create table");

    let payload = b"1\talice\n2\tbob\n3\tcarol\n".to_vec();
    let rows_inserted = copy_in_payload(&client, "COPY copy_from_text FROM STDIN", &payload).await;
    assert_eq!(rows_inserted, 3);

    let n = select_count(&client, "copy_from_text").await;
    assert_eq!(n, 3, "COPY FROM STDIN must land every row");

    shutdown(client, server_handle).await;
}

/// `COPY t TO STDOUT` emits the rows it sees in heap order.
#[tokio::test]
async fn copy_to_stdout_text_emits_rows() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE copy_to_text (id INT, label TEXT)")
        .await
        .expect("create table");
    let payload = b"10\thello\n20\tworld\n".to_vec();
    copy_in_payload(&client, "COPY copy_to_text FROM STDIN", &payload).await;

    let stream = client
        .copy_out("COPY copy_to_text TO STDOUT")
        .await
        .expect("copy_out");
    let bytes = collect_copy_out(stream).await;
    assert_eq!(bytes, payload, "COPY TO STDOUT byte-equality");

    shutdown(client, server_handle).await;
}

/// The exact bytes pushed through `COPY FROM STDIN` come back through
/// `COPY TO STDOUT`. This is the integration "byte-equality of the
/// round-tripped text payload" property the workplan asks for.
#[tokio::test]
async fn copy_round_trip_text_is_byte_identical() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE copy_round_trip (id INT, label TEXT)")
        .await
        .expect("create table");

    let payload = b"1\talice\n2\tbob\n3\tcarol\n4\tdan\n5\teve\n".to_vec();
    copy_in_payload(&client, "COPY copy_round_trip FROM STDIN", &payload).await;

    let stream = client
        .copy_out("COPY copy_round_trip TO STDOUT")
        .await
        .expect("copy_out");
    let echoed = collect_copy_out(stream).await;
    assert_eq!(
        echoed, payload,
        "every byte fed into COPY FROM STDIN must re-emerge from COPY TO STDOUT"
    );

    shutdown(client, server_handle).await;
}

/// Low-cardinality text columns cross the automatic dictionary
/// threshold, so this exercises dictionary-backed heap decode plus
/// wire/COPY output decoding.
#[tokio::test]
async fn copy_round_trip_low_cardinality_text_stays_wire_correct() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE copy_dict_text (id INT, label TEXT)")
        .await
        .expect("create table");

    let mut payload = Vec::new();
    for i in 0..2048 {
        let line = format!("{i}\tlabel{}\n", i % 4);
        payload.extend_from_slice(line.as_bytes());
    }
    copy_in_payload(&client, "COPY copy_dict_text FROM STDIN", &payload).await;

    let stream = client
        .copy_out("COPY copy_dict_text TO STDOUT")
        .await
        .expect("copy_out");
    let echoed = collect_copy_out(stream).await;
    assert_eq!(echoed, payload);

    shutdown(client, server_handle).await;
}

/// `COPY t FROM STDIN WITH (FORMAT CSV)` ingests CSV rows correctly.
#[tokio::test]
async fn copy_from_stdin_csv_lands_rows() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE copy_csv (id INT, label TEXT)")
        .await
        .expect("create table");

    let payload = b"1,alice\n2,\"bob, jr\"\n3,carol\n".to_vec();
    let rows_inserted = copy_in_payload(
        &client,
        "COPY copy_csv FROM STDIN WITH (FORMAT CSV)",
        &payload,
    )
    .await;
    assert_eq!(rows_inserted, 3);

    let n = select_count(&client, "copy_csv").await;
    assert_eq!(n, 3);

    shutdown(client, server_handle).await;
}

/// `COPY t FROM 'file.csv' WITH (... AUTO_DETECT true)` sniffs dialect,
/// streams records, handles quoted newlines, and flushes multi-batch inserts.
#[tokio::test]
async fn copy_from_file_csv_autodetect_streams_batches() {
    let dir = tempfile::tempdir().expect("tempdir");
    let csv_path = dir.path().join("bulk.csv");
    let mut csv = String::from("id;label;note\r\n1;alpha;\"hello\nworld\"\r\n");
    for id in 2..=4101 {
        csv.push_str(&format!("{id};label-{id};note-{id}\r\n"));
    }
    std::fs::write(&csv_path, csv).expect("write csv");

    let (client, _conn_handle, server_handle) = start_server_and_connect().await;
    client
        .batch_execute("CREATE TABLE copy_file_auto (id INT, label TEXT, note TEXT)")
        .await
        .expect("create table");

    let copy_sql = format!(
        "COPY copy_file_auto FROM {} WITH (FORMAT csv, HEADER true, AUTO_DETECT true)",
        sql_string(csv_path.to_str().expect("utf8 path"))
    );
    client.batch_execute(&copy_sql).await.expect("copy file");

    let n = select_count(&client, "copy_file_auto").await;
    assert_eq!(n, 4101);

    let rows = client
        .query("SELECT label, note FROM copy_file_auto WHERE id = 1", &[])
        .await
        .expect("select copied row");
    assert_eq!(rows[0].get::<_, String>(0), "alpha");
    assert_eq!(rows[0].get::<_, String>(1), "hello\nworld");

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn copy_from_file_csv_quarantines_bad_rows_under_error_limit() {
    let dir = tempfile::tempdir().expect("tempdir");
    let csv_path = dir.path().join("bad_rows.csv");
    std::fs::write(
        &csv_path,
        "id,label\n1,ok\nbad,broken\n2,good\n3,too,many\n4,last\n",
    )
    .expect("write csv");

    let (client, _conn_handle, server_handle) = start_server_and_connect().await;
    client
        .batch_execute("CREATE TABLE copy_quarantine (id INT, label TEXT)")
        .await
        .expect("create target");
    client
        .batch_execute(
            "CREATE TABLE csv_rejects (
                filename TEXT,
                line_number BIGINT,
                raw_row TEXT,
                error TEXT
            )",
        )
        .await
        .expect("create rejects");

    let copy_sql = format!(
        "COPY copy_quarantine FROM {} WITH \
         (FORMAT csv, HEADER true, IGNORE_ERRORS = true, MAX_ERRORS = 1000, REJECT_TABLE = 'csv_rejects')",
        sql_string(csv_path.to_str().expect("utf8 path"))
    );
    client.batch_execute(&copy_sql).await.expect("copy file");

    assert_eq!(select_count(&client, "copy_quarantine").await, 3);
    assert_eq!(select_count(&client, "csv_rejects").await, 2);

    let reject_rows = client
        .query(
            "SELECT filename, line_number, raw_row, error FROM csv_rejects ORDER BY line_number",
            &[],
        )
        .await
        .expect("select rejects");
    assert_eq!(
        reject_rows[0].get::<_, String>(0),
        csv_path.display().to_string()
    );
    assert_eq!(reject_rows[0].get::<_, i64>(1), 3);
    assert_eq!(reject_rows[0].get::<_, String>(2), "bad,broken\n");
    assert!(
        reject_rows[0].get::<_, String>(3).contains("invalid digit"),
        "{:?}",
        reject_rows[0].get::<_, String>(3)
    );
    assert_eq!(reject_rows[1].get::<_, i64>(1), 5);
    assert_eq!(reject_rows[1].get::<_, String>(2), "3,too,many\n");
    assert!(
        reject_rows[1]
            .get::<_, String>(3)
            .contains("expected 2 columns, got 3"),
        "{:?}",
        reject_rows[1].get::<_, String>(3)
    );

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn copy_from_file_csv_stops_after_max_errors() {
    let dir = tempfile::tempdir().expect("tempdir");
    let csv_path = dir.path().join("too_many_bad_rows.csv");
    std::fs::write(&csv_path, "id,label\nbad,first\nalso_bad,second\n").expect("write csv");

    let (client, _conn_handle, server_handle) = start_server_and_connect().await;
    client
        .batch_execute("CREATE TABLE copy_quarantine_limit (id INT, label TEXT)")
        .await
        .expect("create target");
    client
        .batch_execute(
            "CREATE TABLE csv_rejects_limit (
                filename TEXT,
                line_number BIGINT,
                raw_row TEXT,
                error TEXT
            )",
        )
        .await
        .expect("create rejects");

    let copy_sql = format!(
        "COPY copy_quarantine_limit FROM {} WITH \
         (FORMAT csv, HEADER true, IGNORE_ERRORS = true, MAX_ERRORS = 1, REJECT_TABLE = 'csv_rejects_limit')",
        sql_string(csv_path.to_str().expect("utf8 path"))
    );
    let err = client
        .batch_execute(&copy_sql)
        .await
        .expect_err("copy exceeds max_errors");
    let message = err
        .as_db_error()
        .map(|db| db.message().to_string())
        .unwrap_or_else(|| err.to_string());
    assert!(message.contains("COPY max_errors exceeded"), "{message}");
    assert_eq!(select_count(&client, "copy_quarantine_limit").await, 0);
    assert_eq!(select_count(&client, "csv_rejects_limit").await, 0);

    shutdown(client, server_handle).await;
}

/// `COPY t FROM STDIN` handles typed Date and Decimal payloads without
/// leaking their physical int storage representation back to clients.
#[tokio::test]
async fn copy_from_stdin_text_lands_date_and_decimal() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE copy_typed (id INT, d DATE, amount DECIMAL(15,2))")
        .await
        .expect("create table");

    let payload = b"1\t1994-01-01\t123.45\n2\t2000-02-29\t-0.50\n".to_vec();
    let rows_inserted = copy_in_payload(&client, "COPY copy_typed FROM STDIN", &payload).await;
    assert_eq!(rows_inserted, 2);

    let stream = client
        .copy_out("COPY copy_typed TO STDOUT")
        .await
        .expect("copy_out");
    let echoed = collect_copy_out(stream).await;
    assert_eq!(echoed, payload);

    shutdown(client, server_handle).await;
}
