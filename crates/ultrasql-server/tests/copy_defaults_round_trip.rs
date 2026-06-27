//! End-to-end `COPY FROM` tests for two PostgreSQL-parity fixes:
//!
//! 1. Text-format NULL marker is decided on the RAW field token (before
//!    backslash de-escaping): raw `\N` is SQL NULL, raw `\\N` is the literal
//!    2-character string `\N`, and an empty field is the empty string. A
//!    round-trip of the literal `\N` through `COPY TO` / `COPY FROM` preserves
//!    it.
//! 2. `COPY t(col-list) FROM` fills columns the list omits from their column
//!    DEFAULT / `SERIAL` sequence / `GENERATED ... STORED` expression, exactly
//!    as `INSERT t(col-list)` does — rather than inserting NULL. An omitted
//!    NOT NULL column with a DEFAULT succeeds; an omitted NOT NULL column with
//!    no default still raises `23502`.

use bytes::Bytes;
use futures::SinkExt;

pub mod support;

use support::{shutdown, start_sample_server};

async fn copy_in_payload(client: &tokio_postgres::Client, sql: &str, payload: &[u8]) -> u64 {
    copy_in_payload_result(client, sql, payload)
        .await
        .expect("finish copy_in")
}

async fn copy_in_payload_result(
    client: &tokio_postgres::Client,
    sql: &str,
    payload: &[u8],
) -> Result<u64, tokio_postgres::Error> {
    let sink = client
        .copy_in::<_, Bytes>(sql)
        .await
        .expect("copy_in establishes COPY FROM STDIN");
    futures::pin_mut!(sink);
    sink.as_mut()
        .send(Bytes::from(payload.to_vec()))
        .await
        .expect("send CopyData");
    sink.finish().await
}

/// Build a one-column (`int4`) binary COPY payload carrying `value`.
fn binary_int4_copy_payload(value: i32) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(b"PGCOPY\n\xff\r\n\0");
    out.extend_from_slice(&0_i32.to_be_bytes()); // flags
    out.extend_from_slice(&0_i32.to_be_bytes()); // header extension length
    out.extend_from_slice(&1_i16.to_be_bytes()); // field count
    out.extend_from_slice(&4_i32.to_be_bytes()); // field length
    out.extend_from_slice(&value.to_be_bytes());
    out.extend_from_slice(&(-1_i16).to_be_bytes()); // trailer
    out
}

/// Read a single row's text columns via `simple_query`. Returns `None` for a
/// SQL NULL column, `Some(text)` otherwise.
async fn select_one_row(client: &tokio_postgres::Client, sql: &str) -> Vec<Option<String>> {
    let messages = client.simple_query(sql).await.expect("simple_query");
    for m in messages {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = m {
            return (0..row.len())
                .map(|i| row.get(i).map(str::to_owned))
                .collect();
        }
    }
    panic!("query returned no row: {sql}");
}

// ── BUG 1: text NULL marker decided on the raw field ────────────────────────

#[tokio::test]
async fn copy_text_null_marker_is_decided_on_raw_field() {
    let running = start_sample_server("copy_defaults").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE copy_nullmarker (id INT, t TEXT)")
        .await
        .expect("create table");

    // Line 1: raw `\N`  -> SQL NULL.
    // Line 2: raw `\\N` -> literal 2-char string `\N`.
    // Line 3: empty raw field -> empty string (not NULL).
    let payload = b"1\t\\N\n2\t\\\\N\n3\t\n".to_vec();
    let n = copy_in_payload(client, "COPY copy_nullmarker FROM STDIN", &payload).await;
    assert_eq!(n, 3);

    let row1 = select_one_row(client, "SELECT t FROM copy_nullmarker WHERE id = 1").await;
    assert_eq!(row1, vec![None], "raw \\N must be SQL NULL");

    let row2 = select_one_row(client, "SELECT t FROM copy_nullmarker WHERE id = 2").await;
    assert_eq!(
        row2,
        vec![Some("\\N".to_owned())],
        "raw \\\\N must be the literal string \\N (2 chars), not NULL"
    );

    let row3 = select_one_row(client, "SELECT t FROM copy_nullmarker WHERE id = 3").await;
    assert_eq!(
        row3,
        vec![Some(String::new())],
        "empty text field must be the empty string, not NULL"
    );

    shutdown(running).await;
}

#[tokio::test]
async fn copy_text_literal_backslash_n_round_trips() {
    let running = start_sample_server("copy_defaults").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE copy_rt (t TEXT)")
        .await
        .expect("create table");
    // Land the literal 2-char string `\N` via COPY FROM (raw `\\N` de-escapes
    // to `\N`), so the round-trip does not depend on E-string parsing.
    copy_in_payload(client, "COPY copy_rt FROM STDIN", b"\\\\N\n").await;
    let stored = select_one_row(client, "SELECT t, length(t) FROM copy_rt").await;
    assert_eq!(
        stored,
        vec![Some("\\N".to_owned()), Some("2".to_owned())],
        "stored value must be the literal \\N (2 chars)"
    );

    // COPY TO STDOUT must emit `\\N` for the literal `\N`.
    use futures::StreamExt;
    let stream = client
        .copy_out("COPY copy_rt TO STDOUT")
        .await
        .expect("copy_out");
    let mut stream = Box::pin(stream);
    let mut out = Vec::new();
    while let Some(chunk) = stream.next().await {
        out.extend_from_slice(&chunk.expect("CopyData chunk"));
    }
    assert_eq!(
        out, b"\\\\N\n",
        "COPY TO must emit the literal \\N as escaped \\\\N"
    );

    // Re-import the emitted bytes into a fresh table: the value comes back as
    // the literal `\N`, not NULL.
    client
        .batch_execute("CREATE TABLE copy_rt2 (t TEXT)")
        .await
        .expect("create table 2");
    copy_in_payload(client, "COPY copy_rt2 FROM STDIN", &out).await;
    let row = select_one_row(client, "SELECT t FROM copy_rt2").await;
    assert_eq!(
        row,
        vec![Some("\\N".to_owned())],
        "round-trip of literal \\N must preserve it, not collapse to NULL"
    );

    shutdown(running).await;
}

// ── BUG 2: COPY t(col-list) applies omitted-column DEFAULTs ──────────────────

#[tokio::test]
async fn copy_column_list_applies_plain_default() {
    let running = start_sample_server("copy_defaults").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE copy_def1 (a INT, b INT DEFAULT 99)")
        .await
        .expect("create table");

    let n = copy_in_payload(client, "COPY copy_def1 (a) FROM STDIN", b"1\n").await;
    assert_eq!(n, 1);

    let row = select_one_row(client, "SELECT a, b FROM copy_def1").await;
    assert_eq!(
        row,
        vec![Some("1".to_owned()), Some("99".to_owned())],
        "omitted column must take its DEFAULT (PG: 1, 99)"
    );

    shutdown(running).await;
}

#[tokio::test]
async fn copy_column_list_applies_not_null_default() {
    let running = start_sample_server("copy_defaults").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE copy_def2 (a INT, b INT NOT NULL DEFAULT 99)")
        .await
        .expect("create table");

    // An omitted NOT NULL column WITH a default must succeed (PG: 1, 99).
    let n = copy_in_payload(client, "COPY copy_def2 (a) FROM STDIN", b"1\n").await;
    assert_eq!(n, 1);

    let row = select_one_row(client, "SELECT a, b FROM copy_def2").await;
    assert_eq!(
        row,
        vec![Some("1".to_owned()), Some("99".to_owned())],
        "omitted NOT NULL column with DEFAULT must not raise 23502"
    );

    shutdown(running).await;
}

#[tokio::test]
async fn copy_column_list_evaluates_generated_stored() {
    let running = start_sample_server("copy_defaults").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE copy_def3 (a INT, g INT GENERATED ALWAYS AS (a * 2) STORED)")
        .await
        .expect("create table");

    let n = copy_in_payload(client, "COPY copy_def3 (a) FROM STDIN", b"5\n").await;
    assert_eq!(n, 1);

    let row = select_one_row(client, "SELECT a, g FROM copy_def3").await;
    assert_eq!(
        row,
        vec![Some("5".to_owned()), Some("10".to_owned())],
        "generated stored column must be evaluated (PG: 5, 10)"
    );

    shutdown(running).await;
}

#[tokio::test]
async fn copy_column_list_advances_serial_sequence() {
    let running = start_sample_server("copy_defaults").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE copy_def6 (id SERIAL, a INT)")
        .await
        .expect("create table");

    let n = copy_in_payload(client, "COPY copy_def6 (a) FROM STDIN", b"100\n200\n").await;
    assert_eq!(n, 2);

    let first = select_one_row(client, "SELECT id, a FROM copy_def6 WHERE a = 100").await;
    assert_eq!(first, vec![Some("1".to_owned()), Some("100".to_owned())]);
    let second = select_one_row(client, "SELECT id, a FROM copy_def6 WHERE a = 200").await;
    assert_eq!(second, vec![Some("2".to_owned()), Some("200".to_owned())]);

    shutdown(running).await;
}

#[tokio::test]
async fn copy_column_list_omitted_no_default_becomes_null() {
    let running = start_sample_server("copy_defaults").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE copy_def4 (a INT, b INT)")
        .await
        .expect("create table");

    let n = copy_in_payload(client, "COPY copy_def4 (a) FROM STDIN", b"7\n").await;
    assert_eq!(n, 1);

    let row = select_one_row(client, "SELECT a, b FROM copy_def4").await;
    assert_eq!(
        row,
        vec![Some("7".to_owned()), None],
        "omitted column with no default must be NULL"
    );

    shutdown(running).await;
}

#[tokio::test]
async fn copy_column_list_omitted_not_null_no_default_raises_23502() {
    let running = start_sample_server("copy_defaults").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE copy_def5 (a INT, b INT NOT NULL)")
        .await
        .expect("create table");

    let err = copy_in_payload_result(client, "COPY copy_def5 (a) FROM STDIN", b"7\n")
        .await
        .expect_err("omitted NOT NULL column with no default must fail");
    let db_error = err.as_db_error().expect("server returns db error");
    assert_eq!(
        db_error.code().code(),
        "23502",
        "must raise not_null_violation: {db_error}"
    );

    shutdown(running).await;
}

#[tokio::test]
async fn copy_column_list_applies_default_with_index_and_skipped_middle_column() {
    let running = start_sample_server("copy_defaults").await;
    let client = &running.client;

    // PRIMARY KEY forces the maintained insert path; the column list (a, c)
    // skips the DEFAULTed middle column b. Both rows must land with b = 99 and
    // the unique index kept current.
    client
        .batch_execute("CREATE TABLE copy_def_idx (a INT PRIMARY KEY, b INT DEFAULT 99, c INT)")
        .await
        .expect("create table");

    let n = copy_in_payload(
        client,
        "COPY copy_def_idx (a, c) FROM STDIN",
        b"1\t7\n2\t8\n",
    )
    .await;
    assert_eq!(n, 2);

    let first = select_one_row(client, "SELECT a, b, c FROM copy_def_idx WHERE a = 1").await;
    assert_eq!(
        first,
        vec![
            Some("1".to_owned()),
            Some("99".to_owned()),
            Some("7".to_owned())
        ]
    );
    let second = select_one_row(client, "SELECT a, b, c FROM copy_def_idx WHERE a = 2").await;
    assert_eq!(
        second,
        vec![
            Some("2".to_owned()),
            Some("99".to_owned()),
            Some("8".to_owned())
        ]
    );

    // The unique index must reject a duplicate key inserted afterwards.
    let dup = client
        .batch_execute("INSERT INTO copy_def_idx (a, c) VALUES (1, 0)")
        .await
        .expect_err("duplicate PK must be rejected, proving the index is maintained");
    assert_eq!(dup.as_db_error().expect("db error").code().code(), "23505");

    shutdown(running).await;
}

#[tokio::test]
async fn copy_binary_column_list_applies_default() {
    let running = start_sample_server("copy_defaults").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE copy_def_bin (a INT, b INT DEFAULT 99)")
        .await
        .expect("create table");

    let payload = binary_int4_copy_payload(1);
    let n = copy_in_payload(
        client,
        "COPY copy_def_bin (a) FROM STDIN WITH (FORMAT binary)",
        &payload,
    )
    .await;
    assert_eq!(n, 1);

    let row = select_one_row(client, "SELECT a, b FROM copy_def_bin").await;
    assert_eq!(
        row,
        vec![Some("1".to_owned()), Some("99".to_owned())],
        "binary COPY column-list must apply the omitted column DEFAULT (PG: 1, 99)"
    );

    shutdown(running).await;
}

#[tokio::test]
async fn copy_full_column_list_does_not_apply_defaults() {
    let running = start_sample_server("copy_defaults").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE copy_def_full (a INT, b INT DEFAULT 99)")
        .await
        .expect("create table");

    // Naming every column (here, by listing both) provides b explicitly, so the
    // DEFAULT must NOT override the streamed value.
    let n = copy_in_payload(client, "COPY copy_def_full (a, b) FROM STDIN", b"1\t7\n").await;
    assert_eq!(n, 1);

    let row = select_one_row(client, "SELECT a, b FROM copy_def_full").await;
    assert_eq!(
        row,
        vec![Some("1".to_owned()), Some("7".to_owned())],
        "explicitly provided column must keep its streamed value, not the DEFAULT"
    );

    shutdown(running).await;
}

#[tokio::test]
async fn copy_no_column_list_unchanged() {
    let running = start_sample_server("copy_defaults").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE copy_def_none (a INT, b INT DEFAULT 99)")
        .await
        .expect("create table");

    // No column list: the stream provides every column. A NULL marker still
    // lands a real NULL (DEFAULT only fills columns the list omits).
    let n = copy_in_payload(client, "COPY copy_def_none FROM STDIN", b"1\t\\N\n").await;
    assert_eq!(n, 1);

    let row = select_one_row(client, "SELECT a, b FROM copy_def_none").await;
    assert_eq!(
        row,
        vec![Some("1".to_owned()), None],
        "full-width COPY must honour an explicit NULL, not substitute the DEFAULT"
    );

    shutdown(running).await;
}
