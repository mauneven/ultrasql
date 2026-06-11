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

use std::sync::Arc;

use arrow_array::{BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use bytes::Bytes;
use futures::SinkExt;
use parquet::arrow::ArrowWriter;

pub mod support;

use support::{shutdown, start_sample_server};

fn sql_string(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

fn write_copy_parquet(path: &std::path::Path, rows: &[(i64, &str, f64, bool)]) {
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int64, false),
        ArrowField::new("label", ArrowDataType::Utf8, false),
        ArrowField::new("score", ArrowDataType::Float64, false),
        ArrowField::new("active", ArrowDataType::Boolean, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|(id, _, _, _)| *id).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|(_, label, _, _)| *label)
                    .collect::<Vec<&str>>(),
            )),
            Arc::new(Float64Array::from(
                rows.iter()
                    .map(|(_, _, score, _)| *score)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(BooleanArray::from(
                rows.iter()
                    .map(|(_, _, _, active)| *active)
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("parquet record batch");
    let file = std::fs::File::create(path).expect("create parquet");
    let mut writer = ArrowWriter::try_new(file, schema, None).expect("parquet writer");
    writer.write(&batch).expect("write parquet batch");
    writer.close().expect("close parquet writer");
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
    sink.finish().await
}

fn pg_binary_copy_header(out: &mut Vec<u8>) {
    out.extend_from_slice(b"PGCOPY\n\xff\r\n\0");
    out.extend_from_slice(&0_i32.to_be_bytes());
    out.extend_from_slice(&0_i32.to_be_bytes());
}

fn pg_binary_copy_i16(out: &mut Vec<u8>, value: i16) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn pg_binary_copy_field(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(
        &i32::try_from(bytes.len())
            .expect("test binary COPY field fits i32")
            .to_be_bytes(),
    );
    out.extend_from_slice(bytes);
}

fn binary_jsonb_copy_payload() -> Vec<u8> {
    let mut out = Vec::new();
    pg_binary_copy_header(&mut out);
    pg_binary_copy_i16(&mut out, 2);
    pg_binary_copy_field(&mut out, &1_i32.to_be_bytes());
    let mut jsonb = vec![1_u8];
    jsonb.extend_from_slice(br#"{"b":"x","a":1}"#);
    pg_binary_copy_field(&mut out, &jsonb);
    pg_binary_copy_i16(&mut out, -1);
    out
}

fn pg_numeric_12_340() -> Vec<u8> {
    vec![
        0x00, 0x02, // ndigits
        0x00, 0x00, // weight
        0x00, 0x00, // sign
        0x00, 0x03, // dscale
        0x00, 0x0c, // 12
        0x0d, 0x48, // 3400
    ]
}

fn binary_numeric_copy_payload() -> Vec<u8> {
    let mut out = Vec::new();
    pg_binary_copy_header(&mut out);
    pg_binary_copy_i16(&mut out, 2);
    pg_binary_copy_field(&mut out, &1_i32.to_be_bytes());
    pg_binary_copy_field(&mut out, &pg_numeric_12_340());
    pg_binary_copy_i16(&mut out, -1);
    out
}

fn binary_money_copy_payload() -> Vec<u8> {
    let mut out = Vec::new();
    pg_binary_copy_header(&mut out);
    pg_binary_copy_i16(&mut out, 2);
    pg_binary_copy_field(&mut out, &1_i32.to_be_bytes());
    pg_binary_copy_field(&mut out, &123_456_i64.to_be_bytes());
    pg_binary_copy_i16(&mut out, -1);
    out
}

fn first_binary_copy_jsonb_field(bytes: &[u8]) -> &[u8] {
    second_binary_copy_field(bytes)
}

fn second_binary_copy_field(bytes: &[u8]) -> &[u8] {
    let magic = b"PGCOPY\n\xff\r\n\0";
    let mut pos = magic.len() + 8;
    let field_count = i16::from_be_bytes([bytes[pos], bytes[pos + 1]]);
    assert_eq!(field_count, 2);
    pos += 2;
    let id_len = i32::from_be_bytes([bytes[pos], bytes[pos + 1], bytes[pos + 2], bytes[pos + 3]]);
    pos += 4 + usize::try_from(id_len).expect("id field length");
    let jsonb_len =
        i32::from_be_bytes([bytes[pos], bytes[pos + 1], bytes[pos + 2], bytes[pos + 3]]);
    pos += 4;
    let len = usize::try_from(jsonb_len).expect("jsonb field length");
    &bytes[pos..pos + len]
}

/// `COPY t FROM STDIN` over a populated relation lands every row.
#[tokio::test]
async fn copy_from_stdin_text_lands_rows() {
    let running = start_sample_server("copy_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE copy_from_text (id INT, label TEXT)")
        .await
        .expect("create table");

    let payload = b"1\talice\n2\tbob\n3\tcarol\n".to_vec();
    let rows_inserted = copy_in_payload(client, "COPY copy_from_text FROM STDIN", &payload).await;
    assert_eq!(rows_inserted, 3);

    let n = select_count(client, "copy_from_text").await;
    assert_eq!(n, 3, "COPY FROM STDIN must land every row");

    shutdown(running).await;
}

#[tokio::test]
async fn copy_from_stdin_jsonb_rejects_invalid_json_text() {
    let running = start_sample_server("copy_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE copy_jsonb_invalid (id INT, doc JSONB)")
        .await
        .expect("create table");

    let err = copy_in_payload_result(
        client,
        "COPY copy_jsonb_invalid FROM STDIN",
        b"1\t{not json}\n",
    )
    .await
    .expect_err("invalid JSONB COPY row is rejected");
    let db_error = err.as_db_error().expect("server returns db error");
    assert!(
        db_error.message().contains("invalid jsonb"),
        "unexpected error: {db_error}"
    );
    assert_eq!(select_count(client, "copy_jsonb_invalid").await, 0);

    shutdown(running).await;
}

#[tokio::test]
async fn copy_from_stdin_binary_jsonb_uses_pg_versioned_payload() {
    let running = start_sample_server("copy_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE copy_binary_jsonb (id INT, doc JSONB)")
        .await
        .expect("create table");

    let rows_inserted = copy_in_payload(
        client,
        "COPY copy_binary_jsonb FROM STDIN WITH (FORMAT binary)",
        &binary_jsonb_copy_payload(),
    )
    .await;
    assert_eq!(rows_inserted, 1);

    let selected = client
        .simple_query("SELECT id, doc FROM copy_binary_jsonb ORDER BY id")
        .await
        .expect("select copied jsonb");
    let row = selected
        .into_iter()
        .find_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .expect("selected row");
    assert_eq!(row.get(0), Some("1"));
    assert_eq!(row.get(1), Some(r#"{"a":1,"b":"x"}"#));

    let stream = client
        .copy_out("COPY copy_binary_jsonb TO STDOUT WITH (FORMAT binary)")
        .await
        .expect("binary copy out");
    let copied_out = collect_copy_out(stream).await;
    let jsonb_field = first_binary_copy_jsonb_field(&copied_out);
    assert_eq!(jsonb_field.first(), Some(&1_u8));
    assert_eq!(&jsonb_field[1..], br#"{"a":1,"b":"x"}"#);

    shutdown(running).await;
}

#[tokio::test]
async fn copy_from_stdin_binary_numeric_uses_pg_numeric_payload() {
    let running = start_sample_server("copy_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE copy_binary_numeric (id INT, amount NUMERIC(12,3))")
        .await
        .expect("create table");

    let rows_inserted = copy_in_payload(
        client,
        "COPY copy_binary_numeric FROM STDIN WITH (FORMAT binary)",
        &binary_numeric_copy_payload(),
    )
    .await;
    assert_eq!(rows_inserted, 1);

    let selected = client
        .simple_query("SELECT amount FROM copy_binary_numeric")
        .await
        .expect("select copied numeric");
    let row = selected
        .into_iter()
        .find_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .expect("selected row");
    assert_eq!(row.get(0), Some("12.340"));

    let stream = client
        .copy_out("COPY copy_binary_numeric TO STDOUT WITH (FORMAT binary)")
        .await
        .expect("binary copy out");
    let copied_out = collect_copy_out(stream).await;
    assert_eq!(second_binary_copy_field(&copied_out), pg_numeric_12_340());

    shutdown(running).await;
}

#[tokio::test]
async fn copy_from_stdin_binary_money_uses_pg_cash_payload() {
    let running = start_sample_server("copy_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE copy_binary_money (id INT, amount MONEY)")
        .await
        .expect("create table");

    let rows_inserted = copy_in_payload(
        client,
        "COPY copy_binary_money FROM STDIN WITH (FORMAT binary)",
        &binary_money_copy_payload(),
    )
    .await;
    assert_eq!(rows_inserted, 1);

    let rows = client
        .simple_query("SELECT amount FROM copy_binary_money")
        .await
        .expect("select money");
    let amount = rows.into_iter().find_map(|message| match message {
        tokio_postgres::SimpleQueryMessage::Row(row) => row.get("amount").map(str::to_owned),
        _ => None,
    });
    assert_eq!(amount.as_deref(), Some("$1,234.56"));

    let stream = client
        .copy_out("COPY copy_binary_money TO STDOUT WITH (FORMAT binary)")
        .await
        .expect("binary copy out");
    let copied_out = collect_copy_out(stream).await;
    assert_eq!(
        second_binary_copy_field(&copied_out),
        123_456_i64.to_be_bytes()
    );

    shutdown(running).await;
}

/// `COPY t TO STDOUT` emits the rows it sees in heap order.
#[tokio::test]
async fn copy_to_stdout_text_emits_rows() {
    let running = start_sample_server("copy_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE copy_to_text (id INT, label TEXT)")
        .await
        .expect("create table");
    let payload = b"10\thello\n20\tworld\n".to_vec();
    copy_in_payload(client, "COPY copy_to_text FROM STDIN", &payload).await;

    let stream = client
        .copy_out("COPY copy_to_text TO STDOUT")
        .await
        .expect("copy_out");
    let bytes = collect_copy_out(stream).await;
    assert_eq!(bytes, payload, "COPY TO STDOUT byte-equality");

    shutdown(running).await;
}

#[tokio::test]
async fn copy_respects_schema_qualifier() {
    let running = start_sample_server("copy_schema_qualifier_guard").await;
    let client = &running.client;

    client
        .batch_execute(
            "CREATE SCHEMA app; \
             CREATE TABLE guarded_copy (id INT, label TEXT); \
             INSERT INTO guarded_copy VALUES (8, 'public')",
        )
        .await
        .expect("create public table and separate schema");

    let copy_out = client.copy_out("COPY app.guarded_copy TO STDOUT").await;
    assert!(
        copy_out.is_err(),
        "qualified COPY TO must not resolve public table"
    );

    let copy_in = client
        .copy_in::<_, Bytes>("COPY app.guarded_copy FROM STDIN")
        .await;
    assert!(
        copy_in.is_err(),
        "qualified COPY FROM must not resolve public table"
    );

    assert_eq!(select_count(client, "guarded_copy").await, 1);

    client
        .batch_execute("DROP TABLE guarded_copy; DROP SCHEMA app")
        .await
        .expect("cleanup COPY qualifier guard");

    shutdown(running).await;
}

#[tokio::test]
async fn copy_from_records_modifications_under_qualified_table_key() {
    let running = start_sample_server("copy_schema_runtime_key").await;
    let client = &running.client;

    client
        .batch_execute(
            "CREATE SCHEMA app; \
             CREATE TABLE copy_mods (id INT, label TEXT); \
             CREATE TABLE app.copy_mods (id INT, label TEXT)",
        )
        .await
        .expect("create same-named COPY targets");

    let rows_inserted =
        copy_in_payload(client, "COPY app.copy_mods FROM STDIN", b"1\tapp\n2\tapp\n").await;
    assert_eq!(rows_inserted, 2);
    assert_eq!(select_count(client, "copy_mods").await, 0);
    assert_eq!(select_count(client, "app.copy_mods").await, 2);

    assert!(
        !running.server.table_modifications.contains_key("copy_mods"),
        "qualified COPY must not dirty public same-name table"
    );
    assert_eq!(
        running
            .server
            .table_modifications
            .get("app.copy_mods")
            .map(|entry| *entry),
        Some(2),
        "qualified COPY must dirty app table key"
    );

    shutdown(running).await;
}

/// The exact bytes pushed through `COPY FROM STDIN` come back through
/// `COPY TO STDOUT`. This is the integration "byte-equality of the
/// round-tripped text payload" property the workplan asks for.
#[tokio::test]
async fn copy_round_trip_text_is_byte_identical() {
    let running = start_sample_server("copy_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE copy_round_trip (id INT, label TEXT)")
        .await
        .expect("create table");

    let payload = b"1\talice\n2\tbob\n3\tcarol\n4\tdan\n5\teve\n".to_vec();
    copy_in_payload(client, "COPY copy_round_trip FROM STDIN", &payload).await;

    let stream = client
        .copy_out("COPY copy_round_trip TO STDOUT")
        .await
        .expect("copy_out");
    let echoed = collect_copy_out(stream).await;
    assert_eq!(
        echoed, payload,
        "every byte fed into COPY FROM STDIN must re-emerge from COPY TO STDOUT"
    );

    shutdown(running).await;
}

/// Low-cardinality text columns cross the automatic dictionary
/// threshold, so this exercises dictionary-backed heap decode plus
/// wire/COPY output decoding.
#[tokio::test]
async fn copy_round_trip_low_cardinality_text_stays_wire_correct() {
    let running = start_sample_server("copy_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE copy_dict_text (id INT, label TEXT)")
        .await
        .expect("create table");

    let mut payload = Vec::new();
    for i in 0..2048 {
        let line = format!("{i}\tlabel{}\n", i % 4);
        payload.extend_from_slice(line.as_bytes());
    }
    copy_in_payload(client, "COPY copy_dict_text FROM STDIN", &payload).await;

    let stream = client
        .copy_out("COPY copy_dict_text TO STDOUT")
        .await
        .expect("copy_out");
    let echoed = collect_copy_out(stream).await;
    assert_eq!(echoed, payload);

    shutdown(running).await;
}

/// `COPY t FROM STDIN WITH (FORMAT CSV)` ingests CSV rows correctly.
#[tokio::test]
async fn copy_from_stdin_csv_lands_rows() {
    let running = start_sample_server("copy_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE copy_csv (id INT, label TEXT)")
        .await
        .expect("create table");

    let payload = b"1,alice\n2,\"bob, jr\"\n3,carol\n".to_vec();
    let rows_inserted = copy_in_payload(
        client,
        "COPY copy_csv FROM STDIN WITH (FORMAT CSV)",
        &payload,
    )
    .await;
    assert_eq!(rows_inserted, 3);

    let n = select_count(client, "copy_csv").await;
    assert_eq!(n, 3);

    shutdown(running).await;
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

    let running = start_sample_server("copy_test").await;
    let client = &running.client;
    client
        .batch_execute("CREATE TABLE copy_file_auto (id INT, label TEXT, note TEXT)")
        .await
        .expect("create table");

    let copy_sql = format!(
        "COPY copy_file_auto FROM {} WITH (FORMAT csv, HEADER true, AUTO_DETECT true)",
        sql_string(csv_path.to_str().expect("utf8 path"))
    );
    client.batch_execute(&copy_sql).await.expect("copy file");

    let n = select_count(client, "copy_file_auto").await;
    assert_eq!(n, 4101);

    let rows = client
        .query("SELECT label, note FROM copy_file_auto WHERE id = 1", &[])
        .await
        .expect("select copied row");
    assert_eq!(rows[0].get::<_, String>(0), "alpha");
    assert_eq!(rows[0].get::<_, String>(1), "hello\nworld");

    shutdown(running).await;
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

    let running = start_sample_server("copy_test").await;
    let client = &running.client;
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

    assert_eq!(select_count(client, "copy_quarantine").await, 3);
    assert_eq!(select_count(client, "csv_rejects").await, 2);

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

    shutdown(running).await;
}

#[tokio::test]
async fn copy_reject_table_follows_search_path() {
    let dir = tempfile::tempdir().expect("tempdir");
    let csv_path = dir.path().join("schema_bad_rows.csv");
    std::fs::write(&csv_path, "id,label\nbad,app\n1,ok\n").expect("write csv");

    let running = start_sample_server("copy_reject_schema").await;
    let client = &running.client;
    client
        .batch_execute(
            "CREATE SCHEMA app; \
             CREATE TABLE app.copy_quarantine (id INT, label TEXT); \
             CREATE TABLE app.csv_rejects (
                 filename TEXT,
                 line_number BIGINT,
                 raw_row TEXT,
                 error TEXT
             ); \
             CREATE TABLE csv_rejects (
                 filename TEXT,
                 line_number BIGINT,
                 raw_row TEXT,
                 error TEXT
             ); \
             SET search_path TO app, public",
        )
        .await
        .expect("create schema-scoped copy tables");

    let copy_sql = format!(
        "COPY copy_quarantine FROM {} WITH \
         (FORMAT csv, HEADER true, IGNORE_ERRORS = true, MAX_ERRORS = 1000, REJECT_TABLE = 'csv_rejects')",
        sql_string(csv_path.to_str().expect("utf8 path"))
    );
    client.batch_execute(&copy_sql).await.expect("copy file");

    assert_eq!(select_count(client, "app.copy_quarantine").await, 1);
    assert_eq!(select_count(client, "app.csv_rejects").await, 1);
    assert_eq!(select_count(client, "public.csv_rejects").await, 0);

    shutdown(running).await;
}

#[tokio::test]
async fn copy_from_file_csv_stops_after_max_errors() {
    let dir = tempfile::tempdir().expect("tempdir");
    let csv_path = dir.path().join("too_many_bad_rows.csv");
    std::fs::write(&csv_path, "id,label\nbad,first\nalso_bad,second\n").expect("write csv");

    let running = start_sample_server("copy_test").await;
    let client = &running.client;
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
    assert_eq!(select_count(client, "copy_quarantine_limit").await, 0);
    assert_eq!(select_count(client, "csv_rejects_limit").await, 0);

    shutdown(running).await;
}

#[tokio::test]
async fn copy_table_to_parquet_exports_queryable_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    let parquet_path = dir.path().join("export.parquet");

    let running = start_sample_server("copy_test").await;
    let client = &running.client;
    client
        .batch_execute(
            "CREATE TABLE copy_to_parquet (
                id BIGINT,
                label TEXT,
                score FLOAT8,
                active BOOLEAN
            )",
        )
        .await
        .expect("create parquet export table");
    client
        .batch_execute(
            "INSERT INTO copy_to_parquet VALUES
                (2, 'beta', 20.5, false),
                (1, 'alpha', 10.25, true)",
        )
        .await
        .expect("seed parquet export table");
    assert_eq!(select_count(client, "copy_to_parquet").await, 2);

    let copy_sql = format!(
        "COPY copy_to_parquet TO {}",
        sql_string(parquet_path.to_str().expect("utf8 parquet path"))
    );
    client
        .batch_execute(&copy_sql)
        .await
        .expect("copy to parquet");

    let read_sql = format!(
        "SELECT id, label, score, active FROM read_parquet({}) ORDER BY id",
        sql_string(parquet_path.to_str().expect("utf8 parquet path"))
    );
    let rows = client.query(&read_sql, &[]).await.expect("read export");
    let values: Vec<(i64, String, f64, bool)> = rows
        .iter()
        .map(|row| {
            (
                row.get::<_, i64>(0),
                row.get::<_, String>(1),
                row.get::<_, f64>(2),
                row.get::<_, bool>(3),
            )
        })
        .collect();
    assert_eq!(
        values,
        vec![
            (1, "alpha".to_owned(), 10.25, true),
            (2, "beta".to_owned(), 20.5, false),
        ]
    );

    shutdown(running).await;
}

#[tokio::test]
async fn copy_table_from_parquet_imports_rows() {
    let dir = tempfile::tempdir().expect("tempdir");
    let parquet_path = dir.path().join("import.parquet");
    write_copy_parquet(
        &parquet_path,
        &[
            (3, "gamma", 30.75, true),
            (1, "alpha", 10.25, true),
            (2, "beta", 20.5, false),
        ],
    );

    let running = start_sample_server("copy_test").await;
    let client = &running.client;
    client
        .batch_execute(
            "CREATE TABLE copy_from_parquet (
                id BIGINT,
                label TEXT,
                score FLOAT8,
                active BOOLEAN
            )",
        )
        .await
        .expect("create parquet import table");

    let copy_sql = format!(
        "COPY copy_from_parquet FROM {}",
        sql_string(parquet_path.to_str().expect("utf8 parquet path"))
    );
    client
        .batch_execute(&copy_sql)
        .await
        .expect("copy from parquet");

    let rows = client
        .query(
            "SELECT id, label, score, active FROM copy_from_parquet ORDER BY id",
            &[],
        )
        .await
        .expect("select parquet import");
    let values: Vec<(i64, String, f64, bool)> = rows
        .iter()
        .map(|row| {
            (
                row.get::<_, i64>(0),
                row.get::<_, String>(1),
                row.get::<_, f64>(2),
                row.get::<_, bool>(3),
            )
        })
        .collect();
    assert_eq!(
        values,
        vec![
            (1, "alpha".to_owned(), 10.25, true),
            (2, "beta".to_owned(), 20.5, false),
            (3, "gamma".to_owned(), 30.75, true),
        ]
    );

    shutdown(running).await;
}

/// `COPY t FROM STDIN` handles typed Date and Decimal payloads without
/// leaking their physical int storage representation back to clients.
#[tokio::test]
async fn copy_from_stdin_text_lands_date_and_decimal() {
    let running = start_sample_server("copy_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE copy_typed (id INT, d DATE, amount DECIMAL(15,2))")
        .await
        .expect("create table");

    let payload = b"1\t1994-01-01\t123.45\n2\t2000-02-29\t-0.50\n".to_vec();
    let rows_inserted = copy_in_payload(client, "COPY copy_typed FROM STDIN", &payload).await;
    assert_eq!(rows_inserted, 2);

    let stream = client
        .copy_out("COPY copy_typed TO STDOUT")
        .await
        .expect("copy_out");
    let echoed = collect_copy_out(stream).await;
    assert_eq!(echoed, payload);

    shutdown(running).await;
}

#[tokio::test]
async fn copy_from_stdin_text_rounds_numeric_to_declared_scale() {
    let running = start_sample_server("copy_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE copy_numeric_round (id INT, amount NUMERIC(8,2))")
        .await
        .expect("create table");

    let payload = b"1\t1.235\n2\t-1.235\n3\t1.2\n".to_vec();
    let rows_inserted =
        copy_in_payload(client, "COPY copy_numeric_round FROM STDIN", &payload).await;
    assert_eq!(rows_inserted, 3);

    let rows = client
        .simple_query("SELECT amount FROM copy_numeric_round ORDER BY id")
        .await
        .expect("select rounded numeric");
    let values: Vec<String> = rows
        .into_iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        })
        .collect();
    assert_eq!(
        values,
        vec!["1.24".to_owned(), "-1.24".to_owned(), "1.20".to_owned()]
    );

    shutdown(running).await;
}

#[tokio::test]
async fn copy_from_stdin_text_money_accepts_currency_format() {
    let running = start_sample_server("copy_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE copy_money_text (id INT, amount MONEY)")
        .await
        .expect("create table");

    let payload = b"1\t$1,234.56\n2\t-$1.23\n3\t12.345\n".to_vec();
    let rows_inserted = copy_in_payload(client, "COPY copy_money_text FROM STDIN", &payload).await;
    assert_eq!(rows_inserted, 3);

    let rows = client
        .simple_query("SELECT amount FROM copy_money_text ORDER BY id")
        .await
        .expect("select money");
    let values: Vec<String> = rows
        .into_iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        })
        .collect();
    assert_eq!(
        values,
        vec![
            "$1,234.56".to_owned(),
            "-$1.23".to_owned(),
            "$12.35".to_owned()
        ]
    );

    shutdown(running).await;
}

#[tokio::test]
async fn copy_to_text_money_uses_lc_monetary_template() {
    let running = start_sample_server("copy_test").await;
    let client = &running.client;
    let euro = "\u{20ac}";

    client
        .batch_execute("CREATE TABLE copy_money_locale (id INT, amount MONEY)")
        .await
        .expect("create table");
    client
        .batch_execute("INSERT INTO copy_money_locale VALUES (1, '$1,234.56'::money)")
        .await
        .expect("insert money");
    client
        .batch_execute("SET lc_monetary = 'de_DE.UTF-8'")
        .await
        .expect("set monetary locale");

    let stream = client
        .copy_out("COPY copy_money_locale TO STDOUT")
        .await
        .expect("copy_out");
    let copied = collect_copy_out(stream).await;

    assert_eq!(
        String::from_utf8(copied).expect("copy output utf8"),
        format!("1\t1.234,56 {euro}\n")
    );

    shutdown(running).await;
}
