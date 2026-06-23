//! End-to-end `EXPLAIN` / `EXPLAIN ANALYZE` / `EXPLAIN (FORMAT JSON)`
//! tests against a real `tokio-postgres` client.
//!
//! Closes the v0.5 wire-protocol gap "`EXPLAIN` / `EXPLAIN ANALYZE` —
//! no `LogicalPlan::Explain`, no session dispatch" (tracked in `TODO.md`).
//! The binder now lowers every `EXPLAIN` statement into
//! `LogicalPlan::Explain { analyze, format, input }`; the session
//! dispatcher renders the wrapped plan into the single-column
//! `"QUERY PLAN"` Text output.
//!
//! Shapes covered:
//!
//! - `EXPLAIN SELECT id FROM t WHERE id = 1` returns one or more text
//!   rows; the body mentions a plan-node label (`Filter`, `Project`,
//!   etc.). No actual execution.
//! - `EXPLAIN ANALYZE SELECT ...` returns the same plan plus an
//!   `Execution Time` row and the actual row count.
//! - `EXPLAIN (FORMAT JSON) SELECT ...` returns text that parses as
//!   JSON and contains the `Node Type` key.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use parquet::arrow::ArrowWriter;
use tokio_postgres::NoTls;
use ultrasql_server::{Server, bind_listener, serve_listener};

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
        "host={host} port={port} user=tester application_name=explain_test",
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

fn collect_plan_text(rows: &[tokio_postgres::Row]) -> String {
    rows.iter()
        .map(|r| r.get::<_, String>(0))
        .collect::<Vec<_>>()
        .join("\n")
}

fn sql_string(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

fn write_people_parquet(
    path: &std::path::Path,
    first_rows: &[(i64, &str, i64)],
    second_rows: &[(i64, &str, i64)],
) {
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int64, false),
        ArrowField::new("name", ArrowDataType::Utf8, false),
        ArrowField::new("score", ArrowDataType::Int64, false),
    ]));
    let file = std::fs::File::create(path).expect("create parquet");
    let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), None).expect("parquet writer");
    writer
        .write(&people_batch(Arc::clone(&schema), first_rows))
        .expect("write first parquet row group");
    writer.flush().expect("flush first row group");
    writer
        .write(&people_batch(schema, second_rows))
        .expect("write second parquet row group");
    writer.close().expect("close parquet");
}

fn people_batch(schema: Arc<ArrowSchema>, rows: &[(i64, &str, i64)]) -> RecordBatch {
    let ids = rows.iter().map(|(id, _, _)| *id).collect::<Vec<_>>();
    let names = rows.iter().map(|(_, name, _)| *name).collect::<Vec<_>>();
    let scores = rows.iter().map(|(_, _, score)| *score).collect::<Vec<_>>();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(Int64Array::from(scores)),
        ],
    )
    .expect("people record batch")
}

/// Plain `EXPLAIN SELECT` returns a plan tree as one or more text rows.
#[tokio::test]
async fn explain_select_returns_plan_text_rows() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL, v INT NOT NULL)")
        .await
        .expect("create");
    client
        .batch_execute("INSERT INTO t VALUES (1, 10), (2, 20)")
        .await
        .expect("seed");

    let rows = client
        .query("EXPLAIN SELECT id FROM t WHERE id = 1", &[])
        .await
        .expect("EXPLAIN");
    assert!(!rows.is_empty(), "EXPLAIN must return at least one row");
    let text = collect_plan_text(&rows);
    // The plan tree always names the relevant nodes; require at least
    // one of the canonical labels to appear.
    let has_node_label = ["Filter", "Project", "Scan"]
        .iter()
        .any(|kw| text.contains(*kw));
    assert!(
        has_node_label,
        "EXPLAIN text should contain a plan-node label, got: {text}"
    );

    shutdown(client, server_handle).await;
}

/// `EXPLAIN ANALYZE SELECT` executes the inner plan, then adds an
/// `Execution Time` annotation and an actual row count.
#[tokio::test]
async fn explain_analyze_executes_and_reports_actual_rows() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL, v INT NOT NULL)")
        .await
        .expect("create");
    client
        .batch_execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .expect("seed");

    let rows = client
        .query("EXPLAIN ANALYZE SELECT id FROM t", &[])
        .await
        .expect("EXPLAIN ANALYZE");
    let text = collect_plan_text(&rows);
    assert!(
        text.contains("Execution Time"),
        "EXPLAIN ANALYZE must report Execution Time, got: {text}"
    );
    assert!(
        text.contains("Actual Rows: 3"),
        "EXPLAIN ANALYZE must report actual row count, got: {text}"
    );

    shutdown(client, server_handle).await;
}

/// Serious-mode `EXPLAIN ANALYZE` reports the execution evidence needed
/// to debug performance work instead of only a root wall-clock count.
#[tokio::test]
async fn explain_analyze_reports_runtime_evidence() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL, v INT NOT NULL)")
        .await
        .expect("create");
    client
        .batch_execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .expect("seed");
    client
        .batch_execute("CREATE INDEX t_id_idx ON t (id)")
        .await
        .expect("create index");

    let rows = client
        .query("EXPLAIN ANALYZE SELECT id FROM t WHERE id = 2", &[])
        .await
        .expect("EXPLAIN ANALYZE");
    let text = collect_plan_text(&rows);

    for required in [
        "Actual Rows: 1",
        "Actual Batches:",
        "Peak Output Memory:",
        "Disk Spill:",
        "SIMD Kernel:",
        "Index Decision:",
        "selected t_id_idx",
        "Late Materialization:",
        "Pushdowns Applied:",
    ] {
        assert!(
            text.contains(required),
            "EXPLAIN ANALYZE missing {required:?}, got: {text}"
        );
    }

    shutdown(client, server_handle).await;
}

/// `EXPLAIN ANALYZE` exposes one metrics row per physical operator so
/// performance regressions have counters below the statement summary.
#[tokio::test]
async fn explain_analyze_reports_operator_metrics() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t_metrics (id INT NOT NULL, payload TEXT)")
        .await
        .expect("create");
    client
        .batch_execute(
            "INSERT INTO t_metrics VALUES
                (0, 'payload-0'),
                (1, 'payload-1'),
                (2, 'payload-2'),
                (3, 'payload-3'),
                (4, 'payload-4'),
                (5, 'payload-5'),
                (6, 'payload-6'),
                (7, 'payload-7')",
        )
        .await
        .expect("seed");

    let rows = client
        .query(
            "EXPLAIN ANALYZE SELECT id FROM t_metrics WHERE id > 1 ORDER BY id LIMIT 2",
            &[],
        )
        .await
        .expect("EXPLAIN ANALYZE");
    let text = collect_plan_text(&rows);

    for required in [
        "Operator Metrics:",
        "operator=Limit",
        "operator=Sort",
        "operator=Filter",
        "operator=Seq Scan",
        "rows_in=",
        "rows_out=",
        "batches=",
        "time_us=",
        "memory_bytes=",
        "spills=",
        "io_bytes=",
        "pruning=",
    ] {
        assert!(
            text.contains(required),
            "EXPLAIN ANALYZE missing {required:?}, got: {text}"
        );
    }

    shutdown(client, server_handle).await;
}

/// When an indexed predicate feeds a projection that needs non-index
/// payload columns, `EXPLAIN ANALYZE` reports the late-materialization
/// prototype: B-tree TID probe first, heap payload fetch after candidate
/// pruning.
#[tokio::test]
async fn explain_analyze_reports_late_materialization_selection() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute(
            "CREATE TABLE t_late (
                id INT NOT NULL,
                payload INT NOT NULL,
                pad01 TEXT,
                pad02 TEXT,
                pad03 TEXT,
                pad04 TEXT,
                pad05 TEXT,
                pad06 TEXT
            )",
        )
        .await
        .expect("create");
    client
        .batch_execute(
            "INSERT INTO t_late VALUES
                (1, 10, 'a1', 'b1', 'c1', 'd1', 'e1', 'f1'),
                (2, 20, 'a2', 'b2', 'c2', 'd2', 'e2', 'f2'),
                (3, 30, 'a3', 'b3', 'c3', 'd3', 'e3', 'f3')",
        )
        .await
        .expect("seed");
    client
        .batch_execute("CREATE INDEX t_late_id_idx ON t_late (id)")
        .await
        .expect("create index");

    let rows = client
        .query(
            "EXPLAIN ANALYZE SELECT payload FROM t_late WHERE id = 2",
            &[],
        )
        .await
        .expect("EXPLAIN ANALYZE");
    let text = collect_plan_text(&rows);

    assert!(
        text.contains("Late Materialization: selected t_late_id_idx"),
        "EXPLAIN ANALYZE must report late materialization selection, got: {text}"
    );

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn explain_analyze_skips_late_materialization_on_narrow_table() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t_late_narrow (id INT NOT NULL, payload INT NOT NULL)")
        .await
        .expect("create");
    client
        .batch_execute("INSERT INTO t_late_narrow VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .expect("seed");
    client
        .batch_execute("CREATE INDEX t_late_narrow_id_idx ON t_late_narrow (id)")
        .await
        .expect("create index");

    let rows = client
        .query(
            "EXPLAIN ANALYZE SELECT payload FROM t_late_narrow WHERE id = 2",
            &[],
        )
        .await
        .expect("EXPLAIN ANALYZE");
    let text = collect_plan_text(&rows);

    assert!(
        text.contains("Late Materialization: not selected"),
        "narrow table must not use late materialization, got: {text}"
    );

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn explain_analyze_late_materialization_handles_limit_order_nulls_and_dml() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute(
            "CREATE TABLE t_late_wide (
                id INT NOT NULL,
                payload INT,
                pad01 TEXT,
                pad02 TEXT,
                pad03 TEXT,
                pad04 TEXT,
                pad05 TEXT,
                pad06 TEXT,
                pad07 TEXT,
                pad08 TEXT
            )",
        )
        .await
        .expect("create");
    client
        .batch_execute(
            "INSERT INTO t_late_wide VALUES
                (1, 10, 'a1', 'b1', 'c1', 'd1', 'e1', 'f1', 'g1', 'h1'),
                (2, NULL, 'a2', 'b2', 'c2', 'd2', 'e2', 'f2', 'g2', 'h2'),
                (3, 30, 'a3', 'b3', 'c3', 'd3', 'e3', 'f3', 'g3', 'h3'),
                (4, 40, 'a4', 'b4', 'c4', 'd4', 'e4', 'f4', 'g4', 'h4'),
                (5, 50, 'a5', 'b5', 'c5', 'd5', 'e5', 'f5', 'g5', 'h5')",
        )
        .await
        .expect("seed");
    client
        .batch_execute("CREATE INDEX t_late_wide_id_idx ON t_late_wide (id)")
        .await
        .expect("create index");
    client
        .batch_execute("UPDATE t_late_wide SET payload = 333, pad08 = 'updated' WHERE id = 3")
        .await
        .expect("update");
    client
        .batch_execute("DELETE FROM t_late_wide WHERE id = 4")
        .await
        .expect("delete");

    let selected = client
        .query(
            "SELECT payload FROM t_late_wide WHERE id >= 2 ORDER BY id LIMIT 3",
            &[],
        )
        .await
        .expect("select");
    let payloads = selected
        .iter()
        .map(|row| row.get::<_, Option<i32>>(0))
        .collect::<Vec<_>>();
    assert_eq!(payloads, vec![None, Some(333), Some(50)]);

    let rows = client
        .query(
            "EXPLAIN ANALYZE SELECT payload FROM t_late_wide WHERE id >= 2 ORDER BY id LIMIT 3",
            &[],
        )
        .await
        .expect("EXPLAIN ANALYZE");
    let text = collect_plan_text(&rows);
    for needle in [
        "Late Materialization: selected t_late_wide_id_idx",
        // Under the lossy-index + heap-recheck model (PG-style, no per-MVCC-delete
        // leaf removal) the `DELETE FROM t_late_wide WHERE id = 4` above leaves id=4's
        // index leaf entry in place, so the late-materialization scan now examines it
        // as a candidate and skips it after the heap visibility recheck: candidates
        // 3 -> 4 and skipped 0 -> 1. `fetched` and the returned rows are unchanged.
        "candidates=4",
        "fetched=3",
        "skipped=1",
    ] {
        assert!(
            text.contains(needle),
            "late materialization EXPLAIN missing {needle}, got: {text}"
        );
    }

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn explain_analyze_reports_parquet_row_groups_scanned_and_skipped() {
    let dir = tempfile::tempdir().expect("tempdir");
    let parquet_path = dir.path().join("people.parquet");
    write_people_parquet(
        &parquet_path,
        &[(1, "Ada", 10), (2, "Linus", 20)],
        &[(100, "Grace", 90), (101, "Katherine", 95)],
    );

    let (client, _conn, server_handle) = start_server_and_connect().await;
    let sql = format!(
        "EXPLAIN ANALYZE SELECT id FROM read_parquet({}) WHERE id >= 100",
        sql_string(parquet_path.to_str().expect("utf8 parquet path")),
    );

    let rows = client.query(&sql, &[]).await.expect("EXPLAIN ANALYZE");
    let text = collect_plan_text(&rows);
    assert!(
        text.contains("Parquet Row Groups: scanned=1 skipped=1"),
        "EXPLAIN ANALYZE must report parquet row groups, got: {text}"
    );
    assert!(
        text.contains("Parquet Columns Read: columns_read=id count=1"),
        "EXPLAIN ANALYZE must report projected parquet columns_read, got: {text}"
    );

    shutdown(client, server_handle).await;
}

/// `EXPLAIN (FORMAT JSON) SELECT` returns JSON-parseable text with the
/// `Node Type` key.
#[tokio::test]
async fn explain_format_json_returns_parseable_json() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL, v INT NOT NULL)")
        .await
        .expect("create");
    client
        .batch_execute("INSERT INTO t VALUES (1, 10)")
        .await
        .expect("seed");

    let rows = client
        .query("EXPLAIN (FORMAT JSON) SELECT id FROM t WHERE id = 1", &[])
        .await
        .expect("EXPLAIN (FORMAT JSON)");
    let text = collect_plan_text(&rows);
    assert!(
        text.contains("\"Node Type\""),
        "EXPLAIN (FORMAT JSON) must contain Node Type key, got: {text}"
    );
    // Top-level shape is an array of one object — the `[` / `]`
    // brackets must appear at the very edges of the document.
    assert!(text.starts_with('['), "JSON must start with '[': {text}");
    assert!(text.ends_with(']'), "JSON must end with ']': {text}");

    shutdown(client, server_handle).await;
}
