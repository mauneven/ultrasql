//! End-to-end `EXPLAIN` / `EXPLAIN ANALYZE` / `EXPLAIN (FORMAT JSON)`
//! tests against a real `tokio-postgres` client.
//!
//! Closes the v0.5 wire-protocol gap "`EXPLAIN` / `EXPLAIN ANALYZE` —
//! no `LogicalPlan::Explain`, no session dispatch" at `ROADMAP.md:336`.
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
        "Pushdowns Applied:",
    ] {
        assert!(
            text.contains(required),
            "EXPLAIN ANALYZE missing {required:?}, got: {text}"
        );
    }

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
