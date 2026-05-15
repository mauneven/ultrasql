//! End-to-end `INSERT INTO t SELECT ...` tests against a real
//! `tokio-postgres` client.
//!
//! Closes the v0.5 wire-protocol gap "`INSERT … SELECT`
//! (`pipeline.rs:1314` returns `Unsupported`)" at `ROADMAP.md:319`. The
//! binder already produced `LogicalPlan::Insert { source: Select … }`;
//! `lower_real_insert` now lowers the inner SELECT through
//! `lower_query` and drives `ModifyTable::Insert` off its batches.
//!
//! Shapes covered:
//!
//! - `INSERT INTO dst SELECT a, b FROM src WHERE a > N` — predicate
//!   filtered, full copy of the matching rows.
//! - `INSERT INTO dst SELECT a, b FROM src` — no predicate, full
//!   copy.
//! - Idempotence: two `INSERT … SELECT` statements double the row
//!   count.
//! - Schema arity mismatch is rejected before any heap write.

use std::collections::HashSet;
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
        "host={host} port={port} user=tester application_name=insert_select_test",
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

/// `INSERT INTO dst SELECT a, b FROM src WHERE a > 100` copies the
/// rows that satisfy the predicate into the destination relation.
#[tokio::test]
async fn insert_select_with_predicate_copies_filtered_rows() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE src (a INT NOT NULL, b INT NOT NULL)")
        .await
        .expect("create src");
    client
        .batch_execute("CREATE TABLE dst (a INT NOT NULL, b INT NOT NULL)")
        .await
        .expect("create dst");

    client
        .batch_execute(
            "INSERT INTO src VALUES \
             (50, 5), (100, 10), (150, 15), (200, 20), (250, 25)",
        )
        .await
        .expect("seed src");

    client
        .batch_execute("INSERT INTO dst SELECT a, b FROM src WHERE a > 100")
        .await
        .expect("INSERT INTO dst SELECT");

    let rows = client
        .query("SELECT a, b FROM dst", &[])
        .await
        .expect("select dst");
    let values: HashSet<(i32, i32)> = rows
        .iter()
        .map(|r| (r.get::<_, i32>(0), r.get::<_, i32>(1)))
        .collect();
    assert_eq!(values, HashSet::from([(150, 15), (200, 20), (250, 25)]));

    shutdown(client, server_handle).await;
}

/// `INSERT INTO dst SELECT a, b FROM src` (no WHERE) copies every row.
#[tokio::test]
async fn insert_select_without_predicate_copies_all_rows() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE src (a INT NOT NULL, b INT NOT NULL)")
        .await
        .expect("create src");
    client
        .batch_execute("CREATE TABLE dst (a INT NOT NULL, b INT NOT NULL)")
        .await
        .expect("create dst");

    client
        .batch_execute("INSERT INTO src VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .expect("seed src");

    client
        .batch_execute("INSERT INTO dst SELECT a, b FROM src")
        .await
        .expect("INSERT INTO dst SELECT");

    let rows = client
        .query("SELECT a, b FROM dst", &[])
        .await
        .expect("select dst");
    let values: HashSet<(i32, i32)> = rows
        .iter()
        .map(|r| (r.get::<_, i32>(0), r.get::<_, i32>(1)))
        .collect();
    assert_eq!(values, HashSet::from([(1, 10), (2, 20), (3, 30)]));

    shutdown(client, server_handle).await;
}

/// Two `INSERT INTO dst SELECT …` statements double the destination's
/// row count — verifies the path isn't a one-shot.
#[tokio::test]
async fn insert_select_runs_idempotently_twice() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE src (a INT NOT NULL, b INT NOT NULL)")
        .await
        .expect("create src");
    client
        .batch_execute("CREATE TABLE dst (a INT NOT NULL, b INT NOT NULL)")
        .await
        .expect("create dst");

    client
        .batch_execute("INSERT INTO src VALUES (1, 10), (2, 20)")
        .await
        .expect("seed src");

    client
        .batch_execute("INSERT INTO dst SELECT a, b FROM src")
        .await
        .expect("first INSERT … SELECT");
    client
        .batch_execute("INSERT INTO dst SELECT a, b FROM src")
        .await
        .expect("second INSERT … SELECT");

    let rows = client
        .query("SELECT a FROM dst", &[])
        .await
        .expect("select dst");
    assert_eq!(rows.len(), 4, "two SELECTs land 2 + 2 = 4 rows");

    shutdown(client, server_handle).await;
}

/// `INSERT … SELECT` with a column-count mismatch must be rejected
/// before any tuple lands in the heap.
#[tokio::test]
async fn insert_select_arity_mismatch_is_rejected() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE src (a INT NOT NULL, b INT NOT NULL, c INT NOT NULL)")
        .await
        .expect("create src");
    client
        .batch_execute("CREATE TABLE dst (a INT NOT NULL, b INT NOT NULL)")
        .await
        .expect("create dst");
    client
        .batch_execute("INSERT INTO src VALUES (1, 2, 3)")
        .await
        .expect("seed src");

    let err = client
        .batch_execute("INSERT INTO dst SELECT a, b, c FROM src")
        .await
        .expect_err("arity mismatch must error");
    let db_err = err
        .as_db_error()
        .expect("server-sent ErrorResponse for arity mismatch");
    assert!(
        db_err.message().to_ascii_lowercase().contains("insert"),
        "expected INSERT-related error message, got {:?}",
        db_err.message()
    );

    // Destination still empty: no partial write should have leaked
    // through.
    let post = client
        .query("SELECT a FROM dst", &[])
        .await
        .expect("select dst");
    assert!(post.is_empty(), "rejected INSERT must not leak rows");

    shutdown(client, server_handle).await;
}
