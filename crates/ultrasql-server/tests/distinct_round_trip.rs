//! End-to-end `SELECT DISTINCT` tests against a real `tokio-postgres`
//! client.
//!
//! Closes the v0.5 wire-protocol gap "`Unique` operator — kernel
//! exists; DISTINCT wire path pending" (`ROADMAP.md:686`) and the v0.6
//! "Hash-based DISTINCT vs Sort-based DISTINCT" item. The binder now
//! lowers `SELECT DISTINCT` into a `LogicalPlan::Aggregate` with the
//! projected columns as group keys and an empty aggregate list; the
//! existing `HashAggregate` operator then deduplicates.
//!
//! `SELECT DISTINCT ON (...)` is still rejected at the binder
//! (`PlanError::NotSupported`); covering it would require carrying the
//! `ON` keys through ORDER BY semantics. The unit test
//! `distinct_on_is_rejected_for_now` pins the current contract so a
//! regression doesn't silently accept it without a proper lowering.

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
        "host={host} port={port} user=tester application_name=distinct_test",
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

/// `SELECT DISTINCT a FROM t` returns every distinct value of `a`
/// exactly once.
#[tokio::test]
async fn select_distinct_single_column_dedups() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)")
        .await
        .expect("create");
    client
        .batch_execute("INSERT INTO t VALUES (1, 10), (1, 11), (2, 20), (2, 22), (3, 30)")
        .await
        .expect("seed");

    let rows = client
        .query("SELECT DISTINCT a FROM t", &[])
        .await
        .expect("select distinct a");
    let values: HashSet<i32> = rows.iter().map(|r| r.get::<_, i32>(0)).collect();
    assert_eq!(values, HashSet::from([1, 2, 3]));

    shutdown(client, server_handle).await;
}

/// `SELECT DISTINCT a, b FROM t` returns every distinct `(a, b)` pair
/// exactly once.
#[tokio::test]
async fn select_distinct_two_columns_dedups_pair() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)")
        .await
        .expect("create");
    client
        .batch_execute(
            "INSERT INTO t VALUES \
             (1, 10), (1, 10), (1, 11), (2, 20), (2, 20), (3, 30), (3, 30), (3, 30)",
        )
        .await
        .expect("seed");

    let rows = client
        .query("SELECT DISTINCT a, b FROM t", &[])
        .await
        .expect("select distinct a, b");
    let values: HashSet<(i32, i32)> = rows
        .iter()
        .map(|r| (r.get::<_, i32>(0), r.get::<_, i32>(1)))
        .collect();
    let expected: HashSet<(i32, i32)> =
        HashSet::from([(1, 10), (1, 11), (2, 20), (3, 30)]);
    assert_eq!(values, expected);

    shutdown(client, server_handle).await;
}

/// `SELECT DISTINCT` over a table with no duplicates simply returns
/// every row.
#[tokio::test]
async fn select_distinct_with_no_duplicates_returns_all_rows() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (a INT NOT NULL)")
        .await
        .expect("create");
    client
        .batch_execute("INSERT INTO t VALUES (1), (2), (3), (4)")
        .await
        .expect("seed");

    let rows = client
        .query("SELECT DISTINCT a FROM t", &[])
        .await
        .expect("select distinct a");
    let values: HashSet<i32> = rows.iter().map(|r| r.get::<_, i32>(0)).collect();
    assert_eq!(values, HashSet::from([1, 2, 3, 4]));

    shutdown(client, server_handle).await;
}

/// `SELECT DISTINCT ON (...)` stays rejected until a dedicated lowering
/// ships. Pin the contract so it does not silently start accepting and
/// returning wrong rows.
#[tokio::test]
async fn select_distinct_on_is_rejected_for_now() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)")
        .await
        .expect("create");
    client
        .batch_execute("INSERT INTO t VALUES (1, 10), (1, 11), (2, 20)")
        .await
        .expect("seed");

    let err = client
        .query("SELECT DISTINCT ON (a) a, b FROM t ORDER BY a, b", &[])
        .await
        .expect_err("DISTINCT ON should still be rejected");
    // The server collapses all `PlanError` variants into a single
    // SQLSTATE (`42P01`) today. A finer-grained mapping
    // (`PlanError::NotSupported` → `0A000`) is out of scope for this
    // wave — pin "DISTINCT ON is the cause" by reading the server-sent
    // error message so a regression that silently accepts DISTINCT ON
    // (and returns wrong rows) still fails this test.
    let db_err = err
        .as_db_error()
        .expect("server-sent ErrorResponse for DISTINCT ON");
    assert!(
        db_err.message().contains("DISTINCT ON"),
        "expected DISTINCT ON in error message, got {:?}",
        db_err.message()
    );

    shutdown(client, server_handle).await;
}
