//! End-to-end tests for FROM-less `SELECT` and the `IS NULL` predicate.
//!
//! Closes two v0.5 ROADMAP gaps:
//! - **`Result` (constant expressions) — `SELECT 1` and similar**
//!   (Other Operators).
//! - **`SELECT … FROM t WHERE col IS NULL` end-to-end verification**
//!   (Binder gaps blocking wire).
//!
//! Driven through the real PostgreSQL wire protocol so the codec,
//! parameter substitution, and `RowDescription` paths are exercised
//! end-to-end.

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
        "host={host} port={port} user=tester application_name=select_constants_test",
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

/// `SELECT 1` returns one row with one int column.
#[tokio::test]
async fn select_one_round_trip() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    let rows = client.query("SELECT 1", &[]).await.expect("SELECT 1");
    assert_eq!(rows.len(), 1, "single-row result expected");
    assert_eq!(rows[0].get::<_, i32>(0), 1);

    shutdown(client, server_handle).await;
}

/// `SELECT 1, 2, 3` returns one row with three int columns in order.
#[tokio::test]
async fn select_multi_constant_round_trip() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    let rows = client
        .query("SELECT 1, 2, 3", &[])
        .await
        .expect("SELECT multi-const");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>(0), 1);
    assert_eq!(rows[0].get::<_, i32>(1), 2);
    assert_eq!(rows[0].get::<_, i32>(2), 3);

    shutdown(client, server_handle).await;
}

/// `SELECT … WHERE col IS NULL` filters out non-NULL rows.
#[tokio::test]
async fn select_where_is_null_round_trip() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL, val INT)")
        .await
        .expect("create table");
    client
        .batch_execute("INSERT INTO t VALUES (1, 10)")
        .await
        .expect("insert with value");
    client
        .batch_execute("INSERT INTO t VALUES (2, NULL)")
        .await
        .expect("insert null");
    client
        .batch_execute("INSERT INTO t VALUES (3, 30)")
        .await
        .expect("insert with value");

    let rows = client
        .query("SELECT id FROM t WHERE val IS NULL", &[])
        .await
        .expect("SELECT WHERE IS NULL");
    assert_eq!(rows.len(), 1, "exactly one row has NULL val");
    assert_eq!(rows[0].get::<_, i32>(0), 2);

    shutdown(client, server_handle).await;
}

/// `SELECT … WHERE col IS NOT NULL` keeps non-NULL rows only.
#[tokio::test]
async fn select_where_is_not_null_round_trip() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL, val INT)")
        .await
        .expect("create table");
    client
        .batch_execute("INSERT INTO t VALUES (1, 10)")
        .await
        .expect("insert");
    client
        .batch_execute("INSERT INTO t VALUES (2, NULL)")
        .await
        .expect("insert null");
    client
        .batch_execute("INSERT INTO t VALUES (3, 30)")
        .await
        .expect("insert");

    let rows = client
        .query("SELECT id FROM t WHERE val IS NOT NULL ORDER BY id", &[])
        .await
        .expect("SELECT WHERE IS NOT NULL");
    let ids: Vec<i32> = rows.iter().map(|r| r.get::<_, i32>(0)).collect();
    assert_eq!(ids, vec![1, 3]);

    shutdown(client, server_handle).await;
}
