//! End-to-end NOT NULL constraint enforcement tests.
//!
//! Closes the v0.5 partial-item §1.21 (constraint enforcement at the
//! executor) for the NOT NULL case. `ModifyTable::Insert` now consults
//! the schema's `Field::nullable` bit and returns
//! `ExecError::NotNullViolation(column_name)` on the first violating
//! row; the server error mapping translates that into SQLSTATE
//! `23502` (`not_null_violation`).
//!
//! UNIQUE / CHECK / FOREIGN KEY enforcement remains on the v0.8
//! constraint roadmap because the `TableEntry` catalog row does not
//! yet carry constraint definitions — those need a separate plumb-
//! through pass.

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
        "host={host} port={port} user=tester application_name=constraint_test",
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

/// `INSERT INTO t VALUES (NULL, ...)` on a NOT NULL column fails with
/// SQLSTATE `23502`.
#[tokio::test]
async fn insert_null_into_not_null_column_returns_23502() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL, v INT)")
        .await
        .expect("create");

    let err = client
        .batch_execute("INSERT INTO t VALUES (NULL, 10)")
        .await
        .expect_err("NOT NULL column rejects NULL");
    let sqlstate = err.code().expect("server-sent SQLSTATE present");
    assert_eq!(
        sqlstate.code(),
        "23502",
        "expected not_null_violation, got {err:?}"
    );

    // The rejected row must not land in the heap.
    let rows = client
        .query("SELECT id FROM t", &[])
        .await
        .expect("select after rejected INSERT");
    assert!(rows.is_empty(), "rejected INSERT must not leak rows");

    shutdown(client, server_handle).await;
}

/// `INSERT INTO t VALUES (..., NULL)` on a nullable column succeeds.
#[tokio::test]
async fn insert_null_into_nullable_column_succeeds() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL, v INT)")
        .await
        .expect("create");
    client
        .batch_execute("INSERT INTO t VALUES (1, NULL)")
        .await
        .expect("nullable column accepts NULL");

    let rows = client
        .query("SELECT id, v FROM t", &[])
        .await
        .expect("select after INSERT");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>(0), 1);
    let v: Option<i32> = rows[0].get(1);
    assert!(v.is_none(), "nullable column carries NULL");

    shutdown(client, server_handle).await;
}

/// Multi-row INSERT where one row violates NOT NULL must be atomic in
/// the sense that the rejected statement leaves no rows behind.
#[tokio::test]
async fn multi_row_insert_aborts_on_not_null_violation() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL, v INT NOT NULL)")
        .await
        .expect("create");

    let err = client
        .batch_execute("INSERT INTO t VALUES (1, 10), (2, NULL), (3, 30)")
        .await
        .expect_err("statement rejects on NOT NULL violation");
    let sqlstate = err.code().expect("server-sent SQLSTATE present");
    assert_eq!(sqlstate.code(), "23502");

    let rows = client
        .query("SELECT id FROM t", &[])
        .await
        .expect("select after rejected multi-row INSERT");
    assert!(
        rows.is_empty(),
        "rejected multi-row INSERT must not leak partial rows, got {rows:?}"
    );

    shutdown(client, server_handle).await;
}
