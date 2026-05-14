//! End-to-end Extended Query Protocol tests against a real
//! `tokio-postgres` client.
//!
//! These tests bind an in-process `ultrasqld` to an ephemeral TCP port,
//! drive it with a stock `tokio-postgres` client (the same one any
//! third-party Rust application would use), and assert that
//! `client.prepare(...).await?` / `client.query(&stmt, &[...]).await?`
//! return the same rows the Simple Query path produces.
//!
//! The shapes exercised here match the v0.5 roadmap milestones for
//! Extended Query dispatch:
//!
//! - `CREATE TABLE`
//! - `INSERT INTO t VALUES (...)`
//! - `SELECT id, val FROM t`
//! - `SELECT id, val FROM t WHERE col = $1`
//! - `SELECT SUM(x) FROM t`
//! - `UPDATE t SET val = $1 WHERE id = $2`
//! - `DELETE FROM t WHERE id = $1`
//!
//! Network setup is identical to the duplex-level tests in `src/lib.rs`,
//! but we go through the loopback TCP stack so the codec exercises real
//! byte ordering on a real socket.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio_postgres::NoTls;
use ultrasql_server::{Server, bind_listener, serve_listener};

/// Bring up a fresh in-process server bound to `127.0.0.1:0` and return
/// `(client, server_task_join_handle)`.
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
        "host={host} port={port} user=tester application_name=extended_query_test",
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

/// Prepared-statement SELECT over the canonical sample table.
///
/// Uses the unnamed prepared statement (the path libpq's `PQexecPrepared`
/// hits by default).
#[tokio::test]
async fn prepared_select_all_returns_sample_rows() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    let stmt = client
        .prepare("SELECT id FROM users")
        .await
        .expect("prepare succeeds");
    let rows = client.query(&stmt, &[]).await.expect("query succeeds");
    assert_eq!(rows.len(), 3, "expected 3 sample rows");

    // tokio-postgres returns binary by default for int4. Use the typed
    // accessor; this exercises the binary-format result encoder.
    let mut ids: Vec<i32> = rows.iter().map(|r| r.get::<_, i32>(0)).collect();
    ids.sort_unstable();
    assert_eq!(ids, vec![1, 2, 3]);

    drop(client);
    // Give the connection task a moment to wind down.
    tokio::time::sleep(Duration::from_millis(20)).await;
    server_handle.abort();
}

/// Prepared SELECT with a positional parameter — the headline shape the
/// roadmap calls out.
#[tokio::test]
async fn prepared_select_with_int_parameter() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    let stmt = client
        .prepare("SELECT id FROM users WHERE id = $1")
        .await
        .expect("prepare succeeds");
    let rows = client
        .query(&stmt, &[&2_i32])
        .await
        .expect("query succeeds");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>(0), 2);

    // Bind a different parameter and re-execute — the same prepared
    // statement is reused (server-side statement cache).
    let rows = client
        .query(&stmt, &[&3_i32])
        .await
        .expect("query succeeds");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>(0), 3);

    // A parameter that matches no row returns zero rows.
    let rows = client
        .query(&stmt, &[&999_i32])
        .await
        .expect("query succeeds");
    assert_eq!(rows.len(), 0);

    drop(client);
    tokio::time::sleep(Duration::from_millis(20)).await;
    server_handle.abort();
}

/// `CREATE TABLE → INSERT VALUES (multi-row) → SELECT id,val → SELECT WHERE id=$1`
/// over the Extended Query path. Exercises the same end-to-end shape
/// that the Simple Query tests cover.
#[tokio::test]
async fn prepared_create_insert_select_filter_round_trip() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    // CREATE TABLE — DDL is rejected by the Extended Query path
    // (documented gap). Use the Simple Query fallback via batch_execute.
    client
        .batch_execute("CREATE TABLE items (id INT NOT NULL, val INT)")
        .await
        .expect("create table via Simple Query");

    // INSERT — multi-row VALUES. Run as a prepared statement so we
    // exercise the Extended Query path.
    let insert = client
        .prepare("INSERT INTO items VALUES ($1, $2)")
        .await
        .expect("prepare insert");
    for (id, v) in [(1_i32, 100_i32), (2, 200), (3, 300)] {
        client
            .execute(&insert, &[&id, &v])
            .await
            .expect("insert executes");
    }

    // SELECT id, val FROM items — full table scan over the real heap.
    let select_all = client
        .prepare("SELECT id, val FROM items")
        .await
        .expect("prepare select_all");
    let rows = client
        .query(&select_all, &[])
        .await
        .expect("query succeeds");
    let mut pairs: Vec<(i32, i32)> = rows
        .iter()
        .map(|r| (r.get::<_, i32>(0), r.get::<_, i32>(1)))
        .collect();
    pairs.sort_unstable();
    assert_eq!(pairs, vec![(1, 100), (2, 200), (3, 300)]);

    // SELECT WHERE col op lit form, but with a parameter.
    let select_one = client
        .prepare("SELECT id, val FROM items WHERE id = $1")
        .await
        .expect("prepare select_one");
    let rows = client
        .query(&select_one, &[&2_i32])
        .await
        .expect("query succeeds");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>(0), 2);
    assert_eq!(rows[0].get::<_, i32>(1), 200);

    drop(client);
    tokio::time::sleep(Duration::from_millis(20)).await;
    server_handle.abort();
}

/// Scalar aggregate over the prepared-statement path.
#[tokio::test]
async fn prepared_scalar_aggregate() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE nums (id INT NOT NULL, x INT)")
        .await
        .expect("create table");

    let insert = client
        .prepare("INSERT INTO nums VALUES ($1, $2)")
        .await
        .expect("prepare insert");
    for i in 1_i32..=10 {
        client
            .execute(&insert, &[&i, &(i * 10)])
            .await
            .expect("insert executes");
    }

    let agg = client
        .prepare("SELECT SUM(x) FROM nums")
        .await
        .expect("prepare aggregate");
    let rows = client.query(&agg, &[]).await.expect("query succeeds");
    assert_eq!(rows.len(), 1);
    // SUM of int4 widens to int8 per PostgreSQL semantics.
    let total: i64 = rows[0].get(0);
    assert_eq!(total, 550);

    drop(client);
    tokio::time::sleep(Duration::from_millis(20)).await;
    server_handle.abort();
}

/// Prepared UPDATE + DELETE round-trip.
#[tokio::test]
async fn prepared_update_and_delete() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE things (id INT NOT NULL, val INT)")
        .await
        .expect("create table");

    let insert = client
        .prepare("INSERT INTO things VALUES ($1, $2)")
        .await
        .expect("prepare insert");
    for (id, v) in [(1_i32, 10_i32), (2, 20), (3, 30)] {
        client.execute(&insert, &[&id, &v]).await.expect("insert");
    }

    // UPDATE val WHERE id = $1
    let upd = client
        .prepare("UPDATE things SET val = $1 WHERE id = $2")
        .await
        .expect("prepare update");
    let n = client
        .execute(&upd, &[&999_i32, &2_i32])
        .await
        .expect("update executes");
    assert_eq!(n, 1, "exactly one row updated");

    // Verify via prepared SELECT.
    let sel = client
        .prepare("SELECT val FROM things WHERE id = $1")
        .await
        .expect("prepare select");
    let rows = client.query(&sel, &[&2_i32]).await.expect("select");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>(0), 999);

    // DELETE WHERE id = $1
    let del = client
        .prepare("DELETE FROM things WHERE id = $1")
        .await
        .expect("prepare delete");
    let n = client.execute(&del, &[&3_i32]).await.expect("delete");
    assert_eq!(n, 1, "exactly one row deleted");

    // Re-scan: only ids 1 and 2 remain.
    let scan = client
        .prepare("SELECT id, val FROM things")
        .await
        .expect("prepare scan");
    let rows = client.query(&scan, &[]).await.expect("scan");
    let mut pairs: Vec<(i32, i32)> = rows
        .iter()
        .map(|r| (r.get::<_, i32>(0), r.get::<_, i32>(1)))
        .collect();
    pairs.sort_unstable();
    assert_eq!(pairs, vec![(1, 10), (2, 999)]);

    drop(client);
    tokio::time::sleep(Duration::from_millis(20)).await;
    server_handle.abort();
}

/// `tokio-postgres` calls `Close(Portal)` after every `query()`. Verify
/// the close handler does not break the session.
#[tokio::test]
async fn repeated_executions_of_same_prepared_statement() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    let stmt = client
        .prepare("SELECT id FROM users WHERE id = $1")
        .await
        .expect("prepare");

    for i in 1..=3_i32 {
        let rows = client.query(&stmt, &[&i]).await.expect("query");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get::<_, i32>(0), i);
    }

    drop(client);
    tokio::time::sleep(Duration::from_millis(20)).await;
    server_handle.abort();
}
