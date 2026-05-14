//! End-to-end `LIMIT n OFFSET m` tests against a real `tokio-postgres`
//! client.
//!
//! Closes the v0.5 P0 wire-protocol gap "Wire OFFSET" by driving an
//! in-process `ultrasqld` with a stock `tokio-postgres` client and
//! asserting that the three documented `LIMIT` / `OFFSET` shapes round
//! trip the wire:
//!
//! - `LIMIT n OFFSET m` — skip `m` rows, then take at most `n`.
//! - `LIMIT 0 OFFSET m` — emits zero rows regardless of the skip.
//! - `OFFSET m` (no `LIMIT`) — emits every row past the skip window.
//!   The binder lowers this as `LIMIT u64::MAX OFFSET m`; the pipeline
//!   saturates `u64::MAX` into the executor's `usize::MAX` "no limit"
//!   sentinel.
//!
//! Each test creates a fresh table per-server so the assertions don't
//! depend on cross-test ordering.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio_postgres::NoTls;
use ultrasql_server::{Server, bind_listener, serve_listener};

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
        "host={host} port={port} user=tester application_name=limit_offset_test",
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

/// Tidy shutdown sequence — drop the client, give the connection task
/// a beat to flush its socket teardown, then abort the listener.
async fn shutdown(
    client: tokio_postgres::Client,
    server_handle: tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
) {
    drop(client);
    tokio::time::sleep(Duration::from_millis(20)).await;
    server_handle.abort();
}

/// Insert a sequence of integer ids into `table`. Each insert runs as a
/// separate statement so the helper does not depend on multi-row VALUES
/// support.
async fn insert_ids(client: &tokio_postgres::Client, table: &str, ids: &[i32]) {
    for id in ids {
        client
            .batch_execute(&format!("INSERT INTO {table} VALUES ({id})"))
            .await
            .expect("insert id");
    }
}

/// Drain a simple-query result into the `id` column parsed as `i32`,
/// preserving the on-wire order so a caller can assert
/// `ORDER BY`-dependent expectations.
fn ids_from(result: &[tokio_postgres::SimpleQueryMessage]) -> Vec<i32> {
    result
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(
                row.get(0)
                    .expect("id present")
                    .parse::<i32>()
                    .expect("id parses"),
            ),
            _ => None,
        })
        .collect()
}

/// `SELECT ... ORDER BY id LIMIT 5 OFFSET 10` over 20 rows returns ids
/// 11..=15 — the canonical "page 3 of size 5" shape.
#[tokio::test]
async fn limit_with_offset_returns_window() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE lo_window (id INT NOT NULL)")
        .await
        .expect("create table");
    let ids: Vec<i32> = (1..=20).collect();
    insert_ids(&client, "lo_window", &ids).await;

    let rows = client
        .simple_query("SELECT id FROM lo_window ORDER BY id LIMIT 5 OFFSET 10")
        .await
        .expect("query succeeds");
    let got = ids_from(&rows);
    assert_eq!(got, vec![11, 12, 13, 14, 15]);

    shutdown(client, server_handle).await;
}

/// `SELECT ... LIMIT 0 OFFSET m` returns no rows regardless of the
/// skip. Confirms the operator short-circuits when the budget is
/// exhausted by the limit alone.
#[tokio::test]
async fn limit_zero_with_offset_returns_no_rows() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE lo_zero (id INT NOT NULL)")
        .await
        .expect("create table");
    insert_ids(&client, "lo_zero", &[1, 2, 3, 4, 5, 6]).await;

    let rows = client
        .simple_query("SELECT id FROM lo_zero LIMIT 0 OFFSET 5")
        .await
        .expect("query succeeds");
    let got = ids_from(&rows);
    assert!(got.is_empty(), "LIMIT 0 must emit no rows, got {got:?}");

    shutdown(client, server_handle).await;
}

/// `SELECT ... ORDER BY id OFFSET 5` (no LIMIT) over 10 rows returns
/// ids 6..=10. The binder synthesises a `Limit { n: u64::MAX, offset }`
/// here; this test verifies the pipeline saturates the sentinel
/// instead of rejecting the statement.
#[tokio::test]
async fn offset_without_limit_returns_tail() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE lo_tail (id INT NOT NULL)")
        .await
        .expect("create table");
    let ids: Vec<i32> = (1..=10).collect();
    insert_ids(&client, "lo_tail", &ids).await;

    let rows = client
        .simple_query("SELECT id FROM lo_tail ORDER BY id OFFSET 5")
        .await
        .expect("query succeeds");
    let got = ids_from(&rows);
    assert_eq!(got, vec![6, 7, 8, 9, 10]);

    shutdown(client, server_handle).await;
}

/// `OFFSET m` where `m` exceeds the row count emits zero rows. Confirms
/// the operator does not over-emit when the skip drains the child
/// before any output row is produced.
#[tokio::test]
async fn offset_past_end_returns_no_rows() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE lo_past (id INT NOT NULL)")
        .await
        .expect("create table");
    insert_ids(&client, "lo_past", &[1, 2, 3]).await;

    let rows = client
        .simple_query("SELECT id FROM lo_past ORDER BY id OFFSET 100")
        .await
        .expect("query succeeds");
    let got = ids_from(&rows);
    assert!(
        got.is_empty(),
        "OFFSET past end must emit no rows, got {got:?}"
    );

    shutdown(client, server_handle).await;
}
