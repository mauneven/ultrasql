//! End-to-end `TRUNCATE` tests against a real `tokio-postgres` client.
//!
//! Closes the v0.5 P0 wire-protocol gap "Wire TRUNCATE" by driving an
//! in-process `ultrasqld` with a stock `tokio-postgres` client and
//! asserting the documented `TRUNCATE` shapes round trip the wire:
//!
//! - `TRUNCATE TABLE t` over a populated relation empties it, and a
//!   subsequent `SELECT COUNT(*) FROM t` returns 0.
//! - `TRUNCATE TABLE t1, t2` empties both relations atomically.
//! - `TRUNCATE TABLE t RESTART IDENTITY` accepts the keyword. UltraSQL
//!   does not yet implement `SERIAL` / sequence catalogs (ROADMAP P1
//!   v0.6) so there is nothing to reseed; the test only confirms the
//!   keyword does not produce an error and that the rows are gone.
//!
//! Implementation notes (server side):
//!
//! - The server cannot drop+recreate the heap relfilenode because the
//!   in-memory `BufferPool<BlankPageLoader>` has no segment manager.
//!   `execute_truncate` instead opens an autocommit MVCC transaction
//!   and stamps `xmax` on every visible row. The result is
//!   MVCC-correct (a pre-truncate snapshot still sees the old rows)
//!   and is `O(rows visible)` rather than `O(1)`.
//! - Dead-tuple pages stay on the heap; the catalog's `n_blocks` hint
//!   is left unchanged so a subsequent `INSERT` can reuse the freed
//!   slots and a future scan still covers the right block range.

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
        "host={host} port={port} user=tester application_name=truncate_test",
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

/// Run `SELECT COUNT(*) FROM <table>` and return the value as `i64`.
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

/// `TRUNCATE TABLE t` over a populated relation empties it. A
/// subsequent `SELECT COUNT(*) FROM t` returns 0; a subsequent
/// `INSERT` lands rows that are again countable.
#[tokio::test]
async fn truncate_single_table_empties_relation() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE trunc_single (id INT NOT NULL)")
        .await
        .expect("create table");
    for id in 1..=5 {
        client
            .batch_execute(&format!("INSERT INTO trunc_single VALUES ({id})"))
            .await
            .expect("insert");
    }
    assert_eq!(select_count(&client, "trunc_single").await, 5);

    client
        .batch_execute("TRUNCATE TABLE trunc_single")
        .await
        .expect("truncate succeeds");
    assert_eq!(
        select_count(&client, "trunc_single").await,
        0,
        "TRUNCATE must empty the relation"
    );

    // Re-insert after truncate is observable, confirming the relation
    // is reusable.
    client
        .batch_execute("INSERT INTO trunc_single VALUES (99)")
        .await
        .expect("re-insert after truncate");
    assert_eq!(select_count(&client, "trunc_single").await, 1);

    shutdown(client, server_handle).await;
}

/// `TRUNCATE TABLE t1, t2` empties every listed relation in a single
/// statement. Both counts go to 0; a third unrelated relation is left
/// untouched to verify the multi-table list is exactly what gets
/// truncated.
#[tokio::test]
async fn truncate_multi_table_empties_each_listed_relation() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE trunc_multi_a (id INT NOT NULL)")
        .await
        .expect("create a");
    client
        .batch_execute("CREATE TABLE trunc_multi_b (id INT NOT NULL)")
        .await
        .expect("create b");
    client
        .batch_execute("CREATE TABLE trunc_multi_c (id INT NOT NULL)")
        .await
        .expect("create c");

    for id in 1..=3 {
        client
            .batch_execute(&format!("INSERT INTO trunc_multi_a VALUES ({id})"))
            .await
            .expect("insert a");
        client
            .batch_execute(&format!("INSERT INTO trunc_multi_b VALUES ({})", id + 10))
            .await
            .expect("insert b");
        client
            .batch_execute(&format!("INSERT INTO trunc_multi_c VALUES ({})", id + 20))
            .await
            .expect("insert c");
    }
    assert_eq!(select_count(&client, "trunc_multi_a").await, 3);
    assert_eq!(select_count(&client, "trunc_multi_b").await, 3);
    assert_eq!(select_count(&client, "trunc_multi_c").await, 3);

    client
        .batch_execute("TRUNCATE TABLE trunc_multi_a, trunc_multi_b")
        .await
        .expect("multi-truncate");

    assert_eq!(select_count(&client, "trunc_multi_a").await, 0);
    assert_eq!(select_count(&client, "trunc_multi_b").await, 0);
    // Third relation untouched — multi-truncate must not bleed into
    // unrelated relations.
    assert_eq!(select_count(&client, "trunc_multi_c").await, 3);

    shutdown(client, server_handle).await;
}

/// `TRUNCATE TABLE t RESTART IDENTITY` is accepted by the parser, the
/// binder, and the wire path. UltraSQL has no sequence catalog today
/// (ROADMAP P1 v0.6) so `RESTART IDENTITY` is accept-and-ignore; this
/// test verifies the keyword does not produce an error and that the
/// rows are still cleared.
#[tokio::test]
async fn truncate_restart_identity_accepts_keyword() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE trunc_restart (id INT NOT NULL)")
        .await
        .expect("create table");
    for id in 1..=4 {
        client
            .batch_execute(&format!("INSERT INTO trunc_restart VALUES ({id})"))
            .await
            .expect("insert");
    }
    assert_eq!(select_count(&client, "trunc_restart").await, 4);

    client
        .batch_execute("TRUNCATE TABLE trunc_restart RESTART IDENTITY")
        .await
        .expect("truncate restart identity succeeds");
    assert_eq!(select_count(&client, "trunc_restart").await, 0);

    shutdown(client, server_handle).await;
}

/// `TRUNCATE` on an empty relation is a no-op and must succeed.
#[tokio::test]
async fn truncate_empty_relation_is_noop() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE trunc_empty (id INT NOT NULL)")
        .await
        .expect("create table");
    assert_eq!(select_count(&client, "trunc_empty").await, 0);

    client
        .batch_execute("TRUNCATE TABLE trunc_empty")
        .await
        .expect("truncate empty relation must succeed");
    assert_eq!(select_count(&client, "trunc_empty").await, 0);

    shutdown(client, server_handle).await;
}
