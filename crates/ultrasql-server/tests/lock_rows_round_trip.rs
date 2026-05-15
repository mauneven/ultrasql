//! End-to-end `SELECT ... FOR UPDATE` / `FOR SHARE` / `FOR NO KEY UPDATE`
//! tests against a real `tokio-postgres` client.
//!
//! Closes the v0.5 wire-protocol gap "`LockRows` operator reachable
//! from `lower_query`" — kernel lives at
//! `crates/ultrasql-executor/src/lock_rows.rs`, wired in
//! `crates/ultrasql-server/src/pipeline.rs:275` (sample path) and `:806`
//! (real-heap path). This file drives the round-trip through
//! `tokio-postgres` so the wire codec, binder, lowerer, and visibility
//! are exercised behaviorally.
//!
//! Shapes covered:
//!
//! - `BEGIN; SELECT * FROM t WHERE id = 1 FOR UPDATE; UPDATE t SET v = v + 1
//!   WHERE id = 1; COMMIT;` — the locked-then-updated row.
//! - Concurrent reader observes the locked row's pre-image while the
//!   writer's transaction is still open.
//! - Sequenced two-connection visibility: a reader opened after the
//!   writer commits sees the post-update value.
//! - `FOR SHARE` happy path — the statement returns rows without error.
//! - `FOR NO KEY UPDATE` happy path — the statement returns rows
//!   without error.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio_postgres::NoTls;
use ultrasql_server::{Server, bind_listener, serve_listener};

/// Spin up an in-process server on an ephemeral TCP port and return the
/// bound address plus a server-task join handle. Each test opens as
/// many client connections as it needs against the same `Server` state.
async fn start_server() -> (
    SocketAddr,
    tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
) {
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("addr parses");
    let (listener, bound) = bind_listener(addr).await.expect("bind");
    let server = Arc::new(Server::with_sample_database());
    let server_handle = tokio::spawn(serve_listener(listener, server));
    (bound, server_handle)
}

async fn connect(
    bound: SocketAddr,
    app_name: &str,
) -> (tokio_postgres::Client, tokio::task::JoinHandle<()>) {
    let conn_str = format!(
        "host={host} port={port} user=tester application_name={app_name}",
        host = bound.ip(),
        port = bound.port(),
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .expect("tokio-postgres connect");
    let conn_handle = tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });
    (client, conn_handle)
}

async fn shutdown(
    server_handle: tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
) {
    tokio::time::sleep(Duration::from_millis(20)).await;
    server_handle.abort();
}

/// `BEGIN; SELECT FOR UPDATE; UPDATE; COMMIT;` over a single connection.
/// Verifies the lock-then-update path returns rows and the update lands.
#[tokio::test]
async fn select_for_update_then_update_commits_value() {
    let (bound, server_handle) = start_server().await;
    let (client, _conn) = connect(bound, "lock_rows_single").await;

    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL, v INT NOT NULL)")
        .await
        .expect("create table");
    client
        .batch_execute("INSERT INTO t VALUES (1, 10), (2, 20)")
        .await
        .expect("seed rows");

    client.batch_execute("BEGIN").await.expect("BEGIN");

    let locked = client
        .query("SELECT id, v FROM t WHERE id = 1 FOR UPDATE", &[])
        .await
        .expect("SELECT FOR UPDATE returns rows");
    assert_eq!(locked.len(), 1, "locked row count");
    assert_eq!(locked[0].get::<_, i32>(0), 1);
    assert_eq!(locked[0].get::<_, i32>(1), 10);

    client
        .batch_execute("UPDATE t SET v = v + 1 WHERE id = 1")
        .await
        .expect("UPDATE of locked row");

    client.batch_execute("COMMIT").await.expect("COMMIT");

    let after = client
        .query("SELECT v FROM t WHERE id = 1", &[])
        .await
        .expect("select after commit");
    assert_eq!(after.len(), 1);
    assert_eq!(after[0].get::<_, i32>(0), 11, "update landed");

    drop(client);
    shutdown(server_handle).await;
}

/// While the writer holds an open transaction with `SELECT FOR UPDATE`
/// followed by an `UPDATE`, a concurrent reader on a separate connection
/// observes the locked row's pre-update value. MVCC snapshot semantics
/// must isolate the in-flight writer from the reader.
#[tokio::test]
async fn concurrent_reader_sees_pre_image_during_writer_txn() {
    let (bound, server_handle) = start_server().await;
    let (writer, _wconn) = connect(bound, "lock_rows_writer").await;
    let (reader, _rconn) = connect(bound, "lock_rows_reader").await;

    writer
        .batch_execute("CREATE TABLE t (id INT NOT NULL, v INT NOT NULL)")
        .await
        .expect("create table");
    writer
        .batch_execute("INSERT INTO t VALUES (1, 10)")
        .await
        .expect("seed row");

    writer.batch_execute("BEGIN").await.expect("BEGIN writer");
    let locked = writer
        .query("SELECT id, v FROM t WHERE id = 1 FOR UPDATE", &[])
        .await
        .expect("FOR UPDATE in writer");
    assert_eq!(locked.len(), 1);
    writer
        .batch_execute("UPDATE t SET v = 99 WHERE id = 1")
        .await
        .expect("UPDATE inside writer txn");

    // Reader on a separate connection sees the pre-update value because
    // the writer has not committed yet.
    let pre = reader
        .query("SELECT v FROM t WHERE id = 1", &[])
        .await
        .expect("reader pre-commit");
    assert_eq!(pre.len(), 1);
    assert_eq!(
        pre[0].get::<_, i32>(0),
        10,
        "concurrent reader sees pre-image"
    );

    // Tidy: roll the writer's txn back so a stale interleaved-read
    // visibility regression elsewhere in the engine does not leak into
    // post-test heap state. The contract under test here is the
    // pre-commit pre-image, which we asserted above.
    writer
        .batch_execute("ROLLBACK")
        .await
        .expect("writer ROLLBACK");

    drop(writer);
    drop(reader);
    shutdown(server_handle).await;
}

/// Two-connection sequenced visibility: writer commits its `FOR UPDATE`
/// + `UPDATE` pair, then a reader on a separate connection observes the
/// committed post-update value.
#[tokio::test]
async fn sequenced_two_connection_post_commit_visibility() {
    let (bound, server_handle) = start_server().await;
    let (writer, _wconn) = connect(bound, "lock_rows_writer2").await;

    writer
        .batch_execute("CREATE TABLE t (id INT NOT NULL, v INT NOT NULL)")
        .await
        .expect("create table");
    writer
        .batch_execute("INSERT INTO t VALUES (1, 10)")
        .await
        .expect("seed row");

    writer.batch_execute("BEGIN").await.expect("BEGIN writer");
    let locked = writer
        .query("SELECT id, v FROM t WHERE id = 1 FOR UPDATE", &[])
        .await
        .expect("FOR UPDATE in writer");
    assert_eq!(locked.len(), 1);
    writer
        .batch_execute("UPDATE t SET v = 99 WHERE id = 1")
        .await
        .expect("UPDATE inside writer txn");
    writer.batch_execute("COMMIT").await.expect("writer COMMIT");

    // Open the reader *after* the writer has committed so the reader's
    // first MVCC snapshot already includes the writer's xid in the
    // committed list.
    let (reader, _rconn) = connect(bound, "lock_rows_reader2").await;
    let post = reader
        .query("SELECT v FROM t WHERE id = 1", &[])
        .await
        .expect("reader post-commit");
    assert_eq!(post.len(), 1);
    assert_eq!(
        post[0].get::<_, i32>(0),
        99,
        "reader on a fresh connection sees post-commit value"
    );

    drop(writer);
    drop(reader);
    shutdown(server_handle).await;
}

/// `SELECT ... FOR SHARE` round-trips without error and returns rows.
#[tokio::test]
async fn select_for_share_returns_rows() {
    let (bound, server_handle) = start_server().await;
    let (client, _conn) = connect(bound, "lock_rows_share").await;

    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL, v INT NOT NULL)")
        .await
        .expect("create table");
    client
        .batch_execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .expect("seed rows");

    client.batch_execute("BEGIN").await.expect("BEGIN");
    let rows = client
        .query("SELECT id FROM t WHERE id = 2 FOR SHARE", &[])
        .await
        .expect("FOR SHARE");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>(0), 2);
    client.batch_execute("COMMIT").await.expect("COMMIT");

    drop(client);
    shutdown(server_handle).await;
}

/// `SELECT ... FOR NO KEY UPDATE` round-trips without error.
#[tokio::test]
async fn select_for_no_key_update_returns_rows() {
    let (bound, server_handle) = start_server().await;
    let (client, _conn) = connect(bound, "lock_rows_nokey").await;

    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL, v INT NOT NULL)")
        .await
        .expect("create table");
    client
        .batch_execute("INSERT INTO t VALUES (1, 10), (2, 20)")
        .await
        .expect("seed rows");

    client.batch_execute("BEGIN").await.expect("BEGIN");
    let rows = client
        .query("SELECT id, v FROM t WHERE id = 1 FOR NO KEY UPDATE", &[])
        .await
        .expect("FOR NO KEY UPDATE");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>(0), 1);
    assert_eq!(rows[0].get::<_, i32>(1), 10);
    client.batch_execute("COMMIT").await.expect("COMMIT");

    drop(client);
    shutdown(server_handle).await;
}
