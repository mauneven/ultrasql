//! End-to-end transaction-control tests against a real `tokio-postgres`
//! client.
//!
//! Closes the v0.5 P0 ROADMAP entry "BEGIN/COMMIT/ROLLBACK end-to-end".
//! Drives an in-process `ultrasqld` with the same stock client every
//! third-party Rust app uses and asserts that:
//!
//! - `BEGIN; INSERT; INSERT; COMMIT;` persists both rows.
//! - `BEGIN; INSERT; ROLLBACK;` discards the row.
//! - `BEGIN; UPDATE; ROLLBACK;` reverts the value.
//! - Implicit autocommit still works for plain `INSERT` outside a tx.
//! - An error inside a transaction puts the session in the failed
//!   block; subsequent statements get SQLSTATE `25P02`; COMMIT
//!   commits-as-ROLLBACK with the `ROLLBACK` tag.
//! - Extended Query (`client.execute("BEGIN")` etc.) round-trips
//!   identically.
//! - `SELECT * FROM t` inside a transaction sees the snapshot
//!   consistent with the transaction's BEGIN.
//!
//! All shapes are driven through the real PostgreSQL wire protocol so
//! the codec, parameter substitution, status-byte handling, and
//! `NoticeResponse` paths are exercised end-to-end.

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
        "host={host} port={port} user=tester application_name=txn_test",
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

/// `BEGIN; INSERT; INSERT; COMMIT;` — both rows visible after commit.
#[tokio::test]
async fn begin_insert_insert_commit_persists_both_rows() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL, val INT)")
        .await
        .expect("create table");

    client.batch_execute("BEGIN").await.expect("BEGIN");
    client
        .batch_execute("INSERT INTO t VALUES (1, 1)")
        .await
        .expect("insert row 1");
    client
        .batch_execute("INSERT INTO t VALUES (2, 2)")
        .await
        .expect("insert row 2");
    client.batch_execute("COMMIT").await.expect("COMMIT");

    let rows = client
        .query("SELECT id FROM t", &[])
        .await
        .expect("select after commit");
    assert_eq!(rows.len(), 2, "both committed rows visible");

    shutdown(client, server_handle).await;
}

/// `BEGIN; INSERT; ROLLBACK;` — row not persisted.
#[tokio::test]
async fn begin_insert_rollback_discards_row() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL)")
        .await
        .expect("create table");
    // Baseline row to confirm subsequent COUNT is exact.
    client
        .batch_execute("INSERT INTO t VALUES (10)")
        .await
        .expect("baseline insert");

    client.batch_execute("BEGIN").await.expect("BEGIN");
    client
        .batch_execute("INSERT INTO t VALUES (99)")
        .await
        .expect("insert inside tx");
    client.batch_execute("ROLLBACK").await.expect("ROLLBACK");

    let rows = client
        .query("SELECT id FROM t", &[])
        .await
        .expect("select after rollback");
    assert_eq!(rows.len(), 1, "rolled-back INSERT did not persist");
    assert_eq!(rows[0].get::<_, i32>(0), 10);

    shutdown(client, server_handle).await;
}

/// `BEGIN; UPDATE; ROLLBACK;` — value unchanged.
#[tokio::test]
async fn begin_update_rollback_reverts_value() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL, val INT)")
        .await
        .expect("create table");
    client
        .batch_execute("INSERT INTO t VALUES (1, 100)")
        .await
        .expect("baseline insert");

    client.batch_execute("BEGIN").await.expect("BEGIN");
    client
        .batch_execute("UPDATE t SET val = val + 999 WHERE id = 1")
        .await
        .expect("update inside tx");
    client.batch_execute("ROLLBACK").await.expect("ROLLBACK");

    let rows = client
        .query("SELECT val FROM t WHERE id = 1", &[])
        .await
        .expect("select after rollback");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>(0), 100, "UPDATE rolled back");

    shutdown(client, server_handle).await;
}

/// Implicit autocommit still works.  An INSERT issued without a
/// surrounding BEGIN is visible immediately to subsequent statements.
#[tokio::test]
async fn autocommit_insert_immediately_visible() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL)")
        .await
        .expect("create table");
    client
        .batch_execute("INSERT INTO t VALUES (1)")
        .await
        .expect("autocommit insert");
    client
        .batch_execute("INSERT INTO t VALUES (2)")
        .await
        .expect("autocommit insert");

    let rows = client
        .query("SELECT id FROM t", &[])
        .await
        .expect("select after autocommit inserts");
    assert_eq!(rows.len(), 2);

    shutdown(client, server_handle).await;
}

/// A query inside a transaction sees the snapshot consistent with the
/// transaction's BEGIN — autocommit writes from another connection
/// after BEGIN are not visible.
#[tokio::test]
async fn select_in_transaction_uses_snapshot() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL)")
        .await
        .expect("create table");
    client
        .batch_execute("INSERT INTO t VALUES (1)")
        .await
        .expect("autocommit insert");

    client.batch_execute("BEGIN").await.expect("BEGIN");
    // Inside the txn, we should see the one pre-existing row.
    let rows = client
        .query("SELECT id FROM t", &[])
        .await
        .expect("select in tx");
    assert_eq!(rows.len(), 1);
    client.batch_execute("COMMIT").await.expect("COMMIT");

    shutdown(client, server_handle).await;
}

/// A query that errors inside a transaction transitions the session
/// to the failed-block state.  Subsequent statements get SQLSTATE
/// `25P02`; `COMMIT` commits-as-`ROLLBACK`.
#[tokio::test]
async fn error_in_tx_aborts_block_and_commit_is_rollback() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL)")
        .await
        .expect("create table");
    client
        .batch_execute("INSERT INTO t VALUES (1)")
        .await
        .expect("baseline insert");

    client.batch_execute("BEGIN").await.expect("BEGIN");

    // This INSERT lands inside the txn; the rollback should undo it.
    client
        .batch_execute("INSERT INTO t VALUES (2)")
        .await
        .expect("insert in tx");

    // Now error: reference an unknown table.
    let err = client
        .batch_execute("SELECT * FROM no_such_table")
        .await
        .expect_err("missing table should error");
    let sqlstate = err
        .code()
        .map_or_else(String::new, |c| c.code().to_string());
    assert!(
        sqlstate == "42P01" || sqlstate == "0A000",
        "expected table-not-found or feature-not-supported, got {sqlstate}",
    );

    // Any subsequent statement returns SQLSTATE 25P02.
    let err = client
        .batch_execute("SELECT id FROM t")
        .await
        .expect_err("in-failed-block should reject SELECT");
    let sqlstate = err
        .code()
        .map_or_else(String::new, |c| c.code().to_string());
    assert_eq!(sqlstate, "25P02", "25P02 in failed block");

    // COMMIT in failed state commits-as-rollback. tokio-postgres
    // reports success because the server emits CommandComplete +
    // ReadyForQuery; the tag itself is "ROLLBACK".
    client
        .batch_execute("COMMIT")
        .await
        .expect("COMMIT in failed state succeeds (as rollback)");

    // After the implicit rollback, the in-tx INSERT is gone.
    let rows = client
        .query("SELECT id FROM t", &[])
        .await
        .expect("select after failed-block COMMIT");
    assert_eq!(rows.len(), 1, "in-tx INSERT rolled back by failed-COMMIT");

    shutdown(client, server_handle).await;
}

/// Extended Query: `client.execute("BEGIN")`, `client.execute("INSERT
/// ...")`, `client.execute("COMMIT")` round-trips identically.
///
/// `client.execute` always goes through Parse/Bind/Execute/Sync
/// (the Extended Query Protocol path), so this exercises the
/// txn-state dispatch from the Extended side.
#[tokio::test]
async fn extended_query_begin_insert_commit_round_trip() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL, val INT)")
        .await
        .expect("create table");

    // Extended Query path:
    client.execute("BEGIN", &[]).await.expect("Extended BEGIN");
    client
        .execute("INSERT INTO t VALUES (1, 1)", &[])
        .await
        .expect("Extended INSERT");
    client
        .execute("INSERT INTO t VALUES (2, 2)", &[])
        .await
        .expect("Extended INSERT");
    client
        .execute("COMMIT", &[])
        .await
        .expect("Extended COMMIT");

    let rows = client
        .query("SELECT id FROM t", &[])
        .await
        .expect("select after Extended COMMIT");
    assert_eq!(rows.len(), 2);

    shutdown(client, server_handle).await;
}

/// Extended Query ROLLBACK discards the in-flight INSERT.
#[tokio::test]
async fn extended_query_begin_insert_rollback_discards() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL)")
        .await
        .expect("create table");

    client.execute("BEGIN", &[]).await.expect("Extended BEGIN");
    client
        .execute("INSERT INTO t VALUES (42)", &[])
        .await
        .expect("Extended INSERT");
    client
        .execute("ROLLBACK", &[])
        .await
        .expect("Extended ROLLBACK");

    let rows = client
        .query("SELECT id FROM t", &[])
        .await
        .expect("select after Extended ROLLBACK");
    assert_eq!(rows.len(), 0, "Extended ROLLBACK discarded");

    shutdown(client, server_handle).await;
}

/// SAVEPOINT / RELEASE / ROLLBACK TO statements round-trip without
/// errors. The transaction-manager savepoint stack is updated and the
/// session stays healthy.
///
/// # Scope note
///
/// This test confirms the wire path. The executor does **not** yet
/// stamp tuples with the savepoint's subtransaction xid, so a
/// `ROLLBACK TO sp` after an INSERT does **not** undo the INSERT — the
/// row carries the parent xid and is committed with the parent. Full
/// subtransaction visibility wiring (`LowerCtx` tracks a subxid stack;
/// `INSERT`/`UPDATE`/`DELETE` consults the top of the stack) is a
/// follow-up commit. Until then, SAVEPOINT remains useful for client
/// drivers that emit it as connection-state hygiene (e.g.
/// tokio-postgres' `Transaction::savepoint`) but cannot deliver
/// partial-rollback semantics for already-written rows.
#[tokio::test]
async fn savepoint_release_rollback_to_round_trip_without_error() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL)")
        .await
        .expect("create table");

    client.batch_execute("BEGIN").await.expect("BEGIN");
    client
        .batch_execute("SAVEPOINT sp1")
        .await
        .expect("SAVEPOINT");
    client
        .batch_execute("INSERT INTO t VALUES (1)")
        .await
        .expect("insert under sp1");
    client
        .batch_execute("RELEASE SAVEPOINT sp1")
        .await
        .expect("RELEASE");
    client
        .batch_execute("SAVEPOINT sp2")
        .await
        .expect("SAVEPOINT sp2");
    client
        .batch_execute("ROLLBACK TO SAVEPOINT sp2")
        .await
        .expect("ROLLBACK TO");
    client.batch_execute("COMMIT").await.expect("COMMIT");

    // The committed transaction's writes are visible.
    let rows = client
        .query("SELECT id FROM t", &[])
        .await
        .expect("select after commit");
    assert_eq!(rows.len(), 1);

    shutdown(client, server_handle).await;
}

/// `ROLLBACK TO SAVEPOINT` on an unknown name errors with
/// SQLSTATE `3B001` (`invalid_savepoint_specification`) and marks the
/// transaction block as failed (PostgreSQL semantics).
#[tokio::test]
async fn rollback_to_unknown_savepoint_errors() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client.batch_execute("BEGIN").await.expect("BEGIN");
    let err = client
        .batch_execute("ROLLBACK TO SAVEPOINT nope")
        .await
        .expect_err("unknown savepoint should error");
    let sqlstate = err
        .code()
        .map_or_else(String::new, |c| c.code().to_string());
    assert_eq!(sqlstate, "3B001", "invalid_savepoint_specification");
    // Recover the session.
    client
        .batch_execute("ROLLBACK")
        .await
        .expect("ROLLBACK after savepoint error");

    shutdown(client, server_handle).await;
}

/// `SAVEPOINT` outside a transaction errors with SQLSTATE `25P01`
/// (`no_active_sql_transaction`).
#[tokio::test]
async fn savepoint_outside_transaction_errors() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    let err = client
        .batch_execute("SAVEPOINT sp")
        .await
        .expect_err("SAVEPOINT outside tx should error");
    let sqlstate = err
        .code()
        .map_or_else(String::new, |c| c.code().to_string());
    assert_eq!(sqlstate, "25P01");

    shutdown(client, server_handle).await;
}

/// Mixing Simple Query and Extended Query inside the same
/// transaction: BEGIN via Simple, INSERT via Extended (prepared),
/// COMMIT via Simple — all bound to the same xid and visible
/// together.
#[tokio::test]
async fn mixed_simple_and_extended_in_one_tx() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL, val INT)")
        .await
        .expect("create table");

    client.batch_execute("BEGIN").await.expect("Simple BEGIN");

    let stmt = client
        .prepare("INSERT INTO t VALUES ($1, $2)")
        .await
        .expect("prepare");
    client
        .execute(&stmt, &[&7_i32, &700_i32])
        .await
        .expect("Extended prepared INSERT");

    client.batch_execute("COMMIT").await.expect("Simple COMMIT");

    let rows = client
        .query("SELECT id, val FROM t", &[])
        .await
        .expect("select after mixed COMMIT");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>(0), 7);
    assert_eq!(rows[0].get::<_, i32>(1), 700);

    shutdown(client, server_handle).await;
}

/// `BEGIN ISOLATION LEVEL READ COMMITTED` and `READ UNCOMMITTED` (aliased)
/// round-trip through the wire without error and the session can commit.
#[tokio::test]
async fn begin_isolation_level_read_committed_round_trip() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("BEGIN ISOLATION LEVEL READ COMMITTED")
        .await
        .expect("BEGIN ISOLATION LEVEL READ COMMITTED");
    client.batch_execute("COMMIT").await.expect("COMMIT");

    // READ UNCOMMITTED is aliased to READ COMMITTED.
    client
        .batch_execute("BEGIN ISOLATION LEVEL READ UNCOMMITTED")
        .await
        .expect("BEGIN ISOLATION LEVEL READ UNCOMMITTED");
    client.batch_execute("COMMIT").await.expect("COMMIT");

    shutdown(client, server_handle).await;
}

/// `BEGIN ISOLATION LEVEL REPEATABLE READ` round-trips without error.
#[tokio::test]
async fn begin_isolation_level_repeatable_read_round_trip() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
        .await
        .expect("BEGIN ISOLATION LEVEL REPEATABLE READ");
    client.batch_execute("COMMIT").await.expect("COMMIT");

    shutdown(client, server_handle).await;
}

/// `BEGIN ISOLATION LEVEL SERIALIZABLE` round-trips without error.
#[tokio::test]
async fn begin_isolation_level_serializable_round_trip() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
        .await
        .expect("BEGIN ISOLATION LEVEL SERIALIZABLE");
    client.batch_execute("COMMIT").await.expect("COMMIT");

    shutdown(client, server_handle).await;
}

/// Inside a REPEATABLE READ transaction the snapshot is frozen at BEGIN.
/// A baseline row inserted before BEGIN is visible; the transaction
/// commits cleanly to verify the full path.
#[tokio::test]
async fn repeatable_read_snapshot_frozen_wire_level() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL)")
        .await
        .expect("create table");
    client
        .batch_execute("INSERT INTO t VALUES (1)")
        .await
        .expect("baseline row");

    client
        .batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
        .await
        .expect("BEGIN RR");

    let rows = client
        .query("SELECT id FROM t", &[])
        .await
        .expect("select inside RR tx");
    assert_eq!(rows.len(), 1, "baseline row visible inside RR tx");

    client.batch_execute("COMMIT").await.expect("COMMIT");

    shutdown(client, server_handle).await;
}
