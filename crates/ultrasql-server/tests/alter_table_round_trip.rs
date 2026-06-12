//! End-to-end `ALTER TABLE` tests against a real `tokio-postgres` client.
//!
//! Closes the v0.5 wire-protocol coverage gap "`ALTER TABLE` — ⚠️ no
//! dedicated round-trip test" at `ROADMAP.md:343`. The Simple-Query
//! dispatcher routes `ALTER TABLE ... ADD COLUMN` through
//! `crates/ultrasql-server/src/session/alter.rs:107`; this file verifies
//! the statement round-trips through `tokio-postgres` and that the
//! schema mutation is observable on subsequent queries.
//!
//! Shapes covered:
//!
//! - `ALTER TABLE ... ADD COLUMN c TYPE` happy path: existing rows get
//!   NULL for the new column; new rows can provide a value.
//! - Repeated `ALTER TABLE ADD COLUMN` cumulates: schema grows.
//! - `ALTER TABLE` against an undefined relation fails with SQLSTATE
//!   `42P01` and the session survives.

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
        "host={host} port={port} user=tester application_name=alter_table_test",
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

/// `ALTER TABLE ADD COLUMN` extends the schema; pre-existing rows
/// receive NULL for the new column, new rows can carry a non-NULL
/// value.
#[tokio::test]
async fn alter_table_add_column_extends_schema_and_back_fills_null() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL, v INT NOT NULL)")
        .await
        .expect("create");
    client
        .batch_execute("INSERT INTO t VALUES (1, 10), (2, 20)")
        .await
        .expect("seed pre-alter rows");

    client
        .batch_execute("ALTER TABLE t ADD COLUMN c INT")
        .await
        .expect("ALTER ADD COLUMN");

    // Pre-existing rows have NULL for c.
    let rows = client
        .query("SELECT id, v, c FROM t", &[])
        .await
        .expect("select after alter");
    assert_eq!(rows.len(), 2);
    for row in &rows {
        let c: Option<i32> = row.get(2);
        assert!(c.is_none(), "pre-alter row has NULL c, got {c:?}");
    }

    // New rows can specify a value for the new column.
    client
        .batch_execute("INSERT INTO t VALUES (3, 30, 999)")
        .await
        .expect("insert with new column");
    let all = client
        .query("SELECT id, v, c FROM t WHERE id = 3", &[])
        .await
        .expect("select new row");
    assert_eq!(all.len(), 1);
    assert_eq!(all[0].get::<_, i32>(0), 3);
    assert_eq!(all[0].get::<_, i32>(1), 30);
    assert_eq!(all[0].get::<_, Option<i32>>(2), Some(999));

    shutdown(client, server_handle).await;
}

/// `ALTER TABLE ADD COLUMN c TYPE NOT NULL` cannot backfill existing
/// rows with NULL; reject before changing the schema.
#[tokio::test]
async fn alter_table_add_not_null_column_rejects_non_empty_table() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL, v INT NOT NULL)")
        .await
        .expect("create");
    client
        .batch_execute("INSERT INTO t VALUES (1, 10), (2, 20)")
        .await
        .expect("seed pre-alter rows");

    let err = client
        .batch_execute("ALTER TABLE t ADD COLUMN c INT NOT NULL")
        .await
        .expect_err("NOT NULL column without a backfill must be rejected");
    assert_eq!(err.code().expect("SQLSTATE").code(), "23502");

    let rows = client
        .query("SELECT id, v FROM t ORDER BY id", &[])
        .await
        .expect("original table remains queryable");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, i32>(0), 1);
    assert_eq!(rows[1].get::<_, i32>(0), 2);

    let err = client
        .query("SELECT c FROM t", &[])
        .await
        .expect_err("failed ALTER must not install column");
    assert_eq!(err.code().expect("SQLSTATE").code(), "42703");

    shutdown(client, server_handle).await;
}

/// Unsupported `ADD COLUMN` constraints must fail closed instead of
/// being silently discarded by the binder.
#[tokio::test]
async fn alter_table_add_column_rejects_unsupported_constraints() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL)")
        .await
        .expect("create");

    let err = client
        .batch_execute("ALTER TABLE t ADD COLUMN c INT CHECK (c > 0)")
        .await
        .expect_err("unsupported ADD COLUMN CHECK must be rejected");
    assert_eq!(err.code().expect("SQLSTATE").code(), "0A000");

    let err = client
        .query("SELECT c FROM t", &[])
        .await
        .expect_err("failed ALTER must not install column");
    assert_eq!(err.code().expect("SQLSTATE").code(), "42703");

    shutdown(client, server_handle).await;
}

/// Two `ALTER TABLE ADD COLUMN` statements stack: the schema grows by
/// each addition and earlier columns are unaffected.
#[tokio::test]
async fn alter_table_add_column_stacks_repeatedly() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL)")
        .await
        .expect("create");
    client
        .batch_execute("INSERT INTO t VALUES (1), (2)")
        .await
        .expect("seed");

    client
        .batch_execute("ALTER TABLE t ADD COLUMN a INT")
        .await
        .expect("ALTER ADD COLUMN a");
    client
        .batch_execute("ALTER TABLE t ADD COLUMN b INT")
        .await
        .expect("ALTER ADD COLUMN b");

    let rows = client
        .query("SELECT id, a, b FROM t", &[])
        .await
        .expect("select after two alters");
    assert_eq!(rows.len(), 2);
    for row in &rows {
        let a: Option<i32> = row.get(1);
        let b: Option<i32> = row.get(2);
        assert!(a.is_none());
        assert!(b.is_none());
    }

    shutdown(client, server_handle).await;
}

/// `ALTER TABLE` on a name that does not resolve fails with SQLSTATE
/// `42P01` and leaves the session live.
#[tokio::test]
async fn alter_table_on_undefined_relation_fails_with_42p01() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    let err = client
        .batch_execute("ALTER TABLE no_such_table ADD COLUMN x INT")
        .await
        .expect_err("alter of undefined relation errors");
    let sqlstate = err.code().expect("server-sent SQLSTATE present");
    assert_eq!(
        sqlstate.code(),
        "42P01",
        "expected undefined_table, got {err:?}"
    );

    // Session still functional.
    client
        .batch_execute("CREATE TABLE alive (id INT NOT NULL)")
        .await
        .expect("session survives prior error");

    shutdown(client, server_handle).await;
}

/// `ALTER TABLE t DROP COLUMN c`: tuples are rewritten without the
/// dropped slot; subsequent SELECTs return the narrower row.
#[tokio::test]
async fn alter_table_drop_column_rewrites_existing_rows() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL, val INT NOT NULL, note INT NOT NULL)")
        .await
        .expect("create");
    client
        .batch_execute("INSERT INTO t VALUES (1, 10, 100), (2, 20, 200)")
        .await
        .expect("seed");

    client
        .batch_execute("ALTER TABLE t DROP COLUMN val")
        .await
        .expect("drop column");

    let rows = client
        .query("SELECT id, note FROM t ORDER BY id", &[])
        .await
        .expect("select after drop");
    assert_eq!(rows.len(), 2);
    let r0_id: i32 = rows[0].get(0);
    let r0_note: i32 = rows[0].get(1);
    let r1_id: i32 = rows[1].get(0);
    let r1_note: i32 = rows[1].get(1);
    assert_eq!((r0_id, r0_note), (1, 100));
    assert_eq!((r1_id, r1_note), (2, 200));

    // Referencing the dropped column now errors at bind time.
    let err = client
        .query("SELECT val FROM t", &[])
        .await
        .expect_err("dropped column is unreachable");
    let sqlstate = err.code().expect("sqlstate present");
    assert!(
        matches!(sqlstate.code(), "42703" | "42000"),
        "expected undefined_column, got {err:?}"
    );

    shutdown(client, server_handle).await;
}

/// `ALTER TABLE t RENAME COLUMN old TO new`: the new name resolves
/// to the same data; the old name is gone.
#[tokio::test]
async fn alter_table_rename_column_preserves_data() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL, val INT NOT NULL)")
        .await
        .expect("create");
    client
        .batch_execute("INSERT INTO t VALUES (1, 99)")
        .await
        .expect("seed");

    client
        .batch_execute("ALTER TABLE t RENAME COLUMN val TO score")
        .await
        .expect("rename column");

    let rows = client
        .query("SELECT id, score FROM t", &[])
        .await
        .expect("select via new name");
    assert_eq!(rows.len(), 1);
    let id: i32 = rows[0].get(0);
    let score: i32 = rows[0].get(1);
    assert_eq!((id, score), (1, 99));

    shutdown(client, server_handle).await;
}

/// `ALTER TABLE t RENAME TO new_name`: the table is reachable under
/// the new name and gone under the old name.
#[tokio::test]
async fn alter_table_rename_table_swaps_name() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL)")
        .await
        .expect("create");
    client
        .batch_execute("INSERT INTO t VALUES (42)")
        .await
        .expect("seed");

    client
        .batch_execute("ALTER TABLE t RENAME TO t_new")
        .await
        .expect("rename table");

    let rows = client
        .query("SELECT id FROM t_new", &[])
        .await
        .expect("select from new name");
    assert_eq!(rows.len(), 1);
    let id: i32 = rows[0].get(0);
    assert_eq!(id, 42);

    let err = client
        .query("SELECT id FROM t", &[])
        .await
        .expect_err("old name no longer resolves");
    let sqlstate = err.code().expect("sqlstate present");
    assert_eq!(sqlstate.code(), "42P01");

    shutdown(client, server_handle).await;
}
