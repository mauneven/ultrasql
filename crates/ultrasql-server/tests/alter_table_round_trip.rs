//! End-to-end `ALTER TABLE` tests against a real `tokio-postgres` client.
//!
//! Closes the v0.5 wire-protocol coverage gap "`ALTER TABLE` — ⚠️ no
//! dedicated round-trip test" (tracked in `TODO.md`). The Simple-Query
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

// ALTER COLUMN SET/DROP NOT NULL and SET/DROP DEFAULT
// ------------------------------------------------------------------------

/// `ALTER COLUMN ... SET NOT NULL` on a column with no NULLs succeeds;
/// afterwards a NULL INSERT and a NULL UPDATE are both rejected `23502`.
#[tokio::test]
async fn alter_column_set_not_null_enforces_on_dml() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL, v INT)")
        .await
        .expect("create");
    client
        .batch_execute("INSERT INTO t VALUES (1, 10), (2, 20)")
        .await
        .expect("seed non-null rows");

    client
        .batch_execute("ALTER TABLE t ALTER COLUMN v SET NOT NULL")
        .await
        .expect("SET NOT NULL on column with no NULLs");

    let err = client
        .batch_execute("INSERT INTO t VALUES (3, NULL)")
        .await
        .expect_err("NULL insert rejected after SET NOT NULL");
    assert_eq!(err.code().expect("SQLSTATE").code(), "23502");

    let err = client
        .batch_execute("UPDATE t SET v = NULL WHERE id = 1")
        .await
        .expect_err("NULL update rejected after SET NOT NULL");
    assert_eq!(err.code().expect("SQLSTATE").code(), "23502");

    // The original rows are untouched and a non-NULL insert still works.
    client
        .batch_execute("INSERT INTO t VALUES (3, 30)")
        .await
        .expect("non-null insert still works");
    let rows = client
        .query("SELECT id FROM t ORDER BY id", &[])
        .await
        .expect("select after enforcement");
    assert_eq!(rows.len(), 3);

    shutdown(client, server_handle).await;
}

/// `ALTER COLUMN ... SET NOT NULL` on a column that already contains a
/// NULL row is rejected `23502` and leaves the column nullable.
#[tokio::test]
async fn alter_column_set_not_null_rejects_existing_null() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL, v INT)")
        .await
        .expect("create");
    client
        .batch_execute("INSERT INTO t VALUES (1, 10), (2, NULL)")
        .await
        .expect("seed with a NULL row");

    let err = client
        .batch_execute("ALTER TABLE t ALTER COLUMN v SET NOT NULL")
        .await
        .expect_err("SET NOT NULL must reject pre-existing NULL");
    assert_eq!(err.code().expect("SQLSTATE").code(), "23502");

    // The column stays nullable: a NULL insert still succeeds.
    client
        .batch_execute("INSERT INTO t VALUES (3, NULL)")
        .await
        .expect("column remains nullable after rejected SET NOT NULL");
    let rows = client
        .query("SELECT id FROM t WHERE v IS NULL ORDER BY id", &[])
        .await
        .expect("select NULL rows");
    assert_eq!(rows.len(), 2);

    shutdown(client, server_handle).await;
}

/// `ALTER COLUMN ... DROP NOT NULL` allows NULLs afterwards; on a PRIMARY
/// KEY column it is rejected `42P16`.
#[tokio::test]
async fn alter_column_drop_not_null_and_pk_rejection() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (id INT PRIMARY KEY, v INT NOT NULL)")
        .await
        .expect("create with PK and NOT NULL column");

    // DROP NOT NULL on a regular NOT NULL column: NULLs allowed after.
    client
        .batch_execute("ALTER TABLE t ALTER COLUMN v DROP NOT NULL")
        .await
        .expect("DROP NOT NULL on a regular column");
    client
        .batch_execute("INSERT INTO t VALUES (1, NULL)")
        .await
        .expect("NULL insert allowed after DROP NOT NULL");

    // DROP NOT NULL on a PRIMARY KEY column is rejected.
    let err = client
        .batch_execute("ALTER TABLE t ALTER COLUMN id DROP NOT NULL")
        .await
        .expect_err("DROP NOT NULL on a PK column must be rejected");
    assert_eq!(err.code().expect("SQLSTATE").code(), "42P16");

    // The PK column stays NOT NULL: a NULL id insert still fails.
    let err = client
        .batch_execute("INSERT INTO t VALUES (NULL, 5)")
        .await
        .expect_err("PK column stays NOT NULL");
    assert_eq!(err.code().expect("SQLSTATE").code(), "23502");

    shutdown(client, server_handle).await;
}

/// `ALTER COLUMN ... SET DEFAULT` applies to new inserts that omit the
/// column; existing rows are unchanged; a later change takes effect for
/// new inserts only.
#[tokio::test]
async fn alter_column_set_default_applies_to_new_inserts_only() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL, v INT)")
        .await
        .expect("create");
    client
        .batch_execute("INSERT INTO t (id) VALUES (1)")
        .await
        .expect("seed row with no default -> NULL");

    client
        .batch_execute("ALTER TABLE t ALTER COLUMN v SET DEFAULT 7")
        .await
        .expect("SET DEFAULT 7");
    client
        .batch_execute("INSERT INTO t (id) VALUES (2)")
        .await
        .expect("insert omitting v uses default");

    // Existing row 1 keeps NULL; new row 2 gets 7.
    let row1 = client
        .query("SELECT v FROM t WHERE id = 1", &[])
        .await
        .expect("select existing row");
    assert_eq!(row1[0].get::<_, Option<i32>>(0), None);
    let row2 = client
        .query("SELECT v FROM t WHERE id = 2", &[])
        .await
        .expect("select new row");
    assert_eq!(row2[0].get::<_, Option<i32>>(0), Some(7));

    // Changing the default again affects only subsequent inserts.
    client
        .batch_execute("ALTER TABLE t ALTER COLUMN v SET DEFAULT 9")
        .await
        .expect("SET DEFAULT 9");
    client
        .batch_execute("INSERT INTO t (id) VALUES (3)")
        .await
        .expect("insert omitting v uses new default");
    let row3 = client
        .query("SELECT v FROM t WHERE id = 3", &[])
        .await
        .expect("select newest row");
    assert_eq!(row3[0].get::<_, Option<i32>>(0), Some(9));
    // Row 2 still has the old default value 7.
    let row2 = client
        .query("SELECT v FROM t WHERE id = 2", &[])
        .await
        .expect("re-select row 2");
    assert_eq!(row2[0].get::<_, Option<i32>>(0), Some(7));

    shutdown(client, server_handle).await;
}

/// `ALTER COLUMN ... DROP DEFAULT` makes omitting the column yield NULL,
/// or `23502` when the column is also NOT NULL.
#[tokio::test]
async fn alter_column_drop_default_yields_null_or_violates_not_null() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL, v INT DEFAULT 5)")
        .await
        .expect("create with a column default");

    client
        .batch_execute("ALTER TABLE t ALTER COLUMN v DROP DEFAULT")
        .await
        .expect("DROP DEFAULT");
    client
        .batch_execute("INSERT INTO t (id) VALUES (1)")
        .await
        .expect("insert omitting v yields NULL after DROP DEFAULT");
    let row = client
        .query("SELECT v FROM t WHERE id = 1", &[])
        .await
        .expect("select row");
    assert_eq!(row[0].get::<_, Option<i32>>(0), None);

    // With a NOT NULL column and no default, omitting it violates 23502.
    client
        .batch_execute("ALTER TABLE t ALTER COLUMN v SET DEFAULT 5")
        .await
        .expect("restore default");
    client
        .batch_execute("DELETE FROM t")
        .await
        .expect("clear rows so SET NOT NULL succeeds");
    client
        .batch_execute("ALTER TABLE t ALTER COLUMN v SET NOT NULL")
        .await
        .expect("SET NOT NULL");
    client
        .batch_execute("ALTER TABLE t ALTER COLUMN v DROP DEFAULT")
        .await
        .expect("DROP DEFAULT on NOT NULL column");
    let err = client
        .batch_execute("INSERT INTO t (id) VALUES (2)")
        .await
        .expect_err("omitting a NOT NULL column with no default violates 23502");
    assert_eq!(err.code().expect("SQLSTATE").code(), "23502");

    shutdown(client, server_handle).await;
}

/// `ALTER COLUMN ... SET NOT NULL` against an undefined column fails
/// `42703` and the session survives.
#[tokio::test]
async fn alter_column_undefined_column_errors() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL)")
        .await
        .expect("create");

    let err = client
        .batch_execute("ALTER TABLE t ALTER COLUMN nope SET NOT NULL")
        .await
        .expect_err("undefined column rejected");
    assert_eq!(err.code().expect("SQLSTATE").code(), "42703");

    // Session survives.
    let rows = client
        .query("SELECT id FROM t", &[])
        .await
        .expect("session survives undefined-column error");
    assert_eq!(rows.len(), 0);

    shutdown(client, server_handle).await;
}
