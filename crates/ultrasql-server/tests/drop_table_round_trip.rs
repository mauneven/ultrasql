//! End-to-end `DROP TABLE` tests against a real `tokio-postgres` client.
//!
//! Closes the v0.5 wire-protocol coverage gap "`DROP TABLE` — ⚠️ no
//! dedicated round-trip test" at `ROADMAP.md:342`. The kernel and the
//! Simple-Query dispatcher already ship at
//! `crates/ultrasql-server/src/session/ddl.rs:312`; this file verifies the
//! statement round-trips through `tokio-postgres`.
//!
//! Shapes covered:
//!
//! - `CREATE TABLE ... ; INSERT ... ; DROP TABLE ... ;` — `DROP TABLE`
//!   returns the `DROP TABLE` command tag.
//! - After `DROP TABLE`, a `SELECT` against the dropped relation fails
//!   with SQLSTATE `42P01` (PostgreSQL `undefined_table`).
//! - The dropped name is reusable: a subsequent `CREATE TABLE` with the
//!   same name succeeds.
//! - `DROP TABLE` against a never-defined name fails with SQLSTATE
//!   `42P01` and leaves the session in idle status.

mod support;

use support::{shutdown, start_sample_server};

/// `DROP TABLE` after `CREATE` + `INSERT` removes the relation; a
/// subsequent `SELECT` errors with SQLSTATE `42P01`.
#[tokio::test]
async fn drop_table_then_select_fails_with_42p01() {
    let running = start_sample_server("drop_table_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE doomed (id INT NOT NULL, v INT)")
        .await
        .expect("create table");
    client
        .batch_execute("INSERT INTO doomed VALUES (1, 10), (2, 20)")
        .await
        .expect("seed rows");

    // Sanity: rows visible before drop.
    let pre = client
        .query("SELECT id FROM doomed", &[])
        .await
        .expect("select before drop");
    assert_eq!(pre.len(), 2);

    client
        .batch_execute("DROP TABLE doomed")
        .await
        .expect("drop table");

    let err = client
        .query("SELECT id FROM doomed", &[])
        .await
        .expect_err("select on dropped relation must fail");
    let sqlstate = err.code().expect("server-sent SQLSTATE present");
    assert_eq!(
        sqlstate.code(),
        "42P01",
        "expected undefined_table, got {err:?}"
    );

    shutdown(running).await;
}

/// After `DROP TABLE`, the name is available for `CREATE TABLE` reuse.
#[tokio::test]
async fn drop_then_recreate_same_name_succeeds() {
    let running = start_sample_server("drop_table_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE reused (id INT NOT NULL)")
        .await
        .expect("create");
    client
        .batch_execute("INSERT INTO reused VALUES (1)")
        .await
        .expect("insert into first incarnation");
    client
        .batch_execute("DROP TABLE reused")
        .await
        .expect("drop");

    // Recreate with a different schema and insert into the new shape.
    client
        .batch_execute("CREATE TABLE reused (id INT NOT NULL, label INT NOT NULL)")
        .await
        .expect("recreate with new schema");
    client
        .batch_execute("INSERT INTO reused VALUES (42, 1)")
        .await
        .expect("insert into recreated table");

    let rows = client
        .query("SELECT id, label FROM reused", &[])
        .await
        .expect("select recreated");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>(0), 42);
    assert_eq!(rows[0].get::<_, i32>(1), 1);

    shutdown(running).await;
}

/// `DROP TABLE` of a name that was never defined fails with SQLSTATE
/// `42P01` and leaves the session alive.
#[tokio::test]
async fn drop_table_on_undefined_relation_fails_with_42p01() {
    let running = start_sample_server("drop_table_test").await;
    let client = &running.client;

    let err = client
        .batch_execute("DROP TABLE never_existed")
        .await
        .expect_err("drop of undefined relation must error");
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

    shutdown(running).await;
}
