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

mod support;

use support::{shutdown, start_sample_server};

/// `SELECT 1` returns one row with one int column.
#[tokio::test]
async fn select_one_round_trip() {
    let running = start_sample_server("select_constants_test").await;
    let client = &running.client;

    let rows = client.query("SELECT 1", &[]).await.expect("SELECT 1");
    assert_eq!(rows.len(), 1, "single-row result expected");
    assert_eq!(rows[0].get::<_, i32>(0), 1);

    shutdown(running).await;
}

/// `SELECT 1, 2, 3` returns one row with three int columns in order.
#[tokio::test]
async fn select_multi_constant_round_trip() {
    let running = start_sample_server("select_constants_test").await;
    let client = &running.client;

    let rows = client
        .query("SELECT 1, 2, 3", &[])
        .await
        .expect("SELECT multi-const");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>(0), 1);
    assert_eq!(rows[0].get::<_, i32>(1), 2);
    assert_eq!(rows[0].get::<_, i32>(2), 3);

    shutdown(running).await;
}

/// `SELECT … WHERE col IS NULL` filters out non-NULL rows.
#[tokio::test]
async fn select_where_is_null_round_trip() {
    let running = start_sample_server("select_constants_test").await;
    let client = &running.client;

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

    shutdown(running).await;
}

/// `SELECT … WHERE col IS NOT NULL` keeps non-NULL rows only.
#[tokio::test]
async fn select_where_is_not_null_round_trip() {
    let running = start_sample_server("select_constants_test").await;
    let client = &running.client;

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

    shutdown(running).await;
}
