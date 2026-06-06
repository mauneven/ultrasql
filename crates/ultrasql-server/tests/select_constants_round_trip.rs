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

pub mod support;

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

/// Row-value `IN` compares tuple fields and preserves SQL NULL semantics.
#[tokio::test]
async fn select_where_row_value_in_round_trip() {
    let running = start_sample_server("select_constants_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE row_value_in (id INT NOT NULL, score INT)")
        .await
        .expect("create table");
    client
        .batch_execute(
            "INSERT INTO row_value_in VALUES \
             (1, 42), (2, 3), (3, 10), (4, 7), (5, NULL)",
        )
        .await
        .expect("insert rows");

    let rows = client
        .query(
            "SELECT id FROM row_value_in \
             WHERE (id, score) IN ((1, 42), (3, 10), (5, 0)) \
             ORDER BY id",
            &[],
        )
        .await
        .expect("row-value IN query");
    let ids: Vec<i32> = rows.iter().map(|row| row.get::<_, i32>(0)).collect();
    assert_eq!(ids, vec![1, 3]);

    shutdown(running).await;
}
