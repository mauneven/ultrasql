//! End-to-end `INSERT INTO t SELECT ...` tests against a real
//! `tokio-postgres` client.
//!
//! Closes the v0.5 wire-protocol gap "`INSERT … SELECT`
//! (`pipeline.rs:1314` returns `Unsupported`)" at `ROADMAP.md:319`. The
//! binder already produced `LogicalPlan::Insert { source: Select … }`;
//! `lower_real_insert` now lowers the inner SELECT through
//! `lower_query` and drives `ModifyTable::Insert` off its batches.
//!
//! Shapes covered:
//!
//! - `INSERT INTO dst SELECT a, b FROM src WHERE a > N` — predicate
//!   filtered, full copy of the matching rows.
//! - `INSERT INTO dst SELECT a, b FROM src` — no predicate, full
//!   copy.
//! - `INSERT INTO dst (b, a) SELECT a, b FROM src` — explicit target
//!   column order maps source positions correctly.
//! - Idempotence: two `INSERT … SELECT` statements double the row
//!   count.
//! - Schema arity mismatch is rejected before any heap write.

use std::collections::HashSet;
mod support;

use support::{shutdown, start_sample_server};

/// `INSERT INTO dst SELECT a, b FROM src WHERE a > 100` copies the
/// rows that satisfy the predicate into the destination relation.
#[tokio::test]
async fn insert_select_with_predicate_copies_filtered_rows() {
    let running = start_sample_server("insert_select_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE src (a INT NOT NULL, b INT NOT NULL)")
        .await
        .expect("create src");
    client
        .batch_execute("CREATE TABLE dst (a INT NOT NULL, b INT NOT NULL)")
        .await
        .expect("create dst");

    client
        .batch_execute(
            "INSERT INTO src VALUES \
             (50, 5), (100, 10), (150, 15), (200, 20), (250, 25)",
        )
        .await
        .expect("seed src");

    client
        .batch_execute("INSERT INTO dst SELECT a, b FROM src WHERE a > 100")
        .await
        .expect("INSERT INTO dst SELECT");

    let rows = client
        .query("SELECT a, b FROM dst", &[])
        .await
        .expect("select dst");
    let values: HashSet<(i32, i32)> = rows
        .iter()
        .map(|r| (r.get::<_, i32>(0), r.get::<_, i32>(1)))
        .collect();
    assert_eq!(values, HashSet::from([(150, 15), (200, 20), (250, 25)]));

    shutdown(running).await;
}

/// `INSERT INTO dst SELECT a, b FROM src` (no WHERE) copies every row.
#[tokio::test]
async fn insert_select_without_predicate_copies_all_rows() {
    let running = start_sample_server("insert_select_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE src (a INT NOT NULL, b INT NOT NULL)")
        .await
        .expect("create src");
    client
        .batch_execute("CREATE TABLE dst (a INT NOT NULL, b INT NOT NULL)")
        .await
        .expect("create dst");

    client
        .batch_execute("INSERT INTO src VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .expect("seed src");

    client
        .batch_execute("INSERT INTO dst SELECT a, b FROM src")
        .await
        .expect("INSERT INTO dst SELECT");

    let rows = client
        .query("SELECT a, b FROM dst", &[])
        .await
        .expect("select dst");
    let values: HashSet<(i32, i32)> = rows
        .iter()
        .map(|r| (r.get::<_, i32>(0), r.get::<_, i32>(1)))
        .collect();
    assert_eq!(values, HashSet::from([(1, 10), (2, 20), (3, 30)]));

    shutdown(running).await;
}

/// Two `INSERT INTO dst SELECT …` statements double the destination's
/// row count — verifies the path isn't a one-shot.
#[tokio::test]
async fn insert_select_runs_idempotently_twice() {
    let running = start_sample_server("insert_select_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE src (a INT NOT NULL, b INT NOT NULL)")
        .await
        .expect("create src");
    client
        .batch_execute("CREATE TABLE dst (a INT NOT NULL, b INT NOT NULL)")
        .await
        .expect("create dst");

    client
        .batch_execute("INSERT INTO src VALUES (1, 10), (2, 20)")
        .await
        .expect("seed src");

    client
        .batch_execute("INSERT INTO dst SELECT a, b FROM src")
        .await
        .expect("first INSERT … SELECT");
    client
        .batch_execute("INSERT INTO dst SELECT a, b FROM src")
        .await
        .expect("second INSERT … SELECT");

    let rows = client
        .query("SELECT a FROM dst", &[])
        .await
        .expect("select dst");
    assert_eq!(rows.len(), 4, "two SELECTs land 2 + 2 = 4 rows");

    shutdown(running).await;
}

/// Explicit destination columns map source positions to target
/// columns, just like VALUES inserts.
#[tokio::test]
async fn insert_select_respects_target_column_order() {
    let running = start_sample_server("insert_select_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE src (x INT NOT NULL, y INT NOT NULL)")
        .await
        .expect("create src");
    client
        .batch_execute("CREATE TABLE dst (a INT NOT NULL, b INT NOT NULL)")
        .await
        .expect("create dst");
    client
        .batch_execute("INSERT INTO src VALUES (7, 70)")
        .await
        .expect("seed src");

    client
        .batch_execute("INSERT INTO dst (b, a) SELECT x, y FROM src")
        .await
        .expect("INSERT SELECT with target column order");

    let rows = client
        .query("SELECT a, b FROM dst", &[])
        .await
        .expect("select dst");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>(0), 70);
    assert_eq!(rows[0].get::<_, i32>(1), 7);

    shutdown(running).await;
}

/// `INSERT … SELECT` with a column-count mismatch must be rejected
/// before any tuple lands in the heap.
#[tokio::test]
async fn insert_select_arity_mismatch_is_rejected() {
    let running = start_sample_server("insert_select_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE src (a INT NOT NULL, b INT NOT NULL, c INT NOT NULL)")
        .await
        .expect("create src");
    client
        .batch_execute("CREATE TABLE dst (a INT NOT NULL, b INT NOT NULL)")
        .await
        .expect("create dst");
    client
        .batch_execute("INSERT INTO src VALUES (1, 2, 3)")
        .await
        .expect("seed src");

    let err = client
        .batch_execute("INSERT INTO dst SELECT a, b, c FROM src")
        .await
        .expect_err("arity mismatch must error");
    let db_err = err
        .as_db_error()
        .expect("server-sent ErrorResponse for arity mismatch");
    assert!(
        db_err.message().to_ascii_lowercase().contains("insert"),
        "expected INSERT-related error message, got {:?}",
        db_err.message()
    );

    // Destination still empty: no partial write should have leaked
    // through.
    let post = client
        .query("SELECT a FROM dst", &[])
        .await
        .expect("select dst");
    assert!(post.is_empty(), "rejected INSERT must not leak rows");

    shutdown(running).await;
}
