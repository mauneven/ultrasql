//! End-to-end tests for FROM-less `SELECT` and the `IS NULL` predicate.
//!
//! Closes two v0.5 open gaps:
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

// ===========================================================================
// Constant-only projection over a base table: `SELECT 1 FROM t` and friends.
//
// Regression battery for the WRONG-RESULT bug where a projection of only
// constant expressions sitting directly over a base-table scan (no intervening
// Filter) returned 0 rows instead of count(t). Root cause: ProjectionPushdown
// narrowed the scan to a zero-column projection, and a zero-column batch is
// structurally forced to zero rows. The fix retains one cheap scan column so
// the row count survives; the row-count assertions below are the gate.
// ===========================================================================

/// Seed a fresh two-column table `t` with `N = 5` rows (ids 1..=5).
async fn seed_t(client: &tokio_postgres::Client) {
    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL, label TEXT)")
        .await
        .expect("create t");
    client
        .batch_execute(
            "INSERT INTO t VALUES \
             (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')",
        )
        .await
        .expect("seed t");
}

/// (1) `SELECT 1 FROM t` -> 5 rows, each value 1.
#[tokio::test]
async fn select_const_int_from_table_returns_all_rows() {
    let running = start_sample_server("select_constants_test").await;
    let client = &running.client;
    seed_t(client).await;

    let rows = client
        .query("SELECT 1 FROM t", &[])
        .await
        .expect("SELECT 1 FROM t");
    assert_eq!(rows.len(), 5, "one constant row per base-table row");
    for row in &rows {
        assert_eq!(row.get::<_, i32>(0), 1);
    }

    shutdown(running).await;
}

/// (2) `SELECT 'x' FROM t` -> 5 rows.
#[tokio::test]
async fn select_const_text_from_table_returns_all_rows() {
    let running = start_sample_server("select_constants_test").await;
    let client = &running.client;
    seed_t(client).await;

    let rows = client
        .query("SELECT 'x' FROM t", &[])
        .await
        .expect("SELECT 'x' FROM t");
    assert_eq!(rows.len(), 5);
    for row in &rows {
        assert_eq!(row.get::<_, &str>(0), "x");
    }

    shutdown(running).await;
}

/// (3) `SELECT now() FROM t` -> 5 rows, all equal (txn-stable now()).
#[tokio::test]
async fn select_now_from_table_returns_all_rows() {
    let running = start_sample_server("select_constants_test").await;
    let client = &running.client;
    seed_t(client).await;

    let rows = client
        .query("SELECT now() FROM t", &[])
        .await
        .expect("SELECT now() FROM t");
    assert_eq!(rows.len(), 5, "one now() row per base-table row");
    let stamps: Vec<std::time::SystemTime> = rows
        .iter()
        .map(|r| r.get::<_, std::time::SystemTime>(0))
        .collect();
    assert!(
        stamps.windows(2).all(|w| w[0] == w[1]),
        "now() is constant across rows of one statement"
    );

    shutdown(running).await;
}

/// (4) `SELECT 1 FROM t LIMIT 1` -> 1 row (existence check).
#[tokio::test]
async fn select_const_from_table_limit_one_is_existence_check() {
    let running = start_sample_server("select_constants_test").await;
    let client = &running.client;
    seed_t(client).await;

    let rows = client
        .query("SELECT 1 FROM t LIMIT 1", &[])
        .await
        .expect("SELECT 1 FROM t LIMIT 1");
    assert_eq!(rows.len(), 1, "existence check returns exactly one row");
    assert_eq!(rows[0].get::<_, i32>(0), 1);

    shutdown(running).await;
}

/// (5) `SELECT 1 FROM t LIMIT 3` -> 3 rows.
#[tokio::test]
async fn select_const_from_table_limit_three() {
    let running = start_sample_server("select_constants_test").await;
    let client = &running.client;
    seed_t(client).await;

    let rows = client
        .query("SELECT 1 FROM t LIMIT 3", &[])
        .await
        .expect("SELECT 1 FROM t LIMIT 3");
    assert_eq!(rows.len(), 3);

    shutdown(running).await;
}

/// (6) `SELECT count(*) FROM (SELECT 1 FROM t) s` -> 1 row, value 5.
#[tokio::test]
async fn count_over_constant_subquery_counts_base_rows() {
    let running = start_sample_server("select_constants_test").await;
    let client = &running.client;
    seed_t(client).await;

    let row = client
        .query_one("SELECT count(*) FROM (SELECT 1 FROM t) s", &[])
        .await
        .expect("count over constant subquery");
    assert_eq!(row.get::<_, i64>(0), 5);

    shutdown(running).await;
}

/// (7) `SELECT id, 1 FROM t` -> 5 rows (mixed projection control).
#[tokio::test]
async fn select_mixed_column_and_const_returns_all_rows() {
    let running = start_sample_server("select_constants_test").await;
    let client = &running.client;
    seed_t(client).await;

    let rows = client
        .query("SELECT id, 1 FROM t ORDER BY id", &[])
        .await
        .expect("SELECT id, 1 FROM t");
    assert_eq!(rows.len(), 5);
    for (i, row) in rows.iter().enumerate() {
        assert_eq!(row.get::<_, i32>(0), i32::try_from(i).unwrap() + 1);
        assert_eq!(row.get::<_, i32>(1), 1);
    }

    shutdown(running).await;
}

/// (8) `SELECT 1 FROM t WHERE …` -> Filter path control (5 vs 0 rows).
#[tokio::test]
async fn select_const_from_table_with_filter_control() {
    let running = start_sample_server("select_constants_test").await;
    let client = &running.client;
    seed_t(client).await;

    let all = client
        .query("SELECT 1 FROM t WHERE id > 0", &[])
        .await
        .expect("WHERE id > 0");
    assert_eq!(all.len(), 5);

    let none = client
        .query("SELECT 1 FROM t WHERE id > 1000", &[])
        .await
        .expect("WHERE id > 1000");
    assert_eq!(none.len(), 0);

    shutdown(running).await;
}

/// (9) `SELECT count(*) FROM t` -> 1 row value 5 (Aggregate control, NOT 5 rows).
#[tokio::test]
async fn select_count_star_aggregate_control() {
    let running = start_sample_server("select_constants_test").await;
    let client = &running.client;
    seed_t(client).await;

    let rows = client
        .query("SELECT count(*) FROM t", &[])
        .await
        .expect("SELECT count(*) FROM t");
    assert_eq!(rows.len(), 1, "aggregate collapses to a single row");
    assert_eq!(rows[0].get::<_, i64>(0), 5);

    shutdown(running).await;
}

/// (10) `INSERT INTO dst SELECT 1 FROM t` -> 5 rows in dst (control).
#[tokio::test]
async fn insert_select_const_from_table_inserts_all_rows() {
    let running = start_sample_server("select_constants_test").await;
    let client = &running.client;
    seed_t(client).await;
    client
        .batch_execute("CREATE TABLE dst (v INT NOT NULL)")
        .await
        .expect("create dst");

    let inserted = client
        .execute("INSERT INTO dst SELECT 1 FROM t", &[])
        .await
        .expect("INSERT … SELECT 1 FROM t");
    assert_eq!(inserted, 5, "one inserted row per base-table row");

    let row = client
        .query_one("SELECT count(*) FROM dst", &[])
        .await
        .expect("count dst");
    assert_eq!(row.get::<_, i64>(0), 5);

    shutdown(running).await;
}

/// (11) `SELECT EXISTS(SELECT 1 FROM t)` -> true; over an empty table -> false.
#[tokio::test]
async fn exists_constant_subquery_reflects_table_population() {
    let running = start_sample_server("select_constants_test").await;
    let client = &running.client;
    seed_t(client).await;
    client
        .batch_execute("CREATE TABLE empty_t (id INT NOT NULL)")
        .await
        .expect("create empty_t");

    let populated = client
        .query_one("SELECT EXISTS(SELECT 1 FROM t)", &[])
        .await
        .expect("EXISTS over populated table");
    assert!(populated.get::<_, bool>(0), "table t has rows");

    let empty = client
        .query_one("SELECT EXISTS(SELECT 1 FROM empty_t)", &[])
        .await
        .expect("EXISTS over empty table");
    assert!(!empty.get::<_, bool>(0), "empty_t has no rows");

    shutdown(running).await;
}

/// (12) Constant projection over a self-JOIN -> correct row count (not 0).
#[tokio::test]
async fn select_const_over_join_returns_correct_row_count() {
    let running = start_sample_server("select_constants_test").await;
    let client = &running.client;
    seed_t(client).await;

    // Each of the 5 ids in `a` matches exactly one row in `b` -> 5 rows.
    let rows = client
        .query("SELECT 1 FROM t a JOIN t b ON a.id = b.id", &[])
        .await
        .expect("constant over self-join");
    assert_eq!(rows.len(), 5);
    for row in &rows {
        assert_eq!(row.get::<_, i32>(0), 1);
    }

    shutdown(running).await;
}

/// (13) `SELECT 1 FROM t` on a single-column table -> 5 rows (edge: do not
/// prune to an empty projection on a 1-column table).
#[tokio::test]
async fn select_const_from_single_column_table_returns_all_rows() {
    let running = start_sample_server("select_constants_test").await;
    let client = &running.client;
    client
        .batch_execute("CREATE TABLE single_col (id INT NOT NULL)")
        .await
        .expect("create single_col");
    client
        .batch_execute("INSERT INTO single_col VALUES (1), (2), (3), (4), (5)")
        .await
        .expect("seed single_col");

    let rows = client
        .query("SELECT 1 FROM single_col", &[])
        .await
        .expect("SELECT 1 FROM single_col");
    assert_eq!(rows.len(), 5);
    for row in &rows {
        assert_eq!(row.get::<_, i32>(0), 1);
    }

    shutdown(running).await;
}
