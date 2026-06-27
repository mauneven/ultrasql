//! End-to-end `ORDER BY` tests against a real `tokio-postgres` client.
//!
//! Closes the v0.5 P0 wire-protocol gap "Wire ORDER BY" by driving an
//! in-process `ultrasqld` with a stock `tokio-postgres` client and
//! asserting that `SELECT ... ORDER BY ...` returns rows in the order
//! PostgreSQL itself would emit them.
//!
//! Shapes covered:
//!
//! - `ORDER BY val ASC`
//! - `ORDER BY val DESC`
//! - `ORDER BY a ASC, b DESC` (multi-key, secondary breaks ties)
//! - `SELECT col, SUM(val) FROM t GROUP BY col ORDER BY col` (Sort
//!   above `HashAggregate`)
//!
//! NULL-placement semantics (PostgreSQL `ASC` → `NULLS LAST`, `DESC` →
//! `NULLS FIRST`) are exercised end-to-end below by the
//! `indexed_nullable_order_by_*` battery, which inserts real
//! `Value::Null` rows over the wire and asserts that an `ORDER BY` over
//! an *indexed* nullable column returns the exact same row-set and order
//! as the same query with no index. That equality is the regression
//! guard for the silent-row-loss bug where a bare ordered index scan
//! over a nullable column dropped every `NULL` row (the i64 B-tree never
//! stores NULL keys); the fix declines the index-ordered fast path for
//! nullable ordering columns and falls back to the heap `Sort`.
//! Operator-level NULL placement is additionally pinned in the executor
//! unit tests (`crates/ultrasql-executor/src/sort.rs`).
//!
//! Each test creates a fresh table per-server so the assertions don't
//! depend on cross-test ordering.

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
        "host={host} port={port} user=tester application_name=order_by_test",
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

/// `ORDER BY val ASC` returns rows in non-decreasing order of `val`.
#[tokio::test]
async fn order_by_asc_returns_rows_in_ascending_order() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE asc_items (id INT NOT NULL, val INT)")
        .await
        .expect("create table");
    for (id, v) in [(3_i32, 30_i32), (1, 10), (4, 40), (2, 20)] {
        client
            .batch_execute(&format!("INSERT INTO asc_items VALUES ({id}, {v})"))
            .await
            .expect("insert");
    }

    let rows = client
        .simple_query("SELECT id, val FROM asc_items ORDER BY val ASC")
        .await
        .expect("query succeeds");

    let vals: Vec<i32> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(
                row.get(1)
                    .expect("val present")
                    .parse::<i32>()
                    .expect("val parses"),
            ),
            _ => None,
        })
        .collect();
    assert_eq!(vals, vec![10, 20, 30, 40]);

    shutdown(client, server_handle).await;
}

/// Runtime errors in ORDER BY expression keys keep their SQLSTATE.
#[tokio::test]
async fn order_by_runtime_cast_error_returns_22p02() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute(
            "CREATE TABLE order_by_cast_items (id INT NOT NULL, raw TEXT NOT NULL);
             INSERT INTO order_by_cast_items VALUES (1, 'not-int')",
        )
        .await
        .expect("setup");

    let err = client
        .simple_query("SELECT id FROM order_by_cast_items ORDER BY CAST(raw AS INTEGER)")
        .await
        .expect_err("ORDER BY runtime cast rejects row");
    assert_eq!(
        err.code().map(tokio_postgres::error::SqlState::code),
        Some("22P02")
    );

    shutdown(client, server_handle).await;
}

/// `ORDER BY val DESC` returns rows in non-increasing order of `val`.
#[tokio::test]
async fn order_by_desc_returns_rows_in_descending_order() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE desc_items (id INT NOT NULL, val INT)")
        .await
        .expect("create table");
    for (id, v) in [(3_i32, 30_i32), (1, 10), (4, 40), (2, 20)] {
        client
            .batch_execute(&format!("INSERT INTO desc_items VALUES ({id}, {v})"))
            .await
            .expect("insert");
    }

    let rows = client
        .simple_query("SELECT id, val FROM desc_items ORDER BY val DESC")
        .await
        .expect("query succeeds");

    let vals: Vec<i32> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(
                row.get(1)
                    .expect("val present")
                    .parse::<i32>()
                    .expect("val parses"),
            ),
            _ => None,
        })
        .collect();
    assert_eq!(vals, vec![40, 30, 20, 10]);

    shutdown(client, server_handle).await;
}

/// `ORDER BY a ASC, b DESC` orders by `a` ascending and breaks ties on
/// `b` descending — confirms multi-key dispatch end-to-end.
#[tokio::test]
async fn order_by_multi_key_secondary_breaks_ties() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE multi_keys (a INT NOT NULL, b INT NOT NULL)")
        .await
        .expect("create table");
    // a=1, b∈{20,40}; a=2, b∈{10,30}. Expected ORDER BY a ASC, b DESC =>
    // (1,40), (1,20), (2,30), (2,10).
    for (a, b) in [(2_i32, 30_i32), (1, 20), (2, 10), (1, 40)] {
        client
            .batch_execute(&format!("INSERT INTO multi_keys VALUES ({a}, {b})"))
            .await
            .expect("insert");
    }

    let rows = client
        .simple_query("SELECT a, b FROM multi_keys ORDER BY a ASC, b DESC")
        .await
        .expect("query succeeds");

    let pairs: Vec<(i32, i32)> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => {
                let a = row.get(0)?.parse::<i32>().ok()?;
                let b = row.get(1)?.parse::<i32>().ok()?;
                Some((a, b))
            }
            _ => None,
        })
        .collect();
    assert_eq!(pairs, vec![(1, 40), (1, 20), (2, 30), (2, 10)]);

    shutdown(client, server_handle).await;
}

/// `ORDER BY col` over the output of `GROUP BY` confirms the Sort is
/// applied *after* the aggregate. The aggregate produces one row per
/// `col` group; the Sort orders those rows.
#[tokio::test]
async fn order_by_over_group_by_aggregate() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE agg_items (grp INT NOT NULL, val INT NOT NULL)")
        .await
        .expect("create table");
    // Groups inserted out of order; sums per group: 3→6, 1→10, 2→20.
    for (g, v) in [(3_i32, 2_i32), (1, 4), (2, 8), (3, 4), (1, 6), (2, 12)] {
        client
            .batch_execute(&format!("INSERT INTO agg_items VALUES ({g}, {v})"))
            .await
            .expect("insert");
    }

    let rows = client
        .simple_query("SELECT grp, SUM(val) FROM agg_items GROUP BY grp ORDER BY grp ASC")
        .await
        .expect("query succeeds");

    let pairs: Vec<(i32, i64)> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => {
                let g = row.get(0)?.parse::<i32>().ok()?;
                let s = row.get(1)?.parse::<i64>().ok()?;
                Some((g, s))
            }
            _ => None,
        })
        .collect();
    assert_eq!(pairs, vec![(1, 10), (2, 20), (3, 6)]);

    shutdown(client, server_handle).await;
}

/// Built-in collations parse and bind in expression and ORDER BY slots. The
/// current runtime supports bytewise C/POSIX/default behavior; locale/ICU
/// tailoring stays an explicit roadmap item.
#[tokio::test]
async fn order_by_builtin_collate_uses_bytewise_order() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE collate_items (name TEXT NOT NULL)")
        .await
        .expect("create table");
    for value in ["b", "A", "a", "B"] {
        client
            .batch_execute(&format!("INSERT INTO collate_items VALUES ('{value}')"))
            .await
            .expect("insert");
    }

    let rows = client
        .simple_query("SELECT name COLLATE \"C\" FROM collate_items ORDER BY name COLLATE \"C\"")
        .await
        .expect("query succeeds");
    let names: Vec<String> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        })
        .collect();
    assert_eq!(names, vec!["A", "B", "a", "b"]);

    client
        .simple_query("SELECT name COLLATE pg_catalog.\"POSIX\" FROM collate_items LIMIT 1")
        .await
        .expect("schema-qualified POSIX collation binds");
    client
        .simple_query("SELECT name COLLATE default FROM collate_items LIMIT 1")
        .await
        .expect("default collation binds");

    let err = client
        .simple_query("SELECT name COLLATE missing_locale FROM collate_items LIMIT 1")
        .await
        .expect_err("unknown collation rejected");
    let message = err
        .as_db_error()
        .map(tokio_postgres::error::DbError::message)
        .unwrap_or_default();
    assert!(
        message.contains("unsupported collation"),
        "unexpected error: {err}"
    );

    shutdown(client, server_handle).await;
}

// ===========================================================================
// Regression battery: ORDER BY over an indexed *nullable* column must not
// silently drop the rows where the ordering column IS NULL.
//
// Before the fix, the planner lowered `ORDER BY <indexed_col>` to a bare
// ordered B-tree IndexScan. The i64 B-tree never stores NULL keys, so the
// NULL rows were never enumerated and silently vanished from the result —
// and the path also ignored NULLS FIRST/LAST. The fix declines the
// index-ordered fast path when the ordering column is nullable and falls
// back to the heap `Sort`, which enumerates ALL rows (NULLs included) and
// places them per the requested (or PG-default) NULLS clause. A NOT NULL
// ordering column keeps the fast directed index scan (no NULLs to lose).
//
// The seed `{1, NULL, 3, 2, NULL}` is the canonical repro.
// ===========================================================================

/// Read column 0 of a `simple_query` result as `Option<i32>`, where a SQL
/// NULL surfaces as `None`. Preserves result order.
fn col0_nullable_i32(rows: &[tokio_postgres::SimpleQueryMessage]) -> Vec<Option<i32>> {
    rows.iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => {
                Some(row.get(0).map(|s| s.parse::<i32>().expect("int parses")))
            }
            _ => None,
        })
        .collect()
}

/// Seed `t(c INT)` with `{1, NULL, 3, 2, NULL}`. The column is nullable.
async fn seed_nullable_c(client: &tokio_postgres::Client, table: &str) {
    client
        .batch_execute(&format!("CREATE TABLE {table} (c INT)"))
        .await
        .expect("create table");
    client
        .batch_execute(&format!(
            "INSERT INTO {table} (c) VALUES (1), (NULL), (3), (2), (NULL)"
        ))
        .await
        .expect("seed rows");
}

/// 1. `ORDER BY c` over an indexed nullable column returns ALL five rows,
///    NULLS LAST by PG default: 1, 2, 3, NULL, NULL.
#[tokio::test]
async fn indexed_nullable_order_by_asc_keeps_null_rows() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;
    seed_nullable_c(&client, "idx_null_asc").await;
    client
        .batch_execute("CREATE INDEX ix_idx_null_asc_c ON idx_null_asc(c)")
        .await
        .expect("create index");

    let rows = client
        .simple_query("SELECT c FROM idx_null_asc ORDER BY c")
        .await
        .expect("query");
    assert_eq!(
        col0_nullable_i32(&rows),
        vec![Some(1), Some(2), Some(3), None, None],
        "ORDER BY c (indexed, nullable) must keep the two NULL rows, NULLS LAST"
    );

    shutdown(client, server_handle).await;
}

/// 2. `ORDER BY c DESC` over an indexed nullable column: NULLS FIRST by PG
///    default: NULL, NULL, 3, 2, 1.
#[tokio::test]
async fn indexed_nullable_order_by_desc_keeps_null_rows() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;
    seed_nullable_c(&client, "idx_null_desc").await;
    client
        .batch_execute("CREATE INDEX ix_idx_null_desc_c ON idx_null_desc(c)")
        .await
        .expect("create index");

    let rows = client
        .simple_query("SELECT c FROM idx_null_desc ORDER BY c DESC")
        .await
        .expect("query");
    assert_eq!(
        col0_nullable_i32(&rows),
        vec![None, None, Some(3), Some(2), Some(1)],
        "ORDER BY c DESC (indexed) must put NULLs first per PG default"
    );

    shutdown(client, server_handle).await;
}

/// 3. `ORDER BY c ASC NULLS FIRST` (indexed) -> NULL, NULL, 1, 2, 3.
#[tokio::test]
async fn indexed_nullable_order_by_asc_nulls_first() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;
    seed_nullable_c(&client, "idx_null_asc_nf").await;
    client
        .batch_execute("CREATE INDEX ix_idx_null_asc_nf_c ON idx_null_asc_nf(c)")
        .await
        .expect("create index");

    let rows = client
        .simple_query("SELECT c FROM idx_null_asc_nf ORDER BY c ASC NULLS FIRST")
        .await
        .expect("query");
    assert_eq!(
        col0_nullable_i32(&rows),
        vec![None, None, Some(1), Some(2), Some(3)],
    );

    shutdown(client, server_handle).await;
}

/// 4. `ORDER BY c DESC NULLS LAST` (indexed) -> 3, 2, 1, NULL, NULL.
#[tokio::test]
async fn indexed_nullable_order_by_desc_nulls_last() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;
    seed_nullable_c(&client, "idx_null_desc_nl").await;
    client
        .batch_execute("CREATE INDEX ix_idx_null_desc_nl_c ON idx_null_desc_nl(c)")
        .await
        .expect("create index");

    let rows = client
        .simple_query("SELECT c FROM idx_null_desc_nl ORDER BY c DESC NULLS LAST")
        .await
        .expect("query");
    assert_eq!(
        col0_nullable_i32(&rows),
        vec![Some(3), Some(2), Some(1), None, None],
    );

    shutdown(client, server_handle).await;
}

/// 5. THE KEY ASSERTION: for an indexed nullable column the result row-set
///    AND order equal the result with NO index — for ASC, DESC, and both
///    explicit NULLS clauses. Drives the same seed twice: once indexed,
///    once after dropping the index, and compares.
#[tokio::test]
async fn indexed_nullable_order_by_equals_non_indexed_for_every_clause() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;
    seed_nullable_c(&client, "idx_null_eq").await;

    let queries = [
        "SELECT c FROM idx_null_eq ORDER BY c",
        "SELECT c FROM idx_null_eq ORDER BY c DESC",
        "SELECT c FROM idx_null_eq ORDER BY c ASC NULLS FIRST",
        "SELECT c FROM idx_null_eq ORDER BY c DESC NULLS LAST",
        "SELECT c FROM idx_null_eq ORDER BY c ASC NULLS LAST",
        "SELECT c FROM idx_null_eq ORDER BY c DESC NULLS FIRST",
    ];

    // Baseline: no index (heap Sort).
    let mut baseline = Vec::new();
    for q in queries {
        let rows = client.simple_query(q).await.expect("baseline query");
        baseline.push(col0_nullable_i32(&rows));
    }

    // Now build the index and re-run the identical queries.
    client
        .batch_execute("CREATE INDEX ix_idx_null_eq_c ON idx_null_eq(c)")
        .await
        .expect("create index");
    for (q, expected) in queries.into_iter().zip(baseline) {
        let rows = client.simple_query(q).await.expect("indexed query");
        assert_eq!(
            col0_nullable_i32(&rows),
            expected,
            "indexed result for `{q}` must equal the non-indexed result exactly"
        );
        // The indexed result must also contain all five rows (no silent loss).
        assert_eq!(col0_nullable_i32(&rows).len(), 5, "indexed `{q}` lost rows");
    }

    shutdown(client, server_handle).await;
}

/// 6. A NOT NULL indexed column (PRIMARY KEY) keeps the fast ordered index
///    scan and returns correct order. Operator-level proof that the fast
///    path is preserved lives in the lowerer unit tests
///    (`pipeline::tests::modify_index`); here we assert the behavioural
///    result is correct.
#[tokio::test]
async fn not_null_pk_indexed_order_by_is_correct() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;
    client
        .batch_execute("CREATE TABLE pk_order (id INT PRIMARY KEY)")
        .await
        .expect("create table");
    client
        .batch_execute("INSERT INTO pk_order (id) VALUES (3), (1), (4), (2)")
        .await
        .expect("seed");

    let asc = client
        .simple_query("SELECT id FROM pk_order ORDER BY id")
        .await
        .expect("asc query");
    assert_eq!(
        col0_nullable_i32(&asc),
        vec![Some(1), Some(2), Some(3), Some(4)]
    );

    let desc = client
        .simple_query("SELECT id FROM pk_order ORDER BY id DESC")
        .await
        .expect("desc query");
    assert_eq!(
        col0_nullable_i32(&desc),
        vec![Some(4), Some(3), Some(2), Some(1)]
    );

    shutdown(client, server_handle).await;
}

/// 7. ORDER BY indexed nullable + LIMIT: a NULL must be able to appear in
///    the LIMIT window. `ORDER BY c LIMIT 4` -> 1, 2, 3, NULL (4th is NULL);
///    `ORDER BY c DESC LIMIT 2` -> NULL, NULL.
#[tokio::test]
async fn indexed_nullable_order_by_with_limit_includes_nulls() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;
    seed_nullable_c(&client, "idx_null_limit").await;
    client
        .batch_execute("CREATE INDEX ix_idx_null_limit_c ON idx_null_limit(c)")
        .await
        .expect("create index");

    let asc4 = client
        .simple_query("SELECT c FROM idx_null_limit ORDER BY c LIMIT 4")
        .await
        .expect("asc limit query");
    assert_eq!(
        col0_nullable_i32(&asc4),
        vec![Some(1), Some(2), Some(3), None],
        "ORDER BY c LIMIT 4 must include the first NULL as the 4th row"
    );

    let desc2 = client
        .simple_query("SELECT c FROM idx_null_limit ORDER BY c DESC LIMIT 2")
        .await
        .expect("desc limit query");
    assert_eq!(
        col0_nullable_i32(&desc2),
        vec![None, None],
        "ORDER BY c DESC LIMIT 2 must return the two NULL rows (NULLS FIRST)"
    );

    shutdown(client, server_handle).await;
}

/// 8. Control: a predicate index scan is unaffected by the fix. `WHERE c > 1`
///    over the indexed nullable column returns 2, 3 — NULLs are correctly
///    excluded by the predicate (three-valued logic), not by the bug.
#[tokio::test]
async fn predicate_index_scan_excludes_nulls_by_predicate() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;
    seed_nullable_c(&client, "idx_null_pred").await;
    client
        .batch_execute("CREATE INDEX ix_idx_null_pred_c ON idx_null_pred(c)")
        .await
        .expect("create index");

    let rows = client
        .simple_query("SELECT c FROM idx_null_pred WHERE c > 1 ORDER BY c")
        .await
        .expect("query");
    assert_eq!(
        col0_nullable_i32(&rows),
        vec![Some(2), Some(3)],
        "WHERE c > 1 excludes NULLs by predicate, not by row loss"
    );

    shutdown(client, server_handle).await;
}

/// 9. A multi-column index whose first column is nullable, with `ORDER BY`
///    on that first column, must still return every row. This path is not
///    served by the single-column ordered index scan (the lowerer only
///    matches single-column int B-trees), so it falls back to `Sort` and
///    enumerates the NULLs regardless. Asserted indexed == non-indexed.
#[tokio::test]
async fn multi_column_index_nullable_leading_order_by_keeps_nulls() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;
    client
        .batch_execute("CREATE TABLE multi_idx_null (a INT, b INT)")
        .await
        .expect("create table");
    client
        .batch_execute(
            "INSERT INTO multi_idx_null (a, b) VALUES (1, 10), (NULL, 20), (3, 30), (2, 40), (NULL, 50)",
        )
        .await
        .expect("seed");

    // Baseline (no index).
    let baseline = col0_nullable_i32(
        &client
            .simple_query("SELECT a FROM multi_idx_null ORDER BY a")
            .await
            .expect("baseline"),
    );
    assert_eq!(
        baseline,
        vec![Some(1), Some(2), Some(3), None, None],
        "baseline must already place NULLs last and keep all rows"
    );

    client
        .batch_execute("CREATE INDEX ix_multi_idx_null_ab ON multi_idx_null(a, b)")
        .await
        .expect("create multi-column index");

    let indexed = col0_nullable_i32(
        &client
            .simple_query("SELECT a FROM multi_idx_null ORDER BY a")
            .await
            .expect("indexed"),
    );
    assert_eq!(
        indexed, baseline,
        "multi-column-index ORDER BY on a nullable leading column must keep all rows"
    );

    shutdown(client, server_handle).await;
}
