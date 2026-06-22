//! End-to-end `SELECT DISTINCT` and `SELECT DISTINCT ON (...)` tests
//! against a real `tokio-postgres` client.
//!
//! Closes the v0.5 wire-protocol gap "`Unique` operator — kernel
//! exists; DISTINCT wire path pending" (tracked in `TODO.md`) and the v0.6
//! "Hash-based DISTINCT vs Sort-based DISTINCT" item. The binder now
//! lowers `SELECT DISTINCT` into a `LogicalPlan::Aggregate` with the
//! projected columns as group keys and an empty aggregate list; the
//! existing `HashAggregate` operator then deduplicates.
//!
//! `SELECT DISTINCT ON (e1, …)` lowers into
//! `Project(DistinctOn(Sort(...)))`: the `Sort` orders by the ON keys
//! followed by the rest of `ORDER BY`, and the `DistinctOn` operator emits
//! the first row of each ON-key group. The `DISTINCT ON` tests below cover
//! the PostgreSQL semantics: latest-per-group, multi-key grouping, ON keys
//! not in the select list, NULL grouping, the 42P10 prefix rule, no-ORDER-BY
//! determinism, and the LIMIT interaction.

use std::collections::HashSet;
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
        "host={host} port={port} user=tester application_name=distinct_test",
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

/// `SELECT DISTINCT a FROM t` returns every distinct value of `a`
/// exactly once.
#[tokio::test]
async fn select_distinct_single_column_dedups() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)")
        .await
        .expect("create");
    client
        .batch_execute("INSERT INTO t VALUES (1, 10), (1, 11), (2, 20), (2, 22), (3, 30)")
        .await
        .expect("seed");

    let rows = client
        .query("SELECT DISTINCT a FROM t", &[])
        .await
        .expect("select distinct a");
    let values: HashSet<i32> = rows.iter().map(|r| r.get::<_, i32>(0)).collect();
    assert_eq!(values, HashSet::from([1, 2, 3]));

    shutdown(client, server_handle).await;
}

/// `SELECT DISTINCT a, b FROM t` returns every distinct `(a, b)` pair
/// exactly once.
#[tokio::test]
async fn select_distinct_two_columns_dedups_pair() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)")
        .await
        .expect("create");
    client
        .batch_execute(
            "INSERT INTO t VALUES \
             (1, 10), (1, 10), (1, 11), (2, 20), (2, 20), (3, 30), (3, 30), (3, 30)",
        )
        .await
        .expect("seed");

    let rows = client
        .query("SELECT DISTINCT a, b FROM t", &[])
        .await
        .expect("select distinct a, b");
    let values: HashSet<(i32, i32)> = rows
        .iter()
        .map(|r| (r.get::<_, i32>(0), r.get::<_, i32>(1)))
        .collect();
    let expected: HashSet<(i32, i32)> = HashSet::from([(1, 10), (1, 11), (2, 20), (3, 30)]);
    assert_eq!(values, expected);

    shutdown(client, server_handle).await;
}

/// `SELECT DISTINCT` over a table with no duplicates simply returns
/// every row.
#[tokio::test]
async fn select_distinct_with_no_duplicates_returns_all_rows() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (a INT NOT NULL)")
        .await
        .expect("create");
    client
        .batch_execute("INSERT INTO t VALUES (1), (2), (3), (4)")
        .await
        .expect("seed");

    let rows = client
        .query("SELECT DISTINCT a FROM t", &[])
        .await
        .expect("select distinct a");
    let values: HashSet<i32> = rows.iter().map(|r| r.get::<_, i32>(0)).collect();
    assert_eq!(values, HashSet::from([1, 2, 3, 4]));

    shutdown(client, server_handle).await;
}

/// The canonical "latest per group": one row per `customer_id`, the one
/// with the greatest `ts`. The ON key matches the leading ORDER BY key, so
/// the row kept per group is the first under `ORDER BY customer_id, ts DESC`.
#[tokio::test]
async fn distinct_on_latest_per_group() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute(
            "CREATE TABLE orders (customer_id INT NOT NULL, order_id INT NOT NULL, ts INT NOT NULL)",
        )
        .await
        .expect("create");
    client
        .batch_execute(
            "INSERT INTO orders VALUES \
             (1, 100, 5), (1, 101, 9), (1, 102, 3), \
             (2, 200, 1), (2, 201, 7), \
             (3, 300, 4)",
        )
        .await
        .expect("seed");

    let rows = client
        .query(
            "SELECT DISTINCT ON (customer_id) customer_id, order_id, ts \
             FROM orders ORDER BY customer_id, ts DESC",
            &[],
        )
        .await
        .expect("distinct on customer_id");
    let got: Vec<(i32, i32, i32)> = rows
        .iter()
        .map(|r| (r.get::<_, i32>(0), r.get::<_, i32>(1), r.get::<_, i32>(2)))
        .collect();
    // One row per customer, the latest ts: customer 1 -> order 101 (ts 9),
    // customer 2 -> order 201 (ts 7), customer 3 -> order 300 (ts 4).
    assert_eq!(got, vec![(1, 101, 9), (2, 201, 7), (3, 300, 4)]);

    shutdown(client, server_handle).await;
}

/// `DISTINCT ON (a) ... ORDER BY a, b` keeps the first row per `a` by `b`.
#[tokio::test]
async fn distinct_on_first_per_group_by_secondary() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)")
        .await
        .expect("create");
    client
        .batch_execute("INSERT INTO t VALUES (1, 30), (1, 10), (1, 20), (2, 5), (2, 9)")
        .await
        .expect("seed");

    let rows = client
        .query("SELECT DISTINCT ON (a) a, b FROM t ORDER BY a, b", &[])
        .await
        .expect("distinct on a order by a, b");
    let got: Vec<(i32, i32)> = rows
        .iter()
        .map(|r| (r.get::<_, i32>(0), r.get::<_, i32>(1)))
        .collect();
    assert_eq!(got, vec![(1, 10), (2, 5)]);

    shutdown(client, server_handle).await;
}

/// `DISTINCT ON (a, b)` groups by the two-key tuple; one row per `(a, b)`.
#[tokio::test]
async fn distinct_on_multiple_keys() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (a INT NOT NULL, b INT NOT NULL, c INT NOT NULL)")
        .await
        .expect("create");
    client
        .batch_execute(
            "INSERT INTO t VALUES \
             (1, 1, 100), (1, 1, 50), (1, 2, 70), (1, 2, 60), (2, 1, 10)",
        )
        .await
        .expect("seed");

    let rows = client
        .query(
            "SELECT DISTINCT ON (a, b) a, b, c FROM t ORDER BY a, b, c",
            &[],
        )
        .await
        .expect("distinct on a, b");
    let got: Vec<(i32, i32, i32)> = rows
        .iter()
        .map(|r| (r.get::<_, i32>(0), r.get::<_, i32>(1), r.get::<_, i32>(2)))
        .collect();
    // First c per (a, b) under ORDER BY a, b, c.
    assert_eq!(got, vec![(1, 1, 50), (1, 2, 60), (2, 1, 10)]);

    shutdown(client, server_handle).await;
}

/// The ON expression need not appear in the select list. Group by `a`
/// while projecting only `b`.
#[tokio::test]
async fn distinct_on_key_not_in_select_list() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)")
        .await
        .expect("create");
    client
        .batch_execute("INSERT INTO t VALUES (1, 70), (1, 10), (2, 40), (2, 20), (3, 5)")
        .await
        .expect("seed");

    let rows = client
        .query("SELECT DISTINCT ON (a) b FROM t ORDER BY a, b", &[])
        .await
        .expect("distinct on a projecting b");
    let got: Vec<i32> = rows.iter().map(|r| r.get::<_, i32>(0)).collect();
    // First b per a: a=1 -> 10, a=2 -> 20, a=3 -> 5.
    assert_eq!(got, vec![10, 20, 5]);

    shutdown(client, server_handle).await;
}

/// NULLs on the ON key group together (NULL is treated as equal to NULL,
/// matching `IS NOT DISTINCT FROM`); exactly one NULL-key row is emitted.
#[tokio::test]
async fn distinct_on_nulls_group_together() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (a INT, b INT NOT NULL)")
        .await
        .expect("create");
    // Two NULL-key rows plus two non-NULL groups.
    client
        .batch_execute("INSERT INTO t VALUES (NULL, 1), (NULL, 2), (1, 10), (1, 11), (2, 20)")
        .await
        .expect("seed");

    let rows = client
        .query("SELECT DISTINCT ON (a) a, b FROM t ORDER BY a, b", &[])
        .await
        .expect("distinct on nullable key");
    let got: Vec<(Option<i32>, i32)> = rows
        .iter()
        .map(|r| (r.get::<_, Option<i32>>(0), r.get::<_, i32>(1)))
        .collect();
    // ORDER BY a defaults to NULLS LAST, so non-NULL groups come first,
    // then the single NULL group (first b = 1).
    assert_eq!(got, vec![(Some(1), 10), (Some(2), 20), (None, 1)]);

    shutdown(client, server_handle).await;
}

/// When `ORDER BY` does not start with the `DISTINCT ON` expressions,
/// PostgreSQL raises SQLSTATE 42P10. We must reject, not return wrong rows.
#[tokio::test]
async fn distinct_on_non_prefix_order_by_is_42p10() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)")
        .await
        .expect("create");
    client
        .batch_execute("INSERT INTO t VALUES (1, 10), (2, 20)")
        .await
        .expect("seed");

    let err = client
        .query("SELECT DISTINCT ON (a) a, b FROM t ORDER BY b", &[])
        .await
        .expect_err("non-prefix ORDER BY must be rejected");
    let db_err = err
        .as_db_error()
        .expect("server-sent ErrorResponse for 42P10");
    assert_eq!(db_err.code().code(), "42P10", "expected SQLSTATE 42P10");
    assert!(
        db_err
            .message()
            .contains("DISTINCT ON expressions must match initial ORDER BY"),
        "unexpected message: {:?}",
        db_err.message()
    );

    shutdown(client, server_handle).await;
}

/// `DISTINCT ON` without `ORDER BY` is allowed: one row per group. We sort
/// by the ON keys for a deterministic result and never error.
#[tokio::test]
async fn distinct_on_without_order_by_one_row_per_group() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)")
        .await
        .expect("create");
    client
        .batch_execute("INSERT INTO t VALUES (3, 1), (1, 2), (2, 3), (1, 4), (3, 5)")
        .await
        .expect("seed");

    let rows = client
        .query("SELECT DISTINCT ON (a) a FROM t", &[])
        .await
        .expect("distinct on without order by");
    let groups: HashSet<i32> = rows.iter().map(|r| r.get::<_, i32>(0)).collect();
    // One row per distinct a, regardless of which row within the group.
    assert_eq!(groups, HashSet::from([1, 2, 3]));
    assert_eq!(rows.len(), 3, "exactly one row per group");

    shutdown(client, server_handle).await;
}

/// `LIMIT` applies after the dedup: the first N deduplicated rows.
#[tokio::test]
async fn distinct_on_with_limit() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)")
        .await
        .expect("create");
    client
        .batch_execute("INSERT INTO t VALUES (1, 10), (1, 11), (2, 20), (2, 21), (3, 30), (4, 40)")
        .await
        .expect("seed");

    let rows = client
        .query(
            "SELECT DISTINCT ON (a) a, b FROM t ORDER BY a, b LIMIT 2",
            &[],
        )
        .await
        .expect("distinct on with limit");
    let got: Vec<(i32, i32)> = rows
        .iter()
        .map(|r| (r.get::<_, i32>(0), r.get::<_, i32>(1)))
        .collect();
    // Deduped groups in a-order are (1,10),(2,20),(3,30),(4,40); LIMIT 2
    // keeps the first two.
    assert_eq!(got, vec![(1, 10), (2, 20)]);

    shutdown(client, server_handle).await;
}
