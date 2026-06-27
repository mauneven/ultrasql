//! End-to-end subquery decorrelation checks.

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
        "host={host} port={port} user=tester application_name=subquery_test",
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

#[tokio::test]
async fn correlated_exists_returns_each_outer_row_once() {
    let (client, _conn, server_handle) = start_server_and_connect().await;
    client
        .batch_execute("CREATE TABLE sq_orders (o_orderkey INT NOT NULL)")
        .await
        .expect("create orders");
    client
        .batch_execute(
            "CREATE TABLE sq_lineitem (
                 l_orderkey INT NOT NULL,
                 l_commit INT NOT NULL,
                 l_receipt INT NOT NULL
             )",
        )
        .await
        .expect("create lineitem");
    client
        .batch_execute("INSERT INTO sq_orders VALUES (1), (2), (3)")
        .await
        .expect("insert orders");
    client
        .batch_execute(
            "INSERT INTO sq_lineitem VALUES
                 (1, 1, 2),
                 (1, 1, 3),
                 (2, 3, 2)",
        )
        .await
        .expect("insert lineitems");

    let rows = client
        .simple_query(
            "SELECT o_orderkey
             FROM sq_orders
             WHERE EXISTS (
                 SELECT *
                 FROM sq_lineitem
                 WHERE l_orderkey = o_orderkey
                   AND l_commit < l_receipt
             )
             ORDER BY o_orderkey",
        )
        .await
        .expect("query succeeds");
    let keys: Vec<i32> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0)?.parse().ok(),
            _ => None,
        })
        .collect();
    assert_eq!(keys, vec![1]);

    let rows = client
        .simple_query(
            "SELECT o_orderkey
             FROM sq_orders
             WHERE NOT EXISTS (
                 SELECT *
                 FROM sq_lineitem
                 WHERE l_orderkey = o_orderkey
                   AND l_commit < l_receipt
             )
             ORDER BY o_orderkey",
        )
        .await
        .expect("NOT EXISTS query succeeds");
    let keys: Vec<i32> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0)?.parse().ok(),
            _ => None,
        })
        .collect();
    assert_eq!(keys, vec![2, 3]);

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn uncorrelated_in_and_scalar_subqueries_lower_before_execution() {
    let (client, _conn, server_handle) = start_server_and_connect().await;
    client
        .batch_execute("CREATE TABLE sq_supplier (s_suppkey INT NOT NULL)")
        .await
        .expect("create supplier");
    client
        .batch_execute("CREATE TABLE sq_blocked (b_suppkey INT NOT NULL)")
        .await
        .expect("create blocked");
    client
        .batch_execute("INSERT INTO sq_supplier VALUES (1), (2), (3)")
        .await
        .expect("insert suppliers");
    client
        .batch_execute("INSERT INTO sq_blocked VALUES (2)")
        .await
        .expect("insert blocked");

    let rows = client
        .simple_query(
            "SELECT s_suppkey
             FROM sq_supplier
             WHERE s_suppkey NOT IN (SELECT b_suppkey FROM sq_blocked)
             ORDER BY s_suppkey",
        )
        .await
        .expect("NOT IN query succeeds");
    let not_in_keys: Vec<i32> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0)?.parse().ok(),
            _ => None,
        })
        .collect();
    assert_eq!(not_in_keys, vec![1, 3]);

    let rows = client
        .simple_query(
            "SELECT s_suppkey
             FROM sq_supplier
             WHERE s_suppkey > (SELECT b_suppkey FROM sq_blocked)
             ORDER BY s_suppkey",
        )
        .await
        .expect("scalar subquery succeeds");
    let scalar_keys: Vec<i32> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0)?.parse().ok(),
            _ => None,
        })
        .collect();
    assert_eq!(scalar_keys, vec![3]);

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn uncorrelated_not_in_returns_no_rows_when_subquery_contains_null() {
    let (client, _conn, server_handle) = start_server_and_connect().await;
    client
        .batch_execute("CREATE TABLE sq_supplier_nulls (s_suppkey INT NOT NULL)")
        .await
        .expect("create supplier");
    client
        .batch_execute("CREATE TABLE sq_blocked_nulls (b_suppkey INT)")
        .await
        .expect("create blocked");
    client
        .batch_execute("INSERT INTO sq_supplier_nulls VALUES (1), (2), (3)")
        .await
        .expect("insert suppliers");
    client
        .batch_execute("INSERT INTO sq_blocked_nulls VALUES (2), (NULL)")
        .await
        .expect("insert blocked");

    let rows = client
        .simple_query(
            "SELECT s_suppkey
             FROM sq_supplier_nulls
             WHERE s_suppkey NOT IN (SELECT b_suppkey FROM sq_blocked_nulls)
             ORDER BY s_suppkey",
        )
        .await
        .expect("NOT IN query succeeds");
    let not_in_keys: Vec<i32> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0)?.parse().ok(),
            _ => None,
        })
        .collect();
    assert_eq!(not_in_keys, Vec::<i32>::new());

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn correlated_in_and_not_in_lower_before_execution() {
    let (client, _conn, server_handle) = start_server_and_connect().await;
    client
        .batch_execute(
            "CREATE TABLE sq_outer_pair (
                 outer_id INT NOT NULL,
                 outer_group INT NOT NULL
             )",
        )
        .await
        .expect("create outer pair");
    client
        .batch_execute(
            "CREATE TABLE sq_inner_pair (
                 inner_id INT NOT NULL,
                 inner_group INT NOT NULL
             )",
        )
        .await
        .expect("create inner pair");
    client
        .batch_execute("INSERT INTO sq_outer_pair VALUES (1, 10), (2, 10), (3, 20), (4, 30)")
        .await
        .expect("insert outer rows");
    client
        .batch_execute("INSERT INTO sq_inner_pair VALUES (1, 10), (3, 20), (5, 10)")
        .await
        .expect("insert inner rows");

    let rows = client
        .simple_query(
            "SELECT outer_id
             FROM sq_outer_pair o
             WHERE outer_id IN (
                 SELECT inner_id
                 FROM sq_inner_pair i
                 WHERE i.inner_group = o.outer_group
             )
             ORDER BY outer_id",
        )
        .await
        .expect("correlated IN query succeeds");
    let in_keys: Vec<i32> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0)?.parse().ok(),
            _ => None,
        })
        .collect();
    assert_eq!(in_keys, vec![1, 3]);

    let rows = client
        .simple_query(
            "SELECT outer_id
             FROM sq_outer_pair o
             WHERE outer_id NOT IN (
                 SELECT inner_id
                 FROM sq_inner_pair i
                 WHERE i.inner_group = o.outer_group
             )
             ORDER BY outer_id",
        )
        .await
        .expect("correlated NOT IN query succeeds");
    let not_in_keys: Vec<i32> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0)?.parse().ok(),
            _ => None,
        })
        .collect();
    assert_eq!(not_in_keys, vec![2, 4]);

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn correlated_not_in_uses_group_local_null_semantics() {
    let (client, _conn, server_handle) = start_server_and_connect().await;
    client
        .batch_execute(
            "CREATE TABLE sq_outer_pair_nulls (
                 outer_id INT,
                 outer_group INT NOT NULL
             )",
        )
        .await
        .expect("create outer pair");
    client
        .batch_execute(
            "CREATE TABLE sq_inner_pair_nulls (
                 inner_id INT,
                 inner_group INT NOT NULL
             )",
        )
        .await
        .expect("create inner pair");
    client
        .batch_execute(
            "INSERT INTO sq_outer_pair_nulls VALUES
                 (1, 10),
                 (2, 10),
                 (3, 20),
                 (4, 30),
                 (NULL, 20),
                 (NULL, 30)",
        )
        .await
        .expect("insert outer rows");
    client
        .batch_execute("INSERT INTO sq_inner_pair_nulls VALUES (1, 10), (NULL, 10), (5, 20)")
        .await
        .expect("insert inner rows");

    let rows = client
        .simple_query(
            "SELECT COALESCE(outer_id, -1), outer_group
             FROM sq_outer_pair_nulls o
             WHERE outer_id NOT IN (
                 SELECT inner_id
                 FROM sq_inner_pair_nulls i
                 WHERE i.inner_group = o.outer_group
             )
             ORDER BY outer_group, 1",
        )
        .await
        .expect("correlated NOT IN query succeeds");
    let mut not_in_rows: Vec<(i32, i32)> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => {
                Some((row.get(0)?.parse().ok()?, row.get(1)?.parse().ok()?))
            }
            _ => None,
        })
        .collect();
    not_in_rows.sort_unstable();
    assert_eq!(not_in_rows, vec![(-1, 30), (3, 20), (4, 30)]);

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn correlated_scalar_aggregate_subquery_lowers_before_execution() {
    let (client, _conn, server_handle) = start_server_and_connect().await;
    client
        .batch_execute(
            "CREATE TABLE sq_part_price (
                 p_partkey INT NOT NULL,
                 p_limit INT NOT NULL
             )",
        )
        .await
        .expect("create part price");
    client
        .batch_execute(
            "CREATE TABLE sq_supply (
                 ps_partkey INT NOT NULL,
                 ps_cost INT NOT NULL
             )",
        )
        .await
        .expect("create supply");
    client
        .batch_execute("INSERT INTO sq_part_price VALUES (1, 5), (2, 7), (3, 9)")
        .await
        .expect("insert part price");
    client
        .batch_execute("INSERT INTO sq_supply VALUES (1, 5), (1, 8), (2, 6), (2, 10)")
        .await
        .expect("insert supply");

    let rows = client
        .simple_query(
            "SELECT p_partkey
             FROM sq_part_price
             WHERE p_limit = (
                 SELECT MIN(ps_cost)
                 FROM sq_supply
                 WHERE ps_partkey = p_partkey
             )
             ORDER BY p_partkey",
        )
        .await
        .expect("correlated scalar aggregate succeeds");
    let keys: Vec<i32> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0)?.parse().ok(),
            _ => None,
        })
        .collect();
    assert_eq!(keys, vec![1]);

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn mixed_exists_not_exists_with_residual_correlation_lowers_before_execution() {
    let (client, _conn, server_handle) = start_server_and_connect().await;
    client
        .batch_execute(
            "CREATE TABLE sq_wait_lineitem (
                 l_orderkey INT NOT NULL,
                 l_suppkey INT NOT NULL,
                 l_receipt INT NOT NULL,
                 l_commit INT NOT NULL
             )",
        )
        .await
        .expect("create wait lineitem");
    client
        .batch_execute(
            "INSERT INTO sq_wait_lineitem VALUES
                 (1, 10, 5, 3),
                 (1, 20, 2, 3),
                 (2, 30, 6, 3),
                 (2, 40, 7, 3),
                 (3, 50, 9, 2)",
        )
        .await
        .expect("insert wait lineitem");

    let rows = client
        .simple_query(
            "SELECT l1.l_orderkey, l1.l_suppkey
             FROM sq_wait_lineitem l1
             WHERE l1.l_receipt > l1.l_commit
               AND EXISTS (
                   SELECT *
                   FROM sq_wait_lineitem l2
                   WHERE l2.l_orderkey = l1.l_orderkey
                     AND l2.l_suppkey <> l1.l_suppkey
               )
               AND NOT EXISTS (
                   SELECT *
                   FROM sq_wait_lineitem l3
                   WHERE l3.l_orderkey = l1.l_orderkey
                     AND l3.l_suppkey <> l1.l_suppkey
                     AND l3.l_receipt > l3.l_commit
               )
             ORDER BY l1.l_orderkey, l1.l_suppkey",
        )
        .await
        .expect("mixed EXISTS/NOT EXISTS query succeeds");
    let keys: Vec<(i32, i32)> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => {
                Some((row.get(0)?.parse().ok()?, row.get(1)?.parse().ok()?))
            }
            _ => None,
        })
        .collect();
    assert_eq!(keys, vec![(1, 10)]);

    shutdown(client, server_handle).await;
}

/// A correlated `COUNT(*)` scalar subquery in the SELECT list must report 0
/// (not NULL) for outer rows with no matching inner rows. Decorrelation lowers
/// the subquery to a LEFT OUTER JOIN against a grouped count, then wraps the
/// joined column in `COALESCE(col, 0)`.
#[tokio::test]
async fn correlated_count_scalar_subquery_returns_zero_for_unmatched_rows() {
    let (client, _conn, server_handle) = start_server_and_connect().await;
    client
        .batch_execute("CREATE TABLE sq_users (u_id INT NOT NULL)")
        .await
        .expect("create users");
    client
        .batch_execute("CREATE TABLE sq_user_orders (o_uid INT NOT NULL)")
        .await
        .expect("create orders");
    client
        .batch_execute("INSERT INTO sq_users VALUES (1), (2), (3)")
        .await
        .expect("insert users");
    // User 1 has two orders, user 2 has one, user 3 has none.
    client
        .batch_execute("INSERT INTO sq_user_orders VALUES (1), (1), (2)")
        .await
        .expect("insert orders");

    let rows = client
        .simple_query(
            "SELECT u_id,
                    (SELECT COUNT(*) FROM sq_user_orders WHERE o_uid = u_id) AS n
             FROM sq_users
             ORDER BY u_id",
        )
        .await
        .expect("correlated COUNT scalar subquery succeeds");
    let counts: Vec<(i32, i64)> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => {
                Some((row.get(0)?.parse().ok()?, row.get(1)?.parse().ok()?))
            }
            _ => None,
        })
        .collect();
    // `ORDER BY u_id` must be honored even though the correlated COUNT subquery
    // decorrelates into a join below the projection — the binder lifts the Sort
    // above the (non-order-preserving) projection — so the rows arrive in u_id
    // order. User 3 must also show 0, NOT NULL (a NULL would `parse().ok()?`-skip
    // and disappear from `counts`).
    assert_eq!(counts, vec![(1, 2), (2, 1), (3, 0)]);

    shutdown(client, server_handle).await;
}

// NOTE: a non-aggregated *correlated* scalar subquery whose inner side matches
// more than one row per outer key is a documented pre-existing limitation —
// decorrelation rewrites it to a LEFT OUTER JOIN that duplicates the outer row
// rather than raising a cardinality error. The `SingleRowAssert` operator added
// for *uncorrelated* scalar subqueries is a single global 1-row guard and does
// not fit the per-outer-key semantics the correlated LEFT-JOIN shape needs, so
// the correlated multi-row case stays a separate roadmap item; the common
// single-row case (covered above) works. The uncorrelated empty / multi-row /
// single-row cases ARE fully fixed — see
// `uncorrelated_scalar_subquery_single_row_assert_battery` below.

/// Adversarial battery for the uncorrelated scalar-subquery single-row guard.
///
/// Each uncorrelated scalar subquery is decorrelated to a CROSS JOIN against a
/// `SingleRowAssert`-wrapped right side, which constrains the subquery to
/// exactly one row: empty → NULL-padded (all outer rows kept), single → value,
/// >1 → SQLSTATE 21000 (`cardinality_violation`). This mirrors PostgreSQL.
#[tokio::test]
async fn uncorrelated_scalar_subquery_single_row_assert_battery() {
    let (client, _conn, server_handle) = start_server_and_connect().await;
    // `t` (3 rows), `e` (empty), `m` (2 rows), `s` (1 row, value 9).
    client
        .batch_execute("CREATE TABLE sra_t (a INT NOT NULL)")
        .await
        .expect("create t");
    client
        .batch_execute("CREATE TABLE sra_e (id INT)")
        .await
        .expect("create e");
    client
        .batch_execute("CREATE TABLE sra_m (id INT NOT NULL)")
        .await
        .expect("create m");
    client
        .batch_execute("CREATE TABLE sra_s (id INT NOT NULL)")
        .await
        .expect("create s");
    client
        .batch_execute("INSERT INTO sra_t VALUES (10), (20), (30)")
        .await
        .expect("insert t");
    client
        .batch_execute("INSERT INTO sra_m VALUES (1), (2)")
        .await
        .expect("insert m");
    client
        .batch_execute("INSERT INTO sra_s VALUES (9)")
        .await
        .expect("insert s");

    // ---- (1) EMPTY uncorrelated scalar in SELECT: 3 rows, scalar all NULL ----
    let rows = client
        .simple_query("SELECT a, (SELECT id FROM sra_e) AS sc FROM sra_t ORDER BY a")
        .await
        .expect("empty scalar in SELECT succeeds");
    let pairs: Vec<(i32, Option<String>)> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => {
                Some((row.get(0)?.parse().ok()?, row.get(1).map(str::to_owned)))
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        pairs,
        vec![(10, None), (20, None), (30, None)],
        "empty scalar subquery must keep all 3 outer rows and NULL-pad the scalar"
    );

    // ---- (3) SINGLE-ROW uncorrelated scalar in SELECT: each row gets 9 ----
    let rows = client
        .simple_query("SELECT a, (SELECT id FROM sra_s) AS sc FROM sra_t ORDER BY a")
        .await
        .expect("single-row scalar in SELECT succeeds");
    let pairs: Vec<(i32, i32)> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => {
                Some((row.get(0)?.parse().ok()?, row.get(1)?.parse().ok()?))
            }
            _ => None,
        })
        .collect();
    assert_eq!(pairs, vec![(10, 9), (20, 9), (30, 9)]);

    // ---- (2) MULTI-ROW uncorrelated scalar in SELECT: ERROR 21000 ----
    let err = client
        .simple_query("SELECT a, (SELECT id FROM sra_m) FROM sra_t")
        .await
        .expect_err("multi-row scalar subquery in SELECT must error");
    assert_eq!(
        err.code().map(tokio_postgres::error::SqlState::code),
        Some("21000"),
        "multi-row scalar subquery must raise cardinality_violation, not fan out"
    );

    // ---- (4) WHERE position: empty → 0 matches; single → matching row; multi → 21000 ----
    let rows = client
        .simple_query("SELECT a FROM sra_t WHERE a = (SELECT id FROM sra_e) ORDER BY a")
        .await
        .expect("empty scalar in WHERE succeeds");
    let matched: Vec<i32> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0)?.parse().ok(),
            _ => None,
        })
        .collect();
    assert!(
        matched.is_empty(),
        "a = NULL is UNKNOWN, so an empty scalar subquery yields no matches"
    );

    let rows = client
        .simple_query("SELECT a FROM sra_t WHERE a = (SELECT id FROM sra_s) * 1 ORDER BY a")
        .await
        .expect("single-row scalar in WHERE succeeds");
    let matched: Vec<i32> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0)?.parse().ok(),
            _ => None,
        })
        .collect();
    // No outer row equals 9, so this is empty; the point is it does not error
    // and does not drop or duplicate rows incorrectly.
    assert!(matched.is_empty());

    let err = client
        .simple_query("SELECT a FROM sra_t WHERE a = (SELECT id FROM sra_m)")
        .await
        .expect_err("multi-row scalar subquery in WHERE must error");
    assert_eq!(
        err.code().map(tokio_postgres::error::SqlState::code),
        Some("21000")
    );

    // ---- (5) Aggregated scalar (no GROUP BY): max(empty)=NULL, count(empty)=0 ----
    let rows = client
        .simple_query("SELECT a, (SELECT max(id) FROM sra_e) AS mx FROM sra_t ORDER BY a")
        .await
        .expect("max over empty succeeds");
    let pairs: Vec<(i32, Option<String>)> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => {
                Some((row.get(0)?.parse().ok()?, row.get(1).map(str::to_owned)))
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        pairs,
        vec![(10, None), (20, None), (30, None)],
        "max() of empty is NULL (one row), still kept for every outer row"
    );

    let rows = client
        .simple_query("SELECT a, (SELECT count(*) FROM sra_e) AS n FROM sra_t ORDER BY a")
        .await
        .expect("count over empty succeeds");
    let pairs: Vec<(i32, i64)> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => {
                Some((row.get(0)?.parse().ok()?, row.get(1)?.parse().ok()?))
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        pairs,
        vec![(10, 0), (20, 0), (30, 0)],
        "count(*) of empty is 0 (NOT NULL, NOT dropped) for every outer row"
    );

    // ---- (6) Scalar subquery used in an expression: a + scalar ----
    let rows = client
        .simple_query("SELECT a + (SELECT id FROM sra_s) AS v FROM sra_t ORDER BY a")
        .await
        .expect("scalar in expression succeeds");
    let vals: Vec<i32> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0)?.parse().ok(),
            _ => None,
        })
        .collect();
    assert_eq!(vals, vec![19, 29, 39], "a + 9 per row");

    // a + NULL = NULL for the empty subquery (every outer row kept, value NULL).
    let rows = client
        .simple_query("SELECT a, a + (SELECT id FROM sra_e) AS v FROM sra_t ORDER BY a")
        .await
        .expect("a + empty scalar succeeds");
    let pairs: Vec<(i32, Option<String>)> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => {
                Some((row.get(0)?.parse().ok()?, row.get(1).map(str::to_owned)))
            }
            _ => None,
        })
        .collect();
    assert_eq!(pairs, vec![(10, None), (20, None), (30, None)]);

    // ---- (8) Nested: a scalar subquery whose body has a scalar subquery ----
    // The outer subquery has FROM sra_s (1 row) and projects a nested scalar
    // subquery `(SELECT id FROM sra_s)`; both decorrelate through SingleRowAssert.
    let rows = client
        .simple_query(
            "SELECT a,
                    (SELECT (SELECT id FROM sra_s) + 1 FROM sra_s) AS sc
             FROM sra_t
             ORDER BY a",
        )
        .await
        .expect("nested scalar subquery succeeds");
    let pairs: Vec<(i32, i32)> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => {
                Some((row.get(0)?.parse().ok()?, row.get(1)?.parse().ok()?))
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        pairs,
        vec![(10, 10), (20, 10), (30, 10)],
        "inner 9 + 1 = 10"
    );

    shutdown(client, server_handle).await;
}

/// Regression for optimizer bug #5: CTE predicate pushdown must not inject the
/// outer filter into the CTE definition when the body reorders the columns. The
/// outer `WHERE sub.y > 5` index is relative to the CTE/body output `[y, z]`,
/// not the definition output `[a, b]`; an unremapped push would filter the
/// wrong column. PostgreSQL returns the row with `b = 10` (`y = 10`).
#[tokio::test]
async fn cte_in_derived_table_with_reordered_body_filters_correct_column() {
    let (client, _conn, server_handle) = start_server_and_connect().await;
    client
        .batch_execute("CREATE TABLE cte_bug5 (a INT NOT NULL, b INT NOT NULL)")
        .await
        .expect("create table");
    client
        .batch_execute("INSERT INTO cte_bug5 VALUES (10, 1), (1, 10)")
        .await
        .expect("insert rows");

    let rows = client
        .simple_query(
            "SELECT y FROM (
                 WITH c AS (SELECT a, b FROM cte_bug5)
                 SELECT b AS y, a AS z FROM c
             ) sub
             WHERE sub.y > 5",
        )
        .await
        .expect("query succeeds");
    let ys: Vec<i32> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0)?.parse().ok(),
            _ => None,
        })
        .collect();
    // `y` is the `b` column; only the (1, 10) row has b = 10 > 5.
    assert_eq!(ys, vec![10], "must filter on b (= y), not a");

    shutdown(client, server_handle).await;
}
