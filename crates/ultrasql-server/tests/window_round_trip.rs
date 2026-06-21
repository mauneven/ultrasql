//! End-to-end window-function tests against a real `tokio-postgres`
//! client.
//!
//! Closes the v0.5 wire-protocol gap "OVER/WindowAgg — kernel exists,
//! not yet wired". The parser now accepts `func(args) OVER (PARTITION
//! BY ... ORDER BY ...)`; the binder rewrites each window call into a
//! synthetic `$wn_N` column emitted by a [`LogicalPlan::Window`]
//! wrapper; the server's `pipeline::lower_query` instantiates the
//! matching [`ultrasql_executor::WindowAgg`] operator.
//!
//! Covered shapes:
//! - `row_number() OVER (PARTITION BY ... ORDER BY ...)`
//! - `rank() OVER (...)` / `dense_rank() OVER (...)`
//! - `lag(expr [, offset [, default]]) OVER (...)`
//! - `lead(expr [, offset [, default]]) OVER (...)`
//! - `first_value(expr) OVER (...)` / `last_value(expr) OVER (...)`
//! - `nth_value(expr, n) OVER (...)`
//! - `ntile(n) OVER (...)`

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
        "host={host} port={port} user=tester application_name=window_test",
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

/// Bootstrap a `wn_input` table with two partitions and varying scores.
async fn seed_input_table(client: &tokio_postgres::Client) {
    client
        .batch_execute("CREATE TABLE wn_input (grp INTEGER NOT NULL, val INTEGER NOT NULL)")
        .await
        .expect("CREATE TABLE wn_input");
    client
        .batch_execute(
            "INSERT INTO wn_input (grp, val) VALUES \
             (1, 10), (1, 30), (1, 20), \
             (2, 100), (2, 50), (2, 75)",
        )
        .await
        .expect("INSERT wn_input rows");
}

#[tokio::test]
async fn row_number_orders_within_partition() {
    let (client, _conn, server_handle) = start_server_and_connect().await;
    seed_input_table(&client).await;

    let rows = client
        .query(
            "SELECT grp, val, row_number() OVER (PARTITION BY grp ORDER BY val) \
             FROM wn_input ORDER BY grp, val",
            &[],
        )
        .await
        .expect("row_number query");

    let observed: Vec<(i32, i32, i64)> = rows
        .iter()
        .map(|r| (r.get::<_, i32>(0), r.get::<_, i32>(1), r.get::<_, i64>(2)))
        .collect();
    assert_eq!(
        observed,
        vec![
            (1, 10, 1),
            (1, 20, 2),
            (1, 30, 3),
            (2, 50, 1),
            (2, 75, 2),
            (2, 100, 3),
        ]
    );

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn window_order_key_runtime_cast_error_returns_22p02() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute(
            "CREATE TABLE wn_cast_input (id INTEGER NOT NULL, raw TEXT NOT NULL);
             INSERT INTO wn_cast_input VALUES (1, 'not-int')",
        )
        .await
        .expect("setup");

    let err = client
        .simple_query(
            "SELECT row_number() OVER (ORDER BY CAST(raw AS INTEGER))
             FROM wn_cast_input",
        )
        .await
        .expect_err("window order-key runtime cast rejects row");
    assert_eq!(
        err.code().map(tokio_postgres::error::SqlState::code),
        Some("22P02")
    );

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn rank_and_dense_rank_handle_ties() {
    let (client, _conn, server_handle) = start_server_and_connect().await;
    client
        .batch_execute("CREATE TABLE wn_ties (val INTEGER NOT NULL)")
        .await
        .expect("CREATE TABLE wn_ties");
    client
        .batch_execute("INSERT INTO wn_ties (val) VALUES (10), (10), (20), (30), (30)")
        .await
        .expect("INSERT wn_ties rows");

    let rows = client
        .query(
            "SELECT val, rank() OVER (ORDER BY val), dense_rank() OVER (ORDER BY val) \
             FROM wn_ties ORDER BY val",
            &[],
        )
        .await
        .expect("rank query");
    let observed: Vec<(i32, i64, i64)> = rows
        .iter()
        .map(|r| (r.get::<_, i32>(0), r.get::<_, i64>(1), r.get::<_, i64>(2)))
        .collect();
    // rank: 1, 1, 3, 4, 4   dense_rank: 1, 1, 2, 3, 3
    assert_eq!(
        observed,
        vec![(10, 1, 1), (10, 1, 1), (20, 3, 2), (30, 4, 3), (30, 4, 3),]
    );

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn lag_and_lead_with_default_value() {
    let (client, _conn, server_handle) = start_server_and_connect().await;
    seed_input_table(&client).await;

    let rows = client
        .query(
            "SELECT grp, val, \
                    lag(val, 1, -1) OVER (PARTITION BY grp ORDER BY val), \
                    lead(val, 1, -1) OVER (PARTITION BY grp ORDER BY val) \
             FROM wn_input ORDER BY grp, val",
            &[],
        )
        .await
        .expect("lag/lead query");
    let observed: Vec<(i32, i32, i32, i32)> = rows
        .iter()
        .map(|r| {
            (
                r.get::<_, i32>(0),
                r.get::<_, i32>(1),
                r.get::<_, i32>(2),
                r.get::<_, i32>(3),
            )
        })
        .collect();
    assert_eq!(
        observed,
        vec![
            (1, 10, -1, 20),
            (1, 20, 10, 30),
            (1, 30, 20, -1),
            (2, 50, -1, 75),
            (2, 75, 50, 100),
            (2, 100, 75, -1),
        ]
    );

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn first_value_and_last_value_within_partition() {
    let (client, _conn, server_handle) = start_server_and_connect().await;
    seed_input_table(&client).await;

    let rows = client
        .query(
            "SELECT grp, val, \
                    first_value(val) OVER (PARTITION BY grp ORDER BY val), \
                    last_value(val)  OVER (PARTITION BY grp ORDER BY val) \
             FROM wn_input ORDER BY grp, val",
            &[],
        )
        .await
        .expect("first/last query");
    let observed: Vec<(i32, i32, i32, i32)> = rows
        .iter()
        .map(|r| {
            (
                r.get::<_, i32>(0),
                r.get::<_, i32>(1),
                r.get::<_, i32>(2),
                r.get::<_, i32>(3),
            )
        })
        .collect();
    // PostgreSQL semantics: the default frame is RANGE UNBOUNDED
    // PRECEDING AND CURRENT ROW, so first_value is the partition min but
    // last_value is the CURRENT row (not the partition max).
    assert_eq!(
        observed,
        vec![
            (1, 10, 10, 10),
            (1, 20, 10, 20),
            (1, 30, 10, 30),
            (2, 50, 50, 50),
            (2, 75, 50, 75),
            (2, 100, 50, 100),
        ]
    );

    shutdown(client, server_handle).await;
}

/// Case 11: `last_value` under the default frame returns the current
/// row; widening to `UNBOUNDED FOLLOWING` returns the true last value.
#[tokio::test]
async fn last_value_default_vs_unbounded_following() {
    let (client, _conn, server_handle) = start_server_and_connect().await;
    client
        .batch_execute("CREATE TABLE lvt (id INTEGER NOT NULL, v INTEGER NOT NULL)")
        .await
        .expect("create lvt");
    client
        .batch_execute("INSERT INTO lvt (id, v) VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .expect("insert lvt");

    let rows = client
        .query(
            "SELECT id, v, \
                last_value(v) OVER (ORDER BY id) AS lv_default, \
                last_value(v) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS lv_whole \
             FROM lvt ORDER BY id",
            &[],
        )
        .await
        .expect("last_value query");
    let observed: Vec<(i32, i32, i32, i32)> = rows
        .iter()
        .map(|r| {
            (
                r.get::<_, i32>(0),
                r.get::<_, i32>(1),
                r.get::<_, i32>(2),
                r.get::<_, i32>(3),
            )
        })
        .collect();
    assert_eq!(
        observed,
        vec![(1, 10, 10, 30), (2, 20, 20, 30), (3, 30, 30, 30)]
    );

    shutdown(client, server_handle).await;
}

/// Case 1: `sum(v) OVER (ORDER BY id)` is a running total; `sum(v)
/// OVER ()` is the whole-partition total.
#[tokio::test]
async fn aggregate_window_running_total_and_whole_partition() {
    let (client, _conn, server_handle) = start_server_and_connect().await;
    client
        .batch_execute("CREATE TABLE s1 (id INTEGER NOT NULL, v INTEGER NOT NULL)")
        .await
        .expect("create s1");
    client
        .batch_execute("INSERT INTO s1 (id, v) VALUES (1,10),(2,20),(3,30),(4,40)")
        .await
        .expect("insert s1");

    let rows = client
        .query(
            "SELECT id, v, \
                sum(v) OVER (ORDER BY id) AS run_default, \
                sum(v) OVER ()           AS whole_part \
             FROM s1 ORDER BY id",
            &[],
        )
        .await
        .expect("running total query");
    let observed: Vec<(i32, i32, i64, i64)> = rows
        .iter()
        .map(|r| {
            (
                r.get::<_, i32>(0),
                r.get::<_, i32>(1),
                r.get::<_, i64>(2),
                r.get::<_, i64>(3),
            )
        })
        .collect();
    assert_eq!(
        observed,
        vec![
            (1, 10, 10, 100),
            (2, 20, 30, 100),
            (3, 30, 60, 100),
            (4, 40, 100, 100),
        ]
    );

    shutdown(client, server_handle).await;
}

/// Case 5: RANGE vs ROWS cumulative with duplicate ORDER BY values.
/// Peers share the RANGE result (stepped), ROWS counts each row.
#[tokio::test]
async fn aggregate_window_range_vs_rows_peers() {
    let (client, _conn, server_handle) = start_server_and_connect().await;
    client
        .batch_execute("CREATE TABLE s5 (id INTEGER NOT NULL, g INTEGER NOT NULL, v INTEGER NOT NULL)")
        .await
        .expect("create s5");
    client
        .batch_execute("INSERT INTO s5 (id,g,v) VALUES (1,1,10),(2,1,20),(3,2,30),(4,2,40),(5,3,50)")
        .await
        .expect("insert s5");

    let rows = client
        .query(
            "SELECT id, \
                sum(v) OVER (ORDER BY g RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_csum, \
                sum(v) OVER (ORDER BY g ROWS  BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rows_csum \
             FROM s5 ORDER BY id",
            &[],
        )
        .await
        .expect("range vs rows query");
    let observed: Vec<(i32, i64, i64)> = rows
        .iter()
        .map(|r| (r.get::<_, i32>(0), r.get::<_, i64>(1), r.get::<_, i64>(2)))
        .collect();
    assert_eq!(
        observed,
        vec![
            (1, 30, 10),
            (2, 30, 30),
            (3, 100, 60),
            (4, 100, 100),
            (5, 150, 150),
        ]
    );

    shutdown(client, server_handle).await;
}

/// Case 16: RANGE offset with two ORDER BY columns is rejected.
#[tokio::test]
async fn range_offset_two_order_columns_errors() {
    let (client, _conn, server_handle) = start_server_and_connect().await;
    client
        .batch_execute("CREATE TABLE s16 (id INTEGER NOT NULL, v INTEGER NOT NULL)")
        .await
        .expect("create s16");
    client
        .batch_execute("INSERT INTO s16 (id, v) VALUES (1, 10), (2, 20)")
        .await
        .expect("insert s16");

    let err = client
        .query(
            "SELECT sum(v) OVER (ORDER BY id, v RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM s16",
            &[],
        )
        .await
        .expect_err("two ORDER BY columns must be rejected");
    let db_err = err.as_db_error().expect("server db error");
    assert!(
        db_err.message().contains("exactly one ORDER BY column"),
        "unexpected error: {db_err}"
    );
    assert_eq!(db_err.code().code(), "42P20", "sqlstate: {db_err:?}");

    shutdown(client, server_handle).await;
}

/// Case 17: a negative ROWS frame offset is rejected at execution.
#[tokio::test]
async fn negative_rows_offset_errors() {
    let (client, _conn, server_handle) = start_server_and_connect().await;
    client
        .batch_execute("CREATE TABLE s17 (id INTEGER NOT NULL, v INTEGER NOT NULL)")
        .await
        .expect("create s17");
    client
        .batch_execute("INSERT INTO s17 (id, v) VALUES (1, 10), (2, 20)")
        .await
        .expect("insert s17");

    let err = client
        .query(
            "SELECT sum(v) OVER (ORDER BY id ROWS BETWEEN -1 PRECEDING AND CURRENT ROW) FROM s17",
            &[],
        )
        .await
        .expect_err("negative offset must be rejected");
    let db_err = err.as_db_error().expect("server db error");
    assert!(
        db_err.message().contains("must not be negative"),
        "unexpected error: {db_err}"
    );
    assert_eq!(db_err.code().code(), "22013", "sqlstate: {db_err:?}");

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn nth_value_returns_kth_row_per_partition() {
    let (client, _conn, server_handle) = start_server_and_connect().await;
    seed_input_table(&client).await;

    let rows = client
        .query(
            "SELECT grp, val, nth_value(val, 2) OVER (PARTITION BY grp ORDER BY val) \
             FROM wn_input ORDER BY grp, val",
            &[],
        )
        .await
        .expect("nth_value query");
    // PostgreSQL semantics: under the default running frame, nth_value(2)
    // is NULL until the frame has grown to include the 2nd row.
    let observed: Vec<(i32, i32, Option<i32>)> = rows
        .iter()
        .map(|r| {
            (
                r.get::<_, i32>(0),
                r.get::<_, i32>(1),
                r.get::<_, Option<i32>>(2),
            )
        })
        .collect();
    assert_eq!(
        observed,
        vec![
            (1, 10, None),
            (1, 20, Some(20)),
            (1, 30, Some(20)),
            (2, 50, None),
            (2, 75, Some(75)),
            (2, 100, Some(75)),
        ]
    );

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn ntile_buckets_rows_evenly() {
    let (client, _conn, server_handle) = start_server_and_connect().await;
    seed_input_table(&client).await;

    let rows = client
        .query(
            "SELECT grp, val, ntile(3) OVER (PARTITION BY grp ORDER BY val) \
             FROM wn_input ORDER BY grp, val",
            &[],
        )
        .await
        .expect("ntile query");
    let observed: Vec<(i32, i32, i64)> = rows
        .iter()
        .map(|r| (r.get::<_, i32>(0), r.get::<_, i32>(1), r.get::<_, i64>(2)))
        .collect();
    assert_eq!(
        observed,
        vec![
            (1, 10, 1),
            (1, 20, 2),
            (1, 30, 3),
            (2, 50, 1),
            (2, 75, 2),
            (2, 100, 3),
        ]
    );

    shutdown(client, server_handle).await;
}
