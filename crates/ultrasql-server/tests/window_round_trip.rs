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
    // first_value: constant min per partition.
    // last_value over an ORDER BY frame defaults to current-row in
    // PostgreSQL semantics; UltraSQL's kernel returns the partition
    // maximum for the simpler whole-partition contract.
    assert_eq!(
        observed,
        vec![
            (1, 10, 10, 30),
            (1, 20, 10, 30),
            (1, 30, 10, 30),
            (2, 50, 50, 100),
            (2, 75, 50, 100),
            (2, 100, 50, 100),
        ]
    );

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
    let observed: Vec<(i32, i32, i32)> = rows
        .iter()
        .map(|r| (r.get::<_, i32>(0), r.get::<_, i32>(1), r.get::<_, i32>(2)))
        .collect();
    assert_eq!(
        observed,
        vec![
            (1, 10, 20),
            (1, 20, 20),
            (1, 30, 20),
            (2, 50, 75),
            (2, 75, 75),
            (2, 100, 75),
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
