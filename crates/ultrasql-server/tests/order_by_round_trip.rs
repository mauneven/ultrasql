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
//! `NULLS FIRST`) live in the executor unit tests
//! (`crates/ultrasql-executor/src/sort.rs::sort_null_ordering_semantics`,
//! which directly exercises `compare_values_nullable`). An end-to-end
//! NULL test against `tokio-postgres` would require `INSERT INTO t
//! VALUES (..., NULL)` to land a `Value::Null` into the heap, which
//! the wire INSERT path does not yet support — `SeqScan::build_batch`
//! rejects `DataType::Null` columns and surfaces "unsupported column
//! type null for batch building." That gap is unrelated to the Sort
//! wiring; the unit-level coverage above ensures Sort itself handles
//! NULLs correctly once the INSERT gap closes.
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
