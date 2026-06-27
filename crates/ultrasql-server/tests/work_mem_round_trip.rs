//! End-to-end tests for the `work_mem` GUC and the spill paths it arms.
//!
//! These prove the server-level plumbing: `SET work_mem` changes the
//! per-statement budget, a query whose working set exceeds a small `work_mem`
//! still returns correct results (the sort / GROUP BY / hash-join operators
//! spill to disk instead of growing the heap without bound), and `SHOW
//! work_mem` reflects the session value. The executor's spill engagement is
//! unit-tested directly in `ultrasql-executor` (e.g.
//! `sort_spills_to_disk_when_work_mem_is_too_small`).

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
        "host={host} port={port} user=tester application_name=work_mem_test",
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

fn show_value(rows: &[tokio_postgres::SimpleQueryMessage]) -> String {
    rows.iter()
        .find_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => {
                Some(row.get(0).expect("value present").to_owned())
            }
            _ => None,
        })
        .expect("a data row")
}

/// `SET work_mem` then `SHOW work_mem` round-trips, and units are honoured.
#[tokio::test]
async fn set_work_mem_changes_show_value() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    // Default (unset) reports the 64 MiB byte count.
    let rows = client.simple_query("SHOW work_mem").await.expect("show");
    assert_eq!(show_value(&rows), (64 * 1024 * 1024).to_string());

    // A bare integer is kilobytes (PostgreSQL GUC unit): 1024 kB -> 1 MiB.
    client
        .batch_execute("SET work_mem = 1024")
        .await
        .expect("set bare kb");
    let rows = client.simple_query("SHOW work_mem").await.expect("show");
    assert_eq!(show_value(&rows), (1024 * 1024).to_string());

    // An explicit unit suffix overrides the default.
    client
        .batch_execute("SET work_mem = '8MB'")
        .await
        .expect("set 8MB");
    let rows = client.simple_query("SHOW work_mem").await.expect("show");
    assert_eq!(show_value(&rows), (8 * 1024 * 1024).to_string());

    // RESET falls back to the default.
    client.batch_execute("RESET work_mem").await.expect("reset");
    let rows = client.simple_query("SHOW work_mem").await.expect("show");
    assert_eq!(show_value(&rows), (64 * 1024 * 1024).to_string());

    shutdown(client, server_handle).await;
}

/// An invalid `work_mem` unit is rejected rather than silently accepted.
#[tokio::test]
async fn set_work_mem_rejects_bad_unit() {
    let (client, _conn, server_handle) = start_server_and_connect().await;
    let result = client.batch_execute("SET work_mem = '5furlongs'").await;
    let err = result.expect_err("SET work_mem with a bad unit must error");
    let db_err = err.as_db_error().expect("a database error");
    assert!(
        db_err.message().contains("work_mem"),
        "error should name the offending GUC, got: {}",
        db_err.message()
    );
    shutdown(client, server_handle).await;
}

/// A large ORDER BY under a *small* `work_mem` still returns fully-sorted,
/// correct results — the sort operator spills to disk instead of growing the
/// heap without bound.
#[tokio::test]
async fn large_sort_under_small_work_mem_returns_correct_results() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE wm_sort (id INT NOT NULL, val INT NOT NULL)")
        .await
        .expect("create");

    // Insert several thousand rows in scrambled order. With work_mem clamped
    // to its 64 KiB minimum this comfortably exceeds the in-memory budget.
    const N: i32 = 5000;
    let mut stmt = String::from("INSERT INTO wm_sort VALUES ");
    for i in 0..N {
        // Pseudo-scramble using i64 math (avoids i32 overflow) so the input
        // is unsorted and the sort has real work to do. The result of
        // `rem_euclid(N)` is in `0..N`, so it always fits back in i32.
        let v = i32::try_from((i64::from(i) * 2_654_435_761).rem_euclid(i64::from(N)))
            .expect("scrambled value fits i32");
        if i > 0 {
            stmt.push(',');
        }
        stmt.push_str(&format!("({i}, {v})"));
    }
    client.batch_execute(&stmt).await.expect("bulk insert");

    // Force spill: minimum effective work_mem.
    client
        .batch_execute("SET work_mem = '64kB'")
        .await
        .expect("set tiny work_mem");

    let rows = client
        .simple_query("SELECT val FROM wm_sort ORDER BY val ASC")
        .await
        .expect("sorted query succeeds under tiny work_mem");

    let vals: Vec<i32> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(
                row.get(0)
                    .expect("val present")
                    .parse::<i32>()
                    .expect("val parses"),
            ),
            _ => None,
        })
        .collect();

    assert_eq!(vals.len(), N as usize, "all rows returned");
    assert!(
        vals.windows(2).all(|w| w[0] <= w[1]),
        "results are fully sorted despite spilling"
    );

    shutdown(client, server_handle).await;
}

/// A large GROUP BY under a small `work_mem` aggregates correctly (the hash
/// aggregate spills rather than ballooning the heap).
#[tokio::test]
async fn large_group_by_under_small_work_mem_returns_correct_results() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE wm_agg (g INT NOT NULL)")
        .await
        .expect("create");

    const GROUPS: i32 = 3000;
    let mut stmt = String::from("INSERT INTO wm_agg VALUES ");
    for i in 0..(GROUPS * 2) {
        if i > 0 {
            stmt.push(',');
        }
        stmt.push_str(&format!("({})", i % GROUPS));
    }
    client.batch_execute(&stmt).await.expect("bulk insert");

    client
        .batch_execute("SET work_mem = '64kB'")
        .await
        .expect("set tiny work_mem");

    let rows = client
        .simple_query("SELECT COUNT(*) FROM (SELECT g FROM wm_agg GROUP BY g) AS t")
        .await
        .expect("group by succeeds under tiny work_mem");

    let count: i64 = rows
        .iter()
        .find_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(
                row.get(0)
                    .expect("count present")
                    .parse::<i64>()
                    .expect("count parses"),
            ),
            _ => None,
        })
        .expect("a count row");

    assert_eq!(
        count,
        i64::from(GROUPS),
        "every distinct group counted once"
    );

    shutdown(client, server_handle).await;
}
