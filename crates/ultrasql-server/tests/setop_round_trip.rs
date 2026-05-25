//! End-to-end `SetOp` tests against a real `tokio-postgres` client.
//!
//! Closes the v0.5 P0 wire-protocol gap "Wire `SetOp`" by driving an
//! in-process `ultrasqld` with a stock `tokio-postgres` client and
//! asserting that `SELECT ... UNION / INTERSECT / EXCEPT ...` produces
//! the rows PostgreSQL itself would emit for the same data.
//!
//! Shapes covered:
//!
//! - `UNION` (distinct duplicates removed).
//! - `UNION ALL` (duplicates kept).
//! - `INTERSECT` (distinct rows in both sides).
//! - `INTERSECT ALL` (multiset min of per-row counts).
//! - `EXCEPT` (distinct left rows absent from right).
//! - `EXCEPT ALL` (multiset diff: subtract right counts from left).
//!
//! ## Why a dedicated integration test
//!
//! The unit tests in `pipeline.rs::tests` confirm the lowerer dispatches
//! `LogicalPlan::SetOp` to the executor's `SetOp` kernel. The
//! integration tests here go one step further: they drive the kernel
//! through the **real wire path** (Simple Query) and validate the
//! decoded rows that PostgreSQL drivers actually see.

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
        "host={host} port={port} user=tester application_name=setop_test",
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

/// Tidy shutdown sequence — drop the client, give the connection task a
/// beat to flush its socket teardown, then abort the listener.
async fn shutdown(
    client: tokio_postgres::Client,
    server_handle: tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
) {
    drop(client);
    tokio::time::sleep(Duration::from_millis(20)).await;
    server_handle.abort();
}

/// Decode a `simple_query` result into a `Vec<i32>` of the first column.
/// Skips non-row protocol messages (`CommandComplete`, `RowDescription`).
fn rows_to_i32_col(rows: &[tokio_postgres::SimpleQueryMessage], col: usize) -> Vec<i32> {
    rows.iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(col)?.parse::<i32>().ok(),
            _ => None,
        })
        .collect()
}

/// Decode a `simple_query` result into the non-null strings from a
/// selected column.
fn rows_to_string_col(rows: &[tokio_postgres::SimpleQueryMessage], col: usize) -> Vec<String> {
    rows.iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(col).map(ToOwned::to_owned),
            _ => None,
        })
        .collect()
}

/// Create a pair of tables `setop_l(id INT)` and `setop_r(id INT)`
/// suffixed with `tag` so every test gets a fresh namespace. The same
/// suffix scheme is used by the JOIN integration tests; it keeps tests
/// runnable in any order and survives an in-process server that is
/// reused across `tokio::test`s.
async fn create_setop_tables(client: &tokio_postgres::Client, tag: &str) -> (String, String) {
    let left = format!("setop_l_{tag}");
    let right = format!("setop_r_{tag}");
    client
        .batch_execute(&format!("CREATE TABLE {left} (id INT NOT NULL)"))
        .await
        .expect("create left");
    client
        .batch_execute(&format!("CREATE TABLE {right} (id INT NOT NULL)"))
        .await
        .expect("create right");
    (left, right)
}

/// Populate `table` with `rows`. Uses single-row INSERTs because v0.5
/// `INSERT INTO ... SELECT` is still gated on the wire matrix.
async fn insert_rows(client: &tokio_postgres::Client, table: &str, rows: &[i32]) {
    for v in rows {
        client
            .batch_execute(&format!("INSERT INTO {table} VALUES ({v})"))
            .await
            .expect("insert row");
    }
}

/// `SELECT id FROM l UNION SELECT id FROM r` — distinct union; both
/// duplicates inside each side and across the boundary are collapsed.
#[tokio::test]
async fn union_distinct_drops_duplicates_across_sides() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;
    let (left, right) = create_setop_tables(&client, "union_d").await;
    insert_rows(&client, &left, &[1, 2, 2, 3]).await;
    insert_rows(&client, &right, &[2, 3, 4]).await;

    let rows = client
        .simple_query(&format!(
            "SELECT id FROM {left} UNION SELECT id FROM {right}"
        ))
        .await
        .expect("query succeeds");
    let mut ids = rows_to_i32_col(&rows, 0);
    ids.sort_unstable();
    assert_eq!(ids, vec![1, 2, 3, 4]);

    shutdown(client, server_handle).await;
}

/// `SELECT id FROM l UNION ALL SELECT id FROM r` — duplicates kept.
#[tokio::test]
async fn union_all_keeps_every_row() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;
    let (left, right) = create_setop_tables(&client, "union_a").await;
    insert_rows(&client, &left, &[1, 2, 2]).await;
    insert_rows(&client, &right, &[2, 3, 3]).await;

    let rows = client
        .simple_query(&format!(
            "SELECT id FROM {left} UNION ALL SELECT id FROM {right}"
        ))
        .await
        .expect("query succeeds");
    let mut ids = rows_to_i32_col(&rows, 0);
    ids.sort_unstable();
    // 3 left rows + 3 right rows; all duplicates preserved.
    assert_eq!(ids, vec![1, 2, 2, 2, 3, 3]);

    shutdown(client, server_handle).await;
}

/// `psql \d` emits set operations with repeated unnamed `NULL` output
/// labels. PostgreSQL permits those duplicate result labels; binding
/// must keep them ordinal-addressable instead of rejecting the schema.
#[tokio::test]
async fn union_all_allows_duplicate_unnamed_null_output_columns() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;
    let (left, _) = create_setop_tables(&client, "dup_null").await;
    insert_rows(&client, &left, &[1, 2]).await;

    let rows = client
        .simple_query(&format!(
            "SELECT 'direct' AS pubname, NULL, NULL FROM {left} WHERE id = 1 \
             UNION ALL \
             SELECT 'all' AS pubname, NULL, NULL FROM {left} WHERE id = 2 \
             ORDER BY 1"
        ))
        .await
        .expect("duplicate unnamed NULL columns bind");
    let mut names = rows_to_string_col(&rows, 0);
    names.sort();
    assert_eq!(names, vec!["all", "direct"]);

    shutdown(client, server_handle).await;
}

/// `SELECT id FROM l INTERSECT SELECT id FROM r` — distinct rows in both.
#[tokio::test]
async fn intersect_distinct_returns_common_rows() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;
    let (left, right) = create_setop_tables(&client, "intersect_d").await;
    insert_rows(&client, &left, &[1, 2, 2, 3]).await;
    insert_rows(&client, &right, &[2, 3, 3, 4]).await;

    let rows = client
        .simple_query(&format!(
            "SELECT id FROM {left} INTERSECT SELECT id FROM {right}"
        ))
        .await
        .expect("query succeeds");
    let mut ids = rows_to_i32_col(&rows, 0);
    ids.sort_unstable();
    assert_eq!(ids, vec![2, 3]);

    shutdown(client, server_handle).await;
}

/// `SELECT id FROM l INTERSECT ALL SELECT id FROM r` — multiset
/// intersection: emit each row up to `min(left_count, right_count)` times.
#[tokio::test]
async fn intersect_all_respects_per_row_min_counts() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;
    let (left, right) = create_setop_tables(&client, "intersect_a").await;
    // left: 1×{1}, 3×{2}, 1×{3}; right: 2×{2}, 1×{3}, 1×{4}.
    // INTERSECT ALL = 0×{1}, 2×{2}, 1×{3}, 0×{4} → [2, 2, 3].
    insert_rows(&client, &left, &[1, 2, 2, 2, 3]).await;
    insert_rows(&client, &right, &[2, 2, 3, 4]).await;

    let rows = client
        .simple_query(&format!(
            "SELECT id FROM {left} INTERSECT ALL SELECT id FROM {right}"
        ))
        .await
        .expect("query succeeds");
    let mut ids = rows_to_i32_col(&rows, 0);
    ids.sort_unstable();
    assert_eq!(ids, vec![2, 2, 3]);

    shutdown(client, server_handle).await;
}

/// `SELECT id FROM l EXCEPT SELECT id FROM r` — distinct left rows
/// absent from right.
#[tokio::test]
async fn except_distinct_returns_left_minus_right_set() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;
    let (left, right) = create_setop_tables(&client, "except_d").await;
    insert_rows(&client, &left, &[1, 2, 2, 3]).await;
    insert_rows(&client, &right, &[2, 4]).await;

    let rows = client
        .simple_query(&format!(
            "SELECT id FROM {left} EXCEPT SELECT id FROM {right}"
        ))
        .await
        .expect("query succeeds");
    let mut ids = rows_to_i32_col(&rows, 0);
    ids.sort_unstable();
    assert_eq!(ids, vec![1, 3]);

    shutdown(client, server_handle).await;
}

/// `SELECT id FROM l EXCEPT ALL SELECT id FROM r` — multiset
/// difference: subtract right counts from left counts.
#[tokio::test]
async fn except_all_subtracts_right_counts_from_left() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;
    let (left, right) = create_setop_tables(&client, "except_a").await;
    // left: 1×{1}, 3×{2}, 1×{3}; right: 1×{2}, 1×{4}.
    // EXCEPT ALL = 1×{1}, 2×{2}, 1×{3}, 0×{4} → [1, 2, 2, 3].
    insert_rows(&client, &left, &[1, 2, 2, 2, 3]).await;
    insert_rows(&client, &right, &[2, 4]).await;

    let rows = client
        .simple_query(&format!(
            "SELECT id FROM {left} EXCEPT ALL SELECT id FROM {right}"
        ))
        .await
        .expect("query succeeds");
    let mut ids = rows_to_i32_col(&rows, 0);
    ids.sort_unstable();
    assert_eq!(ids, vec![1, 2, 2, 3]);

    shutdown(client, server_handle).await;
}
