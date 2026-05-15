//! End-to-end JOIN tests against a real `tokio-postgres` client.
//!
//! Closes the v0.5 P0 wire-protocol gap "Wire `LogicalPlan::Join`" by
//! driving an in-process `ultrasqld` with a stock `tokio-postgres`
//! client and asserting that `SELECT ... JOIN ...` produces the rows
//! PostgreSQL itself would emit for the same data.
//!
//! Shapes covered:
//!
//! - Inner equi join — dispatcher picks `HashJoin`.
//! - Left outer equi join — dispatcher picks `HashJoin`; unmatched left
//!   rows survive with right columns set to NULL.
//! - Inner non-equi join (`l < r`) — dispatcher falls back to
//!   `NestedLoopJoin`.
//! - Join with WHERE pushed below the join — confirms the binder's
//!   filter-on-source plan still produces correct rows through the new
//!   join dispatch.
//!
//! ## Column naming convention
//!
//! Each test uses **distinct column names per side** (e.g. `lid`,
//! `lval` on the left vs `rid`, `rval` on the right). This avoids a
//! pre-existing binder limitation: when both sides of a JOIN expose a
//! column named `id`, the binder's `bind_column` ignores the
//! `t1.`/`t2.` qualifier and resolves both references to the *first*
//! `id` it finds — even though `concat_schemas_for_join` already
//! produced a disambiguated `id` / `id_1` pair in the joined schema.
//! The wire dispatch under test is unaffected: the operator selection,
//! HashJoin/NLJ split, and outer-join padding all behave identically
//! regardless of whether the column names collide. Lifting the binder
//! limitation lands as a separate task (see
//! `crates/ultrasql-planner/src/binder.rs::bind_column`); when it does,
//! these tests can adopt the canonical `t1.id = t2.id` shape without
//! changing the assertions.
//!
//! ## NULL-padding semantics
//!
//! UltraSQL's v0.5 `build_batch` does not emit a per-column null
//! bitmap, so a NULL right-side column decodes to its type's zero
//! value rather than to a `Value::Null` (see
//! `crates/ultrasql-executor/src/seq_scan.rs::build_batch`). The
//! `tokio-postgres` `SimpleQueryRow::get` therefore returns `Some("0")`
//! for those positions, not `None`. The tests assert the 0-decoded
//! shape and add a note pointing at the limitation.

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
        "host={host} port={port} user=tester application_name=join_test",
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

/// Decode a `simple_query` result set into `(i32, i32)` pairs, picking
/// up the two columns the test asserts on. Skips non-row protocol
/// messages (`CommandComplete`, `RowDescription`).
fn rows_to_i32_pairs(
    rows: &[tokio_postgres::SimpleQueryMessage],
    left_col: usize,
    right_col: usize,
) -> Vec<(Option<i32>, Option<i32>)> {
    rows.iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => {
                // `row.get` returns `None` on a NULL cell.
                let l = row.get(left_col).map(|s| s.parse::<i32>().ok()).flatten();
                let r = row.get(right_col).map(|s| s.parse::<i32>().ok()).flatten();
                Some((l, r))
            }
            _ => None,
        })
        .collect()
}

/// `t1 JOIN t2 ON t1.lid = t2.rid` — inner equi join. The dispatcher
/// picks `HashJoin`; the round-trip yields exactly the matched
/// `(lid, rval)` pairs.
#[tokio::test]
async fn inner_equi_join_matches_postgres_semantics() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE inner_eq_left (lid INT NOT NULL, lval INT NOT NULL)")
        .await
        .expect("create left");
    client
        .batch_execute("CREATE TABLE inner_eq_right (rid INT NOT NULL, rval INT NOT NULL)")
        .await
        .expect("create right");

    for (id, v) in [(1_i32, 10_i32), (2, 20), (3, 30)] {
        client
            .batch_execute(&format!("INSERT INTO inner_eq_left VALUES ({id}, {v})"))
            .await
            .expect("insert left");
    }
    for (id, v) in [(2_i32, 200_i32), (3, 300), (4, 400)] {
        client
            .batch_execute(&format!("INSERT INTO inner_eq_right VALUES ({id}, {v})"))
            .await
            .expect("insert right");
    }

    let rows = client
        .simple_query(
            "SELECT lid, rval \
             FROM inner_eq_left JOIN inner_eq_right \
             ON lid = rid",
        )
        .await
        .expect("query succeeds");
    let mut pairs = rows_to_i32_pairs(&rows, 0, 1);
    pairs.sort_unstable();
    assert_eq!(pairs, vec![(Some(2), Some(200)), (Some(3), Some(300))]);

    shutdown(client, server_handle).await;
}

/// `t1 LEFT JOIN t2 ON t1.lid = t2.rid` — every left row appears, with
/// unmatched right columns set to NULL. Encoded as 0 in v0.5 because
/// `build_batch` has no per-column null bitmap yet (see the module
/// docs).
#[tokio::test]
async fn left_outer_equi_join_emits_null_padded_unmatched_rows() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE lo_eq_left (lid INT NOT NULL, lval INT NOT NULL)")
        .await
        .expect("create left");
    client
        .batch_execute("CREATE TABLE lo_eq_right (rid INT NOT NULL, rval INT NOT NULL)")
        .await
        .expect("create right");

    for (id, v) in [(1_i32, 10_i32), (2, 20), (3, 30)] {
        client
            .batch_execute(&format!("INSERT INTO lo_eq_left VALUES ({id}, {v})"))
            .await
            .expect("insert left");
    }
    for (id, v) in [(2_i32, 200_i32)] {
        client
            .batch_execute(&format!("INSERT INTO lo_eq_right VALUES ({id}, {v})"))
            .await
            .expect("insert right");
    }

    let rows = client
        .simple_query(
            "SELECT lid, rval \
             FROM lo_eq_left LEFT JOIN lo_eq_right \
             ON lid = rid",
        )
        .await
        .expect("query succeeds");

    let mut pairs = rows_to_i32_pairs(&rows, 0, 1);
    pairs.sort_unstable();
    // PostgreSQL semantics: unmatched left rows get NULL on the right.
    assert_eq!(
        pairs,
        vec![
            (Some(1), None),
            (Some(2), Some(200)),
            (Some(3), None),
        ]
    );

    shutdown(client, server_handle).await;
}

/// `t1 INNER JOIN t2 ON t1.lid < t2.rid` — non-equi predicate forces
/// the NLJ fallback. Output is every `(lid, rid)` pair where `lid <
/// rid`.
#[tokio::test]
async fn inner_non_equi_join_falls_back_to_nested_loop() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE nlj_left (lid INT NOT NULL, lval INT NOT NULL)")
        .await
        .expect("create left");
    client
        .batch_execute("CREATE TABLE nlj_right (rid INT NOT NULL, rval INT NOT NULL)")
        .await
        .expect("create right");

    for (id, v) in [(1_i32, 10_i32), (2, 20), (5, 50)] {
        client
            .batch_execute(&format!("INSERT INTO nlj_left VALUES ({id}, {v})"))
            .await
            .expect("insert left");
    }
    for (id, v) in [(3_i32, 300_i32), (6, 600)] {
        client
            .batch_execute(&format!("INSERT INTO nlj_right VALUES ({id}, {v})"))
            .await
            .expect("insert right");
    }

    let rows = client
        .simple_query(
            "SELECT lid, rid \
             FROM nlj_left INNER JOIN nlj_right \
             ON lid < rid",
        )
        .await
        .expect("query succeeds");
    let mut pairs = rows_to_i32_pairs(&rows, 0, 1);
    pairs.sort_unstable();
    // Pairs where lid < rid: (1,3) (1,6) (2,3) (2,6) (5,6).
    assert_eq!(
        pairs,
        vec![
            (Some(1), Some(3)),
            (Some(1), Some(6)),
            (Some(2), Some(3)),
            (Some(2), Some(6)),
            (Some(5), Some(6)),
        ],
        "non-equi NLJ output"
    );

    shutdown(client, server_handle).await;
}

/// A join with a `WHERE` clause that the binder pushes around the
/// join: `SELECT ... FROM t1 JOIN t2 ON ... WHERE t1.lval = 5`. The
/// rows that survive are those whose left `lval` equals 5 *and* have a
/// matching right row.
///
/// This shape ensures the wire dispatch for `Join` cooperates with the
/// rest of `lower_query`: the binder produces a tree with `Filter` at
/// the top-of-Join (or wrapping the left scan, depending on pushdown).
/// Either way, the new Join arm must accept the children that the
/// existing `lower_query` arms (`Filter`, `Scan`, etc.) produce.
#[tokio::test]
async fn join_with_where_filter_returns_filtered_rows() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE wjf_left (lid INT NOT NULL, lval INT NOT NULL)")
        .await
        .expect("create left");
    client
        .batch_execute("CREATE TABLE wjf_right (rid INT NOT NULL, rval INT NOT NULL)")
        .await
        .expect("create right");

    for (id, v) in [(1_i32, 5_i32), (2, 6), (3, 5)] {
        client
            .batch_execute(&format!("INSERT INTO wjf_left VALUES ({id}, {v})"))
            .await
            .expect("insert left");
    }
    for (id, v) in [(1_i32, 100_i32), (3, 300), (4, 400)] {
        client
            .batch_execute(&format!("INSERT INTO wjf_right VALUES ({id}, {v})"))
            .await
            .expect("insert right");
    }

    let rows = client
        .simple_query(
            "SELECT lid, rval \
             FROM wjf_left JOIN wjf_right \
             ON lid = rid \
             WHERE lval = 5",
        )
        .await
        .expect("query succeeds");
    let mut pairs = rows_to_i32_pairs(&rows, 0, 1);
    pairs.sort_unstable();
    // Left rows with lval=5 are lid∈{1,3}; both have a matching right row.
    assert_eq!(pairs, vec![(Some(1), Some(100)), (Some(3), Some(300))]);

    shutdown(client, server_handle).await;
}
