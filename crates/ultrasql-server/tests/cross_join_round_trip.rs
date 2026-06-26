//! End-to-end CROSS JOIN / comma-join tests against a real
//! `tokio-postgres` client.
//!
//! Regression coverage for the "projection index out of bounds" wrong-result
//! class: a projection over a comma cross join or explicit `CROSS JOIN` used to
//! hard-error whenever the two joined relations shared a column name (the common
//! case, e.g. both expose `id`).
//!
//! Root cause was in the optimizer's join-reorder path: `concat_schemas`
//! (`crates/ultrasql-optimizer/src/enumeration/mod.rs`) built the reordered
//! cross-join's output schema with `Schema::new(fields).unwrap_or_else(|_|
//! Schema::empty())`. `Schema::new` rejects duplicate column names, so on a
//! name collision the schema silently collapsed to *zero* width and the
//! restoring `Project` then indexed out of bounds. The fix preserves the join
//! width with `Schema::new_with_duplicate_names`; the reordered tree executes by
//! ordinal and the restoring `Project` re-imposes the binder's deduplicated
//! schema, so intermediate field names are irrelevant.
//!
//! Each test seeds `l(id)` with `{1, 2}` and `r(id)` with `{10, 20, 30}` (plus,
//! where a control needs overlap, `r2(id)` with `{1, 2, 3}`) and asserts the
//! exact row count and values PostgreSQL would emit for the same data.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio_postgres::NoTls;
use ultrasql_server::{Server, bind_listener, serve_listener};

/// Spin up an in-process server on an ephemeral TCP port and return a
/// connected `tokio-postgres` client plus the server task handle.
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
        "host={host} port={port} user=tester application_name=cross_join_test",
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

/// Tidy shutdown: drop the client, give the connection task a beat to flush,
/// then abort the listener.
async fn shutdown(
    client: tokio_postgres::Client,
    server_handle: tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
) {
    drop(client);
    tokio::time::sleep(Duration::from_millis(20)).await;
    server_handle.abort();
}

/// Seed `l(id) = {1, 2}` and `r(id) = {10, 20, 30}`.
async fn seed_l_r(client: &tokio_postgres::Client) {
    client
        .batch_execute("CREATE TABLE l (id INT NOT NULL)")
        .await
        .expect("create l");
    client
        .batch_execute("CREATE TABLE r (id INT NOT NULL)")
        .await
        .expect("create r");
    for v in [1, 2] {
        client
            .batch_execute(&format!("INSERT INTO l VALUES ({v})"))
            .await
            .expect("insert l");
    }
    for v in [10, 20, 30] {
        client
            .batch_execute(&format!("INSERT INTO r VALUES ({v})"))
            .await
            .expect("insert r");
    }
}

/// Seed an extra `r2(id) = {1, 2, 3}` that overlaps `l` on `{1, 2}` so equi
/// joins match and `<` predicates genuinely filter.
async fn seed_r2(client: &tokio_postgres::Client) {
    client
        .batch_execute("CREATE TABLE r2 (id INT NOT NULL)")
        .await
        .expect("create r2");
    for v in [1, 2, 3] {
        client
            .batch_execute(&format!("INSERT INTO r2 VALUES ({v})"))
            .await
            .expect("insert r2");
    }
}

/// Count the `DataRow` messages in a simple-query result.
fn row_count(rows: &[tokio_postgres::SimpleQueryMessage]) -> usize {
    rows.iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count()
}

/// Collect a single `i32` column from each row.
fn col_i32(rows: &[tokio_postgres::SimpleQueryMessage], col: usize) -> Vec<i32> {
    rows.iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(
                row.get(col)
                    .and_then(|s| s.parse::<i32>().ok())
                    .expect("i32"),
            ),
            _ => None,
        })
        .collect()
}

/// Collect two `i32` columns from each row as a pair.
fn cols_i32_pair(
    rows: &[tokio_postgres::SimpleQueryMessage],
    a: usize,
    b: usize,
) -> Vec<(i32, i32)> {
    rows.iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => {
                let l = row.get(a).and_then(|s| s.parse::<i32>().ok()).expect("i32");
                let r = row.get(b).and_then(|s| s.parse::<i32>().ok()).expect("i32");
                Some((l, r))
            }
            _ => None,
        })
        .collect()
}

/// Collect three `i32` columns from each row as a triple.
fn cols_i32_triple(rows: &[tokio_postgres::SimpleQueryMessage]) -> Vec<(i32, i32, i32)> {
    rows.iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => {
                let a = row.get(0).and_then(|s| s.parse::<i32>().ok()).expect("i32");
                let b = row.get(1).and_then(|s| s.parse::<i32>().ok()).expect("i32");
                let c = row.get(2).and_then(|s| s.parse::<i32>().ok()).expect("i32");
                Some((a, b, c))
            }
            _ => None,
        })
        .collect()
}

/// 1. `SELECT l.id, r.id FROM l, r` — full Cartesian product, 6 rows, with the
///    correct `(l.id, r.id)` pairs. This is the canonical duplicate-column-name
///    comma cross join that used to error "projection index out of bounds".
#[tokio::test]
async fn comma_cross_join_projects_both_id_columns() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;
    seed_l_r(&client).await;

    let rows = client
        .simple_query("SELECT l.id, r.id FROM l, r")
        .await
        .expect("query succeeds");
    assert_eq!(row_count(&rows), 6, "2 x 3 Cartesian product");

    let mut pairs = cols_i32_pair(&rows, 0, 1);
    pairs.sort_unstable();
    assert_eq!(
        pairs,
        vec![(1, 10), (1, 20), (1, 30), (2, 10), (2, 20), (2, 30)]
    );

    shutdown(client, server_handle).await;
}

/// 2. `SELECT * FROM l CROSS JOIN r` — explicit CROSS JOIN, 6 rows, both `id`
///    columns present (left in col 0, right in col 1).
#[tokio::test]
async fn explicit_cross_join_star_has_both_id_columns() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;
    seed_l_r(&client).await;

    let rows = client
        .simple_query("SELECT * FROM l CROSS JOIN r")
        .await
        .expect("query succeeds");
    assert_eq!(row_count(&rows), 6);

    let mut pairs = cols_i32_pair(&rows, 0, 1);
    pairs.sort_unstable();
    assert_eq!(
        pairs,
        vec![(1, 10), (1, 20), (1, 30), (2, 10), (2, 20), (2, 30)]
    );

    shutdown(client, server_handle).await;
}

/// 3. `SELECT count(*) FROM l, r` — a single row whose value is the Cartesian
///    cardinality, 6.
#[tokio::test]
async fn comma_cross_join_count_star_is_cardinality() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;
    seed_l_r(&client).await;

    let rows = client
        .simple_query("SELECT count(*) FROM l, r")
        .await
        .expect("query succeeds");
    assert_eq!(row_count(&rows), 1);
    assert_eq!(col_i32(&rows, 0), vec![6]);

    shutdown(client, server_handle).await;
}

/// 4. `SELECT 1 FROM l, r` — constant projection over a cross join still emits
///    one row per Cartesian pair (composes with the const-projection fix).
#[tokio::test]
async fn comma_cross_join_const_projection_emits_one_row_per_pair() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;
    seed_l_r(&client).await;

    let rows = client
        .simple_query("SELECT 1 FROM l, r")
        .await
        .expect("query succeeds");
    assert_eq!(row_count(&rows), 6);
    assert_eq!(col_i32(&rows, 0), vec![1, 1, 1, 1, 1, 1]);

    shutdown(client, server_handle).await;
}

/// 5. Three-way comma join `SELECT a.id, b.id, c.id FROM l a, r b, l c` —
///    `2 * 3 * 2 = 12` rows. Exercises the multi-way reorder spine with
///    duplicate names across three leaves.
#[tokio::test]
async fn three_way_comma_join_is_full_product() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;
    seed_l_r(&client).await;

    let rows = client
        .simple_query("SELECT a.id, b.id, c.id FROM l a, r b, l c")
        .await
        .expect("query succeeds");
    assert_eq!(row_count(&rows), 12, "2 x 3 x 2");

    let mut triples = cols_i32_triple(&rows);
    triples.sort_unstable();
    let mut expected = Vec::new();
    for a in [1, 2] {
        for b in [10, 20, 30] {
            for c in [1, 2] {
                expected.push((a, b, c));
            }
        }
    }
    expected.sort_unstable();
    assert_eq!(triples, expected);

    shutdown(client, server_handle).await;
}

/// 6. Comma self-join `SELECT a.id, b.id FROM l a, l b` — the same table joined
///    to itself, 4 rows. The duplicate-name case where *both* sides come from
///    the identical relation schema.
#[tokio::test]
async fn comma_self_join_is_full_product() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;
    seed_l_r(&client).await;

    let rows = client
        .simple_query("SELECT a.id, b.id FROM l a, l b")
        .await
        .expect("query succeeds");
    assert_eq!(row_count(&rows), 4, "2 x 2");

    let mut pairs = cols_i32_pair(&rows, 0, 1);
    pairs.sort_unstable();
    assert_eq!(pairs, vec![(1, 1), (1, 2), (2, 1), (2, 2)]);

    shutdown(client, server_handle).await;
}

/// 7. Duplicate-name comma join whose WHERE becomes a join predicate:
///    `SELECT a.id, b.id FROM l a, r2 b WHERE a.id < b.id`. With
///    `l = {1, 2}` and `r2 = {1, 2, 3}`, only `(1,2), (1,3), (2,3)` survive —
///    proving the equi/predicate path still filters correctly (no regression).
#[tokio::test]
async fn comma_join_with_filter_predicate_filters_correctly() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;
    seed_l_r(&client).await;
    seed_r2(&client).await;

    let rows = client
        .simple_query("SELECT a.id, b.id FROM l a, r2 b WHERE a.id < b.id")
        .await
        .expect("query succeeds");
    assert_eq!(row_count(&rows), 3, "pairs where a.id < b.id");

    let mut pairs = cols_i32_pair(&rows, 0, 1);
    pairs.sort_unstable();
    assert_eq!(pairs, vec![(1, 2), (1, 3), (2, 3)]);

    shutdown(client, server_handle).await;
}

/// 8. Control: `SELECT l.id, r2.id FROM l JOIN r2 ON l.id = r2.id` — an explicit
///    INNER JOIN ON (which already worked) still matches correctly. With the
///    overlapping `r2 = {1, 2, 3}` this yields `(1,1), (2,2)`. The non-empty
///    result rules out a silently-empty false pass.
#[tokio::test]
async fn inner_join_on_equi_predicate_still_matches() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;
    seed_l_r(&client).await;
    seed_r2(&client).await;

    let rows = client
        .simple_query("SELECT l.id, r2.id FROM l JOIN r2 ON l.id = r2.id")
        .await
        .expect("query succeeds");
    assert_eq!(row_count(&rows), 2);

    let mut pairs = cols_i32_pair(&rows, 0, 1);
    pairs.sort_unstable();
    assert_eq!(pairs, vec![(1, 1), (2, 2)]);

    shutdown(client, server_handle).await;
}
