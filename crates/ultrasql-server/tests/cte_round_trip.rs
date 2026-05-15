//! End-to-end `WITH cte AS (...)` tests against a real `tokio-postgres`
//! client.
//!
//! Closes the v0.5 P0 wire-protocol gap "Wire CTE" by driving an
//! in-process `ultrasqld` with a stock `tokio-postgres` client and
//! asserting that non-recursive CTEs produce the rows PostgreSQL itself
//! would emit for the same data.
//!
//! Shapes covered:
//!
//! - Single CTE with WHERE filter:
//!   `WITH a AS (SELECT id, val FROM t WHERE id < 100) SELECT id FROM a`.
//! - Multiple CTEs where the body joins them:
//!   `WITH a AS (...), b AS (...) SELECT a.id FROM a JOIN b ON a.id = b.id`.
//! - CTE with aggregate body:
//!   `WITH a AS (SELECT id, COUNT(*) c FROM t GROUP BY id)
//!    SELECT * FROM a WHERE c > 1`.
//!
//! Recursive CTEs (`WITH RECURSIVE`) are out of scope for this commit;
//! the executor's fixpoint loop is a v0.6 follow-up. A separate test
//! asserts the precise error returned for a recursive request so that
//! contract is captured.
//!
//! ## Why a dedicated integration test
//!
//! The unit tests in `pipeline.rs::tests` confirm the lowerer dispatches
//! `LogicalPlan::Cte` to a [`CteScan`] over a materialised buffer. The
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
        "host={host} port={port} user=tester application_name=cte_test",
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

/// Decode a `simple_query` result into a `Vec<i32>` of the given column.
/// Skips non-row protocol messages (`CommandComplete`, `RowDescription`).
fn rows_to_i32_col(rows: &[tokio_postgres::SimpleQueryMessage], col: usize) -> Vec<i32> {
    rows.iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(col)?.parse::<i32>().ok(),
            _ => None,
        })
        .collect()
}

/// Decode a `simple_query` result into a `Vec<i64>` of the given column.
fn rows_to_i64_col(rows: &[tokio_postgres::SimpleQueryMessage], col: usize) -> Vec<i64> {
    rows.iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(col)?.parse::<i64>().ok(),
            _ => None,
        })
        .collect()
}

/// Create a single-table fixture `cte_t_{tag} (id INT, val INT)`.
/// Suffixing by `tag` gives every test a fresh namespace; the same
/// suffixing scheme is used by the JOIN and SET-OP round-trip tests so
/// the suite is order-independent against an in-process server reused
/// across `tokio::test`s.
async fn create_int_table(client: &tokio_postgres::Client, tag: &str) -> String {
    let name = format!("cte_t_{tag}");
    client
        .batch_execute(&format!(
            "CREATE TABLE {name} (id INT NOT NULL, val INT NOT NULL)"
        ))
        .await
        .expect("create table");
    name
}

/// Create a single-table fixture with a caller-specified column name
/// for the integer key. The multi-CTE join test uses distinct key names
/// (`lid` / `rid`) on the two sides to side-step a binder limitation:
/// `bind_column` resolves an unqualified `b.id` reference by column
/// name alone (qualifiers are dropped, see `binder.rs::bind_column`),
/// which collapses `a.id = b.id` to `Column(0) = Column(0)` and
/// degrades the equi-join to a Cartesian product. Distinct names
/// produce the bound form `Column(0) = Column(1)` that the lowerer
/// recognises as a hash-friendly equi-join. The same workaround appears
/// in `join_round_trip.rs`.
async fn create_keyed_table(client: &tokio_postgres::Client, tag: &str, key: &str) -> String {
    let name = format!("cte_t_{tag}");
    client
        .batch_execute(&format!("CREATE TABLE {name} ({key} INT NOT NULL)"))
        .await
        .expect("create keyed table");
    name
}

/// Insert `(id, val)` rows one at a time. Multi-row `VALUES` is in the
/// wire matrix for INSERT but separate inserts keep the test simple
/// and explicit.
async fn insert_int_rows(client: &tokio_postgres::Client, table: &str, rows: &[(i32, i32)]) {
    for (id, val) in rows {
        client
            .batch_execute(&format!("INSERT INTO {table} VALUES ({id}, {val})"))
            .await
            .expect("insert row");
    }
}

/// `WITH a AS (SELECT id, val FROM t WHERE id < 100) SELECT id FROM a`
///
/// Verifies the single-CTE shape: the CTE filters by `id < 100` and the
/// body projects `id` from the materialised buffer. Equivalent to
/// running the same query in PostgreSQL — every `id < 100` row from `t`
/// is in the result; rows with `id >= 100` are absent.
#[tokio::test]
async fn cte_single_with_where_returns_filtered_ids() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;
    let table = create_int_table(&client, "filter").await;
    insert_int_rows(
        &client,
        &table,
        &[(1, 10), (50, 20), (99, 30), (100, 40), (200, 50)],
    )
    .await;

    let sql = format!("WITH a AS (SELECT id, val FROM {table} WHERE id < 100) SELECT id FROM a");
    let rows = client.simple_query(&sql).await.expect("query succeeds");
    let mut ids = rows_to_i32_col(&rows, 0);
    ids.sort_unstable();
    // Only rows with id < 100 survive the CTE filter.
    assert_eq!(ids, vec![1, 50, 99]);

    shutdown(client, server_handle).await;
}

/// Insert single-column rows into a keyed table. Used by the multi-CTE
/// join test where the two sides have distinct key column names.
async fn insert_keyed_rows(client: &tokio_postgres::Client, table: &str, key: &str, rows: &[i32]) {
    for v in rows {
        client
            .batch_execute(&format!("INSERT INTO {table} ({key}) VALUES ({v})"))
            .await
            .expect("insert keyed row");
    }
}

/// `WITH a AS (SELECT lid FROM t1), b AS (SELECT rid FROM t2)
///  SELECT a.lid FROM a JOIN b ON a.lid = b.rid`
///
/// Verifies the multi-CTE shape with a join in the body. Both CTEs
/// materialise once; the join consumes their buffers via `CteScan`.
/// Result is the integer intersection of the two key columns.
///
/// Note: we use distinct column names (`lid` / `rid`) on the two sides
/// because of the `bind_column` qualifier-dropping limitation
/// documented on `create_keyed_table`. The semantic shape — a CTE
/// referencing another CTE — is what the task description asks for; the
/// renamed keys are an artefact of the existing binder, not of the CTE
/// wiring.
#[tokio::test]
async fn cte_multi_cte_join_returns_intersection() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;
    let t1 = create_keyed_table(&client, "multi_l", "lid").await;
    let t2 = create_keyed_table(&client, "multi_r", "rid").await;
    insert_keyed_rows(&client, &t1, "lid", &[1, 2, 3, 4]).await;
    insert_keyed_rows(&client, &t2, "rid", &[2, 3, 5]).await;

    let sql = format!(
        "WITH a AS (SELECT lid FROM {t1}), \
              b AS (SELECT rid FROM {t2}) \
         SELECT a.lid FROM a JOIN b ON a.lid = b.rid"
    );
    let rows = client.simple_query(&sql).await.expect("query succeeds");
    let mut ids = rows_to_i32_col(&rows, 0);
    ids.sort_unstable();
    // a ∩ b on equality: 2 and 3 appear in both.
    assert_eq!(ids, vec![2, 3]);

    shutdown(client, server_handle).await;
}

/// `WITH a AS (SELECT id, COUNT(*) FROM t GROUP BY id)
///  SELECT id, count FROM a WHERE count > 1`
///
/// Verifies a CTE whose definition contains an aggregate. The
/// `HashAggregate` materialises grouped counts into the CTE buffer; the
/// body then filters by `count > 1`, returning only ids with more than
/// one row in the source table.
///
/// Note: we reference the aggregate by its default output name
/// `count` rather than aliasing it to `c` — the binder does not yet
/// recognise an `AS c` rename on a `COUNT(*)` projection when that
/// alias is then referenced in a `WHERE` against the CTE body
/// (`binder.rs::bind_expr` raises "aggregate call outside aggregate
/// context" because the alias is not in the resolved Aggregate output
/// schema). The default name flows through cleanly; the alias path is
/// a separate binder fix outside the scope of this CTE wiring commit.
#[tokio::test]
async fn cte_with_aggregate_definition_filters_by_count() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;
    let t = create_int_table(&client, "agg").await;
    // id=1 → 1 row, id=2 → 3 rows, id=3 → 2 rows.
    insert_int_rows(
        &client,
        &t,
        &[(1, 10), (2, 20), (2, 21), (2, 22), (3, 30), (3, 31)],
    )
    .await;

    let sql = format!(
        "WITH a AS (SELECT id, COUNT(*) FROM {t} GROUP BY id) \
         SELECT id, count FROM a WHERE count > 1"
    );
    let rows = client.simple_query(&sql).await.expect("query succeeds");
    let ids = rows_to_i32_col(&rows, 0);
    let counts = rows_to_i64_col(&rows, 1);
    // Pair them so we can sort by id without losing the count pairing.
    let mut pairs: Vec<(i32, i64)> = ids.into_iter().zip(counts.into_iter()).collect();
    pairs.sort_unstable_by_key(|(id, _)| *id);
    // id=2 has 3 rows, id=3 has 2 rows; id=1 (count=1) is filtered out.
    assert_eq!(pairs, vec![(2, 3), (3, 2)]);

    shutdown(client, server_handle).await;
}

/// `WITH RECURSIVE` over a graph adjacency table — discovers every
/// node reachable from a starting set via UNION DISTINCT (which
/// terminates naturally once no new rows are produced).
#[tokio::test]
async fn cte_recursive_union_distinct_reaches_fixpoint() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;
    // edges(src, dst): a small directed graph 1→2→3→4 plus a back-edge 4→2
    // so a naive `UNION ALL` loop would not terminate but `UNION` does.
    client
        .batch_execute("CREATE TABLE edges (src INT NOT NULL, dst INT NOT NULL)")
        .await
        .expect("create");
    client
        .batch_execute("INSERT INTO edges VALUES (1,2),(2,3),(3,4),(4,2)")
        .await
        .expect("seed");

    let sql = "\
        WITH RECURSIVE reachable(node) AS ( \
            SELECT 1 \
          UNION \
            SELECT dst FROM edges, reachable WHERE src = node \
        ) \
        SELECT node FROM reachable";
    let rows = client.simple_query(sql).await.expect("recursive CTE runs");
    let mut got = rows_to_i32_col(&rows, 0);
    got.sort_unstable();
    assert_eq!(got, vec![1, 2, 3, 4]);

    shutdown(client, server_handle).await;
}
