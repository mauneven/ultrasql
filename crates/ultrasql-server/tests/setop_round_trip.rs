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
    // left counts: {1:1, 2:3, 3:1}; right counts: {2:2, 3:1, 4:1}.
    // INTERSECT ALL = {1:0, 2:2, 3:1, 4:0} -> [2, 2, 3].
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
    // EXCEPT ALL = {1:1, 2:2, 3:1, 4:0} -> [1, 2, 2, 3].
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

// ---------------------------------------------------------------------------
// Mixed-width adversarial battery.
//
// Set operations over corresponding columns of DIFFERENT-but-compatible
// numeric width must (a) NOT error and (b) NOT silently drop rows. Before
// the fix, `bind_set_op` computed a unified output type per column but
// inserted no cast on either child, so:
//   * UNION over int4/int8 ERRORED in the batch builder ("expected Int64,
//     got Int32"); and
//   * INTERSECT/EXCEPT over equal int4/int8 values SILENTLY RETURNED 0
//     ROWS, because cross-width `Value`/`RowKey` equality is always false.
// The fix casts each side's differing columns to the unified type, so both
// children produce same-width columns and every comparison is same-width.
// ---------------------------------------------------------------------------

/// PostgreSQL type OIDs used to assert the unified set-op output type.
const OID_INT8: u32 = 20;
const OID_NUMERIC: u32 = 1700;
const OID_FLOAT8: u32 = 701;

/// Decode a `simple_query` result into the trimmed text of one column,
/// preserving every emitted row (including duplicates) so multiset shapes
/// are observable. NULLs decode to the empty string.
fn rows_to_text_col(rows: &[tokio_postgres::SimpleQueryMessage], col: usize) -> Vec<String> {
    rows.iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => {
                Some(row.get(col).unwrap_or("").to_owned())
            }
            _ => None,
        })
        .collect()
}

/// Create `setop_mix_<tag>(a INT, b BIGINT)` and seed it so the two
/// columns hold equal values at different widths: a=[1,2,3], b=[2,3,4].
async fn create_mixed_width_table(client: &tokio_postgres::Client, tag: &str) -> String {
    let table = format!("setop_mix_{tag}");
    client
        .batch_execute(&format!(
            "CREATE TABLE {table} (a INT NOT NULL, b BIGINT NOT NULL)"
        ))
        .await
        .expect("create mixed-width table");
    for (a, b) in [(1_i32, 2_i64), (2, 3), (3, 4)] {
        client
            .batch_execute(&format!("INSERT INTO {table} VALUES ({a}, {b})"))
            .await
            .expect("insert mixed-width row");
    }
    table
}

/// Battery #1 — `SELECT a UNION SELECT b` (int4 vs int8). Before the fix
/// this ERRORED in the batch builder. After: it succeeds, dedupes, and the
/// unified output type is bigint (int8).
#[tokio::test]
async fn mixed_width_union_succeeds_and_yields_bigint() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;
    let table = create_mixed_width_table(&client, "union").await;

    // a = {1,2,3}, b = {2,3,4} -> distinct union {1,2,3,4}.
    let rows = client
        .simple_query(&format!(
            "SELECT a FROM {table} UNION SELECT b FROM {table}"
        ))
        .await
        .expect("mixed-width UNION succeeds");
    let mut ids: Vec<i64> = rows_to_text_col(&rows, 0)
        .iter()
        .map(|s| s.parse().expect("bigint"))
        .collect();
    ids.sort_unstable();
    assert_eq!(ids, vec![1, 2, 3, 4]);

    // The unified column type must be bigint (int8).
    let prepared = client
        .prepare(&format!(
            "SELECT a FROM {table} UNION SELECT b FROM {table}"
        ))
        .await
        .expect("prepare mixed UNION");
    assert_eq!(prepared.columns()[0].type_().oid(), OID_INT8);

    shutdown(client, server_handle).await;
}

/// Battery #2 — THE CORRUPTION GATE. `SELECT a INTERSECT SELECT b`
/// (int4 vs int8). Equal cross-width values are 2 and 3. Before the fix
/// this SILENTLY RETURNED 0 ROWS; after, it returns the common values.
#[tokio::test]
async fn mixed_width_intersect_returns_common_values_not_zero_rows() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;
    let table = create_mixed_width_table(&client, "intersect").await;

    // a = {1,2,3}, b = {2,3,4} -> INTERSECT {2,3}.
    let rows = client
        .simple_query(&format!(
            "SELECT a FROM {table} INTERSECT SELECT b FROM {table}"
        ))
        .await
        .expect("mixed-width INTERSECT succeeds");
    let mut ids: Vec<i64> = rows_to_text_col(&rows, 0)
        .iter()
        .map(|s| s.parse().expect("bigint"))
        .collect();
    ids.sort_unstable();
    // The bug returned []; the correct answer is the cross-width common set.
    assert_eq!(ids, vec![2, 3], "INTERSECT must not silently drop rows");

    shutdown(client, server_handle).await;
}

/// Battery #3 — `SELECT a EXCEPT SELECT b` (int4 vs int8). a minus b as a
/// cross-width set: {1,2,3} - {2,3,4} = {1}.
#[tokio::test]
async fn mixed_width_except_returns_cross_width_difference() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;
    let table = create_mixed_width_table(&client, "except").await;

    let rows = client
        .simple_query(&format!(
            "SELECT a FROM {table} EXCEPT SELECT b FROM {table}"
        ))
        .await
        .expect("mixed-width EXCEPT succeeds");
    let mut ids: Vec<i64> = rows_to_text_col(&rows, 0)
        .iter()
        .map(|s| s.parse().expect("bigint"))
        .collect();
    ids.sort_unstable();
    assert_eq!(ids, vec![1], "EXCEPT must compare cross-width values equal");

    shutdown(client, server_handle).await;
}

/// Battery #4 — `SELECT 1 UNION SELECT 1::bigint` dedupes to one row,
/// because `1 == 1::bigint` once both sides are widened to int8.
#[tokio::test]
async fn mixed_width_union_dedups_equal_literals() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    let rows = client
        .simple_query("SELECT 1 UNION SELECT 1::bigint")
        .await
        .expect("literal UNION succeeds");
    let vals = rows_to_text_col(&rows, 0);
    assert_eq!(
        vals,
        vec!["1".to_owned()],
        "1 and 1::bigint dedupe to one row"
    );

    shutdown(client, server_handle).await;
}

/// Battery #5 — `SELECT 1 UNION ALL SELECT 1::bigint` keeps both rows
/// (ALL = multiset, no dedup) even though the values are equal.
#[tokio::test]
async fn mixed_width_union_all_keeps_equal_literals() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    let rows = client
        .simple_query("SELECT 1 UNION ALL SELECT 1::bigint")
        .await
        .expect("literal UNION ALL succeeds");
    let vals = rows_to_text_col(&rows, 0);
    assert_eq!(vals.len(), 2, "UNION ALL keeps both equal rows");
    assert!(vals.iter().all(|v| v == "1"));

    shutdown(client, server_handle).await;
}

/// Battery #6 — int + numeric unifies to numeric (dedupes to one row);
/// int + float8 unifies to double precision.
#[tokio::test]
async fn mixed_width_union_int_numeric_and_int_float() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    // int + numeric -> numeric, 1 == 1.0 dedupes to one row.
    let rows = client
        .simple_query("SELECT 1::int UNION SELECT 1.0::numeric")
        .await
        .expect("int/numeric UNION succeeds");
    assert_eq!(rows_to_text_col(&rows, 0).len(), 1, "1 == 1.0 dedupes");
    let prepared = client
        .prepare("SELECT 1::int UNION SELECT 1.0::numeric")
        .await
        .expect("prepare int/numeric UNION");
    assert_eq!(prepared.columns()[0].type_().oid(), OID_NUMERIC);

    // int + float8 -> double precision, 1 == 1.0 dedupes to one row.
    let rows = client
        .simple_query("SELECT 1::int UNION SELECT 1.0::float8")
        .await
        .expect("int/float8 UNION succeeds");
    assert_eq!(
        rows_to_text_col(&rows, 0).len(),
        1,
        "1 == 1.0 dedupes (float8)"
    );
    let prepared = client
        .prepare("SELECT 1::int UNION SELECT 1.0::float8")
        .await
        .expect("prepare int/float8 UNION");
    assert_eq!(prepared.columns()[0].type_().oid(), OID_FLOAT8);

    shutdown(client, server_handle).await;
}

/// Battery #7 — NULL dedup across widths. `NULL::int UNION NULL::bigint`
/// yields one row (NULLs are equal for set-op dedup in PG);
/// `... UNION ALL ...` yields two.
#[tokio::test]
async fn mixed_width_null_union_dedup_semantics() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    let rows = client
        .simple_query("SELECT NULL::int UNION SELECT NULL::bigint")
        .await
        .expect("NULL UNION succeeds");
    assert_eq!(
        rows_to_text_col(&rows, 0).len(),
        1,
        "NULLs dedupe to one row"
    );

    let rows = client
        .simple_query("SELECT NULL::int UNION ALL SELECT NULL::bigint")
        .await
        .expect("NULL UNION ALL succeeds");
    assert_eq!(
        rows_to_text_col(&rows, 0).len(),
        2,
        "UNION ALL keeps both NULLs"
    );

    shutdown(client, server_handle).await;
}

/// Battery #8 — three-column set op with mixed widths in different
/// positions. Column 1 is int4/int8, column 3 is int8/int4; column 2 is
/// same width on both sides.
#[tokio::test]
async fn mixed_width_three_column_union_mixed_positions() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    // Left  row: (1::int4, 5::int4, 9::int8)
    // Right row: (1::int8, 5::int4, 9::int4)  -> identical cross-width tuple.
    // Distinct UNION must collapse the two equal tuples into one row.
    let rows = client
        .simple_query(
            "SELECT 1::int, 5::int, 9::bigint \
             UNION \
             SELECT 1::bigint, 5::int, 9::int",
        )
        .await
        .expect("3-column mixed UNION succeeds");
    let row_count = rows
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count();
    assert_eq!(row_count, 1, "equal cross-width 3-tuples dedupe to one row");

    shutdown(client, server_handle).await;
}

/// Battery #9 — genuinely incompatible types must still ERROR cleanly,
/// not silently coerce. int UNION text has no implicit cast.
#[tokio::test]
async fn mixed_width_incompatible_types_error() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    let result = client
        .simple_query("SELECT 1::int UNION SELECT 'x'::text")
        .await;
    assert!(
        result.is_err(),
        "int UNION text must error, not silently coerce"
    );

    shutdown(client, server_handle).await;
}

/// Battery #10 — control: same-width int4 UNION int4 is unchanged. (The
/// no-extra-`Project` invariant for same-width sides is asserted by the
/// planner unit test `set_op_same_width_inserts_no_cast_project`.)
#[tokio::test]
async fn same_width_union_unchanged() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;
    let (left, right) = create_setop_tables(&client, "same_width").await;
    insert_rows(&client, &left, &[1, 2, 3]).await;
    insert_rows(&client, &right, &[2, 3, 4]).await;

    let rows = client
        .simple_query(&format!(
            "SELECT id FROM {left} UNION SELECT id FROM {right}"
        ))
        .await
        .expect("same-width UNION succeeds");
    let mut ids = rows_to_i32_col(&rows, 0);
    ids.sort_unstable();
    assert_eq!(ids, vec![1, 2, 3, 4]);

    // Type stays integer (int4), not widened.
    let prepared = client
        .prepare(&format!(
            "SELECT id FROM {left} UNION SELECT id FROM {right}"
        ))
        .await
        .expect("prepare same-width UNION");
    assert_eq!(prepared.columns()[0].type_().oid(), 23 /* int4 */);

    shutdown(client, server_handle).await;
}
