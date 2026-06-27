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

// ---------------------------------------------------------------------------
// Cross-category supertype battery (PG `select_common_type`).
//
// Set operations over corresponding columns whose types differ across a
// PG type category (temporal date/timestamp/timestamptz, string
// char/varchar/text, network inet/cidr) must resolve to the common
// supertype and cast every branch to it, so rows compare / dedupe / match
// in that type. Before the fix the binder kept the LEFT column's type and
// compared physically-different values, silently dropping rows.
// ---------------------------------------------------------------------------

/// PG type OIDs for the resolved cross-category set-op output type.
const OID_TIMESTAMP: u32 = 1114;
const OID_TIMESTAMPTZ: u32 = 1184;
const OID_TEXT: u32 = 25;
const OID_INET: u32 = 869;

/// Count the data rows in a `simple_query` result.
fn row_count(rows: &[tokio_postgres::SimpleQueryMessage]) -> usize {
    rows.iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count()
}

/// TEMPORAL #1 — `date 'd' UNION timestamp 'd 00:00:00'`. The date and the
/// midnight timestamp denote the same instant, so once both branches are
/// cast to `timestamp` they DEDUP to one row, and the output type is
/// `timestamp`.
#[tokio::test]
async fn temporal_date_union_timestamp_dedups_same_instant() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    let rows = client
        .simple_query("SELECT date '2024-01-01' UNION SELECT timestamp '2024-01-01 00:00:00'")
        .await
        .expect("date/timestamp UNION succeeds");
    assert_eq!(
        row_count(&rows),
        1,
        "date and midnight timestamp dedupe to one row"
    );

    let prepared = client
        .prepare("SELECT date '2024-01-01' UNION SELECT timestamp '2024-01-01 00:00:00'")
        .await
        .expect("prepare date/timestamp UNION");
    assert_eq!(prepared.columns()[0].type_().oid(), OID_TIMESTAMP);

    shutdown(client, server_handle).await;
}

/// TEMPORAL #2 — `date 'd' INTERSECT timestamp 'd 00:00:00'` returns the
/// matching instant (NOT silently 0 rows); a non-midnight timestamp does
/// NOT match the date and INTERSECT returns 0 rows correctly.
#[tokio::test]
async fn temporal_date_intersect_timestamp_matches_instant() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    // Matching instant: date midnight == timestamp midnight -> one row.
    let rows = client
        .simple_query("SELECT date '2024-01-01' INTERSECT SELECT timestamp '2024-01-01 00:00:00'")
        .await
        .expect("date/timestamp INTERSECT succeeds");
    assert_eq!(
        row_count(&rows),
        1,
        "matching instant must intersect, not silently drop"
    );

    // Non-matching: the timestamp has a non-zero time, so no shared instant.
    let rows = client
        .simple_query("SELECT date '2024-01-01' INTERSECT SELECT timestamp '2024-01-01 12:00:00'")
        .await
        .expect("date/timestamp INTERSECT (no match) succeeds");
    assert_eq!(
        row_count(&rows),
        0,
        "non-matching instants intersect to zero rows"
    );

    shutdown(client, server_handle).await;
}

/// TEMPORAL #3 — `date 'd' UNION timestamptz 'd 00:00:00+00'` resolves to
/// `timestamptz` and dedupes the same instant to one row.
#[tokio::test]
async fn temporal_date_union_timestamptz_resolves_timestamptz() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    let rows = client
        .simple_query("SELECT date '2024-01-01' UNION SELECT timestamptz '2024-01-01 00:00:00+00'")
        .await
        .expect("date/timestamptz UNION succeeds");
    assert_eq!(row_count(&rows), 1, "same instant dedupes to one row");

    let prepared = client
        .prepare("SELECT date '2024-01-01' UNION SELECT timestamptz '2024-01-01 00:00:00+00'")
        .await
        .expect("prepare date/timestamptz UNION");
    assert_eq!(prepared.columns()[0].type_().oid(), OID_TIMESTAMPTZ);

    shutdown(client, server_handle).await;
}

/// STRING #1 — `'abc'::char(3) UNION 'abc'::text` casts both to `text`;
/// equal text values dedupe to one row, output type is `text`.
#[tokio::test]
async fn string_char_union_text_dedups_equal() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    let rows = client
        .simple_query("SELECT 'abc'::char(3) UNION SELECT 'abc'::text")
        .await
        .expect("char/text UNION succeeds");
    assert_eq!(
        row_count(&rows),
        1,
        "equal char(3)/text values dedupe to one row"
    );

    let prepared = client
        .prepare("SELECT 'abc'::char(3) UNION SELECT 'abc'::text")
        .await
        .expect("prepare char/text UNION");
    assert_eq!(prepared.columns()[0].type_().oid(), OID_TEXT);

    shutdown(client, server_handle).await;
}

/// STRING #2 — `varchar INTERSECT text` returns the matching strings (not
/// silently empty).
#[tokio::test]
async fn string_varchar_intersect_text_matches() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    let rows = client
        .simple_query(
            "SELECT 'x'::varchar(8) UNION ALL SELECT 'y'::varchar(8) \
             INTERSECT \
             SELECT 'y'::text",
        )
        .await
        .expect("varchar/text INTERSECT succeeds");
    let vals = rows_to_text_col(&rows, 0);
    assert_eq!(vals, vec!["y".to_owned()], "matching string must survive");

    shutdown(client, server_handle).await;
}

/// NETWORK — `inet UNION cidr` of the same address dedupes to one row and
/// resolves to `inet`; `inet INTERSECT cidr` returns the matching address.
#[tokio::test]
async fn network_inet_cidr_union_intersect() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    // 192.168.1.0/24 as inet and as cidr denote the same address; once cidr
    // is cast to inet they dedupe to one row.
    let rows = client
        .simple_query("SELECT inet '192.168.1.0/24' UNION SELECT cidr '192.168.1.0/24'")
        .await
        .expect("inet/cidr UNION succeeds");
    assert_eq!(row_count(&rows), 1, "same address dedupes to one row");

    let prepared = client
        .prepare("SELECT inet '192.168.1.0/24' UNION SELECT cidr '192.168.1.0/24'")
        .await
        .expect("prepare inet/cidr UNION");
    assert_eq!(prepared.columns()[0].type_().oid(), OID_INET);

    // INTERSECT must return the matching address, not silently zero rows.
    let rows = client
        .simple_query("SELECT inet '10.0.0.0/8' INTERSECT SELECT cidr '10.0.0.0/8'")
        .await
        .expect("inet/cidr INTERSECT succeeds");
    assert_eq!(
        row_count(&rows),
        1,
        "matching inet/cidr address must intersect"
    );

    shutdown(client, server_handle).await;
}

/// NO-COMMON-TYPE — `int UNION date` has no implicit cast in either
/// direction, so PG raises `datatype_mismatch` (SQLSTATE 42804). It must
/// error cleanly, not return 0 rows or panic.
#[tokio::test]
async fn no_common_type_int_union_date_errors_42804() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    let result = client
        .simple_query("SELECT 1 UNION SELECT '2024-01-01'::date")
        .await;
    let err = result.expect_err("int UNION date must error");
    let code = err
        .as_db_error()
        .map(|e| e.code().code().to_owned())
        .unwrap_or_default();
    assert_eq!(
        code, "42804",
        "no-common-type set op must be datatype_mismatch"
    );

    shutdown(client, server_handle).await;
}

/// CHAINED — a 3-branch UNION mixing date/timestamp/timestamptz resolves to
/// `timestamptz` across all three branches (the supertype of the whole
/// chain), and the three distinct instants stay distinct.
#[tokio::test]
async fn chained_date_timestamp_timestamptz_resolves_timestamptz() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    let sql = "SELECT date '2024-01-01' \
               UNION SELECT timestamp '2024-06-01 12:00:00' \
               UNION SELECT timestamptz '2024-12-01 00:00:00+00'";
    let rows = client
        .simple_query(sql)
        .await
        .expect("3-branch temporal UNION succeeds");
    assert_eq!(row_count(&rows), 3, "three distinct instants stay distinct");

    let prepared = client.prepare(sql).await.expect("prepare chained UNION");
    assert_eq!(
        prepared.columns()[0].type_().oid(),
        OID_TIMESTAMPTZ,
        "chain resolves to timestamptz across all branches"
    );

    shutdown(client, server_handle).await;
}

/// EXCEPT ALL multiplicity under a cross-category cast: `date` rows minus a
/// `timestamp` midnight row subtract by instant. Two equal dates minus one
/// matching timestamp leaves exactly one date row.
#[tokio::test]
async fn except_all_multiplicity_under_temporal_cast() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    let rows = client
        .simple_query(
            "SELECT date '2024-01-01' UNION ALL SELECT date '2024-01-01' \
             EXCEPT ALL \
             SELECT timestamp '2024-01-01 00:00:00'",
        )
        .await
        .expect("temporal EXCEPT ALL succeeds");
    assert_eq!(
        row_count(&rows),
        1,
        "EXCEPT ALL subtracts one matching instant, leaving one"
    );

    shutdown(client, server_handle).await;
}
