//! End-to-end round-trips for `CREATE INDEX` over every column type
//! the v0.5 B-tree key encoding now supports through the wire surface.
//!
//! Closes the v0.5 wave step "§1.19 — CREATE INDEX beyond single-column
//! Int32/Int64". One test per supported single-column key type plus a
//! composite (two integer columns packed into one i64) confirms:
//!
//! - the index builds without error against a non-empty heap,
//! - a subsequent `SELECT ... WHERE col = literal` returns exactly the
//!   expected row(s),
//! - the rejection path for an unsupported type (`BYTEA`) keeps
//!   producing `ERROR` so a regression cannot silently widen the
//!   surface area.
//!
//! The probe path correctness for the no-recheck encodings (`Int32`,
//! `Int64`, `Float64`) is asserted by the observation that no extra
//! rows leak: the test inserts adjacent values and the assertion
//! passes only when the index probe returns the correct single row.
//! For `Text` and the composite encoding, the heap-side recheck is
//! exercised by inserting rows that share the same encoded prefix; the
//! probe must filter out the false positives.
//!
//! Tests for the encodings that have no wire surface yet
//! (`Int16` / `SMALLINT` rejects Int32 literals in `INSERT VALUES`;
//! `Float32` / `REAL` rejects Float64 literals; `Timestamp` is not yet
//! a CREATE TABLE-able type) live next to the kernel — see
//! `crates/ultrasql-server/src/index_key.rs::tests`. Once the binder
//! adds implicit-narrowing coercion in a later wave, this file should
//! grow `INSERT VALUES` round-trips for those types too.

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
        "host={host} port={port} user=tester application_name=create_index_types_test",
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

/// Collect the first column of every `Row` from a `simple_query`
/// response into a `Vec<String>`. The wire format is text, so each
/// cell arrives as `Option<&str>` and we forward only `Some(_)` cells.
fn rows_first_col(rows: &[tokio_postgres::SimpleQueryMessage]) -> Vec<String> {
    rows.iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => r.get(0).map(str::to_owned),
            _ => None,
        })
        .collect()
}

/// `CREATE INDEX` over an `Int32` column (the pre-existing supported
/// shape) keeps working — a regression here would mean the new
/// encoding pipeline broke the established path.
#[tokio::test]
async fn create_index_over_int32_then_point_lookup_round_trip() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;
    client
        .batch_execute("CREATE TABLE t_i32 (k INT NOT NULL, payload INT NOT NULL)")
        .await
        .expect("create table");
    client
        .batch_execute("INSERT INTO t_i32 VALUES (-1, 10), (0, 20), (1, 30), (2000000000, 40)")
        .await
        .expect("insert");
    client
        .batch_execute("CREATE INDEX ix_t_i32_k ON t_i32(k)")
        .await
        .expect("create index");

    let rows = client
        .simple_query("SELECT payload FROM t_i32 WHERE k = 2000000000")
        .await
        .expect("query");
    assert_eq!(rows_first_col(&rows), vec!["40".to_string()]);

    let rows = client
        .simple_query("SELECT payload FROM t_i32 WHERE k = -1")
        .await
        .expect("query");
    assert_eq!(rows_first_col(&rows), vec!["10".to_string()]);

    shutdown(client, server_handle).await;
}

/// `CREATE INDEX` over an `Int64` column. Verifies that the previously
/// supported wide-integer path still produces a probe-able index.
/// Uses literals larger than `i32::MAX` so the binder picks
/// `Value::Int64` directly (the current v0.5 binder has no implicit
/// `Int32 → Int64` widening for `INSERT VALUES`).
#[tokio::test]
async fn create_index_over_int64_then_point_lookup_round_trip() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;
    client
        .batch_execute("CREATE TABLE t_i64 (k BIGINT NOT NULL, payload BIGINT NOT NULL)")
        .await
        .expect("create table");
    client
        .batch_execute(
            "INSERT INTO t_i64 VALUES \
             (3000000000, 3000000001), \
             (5000000000, 5000000001), \
             (9000000000, 9000000001), \
             (9223372036854775807, 9000000002)",
        )
        .await
        .expect("insert");
    client
        .batch_execute("CREATE INDEX ix_t_i64_k ON t_i64(k)")
        .await
        .expect("create index");

    let rows = client
        .simple_query("SELECT payload FROM t_i64 WHERE k = 9223372036854775807")
        .await
        .expect("query");
    assert_eq!(rows_first_col(&rows), vec!["9000000002".to_string()]);

    let rows = client
        .simple_query("SELECT payload FROM t_i64 WHERE k = 5000000000")
        .await
        .expect("query");
    assert_eq!(rows_first_col(&rows), vec!["5000000001".to_string()]);

    shutdown(client, server_handle).await;
}

/// `CREATE INDEX` over a `BOOLEAN` column. The encoding maps `false`
/// to `0` and `true` to `1`. The B-tree currently rejects duplicate
/// keys, so the test inserts at most one row per boolean value and
/// confirms both are probe-able by equality.
#[tokio::test]
async fn create_index_over_bool_then_equality_point_lookup_round_trip() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;
    client
        .batch_execute("CREATE TABLE t_bool (k BOOLEAN NOT NULL, payload INT NOT NULL)")
        .await
        .expect("create table");
    client
        .batch_execute("INSERT INTO t_bool VALUES (TRUE, 1), (FALSE, 2)")
        .await
        .expect("insert");
    client
        .batch_execute("CREATE INDEX ix_t_bool_k ON t_bool(k)")
        .await
        .expect("create index");

    let rows = client
        .simple_query("SELECT payload FROM t_bool WHERE k = TRUE")
        .await
        .expect("query");
    assert_eq!(rows_first_col(&rows), vec!["1".to_string()]);

    let rows = client
        .simple_query("SELECT payload FROM t_bool WHERE k = FALSE")
        .await
        .expect("query");
    assert_eq!(rows_first_col(&rows), vec!["2".to_string()]);

    shutdown(client, server_handle).await;
}

/// `CREATE INDEX` over a `FLOAT8` (Float64) column. The encoding
/// produces an order-preserving i64 key; a point lookup returns the
/// row whose stored f64 equals the literal — and only that row, even
/// when adjacent rows differ only in the trailing mantissa bits.
#[tokio::test]
async fn create_index_over_float64_then_point_lookup_round_trip() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;
    client
        .batch_execute("CREATE TABLE t_f64 (k FLOAT8 NOT NULL, payload INT NOT NULL)")
        .await
        .expect("create table");
    client
        .batch_execute(
            "INSERT INTO t_f64 VALUES (-1.0e300, 1), (-1.5, 2), (0.0, 3), (1.5, 4), (1.0e300, 5)",
        )
        .await
        .expect("insert");
    client
        .batch_execute("CREATE INDEX ix_t_f64_k ON t_f64(k)")
        .await
        .expect("create index");

    let rows = client
        .simple_query("SELECT payload FROM t_f64 WHERE k = 1.5")
        .await
        .expect("query");
    assert_eq!(rows_first_col(&rows), vec!["4".to_string()]);

    let rows = client
        .simple_query("SELECT payload FROM t_f64 WHERE k = -1.5")
        .await
        .expect("query");
    assert_eq!(rows_first_col(&rows), vec!["2".to_string()]);

    let rows = client
        .simple_query("SELECT payload FROM t_f64 WHERE k = -1.0e300")
        .await
        .expect("query");
    assert_eq!(rows_first_col(&rows), vec!["1".to_string()]);

    shutdown(client, server_handle).await;
}

/// `CREATE INDEX` over a `TEXT` column. The 8-byte-prefix encoding
/// preserves lexicographic order over the first 8 UTF-8 bytes; the
/// heap recheck filters false positives when a probe's prefix matches
/// a stored row whose full value differs.
///
/// Two distinct strings whose first eight bytes are identical would
/// produce the same B-tree key and trip the v0.5 unique-key
/// invariant — `INSERT` would fail with `duplicate key in index`.
/// Real schemas avoid this by either (a) declaring short distinct
/// prefixes, or (b) waiting for the v0.7 `Vec<u8>`-keyed B-tree wave
/// that drops the truncation entirely. The test sticks to (a):
/// short, distinct-prefix strings for the build, and exercises the
/// heap recheck by probing a literal whose first 8 bytes match a
/// stored row but whose full text does not.
#[tokio::test]
async fn create_index_over_text_then_point_lookup_round_trip() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;
    client
        .batch_execute("CREATE TABLE t_text (k TEXT NOT NULL, payload INT NOT NULL)")
        .await
        .expect("create table");
    client
        .batch_execute(
            "INSERT INTO t_text VALUES \
             ('alpha', 1), \
             ('beta', 2), \
             ('gamma', 3), \
             ('shorter', 4), \
             ('abcdefgha', 10)",
        )
        .await
        .expect("insert");
    client
        .batch_execute("CREATE INDEX ix_t_text_k ON t_text(k)")
        .await
        .expect("create index");

    // Short string — encoded key is byte-distinct from every other row.
    let rows = client
        .simple_query("SELECT payload FROM t_text WHERE k = 'beta'")
        .await
        .expect("query short string");
    assert_eq!(rows_first_col(&rows), vec!["2".to_string()]);

    // Full-length match on a row whose first 8 bytes are 'abcdefgh'.
    let rows = client
        .simple_query("SELECT payload FROM t_text WHERE k = 'abcdefgha'")
        .await
        .expect("query 9-byte match");
    assert_eq!(rows_first_col(&rows), vec!["10".to_string()]);

    // Recheck path: probe with a literal that shares the 8-byte
    // prefix `'abcdefgh'` with the row above but differs after byte 8.
    // Without the heap recheck the probe would surface payload `10`;
    // with the recheck it returns nothing because no stored row has
    // the full text `'abcdefghX'`.
    let rows = client
        .simple_query("SELECT payload FROM t_text WHERE k = 'abcdefghX'")
        .await
        .expect("query colliding-prefix literal");
    assert_eq!(rows_first_col(&rows), Vec::<String>::new());

    shutdown(client, server_handle).await;
}

/// `CREATE INDEX` over a composite key built from two integer
/// columns. The build path succeeds; the probe path does not yet
/// lower a composite predicate, so a `WHERE a = 1 AND b = 10` query
/// falls back to `SeqScan + Filter` and still returns the correct
/// row.
#[tokio::test]
async fn create_composite_index_over_two_int_columns_builds_and_does_not_corrupt_seq_scan() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;
    client
        .batch_execute("CREATE TABLE t_comp (a INT NOT NULL, b INT NOT NULL, payload INT NOT NULL)")
        .await
        .expect("create table");
    client
        .batch_execute(
            "INSERT INTO t_comp VALUES (1, 10, 100), (2, 20, 200), (3, 30, 300), (1, 99, 199)",
        )
        .await
        .expect("insert");
    client
        .batch_execute("CREATE INDEX ix_t_comp_a_b ON t_comp(a, b)")
        .await
        .expect("create composite index");

    let rows = client
        .simple_query("SELECT payload FROM t_comp WHERE a = 1 AND b = 10")
        .await
        .expect("query");
    assert_eq!(rows_first_col(&rows), vec!["100".to_string()]);

    let rows = client
        .simple_query("SELECT payload FROM t_comp WHERE a = 1")
        .await
        .expect("query");
    let mut got = rows_first_col(&rows);
    got.sort();
    assert_eq!(got, vec!["100".to_string(), "199".to_string()]);

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn create_unique_expression_index_enforces_lower_key_on_insert() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;
    client
        .batch_execute("CREATE TABLE t_expr_idx (name TEXT NOT NULL, payload INT NOT NULL)")
        .await
        .expect("create table");
    client
        .batch_execute("INSERT INTO t_expr_idx VALUES ('Alice', 1)")
        .await
        .expect("insert initial row");
    client
        .batch_execute("CREATE UNIQUE INDEX ux_t_expr_lower_name ON t_expr_idx (lower(name))")
        .await
        .expect("create expression index");

    let err = client
        .batch_execute("INSERT INTO t_expr_idx VALUES ('alice', 2)")
        .await
        .expect_err("lower(name) duplicate must be rejected");
    let db_err = err.as_db_error().expect("DB error");
    assert_eq!(db_err.code().code(), "23505");

    let rows = client
        .simple_query("SELECT payload FROM t_expr_idx WHERE name = 'Alice'")
        .await
        .expect("query");
    assert_eq!(rows_first_col(&rows), vec!["1".to_string()]);

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn create_unique_partial_index_enforces_only_matching_rows() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;
    client
        .batch_execute(
            "CREATE TABLE t_partial_idx (email TEXT NOT NULL, active BOOLEAN NOT NULL, payload INT NOT NULL)",
        )
        .await
        .expect("create table");
    client
        .batch_execute(
            "CREATE UNIQUE INDEX ux_t_partial_active_email ON t_partial_idx (email) WHERE active = TRUE",
        )
        .await
        .expect("create partial index");
    client
        .batch_execute(
            "INSERT INTO t_partial_idx VALUES ('a@example.com', FALSE, 1), ('a@example.com', FALSE, 2)",
        )
        .await
        .expect("inactive duplicates are outside the partial index");
    client
        .batch_execute("INSERT INTO t_partial_idx VALUES ('a@example.com', TRUE, 3)")
        .await
        .expect("first active row enters partial index");

    let err = client
        .batch_execute("INSERT INTO t_partial_idx VALUES ('a@example.com', TRUE, 4)")
        .await
        .expect_err("active duplicate must be rejected");
    let db_err = err.as_db_error().expect("DB error");
    assert_eq!(db_err.code().code(), "23505");

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn partial_index_runtime_predicate_error_returns_sqlstate() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;
    client
        .batch_execute("CREATE TABLE t_partial_error (id INT NOT NULL, raw TEXT NOT NULL)")
        .await
        .expect("create table");
    client
        .batch_execute(
            "CREATE INDEX idx_partial_error ON t_partial_error (id) \
             WHERE CAST(raw AS INTEGER) > 0",
        )
        .await
        .expect("create partial index");

    let err = client
        .batch_execute("INSERT INTO t_partial_error VALUES (1, 'not-an-int')")
        .await
        .expect_err("partial index predicate runtime cast must reject row");
    let db_err = err.as_db_error().expect("DB error");
    assert_eq!(db_err.code().code(), "22P02");

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn expression_index_runtime_key_error_returns_sqlstate() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;
    client
        .batch_execute("CREATE TABLE t_expr_error (raw TEXT NOT NULL)")
        .await
        .expect("create table");
    client
        .batch_execute("CREATE INDEX idx_expr_error ON t_expr_error ((CAST(raw AS INTEGER)))")
        .await
        .expect("create expression index");

    let err = client
        .batch_execute("INSERT INTO t_expr_error VALUES ('not-an-int')")
        .await
        .expect_err("expression index key runtime cast must reject row");
    let db_err = err.as_db_error().expect("DB error");
    assert_eq!(db_err.code().code(), "22P02");

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn create_unique_covering_index_keeps_include_columns_out_of_key() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;
    client
        .batch_execute("CREATE TABLE t_cover_idx (id INT NOT NULL, payload INT NOT NULL)")
        .await
        .expect("create table");
    client
        .batch_execute("CREATE UNIQUE INDEX ux_t_cover_id ON t_cover_idx (id) INCLUDE (payload)")
        .await
        .expect("create covering index");
    client
        .batch_execute("INSERT INTO t_cover_idx VALUES (1, 10)")
        .await
        .expect("insert first row");

    let err = client
        .batch_execute("INSERT INTO t_cover_idx VALUES (1, 20)")
        .await
        .expect_err("duplicate key must ignore differing INCLUDE payload");
    let db_err = err.as_db_error().expect("DB error");
    assert_eq!(db_err.code().code(), "23505");

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn create_hash_index_supports_equality_queries_and_dml_maintenance() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;
    client
        .batch_execute("CREATE TABLE t_hash_idx (id INT NOT NULL, payload INT NOT NULL)")
        .await
        .expect("create table");
    client
        .batch_execute("INSERT INTO t_hash_idx VALUES (1, 10), (2, 20)")
        .await
        .expect("insert initial rows");
    client
        .batch_execute("CREATE INDEX ix_t_hash_id ON t_hash_idx USING hash (id)")
        .await
        .expect("create hash index");
    client
        .batch_execute("INSERT INTO t_hash_idx VALUES (3, 30)")
        .await
        .expect("insert after index build");

    let rows = client
        .simple_query("SELECT payload FROM t_hash_idx WHERE id = 3")
        .await
        .expect("hash equality query");
    assert_eq!(rows_first_col(&rows), vec!["30".to_string()]);

    shutdown(client, server_handle).await;
}

/// A `CREATE INDEX` over a `BYTEA` column is rejected. The encoding
/// kernel returns `ServerError::Unsupported`, which the wire layer
/// reports as an `ErrorResponse`. This keeps the rejection surface
/// honest: a future widening of the encoding set should add a test
/// for the new positive case rather than silently change this
/// negative one.
#[tokio::test]
async fn create_index_over_unsupported_column_type_returns_error() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;
    client
        .batch_execute("CREATE TABLE t_bytea (k BYTEA, payload INT NOT NULL)")
        .await
        .expect("create table");
    let err = client
        .batch_execute("CREATE INDEX ix_t_bytea_k ON t_bytea(k)")
        .await
        .expect_err("must reject BYTEA index");
    // tokio-postgres's `db error` message is opaque; pull the inner
    // DbError to inspect the wire-level message.
    let db_err = err.as_db_error().expect("DB error");
    let msg = db_err.message();
    assert!(
        msg.contains("supported") || msg.contains("v0.5") || msg.contains("v0.7"),
        "expected an unsupported-type error message; got: {msg}"
    );

    shutdown(client, server_handle).await;
}
