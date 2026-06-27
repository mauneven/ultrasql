//! `INTERVAL` projection round-trip tests.
//!
//! Pins the executor fix for issue #25: a projection list with an
//! interval-typed expression used to fail with
//! "projection: unsupported output type interval" because
//! `build_column` (and the materialising `build_batch`) had no
//! `DataType::Interval` arm.
//!
//! Interval now projects as a TEXT (Utf8) column mirroring the streaming
//! row-codec column builder (`Value::Interval{..}.to_string()`), while the
//! schema field keeps `DataType::Interval` so the wire type OID stays `1186`
//! (interval).
//!
//! Driven through the real PostgreSQL wire protocol. `simple_query` is used
//! so the assertions read the exact text the server places on the wire.
//!
//! Scope note: the end-to-end "interval table column in a join/sort" and
//! "timestamp - timestamp / age value" scenarios from the bug report are
//! blocked *upstream* of the executor by separate planner/binder gaps that
//! are out of scope for this fix:
//!   - `CREATE TABLE (c INTERVAL)` is rejected ("column type not implemented").
//!   - `timestamp - timestamp` / `date - date` are rejected by the planner
//!     type checker ("arithmetic operator - on incompatible types").
//!   - the bare `interval '1 day'` text literal binds to NULL because the
//!     literal binder only parses the `INTERVAL '<n>' <unit>` value+unit form.
//! The executor-level materialisation that #26 covers is exercised by the
//! `build_batch` / `batch_to_rows` unit tests in `ultrasql-executor`.

pub mod support;

use support::{shutdown, start_sample_server};

/// Read the single-column text value of the first data row of a query.
async fn scalar_text(client: &tokio_postgres::Client, sql: &str) -> Option<String> {
    let messages = client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("simple_query {sql:?} failed: {e}"));
    messages.into_iter().find_map(|message| match message {
        tokio_postgres::SimpleQueryMessage::Row(row) => Some(row.get(0).map(str::to_owned)),
        _ => None,
    })?
}

/// `SELECT INTERVAL '1' DAY` projects the interval value as text without
/// erroring. This is the direct regression test for #25: before the fix the
/// projection of an interval-typed expression errored
/// "projection: unsupported output type interval".
#[tokio::test]
async fn select_interval_literal_projects_text() {
    let running = start_sample_server("interval_round_trip").await;
    let client = &running.client;

    let value = scalar_text(client, "SELECT INTERVAL '1' DAY").await;
    assert_eq!(
        value.as_deref(),
        Some("0mon 1d 0us"),
        "interval projects its materialised text form (mirrors the streaming path)"
    );

    // A different unit projects too (months / microseconds components).
    let months = scalar_text(client, "SELECT INTERVAL '2' MONTH").await;
    assert_eq!(months.as_deref(), Some("2mon 0d 0us"));
    let hours = scalar_text(client, "SELECT INTERVAL '3' HOUR").await;
    assert_eq!(hours.as_deref(), Some("0mon 0d 10800000000us"));

    shutdown(running).await;
}

/// The projected interval column advertises the PostgreSQL `interval` type
/// OID (1186) in the `RowDescription`, not `text` (25), even though the
/// physical batch column is a Utf8 text column.
#[tokio::test]
async fn projected_interval_reports_interval_type_oid() {
    let running = start_sample_server("interval_round_trip").await;
    let client = &running.client;

    // Prepared-statement metadata exposes the result column's type OID.
    let stmt = client
        .prepare("SELECT INTERVAL '1' DAY")
        .await
        .expect("prepare interval projection");
    assert_eq!(stmt.columns().len(), 1, "single result column");
    assert_eq!(
        stmt.columns()[0].type_().oid(),
        1186,
        "interval column advertises OID 1186, not text (25)"
    );

    shutdown(running).await;
}

/// A NULL-valued interval projects as NULL (not a text sentinel).
///
/// A `CASE` whose result branches are interval-typed yields an interval-typed
/// NULL without needing a table column or a `CAST` (both planner-gapped).
#[tokio::test]
async fn null_interval_projects_as_null() {
    let running = start_sample_server("interval_round_trip").await;
    let client = &running.client;

    let messages = client
        .simple_query("SELECT CASE WHEN false THEN INTERVAL '1' DAY ELSE NULL END")
        .await
        .expect("select null interval");
    let cell = messages.into_iter().find_map(|message| match message {
        tokio_postgres::SimpleQueryMessage::Row(row) => Some(row.get(0).map(str::to_owned)),
        _ => None,
    });
    assert_eq!(cell, Some(None), "NULL interval projects as NULL");

    shutdown(running).await;
}

/// An interval-typed expression survives an intervening `LIMIT` / ordering
/// pipeline that materialises one batch, exercising the projection path over a
/// base scan rather than a pure constant `Result` node.
#[tokio::test]
async fn interval_projection_over_scan_pipeline() {
    let running = start_sample_server("interval_round_trip").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL)")
        .await
        .expect("create t");
    client
        .batch_execute("INSERT INTO t VALUES (1), (2), (3)")
        .await
        .expect("seed t");

    // Project an interval expression for every scanned row; ORDER BY + LIMIT
    // forces a materialised pipeline above the projection.
    let messages = client
        .simple_query("SELECT INTERVAL '5' DAY FROM t ORDER BY id LIMIT 2")
        .await
        .expect("interval projection over scan");
    let cells: Vec<Option<String>> = messages
        .into_iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row.get(0).map(str::to_owned)),
            _ => None,
        })
        .collect();
    assert_eq!(
        cells,
        vec![
            Some("0mon 5d 0us".to_owned()),
            Some("0mon 5d 0us".to_owned())
        ],
        "interval projects for each scanned row through the pipeline"
    );

    shutdown(running).await;
}
