//! End-to-end checks for the first UltraSQL aggregating-index prototype.
//!
//! This is intentionally narrow: `CREATE AGGREGATING INDEX` stores
//! group-key metadata plus SUM/COUNT summaries, the query lowerer can
//! rewrite the matching dashboard aggregate shape to the summary, and
//! DML marks the summary dirty so the next read rebuilds before serving
//! rows. Restart rebuilds the runtime summary from durable heap rows using
//! catalog metadata; page-backed summary-row storage remains a later slice.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio_postgres::NoTls;
use ultrasql_core::Value;
use ultrasql_server::{RuntimeAggregatingIndex, Server, bind_listener, serve_listener};

mod support;

use support::{shutdown as graceful_shutdown, start_persistent_server};

async fn start_server_and_connect() -> (
    tokio_postgres::Client,
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
) {
    start_server_and_connect_to(Arc::new(Server::with_sample_database())).await
}

async fn start_server_and_connect_to(
    server: Arc<Server>,
) -> (
    tokio_postgres::Client,
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
) {
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("addr parses");
    let (listener, bound) = bind_listener(addr).await.expect("bind");
    let server_handle = tokio::spawn(serve_listener(listener, server));
    let conn_str = format!(
        "host={host} port={port} user=tester application_name=aggregating_index_test",
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
    let _ = server_handle.await;
}

async fn tenant_rollup_for(
    client: &tokio_postgres::Client,
    table: &str,
) -> Vec<(i32, i32, i64, i64)> {
    let sql = format!(
        "SELECT tenant_id, bucket, SUM(amount), COUNT(*) \
         FROM {table} \
         WHERE tenant_id = 7 \
         GROUP BY tenant_id, bucket \
         ORDER BY tenant_id, bucket"
    );
    client
        .query(&sql, &[])
        .await
        .expect("rollup query")
        .into_iter()
        .map(|row| (row.get(0), row.get(1), row.get(2), row.get(3)))
        .collect()
}

async fn tenant_rollup(client: &tokio_postgres::Client) -> Vec<(i32, i32, i64, i64)> {
    tenant_rollup_for(client, "fact_events").await
}

fn collect_plan_text(rows: &[tokio_postgres::Row]) -> String {
    rows.iter()
        .map(|r| r.get::<_, String>(0))
        .collect::<Vec<_>>()
        .join("\n")
}

fn catalog_option<'a>(options: &'a [(String, String)], key: &str) -> &'a str {
    options
        .iter()
        .find_map(|(name, value)| (name == key).then_some(value.as_str()))
        .unwrap_or_else(|| panic!("missing catalog option {key} in {options:?}"))
}

fn assert_aggregating_catalog_metadata(server: &Server) {
    let snapshot = server.catalog_snapshot();
    let table = snapshot
        .tables
        .get("fact_events_restart")
        .expect("table catalog entry");
    let index = snapshot
        .indexes
        .get("fact_events_restart_rollup")
        .expect("aggregating index catalog entry");

    assert_eq!(index.access_method, "aggregating");
    assert_eq!(index.table_oid, table.oid);
    assert_eq!(index.columns, vec![0, 1]);
    assert_eq!(
        catalog_option(&index.options, "aggregating.source_table_oid"),
        table.oid.raw().to_string()
    );
    assert_eq!(
        catalog_option(&index.options, "aggregating.index_oid"),
        index.oid.raw().to_string()
    );
    assert_eq!(
        catalog_option(&index.options, "aggregating.group_columns"),
        "0,1"
    );
    assert_eq!(
        catalog_option(&index.options, "aggregating.aggregates"),
        "sum:2;count:*"
    );
    assert_eq!(catalog_option(&index.options, "aggregating.stale"), "false");
    assert_eq!(catalog_option(&index.options, "aggregating.version"), "1");
    assert_eq!(
        catalog_option(&index.options, "aggregating.durable_state"),
        "rebuild_on_restart"
    );
}

fn aggregating_runtime(
    server: &Server,
    table_name: &str,
    index_name: &str,
) -> Arc<RuntimeAggregatingIndex> {
    let snapshot = server.catalog_snapshot();
    let table = snapshot
        .tables
        .get(table_name)
        .unwrap_or_else(|| panic!("table catalog entry for {table_name}"));
    let index = snapshot
        .indexes
        .get(index_name)
        .unwrap_or_else(|| panic!("index catalog entry for {index_name}"));
    let constraints = server
        .table_constraints
        .get(&table.oid)
        .unwrap_or_else(|| panic!("runtime constraints for {table_name}"));
    constraints
        .indexes
        .get(&index.oid)
        .and_then(|metadata| metadata.aggregating.clone())
        .unwrap_or_else(|| panic!("aggregating runtime for {index_name}"))
}

fn assert_runtime_clean_rows(runtime: &RuntimeAggregatingIndex, expected: &[&[&str]]) {
    assert!(
        !runtime.dirty.load(Ordering::Acquire),
        "aggregating summary should be clean after committed maintenance"
    );
    let mut actual = runtime
        .rows
        .read()
        .expect("aggregating runtime rows lock")
        .iter()
        .map(|row| row.iter().map(ToString::to_string).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    actual.sort();
    let mut expected = expected
        .iter()
        .map(|row| {
            row.iter()
                .map(|value| (*value).to_owned())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    expected.sort();
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn create_aggregating_index_rewrites_rollup_and_refreshes_after_dml() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute(
            "CREATE TABLE fact_events (
                tenant_id INT NOT NULL,
                bucket INT NOT NULL,
                amount BIGINT NOT NULL
             )",
        )
        .await
        .expect("create table");
    client
        .batch_execute(
            "INSERT INTO fact_events VALUES
                (7, 1, 10),
                (7, 1, 20),
                (7, 2, 5),
                (8, 1, 100)",
        )
        .await
        .expect("seed");
    client
        .batch_execute(
            "CREATE AGGREGATING INDEX fact_events_rollup
                ON fact_events (tenant_id, bucket, sum(amount), count(*))",
        )
        .await
        .expect("setup aggregating index");

    assert_eq!(
        tenant_rollup(&client).await,
        vec![(7, 1, 30, 2), (7, 2, 5, 1)]
    );

    let rows = client
        .query(
            "EXPLAIN ANALYZE SELECT tenant_id, bucket, SUM(amount), COUNT(*) \
             FROM fact_events \
             WHERE tenant_id = 7 \
             GROUP BY tenant_id, bucket \
             ORDER BY tenant_id, bucket",
            &[],
        )
        .await
        .expect("EXPLAIN ANALYZE");
    let text = collect_plan_text(&rows);
    assert!(
        text.contains("Aggregating Index: selected fact_events_rollup"),
        "EXPLAIN ANALYZE must report aggregating-index rewrite, got: {text}"
    );

    client
        .batch_execute("INSERT INTO fact_events VALUES (7, 1, 12)")
        .await
        .expect("insert after index build");
    assert_eq!(
        tenant_rollup(&client).await,
        vec![(7, 1, 42, 3), (7, 2, 5, 1)]
    );

    client
        .batch_execute("DELETE FROM fact_events WHERE amount = 5")
        .await
        .expect("delete after index build");
    assert_eq!(tenant_rollup(&client).await, vec![(7, 1, 42, 3)]);

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn aggregating_index_explain_analyze_reports_runtime_counters() {
    let server = Arc::new(Server::with_sample_database());
    let (client, _conn, server_handle) = start_server_and_connect_to(Arc::clone(&server)).await;

    client
        .batch_execute(
            "CREATE TABLE fact_events_explain (
                tenant_id INT NOT NULL,
                bucket INT NOT NULL,
                amount BIGINT NOT NULL
             )",
        )
        .await
        .expect("create table");
    client
        .batch_execute(
            "INSERT INTO fact_events_explain VALUES
                (7, 1, 10),
                (7, 1, 20),
                (7, 2, 5),
                (8, 1, 100)",
        )
        .await
        .expect("seed");
    client
        .batch_execute(
            "CREATE AGGREGATING INDEX fact_events_explain_rollup
                ON fact_events_explain (tenant_id, bucket, sum(amount), count(*))",
        )
        .await
        .expect("setup aggregating index");

    let runtime = aggregating_runtime(&server, "fact_events_explain", "fact_events_explain_rollup");
    runtime.mark_dirty();

    let rows = client
        .query(
            "EXPLAIN ANALYZE SELECT tenant_id, bucket, SUM(amount), COUNT(*) \
             FROM fact_events_explain \
             WHERE tenant_id = 7 \
             GROUP BY tenant_id, bucket \
             ORDER BY tenant_id, bucket",
            &[],
        )
        .await
        .expect("EXPLAIN ANALYZE");
    let text = collect_plan_text(&rows);
    for needle in [
        "Aggregating Index: selected fact_events_explain_rollup",
        "aggregating_index_used=true",
        "stale_rebuild_used=true",
        "summary_rows_read=2",
        "base_rows_skipped=3",
    ] {
        assert!(
            text.contains(needle),
            "EXPLAIN ANALYZE missing {needle}, got: {text}"
        );
    }
    assert!(
        !runtime.dirty.load(Ordering::Acquire),
        "EXPLAIN ANALYZE should rebuild stale summary before serving rows"
    );

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn aggregating_index_update_group_key_moves_summary_between_groups() {
    let server = Arc::new(Server::with_sample_database());
    let (client, _conn, server_handle) = start_server_and_connect_to(Arc::clone(&server)).await;

    client
        .batch_execute(
            "CREATE TABLE fact_events_update (
                tenant_id INT NOT NULL,
                bucket INT NOT NULL,
                amount BIGINT NOT NULL
             )",
        )
        .await
        .expect("create table");
    client
        .batch_execute(
            "INSERT INTO fact_events_update VALUES
                (7, 1, 10),
                (7, 1, 20),
                (7, 2, 5),
                (8, 1, 100)",
        )
        .await
        .expect("seed");
    client
        .batch_execute(
            "CREATE AGGREGATING INDEX fact_events_update_rollup
                ON fact_events_update (tenant_id, bucket, sum(amount), count(*))",
        )
        .await
        .expect("setup aggregating index");
    let runtime = aggregating_runtime(&server, "fact_events_update", "fact_events_update_rollup");

    client
        .batch_execute(
            "UPDATE fact_events_update
             SET bucket = 2, amount = 13
             WHERE tenant_id = 7 AND bucket = 1 AND amount = 10",
        )
        .await
        .expect("update group key");

    assert_runtime_clean_rows(
        &runtime,
        &[
            &["7", "1", "20", "1"],
            &["7", "2", "18", "2"],
            &["8", "1", "100", "1"],
        ],
    );
    assert_eq!(
        tenant_rollup_for(&client, "fact_events_update").await,
        vec![(7, 1, 20, 1), (7, 2, 18, 2)]
    );

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn aggregating_index_delete_decrements_summary_and_omits_empty_group() {
    let server = Arc::new(Server::with_sample_database());
    let (client, _conn, server_handle) = start_server_and_connect_to(Arc::clone(&server)).await;

    client
        .batch_execute(
            "CREATE TABLE fact_events_delete (
                tenant_id INT NOT NULL,
                bucket INT NOT NULL,
                amount BIGINT NOT NULL
             )",
        )
        .await
        .expect("create table");
    client
        .batch_execute(
            "INSERT INTO fact_events_delete VALUES
                (7, 1, 10),
                (7, 1, 20),
                (7, 2, 5),
                (8, 1, 100)",
        )
        .await
        .expect("seed");
    client
        .batch_execute(
            "CREATE AGGREGATING INDEX fact_events_delete_rollup
                ON fact_events_delete (tenant_id, bucket, sum(amount), count(*))",
        )
        .await
        .expect("setup aggregating index");
    let runtime = aggregating_runtime(&server, "fact_events_delete", "fact_events_delete_rollup");

    client
        .batch_execute("DELETE FROM fact_events_delete WHERE tenant_id = 7 AND bucket = 2")
        .await
        .expect("delete group");

    assert_runtime_clean_rows(&runtime, &[&["7", "1", "30", "2"], &["8", "1", "100", "1"]]);
    assert_eq!(
        tenant_rollup_for(&client, "fact_events_delete").await,
        vec![(7, 1, 30, 2)]
    );

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn aggregating_index_vacuum_rebuilds_stale_summary_without_corruption() {
    let server = Arc::new(Server::with_sample_database());
    let (client, _conn, server_handle) = start_server_and_connect_to(Arc::clone(&server)).await;

    client
        .batch_execute(
            "CREATE TABLE fact_events_vacuum (
                tenant_id INT NOT NULL,
                bucket INT NOT NULL,
                amount BIGINT NOT NULL
             )",
        )
        .await
        .expect("create table");
    client
        .batch_execute(
            "INSERT INTO fact_events_vacuum VALUES
                (7, 1, 10),
                (7, 1, 20),
                (7, 2, 5),
                (8, 1, 100)",
        )
        .await
        .expect("seed");
    client
        .batch_execute(
            "CREATE AGGREGATING INDEX fact_events_vacuum_rollup
                ON fact_events_vacuum (tenant_id, bucket, sum(amount), count(*))",
        )
        .await
        .expect("setup aggregating index");
    let runtime = aggregating_runtime(&server, "fact_events_vacuum", "fact_events_vacuum_rollup");

    client
        .batch_execute("DELETE FROM fact_events_vacuum WHERE tenant_id = 7 AND bucket = 2")
        .await
        .expect("delete before vacuum");
    {
        let mut rows = runtime.rows.write().expect("aggregating rows lock");
        *rows = vec![vec![
            Value::Int32(7),
            Value::Int32(99),
            Value::Int64(999),
            Value::Int64(999),
        ]];
    }
    runtime.mark_dirty();

    client
        .batch_execute("VACUUM fact_events_vacuum")
        .await
        .expect("vacuum");

    assert_runtime_clean_rows(&runtime, &[&["7", "1", "30", "2"], &["8", "1", "100", "1"]]);
    assert_eq!(
        tenant_rollup_for(&client, "fact_events_vacuum").await,
        vec![(7, 1, 30, 2)]
    );

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn aggregating_index_restarts_with_catalog_metadata_and_rebuilt_summary() {
    let dir = tempfile::tempdir().expect("tempdir");

    {
        let running = start_persistent_server(dir.path(), "aggregating_index_test").await;
        let server = Arc::clone(&running.server);
        let client = &running.client;
        client
            .batch_execute(
                "CREATE TABLE fact_events_restart (
                    tenant_id INT NOT NULL,
                    bucket INT NOT NULL,
                    amount BIGINT NOT NULL
                 )",
            )
            .await
            .expect("create table");
        client
            .batch_execute(
                "INSERT INTO fact_events_restart VALUES
                    (7, 1, 10),
                    (7, 1, 20),
                    (7, 2, 5),
                    (8, 1, 100)",
            )
            .await
            .expect("seed");
        client
            .batch_execute(
                "CREATE AGGREGATING INDEX fact_events_restart_rollup
                    ON fact_events_restart (tenant_id, bucket, sum(amount), count(*))",
            )
            .await
            .expect("setup aggregating index");

        assert_aggregating_catalog_metadata(&server);

        client
            .batch_execute("INSERT INTO fact_events_restart VALUES (7, 1, 12)")
            .await
            .expect("insert after index build");
        client
            .batch_execute("DELETE FROM fact_events_restart WHERE amount = 5")
            .await
            .expect("delete after index build");

        drop(server);
        graceful_shutdown(running).await;
    }

    {
        let running = start_persistent_server(dir.path(), "aggregating_index_test").await;
        let server = Arc::clone(&running.server);
        let client = &running.client;
        assert_aggregating_catalog_metadata(&server);
        assert_eq!(
            tenant_rollup_for(client, "fact_events_restart").await,
            vec![(7, 1, 42, 3)],
            "restart must rebuild aggregating-index summary from durable heap"
        );

        let rows = client
            .query(
                "EXPLAIN ANALYZE SELECT tenant_id, bucket, SUM(amount), COUNT(*) \
                 FROM fact_events_restart \
                 WHERE tenant_id = 7 \
                 GROUP BY tenant_id, bucket \
                 ORDER BY tenant_id, bucket",
                &[],
            )
            .await
            .expect("EXPLAIN ANALYZE after restart");
        let text = collect_plan_text(&rows);
        assert!(
            text.contains("Aggregating Index: selected fact_events_restart_rollup"),
            "restart must rebuild runtime aggregating-index rewrite state, got: {text}"
        );

        drop(server);
        graceful_shutdown(running).await;
    }
}
