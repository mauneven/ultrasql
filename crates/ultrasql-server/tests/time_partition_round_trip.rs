//! End-to-end time-series range partitioning tests.

pub mod support;

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use support::{shutdown as shutdown_persistent, start_persistent_server};
use tokio_postgres::NoTls;
use ultrasql_server::{Server, bind_listener, serve_listener};

async fn start_server_and_connect() -> (
    Arc<Server>,
    tokio_postgres::Client,
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
) {
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("addr parses");
    let (listener, bound) = bind_listener(addr).await.expect("bind");
    let server = Arc::new(Server::with_sample_database());
    let server_handle = tokio::spawn(serve_listener(listener, Arc::clone(&server)));
    let conn_str = format!(
        "host={host} port={port} user=tester application_name=time_partition_test",
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
    (server, client, conn_handle, server_handle)
}

async fn shutdown(
    client: tokio_postgres::Client,
    server_handle: tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
) {
    drop(client);
    tokio::time::sleep(Duration::from_millis(20)).await;
    server_handle.abort();
}

async fn public_table_count(client: &tokio_postgres::Client, table_name: &str) -> i64 {
    client
        .query_one(
            "SELECT COUNT(*) FROM information_schema.tables \
             WHERE table_schema = 'public' AND table_name = $1",
            &[&table_name],
        )
        .await
        .expect("count information_schema table rows")
        .get::<_, i64>(0)
}

#[tokio::test]
async fn range_partitioned_timestamp_table_auto_creates_and_prunes_chunks() {
    let (server, client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute(
            "CREATE TABLE metrics (\
             ts TIMESTAMP NOT NULL, host TEXT NOT NULL, value INT NOT NULL\
             ) PARTITION BY RANGE (ts)",
        )
        .await
        .expect("create partitioned metrics table");

    client
        .batch_execute(
            "INSERT INTO metrics VALUES \
             (TIMESTAMP '2026-05-20 00:00:00', 'a', 10),\
             (TIMESTAMP '2026-05-20 12:00:00', 'b', 20),\
             (TIMESTAMP '2026-05-21 00:00:00', 'c', 30)",
        )
        .await
        .expect("insert partitioned metrics");

    let runtime = server
        .time_partitions
        .get("metrics")
        .expect("partition runtime registered")
        .clone();
    assert_eq!(runtime.chunks.len(), 2, "two daily chunks should exist");

    let all = client
        .query("SELECT host, value FROM metrics ORDER BY value", &[])
        .await
        .expect("scan partitioned parent");
    assert_eq!(all.len(), 3);
    assert_eq!(all[0].get::<_, String>(0), "a");
    assert_eq!(all[2].get::<_, i32>(1), 30);

    let pruned = client
        .query(
            "SELECT host, value FROM metrics \
             WHERE ts >= TIMESTAMP '2026-05-21 00:00:00' \
             ORDER BY value",
            &[],
        )
        .await
        .expect("pruned partitioned scan");
    assert_eq!(pruned.len(), 1);
    assert_eq!(pruned[0].get::<_, String>(0), "c");
    assert_eq!(pruned[0].get::<_, i32>(1), 30);
    assert_eq!(
        runtime.last_scan_total_chunks.load(Ordering::Acquire),
        2,
        "pruning considered both chunks"
    );
    assert_eq!(
        runtime.last_scan_selected_chunks.load(Ordering::Acquire),
        1,
        "timestamp predicate should prune to one chunk"
    );

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn renamed_partitioned_table_keeps_chunk_routing() {
    let (server, client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute(
            "CREATE TABLE metrics_rename (\
             ts TIMESTAMP NOT NULL, host TEXT NOT NULL, value INT NOT NULL\
             ) PARTITION BY RANGE (ts);\
             INSERT INTO metrics_rename VALUES \
             (TIMESTAMP '2026-05-20 00:00:00', 'a', 10),\
             (TIMESTAMP '2026-05-21 00:00:00', 'b', 20);\
             ALTER TABLE metrics_rename RENAME TO metrics_renamed",
        )
        .await
        .expect("create, seed, and rename partitioned table");

    assert!(
        server.time_partitions.get("metrics_renamed").is_some(),
        "partition runtime must move to renamed parent"
    );
    assert!(
        server.time_partitions.get("metrics_rename").is_none(),
        "old parent name must not keep partition runtime"
    );

    let rows = client
        .query(
            "SELECT host, value FROM metrics_renamed ORDER BY value",
            &[],
        )
        .await
        .expect("renamed partition parent scans existing chunks");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, String>(0), "a");
    assert_eq!(rows[1].get::<_, i32>(1), 20);

    client
        .batch_execute(
            "INSERT INTO metrics_renamed VALUES \
             (TIMESTAMP '2026-05-22 00:00:00', 'c', 30)",
        )
        .await
        .expect("insert after rename routes through partition runtime");
    let rows = client
        .query(
            "SELECT host, value FROM metrics_renamed ORDER BY value",
            &[],
        )
        .await
        .expect("renamed partition parent scans new chunk");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[2].get::<_, String>(0), "c");
    assert_eq!(rows[2].get::<_, i32>(1), 30);

    shutdown(client, server_handle).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_partitioned_table_removes_chunks_after_restart() {
    let data_dir = tempfile::TempDir::new().expect("temp data dir");

    let running = start_persistent_server(data_dir.path(), "time_partition_drop_setup").await;
    running
        .client
        .batch_execute(
            "CREATE TABLE metrics_drop_parent (\
             ts TIMESTAMP NOT NULL, host TEXT NOT NULL, value INT NOT NULL\
             ) PARTITION BY RANGE (ts);\
             INSERT INTO metrics_drop_parent VALUES \
             (TIMESTAMP '2026-05-20 00:00:00', 'a', 10),\
             (TIMESTAMP '2026-05-21 00:00:00', 'b', 20)",
        )
        .await
        .expect("create and seed partitioned table before drop");
    let chunk_names = running
        .server
        .time_partitions
        .get("metrics_drop_parent")
        .expect("partition runtime before drop")
        .chunks
        .iter()
        .map(|chunk| chunk.value().table_name.clone())
        .collect::<Vec<_>>();
    assert_eq!(chunk_names.len(), 2);

    running
        .client
        .batch_execute("DROP TABLE metrics_drop_parent")
        .await
        .expect("drop partitioned table");
    assert!(
        running
            .server
            .time_partitions
            .get("metrics_drop_parent")
            .is_none(),
        "partition runtime must be removed immediately after drop"
    );
    assert_eq!(
        public_table_count(&running.client, "metrics_drop_parent").await,
        0
    );
    for chunk_name in &chunk_names {
        assert_eq!(public_table_count(&running.client, chunk_name).await, 0);
    }
    shutdown_persistent(running).await;

    let running = start_persistent_server(data_dir.path(), "time_partition_drop_verify").await;
    assert!(
        running
            .server
            .time_partitions
            .get("metrics_drop_parent")
            .is_none(),
        "partition runtime must stay removed after restart"
    );
    assert_eq!(
        public_table_count(&running.client, "metrics_drop_parent").await,
        0
    );
    for chunk_name in &chunk_names {
        assert_eq!(public_table_count(&running.client, chunk_name).await, 0);
    }

    shutdown_persistent(running).await;
}

#[tokio::test]
async fn partitioned_parent_count_reads_chunks() {
    let (_server, client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute(
            "CREATE TABLE metrics_count (\
             ts TIMESTAMP NOT NULL, host TEXT NOT NULL, value INT NOT NULL\
             ) PARTITION BY RANGE (ts);\
             INSERT INTO metrics_count VALUES \
             (TIMESTAMP '2026-05-20 00:00:00', 'a', 10),\
             (TIMESTAMP '2026-05-21 00:00:00', 'b', 20)",
        )
        .await
        .expect("create and seed partitioned count table");

    let count = client
        .query_one("SELECT COUNT(*) FROM metrics_count", &[])
        .await
        .expect("count partitioned parent")
        .get::<_, i64>(0);
    assert_eq!(count, 2);

    let sum = client
        .query_one("SELECT SUM(value) FROM metrics_count", &[])
        .await
        .expect("sum partitioned parent")
        .get::<_, i64>(0);
    assert_eq!(sum, 30);

    let filtered_sum = client
        .query_one(
            "SELECT SUM(value) FROM metrics_count WHERE value >= 20",
            &[],
        )
        .await
        .expect("filtered sum partitioned parent")
        .get::<_, i64>(0);
    assert_eq!(filtered_sum, 20);

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn partitioned_table_truncate_clears_chunks() {
    let (_server, client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute(
            "CREATE TABLE metrics_truncate (\
             ts TIMESTAMP NOT NULL, host TEXT NOT NULL, value INT NOT NULL\
             ) PARTITION BY RANGE (ts);\
             INSERT INTO metrics_truncate VALUES \
             (TIMESTAMP '2026-05-20 00:00:00', 'a', 10),\
             (TIMESTAMP '2026-05-21 00:00:00', 'b', 20);\
             TRUNCATE TABLE metrics_truncate",
        )
        .await
        .expect("truncate partitioned table");

    let count = client
        .query_one("SELECT COUNT(*) FROM metrics_truncate", &[])
        .await
        .expect("count truncated partitioned table")
        .get::<_, i64>(0);
    assert_eq!(count, 0);

    client
        .batch_execute(
            "INSERT INTO metrics_truncate VALUES \
             (TIMESTAMP '2026-05-22 00:00:00', 'c', 30)",
        )
        .await
        .expect("insert after partitioned truncate");
    let count = client
        .query_one("SELECT COUNT(*) FROM metrics_truncate", &[])
        .await
        .expect("count after partitioned truncate insert")
        .get::<_, i64>(0);
    assert_eq!(count, 1);

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn partitioned_table_add_column_refreshes_runtime_schema() {
    let data_dir = tempfile::TempDir::new().expect("temp data dir");

    let running = start_persistent_server(data_dir.path(), "time_partition_add_column_setup").await;
    running
        .client
        .batch_execute(
            "CREATE TABLE metrics_add_column (\
             ts TIMESTAMP NOT NULL, host TEXT NOT NULL, value INT NOT NULL\
             ) PARTITION BY RANGE (ts);\
             INSERT INTO metrics_add_column VALUES \
             (TIMESTAMP '2026-05-20 00:00:00', 'a', 10);\
             ALTER TABLE metrics_add_column ADD COLUMN note TEXT;\
             INSERT INTO metrics_add_column VALUES \
             (TIMESTAMP '2026-05-21 00:00:00', 'b', 20, 'after')",
        )
        .await
        .expect("alter partitioned table and insert widened row");

    let rows = running
        .client
        .query(
            "SELECT host, value, note FROM metrics_add_column ORDER BY value",
            &[],
        )
        .await
        .expect("scan widened partitioned parent");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, String>(0), "a");
    assert!(rows[0].get::<_, Option<String>>(2).is_none());
    assert_eq!(rows[1].get::<_, String>(0), "b");
    assert_eq!(rows[1].get::<_, i32>(1), 20);
    assert_eq!(
        rows[1].get::<_, Option<String>>(2).as_deref(),
        Some("after")
    );
    shutdown_persistent(running).await;

    let running =
        start_persistent_server(data_dir.path(), "time_partition_add_column_verify").await;
    let rows = running
        .client
        .query(
            "SELECT host, value, note FROM metrics_add_column ORDER BY value",
            &[],
        )
        .await
        .expect("scan widened partitioned parent after restart");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, String>(0), "a");
    assert!(rows[0].get::<_, Option<String>>(2).is_none());
    assert_eq!(rows[1].get::<_, String>(0), "b");
    assert_eq!(rows[1].get::<_, i32>(1), 20);
    assert_eq!(
        rows[1].get::<_, Option<String>>(2).as_deref(),
        Some("after")
    );
    shutdown_persistent(running).await;
}

#[tokio::test]
async fn partitioned_table_drop_column_refreshes_chunk_schema() {
    let data_dir = tempfile::TempDir::new().expect("temp data dir");

    let running =
        start_persistent_server(data_dir.path(), "time_partition_drop_column_setup").await;
    running
        .client
        .batch_execute(
            "CREATE TABLE metrics_drop_column (\
             ts TIMESTAMP NOT NULL, host TEXT NOT NULL, value INT NOT NULL, note TEXT\
             ) PARTITION BY RANGE (ts);\
             INSERT INTO metrics_drop_column VALUES \
             (TIMESTAMP '2026-05-20 00:00:00', 'a', 10, 'before');\
             ALTER TABLE metrics_drop_column DROP COLUMN note;\
             INSERT INTO metrics_drop_column VALUES \
             (TIMESTAMP '2026-05-21 00:00:00', 'b', 20)",
        )
        .await
        .expect("drop partitioned table column and insert narrowed row");

    let rows = running
        .client
        .query(
            "SELECT host, value FROM metrics_drop_column ORDER BY value",
            &[],
        )
        .await
        .expect("scan narrowed partitioned parent");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, String>(0), "a");
    assert_eq!(rows[0].get::<_, i32>(1), 10);
    assert_eq!(rows[1].get::<_, String>(0), "b");
    assert_eq!(rows[1].get::<_, i32>(1), 20);
    shutdown_persistent(running).await;

    let running =
        start_persistent_server(data_dir.path(), "time_partition_drop_column_verify").await;
    let rows = running
        .client
        .query(
            "SELECT host, value FROM metrics_drop_column ORDER BY value",
            &[],
        )
        .await
        .expect("scan narrowed partitioned parent after restart");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, String>(0), "a");
    assert_eq!(rows[0].get::<_, i32>(1), 10);
    assert_eq!(rows[1].get::<_, String>(0), "b");
    assert_eq!(rows[1].get::<_, i32>(1), 20);
    shutdown_persistent(running).await;
}

#[tokio::test]
async fn partitioned_table_rejects_dropping_partition_key() {
    let (server, client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute(
            "CREATE TABLE metrics_drop_key (\
             ts TIMESTAMP NOT NULL, host TEXT NOT NULL, value INT NOT NULL\
             ) PARTITION BY RANGE (ts);\
             INSERT INTO metrics_drop_key VALUES \
             (TIMESTAMP '2026-05-20 00:00:00', 'a', 10)",
        )
        .await
        .expect("create and seed partitioned table");

    let err = client
        .batch_execute("ALTER TABLE metrics_drop_key DROP COLUMN ts")
        .await
        .expect_err("partition key drop must be rejected");
    let message = err
        .as_db_error()
        .map_or_else(|| err.to_string(), |db| db.message().to_owned());
    assert!(
        message.contains("partition"),
        "error should name partition key constraint: {message}"
    );

    assert!(
        server.time_partitions.get("metrics_drop_key").is_some(),
        "failed partition key drop must preserve runtime"
    );
    client
        .batch_execute(
            "INSERT INTO metrics_drop_key VALUES \
             (TIMESTAMP '2026-05-21 00:00:00', 'b', 20)",
        )
        .await
        .expect("insert still routes after rejected partition key drop");
    let count = client
        .query_one("SELECT COUNT(*) FROM metrics_drop_key", &[])
        .await
        .expect("count after rejected partition key drop")
        .get::<_, i64>(0);
    assert_eq!(count, 2);

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn partitioned_table_rename_column_updates_chunk_catalog() {
    let (server, client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute(
            "CREATE TABLE metrics_rename_column (\
             ts TIMESTAMP NOT NULL, host TEXT NOT NULL, value INT NOT NULL\
             ) PARTITION BY RANGE (ts);\
             INSERT INTO metrics_rename_column VALUES \
             (TIMESTAMP '2026-05-20 00:00:00', 'a', 10);\
             ALTER TABLE metrics_rename_column RENAME COLUMN value TO reading;\
             INSERT INTO metrics_rename_column VALUES \
             (TIMESTAMP '2026-05-21 00:00:00', 'b', 20)",
        )
        .await
        .expect("rename partitioned table column and insert rows");

    let rows = client
        .query(
            "SELECT host, reading FROM metrics_rename_column ORDER BY reading",
            &[],
        )
        .await
        .expect("scan renamed partitioned parent");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, String>(0), "a");
    assert_eq!(rows[1].get::<_, i32>(1), 20);

    let runtime = server
        .time_partitions
        .get("metrics_rename_column")
        .expect("partition runtime exists");
    for chunk in &runtime.chunks {
        let chunk_name = chunk.value().table_name.clone();
        let chunk_columns = client
            .query(
                "SELECT column_name FROM information_schema.columns \
                 WHERE table_name = $1 ORDER BY ordinal_position",
                &[&chunk_name],
            )
            .await
            .expect("chunk columns from information_schema");
        let names = chunk_columns
            .iter()
            .map(|row| row.get::<_, String>(0))
            .collect::<Vec<_>>();
        assert_eq!(names, ["ts", "host", "reading"]);
    }

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn partitioned_table_rename_partition_key_survives_restart() {
    let data_dir = tempfile::TempDir::new().expect("temp data dir");

    let running = start_persistent_server(data_dir.path(), "time_partition_rename_key_setup").await;
    running
        .client
        .batch_execute(
            "CREATE TABLE metrics_rename_key (\
             ts TIMESTAMP NOT NULL, host TEXT NOT NULL, value INT NOT NULL\
             ) PARTITION BY RANGE (ts);\
             INSERT INTO metrics_rename_key VALUES \
             (TIMESTAMP '2026-05-20 00:00:00', 'a', 10);\
             ALTER TABLE metrics_rename_key RENAME COLUMN ts TO event_ts;\
             INSERT INTO metrics_rename_key VALUES \
             (TIMESTAMP '2026-05-21 00:00:00', 'b', 20)",
        )
        .await
        .expect("rename partition key and insert rows");
    let rows = running
        .client
        .query(
            "SELECT host, value FROM metrics_rename_key \
             WHERE event_ts >= TIMESTAMP '2026-05-21 00:00:00'",
            &[],
        )
        .await
        .expect("scan renamed partition key before restart");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>(0), "b");
    shutdown_persistent(running).await;

    let running =
        start_persistent_server(data_dir.path(), "time_partition_rename_key_verify").await;
    let partition_column = running
        .server
        .time_partitions
        .get("metrics_rename_key")
        .expect("partition runtime rebuilt after key rename")
        .partition_column
        .clone();
    assert_eq!(partition_column, "event_ts");
    let rows = running
        .client
        .query(
            "SELECT host, value FROM metrics_rename_key \
             WHERE event_ts >= TIMESTAMP '2026-05-21 00:00:00'",
            &[],
        )
        .await
        .expect("scan renamed partition key after restart");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>(0), "b");
    shutdown_persistent(running).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partitioned_table_rebuilds_runtime_after_restart() {
    let data_dir = tempfile::TempDir::new().expect("temp data dir");

    let running = start_persistent_server(data_dir.path(), "time_partition_restart_setup").await;
    running
        .client
        .batch_execute(
            "CREATE TABLE metrics_restart (\
             ts TIMESTAMP NOT NULL, host TEXT NOT NULL, value INT NOT NULL\
             ) PARTITION BY RANGE (ts);\
             INSERT INTO metrics_restart VALUES \
             (TIMESTAMP '2026-05-20 00:00:00', 'a', 10),\
             (TIMESTAMP '2026-05-21 00:00:00', 'b', 20)",
        )
        .await
        .expect("create and seed partitioned table before restart");
    assert!(
        running
            .server
            .time_partitions
            .get("metrics_restart")
            .is_some(),
        "partition runtime must exist before restart"
    );
    shutdown_persistent(running).await;

    let running = start_persistent_server(data_dir.path(), "time_partition_restart_verify").await;
    assert!(
        running
            .server
            .time_partitions
            .get("metrics_restart")
            .is_some(),
        "partition runtime must be rebuilt after restart"
    );
    let rows = running
        .client
        .query(
            "SELECT host, value FROM metrics_restart ORDER BY value",
            &[],
        )
        .await
        .expect("scan restarted partitioned parent");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, String>(0), "a");
    assert_eq!(rows[1].get::<_, i32>(1), 20);

    running
        .client
        .batch_execute(
            "INSERT INTO metrics_restart VALUES \
             (TIMESTAMP '2026-05-22 00:00:00', 'c', 30)",
        )
        .await
        .expect("insert after restart routes to time chunk");
    let count = running
        .client
        .query_one("SELECT COUNT(*) FROM metrics_restart", &[])
        .await
        .expect("count restarted partitioned parent")
        .get::<_, i64>(0);
    assert_eq!(count, 3);
    shutdown_persistent(running).await;
}

#[tokio::test]
async fn same_partitioned_table_name_is_isolated_by_schema() {
    let (server, client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute(
            "CREATE SCHEMA app;\
             CREATE TABLE metrics (\
             ts TIMESTAMP NOT NULL, host TEXT NOT NULL, value INT NOT NULL\
             ) PARTITION BY RANGE (ts);\
             CREATE TABLE app.metrics (\
             ts TIMESTAMP NOT NULL, host TEXT NOT NULL, value INT NOT NULL\
             ) PARTITION BY RANGE (ts);",
        )
        .await
        .expect("create partitioned tables with same relation name");

    client
        .batch_execute(
            "INSERT INTO metrics VALUES \
             (TIMESTAMP '2026-05-20 00:00:00', 'public', 10);\
             INSERT INTO app.metrics VALUES \
             (TIMESTAMP '2026-05-20 00:00:00', 'app', 20);",
        )
        .await
        .expect("insert into schema-isolated partitioned tables");

    assert!(
        server.time_partitions.get("metrics").is_some(),
        "public partition runtime registered"
    );
    assert!(
        server.time_partitions.get("app.metrics").is_some(),
        "qualified partition runtime registered"
    );

    let public_rows = client
        .query("SELECT host, value FROM metrics ORDER BY value", &[])
        .await
        .expect("scan public partitioned parent");
    assert_eq!(public_rows.len(), 1);
    assert_eq!(public_rows[0].get::<_, String>(0), "public");
    assert_eq!(public_rows[0].get::<_, i32>(1), 10);

    let app_rows = client
        .query("SELECT host, value FROM app.metrics ORDER BY value", &[])
        .await
        .expect("scan app partitioned parent");
    assert_eq!(app_rows.len(), 1);
    assert_eq!(app_rows[0].get::<_, String>(0), "app");
    assert_eq!(app_rows[0].get::<_, i32>(1), 20);

    shutdown(client, server_handle).await;
}
