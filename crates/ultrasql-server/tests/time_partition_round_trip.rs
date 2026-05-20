//! End-to-end time-series range partitioning tests.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

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
