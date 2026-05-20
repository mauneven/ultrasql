//! End-to-end tests for same-table columnar secondary storage.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio_postgres::NoTls;
use ultrasql_core::RelationId;
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
        "host={host} port={port} user=tester application_name=columnar_storage_test",
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
async fn committed_row_store_writes_build_columnar_shadow_for_olap_scan() {
    let (server, client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE facts (id INT NOT NULL, v INT NOT NULL)")
        .await
        .expect("create facts table");

    let values = (0..256_i32)
        .map(|i| format!("({i}, {})", i * 2))
        .collect::<Vec<_>>()
        .join(",");
    client
        .batch_execute(&format!("INSERT INTO facts VALUES {values}"))
        .await
        .expect("insert facts");

    server.run_columnarization_cycle();

    let snapshot = server.catalog_snapshot();
    let entry = snapshot.tables.get("facts").expect("facts table").clone();
    drop(snapshot);
    let cached = server
        .heap
        .column_cache
        .get(RelationId(entry.oid))
        .expect("columnar shadow cached for facts");
    assert_eq!(cached.row_count(), 256);
    assert!(cached.segment_count() >= 1, "columnar shadow has segments");

    let stats = server
        .columnar_storage
        .stats("facts")
        .expect("columnar stats for facts");
    assert_eq!(stats.row_count, 256);
    assert_eq!(stats.segment_count, cached.segment_count());
    assert!(!stats.dirty, "columnar stats marked clean after rebuild");

    let rows = client
        .query("SELECT SUM(v) FROM facts", &[])
        .await
        .expect("sum over facts");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i64>(0), 65_280);

    shutdown(client, server_handle).await;
}
