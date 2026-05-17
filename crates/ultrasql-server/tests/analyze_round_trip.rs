//! `ANALYZE` Simple-Query handler tests.
//!
//! Verifies that the wire surface accepts `ANALYZE` and that the
//! server refreshes relation statistics in the in-memory stats catalog.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio_postgres::NoTls;
use ultrasql_server::{Server, bind_listener, serve_listener};

async fn start_server_and_connect() -> (
    tokio_postgres::Client,
    Arc<Server>,
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
) {
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("addr parses");
    let (listener, bound) = bind_listener(addr).await.expect("bind");
    let server = Arc::new(Server::with_sample_database());
    let server_for_task = Arc::clone(&server);
    let server_handle = tokio::spawn(serve_listener(listener, server_for_task));
    let conn_str = format!(
        "host={host} port={port} user=tester application_name=analyze_test",
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
    (client, server, conn_handle, server_handle)
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
async fn analyze_bare_returns_command_tag() {
    let (client, _server, _conn, server_handle) = start_server_and_connect().await;
    client.batch_execute("ANALYZE").await.expect("ANALYZE");
    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn analyze_table_returns_command_tag() {
    let (client, server, _conn, server_handle) = start_server_and_connect().await;
    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL)")
        .await
        .expect("create");
    client
        .batch_execute("INSERT INTO t VALUES (1), (2), (3)")
        .await
        .expect("seed");
    client.batch_execute("ANALYZE t").await.expect("ANALYZE t");
    let stats = server
        .lookup_relation_stats("t")
        .expect("ANALYZE should register relation stats");
    assert_eq!(stats.row_count, 3, "ANALYZE should see all inserted rows");
    // Session survives — subsequent statements work.
    let rows = client
        .query("SELECT id FROM t", &[])
        .await
        .expect("select after ANALYZE");
    assert_eq!(rows.len(), 3);
    shutdown(client, server_handle).await;
}
