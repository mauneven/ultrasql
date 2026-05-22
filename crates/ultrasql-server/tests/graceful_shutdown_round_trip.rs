//! Listener shutdown tests.
//!
//! These cover production teardown behavior that abort-based test helpers
//! cannot prove: stop accepting new connections, let the listener future
//! return `Ok(())`, then restart from the same data directory.

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::oneshot;
use tokio_postgres::NoTls;
use ultrasql_server::{Server, bind_listener, serve_listener_with_shutdown};

struct RunningServer {
    client: tokio_postgres::Client,
    conn_handle: tokio::task::JoinHandle<()>,
    server_handle: tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
    shutdown_tx: oneshot::Sender<()>,
}

async fn start_persistent_server(data_dir: &Path) -> RunningServer {
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("addr parses");
    let (listener, bound) = bind_listener(addr).await.expect("bind");
    let server = Arc::new(Server::init(data_dir).expect("persistent server init"));
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server_handle = tokio::spawn(serve_listener_with_shutdown(listener, server, async move {
        let _ = shutdown_rx.await;
    }));
    let conn_str = format!(
        "host={host} port={port} user=tester application_name=graceful_shutdown_test",
        host = bound.ip(),
        port = bound.port()
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .expect("tokio-postgres connect");
    let conn_handle = tokio::spawn(async move {
        let _ = connection.await;
    });
    RunningServer {
        client,
        conn_handle,
        server_handle,
        shutdown_tx,
    }
}

async fn shutdown_server(running: RunningServer) {
    drop(running.client);
    tokio::time::timeout(Duration::from_secs(2), running.conn_handle)
        .await
        .expect("connection task exits")
        .expect("connection task joins");
    let _ = running.shutdown_tx.send(());
    tokio::time::timeout(Duration::from_secs(2), running.server_handle)
        .await
        .expect("server shutdown completes")
        .expect("server task joins")
        .expect("listener exits cleanly");
}

#[tokio::test]
async fn graceful_shutdown_flushes_table_before_restart() {
    let temp = tempfile::tempdir().expect("tempdir");
    let running = start_persistent_server(temp.path()).await;

    running
        .client
        .batch_execute("CREATE TABLE graceful_shutdown_t (id INT NOT NULL, v INT NOT NULL)")
        .await
        .expect("create before shutdown");
    running
        .client
        .batch_execute("INSERT INTO graceful_shutdown_t VALUES (1, 10), (2, 20)")
        .await
        .expect("insert before shutdown");
    let rows = running
        .client
        .query("SELECT id FROM graceful_shutdown_t ORDER BY id", &[])
        .await
        .expect("select before shutdown");
    assert_eq!(rows.len(), 2);

    shutdown_server(running).await;

    let running = start_persistent_server(temp.path()).await;
    let rows = running
        .client
        .query("SELECT id, v FROM graceful_shutdown_t ORDER BY id", &[])
        .await
        .expect("select after restart");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, i32>(0), 1);
    assert_eq!(rows[0].get::<_, i32>(1), 10);
    assert_eq!(rows[1].get::<_, i32>(0), 2);
    assert_eq!(rows[1].get::<_, i32>(1), 20);

    shutdown_server(running).await;
}
