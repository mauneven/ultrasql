//! Persistent `ALTER TABLE` restart coverage through the PostgreSQL wire path.

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use tokio_postgres::NoTls;
use ultrasql_server::{Server, bind_listener, serve_listener};

async fn start_persistent_server(
    data_dir: &Path,
) -> (
    tokio_postgres::Client,
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
) {
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("addr parses");
    let (listener, bound) = bind_listener(addr).await.expect("bind");
    let server = Arc::new(Server::init(data_dir).expect("persistent server init"));
    let server_handle = tokio::spawn(serve_listener(listener, server));

    let conn_str = format!(
        "host={host} port={port} user=tester application_name=alter_restart_test",
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
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_table_drop_column_rewrite_survives_restart() {
    let data_dir = tempfile::TempDir::new().unwrap();

    let (client, _conn_handle, server_handle) = start_persistent_server(data_dir.path()).await;
    client
        .batch_execute("CREATE TABLE alter_restart (id INT, label TEXT)")
        .await
        .expect("create");
    client
        .batch_execute("INSERT INTO alter_restart VALUES (1, 'alpha'), (2, 'bravo')")
        .await
        .expect("seed");
    client
        .batch_execute("ALTER TABLE alter_restart DROP COLUMN label")
        .await
        .expect("alter");
    let rows = client
        .query("SELECT id FROM alter_restart ORDER BY id", &[])
        .await
        .expect("select after alter");
    assert_eq!(rows.len(), 2);
    shutdown(client, server_handle).await;

    let (client, _conn_handle, server_handle) = start_persistent_server(data_dir.path()).await;
    let rows = client
        .query("SELECT id FROM alter_restart ORDER BY id", &[])
        .await
        .expect("select after restart");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, i32>(0), 1);
    assert_eq!(rows[1].get::<_, i32>(0), 2);
    shutdown(client, server_handle).await;
}
