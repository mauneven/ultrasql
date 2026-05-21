//! Restart checks for durable B-tree index metadata and rebuilt pages.

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use tokio_postgres::NoTls;
use ultrasql_server::{Server, bind_listener, serve_listener};

async fn start_persistent_server_and_connect(
    data_dir: &Path,
) -> (
    Arc<Server>,
    tokio_postgres::Client,
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
) {
    let server = Arc::new(Server::init(data_dir).expect("persistent server init"));
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("addr parses");
    let (listener, bound) = bind_listener(addr).await.expect("bind");
    let server_handle = tokio::spawn(serve_listener(listener, Arc::clone(&server)));
    let conn_str = format!(
        "host={host} port={port} user=tester application_name=btree_restart_test",
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
    let _ = server_handle.await;
}

#[tokio::test]
async fn btree_index_restarts_with_rebuilt_pages() {
    let dir = tempfile::tempdir().expect("tempdir");

    {
        let (server, client, _conn, server_handle) =
            start_persistent_server_and_connect(dir.path()).await;
        client
            .batch_execute("CREATE TABLE backup_restore_smoke (id INT, payload TEXT)")
            .await
            .expect("create table");
        client
            .batch_execute(
                "INSERT INTO backup_restore_smoke VALUES
                    (1, 'alpha'),
                    (2, 'bravo'),
                    (3, 'charlie')",
            )
            .await
            .expect("seed rows");
        client
            .batch_execute("CREATE INDEX backup_restore_smoke_id_idx ON backup_restore_smoke (id)")
            .await
            .expect("create index");
        let before: String = client
            .query_one("SELECT payload FROM backup_restore_smoke WHERE id = 2", &[])
            .await
            .expect("query before restart")
            .get(0);
        assert_eq!(before, "bravo");

        shutdown(client, server_handle).await;
        drop(server);
    }

    {
        let (server, client, _conn, server_handle) =
            start_persistent_server_and_connect(dir.path()).await;
        let count: i64 = client
            .query_one("SELECT COUNT(*) FROM backup_restore_smoke", &[])
            .await
            .expect("count after restart")
            .get(0);
        assert_eq!(count, 3);
        let after: String = client
            .query_one("SELECT payload FROM backup_restore_smoke WHERE id = 2", &[])
            .await
            .expect("index query after restart")
            .get(0);
        assert_eq!(after, "bravo");

        shutdown(client, server_handle).await;
        drop(server);
    }
}
