//! Persistent `COMMENT ON` restart coverage through the PostgreSQL wire path.

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use tokio_postgres::NoTls;
use ultrasql_catalog::bootstrap::PG_CLASS_OID;
use ultrasql_core::Oid;
use ultrasql_server::{Server, bind_listener, serve_listener};

async fn start_persistent_server(
    data_dir: &Path,
) -> (
    Arc<Server>,
    tokio_postgres::Client,
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
) {
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("addr parses");
    let (listener, bound) = bind_listener(addr).await.expect("bind");
    let server = Arc::new(Server::init(data_dir).expect("persistent server init"));
    let server_handle = tokio::spawn(serve_listener(listener, Arc::clone(&server)));

    let conn_str = format!(
        "host={host} port={port} user=tester application_name=comment_restart_test",
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn table_comment_survives_restart() {
    let data_dir = tempfile::TempDir::new().unwrap();

    {
        let (_server, client, _conn_handle, server_handle) =
            start_persistent_server(data_dir.path()).await;
        client
            .batch_execute("CREATE TABLE comment_restart (id INT)")
            .await
            .expect("create");
        client
            .batch_execute("COMMENT ON TABLE comment_restart IS 'durable table docs'")
            .await
            .expect("comment");
        shutdown(client, server_handle).await;
    }

    {
        let (server, client, _conn_handle, server_handle) =
            start_persistent_server(data_dir.path()).await;
        let snapshot = server.catalog_snapshot();
        let table = snapshot
            .tables
            .get("comment_restart")
            .expect("table after restart");
        let row = snapshot
            .descriptions
            .get(&(table.oid, Oid::new(PG_CLASS_OID), 0))
            .expect("table comment after restart");
        assert_eq!(row.description, "durable table docs");
        shutdown(client, server_handle).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cleared_table_comment_stays_cleared_after_restart() {
    let data_dir = tempfile::TempDir::new().unwrap();

    {
        let (_server, client, _conn_handle, server_handle) =
            start_persistent_server(data_dir.path()).await;
        client
            .batch_execute("CREATE TABLE comment_clear_restart (id INT)")
            .await
            .expect("create");
        client
            .batch_execute("COMMENT ON TABLE comment_clear_restart IS 'temporary docs'")
            .await
            .expect("comment");
        client
            .batch_execute("COMMENT ON TABLE comment_clear_restart IS NULL")
            .await
            .expect("clear comment");
        shutdown(client, server_handle).await;
    }

    {
        let (server, client, _conn_handle, server_handle) =
            start_persistent_server(data_dir.path()).await;
        let snapshot = server.catalog_snapshot();
        let table = snapshot
            .tables
            .get("comment_clear_restart")
            .expect("table after restart");
        assert!(
            !snapshot
                .descriptions
                .contains_key(&(table.oid, Oid::new(PG_CLASS_OID), 0))
        );
        shutdown(client, server_handle).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_table_comment_does_not_survive_restart() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let dropped_oid;

    {
        let (server, client, _conn_handle, server_handle) =
            start_persistent_server(data_dir.path()).await;
        client
            .batch_execute("CREATE TABLE comment_drop_restart (id INT)")
            .await
            .expect("create");
        dropped_oid = server
            .catalog_snapshot()
            .tables
            .get("comment_drop_restart")
            .expect("table before drop")
            .oid;
        client
            .batch_execute("COMMENT ON TABLE comment_drop_restart IS 'drop me'")
            .await
            .expect("comment");
        client
            .batch_execute("DROP TABLE comment_drop_restart")
            .await
            .expect("drop");
        shutdown(client, server_handle).await;
    }

    {
        let (server, client, _conn_handle, server_handle) =
            start_persistent_server(data_dir.path()).await;
        let snapshot = server.catalog_snapshot();
        assert!(!snapshot.tables.contains_key("comment_drop_restart"));
        assert!(
            !snapshot
                .descriptions
                .contains_key(&(dropped_oid, Oid::new(PG_CLASS_OID), 0))
        );
        shutdown(client, server_handle).await;
    }
}
