//! End-to-end scalar system-function compatibility tests.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio_postgres::NoTls;
use ultrasql_server::{Server, bind_listener, serve_listener};

async fn start_server_and_connect() -> (
    tokio_postgres::Client,
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
) {
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("addr parses");
    let (listener, bound) = bind_listener(addr).await.expect("bind");
    let server = Arc::new(Server::with_sample_database());
    let server_handle = tokio::spawn(serve_listener(listener, server));
    let conn_str = format!(
        "host={host} port={port} user=tester application_name=system_functions_test",
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

#[tokio::test]
async fn scalar_system_functions_return_postgres_shaped_values() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    let rows = client
        .query(
            "SELECT version(), current_database(), current_user(), pg_typeof(1), pg_size_pretty(2048)",
            &[],
        )
        .await
        .expect("system functions");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>(0), "UltraSQL 0.0.1");
    assert_eq!(rows[0].get::<_, String>(1), "ultrasql");
    assert_eq!(rows[0].get::<_, String>(2), "user");
    assert_eq!(rows[0].get::<_, String>(3), "integer");
    assert_eq!(rows[0].get::<_, String>(4), "2 kB");

    let bare = client
        .query("SELECT current_user, session_user", &[])
        .await
        .expect("bare user functions");
    assert_eq!(bare.len(), 1);
    assert_eq!(bare[0].get::<_, String>(0), "user");
    assert_eq!(bare[0].get::<_, String>(1), "user");

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn pg_relation_size_reports_heap_pages() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE sized (id INT NOT NULL, name TEXT)")
        .await
        .expect("create sized table");
    client
        .batch_execute("INSERT INTO sized VALUES (1, 'a'), (2, 'b')")
        .await
        .expect("insert sized rows");

    let rows = client
        .query(
            "SELECT pg_relation_size('sized'), pg_size_pretty(pg_relation_size('public.sized'))",
            &[],
        )
        .await
        .expect("relation size");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i64>(0), 8192);
    assert_eq!(rows[0].get::<_, String>(1), "8 kB");

    shutdown(client, server_handle).await;
}
