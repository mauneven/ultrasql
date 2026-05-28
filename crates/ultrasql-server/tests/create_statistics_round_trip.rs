//! `CREATE STATISTICS` Simple-Query handler test (§3.3).
//!
//! Accepts the canonical PostgreSQL `CREATE STATISTICS name ON c1, c2
//! FROM table` form and exposes a `pg_statistic_ext` catalog row.

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
        "host={host} port={port} user=tester application_name=create_statistics_test",
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
async fn create_statistics_returns_command_tag() {
    let (client, _conn, server_handle) = start_server_and_connect().await;
    client
        .batch_execute("CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)")
        .await
        .expect("create");
    client
        .batch_execute("CREATE STATISTICS s_ab ON a, b FROM t")
        .await
        .expect("CREATE STATISTICS");
    let rows = client
        .query(
            "SELECT stxname, stxkeys, array_to_string(stxkind, '') \
             FROM pg_catalog.pg_statistic_ext \
             WHERE stxname = 's_ab'",
            &[],
        )
        .await
        .expect("pg_statistic_ext query");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>(0), "s_ab");
    assert_eq!(rows[0].get::<_, String>(1), "1 2");
    assert_eq!(rows[0].get::<_, String>(2), "dfm");
    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn vacuum_returns_command_tag() {
    let (client, _conn, server_handle) = start_server_and_connect().await;
    client.batch_execute("VACUUM").await.expect("VACUUM");
    shutdown(client, server_handle).await;
}
