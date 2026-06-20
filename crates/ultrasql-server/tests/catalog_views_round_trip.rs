//! End-to-end metadata view tests.
//!
//! These tests drive the virtual `pg_catalog` / `information_schema`
//! relations through the normal SQL path used by CLI `\d`-style commands.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio_postgres::NoTls;
use ultrasql_server::{Server, bind_listener, serve_listener};

async fn start_server_and_connect() -> (
    Arc<Server>,
    tokio_postgres::Client,
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
) {
    start_server_and_connect_with(Server::with_sample_database()).await
}

async fn start_server_and_connect_with(
    server: Server,
) -> (
    Arc<Server>,
    tokio_postgres::Client,
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
) {
    start_server_and_connect_with_user(server, "tester").await
}

async fn start_server_and_connect_with_user(
    server: Server,
    user: &str,
) -> (
    Arc<Server>,
    tokio_postgres::Client,
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
) {
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("addr parses");
    let (listener, bound) = bind_listener(addr).await.expect("bind");
    let server = Arc::new(server);
    let server_handle = tokio::spawn(serve_listener(listener, Arc::clone(&server)));
    let conn_str = format!(
        "host={host} port={port} user={user} application_name=catalog_views_test",
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

#[path = "catalog_views_round_trip/catalog_reflection.rs"]
mod catalog_reflection;
#[path = "catalog_views_round_trip/gui_browsers.rs"]
mod gui_browsers;
#[path = "catalog_views_round_trip/jdbc_and_collation.rs"]
mod jdbc_and_collation;
#[path = "catalog_views_round_trip/orm_probes.rs"]
mod orm_probes;
#[path = "catalog_views_round_trip/pg_proc.rs"]
mod pg_proc;
#[path = "catalog_views_round_trip/pg_settings.rs"]
mod pg_settings;
#[path = "catalog_views_round_trip/pg_stat_activity.rs"]
mod pg_stat_activity;
#[path = "catalog_views_round_trip/psql_describe.rs"]
mod psql_describe;
#[path = "catalog_views_round_trip/psql_list.rs"]
mod psql_list;
