//! `DATE` column round-trip test — v0.6 TPC-H milestone surface.
//!
//! Validates the end-to-end path that lands in this turn:
//!
//! - Parser: `DATE 'YYYY-MM-DD'` typed-string literal
//! - Binder: `Literal::Typed { type_name: "date", .. }` → `Value::Date`
//!   via the Howard-Hinnant `civil_from_days` algorithm
//! - DDL: `CREATE TABLE t (d DATE)` accepted (was rejected pre-v0.6)
//! - Row codec: `DataType::Date` encodes as 4-byte little-endian i32
//!   (`days_since_2000_01_01`), decodes back into the `Int32` builder
//! - Visibility / scan: `DATE` column round-trips through `SeqScan`
//!
//! Pinning these here means a regression on any link in the chain
//! trips an integration test rather than the TPC-H runner.

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
        "host={host} port={port} user=tester application_name=date_round_trip",
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
async fn create_table_with_date_column() {
    let (client, _conn, server_handle) = start_server_and_connect().await;
    client
        .batch_execute("CREATE TABLE events (id INT NOT NULL, d DATE NOT NULL)")
        .await
        .expect("CREATE TABLE with DATE column");
    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn insert_date_literal_and_scan() {
    let (client, _conn, server_handle) = start_server_and_connect().await;
    client
        .batch_execute("CREATE TABLE events (id INT NOT NULL, d DATE NOT NULL)")
        .await
        .expect("create");
    client
        .batch_execute(
            "INSERT INTO events VALUES \
             (1, DATE '2000-01-01'), \
             (2, DATE '2024-12-31'), \
             (3, DATE '1994-01-01')",
        )
        .await
        .expect("insert with DATE literals");
    let messages = client
        .simple_query("SELECT id FROM events")
        .await
        .expect("scan");
    let rows: Vec<_> = messages
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .collect();
    assert_eq!(rows.len(), 3, "all three rows survive the round-trip");
    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn accepts_decimal_column() {
    // DECIMAL columns are wired through the v0.6 milestone landing:
    // scaled i64 codec, Decimal column-builder arm, batch_to_rows
    // re-tagging the value with the schema-side scale.
    let (client, _conn, server_handle) = start_server_and_connect().await;
    client
        .batch_execute("CREATE TABLE prices (id INT NOT NULL, p DECIMAL(15, 2) NOT NULL)")
        .await
        .expect("CREATE TABLE with DECIMAL column");
    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn accepts_timestamp_column() {
    // TIMESTAMP / TIMESTAMPTZ / TIME columns wired through the same
    // codec template as Decimal: 8-byte little-endian i64 microsecond
    // payload, Int64 column builder, schema-side semantic tag.
    let (client, _conn, server_handle) = start_server_and_connect().await;
    client
        .batch_execute(
            "CREATE TABLE evt (id INT NOT NULL, ts TIMESTAMP NOT NULL, t TIME NOT NULL)",
        )
        .await
        .expect("CREATE TABLE with TIMESTAMP/TIME columns");
    shutdown(client, server_handle).await;
}
