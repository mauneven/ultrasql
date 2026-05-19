//! End-to-end UUID type and `gen_random_uuid()` tests.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio_postgres::{NoTls, SimpleQueryMessage};
use ultrasql_core::Value;
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
        "host={host} port={port} user=tester application_name=uuid_test",
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

fn simple_rows(messages: &[SimpleQueryMessage]) -> Vec<Vec<String>> {
    messages
        .iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|idx| row.get(idx).unwrap_or("").to_owned())
                    .collect(),
            ),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn uuid_literals_and_gen_random_uuid_round_trip() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (id UUID, label TEXT NOT NULL)")
        .await
        .expect("create uuid table");
    client
        .batch_execute(
            "INSERT INTO t VALUES ('12345678-9abc-def0-1234-56789abcdef0'::uuid, 'fixed')",
        )
        .await
        .expect("insert uuid literal");

    let generated = client
        .simple_query("INSERT INTO t VALUES (gen_random_uuid(), 'generated') RETURNING id")
        .await
        .expect("insert generated uuid");
    let returned = simple_rows(&generated);
    assert_eq!(returned.len(), 1);
    assert!(Value::parse_uuid(&returned[0][0]).is_some());

    let selected = client
        .simple_query("SELECT id, label FROM t ORDER BY label")
        .await
        .expect("select uuid rows");
    let rows = simple_rows(&selected);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], "12345678-9abc-def0-1234-56789abcdef0");
    assert_eq!(rows[0][1], "fixed");
    assert!(Value::parse_uuid(&rows[1][0]).is_some());
    assert_eq!(rows[1][1], "generated");

    shutdown(client, server_handle).await;
}
