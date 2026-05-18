//! Session JIT controls and compiled fused-aggregate round-trip.

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
        "host={host} port={port} user=tester application_name=jit_round_trip",
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
async fn jit_gucs_drive_fused_filter_sum() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    let row = client.query_one("SHOW jit", &[]).await.expect("show jit");
    let shown: &str = row.get(0);
    assert_eq!(shown, "off");

    client.batch_execute("SET jit = on").await.expect("set jit");
    client
        .batch_execute("SET jit_above_cost = 0")
        .await
        .expect("set jit threshold");

    let row = client.query_one("SHOW jit", &[]).await.expect("show jit");
    let shown: &str = row.get(0);
    assert_eq!(shown, "on");

    client
        .batch_execute("CREATE TABLE jit_t (id INT NOT NULL, x INT NOT NULL)")
        .await
        .expect("create table");
    client
        .batch_execute("INSERT INTO jit_t VALUES (1, -3), (2, 0), (3, 10), (4, 7)")
        .await
        .expect("insert rows");

    let row = client
        .query_one("SELECT SUM(x) FROM jit_t WHERE x > 0", &[])
        .await
        .expect("fused aggregate");
    let sum: i64 = row.get(0);
    assert_eq!(sum, 17);

    client
        .batch_execute("CREATE TABLE jit_big (id INT NOT NULL, x BIGINT NOT NULL)")
        .await
        .expect("create bigint table");
    client
        .batch_execute("INSERT INTO jit_big VALUES (1, -30), (2, 0), (3, 100), (4, 7)")
        .await
        .expect("insert bigint rows");

    let row = client
        .query_one("SELECT SUM(x) FROM jit_big WHERE x > 0", &[])
        .await
        .expect("fused bigint aggregate");
    let sum: i64 = row.get(0);
    assert_eq!(sum, 107);

    client.batch_execute("RESET jit").await.expect("reset jit");
    let row = client
        .query_one("SHOW jit", &[])
        .await
        .expect("show reset jit");
    let shown: &str = row.get(0);
    assert_eq!(shown, "off");

    shutdown(client, server_handle).await;
}
