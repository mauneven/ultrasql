//! Persistent `COPY FROM` restart coverage through the PostgreSQL wire path.

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::SinkExt;
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
        "host={host} port={port} user=tester application_name=copy_restart_test",
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

async fn select_count(client: &tokio_postgres::Client, table: &str) -> i64 {
    let rows = client
        .simple_query(&format!("SELECT COUNT(*) FROM {table}"))
        .await
        .expect("count query");
    rows.into_iter()
        .find_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => row
                .get(0)
                .map(|cell| cell.parse::<i64>().expect("count parses")),
            _ => None,
        })
        .expect("COUNT(*) returned a row")
}

async fn copy_in_payload(client: &tokio_postgres::Client, sql: &str, payload: &[u8]) -> u64 {
    let sink = client
        .copy_in::<_, Bytes>(sql)
        .await
        .expect("copy_in establishes COPY FROM STDIN");
    futures::pin_mut!(sink);
    sink.as_mut()
        .send(Bytes::from(payload.to_vec()))
        .await
        .expect("send CopyData");
    sink.finish().await.expect("finish copy_in")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn copy_from_stdin_rows_survive_restart() {
    let data_dir = tempfile::TempDir::new().unwrap();

    let (client, _conn_handle, server_handle) = start_persistent_server(data_dir.path()).await;
    client
        .simple_query("CREATE TABLE copy_restart (id INT, label TEXT)")
        .await
        .expect("create table");
    let copied = copy_in_payload(
        &client,
        "COPY copy_restart (id, label) FROM STDIN WITH (FORMAT csv)",
        b"1,alpha\n2,bravo\n",
    )
    .await;
    assert_eq!(copied, 2);
    assert_eq!(select_count(&client, "copy_restart").await, 2);
    shutdown(client, server_handle).await;

    let (client, _conn_handle, server_handle) = start_persistent_server(data_dir.path()).await;
    assert_eq!(select_count(&client, "copy_restart").await, 2);
    shutdown(client, server_handle).await;
}
