//! End-to-end VECTOR(n) type metadata tests.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio_postgres::{NoTls, SimpleQueryMessage};
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
        "host={host} port={port} user=tester application_name=vector_type_test",
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
async fn create_table_with_vector_column_reports_vector_metadata() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE embeddings (id INT NOT NULL, embedding VECTOR(1536))")
        .await
        .expect("create vector table");

    let messages = client
        .simple_query(
            "SELECT data_type \
             FROM information_schema.columns \
             WHERE table_name = 'embeddings' AND column_name = 'embedding'",
        )
        .await
        .expect("query vector metadata");
    let rows = simple_rows(&messages);
    assert_eq!(rows, vec![vec!["vector".to_owned()]]);

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn insert_and_select_vector_column_round_trips_text_form() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE embeddings (id INT NOT NULL, embedding VECTOR(3))")
        .await
        .expect("create vector table");
    client
        .batch_execute("INSERT INTO embeddings VALUES (1, '[1, 2.5, -3]')")
        .await
        .expect("insert vector row");

    let messages = client
        .simple_query("SELECT embedding FROM embeddings WHERE id = 1")
        .await
        .expect("select vector row");
    let rows = simple_rows(&messages);
    assert_eq!(rows, vec![vec!["[1,2.5,-3]".to_owned()]]);

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn vector_typed_literals_and_casts_round_trip() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE embeddings (id INT NOT NULL, embedding VECTOR(3))")
        .await
        .expect("create vector table");
    client
        .batch_execute(
            "INSERT INTO embeddings VALUES \
             (1, VECTOR '[1,2,3]'), \
             (2, CAST('[4,5,6]' AS VECTOR(3))), \
             (3, '[7,8,9]'::VECTOR(3))",
        )
        .await
        .expect("insert vector rows");

    let messages = client
        .simple_query("SELECT embedding FROM embeddings ORDER BY id")
        .await
        .expect("select vector rows");
    let rows = simple_rows(&messages);
    assert_eq!(
        rows,
        vec![
            vec!["[1,2,3]".to_owned()],
            vec!["[4,5,6]".to_owned()],
            vec!["[7,8,9]".to_owned()],
        ]
    );

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn vector_distance_operators_execute_in_sql() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE embeddings (id INT NOT NULL, embedding VECTOR(3))")
        .await
        .expect("create vector table");
    client
        .batch_execute("INSERT INTO embeddings VALUES (1, '[1, 2, 3]')")
        .await
        .expect("insert vector row");

    let messages = client
        .simple_query(
            "SELECT \
                 embedding <-> '[1,2,4]', \
                 embedding <#> '[4,5,6]', \
                 embedding <=> '[3,-6,3]', \
                 embedding <+> '[3,2,-1]' \
             FROM embeddings WHERE id = 1",
        )
        .await
        .expect("select vector distances");
    let rows = simple_rows(&messages);
    assert_eq!(
        rows,
        vec![vec![
            "1".to_owned(),
            "-32".to_owned(),
            "1".to_owned(),
            "6".to_owned()
        ]]
    );

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn insert_rejects_vector_dimension_mismatch() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE embeddings (id INT NOT NULL, embedding VECTOR(3))")
        .await
        .expect("create vector table");
    let err = client
        .batch_execute("INSERT INTO embeddings VALUES (1, '[1, 2]')")
        .await
        .expect_err("dimension mismatch rejected");
    let message = err
        .as_db_error()
        .map(tokio_postgres::error::DbError::message)
        .unwrap_or_default();
    assert!(
        message.contains("type mismatch") || message.contains("vector"),
        "unexpected error: {err}"
    );

    shutdown(client, server_handle).await;
}
