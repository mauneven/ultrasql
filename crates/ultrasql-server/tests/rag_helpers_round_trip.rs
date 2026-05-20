//! End-to-end RAG helper SQL tests.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio_postgres::{NoTls, SimpleQueryMessage};
use ultrasql_catalog::rag::{
    RagSchemaConfig, create_rag_table_statements, filter_rag_documents_by_metadata_sql,
    insert_rag_chunk_sql, search_rag_embeddings_sql,
};
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
        "host={host} port={port} user=tester application_name=rag_helpers_test",
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

fn bind_sql(template: &str, values: &[(&str, &str)]) -> String {
    let mut sql = template.to_owned();
    for (param, value) in values.iter().rev() {
        sql = sql.replace(param, value);
    }
    sql
}

#[tokio::test]
async fn rag_helpers_execute_as_plain_sql() {
    let (client, _conn, server_handle) = start_server_and_connect().await;
    let config = RagSchemaConfig {
        prefix: "rag".to_owned(),
        embedding_dims: 3,
    };

    for statement in create_rag_table_statements(&config).expect("build rag DDL") {
        client
            .batch_execute(&statement)
            .await
            .expect("create rag primitive table");
    }

    client
        .batch_execute(
            "INSERT INTO rag_documents VALUES \
             ('doc-a', 's3://bucket/a.md', 'Doc A', 'hash-a', '{\"tenant\":\"a\",\"kind\":\"guide\"}', \
              TIMESTAMP '2026-05-19 10:00:00', TIMESTAMP '2026-05-20 10:00:00', \
              TIMESTAMP '2026-05-20 10:05:00', 2, true)",
        )
        .await
        .expect("insert document");

    let chunk_sql = bind_sql(
        &insert_rag_chunk_sql(&config).expect("build chunk helper"),
        &[
            ("$1", "'chunk-a-0'"),
            ("$2", "'doc-a'"),
            ("$3", "0"),
            ("$4", "'normal SQL stays inspectable'"),
            ("$5", "0"),
            ("$6", "4"),
            ("$7", "'{\"section\":\"body\"}'::jsonb"),
            ("$8", "TIMESTAMP '2026-05-20 10:00:01'"),
            ("$9", "TIMESTAMP '2026-05-20 10:00:02'"),
            ("$10", "7"),
            ("$11", "true"),
        ],
    );
    let inserted = client
        .simple_query(&chunk_sql)
        .await
        .expect("chunk helper inserts");
    assert_eq!(
        simple_rows(&inserted),
        vec![vec![
            "chunk-a-0".to_owned(),
            "doc-a".to_owned(),
            "0".to_owned(),
            "7".to_owned(),
            "t".to_owned()
        ]]
    );

    client
        .batch_execute(
            "INSERT INTO rag_embeddings VALUES \
             ('emb-a-0', 'chunk-a-0', VECTOR '[1,0,0]', 'test-model', 'v1', '{\"dims\":3}', \
              TIMESTAMP '2026-05-20 10:01:00', 7, true)",
        )
        .await
        .expect("insert embedding");

    let filtered_sql = bind_sql(
        &filter_rag_documents_by_metadata_sql(&config).expect("build metadata helper"),
        &[("$1", "'{\"tenant\":\"a\"}'::jsonb"), ("$2", "10")],
    );
    let filtered = client
        .simple_query(&filtered_sql)
        .await
        .expect("metadata helper filters");
    assert_eq!(
        simple_rows(&filtered),
        vec![vec![
            "doc-a".to_owned(),
            "s3://bucket/a.md".to_owned(),
            "Doc A".to_owned(),
            "2".to_owned(),
            "832586400000000".to_owned()
        ]]
    );

    let search_sql = bind_sql(
        &search_rag_embeddings_sql(&config).expect("build search helper"),
        &[("$1", "VECTOR '[1,0,0]'"), ("$2", "5")],
    );
    let nearest = client
        .simple_query(&search_sql)
        .await
        .expect("embedding helper searches");
    assert_eq!(
        simple_rows(&nearest),
        vec![vec![
            "emb-a-0".to_owned(),
            "chunk-a-0".to_owned(),
            "7".to_owned(),
            "0".to_owned()
        ]]
    );

    shutdown(client, server_handle).await;
}
