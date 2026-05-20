//! End-to-end RAG storage primitive tests.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio_postgres::{NoTls, SimpleQueryMessage};
use ultrasql_catalog::rag::{RagSchemaConfig, create_rag_table_statements};
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
        "host={host} port={port} user=tester application_name=rag_primitives_test",
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
async fn rag_primitives_store_metadata_recency_versions_and_embeddings() {
    let (client, _conn, server_handle) = start_server_and_connect().await;
    let ddl = create_rag_table_statements(&RagSchemaConfig {
        prefix: "rag".to_owned(),
        embedding_dims: 3,
    })
    .expect("build rag DDL");

    for statement in ddl {
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
              TIMESTAMP '2026-05-20 10:05:00', 2, true), \
             ('doc-old', 's3://bucket/old.md', 'Old Doc', 'hash-old', '{\"tenant\":\"a\",\"kind\":\"old\"}', \
              TIMESTAMP '2026-05-18 10:00:00', TIMESTAMP '2026-05-18 11:00:00', \
              TIMESTAMP '2026-05-18 11:05:00', 1, false)",
        )
        .await
        .expect("insert rag documents");
    client
        .batch_execute(
            "INSERT INTO rag_chunks VALUES \
             ('chunk-a-0', 'doc-a', 0, 'vector search needs exact fallback', 0, 5, \
              '{\"section\":\"intro\"}', TIMESTAMP '2026-05-20 10:00:01', \
              TIMESTAMP '2026-05-20 10:00:02', 2, true), \
             ('chunk-old-0', 'doc-old', 0, 'stale content', 0, 2, \
              '{\"section\":\"archive\"}', TIMESTAMP '2026-05-18 10:00:01', \
              TIMESTAMP '2026-05-18 10:00:02', 1, false)",
        )
        .await
        .expect("insert rag chunks");
    client
        .batch_execute(
            "INSERT INTO rag_embeddings VALUES \
             ('emb-a-0', 'chunk-a-0', VECTOR '[1,0,0]', 'test-model', 'v1', '{\"dims\":3}', \
              TIMESTAMP '2026-05-20 10:01:00', 2, true), \
             ('emb-old-0', 'chunk-old-0', VECTOR '[0,1,0]', 'test-model', 'v1', '{\"dims\":3}', \
              TIMESTAMP '2026-05-18 10:01:00', 1, false)",
        )
        .await
        .expect("insert rag embeddings");

    let recent = client
        .simple_query(
            "SELECT document_id, version \
             FROM rag_documents \
             WHERE is_current = true AND metadata @> '{\"tenant\":\"a\"}'::jsonb \
             ORDER BY updated_at DESC",
        )
        .await
        .expect("query recent documents");
    assert_eq!(
        simple_rows(&recent),
        vec![vec!["doc-a".to_owned(), "2".to_owned()]]
    );

    let chunks = client
        .simple_query(
            "SELECT chunk_id, chunk_index, version \
             FROM rag_chunks \
             WHERE document_id = 'doc-a' AND is_current = true \
             ORDER BY chunk_index",
        )
        .await
        .expect("query current chunks");
    assert_eq!(
        simple_rows(&chunks),
        vec![vec!["chunk-a-0".to_owned(), "0".to_owned(), "2".to_owned()]]
    );

    let nearest = client
        .simple_query(
            "SELECT chunk_id, version \
             FROM rag_embeddings \
             WHERE is_current = true \
             ORDER BY embedding <-> VECTOR '[1,0,0]' \
             LIMIT 1",
        )
        .await
        .expect("query nearest current embedding");
    assert_eq!(
        simple_rows(&nearest),
        vec![vec!["chunk-a-0".to_owned(), "2".to_owned()]]
    );

    shutdown(client, server_handle).await;
}
