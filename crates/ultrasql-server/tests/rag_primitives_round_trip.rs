//! End-to-end RAG storage primitive tests.

use tokio_postgres::SimpleQueryMessage;
use ultrasql_catalog::rag::{RagSchemaConfig, create_rag_table_statements};

pub mod support;

use support::{shutdown, start_sample_server};

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
    let running = start_sample_server("rag_primitives_test").await;
    let client = &running.client;
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
             ('tenant-a', 'doc-a', 's3://bucket/a.md', 'Doc A', 'hash-a', '{\"kind\":\"guide\"}', \
              TIMESTAMP '2026-05-19 10:00:00', TIMESTAMP '2026-05-20 10:00:00', \
              TIMESTAMP '2026-05-20 10:05:00', 2, true), \
             ('tenant-b', 'doc-b', 's3://bucket/b.md', 'Doc B', 'hash-b', '{\"kind\":\"guide\"}', \
              TIMESTAMP '2026-05-19 10:00:00', TIMESTAMP '2026-05-20 10:30:00', \
              TIMESTAMP '2026-05-20 10:35:00', 3, true), \
             ('tenant-a', 'doc-old', 's3://bucket/old.md', 'Old Doc', 'hash-old', '{\"kind\":\"old\"}', \
              TIMESTAMP '2026-05-18 10:00:00', TIMESTAMP '2026-05-18 11:00:00', \
              TIMESTAMP '2026-05-18 11:05:00', 1, false)",
        )
        .await
        .expect("insert rag documents");
    client
        .batch_execute(
            "INSERT INTO rag_chunks VALUES \
             ('tenant-a', 'chunk-a-0', 'doc-a', 0, 'vector search needs exact fallback', 0, 5, \
              '{\"section\":\"intro\"}', TIMESTAMP '2026-05-20 10:00:01', \
              TIMESTAMP '2026-05-20 10:00:02', 2, true), \
             ('tenant-b', 'chunk-b-0', 'doc-b', 0, 'other tenant content', 0, 3, \
              '{\"section\":\"intro\"}', TIMESTAMP '2026-05-20 10:30:01', \
              TIMESTAMP '2026-05-20 10:30:02', 3, true), \
             ('tenant-a', 'chunk-old-0', 'doc-old', 0, 'stale content', 0, 2, \
              '{\"section\":\"archive\"}', TIMESTAMP '2026-05-18 10:00:01', \
              TIMESTAMP '2026-05-18 10:00:02', 1, false)",
        )
        .await
        .expect("insert rag chunks");
    client
        .batch_execute(
            "INSERT INTO rag_embeddings VALUES \
             ('tenant-a', 'emb-a-0', 'chunk-a-0', VECTOR '[1,0,0]', 'test-model', 'v1', '{\"dims\":3}', \
              TIMESTAMP '2026-05-20 10:01:00', 2, true), \
             ('tenant-b', 'emb-b-0', 'chunk-b-0', VECTOR '[1,0,0]', 'test-model', 'v1', '{\"dims\":3}', \
              TIMESTAMP '2026-05-20 10:31:00', 3, true), \
             ('tenant-a', 'emb-old-0', 'chunk-old-0', VECTOR '[0,1,0]', 'test-model', 'v1', '{\"dims\":3}', \
              TIMESTAMP '2026-05-18 10:01:00', 1, false)",
        )
        .await
        .expect("insert rag embeddings");

    client
        .batch_execute(
            "INSERT INTO rag_retrieval_events VALUES \
             ('tenant-a', 'retr-a-0', 'vector search', VECTOR '[1,0,0]', 'hybrid', 3, \
              '{\"kind\":\"guide\"}', '{\"bm25\":1,\"vector\":2}', 1200, \
              TIMESTAMP '2026-05-20 10:02:00'), \
             ('tenant-b', 'retr-b-0', 'vector search', VECTOR '[1,0,0]', 'hybrid', 3, \
              '{\"kind\":\"guide\"}', '{\"bm25\":1,\"vector\":2}', 1500, \
              TIMESTAMP '2026-05-20 10:32:00')",
        )
        .await
        .expect("insert retrieval events");
    client
        .batch_execute(
            "INSERT INTO rag_answer_citations VALUES \
             ('tenant-a', 'cite-a-0', 'retr-a-0', 'answer-a', 'doc-a', 'chunk-a-0', 0, 0.99, \
              'vector search needs exact fallback', '{\"source\":\"retrieval\"}', \
              TIMESTAMP '2026-05-20 10:03:00'), \
             ('tenant-b', 'cite-b-0', 'retr-b-0', 'answer-b', 'doc-b', 'chunk-b-0', 0, 0.75, \
              'other tenant content', '{\"source\":\"retrieval\"}', \
              TIMESTAMP '2026-05-20 10:33:00')",
        )
        .await
        .expect("insert answer citations");
    client
        .batch_execute(
            "INSERT INTO rag_embedding_jobs VALUES \
             ('tenant-a', 'job-a-0', 'chunk-a-0', 'doc-a', 'test-model', 'v1', 10, 'pending', \
              0, 3, NULL, NULL, NULL, TIMESTAMP '2026-05-20 10:01:00', \
              TIMESTAMP '2026-05-20 10:00:00', TIMESTAMP '2026-05-20 10:00:00'), \
             ('tenant-b', 'job-b-0', 'chunk-b-0', 'doc-b', 'test-model', 'v1', 5, 'pending', \
              0, 3, NULL, NULL, NULL, TIMESTAMP '2026-05-20 10:31:00', \
              TIMESTAMP '2026-05-20 10:30:00', TIMESTAMP '2026-05-20 10:30:00')",
        )
        .await
        .expect("insert embedding jobs");

    let recent = client
        .simple_query(
            "SELECT document_id, version \
             FROM rag_documents \
             WHERE tenant_id = 'tenant-a' AND is_current = true AND metadata @> '{\"kind\":\"guide\"}'::jsonb \
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
             WHERE tenant_id = 'tenant-a' AND document_id = 'doc-a' AND is_current = true \
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
             WHERE tenant_id = 'tenant-a' AND is_current = true \
             ORDER BY embedding <-> VECTOR '[1,0,0]' \
             LIMIT 1",
        )
        .await
        .expect("query nearest current embedding");
    assert_eq!(
        simple_rows(&nearest),
        vec![vec!["chunk-a-0".to_owned(), "2".to_owned()]]
    );

    let retrievals = client
        .simple_query(
            "SELECT retrieval_event_id, retrieval_mode, top_k, latency_microseconds \
             FROM rag_retrieval_events \
             WHERE tenant_id = 'tenant-a' \
             ORDER BY retrieved_at DESC",
        )
        .await
        .expect("query retrieval events");
    assert_eq!(
        simple_rows(&retrievals),
        vec![vec![
            "retr-a-0".to_owned(),
            "hybrid".to_owned(),
            "3".to_owned(),
            "1200".to_owned()
        ]]
    );

    let citations = client
        .simple_query(
            "SELECT citation_id, retrieval_event_id, answer_id, chunk_id, score \
             FROM rag_answer_citations \
             WHERE tenant_id = 'tenant-a' \
             ORDER BY citation_index",
        )
        .await
        .expect("query answer citations");
    assert_eq!(
        simple_rows(&citations),
        vec![vec![
            "cite-a-0".to_owned(),
            "retr-a-0".to_owned(),
            "answer-a".to_owned(),
            "chunk-a-0".to_owned(),
            "0.99".to_owned()
        ]]
    );

    let jobs = client
        .simple_query(
            "SELECT job_id, chunk_id, status, priority \
             FROM rag_embedding_jobs \
             WHERE tenant_id = 'tenant-a' \
             ORDER BY priority DESC, available_at ASC",
        )
        .await
        .expect("query embedding jobs");
    assert_eq!(
        simple_rows(&jobs),
        vec![vec![
            "job-a-0".to_owned(),
            "chunk-a-0".to_owned(),
            "pending".to_owned(),
            "10".to_owned()
        ]]
    );

    shutdown(running).await;
}
