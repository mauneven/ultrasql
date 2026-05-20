//! End-to-end RAG helper SQL tests.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::SinkExt;
use tokio_postgres::{NoTls, SimpleQueryMessage};
use ultrasql_catalog::rag::{
    RagSchemaConfig, audit_rag_retrieved_chunk_sql, copy_rag_chunks_sql, copy_rag_documents_sql,
    create_rag_table_statements, filter_rag_documents_by_metadata_sql, insert_rag_chunk_sql,
    search_rag_answer_context_sql, search_rag_embeddings_sql,
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

    let document_payload = b"tenant_id,document_id,source_uri,title,body_hash,metadata,created_at,updated_at,indexed_at,version,is_current\n\
tenant-a,doc-a,s3://bucket/a.md,Doc A,hash-a,\"{\"\"kind\"\":\"\"guide\"\"}\",2026-05-19 10:00:00,2026-05-20 10:00:00,2026-05-20 10:05:00,2,true\n\
tenant-b,doc-b,s3://bucket/b.md,Doc B,hash-b,\"{\"\"kind\"\":\"\"guide\"\"}\",2026-05-19 10:00:00,2026-05-20 10:00:00,2026-05-20 10:05:00,2,true\n";
    let documents_inserted = copy_in_payload(
        &client,
        &copy_rag_documents_sql(&config).expect("build document COPY helper"),
        document_payload,
    )
    .await;
    assert_eq!(documents_inserted, 2);

    let chunk_sql = bind_sql(
        &insert_rag_chunk_sql(&config).expect("build chunk helper"),
        &[
            ("$1", "'tenant-a'"),
            ("$2", "'chunk-a-0'"),
            ("$3", "'doc-a'"),
            ("$4", "0"),
            ("$5", "'normal SQL stays inspectable'"),
            ("$6", "0"),
            ("$7", "4"),
            ("$8", "'{\"section\":\"body\"}'::jsonb"),
            ("$9", "TIMESTAMP '2026-05-20 10:00:01'"),
            ("$10", "TIMESTAMP '2026-05-20 10:00:02'"),
            ("$11", "7"),
            ("$12", "true"),
        ],
    );
    let inserted = client
        .simple_query(&chunk_sql)
        .await
        .expect("chunk helper inserts");
    assert_eq!(
        simple_rows(&inserted),
        vec![vec![
            "tenant-a".to_owned(),
            "chunk-a-0".to_owned(),
            "doc-a".to_owned(),
            "0".to_owned(),
            "7".to_owned(),
            "t".to_owned()
        ]]
    );

    let chunk_payload = b"tenant_id,chunk_id,document_id,chunk_index,content,token_start,token_end,metadata,created_at,updated_at,version,is_current\n\
tenant-b,chunk-b-0,doc-b,0,other tenant content,0,3,\"{\"\"section\"\":\"\"body\"\"}\",2026-05-20 10:00:01,2026-05-20 10:00:02,9,true\n";
    let chunks_inserted = copy_in_payload(
        &client,
        &copy_rag_chunks_sql(&config).expect("build chunk COPY helper"),
        chunk_payload,
    )
    .await;
    assert_eq!(chunks_inserted, 1);

    client
        .batch_execute(
            "INSERT INTO rag_embeddings VALUES \
             ('tenant-a', 'emb-a-0', 'chunk-a-0', VECTOR '[1,0,0]', 'test-model', 'v1', '{\"dims\":3}', \
              TIMESTAMP '2026-05-20 10:01:00', 7, true), \
             ('tenant-b', 'emb-b-0', 'chunk-b-0', VECTOR '[1,0,0]', 'test-model', 'v1', '{\"dims\":3}', \
              TIMESTAMP '2026-05-20 10:01:00', 9, true)",
        )
        .await
        .expect("insert embedding");

    let filtered_sql = bind_sql(
        &filter_rag_documents_by_metadata_sql(&config).expect("build metadata helper"),
        &[
            ("$1", "'tenant-a'"),
            ("$2", "'{\"kind\":\"guide\"}'::jsonb"),
            ("$3", "10"),
        ],
    );
    let filtered = client
        .simple_query(&filtered_sql)
        .await
        .expect("metadata helper filters");
    assert_eq!(
        simple_rows(&filtered),
        vec![vec![
            "tenant-a".to_owned(),
            "doc-a".to_owned(),
            "s3://bucket/a.md".to_owned(),
            "Doc A".to_owned(),
            "2".to_owned(),
            "832586400000000".to_owned()
        ]]
    );

    let search_sql = bind_sql(
        &search_rag_embeddings_sql(&config).expect("build search helper"),
        &[
            ("$1", "'tenant-a'"),
            ("$2", "VECTOR '[1,0,0]'"),
            ("$3", "5"),
        ],
    );
    let nearest = client
        .simple_query(&search_sql)
        .await
        .expect("embedding helper searches");
    assert_eq!(
        simple_rows(&nearest),
        vec![vec![
            "tenant-a".to_owned(),
            "emb-a-0".to_owned(),
            "chunk-a-0".to_owned(),
            "7".to_owned(),
            "0".to_owned()
        ]]
    );

    client
        .batch_execute(
            "INSERT INTO rag_retrieval_events VALUES \
             ('tenant-a', 'retr-a-0', 'normal SQL', VECTOR '[1,0,0]', 'hybrid', 5, \
              '{\"kind\":\"guide\"}', '{\"vector\":1}', 1200, TIMESTAMP '2026-05-20 10:02:00')",
        )
        .await
        .expect("insert retrieval event");

    let context_sql = bind_sql(
        &search_rag_answer_context_sql(&config).expect("build answer context helper"),
        &[
            ("$1", "'tenant-a'"),
            ("$2", "VECTOR '[1,0,0]'"),
            ("$3", "'{\"kind\":\"guide\"}'::jsonb"),
            ("$4", "'{\"section\":\"body\"}'::jsonb"),
            ("$5", "5"),
        ],
    );
    let context = client
        .simple_query(&context_sql)
        .await
        .expect("answer context helper searches");
    assert_eq!(
        simple_rows(&context),
        vec![vec![
            "tenant-a".to_owned(),
            "emb-a-0".to_owned(),
            "chunk-a-0".to_owned(),
            "doc-a".to_owned(),
            "0".to_owned(),
            "normal SQL stays inspectable".to_owned(),
            "7".to_owned(),
            "0".to_owned()
        ]]
    );

    let audit_sql = bind_sql(
        &audit_rag_retrieved_chunk_sql(&config).expect("build retrieved chunk audit helper"),
        &[
            ("$1", "'tenant-a'"),
            ("$2", "'retr-a-0'"),
            ("$3", "'chunk-a-0'"),
            ("$4", "'doc-a'"),
            ("$5", "0"),
            ("$6", "1.0"),
            ("$7", "0.0"),
            ("$8", "'{\"source\":\"answer_context\"}'::jsonb"),
            ("$9", "TIMESTAMP '2026-05-20 10:03:00'"),
        ],
    );
    let audited = client
        .simple_query(&audit_sql)
        .await
        .expect("audit helper inserts retrieved chunk");
    assert_eq!(
        simple_rows(&audited),
        vec![vec![
            "tenant-a".to_owned(),
            "retr-a-0".to_owned(),
            "chunk-a-0".to_owned(),
            "0".to_owned()
        ]]
    );

    let cross_tenant_audit = client
        .simple_query(
            "SELECT chunk_id \
             FROM rag_retrieved_chunks \
             WHERE tenant_id = 'tenant-b'",
        )
        .await
        .expect("query other tenant audit rows");
    assert!(
        simple_rows(&cross_tenant_audit).is_empty(),
        "tenant-a retrieval must not audit tenant-b chunks"
    );

    shutdown(client, server_handle).await;
}
