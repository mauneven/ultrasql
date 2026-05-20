//! Contract tests for canonical RAG storage primitive schemas.

use ultrasql_catalog::rag::{
    RagPrimitiveSchemas, RagSchemaConfig, claim_rag_embedding_jobs_sql,
    complete_rag_embedding_job_sql, copy_rag_chunks_sql, copy_rag_documents_sql,
    create_rag_table_sql, create_rag_tenant_policy_sql, enqueue_rag_embedding_jobs_sql,
    fail_rag_embedding_job_sql, filter_rag_documents_by_metadata_sql, insert_rag_chunk_sql,
    search_rag_embeddings_sql, validate_rag_tenant_id,
};
use ultrasql_core::{DataType, Field, Schema};

fn field<'a>(schema: &'a Schema, name: &str) -> &'a Field {
    schema.find(name).map(|(_, field)| field).unwrap()
}

#[test]
fn rag_schemas_include_metadata_recency_version_and_vector_embedding() {
    let schemas = RagPrimitiveSchemas::new(3).expect("build rag schemas");

    let document_names: Vec<_> = schemas
        .documents
        .fields()
        .iter()
        .map(|field| field.name.as_str())
        .collect();
    assert_eq!(
        document_names,
        vec![
            "tenant_id",
            "document_id",
            "source_uri",
            "title",
            "body_hash",
            "metadata",
            "created_at",
            "updated_at",
            "indexed_at",
            "version",
            "is_current",
        ]
    );
    assert_eq!(
        field(&schemas.documents, "metadata").data_type,
        DataType::Jsonb
    );
    assert_eq!(
        field(&schemas.documents, "tenant_id").data_type,
        DataType::Text { max_len: None }
    );
    assert_eq!(
        field(&schemas.documents, "updated_at").data_type,
        DataType::TimestampTz
    );
    assert_eq!(
        field(&schemas.documents, "version").data_type,
        DataType::Int64
    );

    assert!(schemas.chunks.find("chunk_id").is_some());
    assert!(schemas.chunks.find("tenant_id").is_some());
    assert!(schemas.chunks.find("document_id").is_some());
    assert!(schemas.chunks.find("chunk_index").is_some());
    assert_eq!(
        field(&schemas.chunks, "metadata").data_type,
        DataType::Jsonb
    );
    assert_eq!(field(&schemas.chunks, "version").data_type, DataType::Int64);

    assert!(schemas.embeddings.find("embedding_id").is_some());
    assert!(schemas.embeddings.find("tenant_id").is_some());
    assert!(schemas.embeddings.find("chunk_id").is_some());
    assert_eq!(
        field(&schemas.embeddings, "embedding").data_type,
        DataType::Vector { dims: Some(3) }
    );
    assert_eq!(
        field(&schemas.embeddings, "embedded_at").data_type,
        DataType::TimestampTz
    );
    assert_eq!(
        field(&schemas.embeddings, "metadata").data_type,
        DataType::Jsonb
    );
    assert_eq!(
        field(&schemas.embeddings, "version").data_type,
        DataType::Int64
    );

    let retrieval_names: Vec<_> = schemas
        .retrieval_events
        .fields()
        .iter()
        .map(|field| field.name.as_str())
        .collect();
    assert_eq!(
        retrieval_names,
        vec![
            "tenant_id",
            "retrieval_event_id",
            "query_text",
            "query_embedding",
            "retrieval_mode",
            "top_k",
            "metadata_filter",
            "scoring",
            "latency_microseconds",
            "retrieved_at",
        ]
    );
    assert_eq!(
        field(&schemas.retrieval_events, "query_embedding").data_type,
        DataType::Vector { dims: Some(3) }
    );
    assert!(
        field(&schemas.retrieval_events, "query_embedding").nullable,
        "text-only retrieval events may omit query embedding"
    );
    assert_eq!(
        field(&schemas.retrieval_events, "metadata_filter").data_type,
        DataType::Jsonb
    );
    assert_eq!(
        field(&schemas.retrieval_events, "scoring").data_type,
        DataType::Jsonb
    );

    let citation_names: Vec<_> = schemas
        .answer_citations
        .fields()
        .iter()
        .map(|field| field.name.as_str())
        .collect();
    assert_eq!(
        citation_names,
        vec![
            "tenant_id",
            "citation_id",
            "retrieval_event_id",
            "answer_id",
            "document_id",
            "chunk_id",
            "citation_index",
            "score",
            "quote",
            "metadata",
            "created_at",
        ]
    );
    assert_eq!(
        field(&schemas.answer_citations, "score").data_type,
        DataType::Float64
    );
    assert_eq!(
        field(&schemas.answer_citations, "metadata").data_type,
        DataType::Jsonb
    );

    let job_names: Vec<_> = schemas
        .embedding_jobs
        .fields()
        .iter()
        .map(|field| field.name.as_str())
        .collect();
    assert_eq!(
        job_names,
        vec![
            "tenant_id",
            "job_id",
            "chunk_id",
            "document_id",
            "model",
            "model_version",
            "priority",
            "status",
            "attempt_count",
            "max_attempts",
            "last_error",
            "locked_by",
            "locked_at",
            "available_at",
            "created_at",
            "updated_at",
        ]
    );
    assert_eq!(
        field(&schemas.embedding_jobs, "status").data_type,
        DataType::Text { max_len: None }
    );
    assert_eq!(
        field(&schemas.embedding_jobs, "priority").data_type,
        DataType::Int32
    );
    assert!(
        field(&schemas.embedding_jobs, "locked_by").nullable,
        "unclaimed jobs have no worker lock"
    );
}

#[test]
fn rag_table_sql_uses_prefix_and_dimension() {
    let config = RagSchemaConfig {
        prefix: "tenant_a".to_owned(),
        embedding_dims: 384,
    };

    let sql = create_rag_table_sql(&config).expect("build rag DDL");

    assert!(sql.contains("CREATE TABLE IF NOT EXISTS tenant_a_documents"));
    assert!(sql.contains("CREATE TABLE IF NOT EXISTS tenant_a_chunks"));
    assert!(sql.contains("CREATE TABLE IF NOT EXISTS tenant_a_embeddings"));
    assert!(sql.contains("CREATE TABLE IF NOT EXISTS tenant_a_retrieval_events"));
    assert!(sql.contains("CREATE TABLE IF NOT EXISTS tenant_a_answer_citations"));
    assert!(sql.contains("CREATE TABLE IF NOT EXISTS tenant_a_embedding_jobs"));
    assert!(sql.contains("tenant_id TEXT NOT NULL"));
    assert!(sql.contains("embedding VECTOR(384) NOT NULL"));
    assert!(sql.contains("query_embedding VECTOR(384)"));
    assert!(sql.contains("score FLOAT8 NOT NULL"));
    assert!(sql.contains(
        "retrieval_event_id TEXT NOT NULL REFERENCES tenant_a_retrieval_events(retrieval_event_id)"
    ));
    assert!(sql.contains("metadata JSONB NOT NULL"));
    assert!(sql.contains("updated_at TIMESTAMPTZ NOT NULL"));
    assert!(sql.contains("version BIGINT NOT NULL"));
    assert!(sql.contains("is_current BOOL NOT NULL"));
    assert!(sql.contains("job_id TEXT PRIMARY KEY"));
    assert!(sql.contains("chunk_id TEXT NOT NULL REFERENCES tenant_a_chunks(chunk_id)"));
    assert!(sql.contains("status TEXT NOT NULL"));
    assert!(sql.contains("available_at TIMESTAMPTZ NOT NULL"));
}

#[test]
fn rag_helper_sql_is_plain_visible_sql() {
    let config = RagSchemaConfig {
        prefix: "tenant_a".to_owned(),
        embedding_dims: 384,
    };

    let chunk_insert = insert_rag_chunk_sql(&config).expect("build chunk insert SQL");
    assert!(chunk_insert.starts_with("INSERT INTO tenant_a_chunks"));
    assert!(chunk_insert.contains("tenant_id, chunk_id, document_id, chunk_index, content"));
    assert!(chunk_insert.contains("metadata, created_at, updated_at, version, is_current"));
    assert!(chunk_insert.contains("VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)"));
    assert!(
        chunk_insert.contains("RETURNING tenant_id, chunk_id, document_id, chunk_index, version")
    );

    let embedding_search = search_rag_embeddings_sql(&config).expect("build embedding search SQL");
    assert!(embedding_search.starts_with("SELECT tenant_id, embedding_id, chunk_id, version"));
    assert!(embedding_search.contains("embedding <-> $2 AS distance"));
    assert!(embedding_search.contains("FROM tenant_a_embeddings"));
    assert!(embedding_search.contains("WHERE tenant_id = $1 AND is_current = true"));
    assert!(embedding_search.contains("ORDER BY embedding <-> $2"));
    assert!(embedding_search.ends_with("LIMIT $3"));

    let metadata_filter =
        filter_rag_documents_by_metadata_sql(&config).expect("build metadata filter SQL");
    assert!(metadata_filter.starts_with("SELECT tenant_id, document_id, source_uri, title"));
    assert!(metadata_filter.contains("FROM tenant_a_documents"));
    assert!(
        metadata_filter.contains("WHERE tenant_id = $1 AND is_current = true AND metadata @> $2")
    );
    assert!(metadata_filter.contains("ORDER BY updated_at DESC"));
    assert!(metadata_filter.ends_with("LIMIT $3"));

    let copy_documents = copy_rag_documents_sql(&config).expect("build document COPY SQL");
    assert!(copy_documents.starts_with("COPY tenant_a_documents ("));
    assert!(copy_documents.contains("tenant_id, document_id, source_uri, title, body_hash"));
    assert!(copy_documents.contains("FROM STDIN WITH (FORMAT CSV, HEADER true)"));

    let copy_chunks = copy_rag_chunks_sql(&config).expect("build chunk COPY SQL");
    assert!(copy_chunks.starts_with("COPY tenant_a_chunks ("));
    assert!(copy_chunks.contains("tenant_id, chunk_id, document_id, chunk_index, content"));
    assert!(copy_chunks.contains("FROM STDIN WITH (FORMAT CSV, HEADER true)"));

    let enqueue_jobs = enqueue_rag_embedding_jobs_sql(&config).expect("build enqueue SQL");
    assert!(enqueue_jobs.starts_with("INSERT INTO tenant_a_embedding_jobs"));
    assert!(enqueue_jobs.contains("SELECT c.tenant_id"));
    assert!(enqueue_jobs.contains("FROM tenant_a_chunks c"));
    assert!(enqueue_jobs.contains("WHERE c.tenant_id = $1 AND c.is_current = true"));
    assert!(enqueue_jobs.contains("RETURNING tenant_id, job_id, chunk_id, status"));

    let claim_jobs = claim_rag_embedding_jobs_sql(&config).expect("build claim SQL");
    assert!(claim_jobs.starts_with("WITH claimable AS ("));
    assert!(claim_jobs.contains("UPDATE tenant_a_embedding_jobs"));
    assert!(claim_jobs.contains("status = 'running'"));
    assert!(claim_jobs.contains("locked_by = $2"));
    assert!(
        claim_jobs.contains("WHERE tenant_id = $1 AND status = 'pending' AND available_at <= $4")
    );
    assert!(claim_jobs.contains("ORDER BY priority DESC, available_at ASC, created_at ASC"));
    assert!(claim_jobs.contains("LIMIT $3"));
    assert!(claim_jobs.contains("RETURNING tenant_id, job_id, chunk_id"));

    let complete_job = complete_rag_embedding_job_sql(&config).expect("build complete SQL");
    assert!(complete_job.starts_with("UPDATE tenant_a_embedding_jobs"));
    assert!(complete_job.contains("status = 'succeeded'"));
    assert!(complete_job.contains("WHERE tenant_id = $1 AND job_id = $2 AND status = 'running'"));

    let fail_job = fail_rag_embedding_job_sql(&config).expect("build fail SQL");
    assert!(fail_job.starts_with("UPDATE tenant_a_embedding_jobs"));
    assert!(fail_job.contains("attempt_count = attempt_count + 1"));
    assert!(fail_job.contains("status = CASE"));
    assert!(fail_job.contains("last_error = $3"));

    for sql in [
        chunk_insert,
        embedding_search,
        metadata_filter,
        copy_documents,
        copy_chunks,
        enqueue_jobs,
        claim_jobs,
        complete_job,
        fail_job,
    ] {
        assert!(!sql.contains("rag_helper("));
        assert!(!sql.contains("rag_search("));
        assert!(!sql.contains("CALL "));
    }
}

#[test]
fn rag_tenant_ids_and_policy_sql_are_safe_by_default() {
    assert!(validate_rag_tenant_id("org_123-prod").is_ok());
    assert!(validate_rag_tenant_id("tenant:550e8400-e29b-41d4-a716-446655440000").is_ok());
    assert!(validate_rag_tenant_id("").is_err());
    assert!(validate_rag_tenant_id("bad tenant").is_err());
    assert!(validate_rag_tenant_id("tenant';drop").is_err());

    let config = RagSchemaConfig {
        prefix: "tenant_a".to_owned(),
        embedding_dims: 384,
    };
    let sql = create_rag_tenant_policy_sql(&config).expect("build policy SQL");

    assert!(sql.contains("ALTER TABLE tenant_a_documents ENABLE ROW LEVEL SECURITY"));
    assert!(sql.contains("CREATE POLICY tenant_a_documents_tenant_isolation"));
    assert!(sql.contains("tenant_id = current_setting('ultrasql.tenant_id', true)"));
    assert!(sql.contains("WITH CHECK (tenant_id = current_setting('ultrasql.tenant_id', true))"));
    assert!(sql.contains("ALTER TABLE tenant_a_chunks ENABLE ROW LEVEL SECURITY"));
    assert!(sql.contains("ALTER TABLE tenant_a_embeddings ENABLE ROW LEVEL SECURITY"));
    assert!(sql.contains("ALTER TABLE tenant_a_retrieval_events ENABLE ROW LEVEL SECURITY"));
    assert!(sql.contains("ALTER TABLE tenant_a_answer_citations ENABLE ROW LEVEL SECURITY"));
    assert!(sql.contains("ALTER TABLE tenant_a_embedding_jobs ENABLE ROW LEVEL SECURITY"));
}

#[test]
fn rag_tenant_security_docs_state_enforcement_boundary() {
    let docs = include_str!("../../../docs/rag-tenant-security.md");

    assert!(docs.contains("tenant_id TEXT NOT NULL"));
    assert!(docs.contains("UltraSQL does not yet enforce `CREATE POLICY`"));
    assert!(docs.contains("tenant_id = $1"));
    assert!(docs.contains("safe default"));
}
