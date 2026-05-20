//! Contract tests for canonical RAG storage primitive schemas.

use ultrasql_catalog::rag::{
    RagPrimitiveSchemas, RagSchemaConfig, create_rag_table_sql,
    filter_rag_documents_by_metadata_sql, insert_rag_chunk_sql, search_rag_embeddings_sql,
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
        field(&schemas.documents, "updated_at").data_type,
        DataType::TimestampTz
    );
    assert_eq!(
        field(&schemas.documents, "version").data_type,
        DataType::Int64
    );

    assert!(schemas.chunks.find("chunk_id").is_some());
    assert!(schemas.chunks.find("document_id").is_some());
    assert!(schemas.chunks.find("chunk_index").is_some());
    assert_eq!(
        field(&schemas.chunks, "metadata").data_type,
        DataType::Jsonb
    );
    assert_eq!(field(&schemas.chunks, "version").data_type, DataType::Int64);

    assert!(schemas.embeddings.find("embedding_id").is_some());
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
    assert!(sql.contains("embedding VECTOR(384) NOT NULL"));
    assert!(sql.contains("metadata JSONB NOT NULL"));
    assert!(sql.contains("updated_at TIMESTAMPTZ NOT NULL"));
    assert!(sql.contains("version BIGINT NOT NULL"));
    assert!(sql.contains("is_current BOOL NOT NULL"));
}

#[test]
fn rag_helper_sql_is_plain_visible_sql() {
    let config = RagSchemaConfig {
        prefix: "tenant_a".to_owned(),
        embedding_dims: 384,
    };

    let chunk_insert = insert_rag_chunk_sql(&config).expect("build chunk insert SQL");
    assert!(chunk_insert.starts_with("INSERT INTO tenant_a_chunks"));
    assert!(chunk_insert.contains("chunk_id, document_id, chunk_index, content"));
    assert!(chunk_insert.contains("metadata, created_at, updated_at, version, is_current"));
    assert!(chunk_insert.contains("VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)"));
    assert!(chunk_insert.contains("RETURNING chunk_id, document_id, chunk_index, version"));

    let embedding_search = search_rag_embeddings_sql(&config).expect("build embedding search SQL");
    assert!(embedding_search.starts_with("SELECT embedding_id, chunk_id, version"));
    assert!(embedding_search.contains("embedding <-> $1 AS distance"));
    assert!(embedding_search.contains("FROM tenant_a_embeddings"));
    assert!(embedding_search.contains("WHERE is_current = true"));
    assert!(embedding_search.contains("ORDER BY embedding <-> $1"));
    assert!(embedding_search.ends_with("LIMIT $2"));

    let metadata_filter =
        filter_rag_documents_by_metadata_sql(&config).expect("build metadata filter SQL");
    assert!(metadata_filter.starts_with("SELECT document_id, source_uri, title"));
    assert!(metadata_filter.contains("FROM tenant_a_documents"));
    assert!(metadata_filter.contains("WHERE is_current = true AND metadata @> $1"));
    assert!(metadata_filter.contains("ORDER BY updated_at DESC"));
    assert!(metadata_filter.ends_with("LIMIT $2"));

    for sql in [chunk_insert, embedding_search, metadata_filter] {
        assert!(!sql.contains("rag_helper("));
        assert!(!sql.contains("rag_search("));
        assert!(!sql.contains("CALL "));
    }
}
