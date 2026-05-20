//! Contract tests for canonical RAG storage primitive schemas.

use ultrasql_catalog::rag::{RagPrimitiveSchemas, RagSchemaConfig, create_rag_table_sql};
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
