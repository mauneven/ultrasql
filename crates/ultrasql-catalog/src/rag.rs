//! Canonical schemas for RAG-oriented user storage.
//!
//! These primitives are ordinary SQL tables, not hidden system catalogs. They
//! give applications a reproducible baseline for storing source documents,
//! chunks, embeddings, retrieval events, answer citations, metadata, recency,
//! and version state while the SQL layer grows higher-level RAG helpers around
//! them.

use std::fmt;

use ultrasql_core::{DataType, Field, MAX_VECTOR_DIMS, Schema};

/// Default table prefix for RAG primitive tables.
pub const DEFAULT_RAG_PREFIX: &str = "rag";
/// Session setting used by generated tenant row-policy SQL.
pub const DEFAULT_RAG_TENANT_SETTING: &str = "ultrasql.tenant_id";

/// Configuration for generating RAG primitive table schemas.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RagSchemaConfig {
    /// Prefix used for table names. The default creates `rag_documents`,
    /// `rag_chunks`, `rag_embeddings`, `rag_retrieval_events`, and
    /// `rag_answer_citations`.
    pub prefix: String,
    /// Vector dimension for the embeddings table.
    pub embedding_dims: u32,
}

impl Default for RagSchemaConfig {
    fn default() -> Self {
        Self {
            prefix: DEFAULT_RAG_PREFIX.to_owned(),
            embedding_dims: 1536,
        }
    }
}

/// Concrete table names derived from a [`RagSchemaConfig`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RagTableNames {
    /// Documents table name.
    pub documents: String,
    /// Chunks table name.
    pub chunks: String,
    /// Embeddings table name.
    pub embeddings: String,
    /// Retrieval-events table name.
    pub retrieval_events: String,
    /// Answer-citations table name.
    pub answer_citations: String,
}

/// Canonical schemas for the five RAG primitive tables.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RagPrimitiveSchemas {
    /// Source document rows.
    pub documents: Schema,
    /// Text chunk rows belonging to source documents.
    pub chunks: Schema,
    /// Vector embedding rows belonging to chunks.
    pub embeddings: Schema,
    /// Retrieval events for later auditing and feedback loops.
    pub retrieval_events: Schema,
    /// Answer citations linking generated answers to source chunks.
    pub answer_citations: Schema,
}

/// Error returned for invalid RAG schema configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RagSchemaError {
    message: String,
}

impl RagSchemaError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for RagSchemaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for RagSchemaError {}

impl RagSchemaConfig {
    /// Validate the config and derive concrete table names.
    pub fn table_names(&self) -> Result<RagTableNames, RagSchemaError> {
        validate_identifier(&self.prefix)?;
        validate_dims(self.embedding_dims)?;
        Ok(RagTableNames {
            documents: format!("{}_documents", self.prefix),
            chunks: format!("{}_chunks", self.prefix),
            embeddings: format!("{}_embeddings", self.prefix),
            retrieval_events: format!("{}_retrieval_events", self.prefix),
            answer_citations: format!("{}_answer_citations", self.prefix),
        })
    }
}

impl RagPrimitiveSchemas {
    /// Build canonical RAG primitive schemas for `embedding_dims`.
    pub fn new(embedding_dims: u32) -> Result<Self, RagSchemaError> {
        validate_dims(embedding_dims)?;
        Ok(Self {
            documents: documents_schema()?,
            chunks: chunks_schema()?,
            embeddings: embeddings_schema(embedding_dims)?,
            retrieval_events: retrieval_events_schema(embedding_dims)?,
            answer_citations: answer_citations_schema()?,
        })
    }
}

/// Return SQL that creates the five RAG primitive tables.
///
/// The generated DDL is deliberately plain PostgreSQL-compatible SQL so tests
/// can execute it through the normal wire path. IDs are `TEXT` today because
/// UltraSQL's current B-tree key encoder supports text uniqueness and foreign
/// keys, while UUID B-tree keys remain a separate index-key slice.
pub fn create_rag_table_sql(config: &RagSchemaConfig) -> Result<String, RagSchemaError> {
    Ok(create_rag_table_statements(config)?.join(";\n") + ";")
}

/// Return individual SQL statements for creating RAG primitive tables.
///
/// Use this helper with clients that submit one statement per query.
pub fn create_rag_table_statements(
    config: &RagSchemaConfig,
) -> Result<Vec<String>, RagSchemaError> {
    let names = config.table_names()?;
    let dims = config.embedding_dims;
    Ok(vec![
        format!(
            "\
CREATE TABLE IF NOT EXISTS {documents} (\
tenant_id TEXT NOT NULL CHECK (tenant_id <> ''), \
document_id TEXT PRIMARY KEY, \
source_uri TEXT NOT NULL, \
title TEXT, \
body_hash TEXT NOT NULL, \
metadata JSONB NOT NULL, \
created_at TIMESTAMPTZ NOT NULL, \
updated_at TIMESTAMPTZ NOT NULL, \
indexed_at TIMESTAMPTZ NOT NULL, \
version BIGINT NOT NULL, \
is_current BOOL NOT NULL\
)",
            documents = names.documents
        ),
        format!(
            "\
CREATE TABLE IF NOT EXISTS {chunks} (\
tenant_id TEXT NOT NULL CHECK (tenant_id <> ''), \
chunk_id TEXT PRIMARY KEY, \
document_id TEXT NOT NULL REFERENCES {documents}(document_id), \
chunk_index INTEGER NOT NULL, \
content TEXT NOT NULL, \
token_start INTEGER NOT NULL, \
token_end INTEGER NOT NULL, \
metadata JSONB NOT NULL, \
created_at TIMESTAMPTZ NOT NULL, \
updated_at TIMESTAMPTZ NOT NULL, \
version BIGINT NOT NULL, \
is_current BOOL NOT NULL\
)",
            chunks = names.chunks,
            documents = names.documents
        ),
        format!(
            "\
CREATE TABLE IF NOT EXISTS {embeddings} (\
tenant_id TEXT NOT NULL CHECK (tenant_id <> ''), \
embedding_id TEXT PRIMARY KEY, \
chunk_id TEXT NOT NULL REFERENCES {chunks}(chunk_id), \
embedding VECTOR({dims}) NOT NULL, \
model TEXT NOT NULL, \
model_version TEXT NOT NULL, \
metadata JSONB NOT NULL, \
embedded_at TIMESTAMPTZ NOT NULL, \
version BIGINT NOT NULL, \
is_current BOOL NOT NULL\
)",
            embeddings = names.embeddings,
            chunks = names.chunks,
            dims = dims
        ),
        format!(
            "\
CREATE TABLE IF NOT EXISTS {retrieval_events} (\
tenant_id TEXT NOT NULL CHECK (tenant_id <> ''), \
retrieval_event_id TEXT PRIMARY KEY, \
query_text TEXT NOT NULL, \
query_embedding VECTOR({dims}), \
retrieval_mode TEXT NOT NULL, \
top_k INTEGER NOT NULL, \
metadata_filter JSONB NOT NULL, \
scoring JSONB NOT NULL, \
latency_microseconds BIGINT NOT NULL, \
retrieved_at TIMESTAMPTZ NOT NULL\
)",
            retrieval_events = names.retrieval_events,
            dims = dims
        ),
        format!(
            "\
CREATE TABLE IF NOT EXISTS {answer_citations} (\
tenant_id TEXT NOT NULL CHECK (tenant_id <> ''), \
citation_id TEXT PRIMARY KEY, \
retrieval_event_id TEXT NOT NULL REFERENCES {retrieval_events}(retrieval_event_id), \
answer_id TEXT NOT NULL, \
document_id TEXT NOT NULL REFERENCES {documents}(document_id), \
chunk_id TEXT NOT NULL REFERENCES {chunks}(chunk_id), \
citation_index INTEGER NOT NULL, \
score FLOAT8 NOT NULL, \
quote TEXT, \
metadata JSONB NOT NULL, \
created_at TIMESTAMPTZ NOT NULL\
)",
            answer_citations = names.answer_citations,
            retrieval_events = names.retrieval_events,
            documents = names.documents,
            chunks = names.chunks
        ),
    ])
}

/// Return plain SQL for inserting one tenant-scoped chunk row.
///
/// The statement is intentionally a visible `INSERT` over the canonical
/// chunks table. Parameters map one-for-one to the public columns:
/// `tenant_id`, `chunk_id`, `document_id`, `chunk_index`, `content`,
/// `token_start`, `token_end`, `metadata`, `created_at`, `updated_at`,
/// `version`, `is_current`.
pub fn insert_rag_chunk_sql(config: &RagSchemaConfig) -> Result<String, RagSchemaError> {
    let names = config.table_names()?;
    Ok(format!(
        "\
INSERT INTO {chunks} (\
tenant_id, chunk_id, document_id, chunk_index, content, token_start, token_end, \
metadata, created_at, updated_at, version, is_current\
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) \
RETURNING tenant_id, chunk_id, document_id, chunk_index, version, is_current",
        chunks = names.chunks
    ))
}

/// Return plain SQL for exact current-embedding search inside one tenant.
///
/// The distance expression is selected and repeated in `ORDER BY` so callers
/// can inspect the exact operator used for correctness checks.
pub fn search_rag_embeddings_sql(config: &RagSchemaConfig) -> Result<String, RagSchemaError> {
    let names = config.table_names()?;
    Ok(format!(
        "\
SELECT tenant_id, embedding_id, chunk_id, version, embedding <-> $2 AS distance \
FROM {embeddings} \
WHERE tenant_id = $1 AND is_current = true \
ORDER BY embedding <-> $2 \
LIMIT $3",
        embeddings = names.embeddings
    ))
}

/// Return plain SQL for filtering current documents by JSONB metadata in one tenant.
///
/// The helper is a transparent `metadata @> $2` predicate over the documents
/// table with recency ordering. It does not expand, rank, or rewrite results.
pub fn filter_rag_documents_by_metadata_sql(
    config: &RagSchemaConfig,
) -> Result<String, RagSchemaError> {
    let names = config.table_names()?;
    Ok(format!(
        "\
SELECT tenant_id, document_id, source_uri, title, version, updated_at \
FROM {documents} \
WHERE tenant_id = $1 AND is_current = true AND metadata @> $2 \
ORDER BY updated_at DESC \
LIMIT $3",
        documents = names.documents
    ))
}

/// Return SQL statements for tenant row policies over the RAG tables.
///
/// UltraSQL does not execute `CREATE POLICY` yet. These statements document the
/// intended PostgreSQL-compatible policy shape and can be applied once row-level
/// security lands. Until then, use tenant-scoped helper SQL and application
/// checks.
pub fn create_rag_tenant_policy_sql(config: &RagSchemaConfig) -> Result<String, RagSchemaError> {
    Ok(create_rag_tenant_policy_statements(config)?.join(";\n") + ";")
}

/// Return individual tenant row-policy statements for RAG primitive tables.
pub fn create_rag_tenant_policy_statements(
    config: &RagSchemaConfig,
) -> Result<Vec<String>, RagSchemaError> {
    let names = config.table_names()?;
    Ok(vec![
        tenant_policy_statements_for(&names.documents),
        tenant_policy_statements_for(&names.chunks),
        tenant_policy_statements_for(&names.embeddings),
        tenant_policy_statements_for(&names.retrieval_events),
        tenant_policy_statements_for(&names.answer_citations),
    ]
    .into_iter()
    .flatten()
    .collect())
}

/// Validate the recommended tenant id pattern for AI workload rows.
///
/// Tenant ids are data, not SQL identifiers, and callers must bind them as
/// parameters. This validator keeps logs, metadata, and generated examples
/// predictable by accepting only compact ASCII tenant keys.
pub fn validate_rag_tenant_id(tenant_id: &str) -> Result<(), RagSchemaError> {
    if tenant_id.is_empty() {
        return Err(RagSchemaError::new("tenant_id must not be empty"));
    }
    if tenant_id.len() > 128 {
        return Err(RagSchemaError::new("tenant_id must be at most 128 bytes"));
    }
    if !tenant_id
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.' | ':'))
    {
        return Err(RagSchemaError::new(
            "tenant_id must contain only ascii letters, digits, _, -, ., or :",
        ));
    }
    Ok(())
}

fn documents_schema() -> Result<Schema, RagSchemaError> {
    Schema::new([
        Field::required("tenant_id", DataType::Text { max_len: None }),
        Field::required("document_id", DataType::Text { max_len: None }),
        Field::required("source_uri", DataType::Text { max_len: None }),
        Field::nullable("title", DataType::Text { max_len: None }),
        Field::required("body_hash", DataType::Text { max_len: None }),
        Field::required("metadata", DataType::Jsonb),
        Field::required("created_at", DataType::TimestampTz),
        Field::required("updated_at", DataType::TimestampTz),
        Field::required("indexed_at", DataType::TimestampTz),
        Field::required("version", DataType::Int64),
        Field::required("is_current", DataType::Bool),
    ])
    .map_err(|err| RagSchemaError::new(format!("rag documents schema: {err}")))
}

fn chunks_schema() -> Result<Schema, RagSchemaError> {
    Schema::new([
        Field::required("tenant_id", DataType::Text { max_len: None }),
        Field::required("chunk_id", DataType::Text { max_len: None }),
        Field::required("document_id", DataType::Text { max_len: None }),
        Field::required("chunk_index", DataType::Int32),
        Field::required("content", DataType::Text { max_len: None }),
        Field::required("token_start", DataType::Int32),
        Field::required("token_end", DataType::Int32),
        Field::required("metadata", DataType::Jsonb),
        Field::required("created_at", DataType::TimestampTz),
        Field::required("updated_at", DataType::TimestampTz),
        Field::required("version", DataType::Int64),
        Field::required("is_current", DataType::Bool),
    ])
    .map_err(|err| RagSchemaError::new(format!("rag chunks schema: {err}")))
}

fn embeddings_schema(embedding_dims: u32) -> Result<Schema, RagSchemaError> {
    Schema::new([
        Field::required("tenant_id", DataType::Text { max_len: None }),
        Field::required("embedding_id", DataType::Text { max_len: None }),
        Field::required("chunk_id", DataType::Text { max_len: None }),
        Field::required(
            "embedding",
            DataType::Vector {
                dims: Some(embedding_dims),
            },
        ),
        Field::required("model", DataType::Text { max_len: None }),
        Field::required("model_version", DataType::Text { max_len: None }),
        Field::required("metadata", DataType::Jsonb),
        Field::required("embedded_at", DataType::TimestampTz),
        Field::required("version", DataType::Int64),
        Field::required("is_current", DataType::Bool),
    ])
    .map_err(|err| RagSchemaError::new(format!("rag embeddings schema: {err}")))
}

fn retrieval_events_schema(embedding_dims: u32) -> Result<Schema, RagSchemaError> {
    Schema::new([
        Field::required("tenant_id", DataType::Text { max_len: None }),
        Field::required("retrieval_event_id", DataType::Text { max_len: None }),
        Field::required("query_text", DataType::Text { max_len: None }),
        Field::nullable(
            "query_embedding",
            DataType::Vector {
                dims: Some(embedding_dims),
            },
        ),
        Field::required("retrieval_mode", DataType::Text { max_len: None }),
        Field::required("top_k", DataType::Int32),
        Field::required("metadata_filter", DataType::Jsonb),
        Field::required("scoring", DataType::Jsonb),
        Field::required("latency_microseconds", DataType::Int64),
        Field::required("retrieved_at", DataType::TimestampTz),
    ])
    .map_err(|err| RagSchemaError::new(format!("rag retrieval events schema: {err}")))
}

fn answer_citations_schema() -> Result<Schema, RagSchemaError> {
    Schema::new([
        Field::required("tenant_id", DataType::Text { max_len: None }),
        Field::required("citation_id", DataType::Text { max_len: None }),
        Field::required("retrieval_event_id", DataType::Text { max_len: None }),
        Field::required("answer_id", DataType::Text { max_len: None }),
        Field::required("document_id", DataType::Text { max_len: None }),
        Field::required("chunk_id", DataType::Text { max_len: None }),
        Field::required("citation_index", DataType::Int32),
        Field::required("score", DataType::Float64),
        Field::nullable("quote", DataType::Text { max_len: None }),
        Field::required("metadata", DataType::Jsonb),
        Field::required("created_at", DataType::TimestampTz),
    ])
    .map_err(|err| RagSchemaError::new(format!("rag answer citations schema: {err}")))
}

fn tenant_policy_statements_for(table: &str) -> Vec<String> {
    vec![
        format!("ALTER TABLE {table} ENABLE ROW LEVEL SECURITY"),
        format!(
            "\
CREATE POLICY {table}_tenant_isolation ON {table} \
USING (tenant_id = current_setting('{DEFAULT_RAG_TENANT_SETTING}', true)) \
WITH CHECK (tenant_id = current_setting('{DEFAULT_RAG_TENANT_SETTING}', true))"
        ),
    ]
}

fn validate_dims(dims: u32) -> Result<(), RagSchemaError> {
    if dims == 0 || dims > MAX_VECTOR_DIMS {
        return Err(RagSchemaError::new(format!(
            "embedding_dims must be in 1..={MAX_VECTOR_DIMS}, got {dims}"
        )));
    }
    Ok(())
}

fn validate_identifier(identifier: &str) -> Result<(), RagSchemaError> {
    let mut chars = identifier.chars();
    let Some(first) = chars.next() else {
        return Err(RagSchemaError::new("prefix must not be empty"));
    };
    if !(first == '_' || first.is_ascii_lowercase()) {
        return Err(RagSchemaError::new(
            "prefix must start with lowercase ascii letter or underscore",
        ));
    }
    if !chars.all(|ch| ch == '_' || ch.is_ascii_lowercase() || ch.is_ascii_digit()) {
        return Err(RagSchemaError::new(
            "prefix must contain only lowercase ascii letters, digits, or underscore",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_prefix_rejected() {
        let config = RagSchemaConfig {
            prefix: "Bad-Prefix".to_owned(),
            embedding_dims: 3,
        };

        assert!(config.table_names().is_err());
    }

    #[test]
    fn invalid_dims_rejected() {
        assert!(RagPrimitiveSchemas::new(0).is_err());
        assert!(RagPrimitiveSchemas::new(MAX_VECTOR_DIMS + 1).is_err());
    }
}
