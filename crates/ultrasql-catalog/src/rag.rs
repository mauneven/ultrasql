//! Canonical schemas for RAG-oriented user storage.
//!
//! These primitives are ordinary SQL tables, not hidden system catalogs. They
//! give applications a reproducible baseline for storing source documents,
//! chunks, embeddings, metadata, recency, and version state while the SQL layer
//! grows higher-level RAG helpers around them.

use std::fmt;

use ultrasql_core::{DataType, Field, MAX_VECTOR_DIMS, Schema};

/// Default table prefix for RAG primitive tables.
pub const DEFAULT_RAG_PREFIX: &str = "rag";

/// Configuration for generating RAG primitive table schemas.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RagSchemaConfig {
    /// Prefix used for table names. The default creates `rag_documents`,
    /// `rag_chunks`, and `rag_embeddings`.
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
}

/// Canonical schemas for the three RAG primitive tables.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RagPrimitiveSchemas {
    /// Source document rows.
    pub documents: Schema,
    /// Text chunk rows belonging to source documents.
    pub chunks: Schema,
    /// Vector embedding rows belonging to chunks.
    pub embeddings: Schema,
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
        })
    }
}

/// Return SQL that creates the three RAG primitive tables.
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
    ])
}

fn documents_schema() -> Result<Schema, RagSchemaError> {
    Schema::new([
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
