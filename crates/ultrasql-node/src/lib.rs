//! Node-API bindings for UltraSQL embedded mode.

use std::sync::Mutex;

use napi::bindgen_prelude::*;
use napi_derive::napi;
use ultrasql_server::{EmbeddedDatabase, LocalQueryOutput, ServerError};

/// JavaScript-visible result column.
#[napi(object)]
#[derive(Debug)]
pub struct QueryColumn {
    /// Output column name.
    pub name: String,
    /// PostgreSQL-compatible type OID.
    pub type_oid: u32,
}

/// JavaScript-visible query result.
#[napi(object)]
#[derive(Debug)]
pub struct QueryResult {
    /// Result columns in output order.
    pub columns: Vec<QueryColumn>,
    /// Text-format result rows. `null` represents SQL `NULL`.
    pub rows: Vec<Vec<Option<String>>>,
    /// PostgreSQL-style command tag.
    pub command_tag: String,
}

/// Native embedded UltraSQL database handle.
#[napi]
#[derive(Debug)]
pub struct Database {
    inner: Mutex<EmbeddedDatabase>,
}

#[napi]
impl Database {
    /// Open an embedded database.
    ///
    /// Omit `target` or pass `:memory:` for an in-memory database. Any other
    /// string is treated as a WAL-backed data directory.
    #[napi(constructor)]
    pub fn new(target: Option<String>) -> Result<Self> {
        let db = match target.as_deref().unwrap_or(":memory:") {
            ":memory:" => EmbeddedDatabase::open_memory(),
            path => EmbeddedDatabase::open_path(path).map_err(to_napi_error)?,
        };
        Ok(Self {
            inner: Mutex::new(db),
        })
    }

    /// Execute one SQL statement and return a materialised result.
    #[napi]
    pub fn execute(&self, sql: String) -> Result<QueryResult> {
        let mut db = self
            .inner
            .lock()
            .map_err(|_| Error::from_reason("embedded database mutex poisoned"))?;
        db.execute(&sql).map(to_query_result).map_err(to_napi_error)
    }
}

fn to_query_result(output: LocalQueryOutput) -> QueryResult {
    QueryResult {
        columns: output
            .columns
            .into_iter()
            .map(|column| QueryColumn {
                name: column.name,
                type_oid: column.type_oid,
            })
            .collect(),
        rows: output.rows,
        command_tag: output.command_tag,
    }
}

fn to_napi_error(err: ServerError) -> Error {
    Error::from_reason(err.to_string())
}
