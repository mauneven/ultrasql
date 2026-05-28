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
    /// Wire type OID.
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
        let db = open_embedded(target.as_deref()).map_err(to_napi_error)?;
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

fn open_embedded(target: Option<&str>) -> std::result::Result<EmbeddedDatabase, ServerError> {
    match target.unwrap_or(":memory:") {
        ":memory:" => Ok(EmbeddedDatabase::open_memory()),
        path => EmbeddedDatabase::open_path(path),
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

#[cfg(test)]
mod napi_test_stubs {
    use napi::sys::{napi_env, napi_ref, napi_status};

    #[unsafe(no_mangle)]
    pub(crate) unsafe extern "C" fn napi_delete_reference(
        _env: napi_env,
        _ref: napi_ref,
    ) -> napi_status {
        napi::sys::Status::napi_ok
    }

    #[unsafe(no_mangle)]
    pub(crate) unsafe extern "C" fn napi_reference_unref(
        _env: napi_env,
        _ref: napi_ref,
        result: *mut u32,
    ) -> napi_status {
        if !result.is_null() {
            // SAFETY: the Node-API contract makes `result` an optional out pointer.
            unsafe { *result = 0 };
        }
        napi::sys::Status::napi_ok
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ultrasql_server::LocalResultColumn;

    #[test]
    fn query_result_conversion_preserves_shape() {
        let output = LocalQueryOutput {
            columns: vec![
                LocalResultColumn {
                    name: "id".to_string(),
                    type_oid: 23,
                },
                LocalResultColumn {
                    name: "name".to_string(),
                    type_oid: 25,
                },
            ],
            rows: vec![
                vec![Some("1".to_string()), Some("ada".to_string())],
                vec![Some("2".to_string()), None],
            ],
            command_tag: "SELECT 2".to_string(),
        };

        let result = to_query_result(output);

        assert_eq!(result.command_tag, "SELECT 2");
        assert_eq!(result.columns.len(), 2);
        assert_eq!(result.columns[0].name, "id");
        assert_eq!(result.columns[0].type_oid, 23);
        assert_eq!(result.columns[1].name, "name");
        assert_eq!(result.columns[1].type_oid, 25);
        assert_eq!(
            result.rows,
            vec![
                vec![Some("1".to_string()), Some("ada".to_string())],
                vec![Some("2".to_string()), None],
            ]
        );
    }

    #[test]
    fn embedded_memory_helper_executes_sql_and_returns_materialized_rows() {
        let mut db = open_embedded(None).expect("open memory database");

        db.execute("CREATE TABLE users (id INT, name TEXT)")
            .expect("create table");
        db.execute("INSERT INTO users VALUES (2, 'grace'), (1, 'ada')")
            .expect("insert rows");

        let result = to_query_result(
            db.execute("SELECT id, name FROM users ORDER BY id")
                .expect("select rows"),
        );

        assert_eq!(result.command_tag, "SELECT 2");
        assert_eq!(result.columns[0].name, "id");
        assert_eq!(result.columns[1].name, "name");
        assert_eq!(
            result.rows,
            vec![
                vec![Some("1".to_string()), Some("ada".to_string())],
                vec![Some("2".to_string()), Some("grace".to_string())],
            ]
        );
    }

    #[test]
    fn database_constructor_and_execute_use_embedded_memory() {
        let db = Database::new(Some(":memory:".to_string())).expect("open napi database");

        let create = db
            .execute("CREATE TABLE users (id INT, name TEXT)".to_string())
            .expect("create table");
        assert_eq!(create.command_tag, "CREATE TABLE");

        let insert = db
            .execute("INSERT INTO users VALUES (1, 'ada'), (2, 'grace')".to_string())
            .expect("insert rows");
        assert_eq!(insert.command_tag, "INSERT 0 2");

        let result = db
            .execute("SELECT id, name FROM users ORDER BY id".to_string())
            .expect("select rows");

        assert_eq!(result.command_tag, "SELECT 2");
        assert_eq!(result.columns[0].type_oid, 23);
        assert_eq!(result.columns[1].type_oid, 25);
        assert_eq!(
            result.rows,
            vec![
                vec![Some("1".to_string()), Some("ada".to_string())],
                vec![Some("2".to_string()), Some("grace".to_string())],
            ]
        );
    }

    #[test]
    fn server_errors_become_napi_reasons() {
        let err = to_napi_error(ServerError::UnsupportedProtocol { major: 9, minor: 9 });

        assert!(
            err.reason.contains("unsupported protocol version 9.9"),
            "unexpected napi error: {err:?}"
        );
    }
}
