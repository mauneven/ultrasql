//! End-to-end `FROM generate_series(...)` tests against a real
//! `tokio-postgres` client.
//!
//! Closes the v0.5 wire-protocol gap "`FunctionScan` — kernel exists,
//! not yet wired". Parser now accepts `FROM name(args)` as a
//! `TableRef::Function`; binder lowers it into
//! `LogicalPlan::FunctionScan { name, args, schema }`; the server's
//! `pipeline::lower_function_scan` constructs the matching executor
//! operator. File-backed `read_csv(path_or_glob)` and `sniff_csv(path)` are
//! lowered through the same table-function path without creating catalog
//! tables.

use std::fs;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use parquet::arrow::ArrowWriter;
use tokio_postgres::NoTls;
use ultrasql_server::{Server, bind_listener, serve_listener};

fn sql_string(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

fn write_people_parquet(
    path: &std::path::Path,
    first_rows: &[(i64, &str, i64)],
    second_rows: &[(i64, &str, i64)],
) {
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int64, false),
        ArrowField::new("name", ArrowDataType::Utf8, false),
        ArrowField::new("score", ArrowDataType::Int64, false),
    ]));
    let file = fs::File::create(path).expect("create parquet");
    let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), None).expect("parquet writer");
    writer
        .write(&people_batch(Arc::clone(&schema), first_rows))
        .expect("write first parquet row group");
    writer.flush().expect("flush first row group");
    writer
        .write(&people_batch(schema, second_rows))
        .expect("write second parquet row group");
    writer.close().expect("close parquet");
}

fn people_batch(schema: Arc<ArrowSchema>, rows: &[(i64, &str, i64)]) -> RecordBatch {
    let ids = rows.iter().map(|(id, _, _)| *id).collect::<Vec<_>>();
    let names = rows.iter().map(|(_, name, _)| *name).collect::<Vec<&str>>();
    let scores = rows.iter().map(|(_, _, score)| *score).collect::<Vec<_>>();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(Int64Array::from(scores)),
        ],
    )
    .expect("record batch")
}

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
        "host={host} port={port} user=tester application_name=function_scan_test",
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

#[tokio::test]
async fn generate_series_ascending_emits_inclusive_range() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    let rows = client
        .query("SELECT * FROM generate_series(1, 5)", &[])
        .await
        .expect("generate_series(1, 5)");
    let values: Vec<i64> = rows.iter().map(|r| r.get::<_, i64>(0)).collect();
    assert_eq!(values, vec![1, 2, 3, 4, 5]);

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn generate_series_with_step_skips() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    let rows = client
        .query("SELECT * FROM generate_series(0, 10, 2)", &[])
        .await
        .expect("generate_series(0, 10, 2)");
    let values: Vec<i64> = rows.iter().map(|r| r.get::<_, i64>(0)).collect();
    assert_eq!(values, vec![0, 2, 4, 6, 8, 10]);

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn generate_series_descending_emits_descending() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    let rows = client
        .query("SELECT * FROM generate_series(5, 1, -1)", &[])
        .await
        .expect("generate_series(5, 1, -1)");
    let values: Vec<i64> = rows.iter().map(|r| r.get::<_, i64>(0)).collect();
    assert_eq!(values, vec![5, 4, 3, 2, 1]);

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn unnest_string_to_array_emits_text_rows() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    let rows = client
        .query(
            "SELECT * FROM unnest(string_to_array('red,green', ','))",
            &[],
        )
        .await
        .expect("unnest(string_to_array(...))");
    let values: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
    assert_eq!(values, vec!["red".to_string(), "green".to_string()]);

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn read_csv_single_file_exposes_header_columns_and_rows() {
    let dir = tempfile::tempdir().expect("tempdir");
    let csv_path = dir.path().join("people.csv");
    fs::write(&csv_path, "id,name\n1,Ada\n2,\"Grace Hopper\"\n").expect("write csv");

    let (client, _conn, server_handle) = start_server_and_connect().await;
    let sql = format!(
        "SELECT * FROM read_csv({}) ORDER BY id",
        sql_string(csv_path.to_str().expect("utf8 path"))
    );

    let rows = client.query(&sql, &[]).await.expect("read_csv file");
    assert_eq!(rows[0].columns()[0].name(), "id");
    assert_eq!(rows[0].columns()[1].name(), "name");
    let values: Vec<(String, String)> = rows
        .iter()
        .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
        .collect();
    assert_eq!(
        values,
        vec![
            ("1".to_string(), "Ada".to_string()),
            ("2".to_string(), "Grace Hopper".to_string()),
        ]
    );

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn read_csv_glob_reads_matching_files_in_stable_order() {
    let dir = tempfile::tempdir().expect("tempdir");
    let log_dir = dir.path().join("logs");
    fs::create_dir(&log_dir).expect("create logs dir");
    fs::write(log_dir.join("b.csv"), "id,name\n2,Beta\n").expect("write b csv");
    fs::write(log_dir.join("a.csv"), "id,name\n1,Alpha\n").expect("write a csv");
    fs::write(log_dir.join("ignore.txt"), "id,name\n9,Nope\n").expect("write ignored file");

    let pattern = log_dir.join("*.csv");
    let (client, _conn, server_handle) = start_server_and_connect().await;
    let sql = format!(
        "SELECT id, name, _filename, _row_number FROM read_csv({}) ORDER BY id",
        sql_string(pattern.to_str().expect("utf8 pattern"))
    );

    let rows = client.query(&sql, &[]).await.expect("read_csv glob");
    let values: Vec<(String, String, String, i64)> = rows
        .iter()
        .map(|row| {
            (
                row.get::<_, String>(0),
                row.get::<_, String>(1),
                row.get::<_, String>(2),
                row.get::<_, i64>(3),
            )
        })
        .collect();
    assert_eq!(
        values,
        vec![
            (
                "1".to_string(),
                "Alpha".to_string(),
                log_dir.join("a.csv").display().to_string(),
                1,
            ),
            (
                "2".to_string(),
                "Beta".to_string(),
                log_dir.join("b.csv").display().to_string(),
                1,
            ),
        ]
    );

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn read_csv_array_reads_files_in_argument_order_with_virtual_columns() {
    let dir = tempfile::tempdir().expect("tempdir");
    let first = dir.path().join("b.csv");
    let second = dir.path().join("a.csv");
    fs::write(&first, "id,name\n2,Beta\n3,Beta-2\n").expect("write first csv");
    fs::write(&second, "id,name\n1,Alpha\n").expect("write second csv");

    let (client, _conn, server_handle) = start_server_and_connect().await;
    let sql = format!(
        "SELECT id, name, _filename, _row_number FROM read_csv([{}, {}])",
        sql_string(first.to_str().expect("utf8 first")),
        sql_string(second.to_str().expect("utf8 second")),
    );

    let rows = client.query(&sql, &[]).await.expect("read_csv array");
    let values: Vec<(String, String, String, i64)> = rows
        .iter()
        .map(|row| {
            (
                row.get::<_, String>(0),
                row.get::<_, String>(1),
                row.get::<_, String>(2),
                row.get::<_, i64>(3),
            )
        })
        .collect();
    assert_eq!(
        values,
        vec![
            (
                "2".to_string(),
                "Beta".to_string(),
                first.display().to_string(),
                1,
            ),
            (
                "3".to_string(),
                "Beta-2".to_string(),
                first.display().to_string(),
                2,
            ),
            (
                "1".to_string(),
                "Alpha".to_string(),
                second.display().to_string(),
                1,
            ),
        ]
    );

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn read_parquet_single_file_projects_and_filters() {
    let dir = tempfile::tempdir().expect("tempdir");
    let parquet_path = dir.path().join("people.parquet");
    write_people_parquet(
        &parquet_path,
        &[(1, "ignore-low", 5), (2, "ignore-mid", 8)],
        &[(100, "Ada", 50), (101, "Grace", 60)],
    );

    let (client, _conn, server_handle) = start_server_and_connect().await;
    let sql = format!(
        "SELECT name FROM read_parquet({}) WHERE id >= 100 ORDER BY name",
        sql_string(parquet_path.to_str().expect("utf8 parquet path"))
    );

    let rows = client.query(&sql, &[]).await.expect("read_parquet file");
    assert_eq!(rows[0].columns().len(), 1);
    assert_eq!(rows[0].columns()[0].name(), "name");
    let values: Vec<String> = rows.iter().map(|row| row.get::<_, String>(0)).collect();
    assert_eq!(values, vec!["Ada".to_string(), "Grace".to_string()]);

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn read_parquet_glob_reads_matching_files_in_stable_order() {
    let dir = tempfile::tempdir().expect("tempdir");
    let data_dir = dir.path().join("parquet");
    fs::create_dir(&data_dir).expect("create parquet dir");
    write_people_parquet(
        &data_dir.join("b.parquet"),
        &[(20, "Beta", 2)],
        &[(21, "Beta-2", 3)],
    );
    write_people_parquet(&data_dir.join("a.parquet"), &[(10, "Alpha", 1)], &[]);
    fs::write(data_dir.join("ignore.txt"), "not parquet").expect("write ignored file");

    let pattern = data_dir.join("*.parquet");
    let (client, _conn, server_handle) = start_server_and_connect().await;
    let sql = format!(
        "SELECT id, name FROM read_parquet({}) ORDER BY id",
        sql_string(pattern.to_str().expect("utf8 parquet pattern"))
    );

    let rows = client.query(&sql, &[]).await.expect("read_parquet glob");
    let values: Vec<(i64, String)> = rows
        .iter()
        .map(|row| (row.get::<_, i64>(0), row.get::<_, String>(1)))
        .collect();
    assert_eq!(
        values,
        vec![
            (10, "Alpha".to_string()),
            (20, "Beta".to_string()),
            (21, "Beta-2".to_string()),
        ]
    );

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn sniff_csv_reports_dialect_types_and_prompt() {
    let dir = tempfile::tempdir().expect("tempdir");
    let csv_path = dir.path().join("metrics.csv");
    fs::write(
        &csv_path,
        "id;score;active;name\r\n1;9.5;true;Ada\r\n2;10;false;Grace\r\n",
    )
    .expect("write csv");

    let (client, _conn, server_handle) = start_server_and_connect().await;
    let sql = format!(
        "SELECT * FROM sniff_csv({})",
        sql_string(csv_path.to_str().expect("utf8 path"))
    );

    let rows = client.query(&sql, &[]).await.expect("sniff_csv file");
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row.get::<_, String>("Delimiter"), ";");
    assert_eq!(row.get::<_, String>("Quote"), "\"");
    assert_eq!(row.get::<_, String>("Escape"), "\"");
    assert_eq!(row.get::<_, String>("NewLineDelimiter"), "\\r\\n");
    assert!(row.get::<_, bool>("HasHeader"));

    let columns = row.get::<_, String>("Columns");
    assert!(columns.contains("'id': 'BIGINT'"), "{columns}");
    assert!(columns.contains("'score': 'DOUBLE'"), "{columns}");
    assert!(columns.contains("'active': 'BOOLEAN'"), "{columns}");
    assert!(columns.contains("'name': 'TEXT'"), "{columns}");

    let prompt = row.get::<_, String>("Prompt");
    assert!(prompt.starts_with("FROM read_csv("), "{prompt}");

    let rows = client
        .query(&format!("SELECT * {prompt} ORDER BY id"), &[])
        .await
        .expect("sniff_csv prompt can be queried");
    let values: Vec<(String, String, String, String)> = rows
        .iter()
        .map(|row| {
            (
                row.get::<_, String>(0),
                row.get::<_, String>(1),
                row.get::<_, String>(2),
                row.get::<_, String>(3),
            )
        })
        .collect();
    assert_eq!(
        values,
        vec![
            (
                "1".to_string(),
                "9.5".to_string(),
                "true".to_string(),
                "Ada".to_string(),
            ),
            (
                "2".to_string(),
                "10".to_string(),
                "false".to_string(),
                "Grace".to_string(),
            ),
        ]
    );

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn generate_series_unknown_function_is_unsupported() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    let err = client
        .query("SELECT * FROM bogus_srf(1, 2)", &[])
        .await
        .expect_err("bogus table function must error");
    let db_err = err.as_db_error().expect("server-sent ErrorResponse");
    assert!(
        db_err
            .message()
            .to_ascii_lowercase()
            .contains("table function")
            || db_err
                .message()
                .to_ascii_lowercase()
                .contains("not supported")
            || db_err.message().to_ascii_lowercase().contains("bogus_srf"),
        "expected table-function rejection, got {:?}",
        db_err.message()
    );

    shutdown(client, server_handle).await;
}
