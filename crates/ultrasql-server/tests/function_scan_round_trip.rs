//! End-to-end `FROM generate_series(...)` tests against a real
//! `tokio-postgres` client.
//!
//! Closes the v0.5 wire-protocol gap "`FunctionScan` — kernel exists,
//! not yet wired". Parser now accepts `FROM name(args)` as a
//! `TableRef::Function`; binder lowers it into
//! `LogicalPlan::FunctionScan { name, args, schema }`; the server's
//! `pipeline::lower_function_scan` constructs the matching executor
//! operator. File-backed `read_csv(path_or_glob)` is lowered through the same
//! table-function path without creating catalog tables.

use std::fs;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio_postgres::NoTls;
use ultrasql_server::{Server, bind_listener, serve_listener};

fn sql_string(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
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
    fs::write(dir.path().join("b.csv"), "id,name\n2,Beta\n").expect("write b csv");
    fs::write(dir.path().join("a.csv"), "id,name\n1,Alpha\n").expect("write a csv");
    fs::write(dir.path().join("ignore.txt"), "id,name\n9,Nope\n").expect("write ignored file");

    let pattern = dir.path().join("*.csv");
    let (client, _conn, server_handle) = start_server_and_connect().await;
    let sql = format!(
        "SELECT * FROM read_csv({}) ORDER BY id",
        sql_string(pattern.to_str().expect("utf8 pattern"))
    );

    let rows = client.query(&sql, &[]).await.expect("read_csv glob");
    let values: Vec<(String, String)> = rows
        .iter()
        .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
        .collect();
    assert_eq!(
        values,
        vec![
            ("1".to_string(), "Alpha".to_string()),
            ("2".to_string(), "Beta".to_string()),
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
