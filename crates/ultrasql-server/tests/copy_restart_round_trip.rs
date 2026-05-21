//! Persistent `COPY FROM` restart coverage through the PostgreSQL wire path.

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use bytes::Bytes;
use futures::SinkExt;
use parquet::arrow::ArrowWriter;
use tokio_postgres::NoTls;
use ultrasql_server::{Server, bind_listener, serve_listener};

fn sql_string(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

fn write_copy_parquet(path: &Path, rows: &[(i64, &str, f64, bool)]) {
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int64, false),
        ArrowField::new("label", ArrowDataType::Utf8, false),
        ArrowField::new("score", ArrowDataType::Float64, false),
        ArrowField::new("active", ArrowDataType::Boolean, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|(id, _, _, _)| *id).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|(_, label, _, _)| *label)
                    .collect::<Vec<&str>>(),
            )),
            Arc::new(Float64Array::from(
                rows.iter()
                    .map(|(_, _, score, _)| *score)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(BooleanArray::from(
                rows.iter()
                    .map(|(_, _, _, active)| *active)
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("parquet record batch");
    let file = std::fs::File::create(path).expect("create parquet");
    let mut writer = ArrowWriter::try_new(file, schema, None).expect("parquet writer");
    writer.write(&batch).expect("write parquet batch");
    writer.close().expect("close parquet writer");
}

async fn start_persistent_server(
    data_dir: &Path,
) -> (
    tokio_postgres::Client,
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
) {
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("addr parses");
    let (listener, bound) = bind_listener(addr).await.expect("bind");
    let server = Arc::new(Server::init(data_dir).expect("persistent server init"));
    let server_handle = tokio::spawn(serve_listener(listener, server));

    let conn_str = format!(
        "host={host} port={port} user=tester application_name=copy_restart_test",
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

async fn select_count(client: &tokio_postgres::Client, table: &str) -> i64 {
    let rows = client
        .simple_query(&format!("SELECT COUNT(*) FROM {table}"))
        .await
        .expect("count query");
    rows.into_iter()
        .find_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => row
                .get(0)
                .map(|cell| cell.parse::<i64>().expect("count parses")),
            _ => None,
        })
        .expect("COUNT(*) returned a row")
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn copy_from_stdin_rows_survive_restart() {
    let data_dir = tempfile::TempDir::new().unwrap();

    let (client, _conn_handle, server_handle) = start_persistent_server(data_dir.path()).await;
    client
        .simple_query("CREATE TABLE copy_restart (id INT, label TEXT)")
        .await
        .expect("create table");
    let copied = copy_in_payload(
        &client,
        "COPY copy_restart (id, label) FROM STDIN WITH (FORMAT csv)",
        b"1,alpha\n2,bravo\n",
    )
    .await;
    assert_eq!(copied, 2);
    assert_eq!(select_count(&client, "copy_restart").await, 2);
    shutdown(client, server_handle).await;

    let (client, _conn_handle, server_handle) = start_persistent_server(data_dir.path()).await;
    assert_eq!(select_count(&client, "copy_restart").await, 2);
    shutdown(client, server_handle).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn copy_from_parquet_rows_survive_restart() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let parquet_dir = tempfile::TempDir::new().unwrap();
    let parquet_path = parquet_dir.path().join("import.parquet");
    write_copy_parquet(
        &parquet_path,
        &[(10, "alpha", 1.5, true), (20, "bravo", 2.5, false)],
    );

    let (client, _conn_handle, server_handle) = start_persistent_server(data_dir.path()).await;
    client
        .simple_query(
            "CREATE TABLE parquet_restart (
                id BIGINT,
                label TEXT,
                score DOUBLE,
                active BOOL
            )",
        )
        .await
        .expect("create parquet restart table");
    client
        .simple_query(&format!(
            "COPY parquet_restart FROM {}",
            sql_string(parquet_path.to_str().expect("utf8 parquet path"))
        ))
        .await
        .expect("copy parquet import");
    assert_eq!(select_count(&client, "parquet_restart").await, 2);
    shutdown(client, server_handle).await;

    let (client, _conn_handle, server_handle) = start_persistent_server(data_dir.path()).await;
    assert_eq!(select_count(&client, "parquet_restart").await, 2);
    shutdown(client, server_handle).await;
}
