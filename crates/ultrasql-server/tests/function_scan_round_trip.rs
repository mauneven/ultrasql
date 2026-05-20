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
use std::io::{Read, Write};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;
use std::time::Duration;

use apache_avro::{Codec, Schema as AvroSchema, Writer, types::Value as AvroValue};
use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_ipc::writer::FileWriter as ArrowFileWriter;
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use parquet::arrow::ArrowWriter;
use tokio_postgres::NoTls;
use ultrasql_server::{Server, bind_listener, serve_listener};

static S3_ENDPOINT_ENV_LOCK: Mutex<()> = Mutex::new(());

fn sql_string(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

struct EnvVarGuard<'a> {
    _lock: MutexGuard<'a, ()>,
    key: &'static str,
    old: Option<std::ffi::OsString>,
}

impl EnvVarGuard<'_> {
    fn set(key: &'static str, value: &str) -> Self {
        let guard = S3_ENDPOINT_ENV_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let old = std::env::var_os(key);
        unsafe { std::env::set_var(key, value) };
        Self {
            _lock: guard,
            key,
            old,
        }
    }
}

impl Drop for EnvVarGuard<'_> {
    fn drop(&mut self) {
        match &self.old {
            Some(value) => unsafe { std::env::set_var(self.key, value) },
            None => unsafe { std::env::remove_var(self.key) },
        }
    }
}

struct MockS3 {
    endpoint: String,
    shutdown: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
}

impl MockS3 {
    fn new(objects: Vec<(&str, Vec<u8>)>) -> Self {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind mock s3");
        listener.set_nonblocking(true).expect("mock s3 nonblocking");
        let endpoint = format!("http://{}", listener.local_addr().expect("mock addr"));
        let shutdown = Arc::new(AtomicBool::new(false));
        let thread_shutdown = Arc::clone(&shutdown);
        let objects = objects
            .into_iter()
            .map(|(path, body)| (path.to_owned(), body))
            .collect::<std::collections::BTreeMap<_, _>>();
        let handle = thread::spawn(move || {
            while !thread_shutdown.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((mut stream, _addr)) => handle_mock_s3_stream(&mut stream, &objects),
                    Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(_err) => break,
                }
            }
        });
        Self {
            endpoint,
            shutdown,
            handle: Some(handle),
        }
    }
}

impl Drop for MockS3 {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        let _ = std::net::TcpStream::connect(
            self.endpoint
                .strip_prefix("http://")
                .expect("mock endpoint prefix"),
        );
        if let Some(handle) = self.handle.take() {
            handle.join().expect("mock s3 thread joins");
        }
    }
}

fn handle_mock_s3_stream(
    stream: &mut std::net::TcpStream,
    objects: &std::collections::BTreeMap<String, Vec<u8>>,
) {
    let mut buf = [0_u8; 4096];
    let Ok(n) = stream.read(&mut buf) else {
        return;
    };
    let request = String::from_utf8_lossy(&buf[..n]);
    let Some(target) = request
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
    else {
        return;
    };
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    if query.contains("list-type=2") {
        let prefix = query_param(query, "prefix").unwrap_or_default();
        let bucket = path.trim_start_matches('/');
        let path_prefix = format!("/{bucket}/{prefix}");
        let mut body = String::from(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult><IsTruncated>false</IsTruncated>",
        );
        for key in objects.keys() {
            if key.starts_with(&path_prefix) {
                let object_key = key
                    .strip_prefix(&format!("/{bucket}/"))
                    .expect("bucket prefix");
                body.push_str("<Contents><Key>");
                body.push_str(object_key);
                body.push_str("</Key></Contents>");
            }
        }
        body.push_str("</ListBucketResult>");
        write_mock_response(stream, 200, "application/xml", body.as_bytes());
        return;
    }
    if let Some(body) = objects.get(path) {
        write_mock_response(stream, 200, "application/octet-stream", body);
    } else {
        write_mock_response(stream, 404, "text/plain", b"not found");
    }
}

fn query_param(query: &str, name: &str) -> Option<String> {
    query.split('&').find_map(|part| {
        let (key, value) = part.split_once('=')?;
        if key == name {
            Some(percent_decode(value))
        } else {
            None
        }
    })
}

fn percent_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hex = &value[i + 1..i + 3];
            if let Ok(decoded) = u8::from_str_radix(hex, 16) {
                out.push(decoded);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8(out).expect("mock query utf8")
}

fn write_mock_response(
    stream: &mut std::net::TcpStream,
    status: u16,
    content_type: &str,
    body: &[u8],
) {
    let reason = if status == 200 { "OK" } else { "Not Found" };
    let header = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let _ = stream.write_all(header.as_bytes());
    let _ = stream.write_all(body);
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

fn write_people_arrow(path: &std::path::Path, rows: &[(i64, &str, i64)]) {
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int64, false),
        ArrowField::new("name", ArrowDataType::Utf8, false),
        ArrowField::new("score", ArrowDataType::Int64, false),
    ]));
    let file = fs::File::create(path).expect("create arrow ipc");
    let mut writer =
        ArrowFileWriter::try_new(file, schema.as_ref()).expect("create arrow ipc writer");
    writer
        .write(&people_batch(Arc::clone(&schema), rows))
        .expect("write arrow ipc batch");
    writer.finish().expect("finish arrow ipc file");
}

fn write_people_iceberg_table(table_dir: &std::path::Path) {
    let metadata_dir = table_dir.join("metadata");
    let data_dir = table_dir.join("data");
    fs::create_dir_all(&metadata_dir).expect("create iceberg metadata dir");
    fs::create_dir_all(&data_dir).expect("create iceberg data dir");

    let data_path = data_dir.join("part-00001.parquet");
    write_people_parquet(
        &data_path,
        &[(1, "Ada", 10)],
        &[(2, "Grace", 20), (3, "Linus", 30)],
    );

    let manifest_path = metadata_dir.join("manifest-1.avro");
    write_iceberg_manifest(&manifest_path, &data_path);
    let manifest_list_path = metadata_dir.join("snap-1.avro");
    write_iceberg_manifest_list(&manifest_list_path, &manifest_path);

    fs::write(metadata_dir.join("version-hint.text"), "1\n").expect("write version hint");
    let metadata_json = serde_json::json!({
        "format-version": 2,
        "table-uuid": "00000000-0000-0000-0000-000000000001",
        "location": table_dir.to_str().expect("iceberg table utf8"),
        "last-sequence-number": 1,
        "last-updated-ms": 0,
        "last-column-id": 3,
        "schemas": [{
            "type": "struct",
            "schema-id": 0,
            "fields": [
                {"id": 1, "name": "id", "required": true, "type": "long"},
                {"id": 2, "name": "name", "required": true, "type": "string"},
                {"id": 3, "name": "score", "required": true, "type": "long"}
            ]
        }],
        "current-schema-id": 0,
        "partition-specs": [{"spec-id": 0, "fields": []}],
        "default-spec-id": 0,
        "last-partition-id": 999,
        "properties": {},
        "current-snapshot-id": 1,
        "snapshots": [{
            "snapshot-id": 1,
            "sequence-number": 1,
            "timestamp-ms": 0,
            "manifest-list": manifest_list_path.to_str().expect("manifest list utf8"),
            "summary": {"operation": "append"}
        }],
        "snapshot-log": [{"timestamp-ms": 0, "snapshot-id": 1}],
        "metadata-log": []
    });
    fs::write(
        metadata_dir.join("v1.metadata.json"),
        serde_json::to_string_pretty(&metadata_json).expect("serialize metadata json"),
    )
    .expect("write iceberg metadata json");
}

fn write_empty_iceberg_table(table_dir: &std::path::Path) {
    let metadata_dir = table_dir.join("metadata");
    fs::create_dir_all(&metadata_dir).expect("create empty iceberg metadata dir");
    fs::write(metadata_dir.join("version-hint.text"), "1\n").expect("write version hint");
    let metadata_json = serde_json::json!({
        "format-version": 2,
        "table-uuid": "00000000-0000-0000-0000-000000000002",
        "location": table_dir.to_str().expect("iceberg table utf8"),
        "last-sequence-number": 0,
        "last-updated-ms": 0,
        "last-column-id": 2,
        "schemas": [{
            "type": "struct",
            "schema-id": 0,
            "fields": [
                {"id": 1, "name": "id", "required": true, "type": "long"},
                {"id": 2, "name": "name", "required": false, "type": "string"}
            ]
        }],
        "current-schema-id": 0,
        "partition-specs": [{"spec-id": 0, "fields": []}],
        "default-spec-id": 0,
        "last-partition-id": 999,
        "properties": {},
        "snapshots": [],
        "snapshot-log": [],
        "metadata-log": []
    });
    fs::write(
        metadata_dir.join("v1.metadata.json"),
        serde_json::to_string_pretty(&metadata_json).expect("serialize metadata json"),
    )
    .expect("write empty iceberg metadata json");
}

fn write_iceberg_manifest_list(path: &std::path::Path, manifest_path: &std::path::Path) {
    let schema = AvroSchema::parse_str(
        r#"{
          "type": "record",
          "name": "manifest_file",
          "fields": [
            {"name": "manifest_path", "type": "string"},
            {"name": "manifest_length", "type": "long"},
            {"name": "partition_spec_id", "type": "int"},
            {"name": "content", "type": "int"},
            {"name": "sequence_number", "type": "long"},
            {"name": "min_sequence_number", "type": "long"},
            {"name": "added_snapshot_id", "type": "long"},
            {"name": "added_data_files_count", "type": "int"},
            {"name": "existing_data_files_count", "type": "int"},
            {"name": "deleted_data_files_count", "type": "int"},
            {"name": "added_rows_count", "type": "long"},
            {"name": "existing_rows_count", "type": "long"},
            {"name": "deleted_rows_count", "type": "long"}
          ]
        }"#,
    )
    .expect("manifest-list avro schema");
    let file = fs::File::create(path).expect("create manifest list");
    let mut writer = Writer::with_codec(&schema, file, Codec::Null);
    writer
        .append(AvroValue::Record(vec![
            (
                "manifest_path".to_string(),
                AvroValue::String(manifest_path.to_str().expect("manifest utf8").to_string()),
            ),
            ("manifest_length".to_string(), AvroValue::Long(0)),
            ("partition_spec_id".to_string(), AvroValue::Int(0)),
            ("content".to_string(), AvroValue::Int(0)),
            ("sequence_number".to_string(), AvroValue::Long(1)),
            ("min_sequence_number".to_string(), AvroValue::Long(1)),
            ("added_snapshot_id".to_string(), AvroValue::Long(1)),
            ("added_data_files_count".to_string(), AvroValue::Int(1)),
            ("existing_data_files_count".to_string(), AvroValue::Int(0)),
            ("deleted_data_files_count".to_string(), AvroValue::Int(0)),
            ("added_rows_count".to_string(), AvroValue::Long(3)),
            ("existing_rows_count".to_string(), AvroValue::Long(0)),
            ("deleted_rows_count".to_string(), AvroValue::Long(0)),
        ]))
        .expect("write manifest list row");
    writer.flush().expect("flush manifest list");
}

fn write_iceberg_manifest(path: &std::path::Path, data_path: &std::path::Path) {
    let schema = AvroSchema::parse_str(
        r#"{
          "type": "record",
          "name": "manifest_entry",
          "fields": [
            {"name": "status", "type": "int"},
            {"name": "snapshot_id", "type": ["null", "long"], "default": null},
            {"name": "sequence_number", "type": ["null", "long"], "default": null},
            {"name": "file_sequence_number", "type": ["null", "long"], "default": null},
            {
              "name": "data_file",
              "type": {
                "type": "record",
                "name": "data_file",
                "fields": [
                  {"name": "content", "type": "int"},
                  {"name": "file_path", "type": "string"},
                  {"name": "file_format", "type": "string"},
                  {"name": "record_count", "type": "long"},
                  {"name": "file_size_in_bytes", "type": "long"},
                  {"name": "partition", "type": {"type": "record", "name": "partition", "fields": []}},
                  {"name": "column_sizes", "type": ["null", {"type": "map", "values": "long"}], "default": null},
                  {"name": "value_counts", "type": ["null", {"type": "map", "values": "long"}], "default": null},
                  {"name": "null_value_counts", "type": ["null", {"type": "map", "values": "long"}], "default": null},
                  {"name": "nan_value_counts", "type": ["null", {"type": "map", "values": "long"}], "default": null},
                  {"name": "lower_bounds", "type": ["null", {"type": "map", "values": "bytes"}], "default": null},
                  {"name": "upper_bounds", "type": ["null", {"type": "map", "values": "bytes"}], "default": null},
                  {"name": "key_metadata", "type": ["null", "bytes"], "default": null},
                  {"name": "split_offsets", "type": ["null", {"type": "array", "items": "long"}], "default": null},
                  {"name": "equality_ids", "type": ["null", {"type": "array", "items": "int"}], "default": null},
                  {"name": "sort_order_id", "type": ["null", "int"], "default": null}
                ]
              }
            }
          ]
        }"#,
    )
    .expect("manifest avro schema");
    let file = fs::File::create(path).expect("create manifest");
    let mut writer = Writer::with_codec(&schema, file, Codec::Null);
    writer
        .append(AvroValue::Record(vec![
            ("status".to_string(), AvroValue::Int(1)),
            (
                "snapshot_id".to_string(),
                AvroValue::Union(1, Box::new(AvroValue::Long(1))),
            ),
            (
                "sequence_number".to_string(),
                AvroValue::Union(1, Box::new(AvroValue::Long(1))),
            ),
            (
                "file_sequence_number".to_string(),
                AvroValue::Union(1, Box::new(AvroValue::Long(1))),
            ),
            (
                "data_file".to_string(),
                AvroValue::Record(vec![
                    ("content".to_string(), AvroValue::Int(0)),
                    (
                        "file_path".to_string(),
                        AvroValue::String(data_path.to_str().expect("data utf8").to_string()),
                    ),
                    (
                        "file_format".to_string(),
                        AvroValue::String("PARQUET".to_string()),
                    ),
                    ("record_count".to_string(), AvroValue::Long(3)),
                    (
                        "file_size_in_bytes".to_string(),
                        AvroValue::Long(
                            data_path
                                .metadata()
                                .expect("data metadata")
                                .len()
                                .try_into()
                                .expect("fixture parquet file length fits i64"),
                        ),
                    ),
                    ("partition".to_string(), AvroValue::Record(vec![])),
                    (
                        "column_sizes".to_string(),
                        AvroValue::Union(0, Box::new(AvroValue::Null)),
                    ),
                    (
                        "value_counts".to_string(),
                        AvroValue::Union(0, Box::new(AvroValue::Null)),
                    ),
                    (
                        "null_value_counts".to_string(),
                        AvroValue::Union(0, Box::new(AvroValue::Null)),
                    ),
                    (
                        "nan_value_counts".to_string(),
                        AvroValue::Union(0, Box::new(AvroValue::Null)),
                    ),
                    (
                        "lower_bounds".to_string(),
                        AvroValue::Union(0, Box::new(AvroValue::Null)),
                    ),
                    (
                        "upper_bounds".to_string(),
                        AvroValue::Union(0, Box::new(AvroValue::Null)),
                    ),
                    (
                        "key_metadata".to_string(),
                        AvroValue::Union(0, Box::new(AvroValue::Null)),
                    ),
                    (
                        "split_offsets".to_string(),
                        AvroValue::Union(0, Box::new(AvroValue::Null)),
                    ),
                    (
                        "equality_ids".to_string(),
                        AvroValue::Union(0, Box::new(AvroValue::Null)),
                    ),
                    (
                        "sort_order_id".to_string(),
                        AvroValue::Union(0, Box::new(AvroValue::Null)),
                    ),
                ]),
            ),
        ]))
        .expect("write manifest row");
    writer.flush().expect("flush manifest");
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
async fn json_table_projects_declared_columns_from_jsonb_literal() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    let rows = client
        .query(
            "SELECT ord, id, name, has_score \
             FROM JSON_TABLE(\
                 jsonb '[{\"id\":2,\"name\":\"Grace\",\"score\":20},{\"id\":1,\"name\":\"Ada\"}]', \
                 '$[*]' COLUMNS (\
                     ord FOR ORDINALITY, \
                     id bigint PATH '$.id', \
                     name text, \
                     has_score boolean EXISTS PATH '$.score'\
                 )\
             ) jt \
             ORDER BY id",
            &[],
        )
        .await
        .expect("JSON_TABLE over jsonb literal");

    let values: Vec<(i64, i64, String, bool)> = rows
        .iter()
        .map(|row| {
            (
                row.get::<_, i64>(0),
                row.get::<_, i64>(1),
                row.get::<_, String>(2),
                row.get::<_, bool>(3),
            )
        })
        .collect();
    assert_eq!(
        values,
        vec![
            (2, 1, "Ada".to_string(), false),
            (1, 2, "Grace".to_string(), true),
        ]
    );

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

#[tokio::test(flavor = "current_thread")]
async fn read_csv_s3_glob_reads_matching_objects() {
    let mock = MockS3::new(vec![
        ("/lake/logs/b.csv", b"id,name\n2,Beta\n".to_vec()),
        ("/lake/logs/a.csv", b"id,name\n1,Alpha\n".to_vec()),
        ("/lake/logs/ignore.txt", b"id,name\n9,Nope\n".to_vec()),
    ]);
    let _env = EnvVarGuard::set("ULTRASQL_S3_ENDPOINT", &mock.endpoint);

    let (client, _conn, server_handle) = start_server_and_connect().await;
    let rows = client
        .query(
            "SELECT id, name, _filename, _row_number \
             FROM read_csv('s3://lake/logs/*.csv') ORDER BY id",
            &[],
        )
        .await
        .expect("read_csv s3 glob");
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
                "s3://lake/logs/a.csv".to_string(),
                1,
            ),
            (
                "2".to_string(),
                "Beta".to_string(),
                "s3://lake/logs/b.csv".to_string(),
                1,
            ),
        ]
    );

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn read_csv_rejects_mixed_local_and_object_paths() {
    let dir = tempfile::tempdir().expect("tempdir");
    let csv_path = dir.path().join("people.csv");
    fs::write(&csv_path, "id,name\n1,Ada\n").expect("write csv");

    let (client, _conn, server_handle) = start_server_and_connect().await;
    let sql = format!(
        "SELECT * FROM read_csv([{}, 's3://lake/logs/a.csv'])",
        sql_string(csv_path.to_str().expect("utf8 csv path")),
    );

    let err = client
        .query(&sql, &[])
        .await
        .expect_err("mixed read_csv paths must error");
    let db_err = err.as_db_error().expect("server-sent ErrorResponse");
    assert!(
        db_err
            .message()
            .contains("cannot mix local and object-store paths"),
        "unexpected mixed read_csv error: {}",
        db_err.message()
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
async fn read_csv_reject_path_quarantines_bad_rows() {
    let dir = tempfile::tempdir().expect("tempdir");
    let csv_path = dir.path().join("people.csv");
    let rejects_path = dir.path().join("people.rejects.csv");
    fs::write(&csv_path, "id,name\n1,Ada\n2\n3,Grace\n").expect("write csv");

    let (client, _conn, server_handle) = start_server_and_connect().await;
    let sql = format!(
        "SELECT id, name, _row_number FROM read_csv({}, {}) ORDER BY id",
        sql_string(csv_path.to_str().expect("utf8 csv path")),
        sql_string(rejects_path.to_str().expect("utf8 rejects path")),
    );

    let rows = client
        .query(&sql, &[])
        .await
        .expect("read_csv with reject path");
    let values: Vec<(String, String, i64)> = rows
        .iter()
        .map(|row| {
            (
                row.get::<_, String>(0),
                row.get::<_, String>(1),
                row.get::<_, i64>(2),
            )
        })
        .collect();
    assert_eq!(
        values,
        vec![
            ("1".to_string(), "Ada".to_string(), 1),
            ("3".to_string(), "Grace".to_string(), 3),
        ]
    );

    let reject_csv = fs::read_to_string(&rejects_path).expect("reject artifact");
    assert!(
        reject_csv.contains("filename,row_number,error,raw_row"),
        "reject artifact missing header: {reject_csv}"
    );
    assert!(
        reject_csv.contains(",2,")
            && reject_csv.contains("has 1 columns, expected 2")
            && reject_csv.contains(",2\n"),
        "reject artifact missing quarantined row: {reject_csv}"
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

#[tokio::test(flavor = "current_thread")]
async fn read_parquet_s3_glob_reads_matching_objects() {
    let dir = tempfile::tempdir().expect("tempdir");
    let first = dir.path().join("a.parquet");
    let second = dir.path().join("b.parquet");
    write_people_parquet(&first, &[(10, "Alpha", 1)], &[]);
    write_people_parquet(&second, &[(20, "Beta", 2)], &[(21, "Beta-2", 3)]);
    let mock = MockS3::new(vec![
        (
            "/lake/parquet/b.parquet",
            fs::read(&second).expect("read second parquet"),
        ),
        (
            "/lake/parquet/a.parquet",
            fs::read(&first).expect("read first parquet"),
        ),
        ("/lake/parquet/ignore.txt", b"not parquet".to_vec()),
    ]);
    let _env = EnvVarGuard::set("ULTRASQL_S3_ENDPOINT", &mock.endpoint);

    let (client, _conn, server_handle) = start_server_and_connect().await;
    let rows = client
        .query(
            "SELECT id, name FROM read_parquet('s3://lake/parquet/*.parquet') ORDER BY id",
            &[],
        )
        .await
        .expect("read_parquet s3 glob");
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
async fn read_parquet_rejects_mixed_local_and_object_paths() {
    let dir = tempfile::tempdir().expect("tempdir");
    let parquet_path = dir.path().join("people.parquet");
    write_people_parquet(&parquet_path, &[(1, "Ada", 1)], &[]);

    let (client, _conn, server_handle) = start_server_and_connect().await;
    let sql = format!(
        "SELECT * FROM read_parquet([{}, 's3://lake/parquet/a.parquet'])",
        sql_string(parquet_path.to_str().expect("utf8 parquet path")),
    );

    let err = client
        .query(&sql, &[])
        .await
        .expect_err("mixed read_parquet paths must error");
    let db_err = err.as_db_error().expect("server-sent ErrorResponse");
    assert!(
        db_err
            .message()
            .contains("cannot mix local and object-store paths"),
        "unexpected mixed read_parquet error: {}",
        db_err.message()
    );

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn read_json_single_file_infers_columns_and_rows() {
    let dir = tempfile::tempdir().expect("tempdir");
    let json_path = dir.path().join("people.json");
    fs::write(
        &json_path,
        r#"[
            {"id": 2, "name": "Grace", "active": false, "score": 20.5, "rank": null},
            {"id": 1, "name": "Ada", "active": true, "score": 10.0, "rank": 1}
        ]"#,
    )
    .expect("write json");

    let (client, _conn, server_handle) = start_server_and_connect().await;
    let sql = format!(
        "SELECT id, name, active, rank FROM read_json({}) ORDER BY id",
        sql_string(json_path.to_str().expect("utf8 json path")),
    );

    let rows = client.query(&sql, &[]).await.expect("read_json file");
    let values: Vec<(i64, String, bool, Option<i64>)> = rows
        .iter()
        .map(|row| {
            (
                row.get::<_, i64>(0),
                row.get::<_, String>(1),
                row.get::<_, bool>(2),
                row.get::<_, Option<i64>>(3),
            )
        })
        .collect();
    assert_eq!(
        values,
        vec![
            (1, "Ada".to_string(), true, Some(1)),
            (2, "Grace".to_string(), false, None),
        ]
    );

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn read_ndjson_single_file_reads_line_delimited_objects() {
    let dir = tempfile::tempdir().expect("tempdir");
    let ndjson_path = dir.path().join("people.ndjson");
    fs::write(
        &ndjson_path,
        "{\"id\":2,\"name\":\"Grace\",\"score\":20}\n{\"id\":1,\"name\":\"Ada\",\"score\":10}\n",
    )
    .expect("write ndjson");

    let (client, _conn, server_handle) = start_server_and_connect().await;
    let sql = format!(
        "SELECT id, name FROM read_ndjson({}) ORDER BY id",
        sql_string(ndjson_path.to_str().expect("utf8 ndjson path")),
    );

    let rows = client.query(&sql, &[]).await.expect("read_ndjson file");
    let values: Vec<(i64, String)> = rows
        .iter()
        .map(|row| (row.get::<_, i64>(0), row.get::<_, String>(1)))
        .collect();
    assert_eq!(
        values,
        vec![(1, "Ada".to_string()), (2, "Grace".to_string())]
    );

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn read_arrow_single_file_uses_arrow_record_batches() {
    let dir = tempfile::tempdir().expect("tempdir");
    let arrow_path = dir.path().join("people.arrow");
    write_people_arrow(
        &arrow_path,
        &[(2, "Grace", 20), (1, "Ada", 10), (3, "Linus", 30)],
    );

    let (client, _conn, server_handle) = start_server_and_connect().await;
    let sql = format!(
        "SELECT id, name FROM read_arrow({}) ORDER BY score",
        sql_string(arrow_path.to_str().expect("utf8 arrow path")),
    );

    let rows = client.query(&sql, &[]).await.expect("read_arrow file");
    let values: Vec<(i64, String)> = rows
        .iter()
        .map(|row| (row.get::<_, i64>(0), row.get::<_, String>(1)))
        .collect();
    assert_eq!(
        values,
        vec![
            (1, "Ada".to_string()),
            (2, "Grace".to_string()),
            (3, "Linus".to_string()),
        ]
    );

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn iceberg_scan_reads_current_snapshot_parquet_files() {
    let dir = tempfile::tempdir().expect("tempdir");
    let table_dir = dir.path().join("warehouse").join("people");
    write_people_iceberg_table(&table_dir);

    let (client, _conn, server_handle) = start_server_and_connect().await;
    let sql = format!(
        "SELECT id, name FROM iceberg_scan({}) WHERE score >= 20 ORDER BY id",
        sql_string(table_dir.to_str().expect("iceberg table utf8")),
    );

    let rows = client.query(&sql, &[]).await.expect("iceberg_scan table");
    let values: Vec<(i64, String)> = rows
        .iter()
        .map(|row| (row.get::<_, i64>(0), row.get::<_, String>(1)))
        .collect();
    assert_eq!(
        values,
        vec![(2, "Grace".to_string()), (3, "Linus".to_string())]
    );

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn read_iceberg_alias_reads_current_snapshot_parquet_files() {
    let dir = tempfile::tempdir().expect("tempdir");
    let table_dir = dir.path().join("warehouse").join("people_alias");
    write_people_iceberg_table(&table_dir);

    let (client, _conn, server_handle) = start_server_and_connect().await;
    let sql = format!(
        "SELECT id, name FROM read_iceberg({}) WHERE score >= 20 ORDER BY id",
        sql_string(table_dir.to_str().expect("iceberg table utf8")),
    );

    let rows = client.query(&sql, &[]).await.expect("read_iceberg table");
    let values: Vec<(i64, String)> = rows
        .iter()
        .map(|row| (row.get::<_, i64>(0), row.get::<_, String>(1)))
        .collect();
    assert_eq!(
        values,
        vec![(2, "Grace".to_string()), (3, "Linus".to_string())]
    );

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn iceberg_scan_empty_table_returns_zero_rows() {
    let dir = tempfile::tempdir().expect("tempdir");
    let table_dir = dir.path().join("warehouse").join("empty_people");
    write_empty_iceberg_table(&table_dir);

    let (client, _conn, server_handle) = start_server_and_connect().await;
    let sql = format!(
        "SELECT COUNT(*) FROM iceberg_scan({})",
        sql_string(table_dir.to_str().expect("iceberg table utf8")),
    );

    let rows = client.query(&sql, &[]).await.expect("iceberg empty table");
    assert_eq!(rows[0].get::<_, i64>(0), 0);

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
