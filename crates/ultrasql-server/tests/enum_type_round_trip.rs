pub mod support;

use bytes::BytesMut;
use support::{shutdown, start_persistent_server};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_postgres::SimpleQueryMessage;
use ultrasql_protocol::{BackendMessage, FrontendMessage, decode_backend, encode_frontend};

fn first_i64(rows: &[SimpleQueryMessage], col: usize) -> i64 {
    rows.iter()
        .find_map(|message| match message {
            SimpleQueryMessage::Row(row) => row.get(col)?.parse::<i64>().ok(),
            _ => None,
        })
        .expect("integer result row")
}

fn first_col_strings(rows: &[SimpleQueryMessage]) -> Vec<String> {
    rows.iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        })
        .collect()
}

async fn read_backend_message(stream: &mut TcpStream, buf: &mut BytesMut) -> BackendMessage {
    loop {
        if let Some(message) = decode_backend(buf).expect("backend message decodes") {
            return message;
        }
        let mut chunk = [0_u8; 8192];
        let n = stream.read(&mut chunk).await.expect("socket read");
        assert!(
            n > 0,
            "server closed connection while reading backend message"
        );
        buf.extend_from_slice(&chunk[..n]);
    }
}

async fn simple_query_first_type_oid(addr: std::net::SocketAddr, sql: &str) -> u32 {
    let mut stream = TcpStream::connect(addr).await.expect("raw wire connect");
    let mut out = BytesMut::new();
    encode_frontend(
        &FrontendMessage::StartupMessage {
            protocol_major: 3,
            protocol_minor: 0,
            params: vec![
                ("user".to_owned(), "tester".to_owned()),
                (
                    "application_name".to_owned(),
                    "enum_type_wire_probe".to_owned(),
                ),
            ],
        },
        &mut out,
    );
    stream.write_all(&out).await.expect("write startup");

    let mut input = BytesMut::new();
    loop {
        if matches!(
            read_backend_message(&mut stream, &mut input).await,
            BackendMessage::ReadyForQuery { .. }
        ) {
            break;
        }
    }

    out.clear();
    encode_frontend(
        &FrontendMessage::Query {
            sql: sql.to_owned(),
        },
        &mut out,
    );
    stream.write_all(&out).await.expect("write query");

    let mut type_oid = None;
    loop {
        match read_backend_message(&mut stream, &mut input).await {
            BackendMessage::RowDescription { fields } => {
                type_oid = fields.first().map(|field| field.type_oid);
            }
            BackendMessage::ErrorResponse { fields } => {
                panic!("raw query failed: {fields:?}");
            }
            BackendMessage::ReadyForQuery { .. } => break,
            _ => {}
        }
    }

    out.clear();
    encode_frontend(&FrontendMessage::Terminate, &mut out);
    stream.write_all(&out).await.expect("write terminate");
    type_oid.expect("query produced RowDescription")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn enum_type_catalog_storage_and_wire_survive_restart() {
    let data_dir = tempfile::TempDir::new().unwrap();

    let running = start_persistent_server(data_dir.path(), "enum_type_test").await;
    running
        .client
        .batch_execute("CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')")
        .await
        .expect("create enum type");
    running
        .client
        .batch_execute("CREATE TABLE enum_probe (id INT, mood mood)")
        .await
        .expect("create enum table");
    running
        .client
        .batch_execute("INSERT INTO enum_probe VALUES (1, 'sad'), (2, 'happy')")
        .await
        .expect("insert enum values");

    let type_rows = running
        .client
        .simple_query("SELECT oid FROM pg_type WHERE typname = 'mood'")
        .await
        .expect("pg_type row for enum");
    let enum_oid = u32::try_from(first_i64(&type_rows, 0)).expect("enum oid fits u32");
    assert!(enum_oid >= ultrasql_catalog::FIRST_USER_OID);

    let enum_rows = running
        .client
        .simple_query(&format!(
            "SELECT enumlabel FROM pg_enum
             WHERE enumtypid = {enum_oid}
             ORDER BY enumsortorder"
        ))
        .await
        .expect("pg_enum labels");
    assert_eq!(first_col_strings(&enum_rows), vec!["sad", "ok", "happy"]);

    assert_eq!(
        simple_query_first_type_oid(running.bound, "SELECT mood FROM enum_probe ORDER BY id").await,
        enum_oid
    );

    let values = running
        .client
        .simple_query("SELECT mood FROM enum_probe ORDER BY id")
        .await
        .expect("select enum values");
    assert_eq!(first_col_strings(&values), vec!["sad", "happy"]);
    running
        .client
        .batch_execute("INSERT INTO enum_probe VALUES (3, 'angry')")
        .await
        .expect_err("invalid enum label rejected");
    shutdown(running).await;

    let running = start_persistent_server(data_dir.path(), "enum_type_test").await;
    let type_rows = running
        .client
        .simple_query("SELECT oid FROM pg_type WHERE typname = 'mood'")
        .await
        .expect("pg_type row after restart");
    assert_eq!(
        u32::try_from(first_i64(&type_rows, 0)).expect("enum oid fits u32"),
        enum_oid
    );

    assert_eq!(
        simple_query_first_type_oid(running.bound, "SELECT mood FROM enum_probe ORDER BY id").await,
        enum_oid
    );

    let values = running
        .client
        .simple_query("SELECT mood FROM enum_probe ORDER BY id")
        .await
        .expect("select enum values after restart");
    assert_eq!(first_col_strings(&values), vec!["sad", "happy"]);
    running
        .client
        .batch_execute("INSERT INTO enum_probe VALUES (3, 'angry')")
        .await
        .expect_err("invalid enum label rejected after restart");
    shutdown(running).await;
}

#[tokio::test]
async fn enum_type_keys_distinguish_schema_dot_from_type_dot() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let running = start_persistent_server(data_dir.path(), "enum_type_quoted_dot").await;

    running
        .client
        .batch_execute(
            "CREATE SCHEMA app; \
             CREATE TYPE app.\"mood.type\" AS ENUM ('app'); \
             CREATE SCHEMA \"app.mood\"; \
             CREATE TYPE \"app.mood\".type AS ENUM ('schema'); \
             CREATE TYPE \"plain.mood\" AS ENUM ('plain'); \
             CREATE TABLE app.enum_dot_probe (id INT, mood app.\"mood.type\"); \
             CREATE TABLE \"app.mood\".enum_type_probe (id INT, mood \"app.mood\".type); \
             INSERT INTO app.enum_dot_probe VALUES (1, 'app'); \
             INSERT INTO \"app.mood\".enum_type_probe VALUES (1, 'schema')",
        )
        .await
        .expect("dotted schema and dotted type names do not collide");

    let app_values = running
        .client
        .simple_query("SELECT mood FROM app.enum_dot_probe")
        .await
        .expect("select dotted enum type value");
    assert_eq!(first_col_strings(&app_values), vec!["app"]);

    let schema_values = running
        .client
        .simple_query("SELECT mood FROM \"app.mood\".enum_type_probe")
        .await
        .expect("select dotted schema enum value");
    assert_eq!(first_col_strings(&schema_values), vec!["schema"]);

    let app_cast_values = running
        .client
        .simple_query("SELECT CAST('app' AS app.\"mood.type\")")
        .await
        .expect("cast to dotted enum type name");
    assert_eq!(first_col_strings(&app_cast_values), vec!["app"]);

    let schema_cast_values = running
        .client
        .simple_query("SELECT CAST('schema' AS \"app.mood\".type)")
        .await
        .expect("cast to dotted schema enum type");
    assert_eq!(first_col_strings(&schema_cast_values), vec!["schema"]);

    let public_cast_values = running
        .client
        .simple_query("SELECT CAST('plain' AS \"plain.mood\")")
        .await
        .expect("cast to unqualified dotted public enum type");
    assert_eq!(first_col_strings(&public_cast_values), vec!["plain"]);

    shutdown(running).await;
}
