//! End-to-end `OID` / `REGCLASS` / `REGTYPE` / `PG_LSN` behavior.

pub mod support;

use bytes::BytesMut;
use support::{shutdown, start_persistent_server};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_postgres::SimpleQueryMessage;
use ultrasql_protocol::{BackendMessage, FrontendMessage, decode_backend, encode_frontend};

fn simple_rows(messages: Vec<SimpleQueryMessage>) -> Vec<Vec<String>> {
    messages
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|idx| row.get(idx).expect("column").to_owned())
                    .collect(),
            ),
            _ => None,
        })
        .collect()
}

fn first_i64(rows: Vec<SimpleQueryMessage>) -> i64 {
    rows.into_iter()
        .find_map(|message| match message {
            SimpleQueryMessage::Row(row) => row.get(0)?.parse::<i64>().ok(),
            _ => None,
        })
        .expect("integer result row")
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

async fn simple_query_type_oids(addr: std::net::SocketAddr, sql: &str) -> Vec<u32> {
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
                    "oid_alias_type_probe".to_owned(),
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

    let mut type_oids = None;
    loop {
        match read_backend_message(&mut stream, &mut input).await {
            BackendMessage::RowDescription { fields } => {
                type_oids = Some(fields.iter().map(|field| field.type_oid).collect());
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
    type_oids.expect("query produced RowDescription")
}

#[tokio::test]
async fn regclass_literal_uses_search_path_schema() {
    let data_dir = tempfile::TempDir::new().expect("temp data dir");
    let running = start_persistent_server(data_dir.path(), "regclass_search_path").await;

    running
        .client
        .batch_execute(
            "CREATE SCHEMA app; \
             CREATE TABLE app.regclass_path_probe (id INT); \
             SET search_path TO app, public",
        )
        .await
        .expect("create app regclass probe");

    let table_oid = first_i64(
        running
            .client
            .simple_query("SELECT oid FROM pg_class WHERE relname = 'regclass_path_probe'")
            .await
            .expect("pg_class app table oid"),
    );
    let regclass_oid = first_i64(
        running
            .client
            .simple_query("SELECT 'regclass_path_probe'::regclass")
            .await
            .expect("regclass follows search_path"),
    );
    assert_eq!(regclass_oid, table_oid);

    shutdown(running).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oid_regclass_regtype_pg_lsn_store_cast_wire_and_restart() {
    let data_dir = tempfile::TempDir::new().unwrap();

    let running = start_persistent_server(data_dir.path(), "oid_alias_type_test").await;
    running
        .client
        .batch_execute(
            "CREATE TABLE oid_alias_probe (
                id INT,
                raw OID,
                rel REGCLASS,
                typ REGTYPE,
                wal PG_LSN
            )",
        )
        .await
        .expect("create oid alias table");

    let table_oid = first_i64(
        running
            .client
            .simple_query("SELECT oid FROM pg_class WHERE relname = 'oid_alias_probe'")
            .await
            .expect("pg_class table oid"),
    );
    let int4_oid = first_i64(
        running
            .client
            .simple_query("SELECT oid FROM pg_type WHERE typname = 'int4'")
            .await
            .expect("pg_type int4 oid"),
    );
    assert_eq!(int4_oid, 23);

    running
        .client
        .batch_execute(
            "INSERT INTO oid_alias_probe VALUES (
                1,
                4294967295::oid,
                'oid_alias_probe'::regclass,
                'int4'::regtype,
                '0/16B6C50'::pg_lsn
            )",
        )
        .await
        .expect("insert oid alias values");

    assert_eq!(
        simple_query_type_oids(
            running.bound,
            "SELECT raw, rel, typ, wal FROM oid_alias_probe WHERE id = 1",
        )
        .await,
        vec![26, 2205, 2206, 3220]
    );

    let rows = simple_rows(
        running
            .client
            .simple_query("SELECT raw, rel, typ, wal FROM oid_alias_probe WHERE id = 1")
            .await
            .expect("select oid alias values"),
    );
    assert_eq!(
        rows,
        vec![vec![
            "4294967295".to_owned(),
            table_oid.to_string(),
            int4_oid.to_string(),
            "0/016B6C50".to_owned(),
        ]]
    );

    running
        .client
        .batch_execute("SELECT '-1'::oid")
        .await
        .expect_err("negative oid rejected");
    running
        .client
        .batch_execute("SELECT 'missing_relation'::regclass")
        .await
        .expect_err("missing regclass rejected");
    running
        .client
        .batch_execute("CREATE SCHEMA app")
        .await
        .expect("create app schema for regclass qualifier guard");
    running
        .client
        .batch_execute("SELECT 'app.oid_alias_probe'::regclass")
        .await
        .expect_err("qualified missing regclass must not resolve public table");
    running
        .client
        .batch_execute("SELECT 'missing_type'::regtype")
        .await
        .expect_err("missing regtype rejected");
    running
        .client
        .batch_execute("SELECT 'not-lsn'::pg_lsn")
        .await
        .expect_err("bad pg_lsn rejected");
    shutdown(running).await;

    let running = start_persistent_server(data_dir.path(), "oid_alias_type_test").await;
    assert_eq!(
        first_i64(
            running
                .client
                .simple_query("SELECT oid FROM pg_class WHERE relname = 'oid_alias_probe'")
                .await
                .expect("pg_class table oid after restart"),
        ),
        table_oid
    );
    assert_eq!(
        simple_query_type_oids(
            running.bound,
            "SELECT raw, rel, typ, wal FROM oid_alias_probe WHERE id = 1",
        )
        .await,
        vec![26, 2205, 2206, 3220]
    );
    let rows = simple_rows(
        running
            .client
            .simple_query("SELECT raw, rel, typ, wal FROM oid_alias_probe WHERE id = 1")
            .await
            .expect("select oid alias values after restart"),
    );
    assert_eq!(
        rows,
        vec![vec![
            "4294967295".to_owned(),
            table_oid.to_string(),
            int4_oid.to_string(),
            "0/016B6C50".to_owned(),
        ]]
    );
    shutdown(running).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_type_probe_matches_psycopg_typeinfo_shape() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let running = start_persistent_server(data_dir.path(), "pg_type_typeinfo_probe").await;

    let known = simple_rows(
        running
            .client
            .simple_query(
                "SELECT
                    typname AS name,
                    oid,
                    typarray AS array_oid,
                    oid::regtype::text AS regtype,
                    typdelim AS delimiter
                FROM pg_type t
                WHERE t.oid = to_regtype('int4')
                ORDER BY t.oid",
            )
            .await
            .expect("psycopg typeinfo probe for built-in type"),
    );
    assert_eq!(
        known,
        vec![vec![
            "int4".to_owned(),
            "23".to_owned(),
            "1007".to_owned(),
            "integer".to_owned(),
            ",".to_owned(),
        ]]
    );

    let missing = simple_rows(
        running
            .client
            .simple_query(
                "SELECT
                    typname AS name,
                    oid,
                    typarray AS array_oid,
                    oid::regtype::text AS regtype,
                    typdelim AS delimiter
                FROM pg_type t
                WHERE t.oid = to_regtype('hstore')
                ORDER BY t.oid",
            )
            .await
            .expect("psycopg typeinfo probe for absent extension type"),
    );
    assert!(missing.is_empty());

    shutdown(running).await;
}
