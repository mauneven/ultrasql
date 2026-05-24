mod support;

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
                    "composite_type_wire_probe".to_owned(),
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
async fn composite_type_catalog_storage_and_wire_survive_restart() {
    let data_dir = tempfile::TempDir::new().unwrap();

    let running = start_persistent_server(data_dir.path(), "composite_type_test").await;
    running
        .client
        .batch_execute("CREATE TYPE postal_address AS (street TEXT, zip INT)")
        .await
        .expect("create composite type");
    running
        .client
        .batch_execute("CREATE TABLE contact_book (id INT, addr postal_address)")
        .await
        .expect("create composite table");
    running
        .client
        .batch_execute("INSERT INTO contact_book VALUES (1, '(Main,90210)'), (2, '(Side,10001)')")
        .await
        .expect("insert composite values");

    let type_rows = running
        .client
        .simple_query("SELECT oid FROM pg_type WHERE typname = 'postal_address' AND typtype = 'c'")
        .await
        .expect("pg_type row for composite");
    let composite_oid = u32::try_from(first_i64(&type_rows, 0)).expect("composite oid fits u32");
    assert!(composite_oid >= ultrasql_catalog::FIRST_USER_OID);

    let class_rows = running
        .client
        .simple_query(&format!(
            "SELECT relkind FROM pg_class WHERE oid = {composite_oid}"
        ))
        .await
        .expect("pg_class row for composite");
    assert_eq!(first_col_strings(&class_rows), vec!["c"]);

    let attribute_rows = running
        .client
        .simple_query(&format!(
            "SELECT attname FROM pg_attribute
             WHERE attrelid = {composite_oid}
             ORDER BY attnum"
        ))
        .await
        .expect("pg_attribute rows for composite");
    assert_eq!(first_col_strings(&attribute_rows), vec!["street", "zip"]);

    assert_eq!(
        simple_query_first_type_oid(running.bound, "SELECT addr FROM contact_book ORDER BY id")
            .await,
        composite_oid
    );

    let values = running
        .client
        .simple_query("SELECT addr FROM contact_book ORDER BY id")
        .await
        .expect("select composite values");
    assert_eq!(
        first_col_strings(&values),
        vec!["(Main,90210)", "(Side,10001)"]
    );
    running
        .client
        .batch_execute("INSERT INTO contact_book VALUES (3, '(OnlyStreet)')")
        .await
        .expect_err("invalid composite arity rejected");
    shutdown(running).await;

    let running = start_persistent_server(data_dir.path(), "composite_type_test").await;
    let type_rows = running
        .client
        .simple_query("SELECT oid FROM pg_type WHERE typname = 'postal_address' AND typtype = 'c'")
        .await
        .expect("pg_type row after restart");
    assert_eq!(
        u32::try_from(first_i64(&type_rows, 0)).expect("composite oid fits u32"),
        composite_oid
    );

    let attribute_rows = running
        .client
        .simple_query(&format!(
            "SELECT attname FROM pg_attribute
             WHERE attrelid = {composite_oid}
             ORDER BY attnum"
        ))
        .await
        .expect("pg_attribute rows after restart");
    assert_eq!(first_col_strings(&attribute_rows), vec!["street", "zip"]);

    assert_eq!(
        simple_query_first_type_oid(running.bound, "SELECT addr FROM contact_book ORDER BY id")
            .await,
        composite_oid
    );

    let values = running
        .client
        .simple_query("SELECT addr FROM contact_book ORDER BY id")
        .await
        .expect("select composite values after restart");
    assert_eq!(
        first_col_strings(&values),
        vec!["(Main,90210)", "(Side,10001)"]
    );
    running
        .client
        .batch_execute("INSERT INTO contact_book VALUES (3, '(OnlyStreet)')")
        .await
        .expect_err("invalid composite arity rejected after restart");
    shutdown(running).await;
}
