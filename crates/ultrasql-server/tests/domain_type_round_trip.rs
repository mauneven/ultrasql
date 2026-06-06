pub mod support;

use bytes::BytesMut;
use support::{shutdown, start_persistent_server};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_postgres::SimpleQueryMessage;
use ultrasql_protocol::{BackendMessage, FrontendMessage, decode_backend, encode_frontend};
use ultrasql_server::Server;

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
                    "domain_type_wire_probe".to_owned(),
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
async fn domain_catalog_constraints_coercions_and_wire_survive_restart() {
    let data_dir = tempfile::TempDir::new().unwrap();

    let running = start_persistent_server(data_dir.path(), "domain_type_test").await;
    running
        .client
        .batch_execute("CREATE DOMAIN positive_int AS INT NOT NULL CHECK (VALUE > 0)")
        .await
        .expect("create domain type");
    running
        .client
        .batch_execute("CREATE TABLE domain_probe (id INT, score positive_int)")
        .await
        .expect("create domain table");
    running
        .client
        .batch_execute("INSERT INTO domain_probe VALUES (1, 5), (2, 7)")
        .await
        .expect("insert domain values");

    let type_rows = running
        .client
        .simple_query("SELECT oid FROM pg_type WHERE typname = 'positive_int' AND typtype = 'd'")
        .await
        .expect("pg_type row for domain");
    let domain_oid = u32::try_from(first_i64(&type_rows, 0)).expect("domain oid fits u32");
    assert!(domain_oid >= ultrasql_catalog::FIRST_USER_OID);

    assert_eq!(
        simple_query_first_type_oid(running.bound, "SELECT score FROM domain_probe ORDER BY id")
            .await,
        domain_oid
    );

    let values = running
        .client
        .simple_query("SELECT score FROM domain_probe ORDER BY id")
        .await
        .expect("select domain values");
    assert_eq!(first_col_strings(&values), vec!["5", "7"]);
    running
        .client
        .batch_execute("INSERT INTO domain_probe VALUES (3, 0)")
        .await
        .expect_err("domain CHECK rejects zero");
    running
        .client
        .batch_execute("INSERT INTO domain_probe VALUES (4, NULL)")
        .await
        .expect_err("domain NOT NULL rejects null");
    shutdown(running).await;

    let running = start_persistent_server(data_dir.path(), "domain_type_test").await;
    let type_rows = running
        .client
        .simple_query("SELECT oid FROM pg_type WHERE typname = 'positive_int' AND typtype = 'd'")
        .await
        .expect("pg_type row after restart");
    assert_eq!(
        u32::try_from(first_i64(&type_rows, 0)).expect("domain oid fits u32"),
        domain_oid
    );
    assert_eq!(
        simple_query_first_type_oid(running.bound, "SELECT score FROM domain_probe ORDER BY id")
            .await,
        domain_oid
    );
    running
        .client
        .batch_execute("INSERT INTO domain_probe VALUES (3, -1)")
        .await
        .expect_err("domain CHECK rejects negative after restart");
    running
        .client
        .batch_execute("INSERT INTO domain_probe VALUES (4, NULL)")
        .await
        .expect_err("domain NOT NULL rejects null after restart");
    let values = running
        .client
        .simple_query("SELECT score FROM domain_probe ORDER BY id")
        .await
        .expect("select domain values after restart");
    assert_eq!(first_col_strings(&values), vec!["5", "7"]);
    shutdown(running).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn domain_metadata_rejects_duplicate_domain_rows_on_rebuild() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let metadata_path = data_dir.path().join("pg_domain_runtime.meta");

    let running = start_persistent_server(data_dir.path(), "domain_duplicate_meta").await;
    running
        .client
        .batch_execute("CREATE DOMAIN duplicate_domain AS INT NOT NULL CHECK (VALUE > 0)")
        .await
        .expect("create domain type");
    shutdown(running).await;

    let mut metadata = std::fs::read_to_string(&metadata_path).expect("domain metadata exists");
    let domain_line = metadata
        .lines()
        .find(|line| line.starts_with("domain\t"))
        .expect("domain metadata row")
        .to_owned();
    metadata.push_str(&domain_line);
    metadata.push('\n');
    std::fs::write(&metadata_path, metadata).expect("duplicate domain metadata");

    let err = Server::init(data_dir.path()).expect_err("duplicate domain metadata rejected");
    assert!(
        err.to_string()
            .contains("duplicate domain-runtime metadata"),
        "expected duplicate domain-runtime metadata rejection, got {err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn domain_metadata_rejects_duplicate_check_rows_on_rebuild() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let metadata_path = data_dir.path().join("pg_domain_runtime.meta");

    let running = start_persistent_server(data_dir.path(), "domain_duplicate_check").await;
    running
        .client
        .batch_execute(
            "CREATE DOMAIN duplicate_domain_check AS INT \
             CONSTRAINT duplicate_domain_check_positive CHECK (VALUE > 0)",
        )
        .await
        .expect("create domain type");
    shutdown(running).await;

    let mut metadata = std::fs::read_to_string(&metadata_path).expect("domain metadata exists");
    let check_line = metadata
        .lines()
        .find(|line| line.starts_with("check\t"))
        .expect("domain check metadata row")
        .to_owned();
    metadata.push_str(&check_line);
    metadata.push('\n');
    std::fs::write(&metadata_path, metadata).expect("duplicate domain check metadata");

    let err = Server::init(data_dir.path()).expect_err("duplicate domain check rejected");
    assert!(
        err.to_string()
            .contains("duplicate domain-runtime check metadata"),
        "expected duplicate domain-runtime check metadata rejection, got {err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn domain_metadata_rejects_orphan_check_rows_on_rebuild() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let metadata_path = data_dir.path().join("pg_domain_runtime.meta");

    let running = start_persistent_server(data_dir.path(), "domain_orphan_check").await;
    running
        .client
        .batch_execute(
            "CREATE DOMAIN orphan_domain_check AS INT \
             CONSTRAINT orphan_domain_check_positive CHECK (VALUE > 0)",
        )
        .await
        .expect("create domain type");
    shutdown(running).await;

    let mut metadata = std::fs::read_to_string(&metadata_path).expect("domain metadata exists");
    let check_line = metadata
        .lines()
        .find(|line| line.starts_with("check\t"))
        .expect("domain check metadata row");
    let mut parts = check_line.split('\t').collect::<Vec<_>>();
    parts[1] = "424242";
    metadata.push_str(&parts.join("\t"));
    metadata.push('\n');
    std::fs::write(&metadata_path, metadata).expect("orphan domain check metadata");

    let err = Server::init(data_dir.path()).expect_err("orphan domain check rejected");
    assert!(
        err.to_string()
            .contains("orphan domain-runtime check metadata"),
        "expected orphan domain-runtime check metadata rejection, got {err}"
    );
}
