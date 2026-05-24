//! End-to-end basic `XML` storage and wire rendering.

mod support;

use bytes::{Bytes, BytesMut};
use futures::{SinkExt, StreamExt};
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
                ("application_name".to_owned(), "xml_type_probe".to_owned()),
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

async fn copy_in_payload(client: &tokio_postgres::Client, sql: &str, payload: &[u8]) -> u64 {
    let sink = client
        .copy_in::<_, Bytes>(sql)
        .await
        .expect("copy in starts");
    futures::pin_mut!(sink);
    sink.as_mut()
        .send(Bytes::from(payload.to_vec()))
        .await
        .expect("copy bytes sent");
    sink.finish().await.expect("copy finishes")
}

async fn collect_copy_out(stream: tokio_postgres::CopyOutStream) -> Vec<u8> {
    let mut stream = Box::pin(stream);
    let mut out = Vec::new();
    while let Some(chunk) = stream.next().await {
        out.extend_from_slice(&chunk.expect("copy chunk"));
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn xml_stores_renders_copies_rejects_invalid_and_survives_restart() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let running = start_persistent_server(data_dir.path(), "xml_round_trip").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE xml_probe (id INT, doc XML)")
        .await
        .expect("create XML table");

    client
        .batch_execute(
            "INSERT INTO xml_probe VALUES \
             (1, XML '<root attr=\"v\"><child>text</child></root>')",
        )
        .await
        .expect("insert XML typed literal");

    assert_eq!(
        simple_rows(
            client
                .simple_query(
                    "SELECT column_name, data_type \
                     FROM information_schema.columns \
                     WHERE table_name = 'xml_probe' \
                     ORDER BY ordinal_position",
                )
                .await
                .expect("information_schema columns"),
        ),
        vec![
            vec!["id".to_owned(), "integer".to_owned()],
            vec!["doc".to_owned(), "xml".to_owned()],
        ]
    );

    assert_eq!(
        simple_query_type_oids(running.bound, "SELECT doc FROM xml_probe WHERE id = 1").await,
        vec![142]
    );

    assert_eq!(
        simple_rows(
            client
                .simple_query("SELECT doc FROM xml_probe WHERE id = 1")
                .await
                .expect("select XML"),
        ),
        vec![vec![
            "<root attr=\"v\"><child>text</child></root>".to_owned()
        ]]
    );

    let copied = collect_copy_out(
        client
            .copy_out("COPY xml_probe TO STDOUT")
            .await
            .expect("copy out starts"),
    )
    .await;
    assert_eq!(copied, b"1\t<root attr=\"v\"><child>text</child></root>\n");

    assert_eq!(
        copy_in_payload(
            client,
            "COPY xml_probe FROM STDIN",
            b"2\t<root><copy/></root>\n",
        )
        .await,
        1
    );

    client
        .batch_execute("SELECT '<root>'::xml")
        .await
        .expect_err("malformed XML rejected");

    shutdown(running).await;

    let running = start_persistent_server(data_dir.path(), "xml_round_trip").await;
    assert_eq!(
        simple_query_type_oids(running.bound, "SELECT doc FROM xml_probe ORDER BY id").await,
        vec![142]
    );
    assert_eq!(
        simple_rows(
            running
                .client
                .simple_query("SELECT doc FROM xml_probe ORDER BY id")
                .await
                .expect("select XML after restart"),
        ),
        vec![
            vec!["<root attr=\"v\"><child>text</child></root>".to_owned()],
            vec!["<root><copy/></root>".to_owned()],
        ]
    );

    shutdown(running).await;
}
