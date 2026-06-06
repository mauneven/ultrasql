//! End-to-end JSON/JSONB storage-normalization checks.

pub mod support;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use support::{shutdown, start_sample_server};
use tokio_postgres::SimpleQueryMessage;

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

#[tokio::test]
async fn json_preserves_text_jsonb_canonicalizes_storage() {
    let running = start_sample_server("json_jsonb_normalization_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE json_norm (id INT, raw JSON, bin JSONB)")
        .await
        .expect("create table");

    client
        .batch_execute(
            "INSERT INTO json_norm VALUES \
             (1, JSON '{\"b\": 2, \"a\": 1}', JSONB '{\"b\": 2, \"a\": 1}')",
        )
        .await
        .expect("insert typed JSON values");

    let types = simple_rows(
        client
            .simple_query(
                "SELECT column_name, data_type \
                 FROM information_schema.columns \
                 WHERE table_name = 'json_norm' \
                 ORDER BY ordinal_position",
            )
            .await
            .expect("information_schema.columns"),
    );
    assert_eq!(
        types,
        vec![
            vec!["id".to_owned(), "integer".to_owned()],
            vec!["raw".to_owned(), "json".to_owned()],
            vec!["bin".to_owned(), "jsonb".to_owned()],
        ]
    );

    let rows = simple_rows(
        client
            .simple_query("SELECT raw, bin FROM json_norm WHERE id = 1")
            .await
            .expect("select typed JSON values"),
    );
    assert_eq!(
        rows,
        vec![vec![
            r#"{"b": 2, "a": 1}"#.to_owned(),
            r#"{"a":1,"b":2}"#.to_owned(),
        ]]
    );

    let copy_stream = client
        .copy_out("COPY json_norm TO STDOUT")
        .await
        .expect("copy out starts");
    let copied = collect_copy_out(copy_stream).await;
    assert_eq!(
        copied,
        br#"1	{"b": 2, "a": 1}	{"a":1,"b":2}
"#
    );

    client
        .batch_execute("CREATE TABLE json_copy_norm (id INT, raw JSON, bin JSONB)")
        .await
        .expect("create copy table");
    let rows_inserted = copy_in_payload(
        client,
        "COPY json_copy_norm FROM STDIN",
        br#"2	{"z": 9, "y": [2, 1]}	{"z":9,"y":[2,1]}
"#,
    )
    .await;
    assert_eq!(rows_inserted, 1);

    let copy_rows = simple_rows(
        client
            .simple_query("SELECT raw, bin FROM json_copy_norm WHERE id = 2")
            .await
            .expect("select copied JSON values"),
    );
    assert_eq!(
        copy_rows,
        vec![vec![
            r#"{"z": 9, "y": [2, 1]}"#.to_owned(),
            r#"{"y":[2,1],"z":9}"#.to_owned(),
        ]]
    );

    shutdown(running).await;
}
