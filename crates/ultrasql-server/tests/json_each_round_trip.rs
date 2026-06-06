//! Wire-level regression tests for SQL/JSON table functions.

pub mod support;

use support::{shutdown, start_sample_server};
use tokio_postgres::SimpleQueryMessage;

#[tokio::test]
async fn json_each_expands_object_to_key_value_rows() {
    let running = start_sample_server("json_each_test").await;
    let client = &running.client;

    let messages = client
        .simple_query(
            "SELECT \"key\", value FROM json_each('{\"a\":1,\"b\":\"two\"}'::jsonb) ORDER BY \"key\"",
        )
        .await
        .expect("json_each query");
    let rows: Vec<(String, String)> = messages
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some((
                row.get(0).expect("key").to_owned(),
                row.get(1).expect("value").to_owned(),
            )),
            _ => None,
        })
        .collect();

    assert_eq!(
        rows,
        vec![
            ("a".to_owned(), "1".to_owned()),
            ("b".to_owned(), "\"two\"".to_owned()),
        ]
    );

    shutdown(running).await;
}
