//! Wire-level regression tests for SQL/JSON scalar behavior.

pub mod support;

use support::{shutdown, start_sample_server};
use tokio_postgres::SimpleQueryMessage;

#[tokio::test]
async fn json_build_object_returns_queryable_jsonb() {
    let running = start_sample_server("json_functions_test").await;
    let client = &running.client;

    let messages = client
        .simple_query(
            "SELECT json_build_object('id', 7, 'name', 'Ada', 'meta', '{\"kind\":\"guide\"}'::jsonb)",
        )
        .await
        .expect("json_build_object query");
    let text = messages
        .into_iter()
        .find_map(|message| match message {
            SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        })
        .expect("json_build_object row");
    let got: serde_json::Value = serde_json::from_str(&text).expect("json object");

    assert_eq!(
        got,
        serde_json::json!({
            "id": 7,
            "name": "Ada",
            "meta": {"kind": "guide"},
        })
    );

    shutdown(running).await;
}

#[tokio::test]
async fn jsonb_set_updates_nested_object_path() {
    let running = start_sample_server("json_functions_test").await;
    let client = &running.client;

    let messages = client
        .simple_query(
            "SELECT jsonb_set('{\"meta\":{\"kind\":\"draft\"}}'::jsonb, \
             '{meta,kind}', '\"guide\"'::jsonb)",
        )
        .await
        .expect("jsonb_set query");
    let text = messages
        .into_iter()
        .find_map(|message| match message {
            SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        })
        .expect("jsonb_set row");
    let got: serde_json::Value = serde_json::from_str(&text).expect("json object");

    assert_eq!(
        got,
        serde_json::json!({
            "meta": {"kind": "guide"},
        })
    );

    shutdown(running).await;
}

#[tokio::test]
async fn row_to_json_serializes_row_constructor() {
    let running = start_sample_server("json_functions_test").await;
    let client = &running.client;

    let messages = client
        .simple_query("SELECT row_to_json(ROW(7, 'Ada', '{\"kind\":\"guide\"}'::jsonb))")
        .await
        .expect("row_to_json query");
    let text = messages
        .into_iter()
        .find_map(|message| match message {
            SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        })
        .expect("row_to_json row");
    let got: serde_json::Value = serde_json::from_str(&text).expect("json object");

    assert_eq!(
        got,
        serde_json::json!({
            "f1": 7,
            "f2": "Ada",
            "f3": {"kind": "guide"},
        })
    );

    shutdown(running).await;
}

#[tokio::test]
async fn row_to_json_serializes_whole_table_alias() {
    let running = start_sample_server("json_functions_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE json_row_docs (id INT, name TEXT, meta JSONB)")
        .await
        .expect("create row_to_json table");
    client
        .batch_execute(
            "INSERT INTO json_row_docs VALUES \
             (1, 'Ada', '{\"kind\":\"guide\",\"rank\":1}'::jsonb)",
        )
        .await
        .expect("seed row_to_json table");

    let messages = client
        .simple_query("SELECT row_to_json(d) FROM json_row_docs d WHERE d.id = 1")
        .await
        .expect("row_to_json alias query");
    let text = messages
        .into_iter()
        .find_map(|message| match message {
            SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        })
        .expect("row_to_json alias row");
    let got: serde_json::Value = serde_json::from_str(&text).expect("json object");

    assert_eq!(
        got,
        serde_json::json!({
            "id": 1,
            "name": "Ada",
            "meta": {"kind": "guide", "rank": 1},
        })
    );

    shutdown(running).await;
}
