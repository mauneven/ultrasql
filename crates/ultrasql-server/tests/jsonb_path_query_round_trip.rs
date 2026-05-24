//! Wire-level regression tests for SQL/JSON path query table function.

mod support;

use support::{shutdown, start_sample_server};
use tokio_postgres::SimpleQueryMessage;

#[tokio::test]
async fn jsonb_path_query_expands_selected_values() {
    let running = start_sample_server("jsonb_path_query_test").await;
    let client = &running.client;

    let messages = client
        .simple_query(
            "SELECT value FROM jsonb_path_query(\
             '{\"items\":[{\"id\":1},{\"id\":2}]}'::jsonb, '$.items[*].id') \
             ORDER BY value",
        )
        .await
        .expect("jsonb_path_query");
    let rows: Vec<String> = messages
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        })
        .collect();

    assert_eq!(rows, vec!["1".to_owned(), "2".to_owned()]);

    shutdown(running).await;
}

#[tokio::test]
async fn jsonb_path_query_supports_sql_json_filters_and_recursive_descent() {
    let running = start_sample_server("jsonb_path_query_test").await;
    let client = &running.client;

    let document = "'{\"items\":[\
        {\"id\":1,\"score\":12,\"meta\":{\"kind\":\"guide\"}},\
        {\"id\":2,\"score\":25,\"meta\":{\"kind\":\"paper\"}},\
        {\"id\":3,\"score\":31,\"meta\":{\"kind\":\"guide\"}}\
    ],\"weird-key\":{\"id\":9}}'::jsonb";

    let filtered = client
        .simple_query(&format!(
            "SELECT value FROM jsonb_path_query(\
             {document}, '$.items[*] ? (@.meta.kind == \"guide\").id') \
             ORDER BY value"
        ))
        .await
        .expect("jsonb_path_query filter");
    let rows: Vec<String> = filtered
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        })
        .collect();
    assert_eq!(rows, vec!["1".to_owned(), "3".to_owned()]);

    let quoted_key = client
        .simple_query(&format!(
            "SELECT value FROM jsonb_path_query({document}, '$.\"weird-key\".id')"
        ))
        .await
        .expect("jsonb_path_query quoted key");
    let rows: Vec<String> = quoted_key
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        })
        .collect();
    assert_eq!(rows, vec!["9".to_owned()]);

    let recursive = client
        .simple_query(&format!(
            "SELECT value FROM jsonb_path_query({document}, '$.**.kind') \
             ORDER BY value"
        ))
        .await
        .expect("jsonb_path_query recursive descent");
    let rows: Vec<String> = recursive
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        })
        .collect();
    assert_eq!(
        rows,
        vec![
            "\"guide\"".to_owned(),
            "\"guide\"".to_owned(),
            "\"paper\"".to_owned(),
        ]
    );

    shutdown(running).await;
}

#[tokio::test]
async fn jsonb_path_exists_evaluates_sql_json_predicates() {
    let running = start_sample_server("jsonb_path_query_test").await;
    let client = &running.client;

    let messages = client
        .simple_query(
            "SELECT \
                jsonb_path_exists('{\"items\":[{\"score\":12},{\"score\":25}]}'::jsonb, \
                    '$.items[*] ? (@.score >= 20)'), \
                jsonb_path_exists('{\"items\":[{\"score\":12},{\"score\":25}]}'::jsonb, \
                    '$.items[*] ? (@.score > 99)')",
        )
        .await
        .expect("jsonb_path_exists predicate");
    let row = messages
        .into_iter()
        .find_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .expect("jsonb_path_exists row");

    assert_eq!(row.get(0), Some("t"));
    assert_eq!(row.get(1), Some("f"));

    shutdown(running).await;
}
