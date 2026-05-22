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
