//! Wire-level regression tests for JSON aggregate behavior.
//!
//! `JSON_AGG` is part of the v1.0 SQL surface. These tests use the wire path
//! rather than executor internals so the
//! parser, binder, planner, aggregate executor, and result encoder all stay
//! covered together.

pub mod support;

use support::{shutdown, start_sample_server};
use tokio_postgres::SimpleQueryMessage;

#[tokio::test]
async fn json_agg_returns_jsonb_arrays_over_wire() {
    let running = start_sample_server("json_agg_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE json_agg_t (id INT NOT NULL, name TEXT, doc JSONB)")
        .await
        .expect("create json_agg table");
    client
        .batch_execute(
            "INSERT INTO json_agg_t VALUES
             (1, 'Ada', '{\"kind\":\"guide\"}'::jsonb),
             (2, 'Grace', '{\"kind\":\"paper\"}'::jsonb),
             (3, 'Linus', '{\"kind\":\"kernel\"}'::jsonb)",
        )
        .await
        .expect("seed json_agg rows");

    for (sql, expected) in [
        ("SELECT json_agg(id) FROM json_agg_t", "[1,2,3]"),
        (
            "SELECT json_agg(name) FROM json_agg_t",
            "[\"Ada\",\"Grace\",\"Linus\"]",
        ),
        (
            "SELECT json_agg(doc) FROM json_agg_t",
            "[{\"kind\":\"guide\"},{\"kind\":\"paper\"},{\"kind\":\"kernel\"}]",
        ),
    ] {
        let messages = client.simple_query(sql).await.expect(sql);
        let rows: Vec<String> = messages
            .into_iter()
            .filter_map(|message| match message {
                SimpleQueryMessage::Row(row) => {
                    Some(row.get(0).expect("json_agg column").to_owned())
                }
                SimpleQueryMessage::CommandComplete(_) => None,
                _ => None,
            })
            .collect();

        assert_eq!(rows, vec![expected.to_owned()], "{sql}");
    }

    shutdown(running).await;
}
