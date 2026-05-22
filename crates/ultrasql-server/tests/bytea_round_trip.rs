//! End-to-end BYTEA storage tests over PostgreSQL wire.

use tokio_postgres::SimpleQueryMessage;

mod support;

use support::{shutdown, start_sample_server};

fn simple_rows(messages: &[SimpleQueryMessage]) -> Vec<Vec<String>> {
    messages
        .iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|idx| row.get(idx).unwrap_or("").to_owned())
                    .collect(),
            ),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn bytea_hex_literal_round_trips_through_heap() {
    let running = start_sample_server("bytea_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL, payload BYTEA)")
        .await
        .expect("create bytea table");
    client
        .batch_execute("INSERT INTO t VALUES (1, '\\xdeadbeef'::bytea)")
        .await
        .expect("insert bytea literal");

    let selected = client
        .simple_query("SELECT payload FROM t WHERE id = 1")
        .await
        .expect("select bytea");
    let rows = simple_rows(&selected);
    assert_eq!(rows, vec![vec!["\\xdeadbeef".to_owned()]]);

    shutdown(running).await;
}
