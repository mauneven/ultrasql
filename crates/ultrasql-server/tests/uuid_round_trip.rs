//! End-to-end UUID type and `gen_random_uuid()` tests.

use tokio_postgres::SimpleQueryMessage;
use ultrasql_core::Value;

pub mod support;

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
async fn uuid_literals_and_gen_random_uuid_round_trip() {
    let running = start_sample_server("uuid_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE t (id UUID, label TEXT NOT NULL)")
        .await
        .expect("create uuid table");
    client
        .batch_execute(
            "INSERT INTO t VALUES ('12345678-9abc-def0-1234-56789abcdef0'::uuid, 'fixed')",
        )
        .await
        .expect("insert uuid literal");

    let generated = client
        .simple_query("INSERT INTO t VALUES (gen_random_uuid(), 'generated') RETURNING id")
        .await
        .expect("insert generated uuid");
    let returned = simple_rows(&generated);
    assert_eq!(returned.len(), 1);
    assert!(Value::parse_uuid(&returned[0][0]).is_some());

    let selected = client
        .simple_query("SELECT id, label FROM t ORDER BY label")
        .await
        .expect("select uuid rows");
    let rows = simple_rows(&selected);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], "12345678-9abc-def0-1234-56789abcdef0");
    assert_eq!(rows[0][1], "fixed");
    assert!(Value::parse_uuid(&rows[1][0]).is_some());
    assert_eq!(rows[1][1], "generated");

    shutdown(running).await;
}
