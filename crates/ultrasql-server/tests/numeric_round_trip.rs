//! End-to-end NUMERIC / DECIMAL arithmetic, casts, and wire metadata.

mod support;

use support::{shutdown, start_persistent_server, start_sample_server};

#[tokio::test]
async fn numeric_arithmetic_casts_and_extended_wire_type() {
    let running = start_sample_server("numeric_round_trip").await;
    let client = &running.client;

    let messages = client
        .simple_query(
            "SELECT \
                1::numeric / 6::numeric AS div, \
                '12.340'::numeric AS casted, \
                1.20::numeric + 3::numeric AS sum",
        )
        .await
        .expect("numeric arithmetic query");
    let row = messages
        .into_iter()
        .find_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .expect("one row");
    assert_eq!(row.get("div"), Some("0.166667"));
    assert_eq!(row.get("casted"), Some("12.340"));
    assert_eq!(row.get("sum"), Some("4.20"));

    let rows = client
        .query("SELECT 1.25::numeric AS n", &[])
        .await
        .expect("extended numeric query");
    assert_eq!(rows[0].columns()[0].type_().oid(), 1700);

    shutdown(running).await;
}

#[tokio::test]
async fn numeric_precision_overflow_reports_sqlstate() {
    let data_dir = tempfile::TempDir::new().expect("temp dir");
    let running = start_persistent_server(data_dir.path(), "numeric_round_trip").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE numeric_precision (amount NUMERIC(4,2))")
        .await
        .expect("create numeric precision table");
    client
        .batch_execute("INSERT INTO numeric_precision VALUES (12.34)")
        .await
        .expect("insert in-range numeric");

    let err = client
        .batch_execute("INSERT INTO numeric_precision VALUES (123.45)")
        .await
        .expect_err("numeric precision overflow must fail");
    assert_eq!(
        err.code().map(tokio_postgres::error::SqlState::code),
        Some("22003")
    );
    shutdown(running).await;

    let running = start_persistent_server(data_dir.path(), "numeric_round_trip").await;
    let err = running
        .client
        .batch_execute("INSERT INTO numeric_precision VALUES (123.45)")
        .await
        .expect_err("numeric precision overflow must fail after restart");
    assert_eq!(
        err.code().map(tokio_postgres::error::SqlState::code),
        Some("22003")
    );
    shutdown(running).await;
}
