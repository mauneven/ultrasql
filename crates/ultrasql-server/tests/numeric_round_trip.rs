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
async fn runtime_numeric_typmod_casts_round_and_check_precision() {
    let running = start_sample_server("numeric_runtime_typmod_casts").await;
    let client = &running.client;

    client
        .batch_execute(
            "CREATE TABLE runtime_numeric_casts (
                text_value TEXT NOT NULL,
                float_value DOUBLE PRECISION NOT NULL,
                int_value INT NOT NULL,
                numeric_value NUMERIC(6,3) NOT NULL
            )",
        )
        .await
        .expect("create runtime numeric cast table");
    client
        .batch_execute("INSERT INTO runtime_numeric_casts VALUES ('12.345', 3.456, 7, 12.345)")
        .await
        .expect("insert runtime numeric cast row");

    let rows = client
        .simple_query(
            "SELECT
                CAST(text_value AS NUMERIC(5,2)),
                CAST(float_value AS NUMERIC(5,2)),
                CAST(int_value AS NUMERIC(5,2)),
                CAST(numeric_value AS NUMERIC(5,2))
             FROM runtime_numeric_casts",
        )
        .await
        .expect("runtime numeric typmod casts");
    let values: Vec<Vec<String>> = rows
        .into_iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .filter_map(|idx| row.get(idx).map(str::to_owned))
                    .collect(),
            ),
            _ => None,
        })
        .collect();

    assert_eq!(
        values,
        vec![vec![
            "12.35".to_owned(),
            "3.46".to_owned(),
            "7.00".to_owned(),
            "12.35".to_owned()
        ]]
    );

    client
        .batch_execute("UPDATE runtime_numeric_casts SET text_value = '1234.56'")
        .await
        .expect("update runtime numeric overflow input");
    let err = client
        .simple_query("SELECT CAST(text_value AS NUMERIC(5,2)) FROM runtime_numeric_casts")
        .await
        .expect_err("numeric typmod overflow must fail");
    assert_eq!(
        err.code().map(tokio_postgres::error::SqlState::code),
        Some("22003")
    );

    shutdown(running).await;
}

#[tokio::test]
async fn bare_numeric_heap_preserves_scale_across_restart() {
    let data_dir = tempfile::TempDir::new().expect("temp dir");
    let running = start_persistent_server(data_dir.path(), "numeric_bare_scale").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE bare_numeric_scale (id INT, amount NUMERIC)")
        .await
        .expect("create bare numeric scale table");
    client
        .batch_execute("INSERT INTO bare_numeric_scale VALUES (1, 12.340), (2, 0.166667)")
        .await
        .expect("insert bare numeric scale rows");

    let before = select_bare_numeric_scale(client).await;
    assert_eq!(
        before,
        vec![
            vec!["1".to_owned(), "12.340".to_owned()],
            vec!["2".to_owned(), "0.166667".to_owned()]
        ]
    );
    shutdown(running).await;

    let running = start_persistent_server(data_dir.path(), "numeric_bare_scale").await;
    let after = select_bare_numeric_scale(&running.client).await;
    assert_eq!(after, before);
    shutdown(running).await;
}

async fn select_bare_numeric_scale(client: &tokio_postgres::Client) -> Vec<Vec<String>> {
    client
        .simple_query("SELECT id, amount FROM bare_numeric_scale ORDER BY id")
        .await
        .expect("select bare numeric scale")
        .into_iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .filter_map(|idx| row.get(idx).map(str::to_owned))
                    .collect(),
            ),
            _ => None,
        })
        .collect()
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

#[tokio::test]
async fn runtime_numeric_text_overflow_reports_sqlstate() {
    let running = start_sample_server("numeric_runtime_text_overflow").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE numeric_text_overflow (raw TEXT NOT NULL)")
        .await
        .expect("create numeric text overflow table");
    client
        .batch_execute("INSERT INTO numeric_text_overflow VALUES ('9223372036854775808')")
        .await
        .expect("insert oversized numeric text");

    let err = client
        .simple_query("SELECT CAST(raw AS NUMERIC) FROM numeric_text_overflow")
        .await
        .expect_err("oversized runtime numeric text must fail");
    assert_eq!(
        err.code().map(tokio_postgres::error::SqlState::code),
        Some("22003")
    );

    shutdown(running).await;
}

#[tokio::test]
async fn runtime_numeric_invalid_text_reports_sqlstate() {
    let running = start_sample_server("numeric_runtime_invalid_text").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE numeric_invalid_text (raw TEXT NOT NULL)")
        .await
        .expect("create numeric invalid text table");
    client
        .batch_execute("INSERT INTO numeric_invalid_text VALUES ('not-a-number')")
        .await
        .expect("insert invalid numeric text");

    let err = client
        .simple_query("SELECT CAST(raw AS NUMERIC) FROM numeric_invalid_text")
        .await
        .expect_err("invalid runtime numeric text must fail");
    assert_eq!(
        err.code().map(tokio_postgres::error::SqlState::code),
        Some("22P02")
    );

    shutdown(running).await;
}

#[tokio::test]
async fn numeric_zero_precision_is_rejected() {
    let running = start_sample_server("numeric_round_trip").await;
    let err = running
        .client
        .batch_execute("CREATE TABLE numeric_bad_precision (amount NUMERIC(0,2))")
        .await
        .expect_err("zero precision numeric must fail");
    assert_eq!(
        err.code().map(tokio_postgres::error::SqlState::code),
        Some("42804")
    );
    shutdown(running).await;
}
