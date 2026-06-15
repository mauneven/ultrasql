//! End-to-end `SUMMARIZE` tests over the PostgreSQL wire protocol.

pub mod support;

use support::{shutdown, start_sample_server};

#[tokio::test]
async fn summarize_mixed_table_returns_column_statistics() {
    let running = start_sample_server("summarize_mixed").await;
    let client = &running.client;

    client
        .batch_execute(
            "CREATE TABLE summary_mix (
                id INT NOT NULL,
                label TEXT,
                amount DOUBLE PRECISION,
                flag BOOLEAN,
                d DATE,
                t TIME
             );
             INSERT INTO summary_mix VALUES
                (1, 'a', 1.0, true, DATE '2024-01-01', TIME '01:00:00'),
                (2, 'a', 2.0, false, DATE '2024-01-03', TIME '02:30:00'),
                (3, NULL, 3.0, NULL, NULL, NULL)",
        )
        .await
        .expect("setup summarize table");

    let rows = client
        .query("SUMMARIZE summary_mix", &[])
        .await
        .expect("summarize table");
    assert_eq!(rows.len(), 6);

    assert_eq!(rows[0].get::<_, String>("column_name"), "id");
    assert_eq!(rows[0].get::<_, String>("data_type"), "integer");
    assert_eq!(rows[0].get::<_, i64>("row_count"), 3);
    assert_eq!(rows[0].get::<_, i64>("null_count"), 0);
    assert_eq!(
        rows[0].get::<_, Option<String>>("min").as_deref(),
        Some("1")
    );
    assert_eq!(
        rows[0].get::<_, Option<String>>("max").as_deref(),
        Some("3")
    );
    assert_eq!(rows[0].get::<_, i64>("unique_count"), 3);
    assert_eq!(rows[0].get::<_, Option<f64>>("avg"), Some(2.0));
    assert_eq!(rows[0].get::<_, Option<f64>>("stddev"), Some(1.0));

    assert_eq!(rows[1].get::<_, String>("column_name"), "label");
    assert_eq!(rows[1].get::<_, i64>("null_count"), 1);
    assert_eq!(
        rows[1].get::<_, Option<String>>("min").as_deref(),
        Some("a")
    );
    assert_eq!(
        rows[1].get::<_, Option<String>>("max").as_deref(),
        Some("a")
    );
    assert_eq!(rows[1].get::<_, i64>("unique_count"), 1);
    assert_eq!(rows[1].get::<_, Option<f64>>("avg"), None);
    assert_eq!(rows[1].get::<_, Option<f64>>("stddev"), None);

    assert_eq!(rows[3].get::<_, String>("column_name"), "flag");
    assert_eq!(
        rows[3].get::<_, Option<String>>("min").as_deref(),
        Some("false")
    );
    assert_eq!(
        rows[3].get::<_, Option<String>>("max").as_deref(),
        Some("true")
    );
    assert_eq!(rows[3].get::<_, i64>("unique_count"), 2);

    assert_eq!(rows[4].get::<_, String>("column_name"), "d");
    assert_eq!(
        rows[4].get::<_, Option<String>>("min").as_deref(),
        Some("2024-01-01")
    );
    assert_eq!(
        rows[4].get::<_, Option<String>>("max").as_deref(),
        Some("2024-01-03")
    );

    assert_eq!(rows[5].get::<_, String>("column_name"), "t");
    assert_eq!(
        rows[5].get::<_, Option<String>>("min").as_deref(),
        Some("01:00:00")
    );
    assert_eq!(
        rows[5].get::<_, Option<String>>("max").as_deref(),
        Some("02:30:00")
    );

    shutdown(running).await;
}

#[tokio::test]
async fn summarize_empty_table_returns_zero_counts_and_null_stats() {
    let running = start_sample_server("summarize_empty").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE summary_empty (id INT, label TEXT)")
        .await
        .expect("create empty table");

    let rows = client
        .query("SUMMARIZE summary_empty", &[])
        .await
        .expect("summarize empty table");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, i64>("row_count"), 0);
    assert_eq!(rows[0].get::<_, i64>("null_count"), 0);
    assert_eq!(rows[0].get::<_, Option<String>>("min"), None);
    assert_eq!(rows[0].get::<_, Option<String>>("max"), None);
    assert_eq!(rows[0].get::<_, i64>("unique_count"), 0);
    assert_eq!(rows[0].get::<_, Option<f64>>("avg"), None);
    assert_eq!(rows[0].get::<_, Option<f64>>("stddev"), None);

    shutdown(running).await;
}

#[tokio::test]
async fn summarize_missing_table_returns_actionable_error() {
    let running = start_sample_server("summarize_missing").await;
    let client = &running.client;

    let err = client
        .query("SUMMARIZE missing_summary_t", &[])
        .await
        .expect_err("missing summarize target must fail");
    let message = err
        .as_db_error()
        .map(tokio_postgres::error::DbError::message)
        .unwrap_or("");
    assert!(
        message.contains("missing_summary_t"),
        "error should name missing object: {err}"
    );

    shutdown(running).await;
}

#[tokio::test]
async fn summarize_respects_transaction_rollback_visibility() {
    let running = start_sample_server("summarize_txn").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE summary_txn (id INT)")
        .await
        .expect("create transaction summarize table");
    client
        .batch_execute("BEGIN; INSERT INTO summary_txn VALUES (1)")
        .await
        .expect("insert inside transaction");

    let in_tx = client
        .query("SUMMARIZE summary_txn", &[])
        .await
        .expect("summarize inside transaction");
    assert_eq!(in_tx[0].get::<_, i64>("row_count"), 1);

    client.batch_execute("ROLLBACK").await.expect("rollback");
    let after_rollback = client
        .query("SUMMARIZE summary_txn", &[])
        .await
        .expect("summarize after rollback");
    assert_eq!(after_rollback[0].get::<_, i64>("row_count"), 0);

    shutdown(running).await;
}

#[tokio::test]
async fn summarize_rejects_row_level_security_tables() {
    let running = start_sample_server("summarize_rls").await;
    let client = &running.client;

    client
        .batch_execute(
            "CREATE TABLE summary_rls (tenant_id TEXT);
             INSERT INTO summary_rls VALUES ('tenant_a'), ('tenant_b');
             CREATE POLICY summary_rls_filter ON summary_rls
                USING (tenant_id = current_setting('ultrasql.tenant_id', true));
             ALTER TABLE summary_rls ENABLE ROW LEVEL SECURITY;",
        )
        .await
        .expect("setup RLS summarize table");

    let err = client
        .query("SUMMARIZE summary_rls", &[])
        .await
        .expect_err("SUMMARIZE over RLS table must fail closed");
    let message = err
        .as_db_error()
        .map(tokio_postgres::error::DbError::message)
        .unwrap_or("");
    assert!(
        message.contains("row-level security"),
        "error should name RLS limitation: {err}"
    );

    shutdown(running).await;
}
