//! End-to-end `statement_timeout` coverage.
//!
//! The timeout is a session GUC. It should be visible through
//! `SHOW statement_timeout`, reset to PostgreSQL's default `0` (disabled),
//! and cancel long-running executor work with SQLSTATE `57014`.

use std::time::Duration;

pub mod support;

use support::{shutdown, start_sample_server};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn statement_timeout_set_show_and_reset_round_trip() {
    let running = start_sample_server("statement_timeout_guc_test").await;
    let client = &running.client;

    let row = client
        .query_one("SHOW statement_timeout", &[])
        .await
        .expect("show default statement_timeout");
    assert_eq!(row.get::<_, String>(0), "0");

    client
        .batch_execute("SET statement_timeout = 25")
        .await
        .expect("set statement_timeout");
    let row = client
        .query_one("SHOW statement_timeout", &[])
        .await
        .expect("show configured statement_timeout");
    assert_eq!(row.get::<_, String>(0), "25");

    client
        .batch_execute("RESET statement_timeout")
        .await
        .expect("reset statement_timeout");
    let row = client
        .query_one("SHOW statement_timeout", &[])
        .await
        .expect("show reset statement_timeout");
    assert_eq!(row.get::<_, String>(0), "0");

    shutdown(running).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn statement_timeout_cancels_long_generate_series_and_session_recovers() {
    let running = start_sample_server("statement_timeout_cancel_test").await;
    let client = &running.client;

    client
        .batch_execute("SET statement_timeout = 1")
        .await
        .expect("set statement_timeout");

    let result = tokio::time::timeout(
        Duration::from_secs(2),
        client.simple_query("SELECT COUNT(*) FROM generate_series(1, 1000000000)"),
    )
    .await
    .expect("statement_timeout should resolve the query future");

    let err = match result {
        Ok(rows) => panic!(
            "expected statement_timeout cancellation, got {} protocol messages",
            rows.len()
        ),
        Err(err) => err,
    };
    let sqlstate = err
        .code()
        .map(|code| code.code())
        .expect("statement timeout error carries SQLSTATE");
    assert_eq!(sqlstate, "57014");

    client
        .batch_execute("SET statement_timeout = 0")
        .await
        .expect("disable statement_timeout after cancellation");
    let row = client
        .query_one("SELECT 1", &[])
        .await
        .expect("session remains usable after statement timeout");
    assert_eq!(row.get::<_, i32>(0), 1);

    shutdown(running).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn statement_timeout_cancels_extended_execute_and_session_recovers() {
    let running = start_sample_server("statement_timeout_extended_cancel_test").await;
    let client = &running.client;

    client
        .batch_execute("SET statement_timeout = 1")
        .await
        .expect("set statement_timeout");

    let result = tokio::time::timeout(
        Duration::from_secs(2),
        client.query_one("SELECT COUNT(*) FROM generate_series(1, 1000000000)", &[]),
    )
    .await
    .expect("statement_timeout should resolve the extended query future");

    let err = result.expect_err("expected statement_timeout cancellation");
    let sqlstate = err
        .code()
        .map(|code| code.code())
        .expect("statement timeout error carries SQLSTATE");
    assert_eq!(sqlstate, "57014");

    client
        .batch_execute("SET statement_timeout = 0")
        .await
        .expect("disable statement_timeout after extended cancellation");
    let row = client
        .query_one("SELECT 1", &[])
        .await
        .expect("session remains usable after extended statement timeout");
    assert_eq!(row.get::<_, i32>(0), 1);

    shutdown(running).await;
}
