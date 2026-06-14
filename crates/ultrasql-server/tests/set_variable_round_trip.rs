//! End-to-end SQL `SET VARIABLE` tests.

pub mod support;

use support::{connect_as, shutdown, start_sample_server};
use tokio_postgres::error::SqlState;

#[tokio::test]
async fn set_variable_sets_custom_parameter_for_current_session_only() {
    let running = start_sample_server("set_variable_session").await;
    let client = &running.client;

    client
        .batch_execute("SET VARIABLE ultrasql.tenant = 'acme'")
        .await
        .expect("SET VARIABLE custom parameter");
    let row = client
        .query_one("SHOW ultrasql.tenant", &[])
        .await
        .expect("SHOW custom parameter in same session");
    assert_eq!(row.get::<_, String>(0), "acme");

    let (other, other_conn) =
        connect_as(running.bound, "tester", "set_variable_other_session").await;
    let row = other
        .query_one("SHOW ultrasql.tenant", &[])
        .await
        .expect("SHOW custom parameter in different session");
    assert_eq!(row.get::<_, String>(0), "");
    drop(other);
    other_conn.await.expect("other session connection joins");

    shutdown(running).await;
}

#[tokio::test]
async fn set_variable_to_reuses_existing_validation_and_reset() {
    let running = start_sample_server("set_variable_validation").await;
    let client = &running.client;

    client
        .batch_execute("SET VARIABLE statement_timeout TO 50")
        .await
        .expect("SET VARIABLE statement_timeout");
    let row = client
        .query_one("SHOW statement_timeout", &[])
        .await
        .expect("SHOW statement_timeout after SET VARIABLE");
    assert_eq!(row.get::<_, String>(0), "50");

    let err = client
        .batch_execute("SET VARIABLE statement_timeout = 'bad'")
        .await
        .expect_err("bad statement_timeout must be rejected");
    let db = err.as_db_error().expect("server returned database error");
    assert_eq!(db.code(), &SqlState::FEATURE_NOT_SUPPORTED);
    assert!(
        db.message().contains("invalid statement_timeout"),
        "unexpected error: {db:?}"
    );

    client
        .batch_execute("SET VARIABLE statement_timeout = DEFAULT")
        .await
        .expect("SET VARIABLE statement_timeout DEFAULT");
    let row = client
        .query_one("SHOW statement_timeout", &[])
        .await
        .expect("SHOW statement_timeout after reset");
    assert_eq!(row.get::<_, String>(0), "0");

    shutdown(running).await;
}

#[tokio::test]
async fn set_variable_is_session_scoped_across_transaction_rollback() {
    let running = start_sample_server("set_variable_txn").await;
    let client = &running.client;

    client
        .batch_execute("BEGIN; SET VARIABLE ultrasql.txn = 'rolled'; ROLLBACK")
        .await
        .expect("SET VARIABLE inside rolled-back transaction");
    let row = client
        .query_one("SHOW ultrasql.txn", &[])
        .await
        .expect("SHOW variable after rollback");
    assert_eq!(row.get::<_, String>(0), "rolled");

    shutdown(running).await;
}

#[tokio::test]
async fn set_variable_rejects_unsupported_forms_and_names() {
    let running = start_sample_server("set_variable_reject").await;
    let client = &running.client;

    let err = client
        .simple_query("SET LOCAL VARIABLE ultrasql.tenant = 'acme'")
        .await
        .expect_err("SET LOCAL VARIABLE must be rejected");
    let db = err.as_db_error().expect("server returned database error");
    assert!(
        db.message().contains("SET VARIABLE"),
        "unexpected error: {db:?}"
    );

    let err = client
        .batch_execute("SET VARIABLE bogus_setting = 'x'")
        .await
        .expect_err("unsupported runtime parameter must be rejected");
    let db = err.as_db_error().expect("server returned database error");
    assert_eq!(db.code(), &SqlState::FEATURE_NOT_SUPPORTED);
    assert!(
        db.message().contains("unsupported runtime parameter"),
        "unexpected error: {db:?}"
    );

    shutdown(running).await;
}

#[tokio::test]
async fn set_variable_can_run_as_prepared_statement_without_parameters() {
    let running = start_sample_server("set_variable_prepared").await;
    let client = &running.client;

    let statement = client
        .prepare("SET VARIABLE ultrasql.prep_flag = 'yes'")
        .await
        .expect("prepare SET VARIABLE");
    let affected = client
        .execute(&statement, &[])
        .await
        .expect("execute prepared SET VARIABLE");
    assert_eq!(affected, 0);
    let row = client
        .query_one("SHOW ultrasql.prep_flag", &[])
        .await
        .expect("SHOW prepared variable");
    assert_eq!(row.get::<_, String>(0), "yes");

    shutdown(running).await;
}
