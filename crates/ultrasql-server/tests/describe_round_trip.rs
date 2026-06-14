//! End-to-end SQL `DESCRIBE` tests.

pub mod support;

use support::{shutdown, start_sample_server};

#[tokio::test]
async fn describe_table_returns_stable_column_metadata() {
    let running = start_sample_server("describe-round-trip").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE describe_t (id INT NOT NULL, note TEXT)")
        .await
        .expect("create describe table");

    let rows = client
        .query("DESCRIBE TABLE describe_t", &[])
        .await
        .expect("describe table");
    assert_eq!(rows.len(), 2);

    assert_eq!(rows[0].get::<_, String>("column_name"), "id");
    assert_eq!(rows[0].get::<_, String>("data_type"), "integer");
    assert!(!rows[0].get::<_, bool>("nullable"));
    assert_eq!(rows[0].get::<_, String>("source_schema"), "public");
    assert_eq!(rows[0].get::<_, String>("source_object"), "describe_t");
    assert_eq!(rows[0].get::<_, String>("source_kind"), "table");

    assert_eq!(rows[1].get::<_, String>("column_name"), "note");
    assert_eq!(rows[1].get::<_, String>("data_type"), "text");
    assert!(rows[1].get::<_, bool>("nullable"));

    shutdown(running).await;
}

#[tokio::test]
async fn describe_query_returns_projection_metadata() {
    let running = start_sample_server("describe-round-trip").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE describe_q (id INT NOT NULL, note TEXT)")
        .await
        .expect("create describe query table");

    let rows = client
        .query("DESCRIBE SELECT id, note FROM describe_q", &[])
        .await
        .expect("describe query");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, String>("column_name"), "id");
    assert_eq!(rows[0].get::<_, String>("data_type"), "integer");
    assert!(!rows[0].get::<_, bool>("nullable"));
    assert_eq!(rows[0].get::<_, String>("source_schema"), "");
    assert_eq!(rows[0].get::<_, String>("source_object"), "");
    assert_eq!(rows[0].get::<_, String>("source_kind"), "query");
    assert_eq!(rows[1].get::<_, String>("column_name"), "note");

    shutdown(running).await;
}

#[tokio::test]
async fn describe_missing_table_returns_actionable_error() {
    let running = start_sample_server("describe-round-trip").await;
    let client = &running.client;

    let err = client
        .query("DESCRIBE missing_describe_t", &[])
        .await
        .expect_err("missing describe target must fail");
    let message = err
        .as_db_error()
        .map(tokio_postgres::error::DbError::message)
        .unwrap_or("");
    assert!(
        message.contains("missing_describe_t"),
        "error should name missing object: {err}"
    );

    shutdown(running).await;
}

#[tokio::test]
async fn describe_view_returns_feature_not_supported() {
    let running = start_sample_server("describe-round-trip").await;
    let client = &running.client;

    let err = client
        .query("DESCRIBE VIEW users", &[])
        .await
        .expect_err("describe view must fail until regular views exist");
    let db = err.as_db_error().expect("server db error");
    assert_eq!(db.code().code(), "0A000");
    assert!(
        db.message()
            .contains("DESCRIBE VIEW requires view catalog metadata"),
        "error should name unsupported view metadata: {err}"
    );

    shutdown(running).await;
}
