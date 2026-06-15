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
async fn describe_view_returns_catalog_metadata() {
    let running = start_sample_server("describe-round-trip").await;
    let client = &running.client;

    client
        .batch_execute(
            "CREATE TABLE describe_view_src (id INT NOT NULL, note TEXT);
             CREATE VIEW describe_view_v (view_id, view_note) AS
                 SELECT id, note FROM describe_view_src",
        )
        .await
        .expect("create described view");

    let rows = client
        .query("DESCRIBE VIEW describe_view_v", &[])
        .await
        .expect("describe view");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, String>("column_name"), "view_id");
    assert_eq!(rows[0].get::<_, String>("data_type"), "integer");
    assert!(!rows[0].get::<_, bool>("nullable"));
    assert_eq!(rows[0].get::<_, String>("source_schema"), "public");
    assert_eq!(rows[0].get::<_, String>("source_object"), "describe_view_v");
    assert_eq!(rows[0].get::<_, String>("source_kind"), "view");
    assert_eq!(rows[1].get::<_, String>("column_name"), "view_note");

    shutdown(running).await;
}

#[tokio::test]
async fn describe_rejects_wrong_object_kind() {
    let running = start_sample_server("describe-round-trip").await;
    let client = &running.client;

    client
        .batch_execute(
            "CREATE TABLE describe_kind_src (id INT NOT NULL);
             CREATE VIEW describe_kind_v AS SELECT id FROM describe_kind_src",
        )
        .await
        .expect("create describe kind objects");

    let table_as_view = client
        .query("DESCRIBE VIEW describe_kind_src", &[])
        .await
        .expect_err("DESCRIBE VIEW must reject tables");
    let table_msg = table_as_view
        .as_db_error()
        .map(tokio_postgres::error::DbError::message)
        .unwrap_or("");
    assert!(
        table_msg.contains("is not a view"),
        "wrong-kind error should name table/view mismatch: {table_as_view}"
    );

    let view_as_table = client
        .query("DESCRIBE TABLE describe_kind_v", &[])
        .await
        .expect_err("DESCRIBE TABLE must reject views");
    let view_msg = view_as_table
        .as_db_error()
        .map(tokio_postgres::error::DbError::message)
        .unwrap_or("");
    assert!(
        view_msg.contains("is a view"),
        "wrong-kind error should name view/table mismatch: {view_as_table}"
    );

    let inferred = client
        .query("DESCRIBE describe_kind_v", &[])
        .await
        .expect("unqualified DESCRIBE infers view");
    assert_eq!(inferred[0].get::<_, String>("source_kind"), "view");

    shutdown(running).await;
}
