//! Persistent `DROP TABLE` restart coverage through the PostgreSQL wire path.

mod support;

use support::{shutdown, start_persistent_server};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_table_stays_dropped_after_restart() {
    let data_dir = tempfile::TempDir::new().unwrap();

    let running = start_persistent_server(data_dir.path(), "drop_restart_test").await;
    running
        .client
        .batch_execute("CREATE TABLE drop_restart (id INT)")
        .await
        .expect("create");
    running
        .client
        .batch_execute("INSERT INTO drop_restart VALUES (7)")
        .await
        .expect("insert");
    running
        .client
        .batch_execute("DROP TABLE drop_restart")
        .await
        .expect("drop");
    assert_undefined_table(&running.client, "SELECT id FROM drop_restart").await;
    shutdown(running).await;

    let running = start_persistent_server(data_dir.path(), "drop_restart_test").await;
    assert_undefined_table(&running.client, "SELECT id FROM drop_restart").await;
    shutdown(running).await;
}

async fn assert_undefined_table(client: &tokio_postgres::Client, sql: &str) {
    let err = client.query(sql, &[]).await.expect_err("query should fail");
    let db_error = err.as_db_error().expect("server returns SQLSTATE");
    assert_eq!(db_error.code().code(), "42P01");
}
