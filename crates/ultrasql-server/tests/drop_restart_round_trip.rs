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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_table_is_removed_from_runtime_metadata() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let metadata_path = data_dir.path().join("pg_table_runtime.meta");

    let running = start_persistent_server(data_dir.path(), "drop_runtime_meta_test").await;
    running
        .client
        .batch_execute("CREATE TABLE drop_runtime_meta (id SERIAL, v INT DEFAULT 7)")
        .await
        .expect("create table with runtime metadata");
    let metadata = std::fs::read_to_string(&metadata_path).expect("table runtime metadata exists");
    assert!(
        metadata.contains("drop_runtime_meta"),
        "table runtime metadata should record table before drop: {metadata}"
    );

    running
        .client
        .batch_execute("DROP TABLE drop_runtime_meta")
        .await
        .expect("drop table");
    shutdown(running).await;

    let metadata = std::fs::read_to_string(&metadata_path).expect("table runtime metadata exists");
    assert!(
        !metadata.contains("drop_runtime_meta"),
        "dropped table must be removed from runtime metadata: {metadata}"
    );
}

async fn assert_undefined_table(client: &tokio_postgres::Client, sql: &str) {
    let err = client.query(sql, &[]).await.expect_err("query should fail");
    let db_error = err.as_db_error().expect("server returns SQLSTATE");
    assert_eq!(db_error.code().code(), "42P01");
}
