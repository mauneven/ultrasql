//! Persistent two-phase-commit restart coverage through the PostgreSQL wire path.

pub mod support;

use support::{shutdown, start_persistent_server};

async fn count_rows(client: &tokio_postgres::Client, table: &str) -> i64 {
    let sql = format!("SELECT COUNT(*) FROM {table}");
    client
        .query_one(&sql, &[])
        .await
        .expect("count rows")
        .get(0)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_prepared_survives_restart_and_makes_rows_visible() {
    let data_dir = tempfile::TempDir::new().expect("data dir");
    support::make_data_dir_private(data_dir.path());

    let running = start_persistent_server(data_dir.path(), "two_phase_restart_prepare").await;
    running
        .client
        .batch_execute("CREATE TABLE two_phase_restart (id INT NOT NULL, note TEXT)")
        .await
        .expect("create table");
    running
        .client
        .batch_execute(
            "BEGIN; \
             INSERT INTO two_phase_restart VALUES (1, 'prepared'); \
             PREPARE TRANSACTION 'restart-gid'",
        )
        .await
        .expect("prepare transaction");
    shutdown(running).await;

    let running = start_persistent_server(data_dir.path(), "two_phase_restart_commit").await;
    assert_eq!(count_rows(&running.client, "two_phase_restart").await, 0);
    running
        .client
        .batch_execute("COMMIT PREPARED 'restart-gid'")
        .await
        .expect("commit prepared after restart");
    let row = running
        .client
        .query_one("SELECT note FROM two_phase_restart WHERE id = 1", &[])
        .await
        .expect("prepared row visible after commit");
    let note: &str = row.get(0);
    assert_eq!(note, "prepared");
    shutdown(running).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rollback_prepared_survives_restart_and_discards_rows() {
    let data_dir = tempfile::TempDir::new().expect("data dir");
    support::make_data_dir_private(data_dir.path());

    let running = start_persistent_server(data_dir.path(), "two_phase_restart_prepare").await;
    running
        .client
        .batch_execute("CREATE TABLE two_phase_restart_abort (id INT NOT NULL, note TEXT)")
        .await
        .expect("create table");
    running
        .client
        .batch_execute(
            "BEGIN; \
             INSERT INTO two_phase_restart_abort VALUES (1, 'prepared'); \
             PREPARE TRANSACTION 'restart-rollback-gid'",
        )
        .await
        .expect("prepare transaction");
    shutdown(running).await;

    let running = start_persistent_server(data_dir.path(), "two_phase_restart_rollback").await;
    running
        .client
        .batch_execute("ROLLBACK PREPARED 'restart-rollback-gid'")
        .await
        .expect("rollback prepared after restart");
    assert_eq!(
        count_rows(&running.client, "two_phase_restart_abort").await,
        0
    );
    shutdown(running).await;
}
