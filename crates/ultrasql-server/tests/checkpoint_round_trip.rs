//! End-to-end SQL `CHECKPOINT` tests.

pub mod support;

use support::{shutdown, start_persistent_server, start_sample_server};
use tokio_postgres::error::SqlState;
use ultrasql_wal::RecordType;

#[tokio::test]
async fn checkpoint_writes_durable_wal_record_and_rows_survive_restart() {
    let data_dir = tempfile::TempDir::new().expect("temp data dir");
    support::make_data_dir_private(data_dir.path());

    let running = start_persistent_server(data_dir.path(), "checkpoint_round_trip_setup").await;
    running
        .client
        .batch_execute(
            "CREATE TABLE checkpoint_t (id INT NOT NULL, note TEXT); \
             INSERT INTO checkpoint_t VALUES (1, 'before-checkpoint'); \
             CHECKPOINT",
        )
        .await
        .expect("checkpoint should complete");
    shutdown(running).await;

    let mut checkpoint_records = 0_u64;
    ultrasql_wal::recover(data_dir.path().join("pg_wal"), |record| {
        if record.header.record_type == RecordType::Checkpoint {
            checkpoint_records = checkpoint_records.saturating_add(1);
        }
        Ok(())
    })
    .expect("recover checkpoint WAL");
    assert!(
        checkpoint_records > 0,
        "CHECKPOINT did not write a checkpoint WAL record"
    );

    let running = start_persistent_server(data_dir.path(), "checkpoint_round_trip_verify").await;
    let row = running
        .client
        .query_one("SELECT note FROM checkpoint_t WHERE id = 1", &[])
        .await
        .expect("query row after restart");
    assert_eq!(row.get::<_, String>(0), "before-checkpoint");
    shutdown(running).await;
}

#[tokio::test]
async fn checkpoint_extended_query_succeeds_without_wal() {
    let running = start_sample_server("checkpoint_extended_no_wal").await;
    let rows = running
        .client
        .execute("CHECKPOINT", &[])
        .await
        .expect("extended CHECKPOINT should complete on in-memory server");
    assert_eq!(rows, 0);
    shutdown(running).await;
}

#[tokio::test]
async fn checkpoint_is_rejected_inside_explicit_transaction() {
    let data_dir = tempfile::TempDir::new().expect("temp data dir");
    support::make_data_dir_private(data_dir.path());

    let running = start_persistent_server(data_dir.path(), "checkpoint_txn_reject").await;
    running.client.batch_execute("BEGIN").await.expect("begin");
    let err = running
        .client
        .batch_execute("CHECKPOINT")
        .await
        .expect_err("CHECKPOINT must be rejected inside explicit transaction");
    assert_eq!(
        err.code(),
        Some(&SqlState::FEATURE_NOT_SUPPORTED),
        "expected feature_not_supported for transactional CHECKPOINT"
    );
    let db_error = err.as_db_error().expect("server ErrorResponse");
    assert!(
        db_error
            .message()
            .contains("CHECKPOINT inside an explicit transaction block"),
        "error should explain transaction restriction: {db_error:?}"
    );
    running
        .client
        .batch_execute("ROLLBACK")
        .await
        .expect("rollback failed transaction");
    shutdown(running).await;
}
