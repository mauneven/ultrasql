pub mod support;

use ultrasql_wal::RecordType;

#[tokio::test]
async fn pg_stat_wal_reports_live_append_counters() {
    let data_dir = tempfile::TempDir::new().expect("tempdir");
    let running = support::start_persistent_server(data_dir.path(), "wal_stats_test").await;

    running
        .client
        .batch_execute("CREATE TABLE wal_stats_t (id INT)")
        .await
        .expect("create wal stats table");
    running
        .client
        .batch_execute("INSERT INTO wal_stats_t VALUES (1), (2)")
        .await
        .expect("insert wal stats rows");

    let rows = running
        .client
        .query(
            "SELECT wal_records, wal_bytes, wal_write FROM pg_catalog.pg_stat_wal",
            &[],
        )
        .await
        .expect("query pg_stat_wal");
    assert_eq!(rows.len(), 1);
    assert!(rows[0].get::<_, i64>(0) > 0);
    assert!(rows[0].get::<_, i64>(1) > 0);
    assert!(rows[0].get::<_, i64>(2) > 0);

    support::shutdown(running).await;
}

#[tokio::test]
async fn explicit_rollback_of_persistent_dml_writes_abort_record() {
    let data_dir = tempfile::TempDir::new().expect("tempdir");
    support::make_data_dir_private(data_dir.path());
    let running = support::start_persistent_server(data_dir.path(), "wal_abort_marker_test").await;

    running
        .client
        .batch_execute(
            "CREATE TABLE wal_abort_t (id INT); \
             BEGIN; \
             INSERT INTO wal_abort_t VALUES (1); \
             ROLLBACK",
        )
        .await
        .expect("rollback dml");

    support::shutdown(running).await;

    let mut abort_records = 0_u64;
    ultrasql_wal::recover(data_dir.path().join("pg_wal"), |record| {
        if record.header.record_type == RecordType::Abort {
            abort_records = abort_records.saturating_add(1);
        }
        Ok(())
    })
    .expect("recover WAL");

    assert!(abort_records > 0, "rollback wrote no abort WAL record");
}
