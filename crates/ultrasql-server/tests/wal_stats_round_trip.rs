pub mod support;

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
