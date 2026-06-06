pub mod support;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_statio_user_indexes_exposes_catalog_index_rows() {
    let running = support::start_sample_server("index_statio_test").await;

    running
        .client
        .batch_execute("CREATE TABLE statio_idx_t (id INT NOT NULL, val INT NOT NULL)")
        .await
        .expect("create table");
    running
        .client
        .batch_execute("CREATE INDEX statio_idx_t_id_idx ON statio_idx_t(id)")
        .await
        .expect("create index");

    let rows = running
        .client
        .query(
            "SELECT relname, indexrelname, idx_blks_read, idx_blks_hit \
             FROM pg_catalog.pg_statio_user_indexes \
             WHERE relname = 'statio_idx_t' \
             ORDER BY indexrelname",
            &[],
        )
        .await
        .expect("pg_statio_user_indexes query");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>(0), "statio_idx_t");
    assert_eq!(rows[0].get::<_, String>(1), "statio_idx_t_id_idx");
    assert!(rows[0].get::<_, i64>(2) >= 0);
    assert!(rows[0].get::<_, i64>(3) >= 0);

    support::shutdown(running).await;
}
