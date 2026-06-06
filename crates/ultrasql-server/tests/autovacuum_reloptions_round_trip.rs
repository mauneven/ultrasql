//! Per-table autovacuum reloption round trips.

pub mod support;

use support::{shutdown, start_sample_server};

#[tokio::test]
async fn alter_table_set_autovacuum_options_updates_catalog_entry() {
    let running = start_sample_server("autovacuum_reloptions_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE av_relopts (id INT)")
        .await
        .expect("create table");
    client
        .batch_execute(
            "ALTER TABLE av_relopts SET (\
             autovacuum_vacuum_threshold = 3, \
             autovacuum_vacuum_scale_factor = 0.01, \
             autovacuum_analyze_threshold = 5, \
             autovacuum_analyze_scale_factor = 0.02)",
        )
        .await
        .expect("alter table set reloptions");

    let snapshot = running.server.catalog_snapshot();
    let entry = snapshot
        .tables
        .get("av_relopts")
        .expect("table entry exists");
    assert_eq!(
        entry.options,
        vec![
            ("autovacuum_vacuum_threshold".to_owned(), "3".to_owned()),
            (
                "autovacuum_vacuum_scale_factor".to_owned(),
                "0.01".to_owned()
            ),
            ("autovacuum_analyze_threshold".to_owned(), "5".to_owned()),
            (
                "autovacuum_analyze_scale_factor".to_owned(),
                "0.02".to_owned()
            ),
        ]
    );

    shutdown(running).await;
}
