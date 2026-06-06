pub mod support;

use ultrasql_server::replication::ReplicationSlotStore;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_stat_replication_exposes_persisted_physical_slot_progress() {
    let data_dir = tempfile::TempDir::new().expect("temp data dir");
    let running = support::start_persistent_server(data_dir.path(), "replication_stats_test").await;

    let store = ReplicationSlotStore::open(data_dir.path().join("pg_replslot"))
        .expect("replication slot store opens");
    let mut slot = store
        .get_or_create("standby_a")
        .expect("replication slot can be created");
    slot.restart_lsn = Some("00000001000000000000000A".to_string());
    slot.confirmed_flush_lsn = Some("00000001000000000000000B".to_string());
    store.save(&slot).expect("replication slot can be saved");

    let rows = running
        .client
        .query(
            "SELECT application_name, state, sent_lsn, write_lsn, flush_lsn, replay_lsn, sync_state \
             FROM pg_catalog.pg_stat_replication \
             ORDER BY application_name",
            &[],
        )
        .await
        .expect("pg_stat_replication query succeeds");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>(0), "standby_a");
    assert_eq!(rows[0].get::<_, String>(1), "streaming");
    assert_eq!(
        rows[0].get::<_, Option<String>>(2),
        Some("00000001000000000000000A".to_string())
    );
    assert_eq!(
        rows[0].get::<_, Option<String>>(3),
        Some("00000001000000000000000B".to_string())
    );
    assert_eq!(
        rows[0].get::<_, Option<String>>(4),
        Some("00000001000000000000000B".to_string())
    );
    assert_eq!(
        rows[0].get::<_, Option<String>>(5),
        Some("00000001000000000000000B".to_string())
    );
    assert_eq!(rows[0].get::<_, String>(6), "async");

    support::shutdown(running).await;
}
