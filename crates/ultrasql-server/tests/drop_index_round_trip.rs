//! Wire-level `DROP INDEX` coverage and restart persistence.

mod support;

use support::{shutdown, start_persistent_server};
use ultrasql_catalog::bootstrap::PG_CLASS_OID;
use ultrasql_core::Oid;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drop_index_removes_catalog_metadata_and_survives_restart() {
    let data_dir = tempfile::TempDir::new().expect("temp data dir");
    let index_oid;

    {
        let running = start_persistent_server(data_dir.path(), "drop_index_test").await;
        running
            .client
            .batch_execute("CREATE TABLE drop_index_probe (id INT, name TEXT)")
            .await
            .expect("create table");
        running
            .client
            .batch_execute("CREATE INDEX drop_index_probe_idx ON drop_index_probe(id)")
            .await
            .expect("create index");
        index_oid = running
            .server
            .catalog_snapshot()
            .indexes
            .get("drop_index_probe_idx")
            .expect("index before drop")
            .oid;
        running
            .client
            .batch_execute("COMMENT ON INDEX drop_index_probe_idx IS 'drop index docs'")
            .await
            .expect("comment on index");
        running
            .client
            .batch_execute("DROP INDEX drop_index_probe_idx")
            .await
            .expect("drop index");

        let snapshot = running.server.catalog_snapshot();
        assert!(snapshot.tables.contains_key("drop_index_probe"));
        assert!(!snapshot.indexes.contains_key("drop_index_probe_idx"));
        assert!(
            !snapshot
                .descriptions
                .contains_key(&(index_oid, Oid::new(PG_CLASS_OID), 0)),
            "DROP INDEX must clear index comments immediately"
        );
        let rows = running
            .client
            .query(
                "SELECT indexname FROM pg_catalog.pg_indexes \
                 WHERE indexname = 'drop_index_probe_idx'",
                &[],
            )
            .await
            .expect("pg_indexes after drop");
        assert!(
            rows.is_empty(),
            "dropped index must disappear from pg_indexes"
        );
        shutdown(running).await;
    }

    {
        let running = start_persistent_server(data_dir.path(), "drop_index_restart").await;
        let snapshot = running.server.catalog_snapshot();
        assert!(snapshot.tables.contains_key("drop_index_probe"));
        assert!(!snapshot.indexes.contains_key("drop_index_probe_idx"));
        assert!(
            !snapshot
                .descriptions
                .contains_key(&(index_oid, Oid::new(PG_CLASS_OID), 0))
        );
        shutdown(running).await;
    }
}
