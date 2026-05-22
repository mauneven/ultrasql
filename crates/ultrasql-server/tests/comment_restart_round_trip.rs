//! Persistent `COMMENT ON` restart coverage through the PostgreSQL wire path.

use ultrasql_catalog::bootstrap::PG_CLASS_OID;
use ultrasql_core::Oid;

mod support;

use support::{shutdown, start_persistent_server};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn table_comment_survives_restart() {
    let data_dir = tempfile::TempDir::new().unwrap();

    {
        let running = start_persistent_server(data_dir.path(), "comment_restart_test").await;
        running
            .client
            .batch_execute("CREATE TABLE comment_restart (id INT)")
            .await
            .expect("create");
        running
            .client
            .batch_execute("COMMENT ON TABLE comment_restart IS 'durable table docs'")
            .await
            .expect("comment");
        shutdown(running).await;
    }

    {
        let running = start_persistent_server(data_dir.path(), "comment_restart_test").await;
        let snapshot = running.server.catalog_snapshot();
        let table = snapshot
            .tables
            .get("comment_restart")
            .expect("table after restart");
        let row = snapshot
            .descriptions
            .get(&(table.oid, Oid::new(PG_CLASS_OID), 0))
            .expect("table comment after restart");
        assert_eq!(row.description, "durable table docs");
        shutdown(running).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cleared_table_comment_stays_cleared_after_restart() {
    let data_dir = tempfile::TempDir::new().unwrap();

    {
        let running = start_persistent_server(data_dir.path(), "comment_restart_test").await;
        running
            .client
            .batch_execute("CREATE TABLE comment_clear_restart (id INT)")
            .await
            .expect("create");
        running
            .client
            .batch_execute("COMMENT ON TABLE comment_clear_restart IS 'temporary docs'")
            .await
            .expect("comment");
        running
            .client
            .batch_execute("COMMENT ON TABLE comment_clear_restart IS NULL")
            .await
            .expect("clear comment");
        shutdown(running).await;
    }

    {
        let running = start_persistent_server(data_dir.path(), "comment_restart_test").await;
        let snapshot = running.server.catalog_snapshot();
        let table = snapshot
            .tables
            .get("comment_clear_restart")
            .expect("table after restart");
        assert!(
            !snapshot
                .descriptions
                .contains_key(&(table.oid, Oid::new(PG_CLASS_OID), 0))
        );
        shutdown(running).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_table_comment_does_not_survive_restart() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let dropped_oid;

    {
        let running = start_persistent_server(data_dir.path(), "comment_restart_test").await;
        running
            .client
            .batch_execute("CREATE TABLE comment_drop_restart (id INT)")
            .await
            .expect("create");
        dropped_oid = running
            .server
            .catalog_snapshot()
            .tables
            .get("comment_drop_restart")
            .expect("table before drop")
            .oid;
        running
            .client
            .batch_execute("COMMENT ON TABLE comment_drop_restart IS 'drop me'")
            .await
            .expect("comment");
        running
            .client
            .batch_execute("DROP TABLE comment_drop_restart")
            .await
            .expect("drop");
        shutdown(running).await;
    }

    {
        let running = start_persistent_server(data_dir.path(), "comment_restart_test").await;
        let snapshot = running.server.catalog_snapshot();
        assert!(!snapshot.tables.contains_key("comment_drop_restart"));
        assert!(
            !snapshot
                .descriptions
                .contains_key(&(dropped_oid, Oid::new(PG_CLASS_OID), 0))
        );
        shutdown(running).await;
    }
}
