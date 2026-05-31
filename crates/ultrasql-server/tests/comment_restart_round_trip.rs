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
async fn comment_respects_schema_qualifier() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let running = start_persistent_server(data_dir.path(), "comment_qualifier_guard").await;

    running
        .client
        .batch_execute(
            "CREATE SCHEMA app; \
             CREATE TABLE guarded_comment (id INT, label TEXT); \
             CREATE INDEX guarded_comment_idx ON guarded_comment(id)",
        )
        .await
        .expect("create public table and separate schema");

    let snapshot = running.server.catalog_snapshot();
    let table_oid = snapshot
        .tables
        .get("guarded_comment")
        .expect("table before rejected comments")
        .oid;
    let index_oid = snapshot
        .indexes
        .get("guarded_comment_idx")
        .expect("index before rejected comments")
        .oid;

    running
        .client
        .batch_execute("COMMENT ON TABLE app.guarded_comment IS 'wrong table docs'")
        .await
        .expect_err("qualified table comment must not resolve public table");

    running
        .client
        .batch_execute("COMMENT ON COLUMN app.guarded_comment.label IS 'wrong column docs'")
        .await
        .expect_err("qualified column comment must not resolve public table");

    running
        .client
        .batch_execute("COMMENT ON INDEX app.guarded_comment_idx IS 'wrong index docs'")
        .await
        .expect_err("qualified index comment must not resolve public index");

    let snapshot = running.server.catalog_snapshot();
    assert!(
        !snapshot
            .descriptions
            .contains_key(&(table_oid, Oid::new(PG_CLASS_OID), 0)),
        "wrong-qualified table comment must not write public table description"
    );
    assert!(
        !snapshot
            .descriptions
            .contains_key(&(table_oid, Oid::new(PG_CLASS_OID), 2)),
        "wrong-qualified column comment must not write public column description"
    );
    assert!(
        !snapshot
            .descriptions
            .contains_key(&(index_oid, Oid::new(PG_CLASS_OID), 0)),
        "wrong-qualified index comment must not write public index description"
    );

    running
        .client
        .batch_execute("DROP TABLE guarded_comment; DROP SCHEMA app")
        .await
        .expect("cleanup COMMENT qualifier guard");

    shutdown(running).await;
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_table_clears_dependent_index_comments() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let table_oid;
    let index_oid;

    {
        let running = start_persistent_server(data_dir.path(), "comment_restart_test").await;
        running
            .client
            .batch_execute("CREATE TABLE comment_index_drop (id INT)")
            .await
            .expect("create table");
        running
            .client
            .batch_execute("CREATE INDEX comment_index_drop_idx ON comment_index_drop(id)")
            .await
            .expect("create index");
        let snapshot = running.server.catalog_snapshot();
        table_oid = snapshot
            .tables
            .get("comment_index_drop")
            .expect("table before drop")
            .oid;
        index_oid = snapshot
            .indexes
            .get("comment_index_drop_idx")
            .expect("index before drop")
            .oid;
        running
            .client
            .batch_execute("COMMENT ON TABLE comment_index_drop IS 'drop table docs'")
            .await
            .expect("comment on table");
        running
            .client
            .batch_execute("COMMENT ON INDEX comment_index_drop_idx IS 'drop index docs'")
            .await
            .expect("comment on index");
        running
            .client
            .batch_execute("DROP TABLE comment_index_drop")
            .await
            .expect("drop table");

        let snapshot = running.server.catalog_snapshot();
        assert!(
            !snapshot
                .descriptions
                .contains_key(&(table_oid, Oid::new(PG_CLASS_OID), 0)),
            "DROP TABLE must clear table comments immediately"
        );
        assert!(
            !snapshot
                .descriptions
                .contains_key(&(index_oid, Oid::new(PG_CLASS_OID), 0)),
            "DROP TABLE must clear dependent index comments immediately"
        );
        shutdown(running).await;
    }

    {
        let running = start_persistent_server(data_dir.path(), "comment_restart_test").await;
        let snapshot = running.server.catalog_snapshot();
        assert!(!snapshot.tables.contains_key("comment_index_drop"));
        assert!(!snapshot.indexes.contains_key("comment_index_drop_idx"));
        assert!(
            !snapshot
                .descriptions
                .contains_key(&(table_oid, Oid::new(PG_CLASS_OID), 0))
        );
        assert!(
            !snapshot
                .descriptions
                .contains_key(&(index_oid, Oid::new(PG_CLASS_OID), 0))
        );
        shutdown(running).await;
    }
}
