//! Restart checks for durable B-tree index metadata and rebuilt pages.

mod support;

use support::{shutdown, start_persistent_server};

#[tokio::test]
async fn btree_index_restarts_with_rebuilt_pages() {
    let dir = tempfile::tempdir().expect("tempdir");

    {
        let running = start_persistent_server(dir.path(), "btree_restart_test").await;
        running
            .client
            .batch_execute("CREATE TABLE backup_restore_smoke (id INT, payload TEXT)")
            .await
            .expect("create table");
        running
            .client
            .batch_execute(
                "INSERT INTO backup_restore_smoke VALUES
                    (1, 'alpha'),
                    (2, 'bravo'),
                    (3, 'charlie')",
            )
            .await
            .expect("seed rows");
        running
            .client
            .batch_execute("CREATE INDEX backup_restore_smoke_id_idx ON backup_restore_smoke (id)")
            .await
            .expect("create index");
        let before: String = running
            .client
            .query_one("SELECT payload FROM backup_restore_smoke WHERE id = 2", &[])
            .await
            .expect("query before restart")
            .get(0);
        assert_eq!(before, "bravo");

        shutdown(running).await;
    }

    {
        let running = start_persistent_server(dir.path(), "btree_restart_test").await;
        let count: i64 = running
            .client
            .query_one("SELECT COUNT(*) FROM backup_restore_smoke", &[])
            .await
            .expect("count after restart")
            .get(0);
        assert_eq!(count, 3);
        let after: String = running
            .client
            .query_one("SELECT payload FROM backup_restore_smoke WHERE id = 2", &[])
            .await
            .expect("index query after restart")
            .get(0);
        assert_eq!(after, "bravo");

        shutdown(running).await;
    }
}
