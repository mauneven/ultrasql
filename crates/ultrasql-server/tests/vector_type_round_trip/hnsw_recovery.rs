//! HNSW index durability: opclass survival across DML/vacuum, page-backed restarts, crash-during-build, recovery targets, and corrupt/torn WAL handling.

use super::*;

#[tokio::test]
async fn hnsw_l2_opclass_survives_insert_update_delete_and_vacuum() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE ann_live (id INT NOT NULL, embedding VECTOR(3))")
        .await
        .expect("create vector table");
    client
        .batch_execute(
            "INSERT INTO ann_live VALUES \
             (1, '[9,0,0]'), \
             (2, '[3,0,0]'), \
             (3, '[6,0,0]')",
        )
        .await
        .expect("insert initial vectors");
    client
        .batch_execute(
            "CREATE INDEX ann_live_embedding_hnsw \
             ON ann_live USING hnsw (embedding vector_l2_ops)",
        )
        .await
        .expect("create hnsw l2 opclass index");
    client
        .batch_execute("INSERT INTO ann_live VALUES (4, '[0,0,0]')")
        .await
        .expect("insert into maintained hnsw");
    client
        .batch_execute("UPDATE ann_live SET embedding = VECTOR '[1,0,0]' WHERE id = 2")
        .await
        .expect("update maintained hnsw");
    client
        .batch_execute("DELETE FROM ann_live WHERE id = 1")
        .await
        .expect("delete maintained hnsw");
    client
        .batch_execute("VACUUM ann_live")
        .await
        .expect("vacuum compacts hnsw tombstones");

    let messages = client
        .simple_query(
            "SELECT id FROM ann_live \
             ORDER BY embedding <-> VECTOR '[0,0,0]' LIMIT 3",
        )
        .await
        .expect("top-k query");
    let rows = simple_rows(&messages);
    assert_eq!(
        rows,
        vec![
            vec!["4".to_owned()],
            vec!["2".to_owned()],
            vec!["3".to_owned()]
        ]
    );

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn hnsw_page_backed_index_survives_sql_restart() {
    let dir = tempfile::tempdir().expect("tempdir");

    {
        let running = start_persistent_server(dir.path(), "vector_type_test").await;
        let client = &running.client;
        client
            .batch_execute("CREATE TABLE ann_restart (id INT NOT NULL, embedding VECTOR(3))")
            .await
            .expect("create vector table");
        client
            .batch_execute(
                "INSERT INTO ann_restart VALUES \
                 (1, '[9,0,0]'), \
                 (2, '[3,0,0]'), \
                 (3, '[6,0,0]'), \
                 (4, '[0,0,0]')",
            )
            .await
            .expect("insert vectors");
        client
            .batch_execute(
                "CREATE INDEX ann_restart_embedding_hnsw \
                 ON ann_restart USING hnsw (embedding vector_l2_ops)",
            )
            .await
            .expect("create hnsw index");
        graceful_shutdown(running).await;
    }

    {
        let running = start_persistent_server(dir.path(), "vector_type_test").await;
        let client = &running.client;
        let messages = client
            .simple_query(
                "SELECT id FROM ann_restart \
                 ORDER BY embedding <-> VECTOR '[0,0,0]' LIMIT 3",
            )
            .await
            .expect("top-k after restart");
        let rows = simple_rows(&messages);
        assert_eq!(
            rows,
            vec![
                vec!["4".to_owned()],
                vec!["2".to_owned()],
                vec!["3".to_owned()]
            ]
        );

        let messages = client
            .simple_query(
                "EXPLAIN ANALYZE SELECT id FROM ann_restart \
                 ORDER BY embedding <-> VECTOR '[0,0,0]' LIMIT 3",
            )
            .await
            .expect("explain after restart");
        let text = simple_rows(&messages)
            .into_iter()
            .flatten()
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            text.contains("Vector Index: selected ann_restart_embedding_hnsw (page-backed hnsw)"),
            "EXPLAIN ANALYZE must report page-backed HNSW after restart, got: {text}"
        );

        graceful_shutdown(running).await;
    }
}

#[tokio::test]
async fn hnsw_crash_during_index_build_uses_exact_scan_after_restart() {
    let dir = tempfile::tempdir().expect("tempdir");

    {
        let running = start_persistent_server(dir.path(), "vector_type_test").await;
        let client = &running.client;
        client
            .batch_execute("CREATE TABLE ann_hnsw_crash (id INT NOT NULL, embedding VECTOR(3))")
            .await
            .expect("create vector table");
        client
            .batch_execute(
                "INSERT INTO ann_hnsw_crash VALUES \
                 (1, '[9,0,0]'), \
                 (2, '[1,0,0]'), \
                 (3, '[2,0,0]'), \
                 (4, '[0,0,0]')",
            )
            .await
            .expect("insert vectors");
        graceful_shutdown(running).await;
    }

    {
        let (client, _conn, server_handle) =
            start_crash_persistent_server_and_connect(dir.path()).await;
        client
            .batch_execute(
                "CREATE INDEX ann_hnsw_crash_embedding_idx \
                 ON ann_hnsw_crash USING hnsw (embedding vector_l2_ops)",
            )
            .await
            .expect("create hnsw index");
        shutdown(client, server_handle).await;
    }

    truncate_wal_before_first(dir.path(), RecordType::HnswOp);

    {
        let running = start_persistent_server(dir.path(), "vector_type_test").await;
        let client = &running.client;
        let messages = client
            .simple_query(
                "SELECT id FROM ann_hnsw_crash \
                 ORDER BY embedding <-> VECTOR '[0,0,0]' LIMIT 3",
            )
            .await
            .expect("top-k after partial hnsw build WAL");
        let rows = simple_rows(&messages);
        assert_eq!(
            rows,
            vec![
                vec!["4".to_owned()],
                vec!["2".to_owned()],
                vec!["3".to_owned()]
            ],
            "partial HNSW build must not produce wrong top-k results"
        );

        let messages = client
            .simple_query(
                "EXPLAIN ANALYZE SELECT id FROM ann_hnsw_crash \
                 ORDER BY embedding <-> VECTOR '[0,0,0]' LIMIT 3",
            )
            .await
            .expect("explain after partial hnsw build WAL");
        let text = simple_rows(&messages)
            .into_iter()
            .flatten()
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            !text.contains("selected ann_hnsw_crash_embedding_idx"),
            "partial HNSW build must be ignored after restart, got: {text}"
        );

        graceful_shutdown(running).await;
    }

    let table_runtime_metadata =
        fs::read_to_string(dir.path().join("pg_table_runtime.meta")).expect("runtime metadata");
    assert!(
        !table_runtime_metadata
            .lines()
            .any(|line| line.starts_with("index\t")),
        "stale ANN runtime index metadata must be scrubbed after crash recovery: {table_runtime_metadata}"
    );
}

#[tokio::test]
async fn hnsw_recovery_target_before_index_wal_uses_exact_scan_after_restart() {
    let dir = tempfile::tempdir().expect("tempdir");

    {
        let running = start_persistent_server(dir.path(), "vector_type_test").await;
        let client = &running.client;
        client
            .batch_execute("CREATE TABLE ann_hnsw_pitr (id INT NOT NULL, embedding VECTOR(3))")
            .await
            .expect("create vector table");
        client
            .batch_execute(
                "INSERT INTO ann_hnsw_pitr VALUES \
                 (1, '[9,0,0]'), \
                 (2, '[1,0,0]'), \
                 (3, '[2,0,0]'), \
                 (4, '[0,0,0]')",
            )
            .await
            .expect("insert vectors");
        graceful_shutdown(running).await;
    }

    {
        let (client, _conn, server_handle) =
            start_crash_persistent_server_and_connect(dir.path()).await;
        client
            .batch_execute(
                "CREATE INDEX ann_hnsw_pitr_embedding_idx \
                 ON ann_hnsw_pitr USING hnsw (embedding vector_l2_ops)",
            )
            .await
            .expect("create hnsw index");
        shutdown(client, server_handle).await;
    }

    let target = lsn_before_first_wal_record(dir.path(), RecordType::HnswOp);
    fs::write(
        dir.path().join("recovery.targets"),
        format!("recovery_target_lsn = '{target}'\n"),
    )
    .expect("write recovery target");

    {
        let running = start_persistent_server(dir.path(), "vector_type_test").await;
        let client = &running.client;
        let messages = client
            .simple_query(
                "EXPLAIN ANALYZE SELECT id FROM ann_hnsw_pitr \
                 ORDER BY embedding <-> VECTOR '[0,0,0]' LIMIT 3",
            )
            .await
            .expect("explain after PITR before hnsw WAL");
        let text = simple_rows(&messages)
            .into_iter()
            .flatten()
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            !text.contains("selected ann_hnsw_pitr_embedding_idx"),
            "PITR before HNSW WAL must not replay post-target index sidecar, got: {text}"
        );

        graceful_shutdown(running).await;
    }
}

#[tokio::test]
async fn hnsw_recovery_target_ignores_corrupt_post_target_index_wal() {
    let dir = tempfile::tempdir().expect("tempdir");

    {
        let running = start_persistent_server(dir.path(), "vector_type_test").await;
        let client = &running.client;
        client
            .batch_execute("CREATE TABLE ann_hnsw_pitr_post (id INT NOT NULL, embedding VECTOR(3))")
            .await
            .expect("create vector table");
        client
            .batch_execute(
                "INSERT INTO ann_hnsw_pitr_post VALUES \
                 (1, '[9,0,0]'), \
                 (2, '[1,0,0]'), \
                 (3, '[2,0,0]'), \
                 (4, '[0,0,0]')",
            )
            .await
            .expect("insert vectors");
        client
            .batch_execute(
                "CREATE INDEX ann_hnsw_pitr_post_embedding_idx \
                 ON ann_hnsw_pitr_post USING hnsw (embedding vector_l2_ops)",
            )
            .await
            .expect("create hnsw index");
        graceful_shutdown(running).await;
    }

    let target = wal_end_lsn(dir.path());

    {
        let (client, _conn, server_handle) =
            start_crash_persistent_server_and_connect(dir.path()).await;
        client
            .batch_execute("INSERT INTO ann_hnsw_pitr_post VALUES (5, '[0.5,0,0]')")
            .await
            .expect("insert post-target vector");
        shutdown(client, server_handle).await;
    }

    corrupt_first_vector_wal_payload_after(dir.path(), RecordType::HnswOp, target);
    fs::write(
        dir.path().join("recovery.targets"),
        format!("recovery_target_lsn = '{target}'\n"),
    )
    .expect("write recovery target");

    {
        let running = start_persistent_server(dir.path(), "vector_type_test").await;
        let client = &running.client;
        let messages = client
            .simple_query(
                "EXPLAIN ANALYZE SELECT id FROM ann_hnsw_pitr_post \
                 ORDER BY embedding <-> VECTOR '[0,0,0]' LIMIT 3",
            )
            .await
            .expect("explain after PITR before corrupt post-target hnsw WAL");
        let text = simple_rows(&messages)
            .into_iter()
            .flatten()
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            text.contains("Vector Index: selected ann_hnsw_pitr_post_embedding_idx"),
            "PITR must ignore corrupt post-target HNSW WAL and keep target index valid, got: {text}"
        );

        graceful_shutdown(running).await;
    }
}

#[tokio::test]
async fn hnsw_corrupt_index_wal_marks_index_unavailable_after_restart() {
    let dir = tempfile::tempdir().expect("tempdir");

    {
        let running = start_persistent_server(dir.path(), "vector_type_test").await;
        let client = &running.client;
        client
            .batch_execute("CREATE TABLE ann_hnsw_corrupt (id INT NOT NULL, embedding VECTOR(3))")
            .await
            .expect("create vector table");
        client
            .batch_execute(
                "INSERT INTO ann_hnsw_corrupt VALUES \
                 (1, '[9,0,0]'), \
                 (2, '[1,0,0]'), \
                 (3, '[2,0,0]'), \
                 (4, '[0,0,0]')",
            )
            .await
            .expect("insert vectors");
        client
            .batch_execute(
                "CREATE INDEX ann_hnsw_corrupt_embedding_idx \
                 ON ann_hnsw_corrupt USING hnsw (embedding vector_l2_ops)",
            )
            .await
            .expect("create hnsw index");
        graceful_shutdown(running).await;
    }

    corrupt_first_vector_wal_payload(dir.path(), RecordType::HnswOp);

    {
        let running = start_persistent_server(dir.path(), "vector_type_test").await;
        let client = &running.client;
        let messages = client
            .simple_query(
                "SELECT id FROM ann_hnsw_corrupt \
                 ORDER BY embedding <-> VECTOR '[0,0,0]' LIMIT 3",
            )
            .await
            .expect("top-k after corrupt hnsw WAL");
        let rows = simple_rows(&messages);
        assert_eq!(
            rows,
            vec![
                vec!["4".to_owned()],
                vec!["2".to_owned()],
                vec!["3".to_owned()]
            ],
            "corrupt HNSW WAL must not produce wrong top-k results"
        );

        let messages = client
            .simple_query(
                "EXPLAIN ANALYZE SELECT id FROM ann_hnsw_corrupt \
                 ORDER BY embedding <-> VECTOR '[0,0,0]' LIMIT 3",
            )
            .await
            .expect("explain after corrupt hnsw WAL");
        let text = simple_rows(&messages)
            .into_iter()
            .flatten()
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            text.contains("skipped ann_hnsw_corrupt_embedding_idx: page-backed hnsw unavailable"),
            "corrupt HNSW WAL must mark index unavailable, got: {text}"
        );

        graceful_shutdown(running).await;
    }
}

#[tokio::test]
async fn hnsw_torn_index_wal_tail_uses_exact_scan_after_restart() {
    let dir = tempfile::tempdir().expect("tempdir");

    {
        let running = start_persistent_server(dir.path(), "vector_type_test").await;
        let client = &running.client;
        client
            .batch_execute("CREATE TABLE ann_hnsw_torn (id INT NOT NULL, embedding VECTOR(3))")
            .await
            .expect("create vector table");
        client
            .batch_execute(
                "INSERT INTO ann_hnsw_torn VALUES \
                 (1, '[9,0,0]'), \
                 (2, '[1,0,0]'), \
                 (3, '[2,0,0]'), \
                 (4, '[0,0,0]')",
            )
            .await
            .expect("insert vectors");
        graceful_shutdown(running).await;
    }

    {
        let (client, _conn, server_handle) =
            start_crash_persistent_server_and_connect(dir.path()).await;
        client
            .batch_execute(
                "CREATE INDEX ann_hnsw_torn_embedding_idx \
                 ON ann_hnsw_torn USING hnsw (embedding vector_l2_ops)",
            )
            .await
            .expect("create hnsw index");
        shutdown(client, server_handle).await;
    }

    truncate_inside_first_wal_record(dir.path(), RecordType::HnswOp);

    {
        let running = start_persistent_server(dir.path(), "vector_type_test").await;
        let client = &running.client;
        let messages = client
            .simple_query(
                "SELECT id FROM ann_hnsw_torn \
                 ORDER BY embedding <-> VECTOR '[0,0,0]' LIMIT 3",
            )
            .await
            .expect("top-k after torn hnsw WAL");
        let rows = simple_rows(&messages);
        assert_eq!(
            rows,
            vec![
                vec!["4".to_owned()],
                vec!["2".to_owned()],
                vec!["3".to_owned()]
            ],
            "torn HNSW WAL tail must not produce wrong top-k results"
        );

        let messages = client
            .simple_query(
                "EXPLAIN ANALYZE SELECT id FROM ann_hnsw_torn \
                 ORDER BY embedding <-> VECTOR '[0,0,0]' LIMIT 3",
            )
            .await
            .expect("explain after torn hnsw WAL");
        let text = simple_rows(&messages)
            .into_iter()
            .flatten()
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            !text.contains("selected ann_hnsw_torn_embedding_idx"),
            "torn HNSW WAL must not select incomplete index, got: {text}"
        );

        graceful_shutdown(running).await;
    }
}
