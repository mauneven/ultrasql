//! ANN query and recovery paths: filtered recheck, EXPLAIN counters, IVFFlat crash/corrupt/torn WAL handling, and restart rebuild replay.

use super::*;

#[tokio::test]
async fn exact_vector_top_k_explain_avoids_physical_sort_and_reports_fallback() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute(
            "CREATE TABLE exact_topk_cert (id INT NOT NULL, tenant INT, embedding VECTOR(2))",
        )
        .await
        .expect("create vector table");
    client
        .batch_execute(
            "INSERT INTO exact_topk_cert VALUES \
             (1, 1, '[3,0]'), \
             (2, 1, '[0.2,0]'), \
             (3, 1, '[0,1]'), \
             (4, 2, '[0,0]'), \
             (5, 2, '[0.1,0]')",
        )
        .await
        .expect("insert vectors");

    let messages = client
        .simple_query(
            "SELECT id FROM exact_topk_cert \
             WHERE tenant = 1 \
             ORDER BY embedding <-> VECTOR '[0,0]' LIMIT 2",
        )
        .await
        .expect("exact vector top-k");
    assert_eq!(
        simple_rows(&messages),
        vec![vec!["2".to_owned()], vec!["3".to_owned()]]
    );

    let messages = client
        .simple_query(
            "EXPLAIN ANALYZE SELECT id FROM exact_topk_cert \
             WHERE tenant = 1 \
             ORDER BY embedding <-> VECTOR '[0,0]' LIMIT 2",
        )
        .await
        .expect("exact vector explain");
    let text = simple_rows(&messages)
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join("\n");
    for required in [
        "SIMD Kernel: ultrasql-vec exact_top_k_f32 kernel",
        "method=exact",
        "fallback_used=true",
        "fallback_reason=no matching vector index",
        "kernel=exact_top_k_f32",
        "full_sort=false",
    ] {
        assert!(
            text.contains(required),
            "EXPLAIN ANALYZE missing {required}, got: {text}"
        );
    }
    assert!(
        !text.contains("operator=Sort"),
        "exact vector top-k must not lower to physical Sort, got: {text}"
    );

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn ann_explain_analyze_reports_vector_index_counters() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE ann_explain_cert (id INT NOT NULL, embedding VECTOR(2))")
        .await
        .expect("create vector table");
    client
        .batch_execute(
            "INSERT INTO ann_explain_cert VALUES \
             (1, '[0,0]'), \
             (2, '[1,0]'), \
             (3, '[2,0]')",
        )
        .await
        .expect("insert vectors");
    client
        .batch_execute(
            "CREATE INDEX ann_explain_cert_hnsw \
             ON ann_explain_cert USING hnsw (embedding vector_l2_ops)",
        )
        .await
        .expect("create hnsw index");

    let messages = client
        .simple_query(
            "EXPLAIN ANALYZE SELECT id FROM ann_explain_cert \
             ORDER BY embedding <-> VECTOR '[0,0]' LIMIT 2",
        )
        .await
        .expect("ann explain");
    let text = simple_rows(&messages)
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join("\n");
    for required in [
        "method=hnsw",
        "candidates_scanned=3",
        "exact_rerank_count=3",
        "recall_mode=n/a",
        "fallback_used=false",
        "deleted_candidates_skipped=0",
    ] {
        assert!(
            text.contains(required),
            "EXPLAIN ANALYZE missing {required}, got: {text}"
        );
    }

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn ivfflat_crash_during_index_build_uses_exact_scan_after_restart() {
    let dir = tempfile::tempdir().expect("tempdir");

    {
        let running = start_persistent_server(dir.path(), "vector_type_test").await;
        let client = &running.client;
        client
            .batch_execute("CREATE TABLE ann_ivf_crash (id INT NOT NULL, embedding VECTOR(2))")
            .await
            .expect("create vector table");
        client
            .batch_execute(
                "INSERT INTO ann_ivf_crash VALUES \
                 (1, '[9,0]'), \
                 (2, '[1,0]'), \
                 (3, '[2,0]'), \
                 (4, '[0,0]')",
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
                "CREATE INDEX ann_ivf_crash_embedding_idx \
                 ON ann_ivf_crash USING ivfflat (embedding vector_l2_ops) \
                 WITH (lists = 2, probes = 1)",
            )
            .await
            .expect("create ivfflat index");
        shutdown(client, server_handle).await;
    }

    truncate_wal_before_first(dir.path(), RecordType::IvfFlatOp);

    {
        let running = start_persistent_server(dir.path(), "vector_type_test").await;
        let client = &running.client;
        let messages = client
            .simple_query(
                "SELECT id FROM ann_ivf_crash \
                 ORDER BY embedding <-> VECTOR '[0,0]' LIMIT 3",
            )
            .await
            .expect("top-k after partial ivfflat build WAL");
        let rows = simple_rows(&messages);
        assert_eq!(
            rows,
            vec![
                vec!["4".to_owned()],
                vec!["2".to_owned()],
                vec!["3".to_owned()]
            ],
            "partial IVFFlat build must not produce wrong top-k results"
        );

        let messages = client
            .simple_query(
                "EXPLAIN ANALYZE SELECT id FROM ann_ivf_crash \
                 ORDER BY embedding <-> VECTOR '[0,0]' LIMIT 3",
            )
            .await
            .expect("explain after partial ivfflat build WAL");
        let text = simple_rows(&messages)
            .into_iter()
            .flatten()
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            !text.contains("selected ann_ivf_crash_embedding_idx"),
            "partial IVFFlat build must be ignored after restart, got: {text}"
        );

        graceful_shutdown(running).await;
    }
}

#[tokio::test]
async fn ivfflat_corrupt_index_wal_marks_index_unavailable_after_restart() {
    let dir = tempfile::tempdir().expect("tempdir");

    {
        let running = start_persistent_server(dir.path(), "vector_type_test").await;
        let client = &running.client;
        client
            .batch_execute("CREATE TABLE ann_ivf_corrupt (id INT NOT NULL, embedding VECTOR(2))")
            .await
            .expect("create vector table");
        client
            .batch_execute(
                "INSERT INTO ann_ivf_corrupt VALUES \
                 (1, '[9,0]'), \
                 (2, '[1,0]'), \
                 (3, '[2,0]'), \
                 (4, '[0,0]')",
            )
            .await
            .expect("insert vectors");
        client
            .batch_execute(
                "CREATE INDEX ann_ivf_corrupt_embedding_idx \
                 ON ann_ivf_corrupt USING ivfflat (embedding vector_l2_ops) \
                 WITH (lists = 2, probes = 1)",
            )
            .await
            .expect("create ivfflat index");
        graceful_shutdown(running).await;
    }

    corrupt_first_vector_wal_payload(dir.path(), RecordType::IvfFlatOp);

    {
        let running = start_persistent_server(dir.path(), "vector_type_test").await;
        let client = &running.client;
        let messages = client
            .simple_query(
                "SELECT id FROM ann_ivf_corrupt \
                 ORDER BY embedding <-> VECTOR '[0,0]' LIMIT 3",
            )
            .await
            .expect("top-k after corrupt ivfflat WAL");
        let rows = simple_rows(&messages);
        assert_eq!(
            rows,
            vec![
                vec!["4".to_owned()],
                vec!["2".to_owned()],
                vec!["3".to_owned()]
            ],
            "corrupt IVFFlat WAL must not produce wrong top-k results"
        );

        let messages = client
            .simple_query(
                "EXPLAIN ANALYZE SELECT id FROM ann_ivf_corrupt \
                 ORDER BY embedding <-> VECTOR '[0,0]' LIMIT 3",
            )
            .await
            .expect("explain after corrupt ivfflat WAL");
        let text = simple_rows(&messages)
            .into_iter()
            .flatten()
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            text.contains("skipped ann_ivf_corrupt_embedding_idx: page-backed ivfflat unavailable"),
            "corrupt IVFFlat WAL must mark index unavailable, got: {text}"
        );

        graceful_shutdown(running).await;
    }
}

#[tokio::test]
async fn ivfflat_torn_index_wal_tail_uses_exact_scan_after_restart() {
    let dir = tempfile::tempdir().expect("tempdir");

    {
        let running = start_persistent_server(dir.path(), "vector_type_test").await;
        let client = &running.client;
        client
            .batch_execute("CREATE TABLE ann_ivf_torn (id INT NOT NULL, embedding VECTOR(2))")
            .await
            .expect("create vector table");
        client
            .batch_execute(
                "INSERT INTO ann_ivf_torn VALUES \
                 (1, '[9,0]'), \
                 (2, '[1,0]'), \
                 (3, '[2,0]'), \
                 (4, '[0,0]')",
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
                "CREATE INDEX ann_ivf_torn_embedding_idx \
                 ON ann_ivf_torn USING ivfflat (embedding vector_l2_ops) \
                 WITH (lists = 2, probes = 1)",
            )
            .await
            .expect("create ivfflat index");
        shutdown(client, server_handle).await;
    }

    truncate_inside_first_wal_record(dir.path(), RecordType::IvfFlatOp);

    {
        let running = start_persistent_server(dir.path(), "vector_type_test").await;
        let client = &running.client;
        let messages = client
            .simple_query(
                "SELECT id FROM ann_ivf_torn \
                 ORDER BY embedding <-> VECTOR '[0,0]' LIMIT 3",
            )
            .await
            .expect("top-k after torn ivfflat WAL");
        let rows = simple_rows(&messages);
        assert_eq!(
            rows,
            vec![
                vec!["4".to_owned()],
                vec!["2".to_owned()],
                vec!["3".to_owned()]
            ],
            "torn IVFFlat WAL tail must not produce wrong top-k results"
        );

        let messages = client
            .simple_query(
                "EXPLAIN ANALYZE SELECT id FROM ann_ivf_torn \
                 ORDER BY embedding <-> VECTOR '[0,0]' LIMIT 3",
            )
            .await
            .expect("explain after torn ivfflat WAL");
        let text = simple_rows(&messages)
            .into_iter()
            .flatten()
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            !text.contains("selected ann_ivf_torn_embedding_idx"),
            "torn IVFFlat WAL must not select incomplete index, got: {text}"
        );

        graceful_shutdown(running).await;
    }
}

#[tokio::test]
async fn ann_restart_rebuild_replays_dml_wal_for_hnsw_and_ivfflat() {
    let dir = tempfile::tempdir().expect("tempdir");

    {
        let running = start_persistent_server(dir.path(), "vector_type_test").await;
        let client = &running.client;
        client
            .batch_execute("CREATE TABLE ann_hnsw_rebuild (id INT NOT NULL, embedding VECTOR(2))")
            .await
            .expect("create hnsw rebuild table");
        client
            .batch_execute(
                "INSERT INTO ann_hnsw_rebuild VALUES \
                 (1, '[9,0]'), \
                 (2, '[3,0]'), \
                 (3, '[6,0]')",
            )
            .await
            .expect("insert hnsw rebuild rows");
        client
            .batch_execute("CREATE TABLE ann_ivf_rebuild (id INT NOT NULL, embedding VECTOR(2))")
            .await
            .expect("create ivfflat rebuild table");
        client
            .batch_execute(
                "INSERT INTO ann_ivf_rebuild VALUES \
                 (1, '[9,0]'), \
                 (2, '[3,0]'), \
                 (3, '[6,0]')",
            )
            .await
            .expect("insert ivfflat rebuild rows");
        graceful_shutdown(running).await;
    }

    {
        let (client, _conn, server_handle) =
            start_crash_persistent_server_and_connect(dir.path()).await;
        client
            .batch_execute(
                "CREATE INDEX ann_hnsw_rebuild_idx \
                 ON ann_hnsw_rebuild USING hnsw (embedding vector_l2_ops)",
            )
            .await
            .expect("create hnsw rebuild index");
        client
            .batch_execute(
                "CREATE INDEX ann_ivf_rebuild_idx \
                 ON ann_ivf_rebuild USING ivfflat (embedding vector_l2_ops) \
                 WITH (lists = 2, probes = 2)",
            )
            .await
            .expect("create ivfflat rebuild index");
        client
            .batch_execute("INSERT INTO ann_hnsw_rebuild VALUES (4, '[0,0]')")
            .await
            .expect("insert into hnsw rebuild");
        client
            .batch_execute("UPDATE ann_hnsw_rebuild SET embedding = VECTOR '[1,0]' WHERE id = 2")
            .await
            .expect("update hnsw rebuild");
        client
            .batch_execute("DELETE FROM ann_hnsw_rebuild WHERE id = 1")
            .await
            .expect("delete hnsw rebuild");
        client
            .batch_execute("VACUUM ann_hnsw_rebuild")
            .await
            .expect("vacuum hnsw rebuild");
        client
            .batch_execute("INSERT INTO ann_ivf_rebuild VALUES (4, '[0,0]')")
            .await
            .expect("insert into ivfflat rebuild");
        client
            .batch_execute("UPDATE ann_ivf_rebuild SET embedding = VECTOR '[1,0]' WHERE id = 2")
            .await
            .expect("update ivfflat rebuild");
        client
            .batch_execute("DELETE FROM ann_ivf_rebuild WHERE id = 1")
            .await
            .expect("delete ivfflat rebuild");
        client
            .batch_execute("VACUUM ann_ivf_rebuild")
            .await
            .expect("vacuum ivfflat rebuild");
        shutdown(client, server_handle).await;
    }

    {
        let running = start_persistent_server(dir.path(), "vector_type_test").await;
        let client = &running.client;

        let messages = client
            .simple_query(
                "SELECT id FROM ann_hnsw_rebuild \
                 ORDER BY embedding <-> VECTOR '[0,0]' LIMIT 3",
            )
            .await
            .expect("hnsw top-k after restart rebuild");
        assert_eq!(
            simple_rows(&messages),
            vec![
                vec!["4".to_owned()],
                vec!["2".to_owned()],
                vec!["3".to_owned()]
            ],
            "replayed HNSW DML must preserve exact top-k visibility"
        );
        let messages = client
            .simple_query(
                "EXPLAIN ANALYZE SELECT id FROM ann_hnsw_rebuild \
                 ORDER BY embedding <-> VECTOR '[0,0]' LIMIT 3",
            )
            .await
            .expect("hnsw explain after restart rebuild");
        let text = simple_rows(&messages)
            .into_iter()
            .flatten()
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            text.contains("Vector Index: selected ann_hnsw_rebuild_idx (page-backed hnsw)"),
            "restart rebuild must select recovered HNSW index, got: {text}"
        );

        let messages = client
            .simple_query(
                "SELECT id FROM ann_ivf_rebuild \
                 ORDER BY embedding <-> VECTOR '[0,0]' LIMIT 3",
            )
            .await
            .expect("ivfflat top-k after restart rebuild");
        assert_eq!(
            simple_rows(&messages),
            vec![
                vec!["4".to_owned()],
                vec!["2".to_owned()],
                vec!["3".to_owned()]
            ],
            "replayed IVFFlat DML must preserve exact top-k visibility"
        );
        let messages = client
            .simple_query(
                "EXPLAIN ANALYZE SELECT id FROM ann_ivf_rebuild \
                 ORDER BY embedding <-> VECTOR '[0,0]' LIMIT 3",
            )
            .await
            .expect("ivfflat explain after restart rebuild");
        let text = simple_rows(&messages)
            .into_iter()
            .flatten()
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            text.contains("Vector Index: selected ann_ivf_rebuild_idx (page-backed ivfflat)"),
            "restart rebuild must select recovered IVFFlat index, got: {text}"
        );

        graceful_shutdown(running).await;
    }
}
