//! ANN maintenance certifications: delete/vacuum tombstone cleanup and update relocation preserving top-k.

use super::*;

#[tokio::test]
async fn ann_delete_vacuum_cert_cleans_tombstones_and_preserves_top_k() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE ann_vacuum_cert (id INT NOT NULL, embedding VECTOR(2))")
        .await
        .expect("create vector table");
    client
        .batch_execute(
            "INSERT INTO ann_vacuum_cert VALUES \
             (1, '[0,0]'), \
             (2, '[0.1,0]'), \
             (3, '[0.2,0]'), \
             (4, '[8,0]'), \
             (5, '[9,0]')",
        )
        .await
        .expect("insert vectors");
    client
        .batch_execute(
            "CREATE INDEX ann_vacuum_cert_hnsw \
             ON ann_vacuum_cert USING hnsw (embedding vector_l2_ops)",
        )
        .await
        .expect("create hnsw index");
    client
        .batch_execute("DELETE FROM ann_vacuum_cert WHERE id IN (1, 2)")
        .await
        .expect("delete indexed vectors");

    let messages = client
        .simple_query(
            "SELECT id FROM ann_vacuum_cert \
             ORDER BY embedding <-> VECTOR '[0,0]' LIMIT 3",
        )
        .await
        .expect("top-k after delete");
    let rows = simple_rows(&messages);
    assert_eq!(
        rows,
        vec![
            vec!["3".to_owned()],
            vec!["4".to_owned()],
            vec!["5".to_owned()]
        ],
        "ANN query must not return deleted tuple ids"
    );

    let messages = client
        .simple_query(
            "EXPLAIN ANALYZE SELECT id FROM ann_vacuum_cert \
             ORDER BY embedding <-> VECTOR '[0,0]' LIMIT 3",
        )
        .await
        .expect("explain before vacuum");
    let before = simple_rows(&messages)
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        before.contains("deleted_candidates_skipped=2"),
        "EXPLAIN must report deleted ANN candidates before VACUUM, got: {before}"
    );

    client
        .batch_execute("VACUUM ann_vacuum_cert")
        .await
        .expect("vacuum vector table");
    let messages = client
        .simple_query(
            "EXPLAIN ANALYZE SELECT id FROM ann_vacuum_cert \
             ORDER BY embedding <-> VECTOR '[0,0]' LIMIT 3",
        )
        .await
        .expect("explain after vacuum");
    let after = simple_rows(&messages)
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        after.contains("deleted_candidates_skipped=0"),
        "VACUUM must clean ANN tombstone stats, got: {after}"
    );

    client
        .batch_execute("CREATE TABLE ann_ivf_vacuum_cert (id INT NOT NULL, embedding VECTOR(2))")
        .await
        .expect("create ivfflat vector table");
    client
        .batch_execute(
            "INSERT INTO ann_ivf_vacuum_cert VALUES \
             (1, '[0,0]'), \
             (2, '[0.1,0]'), \
             (3, '[0.2,0]'), \
             (4, '[8,0]'), \
             (5, '[9,0]')",
        )
        .await
        .expect("insert ivfflat vectors");
    client
        .batch_execute(
            "CREATE INDEX ann_ivf_vacuum_cert_idx \
             ON ann_ivf_vacuum_cert USING ivfflat (embedding vector_l2_ops) \
             WITH (lists = 2, probes = 2)",
        )
        .await
        .expect("create ivfflat index");
    client
        .batch_execute("DELETE FROM ann_ivf_vacuum_cert WHERE id IN (1, 2)")
        .await
        .expect("delete ivfflat indexed vectors");

    let messages = client
        .simple_query(
            "SELECT id FROM ann_ivf_vacuum_cert \
             ORDER BY embedding <-> VECTOR '[0,0]' LIMIT 3",
        )
        .await
        .expect("ivfflat top-k after delete");
    assert_eq!(
        simple_rows(&messages),
        vec![
            vec!["3".to_owned()],
            vec!["4".to_owned()],
            vec!["5".to_owned()]
        ],
        "IVFFlat query must not return deleted tuple ids"
    );

    let messages = client
        .simple_query(
            "EXPLAIN ANALYZE SELECT id FROM ann_ivf_vacuum_cert \
             ORDER BY embedding <-> VECTOR '[0,0]' LIMIT 3",
        )
        .await
        .expect("ivfflat explain before vacuum");
    let before = simple_rows(&messages)
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        before.contains("method=ivfflat") && before.contains("deleted_candidates_skipped=2"),
        "IVFFlat EXPLAIN must report deleted ANN candidates before VACUUM, got: {before}"
    );

    client
        .batch_execute("VACUUM ann_ivf_vacuum_cert")
        .await
        .expect("vacuum ivfflat vector table");
    let messages = client
        .simple_query(
            "EXPLAIN ANALYZE SELECT id FROM ann_ivf_vacuum_cert \
             ORDER BY embedding <-> VECTOR '[0,0]' LIMIT 3",
        )
        .await
        .expect("ivfflat explain after vacuum");
    let after = simple_rows(&messages)
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        after.contains("method=ivfflat") && after.contains("deleted_candidates_skipped=0"),
        "VACUUM must clean IVFFlat tombstone stats, got: {after}"
    );

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn ann_update_cert_moves_embedding_from_old_to_new_location() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE ann_update_cert (id INT NOT NULL, embedding VECTOR(2))")
        .await
        .expect("create vector table");
    client
        .batch_execute(
            "INSERT INTO ann_update_cert VALUES \
             (1, '[9,0]'), \
             (2, '[0,0]'), \
             (3, '[1,0]'), \
             (4, '[8,0]')",
        )
        .await
        .expect("insert vectors");
    client
        .batch_execute(
            "CREATE INDEX ann_update_cert_hnsw \
             ON ann_update_cert USING hnsw (embedding vector_l2_ops)",
        )
        .await
        .expect("create hnsw index");
    client
        .batch_execute("UPDATE ann_update_cert SET embedding = VECTOR '[0.05,0]' WHERE id = 1")
        .await
        .expect("update vector");

    let messages = client
        .simple_query(
            "SELECT id FROM ann_update_cert \
             ORDER BY embedding <-> VECTOR '[9,0]' LIMIT 1",
        )
        .await
        .expect("query old location");
    assert_ne!(
        simple_rows(&messages),
        vec![vec!["1".to_owned()]],
        "updated row must not stay ranked at its old vector location"
    );

    let messages = client
        .simple_query(
            "SELECT id FROM ann_update_cert \
             ORDER BY embedding <-> VECTOR '[0,0]' LIMIT 1",
        )
        .await
        .expect("query new location");
    assert_eq!(simple_rows(&messages), vec![vec!["2".to_owned()]]);

    let messages = client
        .simple_query(
            "SELECT id FROM ann_update_cert \
             ORDER BY embedding <-> VECTOR '[0.05,0]' LIMIT 1",
        )
        .await
        .expect("query exact new location");
    assert_eq!(simple_rows(&messages), vec![vec!["1".to_owned()]]);

    client
        .batch_execute("CREATE TABLE ann_ivf_update_cert (id INT NOT NULL, embedding VECTOR(2))")
        .await
        .expect("create ivfflat update table");
    client
        .batch_execute(
            "INSERT INTO ann_ivf_update_cert VALUES \
             (1, '[9,0]'), \
             (2, '[0,0]'), \
             (3, '[1,0]'), \
             (4, '[8,0]')",
        )
        .await
        .expect("insert ivfflat update vectors");
    client
        .batch_execute(
            "CREATE INDEX ann_ivf_update_cert_idx \
             ON ann_ivf_update_cert USING ivfflat (embedding vector_l2_ops) \
             WITH (lists = 2, probes = 2)",
        )
        .await
        .expect("create ivfflat update index");
    client
        .batch_execute("UPDATE ann_ivf_update_cert SET embedding = VECTOR '[0.05,0]' WHERE id = 1")
        .await
        .expect("update ivfflat vector");

    let messages = client
        .simple_query(
            "SELECT id FROM ann_ivf_update_cert \
             ORDER BY embedding <-> VECTOR '[9,0]' LIMIT 1",
        )
        .await
        .expect("query ivfflat old location");
    assert_ne!(
        simple_rows(&messages),
        vec![vec!["1".to_owned()]],
        "updated IVFFlat row must not stay ranked at its old vector location"
    );

    let messages = client
        .simple_query(
            "SELECT id FROM ann_ivf_update_cert \
             ORDER BY embedding <-> VECTOR '[0.05,0]' LIMIT 1",
        )
        .await
        .expect("query ivfflat exact new location");
    assert_eq!(simple_rows(&messages), vec![vec!["1".to_owned()]]);

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn ann_filtered_top_k_uses_index_with_exact_recheck() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute(
            "CREATE TABLE ann_filter_cert (id INT NOT NULL, tenant INT, embedding VECTOR(2))",
        )
        .await
        .expect("create vector table");
    client
        .batch_execute(
            "INSERT INTO ann_filter_cert VALUES \
             (1, 1, '[0,0]'), \
             (2, 1, '[1,0]'), \
             (3, 1, '[2,0]'), \
             (4, 2, '[0.1,0]'), \
             (5, 2, '[0.2,0]')",
        )
        .await
        .expect("insert vectors");
    client
        .batch_execute(
            "CREATE INDEX ann_filter_cert_hnsw \
             ON ann_filter_cert USING hnsw (embedding vector_l2_ops)",
        )
        .await
        .expect("create hnsw index");

    let messages = client
        .simple_query(
            "SELECT id FROM ann_filter_cert \
             WHERE tenant = 1 \
             ORDER BY embedding <-> VECTOR '[0,0]' LIMIT 3",
        )
        .await
        .expect("filtered vector top-k");
    assert_eq!(
        simple_rows(&messages),
        vec![
            vec!["1".to_owned()],
            vec!["2".to_owned()],
            vec!["3".to_owned()]
        ],
        "filtered ANN policy must not return fewer than LIMIT when enough visible rows exist"
    );

    let messages = client
        .simple_query(
            "EXPLAIN ANALYZE SELECT id FROM ann_filter_cert \
             WHERE tenant = 1 \
             ORDER BY embedding <-> VECTOR '[0,0]' LIMIT 3",
        )
        .await
        .expect("filtered vector explain");
    let text = simple_rows(&messages)
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        text.contains("page-backed hnsw"),
        "EXPLAIN must report the HNSW index served the filtered top-k, got: {text}"
    );
    assert!(
        text.contains("filter=exact-recheck"),
        "EXPLAIN must report exact predicate recheck on the filtered ANN candidates, got: {text}"
    );

    shutdown(client, server_handle).await;
}
