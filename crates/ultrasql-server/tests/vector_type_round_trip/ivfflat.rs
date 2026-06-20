//! IVFFlat vector index round-trips: lists/probes behavior, filtered ANN, and page-backed restarts.

use super::*;

#[tokio::test]
async fn ivfflat_l2_opclass_uses_lists_probes_and_survives_dml() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE ann_ivf (id INT NOT NULL, embedding VECTOR(2))")
        .await
        .expect("create vector table");
    client
        .batch_execute(
            "INSERT INTO ann_ivf VALUES \
             (1, '[0,0]'), \
             (2, '[1,0]'), \
             (3, '[9,0]'), \
             (4, '[10,0]')",
        )
        .await
        .expect("insert initial vectors");
    client
        .batch_execute(
            "CREATE INDEX ann_ivf_embedding_idx \
             ON ann_ivf USING ivfflat (embedding vector_l2_ops) \
             WITH (lists = 2, probes = 1)",
        )
        .await
        .expect("create ivfflat index");
    client
        .batch_execute("INSERT INTO ann_ivf VALUES (5, '[9.5,0]')")
        .await
        .expect("insert into ivfflat");
    client
        .batch_execute("UPDATE ann_ivf SET embedding = VECTOR '[8.5,0]' WHERE id = 3")
        .await
        .expect("update ivfflat");
    client
        .batch_execute("DELETE FROM ann_ivf WHERE id = 1")
        .await
        .expect("delete ivfflat");

    let messages = client
        .simple_query(
            "SELECT id FROM ann_ivf \
             ORDER BY embedding <-> VECTOR '[9.4,0]' LIMIT 3",
        )
        .await
        .expect("ivfflat top-k query");
    let rows = simple_rows(&messages);
    assert_eq!(
        rows,
        vec![
            vec!["5".to_owned()],
            vec!["4".to_owned()],
            vec!["3".to_owned()]
        ]
    );

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn ivfflat_filtered_ann_uses_index_and_returns_correct_top_k() {
    // A `WHERE <metadata> ORDER BY <vector> LIMIT k` query against an
    // IVFFlat-indexed column must route through the probes-based ANN path (not
    // the exact filter+sort fallback) and still return the exact top-k among
    // the filtered rows.
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute(
            "CREATE TABLE ann_ivf_filt (id INT NOT NULL, category INT NOT NULL, embedding VECTOR(2))",
        )
        .await
        .expect("create vector table");
    // Two categories interleaved along the x axis: category 1 at even x, 2 at odd.
    let mut values: Vec<String> = Vec::new();
    for i in 0..8 {
        values.push(format!("({}, 1, '[{},0]')", i + 1, i * 2));
    }
    for i in 0..8 {
        values.push(format!("({}, 2, '[{},0]')", i + 9, i * 2 + 1));
    }
    client
        .batch_execute(&format!(
            "INSERT INTO ann_ivf_filt VALUES {}",
            values.join(", ")
        ))
        .await
        .expect("insert vectors");
    client
        .batch_execute(
            "CREATE INDEX ann_ivf_filt_idx \
             ON ann_ivf_filt USING ivfflat (embedding vector_l2_ops) \
             WITH (lists = 4, probes = 1)",
        )
        .await
        .expect("create ivfflat index");

    // category-1 x positions are 0,2,..,14 (ids 1..8). Nearest to 9.1: x=10 (id6),
    // x=8 (id5), x=12 (id7).
    let query = "SELECT id FROM ann_ivf_filt WHERE category = 1 \
                 ORDER BY embedding <-> VECTOR '[9.1,0]' LIMIT 3";
    let rows = simple_rows(
        &client
            .simple_query(query)
            .await
            .expect("filtered ivfflat query"),
    );
    assert_eq!(
        rows,
        vec![
            vec!["6".to_owned()],
            vec!["5".to_owned()],
            vec!["7".to_owned()]
        ],
        "filtered IVFFlat ANN must return the exact top-3 among category=1 rows"
    );

    // EXPLAIN confirms the IVFFlat index served the filtered query.
    let explain = client
        .simple_query(&format!("EXPLAIN ANALYZE {query}"))
        .await
        .expect("explain filtered ivfflat");
    let text = simple_rows(&explain)
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        text.contains("page-backed ivfflat"),
        "EXPLAIN must report the IVFFlat index for the filtered query, got: {text}"
    );

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn ivfflat_filtered_ann_handles_probes_exceeding_list_count() {
    // Regression: a filtered IVFFlat query must not panic when the configured
    // probes exceed the materialized list count (here probes=8 > lists=4). The
    // per-query probe budget is capped at the list count.
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute(
            "CREATE TABLE ann_ivf_probes (id INT NOT NULL, category INT NOT NULL, embedding VECTOR(2))",
        )
        .await
        .expect("create vector table");
    let mut values: Vec<String> = Vec::new();
    for i in 0..8 {
        values.push(format!("({}, 1, '[{},0]')", i + 1, i));
    }
    for i in 0..8 {
        values.push(format!("({}, 2, '[{},0]')", i + 9, i));
    }
    client
        .batch_execute(&format!(
            "INSERT INTO ann_ivf_probes VALUES {}",
            values.join(", ")
        ))
        .await
        .expect("insert vectors");
    client
        .batch_execute(
            "CREATE INDEX ann_ivf_probes_idx \
             ON ann_ivf_probes USING ivfflat (embedding vector_l2_ops) \
             WITH (lists = 4, probes = 8)",
        )
        .await
        .expect("create ivfflat index");

    // The filtered query must succeed (no clamp panic) and return the exact
    // top-2 among category=1 rows nearest [0,0]: ids 1 (x=0) and 2 (x=1).
    let rows = simple_rows(
        &client
            .simple_query(
                "SELECT id FROM ann_ivf_probes WHERE category = 1 \
                 ORDER BY embedding <-> VECTOR '[0,0]' LIMIT 2",
            )
            .await
            .expect("filtered ivfflat query with probes > lists"),
    );
    assert_eq!(rows, vec![vec!["1".to_owned()], vec!["2".to_owned()]]);

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn ivfflat_page_backed_index_survives_sql_restart() {
    let dir = tempfile::tempdir().expect("tempdir");

    {
        let running = start_persistent_server(dir.path(), "vector_type_test").await;
        let client = &running.client;
        client
            .batch_execute("CREATE TABLE ann_ivf_restart (id INT NOT NULL, embedding VECTOR(2))")
            .await
            .expect("create vector table");
        client
            .batch_execute(
                "INSERT INTO ann_ivf_restart VALUES \
                 (1, '[0,0]'), \
                 (2, '[1,0]'), \
                 (3, '[8.5,0]'), \
                 (4, '[10,0]')",
            )
            .await
            .expect("insert vectors");
        client
            .batch_execute(
                "CREATE INDEX ann_ivf_restart_embedding_idx \
                 ON ann_ivf_restart USING ivfflat (embedding vector_l2_ops) \
                 WITH (lists = 2, probes = 1)",
            )
            .await
            .expect("create ivfflat index");
        client
            .batch_execute("INSERT INTO ann_ivf_restart VALUES (5, '[9.5,0]')")
            .await
            .expect("insert post-index vector");
        client
            .batch_execute("DELETE FROM ann_ivf_restart WHERE id = 1")
            .await
            .expect("delete post-index vector");
        graceful_shutdown(running).await;
    }

    {
        let running = start_persistent_server(dir.path(), "vector_type_test").await;
        let client = &running.client;
        let messages = client
            .simple_query(
                "SELECT id FROM ann_ivf_restart \
                 ORDER BY embedding <-> VECTOR '[9.4,0]' LIMIT 3",
            )
            .await
            .expect("ivfflat top-k after restart");
        let rows = simple_rows(&messages);
        assert_eq!(
            rows,
            vec![
                vec!["5".to_owned()],
                vec!["4".to_owned()],
                vec!["3".to_owned()]
            ]
        );

        let messages = client
            .simple_query(
                "EXPLAIN ANALYZE SELECT id FROM ann_ivf_restart \
                 ORDER BY embedding <-> VECTOR '[9.4,0]' LIMIT 3",
            )
            .await
            .expect("explain after restart");
        let text = simple_rows(&messages)
            .into_iter()
            .flatten()
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            text.contains(
                "Vector Index: selected ann_ivf_restart_embedding_idx (page-backed ivfflat)"
            ),
            "EXPLAIN ANALYZE must report page-backed IVFFlat after restart, got: {text}"
        );

        graceful_shutdown(running).await;
    }
}
