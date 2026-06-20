//! Hybrid (dense + lexical) search round-trips: candidate ordering, RRF fusion, EXPLAIN ANALYZE retrieval stats, and dimension-mismatch rejection.

use super::*;

#[tokio::test]
async fn hybrid_search_function_orders_candidates_through_executor() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute(
            "CREATE TABLE hybrid_docs (\
                id INT NOT NULL, \
                content TEXT, \
                embedding VECTOR(2), \
                metadata JSONB\
             )",
        )
        .await
        .expect("create hybrid docs");
    client
        .batch_execute(
            "INSERT INTO hybrid_docs VALUES \
             (1, 'rust sql vector database', '[0,0]', '{\"kind\":\"guide\"}'), \
             (2, 'rust sql hybrid rag', '[0.05,0]', '{\"kind\":\"guide\"}'), \
             (3, 'unrelated stale note', '[0.01,0]', '{\"kind\":\"note\"}'), \
             (4, 'rust sql old guide', '[0.35,0]', '{\"kind\":\"guide\"}')",
        )
        .await
        .expect("insert hybrid docs");

    let messages = client
        .simple_query(
            "SELECT id FROM hybrid_docs \
             WHERE metadata @> '{\"kind\":\"guide\"}' \
             ORDER BY hybrid_search(content, 'rust sql hybrid', embedding, VECTOR '[0,0]') DESC \
             LIMIT 2",
        )
        .await
        .expect("hybrid search query");
    let rows = simple_rows(&messages);
    assert_eq!(rows, vec![vec!["2".to_owned()], vec!["1".to_owned()]]);

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn hybrid_search_rrf_fusion_reranks_versus_weighted() {
    // Data where the lexical and vector rankings disagree:
    //   doc 1: very strong text match, farthest vector
    //   doc 2: weak text match,        closest vector
    //   doc 3: no text match,          middle vector
    // Weighted-linear lets doc 1's large BM25 magnitude dominate, so it wins.
    // RRF only counts ranks, so the rank-balanced doc 2 (text #2, vector #1)
    // beats doc 1 (text #1, vector #3). The two fusion methods therefore
    // return different orders, proving the 'rrf' selector is engaged.
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute(
            "CREATE TABLE rrf_docs (\
                id INT NOT NULL, \
                content TEXT, \
                embedding VECTOR(2)\
             )",
        )
        .await
        .expect("create rrf docs");
    client
        .batch_execute(
            "INSERT INTO rrf_docs VALUES \
             (1, 'alpha beta beta beta alpha', '[0.5,0]'), \
             (2, 'alpha', '[0.01,0]'), \
             (3, 'gamma delta', '[0.2,0]')",
        )
        .await
        .expect("insert rrf docs");

    let weighted = client
        .simple_query(
            "SELECT id FROM rrf_docs \
             ORDER BY hybrid_search(content, 'alpha beta', embedding, VECTOR '[0,0]') DESC \
             LIMIT 3",
        )
        .await
        .expect("weighted hybrid query");
    assert_eq!(
        simple_rows(&weighted),
        vec![
            vec!["1".to_owned()],
            vec!["2".to_owned()],
            vec!["3".to_owned()],
        ],
        "weighted-linear fusion is dominated by doc 1's BM25 magnitude"
    );

    let rrf = client
        .simple_query(
            "SELECT id FROM rrf_docs \
             ORDER BY hybrid_search(content, 'alpha beta', embedding, VECTOR '[0,0]', 'rrf') DESC \
             LIMIT 3",
        )
        .await
        .expect("rrf hybrid query");
    assert_eq!(
        simple_rows(&rrf),
        vec![
            vec!["2".to_owned()],
            vec!["1".to_owned()],
            vec!["3".to_owned()],
        ],
        "RRF balances rank contributions, so the rank-balanced doc 2 wins"
    );

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn hybrid_search_explain_analyze_reports_retrieval_stats() {
    // EXPLAIN ANALYZE on a hybrid query must surface retrieval observability:
    // candidates examined, per-filter pruning, selectivity, top-k emitted, a
    // recall estimate, and per-component score ranges — reflecting the executed
    // path.
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute(
            "CREATE TABLE hybrid_obs (\
                id INT NOT NULL, \
                content TEXT, \
                embedding VECTOR(2), \
                metadata JSONB\
             )",
        )
        .await
        .expect("create hybrid table");
    client
        .batch_execute(
            "INSERT INTO hybrid_obs VALUES \
             (1, 'rust sql vector database', '[0,0]', '{\"kind\":\"guide\"}'), \
             (2, 'rust sql hybrid rag', '[0.05,0]', '{\"kind\":\"guide\"}'), \
             (3, 'unrelated stale note', '[0.01,0]', '{\"kind\":\"note\"}'), \
             (4, 'rust sql old guide', '[0.35,0]', '{\"kind\":\"guide\"}')",
        )
        .await
        .expect("insert hybrid docs");

    let explain = client
        .simple_query(
            "EXPLAIN ANALYZE SELECT id FROM hybrid_obs \
             WHERE metadata @> '{\"kind\":\"guide\"}' \
             ORDER BY hybrid_search(content, 'rust sql hybrid', embedding, VECTOR '[0,0]') DESC \
             LIMIT 2",
        )
        .await
        .expect("hybrid explain analyze");
    let text = simple_rows(&explain)
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join("\n");

    // The scan yields 4 rows; the child Filter prunes the one "note" row (4->3);
    // HybridSearch ranks the 3 survivors and emits the top 2, reporting per-
    // component score ranges and an exact recall estimate over what it examined.
    for needle in [
        "operator=HybridSearch",
        "hybrid_candidates_examined=3",
        "candidates_ranked=3",
        "top_k_emitted=2",
        "recall_estimate=1.000",
        "bm25_score_range=[",
        "vector_similarity_range=[",
        // The metadata filter's pruning is observable on the child Filter.
        "operator=Filter rows_in=4 rows_out=3",
    ] {
        assert!(
            text.contains(needle),
            "EXPLAIN ANALYZE must report `{needle}` for the hybrid query, got: {text}"
        );
    }

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn hybrid_search_explain_analyze_bare_sort_keeps_profile_subtree() {
    // Regression: the bare-Sort hybrid path (no projection, e.g. SELECT *) must
    // not double-wrap in ProfiledOperator, which would drop the HybridSearch
    // profile subtree and its retrieval stats from EXPLAIN ANALYZE.
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute(
            "CREATE TABLE hybrid_bare (id INT NOT NULL, content TEXT, embedding VECTOR(2))",
        )
        .await
        .expect("create hybrid table");
    client
        .batch_execute(
            "INSERT INTO hybrid_bare VALUES \
             (1, 'rust sql vector database', '[0,0]'), \
             (2, 'rust sql hybrid rag', '[0.05,0]'), \
             (3, 'unrelated stale note', '[0.01,0]')",
        )
        .await
        .expect("insert hybrid docs");

    let explain = client
        .simple_query(
            "EXPLAIN ANALYZE SELECT * FROM hybrid_bare \
             ORDER BY hybrid_search(content, 'rust sql hybrid', embedding, VECTOR '[0,0]') DESC \
             LIMIT 2",
        )
        .await
        .expect("bare-sort hybrid explain analyze");
    let text = simple_rows(&explain)
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join("\n");

    // The retrieval stats and the child scan must survive (not be dropped by a
    // double profiling wrap).
    for needle in [
        "hybrid_candidates_examined=3",
        "recall_estimate=1.000",
        "vector_similarity_range=[",
        "operator=Seq Scan",
    ] {
        assert!(
            text.contains(needle),
            "bare-Sort hybrid EXPLAIN ANALYZE dropped `{needle}`, got: {text}"
        );
    }

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn insert_rejects_vector_dimension_mismatch() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE embeddings (id INT NOT NULL, embedding VECTOR(3))")
        .await
        .expect("create vector table");
    let err = client
        .batch_execute("INSERT INTO embeddings VALUES (1, '[1, 2]')")
        .await
        .expect_err("dimension mismatch rejected");
    let message = err
        .as_db_error()
        .map(tokio_postgres::error::DbError::message)
        .unwrap_or_default();
    assert!(
        message.contains("type mismatch") || message.contains("vector"),
        "unexpected error: {err}"
    );

    shutdown(client, server_handle).await;
}
