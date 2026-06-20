//! Session-level vector search behavior: ef_search knob, rolled-back updates, re-embedding migrations, and quantized payload options.

use super::*;

#[tokio::test]
async fn hnsw_ef_search_session_knob_is_accepted_and_applied() {
    // pgvector-compatible per-session ef_search knob: SET is accepted, SHOW
    // reflects it, and a query under the override returns correct neighbors.
    let (client, _conn, server_handle) = start_server_and_connect().await;
    client
        .batch_execute("CREATE TABLE knob (id INT NOT NULL, embedding VECTOR(2))")
        .await
        .expect("create table");
    let values: String = (0..200)
        .map(|i| format!("({i}, '[{i},0]')"))
        .collect::<Vec<_>>()
        .join(",");
    client
        .batch_execute(&format!("INSERT INTO knob (id, embedding) VALUES {values}"))
        .await
        .expect("insert rows");
    client
        .batch_execute("CREATE INDEX knob_emb ON knob USING hnsw (embedding vector_l2_ops)")
        .await
        .expect("create index");

    // Unset → SHOW reports the auto-sized default.
    let shown = client
        .simple_query("SHOW hnsw.ef_search")
        .await
        .expect("show default");
    assert_eq!(simple_rows(&shown), vec![vec!["auto".to_owned()]]);

    // A large override is accepted and SHOW echoes it.
    client
        .batch_execute("SET hnsw.ef_search = 500")
        .await
        .expect("set ef_search");
    let shown = client
        .simple_query("SHOW hnsw.ef_search")
        .await
        .expect("show override");
    assert_eq!(simple_rows(&shown), vec![vec!["500".to_owned()]]);

    // Under the override the nearest-neighbor answer is still correct.
    let near = client
        .simple_query("SELECT id FROM knob ORDER BY embedding <-> VECTOR '[30,0]' LIMIT 3")
        .await
        .expect("knn under override");
    assert_eq!(
        simple_rows(&near),
        vec![
            vec!["30".to_owned()],
            vec!["29".to_owned()],
            vec!["31".to_owned()]
        ]
    );

    // Zero is rejected (must be positive).
    let bad = client.batch_execute("SET hnsw.ef_search = 0").await;
    assert!(bad.is_err(), "ef_search = 0 must be rejected");
    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn rolled_back_embedding_update_does_not_affect_vector_search() {
    // MVCC visibility: an aborted embedding update must leave no trace in vector
    // search — the index reflects committed state only and never drifts to an
    // uncommitted vector.
    let (client, _conn, server_handle) = start_server_and_connect().await;
    client
        .batch_execute("CREATE TABLE m2 (id INT NOT NULL, embedding VECTOR(3))")
        .await
        .expect("create table");
    client
        .batch_execute("INSERT INTO m2 VALUES (1,'[0,0,0]'),(2,'[5,0,0]'),(3,'[9,0,0]')")
        .await
        .expect("insert rows");
    client
        .batch_execute("CREATE INDEX m2_emb ON m2 USING hnsw (embedding vector_l2_ops)")
        .await
        .expect("create index");
    // Move id=3 near the origin, then roll back.
    client
        .batch_execute("BEGIN; UPDATE m2 SET embedding='[0.1,0,0]' WHERE id=3; ROLLBACK;")
        .await
        .expect("rolled-back update");
    // Search reflects committed state only: id=3 stays far, so the top-2 nearest
    // to the origin are id=1 and id=2 — not the rolled-back id=3.
    let messages = client
        .simple_query("SELECT id FROM m2 ORDER BY embedding <-> VECTOR '[0,0,0]' LIMIT 2")
        .await
        .expect("vector search after rollback");
    assert_eq!(
        simple_rows(&messages),
        vec![vec!["1".to_owned()], vec!["2".to_owned()]],
        "rolled-back embedding update must not appear in vector search"
    );
    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn embedding_generations_coexist_during_re_embedding_migration() {
    // Re-embedding a corpus when the model changes: a model_version column tracks
    // which generation produced each vector. During the migration both
    // generations coexist and are queryable independently (each pins its own
    // vectors via the metadata filter), so readers stay consistent.
    let (client, _conn, server_handle) = start_server_and_connect().await;
    client
        .batch_execute(
            "CREATE TABLE corpus \
             (doc_id INT NOT NULL, model_version INT NOT NULL, embedding VECTOR(2), body TEXT)",
        )
        .await
        .expect("create table");
    // Generation 1.
    client
        .batch_execute(
            "INSERT INTO corpus VALUES \
             (1,1,'[0,0]','a'), (2,1,'[5,0]','b'), (3,1,'[9,0]','c')",
        )
        .await
        .expect("generation 1");
    client
        .batch_execute("CREATE INDEX corpus_emb ON corpus USING hnsw (embedding vector_l2_ops)")
        .await
        .expect("create index");
    // Re-embed with a new model in one transaction: generation 2 with different
    // vectors (doc 3 is now nearest the origin instead of doc 1).
    client
        .batch_execute(
            "BEGIN; \
             INSERT INTO corpus VALUES (1,2,'[9,0]','a'), (2,2,'[5,0]','b'), (3,2,'[0,0]','c'); \
             COMMIT;",
        )
        .await
        .expect("generation 2 migration");
    // Old generation still queryable, pinned to its own vectors.
    let gen1 = client
        .simple_query(
            "SELECT doc_id FROM corpus WHERE model_version=1 \
             ORDER BY embedding <-> VECTOR '[0,0]' LIMIT 1",
        )
        .await
        .expect("generation 1 search");
    assert_eq!(simple_rows(&gen1), vec![vec!["1".to_owned()]]);
    // New generation queryable, with its own nearest neighbor.
    let gen2 = client
        .simple_query(
            "SELECT doc_id FROM corpus WHERE model_version=2 \
             ORDER BY embedding <-> VECTOR '[0,0]' LIMIT 1",
        )
        .await
        .expect("generation 2 search");
    assert_eq!(simple_rows(&gen2), vec![vec!["3".to_owned()]]);
    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn ann_quantized_payload_options_work_through_sql_indexes() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE ann_half_payload (id INT NOT NULL, embedding HALFVEC(2))")
        .await
        .expect("create halfvec table");
    client
        .batch_execute(
            "INSERT INTO ann_half_payload VALUES \
             (1, HALFVEC(2) '[0,0]'), \
             (2, HALFVEC(2) '[8,0]')",
        )
        .await
        .expect("insert initial halfvec rows");
    client
        .batch_execute(
            "CREATE INDEX ann_half_payload_hnsw \
             ON ann_half_payload USING hnsw (embedding vector_l2_ops) \
             WITH (payload = bf16)",
        )
        .await
        .expect("create bf16 hnsw index");
    client
        .batch_execute("INSERT INTO ann_half_payload VALUES (3, HALFVEC(2) '[0.5,0]')")
        .await
        .expect("maintain bf16 hnsw");

    let messages = client
        .simple_query(
            "SELECT id FROM ann_half_payload \
             ORDER BY embedding <-> HALFVEC(2) '[0,0]' LIMIT 2",
        )
        .await
        .expect("halfvec hnsw top-k query");
    assert_eq!(
        simple_rows(&messages),
        vec![vec!["1".to_owned()], vec!["3".to_owned()]]
    );

    client
        .batch_execute("CREATE TABLE ann_int8_payload (id INT NOT NULL, embedding VECTOR(2))")
        .await
        .expect("create vector table");
    client
        .batch_execute(
            "INSERT INTO ann_int8_payload VALUES \
             (1, '[0,0]'), \
             (2, '[1,0]'), \
             (3, '[9,0]'), \
             (4, '[10,0]')",
        )
        .await
        .expect("insert initial int8 vectors");
    client
        .batch_execute(
            "CREATE INDEX ann_int8_payload_ivf \
             ON ann_int8_payload USING ivfflat (embedding vector_l2_ops) \
             WITH (lists = 1, probes = 1, payload = int8)",
        )
        .await
        .expect("create int8 ivfflat index");
    client
        .batch_execute("INSERT INTO ann_int8_payload VALUES (5, '[9.5,0]')")
        .await
        .expect("maintain int8 ivfflat");

    let messages = client
        .simple_query(
            "SELECT id FROM ann_int8_payload \
             ORDER BY embedding <-> VECTOR '[9.4,0]' LIMIT 2",
        )
        .await
        .expect("int8 ivfflat top-k query");
    assert_eq!(
        simple_rows(&messages),
        vec![vec!["5".to_owned()], vec!["3".to_owned()]]
    );

    shutdown(client, server_handle).await;
}
