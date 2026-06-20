//! HNSW vector index round-trips: top-k correctness, filtered search, transactional updates, crash recovery, and checkpoint snapshots.

use super::*;

#[tokio::test]
async fn create_hnsw_index_on_vector_column_preserves_top_k_results() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE ann_items (id INT NOT NULL, embedding VECTOR(3))")
        .await
        .expect("create vector table");
    client
        .batch_execute(
            "INSERT INTO ann_items VALUES \
             (1, '[0,0,0]'), \
             (2, '[3,0,0]'), \
             (3, '[1,0,0]'), \
             (4, '[9,0,0]')",
        )
        .await
        .expect("insert vector rows");
    client
        .batch_execute("CREATE INDEX ann_items_embedding_hnsw ON ann_items USING hnsw (embedding)")
        .await
        .expect("create hnsw index");

    let messages = client
        .simple_query("SELECT id FROM ann_items ORDER BY embedding <-> VECTOR '[0,0,0]' LIMIT 2")
        .await
        .expect("top-k query");
    let rows = simple_rows(&messages);
    assert_eq!(rows, vec![vec!["1".to_owned()], vec!["3".to_owned()]]);

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn hnsw_index_after_insert_keeps_top_k_correct() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE ann_after_insert (id INT NOT NULL, embedding VECTOR(3))")
        .await
        .expect("create vector table");
    client
        .batch_execute("INSERT INTO ann_after_insert VALUES (1, '[10,0,0]')")
        .await
        .expect("insert first vector row");
    client
        .batch_execute(
            "CREATE INDEX ann_after_insert_embedding_hnsw \
             ON ann_after_insert USING hnsw (embedding)",
        )
        .await
        .expect("create hnsw index");
    client
        .batch_execute("INSERT INTO ann_after_insert VALUES (2, '[0,0,0]')")
        .await
        .expect("insert row maintained by hnsw");

    let messages = client
        .simple_query(
            "SELECT id FROM ann_after_insert \
             ORDER BY embedding <-> VECTOR '[0,0,0]' LIMIT 1",
        )
        .await
        .expect("top-k query");
    let rows = simple_rows(&messages);
    assert_eq!(rows, vec![vec!["2".to_owned()]]);

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn hnsw_filtered_top_k_returns_correct_filtered_neighbors() {
    // WHERE <predicate> ORDER BY embedding <-> probe LIMIT k must return the
    // true filtered nearest neighbors through the selectivity-aware filtered-ANN
    // path. 200 rows on a line with ef_search default 64 means the over-fetch
    // path traverses the graph (approximate) rather than scanning exhaustively;
    // the post-filter + fallback keep the result exact.
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE fdocs (id INT NOT NULL, kind INT, embedding VECTOR(2))")
        .await
        .expect("create vector table");
    let mut values = String::new();
    for i in 0..200 {
        if i > 0 {
            values.push(',');
        }
        values.push_str(&format!("({i}, {}, '[{i},0]')", i % 4));
    }
    client
        .batch_execute(&format!("INSERT INTO fdocs VALUES {values}"))
        .await
        .expect("insert rows");
    client
        .batch_execute("CREATE INDEX fdocs_emb_hnsw ON fdocs USING hnsw (embedding)")
        .await
        .expect("create hnsw index");

    // Loose filter (kind = 0, ~25%): nearest kind-0 ids to x=61 are 60 (d1),
    // 64 (d3), 56 (d5). The over-fetch ANN path must surface exactly those.
    let loose = client
        .simple_query(
            "SELECT id FROM fdocs WHERE kind = 0 \
             ORDER BY embedding <-> VECTOR '[61,0]' LIMIT 3",
        )
        .await
        .expect("loose filtered query");
    assert_eq!(
        simple_rows(&loose),
        vec![
            vec!["60".to_owned()],
            vec!["64".to_owned()],
            vec!["56".to_owned()],
        ],
    );

    // Selective conjunctive filter (id in [30,32]): nearest to x=40 are 32 (d8),
    // 31 (d9). The crossover still returns the exact filtered top-k.
    let selective = client
        .simple_query(
            "SELECT id FROM fdocs WHERE id >= 30 AND id <= 32 \
             ORDER BY embedding <-> VECTOR '[40,0]' LIMIT 2",
        )
        .await
        .expect("selective filtered query");
    assert_eq!(
        simple_rows(&selective),
        vec![vec!["32".to_owned()], vec!["31".to_owned()]],
    );

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn vector_index_and_heap_agree_after_transactional_update_and_crash() {
    // The moat: text, embedding, and metadata are columns of one ACID table, so
    // updating all three in one transaction and then crashing must recover to a
    // state where the vector index agrees with the heap — the updated row is
    // found at its new vector position, never the stale one.
    let dir = tempfile::tempdir().expect("tempdir");
    {
        let (client, _conn, server_handle) =
            start_crash_persistent_server_and_connect(dir.path()).await;
        client
            .batch_execute(
                "CREATE TABLE memories \
                 (id INT NOT NULL, body TEXT, embedding VECTOR(3), metadata JSONB)",
            )
            .await
            .expect("create table");
        client
            .batch_execute(
                "INSERT INTO memories VALUES \
                 (1, 'alpha', '[0,0,0]', '{\"v\":1}'), \
                 (2, 'beta',  '[5,0,0]', '{\"v\":1}'), \
                 (3, 'gamma', '[9,0,0]', '{\"v\":1}')",
            )
            .await
            .expect("insert rows");
        client
            .batch_execute(
                "CREATE INDEX memories_emb ON memories USING hnsw (embedding vector_l2_ops)",
            )
            .await
            .expect("create index");
        // One transaction updates text + embedding + metadata together: move id=3
        // from far ([9,0,0]) to near ([0.1,0,0]).
        client
            .batch_execute(
                "BEGIN; \
                 UPDATE memories \
                 SET body='gamma-v2', embedding='[0.1,0,0]', metadata='{\"v\":2}' \
                 WHERE id=3; \
                 COMMIT;",
            )
            .await
            .expect("transactional update");
        // Crash: abort the server without a graceful shutdown.
        shutdown(client, server_handle).await;
    }
    {
        let (client, _conn, server_handle) =
            start_crash_persistent_server_and_connect(dir.path()).await;
        // Vector search reflects the committed embedding update after recovery:
        // id=3 (now [0.1,0,0]) is the second-nearest to the origin, ahead of id=2.
        let messages = client
            .simple_query("SELECT id FROM memories ORDER BY embedding <-> VECTOR '[0,0,0]' LIMIT 2")
            .await
            .expect("vector search after recovery");
        assert_eq!(
            simple_rows(&messages),
            vec![vec!["1".to_owned()], vec!["3".to_owned()]],
            "vector index must reflect the committed embedding update after crash recovery"
        );
        // Heap reflects the committed text + metadata from the same transaction.
        let body = client
            .simple_query("SELECT body FROM memories WHERE id=3")
            .await
            .expect("body after recovery");
        assert_eq!(simple_rows(&body), vec![vec!["gamma-v2".to_owned()]]);
        let meta = client
            .simple_query("SELECT metadata->>'v' FROM memories WHERE id=3")
            .await
            .expect("metadata after recovery");
        assert_eq!(simple_rows(&meta), vec![vec!["2".to_owned()]]);
        shutdown(client, server_handle).await;
    }
}

#[tokio::test]
async fn wal_recycling_then_crash_recovers_every_committed_row() {
    // Crash-recovery drill for WAL segment recycling. With small WAL segments a
    // few hundred rows span several segments; a CHECKPOINT then recycles the low
    // ones (removing the files and advancing the recovery floor). After more
    // rows and a crash (no graceful shutdown), restart must seed recovery from
    // the advanced floor — NOT from LSN 0 — and reconstruct every committed row.
    // If recovery ignored the floor it would choke on the missing head segments.
    let dir = tempfile::tempdir().expect("tempdir");
    let segment_size = 16 * 1024; // 16 KiB: a few hundred rows span many segments
    let payload = "x".repeat(100);
    {
        let (client, _conn, server_handle) =
            start_small_segment_crash_server_and_connect(dir.path(), segment_size).await;
        client
            .batch_execute("CREATE TABLE recycled (id INT NOT NULL, payload TEXT)")
            .await
            .expect("create table");
        // 1000 rows of ~100-byte payload — well over a dozen 16 KiB segments.
        let pre: String = (1..=1000)
            .map(|i| format!("({i}, '{payload}')"))
            .collect::<Vec<_>>()
            .join(",");
        client
            .batch_execute(&format!("INSERT INTO recycled VALUES {pre}"))
            .await
            .expect("insert pre-checkpoint rows");
        // Checkpoint: flush + fsync make the rows durable on the heap, then the
        // low WAL segments are recycled.
        client
            .batch_execute("CHECKPOINT")
            .await
            .expect("checkpoint");
        // Post-checkpoint rows live in the WAL retained above the floor.
        client
            .batch_execute("INSERT INTO recycled VALUES (1001, 'post-a'), (1002, 'post-b')")
            .await
            .expect("insert post-checkpoint rows");
        // Crash: abort the server task without a graceful shutdown.
        shutdown(client, server_handle).await;
    }
    // The checkpoint must have actually recycled the original head segment —
    // otherwise this drill would not exercise floor-seeded recovery at all.
    assert!(
        !dir.path()
            .join("pg_wal")
            .join("segment_0000000000")
            .exists(),
        "CHECKPOINT must have recycled the original head WAL segment"
    );
    {
        let (client, _conn, server_handle) =
            start_small_segment_crash_server_and_connect(dir.path(), segment_size).await;
        let count = client
            .simple_query("SELECT COUNT(*) FROM recycled")
            .await
            .expect("count after recovery");
        assert_eq!(
            simple_rows(&count),
            vec![vec!["1002".to_owned()]],
            "every committed row must survive recycling + crash recovery"
        );
        // A pre-checkpoint row (its WAL segment was recycled; it survives on the
        // durable heap) and a post-checkpoint row (replayed from the WAL tail).
        let pre_row = client
            .simple_query("SELECT id FROM recycled WHERE id = 1")
            .await
            .expect("pre-checkpoint row");
        assert_eq!(simple_rows(&pre_row), vec![vec!["1".to_owned()]]);
        let post_row = client
            .simple_query("SELECT payload FROM recycled WHERE id = 1002")
            .await
            .expect("post-checkpoint row");
        assert_eq!(simple_rows(&post_row), vec![vec!["post-b".to_owned()]]);
        shutdown(client, server_handle).await;
    }
}

#[tokio::test]
async fn hnsw_snapshot_at_checkpoint_bounds_restart_replay_and_stays_correct() {
    // A CHECKPOINT writes a durable per-index HNSW snapshot; a restart loads it
    // and replays only the WAL records appended AFTER the checkpoint instead of
    // rebuilding the whole graph. Correctness must match a full replay: vectors
    // inserted both before and after the checkpoint are found. The post-
    // checkpoint vectors live only in the WAL above the snapshot's meta.lsn, so
    // if the LSN-bounded replay wrongly skipped them they would be missing here.
    let dir = tempfile::tempdir().expect("tempdir");
    {
        let (client, _conn, server_handle) =
            start_crash_persistent_server_and_connect(dir.path()).await;
        client
            .batch_execute("CREATE TABLE pts (id INT NOT NULL, embedding VECTOR(3))")
            .await
            .expect("create table");
        client
            .batch_execute("CREATE INDEX pts_emb ON pts USING hnsw (embedding vector_l2_ops)")
            .await
            .expect("create index");
        // Pre-checkpoint: 100 vectors along the x axis at [i,0,0], i=1..=100.
        // 100 > the default ef_search (64), so searches traverse the graph
        // rather than exact-scanning — this actually exercises the index.
        let pre: String = (1..=100)
            .map(|i| format!("({i}, '[{i},0,0]')"))
            .collect::<Vec<_>>()
            .join(",");
        client
            .batch_execute(&format!("INSERT INTO pts VALUES {pre}"))
            .await
            .expect("insert pre-checkpoint rows");
        // Checkpoint: persists a snapshot reflecting ids 1..=100.
        client
            .batch_execute("CHECKPOINT")
            .await
            .expect("checkpoint");
        // Post-checkpoint: recorded ONLY in the WAL above the snapshot LSN.
        // id 200 sits at the origin (the new nearest), id 201 just past it.
        client
            .batch_execute("INSERT INTO pts VALUES (200, '[0,0,0]'), (201, '[0.5,0,0]')")
            .await
            .expect("insert post-checkpoint rows");
        shutdown(client, server_handle).await;
    }
    // The checkpoint must have written a snapshot file.
    let snap_count = std::fs::read_dir(dir.path().join("vecsnap"))
        .map(|rd| {
            rd.filter_map(Result::ok)
                .filter(|e| e.path().extension().is_some_and(|x| x == "snap"))
                .count()
        })
        .unwrap_or(0);
    assert!(
        snap_count >= 1,
        "CHECKPOINT must write at least one vector-index snapshot"
    );
    {
        let (client, _conn, server_handle) =
            start_crash_persistent_server_and_connect(dir.path()).await;
        // Nearest to the origin must be the POST-checkpoint id=200, then id=201,
        // then the pre-checkpoint id=1. Seeing 200/201 proves the bounded replay
        // applied the WAL records above the snapshot; seeing 1 proves the
        // snapshot itself loaded.
        let near = client
            .simple_query("SELECT id FROM pts ORDER BY embedding <-> VECTOR '[0,0,0]' LIMIT 3")
            .await
            .expect("nearest search after restart");
        assert_eq!(
            simple_rows(&near),
            vec![
                vec!["200".to_owned()],
                vec!["201".to_owned()],
                vec!["1".to_owned()],
            ],
            "post-checkpoint vectors must survive a snapshot-bounded restart"
        );
        // A pre-checkpoint vector at the far end is also intact.
        let far = client
            .simple_query("SELECT id FROM pts ORDER BY embedding <-> VECTOR '[100,0,0]' LIMIT 1")
            .await
            .expect("far search after restart");
        assert_eq!(simple_rows(&far), vec![vec!["100".to_owned()]]);
        shutdown(client, server_handle).await;
    }
}

#[tokio::test]
async fn ivfflat_snapshot_at_checkpoint_bounds_restart_replay_and_stays_correct() {
    // A CHECKPOINT writes a durable per-index IVFFlat snapshot; a restart loads it
    // and replays only the WAL appended AFTER the checkpoint. Correctness must
    // match a full replay: vectors inserted both before and after the checkpoint
    // are found, and EXPLAIN confirms the page-backed IVFFlat index (not a heap
    // scan) actually served the query, so the result genuinely exercises the
    // snapshot-reconstructed index.
    let dir = tempfile::tempdir().expect("tempdir");
    {
        let (client, _conn, server_handle) =
            start_crash_persistent_server_and_connect(dir.path()).await;
        client
            .batch_execute("CREATE TABLE ivf_pts (id INT NOT NULL, embedding VECTOR(2))")
            .await
            .expect("create table");
        // Pre-checkpoint: 40 vectors along the x axis at [i,0], i=1..=40.
        let pre: String = (1..=40)
            .map(|i| format!("({i}, '[{i},0]')"))
            .collect::<Vec<_>>()
            .join(",");
        client
            .batch_execute(&format!("INSERT INTO ivf_pts VALUES {pre}"))
            .await
            .expect("insert pre-checkpoint rows");
        client
            .batch_execute(
                "CREATE INDEX ivf_pts_emb ON ivf_pts \
                 USING ivfflat (embedding vector_l2_ops) WITH (lists = 4, probes = 4)",
            )
            .await
            .expect("create ivfflat index");
        // Checkpoint: persists a snapshot reflecting ids 1..=40.
        client
            .batch_execute("CHECKPOINT")
            .await
            .expect("checkpoint");
        // Post-checkpoint: recorded ONLY in the WAL above the snapshot LSN.
        // id 200 sits at the origin (the new nearest), id 201 just past it.
        client
            .batch_execute("INSERT INTO ivf_pts VALUES (200, '[0,0]'), (201, '[0.5,0]')")
            .await
            .expect("insert post-checkpoint rows");
        shutdown(client, server_handle).await;
    }
    // The checkpoint must have written a snapshot file.
    let snap_count = std::fs::read_dir(dir.path().join("vecsnap"))
        .map(|rd| {
            rd.filter_map(Result::ok)
                .filter(|e| e.path().extension().is_some_and(|x| x == "snap"))
                .count()
        })
        .unwrap_or(0);
    assert!(
        snap_count >= 1,
        "CHECKPOINT must write at least one vector-index snapshot"
    );
    {
        let (client, _conn, server_handle) =
            start_crash_persistent_server_and_connect(dir.path()).await;
        // Nearest to the origin must be the POST-checkpoint id=200, then id=201,
        // then the pre-checkpoint id=1. Seeing 200/201 proves the bounded replay
        // applied the WAL above the snapshot; seeing 1 proves the snapshot loaded.
        let near = client
            .simple_query("SELECT id FROM ivf_pts ORDER BY embedding <-> VECTOR '[0,0]' LIMIT 3")
            .await
            .expect("nearest search after restart");
        assert_eq!(
            simple_rows(&near),
            vec![
                vec!["200".to_owned()],
                vec!["201".to_owned()],
                vec!["1".to_owned()],
            ],
            "post-checkpoint vectors must survive a snapshot-bounded restart"
        );
        // EXPLAIN confirms the reconstructed IVFFlat index actually served the
        // query — otherwise a heap fallback would mask a broken snapshot.
        let explain = client
            .simple_query(
                "EXPLAIN ANALYZE SELECT id FROM ivf_pts \
                 ORDER BY embedding <-> VECTOR '[0,0]' LIMIT 3",
            )
            .await
            .expect("explain after restart");
        let text = simple_rows(&explain)
            .into_iter()
            .flatten()
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            text.contains("Vector Index: selected ivf_pts_emb (page-backed ivfflat)"),
            "EXPLAIN must report the page-backed IVFFlat index after restart, got: {text}"
        );
        shutdown(client, server_handle).await;
    }
}
