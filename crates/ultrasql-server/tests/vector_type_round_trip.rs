//! End-to-end VECTOR(n) type metadata tests.

use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tokio_postgres::{NoTls, SimpleQueryMessage, types::Type};
use ultrasql_server::{Server, bind_listener, serve_listener};
use ultrasql_wal::{RecordType, WalRecord};

mod support;

use support::{shutdown as graceful_shutdown, start_persistent_server};

async fn start_server_and_connect() -> (
    tokio_postgres::Client,
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
) {
    start_server_and_connect_to(Arc::new(Server::with_sample_database())).await
}

async fn start_crash_persistent_server_and_connect(
    data_dir: &Path,
) -> (
    tokio_postgres::Client,
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
) {
    start_server_and_connect_to(Arc::new(
        Server::init(data_dir).expect("persistent server init"),
    ))
    .await
}

async fn start_server_and_connect_to(
    server: Arc<Server>,
) -> (
    tokio_postgres::Client,
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
) {
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("addr parses");
    let (listener, bound) = bind_listener(addr).await.expect("bind");
    let server_handle = tokio::spawn(serve_listener(listener, server));
    let conn_str = format!(
        "host={host} port={port} user=tester application_name=vector_type_test",
        host = bound.ip(),
        port = bound.port()
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .expect("tokio-postgres connect");
    let conn_handle = tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });
    (client, conn_handle, server_handle)
}

async fn shutdown(
    client: tokio_postgres::Client,
    server_handle: tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
) {
    drop(client);
    tokio::time::sleep(Duration::from_millis(20)).await;
    server_handle.abort();
    let _ = server_handle.await;
}

fn simple_rows(messages: &[SimpleQueryMessage]) -> Vec<Vec<String>> {
    messages
        .iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|idx| row.get(idx).unwrap_or("").to_owned())
                    .collect(),
            ),
            _ => None,
        })
        .collect()
}

fn sorted_wal_segments(data_dir: &Path) -> Vec<PathBuf> {
    let wal_dir = data_dir.join("pg_wal");
    let mut segments: Vec<_> = fs::read_dir(&wal_dir)
        .unwrap_or_else(|e| panic!("read WAL dir {wal_dir:?}: {e}"))
        .filter_map(Result::ok)
        .filter(|entry| entry.file_name().to_string_lossy().starts_with("segment_"))
        .map(|entry| entry.path())
        .collect();
    segments.sort();
    segments
}

fn truncate_wal_before_first(data_dir: &Path, record_type: RecordType) {
    let segments = sorted_wal_segments(data_dir);
    for (segment_idx, segment) in segments.iter().enumerate() {
        let bytes =
            fs::read(segment).unwrap_or_else(|e| panic!("read WAL segment {segment:?}: {e}"));
        let mut offset = 0;
        while offset < bytes.len() {
            let (record, used) = WalRecord::decode(&bytes[offset..])
                .unwrap_or_else(|e| panic!("decode WAL segment {segment:?} at {offset}: {e}"));
            if record.header.record_type == record_type {
                let keep_len = u64::try_from(offset).expect("WAL offset fits u64");
                if keep_len == 0 {
                    fs::remove_file(segment)
                        .unwrap_or_else(|e| panic!("remove WAL segment {segment:?}: {e}"));
                } else {
                    let file = fs::OpenOptions::new()
                        .write(true)
                        .open(segment)
                        .unwrap_or_else(|e| panic!("open WAL segment {segment:?}: {e}"));
                    file.set_len(keep_len)
                        .unwrap_or_else(|e| panic!("truncate WAL segment {segment:?}: {e}"));
                }
                for later in segments.iter().skip(segment_idx + 1) {
                    fs::remove_file(later)
                        .unwrap_or_else(|e| panic!("remove later WAL segment {later:?}: {e}"));
                }
                return;
            }
            offset += used;
        }
    }
    panic!("WAL record type {record_type:?} not found");
}

fn corrupt_first_vector_wal_payload(data_dir: &Path, record_type: RecordType) {
    let segments = sorted_wal_segments(data_dir);
    for segment in &segments {
        let mut bytes =
            fs::read(segment).unwrap_or_else(|e| panic!("read WAL segment {segment:?}: {e}"));
        let mut offset = 0;
        while offset < bytes.len() {
            let (record, used) = WalRecord::decode(&bytes[offset..])
                .unwrap_or_else(|e| panic!("decode WAL segment {segment:?} at {offset}: {e}"));
            if record.header.record_type == record_type {
                let mut payload = record.payload;
                assert!(
                    payload.len() > 1,
                    "vector WAL payload should include reserved prefix bytes"
                );
                payload[1] = 1;
                let rewritten = WalRecord::new(
                    record_type,
                    record.header.xid,
                    record.header.prev_lsn,
                    record.header.flags,
                    payload,
                );
                assert_eq!(rewritten.header.total_length, record.header.total_length);
                let encoded = rewritten.encode();
                assert_eq!(encoded.len(), used);
                bytes[offset..offset + used].copy_from_slice(&encoded);
                fs::write(segment, bytes)
                    .unwrap_or_else(|e| panic!("rewrite WAL segment {segment:?}: {e}"));
                return;
            }
            offset += used;
        }
    }
    panic!("WAL record type {record_type:?} not found");
}

#[tokio::test]
async fn create_table_with_vector_column_reports_vector_metadata() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE embeddings (id INT NOT NULL, embedding VECTOR(1536))")
        .await
        .expect("create vector table");

    let messages = client
        .simple_query(
            "SELECT data_type \
             FROM information_schema.columns \
             WHERE table_name = 'embeddings' AND column_name = 'embedding'",
        )
        .await
        .expect("query vector metadata");
    let rows = simple_rows(&messages);
    assert_eq!(rows, vec![vec!["vector".to_owned()]]);

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn insert_and_select_vector_column_round_trips_text_form() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE embeddings (id INT NOT NULL, embedding VECTOR(3))")
        .await
        .expect("create vector table");
    client
        .batch_execute("INSERT INTO embeddings VALUES (1, '[1, 2.5, -3]')")
        .await
        .expect("insert vector row");

    let messages = client
        .simple_query("SELECT embedding FROM embeddings WHERE id = 1")
        .await
        .expect("select vector row");
    let rows = simple_rows(&messages);
    assert_eq!(rows, vec![vec!["[1,2.5,-3]".to_owned()]]);

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn tokio_postgres_extended_query_decodes_vector_as_text() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE embeddings (id INT NOT NULL, embedding VECTOR(3))")
        .await
        .expect("create vector table");
    client
        .batch_execute("INSERT INTO embeddings VALUES (1, '[1, 2.5, -3]')")
        .await
        .expect("insert vector row");

    let row = client
        .query_one("SELECT embedding FROM embeddings WHERE id = $1", &[&1_i32])
        .await
        .expect("select vector row");
    assert_eq!(row.columns()[0].type_(), &Type::TEXT);
    let embedding: String = row.get(0);
    assert_eq!(embedding, "[1,2.5,-3]");

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn vector_typed_literals_and_casts_round_trip() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE embeddings (id INT NOT NULL, embedding VECTOR(3))")
        .await
        .expect("create vector table");
    client
        .batch_execute(
            "INSERT INTO embeddings VALUES \
             (1, VECTOR '[1,2,3]'), \
             (2, CAST('[4,5,6]' AS VECTOR(3))), \
             (3, '[7,8,9]'::VECTOR(3))",
        )
        .await
        .expect("insert vector rows");

    let messages = client
        .simple_query("SELECT embedding FROM embeddings ORDER BY id")
        .await
        .expect("select vector rows");
    let rows = simple_rows(&messages);
    assert_eq!(
        rows,
        vec![
            vec!["[1,2,3]".to_owned()],
            vec!["[4,5,6]".to_owned()],
            vec!["[7,8,9]".to_owned()],
        ]
    );

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn vector_family_types_round_trip_text_form() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute(
            "CREATE TABLE vector_family (\
             id INT NOT NULL, \
             h HALFVEC(3), \
             s SPARSEVEC(5), \
             b BITVEC(4))",
        )
        .await
        .expect("create vector family table");
    client
        .batch_execute(
            "INSERT INTO vector_family VALUES \
             (1, HALFVEC(3) '[1,2.5,-3]', SPARSEVEC(5) '{1:1,3:2.5}/5', BITVEC(4) '1010')",
        )
        .await
        .expect("insert vector family row");

    let messages = client
        .simple_query("SELECT h, s, b FROM vector_family WHERE id = 1")
        .await
        .expect("select vector family row");
    let rows = simple_rows(&messages);
    assert_eq!(
        rows,
        vec![vec![
            "[1,2.5,-3]".to_owned(),
            "{1:1,3:2.5}/5".to_owned(),
            "1010".to_owned()
        ]]
    );

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn vector_family_dimension_mismatches_fail_explicitly() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute(
            "CREATE TABLE vector_family_bad (\
             h HALFVEC(3), \
             s SPARSEVEC(5), \
             b BITVEC(4))",
        )
        .await
        .expect("create vector family table");

    for sql in [
        "INSERT INTO vector_family_bad (h) VALUES ('[1,2]')",
        "INSERT INTO vector_family_bad (s) VALUES ('{1:1}/4')",
        "INSERT INTO vector_family_bad (b) VALUES ('101')",
        "SELECT '[1,2]'::HALFVEC(3)",
        "SELECT '{1:1}/4'::SPARSEVEC(5)",
        "SELECT '101'::BITVEC(4)",
        "SELECT HALFVEC(3) '[1,2,3]' <-> HALFVEC(2) '[1,2]'",
        "SELECT SPARSEVEC(5) '{1:1}/5' <-> SPARSEVEC(4) '{1:1}/4'",
        "SELECT BITVEC(4) '1010' <-> BITVEC(3) '101'",
    ] {
        let err = match client.batch_execute(sql).await {
            Ok(()) => panic!("dimension mismatch accepted for {sql}"),
            Err(err) => err,
        };
        let message = err
            .as_db_error()
            .map(tokio_postgres::error::DbError::message)
            .unwrap_or_default();
        assert!(
            message.contains("dimension")
                || message.contains("type mismatch")
                || message.contains("cannot cast"),
            "unexpected error for {sql}: {err}"
        );
    }

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn vector_distance_operators_execute_in_sql() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE embeddings (id INT NOT NULL, embedding VECTOR(3))")
        .await
        .expect("create vector table");
    client
        .batch_execute("INSERT INTO embeddings VALUES (1, '[1, 2, 3]')")
        .await
        .expect("insert vector row");

    let messages = client
        .simple_query(
            "SELECT \
                 embedding <-> '[1,2,4]', \
                 embedding <#> '[4,5,6]', \
                 l2_distance(embedding, VECTOR '[1,2,4]'), \
                 inner_product(embedding, VECTOR '[4,5,6]'), \
                 dot_product(embedding, VECTOR '[4,5,6]'), \
                 embedding <=> '[3,-6,3]', \
                 cosine_distance(embedding, VECTOR '[3,-6,3]'), \
                 vector_dims(embedding), \
                 vector_norm(embedding), \
                 l1_distance(embedding, VECTOR '[3,2,-1]'), \
                 embedding <+> '[3,2,-1]' \
             FROM embeddings WHERE id = 1",
        )
        .await
        .expect("select vector distances");
    let rows = simple_rows(&messages);
    assert_eq!(
        rows,
        vec![vec![
            "1".to_owned(),
            "-32".to_owned(),
            "1".to_owned(),
            "32".to_owned(),
            "32".to_owned(),
            "1".to_owned(),
            "1".to_owned(),
            "3".to_owned(),
            "3.7416573867739413".to_owned(),
            "6".to_owned(),
            "6".to_owned()
        ]]
    );

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn pgvector_metric_functions_run_on_halfvec_and_sparsevec() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    let messages = client
        .simple_query(
            "SELECT \
                 HALFVEC(3) '[1,2,3]' <#> HALFVEC(3) '[4,5,6]', \
                 inner_product(HALFVEC(3) '[1,2,3]', HALFVEC(3) '[4,5,6]'), \
                 SPARSEVEC(5) '{1:1,3:2,5:-1}/5' <-> SPARSEVEC(5) '{1:2,4:3,5:1}/5', \
                 SPARSEVEC(5) '{1:1,3:2,5:-1}/5' <+> SPARSEVEC(5) '{1:2,4:3,5:1}/5', \
                 vector_norm(HALFVEC(2) '[3,4]'), \
                 l2_norm(SPARSEVEC(4) '{1:3,4:4}/4'), \
                 vector_dims(SPARSEVEC(5) '{1:1}/5')",
        )
        .await
        .expect("select halfvec/sparsevec metrics");
    let rows = simple_rows(&messages);
    assert_eq!(
        rows,
        vec![vec![
            "-32".to_owned(),
            "32".to_owned(),
            "4.242640687119285".to_owned(),
            "8".to_owned(),
            "5".to_owned(),
            "5".to_owned(),
            "5".to_owned()
        ]]
    );

    shutdown(client, server_handle).await;
}

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
async fn ann_filtered_top_k_falls_back_to_exact_when_limit_can_be_satisfied() {
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
        text.contains("fallback_used=true"),
        "EXPLAIN must show exact fallback for filtered vector top-k, got: {text}"
    );
    assert!(
        text.contains("filtered vector top-k"),
        "EXPLAIN must name filtered vector fallback reason, got: {text}"
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
