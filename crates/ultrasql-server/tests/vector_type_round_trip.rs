//! End-to-end VECTOR(n) type metadata tests.

use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tokio_postgres::{NoTls, SimpleQueryMessage, types::Type};
use ultrasql_server::{Server, bind_listener, serve_listener};
use ultrasql_wal::{RecordType, WalRecord};

pub mod support;

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

async fn start_small_segment_crash_server_and_connect(
    data_dir: &Path,
    segment_size_bytes: u64,
) -> (
    tokio_postgres::Client,
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
) {
    start_server_and_connect_to(Arc::new(
        Server::init_with_wal_segment_size(data_dir, segment_size_bytes)
            .expect("persistent server init"),
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

fn lsn_before_first_wal_record(data_dir: &Path, record_type: RecordType) -> u64 {
    let segments = sorted_wal_segments(data_dir);
    let mut stream_pos = 0_u64;
    for segment in &segments {
        let bytes =
            fs::read(segment).unwrap_or_else(|e| panic!("read WAL segment {segment:?}: {e}"));
        let mut offset = 0;
        while offset < bytes.len() {
            let (record, used) = WalRecord::decode(&bytes[offset..])
                .unwrap_or_else(|e| panic!("decode WAL segment {segment:?} at {offset}: {e}"));
            if record.header.record_type == record_type {
                return stream_pos + u64::try_from(offset).expect("WAL offset fits u64");
            }
            offset += used;
        }
        stream_pos = stream_pos
            .checked_add(u64::try_from(bytes.len()).expect("WAL segment length fits u64"))
            .expect("WAL stream position fits u64");
    }
    panic!("WAL record type {record_type:?} not found");
}

fn wal_end_lsn(data_dir: &Path) -> u64 {
    let segments = sorted_wal_segments(data_dir);
    let mut stream_pos = 0_u64;
    for segment in &segments {
        let bytes =
            fs::read(segment).unwrap_or_else(|e| panic!("read WAL segment {segment:?}: {e}"));
        let mut offset = 0;
        while offset < bytes.len() {
            let (_record, used) = WalRecord::decode(&bytes[offset..])
                .unwrap_or_else(|e| panic!("decode WAL segment {segment:?} at {offset}: {e}"));
            offset += used;
        }
        stream_pos = stream_pos
            .checked_add(u64::try_from(offset).expect("WAL segment offset fits u64"))
            .expect("WAL stream position fits u64");
    }
    stream_pos
}

fn truncate_inside_first_wal_record(data_dir: &Path, record_type: RecordType) {
    let segments = sorted_wal_segments(data_dir);
    for (segment_idx, segment) in segments.iter().enumerate() {
        let bytes =
            fs::read(segment).unwrap_or_else(|e| panic!("read WAL segment {segment:?}: {e}"));
        let mut offset = 0;
        while offset < bytes.len() {
            let (record, used) = WalRecord::decode(&bytes[offset..])
                .unwrap_or_else(|e| panic!("decode WAL segment {segment:?} at {offset}: {e}"));
            if record.header.record_type == record_type {
                assert!(used > 8, "ANN WAL record should be large enough to tear");
                let keep_len =
                    u64::try_from(offset + (used / 2)).expect("WAL torn offset fits u64");
                let file = fs::OpenOptions::new()
                    .write(true)
                    .open(segment)
                    .unwrap_or_else(|e| panic!("open WAL segment {segment:?}: {e}"));
                file.set_len(keep_len)
                    .unwrap_or_else(|e| panic!("tear WAL segment {segment:?}: {e}"));
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

fn corrupt_first_vector_wal_payload_after(
    data_dir: &Path,
    record_type: RecordType,
    min_record_start_lsn: u64,
) {
    let segments = sorted_wal_segments(data_dir);
    let mut stream_pos = 0_u64;
    for segment in &segments {
        let mut bytes =
            fs::read(segment).unwrap_or_else(|e| panic!("read WAL segment {segment:?}: {e}"));
        let mut offset = 0;
        while offset < bytes.len() {
            let record_start_lsn = stream_pos + u64::try_from(offset).expect("WAL offset fits u64");
            let (record, used) = WalRecord::decode(&bytes[offset..])
                .unwrap_or_else(|e| panic!("decode WAL segment {segment:?} at {offset}: {e}"));
            if record_start_lsn >= min_record_start_lsn && record.header.record_type == record_type
            {
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
                )
                .expect("test WAL record should fit original size limits");
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
        stream_pos = stream_pos
            .checked_add(u64::try_from(bytes.len()).expect("WAL segment length fits u64"))
            .expect("WAL stream position fits u64");
    }
    panic!("WAL record type {record_type:?} not found after LSN {min_record_start_lsn}");
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
                )
                .expect("test WAL record should fit original size limits");
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
async fn vector_sum_and_avg_aggregate_over_wire() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE embeddings (id INT NOT NULL, embedding VECTOR(3))")
        .await
        .expect("create vector table");
    client
        .batch_execute(
            "INSERT INTO embeddings VALUES \
             (1, '[1,2,3]'), \
             (2, '[3,4,5]'), \
             (3, NULL)",
        )
        .await
        .expect("insert vector rows");

    let messages = client
        .simple_query("SELECT sum(embedding), avg(embedding) FROM embeddings")
        .await
        .expect("select vector aggregates");
    let rows = simple_rows(&messages);
    assert_eq!(rows, vec![vec!["[4,6,8]".to_owned(), "[2,3,4]".to_owned()]]);

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
