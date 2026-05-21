//! End-to-end VECTOR(n) type metadata tests.

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use tokio_postgres::{NoTls, SimpleQueryMessage, types::Type};
use ultrasql_server::{Server, bind_listener, serve_listener};

async fn start_server_and_connect() -> (
    tokio_postgres::Client,
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
) {
    start_server_and_connect_to(Arc::new(Server::with_sample_database())).await
}

async fn start_persistent_server_and_connect(
    data_dir: &Path,
) -> (
    tokio_postgres::Client,
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
) {
    let server = Arc::new(Server::init(data_dir).expect("persistent server init"));
    start_server_and_connect_to(server).await
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
        let (client, _conn, server_handle) = start_persistent_server_and_connect(dir.path()).await;
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
        shutdown(client, server_handle).await;
    }

    {
        let (client, _conn, server_handle) = start_persistent_server_and_connect(dir.path()).await;
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

        shutdown(client, server_handle).await;
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
        let (client, _conn, server_handle) = start_persistent_server_and_connect(dir.path()).await;
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
        shutdown(client, server_handle).await;
    }

    {
        let (client, _conn, server_handle) = start_persistent_server_and_connect(dir.path()).await;
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

        shutdown(client, server_handle).await;
    }
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
