//! Core VECTOR(n) type round-trips: metadata, literals, casts, distance operators, and aggregates.

use super::*;

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
