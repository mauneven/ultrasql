//! AI / vector wire workloads: persistent HNSW cold-start, vector
//! ingestion throughput, exact vector top-k, hybrid search latency, and
//! RAG retrieval quality, plus the deterministic vector generators they
//! share.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use ultrasql_catalog::rag::{RagSchemaConfig, create_rag_table_statements};

use super::types::{
    Args, COLD_START_INDEX_LOAD_REQUIRED_METRICS, HybridSearchCertification,
    INGESTION_THROUGHPUT_REQUIRED_METRICS, RagRetrievalCertification, VectorTopKCertification,
};
use super::util::{
    connect_sql_server, directory_size_bytes, simple_count, simple_query_rows,
    start_persistent_bench_server, shutdown_persistent_bench_server, usize_to_f64, usize_to_u128,
    write_json_report,
};

const VECTOR_PRELOAD_CHUNK_ROWS: usize = 1_000;

pub(crate) async fn run_cold_start_index_load_workload(
    args: &Args,
    workload_id: &str,
) -> Result<()> {
    let dir = tempfile::tempdir().context("create cold-start tempdir")?;
    let table = "bench_ai_cold_start";
    let top_k = args.top_k.min(args.rows).max(1);

    let setup_server = start_persistent_bench_server(dir.path()).await?;
    let (setup_client, setup_conn) = connect_sql_server(setup_server.bound).await?;
    setup_client
        .batch_execute(&format!(
            "CREATE TABLE {table} (id INT NOT NULL, embedding VECTOR({dims}))",
            dims = args.vector_dims
        ))
        .await
        .with_context(|| format!("CREATE TABLE {table}"))?;
    preload_vector_chunked(&setup_client, table, args.rows, args.vector_dims).await?;
    setup_client
        .batch_execute(&format!(
            "CREATE INDEX {table}_embedding_hnsw \
             ON {table} USING hnsw (embedding vector_l2_ops)"
        ))
        .await
        .with_context(|| format!("CREATE INDEX {table}_embedding_hnsw"))?;
    let loaded = simple_count(&setup_client, &format!("SELECT COUNT(*) FROM {table}")).await?;
    if loaded != i64::try_from(args.rows).context("rows do not fit i64")? {
        anyhow::bail!(
            "cold-start preload count mismatch: expected {}, observed {loaded}",
            args.rows
        );
    }
    shutdown_persistent_bench_server(setup_client, setup_conn, setup_server).await;

    let restart_started = Instant::now();
    let query_server = start_persistent_bench_server(dir.path()).await?;
    let (client, conn_handle) = connect_sql_server(query_server.bound).await?;
    let restart_time_us = restart_started.elapsed().as_secs_f64() * 1e6;

    let probe = vector_probe_literal(args.vector_dims);
    let expected = expected_vector_topk_answer(args.rows, args.vector_dims, top_k);
    let query = format!(
        "SELECT id FROM {table} \
         ORDER BY embedding <-> VECTOR '{probe}' LIMIT {top_k}"
    );
    let (first_query_us, first_answer) = timed_vector_id_query(&client, &query).await?;
    if first_answer != expected {
        anyhow::bail!(
            "cold-start first query mismatch: expected ids {expected}, observed ids {first_answer}"
        );
    }
    let (second_query_us, second_answer) = timed_vector_id_query(&client, &query).await?;
    if second_answer != expected {
        anyhow::bail!(
            "cold-start second query mismatch: expected ids {expected}, observed ids {second_answer}"
        );
    }
    let explain = simple_query_rows(
        &client
            .simple_query(&format!("EXPLAIN ANALYZE {query}"))
            .await
            .context("cold-start explain analyze")?,
    )
    .into_iter()
    .flatten()
    .collect::<Vec<_>>()
    .join("\n");
    let index_loaded_from_disk = explain.contains("page-backed hnsw");
    shutdown_persistent_bench_server(client, conn_handle, query_server).await;

    let report = serde_json::json!({
        "schema_version": 1,
        "suite": "cold_start_index_load",
        "engine": "ultrasql",
        "workload": workload_id,
        "profile": "smoke",
        "status": "measured",
        "required_metrics": COLD_START_INDEX_LOAD_REQUIRED_METRICS,
        "n_rows": args.rows,
        "vector_dims": args.vector_dims,
        "top_k": top_k,
        "restart_time_us": restart_time_us,
        "first_query_us": first_query_us,
        "second_query_us": second_query_us,
        "index_loaded_from_disk": index_loaded_from_disk,
        "answer": {
            "expected_ids": expected,
            "first_ids": first_answer,
            "second_ids": second_answer,
        },
        "policy": "Cold-start artifact builds a persistent page-backed HNSW index, restarts the SQL server, and verifies the restarted query uses that index."
    });
    write_json_report(args.output.as_ref(), &report, "cross_compare_sql")
}

async fn timed_vector_id_query(
    client: &tokio_postgres::Client,
    query: &str,
) -> Result<(f64, String)> {
    let started = Instant::now();
    let messages = client
        .simple_query(query)
        .await
        .with_context(|| format!("vector id query: {query}"))?;
    let elapsed_us = started.elapsed().as_secs_f64() * 1e6;
    let answer = messages
        .iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0).map(ToOwned::to_owned),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join(",");
    Ok((elapsed_us, answer))
}

#[derive(Clone, Copy, Debug)]
struct VectorIngestTiming {
    insert_us: f64,
    commit_us: f64,
}

pub(crate) async fn run_ingestion_throughput_workload(
    args: &Args,
    workload_id: &str,
) -> Result<()> {
    let dir = tempfile::tempdir().context("create ingestion tempdir")?;
    let server = start_persistent_bench_server(dir.path()).await?;
    let (client, conn_handle) = connect_sql_server(server.bound).await?;
    let wal_before = directory_size_bytes(&dir.path().join("pg_wal"))?;
    let batch_size = 128_usize.min(args.rows.max(1));

    client
        .batch_execute(&format!(
            "CREATE TABLE bench_ai_ingest_plain (id INT NOT NULL, embedding VECTOR({dims}))",
            dims = args.vector_dims
        ))
        .await
        .context("create plain ingestion table")?;
    let plain = ingest_vector_batches(
        &client,
        "bench_ai_ingest_plain",
        args.rows,
        args.vector_dims,
        batch_size,
    )
    .await?;

    client
        .batch_execute(&format!(
            "CREATE TABLE bench_ai_ingest_indexed (id INT NOT NULL, embedding VECTOR({dims}))",
            dims = args.vector_dims
        ))
        .await
        .context("create indexed ingestion table")?;
    client
        .batch_execute(
            "CREATE INDEX bench_ai_ingest_indexed_embedding_hnsw \
             ON bench_ai_ingest_indexed USING hnsw (embedding vector_l2_ops)",
        )
        .await
        .context("create ingestion hnsw index")?;
    let indexed = ingest_vector_batches(
        &client,
        "bench_ai_ingest_indexed",
        args.rows,
        args.vector_dims,
        batch_size,
    )
    .await?;

    for table in ["bench_ai_ingest_plain", "bench_ai_ingest_indexed"] {
        let count = simple_count(&client, &format!("SELECT COUNT(*) FROM {table}")).await?;
        if count != i64::try_from(args.rows).context("rows do not fit i64")? {
            anyhow::bail!(
                "ingestion count mismatch for {table}: expected {}, observed {count}",
                args.rows
            );
        }
    }
    tokio::time::sleep(Duration::from_millis(20)).await;
    let wal_after = directory_size_bytes(&dir.path().join("pg_wal"))?;
    shutdown_persistent_bench_server(client, conn_handle, server).await;

    let indexed_total_us = indexed.insert_us + indexed.commit_us;
    let rows_f64 = usize_to_f64(args.rows, "ingestion row count")?;
    let rows_per_sec = if indexed_total_us > 0.0 {
        rows_f64 * 1_000_000.0 / indexed_total_us
    } else {
        0.0
    };
    let plain_total_us = plain.insert_us + plain.commit_us;
    let rows_per_sec_without_index = if plain_total_us > 0.0 {
        rows_f64 * 1_000_000.0 / plain_total_us
    } else {
        0.0
    };
    let index_update_us = (indexed.insert_us - plain.insert_us).max(0.0);
    let wal_bytes = wal_after.saturating_sub(wal_before);

    let report = serde_json::json!({
        "schema_version": 1,
        "suite": "ingestion_throughput",
        "engine": "ultrasql",
        "workload": workload_id,
        "profile": "smoke",
        "status": "measured",
        "required_metrics": INGESTION_THROUGHPUT_REQUIRED_METRICS,
        "n_rows": args.rows,
        "vector_dims": args.vector_dims,
        "batch_size": batch_size,
        "ingest_path": "insert_batches",
        "rows_per_sec": rows_per_sec,
        "rows_per_sec_with_index": rows_per_sec,
        "rows_per_sec_without_index": rows_per_sec_without_index,
        "wal_bytes": wal_bytes,
        "index_update_us": index_update_us,
        "commit_us": indexed.commit_us,
        "commit_us_with_index": indexed.commit_us,
        "commit_us_without_index": plain.commit_us,
        "insert_us_with_index": indexed.insert_us,
        "insert_us_without_index": plain.insert_us,
        "policy": "Ingestion artifact inserts deterministic vector batches through SQL with and without a pre-created HNSW index; no cross-engine ranking."
    });
    write_json_report(args.output.as_ref(), &report, "cross_compare_sql")
}

async fn ingest_vector_batches(
    client: &tokio_postgres::Client,
    table: &str,
    n_rows: usize,
    dims: usize,
    batch_size: usize,
) -> Result<VectorIngestTiming> {
    client
        .batch_execute("BEGIN")
        .await
        .with_context(|| format!("BEGIN ingest for {table}"))?;
    let insert_started = Instant::now();
    let mut start = 0;
    while start < n_rows {
        let end = (start + batch_size).min(n_rows);
        let mut sql = String::with_capacity((end - start) * (dims * 4 + 32) + 64);
        sql.push_str("INSERT INTO ");
        sql.push_str(table);
        sql.push_str(" VALUES ");
        for row_id in start..end {
            if row_id > start {
                sql.push(',');
            }
            sql.push('(');
            sql.push_str(&row_id.to_string());
            sql.push(',');
            push_vector_literal_for_row(&mut sql, row_id, dims);
            sql.push(')');
        }
        client
            .batch_execute(&sql)
            .await
            .with_context(|| format!("ingest vector batch [{start}, {end}) into {table}"))?;
        start = end;
    }
    let insert_us = insert_started.elapsed().as_secs_f64() * 1e6;
    let commit_started = Instant::now();
    client
        .batch_execute("COMMIT")
        .await
        .with_context(|| format!("COMMIT ingest for {table}"))?;
    let commit_us = commit_started.elapsed().as_secs_f64() * 1e6;
    Ok(VectorIngestTiming {
        insert_us,
        commit_us,
    })
}

fn vector_component(row_id: usize, dim: usize) -> i32 {
    let row = usize_to_u128(row_id);
    let dim = usize_to_u128(dim);
    let value = ((row * 31) + (dim * 17) + ((row % 7) * 13)) % 101;
    i32::try_from(value).unwrap_or(0) - 50
}

fn vector_probe_component(dim: usize) -> i32 {
    let dim = usize_to_u128(dim);
    let value = ((dim * 7) + 3) % 23;
    i32::try_from(value).unwrap_or(0) - 11
}

fn push_vector_literal_for_row(sql: &mut String, row_id: usize, dims: usize) {
    sql.push_str("'[");
    for dim in 0..dims {
        if dim > 0 {
            sql.push(',');
        }
        sql.push_str(&vector_component(row_id, dim).to_string());
    }
    sql.push_str("]'");
}

fn vector_probe_literal(dims: usize) -> String {
    let mut literal = String::with_capacity(dims * 4 + 2);
    literal.push('[');
    for dim in 0..dims {
        if dim > 0 {
            literal.push(',');
        }
        literal.push_str(&vector_probe_component(dim).to_string());
    }
    literal.push(']');
    literal
}

fn vector_l2_squared(row_id: usize, dims: usize) -> i64 {
    let mut sum = 0_i64;
    for dim in 0..dims {
        let delta = i64::from(vector_component(row_id, dim) - vector_probe_component(dim));
        sum += delta * delta;
    }
    sum
}

fn expected_vector_topk_answer(n_rows: usize, dims: usize, top_k: usize) -> String {
    let mut candidates = (0..n_rows)
        .map(|row_id| (vector_l2_squared(row_id, dims), row_id))
        .collect::<Vec<_>>();
    candidates.sort_unstable();
    candidates
        .into_iter()
        .take(top_k.min(n_rows))
        .map(|(_, row_id)| row_id.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

async fn preload_vector_chunked(
    client: &tokio_postgres::Client,
    table: &str,
    n_rows: usize,
    dims: usize,
) -> Result<()> {
    let mut start = 0;
    while start < n_rows {
        let end = (start + VECTOR_PRELOAD_CHUNK_ROWS).min(n_rows);
        let mut sql = String::with_capacity((end - start) * (dims * 4 + 32) + 64);
        sql.push_str("INSERT INTO ");
        sql.push_str(table);
        sql.push_str(" VALUES ");
        for row_id in start..end {
            if row_id > start {
                sql.push(',');
            }
            sql.push('(');
            sql.push_str(&row_id.to_string());
            sql.push(',');
            push_vector_literal_for_row(&mut sql, row_id, dims);
            sql.push(')');
        }
        client
            .batch_execute(&sql)
            .await
            .with_context(|| format!("preload vector chunk [{start}, {end}) into {table}"))?;
        start = end;
    }
    Ok(())
}

/// Shared-table exact vector top-k workload: preload deterministic vectors
/// once, then time exact `ORDER BY distance, id LIMIT k` scans.
pub(crate) async fn run_shared_vector_topk(
    server: SocketAddr,
    n_rows: usize,
    dims: usize,
    top_k: usize,
    warmup: usize,
    total_iters: usize,
    iters_us: &mut Vec<f64>,
) -> Result<VectorTopKCertification> {
    let conn_str = format!("host=127.0.0.1 port={} user=bench_runner", server.port());
    let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
        .await
        .context("tokio-postgres connect to ultrasqld")?;
    let conn_handle = tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("tokio-postgres connection error: {e}");
        }
    });

    let table = "bench_vector_topk_shared";
    let build_started = Instant::now();
    client
        .batch_execute(&format!(
            "CREATE TABLE {table} (id INT NOT NULL, embedding VECTOR({dims}))"
        ))
        .await
        .with_context(|| format!("CREATE TABLE {table}"))?;
    preload_vector_chunked(&client, table, n_rows, dims).await?;
    let build_time_us = build_started.elapsed().as_secs_f64() * 1e6;

    let probe = vector_probe_literal(dims);
    let expected = expected_vector_topk_answer(n_rows, dims, top_k);
    let query = format!(
        "SELECT id, embedding <-> '{probe}' AS distance \
         FROM {table} ORDER BY distance, id LIMIT {top_k}"
    );
    for i in 0..total_iters {
        let started = Instant::now();
        let messages = client
            .simple_query(&query)
            .await
            .with_context(|| format!("vector top-k on {table}"))?;
        let elapsed_us = started.elapsed().as_secs_f64() * 1e6;
        let observed = messages
            .iter()
            .filter_map(|message| match message {
                tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0).map(ToOwned::to_owned),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join(",");
        if observed != expected {
            anyhow::bail!(
                "vector top-k answer mismatch: expected ids {expected}, observed ids {observed}"
            );
        }
        if i >= warmup {
            iters_us.push(elapsed_us);
        }
    }

    drop(client);
    conn_handle.abort();
    Ok(VectorTopKCertification {
        answer: expected,
        build_time_us,
    })
}

pub(crate) async fn run_shared_hybrid_search_latency(
    server: SocketAddr,
    n_rows: usize,
    top_k: usize,
    warmup: usize,
    total_iters: usize,
    iters_us: &mut Vec<f64>,
) -> Result<HybridSearchCertification> {
    let (client, conn_handle) = connect_sql_server(server).await?;
    let table = "bench_hybrid_search_shared";
    let n_rows = n_rows.max(4);
    let top_k = top_k.clamp(1, 3).min(n_rows);
    client
        .batch_execute(&format!(
            "CREATE TABLE {table} (
                id INT NOT NULL,
                content TEXT,
                embedding VECTOR(2),
                metadata JSONB
            )"
        ))
        .await
        .with_context(|| format!("CREATE TABLE {table}"))?;

    let mut sql = String::with_capacity(n_rows * 96);
    sql.push_str("INSERT INTO ");
    sql.push_str(table);
    sql.push_str(" VALUES ");
    for row_id in 0..n_rows {
        if row_id > 0 {
            sql.push(',');
        }
        let (content, vector, kind) = match row_id {
            0 => ("rust sql hybrid rag", "[0,0]", "guide"),
            1 => ("rust sql hybrid database", "[0.05,0]", "guide"),
            2 => ("rust sql vector database", "[0.15,0]", "guide"),
            _ => ("archived unrelated note", "[4,4]", "note"),
        };
        sql.push_str(&format!(
            "({row_id},'{content}',VECTOR '{vector}','{{\"kind\":\"{kind}\"}}')"
        ));
    }
    client
        .batch_execute(&sql)
        .await
        .with_context(|| format!("preload {table}"))?;

    let query = format!(
        "SELECT id FROM {table} \
         WHERE metadata @> '{{\"kind\":\"guide\"}}'::jsonb \
         ORDER BY hybrid_search(content, 'rust sql hybrid', embedding, VECTOR '[0,0]') DESC \
         LIMIT {top_k}"
    );
    let expected_ids = (0..top_k)
        .map(|id| id.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let mut observed_ids = String::new();
    for i in 0..total_iters {
        let started = Instant::now();
        let messages = client
            .simple_query(&query)
            .await
            .with_context(|| format!("hybrid search latency on {table}"))?;
        let elapsed_us = started.elapsed().as_secs_f64() * 1e6;
        let rows = simple_query_rows(&messages);
        if rows.is_empty() {
            anyhow::bail!("hybrid search returned no rows");
        }
        observed_ids = rows
            .iter()
            .filter_map(|row| row.first())
            .cloned()
            .collect::<Vec<_>>()
            .join(",");
        if observed_ids != expected_ids {
            anyhow::bail!(
                "hybrid search answer mismatch: expected ids {expected_ids}, observed ids {observed_ids}"
            );
        }
        if i >= warmup {
            iters_us.push(elapsed_us);
        }
    }

    drop(client);
    conn_handle.abort();
    let row_count_f64 = usize_to_f64(n_rows, "hybrid search row count")?;
    Ok(HybridSearchCertification {
        expected_ids,
        observed_ids,
        recall_at_k: 1.0,
        filter_selectivity: 3.0 / row_count_f64,
    })
}

pub(crate) async fn run_rag_retrieval_quality(
    server: SocketAddr,
    top_k: usize,
    warmup: usize,
    total_iters: usize,
    iters_us: &mut Vec<f64>,
) -> Result<RagRetrievalCertification> {
    let (client, conn_handle) = connect_sql_server(server).await?;
    let config = RagSchemaConfig {
        prefix: "bench_rag".to_owned(),
        embedding_dims: 3,
    };
    for statement in create_rag_table_statements(&config).context("build rag benchmark DDL")? {
        client
            .batch_execute(&statement)
            .await
            .with_context(|| format!("RAG benchmark DDL: {statement}"))?;
    }
    client
        .batch_execute(
            "INSERT INTO bench_rag_documents VALUES \
             ('tenant-a', 'doc-a', 's3://bucket/a.md', 'Doc A', 'hash-a', '{\"kind\":\"guide\"}', \
              TIMESTAMP '2026-05-20 10:00:00', TIMESTAMP '2026-05-20 10:00:00', \
              TIMESTAMP '2026-05-20 10:05:00', 2, true), \
             ('tenant-b', 'doc-b', 's3://bucket/b.md', 'Doc B', 'hash-b', '{\"kind\":\"guide\"}', \
              TIMESTAMP '2026-05-20 10:00:00', TIMESTAMP '2026-05-20 10:30:00', \
              TIMESTAMP '2026-05-20 10:35:00', 3, true)",
        )
        .await
        .context("insert RAG benchmark documents")?;
    for statement in [
        "INSERT INTO bench_rag_chunks VALUES \
         ('tenant-a', 'chunk-alpha', 'doc-a', 0, 'rust sql hybrid retrieval', 0, 4, \
          '{\"section\":\"intro\"}', TIMESTAMP '2026-05-20 10:00:01', \
          TIMESTAMP '2026-05-20 10:00:02', 2, true)",
        "INSERT INTO bench_rag_chunks VALUES \
         ('tenant-a', 'chunk-omega', 'doc-a', 1, 'vector database exact fallback', 4, 8, \
          '{\"section\":\"body\"}', TIMESTAMP '2026-05-20 10:00:03', \
          TIMESTAMP '2026-05-20 10:00:04', 2, true)",
        "INSERT INTO bench_rag_chunks VALUES \
         ('tenant-b', 'chunk-tenant-b', 'doc-b', 0, 'other tenant content', 0, 3, \
          '{\"section\":\"intro\"}', TIMESTAMP '2026-05-20 10:30:01', \
          TIMESTAMP '2026-05-20 10:30:02', 3, true)",
    ] {
        client
            .batch_execute(statement)
            .await
            .with_context(|| format!("insert RAG benchmark chunk: {statement}"))?;
    }
    for statement in [
        "INSERT INTO bench_rag_embeddings VALUES \
         ('tenant-a', 'emb-alpha', 'chunk-alpha', VECTOR '[1,0,0]', 'bench-model', 'v1', '{\"dims\":3}', \
          TIMESTAMP '2026-05-20 10:01:00', 2, true)",
        "INSERT INTO bench_rag_embeddings VALUES \
         ('tenant-a', 'emb-omega', 'chunk-omega', VECTOR '[0.9,0.1,0]', 'bench-model', 'v1', '{\"dims\":3}', \
          TIMESTAMP '2026-05-20 10:01:01', 2, true)",
        "INSERT INTO bench_rag_embeddings VALUES \
         ('tenant-b', 'emb-tenant-b', 'chunk-tenant-b', VECTOR '[1,0,0]', 'bench-model', 'v1', '{\"dims\":3}', \
          TIMESTAMP '2026-05-20 10:31:00', 3, true)",
    ] {
        client
            .batch_execute(statement)
            .await
            .with_context(|| format!("insert RAG benchmark embedding: {statement}"))?;
    }

    let top_k = top_k.clamp(1, 2);
    let expected_chunks = vec!["chunk-alpha".to_owned(), "chunk-omega".to_owned()]
        .into_iter()
        .take(top_k)
        .collect::<Vec<_>>();
    let expected_doc_ids = doc_ids_for_rag_chunks(&expected_chunks);
    let query = format!(
        "SELECT chunk_id FROM bench_rag_embeddings \
         WHERE tenant_id = 'tenant-a' AND is_current = true \
         ORDER BY embedding <-> VECTOR '[1,0,0]' \
         LIMIT {top_k}"
    );
    let mut observed_chunks = Vec::new();
    for i in 0..total_iters {
        let started = Instant::now();
        let messages = client
            .simple_query(&query)
            .await
            .context("RAG retrieval quality query")?;
        let elapsed_us = started.elapsed().as_secs_f64() * 1e6;
        observed_chunks = simple_query_rows(&messages)
            .into_iter()
            .filter_map(|row| row.first().cloned())
            .collect::<Vec<_>>();
        if observed_chunks != expected_chunks {
            anyhow::bail!(
                "RAG retrieval answer mismatch: expected chunks {:?}, observed chunks {:?}",
                expected_chunks,
                observed_chunks
            );
        }
        if i >= warmup {
            iters_us.push(elapsed_us);
        }
    }

    drop(client);
    conn_handle.abort();
    let observed_doc_ids = doc_ids_for_rag_chunks(&observed_chunks);
    Ok(RagRetrievalCertification {
        expected_doc_ids,
        observed_doc_ids,
        expected_chunks,
        observed_chunks,
        recall_at_k: 1.0,
        precision_at_k: 1.0,
        mrr: 1.0,
        answer_citation_coverage: 1.0,
    })
}

fn doc_ids_for_rag_chunks(chunks: &[String]) -> Vec<String> {
    let mut doc_ids = Vec::new();
    for chunk in chunks {
        let Some(doc_id) = rag_doc_id_for_chunk(chunk) else {
            continue;
        };
        if !doc_ids.iter().any(|existing| existing == doc_id) {
            doc_ids.push(doc_id.to_owned());
        }
    }
    doc_ids
}

fn rag_doc_id_for_chunk(chunk_id: &str) -> Option<&'static str> {
    match chunk_id {
        "chunk-alpha" | "chunk-omega" => Some("doc-a"),
        "chunk-tenant-b" => Some("doc-b"),
        _ => None,
    }
}
