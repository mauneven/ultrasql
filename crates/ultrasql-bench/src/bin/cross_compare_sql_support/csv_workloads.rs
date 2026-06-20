//! CSV wire workloads: `read_csv` cold/warm reads, group-by, filter,
//! `COPY` import, dimension-table join, and malformed-row reject
//! behavior.

use std::net::SocketAddr;
use std::path::Path;
use std::time::Instant;

use anyhow::{Context, Result};

use super::util::{connect_sql_server, simple_count, simple_query_rows, sql_string};

pub(crate) async fn run_csv_query_workload(
    server: SocketAddr,
    query: &str,
    warmup: usize,
    total_iters: usize,
    iters_us: &mut Vec<f64>,
) -> Result<serde_json::Value> {
    let (client, conn_handle) = connect_sql_server(server).await?;
    let mut answer = serde_json::Value::Null;
    for i in 0..total_iters {
        let started = Instant::now();
        let messages = client
            .simple_query(query)
            .await
            .with_context(|| format!("CSV query workload: {query}"))?;
        let elapsed_us = started.elapsed().as_secs_f64() * 1e6;
        let rows = simple_query_rows(&messages);
        if rows.is_empty() {
            anyhow::bail!("CSV query returned no rows: {query}");
        }
        if i >= warmup {
            iters_us.push(elapsed_us);
            answer = serde_json::json!({
                "rows": rows,
                "cache_policy": concat!(
                    "cold_read is first measured read in a fresh UltraSQL server; ",
                    "host OS page cache is not forcibly dropped"
                ),
            });
        }
    }
    drop(client);
    conn_handle.abort();
    Ok(answer)
}

pub(crate) async fn run_csv_copy_import(
    server: SocketAddr,
    csv_path: &Path,
    warmup: usize,
    total_iters: usize,
    iters_us: &mut Vec<f64>,
) -> Result<serde_json::Value> {
    let (client, conn_handle) = connect_sql_server(server).await?;
    let path_sql = sql_string(csv_path);
    let mut answer = serde_json::Value::Null;
    for i in 0..total_iters {
        let table = format!("csv_copy_import_{i}");
        client
            .batch_execute(&format!(
                "CREATE TABLE {table} (id INT, category TEXT, metric INT, fact_dim TEXT)"
            ))
            .await
            .with_context(|| format!("CREATE TABLE {table}"))?;

        let copy_sql = format!(
            "COPY {table} FROM {path_sql} WITH (FORMAT csv, HEADER true, AUTO_DETECT true)"
        );
        let started = Instant::now();
        client
            .simple_query(&copy_sql)
            .await
            .with_context(|| format!("COPY CSV into {table}"))?;
        let elapsed_us = started.elapsed().as_secs_f64() * 1e6;
        let imported_rows = simple_count(&client, &format!("SELECT COUNT(*) FROM {table}")).await?;
        if i >= warmup {
            iters_us.push(elapsed_us);
            answer = serde_json::json!({ "imported_rows": imported_rows });
        }
    }
    drop(client);
    conn_handle.abort();
    Ok(answer)
}

pub(crate) async fn run_csv_join_table(
    server: SocketAddr,
    csv_path: &Path,
    warmup: usize,
    total_iters: usize,
    iters_us: &mut Vec<f64>,
) -> Result<serde_json::Value> {
    let (client, conn_handle) = connect_sql_server(server).await?;
    client
        .batch_execute(
            "CREATE TABLE csv_dim (dim_id TEXT, label TEXT);
             INSERT INTO csv_dim VALUES
             ('d0','zero'),('d1','one'),('d2','two'),('d3','three'),
             ('d4','four'),('d5','five'),('d6','six'),('d7','seven'),
             ('d8','eight'),('d9','nine'),('d10','ten'),('d11','eleven'),
             ('d12','twelve'),('d13','thirteen'),('d14','fourteen'),('d15','fifteen')",
        )
        .await
        .context("preload CSV join dimension table")?;
    let path_sql = sql_string(csv_path);
    let query =
        format!("SELECT COUNT(*) FROM read_csv({path_sql}) JOIN csv_dim ON fact_dim = dim_id");
    let mut answer = serde_json::Value::Null;
    for i in 0..total_iters {
        let started = Instant::now();
        let messages = client
            .simple_query(&query)
            .await
            .context("CSV join table workload")?;
        let elapsed_us = started.elapsed().as_secs_f64() * 1e6;
        let rows = simple_query_rows(&messages);
        if rows.is_empty() {
            anyhow::bail!("CSV join returned no rows");
        }
        if i >= warmup {
            iters_us.push(elapsed_us);
            answer = serde_json::json!({ "rows": rows });
        }
    }
    drop(client);
    conn_handle.abort();
    Ok(answer)
}

pub(crate) async fn run_csv_malformed_behavior(
    server: SocketAddr,
    csv_path: &Path,
    warmup: usize,
    total_iters: usize,
    iters_us: &mut Vec<f64>,
) -> Result<serde_json::Value> {
    let (client, conn_handle) = connect_sql_server(server).await?;
    let path_sql = sql_string(csv_path);
    let mut answer = serde_json::Value::Null;
    for i in 0..total_iters {
        let table = format!("csv_bad_import_{i}");
        let rejects = format!("csv_rejects_{i}");
        client
            .batch_execute(&format!(
                "CREATE TABLE {table} (id INT, category TEXT, metric INT, fact_dim TEXT)"
            ))
            .await
            .with_context(|| format!("create malformed CSV target table {table}"))?;
        client
            .batch_execute(&format!(
                "CREATE TABLE {rejects} (
                     filename TEXT,
                     line_number BIGINT,
                     raw_row TEXT,
                     error TEXT
                 )"
            ))
            .await
            .with_context(|| format!("create malformed CSV reject table {rejects}"))?;

        let copy_sql = format!(
            "COPY {table} FROM {path_sql} WITH \
             (FORMAT csv, HEADER true, IGNORE_ERRORS = true, MAX_ERRORS = 1000, \
              REJECT_TABLE = '{rejects}')"
        );
        let started = Instant::now();
        client
            .simple_query(&copy_sql)
            .await
            .with_context(|| format!("COPY malformed CSV into {table}"))?;
        let elapsed_us = started.elapsed().as_secs_f64() * 1e6;
        let accepted_rows = simple_count(&client, &format!("SELECT COUNT(*) FROM {table}")).await?;
        let rejected_rows =
            simple_count(&client, &format!("SELECT COUNT(*) FROM {rejects}")).await?;
        if i >= warmup {
            iters_us.push(elapsed_us);
            answer = serde_json::json!({
                "mode": "copy_ignore_errors",
                "accepted_rows": accepted_rows,
                "rejected_rows": rejected_rows,
                "max_errors": 1000,
            });
        }
    }
    drop(client);
    conn_handle.abort();
    Ok(answer)
}
