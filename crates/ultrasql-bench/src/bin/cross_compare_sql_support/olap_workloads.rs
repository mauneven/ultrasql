//! Analytical (OLAP) wire workloads: sequential scan, scalar/filtered
//! aggregates, Firebolt-style late materialization, dashboard
//! aggregating-index, sparse pruning, and window `row_number()`.

use std::net::SocketAddr;
use std::time::Instant;

use anyhow::{Context, Result};
use tokio_postgres::NoTls;

use super::util::{
    LATE_MAT_PRELOAD_CHUNK_ROWS, PRELOAD_CHUNK_ROWS, measure_simple_query, preload_chunked,
    simple_query_rows,
};

/// Shared-table SELECT-scan workload: preload `n_rows` once, then
/// drain `SELECT id, val FROM t` N times in a row on the same
/// relation (warmup + measured iters) under a single
/// `tokio-postgres` connection.
///
/// Matches the methodology every competitor script uses (the
/// preload is paid once outside the timed region, the persistent
/// driver connection runs N queries against the same materialised
/// relation). Mirrors `run_clickhouse_writes.sh::run_select_scan`.
pub(crate) async fn run_shared_select_scan(
    server: SocketAddr,
    n_rows: usize,
    warmup: usize,
    total_iters: usize,
    iters_us: &mut Vec<f64>,
) -> Result<()> {
    let conn_str = format!("host=127.0.0.1 port={} user=bench_runner", server.port());
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .context("tokio-postgres connect to ultrasqld")?;
    let conn_handle = tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("tokio-postgres connection error: {e}");
        }
    });

    let table = "bench_select_scan_shared";
    client
        .batch_execute(&format!("CREATE TABLE {table} (id INT NOT NULL, val INT)"))
        .await
        .with_context(|| format!("CREATE TABLE {table}"))?;
    preload_chunked(&client, table, n_rows).await?;

    let query = format!("SELECT id, val FROM {table}");
    for i in 0..total_iters {
        let started = Instant::now();
        let messages = client
            .simple_query(&query)
            .await
            .with_context(|| format!("SELECT from {table}"))?;
        let elapsed_us = started.elapsed().as_secs_f64() * 1e6;
        let row_count = messages
            .iter()
            .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
            .count();
        if row_count != n_rows {
            anyhow::bail!("row count mismatch: expected {n_rows}, observed {row_count}");
        }
        if i >= warmup {
            iters_us.push(elapsed_us);
        }
    }

    drop(client);
    conn_handle.abort();
    Ok(())
}

/// Shared-table analytical aggregate workload: preload once, then
/// run `query_fn(table_name)` N times on the same `(id INT, x INT)`
/// relation under a single `tokio-postgres` connection. Drives
/// `SUM(x)`, `AVG(x)`, and `SUM(x) WHERE x > threshold` via a
/// caller-supplied closure that interpolates the table name.
pub(crate) async fn run_shared_olap_aggregate<F>(
    server: SocketAddr,
    n_rows: usize,
    warmup: usize,
    total_iters: usize,
    iters_us: &mut Vec<f64>,
    table: &str,
    query_fn: F,
) -> Result<()>
where
    F: Fn(&str) -> String,
{
    let conn_str = format!("host=127.0.0.1 port={} user=bench_runner", server.port());
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .context("tokio-postgres connect to ultrasqld")?;
    let conn_handle = tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("tokio-postgres connection error: {e}");
        }
    });

    client
        .batch_execute(&format!("CREATE TABLE {table} (id INT NOT NULL, x INT)"))
        .await
        .with_context(|| format!("CREATE TABLE {table}"))?;
    preload_chunked(&client, table, n_rows).await?;

    let query = query_fn(table);
    for i in 0..total_iters {
        let started = Instant::now();
        let messages = client
            .simple_query(&query)
            .await
            .with_context(|| format!("aggregate on {table}"))?;
        let elapsed_us = started.elapsed().as_secs_f64() * 1e6;
        let row_count = messages
            .iter()
            .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
            .count();
        if row_count != 1 {
            anyhow::bail!("aggregate row count mismatch: expected 1, observed {row_count}");
        }
        if i >= warmup {
            iters_us.push(elapsed_us);
        }
    }

    drop(client);
    conn_handle.abort();
    Ok(())
}

const DASHBOARD_TENANTS: usize = 32;
const DASHBOARD_BUCKETS: usize = 64;
const DASHBOARD_FILTER_TENANT: usize = 7;
const LATE_MAT_WIDE_COLUMNS: usize = 100;
const LATE_MAT_TENANTS: usize = 32;
const LATE_MAT_BUCKETS: usize = 128;
const LATE_MAT_FILTER_TENANT: usize = 7;
const LATE_MAT_PAD_COLUMNS: usize = LATE_MAT_WIDE_COLUMNS - 4;
const SPARSE_ROWS_PER_DAY: usize = 256;
const SPARSE_TENANTS: usize = 64;
const SPARSE_BUCKETS: usize = 32;
const SPARSE_FILTER_TENANT: usize = 7;

fn late_materialization_table_ddl(table: &str) -> String {
    let mut sql = format!(
        "CREATE TABLE {table} (
                id INT NOT NULL,
                tenant_id INT NOT NULL,
                bucket INT NOT NULL,
                amount BIGINT NOT NULL"
    );
    for idx in 1..=LATE_MAT_PAD_COLUMNS {
        sql.push_str(&format!(", pad{idx:03} TEXT NOT NULL"));
    }
    sql.push(')');
    sql
}

fn late_materialization_query(table: &str) -> String {
    format!(
        "SELECT amount, pad003, pad096 FROM {table} \
         WHERE tenant_id = {LATE_MAT_FILTER_TENANT}"
    )
}

async fn preload_late_materialization_chunked(
    client: &tokio_postgres::Client,
    table: &str,
    n_rows: usize,
) -> Result<()> {
    let mut start = 0;
    while start < n_rows {
        let end = (start + LATE_MAT_PRELOAD_CHUNK_ROWS).min(n_rows);
        let mut sql = String::with_capacity((end - start) * 128 + 64);
        sql.push_str("INSERT INTO ");
        sql.push_str(table);
        sql.push_str(" VALUES ");
        for row_id in start..end {
            if row_id > start {
                sql.push(',');
            }
            let tenant_id = row_id % LATE_MAT_TENANTS;
            let bucket = (row_id / LATE_MAT_TENANTS) % LATE_MAT_BUCKETS;
            let amount_mod = row_id.wrapping_mul(19) % 2_000;
            let amount = i64::try_from(amount_mod).unwrap_or(0) - 1_000;

            sql.push('(');
            sql.push_str(&row_id.to_string());
            sql.push(',');
            sql.push_str(&tenant_id.to_string());
            sql.push(',');
            sql.push_str(&bucket.to_string());
            sql.push(',');
            sql.push_str(&amount.to_string());
            for pad_idx in 1..=LATE_MAT_PAD_COLUMNS {
                sql.push_str(",'p");
                sql.push_str(&pad_idx.to_string());
                sql.push('_');
                sql.push_str(&(row_id % (pad_idx + 17)).to_string());
                sql.push('\'');
            }
            sql.push(')');
        }
        client.batch_execute(&sql).await.with_context(|| {
            format!("preload late-materialization chunk [{start}, {end}) INSERT into {table}")
        })?;
        start = end;
    }
    Ok(())
}

pub(crate) async fn run_shared_late_materialization(
    server: SocketAddr,
    n_rows: usize,
    warmup: usize,
    total_iters: usize,
    iters_us: &mut Vec<f64>,
) -> Result<serde_json::Value> {
    let conn_str = format!("host=127.0.0.1 port={} user=bench_runner", server.port());
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .context("tokio-postgres connect to ultrasqld")?;
    let conn_handle = tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("tokio-postgres connection error: {e}");
        }
    });

    let late_table = "bench_late_materialization_late";
    let eager_table = "bench_late_materialization_eager";
    client
        .batch_execute(&late_materialization_table_ddl(late_table))
        .await
        .with_context(|| format!("CREATE TABLE {late_table}"))?;
    client
        .batch_execute(&late_materialization_table_ddl(eager_table))
        .await
        .with_context(|| format!("CREATE TABLE {eager_table}"))?;
    preload_late_materialization_chunked(&client, late_table, n_rows).await?;
    preload_late_materialization_chunked(&client, eager_table, n_rows).await?;
    client
        .batch_execute(&format!(
            "CREATE INDEX ix_late_tenant ON {late_table}(tenant_id)"
        ))
        .await
        .with_context(|| format!("CREATE INDEX ix_late_tenant ON {late_table}"))?;

    let late_query = late_materialization_query(late_table);
    let eager_query = late_materialization_query(eager_table);
    let eager = measure_simple_query(
        &client,
        "late-materialization eager baseline",
        &eager_query,
        warmup,
        total_iters,
    )
    .await?;

    let explain_rows = simple_query_rows(
        &client
            .simple_query(&format!("EXPLAIN ANALYZE {late_query}"))
            .await
            .with_context(|| format!("EXPLAIN ANALYZE late materialization on {late_table}"))?,
    );
    let explain_text = explain_rows
        .iter()
        .filter_map(|row| row.first().cloned())
        .collect::<Vec<_>>();
    let late_line = explain_text
        .iter()
        .find(|line| line.starts_with("Late Materialization:"))
        .cloned()
        .context("EXPLAIN ANALYZE did not emit Late Materialization line")?;
    if !late_line.contains("candidates=")
        || !late_line.contains("fetched=")
        || !late_line.contains("skipped=")
    {
        anyhow::bail!("late materialization EXPLAIN line lacks counters: {late_line}");
    }

    let late = measure_simple_query(
        &client,
        "late-materialization indexed late path",
        &late_query,
        warmup,
        total_iters,
    )
    .await?;
    iters_us.extend(late.samples_us.iter().copied());
    let mut eager_rows = eager.rows.clone();
    let mut late_rows = late.rows.clone();
    eager_rows.sort();
    late_rows.sort();
    if late_rows != eager_rows {
        anyhow::bail!(
            "late materialization answer mismatch: eager={:?} late={:?}",
            eager.rows,
            late.rows
        );
    }

    drop(client);
    conn_handle.abort();
    Ok(serde_json::json!({
        "rows": late.rows.len(),
        "tenant_id": LATE_MAT_FILTER_TENANT,
        "wide_columns": LATE_MAT_WIDE_COLUMNS,
        "projected_columns": ["amount", "pad003", "pad096"],
        "query_shape": "wide_payload_projection_with_selective_index_filter",
        "firebolt_style_shape": "wide fact table, selective tenant filter, payload projection",
        "answer_order": "unordered rows sorted before eager/late equality check",
        "explain_late_materialization": late_line,
        "eager_scan_median_us": eager.median_us,
        "eager_scan_samples_us": eager.samples_us,
        "late_materialization_median_us": late.median_us,
        "late_materialization_samples_us": late.samples_us,
        "comparison_policy": "UltraSQL eager and late paths share deterministic rows and query; external competitor artifacts are recorded separately when installed."
    }))
}

async fn preload_dashboard_aggregate_chunked(
    client: &tokio_postgres::Client,
    table: &str,
    n_rows: usize,
) -> Result<()> {
    let mut start = 0;
    while start < n_rows {
        let end = (start + PRELOAD_CHUNK_ROWS).min(n_rows);
        let mut sql = String::with_capacity((end - start) * 40 + 64);
        sql.push_str("INSERT INTO ");
        sql.push_str(table);
        sql.push_str(" VALUES ");
        for row_id in start..end {
            if row_id > start {
                sql.push(',');
            }
            let tenant_id = row_id % DASHBOARD_TENANTS;
            let bucket = (row_id / DASHBOARD_TENANTS) % DASHBOARD_BUCKETS;
            let amount_mod = row_id.wrapping_mul(17) % 1_000;
            let amount = i64::try_from(amount_mod).unwrap_or(0) - 500;

            sql.push('(');
            sql.push_str(&row_id.to_string());
            sql.push(',');
            sql.push_str(&tenant_id.to_string());
            sql.push(',');
            sql.push_str(&bucket.to_string());
            sql.push(',');
            sql.push_str(&amount.to_string());
            sql.push(')');
        }
        client.batch_execute(&sql).await.with_context(|| {
            format!("preload dashboard chunk [{start}, {end}) INSERT into {table}")
        })?;
        start = end;
    }
    Ok(())
}

/// Shared-table dashboard aggregate workload: preload a deterministic
/// fact table once, then run the same filtered grouped aggregate many
/// times. The key order intentionally matches Firebolt's aggregating
/// index shape: filter on the first grouping column, group by the
/// indexed dimensions, and compute `SUM` + `COUNT(*)`.
pub(crate) async fn run_shared_dashboard_aggregate(
    server: SocketAddr,
    n_rows: usize,
    warmup: usize,
    total_iters: usize,
    iters_us: &mut Vec<f64>,
) -> Result<serde_json::Value> {
    let conn_str = format!("host=127.0.0.1 port={} user=bench_runner", server.port());
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .context("tokio-postgres connect to ultrasqld")?;
    let conn_handle = tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("tokio-postgres connection error: {e}");
        }
    });

    let table = "bench_dashboard_aggregate_shared";
    client
        .batch_execute(&format!(
            "CREATE TABLE {table} (
                id INT NOT NULL,
                tenant_id INT NOT NULL,
                bucket INT NOT NULL,
                amount BIGINT NOT NULL
            )"
        ))
        .await
        .with_context(|| format!("CREATE TABLE {table}"))?;
    preload_dashboard_aggregate_chunked(&client, table, n_rows).await?;

    let index_ddl = format!(
        "CREATE AGGREGATING INDEX ix_dashboard_agg ON {table} \
         (tenant_id, bucket, SUM(amount), COUNT(*))"
    );
    client
        .batch_execute(&index_ddl)
        .await
        .with_context(|| format!("CREATE AGGREGATING INDEX ix_dashboard_agg ON {table}"))?;

    let query = format!(
        "SELECT tenant_id, bucket, SUM(amount), COUNT(*) \
         FROM {table} \
         WHERE tenant_id = {DASHBOARD_FILTER_TENANT} \
         GROUP BY tenant_id, bucket \
         ORDER BY tenant_id, bucket"
    );
    let explain_rows = simple_query_rows(
        &client
            .simple_query(&format!("EXPLAIN ANALYZE {query}"))
            .await
            .with_context(|| format!("EXPLAIN ANALYZE dashboard aggregate on {table}"))?,
    );
    let explain_aggregating_index = explain_rows
        .iter()
        .filter_map(|row| row.first().cloned())
        .find(|line| line.starts_with("Aggregating Index:"))
        .context("EXPLAIN ANALYZE did not emit Aggregating Index line")?;
    if !explain_aggregating_index.contains("aggregating_index_used=true") {
        anyhow::bail!(
            "dashboard aggregate did not use aggregating index: {explain_aggregating_index}"
        );
    }

    let mut answer_rows = Vec::new();
    for i in 0..total_iters {
        let started = Instant::now();
        let messages = client
            .simple_query(&query)
            .await
            .with_context(|| format!("dashboard aggregate on {table}"))?;
        let elapsed_us = started.elapsed().as_secs_f64() * 1e6;
        let rows = simple_query_rows(&messages);
        if rows.is_empty() {
            anyhow::bail!("dashboard aggregate returned no rows");
        }
        if i >= warmup {
            iters_us.push(elapsed_us);
            answer_rows = rows;
        }
    }

    drop(client);
    conn_handle.abort();
    Ok(serde_json::json!({
        "rows": answer_rows,
        "query_shape": "filtered_group_by_sum_count",
        "firebolt_index_shape": concat!(
            "CREATE AGGREGATING INDEX idx ON fact_events ",
            "(tenant_id, bucket, SUM(amount), COUNT(*))"
        ),
        "index_ddl": index_ddl,
        "aggregating_index_used": true,
        "explain_aggregating_index": explain_aggregating_index,
    }))
}

fn sparse_filter_days(n_rows: usize) -> (usize, usize) {
    let max_day = n_rows.saturating_sub(1) / SPARSE_ROWS_PER_DAY;
    let start = max_day.saturating_sub(2) / 2;
    let end = (start + 2).min(max_day);
    (start, end)
}

async fn preload_sparse_pruning_chunked(
    client: &tokio_postgres::Client,
    table: &str,
    n_rows: usize,
) -> Result<()> {
    let mut start = 0;
    while start < n_rows {
        let end = (start + PRELOAD_CHUNK_ROWS).min(n_rows);
        let mut sql = String::with_capacity((end - start) * 56 + 64);
        sql.push_str("INSERT INTO ");
        sql.push_str(table);
        sql.push_str(" VALUES ");
        for row_id in start..end {
            if row_id > start {
                sql.push(',');
            }
            let event_day = row_id / SPARSE_ROWS_PER_DAY;
            let tenant_id = ((event_day * 13) + (row_id / 8)) % SPARSE_TENANTS;
            let bucket = row_id % SPARSE_BUCKETS;
            let amount_mod = row_id.wrapping_mul(31) % 2_000;
            let amount = i64::try_from(amount_mod).unwrap_or(0) - 1_000;

            sql.push('(');
            sql.push_str(&row_id.to_string());
            sql.push(',');
            sql.push_str(&event_day.to_string());
            sql.push(',');
            sql.push_str(&tenant_id.to_string());
            sql.push(',');
            sql.push_str(&bucket.to_string());
            sql.push(',');
            sql.push_str(&amount.to_string());
            sql.push(')');
        }
        client.batch_execute(&sql).await.with_context(|| {
            format!("preload sparse-pruning chunk [{start}, {end}) INSERT into {table}")
        })?;
        start = end;
    }
    Ok(())
}

/// Shared-table sparse-pruning workload. UltraSQL runs this as an honest
/// heap-scan baseline; Firebolt's matching script uses `PRIMARY INDEX
/// event_day, tenant_id, bucket` to test sparse granule pruning.
pub(crate) async fn run_shared_sparse_pruning(
    server: SocketAddr,
    n_rows: usize,
    warmup: usize,
    total_iters: usize,
    iters_us: &mut Vec<f64>,
) -> Result<serde_json::Value> {
    let conn_str = format!("host=127.0.0.1 port={} user=bench_runner", server.port());
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .context("tokio-postgres connect to ultrasqld")?;
    let conn_handle = tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("tokio-postgres connection error: {e}");
        }
    });

    let table = "bench_sparse_pruning_shared";
    client
        .batch_execute(&format!(
            "CREATE TABLE {table} (
                id INT NOT NULL,
                event_day INT NOT NULL,
                tenant_id INT NOT NULL,
                bucket INT NOT NULL,
                amount BIGINT NOT NULL
            )"
        ))
        .await
        .with_context(|| format!("CREATE TABLE {table}"))?;
    preload_sparse_pruning_chunked(&client, table, n_rows).await?;

    let (day_start, day_end) = sparse_filter_days(n_rows);
    let query = format!(
        "SELECT event_day, tenant_id, bucket, SUM(amount), COUNT(*) \
         FROM {table} \
         WHERE event_day BETWEEN {day_start} AND {day_end} \
           AND tenant_id = {SPARSE_FILTER_TENANT} \
         GROUP BY event_day, tenant_id, bucket \
         ORDER BY event_day, tenant_id, bucket"
    );
    let mut answer_rows = Vec::new();
    for i in 0..total_iters {
        let started = Instant::now();
        let messages = client
            .simple_query(&query)
            .await
            .with_context(|| format!("sparse pruning aggregate on {table}"))?;
        let elapsed_us = started.elapsed().as_secs_f64() * 1e6;
        let rows = simple_query_rows(&messages);
        if rows.is_empty() {
            anyhow::bail!("sparse pruning aggregate returned no rows");
        }
        if i >= warmup {
            iters_us.push(elapsed_us);
            answer_rows = rows;
        }
    }

    drop(client);
    conn_handle.abort();
    Ok(serde_json::json!({
        "rows": answer_rows,
        "query_shape": "correlated_key_range_filter_group_by_sum_count",
        "firebolt_index_shape": concat!(
            "CREATE FACT TABLE fact_events (...) PRIMARY INDEX ",
            "event_day, tenant_id, bucket"
        ),
        "event_day_start": day_start,
        "event_day_end": day_end,
        "tenant_id": SPARSE_FILTER_TENANT,
    }))
}

/// Shared-table window-function workload: preload `n_rows` once,
/// then drain `SELECT id, row_number() OVER (ORDER BY x) FROM t` N
/// times against the same `(id INT, x INT)` relation under a single
/// `tokio-postgres` connection.
///
/// Mirrors every competitor script's `run_window_row_number`. The
/// query covers the new v0.5 `LogicalPlan::Window` + `WindowAgg` wire
/// path end-to-end; each iteration drains every row through the
/// wire as `tokio_postgres::SimpleQueryMessage::Row`.
pub(crate) async fn run_shared_window_row_number(
    server: SocketAddr,
    n_rows: usize,
    warmup: usize,
    total_iters: usize,
    iters_us: &mut Vec<f64>,
) -> Result<()> {
    let conn_str = format!("host=127.0.0.1 port={} user=bench_runner", server.port());
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .context("tokio-postgres connect to ultrasqld")?;
    let conn_handle = tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("tokio-postgres connection error: {e}");
        }
    });

    let table = "bench_window_row_number_shared";
    client
        .batch_execute(&format!("CREATE TABLE {table} (id INT NOT NULL, x INT)"))
        .await
        .with_context(|| format!("CREATE TABLE {table}"))?;
    preload_chunked(&client, table, n_rows).await?;

    let query = format!("SELECT id, row_number() OVER (ORDER BY x) FROM {table}");
    for i in 0..total_iters {
        let started = Instant::now();
        let messages = client
            .simple_query(&query)
            .await
            .with_context(|| format!("window row_number on {table}"))?;
        let elapsed_us = started.elapsed().as_secs_f64() * 1e6;
        let row_count = messages
            .iter()
            .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
            .count();
        if row_count != n_rows {
            anyhow::bail!(
                "window_row_number row count mismatch: expected {n_rows}, observed {row_count}"
            );
        }
        if i >= warmup {
            iters_us.push(elapsed_us);
        }
    }

    drop(client);
    conn_handle.abort();
    Ok(())
}
