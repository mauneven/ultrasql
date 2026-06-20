//! Transactional (OLTP) wire workloads: bulk INSERT/UPDATE/DELETE,
//! deterministic mixed write/read correctness, and the pgbench-like
//! mixed-OLTP window driver.

use std::net::SocketAddr;
use std::time::Instant;

use anyhow::{Context, Result};
use tokio_postgres::NoTls;

use super::util::{SplitMix64, connect_sql_server, preload_chunked, simple_query_rows, u64_to_f64};

/// Rows packed into each timed INSERT statement for the bulk-insert
/// benchmark. Competitor scripts use the same 10 000-row chunk size inside one
/// transaction. This keeps the workload at the SQL/client level without
/// turning a 1M-row load into 1 000 parser/round-trip cycles.
const INSERT_BENCH_CHUNK_ROWS: usize = 10_000;

/// Run one INSERT iteration: open a fresh wire connection, CREATE a
/// unique table, then insert rows in 10 000-row chunks inside one timed
/// transaction. The CREATE and SQL string construction are outside the timed
/// region.
pub(crate) async fn run_insert_iter(server: SocketAddr, n_rows: usize, ix: usize) -> Result<f64> {
    let conn_str = format!("host=127.0.0.1 port={} user=bench_runner", server.port());
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .context("tokio-postgres connect to ultrasqld")?;
    let conn_handle = tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("tokio-postgres connection error: {e}");
        }
    });

    let table = format!("bench_insert_{ix}");
    client
        .batch_execute(&format!("CREATE TABLE {table} (id INT NOT NULL, val INT)"))
        .await
        .with_context(|| format!("CREATE TABLE {table}"))?;

    let mut chunks = Vec::with_capacity(n_rows.div_ceil(INSERT_BENCH_CHUNK_ROWS));
    let mut start = 0;
    while start < n_rows {
        let end = (start + INSERT_BENCH_CHUNK_ROWS).min(n_rows);
        let mut sql = String::with_capacity((end - start) * 16 + 64);
        sql.push_str("INSERT INTO ");
        sql.push_str(&table);
        sql.push_str(" VALUES ");
        for j in start..end {
            if j > start {
                sql.push(',');
            }
            sql.push('(');
            sql.push_str(&j.to_string());
            sql.push(',');
            sql.push_str(&(j * 10).to_string());
            sql.push(')');
        }
        chunks.push(sql);
        start = end;
    }

    let started = Instant::now();
    client
        .batch_execute("BEGIN")
        .await
        .context("BEGIN insert sample")?;
    for sql in &chunks {
        client
            .batch_execute(sql)
            .await
            .with_context(|| format!("INSERT chunk INTO {table}"))?;
    }
    client
        .batch_execute("COMMIT")
        .await
        .context("COMMIT insert sample")?;
    let elapsed_us = started.elapsed().as_secs_f64() * 1e6;

    drop(client);
    conn_handle.abort();
    Ok(elapsed_us)
}

/// Shared-table bulk DELETE workload: preload `n_rows` once, then
/// time only `DELETE FROM t WHERE id < n_rows` inside a transaction
/// and roll it back after the timed statement.
///
/// This matches the DuckDB and SQLite competitor runners: one
/// persistent driver connection, stable SQL text, identical starting
/// row image for every sample, and rollback outside the timed region.
pub(crate) async fn run_shared_delete(
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

    let table = "bench_delete_shared";
    client
        .batch_execute(&format!("CREATE TABLE {table} (id INT NOT NULL, val INT)"))
        .await
        .with_context(|| format!("CREATE TABLE {table}"))?;
    preload_chunked(&client, table, n_rows).await?;

    let query = format!("DELETE FROM {table} WHERE id < {n_rows}");
    for i in 0..total_iters {
        client
            .batch_execute("BEGIN")
            .await
            .context("BEGIN delete sample")?;
        let started = Instant::now();
        client
            .batch_execute(&query)
            .await
            .with_context(|| format!("DELETE FROM {table}"))?;
        let elapsed_us = started.elapsed().as_secs_f64() * 1e6;
        client
            .batch_execute("ROLLBACK")
            .await
            .context("ROLLBACK delete sample")?;
        if i >= warmup {
            iters_us.push(elapsed_us);
        }
    }

    drop(client);
    conn_handle.abort();
    Ok(())
}

/// Shared-table bulk UPDATE workload: preload `n_rows` once, then
/// time only `UPDATE t SET val = val + 1 WHERE id < n_rows` inside
/// a transaction and roll it back after the timed statement.
///
/// This matches the DuckDB and SQLite competitor runners: one
/// persistent driver connection, stable SQL text, identical starting
/// row image for every sample, and rollback outside the timed region.
/// It measures the UPDATE executor/wire round-trip rather than
/// per-sample table creation or cold parse/bind misses.
pub(crate) async fn run_shared_update(
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

    let table = "bench_update_shared";
    client
        .batch_execute(&format!("CREATE TABLE {table} (id INT NOT NULL, val INT)"))
        .await
        .with_context(|| format!("CREATE TABLE {table}"))?;
    preload_chunked(&client, table, n_rows).await?;

    let query = format!("UPDATE {table} SET val = val + 1 WHERE id < {n_rows}");
    for i in 0..total_iters {
        client
            .batch_execute("BEGIN")
            .await
            .context("BEGIN update sample")?;
        let started = Instant::now();
        client
            .batch_execute(&query)
            .await
            .with_context(|| format!("UPDATE {table}"))?;
        let elapsed_us = started.elapsed().as_secs_f64() * 1e6;
        client
            .batch_execute("ROLLBACK")
            .await
            .context("ROLLBACK update sample")?;
        if i >= warmup {
            iters_us.push(elapsed_us);
        }
    }

    drop(client);
    conn_handle.abort();
    Ok(())
}

fn push_mixed_correctness_row(sql: &mut String, row_id: usize) {
    let val_mod = row_id.wrapping_mul(17) % 1_000;
    let val = i64::try_from(val_mod).unwrap_or(0) - 500;

    sql.push('(');
    sql.push_str(&row_id.to_string());
    sql.push(',');
    sql.push_str(&val.to_string());
    sql.push(')');
}

async fn preload_mixed_correctness_chunked(
    client: &tokio_postgres::Client,
    table: &str,
    n_rows: usize,
) -> Result<()> {
    let mut start = 0;
    while start < n_rows {
        let end = (start + super::util::PRELOAD_CHUNK_ROWS).min(n_rows);
        let mut sql = String::with_capacity((end - start) * 56 + 64);
        sql.push_str("INSERT INTO ");
        sql.push_str(table);
        sql.push_str(" VALUES ");
        for row_id in start..end {
            if row_id > start {
                sql.push(',');
            }
            push_mixed_correctness_row(&mut sql, row_id);
        }
        client.batch_execute(&sql).await.with_context(|| {
            format!("preload mixed-correctness chunk [{start}, {end}) INSERT into {table}")
        })?;
        start = end;
    }
    Ok(())
}

fn mixed_correctness_insert_sql(table: &str, n_rows: usize) -> String {
    let mut sql = String::with_capacity(96);
    sql.push_str("INSERT INTO ");
    sql.push_str(table);
    sql.push_str(" VALUES ");
    push_mixed_correctness_row(&mut sql, n_rows);
    sql
}

fn mixed_correctness_fact_query(table: &str) -> String {
    format!("SELECT SUM(val) FROM {table} WHERE id >= 0")
}

/// Shared mixed correctness workload: preload once, then each timed
/// sample mutates rows and runs a scalar aggregate inside a rolled-back
/// transaction. The answer rows are returned so release rendering can
/// reject cross-engine mismatches before ranking.
pub(crate) async fn run_shared_mixed_correctness(
    server: SocketAddr,
    n_rows: usize,
    warmup: usize,
    total_iters: usize,
    iters_us: &mut Vec<f64>,
) -> Result<serde_json::Value> {
    let (client, conn_handle) = connect_sql_server(server).await?;
    let fact_table = "bench_mixed_correctness_fact";
    let state_table = "bench_mixed_correctness_state";
    client
        .batch_execute(&format!(
            "CREATE TABLE {fact_table} (
                id INT NOT NULL,
                val INT NOT NULL
            )"
        ))
        .await
        .with_context(|| format!("CREATE TABLE {fact_table}"))?;
    client
        .batch_execute(&format!(
            "CREATE TABLE {state_table} (
                id INT NOT NULL,
                val INT NOT NULL
            )"
        ))
        .await
        .with_context(|| format!("CREATE TABLE {state_table}"))?;
    preload_mixed_correctness_chunked(&client, fact_table, n_rows).await?;
    preload_mixed_correctness_chunked(&client, state_table, 16).await?;

    let update_sql = format!("UPDATE {state_table} SET val = val + 7 WHERE id = 0");
    let insert_sql = mixed_correctness_insert_sql(state_table, n_rows);
    let fact_query = mixed_correctness_fact_query(fact_table);
    let batch_sql = format!("{insert_sql}; {update_sql}; {fact_query}");

    let mut answer_rows = Vec::new();
    for i in 0..total_iters {
        client
            .batch_execute("BEGIN")
            .await
            .context("BEGIN mixed-correctness sample")?;
        let started = Instant::now();
        let fact_messages = client
            .simple_query(&batch_sql)
            .await
            .with_context(|| format!("mixed-correctness batch on {state_table}/{fact_table}"))?;
        let elapsed_us = started.elapsed().as_secs_f64() * 1e6;
        client
            .batch_execute("ROLLBACK")
            .await
            .context("ROLLBACK mixed-correctness sample")?;
        let rows = simple_query_rows(&fact_messages);
        if rows.is_empty() {
            anyhow::bail!("mixed-correctness aggregate returned no rows");
        }
        if i >= warmup {
            iters_us.push(elapsed_us);
            answer_rows = rows;
        }
    }

    drop(client);
    conn_handle.abort();
    Ok(serde_json::json!(answer_rows))
}

/// Mixed-OLTP pgbench-like 1-second-window workload.
///
/// Preloads `n_rows` of `(id INT, val INT)` outside the timed region
/// (one persistent wire connection), then runs operations in a tight
/// loop for `MIXED_WINDOW_SECS` real-time seconds: 50% point reads,
/// 30% point updates, 20% inserts (monotonic `id` past the preload).
/// Returns elapsed-microseconds / op_count to match the competitor
/// scripts' `µs/op` shape (`benchmarks/scripts/run_*_writes.sh::run_mixed`).
pub(crate) async fn run_mixed_oltp_iter(
    server: SocketAddr,
    n_rows: usize,
    ix: usize,
) -> Result<f64> {
    use std::time::Duration;

    /// Mirrors `benchmarks/scripts/run_*_writes.sh::run_mixed` window.
    const MIXED_WINDOW_SECS: f64 = 1.0;
    /// SQLite batches 20 statements per subprocess invocation; DuckDB batches
    /// 50. Use the smaller batch so UltraSQL gets comparable wire amortization
    /// without increasing the operation grouping beyond the SQLite baseline.
    const MIXED_BATCH_OPS: usize = 20;

    let conn_str = format!("host=127.0.0.1 port={} user=bench_runner", server.port());
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .context("tokio-postgres connect to ultrasqld")?;
    let conn_handle = tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("tokio-postgres connection error: {e}");
        }
    });

    let table = format!("bench_mixed_{ix}");
    client
        .batch_execute(&format!("CREATE TABLE {table} (id INT NOT NULL, val INT)"))
        .await
        .with_context(|| format!("CREATE TABLE {table}"))?;
    preload_chunked(&client, &table, n_rows).await?;
    let index = format!("bench_mixed_id_idx_{ix}");
    client
        .batch_execute(&format!("CREATE INDEX {index} ON {table} (id)"))
        .await
        .with_context(|| format!("CREATE INDEX {index}"))?;

    // Deterministic per-iteration seed so two iterations with the same
    // `ix` produce identical op streams.
    let seed = 0xBEEFu64.wrapping_add(u64::try_from(ix).unwrap_or(0));
    let mut rng = SplitMix64::new(seed);
    let n_rows_u64 = u64::try_from(n_rows).unwrap_or(u64::MAX);
    let mut next_id = i64::try_from(n_rows).unwrap_or(i64::MAX);

    let window = Duration::from_secs_f64(MIXED_WINDOW_SECS);
    let started = Instant::now();
    let mut count: u64 = 0;
    while started.elapsed() < window {
        let mut sql = String::with_capacity(MIXED_BATCH_OPS * 72 + 16);
        sql.push_str("BEGIN;\n");
        let mut batch_count = 0_u64;
        for _ in 0..MIXED_BATCH_OPS {
            let r = rng.next_unit_f64();
            if r < 0.50 {
                let row_id = i64::try_from(rng.next_u64() % n_rows_u64).unwrap_or(0);
                sql.push_str("SELECT val FROM ");
                sql.push_str(&table);
                sql.push_str(" WHERE id = ");
                sql.push_str(&row_id.to_string());
                sql.push_str(";\n");
            } else if r < 0.80 {
                let row_id = i64::try_from(rng.next_u64() % n_rows_u64).unwrap_or(0);
                sql.push_str("UPDATE ");
                sql.push_str(&table);
                sql.push_str(" SET val = val + 1 WHERE id = ");
                sql.push_str(&row_id.to_string());
                sql.push_str(";\n");
            } else {
                let new_val = rng.next_i32();
                sql.push_str("INSERT INTO ");
                sql.push_str(&table);
                sql.push_str(" (id, val) VALUES (");
                sql.push_str(&next_id.to_string());
                sql.push(',');
                sql.push_str(&new_val.to_string());
                sql.push_str(");\n");
                next_id += 1;
            }
            batch_count = batch_count.saturating_add(1);
        }
        sql.push_str("COMMIT;\n");
        client
            .batch_execute(&sql)
            .await
            .with_context(|| format!("mixed OLTP batch on {table}"))?;
        count = count.saturating_add(batch_count);
    }
    let elapsed_us = started.elapsed().as_secs_f64() * 1e6;
    let op_count = u64_to_f64(count.max(1), "mixed OLTP operation count")?;
    let us_per_op = elapsed_us / op_count;

    drop(client);
    conn_handle.abort();
    Ok(us_per_op)
}
