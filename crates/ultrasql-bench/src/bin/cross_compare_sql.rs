//! UltraSQL wire-protocol cross-engine benchmark driver.
//!
//! Spawns an in-process `ultrasqld` instance bound to an ephemeral
//! local TCP port, then drives a workload from the bench harness
//! through `tokio-postgres` over that real socket. The measurements
//! are end-to-end: TCP send → server message decode → parser → binder
//! → catalog snapshot → autocommit transaction → `ModifyTable` /
//! `SeqScan` over real heap pages → `RowDescription`/`DataRow`/
//! `CommandComplete` encode → TCP receive.
//!
//! This is the apples-to-apples counterpart to the existing
//! competitor scripts (`benchmarks/scripts/run_postgres_writes.sh`,
//! `run_sqlite3_writes.sh`, `run_duckdb_writes.sh`), each of which
//! drives the engine through its native SQL client. Per-engine raw
//! JSON files share the same schema (see
//! `crates/ultrasql-bench/src/bin/results_render.rs`).
//!
//! Output JSON shape:
//!
//! ```json
//! {
//!   "engine": "ultrasql",
//!   "workload": "insert_throughput_10k",
//!   "n_rows": 10000,
//!   "samples": 8,
//!   "median_us": <float>,
//!   "min_us": <float>,
//!   "iterations_us": [<float>, ...]
//! }
//! ```

#![allow(clippy::print_stdout)]
#![allow(clippy::print_stderr)]
// Legacy per-iter OLAP helpers `run_select_iter` / `run_sum_iter` /
// `run_avg_iter` / `run_filter_sum_iter` are retained for the
// historical "fresh table per iter" measurement mode. The current
// `--workload` dispatch routes every OLAP path through the
// shared-table helpers `run_shared_select_scan` /
// `run_shared_olap_aggregate` to match the per-engine competitor
// scripts. Keep the older functions compiling so a follow-on
// `--mode legacy-per-iter` flag can flip back if a reviewer needs
// the old shape.
#![allow(dead_code)]

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use tokio_postgres::NoTls;
use ultrasql_server::{Server, bind_listener, serve_listener};

/// Workload selector. New workloads will be added as the wire
/// pipeline grows to cover more shapes.
#[derive(Copy, Clone, Eq, PartialEq, Debug, ValueEnum)]
enum Workload {
    /// Bulk INSERT of `--rows` (id INT, val INT) tuples through one
    /// multi-row VALUES clause.
    InsertBulk,
    /// Full sequential scan over a freshly-loaded relation; reads
    /// every row through the wire as text-format `DataRow` messages.
    SelectScan,
    /// Whole-relation `SELECT SUM(x) FROM t` analytical aggregate over
    /// a preloaded (id INT, x INT) table. The aggregate result is a
    /// single row; the measurement isolates scan + hash-aggregate cost.
    SumScalar,
    /// Whole-relation `SELECT AVG(x) FROM t` analytical aggregate.
    AvgScalar,
    /// Filtered analytical aggregate
    /// `SELECT SUM(x) FROM t WHERE x > <threshold>`.
    FilterSum,
    /// Bulk UPDATE: preload `--rows` (id INT, val INT) tuples, then
    /// time one `UPDATE bench_update_{ix} SET val = val + 1 WHERE
    /// id < <rows>`. Matches the shape of
    /// `benchmarks/scripts/run_postgres_writes.sh::run_update`.
    UpdateBulk,
    /// Bulk DELETE: preload `--rows` (id INT, val INT) tuples, then
    /// time one `DELETE FROM bench_delete_{ix} WHERE id < <rows>`.
    /// Matches the shape of
    /// `benchmarks/scripts/run_postgres_writes.sh::run_delete`.
    DeleteBulk,
    /// Mixed OLTP pgbench-like — preload `--rows` (id INT, val INT)
    /// tuples, then run a 1-second window of 50% point reads (SELECT
    /// val WHERE id=?), 30% point updates (UPDATE SET val=val+1 WHERE
    /// id=?), 20% inserts (INSERT VALUES (next_id, ?)). Reports
    /// microseconds per operation (elapsed / op_count) — matches the
    /// shape of `benchmarks/scripts/run_*_writes.sh::run_mixed`.
    MixedOltp,
    /// Whole-relation `SELECT id, row_number() OVER (ORDER BY x) FROM
    /// t` over a preloaded `(id INT, x INT)` table. Exercises the
    /// `LogicalPlan::Window` → `WindowAgg` wire end-to-end against the
    /// equivalent built-in on every competitor (PostgreSQL 17 native
    /// `row_number()`, DuckDB native, SQLite 3.25+ native, ClickHouse
    /// `rowNumberInAllBlocks()`). Drains every row through the wire.
    WindowRowNumber,
}

impl Workload {
    fn registry_id(self, n_rows: usize) -> String {
        match self {
            Self::InsertBulk => format!("insert_throughput_{}", k_or_raw(n_rows)),
            Self::SelectScan => format!("select_scan_{}", k_or_raw(n_rows)),
            Self::SumScalar => format!("select_sum_{}_i64", k_or_raw(n_rows)),
            Self::AvgScalar => format!("select_avg_{}_i64", k_or_raw(n_rows)),
            Self::FilterSum => format!("filter_sum_{}_i64", k_or_raw(n_rows)),
            Self::UpdateBulk => format!("update_throughput_{}", k_or_raw(n_rows)),
            Self::DeleteBulk => format!("delete_throughput_{}", k_or_raw(n_rows)),
            // The competitor scripts hard-code the id without a row-
            // count suffix; matching ID keeps results-render's grouping
            // happy.
            Self::MixedOltp => "mixed_oltp_pgbench_like".to_string(),
            Self::WindowRowNumber => format!("window_row_number_{}_i64", k_or_raw(n_rows)),
        }
    }
}

/// Render a row count using `10k` / `1m` notation matching the
/// existing competitor workload ids (`insert_throughput_10k`,
/// `select_sum_65k_i64`).
fn k_or_raw(n: usize) -> String {
    if n >= 1_000_000 && n % 1_000_000 == 0 {
        format!("{}m", n / 1_000_000)
    } else if n >= 1_000 && n % 1_000 == 0 {
        format!("{}k", n / 1_000)
    } else if n == 65_536 {
        // The competitor scripts label `2^16` rows as `65k` even
        // though the exact count is 65 536. Match their workload-id
        // string so the `results-render` table groups them.
        "65k".to_string()
    } else {
        n.to_string()
    }
}

#[derive(Parser, Debug)]
#[command(
    name = "cross_compare_sql",
    about = "UltraSQL wire-protocol cross-engine benchmark driver"
)]
struct Args {
    /// Workload to run.
    #[arg(long, value_enum, default_value_t = Workload::InsertBulk)]
    workload: Workload,
    /// Number of rows in the data set.
    #[arg(long, default_value_t = 10_000)]
    rows: usize,
    /// Warmup iterations (not recorded).
    #[arg(long, default_value_t = 2)]
    warmup: usize,
    /// Measured iterations (median + min reported).
    #[arg(long, default_value_t = 8)]
    iters: usize,
    /// Output JSON file path. When omitted, the JSON is written to
    /// stdout (so the binary composes with `benchmarks/run.sh`).
    #[arg(long)]
    output: Option<PathBuf>,
    /// Explicit workload id override. When omitted, the id is
    /// derived from `--workload` + `--rows`, e.g.
    /// `insert_throughput_10k`.
    #[arg(long)]
    workload_id: Option<String>,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    let args = Args::parse();
    let workload_id = args
        .workload_id
        .clone()
        .unwrap_or_else(|| args.workload.registry_id(args.rows));

    // Bring up an in-process ultrasqld on an ephemeral port.
    let bind_addr: SocketAddr = "127.0.0.1:0".parse()?;
    let (listener, bound) = bind_listener(bind_addr).await.context("bind listener")?;
    let state = Arc::new(Server::with_sample_database());
    let server_task = tokio::spawn(async move {
        if let Err(e) = serve_listener(listener, state).await {
            eprintln!("ultrasqld task exited: {e}");
        }
    });

    // Run warmup + measured iterations.
    let mut iters_us: Vec<f64> = Vec::with_capacity(args.iters);
    let total_iters = args.warmup + args.iters;

    // Shared-table OLAP workloads: preload **once** outside the
    // timed region, then run the query N times against the same
    // relation. Matches the per-engine pattern every competitor
    // script uses (DuckDB / ClickHouse / SQLite / PostgreSQL all
    // build the relation once via their persistent driver
    // connection and time the query repeated N times). Anything
    // else would compare cold-cache UltraSQL against warm-cache
    // peers.
    match args.workload {
        Workload::SelectScan => {
            run_shared_select_scan(bound, args.rows, args.warmup, total_iters, &mut iters_us)
                .await?;
        }
        Workload::SumScalar => {
            run_shared_olap_aggregate(
                bound,
                args.rows,
                args.warmup,
                total_iters,
                &mut iters_us,
                "bench_sum_shared",
                |t| format!("SELECT SUM(x) FROM {t}"),
            )
            .await?;
        }
        Workload::AvgScalar => {
            run_shared_olap_aggregate(
                bound,
                args.rows,
                args.warmup,
                total_iters,
                &mut iters_us,
                "bench_avg_shared",
                |t| format!("SELECT AVG(x) FROM {t}"),
            )
            .await?;
        }
        Workload::FilterSum => {
            let threshold = args.rows / 2;
            run_shared_olap_aggregate(
                bound,
                args.rows,
                args.warmup,
                total_iters,
                &mut iters_us,
                "bench_filter_sum_shared",
                move |t| format!("SELECT SUM(x) FROM {t} WHERE x > {threshold}"),
            )
            .await?;
        }
        Workload::WindowRowNumber => {
            run_shared_window_row_number(bound, args.rows, args.warmup, total_iters, &mut iters_us)
                .await?;
        }
        _ => {
            for i in 0..total_iters {
                let micros = match args.workload {
                    Workload::InsertBulk => run_insert_iter(bound, args.rows, i).await?,
                    Workload::UpdateBulk => run_update_iter(bound, args.rows, i).await?,
                    Workload::DeleteBulk => run_delete_iter(bound, args.rows, i).await?,
                    Workload::MixedOltp => run_mixed_oltp_iter(bound, args.rows, i).await?,
                    Workload::SelectScan
                    | Workload::SumScalar
                    | Workload::AvgScalar
                    | Workload::FilterSum
                    | Workload::WindowRowNumber => unreachable!("handled above"),
                };
                if i >= args.warmup {
                    iters_us.push(micros);
                }
            }
        }
    }

    // Compute median + min.
    iters_us.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let median_us = iters_us[iters_us.len() / 2];
    let min_us = iters_us[0];

    let report = serde_json::json!({
        "engine": "ultrasql",
        "workload": workload_id,
        "n_rows": args.rows,
        "samples": iters_us.len(),
        "median_us": median_us,
        "min_us": min_us,
        "iterations_us": iters_us,
    });
    let serialized = serde_json::to_string(&report)?;
    if let Some(path) = args.output.as_ref() {
        std::fs::write(path, &serialized).with_context(|| format!("write {}", path.display()))?;
        eprintln!("cross_compare_sql: wrote {}", path.display());
    } else {
        println!("{serialized}");
    }

    server_task.abort();
    Ok(())
}

/// Run one INSERT iteration: open a fresh wire connection, CREATE a
/// unique table, run one multi-row INSERT, return elapsed
/// microseconds of the INSERT (the CREATE is outside the timed
/// region).
async fn run_insert_iter(server: SocketAddr, n_rows: usize, ix: usize) -> Result<f64> {
    let conn_str = format!("host=127.0.0.1 port={} user=ultrasql_bench", server.port());
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

    // Build the multi-row INSERT outside the timed window so the
    // measurement isolates the server-side cost (parser → planner →
    // ModifyTable → heap → WAL stub).
    let mut sql = String::with_capacity(n_rows * 16 + 64);
    sql.push_str("INSERT INTO ");
    sql.push_str(&table);
    sql.push_str(" VALUES ");
    for j in 0..n_rows {
        if j > 0 {
            sql.push(',');
        }
        sql.push('(');
        sql.push_str(&j.to_string());
        sql.push(',');
        sql.push_str(&(j * 10).to_string());
        sql.push(')');
    }

    let started = Instant::now();
    client
        .batch_execute(&sql)
        .await
        .with_context(|| format!("INSERT INTO {table}"))?;
    let elapsed_us = started.elapsed().as_secs_f64() * 1e6;

    drop(client);
    conn_handle.abort();
    Ok(elapsed_us)
}

/// Run one SELECT iteration: load `n_rows` into a fresh table
/// (outside the timed region), then time a full sequential scan that
/// drains every row over the wire.
async fn run_select_iter(server: SocketAddr, n_rows: usize, ix: usize) -> Result<f64> {
    let conn_str = format!("host=127.0.0.1 port={} user=ultrasql_bench", server.port());
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .context("tokio-postgres connect to ultrasqld")?;
    let conn_handle = tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("tokio-postgres connection error: {e}");
        }
    });

    let table = format!("bench_select_{ix}");
    client
        .batch_execute(&format!("CREATE TABLE {table} (id INT NOT NULL, val INT)"))
        .await
        .with_context(|| format!("CREATE TABLE {table}"))?;

    // Preload outside the timed region.
    let mut sql = String::with_capacity(n_rows * 16 + 64);
    sql.push_str("INSERT INTO ");
    sql.push_str(&table);
    sql.push_str(" VALUES ");
    for j in 0..n_rows {
        if j > 0 {
            sql.push(',');
        }
        sql.push('(');
        sql.push_str(&j.to_string());
        sql.push(',');
        sql.push_str(&(j * 10).to_string());
        sql.push(')');
    }
    client
        .batch_execute(&sql)
        .await
        .with_context(|| format!("preload INSERT into {table}"))?;

    // Use `simple_query` (Simple Query protocol) rather than `query`
    // (Extended Query Parse/Bind/Execute) — the server's Extended
    // Query dispatch lands in Wave 3. The text-format rows are still
    // fully decoded; we just count them as a sanity check.
    let started = Instant::now();
    let messages = client
        .simple_query(&format!("SELECT id, val FROM {table}"))
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

    drop(client);
    conn_handle.abort();
    Ok(elapsed_us)
}

/// Maximum rows packed into a single `INSERT ... VALUES (...)` statement
/// during preload. A 10 M row inline VALUES list would overrun
/// tokio-postgres' per-message budget and would also stress the
/// server's parser well past the workload under test; 50 000 rows per
/// chunk keeps each statement under a megabyte of SQL text.
const PRELOAD_CHUNK_ROWS: usize = 50_000;

/// Preload `n_rows` of `(id INT, x INT)` rows into `table` via a
/// sequence of multi-row INSERTs, chunked at [`PRELOAD_CHUNK_ROWS`]
/// rows per statement. `x` is set to `id` so analytical workloads
/// (`SUM(x)`, `AVG(x)`, `WHERE x > threshold`) hit non-trivial values.
///
/// Runs outside the timed region for every workload that uses it.
async fn preload_chunked(
    client: &tokio_postgres::Client,
    table: &str,
    n_rows: usize,
) -> Result<()> {
    let mut start = 0;
    while start < n_rows {
        let end = (start + PRELOAD_CHUNK_ROWS).min(n_rows);
        let mut sql = String::with_capacity((end - start) * 24 + 64);
        sql.push_str("INSERT INTO ");
        sql.push_str(table);
        sql.push_str(" VALUES ");
        for j in start..end {
            if j > start {
                sql.push(',');
            }
            sql.push('(');
            sql.push_str(&j.to_string());
            sql.push(',');
            sql.push_str(&j.to_string());
            sql.push(')');
        }
        client
            .batch_execute(&sql)
            .await
            .with_context(|| format!("preload chunk [{start}, {end}) INSERT into {table}"))?;
        start = end;
    }
    Ok(())
}

/// Run one iteration of `SELECT SUM(x) FROM t`.
///
/// Loads `n_rows` of `(id INT, x INT)` outside the timed region, then
/// times a single whole-relation aggregate Simple-Query that returns
/// exactly one `DataRow` with the SUM result.
async fn run_sum_iter(server: SocketAddr, n_rows: usize, ix: usize) -> Result<f64> {
    let conn_str = format!("host=127.0.0.1 port={} user=ultrasql_bench", server.port());
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .context("tokio-postgres connect to ultrasqld")?;
    let conn_handle = tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("tokio-postgres connection error: {e}");
        }
    });

    let table = format!("bench_sum_{ix}");
    client
        .batch_execute(&format!("CREATE TABLE {table} (id INT NOT NULL, x INT)"))
        .await
        .with_context(|| format!("CREATE TABLE {table}"))?;
    preload_chunked(&client, &table, n_rows).await?;

    let started = Instant::now();
    let messages = client
        .simple_query(&format!("SELECT SUM(x) FROM {table}"))
        .await
        .with_context(|| format!("SELECT SUM from {table}"))?;
    let elapsed_us = started.elapsed().as_secs_f64() * 1e6;
    let row_count = messages
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count();
    if row_count != 1 {
        anyhow::bail!("SUM row count mismatch: expected 1, observed {row_count}");
    }

    drop(client);
    conn_handle.abort();
    Ok(elapsed_us)
}

/// Run one iteration of `SELECT AVG(x) FROM t`. See [`run_sum_iter`]
/// for the shape; only the aggregate function differs.
async fn run_avg_iter(server: SocketAddr, n_rows: usize, ix: usize) -> Result<f64> {
    let conn_str = format!("host=127.0.0.1 port={} user=ultrasql_bench", server.port());
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .context("tokio-postgres connect to ultrasqld")?;
    let conn_handle = tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("tokio-postgres connection error: {e}");
        }
    });

    let table = format!("bench_avg_{ix}");
    client
        .batch_execute(&format!("CREATE TABLE {table} (id INT NOT NULL, x INT)"))
        .await
        .with_context(|| format!("CREATE TABLE {table}"))?;
    preload_chunked(&client, &table, n_rows).await?;

    let started = Instant::now();
    let messages = client
        .simple_query(&format!("SELECT AVG(x) FROM {table}"))
        .await
        .with_context(|| format!("SELECT AVG from {table}"))?;
    let elapsed_us = started.elapsed().as_secs_f64() * 1e6;
    let row_count = messages
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count();
    if row_count != 1 {
        anyhow::bail!("AVG row count mismatch: expected 1, observed {row_count}");
    }

    drop(client);
    conn_handle.abort();
    Ok(elapsed_us)
}

/// Run one iteration of `SELECT SUM(x) FROM t WHERE x > <threshold>`.
/// Threshold is `n_rows / 2` so roughly half the rows survive the
/// predicate; this exercises `Filter` + `HashAggregate` on top of `SeqScan`.
async fn run_filter_sum_iter(server: SocketAddr, n_rows: usize, ix: usize) -> Result<f64> {
    let conn_str = format!("host=127.0.0.1 port={} user=ultrasql_bench", server.port());
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .context("tokio-postgres connect to ultrasqld")?;
    let conn_handle = tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("tokio-postgres connection error: {e}");
        }
    });

    let table = format!("bench_filter_sum_{ix}");
    client
        .batch_execute(&format!("CREATE TABLE {table} (id INT NOT NULL, x INT)"))
        .await
        .with_context(|| format!("CREATE TABLE {table}"))?;
    preload_chunked(&client, &table, n_rows).await?;

    let threshold = n_rows / 2;
    let started = Instant::now();
    let messages = client
        .simple_query(&format!("SELECT SUM(x) FROM {table} WHERE x > {threshold}"))
        .await
        .with_context(|| format!("SELECT SUM(...) WHERE from {table}"))?;
    let elapsed_us = started.elapsed().as_secs_f64() * 1e6;
    let row_count = messages
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count();
    if row_count != 1 {
        anyhow::bail!("FILTER SUM row count mismatch: expected 1, observed {row_count}");
    }

    drop(client);
    conn_handle.abort();
    Ok(elapsed_us)
}

/// Run one bulk-UPDATE iteration.
///
/// Preloads `n_rows` of `(id INT, val INT)` outside the timed region,
/// then times a single Simple-Query
/// `UPDATE bench_update_{ix} SET val = val + 1 WHERE id < <n_rows>`.
///
/// The shape mirrors `benchmarks/scripts/run_postgres_writes.sh::run_update`
/// — the postgres script uses `BETWEEN 0 AND 9999` while UltraSQL's
/// v0.5 binder does not yet recognise `BETWEEN` (parser limitation as
/// of this commit), so the predicate is rewritten to `id < n_rows`
/// which selects the identical row set on this monotonic-id preload.
async fn run_update_iter(server: SocketAddr, n_rows: usize, ix: usize) -> Result<f64> {
    let conn_str = format!("host=127.0.0.1 port={} user=ultrasql_bench", server.port());
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .context("tokio-postgres connect to ultrasqld")?;
    let conn_handle = tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("tokio-postgres connection error: {e}");
        }
    });

    let table = format!("bench_update_{ix}");
    client
        .batch_execute(&format!("CREATE TABLE {table} (id INT NOT NULL, val INT)"))
        .await
        .with_context(|| format!("CREATE TABLE {table}"))?;
    preload_chunked(&client, &table, n_rows).await?;

    let started = Instant::now();
    let messages = client
        .simple_query(&format!(
            "UPDATE {table} SET val = val + 1 WHERE id < {n_rows}"
        ))
        .await
        .with_context(|| format!("UPDATE {table}"))?;
    let elapsed_us = started.elapsed().as_secs_f64() * 1e6;
    // CommandComplete carries the row count; a sanity-check tag would
    // require parsing — verifying that the simple_query returned at
    // least a CommandComplete is sufficient here.
    if !messages
        .iter()
        .any(|m| matches!(m, tokio_postgres::SimpleQueryMessage::CommandComplete(_)))
    {
        anyhow::bail!("UPDATE returned no CommandComplete message");
    }

    drop(client);
    conn_handle.abort();
    Ok(elapsed_us)
}

/// Run one bulk-DELETE iteration. See [`run_update_iter`] for the
/// shape; only the SQL statement differs.
async fn run_delete_iter(server: SocketAddr, n_rows: usize, ix: usize) -> Result<f64> {
    let conn_str = format!("host=127.0.0.1 port={} user=ultrasql_bench", server.port());
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .context("tokio-postgres connect to ultrasqld")?;
    let conn_handle = tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("tokio-postgres connection error: {e}");
        }
    });

    let table = format!("bench_delete_{ix}");
    client
        .batch_execute(&format!("CREATE TABLE {table} (id INT NOT NULL, val INT)"))
        .await
        .with_context(|| format!("CREATE TABLE {table}"))?;
    preload_chunked(&client, &table, n_rows).await?;

    let started = Instant::now();
    let messages = client
        .simple_query(&format!("DELETE FROM {table} WHERE id < {n_rows}"))
        .await
        .with_context(|| format!("DELETE FROM {table}"))?;
    let elapsed_us = started.elapsed().as_secs_f64() * 1e6;
    if !messages
        .iter()
        .any(|m| matches!(m, tokio_postgres::SimpleQueryMessage::CommandComplete(_)))
    {
        anyhow::bail!("DELETE returned no CommandComplete message");
    }

    drop(client);
    conn_handle.abort();
    Ok(elapsed_us)
}

/// Shared-table SELECT-scan workload: preload `n_rows` once, then
/// drain `SELECT id, val FROM t` N times in a row on the same
/// relation (warmup + measured iters) under a single
/// `tokio-postgres` connection.
///
/// Matches the methodology every competitor script uses (the
/// preload is paid once outside the timed region, the persistent
/// driver connection runs N queries against the same materialised
/// relation). Mirrors `run_clickhouse_writes.sh::run_select_scan`.
async fn run_shared_select_scan(
    server: SocketAddr,
    n_rows: usize,
    warmup: usize,
    total_iters: usize,
    iters_us: &mut Vec<f64>,
) -> Result<()> {
    let conn_str = format!("host=127.0.0.1 port={} user=ultrasql_bench", server.port());
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
async fn run_shared_olap_aggregate<F>(
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
    let conn_str = format!("host=127.0.0.1 port={} user=ultrasql_bench", server.port());
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

/// Shared-table window-function workload: preload `n_rows` once,
/// then drain `SELECT id, row_number() OVER (ORDER BY x) FROM t` N
/// times against the same `(id INT, x INT)` relation under a single
/// `tokio-postgres` connection.
///
/// Mirrors every competitor script's `run_window_row_number`. The
/// query covers the new v0.5 `LogicalPlan::Window` + `WindowAgg` wire
/// path end-to-end; each iteration drains every row through the
/// wire as `tokio_postgres::SimpleQueryMessage::Row`.
async fn run_shared_window_row_number(
    server: SocketAddr,
    n_rows: usize,
    warmup: usize,
    total_iters: usize,
    iters_us: &mut Vec<f64>,
) -> Result<()> {
    let conn_str = format!("host=127.0.0.1 port={} user=ultrasql_bench", server.port());
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

/// Mixed-OLTP pgbench-like 1-second-window workload.
///
/// Preloads `n_rows` of `(id INT, val INT)` outside the timed region
/// (one persistent wire connection), then runs operations in a tight
/// loop for `MIXED_WINDOW_SECS` real-time seconds: 50% point reads,
/// 30% point updates, 20% inserts (monotonic `id` past the preload).
/// Returns elapsed-microseconds / op_count to match the competitor
/// scripts' `µs/op` shape (`benchmarks/scripts/run_*_writes.sh::run_mixed`).
async fn run_mixed_oltp_iter(server: SocketAddr, n_rows: usize, ix: usize) -> Result<f64> {
    use std::time::Duration;

    /// Mirrors `benchmarks/scripts/run_*_writes.sh::run_mixed` window.
    const MIXED_WINDOW_SECS: f64 = 1.0;

    let conn_str = format!("host=127.0.0.1 port={} user=ultrasql_bench", server.port());
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
        let r = rng.next_unit_f64();
        if r < 0.50 {
            let row_id = i64::try_from(rng.next_u64() % n_rows_u64).unwrap_or(0);
            let _ = client
                .simple_query(&format!("SELECT val FROM {table} WHERE id = {row_id}"))
                .await
                .with_context(|| format!("SELECT WHERE id = ? on {table}"))?;
        } else if r < 0.80 {
            let row_id = i64::try_from(rng.next_u64() % n_rows_u64).unwrap_or(0);
            let _ = client
                .simple_query(&format!(
                    "UPDATE {table} SET val = val + 1 WHERE id = {row_id}"
                ))
                .await
                .with_context(|| format!("UPDATE WHERE id = ? on {table}"))?;
        } else {
            let new_val = rng.next_i32();
            let _ = client
                .simple_query(&format!(
                    "INSERT INTO {table} (id, val) VALUES ({next_id}, {new_val})"
                ))
                .await
                .with_context(|| format!("INSERT into {table}"))?;
            next_id += 1;
        }
        count += 1;
    }
    let elapsed_us = started.elapsed().as_secs_f64() * 1e6;
    let us_per_op = elapsed_us / count.max(1) as f64;

    drop(client);
    conn_handle.abort();
    Ok(us_per_op)
}

/// Compact deterministic SplitMix64 PRNG. Same constants every
/// engine's bench script uses to derive an op stream from the
/// per-iteration seed; kept inline to avoid pulling `rand` into the
/// bench crate dependency tree.
struct SplitMix64(u64);

impl SplitMix64 {
    const fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn next_unit_f64(&mut self) -> f64 {
        // 53 high bits → [0, 1) uniform double, per the standard
        // SplitMix64 → f64 mapping. Matches the SQLite/DuckDB Python
        // baselines' `random.random()` distribution closely enough that
        // the per-op mix matches across engines.
        let bits = self.next_u64() >> 11;
        let scale = 1.0_f64 / (1_u64 << 53) as f64;
        bits as f64 * scale
    }

    fn next_i32(&mut self) -> i32 {
        self.next_u64() as i32
    }
}
