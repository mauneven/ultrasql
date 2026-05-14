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
}

impl Workload {
    fn registry_id(self, n_rows: usize) -> String {
        match self {
            Self::InsertBulk => format!("insert_throughput_{}", k_or_raw(n_rows)),
            Self::SelectScan => format!("select_scan_{}", k_or_raw(n_rows)),
        }
    }
}

/// Render a row count using `10k` / `1m` notation matching the
/// existing competitor workload ids (`insert_throughput_10k`).
fn k_or_raw(n: usize) -> String {
    if n >= 1_000_000 && n % 1_000_000 == 0 {
        format!("{}m", n / 1_000_000)
    } else if n >= 1_000 && n % 1_000 == 0 {
        format!("{}k", n / 1_000)
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
    for i in 0..total_iters {
        let micros = match args.workload {
            Workload::InsertBulk => run_insert_iter(bound, args.rows, i).await?,
            Workload::SelectScan => run_select_iter(bound, args.rows, i).await?,
        };
        if i >= args.warmup {
            iters_us.push(micros);
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
