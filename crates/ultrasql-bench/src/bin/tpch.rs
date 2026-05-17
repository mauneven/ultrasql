//! TPC-H scale-1 benchmark driver.
//!
//! Entry point for the `tpch` binary. Parses CLI arguments and dispatches to
//! the appropriate sub-command in the `ultrasql_bench::tpch` module.
//!
//! ## Subcommands
//!
//! | Subcommand | Description |
//! |------------|-------------|
//! | `init-schema <engine>` | Print DDL for `ultrasql` or `postgres` |
//! | `gen-data <scale> <out-dir>` | Generate `.tbl` files (default scale 1) |
//! | `load <engine> <data-dir>` | Load `.tbl` files into the target engine |
//! | `run-queries <engine>` | Run all 22 queries; optionally write baseline |
//! | `compare <baseline.json>` | Re-run queries; fail on >5% regression |

#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::collections::BTreeMap;
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand, ValueEnum};

use ultrasql_bench::tpch::{baseline, data_gen, load, queries, runner, schema};

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

/// TPC-H scale-1 benchmark harness for UltraSQL.
#[derive(Parser, Debug)]
#[command(name = "tpch", about = "TPC-H scale-1 benchmark harness")]
struct Cli {
    #[command(subcommand)]
    command: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Print DDL for all 8 TPC-H tables targeting the specified engine.
    InitSchema {
        /// Target engine: `ultrasql` or `postgres`.
        engine: EngineArg,
    },

    /// Generate TPC-H `.tbl` files.
    ///
    /// Uses `dbgen` if available on `$PATH`; falls back to a deterministic
    /// synthetic generator otherwise.
    GenData {
        /// TPC-H scale factor (positive integer; 1 = ~1 GB raw data).
        #[arg(default_value_t = 1)]
        scale: u32,

        /// Directory to write `.tbl` files into (created if absent).
        #[arg(default_value = "tpch-data")]
        out_dir: PathBuf,
    },

    /// Load `.tbl` files from `data_dir` into the target engine.
    Load {
        /// Target engine: `ultrasql` or `postgres`.
        engine: EngineArg,

        /// Directory containing the `.tbl` files.
        data_dir: PathBuf,

        /// PostgreSQL connection string (required when `engine = postgres`).
        #[arg(long, default_value = "host=localhost user=postgres dbname=tpch")]
        pg_dsn: String,
    },

    /// Run all 22 TPC-H queries and optionally write a baseline JSON file.
    RunQueries {
        /// Target engine: `ultrasql` or `postgres`.
        engine: EngineArg,

        /// Directory containing the `.tbl` files for UltraSQL runs.
        #[arg(long, default_value = "tpch-data")]
        data_dir: PathBuf,

        /// Number of measured runs per query (after warmup).
        #[arg(long, default_value_t = 5)]
        runs: usize,

        /// Number of warmup runs discarded before measurement.
        #[arg(long, default_value_t = 1)]
        warmup: usize,

        /// Path to write baseline JSON (optional).
        #[arg(long)]
        out: Option<PathBuf>,

        /// PostgreSQL connection string (required when `engine = postgres`).
        #[arg(long, default_value = "host=localhost user=postgres dbname=tpch")]
        pg_dsn: String,
    },

    /// Compare a new run against a recorded baseline; exit non-zero on >5% regression.
    Compare {
        /// Path to the existing baseline JSON.
        baseline: PathBuf,

        /// Directory containing the `.tbl` files for UltraSQL runs.
        #[arg(long, default_value = "tpch-data")]
        data_dir: PathBuf,

        /// PostgreSQL connection string (required when running against postgres).
        #[arg(long, default_value = "host=localhost user=postgres dbname=tpch")]
        pg_dsn: String,

        /// Number of measured runs for the comparison run.
        #[arg(long, default_value_t = 5)]
        runs: usize,

        /// Number of warmup runs discarded before measurement.
        #[arg(long, default_value_t = 1)]
        warmup: usize,
    },
}

/// Engine selection for CLI arguments.
#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
enum EngineArg {
    /// UltraSQL.
    Ultrasql,
    /// PostgreSQL.
    Postgres,
}

impl EngineArg {
    const fn to_schema_engine(self) -> schema::Engine {
        match self {
            Self::Ultrasql => schema::Engine::Ultrasql,
            Self::Postgres => schema::Engine::Postgres,
        }
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() -> std::process::ExitCode {
    match run() {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e:#}");
            std::process::ExitCode::FAILURE
        }
    }
}

fn run() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Cmd::InitSchema { engine } => {
            cmd_init_schema(engine);
            Ok(())
        }
        Cmd::GenData { scale, out_dir } => cmd_gen_data(scale, &out_dir),
        Cmd::Load {
            engine,
            data_dir,
            pg_dsn,
        } => cmd_load(engine, &data_dir, &pg_dsn),
        Cmd::RunQueries {
            engine,
            data_dir,
            runs,
            warmup,
            out,
            pg_dsn,
        } => cmd_run_queries(engine, &data_dir, runs, warmup, out.as_deref(), &pg_dsn),
        Cmd::Compare {
            baseline,
            data_dir,
            runs,
            warmup,
            pg_dsn,
        } => cmd_compare(&baseline, &data_dir, runs, warmup, &pg_dsn),
    }
}

// ---------------------------------------------------------------------------
// Subcommand handlers
// ---------------------------------------------------------------------------

fn cmd_init_schema(engine: EngineArg) {
    let ddl = schema::ddl_for_engine(engine.to_schema_engine());
    for stmt in ddl {
        println!("{stmt}");
        println!();
    }
}

fn cmd_gen_data(scale: u32, out_dir: &std::path::Path) -> Result<()> {
    if !out_dir.exists() {
        std::fs::create_dir_all(out_dir)
            .with_context(|| format!("create {}", out_dir.display()))?;
    }
    let result = data_gen::generate(scale, out_dir).context("data generation")?;
    if result.used_dbgen {
        println!(
            "Generated TPC-H sf{scale} data with dbgen in {}",
            out_dir.display()
        );
    } else {
        println!(
            "Generated synthetic TPC-H sf{scale} data (dbgen unavailable) in {}",
            out_dir.display()
        );
        eprintln!(
            "WARNING: synthetic data is NOT TPC-H-compliant. \
                   Results are not comparable to published benchmarks."
        );
    }
    Ok(())
}

fn cmd_load(engine: EngineArg, data_dir: &std::path::Path, pg_dsn: &str) -> Result<()> {
    match engine {
        EngineArg::Postgres => cmd_load_postgres(data_dir, pg_dsn),
        EngineArg::Ultrasql => {
            for stats in load::load_ultrasql(data_dir)? {
                println!(
                    "  loaded {:>12} rows into {:>12} ({:.0} rows/s)",
                    stats.row_count, stats.table, stats.rows_per_sec
                );
            }
            Ok(())
        }
    }
}

#[cfg(feature = "pg-runner")]
fn cmd_load_postgres(data_dir: &std::path::Path, pg_dsn: &str) -> Result<()> {
    let rt = tokio::runtime::Runtime::new().context("tokio runtime")?;
    let mut client = rt
        .block_on(async {
            let (client, conn) = tokio_postgres::connect(pg_dsn, tokio_postgres::NoTls)
                .await
                .context("connect to postgres")?;
            tokio::spawn(conn);
            Ok::<_, anyhow::Error>(client)
        })
        .context("pg connect")?;

    for table in data_gen::TABLE_NAMES {
        let stats = load::load_postgres(&mut client, table, data_dir, &rt)?;
        println!(
            "  loaded {:>12} rows into {:>12} ({:.0} rows/s)",
            stats.row_count, stats.table, stats.rows_per_sec
        );
    }
    Ok(())
}

#[cfg(not(feature = "pg-runner"))]
fn cmd_load_postgres(_data_dir: &std::path::Path, _pg_dsn: &str) -> Result<()> {
    bail!(
        "NotYetWired: pg-runner feature is not enabled; \
         rebuild with --features pg-runner"
    )
}

fn cmd_run_queries(
    engine: EngineArg,
    data_dir: &std::path::Path,
    runs: usize,
    warmup: usize,
    out: Option<&std::path::Path>,
    pg_dsn: &str,
) -> Result<()> {
    let run_result = match engine {
        EngineArg::Postgres => run_queries_postgres(warmup, runs, pg_dsn)?,
        EngineArg::Ultrasql => runner::run_ultrasql(data_dir, warmup, runs)?,
    };

    // Print per-query summary.
    println!("{:<6}  {:>10}  {:>10}", "query", "median_ms", "p95_ms");
    println!("{}", "-".repeat(32));
    for (label, t) in &run_result.timings {
        println!("{label:<6}  {:>10.1}  {:>10.1}", t.median_ms, t.p95_ms);
    }
    let gm = runner::geometric_mean(&run_result);
    println!("{}", "-".repeat(32));
    println!("{:<6}  {:>10.1}  (geometric mean)", "gm", gm);

    // Write baseline if requested.
    if let Some(path) = out {
        let b = build_baseline(engine, &run_result);
        b.write(path)
            .with_context(|| format!("write baseline to {}", path.display()))?;
        println!("Baseline written to {}", path.display());
    }
    Ok(())
}

fn cmd_compare(
    baseline_path: &std::path::Path,
    data_dir: &std::path::Path,
    runs: usize,
    warmup: usize,
    pg_dsn: &str,
) -> Result<()> {
    let recorded = baseline::Baseline::read(baseline_path)?;
    let engine = parse_engine_from_baseline(&recorded.engine)?;
    let current_run = match engine {
        EngineArg::Postgres => run_queries_postgres(warmup, runs, pg_dsn)?,
        EngineArg::Ultrasql => runner::run_ultrasql(data_dir, warmup, runs)?,
    };
    let current = build_baseline(engine, &current_run);
    baseline::compare(&recorded, &current)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

#[cfg(feature = "pg-runner")]
fn run_queries_postgres(warmup: usize, runs: usize, pg_dsn: &str) -> Result<runner::RunResult> {
    let rt = tokio::runtime::Runtime::new().context("tokio runtime")?;
    let mut client = rt
        .block_on(async {
            let (client, conn) = tokio_postgres::connect(pg_dsn, tokio_postgres::NoTls)
                .await
                .context("connect to postgres")?;
            tokio::spawn(conn);
            Ok::<_, anyhow::Error>(client)
        })
        .context("pg connect")?;
    runner::run_postgres(&mut client, warmup, runs, &rt)
}

#[cfg(not(feature = "pg-runner"))]
fn run_queries_postgres(_warmup: usize, _runs: usize, _pg_dsn: &str) -> Result<runner::RunResult> {
    bail!(
        "NotYetWired: pg-runner feature is not enabled; \
         rebuild with --features pg-runner"
    )
}

/// Builds a [`baseline::Baseline`] from a run result, collecting host info
/// from `std::env::consts` as a fallback when `sysinfo` is unavailable.
fn build_baseline(engine: EngineArg, result: &runner::RunResult) -> baseline::Baseline {
    let host = collect_host_descriptor();
    let engine_str = match engine {
        EngineArg::Postgres => "postgres".to_string(),
        EngineArg::Ultrasql => format!("ultrasql@{}", env!("CARGO_PKG_VERSION")),
    };
    let git_commit = option_env!("GIT_COMMIT").unwrap_or("unknown").to_string();
    let cargo_lock_sha256 = hash_cargo_lock();
    let recorded_at = chrono_now();

    let queries_map: BTreeMap<String, baseline::QueryTimings> = result
        .timings
        .iter()
        .map(|(label, timing)| (label.clone(), timing.clone()))
        .collect();

    baseline::Baseline {
        schema_version: baseline::SCHEMA_VERSION,
        scale_factor: 1,
        engine: engine_str,
        host,
        git_commit,
        cargo_lock_sha256,
        recorded_at,
        queries: queries_map,
    }
}

/// Collects a host descriptor using `std::env::consts` when `sysinfo` is
/// not available.
fn collect_host_descriptor() -> baseline::HostDescriptor {
    baseline::HostDescriptor {
        cpu: std::env::var("BENCH_CPU_MODEL")
            .unwrap_or_else(|_| std::env::consts::ARCH.to_string()),
        cores: std::env::var("BENCH_CPU_CORES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0),
        ram_gb: std::env::var("BENCH_RAM_GB")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0),
        os: format!(
            "{} {}",
            std::env::consts::OS,
            std::env::var("BENCH_OS_VERSION").unwrap_or_else(|_| "unknown".to_string())
        ),
    }
}

/// Returns a SHA-256 hex digest of `Cargo.lock` in the workspace root, or a
/// placeholder when the file is not accessible.
fn hash_cargo_lock() -> String {
    use sha2::{Digest, Sha256};
    // Walk up from the binary location or just try the CWD.
    let candidates = [
        PathBuf::from("Cargo.lock"),
        PathBuf::from("../../Cargo.lock"),
        PathBuf::from("../../../Cargo.lock"),
    ];
    for path in &candidates {
        if let Ok(data) = std::fs::read(path) {
            let mut hasher = Sha256::new();
            hasher.update(&data);
            let digest = hasher.finalize();
            return hex_encode(&digest); // early return on first hit
        }
    }
    "0".repeat(64)
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut acc, b| {
            use std::fmt::Write as _;
            write!(acc, "{b:02x}").expect("String write is infallible");
            acc
        })
}

/// Returns an ISO-8601 timestamp string for right now, using `BENCH_TIMESTAMP`
/// env override when set (useful in tests).
fn chrono_now() -> String {
    if let Ok(ts) = std::env::var("BENCH_TIMESTAMP") {
        return ts;
    }
    // Use a minimal hand-rolled approach to avoid pulling chrono into the
    // default feature set; callers can supply BENCH_TIMESTAMP in tests.
    // In practice the pg-runner path will have chrono available.
    #[cfg(feature = "pg-runner")]
    {
        chrono::Utc::now().to_rfc3339()
    }
    #[cfg(not(feature = "pg-runner"))]
    {
        "unknown".to_string()
    }
}

/// Parses an engine identifier from a baseline `engine` field.
///
/// Recognises `"postgres..."` and `"ultrasql..."` prefixes.
fn parse_engine_from_baseline(engine_str: &str) -> Result<EngineArg> {
    if engine_str.starts_with("postgres") {
        Ok(EngineArg::Postgres)
    } else if engine_str.starts_with("ultrasql") {
        Ok(EngineArg::Ultrasql)
    } else {
        bail!("unknown engine in baseline: {engine_str}")
    }
}

/// Ensure all 22 query texts can be fetched; used in integration testing.
#[allow(dead_code)]
fn all_query_texts() -> Vec<(&'static str, &'static str)> {
    (1u8..=22)
        .filter_map(|n| queries::query(n).map(|sql| (sql, sql)))
        .collect()
}
