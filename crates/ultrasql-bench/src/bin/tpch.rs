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
//! | `validate-results` | Compare UltraSQL query rows against DuckDB |
//! | `compare <baseline.json>` | Re-run queries; fail on >5% regression |

#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::collections::BTreeMap;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand, ValueEnum};
use sha2::{Digest, Sha256};

use ultrasql_bench::tpch::{baseline, data_gen, load, queries, runner, schema};

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

/// TPC-H benchmark harness for UltraSQL.
#[derive(Parser, Debug)]
#[command(name = "tpch", about = "TPC-H benchmark harness")]
struct Cli {
    #[command(subcommand)]
    command: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Print DDL for all 8 TPC-H tables targeting the specified engine.
    InitSchema {
        /// Target engine: `ultrasql`, `postgres`, or `duckdb`.
        engine: EngineArg,
    },

    /// Generate TPC-H `.tbl` files.
    ///
    /// Uses external `dbgen` from `ULTRASQL_TPCH_DBGEN`, `target/tools`, or
    /// `$PATH`; falls back to a deterministic synthetic generator otherwise.
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
        /// Target engine: `ultrasql`, `postgres`, or `duckdb`.
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

        /// Path to `duckdb` CLI binary (required when `engine = duckdb`).
        #[arg(long, default_value = "duckdb")]
        duckdb: PathBuf,

        /// TPC-H scale factor recorded in the output baseline.
        #[arg(long, default_value_t = 1)]
        scale: u32,

        /// Query selector: `all`, `N`, `A-B`, or comma/space separated list.
        #[arg(long, value_name = "LIST")]
        queries: Option<String>,
    },

    /// Compare UltraSQL TPC-H query results against a local DuckDB reference.
    ValidateResults {
        /// Directory containing the `.tbl` files.
        #[arg(long, default_value = "tpch-data")]
        data_dir: PathBuf,

        /// Path to `duckdb` CLI binary.
        #[arg(long, default_value = "duckdb")]
        duckdb: PathBuf,

        /// Query selector: `all`, `N`, `A-B`, or comma/space separated list.
        #[arg(long, value_name = "LIST")]
        queries: Option<String>,

        /// Directory for cached DuckDB expected rows.
        #[arg(long, default_value = "target/tpch-cache")]
        duckdb_cache_dir: PathBuf,

        /// Rebuild cached DuckDB expected rows even if a cache entry exists.
        #[arg(long)]
        refresh_duckdb_cache: bool,

        /// Keep running selected UltraSQL queries after per-query failures.
        #[arg(long)]
        keep_going: bool,
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

        /// Query selector: `all`, `N`, `A-B`, or comma/space separated list.
        #[arg(long, value_name = "LIST")]
        queries: Option<String>,
    },
}

/// Engine selection for CLI arguments.
#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
enum EngineArg {
    /// UltraSQL.
    Ultrasql,
    /// PostgreSQL.
    Postgres,
    /// DuckDB.
    Duckdb,
}

struct RunQueryOptions<'a> {
    engine: EngineArg,
    data_dir: &'a Path,
    runs: usize,
    warmup: usize,
    out: Option<&'a Path>,
    pg_dsn: &'a str,
    duckdb_bin: &'a Path,
    scale: u32,
    query_selector: Option<&'a str>,
}

impl EngineArg {
    const fn to_schema_engine(self) -> schema::Engine {
        match self {
            Self::Ultrasql => schema::Engine::Ultrasql,
            Self::Postgres | Self::Duckdb => schema::Engine::Postgres,
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
            duckdb,
            scale,
            queries,
        } => cmd_run_queries(RunQueryOptions {
            engine,
            data_dir: &data_dir,
            runs,
            warmup,
            out: out.as_deref(),
            pg_dsn: &pg_dsn,
            duckdb_bin: &duckdb,
            scale,
            query_selector: queries.as_deref(),
        }),
        Cmd::ValidateResults {
            data_dir,
            duckdb,
            queries,
            duckdb_cache_dir,
            refresh_duckdb_cache,
            keep_going,
        } => cmd_validate_results(
            &data_dir,
            &duckdb,
            queries.as_deref(),
            &duckdb_cache_dir,
            refresh_duckdb_cache,
            keep_going,
        ),
        Cmd::Compare {
            baseline,
            data_dir,
            runs,
            warmup,
            pg_dsn,
            queries,
        } => cmd_compare(
            &baseline,
            &data_dir,
            runs,
            warmup,
            &pg_dsn,
            queries.as_deref(),
        ),
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
        EngineArg::Duckdb => bail!("DuckDB load is folded into `run-queries duckdb`"),
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

fn cmd_run_queries(opts: RunQueryOptions<'_>) -> Result<()> {
    let queries = runner::selected_queries(opts.query_selector)?;
    let run_result = match opts.engine {
        EngineArg::Postgres => run_queries_postgres(opts.warmup, opts.runs, opts.pg_dsn, &queries)?,
        EngineArg::Duckdb => run_queries_duckdb(
            opts.data_dir,
            opts.duckdb_bin,
            opts.warmup,
            opts.runs,
            &queries,
        )?,
        EngineArg::Ultrasql => {
            runner::run_ultrasql(opts.data_dir, opts.warmup, opts.runs, &queries)?
        }
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
    if let Some(path) = opts.out {
        let b = build_baseline(opts.engine, &run_result, opts.scale, Some(opts.duckdb_bin));
        b.write(path)
            .with_context(|| format!("write baseline to {}", path.display()))?;
        println!("Baseline written to {}", path.display());
    }
    Ok(())
}

fn cmd_validate_results(
    data_dir: &Path,
    duckdb_bin: &Path,
    query_selector: Option<&str>,
    duckdb_cache_dir: &Path,
    refresh_duckdb_cache: bool,
    keep_going: bool,
) -> Result<()> {
    let queries = runner::selected_queries(query_selector)?;
    let expected = run_duckdb_results_cached(
        data_dir,
        duckdb_bin,
        &queries,
        duckdb_cache_dir,
        refresh_duckdb_cache,
    )
    .context("run DuckDB reference")?;
    if keep_going {
        let actual = runner::run_ultrasql_result_outcomes(data_dir, &queries, true)
            .context("run UltraSQL results")?;
        compare_result_outcomes(&expected, &actual)?;
        println!(
            "validated {} TPC-H query result set(s) against DuckDB",
            actual.len()
        );
        return Ok(());
    }
    let actual =
        runner::run_ultrasql_results(data_dir, &queries).context("run UltraSQL results")?;
    compare_result_sets(&expected, &actual)?;
    println!(
        "validated {} TPC-H query result set(s) against DuckDB",
        actual.len()
    );
    Ok(())
}

fn cmd_compare(
    baseline_path: &std::path::Path,
    data_dir: &std::path::Path,
    runs: usize,
    warmup: usize,
    pg_dsn: &str,
    query_selector: Option<&str>,
) -> Result<()> {
    let recorded = baseline::Baseline::read(baseline_path)?;
    let engine = parse_engine_from_baseline(&recorded.engine)?;
    let queries = runner::selected_queries(query_selector)?;
    let current_run = match engine {
        EngineArg::Postgres => run_queries_postgres(warmup, runs, pg_dsn, &queries)?,
        EngineArg::Duckdb => bail!("compare against DuckDB baselines via `run-queries duckdb`"),
        EngineArg::Ultrasql => runner::run_ultrasql(data_dir, warmup, runs, &queries)?,
    };
    let current = build_baseline(engine, &current_run, recorded.scale_factor, None);
    baseline::compare(&recorded, &current)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

#[cfg(feature = "pg-runner")]
fn run_queries_postgres(
    warmup: usize,
    runs: usize,
    pg_dsn: &str,
    queries: &[u8],
) -> Result<runner::RunResult> {
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
    runner::run_postgres(&mut client, warmup, runs, &rt, queries)
}

fn run_queries_duckdb(
    data_dir: &Path,
    duckdb_bin: &Path,
    warmup: usize,
    runs: usize,
    queries: &[u8],
) -> Result<runner::RunResult> {
    use std::time::Instant;

    let tmp = tempfile::tempdir().context("create temporary DuckDB directory")?;
    let db_path = tmp.path().join("tpch.duckdb");
    let setup_sql = duckdb_setup_sql(data_dir)?;
    run_duckdb_sql(duckdb_bin, &db_path, &setup_sql).context("initialize DuckDB timing DB")?;

    let mut timings = Vec::with_capacity(queries.len());
    for &n in queries {
        let label = format!("q{n}");
        let sql = runner::query_sql(n)?;
        for _ in 0..warmup {
            run_duckdb_query(duckdb_bin, &db_path, sql.as_ref())
                .with_context(|| format!("warmup {label}"))?;
        }

        let mut elapsed_ms = Vec::with_capacity(runs);
        for _ in 0..runs {
            let t0 = Instant::now();
            run_duckdb_query(duckdb_bin, &db_path, sql.as_ref())
                .with_context(|| format!("run {label}"))?;
            elapsed_ms.push(t0.elapsed().as_secs_f64() * 1_000.0);
        }

        timings.push((
            label,
            baseline::QueryTimings {
                median_ms: baseline::median(&elapsed_ms),
                p95_ms: baseline::p95(&elapsed_ms),
                runs: elapsed_ms,
            },
        ));
    }

    Ok(runner::RunResult { timings })
}

#[cfg(not(feature = "pg-runner"))]
fn run_queries_postgres(
    _warmup: usize,
    _runs: usize,
    _pg_dsn: &str,
    _queries: &[u8],
) -> Result<runner::RunResult> {
    bail!(
        "NotYetWired: pg-runner feature is not enabled; \
         rebuild with --features pg-runner"
    )
}

/// Builds a [`baseline::Baseline`] from a run result, collecting host info
/// from `std::env::consts` as a fallback when `sysinfo` is unavailable.
fn build_baseline(
    engine: EngineArg,
    result: &runner::RunResult,
    scale: u32,
    duckdb_bin: Option<&Path>,
) -> baseline::Baseline {
    let host = collect_host_descriptor();
    let engine_str = match engine {
        EngineArg::Postgres => "postgres".to_string(),
        EngineArg::Duckdb => {
            duckdb_engine_string(duckdb_bin.unwrap_or_else(|| Path::new("duckdb")))
        }
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
        scale_factor: scale,
        engine: engine_str,
        host,
        git_commit,
        cargo_lock_sha256,
        recorded_at,
        queries: queries_map,
    }
}

fn duckdb_engine_string(duckdb_bin: &Path) -> String {
    std::process::Command::new(duckdb_bin)
        .arg("--version")
        .output()
        .ok()
        .and_then(|output| {
            if output.status.success() {
                String::from_utf8(output.stdout).ok()
            } else {
                None
            }
        })
        .map(|version| format!("duckdb@{}", version.trim()))
        .unwrap_or_else(|| "duckdb".to_string())
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
    } else if engine_str.starts_with("duckdb") {
        Ok(EngineArg::Duckdb)
    } else {
        bail!("unknown engine in baseline: {engine_str}")
    }
}

#[derive(serde::Deserialize, serde::Serialize)]
struct DuckDbResultCache {
    version: u32,
    fingerprint: String,
    queries: Vec<u8>,
    rows: Vec<runner::QueryRows>,
}

fn run_duckdb_results_cached(
    data_dir: &Path,
    duckdb_bin: &Path,
    queries: &[u8],
    cache_dir: &Path,
    refresh: bool,
) -> Result<Vec<runner::QueryRows>> {
    const CACHE_VERSION: u32 = 1;

    let fingerprint = duckdb_cache_fingerprint(data_dir, duckdb_bin, queries)?;
    let cache_path = cache_dir.join(format!("duckdb-results-{fingerprint}.json"));
    if !refresh && cache_path.exists() {
        let raw = std::fs::read_to_string(&cache_path)
            .with_context(|| format!("read {}", cache_path.display()))?;
        let cache: DuckDbResultCache = serde_json::from_str(&raw)
            .with_context(|| format!("parse {}", cache_path.display()))?;
        if cache.version == CACHE_VERSION
            && cache.fingerprint == fingerprint
            && cache.queries == queries
        {
            if progress_enabled() {
                eprintln!("duckdb tpch validate: cache hit {}", cache_path.display());
            }
            return Ok(cache.rows);
        }
    }

    let rows = run_duckdb_results(data_dir, duckdb_bin, queries)?;
    std::fs::create_dir_all(cache_dir)
        .with_context(|| format!("create {}", cache_dir.display()))?;
    let cache = DuckDbResultCache {
        version: CACHE_VERSION,
        fingerprint,
        queries: queries.to_vec(),
        rows: rows.clone(),
    };
    let json = serde_json::to_string_pretty(&cache).context("serialize DuckDB result cache")?;
    std::fs::write(&cache_path, json).with_context(|| format!("write {}", cache_path.display()))?;
    if progress_enabled() {
        eprintln!("duckdb tpch validate: cache write {}", cache_path.display());
    }
    Ok(rows)
}

fn duckdb_cache_fingerprint(data_dir: &Path, duckdb_bin: &Path, queries: &[u8]) -> Result<String> {
    let mut hasher = Sha256::new();
    hasher.update(b"ultrasql-tpch-duckdb-cache-v1");
    hasher.update(duckdb_bin.display().to_string().as_bytes());
    for table in data_gen::TABLE_NAMES {
        hasher.update(table.as_bytes());
        fingerprint_tbl_file(&data_dir.join(format!("{table}.tbl")), &mut hasher)?;
    }
    for &query in queries {
        hasher.update([query]);
        let sql = runner::query_sql(query)?;
        hasher.update(sql.as_ref().as_bytes());
    }
    Ok(hex_encode(&hasher.finalize()))
}

fn fingerprint_tbl_file(path: &Path, hasher: &mut Sha256) -> Result<()> {
    const SAMPLE_BYTES: usize = 64 * 1024;
    const SAMPLE_BYTES_I64: i64 = 64 * 1024;
    const SAMPLE_BYTES_U64: u64 = 64 * 1024;

    let mut file = std::fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
    let metadata = file
        .metadata()
        .with_context(|| format!("metadata {}", path.display()))?;
    hasher.update(path.display().to_string().as_bytes());
    hasher.update(metadata.len().to_le_bytes());
    if let Ok(modified) = metadata.modified() {
        if let Ok(duration) = modified.duration_since(std::time::UNIX_EPOCH) {
            hasher.update(duration.as_secs().to_le_bytes());
            hasher.update(duration.subsec_nanos().to_le_bytes());
        }
    }

    let mut buf = vec![0_u8; SAMPLE_BYTES];
    let front = file
        .read(&mut buf)
        .with_context(|| format!("read head {}", path.display()))?;
    hasher.update(&buf[..front]);

    if metadata.len() > SAMPLE_BYTES_U64 {
        file.seek(SeekFrom::End(-SAMPLE_BYTES_I64))
            .with_context(|| format!("seek tail {}", path.display()))?;
        let tail = file
            .read(&mut buf)
            .with_context(|| format!("read tail {}", path.display()))?;
        hasher.update(&buf[..tail]);
    }
    Ok(())
}

fn run_duckdb_results(
    data_dir: &Path,
    duckdb_bin: &Path,
    queries: &[u8],
) -> Result<Vec<runner::QueryRows>> {
    let tmp = tempfile::tempdir().context("create temporary DuckDB directory")?;
    let db_path = tmp.path().join("tpch.duckdb");
    let setup_sql = duckdb_setup_sql(data_dir)?;
    run_duckdb_sql(duckdb_bin, &db_path, &setup_sql).context("initialize DuckDB reference")?;

    let mut results = Vec::new();
    for &n in queries {
        let label = format!("q{n}");
        let sql = runner::query_sql(n)?;
        let stdout =
            run_duckdb_query(duckdb_bin, &db_path, sql.as_ref()).with_context(|| label.clone())?;
        let rows = parse_csv_rows(&stdout).with_context(|| format!("parse {label} CSV"))?;
        if progress_enabled() {
            eprintln!(
                "duckdb tpch validate: finished {label} ({} rows)",
                rows.len()
            );
        }
        results.push(runner::QueryRows { label, rows });
    }
    Ok(results)
}

fn duckdb_setup_sql(data_dir: &Path) -> Result<String> {
    let mut sql = String::new();
    for ddl in schema::ddl_for_engine(schema::Engine::Ultrasql) {
        sql.push_str(ddl);
        sql.push('\n');
    }
    for table in data_gen::TABLE_NAMES {
        let tbl = data_dir.join(format!("{table}.tbl"));
        let path = sql_path_literal(&tbl)?;
        sql.push_str(&format!(
            "COPY {table} FROM {path} (DELIMITER '|', HEADER false);\n"
        ));
    }
    Ok(sql)
}

fn sql_path_literal(path: &Path) -> Result<String> {
    let absolute =
        std::fs::canonicalize(path).with_context(|| format!("canonicalize {}", path.display()))?;
    let escaped = absolute.display().to_string().replace('\'', "''");
    Ok(format!("'{escaped}'"))
}

fn run_duckdb_sql(duckdb_bin: &Path, db_path: &Path, sql: &str) -> Result<()> {
    let output = Command::new(duckdb_bin)
        .arg(db_path)
        .arg("-c")
        .arg(sql)
        .output()
        .with_context(|| format!("spawn {}", duckdb_bin.display()))?;
    if !output.status.success() {
        bail!(
            "DuckDB failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(())
}

fn run_duckdb_query(duckdb_bin: &Path, db_path: &Path, sql: &str) -> Result<String> {
    let output = Command::new(duckdb_bin)
        .arg(db_path)
        .arg("-csv")
        .arg("-noheader")
        .arg("-nullvalue")
        .arg("\\N")
        .arg("-c")
        .arg(sql)
        .output()
        .with_context(|| format!("spawn {}", duckdb_bin.display()))?;
    if !output.status.success() {
        bail!(
            "DuckDB query failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    String::from_utf8(output.stdout).context("DuckDB emitted non-UTF8 CSV")
}

fn parse_csv_rows(csv: &str) -> Result<Vec<Vec<String>>> {
    let trimmed = csv.strip_suffix('\n').unwrap_or(csv);
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }
    trimmed
        .split('\n')
        .map(|line| parse_csv_line(line.strip_suffix('\r').unwrap_or(line)))
        .collect()
}

fn parse_csv_line(line: &str) -> Result<Vec<String>> {
    let mut cells = Vec::new();
    let mut cell = String::new();
    let mut chars = line.chars().peekable();
    let mut quoted = false;
    while let Some(ch) = chars.next() {
        match ch {
            '"' if quoted && chars.peek() == Some(&'"') => {
                chars.next();
                cell.push('"');
            }
            '"' => quoted = !quoted,
            ',' if !quoted => cells.push(std::mem::take(&mut cell)),
            _ => cell.push(ch),
        }
    }
    if quoted {
        bail!("unterminated quoted CSV field");
    }
    cells.push(cell);
    Ok(cells)
}

fn compare_result_sets(expected: &[runner::QueryRows], actual: &[runner::QueryRows]) -> Result<()> {
    if expected.len() != actual.len() {
        bail!(
            "query count mismatch: DuckDB={} UltraSQL={}",
            expected.len(),
            actual.len()
        );
    }
    for (expected_query, actual_query) in expected.iter().zip(actual) {
        if expected_query.label != actual_query.label {
            bail!(
                "query label mismatch: DuckDB={} UltraSQL={}",
                expected_query.label,
                actual_query.label
            );
        }
        compare_query_rows(expected_query, actual_query)?;
        println!(
            "{} ok ({} rows)",
            actual_query.label,
            actual_query.rows.len()
        );
    }
    Ok(())
}

fn compare_result_outcomes(
    expected: &[runner::QueryRows],
    actual: &[runner::QueryRowsOutcome],
) -> Result<()> {
    if expected.len() != actual.len() {
        bail!(
            "query count mismatch: DuckDB={} UltraSQL={}",
            expected.len(),
            actual.len()
        );
    }

    let mut failures = Vec::new();
    for (expected_query, actual_query) in expected.iter().zip(actual) {
        if expected_query.label != actual_query.label {
            failures.push(format!(
                "query label mismatch: DuckDB={} UltraSQL={}",
                expected_query.label, actual_query.label
            ));
            continue;
        }
        match &actual_query.result {
            Ok(rows) => {
                let actual_rows = runner::QueryRows {
                    label: actual_query.label.clone(),
                    rows: rows.clone(),
                };
                match compare_query_rows(expected_query, &actual_rows) {
                    Ok(()) => println!(
                        "{} ok ({} rows)",
                        actual_query.label,
                        actual_rows.rows.len()
                    ),
                    Err(error) => failures.push(format!("{error:#}")),
                }
            }
            Err(error) => failures.push(format!("{} failed: {error}", actual_query.label)),
        }
    }

    if failures.is_empty() {
        return Ok(());
    }
    for failure in &failures {
        eprintln!("validation failure: {failure}");
    }
    bail!("{} TPC-H validation failure(s)", failures.len())
}

fn compare_query_rows(expected: &runner::QueryRows, actual: &runner::QueryRows) -> Result<()> {
    if expected.rows.len() != actual.rows.len() {
        bail!(
            "{} row count mismatch: DuckDB={} UltraSQL={}\n  DuckDB head: {:?}\n  UltraSQL head: {:?}",
            expected.label,
            expected.rows.len(),
            actual.rows.len(),
            row_context(&expected.rows, 0),
            row_context(&actual.rows, 0)
        );
    }
    for (row_idx, (expected_row, actual_row)) in expected.rows.iter().zip(&actual.rows).enumerate()
    {
        if expected_row.len() != actual_row.len() {
            bail!(
                "{} row {} column count mismatch: DuckDB={} UltraSQL={}",
                expected.label,
                row_idx + 1,
                expected_row.len(),
                actual_row.len()
            );
        }
        for (col_idx, (expected_cell, actual_cell)) in
            expected_row.iter().zip(actual_row).enumerate()
        {
            if !cells_match(expected_cell, actual_cell) {
                let expected_row_in_actual = actual
                    .rows
                    .iter()
                    .position(|row| rows_match(expected_row, row))
                    .map(|idx| idx + 1);
                let actual_row_in_expected = expected
                    .rows
                    .iter()
                    .position(|row| rows_match(row, actual_row))
                    .map(|idx| idx + 1);
                let expected_context = row_context(&expected.rows, row_idx);
                let actual_context = row_context(&actual.rows, row_idx);
                bail!(
                    "{} row {} col {} mismatch: DuckDB=`{}` UltraSQL=`{}`\n  DuckDB row: {:?}\n  UltraSQL row: {:?}\n  DuckDB row appears in UltraSQL at: {:?}\n  UltraSQL row appears in DuckDB at: {:?}\n  DuckDB context: {:?}\n  UltraSQL context: {:?}",
                    expected.label,
                    row_idx + 1,
                    col_idx + 1,
                    expected_cell,
                    actual_cell,
                    expected_row,
                    actual_row,
                    expected_row_in_actual,
                    actual_row_in_expected,
                    expected_context,
                    actual_context
                );
            }
        }
    }
    Ok(())
}

fn row_context(rows: &[Vec<String>], row_idx: usize) -> Vec<(usize, Vec<String>)> {
    let start = row_idx.saturating_sub(2);
    let end = rows.len().min(row_idx + 3);
    rows[start..end]
        .iter()
        .enumerate()
        .map(|(offset, row)| (start + offset + 1, row.clone()))
        .collect()
}

fn rows_match(expected: &[String], actual: &[String]) -> bool {
    expected.len() == actual.len()
        && expected
            .iter()
            .zip(actual)
            .all(|(expected_cell, actual_cell)| cells_match(expected_cell, actual_cell))
}

fn cells_match(expected: &str, actual: &str) -> bool {
    let expected = expected.trim_end();
    let actual = actual.trim_end();
    if expected == actual {
        return true;
    }
    if matches!((expected, actual), ("true", "t") | ("false", "f")) {
        return true;
    }
    let (Some(expected_number), Some(actual_number)) =
        (parse_finite_number(expected), parse_finite_number(actual))
    else {
        return false;
    };
    let diff = (expected_number - actual_number).abs();
    let scale = expected_number.abs().max(actual_number.abs()).max(1.0);
    diff <= 1e-4 || diff <= scale * 1e-9
}

fn parse_finite_number(cell: &str) -> Option<f64> {
    if cell == "\\N" {
        return None;
    }
    cell.parse::<f64>().ok().filter(|value| value.is_finite())
}

fn progress_enabled() -> bool {
    matches!(
        std::env::var("ULTRASQL_TPCH_PROGRESS").ok().as_deref(),
        Some("1") | Some("true") | Some("TRUE") | Some("yes") | Some("YES")
    )
}

/// Ensure all 22 query texts can be fetched; used in integration testing.
#[allow(dead_code)]
fn all_query_texts() -> Vec<(&'static str, &'static str)> {
    (1u8..=22)
        .filter_map(|n| queries::query(n).map(|sql| (sql, sql)))
        .collect()
}
