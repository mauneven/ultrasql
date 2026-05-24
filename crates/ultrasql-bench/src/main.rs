//! UltraSQL benchmark harness command-line entrypoint.

#![allow(clippy::print_stderr)]
#![allow(clippy::print_stdout)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_precision_loss)]
#![allow(clippy::cast_sign_loss)]

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Args, Parser, Subcommand, ValueEnum};
use serde::Serialize;
use ultrasql_bench::ai_gauntlet::{
    FilteredVectorConfig, VectorMemoryConfig, run_filtered_vector_search, run_vector_memory,
};
use ultrasql_bench::ann_vector::{AnnBenchmarkConfig, run_hnsw_ann_benchmark};
use ultrasql_bench::registry::HostInfo;

#[cfg(feature = "sql-bench")]
mod tpcb_wire;

/// UltraSQL benchmark harness.
#[derive(Debug, Parser)]
#[command(name = "ultrasql-bench")]
#[command(about = "UltraSQL macro-benchmark harness")]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

/// Supported benchmark commands.
#[derive(Debug, Subcommand)]
enum Command {
    /// Run deterministic runtime-HNSW ANN vector benchmark.
    AnnVector(AnnVectorArgs),
    /// Run filtered vector search exact-vs-ANN benchmark.
    FilteredVector(FilteredVectorArgs),
    /// Run page-backed HNSW/IVFFlat memory accounting benchmark.
    VectorMemory(VectorMemoryArgs),
    /// Run a PostgreSQL-wire TPC-B-shaped benchmark.
    Tpcb(TpcbArgs),
    /// Run a local five-transaction TPC-C-shaped kernel benchmark.
    Tpcc(TpccArgs),
}

/// ANN vector benchmark arguments.
#[derive(Clone, Debug, Args)]
struct AnnVectorArgs {
    /// Number of vectors to index.
    #[arg(long, default_value_t = 10_000)]
    rows: usize,
    /// Vector dimensions.
    #[arg(long, default_value_t = 8)]
    dims: usize,
    /// Nearest neighbors requested per query.
    #[arg(long = "top-k", default_value_t = 10)]
    top_k: usize,
    /// Measured queries.
    #[arg(long, default_value_t = 50)]
    queries: usize,
    /// Warmup queries excluded from latency percentiles.
    #[arg(long = "warmup", default_value_t = 5)]
    warmup_queries: usize,
    /// HNSW neighbor cap.
    #[arg(long, default_value_t = 16)]
    m: usize,
    /// HNSW search breadth.
    #[arg(long = "ef-search", default_value_t = 64)]
    ef_search: usize,
    /// Deterministic data/probe seed.
    #[arg(long, default_value_t = 0x51_7e_c0_de)]
    seed: u64,
    /// Output JSON path. Writes to stdout when omitted.
    #[arg(long)]
    output: Option<PathBuf>,
}

/// Filtered vector search benchmark arguments.
#[derive(Clone, Debug, Args)]
struct FilteredVectorArgs {
    /// Stable workload id written into the artifact.
    #[arg(long = "workload-id")]
    workload_id: Option<String>,
    /// AI gauntlet profile label.
    #[arg(long, default_value = "smoke")]
    profile: String,
    /// Number of vectors to index.
    #[arg(long, default_value_t = 10_000)]
    rows: usize,
    /// Vector dimensions.
    #[arg(long, default_value_t = 8)]
    dims: usize,
    /// Nearest neighbors requested after filtering.
    #[arg(long = "top-k", default_value_t = 10)]
    top_k: usize,
    /// Measured queries.
    #[arg(long, default_value_t = 50)]
    queries: usize,
    /// Warmup queries excluded from latency percentiles.
    #[arg(long = "warmup", default_value_t = 5)]
    warmup_queries: usize,
    /// Tenant cardinality in deterministic metadata.
    #[arg(long = "tenant-count", default_value_t = 8)]
    tenant_count: usize,
    /// Category cardinality in deterministic metadata.
    #[arg(long = "category-count", default_value_t = 4)]
    category_count: usize,
    /// Tenant selected by the filter.
    #[arg(long = "tenant-id", default_value_t = 3)]
    tenant_id: usize,
    /// Category selected by the filter.
    #[arg(long = "category-id", default_value_t = 2)]
    category_id: usize,
    /// HNSW neighbor cap.
    #[arg(long, default_value_t = 16)]
    m: usize,
    /// HNSW search breadth.
    #[arg(long = "ef-search", default_value_t = 1_024)]
    ef_search: usize,
    /// Deterministic data/probe seed.
    #[arg(long, default_value_t = 0x51_7e_c0_de)]
    seed: u64,
    /// Output JSON path. Writes to stdout when omitted.
    #[arg(long)]
    output: Option<PathBuf>,
}

/// Vector memory benchmark arguments.
#[derive(Clone, Debug, Args)]
struct VectorMemoryArgs {
    /// Stable workload id written into the artifact.
    #[arg(long = "workload-id")]
    workload_id: Option<String>,
    /// AI gauntlet profile label.
    #[arg(long, default_value = "smoke")]
    profile: String,
    /// Number of vectors to build.
    #[arg(long, default_value_t = 10_000)]
    rows: usize,
    /// Vector dimensions.
    #[arg(long, default_value_t = 8)]
    dims: usize,
    /// HNSW neighbor cap.
    #[arg(long, default_value_t = 16)]
    m: usize,
    /// HNSW search breadth.
    #[arg(long = "ef-search", default_value_t = 64)]
    ef_search: usize,
    /// IVFFlat list count.
    #[arg(long, default_value_t = 64)]
    lists: usize,
    /// IVFFlat probe count.
    #[arg(long, default_value_t = 8)]
    probes: usize,
    /// Deterministic data seed.
    #[arg(long, default_value_t = 0x51_7e_c0_de)]
    seed: u64,
    /// Output JSON path. Writes to stdout when omitted.
    #[arg(long)]
    output: Option<PathBuf>,
}

/// TPC-B benchmark arguments.
#[derive(Clone, Debug, Args)]
struct TpcbArgs {
    /// Engine label and launch mode.
    #[arg(long, value_enum, default_value_t = TpcbEngine::Ultrasql)]
    engine: TpcbEngine,
    /// PostgreSQL connection string. Required for postgres17; optional
    /// for ultrasql, which starts an in-process server when omitted.
    #[arg(long)]
    dsn: Option<String>,
    /// pgbench scale factor. Scale 1 uses 100k accounts by default.
    #[arg(long, default_value_t = 1)]
    scale: usize,
    /// Override account count for fast smoke runs.
    #[arg(long)]
    accounts: Option<usize>,
    /// Warmup window in seconds.
    #[arg(long = "warmup", default_value_t = 30)]
    warmup_secs: u64,
    /// Measured window in seconds.
    #[arg(long = "duration", default_value_t = 60)]
    duration_secs: u64,
    /// Number of concurrent clients.
    #[arg(long, default_value_t = 32)]
    connections: usize,
    /// Output JSON path. Writes to stdout when omitted.
    #[arg(long)]
    output: Option<PathBuf>,
}

/// TPC-C local kernel benchmark arguments.
#[derive(Clone, Debug, Args)]
struct TpccArgs {
    /// Artifact profile label.
    #[arg(long, default_value = "local-kernel")]
    profile: String,
    /// Number of measured iterations.
    #[arg(long, default_value_t = 5)]
    iterations: u32,
    /// Number of warmup iterations excluded from samples.
    #[arg(long, default_value_t = 1)]
    warmup: u32,
    /// Output JSON path. Writes to stdout when omitted.
    #[arg(long)]
    output: Option<PathBuf>,
}

/// Engine selector for TPC-B.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum TpcbEngine {
    /// Spawn and benchmark an in-process UltraSQL server unless `--dsn`
    /// is supplied.
    Ultrasql,
    /// Benchmark an existing PostgreSQL 17-compatible server.
    Postgres17,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match cli.command {
        Some(Command::AnnVector(args)) => run_ann_vector(args),
        Some(Command::FilteredVector(args)) => run_filtered_vector(args),
        Some(Command::VectorMemory(args)) => run_vector_memory_command(args),
        Some(Command::Tpcb(args)) => run_tpcb(args),
        Some(Command::Tpcc(args)) => run_tpcc(args),
        None => {
            eprintln!(
                "ultrasql-bench {}: use --help or a subcommand",
                env!("CARGO_PKG_VERSION")
            );
            ExitCode::SUCCESS
        }
    }
}

fn run_ann_vector(args: AnnVectorArgs) -> ExitCode {
    let config = AnnBenchmarkConfig {
        rows: args.rows,
        dims: args.dims,
        top_k: args.top_k,
        queries: args.queries,
        warmup_queries: args.warmup_queries,
        m: args.m,
        ef_search: args.ef_search,
        seed: args.seed,
    };
    match run_hnsw_ann_benchmark(&config, HostInfo::from_env()).and_then(|artifact| {
        let serialized = serde_json::to_string_pretty(&artifact)?;
        if let Some(path) = args.output.as_ref() {
            std::fs::write(path, serialized)?;
            eprintln!("ann-vector benchmark: wrote {}", path.display());
        } else {
            println!("{serialized}");
        }
        Ok(())
    }) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("ann-vector benchmark failed: {err:#}");
            ExitCode::from(1)
        }
    }
}

fn run_filtered_vector(args: FilteredVectorArgs) -> ExitCode {
    let workload = args
        .workload_id
        .clone()
        .unwrap_or_else(|| format!("ai_gauntlet_filtered_vector_search_{}", args.profile));
    let config = FilteredVectorConfig {
        workload,
        profile: args.profile,
        rows: args.rows,
        dims: args.dims,
        top_k: args.top_k,
        queries: args.queries,
        warmup_queries: args.warmup_queries,
        tenant_count: args.tenant_count,
        category_count: args.category_count,
        tenant_id: args.tenant_id,
        category_id: args.category_id,
        m: args.m,
        ef_search: args.ef_search,
        seed: args.seed,
    };
    match run_filtered_vector_search(&config, HostInfo::from_env()).and_then(|artifact| {
        write_artifact("filtered-vector benchmark", args.output.as_ref(), &artifact)
    }) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("filtered-vector benchmark failed: {err:#}");
            ExitCode::from(1)
        }
    }
}

fn run_vector_memory_command(args: VectorMemoryArgs) -> ExitCode {
    let workload = args
        .workload_id
        .clone()
        .unwrap_or_else(|| format!("ai_gauntlet_memory_per_million_vectors_{}", args.profile));
    let config = VectorMemoryConfig {
        workload,
        profile: args.profile,
        rows: args.rows,
        dims: args.dims,
        m: args.m,
        ef_search: args.ef_search,
        lists: args.lists,
        probes: args.probes,
        seed: args.seed,
    };
    match run_vector_memory(&config, HostInfo::from_env()).and_then(|artifact| {
        write_artifact("vector-memory benchmark", args.output.as_ref(), &artifact)
    }) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("vector-memory benchmark failed: {err:#}");
            ExitCode::from(1)
        }
    }
}

#[derive(Serialize)]
struct TpccArtifact {
    schema_version: u32,
    workload: &'static str,
    engine: &'static str,
    profile: String,
    transaction_types: [&'static str; 5],
    iterations: u32,
    warmup: u32,
    host: HostInfo,
    throughput_per_sec: f64,
    p50_latency_us: f64,
    p99_latency_us: f64,
    samples: Vec<f64>,
    certification_scope: &'static str,
}

fn run_tpcc(args: TpccArgs) -> ExitCode {
    let host = HostInfo::from_env();
    let ctx = ultrasql_bench::registry::BenchContext {
        iterations: args.iterations,
        warmup_iterations: args.warmup,
        host: host.clone(),
    };
    let result = ultrasql_bench::runs::tpcc::run(&ctx);
    let artifact = TpccArtifact {
        schema_version: 1,
        workload: "tpcc_5types",
        engine: "ultrasql",
        profile: args.profile,
        transaction_types: [
            "NewOrder",
            "Payment",
            "OrderStatus",
            "Delivery",
            "StockLevel",
        ],
        iterations: args.iterations,
        warmup: args.warmup,
        host,
        throughput_per_sec: result.throughput_per_sec,
        p50_latency_us: result.p50_latency_us,
        p99_latency_us: result.p99_latency_us,
        samples: result.samples,
        certification_scope: "local_kernel_not_wire_certification",
    };
    match write_artifact("tpcc benchmark", args.output.as_ref(), &artifact) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("tpcc benchmark failed: {err:#}");
            ExitCode::from(1)
        }
    }
}

fn write_artifact<T: Serialize>(
    label: &str,
    output: Option<&PathBuf>,
    artifact: &T,
) -> anyhow::Result<()> {
    let serialized = serde_json::to_string_pretty(artifact)?;
    if let Some(path) = output {
        std::fs::write(path, serialized)?;
        eprintln!("{label}: wrote {}", path.display());
    } else {
        println!("{serialized}");
    }
    Ok(())
}

#[cfg(feature = "sql-bench")]
fn run_tpcb(args: TpcbArgs) -> ExitCode {
    match tpcb_wire::run_blocking(args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("tpcb benchmark failed: {err:#}");
            ExitCode::from(1)
        }
    }
}

#[cfg(not(feature = "sql-bench"))]
fn run_tpcb(_args: TpcbArgs) -> ExitCode {
    eprintln!(
        "tpcb requires: cargo run -p ultrasql-bench --features sql-bench --bin ultrasql-bench -- tpcb ..."
    );
    ExitCode::from(2)
}

#[cfg(any(test, feature = "sql-bench"))]
fn percentile(sorted_values: &[f64], quantile: f64) -> f64 {
    if sorted_values.is_empty() {
        return 0.0;
    }
    let rank = (quantile.clamp(0.0, 1.0) * sorted_values.len() as f64).ceil();
    let index = (rank.max(1.0) as usize).saturating_sub(1);
    sorted_values[index.min(sorted_values.len() - 1)]
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn tpcb_cli_defaults_to_v09_cert_shape() {
        let cli = Cli::try_parse_from(["ultrasql-bench", "tpcb"]).expect("parse tpcb args");
        match cli.command {
            Some(Command::Tpcb(args)) => {
                assert_eq!(args.engine, TpcbEngine::Ultrasql);
                assert_eq!(args.scale, 1);
                assert_eq!(args.connections, 32);
                assert_eq!(args.duration_secs, 60);
                assert_eq!(args.warmup_secs, 30);
                assert!(args.output.is_none());
            }
            _ => panic!("tpcb subcommand should parse"),
        }
    }

    #[test]
    fn tpcc_cli_defaults_to_local_kernel_shape() {
        let cli = Cli::try_parse_from(["ultrasql-bench", "tpcc"]).expect("parse tpcc args");
        match cli.command {
            Some(Command::Tpcc(args)) => {
                assert_eq!(args.profile, "local-kernel");
                assert_eq!(args.iterations, 5);
                assert_eq!(args.warmup, 1);
                assert!(args.output.is_none());
            }
            _ => panic!("tpcc subcommand should parse"),
        }
    }

    #[test]
    fn ann_vector_cli_defaults_to_v1_artifact_shape() {
        let cli =
            Cli::try_parse_from(["ultrasql-bench", "ann-vector"]).expect("parse ann-vector args");
        match cli.command {
            Some(Command::AnnVector(args)) => {
                assert_eq!(args.rows, 10_000);
                assert_eq!(args.dims, 8);
                assert_eq!(args.top_k, 10);
                assert_eq!(args.queries, 50);
                assert_eq!(args.warmup_queries, 5);
                assert_eq!(args.m, 16);
                assert_eq!(args.ef_search, 64);
                assert!(args.output.is_none());
            }
            _ => panic!("ann-vector subcommand should parse"),
        }
    }

    #[test]
    fn filtered_vector_cli_defaults_to_phase3_artifact_shape() {
        let cli = Cli::try_parse_from(["ultrasql-bench", "filtered-vector"])
            .expect("parse filtered-vector args");
        match cli.command {
            Some(Command::FilteredVector(args)) => {
                assert_eq!(args.rows, 10_000);
                assert_eq!(args.dims, 8);
                assert_eq!(args.top_k, 10);
                assert_eq!(args.queries, 50);
                assert_eq!(args.warmup_queries, 5);
                assert_eq!(args.tenant_count, 8);
                assert_eq!(args.category_count, 4);
                assert!(args.output.is_none());
            }
            _ => panic!("filtered-vector subcommand should parse"),
        }
    }

    #[test]
    fn vector_memory_cli_defaults_to_phase3_artifact_shape() {
        let cli = Cli::try_parse_from(["ultrasql-bench", "vector-memory"])
            .expect("parse vector-memory args");
        match cli.command {
            Some(Command::VectorMemory(args)) => {
                assert_eq!(args.rows, 10_000);
                assert_eq!(args.dims, 8);
                assert_eq!(args.m, 16);
                assert_eq!(args.lists, 64);
                assert_eq!(args.probes, 8);
                assert!(args.output.is_none());
            }
            _ => panic!("vector-memory subcommand should parse"),
        }
    }

    #[test]
    fn percentile_uses_ceil_rank_for_p99() {
        let values = [10.0, 20.0, 30.0, 40.0, 50.0];
        assert_eq!(percentile(&values, 0.99), 50.0);
        assert_eq!(percentile(&values, 0.50), 30.0);
    }
}
