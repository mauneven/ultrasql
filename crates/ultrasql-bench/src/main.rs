//! UltraSQL benchmark harness command-line entrypoint.

#![allow(clippy::print_stderr)]
#![allow(clippy::print_stdout)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_precision_loss)]
#![allow(clippy::cast_sign_loss)]

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Args, Parser, Subcommand, ValueEnum};

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
    /// Run a PostgreSQL-wire TPC-B-shaped benchmark.
    Tpcb(TpcbArgs),
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
        Some(Command::Tpcb(args)) => run_tpcb(args),
        None => {
            eprintln!(
                "ultrasql-bench {}: use --help or a subcommand",
                env!("CARGO_PKG_VERSION")
            );
            ExitCode::SUCCESS
        }
    }
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
            None => panic!("tpcb subcommand should parse"),
        }
    }

    #[test]
    fn percentile_uses_ceil_rank_for_p99() {
        let values = [10.0, 20.0, 30.0, 40.0, 50.0];
        assert_eq!(percentile(&values, 0.99), 50.0);
        assert_eq!(percentile(&values, 0.50), 30.0);
    }
}
