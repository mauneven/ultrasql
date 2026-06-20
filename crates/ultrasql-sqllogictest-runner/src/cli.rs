//! Command-line interface definition and reference-engine metadata.

use std::path::PathBuf;

use clap::{Parser, ValueEnum};

#[derive(Debug, Parser)]
#[command(author, version, about)]
pub(crate) struct Cli {
    /// Test files or directories to run. Defaults to tests/slt.
    #[arg(value_name = "PATH")]
    pub(crate) paths: Vec<PathBuf>,

    /// Execution mode. Wire connects to an external server; in-process starts one.
    #[arg(long, value_enum, default_value_t = Mode::Wire)]
    pub(crate) mode: Mode,

    /// PostgreSQL wire connection string for UltraSQL.
    #[arg(long, env = "ULTRASQL_SLT_DATABASE_URL")]
    pub(crate) database_url: Option<String>,

    /// Optional reference connection string.
    #[arg(long, env = "ULTRASQL_SLT_REFERENCE_URL")]
    pub(crate) reference_url: Option<String>,

    /// Optional reference engine for differential comparison. Repeat for multiple engines.
    #[arg(long, value_enum)]
    pub(crate) reference_engine: Vec<ReferenceEngine>,

    /// Optional SQLite/DuckDB reference database path. Defaults to a temp file.
    #[arg(long, value_name = "PATH")]
    pub(crate) reference_db: Option<PathBuf>,

    /// Optional JSON output path for SQLLogicTest suite replay timing.
    #[arg(long, value_name = "PATH")]
    pub(crate) benchmark_output: Option<PathBuf>,

    /// Number of times to replay each query record during benchmark mode.
    #[arg(long, default_value_t = 1)]
    pub(crate) benchmark_runs: u32,

    /// Optional total case limit for smoke runs over large imported suites.
    #[arg(long)]
    pub(crate) case_limit: Option<usize>,

    /// Print progress every N executed/filtered cases. Zero disables progress output.
    #[arg(long, default_value_t = 0)]
    pub(crate) progress_every: u64,

    /// Warn when one case takes at least this many milliseconds.
    #[arg(long)]
    pub(crate) slow_case_ms: Option<u128>,

    /// Skip-filter file. Lines are `pattern<TAB>reason`; `#` starts comments.
    #[arg(
        long = "skip-filter",
        value_name = "PATH",
        default_value = "third_party/sqllogictest/filters/unsupported.txt"
    )]
    pub(crate) skip_filters: Vec<PathBuf>,

    /// Enable tests tagged with `# ultrasql:require FEATURE`.
    #[arg(long = "feature", value_name = "FEATURE")]
    pub(crate) features: Vec<String>,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub(crate) enum Mode {
    Wire,
    InProcess,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub(crate) enum ReferenceEngine {
    Postgres,
    Duckdb,
    Sqlite,
}

impl ReferenceEngine {
    pub(crate) fn command(self) -> Option<&'static str> {
        match self {
            Self::Postgres => None,
            Self::Duckdb => Some("duckdb"),
            Self::Sqlite => Some("sqlite3"),
        }
    }

    pub(crate) fn suffix(self) -> &'static str {
        match self {
            Self::Postgres => "postgres",
            Self::Duckdb => "duckdb",
            Self::Sqlite => "sqlite",
        }
    }
}
