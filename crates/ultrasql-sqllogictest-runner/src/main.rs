//! SQLLogicTest runner for UltraSQL.
//!
//! The first implementation is deliberately wire-first: it connects through
//! `tokio-postgres` so every test exercises the same PostgreSQL protocol path
//! used by clients. In-process execution can be added later behind the same
//! parsed test model.

use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::io::Write as _;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use clap::{Parser, ValueEnum};
use tokio_postgres::types::Type;
use tokio_postgres::{Client, NoTls, Row};

#[derive(Debug, Parser)]
#[command(author, version, about)]
struct Cli {
    /// Test files or directories to run. Defaults to tests/slt.
    #[arg(value_name = "PATH")]
    paths: Vec<PathBuf>,

    /// Execution mode. Wire connects to an external server; in-process starts one.
    #[arg(long, value_enum, default_value_t = Mode::Wire)]
    mode: Mode,

    /// PostgreSQL wire connection string for UltraSQL.
    #[arg(long, env = "ULTRASQL_SLT_DATABASE_URL")]
    database_url: Option<String>,

    /// Optional reference connection string.
    #[arg(long, env = "ULTRASQL_SLT_REFERENCE_URL")]
    reference_url: Option<String>,

    /// Optional reference engine for differential comparison. Repeat for multiple engines.
    #[arg(long, value_enum)]
    reference_engine: Vec<ReferenceEngine>,

    /// Optional SQLite/DuckDB reference database path. Defaults to a temp file.
    #[arg(long, value_name = "PATH")]
    reference_db: Option<PathBuf>,

    /// Optional JSON output path for SQLLogicTest suite replay timing.
    #[arg(long, value_name = "PATH")]
    benchmark_output: Option<PathBuf>,

    /// Number of times to replay each query record during benchmark mode.
    #[arg(long, default_value_t = 1)]
    benchmark_runs: u32,

    /// Optional total case limit for smoke runs over large imported suites.
    #[arg(long)]
    case_limit: Option<usize>,

    /// Print progress every N executed/filtered cases. Zero disables progress output.
    #[arg(long, default_value_t = 0)]
    progress_every: u64,

    /// Warn when one case takes at least this many milliseconds.
    #[arg(long)]
    slow_case_ms: Option<u128>,

    /// Skip-filter file. Lines are `pattern<TAB>reason`; `#` starts comments.
    #[arg(
        long = "skip-filter",
        value_name = "PATH",
        default_value = "third_party/sqllogictest/filters/unsupported.txt"
    )]
    skip_filters: Vec<PathBuf>,

    /// Enable tests tagged with `# ultrasql:require FEATURE`.
    #[arg(long = "feature", value_name = "FEATURE")]
    features: Vec<String>,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Mode {
    Wire,
    InProcess,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum ReferenceEngine {
    Postgres,
    Duckdb,
    Sqlite,
}

impl ReferenceEngine {
    fn command(self) -> Option<&'static str> {
        match self {
            Self::Postgres => None,
            Self::Duckdb => Some("duckdb"),
            Self::Sqlite => Some("sqlite3"),
        }
    }

    fn suffix(self) -> &'static str {
        match self {
            Self::Postgres => "postgres",
            Self::Duckdb => "duckdb",
            Self::Sqlite => "sqlite",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StatementExpectation {
    Ok,
    Error,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SortMode {
    NoSort,
    RowSort,
}

#[derive(Clone, Debug)]
enum TestKind {
    Statement {
        expectation: StatementExpectation,
        sql: String,
    },
    Query {
        type_string: String,
        sort_mode: SortMode,
        sql: String,
        expected: QueryExpectation,
    },
}

#[derive(Clone, Debug)]
enum QueryExpectation {
    Values(Vec<String>),
    Hash { value_count: usize, digest: String },
}

#[derive(Clone, Debug)]
struct TestCase {
    path: PathBuf,
    line: usize,
    kind: TestKind,
    skip_reason: Option<String>,
    requires: Vec<String>,
}

impl TestCase {
    fn sql(&self) -> &str {
        match &self.kind {
            TestKind::Statement { sql, .. } | TestKind::Query { sql, .. } => sql,
        }
    }
}

#[derive(Clone, Debug)]
struct SkipPattern {
    pattern: String,
    reason: String,
}

#[derive(Clone, Debug, Default)]
struct SkipFilters {
    patterns: Vec<SkipPattern>,
}

impl SkipFilters {
    fn load_all(paths: &[PathBuf]) -> Result<Self> {
        let mut filters = Self::default();
        for path in paths {
            if !path.exists() {
                continue;
            }
            let text = std::fs::read_to_string(path)
                .with_context(|| format!("read skip filter {}", path.display()))?;
            for (idx, raw_line) in text.lines().enumerate() {
                let trimmed = raw_line.trim();
                if trimmed.is_empty() || trimmed.starts_with('#') {
                    continue;
                }
                let Some((pattern, reason)) = raw_line.split_once('\t') else {
                    bail!(
                        "{}:{} skip filter requires `pattern<TAB>reason`",
                        path.display(),
                        idx + 1
                    );
                };
                let pattern = pattern.trim();
                if pattern.is_empty() {
                    bail!("{}:{} empty skip pattern", path.display(), idx + 1);
                }
                let reason = reason.trim();
                if reason.is_empty() {
                    bail!(
                        "{}:{} skip filter requires an explicit reason",
                        path.display(),
                        idx + 1
                    );
                }
                filters.patterns.push(SkipPattern {
                    pattern: pattern.to_owned(),
                    reason: reason.to_owned(),
                });
            }
        }
        Ok(filters)
    }

    fn skip_reason(&self, path: &Path, sql: &str) -> Option<String> {
        let path = path.to_string_lossy();
        self.patterns.iter().find_map(|filter| {
            if sql.contains(&filter.pattern) || path.contains(&filter.pattern) {
                Some(format!("{} ({})", filter.reason, filter.pattern))
            } else {
                None
            }
        })
    }
}

#[derive(Debug, Default)]
struct Directives {
    file_skip: Option<String>,
    file_requires: Vec<String>,
    next_skip: Option<String>,
    next_requires: Vec<String>,
}

impl Directives {
    fn take_for_case(&mut self) -> (Option<String>, Vec<String>) {
        let skip = self.file_skip.clone().or_else(|| self.next_skip.take());
        let mut requires = self.file_requires.clone();
        requires.append(&mut self.next_requires);
        (skip, requires)
    }
}

#[derive(Debug, Default)]
struct Summary {
    files: u64,
    cases: u64,
    passed: u64,
    failed: u64,
    skipped: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let filters = SkipFilters::load_all(&cli.skip_filters)?;
    let files = collect_input_files(&cli.paths)?;
    let enabled_features: BTreeSet<String> = cli.features.iter().cloned().collect();
    let mut cases_by_file = Vec::new();
    for file in &files {
        let text =
            std::fs::read_to_string(file).with_context(|| format!("read {}", file.display()))?;
        let cases = parse_script(file, &text)?;
        cases_by_file.push((file.clone(), cases));
    }
    if let Some(case_limit) = cli.case_limit {
        apply_case_limit(&mut cases_by_file, case_limit);
    }

    let mut summary = Summary::default();
    for (_file, cases) in &cases_by_file {
        let (client, _in_process_server) = connect_ultrasql_target(&cli).await?;
        let references = connect_reference_targets(&cli).await?;
        summary.files = summary.files.saturating_add(1);
        for case in cases {
            summary.cases = summary.cases.saturating_add(1);
            let case_start = Instant::now();
            match run_case(&client, &references, &filters, &enabled_features, case).await {
                CaseOutcome::Passed => summary.passed = summary.passed.saturating_add(1),
                CaseOutcome::Skipped(reason) => {
                    summary.skipped = summary.skipped.saturating_add(1);
                    println!("skip {}:{} {reason}", case.path.display(), case.line);
                }
                CaseOutcome::Failed(message) => {
                    summary.failed = summary.failed.saturating_add(1);
                    eprintln!("fail {}:{}\n{message}", case.path.display(), case.line);
                }
            }
            let elapsed_ms = case_start.elapsed().as_millis();
            if cli
                .slow_case_ms
                .is_some_and(|threshold_ms| elapsed_ms >= threshold_ms)
            {
                eprintln!(
                    "slow-case {}:{} elapsed_ms={} sql={}",
                    case.path.display(),
                    case.line,
                    elapsed_ms,
                    compact_sql(case.sql())
                );
            }
            if cli.progress_every > 0 && summary.cases % cli.progress_every == 0 {
                eprintln!(
                    "slt progress: cases={} passed={} skipped={} failed={}",
                    summary.cases, summary.passed, summary.skipped, summary.failed
                );
            }
        }
    }

    println!(
        "slt summary: files={} cases={} passed={} skipped={} failed={}",
        summary.files, summary.cases, summary.passed, summary.skipped, summary.failed
    );
    if summary.failed > 0 {
        bail!(
            "SQLLogicTest suite failed with {} failure(s)",
            summary.failed
        );
    }

    if let Some(output_path) = &cli.benchmark_output {
        let cases: Vec<TestCase> = cases_by_file
            .iter()
            .flat_map(|(_, cases)| cases.iter().cloned())
            .collect();
        let benchmarks = run_benchmark_suite(
            &cli,
            &filters,
            &enabled_features,
            &cases,
            cli.benchmark_runs,
        )
        .await?;
        write_benchmark_artifacts(
            output_path,
            &cli.paths,
            &cases,
            cli.benchmark_runs,
            &benchmarks,
        )
        .with_context(|| format!("write benchmark artifact {}", output_path.display()))?;
        println!("slt benchmark artifact: {}", output_path.display());
    }
    Ok(())
}

fn compact_sql(sql: &str) -> String {
    let mut compact = sql.split_whitespace().collect::<Vec<_>>().join(" ");
    const MAX_LEN: usize = 160;
    if compact.len() > MAX_LEN {
        compact.truncate(MAX_LEN);
        compact.push_str("...");
    }
    compact
}

fn apply_case_limit(cases_by_file: &mut Vec<(PathBuf, Vec<TestCase>)>, limit: usize) {
    let mut remaining = limit;
    for (_, cases) in cases_by_file.iter_mut() {
        if cases.len() > remaining {
            cases.truncate(remaining);
            remaining = 0;
        } else {
            remaining = remaining.saturating_sub(cases.len());
        }
    }
    cases_by_file.retain(|(_, cases)| !cases.is_empty());
}

#[derive(Debug)]
struct InProcessServer {
    handle: tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
}

impl Drop for InProcessServer {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

async fn connect_ultrasql_target(cli: &Cli) -> Result<(Client, Option<InProcessServer>)> {
    match cli.mode {
        Mode::Wire => {
            let database_url = cli.database_url.as_deref().ok_or_else(|| {
                anyhow::anyhow!(
                    "missing --database-url or ULTRASQL_SLT_DATABASE_URL for wire-mode execution"
                )
            })?;
            let client = connect_database(database_url, "UltraSQL wire endpoint").await?;
            Ok((client, None))
        }
        Mode::InProcess => {
            let addr = SocketAddr::from(([127, 0, 0, 1], 0));
            let (listener, bound) = ultrasql_server::bind_listener(addr)
                .await
                .context("bind in-process UltraSQL listener")?;
            let server = Arc::new(ultrasql_server::Server::with_sample_database());
            let handle = tokio::spawn(ultrasql_server::serve_listener(listener, server));
            let conn_str = format!(
                "host={host} port={port} user=ultrasql_slt application_name=ultrasql_slt",
                host = bound.ip(),
                port = bound.port()
            );
            let client = connect_database(&conn_str, "in-process UltraSQL wire endpoint").await?;
            Ok((client, Some(InProcessServer { handle })))
        }
    }
}

async fn connect_database(conn_str: &str, label: &str) -> Result<Client> {
    let (client, connection) = tokio_postgres::connect(conn_str, NoTls)
        .await
        .with_context(|| format!("connect {label}"))?;
    let label = label.to_owned();
    tokio::spawn(async move {
        if let Err(err) = connection.await {
            eprintln!("ultrasql-slt: {label} connection error: {err}");
        }
    });
    Ok(client)
}

#[derive(Debug)]
enum ReferenceTarget {
    Postgres(Client),
    Cli(CliReference),
}

impl ReferenceTarget {
    async fn execute_statement(&self, sql: &str) -> Result<()> {
        match self {
            Self::Postgres(client) => client
                .batch_execute(sql)
                .await
                .map_err(|err| anyhow::anyhow!("{}", format_pg_error(&err))),
            Self::Cli(reference) => reference.execute_statement(sql),
        }
    }

    async fn execute_query(
        &self,
        type_string: &str,
        sort_mode: SortMode,
        sql: &str,
    ) -> Result<Vec<String>> {
        match self {
            Self::Postgres(client) => execute_query(client, type_string, sort_mode, sql).await,
            Self::Cli(reference) => reference.execute_query(type_string, sort_mode, sql),
        }
    }
}

#[derive(Debug)]
struct CliReference {
    engine: ReferenceEngine,
    db_path: PathBuf,
    remove_on_drop: bool,
}

impl CliReference {
    fn new(engine: ReferenceEngine, db_path: PathBuf, remove_on_drop: bool) -> Self {
        Self {
            engine,
            db_path,
            remove_on_drop,
        }
    }

    fn execute_statement(&self, sql: &str) -> Result<()> {
        self.run_sql(sql).map(|_| ())
    }

    fn execute_query(
        &self,
        type_string: &str,
        sort_mode: SortMode,
        sql: &str,
    ) -> Result<Vec<String>> {
        let stdout = self.run_sql(sql)?;
        format_cli_reference_rows(&stdout, type_string, sort_mode)
    }

    fn run_sql(&self, sql: &str) -> Result<String> {
        let command = self.engine.command().with_context(|| {
            format!(
                "reference engine {} does not expose a CLI command",
                self.engine.suffix()
            )
        })?;
        let output = Command::new(command)
            .arg("-batch")
            .arg("-bail")
            .arg("-noheader")
            .arg("-list")
            .arg("-nullvalue")
            .arg("NULL")
            .arg("-separator")
            .arg("\n")
            .arg(&self.db_path)
            .arg(sql)
            .output()
            .with_context(|| format!("run {command} reference engine"))?;
        if !output.status.success() {
            bail!(
                "{} reference failed with status {}\nstdout:\n{}\nstderr:\n{}",
                command,
                output.status,
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        String::from_utf8(output.stdout).context("reference output is not UTF-8")
    }
}

impl Drop for CliReference {
    fn drop(&mut self) {
        if self.remove_on_drop {
            let _ = std::fs::remove_file(&self.db_path);
        }
    }
}

async fn connect_reference_targets(cli: &Cli) -> Result<Vec<ReferenceTarget>> {
    let engines = selected_reference_engines(cli)?;
    if engines.is_empty() {
        if cli.reference_db.is_some() {
            bail!("--reference-db requires --reference-engine duckdb or sqlite");
        }
        return Ok(Vec::new());
    }

    let mut references = Vec::with_capacity(engines.len());
    for engine in engines {
        match engine {
            ReferenceEngine::Postgres => {
                if cli.reference_db.is_some() {
                    bail!("--reference-db is only valid with duckdb or sqlite comparison engines");
                }
                let reference_url = cli.reference_url.as_deref().ok_or_else(|| {
                    anyhow::anyhow!("--reference-engine postgres requires --reference-url")
                })?;
                references.push(ReferenceTarget::Postgres(
                    connect_database(reference_url, "PostgreSQL reference endpoint").await?,
                ));
            }
            ReferenceEngine::Duckdb | ReferenceEngine::Sqlite => {
                let (db_path, remove_on_drop) = match &cli.reference_db {
                    Some(path) => (path.clone(), false),
                    None => (temp_reference_db_path(engine)?, true),
                };
                references.push(ReferenceTarget::Cli(CliReference::new(
                    engine,
                    db_path,
                    remove_on_drop,
                )));
            }
        }
    }
    Ok(references)
}

fn selected_reference_engines(cli: &Cli) -> Result<Vec<ReferenceEngine>> {
    let mut engines = cli.reference_engine.clone();
    if cli.reference_url.is_some() && !engines.contains(&ReferenceEngine::Postgres) {
        engines.push(ReferenceEngine::Postgres);
    }
    if cli.reference_url.is_some()
        && engines
            .iter()
            .any(|engine| matches!(engine, ReferenceEngine::Duckdb | ReferenceEngine::Sqlite))
    {
        bail!("--reference-url is only valid with postgres reference engine");
    }
    if cli.reference_db.is_some() {
        let cli_engine_count = engines
            .iter()
            .filter(|engine| matches!(engine, ReferenceEngine::Duckdb | ReferenceEngine::Sqlite))
            .count();
        if cli_engine_count != 1 || engines.len() != 1 {
            bail!("--reference-db requires exactly one duckdb or sqlite reference engine");
        }
    }
    let mut deduped = Vec::with_capacity(engines.len());
    for engine in engines {
        if !deduped.contains(&engine) {
            deduped.push(engine);
        }
    }
    Ok(deduped)
}

fn temp_reference_db_path(engine: ReferenceEngine) -> Result<PathBuf> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock before Unix epoch")?
        .as_nanos();
    Ok(std::env::temp_dir().join(format!(
        "ultrasql-slt-{}-{nanos}.{}",
        std::process::id(),
        engine.suffix()
    )))
}

#[derive(Debug)]
struct EngineBenchmark {
    engine: String,
    ok: bool,
    error: Option<String>,
    statements: u64,
    query_records: u64,
    query_iterations: u64,
    skipped: u64,
    total_ns: u128,
}

impl EngineBenchmark {
    fn failed(engine: impl Into<String>, error: anyhow::Error) -> Self {
        Self {
            engine: engine.into(),
            ok: false,
            error: Some(error.to_string()),
            statements: 0,
            query_records: 0,
            query_iterations: 0,
            skipped: 0,
            total_ns: 0,
        }
    }
}

async fn run_benchmark_suite(
    cli: &Cli,
    filters: &SkipFilters,
    enabled_features: &BTreeSet<String>,
    cases: &[TestCase],
    runs: u32,
) -> Result<Vec<EngineBenchmark>> {
    if runs == 0 {
        bail!("--benchmark-runs must be greater than zero");
    }

    let mut benchmarks = Vec::new();
    let (client, _in_process_server) = connect_ultrasql_target(cli).await?;
    benchmarks.push(
        benchmark_wire_engine("ultrasql", &client, filters, enabled_features, cases, runs).await?,
    );

    for engine in selected_reference_engines(cli)? {
        match engine {
            ReferenceEngine::Postgres => {
                let reference_url = cli.reference_url.as_deref().ok_or_else(|| {
                    anyhow::anyhow!("--reference-engine postgres requires --reference-url")
                })?;
                match connect_database(reference_url, "PostgreSQL benchmark endpoint").await {
                    Ok(client) => {
                        benchmarks.push(
                            benchmark_wire_engine(
                                "postgres",
                                &client,
                                filters,
                                enabled_features,
                                cases,
                                runs,
                            )
                            .await?,
                        );
                    }
                    Err(err) => benchmarks.push(EngineBenchmark::failed("postgres", err)),
                }
            }
            ReferenceEngine::Duckdb | ReferenceEngine::Sqlite => {
                let result = benchmark_cli_engine(engine, filters, enabled_features, cases, runs);
                benchmarks.push(match result {
                    Ok(benchmark) => benchmark,
                    Err(err) => EngineBenchmark::failed(engine.suffix(), err),
                });
            }
        }
    }

    Ok(benchmarks)
}

async fn benchmark_wire_engine(
    engine: &str,
    client: &Client,
    filters: &SkipFilters,
    enabled_features: &BTreeSet<String>,
    cases: &[TestCase],
    runs: u32,
) -> Result<EngineBenchmark> {
    let start = Instant::now();
    let mut statements = 0_u64;
    let mut query_records = 0_u64;
    let mut query_iterations = 0_u64;
    let mut skipped = 0_u64;
    for case in cases {
        if effective_skip_reason(filters, enabled_features, case).is_some() {
            skipped = skipped.saturating_add(1);
            continue;
        }
        match &case.kind {
            TestKind::Statement {
                expectation: StatementExpectation::Ok,
                sql,
            } => {
                client
                    .batch_execute(sql)
                    .await
                    .map_err(|err| anyhow::anyhow!("{}", format_pg_error(&err)))
                    .with_context(|| format!("{engine} benchmark statement {}", case.line))?;
                statements = statements.saturating_add(1);
            }
            TestKind::Statement {
                expectation: StatementExpectation::Error,
                ..
            } => skipped = skipped.saturating_add(1),
            TestKind::Query { sql, .. } => {
                for _ in 0..runs {
                    client
                        .query(sql, &[])
                        .await
                        .map_err(|err| anyhow::anyhow!("{}", format_pg_error(&err)))
                        .with_context(|| format!("{engine} benchmark query {}", case.line))?;
                    query_iterations = query_iterations.saturating_add(1);
                }
                query_records = query_records.saturating_add(1);
            }
        }
    }
    Ok(EngineBenchmark {
        engine: engine.to_owned(),
        ok: true,
        error: None,
        statements,
        query_records,
        query_iterations,
        skipped,
        total_ns: start.elapsed().as_nanos(),
    })
}

fn benchmark_cli_engine(
    engine: ReferenceEngine,
    filters: &SkipFilters,
    enabled_features: &BTreeSet<String>,
    cases: &[TestCase],
    runs: u32,
) -> Result<EngineBenchmark> {
    let command = engine.command().with_context(|| {
        format!(
            "reference engine {} does not expose a CLI command",
            engine.suffix()
        )
    })?;
    let db_path = temp_reference_db_path(engine)?;
    let mut script = String::new();
    let mut statements = 0_u64;
    let mut query_records = 0_u64;
    let mut query_iterations = 0_u64;
    let mut skipped = 0_u64;
    for case in cases {
        if effective_skip_reason(filters, enabled_features, case).is_some() {
            skipped = skipped.saturating_add(1);
            continue;
        }
        match &case.kind {
            TestKind::Statement {
                expectation: StatementExpectation::Ok,
                sql,
            } => {
                push_sql_statement(&mut script, sql);
                statements = statements.saturating_add(1);
            }
            TestKind::Statement {
                expectation: StatementExpectation::Error,
                ..
            } => skipped = skipped.saturating_add(1),
            TestKind::Query { sql, .. } => {
                for _ in 0..runs {
                    push_sql_statement(&mut script, sql);
                    query_iterations = query_iterations.saturating_add(1);
                }
                query_records = query_records.saturating_add(1);
            }
        }
    }

    let start = Instant::now();
    let mut child = Command::new(command)
        .arg("-batch")
        .arg("-bail")
        .arg("-noheader")
        .arg("-list")
        .arg("-nullvalue")
        .arg("NULL")
        .arg("-separator")
        .arg("\n")
        .arg(&db_path)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("spawn {command} benchmark engine"))?;
    {
        let mut stdin = child
            .stdin
            .take()
            .context("benchmark child stdin unavailable")?;
        stdin
            .write_all(script.as_bytes())
            .with_context(|| format!("write {command} benchmark script"))?;
    }
    let output = child
        .wait_with_output()
        .with_context(|| format!("wait for {command} benchmark engine"))?;
    let total_ns = start.elapsed().as_nanos();
    let _ = std::fs::remove_file(&db_path);
    if !output.status.success() {
        bail!(
            "{command} benchmark failed with status {}\nstderr:\n{}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
    }

    Ok(EngineBenchmark {
        engine: engine.suffix().to_owned(),
        ok: true,
        error: None,
        statements,
        query_records,
        query_iterations,
        skipped,
        total_ns,
    })
}

fn push_sql_statement(script: &mut String, sql: &str) {
    script.push_str(sql.trim_end());
    if !sql.trim_end().ends_with(';') {
        script.push(';');
    }
    script.push('\n');
}

fn write_benchmark_artifacts(
    output_path: &Path,
    input_paths: &[PathBuf],
    cases: &[TestCase],
    runs: u32,
    benchmarks: &[EngineBenchmark],
) -> Result<()> {
    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create benchmark directory {}", parent.display()))?;
    }

    let generated_at_unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock before Unix epoch")?
        .as_millis();
    let winner = benchmarks
        .iter()
        .filter(|benchmark| benchmark.ok)
        .min_by_key(|benchmark| benchmark.total_ns)
        .map(|benchmark| benchmark.engine.as_str());

    let mut json = String::new();
    writeln!(&mut json, "{{")?;
    writeln!(&mut json, "  \"format_version\": 1,")?;
    writeln!(&mut json, "  \"suite\": \"sqllogictest\",")?;
    writeln!(
        &mut json,
        "  \"generated_at_unix_ms\": {generated_at_unix_ms},"
    )?;
    writeln!(&mut json, "  \"benchmark_runs\": {runs},")?;
    writeln!(&mut json, "  \"case_count\": {},", cases.len())?;
    write!(&mut json, "  \"input_paths\": [")?;
    for (idx, path) in input_paths.iter().enumerate() {
        if idx > 0 {
            write!(&mut json, ", ")?;
        }
        write!(
            &mut json,
            "\"{}\"",
            escape_json(&path.display().to_string())
        )?;
    }
    writeln!(&mut json, "],")?;
    match winner {
        Some(engine) => writeln!(&mut json, "  \"winner\": \"{}\",", escape_json(engine))?,
        None => writeln!(&mut json, "  \"winner\": null,")?,
    }
    writeln!(&mut json, "  \"engines\": [")?;
    for (idx, benchmark) in benchmarks.iter().enumerate() {
        if idx > 0 {
            writeln!(&mut json, ",")?;
        }
        let avg_ns = if benchmark.query_iterations == 0 {
            0
        } else {
            benchmark.total_ns / u128::from(benchmark.query_iterations)
        };
        write!(
            &mut json,
            "    {{\"name\": \"{}\", \"ok\": {}, \"error\": ",
            escape_json(&benchmark.engine),
            benchmark.ok
        )?;
        match &benchmark.error {
            Some(error) => write!(&mut json, "\"{}\"", escape_json(error))?,
            None => write!(&mut json, "null")?,
        }
        write!(
            &mut json,
            ", \"statements\": {}, \"query_records\": {}, \"query_iterations\": {}, \"skipped\": {}, \"total_ns\": {}, \"avg_ns_per_query_iteration\": {avg_ns}}}",
            benchmark.statements,
            benchmark.query_records,
            benchmark.query_iterations,
            benchmark.skipped,
            benchmark.total_ns
        )?;
    }
    writeln!(&mut json)?;
    writeln!(&mut json, "  ]")?;
    writeln!(&mut json, "}}")?;
    std::fs::write(output_path, json)?;

    let markdown_path = output_path.with_extension("md");
    let mut markdown = String::new();
    writeln!(&mut markdown, "# SQLLogicTest Speed Comparison")?;
    writeln!(&mut markdown)?;
    writeln!(&mut markdown, "- suite: SQLLogicTest replay")?;
    writeln!(&mut markdown, "- benchmark_runs: {runs}")?;
    writeln!(&mut markdown, "- case_count: {}", cases.len())?;
    if let Some(engine) = winner {
        writeln!(&mut markdown, "- fastest_engine: `{engine}`")?;
    }
    writeln!(&mut markdown)?;
    writeln!(
        &mut markdown,
        "| engine | ok | statements | query records | query iterations | skipped | total ms | avg us/query iteration |"
    )?;
    writeln!(
        &mut markdown,
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |"
    )?;
    for benchmark in benchmarks {
        let avg_ns = if benchmark.query_iterations == 0 {
            0
        } else {
            benchmark.total_ns / u128::from(benchmark.query_iterations)
        };
        writeln!(
            &mut markdown,
            "| `{}` | {} | {} | {} | {} | {} | {:.3} | {:.3} |",
            benchmark.engine,
            benchmark.ok,
            benchmark.statements,
            benchmark.query_records,
            benchmark.query_iterations,
            benchmark.skipped,
            benchmark.total_ns as f64 / 1_000_000.0,
            avg_ns as f64 / 1_000.0
        )?;
    }
    writeln!(&mut markdown)?;
    writeln!(
        &mut markdown,
        "This is SQLLogicTest replay replay timing, not TPC-H/ClickBench certification."
    )?;
    std::fs::write(markdown_path, markdown)?;
    Ok(())
}

fn escape_json(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '"' => escaped.push_str("\\\""),
            '\\' => escaped.push_str("\\\\"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            ch if ch.is_control() => {
                let _ = write!(&mut escaped, "\\u{:04x}", u32::from(ch));
            }
            ch => escaped.push(ch),
        }
    }
    escaped
}

fn format_cli_reference_rows(
    stdout: &str,
    type_string: &str,
    sort_mode: SortMode,
) -> Result<Vec<String>> {
    let column_count = type_string.chars().count();
    if column_count == 0 {
        bail!("query type string must declare at least one column");
    }
    let values: Vec<String> = stdout
        .lines()
        .map(|line| line.strip_suffix('\r').unwrap_or(line).to_owned())
        .collect();
    if values.len() % column_count != 0 {
        bail!(
            "reference output produced {} values, not divisible by {column_count} column(s)",
            values.len()
        );
    }
    let mut rows: Vec<Vec<String>> = values
        .chunks(column_count)
        .map(<[String]>::to_vec)
        .collect();
    if matches!(sort_mode, SortMode::RowSort) {
        rows.sort();
    }
    Ok(rows.into_iter().flatten().collect())
}

#[derive(Debug)]
enum CaseOutcome {
    Passed,
    Skipped(String),
    Failed(String),
}

async fn run_case(
    client: &Client,
    references: &[ReferenceTarget],
    filters: &SkipFilters,
    enabled_features: &BTreeSet<String>,
    case: &TestCase,
) -> CaseOutcome {
    if let Some(reason) = effective_skip_reason(filters, enabled_features, case) {
        return CaseOutcome::Skipped(reason);
    }

    match &case.kind {
        TestKind::Statement { expectation, sql } => {
            run_statement_case(client, references, *expectation, sql).await
        }
        TestKind::Query {
            type_string,
            sort_mode,
            sql,
            expected,
        } => run_query_case(client, references, type_string, *sort_mode, sql, expected).await,
    }
}

fn effective_skip_reason(
    filters: &SkipFilters,
    enabled_features: &BTreeSet<String>,
    case: &TestCase,
) -> Option<String> {
    if let Some(reason) = &case.skip_reason {
        return Some(reason.clone());
    }
    if let Some(missing) = case
        .requires
        .iter()
        .find(|feature| !enabled_features.contains(feature.as_str()))
    {
        return Some(format!("missing feature `{missing}`"));
    }
    filters.skip_reason(&case.path, case.sql())
}

async fn run_statement_case(
    client: &Client,
    references: &[ReferenceTarget],
    expectation: StatementExpectation,
    sql: &str,
) -> CaseOutcome {
    let actual = client.batch_execute(sql).await;
    let actual_ok = actual.is_ok();
    let expected_ok = matches!(expectation, StatementExpectation::Ok);
    if actual_ok != expected_ok {
        let detail = actual.err().map_or_else(
            || "statement succeeded".to_owned(),
            |err| format_pg_error(&err),
        );
        return CaseOutcome::Failed(format!(
            "statement expectation mismatch: expected {:?}, got {detail}",
            expectation
        ));
    }

    for reference_client in references {
        let reference_ok = reference_client.execute_statement(sql).await.is_ok();
        if reference_ok != actual_ok {
            return CaseOutcome::Failed(format!(
                "reference statement class mismatch: UltraSQL ok={actual_ok}, reference ok={reference_ok}"
            ));
        }
    }

    CaseOutcome::Passed
}

async fn run_query_case(
    client: &Client,
    references: &[ReferenceTarget],
    type_string: &str,
    sort_mode: SortMode,
    sql: &str,
    expected: &QueryExpectation,
) -> CaseOutcome {
    let actual = match execute_query(client, type_string, sort_mode, sql).await {
        Ok(values) => values,
        Err(err) => return CaseOutcome::Failed(format!("query failed: {err}")),
    };

    if let Err(message) = compare_query_expectation(&actual, expected) {
        return CaseOutcome::Failed(format!("{message}\nactual values:\n{}", actual.join("\n")));
    }

    for reference_client in references {
        let reference_values = match reference_client
            .execute_query(type_string, sort_mode, sql)
            .await
        {
            Ok(values) => values,
            Err(err) => return CaseOutcome::Failed(format!("reference query failed: {err}")),
        };
        if reference_values != actual {
            return CaseOutcome::Failed(format!(
                "reference mismatch:\nreference values:\n{}\nactual values:\n{}",
                reference_values.join("\n"),
                actual.join("\n")
            ));
        }
    }

    CaseOutcome::Passed
}

fn compare_query_expectation(actual: &[String], expected: &QueryExpectation) -> Result<()> {
    match expected {
        QueryExpectation::Values(expected_values) => {
            if actual == expected_values {
                Ok(())
            } else {
                bail!("expected values:\n{}", expected_values.join("\n"));
            }
        }
        QueryExpectation::Hash {
            value_count,
            digest,
        } => {
            if actual.len() != *value_count {
                bail!(
                    "expected {value_count} hashed value(s), got {}",
                    actual.len()
                );
            }
            let actual_digest = hash_query_values(actual);
            if actual_digest == *digest {
                Ok(())
            } else {
                bail!("expected hash {digest}, got {actual_digest}");
            }
        }
    }
}

fn hash_query_values(values: &[String]) -> String {
    let mut repr = values.join("\n");
    repr.push('\n');
    format!("{:x}", md5::compute(repr.as_bytes()))
}

async fn execute_query(
    client: &Client,
    type_string: &str,
    sort_mode: SortMode,
    sql: &str,
) -> Result<Vec<String>> {
    let rows = client
        .query(sql, &[])
        .await
        .map_err(|err| anyhow::anyhow!("{}", format_pg_error(&err)))?;
    let expected_columns = type_string.chars().count();
    let mut formatted_rows = Vec::with_capacity(rows.len());
    for row in rows {
        if row.columns().len() != expected_columns {
            bail!(
                "query returned {} columns, type string declares {expected_columns}",
                row.columns().len()
            );
        }
        formatted_rows.push(format_row(&row)?);
    }
    if matches!(sort_mode, SortMode::RowSort) {
        formatted_rows.sort();
    }
    Ok(formatted_rows.into_iter().flatten().collect())
}

fn format_row(row: &Row) -> Result<Vec<String>> {
    let mut out = Vec::with_capacity(row.columns().len());
    for (idx, column) in row.columns().iter().enumerate() {
        out.push(format_cell(row, idx, column.type_())?);
    }
    Ok(out)
}

fn format_cell(row: &Row, idx: usize, ty: &Type) -> Result<String> {
    if *ty == Type::INT2 {
        let value: Option<i16> = row.try_get(idx)?;
        return Ok(format_nullable(value));
    }
    if *ty == Type::INT4 {
        let value: Option<i32> = row.try_get(idx)?;
        return Ok(format_nullable(value));
    }
    if *ty == Type::INT8 {
        let value: Option<i64> = row.try_get(idx)?;
        return Ok(format_nullable(value));
    }
    if *ty == Type::FLOAT4 {
        let value: Option<f32> = row.try_get(idx)?;
        return Ok(format_nullable(value));
    }
    if *ty == Type::FLOAT8 {
        let value: Option<f64> = row.try_get(idx)?;
        return Ok(format_nullable(value));
    }
    if *ty == Type::BOOL {
        let value: Option<bool> = row.try_get(idx)?;
        return Ok(value.map_or_else(|| "NULL".to_owned(), |v| v.to_string()));
    }
    if *ty == Type::TEXT || *ty == Type::VARCHAR || *ty == Type::BPCHAR || *ty == Type::NAME {
        let value: Option<String> = row.try_get(idx)?;
        return Ok(value.unwrap_or_else(|| "NULL".to_owned()));
    }
    bail!(
        "unsupported result type `{}` at column {}",
        ty.name(),
        idx.saturating_add(1)
    )
}

fn format_nullable<T: ToString>(value: Option<T>) -> String {
    value.map_or_else(|| "NULL".to_owned(), |v| v.to_string())
}

fn format_pg_error(err: &tokio_postgres::Error) -> String {
    if let Some(db_error) = err.as_db_error() {
        return format!("{}: {}", db_error.code().code(), db_error.message());
    }
    err.to_string()
}

fn collect_input_files(paths: &[PathBuf]) -> Result<Vec<PathBuf>> {
    let roots = if paths.is_empty() {
        vec![PathBuf::from("tests/slt")]
    } else {
        paths.to_vec()
    };
    let mut files = Vec::new();
    for root in roots {
        collect_path(&root, &mut files)?;
    }
    files.sort();
    Ok(files)
}

fn collect_path(path: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    if path.is_file() {
        if is_slt_file(path) {
            files.push(path.to_path_buf());
        }
        return Ok(());
    }
    if !path.is_dir() {
        bail!("test path does not exist: {}", path.display());
    }
    let mut entries = std::fs::read_dir(path)
        .with_context(|| format!("read directory {}", path.display()))?
        .collect::<std::result::Result<Vec<_>, _>>()
        .with_context(|| format!("read directory entry {}", path.display()))?;
    entries.sort_by_key(|entry| entry.path());
    for entry in entries {
        collect_path(&entry.path(), files)?;
    }
    Ok(())
}

fn is_slt_file(path: &Path) -> bool {
    path.extension()
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| matches!(ext, "slt" | "test"))
}

fn parse_script(path: &Path, text: &str) -> Result<Vec<TestCase>> {
    let lines: Vec<&str> = text.lines().collect();
    let mut idx = 0;
    let mut cases = Vec::new();
    let mut directives = Directives::default();

    while idx < lines.len() {
        let line_no = idx.saturating_add(1);
        let line = lines[idx].trim();
        idx = idx.saturating_add(1);
        if line.is_empty() {
            continue;
        }
        if parse_directive(line, &mut directives)? {
            continue;
        }
        if line.starts_with('#') {
            continue;
        }
        if line.starts_with("hash-threshold") {
            continue;
        }

        if let Some(rest) = line.strip_prefix("statement") {
            let expectation = parse_statement_expectation(path, line_no, rest)?;
            let (sql, next_idx) = collect_until_blank(&lines, idx);
            idx = next_idx;
            let (skip_reason, requires) = directives.take_for_case();
            cases.push(TestCase {
                path: path.to_path_buf(),
                line: line_no,
                kind: TestKind::Statement { expectation, sql },
                skip_reason,
                requires,
            });
            continue;
        }

        if let Some(rest) = line.strip_prefix("query") {
            let (type_string, sort_mode) = parse_query_header(path, line_no, rest)?;
            let (sql, expected, next_idx) = collect_query(&lines, idx)
                .with_context(|| format!("{}:{line_no} parse query", path.display()))?;
            idx = next_idx;
            let (skip_reason, requires) = directives.take_for_case();
            cases.push(TestCase {
                path: path.to_path_buf(),
                line: line_no,
                kind: TestKind::Query {
                    type_string,
                    sort_mode,
                    sql,
                    expected,
                },
                skip_reason,
                requires,
            });
            continue;
        }

        bail!(
            "{}:{line_no} unsupported SQLLogicTest directive `{line}`",
            path.display()
        );
    }

    Ok(cases)
}

fn parse_directive(line: &str, directives: &mut Directives) -> Result<bool> {
    let Some(rest) = line.strip_prefix("# ultrasql:") else {
        return Ok(false);
    };
    let rest = rest.trim();
    if rest == "skip" || rest.starts_with("skip ") {
        let reason = rest.strip_prefix("skip").unwrap_or_default().trim();
        if reason.is_empty() {
            bail!("skip directive requires an explicit reason");
        }
        directives.next_skip = Some(reason.to_owned());
        return Ok(true);
    }
    if let Some(feature) = rest.strip_prefix("require ") {
        directives.next_requires.push(feature.trim().to_owned());
        return Ok(true);
    }
    if rest == "file-skip" || rest.starts_with("file-skip ") {
        let reason = rest.strip_prefix("file-skip").unwrap_or_default().trim();
        if reason.is_empty() {
            bail!("file-skip directive requires an explicit reason");
        }
        directives.file_skip = Some(reason.to_owned());
        return Ok(true);
    }
    if let Some(feature) = rest.strip_prefix("file-require ") {
        directives.file_requires.push(feature.trim().to_owned());
        return Ok(true);
    }
    bail!("unknown UltraSQL SLT directive `{rest}`")
}

fn parse_statement_expectation(
    path: &Path,
    line_no: usize,
    rest: &str,
) -> Result<StatementExpectation> {
    match rest.split_whitespace().next() {
        Some("ok") => Ok(StatementExpectation::Ok),
        Some("error") => Ok(StatementExpectation::Error),
        other => bail!(
            "{}:{line_no} statement must declare `ok` or `error`, got {:?}",
            path.display(),
            other
        ),
    }
}

fn parse_query_header(path: &Path, line_no: usize, rest: &str) -> Result<(String, SortMode)> {
    let mut tokens = rest.split_whitespace();
    let type_string = tokens
        .next()
        .ok_or_else(|| anyhow::anyhow!("{}:{line_no} query missing type string", path.display()))?
        .to_owned();
    let mut sort_mode = SortMode::NoSort;
    for token in tokens {
        match token {
            "nosort" => sort_mode = SortMode::NoSort,
            "sort" | "rowsort" => sort_mode = SortMode::RowSort,
            _ => {}
        }
    }
    Ok((type_string, sort_mode))
}

fn collect_until_blank(lines: &[&str], mut idx: usize) -> (String, usize) {
    let mut sql = Vec::new();
    while idx < lines.len() {
        let line = lines[idx];
        idx = idx.saturating_add(1);
        if line.trim().is_empty() {
            break;
        }
        sql.push(line);
    }
    (sql.join("\n"), idx)
}

fn collect_query(lines: &[&str], mut idx: usize) -> Result<(String, QueryExpectation, usize)> {
    let mut sql = Vec::new();
    while idx < lines.len() {
        let line = lines[idx];
        idx = idx.saturating_add(1);
        if line.trim() == "----" {
            let (expected, next_idx) = collect_expected(lines, idx);
            return Ok((
                sql.join("\n"),
                parse_query_expectation(&expected)?,
                next_idx,
            ));
        }
        sql.push(line);
    }
    bail!("query missing ---- separator")
}

fn parse_query_expectation(lines: &[String]) -> Result<QueryExpectation> {
    if lines.len() == 1 {
        let line = lines[0].trim();
        if let Some((count, digest)) = parse_hash_expectation(line)? {
            return Ok(QueryExpectation::Hash {
                value_count: count,
                digest,
            });
        }
    }
    Ok(QueryExpectation::Values(lines.to_vec()))
}

fn parse_hash_expectation(line: &str) -> Result<Option<(usize, String)>> {
    let Some((count, digest)) = line.split_once(" values hashing to ") else {
        return Ok(None);
    };
    let value_count = count
        .parse::<usize>()
        .with_context(|| format!("invalid hashed value count `{count}`"))?;
    if digest.len() != 32 || !digest.chars().all(|ch| ch.is_ascii_hexdigit()) {
        bail!("invalid SQLLogicTest MD5 digest `{digest}`");
    }
    Ok(Some((value_count, digest.to_ascii_lowercase())))
}

fn collect_expected(lines: &[&str], mut idx: usize) -> (Vec<String>, usize) {
    let mut expected = Vec::new();
    while idx < lines.len() {
        let line = lines[idx];
        idx = idx.saturating_add(1);
        if line.trim().is_empty() {
            break;
        }
        expected.push(line.trim_end().to_owned());
    }
    (expected, idx)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_cli() -> Cli {
        Cli {
            paths: Vec::new(),
            mode: Mode::Wire,
            database_url: None,
            reference_url: None,
            reference_engine: Vec::new(),
            reference_db: None,
            benchmark_output: None,
            benchmark_runs: 1,
            case_limit: None,
            progress_every: 0,
            slow_case_ms: None,
            skip_filters: Vec::new(),
            features: Vec::new(),
        }
    }

    fn temp_path(prefix: &str, ext: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock after Unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("{prefix}-{}-{nanos}.{ext}", std::process::id()))
    }

    fn case_with_sql(sql: &str) -> TestCase {
        TestCase {
            path: PathBuf::from("suite/basic.slt"),
            line: 7,
            kind: TestKind::Statement {
                expectation: StatementExpectation::Ok,
                sql: sql.to_owned(),
            },
            skip_reason: None,
            requires: Vec::new(),
        }
    }

    #[test]
    fn reference_engine_metadata_is_explicit() {
        assert_eq!(ReferenceEngine::Postgres.command(), None);
        assert_eq!(ReferenceEngine::Duckdb.command(), Some("duckdb"));
        assert_eq!(ReferenceEngine::Sqlite.command(), Some("sqlite3"));
        assert_eq!(ReferenceEngine::Postgres.suffix(), "postgres");
        assert_eq!(ReferenceEngine::Duckdb.suffix(), "duckdb");
        assert_eq!(ReferenceEngine::Sqlite.suffix(), "sqlite");
    }

    #[test]
    fn skip_filters_load_comments_match_sql_and_path() {
        let path = temp_path("ultrasql-slt-filter", "txt");
        std::fs::write(
            &path,
            "\n# comment\nSELECT 9\tunsupported scalar\nsuite/\timported shard\n",
        )
        .expect("write skip filter");

        let filters = SkipFilters::load_all(std::slice::from_ref(&path)).expect("load filters");
        assert_eq!(
            filters.skip_reason(Path::new("x.slt"), "SELECT 9"),
            Some("unsupported scalar (SELECT 9)".to_owned())
        );
        assert_eq!(
            filters.skip_reason(Path::new("suite/basic.slt"), "SELECT 1"),
            Some("imported shard (suite/)".to_owned())
        );
        assert_eq!(filters.skip_reason(Path::new("x.slt"), "SELECT 1"), None);

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn skip_filters_reject_malformed_records() {
        let missing_tab = temp_path("ultrasql-slt-filter-missing-tab", "txt");
        std::fs::write(&missing_tab, "SELECT 1\n").expect("write filter");
        let err = SkipFilters::load_all(std::slice::from_ref(&missing_tab))
            .expect_err("missing reason separator must fail");
        assert!(err.to_string().contains("pattern<TAB>reason"));

        let empty_pattern = temp_path("ultrasql-slt-filter-empty-pattern", "txt");
        std::fs::write(&empty_pattern, "\tno pattern\n").expect("write filter");
        let err = SkipFilters::load_all(std::slice::from_ref(&empty_pattern))
            .expect_err("empty pattern must fail");
        assert!(err.to_string().contains("empty skip pattern"));

        let empty_reason = temp_path("ultrasql-slt-filter-empty-reason", "txt");
        std::fs::write(&empty_reason, "SELECT 1\t \n").expect("write filter");
        let err = SkipFilters::load_all(std::slice::from_ref(&empty_reason))
            .expect_err("empty reason must fail");
        assert!(err.to_string().contains("explicit reason"));

        let _ = std::fs::remove_file(missing_tab);
        let _ = std::fs::remove_file(empty_pattern);
        let _ = std::fs::remove_file(empty_reason);
    }

    #[test]
    fn directives_are_consumed_per_case_and_file_scope_persists() {
        let mut directives = Directives::default();
        parse_directive("# ultrasql:file-skip whole file", &mut directives)
            .expect("file skip directive");
        parse_directive("# ultrasql:file-require json", &mut directives)
            .expect("file require directive");
        parse_directive("# ultrasql:skip next only", &mut directives).expect("next skip directive");
        parse_directive("# ultrasql:require xml", &mut directives).expect("next require directive");

        let (skip, requires) = directives.take_for_case();
        assert_eq!(skip, Some("whole file".to_owned()));
        assert_eq!(requires, vec!["json".to_owned(), "xml".to_owned()]);

        let (skip, requires) = directives.take_for_case();
        assert_eq!(skip, Some("whole file".to_owned()));
        assert_eq!(requires, vec!["json".to_owned()]);
    }

    #[test]
    fn directives_reject_empty_and_unknown_directives() {
        let mut directives = Directives::default();
        let err =
            parse_directive("# ultrasql:skip", &mut directives).expect_err("empty skip must fail");
        assert!(err.to_string().contains("explicit reason"));

        let err = parse_directive("# ultrasql:file-skip", &mut directives)
            .expect_err("empty file skip must fail");
        assert!(err.to_string().contains("explicit reason"));

        let err = parse_directive("# ultrasql:unknown thing", &mut directives)
            .expect_err("unknown directive must fail");
        assert!(err.to_string().contains("unknown UltraSQL SLT directive"));

        assert!(!parse_directive("# other:skip", &mut directives).expect("non UltraSQL comment"));
    }

    #[test]
    fn compact_sql_flattens_and_truncates_long_statements() {
        let sql = format!("SELECT\n{}\nFROM table", "x ".repeat(120));
        let compact = compact_sql(&sql);
        assert!(!compact.contains('\n'));
        assert!(compact.ends_with("..."));
        assert!(compact.len() <= 163);
    }

    #[test]
    fn case_limit_truncates_across_files_and_drops_empty_tails() {
        let case_a = case_with_sql("SELECT 1");
        let case_b = case_with_sql("SELECT 2");
        let case_c = case_with_sql("SELECT 3");
        let mut cases_by_file = vec![
            (PathBuf::from("a.slt"), vec![case_a.clone(), case_b]),
            (PathBuf::from("b.slt"), vec![case_c]),
        ];

        apply_case_limit(&mut cases_by_file, 1);

        assert_eq!(cases_by_file.len(), 1);
        assert_eq!(cases_by_file[0].1.len(), 1);
        assert_eq!(cases_by_file[0].1[0].sql(), case_a.sql());
    }

    #[test]
    fn selected_reference_engines_dedupe_and_validate_inputs() {
        let mut cli = empty_cli();
        cli.reference_engine = vec![ReferenceEngine::Duckdb, ReferenceEngine::Duckdb];
        assert_eq!(
            selected_reference_engines(&cli).expect("dedupe engines"),
            vec![ReferenceEngine::Duckdb]
        );

        cli.reference_url = Some("postgres://example".to_owned());
        let err = selected_reference_engines(&cli).expect_err("mixed URL and CLI engines fail");
        assert!(err.to_string().contains("only valid with postgres"));

        let mut cli = empty_cli();
        cli.reference_url = Some("postgres://example".to_owned());
        assert_eq!(
            selected_reference_engines(&cli).expect("reference URL implies postgres"),
            vec![ReferenceEngine::Postgres]
        );

        let mut cli = empty_cli();
        cli.reference_db = Some(PathBuf::from("ref.db"));
        cli.reference_engine = vec![ReferenceEngine::Postgres];
        let err = selected_reference_engines(&cli).expect_err("db path needs one CLI engine");
        assert!(err.to_string().contains("exactly one duckdb or sqlite"));

        let mut cli = empty_cli();
        cli.reference_db = Some(PathBuf::from("ref.db"));
        cli.reference_engine = vec![ReferenceEngine::Sqlite];
        assert_eq!(
            selected_reference_engines(&cli).expect("sqlite db path accepted"),
            vec![ReferenceEngine::Sqlite]
        );
    }

    #[tokio::test]
    async fn benchmark_suite_rejects_zero_runs_before_connecting() {
        let cli = empty_cli();
        let err = run_benchmark_suite(
            &cli,
            &SkipFilters::default(),
            &BTreeSet::new(),
            &[case_with_sql("SELECT 1")],
            0,
        )
        .await
        .expect_err("zero benchmark runs fail");
        assert!(err.to_string().contains("greater than zero"));
    }

    #[test]
    fn failed_benchmark_records_error_text() {
        let benchmark = EngineBenchmark::failed("duckdb", anyhow::anyhow!("missing binary"));
        assert_eq!(benchmark.engine, "duckdb");
        assert!(!benchmark.ok);
        assert_eq!(benchmark.error, Some("missing binary".to_owned()));
        assert_eq!(benchmark.query_iterations, 0);
    }

    #[test]
    fn push_sql_statement_adds_one_terminator() {
        let mut script = String::new();
        push_sql_statement(&mut script, "SELECT 1");
        push_sql_statement(&mut script, "SELECT 2;\n");
        assert_eq!(script, "SELECT 1;\nSELECT 2;\n");
    }

    #[test]
    fn benchmark_artifacts_escape_json_and_mark_fastest_engine() {
        let output = temp_path("ultrasql-slt-benchmark-artifact", "json");
        let markdown = output.with_extension("md");
        let benchmarks = vec![
            EngineBenchmark {
                engine: "ultra\"sql".to_owned(),
                ok: true,
                error: None,
                statements: 2,
                query_records: 1,
                query_iterations: 4,
                skipped: 0,
                total_ns: 4_000,
            },
            EngineBenchmark {
                engine: "slow".to_owned(),
                ok: false,
                error: Some("bad\nthing".to_owned()),
                statements: 0,
                query_records: 0,
                query_iterations: 0,
                skipped: 1,
                total_ns: 9_000,
            },
        ];

        write_benchmark_artifacts(
            &output,
            &[PathBuf::from("tests/slt/a\"b.slt")],
            &[case_with_sql("SELECT 1")],
            4,
            &benchmarks,
        )
        .expect("write artifacts");

        let json = std::fs::read_to_string(&output).expect("read benchmark json");
        assert!(json.contains("\"winner\": \"ultra\\\"sql\""));
        assert!(json.contains("\"bad\\nthing\""));
        assert!(json.contains("\"avg_ns_per_query_iteration\": 1000"));

        let md = std::fs::read_to_string(&markdown).expect("read benchmark markdown");
        assert!(md.contains("fastest_engine: `ultra\"sql`"));
        assert!(md.contains("| `slow` | false |"));

        let _ = std::fs::remove_file(output);
        let _ = std::fs::remove_file(markdown);
    }

    #[test]
    fn escape_json_handles_quotes_slashes_and_controls() {
        assert_eq!(escape_json("\"\\\n\r\t\u{1f}"), "\\\"\\\\\\n\\r\\t\\u001f");
    }

    #[test]
    fn cli_reference_rows_validate_shape_and_sort_rows() {
        assert_eq!(
            format_cli_reference_rows("b\r\na\n", "T", SortMode::RowSort).expect("format rows"),
            vec!["a".to_owned(), "b".to_owned()]
        );
        assert_eq!(
            format_cli_reference_rows("1\na\n2\nb\n", "IT", SortMode::NoSort)
                .expect("format two-column rows"),
            vec![
                "1".to_owned(),
                "a".to_owned(),
                "2".to_owned(),
                "b".to_owned()
            ]
        );

        let err = format_cli_reference_rows("", "", SortMode::NoSort)
            .expect_err("empty type string must fail");
        assert!(err.to_string().contains("at least one column"));

        let err = format_cli_reference_rows("1\n2\n3\n", "II", SortMode::NoSort)
            .expect_err("ragged values must fail");
        assert!(err.to_string().contains("not divisible"));
    }

    #[test]
    fn effective_skip_reason_prefers_case_then_missing_feature_then_filter() {
        let filters = SkipFilters {
            patterns: vec![SkipPattern {
                pattern: "SELECT".to_owned(),
                reason: "filtered".to_owned(),
            }],
        };
        let mut case = case_with_sql("SELECT 1");
        case.skip_reason = Some("case skip".to_owned());
        case.requires = vec!["json".to_owned()];

        assert_eq!(
            effective_skip_reason(&filters, &BTreeSet::new(), &case),
            Some("case skip".to_owned())
        );

        case.skip_reason = None;
        assert_eq!(
            effective_skip_reason(&filters, &BTreeSet::new(), &case),
            Some("missing feature `json`".to_owned())
        );

        let enabled = BTreeSet::from(["json".to_owned()]);
        assert_eq!(
            effective_skip_reason(&filters, &enabled, &case),
            Some("filtered (SELECT)".to_owned())
        );
    }

    #[test]
    fn query_expectation_values_and_hashes_report_mismatches() {
        compare_query_expectation(
            &["1".to_owned()],
            &QueryExpectation::Values(vec!["1".to_owned()]),
        )
        .expect("matching values");

        let err = compare_query_expectation(
            &["1".to_owned()],
            &QueryExpectation::Values(vec!["2".to_owned()]),
        )
        .expect_err("mismatched values fail");
        assert!(err.to_string().contains("expected values"));

        let digest = hash_query_values(&["1".to_owned()]);
        compare_query_expectation(
            &["1".to_owned()],
            &QueryExpectation::Hash {
                value_count: 1,
                digest,
            },
        )
        .expect("matching hash");

        let err = compare_query_expectation(
            &["1".to_owned(), "2".to_owned()],
            &QueryExpectation::Hash {
                value_count: 1,
                digest: "00000000000000000000000000000000".to_owned(),
            },
        )
        .expect_err("wrong hash count fails");
        assert!(err.to_string().contains("expected 1 hashed"));

        let err = compare_query_expectation(
            &["1".to_owned()],
            &QueryExpectation::Hash {
                value_count: 1,
                digest: "00000000000000000000000000000000".to_owned(),
            },
        )
        .expect_err("wrong hash digest fails");
        assert!(err.to_string().contains("expected hash"));
    }

    #[test]
    fn collect_input_files_accepts_slt_and_test_files_only() {
        let root = temp_path("ultrasql-slt-collect", "dir");
        let nested = root.join("nested");
        std::fs::create_dir_all(&nested).expect("create nested directory");
        let slt = root.join("a.slt");
        let test = nested.join("b.test");
        let ignored = root.join("c.sql");
        std::fs::write(&slt, "").expect("write slt");
        std::fs::write(&test, "").expect("write test");
        std::fs::write(&ignored, "").expect("write ignored");

        let files = collect_input_files(std::slice::from_ref(&root)).expect("collect files");
        assert_eq!(files, vec![slt, test]);
        assert!(is_slt_file(Path::new("x.slt")));
        assert!(is_slt_file(Path::new("x.test")));
        assert!(!is_slt_file(Path::new("x.sql")));

        let missing = collect_input_files(&[root.join("missing")]).expect_err("missing path");
        assert!(missing.to_string().contains("test path does not exist"));

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn parse_script_handles_statements_queries_hashes_and_directives() {
        let path = Path::new("suite/basic.slt");
        let text = "\
# regular comment
hash-threshold 10
# ultrasql:require json
statement ok
CREATE TABLE t (x INTEGER)

statement error
SELECT nope

query IT rowsort
SELECT x, y FROM t
----
2
b
1
a

query I
SELECT 1
----
1 values hashing to B026324C6904B2A9CB4B88D6D61C81D1
";

        let cases = parse_script(path, text).expect("parse script");
        assert_eq!(cases.len(), 4);
        assert_eq!(cases[0].requires, vec!["json".to_owned()]);
        match &cases[0].kind {
            TestKind::Statement { expectation, sql } => {
                assert_eq!(*expectation, StatementExpectation::Ok);
                assert_eq!(sql, "CREATE TABLE t (x INTEGER)");
            }
            TestKind::Query { .. } => panic!("expected statement"),
        }
        match &cases[2].kind {
            TestKind::Query {
                type_string,
                sort_mode,
                expected,
                ..
            } => {
                assert_eq!(type_string, "IT");
                assert_eq!(*sort_mode, SortMode::RowSort);
                assert!(matches!(expected, QueryExpectation::Values(values) if values.len() == 4));
            }
            TestKind::Statement { .. } => panic!("expected query"),
        }
        match &cases[3].kind {
            TestKind::Query { expected, .. } => {
                assert!(matches!(
                    expected,
                    QueryExpectation::Hash {
                        value_count: 1,
                        digest
                    } if digest == "b026324c6904b2a9cb4b88d6d61c81d1"
                ));
            }
            TestKind::Statement { .. } => panic!("expected query"),
        }
    }

    #[test]
    fn parse_script_reports_malformed_records() {
        let path = Path::new("bad.slt");
        let cases = [
            (
                "statement maybe\nSELECT 1\n",
                "statement must declare `ok` or `error`",
            ),
            ("query\nSELECT 1\n----\n1\n", "query missing type string"),
            ("query I\nSELECT 1\n", "query missing ---- separator"),
            (
                "query I\nSELECT 1\n----\nnope values hashing to abc\n",
                "invalid hashed value count",
            ),
            (
                "query I\nSELECT 1\n----\n1 values hashing to xyz\n",
                "invalid SQLLogicTest MD5 digest",
            ),
            ("nonsense\n", "unsupported SQLLogicTest directive"),
        ];

        for (script, message) in cases {
            let err = parse_script(path, script).expect_err("malformed script must fail");
            assert!(
                format!("{err:#}").contains(message),
                "expected `{message}` in `{err:#}`"
            );
        }
    }

    #[test]
    fn query_header_ignores_unknown_options_but_keeps_sort_contract() {
        assert_eq!(
            parse_query_header(Path::new("x.slt"), 1, " I nosort label").expect("parse nosort"),
            ("I".to_owned(), SortMode::NoSort)
        );
        assert_eq!(
            parse_query_header(Path::new("x.slt"), 1, " I sort").expect("parse sort"),
            ("I".to_owned(), SortMode::RowSort)
        );
        assert_eq!(
            parse_query_header(Path::new("x.slt"), 1, " I rowsort").expect("parse rowsort"),
            ("I".to_owned(), SortMode::RowSort)
        );
    }

    #[test]
    fn collectors_stop_at_blank_lines() {
        let lines = ["SELECT 1", "FROM t", "", "ignored"];
        let (sql, idx) = collect_until_blank(&lines, 0);
        assert_eq!(sql, "SELECT 1\nFROM t");
        assert_eq!(idx, 3);

        let query_lines = ["SELECT 1", "----", "1", "", "ignored"];
        let (sql, expected, idx) = collect_query(&query_lines, 0).expect("collect query");
        assert_eq!(sql, "SELECT 1");
        assert!(matches!(expected, QueryExpectation::Values(values) if values == vec!["1"]));
        assert_eq!(idx, 4);
    }
}
