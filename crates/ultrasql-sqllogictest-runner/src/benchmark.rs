//! Benchmark replay across UltraSQL and reference engines plus artifact output.

use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use tokio_postgres::Client;

use crate::cli::{Cli, ReferenceEngine};
use crate::model::{SkipFilters, StatementExpectation, TestCase, TestKind};
use crate::runner::{effective_skip_reason, format_pg_error};
use crate::target::{
    connect_database, connect_ultrasql_target, selected_reference_engines, temp_reference_db_path,
};

#[derive(Debug)]
pub(crate) struct EngineBenchmark {
    pub(crate) engine: String,
    pub(crate) ok: bool,
    pub(crate) error: Option<String>,
    pub(crate) statements: u64,
    pub(crate) query_records: u64,
    pub(crate) query_iterations: u64,
    pub(crate) skipped: u64,
    pub(crate) total_ns: u128,
}

impl EngineBenchmark {
    pub(crate) fn failed(engine: impl Into<String>, error: anyhow::Error) -> Self {
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

pub(crate) async fn run_benchmark_suite(
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

pub(crate) fn push_sql_statement(script: &mut String, sql: &str) {
    script.push_str(sql.trim_end());
    if !sql.trim_end().ends_with(';') {
        script.push(';');
    }
    script.push('\n');
}

pub(crate) fn write_benchmark_artifacts(
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
            "| `{}` | {} | {} | {} | {} | {} | {} | {} |",
            benchmark.engine,
            benchmark.ok,
            benchmark.statements,
            benchmark.query_records,
            benchmark.query_iterations,
            benchmark.skipped,
            format_thousandths(benchmark.total_ns, 1_000_000),
            format_thousandths(avg_ns, 1_000)
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

fn format_thousandths(value: u128, units_per_whole: u128) -> String {
    let whole = value / units_per_whole;
    let remainder = value % units_per_whole;
    let fractional = remainder.saturating_mul(1_000) / units_per_whole;
    format!("{whole}.{fractional:03}")
}

pub(crate) fn escape_json(value: &str) -> String {
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
