//! SQLLogicTest runner for UltraSQL.
//!
//! The first implementation is deliberately wire-first: it connects through
//! `tokio-postgres` so every test exercises the same PostgreSQL protocol path
//! used by clients. In-process execution can be added later behind the same
//! parsed test model.

mod benchmark;
mod cli;
mod model;
mod parser;
mod runner;
mod target;

#[cfg(test)]
mod tests;

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use clap::Parser;

use crate::benchmark::{run_benchmark_suite, write_benchmark_artifacts};
use crate::cli::Cli;
use crate::model::{Summary, TestCase};
use crate::parser::{collect_input_files, parse_script};
use crate::runner::{CaseOutcome, run_case};
use crate::target::{connect_reference_targets, connect_ultrasql_target};

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let filters = crate::model::SkipFilters::load_all(&cli.skip_filters)?;
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
