//! CSV gauntlet workloads for the stage regression gate.
//!
//! The public artifact gauntlet (`benchmarks/csv_benchmark_gauntlet.sh`)
//! compares engines and records raw JSON. These in-process runners keep the
//! same workload names inside `regression-gate` so UltraSQL CSV work has a
//! local regression guard without fabricating competitor numbers.

use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::time::Instant;

use tempfile::NamedTempFile;

use crate::registry::{BenchContext, BenchResult, median_f64, p99_f64, require_bench_ok};

#[allow(dead_code)]
const PROD_ROWS: usize = 10_000;
#[allow(dead_code)]
const SMOKE_ROWS: usize = 512;
#[cfg(test)]
const TEST_ROWS: usize = 128;
const CATEGORIES: [&str; 4] = ["alpha", "beta", "gamma", "delta"];

#[derive(Clone, Copy, Debug)]
struct CsvRow<'a> {
    id: u64,
    category: &'a str,
    value: i64,
}

#[derive(Clone, Copy, Debug)]
struct MalformedOutcome {
    accepted: usize,
    rejected: usize,
}

/// Measures first-touch read + parse of the deterministic CSV file.
pub fn run_cold_read(ctx: &BenchContext) -> BenchResult {
    run_file_backed(ctx, |path| {
        let data = require_bench_ok(fs::read_to_string(path), "read benchmark csv");
        count_rows(&data)
    })
}

/// Measures repeated parse of already-read CSV bytes.
pub fn run_warm_read(ctx: &BenchContext) -> BenchResult {
    let file = write_good_csv(row_count());
    let data = require_bench_ok(fs::read_to_string(file.path()), "read benchmark csv");
    let timed = || count_rows(&data);
    measure(ctx, row_count(), timed)
}

/// Measures a pushed CSV category filter.
pub fn run_filter(ctx: &BenchContext) -> BenchResult {
    run_file_backed(ctx, |path| {
        let data = require_bench_ok(fs::read_to_string(path), "read benchmark csv");
        parse_rows(&data)
            .filter(|row| row.category == "alpha")
            .count()
    })
}

/// Measures grouping directly over CSV rows.
pub fn run_group_by(ctx: &BenchContext) -> BenchResult {
    run_file_backed(ctx, |path| {
        let data = require_bench_ok(fs::read_to_string(path), "read benchmark csv");
        let mut groups = HashMap::<&str, usize>::with_capacity(CATEGORIES.len());
        for row in parse_rows(&data) {
            *groups.entry(row.category).or_insert(0) += 1;
        }
        groups.values().sum::<usize>()
    })
}

/// Measures joining CSV rows to a small in-memory dimension table.
pub fn run_join_table(ctx: &BenchContext) -> BenchResult {
    run_file_backed(ctx, |path| {
        let data = require_bench_ok(fs::read_to_string(path), "read benchmark csv");
        let dims = HashMap::from([
            ("alpha", 3_i64),
            ("beta", 5_i64),
            ("gamma", 7_i64),
            ("delta", 11_i64),
        ]);
        parse_rows(&data)
            .map(|row| row.value * dims.get(row.category).copied().unwrap_or(0))
            .sum::<i64>()
    })
}

/// Measures CSV import materialization into owned rows.
pub fn run_copy_import(ctx: &BenchContext) -> BenchResult {
    run_file_backed(ctx, |path| {
        let data = require_bench_ok(fs::read_to_string(path), "read benchmark csv");
        parse_rows(&data)
            .map(|row| (row.id, row.category.to_owned(), row.value))
            .collect::<Vec<_>>()
            .len()
    })
}

/// Measures malformed-row quarantine accounting.
pub fn run_malformed_behavior(ctx: &BenchContext) -> BenchResult {
    let file = write_bad_csv(row_count());
    let timed = || {
        let data = require_bench_ok(fs::read_to_string(file.path()), "read benchmark csv");
        let outcome = parse_with_rejects(&data);
        std::hint::black_box(outcome.rejected);
        outcome.accepted
    };
    measure(ctx, row_count(), timed)
}

fn run_file_backed<T>(
    ctx: &BenchContext,
    mut timed_body: impl FnMut(&std::path::Path) -> T,
) -> BenchResult {
    let file = write_good_csv(row_count());
    let timed = || timed_body(file.path());
    measure(ctx, row_count(), timed)
}

fn measure<T>(ctx: &BenchContext, rows: usize, mut timed_body: impl FnMut() -> T) -> BenchResult {
    for _ in 0..ctx.warmup_iterations {
        std::hint::black_box(timed_body());
    }

    let mut samples = Vec::with_capacity(usize::try_from(ctx.iterations).unwrap_or(0));
    for _ in 0..ctx.iterations {
        let started = Instant::now();
        std::hint::black_box(timed_body());
        samples.push(started.elapsed().as_secs_f64() * 1_000_000.0);
    }

    let median_us = median_f64(&samples);
    let p99_us = p99_f64(&samples);
    let throughput_per_sec = if median_us > 0.0 {
        rows as f64 / (median_us / 1_000_000.0)
    } else {
        0.0
    };

    BenchResult {
        throughput_per_sec,
        p50_latency_us: median_us,
        p99_latency_us: p99_us,
        samples,
    }
}

fn row_count() -> usize {
    #[cfg(test)]
    {
        TEST_ROWS
    }
    #[cfg(not(test))]
    {
        crate::runs::smoke_row_count(PROD_ROWS, SMOKE_ROWS)
    }
}

fn write_good_csv(rows: usize) -> NamedTempFile {
    let mut file = require_bench_ok(NamedTempFile::new(), "create benchmark csv");
    require_bench_ok(writeln!(file, "id,category,value"), "write header");
    for i in 0..rows {
        let category = CATEGORIES[i % CATEGORIES.len()];
        let value = require_bench_ok(i64::try_from(i), "row id fits i64") * 17 - 9;
        require_bench_ok(writeln!(file, "{i},{category},{value}"), "write row");
    }
    require_bench_ok(file.flush(), "flush benchmark csv");
    file
}

fn write_bad_csv(rows: usize) -> NamedTempFile {
    let mut file = require_bench_ok(NamedTempFile::new(), "create malformed benchmark csv");
    require_bench_ok(writeln!(file, "id,category,value"), "write header");
    for i in 0..rows {
        if i % 31 == 0 {
            require_bench_ok(writeln!(file, "{i},broken"), "write malformed row");
        } else {
            let category = CATEGORIES[i % CATEGORIES.len()];
            let value = require_bench_ok(i64::try_from(i), "row id fits i64") * 17 - 9;
            require_bench_ok(writeln!(file, "{i},{category},{value}"), "write row");
        }
    }
    require_bench_ok(file.flush(), "flush malformed benchmark csv");
    file
}

fn count_rows(data: &str) -> usize {
    parse_rows(data).count()
}

fn parse_rows(data: &str) -> impl Iterator<Item = CsvRow<'_>> {
    data.lines().skip(1).filter_map(|line| parse_row(line).ok())
}

fn parse_with_rejects(data: &str) -> MalformedOutcome {
    let mut outcome = MalformedOutcome {
        accepted: 0,
        rejected: 0,
    };
    for line in data.lines().skip(1) {
        if parse_row(line).is_ok() {
            outcome.accepted += 1;
        } else {
            outcome.rejected += 1;
        }
    }
    outcome
}

fn parse_row(line: &str) -> Result<CsvRow<'_>, ()> {
    let mut fields = line.split(',');
    let id = fields.next().ok_or(())?.parse::<u64>().map_err(|_| ())?;
    let category = fields.next().ok_or(())?;
    let value = fields.next().ok_or(())?.parse::<i64>().map_err(|_| ())?;
    if fields.next().is_some() || category.is_empty() {
        return Err(());
    }
    Ok(CsvRow {
        id,
        category,
        value,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::HostInfo;

    fn ctx() -> BenchContext {
        BenchContext {
            iterations: 2,
            warmup_iterations: 1,
            host: HostInfo {
                cpu: "test".to_owned(),
                cores: 1,
                ram_gb: 1,
                os: "test".to_owned(),
            },
        }
    }

    #[test]
    fn csv_gate_workloads_record_samples() {
        for run in [
            run_cold_read,
            run_warm_read,
            run_filter,
            run_group_by,
            run_join_table,
            run_copy_import,
            run_malformed_behavior,
        ] {
            let result = run(&ctx());
            assert_eq!(result.samples.len(), 2);
            assert!(result.throughput_per_sec > 0.0);
            assert!(result.p99_latency_us > 0.0);
        }
    }

    #[test]
    fn malformed_parser_counts_rejects() {
        let file = write_bad_csv(64);
        let data = fs::read_to_string(file.path()).expect("read bad csv");
        let outcome = parse_with_rejects(&data);
        assert!(outcome.accepted > 0);
        assert!(outcome.rejected > 0);
        assert_eq!(outcome.accepted + outcome.rejected, 64);
    }
}
