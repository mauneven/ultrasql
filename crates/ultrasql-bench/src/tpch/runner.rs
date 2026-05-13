//! TPC-H query timing harness.
//!
//! Runs each of the 22 TPC-H queries against the target engine, collects per-
//! run elapsed times, and computes per-query [`QueryTimings`] records ready for
//! serialisation into a [`crate::tpch::baseline::Baseline`].
//!
//! The Postgres execution path is gated behind the `pg-runner` Cargo feature.
//! Without the feature, [`run_postgres`] returns an error.

use std::time::Instant;

use anyhow::{Context, Result, bail};

use crate::tpch::baseline::{QueryTimings, median, p95};
use crate::tpch::queries;

/// Result from running all 22 queries against one engine.
#[derive(Debug)]
pub struct RunResult {
    /// Per-query label (e.g. `"q1"`) and timing record.
    pub timings: Vec<(String, QueryTimings)>,
}

/// Runs all 22 TPC-H queries against PostgreSQL.
///
/// - `warmup` iterations are discarded.
/// - `runs` iterations are measured.
///
/// Requires the `pg-runner` feature; returns an error otherwise.
#[cfg(feature = "pg-runner")]
pub fn run_postgres(
    client: &mut tokio_postgres::Client,
    warmup: usize,
    runs: usize,
    runtime: &tokio::runtime::Runtime,
) -> Result<RunResult> {
    let mut timings: Vec<(String, QueryTimings)> = Vec::new();

    for n in 1u8..=22 {
        let sql = queries::query(n).expect("queries 1-22 always present");
        let label = format!("q{n}");

        // Warmup passes (discarded).
        for _ in 0..warmup {
            runtime.block_on(async {
                client
                    .simple_query(sql)
                    .await
                    .with_context(|| format!("warmup {label}"))
            })?;
        }

        // Measured passes.
        let mut elapsed_ms: Vec<f64> = Vec::with_capacity(runs);
        for _ in 0..runs {
            let t0 = Instant::now();
            runtime.block_on(async {
                client
                    .simple_query(sql)
                    .await
                    .with_context(|| format!("run {label}"))
            })?;
            let ms = t0.elapsed().as_secs_f64() * 1_000.0;
            elapsed_ms.push(ms);
        }

        timings.push((
            label,
            QueryTimings {
                median_ms: median(&elapsed_ms),
                p95_ms: p95(&elapsed_ms),
                runs: elapsed_ms,
            },
        ));
    }

    Ok(RunResult { timings })
}

/// Stub: returns an error when the `pg-runner` feature is not active.
#[cfg(not(feature = "pg-runner"))]
pub fn run_postgres(_warmup: usize, _runs: usize) -> Result<RunResult> {
    bail!("NotYetWired: pg-runner feature is not enabled; rebuild with --features pg-runner")
}

/// Runs all 22 TPC-H queries against UltraSQL.
///
/// Currently returns `Error::NotYetWired` because the executor's datasource
/// lowering path is not yet available (targeted for v0.6+ executor refactor).
pub fn run_ultrasql(_warmup: usize, _runs: usize) -> Result<RunResult> {
    bail!(
        "NotYetWired: UltraSQL query runner is pending the executor datasource \
         refactor (v0.6+)"
    )
}

/// Computes the geometric mean of all per-query `median_ms` values.
///
/// Returns `0.0` when `result` contains no queries or any median is `<= 0.0`.
pub fn geometric_mean(result: &RunResult) -> f64 {
    if result.timings.is_empty() {
        return 0.0;
    }
    let log_sum: f64 = result
        .timings
        .iter()
        .map(|(_, t)| {
            if t.median_ms > 0.0 {
                t.median_ms.ln()
            } else {
                0.0
            }
        })
        .sum();
    let n = result.timings.len() as f64;
    (log_sum / n).exp()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_run(medians: &[f64]) -> RunResult {
        let timings = medians
            .iter()
            .enumerate()
            .map(|(i, &m)| {
                (
                    format!("q{}", i + 1),
                    QueryTimings {
                        median_ms: m,
                        p95_ms: m * 1.1,
                        runs: vec![m],
                    },
                )
            })
            .collect();
        RunResult { timings }
    }

    #[test]
    fn geometric_mean_single() {
        let r = make_run(&[100.0]);
        let gm = geometric_mean(&r);
        assert!((gm - 100.0).abs() < 1e-9, "expected 100, got {gm}");
    }

    #[test]
    fn geometric_mean_two_values() {
        // geometric mean of 4 and 16 is 8.
        let r = make_run(&[4.0, 16.0]);
        let gm = geometric_mean(&r);
        assert!((gm - 8.0).abs() < 1e-9, "expected 8, got {gm}");
    }

    #[test]
    fn geometric_mean_empty() {
        let r = RunResult { timings: vec![] };
        assert!((geometric_mean(&r) - 0.0).abs() < 1e-9);
    }
}
