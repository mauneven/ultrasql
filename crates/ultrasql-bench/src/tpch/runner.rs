//! TPC-H query timing harness.
//!
//! Runs each of the 22 TPC-H queries against the target engine, collects per-
//! run elapsed times, and computes per-query [`QueryTimings`] records ready for
//! serialisation into a [`crate::tpch::baseline::Baseline`].
//!
//! The Postgres execution path is gated behind the `pg-runner` Cargo feature.
//! Without the feature, [`run_postgres`] returns an error.

use anyhow::Result;
#[cfg(not(feature = "sql-bench"))]
use anyhow::bail;

#[cfg(feature = "pg-runner")]
use anyhow::Context;

use crate::tpch::baseline::QueryTimings;
#[cfg(feature = "pg-runner")]
use crate::tpch::baseline::{median, p95};
#[cfg(feature = "pg-runner")]
use crate::tpch::queries;
#[cfg(feature = "pg-runner")]
use std::time::Instant;

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

/// Runs all 22 TPC-H queries against an in-process UltraSQL server.
///
/// Spawns a fresh `ultrasqld` on an ephemeral port, runs the eight TPC-H
/// `CREATE TABLE` statements, then runs each query for `warmup + runs`
/// iterations and records the measured timings. Queries that fail on the
/// current SQL surface are recorded with `f64::NAN` medians so the caller
/// can see exactly which TPC-H shape is unsupported.
///
/// The data side (loading `.tbl` files into UltraSQL) is owned by
/// [`crate::tpch::load`] — this function does not insert rows. Run after
/// loading data into the same in-process server is a follow-up wiring item
/// once the loader's UltraSQL path lands.
#[cfg(feature = "sql-bench")]
pub fn run_ultrasql(warmup: usize, runs: usize) -> Result<RunResult> {
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::Instant;

    use anyhow::Context;
    use tokio_postgres::NoTls;
    use ultrasql_server::{Server, bind_listener, serve_listener};

    use crate::tpch::baseline::{median, p95};
    use crate::tpch::{queries, schema};

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .context("build tokio runtime")?;

    let (timings, errors) = runtime.block_on(async move {
        let bind_addr: SocketAddr = "127.0.0.1:0"
            .parse()
            .context("parse 127.0.0.1:0")?;
        let (listener, bound) =
            bind_listener(bind_addr).await.context("bind ultrasqld")?;
        let state = Arc::new(Server::with_sample_database());
        let server_task = tokio::spawn(async move {
            if let Err(e) = serve_listener(listener, state).await {
                eprintln!("ultrasqld task exited: {e}");
            }
        });

        let conn_str =
            format!("host=127.0.0.1 port={} user=ultrasql_bench", bound.port());
        let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
            .await
            .context("tokio-postgres connect to ultrasqld")?;
        let conn_handle = tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("tokio-postgres connection error: {e}");
            }
        });

        let mut schema_errors: Vec<String> = Vec::new();
        for stmt in schema::ddl_for_engine(schema::Engine::Ultrasql) {
            if let Err(e) = client.batch_execute(stmt).await {
                let detail = e
                    .as_db_error()
                    .map(|d| d.message().to_owned())
                    .unwrap_or_else(|| e.to_string());
                schema_errors.push(format!(
                    "DDL: {detail}\n    SQL: {head}",
                    head = stmt.lines().next().unwrap_or("").trim()
                ));
            }
        }

        let mut timings: Vec<(String, QueryTimings)> = Vec::new();
        for n in 1_u8..=22 {
            let sql = queries::query(n).expect("queries 1..=22 always present");
            let label = format!("q{n}");
            let mut measured: Vec<f64> = Vec::with_capacity(runs);
            let mut first_error: Option<String> = None;

            let fmt_err = |e: &tokio_postgres::Error| -> String {
                e.as_db_error()
                    .map(|d| d.message().to_owned())
                    .unwrap_or_else(|| e.to_string())
            };

            for _ in 0..warmup {
                if let Err(e) = client.batch_execute(sql).await {
                    first_error.get_or_insert(format!("warmup: {}", fmt_err(&e)));
                    break;
                }
            }
            if first_error.is_none() {
                for _ in 0..runs {
                    let t0 = Instant::now();
                    if let Err(e) = client.batch_execute(sql).await {
                        first_error.get_or_insert(format!("run: {}", fmt_err(&e)));
                        break;
                    }
                    measured.push(t0.elapsed().as_secs_f64() * 1_000.0);
                }
            }

            let (median_ms, p95_ms) = if measured.is_empty() {
                (f64::NAN, f64::NAN)
            } else {
                (median(&measured), p95(&measured))
            };
            if let Some(msg) = first_error {
                eprintln!("ultrasql {label}: {msg}");
            }
            timings.push((
                label,
                QueryTimings {
                    median_ms,
                    p95_ms,
                    runs: measured,
                },
            ));
        }

        drop(client);
        conn_handle.abort();
        server_task.abort();

        Ok::<_, anyhow::Error>((timings, schema_errors))
    })?;

    if !errors.is_empty() {
        eprintln!(
            "ultrasql TPC-H run: {} DDL statement(s) failed; the dependent \
             queries will report NaN medians:",
            errors.len()
        );
        for e in &errors {
            eprintln!("  - {e}");
        }
    }

    Ok(RunResult { timings })
}

/// Stub: returns an error when the `sql-bench` feature is not active.
#[cfg(not(feature = "sql-bench"))]
pub fn run_ultrasql(_warmup: usize, _runs: usize) -> Result<RunResult> {
    bail!(
        "NotYetWired: rebuild ultrasql-bench with --features sql-bench to \
         enable the in-process TPC-H runner"
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
