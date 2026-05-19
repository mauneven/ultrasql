//! TPC-H query timing harness.
//!
//! Runs each of the 22 TPC-H queries against the target engine, collects per-
//! run elapsed times, and computes per-query [`QueryTimings`] records ready for
//! serialisation into a [`crate::tpch::baseline::Baseline`].
//!
//! The Postgres execution path is gated behind the `pg-runner` Cargo feature.
//! Without the feature, [`run_postgres`] returns an error.

use std::borrow::Cow;
use std::collections::BTreeSet;

use anyhow::Result;
#[cfg(not(feature = "sql-bench"))]
use anyhow::bail;

#[cfg(feature = "pg-runner")]
use anyhow::Context;

#[cfg(feature = "pg-runner")]
use crate::tpch::baseline::{median, p95};
use crate::tpch::{baseline::QueryTimings, queries};
#[cfg(feature = "pg-runner")]
use std::time::Instant;

/// Result from running all 22 queries against one engine.
#[derive(Debug)]
pub struct RunResult {
    /// Per-query label (e.g. `"q1"`) and timing record.
    pub timings: Vec<(String, QueryTimings)>,
}

/// Materialized rows for one TPC-H query result.
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct QueryRows {
    /// Query label, e.g. `"q1"`.
    pub label: String,
    /// Text cells per row. SQL NULL is represented as `"\\N"` to match the
    /// DuckDB CLI reference path.
    pub rows: Vec<Vec<String>>,
}

/// Per-query UltraSQL validation outcome for keep-going runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryRowsOutcome {
    /// Query label, e.g. `"q1"`.
    pub label: String,
    /// Rows on success, or a concise execution/collection error.
    pub result: std::result::Result<Vec<Vec<String>>, String>,
}

/// Return query numbers selected by CLI, `ULTRASQL_TPCH_QUERY`, or all 22.
pub fn selected_queries(cli_selector: Option<&str>) -> Result<Vec<u8>> {
    if let Some(raw) = cli_selector {
        return parse_query_selector(raw);
    }
    match std::env::var("ULTRASQL_TPCH_QUERY") {
        Ok(raw) => parse_query_selector(&raw),
        Err(std::env::VarError::NotPresent) => Ok((1_u8..=22).collect()),
        Err(error) => Err(anyhow::anyhow!("read ULTRASQL_TPCH_QUERY: {error}")),
    }
}

/// Parse `all`, `N`, `A-B`, or comma/whitespace separated combinations.
pub fn parse_query_selector(raw: &str) -> Result<Vec<u8>> {
    let raw = raw.trim();
    if raw.eq_ignore_ascii_case("all") {
        return Ok((1_u8..=22).collect());
    }
    if raw.is_empty() {
        anyhow::bail!("query selector must not be empty");
    }

    let mut queries = BTreeSet::new();
    for token in raw.split([',', ' ', '\t', '\n']).filter(|s| !s.is_empty()) {
        if let Some((start, end)) = token.split_once('-') {
            let start = parse_query_number(start)?;
            let end = parse_query_number(end)?;
            if start > end {
                anyhow::bail!("query range `{token}` is descending");
            }
            queries.extend(start..=end);
        } else {
            queries.insert(parse_query_number(token)?);
        }
    }

    Ok(queries.into_iter().collect())
}

fn parse_query_number(raw: &str) -> Result<u8> {
    let query = raw
        .parse::<u8>()
        .map_err(|_| anyhow::anyhow!("query selector must use integers 1..=22, got `{raw}`"))?;
    if !(1..=22).contains(&query) {
        anyhow::bail!("query selector must be between 1 and 22, got `{query}`");
    }
    Ok(query)
}

/// Return SQL text for a TPC-H query number, honoring `ULTRASQL_TPCH_SQL_FILE`.
pub fn query_sql(n: u8) -> Result<Cow<'static, str>> {
    match std::env::var("ULTRASQL_TPCH_SQL_FILE") {
        Ok(path) => Ok(Cow::Owned(std::fs::read_to_string(&path).map_err(
            |error| anyhow::anyhow!("read ULTRASQL_TPCH_SQL_FILE `{path}`: {error}"),
        )?)),
        Err(std::env::VarError::NotPresent) => Ok(Cow::Borrowed(
            queries::query(n).expect("queries 1..=22 always present"),
        )),
        Err(error) => Err(anyhow::anyhow!("read ULTRASQL_TPCH_SQL_FILE: {error}")),
    }
}

#[cfg(feature = "sql-bench")]
fn progress_enabled() -> bool {
    matches!(
        std::env::var("ULTRASQL_TPCH_PROGRESS").ok().as_deref(),
        Some("1") | Some("true") | Some("TRUE") | Some("yes") | Some("YES")
    )
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
    queries: &[u8],
) -> Result<RunResult> {
    let mut timings: Vec<(String, QueryTimings)> = Vec::new();

    for &n in queries {
        let sql = query_sql(n)?;
        let label = format!("q{n}");

        // Warmup passes (discarded).
        for _ in 0..warmup {
            runtime.block_on(async {
                client
                    .simple_query(sql.as_ref())
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
                    .simple_query(sql.as_ref())
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
pub fn run_postgres(_warmup: usize, _runs: usize, _queries: &[u8]) -> Result<RunResult> {
    bail!("NotYetWired: pg-runner feature is not enabled; rebuild with --features pg-runner")
}

/// Runs all 22 TPC-H queries against an in-process UltraSQL server.
///
/// Spawns a fresh `ultrasqld` on an ephemeral port, runs the eight TPC-H
/// `CREATE TABLE` statements, loads the `.tbl` data from `data_dir`, then
/// runs each query for `warmup + runs` iterations and records the measured
/// timings. Queries that fail on the
/// current SQL surface are recorded with `f64::NAN` medians so the caller
/// can see exactly which TPC-H shape is unsupported.
#[cfg(feature = "sql-bench")]
pub fn run_ultrasql(
    data_dir: &std::path::Path,
    warmup: usize,
    runs: usize,
    queries: &[u8],
) -> Result<RunResult> {
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use anyhow::Context;
    use tokio_postgres::NoTls;
    use ultrasql_server::{Server, bind_listener, serve_listener};

    use crate::tpch::baseline::{median, p95};
    use crate::tpch::{load, schema};

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .context("build tokio runtime")?;

    let run_result = runtime.block_on(async move {
        let bind_addr: SocketAddr = "127.0.0.1:0".parse().context("parse 127.0.0.1:0")?;
        let (listener, bound) = bind_listener(bind_addr).await.context("bind ultrasqld")?;
        let state = Arc::new(Server::with_sample_database_pool_frames(
            load::ultrasql_tpch_pool_frames(),
        ));
        let server_state = Arc::clone(&state);
        let server_task = tokio::spawn(async move {
            if let Err(e) = serve_listener(listener, server_state).await {
                eprintln!("ultrasqld task exited: {e}");
            }
        });

        let conn_str = format!("host=127.0.0.1 port={} user=ultrasql_bench", bound.port());
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
        if schema_errors.is_empty() {
            if load::ultrasql_direct_load_enabled() {
                load::load_ultrasql_direct_into_server(state.as_ref(), &client, data_dir)
                    .await
                    .context("direct-load TPC-H data into UltraSQL")?;
            } else {
                load::load_ultrasql_into_client(&client, data_dir)
                    .await
                    .context("load TPC-H data into UltraSQL")?;
            }
            if progress_enabled() {
                eprintln!("ultrasql tpch: load complete");
            }
        }

        let mut timings: Vec<(String, QueryTimings)> = Vec::new();
        for &n in queries {
            let sql = query_sql(n)?;
            let label = format!("q{n}");
            let mut measured: Vec<f64> = Vec::with_capacity(runs);
            let mut first_error: Option<String> = None;

            if progress_enabled() {
                eprintln!("ultrasql tpch: starting {label}");
            }

            let fmt_err = |e: &tokio_postgres::Error| -> String {
                e.as_db_error()
                    .map(|d| d.message().to_owned())
                    .unwrap_or_else(|| e.to_string())
            };

            for _ in 0..warmup {
                if let Err(e) = client.batch_execute(sql.as_ref()).await {
                    first_error.get_or_insert(format!("warmup: {}", fmt_err(&e)));
                    break;
                }
            }
            if first_error.is_none() {
                for _ in 0..runs {
                    let t0 = Instant::now();
                    if let Err(e) = client.batch_execute(sql.as_ref()).await {
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
            } else if progress_enabled() {
                eprintln!("ultrasql tpch: finished {label}");
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
    });
    runtime.shutdown_timeout(Duration::from_secs(2));
    let (timings, errors) = run_result?;

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

/// Runs selected TPC-H queries against in-process UltraSQL and returns rows.
///
/// This is the correctness gate companion to [`run_ultrasql`]: same schema,
/// loader, server path, and query texts, but it collects `simple_query`
/// result rows instead of discarding them for timing.
#[cfg(feature = "sql-bench")]
pub fn run_ultrasql_results(data_dir: &std::path::Path, queries: &[u8]) -> Result<Vec<QueryRows>> {
    let outcomes = run_ultrasql_result_outcomes(data_dir, queries, false)?;
    outcomes
        .into_iter()
        .map(|outcome| match outcome.result {
            Ok(rows) => Ok(QueryRows {
                label: outcome.label,
                rows,
            }),
            Err(error) => anyhow::bail!("{}: {error}", outcome.label),
        })
        .collect()
}

/// Runs selected TPC-H queries against in-process UltraSQL and keeps per-query
/// failures when requested.
#[cfg(feature = "sql-bench")]
pub fn run_ultrasql_result_outcomes(
    data_dir: &std::path::Path,
    queries: &[u8],
    keep_going: bool,
) -> Result<Vec<QueryRowsOutcome>> {
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::Duration;

    use anyhow::Context;
    use tokio_postgres::{NoTls, SimpleQueryMessage};
    use ultrasql_server::{Server, bind_listener, serve_listener};

    use crate::tpch::{load, schema};

    fn collect_rows(messages: &[SimpleQueryMessage]) -> Result<Vec<Vec<String>>> {
        let mut rows = Vec::new();
        for message in messages {
            let SimpleQueryMessage::Row(row) = message else {
                continue;
            };
            let mut out = Vec::with_capacity(row.len());
            for idx in 0..row.len() {
                let cell = row
                    .try_get(idx)
                    .with_context(|| format!("read result column {idx}"))?
                    .unwrap_or("\\N")
                    .to_owned();
                out.push(cell);
            }
            rows.push(out);
        }
        Ok(rows)
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .context("build tokio runtime")?;

    let run_result = runtime.block_on(async move {
        let bind_addr: SocketAddr = "127.0.0.1:0".parse().context("parse 127.0.0.1:0")?;
        let (listener, bound) = bind_listener(bind_addr).await.context("bind ultrasqld")?;
        let state = Arc::new(Server::with_sample_database_pool_frames(
            load::ultrasql_tpch_pool_frames(),
        ));
        let server_state = Arc::clone(&state);
        let server_task = tokio::spawn(async move {
            if let Err(e) = serve_listener(listener, server_state).await {
                eprintln!("ultrasqld task exited: {e}");
            }
        });

        let conn_str = format!("host=127.0.0.1 port={} user=ultrasql_bench", bound.port());
        let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
            .await
            .context("tokio-postgres connect to ultrasqld")?;
        let conn_handle = tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("tokio-postgres connection error: {e}");
            }
        });

        for stmt in schema::ddl_for_engine(schema::Engine::Ultrasql) {
            client
                .batch_execute(stmt)
                .await
                .with_context(|| format!("DDL: {}", stmt.lines().next().unwrap_or("").trim()))?;
        }
        if load::ultrasql_direct_load_enabled() {
            load::load_ultrasql_direct_into_server(state.as_ref(), &client, data_dir)
                .await
                .context("direct-load TPC-H data into UltraSQL")?;
        } else {
            load::load_ultrasql_into_client(&client, data_dir)
                .await
                .context("load TPC-H data into UltraSQL")?;
        }
        if progress_enabled() {
            eprintln!("ultrasql tpch validate: load complete");
        }

        let mut results = Vec::new();
        for &n in queries {
            let label = format!("q{n}");
            let sql = query_sql(n)?;
            if progress_enabled() {
                eprintln!("ultrasql tpch validate: starting {label}");
            }
            match client.simple_query(sql.as_ref()).await {
                Ok(messages) => match collect_rows(&messages) {
                    Ok(rows) => {
                        if progress_enabled() {
                            eprintln!(
                                "ultrasql tpch validate: finished {label} ({} rows)",
                                rows.len()
                            );
                        }
                        results.push(QueryRowsOutcome {
                            label,
                            result: Ok(rows),
                        });
                    }
                    Err(error) if keep_going => {
                        if progress_enabled() {
                            eprintln!("ultrasql tpch validate: {label} collect failed: {error:#}");
                        }
                        results.push(QueryRowsOutcome {
                            label,
                            result: Err(format!("collect rows: {error:#}")),
                        });
                    }
                    Err(error) => {
                        return Err(error).with_context(|| format!("collect {label} rows"));
                    }
                },
                Err(error) if keep_going => {
                    let detail = error
                        .as_db_error()
                        .map(|db| db.message().to_owned())
                        .unwrap_or_else(|| error.to_string());
                    if progress_enabled() {
                        eprintln!("ultrasql tpch validate: {label} failed: {detail}");
                    }
                    results.push(QueryRowsOutcome {
                        label,
                        result: Err(detail),
                    });
                }
                Err(error) => {
                    return Err(error).with_context(|| format!("run {label}"));
                }
            }
        }

        drop(client);
        conn_handle.abort();
        server_task.abort();
        Ok(results)
    });
    runtime.shutdown_timeout(Duration::from_secs(2));
    run_result
}

/// Stub: returns an error when the `sql-bench` feature is not active.
#[cfg(not(feature = "sql-bench"))]
pub fn run_ultrasql_results(
    _data_dir: &std::path::Path,
    _queries: &[u8],
) -> Result<Vec<QueryRows>> {
    bail!(
        "NotYetWired: rebuild ultrasql-bench with --features sql-bench to \
         enable the in-process TPC-H validator"
    )
}

/// Stub: returns an error when the `sql-bench` feature is not active.
#[cfg(not(feature = "sql-bench"))]
pub fn run_ultrasql_result_outcomes(
    _data_dir: &std::path::Path,
    _queries: &[u8],
    _keep_going: bool,
) -> Result<Vec<QueryRowsOutcome>> {
    bail!(
        "NotYetWired: rebuild ultrasql-bench with --features sql-bench to \
         enable the in-process TPC-H validator"
    )
}

/// Stub: returns an error when the `sql-bench` feature is not active.
#[cfg(not(feature = "sql-bench"))]
pub fn run_ultrasql(
    _data_dir: &std::path::Path,
    _warmup: usize,
    _runs: usize,
    _queries: &[u8],
) -> Result<RunResult> {
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
    fn query_selector_accepts_list_and_range() {
        assert_eq!(
            parse_query_selector("4,11,16,18").unwrap(),
            vec![4, 11, 16, 18]
        );
        assert_eq!(
            parse_query_selector("14-16 18").unwrap(),
            vec![14, 15, 16, 18]
        );
    }

    #[test]
    fn query_selector_deduplicates_and_accepts_all() {
        assert_eq!(parse_query_selector("1,1,2").unwrap(), vec![1, 2]);
        assert_eq!(
            parse_query_selector("all").unwrap(),
            (1_u8..=22).collect::<Vec<_>>()
        );
    }

    #[test]
    fn query_selector_rejects_invalid_values() {
        assert!(parse_query_selector("0").is_err());
        assert!(parse_query_selector("23").is_err());
        assert!(parse_query_selector("9-4").is_err());
        assert!(parse_query_selector("").is_err());
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
