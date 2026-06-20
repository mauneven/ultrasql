//! Shared helpers for the wire-protocol cross-engine benchmark driver:
//! lossless numeric conversions, percentile/median math, wire-connection
//! and persistent-server lifecycle, JSON report writing, and the
//! deterministic `SplitMix64` PRNG used by the mixed-OLTP workload.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use sha2::{Digest, Sha256};
use tokio_postgres::NoTls;
use ultrasql_bench::registry::HostInfo;
use ultrasql_server::{Server, bind_listener, serve_listener};

use std::time::Instant;

use super::types::{PersistentBenchServer, TimedQueryMetric};

pub(crate) fn percentile_nearest_rank(sorted_values: &[f64], percentile: f64) -> f64 {
    let Some(last_idx) = sorted_values.len().checked_sub(1) else {
        return f64::NAN;
    };
    let rank_cutoff = usize_to_f64_lossy(sorted_values.len()) * percentile.clamp(0.0, 1.0);
    let mut rank = 1_usize;
    while rank < sorted_values.len() && usize_to_f64_lossy(rank) < rank_cutoff {
        rank += 1;
    }
    let idx = rank.saturating_sub(1).min(last_idx);
    sorted_values[idx]
}

pub(crate) fn usize_to_f64(value: usize, context: &str) -> Result<f64> {
    let converted = usize_to_f64_lossy(value);
    anyhow::ensure!(converted.is_finite(), "{context} does not fit f64");
    Ok(converted)
}

pub(crate) fn u64_to_f64(value: u64, context: &str) -> Result<f64> {
    let converted = u64_to_f64_lossy(value);
    anyhow::ensure!(converted.is_finite(), "{context} does not fit f64");
    Ok(converted)
}

pub(crate) fn usize_to_u64(value: usize) -> u64 {
    let bytes = value.to_le_bytes();
    let mut padded = [0_u8; 8];
    padded[..bytes.len()].copy_from_slice(&bytes);
    u64::from_le_bytes(padded)
}

pub(crate) fn usize_to_f64_lossy(value: usize) -> f64 {
    u64_to_f64_lossy(usize_to_u64(value))
}

pub(crate) fn u64_to_f64_lossy(value: u64) -> f64 {
    let high = u32_from_u64_saturating(value >> 32);
    let low = u32_from_u64_saturating(value & u64::from(u32::MAX));
    f64::from(high) * 4_294_967_296.0 + f64::from(low)
}

pub(crate) fn u32_from_u64_saturating(value: u64) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

pub(crate) fn sql_string(path: &Path) -> String {
    format!("'{}'", path.display().to_string().replace('\'', "''"))
}

pub(crate) async fn connect_sql_server(
    server: SocketAddr,
) -> Result<(tokio_postgres::Client, tokio::task::JoinHandle<()>)> {
    let conn_str = format!("host=127.0.0.1 port={} user=bench_runner", server.port());
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .context("tokio-postgres connect to ultrasqld")?;
    let conn_handle = tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("tokio-postgres connection error: {e}");
        }
    });
    Ok((client, conn_handle))
}

pub(crate) fn simple_query_rows(
    messages: &[tokio_postgres::SimpleQueryMessage],
) -> Vec<Vec<String>> {
    messages
        .iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => {
                let mut values = Vec::new();
                for col in 0..row.columns().len() {
                    values.push(row.get(col).unwrap_or("").to_owned());
                }
                Some(values)
            }
            _ => None,
        })
        .collect()
}

pub(crate) fn answer_sha256(answer: &serde_json::Value) -> Result<String> {
    let bytes = serde_json::to_vec(answer).context("serialize benchmark answer")?;
    let digest = Sha256::digest(&bytes);
    Ok(digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>())
}

pub(crate) fn promote_answer_metrics(
    report: &mut serde_json::Value,
    answer: &serde_json::Value,
    metric_keys: &[&str],
) {
    let Some(answer_object) = answer.as_object() else {
        return;
    };
    for key in metric_keys {
        if let Some(value) = answer_object.get(*key) {
            report[*key] = value.clone();
        }
    }
}

pub(crate) async fn start_persistent_bench_server(
    data_dir: &Path,
) -> Result<PersistentBenchServer> {
    let bind_addr: SocketAddr = "127.0.0.1:0".parse()?;
    let server = Arc::new(Server::init(data_dir).context("persistent server init")?);
    let (listener, bound) = bind_listener(bind_addr).await.context("bind listener")?;
    let handle = tokio::spawn(serve_listener(listener, server));
    Ok(PersistentBenchServer { bound, handle })
}

pub(crate) async fn shutdown_persistent_bench_server(
    client: tokio_postgres::Client,
    conn_handle: tokio::task::JoinHandle<()>,
    server: PersistentBenchServer,
) {
    drop(client);
    conn_handle.abort();
    tokio::time::sleep(Duration::from_millis(20)).await;
    server.handle.abort();
    let _ = server.handle.await;
}

pub(crate) fn directory_size_bytes(path: &Path) -> Result<u64> {
    if !path.exists() {
        return Ok(0);
    }
    let mut total = 0_u64;
    for entry in std::fs::read_dir(path).with_context(|| format!("read {}", path.display()))? {
        let entry = entry.with_context(|| format!("read entry in {}", path.display()))?;
        let metadata = entry
            .metadata()
            .with_context(|| format!("metadata {}", entry.path().display()))?;
        if metadata.is_dir() {
            total = total
                .checked_add(directory_size_bytes(&entry.path())?)
                .context("directory size overflow")?;
        } else if metadata.is_file() {
            total = total
                .checked_add(metadata.len())
                .context("directory size overflow")?;
        }
    }
    Ok(total)
}

pub(crate) fn write_json_report(
    output: Option<&PathBuf>,
    report: &serde_json::Value,
    label: &str,
) -> Result<()> {
    let mut enriched = report.clone();
    if let Some(object) = enriched.as_object_mut()
        && !object.contains_key("host")
    {
        object.insert("host".to_string(), serde_json::json!(HostInfo::from_env()));
    }
    let serialized = serde_json::to_string(&enriched)?;
    if let Some(path) = output {
        std::fs::write(path, &serialized).with_context(|| format!("write {}", path.display()))?;
        eprintln!("{label}: wrote {}", path.display());
    } else {
        println!("{serialized}");
    }
    Ok(())
}

pub(crate) async fn simple_count(client: &tokio_postgres::Client, sql: &str) -> Result<i64> {
    let messages = client
        .simple_query(sql)
        .await
        .with_context(|| format!("run scalar query: {sql}"))?;
    messages
        .iter()
        .find_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => {
                row.get(0).and_then(|value| value.parse::<i64>().ok())
            }
            _ => None,
        })
        .with_context(|| format!("scalar query returned no integer row: {sql}"))
}

pub(crate) fn sorted_f64(mut values: Vec<f64>) -> Vec<f64> {
    ultrasql_bench::sort_f64_nan_last(&mut values);
    values
}

pub(crate) fn median_sorted(values: &[f64]) -> f64 {
    let mid = values.len() / 2;
    if values.len() % 2 == 0 {
        (values[mid - 1] + values[mid]) / 2.0
    } else {
        values[mid]
    }
}

pub(crate) async fn measure_simple_query(
    client: &tokio_postgres::Client,
    label: &str,
    query: &str,
    warmup: usize,
    total_iters: usize,
) -> Result<TimedQueryMetric> {
    let mut samples = Vec::with_capacity(total_iters.saturating_sub(warmup));
    let mut answer_rows = Vec::new();
    for i in 0..total_iters {
        let started = Instant::now();
        let messages = client
            .simple_query(query)
            .await
            .with_context(|| format!("Parquet smoke {label}: {query}"))?;
        let elapsed_us = started.elapsed().as_secs_f64() * 1e6;
        let rows = simple_query_rows(&messages);
        if rows.is_empty() {
            anyhow::bail!("Parquet smoke {label} returned no rows: {query}");
        }
        if i >= warmup {
            samples.push(elapsed_us);
            answer_rows = rows;
        }
    }
    let samples = sorted_f64(samples);
    Ok(TimedQueryMetric {
        median_us: median_sorted(&samples),
        samples_us: samples,
        rows: answer_rows,
    })
}

pub(crate) fn usize_to_u128(value: usize) -> u128 {
    let bytes = value.to_le_bytes();
    let mut padded = [0_u8; 16];
    padded[..bytes.len()].copy_from_slice(&bytes);
    u128::from_le_bytes(padded)
}

/// Compact deterministic SplitMix64 PRNG. Same constants every
/// engine's bench script uses to derive an op stream from the
/// per-iteration seed; kept inline to avoid pulling `rand` into the
/// bench crate dependency tree.
pub(crate) struct SplitMix64(pub(crate) u64);

impl SplitMix64 {
    pub(crate) const fn new(seed: u64) -> Self {
        Self(seed)
    }

    pub(crate) fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    pub(crate) fn next_unit_f64(&mut self) -> f64 {
        // 53 high bits → [0, 1) uniform double, per the standard
        // SplitMix64 → f64 mapping. Matches the SQLite/DuckDB Python
        // baselines' `random.random()` distribution closely enough that
        // the per-op mix matches across engines.
        let bits = self.next_u64() >> 11;
        splitmix_high_bits_to_f64(bits) * (1.0_f64 / 9_007_199_254_740_992.0)
    }

    pub(crate) fn next_i32(&mut self) -> i32 {
        let bytes = self.next_u64().to_le_bytes();
        i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
    }
}

pub(crate) fn splitmix_high_bits_to_f64(bits: u64) -> f64 {
    u64_to_f64_lossy(bits)
}

/// Maximum rows packed into a single `INSERT ... VALUES (...)` statement
/// during preload. A 10 M row inline VALUES list would overrun
/// tokio-postgres' per-message budget and would also stress the
/// server's parser well past the workload under test; 50 000 rows per
/// chunk keeps each statement under a megabyte of SQL text.
pub(crate) const PRELOAD_CHUNK_ROWS: usize = 50_000;
pub(crate) const LATE_MAT_PRELOAD_CHUNK_ROWS: usize = 5_000;

/// Preload `n_rows` of `(id INT, x INT)` rows into `table` via a
/// sequence of multi-row INSERTs, chunked at [`PRELOAD_CHUNK_ROWS`]
/// rows per statement. `x` is set to `id` so analytical workloads
/// (`SUM(x)`, `AVG(x)`, `WHERE x > threshold`) hit non-trivial values.
///
/// Runs outside the timed region for every workload that uses it.
pub(crate) async fn preload_chunked(
    client: &tokio_postgres::Client,
    table: &str,
    n_rows: usize,
) -> Result<()> {
    let mut start = 0;
    while start < n_rows {
        let end = (start + PRELOAD_CHUNK_ROWS).min(n_rows);
        let mut sql = String::with_capacity((end - start) * 24 + 64);
        sql.push_str("INSERT INTO ");
        sql.push_str(table);
        sql.push_str(" VALUES ");
        for j in start..end {
            if j > start {
                sql.push(',');
            }
            sql.push('(');
            sql.push_str(&j.to_string());
            sql.push(',');
            sql.push_str(&j.to_string());
            sql.push(')');
        }
        client
            .batch_execute(&sql)
            .await
            .with_context(|| format!("preload chunk [{start}, {end}) INSERT into {table}"))?;
        start = end;
    }
    Ok(())
}
