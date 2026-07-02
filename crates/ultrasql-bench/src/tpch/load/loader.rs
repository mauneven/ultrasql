//! Engine load entry points and the wire-protocol bulk loaders.
//!
//! This module owns the public [`load_postgres`] / [`load_ultrasql`] entry
//! points (and their feature-gated stubs), the environment-driven load
//! configuration, and the COPY / INSERT loaders that stream `.tbl` rows
//! into an UltraSQL server over the PostgreSQL wire protocol.

use std::path::Path;

use anyhow::{Result, bail};

#[cfg(any(feature = "pg-runner", feature = "sql-bench"))]
use anyhow::Context;
#[cfg(any(feature = "pg-runner", feature = "sql-bench"))]
use bytes::Bytes;
#[cfg(any(feature = "pg-runner", feature = "sql-bench"))]
use futures::SinkExt;
#[cfg(any(feature = "pg-runner", feature = "sql-bench"))]
use std::io::{BufRead, BufReader};

#[cfg(feature = "sql-bench")]
use crate::tpch::{data_gen, schema};

use super::LoadStats;
#[cfg(any(feature = "pg-runner", feature = "sql-bench"))]
use super::arith::tpch_u64_to_f64;
#[cfg(feature = "sql-bench")]
use super::encode::build_ultrasql_insert_sql;
#[cfg(feature = "sql-bench")]
use super::parse_tbl_line;

/// Number of rows per UltraSQL VALUES batch.
#[cfg(feature = "sql-bench")]
pub(crate) const DEFAULT_ULTRASQL_BATCH_SIZE: usize = 256;

/// COPY chunk target for the UltraSQL TPC-H loader.
#[cfg(any(feature = "pg-runner", feature = "sql-bench"))]
pub(crate) const ULTRASQL_COPY_CHUNK_BYTES: usize = 4 * 1024 * 1024;

#[cfg(feature = "sql-bench")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum UltrasqlLoadMethod {
    Copy,
    Insert,
}

#[cfg(feature = "sql-bench")]
pub(crate) fn ultrasql_load_method() -> Result<UltrasqlLoadMethod> {
    match std::env::var("ULTRASQL_TPCH_LOAD_METHOD") {
        Ok(raw) => match raw.to_ascii_lowercase().as_str() {
            "copy" => Ok(UltrasqlLoadMethod::Copy),
            "insert" | "values" => Ok(UltrasqlLoadMethod::Insert),
            other => {
                bail!("unsupported ULTRASQL_TPCH_LOAD_METHOD={other:?}; use `copy` or `insert`")
            }
        },
        Err(std::env::VarError::NotPresent) => Ok(UltrasqlLoadMethod::Copy),
        Err(e) => Err(e).context("read ULTRASQL_TPCH_LOAD_METHOD"),
    }
}

#[cfg(feature = "sql-bench")]
pub(crate) fn ultrasql_batch_size() -> usize {
    std::env::var("ULTRASQL_TPCH_BATCH_SIZE")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|rows| *rows > 0)
        .unwrap_or(DEFAULT_ULTRASQL_BATCH_SIZE)
}

/// Buffer-pool size for the in-process UltraSQL TPC-H harness.
#[cfg(feature = "sql-bench")]
pub(crate) const DEFAULT_ULTRASQL_TPCH_POOL_FRAMES: usize = 262_144;

#[cfg(feature = "sql-bench")]
pub(crate) fn ultrasql_tpch_pool_frames() -> usize {
    std::env::var("ULTRASQL_TPCH_POOL_FRAMES")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|frames| *frames > 0)
        .unwrap_or(DEFAULT_ULTRASQL_TPCH_POOL_FRAMES)
}

#[cfg(feature = "sql-bench")]
pub(crate) fn tpch_progress_enabled() -> bool {
    matches!(
        std::env::var("ULTRASQL_TPCH_PROGRESS").ok().as_deref(),
        Some("1" | "true" | "TRUE" | "yes" | "YES")
    )
}

#[cfg(feature = "sql-bench")]
pub(crate) fn tpch_progress_bytes() -> u64 {
    std::env::var("ULTRASQL_TPCH_PROGRESS_BYTES")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .filter(|bytes| *bytes > 0)
        .unwrap_or(512 * 1024 * 1024)
}

/// Loads one `.tbl` file into PostgreSQL.
///
/// This function is only compiled when the `pg-runner` feature is active.
/// Without the feature it always returns an error.
#[cfg(feature = "pg-runner")]
pub fn load_postgres(
    client: &mut tokio_postgres::Client,
    table: &str,
    data_dir: &Path,
    runtime: &tokio::runtime::Runtime,
) -> Result<LoadStats> {
    let path = data_dir.join(format!("{table}.tbl"));
    let file = std::fs::File::open(&path).with_context(|| format!("open {}", path.display()))?;
    let reader = BufReader::new(file);
    let t0 = std::time::Instant::now();
    let copy_sql = format!("COPY {table} FROM STDIN WITH (DELIMITER '|')");
    let inserted = runtime.block_on(async {
        let sink = client
            .copy_in::<_, Bytes>(&copy_sql)
            .await
            .with_context(|| format!("start COPY into {table}"))?;
        futures::pin_mut!(sink);

        let mut buffer: Vec<u8> = Vec::with_capacity(ULTRASQL_COPY_CHUNK_BYTES);
        let mut total: u64 = 0;
        for line in reader.lines() {
            let line = line.with_context(|| format!("read {}", path.display()))?;
            let line = line.trim_end_matches('|');
            if line.is_empty() {
                continue;
            }
            let needed = line.len().saturating_add(1);
            if !buffer.is_empty() && buffer.len().saturating_add(needed) > ULTRASQL_COPY_CHUNK_BYTES
            {
                let chunk = std::mem::take(&mut buffer);
                sink.as_mut()
                    .send(Bytes::from(chunk))
                    .await
                    .with_context(|| format!("COPY chunk into {table}"))?;
                buffer = Vec::with_capacity(ULTRASQL_COPY_CHUNK_BYTES);
            }
            buffer.extend_from_slice(line.as_bytes());
            buffer.push(b'\n');
            total = total.saturating_add(1);
        }
        if !buffer.is_empty() {
            sink.as_mut()
                .send(Bytes::from(buffer))
                .await
                .with_context(|| format!("COPY final chunk into {table}"))?;
        }
        let inserted = sink
            .finish()
            .await
            .with_context(|| format!("finish COPY into {table}"))?;
        if inserted != total {
            bail!("COPY {table}: server reported {inserted} rows, expected {total}");
        }
        Ok::<u64, anyhow::Error>(inserted)
    })?;

    let elapsed = t0.elapsed().as_secs_f64();
    let rows_per_sec = if elapsed > 0.0 {
        tpch_u64_to_f64(inserted) / elapsed
    } else {
        0.0
    };
    runtime.block_on(async {
        client
            .batch_execute(&format!("ANALYZE {table}"))
            .await
            .with_context(|| format!("ANALYZE {table} after load"))
    })?;

    Ok(LoadStats {
        table: table.to_owned(),
        row_count: inserted,
        rows_per_sec,
    })
}

/// Stub returned when the `pg-runner` feature is not active.
#[cfg(not(feature = "pg-runner"))]
pub fn load_postgres(_table: &str, _data_dir: &Path) -> Result<LoadStats> {
    bail!("NotYetWired: pg-runner feature is not enabled; rebuild with --features pg-runner")
}

/// Loads all TPC-H tables from `data_dir` into UltraSQL.
///
/// Spawns a fresh in-process UltraSQL server, creates the TPC-H schema,
/// loads every `.tbl` file from `data_dir`, and returns per-table stats.
#[cfg(feature = "sql-bench")]
pub fn load_ultrasql(data_dir: &Path) -> Result<Vec<LoadStats>> {
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::Duration;

    use anyhow::Context;
    use tokio_postgres::NoTls;
    use ultrasql_server::{Server, bind_listener, serve_listener};

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .context("build tokio runtime")?;

    let load_result = runtime.block_on(async move {
        let bind_addr: SocketAddr = "127.0.0.1:0".parse().context("parse 127.0.0.1:0")?;
        let (listener, bound) = bind_listener(bind_addr).await.context("bind ultrasqld")?;
        let state = Arc::new(Server::with_sample_database_pool_frames(
            ultrasql_tpch_pool_frames(),
        ));
        let server_task = tokio::spawn(async move {
            if let Err(e) = serve_listener(listener, state).await {
                eprintln!("ultrasqld task exited: {e}");
            }
        });

        let conn_str = format!("host=127.0.0.1 port={} user=tpch_load", bound.port());
        let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
            .await
            .context("connect to ultrasqld")?;
        let conn_handle = tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("tokio-postgres connection error: {e}");
            }
        });

        for stmt in schema::ddl_for_engine(schema::Engine::Ultrasql) {
            client.batch_execute(stmt).await.with_context(|| {
                format!("create schema via `{}`", stmt.lines().next().unwrap_or(""))
            })?;
        }
        let stats = load_ultrasql_into_client(&client, data_dir).await?;

        drop(client);
        conn_handle.abort();
        server_task.abort();
        Ok::<_, anyhow::Error>(stats)
    });
    runtime.shutdown_timeout(Duration::from_secs(2));
    load_result
}

/// Stub returned when the `sql-bench` feature is not active.
#[cfg(not(feature = "sql-bench"))]
pub fn load_ultrasql(_data_dir: &Path) -> Result<Vec<LoadStats>> {
    bail!("NotYetWired: sql-bench feature is not enabled; rebuild with --features sql-bench")
}

/// Loads one TPC-H table over the wire using the configured method
/// (COPY by default, batched INSERT via `ULTRASQL_TPCH_LOAD_METHOD=insert`).
#[cfg(feature = "sql-bench")]
pub(crate) async fn load_ultrasql_table(
    client: &tokio_postgres::Client,
    table: &str,
    data_dir: &Path,
) -> Result<LoadStats> {
    match ultrasql_load_method()? {
        UltrasqlLoadMethod::Copy => load_ultrasql_table_copy(client, table, data_dir).await,
        UltrasqlLoadMethod::Insert => load_ultrasql_table_insert(client, table, data_dir).await,
    }
}

/// Loads all TPC-H tables from `data_dir` into an already-connected UltraSQL client.
#[cfg(feature = "sql-bench")]
pub(crate) async fn load_ultrasql_into_client(
    client: &tokio_postgres::Client,
    data_dir: &Path,
) -> Result<Vec<LoadStats>> {
    let mut stats = Vec::with_capacity(data_gen::TABLE_NAMES.len());
    for table in data_gen::TABLE_NAMES {
        if tpch_progress_enabled() {
            eprintln!("ultrasql tpch load: starting {table}");
        }
        let table_stats = load_ultrasql_table(client, table, data_dir).await?;
        client
            .batch_execute(&format!("ANALYZE {table}"))
            .await
            .with_context(|| format!("ANALYZE {table} after load"))?;
        if tpch_progress_enabled() {
            eprintln!(
                "ultrasql tpch load: loaded {} ({} rows, {:.0} rows/s)",
                table_stats.table, table_stats.row_count, table_stats.rows_per_sec
            );
        }
        stats.push(table_stats);
    }
    Ok(stats)
}

#[cfg(feature = "sql-bench")]
pub(crate) async fn load_ultrasql_table_copy(
    client: &tokio_postgres::Client,
    table: &str,
    data_dir: &Path,
) -> Result<LoadStats> {
    let path = data_dir.join(format!("{table}.tbl"));
    let file = std::fs::File::open(&path).with_context(|| format!("open {}", path.display()))?;
    let reader = BufReader::new(file);
    let t0 = std::time::Instant::now();
    let copy_sql = format!("COPY {table} FROM STDIN WITH (DELIMITER '|')");
    let sink = client
        .copy_in::<_, Bytes>(&copy_sql)
        .await
        .with_context(|| format!("start COPY into {table}"))?;
    futures::pin_mut!(sink);

    let mut buffer: Vec<u8> = Vec::with_capacity(ULTRASQL_COPY_CHUNK_BYTES);
    let mut total: u64 = 0;
    let progress = tpch_progress_enabled();
    let progress_bytes = tpch_progress_bytes();
    let mut sent_bytes = 0_u64;
    let mut next_progress_bytes = progress_bytes;
    for line in reader.lines() {
        let line = line.with_context(|| format!("read {}", path.display()))?;
        let line = line.trim_end_matches('|');
        if line.is_empty() {
            continue;
        }
        let needed = line.len().saturating_add(1);
        if !buffer.is_empty() && buffer.len().saturating_add(needed) > ULTRASQL_COPY_CHUNK_BYTES {
            let chunk = std::mem::take(&mut buffer);
            let chunk_len = u64::try_from(chunk.len()).context("COPY chunk len overflow")?;
            sink.as_mut()
                .send(Bytes::from(chunk))
                .await
                .with_context(|| format!("COPY chunk into {table}"))?;
            sent_bytes = sent_bytes.saturating_add(chunk_len);
            if progress && sent_bytes >= next_progress_bytes {
                let elapsed = t0.elapsed().as_secs_f64();
                let rows_per_sec = if elapsed > 0.0 {
                    tpch_u64_to_f64(total) / elapsed
                } else {
                    0.0
                };
                eprintln!(
                    "ultrasql tpch load: copying {table} ({} rows, {:.1} MiB sent, {:.0} rows/s)",
                    total,
                    tpch_u64_to_f64(sent_bytes) / (1024.0 * 1024.0),
                    rows_per_sec
                );
                next_progress_bytes = sent_bytes.saturating_add(progress_bytes);
            }
            buffer = Vec::with_capacity(ULTRASQL_COPY_CHUNK_BYTES);
        }
        buffer.extend_from_slice(line.as_bytes());
        buffer.push(b'\n');
        total = total.saturating_add(1);
    }
    if !buffer.is_empty() {
        let chunk_len = u64::try_from(buffer.len()).context("COPY final chunk len overflow")?;
        sink.as_mut()
            .send(Bytes::from(buffer))
            .await
            .with_context(|| format!("COPY final chunk into {table}"))?;
        sent_bytes = sent_bytes.saturating_add(chunk_len);
    }
    if progress {
        eprintln!(
            "ultrasql tpch load: finishing {table} COPY ({} rows, {:.1} MiB sent)",
            total,
            tpch_u64_to_f64(sent_bytes) / (1024.0 * 1024.0)
        );
    }
    let inserted = sink
        .finish()
        .await
        .with_context(|| format!("finish COPY into {table}"))?;
    if inserted != total {
        bail!("COPY {table}: server reported {inserted} rows, expected {total}");
    }

    let elapsed = t0.elapsed().as_secs_f64();
    let rows_per_sec = if elapsed > 0.0 {
        tpch_u64_to_f64(inserted) / elapsed
    } else {
        0.0
    };
    Ok(LoadStats {
        table: table.to_owned(),
        row_count: total,
        rows_per_sec,
    })
}

#[cfg(feature = "sql-bench")]
pub(crate) async fn load_ultrasql_table_insert(
    client: &tokio_postgres::Client,
    table: &str,
    data_dir: &Path,
) -> Result<LoadStats> {
    let path = data_dir.join(format!("{table}.tbl"));
    let file = std::fs::File::open(&path).with_context(|| format!("open {}", path.display()))?;
    let reader = BufReader::new(file);
    let t0 = std::time::Instant::now();
    let batch_size = ultrasql_batch_size();

    let mut rows: Vec<Vec<String>> = Vec::with_capacity(batch_size);
    let mut total: u64 = 0;
    let mut inserted = 0_u64;
    for line in reader.lines() {
        let line = line.with_context(|| format!("read {}", path.display()))?;
        if let Some(fields) = parse_tbl_line(&line) {
            rows.push(fields);
            total += 1;
        }
        if rows.len() == batch_size {
            insert_ultrasql_chunk(client, table, &rows).await?;
            inserted += u64::try_from(rows.len()).context("chunk len overflow")?;
            rows.clear();
        }
    }
    if !rows.is_empty() {
        insert_ultrasql_chunk(client, table, &rows).await?;
        inserted += u64::try_from(rows.len()).context("chunk len overflow")?;
    }

    let elapsed = t0.elapsed().as_secs_f64();
    let rows_per_sec = if elapsed > 0.0 {
        tpch_u64_to_f64(inserted) / elapsed
    } else {
        0.0
    };
    Ok(LoadStats {
        table: table.to_owned(),
        row_count: total,
        rows_per_sec,
    })
}

#[cfg(feature = "sql-bench")]
pub(crate) async fn insert_ultrasql_chunk(
    client: &tokio_postgres::Client,
    table: &str,
    rows: &[Vec<String>],
) -> Result<()> {
    let mut pending: Vec<(usize, usize)> = vec![(0, rows.len())];
    while let Some((start, end)) = pending.pop() {
        let chunk = &rows[start..end];
        let sql = build_ultrasql_insert_sql(table, chunk)?;
        match client.batch_execute(&sql).await {
            Ok(()) => {}
            Err(error) if chunk.len() > 1 && is_buffer_pool_exhaustion(&error) => {
                let mid = start + (chunk.len() / 2);
                pending.push((mid, end));
                pending.push((start, mid));
            }
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("insert batch into {table} (rows {}..={})", start + 1, end)
                });
            }
        }
    }
    Ok(())
}

#[cfg(feature = "sql-bench")]
pub(crate) fn is_buffer_pool_exhaustion(error: &tokio_postgres::Error) -> bool {
    error
        .as_db_error()
        .map(|db| db.message().contains("buffer pool exhausted"))
        .unwrap_or_else(|| error.to_string().contains("buffer pool exhausted"))
}
