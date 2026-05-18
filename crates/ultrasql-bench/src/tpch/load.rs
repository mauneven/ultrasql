//! TPC-H `.tbl` file loader.
//!
//! Reads pipe-delimited `.tbl` files produced by `dbgen` (or the synthetic
//! fallback in [`crate::tpch::data_gen`]) and bulk-inserts the rows into the
//! target engine using batched transactions of up to [`BATCH_SIZE`] rows each.
//!
//! The Postgres path is gated behind the `pg-runner` Cargo feature. When the
//! feature is disabled, calling [`load_postgres`] returns an `anyhow` error
//! describing the missing feature gate.

use std::path::Path;

use anyhow::{Context, Result, bail};

#[cfg(any(feature = "pg-runner", feature = "sql-bench"))]
use bytes::Bytes;
#[cfg(any(feature = "pg-runner", feature = "sql-bench"))]
use futures::SinkExt;

#[cfg(any(feature = "pg-runner", feature = "sql-bench"))]
use std::io::{BufRead, BufReader};

#[cfg(any(test, feature = "sql-bench"))]
use std::fmt::Write as _;

#[cfg(feature = "sql-bench")]
use crate::tpch::data_gen;
#[cfg(feature = "sql-bench")]
use crate::tpch::schema;

/// Number of rows per INSERT transaction batch.
pub const BATCH_SIZE: usize = 10_000;

/// Number of rows per UltraSQL VALUES batch.
#[cfg(feature = "sql-bench")]
const DEFAULT_ULTRASQL_BATCH_SIZE: usize = 256;

/// COPY chunk target for the UltraSQL TPC-H loader.
#[cfg(any(feature = "pg-runner", feature = "sql-bench"))]
const ULTRASQL_COPY_CHUNK_BYTES: usize = 4 * 1024 * 1024;

#[cfg(feature = "sql-bench")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum UltrasqlLoadMethod {
    Copy,
    Insert,
}

#[cfg(feature = "sql-bench")]
fn ultrasql_load_method() -> Result<UltrasqlLoadMethod> {
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
fn ultrasql_batch_size() -> usize {
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
fn tpch_progress_enabled() -> bool {
    matches!(
        std::env::var("ULTRASQL_TPCH_PROGRESS").ok().as_deref(),
        Some("1" | "true" | "TRUE" | "yes" | "YES")
    )
}

/// Row-count summary returned after a successful load.
#[derive(Debug)]
pub struct LoadStats {
    /// Name of the table that was loaded.
    pub table: String,
    /// Total rows inserted.
    pub row_count: u64,
    /// Load throughput in rows per second.
    pub rows_per_sec: f64,
}

/// Reads a `.tbl` file and returns the rows as a `Vec<Vec<String>>`.
///
/// Each inner `Vec<String>` is one row; fields are split on `|`. The trailing
/// `|` that `dbgen` appends to every row is silently stripped.
pub fn read_tbl(path: &Path) -> Result<Vec<Vec<String>>> {
    let raw = std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    let mut rows = Vec::new();
    for line in raw.lines() {
        if let Some(fields) = parse_tbl_line(line) {
            rows.push(fields);
        }
    }
    Ok(rows)
}

fn parse_tbl_line(line: &str) -> Option<Vec<String>> {
    let line = line.trim_end_matches('|');
    if line.is_empty() {
        return None;
    }
    Some(line.split('|').map(str::to_owned).collect())
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
        inserted as f64 / elapsed
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

    use anyhow::Context;
    use tokio_postgres::NoTls;
    use ultrasql_server::{Server, bind_listener, serve_listener};

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .context("build tokio runtime")?;

    runtime.block_on(async move {
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

        let conn_str = format!(
            "host=127.0.0.1 port={} user=ultrasql_tpch_load",
            bound.port()
        );
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
    })
}

/// Stub returned when the `sql-bench` feature is not active.
#[cfg(not(feature = "sql-bench"))]
pub fn load_ultrasql(_data_dir: &Path) -> Result<Vec<LoadStats>> {
    bail!("NotYetWired: sql-bench feature is not enabled; rebuild with --features sql-bench")
}

/// Loads all TPC-H tables from `data_dir` into an already-connected UltraSQL client.
#[cfg(feature = "sql-bench")]
pub(crate) async fn load_ultrasql_into_client(
    client: &tokio_postgres::Client,
    data_dir: &Path,
) -> Result<Vec<LoadStats>> {
    let mut stats = Vec::with_capacity(data_gen::TABLE_NAMES.len());
    for table in data_gen::TABLE_NAMES {
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

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Returns the number of columns in the TPC-H table with the given name.
///
/// These counts mirror the TPC-H schema constants in [`crate::tpch::schema`].
pub fn column_count(table: &str) -> usize {
    match table {
        "region" => 3,
        "nation" => 4,
        "supplier" => 7,
        "customer" => 8,
        "part" | "orders" => 9,
        "partsupp" => 5,
        "lineitem" => 16,
        _ => 0,
    }
}

#[cfg(any(test, feature = "sql-bench"))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ColumnKind {
    Int,
    Text,
    Decimal,
    Date,
}

#[cfg(any(test, feature = "sql-bench"))]
fn column_kinds(table: &str) -> &'static [ColumnKind] {
    use ColumnKind::{Date, Decimal, Int, Text};

    match table {
        "region" => &[Int, Text, Text],
        "nation" => &[Int, Text, Int, Text],
        "supplier" => &[Int, Text, Text, Int, Text, Decimal, Text],
        "customer" => &[Int, Text, Text, Int, Text, Decimal, Text, Text],
        "part" => &[Int, Text, Text, Text, Text, Int, Text, Decimal, Text],
        "partsupp" => &[Int, Int, Int, Decimal, Text],
        "orders" => &[Int, Int, Text, Decimal, Date, Text, Text, Int, Text],
        "lineitem" => &[
            Int, Int, Int, Int, Decimal, Decimal, Decimal, Decimal, Text, Text, Date, Date, Date,
            Text, Text, Text,
        ],
        _ => &[],
    }
}

#[cfg(any(test, feature = "sql-bench"))]
fn escape_sql_text(text: &str) -> String {
    text.replace('\'', "''")
}

#[cfg(any(test, feature = "sql-bench"))]
fn format_ultrasql_literal(kind: ColumnKind, raw: &str) -> Result<String> {
    match kind {
        ColumnKind::Int => {
            raw.parse::<i64>()
                .with_context(|| format!("parse integer literal `{raw}`"))?;
            Ok(raw.to_owned())
        }
        ColumnKind::Decimal => {
            raw.parse::<f64>()
                .with_context(|| format!("parse decimal literal `{raw}`"))?;
            Ok(raw.to_owned())
        }
        ColumnKind::Date => Ok(format!("DATE '{}'", escape_sql_text(raw))),
        ColumnKind::Text => Ok(format!("'{}'", escape_sql_text(raw))),
    }
}

#[cfg(any(test, feature = "sql-bench"))]
fn build_ultrasql_insert_sql(table: &str, rows: &[Vec<String>]) -> Result<String> {
    let kinds = column_kinds(table);
    if kinds.is_empty() {
        bail!("unknown TPC-H table `{table}`");
    }
    let mut sql = String::new();
    write!(&mut sql, "INSERT INTO {table} VALUES ").expect("write into String");
    for (row_idx, row) in rows.iter().enumerate() {
        if row.len() != kinds.len() {
            bail!(
                "{table}: row {} has {} fields, expected {}",
                row_idx + 1,
                row.len(),
                kinds.len()
            );
        }
        if row_idx > 0 {
            sql.push(',');
        }
        sql.push('(');
        for (col_idx, field) in row.iter().enumerate() {
            if col_idx > 0 {
                sql.push(',');
            }
            sql.push_str(&format_ultrasql_literal(kinds[col_idx], field)?);
        }
        sql.push(')');
    }
    Ok(sql)
}

#[cfg(feature = "sql-bench")]
async fn load_ultrasql_table(
    client: &tokio_postgres::Client,
    table: &str,
    data_dir: &Path,
) -> Result<LoadStats> {
    match ultrasql_load_method()? {
        UltrasqlLoadMethod::Copy => load_ultrasql_table_copy(client, table, data_dir).await,
        UltrasqlLoadMethod::Insert => load_ultrasql_table_insert(client, table, data_dir).await,
    }
}

#[cfg(feature = "sql-bench")]
async fn load_ultrasql_table_copy(
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
    for line in reader.lines() {
        let line = line.with_context(|| format!("read {}", path.display()))?;
        let line = line.trim_end_matches('|');
        if line.is_empty() {
            continue;
        }
        let needed = line.len().saturating_add(1);
        if !buffer.is_empty() && buffer.len().saturating_add(needed) > ULTRASQL_COPY_CHUNK_BYTES {
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

    let elapsed = t0.elapsed().as_secs_f64();
    let rows_per_sec = if elapsed > 0.0 {
        inserted as f64 / elapsed
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
async fn load_ultrasql_table_insert(
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
        inserted as f64 / elapsed
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
async fn insert_ultrasql_chunk(
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
fn is_buffer_pool_exhaustion(error: &tokio_postgres::Error) -> bool {
    error
        .as_db_error()
        .map(|db| db.message().contains("buffer pool exhausted"))
        .unwrap_or_else(|| error.to_string().contains("buffer pool exhausted"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_tbl_strips_trailing_pipe() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("test.tbl");
        std::fs::write(&path, "1|Alice|42|\n2|Bob|7|\n").expect("write");
        let rows = read_tbl(&path).expect("read");
        assert_eq!(rows.len(), 2);
        // Trailing pipe stripped — 3 fields per row.
        assert_eq!(rows[0].len(), 3, "row 0 should have 3 fields");
        assert_eq!(rows[0][0], "1");
        assert_eq!(rows[0][1], "Alice");
        assert_eq!(rows[0][2], "42");
    }

    #[test]
    fn read_tbl_empty_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("empty.tbl");
        std::fs::write(&path, "").expect("write");
        let rows = read_tbl(&path).expect("read empty");
        assert!(rows.is_empty());
    }

    #[test]
    fn column_count_all_tables() {
        assert_eq!(column_count("region"), 3);
        assert_eq!(column_count("nation"), 4);
        assert_eq!(column_count("supplier"), 7);
        assert_eq!(column_count("customer"), 8);
        assert_eq!(column_count("part"), 9);
        assert_eq!(column_count("partsupp"), 5);
        assert_eq!(column_count("orders"), 9);
        assert_eq!(column_count("lineitem"), 16);
        assert_eq!(column_count("unknown"), 0);
    }

    #[test]
    fn ultrasql_insert_sql_formats_typed_literals() {
        let sql = build_ultrasql_insert_sql(
            "orders",
            &[vec![
                "1".to_owned(),
                "2".to_owned(),
                "O".to_owned(),
                "123.45".to_owned(),
                "1994-01-01".to_owned(),
                "5-LOW".to_owned(),
                "Clerk#000000001".to_owned(),
                "0".to_owned(),
                "note's ok".to_owned(),
            ]],
        )
        .expect("build INSERT sql");
        assert!(sql.contains("123.45"), "decimal literal stays numeric");
        assert!(sql.contains("DATE '1994-01-01'"), "date literal is typed");
        assert!(sql.contains("'note''s ok'"), "text is SQL-escaped");
    }
}
