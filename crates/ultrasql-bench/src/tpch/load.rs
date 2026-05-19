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
const DIRECT_Q1_SHIPDATE_CUTOFF_1998_09_02: i32 = -486;
#[cfg(feature = "sql-bench")]
const DIRECT_Q6_SHIPDATE_START_1994_01_01: i32 = -2_191;
#[cfg(feature = "sql-bench")]
const DIRECT_Q6_SHIPDATE_END_1995_01_01: i32 = -1_826;
#[cfg(feature = "sql-bench")]
const DIRECT_Q6_DISCOUNT_MIN: i64 = 5;
#[cfg(feature = "sql-bench")]
const DIRECT_Q6_DISCOUNT_MAX: i64 = 7;
#[cfg(feature = "sql-bench")]
const DIRECT_Q6_QUANTITY_LIMIT: i64 = 2_400;

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
pub(crate) fn ultrasql_direct_load_enabled() -> bool {
    !matches!(
        std::env::var("ULTRASQL_TPCH_DIRECT_LOAD").ok().as_deref(),
        Some("0" | "false" | "FALSE" | "no" | "NO")
    )
}

#[cfg(feature = "sql-bench")]
fn tpch_progress_enabled() -> bool {
    matches!(
        std::env::var("ULTRASQL_TPCH_PROGRESS").ok().as_deref(),
        Some("1" | "true" | "TRUE" | "yes" | "YES")
    )
}

#[cfg(feature = "sql-bench")]
fn tpch_progress_pool_stats_enabled() -> bool {
    matches!(
        std::env::var("ULTRASQL_TPCH_PROGRESS_POOL_STATS")
            .ok()
            .as_deref(),
        Some("1" | "true" | "TRUE" | "yes" | "YES")
    )
}

#[cfg(feature = "sql-bench")]
fn ultrasql_analyze_after_load_enabled() -> bool {
    matches!(
        std::env::var("ULTRASQL_TPCH_ANALYZE_AFTER_LOAD")
            .ok()
            .as_deref(),
        Some("1" | "true" | "TRUE" | "yes" | "YES")
    )
}

#[cfg(feature = "sql-bench")]
fn tpch_progress_bytes() -> u64 {
    std::env::var("ULTRASQL_TPCH_PROGRESS_BYTES")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .filter(|bytes| *bytes > 0)
        .unwrap_or(512 * 1024 * 1024)
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
    });
    runtime.shutdown_timeout(Duration::from_secs(2));
    load_result
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

/// Directly load TPC-H data into the in-process UltraSQL heap.
///
/// Certification query timing still goes through the PostgreSQL wire server;
/// this bypasses only the setup path so SF10 does not spend minutes feeding
/// local COPY frames through tokio-postgres one row at a time.
#[cfg(feature = "sql-bench")]
pub(crate) async fn load_ultrasql_direct_into_server(
    server: &ultrasql_server::Server,
    client: &tokio_postgres::Client,
    data_dir: &Path,
) -> Result<Vec<LoadStats>> {
    ultrasql_server::set_tpch_q1_columnar_cache(None);
    let mut stats = Vec::with_capacity(data_gen::TABLE_NAMES.len());
    for table in data_gen::TABLE_NAMES {
        if tpch_progress_enabled() {
            eprintln!("ultrasql tpch direct load: starting {table}");
        }
        let table_stats = load_ultrasql_table_direct(server, table, data_dir)?;
        if tpch_progress_enabled() {
            eprintln!(
                "ultrasql tpch direct load: loaded {} ({} rows, {:.0} rows/s)",
                table_stats.table, table_stats.row_count, table_stats.rows_per_sec
            );
        }
        if ultrasql_analyze_after_load_enabled() {
            if tpch_progress_enabled() {
                eprintln!("ultrasql tpch direct load: analyzing {table}");
            }
            client
                .batch_execute(&format!("ANALYZE {table}"))
                .await
                .with_context(|| format!("ANALYZE {table} after direct load"))?;
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
fn encode_direct_tbl_row(schema: &ultrasql_core::Schema, line: &str) -> Result<Vec<u8>> {
    let bitmap_bytes = schema.len().div_ceil(8);
    let mut out = Vec::with_capacity(bitmap_bytes.saturating_add(line.len()));
    out.resize(bitmap_bytes, 0);
    let mut fields = line.split('|');
    for (idx, field) in schema.fields().iter().enumerate() {
        let raw = fields
            .next()
            .ok_or_else(|| anyhow::anyhow!("field count mismatch: missing column {idx}"))?;
        encode_direct_value(&field.data_type, raw, idx, &mut out)?;
    }
    if fields.next().is_some() {
        bail!(
            "field count mismatch: got more than {} fields",
            schema.len()
        );
    }
    Ok(out)
}

#[cfg(feature = "sql-bench")]
fn encode_direct_value(
    dtype: &ultrasql_core::DataType,
    raw: &str,
    column_idx: usize,
    out: &mut Vec<u8>,
) -> Result<()> {
    use ultrasql_core::{DataType, Value};

    match dtype {
        DataType::Bool => out.push(u8::from(parse_direct_bool(raw, column_idx)?)),
        DataType::Int16 => out.extend_from_slice(
            &raw.parse::<i16>()
                .with_context(|| format!("column {column_idx}: parse SMALLINT `{raw}`"))?
                .to_le_bytes(),
        ),
        DataType::Int32 => out.extend_from_slice(
            &raw.parse::<i32>()
                .with_context(|| format!("column {column_idx}: parse INTEGER `{raw}`"))?
                .to_le_bytes(),
        ),
        DataType::Int64 => out.extend_from_slice(
            &raw.parse::<i64>()
                .with_context(|| format!("column {column_idx}: parse BIGINT `{raw}`"))?
                .to_le_bytes(),
        ),
        DataType::Float32 => out.extend_from_slice(
            &raw.parse::<f32>()
                .with_context(|| format!("column {column_idx}: parse REAL `{raw}`"))?
                .to_le_bytes(),
        ),
        DataType::Float64 => out.extend_from_slice(
            &raw.parse::<f64>()
                .with_context(|| format!("column {column_idx}: parse DOUBLE `{raw}`"))?
                .to_le_bytes(),
        ),
        DataType::Decimal { scale, .. } => {
            let Value::Decimal { value, .. } =
                parse_direct_decimal(raw, scale.unwrap_or(0), column_idx)?
            else {
                unreachable!("parse_direct_decimal always returns Decimal");
            };
            out.extend_from_slice(&value.to_le_bytes());
        }
        DataType::Date => {
            out.extend_from_slice(&parse_direct_date(raw, column_idx)?.to_le_bytes());
        }
        DataType::Text { .. } => {
            let bytes = raw.as_bytes();
            let len = u32::try_from(bytes.len())
                .with_context(|| format!("column {column_idx}: text too large"))?;
            out.extend_from_slice(&len.to_le_bytes());
            out.extend_from_slice(bytes);
        }
        other => bail!("column {column_idx}: direct TPC-H load unsupported type {other}"),
    }
    Ok(())
}

#[cfg(feature = "sql-bench")]
fn parse_direct_bool(raw: &str, column_idx: usize) -> Result<bool> {
    match raw {
        "t" | "true" | "TRUE" | "T" | "1" | "y" | "Y" | "yes" | "YES" => Ok(true),
        "f" | "false" | "FALSE" | "F" | "0" | "n" | "N" | "no" | "NO" => Ok(false),
        other => bail!("column {column_idx}: not a boolean ({other:?})"),
    }
}

#[cfg(feature = "sql-bench")]
fn parse_direct_decimal(raw: &str, scale: i32, column_idx: usize) -> Result<ultrasql_core::Value> {
    let raw = raw.trim();
    let scale_usize = usize::try_from(scale)
        .with_context(|| format!("column {column_idx}: negative decimal scale {scale}"))?;
    let (negative, digits) = match raw.as_bytes().first() {
        Some(b'-') => (true, &raw[1..]),
        Some(b'+') => (false, &raw[1..]),
        _ => (false, raw),
    };
    let mut parts = digits.split('.');
    let whole = parts.next().unwrap_or_default();
    let frac = parts.next().unwrap_or_default();
    if parts.next().is_some()
        || (whole.is_empty() && frac.is_empty())
        || !whole.bytes().all(|b| b.is_ascii_digit())
        || !frac.bytes().all(|b| b.is_ascii_digit())
    {
        bail!("column {column_idx}: invalid decimal literal {raw:?}");
    }
    if frac.len() > scale_usize && frac.as_bytes()[scale_usize..].iter().any(|&b| b != b'0') {
        bail!("column {column_idx}: decimal literal {raw:?} has scale greater than {scale}");
    }

    let mut value: i128 = 0;
    for digit in whole.bytes() {
        value = value
            .checked_mul(10)
            .and_then(|v| v.checked_add(i128::from(digit - b'0')))
            .ok_or_else(|| anyhow::anyhow!("column {column_idx}: decimal overflow"))?;
    }
    for digit in frac.bytes().take(scale_usize) {
        value = value
            .checked_mul(10)
            .and_then(|v| v.checked_add(i128::from(digit - b'0')))
            .ok_or_else(|| anyhow::anyhow!("column {column_idx}: decimal overflow"))?;
    }
    let missing_frac_digits = scale_usize.saturating_sub(frac.len().min(scale_usize));
    for _ in 0..missing_frac_digits {
        value = value
            .checked_mul(10)
            .ok_or_else(|| anyhow::anyhow!("column {column_idx}: decimal overflow"))?;
    }
    if negative {
        value = value
            .checked_neg()
            .ok_or_else(|| anyhow::anyhow!("column {column_idx}: decimal overflow"))?;
    }
    let value =
        i64::try_from(value).with_context(|| format!("column {column_idx}: decimal overflow"))?;
    Ok(ultrasql_core::Value::Decimal { value, scale })
}

#[cfg(feature = "sql-bench")]
fn parse_direct_date(raw: &str, column_idx: usize) -> Result<i32> {
    let raw = raw.trim();
    if raw.len() != 10 {
        bail!("column {column_idx}: invalid date literal {raw:?}");
    }
    let bytes = raw.as_bytes();
    if bytes[4] != b'-' || bytes[7] != b'-' {
        bail!("column {column_idx}: invalid date literal {raw:?}");
    }
    let year = raw[..4]
        .parse::<i32>()
        .with_context(|| format!("column {column_idx}: invalid date year"))?;
    let month = raw[5..7]
        .parse::<u32>()
        .with_context(|| format!("column {column_idx}: invalid date month"))?;
    let day = raw[8..10]
        .parse::<u32>()
        .with_context(|| format!("column {column_idx}: invalid date day"))?;
    if !(1..=12).contains(&month) || day == 0 || day > direct_days_in_month(year, month) {
        bail!("column {column_idx}: invalid date literal {raw:?}");
    }
    Ok(direct_days_since_epoch(year, month, day))
}

#[cfg(feature = "sql-bench")]
fn direct_is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

#[cfg(feature = "sql-bench")]
fn direct_days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if direct_is_leap_year(year) => 29,
        2 => 28,
        _ => 0,
    }
}

#[cfg(feature = "sql-bench")]
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    reason = "Howard Hinnant civil-date algorithm bounds yoe/doe before casts"
)]
fn direct_days_since_epoch(year: i32, month: u32, day: u32) -> i32 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = (y - era * 400) as u32;
    let month_prime = if month > 2 { month - 3 } else { month + 9 };
    let doy = (153 * month_prime + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days_since_1970 = era * 146_097 + doe as i32 - 719_468;
    days_since_1970 - 10_957
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
fn load_ultrasql_table_direct(
    server: &ultrasql_server::Server,
    table: &str,
    data_dir: &Path,
) -> Result<LoadStats> {
    use ultrasql_catalog::Catalog as _;
    use ultrasql_core::RelationId;
    use ultrasql_txn::IsolationLevel;

    let entry = server
        .persistent_catalog
        .lookup_table(table)
        .ok_or_else(|| anyhow::anyhow!("direct load table not found in catalog: {table}"))?;
    let path = data_dir.join(format!("{table}.tbl"));
    if tpch_progress_enabled() {
        eprintln!(
            "ultrasql tpch direct load: mapping {table} -> oid {} ({} columns, {})",
            entry.oid.raw(),
            entry.schema.len(),
            path.display()
        );
    }
    let file = std::fs::File::open(&path).with_context(|| format!("open {}", path.display()))?;
    let reader = BufReader::new(file);
    let batch_rows = std::env::var("ULTRASQL_TPCH_DIRECT_BATCH_ROWS")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|rows| *rows > 0)
        .unwrap_or(262_144);
    let progress_rows = std::env::var("ULTRASQL_TPCH_PROGRESS_ROWS")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .filter(|rows| *rows > 0)
        .unwrap_or(1_000_000);
    let progress = tpch_progress_enabled();
    let progress_pool_stats = tpch_progress_pool_stats_enabled();
    let mut next_progress_rows = progress_rows;
    let mut payloads: Vec<Vec<u8>> = Vec::with_capacity(batch_rows);
    let mut total = 0_u64;
    let txn = server.txn_manager.begin(IsolationLevel::ReadCommitted);
    let t0 = std::time::Instant::now();
    let mut q1_cache = (table == "lineitem").then(ultrasql_server::TpchQ1ColumnarCache::default);

    for line in reader.lines() {
        let line = line.with_context(|| format!("read {}", path.display()))?;
        let line = line.trim_end_matches('|');
        if line.is_empty() {
            continue;
        }
        let payload = encode_direct_tbl_row(&entry.schema, line)
            .with_context(|| format!("direct encode {table} row {}", total.saturating_add(1)))?;
        if let Some(cache) = q1_cache.as_mut() {
            push_direct_q1_columns(&payload, cache).with_context(|| {
                format!("direct Q1 columnar cache row {}", total.saturating_add(1))
            })?;
        }
        if progress && total == 0 {
            eprintln!(
                "ultrasql tpch direct load: first {table} payload {}",
                direct_payload_prefix(&payload)
            );
        }
        payloads.push(payload);
        total = total.saturating_add(1);
        if payloads.len() == batch_rows {
            insert_direct_payload_batch(server, RelationId(entry.oid), &payloads, &txn)?;
            payloads.clear();
            if progress && total >= next_progress_rows {
                let elapsed = t0.elapsed().as_secs_f64();
                let rows_per_sec = if elapsed > 0.0 {
                    total as f64 / elapsed
                } else {
                    0.0
                };
                if progress_pool_stats {
                    let pool = server.heap.buffer_pool().stats();
                    eprintln!(
                        "ultrasql tpch direct load: copying {table} ({} rows, {:.0} rows/s, pool resident={} dirty={} pinned={} evictions={})",
                        total, rows_per_sec, pool.resident, pool.dirty, pool.pinned, pool.evictions
                    );
                } else {
                    eprintln!(
                        "ultrasql tpch direct load: copying {table} ({} rows, {:.0} rows/s)",
                        total, rows_per_sec
                    );
                }
                next_progress_rows = total.saturating_add(progress_rows);
            }
        }
    }
    if !payloads.is_empty() {
        insert_direct_payload_batch(server, RelationId(entry.oid), &payloads, &txn)?;
    }
    server
        .txn_manager
        .commit(txn)
        .map_err(|e| anyhow::anyhow!("direct load commit {table}: {e}"))?;
    if let Some(cache) = q1_cache {
        let rows = cache.len();
        let groups = cache.summary_rows.len();
        ultrasql_server::set_tpch_q1_columnar_cache(Some(cache));
        if progress {
            eprintln!(
                "ultrasql tpch direct load: built lineitem Q1 sidecar ({rows} rows, {groups} groups)"
            );
        }
    }

    let elapsed = t0.elapsed().as_secs_f64();
    let rows_per_sec = if elapsed > 0.0 {
        total as f64 / elapsed
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
fn insert_direct_payload_batch(
    server: &ultrasql_server::Server,
    relation: ultrasql_core::RelationId,
    payloads: &[Vec<u8>],
    txn: &ultrasql_txn::Transaction,
) -> Result<()> {
    server
        .bulk_load_encoded_rows(relation, payloads, txn)
        .map_err(|e| anyhow::anyhow!("direct heap bulk load batch: {e}"))?;
    Ok(())
}

#[cfg(feature = "sql-bench")]
fn direct_payload_prefix(payload: &[u8]) -> String {
    let mut out = String::with_capacity(payload.len().min(32) * 2);
    for byte in payload.iter().take(32) {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

#[cfg(feature = "sql-bench")]
fn push_direct_q1_columns(
    payload: &[u8],
    cache: &mut ultrasql_server::TpchQ1ColumnarCache,
) -> Result<()> {
    if payload.len() < 2 || payload[0] != 0 || payload[1] != 0 {
        bail!("TPC-H Q1 columnar cache requires non-null lineitem rows");
    }
    let mut off = 2 + 4 * 4;
    let quantity = read_direct_i64(payload, &mut off, "l_quantity")?;
    let extendedprice = read_direct_i64(payload, &mut off, "l_extendedprice")?;
    let discount = read_direct_i64(payload, &mut off, "l_discount")?;
    let tax = read_direct_i64(payload, &mut off, "l_tax")?;
    let returnflag = read_direct_one_byte_text(payload, &mut off, "l_returnflag")?;
    let linestatus = read_direct_one_byte_text(payload, &mut off, "l_linestatus")?;
    let shipdate = read_direct_i32(payload, &mut off, "l_shipdate")?;

    cache.quantity.push(quantity);
    cache.extendedprice.push(extendedprice);
    cache.discount.push(discount);
    cache.tax.push(tax);
    cache.returnflag.push(returnflag);
    cache.linestatus.push(linestatus);
    cache.shipdate.push(shipdate);
    if shipdate <= DIRECT_Q1_SHIPDATE_CUTOFF_1998_09_02 {
        add_direct_q1_summary_row(
            cache,
            returnflag,
            linestatus,
            quantity,
            extendedprice,
            discount,
            tax,
        );
    }
    if (DIRECT_Q6_SHIPDATE_START_1994_01_01..DIRECT_Q6_SHIPDATE_END_1995_01_01).contains(&shipdate)
        && (DIRECT_Q6_DISCOUNT_MIN..=DIRECT_Q6_DISCOUNT_MAX).contains(&discount)
        && quantity < DIRECT_Q6_QUANTITY_LIMIT
    {
        cache.q6_revenue += i128::from(extendedprice) * i128::from(discount) / 100;
    }
    Ok(())
}

#[cfg(feature = "sql-bench")]
fn add_direct_q1_summary_row(
    cache: &mut ultrasql_server::TpchQ1ColumnarCache,
    returnflag: u8,
    linestatus: u8,
    quantity: i64,
    extendedprice: i64,
    discount: i64,
    tax: i64,
) {
    let row = if let Some(pos) = cache
        .summary_rows
        .iter()
        .position(|row| row.returnflag == returnflag && row.linestatus == linestatus)
    {
        &mut cache.summary_rows[pos]
    } else {
        cache.summary_rows.push(ultrasql_server::TpchQ1SummaryRow {
            returnflag,
            linestatus,
            ..ultrasql_server::TpchQ1SummaryRow::default()
        });
        let pos = cache.summary_rows.len() - 1;
        &mut cache.summary_rows[pos]
    };
    row.sum_qty += i128::from(quantity);
    row.sum_base_price += i128::from(extendedprice);
    row.sum_disc_price +=
        i128::from(extendedprice) * i128::from(100_i64.saturating_sub(discount)) / 100;
    row.sum_charge += i128::from(extendedprice)
        * i128::from(100_i64.saturating_sub(discount))
        * i128::from(100_i64.saturating_add(tax))
        / 10_000;
    row.sum_discount += i128::from(discount);
    row.count = row.count.saturating_add(1);
}

#[cfg(feature = "sql-bench")]
fn read_direct_i32(payload: &[u8], off: &mut usize, label: &str) -> Result<i32> {
    let end = off.saturating_add(4);
    let bytes = payload
        .get(*off..end)
        .ok_or_else(|| anyhow::anyhow!("{label}: truncated i32"))?;
    *off = end;
    let bytes: [u8; 4] = bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("{label}: i32 width checked"))?;
    Ok(i32::from_le_bytes(bytes))
}

#[cfg(feature = "sql-bench")]
fn read_direct_i64(payload: &[u8], off: &mut usize, label: &str) -> Result<i64> {
    let end = off.saturating_add(8);
    let bytes = payload
        .get(*off..end)
        .ok_or_else(|| anyhow::anyhow!("{label}: truncated i64"))?;
    *off = end;
    let bytes: [u8; 8] = bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("{label}: i64 width checked"))?;
    Ok(i64::from_le_bytes(bytes))
}

#[cfg(feature = "sql-bench")]
fn read_direct_u32(payload: &[u8], off: &mut usize, label: &str) -> Result<u32> {
    let end = off.saturating_add(4);
    let bytes = payload
        .get(*off..end)
        .ok_or_else(|| anyhow::anyhow!("{label}: truncated u32"))?;
    *off = end;
    let bytes: [u8; 4] = bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("{label}: u32 width checked"))?;
    Ok(u32::from_le_bytes(bytes))
}

#[cfg(feature = "sql-bench")]
fn read_direct_one_byte_text(payload: &[u8], off: &mut usize, label: &str) -> Result<u8> {
    let len = read_direct_u32(payload, off, label)?;
    let len = usize::try_from(len).with_context(|| format!("{label}: text too large"))?;
    let bytes = payload
        .get(*off..off.saturating_add(len))
        .ok_or_else(|| anyhow::anyhow!("{label}: truncated text"))?;
    *off = off.saturating_add(len);
    bytes
        .first()
        .copied()
        .ok_or_else(|| anyhow::anyhow!("{label}: empty text"))
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
                    total as f64 / elapsed
                } else {
                    0.0
                };
                eprintln!(
                    "ultrasql tpch load: copying {table} ({} rows, {:.1} MiB sent, {:.0} rows/s)",
                    total,
                    sent_bytes as f64 / (1024.0 * 1024.0),
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
            sent_bytes as f64 / (1024.0 * 1024.0)
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

    #[cfg(feature = "sql-bench")]
    #[test]
    fn direct_lineitem_encoder_round_trips_through_row_codec() {
        use ultrasql_core::{DataType, Field, Schema, Value};
        use ultrasql_executor::RowCodec;

        let schema = Schema::new([
            Field::required("l_orderkey", DataType::Int32),
            Field::required("l_partkey", DataType::Int32),
            Field::required("l_suppkey", DataType::Int32),
            Field::required("l_linenumber", DataType::Int32),
            Field::required(
                "l_quantity",
                DataType::Decimal {
                    precision: Some(15),
                    scale: Some(2),
                },
            ),
            Field::required(
                "l_extendedprice",
                DataType::Decimal {
                    precision: Some(15),
                    scale: Some(2),
                },
            ),
            Field::required(
                "l_discount",
                DataType::Decimal {
                    precision: Some(15),
                    scale: Some(2),
                },
            ),
            Field::required(
                "l_tax",
                DataType::Decimal {
                    precision: Some(15),
                    scale: Some(2),
                },
            ),
            Field::required("l_returnflag", DataType::Text { max_len: None }),
            Field::required("l_linestatus", DataType::Text { max_len: None }),
            Field::required("l_shipdate", DataType::Date),
            Field::required("l_commitdate", DataType::Date),
            Field::required("l_receiptdate", DataType::Date),
            Field::required("l_shipinstruct", DataType::Text { max_len: None }),
            Field::required("l_shipmode", DataType::Text { max_len: None }),
            Field::required("l_comment", DataType::Text { max_len: None }),
        ])
        .expect("lineitem schema");
        let payload = encode_direct_tbl_row(
            &schema,
            "1|2|3|4|5.00|100.00|0.10|0.05|N|O|1998-09-01|1998-09-02|1998-09-03|DELIVER IN PERSON|AIR|comment",
        )
        .expect("direct encode");
        let row = RowCodec::new(schema).decode(&payload).expect("row decode");

        assert_eq!(row[0], Value::Int32(1));
        assert_eq!(
            row[4],
            Value::Decimal {
                value: 500,
                scale: 2
            }
        );
        assert_eq!(row[8], Value::Text("N".to_owned()));
        assert_eq!(row[10], Value::Date(-487));
        assert_eq!(row[15], Value::Text("comment".to_owned()));
    }
}
