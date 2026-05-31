//! PostgreSQL-wire sysbench OLTP read/write benchmark implementation.

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use serde::Serialize;
use tokio::task::JoinSet;
use tokio_postgres::{Client, NoTls};
use ultrasql_server::{Server, bind_listener, serve_listener};

use crate::{SysbenchArgs, SysbenchEngine, percentile};

const INSERT_ID_STRIDE: i64 = 10_000_000;
const MEASURED_INSERT_BASE: i64 = 1_000_000_000;

pub(crate) fn run_blocking(args: SysbenchArgs) -> Result<()> {
    let worker_threads = sysbench_worker_threads(args.connections);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads)
        .enable_all()
        .build()
        .context("build tokio runtime for sysbench")?;
    runtime.block_on(run(args))
}

async fn run(args: SysbenchArgs) -> Result<()> {
    let worker_threads = sysbench_worker_threads(args.connections);
    let target = Target::start(args.engine, args.dsn.clone(), worker_threads).await?;
    let config = SysbenchConfig::from_args(&args)?;
    let admin = connect(&target.dsn).await?;
    reset_schema(&admin.client, &config).await?;
    admin.shutdown();

    let warmup = if args.warmup_secs > 0 {
        run_phase(
            &target.dsn,
            &config,
            Duration::from_secs(args.warmup_secs),
            Phase::Warmup,
        )
        .await?
    } else {
        PhaseResult::default()
    };
    let measured = run_phase(
        &target.dsn,
        &config,
        Duration::from_secs(args.duration_secs),
        Phase::Measured,
    )
    .await?;

    let checker = connect(&target.dsn).await?;
    let correctness = verify_correctness(&checker.client, &config, &warmup, &measured).await?;
    checker.shutdown();

    let mut latencies = measured.latency_us.clone();
    ultrasql_bench::sort_f64_nan_last(&mut latencies);
    let elapsed_secs = measured.elapsed.as_secs_f64();
    let throughput_per_sec = if elapsed_secs > 0.0 {
        measured.operations as f64 / elapsed_secs
    } else {
        0.0
    };
    let report = SysbenchReport {
        engine: args.engine.label(),
        workload: "sysbench_oltp_read_write",
        rows: config.rows,
        connections: args.connections,
        warmup_secs: args.warmup_secs,
        duration_secs: args.duration_secs,
        operations: measured.operations,
        reads: measured.reads,
        updates: measured.updates,
        inserts: measured.inserts,
        throughput_per_sec,
        p50_latency_us: percentile(&latencies, 0.50),
        p95_latency_us: percentile(&latencies, 0.95),
        p99_latency_us: percentile(&latencies, 0.99),
        correctness,
    };
    write_report(args.output.as_deref(), &report)?;
    Ok(())
}

struct Target {
    dsn: String,
}

impl Target {
    async fn start(
        engine: SysbenchEngine,
        dsn: Option<String>,
        worker_threads: usize,
    ) -> Result<Self> {
        match (engine, dsn) {
            (SysbenchEngine::Postgres17, Some(dsn)) | (SysbenchEngine::Ultrasql, Some(dsn)) => {
                Ok(Self { dsn })
            }
            (SysbenchEngine::Postgres17, None) => {
                anyhow::bail!("--dsn is required when --engine postgres17")
            }
            (SysbenchEngine::Ultrasql, None) => {
                let bind_addr: SocketAddr = "127.0.0.1:0".parse()?;
                let (listener, bound) = bind_listener(bind_addr).await.context("bind ultrasql")?;
                let state = Arc::new(Server::with_sample_database());
                std::thread::Builder::new()
                    .name("ultrasql-sysbench-server".to_string())
                    .spawn(move || {
                        let runtime = match tokio::runtime::Builder::new_multi_thread()
                            .worker_threads(worker_threads)
                            .enable_all()
                            .build()
                        {
                            Ok(runtime) => runtime,
                            Err(err) => {
                                eprintln!("ultrasql server runtime failed: {err}");
                                return;
                            }
                        };
                        runtime.block_on(async move {
                            if let Err(err) = serve_listener(listener, state).await {
                                eprintln!("ultrasql server exited: {err}");
                            }
                        });
                    })
                    .context("spawn ultrasql server thread")?;
                Ok(Self {
                    dsn: format!("host=127.0.0.1 port={} user=ultrasql_bench", bound.port()),
                })
            }
        }
    }
}

fn sysbench_worker_threads(connections: usize) -> usize {
    connections.saturating_add(2).clamp(2, 64)
}

#[derive(Clone)]
struct SysbenchConfig {
    rows: usize,
    connections: usize,
    table: String,
}

impl SysbenchConfig {
    fn from_args(args: &SysbenchArgs) -> Result<Self> {
        if args.rows == 0 {
            anyhow::bail!("--rows must be >= 1");
        }
        if args.connections == 0 {
            anyhow::bail!("--connections must be >= 1");
        }
        if args.duration_secs == 0 {
            anyhow::bail!("--duration must be >= 1");
        }
        Ok(Self {
            rows: args.rows,
            connections: args.connections,
            table: "ultrasql_sysbench_rw".to_string(),
        })
    }
}

struct DbConn {
    client: Client,
    task: tokio::task::JoinHandle<()>,
}

impl DbConn {
    fn shutdown(self) {
        drop(self.client);
        self.task.abort();
    }
}

async fn connect(dsn: &str) -> Result<DbConn> {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls)
        .await
        .with_context(|| format!("connect to {dsn}"))?;
    let task = tokio::spawn(async move {
        if let Err(err) = connection.await {
            eprintln!("sysbench connection task exited: {err}");
        }
    });
    Ok(DbConn { client, task })
}

async fn reset_schema(client: &Client, config: &SysbenchConfig) -> Result<()> {
    let table = &config.table;
    for ddl in [
        format!("DROP TABLE IF EXISTS {table}"),
        format!(
            "CREATE TABLE {table} \
             (id INT NOT NULL, k INT NOT NULL, c TEXT NOT NULL, pad TEXT NOT NULL)"
        ),
    ] {
        client
            .batch_execute(&ddl)
            .await
            .with_context(|| format!("execute sysbench ddl: {ddl}"))?;
    }
    insert_generated(client, table, config.rows).await?;
    client
        .batch_execute(&format!(
            "CREATE UNIQUE INDEX {table}_id_idx ON {table} (id)"
        ))
        .await
        .with_context(|| format!("CREATE UNIQUE INDEX {table}_id_idx"))?;
    Ok(())
}

async fn insert_generated(client: &Client, table: &str, rows: usize) -> Result<()> {
    const CHUNK: usize = 1_000;
    let mut start = 0;
    while start < rows {
        let end = (start + CHUNK).min(rows);
        let values = (start..end)
            .map(|ix| {
                let k = ix.wrapping_mul(17) % 1_000_000;
                format!("({ix}, {k}, 'c{ix}', 'pad{ix}')")
            })
            .collect::<Vec<_>>()
            .join(",");
        client
            .batch_execute(&format!(
                "INSERT INTO {table} (id, k, c, pad) VALUES {values}"
            ))
            .await
            .with_context(|| format!("preload {table} rows {start}..{end}"))?;
        start = end;
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum Phase {
    Warmup,
    Measured,
}

impl Phase {
    const fn seed_tag(self) -> u64 {
        match self {
            Self::Warmup => 0xA11C_E000_0000_0000,
            Self::Measured => 0xA11C_E111_0000_0000,
        }
    }

    const fn insert_base(self) -> i64 {
        match self {
            Self::Warmup => INSERT_ID_STRIDE,
            Self::Measured => MEASURED_INSERT_BASE,
        }
    }
}

#[derive(Default)]
struct PhaseResult {
    operations: u64,
    reads: u64,
    updates: u64,
    inserts: u64,
    latency_us: Vec<f64>,
    elapsed: Duration,
}

async fn run_phase(
    dsn: &str,
    config: &SysbenchConfig,
    duration: Duration,
    phase: Phase,
) -> Result<PhaseResult> {
    let started = Instant::now();
    let deadline = started + duration;
    let mut tasks = JoinSet::new();
    let shared_dsn = Arc::new(dsn.to_string());
    let shared_config = Arc::new(config.clone());
    for client_id in 0..config.connections {
        let dsn = Arc::clone(&shared_dsn);
        let config = Arc::clone(&shared_config);
        tasks.spawn(async move { run_client(&dsn, &config, client_id, deadline, phase).await });
    }

    let mut result = PhaseResult::default();
    while let Some(joined) = tasks.join_next().await {
        let client_result = joined.context("join sysbench client task")??;
        result.operations = result.operations.saturating_add(client_result.operations);
        result.reads = result.reads.saturating_add(client_result.reads);
        result.updates = result.updates.saturating_add(client_result.updates);
        result.inserts = result.inserts.saturating_add(client_result.inserts);
        result.latency_us.extend(client_result.latency_us);
    }
    result.elapsed = started.elapsed();
    Ok(result)
}

struct ClientResult {
    operations: u64,
    reads: u64,
    updates: u64,
    inserts: u64,
    latency_us: Vec<f64>,
}

async fn run_client(
    dsn: &str,
    config: &SysbenchConfig,
    client_id: usize,
    deadline: Instant,
    phase: Phase,
) -> Result<ClientResult> {
    let conn = connect(dsn).await?;
    let mut rng = SplitMix64::new(phase.seed_tag() ^ u64::try_from(client_id).unwrap_or(0));
    let mut next_insert_id = next_insert_base(config, client_id, phase)?;
    let mut result = ClientResult {
        operations: 0,
        reads: 0,
        updates: 0,
        inserts: 0,
        latency_us: Vec::new(),
    };
    while Instant::now() < deadline {
        let op = next_op(config, &mut rng, &mut next_insert_id)?;
        let before = Instant::now();
        execute_op(&conn.client, &op).await.with_context(|| {
            format!(
                "sysbench client {client_id} operation {}",
                result.operations.saturating_add(1)
            )
        })?;
        result
            .latency_us
            .push(before.elapsed().as_secs_f64() * 1_000_000.0);
        result.operations = result.operations.saturating_add(1);
        match op.kind {
            OpKind::Read => result.reads = result.reads.saturating_add(1),
            OpKind::Update => result.updates = result.updates.saturating_add(1),
            OpKind::Insert => result.inserts = result.inserts.saturating_add(1),
        }
    }
    conn.shutdown();
    Ok(result)
}

fn next_insert_base(config: &SysbenchConfig, client_id: usize, phase: Phase) -> Result<i64> {
    let rows = i64::try_from(config.rows).context("rows conversion")?;
    let client = i64::try_from(client_id).context("client id conversion")?;
    phase
        .insert_base()
        .checked_add(rows)
        .and_then(|base| base.checked_add(client.saturating_mul(INSERT_ID_STRIDE)))
        .context("insert id base overflow")
}

#[derive(Clone, Copy)]
enum OpKind {
    Read,
    Update,
    Insert,
}

struct SysbenchOp {
    kind: OpKind,
    sql: String,
}

fn next_op(
    config: &SysbenchConfig,
    rng: &mut SplitMix64,
    next_insert_id: &mut i64,
) -> Result<SysbenchOp> {
    let rows = u64::try_from(config.rows).context("rows conversion")?;
    let choice = rng.next_u64() % 100;
    let table = &config.table;
    if choice < 50 {
        let row_id = rng.next_u64() % rows;
        Ok(SysbenchOp {
            kind: OpKind::Read,
            sql: format!("SELECT k FROM {table} WHERE id = {row_id}"),
        })
    } else if choice < 80 {
        let row_id = rng.next_u64() % rows;
        Ok(SysbenchOp {
            kind: OpKind::Update,
            sql: format!("UPDATE {table} SET k = k + 1 WHERE id = {row_id}"),
        })
    } else {
        let id = *next_insert_id;
        *next_insert_id = next_insert_id
            .checked_add(1)
            .context("insert id overflow")?;
        let value = rng.next_i32();
        Ok(SysbenchOp {
            kind: OpKind::Insert,
            sql: format!(
                "INSERT INTO {table} (id, k, c, pad) VALUES ({id}, {value}, 'c{id}', 'pad{id}')"
            ),
        })
    }
}

async fn execute_op(client: &Client, op: &SysbenchOp) -> Result<()> {
    const MAX_RETRIES: usize = 16_384;
    for attempt in 0..MAX_RETRIES {
        match client.simple_query(&op.sql).await {
            Ok(messages) => {
                if matches!(op.kind, OpKind::Read) {
                    let rows = row_count(&messages);
                    if rows != 1 {
                        anyhow::bail!("sysbench point read returned {rows} rows for `{}`", op.sql);
                    }
                }
                return Ok(());
            }
            Err(err) if is_retryable_query_error(&err) && attempt + 1 < MAX_RETRIES => {
                let _ = client.batch_execute("ROLLBACK").await;
                if attempt < 128 {
                    tokio::task::yield_now().await;
                } else {
                    let backoff_us = 100_u64
                        + u64::try_from(attempt.saturating_sub(128))
                            .unwrap_or(4_900)
                            .saturating_mul(10)
                            .min(4_900);
                    tokio::time::sleep(Duration::from_micros(backoff_us)).await;
                }
            }
            Err(err) => {
                return Err(err)
                    .with_context(|| format!("execute sysbench operation `{}`", op.sql));
            }
        }
    }
    unreachable!("bounded retry loop either returns or errors")
}

fn row_count(messages: &[tokio_postgres::SimpleQueryMessage]) -> usize {
    messages
        .iter()
        .filter(|msg| matches!(msg, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count()
}

fn is_retryable_conflict_text(text: &str) -> bool {
    text.contains("update on deleted tuple")
        || text.contains("write conflict")
        || text.contains("row lock not available")
        || text.contains("serialization")
        || text.contains("could not serialize")
}

fn is_retryable_query_error(err: &tokio_postgres::Error) -> bool {
    if is_retryable_conflict_text(&err.to_string()) {
        return true;
    }
    let Some(db_error) = err.as_db_error() else {
        return false;
    };
    is_retryable_conflict_text(db_error.message())
        || db_error.detail().is_some_and(is_retryable_conflict_text)
        || db_error.hint().is_some_and(is_retryable_conflict_text)
}

#[derive(Serialize)]
struct CorrectnessReport {
    passed: bool,
    expected_row_count: i64,
    actual_row_count: i64,
    warmup_inserts: u64,
    measured_inserts: u64,
}

async fn verify_correctness(
    client: &Client,
    config: &SysbenchConfig,
    warmup: &PhaseResult,
    measured: &PhaseResult,
) -> Result<CorrectnessReport> {
    let actual_row_count = query_i64(client, &format!("SELECT COUNT(*) FROM {}", config.table))
        .await
        .context("sysbench final row count")?;
    let initial_rows = i64::try_from(config.rows).context("rows conversion")?;
    let warmup_inserts = i64::try_from(warmup.inserts).context("warmup inserts conversion")?;
    let measured_inserts =
        i64::try_from(measured.inserts).context("measured inserts conversion")?;
    let expected_row_count = initial_rows
        .checked_add(warmup_inserts)
        .and_then(|value| value.checked_add(measured_inserts))
        .context("expected row count overflow")?;
    Ok(CorrectnessReport {
        passed: actual_row_count == expected_row_count
            && measured.operations > 0
            && measured.reads > 0
            && measured.updates > 0
            && measured.inserts > 0,
        expected_row_count,
        actual_row_count,
        warmup_inserts: warmup.inserts,
        measured_inserts: measured.inserts,
    })
}

async fn query_i64(client: &Client, sql: &str) -> Result<i64> {
    let row = client
        .query_one(sql, &[])
        .await
        .with_context(|| format!("query integer: {sql}"))?;
    Ok(row.try_get::<_, Option<i64>>(0)?.unwrap_or(0))
}

#[derive(Serialize)]
struct SysbenchReport {
    engine: &'static str,
    workload: &'static str,
    rows: usize,
    connections: usize,
    warmup_secs: u64,
    duration_secs: u64,
    operations: u64,
    reads: u64,
    updates: u64,
    inserts: u64,
    throughput_per_sec: f64,
    p50_latency_us: f64,
    p95_latency_us: f64,
    p99_latency_us: f64,
    correctness: CorrectnessReport,
}

fn write_report(path: Option<&Path>, report: &SysbenchReport) -> Result<()> {
    let serialized = serde_json::to_string_pretty(report)?;
    if let Some(path) = path {
        std::fs::write(path, format!("{serialized}\n"))
            .with_context(|| format!("write {}", path.display()))?;
        eprintln!("sysbench: wrote {}", path.display());
    } else {
        println!("{serialized}");
    }
    Ok(())
}

impl SysbenchEngine {
    const fn label(self) -> &'static str {
        match self {
            Self::Ultrasql => "ultrasql",
            Self::Postgres17 => "postgres17",
        }
    }
}

struct SplitMix64(u64);

impl SplitMix64 {
    const fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn next_i32(&mut self) -> i32 {
        self.next_u64() as i32
    }
}

#[cfg(test)]
mod tests {
    use super::{Phase, SysbenchConfig, is_retryable_conflict_text, next_insert_base, next_op};
    use crate::SysbenchArgs;
    use crate::SysbenchEngine;

    #[test]
    fn retryable_conflicts_cover_serialization_and_ultrasql_lock_errors() {
        assert!(is_retryable_conflict_text("could not serialize access"));
        assert!(is_retryable_conflict_text("row lock not available"));
        assert!(is_retryable_conflict_text(
            "malformed tuple header: update on deleted tuple"
        ));
        assert!(!is_retryable_conflict_text("syntax error"));
    }

    #[test]
    fn insert_id_ranges_do_not_overlap_between_phases_or_clients() {
        let args = SysbenchArgs {
            engine: SysbenchEngine::Ultrasql,
            dsn: None,
            rows: 10_000,
            warmup_secs: 1,
            duration_secs: 1,
            connections: 2,
            output: None,
        };
        let config = SysbenchConfig::from_args(&args).expect("config");

        let warmup_client0 = next_insert_base(&config, 0, Phase::Warmup).expect("warmup c0");
        let warmup_client1 = next_insert_base(&config, 1, Phase::Warmup).expect("warmup c1");
        let measured_client0 = next_insert_base(&config, 0, Phase::Measured).expect("measured c0");

        assert!(warmup_client1 - warmup_client0 >= super::INSERT_ID_STRIDE);
        assert!(measured_client0 - warmup_client0 >= super::INSERT_ID_STRIDE);
        assert!(measured_client0 < i64::from(i32::MAX));
    }

    #[test]
    fn op_mix_generates_all_sysbench_read_write_families() {
        let args = SysbenchArgs {
            engine: SysbenchEngine::Ultrasql,
            dsn: None,
            rows: 1_000,
            warmup_secs: 1,
            duration_secs: 1,
            connections: 1,
            output: None,
        };
        let config = SysbenchConfig::from_args(&args).expect("config");
        let mut rng = super::SplitMix64::new(0x51_7e_c0_de);
        let mut next_insert = next_insert_base(&config, 0, Phase::Measured).expect("base");
        let mut reads = 0;
        let mut updates = 0;
        let mut inserts = 0;
        for _ in 0..200 {
            let op = next_op(&config, &mut rng, &mut next_insert).expect("op");
            if op.sql.starts_with("SELECT") {
                reads += 1;
            } else if op.sql.starts_with("UPDATE") {
                updates += 1;
            } else if op.sql.starts_with("INSERT") {
                inserts += 1;
            }
        }

        assert!(reads > 0);
        assert!(updates > 0);
        assert!(inserts > 0);
    }
}
