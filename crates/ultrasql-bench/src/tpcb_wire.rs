//! PostgreSQL-wire TPC-B benchmark implementation.

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use serde::Serialize;
use tokio::task::JoinSet;
use tokio_postgres::{Client, NoTls};
use ultrasql_server::{Server, bind_listener, serve_listener};

use crate::{TpcbArgs, TpcbEngine, percentile};

const DEFAULT_ACCOUNTS_PER_SCALE: usize = 100_000;
const TELLERS_PER_BRANCH: usize = 10;

pub(crate) fn run_blocking(args: TpcbArgs) -> Result<()> {
    let worker_threads = args.connections.clamp(1, 8);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads)
        .enable_all()
        .build()
        .context("build tokio runtime for tpcb")?;
    runtime.block_on(run(args))
}

async fn run(args: TpcbArgs) -> Result<()> {
    let target = Target::start(args.engine, args.dsn.clone()).await?;
    let config = TpcbConfig::from_args(&args)?;
    let admin = connect(&target.dsn).await?;
    reset_schema(&admin.client, &config).await?;
    admin.shutdown();

    if args.warmup_secs > 0 {
        let _ = run_phase(&target.dsn, &config, Duration::from_secs(args.warmup_secs)).await?;
    }

    let measured = run_phase(
        &target.dsn,
        &config,
        Duration::from_secs(args.duration_secs),
    )
    .await?;
    let checker = connect(&target.dsn).await?;
    let correctness = verify_correctness(&checker.client, &config).await?;
    checker.shutdown();

    let mut latencies = measured.latency_us;
    latencies.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let elapsed_secs = measured.elapsed.as_secs_f64();
    let throughput_per_sec = if elapsed_secs > 0.0 {
        measured.transactions as f64 / elapsed_secs
    } else {
        0.0
    };
    let report = TpcbReport {
        engine: args.engine.label(),
        workload: "tpcb_32conn",
        scale: args.scale,
        accounts: config.accounts,
        branches: config.branches,
        tellers: config.tellers,
        connections: args.connections,
        warmup_secs: args.warmup_secs,
        duration_secs: args.duration_secs,
        transactions: measured.transactions,
        throughput_per_sec,
        p50_latency_us: percentile(&latencies, 0.50),
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
    async fn start(engine: TpcbEngine, dsn: Option<String>) -> Result<Self> {
        match (engine, dsn) {
            (TpcbEngine::Postgres17, Some(dsn)) | (TpcbEngine::Ultrasql, Some(dsn)) => {
                Ok(Self { dsn })
            }
            (TpcbEngine::Postgres17, None) => {
                anyhow::bail!("--dsn is required when --engine postgres17")
            }
            (TpcbEngine::Ultrasql, None) => {
                let bind_addr: SocketAddr = "127.0.0.1:0".parse()?;
                let (listener, bound) = bind_listener(bind_addr).await.context("bind ultrasql")?;
                let state = Arc::new(Server::with_sample_database());
                std::thread::Builder::new()
                    .name("ultrasql-tpcb-server".to_string())
                    .spawn(move || {
                        let runtime = match tokio::runtime::Builder::new_current_thread()
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

#[derive(Clone)]
struct TpcbConfig {
    accounts: usize,
    branches: usize,
    tellers: usize,
    connections: usize,
    table_prefix: String,
}

impl TpcbConfig {
    fn from_args(args: &TpcbArgs) -> Result<Self> {
        if args.scale == 0 {
            anyhow::bail!("--scale must be >= 1");
        }
        if args.connections == 0 {
            anyhow::bail!("--connections must be >= 1");
        }
        let accounts = args
            .accounts
            .unwrap_or_else(|| args.scale.saturating_mul(DEFAULT_ACCOUNTS_PER_SCALE));
        if accounts == 0 {
            anyhow::bail!("--accounts must be >= 1");
        }
        let branches = args.scale.max(1);
        let tellers = branches
            .checked_mul(TELLERS_PER_BRANCH)
            .context("teller count overflow")?;
        Ok(Self {
            accounts,
            branches,
            tellers,
            connections: args.connections,
            table_prefix: "ultrasql_tpcb_cert".to_string(),
        })
    }

    fn branches_table(&self) -> String {
        format!("{}_branches", self.table_prefix)
    }

    fn tellers_table(&self) -> String {
        format!("{}_tellers", self.table_prefix)
    }

    fn accounts_table(&self) -> String {
        format!("{}_accounts", self.table_prefix)
    }

    fn history_table(&self) -> String {
        format!("{}_history", self.table_prefix)
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
            eprintln!("tpcb connection task exited: {err}");
        }
    });
    Ok(DbConn { client, task })
}

async fn reset_schema(client: &Client, config: &TpcbConfig) -> Result<()> {
    let branches = config.branches_table();
    let tellers = config.tellers_table();
    let accounts = config.accounts_table();
    let history = config.history_table();
    for ddl in [
        format!("DROP TABLE IF EXISTS {history}"),
        format!("DROP TABLE IF EXISTS {accounts}"),
        format!("DROP TABLE IF EXISTS {tellers}"),
        format!("DROP TABLE IF EXISTS {branches}"),
        format!("CREATE TABLE {branches} (bid INT NOT NULL, bbalance INT NOT NULL)"),
        format!("CREATE TABLE {tellers} (tid INT NOT NULL, tbalance INT NOT NULL)"),
        format!("CREATE TABLE {accounts} (aid INT NOT NULL, abalance INT NOT NULL)"),
        format!(
            "CREATE TABLE {history} (tid INT NOT NULL, bid INT NOT NULL, aid INT NOT NULL, delta INT NOT NULL)"
        ),
    ] {
        client
            .batch_execute(&ddl)
            .await
            .with_context(|| format!("execute tpcb ddl: {ddl}"))?;
    }

    insert_generated(client, &branches, "bid, bbalance", config.branches, |ix| {
        let bid = ix + 1;
        format!("({bid}, 0)")
    })
    .await?;
    insert_generated(client, &tellers, "tid, tbalance", config.tellers, |ix| {
        let tid = ix + 1;
        format!("({tid}, 0)")
    })
    .await?;
    insert_generated(client, &accounts, "aid, abalance", config.accounts, |ix| {
        let aid = ix + 1;
        format!("({aid}, 0)")
    })
    .await?;
    for ddl in [
        format!("CREATE INDEX {branches}_bid_idx ON {branches} (bid)"),
        format!("CREATE INDEX {tellers}_tid_idx ON {tellers} (tid)"),
        format!("CREATE INDEX {accounts}_aid_idx ON {accounts} (aid)"),
    ] {
        client
            .batch_execute(&ddl)
            .await
            .with_context(|| format!("execute tpcb index ddl: {ddl}"))?;
    }
    Ok(())
}

async fn insert_generated<F>(
    client: &Client,
    table: &str,
    columns: &str,
    rows: usize,
    row_fn: F,
) -> Result<()>
where
    F: Fn(usize) -> String,
{
    const CHUNK: usize = 1_000;
    let mut start = 0;
    while start < rows {
        let end = (start + CHUNK).min(rows);
        let values = (start..end).map(&row_fn).collect::<Vec<_>>().join(",");
        client
            .batch_execute(&format!("INSERT INTO {table} ({columns}) VALUES {values}"))
            .await
            .with_context(|| format!("preload {table} rows {start}..{end}"))?;
        start = end;
    }
    Ok(())
}

struct PhaseResult {
    transactions: u64,
    latency_us: Vec<f64>,
    elapsed: Duration,
}

async fn run_phase(dsn: &str, config: &TpcbConfig, duration: Duration) -> Result<PhaseResult> {
    let started = Instant::now();
    let deadline = started + duration;
    let mut tasks = JoinSet::new();
    let shared_dsn = Arc::new(dsn.to_string());
    let shared_config = Arc::new(config.clone());
    for client_id in 0..config.connections {
        let dsn = Arc::clone(&shared_dsn);
        let config = Arc::clone(&shared_config);
        tasks.spawn(async move { run_client(&dsn, &config, client_id, deadline).await });
    }

    let mut transactions = 0_u64;
    let mut latency_us = Vec::new();
    while let Some(joined) = tasks.join_next().await {
        let client_result = joined.context("join tpcb client task")??;
        transactions = transactions.saturating_add(client_result.transactions);
        latency_us.extend(client_result.latency_us);
    }
    Ok(PhaseResult {
        transactions,
        latency_us,
        elapsed: started.elapsed(),
    })
}

struct ClientResult {
    transactions: u64,
    latency_us: Vec<f64>,
}

async fn run_client(
    dsn: &str,
    config: &TpcbConfig,
    client_id: usize,
    deadline: Instant,
) -> Result<ClientResult> {
    let conn = connect(dsn).await?;
    let mut rng = SplitMix64::new(0x54CF_BEEF_1000_0000 ^ u64::try_from(client_id).unwrap_or(0));
    let mut transactions = 0_u64;
    let mut latency_us = Vec::new();
    while Instant::now() < deadline {
        let tx = next_tx(config, &mut rng)?;
        let before = Instant::now();
        execute_tx(&conn.client, &tx).await?;
        latency_us.push(before.elapsed().as_secs_f64() * 1_000_000.0);
        transactions = transactions.saturating_add(1);
    }
    conn.shutdown();
    Ok(ClientResult {
        transactions,
        latency_us,
    })
}

struct TpcbTx {
    statements: [String; 9],
}

fn next_tx(config: &TpcbConfig, rng: &mut SplitMix64) -> Result<TpcbTx> {
    let aid = bounded_one_based(rng.next_u64(), config.accounts)?;
    let tid = bounded_one_based(rng.next_u64(), config.tellers)?;
    let bid = ((tid - 1) / TELLERS_PER_BRANCH) + 1;
    let delta = i64::try_from((rng.next_u64() % 199) + 1).unwrap_or(1) - 100;
    let accounts = config.accounts_table();
    let tellers = config.tellers_table();
    let branches = config.branches_table();
    let history = config.history_table();
    Ok(TpcbTx {
        statements: [
            "BEGIN".to_string(),
            format!("SELECT bbalance FROM {branches} WHERE bid = {bid} FOR UPDATE"),
            format!("SELECT tbalance FROM {tellers} WHERE tid = {tid} FOR UPDATE"),
            format!("SELECT abalance FROM {accounts} WHERE aid = {aid} FOR UPDATE"),
            format!("UPDATE {accounts} SET abalance = abalance + {delta} WHERE aid = {aid}"),
            format!("UPDATE {tellers} SET tbalance = tbalance + {delta} WHERE tid = {tid}"),
            format!("UPDATE {branches} SET bbalance = bbalance + {delta} WHERE bid = {bid}"),
            format!(
                "INSERT INTO {history} (tid, bid, aid, delta) VALUES ({tid}, {bid}, {aid}, {delta})"
            ),
            "COMMIT".to_string(),
        ],
    })
}

async fn execute_tx(client: &Client, tx: &TpcbTx) -> Result<()> {
    const MAX_RETRIES: usize = 1_024;
    for attempt in 0..MAX_RETRIES {
        match execute_tx_once(client, tx).await {
            Ok(()) => return Ok(()),
            Err(err) if is_retryable_conflict(&err) && attempt + 1 < MAX_RETRIES => {
                let _ = client.batch_execute("ROLLBACK").await;
                if attempt < 128 {
                    tokio::task::yield_now().await;
                } else {
                    let backoff_us = 50_u64
                        + u64::try_from(attempt.saturating_sub(128))
                            .unwrap_or(950)
                            .min(950);
                    tokio::time::sleep(Duration::from_micros(backoff_us)).await;
                }
            }
            Err(err) => return Err(err),
        }
    }
    unreachable!("bounded retry loop either returns or errors")
}

async fn execute_tx_once(client: &Client, tx: &TpcbTx) -> Result<()> {
    for statement in &tx.statements {
        client
            .batch_execute(statement)
            .await
            .with_context(|| format!("execute tpcb statement: {statement}"))?;
    }
    Ok(())
}

fn is_retryable_conflict(err: &anyhow::Error) -> bool {
    let text = format!("{err:#}");
    text.contains("update on deleted tuple")
        || text.contains("write conflict")
        || text.contains("serialization")
        || text.contains("could not serialize")
}

fn bounded_one_based(raw: u64, upper: usize) -> Result<usize> {
    let upper_u64 = u64::try_from(upper).context("upper bound conversion")?;
    let value = (raw % upper_u64) + 1;
    usize::try_from(value).context("bounded value conversion")
}

#[derive(Serialize)]
struct CorrectnessReport {
    passed: bool,
    history_row_count: i64,
    history_delta_sum: i64,
    accounts_balance_sum: i64,
    tellers_balance_sum: i64,
    branches_balance_sum: i64,
}

async fn verify_correctness(client: &Client, config: &TpcbConfig) -> Result<CorrectnessReport> {
    let history_row_count = query_i64_sum(
        client,
        &format!("SELECT COUNT(*) FROM {}", config.history_table()),
    )
    .await?;
    let history_delta_sum = query_i64_sum(
        client,
        &format!("SELECT SUM(delta) FROM {}", config.history_table()),
    )
    .await?;
    let accounts_balance_sum = query_i64_sum(
        client,
        &format!("SELECT SUM(abalance) FROM {}", config.accounts_table()),
    )
    .await?;
    let tellers_balance_sum = query_i64_sum(
        client,
        &format!("SELECT SUM(tbalance) FROM {}", config.tellers_table()),
    )
    .await?;
    let branches_balance_sum = query_i64_sum(
        client,
        &format!("SELECT SUM(bbalance) FROM {}", config.branches_table()),
    )
    .await?;
    Ok(CorrectnessReport {
        passed: history_row_count > 0
            && history_delta_sum == accounts_balance_sum
            && history_delta_sum == tellers_balance_sum
            && history_delta_sum == branches_balance_sum,
        history_row_count,
        history_delta_sum,
        accounts_balance_sum,
        tellers_balance_sum,
        branches_balance_sum,
    })
}

async fn query_i64_sum(client: &Client, sql: &str) -> Result<i64> {
    let row = client
        .query_one(sql, &[])
        .await
        .with_context(|| format!("query aggregate: {sql}"))?;
    Ok(row.try_get::<_, Option<i64>>(0)?.unwrap_or(0))
}

#[derive(Serialize)]
struct TpcbReport {
    engine: &'static str,
    workload: &'static str,
    scale: usize,
    accounts: usize,
    branches: usize,
    tellers: usize,
    connections: usize,
    warmup_secs: u64,
    duration_secs: u64,
    transactions: u64,
    throughput_per_sec: f64,
    p50_latency_us: f64,
    p99_latency_us: f64,
    correctness: CorrectnessReport,
}

fn write_report(path: Option<&Path>, report: &TpcbReport) -> Result<()> {
    let serialized = serde_json::to_string_pretty(report)?;
    if let Some(path) = path {
        std::fs::write(path, format!("{serialized}\n"))
            .with_context(|| format!("write {}", path.display()))?;
        eprintln!("tpcb: wrote {}", path.display());
    } else {
        println!("{serialized}");
    }
    Ok(())
}

impl TpcbEngine {
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
}
