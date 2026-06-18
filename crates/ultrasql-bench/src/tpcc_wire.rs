//! PostgreSQL-wire TPC-C benchmark implementation.

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use num_traits::ToPrimitive;
use serde::Serialize;
use tokio::task::JoinSet;
use tokio_postgres::{Client, NoTls};
use ultrasql_server::{Server, bind_listener, serve_listener};

use crate::{TpccArgs, TpccEngine, percentile};

const DEFAULT_ITEMS: usize = 10_000;
const DEFAULT_CUSTOMERS_PER_DISTRICT: usize = 3_000;
const DISTRICTS_PER_WAREHOUSE: usize = 10;

pub(crate) fn run_blocking(args: TpccArgs) -> Result<()> {
    let worker_threads = tpcc_worker_threads(args.connections);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads)
        .enable_all()
        .build()
        .context("build tokio runtime for tpcc")?;
    runtime.block_on(run(args))
}

async fn run(args: TpccArgs) -> Result<()> {
    let worker_threads = tpcc_worker_threads(args.connections);
    let target = Target::start(args.engine, args.dsn.clone(), worker_threads).await?;
    let config = TpccConfig::from_args(&args)?;
    let admin = connect(&target.dsn).await?;
    reset_schema(&admin.client, &config).await?;
    admin.shutdown();

    let workload = Arc::new(TpccWorkloadState::new(&config));
    if args.warmup_secs > 0 {
        let _ = run_phase(
            &target.dsn,
            &config,
            &workload,
            Duration::from_secs(args.warmup_secs),
        )
        .await?;
    }

    let measured = run_phase(
        &target.dsn,
        &config,
        &workload,
        Duration::from_secs(args.duration_secs),
    )
    .await?;
    let checker = connect(&target.dsn).await?;
    let correctness = verify_correctness(&checker.client, &config, measured.counts).await?;
    checker.shutdown();

    let mut latencies = measured.latency_us;
    ultrasql_bench::sort_f64_nan_last(&mut latencies);
    let elapsed_secs = measured.elapsed.as_secs_f64();
    let throughput_per_sec = if elapsed_secs > 0.0 {
        measured.transactions.to_f64().unwrap_or(f64::MAX) / elapsed_secs
    } else {
        0.0
    };
    let report = TpccReport {
        engine: args.engine.label(),
        workload: "tpcc_5types",
        warehouses: config.warehouses,
        districts: config.district_count(),
        customers_per_district: config.customers_per_district,
        items: config.items,
        initial_orders_per_district: config.initial_orders_per_district,
        order_lines: config.order_lines,
        connections: args.connections,
        warmup_secs: args.warmup_secs,
        duration_secs: args.duration_secs,
        transactions: measured.transactions,
        transaction_counts: measured.counts,
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
    async fn start(engine: TpccEngine, dsn: Option<String>, worker_threads: usize) -> Result<Self> {
        match (engine, dsn) {
            (TpccEngine::Postgres17, Some(dsn)) | (TpccEngine::Ultrasql, Some(dsn)) => {
                Ok(Self { dsn })
            }
            (TpccEngine::Postgres17, None) => {
                anyhow::bail!("--dsn is required when --engine postgres17")
            }
            (TpccEngine::Ultrasql, None) => {
                let bind_addr: SocketAddr = "127.0.0.1:0".parse()?;
                let (listener, bound) = bind_listener(bind_addr).await.context("bind ultrasql")?;
                let state = Arc::new(Server::with_sample_database());
                std::thread::Builder::new()
                    .name("ultrasql-tpcc-server".to_string())
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
                    dsn: format!("host=127.0.0.1 port={} user=bench_runner", bound.port()),
                })
            }
        }
    }
}

fn tpcc_worker_threads(connections: usize) -> usize {
    connections.saturating_add(2).clamp(2, 64)
}

#[derive(Clone)]
struct TpccConfig {
    warehouses: usize,
    items: usize,
    customers_per_district: usize,
    initial_orders_per_district: usize,
    order_lines: usize,
    connections: usize,
    table_prefix: String,
}

impl TpccConfig {
    fn from_args(args: &TpccArgs) -> Result<Self> {
        if args.warehouses == 0 {
            anyhow::bail!("--warehouses must be >= 1");
        }
        if args.connections == 0 {
            anyhow::bail!("--connections must be >= 1");
        }
        let items = args.items.unwrap_or(DEFAULT_ITEMS);
        if items == 0 {
            anyhow::bail!("--items must be >= 1");
        }
        let customers_per_district = args
            .customers_per_district
            .unwrap_or(DEFAULT_CUSTOMERS_PER_DISTRICT);
        if customers_per_district == 0 {
            anyhow::bail!("--customers-per-district must be >= 1");
        }
        let initial_orders_per_district = args
            .initial_orders_per_district
            .unwrap_or(customers_per_district);
        if initial_orders_per_district == 0 {
            anyhow::bail!("--initial-orders-per-district must be >= 1");
        }
        if args.order_lines == 0 {
            anyhow::bail!("--order-lines must be >= 1");
        }
        if args.order_lines > items {
            anyhow::bail!("--order-lines must be <= --items");
        }
        Ok(Self {
            warehouses: args.warehouses,
            items,
            customers_per_district,
            initial_orders_per_district,
            order_lines: args.order_lines,
            connections: args.connections,
            table_prefix: "ultrasql_tpcc_cert".to_string(),
        })
    }

    fn district_count(&self) -> usize {
        self.warehouses * DISTRICTS_PER_WAREHOUSE
    }

    fn initial_order_count(&self) -> usize {
        self.district_count() * self.initial_orders_per_district
    }

    fn warehouse_table(&self) -> String {
        format!("{}_warehouse", self.table_prefix)
    }

    fn district_table(&self) -> String {
        format!("{}_district", self.table_prefix)
    }

    fn customer_table(&self) -> String {
        format!("{}_customer", self.table_prefix)
    }

    fn item_table(&self) -> String {
        format!("{}_item", self.table_prefix)
    }

    fn stock_table(&self) -> String {
        format!("{}_stock", self.table_prefix)
    }

    fn orders_table(&self) -> String {
        format!("{}_orders", self.table_prefix)
    }

    fn new_order_table(&self) -> String {
        format!("{}_new_order", self.table_prefix)
    }

    fn order_line_table(&self) -> String {
        format!("{}_order_line", self.table_prefix)
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
            eprintln!("tpcc connection task exited: {err}");
        }
    });
    Ok(DbConn { client, task })
}

async fn reset_schema(client: &Client, config: &TpccConfig) -> Result<()> {
    let warehouse = config.warehouse_table();
    let district = config.district_table();
    let customer = config.customer_table();
    let item = config.item_table();
    let stock = config.stock_table();
    let orders = config.orders_table();
    let new_order = config.new_order_table();
    let order_line = config.order_line_table();
    let history = config.history_table();

    for ddl in [
        format!("DROP TABLE IF EXISTS {history}"),
        format!("DROP TABLE IF EXISTS {order_line}"),
        format!("DROP TABLE IF EXISTS {new_order}"),
        format!("DROP TABLE IF EXISTS {orders}"),
        format!("DROP TABLE IF EXISTS {stock}"),
        format!("DROP TABLE IF EXISTS {item}"),
        format!("DROP TABLE IF EXISTS {customer}"),
        format!("DROP TABLE IF EXISTS {district}"),
        format!("DROP TABLE IF EXISTS {warehouse}"),
        format!("CREATE TABLE {warehouse} (w_id INT NOT NULL, w_ytd INT NOT NULL)"),
        format!(
            "CREATE TABLE {district} (d_w_id INT NOT NULL, d_id INT NOT NULL, d_ytd INT NOT NULL, d_next_o_id INT NOT NULL, d_next_delivery_o_id INT NOT NULL)"
        ),
        format!(
            "CREATE TABLE {customer} (c_w_id INT NOT NULL, c_d_id INT NOT NULL, c_id INT NOT NULL, c_balance INT NOT NULL, c_ytd_payment INT NOT NULL, c_payment_cnt INT NOT NULL, c_delivery_cnt INT NOT NULL, c_last_order_id INT NOT NULL)"
        ),
        format!("CREATE TABLE {item} (i_id INT NOT NULL, i_price INT NOT NULL)"),
        format!(
            "CREATE TABLE {stock} (s_w_id INT NOT NULL, s_i_id INT NOT NULL, s_quantity INT NOT NULL, s_ytd INT NOT NULL, s_order_cnt INT NOT NULL, s_remote_cnt INT NOT NULL)"
        ),
        format!(
            "CREATE TABLE {orders} (o_w_id INT NOT NULL, o_d_id INT NOT NULL, o_id INT NOT NULL, o_c_id INT NOT NULL, o_carrier_id INT NOT NULL, o_entry_d INT NOT NULL)"
        ),
        format!(
            "CREATE TABLE {new_order} (no_w_id INT NOT NULL, no_d_id INT NOT NULL, no_o_id INT NOT NULL)"
        ),
        format!(
            "CREATE TABLE {order_line} (ol_w_id INT NOT NULL, ol_d_id INT NOT NULL, ol_o_id INT NOT NULL, ol_number INT NOT NULL, ol_i_id INT NOT NULL, ol_supply_w_id INT NOT NULL, ol_quantity INT NOT NULL, ol_amount INT NOT NULL, ol_delivery_d INT NOT NULL)"
        ),
        format!(
            "CREATE TABLE {history} (h_w_id INT NOT NULL, h_d_id INT NOT NULL, h_c_w_id INT NOT NULL, h_c_d_id INT NOT NULL, h_c_id INT NOT NULL, h_amount INT NOT NULL)"
        ),
    ] {
        batch_execute_with_timeout(client, &ddl, "execute tpcc ddl").await?;
    }

    insert_generated(client, &warehouse, "w_id, w_ytd", config.warehouses, |ix| {
        let w_id = ix + 1;
        format!("({w_id}, 0)")
    })
    .await?;
    insert_generated(
        client,
        &district,
        "d_w_id, d_id, d_ytd, d_next_o_id, d_next_delivery_o_id",
        config.district_count(),
        |ix| {
            let w_id = (ix / DISTRICTS_PER_WAREHOUSE) + 1;
            let d_id = (ix % DISTRICTS_PER_WAREHOUSE) + 1;
            let next_o_id = config.initial_orders_per_district + 1;
            format!("({w_id}, {d_id}, 0, {next_o_id}, 1)")
        },
    )
    .await?;
    insert_generated(
        client,
        &customer,
        "c_w_id, c_d_id, c_id, c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_last_order_id",
        config.district_count() * config.customers_per_district,
        |ix| {
            let district_ix = ix / config.customers_per_district;
            let w_id = (district_ix / DISTRICTS_PER_WAREHOUSE) + 1;
            let d_id = (district_ix % DISTRICTS_PER_WAREHOUSE) + 1;
            let c_id = (ix % config.customers_per_district) + 1;
            format!("({w_id}, {d_id}, {c_id}, 0, 0, 0, 0, 0)")
        },
    )
    .await?;
    insert_generated(client, &item, "i_id, i_price", config.items, |ix| {
        let i_id = ix + 1;
        let price = (ix % 100) + 1;
        format!("({i_id}, {price})")
    })
    .await?;
    insert_generated(
        client,
        &stock,
        "s_w_id, s_i_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt",
        config.warehouses * config.items,
        |ix| {
            let w_id = (ix / config.items) + 1;
            let i_id = (ix % config.items) + 1;
            format!("({w_id}, {i_id}, 100, 0, 0, 0)")
        },
    )
    .await?;
    insert_generated(
        client,
        &orders,
        "o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_entry_d",
        config.initial_order_count(),
        |ix| {
            let (w_id, d_id, o_id) = initial_order_key(config, ix);
            let c_id = ((o_id - 1) % config.customers_per_district) + 1;
            format!("({w_id}, {d_id}, {o_id}, {c_id}, 0, {o_id})")
        },
    )
    .await?;
    insert_generated(
        client,
        &new_order,
        "no_w_id, no_d_id, no_o_id",
        config.initial_order_count(),
        |ix| {
            let (w_id, d_id, o_id) = initial_order_key(config, ix);
            format!("({w_id}, {d_id}, {o_id})")
        },
    )
    .await?;
    insert_generated(
        client,
        &order_line,
        "ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d",
        config.initial_order_count() * config.order_lines,
        |ix| {
            let order_ix = ix / config.order_lines;
            let line = (ix % config.order_lines) + 1;
            let (w_id, d_id, o_id) = initial_order_key(config, order_ix);
            let item_id = ((order_ix + line) % config.items) + 1;
            format!("({w_id}, {d_id}, {o_id}, {line}, {item_id}, {w_id}, 5, 10, 0)")
        },
    )
    .await?;

    for ddl in [
        format!("CREATE INDEX {warehouse}_wid_idx ON {warehouse} (w_id)"),
        format!("CREATE INDEX {district}_did_idx ON {district} (d_id)"),
        format!("CREATE INDEX {customer}_cid_idx ON {customer} (c_id)"),
        format!("CREATE INDEX {stock}_iid_idx ON {stock} (s_i_id)"),
    ] {
        batch_execute_with_timeout(client, &ddl, "execute tpcc index ddl").await?;
    }
    Ok(())
}

fn initial_order_key(config: &TpccConfig, ix: usize) -> (usize, usize, usize) {
    let district_ix = ix / config.initial_orders_per_district;
    let w_id = (district_ix / DISTRICTS_PER_WAREHOUSE) + 1;
    let d_id = (district_ix % DISTRICTS_PER_WAREHOUSE) + 1;
    let o_id = (ix % config.initial_orders_per_district) + 1;
    (w_id, d_id, o_id)
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
        let sql = format!("INSERT INTO {table} ({columns}) VALUES {values}");
        batch_execute_with_timeout(
            client,
            &sql,
            &format!("preload {table} rows {start}..{end}"),
        )
        .await?;
        start = end;
    }
    Ok(())
}

struct TpccWorkloadState {
    next_order_id: Vec<AtomicUsize>,
    next_delivery_order_id: Vec<AtomicUsize>,
}

impl TpccWorkloadState {
    fn new(config: &TpccConfig) -> Self {
        Self {
            next_order_id: (0..config.district_count())
                .map(|_| AtomicUsize::new(config.initial_orders_per_district + 1))
                .collect(),
            next_delivery_order_id: (0..config.district_count())
                .map(|_| AtomicUsize::new(1))
                .collect(),
        }
    }

    fn allocate_order(&self, district_index: usize) -> usize {
        self.next_order_id[district_index].fetch_add(1, Ordering::Relaxed)
    }

    fn allocate_delivery(&self, district_index: usize) -> usize {
        self.next_delivery_order_id[district_index].fetch_add(1, Ordering::Relaxed)
    }
}

struct PhaseResult {
    transactions: u64,
    counts: TransactionCounts,
    latency_us: Vec<f64>,
    elapsed: Duration,
}

async fn run_phase(
    dsn: &str,
    config: &TpccConfig,
    workload: &Arc<TpccWorkloadState>,
    duration: Duration,
) -> Result<PhaseResult> {
    let started = Instant::now();
    let deadline = started + duration;
    let mut transactions = 0_u64;
    let mut counts = TransactionCounts::default();
    let mut latency_us = Vec::new();
    if duration > Duration::ZERO {
        let conn = connect(dsn).await?;
        let mut rng = SplitMix64::new(0x710C_CAFE_5AFE_0000);
        for kind in TransactionKind::all() {
            if Instant::now() >= deadline {
                break;
            }
            let tx = next_tx_for_kind(config, workload, &mut rng, kind)?;
            let before = Instant::now();
            if !execute_tx(&conn.client, &tx, deadline).await? {
                break;
            }
            latency_us.push(before.elapsed().as_secs_f64() * 1_000_000.0);
            transactions = transactions.saturating_add(1);
            counts.record(tx.kind);
        }
        conn.shutdown();
    }
    let mut tasks = JoinSet::new();
    let shared_dsn = Arc::new(dsn.to_string());
    let shared_config = Arc::new(config.clone());
    for client_id in 0..config.connections {
        let dsn = Arc::clone(&shared_dsn);
        let config = Arc::clone(&shared_config);
        let workload = Arc::clone(workload);
        tasks.spawn(async move { run_client(&dsn, &config, &workload, client_id, deadline).await });
    }

    while let Some(joined) = tasks.join_next().await {
        let client_result = joined.context("join tpcc client task")??;
        transactions = transactions.saturating_add(client_result.transactions);
        counts.merge(client_result.counts);
        latency_us.extend(client_result.latency_us);
    }
    Ok(PhaseResult {
        transactions,
        counts,
        latency_us,
        elapsed: started.elapsed(),
    })
}

struct ClientResult {
    transactions: u64,
    counts: TransactionCounts,
    latency_us: Vec<f64>,
}

async fn run_client(
    dsn: &str,
    config: &TpccConfig,
    workload: &TpccWorkloadState,
    client_id: usize,
    deadline: Instant,
) -> Result<ClientResult> {
    let conn = connect(dsn).await?;
    let mut rng = SplitMix64::new(0x710C_CAFE_5000_0000 ^ u64::try_from(client_id).unwrap_or(0));
    let mut transactions = 0_u64;
    let mut counts = TransactionCounts::default();
    let mut latency_us = Vec::new();
    while Instant::now() < deadline {
        let tx = next_tx(config, workload, &mut rng)?;
        let before = Instant::now();
        if !execute_tx(&conn.client, &tx, deadline).await? {
            break;
        }
        latency_us.push(before.elapsed().as_secs_f64() * 1_000_000.0);
        transactions = transactions.saturating_add(1);
        counts.record(tx.kind);
    }
    conn.shutdown();
    Ok(ClientResult {
        transactions,
        counts,
        latency_us,
    })
}

struct TpccTx {
    kind: TransactionKind,
    sql: String,
}

fn next_tx(
    config: &TpccConfig,
    workload: &TpccWorkloadState,
    rng: &mut SplitMix64,
) -> Result<TpccTx> {
    let selector = u8::try_from(rng.next_u64() % 100).context("transaction selector in range")?;
    let kind = transaction_kind(selector);
    next_tx_for_kind(config, workload, rng, kind)
}

fn next_tx_for_kind(
    config: &TpccConfig,
    workload: &TpccWorkloadState,
    rng: &mut SplitMix64,
    kind: TransactionKind,
) -> Result<TpccTx> {
    let warehouse = bounded_one_based(rng.next_u64(), config.warehouses)?;
    let district = bounded_one_based(rng.next_u64(), DISTRICTS_PER_WAREHOUSE)?;
    let customer = bounded_one_based(rng.next_u64(), config.customers_per_district)?;
    let district_index = district_index(warehouse, district);
    let sql = match kind {
        TransactionKind::NewOrder => {
            let order_id = workload.allocate_order(district_index);
            new_order_sql(config, warehouse, district, customer, order_id, rng)?
        }
        TransactionKind::Payment => payment_sql(config, warehouse, district, customer, rng)?,
        TransactionKind::OrderStatus => order_status_sql(config, warehouse, district, customer),
        TransactionKind::Delivery => {
            let order_id = workload.allocate_delivery(district_index);
            delivery_sql(config, warehouse, district, customer, order_id, rng)?
        }
        TransactionKind::StockLevel => stock_level_sql(config, warehouse, rng),
    };
    Ok(TpccTx { kind, sql })
}

fn new_order_sql(
    config: &TpccConfig,
    warehouse: usize,
    district: usize,
    customer: usize,
    order_id: usize,
    rng: &mut SplitMix64,
) -> Result<String> {
    let w_id = i32::try_from(warehouse).context("warehouse conversion")?;
    let d_id = i32::try_from(district).context("district conversion")?;
    let c_id = i32::try_from(customer).context("customer conversion")?;
    let o_id = i32::try_from(order_id).context("order conversion")?;
    let district_table = config.district_table();
    let customer_table = config.customer_table();
    let orders_table = config.orders_table();
    let new_order_table = config.new_order_table();
    let order_line_table = config.order_line_table();
    let stock_table = config.stock_table();

    let mut sql = format!(
        "BEGIN;\
         UPDATE {district_table} SET d_next_o_id = d_next_o_id + 1 WHERE d_w_id = {w_id} AND d_id = {d_id};\
         UPDATE {customer_table} SET c_last_order_id = {o_id} WHERE c_w_id = {w_id} AND c_d_id = {d_id} AND c_id = {c_id};\
         INSERT INTO {orders_table} (o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_entry_d) VALUES ({w_id}, {d_id}, {o_id}, {c_id}, 0, {o_id});\
         INSERT INTO {new_order_table} (no_w_id, no_d_id, no_o_id) VALUES ({w_id}, {d_id}, {o_id});"
    );
    let mut used_items = Vec::with_capacity(config.order_lines);
    for line in 1..=config.order_lines {
        let mut item_usize = bounded_one_based(rng.next_u64(), config.items)?;
        while used_items.contains(&item_usize) {
            item_usize = (item_usize % config.items) + 1;
        }
        used_items.push(item_usize);
        let item = i32::try_from(item_usize).context("item conversion")?;
        let quantity = i32::try_from((rng.next_u64() % 10) + 1).unwrap_or(1);
        let amount = quantity.saturating_mul((item % 100) + 1);
        sql.push_str(&format!(
            "UPDATE {stock_table} SET s_quantity = s_quantity - {quantity}, s_ytd = s_ytd + {quantity}, s_order_cnt = s_order_cnt + 1 WHERE s_w_id = {w_id} AND s_i_id = {item};\
             INSERT INTO {order_line_table} (ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d) VALUES ({w_id}, {d_id}, {o_id}, {line}, {item}, {w_id}, {quantity}, {amount}, 0);"
        ));
    }
    sql.push_str("COMMIT");
    Ok(sql)
}

fn payment_sql(
    config: &TpccConfig,
    warehouse: usize,
    district: usize,
    customer: usize,
    rng: &mut SplitMix64,
) -> Result<String> {
    let w_id = i32::try_from(warehouse).context("warehouse conversion")?;
    let d_id = i32::try_from(district).context("district conversion")?;
    let c_id = i32::try_from(customer).context("customer conversion")?;
    let amount = i32::try_from((rng.next_u64() % 5_000) + 1).unwrap_or(1);
    let warehouse_table = config.warehouse_table();
    let district_table = config.district_table();
    let customer_table = config.customer_table();
    let history_table = config.history_table();
    Ok(format!(
        "BEGIN;\
         UPDATE {warehouse_table} SET w_ytd = w_ytd + {amount} WHERE w_id = {w_id};\
         UPDATE {district_table} SET d_ytd = d_ytd + {amount} WHERE d_w_id = {w_id} AND d_id = {d_id};\
         UPDATE {customer_table} SET c_balance = c_balance - {amount}, c_ytd_payment = c_ytd_payment + {amount}, c_payment_cnt = c_payment_cnt + 1 WHERE c_w_id = {w_id} AND c_d_id = {d_id} AND c_id = {c_id};\
         INSERT INTO {history_table} (h_w_id, h_d_id, h_c_w_id, h_c_d_id, h_c_id, h_amount) VALUES ({w_id}, {d_id}, {w_id}, {d_id}, {c_id}, {amount});\
         COMMIT"
    ))
}

fn order_status_sql(
    config: &TpccConfig,
    warehouse: usize,
    district: usize,
    customer: usize,
) -> String {
    let customer_table = config.customer_table();
    let orders_table = config.orders_table();
    let order_line_table = config.order_line_table();
    format!(
        "BEGIN;\
         SELECT c_balance, c_last_order_id FROM {customer_table} WHERE c_w_id = {warehouse} AND c_d_id = {district} AND c_id = {customer};\
         SELECT o_id, o_carrier_id FROM {orders_table} WHERE o_w_id = {warehouse} AND o_d_id = {district} AND o_c_id = {customer};\
         SELECT COUNT(*) FROM {order_line_table} WHERE ol_w_id = {warehouse} AND ol_d_id = {district} AND ol_o_id = {customer};\
         COMMIT"
    )
}

fn delivery_sql(
    config: &TpccConfig,
    warehouse: usize,
    district: usize,
    customer: usize,
    order_id: usize,
    rng: &mut SplitMix64,
) -> Result<String> {
    let w_id = i32::try_from(warehouse).context("warehouse conversion")?;
    let d_id = i32::try_from(district).context("district conversion")?;
    let c_id = i32::try_from(customer).context("customer conversion")?;
    let o_id = i32::try_from(order_id).context("order conversion")?;
    let carrier_id = i32::try_from((rng.next_u64() % 10) + 1).unwrap_or(1);
    let district_table = config.district_table();
    let customer_table = config.customer_table();
    let orders_table = config.orders_table();
    let new_order_table = config.new_order_table();
    let order_line_table = config.order_line_table();
    Ok(format!(
        "BEGIN;\
         DELETE FROM {new_order_table} WHERE no_w_id = {w_id} AND no_d_id = {d_id} AND no_o_id = {o_id};\
         UPDATE {orders_table} SET o_carrier_id = {carrier_id} WHERE o_w_id = {w_id} AND o_d_id = {d_id} AND o_id = {o_id};\
         UPDATE {order_line_table} SET ol_delivery_d = {o_id} WHERE ol_w_id = {w_id} AND ol_d_id = {d_id} AND ol_o_id = {o_id};\
         UPDATE {customer_table} SET c_balance = c_balance + 50, c_delivery_cnt = c_delivery_cnt + 1 WHERE c_w_id = {w_id} AND c_d_id = {d_id} AND c_id = {c_id};\
         UPDATE {district_table} SET d_next_delivery_o_id = d_next_delivery_o_id + 1 WHERE d_w_id = {w_id} AND d_id = {d_id};\
         COMMIT"
    ))
}

fn stock_level_sql(config: &TpccConfig, warehouse: usize, rng: &mut SplitMix64) -> String {
    let threshold = (rng.next_u64() % 20) + 10;
    let stock_table = config.stock_table();
    format!(
        "BEGIN;\
         SELECT COUNT(*) FROM {stock_table} WHERE s_w_id = {warehouse} AND s_quantity < {threshold};\
         COMMIT"
    )
}

async fn execute_tx(client: &Client, tx: &TpccTx, deadline: Instant) -> Result<bool> {
    const MAX_RETRIES: usize = 16_384;
    for attempt in 0..MAX_RETRIES {
        match execute_tx_once(client, tx).await {
            Ok(()) => return Ok(true),
            Err(err) if is_retryable_conflict(&err) && attempt + 1 < MAX_RETRIES => {
                let _ = client.batch_execute("ROLLBACK").await;
                if Instant::now() >= deadline {
                    return Ok(false);
                }
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
            Err(err) => return Err(err),
        }
    }
    unreachable!("bounded retry loop either returns or errors")
}

async fn execute_tx_once(client: &Client, tx: &TpccTx) -> Result<()> {
    for statement in tx
        .sql
        .split(';')
        .map(str::trim)
        .filter(|stmt| !stmt.is_empty())
    {
        batch_execute_with_timeout(client, statement, "execute tpcc transaction statement").await?;
    }
    Ok(())
}

async fn batch_execute_with_timeout(client: &Client, sql: &str, label: &str) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(10), client.batch_execute(sql))
        .await
        .with_context(|| format!("{label} timed out: {sql}"))?
        .with_context(|| format!("{label}: {sql}"))?;
    Ok(())
}

fn is_retryable_conflict(err: &anyhow::Error) -> bool {
    let text = format!("{err:#}");
    text.contains("update on deleted tuple")
        || text.contains("write conflict")
        || text.contains("row lock not available")
        || text.contains("deadlock detected")
        || text.contains("serialization")
        || text.contains("could not serialize")
}

fn bounded_one_based(raw: u64, upper: usize) -> Result<usize> {
    let upper_u64 = u64::try_from(upper).context("upper bound conversion")?;
    let value = (raw % upper_u64) + 1;
    usize::try_from(value).context("bounded value conversion")
}

fn district_index(warehouse: usize, district: usize) -> usize {
    ((warehouse - 1) * DISTRICTS_PER_WAREHOUSE) + (district - 1)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TransactionKind {
    NewOrder,
    Payment,
    OrderStatus,
    Delivery,
    StockLevel,
}

impl TransactionKind {
    const fn all() -> [Self; 5] {
        [
            Self::NewOrder,
            Self::Payment,
            Self::OrderStatus,
            Self::Delivery,
            Self::StockLevel,
        ]
    }
}

fn transaction_kind(selector: u8) -> TransactionKind {
    match selector {
        0..=44 => TransactionKind::NewOrder,
        45..=87 => TransactionKind::Payment,
        88..=91 => TransactionKind::OrderStatus,
        92..=95 => TransactionKind::Delivery,
        _ => TransactionKind::StockLevel,
    }
}

#[derive(Clone, Copy, Debug, Default, Serialize)]
struct TransactionCounts {
    new_order: u64,
    payment: u64,
    order_status: u64,
    delivery: u64,
    stock_level: u64,
}

impl TransactionCounts {
    fn record(&mut self, kind: TransactionKind) {
        match kind {
            TransactionKind::NewOrder => self.new_order = self.new_order.saturating_add(1),
            TransactionKind::Payment => self.payment = self.payment.saturating_add(1),
            TransactionKind::OrderStatus => self.order_status = self.order_status.saturating_add(1),
            TransactionKind::Delivery => self.delivery = self.delivery.saturating_add(1),
            TransactionKind::StockLevel => self.stock_level = self.stock_level.saturating_add(1),
        }
    }

    fn merge(&mut self, other: Self) {
        self.new_order = self.new_order.saturating_add(other.new_order);
        self.payment = self.payment.saturating_add(other.payment);
        self.order_status = self.order_status.saturating_add(other.order_status);
        self.delivery = self.delivery.saturating_add(other.delivery);
        self.stock_level = self.stock_level.saturating_add(other.stock_level);
    }

    const fn all_positive(self) -> bool {
        self.new_order > 0
            && self.payment > 0
            && self.order_status > 0
            && self.delivery > 0
            && self.stock_level > 0
    }
}

#[derive(Serialize)]
struct CorrectnessReport {
    passed: bool,
    all_five_transaction_types: bool,
    history_row_count: i64,
    history_amount_sum: i64,
    warehouse_ytd_sum: i64,
    district_ytd_sum: i64,
    customer_ytd_payment_sum: i64,
    orders_row_count: i64,
    new_order_row_count: i64,
    order_line_row_count: i64,
    low_stock_row_count: i64,
}

async fn verify_correctness(
    client: &Client,
    config: &TpccConfig,
    counts: TransactionCounts,
) -> Result<CorrectnessReport> {
    let history_row_count = query_i64_sum(
        client,
        &format!("SELECT COUNT(*) FROM {}", config.history_table()),
    )
    .await?;
    let history_amount_sum = query_i64_sum(
        client,
        &format!("SELECT SUM(h_amount) FROM {}", config.history_table()),
    )
    .await?;
    let warehouse_ytd_sum = query_i64_sum(
        client,
        &format!("SELECT SUM(w_ytd) FROM {}", config.warehouse_table()),
    )
    .await?;
    let district_ytd_sum = query_i64_sum(
        client,
        &format!("SELECT SUM(d_ytd) FROM {}", config.district_table()),
    )
    .await?;
    let customer_ytd_payment_sum = query_i64_sum(
        client,
        &format!("SELECT SUM(c_ytd_payment) FROM {}", config.customer_table()),
    )
    .await?;
    let orders_row_count = query_i64_sum(
        client,
        &format!("SELECT COUNT(*) FROM {}", config.orders_table()),
    )
    .await?;
    let new_order_row_count = query_i64_sum(
        client,
        &format!("SELECT COUNT(*) FROM {}", config.new_order_table()),
    )
    .await?;
    let order_line_row_count = query_i64_sum(
        client,
        &format!("SELECT COUNT(*) FROM {}", config.order_line_table()),
    )
    .await?;
    let low_stock_row_count = query_i64_sum(
        client,
        &format!(
            "SELECT COUNT(*) FROM {} WHERE s_quantity < 10",
            config.stock_table()
        ),
    )
    .await?;
    let expected_order_lines = orders_row_count
        .saturating_mul(i64::try_from(config.order_lines).context("order line count conversion")?);
    let all_five_transaction_types = counts.all_positive();
    Ok(CorrectnessReport {
        passed: all_five_transaction_types
            && history_row_count > 0
            && history_amount_sum == warehouse_ytd_sum
            && history_amount_sum == district_ytd_sum
            && history_amount_sum == customer_ytd_payment_sum
            && orders_row_count > 0
            && new_order_row_count <= orders_row_count
            && order_line_row_count == expected_order_lines,
        all_five_transaction_types,
        history_row_count,
        history_amount_sum,
        warehouse_ytd_sum,
        district_ytd_sum,
        customer_ytd_payment_sum,
        orders_row_count,
        new_order_row_count,
        order_line_row_count,
        low_stock_row_count,
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
struct TpccReport {
    engine: &'static str,
    workload: &'static str,
    warehouses: usize,
    districts: usize,
    customers_per_district: usize,
    items: usize,
    initial_orders_per_district: usize,
    order_lines: usize,
    connections: usize,
    warmup_secs: u64,
    duration_secs: u64,
    transactions: u64,
    transaction_counts: TransactionCounts,
    throughput_per_sec: f64,
    p50_latency_us: f64,
    p99_latency_us: f64,
    correctness: CorrectnessReport,
}

fn write_report(path: Option<&Path>, report: &TpccReport) -> Result<()> {
    let serialized = serde_json::to_string_pretty(report)?;
    if let Some(path) = path {
        std::fs::write(path, format!("{serialized}\n"))
            .with_context(|| format!("write {}", path.display()))?;
        eprintln!("tpcc: wrote {}", path.display());
    } else {
        println!("{serialized}");
    }
    Ok(())
}

impl TpccEngine {
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

#[cfg(test)]
mod tests {
    use super::{
        SplitMix64, TpccConfig, TpccWorkloadState, is_retryable_conflict, next_tx,
        tpcc_worker_threads, transaction_kind,
    };
    use crate::{TpccArgs, TpccEngine};

    fn test_config() -> TpccConfig {
        let args = TpccArgs {
            engine: TpccEngine::Ultrasql,
            dsn: None,
            warehouses: 1,
            items: Some(128),
            customers_per_district: Some(32),
            initial_orders_per_district: Some(32),
            order_lines: 5,
            warmup_secs: 0,
            duration_secs: 1,
            connections: 1,
            output: None,
        };
        TpccConfig::from_args(&args).expect("test config")
    }

    #[test]
    fn transaction_mix_covers_all_five_types() {
        assert_eq!(transaction_kind(0), super::TransactionKind::NewOrder);
        assert_eq!(transaction_kind(45), super::TransactionKind::Payment);
        assert_eq!(transaction_kind(88), super::TransactionKind::OrderStatus);
        assert_eq!(transaction_kind(92), super::TransactionKind::Delivery);
        assert_eq!(transaction_kind(99), super::TransactionKind::StockLevel);
    }

    #[test]
    fn in_process_server_workers_cover_waiting_tpcc_clients() {
        assert_eq!(tpcc_worker_threads(0), 2);
        assert_eq!(tpcc_worker_threads(1), 3);
        assert_eq!(tpcc_worker_threads(32), 34);
        assert_eq!(tpcc_worker_threads(usize::MAX), 64);
    }

    #[test]
    fn generated_workload_includes_all_five_transaction_shapes() {
        let config = test_config();
        let state = TpccWorkloadState::new(&config);
        let mut rng = SplitMix64::new(1);
        let mut seen = [false; 5];
        for _ in 0..200 {
            let tx = next_tx(&config, &state, &mut rng).expect("tpcc tx");
            match tx.kind {
                super::TransactionKind::NewOrder => {
                    assert!(tx.sql.contains("INSERT INTO ultrasql_tpcc_cert_orders"));
                    seen[0] = true;
                }
                super::TransactionKind::Payment => {
                    assert!(tx.sql.contains("INSERT INTO ultrasql_tpcc_cert_history"));
                    seen[1] = true;
                }
                super::TransactionKind::OrderStatus => {
                    assert!(tx.sql.contains("SELECT c_balance, c_last_order_id"));
                    seen[2] = true;
                }
                super::TransactionKind::Delivery => {
                    assert!(tx.sql.contains("DELETE FROM ultrasql_tpcc_cert_new_order"));
                    seen[3] = true;
                }
                super::TransactionKind::StockLevel => {
                    assert!(
                        tx.sql
                            .contains("SELECT COUNT(*) FROM ultrasql_tpcc_cert_stock")
                    );
                    seen[4] = true;
                }
            }
        }
        assert_eq!(seen, [true; 5]);
    }

    #[test]
    fn row_lock_conflict_is_retryable() {
        let err = anyhow::anyhow!(
            "execution error: type mismatch: write conflict: row lock not available"
        );

        assert!(is_retryable_conflict(&err));
    }
}
