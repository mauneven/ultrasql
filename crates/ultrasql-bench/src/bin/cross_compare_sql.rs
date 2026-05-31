//! UltraSQL wire-protocol cross-engine benchmark driver.
//!
//! Spawns an in-process `ultrasqld` instance bound to an ephemeral
//! local TCP port, or connects to an already-running release artifact
//! with `--server`. It then drives a workload from the bench harness
//! through `tokio-postgres` over that real socket. The measurements are
//! end-to-end: TCP send → server message decode → parser → binder →
//! catalog snapshot → autocommit transaction → `ModifyTable` /
//! `SeqScan` over real heap pages → `RowDescription`/`DataRow`/
//! `CommandComplete` encode → TCP receive.
//!
//! This is the apples-to-apples counterpart to the existing
//! competitor scripts (`benchmarks/scripts/run_postgres_writes.sh`,
//! `run_sqlite3_writes.sh`, `run_duckdb_writes.sh`), each of which
//! drives the engine through its native SQL client. Per-engine raw
//! JSON files share the same schema (see
//! `crates/ultrasql-bench/src/bin/results_render.rs`).
//!
//! Output JSON shape:
//!
//! ```json
//! {
//!   "engine": "ultrasql",
//!   "workload": "insert_throughput_10k",
//!   "n_rows": 10000,
//!   "samples": 8,
//!   "median_us": <float>,
//!   "min_us": <float>,
//!   "iterations_us": [<float>, ...]
//! }
//! ```

#![allow(clippy::print_stdout)]
#![allow(clippy::print_stderr)]
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::cast_lossless
)]
// Legacy per-iter OLAP helpers `run_select_iter` / `run_sum_iter` /
// `run_avg_iter` / `run_filter_sum_iter` are retained for the
// historical "fresh table per iter" measurement mode. The current
// `--workload` dispatch routes every OLAP path through the
// shared-table helpers `run_shared_select_scan` /
// `run_shared_olap_aggregate` to match the per-engine competitor
// scripts. Keep the older functions compiling so a follow-on
// `--mode legacy-per-iter` flag can flip back if a reviewer needs
// the old shape.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use clap::{Parser, ValueEnum};
use parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};
use sha2::{Digest, Sha256};
use tokio_postgres::NoTls;
use ultrasql_bench::registry::HostInfo;
use ultrasql_catalog::rag::{RagSchemaConfig, create_rag_table_statements};
use ultrasql_objectstore::{object_range_cache_metrics, override_s3_endpoint_for_process};
use ultrasql_server::{Server, bind_listener, serve_listener};

const VECTOR_CERTIFICATION_METRICS: &[&str] = &[
    "recall_at_k",
    "p50_latency_us",
    "p95_latency_us",
    "p99_latency_us",
    "build_time_us",
    "memory_bytes",
    "index_size_bytes",
];
const PARQUET_SMOKE_REQUIRED_METRICS: &[&str] = &[
    "scan_us",
    "projection_pushdown_us",
    "predicate_pushdown_us",
    "row_group_pruning_us",
];
const OBJECT_PARQUET_RANGE_REQUIRED_METRICS: &[&str] = &[
    "query_median_us",
    "p50_latency_us",
    "p95_latency_us",
    "p99_latency_us",
    "range_request_count",
    "remote_bytes",
    "cache_hits",
    "cache_misses",
    "whole_object_fetched",
    "projected_out_column_fetched",
];
const HYBRID_SEARCH_REQUIRED_METRICS: &[&str] = &[
    "recall_at_k",
    "p50_latency_us",
    "p95_latency_us",
    "p99_latency_us",
    "filter_selectivity",
    "bm25_score",
    "vector_score",
];
const RAG_RETRIEVAL_REQUIRED_METRICS: &[&str] = &[
    "expected_doc_ids",
    "observed_doc_ids",
    "recall_at_k",
    "precision_at_k",
    "mrr",
    "latency_us",
    "answer_citation_coverage",
];
const COLD_START_INDEX_LOAD_REQUIRED_METRICS: &[&str] = &[
    "restart_time_us",
    "first_query_us",
    "second_query_us",
    "index_loaded_from_disk",
];
const INGESTION_THROUGHPUT_REQUIRED_METRICS: &[&str] =
    &["rows_per_sec", "wal_bytes", "index_update_us", "commit_us"];
const PARQUET_SMOKE_ROW_GROUP_ROWS: usize = 4_096;
const PARQUET_SMOKE_MIN_ROWS: usize = PARQUET_SMOKE_ROW_GROUP_ROWS * 2;

#[derive(Debug, Clone)]
struct VectorTopKCertification {
    answer: String,
    build_time_us: f64,
}

#[derive(Debug, Clone)]
struct ParquetSmokeMetrics {
    rows: usize,
    scan_us: f64,
    projection_pushdown_us: f64,
    predicate_pushdown_us: f64,
    row_group_pruning_us: f64,
    scan_samples_us: Vec<f64>,
    projection_pushdown_samples_us: Vec<f64>,
    predicate_pushdown_samples_us: Vec<f64>,
    row_group_pruning_samples_us: Vec<f64>,
    answer: serde_json::Value,
}

#[derive(Debug, Clone)]
struct ObjectParquetRangeMetrics {
    query_median_us: f64,
    samples_us: Vec<f64>,
    answer: serde_json::Value,
    object_bytes: usize,
    range_request_count: usize,
    requested_range_bytes: u64,
    remote_bytes: u64,
    cache_hits: u64,
    cache_misses: u64,
    length_probe_seen: bool,
    whole_object_fetched: bool,
    projected_out_column_fetched: bool,
    requests: Vec<serde_json::Value>,
}

#[derive(Debug, Clone)]
struct HybridSearchCertification {
    expected_ids: String,
    observed_ids: String,
    recall_at_k: f64,
    filter_selectivity: f64,
}

#[derive(Debug, Clone)]
struct RagRetrievalCertification {
    expected_doc_ids: Vec<String>,
    observed_doc_ids: Vec<String>,
    expected_chunks: Vec<String>,
    observed_chunks: Vec<String>,
    recall_at_k: f64,
    precision_at_k: f64,
    mrr: f64,
    answer_citation_coverage: f64,
}

#[derive(Debug)]
struct PersistentBenchServer {
    bound: SocketAddr,
    handle: tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
}

#[derive(Debug, Clone)]
struct TimedQueryMetric {
    median_us: f64,
    samples_us: Vec<f64>,
    rows: Vec<Vec<String>>,
}

/// Workload selector. New workloads will be added as the wire
/// pipeline grows to cover more shapes.
#[derive(Copy, Clone, Eq, PartialEq, Debug, ValueEnum)]
enum Workload {
    /// Bulk INSERT of `--rows` (id INT, val INT) tuples through one
    /// multi-row VALUES clause.
    InsertBulk,
    /// Full sequential scan over a freshly-loaded relation; reads
    /// every row through the wire as text-format `DataRow` messages.
    SelectScan,
    /// Whole-relation `SELECT SUM(x) FROM t` analytical aggregate over
    /// a preloaded (id INT, x INT) table. The aggregate result is a
    /// single row; the measurement isolates scan + hash-aggregate cost.
    SumScalar,
    /// Whole-relation `SELECT AVG(x) FROM t` analytical aggregate.
    AvgScalar,
    /// Filtered analytical aggregate
    /// `SELECT SUM(x) FROM t WHERE x > <threshold>`.
    FilterSum,
    /// Wide-table payload projection behind a selective indexed filter:
    /// `SELECT amount, pad_c FROM t WHERE tenant_id = ?`. This is the
    /// UltraSQL late-materialization smoke workload for Firebolt-style
    /// wide fact-table filter/projection queries.
    LateMaterialization,
    /// Repeated dashboard-style filtered `GROUP BY` aggregate over a
    /// preloaded fact table:
    /// `SELECT tenant_id, bucket, SUM(amount), COUNT(*) ... GROUP BY
    /// tenant_id, bucket`. This mirrors Firebolt's documented
    /// aggregating-index sweet spot and gives UltraSQL a stable exact
    /// baseline artifact for that competitor class.
    DashboardAggregate,
    /// Sparse-pruning analytical workload over correlated keys:
    /// `WHERE event_day BETWEEN a AND b AND tenant_id = ? GROUP BY
    /// event_day, tenant_id, bucket`. This mirrors Firebolt primary-index
    /// sparse granule pruning without claiming UltraSQL has that index.
    SparsePruning,
    /// Bulk UPDATE: preload `--rows` (id INT, val INT) tuples, then
    /// time one `UPDATE bench_update_{ix} SET val = val + 1 WHERE
    /// id < <rows>`. Matches the shape of
    /// `benchmarks/scripts/run_postgres_writes.sh::run_update`.
    UpdateBulk,
    /// Bulk DELETE: preload `--rows` (id INT, val INT) tuples, then
    /// time one `DELETE FROM bench_delete_{ix} WHERE id < <rows>`.
    /// Matches the shape of
    /// `benchmarks/scripts/run_postgres_writes.sh::run_delete`.
    DeleteBulk,
    /// Mixed OLTP pgbench-like — preload `--rows` (id INT, val INT)
    /// tuples, then run a 1-second window of 50% point reads (SELECT
    /// val WHERE id=?), 30% point updates (UPDATE SET val=val+1 WHERE
    /// id=?), 20% inserts (INSERT VALUES (next_id, ?)). Reports
    /// microseconds per operation (elapsed / op_count) — matches the
    /// shape of `benchmarks/scripts/run_*_writes.sh::run_mixed`.
    MixedOltp,
    /// Deterministic mixed write/read correctness workload. Preloads a
    /// table, then each sample runs UPDATE + INSERT and an aggregate
    /// inside a transaction. The transaction is rolled back after timing
    /// so every sample starts from the same image. The returned rows are
    /// hashed and compared across engines by the release renderer before
    /// ranking.
    MixedCorrectness,
    /// Whole-relation `SELECT id, row_number() OVER (ORDER BY x) FROM
    /// t` over a preloaded `(id INT, x INT)` table. Exercises the
    /// `LogicalPlan::Window` → `WindowAgg` wire end-to-end against the
    /// equivalent built-in on every competitor (PostgreSQL 17 native
    /// `row_number()`, DuckDB native, SQLite 3.25+ native, ClickHouse
    /// `rowNumberInAllBlocks()`). Drains every row through the wire.
    WindowRowNumber,
    /// Exact vector top-k nearest-neighbor query over a preloaded
    /// `(id INT, embedding VECTOR(d))` table:
    /// `ORDER BY embedding <-> probe, id LIMIT k`.
    VectorTopK,
    /// First `COUNT(*)` over `read_csv(path)` in a fresh in-process
    /// server. This is a first-touch engine measurement; it does not
    /// forcibly evict the host OS page cache.
    CsvColdRead,
    /// Repeated `COUNT(*)` over `read_csv(path)` after warmup reads.
    CsvWarmRead,
    /// `COPY t FROM 'file.csv' WITH (FORMAT csv, HEADER true)` into a
    /// fresh table per iteration.
    CsvCopyImport,
    /// `GROUP BY` directly over `read_csv(path)`.
    CsvGroupBy,
    /// Filter predicate directly over `read_csv(path)`.
    CsvFilter,
    /// Join `read_csv(path)` to a preloaded catalog table.
    CsvJoinTable,
    /// Malformed CSV ingestion through `COPY ... IGNORE_ERRORS` into a
    /// reject table.
    CsvMalformedBehavior,
    /// UltraSQL Parquet arena smoke. Generates a deterministic Parquet
    /// file with multiple row groups, then measures full scan,
    /// projection pushdown, predicate pushdown, and row-group pruning
    /// through `read_parquet`.
    ParquetSmoke,
    /// Object-store Parquet smoke. Serves a generated Parquet object
    /// from a local S3-compatible range-only endpoint, then verifies
    /// `read_parquet` uses ranged GETs for footer and selected chunks.
    ObjectParquetRange,
    /// Hybrid SQL search smoke. Times
    /// `ORDER BY hybrid_search(text, query, embedding, probe) DESC LIMIT k`
    /// with a metadata filter and validates expected top-k ids.
    HybridSearchLatency,
    /// RAG retrieval quality smoke. Creates the RAG helper schema,
    /// runs tenant-filtered exact vector retrieval, and records
    /// recall/precision/MRR/citation coverage.
    RagRetrievalQuality,
    /// Persistent HNSW cold-start smoke. Builds a page-backed ANN index,
    /// restarts the SQL server, then times the first and second top-k
    /// queries and verifies the page-backed index is used after restart.
    ColdStartIndexLoad,
    /// Vector ingestion smoke. Inserts deterministic vector batches into
    /// persistent tables with and without a pre-created ANN index, measuring
    /// throughput, WAL growth, index-maintenance delta, and commit latency.
    IngestionThroughput,
}

impl Workload {
    fn registry_id(self, n_rows: usize) -> String {
        self.registry_id_with_shape(n_rows, DEFAULT_VECTOR_DIMS, DEFAULT_TOP_K)
    }

    fn registry_id_with_shape(self, n_rows: usize, vector_dims: usize, top_k: usize) -> String {
        match self {
            Self::InsertBulk => format!("insert_throughput_{}", k_or_raw(n_rows)),
            Self::SelectScan => format!("select_scan_{}", k_or_raw(n_rows)),
            Self::SumScalar => format!("select_sum_{}_i64", k_or_raw(n_rows)),
            Self::AvgScalar => format!("select_avg_{}_i64", k_or_raw(n_rows)),
            Self::FilterSum => format!("filter_sum_{}_i64", k_or_raw(n_rows)),
            Self::LateMaterialization => {
                format!("late_materialization_{}", k_or_raw(n_rows))
            }
            Self::DashboardAggregate => {
                format!("firebolt_aggregate_index_{}", k_or_raw(n_rows))
            }
            Self::SparsePruning => format!("firebolt_sparse_pruning_{}", k_or_raw(n_rows)),
            Self::UpdateBulk => format!("update_throughput_{}", k_or_raw(n_rows)),
            Self::DeleteBulk => format!("delete_throughput_{}", k_or_raw(n_rows)),
            // The competitor scripts hard-code the id without a row-
            // count suffix; matching ID keeps results-render's grouping
            // happy.
            Self::MixedOltp => "mixed_oltp_pgbench_like".to_string(),
            Self::MixedCorrectness => format!("mixed_correctness_{}", k_or_raw(n_rows)),
            Self::WindowRowNumber => format!("window_row_number_{}_i64", k_or_raw(n_rows)),
            Self::VectorTopK => {
                format!(
                    "vector_topk_exact_{}_{}d_k{}",
                    k_or_raw(n_rows),
                    vector_dims,
                    top_k
                )
            }
            Self::CsvColdRead => format!("csv_cold_read_{}", k_or_raw(n_rows)),
            Self::CsvWarmRead => format!("csv_warm_read_{}", k_or_raw(n_rows)),
            Self::CsvCopyImport => format!("csv_copy_import_{}", k_or_raw(n_rows)),
            Self::CsvGroupBy => format!("csv_group_by_{}", k_or_raw(n_rows)),
            Self::CsvFilter => format!("csv_filter_{}", k_or_raw(n_rows)),
            Self::CsvJoinTable => format!("csv_join_table_{}", k_or_raw(n_rows)),
            Self::CsvMalformedBehavior => format!("csv_malformed_behavior_{}", k_or_raw(n_rows)),
            Self::ParquetSmoke => "arena_parquet_smoke".to_string(),
            Self::ObjectParquetRange => "object_parquet_range_smoke".to_string(),
            Self::HybridSearchLatency => "ai_gauntlet_hybrid_search_latency_smoke".to_string(),
            Self::RagRetrievalQuality => "ai_gauntlet_rag_retrieval_quality_smoke".to_string(),
            Self::ColdStartIndexLoad => "ai_gauntlet_cold_start_index_load_smoke".to_string(),
            Self::IngestionThroughput => "ai_gauntlet_ingestion_throughput_smoke".to_string(),
        }
    }
}

const DEFAULT_VECTOR_DIMS: usize = 8;
const DEFAULT_TOP_K: usize = 10;

/// Render a row count using `10k` / `1m` notation matching the
/// existing competitor workload ids (`insert_throughput_10k`,
/// `select_sum_65k_i64`).
fn k_or_raw(n: usize) -> String {
    if n >= 1_000_000 && n % 1_000_000 == 0 {
        format!("{}m", n / 1_000_000)
    } else if n >= 1_000 && n % 1_000 == 0 {
        format!("{}k", n / 1_000)
    } else if n == 65_536 {
        // The competitor scripts label `2^16` rows as `65k` even
        // though the exact count is 65 536. Match their workload-id
        // string so the `results-render` table groups them.
        "65k".to_string()
    } else {
        n.to_string()
    }
}

#[derive(Parser, Debug)]
#[command(
    name = "cross_compare_sql",
    about = "UltraSQL wire-protocol cross-engine benchmark driver"
)]
struct Args {
    /// Workload to run.
    #[arg(long, value_enum, default_value_t = Workload::InsertBulk)]
    workload: Workload,
    /// Number of rows in the data set.
    #[arg(long, default_value_t = 10_000)]
    rows: usize,
    /// Warmup iterations (not recorded).
    #[arg(long, default_value_t = 2)]
    warmup: usize,
    /// Measured iterations (median + min reported).
    #[arg(long, default_value_t = 8)]
    iters: usize,
    /// Output JSON file path. When omitted, the JSON is written to
    /// stdout (so the binary composes with `benchmarks/run.sh`).
    #[arg(long)]
    output: Option<PathBuf>,
    /// Explicit workload id override. When omitted, the id is
    /// derived from `--workload` + `--rows`, e.g.
    /// `insert_throughput_10k`.
    #[arg(long)]
    workload_id: Option<String>,
    /// Vector dimensions for the `vector-top-k` workload.
    #[arg(long, default_value_t = DEFAULT_VECTOR_DIMS)]
    vector_dims: usize,
    /// Number of nearest rows returned by the `vector-top-k` workload.
    #[arg(long, default_value_t = DEFAULT_TOP_K)]
    top_k: usize,
    /// CSV data file for `csv-*` benchmark workloads.
    #[arg(long)]
    csv_path: Option<PathBuf>,
    /// Malformed CSV data file for `csv-malformed-behavior`.
    #[arg(long)]
    csv_bad_path: Option<PathBuf>,
    /// Existing UltraSQL server address to benchmark instead of spawning the
    /// in-process harness server. Used by release-artifact certification.
    #[arg(long)]
    server: Option<SocketAddr>,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    let args = Args::parse();
    if args.vector_dims == 0 {
        anyhow::bail!("--vector-dims must be greater than zero");
    }
    if args.top_k == 0 {
        anyhow::bail!("--top-k must be greater than zero");
    }
    let workload_id = args.workload_id.clone().unwrap_or_else(|| {
        args.workload
            .registry_id_with_shape(args.rows, args.vector_dims, args.top_k)
    });
    if matches!(args.workload, Workload::ColdStartIndexLoad) {
        return run_cold_start_index_load_workload(&args, &workload_id).await;
    }
    if matches!(args.workload, Workload::IngestionThroughput) {
        return run_ingestion_throughput_workload(&args, &workload_id).await;
    }

    // Bring up an in-process ultrasqld on an ephemeral port unless an
    // already-running release artifact was supplied.
    let (bound, _server_thread) = if let Some(server) = args.server {
        (server, None)
    } else {
        let bind_addr: SocketAddr = "127.0.0.1:0".parse()?;
        let (listener, bound) = bind_listener(bind_addr).await.context("bind listener")?;
        let state = Arc::new(Server::with_sample_database());
        let handle = std::thread::Builder::new()
            .name("ultrasql-bench-server".to_string())
            .spawn(move || {
                let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                else {
                    eprintln!("ultrasqld task failed to build runtime");
                    return;
                };
                runtime.block_on(async move {
                    if let Err(e) = serve_listener(listener, state).await {
                        eprintln!("ultrasqld task exited: {e}");
                    }
                });
            })
            .context("spawn benchmark server thread")?;
        (bound, Some(handle))
    };

    // Run warmup + measured iterations.
    let mut iters_us: Vec<f64> = Vec::with_capacity(args.iters);
    let total_iters = args.warmup + args.iters;

    // Shared-table OLAP workloads: preload **once** outside the
    // timed region, then run the query N times against the same
    // relation. Matches the per-engine pattern every competitor
    // script uses (DuckDB / ClickHouse / SQLite / PostgreSQL all
    // build the relation once via their persistent driver
    // connection and time the query repeated N times). Anything
    // else would compare cold-cache UltraSQL against warm-cache
    // peers.
    let mut answer: Option<serde_json::Value> = None;
    let mut vector_topk_certification = None;
    let mut parquet_smoke_metrics = None;
    let mut object_parquet_range_metrics = None;
    let mut hybrid_search_certification = None;
    let mut rag_retrieval_certification = None;
    match args.workload {
        Workload::SelectScan => {
            run_shared_select_scan(bound, args.rows, args.warmup, total_iters, &mut iters_us)
                .await?;
        }
        Workload::SumScalar => {
            run_shared_olap_aggregate(
                bound,
                args.rows,
                args.warmup,
                total_iters,
                &mut iters_us,
                "bench_sum_shared",
                |t| format!("SELECT SUM(x) FROM {t}"),
            )
            .await?;
        }
        Workload::AvgScalar => {
            run_shared_olap_aggregate(
                bound,
                args.rows,
                args.warmup,
                total_iters,
                &mut iters_us,
                "bench_avg_shared",
                |t| format!("SELECT AVG(x) FROM {t}"),
            )
            .await?;
        }
        Workload::FilterSum => {
            let threshold = args.rows / 2;
            run_shared_olap_aggregate(
                bound,
                args.rows,
                args.warmup,
                total_iters,
                &mut iters_us,
                "bench_filter_sum_shared",
                move |t| format!("SELECT SUM(x) FROM {t} WHERE x > {threshold}"),
            )
            .await?;
        }
        Workload::LateMaterialization => {
            answer = Some(
                run_shared_late_materialization(
                    bound,
                    args.rows,
                    args.warmup,
                    total_iters,
                    &mut iters_us,
                )
                .await?,
            );
        }
        Workload::DashboardAggregate => {
            answer = Some(
                run_shared_dashboard_aggregate(
                    bound,
                    args.rows,
                    args.warmup,
                    total_iters,
                    &mut iters_us,
                )
                .await?,
            );
        }
        Workload::SparsePruning => {
            answer = Some(
                run_shared_sparse_pruning(
                    bound,
                    args.rows,
                    args.warmup,
                    total_iters,
                    &mut iters_us,
                )
                .await?,
            );
        }
        Workload::WindowRowNumber => {
            run_shared_window_row_number(bound, args.rows, args.warmup, total_iters, &mut iters_us)
                .await?;
        }
        Workload::UpdateBulk => {
            run_shared_update(bound, args.rows, args.warmup, total_iters, &mut iters_us).await?;
        }
        Workload::MixedCorrectness => {
            answer = Some(
                run_shared_mixed_correctness(
                    bound,
                    args.rows,
                    args.warmup,
                    total_iters,
                    &mut iters_us,
                )
                .await?,
            );
        }
        Workload::VectorTopK => {
            let certification = run_shared_vector_topk(
                bound,
                args.rows,
                args.vector_dims,
                args.top_k,
                args.warmup,
                total_iters,
                &mut iters_us,
            )
            .await?;
            answer = Some(serde_json::json!(certification.answer.clone()));
            vector_topk_certification = Some(certification);
        }
        Workload::CsvColdRead => {
            let csv_path = required_csv_path(&args)?;
            let path_sql = sql_string(csv_path);
            answer = Some(
                run_csv_query_workload(
                    bound,
                    &format!("SELECT COUNT(*) FROM read_csv({path_sql})"),
                    args.warmup,
                    total_iters,
                    &mut iters_us,
                )
                .await?,
            );
        }
        Workload::CsvWarmRead => {
            let csv_path = required_csv_path(&args)?;
            let path_sql = sql_string(csv_path);
            answer = Some(
                run_csv_query_workload(
                    bound,
                    &format!("SELECT COUNT(*) FROM read_csv({path_sql})"),
                    args.warmup,
                    total_iters,
                    &mut iters_us,
                )
                .await?,
            );
        }
        Workload::CsvCopyImport => {
            let csv_path = required_csv_path(&args)?;
            answer = Some(
                run_csv_copy_import(bound, csv_path, args.warmup, total_iters, &mut iters_us)
                    .await?,
            );
        }
        Workload::CsvGroupBy => {
            let csv_path = required_csv_path(&args)?;
            let path_sql = sql_string(csv_path);
            answer = Some(
                run_csv_query_workload(
                    bound,
                    &format!(
                        "SELECT category, COUNT(*) FROM read_csv({path_sql}) \
                         GROUP BY category ORDER BY category"
                    ),
                    args.warmup,
                    total_iters,
                    &mut iters_us,
                )
                .await?,
            );
        }
        Workload::CsvFilter => {
            let csv_path = required_csv_path(&args)?;
            let path_sql = sql_string(csv_path);
            answer = Some(
                run_csv_query_workload(
                    bound,
                    &format!("SELECT COUNT(*) FROM read_csv({path_sql}) WHERE category = 'alpha'"),
                    args.warmup,
                    total_iters,
                    &mut iters_us,
                )
                .await?,
            );
        }
        Workload::CsvJoinTable => {
            let csv_path = required_csv_path(&args)?;
            answer = Some(
                run_csv_join_table(bound, csv_path, args.warmup, total_iters, &mut iters_us)
                    .await?,
            );
        }
        Workload::CsvMalformedBehavior => {
            let csv_path = args
                .csv_bad_path
                .as_ref()
                .or(args.csv_path.as_ref())
                .context("--csv-bad-path or --csv-path is required for csv-malformed-behavior")?;
            answer = Some(
                run_csv_malformed_behavior(
                    bound,
                    csv_path,
                    args.warmup,
                    total_iters,
                    &mut iters_us,
                )
                .await?,
            );
        }
        Workload::ParquetSmoke => {
            let metrics =
                run_parquet_smoke(bound, args.rows, args.warmup, total_iters, &mut iters_us)
                    .await?;
            answer = Some(metrics.answer.clone());
            parquet_smoke_metrics = Some(metrics);
        }
        Workload::ObjectParquetRange => {
            let metrics = run_object_parquet_range_smoke(
                bound,
                args.rows,
                args.warmup,
                total_iters,
                &mut iters_us,
            )
            .await?;
            answer = Some(metrics.answer.clone());
            object_parquet_range_metrics = Some(metrics);
        }
        Workload::HybridSearchLatency => {
            let certification = run_shared_hybrid_search_latency(
                bound,
                args.rows,
                args.top_k,
                args.warmup,
                total_iters,
                &mut iters_us,
            )
            .await?;
            answer = Some(serde_json::json!({
                "expected_ids": certification.expected_ids.clone(),
                "observed_ids": certification.observed_ids.clone(),
            }));
            hybrid_search_certification = Some(certification);
        }
        Workload::RagRetrievalQuality => {
            let certification = run_rag_retrieval_quality(
                bound,
                args.top_k,
                args.warmup,
                total_iters,
                &mut iters_us,
            )
            .await?;
            answer = Some(serde_json::json!({
                "expected_doc_ids": certification.expected_doc_ids.clone(),
                "observed_doc_ids": certification.observed_doc_ids.clone(),
                "expected_chunks": certification.expected_chunks.clone(),
                "observed_chunks": certification.observed_chunks.clone(),
            }));
            rag_retrieval_certification = Some(certification);
        }
        _ => {
            for i in 0..total_iters {
                let micros = match args.workload {
                    Workload::InsertBulk => run_insert_iter(bound, args.rows, i).await?,
                    Workload::DeleteBulk => run_delete_iter(bound, args.rows, i).await?,
                    Workload::MixedOltp => run_mixed_oltp_iter(bound, args.rows, i).await?,
                    Workload::SelectScan
                    | Workload::SumScalar
                    | Workload::AvgScalar
                    | Workload::FilterSum
                    | Workload::LateMaterialization
                    | Workload::DashboardAggregate
                    | Workload::SparsePruning
                    | Workload::WindowRowNumber
                    | Workload::UpdateBulk
                    | Workload::MixedCorrectness
                    | Workload::VectorTopK
                    | Workload::CsvColdRead
                    | Workload::CsvWarmRead
                    | Workload::CsvCopyImport
                    | Workload::CsvGroupBy
                    | Workload::CsvFilter
                    | Workload::CsvJoinTable
                    | Workload::CsvMalformedBehavior
                    | Workload::ParquetSmoke
                    | Workload::ObjectParquetRange
                    | Workload::HybridSearchLatency
                    | Workload::RagRetrievalQuality
                    | Workload::ColdStartIndexLoad
                    | Workload::IngestionThroughput => unreachable!("handled above"),
                };
                if i >= args.warmup {
                    iters_us.push(micros);
                }
            }
        }
    }

    // Compute median + min.
    ultrasql_bench::sort_f64_nan_last(&mut iters_us);
    let median_us = iters_us[iters_us.len() / 2];
    let min_us = iters_us[0];
    let p50_latency_us = percentile_nearest_rank(&iters_us, 0.50);
    let p95_latency_us = percentile_nearest_rank(&iters_us, 0.95);
    let p99_latency_us = percentile_nearest_rank(&iters_us, 0.99);

    let mut report = serde_json::json!({
        "schema_version": 1,
        "engine": "ultrasql",
        "workload": workload_id,
        "status": "measured",
        "n_rows": args.rows,
        "samples": iters_us.len(),
        "median_us": median_us,
        "min_us": min_us,
        "iterations_us": iters_us,
        "host": HostInfo::from_env(),
        "server_addr": bound.to_string(),
        "server_mode": if args.server.is_some() { "external" } else { "in_process" },
        "policy": "Raw measured samples only; no ranking or winner claim.",
    });
    if let Some(answer) = answer {
        let answer_hash = answer_sha256(&answer)?;
        report["answer"] = answer;
        report["answer_sha256"] = serde_json::json!(answer_hash);
    }
    if matches!(args.workload, Workload::LateMaterialization) {
        report["schema_version"] = serde_json::json!(1);
        report["suite"] = serde_json::json!("late_materialization");
        report["profile"] = serde_json::json!("smoke");
        report["status"] = serde_json::json!("measured");
        report["required_metrics"] = serde_json::json!([
            "median_us",
            "p50_latency_us",
            "p95_latency_us",
            "p99_latency_us",
            "explain_late_materialization",
            "eager_scan_median_us",
            "late_materialization_median_us",
            "rows"
        ]);
        report["p50_latency_us"] = serde_json::json!(p50_latency_us);
        report["p95_latency_us"] = serde_json::json!(p95_latency_us);
        report["p99_latency_us"] = serde_json::json!(p99_latency_us);
        report["policy"] = serde_json::json!(
            "Late-materialization smoke validates EXPLAIN counters before recording latency."
        );
    }
    if let Some(certification) = vector_topk_certification {
        report["status"] = serde_json::json!("measured");
        report["vector_dims"] = serde_json::json!(args.vector_dims);
        report["top_k"] = serde_json::json!(args.top_k);
        report["exact"] = serde_json::json!(true);
        report["metric"] = serde_json::json!("l2");
        report["required_metrics"] = serde_json::json!(VECTOR_CERTIFICATION_METRICS);
        report["recall_at_k"] = serde_json::json!(1.0);
        report["p50_latency_us"] = serde_json::json!(p50_latency_us);
        report["p95_latency_us"] = serde_json::json!(p95_latency_us);
        report["p99_latency_us"] = serde_json::json!(p99_latency_us);
        report["build_time_us"] = serde_json::json!(certification.build_time_us);
        report["build_time_scope"] = serde_json::json!("table_load_before_timed_query");
        report["memory_bytes"] = serde_json::Value::Null;
        report["memory_status"] = serde_json::json!("not_measured");
        report["index_size_bytes"] = serde_json::Value::Null;
        report["index_size_status"] = serde_json::json!("not_applicable_exact_scan");
    }
    if let Some(metrics) = parquet_smoke_metrics {
        report["schema_version"] = serde_json::json!(1);
        report["suite"] = serde_json::json!("parquet");
        report["profile"] = serde_json::json!("smoke");
        report["status"] = serde_json::json!("measured");
        report["n_rows"] = serde_json::json!(metrics.rows);
        report["required_metrics"] = serde_json::json!(PARQUET_SMOKE_REQUIRED_METRICS);
        report["scan_us"] = serde_json::json!(metrics.scan_us);
        report["projection_pushdown_us"] = serde_json::json!(metrics.projection_pushdown_us);
        report["predicate_pushdown_us"] = serde_json::json!(metrics.predicate_pushdown_us);
        report["row_group_pruning_us"] = serde_json::json!(metrics.row_group_pruning_us);
        report["scan_samples_us"] = serde_json::json!(metrics.scan_samples_us);
        report["projection_pushdown_samples_us"] =
            serde_json::json!(metrics.projection_pushdown_samples_us);
        report["predicate_pushdown_samples_us"] =
            serde_json::json!(metrics.predicate_pushdown_samples_us);
        report["row_group_pruning_samples_us"] =
            serde_json::json!(metrics.row_group_pruning_samples_us);
        report["row_group_rows"] = serde_json::json!(PARQUET_SMOKE_ROW_GROUP_ROWS);
        report["policy"] = serde_json::json!(
            "Artifact contains measured UltraSQL samples only; no cross-engine ranking."
        );
    }
    if let Some(metrics) = object_parquet_range_metrics {
        report["schema_version"] = serde_json::json!(1);
        report["suite"] = serde_json::json!("object_parquet_range");
        report["profile"] = serde_json::json!("smoke");
        report["status"] = serde_json::json!("measured");
        report["required_metrics"] = serde_json::json!(OBJECT_PARQUET_RANGE_REQUIRED_METRICS);
        report["query_median_us"] = serde_json::json!(metrics.query_median_us);
        report["p50_latency_us"] = serde_json::json!(p50_latency_us);
        report["p95_latency_us"] = serde_json::json!(p95_latency_us);
        report["p99_latency_us"] = serde_json::json!(p99_latency_us);
        report["query_samples_us"] = serde_json::json!(metrics.samples_us);
        report["object_bytes"] = serde_json::json!(metrics.object_bytes);
        report["range_request_count"] = serde_json::json!(metrics.range_request_count);
        report["requested_range_bytes"] = serde_json::json!(metrics.requested_range_bytes);
        report["remote_bytes"] = serde_json::json!(metrics.remote_bytes);
        report["cache_hits"] = serde_json::json!(metrics.cache_hits);
        report["cache_misses"] = serde_json::json!(metrics.cache_misses);
        report["length_probe_seen"] = serde_json::json!(metrics.length_probe_seen);
        report["whole_object_fetched"] = serde_json::json!(metrics.whole_object_fetched);
        report["projected_out_column"] = serde_json::json!("score");
        report["projected_out_column_fetched"] =
            serde_json::json!(metrics.projected_out_column_fetched);
        report["requests"] = serde_json::json!(metrics.requests);
        report["policy"] = serde_json::json!(
            "Artifact certifies ranged object-store Parquet execution; no cross-engine ranking."
        );
    }
    if let Some(certification) = hybrid_search_certification {
        report["schema_version"] = serde_json::json!(1);
        report["suite"] = serde_json::json!("hybrid_search_latency");
        report["profile"] = serde_json::json!("smoke");
        report["status"] = serde_json::json!("measured");
        report["required_metrics"] = serde_json::json!(HYBRID_SEARCH_REQUIRED_METRICS);
        report["top_k"] = serde_json::json!(args.top_k.clamp(1, 3));
        report["recall_at_k"] = serde_json::json!(certification.recall_at_k);
        report["p50_latency_us"] = serde_json::json!(p50_latency_us);
        report["p95_latency_us"] = serde_json::json!(p95_latency_us);
        report["p99_latency_us"] = serde_json::json!(p99_latency_us);
        report["filter_selectivity"] = serde_json::json!(certification.filter_selectivity);
        report["bm25_score"] = serde_json::json!("implicit lexical component");
        report["vector_score"] = serde_json::json!("implicit l2 component");
        report["policy"] = serde_json::json!(
            "Hybrid search artifact validates expected top-k ids before recording latency."
        );
    }
    if let Some(certification) = rag_retrieval_certification {
        report["schema_version"] = serde_json::json!(1);
        report["suite"] = serde_json::json!("rag_retrieval_quality");
        report["profile"] = serde_json::json!("smoke");
        report["status"] = serde_json::json!("measured");
        report["required_metrics"] = serde_json::json!(RAG_RETRIEVAL_REQUIRED_METRICS);
        report["top_k"] = serde_json::json!(certification.expected_chunks.len());
        report["expected_doc_ids"] = serde_json::json!(certification.expected_doc_ids);
        report["observed_doc_ids"] = serde_json::json!(certification.observed_doc_ids);
        report["recall_at_k"] = serde_json::json!(certification.recall_at_k);
        report["precision_at_k"] = serde_json::json!(certification.precision_at_k);
        report["mrr"] = serde_json::json!(certification.mrr);
        report["latency_us"] = serde_json::json!(p50_latency_us);
        report["p50_latency_us"] = serde_json::json!(p50_latency_us);
        report["p95_latency_us"] = serde_json::json!(p95_latency_us);
        report["p99_latency_us"] = serde_json::json!(p99_latency_us);
        report["answer_citation_coverage"] =
            serde_json::json!(certification.answer_citation_coverage);
        report["policy"] = serde_json::json!(
            "RAG retrieval quality artifact uses deterministic tenant-filtered expected chunks."
        );
    }
    let serialized = serde_json::to_string(&report)?;
    if let Some(path) = args.output.as_ref() {
        std::fs::write(path, &serialized).with_context(|| format!("write {}", path.display()))?;
        eprintln!("cross_compare_sql: wrote {}", path.display());
    } else {
        println!("{serialized}");
    }
    Ok(())
}

fn required_csv_path(args: &Args) -> Result<&PathBuf> {
    args.csv_path.as_ref().context("--csv-path is required")
}

fn percentile_nearest_rank(sorted_values: &[f64], percentile: f64) -> f64 {
    let rank = (sorted_values.len() as f64 * percentile).ceil() as usize;
    let idx = rank.saturating_sub(1).min(sorted_values.len() - 1);
    sorted_values[idx]
}

fn sql_string(path: &Path) -> String {
    format!("'{}'", path.display().to_string().replace('\'', "''"))
}

async fn connect_sql_server(
    server: SocketAddr,
) -> Result<(tokio_postgres::Client, tokio::task::JoinHandle<()>)> {
    let conn_str = format!("host=127.0.0.1 port={} user=ultrasql_bench", server.port());
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

fn simple_query_rows(messages: &[tokio_postgres::SimpleQueryMessage]) -> Vec<Vec<String>> {
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

fn answer_sha256(answer: &serde_json::Value) -> Result<String> {
    let bytes = serde_json::to_vec(answer).context("serialize benchmark answer")?;
    let digest = Sha256::digest(&bytes);
    Ok(digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>())
}

async fn start_persistent_bench_server(data_dir: &Path) -> Result<PersistentBenchServer> {
    let bind_addr: SocketAddr = "127.0.0.1:0".parse()?;
    let server = Arc::new(Server::init(data_dir).context("persistent server init")?);
    let (listener, bound) = bind_listener(bind_addr).await.context("bind listener")?;
    let handle = tokio::spawn(serve_listener(listener, server));
    Ok(PersistentBenchServer { bound, handle })
}

async fn shutdown_persistent_bench_server(
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

async fn run_cold_start_index_load_workload(args: &Args, workload_id: &str) -> Result<()> {
    let dir = tempfile::tempdir().context("create cold-start tempdir")?;
    let table = "bench_ai_cold_start";
    let top_k = args.top_k.min(args.rows).max(1);

    let setup_server = start_persistent_bench_server(dir.path()).await?;
    let (setup_client, setup_conn) = connect_sql_server(setup_server.bound).await?;
    setup_client
        .batch_execute(&format!(
            "CREATE TABLE {table} (id INT NOT NULL, embedding VECTOR({dims}))",
            dims = args.vector_dims
        ))
        .await
        .with_context(|| format!("CREATE TABLE {table}"))?;
    preload_vector_chunked(&setup_client, table, args.rows, args.vector_dims).await?;
    setup_client
        .batch_execute(&format!(
            "CREATE INDEX {table}_embedding_hnsw \
             ON {table} USING hnsw (embedding vector_l2_ops)"
        ))
        .await
        .with_context(|| format!("CREATE INDEX {table}_embedding_hnsw"))?;
    let loaded = simple_count(&setup_client, &format!("SELECT COUNT(*) FROM {table}")).await?;
    if loaded != i64::try_from(args.rows).context("rows do not fit i64")? {
        anyhow::bail!(
            "cold-start preload count mismatch: expected {}, observed {loaded}",
            args.rows
        );
    }
    shutdown_persistent_bench_server(setup_client, setup_conn, setup_server).await;

    let restart_started = Instant::now();
    let query_server = start_persistent_bench_server(dir.path()).await?;
    let (client, conn_handle) = connect_sql_server(query_server.bound).await?;
    let restart_time_us = restart_started.elapsed().as_secs_f64() * 1e6;

    let probe = vector_probe_literal(args.vector_dims);
    let expected = expected_vector_topk_answer(args.rows, args.vector_dims, top_k);
    let query = format!(
        "SELECT id FROM {table} \
         ORDER BY embedding <-> VECTOR '{probe}' LIMIT {top_k}"
    );
    let (first_query_us, first_answer) = timed_vector_id_query(&client, &query).await?;
    if first_answer != expected {
        anyhow::bail!(
            "cold-start first query mismatch: expected ids {expected}, observed ids {first_answer}"
        );
    }
    let (second_query_us, second_answer) = timed_vector_id_query(&client, &query).await?;
    if second_answer != expected {
        anyhow::bail!(
            "cold-start second query mismatch: expected ids {expected}, observed ids {second_answer}"
        );
    }
    let explain = simple_query_rows(
        &client
            .simple_query(&format!("EXPLAIN ANALYZE {query}"))
            .await
            .context("cold-start explain analyze")?,
    )
    .into_iter()
    .flatten()
    .collect::<Vec<_>>()
    .join("\n");
    let index_loaded_from_disk = explain.contains("page-backed hnsw");
    shutdown_persistent_bench_server(client, conn_handle, query_server).await;

    let report = serde_json::json!({
        "schema_version": 1,
        "suite": "cold_start_index_load",
        "engine": "ultrasql",
        "workload": workload_id,
        "profile": "smoke",
        "status": "measured",
        "required_metrics": COLD_START_INDEX_LOAD_REQUIRED_METRICS,
        "n_rows": args.rows,
        "vector_dims": args.vector_dims,
        "top_k": top_k,
        "restart_time_us": restart_time_us,
        "first_query_us": first_query_us,
        "second_query_us": second_query_us,
        "index_loaded_from_disk": index_loaded_from_disk,
        "answer": {
            "expected_ids": expected,
            "first_ids": first_answer,
            "second_ids": second_answer,
        },
        "policy": "Cold-start artifact builds a persistent page-backed HNSW index, restarts the SQL server, and verifies the restarted query uses that index."
    });
    write_json_report(args.output.as_ref(), &report, "cross_compare_sql")
}

async fn timed_vector_id_query(
    client: &tokio_postgres::Client,
    query: &str,
) -> Result<(f64, String)> {
    let started = Instant::now();
    let messages = client
        .simple_query(query)
        .await
        .with_context(|| format!("vector id query: {query}"))?;
    let elapsed_us = started.elapsed().as_secs_f64() * 1e6;
    let answer = messages
        .iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0).map(ToOwned::to_owned),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join(",");
    Ok((elapsed_us, answer))
}

#[derive(Clone, Copy, Debug)]
struct VectorIngestTiming {
    insert_us: f64,
    commit_us: f64,
}

async fn run_ingestion_throughput_workload(args: &Args, workload_id: &str) -> Result<()> {
    let dir = tempfile::tempdir().context("create ingestion tempdir")?;
    let server = start_persistent_bench_server(dir.path()).await?;
    let (client, conn_handle) = connect_sql_server(server.bound).await?;
    let wal_before = directory_size_bytes(&dir.path().join("pg_wal"))?;
    let batch_size = 128_usize.min(args.rows.max(1));

    client
        .batch_execute(&format!(
            "CREATE TABLE bench_ai_ingest_plain (id INT NOT NULL, embedding VECTOR({dims}))",
            dims = args.vector_dims
        ))
        .await
        .context("create plain ingestion table")?;
    let plain = ingest_vector_batches(
        &client,
        "bench_ai_ingest_plain",
        args.rows,
        args.vector_dims,
        batch_size,
    )
    .await?;

    client
        .batch_execute(&format!(
            "CREATE TABLE bench_ai_ingest_indexed (id INT NOT NULL, embedding VECTOR({dims}))",
            dims = args.vector_dims
        ))
        .await
        .context("create indexed ingestion table")?;
    client
        .batch_execute(
            "CREATE INDEX bench_ai_ingest_indexed_embedding_hnsw \
             ON bench_ai_ingest_indexed USING hnsw (embedding vector_l2_ops)",
        )
        .await
        .context("create ingestion hnsw index")?;
    let indexed = ingest_vector_batches(
        &client,
        "bench_ai_ingest_indexed",
        args.rows,
        args.vector_dims,
        batch_size,
    )
    .await?;

    for table in ["bench_ai_ingest_plain", "bench_ai_ingest_indexed"] {
        let count = simple_count(&client, &format!("SELECT COUNT(*) FROM {table}")).await?;
        if count != i64::try_from(args.rows).context("rows do not fit i64")? {
            anyhow::bail!(
                "ingestion count mismatch for {table}: expected {}, observed {count}",
                args.rows
            );
        }
    }
    tokio::time::sleep(Duration::from_millis(20)).await;
    let wal_after = directory_size_bytes(&dir.path().join("pg_wal"))?;
    shutdown_persistent_bench_server(client, conn_handle, server).await;

    let indexed_total_us = indexed.insert_us + indexed.commit_us;
    let rows_per_sec = if indexed_total_us > 0.0 {
        args.rows as f64 * 1_000_000.0 / indexed_total_us
    } else {
        0.0
    };
    let plain_total_us = plain.insert_us + plain.commit_us;
    let rows_per_sec_without_index = if plain_total_us > 0.0 {
        args.rows as f64 * 1_000_000.0 / plain_total_us
    } else {
        0.0
    };
    let index_update_us = (indexed.insert_us - plain.insert_us).max(0.0);
    let wal_bytes = wal_after.saturating_sub(wal_before);

    let report = serde_json::json!({
        "schema_version": 1,
        "suite": "ingestion_throughput",
        "engine": "ultrasql",
        "workload": workload_id,
        "profile": "smoke",
        "status": "measured",
        "required_metrics": INGESTION_THROUGHPUT_REQUIRED_METRICS,
        "n_rows": args.rows,
        "vector_dims": args.vector_dims,
        "batch_size": batch_size,
        "ingest_path": "insert_batches",
        "rows_per_sec": rows_per_sec,
        "rows_per_sec_with_index": rows_per_sec,
        "rows_per_sec_without_index": rows_per_sec_without_index,
        "wal_bytes": wal_bytes,
        "index_update_us": index_update_us,
        "commit_us": indexed.commit_us,
        "commit_us_with_index": indexed.commit_us,
        "commit_us_without_index": plain.commit_us,
        "insert_us_with_index": indexed.insert_us,
        "insert_us_without_index": plain.insert_us,
        "policy": "Ingestion artifact inserts deterministic vector batches through SQL with and without a pre-created HNSW index; no cross-engine ranking."
    });
    write_json_report(args.output.as_ref(), &report, "cross_compare_sql")
}

async fn ingest_vector_batches(
    client: &tokio_postgres::Client,
    table: &str,
    n_rows: usize,
    dims: usize,
    batch_size: usize,
) -> Result<VectorIngestTiming> {
    client
        .batch_execute("BEGIN")
        .await
        .with_context(|| format!("BEGIN ingest for {table}"))?;
    let insert_started = Instant::now();
    let mut start = 0;
    while start < n_rows {
        let end = (start + batch_size).min(n_rows);
        let mut sql = String::with_capacity((end - start) * (dims * 4 + 32) + 64);
        sql.push_str("INSERT INTO ");
        sql.push_str(table);
        sql.push_str(" VALUES ");
        for row_id in start..end {
            if row_id > start {
                sql.push(',');
            }
            sql.push('(');
            sql.push_str(&row_id.to_string());
            sql.push(',');
            push_vector_literal_for_row(&mut sql, row_id, dims);
            sql.push(')');
        }
        client
            .batch_execute(&sql)
            .await
            .with_context(|| format!("ingest vector batch [{start}, {end}) into {table}"))?;
        start = end;
    }
    let insert_us = insert_started.elapsed().as_secs_f64() * 1e6;
    let commit_started = Instant::now();
    client
        .batch_execute("COMMIT")
        .await
        .with_context(|| format!("COMMIT ingest for {table}"))?;
    let commit_us = commit_started.elapsed().as_secs_f64() * 1e6;
    Ok(VectorIngestTiming {
        insert_us,
        commit_us,
    })
}

fn directory_size_bytes(path: &Path) -> Result<u64> {
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

fn write_json_report(
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

async fn simple_count(client: &tokio_postgres::Client, sql: &str) -> Result<i64> {
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

fn sorted_f64(mut values: Vec<f64>) -> Vec<f64> {
    ultrasql_bench::sort_f64_nan_last(&mut values);
    values
}

fn median_sorted(values: &[f64]) -> f64 {
    values[values.len() / 2]
}

async fn run_parquet_smoke(
    server: SocketAddr,
    requested_rows: usize,
    warmup: usize,
    total_iters: usize,
    iters_us: &mut Vec<f64>,
) -> Result<ParquetSmokeMetrics> {
    let rows = requested_rows.max(PARQUET_SMOKE_MIN_ROWS);
    let dir = tempfile::tempdir().context("create parquet smoke tempdir")?;
    let parquet_path = dir.path().join("arena_parquet_smoke.parquet");
    write_parquet_smoke_file(&parquet_path, rows)?;

    let (client, conn_handle) = connect_sql_server(server).await?;
    let path_sql = sql_string(&parquet_path);
    let pruning_threshold = rows / 2;
    let workloads = [
        (
            "scan",
            format!("SELECT COUNT(*) FROM read_parquet({path_sql})"),
        ),
        (
            "projection",
            format!("SELECT metric FROM read_parquet({path_sql})"),
        ),
        (
            "predicate",
            format!("SELECT COUNT(*) FROM read_parquet({path_sql}) WHERE category = 'alpha'"),
        ),
        (
            "row_group_pruning",
            format!(
                "SELECT COUNT(*) FROM read_parquet({path_sql}) WHERE id >= {pruning_threshold}"
            ),
        ),
    ];

    let scan = measure_simple_query(
        &client,
        workloads[0].0,
        &workloads[0].1,
        warmup,
        total_iters,
    )
    .await?;
    iters_us.extend(scan.samples_us.iter().copied());
    let projection = measure_simple_query(
        &client,
        workloads[1].0,
        &workloads[1].1,
        warmup,
        total_iters,
    )
    .await?;
    let predicate = measure_simple_query(
        &client,
        workloads[2].0,
        &workloads[2].1,
        warmup,
        total_iters,
    )
    .await?;
    let row_group_pruning = measure_simple_query(
        &client,
        workloads[3].0,
        &workloads[3].1,
        warmup,
        total_iters,
    )
    .await?;

    drop(client);
    conn_handle.abort();
    Ok(ParquetSmokeMetrics {
        rows,
        scan_us: scan.median_us,
        projection_pushdown_us: projection.median_us,
        predicate_pushdown_us: predicate.median_us,
        row_group_pruning_us: row_group_pruning.median_us,
        scan_samples_us: scan.samples_us,
        projection_pushdown_samples_us: projection.samples_us,
        predicate_pushdown_samples_us: predicate.samples_us,
        row_group_pruning_samples_us: row_group_pruning.samples_us,
        answer: serde_json::json!({
            "scan_rows": scan.rows,
            "projection_rows": projection.rows.len(),
            "predicate_rows": predicate.rows,
            "row_group_pruning_rows": row_group_pruning.rows,
            "row_group_pruning_threshold": pruning_threshold,
            "source": "generated_arrow_parquet_with_flushed_row_groups",
        }),
    })
}

async fn measure_simple_query(
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

fn write_parquet_smoke_file(path: &Path, rows: usize) -> Result<()> {
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int64, false),
        ArrowField::new("category", ArrowDataType::Utf8, false),
        ArrowField::new("metric", ArrowDataType::Int64, false),
    ]));
    let file = std::fs::File::create(path)
        .with_context(|| format!("create parquet smoke file {}", path.display()))?;
    let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), None)
        .with_context(|| format!("open parquet smoke writer {}", path.display()))?;
    for start in (0..rows).step_by(PARQUET_SMOKE_ROW_GROUP_ROWS) {
        let end = (start + PARQUET_SMOKE_ROW_GROUP_ROWS).min(rows);
        let ids = (start..end)
            .map(|row| i64::try_from(row).unwrap_or(i64::MAX))
            .collect::<Vec<_>>();
        let categories = (start..end)
            .map(|row| match row % 4 {
                0 => "alpha",
                1 => "beta",
                2 => "gamma",
                _ => "delta",
            })
            .collect::<Vec<_>>();
        let metrics = (start..end)
            .map(|row| i64::try_from(row.wrapping_mul(17) % 1_000).unwrap_or(0))
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(categories)),
                Arc::new(Int64Array::from(metrics)),
            ],
        )
        .context("build parquet smoke record batch")?;
        writer
            .write(&batch)
            .with_context(|| format!("write parquet smoke rows [{start}, {end})"))?;
        writer
            .flush()
            .with_context(|| format!("flush parquet smoke row group ending at {end}"))?;
    }
    writer
        .close()
        .with_context(|| format!("close parquet smoke file {}", path.display()))?;
    Ok(())
}

async fn run_object_parquet_range_smoke(
    server: SocketAddr,
    _requested_rows: usize,
    warmup: usize,
    total_iters: usize,
    iters_us: &mut Vec<f64>,
) -> Result<ObjectParquetRangeMetrics> {
    let rows = 3;
    let dir = tempfile::tempdir().context("create object parquet tempdir")?;
    let parquet_path = dir.path().join("object_range.parquet");
    write_object_range_parquet_file(&parquet_path, rows)?;
    let object_bytes = std::fs::read(&parquet_path)
        .with_context(|| format!("read object parquet {}", parquet_path.display()))?;
    let object_len = object_bytes.len();
    let whole_object_range = format!("bytes=0-{}", object_len.saturating_sub(1));
    let score_ranges = parquet_column_ranges(&parquet_path, "score")?;
    let mock = BenchMockS3::range_only(vec![(
        "/lake/parquet/object_range.parquet",
        object_bytes.clone(),
    )])?;
    let _endpoint_override = override_s3_endpoint_for_process(mock.endpoint.clone());

    let (client, conn_handle) = connect_sql_server(server).await?;
    let query =
        "SELECT name FROM read_parquet('s3://lake/parquet/object_range.parquet') WHERE id >= 100";
    let cache_metrics_before = object_range_cache_metrics();
    let timed =
        measure_simple_query(&client, "object_parquet_range", query, warmup, total_iters).await?;
    iters_us.extend(timed.samples_us.iter().copied());
    drop(client);
    conn_handle.abort();

    let object_requests = mock
        .requests()
        .into_iter()
        .filter(|request| request.path == "/lake/parquet/object_range.parquet")
        .collect::<Vec<_>>();
    if object_requests.is_empty() {
        anyhow::bail!("object Parquet range smoke made no object requests");
    }
    if object_requests
        .iter()
        .any(|request| request.range.is_none())
    {
        anyhow::bail!("object Parquet range smoke made a full-object request");
    }
    let length_probe_seen = object_requests
        .iter()
        .any(|request| request.range.as_deref() == Some("bytes=0-0"));
    if !length_probe_seen {
        anyhow::bail!("object Parquet range smoke did not issue bytes=0-0 length probe");
    }
    let whole_object_fetched = object_requests
        .iter()
        .any(|request| request.range.as_deref() == Some(whole_object_range.as_str()));
    if whole_object_fetched {
        anyhow::bail!("object Parquet range smoke fetched the whole object");
    }
    let projected_out_column_fetched = object_requests
        .iter()
        .any(|request| request_overlaps_any_range(request, &score_ranges));
    if projected_out_column_fetched {
        anyhow::bail!(
            "object Parquet range smoke fetched projected-out score column chunks: requests={object_requests:?} score_ranges={score_ranges:?}"
        );
    }
    let requested_range_bytes = object_requests
        .iter()
        .filter_map(|request| request.range.as_deref().and_then(request_range_bounds))
        .map(|(start, end)| end.saturating_sub(start).saturating_add(1))
        .sum();
    let requests = object_requests
        .iter()
        .map(|request| {
            serde_json::json!({
                "path": request.path,
                "range": request.range,
            })
        })
        .collect::<Vec<_>>();
    let cache_metrics_after = object_range_cache_metrics();

    Ok(ObjectParquetRangeMetrics {
        query_median_us: timed.median_us,
        samples_us: timed.samples_us,
        answer: serde_json::json!({
            "rows": timed.rows,
            "source": "local_s3_range_only_mock",
        }),
        object_bytes: object_len,
        range_request_count: object_requests.len(),
        requested_range_bytes,
        remote_bytes: cache_metrics_after
            .remote_bytes
            .saturating_sub(cache_metrics_before.remote_bytes),
        cache_hits: cache_metrics_after
            .cache_hits
            .saturating_sub(cache_metrics_before.cache_hits),
        cache_misses: cache_metrics_after
            .cache_misses
            .saturating_sub(cache_metrics_before.cache_misses),
        length_probe_seen,
        whole_object_fetched,
        projected_out_column_fetched,
        requests,
    })
}

fn write_object_range_parquet_file(path: &Path, _rows: usize) -> Result<()> {
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int64, false),
        ArrowField::new("name", ArrowDataType::Utf8, false),
        ArrowField::new("score", ArrowDataType::Int64, false),
    ]));
    let file = std::fs::File::create(path)
        .with_context(|| format!("create object range parquet {}", path.display()))?;
    let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), None)
        .with_context(|| format!("open object range parquet writer {}", path.display()))?;
    write_object_range_batch(
        &mut writer,
        Arc::clone(&schema),
        &[(10, "Alpha", 1), (20, "Beta", 2)],
    )?;
    writer
        .flush()
        .context("flush first object range row group")?;
    write_object_range_batch(&mut writer, schema, &[(100, "Zed", 99)])?;
    writer.close().context("close object range parquet")?;
    Ok(())
}

fn write_object_range_batch(
    writer: &mut ArrowWriter<std::fs::File>,
    schema: Arc<ArrowSchema>,
    rows: &[(i64, &str, i64)],
) -> Result<()> {
    let ids = rows.iter().map(|(id, _, _)| *id).collect::<Vec<_>>();
    let names = rows.iter().map(|(_, name, _)| *name).collect::<Vec<_>>();
    let scores = rows.iter().map(|(_, _, score)| *score).collect::<Vec<_>>();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(Int64Array::from(scores)),
        ],
    )
    .context("build object range parquet batch")?;
    writer
        .write(&batch)
        .context("write object range parquet row group")?;
    Ok(())
}

fn parquet_column_ranges(path: &Path, column: &str) -> Result<Vec<(u64, u64)>> {
    let file = std::fs::File::open(path)
        .with_context(|| format!("open parquet metadata {}", path.display()))?;
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(file).context("read parquet metadata")?;
    let column_index = builder
        .schema()
        .fields()
        .iter()
        .position(|field| field.name() == column)
        .with_context(|| format!("metadata column {column} missing"))?;
    Ok((0..builder.metadata().num_row_groups())
        .map(|row_group| {
            let (start, len) = builder
                .metadata()
                .row_group(row_group)
                .column(column_index)
                .byte_range();
            (start, start + len.saturating_sub(1))
        })
        .collect())
}

struct BenchMockS3 {
    endpoint: String,
    shutdown: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
    requests: Arc<Mutex<Vec<BenchMockS3Request>>>,
}

impl BenchMockS3 {
    fn range_only(objects: Vec<(&str, Vec<u8>)>) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").context("bind object range mock")?;
        listener
            .set_nonblocking(true)
            .context("object range mock nonblocking")?;
        let endpoint = format!(
            "http://{}",
            listener
                .local_addr()
                .context("read object range mock addr")?
        );
        let shutdown = Arc::new(AtomicBool::new(false));
        let thread_shutdown = Arc::clone(&shutdown);
        let requests = Arc::new(Mutex::new(Vec::new()));
        let thread_requests = Arc::clone(&requests);
        let objects = objects
            .into_iter()
            .map(|(path, body)| (path.to_owned(), body))
            .collect::<BTreeMap<_, _>>();
        let handle = thread::spawn(move || {
            while !thread_shutdown.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((mut stream, _addr)) => {
                        handle_bench_mock_s3_stream(&mut stream, &objects, &thread_requests);
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(_err) => break,
                }
            }
        });
        Ok(Self {
            endpoint,
            shutdown,
            handle: Some(handle),
            requests,
        })
    }

    fn requests(&self) -> Vec<BenchMockS3Request> {
        self.requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

impl Drop for BenchMockS3 {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        if let Some(addr) = self.endpoint.strip_prefix("http://") {
            let _ = TcpStream::connect(addr);
        }
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

#[derive(Clone, Debug)]
struct BenchMockS3Request {
    path: String,
    range: Option<String>,
}

fn handle_bench_mock_s3_stream(
    stream: &mut TcpStream,
    objects: &BTreeMap<String, Vec<u8>>,
    requests: &Arc<Mutex<Vec<BenchMockS3Request>>>,
) {
    let mut buf = [0_u8; 4096];
    let Ok(n) = stream.read(&mut buf) else {
        return;
    };
    let request = String::from_utf8_lossy(&buf[..n]);
    let range = header_value(&request, "range");
    let Some(target) = request
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
    else {
        return;
    };
    let (path, _query) = target.split_once('?').unwrap_or((target, ""));
    requests
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .push(BenchMockS3Request {
            path: path.to_owned(),
            range: range.clone(),
        });
    if let Some(body) = objects.get(path) {
        if let Some(range) = range {
            if let Some((start, end)) = parse_bytes_range(&range, body.len()) {
                write_mock_range_response(stream, body, start, end);
            } else {
                write_mock_response(stream, 416, "text/plain", b"bad range");
            }
        } else {
            write_mock_response(stream, 400, "text/plain", b"range required");
        }
    } else {
        write_mock_response(stream, 404, "text/plain", b"not found");
    }
}

fn header_value(request: &str, name: &str) -> Option<String> {
    let prefix = format!("{name}:");
    request.lines().find_map(|line| {
        line.to_ascii_lowercase()
            .strip_prefix(&prefix)
            .map(|_| line[prefix.len()..].trim().to_owned())
    })
}

fn parse_bytes_range(range: &str, len: usize) -> Option<(usize, usize)> {
    let range = range.strip_prefix("bytes=")?;
    let (start, end) = range.split_once('-')?;
    let start = start.parse::<usize>().ok()?;
    let end = end.parse::<usize>().ok()?.min(len.checked_sub(1)?);
    (start <= end && end < len).then_some((start, end))
}

fn write_mock_response(stream: &mut TcpStream, status: u16, content_type: &str, body: &[u8]) {
    let reason = match status {
        200 => "OK",
        206 => "Partial Content",
        400 => "Bad Request",
        404 => "Not Found",
        416 => "Range Not Satisfiable",
        _ => "Status",
    };
    let header = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let _ = stream.write_all(header.as_bytes());
    let _ = stream.write_all(body);
}

fn write_mock_range_response(stream: &mut TcpStream, body: &[u8], start: usize, end: usize) {
    let slice = &body[start..=end];
    let header = format!(
        "HTTP/1.1 206 Partial Content\r\nContent-Type: application/octet-stream\r\nContent-Length: {}\r\nContent-Range: bytes {start}-{end}/{}\r\nConnection: close\r\n\r\n",
        slice.len(),
        body.len()
    );
    let _ = stream.write_all(header.as_bytes());
    let _ = stream.write_all(slice);
}

fn request_overlaps_any_range(request: &BenchMockS3Request, ranges: &[(u64, u64)]) -> bool {
    let Some((start, end)) = request.range.as_deref().and_then(request_range_bounds) else {
        return false;
    };
    ranges
        .iter()
        .any(|(range_start, range_end)| start <= *range_end && end >= *range_start)
}

fn request_range_bounds(range: &str) -> Option<(u64, u64)> {
    let range = range.strip_prefix("bytes=")?;
    let (start, end) = range.split_once('-')?;
    Some((start.parse().ok()?, end.parse().ok()?))
}

async fn run_csv_query_workload(
    server: SocketAddr,
    query: &str,
    warmup: usize,
    total_iters: usize,
    iters_us: &mut Vec<f64>,
) -> Result<serde_json::Value> {
    let (client, conn_handle) = connect_sql_server(server).await?;
    let mut answer = serde_json::Value::Null;
    for i in 0..total_iters {
        let started = Instant::now();
        let messages = client
            .simple_query(query)
            .await
            .with_context(|| format!("CSV query workload: {query}"))?;
        let elapsed_us = started.elapsed().as_secs_f64() * 1e6;
        let rows = simple_query_rows(&messages);
        if rows.is_empty() {
            anyhow::bail!("CSV query returned no rows: {query}");
        }
        if i >= warmup {
            iters_us.push(elapsed_us);
            answer = serde_json::json!({
                "rows": rows,
                "cache_policy": concat!(
                    "cold_read is first measured read in a fresh UltraSQL server; ",
                    "host OS page cache is not forcibly dropped"
                ),
            });
        }
    }
    drop(client);
    conn_handle.abort();
    Ok(answer)
}

async fn run_csv_copy_import(
    server: SocketAddr,
    csv_path: &Path,
    warmup: usize,
    total_iters: usize,
    iters_us: &mut Vec<f64>,
) -> Result<serde_json::Value> {
    let (client, conn_handle) = connect_sql_server(server).await?;
    let path_sql = sql_string(csv_path);
    let mut answer = serde_json::Value::Null;
    for i in 0..total_iters {
        let table = format!("csv_copy_import_{i}");
        client
            .batch_execute(&format!(
                "CREATE TABLE {table} (id INT, category TEXT, metric INT, fact_dim TEXT)"
            ))
            .await
            .with_context(|| format!("CREATE TABLE {table}"))?;

        let copy_sql = format!(
            "COPY {table} FROM {path_sql} WITH (FORMAT csv, HEADER true, AUTO_DETECT true)"
        );
        let started = Instant::now();
        client
            .simple_query(&copy_sql)
            .await
            .with_context(|| format!("COPY CSV into {table}"))?;
        let elapsed_us = started.elapsed().as_secs_f64() * 1e6;
        let imported_rows = simple_count(&client, &format!("SELECT COUNT(*) FROM {table}")).await?;
        if i >= warmup {
            iters_us.push(elapsed_us);
            answer = serde_json::json!({ "imported_rows": imported_rows });
        }
    }
    drop(client);
    conn_handle.abort();
    Ok(answer)
}

async fn run_csv_join_table(
    server: SocketAddr,
    csv_path: &Path,
    warmup: usize,
    total_iters: usize,
    iters_us: &mut Vec<f64>,
) -> Result<serde_json::Value> {
    let (client, conn_handle) = connect_sql_server(server).await?;
    client
        .batch_execute(
            "CREATE TABLE csv_dim (dim_id TEXT, label TEXT);
             INSERT INTO csv_dim VALUES
             ('d0','zero'),('d1','one'),('d2','two'),('d3','three'),
             ('d4','four'),('d5','five'),('d6','six'),('d7','seven'),
             ('d8','eight'),('d9','nine'),('d10','ten'),('d11','eleven'),
             ('d12','twelve'),('d13','thirteen'),('d14','fourteen'),('d15','fifteen')",
        )
        .await
        .context("preload CSV join dimension table")?;
    let path_sql = sql_string(csv_path);
    let query =
        format!("SELECT COUNT(*) FROM read_csv({path_sql}) JOIN csv_dim ON fact_dim = dim_id");
    let mut answer = serde_json::Value::Null;
    for i in 0..total_iters {
        let started = Instant::now();
        let messages = client
            .simple_query(&query)
            .await
            .context("CSV join table workload")?;
        let elapsed_us = started.elapsed().as_secs_f64() * 1e6;
        let rows = simple_query_rows(&messages);
        if rows.is_empty() {
            anyhow::bail!("CSV join returned no rows");
        }
        if i >= warmup {
            iters_us.push(elapsed_us);
            answer = serde_json::json!({ "rows": rows });
        }
    }
    drop(client);
    conn_handle.abort();
    Ok(answer)
}

async fn run_csv_malformed_behavior(
    server: SocketAddr,
    csv_path: &Path,
    warmup: usize,
    total_iters: usize,
    iters_us: &mut Vec<f64>,
) -> Result<serde_json::Value> {
    let (client, conn_handle) = connect_sql_server(server).await?;
    let path_sql = sql_string(csv_path);
    let mut answer = serde_json::Value::Null;
    for i in 0..total_iters {
        let table = format!("csv_bad_import_{i}");
        let rejects = format!("csv_rejects_{i}");
        client
            .batch_execute(&format!(
                "CREATE TABLE {table} (id INT, category TEXT, metric INT, fact_dim TEXT)"
            ))
            .await
            .with_context(|| format!("create malformed CSV target table {table}"))?;
        client
            .batch_execute(&format!(
                "CREATE TABLE {rejects} (
                     filename TEXT,
                     line_number BIGINT,
                     raw_row TEXT,
                     error TEXT
                 )"
            ))
            .await
            .with_context(|| format!("create malformed CSV reject table {rejects}"))?;

        let copy_sql = format!(
            "COPY {table} FROM {path_sql} WITH \
             (FORMAT csv, HEADER true, IGNORE_ERRORS = true, MAX_ERRORS = 1000, \
              REJECT_TABLE = '{rejects}')"
        );
        let started = Instant::now();
        client
            .simple_query(&copy_sql)
            .await
            .with_context(|| format!("COPY malformed CSV into {table}"))?;
        let elapsed_us = started.elapsed().as_secs_f64() * 1e6;
        let accepted_rows = simple_count(&client, &format!("SELECT COUNT(*) FROM {table}")).await?;
        let rejected_rows =
            simple_count(&client, &format!("SELECT COUNT(*) FROM {rejects}")).await?;
        if i >= warmup {
            iters_us.push(elapsed_us);
            answer = serde_json::json!({
                "mode": "copy_ignore_errors",
                "accepted_rows": accepted_rows,
                "rejected_rows": rejected_rows,
                "max_errors": 1000,
            });
        }
    }
    drop(client);
    conn_handle.abort();
    Ok(answer)
}

/// Rows packed into each timed INSERT statement for the bulk-insert
/// benchmark. Competitor scripts use the same 10 000-row chunk size inside one
/// transaction. This keeps the workload at the SQL/client level without
/// turning a 1M-row load into 1 000 parser/round-trip cycles.
const INSERT_BENCH_CHUNK_ROWS: usize = 10_000;

/// Run one INSERT iteration: open a fresh wire connection, CREATE a
/// unique table, then insert rows in 10 000-row chunks inside one timed
/// transaction. The CREATE and SQL string construction are outside the timed
/// region.
async fn run_insert_iter(server: SocketAddr, n_rows: usize, ix: usize) -> Result<f64> {
    let conn_str = format!("host=127.0.0.1 port={} user=ultrasql_bench", server.port());
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .context("tokio-postgres connect to ultrasqld")?;
    let conn_handle = tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("tokio-postgres connection error: {e}");
        }
    });

    let table = format!("bench_insert_{ix}");
    client
        .batch_execute(&format!("CREATE TABLE {table} (id INT NOT NULL, val INT)"))
        .await
        .with_context(|| format!("CREATE TABLE {table}"))?;

    let mut chunks = Vec::with_capacity(n_rows.div_ceil(INSERT_BENCH_CHUNK_ROWS));
    let mut start = 0;
    while start < n_rows {
        let end = (start + INSERT_BENCH_CHUNK_ROWS).min(n_rows);
        let mut sql = String::with_capacity((end - start) * 16 + 64);
        sql.push_str("INSERT INTO ");
        sql.push_str(&table);
        sql.push_str(" VALUES ");
        for j in start..end {
            if j > start {
                sql.push(',');
            }
            sql.push('(');
            sql.push_str(&j.to_string());
            sql.push(',');
            sql.push_str(&(j * 10).to_string());
            sql.push(')');
        }
        chunks.push(sql);
        start = end;
    }

    let started = Instant::now();
    client
        .batch_execute("BEGIN")
        .await
        .context("BEGIN insert sample")?;
    for sql in &chunks {
        client
            .batch_execute(sql)
            .await
            .with_context(|| format!("INSERT chunk INTO {table}"))?;
    }
    client
        .batch_execute("COMMIT")
        .await
        .context("COMMIT insert sample")?;
    let elapsed_us = started.elapsed().as_secs_f64() * 1e6;

    drop(client);
    conn_handle.abort();
    Ok(elapsed_us)
}

/// Run one SELECT iteration: load `n_rows` into a fresh table
/// (outside the timed region), then time a full sequential scan that
/// drains every row over the wire.
async fn run_select_iter(server: SocketAddr, n_rows: usize, ix: usize) -> Result<f64> {
    let conn_str = format!("host=127.0.0.1 port={} user=ultrasql_bench", server.port());
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .context("tokio-postgres connect to ultrasqld")?;
    let conn_handle = tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("tokio-postgres connection error: {e}");
        }
    });

    let table = format!("bench_select_{ix}");
    client
        .batch_execute(&format!("CREATE TABLE {table} (id INT NOT NULL, val INT)"))
        .await
        .with_context(|| format!("CREATE TABLE {table}"))?;

    // Preload outside the timed region.
    let mut sql = String::with_capacity(n_rows * 16 + 64);
    sql.push_str("INSERT INTO ");
    sql.push_str(&table);
    sql.push_str(" VALUES ");
    for j in 0..n_rows {
        if j > 0 {
            sql.push(',');
        }
        sql.push('(');
        sql.push_str(&j.to_string());
        sql.push(',');
        sql.push_str(&(j * 10).to_string());
        sql.push(')');
    }
    client
        .batch_execute(&sql)
        .await
        .with_context(|| format!("preload INSERT into {table}"))?;

    // Use `simple_query` (Simple Query protocol) rather than `query`
    // (Extended Query Parse/Bind/Execute) — the server's Extended
    // Query dispatch lands in Wave 3. The text-format rows are still
    // fully decoded; we just count them as a sanity check.
    let started = Instant::now();
    let messages = client
        .simple_query(&format!("SELECT id, val FROM {table}"))
        .await
        .with_context(|| format!("SELECT from {table}"))?;
    let elapsed_us = started.elapsed().as_secs_f64() * 1e6;
    let row_count = messages
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count();
    if row_count != n_rows {
        anyhow::bail!("row count mismatch: expected {n_rows}, observed {row_count}");
    }

    drop(client);
    conn_handle.abort();
    Ok(elapsed_us)
}

/// Maximum rows packed into a single `INSERT ... VALUES (...)` statement
/// during preload. A 10 M row inline VALUES list would overrun
/// tokio-postgres' per-message budget and would also stress the
/// server's parser well past the workload under test; 50 000 rows per
/// chunk keeps each statement under a megabyte of SQL text.
const PRELOAD_CHUNK_ROWS: usize = 50_000;

/// Preload `n_rows` of `(id INT, x INT)` rows into `table` via a
/// sequence of multi-row INSERTs, chunked at [`PRELOAD_CHUNK_ROWS`]
/// rows per statement. `x` is set to `id` so analytical workloads
/// (`SUM(x)`, `AVG(x)`, `WHERE x > threshold`) hit non-trivial values.
///
/// Runs outside the timed region for every workload that uses it.
async fn preload_chunked(
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

/// Run one iteration of `SELECT SUM(x) FROM t`.
///
/// Loads `n_rows` of `(id INT, x INT)` outside the timed region, then
/// times a single whole-relation aggregate Simple-Query that returns
/// exactly one `DataRow` with the SUM result.
async fn run_sum_iter(server: SocketAddr, n_rows: usize, ix: usize) -> Result<f64> {
    let conn_str = format!("host=127.0.0.1 port={} user=ultrasql_bench", server.port());
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .context("tokio-postgres connect to ultrasqld")?;
    let conn_handle = tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("tokio-postgres connection error: {e}");
        }
    });

    let table = format!("bench_sum_{ix}");
    client
        .batch_execute(&format!("CREATE TABLE {table} (id INT NOT NULL, x INT)"))
        .await
        .with_context(|| format!("CREATE TABLE {table}"))?;
    preload_chunked(&client, &table, n_rows).await?;

    let started = Instant::now();
    let messages = client
        .simple_query(&format!("SELECT SUM(x) FROM {table}"))
        .await
        .with_context(|| format!("SELECT SUM from {table}"))?;
    let elapsed_us = started.elapsed().as_secs_f64() * 1e6;
    let row_count = messages
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count();
    if row_count != 1 {
        anyhow::bail!("SUM row count mismatch: expected 1, observed {row_count}");
    }

    drop(client);
    conn_handle.abort();
    Ok(elapsed_us)
}

/// Run one iteration of `SELECT AVG(x) FROM t`. See [`run_sum_iter`]
/// for the shape; only the aggregate function differs.
async fn run_avg_iter(server: SocketAddr, n_rows: usize, ix: usize) -> Result<f64> {
    let conn_str = format!("host=127.0.0.1 port={} user=ultrasql_bench", server.port());
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .context("tokio-postgres connect to ultrasqld")?;
    let conn_handle = tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("tokio-postgres connection error: {e}");
        }
    });

    let table = format!("bench_avg_{ix}");
    client
        .batch_execute(&format!("CREATE TABLE {table} (id INT NOT NULL, x INT)"))
        .await
        .with_context(|| format!("CREATE TABLE {table}"))?;
    preload_chunked(&client, &table, n_rows).await?;

    let started = Instant::now();
    let messages = client
        .simple_query(&format!("SELECT AVG(x) FROM {table}"))
        .await
        .with_context(|| format!("SELECT AVG from {table}"))?;
    let elapsed_us = started.elapsed().as_secs_f64() * 1e6;
    let row_count = messages
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count();
    if row_count != 1 {
        anyhow::bail!("AVG row count mismatch: expected 1, observed {row_count}");
    }

    drop(client);
    conn_handle.abort();
    Ok(elapsed_us)
}

/// Run one iteration of `SELECT SUM(x) FROM t WHERE x > <threshold>`.
/// Threshold is `n_rows / 2` so roughly half the rows survive the
/// predicate; this exercises `Filter` + `HashAggregate` on top of `SeqScan`.
async fn run_filter_sum_iter(server: SocketAddr, n_rows: usize, ix: usize) -> Result<f64> {
    let conn_str = format!("host=127.0.0.1 port={} user=ultrasql_bench", server.port());
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .context("tokio-postgres connect to ultrasqld")?;
    let conn_handle = tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("tokio-postgres connection error: {e}");
        }
    });

    let table = format!("bench_filter_sum_{ix}");
    client
        .batch_execute(&format!("CREATE TABLE {table} (id INT NOT NULL, x INT)"))
        .await
        .with_context(|| format!("CREATE TABLE {table}"))?;
    preload_chunked(&client, &table, n_rows).await?;

    let threshold = n_rows / 2;
    let started = Instant::now();
    let messages = client
        .simple_query(&format!("SELECT SUM(x) FROM {table} WHERE x > {threshold}"))
        .await
        .with_context(|| format!("SELECT SUM(...) WHERE from {table}"))?;
    let elapsed_us = started.elapsed().as_secs_f64() * 1e6;
    let row_count = messages
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count();
    if row_count != 1 {
        anyhow::bail!("FILTER SUM row count mismatch: expected 1, observed {row_count}");
    }

    drop(client);
    conn_handle.abort();
    Ok(elapsed_us)
}

/// Run one bulk-DELETE iteration. Preload happens outside the timed
/// region; only the SQL statement differs from the legacy UPDATE
/// single-iteration shape.
async fn run_delete_iter(server: SocketAddr, n_rows: usize, ix: usize) -> Result<f64> {
    let conn_str = format!("host=127.0.0.1 port={} user=ultrasql_bench", server.port());
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .context("tokio-postgres connect to ultrasqld")?;
    let conn_handle = tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("tokio-postgres connection error: {e}");
        }
    });

    let table = format!("bench_delete_{ix}");
    client
        .batch_execute(&format!("CREATE TABLE {table} (id INT NOT NULL, val INT)"))
        .await
        .with_context(|| format!("CREATE TABLE {table}"))?;
    preload_chunked(&client, &table, n_rows).await?;

    let started = Instant::now();
    let messages = client
        .simple_query(&format!("DELETE FROM {table} WHERE id < {n_rows}"))
        .await
        .with_context(|| format!("DELETE FROM {table}"))?;
    let elapsed_us = started.elapsed().as_secs_f64() * 1e6;
    if !messages
        .iter()
        .any(|m| matches!(m, tokio_postgres::SimpleQueryMessage::CommandComplete(_)))
    {
        anyhow::bail!("DELETE returned no CommandComplete message");
    }

    drop(client);
    conn_handle.abort();
    Ok(elapsed_us)
}

/// Shared-table SELECT-scan workload: preload `n_rows` once, then
/// drain `SELECT id, val FROM t` N times in a row on the same
/// relation (warmup + measured iters) under a single
/// `tokio-postgres` connection.
///
/// Matches the methodology every competitor script uses (the
/// preload is paid once outside the timed region, the persistent
/// driver connection runs N queries against the same materialised
/// relation). Mirrors `run_clickhouse_writes.sh::run_select_scan`.
async fn run_shared_select_scan(
    server: SocketAddr,
    n_rows: usize,
    warmup: usize,
    total_iters: usize,
    iters_us: &mut Vec<f64>,
) -> Result<()> {
    let conn_str = format!("host=127.0.0.1 port={} user=ultrasql_bench", server.port());
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .context("tokio-postgres connect to ultrasqld")?;
    let conn_handle = tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("tokio-postgres connection error: {e}");
        }
    });

    let table = "bench_select_scan_shared";
    client
        .batch_execute(&format!("CREATE TABLE {table} (id INT NOT NULL, val INT)"))
        .await
        .with_context(|| format!("CREATE TABLE {table}"))?;
    preload_chunked(&client, table, n_rows).await?;

    let query = format!("SELECT id, val FROM {table}");
    for i in 0..total_iters {
        let started = Instant::now();
        let messages = client
            .simple_query(&query)
            .await
            .with_context(|| format!("SELECT from {table}"))?;
        let elapsed_us = started.elapsed().as_secs_f64() * 1e6;
        let row_count = messages
            .iter()
            .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
            .count();
        if row_count != n_rows {
            anyhow::bail!("row count mismatch: expected {n_rows}, observed {row_count}");
        }
        if i >= warmup {
            iters_us.push(elapsed_us);
        }
    }

    drop(client);
    conn_handle.abort();
    Ok(())
}

/// Shared-table bulk UPDATE workload: preload `n_rows` once, then
/// time only `UPDATE t SET val = val + 1 WHERE id < n_rows` inside
/// a transaction and roll it back after the timed statement.
///
/// This matches the DuckDB and SQLite competitor runners: one
/// persistent driver connection, stable SQL text, identical starting
/// row image for every sample, and rollback outside the timed region.
/// It measures the UPDATE executor/wire round-trip rather than
/// per-sample table creation or cold parse/bind misses.
async fn run_shared_update(
    server: SocketAddr,
    n_rows: usize,
    warmup: usize,
    total_iters: usize,
    iters_us: &mut Vec<f64>,
) -> Result<()> {
    let conn_str = format!("host=127.0.0.1 port={} user=ultrasql_bench", server.port());
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .context("tokio-postgres connect to ultrasqld")?;
    let conn_handle = tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("tokio-postgres connection error: {e}");
        }
    });

    let table = "bench_update_shared";
    client
        .batch_execute(&format!("CREATE TABLE {table} (id INT NOT NULL, val INT)"))
        .await
        .with_context(|| format!("CREATE TABLE {table}"))?;
    preload_chunked(&client, table, n_rows).await?;

    let query = format!("UPDATE {table} SET val = val + 1 WHERE id < {n_rows}");
    for i in 0..total_iters {
        client
            .batch_execute("BEGIN")
            .await
            .context("BEGIN update sample")?;
        let started = Instant::now();
        let messages = client
            .simple_query(&query)
            .await
            .with_context(|| format!("UPDATE {table}"))?;
        let elapsed_us = started.elapsed().as_secs_f64() * 1e6;
        client
            .batch_execute("ROLLBACK")
            .await
            .context("ROLLBACK update sample")?;
        if !messages
            .iter()
            .any(|m| matches!(m, tokio_postgres::SimpleQueryMessage::CommandComplete(_)))
        {
            anyhow::bail!("UPDATE returned no CommandComplete message");
        }
        if i >= warmup {
            iters_us.push(elapsed_us);
        }
    }

    drop(client);
    conn_handle.abort();
    Ok(())
}

fn push_mixed_correctness_row(sql: &mut String, row_id: usize) {
    let val_mod = row_id.wrapping_mul(17) % 1_000;
    let val = i64::try_from(val_mod).unwrap_or(0) - 500;

    sql.push('(');
    sql.push_str(&row_id.to_string());
    sql.push(',');
    sql.push_str(&val.to_string());
    sql.push(')');
}

async fn preload_mixed_correctness_chunked(
    client: &tokio_postgres::Client,
    table: &str,
    n_rows: usize,
) -> Result<()> {
    let mut start = 0;
    while start < n_rows {
        let end = (start + PRELOAD_CHUNK_ROWS).min(n_rows);
        let mut sql = String::with_capacity((end - start) * 56 + 64);
        sql.push_str("INSERT INTO ");
        sql.push_str(table);
        sql.push_str(" VALUES ");
        for row_id in start..end {
            if row_id > start {
                sql.push(',');
            }
            push_mixed_correctness_row(&mut sql, row_id);
        }
        client.batch_execute(&sql).await.with_context(|| {
            format!("preload mixed-correctness chunk [{start}, {end}) INSERT into {table}")
        })?;
        start = end;
    }
    Ok(())
}

fn mixed_correctness_insert_sql(table: &str, n_rows: usize) -> String {
    let mut sql = String::with_capacity(96);
    sql.push_str("INSERT INTO ");
    sql.push_str(table);
    sql.push_str(" VALUES ");
    push_mixed_correctness_row(&mut sql, n_rows);
    sql
}

fn mixed_correctness_fact_query(table: &str) -> String {
    format!("SELECT SUM(val) FROM {table} WHERE id >= 0")
}

/// Shared mixed correctness workload: preload once, then each timed
/// sample mutates rows and runs a scalar aggregate inside a rolled-back
/// transaction. The answer rows are returned so release rendering can
/// reject cross-engine mismatches before ranking.
async fn run_shared_mixed_correctness(
    server: SocketAddr,
    n_rows: usize,
    warmup: usize,
    total_iters: usize,
    iters_us: &mut Vec<f64>,
) -> Result<serde_json::Value> {
    let (client, conn_handle) = connect_sql_server(server).await?;
    let fact_table = "bench_mixed_correctness_fact";
    let state_table = "bench_mixed_correctness_state";
    client
        .batch_execute(&format!(
            "CREATE TABLE {fact_table} (
                id INT NOT NULL,
                val INT NOT NULL
            )"
        ))
        .await
        .with_context(|| format!("CREATE TABLE {fact_table}"))?;
    client
        .batch_execute(&format!(
            "CREATE TABLE {state_table} (
                id INT NOT NULL,
                val INT NOT NULL
            )"
        ))
        .await
        .with_context(|| format!("CREATE TABLE {state_table}"))?;
    preload_mixed_correctness_chunked(&client, fact_table, n_rows).await?;
    preload_mixed_correctness_chunked(&client, state_table, 16).await?;

    let update_sql = format!("UPDATE {state_table} SET val = val + 7 WHERE id = 0");
    let insert_sql = mixed_correctness_insert_sql(state_table, n_rows);
    let fact_query = mixed_correctness_fact_query(fact_table);
    let batch_sql = format!("{insert_sql}; {update_sql}; {fact_query}");

    let mut answer_rows = Vec::new();
    for i in 0..total_iters {
        client
            .batch_execute("BEGIN")
            .await
            .context("BEGIN mixed-correctness sample")?;
        let started = Instant::now();
        let fact_messages = client
            .simple_query(&batch_sql)
            .await
            .with_context(|| format!("mixed-correctness batch on {state_table}/{fact_table}"))?;
        let elapsed_us = started.elapsed().as_secs_f64() * 1e6;
        client
            .batch_execute("ROLLBACK")
            .await
            .context("ROLLBACK mixed-correctness sample")?;
        let rows = simple_query_rows(&fact_messages);
        if rows.is_empty() {
            anyhow::bail!("mixed-correctness aggregate returned no rows");
        }
        if i >= warmup {
            iters_us.push(elapsed_us);
            answer_rows = rows;
        }
    }

    drop(client);
    conn_handle.abort();
    Ok(serde_json::json!(answer_rows))
}

/// Shared-table analytical aggregate workload: preload once, then
/// run `query_fn(table_name)` N times on the same `(id INT, x INT)`
/// relation under a single `tokio-postgres` connection. Drives
/// `SUM(x)`, `AVG(x)`, and `SUM(x) WHERE x > threshold` via a
/// caller-supplied closure that interpolates the table name.
async fn run_shared_olap_aggregate<F>(
    server: SocketAddr,
    n_rows: usize,
    warmup: usize,
    total_iters: usize,
    iters_us: &mut Vec<f64>,
    table: &str,
    query_fn: F,
) -> Result<()>
where
    F: Fn(&str) -> String,
{
    let conn_str = format!("host=127.0.0.1 port={} user=ultrasql_bench", server.port());
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .context("tokio-postgres connect to ultrasqld")?;
    let conn_handle = tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("tokio-postgres connection error: {e}");
        }
    });

    client
        .batch_execute(&format!("CREATE TABLE {table} (id INT NOT NULL, x INT)"))
        .await
        .with_context(|| format!("CREATE TABLE {table}"))?;
    preload_chunked(&client, table, n_rows).await?;

    let query = query_fn(table);
    for i in 0..total_iters {
        let started = Instant::now();
        let messages = client
            .simple_query(&query)
            .await
            .with_context(|| format!("aggregate on {table}"))?;
        let elapsed_us = started.elapsed().as_secs_f64() * 1e6;
        let row_count = messages
            .iter()
            .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
            .count();
        if row_count != 1 {
            anyhow::bail!("aggregate row count mismatch: expected 1, observed {row_count}");
        }
        if i >= warmup {
            iters_us.push(elapsed_us);
        }
    }

    drop(client);
    conn_handle.abort();
    Ok(())
}

const DASHBOARD_TENANTS: usize = 32;
const DASHBOARD_BUCKETS: usize = 64;
const DASHBOARD_FILTER_TENANT: usize = 7;
const LATE_MAT_WIDE_COLUMNS: usize = 100;
const LATE_MAT_TENANTS: usize = 32;
const LATE_MAT_BUCKETS: usize = 128;
const LATE_MAT_FILTER_TENANT: usize = 7;
const LATE_MAT_PAD_COLUMNS: usize = LATE_MAT_WIDE_COLUMNS - 4;
const SPARSE_ROWS_PER_DAY: usize = 256;
const SPARSE_TENANTS: usize = 64;
const SPARSE_BUCKETS: usize = 32;
const SPARSE_FILTER_TENANT: usize = 7;

fn late_materialization_table_ddl(table: &str) -> String {
    let mut sql = format!(
        "CREATE TABLE {table} (
                id INT NOT NULL,
                tenant_id INT NOT NULL,
                bucket INT NOT NULL,
                amount BIGINT NOT NULL"
    );
    for idx in 1..=LATE_MAT_PAD_COLUMNS {
        sql.push_str(&format!(", pad{idx:03} TEXT NOT NULL"));
    }
    sql.push(')');
    sql
}

fn late_materialization_query(table: &str) -> String {
    format!(
        "SELECT amount, pad003, pad096 FROM {table} \
         WHERE tenant_id = {LATE_MAT_FILTER_TENANT}"
    )
}

async fn preload_late_materialization_chunked(
    client: &tokio_postgres::Client,
    table: &str,
    n_rows: usize,
) -> Result<()> {
    let mut start = 0;
    while start < n_rows {
        let end = (start + PRELOAD_CHUNK_ROWS).min(n_rows);
        let mut sql = String::with_capacity((end - start) * 128 + 64);
        sql.push_str("INSERT INTO ");
        sql.push_str(table);
        sql.push_str(" VALUES ");
        for row_id in start..end {
            if row_id > start {
                sql.push(',');
            }
            let tenant_id = row_id % LATE_MAT_TENANTS;
            let bucket = (row_id / LATE_MAT_TENANTS) % LATE_MAT_BUCKETS;
            let amount_mod = row_id.wrapping_mul(19) % 2_000;
            let amount = i64::try_from(amount_mod).unwrap_or(0) - 1_000;

            sql.push('(');
            sql.push_str(&row_id.to_string());
            sql.push(',');
            sql.push_str(&tenant_id.to_string());
            sql.push(',');
            sql.push_str(&bucket.to_string());
            sql.push(',');
            sql.push_str(&amount.to_string());
            for pad_idx in 1..=LATE_MAT_PAD_COLUMNS {
                sql.push_str(",'p");
                sql.push_str(&pad_idx.to_string());
                sql.push('_');
                sql.push_str(&(row_id % (pad_idx + 17)).to_string());
                sql.push('\'');
            }
            sql.push(')');
        }
        client.batch_execute(&sql).await.with_context(|| {
            format!("preload late-materialization chunk [{start}, {end}) INSERT into {table}")
        })?;
        start = end;
    }
    Ok(())
}

async fn run_shared_late_materialization(
    server: SocketAddr,
    n_rows: usize,
    warmup: usize,
    total_iters: usize,
    iters_us: &mut Vec<f64>,
) -> Result<serde_json::Value> {
    let conn_str = format!("host=127.0.0.1 port={} user=ultrasql_bench", server.port());
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .context("tokio-postgres connect to ultrasqld")?;
    let conn_handle = tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("tokio-postgres connection error: {e}");
        }
    });

    let late_table = "bench_late_materialization_late";
    let eager_table = "bench_late_materialization_eager";
    client
        .batch_execute(&late_materialization_table_ddl(late_table))
        .await
        .with_context(|| format!("CREATE TABLE {late_table}"))?;
    client
        .batch_execute(&late_materialization_table_ddl(eager_table))
        .await
        .with_context(|| format!("CREATE TABLE {eager_table}"))?;
    preload_late_materialization_chunked(&client, late_table, n_rows).await?;
    preload_late_materialization_chunked(&client, eager_table, n_rows).await?;
    client
        .batch_execute(&format!(
            "CREATE INDEX ix_late_tenant ON {late_table}(tenant_id)"
        ))
        .await
        .with_context(|| format!("CREATE INDEX ix_late_tenant ON {late_table}"))?;

    let late_query = late_materialization_query(late_table);
    let eager_query = late_materialization_query(eager_table);
    let eager = measure_simple_query(
        &client,
        "late-materialization eager baseline",
        &eager_query,
        warmup,
        total_iters,
    )
    .await?;

    let explain_rows = simple_query_rows(
        &client
            .simple_query(&format!("EXPLAIN ANALYZE {late_query}"))
            .await
            .with_context(|| format!("EXPLAIN ANALYZE late materialization on {late_table}"))?,
    );
    let explain_text = explain_rows
        .iter()
        .filter_map(|row| row.first().cloned())
        .collect::<Vec<_>>();
    let late_line = explain_text
        .iter()
        .find(|line| line.starts_with("Late Materialization:"))
        .cloned()
        .context("EXPLAIN ANALYZE did not emit Late Materialization line")?;
    if !late_line.contains("candidates=")
        || !late_line.contains("fetched=")
        || !late_line.contains("skipped=")
    {
        anyhow::bail!("late materialization EXPLAIN line lacks counters: {late_line}");
    }

    let late = measure_simple_query(
        &client,
        "late-materialization indexed late path",
        &late_query,
        warmup,
        total_iters,
    )
    .await?;
    iters_us.extend(late.samples_us.iter().copied());
    let mut eager_rows = eager.rows.clone();
    let mut late_rows = late.rows.clone();
    eager_rows.sort();
    late_rows.sort();
    if late_rows != eager_rows {
        anyhow::bail!(
            "late materialization answer mismatch: eager={:?} late={:?}",
            eager.rows,
            late.rows
        );
    }

    drop(client);
    conn_handle.abort();
    Ok(serde_json::json!({
        "rows": late.rows.len(),
        "tenant_id": LATE_MAT_FILTER_TENANT,
        "wide_columns": LATE_MAT_WIDE_COLUMNS,
        "projected_columns": ["amount", "pad003", "pad096"],
        "query_shape": "wide_payload_projection_with_selective_index_filter",
        "firebolt_style_shape": "wide fact table, selective tenant filter, payload projection",
        "answer_order": "unordered rows sorted before eager/late equality check",
        "explain_late_materialization": late_line,
        "eager_scan_median_us": eager.median_us,
        "eager_scan_samples_us": eager.samples_us,
        "late_materialization_median_us": late.median_us,
        "late_materialization_samples_us": late.samples_us,
        "comparison_policy": "UltraSQL eager and late paths share deterministic rows and query; external competitor artifacts are recorded separately when installed."
    }))
}

async fn preload_dashboard_aggregate_chunked(
    client: &tokio_postgres::Client,
    table: &str,
    n_rows: usize,
) -> Result<()> {
    let mut start = 0;
    while start < n_rows {
        let end = (start + PRELOAD_CHUNK_ROWS).min(n_rows);
        let mut sql = String::with_capacity((end - start) * 40 + 64);
        sql.push_str("INSERT INTO ");
        sql.push_str(table);
        sql.push_str(" VALUES ");
        for row_id in start..end {
            if row_id > start {
                sql.push(',');
            }
            let tenant_id = row_id % DASHBOARD_TENANTS;
            let bucket = (row_id / DASHBOARD_TENANTS) % DASHBOARD_BUCKETS;
            let amount_mod = row_id.wrapping_mul(17) % 1_000;
            let amount = i64::try_from(amount_mod).unwrap_or(0) - 500;

            sql.push('(');
            sql.push_str(&row_id.to_string());
            sql.push(',');
            sql.push_str(&tenant_id.to_string());
            sql.push(',');
            sql.push_str(&bucket.to_string());
            sql.push(',');
            sql.push_str(&amount.to_string());
            sql.push(')');
        }
        client.batch_execute(&sql).await.with_context(|| {
            format!("preload dashboard chunk [{start}, {end}) INSERT into {table}")
        })?;
        start = end;
    }
    Ok(())
}

/// Shared-table dashboard aggregate workload: preload a deterministic
/// fact table once, then run the same filtered grouped aggregate many
/// times. The key order intentionally matches Firebolt's aggregating
/// index shape: filter on the first grouping column, group by the
/// indexed dimensions, and compute `SUM` + `COUNT(*)`.
async fn run_shared_dashboard_aggregate(
    server: SocketAddr,
    n_rows: usize,
    warmup: usize,
    total_iters: usize,
    iters_us: &mut Vec<f64>,
) -> Result<serde_json::Value> {
    let conn_str = format!("host=127.0.0.1 port={} user=ultrasql_bench", server.port());
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .context("tokio-postgres connect to ultrasqld")?;
    let conn_handle = tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("tokio-postgres connection error: {e}");
        }
    });

    let table = "bench_dashboard_aggregate_shared";
    client
        .batch_execute(&format!(
            "CREATE TABLE {table} (
                id INT NOT NULL,
                tenant_id INT NOT NULL,
                bucket INT NOT NULL,
                amount BIGINT NOT NULL
            )"
        ))
        .await
        .with_context(|| format!("CREATE TABLE {table}"))?;
    preload_dashboard_aggregate_chunked(&client, table, n_rows).await?;

    let index_ddl = format!(
        "CREATE AGGREGATING INDEX ix_dashboard_agg ON {table} \
         (tenant_id, bucket, SUM(amount), COUNT(*))"
    );
    client
        .batch_execute(&index_ddl)
        .await
        .with_context(|| format!("CREATE AGGREGATING INDEX ix_dashboard_agg ON {table}"))?;

    let query = format!(
        "SELECT tenant_id, bucket, SUM(amount), COUNT(*) \
         FROM {table} \
         WHERE tenant_id = {DASHBOARD_FILTER_TENANT} \
         GROUP BY tenant_id, bucket \
         ORDER BY tenant_id, bucket"
    );
    let explain_rows = simple_query_rows(
        &client
            .simple_query(&format!("EXPLAIN ANALYZE {query}"))
            .await
            .with_context(|| format!("EXPLAIN ANALYZE dashboard aggregate on {table}"))?,
    );
    let explain_aggregating_index = explain_rows
        .iter()
        .filter_map(|row| row.first().cloned())
        .find(|line| line.starts_with("Aggregating Index:"))
        .context("EXPLAIN ANALYZE did not emit Aggregating Index line")?;
    if !explain_aggregating_index.contains("aggregating_index_used=true") {
        anyhow::bail!(
            "dashboard aggregate did not use aggregating index: {explain_aggregating_index}"
        );
    }

    let mut answer_rows = Vec::new();
    for i in 0..total_iters {
        let started = Instant::now();
        let messages = client
            .simple_query(&query)
            .await
            .with_context(|| format!("dashboard aggregate on {table}"))?;
        let elapsed_us = started.elapsed().as_secs_f64() * 1e6;
        let rows = simple_query_rows(&messages);
        if rows.is_empty() {
            anyhow::bail!("dashboard aggregate returned no rows");
        }
        if i >= warmup {
            iters_us.push(elapsed_us);
            answer_rows = rows;
        }
    }

    drop(client);
    conn_handle.abort();
    Ok(serde_json::json!({
        "rows": answer_rows,
        "query_shape": "filtered_group_by_sum_count",
        "firebolt_index_shape": concat!(
            "CREATE AGGREGATING INDEX idx ON fact_events ",
            "(tenant_id, bucket, SUM(amount), COUNT(*))"
        ),
        "index_ddl": index_ddl,
        "aggregating_index_used": true,
        "explain_aggregating_index": explain_aggregating_index,
    }))
}

fn sparse_filter_days(n_rows: usize) -> (usize, usize) {
    let max_day = n_rows.saturating_sub(1) / SPARSE_ROWS_PER_DAY;
    let start = max_day.saturating_sub(2) / 2;
    let end = (start + 2).min(max_day);
    (start, end)
}

async fn preload_sparse_pruning_chunked(
    client: &tokio_postgres::Client,
    table: &str,
    n_rows: usize,
) -> Result<()> {
    let mut start = 0;
    while start < n_rows {
        let end = (start + PRELOAD_CHUNK_ROWS).min(n_rows);
        let mut sql = String::with_capacity((end - start) * 56 + 64);
        sql.push_str("INSERT INTO ");
        sql.push_str(table);
        sql.push_str(" VALUES ");
        for row_id in start..end {
            if row_id > start {
                sql.push(',');
            }
            let event_day = row_id / SPARSE_ROWS_PER_DAY;
            let tenant_id = ((event_day * 13) + (row_id / 8)) % SPARSE_TENANTS;
            let bucket = row_id % SPARSE_BUCKETS;
            let amount_mod = row_id.wrapping_mul(31) % 2_000;
            let amount = i64::try_from(amount_mod).unwrap_or(0) - 1_000;

            sql.push('(');
            sql.push_str(&row_id.to_string());
            sql.push(',');
            sql.push_str(&event_day.to_string());
            sql.push(',');
            sql.push_str(&tenant_id.to_string());
            sql.push(',');
            sql.push_str(&bucket.to_string());
            sql.push(',');
            sql.push_str(&amount.to_string());
            sql.push(')');
        }
        client.batch_execute(&sql).await.with_context(|| {
            format!("preload sparse-pruning chunk [{start}, {end}) INSERT into {table}")
        })?;
        start = end;
    }
    Ok(())
}

/// Shared-table sparse-pruning workload. UltraSQL runs this as an honest
/// heap-scan baseline; Firebolt's matching script uses `PRIMARY INDEX
/// event_day, tenant_id, bucket` to test sparse granule pruning.
async fn run_shared_sparse_pruning(
    server: SocketAddr,
    n_rows: usize,
    warmup: usize,
    total_iters: usize,
    iters_us: &mut Vec<f64>,
) -> Result<serde_json::Value> {
    let conn_str = format!("host=127.0.0.1 port={} user=ultrasql_bench", server.port());
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .context("tokio-postgres connect to ultrasqld")?;
    let conn_handle = tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("tokio-postgres connection error: {e}");
        }
    });

    let table = "bench_sparse_pruning_shared";
    client
        .batch_execute(&format!(
            "CREATE TABLE {table} (
                id INT NOT NULL,
                event_day INT NOT NULL,
                tenant_id INT NOT NULL,
                bucket INT NOT NULL,
                amount BIGINT NOT NULL
            )"
        ))
        .await
        .with_context(|| format!("CREATE TABLE {table}"))?;
    preload_sparse_pruning_chunked(&client, table, n_rows).await?;

    let (day_start, day_end) = sparse_filter_days(n_rows);
    let query = format!(
        "SELECT event_day, tenant_id, bucket, SUM(amount), COUNT(*) \
         FROM {table} \
         WHERE event_day BETWEEN {day_start} AND {day_end} \
           AND tenant_id = {SPARSE_FILTER_TENANT} \
         GROUP BY event_day, tenant_id, bucket \
         ORDER BY event_day, tenant_id, bucket"
    );
    let mut answer_rows = Vec::new();
    for i in 0..total_iters {
        let started = Instant::now();
        let messages = client
            .simple_query(&query)
            .await
            .with_context(|| format!("sparse pruning aggregate on {table}"))?;
        let elapsed_us = started.elapsed().as_secs_f64() * 1e6;
        let rows = simple_query_rows(&messages);
        if rows.is_empty() {
            anyhow::bail!("sparse pruning aggregate returned no rows");
        }
        if i >= warmup {
            iters_us.push(elapsed_us);
            answer_rows = rows;
        }
    }

    drop(client);
    conn_handle.abort();
    Ok(serde_json::json!({
        "rows": answer_rows,
        "query_shape": "correlated_key_range_filter_group_by_sum_count",
        "firebolt_index_shape": concat!(
            "CREATE FACT TABLE fact_events (...) PRIMARY INDEX ",
            "event_day, tenant_id, bucket"
        ),
        "event_day_start": day_start,
        "event_day_end": day_end,
        "tenant_id": SPARSE_FILTER_TENANT,
    }))
}

/// Shared-table window-function workload: preload `n_rows` once,
/// then drain `SELECT id, row_number() OVER (ORDER BY x) FROM t` N
/// times against the same `(id INT, x INT)` relation under a single
/// `tokio-postgres` connection.
///
/// Mirrors every competitor script's `run_window_row_number`. The
/// query covers the new v0.5 `LogicalPlan::Window` + `WindowAgg` wire
/// path end-to-end; each iteration drains every row through the
/// wire as `tokio_postgres::SimpleQueryMessage::Row`.
async fn run_shared_window_row_number(
    server: SocketAddr,
    n_rows: usize,
    warmup: usize,
    total_iters: usize,
    iters_us: &mut Vec<f64>,
) -> Result<()> {
    let conn_str = format!("host=127.0.0.1 port={} user=ultrasql_bench", server.port());
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .context("tokio-postgres connect to ultrasqld")?;
    let conn_handle = tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("tokio-postgres connection error: {e}");
        }
    });

    let table = "bench_window_row_number_shared";
    client
        .batch_execute(&format!("CREATE TABLE {table} (id INT NOT NULL, x INT)"))
        .await
        .with_context(|| format!("CREATE TABLE {table}"))?;
    preload_chunked(&client, table, n_rows).await?;

    let query = format!("SELECT id, row_number() OVER (ORDER BY x) FROM {table}");
    for i in 0..total_iters {
        let started = Instant::now();
        let messages = client
            .simple_query(&query)
            .await
            .with_context(|| format!("window row_number on {table}"))?;
        let elapsed_us = started.elapsed().as_secs_f64() * 1e6;
        let row_count = messages
            .iter()
            .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
            .count();
        if row_count != n_rows {
            anyhow::bail!(
                "window_row_number row count mismatch: expected {n_rows}, observed {row_count}"
            );
        }
        if i >= warmup {
            iters_us.push(elapsed_us);
        }
    }

    drop(client);
    conn_handle.abort();
    Ok(())
}

const VECTOR_PRELOAD_CHUNK_ROWS: usize = 1_000;

fn vector_component(row_id: usize, dim: usize) -> i32 {
    let row = row_id as u128;
    let dim = dim as u128;
    let value = ((row * 31) + (dim * 17) + ((row % 7) * 13)) % 101;
    i32::try_from(value).unwrap_or(0) - 50
}

fn vector_probe_component(dim: usize) -> i32 {
    let dim = dim as u128;
    let value = ((dim * 7) + 3) % 23;
    i32::try_from(value).unwrap_or(0) - 11
}

fn push_vector_literal_for_row(sql: &mut String, row_id: usize, dims: usize) {
    sql.push_str("'[");
    for dim in 0..dims {
        if dim > 0 {
            sql.push(',');
        }
        sql.push_str(&vector_component(row_id, dim).to_string());
    }
    sql.push_str("]'");
}

fn vector_probe_literal(dims: usize) -> String {
    let mut literal = String::with_capacity(dims * 4 + 2);
    literal.push('[');
    for dim in 0..dims {
        if dim > 0 {
            literal.push(',');
        }
        literal.push_str(&vector_probe_component(dim).to_string());
    }
    literal.push(']');
    literal
}

fn vector_l2_squared(row_id: usize, dims: usize) -> i64 {
    let mut sum = 0_i64;
    for dim in 0..dims {
        let delta = i64::from(vector_component(row_id, dim) - vector_probe_component(dim));
        sum += delta * delta;
    }
    sum
}

fn expected_vector_topk_answer(n_rows: usize, dims: usize, top_k: usize) -> String {
    let mut candidates = (0..n_rows)
        .map(|row_id| (vector_l2_squared(row_id, dims), row_id))
        .collect::<Vec<_>>();
    candidates.sort_unstable();
    candidates
        .into_iter()
        .take(top_k.min(n_rows))
        .map(|(_, row_id)| row_id.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

async fn preload_vector_chunked(
    client: &tokio_postgres::Client,
    table: &str,
    n_rows: usize,
    dims: usize,
) -> Result<()> {
    let mut start = 0;
    while start < n_rows {
        let end = (start + VECTOR_PRELOAD_CHUNK_ROWS).min(n_rows);
        let mut sql = String::with_capacity((end - start) * (dims * 4 + 32) + 64);
        sql.push_str("INSERT INTO ");
        sql.push_str(table);
        sql.push_str(" VALUES ");
        for row_id in start..end {
            if row_id > start {
                sql.push(',');
            }
            sql.push('(');
            sql.push_str(&row_id.to_string());
            sql.push(',');
            push_vector_literal_for_row(&mut sql, row_id, dims);
            sql.push(')');
        }
        client
            .batch_execute(&sql)
            .await
            .with_context(|| format!("preload vector chunk [{start}, {end}) into {table}"))?;
        start = end;
    }
    Ok(())
}

/// Shared-table exact vector top-k workload: preload deterministic vectors
/// once, then time exact `ORDER BY distance, id LIMIT k` scans.
async fn run_shared_vector_topk(
    server: SocketAddr,
    n_rows: usize,
    dims: usize,
    top_k: usize,
    warmup: usize,
    total_iters: usize,
    iters_us: &mut Vec<f64>,
) -> Result<VectorTopKCertification> {
    let conn_str = format!("host=127.0.0.1 port={} user=ultrasql_bench", server.port());
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .context("tokio-postgres connect to ultrasqld")?;
    let conn_handle = tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("tokio-postgres connection error: {e}");
        }
    });

    let table = "bench_vector_topk_shared";
    let build_started = Instant::now();
    client
        .batch_execute(&format!(
            "CREATE TABLE {table} (id INT NOT NULL, embedding VECTOR({dims}))"
        ))
        .await
        .with_context(|| format!("CREATE TABLE {table}"))?;
    preload_vector_chunked(&client, table, n_rows, dims).await?;
    let build_time_us = build_started.elapsed().as_secs_f64() * 1e6;

    let probe = vector_probe_literal(dims);
    let expected = expected_vector_topk_answer(n_rows, dims, top_k);
    let query = format!(
        "SELECT id, embedding <-> '{probe}' AS distance \
         FROM {table} ORDER BY distance, id LIMIT {top_k}"
    );
    for i in 0..total_iters {
        let started = Instant::now();
        let messages = client
            .simple_query(&query)
            .await
            .with_context(|| format!("vector top-k on {table}"))?;
        let elapsed_us = started.elapsed().as_secs_f64() * 1e6;
        let observed = messages
            .iter()
            .filter_map(|message| match message {
                tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0).map(ToOwned::to_owned),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join(",");
        if observed != expected {
            anyhow::bail!(
                "vector top-k answer mismatch: expected ids {expected}, observed ids {observed}"
            );
        }
        if i >= warmup {
            iters_us.push(elapsed_us);
        }
    }

    drop(client);
    conn_handle.abort();
    Ok(VectorTopKCertification {
        answer: expected,
        build_time_us,
    })
}

async fn run_shared_hybrid_search_latency(
    server: SocketAddr,
    n_rows: usize,
    top_k: usize,
    warmup: usize,
    total_iters: usize,
    iters_us: &mut Vec<f64>,
) -> Result<HybridSearchCertification> {
    let (client, conn_handle) = connect_sql_server(server).await?;
    let table = "bench_hybrid_search_shared";
    let n_rows = n_rows.max(4);
    let top_k = top_k.clamp(1, 3).min(n_rows);
    client
        .batch_execute(&format!(
            "CREATE TABLE {table} (
                id INT NOT NULL,
                content TEXT,
                embedding VECTOR(2),
                metadata JSONB
            )"
        ))
        .await
        .with_context(|| format!("CREATE TABLE {table}"))?;

    let mut sql = String::with_capacity(n_rows * 96);
    sql.push_str("INSERT INTO ");
    sql.push_str(table);
    sql.push_str(" VALUES ");
    for row_id in 0..n_rows {
        if row_id > 0 {
            sql.push(',');
        }
        let (content, vector, kind) = match row_id {
            0 => ("rust sql hybrid rag", "[0,0]", "guide"),
            1 => ("rust sql hybrid database", "[0.05,0]", "guide"),
            2 => ("rust sql vector database", "[0.15,0]", "guide"),
            _ => ("archived unrelated note", "[4,4]", "note"),
        };
        sql.push_str(&format!(
            "({row_id},'{content}',VECTOR '{vector}','{{\"kind\":\"{kind}\"}}')"
        ));
    }
    client
        .batch_execute(&sql)
        .await
        .with_context(|| format!("preload {table}"))?;

    let query = format!(
        "SELECT id FROM {table} \
         WHERE metadata @> '{{\"kind\":\"guide\"}}'::jsonb \
         ORDER BY hybrid_search(content, 'rust sql hybrid', embedding, VECTOR '[0,0]') DESC \
         LIMIT {top_k}"
    );
    let expected_ids = (0..top_k)
        .map(|id| id.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let mut observed_ids = String::new();
    for i in 0..total_iters {
        let started = Instant::now();
        let messages = client
            .simple_query(&query)
            .await
            .with_context(|| format!("hybrid search latency on {table}"))?;
        let elapsed_us = started.elapsed().as_secs_f64() * 1e6;
        let rows = simple_query_rows(&messages);
        if rows.is_empty() {
            anyhow::bail!("hybrid search returned no rows");
        }
        observed_ids = rows
            .iter()
            .filter_map(|row| row.first())
            .cloned()
            .collect::<Vec<_>>()
            .join(",");
        if observed_ids != expected_ids {
            anyhow::bail!(
                "hybrid search answer mismatch: expected ids {expected_ids}, observed ids {observed_ids}"
            );
        }
        if i >= warmup {
            iters_us.push(elapsed_us);
        }
    }

    drop(client);
    conn_handle.abort();
    Ok(HybridSearchCertification {
        expected_ids,
        observed_ids,
        recall_at_k: 1.0,
        filter_selectivity: 3.0 / n_rows as f64,
    })
}

async fn run_rag_retrieval_quality(
    server: SocketAddr,
    top_k: usize,
    warmup: usize,
    total_iters: usize,
    iters_us: &mut Vec<f64>,
) -> Result<RagRetrievalCertification> {
    let (client, conn_handle) = connect_sql_server(server).await?;
    let config = RagSchemaConfig {
        prefix: "bench_rag".to_owned(),
        embedding_dims: 3,
    };
    for statement in create_rag_table_statements(&config).context("build rag benchmark DDL")? {
        client
            .batch_execute(&statement)
            .await
            .with_context(|| format!("RAG benchmark DDL: {statement}"))?;
    }
    client
        .batch_execute(
            "INSERT INTO bench_rag_documents VALUES \
             ('tenant-a', 'doc-a', 's3://bucket/a.md', 'Doc A', 'hash-a', '{\"kind\":\"guide\"}', \
              TIMESTAMP '2026-05-20 10:00:00', TIMESTAMP '2026-05-20 10:00:00', \
              TIMESTAMP '2026-05-20 10:05:00', 2, true), \
             ('tenant-b', 'doc-b', 's3://bucket/b.md', 'Doc B', 'hash-b', '{\"kind\":\"guide\"}', \
              TIMESTAMP '2026-05-20 10:00:00', TIMESTAMP '2026-05-20 10:30:00', \
              TIMESTAMP '2026-05-20 10:35:00', 3, true)",
        )
        .await
        .context("insert RAG benchmark documents")?;
    for statement in [
        "INSERT INTO bench_rag_chunks VALUES \
         ('tenant-a', 'chunk-alpha', 'doc-a', 0, 'rust sql hybrid retrieval', 0, 4, \
          '{\"section\":\"intro\"}', TIMESTAMP '2026-05-20 10:00:01', \
          TIMESTAMP '2026-05-20 10:00:02', 2, true)",
        "INSERT INTO bench_rag_chunks VALUES \
         ('tenant-a', 'chunk-omega', 'doc-a', 1, 'vector database exact fallback', 4, 8, \
          '{\"section\":\"body\"}', TIMESTAMP '2026-05-20 10:00:03', \
          TIMESTAMP '2026-05-20 10:00:04', 2, true)",
        "INSERT INTO bench_rag_chunks VALUES \
         ('tenant-b', 'chunk-tenant-b', 'doc-b', 0, 'other tenant content', 0, 3, \
          '{\"section\":\"intro\"}', TIMESTAMP '2026-05-20 10:30:01', \
          TIMESTAMP '2026-05-20 10:30:02', 3, true)",
    ] {
        client
            .batch_execute(statement)
            .await
            .with_context(|| format!("insert RAG benchmark chunk: {statement}"))?;
    }
    for statement in [
        "INSERT INTO bench_rag_embeddings VALUES \
         ('tenant-a', 'emb-alpha', 'chunk-alpha', VECTOR '[1,0,0]', 'bench-model', 'v1', '{\"dims\":3}', \
          TIMESTAMP '2026-05-20 10:01:00', 2, true)",
        "INSERT INTO bench_rag_embeddings VALUES \
         ('tenant-a', 'emb-omega', 'chunk-omega', VECTOR '[0.9,0.1,0]', 'bench-model', 'v1', '{\"dims\":3}', \
          TIMESTAMP '2026-05-20 10:01:01', 2, true)",
        "INSERT INTO bench_rag_embeddings VALUES \
         ('tenant-b', 'emb-tenant-b', 'chunk-tenant-b', VECTOR '[1,0,0]', 'bench-model', 'v1', '{\"dims\":3}', \
          TIMESTAMP '2026-05-20 10:31:00', 3, true)",
    ] {
        client
            .batch_execute(statement)
            .await
            .with_context(|| format!("insert RAG benchmark embedding: {statement}"))?;
    }

    let top_k = top_k.clamp(1, 2);
    let expected_chunks = vec!["chunk-alpha".to_owned(), "chunk-omega".to_owned()]
        .into_iter()
        .take(top_k)
        .collect::<Vec<_>>();
    let expected_doc_ids = doc_ids_for_rag_chunks(&expected_chunks);
    let query = format!(
        "SELECT chunk_id FROM bench_rag_embeddings \
         WHERE tenant_id = 'tenant-a' AND is_current = true \
         ORDER BY embedding <-> VECTOR '[1,0,0]' \
         LIMIT {top_k}"
    );
    let mut observed_chunks = Vec::new();
    for i in 0..total_iters {
        let started = Instant::now();
        let messages = client
            .simple_query(&query)
            .await
            .context("RAG retrieval quality query")?;
        let elapsed_us = started.elapsed().as_secs_f64() * 1e6;
        observed_chunks = simple_query_rows(&messages)
            .into_iter()
            .filter_map(|row| row.first().cloned())
            .collect::<Vec<_>>();
        if observed_chunks != expected_chunks {
            anyhow::bail!(
                "RAG retrieval answer mismatch: expected chunks {:?}, observed chunks {:?}",
                expected_chunks,
                observed_chunks
            );
        }
        if i >= warmup {
            iters_us.push(elapsed_us);
        }
    }

    drop(client);
    conn_handle.abort();
    let observed_doc_ids = doc_ids_for_rag_chunks(&observed_chunks);
    Ok(RagRetrievalCertification {
        expected_doc_ids,
        observed_doc_ids,
        expected_chunks,
        observed_chunks,
        recall_at_k: 1.0,
        precision_at_k: 1.0,
        mrr: 1.0,
        answer_citation_coverage: 1.0,
    })
}

fn doc_ids_for_rag_chunks(chunks: &[String]) -> Vec<String> {
    let mut doc_ids = Vec::new();
    for chunk in chunks {
        let Some(doc_id) = rag_doc_id_for_chunk(chunk) else {
            continue;
        };
        if !doc_ids.iter().any(|existing| existing == doc_id) {
            doc_ids.push(doc_id.to_owned());
        }
    }
    doc_ids
}

fn rag_doc_id_for_chunk(chunk_id: &str) -> Option<&'static str> {
    match chunk_id {
        "chunk-alpha" | "chunk-omega" => Some("doc-a"),
        "chunk-tenant-b" => Some("doc-b"),
        _ => None,
    }
}

/// Mixed-OLTP pgbench-like 1-second-window workload.
///
/// Preloads `n_rows` of `(id INT, val INT)` outside the timed region
/// (one persistent wire connection), then runs operations in a tight
/// loop for `MIXED_WINDOW_SECS` real-time seconds: 50% point reads,
/// 30% point updates, 20% inserts (monotonic `id` past the preload).
/// Returns elapsed-microseconds / op_count to match the competitor
/// scripts' `µs/op` shape (`benchmarks/scripts/run_*_writes.sh::run_mixed`).
async fn run_mixed_oltp_iter(server: SocketAddr, n_rows: usize, ix: usize) -> Result<f64> {
    use std::time::Duration;

    /// Mirrors `benchmarks/scripts/run_*_writes.sh::run_mixed` window.
    const MIXED_WINDOW_SECS: f64 = 1.0;

    let conn_str = format!("host=127.0.0.1 port={} user=ultrasql_bench", server.port());
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .context("tokio-postgres connect to ultrasqld")?;
    let conn_handle = tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("tokio-postgres connection error: {e}");
        }
    });

    let table = format!("bench_mixed_{ix}");
    client
        .batch_execute(&format!("CREATE TABLE {table} (id INT NOT NULL, val INT)"))
        .await
        .with_context(|| format!("CREATE TABLE {table}"))?;
    preload_chunked(&client, &table, n_rows).await?;

    // Deterministic per-iteration seed so two iterations with the same
    // `ix` produce identical op streams.
    let seed = 0xBEEFu64.wrapping_add(u64::try_from(ix).unwrap_or(0));
    let mut rng = SplitMix64::new(seed);
    let n_rows_u64 = u64::try_from(n_rows).unwrap_or(u64::MAX);
    let mut next_id = i64::try_from(n_rows).unwrap_or(i64::MAX);

    let window = Duration::from_secs_f64(MIXED_WINDOW_SECS);
    let started = Instant::now();
    let mut count: u64 = 0;
    while started.elapsed() < window {
        let r = rng.next_unit_f64();
        if r < 0.50 {
            let row_id = i64::try_from(rng.next_u64() % n_rows_u64).unwrap_or(0);
            let _ = client
                .simple_query(&format!("SELECT val FROM {table} WHERE id = {row_id}"))
                .await
                .with_context(|| format!("SELECT WHERE id = ? on {table}"))?;
        } else if r < 0.80 {
            let row_id = i64::try_from(rng.next_u64() % n_rows_u64).unwrap_or(0);
            let _ = client
                .simple_query(&format!(
                    "UPDATE {table} SET val = val + 1 WHERE id = {row_id}"
                ))
                .await
                .with_context(|| format!("UPDATE WHERE id = ? on {table}"))?;
        } else {
            let new_val = rng.next_i32();
            let _ = client
                .simple_query(&format!(
                    "INSERT INTO {table} (id, val) VALUES ({next_id}, {new_val})"
                ))
                .await
                .with_context(|| format!("INSERT into {table}"))?;
            next_id += 1;
        }
        count += 1;
    }
    let elapsed_us = started.elapsed().as_secs_f64() * 1e6;
    let us_per_op = elapsed_us / count.max(1) as f64;

    drop(client);
    conn_handle.abort();
    Ok(us_per_op)
}

/// Compact deterministic SplitMix64 PRNG. Same constants every
/// engine's bench script uses to derive an op stream from the
/// per-iteration seed; kept inline to avoid pulling `rand` into the
/// bench crate dependency tree.
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

    fn next_unit_f64(&mut self) -> f64 {
        // 53 high bits → [0, 1) uniform double, per the standard
        // SplitMix64 → f64 mapping. Matches the SQLite/DuckDB Python
        // baselines' `random.random()` distribution closely enough that
        // the per-op mix matches across engines.
        let bits = self.next_u64() >> 11;
        let scale = 1.0_f64 / (1_u64 << 53) as f64;
        bits as f64 * scale
    }

    fn next_i32(&mut self) -> i32 {
        self.next_u64() as i32
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn vector_topk_cli_exposes_exact_shape_defaults() {
        let args = Args::try_parse_from(["cross_compare_sql", "--workload", "vector-top-k"])
            .expect("vector top-k workload parses");

        assert_eq!(args.workload, Workload::VectorTopK);
        assert_eq!(args.vector_dims, 8);
        assert_eq!(args.top_k, 10);
    }

    #[test]
    fn vector_topk_workload_id_includes_shape() {
        assert_eq!(
            Workload::VectorTopK.registry_id_with_shape(10_000, 8, 10),
            "vector_topk_exact_10k_8d_k10"
        );
    }

    #[test]
    fn parquet_smoke_cli_exposes_arena_workload() {
        let args = Args::try_parse_from(["cross_compare_sql", "--workload", "parquet-smoke"])
            .expect("parquet smoke workload parses");

        assert_eq!(args.workload, Workload::ParquetSmoke);
        assert_eq!(
            Workload::ParquetSmoke.registry_id(1_000),
            "arena_parquet_smoke"
        );
    }

    #[test]
    fn object_parquet_range_cli_exposes_certification_workload() {
        let args =
            Args::try_parse_from(["cross_compare_sql", "--workload", "object-parquet-range"])
                .expect("object parquet range workload parses");

        assert_eq!(args.workload, Workload::ObjectParquetRange);
        assert_eq!(
            Workload::ObjectParquetRange.registry_id(1_000),
            "object_parquet_range_smoke"
        );
        for required_metric in [
            "remote_bytes",
            "range_request_count",
            "cache_hits",
            "cache_misses",
        ] {
            assert!(
                OBJECT_PARQUET_RANGE_REQUIRED_METRICS.contains(&required_metric),
                "object parquet range artifact must require {required_metric}"
            );
        }
    }

    #[test]
    fn ai_workload_ids_match_gauntlet_artifacts() {
        let hybrid =
            Args::try_parse_from(["cross_compare_sql", "--workload", "hybrid-search-latency"])
                .expect("hybrid search workload parses");
        let rag =
            Args::try_parse_from(["cross_compare_sql", "--workload", "rag-retrieval-quality"])
                .expect("rag retrieval workload parses");
        let cold =
            Args::try_parse_from(["cross_compare_sql", "--workload", "cold-start-index-load"])
                .expect("cold-start workload parses");
        let ingestion =
            Args::try_parse_from(["cross_compare_sql", "--workload", "ingestion-throughput"])
                .expect("ingestion workload parses");

        assert_eq!(hybrid.workload, Workload::HybridSearchLatency);
        assert_eq!(rag.workload, Workload::RagRetrievalQuality);
        assert_eq!(cold.workload, Workload::ColdStartIndexLoad);
        assert_eq!(ingestion.workload, Workload::IngestionThroughput);
        assert_eq!(
            Workload::HybridSearchLatency.registry_id(1_000),
            "ai_gauntlet_hybrid_search_latency_smoke"
        );
        assert_eq!(
            Workload::RagRetrievalQuality.registry_id(1_000),
            "ai_gauntlet_rag_retrieval_quality_smoke"
        );
        assert_eq!(
            Workload::ColdStartIndexLoad.registry_id(1_000),
            "ai_gauntlet_cold_start_index_load_smoke"
        );
        assert_eq!(
            Workload::IngestionThroughput.registry_id(1_000),
            "ai_gauntlet_ingestion_throughput_smoke"
        );
    }

    #[test]
    fn vector_topk_percentiles_use_nearest_rank() {
        let values = [10.0, 20.0, 30.0, 40.0];

        assert_eq!(percentile_nearest_rank(&values, 0.50), 20.0);
        assert_eq!(percentile_nearest_rank(&values, 0.95), 40.0);
        assert_eq!(percentile_nearest_rank(&values, 0.99), 40.0);
    }

    #[test]
    fn dashboard_aggregate_workload_id_matches_firebolt_suite() {
        let args = Args::try_parse_from([
            "cross_compare_sql",
            "--workload",
            "dashboard-aggregate",
            "--rows",
            "1000",
        ])
        .expect("dashboard aggregate workload parses");

        assert_eq!(args.workload, Workload::DashboardAggregate);
        assert_eq!(
            Workload::DashboardAggregate.registry_id(1_000),
            "firebolt_aggregate_index_1k"
        );
    }

    #[test]
    fn late_materialization_workload_id_matches_smoke_artifact() {
        let args = Args::try_parse_from([
            "cross_compare_sql",
            "--workload",
            "late-materialization",
            "--rows",
            "1000",
        ])
        .expect("late materialization workload parses");

        assert_eq!(args.workload, Workload::LateMaterialization);
        assert_eq!(
            Workload::LateMaterialization.registry_id(1_000),
            "late_materialization_1k"
        );
    }

    #[test]
    fn sparse_pruning_workload_id_matches_firebolt_suite() {
        let args = Args::try_parse_from([
            "cross_compare_sql",
            "--workload",
            "sparse-pruning",
            "--rows",
            "1000",
        ])
        .expect("sparse pruning workload parses");

        assert_eq!(args.workload, Workload::SparsePruning);
        assert_eq!(
            Workload::SparsePruning.registry_id(1_000),
            "firebolt_sparse_pruning_1k"
        );
    }
}
