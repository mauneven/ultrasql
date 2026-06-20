//! Shared types for the wire-protocol cross-engine benchmark driver:
//! required-metric constants, certification result structs, the
//! `Workload` / `StorageMode` selectors, and the clap `Args`.

use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, ValueEnum};

pub(crate) const VECTOR_CERTIFICATION_METRICS: &[&str] = &[
    "recall_at_k",
    "p50_latency_us",
    "p95_latency_us",
    "p99_latency_us",
    "build_time_us",
    "memory_bytes",
    "index_size_bytes",
];
pub(crate) const PARQUET_SMOKE_REQUIRED_METRICS: &[&str] = &[
    "scan_us",
    "projection_pushdown_us",
    "predicate_pushdown_us",
    "row_group_pruning_us",
];
pub(crate) const OBJECT_PARQUET_RANGE_REQUIRED_METRICS: &[&str] = &[
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
pub(crate) const HYBRID_SEARCH_REQUIRED_METRICS: &[&str] = &[
    "recall_at_k",
    "p50_latency_us",
    "p95_latency_us",
    "p99_latency_us",
    "filter_selectivity",
    "bm25_score",
    "vector_score",
];
pub(crate) const RAG_RETRIEVAL_REQUIRED_METRICS: &[&str] = &[
    "expected_doc_ids",
    "observed_doc_ids",
    "recall_at_k",
    "precision_at_k",
    "mrr",
    "latency_us",
    "answer_citation_coverage",
];
pub(crate) const LATE_MATERIALIZATION_REQUIRED_METRICS: &[&str] = &[
    "median_us",
    "p50_latency_us",
    "p95_latency_us",
    "p99_latency_us",
    "explain_late_materialization",
    "eager_scan_median_us",
    "late_materialization_median_us",
    "rows",
];
pub(crate) const LATE_MATERIALIZATION_ANSWER_METRICS: &[&str] = &[
    "rows",
    "tenant_id",
    "wide_columns",
    "projected_columns",
    "query_shape",
    "firebolt_style_shape",
    "answer_order",
    "explain_late_materialization",
    "eager_scan_median_us",
    "eager_scan_samples_us",
    "late_materialization_median_us",
    "late_materialization_samples_us",
    "comparison_policy",
];
pub(crate) const COLD_START_INDEX_LOAD_REQUIRED_METRICS: &[&str] = &[
    "restart_time_us",
    "first_query_us",
    "second_query_us",
    "index_loaded_from_disk",
];
pub(crate) const INGESTION_THROUGHPUT_REQUIRED_METRICS: &[&str] =
    &["rows_per_sec", "wal_bytes", "index_update_us", "commit_us"];
pub(crate) const PARQUET_SMOKE_ROW_GROUP_ROWS: usize = 4_096;
pub(crate) const PARQUET_SMOKE_MIN_ROWS: usize = PARQUET_SMOKE_ROW_GROUP_ROWS * 2;

#[derive(Debug, Clone)]
pub(crate) struct VectorTopKCertification {
    pub(crate) answer: String,
    pub(crate) build_time_us: f64,
}

#[derive(Debug, Clone)]
pub(crate) struct ParquetSmokeMetrics {
    pub(crate) rows: usize,
    pub(crate) scan_us: f64,
    pub(crate) projection_pushdown_us: f64,
    pub(crate) predicate_pushdown_us: f64,
    pub(crate) row_group_pruning_us: f64,
    pub(crate) scan_samples_us: Vec<f64>,
    pub(crate) projection_pushdown_samples_us: Vec<f64>,
    pub(crate) predicate_pushdown_samples_us: Vec<f64>,
    pub(crate) row_group_pruning_samples_us: Vec<f64>,
    pub(crate) answer: serde_json::Value,
}

#[derive(Debug, Clone)]
pub(crate) struct ObjectParquetRangeMetrics {
    pub(crate) query_median_us: f64,
    pub(crate) samples_us: Vec<f64>,
    pub(crate) answer: serde_json::Value,
    pub(crate) object_bytes: usize,
    pub(crate) range_request_count: usize,
    pub(crate) requested_range_bytes: u64,
    pub(crate) remote_bytes: u64,
    pub(crate) cache_hits: u64,
    pub(crate) cache_misses: u64,
    pub(crate) length_probe_seen: bool,
    pub(crate) whole_object_fetched: bool,
    pub(crate) projected_out_column_fetched: bool,
    pub(crate) requests: Vec<serde_json::Value>,
}

#[derive(Debug, Clone)]
pub(crate) struct HybridSearchCertification {
    pub(crate) expected_ids: String,
    pub(crate) observed_ids: String,
    pub(crate) recall_at_k: f64,
    pub(crate) filter_selectivity: f64,
}

#[derive(Debug, Clone)]
pub(crate) struct RagRetrievalCertification {
    pub(crate) expected_doc_ids: Vec<String>,
    pub(crate) observed_doc_ids: Vec<String>,
    pub(crate) expected_chunks: Vec<String>,
    pub(crate) observed_chunks: Vec<String>,
    pub(crate) recall_at_k: f64,
    pub(crate) precision_at_k: f64,
    pub(crate) mrr: f64,
    pub(crate) answer_citation_coverage: f64,
}

#[derive(Debug)]
pub(crate) struct PersistentBenchServer {
    pub(crate) bound: SocketAddr,
    pub(crate) handle: tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
}

#[derive(Debug, Clone)]
pub(crate) struct TimedQueryMetric {
    pub(crate) median_us: f64,
    pub(crate) samples_us: Vec<f64>,
    pub(crate) rows: Vec<Vec<String>>,
}

/// Workload selector. New workloads will be added as the wire
/// pipeline grows to cover more shapes.
#[derive(Copy, Clone, Eq, PartialEq, Debug, ValueEnum)]
pub(crate) enum Workload {
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
    /// Bulk UPDATE: preload `--rows` (id INT, val INT) tuples once,
    /// then time `UPDATE bench_update_shared SET val = val + 1 WHERE
    /// id < <rows>` inside BEGIN/ROLLBACK for every sample.
    UpdateBulk,
    /// Bulk DELETE: preload `--rows` (id INT, val INT) tuples once,
    /// then time `DELETE FROM bench_delete_shared WHERE id < <rows>`
    /// inside BEGIN/ROLLBACK for every sample.
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

#[derive(Copy, Clone, Eq, PartialEq, Debug, ValueEnum)]
pub(crate) enum StorageMode {
    Memory,
    DataDir,
}

impl StorageMode {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::DataDir => "data-dir",
        }
    }

    pub(crate) const fn durability_mode(self) -> &'static str {
        match self {
            Self::Memory => "volatile",
            Self::DataDir => "durable",
        }
    }
}

impl Workload {
    #[cfg(test)]
    pub(crate) fn registry_id(self, n_rows: usize) -> String {
        self.registry_id_with_shape(n_rows, DEFAULT_VECTOR_DIMS, DEFAULT_TOP_K)
    }

    pub(crate) fn registry_id_with_shape(
        self,
        n_rows: usize,
        vector_dims: usize,
        top_k: usize,
    ) -> String {
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

pub(crate) const DEFAULT_VECTOR_DIMS: usize = 8;
pub(crate) const DEFAULT_TOP_K: usize = 10;

/// Render a row count using `10k` / `1m` notation matching the
/// existing competitor workload ids (`insert_throughput_10k`,
/// `select_sum_65k_i64`).
pub(crate) fn k_or_raw(n: usize) -> String {
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
pub(crate) struct Args {
    /// Workload to run.
    #[arg(long, value_enum, default_value_t = Workload::InsertBulk)]
    pub(crate) workload: Workload,
    /// Number of rows in the data set.
    #[arg(long, default_value_t = 10_000)]
    pub(crate) rows: usize,
    /// Warmup iterations (not recorded).
    #[arg(long, default_value_t = 2)]
    pub(crate) warmup: usize,
    /// Measured iterations (median + min reported).
    #[arg(long, default_value_t = 8)]
    pub(crate) iters: usize,
    /// Output JSON file path. When omitted, the JSON is written to
    /// stdout (so the binary composes with `benchmarks/run.sh`).
    #[arg(long)]
    pub(crate) output: Option<PathBuf>,
    /// Explicit workload id override. When omitted, the id is
    /// derived from `--workload` + `--rows`, e.g.
    /// `insert_throughput_10k`.
    #[arg(long)]
    pub(crate) workload_id: Option<String>,
    /// Vector dimensions for the `vector-top-k` workload.
    #[arg(long, default_value_t = DEFAULT_VECTOR_DIMS)]
    pub(crate) vector_dims: usize,
    /// Number of nearest rows returned by the `vector-top-k` workload.
    #[arg(long, default_value_t = DEFAULT_TOP_K)]
    pub(crate) top_k: usize,
    /// CSV data file for `csv-*` benchmark workloads.
    #[arg(long)]
    pub(crate) csv_path: Option<PathBuf>,
    /// Malformed CSV data file for `csv-malformed-behavior`.
    #[arg(long)]
    pub(crate) csv_bad_path: Option<PathBuf>,
    /// Existing UltraSQL server address to benchmark instead of spawning the
    /// in-process harness server. Used by release-artifact certification.
    #[arg(long)]
    pub(crate) server: Option<SocketAddr>,
    /// Storage profile of the UltraSQL server being measured.
    #[arg(long, value_enum, default_value_t = StorageMode::Memory)]
    pub(crate) storage_mode: StorageMode,
}
