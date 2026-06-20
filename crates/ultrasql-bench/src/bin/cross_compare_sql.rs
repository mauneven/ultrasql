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
// The bench harness uses ad-hoc index arithmetic across synthetic data
// generators, iteration counters, and ASCII-table renderers. The
// library crate root carries the matching crate-level allow; replicate
// it here because each binary is its own compilation unit.
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::cast_lossless,
    reason = "bench harness: deterministic synthetic data + iteration math; no impact on engine crates"
)]

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use ultrasql_server::{Server, bind_listener, serve_listener};

#[path = "cross_compare_sql_support/types.rs"]
mod types;
#[path = "cross_compare_sql_support/util.rs"]
mod util;
#[path = "cross_compare_sql_support/report.rs"]
mod report;
#[path = "cross_compare_sql_support/dispatch.rs"]
mod dispatch;
#[path = "cross_compare_sql_support/oltp_workloads.rs"]
mod oltp_workloads;
#[path = "cross_compare_sql_support/olap_workloads.rs"]
mod olap_workloads;
#[path = "cross_compare_sql_support/csv_workloads.rs"]
mod csv_workloads;
#[path = "cross_compare_sql_support/parquet_workloads.rs"]
mod parquet_workloads;
#[path = "cross_compare_sql_support/ai_workloads.rs"]
mod ai_workloads;

use ai_workloads::{run_cold_start_index_load_workload, run_ingestion_throughput_workload};
use dispatch::run_workload;
use report::build_report;
use types::{Args, Workload};

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

    // Run warmup + measured iterations. The dispatch matches the
    // per-engine pattern every competitor script uses: shared-table
    // workloads preload once outside the timed region, then run the
    // query N times against the same relation. Anything else would
    // compare cold-cache UltraSQL against warm-cache peers.
    let mut iters_us: Vec<f64> = Vec::with_capacity(args.iters);
    let total_iters = args.warmup + args.iters;
    let payload = run_workload(bound, &args, total_iters, &mut iters_us).await?;

    let report = build_report(&args, &workload_id, &bound.to_string(), iters_us, payload)?;
    let serialized = serde_json::to_string(&report)?;
    if let Some(path) = args.output.as_ref() {
        std::fs::write(path, &serialized).with_context(|| format!("write {}", path.display()))?;
        eprintln!("cross_compare_sql: wrote {}", path.display());
    } else {
        println!("{serialized}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::types::{
        Args, LATE_MATERIALIZATION_ANSWER_METRICS, LATE_MATERIALIZATION_REQUIRED_METRICS,
        OBJECT_PARQUET_RANGE_REQUIRED_METRICS, Workload,
    };
    use super::util::{
        LATE_MAT_PRELOAD_CHUNK_ROWS, PRELOAD_CHUNK_ROWS, SplitMix64, median_sorted,
        percentile_nearest_rank, promote_answer_metrics, splitmix_high_bits_to_f64, usize_to_u128,
    };
    use clap::Parser;

    /// Deterministic vector component generator mirrored from
    /// `ai_workloads`. The split keeps the generator private to that
    /// module; this test re-derives the identical bounds check it needs.
    fn vector_component(row_id: usize, dim: usize) -> i32 {
        let row = usize_to_u128(row_id);
        let dim = usize_to_u128(dim);
        let value = ((row * 31) + (dim * 17) + ((row % 7) * 13)) % 101;
        i32::try_from(value).unwrap_or(0) - 50
    }

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
    fn vector_component_converts_usize_bounds_without_panics() {
        assert_eq!(usize_to_u128(0), 0);
        assert_eq!(usize_to_u128(1), 1);

        #[cfg(target_pointer_width = "64")]
        assert_eq!(usize_to_u128(usize::MAX), u128::from(u64::MAX));

        #[cfg(target_pointer_width = "32")]
        assert_eq!(usize_to_u128(usize::MAX), u128::from(u32::MAX));

        let component = vector_component(usize::MAX, usize::MAX);
        assert!((-50..=50).contains(&component));
    }

    #[test]
    fn splitmix_high_bits_convert_exactly_with_no_panic_path() {
        assert_eq!(splitmix_high_bits_to_f64(0), 0.0);
        assert_eq!(splitmix_high_bits_to_f64(1), 1.0);
        assert_eq!(splitmix_high_bits_to_f64(1_u64 << 32), 4_294_967_296.0);
        assert_eq!(
            splitmix_high_bits_to_f64((1_u64 << 53) - 1),
            9_007_199_254_740_991.0
        );

        let mut rng = SplitMix64::new(0);
        for _ in 0..1_000 {
            let value = rng.next_unit_f64();
            assert!(value.is_finite());
            assert!((0.0..1.0).contains(&value));
        }
    }

    #[test]
    fn median_sorted_averages_even_sample_count() {
        assert_eq!(median_sorted(&[1.0, 3.0]), 2.0);
        assert_eq!(median_sorted(&[1.0, 2.0, 10.0, 20.0]), 6.0);
        assert_eq!(median_sorted(&[1.0, 2.0, 3.0]), 2.0);
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
    fn late_materialization_preload_uses_wide_row_chunk_size() {
        let late_chunk_rows = std::hint::black_box(LATE_MAT_PRELOAD_CHUNK_ROWS);
        let generic_chunk_rows = std::hint::black_box(PRELOAD_CHUNK_ROWS);

        assert!(
            late_chunk_rows <= 5_000,
            "wide 100-column INSERT chunks must stay small enough for full certification"
        );
        assert!(
            late_chunk_rows < generic_chunk_rows,
            "late-materialization should not reuse the generic narrow-row preload chunk"
        );
    }

    #[test]
    fn late_materialization_promotes_required_answer_metrics() {
        let mut report = serde_json::json!({
            "status": "measured",
            "median_us": 10.0,
            "p50_latency_us": 10.0,
            "p95_latency_us": 10.0,
            "p99_latency_us": 10.0,
        });
        let answer = serde_json::json!({
            "rows": 3,
            "explain_late_materialization": "Late Materialization: candidates=3 fetched=3 skipped=0",
            "eager_scan_median_us": 40.0,
            "late_materialization_median_us": 12.0,
        });

        promote_answer_metrics(&mut report, &answer, LATE_MATERIALIZATION_ANSWER_METRICS);

        for metric in LATE_MATERIALIZATION_REQUIRED_METRICS {
            assert!(
                report.get(*metric).is_some_and(|value| !value.is_null()),
                "late-materialization artifact must expose required metric {metric}"
            );
        }
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
