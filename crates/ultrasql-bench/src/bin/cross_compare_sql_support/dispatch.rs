//! Workload routing for the wire-protocol benchmark driver.
//!
//! `main` parses args, brings up the server, and then hands control
//! here: `run_workload` matches `--workload` to its runner, appends the
//! measured samples to `iters_us`, and returns the per-workload
//! certification/metrics payload for report assembly. Pure routing
//! moved out of `main`; no behavior change.

use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::{Context, Result};

use super::ai_workloads::{
    run_rag_retrieval_quality, run_shared_hybrid_search_latency, run_shared_vector_topk,
};
use super::csv_workloads::{
    run_csv_copy_import, run_csv_join_table, run_csv_malformed_behavior, run_csv_query_workload,
};
use super::olap_workloads::{
    run_shared_dashboard_aggregate, run_shared_late_materialization, run_shared_olap_aggregate,
    run_shared_select_scan, run_shared_sparse_pruning, run_shared_window_row_number,
};
use super::oltp_workloads::{
    run_insert_iter, run_mixed_oltp_iter, run_shared_delete, run_shared_mixed_correctness,
    run_shared_update,
};
use super::parquet_workloads::{run_object_parquet_range_smoke, run_parquet_smoke};
use super::report::ReportPayload;
use super::types::{Args, Workload};
use super::util::sql_string;

/// Drive the workload selected by `args.workload` against `bound`,
/// pushing measured samples onto `iters_us` and returning the payload
/// used to stamp suite-specific fields onto the final report.
///
/// The `ColdStartIndexLoad` and `IngestionThroughput` workloads write
/// their own self-contained reports and are handled by `main` before
/// this is reached; they are unreachable here.
pub(crate) async fn run_workload(
    bound: SocketAddr,
    args: &Args,
    total_iters: usize,
    iters_us: &mut Vec<f64>,
) -> Result<ReportPayload> {
    let mut payload = ReportPayload::default();
    match args.workload {
        Workload::SelectScan => {
            run_shared_select_scan(bound, args.rows, args.warmup, total_iters, iters_us).await?;
        }
        Workload::SumScalar => {
            run_shared_olap_aggregate(
                bound,
                args.rows,
                args.warmup,
                total_iters,
                iters_us,
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
                iters_us,
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
                iters_us,
                "bench_filter_sum_shared",
                move |t| format!("SELECT SUM(x) FROM {t} WHERE x > {threshold}"),
            )
            .await?;
        }
        Workload::LateMaterialization => {
            payload.answer = Some(
                run_shared_late_materialization(
                    bound,
                    args.rows,
                    args.warmup,
                    total_iters,
                    iters_us,
                )
                .await?,
            );
        }
        Workload::DashboardAggregate => {
            payload.answer = Some(
                run_shared_dashboard_aggregate(
                    bound,
                    args.rows,
                    args.warmup,
                    total_iters,
                    iters_us,
                )
                .await?,
            );
        }
        Workload::SparsePruning => {
            payload.answer = Some(
                run_shared_sparse_pruning(bound, args.rows, args.warmup, total_iters, iters_us)
                    .await?,
            );
        }
        Workload::WindowRowNumber => {
            run_shared_window_row_number(bound, args.rows, args.warmup, total_iters, iters_us)
                .await?;
        }
        Workload::UpdateBulk => {
            run_shared_update(bound, args.rows, args.warmup, total_iters, iters_us).await?;
        }
        Workload::DeleteBulk => {
            run_shared_delete(bound, args.rows, args.warmup, total_iters, iters_us).await?;
        }
        Workload::MixedCorrectness => {
            payload.answer = Some(
                run_shared_mixed_correctness(bound, args.rows, args.warmup, total_iters, iters_us)
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
                iters_us,
            )
            .await?;
            payload.answer = Some(serde_json::json!(certification.answer.clone()));
            payload.vector_topk_certification = Some(certification);
        }
        Workload::CsvColdRead => {
            let csv_path = required_csv_path(args)?;
            let path_sql = sql_string(csv_path);
            payload.answer = Some(
                run_csv_query_workload(
                    bound,
                    &format!("SELECT COUNT(*) FROM read_csv({path_sql})"),
                    args.warmup,
                    total_iters,
                    iters_us,
                )
                .await?,
            );
        }
        Workload::CsvWarmRead => {
            let csv_path = required_csv_path(args)?;
            let path_sql = sql_string(csv_path);
            payload.answer = Some(
                run_csv_query_workload(
                    bound,
                    &format!("SELECT COUNT(*) FROM read_csv({path_sql})"),
                    args.warmup,
                    total_iters,
                    iters_us,
                )
                .await?,
            );
        }
        Workload::CsvCopyImport => {
            let csv_path = required_csv_path(args)?;
            payload.answer = Some(
                run_csv_copy_import(bound, csv_path, args.warmup, total_iters, iters_us).await?,
            );
        }
        Workload::CsvGroupBy => {
            let csv_path = required_csv_path(args)?;
            let path_sql = sql_string(csv_path);
            payload.answer = Some(
                run_csv_query_workload(
                    bound,
                    &format!(
                        "SELECT category, COUNT(*) FROM read_csv({path_sql}) \
                         GROUP BY category ORDER BY category"
                    ),
                    args.warmup,
                    total_iters,
                    iters_us,
                )
                .await?,
            );
        }
        Workload::CsvFilter => {
            let csv_path = required_csv_path(args)?;
            let path_sql = sql_string(csv_path);
            payload.answer = Some(
                run_csv_query_workload(
                    bound,
                    &format!("SELECT COUNT(*) FROM read_csv({path_sql}) WHERE category = 'alpha'"),
                    args.warmup,
                    total_iters,
                    iters_us,
                )
                .await?,
            );
        }
        Workload::CsvJoinTable => {
            let csv_path = required_csv_path(args)?;
            payload.answer = Some(
                run_csv_join_table(bound, csv_path, args.warmup, total_iters, iters_us).await?,
            );
        }
        Workload::CsvMalformedBehavior => {
            let csv_path = args
                .csv_bad_path
                .as_ref()
                .or(args.csv_path.as_ref())
                .context("--csv-bad-path or --csv-path is required for csv-malformed-behavior")?;
            payload.answer = Some(
                run_csv_malformed_behavior(bound, csv_path, args.warmup, total_iters, iters_us)
                    .await?,
            );
        }
        Workload::ParquetSmoke => {
            let metrics =
                run_parquet_smoke(bound, args.rows, args.warmup, total_iters, iters_us).await?;
            payload.answer = Some(metrics.answer.clone());
            payload.parquet_smoke_metrics = Some(metrics);
        }
        Workload::ObjectParquetRange => {
            let metrics = run_object_parquet_range_smoke(
                bound,
                args.rows,
                args.warmup,
                total_iters,
                iters_us,
            )
            .await?;
            payload.answer = Some(metrics.answer.clone());
            payload.object_parquet_range_metrics = Some(metrics);
        }
        Workload::HybridSearchLatency => {
            let certification = run_shared_hybrid_search_latency(
                bound,
                args.rows,
                args.top_k,
                args.warmup,
                total_iters,
                iters_us,
            )
            .await?;
            payload.answer = Some(serde_json::json!({
                "expected_ids": certification.expected_ids.clone(),
                "observed_ids": certification.observed_ids.clone(),
            }));
            payload.hybrid_search_certification = Some(certification);
        }
        Workload::RagRetrievalQuality => {
            let certification =
                run_rag_retrieval_quality(bound, args.top_k, args.warmup, total_iters, iters_us)
                    .await?;
            payload.answer = Some(serde_json::json!({
                "expected_doc_ids": certification.expected_doc_ids.clone(),
                "observed_doc_ids": certification.observed_doc_ids.clone(),
                "expected_chunks": certification.expected_chunks.clone(),
                "observed_chunks": certification.observed_chunks.clone(),
            }));
            payload.rag_retrieval_certification = Some(certification);
        }
        _ => {
            for i in 0..total_iters {
                let micros = match args.workload {
                    Workload::InsertBulk => run_insert_iter(bound, args.rows, i).await?,
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
                    | Workload::DeleteBulk
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
    Ok(payload)
}

fn required_csv_path(args: &Args) -> Result<&PathBuf> {
    args.csv_path.as_ref().context("--csv-path is required")
}
