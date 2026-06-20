//! Final JSON report assembly for the wire-protocol benchmark driver.
//!
//! `main` collects raw samples plus an optional per-workload
//! certification/metrics payload, then hands everything here to compute
//! the percentile summary and stamp the suite-specific fields onto the
//! report object. Pure data motion out of `main`; no behavior change.

use anyhow::Result;
use ultrasql_bench::registry::HostInfo;

use super::types::{
    Args, HYBRID_SEARCH_REQUIRED_METRICS, HybridSearchCertification,
    LATE_MATERIALIZATION_ANSWER_METRICS, LATE_MATERIALIZATION_REQUIRED_METRICS,
    OBJECT_PARQUET_RANGE_REQUIRED_METRICS, ObjectParquetRangeMetrics,
    PARQUET_SMOKE_REQUIRED_METRICS, PARQUET_SMOKE_ROW_GROUP_ROWS, ParquetSmokeMetrics,
    RAG_RETRIEVAL_REQUIRED_METRICS, RagRetrievalCertification, VECTOR_CERTIFICATION_METRICS,
    VectorTopKCertification, Workload,
};
use super::util::{answer_sha256, median_sorted, percentile_nearest_rank, promote_answer_metrics};

/// Per-workload payloads collected by `main` before report assembly.
/// Every field is optional because only the active workload populates
/// the matching one; the rest stay `None`.
#[derive(Default)]
pub(crate) struct ReportPayload {
    pub(crate) answer: Option<serde_json::Value>,
    pub(crate) vector_topk_certification: Option<VectorTopKCertification>,
    pub(crate) parquet_smoke_metrics: Option<ParquetSmokeMetrics>,
    pub(crate) object_parquet_range_metrics: Option<ObjectParquetRangeMetrics>,
    pub(crate) hybrid_search_certification: Option<HybridSearchCertification>,
    pub(crate) rag_retrieval_certification: Option<RagRetrievalCertification>,
}

/// Compute the percentile summary and assemble the suite-specific
/// report object. `iters_us` is sorted in place (NaN last) before the
/// median/min/percentiles are read.
pub(crate) fn build_report(
    args: &Args,
    workload_id: &str,
    server_addr: &str,
    mut iters_us: Vec<f64>,
    payload: ReportPayload,
) -> Result<serde_json::Value> {
    let ReportPayload {
        answer,
        vector_topk_certification,
        parquet_smoke_metrics,
        object_parquet_range_metrics,
        hybrid_search_certification,
        rag_retrieval_certification,
    } = payload;

    // Compute median + min.
    ultrasql_bench::sort_f64_nan_last(&mut iters_us);
    let median_us = median_sorted(&iters_us);
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
        "server_addr": server_addr,
        "server_mode": if args.server.is_some() { "external" } else { "in_process" },
        "storage_mode": args.storage_mode.as_str(),
        "durability_mode": args.storage_mode.durability_mode(),
        "policy": "Raw measured samples only; no ranking or winner claim.",
    });
    if let Some(answer) = answer {
        let answer_hash = answer_sha256(&answer)?;
        if matches!(args.workload, Workload::LateMaterialization) {
            promote_answer_metrics(&mut report, &answer, LATE_MATERIALIZATION_ANSWER_METRICS);
        }
        report["answer"] = answer;
        report["answer_sha256"] = serde_json::json!(answer_hash);
    }
    if matches!(args.workload, Workload::LateMaterialization) {
        report["schema_version"] = serde_json::json!(1);
        report["suite"] = serde_json::json!("late_materialization");
        report["profile"] = serde_json::json!("smoke");
        report["status"] = serde_json::json!("measured");
        report["required_metrics"] = serde_json::json!(LATE_MATERIALIZATION_REQUIRED_METRICS);
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
    Ok(report)
}
