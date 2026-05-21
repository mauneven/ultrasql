//! Contract tests for AI benchmark gauntlet orchestration.

use std::fs;
use std::path::PathBuf;

use serde_json::Value;

fn repo_file(path: &str) -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(path);
    fs::read_to_string(&path).unwrap_or_else(|err| panic!("read {}: {err}", path.display()))
}

fn repo_json(path: &str) -> Value {
    serde_json::from_str(&repo_file(path)).unwrap_or_else(|err| panic!("parse {path}: {err}"))
}

#[test]
fn ai_gauntlet_declares_required_suites_and_artifacts() {
    let script = repo_file("benchmarks/ai_benchmark_gauntlet.sh");

    for suite in [
        "exact_vector_scan",
        "ann_recall_latency",
        "hybrid_search_latency",
        "rag_retrieval_quality",
        "filtered_vector_search",
        "ingestion_throughput",
        "memory_per_million_vectors",
        "cold_start_index_load",
    ] {
        assert!(
            script.contains(suite),
            "AI gauntlet script must declare suite {suite}"
        );
    }

    assert!(script.contains("benchmarks/vector_topk_exact.sh"));
    assert!(script.contains("benchmarks/vector_ann_hnsw.sh"));
    assert!(script.contains("--workload hybrid-search-latency"));
    assert!(script.contains("--workload rag-retrieval-quality"));
    assert!(script.contains("VECTOR_TOPK_RENDER_RESULTS=0"));
    assert!(script.contains("ai_benchmark_gauntlet_manifest.json"));
    assert!(script.contains("has_failed"));
    assert!(!script.contains("runner_not_implemented"));
}

#[test]
fn required_ai_gauntlet_suites_have_real_ultrasql_runners() {
    let script = repo_file("benchmarks/ai_benchmark_gauntlet.sh");

    assert!(script.contains("run_filtered_vector_search"));
    assert!(script.contains("run_memory_per_million_vectors"));
    assert!(script.contains("run_cold_start_index_load"));
    assert!(script.contains("run_ingestion_throughput"));
    assert!(script.contains("ultrasql-bench filtered-vector"));
    assert!(script.contains("ultrasql-bench vector-memory"));
    assert!(script.contains("--workload cold-start-index-load"));
    assert!(script.contains("--workload ingestion-throughput"));
    assert!(
        !script.contains("run_missing_suite"),
        "all required UltraSQL AI gauntlet suites must have real runners"
    );
}

#[test]
fn ai_gauntlet_smoke_artifacts_are_measured() {
    let filtered = repo_json(
        "benchmarks/results/latest/raw/ai_gauntlet_filtered_vector_search_smoke-ultrasql.json",
    );
    assert_eq!(filtered["suite"], "filtered_vector_search");
    assert_eq!(filtered["status"], "measured");
    for field in [
        "recall_at_k",
        "p50_latency_us",
        "p95_latency_us",
        "p99_latency_us",
        "filter_selectivity",
        "candidate_expansion_count",
    ] {
        assert!(
            filtered.get(field).is_some(),
            "filtered vector artifact missing {field}"
        );
    }

    let ingestion = repo_json(
        "benchmarks/results/latest/raw/ai_gauntlet_ingestion_throughput_smoke-ultrasql.json",
    );
    assert_eq!(ingestion["suite"], "ingestion_throughput");
    assert_eq!(ingestion["status"], "measured");
    for field in ["rows_per_sec", "wal_bytes", "index_update_us", "commit_us"] {
        assert!(
            ingestion.get(field).is_some(),
            "ingestion artifact missing {field}"
        );
    }

    let rag = repo_json(
        "benchmarks/results/latest/raw/ai_gauntlet_rag_retrieval_quality_smoke-ultrasql.json",
    );
    assert_eq!(rag["suite"], "rag_retrieval_quality");
    assert_eq!(rag["status"], "measured");
    for field in [
        "expected_doc_ids",
        "observed_doc_ids",
        "recall_at_k",
        "precision_at_k",
        "mrr",
        "latency_us",
    ] {
        assert!(rag.get(field).is_some(), "RAG artifact missing {field}");
    }

    let memory = repo_json(
        "benchmarks/results/latest/raw/ai_gauntlet_memory_per_million_vectors_smoke-ultrasql.json",
    );
    assert_eq!(memory["suite"], "memory_per_million_vectors");
    assert_eq!(memory["status"], "measured");
    for field in [
        "index_size_bytes",
        "memory_bytes",
        "bytes_per_vector",
        "build_time_us",
    ] {
        assert!(
            memory.get(field).is_some(),
            "memory artifact missing {field}"
        );
    }

    let cold = repo_json(
        "benchmarks/results/latest/raw/ai_gauntlet_cold_start_index_load_smoke-ultrasql.json",
    );
    assert_eq!(cold["suite"], "cold_start_index_load");
    assert_eq!(cold["status"], "measured");
    for field in [
        "restart_time_us",
        "first_query_us",
        "second_query_us",
        "index_loaded_from_disk",
    ] {
        assert!(
            cold.get(field).is_some(),
            "cold-start artifact missing {field}"
        );
    }
    assert_eq!(cold["index_loaded_from_disk"], true);
}

#[test]
fn ai_gauntlet_manifest_gate_requires_ultrasql_measurements() {
    let manifest = repo_json("benchmarks/results/latest/ai_benchmark_gauntlet_manifest.json");

    assert_eq!(manifest["status"], "passed");
    assert_eq!(manifest["passed"], true);
    let suites = manifest["suites"].as_array().expect("suites array");
    assert!(
        suites.iter().all(|suite| suite["status"] == "passed"),
        "required UltraSQL AI suites must be passed: {suites:?}"
    );
}

#[test]
fn ai_gauntlet_records_required_external_engines() {
    let script = repo_file("benchmarks/ai_benchmark_gauntlet.sh");

    assert!(script.contains("postgres17_pgvector"));
    assert!(script.contains("duckdb_list"));
    assert!(script.contains("clickhouse"));
}

#[test]
fn exact_topk_script_has_clickhouse_exact_scan_path() {
    let script = repo_file("benchmarks/vector_topk_exact.sh");

    assert!(script.contains("run_clickhouse"));
    assert!(script.contains("clickhouse_vector"));
    assert!(script.contains("arrayMap"));
    assert!(script.contains("CLICKHOUSE_BIN"));
}

#[test]
fn certification_runner_exposes_ai_gauntlet_profiles() {
    let script = repo_file("benchmarks/certify.sh");

    assert!(script.contains("ai-gauntlet"));
    assert!(script.contains("run_ai_gauntlet_smoke"));
    assert!(script.contains("run_ai_gauntlet_full"));
    assert!(script.contains("benchmarks/ai_benchmark_gauntlet.sh"));
}
