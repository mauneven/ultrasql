//! Contract tests for the exact vector top-k benchmark wrapper.

use std::fs;
use std::path::PathBuf;

#[test]
fn vector_topk_script_prefers_pgvector_then_duckdb_list_fallback() {
    let script_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("benchmarks/vector_topk_exact.sh");
    let script = fs::read_to_string(&script_path)
        .unwrap_or_else(|err| panic!("read {}: {err}", script_path.display()));

    assert!(script.contains("target/release/cross_compare_sql"));
    assert!(script.contains("CREATE EXTENSION IF NOT EXISTS vector"));
    assert!(script.contains("postgres17_pgvector"));
    assert!(script.contains("list_distance"));
    assert!(script.contains("array_distance"));
    assert!(script.contains("duckdb_list"));
    assert!(script.contains("\"status\": \"not_available\""));
    assert!(script.contains("\"reason\": reason"));
}

#[test]
fn vector_topk_script_certifies_recall_tail_latency_build_memory_and_index_size() {
    let script_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("benchmarks/vector_topk_exact.sh");
    let script = fs::read_to_string(&script_path)
        .unwrap_or_else(|err| panic!("read {}: {err}", script_path.display()));

    assert!(script.contains("REQUIRED_VECTOR_METRICS"));
    assert!(script.contains("recall_at_k"));
    assert!(script.contains("p50_latency_us"));
    assert!(script.contains("p95_latency_us"));
    assert!(script.contains("p99_latency_us"));
    assert!(script.contains("build_time_us"));
    assert!(script.contains("build_time_scope"));
    assert!(script.contains("memory_bytes"));
    assert!(script.contains("memory_status"));
    assert!(script.contains("index_size_bytes"));
    assert!(script.contains("index_size_status"));
    assert!(script.contains("not_applicable_exact_scan"));
}

#[test]
fn vector_ann_script_emits_recall_latency_build_and_memory_artifacts() {
    let script_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("benchmarks/vector_ann_hnsw.sh");
    let script = fs::read_to_string(&script_path)
        .unwrap_or_else(|err| panic!("read {}: {err}", script_path.display()));

    assert!(script.contains("ultrasql-bench ann-vector"));
    assert!(script.contains("VECTOR_ANN_ROWS"));
    assert!(script.contains("VECTOR_ANN_DIMS"));
    assert!(script.contains("recall_at_k"));
    assert!(script.contains("p50_latency_us"));
    assert!(script.contains("p95_latency_us"));
    assert!(script.contains("p99_latency_us"));
    assert!(script.contains("build_time_us"));
    assert!(script.contains("memory_bytes"));
}
