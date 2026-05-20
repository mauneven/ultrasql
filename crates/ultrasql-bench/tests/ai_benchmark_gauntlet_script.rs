//! Contract tests for AI benchmark gauntlet orchestration.

use std::fs;
use std::path::PathBuf;

fn repo_file(path: &str) -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(path);
    fs::read_to_string(&path).unwrap_or_else(|err| panic!("read {}: {err}", path.display()))
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
    assert!(script.contains("VECTOR_TOPK_RENDER_RESULTS=0"));
    assert!(script.contains("ai_benchmark_gauntlet_manifest.json"));
    assert!(script.contains("\"status\": \"not_available\""));
    assert!(script.contains("runner_not_implemented"));
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
