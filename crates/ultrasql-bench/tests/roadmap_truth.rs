//! Contract tests for benchmark and roadmap status text.

use std::fs;
use std::path::PathBuf;

fn repo_file(path: &str) -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(path);
    fs::read_to_string(&path).unwrap_or_else(|err| panic!("read {}: {err}", path.display()))
}

fn collapse_ws(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[test]
fn roadmap_ann_matrix_tracks_page_backed_indexes() {
    let roadmap = repo_file("ROADMAP.md");

    assert!(!roadmap.contains("| Runtime HNSW |"));
    assert!(!roadmap.contains("| Production HNSW |"));
    assert!(!roadmap.contains("| Runtime IVFFlat |"));
    assert!(!roadmap.contains("| Production IVFFlat |"));

    assert!(roadmap.contains("Page-backed HNSW"));
    assert!(roadmap.contains("Page-backed IVFFlat"));
    assert!(roadmap.contains("large-scale recovery certification"));
    assert!(roadmap.contains("WAL replay fuzz/property tests"));
    assert!(roadmap.contains("larger recall/latency artifacts"));
}

#[test]
fn roadmap_ai_gauntlet_lists_measured_runner_set() {
    let roadmap = repo_file("ROADMAP.md");
    let normalized = collapse_ws(&roadmap);

    assert!(normalized.contains("AI gauntlet measured artifacts"));
    for suite in [
        "exact top-k",
        "HNSW ANN recall/latency",
        "hybrid search latency",
        "filtered vector search",
        "RAG retrieval quality",
        "memory per million vectors",
        "ingestion throughput",
        "cold-start index load",
    ] {
        assert!(normalized.contains(suite), "missing suite {suite}");
    }
    assert!(!roadmap.contains("filtered ANN, bulk embedding load"));
}

#[test]
fn roadmap_firebolt_status_is_local_core_only() {
    let roadmap = repo_file("ROADMAP.md");

    assert!(roadmap.contains("target_ratio_ultrasql_vs_firebolt <= 1.0"));
    assert!(roadmap.contains("Firebolt primary-index pruning evidence"));
    assert!(roadmap.contains("local Firebolt Core smoke measured"));
    assert!(roadmap.contains("Firebolt is not_available"));
    assert!(!roadmap.contains("endpoint pending"));
    assert!(!roadmap.contains("Cloud-first"));
}

#[test]
fn roadmap_tpch_sf10_matches_complete_artifact() {
    let done = repo_file("DONE.md");
    let roadmap = repo_file("ROADMAP.md");
    let normalized = collapse_ws(&done);

    assert!(normalized.contains("TPC-H scale 10 (all 22 queries)"));
    assert!(normalized.contains("benchmarks/results/latest/tpch_sf10_certification.json"));
    assert!(normalized.contains("status passed"));
    assert!(normalized.contains("22/22 DuckDB and UltraSQL query timings"));
    assert!(!roadmap.contains("incomplete q21-only query set"));
    assert!(!roadmap.contains("has not been run to completion"));
    assert!(!roadmap.contains("remained in the `lineitem` load"));
    assert!(!roadmap.contains("- [ ] TPC-H scale 10:"));
}

#[test]
fn roadmap_tracks_columnar_scan_mvcc_contract() {
    let done = repo_file("DONE.md");
    let normalized = collapse_ws(&done);

    assert!(normalized.contains("Columnar scan path"));
    assert!(normalized.contains("heap rows remain the OLTP/MVCC source of truth"));
    assert!(normalized.contains("HeapAccess::column_cache"));
    assert!(normalized.contains("committed DML invalidation"));
}
