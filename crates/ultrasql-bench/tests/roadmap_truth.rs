//! Contract tests for benchmark and open-work (TODO.md) status text.

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
    let todo = repo_file("TODO.md");

    assert!(!todo.contains("| Runtime HNSW |"));
    assert!(!todo.contains("| Production HNSW |"));
    assert!(!todo.contains("| Runtime IVFFlat |"));
    assert!(!todo.contains("| Production IVFFlat |"));

    assert!(todo.contains("Page-backed HNSW"));
    assert!(todo.contains("Page-backed IVFFlat"));
    assert!(todo.contains("large-scale recovery certification"));
    assert!(todo.contains("WAL replay fuzz/property tests"));
    assert!(todo.contains("larger recall/latency artifacts"));
}

#[test]
fn roadmap_ai_gauntlet_lists_measured_runner_set() {
    let todo = repo_file("TODO.md");
    let normalized = collapse_ws(&todo);

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
    assert!(!todo.contains("filtered ANN, bulk embedding load"));
}

#[test]
fn roadmap_firebolt_status_is_local_core_only() {
    let todo = repo_file("TODO.md");

    assert!(todo.contains("target_ratio_ultrasql_vs_firebolt <= 1.0"));
    assert!(todo.contains("Firebolt primary-index pruning evidence"));
    assert!(todo.contains("local Firebolt Core smoke measured"));
    assert!(todo.contains("Firebolt is not_available"));
    assert!(!todo.contains("endpoint pending"));
    assert!(!todo.contains("Cloud-first"));
}

#[test]
fn roadmap_tpch_sf10_matches_complete_artifact() {
    let done = repo_file("DONE.md");
    let todo = repo_file("TODO.md");
    let normalized = collapse_ws(&done);

    assert!(normalized.contains("TPC-H scale 10 (all 22 queries)"));
    assert!(normalized.contains("benchmarks/results/latest/tpch_sf10_certification.json"));
    assert!(normalized.contains("status passed"));
    assert!(normalized.contains("22/22 DuckDB and UltraSQL query timings"));
    assert!(!todo.contains("incomplete q21-only query set"));
    assert!(!todo.contains("has not been run to completion"));
    assert!(!todo.contains("remained in the `lineitem` load"));
    assert!(!todo.contains("- [ ] TPC-H scale 10:"));
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
