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

// `roadmap_tpch_sf10_matches_complete_artifact` and
// `roadmap_tracks_columnar_scan_mvcc_contract` were removed on 2026-07-02.
// The first asserted the TPC-H SF10 "status passed" certification that has
// since been WITHDRAWN as invalid (it measured removed answer-cache fast
// paths, not query execution); a test must not re-enforce a retracted claim.
// Both read DONE.md, which was deleted in the same truthfulness pass. TPC-H
// re-certification is tracked as open work in TODO.md.
#[test]
fn roadmap_tpch_claims_are_withdrawn_not_asserted_as_passing() {
    let todo = repo_file("TODO.md");
    let benchmarks = repo_file("BENCHMARKS.md");
    // TODO must record TPC-H as withdrawn/open, never as a passing result.
    assert!(
        todo.contains("withdrawn"),
        "TODO must record the TPC-H retraction"
    );
    // The methodology doc must carry the retraction and must not resurrect a
    // passing ratio claim in prose.
    assert!(
        benchmarks.contains("Retraction") || benchmarks.contains("withdrawn"),
        "BENCHMARKS.md must document the TPC-H retraction"
    );
}
