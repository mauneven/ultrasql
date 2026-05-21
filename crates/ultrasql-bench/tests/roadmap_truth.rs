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

    assert!(roadmap.contains("| Page-backed HNSW |"));
    assert!(roadmap.contains("| Page-backed IVFFlat |"));
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

    assert!(roadmap.contains("local Firebolt Core runner exists"));
    assert!(roadmap.contains("local Firebolt Core run pending measured artifact"));
    assert!(!roadmap.contains("endpoint pending"));
    assert!(!roadmap.contains("Cloud-first"));
}
