//! README benchmark snapshot contract.

use std::fs;
use std::path::PathBuf;

fn repo_path(path: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(path)
}

fn repo_file(path: &str) -> String {
    let path = repo_path(path);
    fs::read_to_string(&path).unwrap_or_else(|err| panic!("read {}: {err}", path.display()))
}

#[test]
fn readme_db_snapshot_matches_latest_raw_artifacts() {
    let readme = repo_file("README.md");
    let scale_md = repo_file("benchmarks/results/latest/scale-sweep/scale_sweep.md");

    assert!(readme.contains("## Release-Artifact DB-vs-DB Benchmark"));
    assert!(readme.contains("benchmarks/run_scale_sweep.sh full"));
    assert!(readme.contains("PostgreSQL 17"));
    assert!(!readme.contains("## Current DB-vs-DB Snapshot"));

    for line in scale_md.lines().filter(|line| line.starts_with('|')) {
        assert!(
            readme.contains(line),
            "README missing release-artifact row: {line}"
        );
    }
}
