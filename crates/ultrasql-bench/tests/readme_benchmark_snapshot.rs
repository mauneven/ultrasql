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

fn median_for(workload: &str, engine: &str) -> f64 {
    let raw = repo_file(&format!(
        "benchmarks/results/latest/raw/{workload}-{engine}.json"
    ));
    let value: serde_json::Value =
        serde_json::from_str(&raw).unwrap_or_else(|err| panic!("parse {workload}-{engine}: {err}"));
    value["median_us"]
        .as_f64()
        .unwrap_or_else(|| panic!("missing median_us for {workload}-{engine}"))
}

fn readme_duration(us: f64, per_op: bool) -> String {
    let suffix = if per_op { "/op" } else { "" };
    if us < 1_000.0 {
        format!("{us:.2} µs{suffix}")
    } else {
        format!("{:.2} ms{suffix}", us / 1_000.0)
    }
}

#[test]
fn readme_db_snapshot_matches_latest_raw_artifacts() {
    let readme = repo_file("README.md");
    assert!(readme.contains("## Current DB-vs-DB Snapshot"));
    assert!(readme.contains("benchmarks/run_wire.sh full"));
    assert!(readme.contains("PostgreSQL 14.22 Homebrew"));

    for (workload, per_op) in [
        ("insert_throughput_10k", false),
        ("select_scan_10k", false),
        ("select_sum_65k_i64", false),
        ("select_avg_1m_i64", false),
        ("filter_sum_1m_i64", false),
        ("update_throughput_10k", false),
        ("delete_throughput_10k", false),
        ("mixed_oltp_pgbench_like", true),
        ("window_row_number_65k_i64", false),
    ] {
        for engine in ["ultrasql", "duckdb", "sqlite3", "postgres17"] {
            let formatted = readme_duration(median_for(workload, engine), per_op);
            assert!(
                readme.contains(&formatted),
                "README missing {workload}-{engine} median {formatted}"
            );
        }
    }
}
