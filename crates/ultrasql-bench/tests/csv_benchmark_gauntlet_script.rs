//! Contract tests for CSV benchmark gauntlet orchestration.

use std::fs;
use std::path::PathBuf;

fn repo_file(path: &str) -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(path);
    fs::read_to_string(&path).unwrap_or_else(|err| panic!("read {}: {err}", path.display()))
}

#[test]
fn csv_gauntlet_declares_required_workloads_and_engines() {
    let script = repo_file("benchmarks/csv_benchmark_gauntlet.sh");

    for workload in [
        "csv_cold_read",
        "csv_warm_read",
        "csv_copy_import",
        "csv_group_by",
        "csv_filter",
        "csv_join_table",
        "csv_malformed_behavior",
    ] {
        assert!(
            script.contains(workload),
            "CSV gauntlet script must declare workload {workload}"
        );
    }

    for engine in ["ultrasql", "duckdb", "clickhouse"] {
        assert!(
            script.contains(engine),
            "CSV gauntlet script must declare engine {engine}"
        );
    }

    assert!(script.contains("csv_benchmark_gauntlet_manifest.json"));
    assert!(script.contains("target/release/cross_compare_sql"));
    assert!(script.contains("\"status\":\"not_available\""));
    assert!(script.contains("No CSV benchmark claim"));
}

#[test]
fn cross_compare_sql_exposes_csv_workloads() {
    let driver = repo_file("crates/ultrasql-bench/src/bin/cross_compare_sql.rs");

    for workload in [
        "CsvColdRead",
        "CsvWarmRead",
        "CsvCopyImport",
        "CsvGroupBy",
        "CsvFilter",
        "CsvJoinTable",
        "CsvMalformedBehavior",
    ] {
        assert!(
            driver.contains(workload),
            "cross_compare_sql must expose {workload}"
        );
    }

    assert!(driver.contains("read_csv"));
    assert!(driver.contains("IGNORE_ERRORS = true"));
    assert!(driver.contains("csv_rejects"));
}

#[test]
fn certification_runner_exposes_csv_gauntlet_profiles() {
    let script = repo_file("benchmarks/certify.sh");

    assert!(script.contains("csv-gauntlet"));
    assert!(script.contains("run_csv_gauntlet_smoke"));
    assert!(script.contains("run_csv_gauntlet_full"));
    assert!(script.contains("benchmarks/csv_benchmark_gauntlet.sh"));
}
