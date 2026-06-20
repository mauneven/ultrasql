//! Contract tests for CSV benchmark gauntlet orchestration.

use std::fs;
use std::path::PathBuf;

fn repo_file(path: &str) -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(path);
    fs::read_to_string(&path).unwrap_or_else(|err| panic!("read {}: {err}", path.display()))
}

/// Full source of the `cross_compare_sql` driver: the binary entrypoint plus
/// every file in its `cross_compare_sql_support/` module directory. The driver
/// is split across modules, so workload definitions may live in any of them.
fn cross_compare_sql_driver_source() -> String {
    let mut src = repo_file("crates/ultrasql-bench/src/bin/cross_compare_sql.rs");
    let support_dir =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/bin/cross_compare_sql_support");
    let mut paths: Vec<PathBuf> = fs::read_dir(&support_dir)
        .unwrap_or_else(|err| panic!("read dir {}: {err}", support_dir.display()))
        .map(|entry| entry.expect("dir entry").path())
        .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("rs"))
        .collect();
    paths.sort();
    for p in paths {
        src.push('\n');
        src.push_str(
            &fs::read_to_string(&p).unwrap_or_else(|err| panic!("read {}: {err}", p.display())),
        );
    }
    src
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
fn csv_gauntlet_sets_ultrasql_read_limit_for_generated_full_dataset() {
    let script = repo_file("benchmarks/csv_benchmark_gauntlet.sh");

    assert!(
        script.contains("CSV_GAUNTLET_CSV_READ_LIMIT_BYTES"),
        "CSV gauntlet must expose an override for the generated CSV read limit"
    );
    assert!(
        script.contains("ULTRASQL_CSV_LOCAL_READ_LIMIT_BYTES"),
        "UltraSQL CSV runs must set the local file read limit explicitly"
    );
}

#[test]
fn cross_compare_sql_exposes_csv_workloads() {
    let driver = cross_compare_sql_driver_source();

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
