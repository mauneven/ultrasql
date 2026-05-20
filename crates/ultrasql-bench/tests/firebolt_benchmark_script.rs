//! Contract tests for Firebolt competitor benchmark orchestration.

mod support;

use std::fs;
use std::path::PathBuf;

use support::bash_command;

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
fn firebolt_aggregate_script_declares_competitor_and_index_workload() {
    let script = repo_file("benchmarks/firebolt_aggregate_index.sh");

    assert!(script.contains("FIREBOLT_URL"));
    assert!(script.contains("FIREBOLT_AGG_ENGINES"));
    assert!(script.contains("firebolt_aggregate_index_manifest.json"));
    assert!(script.contains("CREATE AGGREGATING INDEX"));
    assert!(script.contains("Aggregating Index"));
    assert!(script.contains("output_format=JSON_Compact"));
    assert!(script.contains("target/release/cross_compare_sql"));
    assert!(script.contains("\"status\": \"not_available\""));
    assert!(script.contains("No Firebolt aggregate-index benchmark claim"));
}

#[test]
fn cross_compare_sql_exposes_dashboard_aggregate_workload() {
    let driver = repo_file("crates/ultrasql-bench/src/bin/cross_compare_sql.rs");

    assert!(driver.contains("DashboardAggregate"));
    assert!(driver.contains("firebolt_aggregate_index"));
    assert!(driver.contains("GROUP BY tenant_id, bucket"));
    assert!(driver.contains("COUNT(*)"));
}

#[test]
fn arena_and_certification_expose_firebolt_suite() {
    let arena = repo_file("benchmarks/arena.sh");
    let certify = repo_file("benchmarks/certify.sh");

    assert!(arena.contains("ultrasql,duckdb,clickhouse,postgres,firebolt"));
    assert!(arena.contains("aggregate-index"));
    assert!(arena.contains("benchmarks/firebolt_aggregate_index.sh"));
    assert!(arena.contains("ultrasql,firebolt"));

    assert!(certify.contains("firebolt-aggregate"));
    assert!(certify.contains("run_firebolt_aggregate_smoke"));
    assert!(certify.contains("run_firebolt_aggregate_full"));
    assert!(certify.contains("benchmarks/firebolt_aggregate_index.sh"));
}

#[test]
fn firebolt_aggregate_script_has_valid_bash_syntax() {
    let script = repo_path("benchmarks/firebolt_aggregate_index.sh");
    let Some(mut bash) = bash_command() else {
        eprintln!("skipping bash syntax check: Git Bash not found");
        return;
    };
    let status = bash
        .arg("-n")
        .arg(&script)
        .status()
        .unwrap_or_else(|err| panic!("run bash -n {}: {err}", script.display()));

    assert!(
        status.success(),
        "firebolt_aggregate_index.sh must parse as bash"
    );
}
