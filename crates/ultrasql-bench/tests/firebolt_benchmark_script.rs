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
fn firebolt_sparse_script_declares_primary_index_pruning_workload() {
    let script = repo_file("benchmarks/firebolt_sparse_pruning.sh");

    assert!(script.contains("FIREBOLT_URL"));
    assert!(script.contains("FIREBOLT_SPARSE_ENGINES"));
    assert!(script.contains("firebolt_sparse_pruning_manifest.json"));
    assert!(script.contains("PRIMARY INDEX event_day, tenant_id, bucket"));
    assert!(script.contains("index_granularity"));
    assert!(script.contains("primary_index_pruning_evidence"));
    assert!(script.contains("--workload sparse-pruning"));
    assert!(script.contains("\"status\": \"not_available\""));
    assert!(script.contains("No Firebolt sparse-pruning benchmark claim"));
}

#[test]
fn firebolt_vector_script_declares_hnsw_vector_search_workload() {
    let script = repo_file("benchmarks/firebolt_vector_search.sh");

    assert!(script.contains("FIREBOLT_URL"));
    assert!(script.contains("FIREBOLT_VECTOR_ENGINES"));
    assert!(script.contains("firebolt_vector_search_manifest.json"));
    assert!(script.contains("benchmarks/vector_ann_hnsw.sh"));
    assert!(script.contains("CREATE INDEX"));
    assert!(script.contains("USING HNSW"));
    assert!(script.contains("vector_l2sq_ops"));
    assert!(script.contains("VECTOR_SEARCH"));
    assert!(script.contains("firebolt_hnsw"));
    assert!(script.contains("\"status\": \"not_available\""));
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
fn cross_compare_sql_exposes_sparse_pruning_workload() {
    let driver = repo_file("crates/ultrasql-bench/src/bin/cross_compare_sql.rs");

    assert!(driver.contains("SparsePruning"));
    assert!(driver.contains("firebolt_sparse_pruning"));
    assert!(driver.contains("event_day"));
    assert!(driver.contains("BETWEEN"));
}

#[test]
fn late_materialization_script_declares_firebolt_style_workload() {
    let script = repo_file("benchmarks/late_materialization.sh");
    let driver = repo_file("crates/ultrasql-bench/src/bin/cross_compare_sql.rs");
    let certify = repo_file("benchmarks/certify.sh");

    assert!(script.contains("--workload late-materialization"));
    assert!(script.contains("Late-materialization smoke/full runner"));
    assert!(driver.contains("LateMaterialization"));
    assert!(driver.contains("wide_payload_projection_with_selective_index_filter"));
    assert!(driver.contains("explain_late_materialization"));
    assert!(certify.contains("late-materialization"));
    assert!(certify.contains("run_late_materialization_smoke"));
}

#[test]
fn arena_and_certification_expose_firebolt_suite() {
    let arena = repo_file("benchmarks/arena.sh");
    let certify = repo_file("benchmarks/certify.sh");

    assert!(arena.contains("ultrasql,duckdb,clickhouse,postgres,firebolt"));
    assert!(arena.contains("aggregate-index"));
    assert!(arena.contains("benchmarks/firebolt_aggregate_index.sh"));
    assert!(arena.contains("ultrasql,firebolt"));
    assert!(arena.contains("sparse-pruning"));
    assert!(arena.contains("benchmarks/firebolt_sparse_pruning.sh"));
    assert!(arena.contains("firebolt-vector"));
    assert!(arena.contains("benchmarks/firebolt_vector_search.sh"));

    assert!(certify.contains("firebolt-aggregate"));
    assert!(certify.contains("run_firebolt_aggregate_smoke"));
    assert!(certify.contains("run_firebolt_aggregate_full"));
    assert!(certify.contains("benchmarks/firebolt_aggregate_index.sh"));
    assert!(certify.contains("firebolt-sparse-pruning"));
    assert!(certify.contains("run_firebolt_sparse_pruning_smoke"));
    assert!(certify.contains("run_firebolt_sparse_pruning_full"));
    assert!(certify.contains("benchmarks/firebolt_sparse_pruning.sh"));
    assert!(certify.contains("firebolt-vector"));
    assert!(certify.contains("run_firebolt_vector_smoke"));
    assert!(certify.contains("run_firebolt_vector_full"));
    assert!(certify.contains("benchmarks/firebolt_vector_search.sh"));
}

#[test]
fn firebolt_aggregate_script_has_valid_bash_syntax() {
    for script_name in [
        "benchmarks/firebolt_aggregate_index.sh",
        "benchmarks/firebolt_sparse_pruning.sh",
        "benchmarks/firebolt_vector_search.sh",
        "benchmarks/late_materialization.sh",
    ] {
        assert_firebolt_script_has_valid_bash_syntax(script_name);
    }
}

fn assert_firebolt_script_has_valid_bash_syntax(script_name: &str) {
    let script = repo_path(script_name);
    let Some(mut bash) = bash_command() else {
        eprintln!("skipping bash syntax check: Git Bash not found");
        return;
    };
    let status = bash
        .arg("-n")
        .arg(&script)
        .status()
        .unwrap_or_else(|err| panic!("run bash -n {}: {err}", script.display()));

    assert!(status.success(), "{script_name} must parse as bash");
}
