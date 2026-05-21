//! Contract tests for Firebolt competitor benchmark orchestration.

mod support;

use std::fs;
use std::path::PathBuf;

use support::bash_command;

const LEGACY_FIREBOLT_REMOTE_ENV: &str = concat!("FIREBOLT", "_URL");

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

    assert!(!script.contains(LEGACY_FIREBOLT_REMOTE_ENV));
    assert!(script.contains("FIREBOLT_CORE_ENDPOINT"));
    assert!(script.contains("FIREBOLT_CORE_HELPER"));
    assert!(script.contains("wait"));
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
fn firebolt_aggregate_script_requires_matched_local_core_claims() {
    let script = repo_file("benchmarks/firebolt_aggregate_index.sh");

    assert!(script.contains("--workload dashboard-aggregate"));
    assert!(script.contains("--rows \"$ROWS\""));
    assert!(script.contains("--warmup \"$WARMUP\""));
    assert!(script.contains("--iters \"$ITERS\""));
    assert!(script.contains("FIREBOLT_ROWS=\"$ROWS\""));
    assert!(script.contains("FIREBOLT_WARMUP=\"$WARMUP\""));
    assert!(script.contains("FIREBOLT_ITERS=\"$ITERS\""));
    assert!(script.contains("tenant_id = row_id % 32"));
    assert!(script.contains("bucket = (row_id // 32) % 64"));
    assert!(script.contains("amount = ((row_id * 17) % 1000) - 500"));
    assert!(script.contains("SELECT tenant_id, bucket, SUM(amount), COUNT(*)"));
    assert!(script.contains("WHERE tenant_id = 7"));
    assert!(script.contains("GROUP BY tenant_id, bucket"));
    assert!(script.contains("\"both_engines_measured\": both_measured"));
    assert!(
        script.contains("\"claim_status\": \"eligible\" if both_measured else \"not_claimed\"")
    );
    assert!(script.contains("No Firebolt aggregate-index benchmark claim may be made unless both UltraSQL and local Firebolt Core are measured"));
}

#[test]
fn firebolt_sparse_script_declares_primary_index_pruning_workload() {
    let script = repo_file("benchmarks/firebolt_sparse_pruning.sh");

    assert!(!script.contains(LEGACY_FIREBOLT_REMOTE_ENV));
    assert!(script.contains("FIREBOLT_CORE_ENDPOINT"));
    assert!(script.contains("FIREBOLT_CORE_HELPER"));
    assert!(script.contains("wait"));
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

    assert!(!script.contains(LEGACY_FIREBOLT_REMOTE_ENV));
    assert!(script.contains("FIREBOLT_CORE_ENDPOINT"));
    assert!(script.contains("FIREBOLT_CORE_HELPER"));
    assert!(script.contains("wait"));
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
fn firebolt_core_local_helper_manages_local_docker_core_only() {
    let script = repo_file("benchmarks/firebolt_core_local.sh");
    let docs = repo_file("BENCHMARKS.md");

    for command in ["start", "stop", "status", "wait", "query", "clean"] {
        assert!(script.contains(command), "missing {command} command");
    }
    assert!(script.contains("http://127.0.0.1:3473"));
    assert!(script.contains("FIREBOLT_CORE_ENDPOINT"));
    assert!(script.contains("ghcr.io/firebolt-db/firebolt-core"));
    assert!(script.contains("docker run"));
    assert!(script.contains("SELECT 42;"));
    assert!(script.contains("wait_ready"));
    assert!(script.contains("clean --yes"));
    assert!(script.contains("refusing to clean"));
    assert!(!script.contains(LEGACY_FIREBOLT_REMOTE_ENV));
    assert!(docs.contains("closed-source Docker image"));
    assert!(docs.contains("not vendored"));
    assert!(docs.contains("benchmarks/firebolt_core_local.sh start"));
    assert!(docs.contains("benchmarks/firebolt_core_local.sh query \"SELECT 42;\""));
    assert!(docs.contains("benchmarks/firebolt_core_local.sh clean --yes"));
    assert!(docs.contains("same-host CPU model"));
    assert!(docs.contains("No claim policy"));
    assert!(!docs.contains(LEGACY_FIREBOLT_REMOTE_ENV));
}

#[test]
fn firebolt_core_clean_requires_explicit_yes() {
    let Some(mut bash) = bash_command() else {
        eprintln!("skipping clean safety check: Git Bash not found");
        return;
    };
    let data_dir = std::env::temp_dir().join(format!(
        "ultrasql-firebolt-clean-test-{}",
        std::process::id()
    ));
    fs::create_dir_all(&data_dir).expect("create temp firebolt data dir");
    fs::write(data_dir.join("sentinel"), b"keep").expect("write sentinel");

    let script = repo_path("benchmarks/firebolt_core_local.sh");
    let output = bash
        .arg(&script)
        .arg("clean")
        .env("FIREBOLT_CORE_DATA_DIR", &data_dir)
        .output()
        .unwrap_or_else(|err| panic!("run {} clean: {err}", script.display()));

    assert!(!output.status.success(), "clean without --yes must fail");
    assert!(
        data_dir.join("sentinel").exists(),
        "clean without --yes must not delete persistent data"
    );

    let mut bash = bash_command().expect("bash available after earlier check");
    let status = bash
        .arg(&script)
        .arg("clean")
        .arg("--yes")
        .env("FIREBOLT_CORE_DATA_DIR", &data_dir)
        .status()
        .unwrap_or_else(|err| panic!("run {} clean --yes: {err}", script.display()));

    assert!(status.success(), "clean --yes should succeed");
    assert!(
        !data_dir.exists(),
        "clean --yes should remove explicit data dir"
    );
}

#[test]
fn firebolt_artifacts_have_required_local_core_schema() {
    let required_fields = [
        "docker_image",
        "firebolt_version",
        "core_mode",
        "local_docker",
        "host_cpu",
        "host_memory",
        "dataset_rows",
        "samples",
        "median_us",
        "p95_us",
        "status",
        "docker_unavailable",
    ];
    for path in [
        "benchmarks/firebolt_aggregate_index.sh",
        "benchmarks/firebolt_sparse_pruning.sh",
        "benchmarks/firebolt_vector_search.sh",
        "benchmarks/results/latest/raw/firebolt_aggregate_index_10k-firebolt.json",
        "benchmarks/results/latest/raw/firebolt_sparse_pruning_10k-firebolt.json",
        "benchmarks/results/latest/raw/vector_ann_hnsw_512_8d_k10-firebolt_hnsw.json",
    ] {
        let text = repo_file(path);
        for field in required_fields {
            assert!(text.contains(field), "{path} missing {field}");
        }
    }
}

#[test]
fn cross_compare_sql_exposes_dashboard_aggregate_workload() {
    let driver = repo_file("crates/ultrasql-bench/src/bin/cross_compare_sql.rs");

    assert!(driver.contains("DashboardAggregate"));
    assert!(driver.contains("firebolt_aggregate_index"));
    assert!(driver.contains("CREATE AGGREGATING INDEX ix_dashboard_agg"));
    assert!(driver.contains("aggregating_index_used=true"));
    assert!(driver.contains("explain_aggregating_index"));
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
    assert!(script.contains("LATE_MAT_ENGINES"));
    assert!(script.contains("ultrasql-late,ultrasql-eager,duckdb,clickhouse"));
    assert!(script.contains("not_available"));
    assert!(driver.contains("LateMaterialization"));
    assert!(driver.contains("LATE_MAT_WIDE_COLUMNS: usize = 100"));
    assert!(driver.contains("wide_payload_projection_with_selective_index_filter"));
    assert!(driver.contains("eager_scan_median_us"));
    assert!(driver.contains("late_materialization_median_us"));
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
        "benchmarks/firebolt_core_local.sh",
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
