//! Contract tests for benchmark profile orchestration.

use std::fs;
use std::path::PathBuf;

fn repo_file(path: &str) -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(path);
    fs::read_to_string(&path).unwrap_or_else(|err| panic!("read {}: {err}", path.display()))
}

#[test]
fn certification_runner_splits_smoke_and_full_profiles() {
    let script = repo_file("benchmarks/certify.sh");

    assert!(script.contains("profile=\"${1:-smoke}\""));
    assert!(script.contains("case \"$profile\" in"));
    assert!(script.contains("smoke)"));
    assert!(script.contains("full)"));
    assert!(script.contains("target/release/regression-gate --stage current --smoke"));
    assert!(script.contains("VECTOR_ANN_ROWS=512"));
    assert!(script.contains("benchmarks/tpch_sf10_certify.sh"));
    assert!(script.contains("benchmarks/clickbench_certify.sh"));
    assert!(script.contains("benchmarks/tpcc_certify.sh"));
    assert!(script.contains("benchmarks/sysbench_certify.sh"));
    assert!(script.contains("benchmarks/vector_topk_exact.sh"));
    assert!(script.contains("late-materialization"));
    assert!(script.contains("benchmarks/late_materialization.sh"));
    assert!(script.contains("benchmark_certification_manifest.json"));
}

#[test]
fn ci_bench_uses_pr_smoke_and_scheduled_full_certifications() {
    let workflow = repo_file(".github/workflows/bench.yml");

    assert!(workflow.contains("name: benchmark smoke"));
    assert!(workflow.contains("if: github.event_name == 'pull_request'"));
    assert!(workflow.contains("benchmarks/certify.sh smoke"));
    assert!(workflow.contains("name: benchmark certifications"));
    assert!(workflow.contains(
        "if: github.event_name == 'schedule' || github.event_name == 'workflow_dispatch'"
    ));
    assert!(workflow.contains("benchmarks/certify.sh full"));
}

#[test]
fn tpcc_and_sysbench_certification_wrappers_write_artifacts() {
    let tpcc = repo_file("benchmarks/tpcc_certify.sh");
    let sysbench = repo_file("benchmarks/sysbench_certify.sh");

    assert!(tpcc.contains("tpcc_5types"));
    assert!(tpcc.contains("tpcc_certification.json"));
    assert!(tpcc.contains("runner_not_implemented"));
    assert!(tpcc.contains("exit 2"));

    assert!(sysbench.contains("sysbench_oltp_read_write"));
    assert!(sysbench.contains("cross_compare_sql"));
    assert!(sysbench.contains("--workload mixed-oltp"));
    assert!(sysbench.contains("sysbench_certification.json"));
}

#[test]
fn setup_missing_certifications_are_unavailable_not_failed() {
    let tpcb = repo_file("benchmarks/tpcb_certify.sh");
    let tpch = repo_file("benchmarks/tpch_sf10_certify.sh");

    assert!(tpcb.contains("missing_cross_engine_results"));
    assert!(tpcb.contains("TPCB_ALLOW_ULTRASQL_ONLY"));
    assert!(tpcb.contains("postgres_dsn_missing"));
    assert!(tpcb.contains("sys.exit(2 if reason == \"missing_cross_engine_results\""));

    assert!(tpch.contains("write_setup_summary \"data_dir_missing\""));
    assert!(tpch.contains("write_setup_summary \"duckdb_missing\""));
}
