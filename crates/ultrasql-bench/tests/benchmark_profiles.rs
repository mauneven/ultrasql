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
    assert!(script.contains("rls-tenant"));
    assert!(script.contains("benchmarks/rls_tenant_certify.sh"));
    assert!(script.contains("VECTOR_ANN_ROWS=512"));
    assert!(script.contains("benchmarks/tpch_sf10_certify.sh"));
    assert!(script.contains("benchmarks/tpch_sf1_postgres_certify.sh"));
    assert!(script.contains("benchmarks/clickbench_certify.sh"));
    assert!(script.contains("TPCB_OUT_DIR=\"$OUT_DIR\""));
    assert!(script.contains("benchmarks/tpcc_certify.sh"));
    assert!(script.contains("TPCC_OUT_DIR=\"$OUT_DIR\""));
    assert!(script.contains("benchmarks/sysbench_certify.sh"));
    assert!(script.contains("chaos-recovery"));
    assert!(script.contains("benchmarks/chaos_recovery.sh"));
    assert!(script.contains("benchmarks/vector_topk_exact.sh"));
    assert!(script.contains("late-materialization"));
    assert!(script.contains("benchmarks/late_materialization.sh"));
    assert!(script.contains("benchmark_certification_manifest.json"));
}

#[test]
fn rls_tenant_certification_runner_writes_release_artifact() {
    let script = repo_file("benchmarks/rls_tenant_certify.sh");

    assert!(script.contains("rls_tenant_certification.json"));
    assert!(script.contains("cargo test -p \"$TEST_PACKAGE\" --test \"$TEST_TARGET\""));
    assert!(script.contains("INSERT SELECT"));
    assert!(script.contains("UPDATE new-row WITH CHECK"));
    assert!(script.contains("role scoping"));
    assert!(script.contains("restart persistence"));
    assert!(script.contains("not a benchmark claim"));
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
    assert!(workflow.contains("name: current commit scale sweep"));
    assert!(workflow.contains("benchmarks/run_scale_sweep.sh quick"));
    assert!(workflow.contains("current-commit-scale-sweep"));
}

#[test]
fn legacy_benchmark_runners_allow_isolated_output_dirs() {
    let kernel_runner = repo_file("benchmarks/run.sh");
    let wire_runner = repo_file("benchmarks/run_wire.sh");

    assert!(kernel_runner.contains("out=\"${BENCH_RUN_OUT_DIR:-benchmarks/results/latest}\""));
    assert!(kernel_runner.contains("raw=\"$out/raw\""));
    assert!(kernel_runner.contains("--output-md \"$out/results.md\""));
    assert!(kernel_runner.contains("--output-json \"$out/results.json\""));

    assert!(wire_runner.contains("out=\"${BENCH_WIRE_OUT_DIR:-benchmarks/results/latest}\""));
    assert!(wire_runner.contains("raw=\"$out/raw\""));
    assert!(wire_runner.contains("--output-md \"$out/results.md\""));
    assert!(wire_runner.contains("--output-json \"$out/results.json\""));
}

#[test]
fn tpcc_and_sysbench_certification_wrappers_write_artifacts() {
    let tpcb = repo_file("benchmarks/tpcb_certify.sh");
    let tpcc = repo_file("benchmarks/tpcc_certify.sh");
    let sysbench = repo_file("benchmarks/sysbench_certify.sh");

    assert!(tpcb.contains("OUT_DIR=\"${TPCB_OUT_DIR:-benchmarks/results/latest}\""));
    assert!(tpcb.contains("RAW_DIR=\"$OUT_DIR/raw\""));
    assert!(tpcb.contains("SUMMARY_OUT=\"$OUT_DIR/tpcb_certification.json\""));
    assert!(tpcb.contains("\"kernel_smoke_result\": kernel_smoke_result"));

    assert!(tpcc.contains("tpcc_5types"));
    assert!(tpcc.contains("tpcc_certification.json"));
    assert!(tpcc.contains("OUT_DIR=\"${TPCC_OUT_DIR:-benchmarks/results/latest}\""));
    assert!(tpcc.contains("RAW_DIR=\"$OUT_DIR/raw\""));
    assert!(tpcc.contains("SUMMARY_OUT=\"$OUT_DIR/tpcc_certification.json\""));
    assert!(tpcc.contains("POSTGRES_DSN"));
    assert!(tpcc.contains("TPCC_CONNECTIONS"));
    assert!(tpcc.contains("TMP_ULTRASQL_RESULT"));
    assert!(tpcc.contains("TMP_POSTGRES_RESULT"));
    assert!(tpcc.contains("target_not_met"));
    assert!(tpcc.contains("missing_cross_engine_results"));

    assert!(sysbench.contains("sysbench_oltp_read_write"));
    assert!(sysbench.contains("POSTGRES_DSN"));
    assert!(sysbench.contains("SYSBENCH_ALLOW_ULTRASQL_ONLY"));
    assert!(sysbench.contains("TMP_ULTRASQL_RESULT"));
    assert!(sysbench.contains("TMP_POSTGRES_RESULT"));
    assert!(sysbench.contains("sysbench_certification.json"));
    assert!(sysbench.contains("sysbench_smoke.json"));
    assert!(sysbench.contains("sysbench_oltp_read_write_smoke-ultrasql.json"));
    assert!(sysbench.contains("target_not_met"));
    assert!(sysbench.contains("missing_cross_engine_results"));
}

#[test]
fn setup_missing_certifications_are_unavailable_not_failed() {
    let tpcb = repo_file("benchmarks/tpcb_certify.sh");
    let tpch = repo_file("benchmarks/tpch_sf10_certify.sh");

    assert!(tpcb.contains("missing_cross_engine_results"));
    assert!(tpcb.contains("TPCB_ALLOW_ULTRASQL_ONLY"));
    assert!(tpcb.contains("TPCB_AUTO_POSTGRES"));
    assert!(tpcb.contains("postgres:17"));
    assert!(tpcb.contains("ultrasql-postgres-tpcb"));
    assert!(tpcb.contains("docker_without_desktop_creds"));
    assert!(tpcb.contains("TMP_ULTRASQL_RESULT"));
    assert!(tpcb.contains("mv \"$TMP_ULTRASQL_RESULT\" \"$ULTRASQL_RESULT\""));
    assert!(tpcb.contains("rm -f \"$TMP_ULTRASQL_RESULT\" \"$ULTRASQL_RESULT\""));
    assert!(tpcb.contains("postgres_dsn_missing"));
    assert!(tpcb.contains("sys.exit(2 if reason == \"missing_cross_engine_results\""));
    assert!(tpcb.contains("KERNEL_SMOKE_RESULT"));
    assert!(tpcb.contains("kernel_smoke_failed"));
    assert!(
        !tpcb.contains("tpcb_32conn-ultrasql-kernel.json\" 2>&1 || true"),
        "TPCB kernel smoke must write explicit JSON instead of swallowing failures"
    );

    assert!(tpch.contains("write_setup_summary \"data_dir_missing\""));
    assert!(tpch.contains("write_setup_summary \"duckdb_missing\""));
}

#[test]
fn tpch_sf10_runner_refuses_partial_and_writes_raw_atomically() {
    let tpch = repo_file("benchmarks/tpch_sf10_certify.sh");

    assert!(tpch.contains("partial_query_set_refused"));
    assert!(tpch.contains("TPCH_TIMEOUT_SECONDS"));
    assert!(tpch.contains("TMP_DUCKDB_OUT"));
    assert!(tpch.contains("TMP_ULTRA_OUT"));
    assert!(tpch.contains("mv \"$TMP_DUCKDB_OUT\" \"$DUCKDB_OUT\""));
    assert!(tpch.contains("mv \"$TMP_ULTRA_OUT\" \"$ULTRA_OUT\""));
    assert!(tpch.contains("trap cleanup EXIT"));
}

#[test]
fn tpch_sf1_postgres_runner_compares_same_host_postgres() {
    let tpch = repo_file("benchmarks/tpch_sf1_postgres_certify.sh");

    assert!(tpch.contains("tpch_sf1_postgres"));
    assert!(tpch.contains("POSTGRES_DSN"));
    assert!(tpch.contains("TMP_POSTGRES_OUT"));
    assert!(tpch.contains("TMP_ULTRA_OUT"));
    assert!(tpch.contains("mv \"$TMP_POSTGRES_OUT\" \"$POSTGRES_OUT\""));
    assert!(tpch.contains("mv \"$TMP_ULTRA_OUT\" \"$ULTRA_OUT\""));
    assert!(tpch.contains("partial_query_set_refused"));
    assert!(tpch.contains("ultrasql_gm <= postgres_gm"));
    assert!(tpch.contains("exit \"$summary_status\""));
    assert!(tpch.contains("ANALYZE;"));
    assert!(tpch.contains("autovacuum_enabled = false"));
    assert!(tpch.contains("CREATE INDEX tpch_sf1_lineitem_part_qty_idx"));
}

#[test]
fn clickbench_runner_declares_complete_engine_artifacts_and_host_metadata() {
    let clickbench = repo_file("benchmarks/clickbench_certify.sh");

    assert!(clickbench.contains("CLICKBENCH_ENGINES"));
    assert!(clickbench.contains("duckdb"));
    assert!(clickbench.contains("clickhouse"));
    assert!(clickbench.contains("firebolt"));
    assert!(clickbench.contains("clickbench-duckdb.json"));
    assert!(clickbench.contains("clickbench-clickhouse.json"));
    assert!(clickbench.contains("clickbench-firebolt.json"));
    assert!(clickbench.contains("\"schema_version\": 1"));
    assert!(clickbench.contains("\"host\""));
    assert!(clickbench.contains("\"status\": \"not_available\""));
    assert!(clickbench.contains("run_clickhouse()"));
    assert!(clickbench.contains("clickhouse/create.sql"));
    assert!(clickbench.contains("clickhouse_client_missing"));
    assert!(clickbench.contains("target_ratio_ultrasql_vs_postgres"));
    assert!(clickbench.contains("missing_required_engine_results"));
    assert!(clickbench.contains("target_not_met"));
    assert!(!clickbench.contains("runner_not_implemented_for_engine"));
}

#[test]
fn hot_path_profile_runner_covers_required_flamegraphs() {
    let script = repo_file("benchmarks/hot_path_profiles.sh");
    let docs = repo_file("BENCHMARKS.md");

    assert!(script.contains("hot_path_profiles_manifest.json"));
    assert!(script.contains("release-with-debug"));
    assert!(script.contains("flamegraph"));
    assert!(script.contains("\"status\""));
    assert!(script.contains("\"flamegraph\""));
    for workload in [
        "csv_copy",
        "parquet_filter",
        "vector_topk",
        "hash_aggregate",
        "joins",
        "tpch_q1",
        "tpch_q5",
        "tpch_q6",
    ] {
        assert!(
            script.contains(workload),
            "missing hot profile workload {workload}"
        );
        assert!(
            docs.contains(workload),
            "BENCHMARKS.md missing hot profile docs for {workload}"
        );
    }
}
