//! Contract tests for public benchmark arena orchestration.

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
fn arena_script_declares_public_suites_and_engines() {
    let script = repo_file("benchmarks/arena.sh");

    assert!(script.contains("benchmarks/arena.sh --engines ultrasql,duckdb,clickhouse,postgres"));
    assert!(script.contains("benchmark_arena_manifest.json"));
    assert!(script.contains("benchmark_arena_artifacts.md"));

    for suite in [
        "csv",
        "parquet",
        "object-parquet-range",
        "tpch",
        "clickbench",
        "sqllogictest",
        "vector",
        "jsonb",
    ] {
        assert!(
            script.contains(suite),
            "public arena must declare suite {suite}"
        );
    }

    for engine in ["ultrasql", "duckdb", "clickhouse", "postgres"] {
        assert!(
            script.contains(engine),
            "public arena must declare engine {engine}"
        );
    }
}

#[test]
fn arena_script_composes_existing_runners_without_claiming_winners() {
    let script = repo_file("benchmarks/arena.sh");

    assert!(script.contains("benchmarks/csv_benchmark_gauntlet.sh"));
    assert!(script.contains("benchmarks/tpch_sf10_certify.sh"));
    assert!(script.contains("benchmarks/clickbench_certify.sh"));
    assert!(script.contains("BENCH_CERT_OUT_DIR=\"$OUT_DIR\""));
    assert!(script.contains("benchmarks/slt_speed_compare.sh"));
    assert!(script.contains("benchmarks/ai_benchmark_gauntlet.sh"));
    assert!(script.contains("benchmarks/object_parquet_range.sh"));
    assert!(script.contains("run_parquet_suite"));
    assert!(script.contains("run_object_parquet_range_suite"));
    assert!(script.contains("--workload parquet-smoke"));
    assert!(script.contains("object-parquet-range:ultrasql"));
    assert!(script.contains("if (( ${#selected[@]} > 0 )); then"));
    assert!(script.contains("slt_refs=\"ultrasql\""));
    assert!(script.contains("SLT_BENCH_ENGINES=\"$slt_refs\""));

    assert!(script.contains("\"status\": \"not_available\""));
    assert!(script.contains("No benchmark claim"));
    assert!(script.contains("artifacts only"));
    assert!(!script.contains("results-render"));
    assert!(!script.contains("\"parquet\" \\\n        \"runner_not_implemented\""));
}

#[test]
fn object_parquet_range_script_declares_range_certification() {
    let script = repo_file("benchmarks/object_parquet_range.sh");
    let certify = repo_file("benchmarks/certify.sh");

    assert!(script.contains("object_parquet_range_manifest.json"));
    assert!(script.contains("--workload object-parquet-range"));
    assert!(script.contains("whole-object fetch"));
    assert!(script.contains("ranged object-store reads"));
    assert!(certify.contains("object-parquet-range"));
    assert!(certify.contains("run_object_parquet_range_smoke"));
    assert!(certify.contains("benchmarks/object_parquet_range.sh"));
}

#[test]
fn object_parquet_range_script_has_valid_bash_syntax() {
    let script = repo_path("benchmarks/object_parquet_range.sh");
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
        "object_parquet_range.sh must parse as bash"
    );
}

#[test]
fn arena_script_has_valid_bash_syntax() {
    let script = repo_path("benchmarks/arena.sh");
    let Some(mut bash) = bash_command() else {
        eprintln!("skipping bash syntax check: Git Bash not found");
        return;
    };
    let status = bash
        .arg("-n")
        .arg(&script)
        .status()
        .unwrap_or_else(|err| panic!("run bash -n {}: {err}", script.display()));

    assert!(status.success(), "arena.sh must parse as bash");
}
