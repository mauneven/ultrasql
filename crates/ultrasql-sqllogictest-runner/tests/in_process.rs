//! End-to-end tests for the SQLLogicTest runner binary.

use std::fs;
use std::path::Path;
use std::process;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn in_process_mode_runs_portable_smoke_corpus() {
    let bin = env!("CARGO_BIN_EXE_ultrasql-sqllogictest-runner");
    let suite = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/slt/portable/basic.slt")
        .canonicalize()
        .expect("portable SLT corpus exists");

    let output = Command::new(bin)
        .arg("--mode")
        .arg("in-process")
        .arg(suite)
        .output()
        .expect("run SQLLogicTest runner");

    assert!(
        output.status.success(),
        "runner failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("passed=15"), "stdout:\n{stdout}");
    assert!(stdout.contains("failed=0"), "stdout:\n{stdout}");
}

#[test]
fn in_process_mode_compares_portable_smoke_against_sqlite_when_available() {
    if !command_available("sqlite3") {
        eprintln!("sqlite3 not available; skipping optional differential smoke");
        return;
    }
    run_reference_engine_smoke("sqlite");
}

#[test]
fn in_process_mode_compares_portable_smoke_against_duckdb_when_available() {
    if !command_available("duckdb") {
        eprintln!("duckdb not available; skipping optional differential smoke");
        return;
    }
    run_reference_engine_smoke("duckdb");
}

#[test]
fn in_process_mode_compares_against_multiple_reference_engines_when_available() {
    if !command_available("sqlite3") || !command_available("duckdb") {
        eprintln!("sqlite3 or duckdb not available; skipping optional multi-reference smoke");
        return;
    }

    let bin = env!("CARGO_BIN_EXE_ultrasql-sqllogictest-runner");
    let suite = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/slt/portable/basic.slt")
        .canonicalize()
        .expect("portable SLT corpus exists");

    let output = Command::new(bin)
        .arg("--mode")
        .arg("in-process")
        .arg("--reference-engine")
        .arg("sqlite")
        .arg("--reference-engine")
        .arg("duckdb")
        .arg(suite)
        .output()
        .expect("run SQLLogicTest runner");

    assert!(
        output.status.success(),
        "runner failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("passed=15"), "stdout:\n{stdout}");
    assert!(stdout.contains("failed=0"), "stdout:\n{stdout}");
}

#[test]
fn in_process_mode_writes_benchmark_artifact() {
    let bin = env!("CARGO_BIN_EXE_ultrasql-sqllogictest-runner");
    let suite = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/slt/portable/basic.slt")
        .canonicalize()
        .expect("portable SLT corpus exists");
    let output_path = temp_artifact_path("ultrasql-slt-benchmark", "json");
    let markdown_path = output_path.with_extension("md");
    let _ = fs::remove_file(&output_path);
    let _ = fs::remove_file(&markdown_path);

    let output = Command::new(bin)
        .arg("--mode")
        .arg("in-process")
        .arg("--benchmark-runs")
        .arg("2")
        .arg("--benchmark-output")
        .arg(&output_path)
        .arg(suite)
        .output()
        .expect("run SQLLogicTest runner benchmark");

    assert!(
        output.status.success(),
        "runner failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let json = fs::read_to_string(&output_path).expect("benchmark JSON artifact exists");
    assert!(
        json.contains("\"suite\": \"sqllogictest\""),
        "json:\n{json}"
    );
    assert!(json.contains("\"benchmark_runs\": 2"), "json:\n{json}");
    assert!(json.contains("\"name\": \"ultrasql\""), "json:\n{json}");
    let markdown = fs::read_to_string(&markdown_path).expect("benchmark Markdown artifact exists");
    assert!(
        markdown.contains("SQLLogicTest Speed Comparison"),
        "markdown:\n{markdown}"
    );
    let _ = fs::remove_file(output_path);
    let _ = fs::remove_file(markdown_path);
}

fn run_reference_engine_smoke(engine: &str) {
    let bin = env!("CARGO_BIN_EXE_ultrasql-sqllogictest-runner");
    let suite = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/slt/portable/basic.slt")
        .canonicalize()
        .expect("portable SLT corpus exists");

    let output = Command::new(bin)
        .arg("--mode")
        .arg("in-process")
        .arg("--reference-engine")
        .arg(engine)
        .arg(suite)
        .output()
        .expect("run SQLLogicTest runner");

    assert!(
        output.status.success(),
        "runner failed for {engine}\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("passed=15"), "stdout:\n{stdout}");
    assert!(stdout.contains("failed=0"), "stdout:\n{stdout}");
}

fn command_available(program: &str) -> bool {
    Command::new(program)
        .arg("-version")
        .output()
        .is_ok_and(|output| output.status.success())
}

fn temp_artifact_path(prefix: &str, extension: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after Unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}-{}-{nanos}.{extension}", process::id()))
}
