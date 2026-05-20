//! Differential SQLLogicTest script contract tests.
#![cfg(unix)]

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn differential_script_skips_postgres_without_reference_url() {
    let output = Command::new(differential_script())
        .env("SLT_DIFF_ENGINES", "postgres")
        .env("ULTRASQL_SLT_RUNNER", "/bin/false")
        .env_remove("ULTRASQL_SLT_REFERENCE_URL")
        .env_remove("POSTGRES_URL")
        .output()
        .expect("run differential SLT script");

    assert!(
        output.status.success(),
        "script failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("skip postgres: set ULTRASQL_SLT_REFERENCE_URL or POSTGRES_URL"),
        "stderr:\n{stderr}"
    );
}

#[test]
fn differential_script_rejects_non_portable_paths() {
    let output = Command::new(differential_script())
        .env("SLT_DIFF_ENGINES", "sqlite")
        .env("SLT_DIFF_PATHS", "tests/slt/postgres_compat")
        .env("ULTRASQL_SLT_RUNNER", "/bin/false")
        .output()
        .expect("run differential SLT script");

    assert!(
        !output.status.success(),
        "script unexpectedly succeeded\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("non-portable SQLLogicTest path"),
        "stderr:\n{stderr}"
    );
}

#[test]
fn differential_script_runs_or_skips_all_reference_engines() {
    let output = Command::new(differential_script())
        .env("SLT_DIFF_ENGINES", "postgres,duckdb,sqlite")
        .env(
            "ULTRASQL_SLT_RUNNER",
            env!("CARGO_BIN_EXE_ultrasql-sqllogictest-runner"),
        )
        .env_remove("ULTRASQL_SLT_REFERENCE_URL")
        .env_remove("POSTGRES_URL")
        .output()
        .expect("run differential SLT script");

    assert!(
        output.status.success(),
        "script failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("skip postgres: set ULTRASQL_SLT_REFERENCE_URL or POSTGRES_URL"),
        "stderr:\n{stderr}"
    );
    assert_engine_was_run_or_skipped(&stderr, "duckdb", "duckdb not found");
    assert_engine_was_run_or_skipped(&stderr, "sqlite", "sqlite3 not found");
}

#[test]
fn differential_script_does_not_leak_postgres_url_into_cli_engines() {
    let temp_dir = temp_dir("ultrasql-slt-diff-env");
    fs::create_dir_all(&temp_dir).expect("create temp directory");
    let sqlite = temp_dir.join("sqlite3");
    let runner = temp_dir.join("runner");
    write_executable(&sqlite, "#!/bin/sh\nexit 0\n");
    write_executable(
        &runner,
        "#!/bin/sh\nif [ -n \"${ULTRASQL_SLT_REFERENCE_URL+x}\" ]; then\n  echo leaked-reference-url >&2\n  exit 64\nfi\nexit 0\n",
    );

    let path = format!(
        "{}:{}",
        temp_dir.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let output = Command::new(differential_script())
        .env("PATH", path)
        .env("SLT_DIFF_ENGINES", "sqlite")
        .env("SLT_DIFF_PATHS", "tests/slt/portable/basic.slt")
        .env("ULTRASQL_SLT_RUNNER", &runner)
        .env("ULTRASQL_SLT_REFERENCE_URL", "postgres://example")
        .output()
        .expect("run differential SLT script");

    let _ = fs::remove_dir_all(&temp_dir);
    assert!(
        output.status.success(),
        "script failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn differential_script() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/slt/run_differential.sh")
}

fn assert_engine_was_run_or_skipped(stderr: &str, engine: &str, skip_reason: &str) {
    assert!(
        stderr.contains(&format!("run {engine} differential"))
            || stderr.contains(&format!("skip {engine}: {skip_reason}")),
        "stderr:\n{stderr}"
    );
}

fn write_executable(path: &Path, text: &str) {
    fs::write(path, text).expect("write executable script");
    let mut perms = fs::metadata(path)
        .expect("stat executable script")
        .permissions();
    perms.set_mode(0o755);
    fs::set_permissions(path, perms).expect("chmod executable script");
}

fn temp_dir(prefix: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after Unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}-{}-{nanos}", process::id()))
}
