//! End-to-end tests for the SQLLogicTest runner binary.

use std::fs;
use std::path::{Path, PathBuf};
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

#[test]
fn in_process_mode_accepts_hash_threshold_and_hashed_results() {
    let bin = env!("CARGO_BIN_EXE_ultrasql-sqllogictest-runner");
    let suite = temp_artifact_path("ultrasql-slt-hash", "test");
    fs::write(
        &suite,
        "hash-threshold 1\n\nquery I nosort\nSELECT 1\n----\n1 values hashing to b026324c6904b2a9cb4b88d6d61c81d1\n",
    )
    .expect("write temporary hash SLT");

    let output = Command::new(bin)
        .arg("--mode")
        .arg("in-process")
        .arg(&suite)
        .output()
        .expect("run SQLLogicTest runner");

    assert!(
        output.status.success(),
        "runner failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("passed=1"), "stdout:\n{stdout}");
    assert!(stdout.contains("failed=0"), "stdout:\n{stdout}");
    let _ = fs::remove_file(suite);
}

#[test]
fn portable_corpus_includes_curated_filter_setops_shard() {
    let suite = repo_root().join("tests/slt/portable/filter_setops.slt");
    let text = fs::read_to_string(&suite).expect("curated portable SLT shard exists");
    assert!(
        text.contains("UltraSQL-authored portable SQLLogicTest shard"),
        "{} must document authored provenance",
        suite.display()
    );
    assert!(
        !text.contains("source=/"),
        "{} must not be an imported third-party dump",
        suite.display()
    );
    let case_count = count_slt_cases(&text);
    assert!(
        (12..=30).contains(&case_count),
        "{} must stay as a small reviewed shard, got {case_count} cases",
        suite.display()
    );
}

#[test]
fn portable_corpus_includes_scalar_expression_shard() {
    let suite = repo_root().join("tests/slt/portable/scalar_expressions.slt");
    let text = fs::read_to_string(&suite).expect("curated scalar expression SLT shard exists");
    assert!(
        text.contains("UltraSQL-authored portable SQLLogicTest shard"),
        "{} must document authored provenance",
        suite.display()
    );
    assert!(
        text.contains("portable scalar expression coverage"),
        "{} must name its reviewed scope",
        suite.display()
    );
    let case_count = count_slt_cases(&text);
    assert!(
        (8..=24).contains(&case_count),
        "{} must stay as a small reviewed shard, got {case_count} cases",
        suite.display()
    );
}

#[test]
fn imported_sqllogictest_shards_stay_small_and_provenanced() {
    let imported_root = repo_root().join("tests/slt/portable/imported");
    let mut imported_suites = 0usize;

    for entry in fs::read_dir(&imported_root).expect("imported SLT root exists") {
        let entry = entry.expect("read imported SLT suite entry");
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        imported_suites = imported_suites.saturating_add(1);
        for required in [
            "README.md",
            "IMPORT_MANIFEST.txt",
            "LICENSE.upstream",
            "upstream_commit.txt",
        ] {
            assert!(
                path.join(required).is_file(),
                "{} missing required provenance file {required}",
                path.display()
            );
        }
        let manifest = fs::read_to_string(path.join("IMPORT_MANIFEST.txt"))
            .expect("read imported SLT manifest");
        assert!(
            manifest.lines().any(|line| line.starts_with("commit=")),
            "{} manifest must pin upstream commit",
            path.display()
        );
        let file_count = manifest
            .lines()
            .filter(|line| line.starts_with("file="))
            .count();
        assert!(
            (1..=10).contains(&file_count),
            "{} imports {file_count} files; split reviewed shards instead of dumping suites",
            path.display()
        );
    }

    assert!(
        imported_suites > 0,
        "{} should contain at least one audited imported shard",
        imported_root.display()
    );
}

#[test]
fn postgres_compat_subset_preserves_public_provenance() {
    let subset = repo_root().join("tests/slt/postgres_compat/regression_subset");
    for required in ["README.md", "IMPORT_MANIFEST.txt", "LICENSE.upstream"] {
        assert!(
            subset.join(required).is_file(),
            "{} missing required provenance file {required}",
            subset.display()
        );
    }
    let manifest =
        fs::read_to_string(subset.join("IMPORT_MANIFEST.txt")).expect("read PostgreSQL manifest");
    assert!(
        manifest.contains("source=https://github.com/postgres/postgres"),
        "manifest:\n{manifest}"
    );
    assert!(
        manifest.contains("commit=ddd12d1a5c4d980c5f31dc7d096012547b724e55"),
        "manifest:\n{manifest}"
    );
    assert!(
        manifest.contains("derived_from=src/test/regress/sql/select.sql"),
        "manifest:\n{manifest}"
    );
    let shard = subset.join("select_basics.slt");
    let text = fs::read_to_string(&shard).expect("read PostgreSQL compatibility shard");
    assert!(
        text.contains("# ultrasql:skip row-value IN compatibility not implemented yet"),
        "{} must keep unsupported PostgreSQL regression coverage as explicit skip",
        shard.display()
    );
}

#[test]
fn skip_directive_requires_explicit_reason() {
    let bin = env!("CARGO_BIN_EXE_ultrasql-sqllogictest-runner");
    let suite = temp_artifact_path("ultrasql-slt-empty-skip", "test");
    fs::write(
        &suite,
        "# ultrasql:skip\n\nquery I nosort\nSELECT 1\n----\n1\n",
    )
    .expect("write temporary SLT with empty skip");

    let output = Command::new(bin)
        .arg("--mode")
        .arg("in-process")
        .arg(&suite)
        .output()
        .expect("run SQLLogicTest runner");

    let _ = fs::remove_file(suite);
    assert!(
        !output.status.success(),
        "runner unexpectedly accepted empty skip reason\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("skip directive requires an explicit reason"),
        "stderr:\n{stderr}"
    );
}

#[test]
fn skip_filter_requires_explicit_reason() {
    let bin = env!("CARGO_BIN_EXE_ultrasql-sqllogictest-runner");
    let suite = temp_artifact_path("ultrasql-slt-filter-reason", "test");
    let filter = temp_artifact_path("ultrasql-slt-filter-reason", "txt");
    fs::write(&suite, "query I nosort\nSELECT 1\n----\n1\n").expect("write temporary SLT");
    fs::write(&filter, "SELECT 1\n").expect("write skip filter without reason");

    let output = Command::new(bin)
        .arg("--mode")
        .arg("in-process")
        .arg("--skip-filter")
        .arg(&filter)
        .arg(&suite)
        .output()
        .expect("run SQLLogicTest runner");

    let _ = fs::remove_file(suite);
    let _ = fs::remove_file(filter);
    assert!(
        !output.status.success(),
        "runner unexpectedly accepted skip filter without reason\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("skip filter requires `pattern<TAB>reason`"),
        "stderr:\n{stderr}"
    );
}

#[test]
#[cfg(unix)]
fn differential_wrong_rows_fail() {
    let temp_dir = temp_dir("ultrasql-slt-bad-sqlite");
    fs::create_dir_all(&temp_dir).expect("create temp directory");
    let sqlite = temp_dir.join("sqlite3");
    write_executable(&sqlite, "#!/bin/sh\nprintf '999\\n'\nexit 0\n");
    let suite = temp_dir.join("wrong_rows.test");
    fs::write(&suite, "query I nosort\nSELECT 1\n----\n1\n").expect("write mismatch SLT");

    let path = format!(
        "{}:{}",
        temp_dir.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_ultrasql-sqllogictest-runner"))
        .env("PATH", path)
        .arg("--mode")
        .arg("in-process")
        .arg("--reference-engine")
        .arg("sqlite")
        .arg(&suite)
        .output()
        .expect("run SQLLogicTest runner");

    let _ = fs::remove_dir_all(&temp_dir);
    assert!(
        !output.status.success(),
        "runner unexpectedly accepted wrong reference rows\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("reference mismatch"), "stderr:\n{stderr}");
}

#[test]
fn in_process_mode_case_limit_bounds_suite_execution() {
    let bin = env!("CARGO_BIN_EXE_ultrasql-sqllogictest-runner");
    let suite = temp_artifact_path("ultrasql-slt-limit", "test");
    fs::write(
        &suite,
        "query I nosort\nSELECT 1\n----\n1\n\nquery I nosort\nSELECT 2\n----\n999\n",
    )
    .expect("write temporary limited SLT");

    let output = Command::new(bin)
        .arg("--mode")
        .arg("in-process")
        .arg("--case-limit")
        .arg("1")
        .arg(&suite)
        .output()
        .expect("run SQLLogicTest runner");

    assert!(
        output.status.success(),
        "runner failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("cases=1"), "stdout:\n{stdout}");
    assert!(stdout.contains("passed=1"), "stdout:\n{stdout}");
    assert!(stdout.contains("failed=0"), "stdout:\n{stdout}");
    let _ = fs::remove_file(suite);
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

#[cfg(unix)]
fn write_executable(path: &Path, text: &str) {
    fs::write(path, text).expect("write executable script");
    let mut perms = fs::metadata(path)
        .expect("stat executable script")
        .permissions();
    use std::os::unix::fs::PermissionsExt;
    perms.set_mode(0o755);
    fs::set_permissions(path, perms).expect("chmod executable script");
}

fn temp_artifact_path(prefix: &str, extension: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after Unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}-{}-{nanos}.{extension}", process::id()))
}

#[cfg(unix)]
fn temp_dir(prefix: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after Unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}-{}-{nanos}", process::id()))
}

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn count_slt_cases(text: &str) -> usize {
    text.lines()
        .map(str::trim)
        .filter(|line| line.starts_with("statement ") || line.starts_with("query "))
        .count()
}
