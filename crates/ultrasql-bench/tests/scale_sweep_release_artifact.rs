//! Release-artifact scale-sweep contract.

use std::fs;
use std::path::PathBuf;

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
fn scale_sweep_script_uses_external_release_artifact() {
    let script = repo_file("benchmarks/run_scale_sweep.sh");

    assert!(script.contains("scripts/install.sh"));
    assert!(script.contains("ULTRASQLD_BIN"));
    assert!(script.contains("--server \"127.0.0.1:${port}\""));
    assert!(script.contains("SCALE_SWEEP_PROFILE:-release-ship"));
    assert!(script.contains("cargo build --profile \"$PROFILE\""));
    assert!(script.contains("\"status\": \"not_available\""));
    assert!(script.contains("SCALE_SWEEP_APPEND"));
    assert!(script.contains("benchmarks/scripts/render_scale_sweep.py"));
    assert!(script.contains("run_ultrasql_fresh_insert_samples"));
    assert!(script.contains("10k-row INSERT chunks"));
    assert!(script.contains("benchmarks/scripts/run_clickhouse_writes.sh"));
    assert!(script.contains("ClickHouse"));
    assert!(script.contains("mixed-correctness"));
    assert!(script.contains("mixed_correctness_100k"));
    assert!(script.contains("run_competitor_script"));
    assert!(script.contains("record_competitor_failure"));
    assert!(!script.contains("run_duckdb_writes.sh \"$selector\" || true"));
    assert!(!script.contains("run_sqlite3_writes.sh \"$selector\" || true"));
    assert!(!script.contains("run_postgres_writes.sh \"$selector\" || true"));
    assert!(!script.contains("run_clickhouse_writes.sh \"$selector\" || true"));
}

#[test]
fn release_sweep_workflow_pins_manifest_release_version() {
    let workflow = repo_file(".github/workflows/bench.yml");

    assert!(workflow.contains("id: package-version"));
    assert!(workflow.contains("cargo metadata --no-deps --format-version 1"));
    assert!(
        workflow
            .contains(r#"ULTRASQL_RELEASE_VERSION: v${{ steps.package-version.outputs.version }}"#)
    );
    assert!(
        !workflow.contains("run: benchmarks/run_scale_sweep.sh quick"),
        "release sweep must not install a mutable latest release"
    );
}

#[test]
fn postgres_runner_does_not_swallow_database_setup_failures() {
    let script = repo_file("benchmarks/scripts/run_postgres_writes.sh");

    assert!(script.contains("ensure_database"));
    assert!(script.contains("pg_database"));
    assert!(script.contains("<<'SQL'"));
    assert!(script.contains("insert_throughput_*)"));
    assert!(script.contains("select_sum_*_i64)"));
    assert!(script.contains("filter_sum_*_i64)"));
    assert!(
        !script.contains("createdb -U \"$PGUSER\" \"$PGDATABASE\" 2>/dev/null || true"),
        "PostgreSQL runner must not hide createdb failures"
    );
    assert!(
        !script.contains("-c \"SELECT 1 FROM pg_database WHERE datname = :'db'\""),
        "PostgreSQL runner must not use psql -c with :'db'; this client does not expand it"
    );
}

#[test]
fn clickhouse_runner_requires_tcp_readiness_before_measurement() {
    let script = repo_file("benchmarks/scripts/run_clickhouse_writes.sh");

    assert!(script.contains("clickhouse server did not become ready"));
    assert!(script.contains("clickhouse_ready=0"));
    assert!(script.contains("clickhouse_ready=1"));
    assert!(script.contains("if [[ \"$clickhouse_ready\" -ne 1 ]]; then"));
    assert!(script.contains("mark_unavailable \"clickhouse server did not become ready"));
}

#[test]
fn scale_sweep_renders_clickhouse_as_first_class_competitor() {
    let renderer = repo_file("benchmarks/scripts/render_scale_sweep.py");
    let scale_md = repo_file("benchmarks/results/latest/scale-sweep/scale_sweep.md");
    let scale_json = repo_file("benchmarks/results/latest/scale-sweep/scale_sweep.json");
    let readme = repo_file("README.md");
    let benchmarks = repo_file("BENCHMARKS.md");

    assert!(renderer.contains("\"clickhouse\""));
    assert!(renderer.contains("\"ClickHouse\""));
    assert!(scale_md.contains(
        "| Workload | Rows | UltraSQL | DuckDB | ClickHouse | SQLite | PostgreSQL | Fastest |"
    ));
    assert!(scale_md.contains("% slower"));
    assert!(scale_json.contains("\"clickhouse\""));
    assert!(readme.contains("ClickHouse"));
    assert!(benchmarks.contains("PostgreSQL, and ClickHouse clients"));
    assert!(benchmarks.contains("benchmarks/scripts/run_clickhouse_writes.sh"));
}

#[test]
fn readme_scale_sweep_matches_rendered_artifact() {
    let readme = repo_file("README.md");
    let scale_md = repo_file("benchmarks/results/latest/scale-sweep/scale_sweep.md");

    assert!(readme.contains("## Release-Artifact DB-vs-DB Benchmark"));
    assert!(readme.contains("benchmarks/run_scale_sweep.sh full"));
    assert!(readme.contains("Fastest"));
    assert!(!readme.contains("buffer-pool exhaustion"));

    for line in scale_md.lines().filter(|line| line.starts_with('|')) {
        assert!(
            readme.contains(line),
            "README missing scale-sweep row: {line}"
        );
    }
}

#[test]
fn scale_sweep_verifies_mixed_correctness_before_ranking() {
    let renderer = repo_file("benchmarks/scripts/render_scale_sweep.py");
    let rendered_json = repo_file("benchmarks/results/latest/scale-sweep/scale_sweep.json");
    let rendered_md = repo_file("benchmarks/results/latest/scale-sweep/scale_sweep.md");
    let readme = repo_file("README.md");
    let raw_ultrasql =
        repo_file("benchmarks/results/latest/scale-sweep/raw/mixed_correctness_100k-ultrasql.json");

    assert!(renderer.contains("ANSWER_REQUIRED_WORKLOADS"));
    assert!(renderer.contains("answer_sha256"));
    assert!(renderer.contains("correctness_status"));

    let raw: serde_json::Value =
        serde_json::from_str(&raw_ultrasql).expect("parse mixed_correctness_100k-ultrasql");
    assert_eq!(raw["engine"], "ultrasql");
    assert_eq!(raw["status"], "measured");
    assert_eq!(raw["workload"], "mixed_correctness_100k");
    assert_eq!(raw["n_rows"], 100_000);
    assert_eq!(
        raw["answer_sha256"].as_str().expect("answer hash").len(),
        64
    );
    assert!(raw["answer"].is_array());

    let rendered: serde_json::Value =
        serde_json::from_str(&rendered_json).expect("parse rendered scale_sweep.json");
    let rows = rendered["rows"].as_array().expect("rows array");
    let mixed = rows
        .iter()
        .find(|row| {
            row["workload"].as_str() == Some("mixed_correctness")
                && row["n_rows"].as_u64() == Some(100_000)
        })
        .expect("mixed correctness row");
    assert_eq!(mixed["correctness_status"].as_str(), Some("verified"));
    assert_eq!(
        mixed["answer_sha256"].as_str().expect("answer hash").len(),
        64
    );
    assert_eq!(mixed["fastest_engine"].as_str(), Some("ultrasql"));

    assert!(rendered_md.contains("| Mixed correctness | 100 000 | **"));
    assert!(readme.contains("| Mixed correctness | 100 000 | **"));
}

#[test]
fn scale_sweep_records_million_row_insert_and_update_wins() {
    let raw =
        repo_file("benchmarks/results/latest/scale-sweep/raw/insert_throughput_1m-ultrasql.json");
    let value: serde_json::Value =
        serde_json::from_str(&raw).expect("parse insert_throughput_1m-ultrasql");

    assert_eq!(value["engine"], "ultrasql");
    assert_eq!(value["status"], "measured");
    assert_eq!(value["server_mode"], "external");
    assert_eq!(value["n_rows"], 1_000_000);
    assert!(value["median_us"].as_f64().expect("median_us") > 0.0);

    let rendered_json = repo_file("benchmarks/results/latest/scale-sweep/scale_sweep.json");
    let rendered: serde_json::Value =
        serde_json::from_str(&rendered_json).expect("parse rendered scale_sweep.json");
    let rows = rendered["rows"].as_array().expect("rows array");
    let one_m_insert = rows
        .iter()
        .find(|row| {
            row["workload"].as_str() == Some("insert_throughput")
                && row["n_rows"].as_u64() == Some(1_000_000)
        })
        .expect("1m insert row");
    assert_eq!(one_m_insert["fastest_engine"].as_str(), Some("ultrasql"));

    let rendered_md = repo_file("benchmarks/results/latest/scale-sweep/scale_sweep.md");
    assert!(rendered_md.contains("| INSERT throughput | 1 000 000 | **"));
    assert!(rendered_md.contains("| UPDATE throughput | 1 000 000 | **"));
}

#[test]
fn scale_sweep_records_ultrasql_fastest_for_every_published_row() {
    let rendered_json = repo_file("benchmarks/results/latest/scale-sweep/scale_sweep.json");
    let rendered: serde_json::Value =
        serde_json::from_str(&rendered_json).expect("parse rendered scale_sweep.json");
    let rows = rendered["rows"].as_array().expect("rows array");
    let gaps = rows
        .iter()
        .filter(|row| row["fastest_engine"].as_str() != Some("ultrasql"))
        .map(|row| {
            format!(
                "{} rows={} fastest={}",
                row["workload"].as_str().unwrap_or("<unknown>"),
                row["n_rows"].as_u64().unwrap_or(0),
                row["fastest_engine"].as_str().unwrap_or("<none>")
            )
        })
        .collect::<Vec<_>>();

    assert!(gaps.is_empty(), "non-UltraSQL fastest rows: {gaps:?}");
}

#[test]
fn current_scale_sweep_records_ultrasql_fastest_for_every_row() {
    let current_path = repo_path("benchmarks/results/latest/scale-sweep-current/scale_sweep.json");
    if !current_path.exists() {
        return;
    }
    let rendered_json = fs::read_to_string(&current_path)
        .unwrap_or_else(|err| panic!("read {}: {err}", current_path.display()));
    let rendered: serde_json::Value =
        serde_json::from_str(&rendered_json).expect("parse current scale_sweep.json");
    let rows = rendered["rows"].as_array().expect("rows array");
    let gaps = rows
        .iter()
        .filter(|row| row["fastest_engine"].as_str() != Some("ultrasql"))
        .map(|row| {
            format!(
                "{} rows={} fastest={}",
                row["workload"].as_str().unwrap_or("<unknown>"),
                row["n_rows"].as_u64().unwrap_or(0),
                row["fastest_engine"].as_str().unwrap_or("<none>")
            )
        })
        .collect::<Vec<_>>();

    assert!(
        gaps.is_empty(),
        "current non-UltraSQL fastest rows: {gaps:?}"
    );
}
