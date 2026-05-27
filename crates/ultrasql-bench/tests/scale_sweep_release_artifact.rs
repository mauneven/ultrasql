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
    assert!(script.contains("\"status\": \"not_available\""));
    assert!(script.contains("SCALE_SWEEP_APPEND"));
    assert!(script.contains("benchmarks/scripts/render_scale_sweep.py"));
    assert!(script.contains("run_ultrasql_fresh_insert_samples"));
    assert!(script.contains("10k-row INSERT chunks"));
    assert!(script.contains("benchmarks/scripts/run_clickhouse_writes.sh"));
    assert!(script.contains("ClickHouse"));
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
        "| Workload | Rows | UltraSQL | DuckDB | SQLite | PostgreSQL | ClickHouse | Fastest |"
    ));
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
fn scale_sweep_records_million_row_insert_and_all_current_wins() {
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
    assert!(
        rows.iter()
            .all(|row| row["fastest_engine"].as_str() == Some("ultrasql")),
        "every rendered scale-sweep row should currently have UltraSQL as fastest"
    );

    let rendered_md = repo_file("benchmarks/results/latest/scale-sweep/scale_sweep.md");
    assert!(rendered_md.contains("| INSERT throughput | 1 000 000 | **"));
    assert!(rendered_md.contains("| UPDATE throughput | 1 000 000 | **"));
}
