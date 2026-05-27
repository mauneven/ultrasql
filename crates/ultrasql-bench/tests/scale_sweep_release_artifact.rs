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
}

#[test]
fn readme_scale_sweep_matches_rendered_artifact() {
    let readme = repo_file("README.md");
    let scale_md = repo_file("benchmarks/results/latest/scale-sweep/scale_sweep.md");

    assert!(readme.contains("## Release-Artifact Scale Sweep"));
    assert!(readme.contains("benchmarks/run_scale_sweep.sh full"));
    assert!(readme.contains("v0.0.6 hits buffer-pool exhaustion"));

    for line in scale_md.lines().filter(|line| line.starts_with('|')) {
        assert!(
            readme.contains(line),
            "README missing scale-sweep row: {line}"
        );
    }
}

#[test]
fn scale_sweep_records_release_artifact_gaps_without_ranking_them() {
    let raw =
        repo_file("benchmarks/results/latest/scale-sweep/raw/insert_throughput_1m-ultrasql.json");
    let value: serde_json::Value =
        serde_json::from_str(&raw).expect("parse insert_throughput_1m-ultrasql");

    assert_eq!(value["engine"], "ultrasql");
    assert_eq!(value["status"], "not_available");
    assert_eq!(value["server_mode"], "external");
    assert!(
        value["reason"]
            .as_str()
            .expect("reason")
            .contains("buffer pool exhausted")
    );

    let rendered = repo_file("benchmarks/results/latest/scale-sweep/scale_sweep.md");
    assert!(rendered.contains("| INSERT throughput | 1 000 000 | - |"));
}
