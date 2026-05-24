//! Release-hardening documentation and runner contracts.

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
fn catalog_upgrade_story_is_documented_and_enforced() {
    let docs = repo_file("docs/catalog-upgrades.md");
    let source = repo_file("crates/ultrasql-server/src/catalog_version.rs");

    assert!(docs.contains("catalog.version"));
    assert!(docs.contains("current catalog version is `1`"));
    assert!(docs.contains("startup refuses"));
    assert!(docs.contains("offline migrator"));
    assert!(source.contains("CURRENT_CATALOG_VERSION: u32 = 1"));
    assert!(source.contains("newer than this UltraSQL binary supports"));
}

#[test]
fn backup_restore_smoke_runner_documents_real_verification() {
    let script = repo_file("benchmarks/backup_restore_smoke.sh");
    let docs = repo_file("docs/backup-restore.md");

    assert!(script.contains("backup_restore_smoke_manifest.json"));
    assert!(script.contains("ultrasql --basebackup"));
    assert!(script.contains("ultrasql --pg-dump"));
    assert!(script.contains("ultrasql --pg-restore"));
    assert!(script.contains("SELECT COUNT(*) FROM backup_restore_smoke"));
    assert!(script.contains("SELECT payload FROM backup_restore_smoke WHERE id = 2"));
    assert!(script.contains("\"row_count_verified\""));
    assert!(script.contains("\"index_query_verified\""));
    assert!(script.contains("\"status\": \"not_available\""));
    assert!(docs.contains("row counts"));
    assert!(docs.contains("index query"));
    assert!(docs.contains("benchmarks/backup_restore_smoke.sh"));
}

#[test]
fn backup_restore_smoke_script_is_valid_bash() {
    let Some(mut bash) = bash_command() else {
        eprintln!("skipping backup/restore bash syntax check: Git Bash not found");
        return;
    };
    let status = bash
        .arg("-n")
        .arg(repo_path("benchmarks/backup_restore_smoke.sh"))
        .status()
        .expect("run bash -n backup_restore_smoke.sh");
    assert!(status.success(), "backup_restore_smoke.sh must parse");
}

#[test]
fn configuration_docs_cover_release_knobs() {
    let docs = repo_file("docs/configuration.md");

    for needle in [
        "Memory",
        "WAL",
        "Object-store",
        "ANN",
        "Benchmark modes",
        "RLS",
        "ULTRASQL_DATA_DIR",
        "ULTRASQL_OPS_LISTEN",
        "work_mem",
        "FIREBOLT_CORE_ENDPOINT",
    ] {
        assert!(docs.contains(needle), "configuration docs missing {needle}");
    }
}

#[test]
fn security_ethics_audit_lists_verifiable_release_checks() {
    let docs = repo_file("docs/security-ethics-audit.md");

    for needle in [
        "No tool attribution credits",
        "No proprietary tests",
        "No copied closed-source code",
        "No fake benchmark claims",
        "rg -n",
        "Firebolt Core is a closed-source Docker image",
    ] {
        assert!(
            docs.contains(needle),
            "security/ethics audit missing {needle}"
        );
    }
}

#[test]
fn committed_slt_speed_artifacts_do_not_publish_winners() {
    for path in [
        "benchmarks/results/latest/slt_speed_comparison.json",
        "benchmarks/results/latest/slt_authored_speed_comparison.json",
        "benchmarks/results/latest/slt_hydromatic_smoke_comparison.json",
    ] {
        let artifact = repo_file(path);

        assert!(
            !artifact.contains("\"winner\""),
            "{path} must not publish a winner field"
        );
        assert!(
            artifact.contains("\"policy\""),
            "{path} must record a no-claim policy"
        );
    }
}

#[test]
fn ci_split_matches_release_policy() {
    let ci = repo_file(".github/workflows/ci.yml");
    let bench = repo_file(".github/workflows/bench.yml");
    let fuzz = repo_file(".github/workflows/fuzz.yml");
    let sanitizers = repo_file(".github/workflows/sanitizers.yml");
    let docs = repo_file("docs/release-checklist.md");

    assert!(ci.contains("cargo fmt --all -- --check"));
    assert!(ci.contains("cargo clippy --workspace --all-targets --all-features -- -D warnings"));
    assert!(ci.contains("cargo test  --workspace --all-features"));
    assert!(bench.contains("benchmarks/certify.sh smoke"));
    assert!(bench.contains("benchmarks/certify.sh full"));
    assert!(fuzz.contains("-max_total_time=900"));
    assert!(sanitizers.contains("cargo +nightly test --workspace -Zbuild-std"));
    assert!(sanitizers.contains("cargo +nightly test \\"));
    assert!(sanitizers.contains("-p ultrasql-executor"));
    assert!(sanitizers.contains("cargo +nightly miri setup"));
    assert!(sanitizers.contains("run miri smoke on memory-safety-sensitive crates"));
    assert!(sanitizers.contains(
        "cargo +nightly miri test -p ultrasql-storage page::tests::page_round_trips_via_from_bytes"
    ));
    assert!(docs.contains("PR gate"));
    assert!(docs.contains("Nightly/manual gate"));
}

#[test]
fn release_checklist_maps_69_to_74_to_artifacts() {
    let docs = repo_file("docs/release-checklist.md");

    for needle in [
        "code",
        "test",
        "benchmark or reason",
        "docs",
        "artifact",
        "69 Catalog upgrade story",
        "70 Backup/restore smoke",
        "71 Config docs",
        "72 Security/ethics audit",
        "73 CI split",
        "74 Release checklist",
    ] {
        assert!(docs.contains(needle), "release checklist missing {needle}");
    }
}

#[test]
fn release_user_docs_exist_and_state_current_limits() {
    let changelog = repo_file("CHANGELOG.md");
    let getting_started = repo_file("docs/getting-started.md");
    let migration = repo_file("docs/migration-from-postgresql.md");
    let incompat = repo_file("docs/known-incompatibilities.md");

    for needle in ["Unreleased", "Known gaps", "pre-1.0 releases"] {
        assert!(changelog.contains(needle), "CHANGELOG missing {needle}");
    }
    for needle in [
        "pre-alpha",
        "Build from source",
        "Run tests",
        "SQLLogicTest",
    ] {
        assert!(
            getting_started.contains(needle),
            "getting started missing {needle}"
        );
    }
    for needle in [
        "not a drop-in production replacement yet",
        "COPY",
        "Validation rule",
    ] {
        assert!(
            migration.contains(needle),
            "migration guide missing {needle}"
        );
    }
    for needle in [
        "v1.0 is not released",
        "Ordered-set aggregates",
        "Firebolt comparisons use local Firebolt Core only",
    ] {
        assert!(
            incompat.contains(needle),
            "known incompatibilities missing {needle}"
        );
    }
}
