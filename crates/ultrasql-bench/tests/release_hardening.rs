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
    let manifest = repo_file("benchmarks/results/latest/backup_restore_smoke_manifest.json");

    assert!(script.contains("backup_restore_smoke_manifest.json"));
    assert!(script.contains("ultrasql --basebackup"));
    assert!(script.contains("ultrasql --pg-dump"));
    assert!(script.contains("ultrasql --pg-restore"));
    assert!(script.contains("DUMP_FORMATS"));
    assert!(script.contains("custom directory tar"));
    assert!(script.contains("for format in"));
    assert!(script.contains("--dump-format \"$format\""));
    assert!(script.contains("verify_restored_dump"));
    assert!(script.contains("SELECT COUNT(*) FROM backup_restore_smoke"));
    assert!(script.contains("SELECT payload FROM backup_restore_smoke WHERE id = 2"));
    assert!(script.contains("\"row_count_verified\""));
    assert!(script.contains("\"index_query_verified\""));
    assert!(script.contains("\"dump_format_results\""));
    assert!(script.contains("\"dump_formats_verified\""));
    assert!(script.contains("\"status\": \"not_available\""));
    assert!(docs.contains("row counts"));
    assert!(docs.contains("index query"));
    assert!(docs.contains("custom, directory, and tar"));
    assert!(docs.contains("benchmarks/backup_restore_smoke.sh"));
    assert!(manifest.contains("\"status\": \"measured\""));
    assert!(manifest.contains("\"dump_formats_verified\""));
    for format in ["custom", "directory", "tar"] {
        assert!(
            manifest.contains(&format!("\"format\": \"{format}\"")),
            "backup/restore manifest missing {format} format evidence"
        );
    }
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
fn chaos_recovery_runner_documents_fault_coverage() {
    let script = repo_file("benchmarks/chaos_recovery.sh");
    let docs = repo_file("docs/chaos-recovery.md");
    let release = repo_file("docs/release-checklist.md");
    let done = repo_file("DONE.md");

    for needle in [
        "chaos_recovery_manifest.json",
        "random_kill",
        "wal_truncation",
        "disk_full",
        "CHAOS_SEED",
        "kill -9",
        "truncate_last_wal_segment",
        "ulimit -f",
        "\"restarted_after_kill\"",
        "\"truncated_wal_recovered\"",
        "\"disk_full_recovered_without_corruption\"",
        "\"row_count_verified\"",
        "\"status\": \"not_available\"",
    ] {
        assert!(script.contains(needle), "chaos runner missing {needle}");
    }

    for needle in [
        "random kill",
        "WAL truncation",
        "disk full",
        "benchmarks/chaos_recovery.sh",
        "safe disk-full simulation",
        "chaos_recovery_manifest.json",
    ] {
        assert!(docs.contains(needle), "chaos docs missing {needle}");
    }

    assert!(release.contains("Chaos recovery"));
    assert!(release.contains("benchmarks/chaos_recovery.sh"));
    assert!(done.contains("Chaos testing: random kill, WAL truncation, disk full"));
}

#[test]
fn chaos_recovery_script_is_valid_bash() {
    let Some(mut bash) = bash_command() else {
        eprintln!("skipping chaos bash syntax check: Git Bash not found");
        return;
    };
    let status = bash
        .arg("-n")
        .arg(repo_path("benchmarks/chaos_recovery.sh"))
        .status()
        .expect("run bash -n chaos_recovery.sh");
    assert!(status.success(), "chaos_recovery.sh must parse");
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
        "No proprietary tests",
        "No closed-source code",
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
    assert!(ci.contains("python3 -m unittest discover -s tests/scripts -p 'test_*.py'"));
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
fn driver_certification_matrix_covers_core_ecosystem() {
    let ci = repo_file(".github/workflows/ci.yml");
    let docs = repo_file("docs/driver-certification.md");
    let release = repo_file("docs/release-checklist.md");
    let roadmap = repo_file("ROADMAP.md");
    let harness = repo_file("tests/driver_certification/driver_certification.py");

    for needle in [
        "libpq",
        "psql meta-commands",
        "psycopg2",
        "psycopg3",
        "SQLAlchemy",
        "Django ORM",
        "Rails ActiveRecord",
        "Hibernate ORM",
        "GORM",
        "Prisma",
        "Diesel",
        "node-postgres",
        "pgx",
        "lib/pq",
        "JDBC",
        "Npgsql",
        "GUI introspection probes",
        "pgAdmin",
        "DBeaver",
        "DataGrip",
        "Flyway",
        "Liquibase",
        "Alembic",
    ] {
        assert!(docs.contains(needle), "driver docs missing {needle}");
        assert!(roadmap.contains(needle), "ROADMAP missing {needle}");
        assert!(harness.contains(needle), "cert harness missing {needle}");
    }

    for needle in [r"\d", r"\dt", r"\di", r"\df", r"\dv", r"\du", r"\l", r"\dn"] {
        assert!(docs.contains(needle), "driver docs missing {needle}");
        assert!(
            release.contains(needle),
            "release checklist missing {needle}"
        );
        assert!(roadmap.contains(needle), "ROADMAP missing {needle}");
        assert!(harness.contains(needle), "cert harness missing {needle}");
    }
    assert!(harness.contains("certify_psql_meta_commands"));
    assert!(harness.contains("psql meta-command certification failed"));

    for needle in [
        "actions/setup-node",
        "actions/setup-go",
        "actions/setup-java",
        "actions/setup-dotnet",
        "ruby/setup-ruby",
        "bundle install --gemfile tests/driver_certification/rails/Gemfile",
        "pnpm --dir tests/driver_certification/node install --frozen-lockfile",
        "go mod download",
        "dotnet restore",
        "postgresql-client",
        "cargo fetch --manifest-path tests/driver_certification/diesel/Cargo.toml",
        "Alembic==",
    ] {
        assert!(ci.contains(needle), "CI driver gate missing {needle}");
    }

    for needle in [
        "SQLAlchemy==",
        "Django==",
        "Stock psql meta-command coverage",
        "activerecord",
        "Hibernate ORM",
        "GORM",
        "Prisma",
        "Diesel",
        "tests/driver_certification/rails",
        "tests/driver_certification/node",
        "tests/driver_certification/go",
        "tests/driver_certification/hibernate",
        "tests/driver_certification/diesel",
        "tests/driver_certification/java",
        "tests/driver_certification/dotnet",
        "driver-certification.json",
        "GUI introspection probes",
        "pgAdmin",
        "DBeaver",
        "DataGrip",
        "Flyway",
        "Liquibase",
        "Alembic",
    ] {
        assert!(
            release.contains(needle),
            "release checklist missing {needle}"
        );
    }
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
fn packaging_and_docs_site_surface_is_release_ready() {
    let mkdocs = repo_file("mkdocs.yml");
    let docs_workflow = repo_file(".github/workflows/docs.yml");
    let dockerfile = repo_file("Dockerfile");
    let dockerignore = repo_file(".dockerignore");
    let release = repo_file(".github/workflows/release.yml");
    let nfpm = repo_file("packaging/nfpm.yaml.in");
    let systemd = repo_file("packaging/linux/ultrasqld.service");
    let homebrew = repo_file("packaging/homebrew/ultrasql.rb.in");
    let homebrew_render = repo_file("scripts/render-homebrew-formula.sh");
    let aur = repo_file("packaging/aur/PKGBUILD.in");
    let aur_srcinfo = repo_file("packaging/aur/.SRCINFO.in");
    let aur_render = repo_file("scripts/render-aur-package.sh");
    let chocolatey = repo_file("packaging/chocolatey/ultrasql.nuspec.in");
    let chocolatey_install = repo_file("packaging/chocolatey/tools/chocolateyInstall.ps1.in");
    let chocolatey_uninstall = repo_file("packaging/chocolatey/tools/chocolateyUninstall.ps1.in");
    let windows_installer = repo_file("packaging/windows/ultrasql.nsi.in");
    let nfpm_render = repo_file("scripts/render-nfpm-config.sh");
    let docs = repo_file("docs/packaging.md");
    let install = repo_file("docs/install.md");
    let roadmap = repo_file("ROADMAP.md");

    for needle in [
        "site_url: https://docs.ultrasql.org/",
        "theme:",
        "name: material",
        "Getting Started",
        "Packaging",
    ] {
        assert!(mkdocs.contains(needle), "mkdocs.yml missing {needle}");
    }
    for needle in [
        "actions/configure-pages",
        "mkdocs build --strict",
        "actions/upload-pages-artifact",
        "actions/deploy-pages",
    ] {
        assert!(
            docs_workflow.contains(needle),
            "docs workflow missing {needle}"
        );
    }

    for needle in [
        "FROM rust:",
        "cargo build --locked --profile release-ship",
        "USER 10001:10001",
        "ENTRYPOINT",
        "--listen",
        "0.0.0.0:5432",
        "--data-dir",
        "/var/lib/ultrasql",
    ] {
        assert!(dockerfile.contains(needle), "Dockerfile missing {needle}");
    }
    assert!(dockerignore.contains("target/"));
    assert!(dockerignore.contains("benchmarks/results/"));

    for needle in [
        "docker/build-push-action",
        "ghcr.io/${{ github.repository_owner }}/ultrasql",
        "platforms: linux/amd64",
        "provenance: false",
        "sbom: false",
        "makensis",
        "setup.exe",
        "*.exe",
        "render-nfpm-config.sh",
        "actions/setup-go@v6",
        "go install github.com/goreleaser/nfpm/v2/cmd/nfpm@v2.43.1",
        "$(go env GOPATH)/bin/nfpm",
        "--packager deb",
        "--packager rpm",
        "git archive --format=tar.gz",
        "-source.tar.gz",
        "render-homebrew-formula.sh",
        "ultrasql.rb",
        "actions/setup-node@v5",
        "actions/setup-node@v6",
        "node-version: \"24\"",
        "package-manager-cache: false",
        "environment: main",
        "id-token: write",
        "packages/npm",
        "ultrasql-node",
        "ultrasql.node",
        "npm pack ./packages/npm",
        "*.tgz",
        "npm publish --access public",
        "show npm trusted publishing toolchain",
        "python3 -m unittest discover -s tests/scripts -p 'test_*.py'",
        "render-aur-package.sh",
        "ultrasql-aur-${RELEASE_TAG}.tar.gz",
        "AUR_SSH_PRIVATE_KEY",
        "aur@aur.archlinux.org:ultrasql-bin.git",
        "choco pack",
        "*.nupkg",
        "CHOCOLATEY_API_KEY",
        "choco push",
        "HOMEBREW_TAP_TOKEN",
        "pattern: ultrasql-${{ env.RELEASE_TAG }}-*",
        "*.deb",
        "*.rpm",
    ] {
        assert!(
            release.contains(needle),
            "release workflow missing {needle}"
        );
    }
    assert!(
        !release.contains("goreleaser/nfpm-action"),
        "release workflow must install nfpm from the Go module, not a missing action"
    );
    assert!(
        !release.contains("NODE_AUTH_TOKEN"),
        "npm trusted publishing must not use a long-lived npm write token"
    );
    assert!(
        !release.contains("NPM_TOKEN"),
        "npm trusted publishing must not depend on NPM_TOKEN"
    );

    for needle in [
        "name: ultrasql",
        "arch: \"@ARCH@\"",
        "version: \"@VERSION@\"",
        "/usr/bin/ultrasqld",
        "/lib/systemd/system/ultrasqld.service",
        "packaging/linux/preinstall.sh",
        "packaging/linux/postinstall.sh",
    ] {
        assert!(nfpm.contains(needle), "nfpm template missing {needle}");
    }
    for needle in [
        "User=ultrasql",
        "Group=ultrasql",
        "NoNewPrivileges=true",
        "ReadWritePaths=/var/lib/ultrasql",
    ] {
        assert!(systemd.contains(needle), "systemd unit missing {needle}");
    }

    for needle in [
        "class Ultrasql < Formula",
        "ultrasql-v@VERSION@-source.tar.gz",
        "sha256 \"@SHA256_SOURCE@\"",
        "depends_on \"rust\" => :build",
        "cargo\", \"install\"",
        "crates/ultrasql-server",
        "crates/ultrasql-cli",
        "--profile\", \"release-ship\"",
        "rm_f prefix/\".crates.toml\"",
    ] {
        assert!(
            homebrew.contains(needle),
            "homebrew template missing {needle}"
        );
    }
    for needle in [
        "SHA256_SOURCE",
        "ultrasql-v${version}-source.tar.gz",
        "source tarball checksum missing",
    ] {
        assert!(
            homebrew_render.contains(needle),
            "homebrew renderer missing {needle}"
        );
    }
    for needle in [
        "pkgname=ultrasql-bin",
        "pkgver=@VERSION@",
        "x86_64",
        "aarch64",
        "@SHA256_LINUX_AMD64@",
        "@SHA256_LINUX_ARM64@",
        "install -Dm755",
    ] {
        assert!(aur.contains(needle), "AUR PKGBUILD missing {needle}");
    }
    for needle in [
        "pkgbase = ultrasql-bin",
        "pkgver = @VERSION@",
        "arch = x86_64",
        "arch = aarch64",
        "@SHA256_LINUX_AMD64@",
        "@SHA256_LINUX_ARM64@",
    ] {
        assert!(aur_srcinfo.contains(needle), "AUR SRCINFO missing {needle}");
    }
    for needle in [
        "SHA256_LINUX_AMD64",
        "SHA256_LINUX_ARM64",
        "checksum missing",
        "ultrasql-aur-${tag}.tar.gz",
        "PKGBUILD",
        ".SRCINFO",
    ] {
        assert!(aur_render.contains(needle), "AUR renderer missing {needle}");
    }
    for needle in [
        "<id>ultrasql</id>",
        "<version>@VERSION@</version>",
        "<licenseUrl>",
        "<requireLicenseAcceptance>false</requireLicenseAcceptance>",
        "chocolateyInstall.ps1",
        "chocolateyUninstall.ps1",
    ] {
        assert!(
            chocolatey.contains(needle),
            "Chocolatey nuspec missing {needle}"
        );
    }
    for needle in [
        "Install-ChocolateyPackage",
        "url64bit",
        "@SETUP_SHA256@",
        "ultrasql-@TAG@-x86_64-pc-windows-msvc-setup.exe",
    ] {
        assert!(
            chocolatey_install.contains(needle),
            "Chocolatey install script missing {needle}"
        );
    }
    for needle in ["Uninstall-ChocolateyPackage", "UltraSQL", "Uninstall.exe"] {
        assert!(
            chocolatey_uninstall.contains(needle),
            "Chocolatey uninstall script missing {needle}"
        );
    }
    for needle in [
        "Name \"UltraSQL\"",
        "OutFile \"@OUT_FILE@\"",
        "RequestExecutionLevel admin",
        "ultrasqld.exe",
        "ultrasql.exe",
        "ultrasql-local.exe",
        "WriteUninstaller",
        "AddToPath",
        "RemoveFromPath",
    ] {
        assert!(
            windows_installer.contains(needle),
            "Windows installer template missing {needle}"
        );
    }
    for needle in ["@VERSION@", "@ARCH@", "@ROOT@", "sed"] {
        assert!(
            nfpm_render.contains(needle),
            "nfpm renderer missing {needle}"
        );
    }

    for needle in [
        "docs.ultrasql.org",
        "ghcr.io/mauneven/ultrasql",
        "packages/npm",
        "npm publish",
        "Windows setup EXE",
        "Chocolatey",
        "AUR",
        "yay -S ultrasql-bin",
        "Homebrew tap",
        "clean GHCR platform list",
        "Homebrew",
        "Debian",
        "RPM",
        "release workflow",
    ] {
        assert!(docs.contains(needle), "packaging docs missing {needle}");
        assert!(install.contains(needle), "install docs missing {needle}");
        assert!(roadmap.contains(needle), "ROADMAP missing {needle}");
    }
    for needle in [
        "Workflow filename: release.yml",
        "Environment name: main",
        "Allowed actions: npm publish",
        "GitHub OIDC",
    ] {
        assert!(docs.contains(needle), "packaging docs missing {needle}");
        assert!(install.contains(needle), "install docs missing {needle}");
    }
}

#[test]
fn packaging_scripts_have_valid_bash_syntax() {
    if bash_command().is_none() {
        eprintln!("skipping packaging bash syntax check: Git Bash not found");
        return;
    }

    for script in [
        "scripts/render-homebrew-formula.sh",
        "scripts/render-aur-package.sh",
        "scripts/render-nfpm-config.sh",
        "packaging/linux/preinstall.sh",
        "packaging/linux/postinstall.sh",
    ] {
        let status = bash_command()
            .expect("bash available")
            .arg("-n")
            .arg(repo_path(script))
            .status()
            .unwrap_or_else(|err| panic!("run bash -n {script}: {err}"));
        assert!(status.success(), "{script} must parse");
    }
}

#[test]
fn principal_files_keep_hygiene_guards() {
    for path in [
        "crates/ultrasql-server/src/session/execute.rs",
        "crates/ultrasql-storage/src/heap/delete.rs",
        "crates/ultrasql-storage/src/heap/update_inplace.rs",
        "crates/ultrasql-storage/src/heap/wal_emit.rs",
        "crates/ultrasql-storage/src/heap/scan.rs",
        "crates/ultrasql-storage/src/heap/insert.rs",
        "crates/ultrasql-storage/src/heap/walker.rs",
        "crates/ultrasql-storage/src/heap/helpers.rs",
        "crates/ultrasql-storage/src/heap/update.rs",
    ] {
        let source = repo_file(path);
        assert!(
            !source.contains("#![allow(unused_imports)]"),
            "{path} must prune stale imports instead of suppressing them"
        );
    }

    for path in [
        "crates/ultrasql-storage/src/heap/delete.rs",
        "crates/ultrasql-storage/src/heap/update_inplace.rs",
    ] {
        let source = repo_file(path);
        assert!(
            !source.contains("clippy::cast_possible") && !source.contains("clippy::cast_lossless"),
            "{path} must use checked/widening conversions instead of suppressing cast lints"
        );
        for needle in [" as usize", " as u32", " as u64"] {
            assert!(
                !source.contains(needle),
                "{path} hot heap code must avoid integer casts via `{needle}`"
            );
        }
    }

    let fused_delete = repo_file("crates/ultrasql-executor/src/fused_delete.rs");
    assert!(
        !fused_delete.contains("affected-count schema is well-formed"),
        "fused delete must build static schemas without panic-style expect"
    );

    for path in [
        "crates/ultrasql-executor/src/materialize.rs",
        "crates/ultrasql-executor/src/unique.rs",
        "crates/ultrasql-executor/src/merge_join.rs",
        "crates/ultrasql-executor/src/sort_aggregate.rs",
        "crates/ultrasql-executor/src/set_op.rs",
        "crates/ultrasql-executor/src/nested_loop_join.rs",
        "crates/ultrasql-executor/src/hash_aggregate.rs",
    ] {
        let source = repo_file(path);
        assert!(
            !source.contains("expect(\"just-set\")"),
            "{path} must return ExecError::Internal instead of panicking on state-machine invariants"
        );
    }

    let heap_delete = repo_file("crates/ultrasql-storage/src/heap/delete.rs");
    for needle in [
        "expect(\"8B\")",
        "expect(\"2B\")",
        "expect(\"4-byte id\")",
        "expect(\"4-byte val\")",
    ] {
        assert!(
            !heap_delete.contains(needle),
            "heap delete hot-path decode must return HeapError instead of {needle}"
        );
    }
}

#[test]
fn final_release_requires_operator_reports_green_workflows_and_notes() {
    let operator_docs = repo_file("docs/OPERATOR_SOAK.md");
    let operator_report_docs = repo_file("docs/operator-reports.md");
    let operator_workflow = repo_file(".github/workflows/operator-soak.yml");
    let validator = repo_file("scripts/validate-operator-soak.py");
    let release = repo_file(".github/workflows/release.yml");
    let notes_template = repo_file("docs/release-notes-template.md");
    let notes_renderer = repo_file("scripts/render-release-notes.sh");
    let release_config = repo_file(".github/release.yml");
    let release_checklist = repo_file("docs/release-checklist.md");
    let roadmap = repo_file("ROADMAP.md");

    for needle in [
        "30 continuous days",
        "Three independent operators",
        "operator-reports/*.json",
        "benchmarks/results/latest/operator_soak_status.json",
        "critical_issue_count",
        "high_issue_count",
    ] {
        assert!(operator_docs.contains(needle), "soak docs missing {needle}");
        assert!(
            operator_report_docs.contains(needle),
            "operator report docs missing {needle}"
        );
    }

    for needle in [
        "schedule:",
        "validate operator soak reports",
        "scripts/validate-operator-soak.py",
        "--min-reports 3",
        "--min-days 30",
        "operator-soak-status",
    ] {
        assert!(
            operator_workflow.contains(needle),
            "operator soak workflow missing {needle}"
        );
    }

    for needle in [
        "min_reports",
        "min_days",
        "operator_id",
        "start_time_utc",
        "end_time_utc",
        "critical_issue_count",
        "high_issue_count",
        "status",
        "ready",
        "not_ready",
    ] {
        assert!(
            validator.contains(needle),
            "operator validator missing {needle}"
        );
    }

    for needle in [
        "scripts/validate-operator-soak.py",
        "--strict",
        "operator_soak_status.json",
        "scripts/render-release-notes.sh",
        "RELEASE_NOTES.md",
        "body_path: release/RELEASE_NOTES.md",
    ] {
        assert!(
            release.contains(needle),
            "release workflow missing {needle}"
        );
    }

    for needle in [
        "@RELEASE_TAG@",
        "@RELEASE_RUN_URL@",
        "@OPERATOR_SOAK_STATUS@",
        "Green workflow evidence",
        "GitHub release notes",
        "30-day operator reports",
    ] {
        assert!(
            notes_template.contains(needle),
            "release notes template missing {needle}"
        );
    }
    for needle in ["RELEASE_TAG", "OPERATOR_SOAK_STATUS", "sed"] {
        assert!(
            notes_renderer.contains(needle),
            "release notes renderer missing {needle}"
        );
    }
    for needle in ["changelog:", "exclude:", "categories:"] {
        assert!(
            release_config.contains(needle),
            "github release config missing {needle}"
        );
    }

    for needle in [
        "operator soak reports",
        "latest green CI workflow run id",
        "release workflow run id",
        "GitHub release notes",
        "operator_soak_status.json",
    ] {
        assert!(
            release_checklist.contains(needle),
            "release checklist missing {needle}"
        );
        assert!(roadmap.contains(needle), "ROADMAP missing {needle}");
    }
}

#[test]
fn release_user_docs_exist_and_state_current_limits() {
    let changelog = repo_file("CHANGELOG.md");
    let getting_started = repo_file("docs/getting-started.md");
    let migration = repo_file("docs/migration-guide.md");
    let limitations = repo_file("docs/known-limitations.md");

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
        "not a v1.0 production database yet",
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
        "Broader aggregate coverage",
        "Firebolt comparisons use local Firebolt Core only",
    ] {
        assert!(
            limitations.contains(needle),
            "known limitations missing {needle}"
        );
    }
}
