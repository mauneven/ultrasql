# Release Checklist

Every v1.0 roadmap box maps to code, test, benchmark or reason, docs, and
artifact. Missing evidence keeps the box open.

## PR gate

Run on every PR:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo doc --workspace --all-features --no-deps
benchmarks/certify.sh smoke
```

The GitHub `ci` workflow runs format, clippy, tests, docs, and cargo-deny.
The `bench` workflow runs the PR-safe smoke certification profile for benchmark
touches.

## Nightly/manual gate

Run on schedule or by `workflow_dispatch`:

```bash
benchmarks/certify.sh full
cargo bench --workspace
cargo +nightly fuzz run parser_fuzz -- -max_total_time=900 -rss_limit_mb=4096 -max_len=1024
cargo +nightly miri test -p ultrasql-storage
```

The `bench`, `fuzz`, and `sanitizers` workflows own these slower gates. TPC-H,
ClickBench, Firebolt Core, AI gauntlet, fuzz, and Miri evidence belongs here,
not in the PR-critical path.

## 69-74 Evidence Map

| item | code | test | benchmark or reason | docs | artifact |
| --- | --- | --- | --- | --- | --- |
| 69 Catalog upgrade story | `crates/ultrasql-server/src/catalog_version.rs` | `crates/ultrasql-server/tests/catalog_version_round_trip.rs` | no benchmark; startup safety guard | `docs/catalog-upgrades.md` | `catalog.version` in data dir |
| 70 Backup/restore smoke | `ultrasql --basebackup`, `--pg-dump`, `--pg-restore`; `benchmarks/backup_restore_smoke.sh` | `crates/ultrasql-bench/tests/release_hardening.rs` | `benchmarks/backup_restore_smoke.sh` verifies row counts and index query | `docs/backup-restore.md` | `benchmarks/results/latest/backup_restore_smoke_manifest.json` |
| 71 Config docs | server/CLI flags and benchmark envs | `crates/ultrasql-bench/tests/release_hardening.rs` | no benchmark; documentation surface | `docs/configuration.md` | release checklist entry |
| 72 Security/ethics audit | repository rules and benchmark runners | `crates/ultrasql-bench/tests/release_hardening.rs` | no benchmark; provenance gate | `docs/security-ethics-audit.md` | audit command output in release notes |
| 73 CI split | `.github/workflows/ci.yml`, `bench.yml`, `fuzz.yml`, `sanitizers.yml` | `crates/ultrasql-bench/tests/release_hardening.rs` | PR smoke vs nightly/manual full gates | this file | GitHub Actions run ids |
| 74 Release checklist | this file | `crates/ultrasql-bench/tests/release_hardening.rs` | no benchmark; release evidence index | `docs/release-checklist.md` | completed release issue or tag notes |

## Sign-off rule

Before tagging v1.0, attach:

- latest `ci-passed` run id,
- latest full benchmark certification manifest,
- TPC-H SF10 and ClickBench artifacts or explicit setup-missing reasons,
- Firebolt Core local artifacts or explicit Docker/setup-missing reasons,
- backup/restore smoke manifest,
- security/ethics audit notes,
- config docs hash or commit id.
