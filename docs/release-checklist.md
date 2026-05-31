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
cargo build -p ultrasql-server --bin ultrasqld
python3 -m venv /tmp/ultrasql-driver-cert
/tmp/ultrasql-driver-cert/bin/python -m pip install -r tests/driver_certification/requirements.txt
# Python harness pins psycopg2, psycopg3, SQLAlchemy==2.0.50, Django==6.0.5,
# and Alembic==1.18.4.
# Stock psql meta-command coverage requires the PostgreSQL client package.
bundle install --gemfile tests/driver_certification/rails/Gemfile
# Rails ActiveRecord harness pins activerecord 8.1.3 and pg 1.6.3.
pnpm --dir tests/driver_certification/node install --frozen-lockfile
# Node harness pins node-postgres, Prisma, @prisma/client, and @prisma/adapter-pg.
go -C tests/driver_certification/go mod download
# Go harness pins lib/pq, pgx, and GORM.
cargo fetch --manifest-path tests/driver_certification/diesel/Cargo.toml
# Diesel harness pins Diesel 2.3.9.
dotnet restore --locked-mode tests/driver_certification/dotnet/Ultrasql.DriverCertification.csproj
# The harness compiles tests/driver_certification/java/JdbcCert.java itself.
# Hibernate ORM dependencies resolve from tests/driver_certification/hibernate.
# Flyway and Liquibase dependencies resolve from tests/driver_certification/flyway
# and tests/driver_certification/liquibase.
/tmp/ultrasql-driver-cert/bin/python tests/driver_certification/driver_certification.py \
  --ultrasqld target/debug/ultrasqld
benchmarks/certify.sh smoke
```

The GitHub `ci` workflow runs format, clippy, tests, driver certification,
docs, cargo-deny, and cargo-audit. The `bench` workflow runs the PR-safe smoke
certification profile for benchmark touches.
Driver certification evidence includes stock psql meta-commands `\d`, `\dt`,
`\di`, `\df`, `\dv`, `\du`, `\l`, and `\dn`; GUI introspection probes for
pgAdmin, DBeaver, and DataGrip schema-browser catalog query families; Flyway,
Liquibase, and Alembic migration version-table runs in nontransactional DDL
mode; it is stored as `target/driver-certification.json` and uploaded by CI.
Before final release sign-off, record the latest green CI workflow run id.
RLS tenant certification is part of `benchmarks/certify.sh smoke` and writes
`benchmarks/results/latest/rls_tenant_certification.json`.

## Release artifact gate

Pushing a `vX.Y.Z` tag runs `.github/workflows/release.yml`. The workflow:

- checks that the tag version matches every local workspace crate version,
- runs format, clippy, and full workspace tests before packaging,
- builds `ultrasqld`, `ultrasql`, `ultrasql-local`, and the Node-API
  `ultrasql.node` addon with
  `--profile release-ship --locked`,
- runs binary smoke checks (`--version` / `--help`) before packaging,
- publishes Linux x86_64, Linux ARM64, macOS Intel, macOS Apple Silicon, and
  Windows x86_64 archives,
- publishes `ghcr.io/mauneven/ultrasql:<tag>` from the Dockerfile,
- attaches `ultrasql-<version>.tgz` from `packages/npm` and publishes the
  `ultrasql` npm registry package through npm Trusted Publishing with GitHub
  OIDC,
- builds Debian/Ubuntu `.deb` packages and `.rpm` packages with nFPM,
- renders a Homebrew formula from the macOS archive checksums,
- uploads per-asset `.sha256` files plus `SHASUMS256.txt`,
- validates `operator-reports/*.json`; `v1+` releases require
  `scripts/validate-operator-soak.py --strict`, while `v0.*` prereleases record
  the current soak status without blocking publication,
- renders `RELEASE_NOTES.md` from `docs/release-notes-template.md`,
- marks `v0.*` releases as pre-releases.

Install instructions live in `docs/install.md`. The release workflow provides
distribution plumbing only; it does not override any open correctness,
benchmark, security, or operator-soak gate in this checklist.
Before final release sign-off, record the release workflow run id and the
published GitHub release notes URL.

## Nightly/manual gate

Run on schedule or by `workflow_dispatch`:

```bash
benchmarks/certify.sh full
benchmarks/chaos_recovery.sh full
cargo bench --workspace
for target in parser_fuzz planner_fuzz protocol_fuzz wal_record_fuzz; do
  cargo +nightly fuzz run "$target" -- -max_total_time=900 -rss_limit_mb=4096 -max_len=1024
done
# Mirrors .github/workflows/sanitizers.yml.
cargo +nightly miri setup
cargo +nightly miri test -p ultrasql-core endian::tests::u64_round_trip
cargo +nightly miri test -p ultrasql-core value::tests::display_round_trip_for_simple_values
cargo +nightly miri test -p ultrasql-storage page::tests::page_round_trips_via_from_bytes
cargo +nightly miri test -p ultrasql-storage page::tests::insert_and_read_round_trip
cargo +nightly miri test -p ultrasql-storage buffer_pool::tests::read_after_write_sees_update
cargo +nightly miri test -p ultrasql-parser parser::tests::deeply_nested_parens_rejected_without_overflow
```

The `bench`, `fuzz`, and `sanitizers` workflows own these slower gates. The Miri
section is the same run miri smoke on memory-safety-sensitive crates block used
by CI. TPC-H, ClickBench, Firebolt Core, AI gauntlet, fuzz, and Miri evidence
belongs here, not in the PR-critical path.

## 69-83 Evidence Map

| item | code | test | benchmark or reason | docs | artifact |
| --- | --- | --- | --- | --- | --- |
| 69 Catalog upgrade story | `crates/ultrasql-server/src/catalog_version.rs` | `crates/ultrasql-server/tests/catalog_version_round_trip.rs` | no benchmark; startup safety guard | `docs/catalog-upgrades.md` | `catalog.version` in data dir |
| 70 Backup/restore smoke | `ultrasql --basebackup`, `--pg-dump`, `--pg-restore`; `benchmarks/backup_restore_smoke.sh` | `crates/ultrasql-bench/tests/release_hardening.rs` | `benchmarks/backup_restore_smoke.sh` verifies row counts and index query for custom, directory, and tar dump formats | `docs/backup-restore.md` | `benchmarks/results/latest/backup_restore_smoke_manifest.json` |
| 71 Config docs | server/CLI flags and benchmark envs | `crates/ultrasql-bench/tests/release_hardening.rs` | no benchmark; documentation surface | `docs/configuration.md` | release checklist entry |
| 72 Security/ethics audit | repository rules and benchmark runners | `crates/ultrasql-bench/tests/release_hardening.rs` | no benchmark; provenance gate | `docs/security-ethics-audit.md` | audit command output in release notes |
| 73 CI split | `.github/workflows/ci.yml`, `bench.yml`, `fuzz.yml`, `sanitizers.yml` | `crates/ultrasql-bench/tests/release_hardening.rs` | PR smoke vs nightly/manual full gates | this file | GitHub Actions run ids |
| 74 Release checklist | this file | `crates/ultrasql-bench/tests/release_hardening.rs` | no benchmark; release evidence index | `docs/release-checklist.md` | completed release issue or tag notes |
| 75 Binary installers | `.github/workflows/release.yml`, `scripts/install.sh`, `scripts/install.ps1`, `packaging/windows/ultrasql.nsi.in` | release workflow binary smoke checks | no benchmark; distribution integrity gate | `docs/install.md` | release archives, Windows setup EXE, `.sha256` files |
| 76 Chaos recovery | `benchmarks/chaos_recovery.sh` | `crates/ultrasql-bench/tests/release_hardening.rs` | random kill, WAL truncation, and safe disk-full simulation all recover | `docs/chaos-recovery.md` | `benchmarks/results/latest/chaos_recovery_manifest.json` |
| 77 Docs site | `mkdocs.yml`, `.github/workflows/docs.yml` | `crates/ultrasql-bench/tests/release_hardening.rs` | no benchmark; documentation publication gate | `docs/packaging.md` | GitHub Pages deployment for `docs.ultrasql.org` |
| 78 Docker image | `Dockerfile`, `.dockerignore`, `.github/workflows/release.yml` | `crates/ultrasql-bench/tests/release_hardening.rs` | no benchmark; release packaging gate | `docs/packaging.md` | GHCR image digest for `ghcr.io/mauneven/ultrasql:<tag>` and clean GHCR platform list |
| 78.1 npm / pnpm / Bun package | `packages/npm`, `crates/ultrasql-node`, `.github/workflows/release.yml` | `node --test packages/npm/test/*.test.js`, `cargo test -p ultrasql-node`, `crates/ultrasql-bench/tests/release_hardening.rs` | no benchmark; release packaging gate | `docs/install.md`, `docs/packaging.md` | `ultrasql-<version>.tgz` release asset, `ultrasql.node` in platform archives, npm Trusted Publisher configuration, and npm publish output for `ultrasql` |
| 79 Homebrew formula | `packaging/homebrew/ultrasql.rb.in`, `scripts/render-homebrew-formula.sh`, `.github/workflows/release.yml` | `crates/ultrasql-bench/tests/release_hardening.rs` | no benchmark; release packaging gate | `docs/install.md`, `docs/packaging.md` | rendered `ultrasql.rb` release asset and Homebrew tap publish output |
| 80 Deb/RPM packages | `packaging/nfpm.yaml.in`, `packaging/linux/*`, `.github/workflows/release.yml` | `crates/ultrasql-bench/tests/release_hardening.rs` | no benchmark; release packaging gate | `docs/install.md` | `.deb` and `.rpm` release assets |
| 80.1 AUR package | `packaging/aur/PKGBUILD.in`, `packaging/aur/.SRCINFO.in`, `scripts/render-aur-package.sh`, `.github/workflows/release.yml` | `crates/ultrasql-bench/tests/release_hardening.rs`, `bash -n scripts/render-aur-package.sh` | no benchmark; release packaging gate | `docs/install.md`, `docs/packaging.md` | `ultrasql-aur-<tag>.tar.gz` and AUR publish output for `yay -S ultrasql-bin` |
| 80.2 Chocolatey package | `packaging/windows/ultrasql.nsi.in`, `packaging/chocolatey/*`, `.github/workflows/release.yml` | release workflow `makensis`, `choco pack`, `crates/ultrasql-bench/tests/release_hardening.rs` | no benchmark; release packaging gate | `docs/install.md`, `docs/packaging.md` | Windows setup EXE, `.nupkg`, and `choco push` output |
| 81 Operator soak reports | `scripts/validate-operator-soak.py`, `.github/workflows/operator-soak.yml` | `crates/ultrasql-bench/tests/release_hardening.rs` | no benchmark; 30-day external operation gate | `docs/OPERATOR_SOAK.md`, `docs/operator-reports.md` | `benchmarks/results/latest/operator_soak_status.json` |
| 82 Green CI/release workflows | `.github/workflows/ci.yml`, `.github/workflows/release.yml` | `crates/ultrasql-bench/tests/release_hardening.rs`, actionlint | no benchmark; release governance gate | this file | latest green CI workflow run id and release workflow run id |
| 83 GitHub release notes | `docs/release-notes-template.md`, `scripts/render-release-notes.sh`, `.github/release.yml` | `crates/ultrasql-bench/tests/release_hardening.rs` | no benchmark; release communication gate | `CHANGELOG.md`, this file | rendered `RELEASE_NOTES.md` and GitHub release notes URL |

## Sign-off rule

Before tagging v1.0, attach:

- latest `ci-passed` run id,
- latest green CI workflow run id,
- release workflow run id,
- latest full benchmark certification manifest,
- RLS tenant certification artifact,
- TPC-H SF10 and ClickBench artifacts or explicit setup-missing reasons,
- Firebolt Core local artifacts or explicit Docker/setup-missing reasons,
- AI/vector same-host exact-vector artifacts for UltraSQL,
  PostgreSQL+pgvector, DuckDB, and ClickHouse,
- backup/restore smoke manifest with custom, directory, and tar dump formats,
- chaos recovery manifest,
- docs site deployment URL or failed-run reason,
- Docker image digest,
- npm package tarball asset and publish output,
- Homebrew formula asset,
- Debian and RPM package assets,
- operator soak reports and `operator_soak_status.json`,
- GitHub release notes URL,
- security/ethics audit notes,
- config docs hash or commit id.
