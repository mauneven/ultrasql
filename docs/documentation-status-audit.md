# Documentation Status Audit

Last audited: 2026-06-16.

Scope: every first-party Markdown file found by:

```bash
find . -name '*.md' \
  -not -path './target/*' \
  -not -path './site/*' \
  -not -path './.git/*' \
  -not -path './tests/driver_certification/node/node_modules/*'
```

Vendored `node_modules` Markdown files are excluded because they belong to
third-party packages installed for driver certification.

## Current Project Status Wording

Use this wording in first-party docs:

```text
UltraSQL is alpha. It is suitable for serious local evaluation, driver
compatibility checks, benchmark reproduction, and controlled private preview.
It is not production ready until the release gates close with evidence.
```

Do not use the older lower-maturity label or these unsupported claims:

```text
production ready
best database in all aspects
```

unless quoting a prohibited claim in the claim-policy section.

## Evidence Summary

- Broad implemented surface exists: server, CLI, embedded Node/Bun, PostgreSQL
  wire protocol, parser, binder, optimizer, vectorized executor, MVCC heap, WAL,
  indexes, JSON/JSONB, text search, vector types, HNSW/IVFFlat, COPY, external
  scans, regular views, export/import, pivot/unpivot, packaging, fuzz,
  sanitizer, release, benchmark, and driver-certification work.
- Current benchmark claim remains narrow: UltraSQL was fastest on all 24
  comparable rows in the committed 2026-06-14 release-artifact DB-vs-DB scale
  sweep on the recorded Apple M4 host.
- Current certification artifacts are mixed: TPC-H SF1/SF10 and AI vector
  pgvector certification pass; TPC-B, TPC-C, Sysbench, ClickBench, and Firebolt
  sparse pruning are not passing release-certification targets.
- Production readiness remains unproven: independent 30-day operator soaks,
  external audits, incident drills, and final release evidence still need to
  close.

## First-Party Markdown Inventory

The audit covers these 85 files:

```text
.github/ISSUE_TEMPLATE/bug_report.md
.github/ISSUE_TEMPLATE/feature_request.md
.github/pull_request_template.md
AGENTS.md
ARCHITECTURE.md
BENCHMARKS.md
CHANGELOG.md
CODE_OF_CONDUCT.md
CONTRIBUTING.md
DONE.md
GOVERNANCE.md
OPERATIONS.md
PERFORMANCE.md
README.md
RFC_PROCESS.md
ROADMAP.md
SECURITY.md
SECURITY_AUDIT.md
benchmarks/results/latest/benchmark_arena_artifacts.md
benchmarks/results/latest/methodology.md
benchmarks/results/latest/results.md
benchmarks/results/latest/scale-sweep/results.md
benchmarks/results/latest/scale-sweep/scale_sweep.md
benchmarks/results/latest/slt_authored_speed_comparison.md
benchmarks/results/latest/slt_hydromatic_smoke_comparison.md
benchmarks/results/latest/slt_speed_comparison.md
docs/OPERATOR_SOAK.md
docs/ai-database-strategy.md
docs/backup-restore.md
docs/catalog-upgrades.md
docs/chaos-recovery.md
docs/configuration.md
docs/driver-certification.md
docs/documentation-status-audit.md
docs/external-audits.md
docs/external-validation-outreach.md
docs/getting-started.md
docs/hnsw-index-design.md
docs/incident-drills.md
docs/index.md
docs/install.md
docs/known-limitations.md
docs/migration-guide.md
docs/operator-reports.md
docs/packaging.md
docs/private-preview-runbook.md
docs/production-readiness.md
docs/rag-tenant-security.md
docs/release-checklist.md
docs/release-notes-template.md
docs/security-ethics-audit.md
docs/sql/alter-view.md
docs/sql/checkpoint.md
docs/sql/create-view.md
docs/sql/describe.md
docs/sql/export-import.md
docs/sql/merge.md
docs/sql/pivot.md
docs/sql/set-variable.md
docs/sql/summarize.md
docs/sql/unpivot.md
docs/superpowers/plans/2026-06-14-missing-sql-statements.md
docs/testing/coverage-evidence-2026-05-24.md
docs/testing/coverage-evidence-2026-05-28.md
docs/testing/coverage-evidence-2026-05-29-cli.md
docs/testing/coverage-evidence-2026-05-29-core.md
docs/testing/coverage-evidence-2026-05-29-executor.md
docs/testing/coverage-evidence-2026-05-29-planner.md
docs/testing/coverage-evidence-2026-05-29-server.md
docs/testing/coverage-evidence-2026-05-29-sqllogictest-runner.md
docs/testing/coverage-evidence-2026-05-29-workspace.md
docs/testing/external-sql-test-reuse.md
docs/testing/isolation-suite.md
external-audits/README.md
fuzz/README.md
incident-drills/README.md
operator-reports/README.md
packages/npm/README.md
tests/isolation/NOTICE.hermitage.md
tests/isolation/README.md
tests/slt/README.md
tests/slt/portable/imported/hydromatic/README.md
tests/slt/sql_regression/README.md
tests/slt/sql_regression/regression_subset/README.md
third_party/sqllogictest/README.md
```

## Audit Result

- Public maturity label corrected from the older lower-maturity label to
  `alpha`.
- Production-readiness language kept strict: v1.0 and production use remain
  blocked until release gates close.
- Benchmark wording remains artifact-scoped, not universal.
- External validation docs now invite reviewers/operators without implying
  production readiness.
- Historical evidence ledgers remain historical; do not rewrite old dated
  evidence unless the old entry itself is false.

## Repeat Check

Run this before release notes or website publication:

```bash
rg -n -i "pre[- ]alpha|production ready|best database in all aspects|beats every database" \
  README.md docs packages/npm external-audits incident-drills operator-reports \
  -g '*.md'
```

Expected result: any lower-maturity-label, `production ready`, or
`best database` matches must appear only in claim-policy, forbidden-claim,
historical-audit, or release-gate contexts.
