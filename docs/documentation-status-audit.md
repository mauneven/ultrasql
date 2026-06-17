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
- Current benchmark claim remains narrow and honest: UltraSQL was the fastest
  measured engine on 17 of 24 workloads in the committed release-artifact
  DB-vs-DB scale sweep (pinned commit 77a92d7c) on the recorded Apple M4 host.
  PostgreSQL 17, DuckDB, ClickHouse, and SQLite each win one or more of the
  other workloads (point-mixed OLTP, single-shot INSERT, bulk update/delete),
  and the durable 1M INSERT is recorded not_available. The certification gate
  certifies fair methodology and reports this win/loss scoreboard rather than
  requiring a clean sweep.
- That claim does not mean the full release benchmark-certification gate is
  closed. The committed benchmark certification manifest is smoke-profile
  evidence, while release sign-off still needs full-profile and WAL-backed
  data-dir benchmark evidence.
- Current certification artifacts are mixed: TPC-H SF1/SF10 and AI vector
  pgvector certification pass; TPC-B, TPC-C, Sysbench, ClickBench, and Firebolt
  sparse pruning are not passing release-certification targets.
- Production readiness remains unproven: independent 30-day operator soaks,
  external audits, incident drills, and final release evidence still need to
  close.

## First-Party Markdown Audit Ledger

The audit covers these 85 files. "Checked" means the file was inspected for
stale maturity language, unsupported production-readiness claims, and broad
benchmark claims that are not backed by committed artifacts.

| File | Audit result |
| --- | --- |
| `.github/ISSUE_TEMPLATE/bug_report.md` | GitHub template; no project maturity claim |
| `.github/ISSUE_TEMPLATE/feature_request.md` | GitHub template; no project maturity claim |
| `.github/pull_request_template.md` | GitHub template; no project maturity claim |
| `AGENTS.md` | benchmark/performance policy; checked for claim scoping |
| `ARCHITECTURE.md` | project documentation; checked for stale maturity/overclaim terms |
| `BENCHMARKS.md` | benchmark/performance policy; checked for claim scoping |
| `CHANGELOG.md` | historical evidence ledger; keep dated entries, avoid rewriting old evidence |
| `CODE_OF_CONDUCT.md` | project documentation; checked for stale maturity/overclaim terms |
| `CONTRIBUTING.md` | project documentation; checked for stale maturity/overclaim terms |
| `DONE.md` | historical evidence ledger; keep dated entries, avoid rewriting old evidence |
| `GOVERNANCE.md` | project documentation; checked for stale maturity/overclaim terms |
| `OPERATIONS.md` | project documentation; checked for stale maturity/overclaim terms |
| `PERFORMANCE.md` | benchmark/performance policy; checked for claim scoping |
| `README.md` | status/claim surface; updated or checked against current evidence |
| `RFC_PROCESS.md` | project documentation; checked for stale maturity/overclaim terms |
| `ROADMAP.md` | status/claim surface; updated or checked against current evidence |
| `SECURITY.md` | project documentation; checked for stale maturity/overclaim terms |
| `SECURITY_AUDIT.md` | project documentation; checked for stale maturity/overclaim terms |
| `benchmarks/results/latest/benchmark_arena_artifacts.md` | benchmark artifact; checked for scoped result wording |
| `benchmarks/results/latest/methodology.md` | benchmark artifact; checked for scoped result wording |
| `benchmarks/results/latest/results.md` | benchmark artifact; checked for scoped result wording |
| `benchmarks/results/latest/scale-sweep/results.md` | benchmark artifact; checked for scoped result wording |
| `benchmarks/results/latest/scale-sweep/scale_sweep.md` | benchmark artifact; checked for scoped result wording |
| `benchmarks/results/latest/slt_authored_speed_comparison.md` | benchmark artifact; checked for scoped result wording |
| `benchmarks/results/latest/slt_hydromatic_smoke_comparison.md` | benchmark artifact; checked for scoped result wording |
| `benchmarks/results/latest/slt_speed_comparison.md` | benchmark artifact; checked for scoped result wording |
| `docs/OPERATOR_SOAK.md` | release evidence gate; not-ready wording checked |
| `docs/ai-database-strategy.md` | project documentation; checked for stale maturity/overclaim terms |
| `docs/backup-restore.md` | project documentation; checked for stale maturity/overclaim terms |
| `docs/catalog-upgrades.md` | project documentation; checked for stale maturity/overclaim terms |
| `docs/chaos-recovery.md` | project documentation; checked for stale maturity/overclaim terms |
| `docs/configuration.md` | project documentation; checked for stale maturity/overclaim terms |
| `docs/documentation-status-audit.md` | project documentation; checked for stale maturity/overclaim terms |
| `docs/driver-certification.md` | project documentation; checked for stale maturity/overclaim terms |
| `docs/external-audits.md` | release evidence gate; not-ready wording checked |
| `docs/external-validation-outreach.md` | status/claim surface; updated or checked against current evidence |
| `docs/getting-started.md` | status/claim surface; updated or checked against current evidence |
| `docs/hnsw-index-design.md` | project documentation; checked for stale maturity/overclaim terms |
| `docs/incident-drills.md` | release evidence gate; not-ready wording checked |
| `docs/index.md` | status/claim surface; updated or checked against current evidence |
| `docs/install.md` | status/claim surface; updated or checked against current evidence |
| `docs/known-limitations.md` | status/claim surface; updated or checked against current evidence |
| `docs/migration-guide.md` | project documentation; checked for stale maturity/overclaim terms |
| `docs/operator-reports.md` | release evidence gate; not-ready wording checked |
| `docs/packaging.md` | status/claim surface; updated or checked against current evidence |
| `docs/private-preview-runbook.md` | status/claim surface; updated or checked against current evidence |
| `docs/production-readiness.md` | status/claim surface; updated or checked against current evidence |
| `docs/rag-tenant-security.md` | project documentation; checked for stale maturity/overclaim terms |
| `docs/release-checklist.md` | release evidence gate; not-ready wording checked |
| `docs/release-notes-template.md` | release evidence gate; not-ready wording checked |
| `docs/security-ethics-audit.md` | project documentation; checked for stale maturity/overclaim terms |
| `docs/sql/alter-view.md` | SQL reference; no project maturity claim |
| `docs/sql/checkpoint.md` | SQL reference; no project maturity claim |
| `docs/sql/create-view.md` | SQL reference; no project maturity claim |
| `docs/sql/describe.md` | SQL reference; no project maturity claim |
| `docs/sql/export-import.md` | SQL reference; no project maturity claim |
| `docs/sql/merge.md` | SQL reference; no project maturity claim |
| `docs/sql/pivot.md` | SQL reference; no project maturity claim |
| `docs/sql/set-variable.md` | SQL reference; no project maturity claim |
| `docs/sql/summarize.md` | SQL reference; no project maturity claim |
| `docs/sql/unpivot.md` | SQL reference; no project maturity claim |
| `docs/superpowers/plans/2026-06-14-missing-sql-statements.md` | implementation plan archive; no public maturity claim |
| `docs/testing/coverage-evidence-2026-05-24.md` | historical evidence ledger; keep dated entries, avoid rewriting old evidence |
| `docs/testing/coverage-evidence-2026-05-28.md` | historical evidence ledger; keep dated entries, avoid rewriting old evidence |
| `docs/testing/coverage-evidence-2026-05-29-cli.md` | historical evidence ledger; keep dated entries, avoid rewriting old evidence |
| `docs/testing/coverage-evidence-2026-05-29-core.md` | historical evidence ledger; keep dated entries, avoid rewriting old evidence |
| `docs/testing/coverage-evidence-2026-05-29-executor.md` | historical evidence ledger; keep dated entries, avoid rewriting old evidence |
| `docs/testing/coverage-evidence-2026-05-29-planner.md` | historical evidence ledger; keep dated entries, avoid rewriting old evidence |
| `docs/testing/coverage-evidence-2026-05-29-server.md` | historical evidence ledger; keep dated entries, avoid rewriting old evidence |
| `docs/testing/coverage-evidence-2026-05-29-sqllogictest-runner.md` | historical evidence ledger; keep dated entries, avoid rewriting old evidence |
| `docs/testing/coverage-evidence-2026-05-29-workspace.md` | historical evidence ledger; keep dated entries, avoid rewriting old evidence |
| `docs/testing/external-sql-test-reuse.md` | historical evidence ledger; keep dated entries, avoid rewriting old evidence |
| `docs/testing/isolation-suite.md` | historical evidence ledger; keep dated entries, avoid rewriting old evidence |
| `external-audits/README.md` | release evidence gate; not-ready wording checked |
| `fuzz/README.md` | fuzz docs; no public maturity claim |
| `incident-drills/README.md` | release evidence gate; not-ready wording checked |
| `operator-reports/README.md` | release evidence gate; not-ready wording checked |
| `packages/npm/README.md` | status/claim surface; updated or checked against current evidence |
| `tests/isolation/NOTICE.hermitage.md` | test/imported fixture docs; no public maturity claim |
| `tests/isolation/README.md` | test/imported fixture docs; no public maturity claim |
| `tests/slt/README.md` | test/imported fixture docs; no public maturity claim |
| `tests/slt/portable/imported/hydromatic/README.md` | test/imported fixture docs; no public maturity claim |
| `tests/slt/sql_regression/README.md` | test/imported fixture docs; no public maturity claim |
| `tests/slt/sql_regression/regression_subset/README.md` | test/imported fixture docs; no public maturity claim |
| `third_party/sqllogictest/README.md` | test/imported fixture docs; no public maturity claim |

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
python3 scripts/validate-documentation-status.py
```

The validator checks that this ledger covers every first-party Markdown file,
that stale maturity labels do not return, that unsupported universal claims stay
inside forbidden-claim examples, and that benchmark wording remains backed by
the committed benchmark artifacts.

For a quick text scan, run:

```bash
rg -n -i "pre[- ]alpha|production ready|best database in all aspects|beats every database" \
  README.md docs packages/npm external-audits incident-drills operator-reports \
  -g '*.md'
```

Expected result: any lower-maturity-label, `production ready`, or
`best database` matches must appear only in claim-policy, forbidden-claim,
historical-audit, or release-gate contexts.
