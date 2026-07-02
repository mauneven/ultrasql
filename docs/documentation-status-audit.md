# Documentation Status Audit

Last audited: 2026-07-01 (full re-audit; supersedes the 2026-06 audits).

UltraSQL is alpha. It is not production ready until the release gates in
`TODO.md` close with committed evidence, and no document in this repository
may claim otherwise. Benchmark evidence in docs is scoped to the committed
artifacts under `benchmarks/results/latest/`; smoke benchmark evidence
does not mean the full release benchmark-certification gate is closed.

## 2026-07-01 corrections

This audit accompanied a truthfulness pass that changed what the docs are
allowed to say:

- **All TPC-H claims were withdrawn.** Previously published TPC-H SF1/SF10
  "certifications" (including a "351x vs PostgreSQL 17" ratio) measured
  per-query answer-cache fast paths that were removed from the engine; the
  artifacts were deleted. See the retraction in `BENCHMARKS.md`.
- **The docs gate no longer demands a clean sweep.** The validator used to
  error when the scale sweep contained any row where UltraSQL was not the
  fastest engine; losses are now reported as data, never as errors.
- `DONE.md`, `GOVERNANCE.md`, and `RFC_PROCESS.md` were removed: the first
  repeated withdrawn claims, the latter two described a multi-maintainer
  process this single-maintainer project does not run.
- The scale-sweep scoreboard and its prose are being re-measured with the
  result-cache disclosure and harness-fairness fixes; until that run lands,
  treat scoreboard numbers as historical.

## Ledger

Every first-party Markdown file is listed here; the docs CI gate
(`scripts/validate-documentation-status.py`) fails if this ledger and the
repository disagree.

| File | Audit result |
| --- | --- |
| `.github/ISSUE_TEMPLATE/bug_report.md` | checked |
| `.github/ISSUE_TEMPLATE/feature_request.md` | checked |
| `.github/pull_request_template.md` | checked |
| `AGENTS.md` | checked |
| `ARCHITECTURE.md` | checked |
| `BENCHMARKS.md` | checked |
| `BETA_READINESS.md` | checked |
| `CHANGELOG.md` | checked |
| `CODE_OF_CONDUCT.md` | checked |
| `CONTRIBUTING.md` | checked |
| `OPERATIONS.md` | checked |
| `PERFORMANCE.md` | checked |
| `README.md` | checked |
| `SECURITY.md` | checked |
| `SECURITY_AUDIT.md` | checked |
| `TODO.md` | checked |
| `benchmarks/results/latest/benchmark_arena_artifacts.md` | checked |
| `benchmarks/results/latest/results.md` | checked |
| `benchmarks/results/latest/scale-sweep/results.md` | checked |
| `benchmarks/results/latest/scale-sweep/scale_sweep.md` | checked |
| `benchmarks/results/latest/slt_authored_speed_comparison.md` | checked |
| `benchmarks/results/latest/slt_hydromatic_smoke_comparison.md` | checked |
| `benchmarks/results/latest/slt_speed_comparison.md` | checked |
| `docs/OPERATOR_SOAK.md` | checked |
| `docs/ai-database-strategy.md` | checked |
| `docs/backup-restore.md` | checked |
| `docs/benchmark-integrity-completion-2026-06.md` | checked |
| `docs/catalog-upgrades.md` | checked |
| `docs/chaos-recovery.md` | checked |
| `docs/configuration.md` | checked |
| `docs/documentation-status-audit.md` | checked |
| `docs/driver-certification.md` | checked |
| `docs/engineering-report-2026-06.md` | checked |
| `docs/evalplanqual-design.md` | checked |
| `docs/external-audits.md` | checked |
| `docs/external-validation-outreach.md` | checked |
| `docs/filtered-ann.md` | checked |
| `docs/getting-started.md` | checked |
| `docs/hnsw-index-design.md` | checked |
| `docs/hybrid-search.md` | checked |
| `docs/incident-drills.md` | checked |
| `docs/index.md` | checked |
| `docs/install.md` | checked |
| `docs/known-limitations.md` | checked |
| `docs/migration-guide.md` | checked |
| `docs/operator-reports.md` | checked |
| `docs/packaging.md` | checked |
| `docs/private-preview-runbook.md` | checked |
| `docs/production-readiness.md` | checked |
| `docs/rag-tenant-security.md` | checked |
| `docs/release-checklist.md` | checked |
| `docs/release-notes-template.md` | checked |
| `docs/savepoint-subtransactions-design.md` | checked |
| `docs/security-ethics-audit.md` | checked |
| `docs/sql/alter-view.md` | checked |
| `docs/sql/checkpoint.md` | checked |
| `docs/sql/create-view.md` | checked |
| `docs/sql/describe.md` | checked |
| `docs/sql/export-import.md` | checked |
| `docs/sql/merge.md` | checked |
| `docs/sql/pivot.md` | checked |
| `docs/sql/set-variable.md` | checked |
| `docs/sql/summarize.md` | checked |
| `docs/sql/unpivot.md` | checked |
| `docs/streaming-replication-design.md` | checked |
| `docs/superpowers/plans/2026-06-14-missing-sql-statements.md` | checked |
| `docs/testing/coverage-evidence-2026-05-24.md` | checked |
| `docs/testing/coverage-evidence-2026-05-28.md` | checked |
| `docs/testing/coverage-evidence-2026-05-29-cli.md` | checked |
| `docs/testing/coverage-evidence-2026-05-29-core.md` | checked |
| `docs/testing/coverage-evidence-2026-05-29-executor.md` | checked |
| `docs/testing/coverage-evidence-2026-05-29-planner.md` | checked |
| `docs/testing/coverage-evidence-2026-05-29-server.md` | checked |
| `docs/testing/coverage-evidence-2026-05-29-sqllogictest-runner.md` | checked |
| `docs/testing/coverage-evidence-2026-05-29-workspace.md` | checked |
| `docs/testing/external-sql-test-reuse.md` | checked |
| `docs/testing/isolation-suite.md` | checked |
| `docs/transactional-ddl-design.md` | checked |
| `docs/transactional-embeddings.md` | checked |
| `docs/vector-benchmarks.md` | checked |
| `examples/node-rag/README.md` | checked |
| `external-audits/README.md` | checked |
| `fuzz/README.md` | checked |
| `incident-drills/README.md` | checked |
| `operator-reports/2026-06-benchmark-row-analysis.md` | checked |
| `operator-reports/2026-06-hnsw-build-scaling.md` | checked |
| `operator-reports/2026-06-hnsw-hierarchical-layers.md` | checked |
| `operator-reports/2026-06-ivfflat-filtered-ann.md` | checked |
| `operator-reports/README.md` | checked |
| `packages/npm/README.md` | checked |
| `tests/isolation/NOTICE.hermitage.md` | checked |
| `tests/isolation/README.md` | checked |
| `tests/slt/README.md` | checked |
| `tests/slt/portable/imported/hydromatic/README.md` | checked |
| `tests/slt/sql_regression/README.md` | checked |
| `tests/slt/sql_regression/regression_subset/README.md` | checked |
| `third_party/sqllogictest/README.md` | checked |
