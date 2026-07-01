# Production Readiness Audit

Last audited: 2026-06-16.

Evidence baseline observed before this docs-only audit update:
`7449f11d8c9ee130c228298f2a57407ec01b9580`.

This page records evidence that was current at audit time. Every docs-only
commit creates a newer CI run, so do not treat these run IDs as a permanent
"latest CI" claim. Re-check the current head with:

```bash
git rev-parse HEAD
gh run list --branch main --limit 6 \
  --json databaseId,workflowName,headSha,status,conclusion,url
```

## Verdict

UltraSQL is not production ready for v1.0 yet.

It is a fast alpha database with real server, storage, WAL, MVCC, SQL, client,
packaging, benchmark, fuzz, sanitizer, and release-evidence work in place. The
current evidence does not support a claim that UltraSQL is the best database in
all aspects, or that it is ready for unsupervised production use.

The ethical claim today is narrower:

- UltraSQL was the fastest measured engine on 21 of 24 comparable workloads in
  the committed release-artifact DB-vs-DB scale sweep on the recorded Apple M4
  host; PostgreSQL, DuckDB, ClickHouse, and SQLite each win one or more of the
  remaining 7. This matches the Claim Policy below and the README scoreboard.
- UltraSQL had green `main` CI for commit `8f771ace`. CI for the current
  evidence baseline was not rechecked in this docs pass.
- UltraSQL still lacks required independent production evidence: operator soak
  reports, external audits, incident drills, release-commit driver status, and
  fresh strict benchmark certification.

## Evidence Checked

| Area | Evidence | Current result |
| --- | --- | --- |
| CI | GitHub `ci` run `27593967602` for evidence baseline commit `cc1d5b2c` | in progress during this audit update; do not sign off until a current-head CI run succeeds |
| Previous CI | GitHub `ci` run `27572340131` for commit `8f771ace` | success |
| Docs CI | GitHub `docs` run `27593967575` for commit `cc1d5b2c` | success |
| Production evidence workflow | GitHub run `27540874758` for commit `f9fc5c6f` | success, with not-ready release gates recorded |
| Operator soak workflow | GitHub run `27539947931` for commit `f9fc5c6f` | success, status is `not_ready` |
| Coverage workflow | GitHub run `27535619220` for commit `f9fc5c6f` | success |
| Fuzz workflow | GitHub run `27529191424` for commit `f9fc5c6f` | success |
| Sanitizers workflow | GitHub run `27526490428` for commit `f9fc5c6f` | success |
| Bench workflow | GitHub run `27532810814` for commit `f9fc5c6f` | cancelled; latest committed scale-sweep artifact remains 2026-06-14 |
| Release-artifact scale sweep | `benchmarks/results/latest/scale-sweep/scale_sweep.json` | UltraSQL fastest on 21 of 24 comparable measured rows; the other 3 (1M bulk UPDATE, 1M bulk DELETE, point-op Mixed OLTP) are led by DuckDB / ClickHouse / in-process SQLite |
| Benchmark certification status | `benchmarks/results/latest/benchmark_certification_status.json` | `not_ready`; committed scale sweep is stale, lacks data-dir mode evidence, and predates strict raw-artifact schema |
| Aggregate release gate | `benchmarks/results/latest/release_gate_status.json` | `not_ready`; blockers remain across driver, operator soak, external audit, incident drill, and benchmark gates |
| Operator soak status | `benchmarks/results/latest/operator_soak_status.json` | `not_ready`; 0 valid release reports, need 3 independent 30-day reports |
| External audit status | `benchmarks/results/latest/external_audit_status.json` | `not_ready`; 0 valid reports, need 2 independent reports covering security and correctness for commit `043093e8` |
| Incident drill status | `benchmarks/results/latest/incident_drill_status.json` | `not_ready`; 0 valid release drills, need backup restore, WAL recovery, and disk-full drills |
| Driver compatibility status | `benchmarks/results/latest/driver_compatibility_status.json` | `not_ready`; committed status still lacks a release-status report generated from `target/driver-certification.json` |

## What UltraSQL Can Do Now

- Run as a server over the PostgreSQL wire protocol, as a CLI, as local runner
  binaries, and through the embedded Node/Bun package.
- Parse, bind, optimize, and execute a broad SQL subset over MVCC heap storage
  with WAL, indexes, vectorized execution paths, JSON/JSONB, text search,
  vector types, HNSW/IVFFlat surfaces, COPY, external scans, regular views,
  `ALTER VIEW` metadata operations, `EXPORT DATABASE`, `IMPORT DATABASE`,
  `PIVOT`, and `UNPIVOT`.
- Exercise driver certification for common direct drivers, ORMs, CLI tooling,
  GUI introspection query families, and migration tools.
- Build and test release packaging paths for archives, npm, Docker, Homebrew,
  Debian, RPM, AUR, Chocolatey, and Windows setup artifacts.
- Produce reproducible DB-vs-DB benchmark artifacts against local DuckDB,
  ClickHouse, SQLite, PostgreSQL, and optional local Firebolt Core where setup
  is available.

## What Is Not Proven Yet

- No three independent operators have run the audited release commit for 30
  continuous days with clean reports.
- No two independent external audit reports are present for security and
  correctness.
- No release incident drill reports are present for backup restore, WAL
  recovery, and disk-full response.
- The committed driver status is intentionally `not_ready` until the release
  commit has a fresh `target/driver-certification.json` and strict validation.
- The latest scheduled benchmark workflow for the audited commit was cancelled,
  so the current README benchmark table is the 2026-06-14 release-artifact run,
  not a fresh current-commit full benchmark run.
- `scripts/validate-release-evidence.py` reports the aggregate release gate as
  `not_ready`; missing or stale evidence fails closed.
- Broader production claims still need longer fuzz windows, Miri breadth,
  larger benchmark scales, strict WAL-backed data-dir benchmark certification,
  broader SQL regression coverage, and real incident-response practice.

## Claim Policy

Allowed:

```text
UltraSQL was the fastest measured engine on 21 of 24 workloads in the committed
release-artifact DB-vs-DB scale sweep (pinned commit 77a92d7c) on the recorded
Apple M4 host; PostgreSQL 17, DuckDB, ClickHouse, and SQLite each win one or more
of the other workloads, and the durable 1M INSERT is recorded not_available.
```

Not allowed:

```text
UltraSQL is production ready.
UltraSQL is the best database in every aspect.
UltraSQL beats every database on every workload.
UltraSQL was fastest on all comparable scale-sweep rows.
UltraSQL leads OLTP / has the fastest writes.
```

Those stronger claims require every release gate in
`docs/release-checklist.md` to close with committed evidence.

## Next Required Work

1. Produce strict, release-commit driver certification and commit the status
   artifact for that release commit.
2. Run and validate three independent 30-day operator soaks.
3. Complete independent security and correctness audits.
4. Complete backup-restore, WAL-recovery, and disk-full incident drills.
5. Rerun benchmark certification on the release host with:
   `python3 scripts/run-benchmark-certification.py --mode full`.
6. Close the aggregate release gate with:
   `python3 scripts/validate-release-evidence.py --commit <release-commit> --strict`.
7. Keep expanding correctness coverage, SQL compatibility, fuzzing, Miri,
   sanitizer, crash-recovery, replication, backup, and observability evidence.
