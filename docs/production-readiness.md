# Production Readiness Audit

Last audited: 2026-06-16.

Latest committed release-evidence commit checked:
`043093e87299cc84e46ef37344f98eea50fa0472`.

## Verdict

UltraSQL is not production ready for v1.0 yet.

It is a fast pre-alpha database with real server, storage, WAL, MVCC, SQL,
client, packaging, benchmark, fuzz, sanitizer, and release-evidence work in
place. The current evidence does not support a claim that UltraSQL is the best
database in all aspects, or that it is ready for unsupervised production use.

The ethical claim today is narrower:

- UltraSQL leads the committed 2026-06-14 release-artifact DB-vs-DB scale sweep
  on 24 of 24 comparable measured rows on the recorded Apple M4 host.
- UltraSQL had green `main` CI for the previous evidence/docs commit
  `8f771ace`; `043093e8` CI was still in progress when this non-code outreach
  packet was prepared.
- UltraSQL still lacks required independent production evidence: operator soak
  reports, external audits, incident drills, and a release-commit driver status
  artifact.

## Evidence Checked

| Area | Evidence | Current result |
| --- | --- | --- |
| CI | GitHub `ci` run `27593531225` for commit `043093e8` | in progress during this audit refresh; do not sign off until success |
| Previous CI | GitHub `ci` run `27572340131` for commit `8f771ace` | success |
| Docs CI | GitHub `docs` run `27593531220` for commit `043093e8` | success |
| Production evidence workflow | GitHub run `27540874758` for commit `f9fc5c6f` | success, with not-ready release gates recorded |
| Operator soak workflow | GitHub run `27539947931` for commit `f9fc5c6f` | success, status is `not_ready` |
| Coverage workflow | GitHub run `27535619220` for commit `f9fc5c6f` | success |
| Fuzz workflow | GitHub run `27529191424` for commit `f9fc5c6f` | success |
| Sanitizers workflow | GitHub run `27526490428` for commit `f9fc5c6f` | success |
| Bench workflow | GitHub run `27532810814` for commit `f9fc5c6f` | cancelled; latest committed scale-sweep artifact remains 2026-06-14 |
| Release-artifact scale sweep | `benchmarks/results/latest/scale-sweep/scale_sweep.json` | UltraSQL fastest on 24 of 24 comparable measured rows |
| Operator soak status | `benchmarks/results/latest/operator_soak_status.json` | `not_ready`; 0 valid reports, need 3 independent 30-day reports for commit `043093e8` |
| External audit status | `benchmarks/results/latest/external_audit_status.json` | `not_ready`; 0 valid reports, need 2 independent reports covering security and correctness for commit `043093e8` |
| Incident drill status | `benchmarks/results/latest/incident_drill_status.json` | `not_ready`; 0 valid drills, need backup restore, WAL recovery, and disk-full drills for commit `043093e8` |
| Driver compatibility status | `benchmarks/results/latest/driver_compatibility_status.json` | `not_ready`; committed status lacks `target/driver-certification.json` for the audited commit |

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
  not a new 2026-06-15 full benchmark run.
- Broader production claims still need longer fuzz windows, Miri breadth,
  larger benchmark scales, WAL-backed data-dir benchmark modes, broader SQL
  regression coverage, and real incident-response practice.

## Claim Policy

Allowed:

```text
UltraSQL was fastest on all 24 comparable rows in the committed 2026-06-14
release-artifact DB-vs-DB scale sweep on the recorded Apple M4 host.
```

Not allowed:

```text
UltraSQL is production ready.
UltraSQL is the best database in every aspect.
UltraSQL beats every database on every workload.
```

Those stronger claims require every release gate in
`docs/release-checklist.md` to close with committed evidence.

## Next Required Work

1. Produce strict, release-commit driver certification and commit the status
   artifact for that release commit.
2. Run and validate three independent 30-day operator soaks.
3. Complete independent security and correctness audits.
4. Complete backup-restore, WAL-recovery, and disk-full incident drills.
5. Rerun benchmark certification on the release host, including the
   release-artifact DB-vs-DB scale sweep with ClickHouse enabled.
6. Keep expanding correctness coverage, SQL compatibility, fuzzing, Miri,
   sanitizer, crash-recovery, replication, backup, and observability evidence.
