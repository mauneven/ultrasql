# Production Readiness Audit

Last audited: 2026-07-01.

This page records the current honest readiness verdict. Evidence lives in
committed artifacts, not in this prose; where the two disagree, the artifact
wins.

## Verdict

UltraSQL is not production ready for v1.0 yet.

It is an alpha database with real server, storage, WAL, MVCC, SQL, client,
packaging, benchmark, fuzz, sanitizer, and release-evidence work in place.
The current evidence does not support a claim that UltraSQL is the best
database in all aspects, or that it is ready for unsupervised production use.

## 2026-07-01 truthfulness corrections

- **All TPC-H claims are withdrawn.** The previously published SF1/SF10
  "certifications" measured per-query answer-cache fast paths that have been
  removed from the engine; the artifacts were deleted. No TPC-H claim exists
  until the runners are re-executed against the real executor (see the
  retraction in `BENCHMARKS.md`).
- **The scale-sweep scoreboard is being re-measured.** The committed sweep
  predates the harness-fairness fixes (result-cache disclosure, symmetric
  warmups, persistent-connection inserts for every engine). Until the fresh
  run lands, scoreboard numbers are historical and must not be quoted as
  current.
- The docs gate no longer fails when UltraSQL loses a benchmark row; losses
  are reported as data.

## Evidence status

| Area | Artifact | Current result |
| --- | --- | --- |
| Release-artifact scale sweep | `benchmarks/results/latest/scale-sweep/scale_sweep.json` | committed run predates the 2026-07-01 harness-fairness fixes; re-measurement pending |
| Benchmark certification | `benchmarks/results/latest/benchmark_certification_status.json` | stale (2026-06-17 run, older code); must be regenerated on the release commit — smoke benchmark evidence is not full release benchmark certification |
| Aggregate release gate | `benchmarks/results/latest/release_gate_status.json` | `not_ready`; missing or stale evidence fails closed |
| Operator soak | `benchmarks/results/latest/operator_soak_status.json` | `not_ready`; 0 valid reports, need 3 independent 30-day reports |
| External audits | `benchmarks/results/latest/external_audit_status.json` | `not_ready`; 0 valid reports, need 2 independent reports (security, correctness) |
| Incident drills | `benchmarks/results/latest/incident_drill_status.json` | `not_ready`; 0 valid release drills |
| Driver compatibility | `benchmarks/results/latest/driver_compatibility_status.json` | `not_ready` until regenerated on the release commit |

## What UltraSQL can do now

- Run as a server over the PostgreSQL wire protocol, as a CLI, as local
  runner binaries, and through the embedded Node/Bun package.
- Parse, bind, optimize, and execute a broad SQL subset over MVCC heap
  storage with WAL, indexes, vectorized execution, JSON/JSONB, text search,
  vector types, HNSW/IVFFlat, COPY, and external scans.
- Exercise driver certification for common drivers, ORMs, CLI tooling, GUI
  introspection query families, and migration tools.
- Produce reproducible DB-vs-DB benchmark artifacts against local DuckDB,
  ClickHouse, SQLite, and PostgreSQL.

## What is not proven yet

- No independent operator soaks, external audits, or executed incident
  drills exist; the GA gate fails closed until they do.
- Concurrent OLTP throughput is far behind PostgreSQL on the committed
  TPC-C/TPC-B/sysbench artifacts (honest `passed: false` results); no
  throughput-leadership claim is allowed.
- The refreshed scale sweep with the fairness-fixed harness has not been
  committed yet.

## Claim policy

Allowed: workload-specific claims quoting a committed artifact under
`benchmarks/results/latest/`, naming the host and commit, and disclosing
losses in the same breath.

Not allowed:

```text
UltraSQL is production ready.
UltraSQL is the best database in every aspect.
UltraSQL beats every database on every workload.
UltraSQL was fastest on all comparable scale-sweep rows.
UltraSQL leads OLTP / has the fastest writes.
Any TPC-H number (withdrawn 2026-07-01, pending honest re-measurement).
```

## Next required work

1. Land the fairness-fixed scale sweep and regenerate the README scoreboard
   from it.
2. Re-run TPC-H SF1 through the real executor and publish whatever it shows.
3. Regenerate benchmark certification and driver status on the release
   commit.
4. Close the external evidence gates (soaks, audits, drills) — third
   parties and time, not code.
