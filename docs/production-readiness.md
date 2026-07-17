# Production Readiness Audit

Last audited: 2026-07-17.

This page records the current honest readiness verdict. Evidence lives in
committed artifacts, not in this prose; where the two disagree, the artifact
wins.

## Verdict

UltraSQL is not production ready for v1.0 yet.

It is an alpha database with real server, storage, WAL, MVCC, SQL, client,
packaging, benchmark, fuzz, sanitizer, and release-evidence work in place.
The current evidence does not support a claim that UltraSQL is the best
database in all aspects, or that it is ready for unsupervised production use.

## Evidence corrections

- **All TPC-H claims are withdrawn.** The previously published SF1/SF10
  "certifications" measured per-query answer-cache fast paths that have been
  removed from the engine; the artifacts were deleted. No TPC-H claim exists
  until the runners are re-executed against the real executor (see the
  retraction in `BENCHMARKS.md`).
- **The README table is one committed scale sweep**, not a median of three
  independent sweeps. It is pinned to artifact commit `a6a97af1`, runs
  UltraSQL's result-replay cache disabled, and records UltraSQL as the lowest
  median in 21 of 24 rows. Driver lifecycle, physical schema, bulk API, result
  draining, effective warmup, and durability configuration are not fully
  symmetric; `BENCHMARKS.md` discloses the differences.
- **UPDATE/DELETE throughput is statement timing.** The measured statement runs
  inside `BEGIN`, while the outer `ROLLBACK` and restore work are outside the
  timer. Those rows do not measure durable commit or fsync latency.
- **Benchmark certification is artifact certification.**
  `benchmark_certification_status.json` is `ready` for pinned artifact commit
  `a6a97af1`; it verifies finite raw samples, recomputed medians,
  raw/rendered identity, answer hashes, provenance, and containment. It does
  not certify fair/symmetric methodology, current HEAD, or the aggregate
  release gate. Smoke benchmark evidence is not full release benchmark
  certification.
- The docs gate no longer fails when UltraSQL loses a benchmark row; losses
  are reported as data.

## Evidence status

| Area | Artifact | Current result |
| --- | --- | --- |
| Release-artifact scale sweep | `benchmarks/results/latest/scale-sweep/scale_sweep.json` | one committed data-dir sweep pinned to `a6a97af1`; 24 comparable rows, with 21 UltraSQL lowest medians and 3 reported losses |
| Benchmark certification | `benchmarks/results/latest/benchmark_certification_status.json` | `ready` for pinned artifact commit `a6a97af1`; not a certification of current HEAD or symmetric methodology |
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
- Current HEAD has no fresh release-ship scale-sweep artifact, so the pinned
  `a6a97af1` numbers cannot be used as measurements of current changes.
- WAL-backed VACUUM intentionally defers physical heap compaction and slot
  reuse until reclamation has a crash-safe WAL record. UPDATE redirect chains
  and their index entries are retained for correctness, so persistent
  write-heavy relations can accumulate heap and index bloat.

## Claim policy

Allowed: workload-specific claims quoting a committed artifact under
`benchmarks/results/latest/`, naming the host and artifact commit, describing
the timer boundary, and disclosing losses and material methodology differences
in the same breath.

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

1. Run and commit a fresh data-dir release-ship scale sweep on the selected
   release commit; regenerate the README only from that artifact.
2. Re-run TPC-H through the real executor and publish a result only if the
   complete raw evidence validates.
3. Regenerate benchmark certification, driver status, and the aggregate
   release gate on the selected release commit.
4. Add crash-safe WAL records for physical heap VACUUM/slot reuse and safe
   index-entry retargeting; until then, document and monitor bloat.
5. Close the external evidence gates (soaks, audits, drills) — third parties
   and time, not code.
