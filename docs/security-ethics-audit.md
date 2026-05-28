# Security And Ethics Audit

This audit is a release gate. It prevents provenance mistakes and benchmark
claims that cannot be reproduced.

## Required checks

- No proprietary tests.
- No closed-source code.
- No fake benchmark claims.

## Verification commands

Run these before release:

```bash
rg -n "TH3|proprietary|confidential|internal use only|do not distribute" tests docs crates benchmarks
rg -n "decompiled|reverse engineered|Firebolt source|closed-source code" .
rg -n "faster than|beats|winner|unsupported benchmark claim" README.md ROADMAP.md DONE.md BENCHMARKS.md docs benchmarks
```

Any hit must resolve to one of:

- a project rule forbidding that behavior,
- a public-license note with provenance,
- a benchmark artifact path and command that reproduce the number,
- or deleted/reworded text.

## Firebolt Core

Firebolt Core is a closed-source Docker image used only as an external local
measured engine. UltraSQL does not vendor, redistribute, decompile, or derive
code from that image. Firebolt Core benchmark artifacts must say
`core_mode: local_docker` and must use the committed local helper:

```bash
benchmarks/firebolt_core_local.sh start
benchmarks/firebolt_core_local.sh query "SELECT 42;"
benchmarks/firebolt_core_local.sh stop
```

## SQL test provenance

Portable SQLLogicTest imports must be public and license-reviewed. PostgreSQL
regression subsets must come from public PostgreSQL tests with explicit skip
reasons. The current PostgreSQL regression subset records `select.sql`,
`char.sql`, `varchar.sql`, `numeric.sql`, `type_sanity.sql`,
`text.sql`, `date.sql`, `time.sql`, `timestamp.sql`, `timetz.sql`,
`json.sql`, `jsonb.sql`, `arrays.sql`, `create_index.sql`,
`constraints.sql`, `create_operator.sql`, and `opr_sanity.sql` provenance at a
pinned public commit and keeps unsupported catalog/operator/type-breadth
invariants as visible skips. SQLite TH3 and any proprietary corpus are
forbidden.

Hermitage isolation scenarios are CC BY 4.0 and must keep attribution, pinned
commit provenance, and local reviewer notes beside the tests. UltraSQL ports
selected schedules into Rust integration tests instead of vendoring the upstream
Markdown dump.

## Benchmark claims

A benchmark claim exists only when the committed runner produced a measured
artifact on a recorded host. Missing measured engines, missing datasets, Docker
unavailability, or unsupported SQL shapes must be recorded as `not_available`.
Do not convert those records into wins.
