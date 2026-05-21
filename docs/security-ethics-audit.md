# Security And Ethics Audit

This audit is a release gate. It prevents provenance mistakes and benchmark
claims that cannot be reproduced.

## Required checks

- No tool attribution credits.
- No proprietary tests.
- No copied closed-source code.
- No fake benchmark claims.

## Verification commands

Run these before release:

```bash
rg -n "Co-authored-by|Generated-by|automation|automation|automation|automation|tool attribution|generated" .
rg -n "TH3|proprietary|confidential|internal use only|do not distribute" tests docs crates benchmarks
rg -n "copied from|decompiled|reverse engineered|Firebolt source|closed-source code" .
rg -n "faster than|beats|2x|5x|winner|supremacy" README.md ROADMAP.md BENCHMARKS.md docs benchmarks
```

Any hit must resolve to one of:

- a project rule forbidding that behavior,
- a public-license note with provenance,
- a benchmark artifact path and command that reproduce the number,
- or deleted/reworded text.

## Firebolt Core

Firebolt Core is a closed-source Docker image used only as an external local
competitor. UltraSQL does not vendor, copy, redistribute, decompile, or derive
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
reasons. SQLite TH3 and any proprietary corpus are forbidden.

## Benchmark claims

A benchmark claim exists only when the committed runner produced a measured
artifact on a recorded host. Missing competitors, missing datasets, Docker
unavailability, or unsupported SQL shapes must be recorded as `not_available`.
Do not convert those records into wins.
