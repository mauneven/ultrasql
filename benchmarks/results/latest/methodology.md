# Benchmark Methodology

## How to Reproduce

Run the full suite at the chosen tier:

```sh
benchmarks/run.sh [low|ultra] [engines]
```

- `low` (default): 100 000 rows, 5 measured iterations, 1 warmup. Fast
  feedback; suitable for CI and development.
- `ultra`: 10 000 000 rows, 8 measured iterations, 2 warmup. Publishable
  numbers from a large, cache-pressure-inducing dataset.

Engines default to `postgres17,duckdb,sqlite3,clickhouse,cockroachdb`.

## Host Descriptor

Every published result must record the host that produced it. The fields
below follow the schema documented in `BENCHMARKS.md`:

```yaml
host:
  hostname:      <fill in>
  cpu_model:     <fill in>
  cpu_cores:     <fill in>
  cpu_threads:   <fill in>
  cpu_freq_ghz:  <fill in>
  ram_gb:        <fill in>
  storage:
    type:        <fill in>
    model:       <fill in>
  os:
    kernel:      <fill in>
    name:        <fill in>
    version:     <fill in>
  rust:
    channel:     stable
    version:     <fill in>
    target:      <fill in>
  ultrasql:
    commit:      <fill in>
    profile:     release
  power:
    plugged_in:  true
    thermal_state: nominal
```

## Workloads

All workloads live in `crates/ultrasql-bench/src/bin/`. The `--tier` flag
controls the dataset size and iteration counts.

| Binary                  | Workloads covered                           |
|-------------------------|---------------------------------------------|
| `cross_compare`         | sum, count, min, max, minmax, avg, filter, range, point |
| `cross_compare_writes`  | insert-bulk, update, delete                 |
| `cross_concurrency`     | conc-read-sum, conc-read-point, conc-insert, conc-update |
| `point_lookup`          | point-10m-probes (standalone deep probe)    |

## Raw Output Schema

Each `raw/<workload>-<engine>.json` file is a single JSON object:

```json
{
  "workload":      "<name>",
  "n_rows":        <integer>,
  "samples":       <integer>,
  "median_us":     <float>,
  "min_us":        <float>,
  "iterations_us": [<float>, ...],
  "answer":        "<checksum or summary string>"
}
```

The `results-render` binary reads every file in `raw/` matching `*.json`
and produces `results.md` and `results.json` from them.

## Caveat

UltraSQL at v0.x has no full SQL pipeline for aggregate queries. The read
workloads measure individual SIMD kernels (plus the B+ tree for point
lookups). Every other engine measures its full SQL pipeline
(parse → plan → execute → serialize). The two are not directly comparable;
the caveat appears at the top of every results table.

## Prior Runs

Historical per-run result directories were removed in the 2026-05-13
cleanup. The `benchmarks/baselines/` stage files (used by the regression
gate) are the authoritative historical record. See `BENCHMARKS.md` for
the stage gate policy.
