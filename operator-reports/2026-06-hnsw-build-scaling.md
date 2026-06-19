# Page-backed HNSW build scaling — graph-traversal construction

Date: 2026-06-19. Host: Apple M4 (`Darwin arm64`), rustc 1.95.0, release build.
Reproduce: `benchmarks/vector_hnsw_build_scaling.sh` (page-backed `vector-memory`
workload, `hnsw_build_time_us`). Code: commit `33ef498b`.

## Problem

The page-backed (server-path) HNSW built every new node's neighbor pool by
scanning **all** live nodes (`live_node_snapshot`) and materializing each
vector, so build cost was O(N²). The roadmap measured ~424 s for 50k×128d via
`benchmarks/vector_ann_sift.sh`, making SIFT-scale builds impractical.

## Change (commit `33ef498b`)

1. **Graph-traversal candidate selection** (`collect_construction_candidates`):
   once the live set is large, gather the new node's candidate pool by
   traversing the partially-built navigable graph (greedy descent + best-first
   expansion bounded by `ef_construction`) — the standard HNSW construction
   search — instead of an exhaustive scan. Gated on a measured work budget
   (`live_nodes × dims > 1_000_000`, crossover ≈ 8k nodes at 128d): below it the
   exhaustive scan is exact **and** faster, so small/medium indexes never
   regress.
2. **Zero-copy vector access** (`node_vector_view`): vectors up to ~2k dims live
   in a single overflow page, so distance probes read the `&[f32]` view directly
   instead of allocating a `Vec<f32>` per probe — removing the per-probe
   allocation from both the build traversal and the read-only search path.

The traversal is deterministic (BTreeMap node iteration + total-order
tie-breaks), so WAL replay / snapshot-resumed replay reconstruct an identical
graph (unit-tested). Recall holds: a unit test confirms recall@10 ≥ 0.95 at
ef ≤ 128 over a fully traversal-built graph.

## Measured page-backed `hnsw_build_time_us` (128d, m=16, ef_search=64)

| rows  | before (full-scan) | after (gated traversal) | speedup | build path |
|------:|-------------------:|------------------------:|--------:|-----------|
|  2000 |             0.49 s |                  0.50 s |   ~1.0× | full-scan |
|  5000 |             2.37 s |                  2.46 s |   ~1.0× | full-scan |
| 10000 |            18.68 s |                  8.14 s |    2.3× | traversal |
| 20000 |          ~80 s (¹) |                 21.93 s |   ~3.7× | traversal |

(¹) Extrapolated from the before-curve (2k/5k/10k fit ≈ O(N^2.1)); the 50k
roadmap point was ~424 s by the same curve.

The before-build is super-quadratic; the after-build is sub-quadratic with the
win growing as N grows, and **no regression below the ~8k crossover** (those
sizes still use the exhaustive scan). Extrapolating the after-curve puts 50k at
roughly 80 s (≈ 5–6× faster than the ~424 s baseline).

## Honest remaining gap

This is sub-quadratic but **not** yet pgvector-competitive at SIFT1M. The
residual constant factor is the page-backed arena's `BTreeMap` block lookups:
each traversal probe still chases the page store (and neighbor chains) per node,
a dims-independent fixed cost that the zero-copy view reduced but did not remove.
Closing the SIFT1M gap (build in minutes, committed SIFT1M artifact from the
server wire path) needs an in-memory, densely-indexed mirror of the graph
(vectors + adjacency in flat arrays, node_id-indexed) feeding both the build
traversal and search, so per-node access is O(1) instead of O(log N) pointer
chasing. That mirror is the documented next step in `ROADMAP.md`.
