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
sizes still use the exhaustive scan).

## Update — in-memory graph mirror (the documented next step, now landed)

The gated traversal was still bottlenecked by the page-backed arena's `BTreeMap`
block lookups (a dims-independent fixed cost per probe). Adding a `node_id`-
indexed in-memory mirror of the graph (vectors + adjacency in flat arrays,
rebuilt from pages on load, kept in lockstep on mutation; pages stay
authoritative and snapshots are unchanged) makes per-node access O(1). This
speeds up the full-scan branch too (its candidate scan now reads the mirror, not
page chains), so even small indexes get faster:

| rows  | before (full-scan) | with mirror | speedup vs before |
|------:|-------------------:|------------:|------------------:|
|  2000 |             0.49 s |      0.38 s |             ~1.3× |
|  5000 |             2.37 s |      1.57 s |             ~1.5× |
| 10000 |            18.68 s |      5.25 s |              3.6× |
| 20000 |          ~80 s (¹) |     13.58 s |             ~5.9× |
| 50000 |           ~424 s   |     41.17 s |             ~10×  |

The mirror build is ~O(N^1.4) (50k = 41 s). This makes SIFT1M-scale builds
**feasible** (~45 min extrapolated, versus effectively impractical before).

## Honest remaining gap

The build is feasible at 1M but **not yet pgvector-competitive** ("minutes" at
1M). The residual cost is the single-layer HNSW traversal: per-insert
construction-search cost still grows with N because there is no upper navigation
layer to shorten the descent. Closing that gap is the separate **hierarchical
HNSW layers** roadmap item (per-node levels + per-level neighbor lists), which
would cut the ef needed for a given recall and the traversal depth at large N.
A committed SIFT1M artifact from the server wire path is still required before
any 1M-scale build claim.
