# IVFFlat probes-based ANN — filtered routing + recall artifact

Date: 2026-06-19. Host: Apple M4 (`Darwin arm64`), rustc 1.95.0, release build.
Reproduce: `cargo run --release -p ultrasql-bench --bin vector_recall_sweep -- ivfflat <rows> <dims> <lists> <queries> <probes-list>`
(page-backed `PageBackedIvfFlatIndex`, recall@10 vs brute-force ground truth on
deterministic random vectors).

## Change

- **Per-query probes override** (`PageBackedIvfFlatIndex::search_with_probes`):
  the IVFFlat analog of HNSW's `search_with_ef`. A query can probe more inverted
  lists than the index default to trade latency for recall; probing every list
  is exact.
- **Filtered ANN routing**: `WHERE <metadata> ORDER BY <vector> LIMIT k` against
  an IVFFlat-indexed column now routes through the probes-based ANN path
  (`try_hnsw_filtered_sorted` in `crates/ultrasql-server/src/pipeline/index_scan.rs`),
  scaling probes inversely with the estimated filter selectivity (capped at the
  list count) and then rechecking the predicate exactly on the candidates, with
  an exact-scan fallback if too few survive. Previously only HNSW had a filtered
  ANN path; IVFFlat-indexed filtered queries fell back to a full exact scan.
- **EXPLAIN accuracy**: the `EXPLAIN ANALYZE` vector-index note for a *filtered*
  top-k now reports the ANN index actually serving the query
  (`selected … (page-backed ivfflat); method=ivfflat filter=exact-recheck …`)
  instead of the stale `method=exact fallback_used=true`, which had been wrong
  for HNSW filtered queries too (they already used the ANN path).

## Recall@10 vs probes (50k × 64d, lists=256, 200 queries)

| probes | recall@10 |
|-------:|----------:|
|      1 |    0.0590 |
|      4 |    0.1640 |
|      8 |    0.2570 |
|     16 |    0.3860 |
|     32 |    0.5615 |
|     64 |    0.7445 |
|    128 |    0.9210 |
|    256 |    1.0000 |

Recall climbs monotonically with probes and reaches **1.0 at `probes == lists`**
(probing every list is exact), confirming the over-fetch is correct: a query can
dial in any recall/latency point by probing more lists. Absolute recall at low
probes is modest because 64-dim uniform-random data has no cluster structure
(worst case for IVFFlat partitioning); on structured data the curve is steeper.

## Verification

`ivfflat_filtered_ann_uses_index_and_returns_correct_top_k` asserts a filtered
IVFFlat query returns the exact top-k among the filtered rows and that EXPLAIN
reports the IVFFlat index served it. The 42-test server vector suite and the 10
storage IVFFlat unit tests stay green.

## Remaining

Same-host competitor recall/latency (pgvector IVFFlat) at scale remains release-
host evidence; not runnable here without that service.
