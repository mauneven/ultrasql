# Filtered ANN (metadata-filtered vector search)

A vector search with a `WHERE` filter — "the nearest embeddings *that also match
this metadata*" — is where naive vector stores fall apart. "Filter then exact"
is slow on large tables; "ANN then filter" collapses recall when the filter is
selective (most ANN candidates get filtered out, leaving fewer than `k`).
UltraSQL handles this with a selectivity-aware crossover, so recall does not
fall off a cliff at any selectivity.

## It is just SQL

```sql
SELECT id, body
FROM memories
WHERE tenant_id = 7 AND kind = 'invoice'      -- metadata filter
ORDER BY embedding <-> VECTOR '[...]'          -- nearest-neighbor order
LIMIT 10;
```

The planner recognizes `WHERE … ORDER BY vector_distance LIMIT k` over a table
with an HNSW index and routes it through the filtered-ANN path
(`crates/ultrasql-server/src/pipeline/index_scan.rs`,
`try_hnsw_filtered_top_k_limit`).

## The strategies and the crossover

The persistent HNSW index supports a per-query `ef_search` (exploration budget).
The filtered path uses it to size an over-fetch to the estimated filter
selectivity `s`:

- **Loose filter** (most rows pass): explore roughly `k / s` candidates with the
  HNSW graph, apply the predicate to the candidates, and return the top `k`.
  This is fast — it never scans the whole table.
- **Very selective filter**, or a case where too few ANN candidates survive the
  filter: fall back to the **exact** filter-then-sort path (recall 1.0). The
  fallback is what guarantees recall cannot collapse — when the ANN over-fetch
  cannot confidently deliver `k` survivors, the exact path does.

The selectivity estimate only *sizes* the over-fetch; correctness comes from the
post-filter and the exact fallback, so an inaccurate estimate costs latency, not
recall.

## Measured recall vs latency (no cliff)

`benchmarks/filtered_ann_recall.sh` measures recall@10 against an independent
NumPy brute-force baseline across filter selectivities, end-to-end over the wire,
reporting recall and latency together
(`benchmarks/results/latest/raw/filtered_ann_recall-ultrasql.json`). On an Apple
M4 host, 20 000 rows × 16 dims, HNSW:

| Filter selectivity | Matching rows | recall@10 | p50 latency | Path |
|---|---:|---:|---:|---|
| 0.1 % | 16 | 1.000 | 24.0 ms | exact fallback |
| 1 % | 215 | 1.000 | 24.1 ms | exact fallback |
| 10 % | 2 073 | 0.962 | 0.86 ms | ANN over-fetch |
| 100 % | 20 000 | 0.972 | 0.83 ms | ANN over-fetch |

Recall floor 0.962 across every selectivity — no cliff. Loose filters take the
ANN path at ~28× lower latency than the exact scan; selective filters fall back
to exact and stay at recall 1.0. (A persistent metadata index would also make
the selective-filter latency fast; that remains a future optimization — see
TODO.md.)

## Scope

Implemented for HNSW (the per-query `ef_search` knob). IVFFlat-indexed or
unindexed vector columns use the exact filter+sort path. Reproduce with:

```bash
ULTRASQLD_BIN=target/release-ship/ultrasqld benchmarks/filtered_ann_recall.sh
```
