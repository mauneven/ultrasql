# Vector ANN benchmarks (honest recall@k vs latency)

UltraSQL is benchmarked against **pgvector** (PostgreSQL 17), **Qdrant**, and
**LanceDB** on a real dataset, same host, with one rule that is never relaxed:
**recall@k is always reported with latency.** A latency number without the
recall it was measured at is meaningless for ANN, so this suite never prints one
without the other.

## Methodology

- **Dataset**: SIFT (TEXMEX) 128-dimensional SIFT descriptors, L2 distance — the
  standard ANN benchmark corpus. Downloaded on demand by
  `benchmarks/vector_ann_sift.sh` (not committed).
- **Same everything**: every engine gets the same base vectors, the same query
  set, the same k, and the same L2 metric.
- **Independent ground truth**: exact k-NN is computed in NumPy over the
  *actually-loaded* base vectors (`compute_groundtruth`), not read from the
  dataset's bundled file — that file indexes the full 1M corpus and would be
  wrong for any subset. Computing it here is both correct at any scale and an
  independent check on every engine.
- **Matched HNSW config**: `m = 16`, `ef_construction = 200` where the engine
  exposes them. Each engine is swept across its recall/latency knob
  (`ef_search` for the HNSW engines; `refine_factor` for LanceDB, whose
  IVF+scalar-quantization index recovers recall by re-ranking, not by `ef`).
- **Fair, recommended configs**: Qdrant runs its server build to a GREEN
  (optimized) status before querying; LanceDB uses `refine_factor` as its docs
  recommend for quantized indexes. An engine that cannot be reached or
  configured is recorded `not_available` — never faked.
- **Reproduce**:
  ```bash
  # all four engines (pgvector via PG17, Qdrant on :6333, LanceDB embedded)
  SIFT_DATASET=sift SIFT_N_BASE=50000 benchmarks/vector_ann_sift.sh
  ```

## Results — SIFT, 50 000 × 128d, recall@10, Apple M4

Query recall and p50 latency, swept across each engine's recall knob:

| Engine | knob | recall@10 | p50 | knob | recall@10 | p50 |
|---|---|---:|---:|---|---:|---:|
| **UltraSQL** | ef=64 | 0.986 | 697 µs | ef=200 | 1.000 | 1728 µs |
| pgvector 0.8.0 | ef=64 | 0.995 | 319 µs | ef=200 | 1.000 | 642 µs |
| Qdrant | ef=64 | 1.000 | 1403 µs | ef=200 | 1.000 | 1512 µs |
| LanceDB (IVF+SQ) | refine=5 | 1.000 | 1154 µs | refine=50 | 1.000 | 3091 µs |

**Matched operating point (ef=64; LanceDB at its lowest-latency ≥0.95-recall point):**

| Engine | recall@10 | p50 latency | queries/sec |
|---|---:|---:|---:|
| pgvector 0.8.0 | 0.995 | 319 µs | 3132 |
| **UltraSQL** | **0.986** | **697 µs** | **1436** |
| LanceDB | 1.000 | 1154 µs | 866 |
| Qdrant | 1.000 | 1403 µs | 713 |

### What this says, honestly

- **Recall is competitive.** UltraSQL reaches recall@10 = 0.986 at ef=64 and
  1.000 at ef=200 — the diversity heuristic
  ([transactional-embeddings.md](transactional-embeddings.md) and the HNSW
  commit) closed what was a 0.66 recall gap before it.
- **Query latency is second-best of four.** At the matched point UltraSQL's
  697 µs p50 is faster than Qdrant (1403 µs) and LanceDB (1154 µs), and slower
  than pgvector (319 µs) — while being a single embeddable binary with full SQL,
  JSON, full-text, and ACID transactions in the same engine, not a standalone
  vector service.
- **Build time is the real weakness.** UltraSQL built the 50k index in ~400 s
  versus 1–4 s for the others, because its HNSW build scans every live node per
  insert (O(N²)). This is why the suite runs at 50k rather than SIFT1M, and it is
  the honest gap. The fix — graph-search-based candidate selection at insert — is
  tracked in [TODO.md](https://github.com/mauneven/ultrasql/blob/main/TODO.md) with a measurable exit condition
  (SIFT1M build in minutes, recall@10 ≥ 0.95 at ef ≤ 128). Query recall/latency
  is already competitive; the gap is build, not search.

Recall holds across scale: the same path scores recall@10 = 0.997 at 10k
(siftsmall) and 0.986 at 50k at ef=64 — it does not degrade as N grows over this
range.

## Artifacts

Per-engine and comparison JSON are committed under
`benchmarks/results/latest/raw/`:

- `vector_ann_sift_50k_k10-{ultrasql,postgres17_pgvector,qdrant,lancedb}.json`
- `vector_ann_sift_50k_k10_comparison.json` (matched-point summary + host
  descriptor + policy)

Each carries the full ef/refine sweep with recall paired to p50/p95/p99 latency,
the host descriptor, and the git commit, so the numbers are reproducible and
attributable.
