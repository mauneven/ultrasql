# Engineering report — June 2026

Two missions ran back to back: (A) benchmark integrity and engine hardening,
and (B) the first slice of the AI-native operational-database wedge. This
report records, per part, what shipped, the measured numbers, which acceptance
gates passed, and what honestly remains (with measurable exit conditions in
[ROADMAP.md](../ROADMAP.md)). No number here was hand-edited; every figure comes
from running committed code on an Apple M4 host.

## Mission A — Benchmark integrity & hardening

### A1. Benchmark fairness & honest reconciliation — shipped

The README scale-sweep table claimed UltraSQL beat PostgreSQL by up to
~105,000 % and beat every engine on every row. That was an artifact of an unfair
harness: competitors were measured by **spawning a fresh client process per
query** (PostgreSQL via `psql -c`, one process *per op* in mixed OLTP), against
an old PostgreSQL 14, while UltraSQL used a persistent connection.

Fixes (each a small committed slice):
- **Persistent connections for every engine.** PostgreSQL now runs over one
  `psycopg` connection with server-side prepared statements
  (`benchmarks/scripts/run_postgres_writes.py`); DuckDB/SQLite `mixed_oltp` hold
  one in-process connection instead of spawning per op-batch; ClickHouse was
  already persistent. TPC-H DuckDB timing uses one long-lived session
  (`DuckDbSession`) instead of a process per query.
- **Tuned PostgreSQL 17.** `benchmarks/scripts/pg17_bench_server.sh` brings up a
  same-host PG17 cluster with documented OLTP/analytics tuning.
- **Data-dir (WAL-backed) sweep re-run** with the full validation envelope, so
  the ~190 schema/envelope errors in `benchmark_certification_status.json` are
  gone and `ultrasql_storage_mode = data-dir`.

Honest result (2026-06-16 same-host data-dir sweep): **UltraSQL is fastest on
17 of 23 comparable rows** — every aggregate (SUM/AVG/Filter+SUM, ~2× over
DuckDB, not 40,000 %), the windowed scan, large sequential scans, and
small-batch updates/deletes. It is **not** fastest on:

| Row | Winner | UltraSQL vs winner |
|---|---|---|
| Mixed OLTP (point ops) | PostgreSQL 17 (31 µs/op) | ~15× slower |
| INSERT 10k (single-shot) | PostgreSQL 17 (7.6 ms) | ~1.3× slower |
| UPDATE 1M | DuckDB (2.09 ms) | ~3.1× slower |
| DELETE 100k | DuckDB (417 µs) | ~1.3× slower |
| SELECT scan 100k | ClickHouse (6.20 ms) | ~1.03× slower |
| DELETE 1M | ClickHouse (3.31 ms) | ~1.3× slower |
| INSERT 1M (durable) | — | **fails** (see A2) |

Gates: schema/envelope validation passes; `ultrasql_storage_mode = data-dir`;
README regenerated from the committed renderer and bolds the true fastest engine
per row; the stale "PostgreSQL 14.22" string is gone; BENCHMARKS.md has a
Methodology & Fairness note. The certification is honestly `not_ready` because
UltraSQL is not fastest on every row — reality, not broken artifacts.

### A2. OLTP weakness — documented, not faked

Profiling surfaced a real durable-write bug: a **1,000,000-row INSERT fails in
`--data-dir` mode** (`wal buffer full: 8383400 of 8388608`). The 8 MiB WAL
buffer (`crates/ultrasql-server/src/lib.rs` `WAL_BUFFER_BYTES`) *rejects* records
when full instead of applying backpressure. The README's "INSERT 1M = 337 ms"
was a volatile-memory number; durable mode errors. This and the fair-measurement
OLTP losses are in ROADMAP P0 with exit conditions. No OLTP leadership is
claimed. (This closed the integrity-mission Part 2 honestly; deep TPC-C
group-commit tuning remains open.)

### A4. Build-break regression guard — shipped

A new `ultrasql-wal` `RecordType` variant once broke the CLI WAL decoder with no
test catching it. Added `RecordType::ALL` (kept complete by a const
compile-time exhaustiveness guard) plus tests that round-trip every variant
through `encode→decode` and assert the CLI `decode_wal_payload` routes each
variant to a typed decoder. A new variant now fails compilation or the tests.

(Integrity-mission Part 3, executor/server panic-hardening, was not undertaken
and is noted in the task ledger; it remains available as scoped future work.)

## Mission B — AI-native operational database

### B1. First-class hybrid search — shipped

A single SQL statement now fuses vector similarity + BM25 + SQL/JSON metadata
filters into one ranked top-k:

```sql
SELECT id, body FROM memories
WHERE metadata @> '{"tenant":"acme"}'
ORDER BY hybrid_search(body, 'failed invoice payment', embedding,
                       VECTOR '[...]', 'rrf') DESC
LIMIT 10;
```

- Added **Reciprocal Rank Fusion** (`FusionMethod::Rrf { k }`, default k=60) to
  the hybrid ranker alongside weighted-linear, with a test checking the
  operator's RRF ordering against an independent reference implementation.
- Exposed fusion selection as an optional 5th `hybrid_search` argument
  (`'rrf'` / `'weighted'`), backward compatible. An integration test shows RRF
  reranking away from weighted-linear on data where they disagree
  (weighted → `[1,2,3]`, RRF → `[2,1,3]`).
- `docs/hybrid-search.md` is the worked hybrid-RAG example.

Gates passed: single fused query over vector + BM25 + metadata; ranking matches
a reference fusion implementation; docs example committed. The
selectivity-based vector-index-vs-scan decision (Part 1b) is the Part 2 item
below.

### B7. Positioning & docs — shipped

The README now leads with the AI-native wedge (embeddable, Postgres-compatible,
ACID engine unifying SQL + JSON + full-text + vectors) and the transactional-
consistency moat, while keeping the honest same-host scoreboard and pointing at
ROADMAP for what is open. Every README claim maps to a shipped, tested feature
or an explicitly-open ROADMAP item.

### B2–B6 — scoped with measured baselines (open)

Grounded baseline: unfiltered HNSW **recall@10 = 0.998 at p50 ≈ 257 µs**
(2k×16d, `benchmarks/vector_ann_hnsw.sh`). Filtered vector queries currently
fall back to exact brute force (recall 1.0, no ANN speedup) because the ANN
matcher does not yet recognize `Sort(Filter(Scan))`. The HNSW/IVFFlat index is
rebuilt on DML rather than reflecting committed MVCC online. Exit conditions for
filtered ANN (B2), online index MVCC + recovery (B3), agent-memory primitives
(B4), retrieval observability (B5), and the demo + competitive benchmarks vs
pgvector/LanceDB/Qdrant (B6) are recorded in ROADMAP P2 "AI-Native Retrieval".

## The bottom line

The README's published claims, the certification harness, and reality now
agree. UltraSQL is honestly positioned as the engine that unifies SQL + JSON +
full-text + vectors for RAG and agent memory in one ACID transaction — with the
hybrid-retrieval capability shipped and tested, and the harder ANN/MVCC/agent
work scoped with measurable exit conditions rather than overstated.
