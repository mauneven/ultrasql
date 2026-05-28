# AI Database Strategy

UltraSQL targets AI database workloads by combining three surfaces in one
engine:

- SQL server mode for application state and existing drivers.
- Embedded analytics for files, Parquet, Arrow, CSV, and Node/Bun/Python use.
- Real-time analytics for high-ingest events, projections, sparse pruning,
  JSON, full-text, and vector workloads.

This document is a product and engineering map. Implementation is original
Rust code. External engines are used only as public benchmark references
through documented interfaces and license-reviewed test data.

## Feature Map

### Embedded Analytics

- In-process database with a tiny install footprint and language-native APIs.
- Direct file/lake queries: `read_parquet`, CSV/JSON scans, S3/object-store
  support, globbing, schema inference, and views over files.
- Parquet projection and predicate pushdown, row-group pruning, metadata
  inspection, and Parquet export.
- Vectorized execution with compact vector formats such as flat, constant, and
  dictionary vectors.
- Extension-like feature boundary for optional formats, functions, and remote
  storage integrations.
- Spilling and external execution so analytical queries can exceed memory.

### Real-Time Analytics

- Columnar storage for analytical scans, compression, and high-ingest event
  tables.
- Sparse primary indexes over ordered granules, skip indexes, and zone maps for
  fast pruning at large scale.
- Projections and materialized views maintained by the engine for repeated
  query shapes.
- High-throughput batch insert path that does not block readers.
- JSON analytics without schema explosion.
- Full-text search through native inverted indexes.
- Vector search with exact scan, HNSW ANN, and query-time precision/latency
  controls.
- ClickBench, CSV, Parquet/object-store, vector, and AI gauntlet artifacts as
  first-class same-host benchmark evidence.

## UltraSQL Differentiator: AI Memory Engine

The differentiator is one table surface for state, retrieval, telemetry, and
analytics:

```sql
CREATE AI TABLE memories (
    id uuid PRIMARY KEY,
    tenant_id text NOT NULL,
    session_id text,
    body text NOT NULL,
    embedding vector(1536),
    metadata jsonb,
    created_at timestamptz DEFAULT now()
) WITH (
    text_index = 'bm25',
    vector_index = 'hnsw',
    hybrid_rank = 'rrf',
    projection = '(tenant_id, created_at)',
    recall_target = 0.95
);
```

The engine expands that declaration into:

- A row-store source of truth with MVCC and WAL.
- A columnar shadow for analytics and scans.
- A tenant/time projection for pruning.
- A BM25 inverted index over `body`.
- An HNSW or IVFFlat vector index over `embedding`.
- Optional quantized vector blocks for query-time precision control.

The query shape should be boring SQL:

```sql
SELECT id, body, score
FROM ai_search(
    table => 'memories',
    tenant_id => 'acme',
    query_text => 'failed invoice payment',
    query_embedding => $1,
    top_k => 20,
    recall_target => 0.95,
    max_latency_ms => 25
)
ORDER BY score DESC;
```

`EXPLAIN ANALYZE` must say which path ran: BM25, exact vector, HNSW, filtered
HNSW, columnar scan, projection pruning, or hybrid merge. This keeps benchmark
claims auditable and gives operators enough information to tune production.

## Benchmark Gauntlet

Every AI claim needs raw artifacts:

- Exact top-k across installed reference engines and vector extensions when
  available.
- HNSW recall/latency across rows, dimensions, filters, and update/restart
  cycles.
- Hybrid BM25 + vector search with deterministic answer checksums.
- RAG retrieval quality: recall@k, precision@k, MRR, and citation coverage.
- Ingestion throughput with and without vector/text indexes.
- Memory/index bytes per million vectors.
- Cold-start index load after restart.
- Model telemetry analytics: token usage, latency, tool calls, errors, and
  prompt/version dimensions.

Missing reference engines are allowed only as explicit `not_available`
artifacts with reason fields. No README, release note, or package metadata may
rank a missing engine.

## Implementation Slices

1. Benchmark truth.
   Keep ClickHouse wired into release scale sweep, ClickBench, CSV, Parquet,
   vector, and AI gauntlet runners. Tables render missing engines instead of
   hiding them.

2. Lakehouse certification.
   Certify Parquet/CSV pushdown, object-range reads, file metadata functions,
   and `COPY TO ... FORMAT parquet` against license-reviewed reference
   behavior.

3. Columnar shadow.
   Make OLTP heap remain source of truth while committed rows feed a compressed
   columnar path with DML invalidation, projections, and sparse pruning.

4. Hybrid search.
   Add SQL-visible BM25 + vector hybrid ranking with tenant filters, answer
   checksums, and `EXPLAIN ANALYZE` path evidence.

5. Query-time precision.
   Add quantized vector blocks and query knobs such as `recall_target` and
   `max_latency_ms` so users can trade speed and recall without rebuilding
   every index.

6. AI telemetry.
   Ship a schema and benchmark for traces, prompts, token usage, retrieval
   hits, evaluations, and model/version dimensions. This targets operational AI
   data with fast ingestion and auditable query paths.

## Source References

- DuckDB: https://duckdb.org/
- DuckDB vector execution format: https://duckdb.org/docs/lts/internals/vector
- DuckDB Parquet: https://duckdb.org/docs/lts/data/parquet/overview
- DuckDB extensions: https://duckdb.org/docs/current/extensions/overview
- ClickHouse platform features: https://clickhouse.com/clickhouse
- ClickHouse query-time vector precision: https://clickhouse.com/blog/qbit-vector-search
