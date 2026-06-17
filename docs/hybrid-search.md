# Hybrid search (vector + BM25 + metadata) in one SQL query

UltraSQL fuses dense-vector similarity, BM25 lexical relevance, and arbitrary
SQL/JSON metadata filters into a single ranked result — no second system, no
application-side merge. The retrieval runs as one statement inside one
transaction over the same MVCC table that holds your source rows.

## The table

```sql
CREATE TABLE memories (
    id        INT  NOT NULL,
    body      TEXT,            -- source text (lexical / BM25 input)
    embedding VECTOR(1536),    -- dense embedding (ANN / exact vector input)
    metadata  JSONB           -- tenant, kind, timestamps, anything
);
```

## The hybrid query

A single statement filters by metadata, ranks by fused lexical + vector score,
and returns the top `k`:

```sql
SELECT id, body
FROM memories
WHERE metadata @> '{"tenant":"acme"}'          -- SQL/JSON metadata filter
ORDER BY hybrid_search(
            body,                                -- text column (BM25)
            'failed invoice payment',            -- query text
            embedding,                           -- vector column
            VECTOR '[0.12, -0.04, ...]',         -- query embedding
            'rrf'                                -- fusion method
         ) DESC
LIMIT 10;
```

The `WHERE` clause is applied before scoring, so only matching rows are ranked.
`hybrid_search(...)` is recognized by the planner when used as
`ORDER BY hybrid_search(...) DESC LIMIT k` and lowered to the hybrid ranking
operator.

## Fusion methods

`hybrid_search(text_col, query, vector_col, probe[, fusion])`:

- **`'rrf'`** — Reciprocal Rank Fusion (recommended). Each component
  independently ranks the candidates (best = rank 1); the fused score is
  `Σ weight / (k + rank)` with `k = 60`. RRF is robust to incomparable score
  scales (BM25 magnitude vs cosine similarity), so a strong single-modality
  outlier cannot dominate the ranking.
- **`'weighted'`** — weighted linear sum of normalized component scores. This
  is the default when the fifth argument is omitted, for backward
  compatibility.

The two methods can disagree. Given a document with a very strong lexical match
but a distant vector, and another with a weaker lexical match but the closest
vector, weighted-linear is pulled toward the large BM25 magnitude while RRF
keeps the rank-balanced document on top. See
`crates/ultrasql-server/tests/vector_type_round_trip.rs`
(`hybrid_search_rrf_fusion_reranks_versus_weighted`) for the exact,
reproducible ordering.

## Why this is the moat

Because `body`, `embedding`, and `metadata` are columns of one ACID table,
updating the text, its embedding, and its metadata is a single transaction.
The retrieval reads a consistent MVCC snapshot — the lexical, vector, and
metadata views can never drift out of sync the way a bolt-on vector store plus
a separate search index plus a separate SQL database can.
