# Transactional embedding consistency

A vector store that lives next to your database — Pinecone + Redis + Postgres
stitched together — has no transaction spanning the three. The text, its
embedding, and its metadata commit (or fail) independently, so a crash or a
half-applied update can leave the index pointing at a row that no longer says
what the index thinks it says. UltraSQL puts the text, the vector, and the
metadata in **one table, under one transaction, behind one WAL** — so they are
always consistent with each other, including across a crash.

## One transaction for text + vector + metadata

```sql
BEGIN;
UPDATE memories
   SET body      = 'gamma-v2',
       embedding = '[0.1, 0, 0]',
       metadata  = '{"v": 2}'
 WHERE id = 3;
COMMIT;
```

Until `COMMIT`, no reader and no index sees any of the three changes; after
`COMMIT`, every reader and the vector index see all three. The vector search
re-ranks its candidates against the committed **heap** tuple (it recomputes the
distance from the row it is about to return), so the index can never serve a
vector that disagrees with the row's current text or metadata.

The index reflects **committed MVCC state only**:

- A **rolled-back** embedding update leaves no trace — the search behaves as if
  it never happened, even though the index briefly held the aborted vector.
- The unfiltered top-k path **over-fetches** candidates and rechecks visibility,
  so an aborted or update-superseded tuple sitting nearer the probe than a live
  row cannot occupy a result slot and starve the answer below `k` live rows.
  When too few survive, it falls back to the exact sort path (recall 1.0).

Tested in `crates/ultrasql-server/tests/vector_type_round_trip.rs`:
`rolled_back_embedding_update_does_not_affect_vector_search`.

## Crash recovery: index and heap agree after WAL replay

The HNSW index is page-backed and WAL-logged like the heap. After a crash, WAL
replay restores both, and the index agrees with the heap it indexes — a
committed `UPDATE` of text + vector + metadata is reflected in the vector
search, the body, and the JSON metadata after restart; an uncommitted one is
not.

Tested in the same file:
`vector_index_and_heap_agree_after_transactional_update_and_crash` — it commits a
one-transaction text+vector+metadata update, **aborts the server process**
(crash), restarts from the data directory, replays the WAL, and asserts the
nearest-neighbor order, the body, and `metadata->>'v'` all reflect the committed
update.

## Re-embedding and embedding versioning

When you change embedding models, you re-embed the corpus — and during that
migration both generations must stay queryable so readers are never served a
mix of old-model and new-model vectors. Track the generation with a column:

```sql
CREATE TABLE corpus (
    doc_id        INT NOT NULL,
    model_version INT NOT NULL,        -- which model produced `embedding`
    embedding     VECTOR(768),
    body          TEXT
);

-- Re-embed with the new model in one transaction; old rows untouched.
BEGIN;
INSERT INTO corpus
SELECT doc_id, 2 AS model_version, /* new vector */, body
  FROM corpus WHERE model_version = 1;
COMMIT;

-- Readers pin a generation with an ordinary metadata filter (filtered ANN):
SELECT doc_id, body
  FROM corpus
 WHERE model_version = 2
 ORDER BY embedding <-> VECTOR '[...]'
 LIMIT 10;
```

Both generations coexist and are searched independently — each pins its own
vectors through the metadata filter (the same filtered-ANN path documented in
[filtered-ann.md](filtered-ann.md)). Once generation 2 is verified, drop
generation 1 in a transaction. Tested in the same file:
`embedding_generations_coexist_during_re_embedding_migration`.

## Bring your own vectors — no bundled model, no hidden network

UltraSQL **stores, indexes, and ranks** vectors; it does not **produce** them.
You compute embeddings with whatever model you choose and hand UltraSQL the
result as a `VECTOR` literal (or via `COPY`, or a bound parameter):

```sql
INSERT INTO memories (id, body, embedding)
VALUES (1, 'hello', '[0.12, -0.03, ...]');   -- you embedded "hello" upstream
```

This is a deliberate design choice with two honest guarantees:

- **No embedding model is bundled.** There is no ONNX/Candle/tokenizer/Torch
  dependency anywhere in the workspace, and no model is downloaded at build or
  runtime. The binary stays single-file and small.
- **No hidden network call produces a vector.** A vector only ever enters the
  engine as caller-supplied bytes parsed by `Value::parse_vector`. The engine
  makes no outbound request to embed anything. (The only network client in the
  tree, `ureq` in `ultrasql-objectstore`, serves explicit, user-configured
  object-store reads for external scans — never embeddings.)

You keep full control of the model, its version, batching, and cost; UltraSQL
gives you the ACID, crash-safe place to put the results and the SQL to rank them
alongside your text, JSON, and full-text data in one transaction.
