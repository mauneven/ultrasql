# Embedded RAG / agent-memory demo (Node.js)

A compact, zero-dependency Node script that shows the whole UltraSQL pitch for
building RAG and agent-memory apps:

> One embeddable, Postgres-compatible, **ACID** engine where **text + vectors +
> JSON metadata** live in one table and are **ranked together in one
> transaction** — and survive a process restart. No Pinecone + Elasticsearch +
> Redis to stitch together and keep in sync.

## Run it

```bash
./build.sh            # compiles the native addon → ultrasql_node.node
node rag-demo.cjs
```

`build.sh` runs `cargo build --release -p ultrasql-node` and copies the compiled
addon next to the demo. There is **no `npm install`** — the demo loads the
native addon directly with `require("./ultrasql_node.node")` and has no
JavaScript dependencies.

Requirements: Rust toolchain + Node.js (≥ 18). Built and verified on Node v22,
macOS arm64.

## What it does

```
Ingested 4 memories (text + vector + JSON) in one transaction.

Vector similarity ALONE (what a bolt-on vector store returns):
   #4  invoice payment failed urgent   (tenant=other — wrong tenant!)

UltraSQL — one query fusing vector + BM25 + tenant metadata:
   #2  invoice payment failed for customer
   #1  payment retry succeeded
   #3  user updated profile photo
   (doc 4's embedding is nearest, but the tenant filter excludes it)

After restart (fresh process, WAL-recovered) — same answer:
   #2  invoice payment failed for customer
   #1  payment retry succeeded
   #3  user updated profile photo

   4 memories survived the restart. ACID + durable, one binary.
```

Two things to notice:

- **The fusion reorders.** Doc 2 outranks the lower-id doc 1 because it is the
  better match on *both* the embedding and the BM25 text score — the result is
  ranked, not id-ordered.
- **Only the in-table filter stops the leak.** Doc 4 is the nearest embedding
  *and* wins the full vector+BM25 ranking — fusion alone does not save you. It is
  excluded purely because the same query filters on `metadata->>'tenant'`. There
  is no separate access-control layer to forget. One pass over one ACID table:
  the cross-tenant leak never happens and there is no second system to keep in
  sync.

The script:

1. Opens a **WAL-backed** embedded database on disk (`new Database(dir)`).
2. Ingests four memories — each a `(body TEXT, embedding VECTOR(3), metadata
   JSONB)` row — inside one `BEGIN … COMMIT` transaction.
3. Retrieves with **one SQL statement** that filters by tenant metadata and
   ranks by `hybrid_search(body, query, embedding, probe, 'rrf')` — Reciprocal
   Rank Fusion of BM25 lexical relevance and dense-vector similarity over the
   same MVCC table. Doc 4 is a strong lexical match but belongs to another
   tenant, so the metadata filter excludes it — no separate access-control layer.
4. **Restarts for real**: a brand-new `node` process (`--reopen`) opens the same
   directory, replays the WAL, and runs the identical query — same answer, and a
   `COUNT(*)` proves the committed rows are durable.

## Bring your own vectors

The query embedding (`VECTOR '[1,0,0]'`) is whatever your model produced.
UltraSQL **stores, indexes, and ranks** vectors; it does not generate them —
there is no bundled embedding model and no hidden network call. See
[../../docs/transactional-embeddings.md](../../docs/transactional-embeddings.md).

## How the binding works

`crates/ultrasql-node` is a [Node-API](https://nodejs.org/api/n-api.html) (napi)
addon exposing a `Database` class with `new(target)` (a data-directory path, or
`:memory:`) and `execute(sql)`, which returns `{ columns, rows, commandTag }`.
Statements run through the same parser, planner, executor, transaction manager,
and WAL as a TCP client — embedded mode is the full engine, not a subset.
