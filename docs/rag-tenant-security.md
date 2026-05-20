# RAG Tenant Security

Status: schema and helper SQL contract. UltraSQL does not yet enforce
`CREATE POLICY`; row-level security execution remains a future SQL engine
slice.

## Scope

The RAG primitive tables are ordinary user tables. Security must stay visible:
tenant isolation is expressed through `tenant_id TEXT NOT NULL`, explicit
tenant predicates, and PostgreSQL-shaped row-policy SQL. No RAG helper hides
authorization in opaque functions.

The default table set is:

- `rag_documents`
- `rag_chunks`
- `rag_embeddings`
- `rag_retrieval_events`
- `rag_answer_citations`
- `rag_embedding_jobs`

## Tenant Id Pattern

Every RAG primitive row has a required tenant key:

```sql
tenant_id TEXT NOT NULL
```

Recommended tenant ids are short ASCII keys, at most 128 bytes, containing only
letters, digits, `_`, `-`, `.`, or `:`. Examples:

```text
org_123-prod
tenant:550e8400-e29b-41d4-a716-446655440000
```

Tenant ids are data, not SQL identifiers. Applications must bind them as query
parameters, never concatenate tenant strings into SQL.

## Safe Default Queries

Safe default RAG helpers put tenant id in the first parameter slot and keep the
predicate visible:

```sql
SELECT tenant_id, document_id, source_uri, title, version, updated_at
FROM rag_documents
WHERE tenant_id = $1 AND is_current = true AND metadata @> $2
ORDER BY updated_at DESC
LIMIT $3;
```

```sql
SELECT tenant_id, embedding_id, chunk_id, version, embedding <-> $2 AS distance
FROM rag_embeddings
WHERE tenant_id = $1 AND is_current = true
ORDER BY embedding <-> $2
LIMIT $3;
```

Chunk inserts also carry tenant id explicitly:

```sql
INSERT INTO rag_chunks (
    tenant_id,
    chunk_id,
    document_id,
    chunk_index,
    content,
    token_start,
    token_end,
    metadata,
    created_at,
    updated_at,
    version,
    is_current
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12);
```

Bulk document and chunk ingestion uses visible `COPY FROM STDIN` statements
with explicit column lists. The first CSV column remains `tenant_id`, so high
throughput loads keep the same tenant boundary as single-row inserts:

```sql
COPY rag_documents (
    tenant_id,
    document_id,
    source_uri,
    title,
    body_hash,
    metadata,
    created_at,
    updated_at,
    indexed_at,
    version,
    is_current
) FROM STDIN WITH (FORMAT CSV, HEADER true);
```

```sql
COPY rag_chunks (
    tenant_id,
    chunk_id,
    document_id,
    chunk_index,
    content,
    token_start,
    token_end,
    metadata,
    created_at,
    updated_at,
    version,
    is_current
) FROM STDIN WITH (FORMAT CSV, HEADER true);
```

Background embedding work is modeled as ordinary rows in
`rag_embedding_jobs`. Workers claim tenant-scoped pending jobs with visible
status, priority, lock, attempt, and availability columns; there is no hidden
scheduler or privileged helper.

These helpers are safe default building blocks, not a substitute for database
row-level security. Until UltraSQL enforces RLS, callers must use tenant-scoped
queries for every RAG read and write path.

## Row Policy Shape

Generated policy SQL follows PostgreSQL's visible RLS model:

```sql
ALTER TABLE rag_documents ENABLE ROW LEVEL SECURITY;

CREATE POLICY rag_documents_tenant_isolation ON rag_documents
USING (tenant_id = current_setting('ultrasql.tenant_id', true))
WITH CHECK (tenant_id = current_setting('ultrasql.tenant_id', true));
```

The same policy shape applies to `rag_chunks` and `rag_embeddings`.
The same policy shape also applies to `rag_retrieval_events` and
`rag_answer_citations`, and `rag_embedding_jobs`. `USING` gates reads and
deletes. `WITH CHECK` gates inserts and updates. The session setting is
intentionally named in SQL so application code can audit the security boundary.

## Current Enforcement Boundary

UltraSQL does not yet enforce `CREATE POLICY`, so generated policy SQL is a
contract and migration target. Current enforcement comes from:

- `tenant_id TEXT NOT NULL` in every RAG primitive table.
- Helper SQL requiring `tenant_id = $1` in read paths.
- Insert helper SQL requiring tenant id as the first value.
- Application/session code validating tenant ids before binding.

Do not claim database-enforced tenant isolation until `CREATE POLICY` and
`ALTER TABLE ... ENABLE ROW LEVEL SECURITY` execute and are tested through the
normal wire path.
