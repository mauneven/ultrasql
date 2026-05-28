# HNSW Index Design

Status: design only. No HNSW code is implemented by this document.

## Scope

This document specifies the storage, WAL, MVCC, delete, vacuum, and rebuild
contracts for a future `CREATE INDEX ... USING hnsw` vector access method.
It is the required design checkpoint before adding code.

The first implementation targets `VECTOR(n)` columns and pgvector-compatible
distance operators:

- L2: `<->`
- cosine: `<=>`
- negative inner product: `<#>`

HNSW is an approximate nearest-neighbor index. Exact top-k remains the
correctness oracle and benchmark baseline. Queries must be able to fall back
to exact `ORDER BY distance LIMIT k` when the planner cannot prove that HNSW
is valid for the operator shape.

## Goals

- Persistent, crash-recoverable HNSW graph stored in the storage layer.
- SQL DDL:

```sql
CREATE INDEX items_embedding_hnsw
    ON items USING hnsw (embedding vector_l2_ops)
    WITH (m = 16, ef_construction = 64);
```

- MVCC-correct query results under UltraSQL snapshots.
- Online INSERT/UPDATE/DELETE maintenance after build.
- Tombstone cleanup through VACUUM without corrupting graph reachability.
- Deterministic rebuild path for corruption repair, parameter changes, and
  recall recovery.

## Non-Goals

- No claim of exact results from HNSW.
- No SIMD-specific graph algorithm in the first storage implementation.
  Distance kernels may call `ultrasql-vec`, but graph correctness cannot
  depend on target features.
- No distributed or sharded HNSW graph.
- No proprietary test import.

## Architecture

The HNSW access method lives in `ultrasql-storage` beside B-tree, hash, BRIN,
GIN, and GiST. The planner and server treat it as an index method selected
only for vector top-k shapes:

```sql
SELECT id, embedding <-> $probe AS distance
FROM items
ORDER BY embedding <-> $probe
LIMIT $k;
```

The access method returns heap TIDs plus approximate distances. The executor
must heap-fetch each candidate and apply the statement snapshot before
emitting a row. The index never decides row visibility by itself.

## Graph Layout

Each HNSW index relation has these page classes:

- meta page: one page at block 0.
- node pages: fixed-size node headers plus variable neighbor lists.
- overflow pages: neighbor-list spill storage for high-degree or future
  format expansion.
- free-list pages: reusable slots left by vacuum compaction.

### Meta Page

The meta page stores:

```text
magic:               u32
format_version:      u16
flags:               u16
indexed_table_oid:   u32
indexed_column:      u16
metric:              u8      // l2, cosine, negative_inner_product
dims:                u32
m:                   u16
ef_construction:     u16
entry_node:          NodeId
entry_level:         u16
node_count:          u64
live_node_estimate:  u64
tombstone_count:     u64
build_epoch:         u64
last_rebuild_lsn:    Lsn
```

`entry_node` is structural. It may point to a heap tuple that is invisible or
deleted to a reader. Searches can still route through it; result materializing
must filter through heap visibility.

### Node Identity

`NodeId` is a stable logical index address:

```text
NodeId {
    block: u32,
    slot:  u16,
    gen:   u16,
}
```

The generation field prevents stale edges from reaching a reused slot after
vacuum. A neighbor reference is valid only when `(block, slot, gen)` matches
the target node header.

### Node Record

Each node record stores:

```text
state:        u8       // live, tombstone, free
level:        u16
generation:   u16
heap_tid:     TupleId
vector_hash:  u64
vector_len:   u32
vector_bytes: [f32; dims]
neighbors:    per-level adjacency offsets
```

The index stores a copy of the vector so graph search does not need heap
access for every explored node. The heap tuple remains authoritative for
visibility and current row value. `vector_hash` is used during VACUUM and
rebuild validation to detect stale entries after updates.

### Adjacency Lists

Each level owns an ordered neighbor list:

```text
LevelAdjacency {
    level: u16
    len:   u16
    cap:   u16
    ids:   [NodeId; cap]
}
```

Level 0 has capacity `2 * m`. Upper levels have capacity `m`. Lists are
stored compactly in node pages when possible and spill to overflow pages when
needed. Neighbor order is by the selected graph-pruning heuristic, then
`NodeId` as deterministic tie-breaker.

## WAL

Every structural graph mutation is WAL-logged before the dirty page reaches
disk. WAL records must be redo-only and idempotent.

Required record types:

- `HnswCreateMeta`: initialize meta page and build parameters.
- `HnswInsertNode`: allocate node slot, store vector payload, heap TID, level.
- `HnswSetEntryPoint`: update meta entry node and entry level.
- `HnswReplaceNeighbors`: replace one node's adjacency list at one level.
- `HnswMarkDeleted`: mark node tombstone.
- `HnswFreeNode`: move a tombstoned node to the free list with generation bump.
- `HnswPageSplit`: split or allocate node/overflow/free-list page.
- `HnswRebuildBegin`: mark index invalid and record build epoch.
- `HnswRebuildFinish`: install new graph root and mark index valid.

Redo rules:

- Replaying the same record twice must leave the same page image.
- Neighbor replacement records carry the full final list, not a delta.
- Page records include page LSN checks; records at or below page LSN are skipped.
- Recovery may leave an index `invalid`. The planner must ignore invalid HNSW
  indexes until rebuild completes.

## MVCC Visibility

HNSW nodes are structural routing entries. MVCC truth remains in the heap.

Search uses two phases:

1. Graph traversal explores nodes without checking heap visibility.
2. Candidate materialization heap-fetches TIDs and applies the statement
   snapshot. Invisible, aborted, updated-away, or deleted rows are discarded.

This rule is deliberate. If traversal skipped invisible nodes, a deleted entry
point or bridge node could disconnect reachable live rows and break recall.

Snapshot behavior:

- READ COMMITTED uses the statement snapshot already attached to the query.
- REPEATABLE READ and SERIALIZABLE reuse their transaction snapshot.
- A transaction can see its own inserted vector only after its heap tuple and
  index node are both present.
- A committed tuple visible to a later snapshot must have its HNSW node in a
  valid index. If recovery or a bug can leave such a row unindexed, the index
  must be marked invalid and ignored until rebuild.

Index scans must expose a `recheck` flag to the executor. The executor computes
the true distance from the visible heap value before final ordering and output.

## Inserts And Updates

INSERT maintenance:

1. Heap insert writes the row.
2. HNSW node allocation chooses a random level from a deterministic per-index
   seed plus heap TID.
3. Graph insertion searches from the current entry point, selects neighbors per
   level, writes reciprocal edges, then WAL-logs final neighbor lists.
4. If the inserted node has a higher level than the current entry point, WAL
   updates the meta page after the node is durable.

UPDATE maintenance:

- Non-vector HOT updates do not create a new HNSW node.
- Vector-changing updates are delete-plus-insert: tombstone the old node and
  insert a new node for the new heap tuple version.
- If an UPDATE changes another indexed column but not the vector, HNSW does not
  change unless the heap TID changes in a way that invalidates the old entry.

Partial failures:

- If index maintenance fails before commit, the transaction must abort.
- If crash occurs after heap WAL but before HNSW WAL, recovery leaves the index
  invalid unless the transaction is known aborted.

## Deletes

DELETE does not remove edges immediately. It marks the node tombstone.

Reasons:

- Immediate physical removal can disconnect graph paths.
- Readers may hold snapshots that still need the old tuple version.
- Neighbor repair is expensive and better batched by VACUUM.

Tombstoned nodes:

- remain traversable,
- are never returned as visible results after heap visibility rejects them,
- count toward tombstone density for vacuum scheduling.

If the entry point is tombstoned, it remains valid for routing. Rebuild can
choose a live entry point later.

## Vacuum

VACUUM runs in two modes.

Light vacuum:

- scans tombstone nodes,
- checks heap visibility horizon,
- frees nodes whose heap tuple is no longer visible to any active snapshot,
- bumps node generation before slot reuse,
- keeps neighbor references in other nodes until they are pruned.

Graph repair vacuum:

- triggers when tombstone density or stale-edge density crosses a threshold,
- rewrites neighbor lists for affected nodes,
- removes stale `NodeId` references,
- backfills alternative neighbors using exact local search over nearby live
  nodes,
- WAL-logs full replacement lists.

VACUUM must not promise recall improvement unless measured by a benchmark or
recall test artifact. Its correctness contract is narrower: no stale reused
slot may be followed as if it were the old node, and visible rows must not be
returned without heap recheck.

## Rebuild

Rebuild is the canonical repair path. It is required for:

- parameter changes (`m`, `ef_construction`, metric, dims),
- index corruption,
- excessive tombstone density,
- low measured recall after many deletes,
- format upgrades.

Rebuild procedure:

1. Create a new hidden HNSW relation for the same table and column.
2. Take a catalog snapshot and heap scan all rows visible to the build
   transaction.
3. Insert vectors into the hidden graph in deterministic heap order.
4. WAL-log `HnswRebuildFinish` with old and new relation IDs.
5. Atomically swap the catalog index entry to the new relation.
6. Mark the old relation pending drop after no active snapshot can reference it.

`REINDEX INDEX` uses the same path. `CREATE INDEX CONCURRENTLY` needs a later
two-pass design and is not part of the first implementation.

## Concurrency

Locks:

- meta page latch protects entry point and global counters.
- node page latch protects node header and inline adjacency lists.
- overflow page latch protects spilled adjacency data.
- relation-level build lock serializes rebuild against ordinary inserts.

Latch order:

1. meta page,
2. lower `NodeId`,
3. higher `NodeId`,
4. overflow pages by block number.

Graph insertion must avoid holding heap locks while acquiring HNSW page
latches. It receives the already-formed vector payload and heap TID from DML
maintenance, then mutates graph pages.

Deadlock prevention:

- reciprocal edge updates lock nodes in sorted `NodeId` order,
- long searches run latch-free on copied neighbor lists,
- if a node changes generation during search, the candidate is discarded and
  search continues.

## Planner And Executor Contract

The planner can choose HNSW only when all are true:

- query has `ORDER BY <vector-column> <distance-op> <probe> [ASC] LIMIT k`,
- operator class matches index metric,
- indexed column is `VECTOR(n)` with matching dimensions,
- index is valid,
- no WHERE clause requires exact ordering before filtering unless executor can
  apply the filter as a post-index recheck with enough candidate expansion.

Executor parameters:

- `ef_search`: session or index option, default at least `max(40, 4 * k)`.
- `candidate_multiplier`: raises fetched candidates when MVCC/deletes/filtering
  discard many rows.
- `recheck_exact`: always true for first implementation.

If HNSW returns fewer than `k` visible rows after expansion, executor either:

- repeats with larger `ef_search` until a cap, or
- falls back to exact scan when correctness mode requires complete top-k.

## Observability

Expose per-index counters:

- node count,
- live estimate,
- tombstone count,
- stale-edge estimate,
- entry level,
- last rebuild LSN,
- last vacuum LSN,
- average level-0 degree,
- HNSW scan count,
- heap recheck discard count,
- fallback-to-exact count.

`EXPLAIN` should show:

```text
HnswIndexScan index=items_embedding_hnsw metric=l2 k=10 ef_search=64 recheck=true
```

## Testing

Required tests before the index is usable:

- page codec round-trip and corruption rejection,
- WAL redo for each record type,
- crash recovery after node insert, neighbor replacement, tombstone, vacuum,
  and rebuild swap,
- MVCC visibility with uncommitted insert, aborted insert, committed delete,
  old snapshot after delete, and own-write visibility,
- delete storm followed by vacuum and search,
- generation bump rejects stale reused node references,
- rebuild produces a valid graph from heap rows,
- planner only selects HNSW for supported vector top-k shapes,
- executor exact recheck matches exact top-k on small deterministic data when
  recall parameters are set high enough,
- SQLLogicTest-style portable smoke for DDL errors and unsupported shapes.

Benchmarks:

- exact top-k benchmark remains baseline,
- HNSW benchmark records recall@k and latency on the same deterministic data,
- no published HNSW speedup claim without recall and exact baseline artifacts.

## Implementation Slices

1. Parser/planner method metadata for `USING hnsw`, rejected by executor.
2. Storage page codec for meta, node, overflow, and free-list pages.
3. WAL record types and recovery tests.
4. Offline build from a heap snapshot, no online DML.
5. HNSW scan executor with heap visibility recheck and exact fallback.
6. INSERT maintenance.
7. Tombstone deletes.
8. Light vacuum with generation bump.
9. Graph repair vacuum.
10. Rebuild and catalog swap.
11. Benchmarks with recall@k plus exact top-k baseline.

Each slice must commit atomically with tests that prove its contract.
