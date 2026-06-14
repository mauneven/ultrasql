# CHECKPOINT

`CHECKPOINT` forces a WAL-backed server to durably record a checkpoint fence.

## Syntax

```sql
CHECKPOINT;
```

Options are not supported. Statements such as `CHECKPOINT FAST` are rejected
instead of being accepted silently.

## Behavior

- Persistent servers append a WAL barrier record and wait for it to become
  durable.
- Dirty heap pages whose page LSN is covered by durable WAL are flushed.
- A `RecordType::Checkpoint` WAL record is appended and forced durable.
- The heap checkpoint LSN is published so later page mutations can emit
  full-page-write records when needed.
- In-memory sample servers have no WAL writer; `CHECKPOINT` flushes dirty heap
  pages and returns `CHECKPOINT`.

## Limitations

- `CHECKPOINT` is rejected inside explicit transaction blocks.
- This does not replace backup/restore, external incident-drill, or operator
  soak evidence required by the release checklist.
- Recovery currently still scans WAL normally; checkpoint records are durable
  evidence and a future recovery optimization point, not a claim that redo
  skipping has been enabled.

## Evidence

- Parser and AST: `crates/ultrasql-parser/src/parser/mod.rs`,
  `crates/ultrasql-parser/src/ast.rs`.
- Binder and logical plan: `crates/ultrasql-planner/src/binder/mod.rs`,
  `crates/ultrasql-planner/src/plan.rs`.
- Server execution: `crates/ultrasql-server/src/lib.rs`,
  `crates/ultrasql-server/src/session/execute.rs`,
  `crates/ultrasql-server/src/session/ext.rs`.
- Tests: `crates/ultrasql-parser/src/parser/tests/mod.rs`,
  `crates/ultrasql-planner/src/binder/tests.rs`,
  `crates/ultrasql-storage/src/buffer_pool.rs`,
  `crates/ultrasql-txn/src/manager.rs`,
  `crates/ultrasql-server/tests/checkpoint_round_trip.rs`.
