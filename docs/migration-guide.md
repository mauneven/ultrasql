# Migration Guide

UltraSQL is a SQL database, but it is not a v1.0 production database yet. Treat
any migration as a staged validation project until the release gates close.

## Recommended migration loop

1. Inventory schema objects: tables, indexes, constraints, views, functions,
   extensions, triggers, partitions, roles, and permissions.
2. Compare that inventory against `docs/known-limitations.md`.
3. Export schema with existing database tools in plain SQL form.
4. Apply schema to a disposable UltraSQL data directory.
5. Load a small data sample with `COPY` or INSERT batches.
6. Run application read queries through supported SQL clients.
7. Run write-path and transaction tests, including rollback and savepoints.
8. Re-run SQLLogicTest and application regression suites against both engines.
9. Capture performance with repository benchmark scripts only.
10. Keep the current database as system of record until UltraSQL passes the workload's
    correctness, recovery, and operational gates.

## Safer first workloads

- Analytical SELECTs over simple tables.
- COPY ingest and export experiments.
- JSONB query/operator experiments.
- Vector/RAG prototypes where exact recall and restart behavior are validated.
- SQL test runs that can tolerate currently missing features.

## Avoid for now

- Production finance or data-loss-sensitive systems.
- Workloads requiring extension loading.
- PL/pgSQL-heavy schemas.
- Trigger-heavy schemas.
- Complex role, privilege, and RLS deployments not yet mirrored in UltraSQL.
- Partitioning-heavy systems.
- Logical replication as a primary migration path.

## Data movement

Use plain SQL and `COPY`-oriented flows first. Archive dump/restore breadth is
tracked in `ROADMAP.md`; do not assume every existing dump format restores into
UltraSQL yet.

## Validation rule

A migration is not successful because the schema loads. It is successful only
after query results, transaction behavior, restart recovery, and benchmark
artifacts match the acceptance criteria for the target workload.
