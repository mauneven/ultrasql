# Migration from PostgreSQL

UltraSQL targets PostgreSQL wire and SQL compatibility, but it is not a drop-in production replacement yet.
Treat migration as a staged validation project until the v1.0 gates close.

## Recommended migration loop

1. Inventory schema objects: tables, indexes, constraints, views, functions,
   extensions, triggers, partitions, roles, and permissions.
2. Compare that inventory against `docs/known-incompatibilities.md`.
3. Export schema with PostgreSQL tools in plain SQL form.
4. Apply schema to a disposable UltraSQL data directory.
5. Load a small data sample with `COPY` or INSERT batches.
6. Run application read queries through PostgreSQL wire clients.
7. Run write-path and transaction tests, including rollback and savepoints.
8. Re-run SQLLogicTest and application regression suites against both engines.
9. Capture performance with repository benchmark scripts only.
10. Keep PostgreSQL as system of record until UltraSQL passes the workload's
    correctness, recovery, and operational gates.

## Safer first workloads

- Analytical SELECTs over simple tables.
- COPY ingest and export experiments.
- JSONB query/operator experiments.
- Vector/RAG prototypes where exact recall and restart behavior are validated.
- Compatibility test runs that can tolerate missing PostgreSQL features.

## Avoid for now

- Production finance or data-loss-sensitive systems.
- Workloads requiring full PostgreSQL extension compatibility.
- PL/pgSQL-heavy schemas.
- Trigger-heavy schemas.
- Complex role, privilege, and RLS deployments not yet mirrored in UltraSQL.
- Partitioning-heavy systems.
- Logical replication as a primary migration path.

## Data movement

Use plain SQL and COPY-oriented flows first. `pg_dump` custom/directory/tar
compatibility and full `pg_restore` parity are tracked in `ROADMAP.md`; do not
assume every PostgreSQL dump option restores into UltraSQL yet.

## Validation rule

A migration is not successful because the schema loads. It is successful only
after query results, transaction behavior, restart recovery, and benchmark
artifacts match the acceptance criteria for the target workload.
