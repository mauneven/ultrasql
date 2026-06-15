# EXPORT DATABASE / IMPORT DATABASE

`EXPORT DATABASE` writes a deterministic logical dump directory.
`IMPORT DATABASE` restores that dump into an empty database.

## Syntax

```sql
EXPORT DATABASE TO 'dump-directory';
IMPORT DATABASE FROM 'dump-directory';
```

The path must be a string literal. `EXPORT DATABASE` creates the destination
directory and rejects an existing path. `IMPORT DATABASE` requires a real
directory and rejects non-empty targets.

## Examples

```sql
CREATE SCHEMA app;
CREATE SEQUENCE app.ticket_seq START WITH 7 INCREMENT BY 3;
CREATE TABLE app.items (id INT NOT NULL, label TEXT, qty INT);
CREATE INDEX items_label_idx ON app.items (label);
INSERT INTO app.items VALUES (1, 'alpha', 10), (2, 'beta', NULL);

EXPORT DATABASE TO '/tmp/ultrasql-dump';
```

```sql
IMPORT DATABASE FROM '/tmp/ultrasql-dump';
SELECT id, label, qty FROM app.items ORDER BY id;
```

## Dump Format

The dump is a directory with:

- `manifest.json` — format name, version, schemas, sequences, tables, indexes,
  and table data-file names.
- `schema.sql` — portable DDL for schemas, sequences, tables, and supported
  indexes.
- `data/*.sql` — one deterministic `INSERT` stream per table.
- `checksums.json` — SHA-256 checksums and byte counts for `manifest.json`,
  `schema.sql`, and every data file.

Import validates the manifest format/version and all checksums before executing
any dump SQL.

## Supported Behavior

- Ordinary tables in user schemas are exported with column names, types, and
  `NOT NULL` flags.
- Table data is exported from a repeatable-read heap snapshot.
- User schemas owned by the current user are exported.
- Standalone sequences owned by the current user are exported with bounds,
  increment, cache, cycle state, and current `setval` state.
- Plain btree indexes on table columns are exported and rebuilt through normal
  `CREATE INDEX` execution during import.
- Both statements are rejected inside explicit transaction blocks.

## Limitations

- Import is restore-into-empty only.
- User-defined types, materialized views, views, table constraints beyond
  `NOT NULL`, defaults, generated columns, identity/serial defaults, foreign
  keys, exclusion constraints, row-security policies, comments, advanced index
  metadata, and non-btree indexes are rejected during export.
- Import validates files before execution, but restore DDL/DML is not yet a
  single transactional catalog operation.
- Paths are local filesystem paths on the server process.

## Evidence

- Parser and AST: `crates/ultrasql-parser/src/statements/admin.rs`,
  `crates/ultrasql-parser/src/ast.rs`.
- Binder and logical plan: `crates/ultrasql-planner/src/binder/mod.rs`,
  `crates/ultrasql-planner/src/plan.rs`.
- Server execution: `crates/ultrasql-server/src/session/export_import.rs`,
  `crates/ultrasql-server/src/session/execute.rs`,
  `crates/ultrasql-server/src/session/ext.rs`.
- Tests: `crates/ultrasql-parser/src/statements/admin.rs`,
  `crates/ultrasql-planner/src/binder/tests.rs`,
  `crates/ultrasql-server/tests/export_import_round_trip.rs`.
