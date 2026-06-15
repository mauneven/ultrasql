# CREATE VIEW

`CREATE VIEW` stores a named `SELECT` query and exposes its projected columns
through the catalog. Query execution expands the stored plan against current
base-table rows.

## Syntax

```sql
CREATE [ OR REPLACE ] VIEW view_name [ (column_name [, ...]) ] AS SELECT ...;
```

`OR REPLACE` is accepted by the parser. Creating a missing view with
`CREATE OR REPLACE VIEW` works like `CREATE VIEW`; replacing an existing view is
rejected until dependency-safe replacement is implemented.

## Examples

```sql
CREATE TABLE docs_view_src (
  id INT NOT NULL,
  label TEXT
);

CREATE VIEW docs_view_v (doc_id, doc_label) AS
  SELECT id, label FROM docs_view_src;

SELECT doc_id, doc_label FROM docs_view_v ORDER BY doc_id;
DESCRIBE VIEW docs_view_v;
```

Views are listed in `pg_catalog.pg_class` with `relkind = 'v'` and in
`pg_catalog.pg_views`. They are excluded from `pg_catalog.pg_tables`.

## Supported Behavior

- View definitions must be `SELECT` queries accepted by the normal binder.
- Optional view column aliases must match the source query column count.
- View scans return current base-table rows; data is not materialized.
- View metadata is persisted and rebuilt on server restart.
- `DESCRIBE VIEW` returns stored view column names, types, and nullability.
- Views can be renamed or moved between schemas with `ALTER VIEW`.

## Limitations

- Replacing an existing view with `CREATE OR REPLACE VIEW` is rejected.
- Updatable views are not supported; `INSERT`, `UPDATE`, `DELETE`, and `MERGE`
  targets must be ordinary tables.
- `WITH CHECK OPTION`, `TEMPORARY VIEW`, `RECURSIVE VIEW`, security-barrier
  views, and view options are not supported.
- `pg_catalog.pg_views.definition` is currently `NULL`; source SQL is persisted
  for execution but not exposed as deparsed catalog text.
- `EXPORT DATABASE` rejects databases containing regular views until dump and
  restore coverage includes view definitions and dependency ordering.
- DDL inside explicit transaction blocks is still rejected by the server, so
  view DDL currently runs in autocommit mode.

## Evidence

- Parser and AST: `crates/ultrasql-parser/src/statements/create_view.rs`,
  `crates/ultrasql-parser/src/ast.rs`.
- Binder and logical plan: `crates/ultrasql-planner/src/binder/ddl.rs`,
  `crates/ultrasql-planner/src/plan.rs`.
- Catalog and metadata: `crates/ultrasql-catalog/src/traits.rs`,
  `crates/ultrasql-server/src/lib.rs`,
  `crates/ultrasql-server/src/pipeline/catalog_views.rs`.
- Execution: `crates/ultrasql-server/src/session/ddl.rs`,
  `crates/ultrasql-server/src/session/execute.rs`.
- Tests: `crates/ultrasql-parser/src/statements/create_view.rs`,
  `crates/ultrasql-planner/src/binder/tests.rs`,
  `crates/ultrasql-server/tests/view_round_trip.rs`.
