# ALTER VIEW

`ALTER VIEW` changes regular-view catalog metadata without changing base-table
data.

## Syntax

```sql
ALTER VIEW view_name RENAME TO new_name;
ALTER VIEW view_name SET SCHEMA schema_name;
ALTER VIEW view_name AS SELECT ...;
```

`ALTER VIEW ... AS SELECT` is parsed but rejected with a structured
`NotSupported` error until dependency-safe replacement semantics are available.

## Examples

```sql
CREATE SCHEMA app;
CREATE TABLE docs_alter_src (id INT NOT NULL, label TEXT);
CREATE VIEW docs_alter_v AS SELECT id, label FROM docs_alter_src;

ALTER VIEW docs_alter_v RENAME TO docs_alter_v2;
ALTER VIEW docs_alter_v2 SET SCHEMA app;

DESCRIBE VIEW app.docs_alter_v2;
SELECT id, label FROM app.docs_alter_v2 ORDER BY id;
```

## Supported Behavior

- `RENAME TO` updates the catalog relation, persisted view runtime metadata,
  privileges, row-security owner metadata, and plan-cache state.
- `SET SCHEMA` moves the view to an existing schema after normal catalog and
  privilege checks.
- Ordinary tables are rejected with actionable wrong-object errors.
- Renaming or moving a view is rejected when another regular view depends on it.
- View metadata survives restart after rename and schema move.

## Limitations

- `ALTER VIEW ... AS SELECT` is not supported.
- Dependency tracking is limited to regular views whose stored plans scan the
  changed view. Broader object dependency tracking remains roadmap work.
- DDL inside explicit transaction blocks is rejected by the server. Transaction
  rollback tests therefore assert rejection and unchanged catalog state rather
  than in-transaction DDL rollback.
- `ALTER VIEW OWNER TO`, `ALTER VIEW SET (...)`, `ALTER VIEW RESET (...)`,
  `ALTER VIEW ALTER COLUMN`, and materialized-view refresh/index operations are
  not supported by this statement.

## Evidence

- Parser and AST: `crates/ultrasql-parser/src/statements/create_view.rs`,
  `crates/ultrasql-parser/src/ast.rs`.
- Binder and logical plan: `crates/ultrasql-planner/src/binder/ddl.rs`,
  `crates/ultrasql-planner/src/plan.rs`.
- Catalog mutation APIs: `crates/ultrasql-catalog/src/traits.rs`,
  `crates/ultrasql-catalog/src/memory.rs`,
  `crates/ultrasql-catalog/src/persistent.rs`.
- Execution: `crates/ultrasql-server/src/session/ddl.rs`,
  `crates/ultrasql-server/src/session/execute.rs`.
- Tests: `crates/ultrasql-parser/src/statements/create_view.rs`,
  `crates/ultrasql-planner/src/binder/tests.rs`,
  `crates/ultrasql-server/tests/view_round_trip.rs`.
