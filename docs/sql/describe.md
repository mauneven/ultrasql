# DESCRIBE

`DESCRIBE` returns column metadata for a table or query expression.

## Syntax

```sql
DESCRIBE [ TABLE ] table_name;
DESCRIBE VIEW view_name;
DESCRIBE SELECT ...;
```

## Examples

```sql
CREATE TABLE docs_describe (id INT NOT NULL, body TEXT);
CREATE VIEW docs_describe_v (doc_id, doc_body) AS
    SELECT id, body FROM docs_describe;

DESCRIBE TABLE docs_describe;
DESCRIBE docs_describe;
DESCRIBE VIEW docs_describe_v;
DESCRIBE SELECT id, body FROM docs_describe;
```

The result columns are stable:

| Column | Type | Meaning |
|---|---|---|
| `column_name` | `text` | Output column name. |
| `data_type` | `text` | UltraSQL logical SQL type. |
| `nullable` | `boolean` | Whether the column can contain `NULL`, when known. |
| `source_schema` | `text` | Source schema for table targets. Empty for query targets. |
| `source_object` | `text` | Source table name for table targets. Empty for query targets. |
| `source_kind` | `text` | `table`, `view`, or `query`. |

## Supported Behavior

- Table targets resolve through normal catalog lookup and search-path rules.
- View targets return the stored view schema and verify the target is a
  regular view at execution time.
- Query targets bind the `SELECT` expression and return its projected schema.
- Direct column projections preserve known catalog nullability.
- Missing objects return an undefined-table planner error that names the target.
- PostgreSQL Extended Query result formats are honored for the boolean
  `nullable` column.

## Limitations

- Query-expression nullability is conservative for computed expressions.
- `DESCRIBE` does not expose indexes, defaults, constraints, comments, or
  storage metadata.

## Evidence

- Parser and AST: `crates/ultrasql-parser/src/statements/describe.rs`,
  `crates/ultrasql-parser/src/ast.rs`.
- Binder and logical plan: `crates/ultrasql-planner/src/binder/mod.rs`,
  `crates/ultrasql-planner/src/plan.rs`.
- Server execution: `crates/ultrasql-server/src/session/execute.rs`,
  `crates/ultrasql-server/src/session/ext.rs`.
- Tests: `crates/ultrasql-parser/src/parser/tests/mod.rs`,
  `crates/ultrasql-planner/src/binder/tests.rs`,
  `crates/ultrasql-server/tests/describe_round_trip.rs`.
