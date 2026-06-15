# SUMMARIZE

`SUMMARIZE` scans a table and returns one statistics row per column.

## Syntax

```sql
SUMMARIZE table_name;
```

Only ordinary table names are supported. Query expressions are rejected.

## Examples

```sql
CREATE TABLE docs_summary (
  id INT NOT NULL,
  label TEXT,
  amount DOUBLE PRECISION,
  created_on DATE
);

INSERT INTO docs_summary VALUES
  (1, 'a', 1.0, DATE '2024-01-01'),
  (2, 'a', 2.0, DATE '2024-01-03'),
  (3, NULL, 3.0, NULL);

SUMMARIZE docs_summary;
```

The result columns are stable:

| Column | Type | Meaning |
|---|---|---|
| `column_name` | `text` | Source column name. |
| `data_type` | `text` | UltraSQL logical SQL type. |
| `row_count` | `bigint` | Visible table rows scanned. |
| `null_count` | `bigint` | Visible `NULL` values for the column. |
| `min` | `text` | Minimum non-`NULL` value rendered as SQL text, when orderable. |
| `max` | `text` | Maximum non-`NULL` value rendered as SQL text, when orderable. |
| `unique_count` | `bigint` | Exact count of distinct non-`NULL` values. |
| `avg` | `double precision` | Mean for numeric columns, otherwise `NULL`. |
| `stddev` | `double precision` | Sample standard deviation for numeric columns with at least two values, otherwise `NULL`. |

## Supported Behavior

- `SUMMARIZE table_name` resolves through normal catalog lookup and search-path
  rules.
- The scan uses the statement MVCC snapshot. Inside an explicit transaction it
  sees rows visible to that transaction, and rolled-back rows are not reported
  after rollback.
- `row_count`, `null_count`, and `unique_count` are exact for the scanned
  snapshot.
- `min` and `max` are returned for scalar orderable values including numeric,
  text, boolean, date, and time values.
- Numeric `avg` and `stddev` use the same visible non-`NULL` rows counted by
  the statement.
- Missing tables return an undefined-table planner error that names the target.

## Limitations

- `SUMMARIZE (SELECT ...)` and other query-expression targets are not
  supported.
- Tables with row-level security enabled are rejected fail-closed until
  predicate-aware summaries are implemented.
- `unique_count` is exact and keeps distinct non-`NULL` values in memory, so
  cost grows with table size and column cardinality.
- `min` and `max` for non-orderable complex types return `NULL`.
- No approximate uniqueness algorithm is currently exposed.

## Evidence

- Parser and AST: `crates/ultrasql-parser/src/statements/summarize.rs`,
  `crates/ultrasql-parser/src/ast.rs`.
- Binder and logical plan: `crates/ultrasql-planner/src/binder/mod.rs`,
  `crates/ultrasql-planner/src/plan.rs`.
- Execution: `crates/ultrasql-server/src/pipeline/lower_query.rs`,
  `crates/ultrasql-server/src/pipeline/scan.rs`.
- Serializable read tracking: `crates/ultrasql-server/src/serializable.rs`.
- Tests: `crates/ultrasql-parser/src/parser/tests/mod.rs`,
  `crates/ultrasql-planner/src/binder/tests.rs`,
  `crates/ultrasql-server/tests/summarize_round_trip.rs`.
