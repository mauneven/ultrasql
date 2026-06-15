# UNPIVOT

`UNPIVOT` is a `FROM` table-factor transform that turns selected source columns
into name/value rows.

## Syntax

```sql
FROM source_table
UNPIVOT [ INCLUDE NULLS | EXCLUDE NULLS ] (
  value_output_column
  FOR name_output_column
  IN (source_column [ AS literal_label ] [, ...])
)
```

`EXCLUDE NULLS` is the default.

## Examples

```sql
CREATE TABLE quarterly_sales (
  id INT,
  q1 INT,
  q2 INT
);

INSERT INTO quarterly_sales VALUES
  (1, 10, 20),
  (2, NULL, 5);

SELECT id, quarter, amount
FROM quarterly_sales
UNPIVOT (amount FOR quarter IN (q1 AS 'Q1', q2 AS 'Q2'))
ORDER BY id, quarter;
```

The default result excludes the `q1` value for `id = 2` because it is `NULL`.

## Supported Behavior

- `UNPIVOT` binds as part of `FROM`, after an ordinary table, subquery, or other
  table factor.
- Output columns are all input columns not listed in the `IN` source list,
  followed by the name column and value column.
- Source columns must exist and resolve unambiguously.
- Source column labels must be non-`NULL` literal constants. Without `AS`, the
  source column name is used as the label.
- Source value columns must have compatible types. Matching types are preserved,
  numeric types are widened through normal numeric join rules, and text-like
  values are widened to `text`.
- `EXCLUDE NULLS` skips rows whose unpivoted value is `NULL`.
- `INCLUDE NULLS` keeps rows whose unpivoted value is `NULL`; the value output
  column is nullable in that mode.
- Empty input emits no rows.

## Limitations

- Multi-value tuple unpivoting is not supported.
- `UNPIVOT` does not currently accept computed expressions in the `IN` list;
  each item must name a source column.
- Output ordering is not guaranteed unless the query has an `ORDER BY`.
- Complex non-scalar type compatibility is limited to the existing binder type
  coercion rules.

## Evidence

- Parser and AST: `crates/ultrasql-parser/src/statements/select.rs`,
  `crates/ultrasql-parser/src/ast.rs`.
- Binder and logical plan: `crates/ultrasql-planner/src/binder/from.rs`,
  `crates/ultrasql-planner/src/plan.rs`.
- Optimizer metadata plumbing: `crates/ultrasql-optimizer/src/cost/mod.rs`,
  `crates/ultrasql-optimizer/src/enumeration/mod.rs`,
  `crates/ultrasql-optimizer/src/rules/constant_fold.rs`.
- Execution: `crates/ultrasql-executor/src/pivot.rs`,
  `crates/ultrasql-server/src/pipeline/lower_query.rs`.
- Tests: `crates/ultrasql-parser/src/statements/select.rs`,
  `crates/ultrasql-planner/src/binder/tests.rs`,
  `crates/ultrasql-server/tests/pivot_unpivot_round_trip.rs`.
