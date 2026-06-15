# PIVOT

`PIVOT` is a `FROM` table-factor transform that turns selected row values into
aggregate columns.

## Syntax

```sql
FROM source_table
PIVOT (
  aggregate_function(value_column_or_*)
  FOR pivot_column
  IN (literal_value [ AS output_column ] [, ...])
)
```

Supported aggregate functions are `COUNT(*)`, `COUNT(column)`, `SUM(column)`,
`AVG(column)`, `MIN(column)`, and `MAX(column)`.

## Examples

```sql
CREATE TABLE pivot_sales (
  region TEXT,
  quarter TEXT,
  amount INT
);

INSERT INTO pivot_sales VALUES
  ('east', 'Q1', 10),
  ('east', 'Q1', 5),
  ('east', 'Q2', 7),
  ('west', 'Q1', 3);

SELECT region, q1, q2
FROM pivot_sales
PIVOT (SUM(amount) FOR quarter IN ('Q1' AS q1, 'Q2' AS q2))
ORDER BY region;
```

The result has one row per implicit group. In this example `region` is the
grouping column because `quarter` is the pivot column and `amount` is the
aggregate argument.

## Supported Behavior

- `PIVOT` binds as part of `FROM`, after an ordinary table, subquery, or other
  table factor.
- The output columns are all input columns except the pivot column and aggregate
  argument, followed by one aggregate column per `IN` value.
- `IN` values must be non-`NULL` literal constants and must be comparable to
  the pivot column after literal coercion.
- Duplicate pivot values and duplicate output column names are rejected during
  binding.
- `SUM` and `AVG` support `SMALLINT`, `INTEGER`, `BIGINT`, `REAL`, and
  `DOUBLE PRECISION` arguments. `COUNT`, `MIN`, and `MAX` support any value
  type the executor can count or compare.
- Rows where the pivot column is `NULL` do not contribute to any pivot bucket.
- Missing buckets return the aggregate identity: `COUNT` returns `0`; `SUM`,
  `AVG`, `MIN`, and `MAX` return `NULL`.
- Empty grouped input emits no rows. Empty input with no implicit grouping emits
  one aggregate row, following normal scalar aggregate behavior.

## Limitations

- Only one aggregate expression is supported per `PIVOT`.
- The aggregate argument must be a source column or `*`; arbitrary expressions
  are rejected.
- `FILTER`, `DISTINCT`, ordered-set aggregates, and multiple `FOR` columns are
  not supported.
- `SUM` and `AVG` over `DECIMAL` and `MONEY` are rejected until the executor has
  exact decimal accumulation for this operator.
- Output ordering is not guaranteed unless the query has an `ORDER BY`.

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
