# Public Regression Subset

Source: `https://github.com/postgres/postgres`

Pinned commit: `ddd12d1a5c4d980c5f31dc7d096012547b724e55` (`REL_17_STABLE`, checked 2026-05-21).

License: PostgreSQL license, included at `LICENSE.upstream`.

Imported files:

- `select_basics.slt`
- `parser_type_baseline.slt`
- `index_constraint_operator_baseline.slt`
- `type_specific_baseline.slt`
- `aggregate_window_baseline.slt`
- `type_coercion_baseline.slt`

Derived upstream regression sources:

- `src/test/regress/sql/select.sql`
- `src/test/regress/sql/char.sql`
- `src/test/regress/sql/varchar.sql`
- `src/test/regress/sql/numeric.sql`
- `src/test/regress/sql/text.sql`
- `src/test/regress/sql/date.sql`
- `src/test/regress/sql/time.sql`
- `src/test/regress/sql/timestamp.sql`
- `src/test/regress/sql/timetz.sql`
- `src/test/regress/sql/json.sql`
- `src/test/regress/sql/jsonb.sql`
- `src/test/regress/sql/arrays.sql`
- `src/test/regress/sql/aggregates.sql`
- `src/test/regress/sql/window.sql`
- `src/test/regress/sql/type_sanity.sql`
- `src/test/regress/sql/create_index.sql`
- `src/test/regress/sql/constraints.sql`
- `src/test/regress/sql/create_operator.sql`
- `src/test/regress/sql/opr_sanity.sql`

These are small, hand-curated SQLLogicTest translations of public upstream
regression behavior. The shards use local deterministic fixtures and expected
rows written in SQLLogicTest format; they do not vendor the full upstream
regression suite. Unsupported catalog-wide sanity checks and user-defined
operator DDL stay as explicit `# ultrasql:skip` debt in the relevant shard.
Broader type-specific numeric overflow, collation, timezone-abbreviation, and
SQL/JSON breadth remain roadmap debt.
The aggregate/window shard covers grouped aggregates, HAVING, and core window
functions with deterministic local fixtures.
The type-coercion shard covers explicit casts, assignment-compatible inserts,
NULL casts, `COALESCE`, and `CASE` coercion with deterministic local fixtures.

Run with PostgreSQL reference:

```sh
POSTGRES_URL="host=127.0.0.1 port=5432 user=postgres dbname=ultrasql_slt" \
tests/slt/run_sql_regression.sh
```
