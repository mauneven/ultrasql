# PostgreSQL Regression Compatibility Subset

Source: `https://github.com/postgres/postgres`

Pinned commit: `ddd12d1a5c4d980c5f31dc7d096012547b724e55` (`REL_17_STABLE`, checked 2026-05-21).

License: PostgreSQL license, copied to `LICENSE.upstream`.

Imported files:

- `select_basics.slt`
- `parser_type_baseline.slt`

Derived upstream regression sources:

- `src/test/regress/sql/select.sql`
- `src/test/regress/sql/char.sql`
- `src/test/regress/sql/varchar.sql`
- `src/test/regress/sql/numeric.sql`
- `src/test/regress/sql/type_sanity.sql`

These are small, hand-curated SQLLogicTest translations of public PostgreSQL
regression behavior. The shards use local deterministic fixtures and expected
rows written in SQLLogicTest format; they do not vendor the full PostgreSQL
regression suite.

Run with PostgreSQL reference:

```sh
POSTGRES_URL="host=127.0.0.1 port=5432 user=postgres dbname=ultrasql_slt" \
tests/slt/run_postgres_compat.sh
```
