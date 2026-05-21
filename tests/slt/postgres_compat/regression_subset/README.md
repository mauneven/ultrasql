# PostgreSQL Regression Compatibility Subset

Source: `https://github.com/postgres/postgres`

Pinned commit: `ddd12d1a5c4d980c5f31dc7d096012547b724e55` (`REL_17_STABLE`, checked 2026-05-21).

License: PostgreSQL license, copied to `LICENSE.upstream`.

Imported files:

- `select_basics.slt`

This is a small, hand-curated SQLLogicTest translation of public PostgreSQL
regression `SELECT` behavior. The shard uses local deterministic fixtures and
expected rows written in SQLLogicTest format; it does not vendor the full
PostgreSQL regression suite.

Run with PostgreSQL reference:

```sh
POSTGRES_URL="host=127.0.0.1 port=5432 user=postgres dbname=ultrasql_slt" \
tests/slt/run_postgres_compat.sh
```
