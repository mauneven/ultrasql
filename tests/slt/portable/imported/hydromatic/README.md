# Hydromatic SQL Logic Test Import

Source: `https://github.com/hydromatic/sql-logic-test`

Pinned commit: `0a809c530457bf0e56d637ef19fcaabd2964fd67`

License: MIT, copied to `LICENSE.upstream`.

Notice: copied to `NOTICE.upstream`.

Imported files:

- `src/main/resources/test/index/between/1/slt_good_0.test`
- `src/main/resources/test/index/in/10/slt_good_0.test`

This is a bounded, auditable first shard from the public SQLLogicTest corpus.
The files contain 20,000 generated SQL query records total, so PR smoke runs
should use `--case-limit` while nightly jobs can raise the limit as UltraSQL
query latency improves.

Recommended smoke command:

```sh
target/debug/ultrasql-sqllogictest-runner \
  --mode in-process \
  --case-limit 50 \
  --reference-engine sqlite \
  --reference-engine duckdb \
  tests/slt/portable/imported/hydromatic
```
