# Isolation Suite

UltraSQL tracks isolation with a small executable baseline plus explicit
provenance. The goal is evidence, not broad claims.

## Sources

- `tests/isolation/acid.sql` is UltraSQL-authored and exercises a transfer
  invariant across `COMMIT` and `ROLLBACK`.
- Hermitage scenarios are derived from
  `https://github.com/ept/hermitage/blob/f029bec8e32af6a9506508638fdf74ef61286225/postgres.md`
  with attribution in `tests/isolation/NOTICE.hermitage.md`.
- PostgreSQL's full `src/test/isolation` isolationtester schedule is not
  imported. It remains a future compatibility gate.

## Coverage

`crates/ultrasql-server/tests/isolation_suite_round_trip.rs` runs through the
PostgreSQL wire path and covers:

- `acid.sql`: committed transfer plus rolled-back partial transfer preserve the
  total-balance invariant.
- Hermitage G1a: `READ COMMITTED` prevents dirty reads.
- Hermitage PMP: `REPEATABLE READ` prevents the tested phantom.
- Hermitage G2: `SERIALIZABLE` aborts one write-skew transaction with
  SQLSTATE `40001`.

`crates/ultrasql-txn/tests/hermitage.rs` still covers the broader Hermitage
anomaly matrix at `TransactionManager` level.

## SSI Honesty

UltraSQL installs SSI by default in server mode and records conflicts for
serializable transactions, but the current server integration is relation-level
SSI, not predicate-precise. That means it can abort conflicting serializable
transactions conservatively and pass the covered write-skew scenario, but it is
not yet PostgreSQL's predicate/page/tuple precision or full isolationtester
parity.

Do not describe this as full PostgreSQL SSI parity until predicate-precise locks,
the full PostgreSQL isolationtester schedule, and matching expected outputs are
green.
