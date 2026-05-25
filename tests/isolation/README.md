# UltraSQL Isolation Suite

This directory holds the audited transaction-isolation baseline used by
`crates/ultrasql-server/tests/isolation_suite_round_trip.rs`.

- `acid.sql` is UltraSQL-authored. It is a small account-transfer script that
  checks atomic commit and rollback invariants without importing third-party SQL.
- Hermitage-derived scenarios are implemented as Rust wire-level tests, not as a
  vendored upstream dump. Attribution and license notes live in
  `NOTICE.hermitage.md`.

The suite is intentionally small. It proves specific ACID/Hermitage scenarios
and documents open SSI precision gaps; it is not a replacement for PostgreSQL's
full isolationtester schedule.
