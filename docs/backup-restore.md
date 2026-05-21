# Backup And Restore

UltraSQL v1.0 exposes physical backup and archive-style restore through the
`ultrasql` CLI. These commands operate on an UltraSQL data directory, not on a
remote PostgreSQL server.

## Commands

```bash
ultrasql --data-dir target/ultrasql-data --basebackup target/basebackup
ultrasql --data-dir target/ultrasql-data --pg-dump target/ultrasql.dump --dump-format custom
ultrasql --data-dir target/restored-data --pg-restore target/ultrasql.dump
ultrasql --data-dir target/restored-data validate
```

`--basebackup` copies the data-directory tree and writes
`backup_manifest.json` with file sizes and checksums. `--pg-dump` writes an
UltraSQL archive. `--pg-restore` rehydrates that archive into a target data
directory.

## Smoke Runner

Run the committed smoke:

```bash
benchmarks/backup_restore_smoke.sh
```

The runner starts a local source `ultrasqld`, creates
`backup_restore_smoke`, inserts three rows, creates an index on `id`, stops the
source server, runs `ultrasql --basebackup`, `ultrasql --pg-dump`, and
`ultrasql --pg-restore`, validates the restored directory, starts a restored
server, then checks:

- row counts with `SELECT COUNT(*) FROM backup_restore_smoke`
- index query behavior with `SELECT payload FROM backup_restore_smoke WHERE id = 2`

The artifact is:

```text
benchmarks/results/latest/backup_restore_smoke_manifest.json
```

The artifact status is `measured` only when basebackup, dump, restore,
validation, row counts, and index query checks all pass. Missing local
prerequisites such as `psql` are recorded as `not_available`, not as a failed
backup system and not as a benchmark claim.

## Clean-Copy Rule

For release evidence, copy a data directory only after writers are stopped or
after a future checkpoint-fenced online-backup path is documented. The current
runner uses a small local server and stops it before the physical copy. Do not
publish backup/restore evidence from a live directory copy without an explicit
checkpoint-fencing note.
