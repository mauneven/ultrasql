# Backup And Restore

UltraSQL v1.0 exposes physical backup and archive-style restore through the
`ultrasql` CLI. These commands operate on an UltraSQL data directory, not on a
remote PostgreSQL server.

## Commands

```bash
ultrasql --data-dir target/ultrasql-data --basebackup target/basebackup
ultrasql --data-dir target/ultrasql-data --pg-dump target/ultrasql.dump --dump-format custom
ultrasql --data-dir target/ultrasql-data --pg-dump target/ultrasql-dir --dump-format directory
ultrasql --data-dir target/ultrasql-data --pg-dump target/ultrasql.tar --dump-format tar
ultrasql --data-dir target/restored-data --pg-restore target/ultrasql.dump
ultrasql --data-dir target/restored-data validate
```

`--basebackup` copies the data-directory tree and writes
`backup_manifest.json` with file sizes and SHA-256 checksums. `--pg-dump`
writes an UltraSQL archive in custom, directory, and tar modes. Current dumps
record SHA-256 payload checksums; `--pg-restore` validates directory dump
manifests and archive checksums before restoring files into the target data
directory.

When `--ops-endpoint` is supplied, `--basebackup` and `--pg-dump` call the
server backup fence before and after copying. The dump records the returned
checkpoint fence in the archive or directory manifest so reviewers can tell
whether an online copy used the fence path.

## Smoke Runner

Run the committed smoke:

```bash
benchmarks/backup_restore_smoke.sh
```

The runner starts a local source `ultrasqld`, creates
`backup_restore_smoke`, inserts three rows, creates an index on `id`, stops the
source server, runs `ultrasql --basebackup`, `ultrasql --pg-dump`, and
`ultrasql --pg-restore`, validates each restored directory, starts each restored
server, then checks every requested dump format:

- row counts with `SELECT COUNT(*) FROM backup_restore_smoke`
- index query behavior with `SELECT payload FROM backup_restore_smoke WHERE id = 2`
- the default multi-format matrix: custom, directory, and tar

The artifact is:

```text
benchmarks/results/latest/backup_restore_smoke_manifest.json
```

The artifact status is `measured` only when basebackup, dump, restore,
validation, row counts, and index query checks all pass for every requested
format. The manifest records `dump_format_results` plus
`dump_formats_verified` so release reviewers can see which archive surfaces were
actually exercised. Missing local prerequisites such as `psql` are recorded as
`not_available`, not as a failed backup system and not as a benchmark claim.

## Clean-Copy Rule

For release evidence, copy a data directory only after writers are stopped or
through the checkpoint-fenced `--ops-endpoint` path. The current runner uses a
small local server and stops it before the physical copy. Do not publish
backup/restore evidence from a live directory copy without the recorded
checkpoint-fencing metadata.
