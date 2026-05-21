# Catalog Upgrades

UltraSQL v1.0 data directories carry an explicit catalog version marker at
`catalog.version`. The current catalog version is `1`.

## Startup Contract

`ultrasqld --data-dir DIR` and `ultrasql validate --data-dir DIR` call the same
catalog-version guard before opening WAL-backed storage:

- If `DIR/catalog.version` is missing, startup creates it with `1`.
- If the marker is `1`, startup continues.
- If the marker is lower than `1`, startup continues because no older durable
  catalog version exists yet.
- If the marker is higher than `1`, startup refuses the data directory. A newer
  catalog may contain relation, index, WAL, or heap-visibility state this binary
  cannot interpret safely.
- If the marker is not an unsigned integer, startup refuses the data directory.

The refusal is deliberate. A binary must never silently reinterpret a catalog
created by a newer binary.

## Migration Path

v1.0 has no in-place catalog migration because version `1` is the first durable
catalog marker. Future catalog changes must add an offline migrator before the
newer binary writes a higher marker:

1. Take a clean physical backup or `pg_dump`-style UltraSQL archive.
2. Stop all writers.
3. Run the checked migrator against the stopped data directory.
4. Validate the upgraded directory with `ultrasql validate --data-dir DIR`.
5. Start the newer `ultrasqld` binary.

Downgrades are not supported unless a future release documents a reverse
migration for the exact version pair. Use restore-from-backup instead.

## Operator Checks

```bash
ultrasqld --data-dir target/ultrasql-data
cat target/ultrasql-data/catalog.version
ultrasql --data-dir target/ultrasql-data validate
```

Expected v1.0 marker:

```text
1
```
