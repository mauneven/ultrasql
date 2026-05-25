# Chaos Recovery

`benchmarks/chaos_recovery.sh` records release evidence for three crash and
storage-failure classes:

- random kill: start `ultrasqld`, commit rows, send `kill -9` to that child
  process only, restart, and verify row count plus `ultrasql validate`;
- WAL truncation: commit rows, stop cleanly, truncate bytes from the last
  `pg_wal/segment_*` file, restart, and verify recovery stops at the last good
  record without corrupting visible rows;
- disk full: use safe disk-full simulation by running only the child server
  under `ulimit -f`, drive inserts until a write fails, restart without the
  limit, and verify committed rows plus validation.

The harness writes:

```text
benchmarks/results/latest/chaos_recovery_manifest.json
```

The artifact is `measured` only when all three cases pass. Missing local tools
or unsupported `ulimit -f` are recorded as `status: "not_available"`, not as a
recovery claim. The harness never fills the host filesystem; disk pressure is a
per-process file-size cap scoped to the `ultrasqld` child.

Useful local runs:

```text
benchmarks/chaos_recovery.sh smoke
CHAOS_SEED=20260525 benchmarks/chaos_recovery.sh full
```

Keep the manifest with release evidence when using this as a v1.0 sign-off
gate.
