# Streaming Physical Replication — Design

Status: **design / not yet implemented.** This is the authoritative plan for
UltraSQL's #1 production blocker: continuous, networked physical replication
with a hot standby, synchronous-commit option, and failover. It follows the
same design-first, adversarially-gated, phased-increment discipline as
[`docs/savepoint-subtransactions-design.md`](savepoint-subtransactions-design.md),
[`docs/evalplanqual-design.md`](evalplanqual-design.md), and
[`docs/transactional-ddl-design.md`](transactional-ddl-design.md). Each phase is
independently landable behind its own gate; nothing here ships in one commit.

## Why

A node loss today is a total outage with data loss back to the last local
fsync. `TODO.md` and `docs/production-readiness.md` list HA/DR as a hard release
gate. Replication today is **offline WAL-file directory copying** (the
`--archive-command` / `--restore-command` pair in
`crates/ultrasql-server/src/main_support/cli.rs`) plus startup recovery from
those files — there is no streaming wire protocol, no continuously-applying hot
standby, no synchronous commit, and no failover.

This blocks any honest HA/DR claim. It does **not** block the single-node
read/scan/retrieval story the README scopes; it is required for "safe to deploy
as the primary store."

## What already exists (build on this, do not rebuild)

- **WAL segments + replay.** `ultrasql_wal::applier::replay_into`
  (`crates/ultrasql-wal/src/applier.rs:525`) applies a stream of WAL records to
  the heap/indexes/CLOG; `ultrasql_wal::read_floor`
  (`crates/ultrasql-wal/src/manifest.rs:66`) reads the recovery floor LSN. WAL
  is segment-based with a recycle floor.
- **Startup recovery.** `Server::init`
  (`crates/ultrasql-server/src/server_wal_recovery.rs:256+`) replays existing
  WAL from the floor before accepting appends, rebuilding CLOG
  (commit-aware, visibility-filtered bootstrap) and indexes.
- **Standby signal files.** `apply_startup_signal_files`
  (`crates/ultrasql-server/src/main_support/config.rs`) sets standby mode when
  `standby.signal` / `recovery.signal` is present; `Server::set_standby_mode`
  exists.
- **WAL archiving.** `--archive-command` / `--restore-command` /
  `--restore-max-segments` ship completed segments to/from an archive (the
  offline path). PITR replay-to-target exists (`recovery_target.rs`).
- **Physical replication slot struct.** `ReplicationSlot { restart_lsn,
  confirmed_flush_lsn }` (`crates/ultrasql-server/src/replication.rs:22`) — a
  data structure only; not yet wired to WAL retention or a walsender.
- **Logical replication (separate).** Publications / subscriptions / logical
  slots / CDC live in `replication.rs` too; this design is **physical**
  streaming and is orthogonal to that.

## What is missing

1. The libpq **replication subprotocol** on the wire: a connection opened with
   the `replication` startup parameter, then `IDENTIFY_SYSTEM`,
   `CREATE_REPLICATION_SLOT … PHYSICAL`, `START_REPLICATION … <LSN>` answered
   with a `CopyBothResponse` carrying `XLogData` / keepalive messages. No
   handling exists (grep for `START_REPLICATION` / `CopyBoth` / `XLogData` is
   empty).
2. A **walsender**: a primary-side backend that streams WAL from a start LSN to
   a connected standby and honors a physical slot's `restart_lsn` as the WAL
   retention floor.
3. A **walreceiver**: a standby-side client that connects via
   `primary_conninfo`, issues `START_REPLICATION`, and writes received WAL to
   local segments.
4. **Continuous online apply** on the standby (today apply only runs at
   startup): a loop that replays received WAL as it arrives and tracks
   receive / flush / replay LSNs.
5. **Synchronous commit**: `synchronous_standby_names` + a commit-side wait on
   standby flush/apply acknowledgement. `synchronous_commit` is currently
   accepted but inert.
6. **Failover / promotion**: a trigger (`pg_promote()` / promote signal file),
   timeline IDs + `.history` files, and slot `restart_lsn` pinning retention.

## Architecture

```
  PRIMARY                                   STANDBY
  ┌─────────────────────────┐               ┌──────────────────────────┐
  │ wire startup: replication│   CopyBoth    │ walreceiver              │
  │   -> walsender backend   │ ────────────> │  primary_conninfo        │
  │   reads WAL >= start_lsn │  XLogData /   │  writes local WAL        │
  │   slot.restart_lsn pins  │  keepalive    │   -> continuous apply    │
  │   WAL recycle floor      │ <──────────── │   (replay_into loop)     │
  │   waits on sync ack      │  standby reply│  tracks recv/flush/replay│
  └─────────────────────────┘  (flush LSN)  └──────────────────────────┘
```

- The **walsender** is just another connection mode: the startup packet's
  `replication` parameter routes the session to a replication command loop
  instead of the SQL loop. It reads WAL from `start_lsn` using a new
  incremental WAL **reader** (the inverse of the append path) and frames
  records as `XLogData`. It sends periodic keepalives and consumes the
  standby's reply messages (write/flush/apply LSNs) for sync-commit and slot
  advancement.
- The **walreceiver** is a small client (reusing the existing PG-wire client
  the driver-cert/integration tests already use) that connects, sends
  `START_REPLICATION <restart_lsn>`, and appends received WAL bytes to the
  standby's WAL, then notifies the apply loop.
- **Continuous apply** generalizes `Server::init`'s one-shot recovery into a
  resumable loop driven by newly-flushed WAL, reusing `replay_into`. Apply must
  hold back commit visibility exactly as recovery does.

## Phased increments (each its own PR + gate)

- **Phase 0 — this design doc.**
- **Phase 1 — walsender handshake + physical slot.** Recognize the
  `replication` startup parameter; answer `IDENTIFY_SYSTEM` (systemid,
  timeline, current LSN), `CREATE_REPLICATION_SLOT … PHYSICAL` (persist a
  `ReplicationSlot`, pin `restart_lsn` as the WAL recycle floor in the
  checkpoint/recycle path), and `START_REPLICATION` with a `CopyBothResponse`
  that streams `XLogData` from the requested LSN + keepalives. No standby yet;
  gate with a protocol-level test (a raw client drives the handshake and
  receives WAL bytes) and a slot-retention test (a held slot prevents WAL
  recycling below `restart_lsn`). Requires a new incremental WAL **reader** API
  in `ultrasql-wal`.
  - **Status — Phase 1a (control plane) landed.** The incremental WAL `reader`
    (`ultrasql_wal::reader::read_wal_range`), `replication`-parameter routing,
    `IDENTIFY_SYSTEM`, `CREATE`/`DROP_REPLICATION_SLOT … PHYSICAL` with durable
    on-disk persistence, and the `restart_lsn` recycle-floor clamp in
    `maybe_recycle_wal` are implemented and tested (raw-wire protocol round-trip
    + slot-retention floor logic). **Phase 1b** — `START_REPLICATION` streaming
    `XLogData` over `CopyBoth` + keepalives — is next; until then
    `START_REPLICATION` returns a defined `0A000` (feature_not_supported) error.
- **Phase 2 — walreceiver + WAL landing.** Standby connects via
  `primary_conninfo`, runs `START_REPLICATION`, writes received WAL to local
  segments durably, and sends standby status replies. Gate: a two-node
  in-process test streams WAL primary→standby and asserts byte-identical WAL.
- **Phase 3 — continuous hot-standby apply.** Resumable apply loop replays
  received WAL via `replay_into`; expose receive/flush/replay LSNs and a
  `pg_stat_replication` view + `ultrasql_replication_lag_*` metrics (the
  `/metrics` lag finding). Gate: write on primary → visible on standby within a
  bound; standby is read-only.
- **Phase 4 — synchronous commit.** `synchronous_standby_names`, priority/quorum
  sets, and commit-side waiting on standby flush (or apply) ack wired into the
  commit path (`server_lifecycle.rs` commit protocol). Gate: a committed
  transaction is durable on the standby before COMMIT returns; a lost standby
  with `synchronous_commit=remote_apply` blocks (or degrades per policy).
- **Phase 5 — failover / promotion + timelines.** `pg_promote()` / promote
  signal ends recovery and assigns a new timeline; `.history` files; slots
  survive promotion. Gate: promote a standby, reconnect a new standby on the
  new timeline, verify no divergence.

## Key integration points & risks

- **WAL reader.** The append path is the only WAL access today; Phase 1 needs a
  read-from-LSN cursor over segments that is safe against concurrent append and
  recycling. This is the riskiest new primitive — design it against the floor
  (`read_floor`) and the checkpoint recycle path.
- **Apply ≠ recovery-once.** Continuous apply must not assume a quiescent
  system; reuse `replay_into` but make CLOG/visibility holdback resumable.
- **Sync commit ordering.** Must preserve the existing WAL-durable→visible
  ordering (`server_lifecycle.rs`); the standby ack is an *additional* gate
  before visibility, never a replacement for local durability.
- **Retention.** A physical slot's `restart_lsn` must floor WAL recycling, or a
  lagging standby silently loses segments (the current floor logic lives in the
  checkpoint/recycle path).

## Not in scope here

Logical replication streaming, cascading standbys, and quorum-commit tuning are
follow-ups once Phases 1–5 land. Multiple synchronous standbys beyond a simple
priority list are deferred.
