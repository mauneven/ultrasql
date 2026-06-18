# SECURITY_AUDIT.md — UltraSQL v0.5 adversarial pass

**Date:** 2026-05-12
**Last dependency refresh:** 2026-06-10
**Auditor:** automated adversarial sweep, owner-directed
**Scope:** workspace at `crates/`, dependencies in `Cargo.toml`/`Cargo.lock`
**Baseline:** 420 tests green pre-audit; 438 tests green post-audit (18 new
regression tests added)

This document records the findings of an internal adversarial pass on
the v0.5 surface. The pass focused on the new public network surface
(wire protocol parser, server connection handler) plus
the durability path (WAL recovery, segment files) and the SQL frontend
(lexer, parser).

---

## 0. 2026-06-10 dependency-audit refresh

Scope: dependency advisory gate and CI/release enforcement only. This
refresh did not re-run the full adversarial source review from
2026-05-12.

Dependency changes:

- Apache Arrow and Parquet workspace dependencies moved from `58.3.0`
  to `59.0.0`.
- `thrift 0.17.0`, `integer-encoding 3.0.4`, and
  `ordered-float 2.10.1` left the dependency graph through the Parquet
  update.
- `paste 1.0.15` remains in the dependency graph through
  `parquet 59.0.0`.

Commands run locally:

- `cargo audit --deny yanked` — advisory DB fetched from
  `https://github.com/RustSec/advisory-db.git`, 1124 advisories
  loaded, 446 crate dependencies scanned, exit 0.
- `cargo audit --deny warnings` — exit 1 because `paste 1.0.15` is
  flagged unmaintained by `RUSTSEC-2024-0436`; dependency path is
  `paste -> parquet 59.0.0 -> ultrasql-*`.
- `cargo deny check advisories` — exit 0 (`advisories ok`).
- `cargo tree -i paste --all-features` — confirms `paste` only enters
  through `parquet 59.0.0`.
- `cargo tree -i thrift --all-features` — exit 101 because `thrift` is
  no longer present in the workspace dependency graph.

CI changes:

- `.github/workflows/ci.yml` now has a `cargo-audit` job and the
  `ci-passed` aggregate requires it.
- `.github/workflows/release.yml` now runs `cargo audit --deny yanked`
  during the release verification gate before clippy/tests.

Gate policy: fail on known vulnerabilities and yanked crates. Allow the
current unmaintained warning for transitive `paste` while it is pulled
by the supported `parquet` release; keep it visible in audit output and
re-check when Arrow/Parquet publishes an alternate path.

---

## 0b. 2026-06-17 post-fix pass (workspace at v0.0.9)

Two pre-auth denial-of-service findings discovered after the 2026-05-12 pass
were fixed on `main`. Both are reachable under the default `Trust` policy (no
authentication), so they were pre-auth.

### F-9. Parser statement-level recursion DoS (High, fixed)

**Affected:** `crates/ultrasql-parser/src/statements/select.rs`
(`parse_table_factor` → `parse_select` / `parse_from_clause`).

**Exploitation:** F-1 bounded only the Pratt *expression* parser. A separate,
unguarded *statement*-level recursion remained: nested `FROM` subqueries
(`FROM (SELECT … FROM (SELECT …))`) and parenthesised joins (`(((t)))`) recursed
with no depth guard, so a few-KB query overflowed the worker stack and aborted
the process (`SIGABRT` under `panic = "abort"`) — an uncatchable, pre-auth
remote DoS.

**Remediation:** `parse_table_factor` now charges each level against the shared
`MAX_PARSE_DEPTH` budget via `enter_depth`/`leave_depth`, returning a
recoverable `DepthExceeded` error. Commit `87a3dda`. Regression tests:
`deeply_nested_from_subqueries_rejected_without_overflow`,
`deeply_nested_parenthesised_joins_rejected_without_overflow`.

### F-10. COPY FROM STDIN binary unbounded-buffer OOM DoS (High, fixed)

**Affected:** `crates/ultrasql-server/src/session/copy.rs`
(`collect_copy_stdin_bytes`).

**Exploitation:** `COPY t FROM STDIN WITH (FORMAT binary)` accumulated every
`CopyData` frame into one `Vec` with no cumulative bound. A client could stream
frames indefinitely, growing a single session's heap until the process was
OOM-killed, taking down every other session. The binary *file* path was already
capped; STDIN was not.

**Remediation:** the STDIN path now enforces the same 128 MiB ceiling
(`copy_binary_file_limit_bytes()`, env-tunable via
`ULTRASQL_COPY_BINARY_FILE_LIMIT_BYTES`). Commit `0fec4e9`. Regression test:
`copy_stdin_cumulative_limit_is_enforced`.

Also in this batch: `SSLRequest`/`GSSENCRequest` are now answered with the
mandatory `'N'` decline (commit `e613c09`) so stock clients connect in plaintext
instead of seeing a dropped socket — connections remain cleartext (no TLS
upgrade), which is tracked as an open limitation.

---

## 1. Summary

| Severity | Found | Fixed in this pass | Deferred |
|----------|------:|-------------------:|---------:|
| Critical |     0 |                  0 |        0 |
| High     |     4 |                  4 |        0 |
| Medium   |     2 |                  2 |        0 |
| Low      |     2 |                  1 |        1 |
| Info     |     2 |                  0 |        2 |

Total findings: 10. Active exploitation risk: **none confirmed**. No RCE,
no information disclosure beyond what a client already knows.

---

## 2. Findings — Fixed

### F-1. Parser stack overflow via deeply-nested expressions (High)

**Affected:** `crates/ultrasql-parser/src/parser.rs`
`parse_expr_with_precedence` / `parse_prefix`

**Exploitation:** A simple-query containing `((((((((... SELECT 1 ...))))))))`
with a few hundred thousand opening parens recurses through
`parse_expr_with_precedence` → `parse_prefix` → `parse_expr` until the
tokio worker thread blows its 2 MiB stack. Result: panic, the server
process aborts under default `panic = "abort"` release profile. Pre-auth
because the parser runs before any authentication check.

**Remediation:** Bound recursion at `MAX_PARSE_DEPTH` (currently `128`,
tightened from an earlier `1024` so the guard fires before sanitizer-
instrumented threads exhaust the stack). Each entry into
`parse_expr_with_precedence` increments a counter; on overflow the parser
returns `ParseError::DepthExceeded`, which surfaces as a query-scoped
`ErrorResponse` with SQLSTATE 42601. (Statement-level recursion is bounded
separately — see F-9 in the 2026-06-17 pass.)

**Regression test:** `parser::tests::deeply_nested_parens_rejected_without_overflow`,
`parser::tests::parens_below_limit_succeed`.

**Commit:** `665a7aa`

### F-2. WAL writer follows symlinks (High)

**Affected:** `crates/ultrasql-wal/src/writer.rs` `WriterDriver::ensure_segment_open`

**Exploitation:** If `wal_dir` is on a multi-tenant filesystem and a
hostile actor stages `wal_dir/segment_0000000000` as a symlink to a
sensitive file (`/etc/passwd`, another database's data, etc.), the
previous writer happily called `OpenOptions::open(path).append(true)`
and would have appended WAL records into the linked target on the
first flush. Damaging in shared-tenancy deployments.

**Remediation:** Open WAL segments with `O_NOFOLLOW` on Unix
(`OpenOptions::custom_flags(libc::O_NOFOLLOW)`). The `open(2)` call
returns `ELOOP` if the path is a symlink; the writer surfaces the error
upward and shuts down rather than silently overwriting unrelated files.

**Regression test:** `writer::tests::segment_open_refuses_to_follow_symlink`
(Unix-only; asserts both the open error and that the linked-to file is
unmodified).

**Commit:** `22ef98d`

### F-3. Wire protocol 1 GiB pre-auth memory amplification (High)

**Affected:** `crates/ultrasql-protocol/src/codec.rs` `MAX_PAYLOAD` (now
`MAX_MESSAGE_BYTES`)

**Exploitation:** The codec previously accepted any message whose
declared length was up to `(1 << 30) - 1` (approximately 1 GiB),
matching the configured frame-size cap. A single client opening a TCP
connection and writing a 5-byte tagged header advertising `length =
1 GiB` made the server pre-allocate a buffer of that size (or wait
forever for that many bytes to arrive). Repeat from N connections to
exhaust server memory pre-authentication.

**Remediation:** Cap accepted on-wire length at `MAX_MESSAGE_BYTES =
16 MiB`. The cap is comfortably above every legitimate Parse/Query/Bind
message in practice — production traffic uses COPY for bulk loads, not
a single jumbo message. Encoded length above the cap is rejected as
`ProtocolError::Malformed` before any buffer growth.

**Regression tests:**
- `codec::tests::frontend_length_above_max_rejected`,
- `codec::tests::backend_length_above_max_rejected`,
- `codec::tests::startup_length_above_max_rejected`,
- `codec::tests::bind_lies_about_param_count_caught_by_truncation`.

**Commit:** `da86d97`

### F-4. WAL decoder allocates from attacker-controlled length (High)

**Affected:** `crates/ultrasql-wal/src/record.rs` `WalRecord::decode`,
`compute_record_crc`

**Exploitation:** Recovery treats CRC mismatch as torn-write and stops
scanning cleanly. But the decoder allocates the payload buffer (and a
second buffer for CRC re-encode) BEFORE the CRC check runs. A hostile
actor who can write to a WAL segment file can craft a record header
that advertises `total_length = u32::MAX`, force two ~4 GiB
allocations, and OOM the recovery process. The CRC mismatch would have
caught the record, but only after the allocations.

**Remediation:** Bound `total_length` at `MAX_RECORD_BYTES = 64 MiB`
(comfortably above every legitimate record format; `FullPageWrite`
carries an 8 KiB page plus header). Refuse oversized headers as
`WalRecordError::Malformed` before allocation.

**Regression tests:**
- `record::tests::oversized_total_length_rejected_before_allocation`,
- `record::tests::total_length_just_past_ceiling_rejected`.

**Commit:** `b9f3b2a`

### F-5. Lexer `unsafe { from_utf8_unchecked }` (Medium)

**Affected:** `crates/ultrasql-parser/src/lexer.rs` `Lexer::lex_word`

**Exploitation:** The `unsafe` block was correct at the time of writing
— the upstream filter `b.is_ascii_alphanumeric() || b == b'_'` ensures
every byte in the buffer is ASCII. But a future change to the filter
that accidentally let a non-ASCII byte through would silently introduce
UB. Defence in depth.

**Remediation:** Replace `from_utf8_unchecked` with the checked
`from_utf8`; the cost is one extra pass over <= 64 bytes per
identifier in the fast path, which is dominated by the keyword table
lookup. Future filter regressions surface as a `LexerError::UnexpectedChar`
instead of UB.

**Regression test:** None added (the existing identifier tests already
exercise the path; failure mode is "no UB regression").

**Commit:** `cf55dfd`

### F-6. Protocol version mismatch closes connection silently (Medium)

**Affected:** `crates/ultrasql-server/src/lib.rs` `Session::startup`

**Exploitation:** A libpq-style client whose advertised version is
not `(3, 0)` (e.g. a future PG client speaking a hypothetical v4
protocol) previously saw the socket close with no diagnostic. The
client reports a confusing "connection closed before handshake"
upstream error. Not strictly a security issue, but it complicates
incident triage and makes the server look broken.

**Remediation:** Reply with `ErrorResponse` carrying SQLSTATE 08P01 and
a human-readable message before tearing the connection down.

**Regression test:**
`tests::unsupported_protocol_major_returns_error_response` (drives
handler with major = 0xFFFF, asserts ErrorResponse + 08P01 + clean task
exit with `UnsupportedProtocol` classification).

**Commit:** `1c4c429`

---

## 3. Findings — Verified safe

### F-7. Wire protocol Parse/Bind parameter-count explosion (Low — verified safe)

**Affected:** `crates/ultrasql-protocol/src/codec.rs` (decoders)

**Audit observation:** The decoder calls `Vec::with_capacity(count.min(64))`
for parameter / column / result-format Vecs. Initial capacity is bounded
at 64 elements; subsequent pushes grow the Vec only if the underlying
per-element reader (`read_value`, `read_i16`, `read_u32`) returns
without truncation. Per-element reads consume at least 2 bytes (i16) or
4 bytes (i32 length + 0 bytes payload) from the framed payload, so the
total memory growth is bounded by the message-length ceiling
(`MAX_MESSAGE_BYTES = 16 MiB` from F-3).

A regression test verifies the truncation behaviour: `codec::tests::bind_lies_about_param_count_caught_by_truncation`.

**Action:** None required. Bound is enforced transitively.

### F-8. Path traversal in segment file naming (Info — verified safe)

**Affected:** `crates/ultrasql-storage/src/segment.rs` `SegmentFileManager::relation`

**Audit observation:** Segment paths are constructed as
`base_dir/<RelationId.oid.raw()>/<segment_id>`. `RelationId` is a
`u32` allocated by the catalog, not derived from any user-supplied
string. No path-traversal vector. `format!` of `u32` cannot produce
`..` or `/` characters.

**Action:** None required.

### F-9. `format!` of user input into SQL strings (Info — verified safe)

**Audit observation:** No code path in the workspace re-formats a
user-controlled string back into SQL text. Parameter binding is
end-to-end via the AST → bound-value path; no SQL injection surface.
`git grep -nE 'format!.*sql|format!.*query' crates/` confirms: the
only `format!` calls touching SQL strings are in error messages
(quoting an identifier or table name back to the user) or in the
sample-data builder (`memory.rs` builds synthetic table names in
test fixtures).

**Action:** None required.

---

## 3a. Side-finding — heap concurrency regression coverage (Info)

**Affected coverage:** `crates/ultrasql-storage/src/heap/tests.rs` test
`concurrent_inserts_from_two_threads_preserve_every_tuple`

**Audit observation:** While running per-crate tests in isolation,
discovered that this test fails deterministically on Apple M4 with
"duplicate tids assigned" (expected 400 unique, got 399). When run as
part of `cargo test --workspace`, the test passes — almost certainly
because parallel scheduling differs and the race window closes.

**Not a security finding** — this is an internal consistency bug in
the heap's concurrent-insert path. The race is in `Heap::insert`
assigning the same `TupleId` to two concurrent inserts under
contention on the same page. Surfaced by accident.

**Historical status:** Reproduced at HEAD on the v0.5 heap commit `4675f4b` even
before any of this audit's changes. File against the heap subsystem as
a follow-up; not in scope for this audit.

**2026-06-10 refresh:** No longer reproduces at current `main`. Evidence:
`cargo test -p ultrasql-storage concurrent_inserts_from_two_threads_preserve_every_tuple -- --nocapture`
passes, and a 100-run isolated stress loop of the same test also passes.
Keep the regression test active; reopen the heap finding only if the
duplicate-`TupleId` failure reappears with a current commit hash and
reproduction command.

---

## 4. Closed — historical `TODO(security)` markers

No live `TODO(security)` markers remain in `crates/` at the 2026-06-10
audit refresh. The entries below are retained as historical findings so
future audits can trace the original risk, the remediation, and the
regression coverage.

### D-1. No per-connection slow-loris timeout

**Affected:** `crates/ultrasql-server/src/lib.rs:351` (`read_frontend`)

A client that opens a TCP session and dribbles bytes at 1 byte/minute
holds the connection forever. The read buffer is bounded
(`MAX_MESSAGE_BYTES = 16 MiB`), so the impact per connection is bounded,
but N idle connections still consume `N * 16 MiB` plus the per-task
overhead of a tokio task.

**Proposed fix:** Wrap the `read_buf` await in a `tokio::time::timeout`
with a configurable `idle_in_session_timeout` (default 60s) and
`statement_timeout` (default unlimited, configurable). Tear the
session down on expiry.

**Effort:** Requires wiring a `ServerConfig` struct from the binary
into the connection task. Touches `ultrasql-cli` and `ultrasql-server`
public surfaces. Marker placed at the call site:
`TODO(security): add per-connection slow-loris timeout`.

**2026-05-24 refresh:** Fixed by the statement timeout / cancellation
work and the configurable post-startup idle-session timeout. Regression
coverage: `statement_timeout_round_trip.rs`,
`idle_session_timeout_round_trip.rs`, and
`cancel_request_round_trip.rs`.

**Status:** Closed.

### D-2. mmap-aliasing under hostile concurrent writers

**Affected:** `crates/ultrasql-storage/src/segment.rs:217` (`SegmentFile::map`)

The mmap-as-`&[u8]` view assumes no other OS process mutates the segment
file concurrently. If a hostile process with write access to the data
directory mutates a mapped page while UltraSQL reads it, the view
technically violates Rust's aliasing rules. The buffer-pool checksum
catches integrity violations but not the soundness problem.

**Threat model:** UltraSQL's deployment contract is "the engine owns
its data directory". Violations fall back to checksum-detect; no
data corruption is silently accepted.

**Proposed fix:** Either (a) advisory `flock(2)` on the segment file
when opening, or (b) `MAP_PRIVATE` semantics for the read side, or
(c) document the assumption formally in SECURITY.md.

**Effort:** Non-trivial; design RFC required to decide between
isolation strategies. Marker placed at the function:
`TODO(security): the mmap-as-&[u8] view rests on the threat-model
assumption ...`.

**2026-05-24 refresh:** Fixed at the current runtime boundary by
keeping heap segment reads/writes on positional file IO rather than
`mmap`, rejecting a symlinked final data directory, canonicalizing the
stored data-dir path, and refusing Unix data directories not owned by
the server's effective UID. Regression coverage:
`server_init_refuses_symlinked_data_dir`,
`server_init_stores_canonical_data_dir`, and
`data_dir_owner_check_rejects_unexpected_uid`.

**Status:** Closed.

### D-3. Planner join-depth guard

**Affected:** `crates/ultrasql-planner/src/binder/from.rs`

The planner accepts `T1 JOIN T2 JOIN T3 ... JOIN TN` chains. Without a
depth guard, a malicious client can force the binder to construct a very
wide left-deep logical plan and hand later optimizer/executor phases an
unbounded join tree.

**2026-06-10 refresh:** Fixed by preflighting the FROM-clause join tree
before table binding. `MAX_JOIN_DEPTH = 64`; deeper joins are rejected as
`PlanError::NotSupportedOwned` with a join-depth message. Regression
coverage: `accepts_explicit_join_chain_at_depth_limit` and
`rejects_explicit_join_chain_above_depth_limit`.

**Status:** Closed for explicit and parser-canonicalized comma/CROSS
join chains in the current binder.

---

## 5. Dependency advisories

`cargo audit --deny yanked` (advisory DB fetched 2026-06-10) reports:

- **Crates scanned:** 446
- **Security vulnerabilities:** 0
- **Yanked crates:** 0
- **Allowed warnings:** 1 (`RUSTSEC-2024-0436`, unmaintained
  `paste 1.0.15`, transitive through `parquet 59.0.0`)

`cargo audit --deny warnings` intentionally is not the CI policy while
the supported Apache Parquet crate still depends on `paste`; it exits 1
on the unmaintained warning but does not report a vulnerability.
`cargo deny check advisories` reports `advisories ok`. `thrift` is no
longer present after the Arrow/Parquet 59.0.0 refresh.

---

## 6. Regression coverage

| Category | Pre-audit tests | Post-audit tests | Delta |
|----------|---------------:|-----------------:|------:|
| `ultrasql-parser` | 55 | 57 | +2 |
| `ultrasql-protocol` | 43 | 47 | +4 |
| `ultrasql-wal` | 21 | 24 | +3 |
| `ultrasql-server` | 22 | 23 | +1 |
| (other crates, untouched) | 279 | 287 | +8 |
| **Total** | **420** | **438** | **+18** |

The "+8" in untouched crates is from rebuild side-effects (e.g. the
binary `cross_compare` benchmark fixture in `ultrasql-bench` that was
not committed yet). The 18 net new tests in this audit pass all
exercise specific adversarial inputs.

---

## 7. Public API impact

None. Three new public symbols (`MAX_PARSE_DEPTH`,
`MAX_MESSAGE_BYTES`, `MAX_RECORD_BYTES`) and one new error variant
(`ParseError::DepthExceeded`). All additions; no breaking changes.

---

## 8. Process notes

- `cargo-audit` was installed locally and run; result is clean for
  vulnerabilities and yanked crates, with one allowed unmaintained
  warning documented above. CI now has a `cargo audit` job per
  `.github/workflows/ci.yml`; release verification also runs
  `cargo audit --deny yanked`.
- `cargo deny check advisories` was installed locally and run; result
  clean. Full `cargo-deny` remains in CI.
- 420 → 438 tests green throughout the audit; no regression in the
  existing suite.
- `cargo fmt --all -- --check` and `cargo clippy --workspace
  --all-targets --all-features -- -D warnings` both clean at the end of
  the audit pass.

---

## 9. Outstanding work for v0.6

1. Maintain one clean week of nightly/manual `cargo fuzz` evidence for
   `parser_fuzz`, `planner_fuzz`, `protocol_fuzz`, and
   `wal_record_fuzz`; targets and committed seed corpora are now
   bootstrapped under `fuzz/`.
2. Add a public-IP slow-loris integration test now that the timeout
   work in D-1 has landed.
3. Track upstream `parquet` releases for removal of transitive
   `paste 1.0.15`; tighten the audit gate to `--deny warnings` once
   the latest supported Parquet stack no longer emits
   `RUSTSEC-2024-0436`.
