# AGENTS.md — Operating Manual for UltraSQL

This file is the canonical operating manual for everyone who touches UltraSQL:
contributors, maintainers, reviewers, and development tooling.
It exists at the repository root because it is the first document a new
collaborator should read and the last document a maintainer should
forget.

If you change this file, mention it in your PR description. If you find
yourself violating one of these rules, the rule is wrong or your change
is wrong; resolve the conflict, do not ignore it.

---

## 1. Mission

UltraSQL is a from-scratch SQL OLTP+OLAP engine in pure Rust. It aims for
durable storage, a broad SQL surface, MVCC semantics, and wide client
certification. The mission has three legs:

1. **Correctness first.** A database that loses data, returns wrong
   answers, or violates the isolation level it advertises is not useful
   regardless of how fast it runs. Every subsystem ships with a precise
   contract and tests that exercise the contract.
2. **Performance as discipline.** Performance is the product of
   architectural decisions made daily. Every PR that touches a hot path
   measures the hot path. We do not "trust" optimizations.
3. **No marketing prose.** No marketing language in code or
   documentation. No benchmark claims without reproducible scripts. No
   abstractions whose only purpose is appearance.

---

## 2. Project Philosophy

> Built for current hardware: many cores, deep cache hierarchies, wide SIMD
> units, NVMe-class storage, and measurable end-to-end performance.

Every technical decision must be defensible against the following
ordered priorities. When two conflict, the earlier one wins.

1. Correctness — the system never returns a wrong answer or loses data
   it claimed to have stored.
2. Performance — the system uses the host hardware fully and predictably.
3. Maintainability — a contributor with no prior context can read the
   crate's docs and a single source file and understand the subsystem.
4. Scalability — the design accommodates 10× the current load without
   architectural rewrites.
5. Reproducibility — every behavior, including performance, can be
   reproduced by running a recorded command on a recorded host.
6. Observability — every subsystem emits enough tracing and metrics to
   diagnose a production incident without source modifications.

---

## 3. Code Standards

### 3.1 Language and toolchain

- The codebase is Rust. The minimum supported Rust version is recorded in
  `rust-toolchain.toml` and `Cargo.toml`'s `rust-version`. Bumping the
  MSRV is an RFC-level change.
- Edition is 2024. `unsafe_op_in_unsafe_fn` is denied workspace-wide. New
  `unsafe` blocks require a `// SAFETY:` comment justifying every
  invariant the block depends on.
- Clippy runs with `-D warnings` on CI. The `pedantic`, `nursery`, and
  `cargo` lint groups are enabled; pragmatic relaxations live in
  `Cargo.toml` and require a one-line rationale in the PR if you add to
  the list.
- `rustfmt` is enforced. `cargo fmt --all -- --check` must pass before
  the PR merges.

### 3.2 API surface

- Crate-private items use `pub(crate)`, not `pub`. We pay attention to
  the boundary between internal helpers and the documented surface.
- Every public item — module, type, function, trait, constant — has a
  doc comment. The first line is a sentence; the rest describes
  invariants, error conditions, and performance characteristics that
  callers need to know.
- Public types implement `Debug`. Types that participate in maps, sets,
  or hash-based caches implement `Hash + Eq + PartialEq`. Types that
  cross thread boundaries implement `Send + Sync` (and a comment near
  the declaration justifies the bound).

### 3.3 Style

- No `unwrap()` or `expect()` in non-test code unless the panic
  represents a true unrecoverable invariant violation; in that case use
  `expect("describe-the-invariant")` so the panic message names the
  invariant.
- No `as` casts between integer widths. Use `try_into()` and propagate
  the conversion error, or use the explicit `i64::from(u32_value)`
  idiom when widening losslessly.
- Prefer `&[T]` over `&Vec<T>`, `&str` over `&String`, `impl Iterator<Item=T>`
  over `Vec<T>` in returns where allocation is not required.
- Prefer composition over inheritance-flavored trait hierarchies. We are
  not Java.

---

## 4. Performance Rules

See [PERFORMANCE.md](PERFORMANCE.md) for the longer treatment.

1. **Measure before optimizing.** A change motivated by "this looks
   slow" without a profile is not a performance change; it is a refactor
   with unclear intent.
2. **Measure after optimizing.** A "performance improvement" without a
   before/after benchmark is a regression risk. PRs that touch
   benchmarked paths must include the comparison numbers in the
   description.
3. **No fabricated numbers.** Performance numbers appear in the
   repository only when they trace to a reproducible script and a
   recorded host description. The convention is documented in
   [BENCHMARKS.md](BENCHMARKS.md).
4. **Cache-friendly layouts win.** Struct field order, padding, and
   alignment are deliberate. When you add a field to a hot struct,
   reason about which cache line it lands on and document the choice if
   it is not obvious.
5. **Allocation is a cost.** The hot paths of the executor, the buffer
   pool, and the WAL do not allocate. Use `SmallVec`, `ArrayVec`,
   thread-local arenas, or buffer pools as appropriate.
6. **Synchronization is a cost.** Each new shared mutable structure
   needs a written rationale for the chosen synchronization primitive
   (`parking_lot::Mutex`, `RwLock`, `arc-swap`, atomics, lock-free queue).
   Default to the simplest primitive that meets the workload.
7. **SIMD where it pays.** Vectorized kernels live in `ultrasql-vec`.
   Hand-written intrinsics gate on `cfg(target_arch)` (ARM64 SIMD on
   aarch64, AVX2/AVX-512 on x86_64) and ship alongside a portable
   scalar fallback that produces bit-identical output.

---

## 5. Concurrency Expectations

- The server uses Tokio as its async runtime. Connection handlers are
  async; CPU-bound query execution offloads to a dedicated `rayon`-style
  pool to keep the I/O reactor responsive.
- Lock ordering is documented per subsystem. A consumer of two locks
  must acquire them in the documented order or use `try_lock` with a
  bounded retry loop.
- No `tokio::sync::Mutex` in hot paths. Use `parking_lot::Mutex` for
  fast contended paths and async-aware primitives only where the lock is
  held across an `.await`.
- `DashMap` and `arc-swap` are the default choices for shared state.
  Custom lock-free structures require a written justification (a
  benchmark or a contention profile) before they merge.

---

## 6. Memory Safety Expectations

- The default expectation is safe Rust. `unsafe` is permissible when it
  buys measurable performance or expresses something safe Rust cannot
  (raw pointers into a buffer pool frame, FFI to platform APIs, manual
  union packing for tuple headers).
- Every `unsafe` block has a `// SAFETY:` comment naming the invariants
  the block relies on and the caller's responsibility to maintain them.
- Every `unsafe fn` has a `# Safety` documentation section enumerating
  preconditions.
- Miri-clean is a per-crate goal. Crates that pass Miri have a badge in
  their crate README. Crates with FFI explicitly mark which tests are
  Miri-incompatible.

---

## 7. Documentation Requirements

- Each crate has a top-of-file module doc comment explaining its
  responsibility, its layered position, and its public surface in a
  paragraph.
- Each non-trivial type has a doc comment explaining when to use it,
  when not to use it, and the invariants it maintains.
- ARCHITECTURE.md captures the cross-crate design. When you change a
  subsystem's responsibilities or contracts, you update ARCHITECTURE.md
  in the same PR.
- Decisions that cross subsystem boundaries — choice of execution model,
  on-disk format, wire protocol policy — go through the RFC process
  documented in [RFC_PROCESS.md](RFC_PROCESS.md).

---

## 8. Testing Requirements

UltraSQL practices a layered testing pyramid.

1. **Unit tests** live next to the code they test, behind `#[cfg(test)]`.
2. **Property tests** (`proptest`, `quickcheck`) cover serialization
   round-trips, parser/printer fidelity, planner equivalences, and any
   subsystem with an algebraic specification.
3. **Concurrency tests** stress lock-based and lock-free structures
   under contention. The `loom` crate is used for shared-state models.
4. **Deterministic simulation tests** drive the storage and txn layers
   through a virtual clock and a virtual IO layer to reproduce race
   conditions reliably.
5. **Integration tests** in `tests/` exercise multi-crate workflows.
6. **Fuzz targets** (`cargo fuzz`) cover the parser, the wire protocol
   parser, the WAL record decoder, and the planner. Fuzz corpora live
   under `fuzz/corpus/` and are committed.
7. **Regression benchmarks** (`criterion`) gate performance-sensitive
   PRs. The CI compares against a recorded baseline and fails on
   statistically significant regressions.

A PR is not mergeable unless the relevant layer above is exercised. A
parser change without a parser test is not a parser change.

---

## 9. No-Regression Rules

### Performance gate: measured workload leadership

UltraSQL ships performance claims only when committed scripts and raw
artifacts show the engine leading the measured workload on the recorded
host. The pre-push hook enforces local regressions via `regression-gate`;
cross-engine release claims require fresh same-host certification
artifacts. There is no undocumented "trust me" benchmark path.

- Correctness: a passing test never becomes a `#[ignore]` test in the
  same PR that breaks it. Disabling a test requires a tracking issue.
- API stability: changes to public APIs of versioned crates require a
  semver-major bump unless documented as an unstable pre-1.0 surface.

---

## 10. Commit Standards

UltraSQL uses [Conventional Commits](https://www.conventionalcommits.org/).

```
<type>(<scope>): <subject>

<body>

<footer>
```

Allowed types:

- `feat` — a new user-visible feature.
- `fix` — a bug fix.
- `perf` — a measured performance improvement.
- `refactor` — a code change that neither fixes a bug nor adds a feature.
- `docs` — documentation only.
- `test` — tests only.
- `bench` — benchmark changes.
- `ci` — CI and tooling.
- `build` — build system and dependency changes.
- `chore` — repository hygiene.

Scope is a crate name (`storage`, `wal`, `parser`, `executor`, etc.) or
a cross-cutting concern (`workspace`, `lints`).

Subject is imperative, lowercase, under 70 characters, no trailing
period. The body explains *why* in prose, references issues with
`Refs: #N` or `Closes: #N`, and notes any performance numbers in a
`Perf:` line.

Commits are atomic. A commit either compiles and passes tests, or it is
amended before it lands. We do not ship "WIP" commits to `main`.

---

## 11. Review Standards

A reviewer is a co-author of the change. The reviewer's questions are:

1. Is the contract precise? Can a future maintainer follow it without
   reading the implementation?
2. Do the tests exercise the contract, including the failure modes?
3. Does the implementation match the contract, or does it implement
   something subtly different?
4. Are the performance-sensitive paths benchmarked?
5. Are the synchronization primitives the simplest that meet the need?
6. Are the docs up to date with the change?

A reviewer must run `cargo test --workspace` locally on a meaningful
PR. A green CI is necessary but not sufficient.

---

## 12. CI Expectations

GitHub Actions runs on every PR and on a nightly schedule. Workflows live
under `.github/workflows/` and cover format, lint, tests, fuzz, sanitizers,
and benchmarks. A green CI is necessary but not sufficient — contributors
must also run the local gates below before pushing.

Contributors must run locally before pushing:

- `cargo fmt --all -- --check` — format check.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings` — lint.
- `cargo test --workspace --all-features` — unit, integration, property tests.
- `cargo bench --workspace -- --baseline baseline` — regression benchmarks against
  `benchmarks/results/baseline.json`. Regressions >5% on tagged hot paths block
  the commit; include before/after numbers in the PR.
- `cargo fuzz run <target> -- -max_len=1024 -timeout=1` — smoke fuzz locally;
  the nightly CI job runs a longer 900 s session per target.
- `cargo +nightly miri test --crate <crate>` — Miri on `unsafe`-heavy crates.

Reviewers verify locally with the same commands. CI catching something a
reviewer missed is a bug in the review, not a CI success.

---

## 13. Guidance for Development Tools

- **Read before you write.** When you touch a crate, read its `lib.rs`,
  its public API surface, and the relevant section of ARCHITECTURE.md.
- **Prefer exact code references.** When you mention a function in
  prose, link it as `crate/src/module.rs:line` so a reviewer can jump
  there in one click.
- **Do not invent APIs.** If a function you want does not exist, either
  add it with a contract and tests, or use the existing surface.
- **Do not invent benchmark numbers.** Performance claims must come
  from a recorded benchmark run.
- **Atomic, descriptive commits.** Multiple unrelated edits in one
  commit are a review burden; split them.
- **Do not add tool attribution.** Do not add `Co-authored-by`,
  `Generated-by`, contributor-list entries, AUTHORS entries, or package
  metadata crediting automation or service accounts.
- **Ask before destructive operations.** Branch deletion, force pushes,
  schema migrations, and anything affecting shared infrastructure
  require explicit human approval.
- **Document tradeoffs.** When a design has tradeoffs (and most do),
  spell them out in the PR description or in a comment near the code.
- **Push back when the request is wrong.** "Make this faster" is not a
  task description; "the hash-aggregate regressed 20% on TPC-H Q1 on M4,
  here is the flamegraph" is.

---

## 14. Project Layout (quick reference)

```text
ultrasql/
├── Cargo.toml                     workspace manifest with shared lints
├── README.md                      public-facing overview
├── AGENTS.md                      this file
├── ARCHITECTURE.md                cross-crate design
├── PERFORMANCE.md                 performance engineering rulebook
├── BENCHMARKS.md                  benchmark methodology
├── ROADMAP.md                     shipping plan
├── CONTRIBUTING.md                contributor guide
├── SECURITY.md                    vulnerability disclosure
├── RFC_PROCESS.md                 design change process
├── GOVERNANCE.md                  project governance
├── CODE_OF_CONDUCT.md             behavior expectations
├── crates/
│   ├── ultrasql-core/             foundational types, errors, datums
│   ├── ultrasql-storage/          pages, buffer pool, heap, B+ tree
│   ├── ultrasql-wal/              write-ahead log
│   ├── ultrasql-mvcc/             visibility, snapshots
│   ├── ultrasql-txn/              transaction manager, locking
│   ├── ultrasql-parser/           lexer, parser, AST
│   ├── ultrasql-planner/          binder, logical plans
│   ├── ultrasql-optimizer/        cost-based optimizer
│   ├── ultrasql-executor/         physical execution
│   ├── ultrasql-vec/              vectorized kernels
│   ├── ultrasql-catalog/          system catalog
│   ├── ultrasql-protocol/         wire protocol v3
│   ├── ultrasql-server/           ultrasqld binary
│   ├── ultrasql-cli/              ultrasql interactive client
│   └── ultrasql-bench/            benchmark harness binary
├── benchmarks/                    reproducible benchmark assets and results
├── docs/                          user-facing documentation
├── fuzz/                          cargo-fuzz targets and corpus
├── tests/                         workspace-wide integration tests
└── .github/workflows/             CI definitions
```

---

## 15. Glossary

A handful of terms come up often enough to define here. Subsystem-
specific glossaries live in the relevant crate's docs.

- **OID** — object identifier; a 32-bit ID assigned to each catalog
  entity. Stable for the life of the entity.
- **XID** — transaction identifier; UltraSQL uses a 64-bit XID to avoid
  32-bit wraparound problems.
- **LSN** — log sequence number; a monotonically increasing 64-bit
  position in the WAL.
- **Page** — an 8 KiB unit of on-disk and in-memory storage. The buffer
  pool tracks pages by `PageId`.
- **Tuple** — a row of a relation. Tuples carry MVCC headers
  (`xmin`/`xmax`/`cmin`/`cmax`) and live inside pages.
- **Batch** — a column-oriented unit of execution. A batch holds up to
  4 096 rows; it is the input/output unit of vectorized operators.
- **Pipeline** — a chain of operators with no materialization between
  them. The optimizer splits a plan into pipelines separated by
  pipeline-breakers (sorts, hash builds, aggregates).
- **Catalog snapshot** — an immutable, MVCC-consistent view of the
  system catalog used by a single statement.

---

This document is living. When the project's invariants change, this
document changes with them.
