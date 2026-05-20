# AGENTS.md — Operating Manual for UltraSQL

This file is the canonical operating manual for everyone — development tools,
human contributors, maintainers, and reviewers — who touches UltraSQL.
It exists at the repository root because it is the first document a new
collaborator should read and the last document a maintainer should
forget.

If you change this file, mention it in your PR description. If you find
yourself violating one of these rules, the rule is wrong or your change
is wrong; resolve the conflict, do not ignore it.

---

## 1. Mission

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
  task description; "the hash-aggregate is 20% slower than DuckDB at
  TPC-H Q1 on M4, here is the flamegraph" is.

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
│   ├── ultrasql-protocol/         PostgreSQL wire protocol v3
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
  the PostgreSQL wraparound problem.
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
