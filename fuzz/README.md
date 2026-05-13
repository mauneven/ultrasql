# UltraSQL Fuzz Targets

This directory contains [cargo-fuzz](https://github.com/rust-fuzz/cargo-fuzz) targets
for UltraSQL. Fuzzing requires a nightly Rust toolchain; ordinary `cargo test` does
not touch this crate.

## Prerequisites

```sh
rustup toolchain install nightly
cargo install cargo-fuzz
```

## Available targets

| Target | Contract |
|---|---|
| `parser_fuzz` | `Parser::parse_statements` never panics on any input |

## Running

```sh
# Indefinite run (Ctrl-C to stop)
cargo +nightly fuzz run parser_fuzz

# Bounded run (60 s)
cargo +nightly fuzz run parser_fuzz -- -max_total_time=60

# Minimize a crash reproducer
cargo +nightly fuzz tmin parser_fuzz artifacts/parser_fuzz/<crash-file>
```

## Corpus

Seed inputs live in `corpus/parser_fuzz/`. They cover a representative slice of the
SQL grammar: DDL, DML, CTEs, set operations, expressions, malformed inputs, and
Unicode. cargo-fuzz merges new interesting inputs into this directory automatically.

## Crash reproducers

Any crash found during fuzzing is written to `artifacts/parser_fuzz/`. To reproduce:

```sh
cargo +nightly fuzz run parser_fuzz artifacts/parser_fuzz/<crash-file>
```

Commit crash reproducers alongside a bug fix so they become permanent regression tests.

## ROADMAP gate

ROADMAP §"v0.2 — Planner updates" requires `parser_fuzz` to be 24h CI-clean before
v0.2 can ship. The pre-push hook runs a 15-second smoke of this target when a nightly
toolchain is available.

## Structure

```text
fuzz/
├── Cargo.toml                    standalone crate (excluded from workspace)
├── README.md                     this file
├── fuzz_targets/
│   └── parser_fuzz.rs            fuzz entry point
├── corpus/
│   └── parser_fuzz/              seed SQL inputs (committed)
└── artifacts/
    └── parser_fuzz/              crash reproducers (not committed; gitignored)
```
