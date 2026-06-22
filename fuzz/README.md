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
| `planner_fuzz` | Parser + binder/planner lowering either succeeds or returns typed errors without panics |
| `protocol_fuzz` | Wire-protocol decoders parse, request more bytes, or return typed protocol errors |
| `wal_record_fuzz` | WAL record decoder accepts valid payloads or returns typed decode errors |

## Running

```sh
# Indefinite run (Ctrl-C to stop)
cargo +nightly fuzz run parser_fuzz

# Bounded run (60 s)
cargo +nightly fuzz run parser_fuzz -- -max_total_time=60

# Run every release-gate target with the nightly CI budget
for target in parser_fuzz planner_fuzz protocol_fuzz wal_record_fuzz; do
  cargo +nightly fuzz run "$target" -- -max_total_time=900 -rss_limit_mb=4096 -max_len=1024
done

# Minimize a crash reproducer
cargo +nightly fuzz tmin parser_fuzz artifacts/parser_fuzz/<crash-file>
```

## Corpus

Seed inputs live under `corpus/<target>/`. Parser seeds cover a representative
slice of the SQL grammar: DDL, DML, CTEs, set operations, expressions, malformed
inputs, and Unicode. Protocol and WAL seeds cover decoder edge shapes,
including truncated or malformed payloads. cargo-fuzz merges new interesting
inputs into this directory automatically.

## Crash reproducers

Any crash found during fuzzing is written to `artifacts/<target>/`. To reproduce:

```sh
cargo +nightly fuzz run <target> artifacts/<target>/<crash-file>
```

Commit crash reproducers alongside a bug fix so they become permanent regression tests.

## Release gate

The v1.0 release gate requires parser, planner, protocol, and WAL decoder fuzz
targets to record one clean week of nightly or manual CI evidence. Pull requests
run a 60-second smoke for changed fuzz surfaces; scheduled and manual workflows
run 900-second sessions for every target.

## Structure

```text
fuzz/
├── Cargo.toml                    standalone crate (excluded from workspace)
├── README.md                     this file
├── fuzz_targets/
│   ├── parser_fuzz.rs            parser fuzz entry point
│   ├── planner_fuzz.rs           parser + binder fuzz entry point
│   ├── protocol_fuzz.rs          wire codec fuzz entry point
│   └── wal_record_fuzz.rs        WAL record decoder fuzz entry point
├── corpus/
│   ├── parser_fuzz/              seed SQL inputs (committed)
│   ├── planner_fuzz/             planner seed SQL inputs (committed)
│   ├── protocol_fuzz/            wire-protocol seeds (committed)
│   └── wal_record_fuzz/          WAL record seeds (committed)
└── artifacts/
    └── <target>/                 crash reproducers (not committed; gitignored)
```
