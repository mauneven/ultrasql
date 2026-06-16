# Getting Started

UltraSQL is alpha. Use this guide for local development, SQL testing,
compatibility checks, and benchmark reproduction. Do not treat this as a
production install guide until the v1.0 release gates in `ROADMAP.md` are
closed.

## Install a release archive

macOS and Linux:

```bash
curl -fsSL https://raw.githubusercontent.com/mauneven/ultrasql/main/scripts/install.sh | sh
```

Windows PowerShell:

```powershell
iwr https://raw.githubusercontent.com/mauneven/ultrasql/main/scripts/install.ps1 -UseB | iex
```

See `docs/install.md` for checksum verification and manual archive install.

## Build from source

```bash
git clone https://github.com/mauneven/ultrasql.git
cd ultrasql
git config core.hooksPath .githooks
cargo build --locked --profile release-ship --bin ultrasqld --bin ultrasql --bin ultrasql-local
```

## Run locally

Start the server:

```bash
cargo run --release --bin ultrasqld
```

Connect with the UltraSQL CLI or any supported SQL client that only uses the
currently certified surface:

```bash
cargo run --release --bin ultrasql -- "host=127.0.0.1 port=5432 user=ultrasql"
```

Basic smoke:

```sql
CREATE TABLE docs_smoke (id INT NOT NULL, body TEXT);
INSERT INTO docs_smoke VALUES (1, 'hello'), (2, 'ultrasql');
SELECT COUNT(*), STRING_AGG(body, ',') FROM docs_smoke;
```

## Run tests

Fast local gate:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
```

SQLLogicTest smoke:

```bash
cargo run -p ultrasql-sqllogictest-runner -- tests/slt/ultrasql_specific
```

## Run benchmarks

Benchmark claims must come from recorded scripts and artifacts:

```bash
benchmarks/certify.sh smoke
cargo run --package ultrasql-bench --bin readme-render
```

Full release benchmark certification is slower and belongs to the manual or
nightly gate documented in `docs/release-checklist.md`.
