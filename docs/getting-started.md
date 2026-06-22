# Getting Started

UltraSQL is alpha. Use this guide for local development, SQL testing,
compatibility checks, and benchmark reproduction. Do not treat this as a
production install guide until the v1.0 release gates in `TODO.md` are
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

## 60 seconds to your first hybrid vector query

Once `ultrasqld` is built, you are querying vectors in well under a second. The
one-time build is the only slow part:

```bash
cargo build --release --bin ultrasqld   # one-time
scripts/quickstart-vector.sh            # boots a server, ingests, ranks
```

The script creates a table whose columns are `body TEXT`, `embedding VECTOR(3)`,
and `metadata JSONB`, ingests three rows in one transaction, and runs a single
statement that ranks them by fused vector + BM25 relevance under a tenant filter:

```
 id |                body
----+-------------------------------------
  2 | invoice payment failed for customer
  1 | payment retry succeeded
  3 | user updated profile photo
(3 rows)

Zero to first hybrid vector result in 0.2s (server boot + ingest + query).
```

The result is ranked, not id-ordered — doc 2 wins on both the embedding and the
text. It needs only a built binary and a `psql` client; for a zero-dependency
embedded version (Node, surviving a restart) see
[`examples/node-rag/`](../examples/node-rag/). The vector path is exercised under
sustained concurrent load and a mid-run crash by
`benchmarks/vector_soak.sh` (see [Run benchmarks](#run-benchmarks)).

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

Vector-specific harnesses:

```bash
# recall@k vs latency on SIFT vs pgvector / Qdrant / LanceDB (same host)
SIFT_DATASET=siftsmall benchmarks/vector_ann_sift.sh
# sustained concurrent ANN load + a hard crash + recovery (durability + recall)
benchmarks/vector_soak.sh smoke
```

Full release benchmark certification is slower and belongs to the manual or
nightly gate documented in `docs/release-checklist.md`.
