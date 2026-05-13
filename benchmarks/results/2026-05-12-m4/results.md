# UltraSQL — Microbenchmark Results (2026-05-12)

**Host:** Apple Mac mini, M4, 10 cores, 16 GiB unified RAM, internal
NVMe. macOS 26.5, Darwin 25.5.0.
**Build:** `cargo bench` (release + LTO=fat + codegen-units=1),
`target-cpu=apple-m1`.
**Tool:** criterion 0.5, 100 samples per bench, 2 s measurement
window, 1 s warm-up.
**Commit:** `373bb69e3264bca51ed0b1c57c8784109a815d90`.

These are **microbenchmarks**. They measure isolated kernel latency
and throughput. They do **not** measure end-to-end query workload —
UltraSQL has no working query pipeline yet (see
[ROADMAP.md](../../../ROADMAP.md)). Comparisons against PostgreSQL,
MySQL, SQLite, DuckDB, and ClickHouse are blocked on `v0.5` of the
roadmap.

Reported numbers are the median of 100 samples. Each row's `±` shows
the criterion 95% confidence interval.

---

## Vectorized kernels (`ultrasql-vec`)

| Kernel              |    Rows | Median time | Throughput          |
| ------------------- | ------: | ----------: | ------------------- |
| `eq_i32`            |      64 |    36.5 ns  | 1.75 Gelem/s        |
| `eq_i32`            |   1 024 |    893 ns   | 1.15 Gelem/s        |
| `eq_i32`            |   4 096 |    3.67 µs  | 1.12 Gelem/s        |
| `eq_i32`            |  65 536 |    58.4 µs  | 1.12 Gelem/s        |
| `sum_i64`           |      64 |    3.71 ns  | 17.24 Gelem/s       |
| `sum_i64`           |   1 024 |    57.7 ns  | 17.75 Gelem/s       |
| `sum_i64`           |   4 096 |   249.8 ns  | 16.40 Gelem/s       |
| `sum_i64`           |  65 536 |    4.70 µs  | **13.94 Gelem/s**   |
| `min_f64`           |      64 |    64.5 ns  | 991 Melem/s         |
| `min_f64`           |   1 024 |   1.01 µs   | 1.01 Gelem/s        |
| `min_f64`           |   4 096 |   4.05 µs   | 1.01 Gelem/s        |
| `min_f64`           |  65 536 |   67.3 µs   | 974 Melem/s         |
| `select_i32` (50%)  |      64 |    31.0 ns  | 2.06 Gelem/s        |
| `select_i32` (50%)  |   1 024 |    263 ns   | 3.89 Gelem/s        |
| `select_i32` (50%)  |   4 096 |    999 ns   | 4.10 Gelem/s        |
| `select_i32` (50%)  |  65 536 |   15.6 µs   | 4.20 Gelem/s        |

Notes:
- `sum_i64` saturates the M4's NEON wide-vector add path; LLVM auto-
  vectorizes the scalar fold.
- `eq_i32` is bottlenecked on the bitmap-write path, not the
  comparison. A future SIMD-mask kernel that writes 64 lanes at a
  time will close most of the gap to `sum_i64`.
- `min_f64` carries an extra branch per lane for NaN/null skip; a
  branch-free variant is on the roadmap.

## Page format (`ultrasql-storage`)

| Operation                       |   Median time | Throughput          |
| ------------------------------- | ------------: | ------------------- |
| `page/insert` (16-byte tuples)  |     35.7 µs/page-fill | 439 KiB/s   |
| `page/insert` (64-byte tuples)  |     3.75 µs/page-fill | 16.3 MiB/s  |
| `page/insert` (256-byte tuples) |    441 ns/page-fill   | 553 MiB/s   |
| `page/insert` (1024-byte tuples)|    203 ns/page-fill   | 4.69 GiB/s  |
| `page/read` (scan all slots)    |    128 ns/page        | 936 Melem/s |
| `page/refresh_checksum` (8 KiB) |    269 ns/page        | ≈ 29 GiB/s  |

Notes:
- The `page/insert` benchmark fills one entire page until insertion
  fails, then drops the page; the row is "median time to fill a
  page", which is why small tuple sizes (more inserts per page) take
  longer per page.
- `refresh_checksum` is xxh3-64 truncated to 32 bits over 8 KiB; at
  ≈ 29 GiB/s, the checksum is comfortably faster than any practical
  NVMe write path.

## Buffer pool (`ultrasql-storage`)

| Operation                              |  Median time | Throughput          |
| -------------------------------------- | -----------: | ------------------- |
| `hot_pin` (single page, repeated pin)  |    12.7 ns/op | 79.0 Mops/s         |
| `cycle` (64 pages, 64-frame pool)      |    1.06 µs   | 60.5 Mops/s         |
| `cycle` (256 pages, 64-frame pool)     |    73.5 µs   | 3.48 Mops/s         |

Notes:
- The hot-pin path is fully resident: every access is a hit on the
  CLOCK-cached page. 12.7 ns is one acquire on the per-frame
  reference bit, one fetch_add on the pin counter, and one DashMap
  lookup.
- The 256-page-through-64-frame-pool case forces continuous
  eviction. The CLOCK hand rotates and the slow-path eviction
  serializes through the shard lock. 3.48 Mops/s under churn is the
  current measured ceiling; a CLOCK-Pro upgrade and a fast-path
  reservation queue are on the optimization list.

## WAL (`ultrasql-wal`)

| Operation                            | Median time | Throughput        |
| ------------------------------------ | ----------: | ----------------- |
| `wal/encode` (0-byte payload header) |    20.6 ns  | (header-only)     |
| `wal/encode` (64-byte payload)       |    19.1 ns  | 3.13 GiB/s        |
| `wal/encode` (256-byte payload)      |    21.7 ns  | 10.98 GiB/s       |
| `wal/encode` (1 024-byte payload)    |    36.0 ns  | 26.5 GiB/s        |
| `wal/encode` (4 096-byte payload)    |   107 ns    | **35.6 GiB/s**    |
| `wal/decode` (64-byte payload)       |    48.5 ns  | 1.23 GiB/s        |
| `wal/decode` (256-byte payload)      |    62.6 ns  | 3.81 GiB/s        |
| `wal/decode` (1 024-byte payload)    |   121 ns    | 7.89 GiB/s        |
| `wal/decode` (4 096-byte payload)    |   323 ns    | **11.80 GiB/s**   |
| `wal/buffer_append` (64-byte rec)    |    57.9 ns  | 1.03 GiB/s        |
| `wal/buffer_append` (256-byte rec)   |    61.6 ns  | 3.87 GiB/s        |
| `wal/buffer_append` (1 024-byte rec) |    96.1 ns  | 9.92 GiB/s        |

Notes:
- Encode includes the full CRC32C pass over the entire record (with
  the CRC slot treated as zero).
- Decode is slower than encode because it allocates the payload
  `Vec<u8>` separately and runs the CRC verification pass. A
  zero-copy decode is feasible and tracked.
- `wal/buffer_append` is the full `WalBuffer::append` path: encode +
  mutex acquire + memcpy into the staging buffer + LSN
  assignment. At 96 ns / 1 KiB record, the in-memory WAL ceiling is
  ≈ 10 M records/s on a single producer; persisted throughput will
  be gated by fsync latency once the segment-flusher lands.

---

## What this is, and what it isn't

**Is**: per-kernel CPU latency and throughput on a known build of a
known commit on a known host. Reproducible: every input on this page
maps to a committed bench file under `crates/*/benches/*.rs`. Re-run
with `cargo bench --bench <name>` after pinning the commit.

**Isn't**: a competitive workload claim. There is no SELECT pipeline
yet, no wire protocol, no segment-file I/O. We cannot run TPC-B,
TPC-C, TPC-H, or any sysbench-style workload against UltraSQL.
Comparison numbers will appear once `v0.5` lands.

We will not publish "ultrasql vs postgres" numbers until the
comparison is fair. The bar is the methodology in
[BENCHMARKS.md](../../../BENCHMARKS.md), and we will meet it.

---

## 2026-05-12 speedup round

The original table above is a historical snapshot at commit
`4675f4b` and is not edited. The round documented here covers the
three kernel changes landed on 2026-05-12 (commits `f6447df`,
`cc57037`, `95174db`). Same host, same build profile, same
criterion configuration as the original snapshot.

Raw criterion output: see
[`speedup-round.txt`](./speedup-round.txt). Each `change` line in
that file is criterion's measured delta versus the saved
`old_pre_speedup` baseline taken from the pre-edit `main`.

### Vectorized kernels (`ultrasql-vec`) — after

3 s measurement / 1 s warm-up, 100 samples. The same run is
reproduced in `speedup-round.txt`.

| Kernel              |    Rows | Median time | Throughput          | vs. baseline       |
| ------------------- | ------: | ----------: | ------------------- | ------------------ |
| `eq_i32`            |      64 |   19.57 ns  | 3.27 Gelem/s        | +91% (1.75 → 3.27) |
| `eq_i32`            |   1 024 |   72.9 ns   | 14.05 Gelem/s       | +1 157% (×12.6)    |
| `eq_i32`            |   4 096 |  262.9 ns   | 15.58 Gelem/s       | +1 295% (×14.0)    |
| `eq_i32`            |  65 536 |    4.62 µs  | **14.19 Gelem/s**   | +1 172% (×12.7)    |
| `sum_i64`           |      64 |    3.70 ns  | 17.36 Gelem/s       | ≈ flat (-1%)       |
| `sum_i64`           |   1 024 |   55.9 ns   | 18.34 Gelem/s       | ≈ flat (+2%)       |
| `sum_i64`           |   4 096 |  246.7 ns   | 16.62 Gelem/s       | ≈ flat (+1%)       |
| `sum_i64`           |  65 536 |    4.64 µs  | 14.13 Gelem/s       | ≈ flat (+1%)       |
| `min_f64`           |      64 |    9.03 ns  | 7.09 Gelem/s        | +583% (×6.8)       |
| `min_f64`           |   1 024 |  155.6 ns   | 6.58 Gelem/s        | +589% (×6.9)       |
| `min_f64`           |   4 096 |  620.3 ns   | 6.60 Gelem/s        | +534% (×6.5)       |
| `min_f64`           |  65 536 |   9.81 µs   | **6.68 Gelem/s**    | +574% (×6.7)       |
| `select_i32` (50%)  |      64 |   30.61 ns  | 2.09 Gelem/s        | ≈ flat (+1%)       |
| `select_i32` (50%)  |   1 024 |  254.8 ns   | 4.02 Gelem/s        | ≈ flat (+8%)       |
| `select_i32` (50%)  |   4 096 |  963.5 ns   | 4.25 Gelem/s        | ≈ flat (+5%)       |
| `select_i32` (50%)  |  65 536 |   15.38 µs  | 4.26 Gelem/s        | ≈ flat (+1%)       |

Targets vs. delivered:

- `eq_i32 / 65 536`: target ≥ 5× → delivered **12.7×** (1.12 →
  14.19 Gelem/s).
- `min_f64 / 65 536`: target ≥ 3× → delivered **6.7×** (974 Melem/s
  → 6.68 Gelem/s).
- No other kernel regressed beyond criterion's noise band.

Notes:

- `eq_i32` non-null fast path processes 64 lanes per iteration and
  writes the packed `u64` mask word directly into `Bitmap`'s
  backing buffer (no per-row read-modify-write). On `aarch64` we
  use NEON intrinsics (`vceqq_s32` + powers-of-two AND + horizontal
  add) because LLVM's autovectorizer did not lower the shift-deposit
  shape on `apple-m1`; the `.s` disassembly was inspected. Property
  tests fuzz the SIMD path against an `eq_i32_scalar` reference.
- `min_f64` keeps four parallel `f64::min` accumulators seeded with
  `INFINITY`; LLVM lowers this to a four-deep `fminnm.d` unroll on
  M-series. Autovectorization was sufficient; no hand intrinsics.
- The null-aware variants (`min_f64_nullable`, `eq_i32` null path)
  remain branched per row because garbage in null slots cannot
  participate in a branch-free fold. Both are documented as
  fall-throughs of the fast path.

### Buffer pool (`ultrasql-storage`) — after

| Operation                              |  Median time | Throughput          | vs. baseline |
| -------------------------------------- | -----------: | ------------------- | ------------ |
| `hot_pin` (single page, repeated pin)  |    12.57 ns/op | 79.6 Mops/s       | -1.8% time (≈ noise) |

The `clock_ref` store in the hit path moved from `Release` to
`Relaxed` ordering. The bit is purely advisory for the CLOCK hand
(a torn read just costs one extra rotation, bounded by the
`capacity * 4` sweep cap); the pin counter's `AcqRel` carries the
real happens-before edge with eviction.

### Tests

`cargo test --workspace --release` is green at **440 tests** (was
400 pre-round). The +40 comes from the new unit tests
(`Bitmap::from_words` / `words_mut` happy-path + panic case), the
deterministic boundary-length sweeps for `eq_i32` / `min_f64`, and
two pairs of proptest cases (each runs 64 random inputs) that
cross-validate the SIMD path against its scalar reference. Clippy
remains clean at workspace level under `-D warnings` with
`pedantic + nursery + cargo` enabled.
