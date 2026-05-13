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
