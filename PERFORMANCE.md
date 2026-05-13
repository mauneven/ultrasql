# UltraSQL Performance Engineering Rulebook

Performance in UltraSQL is not a quality we optimize at the end. It is a
property of every decision, every line of code, every dependency. This
document codifies the rules.

The rules are deliberately strict. Database engines are unforgiving:
allocation in a hot path, a redundant cache miss, a misaligned struct,
a careless `Vec::push` inside a loop — any of these takes the engine
from "fast" to "merely competent."

---

## 1. The Hierarchy of Performance Concerns

When in doubt, address concerns in this order. A higher-numbered fix
applied while a lower-numbered concern remains broken is wasted work.

1. **Algorithmic complexity.** O(N²) inside a query is a bug, not a
   performance issue. Profile first; replace the algorithm before
   touching constants.
2. **I/O volume.** Reads and writes that should not happen are the
   biggest performance bug in any database. The right index, the right
   projection, the right join order saves orders of magnitude more
   than any micro-optimization.
3. **Cache behavior.** Once I/O is correct, the CPU is the bottleneck.
   L1/L2/L3 misses, TLB misses, and branch mispredictions dominate.
4. **Synchronization.** Lock contention, false sharing, atomic
   contention, and async wakeup costs are next.
5. **Allocation.** Heap allocations in hot paths are the easiest
   self-inflicted wound. Eliminate them.
6. **SIMD.** Wide-vector kernels are the final layer. They are worth
   doing only after 1 – 5 are honest.

---

## 2. The Profiling Discipline

You cannot improve what you do not measure. We measure with the
following tools, in order of frequency.

- **`cargo bench` (criterion).** Every PR that claims a performance
  improvement runs the relevant criterion benchmarks before and after.
  The PR description quotes both numbers.
- **`samply`** on macOS, **`perf`** on Linux. Sampling profilers
  produce flame graphs that show the actual hot paths.
- **`dtrace`** on macOS, **`bpftrace`** on Linux. For latency-tail
  investigation, sampling is not enough; tracing is.
- **`flamegraph-rs`.** For ad-hoc visualization of a single workload.
- **`heaptrack`**, **`bytehound`**, or `dhat-rs`. For allocation
  profiling.
- **`Instruments`** on macOS. The Time Profiler and System Trace
  templates are first-class tools on the M4 Mac mini host.

A change motivated by "this looks slow" without one of these in hand
is a refactor, not a performance change. Refactors are fine, but they
are reviewed on different criteria.

---

## 3. Allocation Rules

- The executor hot path does not allocate. Any operator's
  `next_batch()` call that allocates anything other than its output
  batch is a bug.
- The buffer pool does not allocate after warm-up. Frames are
  pre-allocated; the page table grows only when shards rehash, which
  is a one-time event per shard.
- The WAL fast path does not allocate. Records are written into a
  pre-allocated ring buffer.
- The parser allocates the AST and tokens once per statement. Token
  buffers are pooled by connection.
- Operators that need scratch space (hash tables, sort buffers, run
  arrays) take a `BumpAllocator` reference from the per-query memory
  pool. The pool resets at end-of-query.

When you need a small, bounded buffer, use `SmallVec` or `ArrayVec`.
When you need a larger buffer with known maximum size, use a fixed-
capacity `Box<[T; N]>` or a pre-allocated `Vec<T>` that you reuse.
When you need a buffer of unknown size in a hot path, you have a
design problem; fix the design.

---

## 4. Cache and Layout

- Struct fields are ordered to minimize padding. The convention is
  largest-first within each cache line. Add `#[repr(C)]` and a
  `static_assertions::assert_eq_size!` block when the struct is
  performance-sensitive.
- Hot 64-byte structs are aligned to a cache line via
  `#[repr(align(64))]`. The `cache_padded!` helper in
  `ultrasql-core` wraps types that need padding (e.g., per-shard
  counters).
- Avoid false sharing. Multi-producer counters and lock guards must
  not share a cache line with unrelated state.
- Prefer arrays of structures or structures of arrays based on the
  access pattern. Vectorized executors use SOA; OLTP heap tuples use
  AOS.
- Use `[u8; N]` over `Vec<u8>` for fixed-size buffers. The compiler
  produces better loops on arrays.

---

## 5. Synchronization

Picking a synchronization primitive is a design decision, not a habit.
The defaults are:

| Need                                                       | Primitive                              |
| ---------------------------------------------------------- | -------------------------------------- |
| Per-thread state, no sharing                               | `thread_local!`                        |
| Rarely contended mutual exclusion                          | `parking_lot::Mutex`                   |
| Many readers, occasional writer                            | `parking_lot::RwLock` or `arc_swap`    |
| Shared state read often, swapped wholesale on update       | `arc_swap::ArcSwap`                    |
| Map-style shared state with high concurrency               | `dashmap::DashMap`                     |
| Single-producer single-consumer queue                      | `crossbeam_queue::ArrayQueue`          |
| Multi-producer multi-consumer queue                        | `crossbeam_queue::SegQueue`            |
| Atomic counter / flag                                      | `std::sync::atomic::*`                 |
| Async coordination across `.await`                         | `tokio::sync::Mutex` / `RwLock`        |

If you reach for a different primitive, the PR description explains
why and quotes a benchmark.

`tokio::sync::Mutex` is permitted only when a guard must be held
across an `.await`. For CPU-bound paths use `parking_lot::Mutex`.

---

## 6. Async vs Sync

UltraSQL's connection layer is async (Tokio). The query execution
layer is synchronous CPU work. The split is deliberate:

- The Tokio reactor handles I/O readiness. Connection accept, message
  parse, and protocol response are async.
- Once a query is dispatched, it executes on a worker thread (the
  `rayon` pool by default). The worker is synchronous; the future
  awaiting the result yields back to the reactor only on completion.
- This split keeps the reactor responsive and the executor cache-hot.
  Mixing async and CPU-bound work in the same task is the most common
  cause of bad tail latencies in Rust database servers.

---

## 7. SIMD Rules

- Kernels live in `ultrasql-vec`.
- Every kernel has a scalar implementation that is the source of
  truth. SIMD paths are validated bit-for-bit against scalar in
  property tests.
- `cfg(target_arch = "aarch64")` for ARM64 SIMD paths,
  `cfg(target_arch = "x86_64")` for AVX2/AVX-512. Use
  `std::arch::is_aarch64_feature_detected!` /
  `is_x86_feature_detected!` to feature-gate at runtime if needed.
- Hand-written intrinsics use `unsafe` with a `// SAFETY:` block per
  call site. The kernel module's `lib.rs` summarizes the invariants
  the kernels uphold.
- Auto-vectorization is welcome. When the compiler is already producing
  good code, do not add intrinsics. Verify by reading the assembly
  (`cargo asm`, `cargo show-asm`).

---

## 8. Benchmark Discipline

This section overlaps with [BENCHMARKS.md](BENCHMARKS.md); read that
file for the methodology. The rules that belong here:

- Every published number is a *measurement*, not a *claim*.
- "Faster than PostgreSQL" without a configuration and a host
  description is a marketing statement, not an engineering one.
- Comparison benchmarks tune the competitor. We do not benchmark
  PostgreSQL with `shared_buffers = 128MB` against ourselves with a
  4 GB buffer pool and call it a win.
- Microbenchmarks measure microseconds. Macrobenchmarks measure
  workloads. Both are necessary; neither substitutes for the other.

---

## 9. Regression Policy

- The `bench` job in CI runs the criterion suite and compares against
  the baseline tracked in `benchmarks/results/baseline.json`.
- A regression of more than 5% on a tagged hot path fails CI.
- A regression of 1 – 5% emits a warning, blocks default merge, and
  may be merged with a maintainer override and a tracking issue.
- The baseline is rolled forward on a cadence (default: every two
  weeks) by the maintainer team. Rolling forward requires that all
  outstanding regressions in the period have been investigated or
  reverted.

---

## 10. Hardware Targets

UltraSQL is built and tuned on the following primary host:

- **Apple Mac mini, M4, 24 GB unified memory, internal NVMe.**

CI also exercises:

- Linux x86_64, AMD EPYC Genoa class (target-cpu `x86-64-v3`).
- Linux ARM64, AWS Graviton 4 class (target-cpu `neoverse-n2`).

When the M4 figures disagree with the AMD or Graviton figures, the
divergence is logged in `benchmarks/results/divergences.md` and a
maintainer investigates. We do not silently regress on platforms we do
not run on daily.

---

## 11. The Two-Hour Rule

If a performance investigation has burned two hours without producing
either a fix or a clearly documented next step, write down what you
have learned, file an issue, and ask for help. UltraSQL is built by
many hands; the worst outcome is silently failing on the same path
twice. The right outcome is a shared performance log.

---

This rulebook is a living document. Send PRs.
