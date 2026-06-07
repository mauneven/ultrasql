//! `insert_throughput` and `insert_throughput_10k` benchmark implementation.
//!
//! Each measured iteration:
//! 1. Creates a fresh in-memory `BufferPool` + `HeapAccess`.
//! 2. Inserts `ROWS_PER_ITER` rows of `(i32 id, i64 val)` into a new
//!    relation.
//!
//! Throughput = `ROWS_PER_ITER / median_elapsed_seconds`.
//!
//! The pool-creation cost is included in the timed region because it
//! corresponds to `CREATE TABLE` cost and is small compared to the
//! inserts at 10 000 rows.
//!
//! # Setup / run split
//!
//! `setup` allocates a fresh pool and inserts `n` rows; the result is
//! kept alive in `SetupState` so tests can call `HeapAccess::scan_visible`
//! to verify the post-state.  `run_one_iter` times a *fresh* pool + insert
//! batch per call — this matches the benchmark's "empty table" signal.
//! `run` calls both in the right order.

use std::sync::Arc;
use std::time::{Duration, Instant};

use ultrasql_core::{CommandId, PageId, RelationId, Xid};
use ultrasql_storage::buffer_pool::{BufferPool, PageLoader};
use ultrasql_storage::heap::{HeapAccess, InsertOptions};
use ultrasql_storage::page::Page;

use crate::registry::{BenchContext, BenchResult, median_f64, p99_f64, require_bench_ok};

/// Full production rows per iteration.
#[cfg(not(test))]
const PROD_ROWS_PER_ITER: usize = 10_000;

/// Reduced rows per iteration for fast unit tests.
#[cfg(test)]
const TEST_ROWS_PER_ITER: usize = 32;

/// Smoke-mode rows per iteration (used when `ULTRASQL_BENCH_SMOKE` is set).
#[cfg(not(test))]
const SMOKE_ROWS_PER_ITER: usize = 50;

/// Relation ID used throughout this benchmark.
const REL: RelationId = RelationId::new(11);

/// In-memory buffer-pool loader.
///
/// Must be `pub(crate)` so [`SetupState`]'s type parameters are visible at
/// the same scope as the struct.
#[derive(Debug, Default)]
pub(crate) struct BlankLoader;

impl PageLoader for BlankLoader {
    fn load(&self, _page_id: PageId) -> ultrasql_core::Result<Page> {
        Ok(Page::new_heap())
    }
}

/// Encodes a `(i32, i64)` row into a 12-byte payload.
fn encode_row(id: i32, val: i64) -> [u8; 12] {
    let mut buf = [0_u8; 12];
    buf[0..4].copy_from_slice(&id.to_le_bytes());
    buf[4..12].copy_from_slice(&val.to_le_bytes());
    buf
}

/// Heap state produced by [`setup`] and consumed by [`run_one_iter`].
///
/// The pool and heap are kept alive here so tests can verify the post-state
/// via [`HeapAccess::scan_visible`] after [`setup`] returns.
pub(crate) struct SetupState {
    /// Number of rows inserted during setup.
    pub(crate) rows: usize,
    /// Pool that backs `heap`; kept alive so the heap remains valid.
    #[cfg(test)]
    pub(crate) pool: Arc<BufferPool<BlankLoader>>,
    /// Heap accessor for the relation populated by [`setup`].
    #[cfg(test)]
    pub(crate) heap: HeapAccess<BlankLoader>,
}

/// Allocates a fresh pool and inserts `n` rows of `(i32 id, i64 val)`
/// via [`HeapAccess::insert_batch`].
///
/// Returns a [`SetupState`] that callers can use for post-state assertions or
/// pass to [`run_one_iter`] for timing.
pub(crate) fn setup(n: usize) -> SetupState {
    let frames = (n / 100).max(1) + 128;
    let pool = Arc::new(BufferPool::new(frames, BlankLoader));
    let heap = HeapAccess::new(Arc::clone(&pool));
    let opts = InsertOptions {
        xmin: Xid::FIRST_USER,
        command_id: CommandId::FIRST,
        wal: None,
        fsm: None,
        vm: None,
    };
    let n_i32 = i32::try_from(n).unwrap_or(i32::MAX);
    let payloads: Vec<[u8; 12]> = (0..n_i32)
        .map(|id| encode_row(id, i64::from(id).wrapping_mul(999_983)))
        .collect();
    let rows: Vec<&[u8]> = payloads.iter().map(<[u8; 12]>::as_slice).collect();
    require_bench_ok(heap.insert_batch(REL, &rows, opts), "setup insert_batch");
    SetupState {
        rows: n,
        #[cfg(test)]
        pool,
        #[cfg(test)]
        heap,
    }
}

/// Times inserting `state.rows` rows into a *fresh* pool.
///
/// Each call allocates a brand-new [`BufferPool`] + [`HeapAccess`] inside the
/// timed region so the measurement is "10 k inserts into an empty relation" —
/// matching the `CREATE TABLE … INSERT INTO …` latency for an empty table.
/// The `state` argument carries only the desired row count; it is not mutated.
///
/// Returns the elapsed wall time.
pub(crate) fn run_one_iter(state: &SetupState) -> Duration {
    let opts = InsertOptions {
        xmin: Xid::FIRST_USER,
        command_id: CommandId::FIRST,
        wal: None,
        fsm: None,
        vm: None,
    };
    let n_i32 = i32::try_from(state.rows).unwrap_or(i32::MAX);
    let payloads: Vec<[u8; 12]> = (0..n_i32)
        .map(|id| encode_row(id, i64::from(id).wrapping_mul(999_983)))
        .collect();
    let rows: Vec<&[u8]> = payloads.iter().map(<[u8; 12]>::as_slice).collect();
    let frames = (state.rows / 100).max(1) + 128;
    let t0 = Instant::now();
    let pool = Arc::new(BufferPool::new(frames, BlankLoader));
    let heap = HeapAccess::new(Arc::clone(&pool));
    let tids = require_bench_ok(
        heap.insert_batch(REL, &rows, opts),
        "benchmark insert_batch",
    );
    let elapsed = t0.elapsed();
    std::hint::black_box(heap.block_count(REL));
    std::hint::black_box(tids.len());
    elapsed
}

/// Runs the insert-throughput benchmark.
pub fn run(ctx: &BenchContext) -> BenchResult {
    #[cfg(test)]
    let rows_per_iter = TEST_ROWS_PER_ITER;
    #[cfg(not(test))]
    let rows_per_iter = crate::runs::smoke_row_count(PROD_ROWS_PER_ITER, SMOKE_ROWS_PER_ITER);

    let state = setup(rows_per_iter);

    for _ in 0..ctx.warmup_iterations {
        run_one_iter(&state);
    }

    let iteration_count = usize::try_from(ctx.iterations).unwrap_or(0);
    let mut samples: Vec<f64> = Vec::with_capacity(iteration_count);
    for _ in 0..ctx.iterations {
        let elapsed = run_one_iter(&state);
        samples.push(elapsed.as_secs_f64() * 1_000_000.0);
    }

    let median_us = median_f64(&samples);
    let p99_us = p99_f64(&samples);
    let rows = crate::runs::count_as_f64(rows_per_iter);
    let throughput_per_sec = if median_us > 0.0 {
        rows / (median_us / 1_000_000.0)
    } else {
        0.0
    };

    BenchResult {
        throughput_per_sec,
        p50_latency_us: median_us,
        p99_latency_us: p99_us,
        samples,
    }
}

#[cfg(test)]
mod tests {
    use ultrasql_core::Xid;
    use ultrasql_mvcc::Snapshot;
    use ultrasql_mvcc::status::test_support::MapOracle;

    use super::*;
    use crate::registry::{BenchContext, HostInfo};

    fn test_ctx() -> BenchContext {
        BenchContext {
            iterations: 2,
            warmup_iterations: 1,
            host: HostInfo {
                cpu: "test".to_string(),
                cores: 1,
                ram_gb: 1,
                os: "test".to_string(),
            },
        }
    }

    #[test]
    fn run_produces_two_samples_with_positive_throughput() {
        let ctx = test_ctx();
        let result = run(&ctx);
        assert_eq!(
            result.samples.len(),
            usize::try_from(ctx.iterations).unwrap_or(0)
        );
        assert!(result.throughput_per_sec > 0.0);
    }

    /// Verifies that `setup(n)` inserts exactly `n` visible tuples, and that
    /// `run_one_iter` performs real work (non-zero elapsed time) without
    /// mutating the setup heap (each iter uses its own fresh pool).
    ///
    /// Uses `scan_visible` with a committed snapshot so the assertion is
    /// independent of any MVCC filtering quirk.
    #[test]
    fn insert_bench_actually_inserts_expected_row_count() {
        let _guard = crate::runs::enable_smoke_mode_for_process();

        // Oracle: FIRST_USER is committed.
        let oracle = MapOracle::new();
        oracle.set_committed(Xid::FIRST_USER);

        // Snapshot: xmin > FIRST_USER so rows inserted by FIRST_USER are visible.
        let snap = Snapshot::new(
            Xid::new(Xid::FIRST_USER.raw() + 1),
            Xid::new(Xid::FIRST_USER.raw() + 1000),
            Xid::new(Xid::FIRST_USER.raw() + 1000),
            ultrasql_core::CommandId::FIRST,
            std::iter::empty(),
        );

        // Part 1: setup populates exactly TEST_ROWS_PER_ITER visible rows.
        let state = setup(TEST_ROWS_PER_ITER);
        assert!(
            Arc::strong_count(&state.pool) >= 2,
            "setup state must retain the pool backing the heap"
        );

        let blocks = state.heap.block_count(REL);
        let visible: Vec<_> = state
            .heap
            .scan_visible(REL, blocks, &snap, &oracle)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(
            visible.len(),
            TEST_ROWS_PER_ITER,
            "setup must insert exactly TEST_ROWS_PER_ITER ({TEST_ROWS_PER_ITER}) \
             visible rows; got {}",
            visible.len()
        );

        // Part 2: run_one_iter uses its own fresh heap each call.
        // Verify real work was done (elapsed > 0) and setup heap is untouched.
        let elapsed = run_one_iter(&state);
        assert!(
            elapsed.as_nanos() > 0,
            "run_one_iter must take non-zero time (no-op detected)"
        );

        let blocks_after = state.heap.block_count(REL);
        let visible_after: Vec<_> = state
            .heap
            .scan_visible(REL, blocks_after, &snap, &oracle)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            visible_after.len(),
            TEST_ROWS_PER_ITER,
            "setup heap must remain at TEST_ROWS_PER_ITER rows after run_one_iter \
             (each timed iter uses a fresh pool)"
        );
    }
}
