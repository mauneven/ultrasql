//! `delete_throughput_10k` benchmark implementation.
//!
//! Preloads 10 000 `(i32 id, i64 val)` rows into a heap relation, then
//! measures deleting every row via [`ultrasql_storage::heap::HeapAccess::delete`].
//!
//! Throughput = `ROWS_PER_ITER / median_elapsed_seconds`.
//!
//! The preload happens outside the timed region. Because deletes are not
//! reversible without re-inserting, the heap is rebuilt for each measured
//! iteration.

use std::sync::Arc;
use std::time::Instant;

use ultrasql_core::{CommandId, PageId, RelationId, TupleId, Xid};
use ultrasql_storage::buffer_pool::{BufferPool, PageLoader};
use ultrasql_storage::heap::{DeleteOptions, HeapAccess, InsertOptions};
use ultrasql_storage::page::Page;

use crate::registry::{BenchContext, BenchResult, median_f64, p99_f64};

/// Full production rows per iteration.
#[cfg(not(test))]
const ROWS_PER_ITER: usize = 10_000;

/// Reduced rows per iteration for fast unit tests.
#[cfg(test)]
const ROWS_PER_ITER: usize = 32;

/// Relation ID used throughout this benchmark.
const REL: RelationId = RelationId::new(17);

/// In-memory buffer-pool loader.
#[derive(Debug, Default)]
struct BlankLoader;

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

/// Preloads `n` rows into a fresh heap and returns the pool, heap, and
/// the list of inserted `TupleId`s in insertion order.
///
/// Uses [`HeapAccess::insert_batch`] so the setup phase does not pay the
/// O(N²) cost of per-row `insert` walking every previously-allocated
/// block on each call.
fn preload(
    n: usize,
) -> (
    Arc<BufferPool<BlankLoader>>,
    HeapAccess<BlankLoader>,
    Vec<TupleId>,
) {
    // Extra factor of 2 headroom: deleted slots stay on-page until VACUUM.
    let frames = (n / 50).max(1) + 256;
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
    let tids: Vec<TupleId> = heap
        .insert_batch(REL, &rows, opts)
        .expect("preload insert_batch must succeed");

    (pool, heap, tids)
}

/// Runs the delete-throughput benchmark.
///
/// Each measured iteration:
/// 1. Rebuilds the heap (untimed preload).
/// 2. Times the deletion of all `ROWS_PER_ITER` rows.
pub fn run(ctx: &BenchContext) -> BenchResult {
    let delete_opts = DeleteOptions {
        xmax: Xid::FIRST_USER,
        cmax: CommandId::FIRST,
        wal: None,
        fsm: None,
        vm: None,
    };

    let run_one = || -> f64 {
        // Rebuild the heap for each iteration (untimed).
        let (_pool, heap, tids) = preload(ROWS_PER_ITER);

        let t0 = Instant::now();
        for &tid in &tids {
            heap.delete(tid, delete_opts)
                .expect("delete must succeed on a live tuple");
        }
        let elapsed = t0.elapsed();
        std::hint::black_box(heap.block_count(REL));
        elapsed.as_secs_f64() * 1_000_000.0 // µs
    };

    for _ in 0..ctx.warmup_iterations {
        run_one();
    }

    let mut samples: Vec<f64> = Vec::with_capacity(ctx.iterations as usize);
    for _ in 0..ctx.iterations {
        samples.push(run_one());
    }

    let median_us = median_f64(&samples);
    let p99_us = p99_f64(&samples);
    let rows = ROWS_PER_ITER as f64;
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
        assert_eq!(result.samples.len(), ctx.iterations as usize);
        assert!(result.throughput_per_sec > 0.0);
    }

    /// After the bench deletes all rows, `scan_visible` must return zero rows.
    ///
    /// Preloads `ROWS_PER_ITER` rows, then deletes every one via the same
    /// `DeleteOptions` the bench uses, and asserts the visible count is zero.
    #[test]
    fn delete_bench_leaves_zero_visible_rows() {
        let _guard = crate::runs::enable_smoke_mode_for_process();

        let (_pool, heap, tids) = preload(ROWS_PER_ITER);

        // Oracle: FIRST_USER is committed (both as inserter and deleter).
        let oracle = MapOracle::new();
        oracle.set_committed(Xid::FIRST_USER);

        // Snapshot seen AFTER FIRST_USER committed.
        let snap = Snapshot::new(
            Xid::new(Xid::FIRST_USER.raw() + 1),
            Xid::new(Xid::FIRST_USER.raw() + 1000),
            Xid::new(Xid::FIRST_USER.raw() + 1000),
            ultrasql_core::CommandId::FIRST,
            std::iter::empty(),
        );

        // Verify rows are visible before deletion.
        let blocks_before = heap.block_count(REL);
        let before: Vec<_> = heap
            .scan_visible(REL, blocks_before, &snap, &oracle)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            before.len(),
            ROWS_PER_ITER,
            "preload must produce exactly ROWS_PER_ITER visible rows before deletion"
        );

        // Delete every row with the same options the bench uses.
        let delete_opts = DeleteOptions {
            xmax: Xid::FIRST_USER,
            cmax: ultrasql_core::CommandId::FIRST,
            wal: None,
            fsm: None,
            vm: None,
        };
        for &tid in &tids {
            heap.delete(tid, delete_opts)
                .expect("delete must succeed on a live tuple");
        }

        // After deletion with FIRST_USER committed, scan_visible must return 0.
        let blocks_after = heap.block_count(REL);
        let after: Vec<_> = heap
            .scan_visible(REL, blocks_after, &snap, &oracle)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            after.len(),
            0,
            "scan_visible must return zero rows after all rows are deleted"
        );
    }
}
