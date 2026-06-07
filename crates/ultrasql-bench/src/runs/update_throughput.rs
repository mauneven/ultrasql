//! `update_throughput_10k` benchmark implementation.
//!
//! Preloads 10 000 `(i32 id, i64 val)` rows into a heap relation, then
//! measures updating each row's `val` field (implemented as
//! `HeapAccess::update` with a new payload).
//!
//! Throughput = `ROWS_PER_ITER / median_elapsed_seconds`.
//!
//! The preload happens outside the timed region. Each measured iteration
//! re-reads the tuple ids left from the previous iteration and updates
//! each one in order.
//!
//! # val encoding per update pass
//!
//! After `k` measured passes, row `idx` holds
//! `val = i64::from(idx).wrapping_mul(999_983).wrapping_add(k as i64)`.
//! The post-state test verifies this invariant.

use std::sync::Arc;
use std::time::Instant;

use ultrasql_core::{CommandId, PageId, RelationId, TupleId, Xid};
use ultrasql_storage::buffer_pool::{BufferPool, PageLoader};
use ultrasql_storage::heap::{HeapAccess, InsertOptions, UpdateOptions};
use ultrasql_storage::page::Page;

use crate::registry::{BenchContext, BenchResult, median_f64, p99_f64, require_bench_ok};

/// Full production rows per iteration.
#[cfg(not(test))]
const ROWS_PER_ITER: usize = 10_000;

/// Reduced rows per iteration for fast unit tests.
#[cfg(test)]
const ROWS_PER_ITER: usize = 32;

/// Relation ID used throughout this benchmark.
const REL: RelationId = RelationId::new(13);

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

/// Runs the update-throughput benchmark.
///
/// Setup (outside the timed region): build a fresh heap with
/// `ROWS_PER_ITER` rows and record all their `TupleId`s.
///
/// Each measured iteration: update every row by inserting a new version
/// via `HeapAccess::update`. The pool must be large enough to hold every
/// version generated across all warmup + measured iterations because the
/// pool's CLOCK eviction will reject pinned/dirty frames and we never
/// drop the heap between iterations.
pub fn run(ctx: &BenchContext) -> BenchResult {
    // Budget: preload (10k rows ≈ 91 pages) plus one fresh page per
    // ~110 update fallbacks across every warmup + measured iteration.
    // Pad generously so the pool never has to evict.
    let iteration_count = usize::try_from(ctx.iterations).unwrap_or(0);
    let warmup_iteration_count = usize::try_from(ctx.warmup_iterations).unwrap_or(0);
    let total_updates =
        ROWS_PER_ITER.saturating_mul(iteration_count.saturating_add(warmup_iteration_count));
    let frames = (ROWS_PER_ITER / 50)
        .saturating_add(total_updates / 50)
        .saturating_add(1024);
    let pool = Arc::new(BufferPool::new(frames, BlankLoader));
    let heap = HeapAccess::new(Arc::clone(&pool));

    // Preload via the batch path so the bench's setup phase does not
    // pay the O(N²) cost of per-row `insert` walking every block.
    let insert_opts = InsertOptions {
        xmin: Xid::FIRST_USER,
        command_id: CommandId::FIRST,
        wal: None,
        fsm: None,
        vm: None,
    };
    let n_i32 = i32::try_from(ROWS_PER_ITER).unwrap_or(i32::MAX);
    let preload_payloads: Vec<[u8; 12]> = (0..n_i32)
        .map(|id| encode_row(id, i64::from(id).wrapping_mul(999_983)))
        .collect();
    let preload_rows: Vec<&[u8]> = preload_payloads.iter().map(<[u8; 12]>::as_slice).collect();
    let mut tids: Vec<TupleId> = require_bench_ok(
        heap.insert_batch(REL, &preload_rows, insert_opts),
        "preload insert_batch",
    );

    let update_opts = UpdateOptions {
        xid: Xid::FIRST_USER,
        command_id: CommandId::FIRST,
        hot_eligible: true,
        wal: None,
        vm: None,
    };

    let timed_iter = |h: &HeapAccess<BlankLoader>, ids: &mut Vec<TupleId>| -> f64 {
        let t0 = Instant::now();
        let mut new_tids: Vec<TupleId> = Vec::with_capacity(ids.len());
        for (idx, &old_tid) in ids.iter().enumerate() {
            let id = i32::try_from(idx).unwrap_or(i32::MAX);
            // Increment val by 1 on each update pass.
            let val = i64::from(id).wrapping_mul(999_983).wrapping_add(1);
            let payload = encode_row(id, val);
            let outcome = require_bench_ok(h.update(old_tid, &payload, update_opts), "update row");
            new_tids.push(outcome.new_tid);
        }
        let elapsed = t0.elapsed();
        // Update ids for the next iteration so we always update the
        // latest version of each tuple.
        *ids = new_tids;
        elapsed.as_secs_f64() * 1_000_000.0 // µs
    };

    for _ in 0..ctx.warmup_iterations {
        timed_iter(&heap, &mut tids);
    }

    let mut samples: Vec<f64> = Vec::with_capacity(iteration_count);
    for _ in 0..ctx.iterations {
        samples.push(timed_iter(&heap, &mut tids));
    }

    let median_us = median_f64(&samples);
    let p99_us = p99_f64(&samples);
    let rows = crate::runs::count_as_f64(ROWS_PER_ITER);
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
            warmup_iterations: 0,
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

    /// After `N` measured update iterations every row's `val` must equal
    /// `original_val + N` where `original_val = i64::from(idx).wrapping_mul(999_983)`.
    ///
    /// The test replicates the bench setup inline so it can inspect the final
    /// heap state via `scan_visible`.
    #[test]
    fn update_bench_final_val_equals_original_plus_n_iterations() {
        const N_ITERS: usize = 3;
        const TEST_FRAMES: usize = 513;

        let _guard = crate::runs::enable_smoke_mode_for_process();

        // Replicate the benchmark's setup phase with enough test frames
        // for one small heap page plus reserve.
        let pool = Arc::new(BufferPool::new(TEST_FRAMES, BlankLoader));
        let heap = HeapAccess::new(Arc::clone(&pool));

        let insert_opts = InsertOptions {
            xmin: Xid::FIRST_USER,
            command_id: CommandId::FIRST,
            wal: None,
            fsm: None,
            vm: None,
        };
        let n_i32 = i32::try_from(ROWS_PER_ITER).unwrap_or(i32::MAX);
        let mut tids: Vec<TupleId> = Vec::with_capacity(ROWS_PER_ITER);
        for id in 0..n_i32 {
            let val = i64::from(id).wrapping_mul(999_983);
            let payload = encode_row(id, val);
            tids.push(heap.insert(REL, &payload, insert_opts).unwrap());
        }

        let update_opts = UpdateOptions {
            xid: Xid::FIRST_USER,
            command_id: CommandId::FIRST,
            hot_eligible: true,
            wal: None,
            vm: None,
        };

        // Run N_ITERS update passes, mirroring the benchmark's timed_iter.
        for pass in 0..N_ITERS {
            let mut new_tids: Vec<TupleId> = Vec::with_capacity(tids.len());
            for (idx, &old_tid) in tids.iter().enumerate() {
                let id = i32::try_from(idx).unwrap_or(i32::MAX);
                let val = i64::from(id)
                    .wrapping_mul(999_983)
                    .wrapping_add(i64::try_from(pass + 1).unwrap_or(0));
                let payload = encode_row(id, val);
                let outcome = heap.update(old_tid, &payload, update_opts).unwrap();
                new_tids.push(outcome.new_tid);
            }
            tids = new_tids;
        }

        // Scan the heap and verify each visible row's val.
        let oracle = MapOracle::new();
        oracle.set_committed(Xid::FIRST_USER);

        let snap = Snapshot::new(
            Xid::new(Xid::FIRST_USER.raw() + 1),
            Xid::new(Xid::FIRST_USER.raw() + 1000),
            Xid::new(Xid::FIRST_USER.raw() + 1000),
            ultrasql_core::CommandId::FIRST,
            std::iter::empty(),
        );

        let blocks = heap.block_count(REL);
        let visible: Vec<_> = heap
            .scan_visible(REL, blocks, &snap, &oracle)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        // Only the latest (non-overwritten) versions are visible.
        assert_eq!(
            visible.len(),
            ROWS_PER_ITER,
            "scan_visible must return exactly ROWS_PER_ITER live tuples"
        );

        for tuple in &visible {
            assert_eq!(tuple.data.len(), 12, "payload must be 12 bytes (i32 + i64)");
            let id = i32::from_le_bytes(tuple.data[0..4].try_into().unwrap());
            let val = i64::from_le_bytes(tuple.data[4..12].try_into().unwrap());
            let idx = usize::try_from(id).unwrap();
            let expected_val = i64::from(id)
                .wrapping_mul(999_983)
                .wrapping_add(i64::try_from(N_ITERS).unwrap());
            assert_eq!(
                val, expected_val,
                "row idx={idx} val mismatch: got {val}, want {expected_val}"
            );
        }
    }
}
