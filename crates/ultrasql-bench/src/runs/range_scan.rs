//! `range_scan` benchmark implementation.
//!
//! Builds an in-memory heap relation with 1 000 000 `(i32 id, i64 val)` rows
//! and measures a full sequential scan per iteration using
//! [`ultrasql_storage::heap::HeapAccess::scan`].
//!
//! Throughput = `ROW_COUNT / median_elapsed_seconds`.

use std::sync::Arc;
use std::time::Instant;

use ultrasql_core::{CommandId, PageId, RelationId, Xid};
use ultrasql_storage::buffer_pool::{BufferPool, PageLoader};
use ultrasql_storage::heap::{HeapAccess, InsertOptions};
use ultrasql_storage::page::Page;

use crate::registry::{BenchContext, BenchResult, median_f64, p99_f64, require_bench_ok};

/// Full production row count: 1 000 000 rows.
#[allow(dead_code)] // used only in non-test builds via smoke_row_count
const PROD_ROW_COUNT: usize = 1_000_000;

/// Reduced row count for fast unit tests.
#[cfg(test)]
const TEST_ROW_COUNT: usize = 500;

/// Smoke-mode row count (used when `ULTRASQL_BENCH_SMOKE` is set).
#[allow(dead_code)] // used only in non-test builds via smoke_row_count
const SMOKE_ROW_COUNT: usize = 200;

/// Relation ID used throughout this benchmark.
const REL: RelationId = RelationId::new(7);

/// In-memory buffer-pool loader for the heap relation.
#[derive(Debug, Default)]
struct BlankLoader;

impl PageLoader for BlankLoader {
    fn load(&self, _page_id: PageId) -> ultrasql_core::Result<Page> {
        Ok(Page::new_heap())
    }
}

/// Encodes a `(i32, i64)` row into a fixed-size 12-byte payload.
fn encode_row(id: i32, val: i64) -> [u8; 12] {
    let mut buf = [0_u8; 12];
    buf[0..4].copy_from_slice(&id.to_le_bytes());
    buf[4..12].copy_from_slice(&val.to_le_bytes());
    buf
}

/// Builds an in-memory heap with `n` rows.
///
/// The buffer pool is sized to hold every page without eviction. Each
/// row is 12 bytes; with a 8 KiB page and ~56-byte MVCC header per
/// tuple we fit roughly 110 rows per page, so `n / 100 + 512` frames
/// is sufficient headroom.
fn build_heap(n: usize) -> (Arc<BufferPool<BlankLoader>>, HeapAccess<BlankLoader>) {
    let frames = (n / 100).max(1) + 512;
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
    for id in 0..n_i32 {
        let val = i64::from(id).wrapping_mul(1_000_000_007);
        let payload = encode_row(id, val);
        require_bench_ok(heap.insert(REL, &payload, opts), "heap build insert");
    }

    (pool, heap)
}

/// Runs the range-scan benchmark.
///
/// Setup (outside the timed region): build a heap with `ROW_COUNT`
/// rows.
///
/// Each measured iteration: scan the heap from block 0 to `block_count`
/// via `HeapAccess::scan` and consume every tuple via `black_box`.
pub fn run(ctx: &BenchContext) -> BenchResult {
    #[cfg(test)]
    let row_count = TEST_ROW_COUNT;
    #[cfg(not(test))]
    let row_count = crate::runs::smoke_row_count(PROD_ROW_COUNT, SMOKE_ROW_COUNT);

    let (_pool, heap) = build_heap(row_count);
    let block_count = heap.block_count(REL);

    let timed_iter = |h: &HeapAccess<BlankLoader>| -> f64 {
        let t0 = Instant::now();
        let mut count: usize = 0;
        for result in h.scan(REL, block_count) {
            let tuple = require_bench_ok(result, "scan freshly built heap");
            std::hint::black_box(&tuple.data);
            count = count.wrapping_add(1);
        }
        let elapsed = t0.elapsed();
        std::hint::black_box(count);
        elapsed.as_secs_f64() * 1_000_000.0 // µs
    };

    for _ in 0..ctx.warmup_iterations {
        timed_iter(&heap);
    }

    let mut samples: Vec<f64> = Vec::with_capacity(ctx.iterations as usize);
    for _ in 0..ctx.iterations {
        samples.push(timed_iter(&heap));
    }

    let median_us = median_f64(&samples);
    let p99_us = p99_f64(&samples);
    let rows = row_count as f64;
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
}
