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

use std::sync::Arc;
use std::time::Instant;

use ultrasql_core::{CommandId, PageId, RelationId, Xid};
use ultrasql_storage::buffer_pool::{BufferPool, PageLoader};
use ultrasql_storage::heap::{HeapAccess, InsertOptions};
use ultrasql_storage::page::Page;

use crate::registry::{BenchContext, BenchResult, median_f64, p99_f64};

/// Full production rows per iteration.
#[allow(dead_code)] // used only in non-test builds via smoke_row_count
const PROD_ROWS_PER_ITER: usize = 10_000;

/// Reduced rows per iteration for fast unit tests.
#[cfg(test)]
const TEST_ROWS_PER_ITER: usize = 100;

/// Smoke-mode rows per iteration (used when `ULTRASQL_BENCH_SMOKE` is set).
#[allow(dead_code)] // used only in non-test builds via smoke_row_count
const SMOKE_ROWS_PER_ITER: usize = 50;

/// Relation ID used throughout this benchmark.
const REL: RelationId = RelationId::new(11);

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

/// Runs one insert iteration: fresh pool + `n` inserts.
///
/// Returns elapsed microseconds.
fn run_one_iteration(n: usize) -> f64 {
    // Size the pool to avoid eviction: ~110 rows per 8 KiB page.
    let frames = (n / 100).max(1) + 128;
    let t0 = Instant::now();

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
        let val = i64::from(id).wrapping_mul(999_983);
        let payload = encode_row(id, val);
        heap.insert(REL, &payload, opts)
            .expect("insert must succeed during benchmark");
    }
    // Prevent the compiler from hoisting any heap operations out of the
    // timed region.
    std::hint::black_box(heap.block_count(REL));

    t0.elapsed().as_secs_f64() * 1_000_000.0 // µs
}

/// Runs the insert-throughput benchmark.
pub fn run(ctx: &BenchContext) -> BenchResult {
    #[cfg(test)]
    let rows_per_iter = TEST_ROWS_PER_ITER;
    #[cfg(not(test))]
    let rows_per_iter = crate::runs::smoke_row_count(PROD_ROWS_PER_ITER, SMOKE_ROWS_PER_ITER);

    for _ in 0..ctx.warmup_iterations {
        run_one_iteration(rows_per_iter);
    }

    let mut samples: Vec<f64> = Vec::with_capacity(ctx.iterations as usize);
    for _ in 0..ctx.iterations {
        samples.push(run_one_iteration(rows_per_iter));
    }

    let median_us = median_f64(&samples);
    let p99_us = p99_f64(&samples);
    let rows = rows_per_iter as f64;
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
