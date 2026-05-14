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

use std::sync::Arc;
use std::time::Instant;

use ultrasql_core::{CommandId, PageId, RelationId, TupleId, Xid};
use ultrasql_storage::buffer_pool::{BufferPool, PageLoader};
use ultrasql_storage::heap::{HeapAccess, InsertOptions, UpdateOptions};
use ultrasql_storage::page::Page;

use crate::registry::{BenchContext, BenchResult, median_f64, p99_f64};

/// Full production rows per iteration.
#[cfg(not(test))]
const ROWS_PER_ITER: usize = 10_000;

/// Reduced rows per iteration for fast unit tests.
#[cfg(test)]
const ROWS_PER_ITER: usize = 100;

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
/// via `HeapAccess::update`. The pool must be large enough to hold both
/// old and new versions simultaneously.
pub fn run(ctx: &BenchContext) -> BenchResult {
    // Budget: inserts + updates may double the page count.
    let frames = (ROWS_PER_ITER / 50).max(1) + 512;
    let pool = Arc::new(BufferPool::new(frames, BlankLoader));
    let heap = HeapAccess::new(Arc::clone(&pool));

    // Preload.
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
        let tid = heap
            .insert(REL, &payload, insert_opts)
            .expect("preload insert must succeed");
        tids.push(tid);
    }

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
            let outcome = h
                .update(old_tid, &payload, update_opts)
                .expect("update must succeed");
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

    let mut samples: Vec<f64> = Vec::with_capacity(ctx.iterations as usize);
    for _ in 0..ctx.iterations {
        samples.push(timed_iter(&heap, &mut tids));
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
