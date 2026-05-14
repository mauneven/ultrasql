//! `mixed_oltp_pgbench_like` benchmark implementation.
//!
//! Simulates a TPC-B-like OLTP mix on a 100 000-row heap relation (or
//! `TEST_ROWS` in test mode):
//!
//! - 50 % point reads (`HeapAccess::fetch` by `TupleId`)
//! - 30 % in-place updates (`HeapAccess::update`)
//! - 20 % inserts (`HeapAccess::insert`)
//!
//! The benchmark runs for a fixed number of iterations (`ctx.iterations`)
//! rather than a fixed wall-clock window, performing `OPS_PER_ITER`
//! operations per iteration to keep the timed unit stable.
//!
//! Throughput = `OPS_PER_ITER / median_elapsed_seconds`.

use std::sync::Arc;
use std::time::Instant;

use ultrasql_core::{CommandId, PageId, RelationId, TupleId, Xid};
use ultrasql_storage::buffer_pool::{BufferPool, PageLoader};
use ultrasql_storage::heap::{HeapAccess, InsertOptions, UpdateOptions};
use ultrasql_storage::page::Page;

use crate::registry::{BenchContext, BenchResult, median_f64, p99_f64};

/// Initial population of the relation.
#[cfg(not(test))]
const INITIAL_ROWS: usize = 100_000;

/// Reduced initial population for fast unit tests.
#[cfg(test)]
const INITIAL_ROWS: usize = 200;

/// Operations per measured iteration.
#[cfg(not(test))]
const OPS_PER_ITER: usize = 10_000;

/// Reduced ops per iteration for fast unit tests.
#[cfg(test)]
const OPS_PER_ITER: usize = 50;

/// Relation ID used throughout this benchmark.
const REL: RelationId = RelationId::new(19);

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

/// Advances an xorshift64 state and returns the new state.
#[inline]
const fn xorshift64(s: u64) -> u64 {
    let mut x = s;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    x
}

/// Runs the mixed-OLTP benchmark.
pub fn run(ctx: &BenchContext) -> BenchResult {
    // Budget: initial rows + ops_per_iter inserts per iteration.
    let max_rows =
        INITIAL_ROWS + OPS_PER_ITER * (ctx.iterations as usize + ctx.warmup_iterations as usize);
    let frames = (max_rows / 50).max(1) + 1_024;
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
    let n_i32 = i32::try_from(INITIAL_ROWS).unwrap_or(i32::MAX);
    let mut tids: Vec<TupleId> = Vec::with_capacity(max_rows);
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

    let run_ops =
        |h: &HeapAccess<BlankLoader>, current_tids: &mut Vec<TupleId>, rng_seed: u64| -> f64 {
            let t0 = Instant::now();
            let mut s = rng_seed;
            let n_tids = current_tids.len();

            for op_idx in 0..OPS_PER_ITER {
                s = xorshift64(s);
                // Determine operation by low 7 bits: 0..49 read, 50..79 update,
                // 80..99 insert.
                let kind = (s % 100) as u8;
                if kind < 50 {
                    // Point read.
                    let idx = (s as usize >> 7) % n_tids;
                    let result = h.fetch(current_tids[idx]);
                    std::hint::black_box(result.is_ok());
                } else if kind < 80 {
                    // Update.
                    let idx = (s as usize >> 7) % n_tids;
                    let old_tid = current_tids[idx];
                    let id = i32::try_from(idx).unwrap_or(i32::MAX);
                    let val = i64::from(id)
                        .wrapping_mul(999_983)
                        .wrapping_add(i64::try_from(op_idx).unwrap_or(0));
                    let payload = encode_row(id, val);
                    if let Ok(outcome) = h.update(old_tid, &payload, update_opts) {
                        current_tids[idx] = outcome.new_tid;
                    }
                } else {
                    // Insert.
                    let id = i32::try_from(n_tids + op_idx).unwrap_or(i32::MAX);
                    let val = i64::from(id).wrapping_mul(999_983);
                    let payload = encode_row(id, val);
                    if let Ok(new_tid) = h.insert(REL, &payload, insert_opts) {
                        current_tids.push(new_tid);
                    }
                }
            }
            let elapsed = t0.elapsed();
            elapsed.as_secs_f64() * 1_000_000.0 // µs
        };

    let mut seed: u64 = 0xABCD_EF01_2345_6789;
    for _ in 0..ctx.warmup_iterations {
        seed = xorshift64(seed);
        run_ops(&heap, &mut tids, seed);
    }

    let mut samples: Vec<f64> = Vec::with_capacity(ctx.iterations as usize);
    for _ in 0..ctx.iterations {
        seed = xorshift64(seed);
        samples.push(run_ops(&heap, &mut tids, seed));
    }

    let median_us = median_f64(&samples);
    let p99_us = p99_f64(&samples);
    let ops = OPS_PER_ITER as f64;
    let throughput_per_sec = if median_us > 0.0 {
        ops / (median_us / 1_000_000.0)
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
