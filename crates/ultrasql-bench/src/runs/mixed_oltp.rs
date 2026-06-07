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

use crate::registry::{BenchContext, BenchResult, median_f64, p99_f64, require_bench_ok};

/// Initial population of the relation.
#[cfg(not(test))]
const INITIAL_ROWS: usize = 100_000;

/// Reduced initial population for fast unit tests.
#[cfg(test)]
const INITIAL_ROWS: usize = 64;

/// Operations per measured iteration.
#[cfg(not(test))]
const OPS_PER_ITER: usize = 10_000;

/// Reduced ops per iteration for fast unit tests.
///
/// Must be large enough that the op-ratio assertions (±5%) have statistical
/// signal — 200 ops gives ≥16 samples per bucket.
#[cfg(test)]
const OPS_PER_ITER: usize = 200;

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

fn rng_index(seed: u64, upper_bound: usize) -> usize {
    let upper_bound_u64 = u64::try_from(upper_bound).unwrap_or(u64::MAX).max(1);
    let reduced = (seed >> 7) % upper_bound_u64;
    usize::try_from(reduced).unwrap_or(0)
}

/// Runs the mixed-OLTP benchmark.
pub fn run(ctx: &BenchContext) -> BenchResult {
    // Budget: initial rows + ops_per_iter inserts per iteration.
    let max_rows =
        INITIAL_ROWS + OPS_PER_ITER * (ctx.iterations as usize + ctx.warmup_iterations as usize);
    let frames = (max_rows / 50).max(1) + 1_024;
    let pool = Arc::new(BufferPool::new(frames, BlankLoader));
    let heap = HeapAccess::new(Arc::clone(&pool));

    // Preload via the batch path so setup does not pay the O(N²) cost
    // of per-row `insert` walking every previously-allocated block.
    let insert_opts = InsertOptions {
        xmin: Xid::FIRST_USER,
        command_id: CommandId::FIRST,
        wal: None,
        fsm: None,
        vm: None,
    };
    let n_i32 = i32::try_from(INITIAL_ROWS).unwrap_or(i32::MAX);
    let preload_payloads: Vec<[u8; 12]> = (0..n_i32)
        .map(|id| encode_row(id, i64::from(id).wrapping_mul(999_983)))
        .collect();
    let preload_rows: Vec<&[u8]> = preload_payloads.iter().map(<[u8; 12]>::as_slice).collect();
    let mut tids: Vec<TupleId> = Vec::with_capacity(max_rows);
    let preloaded = require_bench_ok(
        heap.insert_batch(REL, &preload_rows, insert_opts),
        "preload insert_batch",
    );
    tids.extend(preloaded);

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
                    let idx = rng_index(s, n_tids);
                    let result = h.fetch(current_tids[idx]);
                    std::hint::black_box(result.is_ok());
                } else if kind < 80 {
                    // Update.
                    let idx = rng_index(s, n_tids);
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
    use ultrasql_core::Xid;
    use ultrasql_mvcc::Snapshot;
    use ultrasql_mvcc::status::test_support::MapOracle;

    use super::*;
    use crate::registry::{BenchContext, HostInfo};

    #[test]
    fn run_produces_two_samples_with_positive_throughput() {
        let ctx = BenchContext {
            iterations: 2,
            warmup_iterations: 1,
            host: HostInfo {
                cpu: "test".to_string(),
                cores: 1,
                ram_gb: 1,
                os: "test".to_string(),
            },
        };
        let result = run(&ctx);
        assert_eq!(result.samples.len(), ctx.iterations as usize);
        assert!(result.throughput_per_sec > 0.0);
    }

    /// Builds an MVCC snapshot that sees `FIRST_USER` as committed.
    fn committed_snap() -> (Snapshot, MapOracle) {
        let oracle = MapOracle::new();
        oracle.set_committed(Xid::FIRST_USER);
        let snap = Snapshot::new(
            Xid::new(Xid::FIRST_USER.raw() + 1),
            Xid::new(Xid::FIRST_USER.raw() + 100_000),
            Xid::new(Xid::FIRST_USER.raw() + 100_000),
            ultrasql_core::CommandId::FIRST,
            std::iter::empty(),
        );
        (snap, oracle)
    }

    /// Counts visible rows via `scan_visible`.
    fn visible_count(heap: &HeapAccess<BlankLoader>, snap: &Snapshot, oracle: &MapOracle) -> usize {
        let blocks = heap.block_count(REL);
        heap.scan_visible(REL, blocks, snap, oracle)
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .len()
    }

    /// Counts how many ops of each type a single iteration produces for the
    /// given seed, using the same dispatch rule as `run_ops`.
    fn count_ops(seed: u64) -> (usize, usize, usize) {
        let mut s = seed;
        let (mut reads, mut updates, mut inserts) = (0usize, 0usize, 0usize);
        for _ in 0..OPS_PER_ITER {
            s = xorshift64(s);
            let kind = (s % 100) as u8;
            if kind < 50 {
                reads += 1;
            } else if kind < 80 {
                updates += 1;
            } else {
                inserts += 1;
            }
        }
        (reads, updates, inserts)
    }

    /// Per-op-type ratios must be within ±5 % of the documented 50/30/20 mix,
    /// total ops must equal `OPS_PER_ITER`, and visible row count must be
    /// monotonically non-decreasing across iterations (inserts only grow the
    /// relation; updates and reads do not change row count).
    #[test]
    fn mixed_oltp_op_ratios_and_row_count_are_honest() {
        let _guard = crate::runs::enable_smoke_mode_for_process();

        // Build the heap manually so we can inspect it between iterations.
        let max_rows = INITIAL_ROWS + OPS_PER_ITER * 3; // 3 measured iters
        let frames = (max_rows / 50).max(1) + 1_024;
        let pool = Arc::new(BufferPool::new(frames, BlankLoader));
        let heap = HeapAccess::new(Arc::clone(&pool));

        let insert_opts = InsertOptions {
            xmin: Xid::FIRST_USER,
            command_id: CommandId::FIRST,
            wal: None,
            fsm: None,
            vm: None,
        };
        let n_i32 = i32::try_from(INITIAL_ROWS).unwrap_or(i32::MAX);
        let preload_payloads: Vec<[u8; 12]> = (0..n_i32)
            .map(|id| encode_row(id, i64::from(id).wrapping_mul(999_983)))
            .collect();
        let preload_rows: Vec<&[u8]> = preload_payloads.iter().map(<[u8; 12]>::as_slice).collect();
        let mut tids: Vec<TupleId> = heap
            .insert_batch(REL, &preload_rows, insert_opts)
            .expect("preload must succeed");

        let (snap, oracle) = committed_snap();

        let initial_visible = visible_count(&heap, &snap, &oracle);
        assert_eq!(
            initial_visible, INITIAL_ROWS,
            "preload must produce exactly INITIAL_ROWS visible rows"
        );

        let update_opts = UpdateOptions {
            xid: Xid::FIRST_USER,
            command_id: CommandId::FIRST,
            hot_eligible: true,
            wal: None,
            vm: None,
        };

        let mut seed: u64 = 0xABCD_EF01_2345_6789;
        let mut prev_visible = initial_visible;

        for iter in 0..3_usize {
            seed = xorshift64(seed);

            // Count expected ops from the seed.
            let (expected_reads, expected_updates, expected_inserts) = count_ops(seed);
            let total = expected_reads + expected_updates + expected_inserts;
            assert_eq!(
                total, OPS_PER_ITER,
                "iter {iter}: total ops must equal OPS_PER_ITER"
            );

            // Ratio checks ±5 %.
            let read_pct = expected_reads as f64 / OPS_PER_ITER as f64;
            let update_pct = expected_updates as f64 / OPS_PER_ITER as f64;
            let insert_pct = expected_inserts as f64 / OPS_PER_ITER as f64;
            assert!(
                (read_pct - 0.50).abs() <= 0.05,
                "iter {iter}: read ratio {read_pct:.3} out of 50%±5% window"
            );
            assert!(
                (update_pct - 0.30).abs() <= 0.05,
                "iter {iter}: update ratio {update_pct:.3} out of 30%±5% window"
            );
            assert!(
                (insert_pct - 0.20).abs() <= 0.05,
                "iter {iter}: insert ratio {insert_pct:.3} out of 20%±5% window"
            );

            // Run one iteration.
            let n_tids = tids.len();
            let mut s = seed;
            for op_idx in 0..OPS_PER_ITER {
                s = xorshift64(s);
                let kind = (s % 100) as u8;
                if kind < 50 {
                    let idx = rng_index(s, n_tids);
                    let _ = heap.fetch(tids[idx]);
                } else if kind < 80 {
                    let idx = rng_index(s, n_tids);
                    let old_tid = tids[idx];
                    let id = i32::try_from(idx).unwrap_or(i32::MAX);
                    let val = i64::from(id)
                        .wrapping_mul(999_983)
                        .wrapping_add(i64::try_from(op_idx).unwrap_or(0));
                    let payload = encode_row(id, val);
                    if let Ok(outcome) = heap.update(old_tid, &payload, update_opts) {
                        tids[idx] = outcome.new_tid;
                    }
                } else {
                    let id = i32::try_from(n_tids + op_idx).unwrap_or(i32::MAX);
                    let val = i64::from(id).wrapping_mul(999_983);
                    let payload = encode_row(id, val);
                    if let Ok(new_tid) = heap.insert(REL, &payload, insert_opts) {
                        tids.push(new_tid);
                    }
                }
            }

            // Visible row count must be >= the previous iteration's count
            // (inserts can only grow the relation; updates and reads do not
            // decrease the live tuple count).
            let curr_visible = visible_count(&heap, &snap, &oracle);
            assert!(
                curr_visible >= prev_visible,
                "iter {iter}: visible row count decreased from {prev_visible} to {curr_visible}"
            );
            prev_visible = curr_visible;
        }
    }
}
