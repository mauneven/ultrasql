//! `point_lookup` benchmark implementation.
//!
//! Builds a `BTree<i64>` with 1 000 000 keys (or `TEST_ROWS` in test mode)
//! and measures 1 000 random point lookups per iteration.
//!
//! Throughput = `PROBES_PER_ITER / median_elapsed_seconds`.

use std::sync::Arc;
use std::time::Instant;

use ultrasql_core::{BlockNumber, PageId, RelationId, TupleId, Xid};
use ultrasql_storage::btree::BTree;
use ultrasql_storage::buffer_pool::{BufferPool, PageLoader};
use ultrasql_storage::page::Page;

use crate::registry::{BenchContext, BenchResult, median_f64, p99_f64, require_bench_ok};

/// Full production key count for the `BTree`.
#[cfg(not(test))]
const PROD_KEY_COUNT: usize = 1_000_000;

/// Reduced key count for fast unit tests.
#[cfg(test)]
const TEST_KEY_COUNT: usize = 1_000;

/// Smoke-mode key count (used when `ULTRASQL_BENCH_SMOKE` is set).
#[cfg(not(test))]
const SMOKE_KEY_COUNT: usize = 500;

/// Number of point lookups per measured iteration (production).
#[cfg(not(test))]
const PROD_PROBES_PER_ITER: usize = 1_000;

/// Fewer probes in test mode so tests are fast.
#[cfg(test)]
const TEST_PROBES_PER_ITER: usize = 10;

/// Smoke-mode probes per iteration.
#[cfg(not(test))]
const SMOKE_PROBES_PER_ITER: usize = 20;

/// In-memory buffer-pool loader that returns blank heap pages.
///
/// The `BTree` initialises each freshly-allocated block immediately
/// after a cache miss, so the loader's page contents are not read;
/// it only needs to never fail.
#[derive(Debug, Default)]
struct BlankLoader;

impl PageLoader for BlankLoader {
    fn load(&self, _page_id: PageId) -> ultrasql_core::Result<Page> {
        Ok(Page::new_heap())
    }
}

fn shuffle_index(seed: u64, upper_bound: usize) -> usize {
    let upper_bound_u64 = u64::try_from(upper_bound).unwrap_or(u64::MAX).max(1);
    let reduced = seed % upper_bound_u64;
    usize::try_from(reduced).unwrap_or(0)
}

/// Builds a `BTree<i64>` with `n` deterministically inserted keys.
///
/// Keys are shuffled with an xorshift64 before insertion to avoid
/// pathological worst-case splits. The frame count is sized to avoid
/// any eviction during the build phase.
fn build_tree(n: usize) -> BTree<BlankLoader> {
    // Budget: ~n / 16 leaf pages + headroom for internal pages.
    let frames = (n / 12).max(1) + 4_096;
    let pool = Arc::new(BufferPool::new(frames, BlankLoader));
    let mut tree = require_bench_ok(
        BTree::create(Arc::clone(&pool), RelationId::new(42)),
        "BTree::create",
    );

    let n_i64 = i64::try_from(n).unwrap_or(i64::MAX);
    let mut perm: Vec<i64> = (0..n_i64).collect();

    // Fisher-Yates shuffle via xorshift64.
    let mut s: u64 = 0xDEAD_BEEF_CAFE_F00D;
    for i in (1..perm.len()).rev() {
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        let j = shuffle_index(s, i + 1);
        perm.swap(i, j);
    }

    for &k in &perm {
        let block = u32::try_from(k & 0xFFFF_FFFF).unwrap_or(0);
        let slot = u16::try_from(k.wrapping_mul(31) & 0xFFFF).unwrap_or(0);
        let tid = TupleId::new(
            PageId::new(RelationId::new(42), BlockNumber::new(block)),
            slot,
        );
        require_bench_ok(
            tree.insert::<i64>(k, tid, Xid::FIRST_USER, None),
            "BTree insert during build",
        );
    }

    tree
}

/// Generates `count` random probe keys in `[0, n)` via xorshift64.
fn gen_probe_keys(count: usize, n: usize, seed: u64) -> Vec<i64> {
    let mut s = seed;
    let n_i64 = i64::try_from(n).unwrap_or(i64::MAX);
    let mut keys = Vec::with_capacity(count);
    for _ in 0..count {
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        let raw = i64::from_ne_bytes(s.to_ne_bytes());
        keys.push(raw.rem_euclid(n_i64));
    }
    keys
}

/// Runs the point-lookup benchmark.
///
/// Setup (outside the timed region): build a `BTree` with `KEY_COUNT`
/// keys, generate `PROBES_PER_ITER` probe keys.
///
/// Each measured iteration: perform `PROBES_PER_ITER` lookups and
/// discard results via `black_box`.
pub fn run(ctx: &BenchContext) -> BenchResult {
    #[cfg(test)]
    let key_count = TEST_KEY_COUNT;
    #[cfg(not(test))]
    let key_count = crate::runs::smoke_row_count(PROD_KEY_COUNT, SMOKE_KEY_COUNT);

    #[cfg(test)]
    let probes_per_iter = TEST_PROBES_PER_ITER;
    #[cfg(not(test))]
    let probes_per_iter = crate::runs::smoke_row_count(PROD_PROBES_PER_ITER, SMOKE_PROBES_PER_ITER);

    let tree = build_tree(key_count);
    let probes = gen_probe_keys(probes_per_iter, key_count, 0xCAFE_BABE_1234_5678);

    let timed_iter = |t: &BTree<BlankLoader>, keys: &[i64]| -> f64 {
        let t0 = Instant::now();
        for &k in keys {
            let v = t.lookup::<i64>(k).unwrap_or(None);
            std::hint::black_box(v);
        }
        let elapsed = t0.elapsed();
        elapsed.as_secs_f64() * 1_000_000.0 // µs
    };

    for _ in 0..ctx.warmup_iterations {
        timed_iter(&tree, &probes);
    }

    let iteration_count = usize::try_from(ctx.iterations).unwrap_or(0);
    let mut samples: Vec<f64> = Vec::with_capacity(iteration_count);
    for _ in 0..ctx.iterations {
        samples.push(timed_iter(&tree, &probes));
    }

    let median_us = median_f64(&samples);
    let p99_us = p99_f64(&samples);
    let ops = probes_per_iter as f64;
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
        assert_eq!(
            result.samples.len(),
            usize::try_from(ctx.iterations).unwrap_or(0)
        );
        assert!(result.throughput_per_sec > 0.0);
    }
}
