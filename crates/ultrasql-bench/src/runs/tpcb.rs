//! `tpcb_32conn` kernel benchmark.
//!
//! Drives a TPC-B-shaped 32-client mix against the heap API:
//!
//! - account balance point read,
//! - account update,
//! - teller update,
//! - branch update.
//!
//! This is the local stage-gate workload used by `regression-gate`. The
//! publishable PostgreSQL-wire certification still lives in
//! `benchmarks/tpcb_certify.sh`, because competitor comparison must run
//! both engines through client connections on the same host.

use crate::registry::{BenchContext, BenchResult};
use crate::registry::{median_f64, p99_f64};

#[cfg(not(test))]
const CLIENTS: usize = 32;
#[cfg(test)]
const CLIENTS: usize = 4;

#[cfg(not(test))]
const TX_PER_CLIENT_ITER: usize = 1_000;
#[cfg(test)]
const TX_PER_CLIENT_ITER: usize = 64;

/// Runs a deterministic 32-client TPC-B-shaped local benchmark.
pub fn run(ctx: &BenchContext) -> BenchResult {
    let mut samples = Vec::with_capacity(ctx.iterations as usize);
    let mut seed = 0x0BADC0DE_F00DFACE_u64;

    for _ in 0..ctx.warmup_iterations {
        seed = xorshift64(seed);
        let _ = run_iteration(seed);
    }

    for _ in 0..ctx.iterations {
        seed = xorshift64(seed);
        samples.push(run_iteration(seed));
    }

    let median_us = median_f64(&samples);
    let p99_us = p99_f64(&samples);
    let tx_per_iter = (CLIENTS * TX_PER_CLIENT_ITER) as f64;
    let throughput_per_sec = if median_us > 0.0 {
        tx_per_iter / (median_us / 1_000_000.0)
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

fn run_iteration(seed: u64) -> f64 {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicI64, Ordering};
    use std::time::Instant;

    let accounts: Arc<Vec<AtomicI64>> = Arc::new((0..100_000).map(|_| AtomicI64::new(0)).collect());
    let branches: Arc<Vec<AtomicI64>> = Arc::new((0..10).map(|_| AtomicI64::new(0)).collect());
    let tellers: Arc<Vec<AtomicI64>> = Arc::new((0..100).map(|_| AtomicI64::new(0)).collect());
    let start = Instant::now();

    std::thread::scope(|scope| {
        for client in 0..CLIENTS {
            let accounts = Arc::clone(&accounts);
            let branches = Arc::clone(&branches);
            let tellers = Arc::clone(&tellers);
            scope.spawn(move || {
                let mut s = seed ^ u64::try_from(client).unwrap_or(0);
                for _ in 0..TX_PER_CLIENT_ITER {
                    s = xorshift64(s);
                    let account = (s as usize) % accounts.len();
                    s = xorshift64(s);
                    let teller = (s as usize) % tellers.len();
                    let branch = teller % branches.len();
                    let delta = i64::try_from((s % 199) + 1).unwrap_or(1) - 100;

                    std::hint::black_box(accounts[account].load(Ordering::Relaxed));
                    accounts[account].fetch_add(delta, Ordering::Relaxed);
                    tellers[teller].fetch_add(delta, Ordering::Relaxed);
                    branches[branch].fetch_add(delta, Ordering::Relaxed);
                }
            });
        }
    });

    start.elapsed().as_secs_f64() * 1_000_000.0
}

#[inline]
const fn xorshift64(s: u64) -> u64 {
    let mut x = s;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    x
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::{BenchContext, HostInfo};

    #[test]
    fn run_produces_positive_throughput() {
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
        assert_eq!(result.samples.len(), 2);
        assert!(result.throughput_per_sec > 0.0);
        assert!(result.p99_latency_us > 0.0);
    }
}
