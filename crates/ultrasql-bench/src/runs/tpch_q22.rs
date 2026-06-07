//! TPC-H Query 22 benchmark implementation.
//!
//! Approximates:
//! ```sql
//! SELECT cntrycode,
//!        COUNT(*) AS numcust,
//!        SUM(c_acctbal) AS totacctbal
//! FROM (
//!   SELECT SUBSTRING(c_phone FROM 1 FOR 2) AS cntrycode, c_acctbal
//!   FROM customer
//!   WHERE SUBSTRING(c_phone FROM 1 FOR 2) IN ('13','31','23','29','30','18','17')
//!     AND c_acctbal > (SELECT AVG(c_acctbal)
//!                      FROM customer
//!                      WHERE c_acctbal > 0.00
//!                        AND SUBSTRING(c_phone FROM 1 FOR 2) IN
//!                            ('13','31','23','29','30','18','17'))
//!     AND NOT EXISTS (SELECT * FROM orders WHERE o_custkey = c_custkey)
//! ) AS custsale
//! GROUP BY cntrycode
//! ORDER BY cntrycode;
//! ```
//!
//! The correlated subquery and NOT-EXISTS are approximated by:
//! 1. Computing the average `c_acctbal` among qualifying customers
//!    (those with a matching country code and `c_acctbal > 0`) in one pass.
//! 2. Running a semi-join filter: keep customers whose account balance
//!    exceeds the average and who have no matching order (simulated via
//!    a deterministic hash that marks ~70% of customers as having orders).
//! 3. Grouping the survivors by country code with `COUNT(*)` and `SUM(c_acctbal)`.
//!
//! Throughput = `CUSTOMER_COUNT / median_elapsed_seconds`.

use std::collections::HashMap;
use std::time::Instant;

use crate::registry::{BenchContext, BenchResult, median_f64, p99_f64};

/// Full production customer count: 100 000 customers.
#[cfg(not(test))]
const CUSTOMER_COUNT: usize = 100_000;

/// Reduced customer count for fast unit tests.
#[cfg(test)]
const CUSTOMER_COUNT: usize = 500;

/// The 7 country codes from Q22.
const COUNTRY_CODES: [&str; 7] = ["13", "31", "23", "29", "30", "18", "17"];

/// Synthetic customer row.
///
/// Fields correspond to TPC-H `CUSTOMER` columns.
struct Customer {
    /// Customer key (1-based).
    custkey: u64,
    /// First two characters of `c_phone`, mapped to country code index.
    phone_prefix_idx: usize,
    /// Account balance scaled by 100 (cents).
    acctbal: i64,
}

/// Generates `n` synthetic customers using a deterministic PRNG.
fn generate_customers(n: usize) -> Vec<Customer> {
    let mut s: u64 = 0xA1B2_C3D4_E5F6_0718;
    let mut rows = Vec::with_capacity(n);
    for i in 0..n {
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        // Phone prefix: 70% chance of matching one of the 7 codes.
        let prefix_idx = if s % 10 < 7 {
            rng_index(s >> 8, COUNTRY_CODES.len())
        } else {
            COUNTRY_CODES.len() // sentinel "not matching"
        };
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        // Account balance: -500..10000 (cents).
        let acctbal = i64::try_from(s % 10_501).unwrap_or(0) - 500;
        rows.push(Customer {
            custkey: u64::try_from(i + 1).unwrap_or(1),
            phone_prefix_idx: prefix_idx,
            acctbal,
        });
    }
    rows
}

fn rng_index(seed: u64, upper_bound: usize) -> usize {
    let upper_bound_u64 = u64::try_from(upper_bound).unwrap_or(u64::MAX).max(1);
    let reduced = seed % upper_bound_u64;
    usize::try_from(reduced).unwrap_or(0)
}

/// Simulates whether customer `custkey` has an order.
///
/// Approximately 70% of customers have at least one order, matching
/// the TPC-H scale-factor-1 distribution.
#[inline]
const fn has_order(custkey: u64) -> bool {
    // Deterministic hash: take every 3rd customer as having no orders.
    custkey % 3 != 0
}

/// Runs one Q22 pass and returns the result groups.
fn q22_pass(customers: &[Customer]) -> HashMap<usize, (i64, i64)> {
    // Step 1: compute average acctbal among qualifying customers with
    // positive balance.
    let mut sum_acctbal: i64 = 0;
    let mut count_pos: i64 = 0;
    for c in customers {
        if c.phone_prefix_idx < COUNTRY_CODES.len() && c.acctbal > 0 {
            sum_acctbal = sum_acctbal.wrapping_add(c.acctbal);
            count_pos = count_pos.wrapping_add(1);
        }
    }
    let avg_acctbal = if count_pos > 0 {
        sum_acctbal.wrapping_div(count_pos)
    } else {
        0
    };

    // Step 2: filter and group.
    let mut table: HashMap<usize, (i64, i64)> = HashMap::with_capacity(COUNTRY_CODES.len() * 2);
    for c in customers {
        if c.phone_prefix_idx >= COUNTRY_CODES.len() {
            continue;
        }
        if c.acctbal <= avg_acctbal {
            continue;
        }
        if has_order(c.custkey) {
            continue; // NOT EXISTS filter
        }
        let entry = table.entry(c.phone_prefix_idx).or_insert((0, 0));
        entry.0 = entry.0.wrapping_add(1);
        entry.1 = entry.1.wrapping_add(c.acctbal);
    }

    table
}

/// Runs the TPC-H Q22 benchmark.
pub fn run(ctx: &BenchContext) -> BenchResult {
    let customers = generate_customers(CUSTOMER_COUNT);

    let timed_iter = |data: &[Customer]| -> f64 {
        let t0 = Instant::now();
        let result = q22_pass(data);
        let elapsed = t0.elapsed();
        std::hint::black_box(&result);
        elapsed.as_secs_f64() * 1_000_000.0 // µs
    };

    for _ in 0..ctx.warmup_iterations {
        timed_iter(&customers);
    }

    let mut samples: Vec<f64> = Vec::with_capacity(ctx.iterations as usize);
    for _ in 0..ctx.iterations {
        samples.push(timed_iter(&customers));
    }

    let median_us = median_f64(&samples);
    let p99_us = p99_f64(&samples);
    let count = CUSTOMER_COUNT as f64;
    let throughput_per_sec = if median_us > 0.0 {
        count / (median_us / 1_000_000.0)
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

    #[test]
    fn q22_pass_produces_at_most_seven_groups() {
        let customers = generate_customers(2_000);
        let table = q22_pass(&customers);
        assert!(table.len() <= COUNTRY_CODES.len());
    }
}
