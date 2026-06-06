//! TPC-H Query 1 benchmark implementation.
//!
//! Simulates:
//! ```sql
//! SELECT l_returnflag, l_linestatus,
//!        SUM(l_quantity)                                    AS sum_qty,
//!        SUM(l_extendedprice)                               AS sum_base_price,
//!        SUM(l_extendedprice * (1 - l_discount))            AS sum_disc_price,
//!        SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
//!        AVG(l_quantity)                                    AS avg_qty,
//!        COUNT(*)                                           AS count_order
//! FROM lineitem
//! WHERE l_shipdate <= DATE '1998-09-01'
//! GROUP BY l_returnflag, l_linestatus
//! ORDER BY l_returnflag, l_linestatus;
//! ```
//!
//! The implementation synthesises a 100 000-row `lineitem` table (or
//! `TEST_ROWS` rows in test mode), applies the date filter, and performs
//! the group-by + 5-aggregate pipeline using a `HashMap<(u8, u8), AggState>`.
//!
//! Throughput = `ROW_COUNT / median_elapsed_seconds`.

use std::collections::HashMap;
use std::time::Instant;

use crate::registry::{BenchContext, BenchResult, median_f64, p99_f64};

/// Full production row count: 100 000 lineitem rows.
#[cfg(not(test))]
const PROD_ROW_COUNT: usize = 100_000;

/// Reduced row count for fast unit tests.
#[cfg(test)]
const TEST_ROW_COUNT: usize = 500;

/// Smoke-mode row count (used when `ULTRASQL_BENCH_SMOKE` is set).
#[cfg(not(test))]
const SMOKE_ROW_COUNT: usize = 500;

/// Epoch day cut-off: `1998-09-01` in "days since 1970-01-01".
///
/// Computed as: 28 years × 365 + 7 leap years + month offsets.
/// 1998-09-01 = 1970-01-01 + 10471 days.
const CUTOFF_DAY: i32 = 10_471;

/// Aggregate state per (returnflag, linestatus) group.
#[derive(Default)]
struct AggState {
    count: i64,
    sum_qty: i64,
    sum_base_price: i64,
    sum_disc_price: i64,
    sum_charge: i64,
}

/// Synthesised lineitem row.
///
/// Field names mirror the TPC-H `LINEITEM` schema column names.
#[allow(clippy::struct_field_names)]
struct LineItem {
    l_returnflag: u8,    // 'N', 'R', 'A'
    l_linestatus: u8,    // 'O', 'F'
    l_shipdate_day: i32, // days since 1970-01-01
    l_quantity: i64,     // cents-scaled
    l_extendedprice: i64,
    l_discount: i64, // scaled by 100 (e.g. 5 = 5%)
    l_tax: i64,      // scaled by 100
}

/// Generates `n` synthetic lineitem rows using a deterministic PRNG.
fn generate_lineitem(n: usize) -> Vec<LineItem> {
    let mut s: u64 = 0x1234_5678_9ABC_DEF0;
    let mut rows = Vec::with_capacity(n);

    for _ in 0..n {
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;

        // returnflag: 3 possible values.
        let rf = match s % 3 {
            0 => b'N',
            1 => b'R',
            _ => b'A',
        };
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;

        // linestatus: 2 possible values.
        let ls = if s % 2 == 0 { b'O' } else { b'F' };
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;

        // shipdate: spread across 1996-01-01..1998-12-31 (days 9497..10592).
        // Most rows should be <= CUTOFF_DAY (1998-09-01 = 10471).
        let day_off = i32::try_from(s % 1096).unwrap_or(0);
        let shipdate = 9_497_i32 + day_off;
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;

        // quantity: 1..50
        let qty = i64::try_from((s % 50) + 1).unwrap_or(1);
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;

        // extendedprice: qty * unit_price; unit_price 100..200 (cents)
        let unit_price = i64::try_from((s % 101) + 100).unwrap_or(100);
        let extprice = qty.wrapping_mul(unit_price);
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;

        // discount: 0..10 (percent)
        let discount = i64::try_from(s % 11).unwrap_or(0);
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;

        // tax: 0..8 (percent)
        let tax = i64::try_from(s % 9).unwrap_or(0);

        rows.push(LineItem {
            l_returnflag: rf,
            l_linestatus: ls,
            l_shipdate_day: shipdate,
            l_quantity: qty,
            l_extendedprice: extprice,
            l_discount: discount,
            l_tax: tax,
        });
    }
    rows
}

/// Runs TPC-H Q1 over the synthetic lineitem table.
///
/// Each iteration: filter on `shipdate <= CUTOFF_DAY`, then aggregate.
fn q1_pass(rows: &[LineItem]) -> HashMap<(u8, u8), AggState> {
    let mut table: HashMap<(u8, u8), AggState> = HashMap::with_capacity(8);

    for r in rows {
        if r.l_shipdate_day > CUTOFF_DAY {
            continue;
        }
        let key = (r.l_returnflag, r.l_linestatus);
        let agg = table.entry(key).or_default();

        agg.count = agg.count.wrapping_add(1);
        agg.sum_qty = agg.sum_qty.wrapping_add(r.l_quantity);
        agg.sum_base_price = agg.sum_base_price.wrapping_add(r.l_extendedprice);

        // disc_price = extendedprice * (100 - discount) / 100
        let disc_price = r.l_extendedprice.wrapping_mul(100 - r.l_discount) / 100;
        agg.sum_disc_price = agg.sum_disc_price.wrapping_add(disc_price);

        // charge = disc_price * (100 + tax) / 100
        let charge = disc_price.wrapping_mul(100 + r.l_tax) / 100;
        agg.sum_charge = agg.sum_charge.wrapping_add(charge);
    }

    table
}

/// Runs the TPC-H Q1 benchmark.
pub fn run(ctx: &BenchContext) -> BenchResult {
    #[cfg(test)]
    let row_count = TEST_ROW_COUNT;
    #[cfg(not(test))]
    let row_count = crate::runs::smoke_row_count(PROD_ROW_COUNT, SMOKE_ROW_COUNT);

    let rows = generate_lineitem(row_count);

    let timed_iter = |data: &[LineItem]| -> f64 {
        let t0 = Instant::now();
        let result = q1_pass(data);
        let elapsed = t0.elapsed();
        std::hint::black_box(&result);
        elapsed.as_secs_f64() * 1_000_000.0 // µs
    };

    for _ in 0..ctx.warmup_iterations {
        timed_iter(&rows);
    }

    let mut samples: Vec<f64> = Vec::with_capacity(ctx.iterations as usize);
    for _ in 0..ctx.iterations {
        samples.push(timed_iter(&rows));
    }

    let median_us = median_f64(&samples);
    let p99_us = p99_f64(&samples);
    let row_count_f = row_count as f64;
    let throughput_per_sec = if median_us > 0.0 {
        row_count_f / (median_us / 1_000_000.0)
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
    fn q1_pass_produces_at_most_six_groups() {
        let rows = generate_lineitem(2_000);
        let table = q1_pass(&rows);
        // At most 3 returnflag × 2 linestatus = 6 groups, but the date
        // filter may reduce this further.
        assert!(table.len() <= 6);
    }
}
