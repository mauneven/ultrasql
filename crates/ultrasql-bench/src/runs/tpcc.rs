//! `tpcc_5types` local kernel benchmark.
//!
//! This benchmark exercises the five TPC-C transaction families over a
//! deterministic in-memory state model:
//!
//! - New-Order,
//! - Payment,
//! - Order-Status,
//! - Delivery,
//! - Stock-Level.
//!
//! Publishable v1.0 certification still belongs to
//! `benchmarks/tpcc_certify.sh`, because the release gate must compare
//! UltraSQL and PostgreSQL through concurrent PostgreSQL-wire sessions on
//! the same host. This local kernel removes the former zero-throughput
//! placeholder from the regression gate and catches obvious regressions in
//! the transaction mix.

use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};

use crate::registry::{BenchContext, BenchResult, median_f64, p99_f64};

#[cfg(not(test))]
const CLIENTS: usize = 32;
#[cfg(test)]
const CLIENTS: usize = 4;

#[cfg(not(test))]
const TX_PER_CLIENT_ITER: usize = 500;
#[cfg(test)]
const TX_PER_CLIENT_ITER: usize = 64;

#[cfg(not(test))]
const WAREHOUSES: usize = 8;
#[cfg(test)]
const WAREHOUSES: usize = 2;

const DISTRICTS_PER_WAREHOUSE: usize = 10;

#[cfg(not(test))]
const CUSTOMERS_PER_DISTRICT: usize = 3_000;
#[cfg(test)]
const CUSTOMERS_PER_DISTRICT: usize = 64;

#[cfg(not(test))]
const ITEMS: usize = 10_000;
#[cfg(test)]
const ITEMS: usize = 256;

/// Runs the deterministic five-transaction TPC-C-shaped local benchmark.
pub fn run(ctx: &BenchContext) -> BenchResult {
    let state = TpccState::new();
    let mut samples = Vec::with_capacity(ctx.iterations as usize);
    let mut seed = 0x1EAF_CAFE_BADC_0DE5_u64;

    for _ in 0..ctx.warmup_iterations {
        seed = xorshift64(seed);
        let _ = run_iteration(&state, seed);
    }

    for _ in 0..ctx.iterations {
        seed = xorshift64(seed);
        samples.push(run_iteration(&state, seed));
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

fn run_iteration(state: &TpccState, seed: u64) -> f64 {
    let started = std::time::Instant::now();
    let mut checksum = 0_u64;
    std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(CLIENTS);
        for client in 0..CLIENTS {
            handles.push(scope.spawn(move || {
                let client_seed = seed ^ ((client as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15));
                run_client(state, client_seed)
            }));
        }
        for handle in handles {
            if let Ok(value) = handle.join() {
                checksum ^= value;
            }
        }
    });
    std::hint::black_box(checksum);
    started.elapsed().as_secs_f64() * 1_000_000.0
}

fn run_client(state: &TpccState, mut seed: u64) -> u64 {
    let mut checksum = 0_u64;
    for tx in 0..TX_PER_CLIENT_ITER {
        seed = xorshift64(seed);
        let selector = (seed % 100) as u8;
        checksum = checksum.wrapping_add(match transaction_kind(selector) {
            TransactionKind::NewOrder => state.new_order(seed ^ tx as u64),
            TransactionKind::Payment => state.payment(seed ^ tx as u64),
            TransactionKind::OrderStatus => state.order_status(seed ^ tx as u64),
            TransactionKind::Delivery => state.delivery(seed ^ tx as u64),
            TransactionKind::StockLevel => state.stock_level(seed ^ tx as u64),
        });
    }
    checksum
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TransactionKind {
    NewOrder,
    Payment,
    OrderStatus,
    Delivery,
    StockLevel,
}

fn transaction_kind(selector: u8) -> TransactionKind {
    match selector {
        0..=44 => TransactionKind::NewOrder,
        45..=87 => TransactionKind::Payment,
        88..=91 => TransactionKind::OrderStatus,
        92..=95 => TransactionKind::Delivery,
        _ => TransactionKind::StockLevel,
    }
}

struct TpccState {
    warehouse_ytd: Vec<AtomicI64>,
    district_ytd: Vec<AtomicI64>,
    district_next_order: Vec<AtomicUsize>,
    district_next_delivery: Vec<AtomicUsize>,
    customer_balance: Vec<AtomicI64>,
    customer_last_order: Vec<AtomicUsize>,
    stock_quantity: Vec<AtomicI64>,
}

impl TpccState {
    fn new() -> Self {
        let district_count = WAREHOUSES * DISTRICTS_PER_WAREHOUSE;
        let customer_count = district_count * CUSTOMERS_PER_DISTRICT;
        let stock_count = WAREHOUSES * ITEMS;
        Self {
            warehouse_ytd: (0..WAREHOUSES).map(|_| AtomicI64::new(0)).collect(),
            district_ytd: (0..district_count).map(|_| AtomicI64::new(0)).collect(),
            district_next_order: (0..district_count).map(|_| AtomicUsize::new(1)).collect(),
            district_next_delivery: (0..district_count).map(|_| AtomicUsize::new(1)).collect(),
            customer_balance: (0..customer_count).map(|_| AtomicI64::new(0)).collect(),
            customer_last_order: (0..customer_count).map(|_| AtomicUsize::new(0)).collect(),
            stock_quantity: (0..stock_count).map(|_| AtomicI64::new(100)).collect(),
        }
    }

    fn new_order(&self, seed: u64) -> u64 {
        let warehouse = choose(seed, WAREHOUSES);
        let district = choose(seed >> 8, DISTRICTS_PER_WAREHOUSE);
        let customer = choose(seed >> 16, CUSTOMERS_PER_DISTRICT);
        let district_index = district_index(warehouse, district);
        let customer_index = customer_index(warehouse, district, customer);
        let order_id = self.district_next_order[district_index].fetch_add(1, Ordering::Relaxed);
        self.customer_last_order[customer_index].store(order_id, Ordering::Relaxed);

        let mut checksum = order_id as u64;
        for line in 0..5 {
            let item = choose(seed.rotate_left(line + 1), ITEMS);
            let stock_index = stock_index(warehouse, item);
            let before = self.stock_quantity[stock_index].fetch_sub(1, Ordering::Relaxed);
            if before <= 0 {
                self.stock_quantity[stock_index].store(100, Ordering::Relaxed);
            }
            checksum = checksum.wrapping_add(before as u64);
        }
        checksum
    }

    fn payment(&self, seed: u64) -> u64 {
        let warehouse = choose(seed, WAREHOUSES);
        let district = choose(seed >> 8, DISTRICTS_PER_WAREHOUSE);
        let customer = choose(seed >> 16, CUSTOMERS_PER_DISTRICT);
        let amount = i64::try_from((seed % 5_000) + 1).unwrap_or(1);
        let district_index = district_index(warehouse, district);
        let customer_index = customer_index(warehouse, district, customer);
        let warehouse_ytd = self.warehouse_ytd[warehouse].fetch_add(amount, Ordering::Relaxed);
        let district_ytd = self.district_ytd[district_index].fetch_add(amount, Ordering::Relaxed);
        let balance = self.customer_balance[customer_index].fetch_sub(amount, Ordering::Relaxed);
        (warehouse_ytd as u64) ^ (district_ytd as u64) ^ (balance as u64)
    }

    fn order_status(&self, seed: u64) -> u64 {
        let warehouse = choose(seed, WAREHOUSES);
        let district = choose(seed >> 8, DISTRICTS_PER_WAREHOUSE);
        let customer = choose(seed >> 16, CUSTOMERS_PER_DISTRICT);
        let district_index = district_index(warehouse, district);
        let customer_index = customer_index(warehouse, district, customer);
        let balance = self.customer_balance[customer_index].load(Ordering::Relaxed);
        let last_order = self.customer_last_order[customer_index].load(Ordering::Relaxed);
        let next_order = self.district_next_order[district_index].load(Ordering::Relaxed);
        (balance as u64) ^ (last_order as u64) ^ (next_order as u64)
    }

    fn delivery(&self, seed: u64) -> u64 {
        let warehouse = choose(seed, WAREHOUSES);
        let district = choose(seed >> 8, DISTRICTS_PER_WAREHOUSE);
        let customer = choose(seed >> 16, CUSTOMERS_PER_DISTRICT);
        let district_index = district_index(warehouse, district);
        let customer_index = customer_index(warehouse, district, customer);
        let delivered_order =
            self.district_next_delivery[district_index].fetch_add(1, Ordering::Relaxed);
        let balance = self.customer_balance[customer_index].fetch_add(10, Ordering::Relaxed);
        (delivered_order as u64) ^ (balance as u64)
    }

    fn stock_level(&self, seed: u64) -> u64 {
        let warehouse = choose(seed, WAREHOUSES);
        let start_item = choose(seed >> 16, ITEMS);
        let threshold = i64::try_from((seed % 20) + 10).unwrap_or(10);
        let mut low = 0_u64;
        for offset in 0..20 {
            let item = (start_item + offset) % ITEMS;
            let stock_index = stock_index(warehouse, item);
            if self.stock_quantity[stock_index].load(Ordering::Relaxed) < threshold {
                low += 1;
            }
        }
        low
    }
}

fn district_index(warehouse: usize, district: usize) -> usize {
    (warehouse % WAREHOUSES) * DISTRICTS_PER_WAREHOUSE + (district % DISTRICTS_PER_WAREHOUSE)
}

fn customer_index(warehouse: usize, district: usize, customer: usize) -> usize {
    district_index(warehouse, district) * CUSTOMERS_PER_DISTRICT
        + (customer % CUSTOMERS_PER_DISTRICT)
}

fn stock_index(warehouse: usize, item: usize) -> usize {
    (warehouse % WAREHOUSES) * ITEMS + (item % ITEMS)
}

fn choose(seed: u64, cardinality: usize) -> usize {
    (seed as usize) % cardinality
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
    fn transaction_mix_covers_all_five_types() {
        assert_eq!(transaction_kind(0), TransactionKind::NewOrder);
        assert_eq!(transaction_kind(45), TransactionKind::Payment);
        assert_eq!(transaction_kind(88), TransactionKind::OrderStatus);
        assert_eq!(transaction_kind(92), TransactionKind::Delivery);
        assert_eq!(transaction_kind(99), TransactionKind::StockLevel);
    }

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
