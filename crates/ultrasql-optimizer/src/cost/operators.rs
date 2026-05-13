//! Per-operator cost formulas.
//!
//! Each function corresponds to one physical or logical plan operator and
//! returns a [`CostEstimate`] for that operator given its inputs and the
//! active [`CostGucs`].
//!
//! These formulas are modelled after PostgreSQL's cost model. The intent for
//! v0.6 is to produce plan comparisons that are directionally correct; the
//! exact constants match PG's defaults so that the two optimizers can be
//! compared on equal footing.

use crate::cost::{CostEstimate, CostGucs, StatsSource};

// ============================================================================
// Sequential scan
// ============================================================================

/// Cost of a full sequential heap scan on `table`.
///
/// Formula: `pages * seq_page_cost + rows * cpu_tuple_cost`.
/// Startup cost is 0 because the first row is available immediately.
///
/// The `as f64` casts here are int-to-float conversions, not integer-width
/// casts; they are permitted by AGENTS.md §3.3.
pub fn cost_scan(stats: &dyn StatsSource, table: &str, gucs: &CostGucs) -> CostEstimate {
    let rows = stats.row_count(table) as f64;
    let pages = stats.page_count(table) as f64;
    CostEstimate {
        total_cost: pages.mul_add(gucs.seq_page_cost, rows * gucs.cpu_tuple_cost),
        startup_cost: 0.0,
        rows,
        width: 100, // approximate; refined when column-level stats land
    }
}

// ============================================================================
// Index scan
// ============================================================================

/// Cost of a B-tree index scan with `selectivity` applied to the base rows.
///
/// - Startup cost: `index_height * random_page_cost` (root-to-leaf traversal).
/// - Total cost: startup + `pages * random_page_cost`
///   + `rows * (cpu_tuple_cost + cpu_index_tuple_cost)`.
///
/// `pages` is estimated as `ceil(rows / 100)`, which assumes 100 index
/// entries fit in one page — a conservative heuristic for v0.6.
pub fn cost_index_scan(
    stats: &dyn StatsSource,
    table: &str,
    index_height: u32,
    selectivity: f64,
    gucs: &CostGucs,
) -> CostEstimate {
    let total_rows = stats.row_count(table) as f64;
    let rows = total_rows * selectivity;
    // Conservative: 100 tuples per page.
    let pages = (rows / 100.0).ceil();
    let startup = f64::from(index_height) * gucs.random_page_cost;
    CostEstimate {
        total_cost: rows.mul_add(
            gucs.cpu_tuple_cost + gucs.cpu_index_tuple_cost,
            startup + pages * gucs.random_page_cost,
        ),
        startup_cost: startup,
        rows,
        width: 100,
    }
}

// ============================================================================
// Nested-loop join
// ============================================================================

/// Cost of a nested-loop join.
///
/// - Startup: 0.
/// - Total: `left.total + left.rows * right.total + output_rows * cpu_tuple_cost`.
///
/// The `left.rows * right.total` term models scanning the right side once per
/// left row; this is the dominant cost for NLJ.
pub fn cost_nested_loop(
    left: CostEstimate,
    right: CostEstimate,
    join_sel: f64,
    gucs: &CostGucs,
) -> CostEstimate {
    let rows = (left.rows * right.rows * join_sel).max(0.0);
    CostEstimate {
        total_cost: left.rows.mul_add(right.total_cost, left.total_cost)
            + rows * gucs.cpu_tuple_cost,
        startup_cost: 0.0,
        rows,
        width: left.width.saturating_add(right.width),
    }
}

// ============================================================================
// Hash join
// ============================================================================

/// Cost of a hash join (right side is the build side).
///
/// - Build cost: `right.total + right.rows * cpu_operator_cost * 2.0`
///   (factor of 2 accounts for hashing and storing).
/// - Probe cost: `left.rows * cpu_operator_cost`.
/// - Output cost: `output_rows * cpu_tuple_cost`.
/// - Startup cost: `build_cost` (the hash build is a pipeline breaker).
pub fn cost_hash_join(
    left: CostEstimate,
    right: CostEstimate,
    join_sel: f64,
    gucs: &CostGucs,
) -> CostEstimate {
    let build_cost = (right.rows * gucs.cpu_operator_cost).mul_add(2.0, right.total_cost);
    let probe_cost = left.rows * gucs.cpu_operator_cost;
    let rows = (left.rows * right.rows * join_sel).max(0.0);
    CostEstimate {
        total_cost: left.total_cost + build_cost + probe_cost + rows * gucs.cpu_tuple_cost,
        startup_cost: build_cost,
        rows,
        width: left.width.saturating_add(right.width),
    }
}

// ============================================================================
// Merge join
// ============================================================================

/// Cost of a sort-merge join.
///
/// Assumes both inputs arrive pre-sorted (or have already been costed
/// through [`cost_sort`]). The merge pass itself is linear in the output.
///
/// - Startup: `max(left.startup, right.startup)` (both inputs must deliver
///   their first sorted row before the merge can start).
/// - Total: `left.total + right.total + output_rows * cpu_operator_cost`.
pub fn cost_merge_join(
    left: CostEstimate,
    right: CostEstimate,
    join_sel: f64,
    gucs: &CostGucs,
) -> CostEstimate {
    let rows = (left.rows * right.rows * join_sel).max(0.0);
    let startup = left.startup_cost.max(right.startup_cost);
    CostEstimate {
        total_cost: left.total_cost + right.total_cost + rows * gucs.cpu_operator_cost,
        startup_cost: startup,
        rows,
        width: left.width.saturating_add(right.width),
    }
}

// ============================================================================
// Sort
// ============================================================================

/// Cost of an external sort.
///
/// Formula (approximate comparison sort):
/// `sort_cpu = n * log2(n) * cpu_operator_cost`.
///
/// Sort is a pipeline breaker: both `startup_cost` and `total_cost` equal
/// `input.total_cost + sort_cpu` because no rows are delivered until the sort
/// finishes.
pub fn cost_sort(input: CostEstimate, gucs: &CostGucs) -> CostEstimate {
    let n = input.rows.max(1.0);
    let sort_cpu = n * n.log2() * gucs.cpu_operator_cost;
    let total = input.total_cost + sort_cpu;
    CostEstimate {
        total_cost: total,
        startup_cost: total, // sort fully buffers before emitting rows
        rows: input.rows,
        width: input.width,
    }
}

// ============================================================================
// Aggregate
// ============================================================================

/// Cost of a hash or sort aggregate.
///
/// Formula:
/// - Total: `input.total + input.rows * cpu_operator_cost`
///   + `n_groups * cpu_tuple_cost`.
/// - Startup: `input.startup` (hash agg streams rows in the simple model).
///
/// For sort-aggregate callers should first pass the input through
/// [`cost_sort`] before calling this function.
pub fn cost_aggregate(input: CostEstimate, n_groups: f64, gucs: &CostGucs) -> CostEstimate {
    CostEstimate {
        total_cost: n_groups.mul_add(
            gucs.cpu_tuple_cost,
            input.rows.mul_add(gucs.cpu_operator_cost, input.total_cost),
        ),
        startup_cost: input.startup_cost,
        rows: n_groups,
        width: input.width,
    }
}

// ============================================================================
// Bitmap heap scan
// ============================================================================

/// Cost of a bitmap heap scan preceded by one or more bitmap index scans.
///
/// The bitmap index scan phase costs like a regular index scan but uses
/// sequential I/O for the heap portion: once the bitmap is built, pages are
/// fetched in physical order, amortising random-access overhead.
///
/// - Bitmap build cost: `startup_cost` of `cost_index_scan` (root-to-leaf
///   traversal) plus `match_rows * cpu_index_tuple_cost`.
/// - Heap fetch cost: `heap_pages * seq_page_cost + rows * cpu_tuple_cost`,
///   because pages are read in order (not random).
///
/// `heap_pages` is estimated as `ceil(rows / 100)` (same heuristic as index
/// scan). `rows` = `total_rows * selectivity`.
///
/// This is preferred over a plain `IndexScan` when selectivity ∈ [0.5%, 10%]
/// or when two or more indexes apply.
pub fn cost_bitmap_heap_scan(
    stats: &dyn StatsSource,
    table: &str,
    index_height: u32,
    selectivity: f64,
    gucs: &CostGucs,
) -> CostEstimate {
    let total_rows = stats.row_count(table) as f64;
    let rows = total_rows * selectivity;
    let heap_pages = (rows / 100.0).ceil();
    let startup = rows.mul_add(
        gucs.cpu_index_tuple_cost,
        f64::from(index_height) * gucs.random_page_cost,
    );
    let heap_cost = heap_pages.mul_add(gucs.seq_page_cost, rows * gucs.cpu_tuple_cost);
    CostEstimate {
        total_cost: startup + heap_cost,
        startup_cost: startup,
        rows,
        width: 100,
    }
}

// ============================================================================
// Index-only scan
// ============================================================================

/// Cost of an index-only scan (no heap fetch required).
///
/// When the visibility map indicates all pages are all-visible, the index
/// entry itself contains all required column values and no heap page needs
/// to be read.
///
/// Formula: same as [`cost_index_scan`] but with `random_page_cost` for heap
/// replaced by zero, since no heap I/O is performed.
///
/// - Startup cost: `index_height * random_page_cost`.
/// - Total cost: startup + `index_pages * random_page_cost`
///   + `rows * cpu_index_tuple_cost`.
pub fn cost_index_only_scan(
    stats: &dyn StatsSource,
    table: &str,
    index_height: u32,
    selectivity: f64,
    gucs: &CostGucs,
) -> CostEstimate {
    let total_rows = stats.row_count(table) as f64;
    let rows = total_rows * selectivity;
    let index_pages = (rows / 100.0).ceil();
    let startup = f64::from(index_height) * gucs.random_page_cost;
    CostEstimate {
        total_cost: rows.mul_add(gucs.cpu_index_tuple_cost, startup + index_pages * gucs.random_page_cost),
        startup_cost: startup,
        rows,
        width: 100,
    }
}

// ============================================================================
// Parallel cost annotation
// ============================================================================

/// Apply a parallel-query cost annotation to an existing `CostEstimate`.
///
/// The total cost is divided by `workers` (modelling perfect parallel speedup),
/// then `parallel_setup_cost` is added to the startup cost to account for
/// worker spawning overhead.
///
/// This function annotates the *planner's estimate* only; it does not
/// implement actual parallel execution.  Physical operators remain unchanged;
/// the annotation influences join enumeration and plan selection when
/// `workers > 1`.
///
/// If `workers` is 0 or 1, the estimate is returned unchanged.
pub fn annotate_parallel(
    input: CostEstimate,
    workers: usize,
    gucs: &CostGucs,
) -> CostEstimate {
    if workers <= 1 {
        return input;
    }
    let w = workers as f64;
    CostEstimate {
        total_cost: input.total_cost / w,
        startup_cost: input.startup_cost + gucs.parallel_setup_cost,
        rows: input.rows,
        width: input.width,
    }
}

// ============================================================================
// Filter
// ============================================================================

/// Cost of an in-place row filter (WHERE clause evaluation).
///
/// - Total cost increases by `input.rows * cpu_operator_cost`.
/// - Output rows decrease by `sel`.
/// - Startup and width are unchanged.
pub fn cost_filter(input: CostEstimate, sel: f64, gucs: &CostGucs) -> CostEstimate {
    CostEstimate {
        total_cost: input.rows.mul_add(gucs.cpu_operator_cost, input.total_cost),
        startup_cost: input.startup_cost,
        rows: input.rows * sel,
        width: input.width,
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cost::NoStats;

    struct FixedStats {
        rows: u64,
        pages: u64,
    }
    impl crate::cost::StatsSource for FixedStats {
        fn row_count(&self, _: &str) -> u64 {
            self.rows
        }
        fn page_count(&self, _: &str) -> u64 {
            self.pages
        }
        fn null_frac(&self, _: &str, _: usize) -> f64 {
            0.0
        }
        fn n_distinct(&self, _: &str, _: usize) -> f64 {
            0.0
        }
    }

    fn gucs() -> CostGucs {
        CostGucs::default()
    }

    /// Sequential scan cost is 0 for `NoStats` (rows = 0, pages = 0).
    #[test]
    fn scan_no_stats_is_zero() {
        let est = cost_scan(&NoStats, "t", &gucs());
        assert!((est.total_cost - 0.0).abs() < 1e-9);
        assert!((est.startup_cost - 0.0).abs() < 1e-9);
        assert!((est.rows - 0.0).abs() < 1e-9);
    }

    /// Sequential scan cost grows with page and row count.
    #[test]
    fn scan_grows_with_table_size() {
        let small = FixedStats {
            rows: 100,
            pages: 1,
        };
        let large = FixedStats {
            rows: 1_000_000,
            pages: 10_000,
        };
        let g = gucs();
        assert!(cost_scan(&large, "t", &g).total_cost > cost_scan(&small, "t", &g).total_cost);
    }

    /// Hash join startup equals the build cost (right side).
    #[test]
    fn hash_join_startup_equals_build_cost() {
        let left = CostEstimate {
            total_cost: 10.0,
            startup_cost: 0.0,
            rows: 1000.0,
            width: 8,
        };
        let right = CostEstimate {
            total_cost: 5.0,
            startup_cost: 0.0,
            rows: 100.0,
            width: 8,
        };
        let g = gucs();
        let est = cost_hash_join(left, right, 0.01, &g);
        let expected_build = (right.rows * g.cpu_operator_cost).mul_add(2.0, right.total_cost);
        assert!(
            (est.startup_cost - expected_build).abs() < 1e-9,
            "startup={} expected={expected_build}",
            est.startup_cost,
        );
    }

    /// Sort startup equals total (pipeline breaker).
    #[test]
    fn sort_startup_equals_total() {
        let input = CostEstimate {
            total_cost: 50.0,
            startup_cost: 0.0,
            rows: 500.0,
            width: 16,
        };
        let est = cost_sort(input, &gucs());
        assert!(
            (est.startup_cost - est.total_cost).abs() < 1e-9,
            "sort startup should equal total"
        );
    }

    /// Filter reduces output row count by selectivity.
    #[test]
    fn filter_reduces_rows_by_selectivity() {
        let input = CostEstimate {
            total_cost: 20.0,
            startup_cost: 0.0,
            rows: 1000.0,
            width: 8,
        };
        let est = cost_filter(input, 0.1, &gucs());
        assert!((est.rows - 100.0).abs() < 1e-9, "rows={}", est.rows);
    }

    /// Index scan startup is non-zero (root-to-leaf traversal cost).
    #[test]
    fn index_scan_has_nonzero_startup_for_nonzero_height() {
        let stats = FixedStats {
            rows: 10_000,
            pages: 100,
        };
        let g = gucs();
        let est = cost_index_scan(&stats, "t", 3, 0.05, &g);
        assert!(est.startup_cost > 0.0, "startup should be > 0");
        assert!(est.total_cost >= est.startup_cost);
    }

    /// Nested-loop join is cheaper than hash join for a 1-row build side.
    ///
    /// For very small right sides (1 row) the NLJ avoids the hash build
    /// cost entirely and beats hash join on total cost.
    #[test]
    fn nlj_cheaper_than_hash_join_for_tiny_build_side() {
        let left = CostEstimate {
            total_cost: 5.0,
            startup_cost: 0.0,
            rows: 10.0,
            width: 8,
        };
        let right = CostEstimate {
            total_cost: 0.0,
            startup_cost: 0.0,
            rows: 1.0,
            width: 8,
        };
        let g = gucs();
        let nlj = cost_nested_loop(left, right, 1.0, &g);
        let hj = cost_hash_join(left, right, 1.0, &g);
        assert!(
            nlj.total_cost < hj.total_cost,
            "NLJ={} should be cheaper than HJ={} for 1-row build side",
            nlj.total_cost,
            hj.total_cost,
        );
    }

    /// `BitmapHeapScan` has non-zero startup and uses `seq_page_cost` for heap.
    #[test]
    fn bitmap_heap_scan_has_nonzero_startup_and_scales() {
        let stats = FixedStats { rows: 100_000, pages: 1_000 };
        let g = gucs();
        let est = cost_bitmap_heap_scan(&stats, "t", 3, 0.05, &g);
        assert!(est.startup_cost > 0.0, "startup should be > 0");
        assert!(est.total_cost >= est.startup_cost);
        assert!((est.rows - 5_000.0).abs() < 1e-6, "rows={}", est.rows);
    }

    /// `IndexOnlyScan` is cheaper than `IndexScan` for the same parameters
    /// because it skips heap I/O.
    #[test]
    fn index_only_scan_cheaper_than_index_scan() {
        let stats = FixedStats { rows: 100_000, pages: 1_000 };
        let g = gucs();
        let ios = cost_index_only_scan(&stats, "t", 3, 0.01, &g);
        let idx = cost_index_scan(&stats, "t", 3, 0.01, &g);
        assert!(
            ios.total_cost < idx.total_cost,
            "IndexOnlyScan={} should be < IndexScan={}",
            ios.total_cost,
            idx.total_cost,
        );
    }

    /// `annotate_parallel` divides total cost by `workers` and adds setup.
    #[test]
    fn annotate_parallel_divides_cost_and_adds_setup() {
        let input = CostEstimate {
            total_cost: 1000.0,
            startup_cost: 10.0,
            rows: 500.0,
            width: 8,
        };
        let g = gucs();
        let out = annotate_parallel(input, 4, &g);
        assert!((out.total_cost - 250.0).abs() < 1e-9, "total={}", out.total_cost);
        assert!(out.startup_cost > input.startup_cost, "startup should grow");
    }

    /// `annotate_parallel` with `workers=1` returns unchanged estimate.
    #[test]
    fn annotate_parallel_noop_for_single_worker() {
        let input = CostEstimate {
            total_cost: 500.0,
            startup_cost: 5.0,
            rows: 100.0,
            width: 8,
        };
        let g = gucs();
        let out = annotate_parallel(input, 1, &g);
        assert!((out.total_cost - input.total_cost).abs() < 1e-9);
        assert!((out.startup_cost - input.startup_cost).abs() < 1e-9);
    }
}
