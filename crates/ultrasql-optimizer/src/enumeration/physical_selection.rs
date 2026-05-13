//! Physical operator selection.
//!
//! For each logical plan operator, this module selects the optimal physical
//! implementation based on cost estimates and structural properties of the
//! input plans (e.g. whether inputs are already sorted).
//!
//! ## Heuristics (v0.6)
//!
//! ### Joins
//! - `HashJoin` when an equality predicate exists and the build side fits in
//!   `work_mem` (heuristic: `rows * width <= 256 MiB`).
//! - `MergeJoin` when both inputs are already sorted on the join key.
//! - `NestLoop` otherwise.
//!
//! ### Aggregates
//! - `HashAggregate` unless the input is already sorted on the group keys,
//!   in which case `SortAggregate`.
//!
//! ### Scans
//! - `IndexScan` when an equality or range predicate matches an available
//!   index **and** `selectivity * row_count <= 0.05 * row_count`.
//! - `SeqScan` otherwise.
//! - `BitmapHeapScan` is deferred to v0.7.

use ultrasql_planner::{BinaryOp, LogicalJoinCondition, ScalarExpr, SortKey};

use crate::cost::operators::{cost_hash_join, cost_merge_join, cost_nested_loop};
use crate::cost::{CostEstimate, CostGucs, StatsSource};

// Selectivity window for which BitmapHeapScan is preferred over IndexScan.
// Lower bound: 0.5% (below this, IndexScan's random I/O is fine).
// Upper bound: 10% (above this, SeqScan wins).
const BITMAP_SEL_LO: f64 = 0.005;
const BITMAP_SEL_HI: f64 = 0.10;
use crate::enumeration::PhysicalOp;

// ============================================================================
// IndexHint
// ============================================================================

/// Describes an index available on a table for scan selection.
///
/// Passed to [`select_scan_physical`] so it can reason about index
/// applicability without querying the catalog directly.
#[derive(Clone, Debug)]
pub struct IndexHint {
    /// Index name (for diagnostics; not used in selection logic).
    pub name: String,
    /// 0-based column indices covered by the index, in key order.
    pub columns: Vec<usize>,
    /// Whether the index enforces a UNIQUE constraint.
    pub unique: bool,
    /// Index access method: `"btree"` or `"hash"`.
    pub method: &'static str,
    /// Whether all pages of this index's heap are marked all-visible in
    /// the visibility map.  When `true`, `IndexOnlyScan` is eligible.
    pub all_visible: bool,
}

// ============================================================================
// Work-mem constant (heuristic)
// ============================================================================

/// Heuristic `work_mem` threshold in bytes (256 MiB). When the build side of a
/// hash join is estimated to fit within this budget, `HashJoin` is preferred.
const WORK_MEM_BYTES: f64 = 256.0 * 1024.0 * 1024.0;

// ============================================================================
// select_join_physical
// ============================================================================

/// Select the physical join operator for a logical join.
///
/// Returns the cheapest physical operator given the child cost estimates,
/// the join condition, and whether a parent operator requires sorted output.
///
/// ## Heuristics
///
/// 1. If the join condition contains an equality predicate and the smaller
///    side fits in `work_mem`, choose `HashJoin`.
/// 2. If both inputs are already sorted on the join key (indicated by
///    non-zero startup cost matching total cost), choose `MergeJoin`.
/// 3. Otherwise choose `NestLoop`.
pub fn select_join_physical(
    left: CostEstimate,
    right: CostEstimate,
    condition: &LogicalJoinCondition,
    gucs: &CostGucs,
    _ordering_required: Option<&[SortKey]>,
) -> PhysicalOp {
    let has_equality = condition_has_equality(condition);
    // Use the smaller side as build side for hash join.
    let build_rows = right.rows.min(left.rows);
    let build_width = if right.rows <= left.rows {
        right.width
    } else {
        left.width
    };
    let build_bytes = build_rows * f64::from(build_width);

    if has_equality && build_bytes <= WORK_MEM_BYTES {
        let hj = cost_hash_join(left, right, 0.01, gucs);
        let nlj = cost_nested_loop(left, right, 0.01, gucs);
        if hj.total_cost <= nlj.total_cost {
            return PhysicalOp::HashJoin;
        }
    }

    // MergeJoin: prefer when both sides look like they are already sorted
    // (startup_cost == total_cost is a proxy for "pipeline-breaker already
    //  paid", meaning a sort was already costed in).
    let left_sorted = (left.startup_cost - left.total_cost).abs() < 1e-9 && left.rows > 0.0;
    let right_sorted = (right.startup_cost - right.total_cost).abs() < 1e-9 && right.rows > 0.0;
    if has_equality && left_sorted && right_sorted {
        let mj = cost_merge_join(left, right, 0.01, gucs);
        let nlj = cost_nested_loop(left, right, 0.01, gucs);
        if mj.total_cost <= nlj.total_cost {
            return PhysicalOp::MergeJoin;
        }
    }

    PhysicalOp::NestLoop
}

// ============================================================================
// select_agg_physical
// ============================================================================

/// Select the physical aggregate operator.
///
/// Chooses `SortAggregate` when the input is already sorted on the group keys
/// (heuristic: `startup_cost == total_cost`), otherwise `HashAggregate`.
pub fn select_agg_physical(
    input: CostEstimate,
    group_by: &[ScalarExpr],
    _ordering_required: Option<&[SortKey]>,
    _agg_gucs: &CostGucs, // reserved for future cost-driven choice
) -> PhysicalOp {
    if group_by.is_empty() {
        // No group keys: simple aggregate, hash is always fine.
        return PhysicalOp::HashAggregate;
    }
    // If the input arrives pre-sorted (startup_cost == total_cost), use
    // the streaming SortAggregate to avoid the hash table overhead.
    let pre_sorted = (input.startup_cost - input.total_cost).abs() < 1e-9 && input.rows > 0.0;
    if pre_sorted {
        PhysicalOp::SortAggregate
    } else {
        PhysicalOp::HashAggregate
    }
}

// ============================================================================
// select_scan_physical
// ============================================================================

/// Select the physical scan operator.
///
/// The selection logic, in priority order:
///
/// 1. **`IndexOnlyScan`** — when a matching index exists with `all_visible =
///    true` (all heap pages are all-visible in the VM) and selectivity ≤ 5%.
///    No heap fetch is required.
/// 2. **`BitmapHeapScan`** — when ≥ 2 applicable indexes exist, OR when a
///    single index matches and selectivity ∈ [`BITMAP_SEL_LO`, `BITMAP_SEL_HI`]
///    (0.5%–10%).  Bitmap access reads heap pages in physical order, beating
///    random-I/O index scans for medium selectivities.
/// 3. **`IndexScan`** — when a single index matches and selectivity ≤ 5%.
/// 4. **`SeqScan`** — fallback when no index applies or selectivity is too
///    high for index access.
pub fn select_scan_physical(
    table: &str,
    predicates: &[ScalarExpr],
    available_indexes: &[IndexHint],
    stats: &dyn StatsSource,
    _gucs: &CostGucs,
) -> PhysicalOp {
    let total_rows = stats.row_count(table) as f64;
    if total_rows == 0.0 {
        return PhysicalOp::SeqScan;
    }

    // Collect matching (index, selectivity) pairs.
    let mut matches: Vec<(&IndexHint, f64)> = Vec::new();
    for hint in available_indexes {
        if hint.columns.is_empty() {
            continue;
        }
        let leading_col = hint.columns[0];
        for pred in predicates {
            if predicate_references_column(pred, leading_col) {
                let sel =
                    crate::cost::selectivity::selectivity(pred, stats, table, total_rows as u64);
                matches.push((hint, sel));
                break; // one predicate per index is enough
            }
        }
    }

    // ── 1. IndexOnlyScan ────────────────────────────────────────────────────
    for &(hint, sel) in &matches {
        if hint.all_visible && sel <= 0.05 {
            return PhysicalOp::IndexOnlyScan;
        }
    }

    // ── 2. BitmapHeapScan ───────────────────────────────────────────────────
    // Use BitmapHeapScan when ≥2 indexes match, or when selectivity is in the
    // medium range where sequential-order heap access beats random I/O.
    if matches.len() >= 2 {
        return PhysicalOp::BitmapHeapScan;
    }
    if let Some(&(_, sel)) = matches.first() {
        if (BITMAP_SEL_LO..=BITMAP_SEL_HI).contains(&sel) {
            return PhysicalOp::BitmapHeapScan;
        }
    }

    // ── 3. IndexScan ────────────────────────────────────────────────────────
    if let Some(&(_, sel)) = matches.first() {
        if sel <= 0.05 {
            return PhysicalOp::IndexScan;
        }
    }

    // ── 4. SeqScan (fallback) ───────────────────────────────────────────────
    PhysicalOp::SeqScan
}

// ============================================================================
// Helpers
// ============================================================================

/// Return `true` if the join condition contains an equality (`=`) predicate.
fn condition_has_equality(condition: &LogicalJoinCondition) -> bool {
    match condition {
        LogicalJoinCondition::On(expr) => expr_has_equality(expr),
        LogicalJoinCondition::Using(_) => true,
        LogicalJoinCondition::None => false,
    }
}

/// Return `true` if `expr` contains or is an equality binary operator.
fn expr_has_equality(expr: &ScalarExpr) -> bool {
    match expr {
        ScalarExpr::Binary {
            op: BinaryOp::Eq, ..
        } => true,
        ScalarExpr::Binary {
            op: BinaryOp::And,
            left,
            right,
            ..
        } => expr_has_equality(left) || expr_has_equality(right),
        ScalarExpr::Binary {
            op: BinaryOp::Or,
            left,
            right,
            ..
        } => expr_has_equality(left) && expr_has_equality(right),
        _ => false,
    }
}

/// Return `true` if `pred` directly references `column_idx`.
fn predicate_references_column(pred: &ScalarExpr, column_idx: usize) -> bool {
    match pred {
        ScalarExpr::Column { index, .. } => *index == column_idx,
        ScalarExpr::Binary { left, right, .. } => {
            predicate_references_column(left, column_idx)
                || predicate_references_column(right, column_idx)
        }
        ScalarExpr::Unary { expr, .. } | ScalarExpr::IsNull { expr, .. } => {
            predicate_references_column(expr, column_idx)
        }
        _ => false,
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use ultrasql_core::{DataType, Value};
    use ultrasql_planner::{BinaryOp, ScalarExpr};

    use super::*;
    use crate::cost::{CostGucs, NoStats};

    fn col(idx: usize) -> ScalarExpr {
        ScalarExpr::Column {
            name: format!("c{idx}"),
            index: idx,
            data_type: DataType::Int32,
        }
    }

    fn lit_int(v: i32) -> ScalarExpr {
        ScalarExpr::Literal {
            value: Value::Int32(v),
            data_type: DataType::Int32,
        }
    }

    fn eq_cond(l: ScalarExpr, r: ScalarExpr) -> LogicalJoinCondition {
        LogicalJoinCondition::On(ScalarExpr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(l),
            right: Box::new(r),
            data_type: DataType::Bool,
        })
    }

    fn est(rows: f64, width: u32, total: f64, startup: f64) -> CostEstimate {
        CostEstimate {
            total_cost: total,
            startup_cost: startup,
            rows,
            width,
        }
    }

    /// `HashJoin` is chosen for an equi-join where both sides fit in `work_mem`.
    #[test]
    fn physical_selection_picks_hash_join_for_equi_join_with_small_build_side() {
        let left = est(1000.0, 16, 50.0, 0.0);
        let right = est(100.0, 8, 5.0, 0.0);
        let cond = eq_cond(col(0), col(1));
        let gucs = CostGucs::default();
        let op = select_join_physical(left, right, &cond, &gucs, None);
        assert_eq!(op, PhysicalOp::HashJoin, "expected HashJoin, got {op:?}");
    }

    /// `NestLoop` is chosen when there is no equality predicate.
    #[test]
    fn physical_selection_picks_nestloop_without_equality() {
        let left = est(1000.0, 16, 50.0, 0.0);
        let right = est(100.0, 8, 5.0, 0.0);
        let cond = LogicalJoinCondition::On(ScalarExpr::Binary {
            op: BinaryOp::Lt,
            left: Box::new(col(0)),
            right: Box::new(col(1)),
            data_type: DataType::Bool,
        });
        let gucs = CostGucs::default();
        let op = select_join_physical(left, right, &cond, &gucs, None);
        assert_eq!(op, PhysicalOp::NestLoop, "expected NestLoop, got {op:?}");
    }

    /// `HashAggregate` is chosen when the input is not pre-sorted.
    #[test]
    fn physical_selection_picks_hash_agg_for_unsorted_input() {
        let input = est(1000.0, 16, 50.0, 0.0); // startup < total => not pre-sorted
        let group_by = vec![col(0)];
        let gucs = CostGucs::default();
        let op = select_agg_physical(input, &group_by, None, &gucs);
        assert_eq!(op, PhysicalOp::HashAggregate, "got {op:?}");
    }

    /// `SeqScan` is returned when no indexes are available.
    #[test]
    fn physical_selection_seq_scan_when_no_indexes() {
        let preds = vec![ScalarExpr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(col(0)),
            right: Box::new(lit_int(42)),
            data_type: DataType::Bool,
        }];
        let gucs = CostGucs::default();
        let op = select_scan_physical("t", &preds, &[], &NoStats, &gucs);
        assert_eq!(op, PhysicalOp::SeqScan, "got {op:?}");
    }

    /// `IndexScan` is preferred when a matching index exists and selectivity is low.
    #[test]
    fn physical_selection_index_scan_when_selective_and_index_available() {
        struct HighRowStats;
        impl StatsSource for HighRowStats {
            fn row_count(&self, _: &str) -> u64 {
                100_000
            }
            fn page_count(&self, _: &str) -> u64 {
                1_000
            }
            fn null_frac(&self, _: &str, _: usize) -> f64 {
                0.0
            }
            fn n_distinct(&self, _: &str, _: usize) -> f64 {
                10_000.0
            }
        }
        let preds = vec![ScalarExpr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(col(0)),
            right: Box::new(lit_int(42)),
            data_type: DataType::Bool,
        }];
        let idx = IndexHint {
            name: "idx_c0".into(),
            columns: vec![0],
            unique: false,
            method: "btree",
            all_visible: false,
        };
        let gucs = CostGucs::default();
        let op = select_scan_physical("t", &preds, &[idx], &HighRowStats, &gucs);
        assert_eq!(op, PhysicalOp::IndexScan, "got {op:?}");
    }

    /// USING join condition counts as an equality join.
    #[test]
    fn physical_selection_using_condition_treated_as_equality() {
        let left = est(500.0, 8, 25.0, 0.0);
        let right = est(200.0, 8, 10.0, 0.0);
        let cond = LogicalJoinCondition::Using(vec![(0, 0)]);
        let gucs = CostGucs::default();
        let op = select_join_physical(left, right, &cond, &gucs, None);
        assert_eq!(op, PhysicalOp::HashJoin, "USING should select HashJoin");
    }

    /// Stats that return a fixed row/page/n_distinct count.
    struct RichStats {
        rows: u64,
        pages: u64,
        n_distinct: f64,
    }
    impl StatsSource for RichStats {
        fn row_count(&self, _: &str) -> u64 { self.rows }
        fn page_count(&self, _: &str) -> u64 { self.pages }
        fn null_frac(&self, _: &str, _: usize) -> f64 { 0.0 }
        fn n_distinct(&self, _: &str, _: usize) -> f64 { self.n_distinct }
    }

    /// `BitmapHeapScan` is chosen when ≥ 2 applicable indexes match.
    #[test]
    fn bitmap_heap_scan_selected_for_two_matching_indexes() {
        let stats = RichStats { rows: 100_000, pages: 1_000, n_distinct: 100_000.0 };
        let preds = vec![
            ScalarExpr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(col(0)),
                right: Box::new(lit_int(1)),
                data_type: DataType::Bool,
            },
            ScalarExpr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(col(1)),
                right: Box::new(lit_int(2)),
                data_type: DataType::Bool,
            },
        ];
        let idx0 = IndexHint {
            name: "idx_c0".into(), columns: vec![0], unique: false,
            method: "btree", all_visible: false,
        };
        let idx1 = IndexHint {
            name: "idx_c1".into(), columns: vec![1], unique: false,
            method: "btree", all_visible: false,
        };
        let gucs = CostGucs::default();
        let op = select_scan_physical("t", &preds, &[idx0, idx1], &stats, &gucs);
        assert_eq!(op, PhysicalOp::BitmapHeapScan, "got {op:?}");
    }

    /// `BitmapHeapScan` is chosen for selectivity in the [0.5%, 10%] window.
    #[test]
    fn bitmap_heap_scan_selected_for_medium_selectivity() {
        // n_distinct = 50 → sel = 1/50 = 2% → in [0.5%, 10%]
        let stats = RichStats { rows: 100_000, pages: 1_000, n_distinct: 50.0 };
        let preds = vec![ScalarExpr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(col(0)),
            right: Box::new(lit_int(42)),
            data_type: DataType::Bool,
        }];
        let idx = IndexHint {
            name: "idx_c0".into(), columns: vec![0], unique: false,
            method: "btree", all_visible: false,
        };
        let gucs = CostGucs::default();
        let op = select_scan_physical("t", &preds, &[idx], &stats, &gucs);
        assert_eq!(op, PhysicalOp::BitmapHeapScan, "got {op:?}");
    }

    /// `IndexOnlyScan` is chosen when `all_visible = true` and selectivity ≤ 5%.
    #[test]
    fn index_only_scan_selected_when_all_visible() {
        // n_distinct = 10_000 → sel = 1/10_000 = 0.01% → ≤ 5%
        let stats = RichStats { rows: 100_000, pages: 1_000, n_distinct: 10_000.0 };
        let preds = vec![ScalarExpr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(col(0)),
            right: Box::new(lit_int(7)),
            data_type: DataType::Bool,
        }];
        let idx = IndexHint {
            name: "idx_c0".into(), columns: vec![0], unique: true,
            method: "btree", all_visible: true,
        };
        let gucs = CostGucs::default();
        let op = select_scan_physical("t", &preds, &[idx], &stats, &gucs);
        assert_eq!(op, PhysicalOp::IndexOnlyScan, "got {op:?}");
    }
}
