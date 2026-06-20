//! Cost model for UltraSQL's query optimizer.
//!
//! This module provides:
//! - [`StatsSource`]: a minimal trait over table statistics. Server callers
//!   adapt catalog-backed `ANALYZE` data to this trait, while [`NoStats`]
//!   keeps the cost model available when no statistics exist yet.
//! - [`CostEstimate`]: the output of the cost model for a single plan node.
//! - [`CostGucs`]: cost GUCs (grand unified constants).
//! - [`CostModel`]: the entry point; call [`CostModel::estimate`] on any
//!   [`LogicalPlan`] to obtain a [`CostEstimate`].
//!
//! Sub-modules:
//! - [`selectivity`]: predicate selectivity heuristics.
//! - [`operators`]: per-operator cost formulas (used internally by
//!   [`CostModel::estimate`]; exposed `pub(crate)` for tests and physical
//!   selection).

pub mod operators;
pub mod selectivity;

use num_traits::ToPrimitive;
use ultrasql_planner::LogicalPlan;

pub use crate::cost::operators::{annotate_parallel, cost_bitmap_heap_scan, cost_index_only_scan};
use crate::cost::operators::{
    cost_aggregate, cost_filter, cost_hash_join, cost_nested_loop, cost_scan, cost_sort,
};

// ============================================================================
// StatsSource trait
// ============================================================================

/// Minimal statistics surface the cost model depends on.
///
/// Catalog-backed statistics implement this surface through an adapter. The
/// indirection keeps `cost/` independent of the concrete statistics storage.
///
/// All methods accept a `table` name (case-folded) and, where relevant, a
/// 0-based `column` index.
pub trait StatsSource: Send + Sync {
    /// Estimated number of live rows in `table`. Returns `0` when unknown.
    fn row_count(&self, table: &str) -> u64;

    /// Estimated number of 8 KiB pages that hold `table`'s heap. Returns
    /// `0` when unknown.
    fn page_count(&self, table: &str) -> u64;

    /// Fraction of NULLs in `column` of `table`. Returns `0.0` when unknown.
    /// Guaranteed to be in `[0.0, 1.0]`.
    fn null_frac(&self, table: &str, column: usize) -> f64;

    /// Estimated number of distinct values in `column` of `table`.
    /// `0.0` means no statistics are available. Positive values are counts;
    /// negative values are fractions of `row_count` (PostgreSQL convention).
    fn n_distinct(&self, table: &str, column: usize) -> f64;
}

// ============================================================================
// NoStats
// ============================================================================

/// A [`StatsSource`] that returns all-zero / empty statistics.
///
/// Used as the default when no `ANALYZE` data is available. The cost model
/// degrades gracefully: scans cost zero, selectivity falls back to PG
/// defaults, and join enumeration still runs correctly.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoStats;

impl StatsSource for NoStats {
    fn row_count(&self, _table: &str) -> u64 {
        0
    }

    fn page_count(&self, _table: &str) -> u64 {
        0
    }

    fn null_frac(&self, _table: &str, _column: usize) -> f64 {
        0.0
    }

    fn n_distinct(&self, _table: &str, _column: usize) -> f64 {
        0.0
    }
}

// ============================================================================
// CostEstimate
// ============================================================================

/// The output of the cost model for a single plan node.
///
/// Costs are expressed in arbitrary cost units (ACU); lower is better.
/// The units follow the PostgreSQL convention: a sequential page read is
/// 1.0 ACU, a CPU operation is 0.01 ACU, etc.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CostEstimate {
    /// Estimated wall-clock cost in arbitrary cost units. Lower is better.
    pub total_cost: f64,
    /// Cost up to first-row delivery (relevant for LIMIT optimizations).
    ///
    /// For pipeline-breakers such as sorts and hash-build phases, this equals
    /// `total_cost` because no rows are delivered until the breaker finishes.
    pub startup_cost: f64,
    /// Estimated output row count.
    pub rows: f64,
    /// Estimated output width in bytes (approximate).
    pub width: u32,
}

// ============================================================================
// CostGucs
// ============================================================================

/// Cost GUCs (Grand Unified Constants).
///
/// These constants translate hardware-relative operation costs into the
/// shared cost unit. The defaults match PostgreSQL 17's defaults exactly so
/// that plans from UltraSQL and PG can be compared on equal footing.
#[derive(Clone, Copy, Debug)]
pub struct CostGucs {
    /// Cost of reading one sequential heap page. Default: 1.0.
    pub seq_page_cost: f64,
    /// Cost of reading one random (index-driven) page. Default: 4.0.
    pub random_page_cost: f64,
    /// CPU cost of processing one output tuple. Default: 0.01.
    pub cpu_tuple_cost: f64,
    /// CPU cost of processing one index tuple (index probe). Default: 0.005.
    pub cpu_index_tuple_cost: f64,
    /// CPU cost of a simple operator (comparison, hash, etc.). Default: 0.0025.
    pub cpu_operator_cost: f64,
    /// Fixed overhead added to `startup_cost` when a parallel query is
    /// annotated with `workers > 1`. Models worker spawn + synchronisation
    /// costs.  Default: 1000.0 (PostgreSQL's `parallel_setup_cost`).
    pub parallel_setup_cost: f64,
}

impl Default for CostGucs {
    fn default() -> Self {
        Self {
            seq_page_cost: 1.0,
            random_page_cost: 4.0,
            cpu_tuple_cost: 0.01,
            cpu_index_tuple_cost: 0.005,
            cpu_operator_cost: 0.0025,
            parallel_setup_cost: 1000.0,
        }
    }
}

// ============================================================================
// CostModel
// ============================================================================

/// Entry point for plan costing.
///
/// Construct a `CostModel` with a [`StatsSource`] (and optionally custom
/// [`CostGucs`]), then call [`estimate`] on any [`LogicalPlan`]. The method
/// is recursive: it walks the plan tree bottom-up, combining child estimates
/// with the operator-specific formula.
///
/// ## Usage
///
/// ```rust
/// use ultrasql_optimizer::cost::{CostModel, NoStats};
/// use ultrasql_planner::LogicalPlan;
/// use ultrasql_core::Schema;
///
/// let stats = NoStats;
/// let model = CostModel::new(&stats);
/// let plan = LogicalPlan::Empty { schema: Schema::empty() };
/// let est = model.estimate(&plan);
/// assert_eq!(est.rows, 0.0);
/// ```
///
/// [`estimate`]: CostModel::estimate
pub struct CostModel<'s> {
    /// The statistics source used for cardinality and selectivity estimates.
    pub stats: &'s dyn StatsSource,
    /// The cost GUCs controlling relative hardware costs.
    pub gucs: CostGucs,
}

impl std::fmt::Debug for CostModel<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CostModel")
            .field("stats", &"<dyn StatsSource>")
            .field("gucs", &self.gucs)
            .finish()
    }
}

impl<'s> CostModel<'s> {
    /// Create a `CostModel` with PostgreSQL-default [`CostGucs`].
    #[must_use]
    pub fn new(stats: &'s dyn StatsSource) -> Self {
        Self {
            stats,
            gucs: CostGucs::default(),
        }
    }

    /// Recursively estimate the cost of `plan`.
    ///
    /// The estimate walks the plan tree bottom-up. For unsupported or
    /// terminal variants (e.g. `Empty`, `Values`, DML nodes) a neutral
    /// zero-cost estimate is returned so the caller can still compose plans.
    #[must_use]
    pub fn estimate(&self, plan: &LogicalPlan) -> CostEstimate {
        match plan {
            LogicalPlan::Scan { table, .. } => cost_scan(self.stats, table, &self.gucs),

            LogicalPlan::Filter { input, predicate } => {
                let input_est = self.estimate(input);
                let table = extract_base_table(input);
                let input_rows = if input_est.rows.is_nan() || input_est.rows <= 0.0 {
                    0
                } else {
                    input_est.rows.to_u64().unwrap_or(u64::MAX)
                };
                let sel = selectivity::selectivity(
                    predicate,
                    self.stats,
                    table.as_deref().unwrap_or(""),
                    input_rows,
                );
                cost_filter(input_est, sel, &self.gucs)
            }

            LogicalPlan::Project { input, .. } => {
                // Projection is free: it selects columns but does not add CPU cost.
                self.estimate(input)
            }

            LogicalPlan::Limit { input, n, .. } => {
                let mut est = self.estimate(input);
                est.rows = est.rows.min((*n).to_f64().unwrap_or(f64::MAX));
                est
            }

            LogicalPlan::Sort { input, .. } => {
                let input_est = self.estimate(input);
                cost_sort(input_est, &self.gucs)
            }

            LogicalPlan::Join {
                left,
                right,
                condition,
                ..
            } => {
                let left_est = self.estimate(left);
                let right_est = self.estimate(right);
                // Derive a simple selectivity for the join condition.
                let join_sel = join_selectivity(condition);
                // Default to hash join for cost estimation; physical
                // selection will choose the actual operator later.
                cost_hash_join(left_est, right_est, join_sel, &self.gucs)
            }

            LogicalPlan::Aggregate {
                input, group_by, ..
            } => {
                let input_est = self.estimate(input);
                // Estimated distinct groups: square-root heuristic when no
                // statistics are available.
                let n_groups = if group_by.is_empty() {
                    1.0_f64
                } else {
                    input_est.rows.max(1.0).sqrt()
                };
                cost_aggregate(input_est, n_groups, &self.gucs)
            }

            LogicalPlan::Pivot { input, .. } => {
                let input_est = self.estimate(input);
                let n_groups = input_est.rows.max(1.0).sqrt();
                cost_aggregate(input_est, n_groups, &self.gucs)
            }

            LogicalPlan::Unpivot { input, columns, .. } => {
                let mut est = self.estimate(input);
                est.rows *= columns.len().to_f64().unwrap_or(1.0);
                est
            }

            LogicalPlan::SetOp { left, right, .. } => {
                // Model as nested-loop-style: left cost + right cost + output rows.
                let left_est = self.estimate(left);
                let right_est = self.estimate(right);
                cost_nested_loop(left_est, right_est, 1.0, &self.gucs)
            }

            LogicalPlan::Cte { body, .. } => self.estimate(body),
            LogicalPlan::LockRows { input, .. } => self.estimate(input),
            LogicalPlan::Window { input, .. } => {
                // Window is one full pass over the child + a partition
                // sort. Approximate as the child's cost plus a sort cost.
                let input_est = self.estimate(input);
                cost_sort(input_est, &self.gucs)
            }

            // DML / DDL / source / transaction-control nodes: neutral
            // estimate (rows and cost = 0).
            LogicalPlan::Empty { .. }
            | LogicalPlan::Values { .. }
            | LogicalPlan::Insert { .. }
            | LogicalPlan::Update { .. }
            | LogicalPlan::Delete { .. }
            | LogicalPlan::Merge { .. }
            | LogicalPlan::Truncate { .. }
            | LogicalPlan::CreateTable { .. }
            | LogicalPlan::CreateMaterializedView { .. }
            | LogicalPlan::CreateView { .. }
            | LogicalPlan::CreateTypeEnum { .. }
            | LogicalPlan::CreateTypeComposite { .. }
            | LogicalPlan::CreateDomain { .. }
            | LogicalPlan::CreateOperator { .. }
            | LogicalPlan::CreateIndex { .. }
            | LogicalPlan::CreatePolicy { .. }
            | LogicalPlan::CreateRole { .. }
            | LogicalPlan::AlterRole { .. }
            | LogicalPlan::DropRole { .. }
            | LogicalPlan::GrantPrivileges { .. }
            | LogicalPlan::RevokePrivileges { .. }
            | LogicalPlan::AlterDefaultPrivileges { .. }
            | LogicalPlan::GrantRole { .. }
            | LogicalPlan::RevokeRole { .. }
            | LogicalPlan::CreateSchema { .. }
            | LogicalPlan::DropSchema { .. }
            | LogicalPlan::DropIndex { .. }
            | LogicalPlan::DropTable { .. }
            | LogicalPlan::AlterTable { .. }
            | LogicalPlan::AlterView { .. }
            | LogicalPlan::CreateSequence { .. }
            | LogicalPlan::AlterSequence { .. }
            | LogicalPlan::DropSequence { .. }
            | LogicalPlan::Comment { .. }
            | LogicalPlan::Begin { .. }
            | LogicalPlan::Commit { .. }
            | LogicalPlan::Rollback { .. }
            | LogicalPlan::Savepoint { .. }
            | LogicalPlan::RollbackToSavepoint { .. }
            | LogicalPlan::ReleaseSavepoint { .. }
            | LogicalPlan::PrepareTransaction { .. }
            | LogicalPlan::CommitPrepared { .. }
            | LogicalPlan::RollbackPrepared { .. }
            | LogicalPlan::SetTransaction { .. }
            | LogicalPlan::SetVariable { .. }
            | LogicalPlan::Describe { .. }
            | LogicalPlan::Summarize { .. }
            | LogicalPlan::Checkpoint { .. }
            | LogicalPlan::ExportDatabase { .. }
            | LogicalPlan::ImportDatabase { .. }
            | LogicalPlan::SetRole { .. }
            | LogicalPlan::Listen { .. }
            | LogicalPlan::Notify { .. }
            | LogicalPlan::Unlisten { .. }
            | LogicalPlan::Explain { .. }
            | LogicalPlan::Copy { .. }
            | LogicalPlan::FunctionScan { .. } => zero_estimate(),
        }
    }
}

// ============================================================================
// Helpers
// ============================================================================

/// Returns a zero-cost estimate for plans that produce no interesting rows.
const fn zero_estimate() -> CostEstimate {
    CostEstimate {
        total_cost: 0.0,
        startup_cost: 0.0,
        rows: 0.0,
        width: 0,
    }
}

/// Walk the plan tree to find the innermost `Scan` table name, if any.
/// Used to pass the table context to selectivity functions.
fn extract_base_table(plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::Scan { table, .. } => Some(table.clone()),
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Project { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Sort { input, .. } => extract_base_table(input),
        _ => None,
    }
}

/// Estimate the join output selectivity from a logical join condition.
///
/// For equi-joins we return 0.01 (PG heuristic). For unknown conditions
/// we return 0.333.
const fn join_selectivity(condition: &ultrasql_planner::LogicalJoinCondition) -> f64 {
    use ultrasql_planner::LogicalJoinCondition;
    match condition {
        LogicalJoinCondition::On(expr) => {
            use ultrasql_planner::BinaryOp;
            use ultrasql_planner::ScalarExpr;
            if let ScalarExpr::Binary {
                op: BinaryOp::Eq, ..
            } = expr
            {
                0.01
            } else {
                0.333
            }
        }
        LogicalJoinCondition::Using(_) => 0.01,
        LogicalJoinCondition::None => 1.0,
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    struct FakeStats {
        rows: u64,
        pages: u64,
        null_frac: f64,
        n_distinct: f64,
    }

    impl StatsSource for FakeStats {
        fn row_count(&self, _: &str) -> u64 {
            self.rows
        }
        fn page_count(&self, _: &str) -> u64 {
            self.pages
        }
        fn null_frac(&self, _: &str, _: usize) -> f64 {
            self.null_frac
        }
        fn n_distinct(&self, _: &str, _: usize) -> f64 {
            self.n_distinct
        }
    }

    /// With `NoStats`, `n_distinct` == 0.0, eq selectivity falls back to
    /// 1 / max(1, 1) = 1.0. With `n_distinct` == 100, eq selectivity = 0.01.
    #[test]
    fn selectivity_eq_uses_n_distinct() {
        use ultrasql_core::{DataType, Value};
        use ultrasql_planner::{BinaryOp, ScalarExpr};

        let col_expr = ScalarExpr::Column {
            name: "x".into(),
            index: 0,
            data_type: DataType::Int32,
        };
        let lit_expr = ScalarExpr::Literal {
            value: Value::Int32(5),
            data_type: DataType::Int32,
        };
        let eq_expr = ScalarExpr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(col_expr),
            right: Box::new(lit_expr),
            data_type: DataType::Bool,
        };

        let no_stats = NoStats;
        let sel_no_stats = selectivity::selectivity(&eq_expr, &no_stats, "t", 1000);
        assert!(
            (sel_no_stats - 1.0).abs() < 1e-9,
            "expected 1.0 with no stats, got {sel_no_stats}"
        );

        let fake = FakeStats {
            rows: 1000,
            pages: 10,
            null_frac: 0.0,
            n_distinct: 100.0,
        };
        let sel_fake = selectivity::selectivity(&eq_expr, &fake, "t", 1000);
        assert!(
            (sel_fake - 0.01).abs() < 1e-9,
            "expected 0.01 with n_distinct=100, got {sel_fake}"
        );
    }

    /// `SeqScan` cost scales linearly with row count and page count.
    #[test]
    fn cost_scan_scales_with_row_count() {
        let small = FakeStats {
            rows: 100,
            pages: 1,
            null_frac: 0.0,
            n_distinct: 10.0,
        };
        let large = FakeStats {
            rows: 10_000,
            pages: 100,
            null_frac: 0.0,
            n_distinct: 1000.0,
        };
        let gucs = CostGucs::default();
        let c_small = operators::cost_scan(&small, "t", &gucs);
        let c_large = operators::cost_scan(&large, "t", &gucs);
        assert!(c_large.total_cost > c_small.total_cost);
        assert!((c_small.rows - 100.0).abs() < 1e-9);
        assert!((c_large.rows - 10_000.0).abs() < 1e-9);
    }

    /// Hash join startup exceeds NLJ startup for small inputs.
    #[test]
    fn cost_hash_join_startup_higher_than_nested_loop_startup_for_small_inputs() {
        let left = CostEstimate {
            total_cost: 10.0,
            startup_cost: 0.0,
            rows: 100.0,
            width: 8,
        };
        let right = CostEstimate {
            total_cost: 5.0,
            startup_cost: 0.0,
            rows: 50.0,
            width: 8,
        };
        let gucs = CostGucs::default();
        let hj = operators::cost_hash_join(left, right, 0.01, &gucs);
        let nlj = operators::cost_nested_loop(left, right, 0.01, &gucs);
        assert!(
            hj.startup_cost > nlj.startup_cost,
            "hj={} nlj={}",
            hj.startup_cost,
            nlj.startup_cost,
        );
    }
}
