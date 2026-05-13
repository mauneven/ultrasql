//! UltraSQL cost-based optimizer.
//!
//! Two-phase optimizer: rule-based rewrites (predicate pushdown, constant
//! folding, projection pushdown, subquery decorrelation) followed by a
//! Cascades-style top-down search for join order and physical operator
//! selection. Statistics are sourced from the catalog.
//!
//! ## Module layout
//!
//! | Module | Contents |
//! |--------|----------|
//! | [`rules`] | [`rules::RewriteRule`] trait, [`rules::RuleSet`] registry, all built-in rules |
//! | `error` | [`OptimizeError`] error type |
//! | [`stats`] | Statistics context (stub; wave 6b) |
//! | [`cost`] | Cost model (stub; wave 6b) |
//! | [`enumeration`] | Join enumeration (stub; wave 6b) |
//!
//! ## Quick start
//!
//! ```rust
//! use ultrasql_optimizer::Optimizer;
//! use ultrasql_planner::LogicalPlan;
//! use ultrasql_core::Schema;
//!
//! let plan = LogicalPlan::Empty { schema: Schema::empty() };
//! let optimized = Optimizer::new().optimize(plan).expect("optimize ok");
//! ```

#![forbid(unsafe_op_in_unsafe_fn)]

pub mod cost;
pub mod enumeration;
mod error;
pub mod rules;
pub mod stats;

pub use error::OptimizeError;
pub use rules::{RewriteRule, RuleSet};
pub use stats::{
    AnalyzeOptions, AnalyzeRunner, ColumnStats, EquiDepthHistogram, InMemoryStatsCatalog,
    MostCommonValues, PgStatisticRow, RelationStats, StatsCatalog, StatsError,
};

use tracing::debug;
use ultrasql_planner::LogicalPlan;

// ============================================================================
// Optimizer
// ============================================================================

/// Fixed-point rule-based optimizer.
///
/// The optimizer applies each registered [`RewriteRule`] in registration
/// order, repeating the full pass until either no rule fires in a complete
/// pass (fixed point reached) or the iteration cap is exceeded.
///
/// ## Fixed-point semantics
///
/// A "pass" applies every rule once in order. If any rule in the pass
/// returned `Some(new_plan)`, the pass is marked "changed" and the driver
/// starts a new pass. If no rule fired, the plan has reached a fixed point
/// and the driver returns.
///
/// ## Usage
///
/// ```rust
/// use ultrasql_optimizer::Optimizer;
/// use ultrasql_planner::LogicalPlan;
/// use ultrasql_core::Schema;
///
/// let plan = LogicalPlan::Empty { schema: Schema::empty() };
/// let optimized = Optimizer::new().optimize(plan).unwrap();
/// ```
#[allow(missing_debug_implementations)]
pub struct Optimizer {
    rules: RuleSet,
    max_iterations: u32,
}

impl Optimizer {
    /// Create an optimizer with the default rule set and a 32-iteration cap.
    #[must_use]
    pub fn new() -> Self {
        Self {
            rules: RuleSet::default_rules(),
            max_iterations: 32,
        }
    }

    /// Create an optimizer with a custom rule set and a 32-iteration cap.
    #[must_use]
    pub const fn with_rules(rules: RuleSet) -> Self {
        Self {
            rules,
            max_iterations: 32,
        }
    }

    /// Override the maximum number of rewrite iterations.
    ///
    /// The driver stops and returns [`OptimizeError::DidNotConverge`] if the
    /// plan has not reached a fixed point after `n` full passes.
    #[must_use]
    pub const fn with_max_iterations(mut self, n: u32) -> Self {
        self.max_iterations = n;
        self
    }

    /// Drive the rule loop to a fixed point or until `max_iterations`.
    ///
    /// Each pass applies every registered rule in order. The loop terminates
    /// when no rule fired in a complete pass (fixed point) or when
    /// `max_iterations` is exceeded.
    ///
    /// # Errors
    ///
    /// - [`OptimizeError::DidNotConverge`] if the plan has not reached a fixed
    ///   point within the configured iteration cap.
    /// - [`OptimizeError::RuleFailed`] if any rule returns an error.
    pub fn optimize(&self, plan: LogicalPlan) -> Result<LogicalPlan, OptimizeError> {
        let mut current = plan;

        for iteration in 0..self.max_iterations {
            let mut changed = false;

            for rule in self.rules.rules() {
                if let Some(new_plan) = rule.apply(&current)? {
                    debug!(rule = rule.name(), iteration, "rule applied");
                    current = new_plan;
                    changed = true;
                }
            }

            if !changed {
                return Ok(current);
            }
        }

        Err(OptimizeError::DidNotConverge {
            max_iterations: self.max_iterations,
        })
    }
}

impl Default for Optimizer {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use ultrasql_core::{DataType, Field, Schema, Value};
    use ultrasql_planner::{BinaryOp, LogicalPlan, ScalarExpr};

    use super::*;
    use crate::rules::RewriteRule;

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn scan(table: &str) -> LogicalPlan {
        LogicalPlan::Scan {
            table: table.into(),
            schema: Schema::new([Field::required("id", DataType::Int32)]).expect("schema ok"),
            projection: None,
        }
    }

    fn lit_i32(v: i32) -> ScalarExpr {
        ScalarExpr::Literal {
            value: Value::Int32(v),
            data_type: DataType::Int32,
        }
    }

    fn lit_bool(b: bool) -> ScalarExpr {
        ScalarExpr::Literal {
            value: Value::Bool(b),
            data_type: DataType::Bool,
        }
    }

    fn col(name: &str, idx: usize) -> ScalarExpr {
        ScalarExpr::Column {
            name: name.to_owned(),
            index: idx,
            data_type: DataType::Int32,
        }
    }

    fn eq(l: ScalarExpr, r: ScalarExpr) -> ScalarExpr {
        ScalarExpr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(l),
            right: Box::new(r),
            data_type: DataType::Bool,
        }
    }

    // -----------------------------------------------------------------------
    // Fixed-point and convergence tests
    // -----------------------------------------------------------------------

    /// An empty rule set reaches a fixed point immediately (0 rules fire).
    #[test]
    fn optimizer_converges_on_fixed_point_with_no_applicable_rules() {
        let opt = Optimizer::with_rules(RuleSet::new());
        let plan = scan("t");
        let result = opt.optimize(plan.clone()).expect("should succeed");
        assert_eq!(result, plan, "plan should be unchanged");
    }

    /// A rule that always fires causes `DidNotConverge` at the iteration cap.
    #[test]
    fn optimizer_aborts_with_did_not_converge_when_rules_oscillate() {
        // A synthetic rule that always rewrites the plan (never converges).
        struct AlwaysFires;
        impl RewriteRule for AlwaysFires {
            fn name(&self) -> &'static str {
                "always_fires"
            }
            fn apply(&self, plan: &LogicalPlan) -> Result<Option<LogicalPlan>, OptimizeError> {
                // Wrap the plan in an extra Filter(true, ...) each time.
                Ok(Some(LogicalPlan::Filter {
                    input: Box::new(plan.clone()),
                    predicate: ScalarExpr::Literal {
                        value: Value::Bool(true),
                        data_type: DataType::Bool,
                    },
                }))
            }
        }

        let mut rs = RuleSet::new();
        rs.add(Box::new(AlwaysFires));
        let opt = Optimizer::with_rules(rs).with_max_iterations(3);
        let err = opt.optimize(scan("t")).unwrap_err();
        assert!(
            matches!(err, OptimizeError::DidNotConverge { max_iterations: 3 }),
            "expected DidNotConverge, got {err:?}"
        );
    }

    /// Constant folding fires before predicate pushdown in the default rule set.
    #[test]
    fn optimizer_applies_constant_fold_then_predicate_pushdown_in_sequence() {
        // Plan: Filter(Project(Scan), (1 = 1) AND col = 5)
        // After ConstantFold: Filter(Project(Scan), true AND col = 5) → col = 5
        // After PredicatePushdown: Project(Filter(Scan, col = 5))
        let proj_schema = Schema::new([Field::required("id", DataType::Int32)]).expect("schema ok");
        let project = LogicalPlan::Project {
            input: Box::new(scan("t")),
            exprs: vec![(col("id", 0), "id".into())],
            schema: proj_schema,
        };
        // Predicate: (1 = 1) AND (id = 5)
        let predicate = ScalarExpr::Binary {
            op: BinaryOp::And,
            left: Box::new(eq(lit_i32(1), lit_i32(1))),
            right: Box::new(eq(col("id", 0), lit_i32(5))),
            data_type: DataType::Bool,
        };
        let plan = LogicalPlan::Filter {
            input: Box::new(project),
            predicate,
        };

        let opt = Optimizer::new();
        let result = opt.optimize(plan).expect("optimize ok");

        // After optimization the top node should be a Project with a Filter
        // below it (predicate pushdown moved the Filter under the Project).
        assert!(
            matches!(&result, LogicalPlan::Project { .. }),
            "top node should be Project after pushdown; got: {result:?}"
        );
        if let LogicalPlan::Project { input, .. } = &result {
            assert!(
                matches!(input.as_ref(), LogicalPlan::Filter { .. }),
                "Project input should be Filter; got {input:?}"
            );
        }
    }

    /// Property test: any plan with only a Scan (no `ScalarExpr` literals) is a
    /// fixed point of constant folding.
    #[test]
    fn constant_fold_is_fixed_point_for_plans_without_literals() {
        // A Scan with no ScalarExpr at all — ConstantFold has nothing to do.
        let plan = scan("t");
        let cf = crate::rules::ConstantFold;
        assert!(
            cf.apply(&plan).expect("no error").is_none(),
            "ConstantFold must return None for a plain Scan"
        );
    }

    // proptest: Column-only filter predicates are never folded by ConstantFold.
    proptest! {
        #[test]
        fn proptest_column_filter_not_folded_by_constant_fold(
            idx in 0_usize..10,
            lit_val in 0_i32..1000,
        ) {
            let predicate = eq(col("x", idx), ScalarExpr::Column {
                name: "y".into(),
                index: idx + 1,
                data_type: DataType::Int32,
            });
            let schema = Schema::new([
                Field::required("x", DataType::Int32),
                Field::required("y", DataType::Int32),
            ]).expect("schema ok");
            let plan = LogicalPlan::Filter {
                input: Box::new(LogicalPlan::Scan {
                    table: "t".into(),
                    schema,
                    projection: None,
                }),
                predicate,
            };
            // lit_val is part of the strategy to vary inputs; no direct use needed.
            let _ = lit_val;
            let cf = crate::rules::ConstantFold;
            // A filter predicate with only column references has no literals to fold.
            let result = cf.apply(&plan).expect("no error");
            if let Some(LogicalPlan::Filter { predicate: new_pred, .. }) = result {
                // Any new predicate should not be a plain literal.
                assert!(
                    !matches!(new_pred, ScalarExpr::Literal { .. }),
                    "column-column comparison should not fold to literal"
                );
            }
        }
    }

    /// The optimizer works with a custom rule set (just [`ConstantFold`]).
    #[test]
    fn optimizer_with_custom_rule_set_runs_only_that_rule() {
        let mut rs = RuleSet::new();
        rs.add(Box::new(crate::rules::ConstantFold));
        let opt = Optimizer::with_rules(rs);

        let predicate = eq(lit_i32(2), lit_i32(2));
        let plan = LogicalPlan::Filter {
            input: Box::new(scan("t")),
            predicate,
        };
        let result = opt.optimize(plan).expect("optimize ok");
        if let LogicalPlan::Filter { predicate, .. } = &result {
            assert_eq!(predicate, &lit_bool(true), "2 = 2 should fold to true");
        } else {
            panic!("expected Filter");
        }
    }
}
