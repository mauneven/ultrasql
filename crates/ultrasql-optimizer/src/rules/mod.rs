//! Logical-plan rewrite rules.
//!
//! This module defines the [`RewriteRule`] trait and the [`RuleSet`] registry
//! that the [`crate::Optimizer`] driver iterates to converge a plan to a fixed
//! point.
//!
//! Each rule is a stateless struct that implements [`RewriteRule`]. Rules
//! return `Some(new_plan)` when they transform the plan and `None` when the
//! plan is already in the form the rule prefers. The driver treats `None` as a
//! "no-op vote" and stops iterating when all rules vote no-op in a single
//! pass.
//!
//! ## Built-in rules (registered order)
//!
//! 1. [`ConstantFold`] — fold literal arithmetic, boolean identities, and
//!    `IS NULL` on literals.
//! 2. [`PredicatePushdown`] — push filter predicates through projections and
//!    into join sides.
//! 3. [`ProjectionPushdown`] — prune unused columns from scans.
//! 4. [`OuterJoinElimination`] — convert outer joins to inner joins when
//!    filter predicates imply non-NULL right-side columns.
//! 5. [`LimitPushdown`] — push limits through projections; fuse with sort
//!    (v0.7 placeholder).
//! 6. [`InListToSemi`] — collapse OR-lists of equality on the same column.
//! 7. [`SubqueryDecorrelation`] — transform correlated subqueries to joins.
//! 8. [`CommonSubExprElimination`] — hoist duplicate `ScalarExpr` sub-trees.
//! 9. [`PredicatePushdownSubquery`] — push filters into derived tables and CTEs.
//!
//! ## Advisory rules (registered but produce no structural rewrite)
//!
//! - [`SortElimination`] — logs when a sort can be eliminated via an index;
//!   requires an `IndexHint` catalog to be injected at construction time.
//!   Not included in `default_rules()` (no catalog available statically).

pub mod common_subexpr;
pub mod constant_fold;
pub mod in_list_to_semi;
pub mod limit_pushdown;
pub mod outer_join_elimination;
pub mod predicate_pushdown;
pub mod predicate_pushdown_subquery;
pub mod projection_pushdown;
pub mod semi_join_pushdown;
pub mod sort_elimination;
pub mod subquery_decorrelation;

pub use common_subexpr::CommonSubExprElimination;
pub use constant_fold::ConstantFold;
pub use in_list_to_semi::InListToSemi;
pub use limit_pushdown::LimitPushdown;
pub use outer_join_elimination::OuterJoinElimination;
pub use predicate_pushdown::PredicatePushdown;
pub use predicate_pushdown_subquery::PredicatePushdownSubquery;
pub use projection_pushdown::ProjectionPushdown;
pub use semi_join_pushdown::SemiJoinPushdown;
pub use sort_elimination::SortElimination;
pub use subquery_decorrelation::SubqueryDecorrelation;

use ultrasql_planner::LogicalPlan;

use crate::error::OptimizeError;

// ============================================================================
// RewriteRule trait
// ============================================================================

/// A logical-plan rewrite.
///
/// A rule walks a [`LogicalPlan`] and returns `Some(rewritten)` when it can
/// transform the plan, or `None` when no transformation applies. The
/// optimizer's driver loops over the registered rules until a fixed point is
/// reached (no rule produced `Some` in a complete pass) or the iteration cap
/// is hit.
///
/// ## Implementation contract
///
/// - A rule must be deterministic: given the same input plan it always
///   produces the same output.
/// - A rule must converge: repeated application must eventually reach `None`
///   (the plan cannot oscillate).
/// - A rule must be semantics-preserving: the rewritten plan produces
///   identical results for any valid input.
/// - A rule must not allocate on the no-op path (i.e., when it returns
///   `None`).
///
/// Rules are `Send + Sync` so that the optimizer can be used from async
/// contexts without wrapping rules in a `Mutex`.
pub trait RewriteRule: Send + Sync {
    /// Short, stable name used in tracing and `EXPLAIN` output.
    fn name(&self) -> &'static str;

    /// Apply the rule to `plan`.
    ///
    /// # Returns
    ///
    /// - `Ok(Some(new_plan))` — the rule transformed `plan`.
    /// - `Ok(None)` — the rule did not apply; `plan` is already in the
    ///   preferred form.
    /// - `Err(e)` — the rule encountered an unrecoverable error.
    fn apply(&self, plan: &LogicalPlan) -> Result<Option<LogicalPlan>, OptimizeError>;
}

// ============================================================================
// RuleSet
// ============================================================================

/// An ordered collection of [`RewriteRule`]s.
///
/// The optimizer driver applies rules in the order they are registered. Order
/// matters when one rule's output enables another rule's precondition (e.g.,
/// constant folding before predicate pushdown).
pub struct RuleSet {
    rules: Vec<Box<dyn RewriteRule>>,
}

impl std::fmt::Debug for RuleSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let names: Vec<&str> = self.rules.iter().map(|r| r.name()).collect();
        f.debug_struct("RuleSet").field("rules", &names).finish()
    }
}

impl RuleSet {
    /// Create an empty rule set.
    #[must_use]
    pub fn new() -> Self {
        Self { rules: Vec::new() }
    }

    /// Create a rule set pre-populated with all built-in rules in the
    /// canonical application order.
    ///
    /// [`SortElimination`] is **not** included because it requires an
    /// index catalog to be injected at construction time; add it explicitly
    /// via [`RuleSet::add`] when the catalog is available.
    #[must_use]
    pub fn default_rules() -> Self {
        let mut rs = Self::new();
        rs.add(Box::new(ConstantFold));
        rs.add(Box::new(PredicatePushdown));
        rs.add(Box::new(ProjectionPushdown));
        rs.add(Box::new(OuterJoinElimination));
        rs.add(Box::new(LimitPushdown));
        rs.add(Box::new(InListToSemi));
        rs.add(Box::new(SubqueryDecorrelation));
        rs.add(Box::new(SemiJoinPushdown));
        rs.add(Box::new(CommonSubExprElimination));
        rs.add(Box::new(PredicatePushdownSubquery));
        rs
    }

    /// Append a rule to the end of the registry.
    pub fn add(&mut self, rule: Box<dyn RewriteRule>) {
        self.rules.push(rule);
    }

    /// Ordered slice of registered rules.
    #[must_use]
    pub fn rules(&self) -> &[Box<dyn RewriteRule>] {
        &self.rules
    }
}

impl Default for RuleSet {
    fn default() -> Self {
        Self::new()
    }
}
