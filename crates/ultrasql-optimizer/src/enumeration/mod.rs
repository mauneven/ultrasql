//! Join enumeration and physical operator selection.
//!
//! This module implements the second phase of the UltraSQL optimizer:
//! choosing join order and physical operators for a logical plan tree.
//!
//! ## Sub-modules
//!
//! | Sub-module | Contents |
//! |------------|----------|
//! | [`dpsize`] | Bottom-up DP over relation subsets for <= 10 relations |
//! | [`greedy`] | Greedy pairwise heuristic for > 10 relations |
//! | [`memo`] | Cascades-style memo table (data structures only; search driver lands in v0.7) |
//! | [`physical_selection`] | `NLJ` / `HashJoin` / `MergeJoin`, `HashAgg` / `SortAgg`, `SeqScan` / `IndexScan` |
//!
//! ## Entry point
//!
//! [`choose_enumerator`] picks the right strategy based on the number of
//! relations to join.

pub mod dpsize;
pub mod greedy;
pub mod memo;
pub mod physical_selection;

pub use memo::{Group, GroupExpr, Memo, PhysicalOp};
pub use physical_selection::IndexHint;

use ultrasql_planner::{LogicalPlan, ScalarExpr};

// ============================================================================
// JoinEnumerator trait
// ============================================================================

/// Strategy for ordering a set of relations to be joined.
///
/// Implementations receive a flat list of leaf [`LogicalPlan`] nodes
/// (typically `Scan` or `Filter(Scan, ...)`) and the set of join conditions
/// that connect them, and return an ordered sequence of `LogicalPlan` trees.
/// The caller is responsible for wrapping the result in higher-level plan
/// nodes (sort, aggregate, etc.).
///
/// ## Implementors
///
/// - [`dpsize::DpSize`]: optimal bottom-up DP for <= 10 relations.
/// - [`greedy::Greedy`]: fast pairwise heuristic for > 10 relations.
pub trait JoinEnumerator: Send + Sync {
    /// Enumerate candidate join orderings.
    ///
    /// Returns the best single join tree as a one-element `Vec` for the
    /// current wave. Future waves will return multiple alternatives for the
    /// Cascades search driver to explore.
    ///
    /// ## Arguments
    ///
    /// - `leaves`     -- the leaf relations to join.
    /// - `conditions` -- the join predicates connecting the leaves.
    fn enumerate(&self, leaves: &[LogicalPlan], conditions: &[ScalarExpr]) -> Vec<LogicalPlan>;
}

// ============================================================================
// choose_enumerator
// ============================================================================

/// Select the join enumeration strategy appropriate for `n_relations`.
///
/// - n <= 10: [`dpsize::DpSize`] -- exhaustive DP over subsets.
/// - n > 10:  [`greedy::Greedy`] -- O(n^2) pairwise greedy.
///
/// The threshold of 10 matches PostgreSQL's `geqo_threshold` default.
#[must_use]
pub fn choose_enumerator(n_relations: usize) -> Box<dyn JoinEnumerator> {
    if n_relations <= 10 {
        Box::new(dpsize::DpSize::default())
    } else {
        Box::new(greedy::Greedy::default())
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn choose_enumerator_returns_dp_for_small() {
        let _e = choose_enumerator(5);
        let _e2 = choose_enumerator(10);
    }

    #[test]
    fn choose_enumerator_returns_greedy_for_large() {
        let _e = choose_enumerator(11);
        let _e2 = choose_enumerator(100);
    }

    #[test]
    fn choose_enumerator_boundary_is_inclusive_ten() {
        let e = choose_enumerator(10);
        let result = e.enumerate(&[], &[]);
        assert!(result.is_empty());
    }
}
