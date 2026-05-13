//! Greedy (GEQO-style) join enumerator for large queries.
//!
//! When the number of relations exceeds the DPsize threshold (10), exhaustive
//! enumeration becomes too expensive. This module implements a simple greedy
//! heuristic: at each step pick the pair of available "relation trees" that
//! produces the lowest-cost join, fold them into a single node, and repeat
//! until only one tree remains.
//!
//! ## Complexity
//!
//! O(n²) comparisons per iteration × O(n) iterations = O(n³) total. For
//! n = 100 this is 10⁶ cost evaluations — fast enough for interactive queries.
//!
//! ## Quality
//!
//! The greedy heuristic does *not* guarantee the globally optimal order.
//! However, for large queries the search space is too large for exhaustive
//! enumeration, and greedy gives a reasonable starting point for local
//! improvement heuristics that land in v0.7.

use ultrasql_core::{Field, Schema};
use ultrasql_planner::{LogicalJoinCondition, LogicalJoinType, LogicalPlan, ScalarExpr};

use crate::cost::{CostModel, NoStats};
use crate::enumeration::JoinEnumerator;

// ============================================================================
// Greedy
// ============================================================================

/// Greedy GEQO-style join enumerator.
///
/// At each step, evaluates all O(k²) pairwise joins of the remaining
/// candidates and picks the cheapest, where *k* is the number of remaining
/// partial trees. Iterates until one tree remains.
///
/// The cost model defaults to `CostModel::new(&NoStats)`.
#[derive(Debug, Default)]
pub struct Greedy {
    _private: (),
}

impl Greedy {
    /// Create a new `Greedy` enumerator.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl JoinEnumerator for Greedy {
    /// Greedily join `leaves` until a single plan tree remains.
    ///
    /// Returns an empty `Vec` when `leaves` is empty. Returns the single
    /// leaf when `leaves` has exactly one element.
    fn enumerate(&self, leaves: &[LogicalPlan], conditions: &[ScalarExpr]) -> Vec<LogicalPlan> {
        if leaves.is_empty() {
            return Vec::new();
        }
        if leaves.len() == 1 {
            return vec![leaves[0].clone()];
        }

        let stats = NoStats;
        let model = CostModel::new(&stats);

        // Working set: mutable list of partial trees.
        let mut trees: Vec<LogicalPlan> = leaves.to_vec();

        while trees.len() > 1 {
            let n = trees.len();
            let mut best_cost = f64::INFINITY;
            let mut best_i = 0_usize;
            let mut best_j = 1_usize;

            // Evaluate all distinct pairs (i, j).
            for i in 0..n {
                for j in (i + 1)..n {
                    let candidate = build_join(trees[i].clone(), trees[j].clone(), conditions);
                    let cost = model.estimate(&candidate).total_cost;
                    if cost < best_cost {
                        best_cost = cost;
                        best_i = i;
                        best_j = j;
                    }
                }
            }

            // Fold best pair: replace trees[best_i] with the join, remove trees[best_j].
            let right = trees.remove(best_j);
            let left = trees[best_i].clone();
            trees[best_i] = build_join(left, right, conditions);
        }

        trees
    }
}

// ============================================================================
// Helpers
// ============================================================================

/// Construct a `LogicalPlan::Join` from two children and the first available
/// condition (cross join when no conditions exist).
fn build_join(left: LogicalPlan, right: LogicalPlan, conditions: &[ScalarExpr]) -> LogicalPlan {
    let condition = conditions
        .first()
        .map_or(LogicalJoinCondition::None, |cond| {
            LogicalJoinCondition::On(cond.clone())
        });
    let schema = concat_schemas(left.schema(), right.schema());
    LogicalPlan::Join {
        left: Box::new(left),
        right: Box::new(right),
        join_type: LogicalJoinType::Inner,
        condition,
        schema,
    }
}

/// Concatenate two schemas into a flat schema (cost-estimation helper).
fn concat_schemas(left: &Schema, right: &Schema) -> Schema {
    let mut fields: Vec<Field> = Vec::with_capacity(left.len() + right.len());
    for i in 0..left.len() {
        fields.push(left.field_at(i).clone());
    }
    for i in 0..right.len() {
        fields.push(right.field_at(i).clone());
    }
    Schema::new(fields).unwrap_or_else(|_| Schema::empty())
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use ultrasql_core::{DataType, Field, Schema};
    use ultrasql_planner::LogicalPlan;

    use super::*;

    fn scan(table: &str) -> LogicalPlan {
        LogicalPlan::Scan {
            table: table.into(),
            schema: Schema::new([Field::required("id", DataType::Int32)]).expect("schema ok"),
            projection: None,
        }
    }

    /// A single leaf is returned unchanged.
    #[test]
    fn greedy_single_leaf_returned_as_is() {
        let g = Greedy::new();
        let result = g.enumerate(&[scan("a")], &[]);
        assert_eq!(result.len(), 1);
    }

    /// Two leaves produce one Join.
    #[test]
    fn greedy_two_leaves_produce_join() {
        let g = Greedy::new();
        let result = g.enumerate(&[scan("a"), scan("b")], &[]);
        assert_eq!(result.len(), 1);
        assert!(matches!(&result[0], LogicalPlan::Join { .. }));
    }

    /// The greedy enumerator terminates for 10 relations within reasonable time.
    #[test]
    fn greedy_terminates_on_ten_relations() {
        let leaves: Vec<LogicalPlan> = (0..10).map(|i| scan(&format!("t{i}"))).collect();
        let g = Greedy::new();
        let result = g.enumerate(&leaves, &[]);
        assert_eq!(result.len(), 1, "greedy must produce one plan");
        assert!(matches!(&result[0], LogicalPlan::Join { .. }));
    }

    /// The greedy enumerator terminates for 20 relations (beyond DPsize
    /// threshold) and produces a single Join tree.
    #[test]
    fn greedy_terminates_on_twenty_relations() {
        let leaves: Vec<LogicalPlan> = (0..20).map(|i| scan(&format!("t{i}"))).collect();
        let g = Greedy::new();
        let result = g.enumerate(&leaves, &[]);
        assert_eq!(result.len(), 1);
    }

    /// Empty leaves returns empty.
    #[test]
    fn greedy_empty_returns_empty() {
        let g = Greedy::new();
        assert!(g.enumerate(&[], &[]).is_empty());
    }
}
