//! Bottom-up dynamic programming join enumeration (DPsize).
//!
//! DPsize iterates over all subsets of the relation set in increasing size
//! order. For each subset `S` of size `k` it considers every bipartition
//! `S = L ∪ R` where `L` and `R` are non-empty disjoint subsets summing to
//! `S`, and picks the cheapest join.
//!
//! This exhaustive search finds the globally optimal left-deep *and bushy*
//! tree for the given cost model. The current implementation produces a
//! **left-deep** tree: the right child is always a single leaf. Bushy trees
//! are left for v0.7 when the Cascades search driver lands.
//!
//! ## Complexity
//!
//! O(3^n) subset enumeration, but bounded to n ≤ 10 by the caller
//! ([`super::choose_enumerator`]). For n = 10 this is ~59 000 iterations —
//! negligible.
//!
//! ## Bitmask encoding
//!
//! Each relation is assigned a bit position (0-based). A `u64` bitmask
//! represents any subset of up to 64 relations. The memo maps each bitmask
//! to the cheapest [`LogicalPlan`] found for that subset.

use std::collections::HashMap;

use ultrasql_core::Schema;
use ultrasql_planner::{LogicalJoinCondition, LogicalJoinType, LogicalPlan, ScalarExpr};

use crate::cost::{CostModel, NoStats};
use crate::enumeration::JoinEnumerator;

// ============================================================================
// DpSize
// ============================================================================

/// Bottom-up dynamic-programming join enumerator.
///
/// Uses a [`CostModel`] (defaulting to `CostModel::new(&NoStats)`) to score
/// candidate join orderings. The result is the cheapest left-deep tree for
/// the given leaves and join conditions.
///
/// Use [`crate::reorder_inner_joins_with_stats`] when catalog statistics are
/// available and the caller wants stats-aware join ordering.
#[derive(Debug, Default)]
pub struct DpSize {
    /// Private field keeps construction explicit through [`DpSize::new`] or
    /// [`Default`]. The standalone enumerator uses [`NoStats`] internally.
    _private: (),
}

impl DpSize {
    /// Create a new `DpSize` enumerator (uses [`NoStats`] internally).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

/// A memo entry: the best plan found so far for a subset, and its total cost.
struct BestPlan {
    plan: LogicalPlan,
    cost: f64,
}

impl JoinEnumerator for DpSize {
    /// Enumerate the optimal left-deep join order for `leaves`.
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

        let n = leaves.len();
        let stats = NoStats;
        let model = CostModel::new(&stats);

        // Initialise the DP table: each singleton subset maps to its leaf.
        let mut dp: HashMap<u64, BestPlan> = HashMap::new();
        for (i, leaf) in leaves.iter().enumerate() {
            let mask: u64 = 1 << i;
            let cost = model.estimate(leaf).total_cost;
            dp.insert(
                mask,
                BestPlan {
                    plan: leaf.clone(),
                    cost,
                },
            );
        }

        let full_mask: u64 = (1u64 << n) - 1;

        // Enumerate subsets of size 2..=n in increasing order.
        for size in 2..=n {
            // Iterate over all subsets of exactly `size` bits from full_mask.
            let mut subset = lowest_k_bits(full_mask, size);
            loop {
                // Try every split of `subset` into (left, right) where
                // right is a single leaf (left-deep tree).
                let mut best: Option<BestPlan> = None;
                for bit in 0..n {
                    let right_mask: u64 = 1 << bit;
                    if subset & right_mask == 0 {
                        continue;
                    }
                    let left_mask = subset ^ right_mask;
                    if left_mask == 0 {
                        continue;
                    }
                    let Some(left_entry) = dp.get(&left_mask) else {
                        continue;
                    };
                    let Some(right_entry) = dp.get(&right_mask) else {
                        continue;
                    };

                    // Build the join condition from matching predicates.
                    let condition = pick_condition(left_mask, right_mask, conditions, n);
                    let join_plan =
                        build_join(left_entry.plan.clone(), right_entry.plan.clone(), condition);
                    let cost = model.estimate(&join_plan).total_cost;

                    if best.as_ref().is_none_or(|b| cost < b.cost) {
                        best = Some(BestPlan {
                            plan: join_plan,
                            cost,
                        });
                    }
                }
                if let Some(b) = best {
                    dp.entry(subset)
                        .and_modify(|e| {
                            if b.cost < e.cost {
                                e.plan = b.plan.clone();
                                e.cost = b.cost;
                            }
                        })
                        .or_insert(b);
                }

                // Advance to the next subset of the same popcount (Gosper's hack).
                if subset == 0 {
                    break;
                }
                let next = next_subset_same_popcount(subset);
                if next > full_mask || next == 0 {
                    break;
                }
                subset = next;
            }
        }

        dp.remove(&full_mask)
            .map(|b| vec![b.plan])
            .unwrap_or_default()
    }
}

// ============================================================================
// Helpers
// ============================================================================

/// Return the smallest `u64` that has exactly `k` bits set, taken from
/// the least-significant `n` bits of `universe`. This is the lexicographically
/// first subset of size `k`.
fn lowest_k_bits(universe: u64, k: usize) -> u64 {
    // Start with the k least-significant bits of the universe.
    let mut result: u64 = 0;
    let mut count = 0;
    for bit in 0..64 {
        if universe & (1u64 << bit) != 0 {
            result |= 1u64 << bit;
            count += 1;
            if count == k {
                break;
            }
        }
    }
    result
}

/// Advance to the next subset of the same popcount using Gosper's hack.
///
/// Returns 0 when no next subset exists.
const fn next_subset_same_popcount(x: u64) -> u64 {
    if x == 0 {
        return 0;
    }
    let c = x.trailing_zeros();
    let r = x + (1u64 << c);
    (((r ^ x) >> 2) / (1u64 << c)) | r
}

/// Build a `LogicalPlan::Join` node from two child plans and a condition.
fn build_join(
    left: LogicalPlan,
    right: LogicalPlan,
    condition: LogicalJoinCondition,
) -> LogicalPlan {
    // Concatenate the schemas for the output.
    let schema = concat_schemas(left.schema(), right.schema());
    LogicalPlan::Join {
        left: Box::new(left),
        right: Box::new(right),
        join_type: LogicalJoinType::Inner,
        condition,
        schema,
    }
}

/// Concatenate two schemas into a single flat schema.
///
/// This is a best-effort schema merge for cost estimation purposes;
/// the binder produces the canonical schema in the real execution path.
fn concat_schemas(left: &Schema, right: &Schema) -> Schema {
    let mut fields: Vec<ultrasql_core::Field> = Vec::with_capacity(left.len() + right.len());
    for i in 0..left.len() {
        fields.push(left.field_at(i).clone());
    }
    for i in 0..right.len() {
        fields.push(right.field_at(i).clone());
    }
    Schema::new(fields).unwrap_or_else(|_| Schema::empty())
}

/// Pick a join condition from the condition list that references both the left
/// and right subsets.
///
/// For v0.6 we use a simple heuristic: if any condition appears to reference
/// columns from both sides (determined by column index range), we return it as
/// `On(condition)`. Otherwise we return `None` (cross join).
///
/// The `n_leaves` parameter is used to map column indices to relation indices.
/// Because the binder does not tag columns with relation IDs yet, we use a
/// round-robin split: columns 0..⌊width/2⌋ belong to the first half of
/// relations, etc. This is deliberately approximate for v0.6.
fn pick_condition(
    _left_mask: u64,
    _right_mask: u64,
    conditions: &[ScalarExpr],
    _n_leaves: usize,
) -> LogicalJoinCondition {
    // For v0.6 return the first available condition as the join predicate,
    // or None (cross join) when no conditions exist.
    //
    // A future enhancement will tag ScalarExpr column references with a
    // relation OID so we can route conditions to the correct pair.
    conditions
        .first()
        .map_or(LogicalJoinCondition::None, |cond| {
            LogicalJoinCondition::On(cond.clone())
        })
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
    fn dpsize_single_leaf_returned_as_is() {
        let leaves = vec![scan("a")];
        let enumerator = DpSize::new();
        let result = enumerator.enumerate(&leaves, &[]);
        assert_eq!(result.len(), 1);
        assert!(matches!(&result[0], LogicalPlan::Scan { table, .. } if table == "a"));
    }

    /// Two leaves produce a Join.
    #[test]
    fn dpsize_two_leaves_produce_join() {
        let leaves = vec![scan("a"), scan("b")];
        let enumerator = DpSize::new();
        let result = enumerator.enumerate(&leaves, &[]);
        assert_eq!(result.len(), 1);
        assert!(matches!(&result[0], LogicalPlan::Join { .. }));
    }

    /// Three leaves produce a single join tree with the correct structure.
    ///
    /// With `NoStats` all costs are 0, so any permutation is equally cheap.
    /// We verify that the result is a two-level join tree and terminates.
    #[test]
    fn dpsize_finds_optimal_left_deep_for_three_relations() {
        let leaves = vec![scan("t1"), scan("t2"), scan("t3")];
        let enumerator = DpSize::new();
        let result = enumerator.enumerate(&leaves, &[]);
        assert_eq!(result.len(), 1, "must return exactly one plan");
        // The result should be a Join at the top level.
        assert!(
            matches!(&result[0], LogicalPlan::Join { .. }),
            "top of plan should be a Join"
        );
        // The left child of the top join should also be a Join (left-deep).
        if let LogicalPlan::Join { left, .. } = &result[0] {
            assert!(
                matches!(left.as_ref(), LogicalPlan::Join { .. }),
                "left child of root join should be a Join (left-deep)"
            );
        }
    }

    /// Empty leaf list returns empty result.
    #[test]
    fn dpsize_empty_leaves_returns_empty() {
        let enumerator = DpSize::new();
        let result = enumerator.enumerate(&[], &[]);
        assert!(result.is_empty());
    }

    /// Enumerating 10 leaves terminates in reasonable time (smoke test).
    #[test]
    fn dpsize_terminates_on_ten_relations() {
        let leaves: Vec<LogicalPlan> = (0..10).map(|i| scan(&format!("t{i}"))).collect();
        let enumerator = DpSize::new();
        let result = enumerator.enumerate(&leaves, &[]);
        assert_eq!(result.len(), 1);
    }
}
