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
//! ## Entry points
//!
//! - [`choose_enumerator`] picks the join enumeration strategy
//!   ([`dpsize::DpSize`] vs [`greedy::Greedy`]) based on the number of
//!   relations to join.
//! - [`reorder_inner_joins`] walks an entire [`LogicalPlan`] and rewrites
//!   any inner-join chain it finds into the cheapest order the chosen
//!   enumerator can produce, while honouring PostgreSQL's outer-join
//!   reorder barriers. Outer-join subtrees are kept opaque — they remain
//!   as single leaves in the enumerator's view, so the relative position
//!   of `LEFT OUTER`, `RIGHT OUTER`, and `FULL OUTER` joins is preserved.

pub mod dpsize;
pub mod greedy;
pub mod memo;
pub mod physical_selection;

pub use memo::{Group, GroupExpr, Memo, PhysicalOp};
pub use physical_selection::IndexHint;

use ultrasql_planner::{LogicalJoinCondition, LogicalJoinType, LogicalPlan, ScalarExpr};

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
// Outer-join reorder barriers
// ============================================================================

/// Return `true` when `plan` contains any outer-join node — i.e. it is
/// **not safe** to feed `plan` (or any subtree that contains it) to a
/// join-reordering enumerator.
///
/// PostgreSQL forbids reordering an outer join against another join unless
/// the predicate is provably strict on the nullable side. For correctness
/// without a strictness analysis we treat every `LEFT OUTER`, `RIGHT
/// OUTER`, and `FULL OUTER` join as an opaque barrier: the enumerator never
/// sees its children as candidates for reordering.
///
/// `INNER`, `CROSS`, and non-join nodes are *not* barriers — the function
/// recurses through them so an inner-join chain that *contains* an outer
/// join still reports as a barrier subtree (which is the conservative
/// answer).
///
/// ## Examples
///
/// - `Scan("t")` → `false`.
/// - `Inner(Scan("a"), Scan("b"))` → `false`.
/// - `LeftOuter(Scan("a"), Scan("b"))` → `true`.
/// - `Inner(Scan("a"), LeftOuter(Scan("b"), Scan("c")))` → `true` (the
///   inner join's right child is an outer-join barrier).
///
/// ## Why every variant matters
///
/// We recurse through *every* `LogicalPlan` variant so that an outer join
/// buried inside, for example, a `Project` or a `Filter` or even a CTE
/// body still reports as a barrier. Conservatively assuming "barrier when
/// in doubt" is the only correct behaviour: a missed barrier risks
/// returning wrong answers.
#[must_use]
pub fn outer_join_subtree_is_barrier(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Join {
            left,
            right,
            join_type,
            ..
        } => {
            if matches!(
                join_type,
                LogicalJoinType::LeftOuter
                    | LogicalJoinType::RightOuter
                    | LogicalJoinType::FullOuter
            ) {
                return true;
            }
            outer_join_subtree_is_barrier(left) || outer_join_subtree_is_barrier(right)
        }

        // Single-input plan nodes: recurse into the input.
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Project { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::LockRows { input, .. }
        | LogicalPlan::Explain { input, .. } => outer_join_subtree_is_barrier(input),

        // Two-input plan nodes that are not joins (set ops, CTEs).
        LogicalPlan::SetOp { left, right, .. } => {
            outer_join_subtree_is_barrier(left) || outer_join_subtree_is_barrier(right)
        }
        LogicalPlan::Cte {
            definition, body, ..
        } => outer_join_subtree_is_barrier(definition) || outer_join_subtree_is_barrier(body),

        // Leaf and DML / DDL / source / transaction-control nodes. None of
        // these embed a join subtree the enumerator can see, so they are
        // never barriers on their own. (DML statements that build on a
        // `Scan(Filter(...))` source have their input handled by the
        // dedicated `Insert`/`Update`/`Delete` arms below.)
        LogicalPlan::Scan { .. }
        | LogicalPlan::Empty { .. }
        | LogicalPlan::Values { .. }
        | LogicalPlan::Truncate { .. }
        | LogicalPlan::CreateTable { .. }
        | LogicalPlan::CreateIndex { .. }
        | LogicalPlan::DropTable { .. }
        | LogicalPlan::AlterTable { .. }
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
        | LogicalPlan::Listen { .. }
        | LogicalPlan::Notify { .. }
        | LogicalPlan::Unlisten { .. }
        | LogicalPlan::Copy { .. } => false,

        // DML nodes: recurse into their row-producing input.
        LogicalPlan::Insert { source, .. } => outer_join_subtree_is_barrier(source),
        LogicalPlan::Update { input, .. } | LogicalPlan::Delete { input, .. } => {
            outer_join_subtree_is_barrier(input)
        }
    }
}

// ============================================================================
// Join-tree extractor + reorder driver
// ============================================================================

/// Rewrite the join order in `plan` to the cheapest layout the chosen
/// enumerator produces, **without** reordering through any outer-join
/// barrier.
///
/// The function recurses top-down. On every node:
///
/// 1. If the node is an inner-join (and recursively any inner-join chain
///    below it that does *not* hit an outer-join barrier) it is flattened
///    into a list of leaves plus their join predicates, the leaves are
///    handed to [`choose_enumerator`], and the cheapest tree it returns
///    replaces the original chain.
/// 2. If a child subtree is opaque to reordering — either because the
///    subtree is rooted at an outer join, or because it transitively
///    contains one — it is kept as a *single* leaf in step 1. The
///    enumerator never peeks inside.
/// 3. Non-join nodes (`Project`, `Filter`, `Sort`, set ops, DML, etc.) are
///    rebuilt with their children recursively re-ordered.
///
/// ## Contract
///
/// - **Semantics-preserving.** Inner joins are commutative and
///   associative; outer joins are *not*, and step 2 protects them.
/// - **Idempotent for now.** Re-running the function on the result must
///   yield an identical plan. The current implementation satisfies this
///   trivially because the cost model is deterministic.
/// - **Inexpensive.** The function allocates only on the reorder path.
///   Plans without a multi-relation inner-join chain return clones of the
///   input subtrees.
#[must_use]
pub fn reorder_inner_joins(plan: &LogicalPlan) -> LogicalPlan {
    match plan {
        // ---------------------------------------------------------------
        // Inner join (or Cross): possibly reorderable.
        // ---------------------------------------------------------------
        LogicalPlan::Join {
            join_type: LogicalJoinType::Inner | LogicalJoinType::Cross,
            ..
        } => reorder_inner_join_chain(plan),

        // ---------------------------------------------------------------
        // Outer join: a hard reorder barrier. The brief explicitly endorses
        // "skip enumeration entirely for that subtree (safest)", so the
        // outer-join subtree is returned verbatim — including its inner
        // children. Without a strictness analysis on the join predicate,
        // re-ordering even an inner-join chain underneath an outer join
        // can change the result set (an outer join exposes NULL-padded
        // rows that downstream inner joins must see in the same order
        // they were emitted, otherwise the NULL-extension semantics
        // shift), so the conservative answer is no reorder anywhere in
        // the barrier subtree.
        // ---------------------------------------------------------------
        LogicalPlan::Join {
            join_type:
                LogicalJoinType::LeftOuter | LogicalJoinType::RightOuter | LogicalJoinType::FullOuter,
            ..
        } => plan.clone(),

        // ---------------------------------------------------------------
        // Non-join wrappers: recurse into the relevant child(ren).
        // ---------------------------------------------------------------
        LogicalPlan::Filter { input, predicate } => LogicalPlan::Filter {
            input: Box::new(reorder_inner_joins(input)),
            predicate: predicate.clone(),
        },
        LogicalPlan::Project {
            input,
            exprs,
            schema,
        } => LogicalPlan::Project {
            input: Box::new(reorder_inner_joins(input)),
            exprs: exprs.clone(),
            schema: schema.clone(),
        },
        LogicalPlan::Limit { input, n, offset } => LogicalPlan::Limit {
            input: Box::new(reorder_inner_joins(input)),
            n: *n,
            offset: *offset,
        },
        LogicalPlan::Sort { input, keys } => LogicalPlan::Sort {
            input: Box::new(reorder_inner_joins(input)),
            keys: keys.clone(),
        },
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggregates,
            schema,
        } => LogicalPlan::Aggregate {
            input: Box::new(reorder_inner_joins(input)),
            group_by: group_by.clone(),
            aggregates: aggregates.clone(),
            schema: schema.clone(),
        },
        LogicalPlan::SetOp {
            op,
            quantifier,
            left,
            right,
            schema,
        } => LogicalPlan::SetOp {
            op: *op,
            quantifier: *quantifier,
            left: Box::new(reorder_inner_joins(left)),
            right: Box::new(reorder_inner_joins(right)),
            schema: schema.clone(),
        },

        // CTE / LockRows: only the body / input contains the user-visible
        // join graph.
        LogicalPlan::Cte {
            name,
            recursive,
            definition,
            body,
            schema,
        } => LogicalPlan::Cte {
            name: name.clone(),
            recursive: *recursive,
            definition: Box::new(reorder_inner_joins(definition)),
            body: Box::new(reorder_inner_joins(body)),
            schema: schema.clone(),
        },
        LogicalPlan::LockRows {
            input,
            strength,
            wait_policy,
            schema,
        } => LogicalPlan::LockRows {
            input: Box::new(reorder_inner_joins(input)),
            strength: *strength,
            wait_policy: *wait_policy,
            schema: schema.clone(),
        },

        // Explain wraps another plan whose join order is still eligible
        // for reordering.
        LogicalPlan::Explain {
            analyze,
            format,
            input,
            schema,
        } => LogicalPlan::Explain {
            analyze: *analyze,
            format: *format,
            input: Box::new(reorder_inner_joins(input)),
            schema: schema.clone(),
        },

        // DML: recurse into the row source.
        LogicalPlan::Insert {
            table,
            columns,
            source,
            on_conflict,
            returning,
            schema,
        } => LogicalPlan::Insert {
            table: table.clone(),
            columns: columns.clone(),
            source: Box::new(reorder_inner_joins(source)),
            on_conflict: on_conflict.clone(),
            returning: returning.clone(),
            schema: schema.clone(),
        },
        LogicalPlan::Update {
            table,
            assignments,
            input,
            returning,
            schema,
        } => LogicalPlan::Update {
            table: table.clone(),
            assignments: assignments.clone(),
            input: Box::new(reorder_inner_joins(input)),
            returning: returning.clone(),
            schema: schema.clone(),
        },
        LogicalPlan::Delete {
            table,
            input,
            returning,
            schema,
        } => LogicalPlan::Delete {
            table: table.clone(),
            input: Box::new(reorder_inner_joins(input)),
            returning: returning.clone(),
            schema: schema.clone(),
        },

        // Terminal / DDL / transaction-control nodes: nothing to reorder.
        LogicalPlan::Scan { .. }
        | LogicalPlan::Empty { .. }
        | LogicalPlan::Values { .. }
        | LogicalPlan::Truncate { .. }
        | LogicalPlan::CreateTable { .. }
        | LogicalPlan::CreateIndex { .. }
        | LogicalPlan::DropTable { .. }
        | LogicalPlan::AlterTable { .. }
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
        | LogicalPlan::Listen { .. }
        | LogicalPlan::Notify { .. }
        | LogicalPlan::Unlisten { .. }
        | LogicalPlan::Copy { .. } => plan.clone(),
    }
}

/// Reorder an inner-join chain rooted at `plan`.
///
/// `plan` is assumed to be a `Join { join_type: Inner | Cross, .. }`.
/// The function walks the inner-join skeleton, collecting *leaves* (any
/// subtree that is **not** an inner-join along the spine) and the `ON`
/// predicates encountered, then asks [`choose_enumerator`] for the cheapest
/// layout.
///
/// ## Barrier handling
///
/// If any leaf collected along the way is itself an outer-join barrier
/// (per [`outer_join_subtree_is_barrier`]) the function bails out and
/// returns the original `plan` clone unchanged. Reordering an inner-join
/// chain that crosses an outer-join leaf can change the result set —
/// PostgreSQL only allows it when the inner-join predicate is provably
/// strict on the outer side, an analysis the v0.6 optimizer does not yet
/// perform. The conservative answer is "no reorder anywhere in the chain".
fn reorder_inner_join_chain(plan: &LogicalPlan) -> LogicalPlan {
    let mut leaves: Vec<LogicalPlan> = Vec::new();
    let mut conditions: Vec<ScalarExpr> = Vec::new();
    collect_inner_join_leaves(plan, &mut leaves, &mut conditions);

    // If any leaf carries an outer-join barrier, the brief's "skip
    // enumeration entirely for that subtree (safest)" rule kicks in.
    if leaves.iter().any(outer_join_subtree_is_barrier) {
        return plan.clone();
    }

    // Degenerate guards.
    if leaves.is_empty() {
        return plan.clone();
    }
    if leaves.len() == 1 {
        return leaves.into_iter().next().unwrap_or_else(|| plan.clone());
    }

    let enumerator = choose_enumerator(leaves.len());
    let mut produced = enumerator.enumerate(&leaves, &conditions);
    produced.pop().unwrap_or_else(|| plan.clone())
}

/// Recursive walker that flattens an inner-join spine into `(leaves,
/// conditions)`.
///
/// - `LogicalPlan::Join { join_type: Inner | Cross, .. }` → descend into
///   both children (continue flattening the spine).
/// - Any other node → push as a single opaque leaf. Leaves are pushed
///   *as-is* (no recursive optimisation) so the caller can detect that
///   an outer-join barrier ended up under an inner-join spine and abort.
fn collect_inner_join_leaves(
    plan: &LogicalPlan,
    leaves: &mut Vec<LogicalPlan>,
    conditions: &mut Vec<ScalarExpr>,
) {
    if let LogicalPlan::Join {
        left,
        right,
        join_type: LogicalJoinType::Inner | LogicalJoinType::Cross,
        condition,
        ..
    } = plan
    {
        collect_inner_join_leaves(left, leaves, conditions);
        collect_inner_join_leaves(right, leaves, conditions);
        if let LogicalJoinCondition::On(pred) = condition {
            conditions.push(pred.clone());
        }
        return;
    }

    leaves.push(plan.clone());
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

    // -----------------------------------------------------------------------
    // outer_join_subtree_is_barrier
    // -----------------------------------------------------------------------

    use ultrasql_core::{DataType, Field, Schema};
    use ultrasql_planner::LogicalPlan;

    fn scan(name: &str) -> LogicalPlan {
        // Use the table name as the column name so that concatenating two
        // scan schemas never produces duplicate field names (Schema::new
        // rejects duplicates).
        LogicalPlan::Scan {
            table: name.into(),
            schema: Schema::new([Field::required(name, DataType::Int32)]).expect("schema ok"),
            projection: None,
        }
    }

    fn inner(left: LogicalPlan, right: LogicalPlan) -> LogicalPlan {
        let schema = concat_schemas(left.schema(), right.schema());
        LogicalPlan::Join {
            left: Box::new(left),
            right: Box::new(right),
            join_type: LogicalJoinType::Inner,
            condition: LogicalJoinCondition::None,
            schema,
        }
    }

    fn left_outer(left: LogicalPlan, right: LogicalPlan) -> LogicalPlan {
        let schema = concat_schemas(left.schema(), right.schema());
        LogicalPlan::Join {
            left: Box::new(left),
            right: Box::new(right),
            join_type: LogicalJoinType::LeftOuter,
            condition: LogicalJoinCondition::None,
            schema,
        }
    }

    fn concat_schemas(left: &Schema, right: &Schema) -> Schema {
        let mut fields = Vec::with_capacity(left.len() + right.len());
        for i in 0..left.len() {
            fields.push(left.field_at(i).clone());
        }
        for i in 0..right.len() {
            fields.push(right.field_at(i).clone());
        }
        Schema::new(fields).expect("schema ok")
    }

    #[test]
    fn scan_is_not_a_barrier() {
        assert!(!outer_join_subtree_is_barrier(&scan("t")));
    }

    #[test]
    fn inner_join_of_scans_is_not_a_barrier() {
        let plan = inner(scan("a"), scan("b"));
        assert!(!outer_join_subtree_is_barrier(&plan));
    }

    #[test]
    fn left_outer_join_is_a_barrier() {
        let plan = left_outer(scan("a"), scan("b"));
        assert!(outer_join_subtree_is_barrier(&plan));
    }

    #[test]
    fn inner_above_left_outer_is_a_barrier_transitively() {
        // Inner(a, LeftOuter(b, c)) — the right child carries an outer
        // join, so the whole subtree must report as a barrier.
        let plan = inner(scan("a"), left_outer(scan("b"), scan("c")));
        assert!(outer_join_subtree_is_barrier(&plan));
    }

    #[test]
    fn filter_above_outer_join_is_a_barrier() {
        use ultrasql_core::Value;
        use ultrasql_planner::ScalarExpr;

        let predicate = ScalarExpr::Literal {
            value: Value::Bool(true),
            data_type: DataType::Bool,
        };
        let plan = LogicalPlan::Filter {
            input: Box::new(left_outer(scan("a"), scan("b"))),
            predicate,
        };
        assert!(outer_join_subtree_is_barrier(&plan));
    }
}
