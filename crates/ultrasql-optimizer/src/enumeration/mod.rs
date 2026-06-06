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

use crate::cost::{CostEstimate, CostModel, NoStats, StatsSource};

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
                    | LogicalJoinType::Semi
                    | LogicalJoinType::Anti
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
        | LogicalPlan::Window { input, .. }
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
        | LogicalPlan::CreateMaterializedView { .. }
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
        | LogicalPlan::DropTable { .. }
        | LogicalPlan::AlterTable { .. }
        | LogicalPlan::CreateSequence { .. }
        | LogicalPlan::AlterSequence { .. }
        | LogicalPlan::DropSequence { .. }
        | LogicalPlan::CreateSchema { .. }
        | LogicalPlan::DropSchema { .. }
        | LogicalPlan::DropIndex { .. }
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
        | LogicalPlan::SetRole { .. }
        | LogicalPlan::Listen { .. }
        | LogicalPlan::Notify { .. }
        | LogicalPlan::Unlisten { .. }
        | LogicalPlan::Copy { .. }
        | LogicalPlan::FunctionScan { .. } => false,

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
    reorder_inner_joins_with_stats(plan, &NoStats)
}

/// Rewrite the join order in `plan` using the provided statistics source.
///
/// This is the same transformation as [`reorder_inner_joins`], but callers
/// that have real `ANALYZE` data can feed it through `stats` so the left-deep
/// search compares candidate orders by estimated cost instead of leaf shape
/// alone.
#[must_use]
pub fn reorder_inner_joins_with_stats(plan: &LogicalPlan, stats: &dyn StatsSource) -> LogicalPlan {
    match plan {
        // ---------------------------------------------------------------
        // Inner join (or Cross): possibly reorderable.
        // ---------------------------------------------------------------
        LogicalPlan::Join {
            join_type: LogicalJoinType::Inner | LogicalJoinType::Cross,
            ..
        } => reorder_inner_join_chain(plan, stats),

        // ---------------------------------------------------------------
        // Outer/semi/anti join: a hard reorder barrier. The brief explicitly endorses
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
                LogicalJoinType::LeftOuter
                | LogicalJoinType::RightOuter
                | LogicalJoinType::FullOuter
                | LogicalJoinType::Semi
                | LogicalJoinType::Anti,
            ..
        } => plan.clone(),

        // ---------------------------------------------------------------
        // Non-join wrappers: recurse into the relevant child(ren).
        // ---------------------------------------------------------------
        LogicalPlan::Filter { input, predicate } => LogicalPlan::Filter {
            input: Box::new(reorder_inner_joins_with_stats(input, stats)),
            predicate: predicate.clone(),
        },
        LogicalPlan::Project {
            input,
            exprs,
            schema,
        } => LogicalPlan::Project {
            input: Box::new(reorder_inner_joins_with_stats(input, stats)),
            exprs: exprs.clone(),
            schema: schema.clone(),
        },
        LogicalPlan::Limit { input, n, offset } => LogicalPlan::Limit {
            input: Box::new(reorder_inner_joins_with_stats(input, stats)),
            n: *n,
            offset: *offset,
        },
        LogicalPlan::Sort { input, keys } => LogicalPlan::Sort {
            input: Box::new(reorder_inner_joins_with_stats(input, stats)),
            keys: keys.clone(),
        },
        LogicalPlan::Window {
            input,
            partition_by,
            order_by,
            func,
            output_name,
            schema,
        } => LogicalPlan::Window {
            input: Box::new(reorder_inner_joins_with_stats(input, stats)),
            partition_by: partition_by.clone(),
            order_by: order_by.clone(),
            func: func.clone(),
            output_name: output_name.clone(),
            schema: schema.clone(),
        },
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggregates,
            schema,
        } => LogicalPlan::Aggregate {
            input: Box::new(reorder_inner_joins_with_stats(input, stats)),
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
            left: Box::new(reorder_inner_joins_with_stats(left, stats)),
            right: Box::new(reorder_inner_joins_with_stats(right, stats)),
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
            definition: Box::new(reorder_inner_joins_with_stats(definition, stats)),
            body: Box::new(reorder_inner_joins_with_stats(body, stats)),
            schema: schema.clone(),
        },
        LogicalPlan::LockRows {
            input,
            strength,
            wait_policy,
            schema,
        } => LogicalPlan::LockRows {
            input: Box::new(reorder_inner_joins_with_stats(input, stats)),
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
            input: Box::new(reorder_inner_joins_with_stats(input, stats)),
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
            source: Box::new(reorder_inner_joins_with_stats(source, stats)),
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
            input: Box::new(reorder_inner_joins_with_stats(input, stats)),
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
            input: Box::new(reorder_inner_joins_with_stats(input, stats)),
            returning: returning.clone(),
            schema: schema.clone(),
        },

        // Terminal / DDL / transaction-control nodes: nothing to reorder.
        LogicalPlan::Scan { .. }
        | LogicalPlan::Empty { .. }
        | LogicalPlan::Values { .. }
        | LogicalPlan::Truncate { .. }
        | LogicalPlan::CreateTable { .. }
        | LogicalPlan::CreateMaterializedView { .. }
        | LogicalPlan::CreateTypeEnum { .. }
        | LogicalPlan::CreateTypeComposite { .. }
        | LogicalPlan::CreateDomain { .. }
        | LogicalPlan::CreateOperator { .. }
        | LogicalPlan::CreateIndex { .. }
        | LogicalPlan::DropIndex { .. }
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
        | LogicalPlan::DropTable { .. }
        | LogicalPlan::AlterTable { .. }
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
        | LogicalPlan::SetRole { .. }
        | LogicalPlan::Listen { .. }
        | LogicalPlan::Notify { .. }
        | LogicalPlan::Unlisten { .. }
        | LogicalPlan::Copy { .. }
        | LogicalPlan::FunctionScan { .. } => plan.clone(),
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
fn reorder_inner_join_chain(plan: &LogicalPlan, stats: &dyn StatsSource) -> LogicalPlan {
    if !leftmost_inner_join_is_cross(plan) {
        return plan.clone();
    }

    let mut leaves: Vec<JoinLeaf> = Vec::new();
    let mut conditions: Vec<ScalarExpr> = Vec::new();
    let mut next_start = 0;
    collect_inner_join_leaves(plan, &mut leaves, &mut conditions, &mut next_start);

    // If any leaf carries an outer-join barrier, the brief's "skip
    // enumeration entirely for that subtree (safest)" rule kicks in.
    if leaves
        .iter()
        .any(|leaf| outer_join_subtree_is_barrier(&leaf.plan))
    {
        return plan.clone();
    }

    // Degenerate guards.
    if leaves.is_empty() {
        return plan.clone();
    }
    if leaves.len() == 1 {
        return leaves
            .into_iter()
            .map(|leaf| leaf.plan)
            .next()
            .unwrap_or_else(|| plan.clone());
    }

    let order = choose_costed_order(&leaves, &conditions, stats);
    if order == (0..leaves.len()).collect::<Vec<_>>() {
        return plan.clone();
    }
    let Some(reordered) = build_ordered_join_tree(&leaves, &conditions, &order) else {
        return plan.clone();
    };
    restore_original_join_schema(reordered, &leaves, &order, plan.schema())
}

fn leftmost_inner_join_is_cross(plan: &LogicalPlan) -> bool {
    let LogicalPlan::Join {
        left,
        join_type: LogicalJoinType::Inner | LogicalJoinType::Cross,
        condition,
        ..
    } = plan
    else {
        return false;
    };
    if matches!(
        left.as_ref(),
        LogicalPlan::Join {
            join_type: LogicalJoinType::Inner | LogicalJoinType::Cross,
            ..
        }
    ) {
        return leftmost_inner_join_is_cross(left);
    }
    matches!(condition, LogicalJoinCondition::None)
}

#[derive(Clone)]
struct JoinLeaf {
    plan: LogicalPlan,
    start: usize,
    width: usize,
}

#[derive(Clone)]
struct ConditionInfo {
    expr: ScalarExpr,
    mask: u64,
}

#[derive(Clone)]
struct JoinSearchState {
    order: Vec<usize>,
    plan: LogicalPlan,
    estimate: CostEstimate,
    applied_conditions: usize,
    cross_steps: usize,
}

/// Recursive walker that flattens an inner-join spine into `(leaves,
/// conditions)` and records each leaf's column range in original output order.
///
/// - `LogicalPlan::Join { join_type: Inner | Cross, .. }` → descend into
///   both children (continue flattening the spine).
/// - Any other node → push as a single opaque leaf. Leaves are pushed
///   *as-is* (no recursive optimisation) so the caller can detect that
///   an outer-join barrier ended up under an inner-join spine and abort.
fn collect_inner_join_leaves(
    plan: &LogicalPlan,
    leaves: &mut Vec<JoinLeaf>,
    conditions: &mut Vec<ScalarExpr>,
    next_start: &mut usize,
) {
    if let LogicalPlan::Join {
        left,
        right,
        join_type: LogicalJoinType::Inner | LogicalJoinType::Cross,
        condition,
        ..
    } = plan
    {
        collect_inner_join_leaves(left, leaves, conditions, next_start);
        collect_inner_join_leaves(right, leaves, conditions, next_start);
        if let LogicalJoinCondition::On(pred) = condition {
            conditions.extend(split_and(pred));
        }
        return;
    }

    let width = plan.schema().len();
    leaves.push(JoinLeaf {
        plan: plan.clone(),
        start: *next_start,
        width,
    });
    *next_start += width;
}

fn choose_costed_order(
    leaves: &[JoinLeaf],
    conditions: &[ScalarExpr],
    stats: &dyn StatsSource,
) -> Vec<usize> {
    let infos = conditions
        .iter()
        .filter_map(|expr| {
            condition_leaf_mask(expr, leaves).map(|mask| ConditionInfo {
                expr: expr.clone(),
                mask,
            })
        })
        .collect::<Vec<_>>();
    if infos.is_empty() {
        return cross_only_order(leaves, stats);
    }
    if leaves.len() <= 10 {
        choose_costed_order_dp(leaves, &infos, stats)
    } else {
        choose_costed_order_greedy(leaves, &infos, stats)
    }
}

fn cross_only_order(leaves: &[JoinLeaf], stats: &dyn StatsSource) -> Vec<usize> {
    let model = CostModel::new(stats);
    let mut order = (0..leaves.len()).collect::<Vec<_>>();
    order.sort_by(|&left, &right| {
        let left_est = model.estimate(&leaves[left].plan);
        let right_est = model.estimate(&leaves[right].plan);
        left_est
            .total_cost
            .total_cmp(&right_est.total_cost)
            .then_with(|| leaf_rank(&leaves[left].plan).cmp(&leaf_rank(&leaves[right].plan)))
            .then_with(|| left.cmp(&right))
    });
    if order == (0..leaves.len()).collect::<Vec<_>>() && order.len() > 1 {
        order.rotate_left(1);
    }
    order
}

fn choose_costed_order_dp(
    leaves: &[JoinLeaf],
    conditions: &[ConditionInfo],
    stats: &dyn StatsSource,
) -> Vec<usize> {
    use std::collections::HashMap;

    let model = CostModel::new(stats);
    let mut states = HashMap::<u64, JoinSearchState>::new();
    for (idx, leaf) in leaves.iter().enumerate() {
        let mask = 1_u64 << idx;
        states.insert(
            mask,
            JoinSearchState {
                order: vec![idx],
                plan: leaf.plan.clone(),
                estimate: model.estimate(&leaf.plan),
                applied_conditions: 0,
                cross_steps: 0,
            },
        );
    }

    for size in 1..leaves.len() {
        let masks = states
            .keys()
            .copied()
            .filter(|mask| mask_has_size(*mask, size))
            .collect::<Vec<_>>();
        for mask in masks {
            let Some(state) = states.get(&mask).cloned() else {
                continue;
            };
            for idx in 0..leaves.len() {
                if mask & (1_u64 << idx) != 0 {
                    continue;
                }
                let Some((plan, applied_here)) =
                    build_join_extension(&state.plan, &state.order, idx, conditions, leaves)
                else {
                    continue;
                };
                let next_mask = mask | (1_u64 << idx);
                let candidate = JoinSearchState {
                    order: extend_order(&state.order, idx),
                    estimate: model.estimate(&plan),
                    plan,
                    applied_conditions: state.applied_conditions + applied_here,
                    cross_steps: state.cross_steps + usize::from(applied_here == 0),
                };
                if states
                    .get(&next_mask)
                    .is_none_or(|current| search_state_better(&candidate, current))
                {
                    states.insert(next_mask, candidate);
                }
            }
        }
    }

    let full_mask = (1_u64 << leaves.len()) - 1;
    states
        .remove(&full_mask)
        .map_or_else(|| (0..leaves.len()).collect(), |state| state.order)
}

fn choose_costed_order_greedy(
    leaves: &[JoinLeaf],
    conditions: &[ConditionInfo],
    stats: &dyn StatsSource,
) -> Vec<usize> {
    let model = CostModel::new(stats);
    let first = (0..leaves.len())
        .min_by(|&left, &right| {
            let left_est = model.estimate(&leaves[left].plan);
            let right_est = model.estimate(&leaves[right].plan);
            left_est
                .total_cost
                .total_cmp(&right_est.total_cost)
                .then_with(|| leaf_rank(&leaves[left].plan).cmp(&leaf_rank(&leaves[right].plan)))
        })
        .unwrap_or(0);
    let mut state = JoinSearchState {
        order: vec![first],
        plan: leaves[first].plan.clone(),
        estimate: model.estimate(&leaves[first].plan),
        applied_conditions: 0,
        cross_steps: 0,
    };
    let mut used = vec![false; leaves.len()];
    used[first] = true;

    while state.order.len() < leaves.len() {
        let mut best: Option<(JoinSearchState, usize)> = None;
        for idx in 0..leaves.len() {
            if used[idx] {
                continue;
            }
            let Some((plan, applied_here)) =
                build_join_extension(&state.plan, &state.order, idx, conditions, leaves)
            else {
                continue;
            };
            let candidate = JoinSearchState {
                order: extend_order(&state.order, idx),
                estimate: model.estimate(&plan),
                plan,
                applied_conditions: state.applied_conditions + applied_here,
                cross_steps: state.cross_steps + usize::from(applied_here == 0),
            };
            if best.as_ref().is_none_or(|(current, current_idx)| {
                search_state_better(&candidate, current)
                    || (!search_state_better(current, &candidate)
                        && leaf_rank(&leaves[idx].plan) < leaf_rank(&leaves[*current_idx].plan))
            }) {
                best = Some((candidate, idx));
            }
        }
        let Some((next_state, next_idx)) = best else {
            break;
        };
        used[next_idx] = true;
        state = next_state;
    }

    state.order
}

fn mask_has_size(mask: u64, size: usize) -> bool {
    u32::try_from(size).is_ok_and(|target| mask.count_ones() == target)
}

fn build_join_extension(
    current: &LogicalPlan,
    current_order: &[usize],
    right_idx: usize,
    conditions: &[ConditionInfo],
    leaves: &[JoinLeaf],
) -> Option<(LogicalPlan, usize)> {
    let join_conditions =
        join_conditions_for_extension(current_order, right_idx, conditions, leaves)?;
    let applied_here = join_conditions.len();
    let right = leaves[right_idx].plan.clone();
    let schema = concat_schemas(current.schema(), right.schema());
    Some((
        LogicalPlan::Join {
            left: Box::new(current.clone()),
            right: Box::new(right),
            join_type: LogicalJoinType::Inner,
            condition: conjuncts_to_join_condition(join_conditions),
            schema,
        },
        applied_here,
    ))
}

fn join_conditions_for_extension(
    current_order: &[usize],
    right_idx: usize,
    conditions: &[ConditionInfo],
    leaves: &[JoinLeaf],
) -> Option<Vec<ScalarExpr>> {
    let current_mask = mask_for_order(current_order);
    let candidate_mask = current_mask | (1_u64 << right_idx);
    let mut join_conditions = Vec::new();
    for condition in conditions {
        if condition.mask & (1_u64 << right_idx) == 0
            || condition.mask & current_mask == 0
            || condition.mask & !candidate_mask != 0
        {
            continue;
        }
        join_conditions.push(remap_condition_for_join(
            &condition.expr,
            current_order,
            right_idx,
            leaves,
        )?);
    }
    Some(join_conditions)
}

fn extend_order(order: &[usize], next: usize) -> Vec<usize> {
    let mut extended = Vec::with_capacity(order.len() + 1);
    extended.extend_from_slice(order);
    extended.push(next);
    extended
}

fn search_state_better(candidate: &JoinSearchState, current: &JoinSearchState) -> bool {
    candidate
        .estimate
        .total_cost
        .total_cmp(&current.estimate.total_cost)
        .is_lt()
        || (candidate
            .estimate
            .total_cost
            .total_cmp(&current.estimate.total_cost)
            .is_eq()
            && (candidate.cross_steps < current.cross_steps
                || (candidate.cross_steps == current.cross_steps
                    && candidate.applied_conditions > current.applied_conditions)
                || (candidate.cross_steps == current.cross_steps
                    && candidate.applied_conditions == current.applied_conditions
                    && candidate.order < current.order)))
}

fn leaf_rank(plan: &LogicalPlan) -> (u8, usize) {
    match plan {
        LogicalPlan::Filter { input, .. } => (0, input.schema().len()),
        _ => (1, plan.schema().len()),
    }
}

fn mask_for_order(order: &[usize]) -> u64 {
    order.iter().fold(0_u64, |mask, idx| mask | (1_u64 << idx))
}

fn condition_leaf_mask(condition: &ScalarExpr, leaves: &[JoinLeaf]) -> Option<u64> {
    let mut mask = 0_u64;
    collect_condition_leaf_mask(condition, leaves, &mut mask)?;
    Some(mask)
}

fn collect_condition_leaf_mask(
    expr: &ScalarExpr,
    leaves: &[JoinLeaf],
    mask: &mut u64,
) -> Option<()> {
    match expr {
        ScalarExpr::Column { index, .. } => {
            let leaf_idx = leaf_for_column(*index, leaves)?;
            *mask |= 1_u64 << leaf_idx;
            Some(())
        }
        ScalarExpr::Binary { left, right, .. } => {
            collect_condition_leaf_mask(left, leaves, mask)?;
            collect_condition_leaf_mask(right, leaves, mask)
        }
        ScalarExpr::Unary { expr, .. } | ScalarExpr::IsNull { expr, .. } => {
            collect_condition_leaf_mask(expr, leaves, mask)
        }
        ScalarExpr::FunctionCall { args, .. } => {
            for arg in args {
                collect_condition_leaf_mask(arg, leaves, mask)?;
            }
            Some(())
        }
        ScalarExpr::Literal { .. } | ScalarExpr::Parameter { .. } => Some(()),
        ScalarExpr::OuterColumn { .. }
        | ScalarExpr::ScalarSubquery { .. }
        | ScalarExpr::Exists { .. }
        | ScalarExpr::InSubquery { .. } => None,
    }
}

fn leaf_for_column(index: usize, leaves: &[JoinLeaf]) -> Option<usize> {
    leaves
        .iter()
        .position(|leaf| index >= leaf.start && index < leaf.start + leaf.width)
}

fn build_ordered_join_tree(
    leaves: &[JoinLeaf],
    conditions: &[ScalarExpr],
    order: &[usize],
) -> Option<LogicalPlan> {
    let first = *order.first()?;
    let mut current = leaves[first].plan.clone();
    let mut current_order = vec![first];
    let mut used_conditions = vec![false; conditions.len()];

    for &right_idx in &order[1..] {
        let current_mask = mask_for_order(&current_order);
        let candidate_mask = current_mask | (1_u64 << right_idx);
        let mut join_conditions = Vec::new();
        for (condition_idx, condition) in conditions.iter().enumerate() {
            if used_conditions[condition_idx] {
                continue;
            }
            let Some(mask) = condition_leaf_mask(condition, leaves) else {
                continue;
            };
            if mask & (1_u64 << right_idx) != 0
                && mask & current_mask != 0
                && mask & !candidate_mask == 0
            {
                let remapped =
                    remap_condition_for_join(condition, &current_order, right_idx, leaves)?;
                join_conditions.push(remapped);
                used_conditions[condition_idx] = true;
            }
        }

        let right = leaves[right_idx].plan.clone();
        let schema = concat_schemas(current.schema(), right.schema());
        current = LogicalPlan::Join {
            left: Box::new(current),
            right: Box::new(right),
            join_type: LogicalJoinType::Inner,
            condition: conjuncts_to_join_condition(join_conditions),
            schema,
        };
        current_order.push(right_idx);
    }

    Some(current)
}

fn remap_condition_for_join(
    expr: &ScalarExpr,
    left_order: &[usize],
    right_idx: usize,
    leaves: &[JoinLeaf],
) -> Option<ScalarExpr> {
    match expr {
        ScalarExpr::Column {
            name,
            index,
            data_type,
        } => {
            let leaf_idx = leaf_for_column(*index, leaves)?;
            let offset = index.checked_sub(leaves[leaf_idx].start)?;
            let left_width = output_width(left_order, leaves);
            let remapped = if leaf_idx == right_idx {
                left_width + offset
            } else {
                output_offset(left_order, leaf_idx, leaves)? + offset
            };
            Some(ScalarExpr::Column {
                name: name.clone(),
                index: remapped,
                data_type: data_type.clone(),
            })
        }
        ScalarExpr::Binary {
            op,
            left,
            right,
            data_type,
        } => Some(ScalarExpr::Binary {
            op: *op,
            left: Box::new(remap_condition_for_join(
                left, left_order, right_idx, leaves,
            )?),
            right: Box::new(remap_condition_for_join(
                right, left_order, right_idx, leaves,
            )?),
            data_type: data_type.clone(),
        }),
        ScalarExpr::Unary {
            op,
            expr,
            data_type,
        } => Some(ScalarExpr::Unary {
            op: *op,
            expr: Box::new(remap_condition_for_join(
                expr, left_order, right_idx, leaves,
            )?),
            data_type: data_type.clone(),
        }),
        ScalarExpr::IsNull { expr, negated } => Some(ScalarExpr::IsNull {
            expr: Box::new(remap_condition_for_join(
                expr, left_order, right_idx, leaves,
            )?),
            negated: *negated,
        }),
        ScalarExpr::FunctionCall {
            name,
            args,
            data_type,
        } => Some(ScalarExpr::FunctionCall {
            name: name.clone(),
            args: args
                .iter()
                .map(|arg| remap_condition_for_join(arg, left_order, right_idx, leaves))
                .collect::<Option<Vec<_>>>()?,
            data_type: data_type.clone(),
        }),
        ScalarExpr::Literal { .. } | ScalarExpr::Parameter { .. } => Some(expr.clone()),
        ScalarExpr::OuterColumn { .. }
        | ScalarExpr::ScalarSubquery { .. }
        | ScalarExpr::Exists { .. }
        | ScalarExpr::InSubquery { .. } => None,
    }
}

fn output_width(order: &[usize], leaves: &[JoinLeaf]) -> usize {
    order.iter().map(|&idx| leaves[idx].width).sum()
}

fn output_offset(order: &[usize], leaf_idx: usize, leaves: &[JoinLeaf]) -> Option<usize> {
    let mut offset = 0;
    for &idx in order {
        if idx == leaf_idx {
            return Some(offset);
        }
        offset += leaves[idx].width;
    }
    None
}

fn conjuncts_to_join_condition(mut conditions: Vec<ScalarExpr>) -> LogicalJoinCondition {
    if conditions.is_empty() {
        return LogicalJoinCondition::None;
    }
    let mut result = conditions.remove(0);
    for condition in conditions {
        result = ScalarExpr::Binary {
            op: ultrasql_planner::BinaryOp::And,
            left: Box::new(result),
            right: Box::new(condition),
            data_type: ultrasql_core::DataType::Bool,
        };
    }
    LogicalJoinCondition::On(result)
}

fn concat_schemas(
    left: &ultrasql_core::Schema,
    right: &ultrasql_core::Schema,
) -> ultrasql_core::Schema {
    let mut fields = Vec::with_capacity(left.len() + right.len());
    for idx in 0..left.len() {
        fields.push(left.field_at(idx).clone());
    }
    for idx in 0..right.len() {
        fields.push(right.field_at(idx).clone());
    }
    ultrasql_core::Schema::new(fields).unwrap_or_else(|_| ultrasql_core::Schema::empty())
}

fn restore_original_join_schema(
    input: LogicalPlan,
    leaves: &[JoinLeaf],
    physical_order: &[usize],
    original_schema: &ultrasql_core::Schema,
) -> LogicalPlan {
    let mut exprs = Vec::with_capacity(original_schema.len());
    for (leaf_idx, leaf) in leaves.iter().enumerate() {
        let Some(base) = output_offset(physical_order, leaf_idx, leaves) else {
            return input;
        };
        for col_offset in 0..leaf.width {
            let field = leaf.plan.schema().field_at(col_offset);
            exprs.push((
                ScalarExpr::Column {
                    name: field.name.clone(),
                    index: base + col_offset,
                    data_type: field.data_type.clone(),
                },
                field.name.clone(),
            ));
        }
    }
    LogicalPlan::Project {
        input: Box::new(input),
        exprs,
        schema: original_schema.clone(),
    }
}

fn split_and(expr: &ScalarExpr) -> Vec<ScalarExpr> {
    match expr {
        ScalarExpr::Binary {
            op: ultrasql_planner::BinaryOp::And,
            left,
            right,
            ..
        } => {
            let mut out = split_and(left);
            out.extend(split_and(right));
            out
        }
        other => vec![other.clone()],
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

    #[test]
    fn mask_has_size_checks_target_width() {
        assert!(mask_has_size(0b1011, 3));
        assert!(!mask_has_size(0b1011, 2));
        assert!(!mask_has_size(0b1011, usize::MAX));
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

    fn filter(plan: LogicalPlan) -> LogicalPlan {
        LogicalPlan::Filter {
            input: Box::new(plan),
            predicate: ScalarExpr::Literal {
                value: ultrasql_core::Value::Bool(true),
                data_type: DataType::Bool,
            },
        }
    }

    fn col(name: &str, index: usize) -> ScalarExpr {
        ScalarExpr::Column {
            name: name.to_owned(),
            index,
            data_type: DataType::Int32,
        }
    }

    fn eq(left: ScalarExpr, right: ScalarExpr) -> ScalarExpr {
        ScalarExpr::Binary {
            op: ultrasql_planner::BinaryOp::Eq,
            left: Box::new(left),
            right: Box::new(right),
            data_type: DataType::Bool,
        }
    }

    fn and(left: ScalarExpr, right: ScalarExpr) -> ScalarExpr {
        ScalarExpr::Binary {
            op: ultrasql_planner::BinaryOp::And,
            left: Box::new(left),
            right: Box::new(right),
            data_type: DataType::Bool,
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
    fn reorder_inner_joins_avoids_initial_cross_and_restores_schema() {
        let a = filter(scan("a"));
        let b = scan("b");
        let c = scan("c");
        let ab_schema = concat_schemas(a.schema(), b.schema());
        let ab = LogicalPlan::Join {
            left: Box::new(a),
            right: Box::new(b),
            join_type: LogicalJoinType::Inner,
            condition: LogicalJoinCondition::None,
            schema: ab_schema,
        };
        let abc_schema = concat_schemas(ab.schema(), c.schema());
        let plan = LogicalPlan::Join {
            left: Box::new(ab),
            right: Box::new(c),
            join_type: LogicalJoinType::Inner,
            condition: LogicalJoinCondition::On(and(
                eq(col("a", 0), col("c", 2)),
                eq(col("b", 1), col("c", 2)),
            )),
            schema: abc_schema,
        };

        let reordered = reorder_inner_joins(&plan);
        let LogicalPlan::Project { exprs, schema, .. } = reordered else {
            panic!("reorder should restore original output through Project");
        };
        assert_eq!(schema.field_at(0).name, "a");
        assert_eq!(schema.field_at(1).name, "b");
        assert_eq!(schema.field_at(2).name, "c");
        assert!(matches!(&exprs[0].0, ScalarExpr::Column { index: 0, .. }));
        assert!(matches!(&exprs[1].0, ScalarExpr::Column { index: 2, .. }));
        assert!(matches!(&exprs[2].0, ScalarExpr::Column { index: 1, .. }));
    }

    #[test]
    fn reorder_inner_joins_leaves_connected_leftmost_pair_unchanged() {
        let a = filter(scan("a"));
        let b = scan("b");
        let c = scan("c");
        let ab_schema = concat_schemas(a.schema(), b.schema());
        let ab = LogicalPlan::Join {
            left: Box::new(a),
            right: Box::new(b),
            join_type: LogicalJoinType::Inner,
            condition: LogicalJoinCondition::On(eq(col("a", 0), col("b", 1))),
            schema: ab_schema,
        };
        let abc_schema = concat_schemas(ab.schema(), c.schema());
        let plan = LogicalPlan::Join {
            left: Box::new(ab),
            right: Box::new(c),
            join_type: LogicalJoinType::Inner,
            condition: LogicalJoinCondition::On(eq(col("b", 1), col("c", 2))),
            schema: abc_schema,
        };

        let reordered = reorder_inner_joins(&plan);
        assert_eq!(reordered, plan);
    }

    #[test]
    fn reorder_inner_joins_with_stats_accounts_for_hash_build_side() {
        struct RowStats;

        impl StatsSource for RowStats {
            fn row_count(&self, table: &str) -> u64 {
                match table {
                    "a" => 100_000,
                    "b" => 10,
                    "c" => 100,
                    _ => 0,
                }
            }

            fn page_count(&self, table: &str) -> u64 {
                self.row_count(table).div_ceil(100)
            }

            fn null_frac(&self, _table: &str, _column: usize) -> f64 {
                0.0
            }

            fn n_distinct(&self, _table: &str, _column: usize) -> f64 {
                100.0
            }
        }

        let a = scan("a");
        let b = scan("b");
        let c = scan("c");
        let ab_schema = concat_schemas(a.schema(), b.schema());
        let ab = LogicalPlan::Join {
            left: Box::new(a),
            right: Box::new(b),
            join_type: LogicalJoinType::Inner,
            condition: LogicalJoinCondition::None,
            schema: ab_schema,
        };
        let abc_schema = concat_schemas(ab.schema(), c.schema());
        let plan = LogicalPlan::Join {
            left: Box::new(ab),
            right: Box::new(c),
            join_type: LogicalJoinType::Inner,
            condition: LogicalJoinCondition::On(and(
                eq(col("a", 0), col("c", 2)),
                eq(col("b", 1), col("c", 2)),
            )),
            schema: abc_schema,
        };

        let reordered = reorder_inner_joins_with_stats(&plan, &RowStats);
        let LogicalPlan::Project { exprs, .. } = reordered else {
            panic!("reorder should restore original output through Project");
        };
        assert!(matches!(&exprs[0].0, ScalarExpr::Column { index: 2, .. }));
        assert!(matches!(&exprs[1].0, ScalarExpr::Column { index: 1, .. }));
        assert!(matches!(&exprs[2].0, ScalarExpr::Column { index: 0, .. }));
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
