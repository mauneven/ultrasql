//! Cascades-style memo table.
//!
//! The memo stores equivalence classes of physical plan expressions. Each
//! *group* represents a set of logically equivalent plans; each *group
//! expression* is one physical implementation of that logical plan. The
//! memo deduplicates equivalent expressions via a hash index so the search
//! driver never explores the same plan twice.
//!
//! ## Current state (v0.6)
//!
//! This module ships the data structures (`Memo`, `Group`, `GroupExpr`,
//! `PhysicalOp`) without the search driver. Tests assert constructor,
//! `intern`, and lookup semantics. The top-down Cascades search driver that
//! populates and queries the memo lands in v0.7.
//!
//! ## Design
//!
//! - A `Group` has a stable integer ID. Groups are never deleted.
//! - A `GroupExpr` references child groups by ID, making the memo a DAG.
//! - `Memo::intern` deduplicates by hashing `(op, children)`; if an
//!   equivalent expression already exists in the memo the existing ID is
//!   returned.
//! - The `best_expr` field on each group is `None` until the search driver
//!   runs a costing pass (v0.7).

use std::collections::HashMap;

// ============================================================================
// PhysicalOp
// ============================================================================

/// The physical operator variant stored in a [`GroupExpr`].
///
/// Each variant corresponds to one physical execution strategy. The list
/// will grow as more operators are implemented; it is `#[non_exhaustive]`
/// so that adding new variants is not a breaking change.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum PhysicalOp {
    /// Full sequential heap scan.
    SeqScan,
    /// B-tree or hash index scan.
    IndexScan,
    /// Nested-loop join (outer loop over left, inner loop over right).
    NestLoop,
    /// Hash join (right side is the build side).
    HashJoin,
    /// Sort-merge join (both sides arrive sorted on the join key).
    MergeJoin,
    /// Hash aggregate (group keys hashed into a hash table).
    HashAggregate,
    /// Sort aggregate (input pre-sorted on group keys).
    SortAggregate,
    /// External sort.
    Sort,
    /// Row-level filter (WHERE clause evaluation).
    Filter,
    /// Column projection.
    Project,
    /// Bitmap index scan: evaluates a predicate against an index and
    /// returns a `TidBitmap`.  Multiple `BitmapIndexScan`s can be AND/OR
    /// merged before the heap fetch.
    BitmapIndexScan,
    /// Bitmap heap scan: fetches rows from the heap for every TID set in a
    /// `TidBitmap`.  Preferred when selectivity is in the range [0.5%, 10%]
    /// or when two or more indexes apply to the same table.
    BitmapHeapScan,
    /// Index-only scan: reads index entries directly without a heap fetch
    /// when the visibility map indicates all-visible pages.
    IndexOnlyScan,
}

// ============================================================================
// GroupExpr
// ============================================================================

/// One physical implementation of a logical equivalence class.
///
/// A `GroupExpr` records which physical operator is applied and which
/// child groups (by group ID) supply its inputs. The number of children
/// depends on the operator arity:
/// - Unary (`SeqScan`, `IndexScan`, `Sort`, `Filter`, `Project`, `HashAggregate`, `SortAggregate`): 1 child.
/// - Binary (`NestLoop`, `HashJoin`, `MergeJoin`): 2 children.
/// - Leaf (`SeqScan` without an input group): 0 children.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct GroupExpr {
    /// The physical operator for this expression.
    pub op: PhysicalOp,
    /// Group IDs of child groups, in operator-specific order.
    pub children: Vec<usize>,
}

// ============================================================================
// Group
// ============================================================================

/// An equivalence class of physical plans.
///
/// A group holds all known physical implementations of one logical sub-plan.
/// The search driver (v0.7) will fill `best_expr` after costing all members.
#[derive(Clone, Debug)]
pub struct Group {
    /// Stable group identifier. Assigned by [`Memo`] at creation time.
    pub id: usize,
    /// All known physical expressions in this equivalence class.
    pub exprs: Vec<GroupExpr>,
    /// Index of the cheapest expression (per the cost model) in `exprs`.
    /// `None` until the search driver runs a costing pass.
    pub best_expr: Option<usize>,
}

// ============================================================================
// GroupKey
// ============================================================================

/// A hashable key that identifies a unique `(op, children)` pair.
///
/// Used by [`Memo`] to deduplicate group expressions on `intern`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct GroupKey {
    op: PhysicalOp,
    children: Vec<usize>,
}

impl From<&GroupExpr> for GroupKey {
    fn from(expr: &GroupExpr) -> Self {
        Self {
            op: expr.op.clone(),
            children: expr.children.clone(),
        }
    }
}

// ============================================================================
// Memo
// ============================================================================

/// Cascades-style memo table.
///
/// Stores equivalence groups and deduplicates physical expressions by their
/// `(operator, children)` key. All group IDs are stable for the lifetime of
/// the memo.
///
/// ## Usage
///
/// ```rust
/// use ultrasql_optimizer::enumeration::{Memo, GroupExpr, PhysicalOp};
///
/// let mut memo = Memo::new();
/// let scan = GroupExpr { op: PhysicalOp::SeqScan, children: vec![] };
/// let id1 = memo.intern(scan.clone());
/// let id2 = memo.intern(scan);
/// assert_eq!(id1, id2, "identical expressions share a group");
/// ```
#[derive(Debug)]
pub struct Memo {
    groups: Vec<Group>,
    /// Maps `GroupKey -> group ID`.
    group_index: HashMap<GroupKey, usize>,
}

impl Memo {
    /// Create an empty memo table.
    #[must_use]
    pub fn new() -> Self {
        Self {
            groups: Vec::new(),
            group_index: HashMap::new(),
        }
    }

    /// Insert `expr` into the memo and return the group ID.
    ///
    /// If an equivalent expression (same op and children) already exists,
    /// the existing group ID is returned and `expr` is added to that
    /// group's expression list only if it is not already present.
    pub fn intern(&mut self, expr: GroupExpr) -> usize {
        let key = GroupKey::from(&expr);
        if let Some(&existing_id) = self.group_index.get(&key) {
            // Already exists; add the expression if not already a member.
            let group = &mut self.groups[existing_id];
            if !group.exprs.contains(&expr) {
                group.exprs.push(expr);
            }
            return existing_id;
        }

        // New group.
        let id = self.groups.len();
        self.groups.push(Group {
            id,
            exprs: vec![expr],
            best_expr: None,
        });
        self.group_index.insert(key, id);
        id
    }

    /// Look up a group by its ID.
    ///
    /// # Panics
    ///
    /// Panics if `id` is out of range (i.e. was not returned by
    /// [`intern`]).
    ///
    /// [`intern`]: Memo::intern
    #[must_use]
    pub fn group(&self, id: usize) -> &Group {
        &self.groups[id]
    }

    /// Return the total number of groups in the memo.
    #[must_use]
    pub fn len(&self) -> usize {
        self.groups.len()
    }

    /// Return `true` if the memo contains no groups.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.groups.is_empty()
    }
}

impl Default for Memo {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn scan_expr() -> GroupExpr {
        GroupExpr {
            op: PhysicalOp::SeqScan,
            children: vec![],
        }
    }

    fn hash_join_expr(left: usize, right: usize) -> GroupExpr {
        GroupExpr {
            op: PhysicalOp::HashJoin,
            children: vec![left, right],
        }
    }

    /// Interning the same expression twice returns the same stable group ID.
    #[test]
    fn memo_intern_returns_stable_id() {
        let mut memo = Memo::new();
        let id1 = memo.intern(scan_expr());
        let id2 = memo.intern(scan_expr());
        assert_eq!(id1, id2, "identical expressions must share a group ID");
    }

    /// Two distinct expressions occupy different groups.
    #[test]
    fn memo_distinct_exprs_get_distinct_ids() {
        let mut memo = Memo::new();
        let scan_id = memo.intern(scan_expr());
        let idx_id = memo.intern(GroupExpr {
            op: PhysicalOp::IndexScan,
            children: vec![],
        });
        assert_ne!(scan_id, idx_id, "distinct ops must get distinct group IDs");
    }

    /// A join expression that references scan groups produces the right
    /// group structure.
    #[test]
    fn memo_join_references_child_groups() {
        let mut memo = Memo::new();
        let left_id = memo.intern(scan_expr());
        let right_id = memo.intern(GroupExpr {
            op: PhysicalOp::IndexScan,
            children: vec![],
        });
        let join_id = memo.intern(hash_join_expr(left_id, right_id));

        let join_group = memo.group(join_id);
        assert_eq!(join_group.exprs.len(), 1);
        assert_eq!(join_group.exprs[0].children, vec![left_id, right_id]);
    }

    /// A new memo is empty.
    #[test]
    fn memo_new_is_empty() {
        let memo = Memo::new();
        assert!(memo.is_empty());
        assert_eq!(memo.len(), 0);
    }

    /// `best_expr` is `None` before the search driver runs.
    #[test]
    fn memo_best_expr_initially_none() {
        let mut memo = Memo::new();
        let id = memo.intern(scan_expr());
        assert!(memo.group(id).best_expr.is_none());
    }

    /// After interning N distinct expressions, `len()` equals N.
    #[test]
    fn memo_len_matches_distinct_intern_count() {
        let mut memo = Memo::new();
        for i in 0..5_usize {
            memo.intern(GroupExpr {
                op: PhysicalOp::SeqScan,
                children: vec![i], // distinct children -> distinct keys
            });
        }
        assert_eq!(memo.len(), 5);
    }
}
