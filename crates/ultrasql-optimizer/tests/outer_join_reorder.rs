//! Integration tests for §3.4 of the v0.6 workplan — *Join reordering
//! with outer-join constraints*.
//!
//! These tests pin the contract of
//! [`ultrasql_optimizer::reorder_inner_joins`]:
//!
//! 1. **Inner-only chains may be reordered.** Three inner joins over A,
//!    B, C are eligible for full enumeration.
//! 2. **`LEFT OUTER JOIN` with a non-strict predicate is a reorder
//!    barrier.** When the outermost join is a LEFT JOIN whose predicate
//!    is non-strict (the test uses `t.x IS NULL`, a paradigmatic
//!    non-strict expression — PostgreSQL also lists `COALESCE(...)` here,
//!    but the v0.6 `ScalarExpr` enum does not yet model `COALESCE`, so
//!    `IS NULL` stands in for it as a known non-strict predicate), the
//!    optimizer must **not** reorder any join in the chain.
//! 3. **`FULL OUTER JOIN` is a hard barrier in every direction.** No
//!    sibling subtree may move past it.
//!
//! ## What `reorder_inner_joins` is allowed to do
//!
//! The driver is allowed to rewrite an inner-join chain in any
//! semantically equivalent order. With the cost model wired to
//! [`ultrasql_optimizer::NoStats`] every scan costs zero, so the choice
//! is made by deterministic tie-breaking in
//! [`ultrasql_optimizer::JoinEnumerator::enumerate`]. The test asserts
//! the *direction* of the change — "the leftmost leaf is no longer the
//! input's leftmost" — rather than a particular permutation, so the
//! contract survives future changes to the tie-break rule.

use ultrasql_core::{DataType, Field, Schema};
use ultrasql_optimizer::{outer_join_subtree_is_barrier, reorder_inner_joins};
use ultrasql_planner::{BinaryOp, LogicalJoinCondition, LogicalJoinType, LogicalPlan, ScalarExpr};

// =============================================================================
// Plan-construction helpers
// =============================================================================

/// A `Scan(table)` whose schema carries one Int32 column named after the
/// table (so concatenating two scan schemas never trips on duplicate
/// field names).
fn scan(table: &str) -> LogicalPlan {
    LogicalPlan::Scan {
        table: table.into(),
        schema: Schema::new([Field::required(table, DataType::Int32)]).expect("schema ok"),
        projection: None,
    }
}

/// Concatenate two schemas left-to-right.
fn concat(left: &Schema, right: &Schema) -> Schema {
    let mut fields: Vec<Field> = Vec::with_capacity(left.len() + right.len());
    for i in 0..left.len() {
        fields.push(left.field_at(i).clone());
    }
    for i in 0..right.len() {
        fields.push(right.field_at(i).clone());
    }
    Schema::new(fields).expect("schema ok")
}

/// Build an inner join with no predicate (`CROSS`-style for setup
/// purposes; the enumerator does not care about the exact predicate
/// shape, only its identity).
fn inner_join(left: LogicalPlan, right: LogicalPlan) -> LogicalPlan {
    let schema = concat(left.schema(), right.schema());
    LogicalPlan::Join {
        left: Box::new(left),
        right: Box::new(right),
        join_type: LogicalJoinType::Inner,
        condition: LogicalJoinCondition::None,
        schema,
    }
}

/// Build a `LEFT OUTER JOIN` with the supplied `ON` predicate.
fn left_outer_join(left: LogicalPlan, right: LogicalPlan, predicate: ScalarExpr) -> LogicalPlan {
    let schema = concat(left.schema(), right.schema());
    LogicalPlan::Join {
        left: Box::new(left),
        right: Box::new(right),
        join_type: LogicalJoinType::LeftOuter,
        condition: LogicalJoinCondition::On(predicate),
        schema,
    }
}

/// Build a `FULL OUTER JOIN` with the supplied `ON` predicate.
fn full_outer_join(left: LogicalPlan, right: LogicalPlan, predicate: ScalarExpr) -> LogicalPlan {
    let schema = concat(left.schema(), right.schema());
    LogicalPlan::Join {
        left: Box::new(left),
        right: Box::new(right),
        join_type: LogicalJoinType::FullOuter,
        condition: LogicalJoinCondition::On(predicate),
        schema,
    }
}

/// `Column { name, index, Int32 }`. `index` is irrelevant to the
/// extractor — the reorder driver only inspects join *types*, not column
/// indices — but a real predicate needs *some* index, so we keep one for
/// EXPLAIN readability.
fn col(name: &str, index: usize) -> ScalarExpr {
    ScalarExpr::Column {
        name: name.into(),
        index,
        data_type: DataType::Int32,
    }
}

/// Build a non-strict predicate of the form `col IS NULL`. PostgreSQL
/// classifies `IS NULL` as non-strict because it returns `TRUE` for a
/// NULL input, exactly the case where strict semantics would have
/// returned NULL.
fn is_null(expr: ScalarExpr) -> ScalarExpr {
    ScalarExpr::IsNull {
        expr: Box::new(expr),
        negated: false,
    }
}

/// Walk left-most spine of a join tree and return the leftmost
/// `LogicalPlan::Scan { table }` it can find. Used to detect whether the
/// optimizer rotated the spine.
fn leftmost_scan_table(plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::Scan { table, .. } => Some(table.clone()),
        LogicalPlan::Join { left, .. } => leftmost_scan_table(left),
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Project { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Aggregate { input, .. } => leftmost_scan_table(input),
        _ => None,
    }
}

/// Return the join type at the root of `plan`, if any.
fn root_join_type(plan: &LogicalPlan) -> Option<LogicalJoinType> {
    match plan {
        LogicalPlan::Join { join_type, .. } => Some(*join_type),
        _ => None,
    }
}

// =============================================================================
// Tests
// =============================================================================

/// Three inner joins are fully reorderable.
///
/// Input: `Inner(Inner(Scan(A), Scan(B)), Scan(C))` — leftmost leaf is A.
/// After [`reorder_inner_joins`] the leftmost leaf must change, proving
/// the join order was actually rewritten (i.e. the enumerator ran and
/// produced a different tree).
#[test]
fn three_inner_joins_are_reordered() {
    let input = inner_join(inner_join(scan("a"), scan("b")), scan("c"));
    assert_eq!(
        leftmost_scan_table(&input).as_deref(),
        Some("a"),
        "sanity: input's leftmost leaf is `a`"
    );

    let result = reorder_inner_joins(&input);
    let reordered_body = match &result {
        LogicalPlan::Project { input, .. } => input.as_ref(),
        other => other,
    };

    // The output must still be a left-deep inner-join tree of three
    // relations…
    let LogicalPlan::Join {
        left: outer_left,
        right: _,
        join_type,
        ..
    } = reordered_body
    else {
        panic!("expected a Join at the root, got: {result:?}");
    };
    assert_eq!(*join_type, LogicalJoinType::Inner, "root must remain inner");
    assert!(
        matches!(outer_left.as_ref(), LogicalPlan::Join { .. }),
        "root.left should be another Join (left-deep), got: {outer_left:?}"
    );

    // …but the *order* of leaves must have changed: the leftmost leaf
    // of the produced tree is no longer the input's leftmost leaf
    // (`a`).
    let new_leftmost = leftmost_scan_table(reordered_body).expect("leftmost scan present");
    assert_ne!(
        new_leftmost, "a",
        "the optimizer must have reordered the chain — leftmost leaf is still `a`: {result:?}"
    );
}

/// `LEFT OUTER JOIN` with a non-strict predicate is **not** reordered.
///
/// Input: `LeftOuter(Inner(A, B), C, ON b IS NULL)`.
///
/// The predicate `IS NULL` is non-strict, so PostgreSQL forbids
/// reordering the outer join with its surrounding inner-join chain. The
/// brief instructs us to take the safe option ("skip enumeration
/// entirely for that subtree"), so the output must be **structurally
/// identical** to the input — both the outer-join layer and the inner
/// `(A, B)` chain underneath are preserved verbatim.
#[test]
fn left_outer_join_with_non_strict_predicate_blocks_reorder() {
    // Predicate `b IS NULL` — column index `1` is the `b` column inside
    // the concatenated `(a, b, c)` schema. Index value is informational;
    // the reorder driver does not inspect it.
    let predicate = is_null(col("b", 1));

    let inner_ab = inner_join(scan("a"), scan("b"));
    let input = left_outer_join(inner_ab.clone(), scan("c"), predicate);

    let result = reorder_inner_joins(&input);

    // The tree must come back unchanged — every node, every join type,
    // every child order.
    assert_eq!(
        result, input,
        "LEFT OUTER with non-strict predicate must be left untouched. got: {result:?}"
    );

    // Spot-check the structural invariants the brief calls out
    // explicitly.
    assert_eq!(
        root_join_type(&result),
        Some(LogicalJoinType::LeftOuter),
        "root must remain LEFT OUTER"
    );
    if let LogicalPlan::Join { left, right, .. } = &result {
        // The LEFT side of the LEFT OUTER must remain the original
        // inner-join chain, untouched.
        assert_eq!(
            left.as_ref(),
            &inner_ab,
            "LEFT side of the LEFT OUTER must be left unchanged"
        );
        // The RIGHT side must still be `Scan(c)`.
        assert!(
            matches!(right.as_ref(), LogicalPlan::Scan { table, .. } if table == "c"),
            "RIGHT side of the LEFT OUTER must remain Scan(c), got: {right:?}"
        );
    } else {
        panic!("expected a Join at the root, got: {result:?}");
    }
}

/// A `LEFT OUTER JOIN` that sits *inside* an inner-join chain still
/// blocks reordering across the barrier.
///
/// Input: `Inner(LeftOuter(A, B, b IS NULL), C)`. The inner join sees
/// the LEFT-OUTER subtree as one of its operands; with our conservative
/// "outer joins are opaque" policy, that inner join must not be
/// reordered either.
#[test]
fn inner_join_above_left_outer_is_not_reordered() {
    let predicate = is_null(col("b", 1));
    let outer = left_outer_join(scan("a"), scan("b"), predicate);
    let input = inner_join(outer.clone(), scan("c"));

    let result = reorder_inner_joins(&input);

    assert_eq!(
        result, input,
        "an inner-join chain that contains an outer-join leaf must not be reordered"
    );
}

/// `FULL OUTER JOIN` at any level preserves the outermost shape.
///
/// Two flavours are checked:
///
/// - **Top-level FULL OUTER**: `FullOuter(Inner(A, B), C)`. The whole
///   subtree must come back identical — no rotation, no swap.
/// - **Buried FULL OUTER**: `Inner(FullOuter(A, B), C)`. The inner join
///   *above* the full outer still cannot reorder across the barrier.
#[test]
fn full_outer_join_is_a_hard_barrier_everywhere() {
    let predicate = ScalarExpr::Binary {
        op: BinaryOp::Eq,
        left: Box::new(col("a", 0)),
        right: Box::new(col("b", 1)),
        data_type: DataType::Bool,
    };

    // ── Top-level FULL OUTER ──────────────────────────────────────────
    let top_full = full_outer_join(
        inner_join(scan("a"), scan("b")),
        scan("c"),
        predicate.clone(),
    );
    let result = reorder_inner_joins(&top_full);
    assert_eq!(
        result, top_full,
        "FULL OUTER at the root must be returned verbatim, got: {result:?}"
    );
    assert_eq!(
        root_join_type(&result),
        Some(LogicalJoinType::FullOuter),
        "root must remain FULL OUTER"
    );

    // ── Buried FULL OUTER ─────────────────────────────────────────────
    let buried = inner_join(full_outer_join(scan("a"), scan("b"), predicate), scan("c"));
    let result = reorder_inner_joins(&buried);
    assert_eq!(
        result, buried,
        "an inner join above a FULL OUTER must not be reordered, got: {result:?}"
    );
}

/// A tree with no joins at all is a fixed point of `reorder_inner_joins`.
///
/// Guards against accidental rewrites on plans where the function has
/// nothing legitimate to do.
#[test]
fn plain_scan_is_a_fixed_point() {
    let plan = scan("t");
    let result = reorder_inner_joins(&plan);
    assert_eq!(result, plan, "Scan is a fixed point");
}

/// The public `outer_join_subtree_is_barrier` helper agrees with the
/// reorder driver's behaviour on the inputs we built above. This locks
/// the helper into the public API surface so other optimiser passes
/// (e.g. a future predicate-pushdown rule that wants to know whether
/// it's about to cross a barrier) can rely on the same answer.
#[test]
fn outer_join_subtree_is_barrier_agrees_with_reorder() {
    // Inner-only chains are *not* barriers.
    let inner_chain = inner_join(inner_join(scan("a"), scan("b")), scan("c"));
    assert!(!outer_join_subtree_is_barrier(&inner_chain));

    // LEFT OUTER at the root → barrier.
    let left = left_outer_join(
        inner_join(scan("a"), scan("b")),
        scan("c"),
        is_null(col("b", 1)),
    );
    assert!(outer_join_subtree_is_barrier(&left));

    // FULL OUTER buried inside an inner-join spine → still a barrier.
    let eq = ScalarExpr::Binary {
        op: BinaryOp::Eq,
        left: Box::new(col("a", 0)),
        right: Box::new(col("b", 1)),
        data_type: DataType::Bool,
    };
    let buried_full = inner_join(full_outer_join(scan("a"), scan("b"), eq), scan("c"));
    assert!(outer_join_subtree_is_barrier(&buried_full));
}
