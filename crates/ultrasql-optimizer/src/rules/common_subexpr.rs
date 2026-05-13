//! Common-subexpression elimination (CSE) rewrite rule.
//!
//! [`CommonSubExprElimination`] identifies `ScalarExpr` sub-trees that appear
//! two or more times inside a single `Filter` predicate or `Project` expression
//! list. Each qualifying sub-tree is hoisted into a `LogicalPlan::Project`
//! node injected below the current node, and subsequent references are
//! replaced with `ScalarExpr::Column { index }` lookups.
//!
//! ## Hoisting policy
//!
//! A sub-tree is hoisted only when **both** conditions hold:
//!
//! 1. **Frequency ≥ 2** — the structurally identical sub-tree appears at least
//!    twice in the collected expressions.
//! 2. **Cost ≥ 4 nodes** — the sub-tree contains at least four AST nodes
//!    (counting every `Binary`, `Unary`, `IsNull`, `Column`, `Literal`, and
//!    `Parameter` node). This prevents hoisting trivial column references and
//!    single literals, which would inflate the plan without reducing
//!    computation.
//!
//! ## Algorithm
//!
//! 1. Collect all `ScalarExpr` sub-trees from the target node's expressions.
//! 2. Count occurrences using structural equality (`PartialEq`).
//! 3. Filter to candidates with count ≥ 2 and size ≥ 4.
//! 4. For each candidate (largest first): assign a `__cse{N}` column, add it to
//!    a new `Project` wrapping the input, replace occurrences with `Column`.
//! 5. If any substitution happened, emit the rewritten node above the injected
//!    `Project`.
//!
//! ## Convergence
//!
//! The rule is monotone: each application strictly reduces the number of
//! duplicate sub-trees. It reaches a fixed point after at most one pass per
//! duplicate (bounded by the expression size).

use std::collections::HashMap;

use std::cmp::Reverse;

use ultrasql_core::{Field, Schema};
use ultrasql_planner::{LogicalPlan, ScalarExpr, SortKey};

use crate::error::OptimizeError;
use crate::rules::RewriteRule;

/// Common-subexpression elimination rule.
///
/// Hoists duplicate `ScalarExpr` sub-trees (size ≥ 4, frequency ≥ 2) into
/// a synthetic `Project` node injected below the current `Filter` or `Project`
/// node.
#[derive(Debug)]
pub struct CommonSubExprElimination;

impl RewriteRule for CommonSubExprElimination {
    fn name(&self) -> &'static str {
        "common_subexpr_elimination"
    }

    fn apply(&self, plan: &LogicalPlan) -> Result<Option<LogicalPlan>, OptimizeError> {
        eliminate(plan)
    }
}

// ============================================================================
// Minimum cost threshold (node count)
// ============================================================================

const MIN_TREE_SIZE: usize = 4;

// ============================================================================
// Entry point
// ============================================================================

fn eliminate(plan: &LogicalPlan) -> Result<Option<LogicalPlan>, OptimizeError> {
    match plan {
        // ---------------------------------------------------------------
        // Filter: collect from the predicate, hoist into a Project below.
        // ---------------------------------------------------------------
        LogicalPlan::Filter { input, predicate } => {
            let exprs = vec![predicate.clone()];
            if let Some((new_exprs, inject)) = maybe_hoist(exprs, input) {
                let new_pred = new_exprs.into_iter().next().expect("exactly one expr");
                return Ok(Some(LogicalPlan::Filter {
                    input: Box::new(LogicalPlan::Project {
                        schema: inject.schema.clone(),
                        exprs: inject.exprs,
                        input: input.clone(),
                    }),
                    predicate: new_pred,
                }));
            }
            // Recurse into children.
            let new_input = eliminate(input)?;
            Ok(new_input.map(|i| LogicalPlan::Filter {
                input: Box::new(i),
                predicate: predicate.clone(),
            }))
        }

        // ---------------------------------------------------------------
        // Project: collect from all output expressions, hoist below.
        // ---------------------------------------------------------------
        LogicalPlan::Project {
            input,
            exprs,
            schema,
        } => {
            let raw_exprs: Vec<ScalarExpr> = exprs.iter().map(|(e, _)| e.clone()).collect();
            let names: Vec<String> = exprs.iter().map(|(_, n)| n.clone()).collect();

            if let Some((new_raw, inject)) = maybe_hoist(raw_exprs, input) {
                let new_exprs: Vec<(ScalarExpr, String)> = new_raw.into_iter().zip(names).collect();
                return Ok(Some(LogicalPlan::Project {
                    schema: schema.clone(),
                    exprs: new_exprs,
                    input: Box::new(LogicalPlan::Project {
                        schema: inject.schema.clone(),
                        exprs: inject.exprs,
                        input: input.clone(),
                    }),
                }));
            }
            // Recurse.
            let new_input = eliminate(input)?;
            Ok(new_input.map(|i| LogicalPlan::Project {
                input: Box::new(i),
                exprs: exprs.clone(),
                schema: schema.clone(),
            }))
        }

        // Recurse into other structural nodes.
        LogicalPlan::Sort { input, keys } => {
            let new_input = eliminate(input)?;
            Ok(new_input.map(|i| LogicalPlan::Sort {
                input: Box::new(i),
                keys: keys.clone(),
            }))
        }

        LogicalPlan::Limit { input, n, offset } => {
            let new_input = eliminate(input)?;
            Ok(new_input.map(|i| LogicalPlan::Limit {
                input: Box::new(i),
                n: *n,
                offset: *offset,
            }))
        }

        LogicalPlan::Aggregate {
            input,
            group_by,
            aggregates,
            schema,
        } => {
            let new_input = eliminate(input)?;
            Ok(new_input.map(|i| LogicalPlan::Aggregate {
                input: Box::new(i),
                group_by: group_by.clone(),
                aggregates: aggregates.clone(),
                schema: schema.clone(),
            }))
        }

        LogicalPlan::Join {
            left,
            right,
            join_type,
            condition,
            schema,
        } => {
            let new_left = eliminate(left)?;
            let new_right = eliminate(right)?;
            if new_left.is_none() && new_right.is_none() {
                return Ok(None);
            }
            Ok(Some(LogicalPlan::Join {
                left: Box::new(new_left.unwrap_or_else(|| *left.clone())),
                right: Box::new(new_right.unwrap_or_else(|| *right.clone())),
                join_type: *join_type,
                condition: condition.clone(),
                schema: schema.clone(),
            }))
        }

        // Leaf / DML nodes: nothing to hoist.
        _ => Ok(None),
    }
}

// ============================================================================
// Hoisting infrastructure
// ============================================================================

/// Describes the injected `Project` node that carries the hoisted CSEs.
struct InjectedProject {
    /// The hoisted expressions with their synthetic names.
    exprs: Vec<(ScalarExpr, String)>,
    /// The schema of the injected Project, which is `input.schema() ++ hoisted
    /// columns`.
    schema: Schema,
}

/// Given a list of `ScalarExpr`s and the plan node they belong to, detect
/// duplicate sub-trees meeting the hoisting criteria and return the rewritten
/// expression list alongside a description of the injected Project.
///
/// Returns `None` when no hoisting is possible.
fn maybe_hoist(
    mut exprs: Vec<ScalarExpr>,
    input: &LogicalPlan,
) -> Option<(Vec<ScalarExpr>, InjectedProject)> {
    // Step 1: count all sub-tree occurrences.
    let mut freq: HashMap<ExprKey, (usize, ScalarExpr)> = HashMap::new();
    for e in &exprs {
        collect_subtrees(e, &mut freq);
    }

    // Step 2: filter to candidates.
    let mut candidates: Vec<(usize, ScalarExpr)> = freq
        .into_values()
        .filter(|(count, expr)| *count >= 2 && expr_size(expr) >= MIN_TREE_SIZE)
        .collect();

    if candidates.is_empty() {
        return None;
    }

    // Step 3: order by descending size (hoist largest first to avoid
    // re-processing sub-components).
    candidates.sort_by_key(|item| Reverse(expr_size(&item.1)));

    // Step 4: build the injected Project expression list.
    let input_schema = input.schema();
    let input_width = input_schema.len();

    let mut inject_exprs: Vec<(ScalarExpr, String)> = Vec::new();
    // Start with the input's own columns as pass-throughs.
    for i in 0..input_width {
        let f = input_schema.field_at(i);
        inject_exprs.push((
            ScalarExpr::Column {
                name: f.name.clone(),
                index: i,
                data_type: f.data_type.clone(),
            },
            f.name.clone(),
        ));
    }

    let mut hoisted_names: Vec<(ExprKey, String, usize, ultrasql_core::DataType)> = Vec::new();

    for (_, cand) in &candidates {
        let key = ExprKey::new(cand);
        let dt = cand.data_type();
        let col_idx = inject_exprs.len();
        let name = format!("__cse{col_idx}");
        inject_exprs.push((cand.clone(), name.clone()));
        hoisted_names.push((key, name, col_idx, dt));
    }

    // Step 5: rewrite original expressions — replace matching sub-trees with
    // Column references.
    for e in &mut exprs {
        for (key, name, col_idx, dt) in &hoisted_names {
            *e = substitute(e, key, *col_idx, name, dt);
        }
    }

    // Build the injected project schema: input schema columns + hoisted columns.
    let mut schema_fields: Vec<Field> = Vec::with_capacity(inject_exprs.len());
    for (e, n) in &inject_exprs {
        let dt = e.data_type();
        schema_fields.push(Field::nullable(n.as_str(), dt));
    }
    let schema = Schema::new(schema_fields).expect("cse inject schema ok");

    Some((
        exprs,
        InjectedProject {
            exprs: inject_exprs,
            schema,
        },
    ))
}

// ============================================================================
// Expression size
// ============================================================================

/// Count the total number of AST nodes in an expression tree.
fn expr_size(expr: &ScalarExpr) -> usize {
    match expr {
        ScalarExpr::Column { .. } | ScalarExpr::Literal { .. } | ScalarExpr::Parameter { .. } => 1,
        ScalarExpr::Unary { expr: inner, .. } | ScalarExpr::IsNull { expr: inner, .. } => {
            1 + expr_size(inner)
        }
        ScalarExpr::Binary { left, right, .. } => 1 + expr_size(left) + expr_size(right),
    }
}

// ============================================================================
// Structural expression key (for HashMap)
// ============================================================================

/// A cheaply-cloneable key that implements `Eq` + `Hash` by
/// delegating to `ScalarExpr`'s `PartialEq` and a deterministic hash.
///
/// We use the `Display` representation as a hash key. This is correct because
/// `ScalarExpr::Display` is deterministic and injective for structurally
/// different expressions.
#[derive(Clone, PartialEq, Eq, Hash)]
struct ExprKey(String);

impl ExprKey {
    fn new(expr: &ScalarExpr) -> Self {
        Self(format!("{expr}"))
    }
}

// ============================================================================
// Sub-tree frequency collection
// ============================================================================

/// Walk `expr` recursively and record every sub-tree in `freq`.
fn collect_subtrees(expr: &ScalarExpr, freq: &mut HashMap<ExprKey, (usize, ScalarExpr)>) {
    let key = ExprKey::new(expr);
    let entry = freq.entry(key).or_insert_with(|| (0, expr.clone()));
    entry.0 += 1;

    // Recurse into children.
    match expr {
        ScalarExpr::Binary { left, right, .. } => {
            collect_subtrees(left, freq);
            collect_subtrees(right, freq);
        }
        ScalarExpr::Unary { expr: inner, .. } | ScalarExpr::IsNull { expr: inner, .. } => {
            collect_subtrees(inner, freq);
        }
        ScalarExpr::Column { .. } | ScalarExpr::Literal { .. } | ScalarExpr::Parameter { .. } => {}
    }
}

// ============================================================================
// Substitution
// ============================================================================

/// Replace all sub-trees in `expr` that match `key` with a `Column` reference.
fn substitute(
    expr: &ScalarExpr,
    key: &ExprKey,
    col_idx: usize,
    col_name: &str,
    dt: &ultrasql_core::DataType,
) -> ScalarExpr {
    if &ExprKey::new(expr) == key {
        return ScalarExpr::Column {
            name: col_name.to_owned(),
            index: col_idx,
            data_type: dt.clone(),
        };
    }
    match expr {
        ScalarExpr::Binary {
            op,
            left,
            right,
            data_type,
        } => ScalarExpr::Binary {
            op: *op,
            left: Box::new(substitute(left, key, col_idx, col_name, dt)),
            right: Box::new(substitute(right, key, col_idx, col_name, dt)),
            data_type: data_type.clone(),
        },
        ScalarExpr::Unary {
            op,
            expr: inner,
            data_type,
        } => ScalarExpr::Unary {
            op: *op,
            expr: Box::new(substitute(inner, key, col_idx, col_name, dt)),
            data_type: data_type.clone(),
        },
        ScalarExpr::IsNull {
            expr: inner,
            negated,
        } => ScalarExpr::IsNull {
            expr: Box::new(substitute(inner, key, col_idx, col_name, dt)),
            negated: *negated,
        },
        // Leaf nodes that didn't match — return unchanged.
        ScalarExpr::Column { .. } | ScalarExpr::Literal { .. } | ScalarExpr::Parameter { .. } => {
            expr.clone()
        }
    }
}

// ============================================================================
// Sort-key helpers (used when extending to Sort nodes)
// ============================================================================

/// Extract scalar expressions from a list of `SortKey`s.
#[allow(dead_code)]
fn sort_key_exprs(keys: &[SortKey]) -> Vec<ScalarExpr> {
    keys.iter().map(|k| k.expr.clone()).collect()
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use ultrasql_core::{DataType, Field, Schema, Value};
    use ultrasql_planner::{BinaryOp, LogicalPlan, ScalarExpr, UnaryOp};

    use super::*;
    use crate::rules::RewriteRule;

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn scan(fields: Vec<Field>) -> LogicalPlan {
        LogicalPlan::Scan {
            table: "t".into(),
            schema: Schema::new(fields).expect("schema ok"),
            projection: None,
        }
    }

    fn two_col_scan() -> LogicalPlan {
        scan(vec![
            Field::required("a", DataType::Int32),
            Field::required("b", DataType::Int32),
        ])
    }

    fn col(name: &str, idx: usize) -> ScalarExpr {
        ScalarExpr::Column {
            name: name.into(),
            index: idx,
            data_type: DataType::Int32,
        }
    }

    fn lit_i32(v: i32) -> ScalarExpr {
        ScalarExpr::Literal {
            value: Value::Int32(v),
            data_type: DataType::Int32,
        }
    }

    fn add(l: ScalarExpr, r: ScalarExpr) -> ScalarExpr {
        ScalarExpr::Binary {
            op: BinaryOp::Add,
            left: Box::new(l),
            right: Box::new(r),
            data_type: DataType::Int32,
        }
    }

    fn mul(l: ScalarExpr, r: ScalarExpr) -> ScalarExpr {
        ScalarExpr::Binary {
            op: BinaryOp::Mul,
            left: Box::new(l),
            right: Box::new(r),
            data_type: DataType::Int32,
        }
    }

    fn neg(e: ScalarExpr) -> ScalarExpr {
        ScalarExpr::Unary {
            op: UnaryOp::Neg,
            expr: Box::new(e),
            data_type: DataType::Int32,
        }
    }

    /// Build a large sub-tree (≥ 4 nodes): `(a + b) * (a + b)`.
    fn deep_dup() -> ScalarExpr {
        // (a + b) * (a + b) — (a+b) appears twice, size of (a+b) = 3 nodes.
        // (a + b) * (a + b) root = 7 nodes total; each (a+b) = 3 nodes.
        // But 3 < 4 so (a+b) alone would NOT be hoisted.
        // We need a >= 4 node sub-tree. Use neg((a + b)) = 4 nodes.
        let inner = neg(add(col("a", 0), col("b", 1))); // 4 nodes
        mul(inner.clone(), inner)
    }

    // -----------------------------------------------------------------------
    // Rule name stability
    // -----------------------------------------------------------------------

    #[test]
    fn rule_name_is_stable() {
        assert_eq!(
            CommonSubExprElimination.name(),
            "common_subexpr_elimination"
        );
    }

    // -----------------------------------------------------------------------
    // No-op on plain scan / trivial predicates
    // -----------------------------------------------------------------------

    #[test]
    fn no_op_on_plain_scan() {
        let plan = two_col_scan();
        let result = CommonSubExprElimination.apply(&plan).expect("no error");
        assert!(result.is_none(), "plain Scan should not be rewritten");
    }

    #[test]
    fn no_op_on_filter_with_unique_predicate() {
        // Filter(Scan, a + b = 5) — a + b appears only once.
        let predicate = ScalarExpr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(add(col("a", 0), col("b", 1))),
            right: Box::new(lit_i32(5)),
            data_type: DataType::Bool,
        };
        let plan = LogicalPlan::Filter {
            input: Box::new(two_col_scan()),
            predicate,
        };
        let result = CommonSubExprElimination.apply(&plan).expect("no error");
        assert!(
            result.is_none(),
            "filter with no duplicates should not be rewritten"
        );
    }

    // -----------------------------------------------------------------------
    // Trivial duplicates (size < 4) are NOT hoisted
    // -----------------------------------------------------------------------

    #[test]
    fn trivial_column_references_not_hoisted() {
        // Filter(Scan, a = a) — `a` appears twice but size = 1 < 4.
        let predicate = ScalarExpr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(col("a", 0)),
            right: Box::new(col("a", 0)),
            data_type: DataType::Bool,
        };
        let plan = LogicalPlan::Filter {
            input: Box::new(two_col_scan()),
            predicate,
        };
        let result = CommonSubExprElimination.apply(&plan).expect("no error");
        assert!(
            result.is_none(),
            "trivial column reference should not be hoisted"
        );
    }

    #[test]
    fn three_node_subtree_not_hoisted() {
        // (a + b) appears twice but has 3 nodes — below MIN_TREE_SIZE.
        let inner = add(col("a", 0), col("b", 1)); // 3 nodes
        let predicate = ScalarExpr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(inner.clone()),
            right: Box::new(inner),
            data_type: DataType::Bool,
        };
        let plan = LogicalPlan::Filter {
            input: Box::new(two_col_scan()),
            predicate,
        };
        let result = CommonSubExprElimination.apply(&plan).expect("no error");
        assert!(
            result.is_none(),
            "3-node sub-tree should not be hoisted (< MIN_TREE_SIZE=4)"
        );
    }

    // -----------------------------------------------------------------------
    // Deep duplicate IS hoisted
    // -----------------------------------------------------------------------

    #[test]
    fn deep_duplicate_is_hoisted_into_project_below_filter() {
        // Filter(Scan, neg(a+b) * neg(a+b) = 0)
        // neg(a+b) has 4 nodes and appears twice.
        let dup = deep_dup(); // neg(a+b) * neg(a+b) — neg(a+b) appears 2×
        let predicate = ScalarExpr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(dup),
            right: Box::new(lit_i32(0)),
            data_type: DataType::Bool,
        };
        let plan = LogicalPlan::Filter {
            input: Box::new(two_col_scan()),
            predicate,
        };

        let result = CommonSubExprElimination.apply(&plan).expect("no error");

        assert!(result.is_some(), "deep duplicate should be hoisted");

        let result = result.unwrap();

        // Top node is still a Filter.
        assert!(
            matches!(result, LogicalPlan::Filter { .. }),
            "top node should remain Filter"
        );

        // Below the Filter there should be an injected Project.
        if let LogicalPlan::Filter { input, .. } = &result {
            assert!(
                matches!(input.as_ref(), LogicalPlan::Project { .. }),
                "Filter input should be an injected Project; got {input:?}"
            );
        }
    }

    // -----------------------------------------------------------------------
    // Project node: duplicate expressions in output list
    // -----------------------------------------------------------------------

    #[test]
    fn deep_duplicate_in_project_exprs_is_hoisted() {
        let input_schema = Schema::new(vec![
            Field::required("a", DataType::Int32),
            Field::required("b", DataType::Int32),
        ])
        .expect("schema ok");

        let shared = neg(add(col("a", 0), col("b", 1))); // 4 nodes
        let out_schema = Schema::new(vec![
            Field::nullable("x", DataType::Int32),
            Field::nullable("y", DataType::Int32),
        ])
        .expect("schema ok");

        let plan = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Scan {
                table: "t".into(),
                schema: input_schema,
                projection: None,
            }),
            exprs: vec![(shared.clone(), "x".into()), (shared, "y".into())],
            schema: out_schema,
        };

        let result = CommonSubExprElimination.apply(&plan).expect("no error");
        assert!(
            result.is_some(),
            "project with duplicate exprs should be rewritten"
        );

        if let Some(LogicalPlan::Project { input, .. }) = result {
            assert!(
                matches!(input.as_ref(), LogicalPlan::Project { .. }),
                "project should have an injected project below it"
            );
        }
    }

    // -----------------------------------------------------------------------
    // Idempotence: second application finds no more duplicates
    // -----------------------------------------------------------------------

    #[test]
    fn cse_reaches_fixed_point_on_second_application() {
        let dup = deep_dup();
        let predicate = ScalarExpr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(dup),
            right: Box::new(lit_i32(0)),
            data_type: DataType::Bool,
        };
        let plan = LogicalPlan::Filter {
            input: Box::new(two_col_scan()),
            predicate,
        };

        let rule = CommonSubExprElimination;
        let once = rule
            .apply(&plan)
            .expect("no error")
            .expect("first pass fires");
        // Second pass: the predicate now uses Column references; the
        // duplicated sub-tree no longer appears.
        let twice = rule.apply(&once).expect("no error");
        assert!(
            twice.is_none(),
            "second pass should be a no-op (fixed point reached); got {twice:?}"
        );
    }
}
