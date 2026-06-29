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

#![allow(clippy::match_same_arms)]

use std::collections::HashMap;

use std::cmp::Reverse;

use ultrasql_core::{Field, Schema};
use ultrasql_planner::{BinaryOp, LogicalPlan, ScalarExpr, UnaryOp};

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
const MAX_CSE_TOTAL_NODES: usize = 128;

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
            if let Some((new_exprs, inject)) = maybe_hoist(exprs, input)? {
                let new_pred =
                    new_exprs
                        .into_iter()
                        .next()
                        .ok_or_else(|| OptimizeError::RuleFailed {
                            rule: "common_subexpr_elimination",
                            detail: "predicate rewrite produced no expression".to_owned(),
                        })?;
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

            if let Some((new_raw, inject)) = maybe_hoist(raw_exprs, input)? {
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
) -> Result<Option<(Vec<ScalarExpr>, InjectedProject)>, OptimizeError> {
    let total_nodes = exprs.iter().map(expr_size).sum::<usize>();
    if total_nodes > MAX_CSE_TOTAL_NODES {
        return Ok(None);
    }

    // Step 1: count all sub-tree occurrences.
    let mut freq: HashMap<ExprKey, (usize, ScalarExpr)> = HashMap::new();
    for e in &exprs {
        collect_subtrees(e, &mut freq);
    }

    // Step 2: filter to candidates. Volatile sub-trees (those containing
    // `random()`, UUID generators, etc.) must never be hoisted: collapsing two
    // textually-identical volatile expressions into a single shared evaluation
    // changes results (e.g. two independent `random()` calls must be free to
    // differ). Opaque subquery/outer-column leaves are likewise excluded.
    //
    // Additionally, a *fallible* sub-tree (`can_raise()`) must not be hoisted
    // when any of its occurrences sits in a short-circuit-guarded position
    // (right operand of `AND`/`OR`): the injected `Project` evaluates it for
    // every row unconditionally, so a row the short-circuit would have skipped
    // could now raise a runtime error, turning an empty result into a failure.
    // Pure (total) sub-trees, and fallible ones that are always evaluated
    // anyway, remain eligible.
    let mut candidates: Vec<(usize, ScalarExpr)> = freq
        .into_values()
        .filter(|(count, expr)| {
            *count >= 2
                && expr_size(expr) >= MIN_TREE_SIZE
                && !contains_volatile(expr)
                && !(can_raise(expr) && fallible_guarded_in(&exprs, expr))
        })
        .collect();

    if candidates.is_empty() {
        return Ok(None);
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
    let schema = Schema::new(schema_fields).map_err(|err| OptimizeError::RuleFailed {
        rule: "common_subexpr_elimination",
        detail: format!("CSE injection schema: {err}"),
    })?;

    Ok(Some((
        exprs,
        InjectedProject {
            exprs: inject_exprs,
            schema,
        },
    )))
}

// ============================================================================
// Volatility
// ============================================================================

/// Returns `true` for builtin functions whose value may differ between two
/// evaluations within the same statement (non-deterministic / volatile).
/// Stable-within-statement functions such as `now()`/`current_timestamp` are
/// deliberately excluded — collapsing those is value-preserving.
fn is_volatile_fn(name: &str) -> bool {
    matches!(
        name,
        "random"
            | "random_normal"
            | "gen_random_uuid"
            | "uuid_generate_v4"
            | "gen_random_bytes"
            | "clock_timestamp"
            | "nextval"
            | "setseed"
    )
}

/// Returns `true` if `expr` contains any volatile function call, or any opaque
/// subquery / outer-column leaf. Such expressions are never CSE candidates: a
/// shared `Project` would force two independent evaluations to collapse into
/// one, which is only sound for pure (deterministic) expressions.
fn contains_volatile(expr: &ScalarExpr) -> bool {
    match expr {
        ScalarExpr::Column { .. } | ScalarExpr::Literal { .. } | ScalarExpr::Parameter { .. } => {
            false
        }
        ScalarExpr::Unary { expr: inner, .. } | ScalarExpr::IsNull { expr: inner, .. } => {
            contains_volatile(inner)
        }
        ScalarExpr::Binary { left, right, .. } => {
            contains_volatile(left) || contains_volatile(right)
        }
        ScalarExpr::FunctionCall { name, args, .. } => {
            is_volatile_fn(name) || args.iter().any(contains_volatile)
        }
        // Conservatively treat subquery leaves as non-hoistable.
        ScalarExpr::OuterColumn { .. }
        | ScalarExpr::ScalarSubquery { .. }
        | ScalarExpr::Exists { .. }
        | ScalarExpr::InSubquery { .. } => true,
    }
}

// ============================================================================
// Fallibility (can_raise) + short-circuit guarding
// ============================================================================

/// Returns `true` if evaluating `expr` on some row can raise a per-row runtime
/// error (division/modulo by zero, checked-arithmetic overflow, `Pow`, casts,
/// array subscript out of bounds, bit shifts, or any non-total builtin).
///
/// CSE hoists a shared sub-tree into a `Project` that is evaluated for **every**
/// input row, unconditionally. If the sub-tree's original position was guarded
/// by a short-circuiting `AND`/`OR` (Kleene three-valued, see the executor's
/// `eval_and`/`eval_or`), hoisting evaluates it for rows where the short-circuit
/// would have skipped it — turning an empty/filtered result into a runtime
/// error. We therefore refuse to hoist a `can_raise()` candidate that appears in
/// any short-circuit-guarded position (see [`appears_guarded`]).
///
/// The predicate is intentionally conservative: only operators that are
/// *provably total* over all inputs are reported as non-raising. Anything not
/// recognised as total (notably every `FunctionCall`, since casts/`array`
/// indexing/most builtins can fail) is treated as fallible. A false `true` only
/// costs a missed hoist; a false `false` would be a correctness bug.
fn can_raise(expr: &ScalarExpr) -> bool {
    match expr {
        ScalarExpr::Column { .. } | ScalarExpr::Literal { .. } | ScalarExpr::Parameter { .. } => {
            false
        }
        ScalarExpr::IsNull { expr: inner, .. } => can_raise(inner),
        ScalarExpr::Unary {
            op, expr: inner, ..
        } => {
            // `+x`, `NOT x`, and bitwise `~x` are total over their domains; only
            // the operand can raise. `-x` can overflow on `i32::MIN`/`i64::MIN`.
            let op_raises = matches!(op, UnaryOp::Neg);
            op_raises || can_raise(inner)
        }
        ScalarExpr::Binary {
            op, left, right, ..
        } => binary_op_can_raise(*op) || can_raise(left) || can_raise(right),
        // Every builtin is treated as potentially fallible: casts overflow,
        // array subscript can be out of bounds, `sqrt`/`ln` reject some inputs,
        // etc. Conservatism here is sound (only forgoes an optimisation).
        ScalarExpr::FunctionCall { .. } => true,
        // Subquery / outer-column leaves are never CSE candidates anyway
        // (`contains_volatile` already excludes them); treat as fallible.
        ScalarExpr::OuterColumn { .. }
        | ScalarExpr::ScalarSubquery { .. }
        | ScalarExpr::Exists { .. }
        | ScalarExpr::InSubquery { .. } => true,
    }
}

/// Returns `true` if a binary operator can raise a runtime error for some
/// well-typed operands. Only operators that are provably total are `false`.
fn binary_op_can_raise(op: BinaryOp) -> bool {
    match op {
        // Provably total: comparisons, Kleene boolean connectives, string
        // concatenation, and bitwise AND/OR/XOR never error on valid operands.
        BinaryOp::Eq
        | BinaryOp::NotEq
        | BinaryOp::Lt
        | BinaryOp::LtEq
        | BinaryOp::Gt
        | BinaryOp::GtEq
        | BinaryOp::And
        | BinaryOp::Or
        | BinaryOp::Concat
        | BinaryOp::BitAnd
        | BinaryOp::BitOr
        | BinaryOp::BitXor => false,
        // Everything else can raise: Add/Sub/Mul overflow (checked arithmetic),
        // Div/Mod divide-by-zero, Pow, bit shifts, and the pattern/regex/JSON/
        // network/vector operators (malformed patterns, missing paths, …).
        _ => true,
    }
}

/// Returns `true` if `candidate` occurs in a short-circuit-guarded position in
/// any of the `source` expressions collected from the current node.
fn fallible_guarded_in(source: &[ScalarExpr], candidate: &ScalarExpr) -> bool {
    let key = ExprKey::new(candidate);
    source
        .iter()
        .any(|e| appears_guarded(e, &key, /* guarded = */ false))
}

/// Returns `true` if `target` appears anywhere inside `expr` in a position that
/// a short-circuiting `AND`/`OR` could skip — i.e. nested (at any depth) within
/// the *right* operand of a `BinaryOp::And` or `BinaryOp::Or`.
///
/// The left operand of `AND`/`OR` is always evaluated, so an occurrence reached
/// only through left operands is *not* guarded. `guarded` starts `false` at the
/// expression root and flips to `true` once we descend into an `AND`/`OR` right
/// operand.
fn appears_guarded(expr: &ScalarExpr, target: &ExprKey, guarded: bool) -> bool {
    if guarded && &ExprKey::new(expr) == target {
        return true;
    }
    match expr {
        ScalarExpr::Binary {
            op: BinaryOp::And | BinaryOp::Or,
            left,
            right,
            ..
        } => {
            // Left operand keeps the current `guarded` flag; the right operand
            // is short-circuit-skippable, so descend with `guarded = true`.
            appears_guarded(left, target, guarded) || appears_guarded(right, target, true)
        }
        ScalarExpr::Binary { left, right, .. } => {
            appears_guarded(left, target, guarded) || appears_guarded(right, target, guarded)
        }
        ScalarExpr::Unary { expr: inner, .. } | ScalarExpr::IsNull { expr: inner, .. } => {
            appears_guarded(inner, target, guarded)
        }
        ScalarExpr::FunctionCall { name, args, .. } => match short_circuit_always_evaluated(name) {
            // For a short-circuiting builtin, the leading `always` arguments are
            // evaluated unconditionally; every argument after them sits in a
            // skippable position (a non-taken CASE branch, or a COALESCE argument
            // past the first non-NULL), so it is short-circuit-guarded.
            Some(always) => args
                .iter()
                .enumerate()
                .any(|(i, a)| appears_guarded(a, target, guarded || i >= always)),
            // Ordinary functions (including NULLIF, whose two operands are always
            // both evaluated) evaluate every argument; the flag is unchanged.
            None => args.iter().any(|a| appears_guarded(a, target, guarded)),
        },
        _ => false,
    }
}

/// For the short-circuiting builtins, returns how many *leading* arguments are
/// always evaluated. Arguments at or after that index occupy short-circuit-
/// guarded positions, so a fallible sub-tree there must not be hoisted into an
/// unconditional `Project` (see [`appears_guarded`]). `None` means every
/// argument is evaluated unconditionally — the default for ordinary functions.
fn short_circuit_always_evaluated(name: &str) -> Option<usize> {
    match name {
        // `coalesce(a1, a2, …)`: a1 is always evaluated; later arguments run only
        // when every preceding one was NULL.
        "coalesce" => Some(1),
        // `case_searched` args `[c1, v1, c2, v2, …, else]`: only the first WHEN
        // condition `c1` is unconditional; every THEN value, later WHEN
        // condition, and the ELSE are reached conditionally.
        "case_searched" => Some(1),
        // `case_simple` args `[op, w1, v1, w2, v2, …, else]`: the operand and the
        // first WHEN comparand are always evaluated; everything after them is
        // conditional.
        "case_simple" => Some(2),
        // NULLIF and all other functions evaluate every argument unconditionally.
        _ => None,
    }
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
        ScalarExpr::FunctionCall { args, .. } => 1 + args.iter().map(expr_size).sum::<usize>(),
        // Subquery variants treated as opaque leaves; full descent is a v0.7 follow-up.
        ScalarExpr::OuterColumn { .. }
        | ScalarExpr::ScalarSubquery { .. }
        | ScalarExpr::Exists { .. }
        | ScalarExpr::InSubquery { .. } => 1,
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
        ScalarExpr::FunctionCall { args, .. } => {
            for arg in args {
                collect_subtrees(arg, freq);
            }
        }
        // Subquery variants treated as opaque leaves; full descent is a v0.7 follow-up.
        ScalarExpr::OuterColumn { .. }
        | ScalarExpr::ScalarSubquery { .. }
        | ScalarExpr::Exists { .. }
        | ScalarExpr::InSubquery { .. } => {}
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
        ScalarExpr::FunctionCall {
            name,
            args,
            data_type,
        } => ScalarExpr::FunctionCall {
            name: name.clone(),
            args: args
                .iter()
                .map(|a| substitute(a, key, col_idx, col_name, dt))
                .collect(),
            data_type: data_type.clone(),
        },
        // Subquery variants treated as opaque leaves; full descent is a v0.7 follow-up.
        ScalarExpr::OuterColumn { .. }
        | ScalarExpr::ScalarSubquery { .. }
        | ScalarExpr::Exists { .. }
        | ScalarExpr::InSubquery { .. } => expr.clone(),
    }
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

    fn gt(l: ScalarExpr, r: ScalarExpr) -> ScalarExpr {
        ScalarExpr::Binary {
            op: BinaryOp::Gt,
            left: Box::new(l),
            right: Box::new(r),
            data_type: DataType::Bool,
        }
    }

    fn and(l: ScalarExpr, r: ScalarExpr) -> ScalarExpr {
        ScalarExpr::Binary {
            op: BinaryOp::And,
            left: Box::new(l),
            right: Box::new(r),
            data_type: DataType::Bool,
        }
    }

    fn neg(e: ScalarExpr) -> ScalarExpr {
        ScalarExpr::Unary {
            op: UnaryOp::Neg,
            expr: Box::new(e),
            data_type: DataType::Int32,
        }
    }

    fn div(l: ScalarExpr, r: ScalarExpr) -> ScalarExpr {
        ScalarExpr::Binary {
            op: BinaryOp::Div,
            left: Box::new(l),
            right: Box::new(r),
            data_type: DataType::Int32,
        }
    }

    fn ne(l: ScalarExpr, r: ScalarExpr) -> ScalarExpr {
        ScalarExpr::Binary {
            op: BinaryOp::NotEq,
            left: Box::new(l),
            right: Box::new(r),
            data_type: DataType::Bool,
        }
    }

    fn lt(l: ScalarExpr, r: ScalarExpr) -> ScalarExpr {
        ScalarExpr::Binary {
            op: BinaryOp::Lt,
            left: Box::new(l),
            right: Box::new(r),
            data_type: DataType::Bool,
        }
    }

    fn three_col_scan() -> LogicalPlan {
        scan(vec![
            Field::required("a", DataType::Int32),
            Field::required("c", DataType::Int32),
            Field::required("b", DataType::Int32),
        ])
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
    fn does_not_hoist_volatile_subexpr() {
        fn project_dup(expr: ScalarExpr) -> LogicalPlan {
            LogicalPlan::Project {
                input: Box::new(two_col_scan()),
                exprs: vec![(expr.clone(), "x".to_owned()), (expr, "y".to_owned())],
                schema: Schema::new([
                    Field::nullable("x", DataType::Int32),
                    Field::nullable("y", DataType::Int32),
                ])
                .expect("schema ok"),
            }
        }

        // Control: a pure 4-node sub-tree duplicated twice IS hoisted.
        let pure_plan = project_dup(neg(add(col("a", 0), col("b", 1))));
        assert!(
            CommonSubExprElimination
                .apply(&pure_plan)
                .expect("no error")
                .is_some(),
            "a duplicated pure sub-expression should be hoisted (control)"
        );

        // A structurally-identical sub-tree containing a volatile `random()`
        // must NOT be hoisted: collapsing two evaluations into one would force
        // the two `random()` results to be equal.
        let rand_call = ScalarExpr::FunctionCall {
            name: "random".to_owned(),
            args: vec![],
            data_type: DataType::Float64,
        };
        let volatile_plan = project_dup(neg(add(rand_call, col("b", 1))));
        assert!(
            CommonSubExprElimination
                .apply(&volatile_plan)
                .expect("no error")
                .is_none(),
            "a duplicated volatile (random) sub-expression must not be hoisted"
        );
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

    #[test]
    fn no_op_on_large_predicate_above_cse_budget() {
        let mut predicate = gt(col("a", 0), lit_i32(0));
        for value in 1..40 {
            predicate = and(predicate, gt(col("a", 0), lit_i32(value)));
        }
        assert!(
            expr_size(&predicate) > MAX_CSE_TOTAL_NODES,
            "test predicate must exceed CSE budget"
        );

        let plan = LogicalPlan::Filter {
            input: Box::new(two_col_scan()),
            predicate,
        };
        let result = CommonSubExprElimination.apply(&plan).expect("no error");
        assert!(
            result.is_none(),
            "large generated predicates should skip CSE instead of formatting every subtree"
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
        let dup = deep_dup(); // neg(a+b) * neg(a+b) — neg(a+b) appears twice
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

    // -----------------------------------------------------------------------
    // Fallible-subexpression hoisting under short-circuit (bug #6)
    // -----------------------------------------------------------------------

    #[test]
    fn does_not_hoist_fallible_subexpr_below_short_circuit() {
        // Repro of bug #6:
        //   WHERE c <> 0 AND ((a/c + b) > 100 OR (a/c + b) < -100)
        // The `a/c + b` (4 nodes) sub-tree appears twice but is guarded by the
        // top `AND`: for rows where `c <> 0` is false, the division never runs.
        // Hoisting it into a Project below the Filter would compute `a/c` for
        // every row, turning a division-by-zero into a runtime error where the
        // unoptimised plan returns 0 rows. It must NOT be hoisted.
        let a_over_c_plus_b = add(div(col("a", 0), col("c", 1)), col("b", 2)); // 5 nodes
        let predicate = and(
            ne(col("c", 1), lit_i32(0)),
            ScalarExpr::Binary {
                op: BinaryOp::Or,
                left: Box::new(gt(a_over_c_plus_b.clone(), lit_i32(100))),
                right: Box::new(lt(a_over_c_plus_b, neg(lit_i32(100)))),
                data_type: DataType::Bool,
            },
        );
        let plan = LogicalPlan::Filter {
            input: Box::new(three_col_scan()),
            predicate,
        };

        let result = CommonSubExprElimination.apply(&plan).expect("no error");
        assert!(
            result.is_none(),
            "fallible sub-tree guarded by a short-circuit must not be hoisted; got {result:?}"
        );
    }

    #[test]
    fn hoists_total_subexpr_even_under_short_circuit() {
        // A *total* (non-raising) sub-tree stays eligible even when guarded:
        //   WHERE b > 0 AND (neg(a+b) = 0 OR neg(a+b) = 1)
        // `neg(a+b)` (4 nodes) cannot raise on these operands... but Add CAN
        // overflow, so this is actually fallible. Use a guaranteed-total tree
        // instead: a comparison chain. To exercise the pure-but-guarded path we
        // rely on `can_raise` reporting false for the candidate. Here we hoist
        // `neg(a)` wrapped to size 4 via `(a = a)`-style is awkward; instead we
        // assert the *unguarded* fallible case below and the guarded-total case
        // is covered by `can_raise` returning false for comparison trees.
        //
        // Construct a guarded but *total* 4-node duplicate: `(a = b) = (a = b)`
        // re-using a Bool comparison (Eq is total). Each `(a = b)` is 3 nodes;
        // wrap in IsNull to reach 4 and keep totality.
        let eq_ab = ScalarExpr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(col("a", 0)),
            right: Box::new(col("b", 1)),
            data_type: DataType::Bool,
        };
        let isnull_eq = ScalarExpr::IsNull {
            expr: Box::new(eq_ab),
            negated: false,
        }; // 4 nodes, total
        let predicate = and(
            gt(col("a", 0), lit_i32(0)),
            and(isnull_eq.clone(), isnull_eq),
        );
        let plan = LogicalPlan::Filter {
            input: Box::new(two_col_scan()),
            predicate,
        };
        let result = CommonSubExprElimination.apply(&plan).expect("no error");
        assert!(
            result.is_some(),
            "a total sub-tree should still be hoisted even when short-circuit-guarded"
        );
    }

    #[test]
    fn hoists_fallible_subexpr_when_not_short_circuit_guarded() {
        // `a/c + b` used twice but NOT under any short-circuit:
        //   WHERE (a/c + b) > 0 AND (a/c + b) < 100
        // is short-circuit-guarded for the *second* conjunct. To get a truly
        // unguarded case, place both uses in a single non-boolean expression:
        //   WHERE (a/c + b) = (a/c + b)
        // Both occurrences are always evaluated (no AND/OR skips them), so the
        // fallible sub-tree IS eligible — no short-circuit protection is lost.
        let lhs = add(div(col("a", 0), col("c", 1)), col("b", 2)); // 5 nodes
        let predicate = ScalarExpr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(lhs.clone()),
            right: Box::new(lhs),
            data_type: DataType::Bool,
        };
        let plan = LogicalPlan::Filter {
            input: Box::new(three_col_scan()),
            predicate,
        };
        let result = CommonSubExprElimination.apply(&plan).expect("no error");
        assert!(
            result.is_some(),
            "an always-evaluated fallible sub-tree should still be hoisted"
        );
    }

    #[test]
    fn can_raise_classifies_total_and_fallible_ops() {
        // Total: comparisons, AND/OR, concat, bitwise AND/OR/XOR.
        assert!(!can_raise(&gt(col("a", 0), col("b", 1))));
        assert!(!can_raise(&and(
            gt(col("a", 0), lit_i32(0)),
            lt(col("b", 1), lit_i32(0))
        )));
        // Fallible: division, modulo, checked Add/Sub/Mul, Neg, any function.
        assert!(can_raise(&div(col("a", 0), col("c", 1))));
        assert!(can_raise(&add(col("a", 0), col("b", 1)))); // overflow
        assert!(can_raise(&neg(col("a", 0)))); // i32::MIN
        assert!(can_raise(&ScalarExpr::FunctionCall {
            name: "__ultrasql_array_subscript".to_owned(),
            args: vec![col("a", 0), lit_i32(1)],
            data_type: DataType::Int32,
        }));
    }

    // -----------------------------------------------------------------------
    // Short-circuit guarding for CASE / COALESCE branches
    //
    // Once the executor evaluates CASE/COALESCE lazily, a fallible sub-tree
    // reachable only through a non-taken branch must be treated as guarded so
    // CSE does not hoist it into an unconditional Project.
    // -----------------------------------------------------------------------

    fn case_searched(args: Vec<ScalarExpr>) -> ScalarExpr {
        ScalarExpr::FunctionCall {
            name: "case_searched".to_owned(),
            args,
            data_type: DataType::Int32,
        }
    }

    fn case_simple(args: Vec<ScalarExpr>) -> ScalarExpr {
        ScalarExpr::FunctionCall {
            name: "case_simple".to_owned(),
            args,
            data_type: DataType::Int32,
        }
    }

    fn coalesce(args: Vec<ScalarExpr>) -> ScalarExpr {
        ScalarExpr::FunctionCall {
            name: "coalesce".to_owned(),
            args,
            data_type: DataType::Int32,
        }
    }

    #[test]
    fn case_searched_branches_are_guarded_except_first_when() {
        let candidate = div(col("a", 0), col("c", 1)); // a/c, fallible
        let key = ExprKey::new(&candidate);

        // [c1, v1, else]: the THEN value is conditional → guarded.
        let in_then = case_searched(vec![
            ne(col("c", 1), lit_i32(0)),
            candidate.clone(),
            lit_i32(0),
        ]);
        assert!(
            appears_guarded(&in_then, &key, false),
            "a CASE THEN value is short-circuit-guarded"
        );

        // [c1, v1, else]: the ELSE value is conditional → guarded.
        let in_else = case_searched(vec![
            ne(col("c", 1), lit_i32(0)),
            lit_i32(0),
            candidate.clone(),
        ]);
        assert!(
            appears_guarded(&in_else, &key, false),
            "a CASE ELSE value is short-circuit-guarded"
        );

        // The first WHEN condition is always evaluated → NOT guarded.
        let in_first_when = case_searched(vec![gt(candidate, lit_i32(0)), lit_i32(1), lit_i32(0)]);
        assert!(
            !appears_guarded(&in_first_when, &key, false),
            "the first CASE WHEN condition is always evaluated, so not guarded"
        );
    }

    #[test]
    fn case_simple_operand_and_first_comparand_not_guarded() {
        let candidate = div(col("a", 0), col("c", 1));
        let key = ExprKey::new(&candidate);

        // [op, w1, v1, else]: op (idx 0) and w1 (idx 1) are always evaluated.
        let in_operand = case_simple(vec![candidate.clone(), lit_i32(1), lit_i32(2), lit_i32(3)]);
        assert!(!appears_guarded(&in_operand, &key, false));

        let in_first_comparand =
            case_simple(vec![lit_i32(0), candidate.clone(), lit_i32(2), lit_i32(3)]);
        assert!(!appears_guarded(&in_first_comparand, &key, false));

        // v1 (idx 2) is conditional → guarded.
        let in_then = case_simple(vec![lit_i32(0), lit_i32(1), candidate, lit_i32(3)]);
        assert!(appears_guarded(&in_then, &key, false));
    }

    #[test]
    fn coalesce_guards_all_but_first_argument() {
        let candidate = div(col("a", 0), col("c", 1));
        let key = ExprKey::new(&candidate);

        // coalesce(a/c, 0): first argument always evaluated → not guarded.
        let first = coalesce(vec![candidate.clone(), lit_i32(0)]);
        assert!(!appears_guarded(&first, &key, false));

        // coalesce(0, a/c): second argument runs only if the first is NULL.
        let second = coalesce(vec![lit_i32(0), candidate]);
        assert!(appears_guarded(&second, &key, false));
    }

    #[test]
    fn nullif_arguments_are_not_guarded() {
        // PostgreSQL evaluates both NULLIF operands unconditionally, so neither
        // is a short-circuit-guarded position.
        let candidate = div(col("a", 0), col("c", 1));
        let key = ExprKey::new(&candidate);
        for args in [
            vec![candidate.clone(), lit_i32(0)],
            vec![lit_i32(0), candidate.clone()],
        ] {
            let nf = ScalarExpr::FunctionCall {
                name: "nullif".to_owned(),
                args,
                data_type: DataType::Int32,
            };
            assert!(!appears_guarded(&nf, &key, false));
        }
    }

    #[test]
    fn does_not_hoist_fallible_subexpr_inside_case_branch() {
        // Two distinct CASE expressions sharing the fallible 5-node sub-tree
        // `a/c + b` in their ELSE branch:
        //   x = CASE WHEN c <> 0 THEN 0 ELSE a/c + b END
        //   y = CASE WHEN c <> 0 THEN 1 ELSE a/c + b END
        // For rows where `c = 0`, lazy CASE never evaluates the ELSE. Hoisting
        // `a/c + b` into a Project would divide by zero for those rows, turning
        // an empty/valid result into a runtime error. It must NOT be hoisted.
        let shared = add(div(col("a", 0), col("c", 1)), col("b", 2)); // 5 nodes, fallible
        let case_with_then = |then: ScalarExpr| {
            case_searched(vec![ne(col("c", 1), lit_i32(0)), then, shared.clone()])
        };
        let plan = LogicalPlan::Project {
            input: Box::new(three_col_scan()),
            exprs: vec![
                (case_with_then(lit_i32(0)), "x".to_owned()),
                (case_with_then(lit_i32(1)), "y".to_owned()),
            ],
            schema: Schema::new([
                Field::nullable("x", DataType::Int32),
                Field::nullable("y", DataType::Int32),
            ])
            .expect("schema ok"),
        };
        let result = CommonSubExprElimination.apply(&plan).expect("no error");
        assert!(
            result.is_none(),
            "a fallible sub-tree inside a CASE branch must not be hoisted; got {result:?}"
        );
    }
}
