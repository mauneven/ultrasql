//! EXPLAIN-style rendering for joins, set operations, CTEs, and row locking.
//!
//! Helper bodies split verbatim out of [`super`]'s exhaustive match; output is
//! byte-for-byte identical.

use std::fmt;

use super::super::logical_plan::LogicalPlan;
use super::super::node_types::{
    LogicalJoinCondition, LogicalJoinType, LogicalSetOp, LogicalSetQuantifier, LockStrength,
    LockWaitPolicy,
};

pub(super) fn fmt_join(
    left: &LogicalPlan,
    right: &LogicalPlan,
    join_type: &LogicalJoinType,
    condition: &LogicalJoinCondition,
    indent: usize,
    out: &mut String,
) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    let jt = match join_type {
        LogicalJoinType::Inner => "Inner",
        LogicalJoinType::LeftOuter => "LeftOuter",
        LogicalJoinType::RightOuter => "RightOuter",
        LogicalJoinType::FullOuter => "FullOuter",
        LogicalJoinType::Cross => "Cross",
        LogicalJoinType::Semi => "Semi",
        LogicalJoinType::Anti => "Anti",
    };
    out.push_str("Join[");
    out.push_str(jt);
    out.push_str("]: ");
    match condition {
        LogicalJoinCondition::On(pred) => {
            let _ = fmt::write(out, format_args!("ON {pred}"));
        }
        LogicalJoinCondition::Using(pairs) => {
            out.push_str("USING(");
            for (i, (l, r)) in pairs.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                let _ = fmt::write(out, format_args!("{l}={r}"));
            }
            out.push(')');
        }
        LogicalJoinCondition::None => {
            out.push_str("(none)");
        }
    }
    out.push('\n');
    left.display_into(indent + 2, out);
    right.display_into(indent + 2, out);
}

pub(super) fn fmt_set_op(
    op: &LogicalSetOp,
    quantifier: &LogicalSetQuantifier,
    left: &LogicalPlan,
    right: &LogicalPlan,
    indent: usize,
    out: &mut String,
) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    let op_str = match op {
        LogicalSetOp::Union => "Union",
        LogicalSetOp::Intersect => "Intersect",
        LogicalSetOp::Except => "Except",
    };
    let q_str = match quantifier {
        LogicalSetQuantifier::All => "All",
        LogicalSetQuantifier::Distinct => "Distinct",
    };
    let _ = fmt::write(out, format_args!("SetOp[{op_str} {q_str}]\n"));
    left.display_into(indent + 2, out);
    right.display_into(indent + 2, out);
}

pub(super) fn fmt_cte(
    name: &str,
    recursive: bool,
    definition: &LogicalPlan,
    body: &LogicalPlan,
    indent: usize,
    out: &mut String,
) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    let rec = if recursive { " RECURSIVE" } else { "" };
    let _ = fmt::write(out, format_args!("Cte{rec}: {name}\n"));
    definition.display_into(indent + 2, out);
    body.display_into(indent + 2, out);
}

pub(super) fn fmt_lock_rows(
    input: &LogicalPlan,
    strength: &LockStrength,
    wait_policy: &LockWaitPolicy,
    indent: usize,
    out: &mut String,
) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    let s = match strength {
        LockStrength::Update => "UPDATE",
        LockStrength::NoKeyUpdate => "NO KEY UPDATE",
        LockStrength::Share => "SHARE",
        LockStrength::KeyShare => "KEY SHARE",
    };
    let w = match wait_policy {
        LockWaitPolicy::Wait => "",
        LockWaitPolicy::NoWait => " NOWAIT",
        LockWaitPolicy::SkipLocked => " SKIP LOCKED",
    };
    let _ = fmt::write(out, format_args!("LockRows: FOR {s}{w}\n"));
    input.display_into(indent + 2, out);
}
