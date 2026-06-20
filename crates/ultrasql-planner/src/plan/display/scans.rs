//! EXPLAIN-style rendering for scan, projection, and row-producing nodes.
//!
//! Helper bodies split verbatim out of [`super`]'s exhaustive match; output is
//! byte-for-byte identical.

use std::fmt;

use crate::expr::ScalarExpr;

use super::super::logical_plan::LogicalPlan;
use super::super::node_types::{LogicalWindowFunc, SortKey};

pub(super) fn fmt_scan(table: &str, indent: usize, out: &mut String) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    out.push_str("Scan: ");
    out.push_str(table);
    out.push('\n');
}

pub(super) fn fmt_filter(
    input: &LogicalPlan,
    predicate: &ScalarExpr,
    indent: usize,
    out: &mut String,
) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    out.push_str("Filter: ");
    let _ = fmt::write(out, format_args!("{predicate}"));
    out.push('\n');
    input.display_into(indent + 2, out);
}

pub(super) fn fmt_project(
    input: &LogicalPlan,
    exprs: &[(ScalarExpr, String)],
    indent: usize,
    out: &mut String,
) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    out.push_str("Project: ");
    for (i, (e, n)) in exprs.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        let _ = fmt::write(out, format_args!("{e} AS {n}"));
    }
    out.push('\n');
    input.display_into(indent + 2, out);
}

pub(super) fn fmt_limit(input: &LogicalPlan, n: u64, offset: u64, indent: usize, out: &mut String) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    let _ = fmt::write(out, format_args!("Limit: n={n}, offset={offset}\n"));
    input.display_into(indent + 2, out);
}

pub(super) fn fmt_sort(input: &LogicalPlan, keys: &[SortKey], indent: usize, out: &mut String) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    out.push_str("Sort: ");
    for (i, k) in keys.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        let dir = if k.asc { "ASC" } else { "DESC" };
        let nulls = if k.nulls_first {
            "NULLS FIRST"
        } else {
            "NULLS LAST"
        };
        let _ = fmt::write(out, format_args!("{} {dir} {nulls}", k.expr));
    }
    out.push('\n');
    input.display_into(indent + 2, out);
}

pub(super) fn fmt_window(
    input: &LogicalPlan,
    partition_by: &[ScalarExpr],
    order_by: &[SortKey],
    func: &LogicalWindowFunc,
    output_name: &str,
    indent: usize,
    out: &mut String,
) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    let _ = fmt::write(out, format_args!("Window: {output_name} = {func:?}"));
    if !partition_by.is_empty() {
        out.push_str(" PARTITION BY [");
        for (i, e) in partition_by.iter().enumerate() {
            if i > 0 {
                out.push_str(", ");
            }
            let _ = fmt::write(out, format_args!("{e}"));
        }
        out.push(']');
    }
    if !order_by.is_empty() {
        out.push_str(" ORDER BY [");
        for (i, k) in order_by.iter().enumerate() {
            if i > 0 {
                out.push_str(", ");
            }
            let dir = if k.asc { "ASC" } else { "DESC" };
            let _ = fmt::write(out, format_args!("{} {dir}", k.expr));
        }
        out.push(']');
    }
    out.push('\n');
    input.display_into(indent + 2, out);
}

pub(super) fn fmt_empty(indent: usize, out: &mut String) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    out.push_str("Empty\n");
}

pub(super) fn fmt_values(rows: &[Vec<ScalarExpr>], indent: usize, out: &mut String) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    let _ = fmt::write(out, format_args!("Values: {} row(s)\n", rows.len()));
}

pub(super) fn fmt_function_scan(name: &str, args: &[ScalarExpr], indent: usize, out: &mut String) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    let arg_list = args
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    let _ = fmt::write(out, format_args!("FunctionScan: {name}({arg_list})\n"));
}
