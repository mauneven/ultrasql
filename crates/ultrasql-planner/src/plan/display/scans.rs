//! EXPLAIN-style rendering for scan, projection, and row-producing nodes.
//!
//! Helper bodies split verbatim out of [`super`]'s exhaustive match; output is
//! byte-for-byte identical.

use std::fmt;

use crate::expr::ScalarExpr;

use super::super::logical_plan::LogicalPlan;
use super::super::node_types::{LogicalWindowFrame, LogicalWindowFunc, SortKey};

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

#[allow(clippy::too_many_arguments)]
pub(super) fn fmt_window(
    input: &LogicalPlan,
    partition_by: &[ScalarExpr],
    order_by: &[SortKey],
    func: &LogicalWindowFunc,
    frame: &LogicalWindowFrame,
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
    // Render the frame only when it is non-default. The two default
    // frames (whole-partition without ORDER BY, running with ORDER BY)
    // are suppressed so frame-less query plans keep their existing
    // display and the display tests stay stable.
    if !frame.is_whole_partition_default() && !frame.is_default_running() {
        out.push_str(" FRAME ");
        fmt_window_frame(frame, out);
    }
    out.push('\n');
    input.display_into(indent + 2, out);
}

fn fmt_window_frame(frame: &LogicalWindowFrame, out: &mut String) {
    use super::super::node_types::{BoundFrameExclusion, BoundFrameUnits};
    let units = match frame.units {
        BoundFrameUnits::Rows => "ROWS",
        BoundFrameUnits::Range => "RANGE",
        BoundFrameUnits::Groups => "GROUPS",
    };
    out.push_str(units);
    out.push_str(" BETWEEN ");
    fmt_frame_bound(&frame.start, out);
    out.push_str(" AND ");
    fmt_frame_bound(&frame.end, out);
    match frame.exclude {
        BoundFrameExclusion::NoOthers => {}
        BoundFrameExclusion::CurrentRow => out.push_str(" EXCLUDE CURRENT ROW"),
        BoundFrameExclusion::Group => out.push_str(" EXCLUDE GROUP"),
        BoundFrameExclusion::Ties => out.push_str(" EXCLUDE TIES"),
    }
}

fn fmt_frame_bound(bound: &super::super::node_types::BoundFrameBound, out: &mut String) {
    use super::super::node_types::BoundFrameBound;
    match bound {
        BoundFrameBound::UnboundedPreceding => out.push_str("UNBOUNDED PRECEDING"),
        BoundFrameBound::Preceding(e) => {
            let _ = fmt::write(out, format_args!("{e} PRECEDING"));
        }
        BoundFrameBound::CurrentRow => out.push_str("CURRENT ROW"),
        BoundFrameBound::Following(e) => {
            let _ = fmt::write(out, format_args!("{e} FOLLOWING"));
        }
        BoundFrameBound::UnboundedFollowing => out.push_str("UNBOUNDED FOLLOWING"),
    }
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
