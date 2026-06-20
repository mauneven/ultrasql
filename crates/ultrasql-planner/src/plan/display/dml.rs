//! EXPLAIN-style rendering for data-mutation nodes (INSERT/UPDATE/DELETE/MERGE/
//! TRUNCATE/COPY).
//!
//! Helper bodies split verbatim out of [`super`]'s exhaustive match; output is
//! byte-for-byte identical.

use std::fmt;

use crate::expr::ScalarExpr;

use super::super::ddl_types::{CopyDirection, CopyFormat, CopySource};
use super::super::logical_plan::LogicalPlan;
use super::super::node_types::LogicalMergeClause;

pub(super) fn fmt_insert(
    table: &str,
    columns: &[usize],
    source: &LogicalPlan,
    returning: &[(ScalarExpr, String)],
    indent: usize,
    out: &mut String,
) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    out.push_str("Insert: table=");
    out.push_str(table);
    out.push_str(" cols=[");
    for (i, c) in columns.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        let _ = fmt::write(out, format_args!("{c}"));
    }
    out.push(']');
    if !returning.is_empty() {
        out.push_str(" returning=[");
        for (i, (e, n)) in returning.iter().enumerate() {
            if i > 0 {
                out.push_str(", ");
            }
            let _ = fmt::write(out, format_args!("{e} AS {n}"));
        }
        out.push(']');
    }
    out.push('\n');
    source.display_into(indent + 2, out);
}

pub(super) fn fmt_update(
    table: &str,
    assignments: &[(usize, ScalarExpr)],
    input: &LogicalPlan,
    returning: &[(ScalarExpr, String)],
    indent: usize,
    out: &mut String,
) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    out.push_str("Update: table=");
    out.push_str(table);
    out.push_str(" assignments=[");
    for (i, (idx, e)) in assignments.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        let _ = fmt::write(out, format_args!("col{idx}={e}"));
    }
    out.push(']');
    if !returning.is_empty() {
        out.push_str(" returning=[");
        for (i, (e, n)) in returning.iter().enumerate() {
            if i > 0 {
                out.push_str(", ");
            }
            let _ = fmt::write(out, format_args!("{e} AS {n}"));
        }
        out.push(']');
    }
    out.push('\n');
    input.display_into(indent + 2, out);
}

pub(super) fn fmt_delete(
    table: &str,
    input: &LogicalPlan,
    returning: &[(ScalarExpr, String)],
    indent: usize,
    out: &mut String,
) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    out.push_str("Delete: table=");
    out.push_str(table);
    if !returning.is_empty() {
        out.push_str(" returning=[");
        for (i, (e, n)) in returning.iter().enumerate() {
            if i > 0 {
                out.push_str(", ");
            }
            let _ = fmt::write(out, format_args!("{e} AS {n}"));
        }
        out.push(']');
    }
    out.push('\n');
    input.display_into(indent + 2, out);
}

pub(super) fn fmt_merge(
    target: &str,
    target_alias: &Option<String>,
    source: &LogicalPlan,
    on: &ScalarExpr,
    clauses: &[LogicalMergeClause],
    indent: usize,
    out: &mut String,
) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    out.push_str("Merge: target=");
    out.push_str(target);
    if let Some(alias) = target_alias {
        out.push_str(" alias=");
        out.push_str(alias);
    }
    let _ = fmt::write(out, format_args!(" on={on} clauses={}", clauses.len()));
    out.push('\n');
    source.display_into(indent + 2, out);
}

pub(super) fn fmt_truncate(
    tables: &[String],
    restart_identity: bool,
    cascade: bool,
    indent: usize,
    out: &mut String,
) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    out.push_str("Truncate: tables=[");
    out.push_str(&tables.join(", "));
    out.push(']');
    if restart_identity {
        out.push_str(" RESTART IDENTITY");
    }
    if cascade {
        out.push_str(" CASCADE");
    }
    out.push('\n');
}

pub(super) fn fmt_copy(
    relation: &Option<String>,
    columns: &[usize],
    direction: &CopyDirection,
    source: &CopySource,
    format: &CopyFormat,
    indent: usize,
    out: &mut String,
) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    let dir = match direction {
        CopyDirection::From => "FROM",
        CopyDirection::To => "TO",
    };
    let src = match source {
        CopySource::Stdin => "STDIN",
        CopySource::Stdout => "STDOUT",
        CopySource::File(_) => "FILE",
    };
    let fmt_label = match format {
        CopyFormat::Text => "TEXT",
        CopyFormat::Csv => "CSV",
        CopyFormat::Binary => "BINARY",
        CopyFormat::Parquet => "PARQUET",
    };
    let cols = if columns.is_empty() {
        String::from("*")
    } else {
        columns
            .iter()
            .map(usize::to_string)
            .collect::<Vec<_>>()
            .join(",")
    };
    let target = relation.as_deref().unwrap_or("<query>");
    let _ = fmt::write(
        out,
        format_args!("Copy: {target} ({cols}) {dir} {src} FORMAT={fmt_label}\n"),
    );
}
