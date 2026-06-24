//! EXPLAIN-style rendering for transaction control, session, and utility nodes.
//!
//! Helper bodies split verbatim out of [`super`]'s exhaustive match; output is
//! byte-for-byte identical.

use std::fmt;

use super::super::ddl_types::ExplainFormat;
use super::super::logical_plan::LogicalPlan;
use super::super::node_types::{
    LogicalDescribeTarget, LogicalSetVariableAction, TxnIsolationLevel,
};

pub(super) fn fmt_begin(indent: usize, out: &mut String) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    out.push_str("Begin\n");
}

pub(super) fn fmt_commit(indent: usize, out: &mut String) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    out.push_str("Commit\n");
}

pub(super) fn fmt_rollback(indent: usize, out: &mut String) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    out.push_str("Rollback\n");
}

pub(super) fn fmt_savepoint(name: &str, indent: usize, out: &mut String) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    let _ = fmt::write(out, format_args!("Savepoint: {name}\n"));
}

pub(super) fn fmt_rollback_to_savepoint(name: &str, indent: usize, out: &mut String) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    let _ = fmt::write(out, format_args!("RollbackToSavepoint: {name}\n"));
}

pub(super) fn fmt_release_savepoint(name: &str, indent: usize, out: &mut String) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    let _ = fmt::write(out, format_args!("ReleaseSavepoint: {name}\n"));
}

pub(super) fn fmt_prepare_transaction(gid: &str, indent: usize, out: &mut String) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    let _ = fmt::write(out, format_args!("PrepareTransaction: {gid}\n"));
}

pub(super) fn fmt_commit_prepared(gid: &str, indent: usize, out: &mut String) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    let _ = fmt::write(out, format_args!("CommitPrepared: {gid}\n"));
}

pub(super) fn fmt_rollback_prepared(gid: &str, indent: usize, out: &mut String) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    let _ = fmt::write(out, format_args!("RollbackPrepared: {gid}\n"));
}

pub(super) fn fmt_set_transaction(
    isolation_level: &Option<TxnIsolationLevel>,
    read_only: Option<bool>,
    indent: usize,
    out: &mut String,
) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    out.push_str("SetTransaction:");
    if let Some(level) = isolation_level {
        let _ = fmt::write(out, format_args!(" {level:?}"));
    }
    match read_only {
        Some(true) => out.push_str(" READ ONLY"),
        Some(false) => out.push_str(" READ WRITE"),
        None => {}
    }
    out.push('\n');
}

pub(super) fn fmt_set_variable(
    name: &str,
    action: &LogicalSetVariableAction,
    value: &Option<String>,
    indent: usize,
    out: &mut String,
) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    match value {
        Some(v) => {
            let _ = fmt::write(out, format_args!("SetVariable: {action:?} {name}={v}\n"));
        }
        None => {
            let _ = fmt::write(out, format_args!("SetVariable: {action:?} {name}\n"));
        }
    }
}

pub(super) fn fmt_describe(target: &LogicalDescribeTarget, indent: usize, out: &mut String) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    match target {
        LogicalDescribeTarget::Object {
            name,
            namespace,
            kind,
            ..
        } => {
            let _ = fmt::write(out, format_args!("Describe: {kind:?} {namespace}.{name}\n"));
        }
        LogicalDescribeTarget::Query { .. } => {
            out.push_str("Describe: Query\n");
        }
    }
}

pub(super) fn fmt_summarize(table: &str, namespace: &str, indent: usize, out: &mut String) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    let _ = fmt::write(out, format_args!("Summarize: {namespace}.{table}\n"));
}

pub(super) fn fmt_checkpoint(indent: usize, out: &mut String) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    out.push_str("Checkpoint\n");
}

pub(super) fn fmt_export_database(path: &str, indent: usize, out: &mut String) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    let _ = fmt::write(out, format_args!("ExportDatabase: {path}\n"));
}

pub(super) fn fmt_import_database(path: &str, indent: usize, out: &mut String) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    let _ = fmt::write(out, format_args!("ImportDatabase: {path}\n"));
}

pub(super) fn fmt_set_role(role_name: &Option<String>, indent: usize, out: &mut String) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    let role = role_name.as_deref().unwrap_or("NONE");
    let _ = fmt::write(out, format_args!("SetRole: {role}\n"));
}

pub(super) fn fmt_listen(channel: &str, indent: usize, out: &mut String) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    let _ = fmt::write(out, format_args!("Listen: {channel}\n"));
}

pub(super) fn fmt_notify(channel: &str, payload: &Option<String>, indent: usize, out: &mut String) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    match payload {
        Some(p) => {
            let _ = fmt::write(out, format_args!("Notify: {channel} '{p}'\n"));
        }
        None => {
            let _ = fmt::write(out, format_args!("Notify: {channel}\n"));
        }
    }
}

pub(super) fn fmt_unlisten(channel: &Option<String>, indent: usize, out: &mut String) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    match channel {
        Some(c) => {
            let _ = fmt::write(out, format_args!("Unlisten: {c}\n"));
        }
        None => {
            out.push_str("Unlisten: *\n");
        }
    }
}

pub(super) fn fmt_explain(
    analyze: bool,
    format: &ExplainFormat,
    input: &LogicalPlan,
    indent: usize,
    out: &mut String,
) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    let mode = if analyze { "ANALYZE " } else { "" };
    let fmt_label = match format {
        ExplainFormat::Text => "TEXT",
        ExplainFormat::Json => "JSON",
    };
    let _ = fmt::write(out, format_args!("Explain {mode}({fmt_label})\n"));
    input.display_into(indent + 2, out);
}
