//! EXPLAIN-style rendering for schema-definition nodes (CREATE/ALTER/DROP of
//! tables, views, types, indexes, sequences, schemas, and comments).
//!
//! Helper bodies split verbatim out of [`super`]'s exhaustive match; output is
//! byte-for-byte identical.

use std::fmt;

use ultrasql_core::{DataType, Schema};

use crate::expr::ScalarExpr;

use super::super::ddl_types::{
    LogicalAlterTableAction, LogicalAlterViewAction, LogicalCheckConstraint, LogicalCommentTarget,
    LogicalExclusionConstraint, LogicalUniqueConstraint,
};
use super::super::logical_plan::LogicalPlan;
use super::super::node_types::LogicalIndexMethod;

#[allow(clippy::too_many_arguments)]
pub(super) fn fmt_create_table(
    table_name: &str,
    namespace: &str,
    columns: &Schema,
    if_not_exists: bool,
    checks: &[LogicalCheckConstraint],
    unique_constraints: &[LogicalUniqueConstraint],
    exclusion_constraints: &[LogicalExclusionConstraint],
    indent: usize,
    out: &mut String,
) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    out.push_str("CreateTable: ");
    out.push_str(namespace);
    out.push('.');
    out.push_str(table_name);
    if if_not_exists {
        out.push_str(" IF NOT EXISTS");
    }
    out.push_str(" (");
    for (i, f) in columns.fields().iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        let _ = fmt::write(out, format_args!("{} {:?}", f.name, f.data_type));
        if !f.nullable {
            out.push_str(" NOT NULL");
        }
    }
    out.push_str(")\n");
    for check in checks {
        let _ = fmt::write(
            out,
            format_args!("{pad}  Check: {} = {}\n", check.name, check.expr),
        );
    }
    for unique in unique_constraints {
        let kind = if unique.primary_key {
            "PrimaryKey"
        } else {
            "Unique"
        };
        let _ = fmt::write(
            out,
            format_args!("{pad}  {kind}: {} {:?}\n", unique.name, unique.columns),
        );
    }
    for exclusion in exclusion_constraints {
        let _ = fmt::write(
            out,
            format_args!(
                "{pad}  Exclude {:?}: {} {:?}\n",
                exclusion.method, exclusion.name, exclusion.elements
            ),
        );
    }
}

pub(super) fn fmt_create_materialized_view(
    table_name: &str,
    namespace: &str,
    columns: &Schema,
    source: &LogicalPlan,
    if_not_exists: bool,
    indent: usize,
    out: &mut String,
) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    out.push_str("CreateMaterializedView: ");
    out.push_str(namespace);
    out.push('.');
    out.push_str(table_name);
    if if_not_exists {
        out.push_str(" IF NOT EXISTS");
    }
    out.push_str(" (");
    for (i, f) in columns.fields().iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        let _ = fmt::write(out, format_args!("{} {:?}", f.name, f.data_type));
        if !f.nullable {
            out.push_str(" NOT NULL");
        }
    }
    out.push_str(")\n");
    source.display_into(indent + 2, out);
}

pub(super) fn fmt_create_view(
    table_name: &str,
    namespace: &str,
    columns: &Schema,
    source: &LogicalPlan,
    or_replace: bool,
    indent: usize,
    out: &mut String,
) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    out.push_str("CreateView: ");
    out.push_str(namespace);
    out.push('.');
    out.push_str(table_name);
    if or_replace {
        out.push_str(" OR REPLACE");
    }
    out.push_str(" (");
    for (i, f) in columns.fields().iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        let _ = fmt::write(out, format_args!("{} {:?}", f.name, f.data_type));
        if !f.nullable {
            out.push_str(" NOT NULL");
        }
    }
    out.push_str(")\n");
    source.display_into(indent + 2, out);
}

pub(super) fn fmt_create_type_enum(
    type_name: &str,
    namespace: &str,
    labels: &[String],
    indent: usize,
    out: &mut String,
) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    let _ = fmt::write(
        out,
        format_args!(
            "CreateTypeEnum: {namespace}.{type_name} labels=[{}]\n",
            labels.join(", ")
        ),
    );
}

pub(super) fn fmt_create_type_composite(
    type_name: &str,
    namespace: &str,
    attributes: &Schema,
    indent: usize,
    out: &mut String,
) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    let _ = fmt::write(
        out,
        format_args!("CreateTypeComposite: {namespace}.{type_name} ("),
    );
    for (i, field) in attributes.fields().iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        let _ = fmt::write(out, format_args!("{} {}", field.name, field.data_type));
    }
    out.push_str(")\n");
}

pub(super) fn fmt_create_domain(
    domain_name: &str,
    namespace: &str,
    base_type: &DataType,
    not_null: bool,
    checks: &[LogicalCheckConstraint],
    indent: usize,
    out: &mut String,
) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    let _ = fmt::write(
        out,
        format_args!(
            "CreateDomain: {namespace}.{domain_name} AS {base_type} not_null={not_null} checks={}\n",
            checks.len()
        ),
    );
}

#[allow(clippy::too_many_arguments)]
pub(super) fn fmt_create_operator(
    operator_name: &str,
    namespace: &str,
    left_type: &Option<DataType>,
    right_type: &Option<DataType>,
    procedure: &str,
    result_type: &DataType,
    indent: usize,
    out: &mut String,
) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    let left = left_type
        .as_ref()
        .map_or_else(|| "none".to_owned(), ToString::to_string);
    let right = right_type
        .as_ref()
        .map_or_else(|| "none".to_owned(), ToString::to_string);
    let _ = fmt::write(
        out,
        format_args!(
            "CreateOperator: {namespace}.{operator_name} ({left}, {right}) PROCEDURE {procedure} RETURNS {result_type}\n"
        ),
    );
}

#[allow(clippy::too_many_arguments)]
pub(super) fn fmt_create_index(
    index_name: &str,
    index_namespace: &str,
    table_name: &str,
    columns: &[usize],
    key_exprs: &[ScalarExpr],
    method: &LogicalIndexMethod,
    unique: bool,
    concurrently: bool,
    if_not_exists: bool,
    indent: usize,
    out: &mut String,
) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    let u = if unique { "Unique" } else { "" };
    let c = if concurrently { " Concurrently" } else { "" };
    let inx = if if_not_exists { " IF NOT EXISTS" } else { "" };
    let method = match method {
        LogicalIndexMethod::Btree => "btree",
        LogicalIndexMethod::Hash => "hash",
        LogicalIndexMethod::Gin => "gin",
        LogicalIndexMethod::Gist => "gist",
        LogicalIndexMethod::Brin => "brin",
        LogicalIndexMethod::Hnsw => "hnsw",
        LogicalIndexMethod::IvfFlat => "ivfflat",
        LogicalIndexMethod::Aggregating => "aggregating",
    };
    let _ = fmt::write(
        out,
        format_args!(
            "Create{u}Index{c}{inx}: {qualified_index_name} ON {table_name} USING {method} (keys=[{keys}])\n",
            qualified_index_name = ultrasql_catalog::index_lookup_key(index_namespace, index_name),
            keys = if columns.is_empty() {
                key_exprs
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(",")
            } else {
                columns
                    .iter()
                    .map(usize::to_string)
                    .collect::<Vec<_>>()
                    .join(",")
            }
        ),
    );
}

pub(super) fn fmt_drop_index(
    indexes: &[String],
    index_namespaces: &[Option<String>],
    if_exists: bool,
    cascade: bool,
    indent: usize,
    out: &mut String,
) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    let inx = if if_exists { " IF EXISTS" } else { "" };
    let csc = if cascade { " CASCADE" } else { "" };
    let names = indexes
        .iter()
        .enumerate()
        .map(|(idx, name)| {
            index_namespaces
                .get(idx)
                .and_then(Option::as_ref)
                .map_or_else(|| name.clone(), |namespace| format!("{namespace}.{name}"))
        })
        .collect::<Vec<_>>()
        .join(", ");
    let _ = fmt::write(
        out,
        format_args!("DropIndex{inx}: indexes=[{names}]{csc}\n",),
    );
}

pub(super) fn fmt_drop_table(
    tables: &[String],
    if_exists: bool,
    cascade: bool,
    indent: usize,
    out: &mut String,
) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    let inx = if if_exists { " IF EXISTS" } else { "" };
    let csc = if cascade { " CASCADE" } else { "" };
    let _ = fmt::write(
        out,
        format_args!(
            "DropTable{inx}: tables=[{names}]{csc}\n",
            names = tables.join(", ")
        ),
    );
}

pub(super) fn fmt_alter_table(
    table_name: &str,
    action: &LogicalAlterTableAction,
    indent: usize,
    out: &mut String,
) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    match action {
        LogicalAlterTableAction::AddColumn { column, default } => {
            let _ = fmt::write(
                out,
                format_args!(
                    "AlterTable: {table_name} ADD COLUMN {} {:?}{}{}\n",
                    column.name,
                    column.data_type,
                    if default.is_some() { " DEFAULT" } else { "" },
                    if column.nullable { "" } else { " NOT NULL" }
                ),
            );
        }
        LogicalAlterTableAction::DropColumn { column_name, .. } => {
            let _ = fmt::write(
                out,
                format_args!("AlterTable: {table_name} DROP COLUMN {column_name}\n"),
            );
        }
        LogicalAlterTableAction::RenameColumn {
            old_name, new_name, ..
        } => {
            let _ = fmt::write(
                out,
                format_args!("AlterTable: {table_name} RENAME COLUMN {old_name} TO {new_name}\n"),
            );
        }
        LogicalAlterTableAction::RenameTable { new_name } => {
            let _ = fmt::write(
                out,
                format_args!("AlterTable: {table_name} RENAME TO {new_name}\n"),
            );
        }
        LogicalAlterTableAction::EnableRowLevelSecurity => {
            let _ = fmt::write(
                out,
                format_args!("AlterTable: {table_name} ENABLE ROW LEVEL SECURITY\n"),
            );
        }
        LogicalAlterTableAction::SetOptions { options } => {
            let rendered = options
                .iter()
                .map(|option| format!("{}={}", option.name, option.value))
                .collect::<Vec<_>>()
                .join(", ");
            let _ = fmt::write(
                out,
                format_args!("AlterTable: {table_name} SET ({rendered})\n"),
            );
        }
        LogicalAlterTableAction::AddUniqueConstraint { constraint } => {
            let kind = if constraint.primary_key {
                "PRIMARY KEY"
            } else {
                "UNIQUE"
            };
            let _ = fmt::write(
                out,
                format_args!(
                    "AlterTable: {table_name} ADD CONSTRAINT {} {kind} {:?}\n",
                    constraint.name, constraint.columns
                ),
            );
        }
        LogicalAlterTableAction::AddCheckConstraint { constraint } => {
            let _ = fmt::write(
                out,
                format_args!(
                    "AlterTable: {table_name} ADD CONSTRAINT {} CHECK\n",
                    constraint.name
                ),
            );
        }
        LogicalAlterTableAction::DropConstraint {
            name, if_exists, ..
        } => {
            let exists = if *if_exists { "IF EXISTS " } else { "" };
            let _ = fmt::write(
                out,
                format_args!("AlterTable: {table_name} DROP CONSTRAINT {exists}{name}\n"),
            );
        }
    }
}

pub(super) fn fmt_alter_view(
    view_name: &str,
    action: &LogicalAlterViewAction,
    indent: usize,
    out: &mut String,
) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    match action {
        LogicalAlterViewAction::RenameView { new_name } => {
            let _ = fmt::write(
                out,
                format_args!("AlterView: {view_name} RENAME TO {new_name}\n"),
            );
        }
        LogicalAlterViewAction::SetSchema { new_schema } => {
            let _ = fmt::write(
                out,
                format_args!("AlterView: {view_name} SET SCHEMA {new_schema}\n"),
            );
        }
    }
}

pub(super) fn fmt_create_schema(
    schema_name: &str,
    if_not_exists: bool,
    indent: usize,
    out: &mut String,
) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    let ine = if if_not_exists { " IF NOT EXISTS" } else { "" };
    let _ = fmt::write(out, format_args!("CreateSchema{ine}: {schema_name}\n"));
}

pub(super) fn fmt_drop_schema(
    schemas: &[String],
    if_exists: bool,
    cascade: bool,
    indent: usize,
    out: &mut String,
) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    let ie = if if_exists { " IF EXISTS" } else { "" };
    let csc = if cascade { " CASCADE" } else { "" };
    let _ = fmt::write(
        out,
        format_args!("DropSchema{ie}: schemas=[{}]{csc}\n", schemas.join(", ")),
    );
}

pub(super) fn fmt_create_sequence(
    sequence_name: &str,
    namespace: &str,
    if_not_exists: bool,
    indent: usize,
    out: &mut String,
) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    let ine = if if_not_exists { " IF NOT EXISTS" } else { "" };
    let _ = fmt::write(
        out,
        format_args!("CreateSequence{ine}: {namespace}.{sequence_name}\n"),
    );
}

pub(super) fn fmt_alter_sequence(sequence_name: &str, indent: usize, out: &mut String) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    let _ = fmt::write(out, format_args!("AlterSequence: {sequence_name}\n"));
}

pub(super) fn fmt_drop_sequence(
    sequences: &[String],
    if_exists: bool,
    cascade: bool,
    indent: usize,
    out: &mut String,
) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    let ie = if if_exists { " IF EXISTS" } else { "" };
    let csc = if cascade { " CASCADE" } else { "" };
    let _ = fmt::write(
        out,
        format_args!(
            "DropSequence{ie}: sequences=[{}]{csc}\n",
            sequences.join(", ")
        ),
    );
}

pub(super) fn fmt_comment(
    target: &LogicalCommentTarget,
    comment: &Option<String>,
    indent: usize,
    out: &mut String,
) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    match target {
        LogicalCommentTarget::Table { table } => {
            let action = if comment.is_some() { "SET" } else { "CLEAR" };
            let _ = fmt::write(out, format_args!("Comment: TABLE {table} {action}\n"));
        }
        LogicalCommentTarget::Index { index, namespace } => {
            let action = if comment.is_some() { "SET" } else { "CLEAR" };
            let target = namespace
                .as_ref()
                .map_or_else(|| index.clone(), |schema| format!("{schema}.{index}"));
            let _ = fmt::write(out, format_args!("Comment: INDEX {target} {action}\n"));
        }
        LogicalCommentTarget::Column { table, column, .. } => {
            let action = if comment.is_some() { "SET" } else { "CLEAR" };
            let _ = fmt::write(
                out,
                format_args!("Comment: COLUMN {table}.{column} {action}\n"),
            );
        }
    }
}
