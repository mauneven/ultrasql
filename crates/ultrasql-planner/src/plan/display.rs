//! EXPLAIN-style indented rendering for [`LogicalPlan`].
//!
//! Holds [`LogicalPlan::display`] and its recursive worker plus the
//! [`std::fmt::Display`] impl. The worker is a single exhaustive match over
//! every plan variant and is kept whole to preserve identical output. Split
//! out of the original monolithic `plan.rs` verbatim.

use std::fmt;

use super::ddl_types::{
    CopyDirection, CopyFormat, CopySource, ExplainFormat, LogicalAlterTableAction,
    LogicalAlterViewAction, LogicalCommentTarget, LogicalRoleKind,
};
use super::logical_plan::LogicalPlan;
use super::node_types::{
    AggregateFunc, LogicalDescribeTarget, LogicalIndexMethod, LogicalJoinCondition,
    LogicalJoinType, LogicalSetOp, LogicalSetQuantifier, LockStrength, LockWaitPolicy,
};

impl LogicalPlan {
    /// Render this plan in an indented EXPLAIN-style tree, where every
    /// child line is indented by two additional spaces.
    ///
    /// `indent` is the column the *root* node's text begins at.
    #[must_use]
    pub fn display(&self, indent: usize) -> String {
        let mut out = String::new();
        self.display_into(indent, &mut out);
        out
    }

    #[allow(clippy::too_many_lines)]
    fn display_into(&self, indent: usize, out: &mut String) {
        let pad = " ".repeat(indent);
        match self {
            Self::Scan { table, .. } => {
                out.push_str(&pad);
                out.push_str("Scan: ");
                out.push_str(table);
                out.push('\n');
            }
            Self::Filter { input, predicate } => {
                out.push_str(&pad);
                out.push_str("Filter: ");
                let _ = fmt::write(out, format_args!("{predicate}"));
                out.push('\n');
                input.display_into(indent + 2, out);
            }
            Self::Project { input, exprs, .. } => {
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
            Self::Limit { input, n, offset } => {
                out.push_str(&pad);
                let _ = fmt::write(out, format_args!("Limit: n={n}, offset={offset}\n"));
                input.display_into(indent + 2, out);
            }
            Self::Sort { input, keys } => {
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
            Self::Window {
                input,
                partition_by,
                order_by,
                func,
                output_name,
                ..
            } => {
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
            Self::Empty { .. } => {
                out.push_str(&pad);
                out.push_str("Empty\n");
            }
            Self::Values { rows, .. } => {
                out.push_str(&pad);
                let _ = fmt::write(out, format_args!("Values: {} row(s)\n", rows.len()));
            }
            Self::Insert {
                table,
                columns,
                source,
                returning,
                ..
            } => {
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
            Self::Update {
                table,
                assignments,
                input,
                returning,
                ..
            } => {
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
            Self::Delete {
                table,
                input,
                returning,
                ..
            } => {
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
            Self::Merge {
                target,
                target_alias,
                source,
                on,
                clauses,
                ..
            } => {
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
            Self::Truncate {
                tables,
                restart_identity,
                cascade,
                ..
            } => {
                out.push_str(&pad);
                out.push_str("Truncate: tables=[");
                out.push_str(&tables.join(", "));
                out.push(']');
                if *restart_identity {
                    out.push_str(" RESTART IDENTITY");
                }
                if *cascade {
                    out.push_str(" CASCADE");
                }
                out.push('\n');
            }
            Self::CreateTable {
                table_name,
                namespace,
                columns,
                if_not_exists,
                checks,
                unique_constraints,
                exclusion_constraints,
                ..
            } => {
                out.push_str(&pad);
                out.push_str("CreateTable: ");
                out.push_str(namespace);
                out.push('.');
                out.push_str(table_name);
                if *if_not_exists {
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
            Self::CreateMaterializedView {
                table_name,
                namespace,
                columns,
                source,
                if_not_exists,
                ..
            } => {
                out.push_str(&pad);
                out.push_str("CreateMaterializedView: ");
                out.push_str(namespace);
                out.push('.');
                out.push_str(table_name);
                if *if_not_exists {
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
            Self::CreateView {
                table_name,
                namespace,
                columns,
                source,
                or_replace,
                ..
            } => {
                out.push_str(&pad);
                out.push_str("CreateView: ");
                out.push_str(namespace);
                out.push('.');
                out.push_str(table_name);
                if *or_replace {
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
            Self::CreateTypeEnum {
                type_name,
                namespace,
                labels,
                ..
            } => {
                out.push_str(&pad);
                let _ = fmt::write(
                    out,
                    format_args!(
                        "CreateTypeEnum: {namespace}.{type_name} labels=[{}]\n",
                        labels.join(", ")
                    ),
                );
            }
            Self::CreateTypeComposite {
                type_name,
                namespace,
                attributes,
                ..
            } => {
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
            Self::CreateDomain {
                domain_name,
                namespace,
                base_type,
                not_null,
                checks,
                ..
            } => {
                out.push_str(&pad);
                let _ = fmt::write(
                    out,
                    format_args!(
                        "CreateDomain: {namespace}.{domain_name} AS {base_type} not_null={not_null} checks={}\n",
                        checks.len()
                    ),
                );
            }
            Self::CreateOperator {
                operator_name,
                namespace,
                left_type,
                right_type,
                procedure,
                result_type,
                ..
            } => {
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
            Self::Join {
                left,
                right,
                join_type,
                condition,
                ..
            } => {
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
            Self::Aggregate {
                input,
                group_by,
                aggregates,
                ..
            } => {
                out.push_str(&pad);
                out.push_str("Aggregate: group_by=[");
                for (i, g) in group_by.iter().enumerate() {
                    if i > 0 {
                        out.push_str(", ");
                    }
                    let _ = fmt::write(out, format_args!("{g}"));
                }
                out.push_str("] aggs=[");
                for (i, agg) in aggregates.iter().enumerate() {
                    if i > 0 {
                        out.push_str(", ");
                    }
                    let func_name = match agg.func {
                        AggregateFunc::CountStar => "count(*)",
                        AggregateFunc::Count => "count",
                        AggregateFunc::Sum => "sum",
                        AggregateFunc::Avg => "avg",
                        AggregateFunc::Min => "min",
                        AggregateFunc::Max => "max",
                        AggregateFunc::BoolAnd => "bool_and",
                        AggregateFunc::BoolOr => "bool_or",
                        AggregateFunc::StringAgg => "string_agg",
                        AggregateFunc::ArrayAgg => "array_agg",
                        AggregateFunc::JsonAgg => "json_agg",
                        AggregateFunc::StddevSamp => "stddev_samp",
                        AggregateFunc::StddevPop => "stddev_pop",
                        AggregateFunc::VarSamp => "var_samp",
                        AggregateFunc::VarPop => "var_pop",
                        AggregateFunc::Corr => "corr",
                        AggregateFunc::PercentileCont => "percentile_cont",
                        AggregateFunc::PercentileDisc => "percentile_disc",
                    };
                    if let Some(arg) = &agg.arg {
                        let dist = if agg.distinct { "DISTINCT " } else { "" };
                        let _ = fmt::write(
                            out,
                            format_args!("{func_name}({dist}{arg}) AS {}", agg.output_name),
                        );
                    } else {
                        let _ = fmt::write(out, format_args!("{func_name} AS {}", agg.output_name));
                    }
                }
                out.push_str("]\n");
                input.display_into(indent + 2, out);
            }
            Self::Pivot {
                input,
                group_columns,
                pivot_column,
                aggregate,
                pivot_values,
                ..
            } => {
                out.push_str(&pad);
                let _ = fmt::write(
                    out,
                    format_args!(
                        "Pivot: groups={group_columns:?} pivot_column={pivot_column} func={:?} values=[{}]\n",
                        aggregate.func,
                        pivot_values
                            .iter()
                            .map(|value| value.output_name.as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
                );
                input.display_into(indent + 2, out);
            }
            Self::Unpivot {
                input,
                passthrough_columns,
                columns,
                name_column,
                value_column,
                include_nulls,
                ..
            } => {
                out.push_str(&pad);
                let _ = fmt::write(
                    out,
                    format_args!(
                        "Unpivot: passthrough={passthrough_columns:?} name={name_column} value={value_column} include_nulls={include_nulls} columns=[{}]\n",
                        columns
                            .iter()
                            .map(|column| column.label.as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
                );
                input.display_into(indent + 2, out);
            }
            Self::SetOp {
                op,
                quantifier,
                left,
                right,
                ..
            } => {
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
            Self::Cte {
                name,
                recursive,
                definition,
                body,
                ..
            } => {
                out.push_str(&pad);
                let rec = if *recursive { " RECURSIVE" } else { "" };
                let _ = fmt::write(out, format_args!("Cte{rec}: {name}\n"));
                definition.display_into(indent + 2, out);
                body.display_into(indent + 2, out);
            }
            Self::LockRows {
                input,
                strength,
                wait_policy,
                ..
            } => {
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
            Self::CreateIndex {
                index_name,
                index_namespace,
                table_name,
                columns,
                key_exprs,
                method,
                unique,
                concurrently,
                if_not_exists,
                ..
            } => {
                out.push_str(&pad);
                let u = if *unique { "Unique" } else { "" };
                let c = if *concurrently { " Concurrently" } else { "" };
                let inx = if *if_not_exists { " IF NOT EXISTS" } else { "" };
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
                        qualified_index_name =
                            ultrasql_catalog::index_lookup_key(index_namespace, index_name),
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
            Self::DropIndex {
                indexes,
                index_namespaces,
                if_exists,
                cascade,
                ..
            } => {
                out.push_str(&pad);
                let inx = if *if_exists { " IF EXISTS" } else { "" };
                let csc = if *cascade { " CASCADE" } else { "" };
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
            Self::DropTable {
                tables,
                if_exists,
                cascade,
                ..
            } => {
                out.push_str(&pad);
                let inx = if *if_exists { " IF EXISTS" } else { "" };
                let csc = if *cascade { " CASCADE" } else { "" };
                let _ = fmt::write(
                    out,
                    format_args!(
                        "DropTable{inx}: tables=[{names}]{csc}\n",
                        names = tables.join(", ")
                    ),
                );
            }
            Self::AlterTable {
                table_name, action, ..
            } => {
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
                            format_args!(
                                "AlterTable: {table_name} RENAME COLUMN {old_name} TO {new_name}\n"
                            ),
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
                }
            }
            Self::AlterView {
                view_name, action, ..
            } => {
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
            Self::CreatePolicy { policy, .. } => {
                out.push_str(&pad);
                let _ = fmt::write(
                    out,
                    format_args!(
                        "CreatePolicy: {} ON {}\n",
                        policy.policy_name, policy.table_name
                    ),
                );
            }
            Self::CreateRole {
                kind,
                role_name,
                if_not_exists,
                ..
            } => {
                out.push_str(&pad);
                let ine = if *if_not_exists { " IF NOT EXISTS" } else { "" };
                let keyword = match kind {
                    LogicalRoleKind::Role => "Role",
                    LogicalRoleKind::User => "User",
                };
                let _ = fmt::write(out, format_args!("Create{keyword}{ine}: {role_name}\n"));
            }
            Self::AlterRole {
                kind, role_name, ..
            } => {
                out.push_str(&pad);
                let keyword = match kind {
                    LogicalRoleKind::Role => "Role",
                    LogicalRoleKind::User => "User",
                };
                let _ = fmt::write(out, format_args!("Alter{keyword}: {role_name}\n"));
            }
            Self::DropRole {
                kind,
                roles,
                if_exists,
                cascade,
                ..
            } => {
                out.push_str(&pad);
                let ie = if *if_exists { " IF EXISTS" } else { "" };
                let csc = if *cascade { " CASCADE" } else { "" };
                let keyword = match kind {
                    LogicalRoleKind::Role => "Role",
                    LogicalRoleKind::User => "User",
                };
                let _ = fmt::write(
                    out,
                    format_args!("Drop{keyword}{ie}: roles=[{}]{csc}\n", roles.join(", ")),
                );
            }
            Self::GrantPrivileges {
                object_kind,
                objects,
                grantees,
                grant_option,
                ..
            } => {
                out.push_str(&pad);
                let opt = if *grant_option {
                    " WITH GRANT OPTION"
                } else {
                    ""
                };
                let _ = fmt::write(
                    out,
                    format_args!(
                        "GrantPrivileges: {:?} objects=[{}] grantees=[{}]{opt}\n",
                        object_kind,
                        objects.join(", "),
                        grantees.join(", ")
                    ),
                );
            }
            Self::RevokePrivileges {
                object_kind,
                objects,
                grantees,
                cascade,
                ..
            } => {
                out.push_str(&pad);
                let csc = if *cascade { " CASCADE" } else { "" };
                let _ = fmt::write(
                    out,
                    format_args!(
                        "RevokePrivileges: {:?} objects=[{}] grantees=[{}]{csc}\n",
                        object_kind,
                        objects.join(", "),
                        grantees.join(", ")
                    ),
                );
            }
            Self::AlterDefaultPrivileges {
                target_roles,
                schemas,
                operation,
                object_kind,
                grantees,
                grant_option,
                cascade,
                ..
            } => {
                out.push_str(&pad);
                let scope = if target_roles.is_empty() {
                    "current role".to_owned()
                } else {
                    target_roles.join(", ")
                };
                let schema_scope = if schemas.is_empty() {
                    "all schemas".to_owned()
                } else {
                    schemas.join(", ")
                };
                let opt = if *grant_option {
                    " WITH GRANT OPTION"
                } else if *cascade {
                    " CASCADE"
                } else {
                    ""
                };
                let _ = fmt::write(
                    out,
                    format_args!(
                        "AlterDefaultPrivileges: {:?} {:?} roles=[{}] schemas=[{}] grantees=[{}]{opt}\n",
                        operation,
                        object_kind,
                        scope,
                        schema_scope,
                        grantees.join(", ")
                    ),
                );
            }
            Self::GrantRole {
                roles,
                grantees,
                admin_option,
                ..
            } => {
                out.push_str(&pad);
                let opt = if *admin_option {
                    " WITH ADMIN OPTION"
                } else {
                    ""
                };
                let _ = fmt::write(
                    out,
                    format_args!(
                        "GrantRole: roles=[{}] grantees=[{}]{opt}\n",
                        roles.join(", "),
                        grantees.join(", ")
                    ),
                );
            }
            Self::RevokeRole {
                roles,
                grantees,
                cascade,
                ..
            } => {
                out.push_str(&pad);
                let csc = if *cascade { " CASCADE" } else { "" };
                let _ = fmt::write(
                    out,
                    format_args!(
                        "RevokeRole: roles=[{}] grantees=[{}]{csc}\n",
                        roles.join(", "),
                        grantees.join(", ")
                    ),
                );
            }
            Self::CreateSchema {
                schema_name,
                if_not_exists,
                ..
            } => {
                out.push_str(&pad);
                let ine = if *if_not_exists { " IF NOT EXISTS" } else { "" };
                let _ = fmt::write(out, format_args!("CreateSchema{ine}: {schema_name}\n"));
            }
            Self::DropSchema {
                schemas,
                if_exists,
                cascade,
                ..
            } => {
                out.push_str(&pad);
                let ie = if *if_exists { " IF EXISTS" } else { "" };
                let csc = if *cascade { " CASCADE" } else { "" };
                let _ = fmt::write(
                    out,
                    format_args!("DropSchema{ie}: schemas=[{}]{csc}\n", schemas.join(", ")),
                );
            }
            Self::CreateSequence {
                sequence_name,
                namespace,
                if_not_exists,
                ..
            } => {
                out.push_str(&pad);
                let ine = if *if_not_exists { " IF NOT EXISTS" } else { "" };
                let _ = fmt::write(
                    out,
                    format_args!("CreateSequence{ine}: {namespace}.{sequence_name}\n"),
                );
            }
            Self::AlterSequence { sequence_name, .. } => {
                out.push_str(&pad);
                let _ = fmt::write(out, format_args!("AlterSequence: {sequence_name}\n"));
            }
            Self::DropSequence {
                sequences,
                if_exists,
                cascade,
                ..
            } => {
                out.push_str(&pad);
                let ie = if *if_exists { " IF EXISTS" } else { "" };
                let csc = if *cascade { " CASCADE" } else { "" };
                let _ = fmt::write(
                    out,
                    format_args!(
                        "DropSequence{ie}: sequences=[{}]{csc}\n",
                        sequences.join(", ")
                    ),
                );
            }
            Self::Comment {
                target, comment, ..
            } => {
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
            Self::Begin { .. } => {
                out.push_str(&pad);
                out.push_str("Begin\n");
            }
            Self::Commit { .. } => {
                out.push_str(&pad);
                out.push_str("Commit\n");
            }
            Self::Rollback { .. } => {
                out.push_str(&pad);
                out.push_str("Rollback\n");
            }
            Self::Savepoint { name, .. } => {
                out.push_str(&pad);
                let _ = fmt::write(out, format_args!("Savepoint: {name}\n"));
            }
            Self::RollbackToSavepoint { name, .. } => {
                out.push_str(&pad);
                let _ = fmt::write(out, format_args!("RollbackToSavepoint: {name}\n"));
            }
            Self::ReleaseSavepoint { name, .. } => {
                out.push_str(&pad);
                let _ = fmt::write(out, format_args!("ReleaseSavepoint: {name}\n"));
            }
            Self::PrepareTransaction { gid, .. } => {
                out.push_str(&pad);
                let _ = fmt::write(out, format_args!("PrepareTransaction: {gid}\n"));
            }
            Self::CommitPrepared { gid, .. } => {
                out.push_str(&pad);
                let _ = fmt::write(out, format_args!("CommitPrepared: {gid}\n"));
            }
            Self::RollbackPrepared { gid, .. } => {
                out.push_str(&pad);
                let _ = fmt::write(out, format_args!("RollbackPrepared: {gid}\n"));
            }
            Self::SetTransaction {
                isolation_level, ..
            } => {
                out.push_str(&pad);
                let _ = fmt::write(out, format_args!("SetTransaction: {isolation_level:?}\n"));
            }
            Self::SetVariable {
                name,
                action,
                value,
                ..
            } => {
                out.push_str(&pad);
                match value {
                    Some(v) => {
                        let _ =
                            fmt::write(out, format_args!("SetVariable: {action:?} {name}={v}\n"));
                    }
                    None => {
                        let _ = fmt::write(out, format_args!("SetVariable: {action:?} {name}\n"));
                    }
                }
            }
            Self::Describe { target, .. } => {
                out.push_str(&pad);
                match target {
                    LogicalDescribeTarget::Object {
                        name,
                        namespace,
                        kind,
                        ..
                    } => {
                        let _ = fmt::write(
                            out,
                            format_args!("Describe: {kind:?} {namespace}.{name}\n"),
                        );
                    }
                    LogicalDescribeTarget::Query { .. } => {
                        out.push_str("Describe: Query\n");
                    }
                }
            }
            Self::Summarize {
                table, namespace, ..
            } => {
                out.push_str(&pad);
                let _ = fmt::write(out, format_args!("Summarize: {namespace}.{table}\n"));
            }
            Self::Checkpoint { .. } => {
                out.push_str(&pad);
                out.push_str("Checkpoint\n");
            }
            Self::ExportDatabase { path, .. } => {
                out.push_str(&pad);
                let _ = fmt::write(out, format_args!("ExportDatabase: {path}\n"));
            }
            Self::ImportDatabase { path, .. } => {
                out.push_str(&pad);
                let _ = fmt::write(out, format_args!("ImportDatabase: {path}\n"));
            }
            Self::SetRole { role_name, .. } => {
                out.push_str(&pad);
                let role = role_name.as_deref().unwrap_or("NONE");
                let _ = fmt::write(out, format_args!("SetRole: {role}\n"));
            }
            Self::Listen { channel, .. } => {
                out.push_str(&pad);
                let _ = fmt::write(out, format_args!("Listen: {channel}\n"));
            }
            Self::Notify {
                channel, payload, ..
            } => {
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
            Self::Unlisten { channel, .. } => {
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
            Self::Explain {
                analyze,
                format,
                input,
                ..
            } => {
                out.push_str(&pad);
                let mode = if *analyze { "ANALYZE " } else { "" };
                let fmt_label = match format {
                    ExplainFormat::Text => "TEXT",
                    ExplainFormat::Json => "JSON",
                };
                let _ = fmt::write(out, format_args!("Explain {mode}({fmt_label})\n"));
                input.display_into(indent + 2, out);
            }
            Self::Copy {
                relation,
                columns,
                direction,
                source,
                format,
                ..
            } => {
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
            Self::FunctionScan { name, args, .. } => {
                out.push_str(&pad);
                let arg_list = args
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ");
                let _ = fmt::write(out, format_args!("FunctionScan: {name}({arg_list})\n"));
            }
        }
    }
}

impl fmt::Display for LogicalPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.display(0))
    }
}
