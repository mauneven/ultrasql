//! EXPLAIN-style indented rendering for [`LogicalPlan`].
//!
//! Holds [`LogicalPlan::display`] and its recursive worker plus the
//! [`std::fmt::Display`] impl. The worker is a single exhaustive match over
//! every plan variant; each arm delegates to a category-grouped helper in a
//! sibling module so the output stays byte-for-byte identical while keeping
//! every file small. Split out of the original monolithic `plan.rs` verbatim.

use std::fmt;

use super::logical_plan::LogicalPlan;

mod aggregates;
mod ddl;
mod dml;
mod joins;
mod misc;
mod roles;
mod scans;

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

    fn display_into(&self, indent: usize, out: &mut String) {
        match self {
            Self::Scan { table, .. } => scans::fmt_scan(table, indent, out),
            Self::Filter { input, predicate } => {
                scans::fmt_filter(input, predicate, indent, out);
            }
            Self::Project { input, exprs, .. } => {
                scans::fmt_project(input, exprs, indent, out);
            }
            Self::Limit { input, n, offset } => scans::fmt_limit(input, *n, *offset, indent, out),
            Self::Sort { input, keys } => scans::fmt_sort(input, keys, indent, out),
            Self::DistinctOn { input, on_keys } => {
                scans::fmt_distinct_on(input, on_keys, indent, out);
            }
            Self::Window {
                input,
                partition_by,
                order_by,
                func,
                frame,
                output_name,
                ..
            } => scans::fmt_window(
                input,
                partition_by,
                order_by,
                func,
                frame,
                output_name,
                indent,
                out,
            ),
            Self::Empty { .. } => scans::fmt_empty(indent, out),
            Self::Values { rows, .. } => scans::fmt_values(rows, indent, out),
            Self::Insert {
                table,
                columns,
                source,
                returning,
                ..
            } => dml::fmt_insert(table, columns, source, returning, indent, out),
            Self::Update {
                table,
                assignments,
                input,
                returning,
                ..
            } => dml::fmt_update(table, assignments, input, returning, indent, out),
            Self::Delete {
                table,
                input,
                returning,
                ..
            } => dml::fmt_delete(table, input, returning, indent, out),
            Self::Merge {
                target,
                target_alias,
                source,
                on,
                clauses,
                ..
            } => dml::fmt_merge(target, target_alias, source, on, clauses, indent, out),
            Self::Truncate {
                tables,
                restart_identity,
                cascade,
                ..
            } => dml::fmt_truncate(tables, *restart_identity, *cascade, indent, out),
            Self::CreateTable {
                table_name,
                namespace,
                columns,
                if_not_exists,
                checks,
                unique_constraints,
                exclusion_constraints,
                ..
            } => ddl::fmt_create_table(
                table_name,
                namespace,
                columns,
                *if_not_exists,
                checks,
                unique_constraints,
                exclusion_constraints,
                indent,
                out,
            ),
            Self::CreateMaterializedView {
                table_name,
                namespace,
                columns,
                source,
                if_not_exists,
                ..
            } => ddl::fmt_create_materialized_view(
                table_name,
                namespace,
                columns,
                source,
                *if_not_exists,
                indent,
                out,
            ),
            Self::CreateView {
                table_name,
                namespace,
                columns,
                source,
                or_replace,
                ..
            } => ddl::fmt_create_view(
                table_name,
                namespace,
                columns,
                source,
                *or_replace,
                indent,
                out,
            ),
            Self::CreateTypeEnum {
                type_name,
                namespace,
                labels,
                ..
            } => ddl::fmt_create_type_enum(type_name, namespace, labels, indent, out),
            Self::CreateTypeComposite {
                type_name,
                namespace,
                attributes,
                ..
            } => ddl::fmt_create_type_composite(type_name, namespace, attributes, indent, out),
            Self::CreateDomain {
                domain_name,
                namespace,
                base_type,
                not_null,
                checks,
                ..
            } => ddl::fmt_create_domain(
                domain_name,
                namespace,
                base_type,
                *not_null,
                checks,
                indent,
                out,
            ),
            Self::CreateOperator {
                operator_name,
                namespace,
                left_type,
                right_type,
                procedure,
                result_type,
                ..
            } => ddl::fmt_create_operator(
                operator_name,
                namespace,
                left_type,
                right_type,
                procedure,
                result_type,
                indent,
                out,
            ),
            Self::Join {
                left,
                right,
                join_type,
                condition,
                ..
            } => joins::fmt_join(left, right, join_type, condition, indent, out),
            Self::Aggregate {
                input,
                group_by,
                aggregates,
                ..
            } => aggregates::fmt_aggregate(input, group_by, aggregates, indent, out),
            Self::Pivot {
                input,
                group_columns,
                pivot_column,
                aggregate,
                pivot_values,
                ..
            } => aggregates::fmt_pivot(
                input,
                group_columns,
                *pivot_column,
                aggregate,
                pivot_values,
                indent,
                out,
            ),
            Self::Unpivot {
                input,
                passthrough_columns,
                columns,
                name_column,
                value_column,
                include_nulls,
                ..
            } => aggregates::fmt_unpivot(
                input,
                passthrough_columns,
                columns,
                name_column,
                value_column,
                *include_nulls,
                indent,
                out,
            ),
            Self::SetOp {
                op,
                quantifier,
                left,
                right,
                ..
            } => joins::fmt_set_op(op, quantifier, left, right, indent, out),
            Self::Cte {
                name,
                recursive,
                definition,
                body,
                ..
            } => joins::fmt_cte(name, *recursive, definition, body, indent, out),
            Self::LockRows {
                input,
                strength,
                wait_policy,
                ..
            } => joins::fmt_lock_rows(input, strength, wait_policy, indent, out),
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
            } => ddl::fmt_create_index(
                index_name,
                index_namespace,
                table_name,
                columns,
                key_exprs,
                method,
                *unique,
                *concurrently,
                *if_not_exists,
                indent,
                out,
            ),
            Self::DropIndex {
                indexes,
                index_namespaces,
                if_exists,
                cascade,
                ..
            } => ddl::fmt_drop_index(indexes, index_namespaces, *if_exists, *cascade, indent, out),
            Self::DropTable {
                tables,
                if_exists,
                cascade,
                ..
            } => ddl::fmt_drop_table(tables, *if_exists, *cascade, indent, out),
            Self::AlterTable {
                table_name, action, ..
            } => ddl::fmt_alter_table(table_name, action, indent, out),
            Self::AlterView {
                view_name, action, ..
            } => ddl::fmt_alter_view(view_name, action, indent, out),
            Self::CreatePolicy { policy, .. } => roles::fmt_create_policy(policy, indent, out),
            Self::CreateRole {
                kind,
                role_name,
                if_not_exists,
                ..
            } => roles::fmt_create_role(kind, role_name, *if_not_exists, indent, out),
            Self::AlterRole {
                kind, role_name, ..
            } => roles::fmt_alter_role(kind, role_name, indent, out),
            Self::DropRole {
                kind,
                roles,
                if_exists,
                cascade,
                ..
            } => roles::fmt_drop_role(kind, roles, *if_exists, *cascade, indent, out),
            Self::GrantPrivileges {
                object_kind,
                objects,
                grantees,
                grant_option,
                ..
            } => roles::fmt_grant_privileges(
                object_kind,
                objects,
                grantees,
                *grant_option,
                indent,
                out,
            ),
            Self::RevokePrivileges {
                object_kind,
                objects,
                grantees,
                cascade,
                ..
            } => {
                roles::fmt_revoke_privileges(object_kind, objects, grantees, *cascade, indent, out)
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
            } => roles::fmt_alter_default_privileges(
                target_roles,
                schemas,
                operation,
                object_kind,
                grantees,
                *grant_option,
                *cascade,
                indent,
                out,
            ),
            Self::GrantRole {
                roles,
                grantees,
                admin_option,
                ..
            } => roles::fmt_grant_role(roles, grantees, *admin_option, indent, out),
            Self::RevokeRole {
                roles,
                grantees,
                cascade,
                ..
            } => roles::fmt_revoke_role(roles, grantees, *cascade, indent, out),
            Self::CreateSchema {
                schema_name,
                if_not_exists,
                ..
            } => ddl::fmt_create_schema(schema_name, *if_not_exists, indent, out),
            Self::DropSchema {
                schemas,
                if_exists,
                cascade,
                ..
            } => ddl::fmt_drop_schema(schemas, *if_exists, *cascade, indent, out),
            Self::CreateSequence {
                sequence_name,
                namespace,
                if_not_exists,
                ..
            } => ddl::fmt_create_sequence(sequence_name, namespace, *if_not_exists, indent, out),
            Self::AlterSequence { sequence_name, .. } => {
                ddl::fmt_alter_sequence(sequence_name, indent, out);
            }
            Self::DropSequence {
                sequences,
                if_exists,
                cascade,
                ..
            } => ddl::fmt_drop_sequence(sequences, *if_exists, *cascade, indent, out),
            Self::Comment {
                target, comment, ..
            } => ddl::fmt_comment(target, comment, indent, out),
            Self::Begin { .. } => misc::fmt_begin(indent, out),
            Self::Commit { .. } => misc::fmt_commit(indent, out),
            Self::Rollback { .. } => misc::fmt_rollback(indent, out),
            Self::Savepoint { name, .. } => misc::fmt_savepoint(name, indent, out),
            Self::RollbackToSavepoint { name, .. } => {
                misc::fmt_rollback_to_savepoint(name, indent, out);
            }
            Self::ReleaseSavepoint { name, .. } => misc::fmt_release_savepoint(name, indent, out),
            Self::PrepareTransaction { gid, .. } => {
                misc::fmt_prepare_transaction(gid, indent, out);
            }
            Self::CommitPrepared { gid, .. } => misc::fmt_commit_prepared(gid, indent, out),
            Self::RollbackPrepared { gid, .. } => misc::fmt_rollback_prepared(gid, indent, out),
            Self::SetTransaction {
                isolation_level, ..
            } => misc::fmt_set_transaction(isolation_level, indent, out),
            Self::SetVariable {
                name,
                action,
                value,
                ..
            } => misc::fmt_set_variable(name, action, value, indent, out),
            Self::Describe { target, .. } => misc::fmt_describe(target, indent, out),
            Self::Summarize {
                table, namespace, ..
            } => misc::fmt_summarize(table, namespace, indent, out),
            Self::Checkpoint { .. } => misc::fmt_checkpoint(indent, out),
            Self::ExportDatabase { path, .. } => misc::fmt_export_database(path, indent, out),
            Self::ImportDatabase { path, .. } => misc::fmt_import_database(path, indent, out),
            Self::SetRole { role_name, .. } => misc::fmt_set_role(role_name, indent, out),
            Self::Listen { channel, .. } => misc::fmt_listen(channel, indent, out),
            Self::Notify {
                channel, payload, ..
            } => misc::fmt_notify(channel, payload, indent, out),
            Self::Unlisten { channel, .. } => misc::fmt_unlisten(channel, indent, out),
            Self::Explain {
                analyze,
                format,
                input,
                ..
            } => misc::fmt_explain(*analyze, format, input, indent, out),
            Self::Copy {
                relation,
                columns,
                direction,
                source,
                format,
                ..
            } => dml::fmt_copy(relation, columns, direction, source, format, indent, out),
            Self::FunctionScan { name, args, .. } => {
                scans::fmt_function_scan(name, args, indent, out);
            }
        }
    }
}

impl fmt::Display for LogicalPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.display(0))
    }
}
