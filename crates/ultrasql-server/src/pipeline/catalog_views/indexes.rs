//! `pg_index`, `pg_inherits`, `pg_constraint`, and `pg_policy` scans plus the
//! shared virtual-constraint model.

use std::collections::HashMap;

use ultrasql_core::{DataType, Field, Oid, Schema, Value};
use ultrasql_planner::LogicalReferentialAction;

use crate::pipeline::LowerCtx;

use super::common::*;
use super::roles::role_oid_map;

pub(super) fn schema_pg_index() -> Schema {
    schema([
        Field::required("indexrelid", DataType::Int64),
        Field::required("indrelid", DataType::Int64),
        Field::required("indnatts", DataType::Int16),
        Field::required("indisunique", DataType::Bool),
        Field::required("indisprimary", DataType::Bool),
        Field::required("indisclustered", DataType::Bool),
        Field::required("indisvalid", DataType::Bool),
        Field::required("indisreplident", DataType::Bool),
        Field::required("indkey", DataType::Array(Box::new(DataType::Int16))),
    ])
}

pub(super) fn rows_pg_index(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    let mut indexes: Vec<_> = ctx.catalog_snapshot.indexes.values().collect();
    indexes.sort_by(|a, b| a.name.cmp(&b.name));
    indexes
        .into_iter()
        .map(|idx| {
            vec![
                v_i64(idx.oid.raw()),
                v_i64(idx.table_oid.raw()),
                Value::Int16(i16::try_from(idx.columns.len()).unwrap_or(i16::MAX)),
                Value::Bool(idx.is_unique),
                Value::Bool(idx.is_primary),
                Value::Bool(false),
                Value::Bool(true),
                Value::Bool(false),
                Value::Array {
                    element_type: DataType::Int16,
                    elements: idx
                        .columns
                        .iter()
                        .map(|col| {
                            let attnum = i16::try_from(usize::from(*col) + 1).unwrap_or(i16::MAX);
                            Value::Int16(attnum)
                        })
                        .collect(),
                },
            ]
        })
        .collect()
}

pub(super) fn schema_pg_inherits() -> Schema {
    schema([
        Field::required("inhrelid", DataType::Int64),
        Field::required("inhparent", DataType::Int64),
        Field::required("inhseqno", DataType::Int32),
        Field::required("inhdetachpending", DataType::Bool),
    ])
}

#[derive(Clone, Debug)]
pub(super) struct VirtualConstraint {
    pub(super) oid: i64,
    pub(super) name: String,
    pub(super) kind: &'static str,
    pub(super) table_oid: Oid,
    pub(super) index_oid: Option<Oid>,
    pub(super) table_schema: String,
    pub(super) table_name: String,
    pub(super) columns: Vec<usize>,
    pub(super) foreign_table_oid: Option<Oid>,
    pub(super) foreign_columns: Vec<usize>,
    pub(super) on_delete: LogicalReferentialAction,
    pub(super) on_update: LogicalReferentialAction,
    pub(super) deferrable: bool,
    pub(super) initially_deferred: bool,
    pub(super) check_clause: Option<String>,
}

pub(super) fn virtual_constraints(ctx: &LowerCtx<'_>) -> Vec<VirtualConstraint> {
    let mut out = Vec::new();
    let mut indexes: Vec<_> = ctx.catalog_snapshot.indexes.values().collect();
    indexes.sort_by(|a, b| a.name.cmp(&b.name));
    for index in indexes {
        if !index.is_unique {
            continue;
        }
        let Some(table) = ctx.catalog_snapshot.tables_by_oid.get(&index.table_oid) else {
            continue;
        };
        out.push(VirtualConstraint {
            oid: 30_000 + i64::from(index.oid.raw()),
            name: index.name.clone(),
            kind: unique_index_constraint_kind(ctx, index),
            table_oid: table.oid,
            index_oid: Some(index.oid),
            table_schema: table.schema_name.clone(),
            table_name: table.name.clone(),
            columns: index.columns.iter().map(|c| usize::from(*c)).collect(),
            foreign_table_oid: None,
            foreign_columns: Vec::new(),
            on_delete: LogicalReferentialAction::NoAction,
            on_update: LogicalReferentialAction::NoAction,
            deferrable: false,
            initially_deferred: false,
            check_clause: None,
        });
    }

    let mut runtime: Vec<_> = ctx
        .table_constraints
        .iter()
        .map(|item| (*item.key(), item.value().clone()))
        .collect();
    runtime.sort_by_key(|(oid, _)| oid.raw());
    for (table_oid, constraints) in runtime {
        let Some(table) = ctx.catalog_snapshot.tables_by_oid.get(&table_oid) else {
            continue;
        };
        for (idx, check) in constraints.checks.iter().enumerate() {
            out.push(VirtualConstraint {
                oid: 40_000
                    + i64::from(table_oid.raw()) * 100
                    + i64::try_from(idx).unwrap_or(i64::MAX),
                name: check.name.clone(),
                kind: "c",
                table_oid,
                index_oid: None,
                table_schema: table.schema_name.clone(),
                table_name: table.name.clone(),
                columns: Vec::new(),
                foreign_table_oid: None,
                foreign_columns: Vec::new(),
                on_delete: LogicalReferentialAction::NoAction,
                on_update: LogicalReferentialAction::NoAction,
                deferrable: false,
                initially_deferred: false,
                check_clause: Some(check.expr.to_string()),
            });
        }
        for (idx, fk) in constraints.foreign_keys.iter().enumerate() {
            out.push(VirtualConstraint {
                oid: 50_000
                    + i64::from(table_oid.raw()) * 100
                    + i64::try_from(idx).unwrap_or(i64::MAX),
                name: fk.name.clone(),
                kind: "f",
                table_oid,
                index_oid: None,
                table_schema: table.schema_name.clone(),
                table_name: table.name.clone(),
                columns: fk.columns.clone(),
                foreign_table_oid: Some(fk.target_oid),
                foreign_columns: fk.target_columns.clone(),
                on_delete: fk.on_delete,
                on_update: fk.on_update,
                deferrable: fk.deferrable,
                initially_deferred: fk.initially_deferred,
                check_clause: None,
            });
        }
    }
    out.sort_by(|a, b| {
        (
            a.table_schema.as_str(),
            a.table_name.as_str(),
            a.name.as_str(),
        )
            .cmp(&(
                b.table_schema.as_str(),
                b.table_name.as_str(),
                b.name.as_str(),
            ))
    });
    out
}

pub(super) fn unique_index_constraint_kind(
    ctx: &LowerCtx<'_>,
    index: &ultrasql_catalog::IndexEntry,
) -> &'static str {
    ctx.catalog_snapshot
        .constraints
        .values()
        .find(|row| row.conrelid == index.table_oid && row.conname == index.name)
        .map_or_else(
            || {
                if index.is_primary { "p" } else { "u" }
            },
            constraint_kind,
        )
}

pub(super) fn constraint_kind(row: &ultrasql_catalog::persistent::ConstraintRow) -> &'static str {
    match row.contype {
        ultrasql_catalog::persistent::ConType::Check => "c",
        ultrasql_catalog::persistent::ConType::ForeignKey => "f",
        ultrasql_catalog::persistent::ConType::PrimaryKey => "p",
        ultrasql_catalog::persistent::ConType::Unique => "u",
        ultrasql_catalog::persistent::ConType::Trigger => "t",
        ultrasql_catalog::persistent::ConType::Exclusion => "x",
    }
}

pub(super) fn attnums_text(columns: &[usize]) -> Value {
    if columns.is_empty() {
        return Value::Null;
    }
    v_text(
        columns
            .iter()
            .map(|col| (col + 1).to_string())
            .collect::<Vec<_>>()
            .join(" "),
    )
}

pub(super) fn schema_pg_constraint() -> Schema {
    schema([
        Field::required("oid", DataType::Int64),
        Field::required("conname", text()),
        Field::required("connamespace", DataType::Int64),
        Field::required("contype", DataType::Text { max_len: Some(1) }),
        Field::required("conrelid", DataType::Int64),
        Field::required("conindid", DataType::Int64),
        Field::required("confrelid", DataType::Int64),
        Field::nullable("conkey", text()),
        Field::nullable("confkey", text()),
        Field::required("convalidated", DataType::Bool),
        Field::required("condeferrable", DataType::Bool),
        Field::required("condeferred", DataType::Bool),
    ])
}

pub(super) fn rows_pg_constraint(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    virtual_constraints(ctx)
        .into_iter()
        .map(|c| {
            vec![
                Value::Int64(c.oid),
                v_text(c.name),
                Value::Int64(namespace_oid(&c.table_schema)),
                v_text(c.kind),
                v_i64(c.table_oid.raw()),
                c.index_oid.map_or(Value::Int64(0), |oid| v_i64(oid.raw())),
                c.foreign_table_oid
                    .map_or(Value::Int64(0), |oid| v_i64(oid.raw())),
                attnums_text(&c.columns),
                attnums_text(&c.foreign_columns),
                Value::Bool(true),
                Value::Bool(c.deferrable),
                Value::Bool(c.initially_deferred),
            ]
        })
        .collect()
}

pub(super) fn schema_pg_policy() -> Schema {
    schema([
        Field::required("polname", text()),
        Field::required("polrelid", DataType::Int64),
        Field::required("polcmd", DataType::Text { max_len: Some(1) }),
        Field::required("polpermissive", DataType::Bool),
        Field::required("polroles", DataType::Array(Box::new(DataType::Int64))),
        Field::nullable("polqual", text()),
        Field::nullable("polwithcheck", text()),
    ])
}

pub(super) fn rows_pg_policy(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    let roles = role_oid_map(ctx);
    let mut policies = ctx
        .row_security
        .iter()
        .map(|item| (*item.key(), item.value().clone()))
        .collect::<Vec<_>>();
    policies.sort_by_key(|(oid, _)| oid.raw());

    let mut rows = Vec::new();
    for (table_oid, runtime) in policies {
        if !ctx.catalog_snapshot.tables_by_oid.contains_key(&table_oid) {
            continue;
        }
        let mut table_policies = runtime.policies.clone();
        table_policies.sort_by(|left, right| left.name.cmp(&right.name));
        for policy in table_policies {
            rows.push(vec![
                v_text(policy.name),
                v_i64(table_oid.raw()),
                v_text(policy_command_code(policy.command)),
                Value::Bool(matches!(
                    policy.permissiveness,
                    crate::RuntimeRlsPermissiveness::Permissive
                )),
                Value::Array {
                    element_type: DataType::Int64,
                    elements: policy_role_oids(&policy.roles, &roles),
                },
                policy_expr_text(policy.using.as_ref()),
                policy_expr_text(policy.with_check.as_ref()),
            ]);
        }
    }
    rows
}

pub(super) fn policy_command_code(command: crate::RuntimeRlsCommand) -> &'static str {
    match command {
        crate::RuntimeRlsCommand::All => "*",
        crate::RuntimeRlsCommand::Select => "r",
        crate::RuntimeRlsCommand::Insert => "a",
        crate::RuntimeRlsCommand::Update => "w",
        crate::RuntimeRlsCommand::Delete => "d",
    }
}

pub(super) fn policy_role_oids(policy_roles: &[String], role_oids: &HashMap<String, i64>) -> Vec<Value> {
    if policy_roles.is_empty() {
        return vec![Value::Int64(0)];
    }
    policy_roles
        .iter()
        .map(|role| {
            if role == "public" {
                Value::Int64(0)
            } else {
                Value::Int64(role_oids.get(role).copied().unwrap_or(0))
            }
        })
        .collect()
}

pub(super) fn policy_expr_text(expr: Option<&crate::RuntimeTenantPolicyExpr>) -> Value {
    expr.map_or(Value::Null, |expr| {
        v_text(format!(
            "{} = current_setting('{}', true)",
            expr.column_name, expr.setting_name
        ))
    })
}

