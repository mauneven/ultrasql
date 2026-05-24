//! Column-source mapping helpers for privilege enforcement.

use std::collections::BTreeSet;

use ultrasql_core::Schema;
use ultrasql_planner::{LogicalJoinCondition, LogicalJoinType, LogicalPlan, ScalarExpr};

use crate::auth::PrivilegeKind;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(super) struct ColumnSource {
    pub(super) table: String,
    pub(super) column: String,
}

pub(super) fn plan_sources(plan: &LogicalPlan) -> Vec<Option<ColumnSource>> {
    match plan {
        LogicalPlan::Scan {
            table,
            schema,
            projection,
        } => scan_sources(table, schema, projection.as_deref()),
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::LockRows { input, .. } => plan_sources(input),
        LogicalPlan::Project { input, exprs, .. } => {
            let input_sources = plan_sources(input);
            exprs
                .iter()
                .map(|(expr, _)| expr_direct_source(expr, &input_sources))
                .collect()
        }
        LogicalPlan::Window { input, .. } => {
            let mut sources = plan_sources(input);
            sources.push(None);
            sources
        }
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggregates,
            ..
        } => {
            let input_sources = plan_sources(input);
            let mut sources = Vec::with_capacity(group_by.len() + aggregates.len());
            for expr in group_by {
                sources.push(expr_direct_source(expr, &input_sources));
            }
            sources.extend((0..aggregates.len()).map(|_| None));
            sources
        }
        LogicalPlan::Join {
            left,
            right,
            join_type,
            condition,
            schema,
        } => join_sources(
            plan_sources(left),
            plan_sources(right),
            *join_type,
            condition,
            schema,
        ),
        LogicalPlan::SetOp { schema, .. }
        | LogicalPlan::Cte { schema, .. }
        | LogicalPlan::Values { schema, .. }
        | LogicalPlan::Empty { schema }
        | LogicalPlan::FunctionScan { schema, .. }
        | LogicalPlan::Insert { schema, .. }
        | LogicalPlan::Update { schema, .. }
        | LogicalPlan::Delete { schema, .. }
        | LogicalPlan::Truncate { schema, .. }
        | LogicalPlan::CreateTable { schema, .. }
        | LogicalPlan::CreateMaterializedView { schema, .. }
        | LogicalPlan::CreateTypeEnum { schema, .. }
        | LogicalPlan::CreateTypeComposite { schema, .. }
        | LogicalPlan::CreateDomain { schema, .. }
        | LogicalPlan::CreateIndex { schema, .. }
        | LogicalPlan::CreatePolicy { schema, .. }
        | LogicalPlan::CreateRole { schema, .. }
        | LogicalPlan::AlterRole { schema, .. }
        | LogicalPlan::DropRole { schema, .. }
        | LogicalPlan::GrantPrivileges { schema, .. }
        | LogicalPlan::RevokePrivileges { schema, .. }
        | LogicalPlan::AlterDefaultPrivileges { schema, .. }
        | LogicalPlan::GrantRole { schema, .. }
        | LogicalPlan::RevokeRole { schema, .. }
        | LogicalPlan::DropTable { schema, .. }
        | LogicalPlan::AlterTable { schema, .. }
        | LogicalPlan::CreateSequence { schema, .. }
        | LogicalPlan::AlterSequence { schema, .. }
        | LogicalPlan::DropSequence { schema, .. }
        | LogicalPlan::Comment { schema, .. }
        | LogicalPlan::Begin { schema, .. }
        | LogicalPlan::Commit { schema }
        | LogicalPlan::Rollback { schema }
        | LogicalPlan::Savepoint { schema, .. }
        | LogicalPlan::RollbackToSavepoint { schema, .. }
        | LogicalPlan::ReleaseSavepoint { schema, .. }
        | LogicalPlan::PrepareTransaction { schema, .. }
        | LogicalPlan::CommitPrepared { schema, .. }
        | LogicalPlan::RollbackPrepared { schema, .. }
        | LogicalPlan::SetTransaction { schema, .. }
        | LogicalPlan::SetVariable { schema, .. }
        | LogicalPlan::SetRole { schema, .. }
        | LogicalPlan::Listen { schema, .. }
        | LogicalPlan::Notify { schema, .. }
        | LogicalPlan::Unlisten { schema, .. }
        | LogicalPlan::Copy { schema, .. }
        | LogicalPlan::Explain { schema, .. } => vec![None; schema.len()],
    }
}

pub(super) fn table_sources(table: &str, schema: &Schema) -> Vec<Option<ColumnSource>> {
    schema
        .fields()
        .iter()
        .map(|field| {
            Some(ColumnSource {
                table: table.to_ascii_lowercase(),
                column: field.name.to_ascii_lowercase(),
            })
        })
        .collect()
}

pub(super) fn target_columns(columns: &[usize], schema: &Schema) -> Vec<usize> {
    if columns.is_empty() {
        (0..schema.len()).collect()
    } else {
        columns.to_vec()
    }
}

pub(super) const fn privilege_name(privilege: PrivilegeKind) -> &'static str {
    match privilege {
        PrivilegeKind::Select => "SELECT",
        PrivilegeKind::Insert => "INSERT",
        PrivilegeKind::Update => "UPDATE",
        PrivilegeKind::Delete => "DELETE",
        PrivilegeKind::Truncate => "TRUNCATE",
        PrivilegeKind::References => "REFERENCES",
        PrivilegeKind::Trigger => "TRIGGER",
        PrivilegeKind::Usage => "USAGE",
        PrivilegeKind::Create => "CREATE",
        PrivilegeKind::Connect => "CONNECT",
        PrivilegeKind::Temporary => "TEMPORARY",
        PrivilegeKind::Execute => "EXECUTE",
    }
}

fn scan_sources(
    table: &str,
    schema: &Schema,
    projection: Option<&[usize]>,
) -> Vec<Option<ColumnSource>> {
    match projection {
        Some(projection) => projection
            .iter()
            .enumerate()
            .map(|(output_index, original_index)| {
                let field = if schema.len() == projection.len() {
                    schema.field(output_index)
                } else {
                    schema.field(*original_index)
                }
                .or_else(|| schema.field(output_index))?;
                Some(ColumnSource {
                    table: table.to_ascii_lowercase(),
                    column: field.name.to_ascii_lowercase(),
                })
            })
            .collect(),
        None => table_sources(table, schema),
    }
}

fn expr_direct_source(expr: &ScalarExpr, sources: &[Option<ColumnSource>]) -> Option<ColumnSource> {
    match expr {
        ScalarExpr::Column { index, .. } => sources.get(*index).cloned().flatten(),
        _ => None,
    }
}

fn join_sources(
    left: Vec<Option<ColumnSource>>,
    right: Vec<Option<ColumnSource>>,
    join_type: LogicalJoinType,
    condition: &LogicalJoinCondition,
    schema: &Schema,
) -> Vec<Option<ColumnSource>> {
    if matches!(join_type, LogicalJoinType::Semi | LogicalJoinType::Anti) {
        return left;
    }
    if schema.len() == left.len() + right.len() {
        return left.into_iter().chain(right).collect();
    }
    if let LogicalJoinCondition::Using(pairs) = condition {
        let mut sources = Vec::with_capacity(schema.len());
        let mut used_right = BTreeSet::new();
        for (left_index, right_index) in pairs {
            sources.push(left.get(*left_index).cloned().flatten());
            used_right.insert(*right_index);
        }
        sources.extend(left.into_iter().enumerate().filter_map(|(index, source)| {
            if pairs.iter().any(|(left_index, _)| *left_index == index) {
                None
            } else {
                Some(source)
            }
        }));
        sources.extend(right.into_iter().enumerate().filter_map(|(index, source)| {
            if used_right.contains(&index) {
                None
            } else {
                Some(source)
            }
        }));
        sources.truncate(schema.len());
        sources.resize_with(schema.len(), || None);
        return sources;
    }
    vec![None; schema.len()]
}
