//! Column-level privilege checks for executable logical plans.

use std::collections::BTreeSet;

use tokio::io::{AsyncRead, AsyncWrite};
use ultrasql_catalog::CatalogSnapshot;
use ultrasql_core::Schema;
use ultrasql_planner::{
    Catalog as PlannerCatalog, CopyDirection, LogicalAggregateExpr, LogicalJoinCondition,
    LogicalOnConflict, LogicalPlan, LogicalWindowFunc, ScalarExpr, SortKey,
};

use super::Session;
use super::privilege_sources::{
    ColumnSource, plan_sources, privilege_name, table_sources, target_columns,
};
use crate::auth::{AuthCatalog, PrivilegeKind, PrivilegeObjectKind};
use crate::error::ServerError;

impl<RW> Session<RW>
where
    RW: AsyncRead + AsyncWrite + Unpin,
{
    pub(crate) fn enforce_column_privileges(
        &self,
        plan: &LogicalPlan,
        catalog_snapshot: &CatalogSnapshot,
    ) -> Result<(), ServerError> {
        if self.privilege_bypass() {
            return Ok(());
        }

        let mut collector = ColumnPrivilegeCollector {
            session: self,
            catalog_snapshot,
            requirements: BTreeSet::new(),
        };
        collector.collect_plan(plan, true);
        let roles = self
            .state
            .role_catalog
            .inherited_role_names(&self.current_user);
        for requirement in collector.requirements {
            if !self.state.privilege_catalog.has_column_privilege_for_roles(
                &roles,
                PrivilegeObjectKind::Table,
                &requirement.table,
                &requirement.column,
                requirement.privilege,
            ) {
                return Err(ServerError::InsufficientPrivilege(format!(
                    "{} privilege on column {}.{}",
                    privilege_name(requirement.privilege),
                    requirement.table,
                    requirement.column
                )));
            }
        }
        Ok(())
    }

    fn privilege_bypass(&self) -> bool {
        self.state
            .role_catalog
            .lookup_role(&self.current_user)
            .is_none_or(|role| role.is_superuser)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct ColumnPrivilegeRequirement {
    table: String,
    column: String,
    privilege: PrivilegeKind,
}

struct ColumnPrivilegeCollector<'a, RW> {
    session: &'a Session<RW>,
    catalog_snapshot: &'a CatalogSnapshot,
    requirements: BTreeSet<ColumnPrivilegeRequirement>,
}

impl<RW> ColumnPrivilegeCollector<'_, RW>
where
    RW: AsyncRead + AsyncWrite + Unpin,
{
    fn collect_plan(&mut self, plan: &LogicalPlan, output_observed: bool) {
        match plan {
            LogicalPlan::Scan { .. } => {
                if output_observed {
                    for source in plan_sources(plan).into_iter().flatten() {
                        self.require(source, PrivilegeKind::Select);
                    }
                }
            }
            LogicalPlan::Filter { input, predicate } => {
                let sources = plan_sources(input);
                self.collect_expr(predicate, &sources, PrivilegeKind::Select);
                self.collect_plan(input, output_observed);
            }
            LogicalPlan::Project { input, exprs, .. } => {
                let sources = plan_sources(input);
                for (expr, _) in exprs {
                    self.collect_expr(expr, &sources, PrivilegeKind::Select);
                }
                self.collect_plan(input, false);
            }
            LogicalPlan::Limit { input, .. } | LogicalPlan::LockRows { input, .. } => {
                self.collect_plan(input, output_observed);
            }
            LogicalPlan::Sort { input, keys } => {
                let sources = plan_sources(input);
                self.collect_sort_keys(keys, &sources);
                self.collect_plan(input, output_observed);
            }
            LogicalPlan::Window {
                input,
                partition_by,
                order_by,
                func,
                ..
            } => {
                let sources = plan_sources(input);
                for expr in partition_by {
                    self.collect_expr(expr, &sources, PrivilegeKind::Select);
                }
                self.collect_sort_keys(order_by, &sources);
                self.collect_window_func(func, &sources);
                self.collect_plan(input, output_observed);
            }
            LogicalPlan::Aggregate {
                input,
                group_by,
                aggregates,
                ..
            } => {
                let sources = plan_sources(input);
                for expr in group_by {
                    self.collect_expr(expr, &sources, PrivilegeKind::Select);
                }
                for aggregate in aggregates {
                    self.collect_aggregate(aggregate, &sources);
                }
                self.collect_plan(input, false);
            }
            LogicalPlan::Join {
                left,
                right,
                condition,
                ..
            } => {
                self.collect_join_condition(condition, left, right);
                if output_observed {
                    for source in plan_sources(plan).into_iter().flatten() {
                        self.require(source, PrivilegeKind::Select);
                    }
                }
                self.collect_plan(left, false);
                self.collect_plan(right, false);
            }
            LogicalPlan::SetOp { left, right, .. } => {
                self.collect_plan(left, output_observed);
                self.collect_plan(right, output_observed);
            }
            LogicalPlan::Cte {
                definition, body, ..
            } => {
                self.collect_plan(definition, true);
                self.collect_plan(body, output_observed);
            }
            LogicalPlan::Insert {
                table,
                columns,
                source,
                on_conflict,
                returning,
                ..
            } => {
                if let Some(schema) = self.table_schema(table) {
                    for index in target_columns(columns, &schema) {
                        self.require_table_column(table, &schema, index, PrivilegeKind::Insert);
                    }
                    self.collect_on_conflict(table, &schema, on_conflict.as_ref());
                    self.collect_target_exprs(table, &schema, returning);
                }
                self.collect_plan(source, true);
            }
            LogicalPlan::Update {
                table,
                assignments,
                input,
                returning,
                ..
            } => {
                if let Some(schema) = self.table_schema(table) {
                    let sources = table_sources(table, &schema);
                    for (index, expr) in assignments {
                        self.require_table_column(table, &schema, *index, PrivilegeKind::Update);
                        self.collect_expr(expr, &sources, PrivilegeKind::Select);
                    }
                    self.collect_target_exprs(table, &schema, returning);
                }
                self.collect_plan(input, false);
            }
            LogicalPlan::Delete {
                table,
                input,
                returning,
                ..
            } => {
                if let Some(schema) = self.table_schema(table) {
                    self.collect_target_exprs(table, &schema, returning);
                }
                self.collect_plan(input, false);
            }
            LogicalPlan::CreateMaterializedView { source, .. }
            | LogicalPlan::Explain { input: source, .. } => {
                self.collect_plan(source, true);
            }
            LogicalPlan::Copy {
                relation,
                input,
                columns,
                direction,
                ..
            } => {
                if let Some(source) = input {
                    self.collect_plan(source, true);
                }
                if let Some(table) = relation
                    && let Some(schema) = self.table_schema(table)
                {
                    let privilege = match direction {
                        CopyDirection::From => PrivilegeKind::Insert,
                        CopyDirection::To => PrivilegeKind::Select,
                    };
                    for index in target_columns(columns, &schema) {
                        self.require_table_column(table, &schema, index, privilege);
                    }
                }
            }
            LogicalPlan::Values { rows, .. } => {
                for row in rows {
                    for expr in row {
                        self.collect_expr(expr, &[], PrivilegeKind::Select);
                    }
                }
            }
            LogicalPlan::Empty { .. }
            | LogicalPlan::FunctionScan { .. }
            | LogicalPlan::Truncate { .. }
            | LogicalPlan::CreateTable { .. }
            | LogicalPlan::CreateTypeEnum { .. }
            | LogicalPlan::CreateTypeComposite { .. }
            | LogicalPlan::CreateDomain { .. }
            | LogicalPlan::CreateIndex { .. }
            | LogicalPlan::CreatePolicy { .. }
            | LogicalPlan::CreateRole { .. }
            | LogicalPlan::AlterRole { .. }
            | LogicalPlan::DropRole { .. }
            | LogicalPlan::GrantPrivileges { .. }
            | LogicalPlan::RevokePrivileges { .. }
            | LogicalPlan::AlterDefaultPrivileges { .. }
            | LogicalPlan::GrantRole { .. }
            | LogicalPlan::RevokeRole { .. }
            | LogicalPlan::DropTable { .. }
            | LogicalPlan::AlterTable { .. }
            | LogicalPlan::CreateSequence { .. }
            | LogicalPlan::AlterSequence { .. }
            | LogicalPlan::DropSequence { .. }
            | LogicalPlan::Comment { .. }
            | LogicalPlan::Begin { .. }
            | LogicalPlan::Commit { .. }
            | LogicalPlan::Rollback { .. }
            | LogicalPlan::Savepoint { .. }
            | LogicalPlan::RollbackToSavepoint { .. }
            | LogicalPlan::ReleaseSavepoint { .. }
            | LogicalPlan::PrepareTransaction { .. }
            | LogicalPlan::CommitPrepared { .. }
            | LogicalPlan::RollbackPrepared { .. }
            | LogicalPlan::SetTransaction { .. }
            | LogicalPlan::SetVariable { .. }
            | LogicalPlan::SetRole { .. }
            | LogicalPlan::Listen { .. }
            | LogicalPlan::Notify { .. }
            | LogicalPlan::Unlisten { .. } => {}
        }
    }

    fn collect_on_conflict(
        &mut self,
        table: &str,
        schema: &Schema,
        on_conflict: Option<&LogicalOnConflict>,
    ) {
        let Some(LogicalOnConflict::DoUpdate {
            assignments,
            r#where,
            ..
        }) = on_conflict
        else {
            return;
        };
        let mut sources = table_sources(table, schema);
        sources.extend((0..schema.len()).map(|_| None));
        for (index, expr) in assignments {
            self.require_table_column(table, schema, *index, PrivilegeKind::Update);
            self.collect_expr(expr, &sources, PrivilegeKind::Select);
        }
        if let Some(expr) = r#where {
            self.collect_expr(expr, &sources, PrivilegeKind::Select);
        }
    }

    fn collect_target_exprs(
        &mut self,
        table: &str,
        schema: &Schema,
        exprs: &[(ScalarExpr, String)],
    ) {
        let sources = table_sources(table, schema);
        for (expr, _) in exprs {
            self.collect_expr(expr, &sources, PrivilegeKind::Select);
        }
    }

    fn collect_join_condition(
        &mut self,
        condition: &LogicalJoinCondition,
        left: &LogicalPlan,
        right: &LogicalPlan,
    ) {
        match condition {
            LogicalJoinCondition::On(expr) => {
                let mut sources = plan_sources(left);
                sources.extend(plan_sources(right));
                self.collect_expr(expr, &sources, PrivilegeKind::Select);
            }
            LogicalJoinCondition::Using(pairs) => {
                let left_sources = plan_sources(left);
                let right_sources = plan_sources(right);
                for (left_index, right_index) in pairs {
                    if let Some(Some(source)) = left_sources.get(*left_index) {
                        self.require(source.clone(), PrivilegeKind::Select);
                    }
                    if let Some(Some(source)) = right_sources.get(*right_index) {
                        self.require(source.clone(), PrivilegeKind::Select);
                    }
                }
            }
            LogicalJoinCondition::None => {}
        }
    }

    fn collect_aggregate(
        &mut self,
        aggregate: &LogicalAggregateExpr,
        sources: &[Option<ColumnSource>],
    ) {
        if let Some(expr) = &aggregate.arg {
            self.collect_expr(expr, sources, PrivilegeKind::Select);
        }
        if let Some(expr) = &aggregate.direct_arg {
            self.collect_expr(expr, sources, PrivilegeKind::Select);
        }
        if let Some(key) = &aggregate.order_by {
            self.collect_sort_key(key, sources);
        }
    }

    fn collect_sort_keys(&mut self, keys: &[SortKey], sources: &[Option<ColumnSource>]) {
        for key in keys {
            self.collect_sort_key(key, sources);
        }
    }

    fn collect_sort_key(&mut self, key: &SortKey, sources: &[Option<ColumnSource>]) {
        self.collect_expr(&key.expr, sources, PrivilegeKind::Select);
    }

    fn collect_window_func(&mut self, func: &LogicalWindowFunc, sources: &[Option<ColumnSource>]) {
        match func {
            LogicalWindowFunc::Lag { expr, .. }
            | LogicalWindowFunc::Lead { expr, .. }
            | LogicalWindowFunc::FirstValue(expr)
            | LogicalWindowFunc::LastValue(expr)
            | LogicalWindowFunc::NthValue { expr, .. } => {
                self.collect_expr(expr, sources, PrivilegeKind::Select);
            }
            LogicalWindowFunc::RowNumber
            | LogicalWindowFunc::Rank
            | LogicalWindowFunc::DenseRank
            | LogicalWindowFunc::Ntile(_) => {}
        }
    }

    fn collect_expr(
        &mut self,
        expr: &ScalarExpr,
        sources: &[Option<ColumnSource>],
        privilege: PrivilegeKind,
    ) {
        match expr {
            ScalarExpr::Column { index, .. }
            | ScalarExpr::OuterColumn {
                column_index: index,
                ..
            } => {
                if let Some(Some(source)) = sources.get(*index) {
                    self.require(source.clone(), privilege);
                }
            }
            ScalarExpr::Unary { expr, .. } | ScalarExpr::IsNull { expr, .. } => {
                self.collect_expr(expr, sources, privilege);
            }
            ScalarExpr::Binary { left, right, .. } => {
                self.collect_expr(left, sources, privilege);
                self.collect_expr(right, sources, privilege);
            }
            ScalarExpr::FunctionCall { args, .. } => {
                for arg in args {
                    self.collect_expr(arg, sources, privilege);
                }
            }
            ScalarExpr::ScalarSubquery { subplan, .. } | ScalarExpr::Exists { subplan, .. } => {
                self.collect_plan(subplan, true);
            }
            ScalarExpr::InSubquery { expr, subplan, .. } => {
                self.collect_expr(expr, sources, privilege);
                self.collect_plan(subplan, true);
            }
            ScalarExpr::Literal { .. } | ScalarExpr::Parameter { .. } => {}
        }
    }

    fn table_schema(&self, table: &str) -> Option<Schema> {
        PlannerCatalog::lookup_table(self.catalog_snapshot, table)
            .or_else(|| PlannerCatalog::lookup_table(&self.session.state.catalog, table))
            .map(|meta| meta.schema)
    }

    fn require_table_column(
        &mut self,
        table: &str,
        schema: &Schema,
        index: usize,
        privilege: PrivilegeKind,
    ) {
        if let Some(field) = schema.field(index) {
            self.require(
                ColumnSource {
                    table: table.to_ascii_lowercase(),
                    column: field.name.to_ascii_lowercase(),
                },
                privilege,
            );
        }
    }

    fn require(&mut self, source: ColumnSource, privilege: PrivilegeKind) {
        self.requirements.insert(ColumnPrivilegeRequirement {
            table: source.table,
            column: source.column,
            privilege,
        });
    }
}
