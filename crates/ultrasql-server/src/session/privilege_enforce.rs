//! Column-level privilege checks for executable logical plans.

use std::collections::BTreeSet;

use tokio::io::{AsyncRead, AsyncWrite};
use ultrasql_catalog::{CatalogSnapshot, TableEntry};
use ultrasql_core::Schema;
use ultrasql_planner::{
    Catalog as PlannerCatalog, CopyDirection, LogicalAggregateExpr, LogicalJoinCondition,
    LogicalMergeAction, LogicalOnConflict, LogicalPlan, LogicalWindowFunc, ScalarExpr, SortKey,
};

use super::Session;
use super::privilege_sources::{
    ColumnSource, plan_sources, privilege_name, table_sources, target_columns,
};
use crate::auth::{AuthCatalog, PrivilegeKind, PrivilegeObjectKind};
use crate::builtin_schema_name;
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
            table_requirements: BTreeSet::new(),
            cte_names: Vec::new(),
        };
        collector.collect_plan(plan, true);
        let roles = self
            .state
            .role_catalog
            .inherited_role_names(&self.current_user);
        for requirement in &collector.table_requirements {
            self.ensure_table_schema_usage(&requirement.table, catalog_snapshot, &roles)?;
            if self.owns_table_for_column_privilege(&requirement.table, catalog_snapshot) {
                continue;
            }
            if !self.state.privilege_catalog.has_privilege_for_roles(
                &roles,
                PrivilegeObjectKind::Table,
                &requirement.table,
                requirement.privilege,
            ) {
                return Err(ServerError::InsufficientPrivilege(format!(
                    "{} privilege on table {}",
                    privilege_name(requirement.privilege),
                    requirement.table
                )));
            }
        }
        for requirement in collector.requirements {
            self.ensure_table_schema_usage(&requirement.table, catalog_snapshot, &roles)?;
            if requirement.privilege == PrivilegeKind::Select
                && self.public_catalog_table(&requirement.table, catalog_snapshot)
            {
                continue;
            }
            if self.owns_table_for_column_privilege(&requirement.table, catalog_snapshot) {
                continue;
            }
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

    fn ensure_table_schema_usage(
        &self,
        table: &str,
        catalog_snapshot: &CatalogSnapshot,
        roles: &[String],
    ) -> Result<(), ServerError> {
        let Some(entry) = self.table_entry_for_privilege(table, catalog_snapshot) else {
            return Ok(());
        };
        let schema_name = entry.schema_name.to_ascii_lowercase();
        if builtin_schema_name(&schema_name) {
            return Ok(());
        }
        let current_user = self.current_user.to_ascii_lowercase();
        let owns_schema = self
            .state
            .schemas
            .get(&schema_name)
            .is_some_and(|schema| schema.owner_role.eq_ignore_ascii_case(&current_user));
        if owns_schema {
            return Ok(());
        }
        if self.state.privilege_catalog.has_privilege_for_roles(
            roles,
            PrivilegeObjectKind::Schema,
            &schema_name,
            PrivilegeKind::Usage,
        ) {
            return Ok(());
        }
        Err(ServerError::InsufficientPrivilege(format!(
            "USAGE privilege on schema {schema_name}"
        )))
    }

    fn public_catalog_table(&self, table: &str, catalog_snapshot: &CatalogSnapshot) -> bool {
        if let Some(entry) = self.table_entry_for_privilege(table, catalog_snapshot) {
            return matches!(
                entry.schema_name.as_str(),
                "pg_catalog" | "information_schema"
            );
        }
        crate::pipeline::catalog_views::virtual_catalog_schema(table).is_some()
    }

    fn owns_table_for_column_privilege(
        &self,
        table: &str,
        catalog_snapshot: &CatalogSnapshot,
    ) -> bool {
        let current_user = self.current_user.to_ascii_lowercase();
        if current_user.is_empty() {
            return false;
        }
        let Some(table_oid) = self
            .table_entry_for_privilege(table, catalog_snapshot)
            .map(|entry| entry.oid)
        else {
            return false;
        };
        let Some(runtime) = self.state.row_security.get(&table_oid) else {
            return false;
        };
        !runtime.owner_role.is_empty() && runtime.owner_role.eq_ignore_ascii_case(&current_user)
    }

    fn table_entry_for_privilege<'a>(
        &self,
        table: &str,
        catalog_snapshot: &'a CatalogSnapshot,
    ) -> Option<&'a TableEntry> {
        if let Some(table_oid) = PlannerCatalog::lookup_table_oid(catalog_snapshot, table) {
            return catalog_snapshot.tables_by_oid.get(&table_oid);
        }
        let folded = table.to_ascii_lowercase();
        let (schema, name) = folded.rsplit_once('.')?;
        catalog_snapshot
            .tables
            .get(name)
            .filter(|entry| entry.schema_name.eq_ignore_ascii_case(schema))
    }

    fn privilege_bypass(&self) -> bool {
        self.state
            .role_catalog
            .lookup_role(&self.current_user)
            .is_some_and(|role| role.is_superuser)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct ColumnPrivilegeRequirement {
    table: String,
    column: String,
    privilege: PrivilegeKind,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct TablePrivilegeRequirement {
    table: String,
    privilege: PrivilegeKind,
}

struct ColumnPrivilegeCollector<'a, RW> {
    session: &'a Session<RW>,
    catalog_snapshot: &'a CatalogSnapshot,
    requirements: BTreeSet<ColumnPrivilegeRequirement>,
    table_requirements: BTreeSet<TablePrivilegeRequirement>,
    cte_names: Vec<String>,
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
                name,
                recursive,
                definition,
                body,
                ..
            } => {
                if *recursive {
                    self.cte_names.push(name.to_ascii_lowercase());
                }
                self.collect_plan(definition, true);
                if *recursive {
                    self.cte_names.pop();
                }
                self.cte_names.push(name.to_ascii_lowercase());
                self.collect_plan(body, output_observed);
                self.cte_names.pop();
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
                self.require_table(table, PrivilegeKind::Delete);
                if let Some(schema) = self.table_schema(table) {
                    self.collect_target_exprs(table, &schema, returning);
                }
                self.collect_plan(input, false);
            }
            LogicalPlan::Merge {
                target,
                target_schema,
                source,
                on,
                clauses,
                ..
            } => {
                let mut sources = table_sources(target, target_schema);
                sources.extend(plan_sources(source));
                self.collect_expr(on, &sources, PrivilegeKind::Select);
                for clause in clauses {
                    if let Some(condition) = &clause.condition {
                        self.collect_expr(condition, &sources, PrivilegeKind::Select);
                    }
                    match &clause.action {
                        LogicalMergeAction::Update { assignments } => {
                            for (index, expr) in assignments {
                                self.require_table_column(
                                    target,
                                    target_schema,
                                    *index,
                                    PrivilegeKind::Update,
                                );
                                self.collect_expr(expr, &sources, PrivilegeKind::Select);
                            }
                        }
                        LogicalMergeAction::Delete => {
                            self.require_table(target, PrivilegeKind::Delete);
                        }
                        LogicalMergeAction::Insert { columns, values } => {
                            for index in columns {
                                self.require_table_column(
                                    target,
                                    target_schema,
                                    *index,
                                    PrivilegeKind::Insert,
                                );
                            }
                            for expr in values {
                                self.collect_expr(expr, &sources, PrivilegeKind::Select);
                            }
                        }
                    }
                }
                self.collect_plan(source, false);
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
            | LogicalPlan::CreateOperator { .. }
            | LogicalPlan::CreateIndex { .. }
            | LogicalPlan::DropIndex { .. }
            | LogicalPlan::CreatePolicy { .. }
            | LogicalPlan::CreateRole { .. }
            | LogicalPlan::AlterRole { .. }
            | LogicalPlan::DropRole { .. }
            | LogicalPlan::GrantPrivileges { .. }
            | LogicalPlan::RevokePrivileges { .. }
            | LogicalPlan::AlterDefaultPrivileges { .. }
            | LogicalPlan::GrantRole { .. }
            | LogicalPlan::RevokeRole { .. }
            | LogicalPlan::CreateSchema { .. }
            | LogicalPlan::DropSchema { .. }
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
            | LogicalPlan::Describe { .. }
            | LogicalPlan::Checkpoint { .. }
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
        if self
            .cte_names
            .iter()
            .rev()
            .any(|name| name.eq_ignore_ascii_case(&source.table))
        {
            return;
        }
        self.requirements.insert(ColumnPrivilegeRequirement {
            table: source.table,
            column: source.column,
            privilege,
        });
    }

    fn require_table(&mut self, table: &str, privilege: PrivilegeKind) {
        if self
            .cte_names
            .iter()
            .rev()
            .any(|name| name.eq_ignore_ascii_case(table))
        {
            return;
        }
        self.table_requirements.insert(TablePrivilegeRequirement {
            table: table.to_ascii_lowercase(),
            privilege,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::io::duplex;
    use ultrasql_core::{DataType, Field, Value};
    use ultrasql_planner::{
        AggregateFunc, ConflictTarget, LogicalAggregateExpr, LogicalJoinType, LogicalSetOp,
        LogicalSetQuantifier, LogicalWindowFunc,
    };

    fn users_schema() -> Schema {
        Schema::new([
            Field::required("id", DataType::Int32),
            Field::nullable("name", DataType::Text { max_len: None }),
            Field::nullable("score", DataType::Float64),
        ])
        .expect("schema")
    }

    fn scan(table: &str) -> LogicalPlan {
        LogicalPlan::Scan {
            table: table.to_owned(),
            schema: users_schema(),
            projection: None,
        }
    }

    fn col(name: &str, index: usize, data_type: DataType) -> ScalarExpr {
        ScalarExpr::Column {
            name: name.to_owned(),
            index,
            data_type,
        }
    }

    fn id_col() -> ScalarExpr {
        col("id", 0, DataType::Int32)
    }

    fn name_col() -> ScalarExpr {
        col("name", 1, DataType::Text { max_len: None })
    }

    fn req(column: &str, privilege: PrivilegeKind) -> ColumnPrivilegeRequirement {
        ColumnPrivilegeRequirement {
            table: "users".to_owned(),
            column: column.to_owned(),
            privilege,
        }
    }

    fn collector<'a>(
        session: &'a Session<tokio::io::DuplexStream>,
        snapshot: &'a CatalogSnapshot,
    ) -> ColumnPrivilegeCollector<'a, tokio::io::DuplexStream> {
        ColumnPrivilegeCollector {
            session,
            catalog_snapshot: snapshot,
            requirements: BTreeSet::new(),
            table_requirements: BTreeSet::new(),
            cte_names: Vec::new(),
        }
    }

    #[test]
    fn missing_role_does_not_bypass_column_privileges() {
        let server = Arc::new(crate::Server::with_sample_database());
        let (io, _peer) = duplex(64);
        let session = Session::new(io, server);

        assert!(!session.privilege_bypass());
    }

    #[test]
    fn sample_database_grants_public_select_on_users() {
        let server = crate::Server::with_sample_database();

        assert!(
            server.privilege_catalog.has_column_privilege(
                "tester",
                PrivilegeObjectKind::Table,
                "users",
                "id",
                PrivilegeKind::Select,
            ),
            "sample database must remain readable for trust-auth demo users"
        );
    }

    #[test]
    fn catalog_tables_are_publicly_selectable() {
        let server = Arc::new(crate::Server::with_sample_database());
        let snapshot = server.catalog_snapshot();
        let (io, _peer) = duplex(64);
        let session = Session::new(io, server);

        assert!(session.public_catalog_table("pg_catalog.pg_class", &snapshot));
        assert!(session.public_catalog_table("pg_collation", &snapshot));
        assert!(!session.public_catalog_table("users", &snapshot));
    }

    #[test]
    fn collector_tracks_dml_copy_window_and_subquery_privilege_sources() {
        let server = Arc::new(crate::Server::with_sample_database());
        let snapshot = server.catalog_snapshot();
        let (io, _peer) = duplex(64);
        let mut session = Session::new(io, Arc::clone(&server));
        session.current_user = "ultrasql".to_owned();
        let mut collector = collector(&session, &snapshot);

        let insert = LogicalPlan::Insert {
            table: "users".to_owned(),
            columns: vec![0, 1],
            source: Box::new(LogicalPlan::Values {
                rows: vec![vec![
                    ScalarExpr::Literal {
                        value: Value::Int32(1),
                        data_type: DataType::Int32,
                    },
                    ScalarExpr::Literal {
                        value: Value::Text("a".to_owned()),
                        data_type: DataType::Text { max_len: None },
                    },
                ]],
                schema: Schema::new([
                    Field::required("id", DataType::Int32),
                    Field::nullable("name", DataType::Text { max_len: None }),
                ])
                .expect("values schema"),
            }),
            on_conflict: Some(ultrasql_planner::LogicalOnConflict::DoUpdate {
                target: ConflictTarget { columns: vec![0] },
                assignments: vec![(1, id_col())],
                r#where: Some(name_col()),
            }),
            returning: vec![(name_col(), "name".to_owned())],
            schema: Schema::new([Field::nullable("name", DataType::Text { max_len: None })])
                .expect("returning schema"),
        };
        collector.collect_plan(&insert, true);

        let update = LogicalPlan::Update {
            table: "users".to_owned(),
            assignments: vec![(2, id_col())],
            input: Box::new(LogicalPlan::Filter {
                input: Box::new(scan("users")),
                predicate: ScalarExpr::Binary {
                    op: ultrasql_planner::BinaryOp::Eq,
                    left: Box::new(id_col()),
                    right: Box::new(ScalarExpr::Literal {
                        value: Value::Int32(1),
                        data_type: DataType::Int32,
                    }),
                    data_type: DataType::Bool,
                },
            }),
            returning: vec![(name_col(), "name".to_owned())],
            schema: Schema::empty(),
        };
        collector.collect_plan(&update, true);

        let window = LogicalPlan::Window {
            input: Box::new(scan("users")),
            partition_by: vec![name_col()],
            order_by: vec![SortKey {
                expr: id_col(),
                asc: true,
                nulls_first: false,
            }],
            func: LogicalWindowFunc::Lag {
                expr: ScalarExpr::InSubquery {
                    expr: Box::new(id_col()),
                    subplan: Box::new(scan("users")),
                    negated: false,
                    correlated: true,
                    data_type: DataType::Int32,
                },
                offset: 1,
                default: Value::Null,
            },
            output_name: "lag".to_owned(),
            schema: users_schema(),
        };
        collector.collect_plan(&window, true);

        let aggregate = LogicalPlan::Aggregate {
            input: Box::new(scan("users")),
            group_by: vec![name_col()],
            aggregates: vec![LogicalAggregateExpr {
                func: AggregateFunc::PercentileCont,
                arg: Some(id_col()),
                direct_arg: Some(ScalarExpr::Literal {
                    value: Value::Float64(0.5),
                    data_type: DataType::Float64,
                }),
                order_by: Some(SortKey {
                    expr: id_col(),
                    asc: true,
                    nulls_first: false,
                }),
                distinct: false,
                output_name: "p".to_owned(),
                data_type: DataType::Float64,
            }],
            schema: Schema::empty(),
        };
        collector.collect_plan(&aggregate, true);

        let copy_to = LogicalPlan::Copy {
            relation: Some("users".to_owned()),
            input: None,
            columns: vec![0],
            direction: CopyDirection::To,
            source: ultrasql_planner::CopySource::Stdout,
            format: ultrasql_planner::CopyFormat::Text,
            delimiter: '\t',
            null_str: "\\N".to_owned(),
            header: false,
            auto_detect: false,
            ignore_errors: false,
            max_errors: 0,
            reject_table: None,
            schema: Schema::empty(),
        };
        collector.collect_plan(&copy_to, true);

        assert!(
            collector
                .requirements
                .contains(&req("id", PrivilegeKind::Insert))
        );
        assert!(
            collector
                .requirements
                .contains(&req("name", PrivilegeKind::Insert))
        );
        assert!(
            collector
                .requirements
                .contains(&req("name", PrivilegeKind::Update))
        );
        assert!(
            collector
                .requirements
                .contains(&req("score", PrivilegeKind::Update))
        );
        assert!(
            collector
                .requirements
                .contains(&req("id", PrivilegeKind::Select))
        );
        assert!(
            collector
                .requirements
                .contains(&req("name", PrivilegeKind::Select))
        );
    }

    #[test]
    fn collector_handles_join_setop_and_noop_shapes_without_requirements() {
        let server = Arc::new(crate::Server::with_sample_database());
        let snapshot = server.catalog_snapshot();
        let (io, _peer) = duplex(64);
        let mut session = Session::new(io, Arc::clone(&server));
        session.current_user = "ultrasql".to_owned();
        let mut collector = collector(&session, &snapshot);

        let join = LogicalPlan::Join {
            left: Box::new(scan("users")),
            right: Box::new(scan("users")),
            join_type: LogicalJoinType::Inner,
            condition: LogicalJoinCondition::Using(vec![(0, 0)]),
            schema: users_schema(),
        };
        let set_op = LogicalPlan::SetOp {
            op: LogicalSetOp::Union,
            quantifier: LogicalSetQuantifier::All,
            left: Box::new(join),
            right: Box::new(LogicalPlan::Empty {
                schema: users_schema(),
            }),
            schema: users_schema(),
        };
        let cte = LogicalPlan::Cte {
            name: "u".to_owned(),
            recursive: false,
            definition: Box::new(scan("users")),
            body: Box::new(set_op),
            schema: users_schema(),
        };
        collector.collect_plan(&cte, true);
        collector.collect_plan(
            &LogicalPlan::FunctionScan {
                name: "generate_series".to_owned(),
                args: Vec::new(),
                schema: Schema::empty(),
            },
            true,
        );

        assert!(
            collector
                .requirements
                .contains(&req("id", PrivilegeKind::Select))
        );
        assert!(
            collector
                .requirements
                .contains(&req("name", PrivilegeKind::Select))
        );
        assert!(
            collector
                .requirements
                .iter()
                .all(|requirement| requirement.table != "u"),
            "CTE aliases must not become table privilege requirements"
        );
        assert!(session.privilege_bypass());
    }
}
