//! `CREATE [MATERIALIZED] VIEW` and `ALTER VIEW` DDL handlers. Part of
//! the `session::ddl` module split; reopens the `impl<RW> Session<RW>`
//! block defined in `session/mod.rs`.

use std::collections::HashSet;
use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncWrite};
use ultrasql_catalog::{CatalogSnapshot, MutableCatalog, TableEntry};
use ultrasql_planner::{Catalog as PlannerCatalog, LogicalAlterViewAction, LogicalPlan};

use super::super::Session;
use super::log_failed_ddl_rollback;
use crate::error::ServerError;
use crate::result_encoder::{SelectResult, run_ddl_command};

impl<RW> Session<RW>
where
    RW: AsyncRead + AsyncWrite + Unpin,
{
    /// Persist and populate an append-only materialized view.
    pub(crate) fn execute_create_materialized_view(
        &mut self,
        plan: &LogicalPlan,
        snapshot: &CatalogSnapshot,
    ) -> Result<SelectResult, ServerError> {
        let LogicalPlan::CreateMaterializedView {
            table_name,
            namespace,
            columns,
            source,
            if_not_exists,
            ..
        } = plan
        else {
            return Err(ServerError::Unsupported(
                "execute_create_materialized_view called with wrong plan",
            ));
        };
        self.ensure_schema_exists(namespace)?;
        self.ensure_schema_create_privilege(namespace)?;
        let exists_persistent = snapshot
            .tables
            .contains_key(&ultrasql_catalog::table_lookup_key(namespace, table_name));
        let exists_fallback = self
            .state
            .catalog
            .lookup_table_in_schema(namespace, table_name)
            .is_some();
        if exists_persistent || exists_fallback {
            if *if_not_exists {
                return Ok(run_ddl_command("CREATE MATERIALIZED VIEW"));
            }
            return Err(ServerError::Catalog(
                ultrasql_catalog::CatalogError::already_exists(table_name.clone()),
            ));
        }
        let Some(source_table) = crate::append_only_materialized_source_table(source) else {
            return Err(ServerError::Unsupported(
                "CREATE MATERIALIZED VIEW supports append-only SELECT/FILTER/PROJECT over one table",
            ));
        };

        let oid = self.state.persistent_catalog.next_oid();
        let mut entry =
            TableEntry::new(oid, table_name.clone(), namespace.clone(), columns.clone());
        entry.options.push((
            "ultrasql.relkind".to_owned(),
            "materialized_view".to_owned(),
        ));
        let view_table = ultrasql_catalog::table_lookup_key(namespace, table_name);
        let runtime = Arc::new(crate::MaterializedViewRuntime {
            view_table: view_table.clone(),
            source_table: source_table.to_ascii_lowercase(),
            source: source.as_ref().clone(),
            materialized_rows: std::sync::atomic::AtomicU64::new(0),
        });
        self.state
            .ensure_materialized_view_runtime_metadata_persistable(&runtime)?;
        self.state
            .ensure_create_relation_metadata_slots_persistable()?;
        self.state.persistent_catalog.create_table(entry.clone())?;
        let attr_has_defaults = vec![false; columns.len()];
        let ddl_txn = self
            .state
            .txn_manager
            .begin(ultrasql_txn::IsolationLevel::ReadCommitted);
        let ddl_xid = ddl_txn.xid;
        let ddl_command_id = ddl_txn.current_command;
        let materialized_rows = (|| -> Result<u64, ServerError> {
            self.state
                .persistent_catalog
                .persist_relation_rows_with_defaults(
                    &entry,
                    ultrasql_catalog::persistent::RelKind::MaterializedView,
                    &attr_has_defaults,
                    self.state.heap.as_ref(),
                    ddl_xid,
                    ddl_command_id,
                )?;
            self.materialize_view_delta(&runtime, &ddl_txn)
        })();
        let materialized_rows = match materialized_rows {
            Ok(rows) => rows,
            Err(e) => {
                log_failed_ddl_rollback(
                    self.state.persistent_catalog.drop_table(&view_table),
                    "drop table",
                );
                return Err(self.rollback_catalog_transaction_after_error(
                    ddl_txn,
                    e,
                    "CREATE MATERIALIZED VIEW rollback after materialization error",
                ));
            }
        };
        if let Err(e) = self
            .state
            .persist_materialized_view_runtime_metadata(&runtime, materialized_rows)
        {
            log_failed_ddl_rollback(
                self.state.persistent_catalog.drop_table(&view_table),
                "drop table",
            );
            return Err(self.rollback_catalog_transaction_after_error(
                ddl_txn,
                e,
                "CREATE MATERIALIZED VIEW rollback after runtime metadata error",
            ));
        }
        self.state.commit_transaction(
            ddl_txn,
            true,
            "CREATE MATERIALIZED VIEW catalog transaction",
        )?;
        let mut row_security = self
            .state
            .row_security
            .get(&oid)
            .map(|guard| guard.as_ref().clone())
            .unwrap_or_default();
        if row_security.owner_role.is_empty() {
            row_security.owner_role = self.current_user.to_ascii_lowercase();
        }
        self.state.row_security.insert(oid, Arc::new(row_security));
        self.state.persist_row_security_metadata()?;
        let before_grants = self.state.privilege_catalog.list_grants();
        let before_default_grants = self.state.privilege_catalog.list_default_grants();
        self.state.privilege_catalog.apply_default_privileges(
            &self.current_user,
            namespace,
            crate::auth::PrivilegeObjectKind::Table,
            table_name,
        );
        let grants_changed = before_grants != self.state.privilege_catalog.list_grants()
            || before_default_grants != self.state.privilege_catalog.list_default_grants();
        if grants_changed && let Err(err) = self.state.persist_privilege_metadata() {
            self.state
                .privilege_catalog
                .install_snapshot(before_grants, before_default_grants);
            return Err(err);
        }
        runtime
            .materialized_rows
            .store(materialized_rows, std::sync::atomic::Ordering::Release);
        self.state
            .note_table_modifications(&runtime.view_table, materialized_rows);
        self.state
            .materialized_views
            .insert(runtime.view_table.clone(), runtime);
        self.plan_cache_invalidate();
        Ok(run_ddl_command("CREATE MATERIALIZED VIEW"))
    }

    /// Persist a regular view definition and publish its catalog metadata.
    pub(crate) fn execute_create_view(
        &mut self,
        plan: &LogicalPlan,
        snapshot: &CatalogSnapshot,
    ) -> Result<SelectResult, ServerError> {
        let LogicalPlan::CreateView {
            table_name,
            namespace,
            columns,
            source,
            source_sql,
            or_replace,
            ..
        } = plan
        else {
            return Err(ServerError::Unsupported(
                "execute_create_view called with wrong plan",
            ));
        };
        self.ensure_schema_exists(namespace)?;
        self.ensure_schema_create_privilege(namespace)?;
        let view_key = ultrasql_catalog::table_lookup_key(namespace, table_name);
        let exists_persistent = snapshot.tables.contains_key(&view_key);
        let exists_fallback = self
            .state
            .catalog
            .lookup_table_in_schema(namespace, table_name)
            .is_some();
        if exists_persistent || exists_fallback {
            if *or_replace {
                return Err(ServerError::Unsupported(
                    "CREATE OR REPLACE VIEW is not supported; use DROP TABLE on the view name then CREATE VIEW",
                ));
            }
            return Err(ServerError::Catalog(
                ultrasql_catalog::CatalogError::already_exists(table_name.clone()),
            ));
        }
        if !crate::view_source_shape_matches(source.schema(), columns) {
            return Err(ServerError::Ddl(format!(
                "CREATE VIEW {view_key}: view column list does not match source query"
            )));
        }

        let oid = self.state.persistent_catalog.next_oid();
        let mut entry =
            TableEntry::new(oid, table_name.clone(), namespace.clone(), columns.clone());
        entry
            .options
            .push(("ultrasql.relkind".to_owned(), "view".to_owned()));
        let runtime = Arc::new(crate::RegularViewRuntime {
            view_table: view_key.clone(),
            source_sql: source_sql.clone(),
            search_path: self.session_settings.get("search_path").cloned(),
            source: source.as_ref().clone(),
            columns: columns.clone(),
        });
        self.state
            .ensure_create_relation_metadata_slots_persistable()?;
        self.state
            .ensure_regular_view_runtime_metadata_slots_persistable()?;
        self.state.persistent_catalog.create_table(entry.clone())?;
        let attr_has_defaults = vec![false; columns.len()];
        let ddl_txn = self
            .state
            .txn_manager
            .begin(ultrasql_txn::IsolationLevel::ReadCommitted);
        let persist_result = self
            .state
            .persistent_catalog
            .persist_relation_rows_with_defaults(
                &entry,
                ultrasql_catalog::persistent::RelKind::View,
                &attr_has_defaults,
                self.state.heap.as_ref(),
                ddl_txn.xid,
                ddl_txn.current_command,
            );
        if let Err(e) = persist_result {
            log_failed_ddl_rollback(
                self.state.persistent_catalog.drop_table(&view_key),
                "drop table",
            );
            return Err(self.rollback_catalog_transaction_after_error(
                ddl_txn,
                e.into(),
                "CREATE VIEW rollback after catalog persist error",
            ));
        }
        if let Err(e) = self.state.persist_regular_view_runtime_metadata(&runtime) {
            log_failed_ddl_rollback(
                self.state.persistent_catalog.drop_table(&view_key),
                "drop table",
            );
            return Err(self.rollback_catalog_transaction_after_error(
                ddl_txn,
                e,
                "CREATE VIEW rollback after runtime metadata error",
            ));
        }
        self.state
            .commit_transaction(ddl_txn, true, "CREATE VIEW catalog transaction")?;
        let mut row_security = self
            .state
            .row_security
            .get(&oid)
            .map(|guard| guard.as_ref().clone())
            .unwrap_or_default();
        if row_security.owner_role.is_empty() {
            row_security.owner_role = self.current_user.to_ascii_lowercase();
        }
        self.state.row_security.insert(oid, Arc::new(row_security));
        self.state.persist_row_security_metadata()?;
        let before_grants = self.state.privilege_catalog.list_grants();
        let before_default_grants = self.state.privilege_catalog.list_default_grants();
        self.state.privilege_catalog.apply_default_privileges(
            &self.current_user,
            namespace,
            crate::auth::PrivilegeObjectKind::Table,
            table_name,
        );
        let grants_changed = before_grants != self.state.privilege_catalog.list_grants()
            || before_default_grants != self.state.privilege_catalog.list_default_grants();
        if grants_changed && let Err(err) = self.state.persist_privilege_metadata() {
            self.state
                .privilege_catalog
                .install_snapshot(before_grants, before_default_grants);
            return Err(err);
        }
        self.state.regular_views.insert(view_key, runtime);
        self.plan_cache_invalidate();
        Ok(run_ddl_command("CREATE VIEW"))
    }

    /// Execute metadata-only `ALTER VIEW` actions.
    pub(crate) fn execute_alter_view(
        &mut self,
        plan: &LogicalPlan,
        snapshot: &CatalogSnapshot,
    ) -> Result<SelectResult, ServerError> {
        let LogicalPlan::AlterView {
            view_name, action, ..
        } = plan
        else {
            return Err(ServerError::Unsupported(
                "execute_alter_view called with wrong plan",
            ));
        };
        let entry = snapshot.tables.get(view_name).ok_or_else(|| {
            ServerError::Plan(ultrasql_planner::PlanError::TableNotFound(
                view_name.to_owned(),
            ))
        })?;
        if !crate::is_regular_view_entry(entry) {
            return Err(ServerError::ddl(format!("{view_name} is not a view")));
        }
        self.ensure_table_owner_or_superuser(entry.oid, view_name)?;
        let dependents = self.regular_view_dependents(view_name, &HashSet::new());
        if !dependents.is_empty() {
            return Err(ServerError::DependentObjectsStillExist(format!(
                "cannot alter view {view_name} because other views depend on it: {}",
                dependents.join(", ")
            )));
        }
        match action {
            LogicalAlterViewAction::RenameView { new_name } => {
                self.execute_alter_view_rename(view_name, new_name)
            }
            LogicalAlterViewAction::SetSchema { new_schema } => {
                self.execute_alter_view_set_schema(view_name, new_schema)
            }
        }
    }

    fn execute_alter_view_rename(
        &mut self,
        old_key: &str,
        new_name: &str,
    ) -> Result<SelectResult, ServerError> {
        self.state
            .ensure_create_relation_metadata_slots_persistable()?;
        self.state
            .ensure_regular_view_runtime_metadata_slots_persistable()?;
        let runtime = self
            .state
            .regular_views
            .get(old_key)
            .map(|guard| Arc::clone(guard.value()))
            .ok_or_else(|| {
                ServerError::ddl(format!("missing view runtime metadata for {old_key}"))
            })?;
        let before_grants = self.state.privilege_catalog.list_grants();
        let before_default_grants = self.state.privilege_catalog.list_default_grants();
        let updated_entry = self
            .state
            .persistent_catalog
            .alter_table_rename(old_key, new_name)
            .map_err(ServerError::Catalog)?;
        let new_key = ultrasql_catalog::table_lookup_key(&updated_entry.schema_name, new_name);
        let updated_runtime = Arc::new(crate::RegularViewRuntime {
            view_table: new_key.clone(),
            source_sql: runtime.source_sql.clone(),
            search_path: runtime.search_path.clone(),
            source: runtime.source.clone(),
            columns: runtime.columns.clone(),
        });
        let txn = self
            .state
            .txn_manager
            .begin(ultrasql_txn::IsolationLevel::ReadCommitted);
        if let Err(e) = self
            .state
            .persistent_catalog
            .persist_relation_rows_with_defaults(
                &updated_entry,
                ultrasql_catalog::persistent::RelKind::View,
                &vec![false; updated_entry.schema.len()],
                self.state.heap.as_ref(),
                txn.xid,
                txn.current_command,
            )
        {
            return Err(self.rollback_catalog_transaction_after_error(
                txn,
                e.into(),
                "ALTER VIEW RENAME catalog rollback after persist error",
            ));
        }
        if let Err(e) = self
            .state
            .persist_regular_view_runtime_metadata(&updated_runtime)
        {
            return Err(self.rollback_catalog_transaction_after_error(
                txn,
                e,
                "ALTER VIEW RENAME rollback after runtime metadata error",
            ));
        }
        self.state
            .commit_transaction(txn, true, "ALTER VIEW RENAME catalog transaction")?;
        self.state.regular_views.remove(old_key);
        self.state
            .regular_views
            .insert(new_key.clone(), updated_runtime);
        self.state.persist_row_security_metadata()?;
        let grants_changed = self.state.privilege_catalog.rename_object_grants(
            crate::auth::PrivilegeObjectKind::Table,
            old_key,
            &new_key,
        );
        if grants_changed && let Err(err) = self.state.persist_privilege_metadata() {
            self.state
                .privilege_catalog
                .install_snapshot(before_grants, before_default_grants);
            return Err(err);
        }
        self.plan_cache_invalidate();
        Ok(run_ddl_command("ALTER VIEW"))
    }

    fn execute_alter_view_set_schema(
        &mut self,
        old_key: &str,
        new_schema: &str,
    ) -> Result<SelectResult, ServerError> {
        self.ensure_schema_exists(new_schema)?;
        self.ensure_schema_create_privilege(new_schema)?;
        self.state
            .ensure_create_relation_metadata_slots_persistable()?;
        self.state
            .ensure_regular_view_runtime_metadata_slots_persistable()?;
        let runtime = self
            .state
            .regular_views
            .get(old_key)
            .map(|guard| Arc::clone(guard.value()))
            .ok_or_else(|| {
                ServerError::ddl(format!("missing view runtime metadata for {old_key}"))
            })?;
        let before_grants = self.state.privilege_catalog.list_grants();
        let before_default_grants = self.state.privilege_catalog.list_default_grants();
        let updated_entry = self
            .state
            .persistent_catalog
            .alter_relation_set_schema(old_key, new_schema)
            .map_err(ServerError::Catalog)?;
        let new_key = ultrasql_catalog::table_lookup_key(new_schema, &updated_entry.name);
        let updated_runtime = Arc::new(crate::RegularViewRuntime {
            view_table: new_key.clone(),
            source_sql: runtime.source_sql.clone(),
            search_path: runtime.search_path.clone(),
            source: runtime.source.clone(),
            columns: runtime.columns.clone(),
        });
        let txn = self
            .state
            .txn_manager
            .begin(ultrasql_txn::IsolationLevel::ReadCommitted);
        if let Err(e) = self
            .state
            .persistent_catalog
            .persist_relation_rows_with_defaults(
                &updated_entry,
                ultrasql_catalog::persistent::RelKind::View,
                &vec![false; updated_entry.schema.len()],
                self.state.heap.as_ref(),
                txn.xid,
                txn.current_command,
            )
        {
            return Err(self.rollback_catalog_transaction_after_error(
                txn,
                e.into(),
                "ALTER VIEW SET SCHEMA catalog rollback after persist error",
            ));
        }
        if let Err(e) = self
            .state
            .persist_regular_view_runtime_metadata(&updated_runtime)
        {
            return Err(self.rollback_catalog_transaction_after_error(
                txn,
                e,
                "ALTER VIEW SET SCHEMA rollback after runtime metadata error",
            ));
        }
        self.state
            .commit_transaction(txn, true, "ALTER VIEW SET SCHEMA catalog transaction")?;
        self.state.regular_views.remove(old_key);
        self.state
            .regular_views
            .insert(new_key.clone(), updated_runtime);
        self.state.persist_row_security_metadata()?;
        let grants_changed = self.state.privilege_catalog.rename_object_grants(
            crate::auth::PrivilegeObjectKind::Table,
            old_key,
            &new_key,
        );
        if grants_changed && let Err(err) = self.state.persist_privilege_metadata() {
            self.state
                .privilege_catalog
                .install_snapshot(before_grants, before_default_grants);
            return Err(err);
        }
        self.plan_cache_invalidate();
        Ok(run_ddl_command("ALTER VIEW"))
    }
}
