//! `DROP INDEX` / `DROP TABLE` and `COMMENT` DDL handlers, plus their
//! dependency-tracking helpers. Part of the `session::ddl` module split;
//! reopens the `impl<RW> Session<RW>` block defined in `session/mod.rs`.

use std::collections::HashSet;
use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncWrite};
use ultrasql_catalog::{Catalog, CatalogSnapshot, MutableCatalog};
use ultrasql_core::RelationId;
use ultrasql_planner::{LogicalCommentTarget, LogicalPlan};
use ultrasql_wal::payload::SequenceOpKind;

use super::super::Session;
use super::{log_failed_ddl_rollback, table_entry_lookup_key};
use crate::error::ServerError;
use crate::result_encoder::{SelectResult, run_ddl_command};

impl<RW> Session<RW>
where
    RW: AsyncRead + AsyncWrite + Unpin,
{
    /// Drop one or more indexes and their dependent runtime metadata.
    pub(crate) fn execute_drop_index(
        &self,
        plan: &LogicalPlan,
    ) -> Result<SelectResult, ServerError> {
        let LogicalPlan::DropIndex {
            indexes,
            index_namespaces,
            if_exists,
            ..
        } = plan
        else {
            return Err(ServerError::Unsupported(
                "execute_drop_index called with non-DropIndex plan",
            ));
        };

        let mut entries = Vec::with_capacity(indexes.len());
        for (idx, name) in indexes.iter().enumerate() {
            let namespace = index_namespaces.get(idx).and_then(Option::as_ref);
            let lookup_name = namespace.map_or_else(
                || name.clone(),
                |namespace| ultrasql_catalog::index_lookup_key(namespace, name),
            );
            if let Some(entry) = self.state.persistent_catalog.lookup_index(&lookup_name) {
                let table = self
                    .state
                    .persistent_catalog
                    .lookup_table_by_oid(entry.table_oid);
                if let Some(namespace) = namespace
                    && !entry.schema_name.eq_ignore_ascii_case(namespace)
                {
                    if !*if_exists {
                        return Err(ultrasql_catalog::CatalogError::not_found(lookup_name).into());
                    }
                    continue;
                }
                let table_name = table.map_or_else(
                    || format!("oid {}", entry.table_oid.raw()),
                    |table| table.name,
                );
                self.ensure_table_owner_or_superuser(entry.table_oid, &table_name)?;
                if entry.is_primary {
                    return Err(ServerError::DependentObjectsStillExist(format!(
                        "cannot drop index {} because primary key constraint depends on it",
                        entry.name
                    )));
                }
                if let Some(dependency) = self
                    .state
                    .persistent_catalog
                    .constraint_dependency_for_index(entry.table_oid, &entry.name)
                {
                    return Err(ServerError::DependentObjectsStillExist(format!(
                        "cannot drop index {} because constraint {} depends on it",
                        entry.name, dependency.conname
                    )));
                }
                entries.push(entry);
            } else if !*if_exists {
                return Err(ultrasql_catalog::CatalogError::not_found(lookup_name).into());
            }
        }
        if entries.is_empty() {
            return Ok(run_ddl_command("DROP INDEX"));
        }

        let runtime_metadata_will_be_removed = entries.iter().any(|entry| {
            self.state
                .table_constraints
                .get(&entry.table_oid)
                .is_some_and(|constraints| constraints.indexes.contains_key(&entry.oid))
        });
        if runtime_metadata_will_be_removed {
            self.state
                .ensure_table_runtime_constraints_metadata_slots_persistable()?;
        }

        let ddl_txn = self
            .state
            .txn_manager
            .begin(ultrasql_txn::IsolationLevel::ReadCommitted);
        let ddl_xid = ddl_txn.xid;
        let ddl_command_id = ddl_txn.current_command;
        let persist_result = entries.iter().try_for_each(|entry| {
            self.state.persistent_catalog.persist_index_drop_tombstone(
                entry,
                self.state.heap.as_ref(),
                ddl_xid,
                ddl_command_id,
            )
        });
        if let Err(e) = persist_result {
            return Err(self.rollback_catalog_transaction_after_error(
                ddl_txn,
                e.into(),
                "DROP INDEX catalog rollback after tombstone persist error",
            ));
        }
        self.state
            .commit_transaction(ddl_txn, true, "DROP INDEX catalog transaction")?;

        let mut runtime_metadata_removed = false;
        for entry in entries {
            if let Some(mut constraints) = self
                .state
                .table_constraints
                .get(&entry.table_oid)
                .map(|guard| guard.value().as_ref().clone())
                && constraints.indexes.remove(&entry.oid).is_some()
            {
                self.state
                    .table_constraints
                    .insert(entry.table_oid, Arc::new(constraints));
                runtime_metadata_removed = true;
            }
            self.state
                .persistent_catalog
                .clear_descriptions_for_object(entry.oid);
            self.state
                .persistent_catalog
                .drop_index(&ultrasql_catalog::index_lookup_key(
                    &entry.schema_name,
                    &entry.name,
                ))?;
        }
        if runtime_metadata_removed {
            self.state.persist_table_runtime_constraints_metadata()?;
        }
        self.plan_cache_invalidate();
        Ok(run_ddl_command("DROP INDEX"))
    }

    /// Drop one or more tables.
    ///
    /// The binder has already filtered names through the catalog —
    /// see [`ultrasql_planner::bind`] — so the only failure surface
    /// here is `CatalogError::NotFound`, which can fire only when a
    /// concurrent DDL deleted the relation between the binder and the
    /// dispatcher. Associated indexes are removed by
    /// [`MutableCatalog::drop_table`] in a single atomic snapshot
    /// rotation.
    ///
    /// Heap pages backing the dropped relation are *not* reclaimed in
    /// this wave: the in-memory buffer pool grows on demand and the
    /// segment manager has not yet landed. The dropped name becomes
    /// available immediately for reuse via `CREATE TABLE` — subsequent
    /// inserts will reuse the relation-id space without colliding
    /// because OIDs are monotonic.
    pub(crate) fn execute_drop_table(
        &self,
        plan: &LogicalPlan,
    ) -> Result<SelectResult, ServerError> {
        let LogicalPlan::DropTable {
            tables, cascade, ..
        } = plan
        else {
            return Err(ServerError::Unsupported(
                "execute_drop_table called with non-DropTable plan",
            ));
        };
        let initial_drop_set: HashSet<String> = tables
            .iter()
            .map(|name| name.to_ascii_lowercase())
            .collect();
        let mut drop_set = initial_drop_set.clone();
        let mut drop_names = tables.clone();
        if *cascade {
            for name in self.materialized_view_cascade_drop_names(&mut drop_set) {
                if !drop_names
                    .iter()
                    .any(|existing| existing.eq_ignore_ascii_case(&name))
                {
                    drop_names.push(name);
                }
            }
            for name in self.regular_view_cascade_drop_names(&mut drop_set) {
                if !drop_names
                    .iter()
                    .any(|existing| existing.eq_ignore_ascii_case(&name))
                {
                    drop_names.push(name);
                }
            }
        }
        for name in tables {
            let Some(entry) = self.state.persistent_catalog.lookup_table(name) else {
                continue;
            };
            self.ensure_table_owner_or_superuser(entry.oid, name)?;
            let mut dependents = self.foreign_key_dependents(entry.oid, &drop_set);
            dependents.extend(self.materialized_view_dependents(name, &initial_drop_set));
            dependents.extend(self.regular_view_dependents(name, &initial_drop_set));
            dependents.sort();
            if !dependents.is_empty() && !*cascade {
                return Err(ServerError::DependentObjectsStillExist(format!(
                    "cannot drop table {name} because other objects depend on it: {}",
                    dependents.join(", ")
                )));
            }
        }
        let mut durable_drop_entries = Vec::new();
        for name in &drop_names {
            let Some(entry) = self.state.persistent_catalog.lookup_table(name) else {
                continue;
            };
            durable_drop_entries.push(entry);
            if let Some(runtime) = self.state.time_partitions.get(name) {
                for chunk in runtime.chunks.iter() {
                    if let Some(chunk_entry) = self
                        .state
                        .persistent_catalog
                        .lookup_table(&chunk.value().table_name)
                    {
                        durable_drop_entries.push(chunk_entry);
                    }
                }
            }
        }
        self.state
            .ensure_drop_table_runtime_metadata_slots_persistable(&drop_names)?;
        if !durable_drop_entries.is_empty() {
            let ddl_txn = self
                .state
                .txn_manager
                .begin(ultrasql_txn::IsolationLevel::ReadCommitted);
            let ddl_xid = ddl_txn.xid;
            let ddl_command_id = ddl_txn.current_command;
            let persist_result = durable_drop_entries.iter().try_for_each(|entry| {
                self.state.persistent_catalog.persist_table_drop_tombstone(
                    entry,
                    self.state.heap.as_ref(),
                    ddl_xid,
                    ddl_command_id,
                )
            });
            if let Err(e) = persist_result {
                return Err(self.rollback_catalog_transaction_after_error(
                    ddl_txn,
                    e.into(),
                    "DROP TABLE catalog rollback after tombstone persist error",
                ));
            }
            self.state
                .commit_transaction(ddl_txn, true, "DROP TABLE catalog transaction")?;
        }
        let mut privilege_grants_removed = false;
        let mut sequence_owner_metadata_changed = false;
        for name in &drop_names {
            if let Some(entry) = self.state.persistent_catalog.lookup_table(name) {
                if *cascade {
                    self.drop_foreign_key_dependencies(entry.oid, &drop_set);
                }
                let dependent_index_oids = self
                    .state
                    .persistent_catalog
                    .list_indexes_for_table(entry.oid)
                    .into_iter()
                    .map(|index| index.oid)
                    .collect::<Vec<_>>();
                if let Some((_, runtime)) = self.state.time_partitions.remove(name) {
                    for chunk in runtime.chunks.iter() {
                        log_failed_ddl_rollback(
                            self.state.persistent_catalog.drop_table(&chunk.table_name),
                            "drop table",
                        );
                    }
                }
                self.state.columnar_storage.remove(name);
                let folded_name = name.to_ascii_lowercase();
                self.state.stats_catalog.write().remove(&folded_name);
                self.state
                    .persistent_catalog
                    .replace_statistics(entry.oid, std::iter::empty());
                self.state
                    .persistent_catalog
                    .remove_statistic_ext_for_relation(entry.oid);
                self.state.table_modifications.remove(&folded_name);
                self.state.pending_analyze_tables.remove(&folded_name);
                if let Some((_, constraints)) = self.state.table_constraints.remove(&entry.oid) {
                    for seq_name in constraints.sequence_defaults.iter().flatten() {
                        if let Some(seq) = self.state.sequences.get(seq_name).map(|seq| seq.clone())
                        {
                            seq.emit_wal(
                                SequenceOpKind::Drop,
                                seq_name,
                                RelationId::INVALID,
                                ultrasql_core::Xid::INVALID,
                                self.state.heap.wal_sink().map(|sink| sink.as_ref()),
                            )
                            .map_err(|e| {
                                ServerError::ddl(format!("DROP TABLE owned sequence WAL: {e}"))
                            })?;
                        }
                        let sequence_key = seq_name.to_ascii_lowercase();
                        self.state.sequences.remove(seq_name);
                        self.state.sequence_owners.remove(&sequence_key);
                        self.state.sequence_namespaces.remove(&sequence_key);
                        sequence_owner_metadata_changed = true;
                        self.sequence_state.forget(seq_name);
                        privilege_grants_removed |=
                            self.state.privilege_catalog.remove_object_grants(
                                crate::auth::PrivilegeObjectKind::Sequence,
                                seq_name,
                            );
                    }
                }
                self.state.row_security.remove(&entry.oid);
                privilege_grants_removed |= self
                    .state
                    .privilege_catalog
                    .remove_object_grants(crate::auth::PrivilegeObjectKind::Table, name);
                self.state
                    .persistent_catalog
                    .clear_descriptions_for_object(entry.oid);
                for index_oid in dependent_index_oids {
                    self.state
                        .persistent_catalog
                        .clear_descriptions_for_object(index_oid);
                }
            }
            self.state.materialized_views.remove(name);
            self.state.regular_views.remove(name);
            self.state.persistent_catalog.drop_table(name)?;
        }
        self.state.persist_table_runtime_constraints_metadata()?;
        self.state.persist_row_security_metadata()?;
        if sequence_owner_metadata_changed {
            self.state.persist_sequence_owner_metadata()?;
        }
        if privilege_grants_removed {
            self.state.persist_privilege_metadata()?;
        }
        self.state
            .remove_materialized_view_runtime_metadata(&drop_names)?;
        self.state
            .remove_regular_view_runtime_metadata(&drop_names)?;
        // Any cached plan that referenced this name is now invalid;
        // clear the cache so subsequent statements re-plan.
        self.plan_cache_invalidate();
        Ok(run_ddl_command("DROP TABLE"))
    }

    fn foreign_key_dependents(
        &self,
        target_oid: ultrasql_core::Oid,
        drop_set: &HashSet<String>,
    ) -> Vec<String> {
        let snapshot = self.state.catalog_snapshot();
        let mut out = Vec::new();
        for item in self.state.table_constraints.iter() {
            let table_oid = *item.key();
            let Some(table) = snapshot.tables_by_oid.get(&table_oid) else {
                continue;
            };
            if drop_set.contains(&table_entry_lookup_key(table)) {
                continue;
            }
            for fk in &item.value().foreign_keys {
                if fk.target_oid == target_oid {
                    out.push(format!("{}.{}", table.name, fk.name));
                }
            }
        }
        out.sort();
        out
    }

    fn materialized_view_dependents(
        &self,
        target_table: &str,
        drop_set: &HashSet<String>,
    ) -> Vec<String> {
        let mut out = Vec::new();
        for item in self.state.materialized_views.iter() {
            let runtime = item.value();
            if runtime.source_table.eq_ignore_ascii_case(target_table)
                && !drop_set.contains(&runtime.view_table.to_ascii_lowercase())
            {
                out.push(runtime.view_table.clone());
            }
        }
        out.sort();
        out
    }

    fn materialized_view_cascade_drop_names(&self, drop_set: &mut HashSet<String>) -> Vec<String> {
        let mut out = Vec::new();
        loop {
            let mut changed = false;
            for item in self.state.materialized_views.iter() {
                let runtime = item.value();
                let source = runtime.source_table.to_ascii_lowercase();
                let view = runtime.view_table.to_ascii_lowercase();
                if drop_set.contains(&source) && drop_set.insert(view) {
                    out.push(runtime.view_table.clone());
                    changed = true;
                }
            }
            if !changed {
                break;
            }
        }
        out.sort();
        out
    }

    pub(super) fn regular_view_dependents(
        &self,
        target_table: &str,
        drop_set: &HashSet<String>,
    ) -> Vec<String> {
        let target = target_table.to_ascii_lowercase();
        let mut out = Vec::new();
        for item in self.state.regular_views.iter() {
            let runtime = item.value();
            let view = runtime.view_table.to_ascii_lowercase();
            if drop_set.contains(&view) {
                continue;
            }
            let mut dependencies = HashSet::new();
            collect_plan_scan_tables(&runtime.source, &mut dependencies);
            if dependencies.contains(&target) {
                out.push(runtime.view_table.clone());
            }
        }
        out.sort();
        out
    }

    fn regular_view_cascade_drop_names(&self, drop_set: &mut HashSet<String>) -> Vec<String> {
        let mut out = Vec::new();
        loop {
            let mut changed = false;
            for item in self.state.regular_views.iter() {
                let runtime = item.value();
                let view = runtime.view_table.to_ascii_lowercase();
                if drop_set.contains(&view) {
                    continue;
                }
                let mut dependencies = HashSet::new();
                collect_plan_scan_tables(&runtime.source, &mut dependencies);
                if dependencies
                    .iter()
                    .any(|dependency| drop_set.contains(dependency))
                    && drop_set.insert(view)
                {
                    out.push(runtime.view_table.clone());
                    changed = true;
                }
            }
            if !changed {
                break;
            }
        }
        out.sort();
        out
    }

    fn drop_foreign_key_dependencies(
        &self,
        target_oid: ultrasql_core::Oid,
        drop_set: &HashSet<String>,
    ) {
        let snapshot = self.state.catalog_snapshot();
        let mut updates = Vec::new();
        for item in self.state.table_constraints.iter() {
            let table_oid = *item.key();
            let Some(table) = snapshot.tables_by_oid.get(&table_oid) else {
                continue;
            };
            if drop_set.contains(&table_entry_lookup_key(table)) {
                continue;
            }
            if item
                .value()
                .foreign_keys
                .iter()
                .any(|fk| fk.target_oid == target_oid)
            {
                let mut next = item.value().as_ref().clone();
                next.foreign_keys.retain(|fk| fk.target_oid != target_oid);
                updates.push((table_oid, next));
            }
        }
        for (table_oid, constraints) in updates {
            self.state
                .table_constraints
                .insert(table_oid, Arc::new(constraints));
        }
    }

    pub(crate) fn execute_comment(
        &self,
        plan: &LogicalPlan,
        snapshot: &CatalogSnapshot,
    ) -> Result<SelectResult, ServerError> {
        let LogicalPlan::Comment {
            target, comment, ..
        } = plan
        else {
            return Err(ServerError::Unsupported(
                "execute_comment called with non-Comment plan",
            ));
        };
        let (objoid, objsubid) = match target {
            LogicalCommentTarget::Table { table } => {
                let entry = snapshot
                    .tables
                    .get(table)
                    .ok_or_else(|| ultrasql_catalog::CatalogError::not_found(table.clone()))?;
                self.ensure_table_owner_or_superuser(entry.oid, table)?;
                (entry.oid, 0)
            }
            LogicalCommentTarget::Index { index, namespace } => {
                let entry = snapshot
                    .indexes
                    .get(index)
                    .ok_or_else(|| ultrasql_catalog::CatalogError::not_found(index.clone()))?;
                let table = snapshot.tables_by_oid.get(&entry.table_oid);
                if let Some(namespace) = namespace
                    && !table.is_some_and(|table| table.schema_name.eq_ignore_ascii_case(namespace))
                {
                    return Err(ultrasql_catalog::CatalogError::not_found(index.clone()).into());
                }
                let table_name = table.map_or_else(
                    || format!("oid {}", entry.table_oid.raw()),
                    |table| table.name.clone(),
                );
                self.ensure_table_owner_or_superuser(entry.table_oid, &table_name)?;
                (entry.oid, 0)
            }
            LogicalCommentTarget::Column { table, attnum, .. } => {
                let entry = snapshot
                    .tables
                    .get(table)
                    .ok_or_else(|| ultrasql_catalog::CatalogError::not_found(table.clone()))?;
                self.ensure_table_owner_or_superuser(entry.oid, table)?;
                (entry.oid, *attnum)
            }
        };
        let classoid = ultrasql_core::Oid::new(ultrasql_catalog::bootstrap::PG_CLASS_OID);
        let ddl_txn = self
            .state
            .txn_manager
            .begin(ultrasql_txn::IsolationLevel::ReadCommitted);
        let row = ultrasql_catalog::persistent::DescriptionRow {
            objoid,
            classoid,
            objsubid,
            description: comment.clone().unwrap_or_default(),
        };
        if let Err(e) = self.state.persistent_catalog.persist_description_row(
            &row,
            comment.is_none(),
            self.state.heap.as_ref(),
            ddl_txn.xid,
            ddl_txn.current_command,
        ) {
            return Err(self.rollback_catalog_transaction_after_error(
                ddl_txn,
                e.into(),
                "COMMENT catalog rollback after persist error",
            ));
        }
        self.state
            .commit_transaction(ddl_txn, true, "COMMENT catalog transaction")?;
        self.state
            .persistent_catalog
            .set_description(objoid, classoid, objsubid, comment.clone());
        self.plan_cache_invalidate();
        Ok(run_ddl_command("COMMENT"))
    }
}

fn collect_plan_scan_tables(plan: &LogicalPlan, out: &mut HashSet<String>) {
    match plan {
        LogicalPlan::Scan { table, .. } => {
            out.insert(table.to_ascii_lowercase());
        }
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Project { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Window { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::Pivot { input, .. }
        | LogicalPlan::Unpivot { input, .. }
        | LogicalPlan::LockRows { input, .. } => collect_plan_scan_tables(input, out),
        LogicalPlan::Join { left, right, .. } | LogicalPlan::SetOp { left, right, .. } => {
            collect_plan_scan_tables(left, out);
            collect_plan_scan_tables(right, out);
        }
        LogicalPlan::Cte {
            definition, body, ..
        } => {
            collect_plan_scan_tables(definition, out);
            collect_plan_scan_tables(body, out);
        }
        LogicalPlan::Insert { source, .. } => collect_plan_scan_tables(source, out),
        LogicalPlan::Update { input, .. } | LogicalPlan::Delete { input, .. } => {
            collect_plan_scan_tables(input, out);
        }
        LogicalPlan::Merge { source, .. } => collect_plan_scan_tables(source, out),
        LogicalPlan::Explain { input, .. } => collect_plan_scan_tables(input, out),
        _ => {}
    }
}
