//! Schema DDL execution.

use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncWrite};
use ultrasql_core::{RelationId, Schema};
use ultrasql_planner::LogicalPlan;
use ultrasql_storage::sequence::Sequence;
use ultrasql_wal::payload::SequenceOpKind;

use super::Session;
use crate::auth::PrivilegeObjectKind;
use crate::error::ServerError;
use crate::result_encoder::{self, SelectResult};
use crate::{RuntimeSchema, builtin_schema_name};

struct DropSchemaRuntimeSnapshot {
    schemas: Vec<(String, Arc<RuntimeSchema>)>,
    sequences: Vec<(String, Arc<Sequence>)>,
    sequence_owners: Vec<(String, String)>,
    sequence_namespaces: Vec<(String, String)>,
    sequence_session: crate::SequenceSessionSnapshot,
    privilege_grants: Vec<crate::auth::PrivilegeGrant>,
    default_privilege_grants: Vec<crate::auth::DefaultPrivilegeGrant>,
}

impl<RW> Session<RW>
where
    RW: AsyncRead + AsyncWrite + Unpin,
{
    pub(crate) fn ensure_schema_exists(&self, schema_name: &str) -> Result<(), ServerError> {
        let folded = schema_name.to_ascii_lowercase();
        if builtin_schema_name(&folded) || self.state.schemas.contains_key(&folded) {
            return Ok(());
        }
        Err(ServerError::UndefinedSchema(format!(
            "schema \"{schema_name}\" does not exist"
        )))
    }

    pub(crate) fn execute_create_schema(
        &self,
        plan: &LogicalPlan,
    ) -> Result<SelectResult, ServerError> {
        let LogicalPlan::CreateSchema {
            schema_name,
            if_not_exists,
            ..
        } = plan
        else {
            return Err(ServerError::Unsupported(
                "execute_create_schema called with non-CreateSchema plan",
            ));
        };
        if builtin_schema_name(schema_name) || self.state.schemas.contains_key(schema_name) {
            if *if_not_exists {
                return Ok(result_encoder::run_ddl_command("CREATE SCHEMA"));
            }
            return Err(ServerError::ddl(format!(
                "schema '{schema_name}' already exists"
            )));
        }
        let schema = RuntimeSchema {
            name: schema_name.clone(),
            owner_role: self.current_user.to_ascii_lowercase(),
        };
        self.state
            .schemas
            .insert(schema_name.clone(), Arc::new(schema));
        if let Err(err) = self.state.persist_schema_metadata() {
            self.state.schemas.remove(schema_name);
            return Err(err);
        }
        self.plan_cache_invalidate();
        Ok(result_encoder::run_ddl_command("CREATE SCHEMA"))
    }

    pub(crate) fn execute_drop_schema(
        &mut self,
        plan: &LogicalPlan,
    ) -> Result<SelectResult, ServerError> {
        let LogicalPlan::DropSchema {
            schemas,
            if_exists,
            cascade,
            ..
        } = plan
        else {
            return Err(ServerError::Unsupported(
                "execute_drop_schema called with non-DropSchema plan",
            ));
        };
        let mut drop_set = Vec::new();
        for name in schemas {
            if builtin_schema_name(name) {
                return Err(ServerError::ddl(format!(
                    "cannot drop built-in schema '{name}'"
                )));
            }
            if self.state.schemas.contains_key(name) {
                drop_set.push(name.clone());
            } else if !*if_exists {
                return Err(ServerError::ddl(format!("schema '{name}' does not exist")));
            }
        }
        if drop_set.is_empty() {
            return Ok(result_encoder::run_ddl_command("DROP SCHEMA"));
        }
        for name in &drop_set {
            self.ensure_schema_owner_or_superuser(name)?;
        }
        for name in &drop_set {
            let dependents = self.schema_dependents(name);
            let dependents = if *cascade {
                dependents
                    .into_iter()
                    .filter(|dependent| {
                        !dependent.starts_with("sequence ") && !dependent.starts_with("table ")
                    })
                    .collect::<Vec<_>>()
            } else {
                dependents
            };
            if !dependents.is_empty() {
                let action = if *cascade { "cascade" } else { "restrict" };
                return Err(ServerError::DependentObjectsStillExist(format!(
                    "cannot drop schema {name} with {action} because other objects depend on it: {}",
                    dependents.join(", ")
                )));
            }
        }
        let mut cascade_sequence_names = Vec::new();
        let mut cascade_table_names = Vec::new();
        if *cascade {
            for name in &drop_set {
                cascade_table_names.extend(self.schema_table_names(name)?);
                cascade_sequence_names.extend(self.schema_sequence_names(name));
            }
            cascade_table_names.sort();
            cascade_table_names.dedup();
            cascade_sequence_names.sort();
            cascade_sequence_names.dedup();
            self.state.ensure_schema_metadata_slots_persistable()?;
            if !cascade_sequence_names.is_empty() {
                self.state
                    .ensure_sequence_owner_metadata_slots_persistable()?;
            }
            if self.drop_schema_privilege_metadata_changes(&drop_set, &cascade_sequence_names) {
                self.state.ensure_privilege_metadata_slots_persistable()?;
            }
            if !cascade_table_names.is_empty() {
                self.state
                    .ensure_drop_table_runtime_metadata_slots_persistable(&cascade_table_names)?;
                self.execute_drop_table(&LogicalPlan::DropTable {
                    tables: cascade_table_names.clone(),
                    if_exists: true,
                    cascade: true,
                    schema: Schema::empty(),
                })?;
            }
        }
        let runtime_snapshot = DropSchemaRuntimeSnapshot {
            schemas: drop_set
                .iter()
                .filter_map(|name| {
                    self.state
                        .schemas
                        .get(name)
                        .map(|schema| (name.clone(), Arc::clone(schema.value())))
                })
                .collect(),
            sequences: cascade_sequence_names
                .iter()
                .filter_map(|name| {
                    self.state
                        .sequences
                        .get(name)
                        .map(|seq| (name.clone(), Arc::clone(seq.value())))
                })
                .collect(),
            sequence_owners: cascade_sequence_names
                .iter()
                .filter_map(|name| {
                    self.state
                        .sequence_owners
                        .get(name)
                        .map(|owner| (name.clone(), owner.value().clone()))
                })
                .collect(),
            sequence_namespaces: cascade_sequence_names
                .iter()
                .filter_map(|name| {
                    self.state
                        .sequence_namespaces
                        .get(name)
                        .map(|namespace| (name.clone(), namespace.value().clone()))
                })
                .collect(),
            sequence_session: self.sequence_state.snapshot(),
            privilege_grants: self.state.privilege_catalog.list_grants(),
            default_privilege_grants: self.state.privilege_catalog.list_default_grants(),
        };
        let mut privilege_metadata_changed = false;
        let mut sequence_owner_metadata_changed = false;
        if *cascade {
            for name in &drop_set {
                let (sequence_removed, grants_removed) = self.drop_schema_sequences(name)?;
                sequence_owner_metadata_changed |= sequence_removed;
                privilege_metadata_changed |= grants_removed;
            }
        }
        for name in &drop_set {
            self.state.schemas.remove(name);
            privilege_metadata_changed |= self
                .state
                .privilege_catalog
                .remove_object_grants(crate::auth::PrivilegeObjectKind::Schema, name);
            privilege_metadata_changed |= self
                .state
                .privilege_catalog
                .remove_default_grants_for_schema(name);
        }
        if sequence_owner_metadata_changed {
            if let Err(err) = self.state.persist_sequence_owner_metadata() {
                self.restore_drop_schema_runtime_state(runtime_snapshot);
                return Err(err);
            }
        }
        if let Err(err) = self.state.persist_schema_metadata() {
            self.restore_drop_schema_runtime_state(runtime_snapshot);
            if sequence_owner_metadata_changed
                && let Err(restore_err) = self.state.persist_sequence_owner_metadata()
            {
                return Err(ServerError::ddl(format!(
                    "DROP SCHEMA schema metadata error: {err}; sequence owner metadata rollback failed: {restore_err}"
                )));
            }
            return Err(err);
        }
        if privilege_metadata_changed {
            if let Err(err) = self.state.persist_privilege_metadata() {
                self.restore_drop_schema_runtime_state(runtime_snapshot);
                if sequence_owner_metadata_changed
                    && let Err(restore_err) = self.state.persist_sequence_owner_metadata()
                {
                    return Err(ServerError::ddl(format!(
                        "DROP SCHEMA privilege metadata error: {err}; sequence owner metadata rollback failed: {restore_err}"
                    )));
                }
                if let Err(restore_err) = self.state.persist_schema_metadata() {
                    return Err(ServerError::ddl(format!(
                        "DROP SCHEMA privilege metadata error: {err}; schema metadata rollback failed: {restore_err}"
                    )));
                }
                return Err(err);
            }
        }
        self.plan_cache_invalidate();
        Ok(result_encoder::run_ddl_command("DROP SCHEMA"))
    }

    fn restore_drop_schema_runtime_state(&self, snapshot: DropSchemaRuntimeSnapshot) {
        for (name, schema) in snapshot.schemas {
            self.state.schemas.insert(name, schema);
        }
        for (name, sequence) in snapshot.sequences {
            self.state.sequences.insert(name, sequence);
        }
        for (name, owner) in snapshot.sequence_owners {
            self.state.sequence_owners.insert(name, owner);
        }
        for (name, namespace) in snapshot.sequence_namespaces {
            self.state.sequence_namespaces.insert(name, namespace);
        }
        self.sequence_state
            .restore_snapshot(snapshot.sequence_session);
        self.state
            .privilege_catalog
            .install_snapshot(snapshot.privilege_grants, snapshot.default_privilege_grants);
    }

    fn drop_schema_sequences(&mut self, schema_name: &str) -> Result<(bool, bool), ServerError> {
        let mut sequence_removed = false;
        let mut grants_removed = false;
        for sequence_name in self.schema_sequence_names(schema_name) {
            if let Some(seq) = self
                .state
                .sequences
                .get(&sequence_name)
                .map(|seq| seq.clone())
            {
                seq.emit_wal(
                    SequenceOpKind::Drop,
                    &sequence_name,
                    RelationId::INVALID,
                    ultrasql_core::Xid::INVALID,
                    self.state.heap.wal_sink().map(|sink| sink.as_ref()),
                )
                .map_err(|e| ServerError::ddl(format!("DROP SCHEMA sequence WAL: {e}")))?;
            }
            let sequence_key = sequence_name.to_ascii_lowercase();
            self.state.sequences.remove(&sequence_key);
            self.state.sequence_owners.remove(&sequence_key);
            self.state.sequence_namespaces.remove(&sequence_key);
            self.sequence_state.forget(&sequence_key);
            grants_removed |= self
                .state
                .privilege_catalog
                .remove_object_grants(PrivilegeObjectKind::Sequence, &sequence_key);
            sequence_removed = true;
        }
        Ok((sequence_removed, grants_removed))
    }

    fn schema_sequence_names(&self, schema_name: &str) -> Vec<String> {
        let mut names = self
            .state
            .sequence_namespaces
            .iter()
            .filter_map(|item| {
                if item.value().eq_ignore_ascii_case(schema_name) {
                    Some(item.key().clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        names.sort();
        names.dedup();
        names
    }

    fn schema_table_names(&self, schema_name: &str) -> Result<Vec<String>, ServerError> {
        let snapshot = self.state.catalog_snapshot();
        let mut names = Vec::new();
        for table in snapshot.tables_by_oid.values() {
            if !table.schema_name.eq_ignore_ascii_case(schema_name) {
                continue;
            }
            if let Some(chunk) =
                crate::time_partition::chunk_options_from_entry(table).map_err(ServerError::ddl)?
            {
                let managed_by_dropped_parent = snapshot
                    .tables_by_oid
                    .get(&chunk.parent_oid)
                    .is_some_and(|parent| {
                        parent.schema_name.eq_ignore_ascii_case(schema_name)
                            && self.state.time_partitions.contains_key(
                                &ultrasql_catalog::table_lookup_key(
                                    &parent.schema_name,
                                    &parent.name,
                                ),
                            )
                    });
                if managed_by_dropped_parent {
                    continue;
                }
            }
            names.push(ultrasql_catalog::table_lookup_key(
                &table.schema_name,
                &table.name,
            ));
        }
        names.sort();
        names.dedup();
        Ok(names)
    }

    fn drop_schema_privilege_metadata_changes(
        &self,
        schemas: &[String],
        sequence_names: &[String],
    ) -> bool {
        let grants = self.state.privilege_catalog.list_grants();
        if grants.iter().any(|grant| {
            (grant.object_kind == PrivilegeObjectKind::Schema
                && schemas
                    .iter()
                    .any(|schema| grant.object_name.eq_ignore_ascii_case(schema)))
                || (grant.object_kind == PrivilegeObjectKind::Sequence
                    && sequence_names
                        .iter()
                        .any(|sequence| grant.object_name.eq_ignore_ascii_case(sequence)))
        }) {
            return true;
        }
        self.state
            .privilege_catalog
            .list_default_grants()
            .iter()
            .any(|grant| {
                grant.schema_name.as_deref().is_some_and(|schema_name| {
                    schemas
                        .iter()
                        .any(|schema| schema_name.eq_ignore_ascii_case(schema))
                })
            })
    }

    fn schema_dependents(&self, schema_name: &str) -> Vec<String> {
        let snapshot = self.state.catalog_snapshot();
        let mut dependents = Vec::new();
        for table in snapshot.tables_by_oid.values() {
            if table.schema_name.eq_ignore_ascii_case(schema_name) {
                dependents.push(format!("table {}", table.name));
            }
        }
        for item in self.state.sequence_namespaces.iter() {
            if item.value().eq_ignore_ascii_case(schema_name) {
                dependents.push(format!("sequence {}", item.key()));
            }
        }
        for ty in snapshot.enum_types_by_oid.values() {
            if ty.schema_name.eq_ignore_ascii_case(schema_name) {
                dependents.push(format!("type {}", ty.name));
            }
        }
        for ty in snapshot.composite_types_by_oid.values() {
            if ty.schema_name.eq_ignore_ascii_case(schema_name) {
                dependents.push(format!("type {}", ty.name));
            }
        }
        for ty in snapshot.domain_types_by_oid.values() {
            if ty.schema_name.eq_ignore_ascii_case(schema_name) {
                dependents.push(format!("domain {}", ty.name));
            }
        }
        for operator in self.state.operators.iter() {
            if operator.value().namespace.eq_ignore_ascii_case(schema_name) {
                dependents.push(format!("operator {}", operator.value().name));
            }
        }
        dependents.sort();
        dependents.dedup();
        dependents
    }
}
