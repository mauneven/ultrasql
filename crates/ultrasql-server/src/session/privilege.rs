//! Privilege-management DDL execution.

use tokio::io::{AsyncRead, AsyncWrite};
use ultrasql_planner::{
    LogicalDefaultPrivilegeOperation, LogicalPlan, LogicalPrivilegeKind,
    LogicalPrivilegeObjectKind, LogicalPrivilegeSpec,
};

use super::Session;
use crate::auth::{
    AuthCatalog, DefaultPrivilegeUpdate, PrivilegeKind, PrivilegeObjectKind, PrivilegeRequest,
};
use crate::builtin_schema_name;
use crate::error::ServerError;
use crate::result_encoder::{self, SelectResult};

impl<RW> Session<RW>
where
    RW: AsyncRead + AsyncWrite + Unpin,
{
    pub(crate) fn execute_grant_privileges(
        &self,
        plan: &LogicalPlan,
    ) -> Result<SelectResult, ServerError> {
        let LogicalPlan::GrantPrivileges {
            privileges,
            object_kind,
            objects,
            grantees,
            grant_option,
            ..
        } = plan
        else {
            return Err(ServerError::Unsupported(
                "execute_grant_privileges called with non-GrantPrivileges plan",
            ));
        };
        self.validate_grantees(grantees)?;
        self.ensure_privilege_administration(*object_kind, objects)?;
        let privilege_objects = self.privilege_object_keys(*object_kind, objects)?;
        let before_grants = self.state.privilege_catalog.list_grants();
        let before_default_grants = self.state.privilege_catalog.list_default_grants();
        self.state.privilege_catalog.grant_many(
            &self.current_user,
            convert_object_kind(*object_kind),
            &privilege_objects,
            grantees,
            &convert_privileges(privileges),
            *grant_option,
        );
        if let Err(err) = self.state.persist_privilege_metadata() {
            self.state
                .privilege_catalog
                .install_snapshot(before_grants, before_default_grants);
            return Err(err);
        }
        self.plan_cache_invalidate();
        Ok(result_encoder::run_ddl_command("GRANT"))
    }

    pub(crate) fn execute_revoke_privileges(
        &self,
        plan: &LogicalPlan,
    ) -> Result<SelectResult, ServerError> {
        let LogicalPlan::RevokePrivileges {
            privileges,
            object_kind,
            objects,
            grantees,
            ..
        } = plan
        else {
            return Err(ServerError::Unsupported(
                "execute_revoke_privileges called with non-RevokePrivileges plan",
            ));
        };
        self.validate_grantees(grantees)?;
        self.ensure_privilege_administration(*object_kind, objects)?;
        let privilege_objects = self.privilege_object_keys(*object_kind, objects)?;
        let before_grants = self.state.privilege_catalog.list_grants();
        let before_default_grants = self.state.privilege_catalog.list_default_grants();
        self.state.privilege_catalog.revoke_many(
            convert_object_kind(*object_kind),
            &privilege_objects,
            grantees,
            &convert_privileges(privileges),
        );
        if let Err(err) = self.state.persist_privilege_metadata() {
            self.state
                .privilege_catalog
                .install_snapshot(before_grants, before_default_grants);
            return Err(err);
        }
        self.plan_cache_invalidate();
        Ok(result_encoder::run_ddl_command("REVOKE"))
    }

    pub(crate) fn execute_alter_default_privileges(
        &self,
        plan: &LogicalPlan,
    ) -> Result<SelectResult, ServerError> {
        let LogicalPlan::AlterDefaultPrivileges {
            target_roles,
            schemas,
            operation,
            privileges,
            object_kind,
            grantees,
            grant_option,
            ..
        } = plan
        else {
            return Err(ServerError::Unsupported(
                "execute_alter_default_privileges called with non-AlterDefaultPrivileges plan",
            ));
        };
        self.validate_grantees(grantees)?;
        for schema in schemas {
            self.ensure_schema_exists_for_privilege(schema)?;
        }
        let owner_roles = self.default_privilege_owner_roles(target_roles)?;
        let privilege_requests = convert_privileges(privileges);
        let before_grants = self.state.privilege_catalog.list_grants();
        let before_default_grants = self.state.privilege_catalog.list_default_grants();
        match operation {
            LogicalDefaultPrivilegeOperation::Grant => {
                self.state
                    .privilege_catalog
                    .grant_default_many(DefaultPrivilegeUpdate {
                        grantor: &self.current_user,
                        owner_roles: &owner_roles,
                        schemas,
                        object_kind: convert_object_kind(*object_kind),
                        grantees,
                        privileges: &privilege_requests,
                        grant_option: *grant_option,
                    });
            }
            LogicalDefaultPrivilegeOperation::Revoke => {
                self.state.privilege_catalog.revoke_default_many(
                    &owner_roles,
                    schemas,
                    convert_object_kind(*object_kind),
                    grantees,
                    &privilege_requests,
                );
            }
        }
        if let Err(err) = self.state.persist_privilege_metadata() {
            self.state
                .privilege_catalog
                .install_snapshot(before_grants, before_default_grants);
            return Err(err);
        }
        self.plan_cache_invalidate();
        Ok(result_encoder::run_ddl_command("ALTER DEFAULT PRIVILEGES"))
    }

    fn validate_grantees(&self, grantees: &[String]) -> Result<(), ServerError> {
        for grantee in grantees {
            if grantee == "public" {
                continue;
            }
            if self.state.role_catalog.lookup_role(grantee).is_none() {
                return Err(ServerError::UndefinedObject(format!(
                    "role '{grantee}' does not exist"
                )));
            }
        }
        Ok(())
    }

    fn default_privilege_owner_roles(
        &self,
        target_roles: &[String],
    ) -> Result<Vec<String>, ServerError> {
        let owner_roles = if target_roles.is_empty() {
            vec![self.current_user.clone()]
        } else {
            target_roles.to_vec()
        };
        for owner in &owner_roles {
            if self.state.role_catalog.lookup_role(owner).is_none() {
                return Err(ServerError::UndefinedObject(format!(
                    "role '{owner}' does not exist"
                )));
            }
            if !self.state.role_catalog.can_set_role(&self.auth_user, owner) {
                return Err(ServerError::InsufficientPrivilege(format!(
                    "permission denied to alter default privileges for role {owner}"
                )));
            }
            if !self.auth_role_is_superuser_for_privilege()
                && self.role_is_privileged_default_owner(owner)
            {
                return Err(ServerError::InsufficientPrivilege(format!(
                    "permission denied to alter default privileges for privileged role {owner}"
                )));
            }
        }
        Ok(owner_roles)
    }

    fn ensure_privilege_administration(
        &self,
        object_kind: LogicalPrivilegeObjectKind,
        objects: &[String],
    ) -> Result<(), ServerError> {
        self.ensure_privilege_objects_exist(object_kind, objects)?;
        if self
            .state
            .role_catalog
            .lookup_role(&self.current_user)
            .is_some_and(|role| role.is_superuser)
        {
            return Ok(());
        }
        match object_kind {
            LogicalPrivilegeObjectKind::Table => self.ensure_table_privilege_owner(objects),
            LogicalPrivilegeObjectKind::Schema => self.ensure_schema_privilege_owner(objects),
            LogicalPrivilegeObjectKind::Sequence => self.ensure_sequence_privilege_owner(objects),
            _ => Err(ServerError::InsufficientPrivilege(
                "privilege management requires object ownership or superuser".to_owned(),
            )),
        }
    }

    fn ensure_privilege_objects_exist(
        &self,
        object_kind: LogicalPrivilegeObjectKind,
        objects: &[String],
    ) -> Result<(), ServerError> {
        if object_kind == LogicalPrivilegeObjectKind::Table {
            for table in objects {
                self.table_oid_for_privilege_object(table)?;
            }
        } else if object_kind == LogicalPrivilegeObjectKind::Schema {
            for schema in objects {
                self.ensure_schema_exists_for_privilege(schema)?;
            }
        } else if object_kind == LogicalPrivilegeObjectKind::Sequence {
            for sequence in objects {
                self.sequence_key_for_privilege_object(sequence)?;
            }
        } else if object_kind == LogicalPrivilegeObjectKind::Database {
            for database in objects {
                self.ensure_database_exists_for_privilege(database)?;
            }
        } else if object_kind == LogicalPrivilegeObjectKind::Function {
            for function in objects {
                self.ensure_function_exists_for_privilege(function)?;
            }
        }
        Ok(())
    }

    fn privilege_object_keys(
        &self,
        object_kind: LogicalPrivilegeObjectKind,
        objects: &[String],
    ) -> Result<Vec<String>, ServerError> {
        if object_kind == LogicalPrivilegeObjectKind::Sequence {
            return objects
                .iter()
                .map(|sequence| self.sequence_key_for_privilege_object(sequence))
                .collect();
        }
        Ok(objects.to_vec())
    }

    fn ensure_table_privilege_owner(&self, tables: &[String]) -> Result<(), ServerError> {
        let current_user = self.current_user.to_ascii_lowercase();
        for table in tables {
            let table_oid = self.table_oid_for_privilege_object(table)?;
            let owns_table = self
                .state
                .row_security
                .get(&table_oid)
                .is_some_and(|runtime| runtime.owner_role.eq_ignore_ascii_case(&current_user));
            if !owns_table {
                return Err(ServerError::InsufficientPrivilege(format!(
                    "permission denied to manage privileges on table {table}"
                )));
            }
        }
        Ok(())
    }

    fn table_oid_for_privilege_object(
        &self,
        table: &str,
    ) -> Result<ultrasql_core::Oid, ServerError> {
        let snapshot = self.state.catalog_snapshot();
        let Some((namespace, table_name)) = privilege_relation_parts(table) else {
            return Err(ServerError::ddl(format!("table '{table}' does not exist")));
        };
        let table_key = namespace.map_or_else(
            || ultrasql_catalog::table_lookup_key("public", &table_name),
            |namespace| ultrasql_catalog::table_lookup_key(&namespace, &table_name),
        );
        let Some(meta) = snapshot.tables.get(&table_key) else {
            return Err(ServerError::ddl(format!("table '{table}' does not exist")));
        };
        Ok(meta.oid)
    }

    fn ensure_schema_privilege_owner(&self, schemas: &[String]) -> Result<(), ServerError> {
        let current_user = self.current_user.to_ascii_lowercase();
        for schema in schemas {
            let folded = self.ensure_schema_exists_for_privilege(schema)?;
            let owns_schema = self
                .state
                .schemas
                .get(&folded)
                .is_some_and(|runtime| runtime.owner_role.eq_ignore_ascii_case(&current_user));
            if !owns_schema {
                return Err(ServerError::InsufficientPrivilege(format!(
                    "permission denied to manage privileges on schema {schema}"
                )));
            }
        }
        Ok(())
    }

    fn ensure_schema_exists_for_privilege(&self, schema: &str) -> Result<String, ServerError> {
        let folded = privilege_schema_name(schema);
        if builtin_schema_name(&folded) || self.state.schemas.contains_key(&folded) {
            return Ok(folded);
        }
        Err(ServerError::ddl(format!(
            "schema '{schema}' does not exist"
        )))
    }

    fn ensure_database_exists_for_privilege(&self, database: &str) -> Result<(), ServerError> {
        if database.eq_ignore_ascii_case("ultrasql") {
            return Ok(());
        }
        Err(ServerError::UndefinedObject(format!(
            "database '{database}' does not exist"
        )))
    }

    fn ensure_function_exists_for_privilege(&self, function: &str) -> Result<(), ServerError> {
        let function_name = privilege_function_simple_name(function);
        if crate::pipeline::catalog_views::pg_proc_builtin_exists(&function_name) {
            return Ok(());
        }
        Err(ServerError::UndefinedObject(format!(
            "function '{function}' does not exist"
        )))
    }

    fn ensure_sequence_privilege_owner(&self, sequences: &[String]) -> Result<(), ServerError> {
        let current_user = self.current_user.to_ascii_lowercase();
        for sequence in sequences {
            let sequence_key = self.sequence_key_for_privilege_object(sequence)?;
            let owns_sequence = self
                .state
                .sequence_owners
                .get(&sequence_key)
                .is_some_and(|owner| owner.eq_ignore_ascii_case(&current_user));
            if !owns_sequence {
                return Err(ServerError::InsufficientPrivilege(format!(
                    "permission denied to manage privileges on sequence {sequence}"
                )));
            }
        }
        Ok(())
    }

    fn sequence_key_for_privilege_object(&self, sequence: &str) -> Result<String, ServerError> {
        let Some((namespace, sequence_name)) = privilege_relation_parts(sequence) else {
            return Err(ServerError::ddl(format!(
                "sequence '{sequence}' does not exist"
            )));
        };
        let sequence_key = if let Some(namespace) = namespace {
            crate::sequence_lookup_key(&namespace, &sequence_name)
        } else if self.state.sequences.contains_key(&sequence_name) {
            sequence_name
        } else {
            crate::search_path_schema_names(
                self.session_settings.get("search_path").map(String::as_str),
            )
            .into_iter()
            .map(|namespace| crate::sequence_lookup_key(&namespace, &sequence_name))
            .find(|key| self.state.sequences.contains_key(key))
            .unwrap_or(sequence_name)
        };
        if !self.state.sequences.contains_key(&sequence_key) {
            return Err(ServerError::ddl(format!(
                "sequence '{sequence}' does not exist"
            )));
        }
        Ok(sequence_key)
    }

    fn auth_role_is_superuser_for_privilege(&self) -> bool {
        match self.state.role_catalog.lookup_role(&self.auth_user) {
            Some(role) => role.is_superuser,
            None => self.auth_user.eq_ignore_ascii_case("tester"),
        }
    }

    fn role_is_privileged_default_owner(&self, role_name: &str) -> bool {
        self.state
            .role_catalog
            .lookup_role(role_name)
            .is_some_and(|role| role.is_superuser || role.replication || role.bypass_rls)
    }
}

fn convert_object_kind(kind: LogicalPrivilegeObjectKind) -> PrivilegeObjectKind {
    match kind {
        LogicalPrivilegeObjectKind::Table => PrivilegeObjectKind::Table,
        LogicalPrivilegeObjectKind::Schema => PrivilegeObjectKind::Schema,
        LogicalPrivilegeObjectKind::Database => PrivilegeObjectKind::Database,
        LogicalPrivilegeObjectKind::Sequence => PrivilegeObjectKind::Sequence,
        LogicalPrivilegeObjectKind::Function => PrivilegeObjectKind::Function,
    }
}

fn privilege_relation_parts(object: &str) -> Option<(Option<String>, String)> {
    let parts = crate::parse_pg_identifier_path(object)?;
    match parts.as_slice() {
        [name] => Some((None, name.to_ascii_lowercase())),
        [namespace, name] => Some((
            Some(namespace.to_ascii_lowercase()),
            name.to_ascii_lowercase(),
        )),
        _ => None,
    }
}

fn privilege_schema_name(schema: &str) -> String {
    let folded = schema.to_ascii_lowercase();
    if folded.starts_with('"')
        && let Some(parts) = crate::parse_pg_identifier_path(&folded)
        && let [name] = parts.as_slice()
    {
        return name.to_ascii_lowercase();
    }
    folded
}

fn privilege_function_simple_name(function: &str) -> String {
    let compact = function
        .trim()
        .to_ascii_lowercase()
        .chars()
        .filter(|ch| !ch.is_ascii_whitespace())
        .collect::<String>();
    let base = compact
        .split_once('(')
        .map_or(compact.as_str(), |(base, _)| base);
    base.rsplit_once('.')
        .map_or(base, |(_, name)| name)
        .to_owned()
}

fn convert_privileges(privileges: &[LogicalPrivilegeSpec]) -> Vec<PrivilegeRequest> {
    privileges
        .iter()
        .map(|privilege| PrivilegeRequest {
            privilege: convert_privilege_kind(privilege.kind),
            columns: privilege.columns.clone(),
        })
        .collect()
}

fn convert_privilege_kind(kind: LogicalPrivilegeKind) -> PrivilegeKind {
    match kind {
        LogicalPrivilegeKind::Select => PrivilegeKind::Select,
        LogicalPrivilegeKind::Insert => PrivilegeKind::Insert,
        LogicalPrivilegeKind::Update => PrivilegeKind::Update,
        LogicalPrivilegeKind::Delete => PrivilegeKind::Delete,
        LogicalPrivilegeKind::Truncate => PrivilegeKind::Truncate,
        LogicalPrivilegeKind::References => PrivilegeKind::References,
        LogicalPrivilegeKind::Trigger => PrivilegeKind::Trigger,
        LogicalPrivilegeKind::Usage => PrivilegeKind::Usage,
        LogicalPrivilegeKind::Create => PrivilegeKind::Create,
        LogicalPrivilegeKind::Connect => PrivilegeKind::Connect,
        LogicalPrivilegeKind::Temporary => PrivilegeKind::Temporary,
        LogicalPrivilegeKind::Execute => PrivilegeKind::Execute,
    }
}
