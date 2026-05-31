//! Role-management DDL execution.

use tokio::io::{AsyncRead, AsyncWrite};
use ultrasql_planner::{LogicalPlan, LogicalRoleKind, LogicalRoleOptions};

use super::Session;
use crate::auth::scram::DEFAULT_ITERATIONS;
use crate::auth::{AuthCatalog, PasswordHash, RoleEntry, RoleEntryChanges};
use crate::error::ServerError;
use crate::result_encoder::{self, SelectResult};

impl<RW> Session<RW>
where
    RW: AsyncRead + AsyncWrite + Unpin,
{
    pub(crate) fn execute_create_role(
        &self,
        plan: &LogicalPlan,
    ) -> Result<SelectResult, ServerError> {
        let LogicalPlan::CreateRole {
            kind,
            role_name,
            options,
            if_not_exists,
            ..
        } = plan
        else {
            return Err(ServerError::Unsupported(
                "execute_create_role called with non-CreateRole plan",
            ));
        };
        self.ensure_role_administration()?;
        self.ensure_privileged_role_attributes(options)?;
        let entry = build_role_entry(*kind, role_name, options)?;
        let before_roles = self.state.role_catalog.list_roles();
        let before_memberships = self.state.role_catalog.list_memberships();
        match self.state.role_catalog.create_role(entry) {
            Ok(()) => {
                if let Err(err) = self.state.persist_role_metadata() {
                    self.state
                        .role_catalog
                        .install_snapshot(before_roles, before_memberships);
                    return Err(err);
                }
                self.plan_cache_invalidate();
                Ok(result_encoder::run_ddl_command("CREATE ROLE"))
            }
            Err(ultrasql_catalog::CatalogError::AlreadyExists(_)) if *if_not_exists => {
                Ok(result_encoder::run_ddl_command("CREATE ROLE"))
            }
            Err(err) => Err(err.into()),
        }
    }

    pub(crate) fn execute_alter_role(
        &self,
        plan: &LogicalPlan,
    ) -> Result<SelectResult, ServerError> {
        let LogicalPlan::AlterRole {
            role_name, options, ..
        } = plan
        else {
            return Err(ServerError::Unsupported(
                "execute_alter_role called with non-AlterRole plan",
            ));
        };
        self.ensure_role_administration()?;
        self.ensure_superuser_target_alteration(role_name)?;
        self.ensure_privileged_role_attributes(options)?;
        if role_name.eq_ignore_ascii_case("ultrasql") && bootstrap_role_privileges_change(options) {
            return Err(ServerError::ddl(
                "cannot alter bootstrap role privileges for ultrasql",
            ));
        }
        let changes = build_role_changes(options)?;
        let before_roles = self.state.role_catalog.list_roles();
        let before_memberships = self.state.role_catalog.list_memberships();
        self.state.role_catalog.alter_role(role_name, changes)?;
        if let Err(err) = self.state.persist_role_metadata() {
            self.state
                .role_catalog
                .install_snapshot(before_roles, before_memberships);
            return Err(err);
        }
        self.plan_cache_invalidate();
        Ok(result_encoder::run_ddl_command("ALTER ROLE"))
    }

    pub(crate) fn execute_drop_role(
        &self,
        plan: &LogicalPlan,
    ) -> Result<SelectResult, ServerError> {
        let LogicalPlan::DropRole {
            roles, if_exists, ..
        } = plan
        else {
            return Err(ServerError::Unsupported(
                "execute_drop_role called with non-DropRole plan",
            ));
        };
        self.ensure_role_administration()?;
        for role in roles {
            if role.eq_ignore_ascii_case("ultrasql") {
                return Err(ServerError::ddl("cannot drop bootstrap role ultrasql"));
            }
            if self.state.role_catalog.lookup_role(role).is_none() {
                continue;
            }
            let dependencies = self.role_drop_dependencies(role);
            if !dependencies.is_empty() {
                return Err(ServerError::DependentObjectsStillExist(format!(
                    "cannot drop role {role} because other objects depend on it: {}",
                    dependencies.join(", ")
                )));
            }
        }
        let before_roles = self.state.role_catalog.list_roles();
        let before_memberships = self.state.role_catalog.list_memberships();
        for role in roles {
            match self.state.role_catalog.drop_role(role) {
                Ok(()) => {}
                Err(ultrasql_catalog::CatalogError::NotFound(_)) if *if_exists => {}
                Err(err) => {
                    self.state
                        .role_catalog
                        .install_snapshot(before_roles, before_memberships);
                    return Err(err.into());
                }
            }
        }
        if let Err(err) = self.state.persist_role_metadata() {
            self.state
                .role_catalog
                .install_snapshot(before_roles, before_memberships);
            return Err(err);
        }
        self.plan_cache_invalidate();
        Ok(result_encoder::run_ddl_command("DROP ROLE"))
    }

    fn role_drop_dependencies(&self, role: &str) -> Vec<String> {
        let role = role.to_ascii_lowercase();
        let snapshot = self.state.catalog_snapshot();
        let mut dependencies = Vec::new();

        for item in self.state.row_security.iter() {
            let runtime = item.value();
            if let Some(table) = snapshot.tables_by_oid.get(item.key()) {
                if runtime.owner_role.eq_ignore_ascii_case(&role) {
                    dependencies.push(format!("table {}", table.name));
                }
                for policy in &runtime.policies {
                    if policy
                        .roles
                        .iter()
                        .any(|policy_role| policy_role.eq_ignore_ascii_case(&role))
                    {
                        dependencies.push(format!(
                            "row security policy {} on table {}",
                            policy.name, table.name
                        ));
                    }
                }
            }
        }

        for grant in self.state.privilege_catalog.list_grants() {
            if grant.grantee.eq_ignore_ascii_case(&role) {
                dependencies.push(format!(
                    "{:?} privilege grant on {} to role",
                    grant.object_kind, grant.object_name
                ));
            }
            if grant.grantor.eq_ignore_ascii_case(&role) {
                dependencies.push(format!(
                    "{:?} privilege grant on {} by role",
                    grant.object_kind, grant.object_name
                ));
            }
        }

        for grant in self.state.privilege_catalog.list_default_grants() {
            if grant.owner_role.eq_ignore_ascii_case(&role) {
                dependencies.push(format!(
                    "default {:?} privileges for owned objects",
                    grant.object_kind
                ));
            }
            if grant.grantee.eq_ignore_ascii_case(&role) {
                dependencies.push(format!(
                    "default {:?} privilege grant to role",
                    grant.object_kind
                ));
            }
            if grant.grantor.eq_ignore_ascii_case(&role) {
                dependencies.push(format!(
                    "default {:?} privilege grant by role",
                    grant.object_kind
                ));
            }
        }

        for owner in self.state.sequence_owners.iter() {
            if owner.value().eq_ignore_ascii_case(&role) {
                dependencies.push(format!("sequence {}", owner.key()));
            }
        }

        for schema in self.state.schemas.iter() {
            if schema.value().owner_role.eq_ignore_ascii_case(&role) {
                dependencies.push(format!("schema {}", schema.key()));
            }
        }

        for membership in self.state.role_catalog.list_memberships() {
            if membership.role.eq_ignore_ascii_case(&role) {
                dependencies.push(format!("role membership granted to {}", membership.member));
            }
            if membership.member.eq_ignore_ascii_case(&role) {
                dependencies.push(format!("role membership in {}", membership.role));
            }
            if membership.grantor.eq_ignore_ascii_case(&role) {
                dependencies.push(format!(
                    "role membership grant of {} to {}",
                    membership.role, membership.member
                ));
            }
        }

        dependencies.sort();
        dependencies.dedup();
        dependencies
    }

    pub(crate) fn execute_grant_role(
        &self,
        plan: &LogicalPlan,
    ) -> Result<SelectResult, ServerError> {
        let LogicalPlan::GrantRole {
            roles,
            grantees,
            admin_option,
            ..
        } = plan
        else {
            return Err(ServerError::Unsupported(
                "execute_grant_role called with non-GrantRole plan",
            ));
        };
        self.ensure_role_administration()?;
        self.ensure_privileged_role_membership_grant(roles)?;
        let before_roles = self.state.role_catalog.list_roles();
        let before_memberships = self.state.role_catalog.list_memberships();
        self.state
            .role_catalog
            .grant_roles(&self.current_user, roles, grantees, *admin_option)?;
        if let Err(err) = self.state.persist_role_metadata() {
            self.state
                .role_catalog
                .install_snapshot(before_roles, before_memberships);
            return Err(err);
        }
        self.plan_cache_invalidate();
        Ok(result_encoder::run_ddl_command("GRANT ROLE"))
    }

    pub(crate) fn execute_revoke_role(
        &self,
        plan: &LogicalPlan,
    ) -> Result<SelectResult, ServerError> {
        let LogicalPlan::RevokeRole {
            roles, grantees, ..
        } = plan
        else {
            return Err(ServerError::Unsupported(
                "execute_revoke_role called with non-RevokeRole plan",
            ));
        };
        self.ensure_role_administration()?;
        self.ensure_role_references_exist(roles)?;
        self.ensure_role_references_exist(grantees)?;
        let before_roles = self.state.role_catalog.list_roles();
        let before_memberships = self.state.role_catalog.list_memberships();
        self.state.role_catalog.revoke_roles(roles, grantees);
        if let Err(err) = self.state.persist_role_metadata() {
            self.state
                .role_catalog
                .install_snapshot(before_roles, before_memberships);
            return Err(err);
        }
        self.plan_cache_invalidate();
        Ok(result_encoder::run_ddl_command("REVOKE ROLE"))
    }

    pub(crate) fn execute_set_role(
        &mut self,
        plan: &LogicalPlan,
    ) -> Result<SelectResult, ServerError> {
        let LogicalPlan::SetRole { role_name, .. } = plan else {
            return Err(ServerError::Unsupported(
                "execute_set_role called with non-SetRole plan",
            ));
        };
        match role_name {
            Some(role) => {
                if role.eq_ignore_ascii_case(&self.auth_user) {
                    self.current_user = self.auth_user.clone();
                    return Ok(result_encoder::run_ddl_command("SET ROLE"));
                }
                if !self.state.role_catalog.can_set_role(&self.auth_user, role) {
                    return Err(ServerError::InsufficientPrivilege(format!(
                        "permission denied to set role {role}"
                    )));
                }
                self.ensure_set_role_target_not_privileged(role)?;
                self.current_user = role.clone();
            }
            None => {
                self.current_user = self.auth_user.clone();
            }
        }
        Ok(result_encoder::run_ddl_command("SET ROLE"))
    }

    fn ensure_role_references_exist(&self, roles: &[String]) -> Result<(), ServerError> {
        for role in roles {
            if self.state.role_catalog.lookup_role(role).is_none() {
                return Err(ServerError::UndefinedObject(format!(
                    "role '{role}' does not exist"
                )));
            }
        }
        Ok(())
    }

    fn ensure_role_administration(&self) -> Result<(), ServerError> {
        match self.state.role_catalog.lookup_role(&self.current_user) {
            Some(role) if role.is_superuser || role.create_role => Ok(()),
            Some(_) => Err(ServerError::InsufficientPrivilege(
                "role management requires CREATEROLE".to_owned(),
            )),
            None if self.current_user.eq_ignore_ascii_case("tester") => Ok(()),
            None => Err(ServerError::InsufficientPrivilege(format!(
                "role {} is not registered",
                self.current_user
            ))),
        }
    }

    fn ensure_privileged_role_membership_grant(&self, roles: &[String]) -> Result<(), ServerError> {
        if self.current_role_is_superuser() {
            return Ok(());
        }
        if roles
            .iter()
            .any(|role| self.lookup_role_is_privileged(role.as_str()))
        {
            return Err(ServerError::InsufficientPrivilege(
                "granting privileged role memberships requires SUPERUSER".to_owned(),
            ));
        }
        Ok(())
    }

    fn ensure_set_role_target_not_privileged(&self, role: &str) -> Result<(), ServerError> {
        if self.auth_role_is_superuser() || !self.lookup_role_is_privileged(role) {
            return Ok(());
        }
        Err(ServerError::InsufficientPrivilege(
            "setting privileged roles requires SUPERUSER".to_owned(),
        ))
    }

    fn ensure_privileged_role_attributes(
        &self,
        options: &LogicalRoleOptions,
    ) -> Result<(), ServerError> {
        if !role_options_grant_privileged_attributes(options) {
            return Ok(());
        }
        if self.current_role_is_superuser() {
            return Ok(());
        }
        Err(ServerError::InsufficientPrivilege(
            "setting SUPERUSER, REPLICATION, or BYPASSRLS requires SUPERUSER".to_owned(),
        ))
    }

    fn ensure_superuser_target_alteration(&self, role_name: &str) -> Result<(), ServerError> {
        if self.current_role_is_superuser() {
            return Ok(());
        }
        if self
            .state
            .role_catalog
            .lookup_role(role_name)
            .is_some_and(|role| role_is_privileged_membership_target(&role))
        {
            return Err(ServerError::InsufficientPrivilege(
                "altering privileged roles requires SUPERUSER".to_owned(),
            ));
        }
        Ok(())
    }

    fn auth_role_is_superuser(&self) -> bool {
        match self.state.role_catalog.lookup_role(&self.auth_user) {
            Some(role) => role.is_superuser,
            None => self.auth_user.eq_ignore_ascii_case("tester"),
        }
    }

    fn current_role_is_superuser(&self) -> bool {
        match self.state.role_catalog.lookup_role(&self.current_user) {
            Some(role) => role.is_superuser,
            None => self.current_user.eq_ignore_ascii_case("tester"),
        }
    }

    fn lookup_role_is_privileged(&self, role: &str) -> bool {
        self.state
            .role_catalog
            .lookup_role(role)
            .is_some_and(|role| role_is_privileged_membership_target(&role))
    }
}

fn build_role_entry(
    kind: LogicalRoleKind,
    role_name: &str,
    options: &LogicalRoleOptions,
) -> Result<RoleEntry, ServerError> {
    let mut entry = RoleEntry {
        oid: 0,
        name: role_name.to_owned(),
        password: None,
        is_superuser: false,
        inherit: true,
        create_role: false,
        create_db: false,
        can_login: matches!(kind, LogicalRoleKind::User),
        replication: false,
        bypass_rls: false,
        connection_limit: -1,
        valid_until: None,
    };
    if let Some(value) = options.superuser {
        entry.is_superuser = value;
    }
    if let Some(value) = options.inherit {
        entry.inherit = value;
    }
    if let Some(value) = options.create_role {
        entry.create_role = value;
    }
    if let Some(value) = options.create_db {
        entry.create_db = value;
    }
    if let Some(value) = options.can_login {
        entry.can_login = value;
    }
    if let Some(value) = options.replication {
        entry.replication = value;
    }
    if let Some(value) = options.bypass_rls {
        entry.bypass_rls = value;
    }
    if let Some(value) = options.connection_limit {
        validate_connection_limit(value)?;
        entry.connection_limit = value;
    }
    if let Some(password) = &options.password {
        entry.password = hash_role_password(password.as_deref())?;
    }
    if let Some(value) = &options.valid_until {
        entry.valid_until = parse_valid_until(value)?;
    }
    Ok(entry)
}

fn build_role_changes(options: &LogicalRoleOptions) -> Result<RoleEntryChanges, ServerError> {
    if let Some(value) = options.connection_limit {
        validate_connection_limit(value)?;
    }
    Ok(RoleEntryChanges {
        password: options
            .password
            .as_ref()
            .map(|password| hash_role_password(password.as_deref()))
            .transpose()?,
        is_superuser: options.superuser,
        inherit: options.inherit,
        create_role: options.create_role,
        create_db: options.create_db,
        can_login: options.can_login,
        replication: options.replication,
        bypass_rls: options.bypass_rls,
        connection_limit: options.connection_limit,
        valid_until: options
            .valid_until
            .as_ref()
            .map(|value| parse_valid_until(value))
            .transpose()?,
    })
}

fn bootstrap_role_privileges_change(options: &LogicalRoleOptions) -> bool {
    options.superuser.is_some()
        || options.inherit.is_some()
        || options.create_role.is_some()
        || options.create_db.is_some()
        || options.can_login.is_some()
        || options.connection_limit.is_some()
        || options.valid_until.is_some()
}

fn role_options_grant_privileged_attributes(options: &LogicalRoleOptions) -> bool {
    matches!(options.superuser, Some(true))
        || matches!(options.replication, Some(true))
        || matches!(options.bypass_rls, Some(true))
}

fn role_is_privileged_membership_target(role: &RoleEntry) -> bool {
    role.is_superuser || role.replication || role.bypass_rls
}

fn hash_role_password(password: Option<&str>) -> Result<Option<PasswordHash>, ServerError> {
    let Some(plaintext) = password else {
        return Ok(None);
    };
    let salt = PasswordHash::random_salt();
    PasswordHash::hash_password(plaintext, &salt, DEFAULT_ITERATIONS)
        .map(Some)
        .map_err(|err| ServerError::ddl(format!("role password hashing failed: {err}")))
}

fn validate_connection_limit(value: i32) -> Result<(), ServerError> {
    if value < -1 {
        return Err(ServerError::ddl(
            "role CONNECTION LIMIT must be -1 or greater",
        ));
    }
    Ok(())
}

fn parse_valid_until(value: &str) -> Result<Option<i64>, ServerError> {
    if value.eq_ignore_ascii_case("infinity") {
        return Ok(None);
    }
    let normalized = if value.contains(' ') && !value.contains('T') {
        value.replacen(' ', "T", 1)
    } else {
        value.to_owned()
    };
    let parsed = chrono::DateTime::parse_from_rfc3339(&normalized)
        .map_err(|_| ServerError::ddl("role VALID UNTIL must be an RFC3339 timestamp"))?;
    Ok(Some(parsed.timestamp_micros()))
}
