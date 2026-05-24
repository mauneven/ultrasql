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
        let entry = build_role_entry(*kind, role_name, options)?;
        match self.state.role_catalog.create_role(entry) {
            Ok(()) => {
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
        let changes = build_role_changes(options)?;
        self.state.role_catalog.alter_role(role_name, changes)?;
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
        for role in roles {
            match self.state.role_catalog.drop_role(role) {
                Ok(()) => {}
                Err(ultrasql_catalog::CatalogError::NotFound(_)) if *if_exists => {}
                Err(err) => return Err(err.into()),
            }
        }
        self.plan_cache_invalidate();
        Ok(result_encoder::run_ddl_command("DROP ROLE"))
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
        self.ensure_role_membership_admin()?;
        self.state
            .role_catalog
            .grant_roles(&self.current_user, roles, grantees, *admin_option)?;
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
        self.ensure_role_membership_admin()?;
        self.state.role_catalog.revoke_roles(roles, grantees);
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
                if !self.state.role_catalog.can_set_role(&self.auth_user, role) {
                    return Err(ServerError::InsufficientPrivilege(format!(
                        "permission denied to set role {role}"
                    )));
                }
                self.current_user = role.clone();
            }
            None => {
                self.current_user = self.auth_user.clone();
            }
        }
        Ok(result_encoder::run_ddl_command("SET ROLE"))
    }

    fn ensure_role_membership_admin(&self) -> Result<(), ServerError> {
        let allowed = self
            .state
            .role_catalog
            .lookup_role(&self.current_user)
            .is_some_and(|role| role.is_superuser || role.create_role);
        if allowed {
            Ok(())
        } else {
            Err(ServerError::InsufficientPrivilege(
                "role membership management requires CREATEROLE".to_owned(),
            ))
        }
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
        entry.password = hash_role_password(password.as_deref());
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
            .map(|password| hash_role_password(password.as_deref())),
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

fn hash_role_password(password: Option<&str>) -> Option<PasswordHash> {
    password.map(|plaintext| {
        let salt = PasswordHash::random_salt();
        PasswordHash::hash_password(plaintext, &salt, DEFAULT_ITERATIONS)
    })
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
